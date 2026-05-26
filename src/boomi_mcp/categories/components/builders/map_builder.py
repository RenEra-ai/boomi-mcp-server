"""Issue #26: Direct profile-to-profile transform.map builder.

Emits ``<bns:Component type="transform.map">`` XML from a structured config
that lists per-leaf source/target path mappings. M2 supports only direct
profile-to-profile movement — no map functions (#40), map scripts (#41),
XSLT (#42), lookups, defaults, or expressions.

Reference XML shape verified against live Boomi exports (fetched
2026-05-25):

* reneraai-5RO3DD ``77bb73d5-43ae-4581-8ab9-af615f3778e5`` (Order DB to
  Shipping XML).
* work ``5aa8d537-2f16-4597-8dc9-c4ffbdd9ba94`` (CDS PATCH XML→JSON).

Envelope shape (every emitted map mirrors these segments):

.. code-block:: xml

    <bns:Component type="transform.map" name="..." folderFullPath="...">
      <bns:encryptedValues/>
      <bns:description></bns:description>
      <bns:object>
        <Map xmlns="" fromProfile="<<source UUID>>" toProfile="<<target UUID>>">
          <Mappings>
            <Mapping fromKey="N" fromKeyPath="*[@key='X']/*[@key='Y']"
                     fromNamePath="A/B/C" fromType="profile"
                     toKey="M" toKeyPath="..." toNamePath="..."
                     toType="profile"/>
            ...
          </Mappings>
          <Functions optimizeExecutionOrder="true"/>
          <Defaults/>
          <DocumentCacheJoins/>
        </Map>
      </bns:object>
    </bns:Component>

Index threading: the source / target field indexes (one entry per logical
leaf path → ``{key, key_path, name_path}``) come from the matching profile
builder's ``build_field_index()``. The integration builder is responsible
for computing both indexes from in-spec generated profile components and
passing them to ``DirectMapBuilder.build()`` at apply time.

Literal existing-profile UUIDs with no caller-supplied index cannot be
indexed in M2 (#26 does not parse arbitrary Boomi profile XML — issue #47
owns discovery). Those references fail plan-time with
``MAP_PROFILE_INDEX_UNAVAILABLE``.
"""

from __future__ import annotations

from typing import Any, Dict, List, Mapping, Optional, Tuple

from .connector_builder import BuilderValidationError, _escape_xml
from .profile_generation import (
    DUPLICATE_TARGET_MAPPING,
    MAP_FIELD_NOT_FOUND,
    MAP_PROFILE_INDEX_UNAVAILABLE,
    MAP_PROFILE_REF_REQUIRED,
    PROFILE_FIELD_NOT_MAPPABLE,
    PROFILE_FIELD_VALIDATION_FAILED,
    UNSUPPORTED_TRANSFORM_ROUTE,
)


# Caller-declared keys in the map config that signal an unsupported transform
# route. Each one points at the future issue that owns the feature.
_UNSUPPORTED_TRANSFORM_KEYS: Dict[str, str] = {
    "functions": "#40 (map_function builder)",
    "function_steps": "#40 (map_function builder)",
    "scripts": "#41 (map_script builder)",
    "map_scripts": "#41 (map_script builder)",
    "xslt": "#42 (XSLT transform builder)",
    "xslt_source": "#42 (XSLT transform builder)",
    "default_values": "#40 (map_function builder; defaults bind via constant function)",
    "defaults": "#40 (map_function builder; defaults bind via constant function)",
    "lookup": "#40 (map_function builder; lookups bind via lookup function)",
    "lookups": "#40 (map_function builder; lookups bind via lookup function)",
    "expression": "#40 (map_function builder; expressions bind via custom function)",
    "expressions": "#40 (map_function builder; expressions bind via custom function)",
}

# Profile types this builder accepts as source / target. Must match the
# component_type that the corresponding profile builder advertises.
_SUPPORTED_PROFILE_TYPES: Tuple[str, ...] = ("profile.db", "profile.json", "profile.xml")


_FORBIDDEN_SECRET_FIELDS: Tuple[str, ...] = (
    "password",
    "password_ref",
    "secret",
    "token",
    "access_token",
    "client_secret",
    "api_key",
    "credentials",
    "authorization",
    "bearer",
)


class DirectMapBuilder:
    """Emit transform.map XML for direct profile-to-profile mappings."""

    SUPPORTED_MAP_TYPES: Tuple[str, ...] = ("direct",)
    FORBIDDEN_SECRET_FIELDS: Tuple[str, ...] = _FORBIDDEN_SECRET_FIELDS

    # ------------------------------------------------------------------
    # Secret scanning
    # ------------------------------------------------------------------

    @classmethod
    def scan_forbidden_secret_fields(
        cls, config: Any, _path_prefix: str = ""
    ) -> Optional[BuilderValidationError]:
        if isinstance(config, dict):
            for forbidden in cls.FORBIDDEN_SECRET_FIELDS:
                if forbidden in config:
                    field_path = f"{_path_prefix}{forbidden}"
                    return BuilderValidationError(
                        f"{field_path!r} cannot be supplied in a transform.map "
                        "config — maps do not transport secrets.",
                        error_code="PLAINTEXT_SECRET_REJECTED",
                        field=field_path,
                        hint=(
                            "Remove the secret-shaped field. Map references "
                            "profile components by ID; profile-level secrets "
                            "live on connector-settings via credential_ref."
                        ),
                    )
            for key, value in config.items():
                nested = cls.scan_forbidden_secret_fields(
                    value, _path_prefix=f"{_path_prefix}{key}."
                )
                if nested is not None:
                    return nested
        elif isinstance(config, list):
            base = _path_prefix[:-1] if _path_prefix.endswith(".") else _path_prefix
            for index, item in enumerate(config):
                nested = cls.scan_forbidden_secret_fields(
                    item, _path_prefix=f"{base}[{index}]."
                )
                if nested is not None:
                    return nested
        return None

    @classmethod
    def redact_forbidden_secret_fields_in_place(cls, config: Any) -> None:
        if isinstance(config, dict):
            for forbidden in cls.FORBIDDEN_SECRET_FIELDS:
                if forbidden in config:
                    config[forbidden] = "[REDACTED]"
            for value in config.values():
                cls.redact_forbidden_secret_fields_in_place(value)
        elif isinstance(config, list):
            for item in config:
                cls.redact_forbidden_secret_fields_in_place(item)

    # ------------------------------------------------------------------
    # Plan-time validation
    # ------------------------------------------------------------------

    @classmethod
    def validate_config(
        cls,
        config: Dict[str, Any],
        *,
        source_index: Optional[Mapping[str, Mapping[str, Any]]] = None,
        target_index: Optional[Mapping[str, Mapping[str, Any]]] = None,
    ) -> Optional[BuilderValidationError]:
        """Validate a transform.map direct config.

        ``source_index`` / ``target_index`` may be ``None`` at the earliest
        plan-time check (before integration_builder has resolved $ref
        profile components). When indexes are supplied, each
        ``field_mappings[*]`` is checked against them; otherwise the index-
        sensitive checks are deferred to apply-time (or fail with
        MAP_PROFILE_INDEX_UNAVAILABLE for literal-UUID refs).
        """
        secret_err = cls.scan_forbidden_secret_fields(config)
        if secret_err is not None:
            return secret_err

        # Unsupported transform routes (function/script/xslt/etc).
        for key, pointer in _UNSUPPORTED_TRANSFORM_KEYS.items():
            if key in config:
                return BuilderValidationError(
                    f"{key!r} is not supported by the direct map builder",
                    error_code=UNSUPPORTED_TRANSFORM_ROUTE,
                    field=key,
                    hint=(
                        f"Direct maps in #26 are profile-to-profile only. "
                        f"Use {pointer} for {key!r}."
                    ),
                    details={"unsupported_route": key, "future_issue": pointer},
                )

        map_type = config.get("map_type") or ""
        if map_type not in cls.SUPPORTED_MAP_TYPES:
            return BuilderValidationError(
                f"map_type must be one of {cls.SUPPORTED_MAP_TYPES} "
                f"(got {map_type!r})",
                error_code=PROFILE_FIELD_VALIDATION_FAILED,
                field="map_type",
                hint="Use map_type='direct' for #26.",
            )

        component_name = config.get("component_name")
        if not component_name or not str(component_name).strip():
            return BuilderValidationError(
                "component_name is required",
                error_code=PROFILE_FIELD_VALIDATION_FAILED,
                field="component_name",
                hint="Provide a non-empty component_name string.",
            )

        # Source/target profile IDs and types.
        for side in ("source", "target"):
            ref_key = f"{side}_profile_id"
            type_key = f"{side}_profile_type"
            ref_value = config.get(ref_key)
            type_value = config.get(type_key)
            if not isinstance(ref_value, str) or not ref_value.strip():
                return BuilderValidationError(
                    f"{ref_key} is required",
                    error_code=MAP_PROFILE_REF_REQUIRED,
                    field=ref_key,
                    hint=(
                        f"Provide an in-spec '$ref:KEY' pointing at a profile "
                        f"component, or a literal existing-profile UUID (note: "
                        f"literal UUIDs require an in-spec generated profile "
                        f"index — discovery is tracked by #47)."
                    ),
                    details={"side": side},
                )
            if type_value not in _SUPPORTED_PROFILE_TYPES:
                return BuilderValidationError(
                    f"{type_key} must be one of {_SUPPORTED_PROFILE_TYPES} "
                    f"(got {type_value!r})",
                    error_code=PROFILE_FIELD_VALIDATION_FAILED,
                    field=type_key,
                    hint=(
                        "Declare the referenced profile's component_type so "
                        "the integration builder can compute the field index."
                    ),
                    details={"side": side},
                )

        field_mappings = config.get("field_mappings")
        if not isinstance(field_mappings, list) or not field_mappings:
            return BuilderValidationError(
                "field_mappings must be a non-empty list of "
                "{source_path, target_path} entries",
                error_code=PROFILE_FIELD_VALIDATION_FAILED,
                field="field_mappings",
                hint=(
                    "Declare at least one mapping. Direct maps in #26 are "
                    "profile-to-profile only; the source/target paths must "
                    "appear in the corresponding profile's field index."
                ),
            )

        # Shape-check each mapping entry; we do path-existence checks only
        # when both indexes are available (apply-time-style call).
        seen_target_paths: Dict[str, int] = {}
        for index, mapping in enumerate(field_mappings):
            if not isinstance(mapping, Mapping):
                return BuilderValidationError(
                    f"field_mappings[{index}] must be a mapping object",
                    error_code=PROFILE_FIELD_VALIDATION_FAILED,
                    field=f"field_mappings[{index}]",
                )
            source_path = mapping.get("source_path")
            target_path = mapping.get("target_path")
            if not isinstance(source_path, str) or not source_path.strip():
                return BuilderValidationError(
                    f"field_mappings[{index}].source_path must be a non-blank "
                    "string",
                    error_code=PROFILE_FIELD_VALIDATION_FAILED,
                    field=f"field_mappings[{index}].source_path",
                )
            if not isinstance(target_path, str) or not target_path.strip():
                return BuilderValidationError(
                    f"field_mappings[{index}].target_path must be a non-blank "
                    "string",
                    error_code=PROFILE_FIELD_VALIDATION_FAILED,
                    field=f"field_mappings[{index}].target_path",
                )
            source_path = source_path.strip()
            target_path = target_path.strip()

            # Duplicate target check fires regardless of index availability.
            if target_path in seen_target_paths:
                return BuilderValidationError(
                    f"field_mappings[{index}].target_path is bound more than "
                    "once",
                    error_code=DUPLICATE_TARGET_MAPPING,
                    field=f"field_mappings[{index}].target_path",
                    hint=(
                        "Each destination leaf may receive at most one direct "
                        "mapping. Boomi maps reject ambiguous target writes."
                    ),
                    details={
                        "path": target_path,
                        "first_index": seen_target_paths[target_path],
                        "duplicate_index": index,
                    },
                )
            seen_target_paths[target_path] = index

            # Index-sensitive checks (apply-time).
            if source_index is not None:
                src_entry = source_index.get(source_path)
                if src_entry is None:
                    return BuilderValidationError(
                        f"field_mappings[{index}].source_path is not present "
                        "in the source profile field index",
                        error_code=MAP_FIELD_NOT_FOUND,
                        field=f"field_mappings[{index}].source_path",
                        hint=(
                            "Reference a leaf path declared in the source "
                            "profile component."
                        ),
                        details={"path": source_path, "side": "source"},
                    )
                if not src_entry.get("mappable", False):
                    return BuilderValidationError(
                        f"field_mappings[{index}].source_path resolves to a "
                        "structural node",
                        error_code=PROFILE_FIELD_NOT_MAPPABLE,
                        field=f"field_mappings[{index}].source_path",
                        hint=(
                            "Source paths must point at scalar leaves. "
                            "Object/array/structural-element nodes are not "
                            "mappable."
                        ),
                        details={"path": source_path, "side": "source"},
                    )
            if target_index is not None:
                tgt_entry = target_index.get(target_path)
                if tgt_entry is None:
                    return BuilderValidationError(
                        f"field_mappings[{index}].target_path is not present "
                        "in the target profile field index",
                        error_code=MAP_FIELD_NOT_FOUND,
                        field=f"field_mappings[{index}].target_path",
                        hint=(
                            "Reference a leaf path declared in the target "
                            "profile component."
                        ),
                        details={"path": target_path, "side": "target"},
                    )
                if not tgt_entry.get("mappable", False):
                    return BuilderValidationError(
                        f"field_mappings[{index}].target_path resolves to a "
                        "structural node",
                        error_code=PROFILE_FIELD_NOT_MAPPABLE,
                        field=f"field_mappings[{index}].target_path",
                        hint=(
                            "Target paths must point at scalar leaves. "
                            "Object/array/structural-element nodes are not "
                            "mappable destinations."
                        ),
                        details={"path": target_path, "side": "target"},
                    )

        return None

    # ------------------------------------------------------------------
    # XML emission
    # ------------------------------------------------------------------

    def build(
        self,
        *,
        source_index: Mapping[str, Mapping[str, Any]],
        target_index: Mapping[str, Mapping[str, Any]],
        **params: Any,
    ) -> str:
        """Emit the wrapped <bns:Component type='transform.map'> XML string.

        Requires resolved source / target indexes (each mapping ``logical
        path → {key, key_path, name_path}``). Integration builder is
        responsible for computing these from the in-spec profile components
        before invoking this method at apply time.
        """
        config = dict(params)
        validation_err = self.validate_config(
            config, source_index=source_index, target_index=target_index
        )
        if validation_err is not None:
            raise validation_err

        # By the time we reach apply time, $ref:KEY tokens have been
        # substituted with real Boomi UUIDs by _resolve_dependency_tokens.
        source_profile_id = str(config["source_profile_id"]).strip()
        target_profile_id = str(config["target_profile_id"]).strip()
        if source_profile_id.startswith("$ref:") or target_profile_id.startswith("$ref:"):
            raise BuilderValidationError(
                "source_profile_id and target_profile_id must be resolved to "
                "Boomi UUIDs before XML emission",
                error_code=MAP_PROFILE_INDEX_UNAVAILABLE,
                field=(
                    "source_profile_id"
                    if source_profile_id.startswith("$ref:")
                    else "target_profile_id"
                ),
                hint=(
                    "Integration builder must resolve '$ref:KEY' tokens via "
                    "_resolve_dependency_tokens before invoking build()."
                ),
            )

        component_name = str(config["component_name"]).strip()
        folder_path = config.get("folder_path")
        description = config.get("description") or ""

        mapping_lines: List[str] = []
        for mapping in config["field_mappings"]:
            source_path = str(mapping["source_path"]).strip()
            target_path = str(mapping["target_path"]).strip()
            src = source_index[source_path]
            tgt = target_index[target_path]
            mapping_lines.append(
                f'<Mapping fromKey="{src["key"]}" '
                f'fromKeyPath="{_escape_xml(src["key_path"])}" '
                f'fromNamePath="{_escape_xml(src["name_path"])}" '
                f'fromType="profile" '
                f'toKey="{tgt["key"]}" '
                f'toKeyPath="{_escape_xml(tgt["key_path"])}" '
                f'toNamePath="{_escape_xml(tgt["name_path"])}" '
                f'toType="profile"/>'
            )

        folder_attr = (
            f' folderFullPath="{_escape_xml(str(folder_path))}"'
            if folder_path
            else ""
        )

        return (
            '<bns:Component xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" '
            'xmlns:bns="http://api.platform.boomi.com/" '
            f'type="transform.map"{folder_attr} '
            f'name="{_escape_xml(component_name)}">'
            "<bns:encryptedValues/>"
            f"<bns:description>{_escape_xml(description)}</bns:description>"
            "<bns:object>"
            '<Map xmlns="" '
            f'fromProfile="{_escape_xml(source_profile_id)}" '
            f'toProfile="{_escape_xml(target_profile_id)}">'
            "<Mappings>"
            f"{''.join(mapping_lines)}"
            "</Mappings>"
            '<Functions optimizeExecutionOrder="true"/>'
            "<Defaults/>"
            "<DocumentCacheJoins/>"
            "</Map>"
            "</bns:object>"
            "</bns:Component>"
        )


# ---------------------------------------------------------------------------
# Registry
# ---------------------------------------------------------------------------


MAP_BUILDERS: Dict[Tuple[str, str], type] = {
    ("transform.map", "direct"): DirectMapBuilder,
}


def get_map_builder(component_type: str, map_type: str):
    """Return a map builder instance for (component_type, map_type), or None."""
    if not component_type or not map_type:
        return None
    key = (component_type.lower(), map_type.lower())
    builder_class = MAP_BUILDERS.get(key)
    if builder_class:
        return builder_class()
    return None
