"""Issue #26 + #40 + #41: transform.map builders.

* ``DirectMapBuilder`` (Issue #26) — profile-to-profile direct field maps.
* ``MapFunctionBuilder`` (Issue #40) — structured map-function mappings (the
  standard Boomi map-function rung). Functions are authored as typed JSON
  primitives; their XML encoding lives in
  ``map_function_registry.py``.
* ``MapScriptBuilder`` (Issue #41) — in-map calls to reusable
  ``script.mapping`` components via a ``category="userdefined"
  type="userdefined"`` ``<FunctionStep>``. The standalone reusable script
  component itself is emitted by ``script_mapping_builder.py``.

Reference XML shape evidence (fetched 2026-05-25 for direct, 2026-05-26 for
function additions, 2026-05-26 for #41 script additions):

* reneraai-5RO3DD ``77bb73d5-43ae-4581-8ab9-af615f3778e5`` (Order DB to
  Shipping XML).
* work ``5aa8d537-2f16-4597-8dc9-c4ffbdd9ba94`` (Intapp CDS PATCH XML→JSON).
* reneraai-5RO3DD ``92a8b6a9-9fe4-48c1-87bd-7369acdf6523`` (Slack payload
  map) — ``DocumentPropertyGet`` + ``<Defaults>``.
* reneraai-5RO3DD ``b8a90410-b9c5-401e-80f6-b0544f3a2104`` (CSV→XML
  summary report) — ``Sum2`` + ``DocumentPropertyGet`` showing the
  profile→function-input / function-output→profile mapping wiring.
* work ``f5481730-b9b1-4b67-96eb-3a510feaa734`` — ``String2Lower``.
* work ``e9e1a9b6-1dab-45c4-acf5-c6ba610be9ac`` — full FunctionStep
  attribute set.
* work ``0ba00843-dba8-4a50-8e57-bfcba5fbc315`` — in-map userdefined
  FunctionStep referencing a reusable script.mapping (#41 reference
  shape: ``category="userdefined" type="userdefined" id="..."`` with
  ``<Configuration/>`` empty).

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
            <!-- For function maps, also: -->
            <Mapping fromKey="..." fromKeyPath="..." fromNamePath="..."
                     fromType="profile" toFunction="<<step key>>"
                     toKey="<<input key>>" toType="function"/>
            <Mapping fromFunction="<<step key>>" fromKey="<<output key>>"
                     fromType="function" toKey="..." toKeyPath="..."
                     toNamePath="..." toType="profile"/>
          </Mappings>
          <Functions optimizeExecutionOrder="true">
            <!-- Only populated for function maps -->
            <FunctionStep cacheEnabled="true" category="..." key="N"
                          name="..." position="N" sumEnabled="false"
                          type="..." x="10.0" y="10.0">
              <Inputs>...</Inputs>
              <Outputs>...</Outputs>
              <Configuration>...</Configuration>
            </FunctionStep>
          </Functions>
          <Defaults>
            <!-- Only populated when function_mappings includes default_value -->
            <Default toKey="..." value="..."/>
          </Defaults>
          <DocumentCacheJoins/>
        </Map>
      </bns:object>
    </bns:Component>

Index threading: the source / target field indexes (one entry per logical
leaf path → ``{key, key_path, name_path}``) come from the matching profile
builder's ``build_field_index()``. The integration builder is responsible
for computing both indexes from in-spec generated profile components and
passing them to each builder's ``build()`` at apply time.

Literal existing-profile UUIDs with no caller-supplied index cannot be
indexed in M2 (#26 does not parse arbitrary Boomi profile XML — issue #47
owns discovery). Those references fail plan-time with
``MAP_PROFILE_INDEX_UNAVAILABLE``.
"""

from __future__ import annotations

from typing import Any, Dict, List, Mapping, Optional, Tuple

from .connector_builder import BuilderValidationError, _escape_xml
from .map_function_registry import (
    FUNCTION_OUTPUT_KEY,
    SUPPORTED_FUNCTION_TYPES,
    emit_default_entry,
    emit_function_step,
    get_function_family,
    validate_function_mapping,
)
from .profile_generation import (
    DUPLICATE_TARGET_MAPPING,
    MAP_FIELD_NOT_FOUND,
    MAP_PROFILE_INDEX_UNAVAILABLE,
    MAP_PROFILE_REF_REQUIRED,
    PROFILE_FIELD_NOT_MAPPABLE,
    PROFILE_FIELD_VALIDATION_FAILED,
    SCRIPT_MAPPING_REF_REQUIRED,
    UNSUPPORTED_MAP_FUNCTION_TYPE,
    UNSUPPORTED_TRANSFORM_ROUTE,
)


# Raw-XML escape-hatch keys that always reject regardless of builder. Authors
# must go through the structured contracts; raw XML belongs to the
# ``config["xml"]`` bypass which lives at the integration builder level.
_RAW_XML_REJECT_KEYS: Dict[str, str] = {
    "functions": "Provide structured function_mappings instead of raw <Functions> XML.",
    "function_steps": "Provide structured function_mappings instead of raw <FunctionStep> XML.",
    "xslt": "#42 (XSLT transform builder)",
    "xslt_source": "#42 (XSLT transform builder)",
    "scripts": (
        "Switch to map_type='script' and declare script_mappings[] referencing a "
        "script.mapping component (#41 shipped)."
    ),
    "map_scripts": (
        "Switch to map_type='script' and declare script_mappings[] referencing a "
        "script.mapping component (#41 shipped)."
    ),
    "expression": (
        "Inline Boomi expressions are not a structured primitive. Use a native "
        "function via map_type='function' (#40), or wrap the logic in a "
        "script.mapping component called via map_type='script' (#41)."
    ),
    "expressions": (
        "Inline Boomi expressions are not a structured primitive. Use a native "
        "function via map_type='function' (#40), or wrap the logic in a "
        "script.mapping component called via map_type='script' (#41)."
    ),
}


# Route-class keys rejected by ``DirectMapBuilder`` (direct maps stay
# profile-to-profile only). ``MapFunctionBuilder`` accepts ``function_mappings``
# (its primary input) and absorbs ``default_values``/``lookup`` semantics via
# named function primitives (``default_value`` / ``simple_lookup``).
_DIRECT_ONLY_REJECT_KEYS: Dict[str, str] = {
    "function_mappings": (
        "Switch to map_type='function' and declare function_mappings there."
    ),
    "default_values": (
        "Switch to map_type='function' and declare function_mappings[].function_type='default_value'."
    ),
    "defaults": (
        "Switch to map_type='function' and declare function_mappings[].function_type='default_value'."
    ),
    "lookup": (
        "Switch to map_type='function' and declare "
        "function_mappings[].function_type='simple_lookup'."
    ),
    "lookups": (
        "Switch to map_type='function' and declare "
        "function_mappings[].function_type='simple_lookup'."
    ),
    "script_mappings": (
        "Switch to map_type='script' (or 'map_script') and declare "
        "script_mappings[] referencing a script.mapping component (#41)."
    ),
}


# Function-map route-class rejections — same as ``_DIRECT_ONLY_REJECT_KEYS``
# minus ``function_mappings``; the raw default/lookup keys still reject so
# callers route through the structured primitives.
_FUNCTION_BUILDER_REJECT_KEYS: Dict[str, str] = {
    key: value
    for key, value in _DIRECT_ONLY_REJECT_KEYS.items()
    if key != "function_mappings"
}


# Allowed keys inside a ``script_mappings[]`` entry. Any other key (notably
# ``script_body`` — that belongs on a standalone ``script.mapping`` component,
# not on a transform.map script call) is rejected so the builder can't
# silently drop caller-authored content during XML emission.
_SCRIPT_MAPPING_ENTRY_KEYS: Tuple[str, ...] = (
    "script_component_id",
    "inputs",
    "outputs",
    "script_slot",
    "language",
    "cache_enabled",
)


# Script-map route-class rejections — script maps accept ``script_mappings``
# (their primary input) and optional ``field_mappings`` (mixed direct + script
# maps). Other route classes still reject so callers route through the
# structured primitives owned by the appropriate map_type.
_SCRIPT_BUILDER_REJECT_KEYS: Dict[str, str] = {
    "function_mappings": (
        "Switch to map_type='function' for native function primitives, or "
        "split the function + script work across separate maps."
    ),
    "default_values": (
        "Switch to map_type='function' and declare "
        "function_mappings[].function_type='default_value'."
    ),
    "defaults": (
        "Switch to map_type='function' and declare "
        "function_mappings[].function_type='default_value'."
    ),
    "lookup": (
        "Switch to map_type='function' and declare "
        "function_mappings[].function_type='simple_lookup'."
    ),
    "lookups": (
        "Switch to map_type='function' and declare "
        "function_mappings[].function_type='simple_lookup'."
    ),
}


# Profile types every map builder accepts as source / target. Must match the
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


# ---------------------------------------------------------------------------
# Shared module-level helpers (consumed by both builders)
# ---------------------------------------------------------------------------


def _scan_forbidden_secret_fields(
    config: Any,
    forbidden: Tuple[str, ...],
    _path_prefix: str = "",
) -> Optional[BuilderValidationError]:
    """Recursive secret-shaped key scan shared by both map builders."""
    if isinstance(config, dict):
        for key in forbidden:
            if key in config:
                field_path = f"{_path_prefix}{key}"
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
            nested = _scan_forbidden_secret_fields(
                value, forbidden, _path_prefix=f"{_path_prefix}{key}."
            )
            if nested is not None:
                return nested
    elif isinstance(config, list):
        base = _path_prefix[:-1] if _path_prefix.endswith(".") else _path_prefix
        for index, item in enumerate(config):
            nested = _scan_forbidden_secret_fields(
                item, forbidden, _path_prefix=f"{base}[{index}]."
            )
            if nested is not None:
                return nested
    return None


def _redact_forbidden_secret_fields_in_place(
    config: Any, forbidden: Tuple[str, ...]
) -> None:
    """In-place secret redaction shared by both map builders."""
    if isinstance(config, dict):
        for key in forbidden:
            if key in config:
                config[key] = "[REDACTED]"
        for value in config.values():
            _redact_forbidden_secret_fields_in_place(value, forbidden)
    elif isinstance(config, list):
        for item in config:
            _redact_forbidden_secret_fields_in_place(item, forbidden)


def _validate_raw_xml_reject(
    config: Mapping[str, Any]
) -> Optional[BuilderValidationError]:
    """Reject raw-XML escape-hatch keys and unsupported route-class keys."""
    for key, pointer in _RAW_XML_REJECT_KEYS.items():
        if key in config:
            return BuilderValidationError(
                f"{key!r} is not supported by the map builder",
                error_code=UNSUPPORTED_TRANSFORM_ROUTE,
                field=key,
                hint=pointer,
                details={"unsupported_route": key},
            )
    return None


def _validate_route_class_reject(
    config: Mapping[str, Any], reject_map: Mapping[str, str]
) -> Optional[BuilderValidationError]:
    """Reject route-class keys with a builder-specific hint per key."""
    for key, hint in reject_map.items():
        if key in config:
            return BuilderValidationError(
                f"{key!r} is not supported by this map builder",
                error_code=UNSUPPORTED_TRANSFORM_ROUTE,
                field=key,
                hint=hint,
                details={"unsupported_route": key},
            )
    return None


def _validate_profile_refs(
    config: Mapping[str, Any]
) -> Optional[BuilderValidationError]:
    """Shape-check source / target profile ref + profile_type pairs."""
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
    return None


def _validate_direct_field_mappings(
    field_mappings: Any,
    *,
    source_index: Optional[Mapping[str, Mapping[str, Any]]],
    target_index: Optional[Mapping[str, Mapping[str, Any]]],
    field_prefix: str = "field_mappings",
    seen_target_paths: Optional[Dict[str, int]] = None,
) -> Optional[BuilderValidationError]:
    """Validate a ``field_mappings`` list (shape + duplicate target detection).

    Returns ``None`` on success and tracks duplicates via ``seen_target_paths``
    so callers that intermix ``field_mappings`` and ``function_mappings`` can
    share a unified target-path set.
    """
    if seen_target_paths is None:
        seen_target_paths = {}

    if not isinstance(field_mappings, list):
        return BuilderValidationError(
            f"{field_prefix} must be a list of "
            "{source_path, target_path} entries",
            error_code=PROFILE_FIELD_VALIDATION_FAILED,
            field=field_prefix,
        )

    for index, mapping in enumerate(field_mappings):
        if not isinstance(mapping, Mapping):
            return BuilderValidationError(
                f"{field_prefix}[{index}] must be a mapping object",
                error_code=PROFILE_FIELD_VALIDATION_FAILED,
                field=f"{field_prefix}[{index}]",
            )
        source_path = mapping.get("source_path")
        target_path = mapping.get("target_path")
        if not isinstance(source_path, str) or not source_path.strip():
            return BuilderValidationError(
                f"{field_prefix}[{index}].source_path must be a non-blank "
                "string",
                error_code=PROFILE_FIELD_VALIDATION_FAILED,
                field=f"{field_prefix}[{index}].source_path",
            )
        if not isinstance(target_path, str) or not target_path.strip():
            return BuilderValidationError(
                f"{field_prefix}[{index}].target_path must be a non-blank "
                "string",
                error_code=PROFILE_FIELD_VALIDATION_FAILED,
                field=f"{field_prefix}[{index}].target_path",
            )
        source_path = source_path.strip()
        target_path = target_path.strip()

        if target_path in seen_target_paths:
            return BuilderValidationError(
                f"{field_prefix}[{index}].target_path is bound more than "
                "once",
                error_code=DUPLICATE_TARGET_MAPPING,
                field=f"{field_prefix}[{index}].target_path",
                hint=(
                    "Each destination leaf may receive at most one mapping. "
                    "Boomi maps reject ambiguous target writes."
                ),
                details={
                    "path": target_path,
                    "first_index": seen_target_paths[target_path],
                    "duplicate_index": index,
                },
            )
        seen_target_paths[target_path] = index

        if source_index is not None:
            src_entry = source_index.get(source_path)
            if src_entry is None:
                return BuilderValidationError(
                    f"{field_prefix}[{index}].source_path is not present "
                    "in the source profile field index",
                    error_code=MAP_FIELD_NOT_FOUND,
                    field=f"{field_prefix}[{index}].source_path",
                    hint=(
                        "Reference a leaf path declared in the source "
                        "profile component."
                    ),
                    details={"path": source_path, "side": "source"},
                )
            if not src_entry.get("mappable", False):
                return BuilderValidationError(
                    f"{field_prefix}[{index}].source_path resolves to a "
                    "structural node",
                    error_code=PROFILE_FIELD_NOT_MAPPABLE,
                    field=f"{field_prefix}[{index}].source_path",
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
                    f"{field_prefix}[{index}].target_path is not present "
                    "in the target profile field index",
                    error_code=MAP_FIELD_NOT_FOUND,
                    field=f"{field_prefix}[{index}].target_path",
                    hint=(
                        "Reference a leaf path declared in the target "
                        "profile component."
                    ),
                    details={"path": target_path, "side": "target"},
                )
            if not tgt_entry.get("mappable", False):
                return BuilderValidationError(
                    f"{field_prefix}[{index}].target_path resolves to a "
                    "structural node",
                    error_code=PROFILE_FIELD_NOT_MAPPABLE,
                    field=f"{field_prefix}[{index}].target_path",
                    hint=(
                        "Target paths must point at scalar leaves. "
                        "Object/array/structural-element nodes are not "
                        "mappable destinations."
                    ),
                    details={"path": target_path, "side": "target"},
                )

    return None


def _render_direct_mapping(
    source_entry: Mapping[str, Any], target_entry: Mapping[str, Any]
) -> str:
    """Render one direct profile→profile ``<Mapping>`` element."""
    return (
        f'<Mapping fromKey="{source_entry["key"]}" '
        f'fromKeyPath="{_escape_xml(source_entry["key_path"])}" '
        f'fromNamePath="{_escape_xml(source_entry["name_path"])}" '
        f'fromType="profile" '
        f'toKey="{target_entry["key"]}" '
        f'toKeyPath="{_escape_xml(target_entry["key_path"])}" '
        f'toNamePath="{_escape_xml(target_entry["name_path"])}" '
        f'toType="profile"/>'
    )


def _render_map_envelope(
    *,
    component_name: str,
    folder_path: Optional[str],
    description: str,
    source_profile_id: str,
    target_profile_id: str,
    mappings_xml: str,
    functions_xml: str,
    defaults_xml: str,
) -> str:
    """Wrap the ``<Map>`` body with the standard ``<bns:Component>`` envelope."""
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
        f"<Mappings>{mappings_xml}</Mappings>"
        f"{functions_xml}"
        f"{defaults_xml}"
        "<DocumentCacheJoins/>"
        "</Map>"
        "</bns:object>"
        "</bns:Component>"
    )


# ---------------------------------------------------------------------------
# DirectMapBuilder (Issue #26)
# ---------------------------------------------------------------------------


class DirectMapBuilder:
    """Emit transform.map XML for direct profile-to-profile mappings."""

    SUPPORTED_MAP_TYPES: Tuple[str, ...] = ("direct",)
    FORBIDDEN_SECRET_FIELDS: Tuple[str, ...] = _FORBIDDEN_SECRET_FIELDS

    @classmethod
    def scan_forbidden_secret_fields(
        cls, config: Any, _path_prefix: str = ""
    ) -> Optional[BuilderValidationError]:
        return _scan_forbidden_secret_fields(
            config, cls.FORBIDDEN_SECRET_FIELDS, _path_prefix=_path_prefix
        )

    @classmethod
    def redact_forbidden_secret_fields_in_place(cls, config: Any) -> None:
        _redact_forbidden_secret_fields_in_place(config, cls.FORBIDDEN_SECRET_FIELDS)

    @classmethod
    def validate_config(
        cls,
        config: Dict[str, Any],
        *,
        source_index: Optional[Mapping[str, Mapping[str, Any]]] = None,
        target_index: Optional[Mapping[str, Mapping[str, Any]]] = None,
    ) -> Optional[BuilderValidationError]:
        """Validate a transform.map direct config."""
        secret_err = cls.scan_forbidden_secret_fields(config)
        if secret_err is not None:
            return secret_err

        raw_err = _validate_raw_xml_reject(config)
        if raw_err is not None:
            return raw_err

        direct_err = _validate_route_class_reject(config, _DIRECT_ONLY_REJECT_KEYS)
        if direct_err is not None:
            return direct_err

        map_type = config.get("map_type") or ""
        if map_type not in cls.SUPPORTED_MAP_TYPES:
            return BuilderValidationError(
                f"map_type must be one of {cls.SUPPORTED_MAP_TYPES} "
                f"(got {map_type!r})",
                error_code=PROFILE_FIELD_VALIDATION_FAILED,
                field="map_type",
                hint="Use map_type='direct' for direct profile-to-profile maps.",
            )

        component_name = config.get("component_name")
        if not component_name or not str(component_name).strip():
            return BuilderValidationError(
                "component_name is required",
                error_code=PROFILE_FIELD_VALIDATION_FAILED,
                field="component_name",
                hint="Provide a non-empty component_name string.",
            )

        ref_err = _validate_profile_refs(config)
        if ref_err is not None:
            return ref_err

        field_mappings = config.get("field_mappings")
        if not isinstance(field_mappings, list) or not field_mappings:
            return BuilderValidationError(
                "field_mappings must be a non-empty list of "
                "{source_path, target_path} entries",
                error_code=PROFILE_FIELD_VALIDATION_FAILED,
                field="field_mappings",
                hint=(
                    "Declare at least one mapping. Direct maps are "
                    "profile-to-profile only; the source/target paths must "
                    "appear in the corresponding profile's field index."
                ),
            )

        return _validate_direct_field_mappings(
            field_mappings,
            source_index=source_index,
            target_index=target_index,
        )

    def build(
        self,
        *,
        source_index: Mapping[str, Mapping[str, Any]],
        target_index: Mapping[str, Mapping[str, Any]],
        **params: Any,
    ) -> str:
        """Emit the wrapped <bns:Component type='transform.map'> XML."""
        config = dict(params)
        validation_err = self.validate_config(
            config, source_index=source_index, target_index=target_index
        )
        if validation_err is not None:
            raise validation_err

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
            mapping_lines.append(
                _render_direct_mapping(
                    source_index[source_path], target_index[target_path]
                )
            )

        return _render_map_envelope(
            component_name=component_name,
            folder_path=folder_path,
            description=description,
            source_profile_id=source_profile_id,
            target_profile_id=target_profile_id,
            mappings_xml="".join(mapping_lines),
            functions_xml='<Functions optimizeExecutionOrder="true"/>',
            defaults_xml="<Defaults/>",
        )


# ---------------------------------------------------------------------------
# MapFunctionBuilder (Issue #40)
# ---------------------------------------------------------------------------


class MapFunctionBuilder:
    """Emit transform.map XML for structured map-function mappings.

    Supports the M2.6a function family allow-list defined in
    ``map_function_registry.FUNCTION_FAMILIES``. Each entry in
    ``function_mappings`` declares one mapped output via a typed
    ``function_type`` + ``inputs`` + ``target_path`` + ``parameters``
    contract. Optional ``field_mappings`` are also accepted for mixed maps
    that combine direct copies with transformed fields. Duplicate target
    bindings across both lists are rejected.
    """

    SUPPORTED_MAP_TYPES: Tuple[str, ...] = ("function", "map_function")
    FORBIDDEN_SECRET_FIELDS: Tuple[str, ...] = _FORBIDDEN_SECRET_FIELDS

    @classmethod
    def scan_forbidden_secret_fields(
        cls, config: Any, _path_prefix: str = ""
    ) -> Optional[BuilderValidationError]:
        return _scan_forbidden_secret_fields(
            config, cls.FORBIDDEN_SECRET_FIELDS, _path_prefix=_path_prefix
        )

    @classmethod
    def redact_forbidden_secret_fields_in_place(cls, config: Any) -> None:
        _redact_forbidden_secret_fields_in_place(config, cls.FORBIDDEN_SECRET_FIELDS)

    @classmethod
    def validate_config(
        cls,
        config: Dict[str, Any],
        *,
        source_index: Optional[Mapping[str, Mapping[str, Any]]] = None,
        target_index: Optional[Mapping[str, Mapping[str, Any]]] = None,
    ) -> Optional[BuilderValidationError]:
        """Validate a transform.map function/map_function config."""
        secret_err = cls.scan_forbidden_secret_fields(config)
        if secret_err is not None:
            return secret_err

        raw_err = _validate_raw_xml_reject(config)
        if raw_err is not None:
            return raw_err

        function_reject_err = _validate_route_class_reject(
            config, _FUNCTION_BUILDER_REJECT_KEYS
        )
        if function_reject_err is not None:
            return function_reject_err

        map_type = config.get("map_type") or ""
        if map_type not in cls.SUPPORTED_MAP_TYPES:
            return BuilderValidationError(
                f"map_type must be one of {cls.SUPPORTED_MAP_TYPES} "
                f"(got {map_type!r})",
                error_code=PROFILE_FIELD_VALIDATION_FAILED,
                field="map_type",
                hint=(
                    "Use map_type='function' (or 'map_function') for "
                    "structured map-function mappings."
                ),
            )

        component_name = config.get("component_name")
        if not component_name or not str(component_name).strip():
            return BuilderValidationError(
                "component_name is required",
                error_code=PROFILE_FIELD_VALIDATION_FAILED,
                field="component_name",
                hint="Provide a non-empty component_name string.",
            )

        ref_err = _validate_profile_refs(config)
        if ref_err is not None:
            return ref_err

        function_mappings = config.get("function_mappings")
        if not isinstance(function_mappings, list) or not function_mappings:
            return BuilderValidationError(
                "function_mappings must be a non-empty list",
                error_code=PROFILE_FIELD_VALIDATION_FAILED,
                field="function_mappings",
                hint=(
                    "Declare at least one entry of "
                    "{function_type, inputs, target_path, parameters}. "
                    f"Supported function_type values: {sorted(SUPPORTED_FUNCTION_TYPES)}."
                ),
            )

        seen_target_paths: Dict[str, int] = {}

        for index, fm in enumerate(function_mappings):
            field_prefix = f"function_mappings[{index}]"
            if not isinstance(fm, Mapping):
                return BuilderValidationError(
                    f"{field_prefix} must be a mapping object",
                    error_code=PROFILE_FIELD_VALIDATION_FAILED,
                    field=field_prefix,
                )

            function_type = fm.get("function_type")
            if not isinstance(function_type, str) or not function_type.strip():
                return BuilderValidationError(
                    f"{field_prefix}.function_type must be a non-blank string",
                    error_code=UNSUPPORTED_MAP_FUNCTION_TYPE,
                    field=f"{field_prefix}.function_type",
                    hint=(
                        f"Supported function_type values: "
                        f"{sorted(SUPPORTED_FUNCTION_TYPES)}."
                    ),
                )
            family = get_function_family(function_type)
            if family is None:
                return BuilderValidationError(
                    f"{field_prefix}.function_type {function_type!r} is not "
                    "in the supported set",
                    error_code=UNSUPPORTED_MAP_FUNCTION_TYPE,
                    field=f"{field_prefix}.function_type",
                    hint=(
                        f"Supported function_type values: "
                        f"{sorted(SUPPORTED_FUNCTION_TYPES)}."
                    ),
                    details={
                        "function_type": function_type,
                        "supported": sorted(SUPPORTED_FUNCTION_TYPES),
                    },
                )

            target_path = fm.get("target_path")
            if not isinstance(target_path, str) or not target_path.strip():
                return BuilderValidationError(
                    f"{field_prefix}.target_path must be a non-blank string",
                    error_code=PROFILE_FIELD_VALIDATION_FAILED,
                    field=f"{field_prefix}.target_path",
                )
            target_path = target_path.strip()

            inputs_raw = fm.get("inputs", [])
            if inputs_raw is None:
                inputs_raw = []
            if not isinstance(inputs_raw, list):
                return BuilderValidationError(
                    f"{field_prefix}.inputs must be a list of source profile paths",
                    error_code=PROFILE_FIELD_VALIDATION_FAILED,
                    field=f"{field_prefix}.inputs",
                )
            inputs: List[str] = []
            for input_index, item in enumerate(inputs_raw):
                if not isinstance(item, str) or not item.strip():
                    return BuilderValidationError(
                        f"{field_prefix}.inputs[{input_index}] must be a non-blank string",
                        error_code=PROFILE_FIELD_VALIDATION_FAILED,
                        field=f"{field_prefix}.inputs[{input_index}]",
                    )
                inputs.append(item.strip())

            # Only treat missing / None as "use empty parameters"; preserve
            # other non-mapping values (e.g. "", [], False) so the type check
            # below catches them instead of silently coercing them away.
            parameters_raw = fm.get("parameters")
            if parameters_raw is None:
                parameters_raw = {}
            if not isinstance(parameters_raw, Mapping):
                return BuilderValidationError(
                    f"{field_prefix}.parameters must be a mapping object",
                    error_code=PROFILE_FIELD_VALIDATION_FAILED,
                    field=f"{field_prefix}.parameters",
                )

            family_err = validate_function_mapping(
                family,
                inputs=list(inputs),
                parameters=parameters_raw,
                field_prefix=field_prefix,
            )
            if family_err is not None:
                return family_err

            # Duplicate target across both lists.
            if target_path in seen_target_paths:
                return BuilderValidationError(
                    f"{field_prefix}.target_path is bound more than once",
                    error_code=DUPLICATE_TARGET_MAPPING,
                    field=f"{field_prefix}.target_path",
                    hint=(
                        "Each destination leaf may receive at most one "
                        "mapping across field_mappings and function_mappings."
                    ),
                    details={
                        "path": target_path,
                        "first_index": seen_target_paths[target_path],
                        "duplicate_index": index,
                    },
                )
            seen_target_paths[target_path] = index

            # Index-sensitive checks for inputs + target_path.
            if source_index is not None:
                for input_index, source_path in enumerate(inputs):
                    src_entry = source_index.get(source_path)
                    if src_entry is None:
                        return BuilderValidationError(
                            f"{field_prefix}.inputs[{input_index}] is not "
                            "present in the source profile field index",
                            error_code=MAP_FIELD_NOT_FOUND,
                            field=f"{field_prefix}.inputs[{input_index}]",
                            hint=(
                                "Reference a leaf path declared in the "
                                "source profile component."
                            ),
                            details={"path": source_path, "side": "source"},
                        )
                    if not src_entry.get("mappable", False):
                        return BuilderValidationError(
                            f"{field_prefix}.inputs[{input_index}] resolves "
                            "to a structural node",
                            error_code=PROFILE_FIELD_NOT_MAPPABLE,
                            field=f"{field_prefix}.inputs[{input_index}]",
                            hint=(
                                "Function inputs must be scalar leaves. "
                                "Object/array/structural-element nodes are "
                                "not mappable."
                            ),
                            details={"path": source_path, "side": "source"},
                        )
            if target_index is not None:
                tgt_entry = target_index.get(target_path)
                if tgt_entry is None:
                    return BuilderValidationError(
                        f"{field_prefix}.target_path is not present in the "
                        "target profile field index",
                        error_code=MAP_FIELD_NOT_FOUND,
                        field=f"{field_prefix}.target_path",
                        hint=(
                            "Reference a leaf path declared in the target "
                            "profile component."
                        ),
                        details={"path": target_path, "side": "target"},
                    )
                if not tgt_entry.get("mappable", False):
                    return BuilderValidationError(
                        f"{field_prefix}.target_path resolves to a "
                        "structural node",
                        error_code=PROFILE_FIELD_NOT_MAPPABLE,
                        field=f"{field_prefix}.target_path",
                        hint=(
                            "Target paths must point at scalar leaves. "
                            "Object/array/structural-element nodes are not "
                            "mappable destinations."
                        ),
                        details={"path": target_path, "side": "target"},
                    )

        # Optional field_mappings (mixed map). Validate using shared helper
        # and pass through the cross-list duplicate-target tracker.
        field_mappings = config.get("field_mappings")
        if field_mappings is not None:
            if not isinstance(field_mappings, list):
                return BuilderValidationError(
                    "field_mappings must be a list (omit for function-only maps)",
                    error_code=PROFILE_FIELD_VALIDATION_FAILED,
                    field="field_mappings",
                )
            return _validate_direct_field_mappings(
                field_mappings,
                source_index=source_index,
                target_index=target_index,
                seen_target_paths=seen_target_paths,
            )

        return None

    def build(
        self,
        *,
        source_index: Mapping[str, Mapping[str, Any]],
        target_index: Mapping[str, Mapping[str, Any]],
        **params: Any,
    ) -> str:
        """Emit the wrapped <bns:Component type='transform.map'> XML.

        Mapping order is deterministic:

        1. Direct profile→profile mappings (from ``field_mappings``) in
           declaration order.
        2. For each function mapping (in declaration order):
           a. Profile→function-input mappings (one per mapped input).
           b. Function-output→profile mapping (single output for M2.6a).
        3. ``<FunctionStep>`` blocks emitted in declaration order. IDs and
           positions match the function mapping's 1-based index.
        4. ``<Default toKey value/>`` entries from any ``default_value``
           function mappings (which bypass ``<FunctionStep>`` emission).
        """
        config = dict(params)
        validation_err = self.validate_config(
            config, source_index=source_index, target_index=target_index
        )
        if validation_err is not None:
            raise validation_err

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

        # 1. Direct mappings first.
        for mapping in config.get("field_mappings") or []:
            source_path = str(mapping["source_path"]).strip()
            target_path = str(mapping["target_path"]).strip()
            mapping_lines.append(
                _render_direct_mapping(
                    source_index[source_path], target_index[target_path]
                )
            )

        # 2. Function mappings: build function-input mappings + function-output mappings
        #    + collect non-default-value entries for <Functions> emission and
        #    default-value entries for <Defaults> emission.
        function_blocks: List[str] = []
        default_entries: List[str] = []

        function_step_counter = 0
        for fm in config["function_mappings"]:
            function_type = str(fm["function_type"]).strip().lower()
            family = get_function_family(function_type)
            if family is None:
                # validate_config rejected; defense-in-depth.
                raise BuilderValidationError(
                    f"function_type {function_type!r} is not in the supported set",
                    error_code=UNSUPPORTED_MAP_FUNCTION_TYPE,
                    field="function_type",
                )

            target_path = str(fm["target_path"]).strip()
            inputs = [str(p).strip() for p in (fm.get("inputs") or [])]
            parameters = dict(fm.get("parameters") or {})

            if family.is_default_value_sentinel:
                tgt_entry = target_index[target_path]
                default_entries.append(
                    emit_default_entry(tgt_entry["key"], str(parameters["value"]))
                )
                continue

            function_step_counter += 1
            step_key = function_step_counter

            # 2a. Profile → function input mappings.
            for input_index, source_path in enumerate(inputs, start=1):
                src_entry = source_index[source_path]
                mapping_lines.append(
                    f'<Mapping fromKey="{src_entry["key"]}" '
                    f'fromKeyPath="{_escape_xml(src_entry["key_path"])}" '
                    f'fromNamePath="{_escape_xml(src_entry["name_path"])}" '
                    f'fromType="profile" '
                    f'toFunction="{step_key}" '
                    f'toKey="{input_index}" '
                    f'toType="function"/>'
                )

            # 2b. Function output → profile mapping (single output per M2.6a).
            # fromKey must match the FUNCTION_OUTPUT_KEY emitted in the
            # corresponding FunctionStep's <Outputs> block (live Boomi UI
            # saves use key=2 for single-output families).
            tgt_entry = target_index[target_path]
            mapping_lines.append(
                f'<Mapping fromFunction="{step_key}" '
                f'fromKey="{FUNCTION_OUTPUT_KEY}" '
                f'fromType="function" '
                f'toKey="{tgt_entry["key"]}" '
                f'toKeyPath="{_escape_xml(tgt_entry["key_path"])}" '
                f'toNamePath="{_escape_xml(tgt_entry["name_path"])}" '
                f'toType="profile"/>'
            )

            # 3. FunctionStep emission.
            function_blocks.append(
                emit_function_step(
                    family, step_key=step_key, parameters=parameters
                )
            )

        functions_xml = (
            '<Functions optimizeExecutionOrder="true">'
            f"{''.join(function_blocks)}"
            "</Functions>"
        )
        defaults_xml = (
            f"<Defaults>{''.join(default_entries)}</Defaults>"
            if default_entries
            else "<Defaults/>"
        )

        return _render_map_envelope(
            component_name=component_name,
            folder_path=folder_path,
            description=description,
            source_profile_id=source_profile_id,
            target_profile_id=target_profile_id,
            mappings_xml="".join(mapping_lines),
            functions_xml=functions_xml,
            defaults_xml=defaults_xml,
        )


# ---------------------------------------------------------------------------
# MapScriptBuilder (Issue #41)
# ---------------------------------------------------------------------------


class MapScriptBuilder:
    """Emit transform.map XML that calls reusable script.mapping components.

    Each entry in ``script_mappings`` declares one in-map script call:
    a userdefined ``<FunctionStep>`` referencing a ``script.mapping``
    component by ``id``, plus profile→function-input and
    function-output→profile ``<Mapping>`` rows for each declared input
    and output. Optional ``field_mappings`` are accepted for mixed maps
    that combine direct copies with script calls. Duplicate target
    bindings across both lists reject with ``DUPLICATE_TARGET_MAPPING``.

    Live XML reference: work ``0ba00843-dba8-4a50-8e57-bfcba5fbc315``.
    The in-map shape is ``category="userdefined" type="userdefined"
    id="<scriptComponentId>"`` with ``<Configuration/>`` empty — NOT
    the ``<Configuration><Scripting>...`` block that appears in
    standalone ``transform.function`` wrappers (which remain future
    work outside #41).
    """

    SUPPORTED_MAP_TYPES: Tuple[str, ...] = ("script", "map_script")
    FORBIDDEN_SECRET_FIELDS: Tuple[str, ...] = _FORBIDDEN_SECRET_FIELDS

    @classmethod
    def scan_forbidden_secret_fields(
        cls, config: Any, _path_prefix: str = ""
    ) -> Optional[BuilderValidationError]:
        return _scan_forbidden_secret_fields(
            config, cls.FORBIDDEN_SECRET_FIELDS, _path_prefix=_path_prefix
        )

    @classmethod
    def redact_forbidden_secret_fields_in_place(cls, config: Any) -> None:
        _redact_forbidden_secret_fields_in_place(config, cls.FORBIDDEN_SECRET_FIELDS)

    @classmethod
    def validate_config(
        cls,
        config: Dict[str, Any],
        *,
        source_index: Optional[Mapping[str, Mapping[str, Any]]] = None,
        target_index: Optional[Mapping[str, Mapping[str, Any]]] = None,
    ) -> Optional[BuilderValidationError]:
        """Validate a transform.map script/map_script config."""
        secret_err = cls.scan_forbidden_secret_fields(config)
        if secret_err is not None:
            return secret_err

        raw_err = _validate_raw_xml_reject(config)
        if raw_err is not None:
            return raw_err

        route_err = _validate_route_class_reject(
            config, _SCRIPT_BUILDER_REJECT_KEYS
        )
        if route_err is not None:
            return route_err

        map_type = config.get("map_type") or ""
        if map_type not in cls.SUPPORTED_MAP_TYPES:
            return BuilderValidationError(
                f"map_type must be one of {cls.SUPPORTED_MAP_TYPES} "
                f"(got {map_type!r})",
                error_code=PROFILE_FIELD_VALIDATION_FAILED,
                field="map_type",
                hint=(
                    "Use map_type='script' (or 'map_script') for in-map "
                    "calls to reusable script.mapping components."
                ),
            )

        component_name = config.get("component_name")
        if not component_name or not str(component_name).strip():
            return BuilderValidationError(
                "component_name is required",
                error_code=PROFILE_FIELD_VALIDATION_FAILED,
                field="component_name",
                hint="Provide a non-empty component_name string.",
            )

        ref_err = _validate_profile_refs(config)
        if ref_err is not None:
            return ref_err

        script_mappings = config.get("script_mappings")
        if not isinstance(script_mappings, list) or not script_mappings:
            return BuilderValidationError(
                "script_mappings must be a non-empty list",
                error_code=PROFILE_FIELD_VALIDATION_FAILED,
                field="script_mappings",
                hint=(
                    "Declare at least one entry of "
                    "{script_component_id, inputs, outputs}. inputs map "
                    "source-profile paths to script input variables; "
                    "outputs map script output variables to "
                    "target-profile paths."
                ),
            )

        seen_target_paths: Dict[str, int] = {}

        for index, sm in enumerate(script_mappings):
            field_prefix = f"script_mappings[{index}]"
            if not isinstance(sm, Mapping):
                return BuilderValidationError(
                    f"{field_prefix} must be a mapping object",
                    error_code=PROFILE_FIELD_VALIDATION_FAILED,
                    field=field_prefix,
                )

            # Strict-keys check: reject any key build() would silently drop.
            # Most importantly, ``script_body`` belongs on the standalone
            # ``script.mapping`` component config — accepting it here and
            # ignoring it during emit would silently discard caller-authored
            # code. (Codex r2 P2 finding #1.)
            for unknown_key in sm.keys():
                if unknown_key in _SCRIPT_MAPPING_ENTRY_KEYS:
                    continue
                if unknown_key == "script_body":
                    return BuilderValidationError(
                        f"{field_prefix}.script_body is not accepted on a "
                        "transform.map script_mappings entry — the entry "
                        "references an existing script.mapping component "
                        "by id, not an inline body.",
                        error_code=UNSUPPORTED_TRANSFORM_ROUTE,
                        field=f"{field_prefix}.script_body",
                        hint=(
                            "Declare the script body on a separate "
                            "script.mapping component (component_type="
                            "'script.mapping' with language + script_body "
                            "+ inputs + outputs), then reference its key "
                            "from this entry via script_component_id="
                            "'$ref:<script_key>'."
                        ),
                    )
                return BuilderValidationError(
                    f"{field_prefix}.{unknown_key} is not a recognised "
                    "script_mappings entry key",
                    error_code=PROFILE_FIELD_VALIDATION_FAILED,
                    field=f"{field_prefix}.{unknown_key}",
                    hint=(
                        f"Supported keys: {sorted(_SCRIPT_MAPPING_ENTRY_KEYS)}."
                    ),
                )

            script_component_id = sm.get("script_component_id")
            if not isinstance(script_component_id, str) or not script_component_id.strip():
                return BuilderValidationError(
                    f"{field_prefix}.script_component_id is required",
                    error_code=SCRIPT_MAPPING_REF_REQUIRED,
                    field=f"{field_prefix}.script_component_id",
                    hint=(
                        "Use '$ref:<script_key>' to reference an in-spec "
                        "script.mapping (the integration builder auto-"
                        "synthesizes a transform.function wrapper) or "
                        "'$ref:<wrapper_key>' to reference an in-spec "
                        "transform.function wrapper directly. Literal "
                        "componentIds are not accepted for map script "
                        "calls — Boomi requires the FunctionStep id to "
                        "point at a wrapper component, which the system "
                        "can only synthesize from in-spec components. The "
                        "referenced key must also appear in this map's "
                        "depends_on."
                    ),
                )

            # Type-check cache_enabled before bool() coerces stringy values.
            # ``bool("false")`` is ``True`` in Python because non-empty
            # strings are truthy — so a JSON-deserialized "false" would
            # silently emit ``cacheEnabled="true"`` in the FunctionStep.
            # (Codex r2 P2 finding #2.)
            if "cache_enabled" in sm and not isinstance(
                sm["cache_enabled"], bool
            ):
                return BuilderValidationError(
                    f"{field_prefix}.cache_enabled must be a boolean "
                    f"(got {type(sm['cache_enabled']).__name__})",
                    error_code=PROFILE_FIELD_VALIDATION_FAILED,
                    field=f"{field_prefix}.cache_enabled",
                    hint=(
                        "Pass true or false as a JSON boolean — stringy "
                        "values like \"false\" / \"true\" coerce "
                        "incorrectly under Python truthiness and would "
                        "produce the wrong cacheEnabled attribute."
                    ),
                )

            inputs_raw = sm.get("inputs")
            if not isinstance(inputs_raw, list) or not inputs_raw:
                return BuilderValidationError(
                    f"{field_prefix}.inputs must be a non-empty list",
                    error_code=PROFILE_FIELD_VALIDATION_FAILED,
                    field=f"{field_prefix}.inputs",
                    hint=(
                        "Each entry is {source_path, input_name}. "
                        "source_path is a source-profile logical path; "
                        "input_name matches an <Input name> declared on "
                        "the referenced script.mapping component."
                    ),
                )

            normalized_inputs: List[Tuple[str, str]] = []
            for in_idx, entry in enumerate(inputs_raw):
                if not isinstance(entry, Mapping):
                    return BuilderValidationError(
                        f"{field_prefix}.inputs[{in_idx}] must be a "
                        "mapping object",
                        error_code=PROFILE_FIELD_VALIDATION_FAILED,
                        field=f"{field_prefix}.inputs[{in_idx}]",
                    )
                source_path = entry.get("source_path")
                input_name = entry.get("input_name")
                if not isinstance(source_path, str) or not source_path.strip():
                    return BuilderValidationError(
                        f"{field_prefix}.inputs[{in_idx}].source_path "
                        "must be a non-blank string",
                        error_code=PROFILE_FIELD_VALIDATION_FAILED,
                        field=f"{field_prefix}.inputs[{in_idx}].source_path",
                    )
                if not isinstance(input_name, str) or not input_name.strip():
                    return BuilderValidationError(
                        f"{field_prefix}.inputs[{in_idx}].input_name "
                        "must be a non-blank string",
                        error_code=PROFILE_FIELD_VALIDATION_FAILED,
                        field=f"{field_prefix}.inputs[{in_idx}].input_name",
                    )
                normalized_inputs.append(
                    (source_path.strip(), input_name.strip())
                )

            outputs_raw = sm.get("outputs")
            if not isinstance(outputs_raw, list) or not outputs_raw:
                return BuilderValidationError(
                    f"{field_prefix}.outputs must be a non-empty list",
                    error_code=PROFILE_FIELD_VALIDATION_FAILED,
                    field=f"{field_prefix}.outputs",
                    hint=(
                        "Each entry is {output_name, target_path}. "
                        "output_name matches an <Output name> declared "
                        "on the referenced script.mapping component; "
                        "target_path is a target-profile logical path."
                    ),
                )

            normalized_outputs: List[Tuple[str, str]] = []
            for out_idx, entry in enumerate(outputs_raw):
                if not isinstance(entry, Mapping):
                    return BuilderValidationError(
                        f"{field_prefix}.outputs[{out_idx}] must be a "
                        "mapping object",
                        error_code=PROFILE_FIELD_VALIDATION_FAILED,
                        field=f"{field_prefix}.outputs[{out_idx}]",
                    )
                output_name = entry.get("output_name")
                target_path = entry.get("target_path")
                if not isinstance(output_name, str) or not output_name.strip():
                    return BuilderValidationError(
                        f"{field_prefix}.outputs[{out_idx}].output_name "
                        "must be a non-blank string",
                        error_code=PROFILE_FIELD_VALIDATION_FAILED,
                        field=f"{field_prefix}.outputs[{out_idx}].output_name",
                    )
                if not isinstance(target_path, str) or not target_path.strip():
                    return BuilderValidationError(
                        f"{field_prefix}.outputs[{out_idx}].target_path "
                        "must be a non-blank string",
                        error_code=PROFILE_FIELD_VALIDATION_FAILED,
                        field=f"{field_prefix}.outputs[{out_idx}].target_path",
                    )
                normalized_outputs.append(
                    (output_name.strip(), target_path.strip())
                )

                # Cross-list duplicate target detection.
                tp = target_path.strip()
                if tp in seen_target_paths:
                    return BuilderValidationError(
                        f"{field_prefix}.outputs[{out_idx}].target_path "
                        "is bound more than once",
                        error_code=DUPLICATE_TARGET_MAPPING,
                        field=f"{field_prefix}.outputs[{out_idx}].target_path",
                        hint=(
                            "Each destination leaf may receive at most "
                            "one mapping across field_mappings and "
                            "script_mappings outputs."
                        ),
                        details={
                            "path": tp,
                            "first_index": seen_target_paths[tp],
                            "duplicate_index": index,
                        },
                    )
                seen_target_paths[tp] = index

            # Index-sensitive path existence checks.
            if source_index is not None:
                for in_idx, (source_path, _input_name) in enumerate(
                    normalized_inputs
                ):
                    src_entry = source_index.get(source_path)
                    if src_entry is None:
                        return BuilderValidationError(
                            f"{field_prefix}.inputs[{in_idx}].source_path "
                            "is not present in the source profile field "
                            "index",
                            error_code=MAP_FIELD_NOT_FOUND,
                            field=f"{field_prefix}.inputs[{in_idx}].source_path",
                            hint=(
                                "Reference a leaf path declared in the "
                                "source profile component."
                            ),
                            details={"path": source_path, "side": "source"},
                        )
                    if not src_entry.get("mappable", False):
                        return BuilderValidationError(
                            f"{field_prefix}.inputs[{in_idx}].source_path "
                            "resolves to a structural node",
                            error_code=PROFILE_FIELD_NOT_MAPPABLE,
                            field=f"{field_prefix}.inputs[{in_idx}].source_path",
                            hint=(
                                "Script inputs must be scalar leaves. "
                                "Object/array/structural-element nodes "
                                "are not mappable."
                            ),
                            details={"path": source_path, "side": "source"},
                        )
            if target_index is not None:
                for out_idx, (_output_name, target_path) in enumerate(
                    normalized_outputs
                ):
                    tgt_entry = target_index.get(target_path)
                    if tgt_entry is None:
                        return BuilderValidationError(
                            f"{field_prefix}.outputs[{out_idx}].target_path "
                            "is not present in the target profile field "
                            "index",
                            error_code=MAP_FIELD_NOT_FOUND,
                            field=f"{field_prefix}.outputs[{out_idx}].target_path",
                            hint=(
                                "Reference a leaf path declared in the "
                                "target profile component."
                            ),
                            details={"path": target_path, "side": "target"},
                        )
                    if not tgt_entry.get("mappable", False):
                        return BuilderValidationError(
                            f"{field_prefix}.outputs[{out_idx}].target_path "
                            "resolves to a structural node",
                            error_code=PROFILE_FIELD_NOT_MAPPABLE,
                            field=f"{field_prefix}.outputs[{out_idx}].target_path",
                            hint=(
                                "Target paths must point at scalar "
                                "leaves. Object/array/structural-element "
                                "nodes are not mappable destinations."
                            ),
                            details={"path": target_path, "side": "target"},
                        )

        # Optional field_mappings (mixed direct + script map). Validate
        # via shared helper and pass through the cross-list duplicate
        # tracker.
        field_mappings = config.get("field_mappings")
        if field_mappings is not None:
            if not isinstance(field_mappings, list):
                return BuilderValidationError(
                    "field_mappings must be a list (omit for script-only maps)",
                    error_code=PROFILE_FIELD_VALIDATION_FAILED,
                    field="field_mappings",
                )
            return _validate_direct_field_mappings(
                field_mappings,
                source_index=source_index,
                target_index=target_index,
                seen_target_paths=seen_target_paths,
            )

        return None

    def build(
        self,
        *,
        source_index: Mapping[str, Mapping[str, Any]],
        target_index: Mapping[str, Mapping[str, Any]],
        **params: Any,
    ) -> str:
        """Emit the wrapped ``<bns:Component type='transform.map'>`` XML.

        Mapping order is deterministic:

        1. Direct profile→profile mappings (from ``field_mappings``) in
           declaration order.
        2. For each script mapping (in declaration order):
           a. Profile→function-input mappings (one per declared input).
           b. Function-output→profile mappings (one per declared output).
        3. ``<FunctionStep>`` blocks emitted in declaration order. ``key`` /
           ``position`` match the script mapping's 1-based index. ``id`` is
           the resolved script.mapping component UUID.
        4. ``<Defaults/>`` is emitted empty — script maps do not author
           default values; that's the ``function`` route's job.
        """
        config = dict(params)
        validation_err = self.validate_config(
            config, source_index=source_index, target_index=target_index
        )
        if validation_err is not None:
            raise validation_err

        source_profile_id = str(config["source_profile_id"]).strip()
        target_profile_id = str(config["target_profile_id"]).strip()
        if source_profile_id.startswith("$ref:") or target_profile_id.startswith("$ref:"):
            raise BuilderValidationError(
                "source_profile_id and target_profile_id must be resolved "
                "to Boomi UUIDs before XML emission",
                error_code=MAP_PROFILE_INDEX_UNAVAILABLE,
                field=(
                    "source_profile_id"
                    if source_profile_id.startswith("$ref:")
                    else "target_profile_id"
                ),
                hint=(
                    "Integration builder must resolve '$ref:KEY' tokens "
                    "via _resolve_dependency_tokens before invoking build()."
                ),
            )

        component_name = str(config["component_name"]).strip()
        folder_path = config.get("folder_path")
        description = config.get("description") or ""

        mapping_lines: List[str] = []

        # 1. Direct field_mappings first.
        for mapping in config.get("field_mappings") or []:
            source_path = str(mapping["source_path"]).strip()
            target_path = str(mapping["target_path"]).strip()
            mapping_lines.append(
                _render_direct_mapping(
                    source_index[source_path], target_index[target_path]
                )
            )

        # 2. + 3. Script mappings: profile↔function Mapping rows + FunctionStep.
        function_blocks: List[str] = []
        for step_index, sm in enumerate(config["script_mappings"], start=1):
            step_key = step_index
            script_component_id = str(sm["script_component_id"]).strip()
            if script_component_id.startswith("$ref:"):
                # Defense-in-depth: integration builder must resolve $ref
                # tokens before invoking build().
                raise BuilderValidationError(
                    f"script_mappings[{step_index - 1}].script_component_id "
                    "must be resolved to a Boomi UUID before XML emission",
                    error_code=MAP_PROFILE_INDEX_UNAVAILABLE,
                    field=f"script_mappings[{step_index - 1}].script_component_id",
                    hint=(
                        "Integration builder must resolve '$ref:KEY' "
                        "tokens via _resolve_dependency_tokens before "
                        "invoking build()."
                    ),
                )

            cache_enabled = bool(sm.get("cache_enabled", False))
            display_name = (
                str(sm.get("script_slot") or component_name).strip()
                or component_name
            )

            # 2a. profile → function input mappings (port keys 1..N).
            inputs_xml_parts: List[str] = []
            for input_idx, entry in enumerate(sm["inputs"], start=1):
                source_path = str(entry["source_path"]).strip()
                input_name = str(entry["input_name"]).strip()
                src_entry = source_index[source_path]
                mapping_lines.append(
                    f'<Mapping fromKey="{src_entry["key"]}" '
                    f'fromKeyPath="{_escape_xml(src_entry["key_path"])}" '
                    f'fromNamePath="{_escape_xml(src_entry["name_path"])}" '
                    f'fromType="profile" '
                    f'toFunction="{step_key}" '
                    f'toKey="{input_idx}" '
                    f'toType="function"/>'
                )
                inputs_xml_parts.append(
                    f'<Input key="{input_idx}" name="{_escape_xml(input_name)}"/>'
                )

            # 2b. function output → profile mappings (port keys 1..M).
            outputs_xml_parts: List[str] = []
            for output_idx, entry in enumerate(sm["outputs"], start=1):
                output_name = str(entry["output_name"]).strip()
                target_path = str(entry["target_path"]).strip()
                tgt_entry = target_index[target_path]
                mapping_lines.append(
                    f'<Mapping fromFunction="{step_key}" '
                    f'fromKey="{output_idx}" '
                    f'fromType="function" '
                    f'toKey="{tgt_entry["key"]}" '
                    f'toKeyPath="{_escape_xml(tgt_entry["key_path"])}" '
                    f'toNamePath="{_escape_xml(tgt_entry["name_path"])}" '
                    f'toType="profile"/>'
                )
                outputs_xml_parts.append(
                    f'<Output key="{output_idx}" name="{_escape_xml(output_name)}"/>'
                )

            # 3. userdefined FunctionStep (live shape — empty Configuration).
            y = 10.0 + 50.0 * step_key
            function_blocks.append(
                f'<FunctionStep cacheEnabled="{"true" if cache_enabled else "false"}" '
                f'cacheOption="none" category="userdefined" enabled="true" '
                f'id="{_escape_xml(script_component_id)}" key="{step_key}" '
                f'name="{_escape_xml(display_name)}" position="{step_key}" '
                f'sumEnabled="false" type="userdefined" x="30.0" y="{y}">'
                f'<Inputs>{"".join(inputs_xml_parts)}</Inputs>'
                f'<Outputs>{"".join(outputs_xml_parts)}</Outputs>'
                "<Configuration/>"
                "</FunctionStep>"
            )

        functions_xml = (
            '<Functions optimizeExecutionOrder="true">'
            f"{''.join(function_blocks)}"
            "</Functions>"
        )

        return _render_map_envelope(
            component_name=component_name,
            folder_path=folder_path,
            description=description,
            source_profile_id=source_profile_id,
            target_profile_id=target_profile_id,
            mappings_xml="".join(mapping_lines),
            functions_xml=functions_xml,
            defaults_xml="<Defaults/>",
        )


# ---------------------------------------------------------------------------
# Registry
# ---------------------------------------------------------------------------


MAP_BUILDERS: Dict[Tuple[str, str], type] = {
    ("transform.map", "direct"): DirectMapBuilder,
    ("transform.map", "function"): MapFunctionBuilder,
    ("transform.map", "map_function"): MapFunctionBuilder,
    ("transform.map", "script"): MapScriptBuilder,
    ("transform.map", "map_script"): MapScriptBuilder,
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
