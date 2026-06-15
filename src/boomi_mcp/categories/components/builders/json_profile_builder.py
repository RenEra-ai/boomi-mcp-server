"""Issue #26: Generated JSON profile XML builder.

Emits ``<bns:Component type="profile.json">`` XML from a structured field tree
matching the issue #43 ``profile_from_json_schema`` contract. Pairs with the
direct map builder (``map_builder.DirectMapBuilder``) and the integration
builder routing for ``profile.json`` / ``json.generated``.

Reference XML shapes verified against live Boomi exports (fetched
2026-05-25):

* legacy-ref-acct (decommissioned) ``954783c1-443f-4efd-9f92-ad380d078216`` (Slack Chat
  Message CREATE Request) — flat root-object shape.
* work ``dbe1f2b9-e238-4da0-8211-65570781cf28`` (CDS PATCH Request
  JSON) — nested root → object → array(repeating) → object → entries shape.

Envelope shape (every emitted profile mirrors these segments):

.. code-block:: xml

    <bns:Component type="profile.json" name="..." folderFullPath="...">
      <bns:encryptedValues/>
      <bns:description></bns:description>
      <bns:object>
        <JSONProfile xmlns="" strict="false">
          <DataElements>
            <JSONRootValue dataType="character" isMappable="true" isNode="true"
                           key="1" name="Root">
              <DataFormat><ProfileCharacterFormat/></DataFormat>
              <JSONObject isMappable="false" isNode="true" key="2" name="Object">
                {entries...}
              </JSONObject>
              <Qualifiers><QualifierList/></Qualifiers>
            </JSONRootValue>
          </DataElements>
          <tagLists/>
        </JSONProfile>
      </bns:object>
    </bns:Component>

JSON profile leaf data formats follow the live XML profile conventions:

* ``character`` → ``<DataFormat><ProfileCharacterFormat/></DataFormat>``
* ``number``    → ``<DataFormat><ProfileNumberFormat numberFormat=""/></DataFormat>``
* ``datetime``  → ``<DataFormat><ProfileDateFormat dateFormat="yyyy-MM-dd"/></DataFormat>``
* ``boolean``   → ``<DataFormat/>`` (empty self-closing per Slack live ref)

Key allocation is pre-order dense integers starting at 1 — the JSONRootValue
gets key 1, the wrapper JSONObject gets key 2, and each subsequent emitted
node consumes the next integer. Deterministic ordering is essential for
the map builder which references nodes by ``key_path`` / ``name_path``.
"""

from __future__ import annotations

from typing import Any, Dict, List, Mapping, Optional, Tuple

from ._preservation_policy import OwnedPath, PreservationPolicy
from .connector_builder import BuilderValidationError, _escape_xml
from .profile_generation import (
    DUPLICATE_PROFILE_FIELD_PATH,
    INVALID_PROFILE_FIELD_PATH,
    PROFILE_FIELD_VALIDATION_FAILED,
    UNSUPPORTED_PROFILE_FIELD_TYPE,
    profile_from_json_schema,
)


# JSON leaf data formats — character / number / datetime mirror the existing
# DatabaseReadProfileBuilder ``_SUPPORTED_FIELD_TYPES`` map; boolean is JSON-
# specific and emits an empty ``<DataFormat/>`` per the Slack live reference.
_DATA_FORMAT_TAG: Dict[str, str] = {
    "character": "<DataFormat><ProfileCharacterFormat/></DataFormat>",
    "number": '<DataFormat><ProfileNumberFormat numberFormat=""/></DataFormat>',
    "datetime": '<DataFormat><ProfileDateFormat dateFormat="yyyy-MM-dd"/></DataFormat>',
    "boolean": "<DataFormat/>",
}

# JSON profile leaf type set — matches profile_from_json_schema's
# ``_JSON_LEAF_TYPES``. boolean is JSON-only (XML profiles fall back to
# character format for boolean values).
_SUPPORTED_LEAF_TYPES: Tuple[str, ...] = ("character", "number", "datetime", "boolean")


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


class JSONGeneratedProfileBuilder:
    """Emit profile.json XML for a structured JSON field tree.

    Public surface mirrors ``DatabaseReadProfileBuilder``:

    * ``validate_config(config) -> Optional[BuilderValidationError]`` —
      plan-time validation (no XML emission, no Boomi calls).
    * ``build(**params) -> str`` — emits the wrapped ``<bns:Component>`` XML
      string; raises ``BuilderValidationError`` on malformed input.
    * ``build_field_index(config) -> Dict[str, Dict[str, Any]]`` — returns a
      ``{logical_path: {key, key_path, name_path, data_type, kind, mappable,
      required}}`` index. The direct map builder consumes this to render
      ``<Mapping fromKey/toKey fromKeyPath/toKeyPath fromNamePath/toNamePath/>``.
    """

    SUPPORTED_PROFILE_TYPES: Tuple[str, ...] = ("json.generated",)

    # Class-level constants used by integration_builder for cross-component
    # classification and secret redaction. Mirrors profile_builder.py.
    FORBIDDEN_SECRET_FIELDS: Tuple[str, ...] = _FORBIDDEN_SECRET_FIELDS

    # ------------------------------------------------------------------
    # Secret scanning (mirrors _DatabaseReadProfileBuilderBase pattern)
    # ------------------------------------------------------------------

    @classmethod
    def scan_forbidden_secret_fields(
        cls, config: Any, _path_prefix: str = ""
    ) -> Optional[BuilderValidationError]:
        """Detect plaintext secret-shaped keys at any depth in the config."""
        if isinstance(config, dict):
            for forbidden in cls.FORBIDDEN_SECRET_FIELDS:
                if forbidden in config:
                    field_path = f"{_path_prefix}{forbidden}"
                    return BuilderValidationError(
                        f"{field_path!r} cannot be supplied in a generated JSON "
                        "profile config — JSON profiles do not transport "
                        "secrets.",
                        error_code="PLAINTEXT_SECRET_REJECTED",
                        field=field_path,
                        hint=(
                            "Remove the secret-shaped field. Connection-level "
                            "credentials live on the connector-settings "
                            "component via credential_ref."
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
        cls, config: Dict[str, Any]
    ) -> Optional[BuilderValidationError]:
        """Validate the structured JSON profile config and return None on
        success or a BuilderValidationError describing the first problem."""
        secret_err = cls.scan_forbidden_secret_fields(config)
        if secret_err is not None:
            return secret_err

        profile_type = config.get("profile_type") or ""
        if profile_type not in cls.SUPPORTED_PROFILE_TYPES:
            return BuilderValidationError(
                f"profile_type must be one of {cls.SUPPORTED_PROFILE_TYPES} "
                f"(got {profile_type!r})",
                error_code="UNSUPPORTED_PROFILE_GENERATION_MODE",
                field="profile_type",
                hint=(
                    "Use profile_type='json.generated'. XML/EDI/flat-file "
                    "profile types route through their own builders."
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

        root = config.get("root")
        if not isinstance(root, dict):
            return BuilderValidationError(
                "root must be a JSON profile node dict",
                error_code=PROFILE_FIELD_VALIDATION_FAILED,
                field="root",
                hint=(
                    "Provide a {name, kind: 'object', children: [...]} root "
                    "node describing the JSON payload shape."
                ),
            )

        # Delegate the structural walk to the issue #43 helper, which raises
        # BuilderValidationError on every malformed input. Capture and return.
        try:
            profile_from_json_schema(
                {"format": "json", "root": root},
                component_name=component_name,
            )
        except BuilderValidationError as err:
            return err
        return None

    # ------------------------------------------------------------------
    # Field index (consumed by direct map builder)
    # ------------------------------------------------------------------

    @classmethod
    def build_field_index(
        cls, config: Dict[str, Any]
    ) -> Dict[str, Dict[str, Any]]:
        """Return a per-leaf-and-structural-node index keyed by logical path.

        Each entry carries the live-Boomi ``key`` (integer), ``key_path``
        (XPath-like ``*[@key='N']/...``), and ``name_path`` (segment-named
        path like ``Root/Object/list/Array/list/Object/key``) required by
        the map builder.
        """
        # validate_config first so callers can't bypass shape validation.
        validation_err = cls.validate_config(config)
        if validation_err is not None:
            raise validation_err
        # Walk the tree purely to populate the index. XML emission is in
        # build(); the walker is shared.
        _, index = _walk_root_for_emit(config["root"], emit=False)
        return index

    # ------------------------------------------------------------------
    # XML emission
    # ------------------------------------------------------------------

    def build(self, **params: Any) -> str:
        """Emit the wrapped <bns:Component type='profile.json'> XML string."""
        config = dict(params)
        validation_err = self.validate_config(config)
        if validation_err is not None:
            raise validation_err

        component_name = str(config["component_name"]).strip()
        folder_path = config.get("folder_path")
        description = config.get("description") or ""

        body_xml, _ = _walk_root_for_emit(config["root"], emit=True)

        folder_attr = (
            f' folderFullPath="{_escape_xml(str(folder_path))}"'
            if folder_path
            else ""
        )

        return (
            '<bns:Component xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" '
            'xmlns:bns="http://api.platform.boomi.com/" '
            f'type="profile.json"{folder_attr} '
            f'name="{_escape_xml(component_name)}">'
            "<bns:encryptedValues/>"
            f"<bns:description>{_escape_xml(description)}</bns:description>"
            "<bns:object>"
            '<JSONProfile xmlns="" strict="false">'
            "<DataElements>"
            f"{body_xml}"
            "</DataElements>"
            "<tagLists/>"
            "</JSONProfile>"
            "</bns:object>"
            "</bns:Component>"
        )


# ---------------------------------------------------------------------------
# Shared walker — emits XML and/or builds the field index in one pre-order pass
# ---------------------------------------------------------------------------


def _walk_root_for_emit(
    root: Mapping[str, Any], *, emit: bool
) -> Tuple[str, Dict[str, Dict[str, Any]]]:
    """Walk the root tree once, optionally emit XML, and always return the
    per-node field index keyed by logical path.

    Keeping emission + index-build in one pass guarantees the keys / key
    paths / name paths in the index match the XML byte-for-byte.
    """
    state: Dict[str, Any] = {"next_key": 0}

    def alloc() -> int:
        state["next_key"] += 1
        return state["next_key"]

    lines: List[str] = []
    index: Dict[str, Dict[str, Any]] = {}

    root_name = str(root["name"]).strip()
    root_value_key = alloc()
    root_object_key = alloc()

    # The synthetic JSONObject wrapper inside JSONRootValue is named "Object"
    # by Boomi convention — matches both live references.
    root_key_path = [f"*[@key='{root_value_key}']", f"*[@key='{root_object_key}']"]
    root_name_path = [root_name, "Object"]

    # Index root logical path → JSONRootValue (the structural anchor).
    index[root_name] = {
        "path": root_name,
        "name": root_name,
        "key": root_value_key,
        "key_path": f"*[@key='{root_value_key}']",
        "name_path": root_name,
        "data_type": None,
        "kind": "object",
        "required": bool(root.get("required", False)),
        "mappable": False,
    }

    if emit:
        lines.append(
            f'<JSONRootValue dataType="character" isMappable="true" '
            f'isNode="true" key="{root_value_key}" name="{_escape_xml(root_name)}">'
            "<DataFormat><ProfileCharacterFormat/></DataFormat>"
            f'<JSONObject isMappable="false" isNode="true" '
            f'key="{root_object_key}" name="Object">'
        )

    for child in root.get("children") or []:
        _emit_object_entry(
            child,
            parent_key_path=root_key_path,
            parent_name_path=root_name_path,
            parent_logical_path=root_name,
            lines=lines,
            index=index,
            alloc=alloc,
            emit=emit,
        )

    if emit:
        lines.append(
            "</JSONObject>"
            "<Qualifiers><QualifierList/></Qualifiers>"
            "</JSONRootValue>"
        )

    return "".join(lines), index


def _emit_object_entry(
    node: Mapping[str, Any],
    *,
    parent_key_path: List[str],
    parent_name_path: List[str],
    parent_logical_path: str,
    lines: List[str],
    index: Dict[str, Dict[str, Any]],
    alloc,
    emit: bool,
) -> None:
    """Emit one ``<JSONObjectEntry>`` and recurse for object/array kinds."""
    name = str(node["name"]).strip()
    kind = node["kind"]
    required = bool(node.get("required", False))

    entry_key = alloc()
    logical_path = f"{parent_logical_path}/{name}"
    name_path = parent_name_path + [name]
    key_path = parent_key_path + [f"*[@key='{entry_key}']"]
    required_attr = ' required="true"' if required else ""

    if kind == "simple":
        data_type = node["data_type"]
        if data_type not in _SUPPORTED_LEAF_TYPES:
            # Defense-in-depth — profile_from_json_schema already rejects.
            raise BuilderValidationError(
                f"{logical_path}.data_type={data_type!r} is not supported",
                error_code=UNSUPPORTED_PROFILE_FIELD_TYPE,
                field=f"{logical_path}.data_type",
                details={"data_type": data_type, "path": logical_path},
            )
        if emit:
            lines.append(
                f'<JSONObjectEntry dataType="{data_type}" isMappable="true" '
                f'isNode="true" key="{entry_key}" '
                f'name="{_escape_xml(name)}"{required_attr}>'
                f"{_DATA_FORMAT_TAG[data_type]}"
                "</JSONObjectEntry>"
            )
        index[logical_path] = {
            "path": logical_path,
            "name": name,
            "key": entry_key,
            "key_path": "/".join(key_path),
            "name_path": "/".join(name_path),
            "data_type": data_type,
            "kind": "simple",
            "required": required,
            "mappable": True,
        }
        return

    if kind == "object":
        object_key = alloc()
        object_name_path = name_path + ["Object"]
        object_key_path = key_path + [f"*[@key='{object_key}']"]
        index[logical_path] = {
            "path": logical_path,
            "name": name,
            "key": entry_key,
            "key_path": "/".join(key_path),
            "name_path": "/".join(name_path),
            "data_type": None,
            "kind": "object",
            "required": required,
            "mappable": False,
        }
        if emit:
            lines.append(
                f'<JSONObjectEntry dataType="character" isMappable="true" '
                f'isNode="true" key="{entry_key}" '
                f'name="{_escape_xml(name)}"{required_attr}>'
                "<DataFormat><ProfileCharacterFormat/></DataFormat>"
                f'<JSONObject isMappable="false" isNode="true" '
                f'key="{object_key}" name="Object">'
            )
        for child in node.get("children") or []:
            _emit_object_entry(
                child,
                parent_key_path=object_key_path,
                parent_name_path=object_name_path,
                parent_logical_path=logical_path,
                lines=lines,
                index=index,
                alloc=alloc,
                emit=emit,
            )
        if emit:
            lines.append(
                "</JSONObject>"
                "<Qualifiers><QualifierList/></Qualifiers>"
                "</JSONObjectEntry>"
            )
        return

    if kind == "array":
        # Array container entry, then JSONArray, then JSONArrayElement, then
        # synthetic JSONObject wrapper holding the array element's fields.
        array_key = alloc()
        array_name_path = name_path + ["Array"]
        array_key_path = key_path + [f"*[@key='{array_key}']"]

        element_key = alloc()
        # JSONArrayElement name mirrors the parent ObjectEntry name (Boomi
        # convention — verified against work-profile reference).
        element_name_path = array_name_path + [name]
        element_key_path = array_key_path + [f"*[@key='{element_key}']"]

        element_object_key = alloc()
        element_object_name_path = element_name_path + ["Object"]
        element_object_key_path = element_key_path + [
            f"*[@key='{element_object_key}']"
        ]

        index[logical_path] = {
            "path": logical_path,
            "name": name,
            "key": entry_key,
            "key_path": "/".join(key_path),
            "name_path": "/".join(name_path),
            "data_type": None,
            "kind": "array",
            "required": required,
            "mappable": False,
        }

        if emit:
            lines.append(
                f'<JSONObjectEntry dataType="character" isMappable="true" '
                f'isNode="true" key="{entry_key}" '
                f'name="{_escape_xml(name)}"{required_attr}>'
                "<DataFormat><ProfileCharacterFormat/></DataFormat>"
                f'<JSONArray elementType="repeating" isMappable="false" '
                f'isNode="true" key="{array_key}" name="Array">'
                f'<JSONArrayElement dataType="character" isMappable="true" '
                f'isNode="true" key="{element_key}" maxOccurs="-1" '
                f'minOccurs="0" name="{_escape_xml(name)}">'
                "<DataFormat><ProfileCharacterFormat/></DataFormat>"
                f'<JSONObject isMappable="false" isNode="true" '
                f'key="{element_object_key}" name="Object">'
            )

        # Children of an array node use the "[]" segment in their logical
        # path (matches profile_from_json_schema's path convention).
        children_logical_segment = f"{logical_path}[]"
        for child in node.get("children") or []:
            _emit_object_entry(
                child,
                parent_key_path=element_object_key_path,
                parent_name_path=element_object_name_path,
                parent_logical_path=children_logical_segment,
                lines=lines,
                index=index,
                alloc=alloc,
                emit=emit,
            )

        if emit:
            # Qualifiers terminate the JSONArrayElement (its inner JSONObject
            # has just closed) and then the JSONObjectEntry (its JSONArray
            # has just closed). JSONArray itself contains only the single
            # JSONArrayElement — no Qualifiers child. Mismatches with the
            # live shape trigger Boomi's `Invalid content was found starting
            # with element 'Qualifiers'. One of '{JSONArrayElement}' is
            # expected.` schema rejection.
            lines.append(
                "</JSONObject>"
                "<Qualifiers><QualifierList/></Qualifiers>"
                "</JSONArrayElement>"
                "</JSONArray>"
                "<Qualifiers><QualifierList/></Qualifiers>"
                "</JSONObjectEntry>"
            )
        return

    # Defense-in-depth — profile_from_json_schema already rejects.
    raise BuilderValidationError(
        f"{logical_path}.kind={kind!r} is not supported",
        error_code=PROFILE_FIELD_VALIDATION_FAILED,
        field=f"{logical_path}.kind",
        details={"kind": kind, "path": logical_path},
    )


# Issue #45 — update-preservation policy. The builder owns the
# `<JSONProfile><DataElements>` subtree; unknown JSONProfile siblings
# (e.g., tagLists, ProfileProperties extras) survive the merge.
JSONGeneratedProfileBuilder.PRESERVATION_POLICY = PreservationPolicy(
    component_type="profile.json",
    owned_paths=(OwnedPath(path="bns:object/JSONProfile/DataElements"),),
)
