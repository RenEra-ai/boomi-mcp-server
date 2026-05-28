"""Issue #26: Generated XML profile XML builder (element-only).

Emits ``<bns:Component type="profile.xml">`` XML from a structured element
tree matching the issue #43 ``profile_from_xml_schema`` contract.

Reference XML shapes verified against live Boomi exports (fetched
2026-05-25):

* reneraai-5RO3DD ``74f66e9e-fd30-470c-970e-397ee29fed73`` (Shipping Order
  XML) — nested repeating + non-repeating elements, character / datetime /
  number leaves.
* work ``9570b55c-993c-4715-9bc5-3d8d8353ff1e`` (Intapp CDS PATCH Request
  XML) — flat rows/row/leaves shape with maxOccurs=-1 on the row segment.

Envelope shape (every emitted profile mirrors these segments):

.. code-block:: xml

    <bns:Component type="profile.xml" name="..." folderFullPath="...">
      <bns:encryptedValues/>
      <bns:description></bns:description>
      <bns:object>
        <XMLProfile xmlns="" modelVersion="2" strict="true">
          <ProfileProperties>
            <XMLGeneralInfo/>
            <XMLOptions encoding="utf8" implicitElementOrdering="true"
                        parseRespectMaxOccurs="true" respectMinOccurs="false"
                        respectMinOccursAlways="false"/>
          </ProfileProperties>
          <DataElements>
            <XMLElement dataType="character" isMappable="true" isNode="true"
                        isRoot="true" key="1" loopingOption="unique"
                        maxOccurs="1" minOccurs="1" name="Root"
                        useNamespace="-1" validateData="false">
              <DataFormat><ProfileCharacterFormat/></DataFormat>
              <QualifierList/>
              {nested XMLElement children...}
            </XMLElement>
          </DataElements>
          <Namespaces>
            <XMLNamespace key="-1" name="Empty Namespace"><Types/></XMLNamespace>
          </Namespaces>
          <tagLists/>
        </XMLProfile>
      </bns:object>
    </bns:Component>

M2 is element-only — attributes / namespaces / schema imports raise
``UNSUPPORTED_XML_PROFILE_FEATURE``. Complex XML profiles use the raw-XML
escape hatch or wait for issue #47 (XSD inference).
"""

from __future__ import annotations

from typing import Any, Dict, List, Mapping, Optional, Tuple

from ._preservation_policy import OwnedPath, PreservationPolicy
from .connector_builder import BuilderValidationError, _escape_xml
from .profile_generation import (
    PROFILE_FIELD_VALIDATION_FAILED,
    UNSUPPORTED_PROFILE_FIELD_TYPE,
    UNSUPPORTED_XML_PROFILE_FEATURE,
    profile_from_xml_schema,
)


# XML element leaf data formats — match _SUPPORTED_FIELD_TYPES in
# profile_builder.py for character/number/datetime; boolean stores as
# character format in Boomi XML profiles.
_DATA_FORMAT_TAG: Dict[str, str] = {
    "character": "<DataFormat><ProfileCharacterFormat/></DataFormat>",
    "number": '<DataFormat><ProfileNumberFormat numberFormat=""/></DataFormat>',
    "datetime": '<DataFormat><ProfileDateFormat dateFormat="yyyy-MM-dd"/></DataFormat>',
    "boolean": "<DataFormat><ProfileCharacterFormat/></DataFormat>",
}

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

# XML-specific top-level config keys that signal unsupported profile features.
# Each rejection points callers either at the raw-XML escape hatch or at
# issue #47 (XSD inference).
_UNSUPPORTED_XML_FEATURE_KEYS: Tuple[str, ...] = (
    "attributes",
    "namespaces",
    "namespace_uri",
    "schema_import",
    "xsd",
)


class XMLGeneratedProfileBuilder:
    """Emit profile.xml XML for a structured element tree (element-only)."""

    SUPPORTED_PROFILE_TYPES: Tuple[str, ...] = ("xml.generated",)
    FORBIDDEN_SECRET_FIELDS: Tuple[str, ...] = _FORBIDDEN_SECRET_FIELDS

    # ------------------------------------------------------------------
    # Secret scanning (mirrors JSONGeneratedProfileBuilder)
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
                        f"{field_path!r} cannot be supplied in a generated XML "
                        "profile config — XML profiles do not transport "
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
                hint="Use profile_type='xml.generated'.",
            )

        component_name = config.get("component_name")
        if not component_name or not str(component_name).strip():
            return BuilderValidationError(
                "component_name is required",
                error_code=PROFILE_FIELD_VALIDATION_FAILED,
                field="component_name",
                hint="Provide a non-empty component_name string.",
            )

        # Reject unsupported XML features at the top level and walk children
        # defensively. The deep tree validation goes through
        # profile_from_xml_schema which catches structural problems.
        feature_err = _scan_unsupported_xml_features(config, "")
        if feature_err is not None:
            return feature_err

        root = config.get("root")
        if not isinstance(root, dict):
            return BuilderValidationError(
                "root must be an XML element node dict",
                error_code=PROFILE_FIELD_VALIDATION_FAILED,
                field="root",
                hint=(
                    "Provide a {name, kind: 'element', children: [...]} root "
                    "node describing the XML element tree."
                ),
            )

        try:
            profile_from_xml_schema(
                {"format": "xml", "root": root},
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
        validation_err = cls.validate_config(config)
        if validation_err is not None:
            raise validation_err
        _, index = _walk_root_for_emit(config["root"], emit=False)
        return index

    # ------------------------------------------------------------------
    # XML emission
    # ------------------------------------------------------------------

    def build(self, **params: Any) -> str:
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
            f'type="profile.xml"{folder_attr} '
            f'name="{_escape_xml(component_name)}">'
            "<bns:encryptedValues/>"
            f"<bns:description>{_escape_xml(description)}</bns:description>"
            "<bns:object>"
            '<XMLProfile xmlns="" modelVersion="2" strict="true">'
            "<ProfileProperties>"
            "<XMLGeneralInfo/>"
            '<XMLOptions encoding="utf8" implicitElementOrdering="true" '
            'parseRespectMaxOccurs="true" respectMinOccurs="false" '
            'respectMinOccursAlways="false"/>'
            "</ProfileProperties>"
            "<DataElements>"
            f"{body_xml}"
            "</DataElements>"
            "<Namespaces>"
            '<XMLNamespace key="-1" name="Empty Namespace"><Types/></XMLNamespace>'
            "</Namespaces>"
            "<tagLists/>"
            "</XMLProfile>"
            "</bns:object>"
            "</bns:Component>"
        )


# ---------------------------------------------------------------------------
# Unsupported feature detection
# ---------------------------------------------------------------------------


def _scan_unsupported_xml_features(
    node: Any, path: str
) -> Optional[BuilderValidationError]:
    """Reject XML profile features that M2 does not support.

    Recursively scans every node dict for the keys in
    ``_UNSUPPORTED_XML_FEATURE_KEYS``. Returns the first finding (depth-first).
    """
    if isinstance(node, dict):
        for forbidden in _UNSUPPORTED_XML_FEATURE_KEYS:
            if forbidden in node:
                field_path = f"{path}.{forbidden}" if path else forbidden
                return BuilderValidationError(
                    f"{field_path!r} is not supported by the M2 XML profile "
                    f"builder",
                    error_code=UNSUPPORTED_XML_PROFILE_FEATURE,
                    field=field_path,
                    hint=(
                        "M2 XML profiles are element-only. For attributes, "
                        "namespaces, or schema imports, supply raw XML via "
                        "config={'xml': '...'} (the escape hatch) or wait for "
                        "issue #47 (XSD/sample-XML inference)."
                    ),
                    details={"unsupported_feature": forbidden},
                )
        for key, value in node.items():
            child_err = _scan_unsupported_xml_features(
                value, f"{path}.{key}" if path else key
            )
            if child_err is not None:
                return child_err
    elif isinstance(node, list):
        for index, item in enumerate(node):
            child_err = _scan_unsupported_xml_features(item, f"{path}[{index}]")
            if child_err is not None:
                return child_err
    return None


# ---------------------------------------------------------------------------
# Shared walker — emits XML and/or builds the field index in one pre-order pass
# ---------------------------------------------------------------------------


def _walk_root_for_emit(
    root: Mapping[str, Any], *, emit: bool
) -> Tuple[str, Dict[str, Dict[str, Any]]]:
    state: Dict[str, Any] = {"next_key": 0}

    def alloc() -> int:
        state["next_key"] += 1
        return state["next_key"]

    lines: List[str] = []
    index: Dict[str, Dict[str, Any]] = {}

    _emit_xml_element(
        root,
        parent_key_path=[],
        parent_name_path=[],
        parent_logical_path="",
        lines=lines,
        index=index,
        alloc=alloc,
        emit=emit,
        is_root=True,
    )
    return "".join(lines), index


def _emit_xml_element(
    node: Mapping[str, Any],
    *,
    parent_key_path: List[str],
    parent_name_path: List[str],
    parent_logical_path: str,
    lines: List[str],
    index: Dict[str, Dict[str, Any]],
    alloc,
    emit: bool,
    is_root: bool = False,
) -> None:
    name = str(node["name"]).strip()
    kind = node["kind"]
    if kind != "element":
        raise BuilderValidationError(
            f"element kind={kind!r} is not supported",
            error_code=PROFILE_FIELD_VALIDATION_FAILED,
            field="kind",
            details={"kind": kind, "path": parent_logical_path or name},
        )

    required = bool(node.get("required", False))
    children_raw = node.get("children")
    has_children = isinstance(children_raw, list) and len(children_raw) > 0
    min_occurs = node.get("min_occurs", 1 if is_root else 0)
    max_occurs = node.get("max_occurs", 1)

    element_key = alloc()
    logical_path = name if is_root else f"{parent_logical_path}/{name}"
    name_path = parent_name_path + [name] if not is_root else [name]
    key_path = parent_key_path + [f"*[@key='{element_key}']"]

    is_root_attr = ' isRoot="true"' if is_root else ""

    if has_children:
        # Structural element
        index[logical_path] = {
            "path": logical_path,
            "name": name,
            "key": element_key,
            "key_path": "/".join(key_path),
            "name_path": "/".join(name_path),
            "data_type": None,
            "kind": "element",
            "required": required,
            "min_occurs": min_occurs,
            "max_occurs": max_occurs,
            "mappable": False,
        }
        if emit:
            lines.append(
                f'<XMLElement dataType="character" isMappable="true" '
                f'isNode="true"{is_root_attr} key="{element_key}" '
                f'loopingOption="unique" maxOccurs="{max_occurs}" '
                f'minOccurs="{min_occurs}" name="{_escape_xml(name)}" '
                f'useNamespace="-1" validateData="false">'
                "<DataFormat><ProfileCharacterFormat/></DataFormat>"
                "<QualifierList/>"
            )

        # Children path uses [] when this element repeats (max_occurs != 1).
        children_logical_segment = (
            f"{logical_path}[]" if max_occurs != 1 else logical_path
        )
        for child in children_raw:
            _emit_xml_element(
                child,
                parent_key_path=key_path,
                parent_name_path=name_path,
                parent_logical_path=children_logical_segment,
                lines=lines,
                index=index,
                alloc=alloc,
                emit=emit,
            )

        if emit:
            lines.append("</XMLElement>")
        return

    # Leaf element
    data_type = node.get("data_type")
    if data_type not in _SUPPORTED_LEAF_TYPES:
        raise BuilderValidationError(
            f"{logical_path}.data_type={data_type!r} is not supported",
            error_code=UNSUPPORTED_PROFILE_FIELD_TYPE,
            field=f"{logical_path}.data_type",
            details={"data_type": data_type, "path": logical_path},
        )

    index[logical_path] = {
        "path": logical_path,
        "name": name,
        "key": element_key,
        "key_path": "/".join(key_path),
        "name_path": "/".join(name_path),
        "data_type": data_type,
        "kind": "element",
        "required": required,
        "min_occurs": min_occurs,
        "max_occurs": max_occurs,
        "mappable": True,
    }

    if emit:
        lines.append(
            f'<XMLElement dataType="{data_type}" isMappable="true" '
            f'isNode="true"{is_root_attr} key="{element_key}" '
            f'loopingOption="unique" maxOccurs="{max_occurs}" '
            f'minOccurs="{min_occurs}" name="{_escape_xml(name)}" '
            f'useNamespace="-1" validateData="false">'
            f"{_DATA_FORMAT_TAG[data_type]}"
            "<QualifierList/>"
            "</XMLElement>"
        )


# Issue #45 — update-preservation policy. The builder owns the
# `<XMLProfile><DataElements>` subtree; XMLProfile siblings like
# `Namespaces`, `ProfileProperties` extras, and `tagLists` survive.
XMLGeneratedProfileBuilder.PRESERVATION_POLICY = PreservationPolicy(
    component_type="profile.xml",
    owned_paths=(OwnedPath(path="bns:object/XMLProfile/DataElements"),),
)
