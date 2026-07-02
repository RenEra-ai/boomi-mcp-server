"""Issue #131 (M11.7, epic #118) — ProcessPropertyBuilder.

Emits ``<bns:Component type="processproperty">`` XML from a structured config
declaring the component's Defined Process Property slots. The component is
the profile-agnostic counterpart of ``script.mapping`` (#41): a standalone,
reusable component that ``transform.map`` map functions reference — the
``defined_process_property_get`` / ``defined_process_property_set`` families
carry ``parameters.process_property_component_id`` (``$ref`` → this
component) plus ``parameters.process_property_key`` (a property ``key`` UUID
declared HERE). Explicit caller-supplied keys are the v1 contract: the same
UUID is written into the component and handed to the referencing map
function, so no token mechanism is needed to carry generated keys.

Reference XML shape evidence (all live-captured; #119 census fixtures under
``tests/fixtures/live_xml/m11/``):

* renera ``415e6f5b-499e-4552-a047-d7d0a01e761e`` — minimal one-property
  component (``processproperty_minimal.xml``).
* work ``ba10c271-977a-444a-a340-b211ec38c1ed`` — 18 properties across
  boolean/string/number with populated ``allowedValueSet`` entries
  (``processproperty_allowed_values.xml``).
* work ``1b96c8b3-8d12-4a79-bed0-d4379f4da5b8`` — ``persisted=true`` string
  properties (``processproperty_persisted.xml``).

Envelope shape:

.. code-block:: xml

    <bns:Component type="processproperty" name="..." folderFullPath="...">
      <bns:encryptedValues/>
      <bns:description>...</bns:description>
      <bns:object>
        <DefinedProcessProperties xmlns="">
          <definedProcessProperty key="<UUID>">
            <helpText>...</helpText>
            <label>...</label>
            <type>string</type>
            <defaultValue>...</defaultValue>
            <allowedValues/>
            <persisted>false</persisted>
          </definedProcessProperty>
        </DefinedProcessProperties>
      </bns:object>
    </bns:Component>

Key shape facts (from live XML — do not invent):

* ``<DefinedProcessProperties>`` resets ``xmlns=""`` (same pattern as
  ``script.mapping``'s ``<MappingScript>``).
* Child order inside ``definedProcessProperty`` is fixed: ``helpText``,
  ``label``, ``type``, ``defaultValue``, ``allowedValues``, ``persisted``.
* The property ``key`` is a UUID distinct from the ``label``.
* Live ``<type>`` values observed: ``string`` / ``number`` / ``boolean``;
  ``date`` is companion+docs-corroborated and admitted; ``character`` and
  ``password`` have no evidence and are rejected (#119 census).
* There is NO per-property ``encrypted`` element (the intake brief's
  ``encrypted`` flag does not exist in any live capture) — configs carrying
  one are rejected with an explicit hint.
* ``<allowedValues/>`` is always emitted EMPTY in v1 (no ``allowed_values``
  config key). Because the builder owns the full ``DefinedProcessProperties``
  subtree, a structured UPDATE of a legacy component replaces populated
  ``allowedValueSet`` entries — the schema template warns about this.
* ``componentId`` / ``version`` / dates are server-assigned and ABSENT on
  create (the shared create/update invariant).
"""

from __future__ import annotations

import uuid
from typing import Any, Dict, List, Mapping, Optional, Tuple

from ._preservation_policy import OwnedPath, PreservationPolicy
from .connector_builder import BuilderValidationError, _escape_xml
from .profile_generation import (
    PROCESS_PROPERTY_DEFAULT_INVALID,
    PROCESS_PROPERTY_DUPLICATE_KEY,
    PROCESS_PROPERTY_DUPLICATE_NAME,
    PROCESS_PROPERTY_KEY_INVALID,
    PROCESS_PROPERTY_KEY_REQUIRED,
    PROCESS_PROPERTY_NAME_REQUIRED,
    PROCESS_PROPERTY_PROPERTY_REQUIRED,
    PROCESS_PROPERTY_RAW_XML_UNSUPPORTED,
    PROCESS_PROPERTY_TYPE_UNSUPPORTED,
    PROCESS_PROPERTY_VALIDATION_FAILED,
)


# v1 allow-list (#119 census): string/number/boolean are live-verified;
# date is companion+official-docs corroborated. character and password have
# no live or docs evidence as a Defined Process Property <type> and are
# rejected (password additionally because its secret/default-value policy is
# unresolved — see the architect plan).
_SUPPORTED_PROPERTY_TYPES: Tuple[str, ...] = ("string", "number", "boolean", "date")


# Mirrors ScriptMappingBuilder's secret-shaped key set so one audit covers
# every generated-component builder family.
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


# Top-level config keys this builder accepts. ``component_type`` / ``xml``
# are integration-builder plumbing (the dispatcher threads component_type
# into the payload; ``xml`` is the raw escape hatch handled upstream) —
# same rationale as ScriptMappingBuilder's allow-list.
_ALLOWED_TOP_LEVEL_KEYS: Tuple[str, ...] = (
    "component_type",
    "component_name",
    "folder_path",
    "description",
    "properties",
    "xml",
)


# Per-property config keys. Deliberately absent: ``encrypted`` (no live
# evidence such an element exists) and ``allowed_values`` (v1 always emits
# an empty <allowedValues/>). Both get targeted rejection hints.
_ALLOWED_PROPERTY_KEYS: Tuple[str, ...] = (
    "key",
    "name",
    "type",
    "default_value",
    "help_text",
    "persisted",
)

_REJECTED_PROPERTY_KEY_HINTS: Dict[str, str] = {
    "encrypted": (
        "No per-property encrypted element exists in any live processproperty "
        "capture (#119 census) — remove it. Component-level encryption uses "
        "<bns:encryptedValues/>, which is preserved automatically on update."
    ),
    "allowed_values": (
        "v1 always emits an empty <allowedValues/>; constrained value sets are "
        "a follow-up. NOTE: a structured update replaces the whole "
        "DefinedProcessProperties subtree, so legacy allowedValueSet entries "
        "do not survive a v1 structured update."
    ),
    "label": "Use 'name' — it becomes the <label> (the display name map functions reference).",
}


def _scan_forbidden_secret_fields(
    config: Any, _path_prefix: str = ""
) -> Optional[BuilderValidationError]:
    """Recursive secret-shaped key scan (dict keys only)."""
    if isinstance(config, dict):
        for key in _FORBIDDEN_SECRET_FIELDS:
            if key in config:
                field_path = f"{_path_prefix}{key}"
                return BuilderValidationError(
                    f"{field_path!r} cannot be supplied in a processproperty "
                    "config — property components must not transport credentials.",
                    error_code="PLAINTEXT_SECRET_REJECTED",
                    field=field_path,
                    hint=(
                        "Remove the secret-shaped field. Secrets belong in "
                        "connection components / environment extensions, not "
                        "in Defined Process Property defaults."
                    ),
                )
        for key, value in config.items():
            nested = _scan_forbidden_secret_fields(
                value, _path_prefix=f"{_path_prefix}{key}."
            )
            if nested is not None:
                return nested
    elif isinstance(config, list):
        base = _path_prefix[:-1] if _path_prefix.endswith(".") else _path_prefix
        for index, item in enumerate(config):
            nested = _scan_forbidden_secret_fields(
                item, _path_prefix=f"{base}[{index}]."
            )
            if nested is not None:
                return nested
    return None


def _is_uuid(value: str) -> bool:
    # Canonical lowercase form only — the key is emitted verbatim and map
    # functions reference it byte-for-byte, so normalization variants would
    # silently break the coupling.
    try:
        return str(uuid.UUID(value)) == value
    except (ValueError, AttributeError, TypeError):
        return False


class ProcessPropertyBuilder:
    """Emit ``processproperty`` component XML from structured config."""

    SUPPORTED_COMPONENT_TYPES: Tuple[str, ...] = ("processproperty",)
    FORBIDDEN_SECRET_FIELDS: Tuple[str, ...] = _FORBIDDEN_SECRET_FIELDS
    SUPPORTED_PROPERTY_TYPES: Tuple[str, ...] = _SUPPORTED_PROPERTY_TYPES

    # ------------------------------------------------------------------
    # Public secret-scan helpers (mirrors ScriptMappingBuilder shape).
    # ------------------------------------------------------------------

    @classmethod
    def scan_forbidden_secret_fields(
        cls, config: Any, _path_prefix: str = ""
    ) -> Optional[BuilderValidationError]:
        return _scan_forbidden_secret_fields(config, _path_prefix=_path_prefix)

    @classmethod
    def redact_forbidden_secret_fields_in_place(cls, config: Any) -> None:
        if isinstance(config, dict):
            for key in cls.FORBIDDEN_SECRET_FIELDS:
                if key in config:
                    config[key] = "[REDACTED]"
            for value in config.values():
                cls.redact_forbidden_secret_fields_in_place(value)
        elif isinstance(config, list):
            for item in config:
                cls.redact_forbidden_secret_fields_in_place(item)

    # ------------------------------------------------------------------
    # Validation
    # ------------------------------------------------------------------

    @classmethod
    def validate_config(
        cls, config: Dict[str, Any]
    ) -> Optional[BuilderValidationError]:
        """Validate a processproperty config; return None on success."""
        # 1. Secret-shaped key scan (deep).
        secret_err = cls.scan_forbidden_secret_fields(config)
        if secret_err is not None:
            return secret_err

        # 2. Unknown top-level keys (raw-XML subtree smuggling gets the
        # dedicated error code so the hint can point at the escape hatch).
        for key in config.keys():
            if key in _ALLOWED_TOP_LEVEL_KEYS:
                continue
            if key in ("defined_process_properties", "object", "bns_object"):
                return BuilderValidationError(
                    f"{key!r} is not accepted — the builder owns the "
                    "DefinedProcessProperties subtree.",
                    error_code=PROCESS_PROPERTY_RAW_XML_UNSUPPORTED,
                    field=key,
                    hint=(
                        "Declare 'properties' entries instead of raw XML. The "
                        "raw-XML escape hatch is config['xml'], handled "
                        "upstream of this builder."
                    ),
                )
            return BuilderValidationError(
                f"unknown top-level field {key!r} for processproperty",
                error_code=PROCESS_PROPERTY_VALIDATION_FAILED,
                field=key,
                hint=f"Supported top-level keys: {sorted(_ALLOWED_TOP_LEVEL_KEYS)}.",
            )

        # 3. component_name.
        component_name = config.get("component_name")
        if not isinstance(component_name, str) or not component_name.strip():
            return BuilderValidationError(
                "component_name is required",
                error_code=PROCESS_PROPERTY_NAME_REQUIRED,
                field="component_name",
                hint="Provide a non-empty component_name string.",
            )

        # 4. properties list.
        properties = config.get("properties")
        if not isinstance(properties, list) or not properties:
            return BuilderValidationError(
                "properties must be a non-empty list of property definitions",
                error_code=PROCESS_PROPERTY_PROPERTY_REQUIRED,
                field="properties",
                hint=(
                    "Each entry is {key: <stable UUID>, name: <label>, type: "
                    f"{list(_SUPPORTED_PROPERTY_TYPES)}, default_value?, "
                    "help_text?, persisted?}."
                ),
            )

        seen_keys: Dict[str, str] = {}
        seen_names: Dict[str, str] = {}
        for index, entry in enumerate(properties):
            field_prefix = f"properties[{index}]"
            if not isinstance(entry, Mapping):
                return BuilderValidationError(
                    f"{field_prefix} must be a mapping object",
                    error_code=PROCESS_PROPERTY_VALIDATION_FAILED,
                    field=field_prefix,
                )
            for key in entry.keys():
                if key in _ALLOWED_PROPERTY_KEYS:
                    continue
                hint = _REJECTED_PROPERTY_KEY_HINTS.get(
                    key, f"Allowed property keys: {sorted(_ALLOWED_PROPERTY_KEYS)}."
                )
                return BuilderValidationError(
                    f"{field_prefix}.{key} is not supported",
                    error_code=PROCESS_PROPERTY_VALIDATION_FAILED,
                    field=f"{field_prefix}.{key}",
                    hint=hint,
                )

            # key — required explicit stable UUID (the v1 contract: the same
            # UUID is handed to the referencing map function's
            # process_property_key parameter).
            prop_key = entry.get("key")
            if not isinstance(prop_key, str) or not prop_key.strip():
                return BuilderValidationError(
                    f"{field_prefix}.key is required",
                    error_code=PROCESS_PROPERTY_KEY_REQUIRED,
                    field=f"{field_prefix}.key",
                    hint=(
                        "Supply an explicit stable UUID per property (v1 "
                        "contract) — the same value the referencing map "
                        "function passes as process_property_key."
                    ),
                )
            prop_key = prop_key.strip()
            if not _is_uuid(prop_key):
                return BuilderValidationError(
                    f"{field_prefix}.key must be a lowercase canonical UUID "
                    f"(got {prop_key!r})",
                    error_code=PROCESS_PROPERTY_KEY_INVALID,
                    field=f"{field_prefix}.key",
                    hint="Example: 0e89ebf1-cd46-46df-904e-94c7e7ade31e.",
                )
            if prop_key in seen_keys:
                return BuilderValidationError(
                    f"{field_prefix}.key duplicates {seen_keys[prop_key]}",
                    error_code=PROCESS_PROPERTY_DUPLICATE_KEY,
                    field=f"{field_prefix}.key",
                    hint="Every property key must be unique in the component.",
                )
            seen_keys[prop_key] = f"{field_prefix}.key"

            # name — required non-blank; becomes the <label>.
            name = entry.get("name")
            if not isinstance(name, str) or not name.strip():
                return BuilderValidationError(
                    f"{field_prefix}.name is required",
                    error_code=PROCESS_PROPERTY_NAME_REQUIRED,
                    field=f"{field_prefix}.name",
                    hint="The name becomes the property <label> (display name).",
                )
            name_key = name.strip()
            if name_key in seen_names:
                return BuilderValidationError(
                    f"{field_prefix}.name duplicates {seen_names[name_key]}",
                    error_code=PROCESS_PROPERTY_DUPLICATE_NAME,
                    field=f"{field_prefix}.name",
                    hint="Property labels must be unique in the component.",
                )
            seen_names[name_key] = f"{field_prefix}.name"

            # type — v1 allow-list.
            prop_type = entry.get("type")
            if (
                not isinstance(prop_type, str)
                or prop_type.strip() not in _SUPPORTED_PROPERTY_TYPES
            ):
                return BuilderValidationError(
                    f"{field_prefix}.type must be one of "
                    f"{_SUPPORTED_PROPERTY_TYPES} (got {prop_type!r})",
                    error_code=PROCESS_PROPERTY_TYPE_UNSUPPORTED,
                    field=f"{field_prefix}.type",
                    hint=(
                        "string/number/boolean are live-verified; date is "
                        "docs-corroborated. character/password have no "
                        "evidence as Defined Process Property types (#119)."
                    ),
                )

            # default_value / help_text — strings when provided.
            for str_key in ("default_value", "help_text"):
                value = entry.get(str_key)
                if value is not None and not isinstance(value, str):
                    return BuilderValidationError(
                        f"{field_prefix}.{str_key} must be a string when provided",
                        error_code=PROCESS_PROPERTY_DEFAULT_INVALID,
                        field=f"{field_prefix}.{str_key}",
                        hint="Pass a string (may be empty), or omit the key.",
                    )

            # persisted — bool when provided.
            persisted = entry.get("persisted")
            if persisted is not None and not isinstance(persisted, bool):
                return BuilderValidationError(
                    f"{field_prefix}.persisted must be a boolean when provided",
                    error_code=PROCESS_PROPERTY_VALIDATION_FAILED,
                    field=f"{field_prefix}.persisted",
                    hint="true persists the value at atom level; default false.",
                )

        return None

    # ------------------------------------------------------------------
    # XML emission
    # ------------------------------------------------------------------

    def build(self, **params: Any) -> str:
        """Emit the ``<bns:Component type='processproperty'>`` XML."""
        config = dict(params)
        validation_err = self.validate_config(config)
        if validation_err is not None:
            raise validation_err

        component_name = str(config["component_name"]).strip()
        folder_path = config.get("folder_path")
        description = config.get("description") or ""

        property_parts: List[str] = []
        for entry in config["properties"]:
            prop_key = str(entry["key"]).strip()
            label = str(entry["name"]).strip()
            prop_type = str(entry["type"]).strip()
            default_value = entry.get("default_value") or ""
            help_text = entry.get("help_text") or ""
            persisted = "true" if bool(entry.get("persisted", False)) else "false"
            property_parts.append(
                f'<definedProcessProperty key="{_escape_xml(prop_key)}">'
                f"<helpText>{_escape_xml(help_text)}</helpText>"
                f"<label>{_escape_xml(label)}</label>"
                f"<type>{_escape_xml(prop_type)}</type>"
                f"<defaultValue>{_escape_xml(default_value)}</defaultValue>"
                "<allowedValues/>"
                f"<persisted>{persisted}</persisted>"
                "</definedProcessProperty>"
            )

        folder_attr = (
            f' folderFullPath="{_escape_xml(str(folder_path))}"'
            if folder_path
            else ""
        )
        return (
            '<bns:Component xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" '
            'xmlns:bns="http://api.platform.boomi.com/" '
            f'type="processproperty"{folder_attr} '
            f'name="{_escape_xml(component_name)}">'
            "<bns:encryptedValues/>"
            f"<bns:description>{_escape_xml(description)}</bns:description>"
            "<bns:object>"
            '<DefinedProcessProperties xmlns="">'
            f"{''.join(property_parts)}"
            "</DefinedProcessProperties>"
            "</bns:object>"
            "</bns:Component>"
        )


# Component-type registry — single-key dispatch, same shape as
# SCRIPT_MAPPING_BUILDERS (no sub-protocol dimension).
PROCESS_PROPERTY_BUILDERS: Dict[str, type] = {
    "processproperty": ProcessPropertyBuilder,
}


# Update-preservation policy: the builder owns the entire
# `<DefinedProcessProperties>` subtree. bns:encryptedValues, processOverrides,
# and unknown bns:Component/bns:object siblings are preserved automatically.
ProcessPropertyBuilder.PRESERVATION_POLICY = PreservationPolicy(
    component_type="processproperty",
    owned_paths=(OwnedPath(path="bns:object/DefinedProcessProperties"),),
)


def get_process_property_builder(component_type: str) -> Optional[type]:
    """Return the builder class for ``component_type`` or ``None``."""
    return PROCESS_PROPERTY_BUILDERS.get(component_type)
