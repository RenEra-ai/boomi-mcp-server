"""Issue #41 — ScriptMappingBuilder.

Emits ``<bns:Component type="script.mapping">`` XML from a structured config
that wraps a caller-authored Boomi Map Script (Groovy 1 / Groovy 2 /
JavaScript) with declared ``<Input>`` / ``<Output>`` variables. The emitted
component is a reusable map-script primitive that ``MapScriptBuilder``
references from ``transform.map`` via a userdefined ``<FunctionStep>``.

Reference XML shape evidence (fetched 2026-05-26 for #41):

* work ``ece5ce1e-6294-46af-a580-b3e1e65324e6`` — "[Time 3E Reverse
  Sync] Map Function Script for WorkDate". Confirms ``<MappingScript>``
  envelope with ``language``/``preserveOrder``/``useCache`` attributes,
  child order ``<script>`` then ``<Input>`` then ``<Output>``, Output
  starting at ``index="2"`` immediately after a single Input at
  ``index="1"``.
* work ``974b7950-e0e7-405d-90c0-255297439c02`` — "[Time
  Submission] Clean Invalid XML Characters from String and Uppercase
  Check". Same envelope and child-attribute shape across a second
  example, confirming the rule is monotonic indexing across Inputs
  then Outputs (not a fixed Output offset).

Envelope shape:

.. code-block:: xml

    <bns:Component type="script.mapping" name="..." folderFullPath="...">
      <bns:encryptedValues/>
      <bns:description>...</bns:description>
      <bns:object>
        <MappingScript xmlns="" language="groovy2" preserveOrder="true"
                       useCache="true">
          <script>...XML-escaped caller-authored body, plain text...</script>
          <Input dataType="character" index="1" name="..."/>
          <Output index="2" name="..."/>
        </MappingScript>
      </bns:object>
    </bns:Component>

Key shape facts (from live XML — do not invent):

* The ``<MappingScript>`` element resets ``xmlns=""`` and carries exactly
  three attributes: ``language``, ``preserveOrder``, ``useCache``.
* Children appear in order: ``<script>``, then all ``<Input>`` entries,
  then all ``<Output>`` entries.
* ``<script>`` body is plain XML-escaped text (NOT CDATA). All
  caller-authored content routes through ``_escape_xml``.
* ``<Input>`` carries ``dataType``, ``index``, ``name`` (in that
  attribute order in the live exports).
* ``<Output>`` carries ``index`` and ``name`` only — **NO dataType**.
* Indexing rule (verified across both examples): inputs receive 1-based
  indexes 1..N; outputs continue monotonically at N+1..N+M. The first
  Output index is ``len(inputs) + 1``.
"""

from __future__ import annotations

import re
from typing import Any, Dict, List, Mapping, Optional, Tuple

from .connector_builder import BuilderValidationError, _escape_xml
from .profile_generation import (
    SCRIPT_MAPPING_BODY_REQUIRED,
    SCRIPT_MAPPING_LANGUAGE_UNSUPPORTED,
    SCRIPT_MAPPING_VALIDATION_FAILED,
    SCRIPT_MAPPING_VARIABLE_INVALID,
    UNSUPPORTED_TRANSFORM_ROUTE,
)


# Boomi script-runtime language values. Live ``script.mapping`` exports
# use the exact strings ``groovy2`` / ``groovy`` / ``javascript`` on the
# ``<MappingScript language="...">`` attribute, so we pass the caller's
# value through verbatim after the membership check.
_SUPPORTED_LANGUAGES: Tuple[str, ...] = ("groovy", "groovy2", "javascript")


# Per Boomi docs (Custom script inputs and outputs):
#   Character inputs are passed as empty strings for null or omitted
#   source values; date, integer, and float inputs can be null.
_SUPPORTED_INPUT_DATA_TYPES: Tuple[str, ...] = (
    "character",
    "date",
    "integer",
    "float",
)


# Language-safe variable identifier (matches Groovy + JavaScript intersection
# rules — start with letter/underscore, then alphanumerics/underscores).
_IDENTIFIER_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


# Secret-shaped key names that script.mapping configs must never carry.
# Mirrors the set rejected by ``map_builder.DirectMapBuilder.FORBIDDEN_SECRET_FIELDS``
# so a single audit covers every map-family builder. ``script_body`` may
# legitimately contain credential-looking string content (the body is
# opaque caller-authored code), so the scan checks dict KEYS only — same
# semantics as the map-side helper.
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


# Raw-XML escape-hatch keys that must reject inside a structured config.
# The legitimate raw-XML path is ``config["xml"]`` which is handled by the
# integration builder upstream (``script.mapping`` apply branch skips the
# builder when ``payload["xml"]`` is set).
_RAW_XML_REJECT_KEYS: Dict[str, str] = {
    "functions": "Raw <Functions> XML is not accepted; supply structured inputs/outputs instead.",
    "function_steps": "Raw <FunctionStep> XML is not accepted; supply structured inputs/outputs instead.",
    "scripts": "Raw <scripts> XML is not accepted; script.mapping accepts a single script_body string.",
    "xslt": "XSLT is unrelated to script.mapping (XSLT remains future work, #42).",
    "xslt_source": "XSLT is unrelated to script.mapping (XSLT remains future work, #42).",
    "expression": "Boomi expressions are not a script.mapping primitive; place expression logic in script_body.",
    "expressions": "Boomi expressions are not a script.mapping primitive; place expression logic in script_body.",
}


# Top-level config keys this builder accepts. Anything else triggers
# SCRIPT_MAPPING_VALIDATION_FAILED with a hint listing the supported set.
# ``component_type`` is allowed because the integration builder threads it
# in alongside the rest of the payload; the builder ignores its content
# and the dispatcher upstream is what actually selects this builder.
_ALLOWED_TOP_LEVEL_KEYS: Tuple[str, ...] = (
    "component_type",
    "component_name",
    "folder_path",
    "description",
    "language",
    "script_body",
    "inputs",
    "outputs",
    "preserve_order",
    "use_cache",
    # ``xml`` is the raw-XML escape hatch handled upstream — never reach
    # this builder, but allow-list it so a stray pass-through doesn't
    # trigger SCRIPT_MAPPING_VALIDATION_FAILED.
    "xml",
)


def _scan_forbidden_secret_fields(
    config: Any, _path_prefix: str = ""
) -> Optional[BuilderValidationError]:
    """Recursive secret-shaped key scan (dict keys only)."""
    if isinstance(config, dict):
        for key in _FORBIDDEN_SECRET_FIELDS:
            if key in config:
                field_path = f"{_path_prefix}{key}"
                return BuilderValidationError(
                    f"{field_path!r} cannot be supplied in a script.mapping "
                    "config — script bodies must not transport credentials.",
                    error_code="PLAINTEXT_SECRET_REJECTED",
                    field=field_path,
                    hint=(
                        "Remove the secret-shaped field. Scripts should "
                        "reference credentials via process-property / "
                        "extension lookups at runtime, not embed them in "
                        "the component config."
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


def _validate_identifier(
    raw: Any, field_path: str
) -> Tuple[Optional[str], Optional[BuilderValidationError]]:
    """Return ``(stripped_value, None)`` on success or ``(None, error)``."""
    if not isinstance(raw, str) or not raw.strip():
        return None, BuilderValidationError(
            f"{field_path} must be a non-blank string",
            error_code=SCRIPT_MAPPING_VARIABLE_INVALID,
            field=field_path,
        )
    stripped = raw.strip()
    if not _IDENTIFIER_RE.match(stripped):
        return None, BuilderValidationError(
            f"{field_path} must be a language-safe identifier "
            f"(got {stripped!r})",
            error_code=SCRIPT_MAPPING_VARIABLE_INVALID,
            field=field_path,
            hint=(
                "Variable names must start with a letter or underscore "
                "and contain only letters, digits, and underscores so "
                "they bind cleanly to Groovy / JavaScript script bodies."
            ),
        )
    return stripped, None


class ScriptMappingBuilder:
    """Emit ``script.mapping`` component XML from structured config."""

    SUPPORTED_COMPONENT_TYPES: Tuple[str, ...] = ("script.mapping",)
    FORBIDDEN_SECRET_FIELDS: Tuple[str, ...] = _FORBIDDEN_SECRET_FIELDS

    # ------------------------------------------------------------------
    # Public secret-scan helpers (mirrors DirectMapBuilder shape).
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
        """Validate a script.mapping config; return None on success."""
        # 1. Secret-shaped key scan (deep).
        secret_err = cls.scan_forbidden_secret_fields(config)
        if secret_err is not None:
            return secret_err

        # 2. Raw-XML / unsupported escape-hatch rejection.
        for key, hint in _RAW_XML_REJECT_KEYS.items():
            if key in config:
                return BuilderValidationError(
                    f"{key!r} is not supported by ScriptMappingBuilder",
                    error_code=UNSUPPORTED_TRANSFORM_ROUTE,
                    field=key,
                    hint=hint,
                    details={"unsupported_route": key},
                )

        # 3. Unknown top-level keys.
        for key in config.keys():
            if key not in _ALLOWED_TOP_LEVEL_KEYS:
                return BuilderValidationError(
                    f"unknown top-level field {key!r} for script.mapping",
                    error_code=SCRIPT_MAPPING_VALIDATION_FAILED,
                    field=key,
                    hint=(
                        "Supported top-level keys: "
                        f"{sorted(_ALLOWED_TOP_LEVEL_KEYS)}."
                    ),
                )

        # 4. component_name.
        component_name = config.get("component_name")
        if not isinstance(component_name, str) or not component_name.strip():
            return BuilderValidationError(
                "component_name is required",
                error_code=SCRIPT_MAPPING_VALIDATION_FAILED,
                field="component_name",
                hint="Provide a non-empty component_name string.",
            )

        # 5. language.
        language = config.get("language")
        if not isinstance(language, str) or language.strip() not in _SUPPORTED_LANGUAGES:
            return BuilderValidationError(
                f"language must be one of {_SUPPORTED_LANGUAGES} "
                f"(got {language!r})",
                error_code=SCRIPT_MAPPING_LANGUAGE_UNSUPPORTED,
                field="language",
                hint=(
                    "Use 'groovy2' for Boomi Groovy 2 (recommended), "
                    "'groovy' for legacy Groovy 1, or 'javascript' for "
                    "the Boomi JavaScript runtime."
                ),
                details={
                    "language": language,
                    "supported": list(_SUPPORTED_LANGUAGES),
                },
            )

        # 6. script_body — present, string, non-blank.
        script_body = config.get("script_body")
        if not isinstance(script_body, str) or not script_body.strip():
            return BuilderValidationError(
                "script_body must be a non-blank caller-authored string",
                error_code=SCRIPT_MAPPING_BODY_REQUIRED,
                field="script_body",
                hint=(
                    "Provide the script source as a string. Bodies are "
                    "XML-escaped and emitted verbatim — Boomi runs them "
                    "in the chosen language runtime."
                ),
            )

        # 7. inputs.
        inputs_raw = config.get("inputs")
        if not isinstance(inputs_raw, list) or not inputs_raw:
            return BuilderValidationError(
                "inputs must be a non-empty list of {name, data_type} entries",
                error_code=SCRIPT_MAPPING_VALIDATION_FAILED,
                field="inputs",
                hint=(
                    "Declare at least one input. Boomi sets the mapped "
                    "input values before the script runs."
                ),
            )
        seen_names: Dict[str, str] = {}
        for index, entry in enumerate(inputs_raw):
            field_prefix = f"inputs[{index}]"
            if not isinstance(entry, Mapping):
                return BuilderValidationError(
                    f"{field_prefix} must be a mapping object",
                    error_code=SCRIPT_MAPPING_VALIDATION_FAILED,
                    field=field_prefix,
                )
            name, ident_err = _validate_identifier(
                entry.get("name"), f"{field_prefix}.name"
            )
            if ident_err is not None:
                return ident_err
            if name in seen_names:
                return BuilderValidationError(
                    f"{field_prefix}.name duplicates an earlier input/output "
                    f"variable {name!r}",
                    error_code=SCRIPT_MAPPING_VARIABLE_INVALID,
                    field=f"{field_prefix}.name",
                    hint=(
                        "Input and output variable names share one "
                        "namespace inside the script. Choose unique names."
                    ),
                    details={
                        "name": name,
                        "first_seen": seen_names[name],
                        "duplicate_seen": field_prefix,
                    },
                )
            seen_names[name] = field_prefix
            data_type = entry.get("data_type")
            if not isinstance(data_type, str) or data_type.strip() not in _SUPPORTED_INPUT_DATA_TYPES:
                return BuilderValidationError(
                    f"{field_prefix}.data_type must be one of "
                    f"{_SUPPORTED_INPUT_DATA_TYPES} (got {data_type!r})",
                    error_code=SCRIPT_MAPPING_VARIABLE_INVALID,
                    field=f"{field_prefix}.data_type",
                    hint=(
                        "Boomi sets mapped input values before script "
                        "execution; character/date/integer/float are the "
                        "documented data_type values."
                    ),
                )

        # 8. outputs.
        outputs_raw = config.get("outputs")
        if not isinstance(outputs_raw, list) or not outputs_raw:
            return BuilderValidationError(
                "outputs must be a non-empty list of {name} entries",
                error_code=SCRIPT_MAPPING_VALIDATION_FAILED,
                field="outputs",
                hint=(
                    "Declare at least one output. The script is "
                    "responsible for assigning each output variable's "
                    "value before returning."
                ),
            )
        for index, entry in enumerate(outputs_raw):
            field_prefix = f"outputs[{index}]"
            if not isinstance(entry, Mapping):
                return BuilderValidationError(
                    f"{field_prefix} must be a mapping object",
                    error_code=SCRIPT_MAPPING_VALIDATION_FAILED,
                    field=field_prefix,
                )
            # Output entries must NOT carry data_type (Boomi infers the
            # output type from the value assigned by the script). Reject
            # so authors don't ship dead config that quietly diverges
            # from live XML.
            if "data_type" in entry:
                return BuilderValidationError(
                    f"{field_prefix}.data_type is not supported (output "
                    "type is inferred from the value assigned by the "
                    "script at run-time)",
                    error_code=SCRIPT_MAPPING_VARIABLE_INVALID,
                    field=f"{field_prefix}.data_type",
                )
            name, ident_err = _validate_identifier(
                entry.get("name"), f"{field_prefix}.name"
            )
            if ident_err is not None:
                return ident_err
            if name in seen_names:
                return BuilderValidationError(
                    f"{field_prefix}.name duplicates an earlier input/output "
                    f"variable {name!r}",
                    error_code=SCRIPT_MAPPING_VARIABLE_INVALID,
                    field=f"{field_prefix}.name",
                    hint=(
                        "Input and output variable names share one "
                        "namespace inside the script. Choose unique names."
                    ),
                    details={
                        "name": name,
                        "first_seen": seen_names[name],
                        "duplicate_seen": field_prefix,
                    },
                )
            seen_names[name] = field_prefix

        # 9. preserve_order / use_cache type-check.
        for key in ("preserve_order", "use_cache"):
            if key in config and not isinstance(config[key], bool):
                return BuilderValidationError(
                    f"{key} must be a boolean (got {type(config[key]).__name__})",
                    error_code=SCRIPT_MAPPING_VALIDATION_FAILED,
                    field=key,
                )

        return None

    # ------------------------------------------------------------------
    # XML emission
    # ------------------------------------------------------------------

    def build(self, **params: Any) -> str:
        """Emit the wrapped ``<bns:Component type='script.mapping'>`` XML."""
        config = dict(params)
        validation_err = self.validate_config(config)
        if validation_err is not None:
            raise validation_err

        component_name = str(config["component_name"]).strip()
        folder_path = config.get("folder_path")
        description = config.get("description") or ""
        language = config["language"].strip()
        script_body = config["script_body"]
        preserve_order = bool(config.get("preserve_order", True))
        use_cache = bool(config.get("use_cache", True))

        # Collect (name, data_type) pairs for inputs and names for outputs
        # in declaration order. Validation already normalized everything.
        inputs: List[Tuple[str, str]] = [
            (str(entry["name"]).strip(), str(entry["data_type"]).strip())
            for entry in config["inputs"]
        ]
        outputs: List[str] = [
            str(entry["name"]).strip() for entry in config["outputs"]
        ]

        input_xml_parts: List[str] = []
        for index, (name, data_type) in enumerate(inputs, start=1):
            input_xml_parts.append(
                f'<Input dataType="{data_type}" index="{index}" '
                f'name="{_escape_xml(name)}"/>'
            )

        output_xml_parts: List[str] = []
        for index, name in enumerate(outputs, start=len(inputs) + 1):
            output_xml_parts.append(
                f'<Output index="{index}" name="{_escape_xml(name)}"/>'
            )

        body_xml = (
            f'<MappingScript xmlns="" language="{language}" '
            f'preserveOrder="{"true" if preserve_order else "false"}" '
            f'useCache="{"true" if use_cache else "false"}">'
            f"<script>{_escape_xml(script_body)}</script>"
            f"{''.join(input_xml_parts)}"
            f"{''.join(output_xml_parts)}"
            "</MappingScript>"
        )

        folder_attr = (
            f' folderFullPath="{_escape_xml(str(folder_path))}"'
            if folder_path
            else ""
        )
        return (
            '<bns:Component xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" '
            'xmlns:bns="http://api.platform.boomi.com/" '
            f'type="script.mapping"{folder_attr} '
            f'name="{_escape_xml(component_name)}">'
            "<bns:encryptedValues/>"
            f"<bns:description>{_escape_xml(description)}</bns:description>"
            "<bns:object>"
            f"{body_xml}"
            "</bns:object>"
            "</bns:Component>"
        )


# Component-type registry — mirrors the per-(component_type, sub_protocol)
# tuple keys used by ``MAP_BUILDERS`` for transform.map. ``script.mapping``
# has no sub-protocol so the key is just the component type.
SCRIPT_MAPPING_BUILDERS: Dict[str, type] = {
    "script.mapping": ScriptMappingBuilder,
}


def get_script_mapping_builder(component_type: str) -> Optional[type]:
    """Return the builder class for ``component_type`` or ``None``."""
    return SCRIPT_MAPPING_BUILDERS.get(component_type)
