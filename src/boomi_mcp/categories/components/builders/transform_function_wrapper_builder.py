"""Issue #41 — TransformFunctionWrapperBuilder.

Emits ``<bns:Component type="transform.function">`` XML for the reusable
function wrapper component that bridges a ``transform.map`` to a
``script.mapping``. Boomi requires this wrapper indirection: a map's
userdefined ``<FunctionStep id="...">`` MUST reference a ``transform.function``
component, never a ``script.mapping`` directly. The transform.function
internally references the script.mapping via ``<Configuration><Scripting
componentId="...">``.

Reference XML shape evidence (fetched 2026-05-26 for #41 r3 fix):

* work ``b8eaeeba-0417-4d22-923e-bd99ea0bac0b`` — "[Intapp Time 3E Reverse
  Sync] Workdate Strip Time Component from DateTime-Assumed YYYY-MM-DD
  Format". Confirms the full ``transform.function`` envelope shape:
  external ``<Inputs>``/``<Outputs>``, a ``<Steps>`` block with one inner
  ``<FunctionStep category="Scripting" type="Scripting">`` carrying
  ``<Configuration><Scripting componentId="..." useComponent="true">``
  with inline ``<ScriptToExecute>`` plus duplicated Input/Output
  declarations, and a ``<Mappings>`` block wiring editor (function=0)
  ports to the inner Scripting step (function=1) ports.

Envelope shape:

.. code-block:: xml

    <bns:Component type="transform.function" name="..." folderFullPath="...">
      <bns:encryptedValues/>
      <bns:description>...</bns:description>
      <bns:object>
        <Function xmlns="">
          <Inputs>
            <Input key="1" name="<<outer input name>>"/>
            ...
          </Inputs>
          <Outputs>
            <Output key="1" name="<<outer output name>>"/>
            ...
          </Outputs>
          <Steps>
            <FunctionStep cacheEnabled="true" cacheOption="map"
                          category="Scripting" key="1" name="Scripting"
                          position="1" sumEnabled="false" type="Scripting"
                          x="183.0" y="364.0">
              <Inputs>
                <Input key="1" name="<<script input name>>"/>
                ...
              </Inputs>
              <Outputs>
                <Output key="N+1" name="<<script output name>>"/>
                ...
              </Outputs>
              <Configuration>
                <Scripting componentId="<<script.mapping uuid>>"
                           language="groovy2" preserveOrder="true"
                           useCache="true" useComponent="true">
                  <ScriptToExecute>...</ScriptToExecute>
                  <Input dataType="character" index="1"
                         name="<<script input name>>"/>
                  ...
                  <Output index="N+1" name="<<script output name>>"/>
                  ...
                </Scripting>
              </Configuration>
            </FunctionStep>
          </Steps>
          <Mappings>
            <Mapping fromFunction="1" fromKey="<<script output key>>"
                     fromNamePath="Scripting/<<output name>>"
                     fromType="function" toFunction="0"
                     toKey="<<outer output key>>"
                     toNamePath="Editor/<<outer output name>>"
                     toType="function"/>
            <Mapping fromFunction="0" fromKey="<<outer input key>>"
                     fromNamePath="Editor/<<outer input name>>"
                     fromType="function" toFunction="1"
                     toKey="<<script input key>>"
                     toNamePath="Scripting/<<script input name>>"
                     toType="function"/>
          </Mappings>
        </Function>
      </bns:object>
    </bns:Component>

Key shape facts (from live XML — do not invent):

* The outer ``<Function>`` element resets ``xmlns=""``.
* External Input/Output ``key`` attributes are 1-based within their port
  list (independent input + output port spaces).
* The inner Scripting ``<FunctionStep>`` carries ``cacheEnabled="true"
  cacheOption="map"`` and ``category="Scripting" type="Scripting"``.
* The inner FunctionStep's outer-shell Inputs/Outputs use ``key=``
  (1-based per port list). The inner Scripting's ``<Input>``/``<Output>``
  elements use ``index=`` continuing monotonically: inputs 1..N, then
  outputs N+1..N+M (mirroring the script.mapping component's own indexing
  rule).
* ``<Scripting useComponent="true">`` directs Boomi to use the referenced
  script.mapping component at runtime, with the inline ``<ScriptToExecute>``
  serving as a cached / version-pinned snapshot.
* ``<Mappings>`` wires the editor (virtual ``function=0``) to the inner
  Scripting step (``function=1``). Two mappings per input/output pair:
  editor input → scripting input, scripting output → editor output.
* ``toNamePath`` / ``fromNamePath`` use the form ``Editor/<name>`` and
  ``Scripting/<name>``.

The wrapper is materialised automatically by the integration builder's
plan-time pass when a ``transform.map`` (``map_type='script'``) references a
``script.mapping`` via ``$ref:KEY``. End users do not author transform.function
wrappers directly through this builder; the schema template surface
treats it as an internal synthesis primitive.
"""

from __future__ import annotations

from typing import Any, Dict, List, Mapping, Optional, Tuple

from ._preservation_policy import OwnedPath, PreservationPolicy
from .connector_builder import BuilderValidationError, _escape_xml
from .profile_generation import (
    SCRIPT_MAPPING_BODY_REQUIRED,
    SCRIPT_MAPPING_LANGUAGE_UNSUPPORTED,
    SCRIPT_MAPPING_VALIDATION_FAILED,
    SCRIPT_MAPPING_VARIABLE_INVALID,
)


_SUPPORTED_LANGUAGES: Tuple[str, ...] = ("groovy", "groovy2", "javascript")
_SUPPORTED_INPUT_DATA_TYPES: Tuple[str, ...] = (
    "character",
    "date",
    "integer",
    "float",
)


_ALLOWED_TOP_LEVEL_KEYS: Tuple[str, ...] = (
    "component_type",
    "component_name",
    "folder_path",
    "description",
    # The script.mapping the wrapper references at runtime.
    "script_component_id",
    # Mirrored from the referenced script.mapping at synthesis time.
    "language",
    "preserve_order",
    "use_cache",
    "script_body",
    "inputs",
    "outputs",
    "xml",
)


# Same secret-shaped key set used by the map / script builders so the
# integration builder's two-tier scan covers every plan-time entry point.
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


def _scan_forbidden_secret_fields(
    config: Any, _path_prefix: str = ""
) -> Optional[BuilderValidationError]:
    """Recursive secret-shaped key scan (dict keys only)."""
    if isinstance(config, dict):
        for key in _FORBIDDEN_SECRET_FIELDS:
            if key in config:
                field_path = f"{_path_prefix}{key}"
                return BuilderValidationError(
                    f"{field_path!r} cannot be supplied in a "
                    "transform.function wrapper config.",
                    error_code="PLAINTEXT_SECRET_REJECTED",
                    field=field_path,
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


class TransformFunctionWrapperBuilder:
    """Emit ``transform.function`` wrapper XML from structured config."""

    SUPPORTED_COMPONENT_TYPES: Tuple[str, ...] = ("transform.function",)
    FORBIDDEN_SECRET_FIELDS: Tuple[str, ...] = _FORBIDDEN_SECRET_FIELDS

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

    @classmethod
    def validate_config(
        cls, config: Dict[str, Any]
    ) -> Optional[BuilderValidationError]:
        """Validate a transform.function wrapper config.

        Mirrors the pattern used by DirectMapBuilder / MapScriptBuilder:
        ``$ref:KEY`` tokens for ``script_component_id`` are accepted at
        plan time (integration builder resolves them before invoking
        ``build()``), but ``build()`` rejects unresolved refs as
        ``MAP_PROFILE_INDEX_UNAVAILABLE``.
        """
        # Unknown top-level keys — defence-in-depth against silent drops.
        for key in config.keys():
            if key not in _ALLOWED_TOP_LEVEL_KEYS:
                return BuilderValidationError(
                    f"unknown top-level field {key!r} for transform.function",
                    error_code=SCRIPT_MAPPING_VALIDATION_FAILED,
                    field=key,
                    hint=(
                        "Supported top-level keys: "
                        f"{sorted(_ALLOWED_TOP_LEVEL_KEYS)}."
                    ),
                )

        component_name = config.get("component_name")
        if not isinstance(component_name, str) or not component_name.strip():
            return BuilderValidationError(
                "component_name is required",
                error_code=SCRIPT_MAPPING_VALIDATION_FAILED,
                field="component_name",
                hint="Provide a non-empty component_name string.",
            )

        script_component_id = config.get("script_component_id")
        if not isinstance(script_component_id, str) or not script_component_id.strip():
            return BuilderValidationError(
                "script_component_id is required",
                error_code=SCRIPT_MAPPING_VALIDATION_FAILED,
                field="script_component_id",
                hint=(
                    "Provide a '$ref:KEY' pointing at an in-spec "
                    "script.mapping component, or the resolved Boomi UUID."
                ),
            )

        language = config.get("language")
        if not isinstance(language, str) or language.strip() not in _SUPPORTED_LANGUAGES:
            return BuilderValidationError(
                f"language must be one of {_SUPPORTED_LANGUAGES} "
                f"(got {language!r})",
                error_code=SCRIPT_MAPPING_LANGUAGE_UNSUPPORTED,
                field="language",
                details={
                    "language": language,
                    "supported": list(_SUPPORTED_LANGUAGES),
                },
            )

        script_body = config.get("script_body")
        if not isinstance(script_body, str) or not script_body.strip():
            return BuilderValidationError(
                "script_body must be a non-blank caller-authored string",
                error_code=SCRIPT_MAPPING_BODY_REQUIRED,
                field="script_body",
                hint=(
                    "The wrapper carries an inline ScriptToExecute snapshot "
                    "of the referenced script.mapping's body. Boomi uses "
                    "the referenced component at runtime when "
                    "useComponent='true', but the inline body is the "
                    "version-pinned cache."
                ),
            )

        inputs_raw = config.get("inputs")
        if not isinstance(inputs_raw, list) or not inputs_raw:
            return BuilderValidationError(
                "inputs must be a non-empty list of {name, data_type}",
                error_code=SCRIPT_MAPPING_VALIDATION_FAILED,
                field="inputs",
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
            name = entry.get("name")
            if not isinstance(name, str) or not name.strip():
                return BuilderValidationError(
                    f"{field_prefix}.name must be a non-blank string",
                    error_code=SCRIPT_MAPPING_VARIABLE_INVALID,
                    field=f"{field_prefix}.name",
                )
            name = name.strip()
            if name in seen_names:
                return BuilderValidationError(
                    f"{field_prefix}.name duplicates {name!r}",
                    error_code=SCRIPT_MAPPING_VARIABLE_INVALID,
                    field=f"{field_prefix}.name",
                )
            seen_names[name] = field_prefix
            data_type = entry.get("data_type")
            if not isinstance(data_type, str) or data_type.strip() not in _SUPPORTED_INPUT_DATA_TYPES:
                return BuilderValidationError(
                    f"{field_prefix}.data_type must be one of "
                    f"{_SUPPORTED_INPUT_DATA_TYPES} (got {data_type!r})",
                    error_code=SCRIPT_MAPPING_VARIABLE_INVALID,
                    field=f"{field_prefix}.data_type",
                )

        outputs_raw = config.get("outputs")
        if not isinstance(outputs_raw, list) or not outputs_raw:
            return BuilderValidationError(
                "outputs must be a non-empty list of {name}",
                error_code=SCRIPT_MAPPING_VALIDATION_FAILED,
                field="outputs",
            )
        for index, entry in enumerate(outputs_raw):
            field_prefix = f"outputs[{index}]"
            if not isinstance(entry, Mapping):
                return BuilderValidationError(
                    f"{field_prefix} must be a mapping object",
                    error_code=SCRIPT_MAPPING_VALIDATION_FAILED,
                    field=field_prefix,
                )
            if "data_type" in entry:
                return BuilderValidationError(
                    f"{field_prefix}.data_type is not accepted "
                    "(script-mapping outputs infer type at runtime)",
                    error_code=SCRIPT_MAPPING_VARIABLE_INVALID,
                    field=f"{field_prefix}.data_type",
                )
            name = entry.get("name")
            if not isinstance(name, str) or not name.strip():
                return BuilderValidationError(
                    f"{field_prefix}.name must be a non-blank string",
                    error_code=SCRIPT_MAPPING_VARIABLE_INVALID,
                    field=f"{field_prefix}.name",
                )
            name = name.strip()
            if name in seen_names:
                return BuilderValidationError(
                    f"{field_prefix}.name duplicates {name!r}",
                    error_code=SCRIPT_MAPPING_VARIABLE_INVALID,
                    field=f"{field_prefix}.name",
                )
            seen_names[name] = field_prefix

        for key in ("preserve_order", "use_cache"):
            if key in config and not isinstance(config[key], bool):
                return BuilderValidationError(
                    f"{key} must be a boolean",
                    error_code=SCRIPT_MAPPING_VALIDATION_FAILED,
                    field=key,
                )

        return None

    def build(self, **params: Any) -> str:
        """Emit the wrapped ``<bns:Component type='transform.function'>`` XML."""
        config = dict(params)
        validation_err = self.validate_config(config)
        if validation_err is not None:
            raise validation_err

        component_name = str(config["component_name"]).strip()
        folder_path = config.get("folder_path")
        description = config.get("description") or ""
        script_component_id = str(config["script_component_id"]).strip()
        if script_component_id.startswith("$ref:"):
            raise BuilderValidationError(
                "script_component_id must be resolved to a Boomi UUID "
                "before XML emission",
                error_code="MAP_PROFILE_INDEX_UNAVAILABLE",
                field="script_component_id",
                hint=(
                    "Integration builder must resolve '$ref:KEY' tokens "
                    "via _resolve_dependency_tokens before invoking build()."
                ),
            )
        language = config["language"].strip()
        preserve_order = bool(config.get("preserve_order", True))
        use_cache = bool(config.get("use_cache", True))
        script_body = config["script_body"]

        inputs: List[Tuple[str, str]] = [
            (str(entry["name"]).strip(), str(entry["data_type"]).strip())
            for entry in config["inputs"]
        ]
        outputs: List[str] = [
            str(entry["name"]).strip() for entry in config["outputs"]
        ]

        # 1. External Function ports (the wrapper's API surface — what the
        # calling map binds to). Live shape: 1-based per port list.
        outer_inputs_xml = "".join(
            f'<Input key="{i + 1}" name="{_escape_xml(name)}"/>'
            for i, (name, _dt) in enumerate(inputs)
        )
        outer_outputs_xml = "".join(
            f'<Output key="{i + 1}" name="{_escape_xml(name)}"/>'
            for i, name in enumerate(outputs)
        )

        # 2. Inner Scripting step ports. Outer-shell Inputs/Outputs use
        # 1-based ``key=`` per port list. Inner Scripting Input/Output use
        # the monotonic ``index=`` rule from script.mapping (1..N then
        # N+1..N+M). Outer-shell Output ``key=`` matches the corresponding
        # Scripting Output ``index=`` (live evidence: outer Output
        # key="2" with inner Output index="2").
        inner_step_inputs_xml = "".join(
            f'<Input key="{i + 1}" name="{_escape_xml(name)}"/>'
            for i, (name, _dt) in enumerate(inputs)
        )
        inner_step_outputs_xml = "".join(
            f'<Output key="{len(inputs) + i + 1}" name="{_escape_xml(name)}"/>'
            for i, name in enumerate(outputs)
        )

        # 3. Scripting Configuration: componentId reference + inline body
        # snapshot + variable declarations.
        scripting_inputs_xml = "".join(
            f'<Input dataType="{dt}" index="{i + 1}" name="{_escape_xml(name)}"/>'
            for i, (name, dt) in enumerate(inputs)
        )
        scripting_outputs_xml = "".join(
            f'<Output index="{len(inputs) + i + 1}" name="{_escape_xml(name)}"/>'
            for i, name in enumerate(outputs)
        )

        scripting_xml = (
            f'<Scripting componentId="{_escape_xml(script_component_id)}" '
            f'language="{language}" '
            f'preserveOrder="{"true" if preserve_order else "false"}" '
            f'useCache="{"true" if use_cache else "false"}" '
            f'useComponent="true">'
            f"<ScriptToExecute>{_escape_xml(script_body)}</ScriptToExecute>"
            f"{scripting_inputs_xml}"
            f"{scripting_outputs_xml}"
            "</Scripting>"
        )

        inner_step_xml = (
            '<FunctionStep cacheEnabled="true" cacheOption="map" '
            'category="Scripting" key="1" name="Scripting" position="1" '
            'sumEnabled="false" type="Scripting" x="183.0" y="364.0">'
            f"<Inputs>{inner_step_inputs_xml}</Inputs>"
            f"<Outputs>{inner_step_outputs_xml}</Outputs>"
            f"<Configuration>{scripting_xml}</Configuration>"
            "</FunctionStep>"
        )

        # 4. Editor↔Scripting wiring (function=0 = editor, function=1 =
        # Scripting step). One mapping per input + one per output:
        #   - editor input  (key=K) → scripting input  (key=K)
        #   - scripting output (key=N+M) → editor output (key=M)
        mappings_xml_parts: List[str] = []
        for i, (name, _dt) in enumerate(inputs):
            input_key = i + 1
            mappings_xml_parts.append(
                f'<Mapping fromFunction="0" fromKey="{input_key}" '
                f'fromNamePath="Editor/{_escape_xml(name)}" '
                f'fromType="function" '
                f'toFunction="1" toKey="{input_key}" '
                f'toNamePath="Scripting/{_escape_xml(name)}" '
                f'toType="function"/>'
            )
        for i, name in enumerate(outputs):
            outer_output_key = i + 1
            inner_output_key = len(inputs) + i + 1
            mappings_xml_parts.append(
                f'<Mapping fromFunction="1" fromKey="{inner_output_key}" '
                f'fromNamePath="Scripting/{_escape_xml(name)}" '
                f'fromType="function" '
                f'toFunction="0" toKey="{outer_output_key}" '
                f'toNamePath="Editor/{_escape_xml(name)}" '
                f'toType="function"/>'
            )

        function_body_xml = (
            '<Function xmlns="">'
            f"<Inputs>{outer_inputs_xml}</Inputs>"
            f"<Outputs>{outer_outputs_xml}</Outputs>"
            f"<Steps>{inner_step_xml}</Steps>"
            f"<Mappings>{''.join(mappings_xml_parts)}</Mappings>"
            "</Function>"
        )

        folder_attr = (
            f' folderFullPath="{_escape_xml(str(folder_path))}"'
            if folder_path
            else ""
        )
        return (
            '<bns:Component xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" '
            'xmlns:bns="http://api.platform.boomi.com/" '
            f'type="transform.function"{folder_attr} '
            f'name="{_escape_xml(component_name)}">'
            "<bns:encryptedValues/>"
            f"<bns:description>{_escape_xml(description)}</bns:description>"
            "<bns:object>"
            f"{function_body_xml}"
            "</bns:object>"
            "</bns:Component>"
        )


TRANSFORM_FUNCTION_WRAPPER_BUILDERS: Dict[str, type] = {
    "transform.function": TransformFunctionWrapperBuilder,
}


# Issue #45 — update-preservation policy. The wrapper builder owns the
# entire `<Function>` subtree (Inputs, Outputs, Steps, Mappings); everything
# else inside the component (encryptedValues, processOverrides, unknown
# bns:Component children) is preserved.
TransformFunctionWrapperBuilder.PRESERVATION_POLICY = PreservationPolicy(
    component_type="transform.function",
    owned_paths=(OwnedPath(path="bns:object/Function"),),
)


def get_transform_function_wrapper_builder(
    component_type: str,
) -> Optional[type]:
    """Return the wrapper builder for ``component_type`` or ``None``."""
    return TRANSFORM_FUNCTION_WRAPPER_BUILDERS.get(component_type)
