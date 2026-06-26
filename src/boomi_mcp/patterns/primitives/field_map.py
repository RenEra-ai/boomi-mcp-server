"""Issue #27: ``field_map`` transform primitive.

Compiles explicit, caller-authored transform intent into a target JSON
payload profile plus a ``transform.map`` component, choosing the map route
deterministically from the operation mix:

  * direct only                      -> ``transform.map`` map_type='direct'
  * any map_function, no map_script  -> map_type='function' (+ optional direct)
  * any map_script, no map_function  -> map_type='script'   (+ optional direct)
  * map_function AND map_script      -> rejected (UNSUPPORTED_TRANSFORM_ROUTE)

Inline ``map_script`` bodies are emitted as standalone ``script.mapping``
components referenced from the map by ``$ref`` token. The primitive emits
JSON component specs only; every byte of XML and all structured validation
is delegated to the existing JSON-profile / map / script builders. It never
falls back to ``script.processing``.
"""

from __future__ import annotations

from typing import Any, Dict, List, Literal, Optional

from pydantic import BaseModel, ConfigDict, Field, model_validator

from ...categories.components.builders.json_profile_builder import (
    JSONGeneratedProfileBuilder,
)
from ...categories.components.builders.map_builder import (
    DirectMapBuilder,
    MapFunctionBuilder,
    MapScriptBuilder,
)
from ...categories.components.builders.profile_generation import (
    SCRIPT_MAPPING_REF_REQUIRED,
    UNSUPPORTED_TRANSFORM_ROUTE,
)
from ...categories.components.builders.connector_builder import (
    BuilderValidationError,
)
from ...categories.components.builders.script_mapping_builder import (
    ScriptMappingBuilder,
)
from ...models.integration_models import IntegrationComponentSpec
from ..base import (
    PatternIOContract,
    PatternKind,
    PatternMetadata,
    PrimitiveBuildContext,
    PrimitivePattern,
)
from ._helpers import (
    ROLE_TARGET_PROFILE,
    ROLE_TRANSFORM_MAP,
    primitive_component_key,
    raise_for_builder_error,
    ref_key,
    script_slot_key,
    source_type_to_script_input_type,
)

_PROFILE_TYPE = Literal["profile.db", "profile.json", "profile.xml"]
_REF_PREFIX = "$ref:"


# ---------------------------------------------------------------------------
# Parameter models (strict)
# ---------------------------------------------------------------------------


class SourceBinding(BaseModel):
    """Source profile binding produced upstream (e.g. by db_extract)."""

    model_config = ConfigDict(extra="forbid")

    source_profile_id: str = Field(
        ..., description="'$ref:KEY' to an in-spec profile or a literal profile UUID"
    )
    source_profile_type: _PROFILE_TYPE
    source_field_index: Dict[str, Dict[str, Any]] = Field(
        ...,
        description="Per-leaf source index ({path: {data_type, mappable, ...}})",
    )


class DirectOp(BaseModel):
    model_config = ConfigDict(extra="forbid")

    source_field: str
    target_path: str


class MapFunctionOp(BaseModel):
    model_config = ConfigDict(extra="forbid")

    function_type: str
    inputs: List[str] = Field(default_factory=list)
    target_path: str
    parameters: Dict[str, Any] = Field(default_factory=dict)


class ScriptInput(BaseModel):
    model_config = ConfigDict(extra="forbid")

    source_path: str
    input_name: str


class ScriptOutput(BaseModel):
    model_config = ConfigDict(extra="forbid")

    output_name: str
    target_path: str


class MapScriptOp(BaseModel):
    """One in-map script call.

    Exactly one of ``script_body`` (inline — emits a script.mapping component)
    or ``script_component_ref`` (a ``$ref:KEY`` to an existing in-spec
    script.mapping/wrapper) must be supplied.
    """

    model_config = ConfigDict(extra="forbid")

    inputs: List[ScriptInput] = Field(..., min_length=1)
    outputs: List[ScriptOutput] = Field(..., min_length=1)
    language: Optional[str] = Field(default=None, description="Required with script_body")
    script_body: Optional[str] = Field(default=None)
    script_component_ref: Optional[str] = Field(
        default=None, description="'$ref:KEY' to an existing in-spec script component"
    )

    @model_validator(mode="after")
    def _require_one_script_source(self) -> "MapScriptOp":
        has_body = bool(self.script_body and self.script_body.strip())
        has_ref = self.script_component_ref is not None
        if has_body == has_ref:
            raise ValueError(
                "map_script op requires exactly one of script_body (inline) or "
                "script_component_ref ('$ref:KEY')"
            )
        if has_body and not (self.language and self.language.strip()):
            raise ValueError("language is required when script_body is provided")
        return self


class FieldMapComponentNames(BaseModel):
    model_config = ConfigDict(extra="forbid")

    target_profile: Optional[str] = None
    transform_map: Optional[str] = None
    script_prefix: Optional[str] = None


class FieldMapParameters(BaseModel):
    model_config = ConfigDict(extra="forbid")

    key_prefix: str = Field(..., description="Stable key prefix for deterministic keys")
    source: SourceBinding
    target_payload_profile: Dict[str, Any] = Field(
        ..., description="JSON profile tree ({format: 'json', root: {...}})"
    )
    direct: List[DirectOp] = Field(default_factory=list)
    map_function: List[MapFunctionOp] = Field(default_factory=list)
    map_script: List[MapScriptOp] = Field(default_factory=list)
    component_names: FieldMapComponentNames = Field(
        default_factory=FieldMapComponentNames
    )

    @model_validator(mode="after")
    def _require_at_least_one_operation(self) -> "FieldMapParameters":
        if not (self.direct or self.map_function or self.map_script):
            raise ValueError(
                "field_map requires at least one operation (direct, "
                "map_function, or map_script)"
            )
        return self


# ---------------------------------------------------------------------------
# Primitive
# ---------------------------------------------------------------------------


class FieldMapPrimitive(PrimitivePattern):
    """Compile transform intent into a target profile + transform.map."""

    metadata = PatternMetadata(
        name="field_map",
        version="1.0.0",
        kind=PatternKind.PRIMITIVE,
        description=(
            "Compile explicit direct / function / script transform operations "
            "into a generated JSON target profile and a transform.map "
            "component. The route is chosen deterministically; mixing function "
            "and script operations in one call is rejected."
        ),
        tags=["transform", "map", "field-map"],
        use_cases=[
            "Map database source fields to a JSON payload via direct copies",
            "Apply native map functions or reusable map scripts to fields",
        ],
        not_for=[
            "Mixing map functions and map scripts in one map (split them)",
            "Falling back to script.processing",
            "Profile discovery or schema inference",
        ],
    )
    parameters_model = FieldMapParameters

    input_contract = PatternIOContract(
        name="database_extract_result",
        description=(
            "Source profile binding and field index from an upstream "
            "profile-backed source (db_extract or rest_fetch)."
        ),
        profile_type="database",
        schema_={
            "type": "object",
            "required": [
                "source_profile_id",
                "source_profile_type",
                "source_field_index",
            ],
            "properties": {
                "source_profile_id": {"type": "string"},
                "source_profile_type": {"type": "string"},
                "source_field_index": {"type": "object"},
            },
        },
    )
    output_contract = PatternIOContract(
        name="mapped_payload",
        description="Generated JSON payload profile plus the transform.map that fills it.",
        profile_type="json",
        media_type="application/json",
        schema_={
            "type": "object",
            "properties": {
                "target_profile_key": {"type": "string"},
                "target_field_index": {"type": "object"},
                "map_key": {"type": "string"},
                "map_route": {"type": "string"},
            },
        },
    )
    required_builders = [
        "JSONGeneratedProfileBuilder",
        "DirectMapBuilder",
        "MapFunctionBuilder",
        "MapScriptBuilder",
        "ScriptMappingBuilder",
    ]

    @classmethod
    def emit_components(
        cls,
        context: PrimitiveBuildContext,
        parameters: BaseModel,
    ) -> List[IntegrationComponentSpec]:
        params: FieldMapParameters = parameters  # type: ignore[assignment]
        route = cls._select_route(params)
        folder = context.folder_path

        target_key = primitive_component_key(params.key_prefix, ROLE_TARGET_PROFILE)
        map_key = primitive_component_key(params.key_prefix, ROLE_TRANSFORM_MAP)

        target_profile, target_index = cls._emit_target_profile(
            context, params, target_key, folder
        )

        map_common: Dict[str, Any] = {
            "component_name": (
                params.component_names.transform_map
                or f"{context.component_prefix} Field Map"
            ),
            "source_profile_id": params.source.source_profile_id,
            "source_profile_type": params.source.source_profile_type,
            "target_profile_id": f"{_REF_PREFIX}{target_key}",
            "target_profile_type": "profile.json",
        }
        if folder:
            map_common["folder_path"] = folder
        source_index = params.source.source_field_index

        # When the source profile is referenced by $ref to an in-spec
        # component (e.g. db_extract's read profile), the map must depend on
        # it — build_integration rejects a map whose source/target $ref is
        # absent from depends_on (MAP_PROFILE_REF_REQUIRED). The target
        # profile $ref is always in-spec; the source may be a $ref or a
        # literal (literals are not in-spec keys and so are not dependencies).
        profile_deps = [target_key]
        source_ref = ref_key(params.source.source_profile_id)
        if source_ref:
            profile_deps.append(source_ref)

        if route == "direct":
            map_component = cls._emit_direct_map(
                map_common, params, source_index, target_index, map_key, profile_deps
            )
            return [target_profile, map_component]

        if route == "function":
            map_component = cls._emit_function_map(
                map_common, params, source_index, target_index, map_key, profile_deps
            )
            return [target_profile, map_component]

        # script route
        script_components, map_component = cls._emit_script_map(
            context, map_common, params, source_index, target_index, map_key, profile_deps, folder
        )
        return [target_profile, *script_components, map_component]

    # ------------------------------------------------------------------
    # Route selection
    # ------------------------------------------------------------------

    @staticmethod
    def _select_route(params: FieldMapParameters) -> str:
        has_function = bool(params.map_function)
        has_script = bool(params.map_script)
        if has_function and has_script:
            raise BuilderValidationError(
                "field_map cannot mix map_function and map_script operations "
                "in a single map",
                error_code=UNSUPPORTED_TRANSFORM_ROUTE,
                field="map_script",
                hint=(
                    "A Boomi transform.map uses one function route. Split the "
                    "function operations and the script operations into "
                    "separate field_map primitive calls (and separate maps)."
                ),
            )
        if has_function:
            return "function"
        if has_script:
            return "script"
        return "direct"

    # ------------------------------------------------------------------
    # Target profile
    # ------------------------------------------------------------------

    @classmethod
    def _emit_target_profile(
        cls,
        context: PrimitiveBuildContext,
        params: FieldMapParameters,
        target_key: str,
        folder: Optional[str],
    ):
        target_name = (
            params.component_names.target_profile
            or f"{context.component_prefix} Target Profile"
        )
        payload = params.target_payload_profile
        fmt = payload.get("format", "json")
        if fmt != "json":
            raise BuilderValidationError(
                f"target_payload_profile.format must be 'json' (got {fmt!r})",
                error_code="UNSUPPORTED_PROFILE_GENERATION_MODE",
                field="target_payload_profile.format",
                hint="field_map emits a json.generated target profile.",
            )
        config: Dict[str, Any] = {
            "profile_type": "json.generated",
            "component_name": target_name,
            "root": payload.get("root"),
        }
        if folder:
            config["folder_path"] = folder
        raise_for_builder_error(JSONGeneratedProfileBuilder.validate_config(config))
        target_index = JSONGeneratedProfileBuilder.build_field_index(config)
        component = IntegrationComponentSpec(
            key=target_key,
            type="profile.json",
            action="create",
            name=target_name,
            config=config,
        )
        return component, target_index

    # ------------------------------------------------------------------
    # Map routes
    # ------------------------------------------------------------------

    @classmethod
    def _emit_direct_map(
        cls,
        map_common: Dict[str, Any],
        params: FieldMapParameters,
        source_index: Dict[str, Dict[str, Any]],
        target_index: Dict[str, Dict[str, Any]],
        map_key: str,
        profile_deps: List[str],
    ) -> IntegrationComponentSpec:
        config = dict(map_common)
        config["map_type"] = "direct"
        config["field_mappings"] = cls._direct_field_mappings(params)
        raise_for_builder_error(
            DirectMapBuilder.validate_config(
                config, source_index=source_index, target_index=target_index
            )
        )
        return IntegrationComponentSpec(
            key=map_key,
            type="transform.map",
            action="create",
            name=config["component_name"],
            config=config,
            depends_on=list(profile_deps),
        )

    @classmethod
    def _emit_function_map(
        cls,
        map_common: Dict[str, Any],
        params: FieldMapParameters,
        source_index: Dict[str, Dict[str, Any]],
        target_index: Dict[str, Dict[str, Any]],
        map_key: str,
        profile_deps: List[str],
    ) -> IntegrationComponentSpec:
        config = dict(map_common)
        config["map_type"] = "function"
        config["function_mappings"] = [op.model_dump() for op in params.map_function]
        if params.direct:
            config["field_mappings"] = cls._direct_field_mappings(params)
        raise_for_builder_error(
            MapFunctionBuilder.validate_config(
                config, source_index=source_index, target_index=target_index
            )
        )
        return IntegrationComponentSpec(
            key=map_key,
            type="transform.map",
            action="create",
            name=config["component_name"],
            config=config,
            depends_on=list(profile_deps),
        )

    @classmethod
    def _emit_script_map(
        cls,
        context: PrimitiveBuildContext,
        map_common: Dict[str, Any],
        params: FieldMapParameters,
        source_index: Dict[str, Dict[str, Any]],
        target_index: Dict[str, Dict[str, Any]],
        map_key: str,
        profile_deps: List[str],
        folder: Optional[str],
    ):
        # 1) Resolve a script ref per op (inline -> emit later; external -> $ref).
        script_refs: List[str] = []
        inline_slots: List[tuple] = []  # (slot, op, script_key)
        slot = 0
        for op in params.map_script:
            if op.script_body and op.script_body.strip():
                script_key = script_slot_key(params.key_prefix, slot)
                script_refs.append(f"{_REF_PREFIX}{script_key}")
                inline_slots.append((slot, op, script_key))
                slot += 1
            else:
                ref = op.script_component_ref or ""
                if not ref.startswith(_REF_PREFIX) or not ref[len(_REF_PREFIX):].strip():
                    raise BuilderValidationError(
                        "script_component_ref must be a '$ref:KEY' token pointing "
                        "at an in-spec script.mapping or transform.function "
                        "component",
                        error_code=SCRIPT_MAPPING_REF_REQUIRED,
                        field="script_component_ref",
                        hint=(
                            "Literal component IDs are not accepted — the in-map "
                            "FunctionStep must reference a wrapper the integration "
                            "builder synthesizes from an in-spec component. Use "
                            "'$ref:<script_key>'."
                        ),
                    )
                script_refs.append(ref)

        # 2) Build the script_mappings entries (1:1 with map_script ops).
        script_mappings: List[Dict[str, Any]] = []
        for op, ref in zip(params.map_script, script_refs):
            script_mappings.append(
                {
                    "script_component_id": ref,
                    "inputs": [
                        {"source_path": i.source_path, "input_name": i.input_name}
                        for i in op.inputs
                    ],
                    "outputs": [
                        {"output_name": o.output_name, "target_path": o.target_path}
                        for o in op.outputs
                    ],
                }
            )

        config = dict(map_common)
        config["map_type"] = "script"
        config["script_mappings"] = script_mappings
        if params.direct:
            config["field_mappings"] = cls._direct_field_mappings(params)

        # 3) Validate the map (source/target paths, mappability, duplicates).
        raise_for_builder_error(
            MapScriptBuilder.validate_config(
                config, source_index=source_index, target_index=target_index
            )
        )

        # 4) Emit standalone script.mapping components for inline bodies. Paths
        #    are already validated above, so source-index lookups succeed.
        script_components: List[IntegrationComponentSpec] = []
        script_name_prefix = (
            params.component_names.script_prefix
            or f"{context.component_prefix} Map Script"
        )
        for emit_slot, op, script_key in inline_slots:
            script_inputs = []
            for i in op.inputs:
                src_entry = source_index.get(i.source_path) or {}
                data_type = source_type_to_script_input_type(src_entry.get("data_type"))
                script_inputs.append({"name": i.input_name, "data_type": data_type})
            script_outputs = [{"name": o.output_name} for o in op.outputs]
            script_config: Dict[str, Any] = {
                "component_type": "script.mapping",
                "component_name": f"{script_name_prefix} {emit_slot + 1}",
                "language": op.language,
                "script_body": op.script_body,
                "inputs": script_inputs,
                "outputs": script_outputs,
            }
            if folder:
                script_config["folder_path"] = folder
            raise_for_builder_error(ScriptMappingBuilder.validate_config(script_config))
            script_components.append(
                IntegrationComponentSpec(
                    key=script_key,
                    type="script.mapping",
                    action="create",
                    name=script_config["component_name"],
                    config=script_config,
                )
            )

        # 5) depends_on: source/target profiles + every referenced script key.
        depends_on = list(profile_deps)
        for ref in script_refs:
            depends_on.append(ref[len(_REF_PREFIX):])

        map_component = IntegrationComponentSpec(
            key=map_key,
            type="transform.map",
            action="create",
            name=config["component_name"],
            config=config,
            depends_on=depends_on,
        )
        return script_components, map_component

    # ------------------------------------------------------------------
    # Shared
    # ------------------------------------------------------------------

    @staticmethod
    def _direct_field_mappings(params: FieldMapParameters) -> List[Dict[str, str]]:
        return [
            {"source_path": op.source_field, "target_path": op.target_path}
            for op in params.direct
        ]
