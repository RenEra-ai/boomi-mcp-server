"""api_to_api_sync archetype (M5.7, issue #73).

Exposes API-to-API sync as a **thin preset over the M5 ``sync_pipeline``**: a
REST fetch source, an optional transform map, and a REST send target wired as a
verified-linear ``fetch -> map -> send`` semantic stage graph. Unlike
``database_to_api_sync`` (which pre-lowers its pipeline to a
``process_kind="database_to_api_sync"`` core at emit time), this preset emits the
``main_process`` with ``config.process_kind="sync_pipeline"`` and the stage graph
INTACT, so ``build_integration`` routes it through :class:`SyncPipelineBuilder`.
That is the literal realization of the acceptance criterion "preset maps to
``sync_pipeline`` stages rather than a custom pairwise process builder".

It reuses the shipped #72 (``rest_fetch``), #27 (``field_map``), and #28
(``rest_send_with_retry``) primitives plus the JSON profile builder; the only
component it emits itself is the source response profile (the ``rest_fetch``
primitive *binds* a response profile, it does not generate one). Every byte of
XML and all structured validation are produced by the existing builders through
those primitives — this file emits JSON component specs only, never raw XML, no
payload/body templates, and never calls a live Boomi account.

Scope (M5.7): static REST fetch + static REST send. Runtime-bound query / path /
header / watermark behavior (#96 M5.4a) is OUT of scope — a dynamic ``{token}``
path is rejected at the contract layer, mirroring ``SyncPipelineBuilder``'s
rejection of ``runtime_bindings`` on a stage. Pagination, conditional requests,
retry/DLQ, and schedule activation are likewise out of scope for this thin pass.
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional, Set

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    model_validator,
)

from ...categories.components.builders.profile_generation import (
    profile_from_json_schema,
    validate_field_mappings,
)
from ...models.integration_models import (
    IntegrationComponentSpec,
    IntegrationSpecV1,
)
from ...models.pipeline_models import PipelineSpec
from ..base import (
    ArchetypePattern,
    PatternExample,
    PatternKind,
    PatternMetadata,
    PrimitiveBuildContext,
)
from ...recipes.builtins.catalog import RECIPE_API_TO_API_SYNC
from ..recipe_bridge import run_sync_preset_recipe
from ..primitives._helpers import (
    ROLE_REST_CONNECTION,
    ROLE_REST_OPERATION,
    ROLE_REST_SOURCE_CONNECTION,
    ROLE_REST_SOURCE_OPERATION,
    ROLE_TRANSFORM_MAP,
    primitive_component_key,
)
from ..primitives.field_map import FieldMapPrimitive
from ..primitives.rest_fetch import RestFetchPrimitive
from ..primitives.rest_send import (
    RestSendWithRetryPrimitive,
)


# Shared contracts/helpers now live in the neutral modules (#151, M12.14); this
# archetype no longer imports its scheduled sibling.
from ..archetype_parameters import (
    ApiSource,
    ApiTarget,
    ApiTransformConfig,
    DirectApiTransformOperation,
    MapFunctionApiTransformOperation,
    MapScriptApiTransformOperation,
    NamingConfig,
    _flatten_payload_profile_leaves,
    _required_simple_leaf_paths,
)
from ..archetype_assembly import (
    _MAIN_PROCESS_KEY,
    _SOURCE_PREFIX,
    _SOURCE_RESPONSE_PROFILE_KEY,
    _TARGET_PREFIX,
    _TRANSFORM_PREFIX,
    _build_field_map_params,
    _build_rest_fetch_params,
    _build_rest_send_params,
    _build_source_response_profile,
    _component_names,
    _named,
    _operation_summaries,
    _script_var_name,
)

# Example payload sentinel — intentionally NOT a reusable path/payload template.
_EXAMPLE_PATH_SENTINEL = "/v1/<<source resource>>"


# ---------------------------------------------------------------------------
# Top-level parameters
# ---------------------------------------------------------------------------


class ApiToApiSyncParameters(BaseModel):
    model_config = ConfigDict(extra="forbid")

    naming: NamingConfig = Field(
        ...,
        description="Naming, folder, and runtime-hint configuration for the emitted integration.",
    )
    source: ApiSource = Field(
        ...,
        description="REST source: connector binding, static fetch request, and JSON response profile tree.",
    )
    target: ApiTarget = Field(
        ...,
        description="REST target: connector binding, static send request, and JSON payload profile tree.",
    )
    transform: ApiTransformConfig = Field(
        ...,
        description=(
            "Typed transform operations moving source response leaves into target "
            "payload leaves (direct/map_function/map_script; xslt rejected)."
        ),
    )

    @model_validator(mode="after")
    def _validate_transform_refs(self) -> "ApiToApiSyncParameters":
        source_leaves: Dict[str, str] = _flatten_payload_profile_leaves(
            self.source.response_profile
        )
        target_leaves: Dict[str, str] = _flatten_payload_profile_leaves(
            self.target.payload_profile
        )

        unknown_source_refs = 0
        unknown_target_refs = 0
        duplicate_target_bindings = 0
        script_var_collisions = 0
        bound_target_paths: Set[str] = set()

        def _bind(target_path: str) -> None:
            nonlocal duplicate_target_bindings
            if target_path in bound_target_paths:
                duplicate_target_bindings += 1
            else:
                bound_target_paths.add(target_path)

        for op in self.transform.operations:
            if isinstance(op, DirectApiTransformOperation):
                if op.source_path not in source_leaves:
                    unknown_source_refs += 1
                if op.target_path in target_leaves:
                    _bind(op.target_path)
                else:
                    unknown_target_refs += 1
            elif isinstance(op, MapFunctionApiTransformOperation):
                for inp in op.inputs:
                    if inp not in source_leaves:
                        unknown_source_refs += 1
                if op.target_path in target_leaves:
                    _bind(op.target_path)
                else:
                    unknown_target_refs += 1
            elif isinstance(op, MapScriptApiTransformOperation):
                for inp in op.inputs:
                    if inp not in source_leaves:
                        unknown_source_refs += 1
                for out in op.outputs:
                    if out in target_leaves:
                        _bind(out)
                    else:
                        unknown_target_refs += 1
                # The in-script variable for each path is its sanitized final
                # segment; inputs and outputs share one namespace inside the
                # script (ScriptMappingBuilder), so two paths deriving the same
                # variable name cannot be expressed. Reject here (clear, early)
                # instead of failing deep with SCRIPT_MAPPING_VARIABLE_INVALID.
                script_vars = [_script_var_name(p) for p in op.inputs] + [
                    _script_var_name(p) for p in op.outputs
                ]
                if len(set(script_vars)) != len(script_vars):
                    script_var_collisions += 1

        required_target_paths = _required_simple_leaf_paths(self.target.payload_profile)
        unmapped_required_count = len(required_target_paths - bound_target_paths)

        issues: List[str] = []
        if unknown_source_refs:
            issues.append(
                f"transform.operations contain {unknown_source_refs} reference(s) "
                "to a source path that is not a declared simple leaf in "
                "source.response_profile"
            )
        if unknown_target_refs:
            issues.append(
                f"transform.operations contain {unknown_target_refs} reference(s) "
                "to a target path that is not a declared simple leaf in "
                "target.payload_profile"
            )
        if duplicate_target_bindings:
            issues.append(
                f"transform.operations bind {duplicate_target_bindings} target "
                "leaf path(s) more than once; every leaf may be the destination of "
                "at most one direct/map_function/map_script output"
            )
        if unmapped_required_count:
            issues.append(
                f"transform.operations leave {unmapped_required_count} required "
                "target leaf path(s) unmapped; every required simple leaf in "
                "target.payload_profile must be the destination of at least one "
                "direct/map_function/map_script output"
            )
        if script_var_collisions:
            issues.append(
                f"{script_var_collisions} map_script operation(s) derive two or "
                "more identical in-script variable names from distinct paths (each "
                "variable is a path's sanitized final segment, and inputs/outputs "
                "share one namespace); rename the colliding leaves so every "
                "map_script input/output path yields a unique variable name"
            )

        if issues:
            raise ValueError(" | ".join(issues))

        return self


def _build_pipeline_dict(parameters: "ApiToApiSyncParameters") -> Dict[str, Any]:
    """Build the verified-linear fetch -> map -> send sync_pipeline stage graph.

    The $ref tokens nested in stage config are resolved generically by
    build_integration's _resolve_dependency_tokens at apply time; at plan time
    SyncPipelineBuilder lowers this graph and the integration builder runs the
    proven $ref-reachability check on the lowered source/transform/target.
    """
    source_conn_key = primitive_component_key(_SOURCE_PREFIX, ROLE_REST_SOURCE_CONNECTION)
    source_op_key = primitive_component_key(_SOURCE_PREFIX, ROLE_REST_SOURCE_OPERATION)
    map_key = primitive_component_key(_TRANSFORM_PREFIX, ROLE_TRANSFORM_MAP)
    target_conn_key = primitive_component_key(_TARGET_PREFIX, ROLE_REST_CONNECTION)
    target_op_key = primitive_component_key(_TARGET_PREFIX, ROLE_REST_OPERATION)

    return {
        "stages": [
            {
                "key": "fetch",
                "kind": "fetch",
                "config": {
                    "primitive": "rest_fetch",
                    "connection_id": f"$ref:{source_conn_key}",
                    "operation_id": f"$ref:{source_op_key}",
                },
            },
            {
                "key": "map",
                "kind": "map",
                "config": {
                    "primitive": "map",
                    "map_ref": f"$ref:{map_key}",
                },
            },
            {
                "key": "send",
                "kind": "send",
                "config": {
                    "primitive": "rest_send",
                    "action_type": parameters.target.send_request.method,
                    "connection_id": f"$ref:{target_conn_key}",
                    "operation_id": f"$ref:{target_op_key}",
                },
            },
        ],
        "dependencies": [
            {"from_stage": "fetch", "to_stage": "map"},
            {"from_stage": "map", "to_stage": "send"},
        ],
    }


def _build_main_process(
    parameters: "ApiToApiSyncParameters",
    overrides: Dict[str, str],
    pipeline_dict: Dict[str, Any],
) -> IntegrationComponentSpec:
    naming = parameters.naming
    process_name = (
        _named(overrides, "process", _MAIN_PROCESS_KEY)
        or f"{naming.component_prefix} API to API Sync"
    )

    # process_kind="sync_pipeline" with the stage graph INTACT — build_integration
    # routes it through SyncPipelineBuilder (do NOT pre-lower). No reliability /
    # source / target / transform / dynamic_path top-level keys (the sync_pipeline
    # top-level gate rejects them); only folder_name placement is carried.
    config: Dict[str, Any] = {
        "process_kind": "sync_pipeline",
        "pipeline": pipeline_dict,
    }
    if naming.folder_path:
        config["folder_name"] = naming.folder_path

    # depends_on must contain exactly the keys referenced by $ref tokens in the
    # LOWERED process config (the source/target connection+operation and the map).
    # The two profiles are depended transitively by the operation/map components.
    depends_on = [
        primitive_component_key(_SOURCE_PREFIX, ROLE_REST_SOURCE_CONNECTION),
        primitive_component_key(_SOURCE_PREFIX, ROLE_REST_SOURCE_OPERATION),
        primitive_component_key(_TRANSFORM_PREFIX, ROLE_TRANSFORM_MAP),
        primitive_component_key(_TARGET_PREFIX, ROLE_REST_CONNECTION),
        primitive_component_key(_TARGET_PREFIX, ROLE_REST_OPERATION),
    ]

    return IntegrationComponentSpec(
        key=_MAIN_PROCESS_KEY,
        type="process",
        action="create",
        name=process_name,
        config=config,
        depends_on=depends_on,
    )


# ---------------------------------------------------------------------------
# Archetype
# ---------------------------------------------------------------------------


class ApiToApiSyncArchetype(ArchetypePattern):
    metadata = PatternMetadata(
        name="api_to_api_sync",
        version="0.1.0",
        kind=PatternKind.ARCHETYPE,
        description=(
            "Archetype for replicating records from a REST API source to a REST "
            "API target as a thin preset over the M5 sync_pipeline. Validates "
            "parameters (caller-supplied JSON source response profile and target "
            "payload profile trees plus typed transform operations) and emits an "
            "executable IntegrationSpecV1 whose main process carries "
            "process_kind='sync_pipeline' with an intact fetch -> map -> send "
            "stage graph (NOT a custom pairwise process builder). Every byte of "
            "XML is produced by the existing component builders through the "
            "rest_fetch / field_map / rest_send primitives; the archetype emits "
            "JSON component specs only and never calls Boomi. Static REST only — "
            "runtime-bound path/query/header (#96), pagination, retry/DLQ, "
            "watermark, and schedule activation are out of scope for this preset."
        ),
        tags=[
            "api",
            "rest",
            "sync",
            "m5",
            "sync-pipeline",
            "no-boomi-mutation",
        ],
        use_cases=[
            "replicate records from a REST API source to a REST API target",
            "stand up a REST fetch -> transform -> REST send pipeline as a preset",
        ],
        not_for=[
            "database sources or targets (use database_to_api_sync)",
            "runtime-bound query/path/header or watermark behavior (#96 M5.4a)",
            "pagination loops, retry/DLQ, or deploying/scheduling the process",
        ],
    )
    parameters_model = ApiToApiSyncParameters

    capability_notes = [
        "Discoverable, fully-typed parameter contract for a REST -> REST sync.",
        "Strict per-field validation surfaces structured PARAM_VALIDATION_FAILED errors.",
        "Emits a main process with process_kind='sync_pipeline' and an intact fetch -> map -> send stage graph; build_integration routes it through the verified-linear SyncPipelineBuilder.",
        "Caller-supplied JSON source response profile and target payload profile trees are the source of truth; the preset generates the source profile and binds it as the fetch output shape.",
        "Emits executable component specs (REST source, JSON transform, REST target, process) for build_integration(action='plan').",
        "All XML is produced by the existing component builders through the shipped rest_fetch / field_map / rest_send primitives; the archetype emits JSON component specs only.",
        "The generated plan is inspectable through the existing MCP planning/review flows (build_integration plan, review_transformation, plan_integration_design).",
        "Sets spec.pipeline to the same semantic stage graph so the plan is inspectable as a pipeline.",
        "Credentials cross the contract only as opaque credential_ref values and are never echoed in errors.",
    ]
    limitations = [
        "Emits JSON component specs only; performs no Boomi mutation and exposes no raw XML or payload/body templates.",
        "REST source is GET-only; REST target carries the configured HTTP method.",
        "Static REST only: a '{token}' dynamic path is rejected (runtime path binding is #96 M5.4a); query parameters and headers are static.",
        "Pagination, conditional requests, retry/DLQ, watermark, and schedule activation are out of scope for this preset.",
        "REST create-mode emits only auth='none'; secured auth (basic / bearer / oauth2) requires binding.mode='reuse'.",
        "Create-mode connection default_headers are applied as operation-level request headers (operation-specific headers win on conflict); a reuse-mode connection carries its own configured headers.",
        "map_script materializes only an inline script_body; external script_component_ref reuse is rejected (#51). Each in-script variable is a path's sanitized final segment; two map_script paths that derive the same variable name are rejected (rename the colliding leaves).",
        "Does not mix map_function and map_script in one call (UNSUPPORTED_TRANSFORM_ROUTE); split into separate maps.",
        "operation_type='xslt' is rejected; the XSLT decision is owned by issue #42.",
        "credential_ref values are opaque end-to-end; the contract never resolves or validates secrets.",
    ]
    examples = [
        PatternExample(
            name="minimal_rest_to_rest_sync",
            description=(
                "Smallest valid payload: create-mode REST source with no auth and "
                "a one-leaf JSON response profile, create-mode REST target with no "
                "auth and a one-leaf JSON payload profile, a single direct "
                "transform operation. Placeholder sentinels only — not a reusable "
                "template."
            ),
            parameters={
                "naming": {
                    "integration_name": "demo-api-to-api-sync",
                    "component_prefix": "DEMO",
                },
                "source": {
                    "binding": {
                        "mode": "create",
                        "settings": {
                            "base_url": "https://source.example.com",
                            "auth_mode": "none",
                        },
                    },
                    "fetch_request": {
                        "path": _EXAMPLE_PATH_SENTINEL,
                    },
                    "response_profile": {
                        "format": "json",
                        "root": {
                            "name": "Root",
                            "kind": "object",
                            "children": [
                                {
                                    "name": "source_a",
                                    "kind": "simple",
                                    "data_type": "character",
                                },
                            ],
                        },
                    },
                },
                "target": {
                    "binding": {
                        "mode": "create",
                        "settings": {
                            "base_url": "https://target.example.com",
                            "auth_mode": "none",
                        },
                    },
                    "send_request": {
                        "method": "POST",
                        "path": "/v1/<<target resource>>",
                    },
                    "payload_profile": {
                        "format": "json",
                        "root": {
                            "name": "Root",
                            "kind": "object",
                            "children": [
                                {
                                    "name": "target_a",
                                    "kind": "simple",
                                    "data_type": "character",
                                },
                            ],
                        },
                    },
                },
                "transform": {
                    "operations": [
                        {
                            "operation_type": "direct",
                            "source_path": "Root/source_a",
                            "target_path": "Root/target_a",
                        },
                    ],
                },
            },
        ),
        PatternExample(
            name="reuse_connections_with_function",
            description=(
                "Fuller payload: reuse-mode REST source and target by component id "
                "(secured auth uses connection reuse) with nested JSON profiles "
                "and two transform operations (one direct, one map_function). "
                "Examples deliberately exclude map_script declarations to keep the "
                "published payload free of language tokens."
            ),
            parameters={
                "naming": {
                    "integration_name": "demo-api-to-api-enriched",
                    "component_prefix": "DEMO-ENR",
                    "folder_path": "Integrations/API/Sync",
                    "runtime_hints": {"atom_pool": "primary"},
                },
                "source": {
                    "binding": {
                        "mode": "reuse",
                        "component_id": "<<existing REST source connection id>>",
                    },
                    "fetch_request": {
                        "path": "/v1/<<source resource>>",
                        "query_parameters": {"limit": "100"},
                    },
                    "response_profile": {
                        "format": "json",
                        "root": {
                            "name": "Root",
                            "kind": "object",
                            "children": [
                                {
                                    "name": "id",
                                    "kind": "simple",
                                    "data_type": "character",
                                    "required": True,
                                },
                                {
                                    "name": "updated_at",
                                    "kind": "simple",
                                    "data_type": "datetime",
                                },
                            ],
                        },
                    },
                },
                "target": {
                    "binding": {
                        "mode": "reuse",
                        "component_id": "<<existing REST target connection id>>",
                    },
                    "send_request": {
                        "method": "PUT",
                        "path": "/v1/<<target resource>>",
                    },
                    "payload_profile": {
                        "format": "json",
                        "root": {
                            "name": "Root",
                            "kind": "object",
                            "children": [
                                {
                                    "name": "external_id",
                                    "kind": "simple",
                                    "data_type": "character",
                                    "required": True,
                                },
                                {
                                    "name": "modified",
                                    "kind": "simple",
                                    "data_type": "datetime",
                                },
                            ],
                        },
                    },
                },
                "transform": {
                    "operations": [
                        {
                            "operation_type": "direct",
                            "source_path": "Root/id",
                            "target_path": "Root/external_id",
                            "documentation_hint": "carry the source id verbatim",
                        },
                        {
                            "operation_type": "map_function",
                            "function_type": "date_format",
                            "inputs": ["Root/updated_at"],
                            "target_path": "Root/modified",
                            "parameters": {
                                "input_format": "<<source datetime format>>",
                                "output_format": "<<target datetime format>>",
                            },
                        },
                    ],
                },
            },
        ),
    ]

    @classmethod
    def emit_spec(
        cls, parameters: ApiToApiSyncParameters, *, recipe_version: Optional[str] = None
    ) -> IntegrationSpecV1:
        naming = parameters.naming
        source_binding = parameters.source.binding
        target_binding = parameters.target.binding
        source_fetch = parameters.source.fetch_request
        target_send = parameters.target.send_request
        source_profile = parameters.source.response_profile
        payload_profile = parameters.target.payload_profile

        overrides = _component_names(naming)
        context = PrimitiveBuildContext(
            integration_name=naming.integration_name,
            component_prefix=naming.component_prefix,
            folder_path=naming.folder_path,
        )

        # ---- Endpoint summaries — no resolved URLs, no payload bodies ----
        source_endpoint: Dict[str, Any] = {
            "key": "rest_source",
            "type": "rest",
            "direction": "source",
            "binding_mode": source_binding.mode,
            "method": "GET",
            "executable": False,
        }
        if source_binding.mode == "create" and source_binding.settings is not None:
            source_endpoint["auth_mode"] = source_binding.settings.auth_mode
        else:
            if source_binding.component_id:
                source_endpoint["component_id"] = source_binding.component_id
            if source_binding.component_name:
                source_endpoint["component_name"] = source_binding.component_name

        target_endpoint: Dict[str, Any] = {
            "key": "rest_target",
            "type": "rest",
            "direction": "target",
            "binding_mode": target_binding.mode,
            "method": target_send.method,
            "expected_status_codes": list(target_send.expected_status_codes),
            "executable": False,
        }
        if target_binding.mode == "create" and target_binding.settings is not None:
            target_endpoint["auth_mode"] = target_binding.settings.auth_mode
        else:
            if target_binding.component_id:
                target_endpoint["component_id"] = target_binding.component_id
            if target_binding.component_name:
                target_endpoint["component_name"] = target_binding.component_name

        # ---- Profile summaries — leaf path index + data type only ----
        source_leaves = _flatten_payload_profile_leaves(source_profile)
        source_profile_summary: Dict[str, Any] = {
            "format": source_profile.format,
            "root_name": source_profile.root.name,
            "leaf_count": len(source_leaves),
            "leaves": [
                {"path": path, "data_type": data_type}
                for path, data_type in sorted(source_leaves.items())
            ],
        }
        target_leaves = _flatten_payload_profile_leaves(payload_profile)
        target_profile_summary: Dict[str, Any] = {
            "format": payload_profile.format,
            "root_name": payload_profile.root.name,
            "leaf_count": len(target_leaves),
            "leaves": [
                {"path": path, "data_type": data_type}
                for path, data_type in sorted(target_leaves.items())
            ],
        }

        # ---- Transform-review metadata (consumed by review_transformation) ----
        src_gen = profile_from_json_schema(
            source_profile, component_name=f"{naming.component_prefix} Source Profile"
        )
        tgt_gen = profile_from_json_schema(
            payload_profile, component_name=f"{naming.component_prefix} Target Profile"
        )
        direct_field_mappings = validate_field_mappings(
            src_gen["field_index_by_path"],
            tgt_gen["field_index_by_path"],
            [
                {"source_field": op.source_path, "target_path": op.target_path}
                for op in parameters.transform.operations
                if isinstance(op, DirectApiTransformOperation)
            ],
        )

        flows: List[Dict[str, Any]] = [
            {
                "key": "fetch",
                "name": "Fetch from REST source",
                "source": "rest_source",
                "target": None,
                "operation": "rest_fetch",
                "executable": False,
            },
            {
                "key": "transform",
                "name": "Map source response to target payload",
                "source": "fetch",
                "target": None,
                "operation": "transform",
                "executable": False,
                "source_schema": source_profile_summary,
                "target_payload_profile": target_profile_summary,
                "operations": _operation_summaries(parameters),
                "source_profile_generation": src_gen,
                "target_profile_generation": tgt_gen,
                "direct_field_mappings": direct_field_mappings,
            },
            {
                "key": "send",
                "name": "Send to REST target",
                "source": "transform",
                "target": "rest_target",
                "operation": "rest_send",
                "executable": False,
            },
        ]

        naming_block: Dict[str, Any] = {
            "archetype": "api_to_api_sync",
            "integration_name": naming.integration_name,
            "component_prefix": naming.component_prefix,
            "component_names": naming.component_names or {},
        }
        if naming.convention:
            naming_block["convention"] = naming.convention
        folders_block: Dict[str, Any] = (
            {"path": naming.folder_path} if naming.folder_path else {}
        )
        runtime_block: Dict[str, Any] = dict(naming.runtime_hints or {})

        # ---- Executable component assembly ----
        # rest_fetch only BINDS a response profile, so emit the source profile
        # first and feed its field index to both the fetch and the field map.
        components: List[IntegrationComponentSpec] = []
        source_profile_component, source_field_index = _build_source_response_profile(
            parameters, overrides, naming.folder_path
        )
        components.append(source_profile_component)
        components.extend(
            RestFetchPrimitive.emit_components(
                context,
                _build_rest_fetch_params(
                    parameters,
                    overrides,
                    response_profile_key=_SOURCE_RESPONSE_PROFILE_KEY,
                    source_field_index=source_field_index,
                ),
            )
        )
        components.extend(
            FieldMapPrimitive.emit_components(
                context,
                _build_field_map_params(
                    parameters,
                    overrides,
                    response_profile_key=_SOURCE_RESPONSE_PROFILE_KEY,
                    source_field_index=source_field_index,
                ),
            )
        )
        components.extend(
            RestSendWithRetryPrimitive.emit_components(
                context, _build_rest_send_params(parameters, overrides)
            )
        )
        pipeline_dict = _build_pipeline_dict(parameters)
        components.append(_build_main_process(parameters, overrides, pipeline_dict))

        # #145 M12.10: the same components, routed through the typed contribution
        # path. It runs AFTER the legacy materialization is complete and BEFORE
        # the spec is assembled, so every existing archetype error still fires
        # first and in its existing order — a recipe failure can never preempt
        # one, and the emitted spec below is byte-unchanged.
        run_sync_preset_recipe(
            recipe_id=RECIPE_API_TO_API_SYNC,
            components=components,
            process=components[-1],
            recipe_version=recipe_version,
        )

        return IntegrationSpecV1(
            version="1.0",
            name=naming.integration_name,
            mode="redesign",
            components=components,
            goals=[
                "Replicate records from a REST API source to a REST API target "
                "via a static fetch -> transform -> send pipeline.",
                "Emit executable component specs whose main process is a "
                "process_kind='sync_pipeline' stage graph (fetch -> map -> send) "
                "for build_integration(action='plan'); the plan is inspectable "
                "through the existing MCP planning/review flows. Deployment, "
                "schedule activation, pagination, retry/DLQ, and runtime-bound "
                "behavior remain out of scope.",
            ],
            endpoints=[source_endpoint, target_endpoint],
            flows=flows,
            naming=naming_block,
            folders=folders_block,
            runtime=runtime_block,
            # Expose the same semantic stage graph for plan inspectability (the
            # field is inert for the builder — the main process config drives XML).
            pipeline=PipelineSpec(**pipeline_dict),
            validation_rules={
                "contract_only": False,
                "component_count": len(components),
                "raw_xml_exposed": False,
                "boomi_mutation": False,
                "metadata_version": "0.1.0",
                "process_kind": "sync_pipeline",
                "transform_review": {
                    "supported_actions": [
                        "list_fields",
                        "validate_unmapped",
                        "mapping_diff",
                        "generate_test_payload",
                        "compare_expected_actual",
                    ],
                    "recommended_before_apply": [
                        "validate_unmapped",
                        "generate_test_payload",
                    ],
                },
                "limitations": {
                    "rest_source": "GET-only static fetch; runtime-bound path/query/header is #96 (M5.4a)",
                    "rest_create_auth": "auth='none' only; secured auth requires reuse",
                    "pagination": "out of scope for this preset (#72 records pagination metadata only)",
                    "reliability": "retry/DLQ not emitted (sync_pipeline is verified-linear, M5.2)",
                    "watermark": "out of scope for this preset",
                    "schedule_activation": "M3 (deploy to a runtime first)",
                    "map_script": "inline script_body only; external script_component_ref rejected (#51)",
                },
                "profile_schema_strategy": (
                    "M5.7 uses a caller-supplied JSON response profile for the REST "
                    "source and a caller-supplied JSON payload profile for the REST "
                    "target; no API browse, schema introspection, or response "
                    "sampling is performed. The source profile is generated and "
                    "bound as the fetch output shape; metadata/sample inference is "
                    "available separately via infer_profile_fields (issue #47)."
                ),
                "transform_routes": {
                    "direct": "#26",
                    "map_function": "#40",
                    "map_script": "#41",
                    "xslt": "#42 (rejected)",
                },
            },
        )
