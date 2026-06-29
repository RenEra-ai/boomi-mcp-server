"""api_to_database_sync archetype (M5.8, issue #74).

Exposes API-to-database sync as a **thin preset over the M5 ``sync_pipeline``**
— the database-target sibling of #73's ``api_to_api_sync``: a REST fetch source,
an optional transform map, and a database **write** target wired as a
verified-linear ``fetch -> map -> write`` semantic stage graph. Like
``api_to_api_sync`` (and unlike ``database_to_api_sync``, which pre-lowers its
pipeline), it emits the ``main_process`` with ``config.process_kind="sync_pipeline"``
and the stage graph INTACT, so ``build_integration`` routes it through
:class:`SyncPipelineBuilder`. That is the literal realization of the acceptance
criterion "preset maps to ``sync_pipeline`` stages rather than a custom pairwise
process builder".

The database write target routes through the confirmed #32 component builders
(``DatabaseConnectorBuilder`` / ``DatabaseWriteProfileBuilder`` /
``DatabaseSendOperationBuilder``) via the #74 ``db_write`` primitive; the
transform map binds the write profile as its target (``Fields/<col>`` /
``Conditions/<col>``). Unconfirmed write-profile variants (any ``statement_type``
outside #32's confirmed set — e.g. ``upsert``) remain blocked by the write-profile
builder (``UNSUPPORTED_DB_STATEMENT_TYPE``).

It reuses the shipped #72 (``rest_fetch``), #27 (``field_map``), and #74
(``db_write``) primitives plus the JSON profile builder; the only component it
emits itself is the source response profile (``rest_fetch`` *binds* a response
profile, it does not generate one). Every byte of XML and all structured
validation are produced by the existing builders through those primitives — this
file emits JSON component specs only, never raw XML, no payload/body templates,
and never calls a live Boomi account.

Scope (M5.8): static REST fetch + database write. Runtime-bound query / path /
header / watermark behavior (#96 M5.4a) is OUT of scope — a dynamic ``{token}``
path is rejected at the contract layer (inherited from the reused ``ApiSource``
contract), mirroring ``SyncPipelineBuilder``'s rejection of ``runtime_bindings``
on a stage. Pagination, conditional requests, retry/DLQ, and schedule activation
are likewise out of scope for this thin pass.
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional, Set

from pydantic import BaseModel, ConfigDict, Field, model_validator

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
from ..primitives._helpers import (
    ROLE_DB_CONNECTION,
    ROLE_DB_WRITE_OPERATION,
    ROLE_DB_WRITE_PROFILE,
    ROLE_REST_SOURCE_CONNECTION,
    ROLE_REST_SOURCE_OPERATION,
    ROLE_SCRIPT,
    ROLE_TRANSFORM_MAP,
    primitive_component_key,
)
from ..primitives.db_extract import DbConnection
from ..primitives.db_write import (
    DbSendOperationParams,
    DbWriteParameters,
    DbWritePrimitive,
    DbWriteProfileParams,
)
from ..primitives.field_map import FieldMapParameters, FieldMapPrimitive
from ..primitives.rest_fetch import RestFetchPrimitive

# Reuse #73's REST source + transform contract models and source-build helpers
# verbatim so the two presets stay byte-aligned and the diff stays minimal. The
# source-build helpers are duck-typed on ``parameters.source`` / ``parameters.naming``
# (an ApiSource + NamingConfig), which this archetype's parameters also carry.
from .api_to_api_sync import (
    ApiSource,
    ApiTransformConfig,
    DirectApiTransformOperation,
    MapFunctionApiTransformOperation,
    MapScriptApiTransformOperation,
    _build_rest_fetch_params,
    _build_source_response_profile,
    _operation_summaries,
    _script_var_name,
    _SOURCE_PREFIX,
    _SOURCE_RESPONSE_PROFILE_KEY,
    _TRANSFORM_PREFIX,
)

# Reuse the sibling archetype's proven naming contract + secret-safe helpers.
from .database_to_api_sync import (
    UNSUPPORTED_SCRIPT_COMPONENT_REF,
    NamingConfig,
    _coerce_primitive_params,
    _component_names,
    _flatten_payload_profile_leaves,
    _named,
)
from ...categories.components.builders.connector_builder import (
    BuilderValidationError,
)

# ---------------------------------------------------------------------------
# Assembly constants (issue #74)
# ---------------------------------------------------------------------------

# Stable primitive key prefixes — the emitted component keys are
# ``{prefix}_{role}`` (e.g. ``source_rest_source_connection``,
# ``transform_transform_map``, ``target_db_write_operation``). The archetype
# assembles its $ref wiring from these deterministic keys, so they must stay
# stable. The source + transform prefixes/keys are shared with #73.
_TARGET_PREFIX = "target"
_MAIN_PROCESS_KEY = "main_process"


# ---------------------------------------------------------------------------
# Target — database write (#32 builders via the db_write primitive)
# ---------------------------------------------------------------------------


class DbTarget(BaseModel):
    model_config = ConfigDict(extra="forbid")

    connection: DbConnection = Field(
        ...,
        description=(
            "How the database TARGET connector is materialized — create new "
            "connector-settings (driver_id + auth) or reuse an existing Boomi "
            "connection by id/name. Credentials are never echoed; route them via "
            "the connection's credential_ref."
        ),
    )
    write_profile: DbWriteProfileParams = Field(
        ...,
        description=(
            "Database Write profile: caller-authored statement_type "
            "(standardinsertupdatedelete / dynamicinsert / dynamicupdate / "
            "dynamicdelete / storedprocedurewrite) plus columns (fields) and "
            "WHERE keys (conditions). Unconfirmed variants (e.g. 'upsert') are "
            "rejected by the write-profile builder."
        ),
    )
    send_operation: DbSendOperationParams = Field(
        default_factory=DbSendOperationParams,
        description="Optional database Send tuning (commit_option / batch_count / enable_batching).",
    )


# ---------------------------------------------------------------------------
# Top-level parameters
# ---------------------------------------------------------------------------


class ApiToDatabaseSyncParameters(BaseModel):
    model_config = ConfigDict(extra="forbid")

    naming: NamingConfig = Field(
        ...,
        description="Naming, folder, and runtime-hint configuration for the emitted integration.",
    )
    source: ApiSource = Field(
        ...,
        description="REST source: connector binding, static fetch request, and JSON response profile tree.",
    )
    target: DbTarget = Field(
        ...,
        description="Database target: connection binding, write profile (statement type + columns), and Send tuning.",
    )
    transform: ApiTransformConfig = Field(
        ...,
        description=(
            "Typed transform operations moving source response leaves into "
            "database write-profile columns/conditions (direct/map_function/"
            "map_script; xslt rejected). Target paths are 'Fields/<col>' and "
            "'Conditions/<col>' (the write profile field index)."
        ),
    )

    @model_validator(mode="after")
    def _validate_transform_refs(self) -> "ApiToDatabaseSyncParameters":
        source_leaves: Dict[str, str] = _flatten_payload_profile_leaves(
            self.source.response_profile
        )
        # The database write profile's map-target field index ("Fields/<col>" /
        # "Conditions/<col>"). When the statement_type is unsupported (or a valid
        # type is missing its required fields/conditions) the index is empty — do
        # NOT pre-empt the write-profile builder's precise emit-time rejection
        # (UNSUPPORTED_DB_STATEMENT_TYPE / MISSING_DB_*) with a transform-path
        # error; only validate target paths against a non-empty index.
        write_index = DbWritePrimitive.build_field_index(self.target.write_profile)
        target_leaves: Set[str] = set(write_index.keys())
        validate_targets = bool(write_index)
        required_target_paths: Set[str] = {
            path for path, entry in write_index.items() if entry.get("required")
        }

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

        def _check_target(target_path: str) -> None:
            nonlocal unknown_target_refs
            if not validate_targets:
                return
            if target_path in target_leaves:
                _bind(target_path)
            else:
                unknown_target_refs += 1

        for op in self.transform.operations:
            if isinstance(op, DirectApiTransformOperation):
                if op.source_path not in source_leaves:
                    unknown_source_refs += 1
                _check_target(op.target_path)
            elif isinstance(op, MapFunctionApiTransformOperation):
                for inp in op.inputs:
                    if inp not in source_leaves:
                        unknown_source_refs += 1
                _check_target(op.target_path)
            elif isinstance(op, MapScriptApiTransformOperation):
                for inp in op.inputs:
                    if inp not in source_leaves:
                        unknown_source_refs += 1
                for out in op.outputs:
                    _check_target(out)
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

        unmapped_required_count = (
            len(required_target_paths - bound_target_paths) if validate_targets else 0
        )

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
                "to a target path that is not a declared write-profile column "
                "('Fields/<col>') or condition ('Conditions/<col>')"
            )
        if duplicate_target_bindings:
            issues.append(
                f"transform.operations bind {duplicate_target_bindings} write-profile "
                "target path(s) more than once; every column/condition may be the "
                "destination of at most one direct/map_function/map_script output"
            )
        if unmapped_required_count:
            issues.append(
                f"transform.operations leave {unmapped_required_count} required "
                "write-profile target path(s) unmapped; every mandatory column and "
                "every WHERE condition must be the destination of at least one "
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


# ---------------------------------------------------------------------------
# Assembly helpers
# ---------------------------------------------------------------------------


def _build_db_write_params(
    parameters: "ApiToDatabaseSyncParameters", overrides: Dict[str, str]
) -> DbWriteParameters:
    target = parameters.target

    component_names: Dict[str, str] = {}
    conn_name = _named(
        overrides,
        ROLE_DB_CONNECTION,
        primitive_component_key(_TARGET_PREFIX, ROLE_DB_CONNECTION),
    )
    if conn_name:
        component_names["connection"] = conn_name
    write_name = _named(
        overrides,
        ROLE_DB_WRITE_PROFILE,
        primitive_component_key(_TARGET_PREFIX, ROLE_DB_WRITE_PROFILE),
    )
    if write_name:
        component_names["write_profile"] = write_name
    op_name = _named(
        overrides,
        ROLE_DB_WRITE_OPERATION,
        primitive_component_key(_TARGET_PREFIX, ROLE_DB_WRITE_OPERATION),
    )
    if op_name:
        component_names["send_operation"] = op_name

    return _coerce_primitive_params(
        DbWriteParameters,
        {
            "key_prefix": _TARGET_PREFIX,
            "connection": target.connection,
            "write_profile": target.write_profile,
            "operation": target.send_operation,
            "component_names": component_names,
        },
        field="target",
    )


def _build_field_map_params(
    parameters: "ApiToDatabaseSyncParameters",
    overrides: Dict[str, str],
    *,
    response_profile_key: str,
    source_field_index: Dict[str, Dict[str, Any]],
    write_profile_key: str,
    target_field_index: Dict[str, Dict[str, Any]],
) -> FieldMapParameters:
    """Build field_map params binding the transform map to the DB write profile.

    Mirrors ``api_to_api_sync._build_field_map_params`` op conversion, but binds
    the map's TARGET to the pre-existing database Write profile (``profile.db``,
    via ``target_binding``) instead of generating a JSON payload profile. The
    source binding is identical (the generated JSON source response profile).
    """
    transform = parameters.transform

    direct: List[Dict[str, Any]] = []
    map_function: List[Dict[str, Any]] = []
    map_script: List[Dict[str, Any]] = []
    for op in transform.operations:
        if isinstance(op, DirectApiTransformOperation):
            direct.append({"source_field": op.source_path, "target_path": op.target_path})
        elif isinstance(op, MapFunctionApiTransformOperation):
            entry: Dict[str, Any] = {
                "function_type": op.function_type,
                "inputs": list(op.inputs),
                "target_path": op.target_path,
            }
            if op.parameters:
                entry["parameters"] = dict(op.parameters)
            map_function.append(entry)
        elif isinstance(op, MapScriptApiTransformOperation):
            # External script-component reuse would plan with a dangling dependency
            # (the component is not in the emitted spec). Reject it with a clear
            # error instead of an unplannable spec (mirrors #73 api_to_api_sync).
            if op.script_component_ref is not None:
                raise BuilderValidationError(
                    "map_script.script_component_ref is not supported by this "
                    "archetype — the referenced script component is not part of "
                    "the emitted spec, so the plan cannot resolve it.",
                    error_code=UNSUPPORTED_SCRIPT_COMPONENT_REF,
                    field="transform.operations.script_component_ref",
                    hint=(
                        "Provide the script inline via map_script.script_body so "
                        "the archetype materializes the script.mapping component "
                        "in the spec. External script-component reuse is deferred "
                        "to #51."
                    ),
                )
            # The contract's inputs/outputs are leaf paths; field_map's MapScriptOp
            # needs named ports, so derive a language-safe variable name from each
            # path (sanitized final segment). Uniqueness across the shared
            # input/output namespace was already enforced by the contract
            # validator. field_map enforces that script_body is present.
            script_entry: Dict[str, Any] = {
                "inputs": [
                    {"source_path": path, "input_name": _script_var_name(path)}
                    for path in op.inputs
                ],
                "outputs": [
                    {"output_name": _script_var_name(path), "target_path": path}
                    for path in op.outputs
                ],
                "language": op.language,
            }
            if op.script_body is not None:
                script_entry["script_body"] = op.script_body
            map_script.append(script_entry)

    component_names: Dict[str, str] = {}
    map_name = _named(
        overrides,
        ROLE_TRANSFORM_MAP,
        primitive_component_key(_TRANSFORM_PREFIX, ROLE_TRANSFORM_MAP),
    )
    if map_name:
        component_names["transform_map"] = map_name
    script_prefix = _named(overrides, ROLE_SCRIPT, f"{_TRANSFORM_PREFIX}_{ROLE_SCRIPT}")
    if script_prefix:
        component_names["script_prefix"] = script_prefix

    return _coerce_primitive_params(
        FieldMapParameters,
        {
            "key_prefix": _TRANSFORM_PREFIX,
            "source": {
                "source_profile_id": f"$ref:{response_profile_key}",
                "source_profile_type": "profile.json",
                "source_field_index": source_field_index,
            },
            # Bind the pre-existing DB write profile as the map target (#74) — no
            # generated JSON target profile.
            "target_binding": {
                "target_profile_id": f"$ref:{write_profile_key}",
                "target_profile_type": "profile.db",
                "target_field_index": target_field_index,
            },
            "direct": direct,
            "map_function": map_function,
            "map_script": map_script,
            "component_names": component_names,
        },
        field="transform",
    )


def _build_pipeline_dict(parameters: "ApiToDatabaseSyncParameters") -> Dict[str, Any]:
    """Build the verified-linear fetch -> map -> write sync_pipeline stage graph.

    The $ref tokens nested in stage config are resolved generically by
    build_integration's _resolve_dependency_tokens at apply time; at plan time
    SyncPipelineBuilder lowers this graph (fetch -> [map] -> write) and the
    integration builder runs the proven $ref-reachability + ref-type checks.
    """
    source_conn_key = primitive_component_key(_SOURCE_PREFIX, ROLE_REST_SOURCE_CONNECTION)
    source_op_key = primitive_component_key(_SOURCE_PREFIX, ROLE_REST_SOURCE_OPERATION)
    map_key = primitive_component_key(_TRANSFORM_PREFIX, ROLE_TRANSFORM_MAP)
    target_conn_key = primitive_component_key(_TARGET_PREFIX, ROLE_DB_CONNECTION)
    target_op_key = primitive_component_key(_TARGET_PREFIX, ROLE_DB_WRITE_OPERATION)

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
                "key": "write",
                "kind": "write",
                "config": {
                    # action_type defaults to "Send" in the sync_pipeline lowering
                    # (a write stage is always a database Send), like a fetch stage
                    # defaults to GET — so it is intentionally omitted here.
                    "primitive": "db_write",
                    "connection_id": f"$ref:{target_conn_key}",
                    "operation_id": f"$ref:{target_op_key}",
                },
            },
        ],
        "dependencies": [
            {"from_stage": "fetch", "to_stage": "map"},
            {"from_stage": "map", "to_stage": "write"},
        ],
    }


def _build_main_process(
    parameters: "ApiToDatabaseSyncParameters",
    overrides: Dict[str, str],
    pipeline_dict: Dict[str, Any],
) -> IntegrationComponentSpec:
    naming = parameters.naming
    process_name = (
        _named(overrides, "process", _MAIN_PROCESS_KEY)
        or f"{naming.component_prefix} API to Database Sync"
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
    # LOWERED process config (the source connection+operation, the map, and the
    # target DB connection+Send operation). The source response profile and the
    # write profile are depended transitively by the operation/map components.
    depends_on = [
        primitive_component_key(_SOURCE_PREFIX, ROLE_REST_SOURCE_CONNECTION),
        primitive_component_key(_SOURCE_PREFIX, ROLE_REST_SOURCE_OPERATION),
        primitive_component_key(_TRANSFORM_PREFIX, ROLE_TRANSFORM_MAP),
        primitive_component_key(_TARGET_PREFIX, ROLE_DB_CONNECTION),
        primitive_component_key(_TARGET_PREFIX, ROLE_DB_WRITE_OPERATION),
    ]

    return IntegrationComponentSpec(
        key=_MAIN_PROCESS_KEY,
        type="process",
        action="create",
        name=process_name,
        config=config,
        depends_on=depends_on,
    )


def _target_write_profile_summary(
    write_profile: DbWriteProfileParams,
    write_index: Dict[str, Dict[str, Any]],
) -> Dict[str, Any]:
    """Inert review summary of the database write target (no XML, no SQL echo)."""
    return {
        "kind": "database.write",
        "statement_type": write_profile.statement_type,
        "field_count": sum(1 for p in write_index if p.startswith("Fields/")),
        "condition_count": sum(1 for p in write_index if p.startswith("Conditions/")),
        "target_paths": [
            {
                "path": path,
                "data_type": entry.get("data_type"),
                "required": bool(entry.get("required")),
            }
            for path, entry in sorted(write_index.items())
        ],
    }


# ---------------------------------------------------------------------------
# Archetype
# ---------------------------------------------------------------------------


class ApiToDatabaseSyncArchetype(ArchetypePattern):
    metadata = PatternMetadata(
        name="api_to_database_sync",
        version="0.1.0",
        kind=PatternKind.ARCHETYPE,
        description=(
            "Archetype for replicating records from a REST API source to a "
            "relational database target as a thin preset over the M5 "
            "sync_pipeline. Validates parameters (caller-supplied JSON source "
            "response profile, database write profile, and typed transform "
            "operations) and emits an executable IntegrationSpecV1 whose main "
            "process carries process_kind='sync_pipeline' with an intact "
            "fetch -> map -> write stage graph (NOT a custom pairwise process "
            "builder). The database write routes through the confirmed #32 "
            "component builders; unconfirmed write-profile variants stay blocked. "
            "Every byte of XML is produced by the existing component builders "
            "through the rest_fetch / field_map / db_write primitives."
        ),
        tags=["sync", "api", "database", "rest", "write", "sync_pipeline"],
        use_cases=[
            "Replicate records fetched from a REST API into a relational table",
            "Land API responses into a database via insert/update/delete or a stored procedure",
        ],
        not_for=[
            "Database sources (use database_to_api_sync)",
            "REST/API targets (use api_to_api_sync)",
            "Runtime-bound path/query/header/watermark behavior (#96 M5.4a)",
            "upsert / unconfirmed database write-profile variants",
            "Pagination, retry/DLQ, or schedule activation (out of scope for this preset)",
        ],
    )
    parameters_model = ApiToDatabaseSyncParameters

    capability_notes = [
        "Discoverable, fully-typed parameter contract for a REST -> database sync.",
        "Strict per-field validation surfaces structured PARAM_VALIDATION_FAILED errors.",
        "Emits a main process with process_kind='sync_pipeline' and an intact fetch -> map -> write stage graph; build_integration routes it through the verified-linear SyncPipelineBuilder.",
        "The database write target routes through the confirmed #32 component builders (a profile.db write profile + a database Send connector-action); the transform map binds that write profile (Fields/Conditions) as its target.",
        "Caller-supplied JSON source response profile and database write profile are the source of truth; the preset generates the source profile and binds it as the fetch output shape.",
        "Emits executable component specs (REST source, transform map, database write group, process) for build_integration(action='plan').",
        "All XML is produced by the existing component builders through the shipped rest_fetch / field_map / db_write primitives; the archetype emits JSON component specs only.",
        "The generated plan is inspectable through the existing MCP planning/review flows (build_integration plan, plan_integration_design).",
        "Sets spec.pipeline to the same semantic stage graph so the plan is inspectable as a pipeline.",
        "Credentials cross the contract only as opaque credential_ref values and are never echoed in errors.",
    ]
    limitations = [
        "Emits JSON component specs only; performs no Boomi mutation and exposes no raw XML or SQL/payload templates.",
        "REST source is GET-only; the database target is a Send (write) operation.",
        "Static REST only: a '{token}' dynamic path is rejected (runtime path binding is #96 M5.4a); query parameters and headers are static.",
        "Database write statement_type is one of standardinsertupdatedelete / dynamicinsert / dynamicupdate / dynamicdelete / storedprocedurewrite; unconfirmed variants (e.g. upsert) are rejected (UNSUPPORTED_DB_STATEMENT_TYPE).",
        "Transform target paths address the write profile field index ('Fields/<col>' and 'Conditions/<col>'); every mandatory column and WHERE condition must be mapped.",
        "Pagination, conditional requests, retry/DLQ, watermark, and schedule activation are out of scope for this preset.",
        "REST create-mode emits only auth='none'; secured source auth (basic / bearer / oauth2) requires binding.mode='reuse'.",
        "map_script materializes only an inline script_body; external script_component_ref reuse is rejected (#51). Each in-script variable is a path's sanitized final segment; two map_script paths that derive the same variable name are rejected.",
        "Does not mix map_function and map_script in one call (UNSUPPORTED_TRANSFORM_ROUTE); split into separate maps.",
        "operation_type='xslt' is rejected; the XSLT decision is owned by issue #42.",
        "credential_ref values are opaque end-to-end; the contract never resolves or validates secrets.",
    ]

    examples = [
        PatternExample(
            name="rest_to_database_dynamic_insert",
            description=(
                "Fetch customers from a REST source and insert them into a "
                "database table via a dynamic insert write profile, as a "
                "process_kind='sync_pipeline' fetch -> map -> write graph."
            ),
            parameters={
                "naming": {
                    "integration_name": "Customer API to DB Sync",
                    "component_prefix": "CustSync",
                },
                "source": {
                    "binding": {"mode": "create", "settings": {"base_url": "https://api.example.com", "auth_mode": "none"}},
                    "fetch_request": {"path": "/v1/<<source resource>>"},
                    "response_profile": {
                        "format": "json",
                        "root": {
                            "name": "Root",
                            "kind": "object",
                            "children": [
                                {"name": "id", "kind": "simple", "data_type": "character"},
                                {"name": "name", "kind": "simple", "data_type": "character"},
                            ],
                        },
                    },
                },
                "target": {
                    "connection": {"mode": "reuse", "component_name": "<<existing DB connection>>"},
                    "write_profile": {
                        "statement_type": "dynamicinsert",
                        "table_name": "<<target table>>",
                        "fields": [
                            {"name": "id", "data_type": "character", "mandatory": True},
                            {"name": "name", "data_type": "character"},
                        ],
                    },
                },
                "transform": {
                    "operations": [
                        {"operation_type": "direct", "source_path": "Root/id", "target_path": "Fields/id"},
                        {"operation_type": "direct", "source_path": "Root/name", "target_path": "Fields/name"},
                    ]
                },
            },
        )
    ]

    @classmethod
    def emit_spec(cls, parameters: ApiToDatabaseSyncParameters) -> IntegrationSpecV1:
        naming = parameters.naming
        source_binding = parameters.source.binding
        source_fetch = parameters.source.fetch_request
        source_profile = parameters.source.response_profile
        write_profile = parameters.target.write_profile

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

        target_connection = parameters.target.connection
        target_endpoint: Dict[str, Any] = {
            "key": "database_target",
            "type": "database",
            "direction": "target",
            "binding_mode": target_connection.mode,
            "operation": "Send",
            "statement_type": write_profile.statement_type,
            "executable": False,
        }
        if target_connection.mode == "create":
            target_endpoint["driver_id"] = target_connection.driver_id
        else:
            if target_connection.component_id:
                target_endpoint["component_id"] = target_connection.component_id
            if target_connection.component_name:
                target_endpoint["component_name"] = target_connection.component_name

        # ---- Profile summaries ----
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
        write_index = DbWritePrimitive.build_field_index(write_profile)
        target_profile_summary = _target_write_profile_summary(write_profile, write_index)

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
                "name": "Map source response to database write profile",
                "source": "fetch",
                "target": None,
                "operation": "transform",
                "executable": False,
                "source_schema": source_profile_summary,
                "target_write_profile": target_profile_summary,
                "operations": _operation_summaries(parameters),
            },
            {
                "key": "write",
                "name": "Write to database target",
                "source": "transform",
                "target": "database_target",
                "operation": "db_write",
                "executable": False,
            },
        ]

        naming_block: Dict[str, Any] = {
            "archetype": "api_to_database_sync",
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
        # first and feed its field index to both the fetch and the field map. The
        # db_write group emits the DB connection, write profile, and Send op; the
        # field map binds the write profile (profile.db) as its target.
        write_profile_key = primitive_component_key(_TARGET_PREFIX, ROLE_DB_WRITE_PROFILE)
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
        # Emit the db_write target group BEFORE the map so the write profile
        # exists for the map's target binding (the integration builder topo-sorts
        # by depends_on, but emitting in dependency order keeps the spec readable).
        components.extend(
            DbWritePrimitive.emit_components(
                context, _build_db_write_params(parameters, overrides)
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
                    write_profile_key=write_profile_key,
                    target_field_index=write_index,
                ),
            )
        )
        pipeline_dict = _build_pipeline_dict(parameters)
        components.append(_build_main_process(parameters, overrides, pipeline_dict))

        return IntegrationSpecV1(
            version="1.0",
            name=naming.integration_name,
            mode="redesign",
            components=components,
            goals=[
                "Replicate records from a REST API source to a relational "
                "database target via a static fetch -> transform -> write pipeline.",
                "Emit executable component specs whose main process is a "
                "process_kind='sync_pipeline' stage graph (fetch -> map -> write) "
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
                    "database_write": (
                        "statement_type one of standardinsertupdatedelete / "
                        "dynamicinsert / dynamicupdate / dynamicdelete / "
                        "storedprocedurewrite; unconfirmed variants (e.g. upsert) "
                        "are blocked (UNSUPPORTED_DB_STATEMENT_TYPE)"
                    ),
                    "pagination": "out of scope for this preset (#72 records pagination metadata only)",
                    "reliability": "retry/DLQ not emitted (sync_pipeline is verified-linear, M5.2)",
                    "watermark": "out of scope for this preset",
                    "schedule_activation": "M3 (deploy to a runtime first)",
                    "map_script": "inline script_body only; external script_component_ref rejected (#51)",
                },
                "profile_schema_strategy": (
                    "M5.8 uses a caller-supplied JSON response profile for the REST "
                    "source and a caller-supplied database write profile (columns + "
                    "conditions) for the database target; no API browse, schema "
                    "introspection, response sampling, or SQL generation is "
                    "performed. The source profile is generated and bound as the "
                    "fetch output shape; the write profile is the map target."
                ),
                "transform_routes": {
                    "direct": "#26",
                    "map_function": "#40",
                    "map_script": "#41",
                    "xslt": "#42 (rejected)",
                },
            },
        )
