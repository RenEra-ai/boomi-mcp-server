"""http_listener_to_db archetype (M6, issue #12).

Exposes an inbound-HTTP-to-database integration as a **thin preset over the M5
``sync_pipeline``** — the event-triggered sibling of ``api_to_database_sync``:
a bare Web Services Server (WSS) listener source, an optional transform map,
and a database **write** target wired as a verified-linear
``listener -> map -> write`` semantic stage graph. It emits the
``main_process`` with ``config.process_kind="sync_pipeline"`` and the stage
graph INTACT, so ``build_integration`` routes it through
:class:`SyncPipelineBuilder` — which collapses the listener stage into the
live-verified Listen start shape (connectoraction INSIDE the start shape, no
connection component) and locks the listener process options
(``allowSimultaneous="true"`` / ``updateRunDates="false"``).

It reuses the M6 ``wss_listen`` primitive, the #27 ``field_map`` primitive, and
the #74 ``db_write`` primitive plus the JSON profile builder; the only
component it emits itself is the listener request profile (``wss_listen``
*binds* a request profile, it does not generate one). Every byte of XML and all
structured validation are produced by the existing builders through those
primitives — this file emits JSON component specs only, never raw XML, and
never calls a live Boomi account.

Scope (M6): bare WSS on basic/intermediate runtimes; ``apiType=advanced``
requires an API Service Component and is deferred to #133. The listener is
JSON-input (``singlejson``/``multijson``) with an ack-only response
(``outputType="none"``); retry/DLQ and listener response mapping are out of
scope for this thin pass. Deploy-time verification (apiType preflight, path
collisions, live probe, execution readback) is owned by ``orchestrate_deploy``'s
``listener_verify`` stage.
"""

from __future__ import annotations

from typing import Any, Dict, List, Literal, Optional, Set

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
    ROLE_TRANSFORM_MAP,
    ROLE_WSS_LISTENER_OPERATION,
    primitive_component_key,
    raise_for_builder_error,
)
from ..primitives.db_write import DbWritePrimitive
from ..primitives.field_map import FieldMapPrimitive
from ..primitives.inbound_validate import (
    InboundValidateParameters,
    InboundValidatePrimitive,
)
from ..primitives.wss_listen import (
    WssListenParameters,
    WssListenPrimitive,
    compute_wss_endpoint,
    wss_http_method,
)

# Reuse #73/#74's transform contract + source-profile leaf helpers verbatim so
# the listener presets stay byte-aligned with the scheduled ones. The transform
# op classes are duck-typed on ``parameters.transform`` / the source leaves.
from .api_to_api_sync import (
    ApiTransformConfig,
    DirectApiTransformOperation,
    MapFunctionApiTransformOperation,
    MapScriptApiTransformOperation,
    _script_var_name,
    _SOURCE_PREFIX,
    _TRANSFORM_PREFIX,
)

# Reuse the DB-target contract + its param builders (duck-typed on
# ``parameters.target`` / ``parameters.transform``) so the listener->DB preset
# emits byte-identical target components to api_to_database_sync.
from .api_to_database_sync import (
    DbTarget,
    _TARGET_PREFIX,
    _build_db_write_params,
    _build_field_map_params,
    _target_write_profile_summary,
)

# Reuse the sibling archetypes' proven naming contract + secret-safe helpers.
from .database_to_api_sync import (
    JSONPayloadProfile,
    NamingConfig,
    _coerce_primitive_params,
    _component_names,
    _flatten_payload_profile_leaves,
    _named,
)
from ...categories.components.builders.json_profile_builder import (
    JSONGeneratedProfileBuilder,
)

# ---------------------------------------------------------------------------
# Assembly constants (M6, issue #12)
# ---------------------------------------------------------------------------

_MAIN_PROCESS_KEY = "main_process"
# The one component this preset emits itself: the JSON request profile the
# wss_listen operation binds and the transform map reads.
_LISTENER_REQUEST_PROFILE_KEY = "listener_request_profile"
# Role key for the request-profile display-name override.
_ROLE_LISTENER_PROFILE = "listener_request_profile"


# ---------------------------------------------------------------------------
# Listener source contract (shared with http_listener_to_rest)
# ---------------------------------------------------------------------------


class ListenerSource(BaseModel):
    """Bare-WSS listener source: operation shape + inbound payload profile."""

    model_config = ConfigDict(extra="forbid")

    object_name: str = Field(
        ...,
        description=(
            "WSS objectName — stored verbatim on the operation; the served bare "
            "listener endpoint is /ws/simple/{operationtype}{SentenceCase(objectName)} "
            "(Boomi upper-cases its first letter on the served path — "
            "live-settled 2026-07-04). Use a unique, project-specific name; "
            "duplicate paths on one runtime route unpredictably."
        ),
    )
    operation_type: str = Field(
        default="EXECUTE",
        description=(
            "WSS operationType: GET | QUERY | CREATE | UPDATE | UPSERT | DELETE "
            "| EXECUTE. NOT an HTTP verb — the HTTP method derives from "
            "input_type (JSON input -> POST)."
        ),
    )
    input_type: Literal["singlejson", "multijson"] = Field(
        default="singlejson",
        description=(
            "Inbound JSON document shape. This preset is JSON-input only (the "
            "transform maps the declared payload profile leaves); both values "
            "arrive as HTTP POST."
        ),
    )
    response_content_type: Literal[
        "application/json", "application/xml", "text/plain"
    ] = Field(
        default="text/plain",
        description="Content type of the ack response (the listener responds ack-only).",
    )
    payload_profile: JSONPayloadProfile = Field(
        ...,
        description=(
            "Caller-supplied JSON profile tree describing the inbound request "
            "body. The preset generates a JSON profile from this tree and binds "
            "it as the listener's request profile; transform source_path "
            "references resolve against its simple leaves."
        ),
    )
    label: Optional[str] = Field(
        default=None, description="Optional userlabel for the Listen start shape."
    )

    @model_validator(mode="after")
    def _require_non_blank(self) -> "ListenerSource":
        if not self.object_name or not self.object_name.strip():
            raise ValueError("listener.object_name must be non-blank")
        return self


class InboundValidationConfig(BaseModel):
    """Opt-in build-time inbound-contract validation (M6 inbound_validate)."""

    model_config = ConfigDict(extra="forbid")

    enabled: bool = Field(
        default=False,
        description=(
            "When true, the inbound_validate primitive asserts at build time "
            "that the listener binds a JSON request profile (mode="
            "'profile_bound'). No runtime validation shape is emitted — Boomi's "
            "WSS does not schema-validate at the perimeter."
        ),
    )
    mode: Literal["profile_bound"] = "profile_bound"


# ---------------------------------------------------------------------------
# Top-level parameters
# ---------------------------------------------------------------------------


class HttpListenerToDbParameters(BaseModel):
    model_config = ConfigDict(extra="forbid")

    naming: NamingConfig = Field(
        ...,
        description="Naming, folder, and runtime-hint configuration for the emitted integration.",
    )
    listener: ListenerSource = Field(
        ...,
        description="Inbound WSS listener: operation shape and JSON request payload profile tree.",
    )
    target: DbTarget = Field(
        ...,
        description="Database target: connection binding, write profile (statement type + columns), and Send tuning.",
    )
    transform: ApiTransformConfig = Field(
        ...,
        description=(
            "Typed transform operations moving inbound payload leaves into "
            "database write-profile columns/conditions (direct/map_function/"
            "map_script; xslt rejected). Target paths are 'Fields/<col>' and "
            "'Conditions/<col>' (the write profile field index)."
        ),
    )
    inbound_validation: InboundValidationConfig = Field(
        default_factory=InboundValidationConfig,
        description="Opt-in build-time inbound-contract validation (profile_bound).",
    )

    @model_validator(mode="after")
    def _validate_transform_refs(self) -> "HttpListenerToDbParameters":
        source_leaves: Dict[str, str] = _flatten_payload_profile_leaves(
            self.listener.payload_profile
        )
        # The database write profile's map-target field index ("Fields/<col>" /
        # "Conditions/<col>"). When the statement_type is unsupported (or a valid
        # type is missing its required fields/conditions) the index is empty — do
        # NOT pre-empt the write-profile builder's precise emit-time rejection
        # with a transform-path error; only validate targets against a non-empty
        # index (mirrors api_to_database_sync).
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
                "listener.payload_profile"
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
# Assembly helpers (shared with http_listener_to_rest via import)
# ---------------------------------------------------------------------------


def _build_listener_request_profile(parameters, overrides: Dict[str, str], folder):
    """Emit the listener request profile component and return (component, field_index).

    ``wss_listen`` only *binds* a request profile, so the preset generates one
    from the caller's listener.payload_profile tree (the exact analogue of
    ``_build_source_response_profile`` on the fetch presets). The same field
    index feeds the field_map source binding. Duck-typed on
    ``parameters.naming`` / ``parameters.listener``.
    """
    naming = parameters.naming
    profile_name = (
        _named(overrides, _ROLE_LISTENER_PROFILE)
        or f"{naming.component_prefix} Listener Request Profile"
    )
    config: Dict[str, Any] = {
        "profile_type": "json.generated",
        "component_name": profile_name,
        "root": parameters.listener.payload_profile.model_dump()["root"],
    }
    if folder:
        config["folder_path"] = folder
    raise_for_builder_error(JSONGeneratedProfileBuilder.validate_config(config))
    field_index = JSONGeneratedProfileBuilder.build_field_index(config)
    component = IntegrationComponentSpec(
        key=_LISTENER_REQUEST_PROFILE_KEY,
        type="profile.json",
        action="create",
        name=profile_name,
        config=config,
    )
    return component, field_index


def _build_wss_listen_params(parameters, overrides: Dict[str, str]) -> WssListenParameters:
    """Build wss_listen params from the archetype's listener contract.

    Duck-typed on ``parameters.listener`` (a :class:`ListenerSource`); shared by
    both listener presets. The request profile is always the generated
    ``listener_request_profile`` ($ref), output is locked ack-only.
    """
    listener = parameters.listener
    component_names: Dict[str, str] = {}
    op_name = _named(
        overrides,
        ROLE_WSS_LISTENER_OPERATION,
        primitive_component_key(_SOURCE_PREFIX, ROLE_WSS_LISTENER_OPERATION),
    )
    if op_name:
        component_names["operation"] = op_name
    return _coerce_primitive_params(
        WssListenParameters,
        {
            "key_prefix": _SOURCE_PREFIX,
            "object_name": listener.object_name,
            "operation_type": listener.operation_type,
            "input_type": listener.input_type,
            "output_type": "none",
            "response_content_type": listener.response_content_type,
            "request_profile_id": f"$ref:{_LISTENER_REQUEST_PROFILE_KEY}",
            "label": listener.label,
            "component_names": component_names,
        },
        field="listener",
    )


def _run_inbound_validation(parameters) -> Optional[Dict[str, Any]]:
    """Run the opt-in inbound_validate contract; return its metadata block.

    Duck-typed on ``parameters.listener`` / ``parameters.inbound_validation``.
    Returns None when validation is disabled. Raises the primitive's
    INBOUND_VALIDATION_UNSATISFIABLE when requested but unsatisfiable.
    """
    if not parameters.inbound_validation.enabled:
        return None
    params = InboundValidateParameters(
        mode=parameters.inbound_validation.mode,
        listener_input_type=parameters.listener.input_type,
        listener_request_profile_id=f"$ref:{_LISTENER_REQUEST_PROFILE_KEY}",
    )
    fragment = InboundValidatePrimitive.emit_fragment(
        PrimitiveBuildContext(
            integration_name=parameters.naming.integration_name,
            component_prefix=parameters.naming.component_prefix,
            folder_path=parameters.naming.folder_path,
        ),
        params,
    )
    return fragment["metadata"]["inbound_validation"]


def _listener_metadata_block(parameters, *, inbound_validation) -> Dict[str, Any]:
    """The ``validation_rules['listener']`` block orchestrate_deploy reads.

    Carries the computed endpoint so the ``listener_verify`` stage can probe the
    deployed route without re-deriving the formula. Duck-typed on
    ``parameters.listener``.
    """
    listener = parameters.listener
    operation_type = listener.operation_type.strip().upper()
    object_name = listener.object_name.strip()
    block: Dict[str, Any] = {
        "object_name": object_name,
        "operation_type": operation_type,
        "input_type": listener.input_type,
        "output_type": "none",
        "response_content_type": listener.response_content_type,
        "http_method": wss_http_method(listener.input_type),
        "endpoint_path": compute_wss_endpoint(operation_type, object_name),
        # Bare WSS serves basic/intermediate runtimes; advanced needs an API
        # Service Component (#133). listener_verify enforces this preflight.
        "api_type_requirement": "basic|intermediate (bare WSS); advanced requires an API Service Component (#133)",
        "test_mode_supported": False,
    }
    if inbound_validation is not None:
        block["inbound_validation"] = inbound_validation
    return block


def _listener_endpoint_summary(parameters) -> Dict[str, Any]:
    """Inert endpoint summary of the WSS listener source (no URLs resolved)."""
    listener = parameters.listener
    operation_type = listener.operation_type.strip().upper()
    object_name = listener.object_name.strip()
    return {
        "key": "wss_listener",
        "type": "wss",
        "direction": "source",
        "binding_mode": "create",
        "method": wss_http_method(listener.input_type),
        "endpoint_path": compute_wss_endpoint(operation_type, object_name),
        "executable": False,
    }


def _build_listener_pipeline_dict(target_kind: str, target_primitive: str,
                                  target_conn_key: str, target_op_key: str) -> Dict[str, Any]:
    """Build the verified-linear listener -> map -> {write|send} stage graph."""
    wss_op_key = primitive_component_key(_SOURCE_PREFIX, ROLE_WSS_LISTENER_OPERATION)
    map_key = primitive_component_key(_TRANSFORM_PREFIX, ROLE_TRANSFORM_MAP)
    target_stage: Dict[str, Any] = {
        "key": target_kind,
        "kind": target_kind,
        "config": {
            "primitive": target_primitive,
            "connection_id": f"$ref:{target_conn_key}",
            "operation_id": f"$ref:{target_op_key}",
        },
    }
    return {
        "stages": [
            {
                "key": "listener",
                "kind": "listener",
                "config": {
                    "primitive": "wss_listen",
                    "operation_id": f"$ref:{wss_op_key}",
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
            target_stage,
        ],
        "dependencies": [
            {"from_stage": "listener", "to_stage": "map"},
            {"from_stage": "map", "to_stage": target_kind},
        ],
    }


def _operation_summaries_listener(parameters) -> List[Dict[str, Any]]:
    """Inert transform-operation summaries (no scripts echoed)."""
    summaries: List[Dict[str, Any]] = []
    for op in parameters.transform.operations:
        if isinstance(op, DirectApiTransformOperation):
            summaries.append(
                {"operation_type": "direct", "source_path": op.source_path, "target_path": op.target_path}
            )
        elif isinstance(op, MapFunctionApiTransformOperation):
            summaries.append(
                {
                    "operation_type": "map_function",
                    "function_type": op.function_type,
                    "inputs": list(op.inputs),
                    "target_path": op.target_path,
                }
            )
        elif isinstance(op, MapScriptApiTransformOperation):
            summaries.append(
                {
                    "operation_type": "map_script",
                    "script_slot": op.script_slot,
                    "language": op.language,
                    "inputs": list(op.inputs),
                    "outputs": list(op.outputs),
                }
            )
    return summaries


def _build_listener_main_process(
    parameters,
    overrides: Dict[str, str],
    pipeline_dict: Dict[str, Any],
    *,
    default_name_suffix: str,
    target_conn_key: str,
    target_op_key: str,
) -> IntegrationComponentSpec:
    """The sync_pipeline main process for a listener preset (graph INTACT)."""
    naming = parameters.naming
    process_name = (
        _named(overrides, "process", _MAIN_PROCESS_KEY)
        or f"{naming.component_prefix} {default_name_suffix}"
    )
    config: Dict[str, Any] = {
        "process_kind": "sync_pipeline",
        "pipeline": pipeline_dict,
    }
    if naming.folder_path:
        config["folder_name"] = naming.folder_path
    depends_on = [
        primitive_component_key(_SOURCE_PREFIX, ROLE_WSS_LISTENER_OPERATION),
        primitive_component_key(_TRANSFORM_PREFIX, ROLE_TRANSFORM_MAP),
        target_conn_key,
        target_op_key,
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


class HttpListenerToDbArchetype(ArchetypePattern):
    metadata = PatternMetadata(
        name="http_listener_to_db",
        version="0.1.0",
        kind=PatternKind.ARCHETYPE,
        description=(
            "Archetype for landing inbound HTTP payloads into a relational "
            "database as a thin preset over the M5 sync_pipeline: a bare Web "
            "Services Server listener source (Listen start shape, no connection "
            "component, listener process options locked), an optional transform "
            "map, and a database write target — a verified-linear "
            "listener -> map -> write stage graph. The endpoint is "
            "/ws/simple/{operationtype}{SentenceCase(objectName)} on basic/intermediate "
            "runtimes (apiType=advanced needs an API Service Component, #133). "
            "Every byte of XML is produced by the existing component builders "
            "through the wss_listen / field_map / db_write primitives."
        ),
        tags=["listener", "wss", "inbound", "event", "database", "write", "sync_pipeline"],
        use_cases=[
            "Receive JSON webhooks and insert/update rows in a relational table",
            "Event-triggered database landing of pushed payloads",
        ],
        not_for=[
            "Scheduled/polling sources (use database_to_api_sync / api_to_database_sync)",
            "apiType=advanced runtimes without an API Service Component (#133)",
            "Listener response body mapping (the listener acks with outputType=none)",
            "Retry/DLQ on the listener path (M6 is verified-linear; parity is a follow-up)",
        ],
    )
    parameters_model = HttpListenerToDbParameters

    capability_notes = [
        "Discoverable, fully-typed parameter contract for an inbound-HTTP -> database integration.",
        "Emits a main process with process_kind='sync_pipeline' and an intact listener -> map -> write stage graph; SyncPipelineBuilder lowers the listener stage to the live-verified Listen start shape (connectoraction inside the start shape, no connection component).",
        "Listener process options are locked by construction: allowSimultaneous='true', updateRunDates='false' (live-captured invariants; defaults cause HTTP 500 under concurrency).",
        "The generated listener request profile is the transform's source shape; the database write profile (Fields/Conditions) is the map target through the confirmed #32 builders.",
        "Records the computed listener endpoint (/ws/simple/{operationtype}{SentenceCase(objectName)}, HTTP method from input_type) in validation_rules.listener for orchestrate_deploy's listener_verify stage.",
        "Opt-in inbound_validation (mode='profile_bound') asserts at build time that the listener binds a JSON request profile.",
        "Emits executable component specs for build_integration(action='plan'); all XML comes from the existing builders.",
    ]
    limitations = [
        "Emits JSON component specs only; performs no Boomi mutation and exposes no raw XML.",
        "Bare WSS only: serves basic/intermediate runtimes; apiType=advanced requires an API Service Component (deferred to #133).",
        "JSON input only (singlejson/multijson -> HTTP POST); inbound GET/query-parameter flows are out of scope.",
        "Ack-only response (outputType='none'): HTTP 200 does NOT imply process success — verify via execution records (listener_verify does this).",
        "Listener processes cannot run in Test mode; behavioral verification is deploy + live probe + execution readback.",
        "inbound_validation is a build-time contract (profile binding), not runtime payload validation — Boomi WSS does not schema-validate at the perimeter.",
        "Retry/DLQ, listener response mapping, and multi-route listeners are out of scope for this preset.",
        "Database write statement_type is limited to the #32-confirmed set; unconfirmed variants (e.g. upsert) are rejected.",
    ]

    examples = [
        PatternExample(
            name="webhook_to_database_insert",
            description=(
                "Receive a JSON webhook on /ws/simple/executeOrderIntake and "
                "insert the payload fields into a database table, as a "
                "process_kind='sync_pipeline' listener -> map -> write graph."
            ),
            parameters={
                "naming": {
                    "integration_name": "Order Intake Listener",
                    "component_prefix": "OrderIntake",
                },
                "listener": {
                    "object_name": "orderIntake",
                    "operation_type": "EXECUTE",
                    "payload_profile": {
                        "format": "json",
                        "root": {
                            "name": "Root",
                            "kind": "object",
                            "children": [
                                {"name": "id", "kind": "simple", "data_type": "character"},
                                {"name": "amount", "kind": "simple", "data_type": "number"},
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
                            {"name": "amount", "data_type": "number"},
                        ],
                    },
                },
                "transform": {
                    "operations": [
                        {"operation_type": "direct", "source_path": "Root/id", "target_path": "Fields/id"},
                        {"operation_type": "direct", "source_path": "Root/amount", "target_path": "Fields/amount"},
                    ]
                },
                "inbound_validation": {"enabled": True},
            },
        )
    ]

    @classmethod
    def emit_spec(cls, parameters: HttpListenerToDbParameters) -> IntegrationSpecV1:
        naming = parameters.naming
        write_profile = parameters.target.write_profile

        overrides = _component_names(naming)
        context = PrimitiveBuildContext(
            integration_name=naming.integration_name,
            component_prefix=naming.component_prefix,
            folder_path=naming.folder_path,
        )

        # Opt-in build-time inbound contract (raises when unsatisfiable).
        inbound_validation_meta = _run_inbound_validation(parameters)

        # ---- Endpoint summaries — no resolved URLs, no payload bodies ----
        source_endpoint = _listener_endpoint_summary(parameters)
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
        source_leaves = _flatten_payload_profile_leaves(parameters.listener.payload_profile)
        source_profile_summary: Dict[str, Any] = {
            "format": parameters.listener.payload_profile.format,
            "root_name": parameters.listener.payload_profile.root.name,
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
                "key": "listen",
                "name": "Listen for inbound HTTP payloads",
                "source": "wss_listener",
                "target": None,
                "operation": "wss_listen",
                "executable": False,
            },
            {
                "key": "transform",
                "name": "Map inbound payload to database write profile",
                "source": "listen",
                "target": None,
                "operation": "transform",
                "executable": False,
                "source_schema": source_profile_summary,
                "target_write_profile": target_profile_summary,
                "operations": _operation_summaries_listener(parameters),
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
            "archetype": "http_listener_to_db",
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
        # wss_listen only BINDS a request profile, so emit the listener profile
        # first and feed its field index to the field map source binding. The
        # db_write group emits the DB connection, write profile, and Send op;
        # the field map binds the write profile (profile.db) as its target.
        write_profile_key = primitive_component_key(_TARGET_PREFIX, ROLE_DB_WRITE_PROFILE)
        target_conn_key = primitive_component_key(_TARGET_PREFIX, ROLE_DB_CONNECTION)
        target_op_key = primitive_component_key(_TARGET_PREFIX, ROLE_DB_WRITE_OPERATION)
        components: List[IntegrationComponentSpec] = []
        listener_profile_component, listener_field_index = _build_listener_request_profile(
            parameters, overrides, naming.folder_path
        )
        components.append(listener_profile_component)
        components.extend(
            WssListenPrimitive.emit_components(
                context, _build_wss_listen_params(parameters, overrides)
            )
        )
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
                    response_profile_key=_LISTENER_REQUEST_PROFILE_KEY,
                    source_field_index=listener_field_index,
                    write_profile_key=write_profile_key,
                    target_field_index=write_index,
                ),
            )
        )
        pipeline_dict = _build_listener_pipeline_dict(
            "write", "db_write", target_conn_key, target_op_key
        )
        components.append(
            _build_listener_main_process(
                parameters,
                overrides,
                pipeline_dict,
                default_name_suffix="HTTP Listener to Database",
                target_conn_key=target_conn_key,
                target_op_key=target_op_key,
            )
        )

        return IntegrationSpecV1(
            version="1.0",
            name=naming.integration_name,
            mode="redesign",
            components=components,
            goals=[
                "Land inbound HTTP payloads into a relational database via an "
                "event-triggered listener -> transform -> write pipeline.",
                "Emit executable component specs whose main process is a "
                "process_kind='sync_pipeline' stage graph (listener -> map -> "
                "write) for build_integration(action='plan'). Deploy-time "
                "listener verification (apiType preflight, collision check, "
                "live probe, execution readback) is owned by orchestrate_deploy.",
            ],
            endpoints=[source_endpoint, target_endpoint],
            flows=flows,
            naming=naming_block,
            folders=folders_block,
            runtime=runtime_block,
            pipeline=PipelineSpec(**pipeline_dict),
            validation_rules={
                "contract_only": False,
                "component_count": len(components),
                "raw_xml_exposed": False,
                "boomi_mutation": False,
                "metadata_version": "0.1.0",
                "process_kind": "sync_pipeline",
                # orchestrate_deploy's listener_verify stage reads this block
                # from the build registry — keep the key set stable.
                "listener": _listener_metadata_block(
                    parameters, inbound_validation=inbound_validation_meta
                ),
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
                    "listener": (
                        "bare WSS (basic/intermediate apiType); advanced needs an "
                        "API Service Component (#133). JSON input only; ack-only "
                        "response; no Test mode — verify via listener_verify."
                    ),
                    "database_write": (
                        "statement_type one of standardinsertupdatedelete / "
                        "dynamicinsert / dynamicupdate / dynamicdelete / "
                        "storedprocedurewrite; unconfirmed variants (e.g. upsert) "
                        "are blocked (UNSUPPORTED_DB_STATEMENT_TYPE)"
                    ),
                    "reliability": "retry/DLQ not emitted (listener path is verified-linear in M6)",
                    "map_script": "inline script_body only; external script_component_ref rejected (#51)",
                },
                "profile_schema_strategy": (
                    "M6 uses a caller-supplied JSON payload profile for the "
                    "inbound request body and a caller-supplied database write "
                    "profile (columns + conditions) for the target; no schema "
                    "introspection or SQL generation is performed. The request "
                    "profile is generated and bound as the listener input shape; "
                    "the write profile is the map target."
                ),
                "transform_routes": {
                    "direct": "#26",
                    "map_function": "#40",
                    "map_script": "#41",
                    "xslt": "#42 (rejected)",
                },
            },
        )
