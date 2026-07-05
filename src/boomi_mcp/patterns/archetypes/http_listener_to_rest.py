"""http_listener_to_rest archetype (M6, issue #12).

Exposes an inbound-HTTP-to-REST-API integration as a **thin preset over the M5
``sync_pipeline``** — the event-triggered sibling of ``api_to_api_sync``: a
bare Web Services Server (WSS) listener source, an optional transform map, and
a REST send target wired as a verified-linear ``listener -> map -> send``
semantic stage graph. It emits the ``main_process`` with
``config.process_kind="sync_pipeline"`` and the stage graph INTACT, so
``build_integration`` routes it through :class:`SyncPipelineBuilder` — which
collapses the listener stage into the live-verified Listen start shape
(connectoraction INSIDE the start shape, no connection component) and locks the
listener process options (``allowSimultaneous="true"`` /
``updateRunDates="false"``).

It reuses the M6 ``wss_listen`` primitive, the #27 ``field_map`` primitive, and
the #28 ``rest_send_with_retry`` primitive plus the JSON profile builder; the
only component it emits itself is the listener request profile (``wss_listen``
*binds* a request profile, it does not generate one). Every byte of XML and all
structured validation are produced by the existing builders through those
primitives — this file emits JSON component specs only, never raw XML, and
never calls a live Boomi account.

Scope (M6): bare WSS on basic/intermediate runtimes; ``apiType=advanced``
requires an API Service Component and is deferred to #133. The listener is
JSON-input with an ack-only response (``outputType="none"``); the REST send is
static (no runtime path binding). Deploy-time verification (apiType preflight,
path collisions, live probe, execution readback) is owned by
``orchestrate_deploy``'s ``listener_verify`` stage.
"""

from __future__ import annotations

from typing import Any, Dict, List, Set

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
    ROLE_REST_CONNECTION,
    ROLE_REST_OPERATION,
    primitive_component_key,
)
from ..primitives.field_map import FieldMapPrimitive
from ..primitives.rest_send import RestSendWithRetryPrimitive
from ..primitives.wss_listen import WssListenPrimitive

# Reuse #73's REST target contract + assembly helpers verbatim (duck-typed on
# ``parameters.target`` / ``parameters.transform`` / ``parameters.naming``).
from .api_to_api_sync import (
    ApiTarget,
    ApiTransformConfig,
    DirectApiTransformOperation,
    MapFunctionApiTransformOperation,
    MapScriptApiTransformOperation,
    _build_field_map_params,
    _build_rest_send_params,
    _script_var_name,
    _TARGET_PREFIX,
)

# Shared listener source contract + assembly helpers (M6 sibling preset).
from .http_listener_to_db import (
    InboundValidationConfig,
    ListenerSource,
    _LISTENER_REQUEST_PROFILE_KEY,
    _build_listener_main_process,
    _build_listener_pipeline_dict,
    _build_listener_request_profile,
    _build_wss_listen_params,
    _listener_endpoint_summary,
    _listener_metadata_block,
    _operation_summaries_listener,
    _run_inbound_validation,
)

# Reuse the sibling archetypes' proven naming contract + secret-safe helpers.
from .database_to_api_sync import (
    NamingConfig,
    _component_names,
    _flatten_payload_profile_leaves,
    _required_simple_leaf_paths,
)


# ---------------------------------------------------------------------------
# Top-level parameters
# ---------------------------------------------------------------------------


class HttpListenerToRestParameters(BaseModel):
    model_config = ConfigDict(extra="forbid")

    naming: NamingConfig = Field(
        ...,
        description="Naming, folder, and runtime-hint configuration for the emitted integration.",
    )
    listener: ListenerSource = Field(
        ...,
        description="Inbound WSS listener: operation shape and JSON request payload profile tree.",
    )
    target: ApiTarget = Field(
        ...,
        description="REST target: connector binding, static send request, and JSON payload profile tree.",
    )
    transform: ApiTransformConfig = Field(
        ...,
        description=(
            "Typed transform operations moving inbound payload leaves into "
            "target payload leaves (direct/map_function/map_script; xslt rejected)."
        ),
    )
    inbound_validation: InboundValidationConfig = Field(
        default_factory=InboundValidationConfig,
        description="Opt-in build-time inbound-contract validation (profile_bound).",
    )

    @model_validator(mode="after")
    def _validate_transform_refs(self) -> "HttpListenerToRestParameters":
        source_leaves: Dict[str, str] = _flatten_payload_profile_leaves(
            self.listener.payload_profile
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
                "listener.payload_profile"
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


# ---------------------------------------------------------------------------
# Archetype
# ---------------------------------------------------------------------------


class HttpListenerToRestArchetype(ArchetypePattern):
    metadata = PatternMetadata(
        name="http_listener_to_rest",
        version="0.1.0",
        kind=PatternKind.ARCHETYPE,
        description=(
            "Archetype for forwarding inbound HTTP payloads to a REST API as a "
            "thin preset over the M5 sync_pipeline: a bare Web Services Server "
            "listener source (Listen start shape, no connection component, "
            "listener process options locked), an optional transform map, and a "
            "REST send target — a verified-linear listener -> map -> send stage "
            "graph. The endpoint is /ws/simple/{operationtype}{SentenceCase(objectName)} on "
            "basic/intermediate runtimes (apiType=advanced needs an API Service "
            "Component, #133). Every byte of XML is produced by the existing "
            "component builders through the wss_listen / field_map / "
            "rest_send_with_retry primitives."
        ),
        tags=["listener", "wss", "inbound", "event", "api", "rest", "sync_pipeline"],
        use_cases=[
            "Receive JSON webhooks and forward mapped payloads to a downstream REST API",
            "Event-triggered fan-in: accept pushed payloads and relay them onward",
        ],
        not_for=[
            "Scheduled/polling sources (use api_to_api_sync / database_to_api_sync)",
            "apiType=advanced runtimes without an API Service Component (#133)",
            "Listener response body mapping (the listener acks with outputType=none)",
            "Runtime-bound REST paths (#96 dynamic path is out of scope here)",
        ],
    )
    parameters_model = HttpListenerToRestParameters

    capability_notes = [
        "Discoverable, fully-typed parameter contract for an inbound-HTTP -> REST integration.",
        "Emits a main process with process_kind='sync_pipeline' and an intact listener -> map -> send stage graph; SyncPipelineBuilder lowers the listener stage to the live-verified Listen start shape (connectoraction inside the start shape, no connection component).",
        "Listener process options are locked by construction: allowSimultaneous='true', updateRunDates='false' (live-captured invariants).",
        "The generated listener request profile is the transform's source shape; the target payload profile is generated and bound as the REST send request body.",
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
        "inbound_validation is a build-time contract (profile binding), not runtime payload validation.",
        "Static REST send only: a '{token}' dynamic path is rejected (runtime path binding is #96).",
        "REST create-mode emits only auth='none'; secured target auth (basic / bearer / oauth2) requires binding.mode='reuse'.",
        "Retry/DLQ and listener response mapping are out of scope for this preset.",
    ]

    examples = [
        PatternExample(
            name="webhook_relay_to_rest",
            description=(
                "Receive a JSON webhook on /ws/simple/executeEventRelay and POST "
                "the mapped payload to a downstream REST API, as a "
                "process_kind='sync_pipeline' listener -> map -> send graph."
            ),
            parameters={
                "naming": {
                    "integration_name": "Event Relay Listener",
                    "component_prefix": "EventRelay",
                },
                "listener": {
                    "object_name": "eventRelay",
                    "operation_type": "EXECUTE",
                    "payload_profile": {
                        "format": "json",
                        "root": {
                            "name": "Root",
                            "kind": "object",
                            "children": [
                                {"name": "event_id", "kind": "simple", "data_type": "character"},
                                {"name": "detail", "kind": "simple", "data_type": "character"},
                            ],
                        },
                    },
                },
                "target": {
                    "binding": {"mode": "create", "settings": {"base_url": "https://api.example.com", "auth_mode": "none"}},
                    "send_request": {"method": "POST", "path": "/v1/<<target resource>>"},
                    "payload_profile": {
                        "format": "json",
                        "root": {
                            "name": "Root",
                            "kind": "object",
                            "children": [
                                {"name": "id", "kind": "simple", "data_type": "character"},
                                {"name": "message", "kind": "simple", "data_type": "character"},
                            ],
                        },
                    },
                },
                "transform": {
                    "operations": [
                        {"operation_type": "direct", "source_path": "Root/event_id", "target_path": "Root/id"},
                        {"operation_type": "direct", "source_path": "Root/detail", "target_path": "Root/message"},
                    ]
                },
                "inbound_validation": {"enabled": True},
            },
        )
    ]

    @classmethod
    def emit_spec(cls, parameters: HttpListenerToRestParameters) -> IntegrationSpecV1:
        naming = parameters.naming
        target = parameters.target

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
        target_endpoint: Dict[str, Any] = {
            "key": "rest_target",
            "type": "rest",
            "direction": "target",
            "binding_mode": target.binding.mode,
            "method": target.send_request.method,
            "executable": False,
        }
        if target.binding.mode == "create" and target.binding.settings is not None:
            target_endpoint["auth_mode"] = target.binding.settings.auth_mode
        else:
            if target.binding.component_id:
                target_endpoint["component_id"] = target.binding.component_id
            if target.binding.component_name:
                target_endpoint["component_name"] = target.binding.component_name

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
        target_leaves = _flatten_payload_profile_leaves(target.payload_profile)
        target_profile_summary: Dict[str, Any] = {
            "format": target.payload_profile.format,
            "root_name": target.payload_profile.root.name,
            "leaf_count": len(target_leaves),
            "leaves": [
                {"path": path, "data_type": data_type}
                for path, data_type in sorted(target_leaves.items())
            ],
        }

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
                "name": "Map inbound payload to target payload",
                "source": "listen",
                "target": None,
                "operation": "transform",
                "executable": False,
                "source_schema": source_profile_summary,
                "target_payload_profile": target_profile_summary,
                "operations": _operation_summaries_listener(parameters),
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
            "archetype": "http_listener_to_rest",
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
        # field map generates the target payload profile; rest_send binds it as
        # the request body.
        target_conn_key = primitive_component_key(_TARGET_PREFIX, ROLE_REST_CONNECTION)
        target_op_key = primitive_component_key(_TARGET_PREFIX, ROLE_REST_OPERATION)
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
            FieldMapPrimitive.emit_components(
                context,
                _build_field_map_params(
                    parameters,
                    overrides,
                    response_profile_key=_LISTENER_REQUEST_PROFILE_KEY,
                    source_field_index=listener_field_index,
                ),
            )
        )
        components.extend(
            RestSendWithRetryPrimitive.emit_components(
                context, _build_rest_send_params(parameters, overrides)
            )
        )
        pipeline_dict = _build_listener_pipeline_dict(
            "send", "rest_send", target_conn_key, target_op_key
        )
        # The lowered send stage needs an explicit HTTP method (rest_send has no
        # default) — carry the caller's method onto the stage config.
        for stage in pipeline_dict["stages"]:
            if stage["key"] == "send":
                stage["config"]["action_type"] = target.send_request.method
        components.append(
            _build_listener_main_process(
                parameters,
                overrides,
                pipeline_dict,
                default_name_suffix="HTTP Listener to REST",
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
                "Forward inbound HTTP payloads to a REST API target via an "
                "event-triggered listener -> transform -> send pipeline.",
                "Emit executable component specs whose main process is a "
                "process_kind='sync_pipeline' stage graph (listener -> map -> "
                "send) for build_integration(action='plan'). Deploy-time "
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
                    "rest_target": "static send only; runtime-bound path/query/header is #96 (M5.4a)",
                    "rest_create_auth": "auth='none' only; secured auth requires reuse",
                    "reliability": "retry/DLQ not emitted (listener path is verified-linear in M6)",
                    "map_script": "inline script_body only; external script_component_ref rejected (#51)",
                },
                "profile_schema_strategy": (
                    "M6 uses a caller-supplied JSON payload profile for the "
                    "inbound request body and a caller-supplied JSON payload "
                    "profile for the REST target; no schema introspection or "
                    "response sampling is performed. The request profile is "
                    "generated and bound as the listener input shape."
                ),
                "transform_routes": {
                    "direct": "#26",
                    "map_function": "#40",
                    "map_script": "#41",
                    "xslt": "#42 (rejected)",
                },
            },
        )
