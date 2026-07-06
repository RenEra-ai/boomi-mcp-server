"""M6 (issue #12): ``wss_listen`` inbound Web Services Server listener primitive.

The first INBOUND source primitive — the event-triggered counterpart to
``db_extract`` / ``rest_fetch``. Materializes the single component a bare-WSS
listener needs:

  1. a Web Services Server ``connector-action`` **Listen** operation
     (``WebServicesServerListenAction``) built through the M6
     ``WssListenerOperationBuilder``.

There is deliberately NO connection component: Boomi's Web Services Server
binds inside the process **start shape** (``connectoraction actionType="Listen"``
with no ``connectionId``), which the process-flow builder emits from the
``listener`` sync_pipeline stage this primitive's fragment feeds.

It also exports the endpoint computation shared with ``orchestrate_deploy``'s
``listener_verify`` stage:

  * bare-WSS path — ``/ws/simple/{lowercase(operationType)}{SentenceCase(objectName)}``
    (Boomi upper-cases the objectName's FIRST letter on the served path while
    the component stores it verbatim — LIVE-SETTLED 2026-07-04 M6 QA on the
    renera local atom: objectName ``qaM6IntakeA`` served
    ``/ws/simple/executeQaM6IntakeA`` with 200 and 404'd the verbatim form), and
  * HTTP method — derived from ``input_type`` (``none`` -> GET, anything else
    -> POST); the method is never set on the operation component.

Like every source primitive, this emits JSON ``IntegrationComponentSpec``
objects only — all XML authoring and structured validation is delegated to the
builder layer. Bare WSS serves basic/intermediate runtimes; ``apiType=advanced``
runtimes route through an API Service Component wrapper (#133) — the listener
archetypes emit one via ``asc_wrapper.enabled=true``, and this module hosts the
shared ``/ws/rest`` endpoint helpers (``compute_asc_endpoint`` et al.).
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional

from pydantic import BaseModel, ConfigDict, Field, model_validator

from ...categories.components.builders.connector_builder import (
    WssListenerOperationBuilder,
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
    ROLE_WSS_LISTENER_OPERATION,
    primitive_component_key,
    raise_for_builder_error,
    ref_key,
)


def sentence_case_object_name(object_name: str) -> str:
    """objectName with its first letter upper-cased — Boomi's served-path
    transformation for bare ``/ws/simple`` routes (the component XML keeps the
    verbatim spelling). LIVE-SETTLED 2026-07-04 (M6 QA, renera local atom,
    intermediate apiType): the companion's sentence-casing claim was right and
    the issue-#12 "verbatim" note was wrong."""
    return object_name[:1].upper() + object_name[1:] if object_name else object_name


def compute_wss_endpoint(operation_type: str, object_name: str) -> str:
    """Bare-WSS endpoint path:
    ``/ws/simple/{lowercase(operationType)}{SentenceCase(objectName)}``.

    Boomi appends the objectName with its FIRST letter upper-cased (no
    separator, rest of the casing preserved) — live-settled 2026-07-04 M6 QA
    (``qaM6IntakeA`` + EXECUTE serves ``/ws/simple/executeQaM6IntakeA``; the
    verbatim form 404s). Shared by the primitive fragment, the listener
    archetypes, and orchestration so the formula exists in exactly one place;
    the listener_verify probe keeps a verbatim-casing 404 fallback as a
    defensive diagnostic.
    """
    name = sentence_case_object_name(str(object_name).strip())
    return f"/ws/simple/{str(operation_type).strip().lower()}{name}"


# Shared WSS/ASC endpoint-formula helpers live in the builders layer (below
# both patterns and deployment) so every consumer imports downward — this
# module re-exports them for pattern-layer callers. See
# categories/components/builders/_api_service_paths.py for the live-grounded
# formula documentation (#133).
from ...categories.components.builders._api_service_paths import (  # noqa: F401
    api_service_http_method,
    compute_asc_endpoint,
    effective_api_service_route,
    normalize_api_service_path_segment,
    wss_http_method,
)


class WssListenComponentNames(BaseModel):
    """Caller display-name overrides for the emitted component."""

    model_config = ConfigDict(extra="forbid")

    operation: Optional[str] = None


class WssListenParameters(BaseModel):
    model_config = ConfigDict(extra="forbid")

    key_prefix: str = Field(..., description="Stable key prefix for deterministic component keys")
    object_name: str = Field(
        ...,
        description=(
            "WSS objectName — stored verbatim on the operation; the served bare "
            "listener endpoint is /ws/simple/{operationtype}{SentenceCase(objectName)} "
            "(Boomi upper-cases its first letter on the path — live-settled "
            "2026-07-04). Use a unique, project-specific name to avoid path "
            "collisions."
        ),
    )
    operation_type: str = Field(
        default="EXECUTE",
        description=(
            "WSS operationType: GET | QUERY | CREATE | UPDATE | UPSERT | DELETE "
            "| EXECUTE. NOT an HTTP verb — the method derives from input_type."
        ),
    )
    input_type: str = Field(
        default="singlejson",
        description=(
            "Inbound document shape: none | singledata | singlejson | multijson "
            "| singlexml | multixml. Also selects the HTTP method (none -> GET, "
            "else POST)."
        ),
    )
    output_type: str = Field(
        default="none",
        description="Response document shape (same vocabulary); 'none' = ack-only 200.",
    )
    response_content_type: str = Field(
        default="text/plain",
        description="application/json | application/xml | text/plain.",
    )
    request_profile_id: Optional[str] = Field(
        default=None,
        description=(
            "'$ref:KEY' to an in-spec JSON/XML profile or a literal profile "
            "UUID. Optional even for JSON input — the live Process Library "
            "listener serves singlejson with no requestProfile (payload read "
            "via DDP)."
        ),
    )
    response_profile_id: Optional[str] = Field(
        default=None,
        description="Response profile ref; only valid when output_type is a JSON/XML type.",
    )
    label: Optional[str] = Field(
        default=None, description="Optional userlabel for the Listen start shape binding."
    )
    component_names: WssListenComponentNames = Field(
        default_factory=WssListenComponentNames
    )

    @model_validator(mode="after")
    def _require_non_blank(self) -> "WssListenParameters":
        if not self.key_prefix or not self.key_prefix.strip():
            raise ValueError("key_prefix must be non-blank")
        if not self.object_name or not self.object_name.strip():
            raise ValueError("object_name must be non-blank")
        for fname in ("request_profile_id", "response_profile_id"):
            value = getattr(self, fname)
            if value is None:
                continue
            stripped = value.strip()
            if not stripped:
                raise ValueError(f"{fname} must be non-blank when set")
            if stripped.startswith("$ref:") and not stripped[len("$ref:"):].strip():
                raise ValueError(f"{fname} '$ref:' token must name a non-empty key ('$ref:KEY')")
        return self


class WssListenPrimitive(PrimitivePattern):
    """Emit the WSS Listen operation component plus a listener source fragment."""

    metadata = PatternMetadata(
        name="wss_listen",
        version="1.0.0",
        kind=PatternKind.PRIMITIVE,
        description=(
            "Materialize the inbound Web Services Server Listen operation "
            "(WebServicesServerListenAction) from caller config and emit a "
            "listener source fragment with the computed bare-WSS endpoint "
            "(/ws/simple/{operationtype}{SentenceCase(objectName)} — Boomi "
            "upper-cases the first objectName letter on the served path, "
            "live-settled 2026-07-04) and derived HTTP method "
            "(input_type none -> GET, else POST). The listener has NO "
            "connection component — it binds inside the process start shape "
            "(actionType='Listen'). Bare WSS serves basic/intermediate "
            "runtimes; apiType=advanced routes only through an API Service "
            "Component wrapper (listener archetypes: asc_wrapper.enabled=true). "
            "Emits JSON component specs for the "
            "WssListenerOperationBuilder; never calls a live API."
        ),
        tags=["source", "listener", "wss", "inbound", "event"],
        use_cases=[
            "Trigger an integration from an inbound HTTP request (web listener)",
            "Receive JSON payloads pushed by an external system",
        ],
        not_for=[
            "Outbound API calls (use rest_fetch / rest_send / soap_*)",
            "apiType=advanced runtimes without an API Service Component (#133)",
            "SOAP inbound endpoints (bare WSS /ws/simple is the M6 scope)",
        ],
    )
    parameters_model = WssListenParameters

    output_contract = PatternIOContract(
        name="wss_listen_result",
        description="WSS Listen operation binding plus computed endpoint metadata.",
        schema_={
            "type": "object",
            "properties": {
                "wss_operation_key": {"type": "string"},
                "object_name": {"type": "string"},
                "operation_type": {"type": "string"},
                "input_type": {"type": "string"},
                "output_type": {"type": "string"},
                "http_method": {"type": "string"},
                "endpoint_path": {"type": "string"},
                "response_content_type": {"type": "string"},
            },
            "required": [
                "wss_operation_key",
                "object_name",
                "operation_type",
                "http_method",
                "endpoint_path",
            ],
        },
    )
    required_builders = ["WssListenerOperationBuilder"]

    @classmethod
    def emit_components(
        cls,
        context: PrimitiveBuildContext,
        parameters: BaseModel,
    ) -> List[IntegrationComponentSpec]:
        params: WssListenParameters = parameters  # type: ignore[assignment]

        op_key = primitive_component_key(params.key_prefix, ROLE_WSS_LISTENER_OPERATION)
        op_name = (
            params.component_names.operation
            or f"{context.component_prefix} Web Listener"
        )
        config: Dict[str, Any] = {
            "connector_type": "wss",
            "operation_mode": "listen",
            "component_name": op_name,
            "object_name": params.object_name.strip(),
            "operation_type": params.operation_type,
            "input_type": params.input_type,
            "output_type": params.output_type,
            "response_content_type": params.response_content_type,
        }
        if params.request_profile_id is not None:
            config["request_profile"] = params.request_profile_id.strip()
        if params.response_profile_id is not None:
            config["response_profile"] = params.response_profile_id.strip()
        if context.folder_path:
            config["folder_name"] = context.folder_path
        raise_for_builder_error(WssListenerOperationBuilder.validate_config(config))

        # Each in-spec profile referenced by $ref must appear in depends_on so
        # build_integration orders the profile before the operation and resolves
        # the token. Literal UUID profiles are external (not dependencies).
        depends_on: List[str] = []
        for profile_value in (params.request_profile_id, params.response_profile_id):
            profile_ref = ref_key(profile_value)
            if profile_ref and profile_ref not in depends_on:
                depends_on.append(profile_ref)

        return [
            IntegrationComponentSpec(
                key=op_key,
                type="connector-action",
                action="create",
                name=op_name,
                config=config,
                depends_on=depends_on,
            )
        ]

    @classmethod
    def emit_fragment(
        cls,
        context: PrimitiveBuildContext,
        parameters: BaseModel,
    ) -> Dict[str, Any]:
        params: WssListenParameters = parameters  # type: ignore[assignment]

        op_key = primitive_component_key(params.key_prefix, ROLE_WSS_LISTENER_OPERATION)
        source: Dict[str, Any] = {
            "connector_type": "wss",
            "action_type": "Listen",
            "operation_id": f"$ref:{op_key}",
            "label": params.label or f"{context.component_prefix} Web Listener",
        }
        operation_type = params.operation_type.strip().upper()
        object_name = params.object_name.strip()
        return {
            "process_config": {"source": source},
            "depends_on": [op_key],
            "metadata": {
                "wss_listen": {
                    "object_name": object_name,
                    "operation_type": operation_type,
                    "input_type": params.input_type.strip().lower(),
                    "output_type": params.output_type.strip().lower(),
                    "response_content_type": params.response_content_type.strip().lower(),
                    "http_method": wss_http_method(params.input_type),
                    "endpoint_path": compute_wss_endpoint(operation_type, object_name),
                    # Listener processes cannot run in Test mode (gotcha KB
                    # listener_wss) — behavioral verification is the deploy +
                    # live probe + execution-record readback in listener_verify.
                    "test_mode_supported": False,
                }
            },
        }
