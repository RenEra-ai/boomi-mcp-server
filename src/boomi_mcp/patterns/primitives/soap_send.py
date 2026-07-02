"""Issue #126: ``soap_send`` target primitive.

Materializes the SOAP Client target group consumed by a sync_pipeline
``soap_send`` stage:

  1. a SOAP Client ``connector-settings`` (created from caller config, or a
     reference-only reuse of an existing connection), and
  2. a SOAP Client ``connector-action`` EXECUTE operation that binds to the
     connection at process time and references XML request/response profiles.

It also emits a process *target* fragment (``emit_fragment``). The primitive is
a thin adapter over the semantic pipeline model — the request payload is
supplied by the surrounding map/pipeline stage via the XML request profile, NOT
as a canned SOAP envelope. Every byte of XML and all structured validation is
delegated to the existing ``SoapClientConnectionBuilder`` /
``SoapClientOperationBuilder``; no live Boomi API is called.
"""

from __future__ import annotations

from typing import Any, Dict, List

from pydantic import BaseModel, ConfigDict, Field

from ...models.integration_models import IntegrationComponentSpec
from ..base import (
    PatternIOContract,
    PatternKind,
    PatternMetadata,
    PrimitiveBuildContext,
    PrimitivePattern,
)
from ._helpers import ROLE_SOAP_CONNECTION, ROLE_SOAP_OPERATION, primitive_component_key
from ._soap_common import (
    SOAP_CONNECTOR_ALIAS,
    SoapComponentNames,
    SoapConnection,
    SoapOperationParams,
    emit_soap_connection,
    emit_soap_operation,
)


class SoapSendParameters(BaseModel):
    model_config = ConfigDict(extra="forbid")

    key_prefix: str = Field(..., description="Stable key prefix for deterministic component keys")
    connection: SoapConnection
    operation: SoapOperationParams
    component_names: SoapComponentNames = Field(default_factory=SoapComponentNames)


class SoapSendPrimitive(PrimitivePattern):
    """Emit the SOAP Client target group plus a process target fragment."""

    metadata = PatternMetadata(
        name="soap_send",
        version="1.0.0",
        kind=PatternKind.PRIMITIVE,
        description=(
            "Materialize the SOAP Client target group (connection, EXECUTE "
            "operation) from caller config and emit a process target fragment for "
            "sync_pipeline assembly. The SOAP Client exposes a single outbound "
            "EXECUTE action; the request body is supplied by the surrounding "
            "map/pipeline stage via the XML request profile, never as a canned "
            "SOAP envelope. Emits JSON component specs for the existing SOAP Client "
            "builders; WSDL discovery is out of scope (caller-provided metadata)."
        ),
        tags=["target", "soap", "send"],
        use_cases=[
            "Send mapped records to a SOAP web service via a created connection",
            "Reuse an existing SOAP connection as a send target",
        ],
        not_for=[
            "REST, database, or file targets",
            "Authoring SOAP envelopes / request payloads (supplied by the map stage)",
            "WSDL discovery / browsing (out of scope, #126)",
        ],
    )
    parameters_model = SoapSendParameters

    output_contract = PatternIOContract(
        name="soap_send_result",
        description="SOAP target binding (connection + EXECUTE operation) and process target fragment.",
        schema_={
            "type": "object",
            "properties": {
                "soap_connection_key": {"type": "string"},
                "soap_operation_key": {"type": "string"},
                "target_fragment": {"type": "object"},
            },
            "required": ["soap_connection_key", "soap_operation_key"],
        },
    )
    required_builders = [
        "SoapClientConnectionBuilder",
        "SoapClientOperationBuilder",
    ]

    @classmethod
    def emit_components(
        cls,
        context: PrimitiveBuildContext,
        parameters: BaseModel,
    ) -> List[IntegrationComponentSpec]:
        params: SoapSendParameters = parameters  # type: ignore[assignment]
        conn_key = primitive_component_key(params.key_prefix, ROLE_SOAP_CONNECTION)
        op_key = primitive_component_key(params.key_prefix, ROLE_SOAP_OPERATION)
        folder = context.folder_path

        connection = emit_soap_connection(
            context,
            params.connection,
            conn_key,
            folder,
            name_override=params.component_names.connection,
            default_name=f"{context.component_prefix} SOAP Connection",
        )
        operation = emit_soap_operation(
            context,
            params.operation,
            op_key,
            conn_key,
            folder,
            name_override=params.component_names.operation,
            default_name=f"{context.component_prefix} SOAP Send",
        )
        # Deterministic dependency order: connection, then operation.
        return [connection, operation]

    @classmethod
    def emit_fragment(
        cls,
        context: PrimitiveBuildContext,
        parameters: BaseModel,
    ) -> Dict[str, Any]:
        params: SoapSendParameters = parameters  # type: ignore[assignment]
        conn_key = primitive_component_key(params.key_prefix, ROLE_SOAP_CONNECTION)
        op_key = primitive_component_key(params.key_prefix, ROLE_SOAP_OPERATION)

        target: Dict[str, Any] = {
            "connector_type": SOAP_CONNECTOR_ALIAS,
            "connection_id": f"$ref:{conn_key}",
            "operation_id": f"$ref:{op_key}",
            "action_type": "EXECUTE",
            "label": f"{context.component_prefix} SOAP Send",
        }
        return {
            "process_config": {"target": target},
            "depends_on": [conn_key, op_key],
        }
