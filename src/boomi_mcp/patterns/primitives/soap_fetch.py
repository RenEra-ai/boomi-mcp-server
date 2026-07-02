"""Issue #126: ``soap_fetch`` source primitive.

The SOAP counterpart of ``rest_fetch``. Materializes the SOAP Client source
group consumed by a sync_pipeline ``soap_fetch`` stage:

  1. a SOAP Client ``connector-settings`` (created from caller config, or a
     reference-only reuse of an existing connection), and
  2. a SOAP Client ``connector-action`` EXECUTE operation that binds to the
     connection at process time and references XML request/response profiles.

It also emits a process *source* fragment (``emit_fragment``). The SOAP Client
exposes a single outbound EXECUTE action (there is no GET/SEND verb split), so a
``soap_fetch`` source and a ``soap_send`` target share the same EXECUTE
operation shape and differ only in the fragment slot they fill. All XML and
validation is delegated to the existing SOAP Client builders; WSDL discovery is
out of scope and no live Boomi API is called.
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
from ._helpers import (
    ROLE_SOAP_SOURCE_CONNECTION,
    ROLE_SOAP_SOURCE_OPERATION,
    primitive_component_key,
)
from ._soap_common import (
    SOAP_CONNECTOR_ALIAS,
    SoapComponentNames,
    SoapConnection,
    SoapOperationParams,
    emit_soap_connection,
    emit_soap_operation,
)


class SoapFetchParameters(BaseModel):
    model_config = ConfigDict(extra="forbid")

    key_prefix: str = Field(..., description="Stable key prefix for deterministic component keys")
    connection: SoapConnection
    operation: SoapOperationParams
    component_names: SoapComponentNames = Field(default_factory=SoapComponentNames)


class SoapFetchPrimitive(PrimitivePattern):
    """Emit the SOAP Client source group plus a process source fragment."""

    metadata = PatternMetadata(
        name="soap_fetch",
        version="1.0.0",
        kind=PatternKind.PRIMITIVE,
        description=(
            "Materialize the SOAP Client source group (connection, EXECUTE "
            "operation) from caller config and emit a process source fragment for "
            "sync_pipeline assembly — the SOAP counterpart of rest_fetch. The SOAP "
            "Client exposes a single outbound EXECUTE action; the request is driven "
            "by the XML request profile, never a canned SOAP envelope. Emits JSON "
            "component specs for the existing SOAP Client builders; WSDL discovery "
            "is out of scope (caller-provided metadata)."
        ),
        tags=["source", "soap", "fetch"],
        use_cases=[
            "Fetch records from a SOAP web service via a created connection",
            "Reuse an existing SOAP connection as a source",
        ],
        not_for=[
            "REST, database, or file sources",
            "Authoring SOAP envelopes / request payloads (supplied by the map stage)",
            "WSDL discovery / browsing (out of scope, #126)",
        ],
    )
    parameters_model = SoapFetchParameters

    output_contract = PatternIOContract(
        name="soap_fetch_result",
        description="SOAP source binding (connection + EXECUTE operation) and process source fragment.",
        schema_={
            "type": "object",
            "properties": {
                "soap_connection_key": {"type": "string"},
                "soap_operation_key": {"type": "string"},
                "source_fragment": {"type": "object"},
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
        params: SoapFetchParameters = parameters  # type: ignore[assignment]
        conn_key = primitive_component_key(params.key_prefix, ROLE_SOAP_SOURCE_CONNECTION)
        op_key = primitive_component_key(params.key_prefix, ROLE_SOAP_SOURCE_OPERATION)
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
            default_name=f"{context.component_prefix} SOAP Fetch",
        )
        return [connection, operation]

    @classmethod
    def emit_fragment(
        cls,
        context: PrimitiveBuildContext,
        parameters: BaseModel,
    ) -> Dict[str, Any]:
        params: SoapFetchParameters = parameters  # type: ignore[assignment]
        conn_key = primitive_component_key(params.key_prefix, ROLE_SOAP_SOURCE_CONNECTION)
        op_key = primitive_component_key(params.key_prefix, ROLE_SOAP_SOURCE_OPERATION)

        source: Dict[str, Any] = {
            "connector_type": SOAP_CONNECTOR_ALIAS,
            "connection_id": f"$ref:{conn_key}",
            "operation_id": f"$ref:{op_key}",
            "action_type": "EXECUTE",
            "label": f"{context.component_prefix} SOAP Fetch",
        }
        return {
            "process_config": {"source": source},
            "depends_on": [conn_key, op_key],
        }
