"""Issue #126: shared parameter models + emit helpers for the SOAP primitives.

``soap_fetch`` (source) and ``soap_send`` (target) both materialize the same
two-component SOAP Client group — a ``connector-settings`` connection and a
single ``connector-action`` EXECUTE operation — and differ only in which
process fragment slot (source vs target) they emit. This module holds the shared
strict parameter models and the per-role emit helpers so each primitive file
stays a thin adapter.

Every byte of XML and all structured validation is delegated to the existing
``SoapClientConnectionBuilder`` / ``SoapClientOperationBuilder``. No canned SOAP
envelopes / payloads, no WSDL discovery, no live Boomi API calls.
"""

from __future__ import annotations

from typing import Annotated, Any, Dict, List, Literal, Optional, Union

from pydantic import BaseModel, ConfigDict, Field, StrictBool, field_validator, model_validator

from ...categories.components.builders.connector_builder import (
    SoapClientConnectionBuilder,
    SoapClientOperationBuilder,
)
from ...models.integration_models import IntegrationComponentSpec
from ..base import PrimitiveBuildContext
from ._helpers import raise_for_builder_error, ref_key

# The SOAP Client alias the fragments and component configs carry (the plan /
# integration builder canonicalizes it to `wssoapclientsdk`).
SOAP_CONNECTOR_ALIAS = "soap_client"


# ---------------------------------------------------------------------------
# Connection parameter models (strict — extra keys rejected at the boundary).
# ---------------------------------------------------------------------------


class SoapConnectionCreate(BaseModel):
    """Create a new SOAP Client connector-settings from caller config.

    Security-mode gating, URL/username presence, credential_ref shape, cert-alias
    UUID checks, and the recursive secret scan are delegated to
    ``SoapClientConnectionBuilder.validate_config`` — this model only fixes the
    accepted key surface so unknown or secret-shaped top-level keys are rejected
    early.
    """

    model_config = ConfigDict(extra="forbid")

    mode: Literal["create"]
    wsdl_url: str = Field(..., description="SOAP service WSDL URL (...?wsdl); maps to the connection `url` field")
    endpoint_url: str = Field(..., description="SOAP service endpoint URL (the POST address); maps to `endpoint`")
    security: Optional[str] = Field(default=None, description="Connection security mode (only NETWORK_AUTH for v1)")
    username: Optional[str] = Field(default=None)
    credential_ref: Optional[str] = Field(
        default=None,
        description="Opaque credential reference (credential://...); never written to XML",
    )
    client_ssl_alias: Optional[str] = Field(default=None, description="Boomi client certificate component id (UUID)")
    trust_ssl_alias: Optional[str] = Field(default=None, description="Boomi trust certificate component id (UUID)")
    description: Optional[str] = Field(default=None)


class SoapConnectionReuse(BaseModel):
    """Reference an existing SOAP connection without mutating it."""

    model_config = ConfigDict(extra="forbid")

    mode: Literal["reuse"]
    component_id: Optional[str] = Field(default=None, description="Existing connector-settings component id")
    component_name: Optional[str] = Field(
        default=None, description="Existing connector-settings display name (resolved to exactly one component)"
    )

    @field_validator("component_id", "component_name", mode="before")
    @classmethod
    def _blank_to_none(cls, value: Any) -> Any:
        if isinstance(value, str):
            stripped = value.strip()
            return stripped or None
        return value

    @model_validator(mode="after")
    def _require_exactly_one_binding(self) -> "SoapConnectionReuse":
        if bool(self.component_id) == bool(self.component_name):
            raise ValueError(
                "reuse connection requires exactly one non-blank component_id or component_name"
            )
        return self


SoapConnection = Annotated[
    Union[SoapConnectionCreate, SoapConnectionReuse],
    Field(discriminator="mode"),
]


# ---------------------------------------------------------------------------
# Operation parameter models (strict). The WSDL metadata is caller-provided
# WSDL-derived data — never a canned SOAP envelope or request payload.
# ---------------------------------------------------------------------------


class WsdlParameter(BaseModel):
    """One SOAP body parameter (WSDL input/output element)."""

    model_config = ConfigDict(extra="forbid")

    name: str
    element_name: str
    element_ns: str
    soap_location: Optional[str] = Field(default=None, description="SOAP location (default 'body')")
    hidden: Optional[StrictBool] = Field(default=None)


class WsdlMetadata(BaseModel):
    """Structured WSDL-derived metadata used to generate the operation cookie.

    Mirrors ``SoapClientOperationBuilder._REQUIRED_WSDL_METADATA_FIELDS``; the
    builder re-validates it (single source of truth for the byte-locked cookie).
    """

    model_config = ConfigDict(extra="forbid")

    operation_name: str
    soap_action: str
    metadata_connection_url: str
    service_name: str
    service_namespace: str
    port_name: str
    binding_style: str
    binding_use: str
    binding_protocol: str
    operation_style: str
    operation_use: str
    input_message_name: str
    input_message_namespace: str
    output_message_name: str
    output_message_namespace: str
    input_parameters: List[WsdlParameter] = Field(..., min_length=1)
    output_parameters: List[WsdlParameter] = Field(..., min_length=1)
    rpc_optional_parameters: Optional[StrictBool] = Field(default=None)
    using_envelope: Optional[StrictBool] = Field(default=None)


class SoapOperationParams(BaseModel):
    """SOAP Client EXECUTE-operation parameters (validated by the builder)."""

    model_config = ConfigDict(extra="forbid")

    request_profile_id: str = Field(..., description="'$ref:KEY' or literal XML profile UUID for the request")
    response_profile_id: str = Field(..., description="'$ref:KEY' or literal XML profile UUID for the response")
    request_profile_type: Optional[str] = Field(default=None, description="xml (only)")
    response_profile_type: Optional[str] = Field(default=None, description="xml (only)")
    object_type_id: Optional[str] = Field(default=None, description="defaults to the WSDL operation name")
    object_type_name: Optional[str] = Field(default=None, description="defaults to the WSDL operation name")
    return_application_errors: Optional[StrictBool] = Field(default=None)
    track_response: Optional[StrictBool] = Field(default=None)
    expose_request_envelope: Optional[StrictBool] = Field(default=None)
    expose_response_envelope: Optional[StrictBool] = Field(default=None)
    attachment_cache_id: Optional[str] = Field(default=None, description="Boomi document-cache component id (UUID)")
    wsdl_metadata: WsdlMetadata


class SoapComponentNames(BaseModel):
    """Optional display-name overrides per emitted component role."""

    model_config = ConfigDict(extra="forbid")

    connection: Optional[str] = None
    operation: Optional[str] = None


# ---------------------------------------------------------------------------
# Per-role emit helpers (shared by soap_fetch and soap_send).
# ---------------------------------------------------------------------------


def emit_soap_connection(
    context: PrimitiveBuildContext,
    connection: Union[SoapConnectionCreate, SoapConnectionReuse],
    conn_key: str,
    folder: Optional[str],
    *,
    name_override: Optional[str],
    default_name: str,
) -> IntegrationComponentSpec:
    """Emit the SOAP connector-settings spec (create or reference-only reuse)."""
    if connection.mode == "create":
        conn_name = name_override or default_name
        config: Dict[str, Any] = {
            "connector_type": SOAP_CONNECTOR_ALIAS,
            "component_name": conn_name,
            "wsdl_url": connection.wsdl_url,
            "endpoint_url": connection.endpoint_url,
        }
        for field in ("security", "username", "credential_ref", "client_ssl_alias", "trust_ssl_alias", "description"):
            value = getattr(connection, field)
            if value is not None:
                config[field] = value
        if folder:
            config["folder_name"] = folder
        raise_for_builder_error(SoapClientConnectionBuilder.validate_config(config))
        return IntegrationComponentSpec(
            key=conn_key,
            type="connector-settings",
            action="create",
            name=conn_name,
            config=config,
        )

    # reuse — reference an existing connection without mutating it.
    config = {"reference_only": True, "connector_type": SOAP_CONNECTOR_ALIAS}
    if connection.component_id:
        config["component_id"] = connection.component_id
    if connection.component_name:
        config["component_name"] = connection.component_name
    return IntegrationComponentSpec(
        key=conn_key,
        type="connector-settings",
        action="create",
        name=connection.component_name,
        component_id=connection.component_id,
        config=config,
    )


def emit_soap_operation(
    context: PrimitiveBuildContext,
    operation: SoapOperationParams,
    op_key: str,
    conn_key: str,
    folder: Optional[str],
    *,
    name_override: Optional[str],
    default_name: str,
) -> IntegrationComponentSpec:
    """Emit the SOAP connector-action EXECUTE spec."""
    op_name = name_override or default_name
    config: Dict[str, Any] = {
        "connector_type": SOAP_CONNECTOR_ALIAS,
        "operation_mode": "execute",
        "component_name": op_name,
        "connection_ref_key": conn_key,
        "request_profile_id": operation.request_profile_id,
        "response_profile_id": operation.response_profile_id,
        "wsdl_metadata": operation.wsdl_metadata.model_dump(exclude_none=True),
    }
    for field in (
        "request_profile_type",
        "response_profile_type",
        "object_type_id",
        "object_type_name",
        "return_application_errors",
        "track_response",
        "expose_request_envelope",
        "expose_response_envelope",
        "attachment_cache_id",
    ):
        value = getattr(operation, field)
        if value is not None:
            config[field] = value
    if folder:
        config["folder_name"] = folder

    raise_for_builder_error(SoapClientOperationBuilder.validate_config(config))

    depends_on = [conn_key]
    for profile_ref in (ref_key(operation.request_profile_id), ref_key(operation.response_profile_id)):
        if profile_ref:
            depends_on.append(profile_ref)

    return IntegrationComponentSpec(
        key=op_key,
        type="connector-action",
        action="create",
        name=op_name,
        config=config,
        depends_on=depends_on,
    )
