"""Issue #28: ``rest_send_with_retry`` target primitive.

Materializes the REST target send-component group consumed by a
``database_to_api_sync`` flow:

  1. a REST Client ``connector-settings`` (created from caller config, or a
     reference-only reuse of an existing connection), and
  2. a REST Client ``connector-action`` execute operation that binds to the
     connection at process time and optionally references request/response
     profiles.

It also emits a process *fragment* (``emit_fragment``) describing the REST
target binding for issue #29 archetype assembly. This primitive's
``retry_policy`` is carried as planning metadata only — it does not drive the
process ``reliability.retry_count``. Process-level Try/Catch retry is wired
separately via the archetype's ``RetryPolicy`` (#88 M4.5.3); this fragment's
retry_policy is representation-only.

The primitive emits JSON ``IntegrationComponentSpec`` objects only — every
byte of XML and all structured validation is delegated to the existing
``RestClientConnectionBuilder`` / ``RestClientOperationBuilder``. It does not
author REST paths, payloads, or call any live Boomi API.
"""

from __future__ import annotations

from typing import Annotated, Any, Dict, List, Literal, Optional, Union

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    StrictBool,
    StrictInt,
    field_validator,
    model_validator,
)

from ...categories.components.builders.connector_builder import (
    RestClientConnectionBuilder,
    RestClientOperationBuilder,
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
    ROLE_REST_CONNECTION,
    ROLE_REST_OPERATION,
    primitive_component_key,
    raise_for_builder_error,
    ref_key,
)


# ---------------------------------------------------------------------------
# Parameter models (strict — extra keys rejected at the boundary, which also
# blocks secret-shaped top-level keys before the builder secret scan runs).
# ---------------------------------------------------------------------------


class RestConnectionCreate(BaseModel):
    """Create a new REST Client connector-settings from caller config.

    Auth-mode gating, base_url shape, OAuth2 sub-block, and recursive secret
    scanning are delegated to ``RestClientConnectionBuilder.validate_config`` —
    this model only fixes the accepted key surface so unknown or secret-shaped
    top-level keys are rejected early.
    """

    model_config = ConfigDict(extra="forbid")

    mode: Literal["create"]
    base_url: str = Field(..., description="REST API base URL (http/https); the operation appends the path")
    auth: str = Field(..., description="Connection auth mode (NONE / BASIC / NTLM / OAUTH2)")
    username: Optional[str] = Field(default=None)
    credential_ref: Optional[str] = Field(
        default=None,
        description="Opaque credential reference (e.g. credential://...); never written to XML",
    )
    domain: Optional[str] = Field(default=None)
    workstation: Optional[str] = Field(default=None)
    # StrictBool: reject string/int (e.g. "false", 1) at the param boundary so
    # they cannot coerce and bypass RestClientConnectionBuilder's non-bool check.
    preemptive: Optional[StrictBool] = Field(default=None)
    private_certificate_ref: Optional[str] = Field(default=None)
    public_certificate_ref: Optional[str] = Field(default=None)
    oauth2: Optional[Dict[str, Any]] = Field(
        default=None, description="OAuth2 sub-block; client_secret_ref must be a credential reference"
    )
    # StrictInt: reject bool/str/float at the param boundary so they cannot
    # coerce (e.g. True->1, "5"->5) and bypass RestClientConnectionBuilder's
    # timeout type check, which would otherwise emit an altered timeout value.
    connect_timeout_ms: Optional[StrictInt] = Field(default=None)
    read_timeout_ms: Optional[StrictInt] = Field(default=None)
    cookie_scope: Optional[str] = Field(default=None)
    connection_pooling: Optional[Dict[str, Any]] = Field(default=None)
    description: Optional[str] = Field(default=None)


class RestConnectionReuse(BaseModel):
    """Reference an existing REST connection without mutating it.

    Resolution-by-``component_name`` trusts the in-spec ``connector_type``
    marker and matches by component metadata type + name; it does not fetch the
    live connector to verify the resolved component is actually a REST
    connector (live subtype verification is a separate discovery concern, not
    covered by issue #47's profile-field inference).
    Prefer ``component_id`` when the exact connection is known.
    """

    model_config = ConfigDict(extra="forbid")

    mode: Literal["reuse"]
    component_id: Optional[str] = Field(
        default=None, description="Existing connector-settings component id"
    )
    component_name: Optional[str] = Field(
        default=None, description="Existing connector-settings display name (resolved to exactly one component)"
    )

    @field_validator("component_id", "component_name", mode="before")
    @classmethod
    def _blank_to_none(cls, value: Any) -> Any:
        # Treat a blank / whitespace-only binding as absent so it cannot pass
        # the exactly-one check below and become a fake component id.
        if isinstance(value, str):
            stripped = value.strip()
            return stripped or None
        return value

    @model_validator(mode="after")
    def _require_exactly_one_binding(self) -> "RestConnectionReuse":
        if bool(self.component_id) == bool(self.component_name):
            raise ValueError(
                "reuse connection requires exactly one non-blank component_id "
                "or component_name"
            )
        return self


RestConnection = Annotated[
    Union[RestConnectionCreate, RestConnectionReuse],
    Field(discriminator="mode"),
]


class RestOperationParams(BaseModel):
    """REST Client execute-operation parameters (validated by the builder)."""

    model_config = ConfigDict(extra="forbid")

    method: str = Field(..., description="HTTP method (GET/POST/PUT/PATCH/DELETE/HEAD/OPTIONS/TRACE)")
    path: str = Field(..., description="Endpoint path appended to the connection base_url; stored verbatim")
    query_parameters: Optional[Dict[str, str]] = Field(default=None)
    request_headers: Optional[Dict[str, str]] = Field(default=None)
    request_profile_id: Optional[str] = Field(
        default=None, description="'$ref:KEY' or literal profile UUID for the request body"
    )
    response_profile_id: Optional[str] = Field(
        default=None, description="'$ref:KEY' or literal profile UUID for the response body"
    )
    request_profile_type: Optional[str] = Field(default=None, description="none | xml | json")
    response_profile_type: Optional[str] = Field(default=None, description="none | xml | json")
    follow_redirects: Optional[str] = Field(default=None, description="NONE | STRICT | LAX")
    # StrictBool: reject string/int (e.g. "false", 1) at the param boundary so
    # they cannot coerce and bypass RestClientOperationBuilder's non-bool check.
    return_application_errors: Optional[StrictBool] = Field(default=None)
    track_response: Optional[StrictBool] = Field(default=None)


class RestRetryPolicy(BaseModel):
    """Planning-only retry metadata for issue #29 assembly.

    Representation only: it is carried in the fragment metadata and does NOT
    flow into the process ``reliability.retry_count``. Process-level Try/Catch
    retry is wired separately through the archetype's ``RetryPolicy`` →
    ``reliability.retry_count`` (#88 M4.5.3); this field stays representational.
    """

    model_config = ConfigDict(extra="forbid")

    # Mirrors the Boomi Try/Catch Retry Count range (0..5) so the recorded
    # intent stays inside what the platform can express. StrictInt rejects
    # bool/str so e.g. max_attempts=True can't coerce to 1.
    max_attempts: Optional[StrictInt] = Field(default=None, ge=0, le=5)
    description: Optional[str] = Field(default=None)


class RestSendComponentNames(BaseModel):
    """Optional display-name overrides per emitted component role."""

    model_config = ConfigDict(extra="forbid")

    connection: Optional[str] = None
    operation: Optional[str] = None


class RestSendWithRetryParameters(BaseModel):
    model_config = ConfigDict(extra="forbid")

    key_prefix: str = Field(..., description="Stable key prefix for deterministic component keys")
    connection: RestConnection
    operation: RestOperationParams
    retry_policy: Optional[RestRetryPolicy] = Field(default=None)
    component_names: RestSendComponentNames = Field(default_factory=RestSendComponentNames)


# ---------------------------------------------------------------------------
# Primitive
# ---------------------------------------------------------------------------


class RestSendWithRetryPrimitive(PrimitivePattern):
    """Emit the REST target send-component group plus a process target fragment."""

    metadata = PatternMetadata(
        name="rest_send_with_retry",
        version="1.0.0",
        kind=PatternKind.PRIMITIVE,
        description=(
            "Materialize the REST target send group (connection, execute "
            "operation) from caller config and emit a process target fragment "
            "for archetype assembly. Emits JSON component specs for the "
            "existing REST Client builders; retry intent is recorded as "
            "planning metadata only and does not enable unverified process "
            "retry/DLQ behavior."
        ),
        tags=["target", "rest", "send"],
        use_cases=[
            "Send mapped records to a REST API target via a created connection",
            "Reuse an existing REST connection as a send target",
        ],
        not_for=[
            "Database or file targets",
            "Process-level retry/DLQ wiring (owned by the archetype RetryPolicy + ProcessFlowBuilder, not this primitive's metadata)",
            "Authoring request payloads, paths, or credentials",
        ],
    )
    parameters_model = RestSendWithRetryParameters

    output_contract = PatternIOContract(
        name="rest_send_result",
        description="REST target binding (connection + execute operation) and process target fragment.",
        schema_={
            "type": "object",
            "properties": {
                "rest_connection_key": {"type": "string"},
                "rest_operation_key": {"type": "string"},
                "target_fragment": {"type": "object"},
            },
            "required": ["rest_connection_key", "rest_operation_key"],
        },
    )
    required_builders = [
        "RestClientConnectionBuilder",
        "RestClientOperationBuilder",
    ]

    @classmethod
    def emit_components(
        cls,
        context: PrimitiveBuildContext,
        parameters: BaseModel,
    ) -> List[IntegrationComponentSpec]:
        params: RestSendWithRetryParameters = parameters  # type: ignore[assignment]

        conn_key = primitive_component_key(params.key_prefix, ROLE_REST_CONNECTION)
        op_key = primitive_component_key(params.key_prefix, ROLE_REST_OPERATION)
        folder = context.folder_path

        connection = cls._emit_connection(context, params, conn_key, folder)
        operation = cls._emit_operation(context, params, op_key, conn_key, folder)

        # Deterministic dependency order: connection, then operation.
        return [connection, operation]

    @classmethod
    def emit_fragment(
        cls,
        context: PrimitiveBuildContext,
        parameters: BaseModel,
    ) -> Dict[str, Any]:
        params: RestSendWithRetryParameters = parameters  # type: ignore[assignment]

        conn_key = primitive_component_key(params.key_prefix, ROLE_REST_CONNECTION)
        op_key = primitive_component_key(params.key_prefix, ROLE_REST_OPERATION)

        target: Dict[str, Any] = {
            "connector_type": "rest",
            "connection_id": f"$ref:{conn_key}",
            "operation_id": f"$ref:{op_key}",
            "action_type": params.operation.method.upper(),
            "label": f"{context.component_prefix} REST Send",
        }
        fragment: Dict[str, Any] = {
            "process_config": {"target": target},
            "depends_on": [conn_key, op_key],
        }
        if params.retry_policy is not None:
            # Planning metadata only — not consumed by ProcessFlowBuilder.
            fragment["metadata"] = {
                "retry_policy": params.retry_policy.model_dump(exclude_none=True)
            }
        return fragment

    # ------------------------------------------------------------------
    # Per-role emission
    # ------------------------------------------------------------------

    @classmethod
    def _emit_connection(
        cls,
        context: PrimitiveBuildContext,
        params: RestSendWithRetryParameters,
        conn_key: str,
        folder: Optional[str],
    ) -> IntegrationComponentSpec:
        # Issue #92 M4.5.7: REST endpoint environment-extension declarations
        # (base URL field id / xpath) are DEFERRED — companion_unverified until a
        # UI-authored REST fixture pins the exact field metadata, and create-mode
        # REST auth is NONE here (no credential to externalize). The DB source
        # connection's credential fields are declared by the archetype instead;
        # see process_flow_builder._emit_process_overrides.
        connection = params.connection

        if connection.mode == "create":
            conn_name = (
                params.component_names.connection
                or f"{context.component_prefix} REST Connection"
            )
            config: Dict[str, Any] = {
                "connector_type": "rest",
                "component_name": conn_name,
                "base_url": connection.base_url,
                "auth": connection.auth,
            }
            for field in (
                "username",
                "credential_ref",
                "domain",
                "workstation",
                "preemptive",
                "private_certificate_ref",
                "public_certificate_ref",
                "oauth2",
                "connect_timeout_ms",
                "read_timeout_ms",
                "cookie_scope",
                "connection_pooling",
                "description",
            ):
                value = getattr(connection, field)
                if value is not None:
                    config[field] = value
            if folder:
                config["folder_name"] = folder
            raise_for_builder_error(RestClientConnectionBuilder.validate_config(config))
            return IntegrationComponentSpec(
                key=conn_key,
                type="connector-settings",
                action="create",
                name=conn_name,
                config=config,
            )

        # reuse — reference an existing connection without mutating it.
        config = {"reference_only": True, "connector_type": "rest"}
        if connection.component_id:
            config["component_id"] = connection.component_id
        if connection.component_name:
            config["component_name"] = connection.component_name
        return IntegrationComponentSpec(
            key=conn_key,
            type="connector-settings",
            action="create",
            # name drives by-name resolution; left None for the id binding.
            name=connection.component_name,
            component_id=connection.component_id,
            config=config,
        )

    @classmethod
    def _emit_operation(
        cls,
        context: PrimitiveBuildContext,
        params: RestSendWithRetryParameters,
        op_key: str,
        conn_key: str,
        folder: Optional[str],
    ) -> IntegrationComponentSpec:
        op = params.operation
        op_name = (
            params.component_names.operation
            or f"{context.component_prefix} REST Send"
        )
        config: Dict[str, Any] = {
            "connector_type": "rest",
            "operation_mode": "execute",
            "component_name": op_name,
            "connection_ref_key": conn_key,
            "method": op.method,
            "path": op.path,
        }
        for field in (
            "query_parameters",
            "request_headers",
            "request_profile_id",
            "response_profile_id",
            "request_profile_type",
            "response_profile_type",
            "follow_redirects",
            "return_application_errors",
            "track_response",
        ):
            value = getattr(op, field)
            if value is not None:
                config[field] = value
        if folder:
            config["folder_name"] = folder

        raise_for_builder_error(RestClientOperationBuilder.validate_config(config))

        # Each in-spec profile referenced by $ref must appear in depends_on so
        # build_integration orders the profile before the operation and
        # resolves the token (_check_rest_operation_dependencies enforces this).
        # Literal UUIDs are external and are not added as dependencies.
        depends_on = [conn_key]
        for profile_ref in (
            ref_key(op.request_profile_id),
            ref_key(op.response_profile_id),
        ):
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
