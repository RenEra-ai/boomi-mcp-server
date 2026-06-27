"""Issue #72 (M5.4): ``rest_fetch`` REST source primitive.

The REST source counterpart to :mod:`db_extract`. Materializes the REST
fetch source-component group consumed by an API-sourced flow (``api_to_api_sync``
/ ``api_to_database_sync``):

  1. a REST Client ``connector-settings`` (created from caller config, or a
     reference-only reuse of an existing connection), and
  2. a REST Client ``connector-action`` **execute GET** operation that reads the
     upstream API through an explicit response profile and binds to the
     connection at process time.

It also emits a process *fragment* (``emit_fragment``) describing the REST source
binding plus the explicit output shape, pagination/conditional-request metadata,
and the operation slot declarations.

Scope (M5.4, #72): #72 declares the param/header/path slots. Issue #96 (M5.4a)
adds the ``runtime_bindings`` that fill those slots: a **path** binding lowers into
the live-proven ``dynamic_path`` block (Set Properties DDP + connector-step "Path"
dynamic operation property), which the process-flow builder emits as non-empty
``dynamicProperties``; query/header bindings (and ddp/dpp path sources) are validated
and carried as ``pending_live_verify`` metadata until QA captures the exact REST
Client dynamic operation property XML — never a guessed emission. Pagination and
conditional-request behavior are carried as validated config/metadata, never canned
request templates or process loops.

Like the other source/transform primitives, this emits JSON
``IntegrationComponentSpec`` objects only — every byte of XML and all structured
validation is delegated to the existing ``RestClientConnectionBuilder`` /
``RestClientOperationBuilder``. It does not author REST paths, payloads, or call
any live Boomi API. The GET request body is empty, so this primitive sets no
request-profile fields; under #50 conditional emission the REST operation builder
therefore emits no ``requestProfileType`` attr. It does carry the response profile
id+type (a fetch needs an output profile), so ``responseProfile`` /
``responseProfileType`` are emitted from those explicit values.
"""

from __future__ import annotations

import re
from typing import Any, Dict, List, Literal, Optional

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
    BuilderValidationError,
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
    ROLE_REST_SOURCE_CONNECTION,
    ROLE_REST_SOURCE_OPERATION,
    _key_looks_secret,
    primitive_component_key,
    raise_for_builder_error,
    ref_key,
    slugify,
    value_looks_secret,
)
from .rest_runtime import (
    OperationSlot,
    RuntimeBinding,
    path_bindings_to_dynamic_path,
    pending_runtime_bindings,
    synth_path_replacements,
    validate_runtime_bindings,
)
from .rest_send import RestConnection, RestSendComponentNames

# Primitive-layer error code for an invalid operation-slot declaration (no
# builder owns the slot vocabulary — it is recorded as #96 metadata only).
REST_FETCH_SLOT_INVALID = "REST_FETCH_SLOT_INVALID"
# Primitive-layer error code for an invalid pagination / conditional-request
# metadata combination.
REST_FETCH_METADATA_INVALID = "REST_FETCH_METADATA_INVALID"

# `{token}` placeholders in an operation path (e.g. /v1/items/{id}/notes/{noteId}).
_PATH_TOKEN_RE = re.compile(r"\{([^{}]+)\}")

# Pagination fields each mode is allowed to carry (besides mode/max_pages).
_PAGINATION_MODE_FIELDS: Dict[str, frozenset] = {
    "none": frozenset(),
    "page": frozenset({"page_parameter", "page_size_parameter", "page_size", "initial_page"}),
    "offset": frozenset({"offset_parameter", "limit_parameter", "limit", "initial_offset"}),
    "cursor": frozenset({"cursor_parameter", "next_cursor_path", "initial_cursor"}),
    "link_header": frozenset({"header_name", "relation"}),
}
_PAGINATION_ALL_MODE_FIELDS: frozenset = frozenset(
    field for fields in _PAGINATION_MODE_FIELDS.values() for field in fields
)
# Required driver field(s) per pagination mode.
_PAGINATION_REQUIRED_FIELDS: Dict[str, List[str]] = {
    "page": ["page_parameter"],
    "offset": ["offset_parameter"],
    "cursor": ["cursor_parameter", "next_cursor_path"],
    "link_header": [],
}


def _slot_error(message: str, field: str, **details: Any) -> BuilderValidationError:
    return BuilderValidationError(
        message,
        error_code=REST_FETCH_SLOT_INVALID,
        field=field,
        hint=(
            "Declare each slot once per location with a non-secret name; path "
            "slots must match a '{token}' in operation.path, and a static "
            "query/header entry may not reuse a declared dynamic slot name. "
            "Runtime slot values are bound by #96 (M5.4a)."
        ),
        details=details or None,
    )


def _metadata_error(message: str, field: str, **details: Any) -> BuilderValidationError:
    return BuilderValidationError(
        message,
        error_code=REST_FETCH_METADATA_INVALID,
        field=field,
        hint=(
            "Pagination/conditional-request behavior is recorded as validated "
            "metadata only (no request templates). Set only the fields its mode "
            "requires and leave others unset."
        ),
        details=details or None,
    )


# ---------------------------------------------------------------------------
# Parameter models (strict — extra keys rejected at the boundary, which also
# blocks secret-shaped top-level keys before the builder secret scan runs).
# ---------------------------------------------------------------------------


class RestFetchOperationParams(BaseModel):
    """REST Client GET source-operation parameters (validated by the builder).

    No ``method`` field — a #72 fetch is GET-only (forced to ``"GET"`` before the
    builder runs). No request-profile fields — the GET request body is guaranteed
    empty, so the #50 emission freeze is honored by never setting them.
    """

    model_config = ConfigDict(extra="forbid")

    path: str = Field(..., description="Endpoint path appended to the connection base_url; stored verbatim")
    query_parameters: Optional[Dict[str, str]] = Field(default=None)
    request_headers: Optional[Dict[str, str]] = Field(default=None)
    follow_redirects: Optional[str] = Field(default=None, description="NONE | STRICT | LAX")
    # StrictBool: reject string/int (e.g. "false", 1) at the param boundary so
    # they cannot coerce and bypass RestClientOperationBuilder's non-bool check.
    return_application_errors: Optional[StrictBool] = Field(default=None)
    track_response: Optional[StrictBool] = Field(default=None)

    @model_validator(mode="after")
    def _require_non_blank_path(self) -> "RestFetchOperationParams":
        if not self.path or not self.path.strip():
            raise ValueError("operation.path must be a non-blank endpoint path")
        return self


class RestFetchResponseShape(BaseModel):
    """The required, explicit output shape the fetch source emits downstream.

    A *binding* (profile id + field index), not a generated component — the
    response profile is authored upstream (a sibling primitive or the spec
    author), referenced here by ``$ref`` or a literal UUID. Compatible with
    ``FieldMapPrimitive.SourceBinding`` so the fetch can feed map/transform
    stages through the same pipeline contract.
    """

    model_config = ConfigDict(extra="forbid")

    profile_id: str = Field(
        ..., description="'$ref:KEY' to an in-spec response profile or a literal profile UUID"
    )
    profile_type: Literal["profile.json", "profile.xml"]
    field_index: Dict[str, Dict[str, Any]] = Field(
        ..., description="Per-leaf response index ({path: {data_type, mappable, ...}})"
    )

    @model_validator(mode="after")
    def _require_non_blank_and_non_empty(self) -> "RestFetchResponseShape":
        if not self.profile_id or not self.profile_id.strip():
            raise ValueError("response.profile_id must be a non-blank '$ref:KEY' or profile UUID")
        # A '$ref:' token must name a non-empty key — a bare '$ref:' would publish
        # an unresolvable output-shape reference (ref_key() yields None, so it is
        # neither added to depends_on nor resolvable at apply time).
        stripped = self.profile_id.strip()
        if stripped.startswith("$ref:") and not stripped[len("$ref:"):].strip():
            raise ValueError(
                "response.profile_id '$ref:' token must name a non-empty key ('$ref:KEY')"
            )
        if not self.field_index:
            raise ValueError("response.field_index must be a non-empty per-leaf index")
        return self


class PaginationMeta(BaseModel):
    """Validated pagination metadata (no request templates / process loops).

    Each ``mode`` requires its own driver field(s); all fields are optional on the
    model and a ``model_validator`` rejects fields set under a non-matching mode so
    a stray ``page_parameter`` under ``mode='none'`` can never be silently dropped.
    """

    model_config = ConfigDict(extra="forbid")

    mode: Literal["none", "page", "offset", "cursor", "link_header"] = "none"
    # page
    page_parameter: Optional[str] = None
    page_size_parameter: Optional[str] = None
    page_size: Optional[StrictInt] = Field(default=None, ge=1)
    initial_page: Optional[StrictInt] = Field(default=None, ge=0)
    # offset
    offset_parameter: Optional[str] = None
    limit_parameter: Optional[str] = None
    limit: Optional[StrictInt] = Field(default=None, ge=1)
    initial_offset: Optional[StrictInt] = Field(default=None, ge=0)
    # cursor
    cursor_parameter: Optional[str] = None
    next_cursor_path: Optional[str] = None
    initial_cursor: Optional[str] = None
    # link_header
    header_name: Optional[str] = None
    relation: Optional[str] = None
    # shared bound
    max_pages: Optional[StrictInt] = Field(default=None, ge=1)

    @field_validator(
        "page_parameter", "page_size_parameter", "offset_parameter", "limit_parameter",
        "cursor_parameter", "next_cursor_path", "initial_cursor", "header_name", "relation",
        mode="before",
    )
    @classmethod
    def _blank_str_field_to_none(cls, value: Any) -> Any:
        # Normalize a blank/whitespace-only string to None so a blank pagination
        # field is consistently "unset" — not silently kept out of cross-mode
        # rejection by _set_field() yet still emitted into the fragment (e.g. a
        # blank header_name under mode='none' would otherwise leak through). A
        # blank required driver field then correctly fails its required check.
        if isinstance(value, str):
            return value.strip() or None
        return value

    @model_validator(mode="after")
    def _validate_mode_fields(self) -> "PaginationMeta":
        allowed = _PAGINATION_MODE_FIELDS[self.mode]
        # Reject any mode-specific field set outside the active mode.
        for fname in _PAGINATION_ALL_MODE_FIELDS:
            if fname in allowed:
                continue
            if _set_field(getattr(self, fname)):
                raise _metadata_error(
                    f"pagination.{fname} is not valid under mode={self.mode!r}",
                    field=f"pagination.{fname}",
                    mode=self.mode,
                )
        if self.mode == "none":
            if self.max_pages is not None:
                raise _metadata_error(
                    "pagination.max_pages is not valid under mode='none'",
                    field="pagination.max_pages",
                )
            return self
        # Required driver field(s) per mode.
        for fname in _PAGINATION_REQUIRED_FIELDS[self.mode]:
            if not _nonblank(getattr(self, fname)):
                raise _metadata_error(
                    f"pagination.{fname} is required under mode={self.mode!r}",
                    field=f"pagination.{fname}",
                    mode=self.mode,
                )
        return self

    def resolved(self) -> Dict[str, Any]:
        """Materialize the metadata with mode-appropriate defaults.

        ``link_header`` mode carries default ``header_name="Link"`` /
        ``relation="next"`` so #96 knows which response header + link relation to
        follow even when the caller omits them. (The fields cannot default on the
        model itself — a static default would make the cross-mode validator reject
        them under every other mode.) Other modes dump verbatim.
        """
        dumped = self.model_dump(exclude_none=True)
        if self.mode == "link_header":
            dumped.setdefault("header_name", "Link")
            dumped.setdefault("relation", "next")
        return dumped


class ConditionalRequestMeta(BaseModel):
    """Validated conditional-request (ETag / Last-Modified) metadata.

    Recorded as metadata only — no runtime state binding in #72 (that is #96).
    """

    model_config = ConfigDict(extra="forbid")

    enabled: bool = False
    validator: Optional[Literal["etag", "last_modified"]] = None
    request_header: Optional[str] = None
    response_header: Optional[str] = None
    state_key: Optional[str] = None
    on_not_modified: Optional[Literal["skip", "emit_empty"]] = None

    @model_validator(mode="after")
    def _validate_conditional(self) -> "ConditionalRequestMeta":
        if not self.enabled:
            for fname in ("validator", "request_header", "response_header", "state_key", "on_not_modified"):
                if getattr(self, fname) is not None:
                    raise _metadata_error(
                        f"conditional_request.{fname} requires conditional_request.enabled=true",
                        field=f"conditional_request.{fname}",
                    )
            return self
        if self.validator is None:
            raise _metadata_error(
                "conditional_request.validator (etag|last_modified) is required when enabled",
                field="conditional_request.validator",
            )
        # Header names must be non-blank and never secret-shaped.
        for fname in ("request_header", "response_header"):
            value = getattr(self, fname)
            if value is None:
                continue
            if not value.strip():
                raise _metadata_error(
                    f"conditional_request.{fname} must be a non-blank header name",
                    field=f"conditional_request.{fname}",
                )
            if _key_looks_secret(value) or value_looks_secret(value):
                raise _metadata_error(
                    f"conditional_request.{fname} {value!r} looks secret-shaped",
                    field=f"conditional_request.{fname}",
                )
        return self

    def resolved(self) -> Dict[str, Any]:
        """Materialize the metadata with validator-default header names."""
        if not self.enabled:
            return {"enabled": False}
        defaults = {
            "etag": {"request_header": "If-None-Match", "response_header": "ETag"},
            "last_modified": {
                "request_header": "If-Modified-Since",
                "response_header": "Last-Modified",
            },
        }[self.validator]
        out: Dict[str, Any] = {
            "enabled": True,
            "validator": self.validator,
            "request_header": self.request_header or defaults["request_header"],
            "response_header": self.response_header or defaults["response_header"],
            "on_not_modified": self.on_not_modified or "skip",
        }
        if self.state_key is not None:
            out["state_key"] = self.state_key
        return out


class RestFetchParameters(BaseModel):
    model_config = ConfigDict(extra="forbid")

    key_prefix: str = Field(..., description="Stable key prefix for deterministic component keys")
    connection: RestConnection
    operation: RestFetchOperationParams
    response: RestFetchResponseShape
    path_slots: Optional[List[OperationSlot]] = Field(default=None)
    query_parameter_slots: Optional[List[OperationSlot]] = Field(default=None)
    request_header_slots: Optional[List[OperationSlot]] = Field(default=None)
    # Issue #96 (M5.4a): runtime values bound into the declared slots above. Path
    # bindings lower into the live-proven dynamic_path block; query/header bindings
    # are validated and carried as pending_live_verify metadata.
    runtime_bindings: Optional[List[RuntimeBinding]] = Field(default=None)
    pagination: PaginationMeta = Field(default_factory=PaginationMeta)
    conditional_request: ConditionalRequestMeta = Field(
        default_factory=ConditionalRequestMeta
    )
    component_names: RestSendComponentNames = Field(default_factory=RestSendComponentNames)

    @model_validator(mode="after")
    def _validate_slots(self) -> "RestFetchParameters":
        path_tokens = set(_PATH_TOKEN_RE.findall(self.operation.path or ""))
        self._validate_slot_location(
            "path_slots", self.path_slots, path_tokens=path_tokens, static_keys=None
        )
        self._validate_slot_location(
            "query_parameter_slots",
            self.query_parameter_slots,
            path_tokens=None,
            static_keys=set((self.operation.query_parameters or {}).keys()),
        )
        self._validate_slot_location(
            "request_header_slots",
            self.request_header_slots,
            path_tokens=None,
            static_keys=set((self.operation.request_headers or {}).keys()),
        )
        # Issue #96: validate each runtime binding against the declared slots.
        validate_runtime_bindings(
            self.runtime_bindings,
            path_slots=self.path_slots,
            query_parameter_slots=self.query_parameter_slots,
            request_header_slots=self.request_header_slots,
            path_tokens=path_tokens,
        )
        return self

    @staticmethod
    def _validate_slot_location(
        field: str,
        slots: Optional[List[OperationSlot]],
        *,
        path_tokens: Optional[set],
        static_keys: Optional[set],
    ) -> None:
        if not slots:
            return
        seen: set = set()
        for slot in slots:
            name = slot.name
            if not name or not name.strip():
                raise _slot_error(f"{field} entry has a blank name", field=field)
            if name in seen:
                raise _slot_error(
                    f"{field} declares duplicate slot name {name!r}", field=field, offending_name=name
                )
            seen.add(name)
            if _key_looks_secret(name):
                raise _slot_error(
                    f"{field} slot name {name!r} looks secret-shaped",
                    field=field,
                    offending_name=name,
                )
            if path_tokens is not None and name not in path_tokens:
                raise _slot_error(
                    f"path_slots name {name!r} does not match a '{{token}}' in operation.path",
                    field=field,
                    offending_name=name,
                )
            if static_keys is not None and name in static_keys:
                raise _slot_error(
                    f"{field} slot {name!r} duplicates a static {field.split('_')[0]} entry",
                    field=field,
                    offending_name=name,
                )


# ---------------------------------------------------------------------------
# Primitive
# ---------------------------------------------------------------------------


class RestFetchPrimitive(PrimitivePattern):
    """Emit the REST fetch source-component group plus a process source fragment."""

    metadata = PatternMetadata(
        name="rest_fetch",
        version="1.0.0",
        kind=PatternKind.PRIMITIVE,
        description=(
            "Materialize the REST fetch source group (connection, execute GET "
            "operation) from caller config and emit a process source fragment "
            "with an explicit output shape. Declares operation param/header/path "
            "slots (#72) and binds them with runtime_bindings (#96): a path "
            "binding lowers into the live-proven dynamic_path block the builder "
            "emits as dynamicProperties; query/header bindings are validated and "
            "carried as pending_live_verify metadata. Emits JSON component specs "
            "for the existing REST Client builders; never authors paths or calls "
            "a live API."
        ),
        tags=["source", "rest", "fetch"],
        use_cases=[
            "Read records from a REST API source via a created connection",
            "Reuse an existing REST connection as a fetch source",
            "Bind a per-document REST path via a profile-field runtime binding (#96)",
        ],
        not_for=[
            "Database or file sources (use db_extract)",
            "REST writes / non-GET methods (fetch is GET-only in M5.4)",
            "Query/header or DDP/DPP runtime XML emission (validated but pending live verify, #96)",
        ],
    )
    parameters_model = RestFetchParameters

    output_contract = PatternIOContract(
        name="rest_fetch_result",
        description="REST source binding (connection + GET operation) and explicit output shape.",
        schema_={
            "type": "object",
            "properties": {
                "rest_connection_key": {"type": "string"},
                "rest_operation_key": {"type": "string"},
                "response_profile_id": {"type": "string"},
                "response_profile_type": {"type": "string"},
                "response_field_index": {"type": "object"},
                "source_fragment": {"type": "object"},
            },
            "required": [
                "rest_connection_key",
                "rest_operation_key",
                "response_profile_id",
                "response_profile_type",
                "response_field_index",
            ],
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
        params: RestFetchParameters = parameters  # type: ignore[assignment]

        conn_key = primitive_component_key(params.key_prefix, ROLE_REST_SOURCE_CONNECTION)
        op_key = primitive_component_key(params.key_prefix, ROLE_REST_SOURCE_OPERATION)
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
        params: RestFetchParameters = parameters  # type: ignore[assignment]

        conn_key = primitive_component_key(params.key_prefix, ROLE_REST_SOURCE_CONNECTION)
        op_key = primitive_component_key(params.key_prefix, ROLE_REST_SOURCE_OPERATION)

        source: Dict[str, Any] = {
            "connector_type": "rest",
            "connection_id": f"$ref:{conn_key}",
            "operation_id": f"$ref:{op_key}",
            "action_type": "GET",
            "label": f"{context.component_prefix} REST Fetch",
        }
        # Issue #96: lower path runtime bindings into the live-proven dynamic_path
        # block (Set Properties DDP + connector-step "Path" property). A ddp/dpp
        # path source or an all-static path raises PROCESS_RUNTIME_BINDING_UNVERIFIED
        # — plan-time failure, never a guessed emission. Query/header bindings stay
        # in pending metadata (not yet live-proven for this REST Client subtype).
        dynamic_path = path_bindings_to_dynamic_path(
            params.runtime_bindings,
            path_template=params.operation.path,
            ddp_name=f"{slugify(params.key_prefix)}_path".upper(),
        )
        depends_on = [conn_key, op_key]
        if dynamic_path is not None:
            source["dynamic_path"] = dynamic_path
            # The dynamic_path may reference an in-spec profile ($ref:KEY) for a
            # profile_field path source; that ref must be a process dependency so
            # ProcessFlowBuilder's $ref-reachability check resolves it (#96 review).
            dyn_profile_ref = ref_key(dynamic_path.get("request_profile_id"))
            if dyn_profile_ref and dyn_profile_ref not in depends_on:
                depends_on.append(dyn_profile_ref)
        rest_fetch_meta: Dict[str, Any] = {
            "output_shape": {
                "profile_id": params.response.profile_id,
                "profile_type": params.response.profile_type,
                "field_index": params.response.field_index,
            },
            "pagination": params.pagination.resolved(),
            "conditional_request": params.conditional_request.resolved(),
            "operation_slots": cls._slot_metadata(params),
            # A REST GET inherits the upstream document as its request body
            # and some APIs reject GET-with-body; the fetch guarantees an
            # EMPTY request document (the source connectoraction emits empty
            # <parameters/><dynamicProperties/> when no path binding is set).
            "request_document": "empty",
            # Connector responses generate NEW documents (replace, not merge).
            "response_replaces_document": True,
        }
        if params.runtime_bindings:
            rest_fetch_meta["runtime_bindings"] = [
                b.model_dump(exclude_none=True) for b in params.runtime_bindings
            ]
            pending = pending_runtime_bindings(params.runtime_bindings)
            if pending:
                rest_fetch_meta["runtime_bindings_pending"] = {
                    "emission_status": "pending_live_verify",
                    "bindings": pending,
                }
        fragment: Dict[str, Any] = {
            "process_config": {"source": source},
            "depends_on": depends_on,
            "metadata": {"rest_fetch": rest_fetch_meta},
        }
        return fragment

    # ------------------------------------------------------------------
    # Per-role emission
    # ------------------------------------------------------------------

    @classmethod
    def _emit_connection(
        cls,
        context: PrimitiveBuildContext,
        params: RestFetchParameters,
        conn_key: str,
        folder: Optional[str],
    ) -> IntegrationComponentSpec:
        connection = params.connection

        if connection.mode == "create":
            # Distinct default display name from rest_send's "<prefix> REST
            # Connection" so a same-prefix api_to_api_sync assembly that creates
            # BOTH a rest_fetch source connection and a rest_send target connection
            # does not trip the COMPONENT_NAME_NOT_UNIQUE name-governance lint
            # (the source-specific component key alone is not enough — the emitted
            # display name must differ too). The operation default ("REST Fetch")
            # already differs from rest_send's ("REST Send").
            conn_name = (
                params.component_names.connection
                or f"{context.component_prefix} REST Source Connection"
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
        params: RestFetchParameters,
        op_key: str,
        conn_key: str,
        folder: Optional[str],
    ) -> IntegrationComponentSpec:
        op = params.operation
        op_name = (
            params.component_names.operation
            or f"{context.component_prefix} REST Fetch"
        )
        config: Dict[str, Any] = {
            "connector_type": "rest",
            "operation_mode": "execute",
            "component_name": op_name,
            "connection_ref_key": conn_key,
            # Fetch is GET-only in M5.4; force it regardless of how slots are declared.
            "method": "GET",
            "path": op.path,
            # The response shape is the required explicit output of a fetch source.
            "response_profile_id": params.response.profile_id,
            "response_profile_type": (
                "xml" if params.response.profile_type == "profile.xml" else "json"
            ),
        }
        for field in (
            "query_parameters",
            "request_headers",
            "follow_redirects",
            "return_application_errors",
            "track_response",
        ):
            value = getattr(op, field)
            if value is not None:
                config[field] = value
        if folder:
            config["folder_name"] = folder

        # Issue #96: when a path runtime binding supplies the path at the process
        # step, the operation carries a BLANK path (the per-document path is built
        # by the Set Properties DDP + connector "Path" property the fragment's
        # dynamic_path block drives). The REST operation builder only permits a
        # blank path when a usable path_replacements marker is present, so reuse
        # that #100 marker (synthesized from the path bindings, name<->token
        # coverage checked against the TEMPLATE path below; build-only, not emitted
        # into XML). Gate the marker + blanking on SUCCESSFUL lowering: a ddp/dpp/
        # all-static path source raises PROCESS_RUNTIME_BINDING_UNVERIFIED here (the
        # SAME failure emit_fragment surfaces), so emit_components never produces a
        # blank-path operation with no process dynamic_path to fill it (#96 review).
        path_dynamic = path_bindings_to_dynamic_path(
            params.runtime_bindings,
            path_template=op.path,
            ddp_name=f"{slugify(params.key_prefix)}_path".upper(),
        )
        if path_dynamic is not None:
            config["path_replacements"] = synth_path_replacements(params.runtime_bindings)

        # No request_profile_id / request_profile_type: the GET request body is
        # empty (the #72 empty-request guarantee), so under #50 conditional
        # emission the builder emits no request-profile attrs at all.
        raise_for_builder_error(RestClientOperationBuilder.validate_config(config))

        if path_dynamic is not None:
            config["path"] = ""

        # Each in-spec profile referenced by $ref must appear in depends_on so
        # build_integration orders the profile before the operation and resolves
        # the token. A literal UUID response profile is external (not a dependency).
        depends_on = [conn_key]
        response_ref = ref_key(params.response.profile_id)
        if response_ref:
            depends_on.append(response_ref)

        return IntegrationComponentSpec(
            key=op_key,
            type="connector-action",
            action="create",
            name=op_name,
            config=config,
            depends_on=depends_on,
        )

    # ------------------------------------------------------------------
    # Slot metadata (#96 consumes this; #72 only records it)
    # ------------------------------------------------------------------

    @staticmethod
    def _slot_metadata(params: RestFetchParameters) -> Dict[str, List[Dict[str, Any]]]:
        def dump(slots: Optional[List[OperationSlot]]) -> List[Dict[str, Any]]:
            return [slot.model_dump(exclude_none=True) for slot in (slots or [])]

        return {
            "path": dump(params.path_slots),
            "query_parameter": dump(params.query_parameter_slots),
            "request_header": dump(params.request_header_slots),
        }


# ---------------------------------------------------------------------------
# Local helpers
# ---------------------------------------------------------------------------


def _nonblank(value: Any) -> bool:
    if isinstance(value, str):
        return bool(value.strip())
    return value is not None


def _set_field(value: Any) -> bool:
    """True when an optional field carries a meaningful (non-None/non-blank) value."""
    if value is None:
        return False
    if isinstance(value, str):
        return bool(value.strip())
    return True
