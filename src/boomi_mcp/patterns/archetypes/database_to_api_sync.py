"""database_to_api_sync archetype (M2.1a contract + M2.9 executable assembly).

Exposes a strict Pydantic parameter contract for a SQL Server source -> REST
target sync. Issue #29 turned this from contract-only output into an executable
assembly: it now composes the shipped #27 (db_extract, field_map) and #28
(rest_send_with_retry + operational) primitives into an executable
IntegrationSpecV1 (DB source, JSON transform, REST target, structured process)
suitable for build_integration(action='plan'). Every byte of XML is produced by
the existing component builders through those primitives; this file emits JSON
component specs only and never calls a live Boomi account. DLQ now wires the
verified Try/Catch + DLQ catch path for modes document_cache_ref /
error_subprocess_ref (process retry_count=0; #51 M3.R1a). Caller retry
(max_attempts>1), schedule activation, watermark update, and dynamic
operation-property wiring remain represented as metadata only and are deferred
(#51 R1b / M3).

M2.1a (issue #44) replaces the legacy ``transform.mappings`` /
``transform.payload_template`` / ``transform.script_slots`` surface with:

  * caller-declared DB read result fields under ``source.read_operation.result_schema``,
  * caller-supplied JSON profile tree under ``target.payload_profile``, and
  * discriminated typed transform operations under ``transform.operations``
    (``direct`` -> #26, ``map_function`` -> #40, ``map_script`` -> #41;
    ``xslt`` is rejected with a pointer to #42).

The archetype does not parse SQL, browse the database, sample rows, infer
schema, or import existing integrations. Read-only profile-field inference from
supplied metadata / sample JSON / XSD / sample XML is available separately via
infer_profile_fields (issue #47); integration import is issue #48; live SQL
parsing / DB browse / row sampling remain out of scope.
"""

from __future__ import annotations

import re
from typing import Annotated, Any, Dict, List, Literal, Optional, Set, Tuple, Union

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    ValidationError,
    field_validator,
    model_validator,
)

from ...categories.components.builders.connector_builder import (
    BuilderValidationError,
)
from ...categories.components.builders.profile_generation import (
    build_profile_generation_artifacts,
)
from ...models.integration_models import (
    IntegrationComponentSpec,
    IntegrationSpecV1,
)
from ..base import (
    ArchetypePattern,
    PatternExample,
    PatternKind,
    PatternMetadata,
    PrimitiveBuildContext,
)
from ..primitives._helpers import (
    ROLE_DB_CONNECTION,
    ROLE_DB_GET_OPERATION,
    ROLE_DB_READ_PROFILE,
    ROLE_REST_CONNECTION,
    ROLE_REST_OPERATION,
    ROLE_SCRIPT,
    ROLE_TARGET_PROFILE,
    ROLE_TRANSFORM_MAP,
    primitive_component_key,
)
from ..primitives.db_extract import DbExtractParameters, DbExtractPrimitive
from ..primitives.field_map import FieldMapParameters, FieldMapPrimitive
from ..primitives.operational import (
    ErrorClassifierParameters,
    ErrorClassifierPrimitive,
    RunMetadataParameters,
    RunMetadataPrimitive,
    ScheduleEnvelopeParameters,
    ScheduleEnvelopePrimitive,
    WatermarkStateParameters,
    WatermarkStatePrimitive,
)
from ..primitives.rest_send import (
    RestSendWithRetryParameters,
    RestSendWithRetryPrimitive,
)

# ---------------------------------------------------------------------------
# Assembly constants (issue #29)
# ---------------------------------------------------------------------------

# Stable primitive key prefixes — the emitted component keys are
# ``{prefix}_{role}`` (e.g. ``source_db_connection``, ``transform_transform_map``,
# ``target_rest_operation``). The archetype assembles its $ref wiring from these
# deterministic keys, so they must stay stable across releases.
_SOURCE_PREFIX = "source"
_TRANSFORM_PREFIX = "transform"
_TARGET_PREFIX = "target"
_MAIN_PROCESS_KEY = "main_process"

# REST create-mode auth: M2 only emits an unauthenticated created connection.
# Secured auth (basic / bearer / oauth2) requires an existing connection via
# binding.mode='reuse' — the contract carries no username, OAuth2 sub-block, or
# bearer header surface, and the REST Client builder rejects those modes without
# them. The error code mirrors RestClientConnectionBuilder's vocabulary.
_REST_CREATE_AUTH_MAP = {"none": "NONE"}
UNSUPPORTED_REST_AUTH_MODE = "UNSUPPORTED_REST_AUTH_MODE"

# A map_script's script_component_ref points at a script component that the
# archetype does not (and cannot) emit into the spec, so the planned spec would
# carry a dangling dependency. M2 materializes scripts only via inline
# script_body; external script-component reuse is deferred (#51).
UNSUPPORTED_SCRIPT_COMPONENT_REF = "UNSUPPORTED_SCRIPT_COMPONENT_REF"


# ---------------------------------------------------------------------------
# Reusable validators
# ---------------------------------------------------------------------------


_URL_RE = re.compile(r"^https?://\S+$", re.IGNORECASE)


def _stripped_nonblank(value: str) -> str:
    stripped = value.strip()
    if not stripped:
        raise ValueError("must not be blank")
    return stripped


# Secret-shape substring list. Mirrors
# src/boomi_mcp/categories/components/builders/process_flow_builder.py's
# FORBIDDEN_SECRET_FIELDS verbatim — case-insensitive substring match catches
# camelCase, snake-prefixed, and SCREAMING-CASE variants. credential_ref and
# similar *_ref keys carry opaque URI references and are intentionally NOT in
# this list. Codex review r1 P2: map_function.parameters is the only
# schema-opaque dict the archetype echoes back into IntegrationSpecV1 on
# success, so plaintext secret keys must be rejected at parameter-validation
# time before they can leak through the spec.
_FORBIDDEN_SECRET_KEY_SUBSTRINGS = (
    "password",
    "passcode",
    "secret",
    "private_key",
    "api_key",
    "apikey",
    "api-key",
    "auth_token",
    "access_token",
    "client_secret",
    "token",
    "authorization",
    "bearer",
    "credentials",
)


def _key_matches_secret_shape(key: Any) -> Optional[str]:
    """Return the matched forbidden substring or None."""
    if not isinstance(key, str):
        return None
    lowered = key.lower()
    for forbidden in _FORBIDDEN_SECRET_KEY_SUBSTRINGS:
        if forbidden in lowered:
            return forbidden
    return None


def _scan_for_secret_shaped_keys(value: Any) -> bool:
    """Recursively walk dict/list containers; True iff any dict key (at any
    depth) matches a forbidden substring. Used by map_function.parameters
    validation to reject plaintext secret-shaped keys before they reach the
    emitted IntegrationSpec."""
    if isinstance(value, dict):
        for key, sub in value.items():
            if _key_matches_secret_shape(key) is not None:
                return True
            if _scan_for_secret_shaped_keys(sub):
                return True
    elif isinstance(value, list):
        for item in value:
            if _scan_for_secret_shaped_keys(item):
                return True
    return False


# ---------------------------------------------------------------------------
# Naming
# ---------------------------------------------------------------------------


class NamingConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    integration_name: str = Field(
        ...,
        description=(
            "Logical integration name; used as the emitted IntegrationSpecV1.name "
            "and as the human-facing label for downstream component naming."
        ),
    )
    component_prefix: str = Field(
        ...,
        description=(
            "Prefix applied to every emitted Boomi component's default display "
            "name (e.g. '<prefix> DB Connection'). Recorded under "
            "spec.naming.component_prefix. Per-role overrides via component_names "
            "take precedence over the prefixed default."
        ),
    )
    component_names: Optional[Dict[str, str]] = Field(
        default=None,
        description=(
            "Optional explicit overrides for individual component display names, "
            "keyed by component role (e.g. 'db_connection', 'rest_connection'). "
            "Values that pass this contract are surfaced verbatim under "
            "spec.naming.component_names; the contract does not assign defaults."
        ),
    )
    folder_path: Optional[str] = Field(
        default=None,
        description=(
            "Optional Boomi folder path under which components will be created "
            "by future executable builders (e.g. 'Integrations/CRM/Sync'). "
            "Echoed in spec.folders.path without normalization."
        ),
    )
    runtime_hints: Optional[Dict[str, Any]] = Field(
        default=None,
        description=(
            "Optional free-form runtime hints (e.g. atom selection, environment "
            "tags). Surfaced verbatim under spec.runtime; the contract does not "
            "interpret keys."
        ),
    )

    @field_validator("integration_name", "component_prefix")
    @classmethod
    def _strip_required_strings(cls, value: str) -> str:
        return _stripped_nonblank(value)

    @field_validator("folder_path")
    @classmethod
    def _strip_optional_folder(cls, value: Optional[str]) -> Optional[str]:
        if value is None:
            return None
        return _stripped_nonblank(value)


# ---------------------------------------------------------------------------
# Source result schema — DB read output fields (caller-declared, M2.1a)
# ---------------------------------------------------------------------------


class DBResultField(BaseModel):
    model_config = ConfigDict(extra="forbid")

    name: str = Field(
        ...,
        description=(
            "Logical field name produced by the DB read operation. The contract "
            "never parses the read SQL or browses the database; every output "
            "field consumed by downstream profile/map builders must be declared "
            "here."
        ),
    )
    data_type: Literal["character", "number", "datetime"] = Field(
        ...,
        description=(
            "Conservative M2 source field data type. 'character' covers "
            "VARCHAR/CHAR/CLOB-like strings; 'number' covers INTEGER/DECIMAL/"
            "NUMERIC/FLOAT; 'datetime' covers TIMESTAMP/DATE/TIME. Boolean and "
            "binary types are deferred until DB profile builders expand their "
            "supported set."
        ),
    )
    required: bool = Field(
        default=False,
        description=(
            "Whether the field is expected to be present in every record. "
            "Surfaced verbatim to downstream profile/map builders."
        ),
    )
    description: Optional[str] = Field(
        default=None,
        description=(
            "Optional human-readable description of the field. Non-executable; "
            "downstream builders may surface it in profile docs."
        ),
    )

    @field_validator("name")
    @classmethod
    def _strip_and_check_reserved(cls, value: str) -> str:
        # Reject the path-segment separator and array repetition marker so a DB
        # field name can never collide with the logical path conventions used
        # by the issue #43 profile generation helpers and downstream profile/
        # map builders (Root/list[]/key style). Without this guard a caller
        # could pass result_schema validation with `customer/id` and then
        # crash emit_spec() inside profile_generation with an opaque
        # ARCHETYPE_BUILD_FAILED — the strict contract must own this rejection
        # so callers see a structured PARAM_VALIDATION_FAILED instead.
        stripped = _stripped_nonblank(value)
        for reserved in ("/", "[", "]"):
            if reserved in stripped:
                raise ValueError(
                    "DBResultField.name must not contain the reserved path "
                    "characters '/', '[', or ']'; these are used by issue #43 "
                    "profile field generation to form logical field paths"
                )
        return stripped

    @field_validator("description")
    @classmethod
    def _strip_optional(cls, value: Optional[str]) -> Optional[str]:
        if value is None:
            return None
        return _stripped_nonblank(value)


class DBResultSchema(BaseModel):
    model_config = ConfigDict(extra="forbid")

    fields: List[DBResultField] = Field(
        ...,
        min_length=1,
        description=(
            "Caller-declared output fields produced by the DB read operation. "
            "Must be non-empty and unique by name. Issue #43 consumes this list "
            "to generate the DB read profile in M2; #26/#40/#41 consume it as "
            "the source-side reference set for transform operations."
        ),
    )

    @model_validator(mode="after")
    def _unique_field_names(self) -> "DBResultSchema":
        seen: Set[str] = set()
        duplicate_count = 0
        for f in self.fields:
            if f.name in seen:
                duplicate_count += 1
            else:
                seen.add(f.name)
        if duplicate_count:
            # The offending field names are deliberately not echoed: this
            # mirrors pattern_validation_error()'s policy of never echoing
            # caller-supplied input values back through the error envelope.
            raise ValueError(
                f"result_schema.fields contains {duplicate_count} duplicate "
                "field name(s); every entry must use a unique name"
            )
        return self


# ---------------------------------------------------------------------------
# Source — Database (SQL Server only in M2.1)
# ---------------------------------------------------------------------------


class DbCreateSettings(BaseModel):
    model_config = ConfigDict(extra="forbid")

    driver: Literal["microsoft_jdbc", "jtds"] = Field(
        ...,
        description=(
            "JDBC driver family for the SQL Server connection. 'microsoft_jdbc' "
            "uses Microsoft's official driver; 'jtds' uses the open-source "
            "jTDS driver. Other DB families (Postgres, Oracle) are deferred to "
            "future M2 increments."
        ),
    )
    auth_mode: Literal["username_password", "windows_integrated"] = Field(
        ...,
        description=(
            "Authentication mode for the database connection. 'username_password' "
            "requires credential_ref; 'windows_integrated' uses the runtime's "
            "Windows identity and ignores credential_ref/username."
        ),
    )
    host: str = Field(..., description="Database server hostname or IP.")
    port: int = Field(
        default=1433,
        ge=1,
        le=65535,
        description="Database server TCP port. Defaults to the SQL Server port 1433.",
    )
    database: str = Field(..., description="Target database (catalog) name.")
    username: Optional[str] = Field(
        default=None,
        description=(
            "Database username for 'username_password' auth. Required when "
            "auth_mode='username_password'; must be omitted when "
            "auth_mode='windows_integrated'."
        ),
    )
    credential_ref: Optional[str] = Field(
        default=None,
        description=(
            "Opaque reference to a secret-store entry that resolves to the "
            "database password at execution time. Required when "
            "auth_mode='username_password'; must be omitted when "
            "auth_mode='windows_integrated'. The contract never resolves, "
            "validates, or transmits the underlying secret."
        ),
    )
    jdbc_options: Dict[str, str] = Field(
        default_factory=dict,
        description=(
            "Optional JDBC URL options (e.g. 'encrypt' -> 'true', "
            "'trustServerCertificate' -> 'true'). Surfaced verbatim to "
            "downstream builders; the contract does not interpret keys."
        ),
    )

    @field_validator("host", "database")
    @classmethod
    def _strip_required(cls, value: str) -> str:
        return _stripped_nonblank(value)

    @field_validator("username", "credential_ref")
    @classmethod
    def _strip_optional(cls, value: Optional[str]) -> Optional[str]:
        if value is None:
            return None
        return _stripped_nonblank(value)

    @model_validator(mode="after")
    def _enforce_auth_mode_consistency(self) -> "DbCreateSettings":
        if self.auth_mode == "username_password":
            missing: List[str] = []
            if not self.username:
                missing.append("username")
            if not self.credential_ref:
                missing.append("credential_ref")
            if missing:
                raise ValueError(
                    "auth_mode='username_password' requires "
                    + " and ".join(missing)
                )
        else:  # windows_integrated
            unused: List[str] = []
            if self.username is not None:
                unused.append("username")
            if self.credential_ref is not None:
                unused.append("credential_ref")
            if unused:
                raise ValueError(
                    "auth_mode='windows_integrated' must not supply "
                    + " or ".join(unused)
                )
        return self


class DbConnectionBinding(BaseModel):
    model_config = ConfigDict(extra="forbid")

    mode: Literal["create", "reuse"] = Field(
        ...,
        description=(
            "How to materialize the database connector. 'create' builds a new "
            "Boomi connector from settings (later milestone); 'reuse' references "
            "an existing connector by component_id or component_name."
        ),
    )
    settings: Optional[DbCreateSettings] = Field(
        default=None,
        description=(
            "DB connector settings to create. Required when mode='create'; "
            "must be omitted when mode='reuse'."
        ),
    )
    component_id: Optional[str] = Field(
        default=None,
        description=(
            "Existing Boomi connector component id to reuse. Required when "
            "mode='reuse' if component_name is not supplied; must be omitted "
            "when mode='create'."
        ),
    )
    component_name: Optional[str] = Field(
        default=None,
        description=(
            "Existing Boomi connector component name to reuse (resolved at "
            "execution time). Required when mode='reuse' if component_id is "
            "not supplied; must be omitted when mode='create'."
        ),
    )

    @field_validator("component_id", "component_name")
    @classmethod
    def _strip_optional(cls, value: Optional[str]) -> Optional[str]:
        if value is None:
            return None
        return _stripped_nonblank(value)

    @model_validator(mode="after")
    def _enforce_mode_consistency(self) -> "DbConnectionBinding":
        if self.mode == "create":
            if self.settings is None:
                raise ValueError("mode='create' requires settings")
            if self.component_id or self.component_name:
                raise ValueError(
                    "mode='create' must not supply component_id or component_name"
                )
        else:  # reuse
            if not (self.component_id or self.component_name):
                raise ValueError(
                    "mode='reuse' requires component_id or component_name"
                )
            if self.settings is not None:
                raise ValueError("mode='reuse' must not supply settings")
        return self


class DbReadParameter(BaseModel):
    model_config = ConfigDict(extra="forbid")

    name: str = Field(
        ...,
        description=(
            "Bind-parameter name as referenced by the user-authored SQL "
            "(e.g. ':since' or '@since', depending on driver conventions)."
        ),
    )
    direction: Literal["in", "out"] = Field(
        default="in",
        description=(
            "Direction of the bind parameter: 'in' for inputs supplied at "
            "invocation, 'out' for parameters returned from the call site."
        ),
    )
    sql_type: Optional[str] = Field(
        default=None,
        description=(
            "Optional JDBC SQL type hint (e.g. 'VARCHAR', 'TIMESTAMP'). The "
            "contract does not validate the value; downstream builders pass it "
            "through to the database operation profile."
        ),
    )

    @field_validator("name")
    @classmethod
    def _strip_required(cls, value: str) -> str:
        return _stripped_nonblank(value)

    @field_validator("sql_type")
    @classmethod
    def _strip_optional(cls, value: Optional[str]) -> Optional[str]:
        if value is None:
            return None
        return _stripped_nonblank(value)


class DbReadOperation(BaseModel):
    model_config = ConfigDict(extra="forbid")

    sql: str = Field(
        ...,
        description=(
            "User- or LLM-authored read statement executed against the source "
            "database. The contract never generates SQL and never rewrites the "
            "value; it only validates that the string is non-blank."
        ),
    )
    result_schema: DBResultSchema = Field(
        ...,
        description=(
            "Caller-declared output schema for the read operation. The contract "
            "never infers result fields from sql, browse, metadata, or row "
            "samples; transforms must reference declared fields by name."
        ),
    )
    parameters: List[DbReadParameter] = Field(
        default_factory=list,
        description=(
            "Bind parameters referenced by the SQL statement. The contract "
            "does not parse the SQL; supplying parameters here is purely "
            "declarative for downstream builders."
        ),
    )
    batch_size: Optional[int] = Field(
        default=None,
        ge=1,
        description=(
            "Optional batch size for the database read operation. Surfaced "
            "verbatim to downstream builders; the contract does not impose a "
            "maximum."
        ),
    )
    fetch_size: Optional[int] = Field(
        default=None,
        ge=1,
        description=(
            "Optional JDBC fetch size hint for streaming large result sets. "
            "Surfaced verbatim to downstream builders."
        ),
    )
    max_rows: Optional[int] = Field(
        default=None,
        ge=1,
        description=(
            "Optional ceiling on the number of rows returned per execution. "
            "Surfaced verbatim to downstream builders; the contract does not "
            "enforce it."
        ),
    )
    link_element: Optional[str] = Field(
        default=None,
        description=(
            "Optional name of a link element used when the database operation "
            "feeds into a downstream nested call. Surfaced verbatim to "
            "downstream builders."
        ),
    )

    @field_validator("sql")
    @classmethod
    def _strip_sql(cls, value: str) -> str:
        return _stripped_nonblank(value)

    @field_validator("link_element")
    @classmethod
    def _strip_optional(cls, value: Optional[str]) -> Optional[str]:
        if value is None:
            return None
        return _stripped_nonblank(value)


class DatabaseSource(BaseModel):
    model_config = ConfigDict(extra="forbid")

    binding: DbConnectionBinding = Field(
        ...,
        description=(
            "How the database connector is materialized (create new settings "
            "or reuse an existing Boomi component)."
        ),
    )
    read_operation: DbReadOperation = Field(
        ...,
        description=(
            "The database read operation (SQL, declared result schema, bind "
            "parameters, batching hints) that produces records for transformation "
            "and send."
        ),
    )


# ---------------------------------------------------------------------------
# Target — REST + JSON payload profile (caller-supplied, M2.1a)
# ---------------------------------------------------------------------------


class RestCreateSettings(BaseModel):
    model_config = ConfigDict(extra="forbid")

    base_url: str = Field(
        ...,
        description=(
            "Absolute http(s) base URL for the REST target (e.g. "
            "'https://api.example.com'). The contract validates the scheme "
            "and overall shape only; it does not call the URL."
        ),
    )
    auth_mode: Literal[
        "none", "basic", "bearer_token", "oauth2_client_credentials"
    ] = Field(
        ...,
        description=(
            "Authentication mode for the REST target. 'none' requires no "
            "credential_ref; every other mode requires a credential_ref that "
            "resolves to the appropriate secret at execution time."
        ),
    )
    credential_ref: Optional[str] = Field(
        default=None,
        description=(
            "Opaque reference to a secret-store entry that resolves to the "
            "REST credential at execution time. Required when auth_mode is "
            "not 'none'; must be omitted when auth_mode='none'. The contract "
            "never resolves, validates, or transmits the underlying secret."
        ),
    )
    default_headers: Dict[str, str] = Field(
        default_factory=dict,
        description=(
            "Optional default headers applied to every REST request "
            "(e.g. 'Content-Type' -> 'application/json'). Surfaced verbatim "
            "to downstream builders."
        ),
    )

    @field_validator("base_url")
    @classmethod
    def _validate_url(cls, value: str) -> str:
        stripped = _stripped_nonblank(value)
        if not _URL_RE.match(stripped):
            raise ValueError("base_url must be an absolute http(s) URL")
        return stripped

    @field_validator("credential_ref")
    @classmethod
    def _strip_optional(cls, value: Optional[str]) -> Optional[str]:
        if value is None:
            return None
        return _stripped_nonblank(value)

    @model_validator(mode="after")
    def _enforce_auth_mode_consistency(self) -> "RestCreateSettings":
        if self.auth_mode == "none":
            if self.credential_ref is not None:
                raise ValueError(
                    "auth_mode='none' must not supply credential_ref"
                )
        else:
            if not self.credential_ref:
                raise ValueError(
                    "credential_ref is required when auth_mode is not 'none'"
                )
        return self


class RestConnectionBinding(BaseModel):
    model_config = ConfigDict(extra="forbid")

    mode: Literal["create", "reuse"] = Field(
        ...,
        description=(
            "How to materialize the REST connector. 'create' builds a new Boomi "
            "connector from settings (later milestone); 'reuse' references an "
            "existing connector by component_id or component_name."
        ),
    )
    settings: Optional[RestCreateSettings] = Field(
        default=None,
        description=(
            "REST connector settings to create. Required when mode='create'; "
            "must be omitted when mode='reuse'."
        ),
    )
    component_id: Optional[str] = Field(
        default=None,
        description=(
            "Existing Boomi connector component id to reuse. Required when "
            "mode='reuse' if component_name is not supplied; must be omitted "
            "when mode='create'."
        ),
    )
    component_name: Optional[str] = Field(
        default=None,
        description=(
            "Existing Boomi connector component name to reuse (resolved at "
            "execution time). Required when mode='reuse' if component_id is "
            "not supplied; must be omitted when mode='create'."
        ),
    )

    @field_validator("component_id", "component_name")
    @classmethod
    def _strip_optional(cls, value: Optional[str]) -> Optional[str]:
        if value is None:
            return None
        return _stripped_nonblank(value)

    @model_validator(mode="after")
    def _enforce_mode_consistency(self) -> "RestConnectionBinding":
        if self.mode == "create":
            if self.settings is None:
                raise ValueError("mode='create' requires settings")
            if self.component_id or self.component_name:
                raise ValueError(
                    "mode='create' must not supply component_id or component_name"
                )
        else:
            if not (self.component_id or self.component_name):
                raise ValueError(
                    "mode='reuse' requires component_id or component_name"
                )
            if self.settings is not None:
                raise ValueError("mode='reuse' must not supply settings")
        return self


class RestQueryParameter(BaseModel):
    model_config = ConfigDict(extra="forbid")

    name: str = Field(
        ...,
        description="Query-string parameter name as it appears on the request URL.",
    )
    value_source: Literal["literal", "watermark"] = Field(
        ...,
        description=(
            "Where the value comes from at execution time: 'literal' uses "
            "literal_value; 'watermark' takes the value from the configured "
            "execution.watermark. A future increment may add a 'mapping' "
            "source once the field-reference shape is designed; it is "
            "intentionally omitted here to keep payloads compileable."
        ),
    )
    literal_value: Optional[str] = Field(
        default=None,
        description=(
            "Literal value used when value_source='literal'. The contract "
            "does not require a value for the other sources."
        ),
    )

    @field_validator("name")
    @classmethod
    def _strip_required(cls, value: str) -> str:
        return _stripped_nonblank(value)

    @field_validator("literal_value")
    @classmethod
    def _strip_optional(cls, value: Optional[str]) -> Optional[str]:
        if value is None:
            return None
        return _stripped_nonblank(value)

    @model_validator(mode="after")
    def _enforce_literal_value(self) -> "RestQueryParameter":
        if self.value_source == "literal" and self.literal_value is None:
            raise ValueError(
                "literal_value is required when value_source='literal'"
            )
        return self


class RestSendRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    method: Literal["GET", "POST", "PUT", "PATCH", "DELETE"] = Field(
        default="POST",
        description="HTTP method used for the REST send request.",
    )
    path: str = Field(
        ...,
        description=(
            "Path appended to base_url (e.g. '/v1/customers'). The contract "
            "validates that the value is non-blank but does not normalize "
            "leading or trailing slashes."
        ),
    )
    query_parameters: List[RestQueryParameter] = Field(
        default_factory=list,
        description=(
            "Optional query-string parameters. Each entry declares its name "
            "and where its value comes from at execution time."
        ),
    )
    expected_status_codes: List[int] = Field(
        default_factory=lambda: [200, 201, 202],
        description=(
            "HTTP status codes considered successful for the send. Defaults "
            "to 200/201/202. Other status codes are routed through the "
            "reliability/error_classifier configuration."
        ),
    )

    @field_validator("path")
    @classmethod
    def _strip_path(cls, value: str) -> str:
        return _stripped_nonblank(value)


class JSONProfileNode(BaseModel):
    """A node in the caller-supplied JSON payload profile tree.

    The contract represents the target payload as a deterministic profile tree
    rather than as a raw body template. Downstream builders (issue #43 for the
    JSON profile, #26/#40/#41 for transforms) consume this tree to generate a
    Boomi JSON profile and map. Only ``kind='simple'`` nodes are valid
    transform targets.
    """

    model_config = ConfigDict(extra="forbid")

    name: str = Field(
        ...,
        description=(
            "Profile node name. Used as the JSON object entry name, the array "
            "name (with '[]' appended when forming logical paths), or the root "
            "element name."
        ),
    )
    kind: Literal["simple", "object", "array"] = Field(
        ...,
        description=(
            "Profile node kind. 'simple' is a leaf value (transform-targetable); "
            "'object' contains named children as object entries; 'array' "
            "repeats its children as the element shape."
        ),
    )
    data_type: Optional[Literal["character", "number", "datetime", "boolean"]] = Field(
        default=None,
        description=(
            "Leaf data type. Required when kind='simple'; must be omitted for "
            "kind='object' and kind='array'. Boolean is supported for JSON "
            "leaves; DB source result fields stay character/number/datetime "
            "until DB profile builders expand their supported set."
        ),
    )
    required: bool = Field(
        default=False,
        description=(
            "Whether the JSON node is required in the emitted payload. "
            "Surfaced verbatim to downstream profile/map builders."
        ),
    )
    description: Optional[str] = Field(
        default=None,
        description=(
            "Optional human-readable description of the JSON node. "
            "Non-executable."
        ),
    )
    children: Optional[List["JSONProfileNode"]] = Field(
        default=None,
        description=(
            "Child nodes. Required and non-empty for kind='object' (named "
            "entries) and kind='array' (element shape, expressed as the "
            "entries reached under the array repetition segment). Must be "
            "omitted for kind='simple'."
        ),
    )

    @field_validator("name")
    @classmethod
    def _strip_and_check_reserved(cls, value: str) -> str:
        # Reject the path-segment separator and the array repetition marker
        # so distinct profile leaves can never flatten to the same logical
        # path. Without this guard a leaf literally named ``a/b`` would
        # collide with object ``a`` -> leaf ``b`` (both flatten to
        # ``Root/a/b``), and a leaf named ``list[]`` would collide with an
        # array ``list`` containing one child.
        stripped = _stripped_nonblank(value)
        for reserved in ("/", "[", "]"):
            if reserved in stripped:
                raise ValueError(
                    "JSONProfileNode.name must not contain the reserved "
                    "path characters '/', '[', or ']'; these are used to "
                    "form logical leaf paths (e.g. 'Root/list[]/key')"
                )
        return stripped

    @field_validator("description")
    @classmethod
    def _strip_optional(cls, value: Optional[str]) -> Optional[str]:
        if value is None:
            return None
        return _stripped_nonblank(value)

    @model_validator(mode="after")
    def _shape_consistency(self) -> "JSONProfileNode":
        if self.kind == "simple":
            if self.data_type is None:
                raise ValueError("kind='simple' requires data_type")
            if self.children is not None:
                raise ValueError("kind='simple' must not supply children")
        else:
            if self.data_type is not None:
                raise ValueError(
                    f"kind={self.kind!r} must not supply data_type"
                )
            if not self.children:
                raise ValueError(
                    f"kind={self.kind!r} requires non-empty children"
                )
            seen: Set[str] = set()
            duplicate_count = 0
            for child in self.children:
                if child.name in seen:
                    duplicate_count += 1
                else:
                    seen.add(child.name)
            if duplicate_count:
                # Do not echo child names — defense-in-depth against secret
                # echo in case callers ever use sensitive identifiers.
                raise ValueError(
                    f"kind={self.kind!r} has {duplicate_count} duplicate child "
                    "name(s); every child must use a unique name"
                )
        return self


class JSONPayloadProfile(BaseModel):
    model_config = ConfigDict(extra="forbid")

    format: Literal["json"] = Field(
        default="json",
        description=(
            "Profile format. M2 supports 'json' only; XML / EDI / flat-file "
            "target profile families are deferred to a later milestone."
        ),
    )
    root: JSONProfileNode = Field(
        ...,
        description=(
            "Root node of the JSON payload profile. Must be kind='object'; "
            "arrays and simple leaves are not valid JSON profile roots in M2 "
            "(Boomi JSON profiles require exactly one root object)."
        ),
    )

    @model_validator(mode="after")
    def _root_must_be_object(self) -> "JSONPayloadProfile":
        if self.root.kind != "object":
            raise ValueError(
                "payload_profile.root.kind must be 'object'; arrays and simple "
                "leaves are not valid JSON profile roots in M2"
            )
        return self


class RestTarget(BaseModel):
    model_config = ConfigDict(extra="forbid")

    binding: RestConnectionBinding = Field(
        ...,
        description=(
            "How the REST connector is materialized (create new settings or "
            "reuse an existing Boomi component)."
        ),
    )
    send_request: RestSendRequest = Field(
        ...,
        description=(
            "REST send request configuration (method, path, query parameters, "
            "expected status codes) applied to every record dispatched to the "
            "target."
        ),
    )
    payload_profile: JSONPayloadProfile = Field(
        ...,
        description=(
            "Caller-supplied JSON profile tree describing the request body. "
            "The contract represents target intent as a deterministic profile "
            "tree (not a raw body template); only kind='simple' leaves are "
            "valid transform targets."
        ),
    )


# ---------------------------------------------------------------------------
# Transform — discriminated typed operations (M2.1a)
# ---------------------------------------------------------------------------


class _BaseTransformOperation(BaseModel):
    model_config = ConfigDict(extra="forbid")

    documentation_hint: Optional[str] = Field(
        default=None,
        description=(
            "Optional non-executable human-readable note about the operation's "
            "intent. Downstream builders must not parse or execute the value; "
            "it exists to preserve task-authored context, not as a routing "
            "signal."
        ),
    )

    @field_validator("documentation_hint")
    @classmethod
    def _strip_optional_hint(cls, value: Optional[str]) -> Optional[str]:
        if value is None:
            return None
        return _stripped_nonblank(value)


class DirectTransformOperation(_BaseTransformOperation):
    operation_type: Literal["direct"] = Field(
        ...,
        description=(
            "Discriminator: 'direct' routes to a one-to-one Boomi map step. "
            "Future builder: issue #26."
        ),
    )
    source_field: str = Field(
        ...,
        description=(
            "Name of a field declared in source.read_operation.result_schema."
            "fields. The cross-field validator rejects unknown names."
        ),
    )
    target_path: str = Field(
        ...,
        description=(
            "Logical leaf path inside target.payload_profile (slash-separated, "
            "e.g. 'Root/name' or 'Root/list[]/key'). Must reference a "
            "kind='simple' leaf; object and array nodes cannot be transform "
            "targets."
        ),
    )

    @field_validator("source_field", "target_path")
    @classmethod
    def _strip_required(cls, value: str) -> str:
        return _stripped_nonblank(value)


class MapFunctionTransformOperation(_BaseTransformOperation):
    operation_type: Literal["map_function"] = Field(
        ...,
        description=(
            "Discriminator: 'map_function' routes to a Boomi map function step. "
            "Future builder: issue #40."
        ),
    )
    function_type: str = Field(
        ...,
        description=(
            "Task-authored function route name (e.g. 'trim', 'uppercase', "
            "'concat'). The contract surfaces the value verbatim; issue #40 "
            "owns the concrete allowed-set."
        ),
    )
    inputs: List[str] = Field(
        ...,
        min_length=1,
        description=(
            "One or more function inputs. Each entry must reference a name "
            "declared in source.read_operation.result_schema.fields."
        ),
    )
    target_path: str = Field(
        ...,
        description=(
            "Logical leaf path inside target.payload_profile. Must reference a "
            "kind='simple' leaf."
        ),
    )
    parameters: Optional[Dict[str, Any]] = Field(
        default=None,
        description=(
            "Optional opaque parameter object surfaced verbatim to issue #40. "
            "The contract does not interpret keys or values."
        ),
    )

    @field_validator("function_type", "target_path")
    @classmethod
    def _strip_required(cls, value: str) -> str:
        return _stripped_nonblank(value)

    @field_validator("inputs")
    @classmethod
    def _strip_inputs(cls, value: List[str]) -> List[str]:
        cleaned: List[str] = []
        for item in value:
            if not isinstance(item, str):
                raise ValueError("inputs entries must be strings")
            cleaned.append(_stripped_nonblank(item))
        return cleaned

    @field_validator("parameters")
    @classmethod
    def _reject_plaintext_secret_keys(
        cls, value: Optional[Dict[str, Any]]
    ) -> Optional[Dict[str, Any]]:
        # parameters is the only schema-opaque dict the archetype echoes back
        # in IntegrationSpec.flows[transform].operations[*].parameters on a
        # successful build. Reject plaintext secret-shaped keys at any nesting
        # depth so the spec output never leaks plaintext. The offending key
        # name is not echoed back through the error envelope — callers route
        # secrets via the connector binding's credential_ref instead.
        if value is None:
            return None
        if _scan_for_secret_shaped_keys(value):
            raise ValueError(
                "map_function.parameters contains a key whose name matches a "
                "forbidden secret-shaped substring (e.g. password / token / "
                "secret / api_key / bearer / authorization). Reference "
                "connector secrets via the connector binding's credential_ref "
                "instead; map_function.parameters is echoed back in the "
                "emitted IntegrationSpec and must not carry plaintext secrets."
            )
        return value


class MapScriptTransformOperation(_BaseTransformOperation):
    operation_type: Literal["map_script"] = Field(
        ...,
        description=(
            "Discriminator: 'map_script' routes to a Boomi map script step "
            "rendered as an in-map userdefined FunctionStep referencing a "
            "reusable script.mapping component (issue #41)."
        ),
    )
    script_slot: str = Field(
        ...,
        description=(
            "Stable task-authored slot name (e.g. 'pre_send', 'enrich_row') "
            "used to identify the script's role inside the archetype "
            "summary. Carried through to ScriptMappingBuilder / "
            "MapScriptBuilder verbatim."
        ),
    )
    language: Literal["groovy2", "groovy", "javascript"] = Field(
        ...,
        description=(
            "Script language. 'groovy2' targets the recommended modern Boomi "
            "Groovy 2 runtime; 'groovy' targets legacy Groovy 1; 'javascript' "
            "targets the Boomi JavaScript runtime. The script.mapping "
            "component's own language attribute is the source of truth at "
            "emit time; this field is informational for archetype callers."
        ),
    )
    inputs: List[str] = Field(
        ...,
        min_length=1,
        description=(
            "Source field names consumed by the script. Each entry must "
            "reference a name declared in source.read_operation.result_schema."
            "fields."
        ),
    )
    outputs: List[str] = Field(
        ...,
        min_length=1,
        description=(
            "Target leaf paths populated by the script. Each entry must "
            "reference a kind='simple' leaf in target.payload_profile."
        ),
    )
    script_component_ref: Optional[str] = Field(
        default=None,
        description=(
            "Reference to a reusable script wiring. Use '$ref:KEY' pointing "
            "at an in-spec script.mapping (downstream wrapper synthesis "
            "auto-creates the transform.function wrapper) or '$ref:KEY' "
            "pointing at an in-spec transform.function wrapper. A literal "
            "componentId may be supplied at the archetype layer for "
            "downstream tooling that wraps existing-Boomi script reuse, "
            "but build_integration's #41 contract rejects literal IDs in "
            "the corresponding map's script_mappings[].script_component_id "
            "— Boomi requires the map FunctionStep id to point at a "
            "transform.function wrapper, which can only be synthesized "
            "from in-spec components. Callers reusing existing scripts "
            "should declare an in-spec transform.function wrapper that "
            "embeds the existing script.mapping's componentId."
        ),
    )
    script_body: Optional[str] = Field(
        default=None,
        description=(
            "Caller-authored script source. Issue #29 materializes an inline "
            "script_body into an in-spec script.mapping component (referenced "
            "by the transform.map), so an inline body is the supported way to "
            "route a map script through this archetype. The emitted operation "
            "summary still round-trips the full body verbatim (alongside "
            "``script_body_present``). script_component_ref (external reuse) is "
            "rejected at assembly because the referenced component is not part "
            "of the emitted spec — provide script_body instead (#51 owns "
            "external script-component reuse)."
        ),
    )

    @field_validator("script_slot")
    @classmethod
    def _strip_required(cls, value: str) -> str:
        return _stripped_nonblank(value)

    @field_validator("script_component_ref")
    @classmethod
    def _strip_optional_ref(cls, value: Optional[str]) -> Optional[str]:
        if value is None:
            return None
        return _stripped_nonblank(value)

    @field_validator("script_body")
    @classmethod
    def _strip_optional_body(cls, value: Optional[str]) -> Optional[str]:
        if value is None:
            return None
        # Non-blank if present — silently accepting an empty string would
        # discard caller intent. Empty/whitespace-only bodies fail loudly.
        return _stripped_nonblank(value)

    @field_validator("inputs", "outputs")
    @classmethod
    def _strip_input_paths(cls, value: List[str]) -> List[str]:
        cleaned: List[str] = []
        for item in value:
            if not isinstance(item, str):
                raise ValueError("entries must be strings")
            cleaned.append(_stripped_nonblank(item))
        return cleaned


TransformOperation = Annotated[
    Union[
        DirectTransformOperation,
        MapFunctionTransformOperation,
        MapScriptTransformOperation,
    ],
    Field(discriminator="operation_type"),
]


class TransformConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    operations: List[TransformOperation] = Field(
        ...,
        min_length=1,
        description=(
            "Typed transform operations. Every operation carries an "
            "operation_type discriminator selecting its compile route: "
            "'direct' (one-to-one field mapping; issue #26), 'map_function' "
            "(Boomi map function step; issue #40), or 'map_script' (map "
            "script component; issue #41). operation_type='xslt' is rejected "
            "with a pointer to issue #42. Legacy free-form transform_hint, "
            "payload_template, and script_slots are no longer accepted as "
            "executable routes."
        ),
    )

    @model_validator(mode="before")
    @classmethod
    def _reject_xslt_with_42_pointer(cls, data: Any) -> Any:
        # mode='before' runs before the discriminator picks a variant, so an
        # explicit 'xslt' value can be surfaced with a friendly #42 pointer
        # rather than the generic union_tag_invalid error. The offending index
        # is included, but no caller-supplied content is echoed.
        if isinstance(data, dict):
            ops = data.get("operations")
            if isinstance(ops, list):
                for idx, op in enumerate(ops):
                    if isinstance(op, dict):
                        op_type = op.get("operation_type")
                        if (
                            isinstance(op_type, str)
                            and op_type.strip().lower() == "xslt"
                        ):
                            raise ValueError(
                                f"operations[{idx}].operation_type='xslt' is "
                                "not supported in M2; see issue #42 for the "
                                "XSLT support decision."
                            )
        return data


# ---------------------------------------------------------------------------
# Execution
# ---------------------------------------------------------------------------


class Schedule(BaseModel):
    model_config = ConfigDict(extra="forbid")

    cron: str = Field(
        ...,
        description=(
            "Cron expression for the scheduled trigger. The contract does "
            "not parse or validate the cron syntax; downstream builders pass "
            "it to the Boomi schedule shape verbatim."
        ),
    )
    timezone: Optional[str] = Field(
        default=None,
        description=(
            "Optional IANA timezone string (e.g. 'UTC', 'America/New_York'). "
            "Surfaced verbatim to downstream builders; the contract does not "
            "validate it."
        ),
    )

    @field_validator("cron")
    @classmethod
    def _strip_cron(cls, value: str) -> str:
        return _stripped_nonblank(value)

    @field_validator("timezone")
    @classmethod
    def _strip_optional(cls, value: Optional[str]) -> Optional[str]:
        if value is None:
            return None
        return _stripped_nonblank(value)


class ExecutionTrigger(BaseModel):
    model_config = ConfigDict(extra="forbid")

    mode: Literal["manual", "scheduled"] = Field(
        ...,
        description=(
            "How the integration is started. 'manual' is invoked on demand; "
            "'scheduled' requires a Schedule and is fired by the Boomi "
            "scheduler once executable builders ship."
        ),
    )
    schedule: Optional[Schedule] = Field(
        default=None,
        description=(
            "Schedule configuration. Required when mode='scheduled'; must be "
            "omitted when mode='manual'."
        ),
    )

    @model_validator(mode="after")
    def _enforce_schedule_consistency(self) -> "ExecutionTrigger":
        if self.mode == "scheduled" and self.schedule is None:
            raise ValueError("mode='scheduled' requires schedule")
        if self.mode == "manual" and self.schedule is not None:
            raise ValueError("mode='manual' must not supply schedule")
        return self


class Watermark(BaseModel):
    model_config = ConfigDict(extra="forbid")

    field: str = Field(
        ...,
        description=(
            "Name of a source result field driving high-water-mark "
            "advancement (e.g. 'last_modified_at'). Must reference a name "
            "declared in source.read_operation.result_schema.fields; the "
            "cross-field validator rejects unknown names."
        ),
    )
    kind: Literal["timestamp", "sequence"] = Field(
        ...,
        description=(
            "Watermark kind. 'timestamp' compares chronological values; "
            "'sequence' compares monotonically-increasing integers."
        ),
    )
    initial_value: Optional[str] = Field(
        default=None,
        description=(
            "Optional initial high-water-mark value used on the first run "
            "before any state has been persisted. Surfaced verbatim to "
            "downstream builders."
        ),
    )
    persistence: Literal["dpp", "external_store"] = Field(
        default="dpp",
        description=(
            "Where the watermark is persisted. 'dpp' uses Boomi Dynamic "
            "Process Properties; 'external_store' delegates to an external "
            "key/value store whose binding is configured by future builders."
        ),
    )

    @field_validator("field")
    @classmethod
    def _strip_field(cls, value: str) -> str:
        return _stripped_nonblank(value)

    @field_validator("initial_value")
    @classmethod
    def _strip_optional(cls, value: Optional[str]) -> Optional[str]:
        if value is None:
            return None
        return _stripped_nonblank(value)


class ExecutionConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    trigger: ExecutionTrigger = Field(
        ...,
        description=(
            "How the integration is started (manual or scheduled). Scheduled "
            "triggers require a Schedule."
        ),
    )
    watermark: Optional[Watermark] = Field(
        default=None,
        description=(
            "Optional high-water-mark configuration for incremental syncs. "
            "Omit for full-extract runs."
        ),
    )
    run_metadata: Dict[str, str] = Field(
        default_factory=dict,
        description=(
            "Optional opaque key/value metadata associated with every run "
            "(e.g. business owner, runbook URL). Surfaced verbatim to "
            "downstream builders; the contract does not interpret keys."
        ),
    )


# ---------------------------------------------------------------------------
# Reliability
# ---------------------------------------------------------------------------


class RetryPolicy(BaseModel):
    model_config = ConfigDict(extra="forbid")

    max_attempts: int = Field(
        default=1,
        ge=1,
        description=(
            "Maximum number of attempts per record (1 means no retry). The "
            "contract surfaces this value verbatim; downstream builders wire "
            "the retry shape."
        ),
    )
    backoff: Literal["none", "fixed", "exponential"] = Field(
        default="none",
        description=(
            "Backoff strategy between retry attempts. 'none' retries "
            "immediately; 'fixed' waits a constant interval; 'exponential' "
            "doubles the interval each attempt."
        ),
    )
    initial_interval_seconds: Optional[int] = Field(
        default=None,
        ge=0,
        description=(
            "Initial backoff interval in seconds for 'fixed' / 'exponential' "
            "backoffs. The contract does not enforce a default; downstream "
            "builders pick one when this value is omitted."
        ),
    )


class DlqTarget(BaseModel):
    """Dead-letter destination, aligned to the builder's verified DLQ modes.

    The process builder (issue #51 M3.R1a) emits a verified Try/Catch + DLQ
    catch path for exactly two modes — ``document_cache_ref`` (catch leg routes
    to a Document Cache, bound via ``document_cache_id``) and
    ``error_subprocess_ref`` (catch leg calls an error subprocess, bound via
    ``process_id``). The binding is a literal Boomi component id or a
    ``$ref:KEY`` token whose KEY is an in-spec component. Legacy
    folder/topic/queue routing is NOT an emittable builder mode; it is retained
    ONLY as an explicitly-labeled ``guidance_only`` alias that records intent
    as metadata (no wiring) — never silently accepted as a real DLQ.
    """

    model_config = ConfigDict(extra="forbid")

    mode: Literal["document_cache_ref", "error_subprocess_ref", "guidance_only"] = Field(
        ...,
        description=(
            "Verified DLQ mode. 'document_cache_ref' / 'error_subprocess_ref' "
            "emit a real Try/Catch + DLQ catch path (require document_cache_id "
            "/ process_id). 'guidance_only' records legacy folder/topic/queue "
            "intent as metadata only and emits no wiring."
        ),
    )
    document_cache_id: Optional[str] = Field(
        default=None,
        description=(
            "DLQ Document Cache binding for mode='document_cache_ref': a literal "
            "Boomi component id, or a '$ref:KEY' token referencing an in-spec "
            "Document Cache component. Required for that mode; rejected otherwise."
        ),
    )
    process_id: Optional[str] = Field(
        default=None,
        description=(
            "Error-subprocess binding for mode='error_subprocess_ref': a literal "
            "Boomi component id, or a '$ref:KEY' token referencing an in-spec "
            "process/subprocess component. Required for that mode; rejected "
            "otherwise."
        ),
    )
    kind: Optional[Literal["folder", "topic", "queue"]] = Field(
        default=None,
        description=(
            "Legacy routing kind — accepted ONLY with mode='guidance_only' "
            "(recorded as metadata, never wired)."
        ),
    )
    address: Optional[str] = Field(
        default=None,
        description=(
            "Legacy destination address — accepted ONLY with "
            "mode='guidance_only'. Never echoed back (may carry sensitive "
            "content); only its presence is recorded."
        ),
    )
    reason: Optional[str] = Field(
        default=None,
        description="Optional free-form note for a guidance_only target.",
    )

    @field_validator("document_cache_id", "process_id", "address")
    @classmethod
    def _strip_required_present(cls, value: Optional[str]) -> Optional[str]:
        if value is None:
            return None
        return _stripped_nonblank(value)

    @model_validator(mode="after")
    def _enforce_mode_fields(self) -> "DlqTarget":
        if self.mode == "document_cache_ref":
            if not self.document_cache_id:
                raise ValueError(
                    "dlq.target.mode='document_cache_ref' requires "
                    "document_cache_id (a Boomi component id or '$ref:KEY')."
                )
            if self.process_id or self.kind or self.address:
                raise ValueError(
                    "dlq.target.mode='document_cache_ref' accepts only "
                    "document_cache_id (no process_id/kind/address)."
                )
        elif self.mode == "error_subprocess_ref":
            if not self.process_id:
                raise ValueError(
                    "dlq.target.mode='error_subprocess_ref' requires process_id "
                    "(a Boomi component id or '$ref:KEY')."
                )
            if self.document_cache_id or self.kind or self.address:
                raise ValueError(
                    "dlq.target.mode='error_subprocess_ref' accepts only "
                    "process_id (no document_cache_id/kind/address)."
                )
        else:  # guidance_only
            if not self.kind or not self.address:
                raise ValueError(
                    "dlq.target.mode='guidance_only' requires kind and address "
                    "(legacy folder/topic/queue intent recorded as metadata only "
                    "— no builder wiring)."
                )
            if self.document_cache_id or self.process_id:
                raise ValueError(
                    "dlq.target.mode='guidance_only' must not set "
                    "document_cache_id/process_id."
                )
        return self


class DlqPolicy(BaseModel):
    model_config = ConfigDict(extra="forbid")

    enabled: bool = Field(
        default=False,
        description=(
            "Whether dead-letter routing is enabled. When True, target is "
            "required."
        ),
    )
    target: Optional[DlqTarget] = Field(
        default=None,
        description=(
            "Dead-letter destination. Required when enabled=True; must be "
            "omitted when enabled=False."
        ),
    )

    @model_validator(mode="after")
    def _enforce_target(self) -> "DlqPolicy":
        if self.enabled and self.target is None:
            raise ValueError("dlq.enabled=True requires target")
        if not self.enabled and self.target is not None:
            raise ValueError("dlq.enabled=False must not supply target")
        return self


class ErrorClassifier(BaseModel):
    model_config = ConfigDict(extra="forbid")

    retriable_status_codes: List[int] = Field(
        default_factory=lambda: [502, 503, 504],
        description=(
            "HTTP status codes treated as retriable by the reliability "
            "policy. Defaults to common transient codes."
        ),
    )
    terminal_status_codes: List[int] = Field(
        default_factory=lambda: [400, 401, 403, 404, 422],
        description=(
            "HTTP status codes treated as terminal (no retry) by the "
            "reliability policy. Defaults to common client-error codes."
        ),
    )
    custom_rules: List[str] = Field(
        default_factory=list,
        description=(
            "Optional free-form rule labels describing additional classifier "
            "behavior to be implemented by downstream builders (e.g. "
            "'rate_limit_exhausted'). Values are opaque labels; no scripts."
        ),
    )


class ReliabilityConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    retry: RetryPolicy = Field(
        ...,
        description=(
            "Retry policy applied to retriable failures during the send step."
        ),
    )
    dlq: DlqPolicy = Field(
        ...,
        description=(
            "Dead-letter queue policy applied after retries are exhausted."
        ),
    )
    error_classifier: ErrorClassifier = Field(
        ...,
        description=(
            "Rules that classify response errors as retriable or terminal. "
            "Drives the retry and DLQ policies above."
        ),
    )


# ---------------------------------------------------------------------------
# Profile flatten helper (M2.1a logical leaf-path index)
# ---------------------------------------------------------------------------


def _flatten_payload_profile_leaves(
    profile: JSONPayloadProfile,
) -> Dict[str, str]:
    """Return mapping of leaf logical path -> data_type for every simple leaf.

    Walks the root downward producing slash-separated paths. Arrays append
    ``[]`` to their own segment (e.g. ``Root/list[]/key``); only nodes with
    ``kind='simple'`` become leaves.
    """
    leaves: Dict[str, str] = {}

    def _walk(node: JSONProfileNode, prefix: str) -> None:
        if node.kind == "simple":
            # data_type presence is guaranteed by JSONProfileNode._shape_consistency.
            leaves[prefix] = node.data_type or ""
            return
        segment = f"{prefix}[]" if node.kind == "array" else prefix
        for child in node.children or []:
            _walk(child, f"{segment}/{child.name}")

    _walk(profile.root, profile.root.name)
    return leaves


def _required_simple_leaf_paths(profile: JSONPayloadProfile) -> Set[str]:
    """Return the set of logical leaf paths whose JSON profile node is
    ``kind='simple'`` AND ``required=True``.

    Uses the same path convention as ``_flatten_payload_profile_leaves``.
    Required structural nodes (object/array) are excluded because they are
    not transform-targetable — only their simple leaf descendants can
    receive a direct/map_function/map_script output.
    """
    required: Set[str] = set()

    def _walk(node: JSONProfileNode, prefix: str) -> None:
        if node.kind == "simple":
            if node.required:
                required.add(prefix)
            return
        segment = f"{prefix}[]" if node.kind == "array" else prefix
        for child in node.children or []:
            _walk(child, f"{segment}/{child.name}")

    _walk(profile.root, profile.root.name)
    return required


# ---------------------------------------------------------------------------
# Top-level parameters
# ---------------------------------------------------------------------------


class DatabaseToApiSyncParameters(BaseModel):
    model_config = ConfigDict(extra="forbid")

    naming: NamingConfig = Field(
        ...,
        description=(
            "Naming, folder, and runtime-hint configuration for the emitted "
            "integration."
        ),
    )
    source: DatabaseSource = Field(
        ...,
        description=(
            "Database source configuration: connector binding, read operation, "
            "and caller-declared result schema."
        ),
    )
    target: RestTarget = Field(
        ...,
        description=(
            "REST target configuration: connector binding, send request, and "
            "caller-supplied JSON payload profile tree."
        ),
    )
    transform: TransformConfig = Field(
        ...,
        description=(
            "Typed transform operations that move source result fields into "
            "target JSON profile leaves. Discriminated by operation_type: "
            "direct (#26), map_function (#40), map_script (#41); xslt is "
            "rejected (#42)."
        ),
    )
    execution: ExecutionConfig = Field(
        ...,
        description=(
            "Execution trigger, optional watermark, and run metadata for "
            "the integration."
        ),
    )
    reliability: ReliabilityConfig = Field(
        ...,
        description=(
            "Retry policy, dead-letter policy, and error classifier applied "
            "to the send step."
        ),
    )

    @model_validator(mode="after")
    def _enforce_watermark_consistency(self) -> "DatabaseToApiSyncParameters":
        if self.execution.watermark is not None:
            return self
        has_watermark_param = any(
            qp.value_source == "watermark"
            for qp in self.target.send_request.query_parameters
        )
        if has_watermark_param:
            # The offending parameter names are deliberately omitted: this
            # error envelope mirrors pattern_validation_error()'s policy of
            # never echoing caller-supplied input values, which can contain
            # credentials or other sensitive content.
            raise ValueError(
                "target.send_request.query_parameters with "
                "value_source='watermark' require execution.watermark to be "
                "configured"
            )
        return self

    @model_validator(mode="after")
    def _validate_transform_refs(self) -> "DatabaseToApiSyncParameters":
        source_field_names: Set[str] = {
            f.name for f in self.source.read_operation.result_schema.fields
        }
        target_leaves: Dict[str, str] = _flatten_payload_profile_leaves(
            self.target.payload_profile
        )

        unknown_source_refs = 0
        unknown_target_refs = 0
        duplicate_target_bindings = 0
        bound_target_paths: Set[str] = set()

        def _bind(target_path: str) -> None:
            nonlocal duplicate_target_bindings
            if target_path in bound_target_paths:
                duplicate_target_bindings += 1
            else:
                bound_target_paths.add(target_path)

        for op in self.transform.operations:
            if isinstance(op, DirectTransformOperation):
                if op.source_field not in source_field_names:
                    unknown_source_refs += 1
                if op.target_path in target_leaves:
                    _bind(op.target_path)
                else:
                    unknown_target_refs += 1
            elif isinstance(op, MapFunctionTransformOperation):
                for inp in op.inputs:
                    if inp not in source_field_names:
                        unknown_source_refs += 1
                if op.target_path in target_leaves:
                    _bind(op.target_path)
                else:
                    unknown_target_refs += 1
            elif isinstance(op, MapScriptTransformOperation):
                for inp in op.inputs:
                    if inp not in source_field_names:
                        unknown_source_refs += 1
                for out in op.outputs:
                    if out in target_leaves:
                        _bind(out)
                    else:
                        unknown_target_refs += 1

        # Issue #43 review r2 P2: every required simple leaf in the JSON
        # payload profile must be the destination of at least one transform
        # output, otherwise downstream profile/map builders (#26) could emit a
        # payload that omits a required field. The offending paths are
        # intentionally NOT echoed in the error message — same defense-in-depth
        # policy as the duplicate_target_bindings branch, since profile node
        # names can carry caller-specific identifiers.
        required_target_paths = _required_simple_leaf_paths(self.target.payload_profile)
        unmapped_required_count = len(required_target_paths - bound_target_paths)

        issues: List[str] = []
        if unknown_source_refs:
            issues.append(
                f"transform.operations contain {unknown_source_refs} "
                "reference(s) to a source field name not declared in "
                "source.read_operation.result_schema.fields"
            )
        if unknown_target_refs:
            issues.append(
                f"transform.operations contain {unknown_target_refs} "
                "reference(s) to a target path that is not a declared simple "
                "leaf in target.payload_profile"
            )
        if duplicate_target_bindings:
            issues.append(
                f"transform.operations bind {duplicate_target_bindings} "
                "target leaf path(s) more than once; every leaf may be the "
                "destination of at most one direct/map_function/map_script "
                "output"
            )
        if unmapped_required_count:
            issues.append(
                f"transform.operations leave {unmapped_required_count} "
                "required target leaf path(s) unmapped; every required "
                "simple leaf in target.payload_profile must be the "
                "destination of at least one direct/map_function/map_script "
                "output"
            )

        if issues:
            raise ValueError(" | ".join(issues))

        if self.execution.watermark is not None:
            if self.execution.watermark.field not in source_field_names:
                raise ValueError(
                    "execution.watermark.field must reference a name declared "
                    "in source.read_operation.result_schema.fields"
                )

        return self


# ---------------------------------------------------------------------------
# Archetype
# ---------------------------------------------------------------------------


# Example payload sentinels — these intentionally do NOT look like real SQL,
# OData filters, SOAP envelopes, REST payloads, field mappings, or scripts.
# They exist only to demonstrate the parameter shape.
_EXAMPLE_SQL_SENTINEL = "<<user-authored DB read statement>>"


# ---------------------------------------------------------------------------
# Issue #29 assembly helpers
# ---------------------------------------------------------------------------
#
# These turn the validated archetype contract into primitive parameter objects
# and a structured main_process component. Every byte of XML and all structured
# component validation is delegated to the existing builders through the #27/#28
# primitives — the archetype only maps fields and wires deterministic $ref keys.
# Fields the current builders cannot emit are metadata-deferred (never silently
# dropped) under validation_rules.operational_intent.deferred.


def _coerce_primitive_params(model_cls, data: Dict[str, Any], *, field: str):
    """Build a primitive parameter model, converting a pydantic
    ``ValidationError`` into a clean, secret-safe ``BuilderValidationError``.

    The archetype contract is intentionally laxer than some primitive param
    models (e.g. ``Schedule.cron`` accepts any string but ScheduleEnvelope
    requires a 5-part cron; a map_script op may omit both body and ref at the
    contract layer but field_map requires exactly one). Without this, such a
    caller error surfaces as the opaque ``ARCHETYPE_BUILD_FAILED`` last-resort
    envelope. We rebuild the message from each error's ``loc`` + ``msg`` only —
    never the ``input`` value — so caller-supplied (possibly sensitive) values
    are never echoed.
    """
    try:
        return model_cls.model_validate(data)
    except ValidationError as exc:
        problems = "; ".join(
            ": ".join(
                part
                for part in (
                    ".".join(str(p) for p in err.get("loc", ())),
                    str(err.get("msg", "")),
                )
                if part
            )
            for err in exc.errors()
        )
        raise BuilderValidationError(
            f"{field} could not be assembled from the archetype parameters: "
            f"{problems}",
            error_code="ARCHETYPE_PARAM_INVALID",
            field=field,
            hint=(
                "Adjust the archetype parameters so the primitive can validate "
                "them — e.g. a 5-part cron for scheduled triggers, or exactly "
                "one of script_body / script_component_ref ('$ref:KEY') per "
                "map_script operation."
            ),
        ) from exc


def _component_names(naming: "NamingConfig") -> Dict[str, str]:
    """Caller component-name overrides.

    Keyed by component role per the public schema (``db_connection``,
    ``db_read_profile``, ``db_get_operation``, ``target_profile``,
    ``transform_map``, ``script``, ``rest_connection``, ``rest_operation``,
    ``process``). The prefixed emitted key (e.g. ``source_db_connection``) is
    also honored as a fallback.
    """
    return dict(naming.component_names or {})


def _named(overrides: Dict[str, str], *keys: str) -> Optional[str]:
    """First non-blank override among ``keys`` (role key first, then the
    prefixed emitted key as a fallback)."""
    for key in keys:
        value = overrides.get(key)
        if isinstance(value, str) and value.strip():
            return value.strip()
    return None


def _build_db_extract_params(
    parameters: "DatabaseToApiSyncParameters", overrides: Dict[str, str]
) -> DbExtractParameters:
    source = parameters.source
    binding = source.binding
    read = source.read_operation

    if binding.mode == "create":
        settings = binding.settings  # guaranteed present by the contract validator
        connection: Dict[str, Any] = {
            "mode": "create",
            # driver maps 1:1 onto DatabaseConnectorBuilder.SUPPORTED_DRIVER_IDS
            # ('microsoft_jdbc' is a recognized alias of 'sqlserver').
            "driver_id": settings.driver,
            # 'windows_integrated' is rejected by DatabaseConnectorBuilder
            # (UNSUPPORTED_DB_AUTH_MODE) — passed through so the builder, not the
            # archetype, owns the auth-mode vocabulary.
            "auth_mode": settings.auth_mode,
            "host": settings.host,
            "port": settings.port,
            "dbname": settings.database,
        }
        if settings.username is not None:
            connection["username"] = settings.username
        if settings.credential_ref is not None:
            connection["credential_ref"] = settings.credential_ref
        # jdbc_options (Dict[str,str]) is metadata-deferred (see
        # _deferred_intent): the contract carries no verbatim JDBC suffix, and
        # synthesizing one would violate the no-template rule.
    else:
        connection = {"mode": "reuse"}
        if binding.component_id:
            connection["component_id"] = binding.component_id
        if binding.component_name:
            connection["component_name"] = binding.component_name

    output_fields = [
        {"name": f.name, "data_type": f.data_type, "mandatory": f.required}
        for f in read.result_schema.fields
    ]
    read_profile: Dict[str, Any] = {"query": read.sql, "output_fields": output_fields}
    if read.parameters:
        # The Select read profile takes name + mappability; sql_type/direction
        # are not builder-supported and are metadata-deferred. 'in' parameters
        # are the mappable bind inputs.
        read_profile["parameters"] = [
            {"name": p.name, "mappable": (p.direction == "in")}
            for p in read.parameters
        ]

    operation: Dict[str, Any] = {}
    if read.batch_size is not None:
        operation["batch_count"] = read.batch_size
    if read.max_rows is not None:
        operation["max_rows"] = read.max_rows
    # fetch_size / link_element have no DB Get operation builder field — deferred.

    # Overrides are keyed by the documented component role (e.g. 'db_connection');
    # the prefixed emitted key ('source_db_connection') is accepted as a fallback.
    component_names: Dict[str, str] = {}
    conn_name = _named(
        overrides, ROLE_DB_CONNECTION, primitive_component_key(_SOURCE_PREFIX, ROLE_DB_CONNECTION)
    )
    if conn_name:
        component_names["connection"] = conn_name
    read_name = _named(
        overrides, ROLE_DB_READ_PROFILE, primitive_component_key(_SOURCE_PREFIX, ROLE_DB_READ_PROFILE)
    )
    if read_name:
        component_names["read_profile"] = read_name
    op_name = _named(
        overrides, ROLE_DB_GET_OPERATION, primitive_component_key(_SOURCE_PREFIX, ROLE_DB_GET_OPERATION)
    )
    if op_name:
        component_names["get_operation"] = op_name

    return _coerce_primitive_params(
        DbExtractParameters,
        {
            "key_prefix": _SOURCE_PREFIX,
            "connection": connection,
            "read_profile": read_profile,
            "operation": operation,
            "component_names": component_names,
        },
        field="source",
    )


def _build_field_map_params(
    parameters: "DatabaseToApiSyncParameters", overrides: Dict[str, str]
) -> FieldMapParameters:
    read = parameters.source.read_operation
    transform = parameters.transform

    # The DB Select result fields are the source leaf index; their logical path
    # is the field name (a flat result set), matching the transform.map
    # source_path the direct/function/script routes emit.
    source_field_index = {
        f.name: {"data_type": f.data_type, "mappable": True}
        for f in read.result_schema.fields
    }
    source_profile_key = primitive_component_key(_SOURCE_PREFIX, ROLE_DB_READ_PROFILE)

    direct: List[Dict[str, Any]] = []
    map_function: List[Dict[str, Any]] = []
    map_script: List[Dict[str, Any]] = []
    for op in transform.operations:
        if isinstance(op, DirectTransformOperation):
            direct.append({"source_field": op.source_field, "target_path": op.target_path})
        elif isinstance(op, MapFunctionTransformOperation):
            entry: Dict[str, Any] = {
                "function_type": op.function_type,
                "inputs": list(op.inputs),
                "target_path": op.target_path,
            }
            if op.parameters:
                entry["parameters"] = dict(op.parameters)
            map_function.append(entry)
        elif isinstance(op, MapScriptTransformOperation):
            # script_component_ref points at a script component the archetype
            # cannot emit into this spec, so it would plan with a dangling
            # dependency (build_integration would reject it). M2 materializes a
            # map script only from an inline script_body (which field_map emits
            # as an in-spec script.mapping). Reject the ref with a clear error
            # instead of producing a non-plannable "executable" spec.
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
            # The contract's inputs are source field names and outputs are
            # target leaf paths; field_map's MapScriptOp needs named ports, so
            # derive input_name from the field name and output_name from the
            # leaf segment. field_map enforces that script_body is present.
            script_entry: Dict[str, Any] = {
                "inputs": [{"source_path": name, "input_name": name} for name in op.inputs],
                "outputs": [
                    {"output_name": path.rsplit("/", 1)[-1], "target_path": path}
                    for path in op.outputs
                ],
                "language": op.language,
            }
            if op.script_body is not None:
                script_entry["script_body"] = op.script_body
            map_script.append(script_entry)

    # Role-keyed overrides (e.g. 'target_profile', 'transform_map', 'script'),
    # with the prefixed emitted key accepted as a fallback.
    component_names: Dict[str, str] = {}
    target_profile_name = _named(
        overrides, ROLE_TARGET_PROFILE, primitive_component_key(_TRANSFORM_PREFIX, ROLE_TARGET_PROFILE)
    )
    if target_profile_name:
        component_names["target_profile"] = target_profile_name
    map_name = _named(
        overrides, ROLE_TRANSFORM_MAP, primitive_component_key(_TRANSFORM_PREFIX, ROLE_TRANSFORM_MAP)
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
                "source_profile_id": f"$ref:{source_profile_key}",
                "source_profile_type": "profile.db",
                "source_field_index": source_field_index,
            },
            "target_payload_profile": parameters.target.payload_profile.model_dump(),
            "direct": direct,
            "map_function": map_function,
            "map_script": map_script,
            "component_names": component_names,
        },
        field="transform",
    )


def _build_rest_send_params(
    parameters: "DatabaseToApiSyncParameters", overrides: Dict[str, str]
) -> RestSendWithRetryParameters:
    target = parameters.target
    binding = target.binding
    send = target.send_request

    if binding.mode == "create":
        settings = binding.settings  # guaranteed present by the contract validator
        auth = _REST_CREATE_AUTH_MAP.get(settings.auth_mode)
        if auth is None:
            raise BuilderValidationError(
                "REST create-mode auth is not supported for executable "
                "assembly in M2 (only an unauthenticated connection can be "
                "created); use an existing connection instead.",
                error_code=UNSUPPORTED_REST_AUTH_MODE,
                field="target.binding.settings.auth_mode",
                hint=(
                    "Set target.binding.mode='reuse' with an existing REST "
                    "Client connection (component_id or component_name) for "
                    "secured auth, or wait for a verified connector-auth "
                    "extension (#51). The archetype never echoes credentials."
                ),
            )
        connection: Dict[str, Any] = {
            "mode": "create",
            "base_url": settings.base_url,
            "auth": auth,
        }
        # default_headers has no RestConnectionCreate field — metadata-deferred.
    else:
        connection = {"mode": "reuse"}
        if binding.component_id:
            connection["component_id"] = binding.component_id
        if binding.component_name:
            connection["component_name"] = binding.component_name

    target_profile_key = primitive_component_key(_TRANSFORM_PREFIX, ROLE_TARGET_PROFILE)
    operation: Dict[str, Any] = {
        "method": send.method,
        "path": send.path,
        # Bind the operation request body to the generated JSON payload profile.
        "request_profile_id": f"$ref:{target_profile_key}",
        "request_profile_type": "json",
    }
    # Only literal query parameters are emitted onto the operation. Watermark-
    # sourced parameters need dynamic operation-property wiring (#51) and are
    # represented as operational intent, never as static query parameters.
    literal_qp = {
        qp.name: qp.literal_value
        for qp in send.query_parameters
        if qp.value_source == "literal" and qp.literal_value is not None
    }
    if literal_qp:
        operation["query_parameters"] = literal_qp

    # Role-keyed overrides (e.g. 'rest_connection', 'rest_operation'), with the
    # prefixed emitted key accepted as a fallback.
    component_names: Dict[str, str] = {}
    conn_name = _named(
        overrides, ROLE_REST_CONNECTION, primitive_component_key(_TARGET_PREFIX, ROLE_REST_CONNECTION)
    )
    if conn_name:
        component_names["connection"] = conn_name
    op_name = _named(
        overrides, ROLE_REST_OPERATION, primitive_component_key(_TARGET_PREFIX, ROLE_REST_OPERATION)
    )
    if op_name:
        component_names["operation"] = op_name

    return _coerce_primitive_params(
        RestSendWithRetryParameters,
        {
            "key_prefix": _TARGET_PREFIX,
            "connection": connection,
            "operation": operation,
            "component_names": component_names,
        },
        field="target",
    )


def _ref_dep_key(binding: Optional[str]) -> Optional[str]:
    """If a DLQ binding is a ``$ref:KEY`` token, return KEY (to add to the
    process ``depends_on`` so the builder's $ref-reachability walk passes);
    otherwise None (a literal component id needs no dependency edge)."""
    if isinstance(binding, str) and binding.startswith("$ref:"):
        key = binding[len("$ref:"):].strip()
        return key or None
    return None


def _derive_process_reliability(
    reliability: "ReliabilityConfig",
) -> Tuple[Dict[str, Any], Optional[str]]:
    """Map the caller's reliability config to the emitted process
    ``reliability`` block plus an optional ``$ref`` dependency key for the DLQ
    catch-leg binding.

    The emitted process always carries ``retry_count == 0`` — caller retry
    (max_attempts > 1) is recorded as intent only and remains gated for #51 R1b
    (retryCount→interval mapping). For ``retry_count == 0`` the builder emits
    the verified Try/Catch + DLQ catch path when a wired DLQ mode and its
    binding id are present (issue #51 M3.R1a). ``guidance_only`` and ``disabled``
    emit no Try/Catch.
    """
    dlq = reliability.dlq
    if not dlq.enabled or dlq.target is None:
        return {"retry_count": 0, "dlq": {"mode": "disabled"}}, None

    target = dlq.target
    if target.mode == "document_cache_ref":
        block = {
            "retry_count": 0,
            "dlq": {
                "mode": "document_cache_ref",
                "document_cache_id": target.document_cache_id,
            },
        }
        return block, _ref_dep_key(target.document_cache_id)
    if target.mode == "error_subprocess_ref":
        block = {
            "retry_count": 0,
            "dlq": {"mode": "error_subprocess_ref", "process_id": target.process_id},
        }
        return block, _ref_dep_key(target.process_id)

    # guidance_only — recorded as intent in operational_intent; no builder wiring.
    return {"retry_count": 0, "dlq": {"mode": "disabled"}}, None


def _build_main_process(
    parameters: "DatabaseToApiSyncParameters", overrides: Dict[str, str]
) -> IntegrationComponentSpec:
    naming = parameters.naming
    send = parameters.target.send_request
    reliability_block, dlq_dep_key = _derive_process_reliability(parameters.reliability)

    db_conn_key = primitive_component_key(_SOURCE_PREFIX, ROLE_DB_CONNECTION)
    db_op_key = primitive_component_key(_SOURCE_PREFIX, ROLE_DB_GET_OPERATION)
    map_key = primitive_component_key(_TRANSFORM_PREFIX, ROLE_TRANSFORM_MAP)
    rest_conn_key = primitive_component_key(_TARGET_PREFIX, ROLE_REST_CONNECTION)
    rest_op_key = primitive_component_key(_TARGET_PREFIX, ROLE_REST_OPERATION)

    process_name = (
        _named(overrides, "process", _MAIN_PROCESS_KEY)
        or f"{naming.component_prefix} DB to API Sync"
    )

    config: Dict[str, Any] = {
        "process_kind": "database_to_api_sync",
        "source": {
            "connector_type": "database",
            "connection_id": f"$ref:{db_conn_key}",
            "operation_id": f"$ref:{db_op_key}",
            "action_type": "Get",
        },
        "transform": {"mode": "map_ref", "map_ref": f"$ref:{map_key}"},
        "target": {
            "connector_type": "rest",
            "connection_id": f"$ref:{rest_conn_key}",
            "operation_id": f"$ref:{rest_op_key}",
            "action_type": send.method,
        },
        # Reliability derived from the caller's DLQ policy. For retry_count == 0
        # (always, here — caller retry is intent-only, gated for #51 R1b) a
        # verified DLQ mode (document_cache_ref / error_subprocess_ref) emits the
        # live Try/Catch + DLQ catch path via ProcessFlowBuilder (#51 M3.R1a);
        # disabled / guidance_only emit no Try/Catch.
        "reliability": reliability_block,
    }
    if naming.folder_path:
        config["folder_name"] = naming.folder_path

    # depends_on must contain exactly the keys referenced by $ref tokens in the
    # process config (ProcessFlowBuilder enforces this). The read profile and
    # target profile are depended transitively by the operation/map components.
    # A $ref DLQ binding adds one more edge so the catch-leg target is reachable.
    depends_on = [db_conn_key, db_op_key, map_key, rest_conn_key, rest_op_key]
    if dlq_dep_key is not None and dlq_dep_key not in depends_on:
        depends_on.append(dlq_dep_key)

    return IntegrationComponentSpec(
        key=_MAIN_PROCESS_KEY,
        type="process",
        action="create",
        name=process_name,
        config=config,
        depends_on=depends_on,
    )


def _default_dpp_name(field: str) -> str:
    """Deterministic default Dynamic Process Property name for a DPP watermark.

    The #29 contract carries no caller dpp_name, but #51 needs a stable property
    name to wire the watermark. Derived from the tracked field (sanitized to an
    identifier-safe token); marked ``dpp_name_generated`` in the metadata so a
    follow-up can honor or override it.
    """
    safe = re.sub(r"[^A-Za-z0-9]+", "_", field).strip("_") or "field"
    return f"watermark_{safe}"


def _watermark_intent(
    watermark: "Optional[Watermark]", context: PrimitiveBuildContext
) -> Dict[str, Any]:
    """Watermark strategy as metadata only (no executable update wiring in M2)."""
    if watermark is None:
        fragment = WatermarkStatePrimitive.emit_fragment(
            context,
            _coerce_primitive_params(
                WatermarkStateParameters, {"enabled": False}, field="execution.watermark"
            ),
        )
        return fragment["metadata"]["watermark"]

    if watermark.persistence == "dpp":
        params = _coerce_primitive_params(
            WatermarkStateParameters,
            {
                "enabled": True,
                "field": watermark.field,
                "kind": watermark.kind,
                "persistence": "dpp",
                # The contract has no dpp_name; supply a deterministic default so
                # #51 has a property name to wire (flagged generated below).
                "dpp_name": _default_dpp_name(watermark.field),
                "initial_value": watermark.initial_value,
            },
            field="execution.watermark",
        )
        fragment = WatermarkStatePrimitive.emit_fragment(context, params)
        intent = fragment["metadata"]["watermark"]
        intent["dpp_name_generated"] = True
        return intent

    # external_store: the contract carries no store_ref, so the primitive cannot
    # validate it. Represent the intent as metadata; store wiring is deferred.
    intent: Dict[str, Any] = {
        "enabled": True,
        "field": watermark.field,
        "kind": watermark.kind,
        "persistence": "external_store",
        "deferred_to": "#51",
        "note": "external-store watermark wiring (store reference) is deferred",
    }
    if watermark.initial_value is not None:
        intent["initial_value"] = watermark.initial_value
    return intent


def _deferred_intent(parameters: "DatabaseToApiSyncParameters") -> Dict[str, Any]:
    """Caller intent the current builders cannot emit — recorded, not dropped.

    Records only counts + notes (never the caller-authored keys/values) so the
    metadata can never echo a header value or JDBC option that might be
    sensitive.
    """
    deferred: Dict[str, Any] = {}
    sbind = parameters.source.binding
    if sbind.mode == "create" and sbind.settings and sbind.settings.jdbc_options:
        deferred["jdbc_options"] = {
            "count": len(sbind.settings.jdbc_options),
            "note": (
                "JDBC option map is not serialized onto the created connection "
                "in M2; use binding.mode='reuse' for connections needing JDBC "
                "URL options."
            ),
        }
    read = parameters.source.read_operation
    read_deferred: Dict[str, Any] = {}
    if read.fetch_size is not None:
        read_deferred["fetch_size"] = "metadata-only (no DB Get operation builder field in M2)"
    if read.link_element is not None:
        read_deferred["link_element"] = "metadata-only (no DB Get operation builder field in M2)"
    # The Select read profile only carries name + mappability; sql_type and a
    # non-default ('out') direction have no builder field, so the caller's
    # typing intent is preserved here rather than silently dropped. Bind
    # parameter names/types are SQL identifiers (not credentials), consistent
    # with how result-field names already surface in the contract flow summary.
    typed_parameters = [
        {"name": p.name, "sql_type": p.sql_type, "direction": p.direction}
        for p in (read.parameters or [])
        if p.sql_type is not None or p.direction != "in"
    ]
    if typed_parameters:
        read_deferred["bind_parameter_typing"] = typed_parameters
    if read_deferred:
        deferred["read_operation"] = read_deferred
    tbind = parameters.target.binding
    if tbind.mode == "create" and tbind.settings and tbind.settings.default_headers:
        deferred["default_headers"] = {
            "count": len(tbind.settings.default_headers),
            "note": "REST default headers are not emitted onto the created connection in M2.",
        }
    return deferred


def _build_operational_intent(
    parameters: "DatabaseToApiSyncParameters", context: PrimitiveBuildContext
) -> Dict[str, Any]:
    """Compose the operational primitives' fragments into intent metadata.

    Verified DLQ modes (document_cache_ref / error_subprocess_ref) ARE wired
    into the emitted process reliability (#51 M3.R1a); this records the matching
    intent. Retry (max_attempts>1) and guidance_only DLQ remain metadata-only.
    """
    execution = parameters.execution
    reliability = parameters.reliability
    send = parameters.target.send_request
    intent: Dict[str, Any] = {}

    # --- execution trigger (schedule) ---
    trigger = execution.trigger
    if trigger.mode == "scheduled" and trigger.schedule is not None:
        schedule_params = _coerce_primitive_params(
            ScheduleEnvelopeParameters,
            {
                "mode": "scheduled",
                "cron": trigger.schedule.cron,
                "timezone": trigger.schedule.timezone,
            },
            field="execution.trigger",
        )
    else:
        schedule_params = _coerce_primitive_params(
            ScheduleEnvelopeParameters, {"mode": "manual"}, field="execution.trigger"
        )
    schedule_fragment = ScheduleEnvelopePrimitive.emit_fragment(context, schedule_params)

    exec_intent: Dict[str, Any] = {}
    trigger_fragment = (
        schedule_fragment.get("process_config", {})
        .get("execution", {})
        .get("trigger")
    )
    if trigger_fragment:
        exec_intent["trigger"] = trigger_fragment
    schedule_meta = schedule_fragment.get("metadata", {}).get("schedule")
    if schedule_meta:
        intent["schedule"] = schedule_meta

    # --- run metadata ---
    if execution.run_metadata:
        run_fragment = RunMetadataPrimitive.emit_fragment(
            context,
            _coerce_primitive_params(
                RunMetadataParameters,
                {"static_metadata": dict(execution.run_metadata)},
                field="execution.run_metadata",
            ),
        )
        run_exec = run_fragment.get("process_config", {}).get("execution", {})
        if "run_metadata" in run_exec:
            exec_intent["run_metadata"] = run_exec["run_metadata"]
        dpp = run_fragment.get("metadata", {}).get("dynamic_process_properties")
        if dpp:
            exec_intent["dynamic_process_properties"] = dpp
    if exec_intent:
        intent["execution"] = exec_intent

    # --- watermark (metadata only) ---
    intent["watermark"] = _watermark_intent(execution.watermark, context)

    # --- reliability (error classifier + requested retry/DLQ intent) ---
    reliability_intent: Dict[str, Any] = {}
    classifier_fragment = ErrorClassifierPrimitive.emit_fragment(
        context,
        _coerce_primitive_params(
            ErrorClassifierParameters,
            {
                "retriable_status_codes": list(
                    reliability.error_classifier.retriable_status_codes
                ),
                "terminal_status_codes": list(
                    reliability.error_classifier.terminal_status_codes
                ),
                "custom_rules": list(reliability.error_classifier.custom_rules),
            },
            field="reliability.error_classifier",
        ),
    )
    classifier = (
        classifier_fragment.get("process_config", {})
        .get("reliability", {})
        .get("error_classifier")
    )
    if classifier:
        reliability_intent["error_classifier"] = classifier

    # Requested retry is recorded as intent only; the process stays retry_count=0.
    # retry > 1 mapping (retryCount→interval) is gated for #51 R1b.
    retry_intent: Dict[str, Any] = {
        "requested_max_attempts": reliability.retry.max_attempts,
        "backoff": reliability.retry.backoff,
        "process_retry_count": 0,
        "deferred_to": "#51 R1b",
    }
    if reliability.retry.initial_interval_seconds is not None:
        retry_intent["initial_interval_seconds"] = reliability.retry.initial_interval_seconds
    reliability_intent["retry"] = retry_intent

    # Record the DLQ block actually emitted into the process (truthful — the
    # archetype now wires verified DLQ modes directly into process.reliability;
    # disabled / guidance_only emit {"mode": "disabled"}). #51 M3.R1a.
    emitted_block, _ = _derive_process_reliability(reliability)
    reliability_intent["dlq"] = emitted_block["dlq"]
    if reliability.dlq.enabled and reliability.dlq.target is not None:
        target = reliability.dlq.target
        if target.mode in ("document_cache_ref", "error_subprocess_ref"):
            binding = (
                target.document_cache_id
                if target.mode == "document_cache_ref"
                else target.process_id
            )
            # The binding is a Boomi component id / $ref token — structural, not
            # a secret — so it is safe to record (it also appears in the spec).
            reliability_intent["dlq_requested"] = {
                "requested": True,
                "status": "emitted",
                "builder_mode": target.mode,
                "binding": binding,
            }
        else:  # guidance_only
            # Never echo the legacy address (may carry sensitive content) — only
            # its presence + the routing kind.
            reliability_intent["dlq_requested"] = {
                "requested": True,
                "status": "guidance_only",
                "kind": target.kind,
                "address_present": target.address is not None,
                "reason": target.reason,
                "note": (
                    "folder/topic/queue is not a verified builder DLQ mode; "
                    "recorded as guidance only (no wiring)."
                ),
            }
    intent["reliability"] = reliability_intent

    # --- expected status codes ---
    intent["expected_status_codes"] = list(send.expected_status_codes)

    # --- watermark-sourced query parameters (metadata only) ---
    # Watermark-bound query parameters are NOT emitted as static REST operation
    # query parameters; they need dynamic operation-property wiring (#51). Their
    # names are preserved here so the caller's intent is not lost between
    # build_from_archetype and the #51 follow-up.
    watermark_query_parameters = [
        {"name": qp.name, "bound_to": "watermark", "deferred_to": "#51"}
        for qp in send.query_parameters
        if qp.value_source == "watermark"
    ]
    if watermark_query_parameters:
        intent["watermark_query_parameters"] = watermark_query_parameters

    # --- deferred fields ---
    deferred = _deferred_intent(parameters)
    if deferred:
        intent["deferred"] = deferred

    return intent


class DatabaseToApiSyncArchetype(ArchetypePattern):
    metadata = PatternMetadata(
        name="database_to_api_sync",
        version="0.4.0",
        kind=PatternKind.ARCHETYPE,
        description=(
            "Archetype for replicating SQL Server records to a REST API on a "
            "manual or scheduled trigger. Validates parameters (caller-declared "
            "DB result fields, a caller-supplied JSON payload profile tree, and "
            "typed transform operations) and emits an executable "
            "IntegrationSpecV1 — DB source, JSON transform, REST target, and a "
            "structured process — suitable for build_integration(action='plan'). "
            "Every byte of XML is produced by the existing component builders; "
            "the archetype emits JSON component specs only and never calls "
            "Boomi. Emits a verified Try/Catch + DLQ catch path when DLQ is "
            "enabled with mode document_cache_ref or error_subprocess_ref "
            "(process retry_count stays 0; #51 M3.R1a). Schedule activation, "
            "watermark update, dynamic operation-property wiring, and "
            "retry>1+DLQ remain deferred (#51 R1b / M3)."
        ),
        tags=[
            "database",
            "rest",
            "sync",
            "m2",
            "executable",
            "sql-server",
            "no-boomi-mutation",
        ],
        use_cases=[
            "replicate SQL Server table changes to a REST API",
            "scheduled incremental sync with watermark",
        ],
        not_for=[
            "bidirectional sync",
            "real-time change-data-capture",
            "deploying, scheduling, or executing the process (M3)",
        ],
    )
    parameters_model = DatabaseToApiSyncParameters

    capability_notes = [
        "Discoverable, fully-typed parameter contract for a SQL Server -> REST sync.",
        "Strict per-field validation surfaces structured PARAM_VALIDATION_FAILED errors.",
        "Caller-declared DB result schema and caller-supplied JSON profile tree are the M2 source of truth.",
        "Emits executable component specs (DB source, JSON transform, REST target, process) for build_integration(action='plan').",
        "All XML is produced by the existing component builders; the archetype emits JSON component specs only.",
        "Emits a verified Try/Catch + DLQ catch path when dlq.enabled with mode document_cache_ref or error_subprocess_ref (process retry_count stays 0; #51 M3.R1a).",
        "Credentials cross the contract only as opaque credential_ref values and are never echoed in errors.",
    ]
    limitations = [
        "Emits JSON component specs only; performs no Boomi mutation and exposes no raw XML.",
        "DLQ wiring is emitted for mode document_cache_ref / error_subprocess_ref (process retry_count=0); legacy folder/topic/queue targets require mode='guidance_only' and are recorded as metadata only (no wiring).",
        "Combining caller retry (max_attempts>1) with emitted DLQ is gated for M4.5.3 (#51 R1b); the emitted process keeps retry_count=0 and records retry as intent.",
        "Schedule intent is represented as metadata only; deployment and schedule activation are M3.",
        "Watermark is represented as metadata only; watermark-update and dynamic operation-property wiring are deferred (#51).",
        "REST create-mode emits only auth='none'; secured auth (basic / bearer / oauth2) requires binding.mode='reuse'.",
        "DB create-mode supports auth_mode='username_password' only; 'windows_integrated' requires reuse (#51).",
        "jdbc_options and REST default_headers are metadata-deferred (no builder field in M2); use reuse for connections needing them.",
        "Does not mix map_function and map_script in one call (UNSUPPORTED_TRANSFORM_ROUTE); split into separate maps.",
        "Does not infer DB result fields from SQL, browse, metadata, or row samples; run infer_profile_fields (issue #47) separately for read-only inference from supplied metadata summaries / sample JSON / XSD / sample XML.",
        "Does not import existing integrations (issue #48 owns import / draft).",
        "operation_type='xslt' is rejected; the XSLT decision is owned by issue #42.",
        "credential_ref values are opaque end-to-end; the contract never resolves or validates secrets.",
    ]
    examples = [
        PatternExample(
            name="minimal_manual_sync",
            description=(
                "Smallest valid payload: create-mode SQL Server source with one "
                "declared result field, create-mode REST target with no auth and "
                "a one-leaf JSON payload profile, a single direct transform "
                "operation, manual trigger, DLQ disabled. Placeholder sentinels "
                "only — not a reusable template."
            ),
            parameters={
                "naming": {
                    "integration_name": "demo-db-to-api-sync",
                    "component_prefix": "DEMO",
                },
                "source": {
                    "binding": {
                        "mode": "create",
                        "settings": {
                            "driver": "microsoft_jdbc",
                            "auth_mode": "username_password",
                            "host": "db.internal",
                            "database": "AppDB",
                            "username": "svc_sync",
                            "credential_ref": "secrets/db/svc_sync",
                        },
                    },
                    "read_operation": {
                        "sql": _EXAMPLE_SQL_SENTINEL,
                        "result_schema": {
                            "fields": [
                                {
                                    "name": "source_field_a",
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
                            "base_url": "https://api.example.com",
                            "auth_mode": "none",
                        },
                    },
                    "send_request": {
                        "method": "POST",
                        "path": "/v1/items",
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
                            "source_field": "source_field_a",
                            "target_path": "Root/target_a",
                        },
                    ],
                },
                "execution": {
                    "trigger": {"mode": "manual"},
                },
                "reliability": {
                    "retry": {"max_attempts": 1},
                    "dlq": {"enabled": False},
                    "error_classifier": {},
                },
            },
        ),
        PatternExample(
            name="scheduled_with_watermark",
            description=(
                "Fuller payload: reuse-mode DB connection by component id with "
                "two declared result fields, reuse-mode REST target by "
                "component id (secured REST auth uses connection reuse in M2) "
                "and a nested JSON payload profile, two transform operations "
                "(one direct, one map_function), scheduled trigger, timestamp "
                "watermark, retry, DLQ enabled, and run metadata. Examples "
                "deliberately exclude map_script declarations to keep the "
                "published payload free of language tokens covered by the "
                "hygiene-marker guard."
            ),
            parameters={
                "naming": {
                    "integration_name": "demo-db-to-api-incremental",
                    "component_prefix": "DEMO-INC",
                    "folder_path": "Integrations/CRM/Sync",
                    "runtime_hints": {"atom_pool": "primary"},
                },
                "source": {
                    "binding": {
                        "mode": "reuse",
                        "component_id": "<<existing connector id>>",
                    },
                    "read_operation": {
                        "sql": _EXAMPLE_SQL_SENTINEL,
                        "result_schema": {
                            "fields": [
                                {
                                    "name": "source_a",
                                    "data_type": "character",
                                    "required": True,
                                },
                                {
                                    "name": "source_b",
                                    "data_type": "datetime",
                                },
                            ],
                        },
                        "parameters": [
                            {
                                "name": "<<bind parameter name>>",
                                "direction": "in",
                            },
                        ],
                        "batch_size": 500,
                    },
                },
                "target": {
                    "binding": {
                        "mode": "reuse",
                        "component_id": "<<existing REST connection id>>",
                    },
                    "send_request": {
                        "method": "POST",
                        "path": "/v1/customers",
                        "query_parameters": [
                            {
                                "name": "since",
                                "value_source": "watermark",
                            },
                        ],
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
                                    "required": True,
                                },
                                {
                                    "name": "target_b",
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
                            "source_field": "source_a",
                            "target_path": "Root/target_a",
                            "documentation_hint": "carry first column verbatim",
                        },
                        {
                            "operation_type": "map_function",
                            "function_type": "date_format",
                            "inputs": ["source_b"],
                            "target_path": "Root/target_b",
                            "parameters": {
                                "input_format": "<<source datetime format>>",
                                "output_format": "<<target datetime format>>",
                            },
                        },
                    ],
                },
                "execution": {
                    "trigger": {
                        "mode": "scheduled",
                        "schedule": {
                            "cron": "0 2 * * *",
                            "timezone": "UTC",
                        },
                    },
                    "watermark": {
                        "field": "source_b",
                        "kind": "timestamp",
                        "persistence": "dpp",
                    },
                    "run_metadata": {"owner": "crm-team"},
                },
                "reliability": {
                    "retry": {
                        "max_attempts": 5,
                        "backoff": "exponential",
                        "initial_interval_seconds": 2,
                    },
                    "dlq": {
                        "enabled": True,
                        "target": {
                            "mode": "document_cache_ref",
                            "document_cache_id": "<<dlq document cache component id>>",
                        },
                    },
                    "error_classifier": {
                        "custom_rules": ["rate_limit_exhausted"],
                    },
                },
            },
        ),
    ]

    @classmethod
    def emit_spec(
        cls, parameters: DatabaseToApiSyncParameters
    ) -> IntegrationSpecV1:
        naming = parameters.naming
        source_binding = parameters.source.binding
        target_binding = parameters.target.binding
        target_send = parameters.target.send_request
        result_schema = parameters.source.read_operation.result_schema
        payload_profile = parameters.target.payload_profile
        operations = parameters.transform.operations

        # Endpoint summaries — no SQL, no payload bodies, no resolved URLs.
        db_endpoint: Dict[str, Any] = {
            "key": "db_source",
            "type": "database",
            "direction": "source",
            "binding_mode": source_binding.mode,
            "executable": False,
        }
        if source_binding.mode == "create" and source_binding.settings is not None:
            db_endpoint["driver"] = source_binding.settings.driver
            db_endpoint["auth_mode"] = source_binding.settings.auth_mode
        else:
            if source_binding.component_id:
                db_endpoint["component_id"] = source_binding.component_id
            if source_binding.component_name:
                db_endpoint["component_name"] = source_binding.component_name

        rest_endpoint: Dict[str, Any] = {
            "key": "rest_target",
            "type": "rest",
            "direction": "target",
            "binding_mode": target_binding.mode,
            "method": target_send.method,
            "executable": False,
        }
        if target_binding.mode == "create" and target_binding.settings is not None:
            rest_endpoint["auth_mode"] = target_binding.settings.auth_mode
        else:
            if target_binding.component_id:
                rest_endpoint["component_id"] = target_binding.component_id
            if target_binding.component_name:
                rest_endpoint["component_name"] = target_binding.component_name

        # Source schema summary — names + data types only, no SQL or row data.
        source_schema_summary: Dict[str, Any] = {
            "field_count": len(result_schema.fields),
            "fields": [
                {
                    "name": f.name,
                    "data_type": f.data_type,
                    "required": f.required,
                }
                for f in result_schema.fields
            ],
        }

        # Target payload-profile summary — leaf path index + data type only,
        # never a raw JSON body sample.
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

        # Transform operations summary — route + full operand structure so
        # downstream issues (#26/#40/#41) can compile the right rung directly
        # from the spec without re-reading the original archetype payload.
        # For map_script: ``script_body`` round-trips verbatim alongside
        # ``script_body_present`` so #41 wrapper-synthesis tooling can
        # materialise the matching script.mapping component from spec
        # metadata alone (Codex r3 P2 #3 — dropping the body would be
        # data-loss between build_from_archetype and downstream
        # compilation).
        operation_summaries: List[Dict[str, Any]] = []
        for op in operations:
            if isinstance(op, DirectTransformOperation):
                summary: Dict[str, Any] = {
                    "operation_type": "direct",
                    "future_builder_issue": "#26",
                    "source_field": op.source_field,
                    "target_path": op.target_path,
                }
                if op.documentation_hint is not None:
                    summary["documentation_hint"] = op.documentation_hint
                operation_summaries.append(summary)
            elif isinstance(op, MapFunctionTransformOperation):
                summary = {
                    "operation_type": "map_function",
                    "future_builder_issue": "#40",
                    "function_type": op.function_type,
                    "inputs": list(op.inputs),
                    "input_count": len(op.inputs),
                    "target_path": op.target_path,
                }
                if op.parameters is not None:
                    summary["parameters"] = dict(op.parameters)
                if op.documentation_hint is not None:
                    summary["documentation_hint"] = op.documentation_hint
                operation_summaries.append(summary)
            elif isinstance(op, MapScriptTransformOperation):
                summary = {
                    "operation_type": "map_script",
                    "future_builder_issue": "#41",
                    "script_slot": op.script_slot,
                    "language": op.language,
                    "inputs": list(op.inputs),
                    "input_count": len(op.inputs),
                    "outputs": list(op.outputs),
                    "output_count": len(op.outputs),
                    # Presence boolean for quick contract checks. The actual
                    # body is round-tripped below when supplied — downstream
                    # build_integration / #41 wrapper synthesis needs the
                    # runnable content to materialise a script.mapping
                    # component, so dropping it here would be data-loss
                    # (Codex r3 P2 finding #3).
                    "script_body_present": op.script_body is not None,
                }
                if op.script_body is not None:
                    summary["script_body"] = op.script_body
                if op.script_component_ref is not None:
                    summary["script_component_ref"] = op.script_component_ref
                if op.documentation_hint is not None:
                    summary["documentation_hint"] = op.documentation_hint
                operation_summaries.append(summary)

        # Issue #43 (M2.5b): build deterministic, builder-ready profile field
        # payloads + path indexes from the validated DB result schema and JSON
        # payload profile tree, plus normalized direct mapping metadata. By the
        # time we reach emit_spec all the structural validation has already run
        # (Pydantic + _validate_transform_refs), so these helpers are expected to
        # succeed for any payload that passed validate_parameters; they are still
        # invoked through the same code path the issue #26 emission and issue #47
        # (infer_profile_fields) layers use, so any
        # divergence between the strict contract and the generation helpers
        # surfaces immediately rather than as a downstream builder failure.
        gen_artifacts = build_profile_generation_artifacts(
            result_schema,
            payload_profile,
            direct_operations=[
                op for op in operations
                if isinstance(op, DirectTransformOperation)
            ],
        )

        # Flow summaries — labels + new schema/operation metadata.
        flows: List[Dict[str, Any]] = [
            {
                "key": "extract",
                "name": "Read from database",
                "source": "db_source",
                "target": None,
                "operation": "db_get",
                "executable": False,
            },
            {
                "key": "transform",
                "name": "Map source to JSON payload",
                "source": "extract",
                "target": None,
                "operation": "transform",
                "executable": False,
                "source_schema": source_schema_summary,
                "target_payload_profile": target_profile_summary,
                "operations": operation_summaries,
                # Issue #43 (M2.5b): generated profile field payloads + indexes
                # consumed by issue #26 / #40 / #41 builders without
                # reimplementing field indexing per builder.
                "source_profile_generation": gen_artifacts["source"],
                "target_profile_generation": gen_artifacts["target"],
                "direct_field_mappings": gen_artifacts["direct_mappings"],
            },
            {
                "key": "send",
                "name": "Send to REST target",
                "source": "transform",
                "target": "rest_target",
                "operation": "rest_send",
                "executable": False,
            },
            {
                "key": "reliability",
                "name": "Retry / DLQ",
                "source": "send",
                "target": "dlq" if parameters.reliability.dlq.enabled else None,
                "operation": "reliability",
                "executable": False,
            },
        ]
        if parameters.execution.watermark is not None:
            flows.append(
                {
                    "key": "watermark",
                    "name": "Advance watermark",
                    "source": "send",
                    "target": None,
                    "operation": "watermark",
                    "executable": False,
                }
            )

        naming_block: Dict[str, Any] = {
            "archetype": "database_to_api_sync",
            "integration_name": naming.integration_name,
            "component_prefix": naming.component_prefix,
            "component_names": naming.component_names or {},
        }

        folders_block: Dict[str, Any] = (
            {"path": naming.folder_path} if naming.folder_path else {}
        )
        runtime_block: Dict[str, Any] = dict(naming.runtime_hints or {})

        # ---- Issue #29: executable component assembly --------------------
        # Compose the shipped #27 (db_extract, field_map) and #28
        # (rest_send_with_retry) primitives into the component list, then append
        # the structured process. Any BuilderValidationError raised here
        # (UNSUPPORTED_REST_AUTH_MODE, UNSUPPORTED_DB_AUTH_MODE,
        # UNSUPPORTED_TRANSFORM_ROUTE, SCRIPT_MAPPING_REF_REQUIRED, …) propagates
        # to the authoring layer, which returns a structured PatternError
        # without echoing caller parameters.
        overrides = _component_names(naming)
        context = PrimitiveBuildContext(
            integration_name=naming.integration_name,
            component_prefix=naming.component_prefix,
            folder_path=naming.folder_path,
        )

        components: List[IntegrationComponentSpec] = []
        components.extend(
            DbExtractPrimitive.emit_components(
                context, _build_db_extract_params(parameters, overrides)
            )
        )
        components.extend(
            FieldMapPrimitive.emit_components(
                context, _build_field_map_params(parameters, overrides)
            )
        )
        components.extend(
            RestSendWithRetryPrimitive.emit_components(
                context, _build_rest_send_params(parameters, overrides)
            )
        )
        components.append(_build_main_process(parameters, overrides))

        operational_intent = _build_operational_intent(parameters, context)

        return IntegrationSpecV1(
            version="1.0",
            name=naming.integration_name,
            mode="redesign",
            components=components,
            goals=[
                "Replicate data from a SQL Server source to a REST target on a "
                "manual or scheduled trigger.",
                "Emit executable component specs (DB source, JSON transform, "
                "REST target, structured process) for build_integration("
                "action='plan'); a verified Try/Catch + DLQ catch path is "
                "emitted for DLQ modes document_cache_ref / error_subprocess_ref "
                "(retry_count=0; #51 M3.R1a). Deployment, schedule activation, "
                "and retry>1+DLQ remain M3 / #51 R1b.",
            ],
            endpoints=[db_endpoint, rest_endpoint],
            flows=flows,
            naming=naming_block,
            folders=folders_block,
            runtime=runtime_block,
            validation_rules={
                "contract_only": False,
                "component_count": len(components),
                "raw_xml_exposed": False,
                "boomi_mutation": False,
                "metadata_version": "0.4.0",
                # Representation of trigger / schedule / watermark / retry intent /
                # DLQ intent / error classifier / run metadata / expected status
                # codes / deferred follow-up notes. Verified DLQ modes are wired
                # into the process reliability (#51 M3.R1a); retry>1 stays gated.
                "operational_intent": operational_intent,
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
                    "schedule_activation": "M3 (deploy to a runtime first)",
                    "process_dlq": "emitted for document_cache_ref / error_subprocess_ref (retry_count=0; #51 M3.R1a)",
                    "process_retry_gt1": "#51 R1b (retry max_attempts>1 mapping; emitted retry_count stays 0)",
                    "watermark_update": "#51 (dynamic operation-property wiring)",
                    "db_create_auth": (
                        "username_password only; windows_integrated requires reuse"
                    ),
                    "rest_create_auth": (
                        "auth='none' only; secured auth requires reuse"
                    ),
                    "jdbc_options_and_default_headers": (
                        "metadata-deferred; no builder field in M2 (use reuse)"
                    ),
                },
                "profile_schema_strategy": (
                    "M2 uses caller-declared DB read result fields and a "
                    "caller-supplied JSON profile tree for the REST target; "
                    "no SQL parsing, DB metadata introspection, row sampling, "
                    "or Boomi browse is performed by this archetype. Issue "
                    "#43 generates deterministic source/target profile field "
                    "indexes and normalized direct mapping metadata on the "
                    "transform flow for downstream profile/map builders "
                    "(issue #26); metadata/sample inference is available "
                    "separately via infer_profile_fields (issue #47)."
                ),
                "transform_routes": {
                    "direct": "#26",
                    "map_function": "#40",
                    "map_script": "#41",
                    "xslt": "#42 (rejected in M2)",
                },
            },
        )
