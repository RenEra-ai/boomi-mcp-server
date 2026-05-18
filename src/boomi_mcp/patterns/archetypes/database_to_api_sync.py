"""Issue #21: contract-only database_to_api_sync archetype (M2.1).

Exposes a strict Pydantic parameter contract for a SQL Server source → REST
target sync. The archetype validates the schema and emits a non-executable
IntegrationSpecV1 (zero components). Executable component emission is owned
by M2.9; this file deliberately does not touch any builder, profile, or live
Boomi account.
"""

from __future__ import annotations

import re
from typing import Any, Dict, List, Literal, Optional

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from ...models.integration_models import IntegrationSpecV1
from ..base import ArchetypePattern, PatternExample, PatternKind, PatternMetadata


# ---------------------------------------------------------------------------
# Reusable validators
# ---------------------------------------------------------------------------


_URL_RE = re.compile(r"^https?://\S+$", re.IGNORECASE)


def _stripped_nonblank(value: str) -> str:
    stripped = value.strip()
    if not stripped:
        raise ValueError("must not be blank")
    return stripped


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
            "Prefix applied to every emitted Boomi component name in M2.9. "
            "Recorded under spec.naming.component_prefix; the contract emits "
            "zero components, so this is reserved for the executable stage."
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
            "in M2.9 (e.g. 'Integrations/CRM/Sync'). Echoed in spec.folders.path "
            "without normalization."
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
            "'trustServerCertificate' -> 'true'). Surfaced verbatim to the "
            "M2.9 builder; the contract does not interpret keys."
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
            "How to materialize the database connector in M2.9. 'create' builds "
            "a new Boomi connector from settings; 'reuse' references an existing "
            "connector by component_id or component_name."
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
            "contract does not validate the value; M2.9 passes it through to "
            "the database operation profile."
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
            "User- or LLM-authored SELECT statement executed against the "
            "source database. The contract never generates SQL and never "
            "rewrites the value; it only validates that the string is "
            "non-blank."
        ),
    )
    parameters: List[DbReadParameter] = Field(
        default_factory=list,
        description=(
            "Bind parameters referenced by the SQL statement. The contract "
            "does not parse the SQL — supplying parameters here is purely "
            "declarative for the M2.9 builder."
        ),
    )
    batch_size: Optional[int] = Field(
        default=None,
        ge=1,
        description=(
            "Optional batch size for the database read operation. Surfaced "
            "verbatim to M2.9; the contract does not impose a maximum."
        ),
    )
    fetch_size: Optional[int] = Field(
        default=None,
        ge=1,
        description=(
            "Optional JDBC fetch size hint for streaming large result sets. "
            "Surfaced verbatim to M2.9."
        ),
    )
    max_rows: Optional[int] = Field(
        default=None,
        ge=1,
        description=(
            "Optional ceiling on the number of rows returned per execution. "
            "Surfaced verbatim to M2.9; the contract does not enforce it."
        ),
    )
    link_element: Optional[str] = Field(
        default=None,
        description=(
            "Optional name of a link element used when the database operation "
            "feeds into a downstream nested call. Surfaced verbatim to M2.9."
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
            "The database read operation (SQL, bind parameters, batching "
            "hints) that produces records for transformation and send."
        ),
    )


# ---------------------------------------------------------------------------
# Target — REST
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
            "to M2.9."
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
            "How to materialize the REST connector in M2.9. 'create' builds "
            "a new Boomi connector from settings; 'reuse' references an "
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


# ---------------------------------------------------------------------------
# Transform
# ---------------------------------------------------------------------------


class FieldMapping(BaseModel):
    model_config = ConfigDict(extra="forbid")

    source_field: str = Field(
        ...,
        description=(
            "Source field name as produced by the database read operation."
        ),
    )
    target_field: str = Field(
        ...,
        description=(
            "Target field name as expected by the REST send request payload."
        ),
    )
    transform_hint: Optional[str] = Field(
        default=None,
        description=(
            "Optional short, free-form hint describing the per-field "
            "transformation (e.g. 'trim', 'uppercase'). The contract does "
            "not interpret or execute the value; it is surfaced verbatim to "
            "M2.9. No scripts or templating syntax."
        ),
    )

    @field_validator("source_field", "target_field")
    @classmethod
    def _strip_required(cls, value: str) -> str:
        return _stripped_nonblank(value)

    @field_validator("transform_hint")
    @classmethod
    def _strip_optional(cls, value: Optional[str]) -> Optional[str]:
        if value is None:
            return None
        return _stripped_nonblank(value)


class TransformConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    mappings: List[FieldMapping] = Field(
        ...,
        min_length=1,
        description=(
            "One or more field mappings from source rows to target payload. "
            "At least one mapping is required; the contract never invents "
            "mappings or generates a template."
        ),
    )
    payload_template: Optional[str] = Field(
        default=None,
        description=(
            "Optional user- or LLM-authored payload template for the REST "
            "request body. The contract has no default and never generates "
            "this value; M2.9 treats it as an opaque string."
        ),
    )
    script_slots: Dict[str, str] = Field(
        default_factory=dict,
        description=(
            "Optional named user-authored script slots (e.g. 'pre_send', "
            "'post_send'). Keys are role names; values are opaque scripts "
            "that M2.9 may attach to the corresponding integration shape. "
            "The contract never parses or executes scripts."
        ),
    )

    @field_validator("payload_template")
    @classmethod
    def _strip_optional(cls, value: Optional[str]) -> Optional[str]:
        if value is None:
            return None
        return _stripped_nonblank(value)


# ---------------------------------------------------------------------------
# Execution
# ---------------------------------------------------------------------------


class Schedule(BaseModel):
    model_config = ConfigDict(extra="forbid")

    cron: str = Field(
        ...,
        description=(
            "Cron expression for the scheduled trigger. The contract does "
            "not parse or validate the cron syntax; M2.9 passes it to the "
            "Boomi schedule shape verbatim."
        ),
    )
    timezone: Optional[str] = Field(
        default=None,
        description=(
            "Optional IANA timezone string (e.g. 'UTC', 'America/New_York'). "
            "Surfaced verbatim to M2.9; the contract does not validate it."
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
            "scheduler in M2.9."
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
            "Source column or output field that drives the high-water-mark "
            "advancement (e.g. 'last_modified_at')."
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
            "before any state has been persisted. Surfaced verbatim to M2.9."
        ),
    )
    persistence: Literal["dpp", "external_store"] = Field(
        default="dpp",
        description=(
            "Where the watermark is persisted. 'dpp' uses Boomi Dynamic "
            "Process Properties; 'external_store' delegates to an external "
            "key/value store whose binding is configured in M2.9."
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
            "(e.g. business owner, runbook URL). Surfaced verbatim to M2.9; "
            "the contract does not interpret keys."
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
            "contract surfaces this value verbatim; M2.9 wires the retry shape."
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
            "backoffs. The contract does not enforce a default; M2.9 picks "
            "one when this value is omitted."
        ),
    )


class DlqTarget(BaseModel):
    model_config = ConfigDict(extra="forbid")

    kind: Literal["folder", "topic", "queue"] = Field(
        ...,
        description=(
            "Kind of dead-letter destination: a Boomi folder, a messaging "
            "topic, or a queue. The contract does not validate the target's "
            "existence; M2.9 wires the destination."
        ),
    )
    address: str = Field(
        ...,
        description=(
            "Address of the dead-letter destination (e.g. folder path, topic "
            "name, queue URL). Surfaced verbatim to M2.9."
        ),
    )

    @field_validator("address")
    @classmethod
    def _strip_address(cls, value: str) -> str:
        return _stripped_nonblank(value)


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
            "behavior to be implemented in M2.9 (e.g. 'rate_limit_exhausted'). "
            "Values are opaque labels; no scripts."
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
            "Database source configuration: connector binding and read "
            "operation."
        ),
    )
    target: RestTarget = Field(
        ...,
        description=(
            "REST target configuration: connector binding and send request."
        ),
    )
    transform: TransformConfig = Field(
        ...,
        description=(
            "Field mappings, optional payload template, and optional named "
            "script slots that move source records into the target payload."
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


# ---------------------------------------------------------------------------
# Archetype
# ---------------------------------------------------------------------------


# Example payload sentinels — these intentionally do NOT look like real SQL,
# OData filters, SOAP envelopes, REST payloads, field mappings, or scripts.
# They exist only to demonstrate the parameter shape.
_EXAMPLE_SQL_SENTINEL = "<<user-authored DB read query>>"
_EXAMPLE_PAYLOAD_SENTINEL = "<<user-authored REST body template>>"


class DatabaseToApiSyncArchetype(ArchetypePattern):
    metadata = PatternMetadata(
        name="database_to_api_sync",
        version="0.1.0",
        kind=PatternKind.ARCHETYPE,
        description=(
            "Contract-only archetype for replicating SQL Server records to a "
            "REST API on a manual or scheduled trigger. Validates parameters "
            "and emits a non-executable IntegrationSpecV1; executable "
            "component emission is owned by M2.9."
        ),
        tags=[
            "database",
            "rest",
            "sync",
            "m2",
            "contract-only",
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
            "executable component emission before M2.9",
        ],
    )
    parameters_model = DatabaseToApiSyncParameters

    capability_notes = [
        "Discoverable, fully-typed parameter contract for a SQL Server -> REST sync.",
        "Strict per-field validation surfaces structured PARAM_VALIDATION_FAILED errors.",
        "Credentials cross the contract only as opaque credential_ref values.",
        "Emits a zero-component IntegrationSpecV1; M2.9 owns executable component emission.",
    ]
    limitations = [
        "Emits no executable Boomi components and performs no Boomi mutation.",
        "Does not expose or generate raw XML.",
        "SQL Server is the only supported database family in M2.1; Postgres / Oracle / Snowflake are deferred.",
        "credential_ref values are opaque end-to-end; the contract never resolves or validates secrets.",
    ]
    examples = [
        PatternExample(
            name="minimal_manual_sync",
            description=(
                "Smallest valid payload: create-mode SQL Server source, create-mode "
                "REST target with no auth, a single field mapping, manual trigger, "
                "DLQ disabled. Demonstrates the parameter shape only; the SQL and "
                "payload template are placeholders, not reusable templates."
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
                },
                "transform": {
                    "mappings": [
                        {
                            "source_field": "<<source field name>>",
                            "target_field": "<<target field name>>",
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
                "Fuller payload: reuse-mode DB connection by component id, "
                "create-mode REST target with bearer-token credential_ref, "
                "scheduled trigger, timestamp watermark, retry, DLQ enabled, "
                "and run metadata. SQL / mappings remain placeholder sentinels "
                "to keep the example free of reusable template content."
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
                        "parameters": [
                            {"name": "<<bind parameter name>>", "direction": "in"},
                        ],
                        "batch_size": 500,
                    },
                },
                "target": {
                    "binding": {
                        "mode": "create",
                        "settings": {
                            "base_url": "https://api.example.com",
                            "auth_mode": "bearer_token",
                            "credential_ref": "secrets/rest/bearer",
                            "default_headers": {"Accept": "application/json"},
                        },
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
                },
                "transform": {
                    "mappings": [
                        {"source_field": "<<source field a>>", "target_field": "<<target field a>>"},
                        {"source_field": "<<source field b>>", "target_field": "<<target field b>>"},
                    ],
                    "payload_template": _EXAMPLE_PAYLOAD_SENTINEL,
                    "script_slots": {"pre_send": "<<user-authored hook body>>"},
                },
                "execution": {
                    "trigger": {
                        "mode": "scheduled",
                        "schedule": {"cron": "<<cron expression>>", "timezone": "UTC"},
                    },
                    "watermark": {
                        "field": "<<watermark column>>",
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
                            "kind": "queue",
                            "address": "<<dlq queue address>>",
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

        # Flow summaries — labels only.
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
                "name": "Map / template payload",
                "source": "extract",
                "target": None,
                "operation": "transform",
                "executable": False,
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

        return IntegrationSpecV1(
            version="1.0",
            name=naming.integration_name,
            mode="redesign",
            components=[],
            goals=[
                "Replicate data from a SQL Server source to a REST target on a "
                "manual or scheduled trigger.",
                "Executable Boomi components are deferred to M2.9; this build "
                "emits the contract only.",
            ],
            endpoints=[db_endpoint, rest_endpoint],
            flows=flows,
            naming=naming_block,
            folders=folders_block,
            runtime=runtime_block,
            validation_rules={
                "contract_only": True,
                "component_count": 0,
                "raw_xml_exposed": False,
                "boomi_mutation": False,
                "requires_m2_9_for_executable_components": True,
            },
        )
