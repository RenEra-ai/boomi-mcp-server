"""Neutral archetype parameter contracts (issue #151, M12.14).

The strict Pydantic parameter models and their reusable validators, lifted out of
``patterns/archetypes/database_to_api_sync.py`` and
``patterns/archetypes/api_to_api_sync.py`` so the archetypes that SURVIVE the
M12.22 (#160) deletion can import their schemas without importing a module that
is scheduled for removal.

Nothing here is an :class:`ArchetypePattern` or :class:`PrimitivePattern` subclass:
``PatternRegistry.from_package`` walks every module in ``boomi_mcp.patterns`` and
registers every pattern class it finds in a module's globals, with no
``__module__`` filter, so defining or importing one here would silently add a
catalog entry.

This module must not import anything under ``patterns.archetypes`` or
``patterns.composition`` — that dependency direction is the whole point of the
extraction — and must not call a recipe engine entry point (``run_recipes`` /
``run_sync_preset_recipe`` / ``run_fanout_recipe``): the served recipe-layer digest
tracks the modules that invoke them, and this module is deliberately not one.

Assembly helpers built ON these models live in ``patterns/archetype_assembly.py``;
that module imports this one and never the reverse.
"""

from __future__ import annotations

import re
from typing import Annotated, Any, Dict, List, Literal, Optional, Set, Union

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    StrictBool,
    field_validator,
    model_validator,
)


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
# this list. Codex review r1 P2 / issue #127 B1: map_function.parameters and
# naming.runtime_hints are the schema-opaque dicts the archetype echoes back
# into IntegrationSpecV1 on success (the former under the transform operation,
# the latter under spec.runtime), so plaintext secret keys must be rejected at
# parameter-validation time before they can leak through the spec.
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
    convention: Optional[str] = Field(
        default=None,
        description=(
            "Optional component-naming convention to activate in the emitted "
            "spec's naming block (issue #102 D1). Set to 'bracketed' to have "
            "build_integration's name-governance lint flag names that do not "
            "follow the bracketed account convention. Default None (off) — the "
            "archetype emits descriptive names that are not bracketed, so "
            "activating it by default would flag the archetype's own output; it "
            "is the caller's opt-in."
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

    @field_validator("runtime_hints")
    @classmethod
    def _reject_plaintext_secret_keys(
        cls, value: Optional[Dict[str, Any]]
    ) -> Optional[Dict[str, Any]]:
        # runtime_hints is echoed verbatim under spec.runtime (issue #127 B1),
        # so reject plaintext secret-shaped keys at any nesting depth before
        # they can leak through the emitted spec — the same concern the
        # map_function.parameters scan exists to prevent. The offending key
        # name is NOT echoed back through the error envelope; callers route
        # connector secrets via the connection binding's credential_ref.
        if value is None:
            return None
        if _scan_for_secret_shaped_keys(value):
            raise ValueError(
                "naming.runtime_hints contains a key whose name matches a "
                "forbidden secret-shaped substring (e.g. password / token / "
                "secret / api_key / bearer / authorization). Reference "
                "connector secrets via the connection binding's credential_ref "
                "instead; naming.runtime_hints is echoed back in the emitted "
                "IntegrationSpec and must not carry plaintext secrets."
            )
        return value


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
            "Existing Boomi connector component id to reuse. Supply exactly one "
            "of component_id or component_name when mode='reuse'; must be "
            "omitted when mode='create'."
        ),
    )
    component_name: Optional[str] = Field(
        default=None,
        description=(
            "Existing Boomi connector component name to reuse (resolved at "
            "execution time). Supply exactly one of component_id or "
            "component_name when mode='reuse'; must be omitted when "
            "mode='create'."
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
            # Issue #127 B2: require exactly one reuse identifier. Both-present
            # passed the public contract before and was only rejected far
            # downstream; reject it here so the origin is the binding.
            identifiers = [self.component_id, self.component_name]
            if sum(1 for value in identifiers if value) != 1:
                raise ValueError(
                    "mode='reuse' requires exactly one of component_id or "
                    "component_name"
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
            "Existing Boomi connector component id to reuse. Supply exactly one "
            "of component_id or component_name when mode='reuse'; must be "
            "omitted when mode='create'."
        ),
    )
    component_name: Optional[str] = Field(
        default=None,
        description=(
            "Existing Boomi connector component name to reuse (resolved at "
            "execution time). Supply exactly one of component_id or "
            "component_name when mode='reuse'; must be omitted when "
            "mode='create'."
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
            # Issue #127 B2: require exactly one reuse identifier (see
            # DbConnectionBinding). Both-present passed the public contract
            # before and was only rejected downstream.
            identifiers = [self.component_id, self.component_name]
            if sum(1 for value in identifiers if value) != 1:
                raise ValueError(
                    "mode='reuse' requires exactly one of component_id or "
                    "component_name"
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


class RestPathReplacement(BaseModel):
    """A single dynamic path-segment binding for the REST send request.

    Issue #100 G2: the Boomi REST Client connector cannot declare in-operation
    URL path parameters (that is an HTTP Client feature). A per-document path is
    instead supplied at process time via the connector step's "Path" dynamic
    operation property, whose value is built by a Set Properties step that
    concatenates the static path segments with the mapped leaf value(s). Each
    replacement maps a ``{name}`` token in ``RestSendRequest.path`` to a mapped
    target leaf (``target_path``). Grounded in a live REST Client export.
    """
    # Evidence: .codex/plans/issue-100-live-captures.md. Kept in a comment, not
    # in the docstring: pydantic publishes the docstring as this model's served
    # `description`, and no MCP tool can fetch a repository path (#146).

    model_config = ConfigDict(extra="forbid")

    name: str = Field(
        ...,
        description=(
            "Replacement token name. Must appear literally as '{name}' in "
            "RestSendRequest.path. Case-sensitive; the per-endpoint Dynamic "
            "Document Property name is derived from the path resource."
        ),
    )
    target_path: str = Field(
        ...,
        description=(
            "Logical leaf path inside target.payload_profile whose mapped value "
            "is bound into the path at the '{name}' position. Must be a declared "
            "simple leaf that a transform output binds."
        ),
    )

    @field_validator("name", "target_path")
    @classmethod
    def _strip_required(cls, value: str) -> str:
        return _stripped_nonblank(value)

    @field_validator("name")
    @classmethod
    def _reject_brace_in_name(cls, value: str) -> str:
        # Issue #127 B3 (review r1): a replacement name that itself contains a
        # brace (e.g. 'clientId}{region') would let the residual-brace check's
        # token stripping erase unrelated '{token}'s from the path, masking
        # malformed input and deferring rejection to the build layer. A valid
        # token name never contains '{' or '}', so reject it here at the
        # contract layer (PARAM_VALIDATION_FAILED).
        if "{" in value or "}" in value:
            raise ValueError(
                "path_replacements name must not contain '{' or '}'"
            )
        return value


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
            "leading or trailing slashes. May contain '{name}' tokens bound by "
            "path_replacements for per-document dynamic paths (issue #100 G2)."
        ),
    )
    query_parameters: List[RestQueryParameter] = Field(
        default_factory=list,
        description=(
            "Optional query-string parameters. Each entry declares its name "
            "and where its value comes from at execution time."
        ),
    )
    path_replacements: List[RestPathReplacement] = Field(
        default_factory=list,
        description=(
            "Optional per-document dynamic path bindings (issue #100 G2). Each "
            "maps a '{name}' token in 'path' to a mapped target leaf. When "
            "empty, the path is sent verbatim (static, byte-for-byte the "
            "pre-#100 behavior)."
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


# A `{token}` in a fetch/send path signals a per-document dynamic path — runtime
# path binding is #96 (M5.4a) and is OUT of scope for this M5.7 preset (and the
# thin sync_pipeline stage rejects runtime_bindings). Reject it at the contract.
_DYNAMIC_PATH_TOKEN_RE = re.compile(r"\{[^{}]*\}")


def _reject_dynamic_path(value: str) -> str:
    """Reject a `{token}` dynamic path (runtime path binding is #96, out of scope).

    Shared by the static-REST presets (api_to_api_sync, api_to_database_sync), so
    the message stays preset-neutral — it names no single preset/milestone.
    """
    stripped = _stripped_nonblank(value)
    if _DYNAMIC_PATH_TOKEN_RE.search(stripped):
        raise ValueError(
            "path must be static for this sync_pipeline preset; a "
            "'{token}' per-document dynamic path is runtime-bound behavior owned "
            "by #96 (M5.4a) and is not exposed here. Use a static path."
        )
    return stripped


# ---------------------------------------------------------------------------
# Source / target request contracts (static REST, no runtime binding)
# ---------------------------------------------------------------------------


class ApiFetchRequest(BaseModel):
    """Static REST GET fetch-request configuration for the source stage."""

    model_config = ConfigDict(extra="forbid")

    path: str = Field(
        ...,
        description=(
            "Static endpoint path appended to the source connection base_url "
            "(e.g. '/v1/customers'). Must be non-blank and must NOT contain a "
            "'{token}' dynamic segment — runtime path binding is #96 (M5.4a) and "
            "is out of scope for this preset."
        ),
    )
    query_parameters: Optional[Dict[str, str]] = Field(
        default=None,
        description=(
            "Optional static query-string parameters applied to the GET request. "
            "REST Client query parameters are static (Boomi UI verified); a "
            "per-request dynamic value is out of scope (#96)."
        ),
    )
    request_headers: Optional[Dict[str, str]] = Field(
        default=None,
        description="Optional static request headers applied to the GET request.",
    )
    follow_redirects: Optional[str] = Field(
        default=None, description="Redirect policy: NONE | STRICT | LAX."
    )
    return_application_errors: Optional[StrictBool] = Field(
        default=None,
        description="Whether the operation surfaces application-level errors instead of failing.",
    )
    track_response: Optional[StrictBool] = Field(
        default=None, description="Whether the connector tracks the response document."
    )

    @field_validator("path")
    @classmethod
    def _validate_static_path(cls, value: str) -> str:
        return _reject_dynamic_path(value)


class ApiSendRequest(BaseModel):
    """Static REST send-request configuration for the target stage."""

    model_config = ConfigDict(extra="forbid")

    method: Literal["GET", "POST", "PUT", "PATCH", "DELETE"] = Field(
        default="POST",
        description="HTTP method used for the REST send request.",
    )
    path: str = Field(
        ...,
        description=(
            "Static endpoint path appended to the target connection base_url "
            "(e.g. '/v1/items'). Must be non-blank and must NOT contain a "
            "'{token}' dynamic segment — runtime path binding is #96 (M5.4a) and "
            "is out of scope for this preset."
        ),
    )
    query_parameters: Optional[Dict[str, str]] = Field(
        default=None,
        description="Optional static query-string parameters applied to the send request.",
    )
    request_headers: Optional[Dict[str, str]] = Field(
        default=None,
        description="Optional static request headers applied to the send request.",
    )
    expected_status_codes: List[int] = Field(
        default_factory=lambda: [200, 201, 202],
        description=(
            "HTTP status codes considered successful for the send (recorded as "
            "metadata; routing of other codes through retry/DLQ is out of scope "
            "for this preset). Defaults to 200/201/202."
        ),
    )

    @field_validator("path")
    @classmethod
    def _validate_static_path(cls, value: str) -> str:
        return _reject_dynamic_path(value)


class ApiSource(BaseModel):
    model_config = ConfigDict(extra="forbid")

    binding: RestConnectionBinding = Field(
        ...,
        description=(
            "How the REST SOURCE connector is materialized (create new settings "
            "or reuse an existing Boomi component). Create-mode supports auth "
            "'none' only; secured auth requires mode='reuse'."
        ),
    )
    fetch_request: ApiFetchRequest = Field(
        ...,
        description="Static REST GET fetch-request configuration for the source.",
    )
    response_profile: JSONPayloadProfile = Field(
        ...,
        description=(
            "Caller-supplied JSON profile tree describing the source API response "
            "body. The preset emits a generated JSON profile from this tree and "
            "binds it as the fetch source's output shape; transform source_path "
            "references resolve against its simple leaves."
        ),
    )


class ApiTarget(BaseModel):
    model_config = ConfigDict(extra="forbid")

    binding: RestConnectionBinding = Field(
        ...,
        description=(
            "How the REST TARGET connector is materialized (create new settings "
            "or reuse an existing Boomi component). Create-mode supports auth "
            "'none' only; secured auth requires mode='reuse'."
        ),
    )
    send_request: ApiSendRequest = Field(
        ...,
        description="Static REST send-request configuration for the target.",
    )
    payload_profile: JSONPayloadProfile = Field(
        ...,
        description=(
            "Caller-supplied JSON profile tree describing the target request "
            "body. The preset generates a JSON profile + transform map from it; "
            "only kind='simple' leaves are valid transform targets."
        ),
    )


# ---------------------------------------------------------------------------
# Transform — discriminated typed operations (source_path based, M5.7)
# ---------------------------------------------------------------------------


class _BaseApiTransformOperation(BaseModel):
    model_config = ConfigDict(extra="forbid")

    documentation_hint: Optional[str] = Field(
        default=None,
        description=(
            "Optional non-executable human-readable note about the operation's "
            "intent. Downstream builders must not parse or execute the value."
        ),
    )

    @field_validator("documentation_hint")
    @classmethod
    def _strip_optional_hint(cls, value: Optional[str]) -> Optional[str]:
        if value is None:
            return None
        return _stripped_nonblank(value)


class DirectApiTransformOperation(_BaseApiTransformOperation):
    operation_type: Literal["direct"] = Field(
        ...,
        description="Discriminator: 'direct' routes to a one-to-one Boomi map step (#26).",
    )
    source_path: str = Field(
        ...,
        description=(
            "Logical leaf path inside source.response_profile (slash-separated, "
            "e.g. 'Root/id' or 'Root/items[]/sku'). Must reference a kind='simple' "
            "leaf; the cross-field validator rejects unknown paths."
        ),
    )
    target_path: str = Field(
        ...,
        description=(
            "Logical leaf path inside target.payload_profile. Must reference a "
            "kind='simple' leaf; object and array nodes cannot be transform targets."
        ),
    )

    @field_validator("source_path", "target_path")
    @classmethod
    def _strip_required(cls, value: str) -> str:
        return _stripped_nonblank(value)


class MapFunctionApiTransformOperation(_BaseApiTransformOperation):
    operation_type: Literal["map_function"] = Field(
        ...,
        description="Discriminator: 'map_function' routes to a Boomi map function step (#40).",
    )
    function_type: str = Field(
        ...,
        description=(
            "Task-authored function route name (e.g. 'trim', 'uppercase', "
            "'concat'). Surfaced verbatim; issue #40 owns the concrete allowed-set."
        ),
    )
    inputs: List[str] = Field(
        ...,
        min_length=1,
        description=(
            "One or more function inputs. Each entry must reference a kind='simple' "
            "leaf path in source.response_profile."
        ),
    )
    target_path: str = Field(
        ...,
        description="Logical leaf path inside target.payload_profile. Must reference a kind='simple' leaf.",
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
        # parameters is the only schema-opaque dict the archetype echoes back in
        # the emitted IntegrationSpec, so reject plaintext secret-shaped keys at
        # any nesting depth (mirrors database_to_api_sync). The offending key name
        # is not echoed — callers route secrets via the connector credential_ref.
        if value is None:
            return None
        if _scan_for_secret_shaped_keys(value):
            raise ValueError(
                "map_function.parameters contains a key whose name matches a "
                "forbidden secret-shaped substring (e.g. password / token / "
                "secret / api_key / bearer / authorization). Reference connector "
                "secrets via the connection binding's credential_ref instead; "
                "map_function.parameters is echoed back in the emitted "
                "IntegrationSpec and must not carry plaintext secrets."
            )
        return value


class MapScriptApiTransformOperation(_BaseApiTransformOperation):
    operation_type: Literal["map_script"] = Field(
        ...,
        description=(
            "Discriminator: 'map_script' routes to a Boomi map script step "
            "rendered as an in-map FunctionStep referencing a script.mapping "
            "component materialized from an inline script_body (#41)."
        ),
    )
    script_slot: str = Field(
        ...,
        description="Stable task-authored slot name identifying the script's role in the summary.",
    )
    language: Literal["groovy2", "groovy", "javascript"] = Field(
        ...,
        description="Script language (groovy2 recommended; groovy = legacy Groovy 1; javascript).",
    )
    inputs: List[str] = Field(
        ...,
        min_length=1,
        description=(
            "Source leaf paths consumed by the script (each a kind='simple' leaf "
            "in source.response_profile). The in-script variable for each path is "
            "its final segment sanitized to a language-safe identifier (e.g. "
            "'Root/order-id' -> 'order_id'); two paths that derive the same "
            "variable name (across inputs AND outputs) are rejected."
        ),
    )
    outputs: List[str] = Field(
        ...,
        min_length=1,
        description=(
            "Target leaf paths populated by the script (each a kind='simple' leaf "
            "in target.payload_profile). The in-script variable for each path is "
            "its final segment sanitized to a language-safe identifier; it must not "
            "collide with another input/output variable name."
        ),
    )
    script_component_ref: Optional[str] = Field(
        default=None,
        description=(
            "External script-component reuse is NOT supported by this preset (the "
            "referenced component is not part of the emitted spec). Provide "
            "script_body instead; #51 owns external script reuse."
        ),
    )
    script_body: Optional[str] = Field(
        default=None,
        description=(
            "Caller-authored script source materialized into an in-spec "
            "script.mapping component referenced by the transform.map."
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

    @model_validator(mode="after")
    def _require_script_material(self) -> "MapScriptApiTransformOperation":
        # Issue #127 A2: a map_script op with neither script_body nor
        # script_component_ref carries no script to materialize. Reject it at
        # the contract layer so callers get a clear origin instead of the
        # downstream FieldMapPrimitive ARCHETYPE_PARAM_INVALID. (This preset
        # only supports inline script_body; script_component_ref reuse is still
        # rejected later at assembly — #51 owns external reuse.)
        if self.script_component_ref is None and self.script_body is None:
            raise ValueError(
                "map_script requires script_body when script_component_ref is "
                "absent"
            )
        return self


ApiTransformOperation = Annotated[
    Union[
        DirectApiTransformOperation,
        MapFunctionApiTransformOperation,
        MapScriptApiTransformOperation,
    ],
    Field(discriminator="operation_type"),
]


class ApiTransformConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    operations: List[ApiTransformOperation] = Field(
        ...,
        min_length=1,
        description=(
            "Typed transform operations moving source response leaves into target "
            "payload leaves. Discriminated by operation_type: 'direct' (#26), "
            "'map_function' (#40), 'map_script' (#41). operation_type='xslt' is "
            "rejected with a pointer to issue #42."
        ),
    )

    @model_validator(mode="before")
    @classmethod
    def _reject_xslt_with_42_pointer(cls, data: Any) -> Any:
        # mode='before' runs before the discriminator picks a variant, so an
        # explicit 'xslt' value gets a friendly #42 pointer rather than the
        # generic union_tag_invalid error. The offending index is included; no
        # caller-supplied content is echoed.
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
                                f"operations[{idx}].operation_type='xslt' is not "
                                "supported; see issue #42 for the XSLT decision."
                            )
        return data
