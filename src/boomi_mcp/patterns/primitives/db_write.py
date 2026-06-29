"""Issue #74: ``db_write`` target primitive.

Materializes the database target-write component group consumed by an
``api_to_database_sync`` flow (the write counterpart of #27's ``db_extract``):

  1. a database ``connector-settings`` (created from caller config, or a
     reference-only reuse of an existing connection),
  2. a ``profile.db`` Write profile (``profile_type="database.write"`` — caller
     authored statement type + columns/conditions, no generated SQL), and
  3. a database ``connector-action`` Send operation that writes through the
     write profile and binds to the connection at process time.

The primitive emits JSON ``IntegrationComponentSpec`` objects only — every byte
of XML and all structured validation is delegated to the already-shipped #32
``DatabaseConnectorBuilder`` / ``DatabaseWriteProfileBuilder`` /
``DatabaseSendOperationBuilder``. It does not generate SQL, browse the database,
infer schema, or call any live Boomi API. Unsupported write-profile variants
(any ``statement_type`` outside #32's confirmed set — e.g. ``upsert``) stay
blocked because validation is delegated verbatim to the write-profile builder
(``UNSUPPORTED_DB_STATEMENT_TYPE``).
"""

from __future__ import annotations

from typing import Any, Dict, List, Literal, Optional

from pydantic import BaseModel, ConfigDict, Field

from ...categories.components.builders.connector_builder import (
    DatabaseConnectorBuilder,
    DatabaseSendOperationBuilder,
)
from ...categories.components.builders.profile_builder import (
    DatabaseWriteProfileBuilder,
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
    ROLE_DB_CONNECTION,
    ROLE_DB_WRITE_OPERATION,
    ROLE_DB_WRITE_PROFILE,
    primitive_component_key,
    raise_for_builder_error,
)

# Reuse the #27 db_extract connection contract verbatim (create/reuse discriminated
# union) so the source-extract and target-write groups share one connection shape.
from .db_extract import DbConnection

# DB write profile / parameter field types accepted by the profile builder
# (DatabaseWriteProfileBuilder._SUPPORTED_FIELD_TYPES).
_DB_FIELD_TYPE = Literal["character", "number", "datetime"]


# ---------------------------------------------------------------------------
# Parameter models (strict — extra keys are rejected at the param boundary,
# which also blocks secret-shaped keys before the builder secret scan runs).
# Per-statement-type shape rules (which keys are required/forbidden for each
# statement_type, and the supported statement_type set) are NOT re-implemented
# here — they are delegated verbatim to DatabaseWriteProfileBuilder.validate_config
# so its structured errors (UNSUPPORTED_DB_STATEMENT_TYPE / MISSING_DB_* / ...)
# surface unchanged.
# ---------------------------------------------------------------------------


class DbWriteField(BaseModel):
    """One DBFields/DatabaseElement column written by the statement."""

    model_config = ConfigDict(extra="forbid")

    name: str
    data_type: _DB_FIELD_TYPE = "character"
    mandatory: bool = False
    enforce_unique: bool = False


class DbWriteCondition(BaseModel):
    """One DBConditions/DBCondition WHERE key (dynamic update/delete)."""

    model_config = ConfigDict(extra="forbid")

    name: str
    data_type: _DB_FIELD_TYPE = "character"


class DbWriteProfileParams(BaseModel):
    """Database Write profile parameters.

    ``statement_type`` is a plain string (not a Literal) so an unsupported
    variant surfaces the builder's ``UNSUPPORTED_DB_STATEMENT_TYPE`` error rather
    than a generic pydantic enum error — the "unconfirmed write-profile variants
    remain blocked" acceptance criterion is realized by the #32 builder, not a
    re-implemented allow-list here.
    """

    model_config = ConfigDict(extra="forbid")

    statement_type: str = Field(
        ...,
        description=(
            "Write statement variant: standardinsertupdatedelete, dynamicinsert, "
            "dynamicupdate, dynamicdelete, or storedprocedurewrite. Any other "
            "value (e.g. 'upsert') is rejected by the write-profile builder."
        ),
    )
    sql: Optional[str] = Field(
        default=None,
        description="Caller-authored write SQL (required for standard / stored-procedure; omitted for dynamic*).",
    )
    table_name: Optional[str] = Field(
        default=None, description="Target table for the dynamic* statement types."
    )
    stored_procedure: Optional[str] = Field(
        default=None, description="Procedure name for storedprocedurewrite."
    )
    fields: List[DbWriteField] = Field(
        default_factory=list,
        description="Written columns (required for every type except dynamicdelete).",
    )
    conditions: List[DbWriteCondition] = Field(
        default_factory=list,
        description="WHERE-key columns (required for dynamicupdate/dynamicdelete; rejected otherwise).",
    )


class DbSendOperationParams(BaseModel):
    """Optional database Send operation tuning."""

    model_config = ConfigDict(extra="forbid")

    commit_option: Optional[str] = Field(
        default=None, description="'commitprofile' (default) or 'commitrows'."
    )
    batch_count: Optional[int] = Field(default=None, ge=0)
    enable_batching: Optional[bool] = Field(default=None)


class DbWriteComponentNames(BaseModel):
    """Optional display-name overrides per emitted component role."""

    model_config = ConfigDict(extra="forbid")

    connection: Optional[str] = None
    write_profile: Optional[str] = None
    send_operation: Optional[str] = None


class DbWriteParameters(BaseModel):
    model_config = ConfigDict(extra="forbid")

    key_prefix: str = Field(
        ..., description="Stable key prefix for deterministic component keys"
    )
    connection: DbConnection
    write_profile: DbWriteProfileParams
    operation: DbSendOperationParams = Field(default_factory=DbSendOperationParams)
    component_names: DbWriteComponentNames = Field(
        default_factory=DbWriteComponentNames
    )


def _write_profile_config(
    write_profile: DbWriteProfileParams,
    *,
    component_name: str,
    folder: Optional[str] = None,
) -> Dict[str, Any]:
    """Build the profile.db Write config dict from caller params.

    Shared by :meth:`DbWritePrimitive.emit_components` (which validates it) and
    :meth:`DbWritePrimitive.build_field_index` (which only reads statement_type /
    fields / conditions). ``fields``/``conditions`` are included only when
    non-empty so a stray empty list never trips the builder's per-statement-type
    presence rules (an absent key lets the builder raise the precise MISSING_DB_*
    error for the statement types that require it).
    """
    config: Dict[str, Any] = {
        "profile_type": "database.write",
        "component_name": component_name,
        "statement_type": write_profile.statement_type,
    }
    for attr in ("sql", "table_name", "stored_procedure"):
        value = getattr(write_profile, attr)
        if value is not None:
            config[attr] = value
    if write_profile.fields:
        config["fields"] = [field.model_dump() for field in write_profile.fields]
    if write_profile.conditions:
        config["conditions"] = [
            condition.model_dump() for condition in write_profile.conditions
        ]
    if folder:
        config["folder_name"] = folder
    return config


# ---------------------------------------------------------------------------
# Primitive
# ---------------------------------------------------------------------------


class DbWritePrimitive(PrimitivePattern):
    """Emit the database target-write component group."""

    metadata = PatternMetadata(
        name="db_write",
        version="1.0.0",
        kind=PatternKind.PRIMITIVE,
        description=(
            "Materialize the database target write group (connection, write "
            "profile, Send operation) from a caller-declared statement type and "
            "columns/conditions. Emits JSON component specs for the existing #32 "
            "database builders; never generates SQL or browses the database."
        ),
        tags=["target", "database", "write"],
        use_cases=[
            "Write rows to a relational target via a caller-authored write profile",
            "Reuse an existing database connection as a write target",
        ],
        not_for=[
            "REST/API targets",
            "Database read or Get operations (use db_extract)",
            "Schema discovery, SQL generation, or upsert / unconfirmed write variants",
        ],
    )
    parameters_model = DbWriteParameters

    output_contract = PatternIOContract(
        name="database_write_target",
        description="Database Write profile + Send operation consumed as a map target.",
        profile_type="database",
        media_type="application/xml",
        schema_={
            "type": "object",
            "properties": {
                "target_profile_key": {"type": "string"},
                "target_field_index": {"type": "object"},
                "db_connection_key": {"type": "string"},
                "db_operation_key": {"type": "string"},
            },
            "required": [
                "target_profile_key",
                "target_field_index",
                "db_connection_key",
                "db_operation_key",
            ],
        },
    )
    required_builders = [
        "DatabaseConnectorBuilder",
        "DatabaseWriteProfileBuilder",
        "DatabaseSendOperationBuilder",
    ]

    @classmethod
    def build_field_index(
        cls, write_profile: DbWriteProfileParams
    ) -> Dict[str, Dict[str, Any]]:
        """Return the write profile's namespace-prefixed map-target field index.

        Keyed by ``Fields/<col>`` / ``Conditions/<col>`` (see
        ``DatabaseWriteProfileBuilder.build_field_index``). Used by the
        api_to_database_sync archetype to validate transform target paths against
        the real write-profile shape. The component_name is irrelevant to the
        index, so a placeholder is passed.
        """
        config = _write_profile_config(write_profile, component_name="_")
        return DatabaseWriteProfileBuilder.build_field_index(config)

    @classmethod
    def emit_components(
        cls,
        context: PrimitiveBuildContext,
        parameters: BaseModel,
    ) -> List[IntegrationComponentSpec]:
        params: DbWriteParameters = parameters  # type: ignore[assignment]

        conn_key = primitive_component_key(params.key_prefix, ROLE_DB_CONNECTION)
        write_key = primitive_component_key(params.key_prefix, ROLE_DB_WRITE_PROFILE)
        op_key = primitive_component_key(params.key_prefix, ROLE_DB_WRITE_OPERATION)
        folder = context.folder_path

        connection = cls._emit_connection(context, params, conn_key, folder)
        write_profile = cls._emit_write_profile(context, params, write_key, folder)
        send_operation = cls._emit_send_operation(
            context, params, op_key, conn_key, write_key, folder
        )

        # Deterministic dependency order: connection, write profile, send op.
        return [connection, write_profile, send_operation]

    # ------------------------------------------------------------------
    # Per-role emission
    # ------------------------------------------------------------------

    @classmethod
    def _emit_connection(
        cls,
        context: PrimitiveBuildContext,
        params: DbWriteParameters,
        conn_key: str,
        folder: Optional[str],
    ) -> IntegrationComponentSpec:
        connection = params.connection

        if connection.mode == "create":
            conn_name = (
                params.component_names.connection
                or f"{context.component_prefix} DB Connection"
            )
            config: Dict[str, Any] = {
                "connector_type": "database",
                "component_name": conn_name,
                "driver_id": connection.driver_id,
                "auth_mode": connection.auth_mode,
            }
            for field in (
                "username",
                "credential_ref",
                "host",
                "port",
                "dbname",
                "additional",
                "custom_class_name",
                "connection_url",
                "pooling",
                "write_options",
            ):
                value = getattr(connection, field)
                if value is not None:
                    config[field] = value
            if folder:
                config["folder_name"] = folder
            raise_for_builder_error(DatabaseConnectorBuilder.validate_config(config))
            return IntegrationComponentSpec(
                key=conn_key,
                type="connector-settings",
                action="create",
                name=conn_name,
                config=config,
            )

        # reuse — reference an existing connection without mutating it.
        config = {"reference_only": True, "connector_type": "database"}
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
    def _emit_write_profile(
        cls,
        context: PrimitiveBuildContext,
        params: DbWriteParameters,
        write_key: str,
        folder: Optional[str],
    ) -> IntegrationComponentSpec:
        write_name = (
            params.component_names.write_profile
            or f"{context.component_prefix} DB Write Profile"
        )
        config = _write_profile_config(
            params.write_profile, component_name=write_name, folder=folder
        )
        raise_for_builder_error(DatabaseWriteProfileBuilder.validate_config(config))
        return IntegrationComponentSpec(
            key=write_key,
            type="profile.db",
            action="create",
            name=write_name,
            config=config,
        )

    @classmethod
    def _emit_send_operation(
        cls,
        context: PrimitiveBuildContext,
        params: DbWriteParameters,
        op_key: str,
        conn_key: str,
        write_key: str,
        folder: Optional[str],
    ) -> IntegrationComponentSpec:
        op_name = (
            params.component_names.send_operation
            or f"{context.component_prefix} DB Send"
        )
        config: Dict[str, Any] = {
            "connector_type": "database",
            "operation_mode": "send",
            "component_name": op_name,
            "write_profile_id": f"$ref:{write_key}",
            "connection_ref_key": conn_key,
        }
        if params.operation.commit_option is not None:
            config["commit_option"] = params.operation.commit_option
        if params.operation.batch_count is not None:
            config["batch_count"] = params.operation.batch_count
        if params.operation.enable_batching is not None:
            config["enable_batching"] = params.operation.enable_batching
        if folder:
            config["folder_name"] = folder
        raise_for_builder_error(DatabaseSendOperationBuilder.validate_config(config))
        return IntegrationComponentSpec(
            key=op_key,
            type="connector-action",
            action="create",
            name=op_name,
            config=config,
            depends_on=[conn_key, write_key],
        )
