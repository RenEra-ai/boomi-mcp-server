"""Issue #27: ``db_extract`` source primitive.

Materializes the database source-extraction component group consumed by a
``database_to_api_sync`` flow:

  1. a database ``connector-settings`` (created from caller config, or a
     reference-only reuse of an existing connection),
  2. a ``profile.db`` Select-statement read profile (caller-authored SQL +
     explicit output fields), and
  3. a database ``connector-action`` Get operation that reads through the
     profile and binds to the connection at process time.

The primitive emits JSON ``IntegrationComponentSpec`` objects only — every
byte of XML and all structured validation is delegated to the existing
``DatabaseConnectorBuilder`` / ``DatabaseReadProfileBuilder`` /
``DatabaseGetOperationBuilder``. It does not generate SQL, browse the
database, infer schema, or call any live Boomi API.
"""

from __future__ import annotations

from typing import Annotated, Any, Dict, List, Literal, Optional, Union

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from ...categories.components.builders.connector_builder import (
    DatabaseConnectorBuilder,
    DatabaseGetOperationBuilder,
)
from ...categories.components.builders.process_flow_builder import (
    DB_CONNECTION_EXTENSION_FIELDS_CREDENTIAL,
    DB_CONNECTION_EXTENSION_FIELDS_ENDPOINT,
)
from ...categories.components.builders.profile_builder import (
    DatabaseReadProfileBuilder,
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
    ROLE_DB_GET_OPERATION,
    ROLE_DB_READ_PROFILE,
    primitive_component_key,
    raise_for_builder_error,
)

# DB read profile / parameter field types accepted by the profile builder.
_DB_FIELD_TYPE = Literal["character", "number", "datetime"]


def db_connection_extension_fields(
    *, credentials: bool = True, endpoint: bool = False
) -> List[Dict[str, str]]:
    """Ordered DB source-connection field declarations for environment extensions.

    Issue #92 M4.5.7. Returns endpoint fields (host, port) first, then credential
    fields (username, password) — matching the live-verified exemplar order — so a
    ``database_to_api_sync`` archetype can declare which connection fields become
    per-environment override points. Each field is a fresh ``{id, label, xpath}``
    dict (copied from the builder-owned constants so callers cannot mutate them).
    Returns ``[]`` when both flags are off.

    The xpath / id contract is owned by ``process_flow_builder`` (which emits the
    declaration XML); this primitive only selects which fields to declare.
    """
    fields: List[Dict[str, str]] = []
    if endpoint:
        fields.extend(dict(field) for field in DB_CONNECTION_EXTENSION_FIELDS_ENDPOINT)
    if credentials:
        fields.extend(dict(field) for field in DB_CONNECTION_EXTENSION_FIELDS_CREDENTIAL)
    return fields


# ---------------------------------------------------------------------------
# Parameter models (strict — extra keys are rejected at the param boundary,
# which also blocks secret-shaped keys before the builder secret scan runs).
# ---------------------------------------------------------------------------


class DbConnectionCreate(BaseModel):
    """Create a new database connector-settings from caller config.

    Driver / shape / auth validation is delegated to
    ``DatabaseConnectorBuilder.validate_config`` — this model only fixes the
    accepted key surface so unknown or secret-shaped keys are rejected early.
    """

    model_config = ConfigDict(extra="forbid")

    mode: Literal["create"]
    driver_id: str = Field(..., description="Database driver id (e.g. sqlserver, mysql, oracle, custom)")
    auth_mode: str = Field(default="username_password", description="Connector auth mode")
    username: Optional[str] = Field(default=None)
    credential_ref: Optional[str] = Field(
        default=None,
        description="Opaque credential reference (e.g. credential://...); never written to XML",
    )
    host: Optional[str] = Field(default=None)
    port: Optional[int] = Field(default=None)
    dbname: Optional[str] = Field(default=None)
    additional: Optional[str] = Field(default=None, description="JDBC URL suffix appended verbatim")
    custom_class_name: Optional[str] = Field(default=None)
    connection_url: Optional[str] = Field(default=None)
    pooling: Optional[Dict[str, Any]] = Field(default=None)
    write_options: Optional[Dict[str, Any]] = Field(default=None)


class DbConnectionReuse(BaseModel):
    """Reference an existing database connection without mutating it.

    Resolution-by-``component_name`` trusts the in-spec ``connector_type``
    marker and matches by component metadata type + name; it does not fetch
    the live connector to verify the resolved component is actually a database
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
        # the exactly-one check below and become a fake component id (a "  "
        # id would otherwise survive as a truthy top-level component_id and be
        # planned as reuse). Real values are stripped.
        if isinstance(value, str):
            stripped = value.strip()
            return stripped or None
        return value

    @model_validator(mode="after")
    def _require_exactly_one_binding(self) -> "DbConnectionReuse":
        if bool(self.component_id) == bool(self.component_name):
            raise ValueError(
                "reuse connection requires exactly one non-blank component_id "
                "or component_name"
            )
        return self


DbConnection = Annotated[
    Union[DbConnectionCreate, DbConnectionReuse],
    Field(discriminator="mode"),
]


class DbOutputField(BaseModel):
    """One column of the Select result set (declared, not discovered)."""

    model_config = ConfigDict(extra="forbid")

    name: str
    data_type: _DB_FIELD_TYPE = "character"
    mandatory: bool = False
    enforce_unique: bool = False


class DbReadParameter(BaseModel):
    """One optional bind parameter for the Select statement."""

    model_config = ConfigDict(extra="forbid")

    name: str
    data_type: _DB_FIELD_TYPE = "character"
    mappable: bool = False


class DbReadProfileParams(BaseModel):
    model_config = ConfigDict(extra="forbid")

    query: str = Field(..., description="Caller-authored SQL Select statement, stored verbatim")
    output_fields: List[DbOutputField] = Field(
        ..., min_length=1, description="Explicit result-set columns"
    )
    parameters: Optional[List[DbReadParameter]] = Field(default=None)


class DbGetOperationParams(BaseModel):
    model_config = ConfigDict(extra="forbid")

    batch_count: Optional[int] = Field(default=None, ge=0)
    max_rows: Optional[int] = Field(default=None, ge=0)


class DbExtractComponentNames(BaseModel):
    """Optional display-name overrides per emitted component role."""

    model_config = ConfigDict(extra="forbid")

    connection: Optional[str] = None
    read_profile: Optional[str] = None
    get_operation: Optional[str] = None


class DbExtractParameters(BaseModel):
    model_config = ConfigDict(extra="forbid")

    key_prefix: str = Field(
        ..., description="Stable key prefix for deterministic component keys"
    )
    connection: DbConnection
    read_profile: DbReadProfileParams
    operation: DbGetOperationParams = Field(default_factory=DbGetOperationParams)
    component_names: DbExtractComponentNames = Field(
        default_factory=DbExtractComponentNames
    )


# ---------------------------------------------------------------------------
# Primitive
# ---------------------------------------------------------------------------


class DbExtractPrimitive(PrimitivePattern):
    """Emit the database source-extraction component group."""

    metadata = PatternMetadata(
        name="db_extract",
        version="1.0.0",
        kind=PatternKind.PRIMITIVE,
        description=(
            "Materialize the database source extraction group (connection, "
            "read profile, Get operation) from caller-declared SQL and "
            "result fields. Emits JSON component specs for the existing "
            "database builders; never generates SQL or browses the database."
        ),
        tags=["source", "database", "extract"],
        use_cases=[
            "Extract rows from a relational source via a caller-authored Select",
            "Reuse an existing database connection as an extraction source",
        ],
        not_for=[
            "REST/API sources",
            "Database write or send operations",
            "Schema discovery, SQL generation, or row sampling",
        ],
    )
    parameters_model = DbExtractParameters

    output_contract = PatternIOContract(
        name="database_extract_result",
        description="Result set emitted by the database Get operation.",
        profile_type="database",
        media_type="application/xml",
        schema_={
            "type": "object",
            "properties": {
                "source_profile_key": {"type": "string"},
                "source_field_index": {"type": "object"},
                "db_connection_key": {"type": "string"},
                "db_operation_key": {"type": "string"},
            },
            "required": [
                "source_profile_key",
                "source_field_index",
                "db_connection_key",
                "db_operation_key",
            ],
        },
    )
    required_builders = [
        "DatabaseConnectorBuilder",
        "DatabaseReadProfileBuilder",
        "DatabaseGetOperationBuilder",
    ]

    @classmethod
    def emit_components(
        cls,
        context: PrimitiveBuildContext,
        parameters: BaseModel,
    ) -> List[IntegrationComponentSpec]:
        params: DbExtractParameters = parameters  # type: ignore[assignment]

        conn_key = primitive_component_key(params.key_prefix, ROLE_DB_CONNECTION)
        read_key = primitive_component_key(params.key_prefix, ROLE_DB_READ_PROFILE)
        op_key = primitive_component_key(params.key_prefix, ROLE_DB_GET_OPERATION)
        folder = context.folder_path

        connection = cls._emit_connection(context, params, conn_key, folder)
        read_profile = cls._emit_read_profile(context, params, read_key, folder)
        get_operation = cls._emit_get_operation(
            context, params, op_key, conn_key, read_key, folder
        )

        # Deterministic dependency order: connection, read profile, get op.
        return [connection, read_profile, get_operation]

    # ------------------------------------------------------------------
    # Per-role emission
    # ------------------------------------------------------------------

    @classmethod
    def _emit_connection(
        cls,
        context: PrimitiveBuildContext,
        params: DbExtractParameters,
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
    def _emit_read_profile(
        cls,
        context: PrimitiveBuildContext,
        params: DbExtractParameters,
        read_key: str,
        folder: Optional[str],
    ) -> IntegrationComponentSpec:
        read_name = (
            params.component_names.read_profile
            or f"{context.component_prefix} DB Read Profile"
        )
        config: Dict[str, Any] = {
            "profile_type": "database.read",
            "component_name": read_name,
            "query": params.read_profile.query,
            "output_fields": [
                field.model_dump() for field in params.read_profile.output_fields
            ],
        }
        if params.read_profile.parameters:
            config["parameters"] = [
                parameter.model_dump()
                for parameter in params.read_profile.parameters
            ]
        if folder:
            config["folder_name"] = folder
        raise_for_builder_error(DatabaseReadProfileBuilder.validate_config(config))
        return IntegrationComponentSpec(
            key=read_key,
            type="profile.db",
            action="create",
            name=read_name,
            config=config,
        )

    @classmethod
    def _emit_get_operation(
        cls,
        context: PrimitiveBuildContext,
        params: DbExtractParameters,
        op_key: str,
        conn_key: str,
        read_key: str,
        folder: Optional[str],
    ) -> IntegrationComponentSpec:
        op_name = (
            params.component_names.get_operation
            or f"{context.component_prefix} DB Get"
        )
        config: Dict[str, Any] = {
            "connector_type": "database",
            "operation_mode": "get",
            "component_name": op_name,
            "read_profile_id": f"$ref:{read_key}",
            "connection_ref_key": conn_key,
        }
        if params.operation.batch_count is not None:
            config["batch_count"] = params.operation.batch_count
        if params.operation.max_rows is not None:
            config["max_rows"] = params.operation.max_rows
        if folder:
            config["folder_name"] = folder
        raise_for_builder_error(DatabaseGetOperationBuilder.validate_config(config))
        return IntegrationComponentSpec(
            key=op_key,
            type="connector-action",
            action="create",
            name=op_name,
            config=config,
            depends_on=[conn_key, read_key],
        )
