"""The typed DB -> transform -> REST sync preset (issue #159 S2, lane L1).

``boomi.archetype.database_to_api_sync`` 1.0.0 replaces the legacy
``database_to_api_sync`` archetype (``patterns/archetypes/database_to_api_sync.py``,
read-only oracle, never imported here). It is built exactly like fan-out 2.0.0
(``fanout.py``): a closed, secret-free recipe input the PURE executor reads; a
closed component material only the TRUSTED preparer reads; the shared
component-material boundary (``recipes.component_material``) for every
credential, URL, query, selected-base and assembly rule.

The executor emits ONE canonical process unit:

* without a DLQ: ``source -> map_ref -> target -> stop`` (the legacy archetype
  emits no Try/Catch when the DLQ is disabled);
* with a DLQ: golden-000086's connector-scoped double guard — a Try/Catch around
  the DB Get that continues, the map between the handlers, and a Try/Catch
  around the REST send — each catch an optional Notify plus Add to Cache into
  the dead-letter Document Cache, retry count ZERO in both guards.

**Retry (architect decision, #159 S2).** A retried REST POST is not admissible
on the public path today: no POST operation record exists to grant the retry,
and a created operation can never be granted one. ``max_attempts`` is parsed in
``1..6`` (the legacy range) so that ``2..6`` is REFUSED by name — with cause
``DB_TO_API_RETRY_UNAVAILABLE``, before anything is assembled — rather than
silently reduced. ``1`` is the only supported value. No caller-authored
idempotency field exists.

**Dead-letter cache.** An explicit reference-only binding to an EXISTING
Document Cache, selected by base-component key and checked by the shared
selected-base rule. The one cache receives DB-profile documents (the read's
catch) and JSON-profile documents (the send's catch), so only an unprofiled
cache (``profileType="profile.none"``, no profile bound) admits both. The
preparer never reads the account: the workflow reads the cache's stored XML
and the compiler's profile rule refuses a profiled or unreadable cache.

Local error scopes and staged outcomes are out of scope (issue #185).
"""

from __future__ import annotations

from dataclasses import dataclass as _dataclass
from types import MappingProxyType as _MappingProxyType
from typing import Any as _Any
from typing import Dict as _Dict
from typing import List as _List
from typing import Literal as _Literal
from typing import Mapping as _Mapping
from typing import Optional, Tuple

from pydantic import Field, StrictInt, field_validator, model_validator
from pydantic import ValidationError as _ValidationError
from pydantic_core import PydanticCustomError

from ...errors import (
    COMPOSITION_COMPONENT_KEY_COLLISION,
    COMPOSITION_CONTRACT_MISMATCH,
    DB_TO_API_RETRY_UNAVAILABLE,
    GOVERNANCE_WATERMARK_INCONSISTENT,
)
from ...models.derived_flows import (
    LinearConnectorFactV1,
    LinearReliabilityFactV1,
    LinearRouteProjectionFactsV1,
    LinearTransformFactV1,
    RecipeOperationHintV1,
)
from ...models.governance_intent import WatermarkDeclarationV1
from ...models.integration_models import IntegrationComponentSpec
from ...models.process_component import (
    ProcessAuthoringUnitV1,
    ProcessComponentEnvelopeV1,
    ProcessConnectionOverrideV1,
    ProcessExtensionBindingsV1,
    ProcessOverrideFieldV1,
)
from ...models.process_ir import NotifyNodeV1, ProcessIRV1
from ...models.process_ir_tokens import CAUGHT_ERROR_PROPERTY_ID, NOTIFY_LEVELS
from ...models.recipe_contributions import (
    ConstraintRequirementV1,
    ProcessUnitContributionV1,
    RecipeComponentKey,
    RecordedIntentContributionV1,
    RequireCapabilityV1,
    RequireComponentV1,
    RequireProcessV1,
)
from ...patterns.archetype_parameters import (
    JSONPayloadProfile,
    _flatten_payload_profile_leaves,
)
from ...patterns.base import PrimitiveBuildContext
from ..component_material import (
    PreparedRecipeComponentsV1,
    _DEFAULT_STATUS_CODES_V2,
    _DbAuthModeV2,
    _FanoutMaterialModel,
    _RestMethodV2,
    _check_base_component,
    _refusal,
    _rt,
    FanoutDbConnectionMaterialV2,
    FanoutDbReadMaterialV2,
    FanoutDirectOperationV2,
    FanoutFunctionOperationV2,
    FanoutRestConnectionMaterialV2,
    FanoutRestCreateMaterialV2,
    FanoutRestOperationMaterialV2,
    FanoutScriptReferenceOperationV2,
    FanoutTransformOperationV2,
    authorize_db_connection,
    authorize_db_read,
    authorize_rest_connection,
    authorize_rest_request,
    authorize_transform_operations,
    check_connection_binding,
    check_credential_boundary,
    check_rest_request,
    emit_db_read_components,
    emit_rest_send_components,
    emit_transform_components,
    material_field_classes,
    register_material_root,
)
from ..contracts import RecipeInputBase

RECIPE_ID = "boomi.archetype.database_to_api_sync"
RECIPE_VERSION = "1.0.0"
_VERSION = "1"

#: The legacy archetype's fixed spellings (``_build_main_process`` and its flow
#: rows). Pinned against the legacy route by the L1 tests.
_PROCESS_NAME_SUFFIX = "DB to API Sync"
_TRANSFORM_ROW_NAME = "Map source to JSON payload"
#: The catch terminal's label: the legacy builder's DLQ cache shape label
#: (golden-000088, ``issue184_cache_notify_archetype_terminal.xml``). The legacy
#: process labels no connector and no map step, so neither does this root.
_DLQ_CACHE_LABEL = "Route caught errors to DLQ cache"

#: The only supported attempt count. ``2..6`` parse so they can be refused by name.
_SUPPORTED_MAX_ATTEMPTS = 1
_RETRY_POINTER = "/raw_input/reliability/retry/max_attempts"


# ---------------------------------------------------------------------------
# Recipe input — closed and secret-free (what the PURE executor receives)
# ---------------------------------------------------------------------------


class DbToApiNamingV1(RecipeInputBase):
    component_prefix: str = Field(min_length=1)
    folder_name: Optional[str] = Field(default=None, min_length=1)
    process_name: Optional[str] = Field(default=None, min_length=1)


class DbToApiSourceV1(RecipeInputBase):
    """The database source: binding classification only."""

    binding_mode: _Literal["create", "reuse"]
    #: The create-mode connection's auth mode; refused for reuse. It decides
    #: whether the source connection's credential fields become override points.
    auth_mode: Optional[_DbAuthModeV2] = None  # type: ignore[valid-type]
    #: An EXISTING base component this source binds to, by key. When set, the
    #: component material carries no source connection.
    connection_key: Optional[RecipeComponentKey] = None

    @model_validator(mode="after")
    def _auth_mode_matches_binding(self) -> "DbToApiSourceV1":
        if (self.binding_mode == "create") != (self.auth_mode is not None):
            raise PydanticCustomError(
                "db_to_api_source_auth_mode",
                "auth_mode is required for a create-mode source and refused otherwise",
            )
        return self


class DbToApiTransformV1(RecipeInputBase):
    #: The name of the served transform flow row. The map step itself is
    #: unlabelled, as the legacy process emitted it.
    map_label: Optional[str] = Field(default=None, min_length=1)


class DbToApiTargetV1(RecipeInputBase):
    binding_mode: _Literal["create", "reuse"]
    connection_key: Optional[RecipeComponentKey] = None
    method: _RestMethodV2 = "POST"  # type: ignore[valid-type]


class DbToApiWatermarkV1(RecipeInputBase):
    """The typed watermark intent. Recorded, never wired."""

    field: str = Field(min_length=1)
    kind: _Literal["timestamp", "sequence"]
    initial_value: Optional[str] = None
    query_parameter_refs: Tuple[str, ...] = ()


class DbToApiRetryV1(RecipeInputBase):
    max_attempts: StrictInt = Field(
        default=_SUPPORTED_MAX_ATTEMPTS,
        ge=1,
        le=6,
        description=(
            "Total attempts per document. Only 1 is supported: a retried REST "
            "send needs an attested operation grant the public path does not "
            "have yet, so 2..6 are refused with DB_TO_API_RETRY_UNAVAILABLE."
        ),
    )


class DbToApiDlqV1(RecipeInputBase):
    #: The base component key of an EXISTING, reference-only, unprofiled
    #: Document Cache (``profileType="profile.none"``).
    cache_key: RecipeComponentKey


class DbToApiCatchNotifyV1(RecipeInputBase):
    """A log-only Notify at the head of each catch leg (requires a DLQ)."""

    # The served enumeration is derived from the SAME authority the validator
    # below reaches through ``NotifyNodeV1`` (``NOTIFY_LEVELS``); the field stays a
    # string so a case or whitespace variant is still normalized, not refused.
    level: str = Field(
        description=(
            "Platform log level for the Notify at the head of each catch leg. "
            "Case-insensitive; one of " + ", ".join(NOTIFY_LEVELS) + "."
        ),
        json_schema_extra={"enum": list(NOTIFY_LEVELS)},
    )
    message_template: str = Field(
        min_length=1,
        description=(
            "Plain-text log message. It must contain the literal text "
            + CAUGHT_ERROR_PROPERTY_ID
            + " (the caught-error property, bound once per occurrence), must not "
            "contain a brace, and must not read as a JSON object or array; "
            "otherwise the input is refused with db_to_api_catch_notify_invalid."
        ),
    )

    @field_validator("level")
    @classmethod
    def _normalize_level(cls, value: str) -> str:
        return value.strip().upper()

    @model_validator(mode="after")
    def _notify_authority(self) -> "DbToApiCatchNotifyV1":
        # The ProcessIR Notify node is the authority for the level vocabulary
        # and the caught-error token rule. Nothing submitted is echoed.
        try:
            NotifyNodeV1(kind="notify", level=self.level, message_template=self.message_template)
        except _ValidationError:
            raise PydanticCustomError(
                "db_to_api_catch_notify_invalid",
                "catch_notify must name a supported level and reference the caught-error property",
            ) from None
        return self


class DbToApiReliabilityV1(RecipeInputBase):
    retry: DbToApiRetryV1 = Field(default_factory=DbToApiRetryV1)
    dlq: Optional[DbToApiDlqV1] = None
    catch_notify: Optional[DbToApiCatchNotifyV1] = None

    @model_validator(mode="after")
    def _notify_requires_dlq(self) -> "DbToApiReliabilityV1":
        if self.catch_notify is not None and self.dlq is None:
            raise PydanticCustomError(
                "db_to_api_catch_notify_requires_dlq",
                "catch_notify rides the dead-letter catch leg and requires dlq",
            )
        return self


class DbToApiEnvironmentExtensionsV1(RecipeInputBase):
    """Which connection fields the process declares as per-environment override
    points — the legacy ``EnvironmentExtensionsConfig`` switches and defaults."""

    credential_connection_fields: bool = True
    endpoint_connection_fields: bool = False
    rest_credential_connection_fields: bool = True
    rest_endpoint_connection_fields: bool = False


class DatabaseToApiSyncInputV1(RecipeInputBase):
    """Topology, identities and binding classifications. Carries NO connection
    material: hosts, credentials, SQL, URLs, paths and mappings travel in
    :class:`DatabaseToApiSyncMaterialV1`, which only the trusted preparer reads."""

    version: _Literal["1"]
    process_key: RecipeComponentKey
    naming: DbToApiNamingV1
    source: DbToApiSourceV1
    transform: DbToApiTransformV1 = Field(default_factory=DbToApiTransformV1)
    target: DbToApiTargetV1
    reliability: DbToApiReliabilityV1 = Field(default_factory=DbToApiReliabilityV1)
    watermark: Optional[DbToApiWatermarkV1] = None
    environment_extensions: DbToApiEnvironmentExtensionsV1 = Field(
        default_factory=DbToApiEnvironmentExtensionsV1
    )


# ---------------------------------------------------------------------------
# Component material — closed (what only the TRUSTED preparer receives)
# ---------------------------------------------------------------------------


class DbToApiSourceMaterialV1(_FanoutMaterialModel):
    #: Absent exactly when the recipe input names a base ``connection_key``.
    connection: Optional[FanoutDbConnectionMaterialV2] = None
    read: FanoutDbReadMaterialV2


class DbToApiTransformMaterialV1(_FanoutMaterialModel):
    operations: Tuple[FanoutTransformOperationV2, ...] = Field(min_length=1)


class DbToApiTargetMaterialV1(_FanoutMaterialModel):
    #: Absent exactly when the recipe input names a base ``connection_key``.
    connection: Optional[FanoutRestConnectionMaterialV2] = None
    operation: FanoutRestOperationMaterialV2


class DatabaseToApiSyncMaterialV1(_FanoutMaterialModel):
    version: _Literal["1"]
    source: DbToApiSourceMaterialV1
    transform: DbToApiTransformMaterialV1
    payload_profile: JSONPayloadProfile
    target: DbToApiTargetMaterialV1


def _root_field_classes() -> _Mapping[Tuple[type, str], Optional[str]]:
    """This preset's ROOT-field classifications. Every shared block they reach
    is classified by the shared table."""
    return _MappingProxyType(
        {
            (DatabaseToApiSyncMaterialV1, "version"): None,  # closed literal
            (DatabaseToApiSyncMaterialV1, "source"): None,  # structural (model)
            (DatabaseToApiSyncMaterialV1, "transform"): None,  # structural (model)
            (DatabaseToApiSyncMaterialV1, "payload_profile"): None,  # structural (model)
            (DatabaseToApiSyncMaterialV1, "target"): None,  # structural (model)
            (DbToApiSourceMaterialV1, "connection"): None,  # structural (variant)
            (DbToApiSourceMaterialV1, "read"): None,  # structural (model)
            (DbToApiTransformMaterialV1, "operations"): None,  # structural (variants)
            (DbToApiTargetMaterialV1, "connection"): None,  # structural (variant)
            (DbToApiTargetMaterialV1, "operation"): None,  # structural (model)
        }
    )


register_material_root(DatabaseToApiSyncMaterialV1, _root_field_classes)


# ---------------------------------------------------------------------------
# Shared key derivation (preparer AND executor)
# ---------------------------------------------------------------------------


@_dataclass(frozen=True)
class _Keys:
    db_connection: str
    db_read_profile: str
    db_get_operation: str
    target_profile: str
    transform_map: str
    rest_connection: str
    rest_operation: str
    dlq_cache: Optional[str]


def _keys(inp: DatabaseToApiSyncInputV1) -> _Keys:
    rt = _rt()
    key = rt.primitive_component_key
    return _Keys(
        db_connection=inp.source.connection_key or key(rt.SOURCE_PREFIX, rt.ROLE_DB_CONNECTION),
        db_read_profile=key(rt.SOURCE_PREFIX, rt.ROLE_DB_READ_PROFILE),
        db_get_operation=key(rt.SOURCE_PREFIX, rt.ROLE_DB_GET_OPERATION),
        target_profile=key(rt.TRANSFORM_PREFIX, rt.ROLE_TARGET_PROFILE),
        transform_map=key(rt.TRANSFORM_PREFIX, rt.ROLE_TRANSFORM_MAP),
        rest_connection=inp.target.connection_key or key(rt.TARGET_PREFIX, rt.ROLE_REST_CONNECTION),
        rest_operation=key(rt.TARGET_PREFIX, rt.ROLE_REST_OPERATION),
        dlq_cache=inp.reliability.dlq.cache_key if inp.reliability.dlq is not None else None,
    )


def _validate_topology(inp: DatabaseToApiSyncInputV1, *, phase: str) -> None:
    """The material-free rules, run FIRST by the preparer and again by the
    executor, so neither half can emit for an input the other refuses."""
    # The architect retry decision: refused by name, never silently reduced.
    if inp.reliability.retry.max_attempts != _SUPPORTED_MAX_ATTEMPTS:
        raise _refusal(DB_TO_API_RETRY_UNAVAILABLE, _RETRY_POINTER, phase=phase)
    keys = _keys(inp)
    if (
        inp.source.connection_key is not None
        and inp.source.connection_key == inp.target.connection_key
    ):
        raise _refusal(
            COMPOSITION_COMPONENT_KEY_COLLISION, "/raw_input/target/connection_key", phase=phase
        )
    named = {
        keys.db_connection,
        keys.db_read_profile,
        keys.db_get_operation,
        keys.target_profile,
        keys.transform_map,
        keys.rest_connection,
        keys.rest_operation,
    }
    if keys.dlq_cache is not None:
        if keys.dlq_cache in named:
            raise _refusal(
                COMPOSITION_COMPONENT_KEY_COLLISION,
                "/raw_input/reliability/dlq/cache_key",
                phase=phase,
            )
        named.add(keys.dlq_cache)
    if inp.process_key in named:
        raise _refusal(COMPOSITION_COMPONENT_KEY_COLLISION, "/raw_input/process_key", phase=phase)


# ---------------------------------------------------------------------------
# The preparer (trusted, server-side)
# ---------------------------------------------------------------------------


def prepare_database_to_api_sync(
    inp: DatabaseToApiSyncInputV1,
    material: DatabaseToApiSyncMaterialV1,
    *,
    base_catalog: _Mapping[str, IntegrationComponentSpec],
) -> PreparedRecipeComponentsV1:
    """Generate every supporting component spec for one invocation.

    Order is load-bearing; every refusal is value-free and precedes assembly:

    1. material-free topology (the retry refusal first);
    2. the shared credential boundary;
    3. connection bindings (shared selected-base rules) and the DLQ cache base;
    4. value rules through the shared authorities (their RETURNED values are
       what every later step reads);
    5. the REST request target, judged on the authorized values;
    6. the producer -> consumer contract and the watermark rules;
    7. assembly through the shared DB-read, transform and REST-send blocks.
    """
    _validate_topology(inp, phase="input")
    keys = _keys(inp)
    selected: _List[str] = []
    create_bound: _List[Tuple[str, str]] = []

    # -- 2. credential boundary --------------------------------------------
    check_credential_boundary(material, material_field_classes(DatabaseToApiSyncMaterialV1))

    # -- 3. bindings ---------------------------------------------------------
    source_key, source_bound = check_connection_binding(
        base_catalog,
        selected_key=inp.source.connection_key,
        selected_key_pointer="/raw_input/source/connection_key",
        material_connection=material.source.connection,
        material_pointer="/component_material/source/connection",
        binding_mode=inp.source.binding_mode,
        family="database",
        auth_config_key="auth_mode",
        auth_expected=str(inp.source.auth_mode),
        auth_pointer="/raw_input/source/auth_mode",
    )
    if source_bound is not None:
        create_bound.append(source_bound)
    if source_key is not None:
        selected.append(source_key)
    target_key, target_bound = check_connection_binding(
        base_catalog,
        selected_key=inp.target.connection_key,
        selected_key_pointer="/raw_input/target/connection_key",
        material_connection=material.target.connection,
        material_pointer="/component_material/target/connection",
        binding_mode=inp.target.binding_mode,
        family="rest",
        auth_config_key="auth",
        auth_expected=_rt().REST_CREATE_AUTH_MAP["none"],
        auth_pointer="/raw_input/target/connection_key",
    )
    if target_bound is not None:
        create_bound.append(target_bound)
    if target_key is not None and target_key not in selected:
        selected.append(target_key)
    if keys.dlq_cache is not None:
        # An EXISTING cache: reference-only and written under no conflict
        # policy — the shared selected-base rule for a reuse binding.
        _check_base_component(
            base_catalog,
            keys.dlq_cache,
            path="/raw_input/reliability/dlq/cache_key",
            component_type="documentcache",
            binding_mode="reuse",
        )
        selected.append(keys.dlq_cache)

    # -- 4. value rules --------------------------------------------------------
    db = authorize_db_connection(
        material.source.connection,
        pointer="/component_material/source/connection",
        auth_mode=inp.source.auth_mode,
    )
    read = authorize_db_read(material.source.read, pointer="/component_material/source/read")
    rest_connection = authorize_rest_connection(
        material.target.connection, pointer="/component_material/target/connection"
    )
    request = authorize_rest_request(
        material.target.operation,
        pointer="/component_material/target/operation",
        method=inp.target.method,
        # Validated for shape only: the legacy route recorded expected status
        # codes as intent and emitted them nowhere (no operation field, no
        # frozen row), so this preset accepts none and the authority sees its
        # default.
        expected_status_codes=_DEFAULT_STATUS_CODES_V2,
    )
    operations = authorize_transform_operations(
        material.transform.operations,
        base_catalog=base_catalog,
        pointer="/component_material/transform/operations",
    )

    # -- 5. the request target ------------------------------------------------
    rest_target = check_rest_request(
        connection_key=keys.rest_connection,
        path=request.path,
        path_pointer="/component_material/target/operation/path",
        created_base_url=(
            rest_connection.base_url
            if isinstance(material.target.connection, FanoutRestCreateMaterialV2)
            else None
        ),
        connection_pointer="/component_material/target/connection",
        selected_key=inp.target.connection_key,
        selected_key_pointer="/raw_input/target/connection_key",
        binding_mode=inp.target.binding_mode,
        base_catalog=base_catalog,
        query_parameters=request.query_parameters,
        query_parameters_pointer="/component_material/target/operation/query_parameters",
    )

    # -- 6. producer -> consumer contract and watermark ----------------------
    rt = _rt()
    source_fields = {field.name for field in read.result_schema.fields}
    leaves = _flatten_payload_profile_leaves(material.payload_profile)
    bound_paths: set = set()
    for op_index, operation in enumerate(operations):
        pointer = f"/component_material/transform/operations/{op_index}"
        if isinstance(operation, FanoutDirectOperationV2):
            inputs, outputs = [operation.source_field], [operation.target_path]
        elif isinstance(operation, FanoutFunctionOperationV2):
            inputs, outputs = list(operation.inputs), [operation.target_path]
        else:
            inputs, outputs = list(operation.inputs), list(operation.outputs)
            _check_base_component(
                base_catalog,
                operation.script_component_key,
                path=pointer + "/script_component_key",
                component_type="script.mapping",
            )
            if operation.script_component_key not in selected:
                selected.append(operation.script_component_key)
        if any(name not in source_fields for name in inputs):
            raise _refusal(COMPOSITION_CONTRACT_MISMATCH, pointer, phase="input")
        for output in outputs:
            # A leaf bound twice is refused like the legacy contract did.
            if output not in leaves or output in bound_paths:
                raise _refusal(COMPOSITION_CONTRACT_MISMATCH, pointer, phase="input")
            bound_paths.add(output)
    index = rt.profile_from_json_schema(material.payload_profile)["field_index_by_path"]
    if rt.required_target_coverage_gaps(index, bound_paths):
        raise _refusal(
            COMPOSITION_CONTRACT_MISMATCH, "/component_material/payload_profile", phase="input"
        )

    qp_pointer = "/component_material/target/operation/query_parameters"
    watermark_names = tuple(
        parameter.name
        for parameter in request.query_parameters
        if parameter.value_source == "watermark"
    )
    if watermark_names and inp.watermark is None:
        raise _refusal(GOVERNANCE_WATERMARK_INCONSISTENT, qp_pointer, phase="input")
    deferred: Tuple[Tuple[Tuple[str, str], Tuple[str, ...]], ...] = ()
    if watermark_names:
        deferred = (((inp.process_key, keys.rest_operation), watermark_names),)
    if inp.watermark is not None:
        if inp.watermark.field not in source_fields:
            raise _refusal(
                GOVERNANCE_WATERMARK_INCONSISTENT, "/raw_input/watermark/field", phase="input"
            )
        refs = inp.watermark.query_parameter_refs
        seen: set = set()
        for ref_index, name in enumerate(refs):
            if name in seen or name not in watermark_names:
                raise _refusal(
                    GOVERNANCE_WATERMARK_INCONSISTENT,
                    f"/raw_input/watermark/query_parameter_refs/{ref_index}",
                    phase="input",
                )
            seen.add(name)
        # A watermark-sourced parameter is never emitted; the declaration's refs
        # are the only record of it, so one the refs omit is refused.
        for qp_index, parameter in enumerate(request.query_parameters):
            if parameter.value_source == "watermark" and parameter.name not in refs:
                raise _refusal(
                    GOVERNANCE_WATERMARK_INCONSISTENT, f"{qp_pointer}/{qp_index}", phase="input"
                )

    # -- 7. assembly -----------------------------------------------------------
    context = PrimitiveBuildContext(
        integration_name=inp.naming.component_prefix,
        component_prefix=inp.naming.component_prefix,
        folder_path=inp.naming.folder_name,
    )
    components: _List[IntegrationComponentSpec] = []
    components.extend(
        emit_db_read_components(
            db,
            read,
            context=context,
            key_prefix=rt.SOURCE_PREFIX,
            pointer="/component_material/source",
            base_connection_key=inp.source.connection_key,
        )
    )
    components.extend(
        emit_transform_components(
            operations,
            context=context,
            key_prefix=rt.TRANSFORM_PREFIX,
            pointer="/component_material/transform",
            source={
                "source_profile_id": f"$ref:{keys.db_read_profile}",
                "source_profile_type": "profile.db",
                "source_field_index": {
                    field.name: {"data_type": field.data_type, "mappable": True}
                    for field in read.result_schema.fields
                },
            },
            target_payload_profile=material.payload_profile.model_dump(),
        )
    )
    components.extend(
        emit_rest_send_components(
            rest_connection,
            request,
            context=context,
            key_prefix=rt.TARGET_PREFIX,
            pointer="/component_material/target",
            request_profile_key=keys.target_profile,
            base_connection_key=inp.target.connection_key,
        )
    )
    return PreparedRecipeComponentsV1(
        supporting_components=tuple(components),
        selected_base_keys=tuple(selected),
        create_bound_base_keys=tuple(create_bound),
        rest_request_targets=(rest_target,),
        deferred_query_parameter_names=deferred,
        projection_facts=_projection_facts(inp, keys, operations),
    )


def _projection_facts(
    inp: DatabaseToApiSyncInputV1, keys: _Keys, operations: Tuple[_Any, ...]
) -> LinearRouteProjectionFactsV1:
    """Typed POINTERS to the components the root binds, plus the transform's
    documentation hints (caller content the legacy row served but no map
    component carries). The reliability row is always served; its DLQ target
    only when the root writes the cache."""
    return LinearRouteProjectionFactsV1(
        process_key=inp.process_key,
        source=LinearConnectorFactV1(
            connection_key=keys.db_connection, operation_key=keys.db_get_operation
        ),
        transform=LinearTransformFactV1(
            detail="generated_artifacts",
            row_name=inp.transform.map_label or _TRANSFORM_ROW_NAME,
            documentation_hints=tuple(
                RecipeOperationHintV1(
                    operation_type=operation.operation_type,
                    target_path=target_path,
                    documentation_hint=operation.documentation_hint,
                )
                for operation in operations
                if operation.documentation_hint is not None
                for target_path in (
                    operation.outputs
                    if isinstance(operation, FanoutScriptReferenceOperationV2)
                    else (operation.target_path,)
                )
            ),
        ),
        target=LinearConnectorFactV1(
            connection_key=keys.rest_connection, operation_key=keys.rest_operation
        ),
        reliability=LinearReliabilityFactV1(dlq_component_key=keys.dlq_cache),
    )


# ---------------------------------------------------------------------------
# The PURE executor
# ---------------------------------------------------------------------------


def _catch_body(inp: DatabaseToApiSyncInputV1, keys: _Keys) -> _Dict[str, _Any]:
    notify = inp.reliability.catch_notify
    return {
        "steps": (
            [{"kind": "notify", "level": notify.level, "message_template": notify.message_template}]
            if notify is not None
            else []
        ),
        "terminal": {
            "kind": "cache_put",
            "cache_ref": f"$ref:{keys.dlq_cache}",
            "label": _DLQ_CACHE_LABEL,
        },
    }


def _root(inp: DatabaseToApiSyncInputV1, keys: _Keys) -> ProcessIRV1:
    map_step = {
        "kind": "map_ref",
        "map_ref": f"$ref:{keys.transform_map}",
        "label": None,
    }
    if keys.dlq_cache is None:
        steps: _List[_Dict[str, _Any]] = [
            {
                "kind": "source",
                "connection_ref": f"$ref:{keys.db_connection}",
                "operation_ref": f"$ref:{keys.db_get_operation}",
            },
            map_step,
            {
                "kind": "target",
                "connection_ref": f"$ref:{keys.rest_connection}",
                "operation_ref": f"$ref:{keys.rest_operation}",
            },
            {"kind": "stop"},
        ]
    else:
        # golden-000086: two connector-scoped guards, the map between them,
        # retry count zero in both (the retry decision).
        steps = [
            {
                "kind": "try_catch",
                "scope": "connector",
                "retry": {"count": 0},
                "try_body": {
                    "steps": [
                        {
                            "kind": "connector_call",
                            "operation_ref": f"$ref:{keys.db_get_operation}",
                            "action": "get",
                        }
                    ],
                    "terminal": {"kind": "continue"},
                },
                "catch_body": _catch_body(inp, keys),
            },
            map_step,
            {
                "kind": "try_catch",
                "scope": "connector",
                "retry": {"count": 0},
                "try_body": {
                    "steps": [
                        {
                            "kind": "connector_call",
                            "operation_ref": f"$ref:{keys.rest_operation}",
                        }
                    ],
                    "terminal": {"kind": "stop"},
                },
                "catch_body": _catch_body(inp, keys),
            },
        ]
    return ProcessIRV1.model_validate({"version": "1", "body": {"kind": "sequence", "steps": steps}})


def _extension_bindings(inp: DatabaseToApiSyncInputV1, keys: _Keys) -> ProcessExtensionBindingsV1:
    """Per-environment override points by the legacy archetype's policy,
    derived from the ACTUAL bindings: the DB source declares when reused or
    created with ``username_password`` (``windows_integrated`` create declares
    nothing); the REST target declares credentials only when reused (a created
    REST connection is always unauthenticated) and its base URL on opt-in."""
    rt = _rt()
    switches = inp.environment_extensions
    entries: _List[Tuple[str, str, _List[_Dict[str, str]]]] = []
    if inp.source.binding_mode == "reuse" or inp.source.auth_mode == "username_password":
        entries.append(
            (
                keys.db_connection,
                "database",
                rt.db_connection_extension_fields(
                    credentials=switches.credential_connection_fields,
                    endpoint=switches.endpoint_connection_fields,
                ),
            )
        )
    entries.append(
        (
            keys.rest_connection,
            "rest",
            rt.rest_connection_extension_fields(
                credentials=(
                    inp.target.binding_mode == "reuse"
                    and switches.rest_credential_connection_fields
                ),
                endpoint=switches.rest_endpoint_connection_fields,
            ),
        )
    )
    return ProcessExtensionBindingsV1(
        connections=tuple(
            ProcessConnectionOverrideV1(
                connection_id=f"$ref:{key}",
                connector_type=connector_type,
                fields=tuple(ProcessOverrideFieldV1(**field) for field in fields),
            )
            for key, connector_type, fields in entries
            if fields
        )
    )


def _dependencies(keys: _Keys) -> Tuple[str, ...]:
    """The root's edges, in the legacy main-process order, plus the DLQ cache."""
    ordered = [
        keys.db_connection,
        keys.db_get_operation,
        keys.transform_map,
        keys.rest_connection,
        keys.rest_operation,
    ]
    if keys.dlq_cache is not None:
        ordered.append(keys.dlq_cache)
    return tuple(ordered)


def _required_components(keys: _Keys) -> Tuple[Tuple[str, str], ...]:
    required = [
        (keys.db_connection, "connector-settings"),
        (keys.db_read_profile, "profile.db"),
        (keys.db_get_operation, "connector-action"),
        (keys.target_profile, "profile.json"),
        (keys.transform_map, "transform.map"),
        (keys.rest_connection, "connector-settings"),
        (keys.rest_operation, "connector-action"),
    ]
    if keys.dlq_cache is not None:
        required.append((keys.dlq_cache, "documentcache"))
    return tuple(required)


def _capability(requirement_id: str, subject: str) -> ConstraintRequirementV1:
    return ConstraintRequirementV1(
        contribution_kind="constraint_requirement",
        version=_VERSION,
        requirement_id=requirement_id,
        requirement=RequireCapabilityV1(
            kind="capability",
            authority="process_emitter",
            subject=subject,
            required_state="supported",
        ),
    )


def emit_database_to_api_sync(inp: DatabaseToApiSyncInputV1) -> Tuple[object, ...]:
    """The PURE executor: closed recipe input in, one process unit out.

    Emits, in order: one ``ProcessUnitContributionV1`` (root + envelope with
    explicit edges and typed extension bindings), the recorded watermark intent
    when declared, and the requirements — the process, every supporting
    component by key and type, and the emitter capabilities the root uses."""
    _validate_topology(inp, phase="execution")
    keys = _keys(inp)
    naming = inp.naming
    envelope = ProcessComponentEnvelopeV1(
        component_key=inp.process_key,
        name=naming.process_name or f"{naming.component_prefix} {_PROCESS_NAME_SUFFIX}",
        action="create",
        folder_name=naming.folder_name,
        depends_on=_dependencies(keys),
        process_extensions=_extension_bindings(inp, keys),
    )
    contributions: list = [
        ProcessUnitContributionV1(
            contribution_kind="process_unit",
            version="1",
            contribution_id="unit.process",
            unit=ProcessAuthoringUnitV1(envelope=envelope, process_ir=_root(inp, keys)),
        )
    ]
    if inp.watermark is not None:
        contributions.append(
            RecordedIntentContributionV1(
                contribution_kind="recorded_intent",
                version="1",
                intent_id="watermark",
                process_key=inp.process_key,
                declaration=WatermarkDeclarationV1(
                    source_profile_ref=f"$ref:{keys.db_read_profile}",
                    field=inp.watermark.field,
                    kind=inp.watermark.kind,
                    initial_value=inp.watermark.initial_value,
                    query_parameter_refs=inp.watermark.query_parameter_refs,
                ),
                status="recorded_not_wired",
            )
        )
    contributions.append(
        ConstraintRequirementV1(
            contribution_kind="constraint_requirement",
            version=_VERSION,
            requirement_id="req.process",
            requirement=RequireProcessV1(kind="process", process_key=inp.process_key),
        )
    )
    for position, (key, component_type) in enumerate(_required_components(keys)):
        contributions.append(
            ConstraintRequirementV1(
                contribution_kind="constraint_requirement",
                version=_VERSION,
                requirement_id=f"req.component.{position}",
                requirement=RequireComponentV1(
                    kind="component", component_key=key, component_type=component_type
                ),
            )
        )
    # A connector call lowers to the same source/target connector shapes the
    # plain chain uses (the entry call is the source shape).
    contributions.append(_capability("req.capability.source", "connectoraction_source"))
    contributions.append(_capability("req.capability.target", "connectoraction_target"))
    contributions.append(_capability("req.capability.map", "map"))
    if keys.dlq_cache is not None:
        contributions.append(_capability("req.capability.catch", "catcherrors"))
        contributions.append(_capability("req.capability.cache", "doccacheload"))
        if inp.reliability.catch_notify is not None:
            contributions.append(_capability("req.capability.notify", "notify"))
    return tuple(contributions)


__all__ = [
    "RECIPE_ID",
    "RECIPE_VERSION",
    "DatabaseToApiSyncInputV1",
    "DatabaseToApiSyncMaterialV1",
    "emit_database_to_api_sync",
    "prepare_database_to_api_sync",
]
