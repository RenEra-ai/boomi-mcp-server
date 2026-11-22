"""Built-in sync-pipeline recipes: API->API and API->DB (issue #145 M12.10).

Both presets are thin ``sync_pipeline`` presets — fetch, map, send — so their
native ProcessIR root is the same four-step sequence, and only the target's
connector family differs. They are registered as TWO recipes rather than one
parameterized recipe. Their declared capability requirements are IDENTICAL today
— both are source/target/map emitters — so the reason is not a present
difference but a structural one: two registrations give each preset its own
descriptor, its own provenance hash and its own place to diverge when the
Database send eventually needs a requirement the REST send does not. One
registration would have to declare the union from the day they diverged, and an
API->API run would then pass a preflight for an emitter it never touches.

**Nothing in this module can reach a credential.** The input model carries
component references, closed enumerations, optional labels, and opaque slot
names. The connection settings, SQL, headers and profile bodies the archetype
legitimately accepts stay in the compatibility adapter's private catalog and are
never projected here.
"""

from __future__ import annotations

from typing import Optional, Tuple

from pydantic import Field

from ...models.process_ir import ProcessIRV1
from ...models.recipe_contributions import (
    ComponentContributionV1,
    ConstraintRequirementV1,
    ProcessIRPatchV1,
    RecipeComponentKey,
    RecipeComponentType,
    RecipeSemanticId,
    RequireCapabilityV1,
    RequireComponentV1,
    RequireProcessV1,
    SetProcessRootV1,
)
from ..contracts import RecipeInputBase

_VERSION = "1"


class SyncComponentSlotV1(RecipeInputBase):
    """The header of one component the recipe will contribute, plus its slot."""

    contribution_id: RecipeSemanticId
    component_key: RecipeComponentKey
    component_type: RecipeComponentType
    materialization_mode: str = Field(pattern=r"^(create|update|reuse_reference)$")
    materializer_slot: RecipeSemanticId


class SyncRecipeInputV1(RecipeInputBase):
    """The SAFE projection of a sync preset's parameters."""

    version: str = Field(pattern=r"^1$")
    process_key: RecipeComponentKey
    source_connection_ref: str = Field(min_length=1)
    source_operation_ref: str = Field(min_length=1)
    map_ref: str = Field(min_length=1)
    target_connection_ref: str = Field(min_length=1)
    target_operation_ref: str = Field(min_length=1)
    source_label: Optional[str] = None
    map_label: Optional[str] = None
    target_label: Optional[str] = None
    component_slots: Tuple[SyncComponentSlotV1, ...] = Field(min_length=1)


def _root(inp: SyncRecipeInputV1) -> ProcessIRV1:
    return ProcessIRV1.model_validate(
        {
            "version": "1",
            "body": {
                "kind": "sequence",
                "steps": [
                    {
                        "kind": "source",
                        "connection_ref": inp.source_connection_ref,
                        "operation_ref": inp.source_operation_ref,
                        "label": inp.source_label,
                    },
                    {
                        "kind": "map_ref",
                        "map_ref": inp.map_ref,
                        "label": inp.map_label,
                    },
                    {
                        "kind": "target",
                        "connection_ref": inp.target_connection_ref,
                        "operation_ref": inp.target_operation_ref,
                        "label": inp.target_label,
                    },
                    {"kind": "stop"},
                ],
            },
        }
    )


def _contributions(inp: SyncRecipeInputV1) -> Tuple[object, ...]:
    """Ordered components, then the root patch, then the requirements.

    Component order IS the preset's existing ``spec.components`` order — the
    caller projects it that way — and nothing here re-sorts it. That order is
    what the parity fixtures pin.
    """
    contributions: list = [
        ComponentContributionV1(
            contribution_kind="component_contribution",
            version=_VERSION,
            contribution_id=slot.contribution_id,
            component_key=slot.component_key,
            component_type=slot.component_type,
            materialization_mode=slot.materialization_mode,
            materializer_slot=slot.materializer_slot,
        )
        for slot in inp.component_slots
    ]
    contributions.append(
        ProcessIRPatchV1(
            contribution_kind="process_ir_patch",
            version=_VERSION,
            process_key=inp.process_key,
            operations=(
                SetProcessRootV1(
                    operation_id="op.root",
                    op="set_process_root",
                    slot="root",
                    root=_root(inp),
                ),
            ),
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
    for slot in inp.component_slots:
        contributions.append(
            ConstraintRequirementV1(
                contribution_kind="constraint_requirement",
                version=_VERSION,
                requirement_id=f"req.component.{slot.contribution_id}",
                requirement=RequireComponentV1(
                    kind="component",
                    component_key=slot.component_key,
                    component_type=slot.component_type,
                ),
            )
        )
    contributions.append(
        ConstraintRequirementV1(
            contribution_kind="constraint_requirement",
            version=_VERSION,
            requirement_id="req.capability.target",
            requirement=RequireCapabilityV1(
                kind="capability",
                authority="process_emitter",
                subject="connectoraction_target",
                required_state="supported",
            ),
        )
    )
    return tuple(contributions)


def emit_api_to_api_sync(inp: SyncRecipeInputV1) -> Tuple[object, ...]:
    """REST fetch -> map -> REST send."""
    return _contributions(inp)


def emit_api_to_database_sync(inp: SyncRecipeInputV1) -> Tuple[object, ...]:
    """REST fetch -> map -> Database send."""
    return _contributions(inp)


__all__ = [
    "SyncComponentSlotV1",
    "SyncRecipeInputV1",
    "emit_api_to_api_sync",
    "emit_api_to_database_sync",
]


# ===========================================================================
# #159 S2: API->DB 1.0.0 — a typed direct process unit with component material
# ===========================================================================
#
# Registered BESIDE the 0.1.0 slot recipe above, which stays unchanged (and the
# default) through S2. Unlike 0.1.0, this version builds every supporting
# component itself, through the shared component-material channel
# (``recipes.component_material``, its declared implementation dependency):
#
# * :class:`ApiToDatabaseSyncInputV1` — the closed, secret-free recipe input the
#   PURE executor sees: topology, binding classifications, names;
# * :class:`ApiToDatabaseSyncMaterialV1` — the closed component material only
#   the trusted preparer reads: the REST source connection, the fetch request
#   and its JSON response profile, the shared DB-write block, and the transform
#   operations;
# * :func:`prepare_api_to_database_sync_v1` — refusals first (value-free, before
#   any component exists), then the components in the legacy preset's order;
# * :func:`emit_api_to_database_sync_v1` — one ``ProcessUnitContributionV1``
#   (REST fetch -> map -> database write -> stop) and its requirements.
#
# The material is the ONLY place a connection setting can travel; the recipe
# input, the contributions and provenance never carry one.

from dataclasses import dataclass as _dataclass
from types import MappingProxyType as _MappingProxyType
from typing import Any as _Any
from typing import Dict as _Dict
from typing import List as _List
from typing import Literal as _Literal
from typing import Mapping as _Mapping

from pydantic import model_validator as _model_validator
from pydantic_core import PydanticCustomError as _PydanticCustomError

from ...errors import (
    COMPOSITION_COMPONENT_KEY_COLLISION as _KEY_COLLISION,
    COMPOSITION_CONTRACT_MISMATCH as _CONTRACT_MISMATCH,
    COMPOSITION_UNSUPPORTED_TOPOLOGY as _UNSUPPORTED_TOPOLOGY,
    GOVERNANCE_WATERMARK_INCONSISTENT as _WATERMARK_INCONSISTENT,
)
from ...models.derived_flows import (
    LinearConnectorFactV1,
    LinearRouteProjectionFactsV1,
    LinearTransformFactV1,
    RecipeOperationHintV1,
)
from ...models.integration_models import IntegrationComponentSpec
from ...models.process_component import (
    ProcessAuthoringUnitV1,
    ProcessComponentEnvelopeV1,
)
from ...models.recipe_contributions import ProcessUnitContributionV1
from ...patterns.archetype_parameters import (
    JSONPayloadProfile,
    _flatten_payload_profile_leaves,
)
from ...patterns.base import PrimitiveBuildContext
from ..component_material import (
    DbWriteMaterialV1,
    FanoutDirectOperationV2,
    FanoutFunctionOperationV2,
    FanoutRestConnectionMaterialV2,
    FanoutRestCreateMaterialV2,
    FanoutRestOperationMaterialV2,
    FanoutTransformOperationV2,
    PreparedRecipeComponentsV1,
    _DbAuthModeV2,
    _FanoutMaterialModel,
    _authority_check,
    _builder_check,
    _refusal,
    _rt,
    authorize_db_write_material,
    authorize_rest_connection,
    authorize_rest_request,
    authorize_transform_operations,
    check_connection_binding,
    check_credential_boundary,
    check_rest_request,
    emit_db_write_components,
    emit_transform_components,
    material_field_classes,
    register_material_root,
    rest_connection_params,
)

_API_DB_VERSION = "1"

#: Fixed spellings of the legacy preset, pinned against the legacy archetype's
#: emitted spec by ``tests/patterns/test_api_to_database_sync_e2e.py``.
_API_DB_PROCESS_NAME_SUFFIX = "API to Database Sync"
#: The served transform flow row's default name (the legacy preset's row).
_API_DB_TRANSFORM_ROW_NAME = "Map source response to database write profile"
#: The legacy source response profile's default display-name suffix.
_API_DB_SOURCE_PROFILE_SUFFIX = "Source Profile"


# ---------------------------------------------------------------------------
# Recipe input — closed and secret-free (what the PURE executor receives)
# ---------------------------------------------------------------------------


class ApiDbNamingV1(RecipeInputBase):
    component_prefix: str = Field(min_length=1)
    folder_name: Optional[str] = Field(default=None, min_length=1)
    process_name: Optional[str] = Field(default=None, min_length=1)


class ApiDbSourceV1(RecipeInputBase):
    """The REST source: binding classification only."""

    binding_mode: _Literal["create", "reuse"]
    #: An EXISTING base component (a ``base_components`` entry of the intent)
    #: the source binds to, by key. When set, the material carries no source
    #: connection.
    connection_key: Optional[RecipeComponentKey] = None


class ApiDbTargetV1(RecipeInputBase):
    """The database write target: binding classification only."""

    binding_mode: _Literal["create", "reuse"]
    #: The create-mode connection's auth mode (required for create, refused
    #: otherwise), exactly as on the fan-out DB source.
    auth_mode: Optional[_DbAuthModeV2] = None  # type: ignore[valid-type]
    connection_key: Optional[RecipeComponentKey] = None

    @_model_validator(mode="after")
    def _auth_mode_matches_binding(self) -> "ApiDbTargetV1":
        if (self.binding_mode == "create") != (self.auth_mode is not None):
            raise _PydanticCustomError(
                "api_db_target_auth_mode",
                "auth_mode is required for a create-mode target and refused otherwise",
            )
        return self


class ApiDbTransformV1(RecipeInputBase):
    #: The NAME of the served transform flow row. Defaults to the legacy row name.
    map_label: Optional[str] = Field(
        default=None,
        min_length=1,
        description="The name of the served transform flow row. Defaults to the legacy row name.",
    )


class ApiToDatabaseSyncInputV1(RecipeInputBase):
    """The 1.0.0 recipe input: topology, identities and binding classifications.

    Carries NO connection material. URLs, paths, profiles, the write profile and
    the mapping travel in :class:`ApiToDatabaseSyncMaterialV1`, which only the
    trusted preparer reads.
    """

    version: _Literal["1"]
    process_key: RecipeComponentKey
    naming: ApiDbNamingV1
    source: ApiDbSourceV1
    target: ApiDbTargetV1
    transform: ApiDbTransformV1 = Field(default_factory=ApiDbTransformV1)


# ---------------------------------------------------------------------------
# Component material — closed, strict, repr-redacted (preparer only)
# ---------------------------------------------------------------------------


class ApiDbSourceMaterialV1(_FanoutMaterialModel):
    """The REST fetch source: connection, the GET request, the response shape."""

    #: Absent exactly when the recipe input names a base ``connection_key``.
    connection: Optional[FanoutRestConnectionMaterialV2] = None
    request: FanoutRestOperationMaterialV2
    #: The caller-authored JSON response profile: generated as the source
    #: profile, bound as the fetch's output shape and read by the map.
    response_profile: JSONPayloadProfile


class ApiDbTransformMaterialV1(_FanoutMaterialModel):
    operations: Tuple[FanoutTransformOperationV2, ...] = Field(min_length=1)


class ApiToDatabaseSyncMaterialV1(_FanoutMaterialModel):
    """The closed component material the API->DB 1.0.0 preparer consumes."""

    version: _Literal["1"]
    source: ApiDbSourceMaterialV1
    #: The shared DB-write block (connection, write profile, Send tuning).
    target: DbWriteMaterialV1
    transform: ApiDbTransformMaterialV1


def _api_db_root_field_classes_v1() -> _Mapping[Tuple[type, str], Optional[str]]:
    """The API->DB root-field classifications: this preset's own material
    models. Every shared block they reach is classified by the shared table."""
    return _MappingProxyType(
        {
            (ApiToDatabaseSyncMaterialV1, "version"): None,  # closed literal
            (ApiToDatabaseSyncMaterialV1, "source"): None,  # structural (model)
            (ApiToDatabaseSyncMaterialV1, "target"): None,  # structural (model)
            (ApiToDatabaseSyncMaterialV1, "transform"): None,  # structural (model)
            (ApiDbSourceMaterialV1, "connection"): None,  # structural (variant)
            (ApiDbSourceMaterialV1, "request"): None,  # structural (model)
            (ApiDbSourceMaterialV1, "response_profile"): None,  # structural (model)
            (ApiDbTransformMaterialV1, "operations"): None,  # structural (variants)
        }
    )


register_material_root(ApiToDatabaseSyncMaterialV1, _api_db_root_field_classes_v1)


# ---------------------------------------------------------------------------
# Shared key derivation (preparer AND executor)
# ---------------------------------------------------------------------------


@_dataclass(frozen=True)
class _ApiDbKeysV1:
    source_profile: str
    source_connection: str
    source_operation: str
    target_connection: str
    target_write_profile: str
    target_operation: str
    transform_map: str


def _api_db_keys_v1(inp: ApiToDatabaseSyncInputV1) -> _ApiDbKeysV1:
    """The legacy preset's deterministic keys, from the surviving key authorities."""
    from ...patterns import archetype_assembly
    from ...patterns.primitives import _helpers

    rt = _rt()
    key = rt.primitive_component_key
    return _ApiDbKeysV1(
        source_profile=archetype_assembly._SOURCE_RESPONSE_PROFILE_KEY,
        source_connection=inp.source.connection_key
        or key(rt.SOURCE_PREFIX, _helpers.ROLE_REST_SOURCE_CONNECTION),
        source_operation=key(rt.SOURCE_PREFIX, _helpers.ROLE_REST_SOURCE_OPERATION),
        target_connection=inp.target.connection_key
        or key(rt.TARGET_PREFIX, rt.ROLE_DB_CONNECTION),
        target_write_profile=key(rt.TARGET_PREFIX, rt.ROLE_DB_WRITE_PROFILE),
        target_operation=key(rt.TARGET_PREFIX, rt.ROLE_DB_WRITE_OPERATION),
        transform_map=key(rt.TRANSFORM_PREFIX, rt.ROLE_TRANSFORM_MAP),
    )


def _validate_api_db_topology_v1(inp: ApiToDatabaseSyncInputV1, *, phase: str) -> None:
    """The material-free rules, shared by the preparer and the executor."""
    if (
        inp.source.connection_key is not None
        and inp.source.connection_key == inp.target.connection_key
    ):
        raise _refusal(
            _UNSUPPORTED_TOPOLOGY, "/raw_input/target/connection_key", phase=phase
        )
    keys = _api_db_keys_v1(inp)
    generated = {
        keys.source_profile,
        keys.source_operation,
        keys.target_write_profile,
        keys.target_operation,
        keys.transform_map,
    }
    for pointer, selected in (
        ("/raw_input/source/connection_key", inp.source.connection_key),
        ("/raw_input/target/connection_key", inp.target.connection_key),
    ):
        if selected is not None and selected in generated:
            raise _refusal(_KEY_COLLISION, pointer, phase=phase)
    named = generated | {keys.source_connection, keys.target_connection}
    if inp.process_key in named:
        raise _refusal(_KEY_COLLISION, "/raw_input/process_key", phase=phase)


# ---------------------------------------------------------------------------
# The preparer (trusted, server-side)
# ---------------------------------------------------------------------------


def _check_fetch_request_v1(request: _Any, *, pointer: str) -> None:
    """The legacy fetch authority's rules (``ApiFetchRequest``: no ``{token}``
    path segment, a non-blank path) beside the shared request authority —
    integrator decision 2026-11-22. Value-free at ``pointer``/path."""
    from ...patterns.archetype_parameters import ApiFetchRequest

    literal = {
        parameter.name: parameter.literal_value
        for parameter in request.query_parameters
        if parameter.value_source == "literal" and parameter.literal_value is not None
    }
    _authority_check(
        pointer + "/path",
        ApiFetchRequest,
        path=request.path,
        query_parameters=literal or None,
    )


def _source_profile_component_v1(
    material: ApiToDatabaseSyncMaterialV1,
    keys: _ApiDbKeysV1,
    naming: ApiDbNamingV1,
) -> Tuple[IntegrationComponentSpec, _Dict[str, _Any]]:
    """The JSON source response profile (``rest_fetch`` binds one, it does not
    generate one) and its generated field index, through the JSON profile
    builder — the port of the legacy preset's source-profile emission."""
    rt = _rt()
    name = f"{naming.component_prefix} {_API_DB_SOURCE_PROFILE_SUFFIX}"
    config: _Dict[str, _Any] = {
        "profile_type": "json.generated",
        "component_name": name,
        "root": material.source.response_profile.model_dump()["root"],
    }
    if naming.folder_name:
        config["folder_path"] = naming.folder_name
    pointer = "/component_material/source/response_profile"
    _builder_check(
        pointer,
        lambda: rt.raise_for_builder_error(rt.JSONGeneratedProfileBuilder.validate_config(config)),
    )
    index = rt.JSONGeneratedProfileBuilder.build_field_index(config)
    component = IntegrationComponentSpec(
        key=keys.source_profile,
        type="profile.json",
        action="create",
        name=name,
        config=config,
    )
    return component, index


def _fetch_components_v1(
    inp: ApiToDatabaseSyncInputV1,
    connection: _Any,
    request: _Any,
    keys: _ApiDbKeysV1,
    context: PrimitiveBuildContext,
    field_index: _Mapping[str, _Any],
) -> _List[IntegrationComponentSpec]:
    """The REST fetch group through the ``rest_fetch`` primitive: connection and
    GET operation (or, base-bound, the operation only), bound to the generated
    response profile. Only LITERAL query parameters are emitted."""
    from ...patterns.primitives.rest_fetch import RestFetchParameters, RestFetchPrimitive

    rt = _rt()
    operation: _Dict[str, _Any] = {"path": request.path}
    literal = {
        parameter.name: parameter.literal_value
        for parameter in request.query_parameters
        if parameter.value_source == "literal" and parameter.literal_value is not None
    }
    if literal:
        operation["query_parameters"] = literal
    pointer = "/component_material/source"
    params = _builder_check(
        pointer,
        lambda: RestFetchParameters.model_validate(
            {
                "key_prefix": rt.SOURCE_PREFIX,
                "connection": rest_connection_params(
                    connection, base_connection_key=inp.source.connection_key or ""
                ),
                "operation": operation,
                "response": {
                    "profile_id": f"$ref:{keys.source_profile}",
                    "profile_type": "profile.json",
                    "field_index": dict(field_index),
                },
            }
        ),
    )
    if inp.source.connection_key is None:
        return _builder_check(pointer, lambda: RestFetchPrimitive.emit_components(context, params))
    return [
        _builder_check(
            pointer + "/request",
            lambda: RestFetchPrimitive._emit_operation(
                context,
                params,
                keys.source_operation,
                inp.source.connection_key,
                context.folder_path,
            ),
        )
    ]


def _check_transform_contract_v1(
    operations: Tuple[_Any, ...],
    source_leaves: _Mapping[str, str],
    write_index: _Mapping[str, _Any],
) -> None:
    """The port of the legacy ``_validate_transform_refs``, on the AUTHORIZED
    operations: every input a declared response-profile leaf, every output a
    write-profile column/condition, each bound at most once, every required
    column/condition bound. Value-free, at the operation's pointer."""
    rt = _rt()
    bound: set = set()
    for op_index, operation in enumerate(operations):
        here = f"/component_material/transform/operations/{op_index}"
        if isinstance(operation, FanoutDirectOperationV2):
            inputs, outputs = [operation.source_field], [operation.target_path]
        elif isinstance(operation, FanoutFunctionOperationV2):
            inputs, outputs = list(operation.inputs), [operation.target_path]
        else:
            # The shared map-script assembly names each input port by its
            # source field; a JSON leaf PATH is not a script identifier, so a
            # referenced script cannot be bound to a REST response here.
            raise _refusal(_UNSUPPORTED_TOPOLOGY, here + "/operation_type", phase="input")
        if any(name not in source_leaves for name in inputs):
            raise _refusal(_CONTRACT_MISMATCH, here, phase="input")
        for output in outputs:
            if output not in write_index or output in bound:
                raise _refusal(_CONTRACT_MISMATCH, here, phase="input")
            bound.add(output)
    if rt.required_target_coverage_gaps(dict(write_index), bound):
        raise _refusal(_CONTRACT_MISMATCH, "/component_material/transform", phase="input")


def prepare_api_to_database_sync_v1(
    inp: ApiToDatabaseSyncInputV1,
    material: ApiToDatabaseSyncMaterialV1,
    *,
    base_catalog: _Mapping[str, IntegrationComponentSpec],
) -> PreparedRecipeComponentsV1:
    """Generate every supporting component spec for one API->DB 1.0.0 invocation.

    The shared boundary's order, every refusal value-free and before any
    component exists: (1) the material-free topology; (2) the credential
    boundary; (3) each connection's binding; (4) the value authorities — the
    REST source connection and GET request (shared ``authorize_rest_request``
    AND the legacy fetch rules), the DB-write block, the transform operations;
    (5) the single-request target check on the AUTHORIZED values; (6) the
    producer -> consumer contract; then (7) the components in the legacy
    preset's order: source profile, REST fetch group, DB-write group, map.
    """
    _validate_api_db_topology_v1(inp, phase="input")
    keys = _api_db_keys_v1(inp)
    naming = inp.naming
    selected: _List[str] = []
    create_bound: _List[Tuple[str, str]] = []

    # -- 2. the credential boundary ----------------------------------------
    check_credential_boundary(material, material_field_classes(ApiToDatabaseSyncMaterialV1))

    # -- 3. connection bindings --------------------------------------------
    for selected_key, bound in (
        check_connection_binding(
            base_catalog,
            selected_key=inp.source.connection_key,
            selected_key_pointer="/raw_input/source/connection_key",
            material_connection=material.source.connection,
            material_pointer="/component_material/source/connection",
            binding_mode=inp.source.binding_mode,
            family="rest",
            auth_config_key="auth",
            auth_expected=_rt().REST_CREATE_AUTH_MAP["none"],
            auth_pointer="/raw_input/source/connection_key",
        ),
        check_connection_binding(
            base_catalog,
            selected_key=inp.target.connection_key,
            selected_key_pointer="/raw_input/target/connection_key",
            material_connection=material.target.connection,
            material_pointer="/component_material/target/connection",
            binding_mode=inp.target.binding_mode,
            family="database",
            auth_config_key="auth_mode",
            auth_expected=str(inp.target.auth_mode),
            auth_pointer="/raw_input/target/auth_mode",
        ),
    ):
        if bound is not None:
            create_bound.append(bound)
        if selected_key is not None and selected_key not in selected:
            selected.append(selected_key)

    # -- 4. value authorities ----------------------------------------------
    source_connection = authorize_rest_connection(
        material.source.connection, pointer="/component_material/source/connection"
    )
    request = authorize_rest_request(
        material.source.request,
        pointer="/component_material/source/request",
        method="GET",
        expected_status_codes=_default_status_codes(),
    )
    for qp_index, parameter in enumerate(request.query_parameters):
        if parameter.value_source == "watermark":
            # No watermark intent exists on this preset: a watermark-sourced
            # parameter would be recorded nowhere (refused, never dropped).
            raise _refusal(
                _WATERMARK_INCONSISTENT,
                f"/component_material/source/request/query_parameters/{qp_index}",
                phase="input",
            )
    _check_fetch_request_v1(request, pointer="/component_material/source/request")
    write = authorize_db_write_material(
        material.target, pointer="/component_material/target", auth_mode=inp.target.auth_mode
    )
    operations = authorize_transform_operations(
        material.transform.operations,
        base_catalog=base_catalog,
        pointer="/component_material/transform/operations",
    )

    # -- 5. the single-request target check (authorized values) -------------
    rest_target = check_rest_request(
        connection_key=keys.source_connection,
        path=request.path,
        path_pointer="/component_material/source/request/path",
        created_base_url=(
            source_connection.base_url
            if isinstance(material.source.connection, FanoutRestCreateMaterialV2)
            else None
        ),
        connection_pointer="/component_material/source/connection",
        selected_key=inp.source.connection_key,
        selected_key_pointer="/raw_input/source/connection_key",
        binding_mode=inp.source.binding_mode,
        base_catalog=base_catalog,
        query_parameters=request.query_parameters,
        query_parameters_pointer="/component_material/source/request/query_parameters",
    )

    # -- 6. producer -> consumer contract ----------------------------------
    source_leaves = _flatten_payload_profile_leaves(material.source.response_profile)
    _check_transform_contract_v1(operations, source_leaves, write.field_index)

    # -- 7. components -----------------------------------------------------
    context = PrimitiveBuildContext(
        integration_name=naming.component_prefix,
        component_prefix=naming.component_prefix,
        folder_path=naming.folder_name,
    )
    profile, field_index = _source_profile_component_v1(material, keys, naming)
    components: _List[IntegrationComponentSpec] = [profile]
    components.extend(
        _fetch_components_v1(inp, source_connection, request, keys, context, field_index)
    )
    components.extend(
        emit_db_write_components(
            write,
            context=context,
            key_prefix=_rt().TARGET_PREFIX,
            pointer="/component_material/target",
            base_connection_key=inp.target.connection_key,
        )
    )
    components.extend(
        emit_transform_components(
            operations,
            context=context,
            key_prefix=_rt().TRANSFORM_PREFIX,
            pointer="/component_material/transform",
            source={
                "source_profile_id": f"$ref:{keys.source_profile}",
                "source_profile_type": "profile.json",
                "source_field_index": dict(field_index),
            },
            target_binding={
                "target_profile_id": f"$ref:{keys.target_write_profile}",
                "target_profile_type": "profile.db",
                "target_field_index": dict(write.field_index),
            },
        )
    )
    return PreparedRecipeComponentsV1(
        supporting_components=tuple(components),
        selected_base_keys=tuple(selected),
        create_bound_base_keys=tuple(create_bound),
        rest_request_targets=(rest_target,),
        projection_facts=_api_db_projection_facts_v1(inp, keys, operations),
    )


def _default_status_codes() -> Tuple[int, ...]:
    """The REST request authority's own success-status default (a GET fetch
    declares no status expectation of its own)."""
    from ..component_material import _DEFAULT_STATUS_CODES_V2

    return _DEFAULT_STATUS_CODES_V2


def _api_db_projection_facts_v1(
    inp: ApiToDatabaseSyncInputV1,
    keys: _ApiDbKeysV1,
    operations: Tuple[_Any, ...],
) -> LinearRouteProjectionFactsV1:
    """The linear facts (pointers to the keys both halves derive) plus the one
    non-derivable transform fact. The frozen API->DB rows serve no status set."""
    return LinearRouteProjectionFactsV1(
        process_key=inp.process_key,
        source=LinearConnectorFactV1(
            connection_key=keys.source_connection, operation_key=keys.source_operation
        ),
        transform=LinearTransformFactV1(
            detail="schema_summary",
            row_name=inp.transform.map_label or _API_DB_TRANSFORM_ROW_NAME,
            documentation_hints=tuple(
                RecipeOperationHintV1(
                    operation_type=operation.operation_type,
                    target_path=operation.target_path,
                    documentation_hint=operation.documentation_hint,
                )
                for operation in operations
                if operation.documentation_hint is not None
            ),
        ),
        target=LinearConnectorFactV1(
            connection_key=keys.target_connection, operation_key=keys.target_operation
        ),
    )


# ---------------------------------------------------------------------------
# The PURE executor
# ---------------------------------------------------------------------------


def _api_db_root_v1(keys: _ApiDbKeysV1) -> ProcessIRV1:
    """REST fetch -> map -> database write -> stop."""
    return ProcessIRV1.model_validate(
        {
            "version": "1",
            "body": {
                "kind": "sequence",
                "steps": [
                    {
                        "kind": "source",
                        "connection_ref": f"$ref:{keys.source_connection}",
                        "operation_ref": f"$ref:{keys.source_operation}",
                    },
                    {"kind": "map_ref", "map_ref": f"$ref:{keys.transform_map}"},
                    {
                        "kind": "target",
                        "connection_ref": f"$ref:{keys.target_connection}",
                        "operation_ref": f"$ref:{keys.target_operation}",
                    },
                    {"kind": "stop"},
                ],
            },
        }
    )


def emit_api_to_database_sync_v1(inp: ApiToDatabaseSyncInputV1) -> Tuple[object, ...]:
    """The PURE 1.0.0 executor: closed recipe input in, one process unit out.

    Receives no component material. Emits the ``ProcessUnitContributionV1``
    (dependency edges in the legacy main-process order), then the requirements:
    the process, every supporting component by key and type, and the emitter
    capability.
    """
    _validate_api_db_topology_v1(inp, phase="execution")
    keys = _api_db_keys_v1(inp)
    naming = inp.naming
    envelope = ProcessComponentEnvelopeV1(
        component_key=inp.process_key,
        name=naming.process_name or f"{naming.component_prefix} {_API_DB_PROCESS_NAME_SUFFIX}",
        action="create",
        folder_name=naming.folder_name,
        depends_on=(
            keys.source_connection,
            keys.source_operation,
            keys.transform_map,
            keys.target_connection,
            keys.target_operation,
        ),
    )
    contributions: list = [
        ProcessUnitContributionV1(
            contribution_kind="process_unit",
            version=_API_DB_VERSION,
            contribution_id="unit.process",
            unit=ProcessAuthoringUnitV1(envelope=envelope, process_ir=_api_db_root_v1(keys)),
        ),
        ConstraintRequirementV1(
            contribution_kind="constraint_requirement",
            version=_API_DB_VERSION,
            requirement_id="req.process",
            requirement=RequireProcessV1(kind="process", process_key=inp.process_key),
        ),
    ]
    required = (
        (keys.source_profile, "profile.json"),
        (keys.source_connection, "connector-settings"),
        (keys.source_operation, "connector-action"),
        (keys.target_connection, "connector-settings"),
        (keys.target_write_profile, "profile.db"),
        (keys.target_operation, "connector-action"),
        (keys.transform_map, "transform.map"),
    )
    for position, (key, component_type) in enumerate(required):
        contributions.append(
            ConstraintRequirementV1(
                contribution_kind="constraint_requirement",
                version=_API_DB_VERSION,
                requirement_id=f"req.component.{position}",
                requirement=RequireComponentV1(
                    kind="component", component_key=key, component_type=component_type
                ),
            )
        )
    contributions.append(
        ConstraintRequirementV1(
            contribution_kind="constraint_requirement",
            version=_API_DB_VERSION,
            requirement_id="req.capability.target",
            requirement=RequireCapabilityV1(
                kind="capability",
                authority="process_emitter",
                subject="connectoraction_target",
                required_state="supported",
            ),
        )
    )
    return tuple(contributions)


__all__ = [
    *__all__,
    "ApiToDatabaseSyncInputV1",
    "ApiToDatabaseSyncMaterialV1",
    "emit_api_to_database_sync_v1",
    "prepare_api_to_database_sync_v1",
]
