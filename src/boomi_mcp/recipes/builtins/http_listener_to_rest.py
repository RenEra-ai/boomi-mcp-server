"""The typed inbound-HTTP -> REST recipe preset (issue #159 S2).

``boomi.archetype.http_listener_to_rest`` 1.0.0: a Web Services Server listener
entry, one transform map, and a static REST send target, emitted as ONE direct
canonical process unit:

    listener(operation) -> map_ref -> target(connection, send operation) -> stop

It replaces the legacy ``patterns/archetypes/http_listener_to_rest`` archetype
(a ``sync_pipeline`` stage graph). The listener parts — input, material
blocks, request profile, Listen operation, API Service wrapper and the
uncallable-route refusal — are the ``http_listener_to_db`` preset's, imported
from that module (declared as an implementation dependency). The REST target
uses the SHARED REST blocks and authorities of ``recipes.component_material``:
connection create-or-reuse, the request-target grammar, the literal
query-name allowlist and the reused-connection stored-URL recheck.
"""

from __future__ import annotations

from dataclasses import dataclass
from types import MappingProxyType
from typing import List, Literal, Mapping, Optional, Tuple

from pydantic import Field

from ...errors import COMPOSITION_UNSUPPORTED_TOPOLOGY
from ...models.derived_flows import (
    LinearConnectorFactV1,
    LinearRouteProjectionFactsV1,
    LinearTransformFactV1,
)
from ...models.integration_models import IntegrationComponentSpec
from ...models.recipe_contributions import RecipeComponentKey
from ...patterns.archetype_parameters import (
    ApiSendRequest,
    JSONPayloadProfile,
    _flatten_payload_profile_leaves,
)
from ..component_material import (
    FanoutRestConnectionMaterialV2,
    FanoutRestCreateMaterialV2,
    FanoutRestOperationMaterialV2,
    PreparedRecipeComponentsV1,
    _DEFAULT_STATUS_CODES_V2,
    _FanoutMaterialModel,
    _RestMethodV2,
    _authority_check,
    _refusal,
    _rt,
    authorize_rest_connection,
    authorize_rest_request,
    authorize_transform_operations,
    check_connection_binding,
    check_credential_boundary,
    check_rest_request,
    emit_rest_send_components,
    emit_transform_components,
    register_material_root,
)
from ..contracts import RecipeInputBase
from .http_listener_to_db import (
    ApiServiceMaterialV1,
    ListenerEntryV1,
    ListenerNamingV1,
    ListenerOperationMaterialV1,
    ListenerTransformMaterialV1,
    check_api_service_presence,
    check_api_service_route,
    check_listen_operation,
    check_process_key_free,
    check_transform_contract,
    emit_api_service,
    emit_listen_operation,
    emit_listener_request_profile,
    listener_context,
    listener_keys,
    listener_named_keys,
    listener_part_field_classes,
    listener_required_components,
    listener_root,
    listener_source_binding,
    listener_unit_contributions,
)

_PROCESS_NAME_SUFFIX = "HTTP Listener to REST"
_TRANSFORM_ROW_NAME = "Map inbound payload to target payload"


# ---------------------------------------------------------------------------
# Recipe input — closed and secret-free
# ---------------------------------------------------------------------------


class ListenerRestTargetV1(RecipeInputBase):
    """The REST send target: binding classification and method only."""

    binding_mode: Literal["create", "reuse"]
    #: An EXISTING base connection (a ``base_components`` entry) by key. When
    #: set, the material carries no connection.
    connection_key: Optional[RecipeComponentKey] = None
    method: _RestMethodV2 = "POST"  # type: ignore[valid-type]


class HttpListenerToRestInputV1(RecipeInputBase):
    """The recipe input: topology, identities and binding classifications.
    Carries NO component material."""

    version: Literal["1"]
    process_key: RecipeComponentKey = "main_process"
    naming: ListenerNamingV1
    listener: ListenerEntryV1 = Field(default_factory=ListenerEntryV1)
    target: ListenerRestTargetV1


# ---------------------------------------------------------------------------
# Component material
# ---------------------------------------------------------------------------


class ListenerRestTargetMaterialV1(_FanoutMaterialModel):
    """The REST target: the shared connection and request blocks, and the JSON
    payload profile the transform generates and the send operation posts."""

    #: Absent exactly when the recipe input names a base ``connection_key``.
    connection: Optional[FanoutRestConnectionMaterialV2] = None
    operation: FanoutRestOperationMaterialV2
    payload_profile: JSONPayloadProfile


class HttpListenerToRestMaterialV1(_FanoutMaterialModel):
    """The closed component material the listener -> REST preparer consumes."""

    version: Literal["1"]
    listener: ListenerOperationMaterialV1
    #: Present only with publish_mode 'api_service'; absent there = all defaults.
    api_service: Optional[ApiServiceMaterialV1] = None
    transform: ListenerTransformMaterialV1
    target: ListenerRestTargetMaterialV1


def _root_field_classes() -> Mapping[Tuple[type, str], Optional[str]]:
    rows = listener_part_field_classes()
    rows.update(
        {
            (HttpListenerToRestMaterialV1, "version"): None,  # closed literal
            (HttpListenerToRestMaterialV1, "listener"): None,  # structural (model)
            (HttpListenerToRestMaterialV1, "api_service"): None,  # structural (model)
            (HttpListenerToRestMaterialV1, "transform"): None,  # structural (model)
            (HttpListenerToRestMaterialV1, "target"): None,  # structural (model)
            (ListenerRestTargetMaterialV1, "connection"): None,  # structural (variant)
            (ListenerRestTargetMaterialV1, "operation"): None,  # structural (shared block)
            (ListenerRestTargetMaterialV1, "payload_profile"): None,  # structural (shared block)
        }
    )
    return MappingProxyType(rows)


register_material_root(HttpListenerToRestMaterialV1, _root_field_classes)


# ---------------------------------------------------------------------------
# Keys
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class _RestTargetKeysV1:
    connection: str
    operation: str
    payload_profile: str


def _rest_target_keys(inp: HttpListenerToRestInputV1) -> _RestTargetKeysV1:
    rt = _rt()
    return _RestTargetKeysV1(
        connection=inp.target.connection_key
        or rt.primitive_component_key(rt.TARGET_PREFIX, rt.ROLE_REST_CONNECTION),
        operation=rt.primitive_component_key(rt.TARGET_PREFIX, rt.ROLE_REST_OPERATION),
        payload_profile=rt.primitive_component_key(rt.TRANSFORM_PREFIX, rt.ROLE_TARGET_PROFILE),
    )


def _rest_named_keys(inp: HttpListenerToRestInputV1) -> Tuple[str, ...]:
    target = _rest_target_keys(inp)
    return listener_named_keys(inp) + (target.connection, target.operation, target.payload_profile)


# ---------------------------------------------------------------------------
# The preparer (trusted, server-side)
# ---------------------------------------------------------------------------


def prepare_http_listener_to_rest(
    inp: HttpListenerToRestInputV1,
    material: HttpListenerToRestMaterialV1,
    *,
    base_catalog: Mapping[str, IntegrationComponentSpec],
) -> PreparedRecipeComponentsV1:
    """Generate every supporting component spec for one invocation.

    Order is load-bearing and every refusal is value-free:

    1. the material-free topology rule (the process key names no component);
    2. the credential boundary over the whole material (shared walker);
    3. agreement: API Service material only when published there, and the REST
       connection binding (shared selected-base rules; a created REST
       connection is always unauthenticated);
    4. the request: a STATIC path (the legacy send authority, ``ApiSendRequest``,
       refuses a ``{token}`` path), literal query parameters only, and the
       shared single-request target check on the joined target; then the value
       authorities (Listen operation, REST connection and request, transform
       leaves) and the uncallable-route refusal;
    5. the producer -> consumer contract (request-profile leaves -> payload
       leaves);
    6. ONLY THEN the components, in the legacy order: request profile, Listen
       operation, target payload profile and map, REST connection and send
       operation, and the API Service component last.
    """
    check_process_key_free(inp.process_key, _rest_named_keys(inp), phase="input")
    keys, target_keys = listener_keys(inp), _rest_target_keys(inp)
    selected: List[str] = []
    create_bound: List[Tuple[str, str]] = []
    target = material.target

    # -- 2. the credential boundary ----------------------------------------
    check_credential_boundary(material)

    # -- 3. agreement ------------------------------------------------------
    check_api_service_presence(inp, material)
    target_key, target_bound = check_connection_binding(
        base_catalog,
        selected_key=inp.target.connection_key,
        selected_key_pointer="/raw_input/target/connection_key",
        material_connection=target.connection,
        material_pointer="/component_material/target/connection",
        binding_mode=inp.target.binding_mode,
        family="rest",
        auth_config_key="auth",
        auth_expected=_rt().REST_CREATE_AUTH_MAP["none"],
        auth_pointer="/raw_input/target/connection_key",
    )
    if target_bound is not None:
        create_bound.append(target_bound)
    if target_key is not None:
        selected.append(target_key)

    # -- 4. the request, then the value authorities ------------------------
    path_pointer = "/component_material/target/operation/path"
    _authority_check(path_pointer, ApiSendRequest, method=inp.target.method, path=target.operation.path)
    for qp_index, parameter in enumerate(target.operation.query_parameters):
        if parameter.value_source != "literal":
            # A listener route has no watermark: such a parameter would be
            # recorded intent nothing reads, so it is refused, not dropped.
            raise _refusal(
                COMPOSITION_UNSUPPORTED_TOPOLOGY,
                f"/component_material/target/operation/query_parameters/{qp_index}/value_source",
                phase="input",
            )
    rest_target = check_rest_request(
        connection_key=target_keys.connection,
        path=target.operation.path.strip(),
        path_pointer=path_pointer,
        created_base_url=(
            target.connection.base_url.strip()
            if isinstance(target.connection, FanoutRestCreateMaterialV2)
            else None
        ),
        connection_pointer="/component_material/target/connection",
        selected_key=inp.target.connection_key,
        selected_key_pointer="/raw_input/target/connection_key",
        binding_mode=inp.target.binding_mode,
        base_catalog=base_catalog,
        query_parameters=target.operation.query_parameters,
        query_parameters_pointer="/component_material/target/operation/query_parameters",
    )
    check_listen_operation(material)
    connection = authorize_rest_connection(
        target.connection, pointer="/component_material/target/connection"
    )
    request = authorize_rest_request(
        target.operation,
        pointer="/component_material/target/operation",
        method=inp.target.method,
        expected_status_codes=_DEFAULT_STATUS_CODES_V2,
    )
    operations = authorize_transform_operations(
        material.transform.operations,
        base_catalog=base_catalog,
        pointer="/component_material/transform/operations",
    )
    check_api_service_route(inp, material)

    # -- 5. producer -> consumer contract ----------------------------------
    payload_index = _rt().profile_from_json_schema(target.payload_profile)["field_index_by_path"]
    check_transform_contract(
        operations,
        source_leaves=_flatten_payload_profile_leaves(material.listener.request_profile),
        target_paths=_flatten_payload_profile_leaves(target.payload_profile),
        validate_targets=True,
        required_gaps=lambda bound: _rt().required_target_coverage_gaps(payload_index, bound),
        base_catalog=base_catalog,
        selected=selected,
    )
    # -- 6. components -----------------------------------------------------
    context = listener_context(inp)
    profile, field_index = emit_listener_request_profile(material, context)
    components: List[IntegrationComponentSpec] = [profile]
    components.extend(emit_listen_operation(inp, material, context))
    components.extend(
        emit_transform_components(
            operations,
            context=context,
            key_prefix=_rt().TRANSFORM_PREFIX,
            pointer="/component_material/transform",
            source=listener_source_binding(field_index),
            target_payload_profile=target.payload_profile.model_dump(),
        )
    )
    components.extend(
        emit_rest_send_components(
            connection,
            request,
            context=context,
            key_prefix=_rt().TARGET_PREFIX,
            pointer="/component_material/target",
            request_profile_key=target_keys.payload_profile,
            base_connection_key=inp.target.connection_key,
        )
    )
    components.extend(emit_api_service(inp, material, context))

    return PreparedRecipeComponentsV1(
        supporting_components=tuple(components),
        selected_base_keys=tuple(selected),
        create_bound_base_keys=tuple(create_bound),
        rest_request_targets=(rest_target,),
        projection_facts=LinearRouteProjectionFactsV1(
            process_key=inp.process_key,
            source=LinearConnectorFactV1(operation_key=keys.listen_operation),
            transform=LinearTransformFactV1(detail="schema_summary", row_name=_TRANSFORM_ROW_NAME),
            # No expected_status_codes: the frozen listener -> REST rows serve none.
            target=LinearConnectorFactV1(
                connection_key=target_keys.connection, operation_key=target_keys.operation
            ),
        ),
    )


# ---------------------------------------------------------------------------
# The PURE executor
# ---------------------------------------------------------------------------


def emit_http_listener_to_rest(inp: HttpListenerToRestInputV1) -> Tuple[object, ...]:
    """The PURE executor: closed recipe input in, one process unit out."""
    check_process_key_free(inp.process_key, _rest_named_keys(inp), phase="execution")
    keys, target = listener_keys(inp), _rest_target_keys(inp)
    required = listener_required_components(keys) + (
        (target.payload_profile, "profile.json"),
        (target.connection, "connector-settings"),
        (target.operation, "connector-action"),
    )
    if keys.api_service is not None:
        required += ((keys.api_service, "webservice"),)
    return listener_unit_contributions(
        inp,
        process_name=f"{inp.naming.component_prefix} {_PROCESS_NAME_SUFFIX}",
        root=listener_root(inp, keys, connection_key=target.connection, operation_key=target.operation),
        depends_on=(keys.listen_operation, keys.transform_map, target.connection, target.operation),
        required=required,
    )


__all__ = [
    "HttpListenerToRestInputV1",
    "HttpListenerToRestMaterialV1",
    "ListenerRestTargetMaterialV1",
    "ListenerRestTargetV1",
    "emit_http_listener_to_rest",
    "prepare_http_listener_to_rest",
]
