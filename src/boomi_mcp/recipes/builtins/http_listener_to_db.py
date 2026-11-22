"""The typed inbound-HTTP -> database recipe preset (issue #159 S2).

``boomi.archetype.http_listener_to_db`` 1.0.0: a Web Services Server listener
entry, one transform map, and a database write target, emitted as ONE direct
canonical process unit:

    listener(operation) -> map_ref -> target(connection, write operation) -> stop

It replaces the legacy ``patterns/archetypes/http_listener_to_db`` archetype,
which emitted a ``sync_pipeline`` stage graph for the interim lift. Nothing here
imports that archetype: the legacy route stays callable as a TEST oracle only.

**The listener is authored as canonical ProcessIR.** The root's first step is a
``listener`` entry naming only the Listen operation (#158). The process is
started by its inbound requests, and the listener process options follow from
that entry — the compiler's entry derivation decides the execution profile; no
option is authored here.

**Two halves, one trust split** (as fan-out 2.0.0, the worked example):

* :func:`prepare_http_listener_to_db` — server-side, TRUSTED. It receives the
  validated recipe input AND the closed component material (the listener
  operation and request profile, the optional API Service route, the transform
  operations, and the SHARED DB-write block) and builds every supporting
  component spec through the surviving primitives (``wss_listen``,
  ``field_map``, ``db_write``). Every refusal is value-free and runs before any
  component exists.
* :func:`emit_http_listener_to_db` — the PURE executor. It sees only the closed,
  secret-free recipe input and emits one ``ProcessUnitContributionV1`` and its
  requirements. Both halves derive the component keys from the same function.

The listener parts (input, material blocks, request profile, Listen operation,
API Service wrapper, uncallable-route refusal) are shared with
``http_listener_to_rest``, which imports them from here and declares this
module as an implementation dependency.
"""

from __future__ import annotations

from dataclasses import dataclass
from types import MappingProxyType
from typing import Any, Dict, List, Literal, Mapping, Optional, Tuple

from pydantic import Field, model_validator
from pydantic_core import PydanticCustomError

from ...errors import (
    COMPOSITION_COMPONENT_KEY_COLLISION,
    COMPOSITION_CONTRACT_MISMATCH,
)
from ...models.derived_flows import (
    LinearConnectorFactV1,
    LinearRouteProjectionFactsV1,
    LinearTransformFactV1,
)
from ...models.integration_models import IntegrationComponentSpec
from ...models.process_component import (
    ProcessAuthoringUnitV1,
    ProcessComponentEnvelopeV1,
    ProcessExtensionBindingsV1,
)
from ...models.process_ir import ProcessIRV1
from ...models.recipe_contributions import (
    ConstraintRequirementV1,
    ProcessUnitContributionV1,
    RecipeComponentKey,
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
    DbWriteMaterialV1,
    FanoutDirectOperationV2,
    FanoutFunctionOperationV2,
    FanoutTransformOperationV2,
    PreparedRecipeComponentsV1,
    _DbAuthModeV2,
    _FanoutMaterialModel,
    _builder_check,
    _check_base_component,
    _refusal,
    _rt,
    authorize_db_write_material,
    authorize_transform_operations,
    check_connection_binding,
    check_credential_boundary,
    emit_db_write_components,
    emit_transform_components,
    register_material_root,
)
from ..contracts import RecipeInputBase

_VERSION = "1"

#: The fixed key of the generated listener request profile (legacy spelling;
#: ``wss_listen`` only BINDS a request profile, so the preset emits one).
LISTENER_REQUEST_PROFILE_KEY = "listener_request_profile"
#: The fixed key of the optional API Service component (legacy spelling).
API_SERVICE_KEY = "api_service"

#: Fixed spellings the legacy archetypes served, pinned by the frozen #157
#: rows and the legacy-oracle parity tests.
_LISTENER_PROFILE_NAME_SUFFIX = "Listener Request Profile"
_API_SERVICE_NAME_SUFFIX = "API Service"
_PROCESS_NAME_SUFFIX = "HTTP Listener to Database"
_TRANSFORM_ROW_NAME = "Map inbound payload to database write profile"
#: The listener is ack-only: the Listen operation never returns a document.
_LISTENER_OUTPUT_TYPE = "none"


def _listener_rt():
    """The listener primitive and API Service path authorities, imported at
    CALL time for the same reason as ``component_material._rt``: they reach
    ``boomi_mcp`` by absolute import, and the recipe layer also loads as
    ``src.boomi_mcp``."""
    from types import SimpleNamespace

    from ...categories.components.builders._api_service_paths import (
        api_service_http_method,
        route_refuses_its_input,
    )
    from ...categories.components.builders.json_profile_builder import (
        JSONGeneratedProfileBuilder,
    )
    from ...categories.deployment.orchestration import LISTENER_ASC_ROUTE_UNCALLABLE
    from ...patterns.primitives import _helpers as helpers
    from ...patterns.primitives import wss_listen

    return SimpleNamespace(
        api_service_http_method=api_service_http_method,
        route_refuses_its_input=route_refuses_its_input,
        JSONGeneratedProfileBuilder=JSONGeneratedProfileBuilder,
        LISTENER_ASC_ROUTE_UNCALLABLE=LISTENER_ASC_ROUTE_UNCALLABLE,
        ROLE_WSS_LISTENER_OPERATION=helpers.ROLE_WSS_LISTENER_OPERATION,
        WssListenParameters=wss_listen.WssListenParameters,
        WssListenPrimitive=wss_listen.WssListenPrimitive,
    )


# ---------------------------------------------------------------------------
# Recipe input — closed and secret-free (what the PURE executor receives)
# ---------------------------------------------------------------------------


class ListenerNamingV1(RecipeInputBase):
    """Display naming. Component names derive from ``component_prefix``."""

    component_prefix: str = Field(min_length=1)
    folder_name: Optional[str] = Field(default=None, min_length=1)
    process_name: Optional[str] = Field(default=None, min_length=1)


class ListenerEntryV1(RecipeInputBase):
    """The listener entry: its root label, the build-time inbound contract and
    how the listener is published."""

    label: Optional[str] = Field(
        default=None, min_length=1, description="Optional label of the listener entry step."
    )
    inbound_validation: bool = Field(
        default=False,
        description=(
            "When true, the compiler proves at build time, from the resolved "
            "Listen operation, that inbound documents are JSON bound to the "
            "generated request profile (mode 'profile_bound'). Nothing is "
            "emitted for it and no payload is validated at run time."
        ),
    )
    publish_mode: Literal["bare_wss", "api_service"] = Field(
        default="bare_wss",
        description=(
            "'bare_wss' serves the listener on /ws/simple/... (basic and "
            "intermediate runtimes). 'api_service' adds an API Service component "
            "whose one route publishes the process on /ws/rest/... (required on "
            "apiType=advanced runtimes)."
        ),
    )


class ListenerDbTargetV1(RecipeInputBase):
    """The database write target: binding classification only."""

    binding_mode: Literal["create", "reuse"]
    #: The create-mode connection's auth mode.
    auth_mode: Optional[_DbAuthModeV2] = None  # type: ignore[valid-type]
    #: An EXISTING base component (a ``base_components`` entry of the intent)
    #: the target binds to, by key. When set, the material carries no connection.
    connection_key: Optional[RecipeComponentKey] = None

    @model_validator(mode="after")
    def _auth_mode_matches_binding(self) -> "ListenerDbTargetV1":
        if (self.binding_mode == "create") != (self.auth_mode is not None):
            raise PydanticCustomError(
                "listener_target_auth_mode",
                "auth_mode is required for a create-mode target and refused otherwise",
            )
        return self


class HttpListenerToDbInputV1(RecipeInputBase):
    """The recipe input: topology, identities and binding classifications.

    Carries NO component material: the listener operation, profiles, transform
    operations and connection settings travel in :class:`HttpListenerToDbMaterialV1`.
    """

    version: Literal["1"]
    process_key: RecipeComponentKey = "main_process"
    naming: ListenerNamingV1
    listener: ListenerEntryV1 = Field(default_factory=ListenerEntryV1)
    target: ListenerDbTargetV1


# ---------------------------------------------------------------------------
# Component material — closed, strict, repr-redacted (preparer only)
# ---------------------------------------------------------------------------


class ListenerOperationMaterialV1(_FanoutMaterialModel):
    """The Web Services Server Listen operation and its inbound request body.

    Value rules are ``wss_listen``'s and the Listen operation builder's."""

    object_name: str = Field(
        min_length=1,
        description=(
            "WSS objectName, stored verbatim on the operation. The bare listener "
            "is served on /ws/simple/{operationtype}{SentenceCase(objectName)}. "
            "Use a unique, project-specific name."
        ),
    )
    operation_type: str = Field(
        default="EXECUTE",
        min_length=1,
        description=(
            "WSS operationType: GET | QUERY | CREATE | UPDATE | UPSERT | DELETE | "
            "EXECUTE. Not an HTTP verb; behind an API Service route it decides "
            "the inherited method."
        ),
    )
    input_type: Literal["singlejson", "multijson"] = Field(
        default="singlejson",
        description="Inbound JSON document shape. This preset is JSON-input only.",
    )
    response_content_type: Literal["application/json", "application/xml", "text/plain"] = Field(
        default="text/plain",
        description="Content type of the ack response (the listener responds ack-only).",
    )
    request_profile: JSONPayloadProfile = Field(
        description=(
            "JSON profile tree of the inbound request body. The preset generates "
            "a JSON profile from it and binds it as the listener's request "
            "profile; transform inputs name its simple leaves by path."
        ),
    )


class ApiServiceMaterialV1(_FanoutMaterialModel):
    """The API Service route publishing the listener process (publish_mode
    'api_service'). Empty strings and ``None`` inherit from the Listen operation."""

    base_url_path: str = Field(
        default="",
        description=(
            "Base urlPath (the first /ws/rest/<base>/ segment); empty contributes "
            "no segment. The platform serves one deployed API Service per base "
            "path, so set a unique value per environment."
        ),
    )
    route_url_path: str = Field(
        default="", description="Route urlPath (trailing segment); empty contributes none."
    )
    object_name: Optional[str] = Field(
        default=None,
        min_length=1,
        description="Route objectName override; default inherits the Listen operation's.",
    )
    http_method: Optional[Literal["GET", "POST", "PUT", "DELETE", "PATCH"]] = Field(
        default=None,
        description="Route method override; default inherits it from the operation type.",
    )
    title: Optional[str] = Field(default=None, min_length=1, description="Display title.")
    version: str = Field(default="1.0.0", min_length=1, description="Display version.")
    component_name: Optional[str] = Field(
        default=None, min_length=1, description="Component name; default '<prefix> API Service'."
    )


class ListenerTransformMaterialV1(_FanoutMaterialModel):
    """The transform: shared mapping leaves whose inputs are request-profile
    leaf PATHS (e.g. ``Root/id``)."""

    operations: Tuple[FanoutTransformOperationV2, ...] = Field(min_length=1)


class HttpListenerToDbMaterialV1(_FanoutMaterialModel):
    """The closed component material the listener -> DB preparer consumes."""

    version: Literal["1"]
    listener: ListenerOperationMaterialV1
    #: Present only with publish_mode 'api_service'; absent there = all defaults.
    api_service: Optional[ApiServiceMaterialV1] = None
    transform: ListenerTransformMaterialV1
    target: DbWriteMaterialV1


def listener_part_field_classes() -> Dict[Tuple[type, str], Optional[str]]:
    """Classifications of the listener part blocks both listener roots reach.
    None of them carries a secret: names, closed literals, display text and
    route path segments emitted into component configuration by design."""
    return {
        (ListenerOperationMaterialV1, "object_name"): None,  # an identifier emitted by design
        (ListenerOperationMaterialV1, "operation_type"): None,  # closed by the Listen builder
        (ListenerOperationMaterialV1, "input_type"): None,  # closed literal
        (ListenerOperationMaterialV1, "response_content_type"): None,  # closed literal
        (ListenerOperationMaterialV1, "request_profile"): None,  # structural (model)
        (ApiServiceMaterialV1, "base_url_path"): None,  # a route path segment
        (ApiServiceMaterialV1, "route_url_path"): None,  # a route path segment
        (ApiServiceMaterialV1, "object_name"): None,  # an identifier
        (ApiServiceMaterialV1, "http_method"): None,  # closed literal
        (ApiServiceMaterialV1, "title"): None,  # display text
        (ApiServiceMaterialV1, "version"): None,  # display text
        (ApiServiceMaterialV1, "component_name"): None,  # display name
        (ListenerTransformMaterialV1, "operations"): None,  # structural (variants)
    }


def _root_field_classes() -> Mapping[Tuple[type, str], Optional[str]]:
    rows = listener_part_field_classes()
    rows.update(
        {
            (HttpListenerToDbMaterialV1, "version"): None,  # closed literal
            (HttpListenerToDbMaterialV1, "listener"): None,  # structural (model)
            (HttpListenerToDbMaterialV1, "api_service"): None,  # structural (model)
            (HttpListenerToDbMaterialV1, "transform"): None,  # structural (model)
            (HttpListenerToDbMaterialV1, "target"): None,  # structural (shared block)
        }
    )
    return MappingProxyType(rows)


register_material_root(HttpListenerToDbMaterialV1, _root_field_classes)


# ---------------------------------------------------------------------------
# Shared key derivation (preparer AND executor)
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class ListenerKeysV1:
    """The listener-side keys both listener presets name."""

    request_profile: str
    listen_operation: str
    transform_map: str
    api_service: Optional[str]


def listener_keys(inp: Any) -> ListenerKeysV1:
    rt = _rt()
    return ListenerKeysV1(
        request_profile=LISTENER_REQUEST_PROFILE_KEY,
        listen_operation=rt.primitive_component_key(
            rt.SOURCE_PREFIX, _listener_rt().ROLE_WSS_LISTENER_OPERATION
        ),
        transform_map=rt.primitive_component_key(rt.TRANSFORM_PREFIX, rt.ROLE_TRANSFORM_MAP),
        api_service=API_SERVICE_KEY if inp.listener.publish_mode == "api_service" else None,
    )


@dataclass(frozen=True)
class _DbTargetKeysV1:
    connection: str
    write_profile: str
    write_operation: str


def _db_target_keys(inp: HttpListenerToDbInputV1) -> _DbTargetKeysV1:
    rt = _rt()
    return _DbTargetKeysV1(
        connection=inp.target.connection_key
        or rt.primitive_component_key(rt.TARGET_PREFIX, rt.ROLE_DB_CONNECTION),
        write_profile=rt.primitive_component_key(rt.TARGET_PREFIX, rt.ROLE_DB_WRITE_PROFILE),
        write_operation=rt.primitive_component_key(rt.TARGET_PREFIX, rt.ROLE_DB_WRITE_OPERATION),
    )


def check_process_key_free(process_key: str, named: Tuple[str, ...], *, phase: str) -> None:
    """The root's own key may not be any component key the route names."""
    if process_key in named:
        raise _refusal(COMPOSITION_COMPONENT_KEY_COLLISION, "/raw_input/process_key", phase=phase)


def listener_named_keys(inp: Any) -> Tuple[str, ...]:
    keys = listener_keys(inp)
    return (keys.request_profile, keys.listen_operation, keys.transform_map, API_SERVICE_KEY)


def _db_named_keys(inp: HttpListenerToDbInputV1) -> Tuple[str, ...]:
    target = _db_target_keys(inp)
    return listener_named_keys(inp) + (
        target.connection,
        target.write_profile,
        target.write_operation,
    )


# ---------------------------------------------------------------------------
# Listener preparation (shared by both listener presets)
# ---------------------------------------------------------------------------


def check_api_service_presence(inp: Any, material: Any) -> None:
    """API Service material is accepted only when the input publishes there."""
    if inp.listener.publish_mode != "api_service" and material.api_service is not None:
        raise _refusal(COMPOSITION_CONTRACT_MISMATCH, "/component_material/api_service", phase="input")


def _api_service_material(material: Any) -> ApiServiceMaterialV1:
    return material.api_service if material.api_service is not None else ApiServiceMaterialV1()


def check_listen_operation(material: Any) -> None:
    """The Listen operation's value rules (``wss_listen`` parameters), before
    any component exists."""
    lrt = _listener_rt()
    _builder_check(
        "/component_material/listener",
        lambda: lrt.WssListenParameters(
            key_prefix=_rt().SOURCE_PREFIX,
            object_name=material.listener.object_name,
            operation_type=material.listener.operation_type,
            input_type=material.listener.input_type,
            output_type=_LISTENER_OUTPUT_TYPE,
            response_content_type=material.listener.response_content_type,
        ),
    )


def check_api_service_route(inp: Any, material: Any) -> None:
    """Refuse an API Service route nothing can call, before any component.

    An all-inherit route for a GET or QUERY operation serves GET, and the
    platform refuses GET for an operation expecting input — which every input
    type this preset accepts is (#158, measured). ``orchestrate_deploy`` refuses
    such a route before packaging with the same cause; refusing it here keeps
    apply from creating it at all. The pointer names the field that decides the
    method: the explicit route method when one is set, else the operation type.
    """
    if inp.listener.publish_mode != "api_service":
        return
    lrt = _listener_rt()
    service = _api_service_material(material)
    method = lrt.api_service_http_method(
        service.http_method, operation_type=material.listener.operation_type
    )
    if lrt.route_refuses_its_input(method, material.listener.input_type):
        pointer = (
            "/component_material/api_service/http_method"
            if service.http_method
            else "/component_material/listener/operation_type"
        )
        raise _refusal(lrt.LISTENER_ASC_ROUTE_UNCALLABLE, pointer, phase="input")


def emit_listener_request_profile(
    material: Any, context: PrimitiveBuildContext
) -> Tuple[IntegrationComponentSpec, Dict[str, Any]]:
    """The generated listener request profile and its field index (port of the
    legacy archetype's request-profile assembly, through the JSON profile
    builder)."""
    lrt = _listener_rt()
    name = f"{context.component_prefix} {_LISTENER_PROFILE_NAME_SUFFIX}"
    config: Dict[str, Any] = {
        "profile_type": "json.generated",
        "component_name": name,
        "root": material.listener.request_profile.model_dump()["root"],
    }
    if context.folder_path:
        config["folder_path"] = context.folder_path
    pointer = "/component_material/listener/request_profile"
    _builder_check(
        pointer,
        lambda: _rt().raise_for_builder_error(lrt.JSONGeneratedProfileBuilder.validate_config(config)),
    )
    field_index = lrt.JSONGeneratedProfileBuilder.build_field_index(config)
    return (
        IntegrationComponentSpec(
            key=LISTENER_REQUEST_PROFILE_KEY,
            type="profile.json",
            action="create",
            name=name,
            config=config,
        ),
        field_index,
    )


def emit_listen_operation(
    inp: Any, material: Any, context: PrimitiveBuildContext
) -> List[IntegrationComponentSpec]:
    """The Listen operation through the ``wss_listen`` primitive: ack-only
    output, bound to the generated request profile."""
    lrt = _listener_rt()
    listener = material.listener
    pointer = "/component_material/listener"
    params = _builder_check(
        pointer,
        lambda: lrt.WssListenParameters(
            key_prefix=_rt().SOURCE_PREFIX,
            object_name=listener.object_name,
            operation_type=listener.operation_type,
            input_type=listener.input_type,
            output_type=_LISTENER_OUTPUT_TYPE,
            response_content_type=listener.response_content_type,
            request_profile_id=f"$ref:{LISTENER_REQUEST_PROFILE_KEY}",
            label=inp.listener.label,
        ),
    )
    return _builder_check(pointer, lambda: lrt.WssListenPrimitive.emit_components(context, params))


def emit_api_service(
    inp: Any, material: Any, context: PrimitiveBuildContext
) -> List[IntegrationComponentSpec]:
    """The API Service component (publish_mode 'api_service'): ONE route to the
    listener process, ordered after it (port of the legacy wrapper assembly).
    Empty override attributes inherit from the linked Listen operation."""
    if inp.listener.publish_mode != "api_service":
        return []
    service = _api_service_material(material)
    name = (service.component_name or "").strip() or (
        f"{context.component_prefix} {_API_SERVICE_NAME_SUFFIX}"
    )
    route: Dict[str, Any] = {
        "process": f"$ref:{inp.process_key}",
        "http_method": (service.http_method or "").strip().upper(),
        "url_path": service.route_url_path.strip(),
        "object_name": (service.object_name or "").strip(),
        # Input/output types always inherit from the Listen operation.
        "input_type": "",
        "output_type": "",
    }
    config: Dict[str, Any] = {
        "component_type": "webservice",
        "component_name": name,
        "base_url_path": service.base_url_path.strip(),
        "version": service.version,
        "routes": [route],
    }
    if service.title:
        config["title"] = service.title
    if context.folder_path:
        config["folder_path"] = context.folder_path
    return [
        IntegrationComponentSpec(
            key=API_SERVICE_KEY,
            type="webservice",
            action="create",
            name=name,
            config=config,
            depends_on=[inp.process_key],
        )
    ]


def check_transform_contract(
    operations: Tuple[Any, ...],
    *,
    source_leaves: Mapping[str, str],
    target_paths: Mapping[str, Any],
    validate_targets: bool,
    required_gaps,
    base_catalog: Mapping[str, IntegrationComponentSpec],
    selected: List[str],
) -> None:
    """The producer -> consumer contract over the AUTHORIZED transform
    operations: every input is a declared request-profile leaf, every output a
    declared target path bound at most once, and every required target path
    bound. A map-script reference must name a base ``script.mapping`` component
    (selected once). Refusals point at the operation."""
    bound: set = set()
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
        if any(name not in source_leaves for name in inputs):
            raise _refusal(COMPOSITION_CONTRACT_MISMATCH, pointer, phase="input")
        for output in outputs:
            if validate_targets and output not in target_paths:
                raise _refusal(COMPOSITION_CONTRACT_MISMATCH, pointer, phase="input")
            if output in bound:
                raise _refusal(COMPOSITION_CONTRACT_MISMATCH, pointer, phase="input")
            bound.add(output)
    if required_gaps(bound):
        raise _refusal(COMPOSITION_CONTRACT_MISMATCH, "/component_material/transform", phase="input")


def listener_source_binding(field_index: Mapping[str, Any]) -> Dict[str, Any]:
    """The field map's source: the generated listener request profile."""
    return {
        "source_profile_id": f"$ref:{LISTENER_REQUEST_PROFILE_KEY}",
        "source_profile_type": "profile.json",
        "source_field_index": dict(field_index),
    }


def listener_context(inp: Any) -> PrimitiveBuildContext:
    naming = inp.naming
    return PrimitiveBuildContext(
        integration_name=naming.component_prefix,
        component_prefix=naming.component_prefix,
        folder_path=naming.folder_name,
    )


# ---------------------------------------------------------------------------
# The preparer (trusted, server-side)
# ---------------------------------------------------------------------------


def prepare_http_listener_to_db(
    inp: HttpListenerToDbInputV1,
    material: HttpListenerToDbMaterialV1,
    *,
    base_catalog: Mapping[str, IntegrationComponentSpec],
) -> PreparedRecipeComponentsV1:
    """Generate every supporting component spec for one invocation.

    Order is load-bearing and every refusal is value-free:

    1. the material-free topology rule (the process key names no component);
    2. the credential boundary over the whole material (shared walker);
    3. material <-> input agreement: API Service material only when published
       there, and the DB connection binding (shared selected-base rules);
    4. value rules through the authorities — the Listen operation (``wss_listen``
       parameters), the DB-write block (write-profile builder), the transform
       leaves — and the uncallable-route refusal;
    5. the producer -> consumer contract (request-profile leaves -> write-profile
       ``Fields/<col>`` / ``Conditions/<col>``);
    6. ONLY THEN the components, in the legacy order: request profile, Listen
       operation, DB connection / write profile / Send operation, map, and the
       API Service component last (it routes to the process).
    """
    check_process_key_free(inp.process_key, _db_named_keys(inp), phase="input")
    keys, target_keys = listener_keys(inp), _db_target_keys(inp)
    selected: List[str] = []
    create_bound: List[Tuple[str, str]] = []

    # -- 2. the credential boundary ----------------------------------------
    check_credential_boundary(material)

    # -- 3. agreement ------------------------------------------------------
    check_api_service_presence(inp, material)
    target_key, target_bound = check_connection_binding(
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
    )
    if target_bound is not None:
        create_bound.append(target_bound)
    if target_key is not None:
        selected.append(target_key)

    # -- 4. value rules ----------------------------------------------------
    check_listen_operation(material)
    authorized = authorize_db_write_material(
        material.target, pointer="/component_material/target", auth_mode=inp.target.auth_mode
    )
    operations = authorize_transform_operations(
        material.transform.operations,
        base_catalog=base_catalog,
        pointer="/component_material/transform/operations",
    )
    check_api_service_route(inp, material)

    # -- 5. producer -> consumer contract ----------------------------------
    write_index = dict(authorized.field_index)
    check_transform_contract(
        operations,
        source_leaves=_flatten_payload_profile_leaves(material.listener.request_profile),
        target_paths=write_index,
        # An empty index means the write profile itself is refused by the
        # builder at assembly; the transform is not judged against it first.
        validate_targets=bool(write_index),
        required_gaps=lambda bound: _rt().required_target_coverage_gaps(write_index, bound),
        base_catalog=base_catalog,
        selected=selected,
    )

    # -- 6. components -----------------------------------------------------
    context = listener_context(inp)
    profile, field_index = emit_listener_request_profile(material, context)
    components: List[IntegrationComponentSpec] = [profile]
    components.extend(emit_listen_operation(inp, material, context))
    components.extend(
        emit_db_write_components(
            authorized,
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
            source=listener_source_binding(field_index),
            target_binding={
                "target_profile_id": f"$ref:{target_keys.write_profile}",
                "target_profile_type": "profile.db",
                "target_field_index": write_index,
            },
        )
    )
    components.extend(emit_api_service(inp, material, context))

    return PreparedRecipeComponentsV1(
        supporting_components=tuple(components),
        selected_base_keys=tuple(selected),
        create_bound_base_keys=tuple(create_bound),
        projection_facts=LinearRouteProjectionFactsV1(
            process_key=inp.process_key,
            source=LinearConnectorFactV1(operation_key=keys.listen_operation),
            transform=LinearTransformFactV1(detail="schema_summary", row_name=_TRANSFORM_ROW_NAME),
            target=LinearConnectorFactV1(
                connection_key=target_keys.connection, operation_key=target_keys.write_operation
            ),
        ),
    )


# ---------------------------------------------------------------------------
# The PURE executor
# ---------------------------------------------------------------------------


def listener_root(
    inp: Any, keys: ListenerKeysV1, *, connection_key: str, operation_key: str
) -> ProcessIRV1:
    """listener -> map_ref -> target -> stop (the ENDPOINT listener form)."""
    entry: Dict[str, Any] = {
        "kind": "listener",
        "operation_ref": f"$ref:{keys.listen_operation}",
        "label": inp.listener.label,
    }
    if inp.listener.inbound_validation:
        entry["inbound_validation"] = {"mode": "profile_bound"}
    return ProcessIRV1.model_validate(
        {
            "version": "1",
            "body": {
                "kind": "sequence",
                "steps": [
                    entry,
                    {"kind": "map_ref", "map_ref": f"$ref:{keys.transform_map}", "label": None},
                    {
                        "kind": "target",
                        "connection_ref": f"$ref:{connection_key}",
                        "operation_ref": f"$ref:{operation_key}",
                        "label": None,
                    },
                    {"kind": "stop"},
                ],
            },
        }
    )


#: The emitter capabilities a listener -> map -> target root needs.
LISTENER_ROOT_CAPABILITIES: Tuple[str, ...] = ("start_listen", "map", "connectoraction_target")


def listener_unit_contributions(
    inp: Any,
    *,
    process_name: str,
    root: ProcessIRV1,
    depends_on: Tuple[str, ...],
    required: Tuple[Tuple[str, str], ...],
) -> Tuple[object, ...]:
    """One process unit (no process extensions: the legacy listener archetypes
    declared none) plus its requirements: the process, every supporting
    component by key and type, and the emitter capabilities."""
    naming = inp.naming
    envelope = ProcessComponentEnvelopeV1(
        component_key=inp.process_key,
        name=naming.process_name or process_name,
        action="create",
        folder_name=naming.folder_name,
        depends_on=depends_on,
        process_extensions=ProcessExtensionBindingsV1(),
    )
    contributions: list = [
        ProcessUnitContributionV1(
            contribution_kind="process_unit",
            version="1",
            contribution_id="unit.process",
            unit=ProcessAuthoringUnitV1(envelope=envelope, process_ir=root),
        ),
        ConstraintRequirementV1(
            contribution_kind="constraint_requirement",
            version=_VERSION,
            requirement_id="req.process",
            requirement=RequireProcessV1(kind="process", process_key=inp.process_key),
        ),
    ]
    for position, (key, component_type) in enumerate(required):
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
    for subject in LISTENER_ROOT_CAPABILITIES:
        contributions.append(
            ConstraintRequirementV1(
                contribution_kind="constraint_requirement",
                version=_VERSION,
                requirement_id=f"req.capability.{subject}",
                requirement=RequireCapabilityV1(
                    kind="capability",
                    authority="process_emitter",
                    subject=subject,
                    required_state="supported",
                ),
            )
        )
    return tuple(contributions)


def listener_required_components(keys: ListenerKeysV1) -> Tuple[Tuple[str, str], ...]:
    return (
        (keys.request_profile, "profile.json"),
        (keys.listen_operation, "connector-action"),
        (keys.transform_map, "transform.map"),
    )


def emit_http_listener_to_db(inp: HttpListenerToDbInputV1) -> Tuple[object, ...]:
    """The PURE executor: closed recipe input in, one process unit out."""
    check_process_key_free(inp.process_key, _db_named_keys(inp), phase="execution")
    keys, target = listener_keys(inp), _db_target_keys(inp)
    required = listener_required_components(keys) + (
        (target.connection, "connector-settings"),
        (target.write_profile, "profile.db"),
        (target.write_operation, "connector-action"),
    )
    if keys.api_service is not None:
        required += ((keys.api_service, "webservice"),)
    return listener_unit_contributions(
        inp,
        process_name=f"{inp.naming.component_prefix} {_PROCESS_NAME_SUFFIX}",
        root=listener_root(
            inp, keys, connection_key=target.connection, operation_key=target.write_operation
        ),
        # The legacy main-process edges, in the legacy order (never the API
        # Service: it depends on the process, not the other way round).
        depends_on=(keys.listen_operation, keys.transform_map, target.connection, target.write_operation),
        required=required,
    )


__all__ = [
    "API_SERVICE_KEY",
    "ApiServiceMaterialV1",
    "HttpListenerToDbInputV1",
    "HttpListenerToDbMaterialV1",
    "LISTENER_REQUEST_PROFILE_KEY",
    "ListenerDbTargetV1",
    "ListenerEntryV1",
    "ListenerNamingV1",
    "ListenerOperationMaterialV1",
    "ListenerTransformMaterialV1",
    "emit_http_listener_to_db",
    "prepare_http_listener_to_db",
]
