"""Neutral archetype assembly helpers (issue #151, M12.14).

Shared primitive-key prefixes, REST auth vocabulary and component-assembly helpers,
lifted out of ``patterns/archetypes/database_to_api_sync.py`` and
``patterns/archetypes/api_to_api_sync.py`` so the archetypes that SURVIVE the M12.22
(#160) deletion do not import a module scheduled for removal.

Only the helpers a surviving archetype actually imports were moved. The
route-specific ``_build_field_map_params`` / ``_build_rest_send_params`` in
``database_to_api_sync`` and ``api_to_database_sync`` are DIFFERENT functions that
happen to share a name; they stay in their own modules. The copy here is the
``api_to_api_sync`` one, which ``http_listener_to_rest`` imports.

Same two prohibitions as ``archetype_parameters``: no import from
``patterns.archetypes`` / ``patterns.composition``, and no recipe-engine entry-point
call. This module imports ``archetype_parameters``; the reverse never happens.
"""

from __future__ import annotations

import re
from typing import Any, Dict, List, Optional, Protocol, Set

from pydantic import ValidationError

from ..categories.components.builders.connector_builder import (
    BuilderValidationError,
)
from ..categories.components.builders.json_profile_builder import (
    JSONGeneratedProfileBuilder,
)
from ..models.integration_models import IntegrationComponentSpec
from .primitives._helpers import (
    ROLE_REST_CONNECTION,
    ROLE_REST_OPERATION,
    ROLE_REST_SOURCE_CONNECTION,
    ROLE_REST_SOURCE_OPERATION,
    ROLE_SCRIPT,
    ROLE_TARGET_PROFILE,
    ROLE_TRANSFORM_MAP,
    primitive_component_key,
    raise_for_builder_error,
)
from .primitives.field_map import FieldMapParameters
from .primitives.rest_fetch import RestFetchParameters
from .primitives.rest_send import RestSendWithRetryParameters
from .archetype_parameters import (
    DirectApiTransformOperation,
    MapFunctionApiTransformOperation,
    MapScriptApiTransformOperation,
    NamingConfig,
    RestConnectionBinding,
)


class _ApiParametersLike(Protocol):
    """The narrow shape the moved REST helpers read off a root parameters model.

    Structural, so it costs nothing at runtime and — unlike the former
    ``"_ApiParametersLike"`` forward reference — does not point this neutral
    module back at an archetype scheduled for deletion.
    """

    source: Any
    transform: Any
    target: Any

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


# The one component this preset emits itself: the JSON source response profile the
# rest_fetch source binds and the transform map reads (rest_fetch only *binds* a
# response profile — it does not generate one).
_SOURCE_RESPONSE_PROFILE_KEY = "source_response_profile"


# Role keys for the source response profile name override and the (out-of-scope)
# script name override.
_ROLE_SOURCE_PROFILE = "source_profile"


# Characters not allowed in a Boomi script.mapping variable identifier
# (ScriptMappingBuilder enforces ^[A-Za-z_][A-Za-z0-9_]*$). Used to derive a
# language-safe in-script variable name from a JSON leaf path segment.
_NON_IDENTIFIER_RE = re.compile(r"[^A-Za-z0-9_]+")


def _script_var_name(path: str) -> str:
    """Derive a language-safe map_script variable name from a JSON leaf path.

    The variable is the path's FINAL segment, sanitized: every run of
    non-identifier characters collapses to ``_`` (so ``order-id`` -> ``order_id``)
    and a leading digit / empty result is prefixed with ``_`` so the result always
    matches ScriptMappingBuilder's ``^[A-Za-z_][A-Za-z0-9_]*$``. Underscores are
    identifier-safe, so leaves that are ALREADY valid identifiers are preserved
    verbatim — including leading/trailing underscores (``_id`` stays ``_id``, and
    ``_id`` vs ``id`` are distinct, not a collision). Two distinct paths can still
    derive the same variable (e.g. ``Root/a/id`` and ``Root/b/id`` -> the shared
    ``id`` namespace); the contract validator rejects that collision rather than
    letting it fail deep in the builder.
    """
    leaf = path.rsplit("/", 1)[-1]
    sanitized = _NON_IDENTIFIER_RE.sub("_", leaf)
    if not sanitized or sanitized[0].isdigit():
        sanitized = "_" + sanitized
    return sanitized


# ---------------------------------------------------------------------------
# Assembly helpers
# ---------------------------------------------------------------------------


def _map_rest_connection(
    binding: RestConnectionBinding, *, field: str
) -> Dict[str, Any]:
    """Map a contract RestConnectionBinding to a primitive RestConnection dict.

    Mirrors database_to_api_sync._build_rest_send_params: a create binding emits
    an unauthenticated connection (auth 'none' only — secured auth requires
    reuse, rejected with UNSUPPORTED_REST_AUTH_MODE); a reuse binding references
    an existing connection by id or name. The archetype never echoes credentials.
    """
    if binding.mode == "create":
        settings = binding.settings  # guaranteed present by the contract validator
        auth = _REST_CREATE_AUTH_MAP.get(settings.auth_mode)
        if auth is None:
            raise BuilderValidationError(
                "REST create-mode auth is not supported for executable assembly "
                "(only an unauthenticated connection can be created); use an "
                "existing connection instead.",
                error_code=UNSUPPORTED_REST_AUTH_MODE,
                field=f"{field}.binding.settings.auth_mode",
                hint=(
                    f"Set {field}.binding.mode='reuse' with an existing REST "
                    "Client connection (component_id or component_name) for "
                    "secured auth. The archetype never echoes credentials."
                ),
            )
        connection: Dict[str, Any] = {
            "mode": "create",
            "base_url": settings.base_url,
            "auth": auth,
        }
        # NOTE: settings.default_headers is intentionally NOT mapped here —
        # RestConnectionCreate has no such field. The fetch/send param builders
        # instead apply create-mode default_headers as operation-level
        # request_headers (so the caller's headers are honored, not dropped).
        return connection

    connection = {"mode": "reuse"}
    if binding.component_id:
        connection["component_id"] = binding.component_id
    if binding.component_name:
        connection["component_name"] = binding.component_name
    return connection


def _create_default_headers(binding: RestConnectionBinding) -> Dict[str, str]:
    """Return create-mode connection ``default_headers`` (empty for reuse).

    RestConnectionCreate carries no default_headers field, so these are applied as
    operation-level request_headers instead of being silently dropped. Reuse-mode
    bindings have no settings, so they contribute nothing here (an existing
    connection already carries its own configured headers).
    """
    if binding.mode == "create" and binding.settings is not None:
        return dict(binding.settings.default_headers or {})
    return {}


def _reject_case_variant_headers(headers: Dict[str, str], *, field: str) -> None:
    """Reject two keys in one header dict that differ only in letter case.

    HTTP header names are case-insensitive (RFC 7230); a single caller dict
    carrying both ``Accept`` and ``accept`` would otherwise emit two entries for
    one logical header (issue #127 A1). The cross-dict operation-wins dedupe in
    ``_merge_request_headers`` only resolves conflicts *between* the default and
    operation dicts, not *within* one. Raising (rather than silently keeping the
    last spelling, which depends on dict insertion order) surfaces the authoring
    error as a structured ``ARCHETYPE_PARAM_INVALID``. The offending key name is
    NOT echoed (defense-in-depth).
    """
    seen: Set[str] = set()
    for name in headers:
        lower = name.lower()
        if lower in seen:
            raise BuilderValidationError(
                "request headers contain two entries that differ only in "
                "letter case; HTTP header names are case-insensitive, so "
                "declare each header once.",
                error_code="ARCHETYPE_PARAM_INVALID",
                field=field,
                hint="Merge the case-variant header keys into a single entry.",
            )
        seen.add(lower)


def _merge_request_headers(
    default_headers: Dict[str, str],
    operation_headers: Optional[Dict[str, str]],
    *,
    default_field: str,
    operation_field: str,
) -> Optional[Dict[str, str]]:
    """Merge connection default_headers with operation headers (operation wins).

    Operation-level headers are more specific than connection defaults, so a
    header set in both resolves to the operation value. HTTP header names are
    case-insensitive (RFC 7230), so the conflict is resolved on the lowercased
    name — an operation ``{"accept": ...}`` overrides a default ``{"Accept": ...}``
    (emitting only the operation header, with its original spelling), rather than
    leaking two case-variant entries for the same header. Each input dict is
    first checked for case-variant duplicates *within* itself (issue #127 A1).
    Returns None when both are empty so the operation config omits
    request_headers entirely.
    """
    _reject_case_variant_headers(default_headers, field=default_field)
    operation_headers = operation_headers or {}
    _reject_case_variant_headers(operation_headers, field=operation_field)
    operation_lower = {name.lower() for name in operation_headers}
    merged = {
        name: value
        for name, value in default_headers.items()
        if name.lower() not in operation_lower
    }
    merged.update(operation_headers)
    return merged or None


def _build_rest_fetch_params(
    parameters: "_ApiParametersLike",
    overrides: Dict[str, str],
    *,
    response_profile_key: str,
    source_field_index: Dict[str, Dict[str, Any]],
) -> RestFetchParameters:
    source = parameters.source
    fetch = source.fetch_request

    operation: Dict[str, Any] = {"path": fetch.path}
    # Apply create-mode connection default_headers as operation request_headers
    # (operation headers win on key conflict) so they are honored, not dropped.
    request_headers = _merge_request_headers(
        _create_default_headers(source.binding),
        fetch.request_headers,
        default_field="source.binding.settings.default_headers",
        operation_field="source.fetch_request.request_headers",
    )
    if request_headers is not None:
        operation["request_headers"] = request_headers
    for attr in (
        "query_parameters",
        "follow_redirects",
        "return_application_errors",
        "track_response",
    ):
        value = getattr(fetch, attr)
        if value is not None:
            operation[attr] = value

    component_names: Dict[str, str] = {}
    conn_name = _named(
        overrides,
        ROLE_REST_SOURCE_CONNECTION,
        primitive_component_key(_SOURCE_PREFIX, ROLE_REST_SOURCE_CONNECTION),
    )
    if conn_name:
        component_names["connection"] = conn_name
    op_name = _named(
        overrides,
        ROLE_REST_SOURCE_OPERATION,
        primitive_component_key(_SOURCE_PREFIX, ROLE_REST_SOURCE_OPERATION),
    )
    if op_name:
        component_names["operation"] = op_name

    return _coerce_primitive_params(
        RestFetchParameters,
        {
            "key_prefix": _SOURCE_PREFIX,
            "connection": _map_rest_connection(source.binding, field="source"),
            "operation": operation,
            "response": {
                "profile_id": f"$ref:{response_profile_key}",
                "profile_type": "profile.json",
                "field_index": source_field_index,
            },
            "component_names": component_names,
        },
        field="source",
    )


def _build_field_map_params(
    parameters: "_ApiParametersLike",
    overrides: Dict[str, str],
    *,
    response_profile_key: str,
    source_field_index: Dict[str, Dict[str, Any]],
) -> FieldMapParameters:
    transform = parameters.transform

    direct: List[Dict[str, Any]] = []
    map_function: List[Dict[str, Any]] = []
    map_script: List[Dict[str, Any]] = []
    for op in transform.operations:
        if isinstance(op, DirectApiTransformOperation):
            direct.append({"source_field": op.source_path, "target_path": op.target_path})
        elif isinstance(op, MapFunctionApiTransformOperation):
            entry: Dict[str, Any] = {
                "function_type": op.function_type,
                "inputs": list(op.inputs),
                "target_path": op.target_path,
            }
            if op.parameters:
                entry["parameters"] = dict(op.parameters)
            map_function.append(entry)
        elif isinstance(op, MapScriptApiTransformOperation):
            # External script-component reuse would plan with a dangling
            # dependency (the component is not in the emitted spec). Reject it
            # with a clear error instead of an unplannable "executable" spec.
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
            # The contract's inputs/outputs are JSON leaf paths; field_map's
            # MapScriptOp needs named ports, so derive a language-safe variable
            # name from each path (sanitized final segment). Uniqueness across the
            # shared input/output namespace was already enforced by the contract
            # validator. field_map enforces that script_body is present.
            script_entry: Dict[str, Any] = {
                "inputs": [
                    {"source_path": path, "input_name": _script_var_name(path)}
                    for path in op.inputs
                ],
                "outputs": [
                    {"output_name": _script_var_name(path), "target_path": path}
                    for path in op.outputs
                ],
                "language": op.language,
            }
            if op.script_body is not None:
                script_entry["script_body"] = op.script_body
            map_script.append(script_entry)

    component_names: Dict[str, str] = {}
    target_profile_name = _named(
        overrides,
        ROLE_TARGET_PROFILE,
        primitive_component_key(_TRANSFORM_PREFIX, ROLE_TARGET_PROFILE),
    )
    if target_profile_name:
        component_names["target_profile"] = target_profile_name
    map_name = _named(
        overrides,
        ROLE_TRANSFORM_MAP,
        primitive_component_key(_TRANSFORM_PREFIX, ROLE_TRANSFORM_MAP),
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
                "source_profile_id": f"$ref:{response_profile_key}",
                "source_profile_type": "profile.json",
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
    parameters: "_ApiParametersLike", overrides: Dict[str, str]
) -> RestSendWithRetryParameters:
    target = parameters.target
    send = target.send_request

    target_profile_key = primitive_component_key(_TRANSFORM_PREFIX, ROLE_TARGET_PROFILE)
    operation: Dict[str, Any] = {
        "method": send.method,
        "path": send.path,
        # Bind the operation request body to the generated JSON payload profile.
        "request_profile_id": f"$ref:{target_profile_key}",
        "request_profile_type": "json",
    }
    if send.query_parameters:
        operation["query_parameters"] = dict(send.query_parameters)
    # Apply create-mode connection default_headers as operation request_headers
    # (operation headers win on key conflict) so they are honored, not dropped.
    request_headers = _merge_request_headers(
        _create_default_headers(target.binding),
        send.request_headers,
        default_field="target.binding.settings.default_headers",
        operation_field="target.send_request.request_headers",
    )
    if request_headers is not None:
        operation["request_headers"] = request_headers

    component_names: Dict[str, str] = {}
    conn_name = _named(
        overrides,
        ROLE_REST_CONNECTION,
        primitive_component_key(_TARGET_PREFIX, ROLE_REST_CONNECTION),
    )
    if conn_name:
        component_names["connection"] = conn_name
    op_name = _named(
        overrides,
        ROLE_REST_OPERATION,
        primitive_component_key(_TARGET_PREFIX, ROLE_REST_OPERATION),
    )
    if op_name:
        component_names["operation"] = op_name

    return _coerce_primitive_params(
        RestSendWithRetryParameters,
        {
            "key_prefix": _TARGET_PREFIX,
            "connection": _map_rest_connection(target.binding, field="target"),
            "operation": operation,
            "component_names": component_names,
        },
        field="target",
    )


def _build_source_response_profile(
    parameters: "_ApiParametersLike",
    overrides: Dict[str, str],
    folder: Optional[str],
):
    """Emit the source response profile component and return (component, field_index).

    rest_fetch only *binds* a response profile, so the preset generates one from
    the caller's source.response_profile tree (analogous to how db_extract emits
    the DB read profile). The same field index feeds both the fetch response
    binding and the field_map source binding.
    """
    naming = parameters.naming
    # Distinct default display name from field_map's "<prefix> Target Profile" so
    # a same-prefix assembly does not trip the COMPONENT_NAME_NOT_UNIQUE lint.
    profile_name = (
        _named(overrides, _ROLE_SOURCE_PROFILE)
        or f"{naming.component_prefix} Source Profile"
    )
    config: Dict[str, Any] = {
        "profile_type": "json.generated",
        "component_name": profile_name,
        "root": parameters.source.response_profile.model_dump()["root"],
    }
    if folder:
        config["folder_path"] = folder
    raise_for_builder_error(JSONGeneratedProfileBuilder.validate_config(config))
    field_index = JSONGeneratedProfileBuilder.build_field_index(config)
    component = IntegrationComponentSpec(
        key=_SOURCE_RESPONSE_PROFILE_KEY,
        type="profile.json",
        action="create",
        name=profile_name,
        config=config,
    )
    return component, field_index


def _operation_summaries(
    parameters: "_ApiParametersLike",
) -> List[Dict[str, Any]]:
    summaries: List[Dict[str, Any]] = []
    for op in parameters.transform.operations:
        if isinstance(op, DirectApiTransformOperation):
            summary: Dict[str, Any] = {
                "operation_type": "direct",
                "future_builder_issue": "#26",
                # Surface BOTH the API-oriented source_path AND the field_map
                # source_field alias so a downstream consumer can match either.
                "source_path": op.source_path,
                "source_field": op.source_path,
                "target_path": op.target_path,
            }
            if op.documentation_hint is not None:
                summary["documentation_hint"] = op.documentation_hint
            summaries.append(summary)
        elif isinstance(op, MapFunctionApiTransformOperation):
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
            summaries.append(summary)
        elif isinstance(op, MapScriptApiTransformOperation):
            summary = {
                "operation_type": "map_script",
                "future_builder_issue": "#41",
                "script_slot": op.script_slot,
                "language": op.language,
                "inputs": list(op.inputs),
                "input_count": len(op.inputs),
                "outputs": list(op.outputs),
                "output_count": len(op.outputs),
                # The in-script variable names derived from each path (so a caller
                # knows which identifiers to reference in script_body).
                "input_variables": [_script_var_name(p) for p in op.inputs],
                "output_variables": [_script_var_name(p) for p in op.outputs],
                "script_body_present": op.script_body is not None,
            }
            if op.script_body is not None:
                summary["script_body"] = op.script_body
            if op.documentation_hint is not None:
                summary["documentation_hint"] = op.documentation_hint
            summaries.append(summary)
    return summaries
