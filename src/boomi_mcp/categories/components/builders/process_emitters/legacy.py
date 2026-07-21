"""Legacy process-emitter adapters (issue #138 M12.3).

These preserve the historical private ``_emit_*`` signatures and behavior
(validation ``BuilderValidationError`` bypass-guards included) so
``process_flow_builder`` — and every test that imports an emitter directly —
keeps working unchanged, while the byte serialization itself now lives in the
shared :mod:`rendering` module. Each adapter validates exactly as before, builds
the neutral render value objects from the already-validated legacy dictionaries,
and calls the single shared renderer.

The module imports NOTHING from ``process_flow_builder`` (that would close an
import cycle); it imports the escaping/constants it needs from ``connector_builder``
and ``rendering``. The historical geometry-derivation stays here (a legacy shape's
coordinates are computed from its positional ``shape_index``); the ProcessIR
registry reads coordinates from the emission plan instead and never calls these
adapters.
"""

from __future__ import annotations

import re
from typing import Any, Dict, List, Optional, Tuple

from ..connector_builder import BuilderValidationError, _escape_xml
from . import rendering
from .rendering import (
    _CATCH_DRAGPOINT_Y,
    _CATCH_SHAPE_Y,
    _DATAPROCESS_OPERATIONS,
    _DATAPROCESS_PROFILE_TYPES,
    _DATAPROCESS_SCRIPT_LANGUAGE,
    _DOCCACHE_RETRIEVE_DEFAULT_EMPTY_BEHAVIOR,
    _DOCCACHE_RETRIEVE_EMPTY_BEHAVIORS,
    _DRAGPOINT_Y,
    _NOTIFY_CAUGHT_ERROR_TOKEN,
    _SHAPE_Y,
    _START_SHAPE_X,
    _START_SHAPE_Y,
    RenderDataProcessStep,
    RenderDecisionValue,
    RenderExceptionBinding,
    RenderPropertySource,
    RenderTransition,
    ShapeRenderContext,
    _dragpoint_x,
    _escape_message_format_text,
    _looks_like_json,
    _shape_x,
    linear_ctx,
    linear_transitions,
)

# ---------------------------------------------------------------------------
# Linear leaf shapes
# ---------------------------------------------------------------------------


def _emit_start_noaction(
    shape_name: str, next_name: Optional[str], shape_index: int
) -> str:
    ctx = ShapeRenderContext(
        shape_id=shape_name,
        x=_START_SHAPE_X,
        y=_START_SHAPE_Y,
        transitions=linear_transitions(shape_index, [next_name]),
    )
    return rendering.render_start_noaction(ctx)


def _emit_start_listen(
    shape_name: str,
    params: Dict[str, Any],
    next_name: Optional[str],
    shape_index: int,
) -> str:
    ctx = ShapeRenderContext(
        shape_id=shape_name,
        x=_START_SHAPE_X,
        y=_START_SHAPE_Y,
        transitions=linear_transitions(shape_index, [next_name]),
    )
    return rendering.render_start_listen(
        ctx,
        userlabel=params.get("userlabel") or "",
        operation_id=params.get("operation_id") or "",
    )


def _emit_connectoraction(
    shape_name: str,
    params: Dict[str, Any],
    next_name: Optional[str],
    shape_index: int,
) -> str:
    # Issue #100 G2 dynamic-path branch (legacy-only): compute the
    # <dynamicProperties> Path body + parameter-profile attribute exactly as
    # before, then hand the pre-formed XML to the shared connector renderer.
    dynamic_path = params.get("dynamic_path")
    if isinstance(dynamic_path, dict) and dynamic_path:
        ddp_name = _escape_xml(str(dynamic_path.get("ddp_name") or "").strip())
        profile_id = _escape_xml(str(dynamic_path.get("request_profile_id") or "").strip())
        has_profile_segment = any(
            isinstance(seg, dict) and seg.get("type") == "profile"
            for seg in (dynamic_path.get("segments") or [])
        )
        parameter_profile_attr = (
            f' parameter-profile="{profile_id}"' if (profile_id and has_profile_segment) else ''
        )
        inner = (
            '<parameters/>'
            '<dynamicProperties>'
            '<propertyvalue childKey="" key="path" name="Path" valueType="track">'
            f'<trackparameter defaultValue="" propertyId="dynamicdocument.{ddp_name}" '
            f'propertyName="Dynamic Document Property - {ddp_name}"/>'
            '</propertyvalue>'
            '</dynamicProperties>'
        )
    else:
        parameter_profile_attr = ''
        inner = '<parameters/><dynamicProperties/>'

    ctx = linear_ctx(shape_name, shape_index, [next_name])
    return rendering.render_connectoraction(
        ctx,
        userlabel=params.get("userlabel") or "",
        connector_type=params["connector_type"],
        action_type=params["action_type"],
        connection_id=params["connection_id"],
        operation_id=params["operation_id"],
        inner=inner,
        parameter_profile_attr=parameter_profile_attr,
    )


def _emit_message(
    shape_name: str,
    params: Dict[str, Any],
    next_name: Optional[str],
    shape_index: int,
) -> str:
    ctx = linear_ctx(shape_name, shape_index, [next_name])
    return rendering.render_message(
        ctx, userlabel=params.get("userlabel") or "", text=params.get("text") or ""
    )


def _emit_map(
    shape_name: str,
    params: Dict[str, Any],
    next_name: Optional[str],
    shape_index: int,
) -> str:
    ctx = linear_ctx(shape_name, shape_index, [next_name])
    return rendering.render_map(
        ctx, userlabel=params.get("userlabel") or "", map_id=params.get("map_id") or ""
    )


def _emit_flowcontrol(
    shape_name: str,
    params: Dict[str, Any],
    next_name: Optional[str],
    shape_index: int,
) -> str:
    for_each_count = params.get("for_each_count")
    if (
        not isinstance(for_each_count, int)
        or isinstance(for_each_count, bool)
        or for_each_count <= 0
    ):
        raise BuilderValidationError(
            "flow_control.for_each_count must be a positive integer (documents per batch).",
            error_code="PROCESS_FLOW_CONTROL_CONFIG_INVALID",
            field="flow_control.for_each_count",
            hint="v1 supports per-document batching: set for_each_count to the batch size, e.g. 10.",
        )
    ctx = linear_ctx(shape_name, shape_index, [next_name])
    return rendering.render_flowcontrol(
        ctx, userlabel=params.get("userlabel") or "", for_each_count=for_each_count
    )


# ---------------------------------------------------------------------------
# Data Process
# ---------------------------------------------------------------------------


def _dataprocess_option_tag(step: Dict[str, Any]) -> str:
    """json -> ``JSONOptions``, xml -> ``XMLOptions`` (raises on anything else)."""
    profile_type = str(step.get("profile_type") or "").strip().lower()
    if profile_type == "json":
        return "JSONOptions"
    if profile_type == "xml":
        return "XMLOptions"
    raise BuilderValidationError(
        f"Unsupported Data Process profile_type {profile_type!r}.",
        error_code="PROCESS_DATAPROCESS_CONFIG_INVALID",
        field="transform.steps[].profile_type",
        hint=f"Allowed: {sorted(_DATAPROCESS_PROFILE_TYPES)}.",
    )


def _legacy_dp_step(step: Dict[str, Any], index: int) -> RenderDataProcessStep:
    """Validate an ordered Data Process step (raising as before) and build the
    neutral render value object. ``key`` == ``index`` (the historical form)."""
    operation = str(step.get("operation") or "").strip()
    meta = _DATAPROCESS_OPERATIONS.get(operation)
    if meta is None:
        raise BuilderValidationError(
            f"Unsupported Data Process operation {operation!r}.",
            error_code="PROCESS_DATAPROCESS_OPERATION_UNSUPPORTED",
            field="transform.steps[].operation",
            hint=f"Supported: {sorted(_DATAPROCESS_OPERATIONS)}.",
        )
    if operation == "custom_scripting":
        return RenderDataProcessStep(
            operation=operation,
            index=index,
            key=index,
            name=meta["name"],
            processtype=meta["processtype"],
            script=str(step.get("script") or ""),
            language=_DATAPROCESS_SCRIPT_LANGUAGE,
            use_cache=True,
        )
    # split_documents / combine_documents — validate profile_type (raises).
    _dataprocess_option_tag(step)
    return RenderDataProcessStep(
        operation=operation,
        index=index,
        key=index,
        name=meta["name"],
        processtype=meta["processtype"],
        profile_type=str(step.get("profile_type") or ""),
        profile_id=str(step.get("profile_id") or ""),
        link_element_key=str(step.get("link_element_key") or ""),
        link_element_name=str(step.get("link_element_name") or ""),
        combine_into_link_element_key=str(step.get("combine_into_link_element_key") or "null"),
    )


def _emit_dataprocess_step(step: Dict[str, Any], index: int) -> str:
    return rendering.render_dataprocess_step(_legacy_dp_step(step, index))


def _emit_dataprocess_split_body(step: Dict[str, Any]) -> str:
    _dataprocess_option_tag(step)  # validate/raise on bad profile_type
    return rendering._render_dataprocess_split_body(
        RenderDataProcessStep(
            operation="split_documents",
            index=1,
            key=1,
            name="",
            processtype="",
            profile_type=str(step.get("profile_type") or ""),
            profile_id=str(step.get("profile_id") or ""),
            link_element_key=str(step.get("link_element_key") or ""),
            link_element_name=str(step.get("link_element_name") or ""),
        )
    )


def _emit_dataprocess_combine_body(step: Dict[str, Any]) -> str:
    _dataprocess_option_tag(step)  # validate/raise on bad profile_type
    return rendering._render_dataprocess_combine_body(
        RenderDataProcessStep(
            operation="combine_documents",
            index=1,
            key=1,
            name="",
            processtype="",
            profile_type=str(step.get("profile_type") or ""),
            profile_id=str(step.get("profile_id") or ""),
            link_element_key=str(step.get("link_element_key") or ""),
            link_element_name=str(step.get("link_element_name") or ""),
            combine_into_link_element_key=str(step.get("combine_into_link_element_key") or "null"),
        )
    )


def _emit_dataprocess(
    shape_name: str,
    params: Dict[str, Any],
    next_name: Optional[str],
    shape_index: int,
) -> str:
    steps = params.get("steps") or []
    if not steps:
        raise BuilderValidationError(
            "transform.steps must be a non-empty list when mode='dataprocess'.",
            error_code="PROCESS_DATAPROCESS_CONFIG_INVALID",
            field="transform.steps",
            hint="Provide at least one Data Process operation step.",
        )
    step_objs = tuple(_legacy_dp_step(step, i) for i, step in enumerate(steps, start=1))
    ctx = linear_ctx(shape_name, shape_index, [next_name])
    return rendering.render_dataprocess(
        ctx, userlabel=params.get("userlabel") or "", steps=step_objs
    )


# ---------------------------------------------------------------------------
# Document Cache retrieve / remove
# ---------------------------------------------------------------------------


def _emit_doccacheretrieve(
    shape_name: str,
    params: Dict[str, Any],
    next_name: Optional[str],
    shape_index: int,
) -> str:
    doc_cache_id = str(params.get("document_cache_id") or "").strip()
    if not doc_cache_id:
        raise BuilderValidationError(
            "transform.document_cache_id is required when mode='doccacheretrieve'.",
            error_code="PROCESS_DOCCACHE_RETRIEVE_CONFIG_INVALID",
            field="transform.document_cache_id",
            hint=(
                "Pass the Document Cache component id (a literal id or a $ref:KEY "
                "token in depends_on) to retrieve documents from."
            ),
        )
    empty_cache_behavior = str(
        params.get("empty_cache_behavior") or _DOCCACHE_RETRIEVE_DEFAULT_EMPTY_BEHAVIOR
    ).strip()
    if empty_cache_behavior not in _DOCCACHE_RETRIEVE_EMPTY_BEHAVIORS:
        raise BuilderValidationError(
            f"transform.empty_cache_behavior {empty_cache_behavior!r} is not supported.",
            error_code="PROCESS_DOCCACHE_RETRIEVE_CONFIG_INVALID",
            field="transform.empty_cache_behavior",
            hint=(
                "v1 supports only 'stopprocess' (Stop document execution); the "
                "backward-compat 'fail document with errors' behavior is deferred."
            ),
        )
    if params.get("load_all_documents", True) is not True:
        raise BuilderValidationError(
            "transform.load_all_documents must be true when mode='doccacheretrieve'.",
            error_code="PROCESS_DOCCACHE_RETRIEVE_CONFIG_INVALID",
            field="transform.load_all_documents",
            hint=(
                "v1 retrieves ALL cached documents (loadAllDoc=true, empty "
                "cacheKeyValues). Keyed/index retrieval is deferred."
            ),
        )
    ctx = linear_ctx(shape_name, shape_index, [next_name])
    return rendering.render_doccacheretrieve(
        ctx,
        userlabel=params.get("userlabel") or "",
        doc_cache_id=doc_cache_id,
        empty_cache_behavior=empty_cache_behavior,
    )


def _emit_doccacheremove(
    shape_name: str,
    params: Dict[str, Any],
    next_name: Optional[str],
    shape_index: int,
) -> str:
    doc_cache_id = str(params.get("document_cache_id") or "").strip()
    if not doc_cache_id:
        raise BuilderValidationError(
            "transform.document_cache_id is required when mode='doccacheremove'.",
            error_code="PROCESS_DOCCACHE_REMOVE_CONFIG_INVALID",
            field="transform.document_cache_id",
            hint=(
                "Pass the Document Cache component id (a literal id or a $ref:KEY "
                "token in depends_on) to remove documents from."
            ),
        )
    if params.get("remove_all_documents", True) is not True:
        raise BuilderValidationError(
            "transform.remove_all_documents must be true when mode='doccacheremove'.",
            error_code="PROCESS_DOCCACHE_REMOVE_CONFIG_INVALID",
            field="transform.remove_all_documents",
            hint=(
                "v1 removes ALL cached documents (removeAllDocuments=true, empty "
                "cacheKeyValues). Keyed/index removal is deferred."
            ),
        )
    ctx = linear_ctx(shape_name, shape_index, [next_name])
    return rendering.render_doccacheremove(
        ctx, userlabel=params.get("userlabel") or "", doc_cache_id=doc_cache_id
    )


# ---------------------------------------------------------------------------
# Return Documents / Stop (terminals)
# ---------------------------------------------------------------------------


def _emit_returndocuments(
    shape_name: str,
    params: Dict[str, Any],
    next_name: Optional[str],
    shape_index: int,
) -> str:
    ctx = ShapeRenderContext(
        shape_id=shape_name, x=_shape_x(shape_index), y=_SHAPE_Y, transitions=()
    )
    return rendering.render_returndocuments(ctx, label=params.get("label") or "")


def _emit_stop(shape_name: str, params: Dict[str, Any], y: float = _SHAPE_Y) -> str:
    shape_index = int(re.sub(r"\D", "", shape_name) or "1")
    ctx = ShapeRenderContext(
        shape_id=shape_name, x=_shape_x(shape_index), y=y, transitions=()
    )
    return rendering.render_stop(ctx, continue_=params.get("continue_", True))


# ---------------------------------------------------------------------------
# Set Properties (documentproperties)
# ---------------------------------------------------------------------------


def _legacy_property_source(source: Dict[str, Any]) -> RenderPropertySource:
    value_type = str(source.get("value_type") or "").strip()
    if value_type == "static":
        return RenderPropertySource(value_type="static", value=str(source.get("value") or ""))
    if value_type == "current":
        return RenderPropertySource(value_type="current")
    if value_type == "profile":
        return RenderPropertySource(
            value_type="profile",
            element_id=str(source.get("element_id") or ""),
            element_name=str(source.get("element_name") or ""),
            profile_id=str(source.get("profile_id") or "").strip(),
            profile_type=str(source.get("profile_type") or "profile.json").strip(),
        )
    if value_type == "ddp":
        return RenderPropertySource(
            value_type="ddp",
            property_name=str(source.get("property_name") or "").strip(),
            default_value=str(source.get("default_value") or ""),
        )
    if value_type == "dpp":
        return RenderPropertySource(
            value_type="dpp",
            property_name=str(source.get("property_name") or "").strip(),
            default_value=str(source.get("default_value") or ""),
        )
    raise BuilderValidationError(  # pragma: no cover — validators reject first
        f"Unknown property source value_type {value_type!r}.",
        error_code="PROCESS_PROPERTY_SOURCE_INVALID",
        field="source_values",
        hint="Internal builder bug — please report.",
    )


def _emit_property_source_value(key: int, source: Dict[str, Any]) -> str:
    return rendering.render_property_source_value(key, _legacy_property_source(source))


def _emit_documentproperty_assignment(
    scope: str, name: str, persist: bool, sourcevalues_xml: str
) -> str:
    return rendering.render_documentproperty_assignment(scope, name, persist, sourcevalues_xml)


def _emit_setproperties_shape(
    shape_name: str,
    properties_xml: str,
    next_name: Optional[str],
    shape_index: int,
    userlabel: str = "",
) -> str:
    ctx = linear_ctx(shape_name, shape_index, [next_name])
    return rendering.render_setproperties_shape(
        ctx, userlabel=userlabel, properties_xml=properties_xml
    )


def _emit_setproperties_step(
    shape_name: str,
    params: Dict[str, Any],
    next_name: Optional[str],
    shape_index: int,
) -> str:
    sources = params.get("source_values") or []
    sourcevalues = "".join(
        _emit_property_source_value(i, src) for i, src in enumerate(sources, start=1)
    )
    prop = _emit_documentproperty_assignment(
        str(params.get("scope") or "ddp"),
        str(params.get("name") or ""),
        bool(params.get("persist", False)),
        sourcevalues,
    )
    return _emit_setproperties_shape(
        shape_name,
        prop,
        next_name,
        shape_index,
        userlabel=str(params.get("userlabel") or ""),
    )


def _emit_setproperties(
    shape_name: str,
    params: Dict[str, Any],
    next_name: Optional[str],
    shape_index: int,
) -> str:
    ddp_name = str(params.get("ddp_name") or "").strip()
    profile_id = str(params.get("request_profile_id") or "").strip()
    profile_type = str(params.get("profile_type") or "profile.json").strip()
    segments = params.get("segments") or []

    sources: List[Dict[str, Any]] = []
    for seg in segments:
        seg_type = str(seg.get("type") or "").strip()
        if seg_type == "static":
            sources.append({"value_type": "static", "value": str(seg.get("value") or "")})
        elif seg_type == "profile":
            sources.append(
                {
                    "value_type": "profile",
                    "element_id": str(seg.get("element_id") or ""),
                    "element_name": str(seg.get("element_name") or ""),
                    "profile_id": profile_id,
                    "profile_type": profile_type,
                }
            )
        elif seg_type == "ddp":
            sources.append(
                {"value_type": "ddp", "property_name": str(seg.get("property_name") or "").strip()}
            )
        elif seg_type == "dpp":
            sources.append(
                {"value_type": "dpp", "property_name": str(seg.get("property_name") or "").strip()}
            )
        else:  # pragma: no cover — _validate_dynamic_path rejects other types
            raise BuilderValidationError(
                f"Unknown dynamic_path segment type {seg_type!r}.",
                error_code="PROCESS_XML_VALIDATION_FAILED",
                field="target.dynamic_path.segments",
                hint="Internal builder bug — please report.",
            )

    sourcevalues = "".join(
        _emit_property_source_value(i, src) for i, src in enumerate(sources, start=1)
    )
    prop = _emit_documentproperty_assignment("ddp", ddp_name, False, sourcevalues)
    return _emit_setproperties_shape(shape_name, prop, next_name, shape_index)


# ---------------------------------------------------------------------------
# Dragpoints helper (linear)
# ---------------------------------------------------------------------------


def _emit_dragpoints(
    next_names: List[Optional[str]], shape_index: int, y: float = _DRAGPOINT_Y
) -> str:
    return rendering.render_dragpoints(linear_transitions(shape_index, next_names, y=y))


# ---------------------------------------------------------------------------
# Branch / Decision leaf shapes
# ---------------------------------------------------------------------------


def _emit_branch(
    shape_name: str, leg_first_names: List[str], shape_index: int, *, userlabel: str = ""
) -> str:
    transitions: Tuple[RenderTransition, ...] = tuple(
        RenderTransition(
            dragpoint_name=f"{shape_name}.dragpoint{i}",
            to_shape_id=to_shape,
            x=_dragpoint_x(shape_index),
            y=_DRAGPOINT_Y,
            identifier=str(i),
            text=str(i),
        )
        for i, to_shape in enumerate(leg_first_names, start=1)
    )
    ctx = ShapeRenderContext(
        shape_id=shape_name, x=_shape_x(shape_index), y=_SHAPE_Y, transitions=transitions
    )
    return rendering.render_branch(
        ctx, userlabel=userlabel, num_branches=len(leg_first_names)
    )


def _legacy_decision_value(operand: Dict[str, Any], field: str) -> RenderDecisionValue:
    value_type = str(operand.get("value_type") or "").strip()
    if value_type == "track":
        property_id = str(operand.get("property_id") or "").strip()
        if not property_id:
            raise BuilderValidationError(
                f"{field}.property_id is required (non-blank) for a track operand.",
                error_code="PROCESS_DECISION_CONFIG_INVALID",
                field=f"{field}.property_id",
                hint="Provide the tracked property id, e.g. 'dynamicdocument.DDP_STATUS'.",
            )
        return RenderDecisionValue(
            value_type="track",
            property_id=property_id,
            default_value=str(operand.get("default_value") or ""),
            property_name=str(operand.get("property_name") or ""),
        )
    if value_type == "static":
        static_value = operand.get("static_value")
        if not isinstance(static_value, str):
            raise BuilderValidationError(
                f"{field}.static_value is required (a string, may be empty) for a static operand.",
                error_code="PROCESS_DECISION_CONFIG_INVALID",
                field=f"{field}.static_value",
                hint="Use an empty string to compare against an empty value (the 'is empty' check).",
            )
        return RenderDecisionValue(value_type="static", static_value=static_value)
    raise BuilderValidationError(
        f"{field}.value_type must be one of {sorted(rendering._DECISION_VALUE_TYPES)}.",
        error_code="PROCESS_DECISION_CONFIG_INVALID",
        field=f"{field}.value_type",
        hint="v1 supports 'track' (a DDP/DPP) and 'static' (a literal value).",
    )


def _emit_decisionvalue(operand: Dict[str, Any], field: str) -> str:
    return rendering.render_decisionvalue(_legacy_decision_value(operand, field))


def _emit_decision(
    shape_name: str,
    decision_config: Dict[str, Any],
    true_to: str,
    false_to: str,
    shape_index: int,
) -> str:
    left = _legacy_decision_value(decision_config.get("left") or {}, "decision.left")
    right = _legacy_decision_value(decision_config.get("right") or {}, "decision.right")
    transitions = (
        RenderTransition(
            dragpoint_name=f"{shape_name}.dragpoint1",
            to_shape_id=true_to,
            x=_dragpoint_x(shape_index),
            y=_DRAGPOINT_Y,
            identifier="true",
            text="True",
        ),
        RenderTransition(
            dragpoint_name=f"{shape_name}.dragpoint2",
            to_shape_id=false_to,
            x=_dragpoint_x(shape_index),
            y=_CATCH_DRAGPOINT_Y,
            identifier="false",
            text="False",
        ),
    )
    ctx = ShapeRenderContext(
        shape_id=shape_name, x=_shape_x(shape_index), y=_SHAPE_Y, transitions=transitions
    )
    return rendering.render_decision(
        ctx,
        label=decision_config.get("label") or "",
        comparison=decision_config.get("comparison") or "",
        left=left,
        right=right,
    )


# ---------------------------------------------------------------------------
# Try/Catch + DLQ + Notify catch-path leaf shapes (legacy-only)
# ---------------------------------------------------------------------------


def _emit_catcherrors(
    shape_name: str, try_to: str, catch_to: str, shape_index: int, retry_count: int = 0
) -> str:
    transitions = (
        RenderTransition(
            dragpoint_name=f"{shape_name}.dragpoint1",
            to_shape_id=try_to,
            x=_dragpoint_x(shape_index),
            y=_DRAGPOINT_Y,
            identifier="default",
            text="Try",
        ),
        RenderTransition(
            dragpoint_name=f"{shape_name}.dragpoint2",
            to_shape_id=catch_to,
            x=_dragpoint_x(shape_index),
            y=_CATCH_DRAGPOINT_Y,
            identifier="error",
            text="Catch",
        ),
    )
    ctx = ShapeRenderContext(
        shape_id=shape_name, x=_shape_x(shape_index), y=_SHAPE_Y, transitions=transitions
    )
    return rendering.render_catcherrors(ctx, retry_count=retry_count)


def _emit_notify(
    shape_name: str,
    catch_notify: Dict[str, Any],
    next_name: Optional[str],
    shape_index: int,
) -> str:
    level = str(catch_notify.get("level") or "").strip().upper()
    template = str(catch_notify.get("message_template") or "")
    message = _escape_message_format_text(template).replace(_NOTIFY_CAUGHT_ERROR_TOKEN, "{1}")
    ctx = ShapeRenderContext(
        shape_id=shape_name,
        x=_shape_x(shape_index),
        y=_CATCH_SHAPE_Y,
        transitions=linear_transitions(shape_index, [next_name], y=_CATCH_DRAGPOINT_Y),
    )
    return rendering.render_notify(ctx, level=level, message=message)


def _emit_exception_parameters(parameter_source: str) -> str:
    return rendering.render_exception_parameters(RenderExceptionBinding(kind=parameter_source))


def _emit_exception(
    shape_name: str,
    catch_exception: Dict[str, Any],
    shape_index: int,
    *,
    y: float = _CATCH_SHAPE_Y,
) -> str:
    template = str(catch_exception.get("message_template") or "")
    message = _escape_message_format_text(template)
    source = str(catch_exception.get("parameter_source") or "caught_error").strip().lower()
    ctx = ShapeRenderContext(
        shape_id=shape_name, x=_shape_x(shape_index), y=y, transitions=()
    )
    return rendering.render_exception(
        ctx,
        title=catch_exception.get("title") or "",
        stop_single_document=bool(catch_exception.get("stop_single_document", False)),
        message=message,
        binding=RenderExceptionBinding(kind=source),
    )


def _emit_doccacheload(
    shape_name: str,
    doc_cache_id: str,
    shape_index: int,
    next_name: Optional[str] = None,
    *,
    y: float = _CATCH_SHAPE_Y,
    dragpoint_y: float = _CATCH_DRAGPOINT_Y,
    userlabel: str = "Route caught errors to DLQ cache",
) -> str:
    transitions = (
        linear_transitions(shape_index, [next_name], y=dragpoint_y) if next_name else ()
    )
    ctx = ShapeRenderContext(
        shape_id=shape_name, x=_shape_x(shape_index), y=y, transitions=transitions
    )
    return rendering.render_doccacheload(ctx, userlabel=userlabel, doc_cache_id=doc_cache_id)


def _emit_processcall(
    shape_name: str,
    process_id: str,
    shape_index: int,
    next_name: Optional[str] = None,
    *,
    wait: bool = True,
    abort: bool = True,
    y: float = _CATCH_SHAPE_Y,
    dragpoint_y: float = _CATCH_DRAGPOINT_Y,
    userlabel: str = "Route caught errors to error subprocess",
) -> str:
    transitions = (
        linear_transitions(shape_index, [next_name], y=dragpoint_y) if next_name else ()
    )
    ctx = ShapeRenderContext(
        shape_id=shape_name, x=_shape_x(shape_index), y=y, transitions=transitions
    )
    return rendering.render_processcall(
        ctx, userlabel=userlabel, process_id=process_id, wait=wait, abort=abort
    )
