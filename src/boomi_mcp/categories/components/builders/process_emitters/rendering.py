"""Byte-exact process-shape renderers (issue #138 M12.3).

The SINGLE copy of every process-shape XML template and the deterministic
layout/dragpoint primitives. Every template literal is transcribed VERBATIM from
the historical ``process_flow_builder`` emitters (whose provenance comments name
the live ``work``-account captures each shape was transcribed from); only the
inputs change — a shape's geometry, identity and outgoing wiring now arrive in a
frozen :class:`ShapeRenderContext` instead of being recomputed from a positional
``shape_index``. Escaping stays INSIDE each renderer exactly as before, so the
raw values a caller passes are escaped in one place and the emitted bytes are
unchanged.

Determinism / byte-parity rules (do not "clean up"):

* Attribute order, quoting, ``x="96.0"`` float ``.0`` rendering, self-closing
  spellings (``<parameters/>``, ``<dragpoints/>``), child order and the absence
  of a trailing newline are all load-bearing — the goldens compare raw bytes.
* Renderers never reserialize through ``ElementTree``; they build strings.
* Renderers are PURE: they raise nothing and validate nothing. The legacy
  adapters (``legacy.py``) keep the historical ``BuilderValidationError``
  bypass-guards; the ProcessIR registry does its own fail-closed preflight.

This module imports nothing from ``process_flow_builder`` (that would close an
import cycle). It owns the emission-domain constants the templates need; the
builder imports them back from here for its validation code.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Dict, Optional, Tuple

from ..connector_builder import WSS_SUBTYPE, _escape_xml

# ---------------------------------------------------------------------------
# Emission-domain constants (moved from process_flow_builder to break the cycle;
# the builder now imports these back from here for its validation code).
# ---------------------------------------------------------------------------

# Issue #113 M10.9 Decision operand sources (v1): 'track' (a DDP/DPP) and
# 'static' (a literal value).
_DECISION_VALUE_TYPES = frozenset({"track", "static"})

# Issue #106 M10.2 / #115 M10.2a Data Process operations. ``processtype``/``name``
# are the exact Boomi step attributes; ``name`` MUST stay the standard operation
# name (a custom step name causes GUI display issues).
_DATAPROCESS_OPERATIONS: Dict[str, Dict[str, str]] = {
    "custom_scripting": {"processtype": "12", "name": "Custom Scripting"},
    "split_documents": {"processtype": "8", "name": "Split Documents"},
    "combine_documents": {"processtype": "9", "name": "Combine Documents"},
}
# The only script engine the typed builder accepts/emits for Custom Scripting.
_DATAPROCESS_SCRIPT_LANGUAGE = "groovy2"
# json -> <JSONOptions>, xml -> <XMLOptions>.
_DATAPROCESS_PROFILE_TYPES = frozenset({"json", "xml"})

# Only the live-verified "Stop document execution (recommended)" wire value.
_DOCCACHE_RETRIEVE_EMPTY_BEHAVIORS = frozenset({"stopprocess"})
_DOCCACHE_RETRIEVE_DEFAULT_EMPTY_BEHAVIOR = "stopprocess"

# Issue #89 Notify / #108 Exception caught-error track binding token.
_NOTIFY_CAUGHT_ERROR_TOKEN = "meta.base.catcherrorsmessage"

# ---------------------------------------------------------------------------
# Visual layout. Geometry is decorative only — process correctness is driven by
# toShape wiring. Numbers approximate the live Renera examples. Transcribed
# verbatim from process_flow_builder; kept here so both callers share one copy.
# ---------------------------------------------------------------------------
_SHAPE_Y = 96.0
_START_SHAPE_X = 96.0
_START_SHAPE_Y = 94.0
_SHAPE_X_STEP = 160.0
_DRAGPOINT_X_OFFSET = 144.0
_DRAGPOINT_Y = 104.0
# Catch-path row sits below the Try row (issue #51 M3.R1a).
_CATCH_SHAPE_Y = 456.0
_CATCH_DRAGPOINT_Y = 464.0


def _shape_x(index: int) -> float:
    # index is 1-based.
    return _START_SHAPE_X + (index - 1) * _SHAPE_X_STEP


def _dragpoint_x(shape_index: int) -> float:
    return _shape_x(shape_index) + _DRAGPOINT_X_OFFSET


# ---------------------------------------------------------------------------
# Neutral render value objects — frozen, slotted, serialization-only. They carry
# only fully-resolved references and counters; never raw legacy config, an
# IntegrationSpecV1/PipelineSpec, or a component-lookup facility.
# ---------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class RenderTransition:
    """One outgoing wire (a ``<dragpoint>``), with its exact resolved fields.

    ``identifier``/``text`` are set only for Branch and Decision (and the
    legacy-only catcherrors), which label their dragpoints; ``None`` for a plain
    linear edge. The legacy adapters compute these from the shape geometry; the
    ProcessIR registry reads them verbatim from an ``EmissionTransitionV1``.
    """

    dragpoint_name: str
    to_shape_id: str
    x: float
    y: float
    identifier: Optional[str] = None
    text: Optional[str] = None


@dataclass(frozen=True, slots=True)
class ShapeRenderContext:
    """A shape's identity, geometry and ordered outgoing wiring.

    Carries no raw config and no lookup facility — only the resolved shape id,
    coordinates and transitions. Every renderer takes one of these plus the
    shape-specific resolved fields as explicit keyword arguments.
    """

    shape_id: str
    x: float
    y: float
    transitions: Tuple[RenderTransition, ...] = ()


@dataclass(frozen=True, slots=True)
class RenderDataProcessStep:
    """One resolved Data Process ``<step>`` (issue #106/#115)."""

    operation: str
    index: int
    key: int
    name: str
    processtype: str
    # custom_scripting
    script: str = ""
    language: str = _DATAPROCESS_SCRIPT_LANGUAGE
    use_cache: bool = True
    # split_documents / combine_documents
    profile_type: str = ""
    profile_id: str = ""
    link_element_key: str = ""
    link_element_name: str = ""
    combine_into_link_element_key: str = "null"


@dataclass(frozen=True, slots=True)
class RenderPropertySource:
    """One resolved ``<parametervalue>`` source for a property assignment (#121)."""

    value_type: str
    value: str = ""
    element_id: str = ""
    element_name: str = ""
    profile_id: str = ""
    profile_type: str = "profile.json"
    property_name: str = ""
    default_value: str = ""
    process_property: str = ""


@dataclass(frozen=True, slots=True)
class RenderDecisionValue:
    """A resolved Decision operand (issue #113)."""

    value_type: str
    property_id: str = ""
    default_value: str = ""
    property_name: str = ""
    static_value: str = ""


@dataclass(frozen=True, slots=True)
class RenderExceptionBinding:
    """A resolved Exception ``<exParameters>`` binding (issue #108).

    ``kind`` is one of ``none`` / ``current_document`` / ``caught_error``.
    """

    kind: str


# ---------------------------------------------------------------------------
# Dragpoint serialization (single copy of the two attribute-order forms).
# ---------------------------------------------------------------------------


def render_dragpoints(transitions: Tuple[RenderTransition, ...]) -> str:
    """Serialize the inner ``<dragpoint .../>`` children for a shape.

    Two byte-exact forms, matching the historical emitters:

    * labeled (``identifier``/``text`` set — Branch, Decision, catcherrors):
      attribute order ``identifier name text toShape x y``;
    * plain (linear edge): attribute order ``name toShape x y``.

    ``dragpoint_name``/``identifier``/``text`` are generated safe values and are
    emitted unescaped exactly as the historical inline emitters did; ``toShape``
    is XML-escaped.
    """
    parts = []
    for t in transitions:
        if t.identifier is not None:
            parts.append(
                f'<dragpoint identifier="{t.identifier}" name="{t.dragpoint_name}" '
                f'text="{t.text}" toShape="{_escape_xml(t.to_shape_id)}" '
                f'x="{t.x}" y="{t.y}"/>'
            )
        else:
            parts.append(
                f'<dragpoint name="{t.dragpoint_name}" '
                f'toShape="{_escape_xml(t.to_shape_id)}" '
                f'x="{t.x}" y="{t.y}"/>'
            )
    return "".join(parts)


def _dragpoints_block(transitions: Tuple[RenderTransition, ...]) -> str:
    """The conditional block form used by doccacheload / processcall: an empty
    ``<dragpoints/>`` when terminal (no transitions), else the wrapped children.
    """
    if not transitions:
        return "<dragpoints/>"
    return f"<dragpoints>{render_dragpoints(transitions)}</dragpoints>"


# ---------------------------------------------------------------------------
# Layout transition builders (legacy geometry path). The ProcessIR registry does
# NOT use these — it reads coordinates verbatim from the emission plan.
# ---------------------------------------------------------------------------


def linear_transitions(
    shape_index: int, next_names, y: float = _DRAGPOINT_Y
) -> Tuple[RenderTransition, ...]:
    """Build the plain (unlabeled) transitions for a linear shape, matching the
    historical ``_emit_dragpoints``: one dragpoint per non-None next name, named
    ``shape{index}.dragpoint{N}`` at ``_dragpoint_x(index)``.
    """
    out = []
    point_index = 0
    for to_shape in next_names:
        if to_shape is None:
            continue
        point_index += 1
        out.append(
            RenderTransition(
                dragpoint_name=f"shape{shape_index}.dragpoint{point_index}",
                to_shape_id=to_shape,
                x=_dragpoint_x(shape_index),
                y=y,
            )
        )
    return tuple(out)


def linear_ctx(
    shape_name: str,
    shape_index: int,
    next_names,
    *,
    x: Optional[float] = None,
    y: float = _SHAPE_Y,
    dragpoint_y: float = _DRAGPOINT_Y,
) -> ShapeRenderContext:
    """Build a ``ShapeRenderContext`` for a linear legacy shape."""
    return ShapeRenderContext(
        shape_id=shape_name,
        x=_shape_x(shape_index) if x is None else x,
        y=y,
        transitions=linear_transitions(shape_index, next_names, y=dragpoint_y),
    )


# ---------------------------------------------------------------------------
# Message MessageFormat helpers (issue #102 C3) — moved verbatim.
# ---------------------------------------------------------------------------


def _looks_like_json(text: str) -> bool:
    """True when ``text`` is a JSON object/array literal."""
    stripped = text.strip()
    if not stripped or stripped[0] not in "{[":
        return False
    try:
        parsed = json.loads(stripped)
    except (ValueError, TypeError):
        return False
    return isinstance(parsed, (dict, list))


def _escape_message_format_text(text: str) -> str:
    """Escape free text for a Boomi Message/Notify MessageFormat field (#102 C3).

    Doubles every apostrophe and, when the body is a JSON object/array, wraps the
    doubled result in single quotes so its ``{ }`` braces are not read as ``{N}``
    variable placeholders. Emission owns this escaping (transcribed verbatim).
    """
    doubled = text.replace("'", "''")
    if _looks_like_json(text):
        return f"'{doubled}'"
    return doubled


# ---------------------------------------------------------------------------
# Shape renderers (one per produced Boomi shape form). Bodies are byte-identical
# to the historical ``_emit_*`` templates.
# ---------------------------------------------------------------------------


def render_start_noaction(ctx: ShapeRenderContext) -> str:
    dragpoints = render_dragpoints(ctx.transitions)
    return (
        f'<shape image="start" name="{ctx.shape_id}" shapetype="start" '
        f'userlabel="" x="{ctx.x}" y="{ctx.y}">'
        '<configuration><noaction/></configuration>'
        f'<dragpoints>{dragpoints}</dragpoints>'
        '</shape>'
    )


def render_start_listen(ctx: ShapeRenderContext, *, userlabel: str, operation_id: str) -> str:
    """WSS Listen start shape (M6, #12) — legacy-only (no registry emitter_kind)."""
    dragpoints = render_dragpoints(ctx.transitions)
    userlabel = _escape_xml(userlabel or "")
    operation_id = _escape_xml(str(operation_id or "").strip())
    return (
        f'<shape image="start" name="{ctx.shape_id}" shapetype="start" '
        f'userlabel="{userlabel}" x="{ctx.x}" y="{ctx.y}">'
        '<configuration>'
        '<connectoraction actionType="Listen" '
        'allowDynamicCredentials="NONE" '
        f'connectorType="{WSS_SUBTYPE}" '
        'hideSettings="true" '
        f'operationId="{operation_id}">'
        '<parameters/>'
        '</connectoraction>'
        '</configuration>'
        f'<dragpoints>{dragpoints}</dragpoints>'
        '</shape>'
    )


def render_connectoraction(
    ctx: ShapeRenderContext,
    *,
    userlabel: str,
    connector_type: str,
    action_type: str,
    connection_id: str,
    operation_id: str,
    inner: str = '<parameters/><dynamicProperties/>',
    parameter_profile_attr: str = '',
) -> str:
    """Mid-flow connectoraction shape.

    ``inner`` / ``parameter_profile_attr`` are pre-formed XML supplied by the
    caller: the simple form (the ProcessIR registry) uses the defaults; the
    legacy dynamic-path branch (issue #100 G2) supplies the ``<dynamicProperties>``
    body and the ``parameter-profile`` attribute.
    """
    dragpoints = render_dragpoints(ctx.transitions)
    userlabel = _escape_xml(userlabel or "")
    connector_type = _escape_xml(connector_type)
    action_type = _escape_xml(action_type)
    connection_id = _escape_xml(connection_id)
    operation_id = _escape_xml(operation_id)
    return (
        f'<shape image="connectoraction_icon" name="{ctx.shape_id}" '
        f'shapetype="connectoraction" userlabel="{userlabel}" '
        f'x="{ctx.x}" y="{ctx.y}">'
        '<configuration>'
        f'<connectoraction actionType="{action_type}" '
        'allowDynamicCredentials="NONE" '
        f'connectionId="{connection_id}" '
        f'connectorType="{connector_type}" '
        'hideSettings="false" '
        f'operationId="{operation_id}"{parameter_profile_attr}>'
        f'{inner}'
        '</connectoraction>'
        '</configuration>'
        f'<dragpoints>{dragpoints}</dragpoints>'
        '</shape>'
    )


def render_message(ctx: ShapeRenderContext, *, userlabel: str, text: str) -> str:
    dragpoints = render_dragpoints(ctx.transitions)
    userlabel = _escape_xml(userlabel or "")
    text = _escape_xml(_escape_message_format_text(text or ""))
    return (
        f'<shape image="message_icon" name="{ctx.shape_id}" shapetype="message" '
        f'userlabel="{userlabel}" x="{ctx.x}" y="{ctx.y}">'
        '<configuration>'
        '<message combined="false">'
        f'<msgTxt>{text}</msgTxt>'
        '<msgParameters/>'
        '</message>'
        '</configuration>'
        f'<dragpoints>{dragpoints}</dragpoints>'
        '</shape>'
    )


def render_map(ctx: ShapeRenderContext, *, userlabel: str, map_id: str) -> str:
    dragpoints = render_dragpoints(ctx.transitions)
    userlabel = _escape_xml(userlabel or "")
    map_id = _escape_xml(map_id or "")
    return (
        f'<shape image="map_icon" name="{ctx.shape_id}" shapetype="map" '
        f'userlabel="{userlabel}" x="{ctx.x}" y="{ctx.y}">'
        '<configuration>'
        f'<map mapId="{map_id}"/>'
        '</configuration>'
        f'<dragpoints>{dragpoints}</dragpoints>'
        '</shape>'
    )


def render_flowcontrol(ctx: ShapeRenderContext, *, userlabel: str, for_each_count: int) -> str:
    userlabel = _escape_xml(userlabel or "")
    dragpoints = render_dragpoints(ctx.transitions)
    return (
        f'<shape image="flowcontrol_icon" name="{ctx.shape_id}" '
        f'shapetype="flowcontrol" userlabel="{userlabel}" '
        f'x="{ctx.x}" y="{ctx.y}">'
        '<configuration>'
        f'<flowcontrol chunkStyle="threadOnly" chunks="0" forEachCount="{for_each_count}"/>'
        '</configuration>'
        f'<dragpoints>{dragpoints}</dragpoints>'
        '</shape>'
    )


def render_dataprocess_step(step: RenderDataProcessStep) -> str:
    """Render one Data Process ``<step>`` (issue #106 / #115 M10.2a)."""
    open_tag = (
        f'<step index="{step.index}" key="{step.key}" '
        f'name="{step.name}" processtype="{step.processtype}">'
    )
    if step.operation == "custom_scripting":
        script = _escape_xml(step.script or "")
        use_cache = "true" if step.use_cache else "false"
        body = (
            f'<dataprocessscript language="{step.language}" useCache="{use_cache}">'
            f'<script>{script}</script>'
            '</dataprocessscript>'
        )
    elif step.operation == "split_documents":
        body = _render_dataprocess_split_body(step)
    else:  # combine_documents
        body = _render_dataprocess_combine_body(step)
    return f"{open_tag}{body}</step>"


def _dataprocess_option_tag(profile_type: str) -> str:
    return "JSONOptions" if profile_type == "json" else "XMLOptions"


def _render_dataprocess_split_body(step: RenderDataProcessStep) -> str:
    tag = _dataprocess_option_tag(str(step.profile_type or "").strip().lower())
    profile_type = _escape_xml(step.profile_type or "")
    link_key = _escape_xml(step.link_element_key or "")
    link_name = _escape_xml(step.link_element_name or "")
    profile_id = _escape_xml(step.profile_id or "")
    return (
        f'<documentsplit profileType="{profile_type}"><SplitOptions>'
        f'<{tag} linkElementKey="{link_key}" linkElementName="{link_name}" '
        f'profileId="{profile_id}"/>'
        '</SplitOptions></documentsplit>'
    )


def _render_dataprocess_combine_body(step: RenderDataProcessStep) -> str:
    tag = _dataprocess_option_tag(str(step.profile_type or "").strip().lower())
    profile_type = _escape_xml(step.profile_type or "")
    combine_into = _escape_xml(step.combine_into_link_element_key or "null")
    link_key = _escape_xml(step.link_element_key or "")
    link_name = _escape_xml(step.link_element_name or "")
    profile_id = _escape_xml(step.profile_id or "")
    return (
        f'<dataprocesscombine profileType="{profile_type}">'
        f'<{tag} combineIntoLinkElementKey="{combine_into}" '
        f'linkElementKey="{link_key}" linkElementName="{link_name}" '
        f'profileId="{profile_id}"/>'
        '</dataprocesscombine>'
    )


def render_dataprocess(
    ctx: ShapeRenderContext, *, userlabel: str, steps: Tuple[RenderDataProcessStep, ...]
) -> str:
    dragpoints = render_dragpoints(ctx.transitions)
    userlabel = _escape_xml(userlabel or "")
    step_parts = [render_dataprocess_step(step) for step in steps]
    return (
        f'<shape image="dataprocess_icon" name="{ctx.shape_id}" '
        f'shapetype="dataprocess" userlabel="{userlabel}" '
        f'x="{ctx.x}" y="{ctx.y}">'
        '<configuration>'
        f'<dataprocess>{"".join(step_parts)}</dataprocess>'
        '</configuration>'
        f'<dragpoints>{dragpoints}</dragpoints>'
        '</shape>'
    )


def render_doccacheretrieve(
    ctx: ShapeRenderContext, *, userlabel: str, doc_cache_id: str, empty_cache_behavior: str
) -> str:
    dragpoints = render_dragpoints(ctx.transitions)
    userlabel = _escape_xml(userlabel or "")
    doc_cache_id = _escape_xml(str(doc_cache_id or "").strip())
    empty_cache_behavior_xml = _escape_xml(empty_cache_behavior)
    return (
        f'<shape image="doccacheretrieve_icon" name="{ctx.shape_id}" '
        f'shapetype="doccacheretrieve" userlabel="{userlabel}" '
        f'x="{ctx.x}" y="{ctx.y}">'
        '<configuration>'
        f'<doccacheretrieve docCache="{doc_cache_id}" '
        f'emptyCacheBehavior="{empty_cache_behavior_xml}" loadAllDoc="true">'
        '<cacheKeyValues/>'
        '</doccacheretrieve>'
        '</configuration>'
        f'<dragpoints>{dragpoints}</dragpoints>'
        '</shape>'
    )


def render_doccacheremove(
    ctx: ShapeRenderContext, *, userlabel: str, doc_cache_id: str
) -> str:
    dragpoints = render_dragpoints(ctx.transitions)
    userlabel = _escape_xml(userlabel or "")
    doc_cache_id = _escape_xml(str(doc_cache_id or "").strip())
    return (
        f'<shape image="doccacheremove_icon" name="{ctx.shape_id}" '
        f'shapetype="doccacheremove" userlabel="{userlabel}" '
        f'x="{ctx.x}" y="{ctx.y}">'
        '<configuration>'
        f'<doccacheremove docCache="{doc_cache_id}" removeAllDocuments="true">'
        '<cacheKeyValues/>'
        '</doccacheremove>'
        '</configuration>'
        f'<dragpoints>{dragpoints}</dragpoints>'
        '</shape>'
    )


def render_returndocuments(ctx: ShapeRenderContext, *, label: str) -> str:
    """Terminal Return Documents shape (issue #107). ``label`` maps to BOTH the
    shape ``userlabel`` and the inner ``<returndocuments label>``."""
    label = _escape_xml(str(label or ""))
    return (
        f'<shape image="returndocuments_icon" name="{ctx.shape_id}" '
        f'shapetype="returndocuments" userlabel="{label}" '
        f'x="{ctx.x}" y="{ctx.y}">'
        '<configuration>'
        f'<returndocuments label="{label}"/>'
        '</configuration>'
        '<dragpoints/>'
        '</shape>'
    )


def render_property_source_value(key: int, source: RenderPropertySource) -> str:
    """Emit one ``<parametervalue>`` for a property assignment (issue #121)."""
    value_type = source.value_type
    if value_type == "static":
        value = _escape_xml(source.value or "")
        return (
            f'<parametervalue key="{key}" usesEncryption="false" valueType="static">'
            f'<staticparameter staticproperty="{value}"/>'
            '</parametervalue>'
        )
    if value_type == "current":
        return f'<parametervalue key="{key}" usesEncryption="false" valueType="current"/>'
    if value_type == "profile":
        element_id = _escape_xml(source.element_id or "")
        element_name = _escape_xml(source.element_name or "")
        profile_id = _escape_xml(str(source.profile_id or "").strip())
        profile_type = _escape_xml(str(source.profile_type or "profile.json").strip())
        return (
            f'<parametervalue key="{key}" usesEncryption="false" valueType="profile">'
            f'<profileelement elementId="{element_id}" '
            f'elementName="{element_name}" '
            f'profileId="{profile_id}" profileType="{profile_type}"/>'
            '</parametervalue>'
        )
    if value_type == "ddp":
        name = _escape_xml(str(source.property_name or "").strip())
        default = _escape_xml(source.default_value or "")
        return (
            f'<parametervalue key="{key}" usesEncryption="false" valueType="track">'
            f'<trackparameter defaultValue="{default}" propertyId="dynamicdocument.{name}" '
            f'propertyName="Dynamic Document Property - {name}"/>'
            '</parametervalue>'
        )
    # dpp
    name = _escape_xml(str(source.property_name or "").strip())
    default = _escape_xml(source.default_value or "")
    return (
        f'<parametervalue key="{key}" usesEncryption="false" valueType="process">'
        f'<processparameter processproperty="{name}" processpropertydefaultvalue="{default}"/>'
        '</parametervalue>'
    )


def render_documentproperty_assignment(
    scope: str, name: str, persist: bool, sourcevalues_xml: str
) -> str:
    """Emit one ``<documentproperty>`` assignment (issue #121)."""
    esc = _escape_xml(str(name or "").strip())
    if scope == "ddp":
        display = f"Dynamic Document Property - {esc}"
        property_id = f"dynamicdocument.{esc}"
        persist_text = "false"
    else:
        display = f"Dynamic Process Property - {esc}"
        property_id = f"process.{esc}"
        persist_text = "true" if persist else "false"
    return (
        '<documentproperty defaultValue="" isDynamicCredential="false" '
        f'isTradingPartner="false" name="{display}" '
        f'persist="{persist_text}" propertyId="{property_id}" shouldEncrypt="false">'
        f'<sourcevalues>{sourcevalues_xml}</sourcevalues>'
        '</documentproperty>'
    )


def render_setproperties_shape(ctx: ShapeRenderContext, *, userlabel: str, properties_xml: str) -> str:
    """Emit the ``documentproperties`` shape wrapper around assignment(s)."""
    dragpoints = render_dragpoints(ctx.transitions)
    return (
        f'<shape image="documentproperties_icon" name="{ctx.shape_id}" '
        f'shapetype="documentproperties" userlabel="{_escape_xml(userlabel)}" '
        f'x="{ctx.x}" y="{ctx.y}">'
        f'<configuration><documentproperties>{properties_xml}'
        '</documentproperties></configuration>'
        f'<dragpoints>{dragpoints}</dragpoints>'
        '</shape>'
    )


def render_stop(ctx: ShapeRenderContext, *, continue_: bool) -> str:
    cont = "true" if continue_ else "false"
    return (
        f'<shape image="stop_icon" name="{ctx.shape_id}" shapetype="stop" '
        f'x="{ctx.x}" y="{ctx.y}">'
        f'<configuration><stop continue="{cont}"/></configuration>'
        '<dragpoints/>'
        '</shape>'
    )


def render_branch(ctx: ShapeRenderContext, *, userlabel: str, num_branches: int) -> str:
    """Branch (N-way forward fan-out) shape (issue #112). Dragpoints are the
    labeled transitions in ``ctx``."""
    dragpoints = render_dragpoints(ctx.transitions)
    return (
        f'<shape image="branch_icon" name="{ctx.shape_id}" shapetype="branch" '
        f'userlabel="{_escape_xml(userlabel)}" x="{ctx.x}" y="{ctx.y}">'
        f'<configuration><branch numBranches="{num_branches}"/></configuration>'
        f'<dragpoints>{dragpoints}</dragpoints>'
        '</shape>'
    )


def render_decisionvalue(operand: RenderDecisionValue) -> str:
    """Emit one ``<decisionvalue>`` operand (issue #113)."""
    if operand.value_type == "track":
        default_value = _escape_xml(operand.default_value or "")
        property_name = _escape_xml(operand.property_name or "")
        return (
            '<decisionvalue valueType="track">'
            f'<trackparameter defaultValue="{default_value}" '
            f'propertyId="{_escape_xml(operand.property_id)}" '
            f'propertyName="{property_name}"/>'
            '</decisionvalue>'
        )
    # static
    return (
        '<decisionvalue valueType="static">'
        f'<staticparameter staticproperty="{_escape_xml(operand.static_value)}"/>'
        '</decisionvalue>'
    )


def render_decision(
    ctx: ShapeRenderContext,
    *,
    label: str,
    comparison: str,
    left: RenderDecisionValue,
    right: RenderDecisionValue,
) -> str:
    """Decision (conditional two-path routing) shape (issue #113). The two
    labeled dragpoints (true/false) are the transitions in ``ctx``."""
    label = _escape_xml(str(label or ""))
    comparison = _escape_xml(str(comparison or "").strip())
    left_xml = render_decisionvalue(left)
    right_xml = render_decisionvalue(right)
    dragpoints = render_dragpoints(ctx.transitions)
    return (
        f'<shape image="decision_icon" name="{ctx.shape_id}" shapetype="decision" '
        f'userlabel="{label}" x="{ctx.x}" y="{ctx.y}">'
        f'<configuration><decision comparison="{comparison}" name="{label}">'
        f'{left_xml}{right_xml}</decision></configuration>'
        f'<dragpoints>{dragpoints}</dragpoints>'
        '</shape>'
    )


def render_catcherrors(ctx: ShapeRenderContext, *, retry_count: int = 0) -> str:
    """Verified catcherrors Try/Catch shape (legacy-only). The Try/Catch labeled
    dragpoints are the transitions in ``ctx``."""
    retry_label = "no retry" if retry_count == 0 else f"retry {retry_count}"
    dragpoints = render_dragpoints(ctx.transitions)
    return (
        f'<shape image="catcherrors_icon" name="{ctx.shape_id}" '
        f'shapetype="catcherrors" '
        f'userlabel="Try/Catch all errors ({retry_label}) - route caught documents to the failure handler" '
        f'x="{ctx.x}" y="{ctx.y}">'
        f'<configuration><catcherrors catchAll="true" retryCount="{retry_count}"/></configuration>'
        f'<dragpoints>{dragpoints}</dragpoints>'
        '</shape>'
    )


def render_notify(ctx: ShapeRenderContext, *, level: str, message: str) -> str:
    """Verified Notify catch-leg step (legacy-only, issue #89).

    ``message`` is the already MessageFormat-escaped-and-{1}-substituted body
    (the caller owns that transform, which references the shared MessageFormat
    escaper); this renderer XML-escapes it and wraps the fixed template.
    """
    dragpoints = render_dragpoints(ctx.transitions)
    return (
        f'<shape image="notify_icon" name="{ctx.shape_id}" shapetype="notify" '
        f'userlabel="Notify caught error to the process log" '
        f'x="{ctx.x}" y="{ctx.y}">'
        '<configuration>'
        '<notify disableEvent="true" enableUserLog="false" perExecution="false" '
        'title="Catch path notification">'
        f'<notifyMessage>{_escape_xml(message)}</notifyMessage>'
        f'<notifyMessageLevel>{_escape_xml(level)}</notifyMessageLevel>'
        '<notifyParameters>'
        '<parametervalue key="0" valueType="track">'
        f'<trackparameter defaultValue="" propertyId="{_escape_xml(_NOTIFY_CAUGHT_ERROR_TOKEN)}" '
        'propertyName="Base - Try/Catch Message"/>'
        '</parametervalue>'
        '</notifyParameters>'
        '</notify>'
        '</configuration>'
        f'<dragpoints>{dragpoints}</dragpoints>'
        '</shape>'
    )


def render_exception_parameters(binding: RenderExceptionBinding) -> str:
    """Emit the ``<exParameters>`` binding for the single ``{1}`` placeholder
    (issue #108). ``none`` emits nothing; ``current_document`` binds the current
    document; ``caught_error`` binds the Try/Catch message token."""
    if binding.kind == "none":
        return ""
    if binding.kind == "current_document":
        return '<exParameters><parametervalue key="0" valueType="current"/></exParameters>'
    return (
        '<exParameters>'
        '<parametervalue key="0" valueType="track">'
        f'<trackparameter defaultValue="" propertyId="{_escape_xml(_NOTIFY_CAUGHT_ERROR_TOKEN)}" '
        'propertyName="Base - Try/Catch Message"/>'
        '</parametervalue>'
        '</exParameters>'
    )


def render_exception(
    ctx: ShapeRenderContext,
    *,
    title: str,
    stop_single_document: bool,
    message: str,
    binding: RenderExceptionBinding,
) -> str:
    """Deliberate Exception (Throw) terminal shape (issue #108).

    ``message`` is the already MessageFormat-escaped body; this renderer
    XML-escapes it. TERMINAL — empty ``<dragpoints/>``."""
    title_attr = _escape_xml(str(title or ""))
    stop_single = str(bool(stop_single_document)).lower()
    message = _escape_xml(message)
    return (
        f'<shape image="exception_icon" name="{ctx.shape_id}" shapetype="exception" '
        f'userlabel="{title_attr}" x="{ctx.x}" y="{ctx.y}">'
        '<configuration>'
        f'<exception stopProcessReturnSingleDoc="false" stopsingledoc="{stop_single}" '
        f'title="{title_attr}">'
        f'<exMessage>{message}</exMessage>'
        f'{render_exception_parameters(binding)}'
        '</exception>'
        '</configuration>'
        '<dragpoints/>'
        '</shape>'
    )


def render_doccacheload(ctx: ShapeRenderContext, *, userlabel: str, doc_cache_id: str) -> str:
    """Verified document-cache Add-to-Cache shape. Terminal (empty dragpoints) or
    forward, from ``ctx.transitions``."""
    dragpoints_xml = _dragpoints_block(ctx.transitions)
    return (
        f'<shape image="doccacheload_icon" name="{ctx.shape_id}" '
        f'shapetype="doccacheload" userlabel="{_escape_xml(userlabel)}" '
        f'x="{ctx.x}" y="{ctx.y}">'
        f'<configuration><doccacheload docCache="{_escape_xml(doc_cache_id)}"/></configuration>'
        f'{dragpoints_xml}'
        '</shape>'
    )


def render_processcall(
    ctx: ShapeRenderContext, *, userlabel: str, process_id: str, wait: bool, abort: bool
) -> str:
    """Verified ``processcall`` shape. Terminal (empty dragpoints) or forward,
    from ``ctx.transitions``."""
    wait_s = "true" if wait else "false"
    abort_s = "true" if abort else "false"
    dragpoints_xml = _dragpoints_block(ctx.transitions)
    return (
        f'<shape image="processcall_icon" name="{ctx.shape_id}" '
        f'shapetype="processcall" userlabel="{_escape_xml(userlabel)}" '
        f'x="{ctx.x}" y="{ctx.y}">'
        '<configuration>'
        f'<processcall abort="{abort_s}" processId="{_escape_xml(process_id)}" wait="{wait_s}">'
        '<parameters/><returnpaths/>'
        '</processcall>'
        '</configuration>'
        f'{dragpoints_xml}'
        '</shape>'
    )
