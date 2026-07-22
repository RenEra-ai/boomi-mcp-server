"""Typed ProcessIR process-emitter registry (issue #138 M12.3).

The first — and, in #138, ONLY — consumer of ``EmissionPlanV1``. Given a compiled
emission plan and its resolved symbol table, :func:`emit_process` turns the plan
into process XML by dispatching each planned shape to a typed emitter keyed by
the closed ``emitter_kind`` discriminator. The emitters reuse the SAME byte
serializers the legacy builder uses (``process_emitters.rendering``), so the
registry and the legacy path emit byte-identical XML for the same logical shape.

DARK / test-only in #138: no MCP tool, no production builder, and no schema
constructs or consumes this. It stays out of ``process_ir.__all__`` and is
imported directly. #139 owns the production adapter/cutover.

Design invariants:

* The registry NEVER recomputes geometry — shape ids, coordinates, dragpoints and
  synthetic nodes are consumed verbatim from ``EmissionPlanV1``. Emission order is
  ``EmissionPlanV1.nodes`` order; the registry mapping order is irrelevant.
* Emitters receive only a typed ``EmitterInputV1`` member and an
  :class:`EmitterContext` (the plan node + the symbols it declared it needs) —
  never a raw ``IntegrationSpecV1``/``PipelineSpec``/legacy builder config, and no
  facility to mutate anything.
* Whole-plan fail-closed: every node is pre-flighted (registration, typed input,
  renderer preconditions, outgoing cardinality, symbol requirements) BEFORE any
  XML is produced. A single failing node aborts the whole plan with a
  ``ProcessIRCompileError`` carrying value-free ``PROCESS_IR_COMPILE_*``
  diagnostics; no partial output escapes.
"""

from __future__ import annotations

import xml.etree.ElementTree as ET
from dataclasses import dataclass
from types import MappingProxyType
from typing import Callable, Dict, List, Mapping, Optional, Tuple

from pydantic import BaseModel, TypeAdapter

from ...categories.components.builders.process_emitters import rendering
from ...categories.components.builders.process_emitters.rendering import (
    RenderDataProcessStep,
    RenderDecisionValue,
    RenderExceptionBinding,
    RenderPropertySource,
    RenderTransition,
    ShapeRenderContext,
)
from ...categories.components.process_graph_verifier import verify_process_graph
from ...errors import (
    PROCESS_IR_COMPILE_EMITTER_INPUT_INVALID,
    PROCESS_IR_COMPILE_EMITTER_MISSING,
    PROCESS_IR_COMPILE_INTERNAL,
    PROCESS_IR_COMPILE_SYMBOL_UNRESOLVED,
    PROCESS_IR_COMPILE_VERIFIER_FAILED,
    PROCESS_IR_COMPILE_XML_INVALID,
)
from .contracts import (
    BranchInputV1,
    ConnectorActionInputV1,
    DataProcessInputV1,
    DecisionInputV1,
    DocCacheLoadInputV1,
    DocCacheRemoveInputV1,
    DocCacheRetrieveInputV1,
    EmissionNodeV1,
    EmissionPlanV1,
    EmitterInputV1,
    ExceptionInputV1,
    FlowControlInputV1,
    MapInputV1,
    MessageInputV1,
    ProcessCallInputV1,
    ReturnDocumentsInputV1,
    SetPropertiesStepInputV1,
    StartNoActionInputV1,
    StopInputV1,
    SymbolTableV1,
)
from .diagnostics import CompilerDiagnostic, ProcessIRCompileError, diagnostic

# The single capability level #138 ships. A registration declares which level it
# supports; a plan compiled above that level fails closed (EMITTER_MISSING).
CapabilityLevel = str
CAPABILITY_PROCESS_IR_V1: CapabilityLevel = "process_ir_v1"

# Wire prefixes the renderers derive for Set-Properties (mirrors the #137 lowering
# and the legacy builder). Used to recover the bare property name from the
# fully-resolved typed ``property_id`` so the shared renderer reproduces it.
_DDP_PROPERTY_PREFIX = "dynamicdocument."
_DPP_PROPERTY_PREFIX = "process."

# Boomi component types the resolved symbols must carry for each reference slot.
_CONNECTOR_SETTINGS_TYPES = ("connector-settings",)
_CONNECTOR_ACTION_TYPES = ("connector-action",)
_MAP_TYPES = ("transform.map",)
_CACHE_TYPES = ("documentcache",)
_PROCESS_TYPES = ("process",)
# A Data Process split/combine step declares its profile KIND (``json``/``xml``);
# the resolved profile symbol must carry the matching Boomi component type. A
# Set-Properties profile source already declares the full component type
# (``profile.json``/``profile.xml``/``profile.db``), so it is required verbatim.
# Requiring the SPECIFIC type (not the whole family) keeps the fail-closed check
# from accepting a json-declared step backed only by an xml profile — a mismatch
# the topology-only graph verifier cannot catch (#138 review).
_DP_PROFILE_COMPONENT_TYPE = {"json": "profile.json", "xml": "profile.xml"}
_SETPROP_PROFILE_TYPES = ("profile.db", "profile.json", "profile.xml")


# ---------------------------------------------------------------------------
# Frozen registry / artifact value objects
# ---------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class SymbolRequirement:
    """One component the node needs resolved, and the types that satisfy it."""

    slot: str
    component_id: str
    component_types: Tuple[str, ...]


@dataclass(frozen=True, slots=True)
class OutgoingCardinality:
    """Allowed outgoing-edge count for a node kind.

    ``kind`` is ``exact`` (``value`` edges), ``branch`` (``num_branches`` edges),
    or ``zero_or_one`` (a terminal-or-continuing cache-load).
    """

    kind: str
    value: int = 0


EXACT_ONE = OutgoingCardinality("exact", 1)
EXACT_ZERO = OutgoingCardinality("exact", 0)
EXACT_TWO = OutgoingCardinality("exact", 2)
BRANCH = OutgoingCardinality("branch")
ZERO_OR_ONE = OutgoingCardinality("zero_or_one")


@dataclass(frozen=True, slots=True)
class EmitterContext:
    """The resolved inputs an emitter is allowed to see: the plan node and the
    symbols it declared it needs. No raw config, no lookup facility."""

    node: EmissionNodeV1
    resolved_symbols: Tuple[object, ...]
    capability_level: CapabilityLevel


@dataclass(frozen=True, slots=True)
class EmitterRegistration:
    """One typed emitter. The ``emit`` callable takes only a typed
    ``EmitterInputV1`` member and an :class:`EmitterContext`."""

    emitter_kind: str
    input_type: type
    produced_shape_type: str
    supported_capability: CapabilityLevel
    outgoing: OutgoingCardinality
    requirements: Callable[[BaseModel], Tuple[SymbolRequirement, ...]]
    emit: Callable[[BaseModel, "EmitterContext"], str]
    precondition: Optional[Callable[[BaseModel], Optional[str]]] = None


@dataclass(frozen=True, slots=True)
class VerifierIssueV1:
    code: str
    shape: str
    shape_type: str
    message: str
    remediation: str


@dataclass(frozen=True, slots=True)
class ProcessVerifierSummaryV1:
    errors: Tuple[VerifierIssueV1, ...]
    warnings: Tuple[VerifierIssueV1, ...]
    shapes_checked: int


@dataclass(frozen=True, slots=True)
class ProcessEmissionArtifactV1:
    """The verified internal process artifact: the ordered shape XML parts, the
    minimal ``<process>`` wrapper they were verified in, and the verifier result.

    NOT a deployable Boomi Component envelope — #139 owns wrapping this for apply.
    """

    shape_xml_parts: Tuple[str, ...]
    process_xml: str
    verifier: ProcessVerifierSummaryV1


# ---------------------------------------------------------------------------
# Typed-input -> neutral render value object translation
# ---------------------------------------------------------------------------


def _shape_context(node: EmissionNodeV1) -> ShapeRenderContext:
    """Build a render context from the plan node — geometry and wiring VERBATIM."""
    return ShapeRenderContext(
        shape_id=node.shape_id,
        x=node.layout.x,
        y=node.layout.y,
        transitions=tuple(
            RenderTransition(
                dragpoint_name=t.dragpoint_name,
                to_shape_id=t.to_shape_id,
                x=t.x,
                y=t.y,
                identifier=t.identifier,
                text=t.text,
            )
            for t in node.outgoing
        ),
    )


def _render_step(step: BaseModel) -> RenderDataProcessStep:
    op = step.operation
    if op == "custom_scripting":
        return RenderDataProcessStep(
            operation=op,
            index=step.index,
            key=step.key,
            name=step.name,
            processtype=step.processtype,
            script=step.script,
            language=step.language,
            use_cache=step.use_cache,
        )
    common = dict(
        operation=op,
        index=step.index,
        key=step.key,
        name=step.name,
        processtype=step.processtype,
        profile_type=step.profile_type,
        profile_id=step.profile_id,
        link_element_key=step.link_element_key,
        link_element_name=step.link_element_name,
    )
    if op == "combine_documents":
        return RenderDataProcessStep(
            combine_into_link_element_key=step.combine_into_link_element_key, **common
        )
    return RenderDataProcessStep(**common)


def _render_operand(operand: BaseModel) -> RenderDecisionValue:
    if operand.value_type == "track":
        return RenderDecisionValue(
            value_type="track",
            property_id=operand.property_id,
            property_name=operand.property_name or "",
            default_value=operand.default_value or "",
        )
    return RenderDecisionValue(value_type="static", static_value=operand.static_value)


def _render_property_source(source: BaseModel) -> RenderPropertySource:
    vt = source.value_type
    if vt == "static":
        return RenderPropertySource(value_type="static", value=source.value)
    if vt == "current":
        return RenderPropertySource(value_type="current")
    if vt == "profile":
        return RenderPropertySource(
            value_type="profile",
            element_id=source.element_id,
            element_name=source.element_name,
            profile_id=source.profile_id,
            profile_type=source.profile_type,
        )
    if vt == "ddp":
        # render derives ``dynamicdocument.{name}`` from the bare name; recover it.
        bare = source.property_id
        if bare.startswith(_DDP_PROPERTY_PREFIX):
            bare = bare[len(_DDP_PROPERTY_PREFIX):]
        return RenderPropertySource(
            value_type="ddp", property_name=bare, default_value=source.default_value
        )
    # dpp — ``process_property`` is already the bare name the renderer emits.
    return RenderPropertySource(
        value_type="dpp",
        property_name=source.process_property,
        default_value=source.default_value,
    )


# ---------------------------------------------------------------------------
# Emit callables (typed input + context -> shape XML). Each reuses the shared
# renderer; none recomputes geometry.
# ---------------------------------------------------------------------------


def _emit_start(inp, ctx):
    return rendering.render_start_noaction(_shape_context(ctx.node))


def _emit_connector(inp, ctx):
    return rendering.render_connectoraction(
        _shape_context(ctx.node),
        userlabel=inp.userlabel,
        connector_type=inp.connector_type,
        action_type=inp.action_type,
        connection_id=inp.connection_id,
        operation_id=inp.operation_id,
    )


def _emit_message(inp, ctx):
    return rendering.render_message(_shape_context(ctx.node), userlabel=inp.userlabel, text=inp.text)


def _emit_map(inp, ctx):
    return rendering.render_map(_shape_context(ctx.node), userlabel=inp.userlabel, map_id=inp.map_id)


def _emit_flowcontrol(inp, ctx):
    return rendering.render_flowcontrol(
        _shape_context(ctx.node), userlabel=inp.userlabel, for_each_count=inp.for_each_count
    )


def _emit_dataprocess(inp, ctx):
    steps = tuple(_render_step(step) for step in inp.steps)
    return rendering.render_dataprocess(
        _shape_context(ctx.node), userlabel=inp.userlabel, steps=steps
    )


def _emit_doccacheload(inp, ctx):
    return rendering.render_doccacheload(
        _shape_context(ctx.node), userlabel=inp.userlabel, doc_cache_id=inp.document_cache_id
    )


def _emit_doccacheretrieve(inp, ctx):
    return rendering.render_doccacheretrieve(
        _shape_context(ctx.node),
        userlabel=inp.userlabel,
        doc_cache_id=inp.document_cache_id,
        empty_cache_behavior=inp.empty_cache_behavior,
    )


def _emit_doccacheremove(inp, ctx):
    return rendering.render_doccacheremove(
        _shape_context(ctx.node), userlabel=inp.userlabel, doc_cache_id=inp.document_cache_id
    )


def _emit_setproperties(inp, ctx):
    prefix = _DDP_PROPERTY_PREFIX if inp.scope == "ddp" else _DPP_PROPERTY_PREFIX
    name = inp.property_id
    if name.startswith(prefix):
        name = name[len(prefix):]
    sourcevalues = "".join(
        rendering.render_property_source_value(i, _render_property_source(source))
        for i, source in enumerate(inp.source_values, start=1)
    )
    prop = rendering.render_documentproperty_assignment(
        inp.scope, name, inp.persist, sourcevalues
    )
    return rendering.render_setproperties_shape(
        _shape_context(ctx.node), userlabel=inp.userlabel, properties_xml=prop
    )


def _emit_processcall(inp, ctx):
    return rendering.render_processcall(
        _shape_context(ctx.node),
        userlabel=inp.userlabel,
        process_id=inp.process_id,
        wait=inp.wait,
        abort=inp.abort,
    )


def _emit_branch(inp, ctx):
    return rendering.render_branch(
        _shape_context(ctx.node), userlabel=inp.userlabel, num_branches=inp.num_branches
    )


def _emit_decision(inp, ctx):
    return rendering.render_decision(
        _shape_context(ctx.node),
        label=inp.userlabel,
        comparison=inp.comparison,
        left=_render_operand(inp.left),
        right=_render_operand(inp.right),
    )


def _emit_exception(inp, ctx):
    return rendering.render_exception(
        _shape_context(ctx.node),
        title=inp.title,
        stop_single_document=inp.stop_single_document,
        message=rendering._escape_message_format_text(inp.message_template),
        binding=RenderExceptionBinding(kind=inp.binding.binding),
    )


def _emit_stop(inp, ctx):
    return rendering.render_stop(_shape_context(ctx.node), continue_=inp.continue_)


def _emit_returndocuments(inp, ctx):
    return rendering.render_returndocuments(_shape_context(ctx.node), label=inp.label)


# ---------------------------------------------------------------------------
# Symbol requirements
# ---------------------------------------------------------------------------


def _no_requirements(inp) -> Tuple[SymbolRequirement, ...]:
    return ()


def _req_connector(inp) -> Tuple[SymbolRequirement, ...]:
    return (
        SymbolRequirement("connection", inp.connection_id, _CONNECTOR_SETTINGS_TYPES),
        SymbolRequirement("operation", inp.operation_id, _CONNECTOR_ACTION_TYPES),
    )


def _req_map(inp) -> Tuple[SymbolRequirement, ...]:
    return (SymbolRequirement("map", inp.map_id, _MAP_TYPES),)


def _req_dataprocess(inp) -> Tuple[SymbolRequirement, ...]:
    reqs = []
    for step in inp.steps:
        profile_id = getattr(step, "profile_id", "")
        if not profile_id:
            continue
        # Exact profile-kind match (no normalization) — mirror the legacy validator
        # ``_validate_dataprocess_profile_step``, which tests exact membership. An
        # unsupported/denormalized kind maps to nothing → the requirement can never
        # be satisfied (fails closed); the precondition also flags it INVALID.
        component_type = _DP_PROFILE_COMPONENT_TYPE.get(getattr(step, "profile_type", None))
        types = (component_type,) if component_type else ()
        reqs.append(SymbolRequirement("profile", profile_id, types))
    return tuple(reqs)


def _req_cache(inp) -> Tuple[SymbolRequirement, ...]:
    return (SymbolRequirement("cache", inp.document_cache_id, _CACHE_TYPES),)


def _req_setproperties(inp) -> Tuple[SymbolRequirement, ...]:
    # Each profile source declares its full component type (``profile.json`` etc.);
    # require exactly that. A declared type outside the supported set can never be
    # satisfied by a valid profile symbol, so it fails closed.
    reqs = []
    for source in inp.source_values:
        if source.value_type != "profile":
            continue
        declared = source.profile_type
        types = (declared,) if declared in _SETPROP_PROFILE_TYPES else ()
        reqs.append(SymbolRequirement("profile", source.profile_id, types))
    return tuple(reqs)


def _req_process(inp) -> Tuple[SymbolRequirement, ...]:
    return (SymbolRequirement("process", inp.process_id, _PROCESS_TYPES),)


# ---------------------------------------------------------------------------
# Renderer preconditions (fail-closed input checks beyond the typed schema)
# ---------------------------------------------------------------------------

_DP_PROFILE_KINDS = frozenset({"json", "xml"})


def _pre_doccacheretrieve(inp) -> Optional[str]:
    if inp.load_all_documents is not True:
        return "load_all_documents must be true"
    if inp.empty_cache_behavior not in rendering._DOCCACHE_RETRIEVE_EMPTY_BEHAVIORS:
        return "unsupported empty_cache_behavior"
    return None


def _pre_doccacheremove(inp) -> Optional[str]:
    if inp.remove_all_documents is not True:
        return "remove_all_documents must be true"
    return None


def _blank(value) -> bool:
    return not isinstance(value, str) or not value.strip()


def _pre_dataprocess(inp) -> Optional[str]:
    # Mirror the legacy ``validate_config`` gate (process_flow_builder
    # ``_validate_dataprocess_transform`` / ``_validate_dataprocess_profile_step``)
    # — the authoritative authoring path REJECTS these before emission, so the
    # registry preflight must reject them too (the bypass emitter is lenient by
    # design, but it is not the contract). An empty ``<dataprocess>``, empty
    # ``<script>`` or blank link element is well-formed XML the topology verifier
    # cannot catch and would silently drop documents at runtime.
    if not inp.steps:
        return "data process requires at least one step"
    for step in inp.steps:
        if step.operation == "custom_scripting":
            if step.language != rendering._DATAPROCESS_SCRIPT_LANGUAGE:
                return "unsupported custom-scripting language"
            if step.use_cache is not True:
                return "custom-scripting useCache must be true"
            if _blank(step.script):
                return "custom-scripting script must be a non-empty string"
        else:  # split_documents / combine_documents
            # Exact membership (no strip/lower) — the legacy validate_config gate
            # (_validate_dataprocess_profile_step) tests exact membership, so
            # ``"JSON"`` / ``" json "`` must fail here too (and they would also emit a
            # divergent ``profileType`` attribute).
            if getattr(step, "profile_type", None) not in _DP_PROFILE_KINDS:
                return "unsupported data-process profile_type"
            for key in ("profile_id", "link_element_key", "link_element_name"):
                if _blank(getattr(step, key, "")):
                    return f"data-process {key} must be a non-empty string"
            if step.operation == "combine_documents" and _blank(
                getattr(step, "combine_into_link_element_key", "")
            ):
                return "combine_into_link_element_key must be a non-empty string"
    return None


def _pre_decision(inp) -> Optional[str]:
    # The legacy decision emitter raises on a track operand with a blank
    # property_id (it would emit ``propertyId=""``).
    for operand in (inp.left, inp.right):
        if operand.value_type == "track" and not str(operand.property_id or "").strip():
            return "decision track operand requires a property id"
    return None


def _pre_exception(inp) -> Optional[str]:
    # The resolved ``binding`` and the legacy ``parameter_source`` must agree — the
    # legacy emitter derives the exParameters form from parameter_source, so an
    # inconsistent pair would emit a binding that disagrees with the authored
    # source. Mirror the legacy mapping (anything but none/current_document ->
    # caught_error).
    src = str(inp.parameter_source or "caught_error").strip().lower()
    expected = src if src in ("none", "current_document") else "caught_error"
    if inp.binding.binding != expected:
        return "exception binding disagrees with parameter_source"
    return None


# ---------------------------------------------------------------------------
# Default registrations (17 discriminator keys; 16 model classes — the connector
# source/target keys share one renderer).
# ---------------------------------------------------------------------------

_REGISTRATIONS: Tuple[EmitterRegistration, ...] = (
    EmitterRegistration("start_noaction", StartNoActionInputV1, "start", CAPABILITY_PROCESS_IR_V1, EXACT_ONE, _no_requirements, _emit_start),
    EmitterRegistration("connectoraction_source", ConnectorActionInputV1, "connectoraction", CAPABILITY_PROCESS_IR_V1, EXACT_ONE, _req_connector, _emit_connector),
    EmitterRegistration("connectoraction_target", ConnectorActionInputV1, "connectoraction", CAPABILITY_PROCESS_IR_V1, EXACT_ONE, _req_connector, _emit_connector),
    EmitterRegistration("message", MessageInputV1, "message", CAPABILITY_PROCESS_IR_V1, EXACT_ONE, _no_requirements, _emit_message),
    EmitterRegistration("map", MapInputV1, "map", CAPABILITY_PROCESS_IR_V1, EXACT_ONE, _req_map, _emit_map),
    EmitterRegistration("flowcontrol", FlowControlInputV1, "flowcontrol", CAPABILITY_PROCESS_IR_V1, EXACT_ONE, _no_requirements, _emit_flowcontrol),
    EmitterRegistration("dataprocess", DataProcessInputV1, "dataprocess", CAPABILITY_PROCESS_IR_V1, EXACT_ONE, _req_dataprocess, _emit_dataprocess, _pre_dataprocess),
    EmitterRegistration("doccacheload", DocCacheLoadInputV1, "doccacheload", CAPABILITY_PROCESS_IR_V1, ZERO_OR_ONE, _req_cache, _emit_doccacheload),
    EmitterRegistration("doccacheretrieve", DocCacheRetrieveInputV1, "doccacheretrieve", CAPABILITY_PROCESS_IR_V1, EXACT_ONE, _req_cache, _emit_doccacheretrieve, _pre_doccacheretrieve),
    EmitterRegistration("doccacheremove", DocCacheRemoveInputV1, "doccacheremove", CAPABILITY_PROCESS_IR_V1, EXACT_ONE, _req_cache, _emit_doccacheremove, _pre_doccacheremove),
    EmitterRegistration("setproperties_step", SetPropertiesStepInputV1, "documentproperties", CAPABILITY_PROCESS_IR_V1, EXACT_ONE, _req_setproperties, _emit_setproperties),
    EmitterRegistration("processcall", ProcessCallInputV1, "processcall", CAPABILITY_PROCESS_IR_V1, EXACT_ONE, _req_process, _emit_processcall),
    EmitterRegistration("branch", BranchInputV1, "branch", CAPABILITY_PROCESS_IR_V1, BRANCH, _no_requirements, _emit_branch),
    EmitterRegistration("decision", DecisionInputV1, "decision", CAPABILITY_PROCESS_IR_V1, EXACT_TWO, _no_requirements, _emit_decision, _pre_decision),
    EmitterRegistration("exception", ExceptionInputV1, "exception", CAPABILITY_PROCESS_IR_V1, EXACT_ZERO, _no_requirements, _emit_exception, _pre_exception),
    EmitterRegistration("stop", StopInputV1, "stop", CAPABILITY_PROCESS_IR_V1, EXACT_ZERO, _no_requirements, _emit_stop),
    EmitterRegistration("returndocuments", ReturnDocumentsInputV1, "returndocuments", CAPABILITY_PROCESS_IR_V1, EXACT_ZERO, _no_requirements, _emit_returndocuments),
)


def _build_registry(
    registrations: Tuple[EmitterRegistration, ...]
) -> Mapping[str, EmitterRegistration]:
    registry: Dict[str, EmitterRegistration] = {}
    for reg in registrations:
        if reg.emitter_kind in registry:
            raise ValueError(f"duplicate emitter registration: {reg.emitter_kind!r}")
        registry[reg.emitter_kind] = reg
    # Read-only after construction: nothing may add a kind (e.g. ``emit_fragment``)
    # to the canonical registry after coverage validation.
    return MappingProxyType(registry)


def discriminator_keys() -> frozenset:
    """The closed ``emitter_kind`` discriminator values of ``EmitterInputV1``."""
    schema = TypeAdapter(EmitterInputV1).json_schema()
    mapping = (schema.get("discriminator") or {}).get("mapping") or {}
    return frozenset(mapping.keys())


_REGISTRY = _build_registry(_REGISTRATIONS)


def _validate_coverage() -> None:
    keys = frozenset(_REGISTRY.keys())
    expected = discriminator_keys()
    if keys != expected:
        missing = expected - keys
        extra = keys - expected
        raise ValueError(
            "process-emitter registry does not cover EmitterInputV1 exactly: "
            f"missing={sorted(missing)} extra={sorted(extra)}"
        )


_validate_coverage()


def registry_keys() -> frozenset:
    return frozenset(_REGISTRY.keys())


def registration_for(emitter_kind: str) -> Optional[EmitterRegistration]:
    return _REGISTRY.get(emitter_kind)


# ---------------------------------------------------------------------------
# Preflight + emit
# ---------------------------------------------------------------------------


def _component_symbol_index(symbols: SymbolTableV1) -> Dict[str, tuple]:
    """component_id -> the resolved symbols carrying it, in canonical ref order.

    Keeps the actual ``ComponentSymbolV1`` objects (not just their types) so the
    emitter context can advertise the real resolved symbols. ``SymbolTableV1`` is
    already sorted by ``ref``, so iteration preserves canonical order; two refs may
    share one component id (intentional reuse), so the value is a tuple.
    """
    index: Dict[str, list] = {}
    for symbol in symbols.symbols:
        index.setdefault(symbol.component_id, []).append(symbol)
    return {cid: tuple(syms) for cid, syms in index.items()}


def _cardinality_ok(card: OutgoingCardinality, inp, node: EmissionNodeV1) -> bool:
    n = len(node.outgoing)
    if card.kind == "exact":
        return n == card.value
    if card.kind == "branch":
        return n == inp.num_branches
    if card.kind == "zero_or_one":
        return n in (0, 1)
    return False


def _preflight_node(
    node: EmissionNodeV1,
    id_index: Dict[str, set],
    capability: CapabilityLevel,
) -> Tuple[List[CompilerDiagnostic], Optional[EmitterRegistration], Tuple[object, ...]]:
    """Check one node. Returns (diagnostics, registration, narrowed_symbols)."""
    diags: List[CompilerDiagnostic] = []
    inp = node.emitter_input
    path = node.source_path or ""
    reg = _REGISTRY.get(inp.emitter_kind)
    if reg is None or reg.supported_capability != capability:
        diags.append(
            diagnostic(PROCESS_IR_COMPILE_EMITTER_MISSING, "xml_emission", path,
                       internal_node_id=node.cfg_node_id)
        )
        return diags, None, ()
    # Exact type, not isinstance: a subclass of the expected input model (e.g. one
    # smuggling raw legacy config) would pass isinstance and reach the emitter,
    # violating the "emitters receive no raw legacy config" contract.
    if type(inp) is not reg.input_type:
        diags.append(
            diagnostic(PROCESS_IR_COMPILE_EMITTER_INPUT_INVALID, "xml_emission", path,
                       internal_node_id=node.cfg_node_id)
        )
        return diags, None, ()
    if reg.precondition is not None and reg.precondition(inp) is not None:
        diags.append(
            diagnostic(PROCESS_IR_COMPILE_EMITTER_INPUT_INVALID, "xml_emission", path,
                       internal_node_id=node.cfg_node_id)
        )
    if not _cardinality_ok(reg.outgoing, inp, node):
        diags.append(
            diagnostic(PROCESS_IR_COMPILE_EMITTER_INPUT_INVALID, "xml_emission", path,
                       internal_node_id=node.cfg_node_id)
        )
    # Resolve each requirement to its actual compatible symbols. A node may repeat
    # the same (component_id, types) requirement (e.g. many Data Process steps
    # sharing one profile) and that id may have many aliases, so memoize the alias
    # scan per requirement key and deduplicate refs incrementally — never an
    # O(requirements x aliases) accumulate-then-dedup.
    matched: Dict[str, object] = {}  # ref -> symbol
    match_cache: Dict[tuple, tuple] = {}
    for req in reg.requirements(inp):
        key = (req.component_id, req.component_types)
        matches = match_cache.get(key)
        if matches is None:
            candidates = id_index.get(req.component_id, ())
            matches = tuple(s for s in candidates if s.component_type in req.component_types)
            match_cache[key] = matches
            # Merge each distinct requirement's matches ONCE, on the cache miss, so
            # R duplicate requirements do not re-merge A aliases R times.
            for s in matches:
                matched.setdefault(s.ref, s)
        # Unresolved is per-requirement (a repeated missing id is still one defect
        # per authored reference), so the diagnostic is appended on hit or miss.
        if not matches:
            diags.append(
                diagnostic(PROCESS_IR_COMPILE_SYMBOL_UNRESOLVED, "reference_resolution", path,
                           internal_node_id=node.cfg_node_id)
            )
    # Canonical ref order; these are the ACTUAL resolved symbols.
    narrowed = tuple(sorted(matched.values(), key=lambda s: s.ref))
    return diags, reg, narrowed


def _verifier_summary(result: Mapping) -> ProcessVerifierSummaryV1:
    def _issues(items):
        return tuple(
            VerifierIssueV1(
                code=str(i.get("code", "")),
                shape=str(i.get("shape", "")),
                shape_type=str(i.get("shape_type", "")),
                message=str(i.get("message", "")),
                remediation=str(i.get("remediation", "")),
            )
            for i in items
        )

    return ProcessVerifierSummaryV1(
        errors=_issues(result.get("errors", [])),
        warnings=_issues(result.get("warnings", [])),
        shapes_checked=int(result.get("shapes_checked", 0)),
    )


def emit_process(
    emission_plan: EmissionPlanV1,
    resolved_symbols: SymbolTableV1,
    *,
    capability_level: CapabilityLevel = CAPABILITY_PROCESS_IR_V1,
) -> ProcessEmissionArtifactV1:
    """Turn a compiled emission plan into a verified internal process artifact.

    Fail-closed: every node is pre-flighted before any XML is produced; any
    failure raises a single ``ProcessIRCompileError`` with value-free diagnostics
    and produces no output.
    """
    id_index = _component_symbol_index(resolved_symbols)

    # Whole-plan preflight — accumulate every node's diagnostics, then abort once.
    all_diags: List[CompilerDiagnostic] = []
    node_regs: List[Tuple[EmissionNodeV1, EmitterRegistration, Tuple[object, ...]]] = []
    for node in emission_plan.nodes:
        diags, reg, narrowed = _preflight_node(node, id_index, capability_level)
        all_diags.extend(diags)
        if reg is not None and not diags:
            node_regs.append((node, reg, narrowed))
    if all_diags:
        raise ProcessIRCompileError(all_diags)

    # Emit each shape in plan order (never registry order).
    parts: List[str] = []
    try:
        for node, reg, narrowed in node_regs:
            ctx = EmitterContext(
                node=node, resolved_symbols=narrowed, capability_level=capability_level
            )
            parts.append(reg.emit(node.emitter_input, ctx))
    except ProcessIRCompileError:
        raise
    except Exception:  # noqa: BLE001 — never leak internals
        raise ProcessIRCompileError(
            [diagnostic(PROCESS_IR_COMPILE_INTERNAL, "xml_emission", "")]
        ) from None

    process_xml = "<process xmlns=\"\"><shapes>" + "".join(parts) + "</shapes></process>"

    # Structural cross-check: parse-back, shape count/name/type agree with the plan.
    try:
        root = ET.fromstring(process_xml)
    except ET.ParseError:
        raise ProcessIRCompileError(
            [diagnostic(PROCESS_IR_COMPILE_XML_INVALID, "xml_emission", "")]
        ) from None
    shape_elems = list(root.find("shapes"))
    if len(shape_elems) != len(node_regs):
        raise ProcessIRCompileError(
            [diagnostic(PROCESS_IR_COMPILE_XML_INVALID, "xml_emission", "")]
        )
    for elem, (node, reg, _narrowed) in zip(shape_elems, node_regs):
        if elem.get("name") != node.shape_id or elem.get("shapetype") != reg.produced_shape_type:
            raise ProcessIRCompileError(
                [diagnostic(PROCESS_IR_COMPILE_XML_INVALID, "xml_emission",
                            node.source_path or "")]
            )

    # Post-emission structural oracle. Guard the invocation/adaptation so an
    # unexpected verifier failure becomes a value-free INTERNAL diagnostic rather
    # than escaping raw (its text could echo emitted content).
    try:
        verifier = _verifier_summary(verify_process_graph(process_xml))
    except Exception:  # noqa: BLE001 — never leak internals
        raise ProcessIRCompileError(
            [diagnostic(PROCESS_IR_COMPILE_INTERNAL, "post_emission_verification", "")]
        ) from None
    if verifier.errors:
        raise ProcessIRCompileError(
            [diagnostic(PROCESS_IR_COMPILE_VERIFIER_FAILED, "post_emission_verification", "")]
        )

    return ProcessEmissionArtifactV1(
        shape_xml_parts=tuple(parts),
        process_xml=process_xml,
        verifier=verifier,
    )


__all__ = [
    "CAPABILITY_PROCESS_IR_V1",
    "CapabilityLevel",
    "EmitterContext",
    "EmitterRegistration",
    "OutgoingCardinality",
    "ProcessEmissionArtifactV1",
    "ProcessVerifierSummaryV1",
    "SymbolRequirement",
    "VerifierIssueV1",
    "discriminator_keys",
    "emit_process",
    "registration_for",
    "registry_keys",
]
