"""Issue #137 (M12.2): ProcessIR compiler parity, determinism, and purity.

The load-bearing test here is ``test_plan_matches_legacy_builder_xml``: it
projects the UNCHANGED legacy builder's XML into (shape, shapetype, geometry,
dragpoint) facts and requires the emission plan to describe exactly those facts,
for all three golden IR documents and all ten frozen codec parity cases. That
makes the legacy builder — not a hand-written expectation — the parity oracle.

Deliberately NOT here: a test-only plan->XML emitter. Emission is #138's
boundary; inventing a second emitter to test the plan would prove only that the
two agreed with each other.
"""

import copy
import json
import re
import sys
import xml.etree.ElementTree as ET
from pathlib import Path

import pytest

_src = str(Path(__file__).resolve().parent.parent / "src")
if _src not in sys.path:
    sys.path.insert(0, _src)

from boomi_mcp.categories.components.builders.process_flow_builder import (
    ProcessFlowBuilder,
    WrapperSubprocessBuilder,
)
from boomi_mcp.categories.components.process_graph_verifier import verify_process_graph
from boomi_mcp.compiler.process_ir import (
    ComponentSymbolV1,
    ProcessIRCompileError,
    SymbolTableV1,
    canonical_cfg_json,
    canonical_emission_plan_json,
    check_cfg_invariants,
    check_emission_plan_invariants,
    compile_process_ir_v1,
    lower_cfg_to_emission_plan,
    lower_process_ir_to_cfg,
    parse_and_compile_process_ir_v1,
)
from boomi_mcp.errors import PROCESS_IR_CAPABILITY_UNSUPPORTED
from boomi_mcp.models._process_ir_compat import (
    ConnectorBindingV1,
    ConnectorResolutionContextV1,
    ir_to_legacy_flow_sequence,
    legacy_flow_sequence_to_ir,
)
from boomi_mcp.models.process_ir import parse_process_ir_v1

_FIXTURES = Path(__file__).resolve().parent / "fixtures" / "process_ir"
GOLDEN_DOCS = json.loads((_FIXTURES / "process_ir_v1.json").read_text())
_COMPAT_RAW = json.loads((_FIXTURES / "flow_sequence_compat_cases.json").read_text())
_SHARED = _COMPAT_RAW["shared"]
COMPILER_GOLDEN_PATH = _FIXTURES / "process_ir_compiler_v1.json"


def _resolve_placeholders(value):
    """Expand the fixture's ``@shared`` placeholders (mirrors the codec test)."""
    if isinstance(value, str) and value.startswith("@"):
        return copy.deepcopy(_SHARED[value[1:]])
    if isinstance(value, dict):
        return {key: _resolve_placeholders(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_resolve_placeholders(item) for item in value]
    return value


COMPAT_CASES = {
    name: _resolve_placeholders(case) for name, case in _COMPAT_RAW["cases"].items()
}

# Connector bindings, keyed by operation_ref exactly as the #136 codec keys them.
_BINDINGS = _SHARED["bindings"]


# ---------------------------------------------------------------------------
# Symbol-table construction
# ---------------------------------------------------------------------------

_REF_FIELDS_BY_SEMANTIC = {
    "connector": ("connection_ref", "operation_ref"),
    "map": ("map_ref",),
    "cache_put": ("cache_ref",),
    "cache_get": ("cache_ref",),
    "document_cache_retrieve": ("cache_ref",),
    "cache_remove": ("cache_ref",),
    "process_call": ("process_ref",),
}


def _refs_in(cfg):
    """Every authored reference the emission plan will have to resolve."""
    refs = set()
    for node in cfg.nodes:
        semantic = node.semantic
        for field in _REF_FIELDS_BY_SEMANTIC.get(semantic.semantic_kind, ()):
            refs.add(getattr(semantic, field))
        if semantic.semantic_kind == "data_process":
            for step in semantic.steps:
                if getattr(step, "profile_ref", None):
                    refs.add(step.profile_ref)
        if semantic.semantic_kind == "set_property":
            for source in semantic.source_values:
                if getattr(source, "profile_ref", None):
                    refs.add(source.profile_ref)
    return refs


def _symbols_for(cfg, *, reverse=False):
    """Build a symbol table covering ``cfg``.

    Component ids are the reference token itself — a sentinel, so no live id is
    ever committed, and any id that leaks into a diagnostic is obvious.
    """
    refs = sorted(_refs_in(cfg), reverse=reverse)
    symbols = []
    for ref in refs:
        binding = _BINDINGS.get(ref)
        symbols.append(
            ComponentSymbolV1(
                ref=ref,
                component_id=ref,
                component_type="sentinel",
                connector_type=binding["connector_type"] if binding else None,
                action_type=binding["action_type"] if binding else None,
            )
        )
    return SymbolTableV1(symbols=tuple(symbols))


def _context(*, with_fallback):
    return ConnectorResolutionContextV1(
        operation_bindings={
            ref: ConnectorBindingV1(**binding) for ref, binding in _BINDINGS.items()
        },
        fallback_target=copy.deepcopy(_SHARED["target"]) if with_fallback else None,
    )


def _compile(payload):
    ir = parse_process_ir_v1(payload)
    cfg = lower_process_ir_to_cfg(ir)
    symbols = _symbols_for(cfg)
    plan = lower_cfg_to_emission_plan(cfg, symbols)
    check_cfg_invariants(cfg)
    check_emission_plan_invariants(plan, cfg, symbols)
    return ir, cfg, plan


# ---------------------------------------------------------------------------
# Legacy-XML projection (the parity oracle)
# ---------------------------------------------------------------------------

# emitter kind -> the shapetype the legacy emitter writes. Several emitter kinds
# deliberately collapse onto one shapetype (both connector roles -> connectoraction;
# both DDP and DPP -> documentproperties; cache_get and document_cache_retrieve ->
# doccacheretrieve), which is exactly the legacy behaviour being pinned.
_PLAN_SHAPETYPE = {
    "start_noaction": "start",
    "connectoraction_source": "connectoraction",
    "connectoraction_target": "connectoraction",
    "message": "message",
    "map": "map",
    "flowcontrol": "flowcontrol",
    "dataprocess": "dataprocess",
    "doccacheload": "doccacheload",
    "doccacheretrieve": "doccacheretrieve",
    "doccacheremove": "doccacheremove",
    "setproperties_step": "documentproperties",
    "processcall": "processcall",
    "branch": "branch",
    "decision": "decision",
    "exception": "exception",
    "stop": "stop",
    "returndocuments": "returndocuments",
}


def _shape_ordinal(name):
    return int(re.sub(r"\D", "", name) or "0")


def _xml_facts(process_xml):
    """Project emitted XML into comparable shape/wiring facts."""
    root = ET.fromstring(process_xml)
    facts = []
    for shape in root.iter("shape"):
        dragpoints = tuple(
            (
                point.get("name"),
                point.get("toShape"),
                point.get("x"),
                point.get("y"),
                point.get("identifier"),
                point.get("text"),
            )
            for point in shape.iter("dragpoint")
        )
        facts.append(
            (
                shape.get("name"),
                shape.get("shapetype"),
                shape.get("x"),
                shape.get("y"),
                dragpoints,
            )
        )
    return sorted(facts, key=lambda row: _shape_ordinal(row[0]))


def _plan_facts(plan):
    """Project an emission plan into the same fact shape."""
    facts = []
    for node in plan.nodes:
        dragpoints = tuple(
            (
                transition.dragpoint_name,
                transition.to_shape_id,
                str(transition.x),
                str(transition.y),
                transition.identifier,
                transition.text,
            )
            for transition in node.outgoing
        )
        facts.append(
            (
                node.shape_id,
                _PLAN_SHAPETYPE[node.emitter_input.emitter_kind],
                str(node.layout.x),
                str(node.layout.y),
                dragpoints,
            )
        )
    return facts


# Emitter-input field -> the legacy XML attribute it must equal, per shapetype.
# Escaped/derived text (message body, exception template) is deliberately absent:
# MessageFormat and XML escaping are the #138 emitter's boundary, so the plan
# carries the RAW value and comparing it here would pin the wrong layer.
_CONFIG_FIELD_MAP = {
    "connectoraction": {
        "connector_type": "connectorType",
        "action_type": "actionType",
        "connection_id": "connectionId",
        "operation_id": "operationId",
    },
    "map": {"map_id": "mapId"},
    "doccacheload": {"document_cache_id": "docCache"},
    "doccacheretrieve": {
        "document_cache_id": "docCache",
        "empty_cache_behavior": "emptyCacheBehavior",
    },
    "doccacheremove": {"document_cache_id": "docCache"},
    "processcall": {"process_id": "processId"},
    "decision": {"comparison": "comparison"},
    "returndocuments": {"label": "label"},
}


def _config_element(shape):
    configuration = shape.find("configuration")
    if configuration is None or len(configuration) == 0:
        return None
    return configuration[0]


def _legacy_config_attrs(process_xml):
    """Map shape name -> (config tag, attrib dict) from emitted XML."""
    root = ET.fromstring(process_xml)
    out = {}
    for shape in root.iter("shape"):
        element = _config_element(shape)
        if element is not None:
            out[shape.get("name")] = (element.tag, dict(element.attrib))
    return out


def _assert_emitter_inputs_match_legacy_config(plan, process_xml):
    """Every resolved emitter-input value must equal the legacy XML attribute.

    This is what catches connector-alias and wire-prefix drift — the shape/
    geometry projection alone cannot see inside ``<configuration>``.
    """
    legacy = _legacy_config_attrs(process_xml)
    checked = 0
    for node in plan.nodes:
        shapetype = _PLAN_SHAPETYPE[node.emitter_input.emitter_kind]
        fields = _CONFIG_FIELD_MAP.get(shapetype)
        entry = legacy.get(node.shape_id)
        if not fields or entry is None:
            continue
        _tag, attrib = entry
        for field, attribute in fields.items():
            planned = getattr(node.emitter_input, field, None)
            if planned is None or attribute not in attrib:
                continue
            assert planned == attrib[attribute], (
                node.shape_id,
                field,
                planned,
                attrib[attribute],
            )
            checked += 1
    return checked


def _assert_property_wire_ids_match_legacy(plan, process_xml):
    """DDP/DPP wire ids and persist flags must match the legacy emitter."""
    root = ET.fromstring(process_xml)
    by_shape = {shape.get("name"): shape for shape in root.iter("shape")}
    checked = 0
    for node in plan.nodes:
        if node.emitter_input.emitter_kind != "setproperties_step":
            continue
        prop = by_shape[node.shape_id].find(
            "configuration/documentproperties/documentproperty"
        )
        assert prop is not None, node.shape_id
        assert node.emitter_input.property_id == prop.get("propertyId")
        assert node.emitter_input.display_name == prop.get("name")
        assert str(node.emitter_input.persist).lower() == prop.get("persist")
        checked += 1
    return checked


def _build_legacy(config, name="ParityProcess"):
    builder = (
        WrapperSubprocessBuilder
        if config.get("process_kind") == "wrapper_subprocess"
        else ProcessFlowBuilder
    )
    return builder.build(config, name=name, folder_name="ParityFolder")


# ---------------------------------------------------------------------------
# Parity against the unchanged legacy builder
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("doc_name", sorted(GOLDEN_DOCS))
def test_plan_matches_legacy_builder_xml_for_goldens(doc_name):
    """Every golden IR document plans exactly the legacy builder's shape graph."""
    ir, _cfg, plan = _compile(GOLDEN_DOCS[doc_name])
    legacy_config = ir_to_legacy_flow_sequence(ir, _context(with_fallback=True))
    assert _plan_facts(plan) == _xml_facts(_build_legacy(legacy_config))


@pytest.mark.parametrize("case_name", sorted(COMPAT_CASES))
def test_plan_matches_legacy_builder_xml_for_compat_cases(case_name):
    """All ten frozen codec cases: legacy config -> IR -> plan == legacy XML."""
    case = COMPAT_CASES[case_name]
    config = copy.deepcopy(case["config"])
    ir = legacy_flow_sequence_to_ir(config)
    cfg = lower_process_ir_to_cfg(ir)
    symbols = _symbols_for(cfg)
    plan = lower_cfg_to_emission_plan(cfg, symbols)
    check_cfg_invariants(cfg)
    check_emission_plan_invariants(plan, cfg, symbols)
    assert _plan_facts(plan) == _xml_facts(_build_legacy(config))


@pytest.mark.parametrize("doc_name", sorted(GOLDEN_DOCS))
def test_emitter_inputs_match_legacy_configuration_for_goldens(doc_name):
    """Resolved emitter-input values must equal the legacy `<configuration>`.

    The shape/geometry projection above cannot see inside `<configuration>`, so
    it happily passed while the plan carried a raw connector alias
    (``rest_client``) that the legacy builder canonicalizes to
    ``officialboomi-X3979C-rest-prod``. This closes that blind spot.
    """
    ir, _cfg, plan = _compile(GOLDEN_DOCS[doc_name])
    legacy_config = ir_to_legacy_flow_sequence(ir, _context(with_fallback=True))
    process_xml = _build_legacy(legacy_config)
    checked = _assert_emitter_inputs_match_legacy_config(plan, process_xml)
    checked += _assert_property_wire_ids_match_legacy(plan, process_xml)
    assert checked > 0, "projection compared nothing — the test would be vacuous"


@pytest.mark.parametrize("case_name", sorted(COMPAT_CASES))
def test_emitter_inputs_match_legacy_configuration_for_compat_cases(case_name):
    config = copy.deepcopy(COMPAT_CASES[case_name]["config"])
    ir = legacy_flow_sequence_to_ir(config)
    cfg = lower_process_ir_to_cfg(ir)
    plan = lower_cfg_to_emission_plan(cfg, _symbols_for(cfg))
    process_xml = _build_legacy(config)
    checked = _assert_emitter_inputs_match_legacy_config(plan, process_xml)
    checked += _assert_property_wire_ids_match_legacy(plan, process_xml)
    assert checked > 0, "projection compared nothing — the test would be vacuous"


def test_connector_canonicalization_matches_the_legacy_builder():
    """Pin the compiler's normalization against the builder's own helper.

    The compiler reuses ``_canonical_connector_type`` lazily rather than
    duplicating the alias table; this fails loudly if that reuse is ever
    replaced by a local copy that drifts.
    """
    from boomi_mcp.categories.components.builders.process_flow_builder import (
        _canonical_connector_type,
    )
    from boomi_mcp.compiler.process_ir.lowering import _canonical_connector_metadata

    for alias in ("rest_client", "rest", "database", "soap_client", "wssoapclientsdk"):
        canonical = _canonical_connector_type(alias)
        target_type, target_action = _canonical_connector_metadata(
            "target", alias, " send "
        )
        assert target_type == canonical
        assert target_action == "SEND"

        source_type, source_action = _canonical_connector_metadata(
            "source", alias, " Get "
        )
        if alias in ("rest_client", "rest"):
            assert source_type == canonical
            assert source_action == "GET"
        else:
            assert source_type == canonical.lower()
            assert source_action == "Get"


def test_padded_property_name_is_stripped_like_the_legacy_emitter():
    """``_validate_bare_property_name`` accepts a padded name; the wire must not.

    The validator checks the STRIPPED string, but the model stores the original,
    so `" DDP_X "` is a valid ProcessIR payload. The legacy emitter strips it, so
    the compiler must too — otherwise the wire id becomes
    ``dynamicdocument. DDP_X ``.
    """
    payload = {
        "version": "1",
        "body": {
            "kind": "sequence",
            "steps": [
                {
                    "kind": "source",
                    "connection_ref": "$ref:db_conn",
                    "operation_ref": "$ref:db_op",
                },
                # Written first, so the lineage validator accepts the read below.
                {
                    "kind": "set_ddp",
                    "name": "SRC_PADDED",
                    "source_values": [{"value_type": "static", "value": "seed"}],
                },
                {
                    "kind": "set_ddp",
                    "name": "  DDP_PADDED  ",
                    "source_values": [
                        {"value_type": "ddp", "property_name": "  SRC_PADDED  "}
                    ],
                },
                {
                    "kind": "target",
                    "connection_ref": "$ref:rest_conn",
                    "operation_ref": "$ref:rest_op",
                },
                {"kind": "stop"},
            ],
        },
    }
    # The padded name really is accepted by the #136 validator, and stored raw.
    ir = parse_process_ir_v1(payload)
    assert ir.body.steps[2].name == "  DDP_PADDED  "

    cfg = lower_process_ir_to_cfg(ir)
    plan = lower_cfg_to_emission_plan(cfg, _symbols_for(cfg))
    properties = [
        node.emitter_input
        for node in plan.nodes
        if node.emitter_input.emitter_kind == "setproperties_step"
    ]
    assert len(properties) == 2
    prop = properties[1]  # the one authored with a padded name
    assert prop.property_id == "dynamicdocument.DDP_PADDED"
    assert prop.display_name == "Dynamic Document Property - DDP_PADDED"
    assert prop.source_values[0].property_id == "dynamicdocument.SRC_PADDED"
    assert prop.source_values[0].property_name == "Dynamic Document Property - SRC_PADDED"

    # And it agrees with what the legacy emitter actually writes.
    legacy_config = ir_to_legacy_flow_sequence(ir, _context(with_fallback=True))
    _assert_property_wire_ids_match_legacy(plan, _build_legacy(legacy_config))


@pytest.mark.parametrize("case_name", sorted(COMPAT_CASES))
def test_legacy_xml_for_compat_cases_verifies_clean(case_name):
    """The XML the plan is pinned against is itself graph-valid."""
    config = copy.deepcopy(COMPAT_CASES[case_name]["config"])
    report = verify_process_graph(_build_legacy(config))
    assert report["errors"] == []


# ---------------------------------------------------------------------------
# Structural acceptance criteria
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("doc_name", sorted(GOLDEN_DOCS))
def test_exactly_one_entry_and_all_paths_terminate(doc_name):
    _ir, cfg, plan = _compile(GOLDEN_DOCS[doc_name])

    inbound = {node.node_id: 0 for node in cfg.nodes}
    for edge in cfg.edges:
        inbound[edge.target_node_id] += 1
    assert [n for n, c in inbound.items() if c == 0] == [cfg.entry_node_id]

    outbound = {node.node_id: 0 for node in cfg.nodes}
    for edge in cfg.edges:
        outbound[edge.source_node_id] += 1
    for node in cfg.nodes:
        if outbound[node.node_id] == 0:
            assert node.exit_role is not None
    assert plan.terminal_shape_ids


@pytest.mark.parametrize("doc_name", sorted(GOLDEN_DOCS))
def test_cfg_carries_no_layout_or_shape_state(doc_name):
    """Shape ids, geometry, and dragpoints must exist ONLY in the plan."""
    _ir, cfg, _plan = _compile(GOLDEN_DOCS[doc_name])
    blob = canonical_cfg_json(cfg)
    for forbidden in ("shape", "dragpoint", "\"x\"", "\"y\"", "layout"):
        assert forbidden not in blob, forbidden


@pytest.mark.parametrize("doc_name", sorted(GOLDEN_DOCS))
def test_synthetic_nodes_have_no_authored_provenance(doc_name):
    _ir, cfg, plan = _compile(GOLDEN_DOCS[doc_name])
    cfg_paths = {node.source_path for node in cfg.nodes}
    for node in plan.nodes:
        if node.origin == "synthetic":
            assert node.cfg_node_id is None
            assert node.source_path is None
            assert node.synthetic_role in ("start", "terminal_stop")
        else:
            assert node.source_path in cfg_paths


def test_routed_target_gets_a_synthetic_stop_but_authored_stop_does_not():
    """The synthetic/authored Stop split is the whole point of plan ownership.

    A branch-leg ``target`` has no Stop in IR, so the compiler owns one. A root
    ``target`` is followed by an AUTHORED ``stop`` node, which must stay
    ``origin="ir"`` — synthesising a second one would double the Stop.
    """
    _ir, cfg, plan = _compile(GOLDEN_DOCS["control_flow"])
    routed = [n for n in cfg.nodes if n.exit_role == "routed_target"]
    assert routed, "control_flow must exercise a routed target"
    synthetic_stops = [n for n in plan.nodes if n.synthetic_role == "terminal_stop"]
    assert len(synthetic_stops) == len(routed)

    _ir2, cfg2, plan2 = _compile(GOLDEN_DOCS["linear_flow"])
    assert [n.exit_role for n in cfg2.nodes if n.exit_role] == ["stop"]
    assert [n for n in plan2.nodes if n.synthetic_role == "terminal_stop"] == []
    authored_stop = [
        n for n in plan2.nodes if n.emitter_input.emitter_kind == "stop"
    ]
    assert len(authored_stop) == 1
    assert authored_stop[0].origin == "ir"


def _decision_stop_config():
    """A decision whose false arm ends in a plain Stop.

    None of the ten frozen codec cases exercises this shape (their false arms
    end in a branch), so it is constructed here rather than assumed — it is the
    one place where an IR-AUTHORED Stop and a COMPILER-OWNED Stop appear in the
    same process, which is precisely the distinction under test.
    """
    return {
        "process_kind": "database_to_api_sync",
        "source": copy.deepcopy(_SHARED["source"]),
        "target": copy.deepcopy(_SHARED["target"]),
        "flow_sequence": [
            {
                "kind": "decision",
                "comparison": "equals",
                "label": "route",
                "left": {
                    "value_type": "track",
                    "property_id": "dynamicdocument.DDP_STATUS",
                    "property_name": "DDP_STATUS",
                    "default_value": "unset",
                },
                "right": {"value_type": "static", "static_value": "ok"},
                "true_steps": [{"kind": "message", "message_text": "accepted"}],
                "false_steps": [{"kind": "message", "message_text": "rejected"}],
            }
        ],
    }


def test_decision_false_arm_stop_is_authored_while_routed_stop_is_synthetic():
    """``DecisionFalseArmV1.terminal`` is a real ``StopNodeV1`` in IR.

    The legacy builder INVENTS every Stop it emits — there is no ``stop`` kind in
    ``_FLOW_SEQUENCE_ALLOWED_KINDS`` at all. IR, by contrast, authors this one.
    So the same process must show an ``origin="ir"`` Stop (the false arm) and an
    ``origin="synthetic"`` Stop (after the true arm's routed target), and the
    compiler must not confuse them.
    """
    config = _decision_stop_config()
    ir = legacy_flow_sequence_to_ir(copy.deepcopy(config))
    cfg = lower_process_ir_to_cfg(ir)
    symbols = _symbols_for(cfg)
    plan = lower_cfg_to_emission_plan(cfg, symbols)
    check_cfg_invariants(cfg)
    check_emission_plan_invariants(plan, cfg, symbols)

    false_arm_stops = [
        node
        for node in cfg.nodes
        if node.semantic.semantic_kind == "stop"
        and node.source_path.endswith("/false_arm/terminal")
    ]
    assert len(false_arm_stops) == 1
    planned = {node.cfg_node_id: node for node in plan.nodes if node.origin == "ir"}
    authored_stop = planned[false_arm_stops[0].node_id]
    assert authored_stop.origin == "ir"
    assert authored_stop.synthetic_role is None

    synthetic_stops = [n for n in plan.nodes if n.synthetic_role == "terminal_stop"]
    assert len(synthetic_stops) == 1
    assert synthetic_stops[0].shape_id != authored_stop.shape_id

    # And the whole thing still matches the unchanged builder byte for byte.
    assert _plan_facts(plan) == _xml_facts(_build_legacy(config))


def test_branch_and_decision_edges_are_ordered_and_typed():
    _ir, cfg, _plan = _compile(GOLDEN_DOCS["control_flow"])
    decisions = [n for n in cfg.nodes if n.semantic.semantic_kind == "decision"]
    assert decisions
    for node in decisions:
        outs = [e for e in cfg.edges if e.source_node_id == node.node_id]
        assert [e.kind for e in outs] == ["decision_outcome"] * 2
        assert [e.outcome for e in outs] == ["true", "false"]

    branches = [n for n in cfg.nodes if n.semantic.semantic_kind == "branch"]
    assert branches
    for node in branches:
        outs = [e for e in cfg.edges if e.source_node_id == node.node_id]
        assert [e.kind for e in outs] == ["branch_leg"] * len(outs)
        assert [e.leg_ordinal for e in outs] == list(range(1, len(outs) + 1))
        assert len(outs) == node.semantic.leg_count


def test_no_catch_edges_are_generated_in_v1():
    for doc in GOLDEN_DOCS.values():
        _ir, cfg, _plan = _compile(doc)
        assert all(edge.kind != "catch" for edge in cfg.edges)


# ---------------------------------------------------------------------------
# Determinism and purity
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("doc_name", sorted(GOLDEN_DOCS))
def test_two_compilations_are_byte_identical(doc_name):
    _ir1, cfg1, plan1 = _compile(GOLDEN_DOCS[doc_name])
    _ir2, cfg2, plan2 = _compile(GOLDEN_DOCS[doc_name])
    assert canonical_cfg_json(cfg1) == canonical_cfg_json(cfg2)
    assert canonical_emission_plan_json(plan1) == canonical_emission_plan_json(plan2)


def _reorder_keys(value):
    """Recursively reverse object key order, preserving every LIST order.

    Semantic order lives in lists; object key order is presentation only, so
    reversing keys must not move a single byte of compiler output.
    """
    if isinstance(value, dict):
        return {key: _reorder_keys(value[key]) for key in reversed(list(value))}
    if isinstance(value, list):
        return [_reorder_keys(item) for item in value]
    return value


@pytest.mark.parametrize("doc_name", sorted(GOLDEN_DOCS))
def test_input_key_order_cannot_change_output(doc_name):
    _ir, cfg, plan = _compile(GOLDEN_DOCS[doc_name])
    _ir2, cfg2, plan2 = _compile(_reorder_keys(GOLDEN_DOCS[doc_name]))
    assert canonical_cfg_json(cfg2) == canonical_cfg_json(cfg)
    assert canonical_emission_plan_json(plan2) == canonical_emission_plan_json(plan)


@pytest.mark.parametrize("doc_name", sorted(GOLDEN_DOCS))
def test_symbol_insertion_order_cannot_change_output(doc_name):
    ir = parse_process_ir_v1(GOLDEN_DOCS[doc_name])
    cfg = lower_process_ir_to_cfg(ir)
    forward = lower_cfg_to_emission_plan(cfg, _symbols_for(cfg))
    reverse = lower_cfg_to_emission_plan(cfg, _symbols_for(cfg, reverse=True))
    assert canonical_emission_plan_json(forward) == canonical_emission_plan_json(
        reverse
    )


@pytest.mark.parametrize("doc_name", sorted(GOLDEN_DOCS))
def test_mutating_the_ir_after_lowering_does_not_change_snapshots(doc_name):
    """The CFG snapshots authored values; it must not alias the caller's models."""
    ir, cfg, plan = _compile(GOLDEN_DOCS[doc_name])
    before_cfg = canonical_cfg_json(cfg)
    before_plan = canonical_emission_plan_json(plan)

    # ``_ProcessIRBase`` is extra="forbid" but NOT frozen, so a caller really can
    # mutate a parsed model in place — which is exactly the aliasing risk.
    mutated = False
    for step in ir.body.steps:
        if getattr(step, "label", None) is not None or hasattr(step, "label"):
            try:
                step.label = "MUTATED-AFTER-LOWERING"
                mutated = True
            except (AttributeError, ValueError):  # pragma: no cover
                pass
            break
    assert mutated, "fixture must expose a mutable authored field"

    assert canonical_cfg_json(cfg) == before_cfg
    assert canonical_emission_plan_json(plan) == before_plan


# ---------------------------------------------------------------------------
# Canonical golden pin
# ---------------------------------------------------------------------------


def _compiler_golden_payload():
    out = {}
    for name in sorted(GOLDEN_DOCS):
        _ir, cfg, plan = _compile(GOLDEN_DOCS[name])
        out[name] = {
            "cfg": json.loads(canonical_cfg_json(cfg)),
            "plan": json.loads(canonical_emission_plan_json(plan)),
        }
    return json.dumps(out, sort_keys=True, separators=(",", ":"), ensure_ascii=True)


def test_canonical_compiler_golden_pin():
    """Byte-pin the CFG/plan snapshots so a silent lowering change cannot land."""
    assert COMPILER_GOLDEN_PATH.read_text() == _compiler_golden_payload()


def test_canonical_compiler_golden_is_stable_within_a_run():
    assert _compiler_golden_payload() == _compiler_golden_payload()


# ---------------------------------------------------------------------------
# Closed cases and guards
# ---------------------------------------------------------------------------


def test_return_documents_with_a_control_terminal_is_unrepresentable():
    """Both the legacy validator and the codec reject it, so no plan branch exists."""
    config = copy.deepcopy(COMPAT_CASES["exception_terminal"]["config"])
    config["return_documents"] = {"enabled": True, "label": "out"}
    error = ProcessFlowBuilder.validate_config(config, depends_on=_SHARED["depends_on"])
    assert error is not None
    assert error.error_code == "PROCESS_FLOW_SEQUENCE_CONFIG_INVALID"


def test_listener_source_is_rejected_with_the_136_capability_code():
    """A listener entry fuses start+connector in legacy; this compiler cannot.

    Fail closed rather than silently emit the wrong shape pair. The code is
    #136's ``PROCESS_IR_CAPABILITY_UNSUPPORTED`` — referenced, never
    re-registered (that would flip its owner in ``ERROR_TAXONOMY``).
    """
    ir = parse_process_ir_v1(GOLDEN_DOCS["linear_flow"])
    cfg = lower_process_ir_to_cfg(ir)
    symbols = SymbolTableV1(
        symbols=tuple(
            ComponentSymbolV1(
                ref=symbol.ref,
                component_id=symbol.component_id,
                component_type=symbol.component_type,
                connector_type=(
                    "wss" if symbol.connector_type == "database" else symbol.connector_type
                ),
                action_type=symbol.action_type,
            )
            for symbol in _symbols_for(cfg).symbols
        )
    )
    with pytest.raises(ProcessIRCompileError) as excinfo:
        lower_cfg_to_emission_plan(cfg, symbols)
    assert excinfo.value.diagnostics[0].code == PROCESS_IR_CAPABILITY_UNSUPPORTED
    assert excinfo.value.diagnostics[0].phase == "reference_resolution"


def test_unresolved_reference_is_an_emission_plan_error():
    ir = parse_process_ir_v1(GOLDEN_DOCS["linear_flow"])
    cfg = lower_process_ir_to_cfg(ir)
    with pytest.raises(ProcessIRCompileError) as excinfo:
        lower_cfg_to_emission_plan(cfg, SymbolTableV1(symbols=()))
    diagnostic = excinfo.value.diagnostics[0]
    assert diagnostic.phase == "reference_resolution"
    assert diagnostic.path.startswith("/body/steps/")
    assert diagnostic.node_identity.startswith("/body/steps/")
    assert diagnostic.remediation


def test_parse_and_compile_translates_schema_diagnostics_verbatim():
    """#136 codes/messages must survive translation unchanged (ADR-001 §7)."""
    with pytest.raises(ProcessIRCompileError) as excinfo:
        parse_and_compile_process_ir_v1({"version": "9"}, SymbolTableV1(symbols=()))
    diagnostic = excinfo.value.diagnostics[0]
    assert diagnostic.code == "PROCESS_IR_SCHEMA_VERSION_UNSUPPORTED"
    assert diagnostic.phase == "schema"
    assert diagnostic.path == "/version"


def test_parse_and_compile_round_trips_a_golden_document():
    doc = GOLDEN_DOCS["wrapper_flow"]
    cfg_only = lower_process_ir_to_cfg(parse_process_ir_v1(doc))
    ir, cfg, plan = parse_and_compile_process_ir_v1(doc, _symbols_for(cfg_only))
    assert ir.version == "1"
    assert canonical_cfg_json(cfg)
    assert plan.entry_shape_id == "shape1"


def test_compile_process_ir_v1_returns_checked_artifacts():
    ir = parse_process_ir_v1(GOLDEN_DOCS["control_flow"])
    cfg_only = lower_process_ir_to_cfg(ir)
    cfg, plan = compile_process_ir_v1(ir, _symbols_for(cfg_only))
    check_cfg_invariants(cfg)
    check_emission_plan_invariants(plan, cfg, _symbols_for(cfg_only))


def test_duplicate_symbol_reference_is_rejected():
    with pytest.raises(Exception):
        SymbolTableV1(
            symbols=(
                ComponentSymbolV1(ref="$ref:a", component_id="1", component_type="t"),
                ComponentSymbolV1(ref="$ref:a", component_id="2", component_type="t"),
            )
        )


def test_component_reuse_across_two_references_is_allowed():
    table = SymbolTableV1(
        symbols=(
            ComponentSymbolV1(ref="$ref:b", component_id="same", component_type="t"),
            ComponentSymbolV1(ref="$ref:a", component_id="same", component_type="t"),
        )
    )
    # Canonicalised by ref, so caller insertion order cannot reach output.
    assert [symbol.ref for symbol in table.symbols] == ["$ref:a", "$ref:b"]
