"""#158: the relaxed entry-shape invariants still reject every mutation they did.

The emission plan's ordinal-1 contract used to be "exactly one synthetic
``start_noaction`` at shape1". #158 re-expressed it as **exactly one
compiler-synthesized entry shape at shape1, of one of two admitted forms** — the
no-action Start for a scheduled root, the fused listener Start for a listener
root — so a listener entry could be emitted without weakening what the checks
guarantee: the compiler owns the entry, and a caller can author neither its
geometry, nor its wiring, nor its input.

These are the per-site adversarials the acceptance criteria ask for. Every one
starts from a plan the compiler REALLY produced (so the baseline is not a
hand-built shape production never emits) and applies exactly one mutation; each
asserts the exact refusal code. Where a scheduled control is meaningful it is
asserted too, so a check that became form-blind in either direction fails.

Fixture provenance: the listener flow is the audited listener form — it is the
IR the `listener_entry` capability witness compiles to the frozen legacy golden
``sync_pipeline_listener_send.xml`` byte-for-byte — plus a map step, the shape of
``sync_pipeline_listener_map_send.xml``. Symbol ids are opaque test values.
"""

from __future__ import annotations

import pytest

from boomi_mcp.compiler.process_ir.contracts import (
    SHAPE_Y,
    START_SHAPE_X,
    START_SHAPE_Y,
    ComponentSymbolV1,
    StartListenInputV1,
    StartNoActionInputV1,
    SymbolTableV1,
    shape_x,
)
from boomi_mcp.compiler.process_ir.diagnostics import ProcessIRCompileError
from boomi_mcp.compiler.process_ir.entry_policy import derive_process_entry
from boomi_mcp.compiler.process_ir.execution_profile import (
    derive_process_execution_profile,
)
from boomi_mcp.compiler.process_ir.invariants import (
    check_cfg_invariants,
    check_emission_plan_invariants,
)
from boomi_mcp.compiler.process_ir.lowering import _emitter_input_for
from boomi_mcp.compiler.process_ir.pipeline import compile_process_ir_v1
from boomi_mcp.errors import (
    PROCESS_IR_COMPILE_EMISSION_PLAN_INVALID,
    PROCESS_IR_COMPILE_INTERNAL,
    PROCESS_IR_COMPILE_NONDETERMINISTIC,
)
from boomi_mcp.models.process_ir import parse_process_ir_v1

_PLAN_INVALID = PROCESS_IR_COMPILE_EMISSION_PLAN_INVALID


def _symbols():
    return SymbolTableV1(
        symbols=(
            ComponentSymbolV1(ref="$ref:wss_op", component_id="WSSOP-1",
                              component_type="connector-action",
                              connector_type="wss", action_type="Listen"),
            # A SECOND valid Listen operation: a forged Start naming it passes every
            # "is this id in the table" check, so only exact re-derivation refuses it.
            ComponentSymbolV1(ref="$ref:wss_op_2", component_id="WSSOP-2",
                              component_type="connector-action",
                              connector_type="wss", action_type="Listen"),
            ComponentSymbolV1(ref="$ref:map", component_id="MAP-1",
                              component_type="transform.map"),
            ComponentSymbolV1(ref="$ref:tc", component_id="TGT-CONN",
                              component_type="connector-settings",
                              connector_type="rest"),
            ComponentSymbolV1(ref="$ref:to", component_id="TGT-OP",
                              component_type="connector-action",
                              connector_type="rest", action_type="POST"),
            ComponentSymbolV1(ref="$ref:sc", component_id="SRC-CONN",
                              component_type="connector-settings",
                              connector_type="database"),
            ComponentSymbolV1(ref="$ref:so", component_id="SRC-OP",
                              component_type="connector-action",
                              connector_type="database", action_type="Get"),
        )
    )


def _listener_doc(label=None):
    listener = {"kind": "listener", "operation_ref": "$ref:wss_op"}
    if label is not None:
        listener["label"] = label
    return {"version": "1", "body": {"kind": "sequence", "steps": [
        listener,
        {"kind": "map_ref", "map_ref": "$ref:map"},
        {"kind": "target", "connection_ref": "$ref:tc", "operation_ref": "$ref:to"},
        {"kind": "stop"},
    ]}}


def _scheduled_doc():
    return {"version": "1", "body": {"kind": "sequence", "steps": [
        {"kind": "source", "connection_ref": "$ref:sc", "operation_ref": "$ref:so"},
        {"kind": "map_ref", "map_ref": "$ref:map"},
        {"kind": "target", "connection_ref": "$ref:tc", "operation_ref": "$ref:to"},
        {"kind": "stop"},
    ]}}


def _compiled(doc):
    symbols = _symbols()
    cfg, plan = compile_process_ir_v1(parse_process_ir_v1(doc), symbols)
    return cfg, plan, symbols


def _refusal(plan, cfg, symbols):
    with pytest.raises(ProcessIRCompileError) as excinfo:
        check_emission_plan_invariants(plan, cfg, symbols)
    return excinfo.value.diagnostics[0]


#: The static message each physical-entry site fails with. Pinned so a test that
#: names a site proves THAT site refuses — an overlapping later check would
#: otherwise let a test pass after its own site was deleted.
_SITE_FORM = "in the form the entry policy derives"
_SITE_ENTRY_ID = "the declared entry shape is not shape1"
_SITE_START_GEOMETRY = "the synthetic start shape has non-parity geometry"
_SITE_ONE_START = "the plan must contain exactly one synthetic start shape"
_SITE_SUCCESSOR = "the synthetic start shape must wire to the shape the entry policy derives"
_SITE_INPUT = "synthetic plan node carries the wrong emitter input"
_SITE_BODY_GEOMETRY = "plan shape geometry does not match the parity formula"
_SITE_ONE_WIRE = "the synthetic start must carry exactly one synthetic wire"


def _site(diagnostic, fragment):
    assert diagnostic.code == _PLAN_INVALID, diagnostic.code
    assert fragment in diagnostic.message, diagnostic.message


def _with_start(plan, **update):
    start = plan.nodes[0].model_copy(update=update)
    return plan.model_copy(update={"nodes": (start,) + tuple(plan.nodes[1:])})


def _with_start_wires(plan, wires):
    return _with_start(plan, outgoing=tuple(wires))


# ---------------------------------------------------------------------------
# The fused form itself, and the scheduled control
# ---------------------------------------------------------------------------


def test_listener_is_fused_once_at_shape1():
    cfg, plan, symbols = _compiled(_listener_doc())
    check_emission_plan_invariants(plan, cfg, symbols)  # the baseline is legal

    start = plan.nodes[0]
    assert (start.shape_id, start.origin, start.synthetic_role) == ("shape1", "synthetic", "start")
    assert start.emitter_input == StartListenInputV1(operation_id="WSSOP-1", userlabel="")
    assert (start.layout.x, start.layout.y) == (START_SHAPE_X, START_SHAPE_Y)

    # The listener CFG node has NO plan node of its own; every other node has
    # exactly one, in order, numbered from shape2.
    listener_id = cfg.entry_node_id
    assert cfg.nodes[0].semantic.semantic_kind == "listener"
    ir_nodes = [n for n in plan.nodes if n.origin == "ir"]
    assert [n.cfg_node_id for n in ir_nodes] == [n.node_id for n in cfg.nodes[1:]]
    assert listener_id not in {n.cfg_node_id for n in ir_nodes}
    assert [n.shape_id for n in plan.nodes] == ["shape1", "shape2", "shape3", "shape4"]
    assert plan.nodes[1].layout.x == shape_x(2)
    # ...and the Start's one synthetic wire reaches the listener's SUCCESSOR.
    assert [t.to_shape_id for t in start.outgoing] == ["shape2"]
    assert derive_process_execution_profile(cfg, symbols) == "listener"


def test_scheduled_control_is_unchanged():
    cfg, plan, symbols = _compiled(_scheduled_doc())
    check_emission_plan_invariants(plan, cfg, symbols)
    start = plan.nodes[0]
    assert start.emitter_input == StartNoActionInputV1()
    # The scheduled Start absorbs nothing: every CFG node has its own plan node.
    assert [n.cfg_node_id for n in plan.nodes if n.origin == "ir"] == [
        n.node_id for n in cfg.nodes
    ]
    assert [t.to_shape_id for t in start.outgoing] == ["shape2"]
    assert derive_process_execution_profile(cfg, symbols) == "scheduled"


# ---------------------------------------------------------------------------
# The eight physical-entry sites
# ---------------------------------------------------------------------------


def test_entry_rejects_wrong_form_or_origin():
    """Site 1: shape1 is compiler-owned and carries the POLICY-selected form."""
    cfg, plan, symbols = _compiled(_listener_doc())
    # The other admitted form, on a listener root.
    _site(_refusal(
        _with_start(plan, emitter_input=StartNoActionInputV1()), cfg, symbols
    ), _SITE_FORM)
    # ...and the reverse, on a scheduled root.
    s_cfg, s_plan, s_symbols = _compiled(_scheduled_doc())
    _site(_refusal(
        _with_start(s_plan, emitter_input=StartListenInputV1(operation_id="WSSOP-1")),
        s_cfg, s_symbols,
    ), _SITE_FORM)
    # A Start claiming IR origin for the absorbed listener.
    _site(_refusal(
        _with_start(plan, origin="ir", synthetic_role=None,
                    cfg_node_id=cfg.entry_node_id, source_path="/body/steps/0"),
        cfg, symbols,
    ), _SITE_FORM)


def test_entry_rejects_retargeted_entry_id():
    """Site 2: the declared entry shape stays shape1."""
    cfg, plan, symbols = _compiled(_listener_doc())
    _site(_refusal(
        plan.model_copy(update={"entry_shape_id": "shape2"}), cfg, symbols
    ), _SITE_ENTRY_ID)


@pytest.mark.parametrize("axis", ["x", "y"])
def test_entry_rejects_authored_coordinates(axis):
    """Site 3: the Start's coordinates are fixed, on each axis independently."""
    cfg, plan, symbols = _compiled(_listener_doc())
    layout = plan.nodes[0].layout.model_copy(
        update={axis: getattr(plan.nodes[0].layout, axis) + 8.0}
    )
    _site(_refusal(_with_start(plan, layout=layout), cfg, symbols), _SITE_START_GEOMETRY)


def test_entry_rejects_second_start():
    """Site 4: exactly one synthetic Start — also when an extra one is disguised."""
    cfg, plan, symbols = _compiled(_listener_doc())
    extra = plan.nodes[-1]
    last = extra.ordinal + 1
    second = plan.nodes[0].model_copy(update={
        "ordinal": last, "shape_id": "shape%d" % last, "outgoing": (),
    })
    doubled = plan.model_copy(update={"nodes": tuple(plan.nodes) + (second,)})
    _site(_refusal(doubled, cfg, symbols), _SITE_ONE_START)
    # A listener Start dressed up as a synthetic terminal Stop.
    disguised = plan.nodes[0].model_copy(update={
        "ordinal": last, "shape_id": "shape%d" % last, "outgoing": (),
        "synthetic_role": "terminal_stop",
    })
    dressed = plan.model_copy(update={"nodes": tuple(plan.nodes) + (disguised,)})
    assert _refusal(dressed, cfg, symbols).code in (
        _PLAN_INVALID, PROCESS_IR_COMPILE_NONDETERMINISTIC
    )


@pytest.mark.parametrize("target", ["shape3", "shape1"])
def test_entry_rejects_wrong_successor(target):
    """Site 5: the Start wires to the policy-derived successor — not a later
    existing shape, and not itself."""
    cfg, plan, symbols = _compiled(_listener_doc())
    wire = plan.nodes[0].outgoing[0].model_copy(update={"to_shape_id": target})
    _site(_refusal(_with_start_wires(plan, [wire]), cfg, symbols), _SITE_SUCCESSOR)


def test_entry_rejects_forged_operation_or_label():
    """Site 6: the Start's WHOLE input is re-derived. Symbol membership alone must
    not suffice — `WSSOP-2` is a valid Listen operation in the table."""
    cfg, plan, symbols = _compiled(_listener_doc(label="In"))
    assert plan.nodes[0].emitter_input.userlabel == "In"
    forged_op = StartListenInputV1(operation_id="WSSOP-2", userlabel="In")
    _site(_refusal(_with_start(plan, emitter_input=forged_op), cfg, symbols), _SITE_INPUT)
    forged_label = StartListenInputV1(operation_id="WSSOP-1", userlabel="Other")
    _site(_refusal(
        _with_start(plan, emitter_input=forged_label), cfg, symbols
    ), _SITE_INPUT)


def test_entry_preserves_body_geometry_formula():
    """Site 7: the generic geometry rule keeps its Start exception — body shapes
    use the body formula, and the Start may not take body coordinates."""
    cfg, plan, symbols = _compiled(_listener_doc())
    moved = plan.nodes[1].model_copy(update={
        "layout": plan.nodes[1].layout.model_copy(update={"x": plan.nodes[1].layout.x + 16})
    })
    body_moved = plan.model_copy(
        update={"nodes": (plan.nodes[0], moved) + tuple(plan.nodes[2:])}
    )
    _site(_refusal(body_moved, cfg, symbols), _SITE_BODY_GEOMETRY)
    start_on_body_row = plan.nodes[0].layout.model_copy(update={"y": SHAPE_Y})
    assert _refusal(
        _with_start(plan, layout=start_on_body_row), cfg, symbols
    ).code == _PLAN_INVALID


def test_entry_rejects_wire_count_or_provenance():
    """Site 8: exactly one synthetic Start wire."""
    cfg, plan, symbols = _compiled(_listener_doc())
    wire = plan.nodes[0].outgoing[0]
    assert _refusal(_with_start_wires(plan, []), cfg, symbols).code == _PLAN_INVALID
    twin = wire.model_copy(update={"local_ordinal": 2,
                                   "dragpoint_name": "shape1.dragpoint2"})
    assert _refusal(_with_start_wires(plan, [wire, twin]), cfg, symbols).code in (
        _PLAN_INVALID, PROCESS_IR_COMPILE_NONDETERMINISTIC
    )
    claimed = wire.model_copy(update={"provenance": "cfg_edge", "cfg_edge_id": "e1"})
    _site(_refusal(_with_start_wires(plan, [claimed]), cfg, symbols), _SITE_ONE_WIRE)


# ---------------------------------------------------------------------------
# Adjacent protections the listener form must not open
# ---------------------------------------------------------------------------


def test_the_absorbed_listener_cannot_reappear_as_a_plan_node():
    """CFG correspondence: the ONE node the Start absorbs has no plan node, and no
    other node may disappear. Adding an IR node for the listener, or dropping a
    body node, is refused.

    MEASURED, and recorded rather than hidden: disabling the correspondence check
    alone leaves both mutations refused — by the Start-successor site and by the
    ordinal-contiguity check, which fire first — because every plan node's wiring
    is re-derived from the CFG, so no single-node insertion or deletion keeps the
    wires consistent. The correspondence check is defense in depth here, and this
    test pins the PROPERTY (the absorbed node cannot reappear, no node can vanish),
    not which of the overlapping checks reports it.
    """
    cfg, plan, symbols = _compiled(_listener_doc())
    listener_node = plan.nodes[1].model_copy(update={
        "cfg_node_id": cfg.entry_node_id, "source_path": "/body/steps/0",
    })
    forged = plan.model_copy(update={"nodes": (plan.nodes[0], listener_node) + tuple(plan.nodes[2:])})
    assert _refusal(forged, cfg, symbols).code in (_PLAN_INVALID, PROCESS_IR_COMPILE_INTERNAL)
    dropped = plan.model_copy(update={"nodes": (plan.nodes[0],) + tuple(plan.nodes[2:])})
    assert _refusal(dropped, cfg, symbols).code in (
        _PLAN_INVALID, PROCESS_IR_COMPILE_NONDETERMINISTIC
    )


def test_forged_ir_origin_node_for_absorbed_listener():
    """Asking for the listener's own emitter input is a forged-plan signal: the
    listener has no shape of its own, so lowering refuses to derive one."""
    cfg, _plan, symbols = _compiled(_listener_doc())
    with pytest.raises(ProcessIRCompileError) as excinfo:
        _emitter_input_for(cfg.nodes[0], symbols.build_index())
    assert excinfo.value.diagnostics[0].code == _PLAN_INVALID


def test_cfg_rejects_listener_off_entry_or_twice_or_fanout():
    """The CFG half: a listener must be the single entry, from the first root step,
    with exactly one successor — re-derived from the graph, never trusted."""
    cfg, _plan, _symbols_ = _compiled(_listener_doc())
    listener = cfg.nodes[0]
    # Off the entry: move the entry to the map node.
    off_entry = cfg.model_copy(update={"entry_node_id": cfg.nodes[1].node_id})
    with pytest.raises(ProcessIRCompileError):
        check_cfg_invariants(off_entry)
    # From a path that is not the first root step.
    relocated = cfg.model_copy(update={"nodes": (
        listener.model_copy(update={"source_path": "/body/steps/1"}),
    ) + tuple(cfg.nodes[1:])})
    with pytest.raises(ProcessIRCompileError) as excinfo:
        check_cfg_invariants(relocated)
    assert excinfo.value.diagnostics[0].code == PROCESS_IR_COMPILE_INTERNAL
    # Twice: a second listener semantic on the map node.
    twice = cfg.model_copy(update={"nodes": (listener, cfg.nodes[1].model_copy(update={
        "semantic": listener.semantic}),) + tuple(cfg.nodes[2:])})
    with pytest.raises(ProcessIRCompileError):
        check_cfg_invariants(twice)
    # The strict entry derivation refuses a fan-out too.
    extra = cfg.edges[0].model_copy(update={
        "edge_id": "e9", "ordinal": 9, "local_ordinal": 2,
        "target_node_id": cfg.nodes[2].node_id,
    })
    with pytest.raises(ProcessIRCompileError) as excinfo:
        derive_process_entry(cfg.model_copy(update={"edges": tuple(cfg.edges) + (extra,)}))
    assert excinfo.value.diagnostics[0].code == PROCESS_IR_COMPILE_INTERNAL


def test_no_connector_call_may_carry_the_entry_role_under_a_listener():
    """Under a listener every outbound call is downstream: forging the entry role
    onto one would emit it with the source-read key and start the flow twice."""
    doc = {"version": "1", "body": {"kind": "sequence", "steps": [
        {"kind": "listener", "operation_ref": "$ref:wss_op"},
        {"kind": "connector_call", "operation_ref": "$ref:get_op"},
        {"kind": "stop"},
    ]}}
    symbols = SymbolTableV1(symbols=_symbols().symbols + (
        ComponentSymbolV1(ref="$ref:rc", component_id="REST-CONN",
                          component_type="connector-settings", connector_type="rest"),
        ComponentSymbolV1(ref="$ref:get_op", component_id="GET-OP",
                          component_type="connector-action", connector_type="rest",
                          action_type="GET", connection_ref="$ref:rc"),
    ))
    cfg, plan = compile_process_ir_v1(parse_process_ir_v1(doc), symbols)
    call = cfg.nodes[1]
    assert call.semantic.role == "downstream"
    assert plan.nodes[1].emitter_input.emitter_kind == "connectoraction_target"
    forged = cfg.model_copy(update={"nodes": (cfg.nodes[0], call.model_copy(update={
        "semantic": call.semantic.model_copy(update={"role": "entry"})}),)
        + tuple(cfg.nodes[2:])})
    with pytest.raises(ProcessIRCompileError) as excinfo:
        check_cfg_invariants(forged)
    assert excinfo.value.diagnostics[0].code == PROCESS_IR_COMPILE_INTERNAL


# ---------------------------------------------------------------------------
# The shared plan invariants, on a LISTENER root (#158 ARCH-158-r1-04)
# ---------------------------------------------------------------------------
#
# Acceptance criterion 2 requires an adversarial per invariant SITE on the
# relaxed entry, and the counterparts in `test_process_ir_compiler_invariants`
# run on scheduled roots only — so nothing proved these still refuse once the
# entry is a fused listener. Each mutation below is independent: it perturbs one
# fact and nothing else, so no earlier site can mask the one under test.


def _body_wire(plan, node_index=1, **update):
    """The listener plan with one body node's single outgoing wire perturbed."""
    node = plan.nodes[node_index]
    wire = node.outgoing[0].model_copy(update=update)
    mutated = node.model_copy(update={"outgoing": (wire,)})
    return plan.model_copy(
        update={
            "nodes": plan.nodes[:node_index] + (mutated,) + plan.nodes[node_index + 1:]
        }
    )


def test_listener_plan_rejects_a_duplicate_shape_id():
    cfg, plan, symbols = _compiled(_listener_doc())
    duplicated = plan.nodes[2].model_copy(update={"shape_id": plan.nodes[1].shape_id})
    broken = plan.model_copy(update={"nodes": plan.nodes[:2] + (duplicated,) + plan.nodes[3:]})
    assert _refusal(broken, cfg, symbols).code == _PLAN_INVALID


def test_listener_plan_rejects_noncontiguous_ordinals():
    cfg, plan, symbols = _compiled(_listener_doc())
    shifted = plan.nodes[1].model_copy(update={"ordinal": plan.nodes[-1].ordinal + 3})
    broken = plan.model_copy(update={"nodes": (plan.nodes[0], shifted) + plan.nodes[2:]})
    assert _refusal(broken, cfg, symbols).code == PROCESS_IR_COMPILE_NONDETERMINISTIC


def test_listener_plan_rejects_a_dangling_transition():
    cfg, plan, symbols = _compiled(_listener_doc())
    assert _refusal(_body_wire(plan, to_shape_id="shape99"), cfg, symbols).code == _PLAN_INVALID


def test_listener_plan_rejects_a_wrong_dragpoint_name():
    cfg, plan, symbols = _compiled(_listener_doc())
    wire = plan.nodes[1].outgoing[0]
    assert _refusal(
        _body_wire(plan, dragpoint_name=wire.dragpoint_name + "7"), cfg, symbols
    ).code == _PLAN_INVALID


def test_listener_plan_rejects_a_wrong_dragpoint_row():
    cfg, plan, symbols = _compiled(_listener_doc())
    wire = plan.nodes[1].outgoing[0]
    assert _refusal(_body_wire(plan, y=wire.y + 999.0), cfg, symbols).code == _PLAN_INVALID


def test_listener_plan_rejects_a_duplicate_local_ordinal():
    cfg, plan, symbols = _compiled(_listener_doc())
    node = plan.nodes[1]
    wire = node.outgoing[0]
    twice = node.model_copy(update={"outgoing": (wire, wire.model_copy())})
    broken = plan.model_copy(update={"nodes": (plan.nodes[0], twice) + plan.nodes[2:]})
    assert _refusal(broken, cfg, symbols).code in (
        _PLAN_INVALID,
        PROCESS_IR_COMPILE_NONDETERMINISTIC,
    )


def test_listener_start_claiming_authored_provenance_is_rejected():
    """The provenance site on its OWN mutation. The case inside the form test
    changes `origin` to `ir` as well, so the form site answers first and the
    provenance check is never reached — a masked site is an unproven one."""
    cfg, plan, symbols = _compiled(_listener_doc())
    start = plan.nodes[0]
    assert (start.origin, start.synthetic_role, start.source_path) == (
        "synthetic", "start", None,
    )
    # ONLY the provenance moves: the Start stays synthetic, in its role, with
    # the emitter input and wiring the policy derived.
    assert _refusal(
        _with_start(plan, source_path="/body/steps/0"), cfg, symbols
    ).code == _PLAN_INVALID


def _perturbed(value):
    """A wrong-but-well-typed value for `value`, or None when there is none."""
    if isinstance(value, bool) or value is None:
        return None
    if isinstance(value, float):
        return value + 999.0
    if isinstance(value, int):
        return value + 3
    if isinstance(value, str):
        return value + "X"
    return None


def _borrowed_wire(field, nodes, node_index, wire_index):
    """The value this wire field holds on ANOTHER wire of the same plan, or None.

    The wire half of `_borrowed` (#158 ARCH-158-r3-01): the synthetic Start's
    wire carries no `cfg_edge_id`, so a type-only perturbation skipped it and
    the one hand-written case moved provenance AND the edge id together — the
    provenance check answered first and the edge-id guard stayed unproven.
    """
    for other_index, node in enumerate(nodes):
        for other_wire, wire in enumerate(node.outgoing):
            if (other_index, other_wire) == (node_index, wire_index):
                continue
            value = getattr(wire, field, None)
            if value is not None and not isinstance(value, (list, tuple)):
                return value
    return None


def _borrowed(field, nodes, index):
    """The value this field holds on ANOTHER node of the same plan, or None.

    A field that is None on one node — the synthetic Start carries no
    `cfg_node_id` and no `source_path`, a body node carries no
    `synthetic_role` — has no "wrong but well-typed" value of its own, and
    skipping it left exactly the guards ARCH-158-r2-04 named unproven. The
    plan itself supplies one: what the field holds where it IS set is a value
    the model admits, and claiming it here is the forgery under test.
    """
    for other, node in enumerate(nodes):
        if other == index:
            continue
        value = getattr(node, field, None)
        if value is not None and not isinstance(value, (list, tuple)):
            return value
    return None


def _plan_field_cases():
    """Every scalar field of the plan's node and wire models, on the synthetic
    Start and on a body node of a LISTENER plan — the adversarial set DERIVED
    from the models rather than hand-picked (#158 ARCH-158-r2-04: mutating only
    dragpoint `y` and only the Start's `source_path` left the dragpoint-`x` and
    `cfg_node_id` guards unproven, and all 23 cases passed with either removed).
    """
    cfg, plan, symbols = _compiled(_listener_doc())
    cases = []
    for label, index in (("start", 0), ("body", 1)):
        node = plan.nodes[index]
        for name in sorted(type(node).model_fields):
            value = getattr(node, name)
            if name == "layout":
                for axis in sorted(type(value).model_fields):
                    new_axis = _perturbed(getattr(value, axis))
                    if new_axis is not None:
                        cases.append(("{0}.layout.{1}".format(label, axis), index, "layout",
                                      value.model_copy(update={axis: new_axis})))
                continue
            new = _perturbed(value)
            if new is None and value is None and name not in ("outgoing", "emitter_input"):
                new = _borrowed(name, plan.nodes, index)
            if new is not None:
                cases.append(("{0}.{1}".format(label, name), index, name, new))
        for wire_index, wire in enumerate(node.outgoing):
            for name in sorted(type(wire).model_fields):
                value = getattr(wire, name)
                new = _perturbed(value)
                if new is None and value is None:
                    new = _borrowed_wire(name, plan.nodes, index, wire_index)
                if new is None:
                    continue
                wires = tuple(
                    w.model_copy(update={name: new}) if i == wire_index else w
                    for i, w in enumerate(node.outgoing)
                )
                cases.append(
                    ("{0}.wire{1}.{2}".format(label, wire_index, name), index, "outgoing", wires)
                )
    return cfg, plan, symbols, cases


_CFG, _PLAN, _SYMBOLS, _FIELD_CASES = _plan_field_cases()


@pytest.mark.parametrize(
    "label,index,field,value",
    _FIELD_CASES,
    ids=[case[0] for case in _FIELD_CASES],
)
def test_every_plan_field_of_a_listener_root_refuses_a_perturbation(label, index, field, value):
    """Acceptance criterion 2, per FIELD rather than per hand-picked example:
    every caller-authored geometry, wiring, identity and provenance value on a
    fused listener entry — and on a body shape beside it — is re-derived by the
    compiler, so perturbing any one of them alone is refused."""
    node = _PLAN.nodes[index].model_copy(update={field: value})
    nodes = _PLAN.nodes[:index] + (node,) + _PLAN.nodes[index + 1:]
    diagnostic = _refusal(_PLAN.model_copy(update={"nodes": nodes}), _CFG, _SYMBOLS)
    assert diagnostic.code in (_PLAN_INVALID, PROCESS_IR_COMPILE_NONDETERMINISTIC), (
        label, diagnostic.code, diagnostic.message,
    )
