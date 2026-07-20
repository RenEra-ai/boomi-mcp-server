"""Issue #137 (M12.2): compiler invariant checker + diagnostic contract.

Every test here hand-builds a MALFORMED internal record. That is the point: the
checkers exist to catch a compiler defect, so they must be exercised against
records the lowering passes would never produce, and they must reject rather
than crash.

Each assertion pins the exact ``(code, phase)`` pair, because the
``PROCESS_IR_SEMANTIC_*`` vs ``PROCESS_IR_COMPILE_*`` split is a contract: it
tells a caller whether to fix their payload or file a compiler bug.
"""

import sys
from pathlib import Path

import pytest

_src = str(Path(__file__).resolve().parent.parent / "src")
if _src not in sys.path:
    sys.path.insert(0, _src)

from boomi_mcp.compiler.process_ir import (
    CfgEdgeV1,
    CfgNodeV1,
    ComponentSymbolV1,
    EmissionLayoutV1,
    EmissionNodeV1,
    EmissionPlanV1,
    EmissionTransitionV1,
    ProcessIRCompileError,
    SemanticCfgV1,
    SymbolTableV1,
    check_cfg_invariants,
    check_emission_plan_invariants,
    node_identity_for,
)
from boomi_mcp.compiler.process_ir.contracts import (
    DECISION_FALSE_DRAGPOINT_Y,
    DRAGPOINT_Y,
    SHAPE_Y,
    START_SHAPE_X,
    START_SHAPE_Y,
    BranchInputV1,
    BranchSemanticV1,
    ConnectorSemanticV1,
    DecisionInputV1,
    DecisionSemanticV1,
    MessageInputV1,
    MessageSemanticV1,
    StartNoActionInputV1,
    StopInputV1,
    StopSemanticV1,
    dragpoint_x,
)
from boomi_mcp.errors import (
    PROCESS_IR_COMPILE_EMISSION_PLAN_INVALID,
    PROCESS_IR_COMPILE_INTERNAL,
    PROCESS_IR_COMPILE_NONDETERMINISTIC,
    PROCESS_IR_SEMANTIC_AMBIGUOUS_FLOW,
    PROCESS_IR_SEMANTIC_MISSING_TERMINAL,
    PROCESS_IR_SEMANTIC_UNREACHABLE,
)

SENTINEL = "SENTINEL-SECRET-VALUE-DO-NOT-LEAK"


# ---------------------------------------------------------------------------
# Builders for well-formed records (each test then breaks exactly one thing)
# ---------------------------------------------------------------------------


def _msg(text="hello"):
    return MessageSemanticV1(text=text)


def _node(ordinal, semantic=None, path=None, exit_role=None):
    return CfgNodeV1(
        node_id="n{0}".format(ordinal),
        ordinal=ordinal,
        source_path=path or "/body/steps/{0}".format(ordinal - 1),
        semantic=semantic or _msg(),
        exit_role=exit_role,
    )


def _edge(ordinal, source, target, kind="ordering", local=1, **kwargs):
    return CfgEdgeV1(
        edge_id="e{0}".format(ordinal),
        ordinal=ordinal,
        source_node_id="n{0}".format(source),
        target_node_id="n{0}".format(target),
        kind=kind,
        local_ordinal=local,
        provenance_path="/body/steps/{0}".format(target - 1),
        **kwargs
    )


def _linear_cfg():
    """message -> stop. The smallest valid CFG."""
    return SemanticCfgV1(
        entry_node_id="n1",
        nodes=(_node(1), _node(2, StopSemanticV1(), exit_role="stop")),
        edges=(_edge(1, 1, 2, kind="terminal"),),
        exit_node_ids=("n2",),
    )


def _decision_cfg():
    """decision -> (true: stop, false: stop)."""
    return SemanticCfgV1(
        entry_node_id="n1",
        nodes=(
            _node(
                1,
                DecisionSemanticV1(
                    comparison="equals",
                    left={"value_type": "static", "static_value": "a"},
                    right={"value_type": "static", "static_value": "b"},
                ),
            ),
            _node(2, StopSemanticV1(), path="/body/steps/0/true_arm/terminal", exit_role="stop"),
            _node(3, StopSemanticV1(), path="/body/steps/0/false_arm/terminal", exit_role="stop"),
        ),
        edges=(
            _edge(1, 1, 2, kind="decision_outcome", local=1, outcome="true"),
            _edge(2, 1, 3, kind="decision_outcome", local=2, outcome="false"),
        ),
        exit_node_ids=("n2", "n3"),
    )


def _branch_cfg(leg_count=2):
    nodes = [_node(1, BranchSemanticV1(leg_count=leg_count))]
    edges = []
    for leg in range(leg_count):
        ordinal = 2 + leg
        nodes.append(
            _node(
                ordinal,
                StopSemanticV1(),
                path="/body/steps/0/legs/{0}/terminal".format(leg),
                exit_role="stop",
            )
        )
        edges.append(
            _edge(
                leg + 1,
                1,
                ordinal,
                kind="branch_leg",
                local=leg + 1,
                leg_ordinal=leg + 1,
            )
        )
    return SemanticCfgV1(
        entry_node_id="n1",
        nodes=tuple(nodes),
        edges=tuple(edges),
        exit_node_ids=tuple("n{0}".format(2 + leg) for leg in range(leg_count)),
    )


def _plan_node(ordinal, emitter=None, *, cfg_node=None, path=None, role=None, out=()):
    origin = "ir" if cfg_node else "synthetic"
    return EmissionNodeV1(
        ordinal=ordinal,
        shape_id="shape{0}".format(ordinal),
        cfg_node_id=cfg_node,
        source_path=path,
        origin=origin,
        synthetic_role=role,
        emitter_input=emitter or MessageInputV1(text="hello"),
        layout=EmissionLayoutV1(
            x=START_SHAPE_X if role == "start" else 96.0 + (ordinal - 1) * 160.0,
            y=START_SHAPE_Y if role == "start" else SHAPE_Y,
        ),
        outgoing=tuple(out),
    )


def _wire(source_ordinal, local, target, **kwargs):
    provenance = kwargs.pop("provenance", "cfg_edge")
    # A cfg_edge-provenance transition must name the CFG edge it came from; the
    # checker verifies the edge exists, leaves this node, and targets this shape.
    cfg_edge = kwargs.pop("cfg_edge_id", "e1" if provenance == "cfg_edge" else None)
    return EmissionTransitionV1(
        local_ordinal=local,
        dragpoint_name="shape{0}.dragpoint{1}".format(source_ordinal, local),
        to_shape_id="shape{0}".format(target),
        x=dragpoint_x(source_ordinal),
        y=kwargs.pop("y", DRAGPOINT_Y),
        provenance=provenance,
        cfg_edge_id=cfg_edge,
        **kwargs
    )


def _linear_plan():
    """Matches ``_linear_cfg``: start -> message -> stop."""
    return EmissionPlanV1(
        entry_shape_id="shape1",
        nodes=(
            _plan_node(
                1,
                StartNoActionInputV1(),
                role="start",
                out=[_wire(1, 1, 2, provenance="synthetic")],
            ),
            _plan_node(
                2,
                MessageInputV1(text="hello"),
                cfg_node="n1",
                path="/body/steps/0",
                out=[_wire(2, 1, 3)],
            ),
            _plan_node(3, StopInputV1(), cfg_node="n2", path="/body/steps/1"),
        ),
        terminal_shape_ids=("shape3",),
    )


def _raises(callable_, *args):
    with pytest.raises(ProcessIRCompileError) as excinfo:
        callable_(*args)
    return excinfo.value.diagnostics[0]


# ---------------------------------------------------------------------------
# Baselines
# ---------------------------------------------------------------------------


def test_well_formed_records_pass():
    check_cfg_invariants(_linear_cfg())
    check_cfg_invariants(_decision_cfg())
    check_cfg_invariants(_branch_cfg())
    plan, cfg = _linear_plan(), _linear_cfg()
    check_emission_plan_invariants(plan, cfg, SymbolTableV1(symbols=()))


# ---------------------------------------------------------------------------
# CFG failure classes
# ---------------------------------------------------------------------------


def test_duplicate_node_id_is_a_compiler_defect():
    cfg = _linear_cfg().model_copy(
        update={"nodes": (_node(1), _node(1, StopSemanticV1(), path="/x", exit_role="stop"))}
    )
    diagnostic = _raises(check_cfg_invariants, cfg)
    assert diagnostic.code == PROCESS_IR_COMPILE_INTERNAL
    assert diagnostic.phase == "semantic_lowering"


def test_duplicate_source_path_is_a_compiler_defect():
    cfg = _linear_cfg().model_copy(
        update={
            "nodes": (
                _node(1, path="/body/steps/0"),
                _node(2, StopSemanticV1(), path="/body/steps/0", exit_role="stop"),
            )
        }
    )
    assert _raises(check_cfg_invariants, cfg).code == PROCESS_IR_COMPILE_INTERNAL


def test_dangling_edge_endpoint_is_a_compiler_defect():
    cfg = _linear_cfg().model_copy(update={"edges": (_edge(1, 1, 9),)})
    assert _raises(check_cfg_invariants, cfg).code == PROCESS_IR_COMPILE_INTERNAL


def test_noncontiguous_node_ordinals_are_nondeterministic():
    cfg = _linear_cfg().model_copy(
        update={
            "nodes": (
                _node(1),
                CfgNodeV1(
                    node_id="n3",
                    ordinal=3,
                    source_path="/body/steps/1",
                    semantic=StopSemanticV1(),
                    exit_role="stop",
                ),
            )
        }
    )
    assert _raises(check_cfg_invariants, cfg).code == PROCESS_IR_COMPILE_NONDETERMINISTIC


def test_noncanonical_edge_order_is_nondeterministic():
    base = _decision_cfg()
    swapped = (base.edges[1].model_copy(update={"edge_id": "e1", "ordinal": 1}),
               base.edges[0].model_copy(update={"edge_id": "e2", "ordinal": 2}))
    # Same source node, so the break is purely the local-ordinal order.
    cfg = base.model_copy(update={"edges": swapped})
    diagnostic = _raises(check_cfg_invariants, cfg)
    assert diagnostic.code == PROCESS_IR_COMPILE_NONDETERMINISTIC


def test_unreachable_node_is_a_semantic_defect():
    """Isolate unreachability from the multiple-entry check.

    A plain disconnected node would have zero inbound edges and trip the
    "exactly one entry" rule first. So the island here is a 2-cycle: every
    island node has an inbound edge, leaving n1 as the sole entry, and the
    island is simply never reached from it.
    """
    cfg = SemanticCfgV1(
        entry_node_id="n1",
        nodes=(
            _node(1, StopSemanticV1(), exit_role="stop"),
            _node(2, path="/body/steps/1"),
            _node(3, path="/body/steps/2"),
        ),
        edges=(_edge(1, 2, 3), _edge(2, 3, 2)),
        exit_node_ids=("n1",),
    )
    assert _raises(check_cfg_invariants, cfg).code == PROCESS_IR_SEMANTIC_UNREACHABLE


def test_backward_edge_is_ambiguous_flow():
    """A backward-ordered graph passes entry/join/cycle/reachability but is invalid.

    ``n1 -> n3 -> n2`` has one entry, no join, no cycle, and full reachability —
    yet plan shape ordinals follow CFG ordinals, so it would wire shape4 back to
    shape3. V1 control flow is forward-only.
    """
    cfg = SemanticCfgV1(
        entry_node_id="n1",
        nodes=(
            _node(1),
            _node(2, StopSemanticV1(), path="/body/steps/1", exit_role="stop"),
            _node(3, path="/body/steps/2"),
        ),
        edges=(_edge(1, 1, 3), _edge(2, 3, 2, kind="terminal")),
        exit_node_ids=("n2",),
    )
    assert _raises(check_cfg_invariants, cfg).code == PROCESS_IR_SEMANTIC_AMBIGUOUS_FLOW


def test_duplicate_local_ordinals_on_one_source_are_nondeterministic():
    """Sorted order alone accepts two edges sharing (source, local_ordinal)."""
    base = _branch_cfg(2)
    edges = (
        base.edges[0],
        base.edges[1].model_copy(update={"local_ordinal": 1}),
    )
    cfg = base.model_copy(update={"edges": edges})
    assert _raises(check_cfg_invariants, cfg).code == PROCESS_IR_COMPILE_NONDETERMINISTIC


def test_stop_semantics_must_carry_the_stop_exit_role():
    """A Stop with ``exit_role=None`` would read as a linear node with a successor."""
    cfg = SemanticCfgV1(
        entry_node_id="n1",
        nodes=(
            _node(1, StopSemanticV1()),
            _node(2, StopSemanticV1(), path="/body/steps/1", exit_role="stop"),
        ),
        edges=(_edge(1, 1, 2, kind="terminal"),),
        exit_node_ids=("n2",),
    )
    assert _raises(check_cfg_invariants, cfg).code == PROCESS_IR_SEMANTIC_AMBIGUOUS_FLOW


def test_non_terminal_semantics_may_not_claim_an_exit_role():
    """A Message marked ``exit_role="stop"`` must not read as a terminal."""
    cfg = SemanticCfgV1(
        entry_node_id="n1",
        nodes=(
            _node(1),
            _node(2, _msg(), path="/body/steps/1", exit_role="stop"),
        ),
        edges=(_edge(1, 1, 2, kind="terminal"),),
        exit_node_ids=("n2",),
    )
    assert _raises(check_cfg_invariants, cfg).code == PROCESS_IR_SEMANTIC_AMBIGUOUS_FLOW


def test_zero_entries_is_ambiguous_flow():
    cfg = SemanticCfgV1(
        entry_node_id="n1",
        nodes=(_node(1), _node(2, StopSemanticV1(), exit_role="stop")),
        edges=(_edge(1, 1, 2, kind="terminal"), _edge(2, 2, 1)),
        exit_node_ids=("n2",),
    )
    assert _raises(check_cfg_invariants, cfg).code == PROCESS_IR_SEMANTIC_AMBIGUOUS_FLOW


def test_multiple_entries_is_ambiguous_flow():
    cfg = SemanticCfgV1(
        entry_node_id="n1",
        nodes=(
            _node(1, StopSemanticV1(), exit_role="stop"),
            _node(2, StopSemanticV1(), path="/body/steps/1", exit_role="stop"),
        ),
        edges=(),
        exit_node_ids=("n1", "n2"),
    )
    assert _raises(check_cfg_invariants, cfg).code == PROCESS_IR_SEMANTIC_AMBIGUOUS_FLOW


def test_join_is_ambiguous_flow():
    cfg = SemanticCfgV1(
        entry_node_id="n1",
        nodes=(
            _node(1, BranchSemanticV1(leg_count=2)),
            _node(2, path="/body/steps/0/legs/0/terminal"),
            _node(3, StopSemanticV1(), path="/body/steps/0/legs/1/terminal", exit_role="stop"),
        ),
        edges=(
            _edge(1, 1, 2, kind="branch_leg", local=1, leg_ordinal=1),
            _edge(2, 1, 3, kind="branch_leg", local=2, leg_ordinal=2),
            _edge(3, 2, 3, kind="terminal"),
        ),
        exit_node_ids=("n3",),
    )
    assert _raises(check_cfg_invariants, cfg).code == PROCESS_IR_SEMANTIC_AMBIGUOUS_FLOW


def test_flow_past_a_terminal_is_ambiguous_flow():
    cfg = SemanticCfgV1(
        entry_node_id="n1",
        nodes=(
            _node(1, StopSemanticV1(), exit_role="stop"),
            _node(2, StopSemanticV1(), path="/body/steps/1", exit_role="stop"),
        ),
        edges=(_edge(1, 1, 2, kind="terminal"),),
        exit_node_ids=("n1", "n2"),
    )
    assert _raises(check_cfg_invariants, cfg).code == PROCESS_IR_SEMANTIC_AMBIGUOUS_FLOW


def test_missing_terminal_is_a_semantic_defect():
    cfg = SemanticCfgV1(
        entry_node_id="n1",
        nodes=(_node(1), _node(2, path="/body/steps/1")),
        edges=(_edge(1, 1, 2),),
        exit_node_ids=(),
    )
    assert _raises(check_cfg_invariants, cfg).code == PROCESS_IR_SEMANTIC_MISSING_TERMINAL


def test_reserved_catch_edge_is_rejected():
    cfg = _linear_cfg().model_copy(
        update={"edges": (_edge(1, 1, 2, kind="catch"),)}
    )
    assert _raises(check_cfg_invariants, cfg).code == PROCESS_IR_SEMANTIC_AMBIGUOUS_FLOW


def test_decision_with_wrong_outcome_order_is_nondeterministic():
    base = _decision_cfg()
    edges = (
        base.edges[0].model_copy(update={"outcome": "false"}),
        base.edges[1].model_copy(update={"outcome": "true"}),
    )
    cfg = base.model_copy(update={"edges": edges})
    assert _raises(check_cfg_invariants, cfg).code == PROCESS_IR_COMPILE_NONDETERMINISTIC


def test_decision_with_one_outcome_is_ambiguous_flow():
    base = _decision_cfg()
    cfg = base.model_copy(
        update={
            "nodes": base.nodes[:2],
            "edges": (base.edges[0],),
            "exit_node_ids": ("n2",),
        }
    )
    assert _raises(check_cfg_invariants, cfg).code == PROCESS_IR_SEMANTIC_AMBIGUOUS_FLOW


def test_branch_leg_count_mismatch_is_a_compiler_defect():
    base = _branch_cfg(2)
    cfg = base.model_copy(
        update={"nodes": (_node(1, BranchSemanticV1(leg_count=3)),) + base.nodes[1:]}
    )
    assert _raises(check_cfg_invariants, cfg).code == PROCESS_IR_COMPILE_INTERNAL


def test_branch_legs_out_of_order_are_nondeterministic():
    base = _branch_cfg(2)
    edges = (
        base.edges[0].model_copy(update={"leg_ordinal": 2}),
        base.edges[1].model_copy(update={"leg_ordinal": 1}),
    )
    cfg = base.model_copy(update={"edges": edges})
    assert _raises(check_cfg_invariants, cfg).code == PROCESS_IR_COMPILE_NONDETERMINISTIC


def test_declared_exits_must_match_actual_exit_roles():
    cfg = _linear_cfg().model_copy(update={"exit_node_ids": ("n1", "n2")})
    assert _raises(check_cfg_invariants, cfg).code == PROCESS_IR_COMPILE_INTERNAL


def test_declared_entry_must_be_the_real_entry():
    cfg = _linear_cfg().model_copy(update={"entry_node_id": "n2"})
    assert _raises(check_cfg_invariants, cfg).code == PROCESS_IR_COMPILE_INTERNAL


# ---------------------------------------------------------------------------
# Emission-plan failure classes
# ---------------------------------------------------------------------------


def _check_plan(plan, cfg=None):
    return _raises(
        check_emission_plan_invariants, plan, cfg or _linear_cfg(), SymbolTableV1(symbols=())
    )


def test_plan_without_a_synthetic_start_is_invalid():
    base = _linear_plan()
    broken = base.model_copy(
        update={
            "nodes": (
                _plan_node(
                    1,
                    MessageInputV1(text="x"),
                    cfg_node="n1",
                    path="/body/steps/0",
                    out=[_wire(1, 1, 2)],
                ),
            )
            + base.nodes[1:]
        }
    )
    assert _check_plan(broken).code == PROCESS_IR_COMPILE_EMISSION_PLAN_INVALID


def test_duplicate_shape_id_is_rejected():
    base = _linear_plan()
    duplicated = base.nodes[2].model_copy(update={"shape_id": "shape2"})
    broken = base.model_copy(update={"nodes": base.nodes[:2] + (duplicated,)})
    assert _check_plan(broken).code == PROCESS_IR_COMPILE_EMISSION_PLAN_INVALID


def test_noncontiguous_plan_ordinals_are_nondeterministic():
    base = _linear_plan()
    shifted = base.nodes[2].model_copy(update={"ordinal": 4, "shape_id": "shape4"})
    broken = base.model_copy(
        update={"nodes": base.nodes[:2] + (shifted,), "terminal_shape_ids": ("shape4",)}
    )
    assert _check_plan(broken).code == PROCESS_IR_COMPILE_NONDETERMINISTIC


def test_dangling_plan_transition_is_rejected():
    base = _linear_plan()
    broken_node = base.nodes[1].model_copy(update={"outgoing": (_wire(2, 1, 9),)})
    broken = base.model_copy(
        update={"nodes": (base.nodes[0], broken_node, base.nodes[2])}
    )
    assert _check_plan(broken).code == PROCESS_IR_COMPILE_EMISSION_PLAN_INVALID


def test_wrong_dragpoint_name_is_rejected():
    base = _linear_plan()
    bad_wire = base.nodes[1].outgoing[0].model_copy(
        update={"dragpoint_name": "shape2.dragpoint7"}
    )
    node = base.nodes[1].model_copy(update={"outgoing": (bad_wire,)})
    broken = base.model_copy(update={"nodes": (base.nodes[0], node, base.nodes[2])})
    assert _check_plan(broken).code == PROCESS_IR_COMPILE_EMISSION_PLAN_INVALID


def test_wrong_geometry_is_rejected():
    base = _linear_plan()
    node = base.nodes[1].model_copy(update={"layout": EmissionLayoutV1(x=1.0, y=2.0)})
    broken = base.model_copy(update={"nodes": (base.nodes[0], node, base.nodes[2])})
    assert _check_plan(broken).code == PROCESS_IR_COMPILE_EMISSION_PLAN_INVALID


def test_wrong_dragpoint_row_is_rejected():
    base = _linear_plan()
    bad_wire = base.nodes[1].outgoing[0].model_copy(update={"y": 999.0})
    node = base.nodes[1].model_copy(update={"outgoing": (bad_wire,)})
    broken = base.model_copy(update={"nodes": (base.nodes[0], node, base.nodes[2])})
    assert _check_plan(broken).code == PROCESS_IR_COMPILE_EMISSION_PLAN_INVALID


def test_terminal_with_outbound_transition_is_ambiguous_flow():
    base = _linear_plan()
    terminal = base.nodes[2].model_copy(update={"outgoing": (_wire(3, 1, 2),)})
    broken = base.model_copy(update={"nodes": base.nodes[:2] + (terminal,)})
    assert _check_plan(broken).code == PROCESS_IR_SEMANTIC_AMBIGUOUS_FLOW


def test_synthetic_node_claiming_authored_provenance_is_rejected():
    base = _linear_plan()
    bad = base.nodes[0].model_copy(update={"source_path": "/body/steps/0"})
    broken = base.model_copy(update={"nodes": (bad,) + base.nodes[1:]})
    assert _check_plan(broken).code == PROCESS_IR_COMPILE_EMISSION_PLAN_INVALID


def test_plan_cfg_correspondence_is_enforced():
    """Dropping a plan node breaks the one-to-one CFG correspondence.

    Pinned to the EXACT code rather than "either of two": an assertion that
    accepts two codes cannot tell a correspondence failure from a missing
    terminal, so it would keep passing if the wrong check fired.
    """
    base = _linear_plan()
    broken = base.model_copy(update={"nodes": base.nodes[:2], "terminal_shape_ids": ()})
    diagnostic = _check_plan(broken)
    assert diagnostic.code == PROCESS_IR_COMPILE_EMISSION_PLAN_INVALID
    assert diagnostic.phase == "emission_planning"
    assert diagnostic.message == (
        "plan nodes do not correspond one-to-one with CFG nodes in order"
    )


def test_diagnostics_pin_every_contract_field():
    """The plan requires code, phase, path, identity, message, remediation, id."""
    cfg = _linear_cfg().model_copy(
        update={
            "nodes": (
                _node(1),
                _node(2, _msg(), path="/body/steps/1", exit_role="stop"),
            )
        }
    )
    diagnostic = _raises(check_cfg_invariants, cfg)
    assert diagnostic.code == PROCESS_IR_SEMANTIC_AMBIGUOUS_FLOW
    assert diagnostic.phase == "semantic_lowering"
    assert diagnostic.path == "/body/steps/1"
    assert diagnostic.node_identity == "/body/steps/1"
    assert diagnostic.internal_node_id == "n2"
    assert diagnostic.message == "exit role is not valid for this node's semantics"
    assert diagnostic.remediation.startswith("Give the flow exactly one entry")


def test_plan_source_path_disagreeing_with_cfg_is_rejected():
    base = _linear_plan()
    node = base.nodes[1].model_copy(update={"source_path": "/body/steps/99"})
    broken = base.model_copy(update={"nodes": (base.nodes[0], node, base.nodes[2])})
    assert _check_plan(broken).code == PROCESS_IR_COMPILE_EMISSION_PLAN_INVALID


def test_unlabelled_branch_dragpoints_are_rejected():
    cfg = _branch_cfg(2)
    plan = EmissionPlanV1(
        entry_shape_id="shape1",
        nodes=(
            _plan_node(
                1, StartNoActionInputV1(), role="start", out=[_wire(1, 1, 2, provenance="synthetic")]
            ),
            _plan_node(
                2,
                BranchInputV1(num_branches=2),
                cfg_node="n1",
                path="/body/steps/0",
                # Missing identifier/text — the legacy emitter always writes them.
                out=[_wire(2, 1, 3), _wire(2, 2, 4)],
            ),
            _plan_node(3, StopInputV1(), cfg_node="n2", path="/body/steps/0/legs/0/terminal"),
            _plan_node(4, StopInputV1(), cfg_node="n3", path="/body/steps/0/legs/1/terminal"),
        ),
        terminal_shape_ids=("shape3", "shape4"),
    )
    assert _check_plan(plan, cfg).code == PROCESS_IR_COMPILE_EMISSION_PLAN_INVALID


def test_decision_dragpoint_labels_must_be_true_True_then_false_False():
    cfg = _decision_cfg()
    plan = EmissionPlanV1(
        entry_shape_id="shape1",
        nodes=(
            _plan_node(
                1, StartNoActionInputV1(), role="start", out=[_wire(1, 1, 2, provenance="synthetic")]
            ),
            _plan_node(
                2,
                DecisionInputV1(
                    comparison="equals",
                    left={"value_type": "static", "static_value": "a"},
                    right={"value_type": "static", "static_value": "b"},
                ),
                cfg_node="n1",
                path="/body/steps/0",
                out=[
                    # Case swapped: identifier must be lowercase, text title-case.
                    _wire(2, 1, 3, identifier="True", text="true"),
                    _wire(2, 2, 4, identifier="False", text="false", y=DECISION_FALSE_DRAGPOINT_Y),
                ],
            ),
            _plan_node(3, StopInputV1(), cfg_node="n2", path="/body/steps/0/true_arm/terminal"),
            _plan_node(4, StopInputV1(), cfg_node="n3", path="/body/steps/0/false_arm/terminal"),
        ),
        terminal_shape_ids=("shape3", "shape4"),
    )
    assert _check_plan(plan, cfg).code == PROCESS_IR_COMPILE_EMISSION_PLAN_INVALID


# ---------------------------------------------------------------------------
# Diagnostic contract
# ---------------------------------------------------------------------------


def test_every_diagnostic_carries_code_path_identity_and_remediation():
    diagnostic = _raises(check_cfg_invariants, _linear_cfg().model_copy(
        update={"exit_node_ids": ("n1", "n2")}
    ))
    assert diagnostic.code
    assert diagnostic.phase in (
        "schema",
        "reference_resolution",
        "semantic_lowering",
        "emission_planning",
    )
    assert diagnostic.path.startswith("/")
    assert diagnostic.node_identity
    assert diagnostic.message
    assert diagnostic.remediation


@pytest.mark.parametrize(
    "path,expected",
    [
        ("/body/steps/0", "/body/steps/0"),
        ("/body/steps/0/label", "/body/steps/0"),
        ("/body/steps/2/legs/1/steps/0/text", "/body/steps/2/legs/1/steps/0"),
        ("/body/steps/2/true_arm/terminal", "/body/steps/2/true_arm/terminal"),
        ("/body/steps/2/false_arm/terminal/legs/0", "/body/steps/2/false_arm/terminal/legs/0"),
        ("/version", "<root>"),
        ("", "<root>"),
    ],
)
def test_node_identity_is_the_nearest_authored_node(path, expected):
    assert node_identity_for(path) == expected


def test_diagnostics_sort_by_phase_then_path_then_code():
    from boomi_mcp.compiler.process_ir.diagnostics import diagnostic, sorted_diagnostics

    unsorted_ = [
        diagnostic(PROCESS_IR_COMPILE_INTERNAL, "emission_planning", "/body/steps/1"),
        diagnostic(PROCESS_IR_SEMANTIC_UNREACHABLE, "semantic_lowering", "/body/steps/2"),
        diagnostic(PROCESS_IR_SEMANTIC_UNREACHABLE, "semantic_lowering", "/body/steps/0"),
    ]
    ordered = sorted_diagnostics(unsorted_)
    assert [item.phase for item in ordered] == [
        "semantic_lowering",
        "semantic_lowering",
        "emission_planning",
    ]
    assert [item.path for item in ordered[:2]] == ["/body/steps/0", "/body/steps/2"]


def test_no_authored_value_leaks_into_a_diagnostic_or_repr():
    """A sentinel seeded into every authored slot must not surface anywhere.

    Diagnostics are logged, so a leaked value is a disclosure. This checks the
    diagnostic fields, the exception's ``str``/``repr``, and the model ``repr``
    that ``_CompilerModel.__repr_args__`` is responsible for suppressing.
    """
    cfg = SemanticCfgV1(
        entry_node_id="n1",
        nodes=(
            _node(1, MessageSemanticV1(text=SENTINEL, label=SENTINEL)),
            _node(
                2,
                ConnectorSemanticV1(
                    role="target",
                    connection_ref=SENTINEL,
                    operation_ref=SENTINEL,
                    label=SENTINEL,
                ),
                path="/body/steps/1",
            ),
        ),
        edges=(_edge(1, 1, 2),),
        exit_node_ids=(),
    )
    with pytest.raises(ProcessIRCompileError) as excinfo:
        check_cfg_invariants(cfg)
    error = excinfo.value

    assert SENTINEL not in str(error)
    assert SENTINEL not in repr(error)
    for item in error.diagnostics:
        for value in (item.code, item.path, item.node_identity, item.message, item.remediation):
            assert SENTINEL not in value

    # And the contracts themselves suppress values in repr.
    assert SENTINEL not in repr(cfg.nodes[0])
    assert SENTINEL not in repr(cfg.nodes[1].semantic)
    assert SENTINEL not in repr(cfg)


def test_symbols_never_leak_into_a_diagnostic():
    cfg = _linear_cfg()
    symbols = SymbolTableV1(
        symbols=(
            ComponentSymbolV1(
                ref="$ref:a",
                component_id=SENTINEL,
                component_type="t",
                connector_type=SENTINEL,
                action_type=SENTINEL,
            ),
        )
    )
    assert SENTINEL not in repr(symbols)
    broken = _linear_plan().model_copy(update={"terminal_shape_ids": ("shape9",)})
    with pytest.raises(ProcessIRCompileError) as excinfo:
        check_emission_plan_invariants(broken, cfg, symbols)
    assert SENTINEL not in str(excinfo.value)


def test_generated_identities_are_positional_not_derived_from_values():
    """Ids must not encode a secret — they are pure ordinals."""
    cfg = SemanticCfgV1(
        entry_node_id="n1",
        nodes=(
            _node(1, MessageSemanticV1(text=SENTINEL)),
            _node(2, StopSemanticV1(), path="/body/steps/1", exit_role="stop"),
        ),
        edges=(_edge(1, 1, 2, kind="terminal"),),
        exit_node_ids=("n2",),
    )
    check_cfg_invariants(cfg)
    assert [node.node_id for node in cfg.nodes] == ["n1", "n2"]
    assert [edge.edge_id for edge in cfg.edges] == ["e1"]


def test_transition_must_match_its_cfg_edge_target():
    """A rewired transition must not pass just because the target shape exists."""
    base = _linear_plan()
    # Turn message->stop into a self-loop on the message shape.
    looped = base.nodes[1].outgoing[0].model_copy(update={"to_shape_id": "shape2"})
    node = base.nodes[1].model_copy(update={"outgoing": (looped,)})
    broken = base.model_copy(update={"nodes": (base.nodes[0], node, base.nodes[2])})
    assert _check_plan(broken).code == PROCESS_IR_COMPILE_EMISSION_PLAN_INVALID


def test_transition_must_reference_an_edge_leaving_this_node():
    base = _linear_plan()
    orphan = base.nodes[1].outgoing[0].model_copy(update={"cfg_edge_id": "e9"})
    node = base.nodes[1].model_copy(update={"outgoing": (orphan,)})
    broken = base.model_copy(update={"nodes": (base.nodes[0], node, base.nodes[2])})
    assert _check_plan(broken).code == PROCESS_IR_COMPILE_EMISSION_PLAN_INVALID


def test_start_must_wire_to_the_cfg_entry_shape():
    base = _linear_plan()
    misrouted = base.nodes[0].outgoing[0].model_copy(update={"to_shape_id": "shape3"})
    node = base.nodes[0].model_copy(update={"outgoing": (misrouted,)})
    broken = base.model_copy(update={"nodes": (node,) + base.nodes[1:]})
    assert _check_plan(broken).code == PROCESS_IR_COMPILE_EMISSION_PLAN_INVALID


def test_emitter_input_must_match_the_cfg_node_semantics():
    """A Map node carrying a MessageInputV1 would serialise the wrong shape."""
    from boomi_mcp.compiler.process_ir.contracts import MapSemanticV1

    cfg = SemanticCfgV1(
        entry_node_id="n1",
        nodes=(
            _node(1, MapSemanticV1(map_ref="$ref:m")),
            _node(2, StopSemanticV1(), path="/body/steps/1", exit_role="stop"),
        ),
        edges=(_edge(1, 1, 2, kind="terminal"),),
        exit_node_ids=("n2",),
    )
    plan = _linear_plan()  # its shape2 carries MessageInputV1, not MapInputV1
    assert (
        _raises(check_emission_plan_invariants, plan, cfg, SymbolTableV1(symbols=())).code
        == PROCESS_IR_COMPILE_EMISSION_PLAN_INVALID
    )


def test_component_id_absent_from_the_symbol_table_is_rejected():
    """The ``symbols`` argument must actually be consulted."""
    from boomi_mcp.compiler.process_ir.contracts import MapInputV1, MapSemanticV1

    cfg = SemanticCfgV1(
        entry_node_id="n1",
        nodes=(
            _node(1, MapSemanticV1(map_ref="$ref:m")),
            _node(2, StopSemanticV1(), path="/body/steps/1", exit_role="stop"),
        ),
        edges=(_edge(1, 1, 2, kind="terminal"),),
        exit_node_ids=("n2",),
    )
    base = _linear_plan()
    mapped = base.nodes[1].model_copy(
        update={"emitter_input": MapInputV1(map_id="never-resolved")}
    )
    plan = base.model_copy(update={"nodes": (base.nodes[0], mapped, base.nodes[2])})

    known = SymbolTableV1(
        symbols=(
            ComponentSymbolV1(ref="$ref:m", component_id="resolved", component_type="t"),
        )
    )
    assert (
        _raises(check_emission_plan_invariants, plan, cfg, known).code
        == PROCESS_IR_COMPILE_EMISSION_PLAN_INVALID
    )
    # And the same plan passes once the id is one the symbol table resolved.
    ok = base.model_copy(
        update={
            "nodes": (
                base.nodes[0],
                base.nodes[1].model_copy(
                    update={"emitter_input": MapInputV1(map_id="resolved")}
                ),
                base.nodes[2],
            )
        }
    )
    check_emission_plan_invariants(ok, cfg, known)


def test_branch_dragpoint_row_is_checked():
    """The Branch block validated labels but never the dragpoint row."""
    cfg = _branch_cfg(2)
    plan = EmissionPlanV1(
        entry_shape_id="shape1",
        nodes=(
            _plan_node(
                1, StartNoActionInputV1(), role="start",
                out=[_wire(1, 1, 2, provenance="synthetic")],
            ),
            _plan_node(
                2,
                BranchInputV1(num_branches=2),
                cfg_node="n1",
                path="/body/steps/0",
                out=[
                    _wire(2, 1, 3, identifier="1", text="1", y=999.0, cfg_edge_id="e1"),
                    _wire(2, 2, 4, identifier="2", text="2", y=999.0, cfg_edge_id="e2"),
                ],
            ),
            _plan_node(3, StopInputV1(), cfg_node="n2", path="/body/steps/0/legs/0/terminal"),
            _plan_node(4, StopInputV1(), cfg_node="n3", path="/body/steps/0/legs/1/terminal"),
        ),
        terminal_shape_ids=("shape3", "shape4"),
    )
    assert _check_plan(plan, cfg).code == PROCESS_IR_COMPILE_EMISSION_PLAN_INVALID


def test_synthetic_stop_must_follow_its_routed_target():
    """Counting synthetic stops is not enough — adjacency is what V1 pins."""
    from boomi_mcp.compiler.process_ir.contracts import (
        ConnectorActionInputV1,
        MessageInputV1 as _Msg,
    )

    cfg = SemanticCfgV1(
        entry_node_id="n1",
        nodes=(
            _node(
                1,
                ConnectorSemanticV1(
                    role="target", connection_ref="$ref:c", operation_ref="$ref:o"
                ),
                exit_role="routed_target",
            ),
        ),
        edges=(),
        exit_node_ids=("n1",),
    )
    # The synthetic stop is present (count matches) but is NOT adjacent: an
    # unrelated shape sits between the routed target and its stop.
    plan = EmissionPlanV1(
        entry_shape_id="shape1",
        nodes=(
            _plan_node(
                1, StartNoActionInputV1(), role="start",
                out=[_wire(1, 1, 2, provenance="synthetic")],
            ),
            _plan_node(
                2,
                ConnectorActionInputV1(
                    emitter_kind="connectoraction_target",
                    connector_type="database",
                    action_type="SEND",
                    connection_id="cid",
                    operation_id="oid",
                ),
                cfg_node="n1",
                path="/body/steps/0",
                out=[_wire(2, 1, 4, provenance="synthetic")],
            ),
            _plan_node(3, _Msg(text="interloper"), role="terminal_stop"),
            _plan_node(4, StopInputV1(), role="terminal_stop"),
        ),
        terminal_shape_ids=("shape4",),
    )
    symbols = SymbolTableV1(
        symbols=(
            ComponentSymbolV1(ref="$ref:c", component_id="cid", component_type="t"),
            # The plan checker now recomputes the emitter input from these, so
            # the operation symbol must carry its derived connector metadata.
            ComponentSymbolV1(
                ref="$ref:o",
                component_id="oid",
                component_type="t",
                connector_type="database",
                action_type="Send",
            ),
        )
    )
    assert (
        _raises(check_emission_plan_invariants, plan, cfg, symbols).code
        == PROCESS_IR_COMPILE_EMISSION_PLAN_INVALID
    )


def test_swapped_decision_transitions_are_rejected():
    """Swapping BOTH cfg_edge_id and to_shape_id leaves each wire self-consistent.

    Each transition still names an edge leaving the node and targets that edge's
    shape — but the dragpoint labels are fixed by POSITION, so True would route
    down the false arm. Only comparing the ORDERED edge-id sequence catches it.
    """
    cfg = _decision_cfg()
    decision_input = DecisionInputV1(
        comparison="equals",
        left={"value_type": "static", "static_value": "a"},
        right={"value_type": "static", "static_value": "b"},
    )

    def _plan(first_edge, first_target, second_edge, second_target):
        return EmissionPlanV1(
            entry_shape_id="shape1",
            nodes=(
                _plan_node(
                    1, StartNoActionInputV1(), role="start",
                    out=[_wire(1, 1, 2, provenance="synthetic")],
                ),
                _plan_node(
                    2, decision_input, cfg_node="n1", path="/body/steps/0",
                    out=[
                        _wire(2, 1, first_target, identifier="true", text="True",
                              cfg_edge_id=first_edge),
                        _wire(2, 2, second_target, identifier="false", text="False",
                              y=DECISION_FALSE_DRAGPOINT_Y, cfg_edge_id=second_edge),
                    ],
                ),
                _plan_node(3, StopInputV1(), cfg_node="n2",
                           path="/body/steps/0/true_arm/terminal"),
                _plan_node(4, StopInputV1(), cfg_node="n3",
                           path="/body/steps/0/false_arm/terminal"),
            ),
            terminal_shape_ids=("shape3", "shape4"),
        )

    # Correct wiring passes.
    check_emission_plan_invariants(
        _plan("e1", 3, "e2", 4), cfg, SymbolTableV1(symbols=())
    )
    # Coherently swapped wiring must NOT.
    swapped = _plan("e2", 4, "e1", 3)
    assert _check_plan(swapped, cfg).code == PROCESS_IR_COMPILE_EMISSION_PLAN_INVALID


def test_ordinary_transition_cannot_hide_behind_synthetic_provenance():
    """Relabelling a real wire as synthetic must not skip CFG correspondence."""
    base = _linear_plan()
    disguised = base.nodes[1].outgoing[0].model_copy(
        update={"provenance": "synthetic", "cfg_edge_id": None}
    )
    node = base.nodes[1].model_copy(update={"outgoing": (disguised,)})
    broken = base.model_copy(update={"nodes": (base.nodes[0], node, base.nodes[2])})
    assert _check_plan(broken).code == PROCESS_IR_COMPILE_EMISSION_PLAN_INVALID


def test_source_endpoint_cannot_be_a_routed_target():
    """``routed_target`` on a SOURCE would append a synthetic Stop after it."""
    cfg = SemanticCfgV1(
        entry_node_id="n1",
        nodes=(
            _node(
                1,
                ConnectorSemanticV1(
                    role="source", connection_ref="$ref:c", operation_ref="$ref:o"
                ),
                path="/body/steps/0/legs/0/terminal",
                exit_role="routed_target",
            ),
        ),
        edges=(),
        exit_node_ids=("n1",),
    )
    assert _raises(check_cfg_invariants, cfg).code == PROCESS_IR_SEMANTIC_AMBIGUOUS_FLOW


def test_routed_target_must_sit_in_a_terminal_position():
    """A ROOT target is followed by an authored Stop and is not itself an exit."""
    cfg = SemanticCfgV1(
        entry_node_id="n1",
        nodes=(
            _node(
                1,
                ConnectorSemanticV1(
                    role="target", connection_ref="$ref:c", operation_ref="$ref:o"
                ),
                path="/body/steps/0",
                exit_role="routed_target",
            ),
        ),
        edges=(),
        exit_node_ids=("n1",),
    )
    assert _raises(check_cfg_invariants, cfg).code == PROCESS_IR_SEMANTIC_AMBIGUOUS_FLOW


def test_synthetic_stop_may_not_carry_outgoing_flow():
    """A terminal_stop wired onward, and omitted from terminal_shape_ids.

    The generic terminal check only inspects shapes that ARE declared terminal,
    so omitting the stop from the declaration used to hide the extra wire.
    """
    from boomi_mcp.compiler.process_ir.contracts import ConnectorActionInputV1

    cfg = SemanticCfgV1(
        entry_node_id="n1",
        nodes=(
            _node(1, BranchSemanticV1(leg_count=2)),
            _node(
                2,
                ConnectorSemanticV1(
                    role="target", connection_ref="$ref:c", operation_ref="$ref:o"
                ),
                path="/body/steps/0/legs/0/terminal",
                exit_role="routed_target",
            ),
            _node(
                3,
                StopSemanticV1(),
                path="/body/steps/0/legs/1/terminal",
                exit_role="stop",
            ),
        ),
        edges=(
            _edge(1, 1, 2, kind="branch_leg", local=1, leg_ordinal=1),
            _edge(2, 1, 3, kind="branch_leg", local=2, leg_ordinal=2),
        ),
        exit_node_ids=("n2", "n3"),
    )
    connector = ConnectorActionInputV1(
        emitter_kind="connectoraction_target",
        connector_type="database",
        action_type="SEND",
        connection_id="cid",
        operation_id="oid",
    )
    symbols = SymbolTableV1(
        symbols=(
            ComponentSymbolV1(ref="$ref:c", component_id="cid", component_type="t"),
            # The plan checker now recomputes the emitter input from these, so
            # the operation symbol must carry its derived connector metadata.
            ComponentSymbolV1(
                ref="$ref:o",
                component_id="oid",
                component_type="t",
                connector_type="database",
                action_type="Send",
            ),
        )
    )

    def _plan(stop_out, terminals):
        return EmissionPlanV1(
            entry_shape_id="shape1",
            nodes=(
                _plan_node(
                    1, StartNoActionInputV1(), role="start",
                    out=[_wire(1, 1, 2, provenance="synthetic")],
                ),
                _plan_node(
                    2, BranchInputV1(num_branches=2), cfg_node="n1",
                    path="/body/steps/0",
                    out=[
                        _wire(2, 1, 3, identifier="1", text="1", cfg_edge_id="e1"),
                        _wire(2, 2, 5, identifier="2", text="2", cfg_edge_id="e2"),
                    ],
                ),
                _plan_node(
                    3, connector, cfg_node="n2",
                    path="/body/steps/0/legs/0/terminal",
                    out=[_wire(3, 1, 4, provenance="synthetic")],
                ),
                _plan_node(4, StopInputV1(), role="terminal_stop", out=stop_out),
                _plan_node(5, StopInputV1(), cfg_node="n3",
                           path="/body/steps/0/legs/1/terminal"),
            ),
            terminal_shape_ids=terminals,
        )

    # Correct: the synthetic stop is inert and declared.
    check_emission_plan_invariants(_plan((), ("shape4", "shape5")), cfg, symbols)
    # Wired onward AND undeclared — must be rejected.
    broken = _plan([_wire(4, 1, 5, provenance="synthetic")], ("shape5",))
    assert (
        _raises(check_emission_plan_invariants, broken, cfg, symbols).code
        == PROCESS_IR_SEMANTIC_AMBIGUOUS_FLOW
    )


def test_routed_target_is_rejected_in_a_decision_false_arm():
    """``DecisionFalseArmV1.terminal`` is Stop/Branch/Exception — never a target.

    A bare "/terminal" suffix test would accept ``/false_arm/terminal`` and the
    planner would append a synthetic Stop on the reject route.
    """
    cfg = SemanticCfgV1(
        entry_node_id="n1",
        nodes=(
            _node(
                1,
                ConnectorSemanticV1(
                    role="target", connection_ref="$ref:c", operation_ref="$ref:o"
                ),
                path="/body/steps/0/false_arm/terminal",
                exit_role="routed_target",
            ),
        ),
        edges=(),
        exit_node_ids=("n1",),
    )
    assert _raises(check_cfg_invariants, cfg).code == PROCESS_IR_SEMANTIC_AMBIGUOUS_FLOW


@pytest.mark.parametrize("path", [
    "/body/steps/0/legs/0/terminal",
    "/body/steps/2/legs/17/terminal",
    "/body/steps/0/true_arm/terminal",
    "/body/steps/1/true_arm/terminal/legs/3/terminal",
])
def test_routed_target_accepted_in_supported_terminal_positions(path):
    cfg = SemanticCfgV1(
        entry_node_id="n1",
        nodes=(
            _node(
                1,
                ConnectorSemanticV1(
                    role="target", connection_ref="$ref:c", operation_ref="$ref:o"
                ),
                path=path,
                exit_role="routed_target",
            ),
        ),
        edges=(),
        exit_node_ids=("n1",),
    )
    check_cfg_invariants(cfg)


def test_plan_validation_scales_linearly_with_node_count():
    """Guard against reintroducing an O(V*E) edge scan in the node loop.

    ``SequenceNodeV1.steps`` has no upper bound, so a per-node rescan of
    ``cfg.edges`` turns a valid large process into hundreds of millions of
    predicates. Doubling the size must roughly double the work, not quadruple it.
    """
    import time

    from boomi_mcp.compiler.process_ir.contracts import MessageInputV1 as _Msg

    def _build(size):
        nodes = [_node(i, _msg("m{0}".format(i)), path="/body/steps/{0}".format(i - 1))
                 for i in range(1, size)]
        nodes.append(
            _node(size, StopSemanticV1(), path="/body/steps/{0}".format(size - 1),
                  exit_role="stop")
        )
        edges = [
            _edge(i, i, i + 1, kind="terminal" if i + 1 == size else "ordering")
            for i in range(1, size)
        ]
        cfg = SemanticCfgV1(
            entry_node_id="n1",
            nodes=tuple(nodes),
            edges=tuple(edges),
            exit_node_ids=("n{0}".format(size),),
        )
        plan_nodes = [
            _plan_node(1, StartNoActionInputV1(), role="start",
                       out=[_wire(1, 1, 2, provenance="synthetic")])
        ]
        for i in range(1, size + 1):
            ordinal = i + 1
            is_last = i == size
            plan_nodes.append(
                _plan_node(
                    ordinal,
                    StopInputV1() if is_last else _Msg(text="m{0}".format(i)),
                    cfg_node="n{0}".format(i),
                    path="/body/steps/{0}".format(i - 1),
                    out=() if is_last else [
                        _wire(ordinal, 1, ordinal + 1, cfg_edge_id="e{0}".format(i))
                    ],
                )
            )
        plan = EmissionPlanV1(
            entry_shape_id="shape1",
            nodes=tuple(plan_nodes),
            terminal_shape_ids=("shape{0}".format(size + 1),),
        )
        return cfg, plan

    symbols = SymbolTableV1(symbols=())

    def _time(size):
        cfg, plan = _build(size)

        def _once():
            start = time.perf_counter()
            check_cfg_invariants(cfg)
            check_emission_plan_invariants(plan, cfg, symbols)
            return time.perf_counter() - start

        # Best-of-3: a single sample was measured at up to 14.1x under parallel
        # load (88% of the threshold), which would eventually flake on a busy CI
        # runner. Taking the minimum collapses that tail to ~8.5x max for ~0.07s.
        return min(_once() for _ in range(3))

    small = _time(400)
    large = _time(3200)
    # 8x the nodes. Measured on this codebase: the grouped implementation costs
    # ~8.3x (linear), a per-node rescan of cfg.edges costs ~30x (quadratic).
    # 16x sits well clear of both, so this discriminates without flaking on a
    # loaded machine. Sizes below ~400 do NOT discriminate — constant factors
    # swamp the difference and the test passes either way.
    assert large < small * 16, (small, large)


class _CountingTuple(tuple):
    """A tuple that records how many times it is iterated."""

    def __new__(cls, items):
        obj = super().__new__(cls, items)
        obj.iterations = 0
        return obj

    def __iter__(self):
        self.iterations += 1
        return super().__iter__()


def test_plan_validation_never_rescans_cfg_edges_per_node():
    """Structural, load-independent proof that validation is not O(V*E).

    The timing test above is calibrated but is still a wall-clock measurement.
    This one counts how many times ``cfg.edges`` is iterated: with edges grouped
    once, the count is CONSTANT in the node count; with a per-node rescan it
    grows as N. A constant count cannot be achieved by a quadratic scan, so this
    cannot flake under load.
    """
    from boomi_mcp.compiler.process_ir.contracts import MessageInputV1 as _Msg

    symbols = SymbolTableV1(symbols=())

    def _count(size):
        nodes = [
            _node(i, _msg("m{0}".format(i)), path="/body/steps/{0}".format(i - 1))
            for i in range(1, size)
        ]
        nodes.append(
            _node(size, StopSemanticV1(), path="/body/steps/{0}".format(size - 1),
                  exit_role="stop")
        )
        edges = [
            _edge(i, i, i + 1, kind="terminal" if i + 1 == size else "ordering")
            for i in range(1, size)
        ]
        cfg = SemanticCfgV1(
            entry_node_id="n1",
            nodes=tuple(nodes),
            edges=tuple(edges),
            exit_node_ids=("n{0}".format(size),),
        )
        plan_nodes = [
            _plan_node(1, StartNoActionInputV1(), role="start",
                       out=[_wire(1, 1, 2, provenance="synthetic")])
        ]
        for i in range(1, size + 1):
            ordinal = i + 1
            is_last = i == size
            plan_nodes.append(
                _plan_node(
                    ordinal,
                    StopInputV1() if is_last else _Msg(text="m{0}".format(i)),
                    cfg_node="n{0}".format(i),
                    path="/body/steps/{0}".format(i - 1),
                    out=() if is_last else [
                        _wire(ordinal, 1, ordinal + 1, cfg_edge_id="e{0}".format(i))
                    ],
                )
            )
        plan = EmissionPlanV1(
            entry_shape_id="shape1",
            nodes=tuple(plan_nodes),
            terminal_shape_ids=("shape{0}".format(size + 1),),
        )
        # ``model_copy(update=...)`` skips validation, so the instrumented tuple
        # survives instead of being coerced back to a plain tuple.
        counting = _CountingTuple(cfg.edges)
        instrumented = cfg.model_copy(update={"edges": counting})
        check_emission_plan_invariants(plan, instrumented, symbols)
        return counting.iterations

    small, large = _count(100), _count(800)
    assert small == large, (
        "cfg.edges iteration count grew from {0} to {1} when the node count grew "
        "8x — validation is rescanning edges per node".format(small, large)
    )


def test_decision_outcome_must_target_its_own_arm():
    """Ordering alone does not bind an outcome to its arm."""
    base = _decision_cfg()
    swapped = (
        base.edges[0].model_copy(update={"target_node_id": "n3"}),
        base.edges[1].model_copy(update={"target_node_id": "n2"}),
    )
    cfg = base.model_copy(update={"edges": swapped})
    diagnostic = _raises(check_cfg_invariants, cfg)
    assert diagnostic.code == PROCESS_IR_SEMANTIC_AMBIGUOUS_FLOW
    assert diagnostic.message == "a decision outcome targets a node outside its own arm"


def test_branch_leg_must_target_its_own_leg():
    base = _branch_cfg(2)
    swapped = (
        base.edges[0].model_copy(update={"target_node_id": "n3"}),
        base.edges[1].model_copy(update={"target_node_id": "n2"}),
    )
    cfg = base.model_copy(update={"edges": swapped})
    diagnostic = _raises(check_cfg_invariants, cfg)
    assert diagnostic.code == PROCESS_IR_SEMANTIC_AMBIGUOUS_FLOW
    assert diagnostic.message == "a branch leg targets a node outside its own leg"


def test_cache_stage_is_rejected_outside_a_branch_leg_terminal():
    """A root or mid-flow cache_put marked ``cache_stage`` would truncate the path."""
    from boomi_mcp.compiler.process_ir.contracts import CachePutSemanticV1

    cfg = SemanticCfgV1(
        entry_node_id="n1",
        nodes=(
            _node(
                1,
                CachePutSemanticV1(cache_ref="$ref:cache"),
                path="/body/steps/0",
                exit_role="cache_stage",
            ),
        ),
        edges=(),
        exit_node_ids=("n1",),
    )
    diagnostic = _raises(check_cfg_invariants, cfg)
    assert diagnostic.code == PROCESS_IR_SEMANTIC_AMBIGUOUS_FLOW
    assert diagnostic.message == "cache_stage is only valid in a branch leg terminal"


def test_cache_stage_accepted_in_a_branch_leg_terminal():
    from boomi_mcp.compiler.process_ir.contracts import CachePutSemanticV1

    cfg = SemanticCfgV1(
        entry_node_id="n1",
        nodes=(
            _node(
                1,
                CachePutSemanticV1(cache_ref="$ref:cache"),
                path="/body/steps/0/legs/1/terminal",
                exit_role="cache_stage",
            ),
        ),
        edges=(),
        exit_node_ids=("n1",),
    )
    check_cfg_invariants(cfg)


def test_edge_id_must_match_its_ordinal():
    cfg = _linear_cfg()
    broken = cfg.model_copy(
        update={"edges": (cfg.edges[0].model_copy(update={"edge_id": "e9"}),)}
    )
    assert _raises(check_cfg_invariants, broken).code == PROCESS_IR_COMPILE_INTERNAL


def test_terminal_shape_ids_must_be_canonical():
    """Duplicates or reordering make two equivalent plans serialise differently."""
    base = _linear_plan()
    duplicated = base.model_copy(update={"terminal_shape_ids": ("shape3", "shape3")})
    assert _check_plan(duplicated).code == PROCESS_IR_COMPILE_NONDETERMINISTIC


def test_emitter_input_is_compared_exactly_against_recomputation():
    """A Stop with continue_=False used to pass; recomputation catches it."""
    base = _linear_plan()
    flipped = base.nodes[2].model_copy(
        update={"emitter_input": StopInputV1(continue_=False)}
    )
    broken = base.model_copy(update={"nodes": base.nodes[:2] + (flipped,)})
    diagnostic = _check_plan(broken)
    assert diagnostic.code == PROCESS_IR_COMPILE_EMISSION_PLAN_INVALID
    assert diagnostic.message == (
        "emitter input does not match the CFG node's resolved semantics"
    )


def test_emitter_input_with_a_foreign_component_id_is_rejected():
    """A map id belonging to an unrelated symbol used to pass membership-only."""
    from boomi_mcp.compiler.process_ir.contracts import MapInputV1, MapSemanticV1

    cfg = SemanticCfgV1(
        entry_node_id="n1",
        nodes=(
            _node(1, MapSemanticV1(map_ref="$ref:m")),
            _node(2, StopSemanticV1(), path="/body/steps/1", exit_role="stop"),
        ),
        edges=(_edge(1, 1, 2, kind="terminal"),),
        exit_node_ids=("n2",),
    )
    symbols = SymbolTableV1(
        symbols=(
            ComponentSymbolV1(ref="$ref:m", component_id="right", component_type="t"),
            ComponentSymbolV1(ref="$ref:other", component_id="wrong", component_type="t"),
        )
    )
    base = _linear_plan()
    # "wrong" IS in the symbol table — just not the one $ref:m resolves to.
    foreign = base.nodes[1].model_copy(
        update={"emitter_input": MapInputV1(map_id="wrong")}
    )
    broken = base.model_copy(update={"nodes": (base.nodes[0], foreign, base.nodes[2])})
    assert (
        _raises(check_emission_plan_invariants, broken, cfg, symbols).code
        == PROCESS_IR_COMPILE_EMISSION_PLAN_INVALID
    )
    correct = base.model_copy(
        update={
            "nodes": (
                base.nodes[0],
                base.nodes[1].model_copy(
                    update={"emitter_input": MapInputV1(map_id="right")}
                ),
                base.nodes[2],
            )
        }
    )
    check_emission_plan_invariants(correct, cfg, symbols)


def test_synthetic_stop_with_flipped_continue_is_rejected():
    """Synthetic inputs are compared whole, not just by emitter kind.

    A synthetic Stop with ``continue_=False`` shares the "stop" emitter kind
    with the correct one, so a kind-only check let it through. Both synthetic
    inputs are fully determined by their role, so the expected value is exact.
    """
    from boomi_mcp.compiler.process_ir.contracts import ConnectorActionInputV1

    cfg = SemanticCfgV1(
        entry_node_id="n1",
        nodes=(
            _node(
                1,
                ConnectorSemanticV1(
                    role="target", connection_ref="$ref:c", operation_ref="$ref:o"
                ),
                path="/body/steps/0/legs/0/terminal",
                exit_role="routed_target",
            ),
        ),
        edges=(),
        exit_node_ids=("n1",),
    )
    symbols = SymbolTableV1(
        symbols=(
            ComponentSymbolV1(ref="$ref:c", component_id="cid", component_type="t"),
            ComponentSymbolV1(
                ref="$ref:o",
                component_id="oid",
                component_type="t",
                connector_type="database",
                action_type="Send",
            ),
        )
    )
    connector = ConnectorActionInputV1(
        emitter_kind="connectoraction_target",
        connector_type="database",
        action_type="SEND",
        connection_id="cid",
        operation_id="oid",
    )

    def _plan(stop_input):
        return EmissionPlanV1(
            entry_shape_id="shape1",
            nodes=(
                _plan_node(1, StartNoActionInputV1(), role="start",
                           out=[_wire(1, 1, 2, provenance="synthetic")]),
                _plan_node(2, connector, cfg_node="n1",
                           path="/body/steps/0/legs/0/terminal",
                           out=[_wire(2, 1, 3, provenance="synthetic")]),
                _plan_node(3, stop_input, role="terminal_stop"),
            ),
            terminal_shape_ids=("shape3",),
        )

    check_emission_plan_invariants(_plan(StopInputV1()), cfg, symbols)
    broken = _plan(StopInputV1(continue_=False))
    assert (
        _raises(check_emission_plan_invariants, broken, cfg, symbols).code
        == PROCESS_IR_COMPILE_EMISSION_PLAN_INVALID
    )


def test_branch_leg_prefix_matching_respects_pointer_boundaries():
    """``/legs/1`` must not match ``/legs/10`` — a Branch may have 25 legs.

    NOTE on construction: this mutates a target's ``source_path``, NOT the edge
    wiring. Retargeting edges cannot discriminate here — reachability runs
    before the leg rule and forces the leg->subtree map to be the identity
    permutation, so any swap makes the *other* leg violate the same rule with
    the same message, and the test would pass even without the boundary fix.
    """
    leg_count = 12
    nodes = [_node(1, BranchSemanticV1(leg_count=leg_count))]
    edges = []
    for leg in range(leg_count):
        ordinal = 2 + leg
        nodes.append(
            _node(
                ordinal,
                StopSemanticV1(),
                path="/body/steps/0/legs/{0}/terminal".format(leg),
                exit_role="stop",
            )
        )
        edges.append(
            _edge(leg + 1, 1, ordinal, kind="branch_leg", local=leg + 1,
                  leg_ordinal=leg + 1)
        )
    cfg = SemanticCfgV1(
        entry_node_id="n1",
        nodes=tuple(nodes),
        edges=tuple(edges),
        exit_node_ids=tuple("n{0}".format(2 + leg) for leg in range(leg_count)),
    )
    check_cfg_invariants(cfg)  # the honest 12-leg branch is valid

    # Leg 2's edge (prefix "/legs/1") now points at a node whose authored path
    # is "/legs/10/..." — which a bare prefix test accepts and a boundary-aware
    # one rejects. Only this one node moves, so exactly one rule is violated.
    moved = list(cfg.nodes)
    moved[2] = moved[2].model_copy(
        update={"source_path": "/body/steps/0/legs/10/terminal/nested"}
    )
    broken = cfg.model_copy(update={"nodes": tuple(moved)})
    diagnostic = _raises(check_cfg_invariants, broken)
    assert diagnostic.code == PROCESS_IR_SEMANTIC_AMBIGUOUS_FLOW
    assert diagnostic.message == "a branch leg targets a node outside its own leg"


def test_decision_arm_prefix_matching_respects_pointer_boundaries():
    """``/true_arm`` must not match ``/true_arm_extra``."""
    base = _decision_cfg()
    nodes = list(base.nodes)
    nodes[1] = nodes[1].model_copy(
        update={"source_path": "/body/steps/0/true_arm_extra/terminal"}
    )
    cfg = base.model_copy(update={"nodes": tuple(nodes)})
    diagnostic = _raises(check_cfg_invariants, cfg)
    assert diagnostic.code == PROCESS_IR_SEMANTIC_AMBIGUOUS_FLOW
    assert diagnostic.message == "a decision outcome targets a node outside its own arm"


def test_omitted_terminal_is_reported_as_missing_not_nondeterministic():
    """An omitted leaf is a MISSING terminal, not a nondeterminism defect.

    Checking canonical order first reported every omission as
    NONDETERMINISTIC and made the empty-declaration branch unreachable.
    """
    base = _linear_plan()
    omitted = base.model_copy(update={"terminal_shape_ids": ()})
    diagnostic = _check_plan(omitted)
    assert diagnostic.code == PROCESS_IR_SEMANTIC_MISSING_TERMINAL
    assert diagnostic.message == (
        "a shape with no outgoing transition is not declared terminal"
    )
    # Duplicates/reordering remain nondeterminism.
    duplicated = base.model_copy(update={"terminal_shape_ids": ("shape3", "shape3")})
    assert _check_plan(duplicated).code == PROCESS_IR_COMPILE_NONDETERMINISTIC


def test_symbol_index_is_state_free():
    """``build_index`` returns a FRESH dict; nothing is cached on the model.

    Caching the index on the model was tried and deliberately abandoned:
    pydantic v2 includes private attrs in ``__eq__`` (a lazy cache makes two
    identical tables compare unequal once one is used), ``model_copy(update=)``
    does not re-run ``model_post_init`` (an eager cache goes stale and resolves
    a PRESENT symbol to ``None``), and a private attr stays writable despite
    ``frozen=True``. This pins the state-free behaviour that replaced it.
    """
    symbols = SymbolTableV1(
        symbols=tuple(
            ComponentSymbolV1(
                ref="$ref:s{0}".format(i),
                component_id="c{0}".format(i),
                component_type="t",
            )
            for i in range(200)
        )
    )

    first, second = symbols.build_index(), symbols.build_index()
    assert first == second and first is not second, "index must be fresh each call"

    # Mutating a returned index cannot reach the table or a later index.
    first["$ref:s0"] = None
    assert symbols.build_index()["$ref:s0"].component_id == "c0"
    assert symbols.lookup("$ref:s0").component_id == "c0"

    # lookup() agrees with the index for present and absent refs.
    assert symbols.lookup("$ref:s199").component_id == "c199"
    assert symbols.lookup("$ref:absent") is None
    assert second.get("$ref:absent") is None

    # No hidden state: using a table never changes its equality or serialization,
    # and a model_copy with new symbols resolves against the NEW symbols.
    twin = SymbolTableV1(symbols=symbols.symbols)
    symbols.build_index()
    symbols.lookup("$ref:s1")
    assert symbols == twin
    assert "index" not in symbols.model_dump_json()

    replaced = symbols.model_copy(
        update={
            "symbols": (
                ComponentSymbolV1(ref="$ref:new", component_id="n", component_type="t"),
            )
        }
    )
    assert replaced.lookup("$ref:new").component_id == "n"
    assert replaced.lookup("$ref:s0") is None
    assert replaced == SymbolTableV1(symbols=replaced.symbols)


def test_synthetic_stop_with_flipped_continue_is_rejected():
    """Synthetic inputs are compared whole, not just by emitter kind.

    A synthetic Stop with ``continue_=False`` shares the "stop" emitter kind
    with the correct one, so a kind-only check let it through. Both synthetic
    inputs are fully determined by their role, so the expected value is exact.
    """
    from boomi_mcp.compiler.process_ir.contracts import ConnectorActionInputV1

    cfg = SemanticCfgV1(
        entry_node_id="n1",
        nodes=(
            _node(
                1,
                ConnectorSemanticV1(
                    role="target", connection_ref="$ref:c", operation_ref="$ref:o"
                ),
                path="/body/steps/0/legs/0/terminal",
                exit_role="routed_target",
            ),
        ),
        edges=(),
        exit_node_ids=("n1",),
    )
    symbols = SymbolTableV1(
        symbols=(
            ComponentSymbolV1(ref="$ref:c", component_id="cid", component_type="t"),
            ComponentSymbolV1(
                ref="$ref:o",
                component_id="oid",
                component_type="t",
                connector_type="database",
                action_type="Send",
            ),
        )
    )
    connector = ConnectorActionInputV1(
        emitter_kind="connectoraction_target",
        connector_type="database",
        action_type="SEND",
        connection_id="cid",
        operation_id="oid",
    )

    def _plan(stop_input):
        return EmissionPlanV1(
            entry_shape_id="shape1",
            nodes=(
                _plan_node(1, StartNoActionInputV1(), role="start",
                           out=[_wire(1, 1, 2, provenance="synthetic")]),
                _plan_node(2, connector, cfg_node="n1",
                           path="/body/steps/0/legs/0/terminal",
                           out=[_wire(2, 1, 3, provenance="synthetic")]),
                _plan_node(3, stop_input, role="terminal_stop"),
            ),
            terminal_shape_ids=("shape3",),
        )

    check_emission_plan_invariants(_plan(StopInputV1()), cfg, symbols)
    broken = _plan(StopInputV1(continue_=False))
    assert (
        _raises(check_emission_plan_invariants, broken, cfg, symbols).code
        == PROCESS_IR_COMPILE_EMISSION_PLAN_INVALID
    )


def test_branch_leg_prefix_matching_respects_pointer_boundaries():
    """``/legs/1`` must not match ``/legs/10`` — a Branch may have 25 legs.

    NOTE on construction: this mutates a target's ``source_path``, NOT the edge
    wiring. Retargeting edges cannot discriminate here — reachability runs
    before the leg rule and forces the leg->subtree map to be the identity
    permutation, so any swap makes the *other* leg violate the same rule with
    the same message, and the test would pass even without the boundary fix.
    """
    leg_count = 12
    nodes = [_node(1, BranchSemanticV1(leg_count=leg_count))]
    edges = []
    for leg in range(leg_count):
        ordinal = 2 + leg
        nodes.append(
            _node(
                ordinal,
                StopSemanticV1(),
                path="/body/steps/0/legs/{0}/terminal".format(leg),
                exit_role="stop",
            )
        )
        edges.append(
            _edge(leg + 1, 1, ordinal, kind="branch_leg", local=leg + 1,
                  leg_ordinal=leg + 1)
        )
    cfg = SemanticCfgV1(
        entry_node_id="n1",
        nodes=tuple(nodes),
        edges=tuple(edges),
        exit_node_ids=tuple("n{0}".format(2 + leg) for leg in range(leg_count)),
    )
    check_cfg_invariants(cfg)  # the honest 12-leg branch is valid

    # Leg 2's edge (prefix "/legs/1") now points at a node whose authored path
    # is "/legs/10/..." — which a bare prefix test accepts and a boundary-aware
    # one rejects. Only this one node moves, so exactly one rule is violated.
    moved = list(cfg.nodes)
    moved[2] = moved[2].model_copy(
        update={"source_path": "/body/steps/0/legs/10/terminal/nested"}
    )
    broken = cfg.model_copy(update={"nodes": tuple(moved)})
    diagnostic = _raises(check_cfg_invariants, broken)
    assert diagnostic.code == PROCESS_IR_SEMANTIC_AMBIGUOUS_FLOW
    assert diagnostic.message == "a branch leg targets a node outside its own leg"


def test_decision_arm_prefix_matching_respects_pointer_boundaries():
    """``/true_arm`` must not match ``/true_arm_extra``."""
    base = _decision_cfg()
    nodes = list(base.nodes)
    nodes[1] = nodes[1].model_copy(
        update={"source_path": "/body/steps/0/true_arm_extra/terminal"}
    )
    cfg = base.model_copy(update={"nodes": tuple(nodes)})
    diagnostic = _raises(check_cfg_invariants, cfg)
    assert diagnostic.code == PROCESS_IR_SEMANTIC_AMBIGUOUS_FLOW
    assert diagnostic.message == "a decision outcome targets a node outside its own arm"


def test_omitted_terminal_is_reported_as_missing_not_nondeterministic():
    """An omitted leaf is a MISSING terminal, not a nondeterminism defect.

    Checking canonical order first reported every omission as
    NONDETERMINISTIC and made the empty-declaration branch unreachable.
    """
    base = _linear_plan()
    omitted = base.model_copy(update={"terminal_shape_ids": ()})
    diagnostic = _check_plan(omitted)
    assert diagnostic.code == PROCESS_IR_SEMANTIC_MISSING_TERMINAL
    assert diagnostic.message == (
        "a shape with no outgoing transition is not declared terminal"
    )
    # Duplicates/reordering remain nondeterminism.
    duplicated = base.model_copy(update={"terminal_shape_ids": ("shape3", "shape3")})
    assert _check_plan(duplicated).code == PROCESS_IR_COMPILE_NONDETERMINISTIC


def test_symbol_lookup_is_indexed_not_a_linear_scan():
    """Symbol resolution must not make plan validation O(nodes x symbols).

    Structural rather than timed: the index is built once at construction, so
    two equivalent tables stay equal and lookups are dict-backed.
    """
    symbols = SymbolTableV1(
        symbols=tuple(
            ComponentSymbolV1(
                ref="$ref:s{0}".format(i),
                component_id="c{0}".format(i),
                component_type="t",
            )
            for i in range(500)
        )
    )
    assert symbols.lookup("$ref:s499").component_id == "c499"
    assert symbols.lookup("$ref:missing") is None
    # Eagerly built, so using one table cannot make it unequal to its twin.
    twin = SymbolTableV1(symbols=symbols.symbols)
    symbols.lookup("$ref:s1")
    assert symbols == twin
    assert "index" not in symbols.model_dump_json()


def test_plan_validation_is_linear_in_symbols_too():
    """Guard the SYMBOL dimension, which the node-count guards cannot see.

    ``test_plan_validation_scales_linearly_with_node_count`` passes an EMPTY
    symbol table and message-only nodes, so symbol-lookup cost is zero there.
    This builds N map nodes each with its OWN ``map_ref`` so nodes and symbols
    grow together — the shape that turns a per-reference scan into O(N²).
    """
    from boomi_mcp.compiler.process_ir.contracts import MapInputV1, MapSemanticV1

    def _count(size):
        nodes = [
            _node(i, MapSemanticV1(map_ref="$ref:m{0}".format(i)),
                  path="/body/steps/{0}".format(i - 1))
            for i in range(1, size)
        ]
        nodes.append(
            _node(size, StopSemanticV1(), path="/body/steps/{0}".format(size - 1),
                  exit_role="stop")
        )
        edges = [
            _edge(i, i, i + 1, kind="terminal" if i + 1 == size else "ordering")
            for i in range(1, size)
        ]
        cfg = SemanticCfgV1(
            entry_node_id="n1", nodes=tuple(nodes), edges=tuple(edges),
            exit_node_ids=("n{0}".format(size),),
        )
        symbols = SymbolTableV1(
            symbols=tuple(
                ComponentSymbolV1(
                    ref="$ref:m{0}".format(i),
                    component_id="c{0}".format(i),
                    component_type="t",
                )
                for i in range(1, size)
            )
        )
        plan_nodes = [
            _plan_node(1, StartNoActionInputV1(), role="start",
                       out=[_wire(1, 1, 2, provenance="synthetic")])
        ]
        for i in range(1, size + 1):
            ordinal = i + 1
            is_last = i == size
            plan_nodes.append(
                _plan_node(
                    ordinal,
                    StopInputV1() if is_last else MapInputV1(map_id="c{0}".format(i)),
                    cfg_node="n{0}".format(i),
                    path="/body/steps/{0}".format(i - 1),
                    out=() if is_last else [
                        _wire(ordinal, 1, ordinal + 1, cfg_edge_id="e{0}".format(i))
                    ],
                )
            )
        plan = EmissionPlanV1(
            entry_shape_id="shape1", nodes=tuple(plan_nodes),
            terminal_shape_ids=("shape{0}".format(size + 1),),
        )
        counting = _CountingTuple(symbols.symbols)
        instrumented = symbols.model_copy(update={"symbols": counting})
        check_emission_plan_invariants(plan, cfg, instrumented)
        return counting.iterations

    small, large = _count(100), _count(800)
    # Index built once per validation, so the symbol tuple is walked a constant
    # number of times regardless of how many nodes resolve references. A
    # per-reference scan would walk it once per reference (~N times).
    assert small == large, (
        "symbol tuple iteration grew from {0} to {1} when nodes+symbols grew 8x "
        "— reference resolution is scanning per lookup".format(small, large)
    )
