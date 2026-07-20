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
    base = _linear_plan()
    broken = base.model_copy(update={"nodes": base.nodes[:2], "terminal_shape_ids": ()})
    assert _check_plan(broken).code in (
        PROCESS_IR_COMPILE_EMISSION_PLAN_INVALID,
        PROCESS_IR_SEMANTIC_MISSING_TERMINAL,
    )


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
            ComponentSymbolV1(ref="$ref:o", component_id="oid", component_type="t"),
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
