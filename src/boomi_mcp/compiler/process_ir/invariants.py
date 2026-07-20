"""Compiler invariant checkers for the CFG and emission plan (issue #137).

These run on *already-constructed* records, including hand-built malformed ones,
so nothing here may assume well-formedness — a checker that crashes on a broken
input is not a checker.

Code mapping (issue #137 failure taxonomy):

===============================================  ===================================
condition                                        code
===============================================  ===================================
node not reachable from the entry                ``PROCESS_IR_SEMANTIC_UNREACHABLE``
a path reaches no exit                           ``PROCESS_IR_SEMANTIC_MISSING_TERMINAL``
multiple entries, invalid successor, join,       ``PROCESS_IR_SEMANTIC_AMBIGUOUS_FLOW``
cycle, or flow past a terminal
duplicate or dangling internal id                ``PROCESS_IR_COMPILE_INTERNAL``
noncanonical tuple/ordinal order                 ``PROCESS_IR_COMPILE_NONDETERMINISTIC``
bad symbol, wiring, layout, or synthesis         ``PROCESS_IR_COMPILE_EMISSION_PLAN_INVALID``
===============================================  ===================================

The split is deliberate and load-bearing: ``SEMANTIC_*`` blames the authored
payload, ``COMPILE_*`` blames the compiler. Reporting a compiler bug as a user
error is how a caller ends up "fixing" correct input.
"""

from __future__ import annotations

from typing import Dict, List, Optional, Set

from ...errors import (
    PROCESS_IR_COMPILE_EMISSION_PLAN_INVALID,
    PROCESS_IR_COMPILE_INTERNAL,
    PROCESS_IR_COMPILE_NONDETERMINISTIC,
    PROCESS_IR_SEMANTIC_AMBIGUOUS_FLOW,
    PROCESS_IR_SEMANTIC_MISSING_TERMINAL,
    PROCESS_IR_SEMANTIC_UNREACHABLE,
)
from .contracts import (
    BRANCH_MAX_LEGS,
    BRANCH_MIN_LEGS,
    DECISION_FALSE_DRAGPOINT_Y,
    DRAGPOINT_Y,
    SHAPE_Y,
    START_SHAPE_X,
    START_SHAPE_Y,
    EmissionPlanV1,
    SemanticCfgV1,
    SymbolTableV1,
    dragpoint_name,
    dragpoint_x,
    shape_id,
    shape_x,
)
from .diagnostics import raise_compile_error

_SEMANTIC_PHASE = "semantic_lowering"
_PLAN_PHASE = "emission_planning"

# Semantic kinds whose successors are control edges rather than a single
# sequential successor.
_CONTROL_KINDS = frozenset({"branch", "decision"})


def _fail(code: str, phase: str, path: str, message: str, node_id: Optional[str] = None):
    return raise_compile_error(
        code, phase, path, internal_node_id=node_id, message=message
    )


def check_cfg_invariants(cfg: SemanticCfgV1) -> None:
    """Validate every structural invariant of a semantic CFG.

    Raises ``ProcessIRCompileError`` on the first violation, with the authored
    path of the offending node.
    """
    nodes = list(cfg.nodes)
    edges = list(cfg.edges)

    if not nodes:
        raise _fail(
            PROCESS_IR_SEMANTIC_MISSING_TERMINAL,
            _SEMANTIC_PHASE,
            "",
            "the lowered CFG has no nodes",
        )

    # --- identities, ordinals, canonical order -----------------------------
    seen_nodes: Set[str] = set()
    seen_paths: Set[str] = set()
    for index, node in enumerate(nodes, start=1):
        if node.node_id in seen_nodes:
            raise _fail(
                PROCESS_IR_COMPILE_INTERNAL,
                _SEMANTIC_PHASE,
                node.source_path,
                "duplicate CFG node id",
                node.node_id,
            )
        seen_nodes.add(node.node_id)
        if node.source_path in seen_paths:
            raise _fail(
                PROCESS_IR_COMPILE_INTERNAL,
                _SEMANTIC_PHASE,
                node.source_path,
                "two CFG nodes share one authored source path",
                node.node_id,
            )
        seen_paths.add(node.source_path)
        if node.ordinal != index:
            raise _fail(
                PROCESS_IR_COMPILE_NONDETERMINISTIC,
                _SEMANTIC_PHASE,
                node.source_path,
                "CFG node ordinals are not contiguous and ascending",
                node.node_id,
            )
        if node.node_id != "n{0}".format(node.ordinal):
            raise _fail(
                PROCESS_IR_COMPILE_INTERNAL,
                _SEMANTIC_PHASE,
                node.source_path,
                "CFG node id does not match its ordinal",
                node.node_id,
            )

    by_id = {node.node_id: node for node in nodes}

    seen_edges: Set[str] = set()
    for index, edge in enumerate(edges, start=1):
        if edge.edge_id in seen_edges:
            raise _fail(
                PROCESS_IR_COMPILE_INTERNAL,
                _SEMANTIC_PHASE,
                edge.provenance_path,
                "duplicate CFG edge id",
            )
        seen_edges.add(edge.edge_id)
        if edge.ordinal != index:
            raise _fail(
                PROCESS_IR_COMPILE_NONDETERMINISTIC,
                _SEMANTIC_PHASE,
                edge.provenance_path,
                "CFG edge ordinals are not contiguous and ascending",
            )
        if edge.source_node_id not in by_id or edge.target_node_id not in by_id:
            raise _fail(
                PROCESS_IR_COMPILE_INTERNAL,
                _SEMANTIC_PHASE,
                edge.provenance_path,
                "CFG edge references a node that does not exist",
            )
        if edge.kind == "catch":
            raise _fail(
                PROCESS_IR_SEMANTIC_AMBIGUOUS_FLOW,
                _SEMANTIC_PHASE,
                edge.provenance_path,
                "catch edges are reserved and are not generated in v1",
            )

    # --- canonical edge ordering ------------------------------------------
    ordering_key = [
        (by_id[edge.source_node_id].ordinal, edge.local_ordinal) for edge in edges
    ]
    if ordering_key != sorted(ordering_key):
        raise _fail(
            PROCESS_IR_COMPILE_NONDETERMINISTIC,
            _SEMANTIC_PHASE,
            edges[0].provenance_path if edges else "",
            "CFG edges are not in canonical (source ordinal, local ordinal) order",
        )

    # --- entry, joins, cycles ---------------------------------------------
    inbound: Dict[str, int] = {node.node_id: 0 for node in nodes}
    outbound: Dict[str, List] = {node.node_id: [] for node in nodes}
    for edge in edges:
        inbound[edge.target_node_id] += 1
        outbound[edge.source_node_id].append(edge)

    entries = [node_id for node_id, count in inbound.items() if count == 0]
    if len(entries) != 1:
        raise _fail(
            PROCESS_IR_SEMANTIC_AMBIGUOUS_FLOW,
            _SEMANTIC_PHASE,
            nodes[0].source_path,
            "the CFG must have exactly one entry node",
        )
    if entries[0] != cfg.entry_node_id:
        raise _fail(
            PROCESS_IR_COMPILE_INTERNAL,
            _SEMANTIC_PHASE,
            by_id[entries[0]].source_path,
            "declared entry node is not the node without inbound edges",
            cfg.entry_node_id,
        )
    for node_id, count in inbound.items():
        if count > 1:
            raise _fail(
                PROCESS_IR_SEMANTIC_AMBIGUOUS_FLOW,
                _SEMANTIC_PHASE,
                by_id[node_id].source_path,
                "joins are not representable: a node has more than one predecessor",
                node_id,
            )

    # --- reachability ------------------------------------------------------
    reached: Set[str] = set()
    stack = [cfg.entry_node_id]
    while stack:
        current = stack.pop()
        if current in reached:
            raise _fail(
                PROCESS_IR_SEMANTIC_AMBIGUOUS_FLOW,
                _SEMANTIC_PHASE,
                by_id[current].source_path,
                "the CFG contains a cycle",
                current,
            )
        reached.add(current)
        for edge in outbound[current]:
            stack.append(edge.target_node_id)
    for node in nodes:
        if node.node_id not in reached:
            raise _fail(
                PROCESS_IR_SEMANTIC_UNREACHABLE,
                _SEMANTIC_PHASE,
                node.source_path,
                "node is not reachable from the control-flow entry",
                node.node_id,
            )

    # --- per-node successor rules -----------------------------------------
    for node in nodes:
        successors = outbound[node.node_id]
        kind = node.semantic.semantic_kind

        if node.exit_role:
            if successors:
                raise _fail(
                    PROCESS_IR_SEMANTIC_AMBIGUOUS_FLOW,
                    _SEMANTIC_PHASE,
                    node.source_path,
                    "flow continues past a terminal node",
                    node.node_id,
                )
            continue

        if kind == "branch":
            legs = [edge for edge in successors if edge.kind == "branch_leg"]
            if len(legs) != len(successors):
                raise _fail(
                    PROCESS_IR_SEMANTIC_AMBIGUOUS_FLOW,
                    _SEMANTIC_PHASE,
                    node.source_path,
                    "a branch may only have branch_leg successors",
                    node.node_id,
                )
            if len(legs) != node.semantic.leg_count:
                raise _fail(
                    PROCESS_IR_COMPILE_INTERNAL,
                    _SEMANTIC_PHASE,
                    node.source_path,
                    "branch leg edge count does not match the declared leg count",
                    node.node_id,
                )
            if not BRANCH_MIN_LEGS <= len(legs) <= BRANCH_MAX_LEGS:
                raise _fail(
                    PROCESS_IR_SEMANTIC_AMBIGUOUS_FLOW,
                    _SEMANTIC_PHASE,
                    node.source_path,
                    "branch leg count is out of range",
                    node.node_id,
                )
            if [edge.leg_ordinal for edge in legs] != list(range(1, len(legs) + 1)):
                raise _fail(
                    PROCESS_IR_COMPILE_NONDETERMINISTIC,
                    _SEMANTIC_PHASE,
                    node.source_path,
                    "branch legs are not in ascending authored order",
                    node.node_id,
                )
            continue

        if kind == "decision":
            outcomes = [edge for edge in successors if edge.kind == "decision_outcome"]
            if len(outcomes) != len(successors) or len(outcomes) != 2:
                raise _fail(
                    PROCESS_IR_SEMANTIC_AMBIGUOUS_FLOW,
                    _SEMANTIC_PHASE,
                    node.source_path,
                    "a decision must have exactly two decision_outcome successors",
                    node.node_id,
                )
            if [edge.outcome for edge in outcomes] != ["true", "false"]:
                raise _fail(
                    PROCESS_IR_COMPILE_NONDETERMINISTIC,
                    _SEMANTIC_PHASE,
                    node.source_path,
                    "decision outcomes must be ordered true then false",
                    node.node_id,
                )
            continue

        if len(successors) != 1:
            raise _fail(
                PROCESS_IR_SEMANTIC_MISSING_TERMINAL
                if not successors
                else PROCESS_IR_SEMANTIC_AMBIGUOUS_FLOW,
                _SEMANTIC_PHASE,
                node.source_path,
                "a non-terminal linear node must have exactly one successor",
                node.node_id,
            )
        if successors[0].kind not in ("ordering", "terminal"):
            raise _fail(
                PROCESS_IR_SEMANTIC_AMBIGUOUS_FLOW,
                _SEMANTIC_PHASE,
                node.source_path,
                "a linear node may only have a sequential successor",
                node.node_id,
            )

    # --- exits -------------------------------------------------------------
    declared = list(cfg.exit_node_ids)
    actual = [node.node_id for node in nodes if node.exit_role]
    if declared != actual:
        raise _fail(
            PROCESS_IR_COMPILE_INTERNAL,
            _SEMANTIC_PHASE,
            nodes[0].source_path,
            "declared exit nodes do not match the nodes carrying an exit role",
        )
    if not actual:
        raise _fail(
            PROCESS_IR_SEMANTIC_MISSING_TERMINAL,
            _SEMANTIC_PHASE,
            nodes[0].source_path,
            "no path reaches a valid terminal",
        )
    # Every leaf must be an exit: a node with no successors and no exit role was
    # already rejected above, so this re-checks the reverse direction cheaply.
    for node in nodes:
        if not outbound[node.node_id] and not node.exit_role:
            raise _fail(
                PROCESS_IR_SEMANTIC_MISSING_TERMINAL,
                _SEMANTIC_PHASE,
                node.source_path,
                "path ends on a node that is not a valid terminal",
                node.node_id,
            )


def check_emission_plan_invariants(
    plan: EmissionPlanV1, cfg: SemanticCfgV1, symbols: SymbolTableV1
) -> None:
    """Validate the emission plan against the CFG it was lowered from."""
    nodes = list(plan.nodes)
    if not nodes:
        raise _fail(
            PROCESS_IR_COMPILE_EMISSION_PLAN_INVALID,
            _PLAN_PHASE,
            "",
            "the emission plan has no nodes",
        )

    # --- shape identities and ordering -------------------------------------
    for index, node in enumerate(nodes, start=1):
        path = node.source_path or ""
        if node.ordinal != index:
            raise _fail(
                PROCESS_IR_COMPILE_NONDETERMINISTIC,
                _PLAN_PHASE,
                path,
                "plan ordinals are not contiguous and ascending",
            )
        if node.shape_id != shape_id(node.ordinal):
            raise _fail(
                PROCESS_IR_COMPILE_EMISSION_PLAN_INVALID,
                _PLAN_PHASE,
                path,
                "plan shape id does not match its ordinal",
            )

    by_shape = {node.shape_id: node for node in nodes}
    if len(by_shape) != len(nodes):
        raise _fail(
            PROCESS_IR_COMPILE_INTERNAL,
            _PLAN_PHASE,
            "",
            "duplicate plan shape id",
        )

    # --- the synthetic Start ------------------------------------------------
    start = nodes[0]
    if (
        start.shape_id != shape_id(1)
        or start.origin != "synthetic"
        or start.synthetic_role != "start"
        or start.emitter_input.emitter_kind != "start_noaction"
    ):
        raise _fail(
            PROCESS_IR_COMPILE_EMISSION_PLAN_INVALID,
            _PLAN_PHASE,
            "",
            "the plan must begin with exactly one synthetic start_noaction at shape1",
        )
    if plan.entry_shape_id != shape_id(1):
        raise _fail(
            PROCESS_IR_COMPILE_EMISSION_PLAN_INVALID,
            _PLAN_PHASE,
            "",
            "the declared entry shape is not shape1",
        )
    if start.layout.x != START_SHAPE_X or start.layout.y != START_SHAPE_Y:
        raise _fail(
            PROCESS_IR_COMPILE_EMISSION_PLAN_INVALID,
            _PLAN_PHASE,
            "",
            "the synthetic start shape has non-parity geometry",
        )
    starts = [item for item in nodes if item.synthetic_role == "start"]
    if len(starts) != 1:
        raise _fail(
            PROCESS_IR_COMPILE_EMISSION_PLAN_INVALID,
            _PLAN_PHASE,
            "",
            "the plan must contain exactly one synthetic start shape",
        )

    # --- CFG correspondence -------------------------------------------------
    cfg_by_id = {node.node_id: node for node in cfg.nodes}
    routed = {
        node.node_id for node in cfg.nodes if node.exit_role == "routed_target"
    }
    planned_cfg_ids = [node.cfg_node_id for node in nodes if node.origin == "ir"]
    if planned_cfg_ids != [node.node_id for node in cfg.nodes]:
        raise _fail(
            PROCESS_IR_COMPILE_EMISSION_PLAN_INVALID,
            _PLAN_PHASE,
            "",
            "plan nodes do not correspond one-to-one with CFG nodes in order",
        )

    synthetic_stops = [
        item for item in nodes if item.synthetic_role == "terminal_stop"
    ]
    if len(synthetic_stops) != len(routed):
        raise _fail(
            PROCESS_IR_COMPILE_EMISSION_PLAN_INVALID,
            _PLAN_PHASE,
            "",
            "synthetic terminal stops do not match the routed-target exits",
        )

    for node in nodes:
        path = node.source_path or ""
        if node.origin == "ir":
            if node.cfg_node_id not in cfg_by_id:
                raise _fail(
                    PROCESS_IR_COMPILE_INTERNAL,
                    _PLAN_PHASE,
                    path,
                    "plan node references a CFG node that does not exist",
                )
            if node.synthetic_role is not None:
                raise _fail(
                    PROCESS_IR_COMPILE_EMISSION_PLAN_INVALID,
                    _PLAN_PHASE,
                    path,
                    "an IR-origin plan node must not carry a synthetic role",
                )
            if node.source_path != cfg_by_id[node.cfg_node_id].source_path:
                raise _fail(
                    PROCESS_IR_COMPILE_EMISSION_PLAN_INVALID,
                    _PLAN_PHASE,
                    path,
                    "plan node source path disagrees with its CFG node",
                )
        else:
            if node.cfg_node_id is not None or node.source_path is not None:
                raise _fail(
                    PROCESS_IR_COMPILE_EMISSION_PLAN_INVALID,
                    _PLAN_PHASE,
                    "",
                    "a synthetic plan node must not claim authored provenance",
                )
            if node.synthetic_role is None:
                raise _fail(
                    PROCESS_IR_COMPILE_EMISSION_PLAN_INVALID,
                    _PLAN_PHASE,
                    "",
                    "a synthetic plan node must declare its synthetic role",
                )

        # --- geometry -------------------------------------------------------
        expected_y = START_SHAPE_Y if node.synthetic_role == "start" else SHAPE_Y
        expected_x = START_SHAPE_X if node.synthetic_role == "start" else shape_x(
            node.ordinal
        )
        if node.layout.x != expected_x or node.layout.y != expected_y:
            raise _fail(
                PROCESS_IR_COMPILE_EMISSION_PLAN_INVALID,
                _PLAN_PHASE,
                path,
                "plan shape geometry does not match the parity formula",
            )

        # --- wiring ---------------------------------------------------------
        transitions = list(node.outgoing)
        if [item.local_ordinal for item in transitions] != list(
            range(1, len(transitions) + 1)
        ):
            raise _fail(
                PROCESS_IR_COMPILE_NONDETERMINISTIC,
                _PLAN_PHASE,
                path,
                "transition local ordinals are not contiguous from 1",
            )
        for transition in transitions:
            if transition.to_shape_id not in by_shape:
                raise _fail(
                    PROCESS_IR_COMPILE_EMISSION_PLAN_INVALID,
                    _PLAN_PHASE,
                    path,
                    "transition targets a shape that does not exist",
                )
            if transition.dragpoint_name != dragpoint_name(
                node.shape_id, transition.local_ordinal
            ):
                raise _fail(
                    PROCESS_IR_COMPILE_EMISSION_PLAN_INVALID,
                    _PLAN_PHASE,
                    path,
                    "dragpoint name does not match its shape and local ordinal",
                )
            if transition.x != dragpoint_x(node.ordinal):
                raise _fail(
                    PROCESS_IR_COMPILE_EMISSION_PLAN_INVALID,
                    _PLAN_PHASE,
                    path,
                    "dragpoint geometry does not match the parity formula",
                )

        kind = node.emitter_input.emitter_kind
        if kind == "decision":
            if [item.identifier for item in transitions] != ["true", "false"] or [
                item.text for item in transitions
            ] != ["True", "False"]:
                raise _fail(
                    PROCESS_IR_COMPILE_EMISSION_PLAN_INVALID,
                    _PLAN_PHASE,
                    path,
                    "decision dragpoint labels must be true/True then false/False",
                )
            if [item.y for item in transitions] != [
                DRAGPOINT_Y,
                DECISION_FALSE_DRAGPOINT_Y,
            ]:
                raise _fail(
                    PROCESS_IR_COMPILE_EMISSION_PLAN_INVALID,
                    _PLAN_PHASE,
                    path,
                    "decision dragpoint rows do not match parity geometry",
                )
        elif kind == "branch":
            expected = [str(index) for index in range(1, len(transitions) + 1)]
            if [item.identifier for item in transitions] != expected or [
                item.text for item in transitions
            ] != expected:
                raise _fail(
                    PROCESS_IR_COMPILE_EMISSION_PLAN_INVALID,
                    _PLAN_PHASE,
                    path,
                    "branch dragpoint labels must be the 1-based leg numbers",
                )
            if node.emitter_input.num_branches != len(transitions):
                raise _fail(
                    PROCESS_IR_COMPILE_EMISSION_PLAN_INVALID,
                    _PLAN_PHASE,
                    path,
                    "branch numBranches does not match its wired leg count",
                )
        else:
            for item in transitions:
                if item.identifier is not None or item.text is not None:
                    raise _fail(
                        PROCESS_IR_COMPILE_EMISSION_PLAN_INVALID,
                        _PLAN_PHASE,
                        path,
                        "only branch and decision dragpoints carry labels",
                    )
                if item.y != DRAGPOINT_Y:
                    raise _fail(
                        PROCESS_IR_COMPILE_EMISSION_PLAN_INVALID,
                        _PLAN_PHASE,
                        path,
                        "dragpoint row does not match parity geometry",
                    )

    # --- terminals ----------------------------------------------------------
    declared_terminals = list(plan.terminal_shape_ids)
    for shape in declared_terminals:
        if shape not in by_shape:
            raise _fail(
                PROCESS_IR_COMPILE_EMISSION_PLAN_INVALID,
                _PLAN_PHASE,
                "",
                "declared terminal shape does not exist",
            )
        if by_shape[shape].outgoing:
            raise _fail(
                PROCESS_IR_SEMANTIC_AMBIGUOUS_FLOW,
                _PLAN_PHASE,
                by_shape[shape].source_path or "",
                "a terminal shape must have no outgoing transitions",
            )
    if not declared_terminals:
        raise _fail(
            PROCESS_IR_SEMANTIC_MISSING_TERMINAL,
            _PLAN_PHASE,
            "",
            "the emission plan declares no terminal shape",
        )
    for node in nodes:
        if not node.outgoing and node.shape_id not in declared_terminals:
            raise _fail(
                PROCESS_IR_SEMANTIC_MISSING_TERMINAL,
                _PLAN_PHASE,
                node.source_path or "",
                "a shape with no outgoing transition is not declared terminal",
            )


__all__: List[str] = [
    "check_cfg_invariants",
    "check_emission_plan_invariants",
]
