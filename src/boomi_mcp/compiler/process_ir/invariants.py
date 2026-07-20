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

import re
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
    StartNoActionInputV1,
    StopInputV1,
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

# Which exit roles each semantic kind may carry. A terminal semantic MUST carry
# its role; a ``target`` is an exit only in a leg/arm position (routed), and a
# ``cache_put`` only as a target-less staging terminal — hence the ``None``
# alternative on those two. Everything else may never claim an exit role.
_ALLOWED_EXIT_ROLES = {
    "stop": ("stop",),
    "return_documents": ("return_documents",),
    "exception": ("exception",),
    "connector": (None, "routed_target"),
    "cache_put": (None, "cache_stage"),
}

# The only authored positions that can hold a target terminal:
# ``BranchLegV1.terminal`` and ``DecisionTrueArmV1.terminal``.
_ROUTED_TARGET_PATH = re.compile(r"(?:/legs/\d+|/true_arm)/terminal$")

# ``cache_stage`` is authored only as ``BranchLegV1.terminal``.
_CACHE_STAGE_PATH = re.compile(r"/legs/\d+/terminal$")

# Emitter-input fields that must name a component resolved through the symbol
# table. Anything here that is absent from the table means the plan carries a
# component reference the caller never supplied.
_RESOLVED_ID_FIELDS = (
    "connection_id",
    "operation_id",
    "map_id",
    "document_cache_id",
    "process_id",
)


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
        if edge.edge_id != "e{0}".format(edge.ordinal):
            raise _fail(
                PROCESS_IR_COMPILE_INTERNAL,
                _SEMANTIC_PHASE,
                edge.provenance_path,
                "CFG edge id does not match its ordinal",
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
    # Sorted order alone is too weak: two edges sharing (source, local_ordinal)
    # sort fine, and plan lowering would silently renumber them. Local ordinals
    # must be unique and contiguous from 1 per source.
    local_by_source = {}
    for edge in edges:
        local_by_source.setdefault(edge.source_node_id, []).append(edge.local_ordinal)
    for source_id, locals_ in local_by_source.items():
        if locals_ != list(range(1, len(locals_) + 1)):
            raise _fail(
                PROCESS_IR_COMPILE_NONDETERMINISTIC,
                _SEMANTIC_PHASE,
                by_id[source_id].source_path,
                "edge local ordinals must be unique and contiguous from 1 per source",
                source_id,
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

    # V1 wiring is forward-only: plan shape ordinals follow CFG ordinals, so a
    # backward edge would emit a shape wired to an EARLIER shape. Checked after
    # reachability because a fully-reachable, acyclic, join-free graph can still
    # be ordered backwards (n1 -> n3 -> n2), which every check above accepts.
    for edge in edges:
        if by_id[edge.target_node_id].ordinal <= by_id[edge.source_node_id].ordinal:
            raise _fail(
                PROCESS_IR_SEMANTIC_AMBIGUOUS_FLOW,
                _SEMANTIC_PHASE,
                edge.provenance_path,
                "control flow must be forward-only: an edge targets an earlier node",
            )

    # --- exit role must agree with the node's semantics --------------------
    # Without this, a Stop carrying ``exit_role=None`` reads as an ordinary
    # linear node (so it is allowed a successor, and the plan would emit a Stop
    # with an outgoing transition the Stop emitter cannot serialise), and a
    # Message carrying ``exit_role="stop"`` reads as a terminal.
    for node in nodes:
        kind = node.semantic.semantic_kind
        role = node.exit_role
        allowed = _ALLOWED_EXIT_ROLES.get(kind, (None,))
        if kind == "connector":
            # Keying on semantic_kind alone would let a SOURCE endpoint claim
            # ``routed_target``, and the plan would then append a synthetic Stop
            # after the source connector. Only a target endpoint can be routed.
            allowed = (None, "routed_target") if node.semantic.role == "target" else (None,)
        if role not in allowed:
            raise _fail(
                PROCESS_IR_SEMANTIC_AMBIGUOUS_FLOW,
                _SEMANTIC_PHASE,
                node.source_path,
                "exit role is not valid for this node's semantics",
                node.node_id,
            )
        # A routed target is valid ONLY where the IR can actually author a
        # target terminal: a Branch leg (``BranchLegV1.terminal``) or a Decision
        # TRUE arm (``DecisionTrueArmV1.terminal``). A bare "/terminal" suffix
        # test would also accept ``/false_arm/terminal``, but
        # ``DecisionFalseArmV1.terminal`` is Stop/Branch/Exception only — no
        # target — so that CFG is unrepresentable, and planning it would append
        # a synthetic Stop on the reject route. A root target is likewise not an
        # exit (an authored Stop follows it).
        if role == "routed_target" and not _ROUTED_TARGET_PATH.search(node.source_path):
            raise _fail(
                PROCESS_IR_SEMANTIC_AMBIGUOUS_FLOW,
                _SEMANTIC_PHASE,
                node.source_path,
                "routed_target is only valid in a branch leg or decision true-arm terminal",
                node.node_id,
            )
        # ``cache_stage`` is the target-less staging leg, authored ONLY as
        # ``BranchLegV1.terminal``. A root or mid-flow cache_put is an ordinary
        # linear node, so accepting the role there would mark it terminal and
        # silently truncate the path.
        if role == "cache_stage" and not _CACHE_STAGE_PATH.search(node.source_path):
            raise _fail(
                PROCESS_IR_SEMANTIC_AMBIGUOUS_FLOW,
                _SEMANTIC_PHASE,
                node.source_path,
                "cache_stage is only valid in a branch leg terminal",
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
            # Bind each leg edge to ITS authored leg subtree; ascending ordinals
            # alone would allow two legs' targets to be swapped.
            for edge in legs:
                # Trailing "/" is load-bearing: without it "/legs/1" also
                # matches "/legs/10..19", which is reachable — a Branch may
                # have up to 25 legs.
                prefix = "{0}/legs/{1}/".format(node.source_path, edge.leg_ordinal - 1)
                target_path = by_id[edge.target_node_id].source_path
                if not target_path.startswith(prefix):
                    raise _fail(
                        PROCESS_IR_SEMANTIC_AMBIGUOUS_FLOW,
                        _SEMANTIC_PHASE,
                        node.source_path,
                        "a branch leg targets a node outside its own leg",
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
            # Order alone does not bind an outcome to its ARM: swapping the two
            # targets keeps the order valid while routing True into the false
            # subtree. Each outcome's target must live under its own arm.
            for edge in outcomes:
                arm = "true_arm" if edge.outcome == "true" else "false_arm"
                # Trailing "/" is load-bearing: a bare prefix test would let
                # "/true_arm_extra/..." satisfy "/true_arm".
                prefix = "{0}/{1}/".format(node.source_path, arm)
                target_path = by_id[edge.target_node_id].source_path
                if not target_path.startswith(prefix):
                    raise _fail(
                        PROCESS_IR_SEMANTIC_AMBIGUOUS_FLOW,
                        _SEMANTIC_PHASE,
                        node.source_path,
                        "a decision outcome targets a node outside its own arm",
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

    # The Start shape is synthetic, so it has no CFG edge to check against —
    # but it must still target the CFG entry node's shape.
    shape_for_cfg_node = {
        node.cfg_node_id: node.shape_id for node in nodes if node.origin == "ir"
    }
    cfg_edges_by_id = {edge.edge_id: edge for edge in cfg.edges}
    # Grouped ONCE. Scanning ``cfg.edges`` inside the per-node loop below would
    # make validation O(V*E), and ``SequenceNodeV1.steps`` has no upper bound —
    # 20k nodes would mean ~400M edge predicates on a perfectly valid input.
    cfg_out_by_source: Dict[str, List] = {}
    for edge in cfg.edges:
        cfg_out_by_source.setdefault(edge.source_node_id, []).append(edge)
    entry_shape = shape_for_cfg_node.get(cfg.entry_node_id)
    if [item.to_shape_id for item in start.outgoing] != [entry_shape]:
        raise _fail(
            PROCESS_IR_COMPILE_EMISSION_PLAN_INVALID,
            _PLAN_PHASE,
            "",
            "the synthetic start shape must wire to the CFG entry node's shape",
        )

    # --- symbol resolution --------------------------------------------------
    # Every component id the plan carries must have come from the symbol table.
    known_component_ids = {symbol.component_id for symbol in symbols.symbols}
    # Built ONCE for the per-node recomputation below.
    symbol_index = symbols.build_index()
    for node in nodes:
        emitter = node.emitter_input
        for field in _RESOLVED_ID_FIELDS:
            value = getattr(emitter, field, None)
            if value is not None and value not in known_component_ids:
                raise _fail(
                    PROCESS_IR_COMPILE_EMISSION_PLAN_INVALID,
                    _PLAN_PHASE,
                    node.source_path or "",
                    "plan carries a component id absent from the symbol table",
                )
        for step in getattr(emitter, "steps", ()) or ():
            profile_id = getattr(step, "profile_id", None)
            if profile_id is not None and profile_id not in known_component_ids:
                raise _fail(
                    PROCESS_IR_COMPILE_EMISSION_PLAN_INVALID,
                    _PLAN_PHASE,
                    node.source_path or "",
                    "plan carries a profile id absent from the symbol table",
                )
        for source in getattr(emitter, "source_values", ()) or ():
            profile_id = getattr(source, "profile_id", None)
            if profile_id is not None and profile_id not in known_component_ids:
                raise _fail(
                    PROCESS_IR_COMPILE_EMISSION_PLAN_INVALID,
                    _PLAN_PHASE,
                    node.source_path or "",
                    "plan carries a profile id absent from the symbol table",
                )

    # --- terminals may not carry outgoing flow ------------------------------
    # Checked BEFORE per-transition wiring so that "flow continues past a
    # terminal" is reported as ambiguous control flow rather than as whatever
    # wiring detail of the bogus transition happens to fail first.
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
    # Counting alone would let a synthetic Stop sit anywhere in the sequence.
    # V1 shape allocation requires it IMMEDIATELY after its routed target, with
    # the target wired to it — that adjacency is what reproduces the legacy
    # ``fallthrough=[target, stop]`` numbering.
    for index, node in enumerate(nodes):
        if node.origin != "ir" or node.cfg_node_id not in routed:
            continue
        following = nodes[index + 1] if index + 1 < len(nodes) else None
        if following is None or following.synthetic_role != "terminal_stop":
            raise _fail(
                PROCESS_IR_COMPILE_EMISSION_PLAN_INVALID,
                _PLAN_PHASE,
                node.source_path or "",
                "a routed target must be immediately followed by its synthetic stop",
            )
        if [item.to_shape_id for item in node.outgoing] != [following.shape_id]:
            raise _fail(
                PROCESS_IR_COMPILE_EMISSION_PLAN_INVALID,
                _PLAN_PHASE,
                node.source_path or "",
                "a routed target must wire to its own synthetic stop",
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
            # Recompute the emitter input from the CFG node + symbols and
            # compare EXACTLY. Checking only the emitter kind plus global
            # component-id membership was far too weak: a wrong semantic value,
            # a Stop with continue_=False, or a map id belonging to an unrelated
            # symbol all passed. Recomputation makes the check total, and is
            # simpler than enumerating per-field rules.
            from .lowering import _emitter_input_for

            expected_input = _emitter_input_for(
                cfg_by_id[node.cfg_node_id], symbol_index
            )
            if node.emitter_input != expected_input:
                raise _fail(
                    PROCESS_IR_COMPILE_EMISSION_PLAN_INVALID,
                    _PLAN_PHASE,
                    path,
                    "emitter input does not match the CFG node's resolved semantics",
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
            # Compare the whole input, not just its kind: a synthetic Stop with
            # ``continue_`` flipped would otherwise pass, since both variants
            # share the "stop" emitter kind. Both synthetic inputs are fully
            # determined by their role, so the expected value is exact.
            expected_synthetic = {
                "start": StartNoActionInputV1(),
                "terminal_stop": StopInputV1(),
            }.get(node.synthetic_role)
            if node.emitter_input != expected_synthetic:
                raise _fail(
                    PROCESS_IR_COMPILE_EMISSION_PLAN_INVALID,
                    _PLAN_PHASE,
                    "",
                    "synthetic plan node carries the wrong emitter input",
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

        # A node's transitions must be its CFG edges, IN ORDER. Checking each
        # transition independently is too weak: swapping BOTH the cfg_edge_id
        # and to_shape_id of a Decision's two transitions leaves each one
        # individually consistent, while the position-fixed dragpoint labels
        # then route True down the false arm. Comparing the ordered edge-id
        # sequence closes that.
        if node.origin == "ir":
            cfg_out = cfg_out_by_source.get(node.cfg_node_id, ())
            if cfg_by_id[node.cfg_node_id].exit_role == "routed_target":
                # Its one transition is the compiler-owned Stop wire, checked
                # for adjacency above; it has no CFG edges by construction.
                if [item.provenance for item in transitions] != ["synthetic"]:
                    raise _fail(
                        PROCESS_IR_COMPILE_EMISSION_PLAN_INVALID,
                        _PLAN_PHASE,
                        path,
                        "a routed target must carry exactly one synthetic stop wire",
                    )
            else:
                if [item.cfg_edge_id for item in transitions] != [
                    edge.edge_id for edge in cfg_out
                ]:
                    raise _fail(
                        PROCESS_IR_COMPILE_EMISSION_PLAN_INVALID,
                        _PLAN_PHASE,
                        path,
                        "plan transitions do not match the node's ordered CFG edges",
                    )
                # Otherwise a malformed plan could relabel an ordinary wire as
                # synthetic and skip CFG correspondence entirely.
                if any(item.provenance != "cfg_edge" for item in transitions):
                    raise _fail(
                        PROCESS_IR_COMPILE_EMISSION_PLAN_INVALID,
                        _PLAN_PHASE,
                        path,
                        "only Start and routed-target wires may be synthetic",
                    )
        elif node.synthetic_role == "start":
            if [item.provenance for item in transitions] != ["synthetic"]:
                raise _fail(
                    PROCESS_IR_COMPILE_EMISSION_PLAN_INVALID,
                    _PLAN_PHASE,
                    path,
                    "the synthetic start must carry exactly one synthetic wire",
                )
        else:
            # A synthetic Stop ends its path, full stop. Without this, a plan
            # with several exits could wire one terminal_stop onward to another
            # exit and omit it from ``terminal_shape_ids`` — the generic
            # terminal check only inspects shapes that ARE declared, so flow
            # would continue through a Stop the emitter cannot serialise.
            if transitions:
                raise _fail(
                    PROCESS_IR_SEMANTIC_AMBIGUOUS_FLOW,
                    _PLAN_PHASE,
                    path,
                    "a synthetic terminal stop must have no outgoing transitions",
                )
            if node.shape_id not in declared_terminals:
                raise _fail(
                    PROCESS_IR_COMPILE_EMISSION_PLAN_INVALID,
                    _PLAN_PHASE,
                    path,
                    "a synthetic terminal stop must be declared terminal",
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
            # A transition must correspond to a REAL CFG edge out of this node,
            # to that edge's target. Checking only that the target shape exists
            # would let a corrupted plan turn a message->stop edge into a
            # self-loop and still pass.
            if transition.provenance == "cfg_edge":
                edge = cfg_edges_by_id.get(transition.cfg_edge_id or "")
                if edge is None or edge.source_node_id != node.cfg_node_id:
                    raise _fail(
                        PROCESS_IR_COMPILE_EMISSION_PLAN_INVALID,
                        _PLAN_PHASE,
                        path,
                        "transition does not reference a CFG edge out of this node",
                    )
                if transition.to_shape_id != shape_for_cfg_node.get(
                    edge.target_node_id
                ):
                    raise _fail(
                        PROCESS_IR_COMPILE_EMISSION_PLAN_INVALID,
                        _PLAN_PHASE,
                        path,
                        "transition target does not match its CFG edge target",
                    )
            elif transition.cfg_edge_id is not None:
                raise _fail(
                    PROCESS_IR_COMPILE_EMISSION_PLAN_INVALID,
                    _PLAN_PHASE,
                    path,
                    "a synthetic transition must not claim a CFG edge",
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
            # Every branch dragpoint sits on the SAME row as an ordinary edge —
            # unlike Decision, a Branch has no second row (builder :4368).
            if any(item.y != DRAGPOINT_Y for item in transitions):
                raise _fail(
                    PROCESS_IR_COMPILE_EMISSION_PLAN_INVALID,
                    _PLAN_PHASE,
                    path,
                    "branch dragpoint row does not match parity geometry",
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
    # Canonical form: every shape with no outgoing flow, in plan order, each
    # exactly once. Membership-only checking accepted duplicates and arbitrary
    # order, either of which makes two equivalent plans serialise differently.
    canonical_terminals = [item.shape_id for item in nodes if not item.outgoing]
    # OMISSIONS first: a leaf shape left out of the declaration is a MISSING
    # terminal, not a nondeterminism defect. Checking canonical order first
    # reported every omission as NONDETERMINISTIC, contradicting the code map.
    # Set membership, not a list scan: ``declared_terminals`` is a list, so a
    # ``not in`` per shape would make this O(T^2) — the same complexity class
    # the symbol-index and edge-grouping fixes exist to prevent.
    declared_terminal_set = set(declared_terminals)
    missing = [
        shape for shape in canonical_terminals if shape not in declared_terminal_set
    ]
    if missing:
        raise _fail(
            PROCESS_IR_SEMANTIC_MISSING_TERMINAL,
            _PLAN_PHASE,
            by_shape[missing[0]].source_path or "",
            "a shape with no outgoing transition is not declared terminal",
        )
    # Whatever remains is a duplicate or a reordering — genuinely nondeterministic.
    # (The old trailing per-node re-check of the same predicate was removed: it
    # is exactly the ``missing`` test above and could never fire.)
    if declared_terminals != canonical_terminals:
        raise _fail(
            PROCESS_IR_COMPILE_NONDETERMINISTIC,
            _PLAN_PHASE,
            "",
            "terminal_shape_ids is not the canonical ordered set of terminal shapes",
        )
    # NOTE: there is deliberately no separate "declares no terminal" branch.
    # It would be dead: an empty canonical set means every node has an outgoing
    # transition, and since plan wiring must match forward-only CFG edges, the
    # highest-ordinal node could only wire to a shape that does not exist —
    # rejected earlier by the ordered-CFG-edge check. Verified by probe.


__all__: List[str] = [
    "check_cfg_invariants",
    "check_emission_plan_invariants",
]
