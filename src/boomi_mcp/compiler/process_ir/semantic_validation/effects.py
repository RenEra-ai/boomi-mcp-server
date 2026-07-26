"""Side-effect classification and NON-connector replay safety (#143). DARK.

Division of labour with #142
----------------------------
Everything about CONNECTOR retry safety already exists and is authoritative:
source re-execution, a retried call whose registry row is
``non_idempotent``/``unverified``, and missing typed idempotency evidence all
keep their #142 codes and are reached through the delegation in ``flow.py``.
This module deliberately adds nothing there.

What it adds is the gap #142 left: a retried region can also replay effects that
are not connector calls at all. Two of them are derivable from the IR with no
extra metadata:

* a ``cache_put`` — replaying it re-writes the cache;
* a ``set_property`` with ``persist=True`` — a PERSISTED property outlives the
  execution, so a replay overwrites state a later run will read.

Both get ``PROCESS_IR_SEMANTIC_RETRY_EFFECT_UNSAFE``, which exists precisely
because #142's codes are connector-shaped and would be the wrong label here.

Ordering
--------
``process_call`` with ``wait=False`` is the one ordering hazard the IR states
outright: the parent does not wait, so nothing downstream can depend on what the
child did. Whether that is a real defect depends on facts the IR does not carry
(what the child actually writes), so the two outcomes are deliberately different
severities:

* the child's effects are undeclared -> ``…SIDE_EFFECT_ORDERING_UNKNOWN``, a
  WARNING. Unproven is not the same as wrong, and blocking here would reject
  every non-waiting call in the repo.
* a downstream read depends on execution state and no in-process writer
  establishes it -> ``…SIDE_EFFECT_ORDERING_UNSAFE``, an ERROR. The dependency
  is demonstrated: the only candidate writer is a call the flow does not wait
  for.

That asymmetry is the issue's "reject demonstrated hazards only" rule.

Reachability of the UNSAFE branch — stated plainly
--------------------------------------------------
As of ProcessIR v1 the AUTHORED surface cannot express the unsafe shape. A
``process_call`` may appear only in a pure process-call sequence
(``process_call_connector_mixing`` is gated), and it is rejected outright inside
a Branch or Decision body, so no property read can follow a non-waiting call in
any authorable document. The CFG can represent it; the schema will not currently
produce it.

The rule is implemented and tested against a hand-built CFG rather than dropped,
and this note exists so the branch is not mistaken for accidentally-dead code. It
is deliberately pre-positioned: the day the mixing gate lifts, the hazard becomes
authorable, and a validator that had to grow the rule at the same moment the gate
opened would be the wrong sequencing. The honest cost is that this one branch is
proven by a synthetic CFG, not by a golden document.
"""

from __future__ import annotations

from typing import Dict, FrozenSet, List, Optional, Set, Tuple

from ....errors import (
    PROCESS_IR_SEMANTIC_RETRY_EFFECT_UNSAFE,
    PROCESS_IR_SEMANTIC_SIDE_EFFECT_ORDERING_UNKNOWN,
    PROCESS_IR_SEMANTIC_SIDE_EFFECT_ORDERING_UNSAFE,
)
from ..error_handling import derive_error_regions
from .contracts import ValidationDiagnosticV1
from .context import PreparedProcessValidationV1
from .findings import finding
from .lineage import CACHE, DDP, DPP, StateKey, _reads_of, _writes_of

_RETRY_PHASE = "retry"
_SIDE_EFFECT_PHASE = "side_effect"


def _replay_hazard(semantic) -> Optional[str]:
    """The non-connector replay hazard a node carries, as a closed token."""
    kind = semantic.semantic_kind
    if kind == "cache_put":
        return "cache_write"
    if kind == "set_property" and getattr(semantic, "persist", False):
        # A NON-persisted property dies with the execution, so replaying its
        # write is harmless. Persistence is exactly what makes it a hazard.
        return "persisted_property"
    return None


def collect_retry_effect_findings(
    prepared: PreparedProcessValidationV1,
) -> Tuple[ValidationDiagnosticV1, ...]:
    """Non-connector effects inside a region with a positive retry count.

    Regions come from #142's own ``derive_error_regions`` rather than a second
    walk of the catch edges — one derivation, so the gate cannot disagree with
    the compiler about what the region contains.
    """
    findings: List[ValidationDiagnosticV1] = []

    for region in derive_error_regions(prepared.cfg):
        if region.retry_count <= 0:
            continue
        for node_id in region.try_node_ids:
            node = prepared.node(node_id)
            if node is None:
                continue
            hazard = _replay_hazard(node.semantic)
            if hazard is None:
                continue
            findings.append(
                finding(
                    PROCESS_IR_SEMANTIC_RETRY_EFFECT_UNSAFE,
                    "error",
                    _RETRY_PHASE,
                    node.source_path,
                    evidence=(
                        ("effect_kind", hazard),
                        ("retry_count", region.retry_count),
                    ),
                    internal_node_id=node_id,
                )
            )

    return tuple(findings)


def collect_ordering_findings(
    prepared: PreparedProcessValidationV1,
) -> Tuple[ValidationDiagnosticV1, ...]:
    """Ordering hazards created by a non-waiting subprocess call."""
    findings: List[ValidationDiagnosticV1] = []

    non_waiting = [
        node
        for node in prepared.cfg.nodes
        if node.semantic.semantic_kind == "process_call"
        and not getattr(node.semantic, "wait", True)
    ]
    if not non_waiting:
        return ()

    # Every execution-scoped key some node in this process writes. A read of a
    # key nothing here writes, downstream of a non-waiting call, has no
    # candidate writer other than that call.
    in_process_writes: Set[StateKey] = set()
    for node in prepared.cfg.nodes:
        for key in _writes_of(node.semantic):
            if key[0] != DDP:
                in_process_writes.add(key)

    for call in non_waiting:
        downstream = _downstream_nodes(prepared, call.node_id)

        findings.append(
            finding(
                PROCESS_IR_SEMANTIC_SIDE_EFFECT_ORDERING_UNKNOWN,
                "warning",
                _SIDE_EFFECT_PHASE,
                call.source_path,
                evidence=(("wait", False), ("effect_kind", "subprocess")),
                internal_node_id=call.node_id,
            )
        )

        for node_id in downstream:
            node = prepared.node(node_id)
            if node is None:
                continue
            for key, has_default in _reads_of(node.semantic):
                if has_default or key[0] == DDP:
                    continue
                if key in in_process_writes:
                    continue
                findings.append(
                    finding(
                        PROCESS_IR_SEMANTIC_SIDE_EFFECT_ORDERING_UNSAFE,
                        "error",
                        _SIDE_EFFECT_PHASE,
                        node.source_path,
                        evidence=(
                            ("state_scope", key[0]),
                            ("wait", False),
                        ),
                        internal_node_id=node_id,
                    )
                )

    return tuple(findings)


def _downstream_nodes(
    prepared: PreparedProcessValidationV1, start: str
) -> Tuple[str, ...]:
    seen: Set[str] = set()
    stack = [e.target_node_id for e in prepared.successors(start)]
    while stack:
        current = stack.pop()
        if current in seen:
            continue
        seen.add(current)
        for edge in prepared.successors(current):
            stack.append(edge.target_node_id)
    return tuple(sorted(seen))


def collect_effect_findings(
    prepared: PreparedProcessValidationV1,
) -> Tuple[ValidationDiagnosticV1, ...]:
    return collect_retry_effect_findings(prepared) + collect_ordering_findings(prepared)


__all__ = [
    "collect_effect_findings",
    "collect_ordering_findings",
    "collect_retry_effect_findings",
]
