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
from .contracts import (
    DEFAULT_VALIDATION_CAPABILITIES,
    ProcessIRValidationCapabilitiesV1,
    ValidationDiagnosticV1,
)
from .context import PreparedProcessValidationV1
from .findings import finding
from .lineage import (
    CACHE,
    DDP,
    DPP,
    StateKey,
    _leg_member_index,
    _reads_of,
    _trusted_effects,
    _writes_of,
)

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


def _declared_replay_hazard(semantic, capabilities) -> Optional[str]:
    """A trusted contract that declares itself NOT replay-safe.

    The declaration is taken at face value, with NO state-footprint condition.
    An earlier version required ``effect.writes or effect.reads``, reasoning
    that a contract touching no tracked state replays nothing observable. That
    is exactly backwards: ``reads``/``writes`` describe DDP/DPP/cache only, so
    a script that posts to an external API, sends mail or moves a file has an
    EMPTY footprint and ``replay_safe`` is the only field that can describe it.
    Gating on the footprint therefore discarded the declaration precisely where
    it carried all of the information, and let a retry duplicate an action the
    author had explicitly marked unsafe.

    ``replay_safe`` defaults to False, so an author who has not considered
    replay gets the hazard reported rather than assumed away — the same
    fail-closed default the rest of the capability contract uses.
    """
    for effect in _trusted_effects(semantic, capabilities):
        if not effect.replay_safe:
            return "declared_unsafe_effect"
    return None


def collect_retry_effect_findings(
    prepared: PreparedProcessValidationV1,
    capabilities: ProcessIRValidationCapabilitiesV1 = DEFAULT_VALIDATION_CAPABILITIES,
) -> Tuple[ValidationDiagnosticV1, ...]:
    """Non-connector effects inside a region with a positive retry count.

    Regions come from #142's own ``derive_error_regions`` rather than a second
    walk of the catch edges — one derivation, so the gate cannot disagree with
    the compiler about what the region contains.

    A node with a TRUSTED effect contract is classified from that contract's
    ``replay_safe`` flag. Without this the flag was declared and never read: a
    map whose contract says it is NOT replay-safe would sit inside a retried
    region unreported, because ``_replay_hazard`` only recognises explicit
    cache and persisted-property writes.
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
                hazard = _declared_replay_hazard(node.semantic, capabilities)
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
    capabilities: ProcessIRValidationCapabilitiesV1 = DEFAULT_VALIDATION_CAPABILITIES,
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
        # A trusted contract's writes establish state exactly as an authored
        # write does — the lineage phase already treats them that way, and
        # omitting them here rejected a valid sequence (unknown async call, a
        # WAITING child that declares it writes A, then a reader of A) because
        # nothing in this set could account for A.
        #
        # A non-waiting call's own declared writes are excluded: they are the
        # hazard under investigation, not a remedy for it. Counting them would
        # let an async writer exempt the very read that races it.
        if node.semantic.semantic_kind == "process_call" and not getattr(
            node.semantic, "wait", True
        ):
            continue
        for effect in _trusted_effects(node.semantic, capabilities):
            for key in effect.writes:
                if key[0] != DDP:
                    in_process_writes.add((key[0], key[1]))

    for call in non_waiting:
        downstream = _execution_downstream(prepared, call.node_id)
        summary = capabilities.subprocess_effect(call.semantic.process_ref)

        if summary is None:
            # No typed summary: the child's effects are undeclared, so the
            # ordering is unproven rather than wrong.
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

        # A declared summary makes the hazard SHARPER, not softer: now we know
        # exactly which keys the child writes, and a downstream read of one of
        # them behind wait=False is demonstrably unordered.
        declared = (
            frozenset((k[0], k[1]) for k in summary.writes) if summary else frozenset()
        )

        for node_id in downstream:
            node = prepared.node(node_id)
            if node is None:
                continue
            # Authored reads AND a trusted contract's declared reads. Scanning
            # only `_reads_of` left the hazard unreported in the one shape that
            # can actually express it: a non-waiting call is authorable in an
            # ORCHESTRATION root, where every sibling step is itself a
            # process_call and no authored read can appear at all. So the
            # contract read was not merely an extra case — downstream of a
            # non-waiting call it is the ONLY kind of read there is, and the
            # lineage phase happily proves it "established" by applying the
            # child's writes synchronously.
            reads = [
                (key, has_default, strict)
                for key, has_default, strict in _reads_of(node.semantic)
            ]
            for effect in _trusted_effects(node.semantic, capabilities):
                # A declared read is exact and always strict: the contract
                # asserts the effect consumes the key, so there is no wire
                # default that could absorb an unordered write.
                reads.extend(((raw[0], raw[1]), False, True) for raw in effect.reads)

            for key, has_default, strict in reads:
                if has_default or key[0] == DDP:
                    continue
                # A non-strict reader (a Decision operand) carries a defined
                # empty default on the wire, so an unordered write cannot make
                # it fail — same rule the lineage phase applies.
                if not strict:
                    continue
                if summary is not None:
                    # The summary is EXACT, so a key the child does not write
                    # cannot race this call. Only `declared` decides. Testing
                    # solely `key in in_process_writes and key not in declared`
                    # reported a read of an unwritten key as an ordering hazard
                    # of a call demonstrably unrelated to it — while the
                    # lineage phase was already reporting the real defect
                    # ("nothing writes this") correctly.
                    if key not in declared:
                        continue
                elif key in in_process_writes:
                    # The child's effects are unknown, but something in this
                    # process does write the key, so the read does not depend
                    # on the call.
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


def _execution_downstream(
    prepared: PreparedProcessValidationV1, start: str
) -> Tuple[str, ...]:
    """Every node that may execute AFTER ``start``.

    Graph descendants are not enough. Branch legs FAN OUT from the Branch node
    and terminate separately, so no node in leg 1 is a descendant of any node
    in leg 0 — yet legs run SEQUENTIALLY, which is precisely why an earlier
    leg's execution-scoped write is visible to a later one. A ``wait=False``
    call in leg 0 can therefore still be running while leg 1 reads what its
    child writes, and a descendants-only scan reported nothing at all for that.

    Later legs of every enclosing Branch are added, so a nested Branch picks up
    the later legs of both its own Branch and the outer one.
    """
    nodes: Set[str] = set(_downstream_nodes(prepared, start))
    legs = _leg_member_index(prepared)
    for (branch_id, ordinal), members in legs.items():
        if start not in members:
            continue
        for (other_branch, other_ordinal), other_members in legs.items():
            if other_branch == branch_id and other_ordinal > ordinal:
                nodes.update(other_members)
    nodes.discard(start)
    return tuple(sorted(nodes))


def collect_effect_findings(
    prepared: PreparedProcessValidationV1,
    capabilities: ProcessIRValidationCapabilitiesV1 = DEFAULT_VALIDATION_CAPABILITIES,
) -> Tuple[ValidationDiagnosticV1, ...]:
    return collect_retry_effect_findings(
        prepared, capabilities
    ) + collect_ordering_findings(prepared, capabilities)


__all__ = [
    "collect_effect_findings",
    "collect_ordering_findings",
    "collect_retry_effect_findings",
]
