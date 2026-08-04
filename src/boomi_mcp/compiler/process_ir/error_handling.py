"""Try/Catch error regions and retry safety (issue #142, M12.7).

The ONE authority on two questions the rest of the compiler must not answer for
itself:

1. *Which nodes does a retry actually re-run?* — :func:`derive_error_regions`
   walks each ``try_catch`` node's two subtrees and returns them as a pair of
   node-id sets.
2. *May those nodes be re-run?* — :func:`validate_error_handling` rejects a
   positive retry whose region would re-execute the flow's document source, and
   any retried call whose action has no registry-backed replay safety.

Regions are DERIVED, never stored. No CFG node, edge, or emission-plan row
carries a region id or a "is in the catch row" flag: lowering calls
:func:`catch_region_node_ids` to place shapes, and the plan invariant checker
calls it again to verify them. That is deliberate — a stored membership flag
would be a second copy of a graph fact, and the checker would then be comparing
lowering's answer against lowering's own note rather than against the graph
(the duplicate-authority failure #140 removed for connection refs, and #141 for
control-branch membership).

Source isolation is likewise derived STRUCTURALLY, from the graph, rather than
read off the authored ``scope``. A scope value is an authored string; the
question "would this retry re-run whatever produced the documents?" is a
property of the graph. Deriving it means a mutated model whose scope disagrees
with its shape cannot smuggle an unsafe region past the check.

Security (ADR-001 §11): every message is a static string selected by code. No
ref, contract name, key, action, family, header, payload, or connector response
is ever interpolated — a diagnostic names the authored JSON path and nothing
else.

**Charter (amended by #146).** Compiler-internal: never exported from
``compiler.process_ir.__all__``, and no MCP tool, schema, or builder may reach
it — with ONE named exception. ``boomi_mcp.authoring.process_ir_projection`` may
call :func:`retry_rule_specs` to publish the RULE (which classification permits
a retry, and what evidence it demands) in the read-only ``process_ir_authoring``
contract.

Only the rule crosses. The region derivation is the part that must not: error
regions are node-id sets over the control-flow graph, and a node id is exactly
the kind of internal handle ADR-001 §6 keeps off an LLM-facing surface. Nothing
here exposes :class:`ErrorRegionV1`, a CFG, or any node id.
"""

from __future__ import annotations

from types import MappingProxyType
from typing import Any, Dict, FrozenSet, List, Mapping, Optional, Tuple

from ...errors import (
    PROCESS_IR_COMPILE_ERROR_REGION_INVALID,
    PROCESS_IR_SEMANTIC_IDEMPOTENCY_EVIDENCE_MISSING,
    PROCESS_IR_SEMANTIC_RETRY_NON_IDEMPOTENT_WRITE,
    PROCESS_IR_SEMANTIC_RETRY_SOURCE_REEXECUTION,
)
from .contracts import SemanticCfgV1, SymbolTableV1, _CompilerModel
from .diagnostics import raise_compile_error

#: The phase every #142 check reports under. All of them run before an emission
#: plan exists, so nothing here can be reached after a component was mutated.
_SEMANTIC_PHASE = "semantic_lowering"

#: Node kinds that PRODUCE the documents a flow works on. A retry region that
#: contains none of these, with none upstream of it, is retrying the producer.
_PRODUCING_KINDS = frozenset({"connector_call", "connector"})

#: Evidence kind required per retry-safety classification. A row absent from this
#: map needs no evidence (``read_only``) or can never be retried at all
#: (``non_idempotent``/``unverified``) — both handled explicitly below, so this
#: map is never the thing that decides whether a retry is allowed.
_REQUIRED_EVIDENCE_KIND: Mapping[str, str] = {
    "idempotent_write": "verified_action",
    "conditionally_idempotent": "key_reference",
}

#: Classifications that can NEVER authorise a positive retry, whatever evidence
#: is attached. ``unverified`` is the fail-closed sentinel for "nobody has
#: established this"; ``non_idempotent`` is a positive statement that replay is
#: unsafe. Neither is overridable by a caller assertion — that is the whole point
#: of routing the decision through the registry instead of through the payload.
_NEVER_RETRYABLE = frozenset({"non_idempotent", "unverified"})


class ErrorRegionV1(_CompilerModel):
    """One ``try_catch`` node's two subtrees, as derived node-id sets.

    Compiler-internal and transient: built on demand, compared, and dropped. It
    is never attached to a CFG or plan model and never serialised.
    """

    try_catch_node_id: str
    source_path: str
    retry_count: int
    try_node_ids: Tuple[str, ...]
    catch_node_ids: Tuple[str, ...]


def _region_defect(path: str, node_id: Optional[str] = None):
    return raise_compile_error(
        PROCESS_IR_COMPILE_ERROR_REGION_INVALID,
        _SEMANTIC_PHASE,
        path,
        internal_node_id=node_id,
    )


def _outgoing_index(cfg: SemanticCfgV1) -> Dict[str, List[Any]]:
    outgoing: Dict[str, List[Any]] = {}
    for edge in cfg.edges:
        outgoing.setdefault(edge.source_node_id, []).append(edge)
    return outgoing


def _collect_subtree(
    start_node_id: str,
    outgoing: Mapping[str, List[Any]],
    *,
    path: str,
    limit: int,
) -> Tuple[str, ...]:
    """Every node reachable from ``start_node_id``, inclusive.

    ``limit`` (the CFG's node count) bounds the walk. ``check_cfg_invariants``
    has already proven the graph is an acyclic tree before this runs, so the
    bound can only be hit by a defect — and hitting it raises rather than looping
    forever, which is the difference between a reported compiler bug and a hung
    process.
    """
    seen: List[str] = []
    seen_set = set()
    stack = [start_node_id]
    while stack:
        node_id = stack.pop()
        if node_id in seen_set:
            # A tree has no re-entry. Reaching one means the region overlaps
            # itself, which the caller reports as a structural defect.
            raise _region_defect(path, node_id)
        seen_set.add(node_id)
        seen.append(node_id)
        if len(seen) > limit:  # pragma: no cover - defect backstop
            raise _region_defect(path, node_id)
        for edge in outgoing.get(node_id, ()):
            stack.append(edge.target_node_id)
    return tuple(seen)


def derive_error_regions(cfg: SemanticCfgV1) -> Tuple[ErrorRegionV1, ...]:
    """Derive one region per ``try_catch`` node, validating the wiring as it goes.

    Raises ``PROCESS_IR_COMPILE_ERROR_REGION_INVALID`` when the derived structure
    is impossible: a ``catch`` edge out of a node that is not a ``try_catch``, a
    ``try_catch`` without exactly one ordering + one catch successor, swapped
    local ordinals, or subtrees that overlap.
    """
    by_id = {node.node_id: node for node in cfg.nodes}
    outgoing = _outgoing_index(cfg)
    limit = len(cfg.nodes)

    # A ``catch`` edge is meaningful ONLY out of an error handler. Checked over
    # every edge (not just the ones reached from a try_catch) so a catch edge
    # grafted onto an unrelated node is caught even though no region walk would
    # ever visit it.
    for edge in cfg.edges:
        if edge.kind != "catch":
            continue
        source = by_id.get(edge.source_node_id)
        if source is None or source.semantic.semantic_kind != "try_catch":
            raise _region_defect(edge.provenance_path, edge.source_node_id)

    regions: List[ErrorRegionV1] = []
    for node in cfg.nodes:
        if node.semantic.semantic_kind != "try_catch":
            continue
        successors = sorted(
            outgoing.get(node.node_id, ()), key=lambda item: item.local_ordinal
        )
        if len(successors) != 2:
            raise _region_defect(node.source_path, node.node_id)
        try_edge, catch_edge = successors
        if (
            try_edge.kind != "ordering"
            or catch_edge.kind != "catch"
            or try_edge.local_ordinal != 1
            or catch_edge.local_ordinal != 2
        ):
            raise _region_defect(node.source_path, node.node_id)

        try_ids = _collect_subtree(
            try_edge.target_node_id, outgoing, path=node.source_path, limit=limit
        )
        catch_ids = _collect_subtree(
            catch_edge.target_node_id, outgoing, path=node.source_path, limit=limit
        )
        # The two paths must stay disjoint. They cannot converge (no joins) and
        # cannot cross-link, so an overlap means the edges were mis-wired.
        if set(try_ids) & set(catch_ids):
            raise _region_defect(node.source_path, node.node_id)
        # Neither subtree may swallow the handler itself.
        if node.node_id in try_ids or node.node_id in catch_ids:
            raise _region_defect(node.source_path, node.node_id)

        regions.append(
            ErrorRegionV1(
                try_catch_node_id=node.node_id,
                source_path=node.source_path,
                retry_count=node.semantic.retry_count,
                try_node_ids=try_ids,
                catch_node_ids=catch_ids,
            )
        )
    return tuple(regions)


def catch_region_node_ids(cfg: SemanticCfgV1) -> FrozenSet[str]:
    """Every node on a recovery path, across all handlers.

    A pure function of the CFG — this is what lowering uses to place catch-row
    shapes and what the plan invariant checker calls again to verify them.
    """
    ids: set = set()
    for region in derive_error_regions(cfg):
        ids.update(region.catch_node_ids)
    return frozenset(ids)


def _producers_upstream_of(
    cfg: SemanticCfgV1, target_node_id: str
) -> bool:
    """Does any node STRICTLY upstream of ``target_node_id`` produce documents?

    This is the source-isolation question, asked of the graph rather than of the
    authored scope. The CFG is a join-free tree at this point, so each node has
    at most one predecessor and "upstream" is an unambiguous walk to the entry.
    """
    by_id = {node.node_id: node for node in cfg.nodes}
    predecessor: Dict[str, str] = {}
    for edge in cfg.edges:
        # At most one predecessor per node — joins are already rejected. A second
        # one would make "the path to here" ambiguous, so it is a defect.
        if edge.target_node_id in predecessor:  # pragma: no cover - defect backstop
            raise _region_defect(edge.provenance_path, edge.target_node_id)
        predecessor[edge.target_node_id] = edge.source_node_id

    seen = set()
    current = predecessor.get(target_node_id)
    while current is not None:
        if current in seen:  # pragma: no cover - defect backstop
            raise _region_defect(by_id[current].source_path, current)
        seen.add(current)
        node = by_id.get(current)
        if node is None:  # pragma: no cover - defect backstop
            raise _region_defect(target_node_id)
        if node.semantic.semantic_kind in _PRODUCING_KINDS:
            return True
        current = predecessor.get(current)
    return False


def validate_error_handling(
    cfg: SemanticCfgV1,
    bindings: Tuple[Any, ...],
    symbols: SymbolTableV1,
) -> None:
    """Reject unsafe retry intent before anything is emitted.

    ``bindings`` are the already-resolved connector-call bindings. They are
    passed in rather than re-resolved so the retry check and the ordinary flow
    checks provably consult the SAME resolution — resolving twice would let the
    two disagree, and the whole point of the safety gate is that it sees what the
    emitter will see.
    """
    regions = derive_error_regions(cfg)
    if not regions:
        # No error handler authored: nothing here applies, and every pre-#142
        # payload takes this exit unchanged.
        return

    binding_by_node = {binding.node_id: binding for binding in bindings}
    contracts = symbols.build_idempotency_index()

    for region in regions:
        if region.retry_count == 0:
            # Retry zero re-runs nothing, so neither source isolation nor replay
            # safety can be violated. A write with no retry is ordinary.
            continue

        # --- source isolation ------------------------------------------------
        # Nothing upstream produced the documents, so the producer is INSIDE the
        # retried region: replaying it re-runs the producer and duplicates
        # everything it already emitted.
        if not _producers_upstream_of(cfg, region.try_catch_node_id):
            raise raise_compile_error(
                PROCESS_IR_SEMANTIC_RETRY_SOURCE_REEXECUTION,
                _SEMANTIC_PHASE,
                "{0}/retry/count".format(region.source_path),
                internal_node_id=region.try_catch_node_id,
            )

        # --- replay safety of every retried call ------------------------------
        # Only the TRY subtree is retried. A call on the recovery path runs once,
        # after the retries are exhausted, so it is deliberately not checked here.
        for node_id in region.try_node_ids:
            binding = binding_by_node.get(node_id)
            if binding is None:
                continue
            safety = binding.capability.retry_safety
            evidence = _authored_evidence(cfg, node_id)

            if safety in _NEVER_RETRYABLE:
                # Checked BEFORE evidence on purpose: an authored claim must not
                # be able to turn an unclassified or known-unsafe write into a
                # retryable one. Evidence discharges an obligation; it never
                # grants permission.
                raise raise_compile_error(
                    PROCESS_IR_SEMANTIC_RETRY_NON_IDEMPOTENT_WRITE,
                    _SEMANTIC_PHASE,
                    "{0}/operation_ref".format(binding.source_path),
                    internal_node_id=node_id,
                )

            required = _REQUIRED_EVIDENCE_KIND.get(safety)
            if required is None:
                # ``read_only``: replaying re-reads. Evidence is not required —
                # but if the caller attached a contract reference anyway it must
                # still resolve, because a dangling reference is a broken payload
                # whichever row it sits on.
                _require_resolvable(evidence, contracts, binding, node_id)
                continue

            if evidence is None or evidence.kind != required:
                raise _evidence_missing(binding, node_id)

            if required == "key_reference":
                contract = contracts.get(evidence.contract_ref or "")
                # The contract must exist AND cover THIS operation. A contract
                # for a different operation is not evidence about this call, and
                # accepting it would make the binding decorative.
                if contract is None or contract.operation_ref != binding.operation_ref:
                    raise _evidence_missing(binding, node_id)


def _authored_evidence(cfg: SemanticCfgV1, node_id: str) -> Optional[Any]:
    for node in cfg.nodes:
        if node.node_id == node_id:
            return getattr(node.semantic, "idempotency", None)
    return None  # pragma: no cover - node ids come from the same CFG


def _require_resolvable(evidence, contracts, binding, node_id: str) -> None:
    if evidence is None or evidence.kind != "key_reference":
        return
    contract = contracts.get(evidence.contract_ref or "")
    if contract is None or contract.operation_ref != binding.operation_ref:
        raise _evidence_missing(binding, node_id)


def _evidence_missing(binding, node_id: str):
    return raise_compile_error(
        PROCESS_IR_SEMANTIC_IDEMPOTENCY_EVIDENCE_MISSING,
        _SEMANTIC_PHASE,
        "{0}/idempotency".format(binding.source_path),
        internal_node_id=node_id,
    )


#: Every replay classification the connector registry can assign. Listed here so
#: the projection publishes a CLOSED set: a classification that exists on a row
#: but not in this tuple would be projected with no rule beside it, which is the
#: silent gap the fail-closed design exists to prevent.
_REPLAY_CLASSIFICATIONS: Tuple[str, ...] = (
    "conditionally_idempotent",
    "idempotent_write",
    "non_idempotent",
    "read_only",
    "unverified",
)


def retry_rule_specs() -> Tuple[Mapping[str, Any], ...]:
    """The retry rule per replay classification, as sorted public data.

    DERIVED from the two tables the checks actually consult
    (:data:`_NEVER_RETRYABLE` and :data:`_REQUIRED_EVIDENCE_KIND`), never
    restated — a hand-written copy of this rule is how a served contract ends up
    promising a retry the compiler refuses.

    The shape a caller needs is three facts per classification:

    * ``retry_permitted`` — may a call so classified sit inside a retried region
      at all? ``False`` for ``non_idempotent`` and ``unverified``, and that
      refusal is absolute: no evidence a caller attaches can lift it, because the
      registry decides replay safety, not the payload;
    * ``required_evidence`` — when a retry IS permitted, the evidence kind that
      discharges the obligation (``verified_action`` for ``idempotent_write``,
      ``key_reference`` for ``conditionally_idempotent``), or empty when none is
      needed (``read_only``);
    * ``evidence_can_authorise`` — always ``False`` where ``retry_permitted`` is
      ``False``. Published explicitly rather than left inferable, because
      "attach evidence and it will work" is the single most likely wrong reading
      of the row.
    """
    rows = []
    for classification in _REPLAY_CLASSIFICATIONS:
        permitted = classification not in _NEVER_RETRYABLE
        rows.append(
            MappingProxyType(
                {
                    "replay_classification": classification,
                    "retry_permitted": permitted,
                    "required_evidence": (
                        _REQUIRED_EVIDENCE_KIND.get(classification, "")
                        if permitted
                        else ""
                    ),
                    "evidence_can_authorise": permitted,
                }
            )
        )
    return tuple(rows)


__all__ = [
    "ErrorRegionV1",
    "catch_region_node_ids",
    "derive_error_regions",
    "retry_rule_specs",
    "validate_error_handling",
]
