"""Prepared validation context: one snapshot, one CFG, ordered indexes (#143).

DARK in slice 2 — nothing calls this yet.

Why the IR is re-validated on entry
-----------------------------------
``ProcessIRV1`` is strict but NOT frozen (unlike the compiler's own contracts,
which are). A caller therefore holds a mutable model, and a validator that read
it directly could be handed an object mutated *after* its last check — the
classic time-of-check/time-of-use gap. Worse, this validator's whole purpose is
to gate a mutation, so a payload that passes validation and then changes is
precisely the failure mode that matters.

The fix is to re-validate a dump at entry and use only that snapshot:

    snapshot = ProcessIRV1.model_validate(ir.model_dump(warnings=False))

Every later phase reads ``prepared.ir``, never the caller's object. Lowering also
happens exactly once here, so validation and the emission that follows it are
guaranteed to be talking about the same graph rather than two independent lowerings
that could disagree.

Why preparation defects are NOT report entries
----------------------------------------------
A failure to snapshot or lower is a *compiler* defect, not an authored one, and
``PROCESS_IR_COMPILE_*`` is not a family this issue owns (ADR-001 §7). So this
module never converts such a failure into a ``ValidationReportV1`` entry — it
lets the exception escape to the compiler's existing ``_guarded`` boundary in
``pipeline.compile_process_ir_v1``, which already renders it as a static,
value-free ``PROCESS_IR_COMPILE_INTERNAL``. Swallowing it here would relabel a
compiler bug as a user error, which is how a caller ends up "fixing" correct
input.

Purity
------
No network, no filesystem, no clock, no global state, no mutation. The same
``(ir, symbols)`` pair always yields the same prepared context.
"""

from __future__ import annotations

from typing import Dict, List, Mapping, Optional, Tuple

from pydantic import ConfigDict

from ....models.process_ir import ProcessIRV1
from ..contracts import (
    CfgEdgeV1,
    CfgNodeV1,
    ComponentSymbolV1,
    SemanticCfgV1,
    SymbolTableV1,
)
from ..lowering import lower_process_ir_to_cfg
from .contracts import _ValidationModel


class PreparedProcessValidationV1(_ValidationModel):
    """Everything the phases read, derived once and shared.

    PRIVATE to the package: it is not re-exported from ``__init__``. Exporting it
    would let a caller assemble a context whose CFG does not correspond to its
    IR, and every phase trusts that correspondence.
    """

    # Mappings are genuinely useful here (phases do id lookups in loops) and this
    # model is internal, so the package-wide "tuples only" rule is relaxed —
    # but the mappings are built once and never mutated after construction.
    model_config = ConfigDict(extra="forbid", frozen=True, arbitrary_types_allowed=True)

    ir: ProcessIRV1
    cfg: SemanticCfgV1
    symbols: SymbolTableV1

    #: node_id -> node
    node_by_id: Mapping[str, CfgNodeV1]
    #: source node_id -> its outgoing edges, in local ordinal order
    outgoing: Mapping[str, Tuple[CfgEdgeV1, ...]]
    #: target node_id -> its incoming edges, in local ordinal order
    incoming: Mapping[str, Tuple[CfgEdgeV1, ...]]
    #: authored ref -> resolved symbol
    symbol_by_ref: Mapping[str, ComponentSymbolV1]

    def node(self, node_id: str) -> Optional[CfgNodeV1]:
        return self.node_by_id.get(node_id)

    def successors(self, node_id: str) -> Tuple[CfgEdgeV1, ...]:
        return self.outgoing.get(node_id, ())

    def predecessors(self, node_id: str) -> Tuple[CfgEdgeV1, ...]:
        return self.incoming.get(node_id, ())

    def symbol(self, ref: Optional[str]) -> Optional[ComponentSymbolV1]:
        if ref is None:
            return None
        return self.symbol_by_ref.get(ref)


def _edge_index(
    edges: Tuple[CfgEdgeV1, ...], key: str
) -> Mapping[str, Tuple[CfgEdgeV1, ...]]:
    """Group edges by an endpoint, ordered by ``local_ordinal`` then ``edge_id``.

    Ordering is part of the meaning, not a convenience: Branch legs execute in
    ``local_ordinal`` order, and the lineage phase reads earlier-leg state as
    visible to later legs. An arbitrary grouping order would silently change
    which leg counts as "earlier".

    ``edge_id`` breaks ties. Two edges out of one node should never share a
    local ordinal, but this index is also built over hand-constructed CFGs in
    tests, and an index that reorders on malformed input is not an index.
    """
    grouped: Dict[str, List[CfgEdgeV1]] = {}
    for edge in edges:
        grouped.setdefault(getattr(edge, key), []).append(edge)
    return {
        node_id: tuple(
            sorted(items, key=lambda e: (e.local_ordinal, _edge_sort_id(e.edge_id)))
        )
        for node_id, items in grouped.items()
    }


def _edge_sort_id(edge_id: str) -> Tuple[int, str]:
    """Numeric-ascending sort for ``eN`` ids.

    Lexical order puts ``e10`` before ``e2``; the compiler's determinism
    contract is numeric-ascending, so a lexical tiebreak here would contradict
    the ordering every other layer uses.
    """
    digits = edge_id[1:]
    return (int(digits), edge_id) if digits.isdigit() else (0, edge_id)


def prepare_validation_context(
    ir: ProcessIRV1, symbols: SymbolTableV1
) -> PreparedProcessValidationV1:
    """Snapshot the IR, lower it once, and index the result.

    Raises whatever the model or lowering raises. That is deliberate — see the
    module docstring on why preparation defects must not become report entries.
    """
    # Re-validate a dump rather than trusting the caller's (mutable) model.
    #
    # `warnings=False` is load-bearing (§6 AR2-01, the same reason the composer
    # and the authoring intake give): this is a public compiler entry, so `ir`
    # may be a model the caller still holds and has mutated — and dumping a
    # mutated model makes pydantic render the caller's authored content, a
    # secret included, into a serializer warning before this value-free
    # re-validation ever runs.
    snapshot = ProcessIRV1.model_validate(ir.model_dump(warnings=False))
    cfg = lower_process_ir_to_cfg(snapshot)

    return PreparedProcessValidationV1(
        ir=snapshot,
        cfg=cfg,
        symbols=symbols,
        node_by_id={node.node_id: node for node in cfg.nodes},
        outgoing=_edge_index(cfg.edges, "source_node_id"),
        incoming=_edge_index(cfg.edges, "target_node_id"),
        symbol_by_ref={symbol.ref: symbol for symbol in symbols.symbols},
    )


__all__ = ["PreparedProcessValidationV1", "prepare_validation_context"]
