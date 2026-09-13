"""Compiler entry policy (issue #158 / M12.20, extended by #184).

How a compiled process ENTERS is one decision with three consequences: which
physical Start form ``shape1`` carries, which execution profile the process is
materialized with, and — for an explicit entry — which CFG node that Start
absorbs and which node its single wire reaches. Before #158 the first was
hard-coded (``start_noaction`` at every synthesis and invariant site) and the
second was re-derived separately from the entry's connector family. This module
is the one authority for all three, so they cannot disagree.

There are exactly three admitted forms, and the set is CLOSED:

* **scheduled** — a compiler-synthesized ``start_noaction`` wired to the CFG
  entry node. Every root that has no explicit entry.
* **listener** — the CFG entry IS the authored ``listener`` node, and the
  compiler FUSES it with the Start: one synthesized ``start_listen`` carrying the
  Listen action for the resolved operation, wired to the listener's sole
  successor. The listener node has no shape of its own.
* **passthrough** (#184) — the CFG entry IS the authored ``passthrough`` node,
  fused the same way: one synthesized ``start_passthrough`` carrying the entry's
  label, wired to its sole successor. It needs no symbol at all.

The two FUSED forms are the EXPLICIT entries: each is declared by the one CFG
semantic kind its row names, and :data:`FUSED_ENTRY_SEMANTIC_KINDS` is derived
from those rows — lowering's connector-role suppression and the CFG invariants
read that set rather than naming a kind of their own.

In every form the Start remains compiler-owned: ``origin="synthetic"``, fixed
geometry, one synthetic wire. A caller can author neither form's geometry nor
its wiring; a fused form only changes WHICH input the synthesized Start
carries, and that input is re-derived from the CFG and the symbol table
wherever it is checked.

Two entry points, deliberately different in strictness:

* :func:`classify_entry` is TOLERANT and reads only the node the CFG names as
  its entry. It answers "which execution profile" for callers that may hold a
  partial graph — the served revision oracle builds stand-in graphs with no
  edges at all — and it must never fail on a shape it cannot classify.
* :func:`derive_process_entry` is STRICT. Emission planning and the plan
  invariants depend on its answer for geometry and wiring, so an explicit entry
  that is not the CFG entry, a second explicit entry (of either kind), or one
  without exactly one successor is a compiler defect and raises.
"""

from __future__ import annotations

from dataclasses import dataclass
from types import MappingProxyType
from typing import Any, FrozenSet, Mapping, Optional, Tuple

from ...errors import (
    PROCESS_IR_COMPILE_CONNECTOR_BINDING_INVALID,
    PROCESS_IR_COMPILE_INTERNAL,
)
from .contracts import StartListenInputV1, StartNoActionInputV1, StartPassthroughInputV1
from .diagnostics import raise_compile_error

#: The three execution profiles, as the materializer keys them.
SCHEDULED = "scheduled"
LISTENER = "listener"
PASSTHROUGH = "passthrough"

#: The CFG semantic kind a listener entry lowers to.
LISTENER_SEMANTIC_KIND = "listener"
#: #184. The CFG semantic kind a passthrough entry lowers to.
PASSTHROUGH_SEMANTIC_KIND = "passthrough"

_PHASE = "emission_planning"


@dataclass(frozen=True)
class EntryFormV1:
    """One admitted physical entry form.

    ``absorbed_semantic_kind`` names the CFG semantic kind the synthesized Start
    FUSES with, or ``None`` for a form that absorbs nothing (scheduled).
    """

    form: str
    start_emitter_kind: str
    execution_profile: str
    absorbed_semantic_kind: Optional[str] = None


#: The CLOSED set of admitted entry forms, keyed by form. A fourth form would
#: mean a fourth Start emitter and a fourth ``<process>`` attribute set, so it is
#: a deliberate extension here, never an inference elsewhere.
ENTRY_FORMS: Mapping[str, EntryFormV1] = MappingProxyType(
    {
        SCHEDULED: EntryFormV1(SCHEDULED, "start_noaction", SCHEDULED),
        LISTENER: EntryFormV1(
            LISTENER, "start_listen", LISTENER, LISTENER_SEMANTIC_KIND
        ),
        PASSTHROUGH: EntryFormV1(
            PASSTHROUGH, "start_passthrough", PASSTHROUGH, PASSTHROUGH_SEMANTIC_KIND
        ),
    }
)

#: The fused form each explicit-entry semantic kind selects. DERIVED from
#: :data:`ENTRY_FORMS`, never listed a second time.
_FORM_BY_ENTRY_SEMANTIC_KIND: Mapping[str, str] = MappingProxyType(
    {
        row.absorbed_semantic_kind: row.form
        for row in ENTRY_FORMS.values()
        if row.absorbed_semantic_kind is not None
    }
)

#: The CFG semantic kinds that are an EXPLICIT entry, fused with the Start. The
#: one set lowering and the CFG invariants consult (#184).
FUSED_ENTRY_SEMANTIC_KINDS: FrozenSet[str] = frozenset(_FORM_BY_ENTRY_SEMANTIC_KIND)


@dataclass(frozen=True)
class ProcessEntryDescriptorV1:
    """The derived entry of one compiled CFG.

    ``absorbed_node_id`` is the CFG node the Start stands in for (the listener or
    the passthrough), or ``None`` for a scheduled entry, whose Start absorbs
    nothing. ``start_target_node_id`` is the CFG node the Start's single wire
    reaches. ``operation_ref``/``label`` are the entry's authored values, carried
    so the Start's emitter input can be re-derived without re-reading the node; a
    passthrough carries no operation, so its ``operation_ref`` is always ``None``.
    """

    form: str
    start_emitter_kind: str
    execution_profile: str
    absorbed_node_id: Optional[str]
    start_target_node_id: str
    operation_ref: Optional[str] = None
    label: Optional[str] = None


def _semantic_kind(node: Any) -> Optional[str]:
    return getattr(getattr(node, "semantic", None), "semantic_kind", None)


def _entry_node(cfg: Any) -> Any:
    """The node the CFG names as its entry, or ``None``.

    Read from ``entry_node_id`` rather than assuming the first node: the
    compiler already decided which node is the entry, and a second opinion
    computed here could disagree with the artifact it describes.
    """
    entry_id = getattr(cfg, "entry_node_id", None)
    for node in getattr(cfg, "nodes", None) or ():
        if getattr(node, "node_id", None) == entry_id:
            return node
    return None


def classify_entry(cfg: Any) -> str:
    """The entry form the CFG's entry node selects: ``"listener"``,
    ``"passthrough"``, or ``"scheduled"``.

    Tolerant by design (see the module docstring): an absent or unresolvable
    entry classifies as scheduled, the profile every process had before an
    explicit entry existed, so a graph this cannot read produces exactly the
    bytes it produced before. Classification is by the entry node's own semantic
    kind — never by the family of some operation symbol — so an unrelated Web
    Services Server operation elsewhere in the table cannot make a scheduled or a
    passthrough process a listener.
    """
    kind = _semantic_kind(_entry_node(cfg))
    if not isinstance(kind, str):
        return SCHEDULED
    return _FORM_BY_ENTRY_SEMANTIC_KIND.get(kind, SCHEDULED)


def derive_process_entry(cfg: Any) -> ProcessEntryDescriptorV1:
    """The strict entry descriptor emission planning and the invariants share."""
    entries = [
        node for node in cfg.nodes if _semantic_kind(node) in FUSED_ENTRY_SEMANTIC_KINDS
    ]
    if not entries:
        scheduled = ENTRY_FORMS[SCHEDULED]
        return ProcessEntryDescriptorV1(
            form=scheduled.form,
            start_emitter_kind=scheduled.start_emitter_kind,
            execution_profile=scheduled.execution_profile,
            absorbed_node_id=None,
            start_target_node_id=cfg.entry_node_id,
        )
    if len(entries) != 1 or entries[0].node_id != cfg.entry_node_id:
        raise raise_compile_error(
            PROCESS_IR_COMPILE_INTERNAL,
            _PHASE,
            "",
            message="an explicit entry must be the single CFG entry node",
        )
    entry = entries[0]
    kind = _semantic_kind(entry)
    outgoing = [edge for edge in cfg.edges if edge.source_node_id == entry.node_id]
    if (
        len(outgoing) != 1
        or outgoing[0].kind not in ("ordering", "terminal")
        or outgoing[0].local_ordinal != 1
    ):
        raise raise_compile_error(
            PROCESS_IR_COMPILE_INTERNAL,
            _PHASE,
            entry.source_path,
            internal_node_id=entry.node_id,
            message="a {0} entry must have exactly one sequential successor".format(kind),
        )
    form = ENTRY_FORMS[_FORM_BY_ENTRY_SEMANTIC_KIND[kind]]
    return ProcessEntryDescriptorV1(
        form=form.form,
        start_emitter_kind=form.start_emitter_kind,
        execution_profile=form.execution_profile,
        absorbed_node_id=entry.node_id,
        start_target_node_id=outgoing[0].target_node_id,
        operation_ref=getattr(entry.semantic, "operation_ref", None),
        label=entry.semantic.label,
    )


def start_emitter_input(
    descriptor: ProcessEntryDescriptorV1, symbol_index: Mapping[str, Any]
):
    """The synthesized Start's emitter input, re-derived from the descriptor.

    The listener form carries the RESOLVED operation id, never the authored ref.
    Reference resolution refuses an unresolvable listener operation long before
    emission planning, so a missing symbol here is a compiler defect. The
    passthrough form (#184) carries only the entry's label and needs no symbol.
    """
    if descriptor.form == SCHEDULED:
        return StartNoActionInputV1()
    if descriptor.form == PASSTHROUGH:
        return StartPassthroughInputV1(userlabel=descriptor.label or "")
    symbol = symbol_index.get(descriptor.operation_ref or "")
    if symbol is None:
        raise raise_compile_error(
            PROCESS_IR_COMPILE_CONNECTOR_BINDING_INVALID,
            _PHASE,
            "",
            message="the listener operation has no resolved symbol",
        )
    return StartListenInputV1(
        operation_id=symbol.component_id, userlabel=descriptor.label or ""
    )


def entry_form_rows() -> Tuple[Tuple[str, str], ...]:
    """The admitted forms as sorted PUBLIC data: (form, execution profile).

    Deliberately omits the emitter kinds — those are compiler vocabulary the
    served contract may not carry.
    """
    return tuple(
        sorted((row.form, row.execution_profile) for row in ENTRY_FORMS.values())
    )


__all__ = [
    "ENTRY_FORMS",
    "FUSED_ENTRY_SEMANTIC_KINDS",
    "LISTENER",
    "LISTENER_SEMANTIC_KIND",
    "PASSTHROUGH",
    "PASSTHROUGH_SEMANTIC_KIND",
    "SCHEDULED",
    "EntryFormV1",
    "ProcessEntryDescriptorV1",
    "classify_entry",
    "derive_process_entry",
    "entry_form_rows",
    "start_emitter_input",
]
