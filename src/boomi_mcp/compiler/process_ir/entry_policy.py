"""Compiler entry policy (issue #158 / M12.20).

How a compiled process ENTERS is one decision with three consequences: which
physical Start form ``shape1`` carries, which execution profile the process is
materialized with, and — for a listener — which CFG node that Start absorbs and
which node its single wire reaches. Before #158 the first was hard-coded
(``start_noaction`` at every synthesis and invariant site) and the second was
re-derived separately from the entry's connector family. This module is the one
authority for all three, so they cannot disagree.

There are exactly two admitted forms, and the set is CLOSED:

* **scheduled** — a compiler-synthesized ``start_noaction`` wired to the CFG
  entry node. Every root that is not a listener.
* **listener** — the CFG entry IS the authored ``listener`` node, and the
  compiler FUSES it with the Start: one synthesized ``start_listen`` carrying the
  Listen action for the resolved operation, wired to the listener's sole
  successor. The listener node has no shape of its own.

In both forms the Start remains compiler-owned: ``origin="synthetic"``, fixed
geometry, one synthetic wire. A caller can author neither form's geometry nor
its wiring; the listener form only changes WHICH input the synthesized Start
carries, and that input is re-derived from the CFG and the symbol table
wherever it is checked.

Two entry points, deliberately different in strictness:

* :func:`classify_entry` is TOLERANT and reads only the node the CFG names as
  its entry. It answers "which execution profile" for callers that may hold a
  partial graph — the served revision oracle builds stand-in graphs with no
  edges at all — and it must never fail on a shape it cannot classify.
* :func:`derive_process_entry` is STRICT. Emission planning and the plan
  invariants depend on its answer for geometry and wiring, so a listener that is
  not the entry, a second listener, or a listener without exactly one successor
  is a compiler defect and raises.
"""

from __future__ import annotations

from dataclasses import dataclass
from types import MappingProxyType
from typing import Any, Mapping, Optional, Tuple

from ...errors import (
    PROCESS_IR_COMPILE_CONNECTOR_BINDING_INVALID,
    PROCESS_IR_COMPILE_INTERNAL,
)
from .contracts import StartListenInputV1, StartNoActionInputV1
from .diagnostics import raise_compile_error

#: The two execution profiles, as the materializer keys them.
SCHEDULED = "scheduled"
LISTENER = "listener"

#: The CFG semantic kind a listener entry lowers to.
LISTENER_SEMANTIC_KIND = "listener"

_PHASE = "emission_planning"


@dataclass(frozen=True)
class EntryFormV1:
    """One admitted physical entry form."""

    form: str
    start_emitter_kind: str
    execution_profile: str


#: The CLOSED set of admitted entry forms, keyed by form. A third form would
#: mean a third Start emitter and a third ``<process>`` attribute set, so it is
#: a deliberate extension here, never an inference elsewhere.
ENTRY_FORMS: Mapping[str, EntryFormV1] = MappingProxyType(
    {
        SCHEDULED: EntryFormV1(SCHEDULED, "start_noaction", SCHEDULED),
        LISTENER: EntryFormV1(LISTENER, "start_listen", LISTENER),
    }
)


@dataclass(frozen=True)
class ProcessEntryDescriptorV1:
    """The derived entry of one compiled CFG.

    ``absorbed_node_id`` is the CFG node the Start stands in for (the listener),
    or ``None`` for a scheduled entry, whose Start absorbs nothing.
    ``start_target_node_id`` is the CFG node the Start's single wire reaches.
    ``operation_ref``/``label`` are the listener's authored values, carried so the
    Start's emitter input can be re-derived without re-reading the node.
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
    """``"listener"`` when the CFG's entry node is a listener, else ``"scheduled"``.

    Tolerant by design (see the module docstring): an absent or unresolvable
    entry classifies as scheduled, the profile every process had before a
    listener entry existed, so a graph this cannot read produces exactly the
    bytes it produced before. Classification is by the entry node's own semantic
    kind — never by the family of some operation symbol — so an unrelated Web
    Services Server operation elsewhere in the table cannot make a scheduled
    process a listener.
    """
    return LISTENER if _semantic_kind(_entry_node(cfg)) == LISTENER_SEMANTIC_KIND else SCHEDULED


def derive_process_entry(cfg: Any) -> ProcessEntryDescriptorV1:
    """The strict entry descriptor emission planning and the invariants share."""
    listeners = [
        node for node in cfg.nodes if _semantic_kind(node) == LISTENER_SEMANTIC_KIND
    ]
    if not listeners:
        scheduled = ENTRY_FORMS[SCHEDULED]
        return ProcessEntryDescriptorV1(
            form=scheduled.form,
            start_emitter_kind=scheduled.start_emitter_kind,
            execution_profile=scheduled.execution_profile,
            absorbed_node_id=None,
            start_target_node_id=cfg.entry_node_id,
        )
    if len(listeners) != 1 or listeners[0].node_id != cfg.entry_node_id:
        raise raise_compile_error(
            PROCESS_IR_COMPILE_INTERNAL,
            _PHASE,
            "",
            message="a listener must be the single CFG entry node",
        )
    listener = listeners[0]
    outgoing = [edge for edge in cfg.edges if edge.source_node_id == listener.node_id]
    if (
        len(outgoing) != 1
        or outgoing[0].kind not in ("ordering", "terminal")
        or outgoing[0].local_ordinal != 1
    ):
        raise raise_compile_error(
            PROCESS_IR_COMPILE_INTERNAL,
            _PHASE,
            listener.source_path,
            internal_node_id=listener.node_id,
            message="a listener entry must have exactly one sequential successor",
        )
    form = ENTRY_FORMS[LISTENER]
    return ProcessEntryDescriptorV1(
        form=form.form,
        start_emitter_kind=form.start_emitter_kind,
        execution_profile=form.execution_profile,
        absorbed_node_id=listener.node_id,
        start_target_node_id=outgoing[0].target_node_id,
        operation_ref=listener.semantic.operation_ref,
        label=listener.semantic.label,
    )


def start_emitter_input(
    descriptor: ProcessEntryDescriptorV1, symbol_index: Mapping[str, Any]
):
    """The synthesized Start's emitter input, re-derived from the descriptor.

    The listener form carries the RESOLVED operation id, never the authored ref.
    Reference resolution refuses an unresolvable listener operation long before
    emission planning, so a missing symbol here is a compiler defect.
    """
    if descriptor.form == SCHEDULED:
        return StartNoActionInputV1()
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
    "LISTENER",
    "LISTENER_SEMANTIC_KIND",
    "SCHEDULED",
    "EntryFormV1",
    "ProcessEntryDescriptorV1",
    "classify_entry",
    "derive_process_entry",
    "entry_form_rows",
    "start_emitter_input",
]
