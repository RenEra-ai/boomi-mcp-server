"""Compiler-derived process execution profile (issue #153 / M12.15).

A Boomi ``<process>`` element carries a different attribute set depending on how
the process is STARTED: a scheduled process and a listener process differ in
``allowSimultaneous``, ``updateRunDates``, and whether
``stopProcessingIfZeroDocuments`` is present at all.

Before #153 that choice was made inside the legacy builder by sniffing the
authored source config (``process_flow_builder`` around the
``source_is_listener`` branch). #153 moves the decision to the COMPILER and
records the result on the materialization plan, for one reason: the profile is a
property of the process GRAPH, and a caller who could author it independently
could author one that contradicts their own entry node — a scheduled flag on a
listener graph, or the reverse. The materializer then consumes only the recorded
profile and never re-inspects the IR, so there is exactly one authority.

**The listener value is deliberately reachable-but-unreached today.** Lowering
still refuses a listener entry outright (``lowering`` raises
``PROCESS_IR_CAPABILITY_UNSUPPORTED`` for a source whose connector family is in
:data:`LISTENER_CONNECTOR_TYPES`, because the legacy path fuses the start and
connector shapes into one ``start_listen`` and this compiler always emits the
``start_noaction`` + ``connectoraction`` pair). So every root that COMPILES today
necessarily derives ``"scheduled"``.

That makes it tempting to write ``return "scheduled"`` and move on. This module
deliberately does not, and the distinction is the point: the classification is
computed from the same runtime authority lowering uses, so

* it cannot drift from the rule that actually rejects listeners, and
* #158 can lift the lowering refusal without touching the plan, the materializer
  interface, or this function.

A constant would satisfy every test that can be written against today's
compilable inputs, which is precisely why it would be the wrong thing to ship.
The accompanying test constructs a CFG whose entry node IS a listener family and
asserts this returns ``"listener"`` — a case the constant version could not pass.
"""

from __future__ import annotations

from typing import Optional

from .contracts import LISTENER_CONNECTOR_TYPES, SemanticCfgV1, SymbolTableV1

#: The two execution profiles a process can have. CLOSED: a third value would
#: have to mean a third ``<process>`` attribute set, and the materializer maps
#: this value straight onto exact option bytes.
ProcessExecutionProfile = str  # Literal["scheduled", "listener"] at the call sites

SCHEDULED = "scheduled"
LISTENER = "listener"


def _entry_node(cfg: SemanticCfgV1):
    """The CFG's own entry node, or ``None`` if the graph names none.

    Read from ``entry_node_id`` rather than assuming ``nodes[0]``: the compiler
    already decided which node is the entry, and a second opinion computed here
    could disagree with the artifact it claims to describe — the same rule the
    plan's terminal-kind summary follows.
    """
    for node in cfg.nodes or ():
        if node.node_id == cfg.entry_node_id:
            return node
    return None


def _operation_connector_family(
    symbols: SymbolTableV1, operation_ref: str
) -> Optional[str]:
    """The connector family recorded on an operation symbol, case-folded.

    The family lives on the OPERATION symbol, which is where lowering reads it
    too. No connector-action component declares its own connection or family in
    the IR — it is a fact of the component plan the compiler receives — so this
    must come from the symbol table and never from the authored node.
    """
    for symbol in symbols.symbols or ():
        if symbol.ref == operation_ref:
            family = symbol.connector_type
            return family.strip().lower() if isinstance(family, str) else None
    return None


def derive_process_execution_profile(
    cfg: SemanticCfgV1, symbols: SymbolTableV1
) -> str:
    """``"scheduled"`` or ``"listener"``, derived from the CFG entry node.

    The rule, stated once: a process is a LISTENER when its entry node is a
    connector acting as the ``source`` whose resolved operation symbol names a
    connector family in :data:`LISTENER_CONNECTOR_TYPES`. Everything else is
    scheduled — including a graph whose entry is a message, a branch, or a
    connector acting as a target.

    :data:`LISTENER_CONNECTOR_TYPES` is imported from the compiler contracts
    rather than re-listed here. That set is the runtime authority lowering
    consults when it refuses a listener entry, and a second hand-written copy of
    it is exactly the unpinned hand-model this repository's structural-fix rule
    forbids: the two would be free to disagree, and the disagreement would be a
    process emitted with the wrong ``<process>`` attributes.

    Unknown or unresolvable entry shapes fall through to ``"scheduled"``. That is
    deliberate and safe rather than a silent guess: ``"scheduled"`` is the
    pre-existing default the legacy assembler has always emitted, so an entry
    this function cannot classify produces exactly the bytes it produced before
    #153 — and a genuinely unresolvable reference is already a compile error
    raised by lowering long before any profile matters.
    """
    entry = _entry_node(cfg)
    if entry is None:
        return SCHEDULED

    semantic = entry.semantic
    if getattr(semantic, "semantic_kind", None) != "connector":
        return SCHEDULED
    if getattr(semantic, "role", None) != "source":
        return SCHEDULED

    family = _operation_connector_family(symbols, getattr(semantic, "operation_ref", ""))
    if family and family in LISTENER_CONNECTOR_TYPES:
        return LISTENER
    return SCHEDULED


__all__ = [
    "LISTENER",
    "SCHEDULED",
    "derive_process_execution_profile",
]
