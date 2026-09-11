"""Compiler-derived process execution profile (issue #153 / M12.15, activated by #158).

A Boomi ``<process>`` element carries a different attribute set depending on how
the process is STARTED: a scheduled process and a listener process differ in
``allowSimultaneous``, ``updateRunDates``, and whether
``stopProcessingIfZeroDocuments`` is present at all.

Before #153 that choice was made inside the legacy builder by sniffing the
authored source config (``process_flow_builder`` around the
``source_is_listener`` branch). #153 moved the decision to the COMPILER and
records the result on the materialization plan, for one reason: the profile is a
property of the process GRAPH, and a caller who could author it independently
could author one that contradicts their own entry node — a scheduled flag on a
listener graph, or the reverse. The materializer then consumes only the recorded
profile and never re-inspects the IR, so there is exactly one authority.

#153 shipped this seam with its listener value reachable-but-unreached, because
lowering refused every listener entry. #158 adds the ``listener`` entry node and
makes the compiler's entry policy (:mod:`.entry_policy`) the one authority on how
a process enters. This function now DELEGATES to that policy rather than keeping
its own rule: the profile, the Start form at ``shape1`` and the node the Start
absorbs are one decision, and a second derivation here could disagree with the
emission plan it is recorded beside. The plan, the materializer interface and
apply's re-derivation are unchanged — only the authority moved.
"""

from __future__ import annotations

from .contracts import SemanticCfgV1, SymbolTableV1
from .entry_policy import LISTENER, SCHEDULED, classify_entry

#: The two execution profiles a process can have. CLOSED: a third value would
#: have to mean a third ``<process>`` attribute set, and the materializer maps
#: this value straight onto exact option bytes.
ProcessExecutionProfile = str  # Literal["scheduled", "listener"] at the call sites


def derive_process_execution_profile(
    cfg: SemanticCfgV1, symbols: SymbolTableV1
) -> str:
    """``"scheduled"`` or ``"listener"``, derived from the CFG entry node.

    The rule, stated once, in the entry policy: a process is a LISTENER when its
    entry node is the authored listener entry. Everything else is scheduled —
    including a graph whose entry is a message, a branch, or a connector call, and
    including one whose symbol table happens to contain a Web Services Server
    operation that the entry does not listen on. Classification never consults an
    operation's connector family, so no unrelated symbol can flip it.

    ``symbols`` stays in the signature: the materialization plan and apply both
    call this with the table they compiled against, and the interface #153
    shipped is not this slice's to change.

    Unknown or unresolvable entry shapes fall through to ``"scheduled"``, the
    pre-existing default the legacy assembler has always emitted, so an entry
    this cannot classify produces exactly the bytes it produced before #153.
    """
    del symbols  # the entry node alone decides; see the docstring
    return classify_entry(cfg)


__all__ = [
    "LISTENER",
    "SCHEDULED",
    "derive_process_execution_profile",
]
