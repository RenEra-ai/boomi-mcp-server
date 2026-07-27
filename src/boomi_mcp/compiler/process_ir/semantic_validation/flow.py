"""Flow-phase collectors: reachability, terminals, nesting, profile/cardinality.

DARK in slice 3 — nothing calls this yet.

Two different strategies, chosen per family on purpose
-----------------------------------------------------
**Structural families are re-derived and ACCUMULATED here.** Reachability,
terminal coverage, per-control-path termination and nesting depth are cheap
graph facts. ``invariants.check_cfg_invariants`` already computes them, but it
raises on the FIRST violation — which is right for a compiler assertion and
wrong for a report whose whole purpose is to show a caller everything at once.
Re-deriving them lets one payload surface five unreachable nodes instead of one.

**Profile and cardinality are DELEGATED, not re-derived.** ``#140``'s
``validate_connector_calls`` encodes map-bracketing, the non-producing-connector
rule, and the whole retry/idempotency gate from ``#142``. Reimplementing that
here to make it accumulate would be trading a correctness risk for a
presentation nicety: two copies of a subtle rule drift, and the migration matrix
classifies that resolver `port-unchanged` precisely so it does not get forked.
So it is invoked as-is and its raised diagnostic is translated into findings.
The cost is honest and bounded — those families stop at their first violation,
exactly as they do today.

The compile-family guard
------------------------
``validate_connector_calls`` can raise ``PROCESS_IR_COMPILE_*`` diagnostics
(a binding the caller could not have authored). Those are compiler defects, not
authored ones, and ADR-001 §7 does not give this issue that family. They are
therefore RE-RAISED rather than translated — a ``ValidationReportV1`` must never
be able to carry one.
"""

from __future__ import annotations

from typing import Dict, List, Optional, Set, Tuple

from ....errors import (
    PROCESS_IR_COMPILE_INTERNAL,
    PROCESS_IR_SEMANTIC_MISSING_TERMINAL,
    PROCESS_IR_SEMANTIC_UNREACHABLE,
    PROCESS_IR_SEMANTIC_UNTERMINATED_PATH,
)
from ..connector_resolution import validate_connector_calls
from ..diagnostics import CompilerDiagnostic, ProcessIRCompileError
from .contracts import ValidationDiagnosticV1
from .context import PreparedProcessValidationV1
from .findings import finding

#: Which validation phase each translated compiler code reports under. A
#: delegated diagnostic keeps its CODE verbatim (codes are the stable contract)
#: but is filed under this package's phase vocabulary so report ordering stays
#: coherent with the natively-collected findings around it.
_PHASE_BY_CODE_SUFFIX = (
    ("PROFILE", "profile"),
    ("CARDINALITY", "cardinality"),
    ("RETRY", "retry"),
    ("IDEMPOTENCY", "retry"),
    ("CATCH", "terminal"),
    ("REFERENCE", "reference"),
    ("CAPABILITY", "capability"),
)

_CONTROL_KINDS = frozenset({"branch", "decision", "try_catch"})


def _phase_for(code: str) -> str:
    for token, phase in _PHASE_BY_CODE_SUFFIX:
        if token in code:
            return phase
    return "capability"


def collect_reachability_findings(
    prepared: PreparedProcessValidationV1,
) -> Tuple[ValidationDiagnosticV1, ...]:
    """Every node not reachable from the entry, not just the first one.

    A cycle would make the walk non-terminating, so ``seen`` guards it. Cycles
    themselves are reported by the ambiguous-flow rule the compiler already
    owns; this collector's job is coverage, and it must not hang on input the
    compiler has not yet rejected.
    """
    reached: Set[str] = set()
    stack: List[str] = [prepared.cfg.entry_node_id]
    while stack:
        current = stack.pop()
        if current in reached:
            continue
        reached.add(current)
        for edge in prepared.successors(current):
            stack.append(edge.target_node_id)

    return tuple(
        finding(
            PROCESS_IR_SEMANTIC_UNREACHABLE,
            "error",
            "reachability",
            node.source_path,
            internal_node_id=node.node_id,
        )
        for node in prepared.cfg.nodes
        if node.node_id not in reached
    )


def collect_terminal_findings(
    prepared: PreparedProcessValidationV1,
) -> Tuple[ValidationDiagnosticV1, ...]:
    """Leaves without an exit role, plus control paths that reach no terminal.

    The two rules are genuinely different and both are needed. The leaf rule
    catches a path that simply stops; the per-control-path rule catches a Branch
    leg that routes into a subtree whose leaves are all valid exits while THAT
    leg reaches none — invisible to a leaf-only scan.
    """
    findings: List[ValidationDiagnosticV1] = []

    for node in prepared.cfg.nodes:
        if prepared.successors(node.node_id):
            continue
        if node.exit_role:
            continue
        findings.append(
            finding(
                PROCESS_IR_SEMANTIC_MISSING_TERMINAL,
                "error",
                "terminal",
                node.source_path,
                internal_node_id=node.node_id,
            )
        )

    for node in prepared.cfg.nodes:
        if node.semantic.semantic_kind not in _CONTROL_KINDS:
            continue
        # A try_catch fans out over ``ordering`` + ``catch``, not branch_leg /
        # decision_outcome — selecting by edge kind would skip it entirely and
        # let an unterminated protected path through.
        if node.semantic.semantic_kind == "try_catch":
            control_edges = prepared.successors(node.node_id)
        else:
            control_edges = tuple(
                edge
                for edge in prepared.successors(node.node_id)
                if edge.kind in ("branch_leg", "decision_outcome")
            )
        for edge in control_edges:
            if not _reaches_exit(prepared, edge.target_node_id):
                findings.append(
                    finding(
                        PROCESS_IR_SEMANTIC_UNTERMINATED_PATH,
                        "error",
                        "terminal",
                        edge.provenance_path,
                        evidence=(
                            (("leg_ordinal", edge.leg_ordinal),)
                            if edge.leg_ordinal is not None
                            else ()
                        ),
                        internal_node_id=node.node_id,
                    )
                )

    return tuple(findings)


def _reaches_exit(prepared: PreparedProcessValidationV1, start: str) -> bool:
    seen: Set[str] = set()
    stack = [start]
    while stack:
        current = stack.pop()
        if current in seen:
            continue
        seen.add(current)
        node = prepared.node(current)
        if node is None:
            continue
        if node.exit_role:
            return True
        for edge in prepared.successors(current):
            stack.append(edge.target_node_id)
    return False


def collect_connector_flow_findings(
    prepared: PreparedProcessValidationV1,
) -> Tuple[ValidationDiagnosticV1, ...]:
    """Delegate to #140/#142 and translate what it raises.

    Codes are preserved VERBATIM — they are the stable contract and every
    existing caller keys on them. Only the presentation changes: a raised
    compiler diagnostic becomes an accumulated finding.

    A ``PROCESS_IR_COMPILE_*`` diagnostic is re-raised untouched. It blames the
    compiler, this issue does not own that family, and a report that could carry
    one would invite a caller to "fix" a compiler bug in their payload.
    """
    try:
        validate_connector_calls(prepared.cfg, prepared.symbols)
    except ProcessIRCompileError as exc:
        if any(d.code.startswith("PROCESS_IR_COMPILE_") for d in exc.diagnostics):
            raise
        return tuple(
            finding(
                item.code,
                "error",
                _phase_for(item.code),
                item.path,
                internal_node_id=item.internal_node_id,
            )
            for item in exc.diagnostics
        )
    except Exception:  # noqa: BLE001 - an UNEXPECTED resolver defect
        # `phase` is load-bearing, and this stage has always reported its own
        # defects as `reference_resolution`. The compiler used to run connector
        # resolution under its own guard, which supplied that phase; now that it
        # runs here, the phase has to be attached here or the outer guard would
        # relabel a resolver crash as `semantic_lowering`.
        #
        # Raised as ProcessIRCompileError so the outer `_guarded` passes it
        # through untouched rather than re-wrapping it.
        raise ProcessIRCompileError(
            [
                CompilerDiagnostic(
                    code=PROCESS_IR_COMPILE_INTERNAL,
                    phase="reference_resolution",
                    path="",
                    node_identity="",
                    message="connector resolution failed unexpectedly",
                    remediation=(
                        "This is a compiler defect, not a payload error. "
                        "Please report it with the process configuration."
                    ),
                )
            ]
        ) from None
    return ()


def collect_flow_findings(
    prepared: PreparedProcessValidationV1,
) -> Tuple[ValidationDiagnosticV1, ...]:
    """Every flow-phase finding, in collector order (the report re-sorts)."""
    return (
        collect_reachability_findings(prepared)
        + collect_terminal_findings(prepared)
        + collect_connector_flow_findings(prepared)
    )


__all__ = [
    "collect_connector_flow_findings",
    "collect_flow_findings",
    "collect_reachability_findings",
    "collect_terminal_findings",
]
