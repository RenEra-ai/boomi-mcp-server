"""Compiler diagnostics: phases, stable codes, and the compile error (issue #137).

Every diagnostic answers four questions the issue's acceptance criteria demand:
*which* stable code, *where* in the AUTHORED payload (an RFC 6901 pointer, never
an internal id alone), *which authored node* it belongs to, and *what to do
about it*.

Phase is load-bearing, not decoration: it separates a caller's mistake
(``schema``, ``reference_resolution``, ``semantic_lowering``) from a compiler
defect (``emission_planning`` invariants), which is exactly the
``PROCESS_IR_SEMANTIC_*`` vs ``PROCESS_IR_COMPILE_*`` family split in ADR-001 §7.

Security: messages and remediations are STATIC strings chosen by code. No
authored value, resolved id, or exception text is ever interpolated — including
in ``__str__`` of the raised error, which is what ends up in a log.
"""

from __future__ import annotations

from typing import Iterable, List, Literal, Optional, Sequence, Tuple

from pydantic import BaseModel, ConfigDict, Field

from ...errors import (
    PROCESS_IR_CAPABILITY_UNSUPPORTED,
    PROCESS_IR_COMPILE_EMISSION_PLAN_INVALID,
    PROCESS_IR_COMPILE_INTERNAL,
    PROCESS_IR_COMPILE_NONDETERMINISTIC,
    PROCESS_IR_SEMANTIC_AMBIGUOUS_FLOW,
    PROCESS_IR_SEMANTIC_MISSING_TERMINAL,
    PROCESS_IR_SEMANTIC_UNREACHABLE,
)

CompilerPhase = Literal[
    "schema",
    "reference_resolution",
    "semantic_lowering",
    "emission_planning",
]

# Diagnostics sort by pipeline order first, so the earliest failure reads first.
_PHASE_RANK = {
    "schema": 0,
    "reference_resolution": 1,
    "semantic_lowering": 2,
    "emission_planning": 3,
}

ROOT_NODE_IDENTITY = "<root>"

# Segment names whose integer child indexes an authored node list.
_NODE_LIST_SEGMENTS = frozenset({"steps", "legs"})

# Segment names that are themselves an authored node position.
_NODE_LEAF_SEGMENTS = frozenset({"terminal"})

_REMEDIATION = {
    PROCESS_IR_SEMANTIC_UNREACHABLE: (
        "Remove the unreachable node, or connect it to the flow: every node must be "
        "reachable from the single entry."
    ),
    PROCESS_IR_SEMANTIC_MISSING_TERMINAL: (
        "End the path in a stop, return_documents, exception, or routed target "
        "terminal."
    ),
    PROCESS_IR_SEMANTIC_AMBIGUOUS_FLOW: (
        "Give the flow exactly one entry and one successor per step; joins, cycles, "
        "and flow continuing past a terminal are not representable in v1."
    ),
    PROCESS_IR_COMPILE_INTERNAL: (
        "This is a compiler defect, not a problem with the authored payload — please "
        "report it with the authored path."
    ),
    PROCESS_IR_COMPILE_NONDETERMINISTIC: (
        "This is a compiler defect: compiler output was not in canonical order. "
        "Please report it with the authored path."
    ),
    PROCESS_IR_COMPILE_EMISSION_PLAN_INVALID: (
        "Provide a symbol for every authored reference; if every reference resolves, "
        "this is a compiler defect — please report it."
    ),
    PROCESS_IR_CAPABILITY_UNSUPPORTED: (
        "This construct is capability-gated in ProcessIR v1; see the capability "
        "manifest for the owning issue."
    ),
}

_MESSAGES = {
    PROCESS_IR_SEMANTIC_UNREACHABLE: "node is not reachable from the control-flow entry",
    PROCESS_IR_SEMANTIC_MISSING_TERMINAL: "control-flow path does not reach a valid terminal",
    PROCESS_IR_SEMANTIC_AMBIGUOUS_FLOW: "control flow is ambiguous",
    PROCESS_IR_COMPILE_INTERNAL: "compiler invariant violated",
    PROCESS_IR_COMPILE_NONDETERMINISTIC: "compiler output is not in canonical order",
    PROCESS_IR_COMPILE_EMISSION_PLAN_INVALID: "emission plan is invalid",
    PROCESS_IR_CAPABILITY_UNSUPPORTED: (
        "the payload requests a gated/unsupported ProcessIR capability"
    ),
}


def node_identity_for(path: str) -> str:
    """Nearest AUTHORED node path for an RFC 6901 pointer.

    ``/body/steps/2/legs/0/steps/1/text`` -> ``/body/steps/2/legs/0/steps/1``.
    A pointer that names no node at all (``""``, ``/version``) -> ``<root>``.

    The identity is what a human matches against their payload, so it must be a
    node boundary, never a leaf field.
    """
    if not path or not path.startswith("/"):
        return ROOT_NODE_IDENTITY
    segments = path.split("/")[1:]
    for index in range(len(segments) - 1, -1, -1):
        segment = segments[index]
        if segment in _NODE_LEAF_SEGMENTS:
            return "/" + "/".join(segments[: index + 1])
        if (
            segment.isdigit()
            and index > 0
            and segments[index - 1] in _NODE_LIST_SEGMENTS
        ):
            return "/" + "/".join(segments[: index + 1])
    return ROOT_NODE_IDENTITY


class CompilerDiagnostic(BaseModel):
    """One compiler diagnostic. Frozen, strict, and free of authored values."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    code: str
    phase: CompilerPhase
    path: str
    node_identity: str
    message: str
    remediation: str
    internal_node_id: Optional[str] = Field(
        default=None,
        description="Compiler-internal node id, when the defect is on a lowered node.",
    )


def diagnostic(
    code: str,
    phase: CompilerPhase,
    path: str,
    *,
    internal_node_id: Optional[str] = None,
    message: Optional[str] = None,
) -> CompilerDiagnostic:
    """Build a diagnostic with the static message/remediation for ``code``."""
    return CompilerDiagnostic(
        code=code,
        phase=phase,
        path=path,
        node_identity=node_identity_for(path),
        message=message or _MESSAGES.get(code, "compiler rejected the payload"),
        remediation=_REMEDIATION.get(
            code, "See the ProcessIR compiler documentation for this code."
        ),
        internal_node_id=internal_node_id,
    )


class ProcessIRCompileError(Exception):
    """Raised when compilation fails. Carries sorted, value-free diagnostics."""

    def __init__(self, diagnostics: Iterable[CompilerDiagnostic]) -> None:
        self.diagnostics: Tuple[CompilerDiagnostic, ...] = tuple(
            sorted(
                diagnostics,
                key=lambda item: (
                    _PHASE_RANK.get(item.phase, len(_PHASE_RANK)),
                    item.path,
                    item.code,
                ),
            )
        )
        summary = "; ".join(
            "{0} at {1}".format(item.code, item.path or ROOT_NODE_IDENTITY)
            for item in self.diagnostics
        )
        super().__init__("ProcessIRV1 compilation failed: {0}".format(summary))

    def __repr__(self) -> str:  # pragma: no cover - trivial
        return "ProcessIRCompileError(codes={0!r})".format(
            [item.code for item in self.diagnostics]
        )


def raise_compile_error(
    code: str,
    phase: CompilerPhase,
    path: str,
    *,
    internal_node_id: Optional[str] = None,
    message: Optional[str] = None,
) -> "ProcessIRCompileError":
    """Construct (never raise) a single-diagnostic error, for ``raise`` at the call site."""
    return ProcessIRCompileError(
        [
            diagnostic(
                code,
                phase,
                path,
                internal_node_id=internal_node_id,
                message=message,
            )
        ]
    )


def internal_defect(path: str, *, internal_node_id: Optional[str] = None) -> ProcessIRCompileError:
    return raise_compile_error(
        PROCESS_IR_COMPILE_INTERNAL,
        "emission_planning",
        path,
        internal_node_id=internal_node_id,
    )


def sorted_diagnostics(
    diagnostics: Sequence[CompilerDiagnostic],
) -> Tuple[CompilerDiagnostic, ...]:
    """Canonical diagnostic order, matching ``ProcessIRCompileError``'s own sort."""
    return ProcessIRCompileError(diagnostics).diagnostics


__all__: List[str] = [
    "ROOT_NODE_IDENTITY",
    "CompilerDiagnostic",
    "CompilerPhase",
    "ProcessIRCompileError",
    "diagnostic",
    "internal_defect",
    "node_identity_for",
    "raise_compile_error",
    "sorted_diagnostics",
]
