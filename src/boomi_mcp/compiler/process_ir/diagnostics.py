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

from types import MappingProxyType
from typing import Iterable, List, Literal, Mapping, Optional, Sequence, Tuple

from pydantic import BaseModel, ConfigDict, Field

from ...errors import (
    PROCESS_IR_CAPABILITY_ERROR_SCOPE_UNSUPPORTED,
    PROCESS_IR_CAPABILITY_NODE_NOT_ALLOWED_IN_BODY,
    PROCESS_IR_CAPABILITY_PROCESS_CALL_RETURN_PATH_BINDING_UNSUPPORTED,
    PROCESS_IR_COMPILE_ERROR_REGION_INVALID,
    PROCESS_IR_SEMANTIC_CATCH_UNTERMINATED,
    PROCESS_IR_SEMANTIC_IDEMPOTENCY_EVIDENCE_MISSING,
    PROCESS_IR_SEMANTIC_RETRY_NON_IDEMPOTENT_WRITE,
    PROCESS_IR_SEMANTIC_RETRY_SOURCE_REEXECUTION,
    PROCESS_IR_COMPILE_CONTROL_WIRING_INVALID,
    PROCESS_IR_SCHEMA_BRANCH_CARDINALITY,
    PROCESS_IR_SEMANTIC_CONTROL_CONTINUATION_UNSUPPORTED,
    PROCESS_IR_SEMANTIC_JOIN_UNSUPPORTED,
    PROCESS_IR_SEMANTIC_NESTING_LIMIT,
    PROCESS_IR_SEMANTIC_UNTERMINATED_PATH,
    PROCESS_IR_CAPABILITY_CONNECTOR_ACTION_UNSUPPORTED,
    PROCESS_IR_CAPABILITY_UNSUPPORTED,
    PROCESS_IR_COMPILE_CONNECTOR_BINDING_INVALID,
    PROCESS_IR_COMPILE_EMISSION_PLAN_INVALID,
    PROCESS_IR_COMPILE_EMITTER_INPUT_INVALID,
    PROCESS_IR_COMPILE_EMITTER_MISSING,
    PROCESS_IR_COMPILE_INTERNAL,
    PROCESS_IR_COMPILE_NONDETERMINISTIC,
    PROCESS_IR_COMPILE_SYMBOL_UNRESOLVED,
    PROCESS_IR_COMPILE_VERIFIER_FAILED,
    PROCESS_IR_COMPILE_XML_INVALID,
    PROCESS_IR_REFERENCE_CONNECTION_MISMATCH,
    PROCESS_IR_REFERENCE_CONNECTION_NOT_FOUND,
    PROCESS_IR_REFERENCE_OPERATION_NOT_FOUND,
    PROCESS_IR_SEMANTIC_AMBIGUOUS_FLOW,
    PROCESS_IR_SEMANTIC_CARDINALITY_MISMATCH,
    PROCESS_IR_SEMANTIC_MISSING_TERMINAL,
    PROCESS_IR_SEMANTIC_PROFILE_MISMATCH,
    PROCESS_IR_SEMANTIC_UNREACHABLE,
)

CompilerPhase = Literal[
    "schema",
    "reference_resolution",
    "semantic_lowering",
    "emission_planning",
    # Issue #138: the typed emitter registry turns an emission plan into XML
    # (xml_emission), then re-checks it with the graph verifier
    # (post_emission_verification). Ranked after emission_planning.
    "xml_emission",
    "post_emission_verification",
]

# Diagnostics sort by pipeline order first, so the earliest failure reads first.
_PHASE_RANK = {
    "schema": 0,
    "reference_resolution": 1,
    "semantic_lowering": 2,
    "emission_planning": 3,
    "xml_emission": 4,
    "post_emission_verification": 5,
}

ROOT_NODE_IDENTITY = "<root>"

# Segment names whose integer child indexes an authored node list.
_NODE_LIST_SEGMENTS = frozenset({"steps", "legs"})

# Segment names that are themselves an authored node position.
_NODE_LEAF_SEGMENTS = frozenset({"terminal"})

_REMEDIATION = {
    # --- #141 M12.6 -------------------------------------------------------
    PROCESS_IR_SCHEMA_BRANCH_CARDINALITY: (
        "A Branch must declare between 2 and 25 legs — the platform's own "
        "documented bound on Branch paths."
    ),
    PROCESS_IR_SEMANTIC_CONTROL_CONTINUATION_UNSUPPORTED: (
        "Move the steps that followed the branch/decision into every leg or arm. "
        "Control nodes are terminal fan-out in ProcessIR v1; nothing may follow one."
    ),
    PROCESS_IR_SEMANTIC_JOIN_UNSUPPORTED: (
        "Give each divergent path its own terminal. ProcessIR v1 emits no join or "
        "merge, so a node may have at most one predecessor."
    ),
    PROCESS_IR_SEMANTIC_NESTING_LIMIT: (
        "Reduce Branch/Decision nesting to at most the documented control depth, or "
        "move the deeper routing into a subprocess. This is a ProcessIR v1 compiler "
        "bound, not a Boomi platform limit."
    ),
    PROCESS_IR_SEMANTIC_UNTERMINATED_PATH: (
        "End every Branch leg and Decision outcome in its own terminal; each "
        "divergent path must terminate independently."
    ),
    PROCESS_IR_CAPABILITY_NODE_NOT_ALLOWED_IN_BODY: (
        "Use a node kind this body slot admits. The admitted set for each slot is "
        "published at get_schema_template(schema_name='process_ir_authoring', "
        "category='placement'); a kind absent from a slot is rejected outright."
    ),
    # --- #175 -------------------------------------------------------------
    PROCESS_IR_CAPABILITY_PROCESS_CALL_RETURN_PATH_BINDING_UNSUPPORTED: (
        "Author the process call as the TERMINAL of its path and remove whatever "
        "followed it — a call whose child returns no documents ends the path, so a "
        "trailing stop is not needed and cannot be emitted. A call that must hand "
        "control onward needs its child's return-document shapes bound to it; that "
        "capability is published as process_call_return_path_binding at "
        "get_schema_template(schema_name='process_ir_authoring', category='capability')."
    ),
    PROCESS_IR_COMPILE_CONTROL_WIRING_INVALID: (
        "This is a compiler defect: derived control-node wiring (count, order, "
        "labels, or target) is wrong. Please report it with the authored path."
    ),
    PROCESS_IR_SEMANTIC_UNREACHABLE: (
        "Remove the unreachable node, or connect it to the flow: every node must be "
        "reachable from the single entry."
    ),
    PROCESS_IR_SEMANTIC_MISSING_TERMINAL: (
        "End the path in a stop, return_documents, exception, routed target, or "
        "process call terminal — a process call ends the path it is on."
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
        "This construct is capability-gated in ProcessIR v1. Fetch its published "
        "state at get_schema_template(schema_name='process_ir_authoring', "
        "category='capability') — 'gated' means not yet, 'unsupported' means never."
    ),
    PROCESS_IR_COMPILE_EMITTER_MISSING: (
        "This node kind has no registered emitter at the current capability level; "
        "this is a compiler defect or a capability gap — please report it."
    ),
    PROCESS_IR_COMPILE_EMITTER_INPUT_INVALID: (
        "The emitter received a typed input that does not match the node kind, or a "
        "shape's outgoing wiring is invalid; this is a compiler defect — please report it."
    ),
    PROCESS_IR_COMPILE_SYMBOL_UNRESOLVED: (
        "Provide a component symbol of the required type for this node's references; "
        "if every reference resolves, this is a compiler defect — please report it."
    ),
    PROCESS_IR_COMPILE_XML_INVALID: (
        "The emitted process XML is malformed or disagrees with the emission plan; "
        "this is a compiler defect — please report it."
    ),
    PROCESS_IR_COMPILE_VERIFIER_FAILED: (
        "The process graph verifier rejected the emitted XML; this is a compiler "
        "defect — please report it with the authored path."
    ),
    # --- issue #140, first-class ConnectorCall --------------------------------
    PROCESS_IR_REFERENCE_OPERATION_NOT_FOUND: (
        "Provide a connector-action component symbol for this call's operation_ref."
    ),
    PROCESS_IR_REFERENCE_CONNECTION_NOT_FOUND: (
        "Name the connection on the OPERATION COMPONENT in the component plan: "
        "set its `config.connection_ref_key` to the key of the connector-settings "
        "component it binds to, and include that component in the plan. The IR "
        "does not author this edge, so it cannot be inferred from the node."
    ),
    PROCESS_IR_REFERENCE_CONNECTION_MISMATCH: (
        "Bind the operation to a connector-settings component of the SAME connector "
        "family as the operation."
    ),
    PROCESS_IR_CAPABILITY_CONNECTOR_ACTION_UNSUPPORTED: (
        "Use a connector family/action pair from the verified connector-call "
        "capability matrix, and either omit the authored action or set it to the "
        "operation's own action. The callable pairs are published at "
        "get_schema_template(schema_name='process_ir_authoring', "
        "category='connector_action')."
    ),
    PROCESS_IR_SEMANTIC_PROFILE_MISMATCH: (
        "Make the map's source profile the preceding call's output profile and its "
        "target profile the following call's input profile."
    ),
    PROCESS_IR_SEMANTIC_CARDINALITY_MISMATCH: (
        "Order the calls so every document consumer follows a call that produces "
        "documents, and place a call that produces none last, before the terminal."
    ),
    PROCESS_IR_COMPILE_CONNECTOR_BINDING_INVALID: (
        "This is a compiler defect: a connector-call binding was missing or "
        "inconsistent at emission time — please report it with the authored path."
    ),
    # --- #142 M12.7 -------------------------------------------------------
    PROCESS_IR_CAPABILITY_ERROR_SCOPE_UNSUPPORTED: (
        "Use a supported error scope in its verified placement: a process scope as "
        "the sole root step, or a connector scope as the last step of a "
        "connector-call sequence."
    ),
    PROCESS_IR_SEMANTIC_RETRY_SOURCE_REEXECUTION: (
        "Set the retry count to zero, or protect a call that runs downstream of the "
        "one producing the documents. Retrying the producing call re-runs it, "
        "duplicating everything it already emitted."
    ),
    PROCESS_IR_SEMANTIC_RETRY_NON_IDEMPOTENT_WRITE: (
        "Set the retry count to zero, or move the call outside the protected scope. "
        "This action has no established retry safety, so replaying it could "
        "duplicate an external effect."
    ),
    PROCESS_IR_SEMANTIC_IDEMPOTENCY_EVIDENCE_MISSING: (
        "Supply the typed idempotency evidence this action requires, and make sure a "
        "referenced contract resolves and names this same operation."
    ),
    PROCESS_IR_SEMANTIC_CATCH_UNTERMINATED: (
        "End the catch body with a stop, an exception, or a staging cache write. "
        "Every caught document must reach a terminal."
    ),
    PROCESS_IR_COMPILE_ERROR_REGION_INVALID: (
        "This is a compiler defect: a derived Try/Catch error region was "
        "structurally invalid — please report it with the authored path."
    ),
}

_MESSAGES = {
    # #177 invariant 1. These seven codes carried a REMEDIATION but no message, so
    # `compiler_diagnostic_specs()` served them with an empty `message` — a served
    # row advertising a diagnostic whose "what is wrong" half does not exist. Three
    # of them reached the authoring projection's summary slot carrying their
    # remediation instead (the other four are also parser codes, and the merge
    # filled them from the parser's table), so the gap was invisible from the
    # projection alone.
    #
    # The text is the COMPILER LAYER'S OWN, not a copy of the parser's: this table
    # is the authority for compiler-served text, and the repo deliberately keeps
    # per-layer remediations distinct and attributes each to its producer. Where
    # the fact is genuinely identical at both layers the wording is identical too;
    # where the compiler's scope is broader (placement AND path composition, below)
    # the wording says so rather than under-describing what the compiler rejects.
    PROCESS_IR_CAPABILITY_NODE_NOT_ALLOWED_IN_BODY: (
        "node placement or path composition is not admitted in this control body"
    ),
    PROCESS_IR_COMPILE_CONTROL_WIRING_INVALID: (
        "compiler-derived control-node wiring is invalid"
    ),
    PROCESS_IR_SCHEMA_BRANCH_CARDINALITY: "branch leg count is outside the 2-25 bound",
    PROCESS_IR_SEMANTIC_CONTROL_CONTINUATION_UNSUPPORTED: (
        "continuation after a branch or decision is not supported"
    ),
    PROCESS_IR_SEMANTIC_JOIN_UNSUPPORTED: (
        "ProcessIR v1 emits no join: a node has more than one predecessor"
    ),
    PROCESS_IR_SEMANTIC_NESTING_LIMIT: (
        "control nesting exceeds the ProcessIR v1 depth bound"
    ),
    PROCESS_IR_SEMANTIC_UNTERMINATED_PATH: (
        "a divergent control path reaches no terminal"
    ),
    PROCESS_IR_SEMANTIC_UNREACHABLE: "node is not reachable from the control-flow entry",
    PROCESS_IR_SEMANTIC_MISSING_TERMINAL: "control-flow path does not reach a valid terminal",
    PROCESS_IR_SEMANTIC_AMBIGUOUS_FLOW: "control flow is ambiguous",
    PROCESS_IR_COMPILE_INTERNAL: "compiler invariant violated",
    PROCESS_IR_COMPILE_NONDETERMINISTIC: "compiler output is not in canonical order",
    PROCESS_IR_COMPILE_EMISSION_PLAN_INVALID: "emission plan is invalid",
    PROCESS_IR_CAPABILITY_UNSUPPORTED: (
        "the payload requests a gated/unsupported ProcessIR capability"
    ),
    PROCESS_IR_COMPILE_EMITTER_MISSING: "no registered emitter for the node kind",
    PROCESS_IR_CAPABILITY_ERROR_SCOPE_UNSUPPORTED: (
        "unsupported error scope or error-scope placement"
    ),
    PROCESS_IR_CAPABILITY_PROCESS_CALL_RETURN_PATH_BINDING_UNSUPPORTED: (
        "a process call may not be followed by another node in ProcessIR v1"
    ),
    PROCESS_IR_SEMANTIC_RETRY_SOURCE_REEXECUTION: (
        "a positive retry count would re-run the flow's document source"
    ),
    PROCESS_IR_SEMANTIC_RETRY_NON_IDEMPOTENT_WRITE: (
        "a retried connector call has no established retry safety"
    ),
    PROCESS_IR_SEMANTIC_IDEMPOTENCY_EVIDENCE_MISSING: (
        "required typed idempotency evidence is absent, of the wrong kind, or unresolved"
    ),
    PROCESS_IR_SEMANTIC_CATCH_UNTERMINATED: "the catch body does not reach a terminal",
    PROCESS_IR_COMPILE_ERROR_REGION_INVALID: (
        "a derived Try/Catch error region is structurally invalid"
    ),
    PROCESS_IR_COMPILE_EMITTER_INPUT_INVALID: "emitter input is invalid for the node kind",
    PROCESS_IR_COMPILE_SYMBOL_UNRESOLVED: "a required component symbol is unresolved",
    PROCESS_IR_COMPILE_XML_INVALID: "emitted process XML is invalid",
    PROCESS_IR_COMPILE_VERIFIER_FAILED: "the process graph verifier reported errors",
    PROCESS_IR_REFERENCE_OPERATION_NOT_FOUND: (
        "the connector call's operation reference does not resolve to a "
        "connector-action symbol"
    ),
    PROCESS_IR_REFERENCE_CONNECTION_NOT_FOUND: (
        "the operation's connection is unknown or does not resolve to a symbol"
    ),
    PROCESS_IR_REFERENCE_CONNECTION_MISMATCH: (
        "the resolved connection is not a connector-settings symbol of the "
        "operation's connector family"
    ),
    PROCESS_IR_CAPABILITY_CONNECTOR_ACTION_UNSUPPORTED: (
        "the connector family/action pair is not a verified connector-call capability"
    ),
    PROCESS_IR_SEMANTIC_PROFILE_MISMATCH: (
        "a map's profile does not match the connector call adjacent to it"
    ),
    PROCESS_IR_SEMANTIC_CARDINALITY_MISMATCH: (
        "a connector call's document cardinality is impossible at its position"
    ),
    PROCESS_IR_COMPILE_CONNECTOR_BINDING_INVALID: (
        "a connector-call binding is missing or inconsistent"
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
            code,
            "Fetch this code's authoring rule with "
            "get_schema_template(schema_name='process_ir_authoring', "
            "category='diagnostic').",
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




#: A shared shape for the #146 authoring projection: (code, message,
#: remediation), sorted by code. Every string is STATIC and selected by code —
#: nothing is interpolated from an authored payload — which is what makes the
#: table safe to serve. Since #177 a code carrying one of the two texts but not
#: the other cannot exist: `compiler_diagnostic_specs()` below refuses to serve an
#: asymmetric or blank registry, and `tests/test_process_ir_served_text_enforcement.py`
#: proves from the emitting modules that every code this compiler can raise is
#: registered here. The previous behaviour — emitting an empty string so a caller
#: "could see the gap" — served seven such rows for four slices and nothing ever
#: looked.


def compiler_diagnostic_specs() -> Tuple[Mapping[str, str], ...]:
    """Static (code, message, remediation) for every compiler diagnostic code.

    Fails closed since #177: an asymmetric or blank registry raises rather than
    serving an empty field. See ``_complete_spec_rows`` in
    ``boomi_mcp.models.process_ir`` for why that is safe on this path and why
    ``diagnostic()`` deliberately does NOT do the same.
    """
    from ...models.process_ir import _complete_spec_rows

    return _complete_spec_rows(_MESSAGES, _REMEDIATION, "compiler")


__all__: List[str] = [
    "ROOT_NODE_IDENTITY",
    "compiler_diagnostic_specs",
    "CompilerDiagnostic",
    "CompilerPhase",
    "ProcessIRCompileError",
    "diagnostic",
    "internal_defect",
    "node_identity_for",
    "raise_compile_error",
    "sorted_diagnostics",
]
