"""Static message/remediation tables and the finding builder (#143).

Named ``findings`` rather than ``diagnostics`` on purpose: the sibling
``process_ir/diagnostics.py`` builds COMPILER diagnostics that are always fatal
and always raised. These build VALIDATION findings, which carry a severity and
are accumulated. Two modules named ``diagnostics`` one package apart would make
every import site ambiguous to a reader.

Security: every message and remediation below is a STATIC string selected by
code. Nothing is interpolated — not an authored value, not a resolved id, not an
exception's text. This is the same rule the compiler's diagnostics follow, and
for the same reason: these strings reach logs.
"""

from __future__ import annotations

from types import MappingProxyType
from typing import Dict, Iterable, List, Mapping, Optional, Tuple

from ....errors import (
    LEGACY_ADAPTER_EXEMPTION_DECISION_PROPERTY_READ,
    LEGACY_ADAPTER_EXEMPTION_OPAQUE_STATE_WRITER,
    LEGACY_ADAPTER_EXEMPTION_STANDALONE_CACHE_READ,
    LEGACY_ADAPTER_EXEMPTION_SUBPROCESS_SUMMARY,
    PROCESS_IR_CAPABILITY_CONNECTOR_ACTION_UNSUPPORTED,
    PROCESS_IR_CAPABILITY_DYNAMIC_PATH_UNSUPPORTED,
    PROCESS_IR_CAPABILITY_EFFECT_CONTRACT_INVALID,
    PROCESS_IR_CAPABILITY_ENTRY_CONTEXT_UNSUPPORTED,
    PROCESS_IR_CAPABILITY_PROCESS_CALL_PLACEMENT_UNSUPPORTED,
    PROCESS_IR_CAPABILITY_PROCESS_CALL_REPEATED_RUN_UNSTABLE,
    PROCESS_IR_REFERENCE_COMPONENT_NOT_FOUND,
    PROCESS_IR_REFERENCE_COMPONENT_TYPE_MISMATCH,
    PROCESS_IR_REFERENCE_CONNECTION_MISMATCH,
    PROCESS_IR_REFERENCE_CONNECTION_NOT_FOUND,
    PROCESS_IR_REFERENCE_LISTENER_OPERATION_INVALID,
    PROCESS_IR_REFERENCE_OPERATION_NOT_FOUND,
    PROCESS_IR_SEMANTIC_CARDINALITY_MISMATCH,
    PROCESS_IR_SEMANTIC_DYNAMIC_PATH_REQUIRED,
    PROCESS_IR_SEMANTIC_IDEMPOTENCY_EVIDENCE_MISSING,
    PROCESS_IR_SEMANTIC_LINEAGE_AMBIGUOUS_LAST_WRITE,
    PROCESS_IR_SEMANTIC_LINEAGE_BRANCH_ORDER_INVALID,
    PROCESS_IR_SEMANTIC_DYNAMIC_PATH_DDP_NOT_ESTABLISHED,
    PROCESS_IR_SEMANTIC_DYNAMIC_PATH_NO_DYNAMIC_SEGMENT,
    PROCESS_IR_SEMANTIC_DYNAMIC_PATH_PROFILE_BINDING_MISMATCH,
    PROCESS_IR_SEMANTIC_LINEAGE_CACHE_WRITER_MISSING,
    PROCESS_IR_SEMANTIC_LINEAGE_DDP_SCOPE_INVALID,
    PROCESS_IR_SEMANTIC_LINEAGE_EFFECT_UNKNOWN,
    PROCESS_IR_SEMANTIC_LINEAGE_EXTERNAL_WRITER_ASSUMED,
    PROCESS_IR_SEMANTIC_LINEAGE_PROPERTY_READ_BEFORE_WRITE,
    PROCESS_IR_SEMANTIC_LISTENER_INBOUND_CONTRACT_UNSATISFIED,
    PROCESS_IR_SEMANTIC_MISSING_TERMINAL,
    PROCESS_IR_SEMANTIC_PROFILE_MISMATCH,
    PROCESS_IR_SEMANTIC_RETRY_EFFECT_UNSAFE,
    PROCESS_IR_SEMANTIC_RETRY_NON_IDEMPOTENT_WRITE,
    PROCESS_IR_SEMANTIC_RETRY_SOURCE_POLICY_REQUIRES_RETRY,
    PROCESS_IR_SEMANTIC_RETRY_SOURCE_POLICY_SCOPE_INVALID,
    PROCESS_IR_SEMANTIC_RETRY_SOURCE_REEXECUTION,
    PROCESS_IR_SEMANTIC_SIDE_EFFECT_ORDERING_UNKNOWN,
    PROCESS_IR_SEMANTIC_SIDE_EFFECT_ORDERING_UNSAFE,
    PROCESS_IR_SEMANTIC_UNREACHABLE,
    PROCESS_IR_SEMANTIC_UNTERMINATED_PATH,
)
from ..diagnostics import (
    _MESSAGES as _COMPILER_MESSAGES,
    _REMEDIATION as _COMPILER_REMEDIATION,
    _UNREGISTERED_CODE_REMEDIATION,
    node_identity_for,
)
from .contracts import ValidationDiagnosticV1, ValidationEvidenceV1

_MESSAGES: Dict[str, str] = {
    PROCESS_IR_REFERENCE_COMPONENT_NOT_FOUND: (
        "an authored component reference does not resolve to a symbol"
    ),
    PROCESS_IR_REFERENCE_COMPONENT_TYPE_MISMATCH: (
        "a resolved symbol is the wrong component type for this role"
    ),
    PROCESS_IR_CAPABILITY_EFFECT_CONTRACT_INVALID: (
        "a typed effect contract is malformed or bound to the wrong component"
    ),
    # #184 amendment 3 §8: raised by lineage when a call discharges its child's contract.
    PROCESS_IR_CAPABILITY_ENTRY_CONTEXT_UNSUPPORTED: (
        "a process is invoked in an entry context ProcessIR v1 does not admit"
    ),
    # #184 amendment 1 rule 8 (QA-184-s1-r21-01): raised by lineage at a call whose No Data
    # child more than one document can reach. The evidence's `state_scope` names the scope
    # of the state involved and does not identify which of the two causes applies.
    PROCESS_IR_CAPABILITY_PROCESS_CALL_REPEATED_RUN_UNSTABLE: (
        "the called No Data process runs once for each document reaching this call, and "
        "each run may change state a later run requires: it requires something of a "
        "document cache it may also add to or empty — it reads that cache before writing "
        "it; or it consumes the profile or uses the properties of the documents it "
        "retrieves from it, including documents it stored there itself, unless every run "
        "ends with nothing it stored left in that cache — a whole-cache removal that runs "
        "whenever its leg does follows its last write to it, no call it makes after that "
        "removal may write that cache, no "
        "call it makes without waiting may write that cache wherever that call stands, its "
        "own or one inside a process it calls, and no outside writer is declared for that "
        "cache on a retrieve of it that names one, since such a writer may refill it between "
        "runs — and this call is authored wait=true and "
        "abort_on_error=true — so a later run may find what an earlier run added or "
        "emptied; or it reads state before writing it while its own state effects are "
        "unknown. A call may write that cache when the contract derived from its ProcessIR "
        "says it writes it, and a call this request cannot derive may write any cache the "
        "calling process can observe"
    ),
    PROCESS_IR_SEMANTIC_LINEAGE_PROPERTY_READ_BEFORE_WRITE: (
        "a property or cache key is read before any write establishes it"
    ),
    PROCESS_IR_SEMANTIC_LINEAGE_DDP_SCOPE_INVALID: (
        "a document property is read outside the document copy that wrote it"
    ),
    PROCESS_IR_SEMANTIC_LINEAGE_BRANCH_ORDER_INVALID: (
        "a Branch leg depends on state written by a later leg"
    ),
    PROCESS_IR_SEMANTIC_LINEAGE_CACHE_WRITER_MISSING: (
        "a document cache is read on a path with no preceding write"
    ),
    PROCESS_IR_SEMANTIC_DYNAMIC_PATH_DDP_NOT_ESTABLISHED: (
        "a bound request path is not provably composed on the documents this "
        "call sends — no writer reaches the call, a step between the writer and "
        "the call handed on documents that do not carry the property, or the "
        "writer composed the path from a value nothing established"
    ),
    PROCESS_IR_SEMANTIC_DYNAMIC_PATH_NO_DYNAMIC_SEGMENT: (
        "the writer composing a bound request path contributes only literal values"
    ),
    PROCESS_IR_SEMANTIC_DYNAMIC_PATH_PROFILE_BINDING_MISMATCH: (
        "a bound request path and its writer disagree about the request profile"
    ),
    PROCESS_IR_SEMANTIC_LINEAGE_AMBIGUOUS_LAST_WRITE: (
        "converging paths leave the last writer undetermined"
    ),
    PROCESS_IR_SEMANTIC_LINEAGE_EFFECT_UNKNOWN: (
        "a map or script has no typed effect contract, so its state effects are unknown"
    ),
    PROCESS_IR_SEMANTIC_LINEAGE_EXTERNAL_WRITER_ASSUMED: (
        "state is assumed to come from a declared external writer"
    ),
    PROCESS_IR_SEMANTIC_SIDE_EFFECT_ORDERING_UNSAFE: (
        "the flow cannot guarantee the ordering these side effects require"
    ),
    PROCESS_IR_SEMANTIC_SIDE_EFFECT_ORDERING_UNKNOWN: (
        "an asynchronous ordering can be proven neither safe nor unsafe"
    ),
    PROCESS_IR_SEMANTIC_RETRY_EFFECT_UNSAFE: (
        "a retried region replays a non-connector effect with no established "
        "replay safety"
    ),
    LEGACY_ADAPTER_EXEMPTION_OPAQUE_STATE_WRITER: (
        "a named legacy policy accepted an opaque state writer as proof"
    ),
    LEGACY_ADAPTER_EXEMPTION_DECISION_PROPERTY_READ: (
        "a named legacy policy accepted a Decision-arm read not established on "
        "every outcome"
    ),
    LEGACY_ADAPTER_EXEMPTION_STANDALONE_CACHE_READ: (
        "a named legacy policy accepted a cache read with no in-process writer"
    ),
    LEGACY_ADAPTER_EXEMPTION_SUBPROCESS_SUMMARY: (
        "a named legacy policy accepted a Process Call with no typed child summary"
    ),
}

_REMEDIATION: Dict[str, str] = {
    PROCESS_IR_REFERENCE_COMPONENT_NOT_FOUND: (
        "Provide a component symbol for this reference in the component plan."
    ),
    PROCESS_IR_REFERENCE_COMPONENT_TYPE_MISMATCH: (
        "Reference a component of the type this role requires."
    ),
    PROCESS_IR_CAPABILITY_EFFECT_CONTRACT_INVALID: (
        "Bind the effect contract to the component it describes; a script "
        "contract must carry the digest of the exact source it covers."
    ),
    PROCESS_IR_CAPABILITY_ENTRY_CONTEXT_UNSUPPORTED: (
        "Call a Data Passthrough child with wait=true: it receives the arriving "
        "documents as one group only through a waiting call. A passthrough process "
        "run directly (a test run or a schedule) starts as No Data, with one empty "
        "document and nothing a caller supplies, so run it through its caller or "
        "remove what it requires of one."
    ),
    PROCESS_IR_CAPABILITY_PROCESS_CALL_REPEATED_RUN_UNSTABLE: (
        "Make the called process run once for these documents. Either give it a Data "
        "Passthrough entry, with a passthrough step first and this call authored "
        "wait=true, so one call runs it once over the whole group; or call it from a "
        "process with no explicit entry, as the terminal of a Branch leg that has no steps "
        "of its own and no step in front of its Branch, which hands the call exactly one "
        "document. Or answer the cause this call was refused for. A child that reads the "
        "cache before writing it needs that cache established before its own write, and an "
        "earlier run may have emptied it: no removal exempts that one — write the cache "
        "before reading it, or give it a Data Passthrough entry and take what it needs from "
        "the documents its callers hand over, which a No Data child never receives. "
        "A "
        "child that never reads the cache before writing it may instead remove the whole "
        "cache after its last write to it, in a later leg of the same Branch that runs no "
        "connector call, cache retrieve or data process before the removal, with this call "
        "authored wait=true and abort_on_error=true, and with no call of its own able to "
        "write that cache after the removal or while the run goes on: move a call that "
        "stands after the removal to before it and wait for it, and wait for every call it "
        "makes, including a call made by a process it calls. Supplying a called process's "
        "ProcessIR helps only where the contract derived from it shows that process does "
        "not write that cache — for a call nothing waits for, that is the only thing that "
        "helps. A cache this request declares an outside writer for, where a retrieve of "
        "that cache authors external_writer, takes no such "
        "exemption at all, whatever the child's own calls do: the writer may refill it "
        "between runs, so either the child stops requiring that cache of its callers or it "
        "runs once for the whole group. A declaration no retrieve names establishes nothing "
        "and withholds nothing. A child that reads state before writing it while its own state effects are "
        "unknown is refused for that read whatever the caches do: make its effects "
        "derivable, by supplying the ProcessIR of what it calls or replacing the step "
        "nothing can inspect. "
        "The rule is published at "
        "get_schema_template(schema_name='process_ir_authoring', node_kind='process_call')."
    ),
    PROCESS_IR_SEMANTIC_LINEAGE_PROPERTY_READ_BEFORE_WRITE: (
        "Write the property or cache key on every path that reaches this read, "
        "or give it a default."
    ),
    PROCESS_IR_SEMANTIC_LINEAGE_DDP_SCOPE_INVALID: (
        "Use a process property for state that must cross document copies. A "
        "document property lives only on the document that wrote it."
    ),
    PROCESS_IR_SEMANTIC_LINEAGE_BRANCH_ORDER_INVALID: (
        "Move the write into an earlier leg. Branch legs run in order, so a "
        "later leg's write is not visible to an earlier one."
    ),
    PROCESS_IR_SEMANTIC_LINEAGE_CACHE_WRITER_MISSING: (
        "Add a cache write ahead of this read on the same path."
    ),
    PROCESS_IR_SEMANTIC_DYNAMIC_PATH_DDP_NOT_ESTABLISHED: (
        "Establish a validated writer on every possible document origin reaching "
        "this call. After cache retrieval, use proved cached writer provenance or a "
        "supported current-document overlay; otherwise write the property after the "
        "read. Defaults and bare established-at-entry declarations do not provide "
        "writer proof."
    ),
    PROCESS_IR_SEMANTIC_DYNAMIC_PATH_NO_DYNAMIC_SEGMENT: (
        "Give the writer at least one non-literal source — a profile element or "
        "another property — or drop the binding and configure a static path on "
        "the operation instead."
    ),
    PROCESS_IR_SEMANTIC_DYNAMIC_PATH_PROFILE_BINDING_MISMATCH: (
        "Name on the binding the same profile the writer reads its elements "
        "from, name none when the writer reads no profile element, and compose "
        "the path from elements of a single profile."
    ),
    PROCESS_IR_SEMANTIC_LINEAGE_AMBIGUOUS_LAST_WRITE: (
        "Make every converging path establish the same last writer, or move the "
        "read inside the path that writes it."
    ),
    PROCESS_IR_SEMANTIC_LINEAGE_EFFECT_UNKNOWN: (
        "Declare a typed effect contract for this map or script, or set the "
        "state explicitly with a property step. A declaration alone is not "
        "enough: its CONTENT must be backed by a server-side authority — "
        "inspection of the resolved map or child process, or a vetted script "
        "registry entry — and a declaration with no such backing is inert. A "
        "missing or inert declaration never establishes state."
    ),
    PROCESS_IR_SEMANTIC_LINEAGE_EXTERNAL_WRITER_ASSUMED: (
        "No change required. Confirm the external writer really does run before "
        "this process."
    ),
    PROCESS_IR_SEMANTIC_SIDE_EFFECT_ORDERING_UNSAFE: (
        "Order the effects so the dependency is established before it is read, "
        "or make the subprocess call wait for completion."
    ),
    PROCESS_IR_SEMANTIC_SIDE_EFFECT_ORDERING_UNKNOWN: (
        "Declare the child's effects with a typed summary so the ordering can be "
        "decided."
    ),
    PROCESS_IR_SEMANTIC_RETRY_EFFECT_UNSAFE: (
        "Set the retry count to zero, move the effect outside the protected "
        "scope, or declare it replay-safe with a typed contract whose content a "
        "server-side authority backs — a declaration the server cannot "
        "corroborate is inert and leaves this finding standing."
    ),
    LEGACY_ADAPTER_EXEMPTION_OPAQUE_STATE_WRITER: (
        "No change required on the legacy surface. Declaring typed effects "
        "removes the exemption."
    ),
    LEGACY_ADAPTER_EXEMPTION_DECISION_PROPERTY_READ: (
        "No change required on the legacy surface. Establishing the property on "
        "every outcome removes the exemption."
    ),
    LEGACY_ADAPTER_EXEMPTION_STANDALONE_CACHE_READ: (
        "No change required on the legacy surface. Adding an in-process writer "
        "removes the exemption."
    ),
    LEGACY_ADAPTER_EXEMPTION_SUBPROCESS_SUMMARY: (
        "No change required on the legacy surface. Declaring a typed child "
        "summary removes the exemption."
    ),
}

#: QA-184-s1-r17-02. Codes this validator raises whose RULE, and so whose words,
#: are the compiler's. `finding()` selects text from THIS module's tables only, so
#: without an entry here each of them reached the typed plan route with the generic
#: fallback, while the served contract published the compiler's words for the code
#: and the compile route (`pipeline._restore`) served them. The text is READ from
#: the compiler's tables when this module loads, never copied, so the two layers
#: cannot drift. A code whose semantic rule decides a different fact needs text of
#: its own in the tables above, not a row here.
#:
#: The set is checked from source in both directions by
#: `tests/test_process_ir_served_text_enforcement.py`: every code a `finding()` call
#: can carry has an entry (`test_every_raised_code_serves_its_own_table_text`), and
#: no entry exists for a code it cannot carry
#: (`test_the_served_code_set_is_exactly_what_the_authorities_account_for`).
_COMPILER_WORDED_CODES: Tuple[str, ...] = (
    # Translated verbatim from `connector_resolution.validate_connector_calls` by
    # `flow.collect_connector_flow_findings`, which re-raises the
    # `PROCESS_IR_COMPILE_*` family instead of translating it.
    PROCESS_IR_CAPABILITY_CONNECTOR_ACTION_UNSUPPORTED,
    PROCESS_IR_CAPABILITY_DYNAMIC_PATH_UNSUPPORTED,
    PROCESS_IR_REFERENCE_CONNECTION_MISMATCH,
    PROCESS_IR_REFERENCE_CONNECTION_NOT_FOUND,
    PROCESS_IR_REFERENCE_LISTENER_OPERATION_INVALID,
    PROCESS_IR_REFERENCE_OPERATION_NOT_FOUND,
    PROCESS_IR_SEMANTIC_CARDINALITY_MISMATCH,
    PROCESS_IR_SEMANTIC_DYNAMIC_PATH_REQUIRED,
    PROCESS_IR_SEMANTIC_IDEMPOTENCY_EVIDENCE_MISSING,
    PROCESS_IR_SEMANTIC_LISTENER_INBOUND_CONTRACT_UNSATISFIED,
    PROCESS_IR_SEMANTIC_RETRY_NON_IDEMPOTENT_WRITE,
    PROCESS_IR_SEMANTIC_RETRY_SOURCE_POLICY_REQUIRES_RETRY,
    PROCESS_IR_SEMANTIC_RETRY_SOURCE_POLICY_SCOPE_INVALID,
    PROCESS_IR_SEMANTIC_RETRY_SOURCE_REEXECUTION,
    # Translated as above AND raised by lineage's own profile checks at maps, cache
    # writes, profile sources, declared-input connector calls fed by a cache read, and
    # process calls, every one of which the compiler's text names.
    PROCESS_IR_SEMANTIC_PROFILE_MISMATCH,
    # Raised by this validator's ports of the compiler's own rules: the flow walks
    # (`flow.collect_reachability_findings` and `collect_terminal_findings`, the walks
    # `invariants` runs on the same graph) and lineage's No Data placement check at a
    # call, which the compiler's text covers where the refusal names the call itself.
    PROCESS_IR_CAPABILITY_PROCESS_CALL_PLACEMENT_UNSUPPORTED,
    PROCESS_IR_SEMANTIC_MISSING_TERMINAL,
    PROCESS_IR_SEMANTIC_UNREACHABLE,
    PROCESS_IR_SEMANTIC_UNTERMINATED_PATH,
)
_MESSAGES.update({code: _COMPILER_MESSAGES[code] for code in _COMPILER_WORDED_CODES})
_REMEDIATION.update(
    {code: _COMPILER_REMEDIATION[code] for code in _COMPILER_WORDED_CODES}
)


def finding(
    code: str,
    severity: str,
    phase: str,
    path: str,
    *,
    evidence: Iterable[Tuple[str, object]] = (),
    internal_node_id: Optional[str] = None,
) -> ValidationDiagnosticV1:
    """Build one finding with the static text registered for ``code``.

    ``evidence`` is given as ``(key, value)`` pairs and validated by
    ``ValidationEvidenceV1`` — an unallowlisted key or an unsafe value shape
    raises here, at construction, rather than leaking into a report.
    """
    return ValidationDiagnosticV1(
        code=code,
        severity=severity,
        phase=phase,
        path=path,
        node_identity=node_identity_for(path),
        message=_MESSAGES.get(code, "semantic validation rejected the payload"),
        # Shared with the compiler's factory rather than copied: see
        # `diagnostics._UNREGISTERED_CODE_REMEDIATION`.
        remediation=_REMEDIATION.get(code, _UNREGISTERED_CODE_REMEDIATION),
        evidence=tuple(
            ValidationEvidenceV1(key=key, value=value) for key, value in evidence
        ),
        internal_node_id=internal_node_id,
    )


def registered_codes() -> Tuple[str, ...]:
    """Codes with static text, so a test can prove none is missing one."""
    return tuple(sorted(_MESSAGES))




#: A shared shape for the #146 authoring projection: (code, message,
#: remediation), sorted by code. Every string is STATIC and selected by code —
#: nothing is interpolated from an authored payload — which is what makes the
#: table safe to serve. Since #177 a code carrying one of the two texts but not
#: the other cannot be served at all: ``finding_specs()`` below refuses an
#: asymmetric or blank registry rather than emitting an empty string. The old
#: rationale — emit the blank "so a caller comparing the served set against the
#: codes they actually receive has to be able to see the gap" — was measured and
#: found false in practice: seven such rows were served for four slices and no
#: caller ever looked, which is why the gap is now a hard failure here instead of
#: a burden on the reader.


def finding_specs() -> Tuple[Mapping[str, str], ...]:
    """Static (code, message, remediation) for every semantic-validation code.

    Fails closed since #177; see ``_complete_spec_rows`` in
    ``boomi_mcp.models.process_ir``.
    """
    from ....models.process_ir import _complete_spec_rows

    return _complete_spec_rows(_MESSAGES, _REMEDIATION, "semantic-validation")


def non_emittable_registered_codes() -> Tuple[str, ...]:
    """Codes registered here that NOTHING can currently raise.

    #177 invariant 1 compares the served code set against the set the emitting
    modules actually raise. A registered-but-unreachable code would look like a
    guard failure, so the one deliberate case is declared here rather than
    allowlisted inside the test — the declaration is production metadata, and the
    guard PROVES it by asserting the source scan finds zero emissions of each
    member (a declaration nobody checks is how a served fact goes stale).

    ``PROCESS_IR_SEMANTIC_LINEAGE_AMBIGUOUS_LAST_WRITE`` is registered against
    the convergence analysis documented at
    ``docs/architecture/PROCESS_IR_SEMANTIC_VALIDATION_V1.md``; ProcessIR v1
    emits no join, so no node has two writers to disambiguate and the rule has no
    reachable case yet.
    """
    return (PROCESS_IR_SEMANTIC_LINEAGE_AMBIGUOUS_LAST_WRITE,)


__all__: List[str] = [
    "finding",
    "finding_specs",
    "non_emittable_registered_codes",
    "registered_codes",
]
