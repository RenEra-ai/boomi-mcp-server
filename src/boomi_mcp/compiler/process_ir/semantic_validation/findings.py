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
    PROCESS_IR_CAPABILITY_EFFECT_CONTRACT_INVALID,
    PROCESS_IR_REFERENCE_COMPONENT_NOT_FOUND,
    PROCESS_IR_REFERENCE_COMPONENT_TYPE_MISMATCH,
    PROCESS_IR_SEMANTIC_LINEAGE_AMBIGUOUS_LAST_WRITE,
    PROCESS_IR_SEMANTIC_LINEAGE_BRANCH_ORDER_INVALID,
    PROCESS_IR_SEMANTIC_LINEAGE_CACHE_WRITER_MISSING,
    PROCESS_IR_SEMANTIC_LINEAGE_DDP_SCOPE_INVALID,
    PROCESS_IR_SEMANTIC_LINEAGE_EFFECT_UNKNOWN,
    PROCESS_IR_SEMANTIC_LINEAGE_EXTERNAL_WRITER_ASSUMED,
    PROCESS_IR_SEMANTIC_LINEAGE_PROPERTY_READ_BEFORE_WRITE,
    PROCESS_IR_SEMANTIC_RETRY_EFFECT_UNSAFE,
    PROCESS_IR_SEMANTIC_SIDE_EFFECT_ORDERING_UNKNOWN,
    PROCESS_IR_SEMANTIC_SIDE_EFFECT_ORDERING_UNSAFE,
)
from ..diagnostics import node_identity_for
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
    PROCESS_IR_SEMANTIC_LINEAGE_AMBIGUOUS_LAST_WRITE: (
        "Make every converging path establish the same last writer, or move the "
        "read inside the path that writes it."
    ),
    PROCESS_IR_SEMANTIC_LINEAGE_EFFECT_UNKNOWN: (
        "Declare a typed effect contract for this map or script, or set the "
        "state explicitly with a property step. An undeclared effect never "
        "establishes state."
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
        "scope, or declare it replay-safe with a typed contract."
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
        remediation=_REMEDIATION.get(
            code,
            "Fetch this code's authoring rule with "
            "get_schema_template(schema_name='process_ir_authoring', "
            "category='diagnostic').",
        ),
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
#: table safe to serve. A code carrying one of the two texts but not the other
#: is emitted with an empty string rather than skipped: a caller comparing the
#: served set against the codes they actually receive has to be able to see the
#: gap.


def finding_specs() -> Tuple[Mapping[str, str], ...]:
    """Static (code, message, remediation) for every semantic-validation code."""
    return tuple(
        MappingProxyType(
            {
                "code": code,
                "message": _MESSAGES.get(code, ""),
                "remediation": _REMEDIATION.get(code, ""),
            }
        )
        for code in sorted(set(_MESSAGES) | set(_REMEDIATION))
    )


__all__: List[str] = ["finding", "finding_specs", "registered_codes"]
