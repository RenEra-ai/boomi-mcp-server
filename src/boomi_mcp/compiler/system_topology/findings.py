"""Static message/remediation tables and the topology finding builder (#144).

Named ``findings`` rather than ``diagnostics`` for the same reason
``semantic_validation.findings`` is: the sibling ``process_ir/diagnostics.py``
builds COMPILER diagnostics that are always fatal and always raised. These build
topology findings, which carry a severity and are accumulated.

Security: every message and remediation below is a STATIC string selected by
code. Nothing is interpolated — not an authored key, not a resolved id, not an
exception's text. These strings reach logs.
"""

from __future__ import annotations

from typing import Dict, Iterable, List, Tuple

from ...errors import (
    TOPOLOGY_APPLY_NOT_SUPPORTED,
    TOPOLOGY_CAPABILITY_GATED,
    TOPOLOGY_DEPENDENCY_CYCLE,
    TOPOLOGY_ENVIRONMENT_MISMATCH,
    TOPOLOGY_REFERENCE_NOT_FOUND,
    TOPOLOGY_REFERENCE_TYPE_MISMATCH,
    TOPOLOGY_RELATION_UNSUPPORTED,
    TOPOLOGY_SCHEMA_DUPLICATE_KEY,
    TOPOLOGY_SCHEMA_INVALID,
    TOPOLOGY_SCHEMA_INVALID_CARDINALITY,
    TOPOLOGY_SCHEMA_UNKNOWN_FIELD,
    TOPOLOGY_SCHEMA_UNKNOWN_OBJECT,
    TOPOLOGY_SCHEMA_UNKNOWN_RELATION,
    TOPOLOGY_SCHEMA_VERSION_UNSUPPORTED,
)
from .contracts import TopologyDiagnosticV1

_MESSAGES: Dict[str, str] = {
    TOPOLOGY_SCHEMA_UNKNOWN_OBJECT: "unknown or missing topology object kind",
    TOPOLOGY_SCHEMA_UNKNOWN_RELATION: "unknown or missing topology relation kind",
    TOPOLOGY_SCHEMA_UNKNOWN_FIELD: (
        "unknown or prohibited field on a strict topology model"
    ),
    TOPOLOGY_SCHEMA_INVALID_CARDINALITY: (
        "collection bound or binding cardinality violated"
    ),
    TOPOLOGY_SCHEMA_DUPLICATE_KEY: (
        "a key or semantic relation is declared more than once"
    ),
    TOPOLOGY_SCHEMA_VERSION_UNSUPPORTED: (
        "unsupported or missing topology document version"
    ),
    TOPOLOGY_SCHEMA_INVALID: (
        "value does not match the strict SystemTopologySpecV1 schema"
    ),
    TOPOLOGY_REFERENCE_NOT_FOUND: (
        "an authored reference does not resolve within this profile's context"
    ),
    TOPOLOGY_REFERENCE_TYPE_MISMATCH: (
        "a reference resolves to an object or component kind this role rejects"
    ),
    TOPOLOGY_RELATION_UNSUPPORTED: (
        "the authored relation shape is not a supported platform lifecycle"
    ),
    TOPOLOGY_CAPABILITY_GATED: (
        "the subject is representable as declared intent only — required "
        "evidence is missing"
    ),
    TOPOLOGY_ENVIRONMENT_MISMATCH: (
        "profile or environment evidence is inconsistent with the authored topology"
    ),
    TOPOLOGY_DEPENDENCY_CYCLE: (
        "the cross-process invocation graph contains a cycle"
    ),
    TOPOLOGY_APPLY_NOT_SUPPORTED: (
        "SystemTopologySpecV1 has no apply, deploy, schedule or execute path"
    ),
}

_REMEDIATION: Dict[str, str] = {
    TOPOLOGY_SCHEMA_UNKNOWN_OBJECT: (
        "Use one of the documented topology object kinds "
        "(see docs/architecture/SYSTEM_TOPOLOGY_V1.md)."
    ),
    TOPOLOGY_SCHEMA_UNKNOWN_RELATION: (
        "Use one of the documented topology relation kinds "
        "(see docs/architecture/SYSTEM_TOPOLOGY_V1.md)."
    ),
    TOPOLOGY_SCHEMA_UNKNOWN_FIELD: (
        "Remove the field — topology models carry opaque references only."
    ),
    TOPOLOGY_SCHEMA_INVALID_CARDINALITY: (
        "Satisfy the documented cardinality: declare at least one object, and "
        "bind every schedule and deployment unit exactly once."
    ),
    TOPOLOGY_SCHEMA_DUPLICATE_KEY: (
        "Give every object and relation a unique key, and declare each semantic "
        "relation once."
    ),
    TOPOLOGY_SCHEMA_VERSION_UNSUPPORTED: (
        "Set version to the supported SystemTopology version '1'."
    ),
    TOPOLOGY_SCHEMA_INVALID: (
        "Fix the value type/shape at the referenced path to match the "
        "SystemTopologySpecV1 schema."
    ),
    TOPOLOGY_REFERENCE_NOT_FOUND: (
        "Declare the referenced object in this document, or supply the "
        "component/platform fact in the same credential profile."
    ),
    TOPOLOGY_REFERENCE_TYPE_MISMATCH: (
        "Reference an object of the kind this relation role accepts "
        "(see the endpoint matrix in docs/architecture/SYSTEM_TOPOLOGY_V1.md)."
    ),
    TOPOLOGY_RELATION_UNSUPPORTED: (
        "Use the supported lifecycle shape for this relation, or remove it: a "
        "listener process is invoked by its API service rather than scheduled; "
        "a schedule binds one process to one RUNTIME and is bound once; a "
        "deployment unit targets exactly one process and one environment; and "
        "a process cannot call itself, so a self-recursive Process Call must "
        "be removed."
    ),
    TOPOLOGY_CAPABILITY_GATED: (
        "The subject stays blocked declared intent and will not enter an "
        "executable or planning bucket. Two cases. If the relation KIND is "
        "supported, supply the trusted witness it needs and the finding "
        "clears: for a Process Call or a shared cache/property use, a ProcessIR "
        "node when the subject is planned or the component's own XML when it "
        "exists; for an API service route, a typed builder projection when "
        "planned or the API Service Component's own XML when it exists. If the "
        "KIND itself is gated, no evidence you can supply will clear it and "
        "adding support requires a separate evidence-backed issue. The "
        "finding's own phase says which case this is: 'capability' for a gated "
        "kind, 'relation' for a supported kind missing its witness."
    ),
    TOPOLOGY_ENVIRONMENT_MISMATCH: (
        "Topology never crosses a credential profile, and an authored "
        "environment classification must match what discovery reports. Five "
        "causes, three remedies. Align the profile when: the context names a "
        "different profile than the topology does; or the snapshot envelope "
        "does. Update the authored classification when: discovery reports one "
        "classification for the environment and the topology declares the "
        "other (or omit the optional field to stop asserting it). Re-capture "
        "the snapshot from a single account when: an individual fact INSIDE "
        "the snapshot names another account even though the envelope agrees; "
        "or discovery reports more than one classification for one "
        "environment, in which case no authored value can satisfy it."
    ),
    TOPOLOGY_DEPENDENCY_CYCLE: (
        "Break the cross-process invocation cycle at the referenced relation."
    ),
    TOPOLOGY_APPLY_NOT_SUPPORTED: (
        "Use the planning contract, then the existing separately-authorized "
        "lifecycle tools to act on its output."
    ),
}

#: Every code this package can emit. Checked in both directions at import so a
#: code added to the taxonomy without text — or text left behind after a code is
#: dropped — fails loudly here instead of producing a KeyError at the one call
#: site that happens to use it.
TOPOLOGY_FINDING_CODES: Tuple[str, ...] = tuple(sorted(_MESSAGES))

if set(_MESSAGES) != set(_REMEDIATION):  # pragma: no cover — import-time guard
    raise ValueError(
        "topology findings: message and remediation tables disagree on "
        f"{sorted(set(_MESSAGES) ^ set(_REMEDIATION))}"
    )


def topology_finding(
    code: str,
    *,
    severity: str,
    phase: str,
    path: str,
    subject: str = "",
    provenance: Iterable[str] = (),
) -> TopologyDiagnosticV1:
    """Build one finding. Text is SELECTED by code and cannot be supplied.

    There is deliberately no ``message`` parameter. Letting a caller pass one is
    how an authored value ends up in a log line — and once the parameter exists,
    every future call site is a place it can happen.
    """
    return TopologyDiagnosticV1(
        code=code,
        severity=severity,  # type: ignore[arg-type]
        phase=phase,  # type: ignore[arg-type]
        path=path,
        subject=subject,
        message=_MESSAGES[code],
        remediation=_REMEDIATION[code],
        provenance=tuple(provenance),
    )


__all__: List[str] = ["TOPOLOGY_FINDING_CODES", "topology_finding"]
