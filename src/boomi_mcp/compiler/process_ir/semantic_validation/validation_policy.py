"""Named, registry-owned legacy exemptions (#143). DARK in slice 7.

Why exemptions exist at all
---------------------------
Slice 4 inverted the legacy default: an undeclared map or script now contributes
uncertainty rather than proof. That is the correct rule for canonical ProcessIR,
and it is strictly stricter than the legacy walker — so some payloads that build
today would start failing if the new rule were applied to the legacy surface
unchanged. The issue's answer is not to weaken the rule but to name the gap:

    "Legacy exemptions live in named adapters, never in global validators."

Each exemption downgrades ONE specific error code to an ADVISORY, and records
which exemption did it. The finding does not disappear — it is reclassified, so
the migration ledger can point at the exact rows that still need work and the
removal gate is visible rather than folklore.

Why a caller cannot select one
------------------------------
A policy is looked up by ADAPTER IDENTITY, from an immutable module-level
registry. There is no policy field on ``ProcessIRV1``, no policy argument on
``validate_process_ir``, and no name a payload could supply that would reach
``lookup_policy``. That is the whole point of the issue's "no free-form
'trust me' flags" rule: an exemption a caller can request is not an exemption,
it is a bypass.

``validate_process_ir`` therefore always runs STRICT. Only the internal legacy
adapter path applies a policy, and only the one registered for itself.
"""

from __future__ import annotations

from typing import Dict, FrozenSet, List, Mapping, Optional, Tuple

from ....errors import (
    LEGACY_ADAPTER_EXEMPTION_DECISION_PROPERTY_READ,
    LEGACY_ADAPTER_EXEMPTION_OPAQUE_STATE_WRITER,
    LEGACY_ADAPTER_EXEMPTION_STANDALONE_CACHE_READ,
    LEGACY_ADAPTER_EXEMPTION_SUBPROCESS_SUMMARY,
    PROCESS_IR_SEMANTIC_LINEAGE_CACHE_WRITER_MISSING,
    PROCESS_IR_SEMANTIC_LINEAGE_EFFECT_UNKNOWN,
    PROCESS_IR_SEMANTIC_LINEAGE_PROPERTY_READ_BEFORE_WRITE,
    PROCESS_IR_SEMANTIC_SIDE_EFFECT_ORDERING_UNKNOWN,
)
from .contracts import (
    ValidationDiagnosticV1,
    ValidationReportV1,
    build_validation_report,
)
from .findings import finding

#: Which canonical code each named exemption covers. One exemption -> one code:
#: an exemption that covered "several related things" would be impossible to
#: retire, because nobody could say what retiring it would re-break.
_EXEMPT_CODE: Mapping[str, str] = {
    LEGACY_ADAPTER_EXEMPTION_OPAQUE_STATE_WRITER: (
        PROCESS_IR_SEMANTIC_LINEAGE_PROPERTY_READ_BEFORE_WRITE
    ),
    LEGACY_ADAPTER_EXEMPTION_DECISION_PROPERTY_READ: (
        PROCESS_IR_SEMANTIC_LINEAGE_EFFECT_UNKNOWN
    ),
    LEGACY_ADAPTER_EXEMPTION_STANDALONE_CACHE_READ: (
        PROCESS_IR_SEMANTIC_LINEAGE_CACHE_WRITER_MISSING
    ),
    LEGACY_ADAPTER_EXEMPTION_SUBPROCESS_SUMMARY: (
        PROCESS_IR_SEMANTIC_SIDE_EFFECT_ORDERING_UNKNOWN
    ),
}


class LegacyValidationPolicyV1:
    """One adapter's exemption set. Immutable by construction."""

    __slots__ = ("adapter", "_exemptions")

    def __init__(self, adapter: str, exemptions: Tuple[str, ...]) -> None:
        for code in exemptions:
            if code not in _EXEMPT_CODE:
                raise ValueError("unknown exemption code")
        self.adapter = adapter
        self._exemptions: FrozenSet[str] = frozenset(exemptions)

    @property
    def exemptions(self) -> FrozenSet[str]:
        return self._exemptions

    def exemption_for(self, code: str) -> Optional[str]:
        """The exemption covering ``code`` under this policy, if any."""
        for exemption in sorted(self._exemptions):
            if _EXEMPT_CODE[exemption] == code:
                return exemption
        return None


#: The complete set of shipped policies, keyed by ADAPTER identity. Module-level
#: and never mutated: adding an exemption is a code change with a review, which
#: is exactly the property "registry-owned" is meant to buy.
_POLICY_REGISTRY: Dict[str, LegacyValidationPolicyV1] = {
    "flow_sequence": LegacyValidationPolicyV1(
        "flow_sequence",
        (
            LEGACY_ADAPTER_EXEMPTION_OPAQUE_STATE_WRITER,
            LEGACY_ADAPTER_EXEMPTION_DECISION_PROPERTY_READ,
            LEGACY_ADAPTER_EXEMPTION_STANDALONE_CACHE_READ,
        ),
    ),
    "wrapper_subprocess": LegacyValidationPolicyV1(
        "wrapper_subprocess",
        (LEGACY_ADAPTER_EXEMPTION_SUBPROCESS_SUMMARY,),
    ),
    "sync_pipeline": LegacyValidationPolicyV1(
        "sync_pipeline",
        (LEGACY_ADAPTER_EXEMPTION_OPAQUE_STATE_WRITER,),
    ),
}


def lookup_policy(adapter: str) -> Optional[LegacyValidationPolicyV1]:
    """The registered policy for an adapter, or None.

    Takes an ADAPTER name, never a caller-supplied token. An unknown name
    returns None, which means strict — fail closed, so a typo tightens rather
    than loosens.
    """
    return _POLICY_REGISTRY.get(adapter)


def registered_adapters() -> Tuple[str, ...]:
    return tuple(sorted(_POLICY_REGISTRY))


def apply_policy(
    report: ValidationReportV1, policy: Optional[LegacyValidationPolicyV1]
) -> ValidationReportV1:
    """Reclassify exempted ERRORS as advisories, recording which exemption did it.

    Nothing is dropped. The exempted finding survives as an advisory carrying
    the exemption code in its evidence, so a report always shows what the strict
    rule would have said — an exemption that silently deleted a finding would
    make the migration ledger unfalsifiable.

    Warnings and advisories pass through untouched: they never blocked, so
    exempting them would change nothing except to hide them.
    """
    if policy is None:
        return report

    kept: List[ValidationDiagnosticV1] = []
    for item in report.errors:
        exemption = policy.exemption_for(item.code)
        if exemption is None:
            kept.append(item)
            continue
        kept.append(
            finding(
                exemption,
                "advisory",
                "compatibility",
                item.path,
                evidence=(("related_code", item.code), ("exemption", "legacy_adapter")),
                internal_node_id=item.internal_node_id,
            )
        )
    return build_validation_report(
        tuple(kept) + report.warnings + report.advisories
    )


__all__ = [
    "LegacyValidationPolicyV1",
    "apply_policy",
    "lookup_policy",
    "registered_adapters",
]
