"""SystemTopology planner contracts: diagnostics, capability report, plan (#144).

DARK. Nothing at runtime imports this package; no MCP tool or action is
registered (ADR-001 assigns the planning surface to #146).

Why a third contract layer
--------------------------
The repo already has ``CompilerDiagnostic`` ("the compile failed") and
``ValidationDiagnosticV1`` ("your ProcessIR payload is wrong, ranked"). A
``TopologyDiagnosticV1`` answers a third question — "here is what this topology
can and cannot be planned into, and why" — and differs from both in what it is
allowed to say:

* **Capability is a first-class verdict, not an error.** A gated subject is not
  a mistake the caller made; it is a statement about the evidence available
  today. It must be reportable without implying the payload is malformed.
* **Ownership.** ADR-001 §7 reserves ``TOPOLOGY_*`` to #144. A topology report
  may not carry a ``PROCESS_IR_*`` code — that would blame process semantics for
  a cross-process relationship problem, sending a caller to rewrite the wrong
  file — and the planner's own internal defects raise
  :class:`~.invariants.TopologyPlanningInvariantError` rather than being
  laundered into an authored-payload diagnostic.

Security
--------
``message`` and ``remediation`` are STATIC strings selected by code — never
interpolated. ``subject`` and ``provenance`` are constrained to structural
tokens, and ``__repr_args__`` suppresses every non-structural field, so neither
a log line nor a traceback can carry authored text.
"""

from __future__ import annotations

import json
import re
from typing import Dict, FrozenSet, Iterable, List, Literal, Tuple

from pydantic import BaseModel, ConfigDict, Field, computed_field, field_validator

# --------------------------------------------------------------------------
# vocabularies
# --------------------------------------------------------------------------

TopologySeverityV1 = Literal["error", "warning", "advisory"]

#: Only ``error`` reaches ``blockers``. ``warning`` and ``advisory`` reach
#: ``guidance`` — which is what lets a tightened rule ship before it blocks.
#: ``unresolved_decisions`` is a SEPARATE typed bucket carrying no severity at
#: all: "we do not know yet" is not a defect on a scale with "this is wrong".
TOPOLOGY_SEVERITY_ORDER: Tuple[str, ...] = ("error", "warning", "advisory")

TopologyPhaseV1 = Literal[
    "model",
    "capability",
    "reference",
    "relation",
    "lifecycle",
    "environment",
    "dependency",
    "plan_invariant",
]

#: Normative phase order. RANK, not name, drives sorting — so the earliest
#: failure in the pipeline reads first regardless of alphabet. Reordering this
#: tuple changes report ordering, which is a contract change.
TOPOLOGY_PHASE_ORDER: Tuple[str, ...] = (
    "model",
    "capability",
    "reference",
    "relation",
    "lifecycle",
    "environment",
    "dependency",
    "plan_invariant",
)

_PHASE_RANK: Dict[str, int] = {
    phase: index for index, phase in enumerate(TOPOLOGY_PHASE_ORDER)
}

TopologyCapabilityStateV1 = Literal[
    "emittable",
    "plannable-only",
    "guidance-only",
    "gated-no-evidence",
    "unsupported",
]

TopologyEvidenceStatusV1 = Literal[
    "verified",
    "corroborating_only",
    "unavailable",
    "conflicting",
    "not_captured",
]

# A structural token: lowercase identifier-ish, dots/slashes/dashes/colons/hashes
# for module paths and doc anchors. Deliberately excludes spaces, uppercase and
# newlines — a component name, a label, or an exception's text cannot match.
_SAFE_TOKEN = re.compile(r"^[a-z0-9][a-z0-9_./#:-]{0,95}$")

# Fields safe to expose in reprs. Everything else is suppressed.
_REPR_SAFE_FIELDS: FrozenSet[str] = frozenset(
    {
        "version",
        "code",
        "severity",
        "phase",
        "state",
        "leg",
        "status",
        "kind",
        "subject_kind",
        "namespace",
        "basis",
        "owner",
        "apply_supported",
    }
)


class _TopologyPlanningModel(BaseModel):
    """Frozen, strict base for every DERIVED planner output.

    The repr allowlist is NOT imported from ``semantic_validation.contracts``,
    deliberately: that package's safe-field set answers "what is structural in a
    ProcessIR finding", and sharing one frozenset would mean a widening there
    silently widens what a topology traceback may print. Two authorities, two
    allowlists.
    """

    model_config = ConfigDict(extra="forbid", frozen=True)

    def __repr_args__(self):  # noqa: D105 — pydantic hook
        for key, value in super().__repr_args__():
            if key in _REPR_SAFE_FIELDS:
                yield key, value
            else:
                yield key, "..."


# --------------------------------------------------------------------------
# diagnostics
# --------------------------------------------------------------------------


class TopologyDiagnosticV1(_TopologyPlanningModel):
    """One topology finding: stable code, severity, authored position, provenance."""

    code: str
    severity: TopologySeverityV1
    phase: TopologyPhaseV1
    path: str
    subject: str = ""
    message: str
    remediation: str
    provenance: Tuple[str, ...] = ()

    @field_validator("code")
    @classmethod
    def _only_the_topology_family(cls, value: str) -> str:
        """A topology report may carry ONLY ``TOPOLOGY_*`` codes.

        ADR-001 §7 gives ``TOPOLOGY_*`` to #144 and the ProcessIR families to
        #136-#143. A ``PROCESS_IR_*`` code surfacing here would tell a caller
        that a cross-process relationship problem is a process-semantics
        problem, sending them to rewrite the wrong artifact entirely.

        Enforced structurally rather than by convention because the docs already
        claimed it and nothing checked — the same reason
        ``ValidationDiagnosticV1`` walls off the compile family.
        """
        if not value.startswith("TOPOLOGY_"):
            raise ValueError("a topology report can only carry TOPOLOGY_* codes")
        return value

    @field_validator("subject")
    @classmethod
    def _subject_is_a_structural_token(cls, value: str) -> str:
        """``subject`` names a KIND or a registry feature row, never an instance.

        This is the redaction control that matters: an authored object key such
        as ``customer_email_sync`` is lexically indistinguishable from a
        structural token, so no value rule could reject it. The reason it cannot
        appear is that callers never put one here — collectors pass the kind.
        """
        if value == "":
            return value
        if not _SAFE_TOKEN.match(value):
            raise ValueError("subject must be a structural token")
        return value

    @field_validator("provenance")
    @classmethod
    def _provenance_entries_are_structural(cls, value: Tuple[str, ...]) -> Tuple[str, ...]:
        for item in value:
            if not _SAFE_TOKEN.match(item):
                raise ValueError("provenance entries must be structural tokens")
        # Canonicalize on the way in so two findings differing only in the order
        # their provenance was appended dedup against each other.
        return tuple(sorted(value))

    def dedup_key(self) -> Tuple[str, str, str, Tuple[str, ...]]:
        """Identity for deduplication: code, path, subject, provenance.

        Severity is deliberately NOT part of this key — buckets dedup
        independently, so the same code at the same path may legitimately appear
        once as an error and once as an advisory from different phases.
        """
        return (self.code, self.path, self.subject, self.provenance)

    def sort_key(self) -> Tuple[int, str, str, str, Tuple[str, ...]]:
        """Total order: phase rank, path, subject, code, provenance.

        Every component is needed for a TOTAL order. Stopping at ``code`` would
        leave two findings that differ only in provenance in arbitrary relative
        order — stable within a process, unstable across runs, which is exactly
        the defect the determinism criterion exists to prevent.
        """
        return (
            _PHASE_RANK.get(self.phase, len(_PHASE_RANK)),
            self.path,
            self.subject,
            self.code,
            self.provenance,
        )


class TopologyValidationReportV1(_TopologyPlanningModel):
    """Immutable, deterministically ordered validation result."""

    version: Literal["1"] = "1"
    errors: Tuple[TopologyDiagnosticV1, ...] = ()
    warnings: Tuple[TopologyDiagnosticV1, ...] = ()
    advisories: Tuple[TopologyDiagnosticV1, ...] = ()

    @computed_field  # type: ignore[prop-decorator]
    @property
    def is_valid(self) -> bool:
        """True when nothing BLOCKS. Warnings and advisories do not."""
        return not self.errors


# --------------------------------------------------------------------------
# capability report
# --------------------------------------------------------------------------


class CapabilityEvidenceLegV1(_TopologyPlanningModel):
    """One leg of the three-way evidence rule: source, documentation, or live.

    The issue's research gate asks each relation to be confirmed "in source,
    authoritative docs, and at least one real component where available". Three
    separate legs, each with its own status, is what makes "where available"
    checkable instead of rhetorical: a subject whose live leg is ``unavailable``
    is visibly different from one nobody looked at (``not_captured``) and from
    one where the sources disagree (``conflicting``).

    A ``conflicting`` leg is REPORTED, never resolved by precedence. Picking a
    winner would hide exactly the case a human needs to see.
    """

    leg: Literal["source", "documentation", "live"]
    status: TopologyEvidenceStatusV1
    reference: str

    @field_validator("reference")
    @classmethod
    def _reference_is_a_structural_token(cls, value: str) -> str:
        if not _SAFE_TOKEN.match(value):
            raise ValueError("evidence reference must be a structural token")
        return value


class TopologyCapabilityEntryV1(_TopologyPlanningModel):
    """The capability verdict for one object kind, relation kind, or feature row."""

    subject: str
    subject_kind: Literal["object", "relation", "feature"]
    state: TopologyCapabilityStateV1
    source: CapabilityEvidenceLegV1
    documentation: CapabilityEvidenceLegV1
    live: CapabilityEvidenceLegV1
    present_in_spec: bool = False

    def sort_key(self) -> Tuple[str, str]:
        return (self.subject_kind, self.subject)


class TopologyCapabilityReportV1(_TopologyPlanningModel):
    """Every registered subject and its verdict — including absent ones.

    Absent kinds are included deliberately. A report that listed only what the
    caller happened to author would make "queues are gated" invisible to anyone
    who did not already try to use one, which defeats the purpose of publishing
    a capability surface at all.
    """

    version: Literal["1"] = "1"
    revision: str
    entries: Tuple[TopologyCapabilityEntryV1, ...] = ()


# --------------------------------------------------------------------------
# plan buckets
# --------------------------------------------------------------------------


class ResolvedTopologyReferenceV1(_TopologyPlanningModel):
    """An authored reference that resolved, and what it resolved to."""

    object_key: str
    object_kind: str
    resolution: Literal["component_plan_symbol", "existing_component", "platform_resource"]


class ComponentPlanPrerequisiteV1(_TopologyPlanningModel):
    """A component this topology needs built before it could be realized.

    ``owner`` is pinned to ``component_plan`` and is the structural marker that
    keeps two graphs apart: this is a BUILD-ordering fact owned by
    ``IntegrationSpecV1``, and topology only reports it. The planner never calls
    a materializer, and a prerequisite is not an instruction to build.
    """

    owner: Literal["component_plan"] = "component_plan"
    component_key: str
    component_type: str


class PlannedTopologyRelationV1(_TopologyPlanningModel):
    """A relation that passed validation and has a trusted witness.

    ``namespace`` is pinned to ``system_topology``: this is a RUNTIME
    relationship, not a build dependency and not a CFG edge. The literal is what
    makes the three graphs structurally non-interchangeable rather than merely
    documented as different.
    """

    namespace: Literal["system_topology"] = "system_topology"
    relation_key: str
    relation_kind: str
    witness: Literal["process_ir", "component_xml", "live_fact", "typed_builder"]


class TopologyRuntimeOrderV1(_TopologyPlanningModel):
    """A deterministic linearization of the ProcessCall graph — and ONLY that.

    ``basis`` is pinned to ``process_call``. Cache use, property use, schedule
    binding, deployment binding and API routing are all real relations that are
    deliberately NOT edges here: none of them implies "A must run before B", and
    folding them in would produce an order that looks authoritative and means
    nothing.
    """

    namespace: Literal["topology_runtime"] = "topology_runtime"
    basis: Literal["process_call"] = "process_call"
    order: Tuple[str, ...] = ()


class TopologyGuidanceV1(_TopologyPlanningModel):
    """Static advice: something true and useful that blocks nothing."""

    subject: str
    message: str


class TopologyDecisionV1(_TopologyPlanningModel):
    """An explicitly unresolved decision, surfaced rather than guessed.

    Separate from both blockers and guidance because it is neither: the plan is
    not wrong and there is no advice to give — a human has to choose, or
    evidence has to arrive.
    """

    subject: str
    question: str


class SystemTopologyPlanV1(_TopologyPlanningModel):
    """The deterministic planning result.

    ``apply_supported`` is ``Literal[False]`` — a type, not a runtime value. A
    plan asserting it is appliable is unconstructible, so no code path, test
    double, or future edit can produce one by setting a flag.
    """

    version: Literal["1"] = "1"
    spec_sha256: str
    apply_supported: Literal[False] = False
    capability_report: TopologyCapabilityReportV1
    validation: TopologyValidationReportV1
    resolved_references: Tuple[ResolvedTopologyReferenceV1, ...] = ()
    executable_component_prerequisites: Tuple[ComponentPlanPrerequisiteV1, ...] = ()
    planning_only_relations: Tuple[PlannedTopologyRelationV1, ...] = ()
    runtime_process_order: TopologyRuntimeOrderV1 = Field(
        default_factory=TopologyRuntimeOrderV1
    )
    guidance: Tuple[TopologyGuidanceV1, ...] = ()
    blockers: Tuple[TopologyDiagnosticV1, ...] = ()
    unresolved_decisions: Tuple[TopologyDecisionV1, ...] = ()


# --------------------------------------------------------------------------
# assembly helpers
# --------------------------------------------------------------------------


def _bucket(
    diagnostics: Iterable[TopologyDiagnosticV1], severity: str
) -> Tuple[TopologyDiagnosticV1, ...]:
    seen: Dict[Tuple[str, str, str, Tuple[str, ...]], None] = {}
    kept: List[TopologyDiagnosticV1] = []
    for item in sorted(
        (d for d in diagnostics if d.severity == severity),
        key=lambda d: d.sort_key(),
    ):
        key = item.dedup_key()
        if key in seen:
            continue
        seen[key] = None
        kept.append(item)
    return tuple(kept)


def build_topology_report(
    diagnostics: Iterable[TopologyDiagnosticV1],
) -> TopologyValidationReportV1:
    """Bucket by severity, sort each bucket totally, drop exact duplicates.

    Sorting happens BEFORE deduplication so which of a duplicate pair survives is
    itself deterministic, rather than depending on collection order.
    """
    collected = tuple(diagnostics)
    return TopologyValidationReportV1(
        errors=_bucket(collected, "error"),
        warnings=_bucket(collected, "warning"),
        advisories=_bucket(collected, "advisory"),
    )


def _canonical_json(value: object) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=True)


def canonical_topology_report_json(report: TopologyValidationReportV1) -> str:
    """Canonical serialization, matching the #136/#137/#143 recipe.

    ``sort_keys=True`` orders object KEYS only; tuple order — which is the
    report's meaning — is preserved.
    """
    return _canonical_json(report.model_dump(mode="json"))


def canonical_topology_plan_json(plan: SystemTopologyPlanV1) -> str:
    return _canonical_json(plan.model_dump(mode="json"))


__all__: List[str] = [
    "TOPOLOGY_PHASE_ORDER",
    "TOPOLOGY_SEVERITY_ORDER",
    "CapabilityEvidenceLegV1",
    "ComponentPlanPrerequisiteV1",
    "PlannedTopologyRelationV1",
    "ResolvedTopologyReferenceV1",
    "SystemTopologyPlanV1",
    "TopologyCapabilityEntryV1",
    "TopologyCapabilityReportV1",
    "TopologyCapabilityStateV1",
    "TopologyDecisionV1",
    "TopologyDiagnosticV1",
    "TopologyEvidenceStatusV1",
    "TopologyGuidanceV1",
    "TopologyPhaseV1",
    "TopologyRuntimeOrderV1",
    "TopologySeverityV1",
    "TopologyValidationReportV1",
    "build_topology_report",
    "canonical_topology_plan_json",
    "canonical_topology_report_json",
]
