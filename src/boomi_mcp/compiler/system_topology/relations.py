"""Lifecycle, witness, environment and guidance collectors (#144).

Three separate phases share this module because they share the same question —
"is this relation real?" — asked at three depths:

* ``relation``: is the SHAPE a lifecycle the platform has? (A scheduled listener
  is not; a multi-process deployment unit is not.)
* ``lifecycle``: does a trusted WITNESS exist for it? A supported kind with no
  witness is gated, not accepted.
* ``environment``: is the evidence from the right PROFILE, and does an authored
  classification match what was observed?

Guidance and unresolved decisions are derived here too, because the same
evidence that gates a relation is what makes a decision unresolved.
"""

from __future__ import annotations

from typing import Dict, List, Optional, Set, Tuple

from ...errors import (
    TOPOLOGY_CAPABILITY_GATED,
    TOPOLOGY_ENVIRONMENT_MISMATCH,
    TOPOLOGY_RELATION_UNSUPPORTED,
)
from ...models.system_topology import SystemTopologySpecV1
from .capabilities import capability_for
from .contracts import (
    PlannedTopologyRelationV1,
    TopologyDecisionV1,
    TopologyDiagnosticV1,
    TopologyGuidanceV1,
)
from .context import PreparedTopologyContextV1
from .findings import topology_finding

#: Relation kinds that require a trusted witness before they may be planned.
#: The other kinds are structural bindings the caller declares outright —
#: nothing external witnesses "I intend to deploy this here".
_WITNESS_REQUIRED: Tuple[str, ...] = (
    "process_call",
    "api_service_route",
    "document_cache_use",
    "process_property_use",
)

_REF_TOKEN_PREFIX = "$ref:"

#: Which witness kinds a subject of each FORM may be established by.
#:
#: A planned (``$ref:``) component has no deployed artifact to read, so its
#: authored intent — the ProcessIR root, or the typed builder for an ASC — is
#: the authority for what it will do. An EXISTING component does have one, and
#: only that artifact witnesses what it actually does: an authored ProcessIR for
#: an existing process may describe an intended future shape, and accepting it
#: would let a plan assert an edge the deployed component does not have.
#:
#: ``evidence.py`` already enforces this when it CONSTRUCTS witnesses, but the
#: context is a public input — a caller can hand over
#: ``ProcessCallEvidenceV1(witness="process_ir")`` for a literal id directly,
#: which bypasses the constructor entirely. The rule therefore has to hold at
#: the point of use as well as the point of manufacture.
_WITNESS_FORMS: Dict[str, Tuple[str, ...]] = {
    "planned": ("process_ir", "typed_builder"),
    "existing": ("component_xml",),
}


def _form(component_ref: str) -> str:
    return "planned" if component_ref.startswith(_REF_TOKEN_PREFIX) else "existing"


def _witness_form_ok(subject_ref: str, witness: Optional[str]) -> bool:
    if witness is None:
        return False
    return witness in _WITNESS_FORMS[_form(subject_ref)]


def _object_index(spec: SystemTopologySpecV1) -> Dict[str, object]:
    index: Dict[str, object] = {}
    for obj in spec.objects:
        index.setdefault(obj.key, obj)
    return index


def _component_ref(obj: object) -> str:
    return getattr(obj, "component_ref", "")


def collect_relation_findings(
    spec: SystemTopologySpecV1,
) -> Tuple[TopologyDiagnosticV1, ...]:
    """Shape rules: lifecycles the platform does not have.

    None of these is expressible as a per-model validator, because each is a
    statement about how several relations interact.
    """
    findings: List[TopologyDiagnosticV1] = []

    # A listener process is INVOKED by its API service. Scheduling one asserts a
    # second, independent trigger for the same process — the platform binds a
    # schedule to a process/atom pair regardless, so this would not error at
    # deploy time; it would just quietly run the listener on a timer too.
    listener_keys = {
        rel.listener_process
        for rel in spec.relations
        if rel.kind == "api_service_route"
    }
    for index, rel in enumerate(spec.relations):
        if rel.kind == "schedule_binding" and rel.process in listener_keys:
            findings.append(
                topology_finding(
                    TOPOLOGY_RELATION_UNSUPPORTED,
                    severity="error",
                    phase="relation",
                    path=f"/relations/{index}/process",
                    subject="schedule_binding",
                    provenance=(capability_for("schedule_binding").source.reference,),
                )
            )

    # One schedule binds exactly one (process, runtime) pair — the platform's own
    # identity for a schedule IS that pair, so a second binding for the same
    # schedule object describes two schedules wearing one name.
    schedule_bindings: Dict[str, int] = {}
    for index, rel in enumerate(spec.relations):
        if rel.kind != "schedule_binding":
            continue
        if rel.schedule in schedule_bindings:
            findings.append(
                topology_finding(
                    TOPOLOGY_RELATION_UNSUPPORTED,
                    severity="error",
                    phase="relation",
                    path=f"/relations/{index}/schedule",
                    subject="schedule_binding",
                    provenance=(capability_for("schedule_binding").live.reference,),
                )
            )
        else:
            schedule_bindings[rel.schedule] = index

    # One deployment unit targets exactly one process at one environment.
    # ``orchestrate_deploy`` requires exactly one process, and no live evidence
    # supports atomic multi-process deployment; allowing a second binding would
    # let a plan imply an atomicity the platform does not offer.
    unit_bindings: Dict[str, int] = {}
    for index, rel in enumerate(spec.relations):
        if rel.kind != "deployment_binding":
            continue
        if rel.deployment_unit in unit_bindings:
            findings.append(
                topology_finding(
                    TOPOLOGY_RELATION_UNSUPPORTED,
                    severity="error",
                    phase="relation",
                    path=f"/relations/{index}/deployment_unit",
                    subject="deployment_binding",
                    provenance=(
                        capability_for("atomic_multi_process_deployment").source.reference,
                    ),
                )
            )
        else:
            unit_bindings[rel.deployment_unit] = index

    # A process may not call itself. Self-recursion is a cycle of length one; it
    # is caught here rather than only by the cycle detector so the finding names
    # the offending role instead of a graph.
    for index, rel in enumerate(spec.relations):
        if rel.kind == "process_call" and rel.caller_process == rel.callee_process:
            findings.append(
                topology_finding(
                    TOPOLOGY_RELATION_UNSUPPORTED,
                    severity="error",
                    phase="relation",
                    path=f"/relations/{index}/callee_process",
                    subject="process_call",
                )
            )

    return tuple(findings)


def collect_lifecycle_findings(
    spec: SystemTopologySpecV1,
    prepared: PreparedTopologyContextV1,
) -> Tuple[Tuple[TopologyDiagnosticV1, ...], Tuple[PlannedTopologyRelationV1, ...]]:
    """Witness gating: a supported relation with no trusted evidence is gated.

    Returns both the findings and the relations that DID earn a witness, because
    the two are decided by the same lookup and re-deriving one from the other
    would be a second place for the rule to live.

    Same code as kind-level gating, different phase (``relation`` vs
    ``capability``). That is intentional and sorts correctly — phase rank
    precedes path in the total order — and it keeps the two testable
    independently: a queue object must block with nothing referencing it, and a
    witness-less ProcessCall must block even though its kind is fully supported.
    """
    findings: List[TopologyDiagnosticV1] = []
    planned: List[PlannedTopologyRelationV1] = []
    objects = _object_index(spec)
    ctx = prepared.context

    call_witnesses = {
        (e.caller_component_ref, e.callee_component_ref): e.witness
        for e in ctx.process_call_evidence
    }
    route_witnesses = {
        (e.api_service_component_ref, e.listener_component_ref): e.witness
        for e in ctx.api_service_route_evidence
    }
    use_witnesses = {
        (e.process_component_ref, e.resource_component_ref, e.resource_kind): e.witness
        for e in ctx.shared_resource_use_evidence
    }

    for index, rel in enumerate(spec.relations):
        if rel.kind not in _WITNESS_REQUIRED:
            # Structural bindings the caller declares. They still had to pass
            # reference and shape checks to get here.
            if rel.kind in ("schedule_binding", "deployment_binding"):
                planned.append(
                    PlannedTopologyRelationV1(
                        relation_key=rel.key,
                        relation_kind=rel.kind,
                        witness="live_fact",
                    )
                )
            continue

        witness = None
        # The subject whose artifact does the witnessing: the CALLER of a call,
        # the API service that routes, the process that uses a resource.
        subject_ref = ""
        if rel.kind == "process_call":
            caller = _component_ref(objects.get(rel.caller_process))
            callee = _component_ref(objects.get(rel.callee_process))
            subject_ref = caller
            witness = call_witnesses.get((caller, callee))
        elif rel.kind == "api_service_route":
            asc = _component_ref(objects.get(rel.api_service))
            listener = _component_ref(objects.get(rel.listener_process))
            subject_ref = asc
            witness = route_witnesses.get((asc, listener))
        elif rel.kind == "document_cache_use":
            process = _component_ref(objects.get(rel.process))
            cache = _component_ref(objects.get(rel.document_cache))
            subject_ref = process
            witness = use_witnesses.get((process, cache, "document_cache"))
        elif rel.kind == "process_property_use":
            process = _component_ref(objects.get(rel.process))
            prop = _component_ref(objects.get(rel.process_property))
            subject_ref = process
            witness = use_witnesses.get((process, prop, "process_property"))

        if not _witness_form_ok(subject_ref, witness):
            findings.append(
                topology_finding(
                    TOPOLOGY_CAPABILITY_GATED,
                    severity="error",
                    phase="relation",
                    path=f"/relations/{index}",
                    subject=rel.kind,
                    provenance=(capability_for(rel.kind).live.reference,),
                )
            )
        else:
            planned.append(
                PlannedTopologyRelationV1(
                    relation_key=rel.key,
                    relation_kind=rel.kind,
                    witness=witness,  # type: ignore[arg-type]
                )
            )

    return tuple(findings), tuple(
        sorted(planned, key=lambda p: (p.relation_kind, p.relation_key))
    )


def collect_environment_findings(
    spec: SystemTopologySpecV1,
    prepared: PreparedTopologyContextV1,
) -> Tuple[TopologyDiagnosticV1, ...]:
    """Profile isolation and environment-classification agreement.

    The profile check is the security-relevant one: a fact read from another
    account is not weaker evidence, it is evidence about a different system.
    """
    findings: List[TopologyDiagnosticV1] = []
    ctx = prepared.context
    snapshot = ctx.snapshot

    if ctx.profile != spec.profile_ref:
        findings.append(
            topology_finding(
                TOPOLOGY_ENVIRONMENT_MISMATCH,
                severity="error",
                phase="environment",
                path="/profile_ref",
                subject="environment",
            )
        )

    if snapshot is not None and snapshot.profile != spec.profile_ref:
        findings.append(
            topology_finding(
                TOPOLOGY_ENVIRONMENT_MISMATCH,
                severity="error",
                phase="environment",
                path="/profile_ref",
                subject="environment",
            )
        )

    # A snapshot can carry the right profile while an individual fact inside it
    # names another account. ``prepare_topology_context`` discards those so they
    # cannot resolve anything, but discarding silently would leave a caller
    # wondering why a component they can see in the snapshot does not resolve —
    # and a mixed-profile snapshot is a defect in whatever produced it.
    if prepared.foreign_profile_fact_count:
        findings.append(
            topology_finding(
                TOPOLOGY_ENVIRONMENT_MISMATCH,
                severity="error",
                phase="environment",
                path="/profile_ref",
                subject="environment",
                provenance=("capture:discovery/mixed-profile-snapshot",),
            )
        )

    if snapshot is not None:
        observed = {e.environment_id: e.classification for e in snapshot.environments}
        for index, obj in enumerate(spec.objects):
            if obj.kind != "environment" or obj.classification is None:
                continue
            actual = observed.get(obj.environment_ref)
            # Only a CONTRADICTION is a finding. An environment discovery did
            # not see is a not-found problem, already reported by the reference
            # collector; reporting it twice under a different code would send a
            # caller chasing a classification bug that does not exist.
            if actual is not None and actual != obj.classification:
                findings.append(
                    topology_finding(
                        TOPOLOGY_ENVIRONMENT_MISMATCH,
                        severity="error",
                        phase="environment",
                        path=f"/objects/{index}/classification",
                        subject="environment",
                        provenance=(capability_for("environment").live.reference,),
                    )
                )

    return tuple(findings)


def derive_guidance(
    spec: SystemTopologySpecV1,
    prepared: PreparedTopologyContextV1,
) -> Tuple[TopologyGuidanceV1, ...]:
    """Static advice earned by what the spec actually contains.

    Every string here is static and selected by subject — same rule as findings.
    """
    guidance: List[TopologyGuidanceV1] = []
    kinds: Set[str] = {obj.kind for obj in spec.objects} | {
        rel.kind for rel in spec.relations
    }

    if "schedule" in kinds:
        guidance.append(
            TopologyGuidanceV1(
                subject="schedule_content",
                message=(
                    "Schedule content (cron/interval, retry policy, active state) "
                    "is not modeled: every schedule observed live carries an empty "
                    "body, so no shape has evidence. Set it with the existing "
                    "schedule tools after the components exist."
                ),
            )
        )
    if "api_service" in kinds or "api_service_route" in kinds:
        guidance.append(
            TopologyGuidanceV1(
                subject="api_service",
                message=(
                    "No API Service Component exists in either live profile today, "
                    "so the live leg of this relation's evidence is unavailable. "
                    "The typed builder and its recorded fixtures are the only "
                    "current support."
                ),
            )
        )
    if "deployment_unit" in kinds or "deployment_binding" in kinds:
        guidance.append(
            TopologyGuidanceV1(
                subject="deployment_unit",
                message=(
                    "Deployment intent is planning-only. Live reads establish "
                    "that deployment records exist and can be listed; they "
                    "establish nothing about creating one, so this contract has "
                    "no apply path. Use a separately-authorized deployment tool."
                ),
            )
        )
    if prepared.context.dependency_corroboration:
        guidance.append(
            TopologyGuidanceV1(
                subject="dependency_api_as_process_call_witness",
                message=(
                    "Dependency-API rows corroborate a relation but never "
                    "establish one: the response is a flat one-level list mixing "
                    "component types with no edge kind."
                ),
            )
        )
    return tuple(sorted(guidance, key=lambda g: g.subject))


def derive_unresolved_decisions(
    spec: SystemTopologySpecV1,
    prepared: PreparedTopologyContextV1,
) -> Tuple[TopologyDecisionV1, ...]:
    """Questions a human must answer or evidence must settle. Never guessed."""
    decisions: List[TopologyDecisionV1] = [
        TopologyDecisionV1(
            subject="account_capability_limits",
            question=(
                "No account capability/limit capture exists, so this plan cannot "
                "say whether the target account has the licence headroom for the "
                "components it lists. Capture limits before relying on it."
            ),
        )
    ]
    if prepared.context.snapshot is None:
        decisions.append(
            TopologyDecisionV1(
                subject="live_revalidation",
                question=(
                    "This plan was produced without a live discovery snapshot, so "
                    "existing-component references and environment classifications "
                    "are unverified. Re-run with a snapshot before acting on it."
                ),
            )
        )
    else:
        pages = prepared.context.snapshot.pagination
        unobserved = tuple(page for page in pages if not page.observed)
        if unobserved:
            # Reported separately from truncation, even though an unobserved
            # page is also treated as truncated. "The query did not answer" and
            # "there was more to read" call for different actions, and folding
            # the first into the second hides an outage behind a paging notice.
            decisions.append(
                TopologyDecisionV1(
                    subject="discovery_unobserved_query",
                    question=(
                        "At least one component query did not answer, so this "
                        "snapshot cannot say whether those components exist. "
                        "Re-run discovery before relying on any absence — in "
                        "particular, an unobserved queue listing is not "
                        "evidence that the account has no queues."
                    ),
                )
            )
        truncated = tuple(page for page in pages if page.observed and page.truncated)
        if truncated:
            decisions.append(
                TopologyDecisionV1(
                    subject="discovery_pagination",
                    question=(
                        "At least one component query was truncated, so absence "
                        "from this snapshot is not evidence of absence in the "
                        "account. Page through fully before treating a "
                        "not-found reference as real."
                    ),
                )
            )
    if any(obj.kind == "deployment_unit" for obj in spec.objects):
        decisions.append(
            TopologyDecisionV1(
                subject="topology_apply",
                question=(
                    "Deployment intent is declared but this contract has no apply "
                    "path. Decide which separately-authorized tool performs the "
                    "deployment, and in what order."
                ),
            )
        )
    return tuple(sorted(decisions, key=lambda d: d.subject))


__all__: List[str] = [
    "collect_environment_findings",
    "collect_lifecycle_findings",
    "collect_relation_findings",
    "derive_guidance",
    "derive_unresolved_decisions",
]
