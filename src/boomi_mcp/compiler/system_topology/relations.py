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
    TOPOLOGY_SCHEMA_INVALID_CARDINALITY,
)
from ...models.system_topology import SystemTopologySpecV1
from .capabilities import capability_for
from .contracts import (
    PlannedTopologyRelationV1,
    TopologyDecisionV1,
    TopologyDiagnosticV1,
    TopologyGuidanceV1,
)
from .context import PreparedTopologyContextV1, _normalize_component_type
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


def _process_ir_available(subject_ref: str, ctx) -> bool:
    """Does the ComponentPlan symbol behind this planned ref declare a ProcessIR root?

    ``has_process_ir`` was carried on every symbol and consumed by nothing, so
    a caller could label evidence ``witness="process_ir"`` for a planned process
    whose symbol says it has no ProcessIR at all, and the relation planned
    clean. The label was trusted on its own word; this is what makes it a claim
    about the plan rather than about the string.
    """
    if not subject_ref.startswith(_REF_TOKEN_PREFIX):
        return False
    key = subject_ref[len(_REF_TOKEN_PREFIX) :]
    # The SELECTED row, by the same deterministic rule resolution uses
    # (sorted-last per key), not "any row with this key". Duplicate symbol rows
    # are permitted, so ``any()`` let a conflicting row — a ``documentcache``
    # entry marked ``has_process_ir=True`` — authorize a ProcessIR witness for
    # the ``process`` row that was actually selected and says it has none.
    #
    # Two steps, and they must not be collapsed into one sort.
    #
    # 1. Pick the TYPE exactly as resolution does: sorted-last over the
    #    NORMALIZED type. Sorting raw types picks a different row whenever a
    #    duplicate uses a builder-legal case variant — ``PROCESS`` sorts before
    #    ``documentcache`` while ``process`` sorts after it — so the two
    #    disagreed and a valid planned relation was gated.
    #
    # 2. Read the flag from EVERY row of that selected type, and require them to
    #    agree. Sorting ``(type, flag)`` tuples together made the boolean a
    #    tie-breaker, so two alias rows that normalize alike but disagree on the
    #    flag always yielded ``True`` — fail-open, in the one helper whose whole
    #    job is to stop a witness being authorized by something that is not
    #    there. Resolution makes no such choice; it selects a type and nothing
    #    else. Disagreement is unresolvable evidence, so it fails closed.
    rows = [
        (_normalize_component_type(symbol.component_type), symbol.has_process_ir)
        for symbol in ctx.component_plan_symbols
        if symbol.component_key == key
    ]
    if not rows:
        return False
    selected_type = sorted(component_type for component_type, _ in rows)[-1]
    flags = {flag for component_type, flag in rows if component_type == selected_type}
    return flags == {True}


def _accepted_witness(subject_ref: str, candidates) -> Optional[str]:
    """The witness this subject's form accepts, or None.

    Takes ALL candidates for the edge, not whichever one a dict comprehension
    happened to keep. Building the lookup as ``{key: row.witness for row in ...}``
    silently retains the LAST row for a duplicated key, so a context holding a
    valid ``process_ir`` and a stale ``component_xml`` for the same edge gated
    or accepted purely on their order in the tuple — the same evidence SET,
    two different verdicts. Nothing constrains that tuple to be unique, and a
    contract whose central claim is determinism cannot decide on input order.

    "Any candidate of the accepted form" is also monotone: adding evidence can
    turn a gated relation into a witnessed one, never the reverse.
    """
    accepted = sorted(
        w for w in candidates if w in _WITNESS_FORMS[_form(subject_ref)]
    )
    # Sorted, then first. Today the choice is unobservable — every evidence
    # model's ``witness`` Literal overlaps each form's accepted set in exactly
    # ONE value, so ``accepted`` never holds two distinct kinds. The sort is
    # here for the day a Literal widens: at that point the tie becomes real, and
    # a total order is what keeps the plan from depending on input order again.
    # ``test_at_most_one_witness_kind_is_accepted_per_form`` pins the invariant
    # so the widening surfaces rather than silently reintroducing ambiguity.
    return accepted[0] if accepted else None


def _object_index(spec: SystemTopologySpecV1) -> Dict[str, object]:
    index: Dict[str, object] = {}
    for obj in spec.objects:
        index.setdefault(obj.key, obj)
    return index


def _component_ref(obj: object) -> str:
    return getattr(obj, "component_ref", "")


def _platform_ref(obj: object, field: str) -> str:
    return getattr(obj, field, "")


def _binding_corroborated(rel, objects, ctx) -> bool:
    """Does a live snapshot actually contain this structural binding?

    A schedule binding is corroborated when the snapshot holds a schedule fact
    for the same (process, runtime) pair; a deployment binding when it holds a
    deployment record for the same (component, environment) pair. Both compare
    against LITERAL platform ids — a planned ``$ref`` component does not exist
    yet, so nothing live can corroborate it, and saying otherwise would be the
    same overclaim as the ``live_fact`` label itself.
    """
    snapshot = ctx.snapshot
    # A snapshot whose ENVELOPE names another account corroborates nothing here,
    # whatever its individual rows claim about themselves.
    if snapshot is None or snapshot.profile != ctx.profile:
        return False
    # Profile-filtered. These scans read the RAW snapshot, so a foreign-account
    # fact whose ids happen to match was accepted as corroboration and the plan
    # published a foreign ``live_fact`` alongside its own profile-mismatch
    # blocker. ``prepare_topology_context`` counts such rows as foreign; this
    # helper has to refuse them too.
    profile = ctx.profile

    # ENFORCED, not merely documented. A planned ``$ref`` component does not
    # exist yet, so nothing live can corroborate it — but a snapshot fact whose
    # ``process_id`` literally read ``$ref:kp`` matched by string equality and
    # promoted the binding to ``live_fact``. The docstring said literal ids; the
    # code compared whatever it was handed.
    subject_ref = _component_ref(objects.get(getattr(rel, "process", "")))
    if not subject_ref or subject_ref.startswith(_REF_TOKEN_PREFIX):
        return False

    if rel.kind == "schedule_binding":
        process_ref = _component_ref(objects.get(rel.process))
        runtime_ref = _platform_ref(objects.get(rel.runtime), "runtime_ref")
        return any(
            fact.profile == profile
            and fact.process_id == process_ref
            and fact.runtime_id == runtime_ref
            for fact in snapshot.schedule_bindings
        )
    if rel.kind == "deployment_binding":
        process_ref = _component_ref(objects.get(rel.process))
        environment_ref = _platform_ref(objects.get(rel.environment), "environment_ref")
        return any(
            fact.profile == profile
            and fact.component_id == process_ref
            and fact.environment_id == environment_ref
            for fact in snapshot.deployments
        )
    return False


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
            # A CARDINALITY violation, not an unsupported lifecycle: the shape
            # is fine, there is simply one binding too many. The plan specifies
            # this code at the later binding, and using the lifecycle code here
            # would send a caller looking for a shape problem that is not there.
            findings.append(
                topology_finding(
                    TOPOLOGY_SCHEMA_INVALID_CARDINALITY,
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
                    TOPOLOGY_SCHEMA_INVALID_CARDINALITY,
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

    # Every candidate per edge, not one survivor of a dict comprehension.
    call_witnesses: Dict[Tuple[str, str], List[str]] = {}
    for e in ctx.process_call_evidence:
        call_witnesses.setdefault(
            (e.caller_component_ref, e.callee_component_ref), []
        ).append(e.witness)
    route_witnesses: Dict[Tuple[str, str], List[str]] = {}
    for e in ctx.api_service_route_evidence:
        route_witnesses.setdefault(
            (e.api_service_component_ref, e.listener_component_ref), []
        ).append(e.witness)
    use_witnesses: Dict[Tuple[str, str, str], List[str]] = {}
    for e in ctx.shared_resource_use_evidence:
        use_witnesses.setdefault(
            (e.process_component_ref, e.resource_component_ref, e.resource_kind), []
        ).append(e.witness)

    for index, rel in enumerate(spec.relations):
        if rel.kind not in _WITNESS_REQUIRED:
            # Structural bindings the caller declares. They still had to pass
            # reference and shape checks to get here.
            if rel.kind in ("schedule_binding", "deployment_binding"):
                # Corroborated only when a snapshot actually contains the
                # matching binding; otherwise it is the caller's declaration and
                # is labelled as such. Claiming ``live_fact`` with no snapshot
                # at all put a corroboration the planner never had into the
                # plan's own output.
                planned.append(
                    PlannedTopologyRelationV1(
                        relation_key=rel.key,
                        relation_kind=rel.kind,
                        witness=(
                            "live_fact"
                            if _binding_corroborated(rel, objects, ctx)
                            else "declared_intent"
                        ),
                    )
                )
            continue

        candidates: List[str] = []
        # The subject whose artifact does the witnessing: the CALLER of a call,
        # the API service that routes, the process that uses a resource.
        subject_ref = ""
        if rel.kind == "process_call":
            caller = _component_ref(objects.get(rel.caller_process))
            callee = _component_ref(objects.get(rel.callee_process))
            subject_ref = caller
            candidates = call_witnesses.get((caller, callee), [])
        elif rel.kind == "api_service_route":
            asc = _component_ref(objects.get(rel.api_service))
            listener = _component_ref(objects.get(rel.listener_process))
            subject_ref = asc
            candidates = route_witnesses.get((asc, listener), [])
        elif rel.kind == "document_cache_use":
            process = _component_ref(objects.get(rel.process))
            cache = _component_ref(objects.get(rel.document_cache))
            subject_ref = process
            candidates = use_witnesses.get((process, cache, "document_cache"), [])
        elif rel.kind == "process_property_use":
            process = _component_ref(objects.get(rel.process))
            prop = _component_ref(objects.get(rel.process_property))
            subject_ref = process
            candidates = use_witnesses.get((process, prop, "process_property"), [])

        witness = _accepted_witness(subject_ref, candidates)
        # A ``process_ir`` witness is only as good as the ProcessIR root behind
        # it. Without one there is nothing for the claim to be true OF.
        if witness == "process_ir" and not _process_ir_available(subject_ref, ctx):
            witness = None
        if witness is None:
            # The normative ``lifecycle`` phase. Kind-level gating stays in
            # ``capability``; this is the witness question, which the plan's
            # phase order names separately and which was previously folded into
            # ``relation``, leaving ``lifecycle`` unused entirely.
            findings.append(
                topology_finding(
                    TOPOLOGY_CAPABILITY_GATED,
                    severity="error",
                    phase="lifecycle",
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
        # Every OBSERVED classification per environment, profile-filtered.
        #
        # Three defects in one line, previously: a ``{id: classification}``
        # comprehension is last-wins, so a duplicate row with ``classification
        # is None`` — the designed output of ``_opt_classification`` for a
        # missing or mis-cased field — ERASED a real contradiction and a blocked
        # plan came back valid. That is failing OPEN, not merely
        # order-dependent. It also read the raw snapshot, so a foreign-account
        # row supplied the classification for this account's environment.
        #
        # Unobserved rows contribute nothing rather than overwriting; a
        # contradiction against any observed value is a finding.
        observed: Dict[str, Set[str]] = {}
        # The envelope gate applies HERE as well. This scan read the raw
        # snapshot, so a row stamped with the context's profile inside a
        # foreign-envelope capture still produced a classification
        # contradiction — from the very snapshot the planner had just declared
        # untrustworthy.
        #
        # And the gate is anchored on the AUTHORED profile, not merely on an
        # internally-coherent context — the same predicate that qualifies
        # absence authority in ``collect_reference_findings``. Gating on
        # ``ctx.profile`` alone made the two halves of one report disagree about
        # the very same evidence: a context and snapshot that agreed with each
        # OTHER but not with the spec were refused as proof that this account's
        # components are missing, and simultaneously accepted as proof that this
        # account's authored classification is wrong — with a remediation that
        # says to align the document with the other account's data. Agreement
        # among the wrong sources is not evidence, in either direction.
        envelope_ok = snapshot.profile == ctx.profile == spec.profile_ref
        for fact in snapshot.environments if envelope_ok else ():
            # Per-fact, on the same anchor. A row naming another account inside
            # an otherwise-matching envelope is discarded for resolution, so
            # letting it supply that environment's classification would have one
            # report say both "this environment does not resolve in your
            # context" and "your classification for it disagrees with what
            # discovery observed."
            if fact.profile != spec.profile_ref or fact.classification is None:
                continue
            observed.setdefault(fact.environment_id, set()).add(fact.classification)
        for index, obj in enumerate(spec.objects):
            if obj.kind != "environment" or obj.classification is None:
                continue
            seen = observed.get(obj.environment_ref)
            # EVERY observation must agree, not merely one of them.
            # ``obj.classification not in seen`` was too weak: with both TEST
            # and PROD observed for one environment, an authored TEST is "in"
            # the set and passed silently — leaving the plan valid on evidence
            # that contradicts itself. Inconsistent duplicate observations are
            # exactly the case a caller needs told about.
            #
            # An environment discovery did not see, or saw without a
            # classification, is still not a classification problem: ``seen`` is
            # empty and nothing fires. The reference collector already reports a
            # genuinely missing environment, and reporting it twice under a
            # different code would send a caller chasing a bug that does not
            # exist.
            if seen and seen != {obj.classification}:
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
                # "No shape has evidence" was false of one of the three things
                # it listed. ``ScheduleBindingFactV1.observed_max_retry`` exists
                # precisely because retry IS observed live — its own docstring
                # calls it the one piece of schedule configuration with live
                # evidence. Not-modeled and no-evidence are different claims,
                # and collapsing them contradicted the snapshot in the same
                # package.
                message=(
                    "Schedule content (cron/interval, retry policy, active "
                    "state) is not modeled here. Every schedule body observed "
                    "live was empty, so cron/interval shape has no evidence to "
                    "model from. Retry differs: where a capture observes a "
                    "max-retry value it is recorded as a discovery observation "
                    "rather than as authored intent, so it is never something "
                    "this contract sets. Set schedule "
                    "content with the existing schedule tools after the "
                    "components exist."
                ),
            )
        )
    if "api_service" in kinds or "api_service_route" in kinds:
        guidance.append(
            TopologyGuidanceV1(
                subject="api_service",
                # About the CAPTURE behind the capability rows, not about the
                # account as it stands. "No API Service Component exists in
                # either live profile today" is a live universal in the present
                # tense, and the plan's own payload can refute it: the shipped
                # capture reads ``webservice``, so a resolved
                # ``existing_component`` row published beside this sentence
                # said both. "Either live profile" was stale on top of that —
                # there is one profile now. A statement about what a past
                # capture observed stays true whatever today's snapshot holds.
                message=(
                    "The live evidence leg for this relation is unavailable: no "
                    "API Service Component was observed when this contract's "
                    "capability rows were captured, so route behavior rests on "
                    "the typed builder and its recorded fixtures rather than on "
                    "live confirmation."
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
    snapshot = prepared.context.snapshot
    # A snapshot from ANOTHER account is not revalidation of this one. Testing
    # only for ``is None`` let a foreign snapshot suppress the decision and then
    # publish that account's paging gaps, telling the caller about omega's
    # truncation instead of that alpha was never read.
    #
    # BOTH anchors, not either. Usability is decided against the context's
    # profile — ``prepare_topology_context`` discards every row of an
    # envelope-mismatched snapshot — while relevance is decided against the
    # authored one. Checking only the spec left the third shape unfixed: a
    # snapshot matching the SPEC but not the CONTEXT contributed nothing at all,
    # and the caller was still handed that discarded capture's pagination notice
    # instead of being told to re-run with a snapshot the context can use.
    if (
        snapshot is None
        or snapshot.profile != spec.profile_ref
        or snapshot.profile != prepared.context.profile
    ):
        decisions.append(
            TopologyDecisionV1(
                subject="live_revalidation",
                # True in all THREE triggers, and its remedy actionable in each.
                # "Produced without a live discovery snapshot" was false in two
                # of them — a snapshot was supplied and then refused — beside a
                # blocker whose own remediation names the snapshot envelope.
                # The first correction still failed the THIRD trigger, which
                # fires alone when the snapshot matches the spec but not the
                # context: there both "none was supplied" and "belongs to a
                # different account than the one being planned" are false of a
                # genuine capture of exactly the planned account, and "re-run
                # with a snapshot for this account" is inert — following it
                # reproduces the same plan. Matching BOTH anchors is the
                # condition actually being tested, so it is the condition
                # stated, and supplying a capture that matches both is a step
                # that changes the outcome in every trigger.
                question=(
                    "No live discovery snapshot applies to this plan. Either "
                    "none was supplied, or the one supplied names an account "
                    "that does not match BOTH this plan's profile_ref and the "
                    "resolution context it was handed to. Existing-component "
                    "references and environment classifications are therefore "
                    "unverified. Supply a snapshot whose profile matches both "
                    "before acting on it."
                ),
            )
        )
    else:
        # The environment listing is the OTHER query that can fail to answer,
        # and until it was given an observation flag it had no way to say so.
        # Giving it one created a third state — observed / observed-empty /
        # did-not-answer — whose only published trace was one missing row in
        # ``resolved_references``: no blocker, no warning, ``is_valid`` true.
        # That is the silence the component path already refuses, for the same
        # reason and in the same words. Reported under its own subject rather
        # than folded into ``discovery_unobserved_query``: that decision's text
        # is about component queries and tells the caller what an unobserved
        # QUEUE listing does not prove, which is not the action to take here.
        if not prepared.environment_inventory_observed:
            decisions.append(
                TopologyDecisionV1(
                    subject="environment_inventory_unobserved",
                    # Scoped to ABSENCE, exactly like the component sibling.
                    # "No environment reference was judged against it" was
                    # false and self-contradicting: a snapshot assembled by hand
                    # (or by a pre-envelope adapter) can carry environment rows
                    # while claiming no observation, and ``resolve_topology_
                    # references`` still resolves against those rows — correctly,
                    # because a PRESENT row is positive evidence that something
                    # saw it. What an unobserved listing cannot support is the
                    # inference from silence. One plan may not report an
                    # environment as resolved and simultaneously say nothing was
                    # judged.
                    question=(
                        "The environment listing did not answer, so this "
                        "snapshot cannot be read as a complete environment "
                        "inventory. Rows it does carry still resolve; what it "
                        "cannot witness is ABSENCE — a reference missing from "
                        "it was left unjudged rather than reported not-found. "
                        "Re-run discovery before relying on an environment "
                        "being absent."
                    ),
                )
            )
        pages = snapshot.pagination
        unobserved = tuple(page for page in pages if not page.observed)
        if unobserved:
            # Reported separately from truncation, even though an unobserved
            # page is also treated as truncated. "The query did not answer" and
            # "there was more to read" call for different actions, and folding
            # the first into the second hides an outage behind a paging notice.
            decisions.append(
                TopologyDecisionV1(
                    subject="discovery_unobserved_query",
                    # Absence-scoped, for the same reason the environment
                    # notice is: "cannot say whether those components exist"
                    # publishes beside the rows that DID resolve out of the same
                    # snapshot. An unanswered listing withdraws the inference
                    # from silence, not the rows in hand.
                    question=(
                        "At least one component query did not answer. Rows the "
                        "snapshot does carry still resolve; what it cannot "
                        "witness is ABSENCE — a component missing from an "
                        "unanswered listing was left unjudged rather than "
                        "reported not-found. Re-run discovery before relying "
                        "on any absence — in particular, an unobserved queue "
                        "listing is not evidence that the account has no "
                        "queues."
                    ),
                )
            )
        truncated = tuple(page for page in pages if page.observed and page.truncated)
        if truncated:
            decisions.append(
                TopologyDecisionV1(
                    subject="discovery_pagination",
                    # Scoped to COMPONENT references, because that is all
                    # pagination witnesses. Environment and runtime absence are
                    # judged from their own listings, so a plan could block on
                    # ``/objects/N/environment_ref`` while this notice told the
                    # caller that absence proved nothing and to page through
                    # before believing it — advice that is inert against that
                    # blocker: paging fully retires the notice and leaves the
                    # blocker exactly where it was. Narrowing it to COMPONENT
                    # references did not go far enough: no published component
                    # not-found is a paging artifact either. A literal id is
                    # only reported missing when ``expected in complete``, and
                    # ``complete`` is exactly the types whose page was observed
                    # AND untruncated; a ``$ref`` resolves against the symbol
                    # table, complete by construction. Paging is therefore
                    # inert against every blocker there is. What truncation
                    # actually costs is COVERAGE, and that is all this notice
                    # now claims.
                    #
                    # It deliberately makes NO claim about what re-running does
                    # to existing findings. Two attempts at one did not survive
                    # contact with the planner. "Paging can only add findings"
                    # is false: ``_collect`` skips the dependency phase whenever
                    # a reference finding exists, so a page that adds one
                    # REMOVES an already-reported cycle. "A literal id is only
                    # reported missing from a complete listing" is true of that
                    # id and still does not bound the outcome, because
                    # ``component_ids`` is keyed by id across every type — so
                    # paging a DIFFERENT type can turn a not-found into a type
                    # mismatch and retire it. A notice whose job is to report a
                    # coverage gap has no business predicting a re-run.
                    question=(
                        "At least one component query was truncated, so any "
                        "component on a page that was not read was left "
                        "UNJUDGED — neither confirmed nor refuted. Page "
                        "through fully to have those judged. This notice is "
                        "about COVERAGE, and says nothing about the findings "
                        "already in this plan: re-running against a complete "
                        "capture can change them in either direction, so read "
                        "the new plan rather than assuming this one held. "
                        "Environment and runtime references are outside this "
                        "notice's scope — their absence is judged from their "
                        "own listings, not from pagination."
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
