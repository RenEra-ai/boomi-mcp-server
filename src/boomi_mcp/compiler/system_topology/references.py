"""Reference resolution: does an authored role name something real, of the right kind (#144).

Two questions, two codes, resolved independently so a caller sees both problems
in one pass:

* ``TOPOLOGY_REFERENCE_NOT_FOUND`` — the role names an object key this document
  does not declare, or a component/platform id this profile's context does not
  contain.
* ``TOPOLOGY_REFERENCE_TYPE_MISMATCH`` — the key resolves, but to the wrong kind.

Diagnostics never carry the failed key. That is the whole reason the endpoint
matrix is derived from the models rather than described in prose: the finding
can name the ROLE and the expected kind — both structural — and still be
actionable without echoing an authored value into a log line.
"""

from __future__ import annotations

from typing import Dict, List, Mapping, Tuple

from ...errors import TOPOLOGY_REFERENCE_NOT_FOUND, TOPOLOGY_REFERENCE_TYPE_MISMATCH
from ...models.system_topology import SystemTopologySpecV1
from .contracts import ResolvedTopologyReferenceV1, TopologyDiagnosticV1
from .context import PreparedTopologyContextV1
from .findings import topology_finding

#: Which object kind each relation role accepts. Keyed ``(relation kind, role)``
#: so an endpoint rule is a lookup, not a chain of ``if`` statements that a new
#: relation kind can silently fall through.
TOPOLOGY_ENDPOINT_MATRIX: Mapping[Tuple[str, str], str] = {
    ("process_call", "caller_process"): "process",
    ("process_call", "callee_process"): "process",
    ("api_service_route", "api_service"): "api_service",
    ("api_service_route", "listener_process"): "process",
    ("document_cache_use", "process"): "process",
    ("document_cache_use", "document_cache"): "document_cache",
    ("process_property_use", "process"): "process",
    ("process_property_use", "process_property"): "process_property",
    ("schedule_binding", "schedule"): "schedule",
    ("schedule_binding", "process"): "process",
    ("schedule_binding", "runtime"): "runtime",
    ("deployment_binding", "deployment_unit"): "deployment_unit",
    ("deployment_binding", "process"): "process",
    ("deployment_binding", "environment"): "environment",
    ("queue_reference", "process"): "process",
    ("queue_reference", "external_queue"): "external_queue",
    ("event_stream_reference", "process"): "process",
    ("event_stream_reference", "external_event_stream"): "external_event_stream",
}

_REF_TOKEN_PREFIX = "$ref:"

#: Object kinds whose identity is a component reference, and the Boomi component
#: TYPE each one requires. Carrying the expected type is what makes a
#: wrong-kinded reference detectable: an id-only index resolves a ``process``
#: object pointing at a Document Cache component just as happily as a correct
#: one, and the planner then treats a cache as a process.
_COMPONENT_BACKED = {
    "process": "process",
    "api_service": "webservice",
    "document_cache": "documentcache",
    "process_property": "processproperty",
}


def resolve_topology_references(
    spec: SystemTopologySpecV1,
    prepared: PreparedTopologyContextV1,
    blocked_object_indexes=(),
) -> Tuple[ResolvedTopologyReferenceV1, ...]:
    """Resolve every OBJECT to what backs it, in a stable order.

    ``blocked_object_indexes`` are authored positions that already drew a
    blocker and must not be resolved. Without it a duplicate object KEY — which
    the model rules correctly report — produced two resolution rows for one key
    and tripped the uniqueness invariant, so the planner emitted the right
    diagnostic and then RAISED ``TopologyPlanningInvariantError`` on top of it.
    That exception's own contract is that it is never raised because of authored
    input; a duplicate key is authored input.

    A ``$ref:KEY`` resolves against ComponentPlan symbols; a literal id resolves
    against live component facts; a platform ref (runtime, environment,
    external resource) resolves against the snapshot. An object that resolves to
    nothing simply does not appear here — the missing-reference finding is the
    collector's job, and returning a half-resolved row would make downstream
    bucket rules depend on interpreting a sentinel.
    """
    symbols = dict(prepared.symbols)
    component_ids = dict(prepared.components)
    environment_ids = set(prepared.environment_ids)
    runtime_ids = set(prepared.runtime_ids)

    blocked = set(blocked_object_indexes)
    resolved: List[ResolvedTopologyReferenceV1] = []
    seen_keys: set = set()
    for object_index, obj in enumerate(spec.objects):
        if object_index in blocked or obj.key in seen_keys:
            continue
        seen_keys.add(obj.key)
        if obj.kind in _COMPONENT_BACKED:
            ref = obj.component_ref  # type: ignore[union-attr]
            # A reference resolves only when the backing component is the RIGHT
            # TYPE. Resolving on the id alone would put a Document Cache into
            # ``resolved_references`` as a process, and the type mismatch the
            # collector reports would then contradict the plan's own resolution
            # table.
            expected = _COMPONENT_BACKED[obj.kind]
            if ref.startswith(_REF_TOKEN_PREFIX):
                if symbols.get(ref[len(_REF_TOKEN_PREFIX) :]) == expected:
                    resolved.append(
                        ResolvedTopologyReferenceV1(
                            object_key=obj.key,
                            object_kind=obj.kind,
                            resolution="component_plan_symbol",
                        )
                    )
            elif component_ids.get(ref) == expected:
                resolved.append(
                    ResolvedTopologyReferenceV1(
                        object_key=obj.key,
                        object_kind=obj.kind,
                        resolution="existing_component",
                    )
                )
        elif obj.kind == "environment":
            if obj.environment_ref in environment_ids:  # type: ignore[union-attr]
                resolved.append(
                    ResolvedTopologyReferenceV1(
                        object_key=obj.key,
                        object_kind=obj.kind,
                        resolution="platform_resource",
                    )
                )
        elif obj.kind == "runtime":
            if obj.runtime_ref in runtime_ids:  # type: ignore[union-attr]
                resolved.append(
                    ResolvedTopologyReferenceV1(
                        object_key=obj.key,
                        object_kind=obj.kind,
                        resolution="platform_resource",
                    )
                )
    return tuple(sorted(resolved, key=lambda r: (r.object_kind, r.object_key)))


def collect_reference_findings(
    spec: SystemTopologySpecV1,
    prepared: PreparedTopologyContextV1,
) -> Tuple[TopologyDiagnosticV1, ...]:
    """Every unresolved or wrongly-typed reference, accumulated and ordered."""
    from ...models.system_topology import TOPOLOGY_RELATION_ROLES

    findings: List[TopologyDiagnosticV1] = []
    objects_by_key: Dict[str, str] = {}
    for obj in spec.objects:
        # First declaration wins; a duplicate key is already a schema error, and
        # letting the later one shadow it would report a second, confusing
        # type mismatch on top of the duplicate.
        objects_by_key.setdefault(obj.key, obj.kind)

    symbols = dict(prepared.symbols)
    environment_ids = set(prepared.environment_ids)
    runtime_ids = set(prepared.runtime_ids)
    # Absence authority is qualified to the AUTHORED target profile, not merely
    # to an internally-coherent context. A context and snapshot that agree with
    # EACH OTHER but not with the spec still described a different account, and
    # its empty listing was allowed to prove that the spec's components do not
    # exist. Agreement among the wrong sources is not evidence.
    same_account = prepared.context.profile == spec.profile_ref and (
        prepared.context.snapshot is None
        or prepared.context.snapshot.profile == spec.profile_ref
    )
    complete = set(prepared.complete_component_types) if same_account else set()
    # The TYPE comparison answers to the same predicate, which it did not.
    # "A wrong type is conclusive from the fact alone" holds WITHIN an account;
    # across accounts the id names a different thing entirely, and this module's
    # own context docstring is the refutation — two profiles legitimately hold
    # the same component ids for different things. So a coherent capture of
    # omega, which may not confirm alpha's reference, witness its absence, or
    # supply its classification, was still allowed to REFUTE its type: a
    # ``TOPOLOGY_REFERENCE_TYPE_MISMATCH`` published about an account nobody
    # read. Emptying the index is the whole fix — absence already keys on
    # ``complete``, which is empty here for the same reason.
    component_ids = dict(prepared.components) if same_account else {}

    def _object_finding(code, index, field, kind):
        findings.append(
            topology_finding(
                code,
                severity="error",
                phase="reference",
                path=f"/objects/{index}/{field}",
                subject=kind,
            )
        )

    # Objects: does the thing this object claims to name exist in context, and
    # is it the right KIND of thing?
    #
    # Absence and mismatch are asymmetric. A wrong TYPE is conclusive from the
    # fact alone — we read the component and it is a Document Cache. Absence is
    # only conclusive when the listing that would have contained it was both
    # observed and complete: a literal id missing from a 100-of-186 page is not
    # evidence that the component does not exist, and reporting it as not-found
    # would contradict the pagination provenance this contract records precisely
    # so absence is not over-read.
    #
    # The whole loop is skipped when the CONTEXT names another account. Every
    # judgement in it reads the context, so a foreign one does not merely fail
    # to confirm — it changes the answer: an omega context carrying a symbol
    # named ``ka`` silently REMOVED alpha's ``TOPOLOGY_REFERENCE_NOT_FOUND``,
    # so the same mismatch produced a different report depending on what the
    # wrong account happened to contain. Judging nothing is the only consistent
    # reading, and it is the same rule as ``complete`` and ``component_ids``
    # above, applied to the one consumer that still read through it. Reporting
    # everything as not-found instead would over-claim absence from evidence
    # that was never about this account.
    #
    # Gated on the CONTEXT alone, not on ``same_account``. ``same_account`` is a
    # conjunction — context AND snapshot envelope — which is the right predicate
    # for everything snapshot-derived (``complete``, ``component_ids``, and the
    # environment/runtime branches, each of which keeps it). It is the wrong one
    # here, because the ``$ref`` branch reads ``prepared.symbols``: a
    # ComponentPlan symbol table that arrives on the CONTEXT and is qualified by
    # the context's profile. Using the conjunction silenced this loop whenever
    # merely the SNAPSHOT was foreign, deleting a real type mismatch from a plan
    # whose context matched the spec exactly — and the invariant checker
    # certified it, because it re-derives suppression from the very blocker that
    # had gone missing.
    same_context = prepared.context.profile == spec.profile_ref
    for index, obj in enumerate(spec.objects if same_context else ()):
        if obj.kind in _COMPONENT_BACKED:
            ref = obj.component_ref  # type: ignore[union-attr]
            expected = _COMPONENT_BACKED[obj.kind]
            if ref.startswith(_REF_TOKEN_PREFIX):
                key = ref[len(_REF_TOKEN_PREFIX) :]
                actual = symbols.get(key)
                if actual is None:
                    # UNCONDITIONAL. A ComponentPlan symbol table is
                    # authoritative and complete by construction — it IS the
                    # plan, not a page of it — and that holds when it is empty
                    # too: a ``$ref:`` naming nothing resolves to nothing.
                    #
                    # Guarding this on ``if symbols`` made the verdict depend on
                    # whether some UNRELATED symbol happened to be present: the
                    # same document flipped from "no blockers, binding
                    # plannable" to "blocked, binding withdrawn" when one was
                    # added. It also left the endpoint *unjudged* rather than
                    # blocked, which defeats the endpoint-withdrawal rule.
                    _object_finding(
                        TOPOLOGY_REFERENCE_NOT_FOUND, index, "component_ref", obj.kind
                    )
                elif actual != expected:
                    _object_finding(
                        TOPOLOGY_REFERENCE_TYPE_MISMATCH, index, "component_ref", obj.kind
                    )
            else:
                actual = component_ids.get(ref)
                if actual is None:
                    if expected in complete:
                        _object_finding(
                            TOPOLOGY_REFERENCE_NOT_FOUND, index, "component_ref", obj.kind
                        )
                elif actual != expected:
                    _object_finding(
                        TOPOLOGY_REFERENCE_TYPE_MISMATCH, index, "component_ref", obj.kind
                    )
        elif obj.kind == "environment" and (
            prepared.environment_inventory_observed and same_account
        ):
            # Guarded on the SNAPSHOT existing, not on the id set being
            # non-empty. ``list_environments`` returns the whole set — it is not
            # paged, unlike component queries — so once discovery ran, an empty
            # result genuinely means the account has no environments and an
            # authored reference to one is unresolvable. Guarding on
            # non-emptiness let an empty inventory wave every environment
            # through, and its deployment binding with it.
            if obj.environment_ref not in environment_ids:  # type: ignore[union-attr]
                _object_finding(
                    TOPOLOGY_REFERENCE_NOT_FOUND, index, "environment_ref", obj.kind
                )
        elif obj.kind == "runtime" and (
            prepared.runtime_inventory_complete and same_account
        ):
            # Only an authoritative inventory can witness absence. Discovery
            # derives runtimes from SCHEDULE rows, so it sees only runtimes that
            # already have one — treating that as an inventory would report
            # every unscheduled runtime as not-found and block the primary use
            # case, binding the first schedule to a runtime.
            if obj.runtime_ref not in runtime_ids:  # type: ignore[union-attr]
                _object_finding(
                    TOPOLOGY_REFERENCE_NOT_FOUND, index, "runtime_ref", obj.kind
                )

    # Relations: does each role name a declared object, of the accepted kind?
    for index, rel in enumerate(spec.relations):
        for role in TOPOLOGY_RELATION_ROLES[rel.kind]:
            target_key = getattr(rel, role)
            expected = TOPOLOGY_ENDPOINT_MATRIX[(rel.kind, role)]
            actual = objects_by_key.get(target_key)
            if actual is None:
                findings.append(
                    topology_finding(
                        TOPOLOGY_REFERENCE_NOT_FOUND,
                        severity="error",
                        phase="reference",
                        path=f"/relations/{index}/{role}",
                        subject=expected,
                    )
                )
            elif actual != expected:
                findings.append(
                    topology_finding(
                        TOPOLOGY_REFERENCE_TYPE_MISMATCH,
                        severity="error",
                        phase="reference",
                        path=f"/relations/{index}/{role}",
                        subject=expected,
                    )
                )
    return tuple(findings)


__all__: List[str] = [
    "TOPOLOGY_ENDPOINT_MATRIX",
    "collect_reference_findings",
    "resolve_topology_references",
]
