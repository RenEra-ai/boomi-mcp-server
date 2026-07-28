"""The topology validation and planning pipeline (#144).

Both entry points are PURE: no network, no filesystem, no clock, no environment
access, no SDK, no mutation. They are functions of ``(spec, context)`` and
nothing else, which is what makes the no-mutation claim checkable by running
them under an audit hook rather than by auditing call graphs.

Phase order is fixed and normative:

``model → capability → reference → relation → lifecycle → environment →
dependency → plan_invariant``

Collectors run in that order and their findings ACCUMULATE — a caller fixes
everything in one pass instead of discovering defects one round-trip at a time.
Ordering matters for reading, not for short-circuiting: only the graph phase
depends on an earlier one, because a cycle report over unresolved references
would name relations that do not resolve to anything.
"""

from __future__ import annotations

import hashlib
from typing import List, Tuple

from ...errors import TOPOLOGY_APPLY_NOT_SUPPORTED
from ...models.system_topology import (
    SystemTopologySpecV1,
    canonical_system_topology_json,
)
from .capabilities import build_capability_report, collect_capability_findings
from .context import (
    PreparedTopologyContextV1,
    TopologyResolutionContextV1,
    prepare_topology_context,
)
from .contracts import (
    ComponentPlanPrerequisiteV1,
    SystemTopologyPlanV1,
    TopologyDiagnosticV1,
    TopologyRuntimeOrderV1,
    TopologyValidationReportV1,
    build_topology_report,
)
from .dependencies import collect_dependency_findings, derive_runtime_process_order
from .findings import topology_finding
from .invariants import check_topology_plan_invariants
from .references import collect_reference_findings, resolve_topology_references
from .relations import (
    collect_environment_findings,
    collect_lifecycle_findings,
    collect_relation_findings,
    derive_guidance,
    derive_unresolved_decisions,
)

_REF_TOKEN_PREFIX = "$ref:"


def _collect(
    spec: SystemTopologySpecV1, prepared: PreparedTopologyContextV1
) -> Tuple[Tuple[TopologyDiagnosticV1, ...], Tuple]:
    """Run every collector in phase order and return findings plus planned relations."""
    findings: List[TopologyDiagnosticV1] = []
    findings.extend(collect_capability_findings(spec))

    reference_findings = collect_reference_findings(spec, prepared)
    findings.extend(reference_findings)

    findings.extend(collect_relation_findings(spec))

    lifecycle_findings, planned = collect_lifecycle_findings(spec, prepared)
    findings.extend(lifecycle_findings)

    findings.extend(collect_environment_findings(spec, prepared))

    # The cycle check runs only over a graph whose references resolved. Over an
    # unresolved graph it would report a cycle among relations that do not point
    # at anything — technically true of the authored text, and useless.
    if not reference_findings:
        findings.extend(collect_dependency_findings(spec))

    return tuple(findings), planned


def validate_system_topology(
    spec: SystemTopologySpecV1,
    context: TopologyResolutionContextV1,
) -> TopologyValidationReportV1:
    """Validate a topology against its context. Pure; reports, never raises."""
    prepared = prepare_topology_context(context)
    findings, _planned = _collect(spec, prepared)
    return build_topology_report(findings)


def plan_system_topology(
    spec: SystemTopologySpecV1,
    context: TopologyResolutionContextV1,
    requested_operation: str = "plan",
) -> SystemTopologyPlanV1:
    """Produce the deterministic topology plan.

    ``requested_operation`` other than ``"plan"`` is refused BEFORE the context
    is read at all. That ordering is deliberate and testable: a spy context that
    raises on any attribute access must still get a clean refusal, which proves
    the refusal is not merely the last check after everything else already ran.
    """
    if requested_operation != "plan":
        refusal = topology_finding(
            TOPOLOGY_APPLY_NOT_SUPPORTED,
            severity="error",
            phase="model",
            # The request ENVELOPE, not the authored document — ``operation`` is
            # a function argument, and there is no spec field it could point at.
            # ADR-001 §7 already makes this accommodation for the M12_* family,
            # where the audited scope stands in for the authored JSON path.
            path="/operation",
            subject="topology_apply",
        )
        report = build_topology_report((refusal,))
        empty_spec_bytes = canonical_system_topology_json(spec).encode("utf-8")
        return SystemTopologyPlanV1(
            spec_sha256=hashlib.sha256(empty_spec_bytes).hexdigest(),
            capability_report=build_capability_report(spec),
            validation=report,
            blockers=report.errors,
        )

    prepared = prepare_topology_context(context)
    findings, planned = _collect(spec, prepared)
    report = build_topology_report(findings)

    resolved = resolve_topology_references(spec, prepared)

    # Only a $ref-backed object whose symbol is present becomes a prerequisite.
    # A literal id names something that already exists — reporting it as
    # something to build would be an instruction to rebuild a live component.
    symbols = {
        symbol.component_key: symbol
        for symbol in prepared.context.component_plan_symbols
    }
    prerequisites: List[ComponentPlanPrerequisiteV1] = []
    seen_keys: set = set()
    for obj in spec.objects:
        ref = getattr(obj, "component_ref", "")
        if not ref.startswith(_REF_TOKEN_PREFIX):
            continue
        key = ref[len(_REF_TOKEN_PREFIX) :]
        symbol = symbols.get(key)
        if symbol is None or key in seen_keys:
            continue
        seen_keys.add(key)
        prerequisites.append(
            ComponentPlanPrerequisiteV1(
                component_key=symbol.component_key,
                component_type=symbol.component_type,
            )
        )

    # A blocked plan reports no runtime order. An order derived from a graph
    # whose relations did not all survive validation would look authoritative
    # while describing something the caller is not allowed to act on.
    order = (
        derive_runtime_process_order(spec)
        if not report.errors
        else TopologyRuntimeOrderV1(order=())
    )

    # Only relations that survived every phase may be planned. ``planned`` is
    # what the witness collector accepted; anything that later drew an error is
    # withdrawn here rather than being trusted from an earlier phase.
    #
    # Withdrawal follows the OBJECT graph too, not just the relation's own path.
    # An endpoint's failure is reported under ``/objects/N`` — a different path
    # entirely — so a filter that looked only at ``/relations/N`` left a
    # structural binding in ``planning_only_relations`` while it pointed at an
    # object that does not resolve. A plan is not allowed to present a relation
    # as plannable when one of its ends is blocked.
    blocked_paths = {d.path for d in report.errors}
    blocked_object_keys = {
        spec.objects[index].key
        for index in _blocked_object_indexes(blocked_paths)
        if 0 <= index < len(spec.objects)
    }
    surviving = tuple(
        relation
        for index, relation in _indexed_relations(spec, planned)
        if f"/relations/{index}" not in blocked_paths
        and not any(
            path.startswith(f"/relations/{index}/") for path in blocked_paths
        )
        and not _touches_blocked_object(spec, index, blocked_object_keys)
    )

    plan = SystemTopologyPlanV1(
        spec_sha256=hashlib.sha256(
            canonical_system_topology_json(spec).encode("utf-8")
        ).hexdigest(),
        capability_report=build_capability_report(spec),
        validation=report,
        resolved_references=resolved,
        executable_component_prerequisites=tuple(
            sorted(prerequisites, key=lambda p: p.component_key)
        ),
        planning_only_relations=surviving,
        runtime_process_order=order,
        guidance=derive_guidance(spec, prepared),
        blockers=report.errors,
        unresolved_decisions=derive_unresolved_decisions(spec, prepared),
    )
    check_topology_plan_invariants(plan, prepared)
    return plan


def _blocked_object_indexes(blocked_paths) -> Tuple[int, ...]:
    """Authored object indexes named by any error path under ``/objects/``."""
    indexes = []
    for path in blocked_paths:
        parts = path.split("/")
        if len(parts) >= 3 and parts[1] == "objects":
            try:
                indexes.append(int(parts[2]))
            except ValueError:
                continue
    return tuple(sorted(set(indexes)))


def _touches_blocked_object(
    spec: SystemTopologySpecV1, index: int, blocked_keys
) -> bool:
    if not blocked_keys or index < 0 or index >= len(spec.relations):
        return False
    from ...models.system_topology import TOPOLOGY_RELATION_ROLES

    relation = spec.relations[index]
    return any(
        getattr(relation, role) in blocked_keys
        for role in TOPOLOGY_RELATION_ROLES[relation.kind]
    )


def _indexed_relations(spec: SystemTopologySpecV1, planned) -> Tuple:
    """Pair each planned relation with its authored index.

    Matching on the relation KEY rather than on position: ``planned`` is sorted
    by (kind, key) for determinism, so its order no longer corresponds to the
    authored one.
    """
    index_by_key = {rel.key: index for index, rel in enumerate(spec.relations)}
    return tuple(
        (index_by_key.get(relation.relation_key, -1), relation)
        for relation in planned
    )


__all__: List[str] = ["plan_system_topology", "validate_system_topology"]
