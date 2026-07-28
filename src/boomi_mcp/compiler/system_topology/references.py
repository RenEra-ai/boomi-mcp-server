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

#: Object kinds whose identity is a component reference (vs a platform resource
#: ref, vs nothing at all).
_COMPONENT_BACKED = frozenset(
    {"process", "api_service", "document_cache", "process_property"}
)


def resolve_topology_references(
    spec: SystemTopologySpecV1,
    prepared: PreparedTopologyContextV1,
) -> Tuple[ResolvedTopologyReferenceV1, ...]:
    """Resolve every OBJECT to what backs it, in a stable order.

    A ``$ref:KEY`` resolves against ComponentPlan symbols; a literal id resolves
    against live component facts; a platform ref (runtime, environment,
    external resource) resolves against the snapshot. An object that resolves to
    nothing simply does not appear here — the missing-reference finding is the
    collector's job, and returning a half-resolved row would make downstream
    bucket rules depend on interpreting a sentinel.
    """
    symbols = {
        symbol.component_key: symbol
        for symbol in prepared.context.component_plan_symbols
    }
    component_ids = set(prepared.component_ids)
    environment_ids = set(prepared.environment_ids)
    runtime_ids = set(prepared.runtime_ids)

    resolved: List[ResolvedTopologyReferenceV1] = []
    for obj in spec.objects:
        if obj.kind in _COMPONENT_BACKED:
            ref = obj.component_ref  # type: ignore[union-attr]
            if ref.startswith(_REF_TOKEN_PREFIX):
                if ref[len(_REF_TOKEN_PREFIX) :] in symbols:
                    resolved.append(
                        ResolvedTopologyReferenceV1(
                            object_key=obj.key,
                            object_kind=obj.kind,
                            resolution="component_plan_symbol",
                        )
                    )
            elif ref in component_ids:
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

    symbols = {
        symbol.component_key for symbol in prepared.context.component_plan_symbols
    }
    component_ids = set(prepared.component_ids)
    environment_ids = set(prepared.environment_ids)
    runtime_ids = set(prepared.runtime_ids)

    # Objects: does the thing this object claims to name exist in context?
    # Only checked when a snapshot or symbol table is present — with neither,
    # everything would be "not found", which is noise rather than a finding.
    have_context = bool(symbols or component_ids or environment_ids or runtime_ids)
    if have_context:
        for index, obj in enumerate(spec.objects):
            if obj.kind in _COMPONENT_BACKED:
                ref = obj.component_ref  # type: ignore[union-attr]
                known = (
                    ref[len(_REF_TOKEN_PREFIX) :] in symbols
                    if ref.startswith(_REF_TOKEN_PREFIX)
                    else ref in component_ids
                )
                if not known:
                    findings.append(
                        topology_finding(
                            TOPOLOGY_REFERENCE_NOT_FOUND,
                            severity="error",
                            phase="reference",
                            path=f"/objects/{index}/component_ref",
                            subject=obj.kind,
                        )
                    )
            elif obj.kind == "environment" and environment_ids:
                if obj.environment_ref not in environment_ids:  # type: ignore[union-attr]
                    findings.append(
                        topology_finding(
                            TOPOLOGY_REFERENCE_NOT_FOUND,
                            severity="error",
                            phase="reference",
                            path=f"/objects/{index}/environment_ref",
                            subject=obj.kind,
                        )
                    )
            elif obj.kind == "runtime" and runtime_ids:
                if obj.runtime_ref not in runtime_ids:  # type: ignore[union-attr]
                    findings.append(
                        topology_finding(
                            TOPOLOGY_REFERENCE_NOT_FOUND,
                            severity="error",
                            phase="reference",
                            path=f"/objects/{index}/runtime_ref",
                            subject=obj.kind,
                        )
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
