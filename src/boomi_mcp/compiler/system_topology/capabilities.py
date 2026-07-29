"""The topology capability registry: what may be planned, and on what evidence (#144).

This module is the issue's central claim made checkable. Every object kind,
every relation kind, and every deliberately-excluded feature carries a state and
three evidence legs, and the registry is validated against the two discriminated
unions AT IMPORT — in both directions — so a kind cannot be added to the schema
without someone deciding what evidence supports it.

The five states
---------------
* ``emittable`` — an existing typed ComponentPlan materializer can produce the
  component prerequisite, after separate build authorization. The topology
  planner REPORTS that prerequisite and never calls the materializer.
* ``plannable-only`` — source/docs plus a trusted witness support reference,
  lifecycle and ordering validation, but there is no apply path at all.
* ``guidance-only`` — evidence supports static advice or an unresolved decision,
  nothing more. The subject cannot enter an executable or planning bucket.
* ``gated-no-evidence`` — the intent is worth representing, but required
  evidence is missing. Always yields a blocker.
* ``unsupported`` — policy or the platform rejects it. Either absent from the
  schema entirely, or a named diagnostic.

Absence is denial: a subject with no registration is not "probably fine", it is
a coverage failure that raises at import.

Live evidence as of the #144 capture (see docs/architecture/SYSTEM_TOPOLOGY_V1.md)
----------------------------------------------------------------------------------
Four captures bound what may be claimed here, and each is why a specific row
below is weaker than the issue's prose suggested:

1. **The capture observed no ``webservice`` components.** The API-service
   object stays ``emittable`` (the typed builder and its analyzer fixtures are
   real), but its live leg is ``unavailable`` — the three-way rule's live leg is
   genuinely missing, not satisfied-by-assumption.
2. **``analyze_component dependencies`` is a flat, one-level, MIXED-type
   reference list with no edge kind.** It cannot witness a ProcessCall; it
   corroborates one. Promoting it would let "process A references profile B"
   read as "process A calls process B".
3. **Every schedule body observed carries an empty ``schedules: []`` array.**
   Cron/interval CONTENT has no evidence to model from, so it is guidance-only
   and absent from the schema. Retry and active state are observed and are
   recorded as snapshot observations — reading a value is not evidence that this
   contract may set one.
4. **Deployment reads establish that a (component, environment) record exists.
   They establish nothing about whether this planner may CREATE one.** Reading a
   deployment and being able to make one are different capabilities, and only
   the first was ever observed. (An earlier draft justified this with "every
   live deployment record is inactive" — measured on one profile, and false on
   the other, which has an active record. The verdict was right; the reason was
   not, and a reason that can be falsified by looking at a second account is not
   the reason to publish.)
"""

from __future__ import annotations

from types import MappingProxyType
from typing import Dict, FrozenSet, Iterable, List, Mapping, Tuple

from ...errors import TOPOLOGY_CAPABILITY_GATED
from ...models.system_topology import (
    TOPOLOGY_OBJECT_KINDS,
    TOPOLOGY_RELATION_KINDS,
    SystemTopologySpecV1,
)
from .contracts import (
    CapabilityEvidenceLegV1,
    TopologyCapabilityEntryV1,
    TopologyCapabilityReportV1,
    TopologyDiagnosticV1,
)
from .findings import topology_finding

#: Bumped when a state or evidence leg changes. A plan records it so a stored
#: plan can be told apart from one produced under different evidence.
SYSTEM_TOPOLOGY_CAPABILITY_REVISION = "1"


class CapabilityRegistrationV1(TopologyCapabilityEntryV1):
    """A static registry row. Same shape as a report entry, minus spec presence."""


def _leg(leg: str, status: str, reference: str) -> CapabilityEvidenceLegV1:
    return CapabilityEvidenceLegV1(
        leg=leg,  # type: ignore[arg-type]
        status=status,  # type: ignore[arg-type]
        reference=reference,
    )


def _row(
    subject: str,
    subject_kind: str,
    state: str,
    *,
    source: Tuple[str, str],
    documentation: Tuple[str, str],
    live: Tuple[str, str],
) -> CapabilityRegistrationV1:
    return CapabilityRegistrationV1(
        subject=subject,
        subject_kind=subject_kind,  # type: ignore[arg-type]
        state=state,  # type: ignore[arg-type]
        source=_leg("source", source[0], source[1]),
        documentation=_leg("documentation", documentation[0], documentation[1]),
        live=_leg("live", live[0], live[1]),
    )


_ROWS: Tuple[CapabilityRegistrationV1, ...] = (
    # ---------------- objects ----------------
    _row(
        "process",
        "object",
        "emittable",
        source=("verified", "boomi_mcp.models.process_ir"),
        documentation=("verified", "adr-001#3"),
        live=("verified", "capture:query_components/process"),
    ),
    _row(
        # Emittable, but with the live leg honestly empty: the typed builder and
        # its analyzer fixtures exist, and zero webservice components do.
        "api_service",
        "object",
        "emittable",
        source=("verified", "boomi_mcp.categories.integration_builder"),
        documentation=("verified", "adr-001#3"),
        live=("unavailable", "capture:query_components/webservice-empty"),
    ),
    _row(
        "document_cache",
        "object",
        "emittable",
        source=("verified", "boomi_mcp.models.cache_property_models"),
        documentation=("verified", "adr-001#3"),
        live=("verified", "capture:query_components/documentcache"),
    ),
    _row(
        "process_property",
        "object",
        "emittable",
        source=("verified", "boomi_mcp.models.cache_property_models"),
        documentation=("verified", "adr-001#3"),
        live=("verified", "capture:query_components/processproperty"),
    ),
    _row(
        "runtime",
        "object",
        "plannable-only",
        source=("verified", "boomi_mcp.categories.schedules"),
        documentation=("verified", "adr-001#3"),
        live=("verified", "capture:manage_schedules/atom-id"),
    ),
    _row(
        "environment",
        "object",
        "plannable-only",
        source=("verified", "boomi_mcp.categories.environments"),
        documentation=("verified", "adr-001#3"),
        live=("verified", "capture:manage_environments/classification"),
    ),
    _row(
        # The binding is evidenced; the CONTENT is not. See schedule_content.
        "schedule",
        "object",
        "plannable-only",
        source=("verified", "boomi_mcp.categories.schedules"),
        documentation=("verified", "adr-001#3"),
        live=("corroborating_only", "capture:manage_schedules/empty-body"),
    ),
    _row(
        "deployment_unit",
        "object",
        "plannable-only",
        source=("verified", "boomi_mcp.categories.deployment"),
        documentation=("verified", "adr-001#3"),
        live=("corroborating_only", "capture:manage_deployment/records-read-only"),
    ),
    _row(
        "external_queue",
        "object",
        "gated-no-evidence",
        source=("corroborating_only", "boomi_mcp.categories.troubleshooting"),
        documentation=("unavailable", "adr-001#12"),
        live=("unavailable", "capture:query_components/queue-empty"),
    ),
    _row(
        "external_event_stream",
        "object",
        "gated-no-evidence",
        source=("not_captured", "adr-001#12"),
        documentation=("unavailable", "adr-001#12"),
        live=("unavailable", "capture:query_components/queue-empty"),
    ),
    # ---------------- relations ----------------
    _row(
        "process_call",
        "relation",
        "plannable-only",
        source=("verified", "boomi_mcp.models.process_ir"),
        documentation=("verified", "adr-001#3"),
        live=("corroborating_only", "capture:analyze_component/mixed-deps"),
    ),
    _row(
        "api_service_route",
        "relation",
        "plannable-only",
        source=("verified", "boomi_mcp.categories.components"),
        documentation=("verified", "adr-001#3"),
        live=("unavailable", "capture:query_components/webservice-empty"),
    ),
    _row(
        "document_cache_use",
        "relation",
        "plannable-only",
        source=("verified", "boomi_mcp.models.process_ir"),
        documentation=("verified", "adr-001#3"),
        live=("corroborating_only", "capture:analyze_component/mixed-deps"),
    ),
    _row(
        "process_property_use",
        "relation",
        "plannable-only",
        source=("verified", "boomi_mcp.models.process_ir"),
        documentation=("verified", "adr-001#3"),
        live=("corroborating_only", "capture:analyze_component/mixed-deps"),
    ),
    _row(
        "schedule_binding",
        "relation",
        "plannable-only",
        source=("verified", "boomi_mcp.categories.schedules"),
        documentation=("verified", "adr-001#3"),
        live=("verified", "capture:manage_schedules/process-atom-pair"),
    ),
    _row(
        "deployment_binding",
        "relation",
        "plannable-only",
        source=("verified", "boomi_mcp.categories.deployment"),
        documentation=("verified", "adr-001#3"),
        live=("corroborating_only", "capture:manage_deployment/records-read-only"),
    ),
    _row(
        "queue_reference",
        "relation",
        "gated-no-evidence",
        source=("corroborating_only", "boomi_mcp.categories.troubleshooting"),
        documentation=("unavailable", "adr-001#12"),
        live=("unavailable", "capture:query_components/queue-empty"),
    ),
    _row(
        "event_stream_reference",
        "relation",
        "gated-no-evidence",
        source=("not_captured", "adr-001#12"),
        documentation=("unavailable", "adr-001#12"),
        live=("unavailable", "capture:query_components/queue-empty"),
    ),
    # ---------------- feature rows ----------------
    # Published so that what is ABSENT from the schema is visible, and visibly
    # absent for a stated reason, rather than merely missing.
    _row(
        "schedule_content",
        "feature",
        "guidance-only",
        source=("verified", "boomi_mcp.categories.schedules"),
        documentation=("verified", "adr-001#3"),
        live=("unavailable", "capture:manage_schedules/empty-body"),
    ),
    _row(
        "account_capability_limits",
        "feature",
        "gated-no-evidence",
        source=("not_captured", "boomi_mcp.categories.account"),
        documentation=("not_captured", "adr-001#3"),
        live=("not_captured", "capture:none"),
    ),
    _row(
        "dependency_api_as_process_call_witness",
        "feature",
        "unsupported",
        source=("verified", "boomi_mcp.categories.components"),
        documentation=("verified", "adr-001#6"),
        live=("conflicting", "capture:analyze_component/mixed-deps"),
    ),
    _row(
        "listener_status_as_api_route_witness",
        "feature",
        "unsupported",
        source=("conflicting", "boomi_mcp.categories.listeners"),
        documentation=("conflicting", "adr-001#6"),
        live=("unavailable", "capture:query_components/webservice-empty"),
    ),
    _row(
        "schedule_environment_binding",
        "feature",
        "unsupported",
        source=("verified", "boomi_mcp.categories.schedules"),
        documentation=("verified", "adr-001#3"),
        live=("verified", "capture:manage_schedules/process-atom-pair"),
    ),
    _row(
        "topology_apply",
        "feature",
        "unsupported",
        source=("verified", "adr-001#3"),
        documentation=("verified", "adr-001#3"),
        live=("not_captured", "capture:none"),
    ),
    _row(
        "atomic_multi_process_deployment",
        "feature",
        "unsupported",
        source=("verified", "boomi_mcp.categories.deployment"),
        documentation=("verified", "adr-001#3"),
        live=("unavailable", "capture:manage_deployment/records-read-only"),
    ),
    _row(
        "queue_mutation",
        "feature",
        "unsupported",
        source=("verified", "adr-001#12"),
        documentation=("verified", "adr-001#12"),
        live=("unavailable", "capture:query_components/queue-empty"),
    ),
)


def _validate_coverage(
    object_kinds: Iterable[str],
    relation_kinds: Iterable[str],
    rows: Iterable[CapabilityRegistrationV1],
) -> None:
    """Assert registry membership matches the unions EXACTLY, both directions.

    A build defect, not a ``TOPOLOGY_*`` diagnostic: an unregistered kind is not
    an authored payload's fault, and reporting it as one would tell a caller to
    fix input that is perfectly correct.

    Exposed as a function rather than inlined at module scope purely so a test
    can drive it with a deliberately short set — a coverage check nobody can
    make fail is indistinguishable from one that does nothing.
    """
    collected = tuple(rows)
    seen: Dict[str, None] = {}
    for row in collected:
        if row.subject in seen:
            raise ValueError(
                f"topology capability registry: duplicate subject {row.subject!r}"
            )
        seen[row.subject] = None

    registered_objects = {r.subject for r in collected if r.subject_kind == "object"}
    registered_relations = {r.subject for r in collected if r.subject_kind == "relation"}

    for label, declared, registered in (
        ("object", set(object_kinds), registered_objects),
        ("relation", set(relation_kinds), registered_relations),
    ):
        if declared != registered:
            missing = sorted(declared - registered)
            extra = sorted(registered - declared)
            raise ValueError(
                f"topology capability registry: {label} coverage mismatch "
                f"(unregistered={missing}, unknown={extra})"
            )


_validate_coverage(TOPOLOGY_OBJECT_KINDS, TOPOLOGY_RELATION_KINDS, _ROWS)

SYSTEM_TOPOLOGY_CAPABILITIES: Mapping[str, CapabilityRegistrationV1] = MappingProxyType(
    {row.subject: row for row in _ROWS}
)

#: Subjects whose mere presence in a spec is a blocker.
GATED_SUBJECTS: FrozenSet[str] = frozenset(
    row.subject for row in _ROWS if row.state == "gated-no-evidence"
)


def capability_for(subject: str) -> CapabilityRegistrationV1:
    """The registration for a subject. Absence is a build defect, never a default."""
    try:
        return SYSTEM_TOPOLOGY_CAPABILITIES[subject]
    except KeyError:  # pragma: no cover — coverage is pinned at import
        raise ValueError(
            f"topology capability registry: no registration for {subject!r}"
        ) from None


def build_capability_report(spec: SystemTopologySpecV1) -> TopologyCapabilityReportV1:
    """The FULL report — every registered subject, present in the spec or not."""
    present = {obj.kind for obj in spec.objects} | {rel.kind for rel in spec.relations}
    entries = tuple(
        sorted(
            (
                TopologyCapabilityEntryV1(
                    subject=row.subject,
                    subject_kind=row.subject_kind,
                    state=row.state,
                    source=row.source,
                    documentation=row.documentation,
                    live=row.live,
                    present_in_spec=row.subject in present,
                )
                for row in _ROWS
            ),
            key=lambda entry: entry.sort_key(),
        )
    )
    return TopologyCapabilityReportV1(
        revision=SYSTEM_TOPOLOGY_CAPABILITY_REVISION, entries=entries
    )


def collect_capability_findings(
    spec: SystemTopologySpecV1,
) -> Tuple[TopologyDiagnosticV1, ...]:
    """KIND-level gating: a gated kind blocks by being present at all.

    Witness-level gating (a supported relation with no trusted evidence) belongs
    to the ``relation`` phase, not here. Splitting them matters: a queue object
    must block even when nothing references it, and a witness-less ProcessCall
    must block even though ``process_call`` is a perfectly supported kind.
    """
    findings: List[TopologyDiagnosticV1] = []
    for index, obj in enumerate(spec.objects):
        if obj.kind in GATED_SUBJECTS:
            findings.append(
                topology_finding(
                    TOPOLOGY_CAPABILITY_GATED,
                    severity="error",
                    phase="capability",
                    path=f"/objects/{index}",
                    subject=obj.kind,
                    provenance=(capability_for(obj.kind).live.reference,),
                )
            )
    for index, rel in enumerate(spec.relations):
        if rel.kind in GATED_SUBJECTS:
            findings.append(
                topology_finding(
                    TOPOLOGY_CAPABILITY_GATED,
                    severity="error",
                    phase="capability",
                    path=f"/relations/{index}",
                    subject=rel.kind,
                    provenance=(capability_for(rel.kind).live.reference,),
                )
            )
    return tuple(findings)


__all__: List[str] = [
    "GATED_SUBJECTS",
    "SYSTEM_TOPOLOGY_CAPABILITIES",
    "SYSTEM_TOPOLOGY_CAPABILITY_REVISION",
    "CapabilityRegistrationV1",
    "build_capability_report",
    "capability_for",
    "collect_capability_findings",
]
