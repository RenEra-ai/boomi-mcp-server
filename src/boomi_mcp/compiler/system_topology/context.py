"""Typed, data-only planner inputs and the prepared index (#144).

Everything the planner is allowed to know arrives through
:class:`TopologyResolutionContextV1`. That is a deliberate seam: the planner is
a pure function of ``(spec, context)``, so "does this planner touch the network"
is answerable by looking at what a context can hold rather than by auditing call
graphs. No model here may contain ``Any``, a callable, an SDK client, an action
name, raw XML, a config bag, or a credential — and a test walks the annotations
to prove it.

Profile qualification
---------------------
Every fact carries the profile it was read from, and the planner rejects a fact
whose profile differs from the spec's. Live data is never portable authority:
two profiles legitimately hold the same component ids for different things, and
a fact that has forgotten where it came from is how a cross-account claim gets
made by accident.
"""

from __future__ import annotations

from typing import FrozenSet, List, Literal, Optional, Tuple

from ...models.integration_models import IntegrationSpecV1
from .contracts import _TopologyPlanningModel


class ComponentPlanSymbolV1(_TopologyPlanningModel):
    """One ComponentPlan symbol, projected down to what topology may see.

    ``materialization_dependencies`` is carried but NEVER used for topology
    ordering — it exists so a prerequisite can be reported faithfully, and the
    graph-namespace test proves changing it leaves the runtime order
    byte-identical.
    """

    component_key: str
    component_type: str
    materialization_dependencies: Tuple[str, ...] = ()
    has_process_ir: bool = False


class ComponentFactV1(_TopologyPlanningModel):
    """An existing component observed live, reduced to identity and type."""

    profile: str
    component_id: str
    component_type: str


class EnvironmentFactV1(_TopologyPlanningModel):
    """An observed environment.

    ``classification`` is OPTIONAL and is set only when the source genuinely
    carried one. Defaulting a missing value to ``TEST`` would manufacture an
    observation: an author correctly declaring ``PROD`` would then be blocked by
    ``TOPOLOGY_ENVIRONMENT_MISMATCH`` against a value nobody ever read, which is
    the precise opposite of the environment collector's rule that only a
    genuine CONTRADICTION is a finding.
    """

    profile: str
    environment_id: str
    classification: Optional[Literal["TEST", "PROD"]] = None


class RuntimeFactV1(_TopologyPlanningModel):
    profile: str
    runtime_id: str


class ScheduleBindingFactV1(_TopologyPlanningModel):
    """An observed schedule binding.

    ``active`` and ``has_schedule_body`` are observations, not authored intent.
    Both were False for every live schedule captured, which is precisely why
    schedule content is guidance-only.
    """

    profile: str
    process_id: str
    runtime_id: str
    active: bool = False
    has_schedule_body: bool = False


class DeploymentFactV1(_TopologyPlanningModel):
    """An observed deployment record.

    ``active`` is recorded faithfully and carries no capability meaning either
    way. Active records DO exist in the live account; that neither grants nor
    withholds an apply path here, because listing a deployment and creating one
    are different capabilities and only the first was ever observed.
    """

    profile: str
    component_id: str
    environment_id: str
    active: bool = False


class ProcessCallEvidenceV1(_TopologyPlanningModel):
    """A TRUSTED witness that one process invokes another.

    ``witness`` says how it was established. ``live_fact`` is deliberately not a
    member: the dependency API cannot distinguish a call from a reference, so
    there is no live read that alone establishes this edge.
    """

    caller_component_ref: str
    callee_component_ref: str
    witness: Literal["process_ir", "component_xml"]


class ApiServiceRouteEvidenceV1(_TopologyPlanningModel):
    """A trusted witness that an API service routes to a listener process."""

    api_service_component_ref: str
    listener_component_ref: str
    witness: Literal["component_xml", "typed_builder"]


class SharedResourceUseEvidenceV1(_TopologyPlanningModel):
    """A trusted witness that a process uses a shared cache or property."""

    process_component_ref: str
    resource_component_ref: str
    resource_kind: Literal["document_cache", "process_property"]
    witness: Literal["process_ir", "component_xml"]


class DependencyCorroborationV1(_TopologyPlanningModel):
    """A flat component-reference row from the dependency API.

    Explicitly NOT a witness. The API returns a one-level mixed-type list with
    no edge kind, so this says "B appears in A's dependency list" and nothing
    about WHY. Kept because it is genuinely useful for cross-checking a witness,
    and typed separately so it can never be mistaken for one.
    """

    parent_component_ref: str
    child_component_ref: str
    child_component_type: str


class DiscoveryPageProvenanceV1(_TopologyPlanningModel):
    """Pagination truth for one live query.

    Recorded because the counts in this issue's own description were page-capped
    artifacts: a ``total_count`` of 100 alongside ``total_available`` of 186 is
    not "100 components". ``truncated`` makes the difference impossible to read
    past.

    ``observed`` separates "the query ran and found nothing" from "the query did
    not answer". Without it the two are the same row, and that collapse lands
    exactly where this contract is most exposed: all four queue and Event
    Streams gates cite an empty ``query_components`` result as their live
    evidence. A failed call recorded as a confident zero would turn an outage
    into a capability claim.
    """

    component_type: str
    returned_count: int
    total_available: Optional[int] = None
    has_more: bool = False
    observed: bool = True

    @property
    def truncated(self) -> bool:
        # Fail closed: an unobserved page is treated as truncated, because "we
        # do not know what is there" and "there is more there" have the same
        # consequence for a caller — absence from this snapshot is not evidence
        # of absence in the account.
        if not self.observed:
            return True
        if self.has_more:
            return True
        if self.total_available is None:
            return False
        return self.total_available > self.returned_count


class TopologyDiscoverySnapshotV1(_TopologyPlanningModel):
    """A sanitized, profile-qualified, timestamped read of live topology.

    ``captured_at``, ``source_revision`` and ``service_release`` are mandatory:
    the issue's own evidence notes that the live MCP service runs behind the
    checkout, so a capability claim that cannot say WHICH release it observed is
    not a claim anyone can check.
    """

    profile: str
    captured_at: str
    source_revision: str
    service_release: str
    components: Tuple[ComponentFactV1, ...] = ()
    environments: Tuple[EnvironmentFactV1, ...] = ()
    runtimes: Tuple[RuntimeFactV1, ...] = ()
    schedule_bindings: Tuple[ScheduleBindingFactV1, ...] = ()
    deployments: Tuple[DeploymentFactV1, ...] = ()
    pagination: Tuple[DiscoveryPageProvenanceV1, ...] = ()


class TopologyResolutionContextV1(_TopologyPlanningModel):
    """Everything the planner may know. Pure data, no behavior, no I/O."""

    profile: str
    component_plan_symbols: Tuple[ComponentPlanSymbolV1, ...] = ()
    snapshot: Optional[TopologyDiscoverySnapshotV1] = None
    process_call_evidence: Tuple[ProcessCallEvidenceV1, ...] = ()
    api_service_route_evidence: Tuple[ApiServiceRouteEvidenceV1, ...] = ()
    shared_resource_use_evidence: Tuple[SharedResourceUseEvidenceV1, ...] = ()
    dependency_corroboration: Tuple[DependencyCorroborationV1, ...] = ()


class PreparedTopologyContextV1(_TopologyPlanningModel):
    """Revalidated context plus ordered lookup indexes.

    Private on purpose, and the planner builds it itself rather than accepting
    one. Taking a caller-supplied prepared context would let a caller hand over
    indexes that disagree with the facts they were supposedly built from — and
    the whole value of the revalidation step is that it is not skippable.
    """

    context: TopologyResolutionContextV1
    symbol_keys: Tuple[str, ...] = ()
    component_ids: Tuple[str, ...] = ()
    environment_ids: Tuple[str, ...] = ()
    runtime_ids: Tuple[str, ...] = ()


def project_component_plan_symbols(
    spec: IntegrationSpecV1,
    *,
    process_ir_keys: FrozenSet[str] = frozenset(),
) -> Tuple[ComponentPlanSymbolV1, ...]:
    """Project an ``IntegrationSpecV1`` down to topology-visible symbols.

    Reads ONLY ``key``, ``type`` and ``depends_on``. It does not touch
    ``config`` — which is why ``has_process_ir`` is a caller-supplied
    ``process_ir_keys`` set rather than something derived here.

    That is not squeamishness. ``IntegrationComponentSpec`` has no
    "has a ProcessIR root" field; the only in-tree signal is
    ``config.process_kind``, and ``config`` is exactly the open, caller-authored
    bag topology is forbidden to read. Deriving the flag would mean reaching into
    it, so the fact is passed in by whoever legitimately knows.
    """
    return tuple(
        ComponentPlanSymbolV1(
            component_key=component.key,
            component_type=component.type,
            materialization_dependencies=tuple(component.depends_on),
            has_process_ir=component.key in process_ir_keys,
        )
        for component in spec.components
    )


def prepare_topology_context(
    context: TopologyResolutionContextV1,
) -> PreparedTopologyContextV1:
    """Revalidate the context and build ordered indexes.

    The round-trip through ``model_dump`` / ``model_validate`` is not paranoia
    about pydantic — it is a guard against a context assembled by
    ``model_construct`` or mutated through a frozen model's ``__dict__``, both of
    which skip validators entirely. A planner that trusts unvalidated input has
    no basis for any of its guarantees.

    Indexes are TUPLES, not dicts: the plan's determinism claim depends on every
    derived collection having an explicit total order, and a dict's order is an
    insertion accident.
    """
    revalidated = TopologyResolutionContextV1.model_validate(
        context.model_dump(mode="python")
    )
    snapshot = revalidated.snapshot
    return PreparedTopologyContextV1(
        context=revalidated,
        symbol_keys=tuple(
            sorted(s.component_key for s in revalidated.component_plan_symbols)
        ),
        component_ids=tuple(sorted(c.component_id for c in snapshot.components))
        if snapshot
        else (),
        environment_ids=tuple(
            sorted(e.environment_id for e in snapshot.environments)
        )
        if snapshot
        else (),
        runtime_ids=tuple(sorted(r.runtime_id for r in snapshot.runtimes))
        if snapshot
        else (),
    )


__all__: List[str] = [
    "ApiServiceRouteEvidenceV1",
    "ComponentFactV1",
    "ComponentPlanSymbolV1",
    "DependencyCorroborationV1",
    "DeploymentFactV1",
    "DiscoveryPageProvenanceV1",
    "EnvironmentFactV1",
    "PreparedTopologyContextV1",
    "ProcessCallEvidenceV1",
    "RuntimeFactV1",
    "ScheduleBindingFactV1",
    "SharedResourceUseEvidenceV1",
    "TopologyDiscoverySnapshotV1",
    "TopologyResolutionContextV1",
    "prepare_topology_context",
    "project_component_plan_symbols",
]
