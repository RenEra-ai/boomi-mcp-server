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

#: Component-type aliases an ``IntegrationSpecV1`` may legitimately carry, for
#: the four kinds topology is component-backed by. A COPY of the relevant subset
#: of ``categories.integration_builder._TYPE_ALIASES`` — the compiler must not
#: depend on the tool layer (ADR-001 §6), the same reason the secret list and
#: the DOCTYPE screen are copied. A test pins equality with the builder's map so
#: the two cannot drift.
#:
#: Without normalization a spec authored with ``type="api_service"`` — which the
#: builder accepts and resolves to ``webservice`` — would be reported as a
#: TOPOLOGY_REFERENCE_TYPE_MISMATCH against its own API-service object. Latent
#: today because ``build_integration`` normalizes before planning, but the
#: projection takes a RAW spec and must not assume a caller went through it.
_COMPONENT_TYPE_ALIASES = {
    "api.service": "webservice",
    "api_service": "webservice",
    "webservice": "webservice",
    "process": "process",
    "documentcache": "documentcache",
    "processproperty": "processproperty",
}


def _normalize_component_type(value: str) -> str:
    """Case-fold and de-alias, mirroring the builder's own normalizer."""
    key = (value or "").strip().lower()
    return _COMPONENT_TYPE_ALIASES.get(key, key)


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

    #: Is ``runtimes`` an INVENTORY, or just the runtimes that happened to turn
    #: up? Discovery derives runtimes from schedule rows, so it only ever sees
    #: runtimes that already have a schedule — which makes absence from the list
    #: meaningless, and treating it as authoritative would report every
    #: unscheduled runtime as not-found and block the primary use case: binding
    #: the FIRST schedule to a runtime. Nothing sets this True today; the read
    #: port has no runtime-list method, and inventing one would be a capability
    #: claim with no evidence behind it.
    runtime_inventory_complete: bool = False


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
    #: ``(component_key, component_type)`` — the TYPE is carried, not discarded.
    #: An id-only index cannot tell a process object pointing at a Document
    #: Cache component from a correct reference, so it silently resolves the
    #: wrong component instead of raising TOPOLOGY_REFERENCE_TYPE_MISMATCH.
    symbols: Tuple[Tuple[str, str], ...] = ()
    #: ``(component_id, component_type)``, same reason.
    components: Tuple[Tuple[str, str], ...] = ()
    environment_ids: Tuple[str, ...] = ()
    runtime_ids: Tuple[str, ...] = ()
    #: Component types whose live listing was OBSERVED and NOT truncated.
    #: Absence is conclusive only for these: a literal id missing from a
    #: 100-of-186 page is not evidence that the component does not exist, and
    #: reporting it as not-found contradicts the pagination provenance this
    #: contract records precisely so absence is not over-read.
    complete_component_types: Tuple[str, ...] = ()
    runtime_inventory_complete: bool = False
    #: How many facts were discarded for naming a different profile. A COUNT,
    #: never the values — the whole point is that they are foreign.
    foreign_profile_fact_count: int = 0


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
            component_type=_normalize_component_type(component.type),
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

    # Normalized HERE, not only in ``project_component_plan_symbols``.
    # ``TopologyResolutionContextV1`` is a public input: a caller can assemble
    # ``component_plan_symbols`` directly and never touch the projection helper,
    # and a builder-legal alias such as ``api_service`` would then be compared
    # verbatim against ``webservice`` and reported as a type mismatch against
    # its own object. ``prepare_topology_context`` is the one gate every path
    # passes through, so the rule belongs here.
    symbols = tuple(
        sorted(
            (s.component_key, _normalize_component_type(s.component_type))
            for s in revalidated.component_plan_symbols
        )
    )
    if snapshot is None:
        return PreparedTopologyContextV1(context=revalidated, symbols=symbols)

    # Profile isolation is enforced per FACT, not just on the snapshot envelope.
    # A snapshot may carry the right profile while an individual fact inside it
    # names another account — and an index built without checking would let a
    # foreign component id resolve with no mismatch reported at all. Discarded
    # rather than merely flagged: a fact from another account is not weaker
    # evidence about this one, it is evidence about a different system.
    profile = snapshot.profile
    kept_components = [c for c in snapshot.components if c.profile == profile]
    kept_environments = [e for e in snapshot.environments if e.profile == profile]
    kept_runtimes = [r for r in snapshot.runtimes if r.profile == profile]
    foreign = (
        len(snapshot.components)
        - len(kept_components)
        + len(snapshot.environments)
        - len(kept_environments)
        + len(snapshot.runtimes)
        - len(kept_runtimes)
        + sum(1 for s in snapshot.schedule_bindings if s.profile != profile)
        + sum(1 for d in snapshot.deployments if d.profile != profile)
    )

    # The THIRD type-bearing field, normalized like the other two. Its consumer
    # compares these against the same canonical vocabulary the expected object
    # type comes from, so a page recorded as ``api_service`` — or merely
    # ``Webservice`` — never matched ``webservice``, absence was never
    # conclusive, and a ghost component whose absence IS fully witnessed by a
    # complete observed listing went unjudged rather than blocked. That is the
    # failure mode the unconditional ``$ref`` rule exists to prevent, reached by
    # a different route.
    complete = tuple(
        sorted(
            {
                _normalize_component_type(page.component_type)
                for page in snapshot.pagination
                if page.observed and not page.truncated
            }
        )
    )

    return PreparedTopologyContextV1(
        context=revalidated,
        symbols=symbols,
        components=tuple(
            sorted(
                (c.component_id, _normalize_component_type(c.component_type))
                for c in kept_components
            )
        ),
        environment_ids=tuple(sorted(e.environment_id for e in kept_environments)),
        runtime_ids=tuple(sorted(r.runtime_id for r in kept_runtimes)),
        complete_component_types=complete,
        runtime_inventory_complete=snapshot.runtime_inventory_complete,
        foreign_profile_fact_count=foreign,
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
