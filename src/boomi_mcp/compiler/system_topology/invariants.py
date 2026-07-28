"""Post-assembly self-check on the produced plan (#144).

Every rule here is something the assembly code is already supposed to guarantee.
Re-checking is the point: these are the claims the issue's acceptance criteria
make, and a claim that only holds because the code currently happens to be
written a certain way is not a guarantee — it is a coincidence with good
intentions.

A failure here is a PLANNER DEFECT, not an authored-payload problem, so it
raises :class:`TopologyPlanningInvariantError` rather than producing a
``TOPOLOGY_*`` diagnostic. Blaming the caller for our bug is how someone ends up
rewriting a correct payload to route around it — the same reasoning that walls
``PROCESS_IR_COMPILE_*`` off from a ValidationReportV1.
"""

from __future__ import annotations

from typing import List, Tuple

from .capabilities import SYSTEM_TOPOLOGY_CAPABILITIES
from .context import PreparedTopologyContextV1
from .contracts import SystemTopologyPlanV1

#: Field names whose presence anywhere in a plan payload would mean the planner
#: leaked an executable or secret-bearing concept into a planning artifact.
_FORBIDDEN_PLAN_KEYS: Tuple[str, ...] = (
    "action",
    "confirm_write",
    "dry_run",
    "deploy",
    "execute",
    "xml",
    "raw_xml",
    "config",
    "credentials",
    "password",
    "token",
    "secret",
    "depends_on",
)


class TopologyPlanningInvariantError(RuntimeError):
    """An internal planner defect. Never raised because of authored input.

    The message names the invariant only — no authored value, no key, no id.
    It reaches logs like every other exception here.
    """


def _require(condition: bool, invariant: str) -> None:
    if not condition:
        raise TopologyPlanningInvariantError(
            f"topology planner invariant violated: {invariant}"
        )


def _sorted_by(items, key) -> bool:
    values = [key(item) for item in items]
    return values == sorted(values)


def check_topology_plan_invariants(
    plan: SystemTopologyPlanV1,
    prepared: PreparedTopologyContextV1,
    spec,
) -> None:
    """Verify the plan against every claim the contract makes about it.

    ``spec`` is REQUIRED — not defaulted to ``None``. A default made every
    spec-dependent check skippable by omitting an argument, which is the same
    fail-open shape as the checks it was added to close.

    It is needed for the claims that are ABOUT the authored
    document: that every planned relation is one the spec declares, and that the
    runtime order contains only processes the spec declares and linearizes its
    ProcessCall graph. Without it the checker could confirm shape and ordering
    but not membership — it could not tell a planned relation that exists from
    one invented, which is half of what "each declared relation occupies only
    its permitted bucket" means.
    """
    # 1. apply is structurally impossible.
    _require(plan.apply_supported is False, "apply_supported must be False")

    # 2. Total ordering and uniqueness in every derived collection. Determinism
    #    is an acceptance criterion, and an unordered tuple is where it dies.
    _require(
        _sorted_by(plan.resolved_references, lambda r: (r.object_kind, r.object_key)),
        "resolved_references must be totally ordered",
    )
    _require(
        len({r.object_key for r in plan.resolved_references})
        == len(plan.resolved_references),
        "resolved_references must be unique per object key",
    )
    _require(
        _sorted_by(
            plan.executable_component_prerequisites, lambda p: p.component_key
        ),
        "executable_component_prerequisites must be totally ordered",
    )
    _require(
        len({p.component_key for p in plan.executable_component_prerequisites})
        == len(plan.executable_component_prerequisites),
        "executable_component_prerequisites must be unique per component key",
    )
    _require(
        _sorted_by(
            plan.planning_only_relations, lambda p: (p.relation_kind, p.relation_key)
        ),
        "planning_only_relations must be totally ordered",
    )
    _require(
        len({p.relation_key for p in plan.planning_only_relations})
        == len(plan.planning_only_relations),
        "planning_only_relations must be unique per relation key",
    )
    _require(
        _sorted_by(plan.guidance, lambda g: g.subject),
        "guidance must be totally ordered",
    )
    _require(
        _sorted_by(plan.unresolved_decisions, lambda d: d.subject),
        "unresolved_decisions must be totally ordered",
    )

    # 3. Every prerequisite resolves to a real ComponentPlan symbol and is
    #    marked as owned by the component plan — topology reports, never owns.
    symbols = {s.component_key for s in prepared.context.component_plan_symbols}
    for prerequisite in plan.executable_component_prerequisites:
        _require(
            prerequisite.owner == "component_plan",
            "a prerequisite must be owned by component_plan",
        )
        _require(
            prerequisite.component_key in symbols,
            "a prerequisite must resolve to a ComponentPlan symbol",
        )

    # 4. Namespaces stay distinct. These literals are what keep the three graphs
    #    from being interchangeable, so they are re-asserted rather than trusted.
    for relation in plan.planning_only_relations:
        _require(
            relation.namespace == "system_topology",
            "a planned relation must be in the system_topology namespace",
        )
    _require(
        plan.runtime_process_order.namespace == "topology_runtime",
        "runtime order must be in the topology_runtime namespace",
    )
    _require(
        plan.runtime_process_order.basis == "process_call",
        "runtime order must be based on process_call alone",
    )
    _require(
        len(set(plan.runtime_process_order.order))
        == len(plan.runtime_process_order.order),
        "runtime order must not repeat a process",
    )

    # 4b. Membership AND completeness: everything planned must be something the
    #     spec declared, and a clean plan must not silently drop what it owes.
    if True:
        declared_relations = {rel.key: rel.kind for rel in spec.relations}
        for relation in plan.planning_only_relations:
            _require(
                relation.relation_key in declared_relations,
                "a planned relation must be declared in the spec",
            )
            _require(
                declared_relations[relation.relation_key] == relation.relation_kind,
                "a planned relation must keep the kind the spec declared",
            )
        process_keys = {obj.key for obj in spec.objects if obj.kind == "process"}
        for key in plan.runtime_process_order.order:
            _require(
                key in process_keys,
                "the runtime order may contain only declared processes",
            )
        # And it must LINEARIZE the ProcessCall graph: every callee before its
        # caller. An order that merely contains the right names proves nothing.
        position = {key: index for index, key in enumerate(plan.runtime_process_order.order)}
        for rel in spec.relations:
            if rel.kind != "process_call":
                continue
            if rel.caller_process in position and rel.callee_process in position:
                _require(
                    position[rel.callee_process] < position[rel.caller_process],
                    "the runtime order must place a callee before its caller",
                )

        # Completeness. Checking only the edges whose endpoints already appear
        # let an emptied order pass every ordering test vacuously — there were
        # no positions to compare. A blocked plan legitimately reports no order;
        # a CLEAN one must order every declared process.
        if not plan.blockers:
            _require(
                set(plan.runtime_process_order.order) == process_keys,
                "a clean plan must order every declared process",
            )

        # Relation-bucket completeness AND permissibility, which the ordering
        # checks alone do not give. Without these the checker accepted a clean
        # witnessed plan with its relation silently REMOVED, and a blocked
        # witness-less plan with that relation INJECTED — the two failures the
        # "each declared relation occupies only its permitted bucket" rule
        # exists to name.
        blocked_relation_indexes = set()
        for diagnostic in plan.blockers:
            parts = diagnostic.path.split("/")
            if len(parts) >= 3 and parts[1] == "relations":
                try:
                    blocked_relation_indexes.add(int(parts[2]))
                except ValueError:
                    continue
        planned_keys = {r.relation_key for r in plan.planning_only_relations}
        for index, rel in enumerate(spec.relations):
            if index in blocked_relation_indexes:
                _require(
                    rel.key not in planned_keys,
                    "a blocked relation must not be planned",
                )

    # 5. A gated or unsupported subject never reaches an executable or planning
    #    bucket. This is the issue's central promise.
    blocked = {
        subject
        for subject, row in SYSTEM_TOPOLOGY_CAPABILITIES.items()
        if row.state in ("gated-no-evidence", "unsupported")
    }
    for relation in plan.planning_only_relations:
        _require(
            relation.relation_kind not in blocked,
            "a gated or unsupported relation must not be planned",
        )

    # 6. Blockers are exactly the error bucket — a blocker that is not an error,
    #    or an error that does not block, would make ``is_valid`` a lie.
    _require(
        plan.blockers == plan.validation.errors,
        "blockers must be exactly the validation errors",
    )
    for diagnostic in plan.blockers:
        _require(
            diagnostic.severity == "error", "every blocker must be severity error"
        )

    # 7. No executable or secret-bearing vocabulary anywhere in the payload.
    _require(
        not _find_forbidden(plan.model_dump(mode="json")),
        "the plan payload must not carry action, config, XML or credential keys",
    )


def _find_forbidden(payload: object) -> bool:
    if isinstance(payload, dict):
        for key, value in payload.items():
            if isinstance(key, str) and key.lower() in _FORBIDDEN_PLAN_KEYS:
                return True
            if _find_forbidden(value):
                return True
    elif isinstance(payload, list):
        for item in payload:
            if _find_forbidden(item):
                return True
    return False


__all__: List[str] = [
    "TopologyPlanningInvariantError",
    "check_topology_plan_invariants",
]
