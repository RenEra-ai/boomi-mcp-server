"""Deterministic contribution composition and conflict detection (issue #145).

Composition has exactly one order, and it is a property of the inputs rather than
of the wall clock, the iteration order of a dict, or the sequence in which
invocations happened to be assembled:

1. invocations are topologically sorted on their declared recipe dependencies,
   with ties broken by ``(recipe_id, parsed SemVer, invocation_id)``;
2. each executor's returned contribution tuple keeps its own order;
3. operations are applied in a FIXED PHASE sequence — roots, then linear
   inserts, then Branch legs, then topology objects, then topology relations,
   then components, then constraints.

The phase sequence is what makes "append a leg" well-defined regardless of which
recipe ran first: every root exists before any insert or append is attempted, and
every object exists before any relation is resolved. Without it, two orderings of
the same recipe set could produce a target-not-found in one and success in the
other.

**Nothing is re-sorted after normalization.** Branch-leg order, contribution
order and step order are SEMANTIC — they decide what the process does — so the
only sorting anywhere in this module is on the invocation list itself, before any
contribution is read.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List, Mapping, Optional, Sequence, Tuple

from ..errors import (
    RECIPE_CONSTRAINT_FAILED,
    RECIPE_PATCH_CONFLICT,
    RECIPE_PATCH_TARGET_NOT_FOUND,
)
from ..models.process_ir import ProcessIRV1
from ..models.recipe_contributions import canonical_recipe_contribution_json
from .contracts import RecipeDescriptorV1, RecipeInputBase, parse_semver
from .errors import DIRECT_AUTHORING_PRODUCER, RecipeError, recipe_diagnostic

# Merge rules, by the operation they govern.
_MERGE_INSERT_STEP = "insert_root_linear_step"
_MERGE_APPEND_LEG = "append_root_terminal_leg"
_MERGE_DEDUPE_CONSTRAINT = "dedupe_identical_constraint"

#: The terminal kinds that form the root's indivisible tail. ``target`` is not
#: here on purpose: a ``target`` is followed by its own ``stop``, and the pair is
#: what "before the terminal" must not split.
_TERMINAL_CONTROL_KINDS = frozenset(
    {"branch", "decision", "exception", "try_catch", "return_documents", "stop"}
)


@dataclass(frozen=True)
class RecipeInvocationV1:
    """One requested run of one registered recipe."""

    invocation_id: str
    descriptor: RecipeDescriptorV1
    validated_input: RecipeInputBase


@dataclass(frozen=True)
class AttributedContributionV1:
    """A contribution, plus who produced it and where in their tuple."""

    invocation_id: str
    recipe_id: str
    recipe_version: str
    index: int
    contribution: Any


@dataclass(frozen=True)
class ComposedContributionsV1:
    """The composed result: ordered, conflict-free, ready for validation."""

    process_roots: Tuple[Tuple[str, ProcessIRV1], ...]
    component_slots: Tuple[AttributedContributionV1, ...]
    topologies: Tuple[Tuple[str, Dict[str, Any]], ...]
    constraints: Tuple[AttributedContributionV1, ...]


def order_invocations(
    invocations: Sequence[RecipeInvocationV1],
) -> Tuple[RecipeInvocationV1, ...]:
    """Stable topological order over declared recipe dependencies.

    Kahn's algorithm with a SORTED ready set: at every step, take the smallest
    invocation by ``(recipe_id, parsed_semver, invocation_id)`` among those whose
    dependencies are ALL already placed, then re-evaluate readiness. The result
    depends only on the dependency graph and the tie-break key — never on the
    order the caller happened to assemble the list in.

    Re-evaluating after every placement is the whole algorithm, not a detail. An
    earlier version sorted once and then swept the list front to back, placing
    everything it found ready in that pass. That is deterministic too, but it is
    a LAYERED order, not this one, and the two disagree: for ``r.a`` depending on
    ``r.b`` plus an independent ``r.c``, the sweep yields ``r.b, r.c, r.a``
    because ``r.c`` is reached before the pass loops back to the now-ready
    ``r.a``, while a sorted ready set yields ``r.b, r.a, r.c``. Since invocation
    order decides contribution order, and contribution order decides component
    order and Branch-leg append order, the difference is semantic — it reaches
    the emitted bytes. The docstring named this algorithm; the code now is it
    (issue #145, §6 architect review).

    A dependency on a recipe that is not in this invocation set is not an error
    here: prerequisites are a descriptor fact the engine preflights, and treating
    an absent one as a cycle would report the wrong failure.
    """
    def sort_key(inv: RecipeInvocationV1):
        return (
            inv.descriptor.recipe_id,
            parse_semver(inv.descriptor.recipe_version),
            inv.invocation_id,
        )

    by_id: Dict[Tuple[str, str], List[RecipeInvocationV1]] = {}
    for inv in invocations:
        by_id.setdefault(
            (inv.descriptor.recipe_id, inv.descriptor.recipe_version), []
        ).append(inv)

    remaining = sorted(invocations, key=sort_key)
    ordered: List[RecipeInvocationV1] = []
    placed: set = set()

    def is_ready(inv: RecipeInvocationV1) -> bool:
        for prerequisite in inv.descriptor.prerequisites:
            if getattr(prerequisite, "kind", None) != "recipe":
                continue
            dep_key = (prerequisite.recipe_id, prerequisite.recipe_version)
            if dep_key not in by_id:
                continue  # not in this run; the engine preflights it
            if not all(d.invocation_id in placed for d in by_id[dep_key]):
                return False
        return True

    while remaining:
        # ``remaining`` is sorted and stays sorted under removal, so the first
        # ready element IS the smallest ready element.
        nxt = next((inv for inv in remaining if is_ready(inv)), None)
        if nxt is None:
            # A genuine cycle. Fail closed with a deterministic order rather
            # than looping: the caller's registration graph is the defect.
            raise ValueError(
                "recipe prerequisite cycle among: "
                + ", ".join(sorted(i.descriptor.recipe_id for i in remaining))
            )
        ordered.append(nxt)
        placed.add(nxt.invocation_id)
        remaining.remove(nxt)
    return tuple(ordered)


def _conflict(
    first: Tuple[str, str, str], second: Tuple[str, str, str], *, target: str
) -> RecipeError:
    """A two-producer conflict, in normalized writer order.

    ALWAYS exactly two producers, even when both are the same recipe id — "this
    recipe conflicts with itself" is a real and confusing situation, and
    collapsing the pair would hide which invocation was the second writer.
    """
    return RecipeError(
        (
            recipe_diagnostic(
                RECIPE_PATCH_CONFLICT,
                phase="composition",
                target=target,
                recipe_ids=(first[0], second[0]),
                recipe_versions=(first[1], second[1]),
                invocation_ids=(first[2], second[2]),
            ),
        )
    )


def _producer(item: AttributedContributionV1) -> Tuple[str, str, str]:
    return (item.recipe_id, item.recipe_version, item.invocation_id)


def _declares(
    descriptors: Mapping[Any, RecipeDescriptorV1],
    producer: Tuple[str, str, str],
    rule: str,
) -> bool:
    """Whether the producer's OWN descriptor version declares ``rule``.

    Looked up by ``(recipe_id, recipe_version)``. Keying on the id alone let two
    versions of one recipe share a policy, so the decision read one version's
    rules for both writers.
    """
    descriptor = descriptors.get((producer[0], producer[1]))
    if descriptor is None or descriptor.conflict_policy is None:
        return False
    return rule in descriptor.conflict_policy.merge_rules


def _terminal_split(steps: List[Any]) -> int:
    """Index where the root's indivisible terminal unit begins.

    A ``target`` immediately followed by a ``stop`` is ONE unit — the emitter
    routes documents to the target and then stops, and a step wedged between
    them belongs to no path. Everything else that can terminate a root is a
    single step.
    """
    if len(steps) >= 2 and steps[-2].kind == "target" and steps[-1].kind == "stop":
        return len(steps) - 2
    if steps and steps[-1].kind in _TERMINAL_CONTROL_KINDS:
        return len(steps) - 1
    return len(steps)


def compose(
    attributed: Sequence[AttributedContributionV1],
    descriptors: Mapping[Tuple[str, str], RecipeDescriptorV1],
    *,
    direct_process_roots: Mapping[str, ProcessIRV1] = None,
    direct_topologies: Mapping[str, Dict[str, Any]] = None,
) -> ComposedContributionsV1:
    """Apply every contribution in the fixed phase order."""
    direct_roots = dict(direct_process_roots or {})
    direct_topos = dict(direct_topologies or {})

    process_patches = [
        item for item in attributed if item.contribution.contribution_kind == "process_ir_patch"
    ]
    topology_patches = [
        item
        for item in attributed
        if item.contribution.contribution_kind == "system_topology_patch"
    ]
    components = [
        item
        for item in attributed
        if item.contribution.contribution_kind == "component_contribution"
    ]
    constraints = [
        item
        for item in attributed
        if item.contribution.contribution_kind == "constraint_requirement"
    ]

    roots = _compose_process_roots(process_patches, descriptors, direct_roots)
    topologies = _compose_topologies(topology_patches, descriptors, direct_topos)
    ordered_components = _compose_components(components, descriptors)
    ordered_constraints = _compose_constraints(constraints, descriptors)

    return ComposedContributionsV1(
        process_roots=tuple(roots.items()),
        component_slots=tuple(ordered_components),
        topologies=tuple(topologies.items()),
        constraints=tuple(ordered_constraints),
    )


# ---------------------------------------------------------------------------
# Phase 1-3: process roots
# ---------------------------------------------------------------------------


def _compose_process_roots(
    patches: Sequence[AttributedContributionV1],
    descriptors: Mapping[Tuple[str, str], RecipeDescriptorV1],
    direct_roots: Dict[str, ProcessIRV1],
) -> Dict[str, ProcessIRV1]:
    roots: Dict[str, Any] = {
        key: root.model_dump(mode="json") for key, root in direct_roots.items()
    }
    root_writers: Dict[str, Tuple[str, str, str]] = {
        key: (DIRECT_AUTHORING_PRODUCER, "", "") for key in direct_roots
    }

    # Phase 1 — establish roots.
    for item in patches:
        for operation in item.contribution.operations:
            if operation.op != "set_process_root":
                continue
            key = item.contribution.process_key
            if key in roots:
                raise _conflict(
                    root_writers[key], _producer(item), target=f"process:{key}/root"
                )
            roots[key] = operation.root.model_dump(mode="json")
            root_writers[key] = _producer(item)

    # Phase 2 — insert linear steps before the terminal unit.
    insert_writers: Dict[str, Tuple[str, str, str]] = {}
    for item in patches:
        for operation in item.contribution.operations:
            if operation.op != "insert_root_linear_step":
                continue
            key = item.contribution.process_key
            if key not in roots:
                raise RecipeError(
                    (
                        recipe_diagnostic(
                            RECIPE_PATCH_TARGET_NOT_FOUND,
                            phase="composition",
                            target="root",
                            recipe_ids=(item.recipe_id,),
                            recipe_versions=(item.recipe_version,),
                            invocation_ids=(item.invocation_id,),
                        ),
                    )
                )
            previous = insert_writers.get(key)
            if previous is not None and not (
                _declares(descriptors, previous, _MERGE_INSERT_STEP)
                and _declares(descriptors, _producer(item), _MERGE_INSERT_STEP)
            ):
                raise _conflict(
                    previous, _producer(item), target=f"process:{key}/root.before_terminal"
                )
            insert_writers[key] = _producer(item)
            body = roots[key]["body"]
            steps = list(body["steps"])
            split = _terminal_split_payload(steps)
            steps.insert(split, operation.step.model_dump(mode="json"))
            body["steps"] = steps

    # Phase 3 — append legs to the terminal Branch.
    leg_writers: Dict[str, Tuple[str, str, str]] = {}
    for item in patches:
        for operation in item.contribution.operations:
            if operation.op != "append_root_terminal_leg":
                continue
            key = item.contribution.process_key
            steps = roots.get(key, {}).get("body", {}).get("steps") if key in roots else None
            if not steps or steps[-1].get("kind") != "branch":
                raise RecipeError(
                    (
                        recipe_diagnostic(
                            RECIPE_PATCH_TARGET_NOT_FOUND,
                            phase="composition",
                            target="root.terminal.branch.legs",
                            recipe_ids=(item.recipe_id,),
                            recipe_versions=(item.recipe_version,),
                            invocation_ids=(item.invocation_id,),
                        ),
                    )
                )
            previous = leg_writers.get(key)
            if previous is not None and not (
                _declares(descriptors, previous, _MERGE_APPEND_LEG)
                and _declares(descriptors, _producer(item), _MERGE_APPEND_LEG)
            ):
                raise _conflict(
                    previous,
                    _producer(item),
                    target=f"process:{key}/root.terminal.branch.legs",
                )
            leg_writers[key] = _producer(item)
            steps[-1]["legs"] = list(steps[-1]["legs"]) + [
                operation.leg.model_dump(mode="json")
            ]

    # Re-parse every assembled root. Composition works on dumped payloads so a
    # frozen model never has to be mutated in place; parsing back is what proves
    # the result is still a valid ProcessIRV1 — cardinality, terminal rules and
    # all — rather than a dict that merely looks like one.
    #
    # The failure is translated, not propagated. ADR §8 states the rule without
    # qualification: a canonical rejection never leaves the recipe layer wearing
    # a ``PROCESS_IR_*`` code, it becomes ``RECIPE_CONSTRAINT_FAILED`` carrying
    # the canonical codes as value-free causes. This call sits INSIDE ``compose``,
    # which the engine invokes before the wrapper around ``_compile_processes``,
    # so an untranslated error here was the one path by which a raw
    # ``ProcessIRValidationError`` could reach a ``run_recipes`` caller — and
    # every such caller would then have to know a taxonomy the ADR promises it
    # never sees (issue #145, §6 architect review).
    # Catches the CANONICAL error specifically, never a bare ``Exception``. A
    # blanket catch would launder an internal bug — a ``TypeError`` from a future
    # edit — into ``RECIPE_CONSTRAINT_FAILED`` with empty ``cause_codes`` and a
    # remediation reading "fix the condition the cause codes name", which name
    # nothing. A crash should surface as a crash (issue #145, live QA).
    from ..models.process_ir import ProcessIRValidationError, parse_process_ir_v1

    parsed: Dict[str, ProcessIRV1] = {}
    for key, payload in roots.items():
        try:
            parsed[key] = parse_process_ir_v1(payload)
        except ProcessIRValidationError as exc:
            codes = tuple(
                getattr(d, "code", "") for d in getattr(exc, "diagnostics", ())
            )
            raise RecipeError(
                (
                    recipe_diagnostic(
                        RECIPE_CONSTRAINT_FAILED,
                        phase="composition",
                        target=f"process:{key}",
                        cause_codes=tuple(c for c in codes if c),
                    ),
                )
            ) from None
    return parsed


def _terminal_split_payload(steps: List[Dict[str, Any]]) -> int:
    if (
        len(steps) >= 2
        and steps[-2].get("kind") == "target"
        and steps[-1].get("kind") == "stop"
    ):
        return len(steps) - 2
    if steps and steps[-1].get("kind") in _TERMINAL_CONTROL_KINDS:
        return len(steps) - 1
    return len(steps)


# ---------------------------------------------------------------------------
# Phase 4-5: topology
# ---------------------------------------------------------------------------


def _compose_topologies(
    patches: Sequence[AttributedContributionV1],
    descriptors: Mapping[Tuple[str, str], RecipeDescriptorV1],
    direct_topologies: Dict[str, Dict[str, Any]],
) -> Dict[str, Dict[str, Any]]:
    # A direct base is STRICTLY VALIDATED before anything is composed onto it.
    # Cherry-picking three keys and stamping ``version: "1"`` accepted whatever
    # arrived: a base declaring another version had that declaration silently
    # overwritten with the one the composer wanted, unknown or forbidden fields
    # were dropped instead of rejected, and a base with no ``profile_ref`` raised
    # a bare ``KeyError`` out of a dict subscript. Parsing is what turns all
    # three into one attributed diagnostic, and it has to happen HERE — after
    # composition the dropped fields are gone, so the later parse in
    # ``_plan_topologies`` cannot see what was silently normalized away
    # (issue #145, §6 architect review).
    # The CANONICAL error only — same reasoning as the ProcessIR re-parse above,
    # and the same shape ``engine._plan_topologies`` already used. A missing
    # ``profile_ref`` is now a validation diagnostic rather than a ``KeyError``
    # because the model requires the field, not because a blanket catch swallows
    # the subscript.
    from ..models.system_topology import (
        SystemTopologyValidationError,
        parse_system_topology_v1,
    )

    docs: Dict[str, Dict[str, Any]] = {}
    for key, payload in direct_topologies.items():
        try:
            spec = parse_system_topology_v1(payload)
        except SystemTopologyValidationError as exc:
            codes = tuple(
                getattr(d, "code", "") for d in getattr(exc, "diagnostics", ())
            )
            raise RecipeError(
                (
                    recipe_diagnostic(
                        RECIPE_CONSTRAINT_FAILED,
                        phase="composition",
                        target=f"direct_topology:{key}",
                        cause_codes=tuple(c for c in codes if c),
                    ),
                )
            ) from None
        dumped = spec.model_dump(mode="json")
        docs[key] = {
            "version": dumped["version"],
            "profile_ref": dumped["profile_ref"],
            "objects": list(dumped.get("objects", [])),
            "relations": list(dumped.get("relations", [])),
        }
    profile_writers: Dict[str, Tuple[str, str, str]] = {
        key: (DIRECT_AUTHORING_PRODUCER, "", "") for key in direct_topologies
    }
    object_writers: Dict[Tuple[str, str], Tuple[str, str, str]] = {}
    relation_writers: Dict[Tuple[str, str], Tuple[str, str, str]] = {}

    # Phase 4 — objects.
    for item in patches:
        patch = item.contribution
        doc = docs.get(patch.topology_id)
        if doc is None:
            doc = {
                "version": "1",
                "profile_ref": patch.profile_ref,
                "objects": [],
                "relations": [],
            }
            docs[patch.topology_id] = doc
            profile_writers[patch.topology_id] = _producer(item)
        elif doc["profile_ref"] != patch.profile_ref:
            # Topology never crosses a credential profile (#144). Two patches
            # naming different profiles for one document are describing two
            # different topologies that happen to share a name.
            raise _conflict(
                profile_writers[patch.topology_id],
                _producer(item),
                target=f"topology:{patch.topology_id}/profile_ref",
            )
        for operation in patch.operations:
            if operation.op != "add_object":
                continue
            key = (patch.topology_id, operation.object.key)
            previous = object_writers.get(key)
            if previous is not None or any(
                obj["key"] == operation.object.key for obj in doc["objects"]
            ):
                raise _conflict(
                    previous or (DIRECT_AUTHORING_PRODUCER, "", ""),
                    _producer(item),
                    target=f"topology:{patch.topology_id}/objects/{operation.object.key}",
                )
            # Distinct keys compose unconditionally; the enforcement that matters
            # is the repeated-key conflict above. There is deliberately no merge
            # rule to declare for this — one existed briefly and gated nothing.
            object_writers[key] = _producer(item)
            doc["objects"].append(operation.object.model_dump(mode="json"))

    # Phase 5 — relations. After EVERY object, so a relation may name an object
    # a later-ordered recipe contributed.
    for item in patches:
        patch = item.contribution
        doc = docs[patch.topology_id]
        object_keys = {obj["key"] for obj in doc["objects"]}
        for operation in patch.operations:
            if operation.op != "add_relation":
                continue
            relation = operation.relation
            key = (patch.topology_id, relation.key)
            previous = relation_writers.get(key)
            if previous is not None or any(
                rel["key"] == relation.key for rel in doc["relations"]
            ):
                raise _conflict(
                    previous or (DIRECT_AUTHORING_PRODUCER, "", ""),
                    _producer(item),
                    target=f"topology:{patch.topology_id}/relations/{relation.key}",
                )
            missing = [
                name
                for name, value in relation.model_dump(mode="json").items()
                if name not in ("kind", "key") and value not in object_keys
            ]
            if missing:
                raise RecipeError(
                    (
                        recipe_diagnostic(
                            RECIPE_PATCH_TARGET_NOT_FOUND,
                            phase="composition",
                            target=f"topology:{patch.topology_id}/relations/{relation.kind}",
                            recipe_ids=(item.recipe_id,),
                            recipe_versions=(item.recipe_version,),
                            invocation_ids=(item.invocation_id,),
                        ),
                    )
                )
            relation_writers[key] = _producer(item)
            doc["relations"].append(relation.model_dump(mode="json"))

    return docs


# ---------------------------------------------------------------------------
# Phase 6-7: components and constraints
# ---------------------------------------------------------------------------


def _compose_components(
    items: Sequence[AttributedContributionV1],
    descriptors: Mapping[Tuple[str, str], RecipeDescriptorV1],
) -> List[AttributedContributionV1]:
    """Order component contributions, rejecting two writers for one slot or key.

    Both keys are contested resources, and only one of them used to be checked.
    A ``materializer_slot`` selects an entry in the private catalog, so two
    contributions naming the same slot with different component keys are two
    producers claiming one materialization — the exact shape ``RECIPE_PATCH_CONFLICT``
    exists to report, naming both. It was reaching materialization instead, where
    the slot's header no longer matched the second claimant's key and it surfaced
    as ``RECIPE_CONTRIBUTION_INVALID`` attributed to NEITHER producer: a
    two-writer conflict wearing a one-writer code, with nothing in the diagnostic
    to say which recipes disagreed (issue #145, §6 architect review).
    """
    ordered: List[AttributedContributionV1] = []
    writers: Dict[str, Tuple[str, str, str]] = {}
    slot_writers: Dict[str, Tuple[str, str, str]] = {}
    for item in items:
        key = item.contribution.component_key
        previous = writers.get(key)
        if previous is not None:
            raise _conflict(previous, _producer(item), target=f"component:{key}")
        slot = item.contribution.materializer_slot
        previous_slot = slot_writers.get(slot)
        if previous_slot is not None:
            raise _conflict(
                previous_slot, _producer(item), target=f"materializer_slot:{slot}"
            )
        writers[key] = _producer(item)
        slot_writers[slot] = _producer(item)
        ordered.append(item)
    return ordered


def _compose_constraints(
    items: Sequence[AttributedContributionV1],
    descriptors: Mapping[Tuple[str, str], RecipeDescriptorV1],
) -> List[AttributedContributionV1]:
    ordered: List[AttributedContributionV1] = []
    seen: Dict[str, Tuple[str, Tuple[str, str, str]]] = {}
    for item in items:
        requirement_id = item.contribution.requirement_id
        canonical = canonical_recipe_contribution_json(item.contribution)
        previous = seen.get(requirement_id)
        if previous is not None:
            previous_canonical, previous_producer = previous
            if previous_canonical == canonical and (
                _declares(descriptors, previous_producer, _MERGE_DEDUPE_CONSTRAINT)
                and _declares(descriptors, _producer(item), _MERGE_DEDUPE_CONSTRAINT)
            ):
                continue
            raise _conflict(
                previous_producer,
                _producer(item),
                target=f"constraint:{requirement_id}",
            )
        seen[requirement_id] = (canonical, _producer(item))
        ordered.append(item)
    return ordered


__all__ = [
    "AttributedContributionV1",
    "ComposedContributionsV1",
    "RecipeInvocationV1",
    "compose",
    "order_invocations",
]
