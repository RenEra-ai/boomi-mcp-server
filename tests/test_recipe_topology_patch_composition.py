"""SystemTopologyPatch composition and component/constraint merges (#145 M12.10).

Two separate concerns share this file because they share the same composer phase
discipline: topology objects must all exist before any relation resolves, and
component/constraint contributions must keep contribution order while rejecting a
second writer to one key.
"""

import sys
from pathlib import Path

import pytest

_project_root = Path(__file__).resolve().parent.parent
_src = str(_project_root / "src")
if _src not in sys.path:
    sys.path.insert(0, _src)

from boomi_mcp.errors import RECIPE_PATCH_CONFLICT, RECIPE_PATCH_TARGET_NOT_FOUND
from boomi_mcp.models.recipe_contributions import parse_recipe_contribution
from boomi_mcp.recipes import RecipeError, build_test_registry
from boomi_mcp.recipes.composer import AttributedContributionV1, compose
from boomi_mcp.recipes.contracts import RecipeConflictPolicyV1, RecipeRegistrationV1
from boomi_mcp.recipes.builtins.sync import SyncRecipeInputV1, emit_api_to_api_sync

TOPOLOGY = "t.demo"
PROFILE = "profile-x"


def _topology_patch(operations, topology_id=TOPOLOGY, profile_ref=PROFILE):
    return parse_recipe_contribution(
        {
            "contribution_kind": "system_topology_patch",
            "version": "1",
            "topology_id": topology_id,
            "profile_ref": profile_ref,
            "operations": operations,
        }
    )


def _add_object(key, operation_id, kind="process"):
    obj = {"kind": kind, "key": key}
    if kind in ("process", "api_service", "document_cache", "process_property"):
        obj["component_ref"] = f"$ref:{key}_component"
    elif kind == "runtime":
        obj["runtime_ref"] = f"runtime-{key}"
    return {"operation_id": operation_id, "op": "add_object", "object": obj}


def _add_relation(key, operation_id, caller="p1", callee="p2"):
    return {
        "operation_id": operation_id,
        "op": "add_relation",
        "relation": {
            "kind": "process_call",
            "key": key,
            "caller_process": caller,
            "callee_process": callee,
        },
    }


def _attributed(contribution, recipe_id="r.one", invocation="i1", index=0):
    return AttributedContributionV1(
        invocation_id=invocation,
        recipe_id=recipe_id,
        recipe_version="1.0.0",
        index=index,
        contribution=contribution,
    )


def _descriptors(*specs):
    registrations = [
        RecipeRegistrationV1(
            recipe_id=recipe_id,
            recipe_version="1.0.0",
            entry_kind="executable_recipe",
            is_default=True,
            input_model=SyncRecipeInputV1,
            executor=emit_api_to_api_sync,
            output_types=(
                "component_contribution",
                "constraint_requirement",
                "system_topology_patch",
            ),
            conflict_policy=RecipeConflictPolicyV1(merge_rules=tuple(sorted(rules))),
        )
        for recipe_id, rules in specs
    ]
    registry = build_test_registry(tuple(registrations))
    # Keyed by (id, VERSION) — the composer reads a producer's OWN version's
    # policy, so an id-only map would let two versions share one.
    return {(d.recipe_id, d.recipe_version): d for d in registry.descriptors()}


# ---------------------------------------------------------------------------
# Topology objects
# ---------------------------------------------------------------------------


def test_distinct_object_keys_compose():
    composed = compose(
        [
            _attributed(_topology_patch([_add_object("p1", "op.a")]), invocation="i1"),
            _attributed(
                _topology_patch([_add_object("p2", "op.b")]),
                recipe_id="r.two",
                invocation="i2",
            ),
        ],
        _descriptors(
            ("r.one", ()),
            ("r.two", ()),
        ),
    )
    document = dict(composed.topologies)[TOPOLOGY]
    assert [obj["key"] for obj in document["objects"]] == ["p1", "p2"]


def test_a_repeated_object_key_conflicts_naming_both_producers():
    with pytest.raises(RecipeError) as exc:
        compose(
            [
                _attributed(_topology_patch([_add_object("p1", "op.a")]), invocation="i1"),
                _attributed(
                    _topology_patch([_add_object("p1", "op.b")]),
                    recipe_id="r.two",
                    invocation="i2",
                ),
            ],
            _descriptors(("r.one", ()), ("r.two", ())),
        )
    diagnostic = exc.value.diagnostics[0]
    assert diagnostic.code == RECIPE_PATCH_CONFLICT
    assert diagnostic.recipe_ids == ("r.one", "r.two")


def test_differing_profile_refs_for_one_topology_conflict():
    """Topology never crosses a credential profile (#144).

    Two patches naming different profiles are describing two different
    topologies that happen to share a name.
    """
    with pytest.raises(RecipeError) as exc:
        compose(
            [
                _attributed(_topology_patch([_add_object("p1", "op.a")]), invocation="i1"),
                _attributed(
                    _topology_patch([_add_object("p2", "op.b")], profile_ref="other"),
                    recipe_id="r.two",
                    invocation="i2",
                ),
            ],
            _descriptors(("r.one", ()), ("r.two", ())),
        )
    assert exc.value.diagnostics[0].target.endswith("/profile_ref")


# ---------------------------------------------------------------------------
# Topology relations
# ---------------------------------------------------------------------------


def test_a_relation_may_name_an_object_a_later_recipe_contributed():
    """The object phase completes before ANY relation resolves.

    Without that, the same two contributions would succeed or fail depending on
    which recipe happened to run first.
    """
    composed = compose(
        [
            _attributed(
                _topology_patch([_add_relation("r1", "op.rel"), _add_object("p1", "op.a")]),
                invocation="i1",
            ),
            _attributed(
                _topology_patch([_add_object("p2", "op.b")]),
                recipe_id="r.two",
                invocation="i2",
            ),
        ],
        _descriptors(
            ("r.one", ()),
            ("r.two", ()),
        ),
    )
    document = dict(composed.topologies)[TOPOLOGY]
    assert [rel["key"] for rel in document["relations"]] == ["r1"]


def test_a_relation_naming_an_absent_object_is_target_not_found():
    with pytest.raises(RecipeError) as exc:
        compose(
            [
                _attributed(
                    _topology_patch(
                        [_add_object("p1", "op.a"), _add_relation("r1", "op.rel")]
                    )
                )
            ],
            _descriptors(("r.one", ())),
        )
    diagnostic = exc.value.diagnostics[0]
    assert diagnostic.code == RECIPE_PATCH_TARGET_NOT_FOUND
    assert "relations" in diagnostic.target


def test_a_repeated_relation_key_conflicts():
    with pytest.raises(RecipeError) as exc:
        compose(
            [
                _attributed(
                    _topology_patch(
                        [
                            _add_object("p1", "op.a"),
                            _add_object("p2", "op.b"),
                            _add_relation("r1", "op.r1"),
                            _add_relation("r1x", "op.r2"),
                        ]
                    )
                ),
                _attributed(
                    _topology_patch([_add_relation("r1", "op.r3")]),
                    recipe_id="r.two",
                    invocation="i2",
                ),
            ],
            _descriptors(
                ("r.one", ()),
                ("r.two", ()),
            ),
        )
    assert exc.value.diagnostics[0].code == RECIPE_PATCH_CONFLICT


def test_the_topology_graph_namespace_is_separate_from_the_component_dag():
    """Contributing components changes no topology relation, and vice versa."""
    component = parse_recipe_contribution(
        {
            "contribution_kind": "component_contribution",
            "version": "1",
            "contribution_id": "c.0",
            "component_key": "p1_component",
            "component_type": "process",
            "materialization_mode": "create",
            "materializer_slot": "slot.p1",
        }
    )
    patch = _topology_patch(
        [_add_object("p1", "op.a"), _add_object("p2", "op.b"), _add_relation("r1", "op.r")]
    )
    descriptors = _descriptors(("r.one", ()))
    without = compose([_attributed(patch)], descriptors)
    with_component = compose(
        [_attributed(patch), _attributed(component, index=1)], descriptors
    )
    assert without.topologies == with_component.topologies


# ---------------------------------------------------------------------------
# Component contributions
# ---------------------------------------------------------------------------


def _component(key, contribution_id, slot=None):
    return parse_recipe_contribution(
        {
            "contribution_kind": "component_contribution",
            "version": "1",
            "contribution_id": contribution_id,
            "component_key": key,
            "component_type": "process",
            "materialization_mode": "create",
            "materializer_slot": slot or f"slot.{key}",
        }
    )


def test_distinct_component_keys_compose_and_keep_contribution_order():
    composed = compose(
        [
            _attributed(_component("b_key", "c.0"), index=0),
            _attributed(_component("a_key", "c.1"), index=1),
        ],
        _descriptors(("r.one", ())),
    )
    keys = [item.contribution.component_key for item in composed.component_slots]
    assert keys == ["b_key", "a_key"], "contribution order is semantic; never sorted"


def test_a_repeated_component_key_conflicts_naming_both_producers():
    with pytest.raises(RecipeError) as exc:
        compose(
            [
                _attributed(_component("same", "c.0"), recipe_id="r.one", invocation="i1"),
                _attributed(_component("same", "c.1"), recipe_id="r.two", invocation="i2"),
            ],
            _descriptors(
                ("r.one", ()),
                ("r.two", ()),
            ),
        )
    diagnostic = exc.value.diagnostics[0]
    assert diagnostic.code == RECIPE_PATCH_CONFLICT
    assert diagnostic.recipe_ids == ("r.one", "r.two")
    assert diagnostic.target == "component:same"


# ---------------------------------------------------------------------------
# Constraint requirements
# ---------------------------------------------------------------------------


def _constraint(requirement_id, process_key="main_process"):
    return parse_recipe_contribution(
        {
            "contribution_kind": "constraint_requirement",
            "version": "1",
            "requirement_id": requirement_id,
            "requirement": {"kind": "process", "process_key": process_key},
        }
    )


def test_identical_constraints_dedupe_when_both_recipes_declare_the_rule():
    composed = compose(
        [
            _attributed(_constraint("req.p"), recipe_id="r.one", invocation="i1"),
            _attributed(_constraint("req.p"), recipe_id="r.two", invocation="i2"),
        ],
        _descriptors(
            ("r.one", ("dedupe_identical_constraint",)),
            ("r.two", ("dedupe_identical_constraint",)),
        ),
    )
    assert len(composed.constraints) == 1


def test_identical_constraints_conflict_when_the_rule_is_not_declared():
    with pytest.raises(RecipeError):
        compose(
            [
                _attributed(_constraint("req.p"), recipe_id="r.one", invocation="i1"),
                _attributed(_constraint("req.p"), recipe_id="r.two", invocation="i2"),
            ],
            _descriptors(("r.one", ()), ("r.two", ())),
        )


def test_the_same_requirement_id_with_a_different_body_always_conflicts():
    """Dedupe is for IDENTICAL requirements. Two different ones sharing an id
    are a naming collision, not a merge."""
    with pytest.raises(RecipeError) as exc:
        compose(
            [
                _attributed(
                    _constraint("req.p", "process_a"), recipe_id="r.one", invocation="i1"
                ),
                _attributed(
                    _constraint("req.p", "process_b"), recipe_id="r.two", invocation="i2"
                ),
            ],
            _descriptors(
                ("r.one", ("dedupe_identical_constraint",)),
                ("r.two", ("dedupe_identical_constraint",)),
            ),
        )
    assert exc.value.diagnostics[0].code == RECIPE_PATCH_CONFLICT


def test_distinct_requirement_ids_compose_in_contribution_order():
    composed = compose(
        [
            _attributed(_constraint("req.b"), index=0),
            _attributed(_constraint("req.a"), index=1),
        ],
        _descriptors(("r.one", ())),
    )
    assert [c.contribution.requirement_id for c in composed.constraints] == [
        "req.b",
        "req.a",
    ]
