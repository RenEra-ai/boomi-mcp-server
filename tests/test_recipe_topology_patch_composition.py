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
from boomi_mcp.recipes import RecipeError
from boomi_mcp.recipes.registry import build_test_registry
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


def test_a_repeated_materializer_slot_conflicts_naming_both_producers():
    """The slot is a contested resource too, and only the KEY used to be checked.

    Two contributions naming one slot with different component keys are two
    producers claiming one materialization. That reached the catalog instead,
    where the slot's header no longer matched the second claimant and it
    surfaced as ``RECIPE_CONTRIBUTION_INVALID`` attributed to NEITHER recipe — a
    two-writer conflict wearing a one-writer code, with nothing in the
    diagnostic to say who disagreed (issue #145, §6 architect review).
    """
    with pytest.raises(RecipeError) as exc:
        compose(
            [
                _attributed(
                    _component("key_one", "c.0", slot="slot.shared"),
                    recipe_id="r.one",
                    invocation="i1",
                ),
                _attributed(
                    _component("key_two", "c.1", slot="slot.shared"),
                    recipe_id="r.two",
                    invocation="i2",
                ),
            ],
            _descriptors(("r.one", ()), ("r.two", ())),
        )
    diagnostic = exc.value.diagnostics[0]
    assert diagnostic.code == RECIPE_PATCH_CONFLICT
    assert diagnostic.recipe_ids == ("r.one", "r.two")
    assert diagnostic.target == "materializer_slot:slot.shared"


def test_distinct_slots_for_distinct_keys_still_compose():
    """The guard is on COLLISION, not on slots existing — the normal case is
    unaffected, which is what makes the conflict above a real finding rather
    than a blanket refusal."""
    composed = compose(
        [
            _attributed(_component("key_one", "c.0", slot="slot.one"), index=0),
            _attributed(_component("key_two", "c.1", slot="slot.two"), index=1),
        ],
        _descriptors(("r.one", ())),
    )
    assert len(composed.component_slots) == 2


# ---------------------------------------------------------------------------
# Direct topology bases
# ---------------------------------------------------------------------------


def _direct_base(**overrides):
    """A VALID direct base, so a test can vary exactly one thing.

    An empty ``objects`` list fails topology cardinality on its own, which would
    make every test below pass without ever exercising the property it names —
    the "claim wider than the check" failure these tests exist to prevent.
    """
    base = {
        "version": "1",
        "profile_ref": "qa",
        "objects": [{"kind": "process", "key": "p1", "component_ref": "$ref:p1_component"}],
        "relations": [],
    }
    base.update(overrides)
    return base


def test_a_valid_direct_topology_base_still_composes():
    """The control. Without it, the three rejection tests below could all be
    passing because the fixture itself is malformed."""
    composed = compose(
        [], _descriptors(("r.one", ())), direct_topologies={"t1": _direct_base()}
    )
    document = dict(composed.topologies)["t1"]
    assert [obj["key"] for obj in document["objects"]] == ["p1"]


def test_a_direct_topology_base_with_a_wrong_version_is_rejected_not_overwritten():
    """Plan §4 step 1: direct bases are STRICTLY VALIDATED, then composed onto.

    Cherry-picking three keys and stamping ``version: "1"`` accepted whatever
    arrived: a base declaring another version had that declaration silently
    replaced with the one the composer wanted. Validating after composition
    cannot recover this — by then the rewrite has already happened
    (issue #145, §6 architect review).
    """
    with pytest.raises(RecipeError) as exc:
        compose(
            [],
            _descriptors(("r.one", ())),
            direct_topologies={"t1": _direct_base(version="99")},
        )
    assert exc.value.diagnostics[0].target == "direct_topology:t1"
    assert "TOPOLOGY_SCHEMA_VERSION_UNSUPPORTED" in exc.value.diagnostics[0].cause_codes


def test_a_direct_topology_base_with_an_unknown_field_is_rejected_not_dropped():
    """Unknown fields were SELECTED AWAY by the three-key pick, so nothing
    downstream could ever see them — silent acceptance, not rejection."""
    with pytest.raises(RecipeError) as exc:
        compose(
            [],
            _descriptors(("r.one", ())),
            direct_topologies={"t1": _direct_base(smuggled={"raw_xml": "<x/>"})},
        )
    assert exc.value.diagnostics[0].target == "direct_topology:t1"
    assert "TOPOLOGY_SCHEMA_UNKNOWN_FIELD" in exc.value.diagnostics[0].cause_codes


def test_a_direct_topology_base_without_a_profile_ref_is_a_recipe_diagnostic():
    """Previously a bare ``KeyError`` from a dict subscript — an unattributed
    crash where the taxonomy promises a diagnostic."""
    payload = _direct_base()
    del payload["profile_ref"]
    with pytest.raises(RecipeError) as exc:
        compose([], _descriptors(("r.one", ())), direct_topologies={"t1": payload})
    assert exc.value.diagnostics[0].target == "direct_topology:t1"


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


def test_an_internal_bug_in_the_topology_parser_propagates_as_itself():
    """The catch is narrowed to the CANONICAL error, and this is why.

    A blanket ``except Exception`` would launder an internal bug — a
    ``TypeError`` from some future edit — into ``RECIPE_CONSTRAINT_FAILED`` with
    empty ``cause_codes`` and a remediation reading "fix the condition the cause
    codes name", which would name nothing. A crash must surface as a crash
    (issue #145, live QA).
    """
    from unittest.mock import patch

    with patch(
        "boomi_mcp.models.system_topology.parse_system_topology_v1",
        side_effect=TypeError("internal bug"),
    ):
        with pytest.raises(TypeError, match="internal bug"):
            compose(
                [],
                _descriptors(("r.one", ())),
                direct_topologies={"t1": _direct_base()},
            )


def test_narrowing_the_catch_did_not_let_a_validation_failure_escape_raw():
    """The risk of narrowing, tested directly.

    A blanket catch also absorbed shapes that never reach the model's own
    validator — a payload that is not a mapping at all. Those must still arrive
    as recipe diagnostics, which they do because ``parse_system_topology_v1``
    converts them itself rather than because a broad ``except`` hid them.
    """
    for hostile in (None, "nope", 17, [], {"objects": "nope"}):
        with pytest.raises(RecipeError) as exc:
            compose([], _descriptors(("r.one", ())), direct_topologies={"t1": hostile})
        assert exc.value.diagnostics[0].target == "direct_topology:t1"
