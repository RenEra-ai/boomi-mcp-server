"""ProcessIRPatch composition, merges and conflicts (issue #145 M12.10).

The composer's job is that the SAME set of contributions always produces the same
result, and that two writers to one semantic slot are an error unless both said in
advance they expected to share it. These tests drive ``compose`` directly so the
ordering and conflict rules are exercised without a full engine run.
"""

import sys
from pathlib import Path

import pytest

_project_root = Path(__file__).resolve().parent.parent
_src = str(_project_root / "src")
if _src not in sys.path:
    sys.path.insert(0, _src)

from boomi_mcp.errors import RECIPE_PATCH_CONFLICT, RECIPE_PATCH_TARGET_NOT_FOUND
from boomi_mcp.models.process_ir import parse_process_ir_v1
from boomi_mcp.models.recipe_contributions import parse_recipe_contribution
from boomi_mcp.recipes import RecipeError, build_test_registry
from boomi_mcp.recipes.composer import AttributedContributionV1, compose, order_invocations
from boomi_mcp.recipes.composer import RecipeInvocationV1
from boomi_mcp.recipes.contracts import (
    RecipeConflictPolicyV1,
    RecipeDependencyV1,
    RecipeRegistrationV1,
)
from boomi_mcp.recipes.builtins.sync import SyncRecipeInputV1, emit_api_to_api_sync
from boomi_mcp.recipes.errors import DIRECT_AUTHORING_PRODUCER

PROCESS = "main_process"


def _linear_root(with_branch=False):
    steps = [
        {
            "kind": "source",
            "connection_ref": "$ref:src_conn",
            "operation_ref": "$ref:src_op",
        },
        {"kind": "map_ref", "map_ref": "$ref:the_map"},
    ]
    if with_branch:
        steps.append(
            {
                "kind": "branch",
                "label": None,
                "legs": [
                    {
                        "steps": [],
                        "terminal": {
                            "kind": "target",
                            "connection_ref": f"$ref:t{i}_conn",
                            "operation_ref": f"$ref:t{i}_op",
                        },
                    }
                    for i in range(2)
                ],
            }
        )
    else:
        steps.append(
            {
                "kind": "target",
                "connection_ref": "$ref:tgt_conn",
                "operation_ref": "$ref:tgt_op",
            }
        )
        steps.append({"kind": "stop"})
    return {"version": "1", "body": {"kind": "sequence", "steps": steps}}


def _root_patch(with_branch=False, process=PROCESS, operation_id="op.root"):
    return parse_recipe_contribution(
        {
            "contribution_kind": "process_ir_patch",
            "version": "1",
            "process_key": process,
            "operations": [
                {
                    "operation_id": operation_id,
                    "op": "set_process_root",
                    "slot": "root",
                    "root": _linear_root(with_branch),
                }
            ],
        }
    )


def _insert_patch(process=PROCESS, operation_id="op.insert", label="Injected"):
    return parse_recipe_contribution(
        {
            "contribution_kind": "process_ir_patch",
            "version": "1",
            "process_key": process,
            "operations": [
                {
                    "operation_id": operation_id,
                    "op": "insert_root_linear_step",
                    "slot": "root.before_terminal",
                    "step": {"kind": "message", "text": "hello", "label": label},
                }
            ],
        }
    )


def _leg_patch(process=PROCESS, operation_id="op.leg", index=9):
    return parse_recipe_contribution(
        {
            "contribution_kind": "process_ir_patch",
            "version": "1",
            "process_key": process,
            "operations": [
                {
                    "operation_id": operation_id,
                    "op": "append_root_terminal_leg",
                    "slot": "root.terminal.branch.legs",
                    "leg": {
                        "steps": [],
                        "terminal": {
                            "kind": "target",
                            "connection_ref": f"$ref:x{index}_conn",
                            "operation_ref": f"$ref:x{index}_op",
                        },
                    },
                }
            ],
        }
    )


def _attributed(contribution, recipe_id="r.one", version="1.0.0", invocation="i1", index=0):
    return AttributedContributionV1(
        invocation_id=invocation,
        recipe_id=recipe_id,
        recipe_version=version,
        index=index,
        contribution=contribution,
    )


def _descriptors(*specs):
    """``{recipe_id: descriptor}`` for the given (id, merge_rules) pairs."""
    registrations = []
    for recipe_id, rules in specs:
        registrations.append(
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
                    "process_ir_patch",
                ),
                conflict_policy=RecipeConflictPolicyV1(merge_rules=tuple(sorted(rules))),
            )
        )
    registry = build_test_registry(tuple(registrations))
    # Keyed by (id, VERSION) — the composer reads a producer's OWN version's
    # policy, so an id-only map would let two versions share one.
    return {(d.recipe_id, d.recipe_version): d for d in registry.descriptors()}


# ---------------------------------------------------------------------------
# Roots
# ---------------------------------------------------------------------------


def test_a_single_root_composes():
    composed = compose([_attributed(_root_patch())], _descriptors(("r.one", ())))
    roots = dict(composed.process_roots)
    assert set(roots) == {PROCESS}
    assert [s.kind for s in roots[PROCESS].body.steps] == [
        "source",
        "map_ref",
        "target",
        "stop",
    ]


def test_two_roots_for_one_process_conflict_naming_both_producers():
    with pytest.raises(RecipeError) as exc:
        compose(
            [
                _attributed(_root_patch(), recipe_id="r.one", invocation="i1"),
                _attributed(_root_patch(), recipe_id="r.two", invocation="i2"),
            ],
            _descriptors(("r.one", ()), ("r.two", ())),
        )
    diagnostic = exc.value.diagnostics[0]
    assert diagnostic.code == RECIPE_PATCH_CONFLICT
    assert diagnostic.recipe_ids == ("r.one", "r.two")
    assert diagnostic.invocation_ids == ("i1", "i2")
    assert diagnostic.remediation


def test_two_byte_identical_roots_still_conflict():
    """Identical is not the same as intended. Two authors of one root is a bug."""
    with pytest.raises(RecipeError):
        compose(
            [
                _attributed(_root_patch(), recipe_id="r.one", invocation="i1"),
                _attributed(_root_patch(), recipe_id="r.one", invocation="i2"),
            ],
            _descriptors(("r.one", ())),
        )


def test_one_recipe_conflicting_with_itself_still_names_two_producers():
    """Collapsing the pair would hide WHICH invocation was the second writer."""
    with pytest.raises(RecipeError) as exc:
        compose(
            [
                _attributed(_root_patch(), recipe_id="r.one", invocation="i1"),
                _attributed(_root_patch(), recipe_id="r.one", invocation="i2"),
            ],
            _descriptors(("r.one", ())),
        )
    diagnostic = exc.value.diagnostics[0]
    assert diagnostic.recipe_ids == ("r.one", "r.one")
    assert diagnostic.invocation_ids == ("i1", "i2")


def test_a_recipe_root_may_not_replace_a_direct_authored_root():
    with pytest.raises(RecipeError) as exc:
        compose(
            [_attributed(_root_patch())],
            _descriptors(("r.one", ())),
            direct_process_roots={PROCESS: parse_process_ir_v1(_linear_root())},
        )
    diagnostic = exc.value.diagnostics[0]
    assert diagnostic.recipe_ids[0] == DIRECT_AUTHORING_PRODUCER
    assert diagnostic.recipe_ids[1] == "r.one"


# ---------------------------------------------------------------------------
# Linear inserts
# ---------------------------------------------------------------------------


def test_an_insert_lands_before_the_target_stop_pair_never_between_them():
    """A step between a target and its stop is a step on no path."""
    composed = compose(
        [_attributed(_root_patch()), _attributed(_insert_patch())],
        _descriptors(("r.one", ("insert_root_linear_step",))),
    )
    kinds = [s.kind for s in dict(composed.process_roots)[PROCESS].body.steps]
    assert kinds == ["source", "map_ref", "message", "target", "stop"]


def test_an_insert_lands_before_a_terminal_control():
    composed = compose(
        [_attributed(_root_patch(with_branch=True)), _attributed(_insert_patch())],
        _descriptors(("r.one", ("insert_root_linear_step",))),
    )
    kinds = [s.kind for s in dict(composed.process_roots)[PROCESS].body.steps]
    assert kinds == ["source", "map_ref", "message", "branch"]


def test_an_insert_with_no_root_is_a_target_not_found():
    with pytest.raises(RecipeError) as exc:
        compose([_attributed(_insert_patch())], _descriptors(("r.one", ())))
    diagnostic = exc.value.diagnostics[0]
    assert diagnostic.code == RECIPE_PATCH_TARGET_NOT_FOUND
    assert diagnostic.target == "root"


def test_two_inserts_conflict_unless_both_declare_the_merge_rule():
    contributions = [
        _attributed(_root_patch(), recipe_id="r.one", invocation="i1"),
        _attributed(_insert_patch(operation_id="op.a"), recipe_id="r.one", invocation="i1"),
        _attributed(
            _insert_patch(operation_id="op.b", label="Second"),
            recipe_id="r.two",
            invocation="i2",
        ),
    ]
    # r.two does NOT declare the rule -> conflict.
    with pytest.raises(RecipeError) as exc:
        compose(
            contributions,
            _descriptors(("r.one", ("insert_root_linear_step",)), ("r.two", ())),
        )
    assert exc.value.diagnostics[0].code == RECIPE_PATCH_CONFLICT

    # Both declare it -> both inserts land, in normalized contributor order.
    composed = compose(
        contributions,
        _descriptors(
            ("r.one", ("insert_root_linear_step",)),
            ("r.two", ("insert_root_linear_step",)),
        ),
    )
    kinds = [s.kind for s in dict(composed.process_roots)[PROCESS].body.steps]
    assert kinds == ["source", "map_ref", "message", "message", "target", "stop"]


# ---------------------------------------------------------------------------
# Branch legs
# ---------------------------------------------------------------------------


def test_a_leg_appends_to_the_terminal_branch():
    composed = compose(
        [_attributed(_root_patch(with_branch=True)), _attributed(_leg_patch())],
        _descriptors(("r.one", ("append_root_terminal_leg",))),
    )
    branch = dict(composed.process_roots)[PROCESS].body.steps[-1]
    assert branch.kind == "branch"
    assert len(branch.legs) == 3


def test_appending_a_leg_when_the_terminal_is_not_a_branch_is_target_not_found():
    with pytest.raises(RecipeError) as exc:
        compose(
            [_attributed(_root_patch()), _attributed(_leg_patch())],
            _descriptors(("r.one", ("append_root_terminal_leg",))),
        )
    diagnostic = exc.value.diagnostics[0]
    assert diagnostic.code == RECIPE_PATCH_TARGET_NOT_FOUND
    assert diagnostic.target == "root.terminal.branch.legs"


def test_appending_a_leg_with_no_root_at_all_is_target_not_found():
    with pytest.raises(RecipeError) as exc:
        compose([_attributed(_leg_patch())], _descriptors(("r.one", ())))
    assert exc.value.diagnostics[0].code == RECIPE_PATCH_TARGET_NOT_FOUND


def test_two_leg_appenders_conflict_unless_both_declare_the_rule():
    contributions = [
        _attributed(_root_patch(with_branch=True), recipe_id="r.one", invocation="i1"),
        _attributed(_leg_patch(operation_id="op.a", index=1), recipe_id="r.one", invocation="i1"),
        _attributed(_leg_patch(operation_id="op.b", index=2), recipe_id="r.two", invocation="i2"),
    ]
    with pytest.raises(RecipeError):
        compose(
            contributions,
            _descriptors(("r.one", ("append_root_terminal_leg",)), ("r.two", ())),
        )
    composed = compose(
        contributions,
        _descriptors(
            ("r.one", ("append_root_terminal_leg",)),
            ("r.two", ("append_root_terminal_leg",)),
        ),
    )
    assert len(dict(composed.process_roots)[PROCESS].body.steps[-1].legs) == 4


def test_the_final_branch_cardinality_is_still_enforced_by_the_model():
    """25 is the platform bound; the composer does not get to exceed it."""
    contributions = [
        _attributed(_root_patch(with_branch=True), recipe_id="r.one", invocation="i1")
    ]
    for i in range(24):
        contributions.append(
            _attributed(
                _leg_patch(operation_id=f"op.{i}", index=i),
                recipe_id="r.one",
                invocation=f"i{i}",
            )
        )
    with pytest.raises(Exception) as exc:
        compose(contributions, _descriptors(("r.one", ("append_root_terminal_leg",))))
    # A ProcessIR validation failure on re-parse, not a composer conflict.
    assert not isinstance(exc.value, RecipeError)


# ---------------------------------------------------------------------------
# Ordering
# ---------------------------------------------------------------------------


def _invocation(descriptor, invocation_id):
    return RecipeInvocationV1(
        invocation_id=invocation_id,
        descriptor=descriptor,
        validated_input=SyncRecipeInputV1(
            version="1",
            process_key="p",
            source_connection_ref="$ref:a",
            source_operation_ref="$ref:b",
            map_ref="$ref:m",
            target_connection_ref="$ref:c",
            target_operation_ref="$ref:d",
            component_slots=(
                {
                    "contribution_id": "c.0",
                    "component_key": "k",
                    "component_type": "process",
                    "materialization_mode": "create",
                    "materializer_slot": "s.k",
                },
            ),
        ),
    )


def test_invocation_order_is_independent_of_input_order():
    descriptors = _descriptors(("r.a", ()), ("r.b", ()), ("r.c", ()))
    invocations = [
        _invocation(descriptors[("r.c", "1.0.0")], "i3"),
        _invocation(descriptors[("r.a", "1.0.0")], "i1"),
        _invocation(descriptors[("r.b", "1.0.0")], "i2"),
    ]
    forward = [i.invocation_id for i in order_invocations(invocations)]
    backward = [i.invocation_id for i in order_invocations(list(reversed(invocations)))]
    assert forward == backward == ["i1", "i2", "i3"]


def test_ties_break_on_recipe_id_then_semver_then_invocation_id():
    registry = build_test_registry(
        (
            RecipeRegistrationV1(
                recipe_id="r.a",
                recipe_version="1.10.0",
                entry_kind="executable_recipe",
                is_default=True,
                input_model=SyncRecipeInputV1,
                executor=emit_api_to_api_sync,
                output_types=("process_ir_patch",),
                conflict_policy=RecipeConflictPolicyV1(),
            ),
            RecipeRegistrationV1(
                recipe_id="r.a",
                recipe_version="1.9.0",
                entry_kind="executable_recipe",
                is_default=False,
                input_model=SyncRecipeInputV1,
                executor=emit_api_to_api_sync,
                output_types=("process_ir_patch",),
                conflict_policy=RecipeConflictPolicyV1(),
            ),
        )
    )
    older = registry.resolve("r.a", "1.9.0")
    newer = registry.resolve("r.a", "1.10.0")
    ordered = order_invocations(
        [_invocation(newer, "z"), _invocation(older, "a")]
    )
    assert [i.descriptor.recipe_version for i in ordered] == ["1.9.0", "1.10.0"]


def test_a_declared_dependency_orders_before_its_dependent():
    registry = build_test_registry(
        (
            RecipeRegistrationV1(
                recipe_id="r.base",
                recipe_version="1.0.0",
                entry_kind="executable_recipe",
                is_default=True,
                input_model=SyncRecipeInputV1,
                executor=emit_api_to_api_sync,
                output_types=("process_ir_patch",),
                conflict_policy=RecipeConflictPolicyV1(),
            ),
            RecipeRegistrationV1(
                # Sorts FIRST alphabetically, so only the dependency edge can
                # put it second — a tie-break alone would not.
                recipe_id="r.addon",
                recipe_version="1.0.0",
                entry_kind="executable_recipe",
                is_default=True,
                input_model=SyncRecipeInputV1,
                executor=emit_api_to_api_sync,
                output_types=("process_ir_patch",),
                prerequisites=(
                    RecipeDependencyV1(
                        kind="recipe", recipe_id="r.base", recipe_version="1.0.0"
                    ),
                ),
                conflict_policy=RecipeConflictPolicyV1(),
            ),
        )
    )
    ordered = order_invocations(
        [
            _invocation(registry.resolve("r.addon"), "i2"),
            _invocation(registry.resolve("r.base"), "i1"),
        ]
    )
    assert [i.descriptor.recipe_id for i in ordered] == ["r.base", "r.addon"]


def test_the_root_phase_precedes_every_insert_regardless_of_contributor_order():
    """The phase sequence is what makes "insert" well-defined.

    The root arrives SECOND here; without phases the insert would report a
    target-not-found that a different ordering would not.
    """
    composed = compose(
        [
            _attributed(_insert_patch(), recipe_id="r.one", invocation="i1"),
            _attributed(_root_patch(), recipe_id="r.one", invocation="i1"),
        ],
        _descriptors(("r.one", ("insert_root_linear_step",))),
    )
    kinds = [s.kind for s in dict(composed.process_roots)[PROCESS].body.steps]
    assert kinds == ["source", "map_ref", "message", "target", "stop"]


def test_distinct_processes_compose_independently():
    composed = compose(
        [
            _attributed(_root_patch(process="a_process"), invocation="i1"),
            _attributed(_root_patch(process="b_process"), invocation="i2"),
        ],
        _descriptors(("r.one", ())),
    )
    assert {key for key, _ in composed.process_roots} == {"a_process", "b_process"}


def test_the_merge_policy_is_read_per_version_not_per_recipe_id():
    """Two versions of one recipe must not share a merge policy.

    Driven through ``run_recipes``, NOT through ``compose`` directly. The first
    version of this test built the descriptor map itself — already tuple-keyed —
    so it never executed the engine line the bug was on, and reverting that half
    moved zero tests. QA caught it (issue #145). Here the ENGINE builds the map.

    Both directions are asserted, because a broken lookup produces a conflict in
    one of them by accident: ``rule/rule`` must MERGE, and that is the direction
    an id-keyed map silently breaks.
    """
    from boomi_mcp.recipes import (
        MaterializationCatalog,
        RecipeRequestV1,
        run_recipes,
    )

    def registry_for(v2_rules):
        return build_test_registry(
            (
                RecipeRegistrationV1(
                    recipe_id="r.multi",
                    recipe_version="1.0.0",
                    entry_kind="constraint_only",
                    is_default=True,
                    input_model=SyncRecipeInputV1,
                    executor=_shared_constraint_executor,
                    output_types=("constraint_requirement",),
                    conflict_policy=RecipeConflictPolicyV1(
                        merge_rules=("dedupe_identical_constraint",)
                    ),
                ),
                RecipeRegistrationV1(
                    recipe_id="r.multi",
                    recipe_version="2.0.0",
                    entry_kind="constraint_only",
                    is_default=False,
                    input_model=SyncRecipeInputV1,
                    executor=_shared_constraint_executor,
                    output_types=("constraint_requirement",),
                    conflict_policy=RecipeConflictPolicyV1(merge_rules=v2_rules),
                ),
            )
        )

    def run(v2_rules):
        return run_recipes(
            [
                RecipeRequestV1(
                    recipe_id="r.multi",
                    recipe_version="1.0.0",
                    invocation_id="i1",
                    raw_input=_ENGINE_INPUT,
                ),
                RecipeRequestV1(
                    recipe_id="r.multi",
                    recipe_version="2.0.0",
                    invocation_id="i2",
                    raw_input=_ENGINE_INPUT,
                ),
            ],
            catalog=MaterializationCatalog({}),
            registry=registry_for(v2_rules),
        )

    # v2 does NOT declare the rule -> the identical constraints must conflict.
    with pytest.raises(RecipeError) as exc:
        run(())
    assert exc.value.diagnostics[0].code == RECIPE_PATCH_CONFLICT
    assert exc.value.diagnostics[0].recipe_versions == ("1.0.0", "2.0.0")

    # BOTH declare it -> they dedupe. An id-keyed map breaks THIS direction.
    result = run(("dedupe_identical_constraint",))
    assert len(result.composed.constraints) == 1


_ENGINE_INPUT = {
    "version": "1",
    "process_key": "p",
    "source_connection_ref": "$ref:a",
    "source_operation_ref": "$ref:b",
    "map_ref": "$ref:m",
    "target_connection_ref": "$ref:c",
    "target_operation_ref": "$ref:d",
    "component_slots": [
        {
            "contribution_id": "c.0",
            "component_key": "k",
            "component_type": "process",
            "materialization_mode": "create",
            "materializer_slot": "s.k",
        }
    ],
}


def _shared_constraint_executor(inp):
    """One identical requirement, so BOTH versions write the same slot."""
    return (
        parse_recipe_contribution(
            {
                "contribution_kind": "constraint_requirement",
                "version": "1",
                "requirement_id": "req.shared",
                "requirement": {
                    "kind": "capability",
                    "authority": "process_ir",
                    "subject": "rich_branch_decision_bodies",
                    "required_state": "supported",
                },
            }
        ),
    )
