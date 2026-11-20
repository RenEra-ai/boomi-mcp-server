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
from boomi_mcp.recipes import RecipeError
from boomi_mcp.recipes.registry import build_test_registry
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
    with pytest.raises(RecipeError) as exc:
        compose(contributions, _descriptors(("r.one", ("append_root_terminal_leg",))))

    # The cardinality is enforced by the MODEL on re-parse — the composer never
    # counts legs itself — but what leaves the recipe layer is a recipe
    # diagnostic. This test previously pinned the opposite (`not isinstance(...,
    # RecipeError)`), which is how the taxonomy hole survived: ADR §8 says a
    # canonical rejection never escapes wearing a `PROCESS_IR_*` code, and
    # `compose` runs BEFORE the engine's `_compile_processes` wrapper, so this
    # was the one path that contradicted it (issue #145, §6 architect review).
    diagnostic = exc.value.diagnostics[0]
    assert diagnostic.code == "RECIPE_CONSTRAINT_FAILED"
    assert diagnostic.phase == "composition"
    assert diagnostic.target == f"process:{PROCESS}"
    # The canonical code rides along as a value-free cause rather than being
    # swallowed — the caller still learns WHICH rule rejected the result.
    assert diagnostic.cause_codes
    assert all(code.startswith("PROCESS_IR_") for code in diagnostic.cause_codes)


def test_a_composed_root_that_fails_reparse_never_escapes_as_a_canonical_error():
    """The taxonomy claim, asserted against the exception TYPE that escapes.

    Distinct from the cardinality test above: that one pins the diagnostic shape
    for one known rejection, this one pins that NO canonical exception type can
    leave ``compose`` at all. If a future edit moves the re-parse outside the
    translation, the cardinality test could still pass on a different rule while
    this one fails.
    """
    from boomi_mcp.models.process_ir import ProcessIRValidationError

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
    try:
        compose(contributions, _descriptors(("r.one", ("append_root_terminal_leg",))))
    except ProcessIRValidationError as exc:  # pragma: no cover - the defect
        raise AssertionError(
            f"a canonical ProcessIR error escaped the recipe layer: {type(exc).__name__}"
        )
    except RecipeError:
        pass
    else:  # pragma: no cover - the defect
        raise AssertionError("25 legs must be rejected")


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


def test_a_newly_ready_invocation_beats_a_later_independent_one():
    """The ready set is re-sorted after every placement, not swept once.

    This is the ONE graph shape that tells the two deterministic algorithms
    apart, which is why the difference survived a suite that already asserted
    determinism and input-order independence — both hold for either.

    ``r.a`` depends on ``r.b``; ``r.c`` depends on nothing. Sorted-ready Kahn
    places ``r.b``, at which point ``r.a`` becomes ready and is smaller than
    ``r.c``, giving ``b, a, c``. A single sorted sweep gives ``b, c, a``: having
    passed ``r.a`` while it was blocked, the sweep reaches ``r.c`` before it ever
    looks back. Invocation order decides contribution order, which decides
    component order and leg-append order, so this reaches the emitted bytes
    (issue #145, §6 architect review).
    """
    registry = build_test_registry(
        (
            RecipeRegistrationV1(
                recipe_id="r.b",
                recipe_version="1.0.0",
                entry_kind="executable_recipe",
                is_default=True,
                input_model=SyncRecipeInputV1,
                executor=emit_api_to_api_sync,
                output_types=("process_ir_patch",),
                conflict_policy=RecipeConflictPolicyV1(),
            ),
            RecipeRegistrationV1(
                recipe_id="r.a",
                recipe_version="1.0.0",
                entry_kind="executable_recipe",
                is_default=True,
                input_model=SyncRecipeInputV1,
                executor=emit_api_to_api_sync,
                output_types=("process_ir_patch",),
                prerequisites=(
                    RecipeDependencyV1(
                        kind="recipe", recipe_id="r.b", recipe_version="1.0.0"
                    ),
                ),
                conflict_policy=RecipeConflictPolicyV1(),
            ),
            RecipeRegistrationV1(
                recipe_id="r.c",
                recipe_version="1.0.0",
                entry_kind="executable_recipe",
                is_default=True,
                input_model=SyncRecipeInputV1,
                executor=emit_api_to_api_sync,
                output_types=("process_ir_patch",),
                conflict_policy=RecipeConflictPolicyV1(),
            ),
        )
    )
    invocations = [
        _invocation(registry.resolve("r.a"), "i1"),
        _invocation(registry.resolve("r.b"), "i2"),
        _invocation(registry.resolve("r.c"), "i3"),
    ]
    assert [i.descriptor.recipe_id for i in order_invocations(invocations)] == [
        "r.b",
        "r.a",
        "r.c",
    ]
    # ...and still independent of how the caller assembled the list.
    assert [
        i.descriptor.recipe_id for i in order_invocations(list(reversed(invocations)))
    ] == ["r.b", "r.a", "r.c"]


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


def test_an_internal_bug_in_the_process_parser_propagates_as_itself():
    """Same narrowing, same reason, on the assembled-root re-parse.

    Translating a canonical rejection is the contract; swallowing a ``TypeError``
    would report a constraint failure with no cause and send the reader looking
    for a rule that never fired (issue #145, live QA).
    """
    from unittest.mock import patch

    contributions = [
        _attributed(_root_patch(), recipe_id="r.one", invocation="i1")
    ]
    with patch(
        "boomi_mcp.models.process_ir.parse_process_ir_v1",
        side_effect=RuntimeError("internal bug"),
    ):
        with pytest.raises(RuntimeError, match="internal bug"):
            compose(contributions, _descriptors(("r.one", ())))


# ---------------------------------------------------------------------------
# #159 S1: process units in the shared component-key namespace
# ---------------------------------------------------------------------------


def _unit_contribution(
    key=PROCESS, contribution_id="c.unit", with_branch=False, depends_on=(), name="Unit"
):
    return parse_recipe_contribution(
        {
            "contribution_kind": "process_unit",
            "version": "1",
            "contribution_id": contribution_id,
            "unit": {
                "envelope": {
                    "component_key": key,
                    "name": name,
                    "action": "create",
                    "folder_name": "Unit Folder",
                    "description": "a recipe-owned root",
                    "depends_on": list(depends_on),
                    "process_extensions": {
                        "connections": [
                            {
                                "connection_id": "$ref:src_conn",
                                "fields": [{"id": "url", "label": "URL"}],
                            }
                        ]
                    },
                },
                "process_ir": _linear_root(with_branch),
            },
        }
    )


def _slot_contribution(key, slot="slot.k", component_type="process"):
    return parse_recipe_contribution(
        {
            "contribution_kind": "component_contribution",
            "version": "1",
            "contribution_id": "c.slot",
            "component_key": key,
            "component_type": component_type,
            "materialization_mode": "create",
            "materializer_slot": slot,
        }
    )


_UNIT_NAMESPACE_CASES = {
    # two units claiming one key
    "duplicate_unit": dict(
        attributed=lambda: [
            _attributed(_unit_contribution(), recipe_id="r.one", invocation="i1"),
            _attributed(_unit_contribution(), recipe_id="r.two", invocation="i2"),
        ],
        expected=(("r.one", "r.two"), ("i1", "i2")),
        target=f"process:{PROCESS}/root",
    ),
    # a unit, then a set_process_root for its key
    "unit_vs_set_root": dict(
        attributed=lambda: [
            _attributed(_unit_contribution(), recipe_id="r.one", invocation="i1"),
            _attributed(_root_patch(), recipe_id="r.two", invocation="i2"),
        ],
        expected=(("r.one", "r.two"), ("i1", "i2")),
        target=f"process:{PROCESS}/root",
    ),
    # the reverse order: the root patch is the FIRST writer
    "set_root_vs_unit": dict(
        attributed=lambda: [
            _attributed(_root_patch(), recipe_id="r.two", invocation="i1"),
            _attributed(_unit_contribution(), recipe_id="r.one", invocation="i2"),
        ],
        expected=(("r.two", "r.one"), ("i1", "i2")),
        target=f"process:{PROCESS}/root",
    ),
    # a unit claiming a direct-authored root's key
    "unit_vs_direct_root": dict(
        attributed=lambda: [
            _attributed(_unit_contribution(), recipe_id="r.one", invocation="i1"),
        ],
        direct=True,
        expected=((DIRECT_AUTHORING_PRODUCER, "r.one"), ("", "i1")),
        target=f"process:{PROCESS}/root",
    ),
    # a unit and a component slot for one key (shared namespace)
    "unit_vs_slot": dict(
        attributed=lambda: [
            _attributed(_slot_contribution(PROCESS), recipe_id="r.two", invocation="i1"),
            _attributed(_unit_contribution(), recipe_id="r.one", invocation="i2"),
        ],
        expected=(("r.two", "r.one"), ("i1", "i2")),
        target=f"component:{PROCESS}",
    ),
    # a unit naming an existing catalog component no slot contributed
    "unit_vs_catalog_key": dict(
        attributed=lambda: [
            _attributed(_unit_contribution(), recipe_id="r.one", invocation="i1"),
        ],
        catalog_keys=(PROCESS, "unrelated"),
        expected=((DIRECT_AUTHORING_PRODUCER, "r.one"), ("", "i1")),
        target=f"component:{PROCESS}",
    ),
}


@pytest.mark.parametrize("case", sorted(_UNIT_NAMESPACE_CASES))
def test_direct_unit_namespace_collisions_are_conflicts(case):
    spec = _UNIT_NAMESPACE_CASES[case]
    with pytest.raises(RecipeError) as exc:
        compose(
            spec["attributed"](),
            _descriptors(("r.one", ()), ("r.two", ())),
            direct_process_roots=(
                {PROCESS: parse_process_ir_v1(_linear_root())} if spec.get("direct") else None
            ),
            catalog_keys=spec.get("catalog_keys", ()),
        )
    (diagnostic,) = exc.value.diagnostics
    assert diagnostic.code == RECIPE_PATCH_CONFLICT
    assert diagnostic.phase == "composition"
    assert diagnostic.target == spec["target"]
    assert (diagnostic.recipe_ids, diagnostic.invocation_ids) == spec["expected"]


def test_the_namespace_rules_bind_unit_keys_only():
    """The legacy shape — a ``set_process_root`` sharing its key with a ``process``
    slot, and a catalog holding that key — stays legal beside an unrelated unit.
    Without this control a rule widened to every root would pass the conflict
    cases above while breaking every sync preset."""
    composed = compose(
        [
            _attributed(_slot_contribution(PROCESS), invocation="i1", index=0),
            _attributed(_root_patch(), invocation="i1", index=1),
            _attributed(_unit_contribution(key="other_process"), invocation="i1", index=2),
        ],
        _descriptors(("r.one", ())),
        catalog_keys=(PROCESS,),
    )
    # Unit roots are established before ``set_process_root`` roots (phase 1a).
    assert [key for key, _ in composed.process_roots] == ["other_process", PROCESS]
    assert [key for key, _ in composed.process_units] == ["other_process"]
    assert [item.contribution.component_key for item in composed.component_slots] == [PROCESS]


def test_root_patches_apply_to_a_unit_ir_and_preserve_its_envelope():
    """A unit's root is patched exactly like a ``set_process_root`` root, and its
    envelope is carried through byte for byte."""
    rules = ("append_root_terminal_leg", "insert_root_linear_step")
    unit = _unit_contribution(with_branch=True, depends_on=("child_process",))
    via_unit = compose(
        [
            _attributed(unit, index=0),
            _attributed(_insert_patch(), index=1),
            _attributed(_leg_patch(), index=2),
        ],
        _descriptors(("r.one", rules)),
    )
    via_root = compose(
        [
            _attributed(_root_patch(with_branch=True), index=0),
            _attributed(_insert_patch(), index=1),
            _attributed(_leg_patch(), index=2),
        ],
        _descriptors(("r.one", rules)),
    )
    (key, composed_unit), = via_unit.process_units
    assert key == PROCESS
    root = dict(via_unit.process_roots)[PROCESS]
    # The unit's root IS the composed root, with both patches applied...
    assert composed_unit.process_ir.model_dump(mode="json") == root.model_dump(mode="json")
    assert [s.kind for s in root.body.steps] == ["source", "map_ref", "message", "branch"]
    assert len(root.body.steps[-1].legs) == 3
    # ...in the same order a set_process_root root gets them.
    assert root.model_dump(mode="json") == dict(via_root.process_roots)[PROCESS].model_dump(
        mode="json"
    )
    # The envelope is the contributed one, unchanged.
    assert composed_unit.envelope.model_dump(mode="json") == unit.unit.envelope.model_dump(
        mode="json"
    )
    assert composed_unit.envelope.depends_on == ("child_process",)
    assert via_unit.process_unit_contributions[0].contribution is unit


def test_a_recorded_intent_may_target_a_unit_root():
    """Recorded intents check ownership against the composed roots, units included."""
    recorded = parse_recipe_contribution(
        {
            "contribution_kind": "recorded_intent",
            "version": "1",
            "intent_id": "hint.a",
            "process_key": PROCESS,
            "status": "recorded_not_wired",
            "declaration": {"declaration_kind": "runtime_hint", "hint_kind": "note", "value": "x"},
        }
    )
    composed = compose(
        [_attributed(_unit_contribution(), index=0), _attributed(recorded, index=1)],
        _descriptors(("r.one", ())),
    )
    assert [item.contribution.intent_id for item in composed.recorded_intents] == ["hint.a"]


# ---------------------------------------------------------------------------
# #159 S1: process units and supporting components through the ENGINE
# ---------------------------------------------------------------------------


def _unit_executor(inp):
    """Emit one process unit whose root reads ``conn``/``op`` and declares both edges."""
    return (
        parse_recipe_contribution(
            {
                "contribution_kind": "process_unit",
                "version": "1",
                "contribution_id": "c.unit",
                "unit": {
                    "envelope": {
                        "component_key": inp.process_key,
                        "name": "Recipe unit",
                        "action": "create",
                        "folder_name": "Recipe Folder",
                        "depends_on": ["conn", "op"],
                    },
                    "process_ir": {
                        "version": "1",
                        "body": {
                            "kind": "sequence",
                            "steps": [
                                {
                                    "kind": "source",
                                    "connection_ref": "$ref:conn",
                                    "operation_ref": "$ref:op",
                                },
                                {"kind": "message", "text": "hello"},
                                {"kind": "return_documents"},
                            ],
                        },
                    },
                },
            }
        ),
    )


def _unit_registry():
    return build_test_registry(
        (
            RecipeRegistrationV1(
                recipe_id="r.unit",
                recipe_version="1.0.0",
                entry_kind="executable_recipe",
                is_default=True,
                input_model=SyncRecipeInputV1,
                executor=_unit_executor,
                output_types=("process_unit",),
                conflict_policy=RecipeConflictPolicyV1(),
            ),
        )
    )


def _supporting_specs(*overrides):
    import copy as _copy

    sys.path.insert(0, str(Path(__file__).resolve().parent))
    from _m12_11_support import APPLIABLE_CONN, APPLIABLE_OP

    from boomi_mcp.models.integration_models import IntegrationComponentSpec

    specs = [_copy.deepcopy(APPLIABLE_CONN), _copy.deepcopy(APPLIABLE_OP)]
    specs.extend(_copy.deepcopy(item) for item in overrides)
    return [IntegrationComponentSpec(**spec) for spec in specs]


def _run_unit(supporting, process_key="p"):
    from boomi_mcp.recipes import MaterializationCatalog, RecipeRequestV1, run_recipes

    raw = dict(_ENGINE_INPUT, process_key=process_key)
    return run_recipes(
        [RecipeRequestV1(recipe_id="r.unit", invocation_id="i1", raw_input=raw)],
        catalog=MaterializationCatalog({}),
        registry=_unit_registry(),
        supporting_components=supporting,
        connector_metadata={"conn": ("rest", None), "op": ("rest", "GET")},
    )


def test_run_recipes_compiles_a_recipe_emitted_unit_against_supporting_components():
    supporting = _supporting_specs()
    result = _run_unit(supporting)
    (unit,) = result.process_units
    assert unit.envelope.component_key == "p"
    assert unit.envelope.folder_name == "Recipe Folder"
    assert unit.envelope.depends_on == ("conn", "op")
    assert [key for key, _artifact in result.process_artifacts] == ["p"]
    # ``components`` still means the resolved catalog slots — none here — and the
    # trusted supporting specs ride beside them, unchanged.
    assert result.components == ()
    assert result.supporting_components == tuple(supporting)


def test_the_engine_hands_recipe_units_to_the_write_conflict_check(monkeypatch):
    """A unit's envelope (update / create-by-name) makes it a write participant."""
    from boomi_mcp.recipes import engine

    seen = {}
    real = engine.build_symbol_table

    def spy(components, **kwargs):
        seen["processes"] = tuple(kwargs.get("processes", ()))
        seen["process_keys"] = tuple(kwargs.get("process_keys", ()))
        return real(components, **kwargs)

    monkeypatch.setattr(engine, "build_symbol_table", spy)
    result = _run_unit(_supporting_specs())
    assert seen["processes"] == result.process_units
    assert seen["process_keys"] == ("p",)


def test_a_unit_may_not_claim_a_supporting_component_key():
    spare = {
        "key": "spare",
        "type": "connector-settings",
        "name": "Spare conn",
        "action": "create",
        "config": {"connector_type": "rest", "base_url": "https://spare.example.invalid"},
    }
    with pytest.raises(RecipeError) as exc:
        _run_unit(_supporting_specs(spare), process_key="spare")
    (diagnostic,) = exc.value.diagnostics
    assert diagnostic.code == RECIPE_PATCH_CONFLICT
    assert diagnostic.target == "component:spare"
    assert diagnostic.recipe_ids == (DIRECT_AUTHORING_PRODUCER, "r.unit")


def test_two_specs_for_one_component_key_are_refused():
    from boomi_mcp.errors import RECIPE_CONSTRAINT_FAILED

    duplicate = {
        "key": "op",
        "type": "connector-action",
        "name": "Other op",
        "action": "create",
        "config": {"connector_type": "rest", "connection_ref_key": "conn"},
    }
    with pytest.raises(RecipeError) as exc:
        _run_unit(_supporting_specs(duplicate))
    (diagnostic,) = exc.value.diagnostics
    assert diagnostic.code == RECIPE_CONSTRAINT_FAILED
    assert diagnostic.target == "component:op"
    assert diagnostic.cause_codes == ("INTEGRATION_COMPONENT_KEY_DUPLICATE",)
    # Value-free: the diagnostic names the key, never a spec's content.
    assert "Other op" not in str(exc.value) and "Other op" not in repr(diagnostic)


# ---------------------------------------------------------------------------
# #159 S1 (Q5): two recipe roots, one calling the other, through apply
# ---------------------------------------------------------------------------

#: The callee's process key. The CALLER's executor names it, exactly as a real
#: recipe would name a sibling root it was handed.
_CALLEE = "callee_process"


def _caller_unit_executor(inp):
    """A scheduled (No Data) root over the base ``conn``/``op`` that calls the callee.

    One Branch leg reads through the shared operation; the other hands off to
    ``$ref:callee_process`` in a ``process_call``. The envelope declares the
    inter-root edge beside its component edges, at the one exempt position (D4).
    """
    return (_caller_unit(inp.process_key, ["conn", "op", _CALLEE]),)


def _caller_without_its_call_edge_executor(inp):
    """Deviation 10 control: the ``process_call`` IR edge is NOT declared."""
    return (_caller_unit(inp.process_key, ["conn", "op"]),)


def _caller_unit(process_key, depends_on):
    return parse_recipe_contribution(
            {
                "contribution_kind": "process_unit",
                "version": "1",
                "contribution_id": "c.caller",
                "unit": {
                    "envelope": {
                        "component_key": process_key,
                        "name": "Caller unit",
                        "action": "create",
                        "depends_on": list(depends_on),
                    },
                    "process_ir": {
                        "version": "1",
                        "body": {
                            "kind": "sequence",
                            "steps": [
                                {
                                    "kind": "branch",
                                    "label": "call the callee",
                                    "legs": [
                                        {
                                            "steps": [{"kind": "message", "text": "hand off"}],
                                            "terminal": {
                                                "kind": "process_call",
                                                "process_ref": "$ref:" + _CALLEE,
                                            },
                                        },
                                        {
                                            "steps": [
                                                {"kind": "connector_call", "operation_ref": "$ref:op"}
                                            ],
                                            "terminal": {"kind": "stop"},
                                        },
                                    ],
                                },
                            ],
                        },
                    },
                },
            }
        )


def _shared_slot(key, component_type):
    return parse_recipe_contribution(
        {
            "contribution_kind": "component_contribution",
            "version": "1",
            "contribution_id": "c.slot." + key,
            "component_key": key,
            "component_type": component_type,
            "materialization_mode": "create",
            "materializer_slot": key,
        }
    )


def _callee_unit_executor(inp):
    """A Data Passthrough child that reads through the SAME shared base operation.

    It OWNS the one catalog entry per shared component (the slot headers); the
    caller reads the same keys without contributing them, so no duplicate writer
    arises and no composer dedupe rule is needed (Q5). It declares ``conn``
    although its root names only ``op``: an ordering edge with NO IR token,
    which the planner must keep (deviation 10).
    """
    return _callee(inp.process_key, ["conn", "op"])


def _callee_depending_on_its_caller_executor(inp):
    """Deviation 10 control: a declared edge back to the caller closes a cycle."""
    return _callee(inp.process_key, ["conn", "op", "caller_process"])


def _callee(process_key, depends_on):
    return (
        _shared_slot("conn", "connector-settings"),
        _shared_slot("op", "connector-action"),
        parse_recipe_contribution(
            {
                "contribution_kind": "process_unit",
                "version": "1",
                "contribution_id": "c.callee",
                "unit": {
                    "envelope": {
                        "component_key": process_key,
                        "name": "Callee unit",
                        "action": "create",
                        "depends_on": list(depends_on),
                    },
                    "process_ir": {
                        "version": "1",
                        "body": {
                            "kind": "sequence",
                            "steps": [
                                {"kind": "passthrough", "label": "Receive"},
                                {"kind": "connector_call", "operation_ref": "$ref:op"},
                                {"kind": "stop"},
                            ],
                        },
                    },
                },
            }
        ),
    )


def _two_root_registry(caller=None, callee=None):
    caller = caller or _caller_unit_executor
    callee_executor = callee or _callee_unit_executor
    registration = dict(
        recipe_version="1.0.0",
        entry_kind="executable_recipe",
        is_default=True,
        input_model=SyncRecipeInputV1,
        output_types=("process_unit",),
        conflict_policy=RecipeConflictPolicyV1(),
    )
    callee = dict(registration, output_types=("component_contribution", "process_unit"))
    return build_test_registry(
        (
            RecipeRegistrationV1(recipe_id="r.callee", executor=callee_executor, **callee),
            RecipeRegistrationV1(recipe_id="r.caller", executor=caller, **registration),
        )
    )


def _two_root_request():
    from _m12_11_support import APPLIABLE_CONN, APPLIABLE_OP

    from boomi_mcp.models.authoring_workflow import AuthoringRequestV1

    def invocation(recipe_id, invocation_id, process_key):
        return {
            "recipe_id": recipe_id,
            "invocation_id": invocation_id,
            "raw_input": dict(_ENGINE_INPUT, process_key=process_key),
        }

    return AuthoringRequestV1.model_validate(
        {
            "intent": {
                "intent_kind": "recipe",
                "integration_name": "Q5 two roots",
                # The CALLER is listed first, so a callee-first write order is
                # the planner's dependency decision, not the request's order.
                "invocations": [
                    invocation("r.caller", "caller", "caller_process"),
                    invocation("r.callee", "callee", _CALLEE),
                ],
                "base_components": [APPLIABLE_CONN, APPLIABLE_OP],
            }
        }
    )


def test_two_recipe_roots_share_base_components_and_one_calls_the_other(monkeypatch):
    """Q5 in-suite: plan, compile and MATERIALIZE two recipe-emitted roots.

    Both roots read the same base connection and operation (one catalog entry
    each, no composer dedupe rule needed). The caller ends in a ``process_call``
    to the callee: apply must create the callee FIRST and bind the caller's
    emitted reference to the id the callee was actually assigned.
    """
    from unittest.mock import MagicMock, patch

    from boomi_mcp.authoring.workflow import compile_authoring_request_v1
    from boomi_mcp.categories import integration_builder
    from boomi_mcp.categories.integration_builder import (
        _BUILD_REGISTRY,
        build_integration_action,
    )
    from boomi_mcp.recipes import engine as engine_module

    registry = _two_root_registry()
    monkeypatch.setattr(engine_module, "production_registry", lambda: registry)
    monkeypatch.setattr(integration_builder, "paginate_metadata", lambda *a, **k: [])
    request = _two_root_request()

    compiled, internals = compile_authoring_request_v1(
        request, boomi_client=MagicMock(), profile="qa_profile"
    )
    assert compiled.validation_report.is_valid
    spec = internals.normalized.integration_spec
    assert sorted(u.envelope.component_key for u in spec.processes) == [
        _CALLEE,
        "caller_process",
    ]
    assert [c.key for c in spec.components].count("conn") == 1
    assert [c.key for c in spec.components].count("op") == 1
    edges = {(edge.component_key, edge.depends_on) for edge in compiled.component_dependencies}
    assert ("caller_process", _CALLEE) in edges
    assert {("caller_process", "op"), (_CALLEE, "op")} <= edges
    # the callee's root names only `op`; its declared `conn` edge is kept anyway
    assert (_CALLEE, "conn") in edges

    payload = request.model_dump(mode="json")
    payload["expected_capability_revision"] = compiled.revision_binding.capability_revision
    payload["expected_compile_hash"] = compiled.revision_binding.compile_hash

    created = []
    counter = {"n": 0}

    def _component(*_args, **_kwargs):
        counter["n"] += 1
        return {"_success": True, "component_id": "cid-%d" % counter["n"]}

    def _create(_client, _profile, payload_in, **_kwargs):
        process_id = "process-%d" % (len(created) + 1)
        created.append((process_id, payload_in["xml"]))
        return {"_success": True, "component_id": process_id}

    _BUILD_REGISTRY.clear()
    try:
        with patch.object(integration_builder, "_execute_component", side_effect=_component), patch.object(
            integration_builder, "create_component", side_effect=_create
        ), patch.object(
            integration_builder,
            "component_get_xml",
            side_effect=lambda _c, component_id, *a, **k: {
                "type": "process",
                "xml": dict(created).get(component_id, "<bns:Component/>"),
            },
        ):
            result = build_integration_action(
                MagicMock(),
                "qa_profile",
                "apply",
                config={"authoring_request": payload, "dry_run": False},
            )
    finally:
        _BUILD_REGISTRY.clear()

    assert result["_success"] is True, result.get("error")
    assert len(created) == 2
    (callee_id, callee_xml), (caller_id, caller_xml) = created
    # The callee was materialized first, and is NOT the caller.
    assert "hand off" in caller_xml and "hand off" not in callee_xml
    assert "Receive" in callee_xml
    # The caller's emitted reference is bound to the callee's ASSIGNED id.
    assert callee_id in caller_xml
    assert "$ref:" not in caller_xml
    assert caller_id not in callee_xml


@pytest.mark.parametrize(
    "caller,callee,cause",
    [
        pytest.param(
            _caller_without_its_call_edge_executor, None, "INTEGRATION_DEPENDENCY_REQUIRED",
            id="missing_required_ir_edge",
        ),
        pytest.param(
            None, _callee_depending_on_its_caller_executor, "INTEGRATION_DEPENDENCY_CYCLE",
            id="cyclic_declared_edges",
        ),
    ],
)
def test_a_missing_or_cyclic_unit_dependency_is_refused_before_any_write(
    monkeypatch, caller, callee, cause
):
    """Deviation 10: the planner reads DECLARED edges. A ``process_call`` whose
    edge the caller did not declare, and a declared edge that closes a cycle,
    are each refused at compile — so no binding exists that apply could act on,
    and no write door is reached."""
    from unittest.mock import MagicMock

    from _m12_11_support import MutationSpy

    from boomi_mcp.authoring.workflow import AuthoringWorkflowError, compile_authoring_request_v1
    from boomi_mcp.categories import integration_builder
    from boomi_mcp.recipes import engine as engine_module

    spy = MutationSpy().install(monkeypatch)
    registry = _two_root_registry(caller=caller, callee=callee)
    monkeypatch.setattr(engine_module, "production_registry", lambda: registry)
    monkeypatch.setattr(integration_builder, "paginate_metadata", lambda *a, **k: [])
    with pytest.raises(AuthoringWorkflowError) as caught:
        compile_authoring_request_v1(
            _two_root_request(), boomi_client=MagicMock(), profile="qa_profile"
        )
    assert caught.value.code == "AUTHORING_COMPILE_BLOCKED"
    assert [d.cause_codes for d in caught.value.diagnostics] == [(cause,)]
    assert spy.calls == []
