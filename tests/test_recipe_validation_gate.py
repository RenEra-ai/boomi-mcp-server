"""The non-bypassable validation funnel (issue #145 M12.10).

Acceptance criterion: "recipe output cannot bypass model, semantic,
component-plan, capability, or topology validation." That is a claim about which
functions run, in what order, and with what arguments — so these tests SPY on the
canonical chain rather than inferring it from a successful return.

Two properties get the most attention:

* ``compile_process_ir_v1`` is always called with ``validation_policy=None``, and
  ``run_recipes`` has no parameter through which one could be supplied. Structural,
  not conventional — asserted against the live signature.
* ``_build_plan["_success"]`` is not a verdict. A plan carrying a blocking
  ``error_*`` step must produce ZERO ``_execute_component`` calls.
"""

import inspect
import json
import sys
from pathlib import Path
from typing import List
from unittest.mock import MagicMock, patch

import pytest
from pydantic import ConfigDict

_project_root = Path(__file__).resolve().parent.parent
_src = str(_project_root / "src")
if _src not in sys.path:
    sys.path.insert(0, _src)
sys.path.insert(0, str(_project_root / "tests" / "patterns"))

from boomi_mcp.errors import RECIPE_CONSTRAINT_FAILED, RECIPE_OUTPUT_NONDETERMINISTIC
from boomi_mcp.models.integration_models import IntegrationSpecV1
from boomi_mcp.models.recipe_contributions import (
    ConstraintRequirementV1,
    RequireProcessV1,
    parse_recipe_contribution,
)
from boomi_mcp.recipes import (
    MaterializationCatalog,
    RecipeError,
    RecipeInputBase,
    RecipeRequestV1,
    run_recipes,
)
from boomi_mcp.recipes.contracts import RecipeConflictPolicyV1, RecipeRegistrationV1
from boomi_mcp.recipes.registry import build_test_registry
from boomi_mcp.recipes import engine as engine_module
from boomi_mcp.recipes.builtins.catalog import (
    RECIPE_API_TO_API_SYNC,
    RECIPE_DB_REST_FANOUT,
)
from boomi_mcp.recipes.builtins.sync import SyncRecipeInputV1
from boomi_mcp.recipes.contracts import RecipeConflictPolicyV1, RecipeRegistrationV1
from boomi_mcp.categories.integration_authoring import compose_archetypes_action
from boomi_mcp.patterns.recipe_bridge import build_catalog, run_fanout_recipe

from test_archetype_composition import _cache_links, _options, _parts

_PAGINATE = "boomi_mcp.categories.integration_builder.paginate_metadata"


def _composed_spec(links=None):
    options = dict(_options())
    if links:
        options["links"] = links
    response = compose_archetypes_action(parts=_parts(), options=options)
    assert response["_success"] is True
    return IntegrationSpecV1.model_validate(response["integration_spec"])


# ---------------------------------------------------------------------------
# The chain runs, in order, for every assembled process
# ---------------------------------------------------------------------------


def test_the_canonical_chain_fires_in_order_for_every_process(monkeypatch):
    calls = []

    import boomi_mcp.models.process_ir as ir_module
    import boomi_mcp.compiler.process_ir.pipeline as pipeline_module
    import boomi_mcp.compiler.process_ir.emitter_registry as emitter_module

    real_parse = ir_module.parse_process_ir_v1
    real_compile = pipeline_module.compile_process_ir_v1
    real_emit = emitter_module.emit_process

    def spy_parse(payload):
        calls.append("parse")
        return real_parse(payload)

    def spy_compile(ir, symbols, **kwargs):
        calls.append(("compile", kwargs.get("validation_policy", "MISSING")))
        return real_compile(ir, symbols, **kwargs)

    def spy_emit(plan, symbols, **kwargs):
        calls.append("emit")
        return real_emit(plan, symbols, **kwargs)

    monkeypatch.setattr(ir_module, "parse_process_ir_v1", spy_parse)
    monkeypatch.setattr(pipeline_module, "compile_process_ir_v1", spy_compile)
    monkeypatch.setattr(emitter_module, "emit_process", spy_emit)

    spec = _composed_spec()
    run_fanout_recipe(
        recipe_id=RECIPE_DB_REST_FANOUT,
        components=spec.components,
        process=spec.components[-1],
    )

    kinds = [c[0] if isinstance(c, tuple) else c for c in calls]
    assert "parse" in kinds
    assert "compile" in kinds
    assert "emit" in kinds
    assert kinds.index("compile") < kinds.index("emit")


def test_compile_is_always_called_with_no_validation_policy(monkeypatch):
    """The exemption is REACHABLE in the codebase — just not from here."""
    seen = []

    import boomi_mcp.compiler.process_ir.pipeline as pipeline_module

    real_compile = pipeline_module.compile_process_ir_v1

    def spy(ir, symbols, **kwargs):
        seen.append(kwargs.get("validation_policy", "MISSING"))
        return real_compile(ir, symbols, **kwargs)

    monkeypatch.setattr(pipeline_module, "compile_process_ir_v1", spy)

    for links in (None, _cache_links("orders", "billing")):
        spec = _composed_spec(links)
        run_fanout_recipe(
            recipe_id=RECIPE_DB_REST_FANOUT,
            components=spec.components,
            process=spec.components[-1],
        )

    assert seen
    assert all(policy is None for policy in seen), seen


def test_run_recipes_exposes_no_seam_for_a_validation_policy():
    """Structural: there is no parameter through which one could be passed."""
    signature = inspect.signature(run_recipes)
    for name in signature.parameters:
        assert "policy" not in name, name
        assert "exempt" not in name, name

    source = Path(engine_module.__file__).read_text()
    assert "validation_policy=None" in source
    # ...and the ONLY mention is that pinned call site plus its explanation.
    assignments = [
        line
        for line in source.splitlines()
        if "validation_policy=" in line and "validation_policy=None" not in line
    ]
    assert assignments == [], assignments


def test_the_recipe_engine_never_imports_the_legacy_policy_registry():
    source = Path(engine_module.__file__).read_text()
    assert "validation_policy import" not in source
    assert "lookup_policy" not in source


def test_the_verifier_runs_and_reports_no_errors():
    spec = _composed_spec(_cache_links("orders", "billing"))
    result = run_fanout_recipe(
        recipe_id=RECIPE_DB_REST_FANOUT,
        components=spec.components,
        process=spec.components[-1],
    )
    artifact = result.artifact_for(spec.components[-1].key)
    assert artifact.verifier.errors == ()
    assert artifact.verifier.shapes_checked > 0


# ---------------------------------------------------------------------------
# Determinism
# ---------------------------------------------------------------------------


_COUNTER = {"n": 0}


def _nondeterministic_executor(inp):
    """Deliberately impure: a different requirement id on every call."""
    _COUNTER["n"] += 1
    return (
        parse_recipe_contribution(
            {
                "contribution_kind": "constraint_requirement",
                "version": "1",
                "requirement_id": f"req.n{_COUNTER['n']}",
                "requirement": {"kind": "process", "process_key": "p"},
            }
        ),
    )


_RUN_LOG = []


def _counting_executor(inp):
    _RUN_LOG.append(inp.process_key)
    return _deterministic_executor(inp)


def _deterministic_executor(inp):
    """A satisfiable requirement, so the run reaches the determinism check.

    A capability requirement rather than a process one: this registry emits no
    process root, and an unsatisfiable constraint would fail the run before the
    property under test was ever exercised.
    """
    return (
        parse_recipe_contribution(
            {
                "contribution_kind": "constraint_requirement",
                "version": "1",
                "requirement_id": "req.stable",
                "requirement": {
                    "kind": "capability",
                    "authority": "process_ir",
                    "subject": "rich_branch_decision_bodies",
                    "required_state": "supported",
                },
            }
        ),
    )


_VALID_INPUT = {
    "version": "1",
    "process_key": "main_process",
    "source_connection_ref": "$ref:a",
    "source_operation_ref": "$ref:b",
    "map_ref": "$ref:m",
    "target_connection_ref": "$ref:c",
    "target_operation_ref": "$ref:d",
    "component_slots": [
        {
            "contribution_id": "c.0",
            "component_key": "main_process",
            "component_type": "process",
            "materialization_mode": "create",
            "materializer_slot": "slot.main",
        }
    ],
}


def _registry_with(executor):
    return build_test_registry(
        (
            RecipeRegistrationV1(
                recipe_id="test.recipe",
                recipe_version="1.0.0",
                entry_kind="constraint_only",
                is_default=True,
                input_model=SyncRecipeInputV1,
                executor=executor,
                output_types=("constraint_requirement",),
                conflict_policy=RecipeConflictPolicyV1(),
            ),
        )
    )


def _duplicate_contribution_id_executor(inp):
    """Two component contributions sharing one ``contribution_id``.

    Distinct component keys AND distinct slots, so every other uniqueness rule
    in the layer is satisfied — the ONLY thing wrong is the repeated id.
    """
    def _component(key):
        return parse_recipe_contribution(
            {
                "contribution_kind": "component_contribution",
                "version": "1",
                "contribution_id": "c.same",
                "component_key": key,
                "component_type": "process",
                "materialization_mode": "create",
                "materializer_slot": f"slot.{key}",
            }
        )

    return (_component("one"), _component("two"))


def test_a_repeated_contribution_id_within_one_invocation_fails_closed():
    """``contribution_id`` is required to be unique PER INVOCATION.

    Composition keys components on ``component_key``, so nothing downstream ever
    compared the ids and two contributions could share one — leaving the field
    unable to do the single job it exists for, naming one contribution within
    its invocation. An invocation is exactly one executor's returned tuple,
    which is why this is the only scope where the rule can be decided
    (issue #145, §6 architect review).
    """
    registry = build_test_registry(
        (
            RecipeRegistrationV1(
                recipe_id="test.recipe",
                recipe_version="1.0.0",
                entry_kind="executable_recipe",
                is_default=True,
                input_model=SyncRecipeInputV1,
                executor=_duplicate_contribution_id_executor,
                output_types=("component_contribution",),
                conflict_policy=RecipeConflictPolicyV1(),
            ),
        )
    )
    with pytest.raises(RecipeError) as exc:
        run_recipes(
            [
                RecipeRequestV1(
                    recipe_id="test.recipe", invocation_id="i1", raw_input=_VALID_INPUT
                )
            ],
            catalog=MaterializationCatalog({}),
            registry=registry,
        )
    diagnostic = exc.value.diagnostics[0]
    assert diagnostic.code == "RECIPE_CONTRIBUTION_INVALID"
    assert diagnostic.target == "duplicate_contribution_id"
    # Both offending positions are named, not just the second one — the caller
    # has to see the pair to know which to renumber.
    assert diagnostic.contribution_indexes == (0, 1)


def test_a_nondeterministic_executor_fails_closed():
    with pytest.raises(RecipeError) as exc:
        run_recipes(
            [
                RecipeRequestV1(
                    recipe_id="test.recipe", invocation_id="i1", raw_input=_VALID_INPUT
                )
            ],
            catalog=MaterializationCatalog({}),
            registry=_registry_with(_nondeterministic_executor),
        )
    assert exc.value.diagnostics[0].code == RECIPE_OUTPUT_NONDETERMINISTIC


def test_every_executor_is_run_exactly_twice():
    """The determinism proof is a re-run, so the count IS the mechanism.

    Registration rejects a closure, so the counter lives in module state and the
    registered function stays a plain module-level def — the same shape
    production uses.
    """
    _RUN_LOG.clear()
    run_recipes(
        [
            RecipeRequestV1(
                recipe_id="test.recipe",
                invocation_id="i1",
                raw_input=dict(_VALID_INPUT, process_key="main_process"),
            )
        ],
        catalog=MaterializationCatalog({}),
        registry=_registry_with(_counting_executor),
    )
    assert _RUN_LOG == ["main_process", "main_process"]


def test_the_recipe_input_model_is_frozen():
    """A mutable input would let the first run change what the second sees —
    turning the determinism check into a tautology."""
    model = SyncRecipeInputV1.model_validate(_VALID_INPUT)
    with pytest.raises(Exception):
        model.process_key = "other"


# ---------------------------------------------------------------------------
# Component-plan gating
# ---------------------------------------------------------------------------


def _emittable_planner_error_actions():
    """Every ``error_*`` planned action ``_build_plan`` can actually assign.

    Read off the AST of the planner, not off a list maintained by hand. The
    hand-maintained list is precisely what failed: the apply gate enumerated the
    blocking actions, two were added to the planner over time
    (``error_if_exists`` and ``error_wss_validation``) and never added to the
    gate, and the test below asserted the general property while exercising one
    action (issue #145, §6 architect review).

    Covers both forms the planner uses — a plain assignment and the conditional
    expression at the unsupported-structured-update site — by collecting every
    string constant assigned to ``planned_action`` anywhere in the module,
    including the module-level constant it assigns through.
    """
    import ast

    import boomi_mcp.categories.integration_builder as builder

    tree = ast.parse(Path(builder.__file__).read_text())
    found = set()

    def literals(node):
        if isinstance(node, ast.Constant) and isinstance(node.value, str):
            yield node.value
        elif isinstance(node, ast.IfExp):
            yield from literals(node.body)
            yield from literals(node.orelse)
        elif isinstance(node, ast.Name):
            # e.g. `_NAME_GOVERNANCE_ERROR_ACTION`
            value = getattr(builder, node.id, None)
            if isinstance(value, str):
                yield value

    for node in ast.walk(tree):
        if not isinstance(node, ast.Assign):
            continue
        for target in node.targets:
            name = (
                target.id
                if isinstance(target, ast.Name)
                else target.slice.value
                if isinstance(target, ast.Subscript)
                and isinstance(target.slice, ast.Constant)
                else None
            )
            if name == "planned_action":
                found.update(literals(node.value))

    actions = sorted(a for a in found if a.startswith("error_"))
    assert len(actions) >= 11, f"the AST walk lost sites: {actions}"
    return actions


@pytest.mark.parametrize("blocking_action", _emittable_planner_error_actions())
def test_a_blocking_planned_step_produces_zero_execute_component_calls(blocking_action):
    """``_build_plan["_success"]`` alone is NOT a validity verdict.

    Every blocking ``error_*`` step must stop the apply path dead, and the only
    way to see that is to spy on the mutation function itself. Parametrized over
    the actions DERIVED from the planner, so an action added there without being
    handled here fails this test rather than silently permitting partial
    mutation.
    """
    import boomi_mcp.categories.integration_builder as builder

    spec = _composed_spec()
    config = {
        "integration_spec": spec.model_dump(mode="json"),
        "conflict_policy": "reuse",
    }

    def fake_plan(client, cfg, *args, **kwargs):  # noqa: ARG001
        return {
            "_success": True,  # deliberately "successful" AND blocking
            "steps": [
                {
                    "planned_action": blocking_action,
                    "key": "main_process",
                    "name": "Composed Process",
                    "component_key": "main_process",
                    "validation_error": {
                        "error_code": "COMPONENT_NAME_GOVERNANCE_FAILED",
                        "field": "name",
                        "error": "blocked",
                    },
                }
            ],
        }

    with patch.object(builder, "_build_plan", side_effect=fake_plan) as plan_spy, patch.object(
        builder, "_execute_component"
    ) as execute_spy:
        result = builder._apply_plan(MagicMock(), "qa", dict(config, dry_run=False))

    assert plan_spy.called
    assert execute_spy.call_count == 0
    # ...and the refusal is REPORTED, with a reason. A blocking step that halts
    # apply but returns an empty `details` leaves the caller nothing to act on,
    # so the generic branch is the floor for any future `error_*`.
    assert result["_success"] is False
    assert result["details"], f"{blocking_action} halted apply with no explanation"
    assert result["unresolvable_steps"][0]["planned_action"] == blocking_action
    # ...and the message is the action's OWN, not the catch-all. Asserting only
    # "details is non-empty" cannot tell a tailored branch from the generic
    # fallback, so renaming a dedicated guard left the suite green (issue #145,
    # live QA). The fallback names the raw action; a dedicated branch explains.
    detail = result["details"][0]
    generic = f"cannot execute: {blocking_action}" in detail
    assert not generic, (
        f"{blocking_action} fell through to the generic message; its dedicated "
        f"branch is unreachable or misnamed"
    )


@pytest.mark.parametrize(
    "blocking_action,expected_phrase",
    [
        ("error_if_exists", "conflict_policy=fail"),
        ("error_wss_validation", "web-services listener validation"),
        ("error_name_governance", "name governance"),
        ("error_ambiguous_match", "Supply an explicit component_id"),
    ],
)
def test_a_blocking_step_explains_itself_in_its_own_words(
    blocking_action, expected_phrase
):
    """The CONTENT of the refusal, not merely its presence.

    A caller who cannot act on the message has been told the request failed, not
    why. The two actions this delta added to the gate carry the remediation that
    resolves them; the two pre-existing ones are here as controls, since the
    message-content gap spanned the whole chain before this round.
    """
    import boomi_mcp.categories.integration_builder as builder

    def fake_plan(client, cfg, *args, **kwargs):  # noqa: ARG001
        return {
            "_success": True,
            "steps": [
                {
                    "planned_action": blocking_action,
                    "key": "main_process",
                    "name": "Composed Process",
                    "candidates": [{"component_id": "a"}, {"component_id": "b"}],
                    "validation_error": {
                        "error_code": "SOME_CODE",
                        "field": "config",
                        "error": "blocked",
                    },
                }
            ],
        }

    with patch.object(builder, "_build_plan", side_effect=fake_plan), patch.object(
        builder, "_execute_component"
    ) as execute_spy:
        result = builder._apply_plan(
            MagicMock(), "qa", {"integration_spec": {"name": "x"}, "dry_run": False}
        )

    assert execute_spy.call_count == 0
    assert any(expected_phrase in detail for detail in result["details"]), (
        f"{blocking_action} did not explain itself: {result['details']}"
    )


def test_apply_always_replans_before_executing():
    """``_apply_plan`` must call ``_build_plan``; it may never trust a cache."""
    import boomi_mcp.categories.integration_builder as builder

    source = inspect.getsource(builder._apply_plan)
    assert "_build_plan(" in source


def _engine_code():
    """The engine module with docstrings and comments stripped.

    Scanning raw source would match the module docstring, which NAMES
    ``_apply_plan`` precisely to say the engine never calls it. Parsing to an AST
    and walking the calls is the difference between "the string is absent" and
    "the call is absent" — only the second is the property under test.
    """
    import ast

    tree = ast.parse(Path(engine_module.__file__).read_text())
    names = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Call):
            func = node.func
            if isinstance(func, ast.Name):
                names.add(func.id)
            elif isinstance(func, ast.Attribute):
                names.add(func.attr)
    return names


def test_the_recipe_engine_never_calls_execute_component():
    """No recipe path mutates. Asserted against the CALL GRAPH, not the text."""
    called = _engine_code()
    assert "_execute_component" not in called
    assert "_apply_plan" not in called
    assert "_build_plan" not in called


def test_the_recipe_engine_never_calls_a_topology_apply():
    source = Path(engine_module.__file__).read_text()
    assert 'requested_operation="apply"' not in source
    assert "plan_system_topology" in _engine_code()
    assert '"plan"' in source


# ---------------------------------------------------------------------------
# Constraint evaluation
# ---------------------------------------------------------------------------


def _constraint_registry(executor):
    return build_test_registry(
        (
            RecipeRegistrationV1(
                recipe_id="test.constraint",
                recipe_version="1.0.0",
                entry_kind="constraint_only",
                is_default=True,
                input_model=SyncRecipeInputV1,
                executor=executor,
                output_types=("constraint_requirement",),
                conflict_policy=RecipeConflictPolicyV1(),
            ),
        )
    )


def _unsatisfiable_component_constraint(inp):
    return (
        parse_recipe_contribution(
            {
                "contribution_kind": "constraint_requirement",
                "version": "1",
                "requirement_id": "req.absent",
                "requirement": {
                    "kind": "component",
                    "component_key": "no_such_component",
                    "component_type": "process",
                },
            }
        ),
    )


def _gated_capability_constraint(inp):
    return (
        parse_recipe_contribution(
            {
                "contribution_kind": "constraint_requirement",
                "version": "1",
                "requirement_id": "req.gated",
                "requirement": {
                    "kind": "capability",
                    "authority": "process_ir",
                    "subject": "joins",
                    "required_state": "supported",
                },
            }
        ),
    )


def test_an_unsatisfiable_component_requirement_fails_the_run():
    with pytest.raises(RecipeError) as exc:
        run_recipes(
            [
                RecipeRequestV1(
                    recipe_id="test.constraint",
                    invocation_id="i1",
                    raw_input=_VALID_INPUT,
                )
            ],
            catalog=MaterializationCatalog({}),
            registry=_constraint_registry(_unsatisfiable_component_constraint),
        )
    diagnostic = exc.value.diagnostics[0]
    assert diagnostic.code == RECIPE_CONSTRAINT_FAILED
    assert "requirement:component" in diagnostic.cause_codes


def test_a_gated_capability_requirement_fails_the_run():
    with pytest.raises(RecipeError) as exc:
        run_recipes(
            [
                RecipeRequestV1(
                    recipe_id="test.constraint",
                    invocation_id="i1",
                    raw_input=_VALID_INPUT,
                )
            ],
            catalog=MaterializationCatalog({}),
            registry=_constraint_registry(_gated_capability_constraint),
        )
    assert "requirement:capability" in exc.value.diagnostics[0].cause_codes


def test_a_compile_failure_is_reported_with_its_canonical_cause_codes():
    """The recipe layer blames ITSELF and carries the canonical code as a cause."""
    spec = _composed_spec()
    catalog, slots = build_catalog(spec.components)
    # Drop the connector metadata so the compiler cannot resolve the bindings.
    with pytest.raises(RecipeError) as exc:
        run_recipes(
            [
                RecipeRequestV1(
                    recipe_id=RECIPE_DB_REST_FANOUT,
                    invocation_id="i1",
                    raw_input={
                        "version": "1",
                        "process_key": spec.components[-1].key,
                        "source_connection_ref": "$ref:source_db_connection",
                        "source_operation_ref": "$ref:source_db_get_operation",
                        "map_ref": "$ref:transform_transform_map",
                        "targets": [
                            {
                                "target_id": "t.0",
                                "connection_ref": "$ref:target_rest_connection",
                                "operation_ref": "$ref:target_rest_operation",
                                "handoff": "document_stream",
                            },
                            {
                                "target_id": "t.1",
                                "connection_ref": "$ref:target_billing_rest_connection",
                                "operation_ref": "$ref:target_billing_rest_operation",
                                "handoff": "document_stream",
                            },
                        ],
                        "component_slots": slots,
                    },
                )
            ],
            catalog=catalog,
            connector_metadata={},  # <- the omission under test
        )
    diagnostic = exc.value.diagnostics[0]
    assert diagnostic.code == RECIPE_CONSTRAINT_FAILED
    assert diagnostic.cause_codes
    assert all(code.startswith("PROCESS_IR_") for code in diagnostic.cause_codes)


# ---------------------------------------------------------------------------
# Slot resolution
# ---------------------------------------------------------------------------


def test_a_missing_materializer_slot_is_a_contribution_failure():
    spec = _composed_spec()
    _catalog, slots = build_catalog(spec.components)
    empty = MaterializationCatalog({})
    with pytest.raises(RecipeError) as exc:
        run_recipes(
            [
                RecipeRequestV1(
                    recipe_id=RECIPE_API_TO_API_SYNC,
                    invocation_id="i1",
                    raw_input=dict(_VALID_INPUT, component_slots=slots),
                )
            ],
            catalog=empty,
        )
    assert exc.value.diagnostics[0].target == "materializer_slot"


def test_a_header_mismatch_between_slot_and_catalog_is_rejected():
    """The verification that makes the opaque slot safe."""
    spec = _composed_spec()
    catalog, slots = build_catalog(spec.components)
    poisoned = [dict(slot) for slot in slots]
    poisoned[0]["component_type"] = "process"  # it is not a process
    with pytest.raises(RecipeError) as exc:
        run_recipes(
            [
                RecipeRequestV1(
                    recipe_id=RECIPE_API_TO_API_SYNC,
                    invocation_id="i1",
                    raw_input=dict(_VALID_INPUT, component_slots=poisoned),
                )
            ],
            catalog=catalog,
        )
    assert exc.value.diagnostics[0].target == "materializer_slot_header"


# ---------------------------------------------------------------------------
# Live-QA regression (issue #145): the checks that had no test
# ---------------------------------------------------------------------------


def _ctx_registry(kind):
    from boomi_mcp.recipes.contracts import ExecutionContextPrerequisiteV1

    return build_test_registry(
        (
            RecipeRegistrationV1(
                recipe_id="test.ctx",
                recipe_version="1.0.0",
                entry_kind="constraint_only",
                is_default=True,
                input_model=SyncRecipeInputV1,
                executor=_deterministic_executor,
                output_types=("constraint_requirement",),
                prerequisites=(ExecutionContextPrerequisiteV1(kind=kind),),
                conflict_policy=RecipeConflictPolicyV1(),
            ),
        )
    )


def _ctx_request():
    return RecipeRequestV1(
        recipe_id="test.ctx", invocation_id="i1", raw_input=_VALID_INPUT
    )


@pytest.mark.parametrize(
    "kind", ["component_catalog", "process_symbol_catalog", "topology_context"]
)
def test_an_unheld_execution_context_prerequisite_fails_the_run(kind):
    """``ExecutionContextPrerequisiteV1`` says the ENGINE "must hold" it.

    All three kinds used to run unheld — the docstring described an intention
    rather than a check, and a mutant disabling it passed the whole suite
    (issue #145, live QA).
    """
    with pytest.raises(RecipeError):
        run_recipes(
            [_ctx_request()],
            catalog=MaterializationCatalog({}),  # empty, and no topology context
            registry=_ctx_registry(kind),
        )


@pytest.mark.parametrize("kind", ["component_catalog", "process_symbol_catalog"])
def test_a_held_catalog_prerequisite_lets_the_run_proceed(kind):
    """The non-vacuous control: satisfied prerequisites must NOT block."""
    spec = _composed_spec()
    catalog, _slots = build_catalog(spec.components)
    run_recipes([_ctx_request()], catalog=catalog, registry=_ctx_registry(kind))


def test_a_duplicate_invocation_id_is_rejected():
    """``order_invocations`` tracks placement by invocation_id.

    A duplicate marks its twin as already placed, which can order a recipe
    BEFORE its declared dependency (issue #145, live QA). The id is the caller's
    to choose and must be unique.
    """
    spec = _composed_spec()
    catalog, slots = build_catalog(spec.components)
    request = RecipeRequestV1(
        recipe_id=RECIPE_API_TO_API_SYNC,
        invocation_id="same",
        raw_input=dict(_VALID_INPUT, component_slots=slots),
    )
    with pytest.raises(RecipeError) as exc:
        run_recipes([request, request], catalog=catalog)
    assert exc.value.diagnostics[0].target == "duplicate_invocation_id"


# ---------------------------------------------------------------------------
# Topology symbol projection
# ---------------------------------------------------------------------------


def _topology_context(**kwargs):
    from boomi_mcp.compiler.system_topology.context import TopologyResolutionContextV1

    return TopologyResolutionContextV1(profile="qa", **kwargs)


def _composed_with(components, process_keys=()):
    """A minimal ``ComposedContributionsV1`` carrying only what projection reads."""
    from boomi_mcp.recipes.composer import ComposedContributionsV1

    # Projection reads only the process KEYS, never the roots, so a placeholder
    # root keeps the fixture honest about what the function under test consumes.
    return ComposedContributionsV1(
        process_roots=tuple((key, None) for key in process_keys),
        component_slots=(),
        topologies=(),
        constraints=(),
    )


def test_the_runs_own_components_become_topology_symbols():
    """A topology may name a component the SAME RUN contributes.

    Those components are not in the account and not in the caller's snapshot, so
    without projection the only way to resolve them was for the caller to
    redundantly assert symbols for components the engine had just decided to
    build. The plan names ``project_component_plan_symbols`` for exactly this
    (issue #145, §6 architect review).
    """
    from boomi_mcp.models.integration_models import IntegrationComponentSpec

    components = [
        IntegrationComponentSpec(key="main_process", type="process", action="create"),
        IntegrationComponentSpec(key="src_conn", type="connection", action="create"),
    ]
    projected = engine_module._project_topology_context(
        _composed_with(components, process_keys=("main_process",)),
        components,
        _topology_context(),
    )
    by_key = {s.component_key: s for s in projected.component_plan_symbols}
    assert set(by_key) == {"main_process", "src_conn"}
    # ``has_process_ir`` comes from the run's assembled roots, not from config.
    assert by_key["main_process"].has_process_ir is True
    assert by_key["src_conn"].has_process_ir is False


def test_caller_symbols_for_other_components_survive_projection():
    from boomi_mcp.compiler.system_topology.context import ComponentPlanSymbolV1
    from boomi_mcp.models.integration_models import IntegrationComponentSpec

    caller = ComponentPlanSymbolV1(
        component_key="pre_existing", component_type="connection"
    )
    components = [
        IntegrationComponentSpec(key="main_process", type="process", action="create")
    ]
    projected = engine_module._project_topology_context(
        _composed_with(components),
        components,
        _topology_context(component_plan_symbols=(caller,)),
    )
    keys = {s.component_key for s in projected.component_plan_symbols}
    assert keys == {"main_process", "pre_existing"}


def test_the_run_wins_over_a_caller_assertion_about_a_component_it_builds():
    """The run is the authority on what it materializes; a caller assertion
    about one of those components is at best a duplicate and at worst stale."""
    from boomi_mcp.compiler.system_topology.context import ComponentPlanSymbolV1
    from boomi_mcp.models.integration_models import IntegrationComponentSpec

    stale = ComponentPlanSymbolV1(
        component_key="main_process", component_type="connection"
    )
    components = [
        IntegrationComponentSpec(key="main_process", type="process", action="create")
    ]
    projected = engine_module._project_topology_context(
        _composed_with(components, process_keys=("main_process",)),
        components,
        _topology_context(component_plan_symbols=(stale,)),
    )
    symbols = list(projected.component_plan_symbols)
    assert len(symbols) == 1
    assert symbols[0].component_type == "process"  # not the caller's "connection"
    assert symbols[0].has_process_ir is True


def test_plan_topologies_projects_before_planning():
    """The WIRING, not just the helper.

    ``_plan_topologies`` used to hand the caller's context straight through, so
    the helper existing proved nothing. Spying on ``plan_system_topology`` is
    what pins that the projected context is the one the planner actually sees.
    """
    from boomi_mcp.models.integration_models import IntegrationComponentSpec

    components = [
        IntegrationComponentSpec(key="main_process", type="process", action="create")
    ]
    seen = {}

    def fake_plan(spec, context, operation):
        seen["keys"] = {s.component_key for s in context.component_plan_symbols}
        raise RuntimeError("stop after capturing the context")

    composed = _composed_with(components)
    # A VALID assembled topology — an empty `objects` list fails cardinality on
    # its own and the run would never reach the planner, making the spy below
    # silently prove nothing.
    object.__setattr__(
        composed,
        "topologies",
        (
            (
                "t1",
                {
                    "version": "1",
                    "profile_ref": "qa",
                    "objects": [
                        {
                            "kind": "process",
                            "key": "p1",
                            "component_ref": "$ref:main_process",
                        }
                    ],
                    "relations": [],
                },
            ),
        ),
    )
    with patch(
        "boomi_mcp.compiler.system_topology.pipeline.plan_system_topology",
        side_effect=fake_plan,
    ):
        with pytest.raises(RuntimeError, match="stop after capturing"):
            engine_module._plan_topologies(composed, components, _topology_context())

    assert seen.get("keys") == {"main_process"}


class _MemoInputV1(RecipeInputBase):
    """A declared ``List[str]``: frozen refuses assignment, not mutation."""

    model_config = ConfigDict(extra="forbid", frozen=True)
    version: str = "1"
    slots: List[str] = []


#: Counts genuinely fresh computations, so the executor below is NONDETERMINISTIC.
_MEMO_RUNS = {"n": 0}


def _memoizing_executor(validated):
    """Nondeterministic, but it HIDES that by caching run one's answer.

    Each genuine computation yields a different id. Stashing the first one in the
    input — which ``frozen=True`` does not prevent, because it is a ``list`` —
    lets a second run over the SAME object replay it and emit identical bytes.
    That is what made the double execution unable to see the nondeterminism.
    """
    memoized = [slot for slot in validated.slots if slot.startswith("id-")]
    if memoized:
        chosen = memoized[0]
    else:
        _MEMO_RUNS["n"] += 1
        chosen = f"id-{_MEMO_RUNS['n']}"
        validated.slots.append(chosen)
    return (
        ConstraintRequirementV1(
            contribution_kind="constraint_requirement",
            version="1",
            requirement_id=f"req.{chosen}",
            requirement=RequireProcessV1(kind="process", process_key="p.memo"),
        ),
    )


def test_the_determinism_check_uses_two_independent_inputs():
    """``frozen=True`` is shallow, so one shared input made the check vacuous.

    An executor that cached its first-run state in a declared ``List[str]`` read
    the cache back on the second run and produced identical bytes, so the
    byte-compare confirmed a determinism it had itself made impossible to
    observe. Rebuilding the input for the second run turns memoization back into
    the difference it is (issue #145, §6 architect review).
    """
    _MEMO_RUNS["n"] = 0
    # The premise: the field cannot be REPLACED, but its contents can change.
    probe = _MemoInputV1.model_validate({"version": "1", "slots": ["a"]})
    with pytest.raises(Exception):
        probe.slots = ["b"]
    probe.slots.append("mutated")
    assert probe.slots == ["a", "mutated"]

    registry = build_test_registry(
        (
            RecipeRegistrationV1(
                recipe_id="test.memo",
                recipe_version="1.0.0",
                entry_kind="executable_recipe",
                is_default=True,
                input_model=_MemoInputV1,
                executor=_memoizing_executor,
                output_types=("constraint_requirement",),
                conflict_policy=RecipeConflictPolicyV1(),
            ),
        )
    )

    with pytest.raises(RecipeError) as exc:
        run_recipes(
            [
                RecipeRequestV1(
                    recipe_id="test.memo",
                    invocation_id="i1",
                    raw_input={"version": "1", "slots": ["a"]},
                )
            ],
            catalog=MaterializationCatalog({}),
            registry=registry,
        )
    assert exc.value.diagnostics[0].code == RECIPE_OUTPUT_NONDETERMINISTIC
