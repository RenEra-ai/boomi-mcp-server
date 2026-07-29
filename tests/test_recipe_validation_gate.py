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
from unittest.mock import MagicMock, patch

import pytest

_project_root = Path(__file__).resolve().parent.parent
_src = str(_project_root / "src")
if _src not in sys.path:
    sys.path.insert(0, _src)
sys.path.insert(0, str(_project_root / "tests" / "patterns"))

from boomi_mcp.errors import RECIPE_CONSTRAINT_FAILED, RECIPE_OUTPUT_NONDETERMINISTIC
from boomi_mcp.models.integration_models import IntegrationSpecV1
from boomi_mcp.models.recipe_contributions import parse_recipe_contribution
from boomi_mcp.recipes import (
    MaterializationCatalog,
    RecipeError,
    RecipeRequestV1,
    build_test_registry,
    run_recipes,
)
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


def test_a_blocking_planned_step_produces_zero_execute_component_calls():
    """``_build_plan["_success"]`` alone is NOT a validity verdict.

    An injected blocking ``error_*`` step must stop the apply path dead, and the
    only way to see that is to spy on the mutation function itself.
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
                    "planned_action": "error_name_governance",
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
        builder._apply_plan(MagicMock(), "qa", dict(config, dry_run=False))

    assert plan_spy.called
    assert execute_spy.call_count == 0


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
