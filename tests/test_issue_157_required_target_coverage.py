"""Issue #157 (M12.19) — ONE required-target-leaf coverage validator.

The legacy hard gate lived in TWO validator families at SIX sites (the JSON
profile walker in five archetype/composition validators and the DB-write
required-target checks in two more). Now there is one implementation —
``required_target_coverage_gaps`` over the surviving generators' field index —
raised as a hard gate by the typed semantic validation and by the recipe
engine, consumed by the advisory review, and read by every former site.

The route sentinel below is the proof that every entry point reaches the single
implementation. It drives all SIX routes over one missing-leaf case — direct
plan, compile, apply, the recipe engine's public ``run_recipes``, the typed
recipe intent, and the advisory review — and its adversarial control removes a
route's INVOCATION (the name that route resolves), not the algorithm: a no-op
implementation cannot tell a route that calls the gate from one that does not,
while a scoped removal names exactly which routes go quiet and which stay loud.
"""

from __future__ import annotations

import copy
import sys
from pathlib import Path
from typing import Literal
from unittest.mock import MagicMock, patch

import pytest

_src = str(Path(__file__).resolve().parent.parent / "src")
if _src not in sys.path:
    sys.path.insert(0, _src)
sys.path.insert(0, str(Path(__file__).resolve().parent))

from boomi_mcp.authoring.workflow import (  # noqa: E402
    AuthoringWorkflowError,
    compile_authoring_request_v1,
    plan_authoring_request_v1,
)
from boomi_mcp.categories.components.builders import transform_map_validation as tmv  # noqa: E402
from boomi_mcp.categories.components.builders.profile_generation import (  # noqa: E402
    MAP_PROFILE_INDEX_UNAVAILABLE,
    profile_from_json_schema,
)
from boomi_mcp.categories.components.builders.transform_map_validation import (  # noqa: E402
    TRANSFORM_REVIEW_REQUIRED_TARGET_UNMAPPED,
    normalized_map_destinations,
    required_target_coverage_gaps,
    resolve_map_profile_index,
    validate_required_target_coverage,
)
from boomi_mcp.models.authoring_workflow import AuthoringRequestV1  # noqa: E402
from boomi_mcp.models.recipe_contributions import parse_recipe_contribution  # noqa: E402
from boomi_mcp.recipes import RecipeInputBase  # noqa: E402
from boomi_mcp.models.integration_models import IntegrationComponentSpec  # noqa: E402
from boomi_mcp.patterns.primitives.db_write import DbWritePrimitive  # noqa: E402

_PAGINATE = "boomi_mcp.categories.integration_builder.paginate_metadata"
_PROFILE = "issue-157"


@pytest.fixture(autouse=True)
def _offline():
    with patch(_PAGINATE, lambda *a, **k: []):
        yield


# ---------------------------------------------------------------------------
# the implementation, over both target families
# ---------------------------------------------------------------------------


def _json_index(root):
    return profile_from_json_schema({"format": "json", "root": root})["field_index_by_path"]


_ROOT = {
    "name": "Root",
    "kind": "object",
    "children": [
        {"name": "id", "kind": "simple", "data_type": "character", "required": True},
        {"name": "note", "kind": "simple", "data_type": "character", "required": False},
        {"name": "lines", "kind": "array", "children": [
            {"name": "sku", "kind": "simple", "data_type": "character", "required": True},
            {"name": "qty", "kind": "simple", "data_type": "number"},
        ]},
        {"name": "meta", "kind": "object", "required": True, "children": [
            {"name": "src", "kind": "simple", "data_type": "character", "required": True},
        ]},
    ],
}


def test_json_targets_missing_covered_optional_containers_and_nested_arrays():
    index = _json_index(_ROOT)
    assert required_target_coverage_gaps(index, ()) == ("Root/id", "Root/lines[]/sku", "Root/meta/src")
    # a required CONTAINER is never a gap (not mappable); an optional leaf never is
    assert "Root/meta" not in required_target_coverage_gaps(index, ())
    assert "Root/note" not in required_target_coverage_gaps(index, ())
    covered = (("direct", "Root/id"), ("map_function", "Root/lines[]/sku"), ("map_script", "Root/meta/src"))
    assert required_target_coverage_gaps(index, covered) == ()
    # partial coverage names exactly the remaining leaves, sorted
    assert required_target_coverage_gaps(index, (("direct", "Root/id"),)) == ("Root/lines[]/sku", "Root/meta/src")


def test_destinations_are_read_from_all_three_mapping_lists_in_authored_order():
    config = {
        "field_mappings": [{"source_path": "a", "target_path": "Root/id"}],
        "function_mappings": [{"function_type": "f", "inputs": ["a"], "target_path": "Root/lines[]/sku"}],
        "script_mappings": [{"outputs": [{"target_path": "Root/meta/src"}, {"target_path": " "}], "inputs": []}],
        "not_a_list": 3,
    }
    assert normalized_map_destinations(config) == (
        ("direct", "Root/id"), ("map_function", "Root/lines[]/sku"), ("map_script", "Root/meta/src"),
    )


def _write_profile(statement_type="dynamicinsert", fields=("id", "name"), required=("id",)):
    from boomi_mcp.patterns.primitives.db_write import DbWriteProfileParams

    return DbWriteProfileParams(
        table_name="orders",
        statement_type=statement_type,
        fields=[{"name": f, "data_type": "character", "mandatory": f in required} for f in fields],
    )


def test_db_write_targets_use_the_same_implementation_over_the_write_index():
    index = DbWritePrimitive.build_field_index(_write_profile())
    assert index, "the write profile builder produced no index — the case would be vacuous"
    required = required_target_coverage_gaps(index, ())
    assert required and all(path.startswith("Fields/") or path.startswith("Conditions/") for path in required)
    assert required_target_coverage_gaps(index, tuple(("direct", path) for path in required)) == ()


def test_the_db_empty_index_deferral_is_preserved():
    """An empty write index reports NOTHING — the write-profile builder refuses precisely at emit."""
    assert required_target_coverage_gaps({}, ()) == ()
    assert required_target_coverage_gaps(None, ()) == ()


def test_bare_path_destinations_are_accepted_too():
    index = _json_index(_ROOT)
    assert required_target_coverage_gaps(index, ("Root/id", "Root/lines[]/sku", "Root/meta/src")) == ()


# ---------------------------------------------------------------------------
# the six legacy sites read the ONE implementation
# ---------------------------------------------------------------------------


def test_the_legacy_walker_is_gone_and_every_former_site_reads_the_shared_function():
    import boomi_mcp.patterns.archetype_parameters as params

    assert not hasattr(params, "_required_simple_leaf_paths")
    modules = (
        "boomi_mcp.patterns.archetypes.database_to_api_sync",
        "boomi_mcp.patterns.archetypes.api_to_api_sync",
        "boomi_mcp.patterns.archetypes.http_listener_to_rest",
        "boomi_mcp.patterns.archetypes.api_to_database_sync",
        "boomi_mcp.patterns.archetypes.http_listener_to_db",
        "boomi_mcp.patterns.composition",
    )
    import importlib

    for name in modules:
        module = importlib.import_module(name)
        assert module.required_target_coverage_gaps is required_target_coverage_gaps, name


def test_the_archetype_contract_still_refuses_a_missing_required_leaf_through_the_shared_function():
    """Parity with the legacy JSON family: the same refusal text, now from one implementation."""
    from boomi_mcp.patterns.base import PatternKind
    from boomi_mcp.patterns.registry import PatternRegistry

    cls = PatternRegistry.from_package("boomi_mcp.patterns").get("database_to_api_sync", kind=PatternKind.ARCHETYPE)
    params = copy.deepcopy(next(e.parameters for e in cls.examples if e.name == "minimal_manual_sync"))
    params["target"]["payload_profile"]["root"]["children"].append(
        {"name": "must", "kind": "simple", "data_type": "character", "required": True}
    )
    with pytest.raises(Exception) as excinfo:
        cls.validate_parameters(params)
    assert "required target leaf path(s) unmapped" in str(excinfo.value)
    with patch.object(tmv, "required_target_coverage_gaps", lambda *a, **k: ()):
        # the archetype module bound the function at import, so patch its own name
        import boomi_mcp.patterns.archetypes.database_to_api_sync as module

        with patch.object(module, "required_target_coverage_gaps", lambda *a, **k: ()):
            cls.validate_parameters(params)  # the refusal came from the shared function, not a local copy


# ---------------------------------------------------------------------------
# the route sentinel: every entry point reaches the single implementation
# ---------------------------------------------------------------------------


_REPLAY_CASE = (
    Path(__file__).resolve().parent / "fixtures" / "governance" / "issue_157" / "cases"
    / "database_to_api_sync" / "wm_off_dlq_off_create" / "input.canonical.json"
)


def _components(missing_required=True):
    """The committed canonical replay fixture's components — a plan that is clean.

    Causally independent of this slice's implementation (derived from the
    baseline archetype's own emitted components), so the gap below is the ONLY
    thing that separates the failing case from the passing one: a required
    leaf appended to the target profile that no mapping binds.
    """
    import json

    request = json.loads(_REPLAY_CASE.read_text(encoding="utf-8"))
    components = copy.deepcopy(request["intent"]["components"])
    if missing_required:
        target = next(c for c in components if c["type"] == "profile.json")
        target["config"]["root"]["children"].append(
            {"name": "must", "kind": "simple", "data_type": "character", "required": True}
        )
    return components


def _request(components):
    import json

    request = json.loads(_REPLAY_CASE.read_text(encoding="utf-8"))
    request["intent"]["components"] = components
    return AuthoringRequestV1.model_validate(request)


_MAP_KEY = "transform_transform_map"
_TARGET_KEY = "transform_target_profile"


def test_plan_reports_the_gap_by_name_and_compile_refuses_it():
    result, _ = plan_authoring_request_v1(_request(_components()), boomi_client=MagicMock(), profile=_PROFILE)
    hits = [d for d in result.errors if TRANSFORM_REVIEW_REQUIRED_TARGET_UNMAPPED in d.cause_codes]
    assert hits and hits[0].path == "/components/{0}/config/target_profile_id".format(_MAP_KEY)
    assert TRANSFORM_REVIEW_REQUIRED_TARGET_UNMAPPED in result.validation_report.codes
    with pytest.raises(AuthoringWorkflowError):
        compile_authoring_request_v1(_request(_components()), boomi_client=MagicMock(), profile=_PROFILE)
    covering, _ = plan_authoring_request_v1(_request(_components(missing_required=False)), boomi_client=MagicMock(), profile=_PROFILE)
    assert not [d for d in covering.errors if TRANSFORM_REVIEW_REQUIRED_TARGET_UNMAPPED in d.cause_codes]


def _components_missing(names):
    """The replay fixture's components with N named required leaves unbound."""
    import json

    request = json.loads(_REPLAY_CASE.read_text(encoding="utf-8"))
    components = copy.deepcopy(request["intent"]["components"])
    target = next(c for c in components if c["type"] == "profile.json")
    for name in names:
        target["config"]["root"]["children"].append(
            {"name": name, "kind": "simple", "data_type": "character", "required": True}
        )
    return components


def test_the_diagnostic_serves_the_count_and_names_where_the_paths_live():
    """QA-157-r1-03: it promised "the count and the paths" and served neither.

    An `AuthoringDiagnosticV1` has no `details`, and its `evidence` admits only
    structural tokens and codes — deliberately, because this surface is
    value-free and a target leaf path is an authored profile value. So the
    COUNT travels (derived structure) and the caller is pointed at the one
    surface that does name the paths.
    """
    one, _ = plan_authoring_request_v1(
        _request(_components_missing(("qa157_canary_leaf",))),
        boomi_client=MagicMock(), profile=_PROFILE,
    )
    diag = next(d for d in one.errors if TRANSFORM_REVIEW_REQUIRED_TARGET_UNMAPPED in d.cause_codes)
    assert "1 required target leaf path(s) unbound" in diag.message
    assert "travel in the cause diagnostic" not in diag.message
    assert "review_transformation" in diag.remediation
    assert "missing_paths" not in diag.remediation
    # ...and it does NOT claim the envelope is value-free (QA-157-r2-02's
    # sibling, r2-03): the served plan carries a derived preview OF THE
    # CALLER'S OWN REQUEST, so the authored target paths do appear there. The
    # diagnostic's honest claim is narrower — it does not say WHICH leaf.
    assert "value-free" not in diag.remediation
    assert "never echoes" not in diag.remediation
    # EVERY CLAIM THE TEXT STILL MAKES CARRIES ITS WITNESS, HERE, in the same
    # test — which is the structural answer to this defect class appearing
    # twice (QA-157-r1-03, then QA-157-r2-03). Both defects were a hand-written
    # sentence about served content that nothing checked: the first said the
    # count and paths travelled in a field that has none, the second said the
    # envelope never echoes the paths while the derived preview echoes the
    # caller's own. The remediation now makes exactly one checkable claim, and
    # the check runs beside it: `review_transformation` really does name them.
    from boomi_mcp.categories.transformation_review import review_transformation_action

    review = review_transformation_action(
        "validate_unmapped",
        {"integration_spec": {"name": "x", "components": _components_missing(("qa157_canary_leaf",))}},
    )
    named = [
        issue["details"].get("path")
        for issue in review.get("issues", [])
        if issue.get("code") == TRANSFORM_REVIEW_REQUIRED_TARGET_UNMAPPED
    ]
    assert named == ["Root/qa157_canary_leaf"], named
    # VALUE-FREE: the unbound leaf's own name appears nowhere in the served row.
    assert "qa157_canary_leaf" not in (
        diag.message + diag.remediation + diag.path + repr(diag.evidence)
    )
    # the count is DERIVED, not a constant: two gaps say two.
    two, _ = plan_authoring_request_v1(
        _request(_components_missing(("qa157_canary_leaf", "qa157_canary_other"))),
        boomi_client=MagicMock(), profile=_PROFILE,
    )
    other = next(d for d in two.errors if TRANSFORM_REVIEW_REQUIRED_TARGET_UNMAPPED in d.cause_codes)
    assert "2 required target leaf path(s) unbound" in other.message


def _bound_apply_payload(components):
    """An apply payload BOUND to a covering compile, then carrying the gap.

    A typed apply recompiles the request it is handed; binding it to the
    covering compile's revision and compile hash is what gets the request past
    the "bind to a validated compile" preflight and into the recompile, where
    the gap must refuse before any write.
    """
    covering, _ = compile_authoring_request_v1(_request(_components(missing_required=False)), boomi_client=MagicMock(), profile=_PROFILE)
    payload = _request(components).model_dump(mode="json")
    payload["expected_capability_revision"] = covering.revision_binding.capability_revision
    payload["expected_compile_hash"] = covering.revision_binding.compile_hash
    return {"authoring_request": payload, "dry_run": True}


def test_the_apply_preflight_refuses_before_any_write():
    from boomi_mcp.categories import integration_builder
    from boomi_mcp.categories.integration_builder import build_integration_action

    with patch.object(integration_builder, "_execute_component") as execute, patch.object(
        integration_builder, "create_component"
    ) as create:
        result = build_integration_action(MagicMock(), _PROFILE, "apply", _bound_apply_payload(_components()))
    assert result.get("_success") is False
    assert TRANSFORM_REVIEW_REQUIRED_TARGET_UNMAPPED in str(result)
    assert execute.call_count == 0 and create.call_count == 0


def test_the_recipe_engine_refuses_with_the_canonical_cause_code():
    from boomi_mcp.recipes.engine import _validate_component_maps
    from boomi_mcp.recipes.errors import RecipeError

    components = [IntegrationComponentSpec(**c) for c in _components()]
    with pytest.raises(RecipeError) as excinfo:
        _validate_component_maps(components)
    codes = {code for d in excinfo.value.diagnostics for code in (getattr(d, "cause_codes", ()) or ())}
    assert TRANSFORM_REVIEW_REQUIRED_TARGET_UNMAPPED in codes
    _validate_component_maps([IntegrationComponentSpec(**c) for c in _components(missing_required=False)])


def test_the_advisory_review_reports_the_same_gap_once_per_path():
    from boomi_mcp.categories.transformation_review import review_transformation_action

    result = review_transformation_action(
        "validate_unmapped", {"integration_spec": {"name": "x", "components": _components()}}
    )
    issues = [i for i in result.get("issues", []) if i.get("code") == TRANSFORM_REVIEW_REQUIRED_TARGET_UNMAPPED]
    assert [i["details"].get("path") for i in issues] == ["Root/must"]
    assert result.get("unmapped_required_target_paths") == ["Root/must"]


#: The components the test recipe contributes. A module GLOBAL, not a closure:
#: the registry refuses an executor that closes over state, deliberately, so a
#: registered recipe cannot carry hidden per-registration behaviour.
_RECIPE_SLOTS = []


class _CoverageInput(RecipeInputBase):
    version: Literal["1"] = "1"


def _coverage_executor(_inp):
    return tuple(
        parse_recipe_contribution(
            {
                "contribution_kind": "component_contribution",
                "version": "1",
                "contribution_id": "c." + component.key.replace("_", "-"),
                "component_key": component.key,
                "component_type": component.type,
                "materialization_mode": "create",
                "materializer_slot": component.key,
            }
        )
        for component in _RECIPE_SLOTS
    )


def _coverage_registry(component_dicts):
    """A registered recipe contributing exactly these components, by opaque slot.

    The recipe layer is entered through ``run_recipes`` — its public entry — not
    through the private map validator it happens to call. A sentinel that calls
    the private function proves the function exists, not that the route reaches
    it.

    Slot names ARE the component keys, exactly as the typed recipe intent builds
    its own catalog, so both recipe routes resolve the same slots.
    """
    from boomi_mcp.recipes import MaterializationCatalog
    from boomi_mcp.recipes.contracts import RecipeConflictPolicyV1, RecipeRegistrationV1
    from boomi_mcp.recipes.registry import build_test_registry

    components = [IntegrationComponentSpec(**c) for c in component_dicts]
    _RECIPE_SLOTS[:] = components
    catalog = MaterializationCatalog({c.key: c for c in components})
    registry = build_test_registry(
        (
            RecipeRegistrationV1(
                recipe_id="test.coverage",
                recipe_version="1.0.0",
                entry_kind="executable_recipe",
                is_default=True,
                input_model=_CoverageInput,
                executor=_coverage_executor,
                output_types=("component_contribution",),
                conflict_policy=RecipeConflictPolicyV1(),
            ),
        )
    )
    return registry, catalog, components


def _recipe_request():
    from boomi_mcp.recipes import RecipeRequestV1

    return [
        RecipeRequestV1(
            recipe_id="test.coverage", invocation_id="i1", raw_input={"version": "1"}
        )
    ]


def _recipe_intent_request(component_dicts):
    """A typed RECIPE intent — the second recipe route, which lifts no roots."""
    from boomi_mcp.models.authoring_workflow import RecipeAuthoringIntentV1

    return AuthoringRequestV1(
        intent=RecipeAuthoringIntentV1(
            integration_name="coverage",
            base_components=[IntegrationComponentSpec(**c) for c in component_dicts],
            invocations=(
                {
                    "recipe_id": "test.coverage",
                    "invocation_id": "i1",
                    "raw_input": {"version": "1"},
                },
            ),
        )
    )


def _drive_routes(refusals):
    """Drive all six entry points over the same missing-leaf case.

    ``refusals`` collects ``(route, refused)`` so a removal control can name
    which route went quiet. Setup work that is not part of a route — the
    covering compile the apply payload binds to — happens BEFORE this runs, so
    it can never be counted as a route's own hit.
    """
    from boomi_mcp.categories import integration_builder
    from boomi_mcp.categories.integration_builder import build_integration_action
    from boomi_mcp.categories.transformation_review import review_transformation_action
    from boomi_mcp.recipes import MaterializationCatalog, RecipeError, run_recipes
    from boomi_mcp.recipes import engine as engine_module

    comps = _components()
    registry, catalog, _ = _coverage_registry(comps)
    # SETUP, not a route: the covering compile the apply payload binds to runs
    # here, before any route is driven, so it can never be read as apply's own.
    apply_payload = _bound_apply_payload(comps)

    result, _ = plan_authoring_request_v1(_request(comps), boomi_client=MagicMock(), profile=_PROFILE)
    refusals.append(("plan", bool([d for d in result.errors if TRANSFORM_REVIEW_REQUIRED_TARGET_UNMAPPED in d.cause_codes])))

    try:
        compile_authoring_request_v1(_request(comps), boomi_client=MagicMock(), profile=_PROFILE)
        refusals.append(("compile", False))
    except AuthoringWorkflowError as exc:
        refusals.append(("compile", TRANSFORM_REVIEW_REQUIRED_TARGET_UNMAPPED in str(exc.diagnostics)))

    with patch.object(integration_builder, "_execute_component"), patch.object(
        integration_builder, "create_component"
    ):
        applied = build_integration_action(MagicMock(), _PROFILE, "apply", apply_payload)
    refusals.append(("apply", TRANSFORM_REVIEW_REQUIRED_TARGET_UNMAPPED in str(applied)))

    try:
        run_recipes(_recipe_request(), catalog=catalog, registry=registry)
        refusals.append(("recipe_engine", False))
    except RecipeError as exc:
        codes = {c for d in exc.diagnostics for c in (getattr(d, "cause_codes", ()) or ())}
        refusals.append(("recipe_engine", TRANSFORM_REVIEW_REQUIRED_TARGET_UNMAPPED in codes))

    with patch.object(engine_module, "production_registry", lambda: registry):
        try:
            plan_authoring_request_v1(
                _recipe_intent_request(comps), boomi_client=MagicMock(), profile=_PROFILE
            )
            refusals.append(("recipe_intent", False))
        except AuthoringWorkflowError as exc:
            refusals.append(("recipe_intent", TRANSFORM_REVIEW_REQUIRED_TARGET_UNMAPPED in str(exc.diagnostics)))

    review = review_transformation_action(
        "validate_unmapped", {"integration_spec": {"name": "x", "components": comps}}
    )
    refusals.append(("advisory_review", any(
        i.get("code") == TRANSFORM_REVIEW_REQUIRED_TARGET_UNMAPPED for i in review.get("issues", [])
    )))
    return dict(refusals)


_ROUTES = ("plan", "compile", "apply", "recipe_engine", "recipe_intent", "advisory_review")


def test_the_route_sentinel_sees_every_one_of_the_six_entry_points():
    """Every route refuses the SAME missing leaf, and each hits the one implementation."""
    seen = []
    real = tmv.required_target_coverage_gaps

    def spy(*args, **kwargs):
        seen.append(1)
        return real(*args, **kwargs)

    refusals = []
    with patch.object(tmv, "required_target_coverage_gaps", spy):
        result = _drive_routes(refusals)
    assert set(result) == set(_ROUTES), sorted(result)
    assert all(result[route] for route in _ROUTES), result
    assert seen, "the single implementation was never reached"


@pytest.mark.parametrize(
    "binding,quiet,loud",
    [
        # each route resolves the gate by ONE name; removing that name silences
        # exactly the routes that resolve through it, and no others.
        (("boomi_mcp.authoring.workflow", "_required_target_coverage_error"),
         ("plan", "compile", "apply"), ("recipe_engine", "recipe_intent", "advisory_review")),
        (("boomi_mcp.categories.components.builders.transform_map_validation",
          "validate_required_target_coverage"),
         ("plan", "compile", "apply", "recipe_engine", "recipe_intent"), ("advisory_review",)),
        (("boomi_mcp.categories.components.builders.transform_map_validation",
          "required_target_coverage_gaps"),
         _ROUTES, ()),
    ],
)
def test_removing_a_routes_invocation_silences_that_route_and_only_that_route(binding, quiet, loud):
    """The adversarial control removes the INVOCATION, not the algorithm.

    Patching the implementation to a no-op proves only that the algorithm
    decides; it cannot tell a route that calls it from a route that does not.
    Removing the name a route resolves does, and the routes that stay loud are
    the evidence the removal was scoped.
    """
    import importlib

    module = importlib.import_module(binding[0])
    with patch.object(module, binding[1], lambda *a, **k: None if binding[1] != "required_target_coverage_gaps" else ()):
        result = _drive_routes([])
    assert not any(result[route] for route in quiet), result
    assert all(result[route] for route in loud), result


# ---------------------------------------------------------------------------
# the selected artifact, not the candidate config
# ---------------------------------------------------------------------------


def _reused_profile_components(candidate_root):
    comps = _components(missing_required=False)
    tgt = next(c for c in comps if c["key"] == _TARGET_KEY)
    tgt["config"] = {"reference_only": True, "component_name": "Existing Target", "profile_type": "json.generated", "root": candidate_root}
    tgt["component_id"] = "prof-uuid-1"
    return comps


def test_a_reused_profile_is_judged_by_its_selected_index_never_by_the_candidate_config():
    """Candidate says covered; the account's real profile requires more. The gate believes the account."""
    candidate_root = {"name": "Root", "kind": "object", "children": [{"name": "target_a", "kind": "simple", "data_type": "character", "required": True}]}
    comps = _reused_profile_components(candidate_root)
    by_key = {c["key"]: IntegrationComponentSpec(**c) for c in comps}
    map_cfg = dict(by_key[_MAP_KEY].config, component_name="Map")
    # offline, nothing selected: the reused profile is UNAVAILABLE, never the candidate
    assert resolve_map_profile_index(map_cfg["target_profile_id"], by_key, None) is None
    assert validate_required_target_coverage(map_cfg, by_key, None) is None
    # the selected artifact requires `b` too -> a gap the candidate hid
    selected_root = {"name": "Root", "kind": "object", "children": [
        {"name": "target_a", "kind": "simple", "data_type": "character", "required": True},
        {"name": "must", "kind": "simple", "data_type": "character", "required": True}]}
    selected = {_TARGET_KEY: _json_index(selected_root)}
    err = validate_required_target_coverage(map_cfg, by_key, None, selected_indexes=selected)
    assert err is not None and err.error_code == TRANSFORM_REVIEW_REQUIRED_TARGET_UNMAPPED
    assert err.details["missing_paths"] == ["Root/must"]
    # and a selected artifact that IS covered passes
    covered = {_TARGET_KEY: _json_index(candidate_root)}
    assert validate_required_target_coverage(map_cfg, by_key, None, selected_indexes=covered) is None


def test_the_typed_gate_reads_the_selected_index_through_the_builder_resolver():
    """The plan entry resolves the reused profile from the account and refuses on ITS shape."""
    from boomi_mcp.categories import integration_builder

    candidate_root = {"name": "Root", "kind": "object", "children": [{"name": "target_a", "kind": "simple", "data_type": "character", "required": True}]}
    comps = _reused_profile_components(candidate_root)
    selected_root = {"name": "Root", "kind": "object", "children": [
        {"name": "target_a", "kind": "simple", "data_type": "character", "required": True},
        {"name": "must", "kind": "simple", "data_type": "character", "required": True}]}
    discovered = {"profile_component_type": "profile.json", "field_index_by_path": _json_index(selected_root)}
    with patch.object(integration_builder, "_discover_profile_index", lambda client, uuid: discovered if uuid == "prof-uuid-1" else None):
        result, _ = plan_authoring_request_v1(_request(comps), boomi_client=MagicMock(), profile=_PROFILE)
    hits = [d for d in result.errors if TRANSFORM_REVIEW_REQUIRED_TARGET_UNMAPPED in d.cause_codes]
    assert hits and hits[0].subject_id == _MAP_KEY


def test_an_unresolvable_selected_profile_is_unavailable_not_trusted():
    from boomi_mcp.categories import integration_builder

    candidate_root = {"name": "Root", "kind": "object", "children": [{"name": "target_a", "kind": "simple", "data_type": "character", "required": True}]}
    comps = _reused_profile_components(candidate_root)
    from boomi_mcp.categories.components.builders.transform_map_validation import validate_transform_map

    by_key = {c["key"]: IntegrationComponentSpec(**c) for c in comps}
    # the shared map validator names the unavailability; the plan lint serves it as an unexecutable step
    effective = dict(by_key[_MAP_KEY].config, component_name="Map")
    err = validate_transform_map(effective, by_key[_MAP_KEY].depends_on, by_key, None, None)
    assert err is not None and err.error_code == MAP_PROFILE_INDEX_UNAVAILABLE
    with patch.object(integration_builder, "_discover_profile_index", lambda client, uuid: None):
        result, _ = plan_authoring_request_v1(_request(comps), boomi_client=MagicMock(), profile=_PROFILE)
    assert any("error_generated_profile_validation" in d.cause_codes and d.subject_id == _MAP_KEY for d in result.errors)
    # and the coverage gate never invented a gap from the candidate config
    assert not any(TRANSFORM_REVIEW_REQUIRED_TARGET_UNMAPPED in d.cause_codes for d in result.errors)
