"""Tests for the stub minimal integration archetype (Issue #17)."""

import json
from unittest.mock import MagicMock

import pytest
from pydantic import ValidationError

from boomi_mcp.categories.integration_builder import _build_plan
from boomi_mcp.models.integration_models import IntegrationSpecV1
from boomi_mcp.patterns import (
    PatternKind,
    PatternRegistry,
    pattern_validation_error,
)
# #159 S3: the archetypes package re-exports nothing; import the isolated
# defining module directly (kept until #160).
from boomi_mcp.patterns.archetypes.stub_minimal import (
    StubMinimalIntegrationArchetype,
    StubMinimalIntegrationParameters,
)


# ---------------------------------------------------------------------------
# Helpers and canonical fixtures
# ---------------------------------------------------------------------------

def _make_params(**overrides) -> StubMinimalIntegrationParameters:
    base = {"integration_name": "demo-integration"}
    base.update(overrides)
    return StubMinimalIntegrationParameters(**base)


EXPECTED_SPEC_DICT = {
    "version": "1.0",
    "name": "demo-integration",
    "mode": "redesign",
    "components": [],
    "processes": (),
    "goals": [
        "Validate V3 archetype build path without Boomi mutation.",
        "Stub archetype emits no executable Boomi components.",
    ],
    "endpoints": [
        {
            "key": "stub_source",
            "type": "stub",
            "direction": "source",
            "label": "Stub source",
        },
        {
            "key": "stub_target",
            "type": "stub",
            "direction": "target",
            "label": "Stub target",
        },
    ],
    "flows": [
        {
            "key": "stub_noop_flow",
            "name": "Stub no-op flow",
            "source": "stub_source",
            "target": "stub_target",
            "operation": "noop",
            "executable": False,
        },
    ],
    "naming": {
        "archetype": "stub_minimal_integration",
        "component_prefix": "STUB",
    },
    "folders": {},
    "runtime": {},
    "validation_rules": {
        "no_boomi_mutation": True,
        "raw_xml_exposed": False,
        "component_count": 0,
    },
    "pipeline": None,
    "profile_indexes_by_component_id": None,
}


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

def test_metadata_is_stub_marked():
    md = StubMinimalIntegrationArchetype.metadata
    assert md.name == "stub_minimal_integration"
    assert md.version == "0.1.0"
    assert md.kind is PatternKind.ARCHETYPE
    assert {"stub", "test", "safe", "no-boomi-mutation"}.issubset(set(md.tags))
    assert "real integration creation" in md.not_for
    assert "production integration creation" in md.not_for


def test_registry_discovers_stub_via_canonical_package():
    registry = PatternRegistry.from_package("boomi_mcp.patterns")
    assert (
        registry.get("stub_minimal_integration")
        is StubMinimalIntegrationArchetype
    )


@pytest.mark.parametrize(
    "payload, expected_loc",
    [
        ({}, ("integration_name",)),
        ({"integration_name": "   "}, ("integration_name",)),
        ({"integration_name": ""}, ("integration_name",)),
        ({"integration_name": "demo", "goal": "   "}, ("goal",)),
        ({"integration_name": "demo", "rogue": "x"}, ("rogue",)),
    ],
)
def test_validate_parameters_rejects_invalid(payload, expected_loc):
    with pytest.raises(ValidationError) as exc_info:
        StubMinimalIntegrationArchetype.validate_parameters(payload)
    locs = [err["loc"] for err in exc_info.value.errors()]
    assert expected_loc in locs


def test_validate_parameters_strips_whitespace():
    params = StubMinimalIntegrationArchetype.validate_parameters(
        {
            "integration_name": "  demo  ",
            "goal": "  do the thing  ",
            "source_label": "  Src  ",
            "target_label": "  Tgt  ",
            "component_prefix": "  PFX  ",
        }
    )
    assert isinstance(params, StubMinimalIntegrationParameters)
    assert params.integration_name == "demo"
    assert params.goal == "do the thing"
    assert params.source_label == "Src"
    assert params.target_label == "Tgt"
    assert params.component_prefix == "PFX"


def test_validation_failure_converts_with_field_path():
    try:
        StubMinimalIntegrationArchetype.validate_parameters({})
    except ValidationError as exc:
        err = pattern_validation_error(exc, suggestion="Provide integration_name")
    else:
        pytest.fail("Expected ValidationError for missing integration_name")

    assert err.error_code == "PARAM_VALIDATION_FAILED"
    assert err.field_errors
    assert err.field_errors[0].field_path == "integration_name"

    payload = err.to_dict()
    assert payload["_success"] is False
    assert payload["suggestion"] == "Provide integration_name"
    assert any(
        fe["field_path"] == "integration_name" for fe in payload["field_errors"]
    )


def test_emit_spec_returns_zero_components():
    params = _make_params()
    spec = StubMinimalIntegrationArchetype.emit_spec(params)
    assert isinstance(spec, IntegrationSpecV1)
    assert spec.components == []
    assert len(spec.components) == 0
    assert spec.mode == "redesign"


def test_emitted_spec_matches_golden_dict():
    params = _make_params()
    spec = StubMinimalIntegrationArchetype.emit_spec(params)
    assert spec.model_dump() == EXPECTED_SPEC_DICT


def test_serialized_spec_has_no_xml_or_mutation_markers():
    params = _make_params()
    spec = StubMinimalIntegrationArchetype.emit_spec(params)
    payload = json.dumps(spec.model_dump())

    for marker in ("<?xml", "<process", "<component", "<connector", "<operation"):
        assert marker not in payload, f"Unexpected XML marker {marker!r} in serialized spec"

    assert spec.validation_rules["no_boomi_mutation"] is True
    assert spec.validation_rules["raw_xml_exposed"] is False
    assert spec.validation_rules["component_count"] == 0


def test_build_plan_warns_no_components_for_stub():
    params = _make_params()
    spec = StubMinimalIntegrationArchetype.emit_spec(params)
    config = {
        "conflict_policy": "reuse",
        "integration_spec": spec.model_dump(),
    }
    result = _build_plan(MagicMock(), config)

    assert result["_success"] is True
    assert result["steps"] == []
    assert result["warnings"] is not None
    assert (
        "No components were provided; plan contains zero executable steps."
        in result["warnings"]
    )


def test_stub_parameter_schema_has_field_descriptions():
    schema = StubMinimalIntegrationArchetype.parameter_schema()
    properties = schema.get("properties", {})
    assert properties, "stub parameter schema must expose properties"
    # Every property must carry a non-empty description so the JSON Schema
    # alone is enough for an LLM client to fill values without source access.
    for prop_name, prop_schema in properties.items():
        desc = prop_schema.get("description")
        assert desc, f"field {prop_name!r} is missing a description"
        assert isinstance(desc, str) and desc.strip(), (
            f"field {prop_name!r} has an empty description"
        )


def test_stub_describe_includes_enriched_keys():
    described = StubMinimalIntegrationArchetype.describe()
    assert {
        "metadata",
        "parameter_schema",
        "capability_notes",
        "limitations",
        "examples",
        "example_policy",
    } <= set(described.keys())

    assert described["example_policy"] == "example_only_not_reusable_template"
    assert described["capability_notes"], "stub must publish capability_notes"
    assert described["limitations"], "stub must publish limitations"
    assert described["examples"], "stub must publish at least one example"

    # Every example crosses the wire as a plain dict and carries the marker
    # fields that say "not a template".
    for example in described["examples"]:
        assert example["is_template"] is False
        assert example["template_status"] == "example_only_not_reusable_template"
        assert example["name"]
        assert example["description"]


def test_stub_example_payload_has_no_forbidden_template_markers():
    described = StubMinimalIntegrationArchetype.describe()
    # Stub examples MUST be safe documentation. None of the following may
    # appear: SQL keywords, OData filter syntax, SOAP envelopes, XML tags,
    # mapping/groovy/script markers. The check is case-insensitive on a
    # JSON dump of each example so it covers both keys and values.
    forbidden_substrings = (
        "select ", "insert ", "update ", "delete ", "from ", "where ",
        "<?xml", "<soap", "<envelope", "<process", "<connector", "<operation",
        "$filter=", "$select=", "$expand=",
        " def ", "import ", "groovy", "javascript:",
        "script:", "mapping:", " map ",
    )
    for example in described["examples"]:
        payload = json.dumps(example).lower()
        for marker in forbidden_substrings:
            assert marker not in payload, (
                f"example {example['name']!r} contains forbidden marker {marker!r}"
            )


# ---------------------------------------------------------------------------
# #159 S2: boomi.archetype.stub_minimal 1.0.0 — a Stop-only unit, zero components
# ---------------------------------------------------------------------------
#
# Oracle: the frozen #157 case ``stub_minimal/default`` (its historical input
# and its flow/endpoint baselines). The canonical input is DERIVED from the
# legacy input by ``_stub_canonical_request`` and the committed file must equal
# that derivation.

from pathlib import Path as _Path

from boomi_mcp.recipes.builtins.stub_minimal import (  # noqa: E402
    StubMinimalInputV1,
    emit_stub_minimal,
)

_STUB_RECIPE_ID = "boomi.archetype.stub_minimal"
_STUB_VERSION = "1.0.0"
_STUB_CASE = (
    _Path(__file__).resolve().parents[1]
    / "fixtures" / "governance" / "issue_157" / "cases" / "stub_minimal" / "default"
)


def _stub_registration():
    """The registration handed to the integrator (the catalog is not lane-owned)."""
    from boomi_mcp.recipes.builtins import catalog
    from boomi_mcp.recipes.contracts import RecipeRegistrationV1

    return RecipeRegistrationV1(
        recipe_id=_STUB_RECIPE_ID,
        recipe_version=_STUB_VERSION,
        entry_kind="executable_recipe",
        is_default=True,
        input_model=StubMinimalInputV1,
        executor=emit_stub_minimal,
        output_types=("constraint_requirement", "process_unit"),
        capability_requirements=(),
        conflict_policy=catalog._SYNC_CONFLICT_POLICY,
    )


def _stub_registry():
    from boomi_mcp.recipes.builtins import catalog
    from boomi_mcp.recipes.registry import build_test_registry

    registered = {(reg.recipe_id, reg.recipe_version) for reg in catalog.PRODUCTION_REGISTRATIONS}
    extra = () if (_STUB_RECIPE_ID, _STUB_VERSION) in registered else (_stub_registration(),)
    return build_test_registry(catalog.PRODUCTION_REGISTRATIONS + extra)


def _stub_canonical_request():
    """The legacy stub input re-spelled as one pinned 1.0.0 invocation. The
    legacy display labels are not inputs: they must equal the fixed labels the
    stub projection serves, or the case cannot be carried."""
    from boomi_mcp.authoring.derived_flows import STUB_ENDPOINTS

    legacy = json.loads((_STUB_CASE / "input.legacy.json").read_text())
    parameters = StubMinimalIntegrationParameters(**legacy["input"]["parameters"])
    assert parameters.source_label == STUB_ENDPOINTS["source"][1]
    assert parameters.target_label == STUB_ENDPOINTS["target"][1]
    return {
        "contract_version": "2",
        "intent": {
            "integration_name": "issue-157 replay stub_minimal/default",
            "intent_kind": "recipe",
            "invocations": [
                {
                    "recipe_id": _STUB_RECIPE_ID,
                    "recipe_version": _STUB_VERSION,
                    "invocation_id": "stub_minimal",
                    "raw_input": {
                        "version": "1",
                        "process_key": "main_process",
                        "naming": {"component_prefix": parameters.component_prefix},
                    },
                }
            ],
        },
    }


def test_stub_recipe_canonical_input_is_the_derivation_of_the_legacy_input():
    committed = json.loads((_STUB_CASE / "input.canonical.json").read_text())
    assert committed == _stub_canonical_request()
    provenance = json.loads((_STUB_CASE / "provenance.json").read_text())
    assert provenance["case_id"] == "stub_minimal/default"
    assert provenance["executed_at_replay"] is False


def test_stub_recipe_emits_one_stop_only_unit_and_no_components():
    raw_input = _stub_canonical_request()["intent"]["invocations"][0]["raw_input"]
    contributions = emit_stub_minimal(StubMinimalInputV1.model_validate(raw_input))
    kinds = [c.contribution_kind for c in contributions]
    assert kinds == ["process_unit", "constraint_requirement"]
    unit = contributions[0].unit
    steps = [step.kind for step in unit.process_ir.body.steps]
    assert steps == ["passthrough", "stop"]
    assert unit.envelope.depends_on == ()
    assert unit.envelope.process_extensions.connections == ()


@pytest.mark.parametrize(
    "field,value",
    [
        ("goal", "Smoke-test"),
        ("source_label", "Stub source"),
        ("target_label", "Stub target"),
        ("integration_name", "demo-stub-integration"),
    ],
)
def test_stub_recipe_input_refuses_the_legacy_display_fields(field, value):
    with pytest.raises(ValidationError):
        StubMinimalInputV1.model_validate(
            {"version": "1", "process_key": "main_process", field: value}
        )


def test_stub_recipe_registers_without_a_material_channel():
    registry = _stub_registry()
    descriptor = registry.resolve(_STUB_RECIPE_ID, _STUB_VERSION)
    assert descriptor.recipe_version == _STUB_VERSION
    assert registry.component_preparation_for(descriptor) is None


def test_stub_recipe_unit_projects_the_frozen_stub_rows_from_stub_facts():
    """The unit IS stub-projectable: given ``StubRouteProjectionFactsV1`` the
    projector serves exactly the frozen flow and endpoint rows. (How the facts
    reach the workflow for a material-free recipe is an integrator decision.)"""
    from boomi_mcp.authoring.derived_flows import derive_recipe_projection, normalize_flow_row
    from boomi_mcp.models.derived_flows import StubRouteProjectionFactsV1

    raw_input = _stub_canonical_request()["intent"]["invocations"][0]["raw_input"]
    unit = emit_stub_minimal(StubMinimalInputV1.model_validate(raw_input))[0].unit
    projection = derive_recipe_projection(
        [unit],
        [],
        projection_facts={"main_process": StubRouteProjectionFactsV1(process_key="main_process")},
    )
    rows = [
        normalize_flow_row(row.model_dump(mode="json", exclude_unset=True))
        for row in projection.flows
    ]
    endpoints = [row.model_dump(mode="json", exclude_unset=True) for row in projection.endpoints]

    def frozen(name):
        return [
            json.loads(line)["payload"]
            for line in (_STUB_CASE / name).read_text().splitlines()
            if line.strip()
        ]

    assert rows == frozen("flows.baseline.jsonl")
    assert endpoints == frozen("endpoints.baseline.jsonl")
