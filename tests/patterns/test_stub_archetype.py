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
from boomi_mcp.patterns.archetypes import (
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
