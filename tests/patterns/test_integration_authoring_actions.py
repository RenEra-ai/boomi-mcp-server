"""Tests for V3 integration authoring action layer (Issues #18, #19)."""

import json
from unittest.mock import MagicMock

import pytest
from pydantic import BaseModel, ConfigDict

import boomi_mcp.categories.integration_authoring as authoring_module
from boomi_mcp.categories.integration_authoring import (
    build_from_archetype_action,
    get_integration_archetype_action,
    list_integration_archetypes_action,
)
from boomi_mcp.categories.integration_builder import build_integration_action
from boomi_mcp.models.integration_models import IntegrationSpecV1
from boomi_mcp.patterns import (
    ArchetypePattern,
    NoParameters,
    PatternKind,
    PatternMetadata,
    PatternRegistry,
    PatternRegistryError,
    PrimitiveBuildContext,
    PrimitivePattern,
)


# ---------------------------------------------------------------------------
# list_integration_archetypes_action
# ---------------------------------------------------------------------------


def test_list_returns_stub_minimal_integration():
    result = list_integration_archetypes_action()
    assert result["_success"] is True
    assert result["raw_xml_exposed"] is False
    assert result["count"] >= 1
    names = [a["name"] for a in result["archetypes"]]
    assert "stub_minimal_integration" in names
    assert result["query"] is None
    assert result["tags"] is None


def test_list_query_filter_hits_and_misses():
    hit = list_integration_archetypes_action(query="stub")
    assert hit["_success"] is True
    assert "stub_minimal_integration" in [a["name"] for a in hit["archetypes"]]

    miss = list_integration_archetypes_action(query="nonexistent-archetype-xyzzy")
    assert miss["_success"] is True
    assert miss["count"] == 0
    assert miss["archetypes"] == []


def test_list_tags_filter_native_list():
    result = list_integration_archetypes_action(tags=["safe"])
    assert result["_success"] is True
    names = [a["name"] for a in result["archetypes"]]
    assert "stub_minimal_integration" in names


def test_list_tags_filter_comma_string():
    result = list_integration_archetypes_action(tags="safe,test")
    assert result["_success"] is True
    names = [a["name"] for a in result["archetypes"]]
    assert "stub_minimal_integration" in names


def test_list_tags_filter_json_string():
    result = list_integration_archetypes_action(tags='["safe"]')
    assert result["_success"] is True
    names = [a["name"] for a in result["archetypes"]]
    assert "stub_minimal_integration" in names


# ---------------------------------------------------------------------------
# get_integration_archetype_action
# ---------------------------------------------------------------------------


def test_get_returns_metadata_and_parameter_schema():
    result = get_integration_archetype_action("stub_minimal_integration")
    assert result["_success"] is True
    assert result["raw_xml_exposed"] is False
    assert result["next_tool"] == "build_from_archetype"

    arch = result["archetype"]
    assert arch["metadata"]["name"] == "stub_minimal_integration"
    assert arch["metadata"]["version"] == "0.1.0"
    assert arch["metadata"]["kind"] == "archetype"

    schema = arch["parameter_schema"]
    assert "properties" in schema
    assert "integration_name" in schema["properties"]


def test_get_missing_archetype_returns_failure():
    result = get_integration_archetype_action("does-not-exist")
    assert result["_success"] is False
    assert result["error_code"] == "PATTERN_NOT_FOUND"


# ---------------------------------------------------------------------------
# build_from_archetype_action
# ---------------------------------------------------------------------------


def test_build_valid_returns_stub_spec():
    result = build_from_archetype_action(
        "stub_minimal_integration",
        {"integration_name": "demo"},
    )
    assert result["_success"] is True
    assert result["archetype"] == "stub_minimal_integration"
    assert result["archetype_version"] == "0.1.0"
    assert result["raw_xml_exposed"] is False
    assert result["boomi_mutation"] is False

    spec = result["integration_spec"]
    assert spec["name"] == "demo"
    assert spec["mode"] == "redesign"
    assert spec["components"] == []

    assert "build_integration(action='plan'" in result["next_steps"]


def test_build_invalid_parameters_returns_field_error():
    result = build_from_archetype_action("stub_minimal_integration", {})
    assert result["_success"] is False
    assert result["error_code"] == "PARAM_VALIDATION_FAILED"
    paths = [fe["field_path"] for fe in result["field_errors"]]
    assert "integration_name" in paths


def test_build_parameters_as_json_string():
    result = build_from_archetype_action(
        "stub_minimal_integration",
        '{"integration_name": "demo"}',
    )
    assert result["_success"] is True
    assert result["integration_spec"]["name"] == "demo"


def test_build_missing_archetype_returns_failure():
    result = build_from_archetype_action(
        "does-not-exist",
        {"integration_name": "demo"},
    )
    assert result["_success"] is False
    assert result["error_code"] == "PATTERN_NOT_FOUND"


def test_build_bad_parameter_shape_returns_validation_error():
    result = build_from_archetype_action("stub_minimal_integration", "not-json")
    assert result["_success"] is False
    assert result["error_code"] == "PARAM_VALIDATION_FAILED"


# ---------------------------------------------------------------------------
# Cross-cutting: outputs are JSON-serializable and never contain XML
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "callable_factory",
    [
        lambda: list_integration_archetypes_action(),
        lambda: get_integration_archetype_action("stub_minimal_integration"),
        lambda: build_from_archetype_action(
            "stub_minimal_integration",
            {"integration_name": "demo"},
        ),
    ],
)
def test_outputs_have_no_xml_or_primitive_markers(callable_factory):
    result = callable_factory()
    payload = json.dumps(result)
    for marker in ("<?xml", "<process", "<component", "<connector", "<operation"):
        assert marker not in payload, f"Unexpected XML marker {marker!r} in {payload[:200]}..."


# ===========================================================================
# Issue #19 — additional hardening tests
# ===========================================================================


# ---------------------------------------------------------------------------
# Synthetic patterns used to exercise registry behavior without depending on
# whatever real patterns happen to be installed.
# ---------------------------------------------------------------------------


class _SynthArchetypeParameters(BaseModel):
    model_config = ConfigDict(extra="forbid")

    integration_name: str


class _SynthArchetype(ArchetypePattern):
    metadata = PatternMetadata(
        name="_synth_archetype",
        version="0.0.1",
        kind=PatternKind.ARCHETYPE,
        description="Synthetic archetype used only by the Issue #19 tests.",
        tags=["synthetic"],
    )
    parameters_model = _SynthArchetypeParameters

    @classmethod
    def emit_spec(cls, parameters: _SynthArchetypeParameters) -> IntegrationSpecV1:
        return IntegrationSpecV1(version="1.0", name=parameters.integration_name)


class _SynthPrimitive(PrimitivePattern):
    metadata = PatternMetadata(
        name="_synth_primitive",
        version="0.0.1",
        kind=PatternKind.PRIMITIVE,
        description="Synthetic primitive used only by the Issue #19 tests.",
        tags=["synthetic"],
    )
    parameters_model = NoParameters

    @classmethod
    def emit_components(cls, context: PrimitiveBuildContext, parameters):
        return []


class _RecordingArchetype(ArchetypePattern):
    """Archetype whose validate_parameters override records each call."""

    metadata = PatternMetadata(
        name="_recording_archetype",
        version="0.0.1",
        kind=PatternKind.ARCHETYPE,
        description="Records each validate_parameters() call for Issue #19 tests.",
    )
    parameters_model = _SynthArchetypeParameters
    invocations: list = []

    @classmethod
    def validate_parameters(cls, parameters=None):
        cls.invocations.append(parameters)
        return super().validate_parameters(parameters)

    @classmethod
    def emit_spec(cls, parameters: _SynthArchetypeParameters) -> IntegrationSpecV1:
        return IntegrationSpecV1(version="1.0", name=parameters.integration_name)


def _install_registry(monkeypatch, *patterns) -> None:
    registry = PatternRegistry(patterns)
    monkeypatch.setattr(
        authoring_module.PatternRegistry,
        "from_package",
        classmethod(lambda cls, *_a, **_kw: registry),
    )


# ---------------------------------------------------------------------------
# list_integration_archetypes_action — registry filtering / discovery errors
# ---------------------------------------------------------------------------


def test_list_excludes_primitives(monkeypatch):
    _install_registry(monkeypatch, _SynthArchetype, _SynthPrimitive)
    result = list_integration_archetypes_action()
    assert result["_success"] is True
    names = [a["name"] for a in result["archetypes"]]
    kinds = {a["kind"] for a in result["archetypes"]}
    assert "_synth_archetype" in names
    assert "_synth_primitive" not in names
    assert kinds == {"archetype"}


def test_list_returns_structured_error_when_discovery_fails(monkeypatch):
    def _boom(cls, *_a, **_kw):
        raise PatternRegistryError(
            error_code="PATTERN_DISCOVERY_FAILED",
            error="boom — synthetic discovery failure",
            suggestion="Fix the broken module.",
        )

    monkeypatch.setattr(
        authoring_module.PatternRegistry,
        "from_package",
        classmethod(_boom),
    )
    result = list_integration_archetypes_action()
    assert result["_success"] is False
    assert result["error_code"] == "PATTERN_DISCOVERY_FAILED"
    assert "synthetic discovery failure" in result["error"]


def test_list_invalid_tags_returns_structured_error():
    result = list_integration_archetypes_action(tags=42)
    assert result["_success"] is False
    assert result["error_code"] == "INVALID_INPUT"
    assert "tags" in result["error"].lower()


def test_list_invalid_query_returns_structured_error():
    result = list_integration_archetypes_action(query=123)
    assert result["_success"] is False
    assert result["error_code"] == "INVALID_INPUT"
    assert "query" in result["error"].lower()


# ---------------------------------------------------------------------------
# get_integration_archetype_action — schema hardening
# ---------------------------------------------------------------------------


def test_get_schema_is_machine_readable_and_strict():
    result = get_integration_archetype_action("stub_minimal_integration")
    assert result["_success"] is True
    arch = result["archetype"]

    md = arch["metadata"]
    assert md["name"] == "stub_minimal_integration"
    assert md["version"] == "0.1.0"
    assert md["kind"] == "archetype"
    assert isinstance(md["tags"], list)
    assert isinstance(md["use_cases"], list)
    assert isinstance(md["not_for"], list)

    schema = arch["parameter_schema"]
    assert isinstance(schema, dict)
    assert schema.get("type") == "object"
    assert schema.get("additionalProperties") is False, (
        "Pattern parameter schemas must reject extra fields"
    )
    assert "integration_name" in schema["properties"]
    assert "integration_name" in schema.get("required", [])

    payload = json.dumps(result)
    for marker in (
        "<?xml",
        "<process",
        "<component",
        "<connector",
        "<operation",
        "boomi.models",
        "BoomiClient",
    ):
        assert marker not in payload, f"Unexpected leakage marker {marker!r}"


def test_get_returns_structured_error_when_discovery_fails(monkeypatch):
    def _boom(cls, *_a, **_kw):
        raise PatternRegistryError(
            error_code="PATTERN_DISCOVERY_FAILED",
            error="discovery exploded",
        )

    monkeypatch.setattr(
        authoring_module.PatternRegistry,
        "from_package",
        classmethod(_boom),
    )
    result = get_integration_archetype_action("stub_minimal_integration")
    assert result["_success"] is False
    assert result["error_code"] == "PATTERN_DISCOVERY_FAILED"


# ---------------------------------------------------------------------------
# build_from_archetype_action — validation path + secret hygiene
# ---------------------------------------------------------------------------


def test_build_uses_validate_parameters_entry_point(monkeypatch):
    _RecordingArchetype.invocations.clear()
    _install_registry(monkeypatch, _RecordingArchetype)

    result = build_from_archetype_action(
        "_recording_archetype",
        {"integration_name": "demo"},
    )
    assert result["_success"] is True, result
    assert _RecordingArchetype.invocations, (
        "build_from_archetype_action must route through cls.validate_parameters()"
    )
    assert _RecordingArchetype.invocations[0] == {"integration_name": "demo"}


def test_build_validation_error_does_not_echo_input_values():
    secret = "sk_live_super_secret_token_DEADBEEF"
    result = build_from_archetype_action(
        "stub_minimal_integration",
        {"integration_name": "  ", "rogue_field": secret},
    )
    assert result["_success"] is False
    assert result["error_code"] == "PARAM_VALIDATION_FAILED"
    assert result["field_errors"], "expected per-field validation errors"
    for fe in result["field_errors"]:
        assert {"field_path", "message"}.issubset(fe.keys())

    payload = json.dumps(result)
    assert secret not in payload, (
        "PARAM_VALIDATION_FAILED responses must not echo caller-supplied values"
    )


def test_build_success_spec_is_accepted_by_build_integration_plan():
    built = build_from_archetype_action(
        "stub_minimal_integration",
        {"integration_name": "demo"},
    )
    assert built["_success"] is True
    assert built["raw_xml_exposed"] is False
    assert built["boomi_mutation"] is False

    json.dumps(built)  # must be JSON-serializable

    plan = build_integration_action(
        MagicMock(),
        "test-profile",
        "plan",
        {
            "integration_spec": built["integration_spec"],
            "conflict_policy": "reuse",
        },
    )
    assert plan["_success"] is True, plan
    assert plan["steps"] == []
    assert plan["warnings"] is not None
    assert any("zero executable steps" in w for w in plan["warnings"])


def test_get_action_returns_enriched_describe_payload():
    """get_integration_archetype_action surfaces the new describe() fields end-to-end."""
    result = get_integration_archetype_action("stub_minimal_integration")
    assert result["_success"] is True
    arch = result["archetype"]

    # The 4 enrichment keys are present alongside the legacy metadata/parameter_schema.
    for key in ("metadata", "parameter_schema", "capability_notes", "limitations", "examples", "example_policy"):
        assert key in arch, f"archetype payload missing {key!r}"

    assert arch["example_policy"] == "example_only_not_reusable_template"
    assert arch["capability_notes"], "stub archetype must publish capability_notes"
    assert arch["limitations"], "stub archetype must publish limitations"
    assert arch["examples"], "stub archetype must publish at least one example"

    for example in arch["examples"]:
        assert example["is_template"] is False
        assert example["template_status"] == "example_only_not_reusable_template"

    # Every parameter property has a description, so an LLM client can fill the
    # schema without reading source.
    props = arch["parameter_schema"]["properties"]
    assert props, "parameter_schema must expose properties"
    for prop_name, prop_schema in props.items():
        assert prop_schema.get("description"), (
            f"property {prop_name!r} is missing a description"
        )

    # Whole payload remains JSON-serializable.
    json.dumps(result)
