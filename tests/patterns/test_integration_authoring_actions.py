"""Tests for V3 integration authoring action layer (Issue #18)."""

import json

import pytest

from boomi_mcp.categories.integration_authoring import (
    build_from_archetype_action,
    get_integration_archetype_action,
    list_integration_archetypes_action,
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
