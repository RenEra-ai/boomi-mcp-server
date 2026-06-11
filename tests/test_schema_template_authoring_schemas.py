"""Unit tests for get_schema_template's schema_name selector (Issue #10).

Pure-unit against boomi_mcp.categories.meta_tools — no server import, no SDK
calls. Covers the four schema_name families, error envelopes, selector
precedence, and legacy resource_type compatibility.
"""

import sys
from pathlib import Path

_src = str(Path(__file__).resolve().parent.parent / "src")
if _src not in sys.path:
    sys.path.insert(0, _src)

from boomi_mcp.categories.meta_tools import (
    _valid_schema_names,
    get_schema_template_action,
)
from boomi_mcp.errors import (
    SCHEMA_NAME_UNSUPPORTED,
    SCHEMA_SELECTOR_REQUIRED,
    WORKFLOW_SEQUENCE_NOT_FOUND,
)


# ---------------------------------------------------------------------------
# IntegrationSpecV1
# ---------------------------------------------------------------------------


def test_integration_spec_v1_returns_json_schema():
    result = get_schema_template_action(schema_name="IntegrationSpecV1")
    assert result["_success"] is True
    assert result["schema_name"] == "IntegrationSpecV1"
    assert "components" in result["json_schema"]["properties"]
    assert result["raw_xml_exposed"] is False
    assert result["boomi_mutation"] is False
    assert "archetype" in result["hint"]  # archetype-first pointer


# ---------------------------------------------------------------------------
# archetype:<name>
# ---------------------------------------------------------------------------


def test_archetype_schema_returns_parameter_schema_and_metadata():
    result = get_schema_template_action(schema_name="archetype:database_to_api_sync")
    assert result["_success"] is True
    assert result["schema_name"] == "archetype:database_to_api_sync"
    assert result["metadata"]["name"] == "database_to_api_sync"
    assert "properties" in result["parameter_schema"]
    assert result["example_policy"] == "example_only_not_reusable_template"
    assert result["raw_xml_exposed"] is False
    assert result["boomi_mutation"] is False


def test_unknown_archetype_returns_schema_name_unsupported():
    result = get_schema_template_action(schema_name="archetype:__bogus__")
    assert result["_success"] is False
    assert result["error_code"] == SCHEMA_NAME_UNSUPPORTED
    assert "valid_schema_names" in result
    assert "archetype:database_to_api_sync" in result["valid_schema_names"]


# ---------------------------------------------------------------------------
# workflow_sequences / workflow:<name>
# ---------------------------------------------------------------------------


def test_workflow_sequences_returns_all_sequences_and_record_schema():
    result = get_schema_template_action(schema_name="workflow_sequences")
    assert result["_success"] is True
    assert "build_integration_from_description" in result["workflow_sequences"]
    assert result["record_schema"]["required"] == ["description", "steps"]
    assert result["raw_xml_exposed"] is False
    assert result["boomi_mutation"] is False


def test_single_workflow_is_profile_first():
    result = get_schema_template_action(
        schema_name="workflow:build_integration_from_description"
    )
    assert result["_success"] is True
    assert "list_boomi_profiles" in result["workflow"]["steps"][0]


def test_unknown_workflow_returns_workflow_sequence_not_found():
    result = get_schema_template_action(schema_name="workflow:__bogus__")
    assert result["_success"] is False
    assert result["error_code"] == WORKFLOW_SEQUENCE_NOT_FOUND
    assert "build_integration_from_description" in result["valid_workflows"]


# ---------------------------------------------------------------------------
# Selector envelope behavior
# ---------------------------------------------------------------------------


def test_unknown_schema_name_lists_valid_names():
    result = get_schema_template_action(schema_name="__bogus__")
    assert result["_success"] is False
    assert result["error_code"] == SCHEMA_NAME_UNSUPPORTED
    assert "IntegrationSpecV1" in result["valid_schema_names"]
    assert "workflow_sequences" in result["valid_schema_names"]


def test_missing_both_selectors_returns_selector_required():
    result = get_schema_template_action()
    assert result["_success"] is False
    assert result["error_code"] == SCHEMA_SELECTOR_REQUIRED
    assert "valid_types" in result
    assert "valid_schema_names" in result


def test_schema_name_takes_precedence_over_resource_type():
    result = get_schema_template_action(
        resource_type="process", schema_name="IntegrationSpecV1"
    )
    assert result["_success"] is True
    assert result["schema_name"] == "IntegrationSpecV1"


def test_legacy_resource_type_path_unchanged():
    result = get_schema_template_action(resource_type="process", operation="create")
    assert result["_success"] is True


def test_valid_schema_names_covers_all_families():
    names = _valid_schema_names()
    assert "IntegrationSpecV1" in names
    assert "workflow_sequences" in names
    assert any(n.startswith("workflow:") for n in names)
    assert any(n.startswith("archetype:") for n in names)
