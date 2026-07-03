"""Schema-template tests for component / create / processproperty (#131).

The typed create template replaces the generic raw-XML fallback, making the
list_capabilities `processproperty` advertisement truthful. Also covers the
read-only `schema_name='process_property'` authoring reference surface.
"""

import sys
from pathlib import Path

_project_root = str(Path(__file__).resolve().parent.parent)
if _project_root not in sys.path:
    sys.path.insert(0, _project_root)

from boomi_mcp.categories.meta_tools import (
    _valid_schema_names,
    get_schema_template_action,
)


def _create_template():
    result = get_schema_template_action(
        resource_type="component",
        operation="create",
        component_type="processproperty",
    )
    assert result["_success"] is True
    return result


def test_template_resolves_typed_not_generic_fallback():
    result = _create_template()
    assert result["component_type"] == "processproperty"
    # The generic raw-XML fallback advertises config.xml as the only path;
    # the typed template must carry a structured properties template instead.
    assert "properties" in result["template"]
    assert result["raw_xml_exposed"] is False


def test_template_required_and_property_contract():
    result = _create_template()
    assert result["required"] == ["component_type", "component_name", "properties"]
    assert result["property_required"] == ["key", "name", "type"]
    assert set(result["property_optional"]) == {
        "default_value",
        "help_text",
        "persisted",
    }
    assert result["supported_property_types"] == [
        "string",
        "number",
        "boolean",
        "date",
        "password",
    ]


def test_template_documents_key_coupling_and_update_warning():
    result = _create_template()
    assert "process_property_key" in result["map_function_cross_link"]
    assert "$ref" in result["map_function_cross_link"]
    # The v1 full-subtree-replacement warning must be explicit.
    assert "allowedValueSet" in result["update_note"]
    # No per-property encrypted flag exists.
    assert "encrypted" in result["no_encrypted_field_note"]


def test_template_documents_password_policy_and_hidden_mapping():
    result = _create_template()
    # Password defaults must be empty (plaintext-XML policy), values belong
    # in environment extensions / runtime overrides.
    note = result["password_default_note"]
    assert "empty" in note
    assert "PLAINTEXT" in note or "plaintext" in note.lower()
    assert "extensions" in note
    # The UI 'Hidden' -> XML 'password' mapping is documented.
    evidence = result["property_type_evidence_note"]
    assert "Hidden" in evidence
    assert "password" in evidence
    # The type-unsupported error text reflects the five-token set.
    assert "password" in result["error_codes"]["PROCESS_PROPERTY_TYPE_UNSUPPORTED"]


def test_template_error_codes_cover_builder_matrix():
    codes = set(_create_template()["error_codes"])
    assert {
        "PROCESS_PROPERTY_VALIDATION_FAILED",
        "PROCESS_PROPERTY_NAME_REQUIRED",
        "PROCESS_PROPERTY_PROPERTY_REQUIRED",
        "PROCESS_PROPERTY_KEY_REQUIRED",
        "PROCESS_PROPERTY_KEY_INVALID",
        "PROCESS_PROPERTY_TYPE_UNSUPPORTED",
        "PROCESS_PROPERTY_DUPLICATE_KEY",
        "PROCESS_PROPERTY_DUPLICATE_NAME",
        "PROCESS_PROPERTY_DEFAULT_INVALID",
        "PROCESS_PROPERTY_RAW_XML_UNSUPPORTED",
        "PLAINTEXT_SECRET_REJECTED",
    } <= codes


def test_process_property_schema_name_surface():
    assert "process_property" in _valid_schema_names()
    result = get_schema_template_action(schema_name="process_property")
    assert result["_success"] is True
    assert result["read_only"] is True
    assert result["raw_xml_exposed"] is False
    assert "process_property_key" in result["key_label_coupling"]
    assert "component_type='processproperty'" in result["create_template_pointer"]
