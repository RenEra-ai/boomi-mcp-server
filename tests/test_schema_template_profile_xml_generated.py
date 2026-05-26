"""Schema-template tests for component / create / profile.xml / xml.generated."""

import json

from boomi_mcp.categories.meta_tools import get_schema_template_action


_FORBIDDEN_TEMPLATE_SUBSTRINGS = (
    "select ",
    "insert ",
    "update ",
    "delete ",
    " from ",
    " where ",
    "<sql>",
    "<dbstatement",
    "<xmlprofile",
    "<xmlelement",
    "<process",
    "<connector",
    "<?xml",
)


def _call(**kwargs):
    return get_schema_template_action(
        resource_type="component",
        operation="create",
        **kwargs,
    )


def test_full_template_returned_for_xml_generated_protocol():
    result = _call(component_type="profile.xml", protocol="xml.generated")
    assert result["_success"] is True
    assert result["component_type"] == "profile.xml"
    assert result["protocol"] == "xml.generated"


def test_unknown_protocol_returns_structured_error():
    result = _call(component_type="profile.xml", protocol="bogus")
    assert result["_success"] is False
    assert "xml.generated" in result["valid_protocols"]


def test_template_documents_required_fields():
    result = _call(component_type="profile.xml", protocol="xml.generated")
    required = result["required"]
    for expected in ("component_type", "profile_type", "component_name", "root"):
        assert expected in required


def test_template_supported_kinds_is_element_only():
    result = _call(component_type="profile.xml", protocol="xml.generated")
    assert result["supported_kinds"] == ["element"]


def test_template_lists_supported_data_types():
    result = _call(component_type="profile.xml", protocol="xml.generated")
    assert set(result["supported_data_types"]) == {
        "character",
        "number",
        "datetime",
        "boolean",
    }


def test_template_lists_unsupported_xml_features():
    result = _call(component_type="profile.xml", protocol="xml.generated")
    unsupported = result["unsupported_features"]
    for expected in ("attributes", "namespaces", "namespace_uri", "xsd"):
        assert expected in unsupported


def test_template_advertises_unsupported_xml_feature_error_code():
    result = _call(component_type="profile.xml", protocol="xml.generated")
    codes = result["error_codes"]
    for expected in (
        "UNSUPPORTED_PROFILE_GENERATION_MODE",
        "UNSUPPORTED_XML_PROFILE_FEATURE",
        "PROFILE_FIELD_VALIDATION_FAILED",
        "DUPLICATE_PROFILE_FIELD_PATH",
        "UNSUPPORTED_PROFILE_FIELD_TYPE",
        "INVALID_PROFILE_FIELD_PATH",
        "PLAINTEXT_SECRET_REJECTED",
    ):
        assert expected in codes


def test_template_example_uses_placeholders_only():
    result = _call(component_type="profile.xml", protocol="xml.generated")
    example_blob = json.dumps(result["example"]).lower()
    for marker in _FORBIDDEN_TEMPLATE_SUBSTRINGS:
        assert marker not in example_blob


def test_template_defaults_have_no_canned_content():
    result = _call(component_type="profile.xml", protocol="xml.generated")
    template_blob = json.dumps(
        [result.get("template", {}), result.get("defaults", {})]
    ).lower()
    for marker in _FORBIDDEN_TEMPLATE_SUBSTRINGS:
        assert marker not in template_blob


def test_template_out_of_scope_points_at_47():
    result = _call(component_type="profile.xml", protocol="xml.generated")
    oos_blob = " ".join(result["out_of_scope"].values())
    assert "#47" in oos_blob
