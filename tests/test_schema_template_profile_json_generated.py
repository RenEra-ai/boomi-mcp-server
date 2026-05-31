"""Schema-template tests for component / create / profile.json / json.generated.

Issue #26 (M2.6). Mirrors the assertion patterns from
``test_schema_template_connector_action_database_get.py``.
"""

import json

from boomi_mcp.categories.meta_tools import get_schema_template_action


_FORBIDDEN_TEMPLATE_SUBSTRINGS = (
    "select ",
    "insert ",
    "update ",
    "delete ",
    " from ",
    " where ",
    " join ",
    " group by ",
    " order by ",
    "<sql>",
    "<dbstatement",
    "<jsonprofile",
    "<jsonroot",
    "<jsonobject",
    "<process",
    "<connector",
    "<?xml",
    "$filter=",
    "$select=",
    "$expand=",
)


def _call(**kwargs):
    return get_schema_template_action(
        resource_type="component",
        operation="create",
        **kwargs,
    )


def test_full_template_returned_for_json_generated_protocol():
    result = _call(component_type="profile.json", protocol="json.generated")
    assert result["_success"] is True
    assert result["component_type"] == "profile.json"
    assert result["protocol"] == "json.generated"


def test_no_protocol_returns_json_generated_default():
    result = _call(component_type="profile.json")
    assert result["_success"] is True
    assert result["protocol"] == "json.generated"


def test_unknown_protocol_returns_structured_error():
    result = _call(component_type="profile.json", protocol="bogus")
    assert result["_success"] is False
    assert "json.generated" in result["valid_protocols"]


def test_template_documents_required_fields():
    result = _call(component_type="profile.json", protocol="json.generated")
    required = result["required"]
    for expected in ("component_type", "profile_type", "component_name", "root"):
        assert expected in required


def test_template_lists_supported_data_types_and_kinds():
    result = _call(component_type="profile.json", protocol="json.generated")
    assert set(result["supported_data_types"]) == {
        "character",
        "number",
        "datetime",
        "boolean",
    }
    assert set(result["supported_kinds"]) == {"simple", "object", "array"}


def test_template_advertises_issue_26_error_codes():
    result = _call(component_type="profile.json", protocol="json.generated")
    codes = result["error_codes"]
    for expected in (
        "UNSUPPORTED_PROFILE_GENERATION_MODE",
        "PROFILE_FIELD_VALIDATION_FAILED",
        "DUPLICATE_PROFILE_FIELD_PATH",
        "UNSUPPORTED_PROFILE_FIELD_TYPE",
        "INVALID_PROFILE_FIELD_PATH",
        "PLAINTEXT_SECRET_REJECTED",
    ):
        assert expected in codes, f"missing error code {expected}"


def test_template_example_uses_placeholders_only():
    result = _call(component_type="profile.json", protocol="json.generated")
    example_blob = json.dumps(result["example"]).lower()
    for marker in _FORBIDDEN_TEMPLATE_SUBSTRINGS:
        assert marker not in example_blob, (
            f"profile.json/json.generated example contains forbidden marker {marker!r}"
        )


def test_template_defaults_have_no_canned_content():
    result = _call(component_type="profile.json", protocol="json.generated")
    template_blob = json.dumps(
        [result.get("template", {}), result.get("defaults", {})]
    ).lower()
    for marker in _FORBIDDEN_TEMPLATE_SUBSTRINGS:
        assert marker not in template_blob


def test_template_field_tree_rules_describe_path_convention():
    result = _call(component_type="profile.json", protocol="json.generated")
    rules_blob = " ".join(result["field_tree_rules"]).lower()
    assert "object" in rules_blob
    assert "[]" in rules_blob


def test_template_out_of_scope_points_at_47():
    result = _call(component_type="profile.json", protocol="json.generated")
    oos_blob = " ".join(result["out_of_scope"].values())
    assert "#47" in oos_blob


def test_inferred_from_sample_json_points_at_infer_tool():
    result = _call(component_type="profile.json", protocol="json.generated")
    note = result["out_of_scope"]["inferred_from_sample_json"]
    assert "infer_profile_fields" in note
    assert "profile_from_sample_json" in note
    assert "#47" in note  # issue tag retained
