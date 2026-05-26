"""Schema-template tests for component / create / transform.map / direct."""

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
    "<map xmlns",
    "<map fromprofile",
    "<mapping fromkey",
    "<process",
    "<connector",
    "<?xml",
    " def ",
    "import ",
    "groovy",
)


def _call(**kwargs):
    return get_schema_template_action(
        resource_type="component",
        operation="create",
        **kwargs,
    )


def test_full_template_returned_for_direct_protocol():
    result = _call(component_type="transform.map", protocol="direct")
    assert result["_success"] is True
    assert result["component_type"] == "transform.map"
    assert result["protocol"] == "direct"


def test_no_protocol_returns_direct_default():
    result = _call(component_type="transform.map")
    assert result["_success"] is True
    assert result["protocol"] == "direct"


def test_unknown_protocol_returns_structured_error():
    result = _call(component_type="transform.map", protocol="function")
    assert result["_success"] is False
    assert "direct" in result["valid_protocols"]


def test_template_documents_required_fields():
    result = _call(component_type="transform.map", protocol="direct")
    required = result["required"]
    for expected in (
        "component_type",
        "map_type",
        "component_name",
        "source_profile_id",
        "source_profile_type",
        "target_profile_id",
        "target_profile_type",
        "field_mappings",
    ):
        assert expected in required


def test_template_lists_unsupported_routes_with_issue_pointers():
    result = _call(component_type="transform.map", protocol="direct")
    unsupported = result["unsupported_routes"]
    assert "#40" in unsupported.get("functions", "")
    assert "#41" in unsupported.get("scripts", "")
    assert "#42" in unsupported.get("xslt", "")
    assert "lookup" in unsupported
    assert "expression" in unsupported


def test_template_advertises_issue_26_error_codes():
    result = _call(component_type="transform.map", protocol="direct")
    codes = result["error_codes"]
    for expected in (
        "MAP_PROFILE_REF_REQUIRED",
        "MAP_PROFILE_INDEX_UNAVAILABLE",
        "MAP_FIELD_NOT_FOUND",
        "DUPLICATE_TARGET_MAPPING",
        "UNSUPPORTED_TRANSFORM_ROUTE",
        "PROFILE_FIELD_NOT_MAPPABLE",
        "PROFILE_FIELD_VALIDATION_FAILED",
        "PLAINTEXT_SECRET_REJECTED",
    ):
        assert expected in codes


def test_template_example_uses_placeholders_only():
    result = _call(component_type="transform.map", protocol="direct")
    example_blob = json.dumps(result["example"]).lower()
    for marker in _FORBIDDEN_TEMPLATE_SUBSTRINGS:
        assert marker not in example_blob, (
            f"transform.map/direct example contains forbidden marker {marker!r}"
        )


def test_template_defaults_have_no_canned_content():
    result = _call(component_type="transform.map", protocol="direct")
    template_blob = json.dumps(
        [result.get("template", {}), result.get("defaults", {})]
    ).lower()
    for marker in _FORBIDDEN_TEMPLATE_SUBSTRINGS:
        assert marker not in template_blob


def test_template_depends_on_requirements_mention_dollar_ref():
    result = _call(component_type="transform.map", protocol="direct")
    deps_blob = " ".join(result["depends_on_requirements"])
    assert "$ref" in deps_blob


def test_template_out_of_scope_points_at_40_41_42_47():
    result = _call(component_type="transform.map", protocol="direct")
    oos_blob = " ".join(result["out_of_scope"].values())
    assert "#40" in oos_blob
    assert "#41" in oos_blob
    assert "#42" in oos_blob
    assert "#47" in oos_blob
