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
    # 'function' is now a valid transform.map protocol (issue #40); exercise a
    # truly-unknown protocol instead.
    result = _call(component_type="transform.map", protocol="bogus-protocol")
    assert result["_success"] is False
    assert "direct" in result["valid_protocols"]
    assert "function" in result["valid_protocols"]


def test_function_protocol_routes_to_function_template():
    # Sanity-check that 'function' now resolves to the #40 template.
    result = _call(component_type="transform.map", protocol="function")
    assert result["_success"] is True
    assert result["protocol"] == "function"


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
    # #41 shipped — the raw <scripts> escape hatch now points callers at the
    # structured map_type='script' route instead of a future-work marker.
    assert "map_type='script'" in unsupported.get("scripts", "")
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


def test_template_out_of_scope_points_at_40_42_47_after_41_shipped():
    # After #41 shipped, the direct template's out_of_scope no longer carries
    # a "#41" pointer — reusable script-based transforms are supported via
    # map_type='script'. The remaining future-work markers stay: #40 covers
    # advanced function work (chained graphs / standalone reusable function
    # components), #42 is XSLT. Existing-profile-index discovery is no longer
    # attributed to #47: infer_profile_fields (issue #47) infers from supplied
    # artifacts and does NOT index live profile XML — that stays separate work.
    result = _call(component_type="transform.map", protocol="direct")
    oos_blob = " ".join(result["out_of_scope"].values())
    assert "#40" in oos_blob
    assert "#42" in oos_blob
    assert "infer_profile_fields" in oos_blob
    assert "separate future work" in oos_blob.lower()


def test_template_tool_points_at_build_integration():
    # Codex r2: structured maps must be created via build_integration; the
    # template previously pointed at manage_component which only dispatches
    # profile builders.
    result = _call(component_type="transform.map", protocol="direct")
    assert "build_integration" in result["tool"]
    assert "tool_note" in result
    assert "build_integration" in result["tool_note"]
