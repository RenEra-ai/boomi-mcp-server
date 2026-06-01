"""Schema-template tests for component / create / transform.map / script (#41)."""

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
    "<functionstep ",
    "<scripting ",
    "<scripttoexecute",
    "<process",
    "<connector",
    "<?xml",
    " def ",
    "import org.",
    "import groovy.",
    "function (",
    "function(",
)


def _call(**kwargs):
    return get_schema_template_action(
        resource_type="component",
        operation="create",
        **kwargs,
    )


def test_template_resolves_for_script_protocol():
    result = _call(component_type="transform.map", protocol="script")
    assert result["_success"] is True
    assert result["protocol"] == "script"


def test_map_script_protocol_alias_also_resolves():
    result = _call(component_type="transform.map", protocol="map_script")
    assert result["_success"] is True


def test_unknown_protocol_lists_script_alongside_others():
    result = _call(component_type="transform.map", protocol="bogus")
    assert result["_success"] is False
    valid = result["valid_protocols"]
    for expected in ("direct", "function", "map_function", "script", "map_script"):
        assert expected in valid


def test_template_required_fields_include_script_mappings():
    result = _call(component_type="transform.map", protocol="script")
    for expected in (
        "component_type",
        "map_type",
        "component_name",
        "source_profile_id",
        "source_profile_type",
        "target_profile_id",
        "target_profile_type",
        "script_mappings",
    ):
        assert expected in result["required"]


def test_template_lists_field_mappings_as_optional():
    result = _call(component_type="transform.map", protocol="script")
    assert "field_mappings" in result.get("optional", [])


def test_template_lists_supported_map_types():
    result = _call(component_type="transform.map", protocol="script")
    assert "script" in result["supported_map_types"]
    assert "map_script" in result["supported_map_types"]


def test_template_documents_script_component_id_ref_rule():
    # After Codex r3 P1 #1, the rule now documents wrapper auto-synthesis
    # rather than the old "list script_key in depends_on" rule. The
    # synthesized wrapper is auto-added to depends_on, so the caller's
    # surface contract is simpler: declare the script.mapping + reference
    # it via '$ref:<script_key>'.
    result = _call(component_type="transform.map", protocol="script")
    rule = result["script_component_id_rule"]
    assert "$ref" in rule
    assert "wrapper" in rule.lower() or "synthes" in rule.lower()


def test_template_documents_in_map_xml_shape_explicitly():
    # The in-map shape is empty <Configuration/> with userdefined category,
    # NOT the standalone <Scripting>/<ScriptToExecute> shape. The template
    # must call this out so authors don't try to inject inline script
    # bodies through unsupported keys.
    result = _call(component_type="transform.map", protocol="script")
    note = result["in_map_xml_shape_note"]
    assert "userdefined" in note.lower()
    assert "<Configuration/>" in note or "Configuration/" in note


def test_template_advertises_issue_41_error_codes():
    result = _call(component_type="transform.map", protocol="script")
    codes = result["error_codes"]
    for expected in (
        # Reused from #26 / #40.
        "MAP_PROFILE_REF_REQUIRED",
        "MAP_PROFILE_INDEX_UNAVAILABLE",
        "MAP_FIELD_NOT_FOUND",
        "DUPLICATE_TARGET_MAPPING",
        "UNSUPPORTED_TRANSFORM_ROUTE",
        "PROFILE_FIELD_NOT_MAPPABLE",
        "PROFILE_FIELD_VALIDATION_FAILED",
        "PLAINTEXT_SECRET_REJECTED",
        # New for #41.
        "SCRIPT_MAPPING_REF_REQUIRED",
    ):
        assert expected in codes


def test_template_depends_on_documents_three_ref_classes():
    result = _call(component_type="transform.map", protocol="script")
    deps_blob = " ".join(result["depends_on_requirements"]).lower()
    assert "source_profile_id" in deps_blob
    assert "target_profile_id" in deps_blob
    assert "script_component_id" in deps_blob


def test_template_unsupported_routes_keep_function_mappings_rejected():
    result = _call(component_type="transform.map", protocol="script")
    unsupported = result["unsupported_routes"]
    # function_mappings belongs to map_type='function'.
    assert "map_type='function'" in unsupported.get("function_mappings", "") \
        or "function" in unsupported.get("function_mappings", "").lower()
    # Raw <Functions> XML stays rejected.
    assert unsupported.get("functions")
    # XSLT still future (#42).
    assert "#42" in unsupported.get("xslt", "")


def test_template_example_uses_placeholder_values_only():
    result = _call(component_type="transform.map", protocol="script")
    example_blob = json.dumps(result["example"]).lower()
    for marker in _FORBIDDEN_TEMPLATE_SUBSTRINGS:
        assert marker not in example_blob, (
            f"transform.map/script example contains forbidden marker {marker!r}"
        )


def test_template_skeleton_carries_only_placeholders():
    result = _call(component_type="transform.map", protocol="script")
    template_blob = json.dumps(
        [result.get("template", {}), result.get("defaults", {})]
    ).lower()
    for marker in _FORBIDDEN_TEMPLATE_SUBSTRINGS:
        assert marker not in template_blob


def test_template_tool_points_at_build_integration():
    result = _call(component_type="transform.map", protocol="script")
    assert "build_integration" in result["tool"]
    assert "tool_note" in result
    assert "build_integration" in result["tool_note"]


def test_template_out_of_scope_documents_script_processing_is_not_a_fallback():
    result = _call(component_type="transform.map", protocol="script")
    out_of_scope = result["out_of_scope"]
    assert "script_processing_fallback" in out_of_scope
    blob = out_of_scope["script_processing_fallback"].lower()
    assert "fallback" in blob


def test_template_out_of_scope_documents_chained_graphs_future():
    result = _call(component_type="transform.map", protocol="script")
    assert "chained_script_graphs" in result["out_of_scope"]


def test_template_out_of_scope_keeps_42_and_clarifies_existing_profile_index():
    # #42 (XSLT) stays a future-work pointer. Existing-profile-index discovery
    # is no longer attributed to #47: infer_profile_fields (issue #47) infers
    # from supplied artifacts and does NOT index live profile XML.
    result = _call(component_type="transform.map", protocol="script")
    oos_blob = " ".join(result["out_of_scope"].values())
    assert "#42" in oos_blob
    assert "infer_profile_fields" in oos_blob
    assert "separate future work" in oos_blob.lower()


def test_template_recommended_workflow_lists_script_component_step():
    result = _call(component_type="transform.map", protocol="script")
    workflow_blob = " ".join(result["recommended_workflow"]).lower()
    assert "script.mapping" in workflow_blob
