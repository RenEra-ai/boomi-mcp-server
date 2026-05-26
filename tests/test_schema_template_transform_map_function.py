"""Schema-template tests for component / create / transform.map / function."""

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
    "<function ",
    "<simplelookup>",
    "<sequentialvalue ",
    "<defaults>",
    "<default tokey",
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


def test_full_template_returned_for_function_protocol():
    result = _call(component_type="transform.map", protocol="function")
    assert result["_success"] is True
    assert result["component_type"] == "transform.map"
    assert result["protocol"] == "function"


def test_map_function_protocol_alias_also_resolves():
    result = _call(component_type="transform.map", protocol="map_function")
    assert result["_success"] is True
    assert result["component_type"] == "transform.map"


def test_unknown_transform_map_protocol_lists_function_alongside_direct():
    result = _call(component_type="transform.map", protocol="bogus")
    assert result["_success"] is False
    assert "direct" in result["valid_protocols"]
    assert "function" in result["valid_protocols"]
    assert "map_function" in result["valid_protocols"]


def test_template_documents_required_fields_including_function_mappings():
    result = _call(component_type="transform.map", protocol="function")
    required = result["required"]
    for expected in (
        "component_type",
        "map_type",
        "component_name",
        "source_profile_id",
        "source_profile_type",
        "target_profile_id",
        "target_profile_type",
        "function_mappings",
    ):
        assert expected in required, f"missing required field {expected!r}"


def test_template_lists_field_mappings_as_optional():
    result = _call(component_type="transform.map", protocol="function")
    assert "field_mappings" in result.get("optional", [])


def test_template_documents_all_14_supported_function_types():
    result = _call(component_type="transform.map", protocol="function")
    supported = result["supported_function_types"]
    for name in (
        "date_format",
        "default_value",
        "trim",
        "left_trim",
        "right_trim",
        "uppercase",
        "lowercase",
        "append",
        "prepend",
        "replace",
        "remove",
        "simple_lookup",
        "sequential_value",
        "math",
    ):
        assert name in supported, f"missing {name!r} in supported_function_types"


def test_template_documents_sequential_value_authorable_params():
    # Codex r5 fix: sequential_value now exposes Key Name / Fix to Length /
    # Batch Size as authorable parameters (verified live as Input default
    # attributes in the component XML).
    result = _call(component_type="transform.map", protocol="function")
    sv = result["supported_function_types"]["sequential_value"]
    assert "key_name" in sv["required_parameters"]
    assert "fix_to_length" in sv["optional_parameters"]
    assert "batch_size" in sv["optional_parameters"]


def test_template_math_lists_all_8_operations():
    result = _call(component_type="transform.map", protocol="function")
    math = result["supported_function_types"]["math"]
    ops = math["supported_operations"]
    for op in (
        "add",
        "subtract",
        "multiply",
        "divide",
        "set_precision",
        "ceil",
        "floor",
        "abs",
    ):
        assert op in ops


def test_template_math_does_not_advertise_rounding_mode():
    # Codex r2: rounding_mode was previously listed as an optional parameter
    # but the builder rejects it; the template must not promise it either.
    result = _call(component_type="transform.map", protocol="function")
    math = result["supported_function_types"]["math"]
    assert "rounding_mode" not in math.get("optional_parameters", [])


def test_template_math_documents_precision_applicability():
    # Codex r2: precision only valid for set_precision — the template must
    # warn callers so they don't request it for other operations.
    result = _call(component_type="transform.map", protocol="function")
    math = result["supported_function_types"]["math"]
    assert "applicability" in math
    assert "set_precision" in math["applicability"]


def test_template_tool_points_at_build_integration():
    # Codex r2: structured maps must be created via build_integration; the
    # template previously pointed at manage_component which only dispatches
    # profile builders.
    result = _call(component_type="transform.map", protocol="function")
    assert "build_integration" in result["tool"]
    assert "tool_note" in result
    assert "build_integration" in result["tool_note"]


def test_template_advertises_issue_40_error_codes():
    result = _call(component_type="transform.map", protocol="function")
    codes = result["error_codes"]
    for expected in (
        # #26 codes still apply.
        "MAP_PROFILE_REF_REQUIRED",
        "MAP_PROFILE_INDEX_UNAVAILABLE",
        "MAP_FIELD_NOT_FOUND",
        "DUPLICATE_TARGET_MAPPING",
        "UNSUPPORTED_TRANSFORM_ROUTE",
        "PROFILE_FIELD_NOT_MAPPABLE",
        "PROFILE_FIELD_VALIDATION_FAILED",
        "PLAINTEXT_SECRET_REJECTED",
        # #40 additions.
        "UNSUPPORTED_MAP_FUNCTION_TYPE",
        "MAP_FUNCTION_INPUT_COUNT_MISMATCH",
        "MAP_FUNCTION_PARAMETER_MISSING",
        "MAP_FUNCTION_PARAMETER_INVALID",
        "UNSUPPORTED_MATH_OPERATION",
    ):
        assert expected in codes


def test_template_example_uses_placeholders_only():
    result = _call(component_type="transform.map", protocol="function")
    example_blob = json.dumps(result["example"]).lower()
    for marker in _FORBIDDEN_TEMPLATE_SUBSTRINGS:
        assert marker not in example_blob, (
            f"transform.map/function example contains forbidden marker {marker!r}"
        )


def test_template_defaults_have_no_canned_content():
    result = _call(component_type="transform.map", protocol="function")
    template_blob = json.dumps(
        [result.get("template", {}), result.get("defaults", {})]
    ).lower()
    for marker in _FORBIDDEN_TEMPLATE_SUBSTRINGS:
        assert marker not in template_blob


def test_template_unsupported_routes_point_at_41_42():
    result = _call(component_type="transform.map", protocol="function")
    unsupported = result["unsupported_routes"]
    # Raw <Functions> XML escape hatch stays rejected.
    assert "structured function_mappings" in unsupported.get("functions", "").lower() \
        or "structured" in unsupported.get("functions", "").lower()
    assert "#41" in unsupported.get("scripts", "")
    assert "#42" in unsupported.get("xslt", "")


def test_template_depends_on_requirements_mention_dollar_ref():
    result = _call(component_type="transform.map", protocol="function")
    deps_blob = " ".join(result["depends_on_requirements"])
    assert "$ref" in deps_blob


def test_template_out_of_scope_points_at_41_42_47_and_advanced_function_work():
    result = _call(component_type="transform.map", protocol="function")
    oos_blob = " ".join(result["out_of_scope"].values())
    oos_keys = set(result["out_of_scope"].keys())
    assert "#41" in oos_blob
    assert "#42" in oos_blob
    assert "#47" in oos_blob
    assert "standalone" in oos_blob.lower() or "reusable" in oos_blob.lower()
    # Multi-step / chained function pipelines are documented as future work.
    assert "chained_function_graphs" in oos_keys
    assert "multi-step" in oos_blob.lower() or "pipeline" in oos_blob.lower()


def test_template_lists_supported_map_types():
    result = _call(component_type="transform.map", protocol="function")
    assert "function" in result["supported_map_types"]
    assert "map_function" in result["supported_map_types"]
