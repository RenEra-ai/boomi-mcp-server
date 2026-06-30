"""Schema-template tests for component / create / script.mapping (#41)."""

import json

from boomi_mcp.categories.meta_tools import get_schema_template_action


_FORBIDDEN_TEMPLATE_SUBSTRINGS = (
    # SQL: no canned queries inside script bodies / examples.
    "select ",
    "insert ",
    "update ",
    "delete ",
    " from ",
    " where ",
    "<sql>",
    # Boomi component XML: the template must NOT ship raw component XML.
    "<bns:component",
    "<mappingscript",
    "<process",
    "<connector",
    "<?xml",
    # Groovy / JavaScript business markers: no canned script content.
    "import org.",
    "import groovy.",
    " def ",
    "function (",
    "function(",
)


def _call(**kwargs):
    return get_schema_template_action(
        resource_type="component",
        operation="create",
        **kwargs,
    )


def test_template_resolves_for_script_mapping_component_type():
    result = _call(component_type="script.mapping")
    assert result["_success"] is True
    assert result["component_type"] == "script.mapping"


def test_template_required_fields_cover_authoring_contract():
    result = _call(component_type="script.mapping")
    required = result["required"]
    for expected in (
        "component_type",
        "component_name",
        "language",
        "script_body",
        "inputs",
        "outputs",
    ):
        assert expected in required


def test_template_optional_fields_include_preserve_order_and_use_cache():
    result = _call(component_type="script.mapping")
    optional = result["optional"]
    for expected in (
        "folder_path",
        "description",
        "preserve_order",
        "use_cache",
    ):
        assert expected in optional


def test_template_advertises_three_supported_languages():
    result = _call(component_type="script.mapping")
    langs = result["supported_languages"]
    assert set(langs) == {"groovy", "groovy2", "javascript"}


def test_template_lists_documented_input_data_types():
    result = _call(component_type="script.mapping")
    data_types = result["supported_input_data_types"]
    assert set(data_types) == {"character", "date", "integer", "float"}


def test_template_documents_output_data_type_inference():
    result = _call(component_type="script.mapping")
    note = result["output_data_type_inference_note"]
    assert "data_type" in note.lower()
    assert "inferred" in note.lower() or "infer" in note.lower()


def test_template_documents_variable_name_rule():
    result = _call(component_type="script.mapping")
    rule = result["variable_name_rule"]
    # Must mention both namespace sharing and the identifier pattern.
    assert "namespace" in rule.lower()
    assert "A-Za-z" in rule


def test_template_documents_indexing_rule_starting_at_one_then_continuing():
    result = _call(component_type="script.mapping")
    rule = result["indexing_rule"]
    # Inputs at 1, outputs at len(inputs)+1.
    assert "1-based" in rule or "len(inputs)" in rule


def test_template_advertises_issue_41_error_codes():
    result = _call(component_type="script.mapping")
    codes = result["error_codes"]
    for expected in (
        "SCRIPT_MAPPING_VALIDATION_FAILED",
        "SCRIPT_MAPPING_BODY_REQUIRED",
        "SCRIPT_MAPPING_LANGUAGE_UNSUPPORTED",
        "SCRIPT_MAPPING_VARIABLE_INVALID",
        "UNSUPPORTED_TRANSFORM_ROUTE",
        "PLAINTEXT_SECRET_REJECTED",
    ):
        assert expected in codes


def test_template_lists_forbidden_secret_fields():
    result = _call(component_type="script.mapping")
    forbidden = result["forbidden_secret_fields"]
    for expected in (
        "password",
        "token",
        "client_secret",
        "api_key",
        "credentials",
        "authorization",
        "bearer",
    ):
        assert expected in forbidden


def test_template_example_uses_placeholder_script_body_only():
    result = _call(component_type="script.mapping")
    example_blob = json.dumps(result["example"]).lower()
    for marker in _FORBIDDEN_TEMPLATE_SUBSTRINGS:
        assert marker not in example_blob, (
            f"script.mapping example contains forbidden marker {marker!r}"
        )
    # Sanity: placeholder marker present.
    assert "caller-authored script body" in example_blob


def test_template_skeleton_carries_only_placeholders():
    result = _call(component_type="script.mapping")
    template_blob = json.dumps(result.get("template", {})).lower()
    for marker in _FORBIDDEN_TEMPLATE_SUBSTRINGS:
        assert marker not in template_blob


def test_template_tool_points_at_manage_component_create():
    result = _call(component_type="script.mapping")
    assert "manage_component" in result["tool"]
    assert "tool_note" in result


def test_template_out_of_scope_marks_script_processing_as_off_limits():
    result = _call(component_type="script.mapping")
    out_of_scope = result["out_of_scope"]
    assert "script_processing" in out_of_scope
    blob = out_of_scope["script_processing"].lower()
    # Must explicitly call out that script.processing is NOT a fallback.
    assert "fallback" in blob or "not a fallback" in blob or "not a map primitive" in blob


def test_template_out_of_scope_marks_standalone_transform_function_future():
    # Codex r7 P2: the key name was clarified to
    # ``standalone_transform_function_authoring_surface`` since a
    # script-wrapper specialization (#41) DOES ship a transform.function
    # builder — the future-work caveat applies only to the general-purpose
    # authoring surface for non-script userdefined function graphs.
    result = _call(component_type="script.mapping")
    assert "standalone_transform_function_authoring_surface" in result["out_of_scope"]


def test_template_gotchas_mention_input_null_semantics():
    # Boomi docs: character inputs pass as empty strings, date/integer/float
    # can be null. The gotchas list documents this for script authors.
    result = _call(component_type="script.mapping")
    gotchas_blob = " ".join(result["gotchas"]).lower()
    assert "null" in gotchas_blob
    assert "character" in gotchas_blob


def test_template_points_at_script_mapping_authoring_schema():
    # The component template must point authors at the dedicated script_mapping
    # authoring schema + the docs search (scripting affordance work).
    result = _call(component_type="script.mapping")
    blob = json.dumps(result)
    assert "script_mapping" in blob
    assert "search_boomi_docs" in blob


def test_discovery_overview_points_at_script_mapping_authoring_schema():
    # Bug #144: the bare discovery call (no operation) must also surface the
    # map-script authoring pointer — symmetric with the process side.
    result = get_schema_template_action(
        resource_type="component", component_type="script.mapping"
    )
    assert result["_success"] is True
    assert result["filtered_type"] == "script.mapping"
    assert "script_mapping" in result["authoring_guidance"]
    assert "search_boomi_docs" in result["authoring_guidance"]


def test_authoring_schema_follow_up_no_longer_says_not_yet_emittable():
    # Native property map functions are now emittable via map_type='function';
    # the stale "not yet emittable" follow_up must be replaced.
    result = get_schema_template_action(schema_name="script_mapping")
    follow_up = result["follow_up"]
    assert "not yet emittable" not in follow_up
    assert "pending live FunctionStep shape capture" not in follow_up
    assert "map_type='function'" in follow_up
    assert "dynamic_process_property_get" in follow_up
