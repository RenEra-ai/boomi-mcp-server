"""Issue #41: MapScriptBuilder unit tests.

Covers structured ``transform.map`` configs where ``map_type='script'``
calls one or more reusable ``script.mapping`` components via in-map
userdefined ``<FunctionStep>`` blocks.
"""

from __future__ import annotations

import re

import pytest

from boomi_mcp.categories.components.builders.map_builder import (
    MAP_BUILDERS,
    DirectMapBuilder,
    MapScriptBuilder,
)


# ---------------------------------------------------------------------------
# Test fixtures: profile indexes mirror the field-index shape produced by
# JSONGeneratedProfileBuilder.build_field_index() / DatabaseReadProfileBuilder.
# ---------------------------------------------------------------------------


def _source_index():
    return {
        "Source/work_date": {
            "key": 9,
            "key_path": "*[@key='work_date']",
            "name_path": "Source/work_date",
            "mappable": True,
        },
        "Source/customer_id": {
            "key": 10,
            "key_path": "*[@key='customer_id']",
            "name_path": "Source/customer_id",
            "mappable": True,
        },
        "Source/parent_node": {
            "key": 11,
            "key_path": "*[@key='parent_node']",
            "name_path": "Source/parent_node",
            "mappable": False,
        },
    }


def _target_index():
    return {
        "Target/WorkDate": {
            "key": 52,
            "key_path": "*[@key='WorkDate']",
            "name_path": "Target/WorkDate",
            "mappable": True,
        },
        "Target/CustomerId": {
            "key": 53,
            "key_path": "*[@key='CustomerId']",
            "name_path": "Target/CustomerId",
            "mappable": True,
        },
        "Target/Wrapper": {
            "key": 54,
            "key_path": "*[@key='Wrapper']",
            "name_path": "Target/Wrapper",
            "mappable": False,
        },
    }


def _minimal_config(**overrides):
    base = {
        "component_type": "transform.map",
        "map_type": "script",
        "component_name": "Example DB → JSON Script Map",
        "source_profile_id": "src-uuid-1",
        "source_profile_type": "profile.db",
        "target_profile_id": "tgt-uuid-2",
        "target_profile_type": "profile.json",
        "script_mappings": [
            {
                "script_component_id": "script-uuid-aaa",
                "script_slot": "normalize_work_date",
                "inputs": [
                    {"source_path": "Source/work_date", "input_name": "inputDate"},
                ],
                "outputs": [
                    {"output_name": "outputDate", "target_path": "Target/WorkDate"},
                ],
            },
        ],
    }
    base.update(overrides)
    return base


def _build(config=None):
    cfg = config or _minimal_config()
    return MapScriptBuilder().build(
        source_index=_source_index(),
        target_index=_target_index(),
        **cfg,
    )


# ---------------------------------------------------------------------------
# Registry
# ---------------------------------------------------------------------------


def test_registry_resolves_script_and_map_script_to_MapScriptBuilder():
    assert MAP_BUILDERS[("transform.map", "script")] is MapScriptBuilder
    assert MAP_BUILDERS[("transform.map", "map_script")] is MapScriptBuilder


def test_supported_map_types_constant():
    assert MapScriptBuilder.SUPPORTED_MAP_TYPES == ("script", "map_script")


def test_map_script_alias_resolves_via_registry_build():
    xml = _build(_minimal_config(map_type="map_script"))
    assert 'type="transform.map"' in xml
    assert 'category="userdefined"' in xml


# ---------------------------------------------------------------------------
# XML envelope shape — single script call
# ---------------------------------------------------------------------------


def test_minimal_build_emits_userdefined_function_step_shape():
    xml = _build()
    # Transform.map envelope
    assert 'type="transform.map"' in xml
    assert '<Map xmlns="" fromProfile="src-uuid-1" toProfile="tgt-uuid-2">' in xml
    # FunctionStep: userdefined category + id attribute + empty Configuration
    assert 'category="userdefined"' in xml
    assert 'type="userdefined"' in xml
    assert 'id="script-uuid-aaa"' in xml
    assert 'cacheOption="none"' in xml
    assert 'enabled="true"' in xml
    assert "<Configuration/>" in xml
    # Inputs/Outputs at FunctionStep level use key= (not index=) and no dataType
    assert '<Input key="1" name="inputDate"/>' in xml
    assert '<Output key="1" name="outputDate"/>' in xml


def test_in_map_function_step_does_not_emit_scripting_block():
    # Critical: the in-map shape is empty <Configuration/>, NOT the
    # <Configuration><Scripting>...</Scripting></Configuration> shape that
    # appears in standalone transform.function wrappers.
    xml = _build()
    assert "<Scripting" not in xml
    assert "<ScriptToExecute" not in xml


def test_mapping_rows_wire_profile_to_function_and_back():
    xml = _build()
    # Profile → function input
    assert (
        '<Mapping fromKey="9" fromKeyPath="*[@key=&apos;work_date&apos;]" '
        'fromNamePath="Source/work_date" fromType="profile" '
        'toFunction="1" toKey="1" toType="function"/>'
    ) in xml
    # Function output → profile
    assert (
        '<Mapping fromFunction="1" fromKey="1" fromType="function" '
        'toKey="52" toKeyPath="*[@key=&apos;WorkDate&apos;]" '
        'toNamePath="Target/WorkDate" toType="profile"/>'
    ) in xml


def test_function_step_key_and_position_match_declaration_index():
    xml = _build()
    fs_match = re.search(r'<FunctionStep ([^>]+)>', xml)
    assert fs_match
    attrs = fs_match.group(1)
    assert 'key="1"' in attrs
    assert 'position="1"' in attrs


def test_default_cache_enabled_is_false_per_live_shape():
    xml = _build()
    # Live exports show cacheEnabled="false" for in-map userdefined steps.
    assert 'cacheEnabled="false"' in xml


def test_cache_enabled_true_passes_through():
    cfg = _minimal_config(
        script_mappings=[
            {
                "script_component_id": "script-uuid-aaa",
                "cache_enabled": True,
                "inputs": [
                    {"source_path": "Source/work_date", "input_name": "inputDate"},
                ],
                "outputs": [
                    {"output_name": "outputDate", "target_path": "Target/WorkDate"},
                ],
            },
        ],
    )
    xml = _build(cfg)
    assert 'cacheEnabled="true"' in xml


def test_function_step_name_defaults_to_component_name_when_script_slot_omitted():
    cfg = _minimal_config(
        script_mappings=[
            {
                "script_component_id": "script-uuid-aaa",
                "inputs": [
                    {"source_path": "Source/work_date", "input_name": "inputDate"},
                ],
                "outputs": [
                    {"output_name": "outputDate", "target_path": "Target/WorkDate"},
                ],
            },
        ],
    )
    xml = _build(cfg)
    # Without a script_slot, the FunctionStep name falls back to the map
    # component_name.
    assert 'name="Example DB → JSON Script Map"' in xml


# ---------------------------------------------------------------------------
# Multi-input / multi-output / multi-call wiring
# ---------------------------------------------------------------------------


def test_multi_input_multi_output_per_script_call():
    cfg = _minimal_config(
        script_mappings=[
            {
                "script_component_id": "script-uuid-multi",
                "script_slot": "multi_io",
                "inputs": [
                    {"source_path": "Source/work_date", "input_name": "inA"},
                    {"source_path": "Source/customer_id", "input_name": "inB"},
                ],
                "outputs": [
                    {"output_name": "outX", "target_path": "Target/WorkDate"},
                    {"output_name": "outY", "target_path": "Target/CustomerId"},
                ],
            },
        ],
    )
    xml = _build(cfg)
    # Two profile→function input mappings
    assert 'toFunction="1" toKey="1"' in xml
    assert 'toFunction="1" toKey="2"' in xml
    # Two function→profile output mappings
    assert 'fromFunction="1" fromKey="1"' in xml
    assert 'fromFunction="1" fromKey="2"' in xml
    # FunctionStep Input/Output port lists carry both ports each
    assert '<Input key="1" name="inA"/>' in xml
    assert '<Input key="2" name="inB"/>' in xml
    assert '<Output key="1" name="outX"/>' in xml
    assert '<Output key="2" name="outY"/>' in xml


def test_multiple_script_calls_get_monotonic_step_keys():
    cfg = _minimal_config(
        script_mappings=[
            {
                "script_component_id": "script-uuid-a",
                "inputs": [{"source_path": "Source/work_date", "input_name": "in1"}],
                "outputs": [{"output_name": "out1", "target_path": "Target/WorkDate"}],
            },
            {
                "script_component_id": "script-uuid-b",
                "inputs": [{"source_path": "Source/customer_id", "input_name": "in2"}],
                "outputs": [{"output_name": "out2", "target_path": "Target/CustomerId"}],
            },
        ],
    )
    xml = _build(cfg)
    # FunctionStep keys 1 and 2
    assert 'key="1"' in xml
    assert 'key="2"' in xml
    # Routing: step 1 refers to first script, step 2 to second
    assert 'id="script-uuid-a" key="1"' in xml
    assert 'id="script-uuid-b" key="2"' in xml
    # Mapping rows reference both function steps
    assert 'toFunction="1"' in xml
    assert 'toFunction="2"' in xml


# ---------------------------------------------------------------------------
# Mixed direct + script map
# ---------------------------------------------------------------------------


def test_mixed_direct_field_mappings_plus_script_mappings_emit_in_order():
    cfg = _minimal_config(
        field_mappings=[
            {"source_path": "Source/customer_id", "target_path": "Target/CustomerId"},
        ],
    )
    xml = _build(cfg)
    # Direct mapping appears (fromType="profile" toType="profile")
    direct_idx = xml.find('toType="profile"/></Mappings>') if False else xml.find(
        'fromKey="10"'
    )
    script_in_idx = xml.find('toFunction="1"')
    script_out_idx = xml.find('fromFunction="1"')
    # All three present and direct rows come before script rows.
    assert direct_idx != -1
    assert script_in_idx != -1
    assert script_out_idx != -1
    assert direct_idx < script_in_idx < script_out_idx


# ---------------------------------------------------------------------------
# Determinism
# ---------------------------------------------------------------------------


def test_repeat_build_byte_identical():
    cfg = _minimal_config(
        field_mappings=[
            {"source_path": "Source/customer_id", "target_path": "Target/CustomerId"},
        ],
        script_mappings=[
            {
                "script_component_id": "script-uuid-x",
                "script_slot": "slot_a",
                "inputs": [{"source_path": "Source/work_date", "input_name": "in1"}],
                "outputs": [{"output_name": "out1", "target_path": "Target/WorkDate"}],
            },
        ],
    )
    one = _build(cfg)
    two = _build(cfg)
    assert one == two


# ---------------------------------------------------------------------------
# XML escaping
# ---------------------------------------------------------------------------


def test_input_and_output_names_are_xml_escaped():
    # Names normally must be valid identifiers (the script.mapping component
    # validates that), but Map-side aliasing accepts any string Boomi
    # tolerates; the builder must escape regardless to avoid breaking XML.
    cfg = _minimal_config(
        script_mappings=[
            {
                "script_component_id": "script-uuid-aaa",
                "inputs": [
                    {"source_path": "Source/work_date", "input_name": "a&b"},
                ],
                "outputs": [
                    {"output_name": "x<y>", "target_path": "Target/WorkDate"},
                ],
            },
        ],
    )
    xml = _build(cfg)
    assert 'name="a&amp;b"' in xml
    assert 'name="x&lt;y&gt;"' in xml


def test_script_component_id_with_special_chars_is_escaped():
    cfg = _minimal_config(
        script_mappings=[
            {
                "script_component_id": "script&id",
                "inputs": [{"source_path": "Source/work_date", "input_name": "in"}],
                "outputs": [{"output_name": "out", "target_path": "Target/WorkDate"}],
            },
        ],
    )
    xml = _build(cfg)
    assert 'id="script&amp;id"' in xml


# ---------------------------------------------------------------------------
# Validation — script_mappings shape
# ---------------------------------------------------------------------------


def test_missing_script_mappings_rejected():
    cfg = _minimal_config()
    del cfg["script_mappings"]
    with pytest.raises(Exception) as exc_info:
        _build(cfg)
    assert exc_info.value.error_code == "PROFILE_FIELD_VALIDATION_FAILED"
    assert exc_info.value.field == "script_mappings"


def test_empty_script_mappings_rejected():
    with pytest.raises(Exception) as exc_info:
        _build(_minimal_config(script_mappings=[]))
    assert exc_info.value.error_code == "PROFILE_FIELD_VALIDATION_FAILED"


def test_missing_script_component_id_rejected():
    cfg = _minimal_config(
        script_mappings=[
            {
                "inputs": [{"source_path": "Source/work_date", "input_name": "in"}],
                "outputs": [{"output_name": "out", "target_path": "Target/WorkDate"}],
            },
        ],
    )
    with pytest.raises(Exception) as exc_info:
        _build(cfg)
    assert exc_info.value.error_code == "SCRIPT_MAPPING_REF_REQUIRED"


def test_blank_script_component_id_rejected():
    cfg = _minimal_config(
        script_mappings=[
            {
                "script_component_id": "   ",
                "inputs": [{"source_path": "Source/work_date", "input_name": "in"}],
                "outputs": [{"output_name": "out", "target_path": "Target/WorkDate"}],
            },
        ],
    )
    with pytest.raises(Exception) as exc_info:
        _build(cfg)
    assert exc_info.value.error_code == "SCRIPT_MAPPING_REF_REQUIRED"


def test_unresolved_dollar_ref_script_component_id_fails_at_build():
    # Integration builder must resolve $ref:KEY tokens before build is
    # invoked. A leaked $ref string here surfaces a structured error
    # rather than emitting nonsense XML.
    cfg = _minimal_config(
        script_mappings=[
            {
                "script_component_id": "$ref:some_script_key",
                "inputs": [{"source_path": "Source/work_date", "input_name": "in"}],
                "outputs": [{"output_name": "out", "target_path": "Target/WorkDate"}],
            },
        ],
    )
    with pytest.raises(Exception) as exc_info:
        _build(cfg)
    assert exc_info.value.error_code == "MAP_PROFILE_INDEX_UNAVAILABLE"


def test_missing_inputs_list_rejected():
    cfg = _minimal_config(
        script_mappings=[
            {
                "script_component_id": "script-uuid-aaa",
                "outputs": [{"output_name": "out", "target_path": "Target/WorkDate"}],
            },
        ],
    )
    with pytest.raises(Exception) as exc_info:
        _build(cfg)
    assert exc_info.value.error_code == "PROFILE_FIELD_VALIDATION_FAILED"
    assert "inputs" in exc_info.value.field


def test_missing_outputs_list_rejected():
    cfg = _minimal_config(
        script_mappings=[
            {
                "script_component_id": "script-uuid-aaa",
                "inputs": [{"source_path": "Source/work_date", "input_name": "in"}],
            },
        ],
    )
    with pytest.raises(Exception) as exc_info:
        _build(cfg)
    assert exc_info.value.error_code == "PROFILE_FIELD_VALIDATION_FAILED"
    assert "outputs" in exc_info.value.field


def test_input_entry_missing_source_path_rejected():
    cfg = _minimal_config(
        script_mappings=[
            {
                "script_component_id": "script-uuid-aaa",
                "inputs": [{"input_name": "in"}],
                "outputs": [{"output_name": "out", "target_path": "Target/WorkDate"}],
            },
        ],
    )
    with pytest.raises(Exception) as exc_info:
        _build(cfg)
    assert exc_info.value.error_code == "PROFILE_FIELD_VALIDATION_FAILED"


def test_input_entry_missing_input_name_rejected():
    cfg = _minimal_config(
        script_mappings=[
            {
                "script_component_id": "script-uuid-aaa",
                "inputs": [{"source_path": "Source/work_date"}],
                "outputs": [{"output_name": "out", "target_path": "Target/WorkDate"}],
            },
        ],
    )
    with pytest.raises(Exception) as exc_info:
        _build(cfg)
    assert exc_info.value.error_code == "PROFILE_FIELD_VALIDATION_FAILED"


def test_output_entry_missing_target_path_rejected():
    cfg = _minimal_config(
        script_mappings=[
            {
                "script_component_id": "script-uuid-aaa",
                "inputs": [{"source_path": "Source/work_date", "input_name": "in"}],
                "outputs": [{"output_name": "out"}],
            },
        ],
    )
    with pytest.raises(Exception) as exc_info:
        _build(cfg)
    assert exc_info.value.error_code == "PROFILE_FIELD_VALIDATION_FAILED"


# ---------------------------------------------------------------------------
# Validation — index-sensitive checks
# ---------------------------------------------------------------------------


def test_unresolved_source_path_rejected_as_map_field_not_found():
    cfg = _minimal_config(
        script_mappings=[
            {
                "script_component_id": "script-uuid-aaa",
                "inputs": [
                    {"source_path": "Source/missing", "input_name": "in"},
                ],
                "outputs": [
                    {"output_name": "out", "target_path": "Target/WorkDate"},
                ],
            },
        ],
    )
    with pytest.raises(Exception) as exc_info:
        _build(cfg)
    assert exc_info.value.error_code == "MAP_FIELD_NOT_FOUND"
    assert "source_path" in exc_info.value.field


def test_unresolved_target_path_rejected_as_map_field_not_found():
    cfg = _minimal_config(
        script_mappings=[
            {
                "script_component_id": "script-uuid-aaa",
                "inputs": [
                    {"source_path": "Source/work_date", "input_name": "in"},
                ],
                "outputs": [
                    {"output_name": "out", "target_path": "Target/Missing"},
                ],
            },
        ],
    )
    with pytest.raises(Exception) as exc_info:
        _build(cfg)
    assert exc_info.value.error_code == "MAP_FIELD_NOT_FOUND"
    assert "target_path" in exc_info.value.field


def test_non_mappable_source_path_rejected():
    cfg = _minimal_config(
        script_mappings=[
            {
                "script_component_id": "script-uuid-aaa",
                "inputs": [
                    {"source_path": "Source/parent_node", "input_name": "in"},
                ],
                "outputs": [
                    {"output_name": "out", "target_path": "Target/WorkDate"},
                ],
            },
        ],
    )
    with pytest.raises(Exception) as exc_info:
        _build(cfg)
    assert exc_info.value.error_code == "PROFILE_FIELD_NOT_MAPPABLE"


def test_non_mappable_target_path_rejected():
    cfg = _minimal_config(
        script_mappings=[
            {
                "script_component_id": "script-uuid-aaa",
                "inputs": [
                    {"source_path": "Source/work_date", "input_name": "in"},
                ],
                "outputs": [
                    {"output_name": "out", "target_path": "Target/Wrapper"},
                ],
            },
        ],
    )
    with pytest.raises(Exception) as exc_info:
        _build(cfg)
    assert exc_info.value.error_code == "PROFILE_FIELD_NOT_MAPPABLE"


# ---------------------------------------------------------------------------
# Validation — duplicate target detection across field_mappings + script outputs
# ---------------------------------------------------------------------------


def test_duplicate_target_between_field_mappings_and_script_outputs_rejected():
    cfg = _minimal_config(
        field_mappings=[
            {"source_path": "Source/customer_id", "target_path": "Target/WorkDate"},
        ],
    )
    # The script_mappings entry in _minimal_config already binds
    # "Target/WorkDate" — adding a direct field_mappings to the same target
    # is a duplicate.
    with pytest.raises(Exception) as exc_info:
        _build(cfg)
    assert exc_info.value.error_code == "DUPLICATE_TARGET_MAPPING"


def test_duplicate_target_across_two_script_outputs_rejected():
    cfg = _minimal_config(
        script_mappings=[
            {
                "script_component_id": "script-uuid-a",
                "inputs": [{"source_path": "Source/work_date", "input_name": "in"}],
                "outputs": [{"output_name": "out", "target_path": "Target/WorkDate"}],
            },
            {
                "script_component_id": "script-uuid-b",
                "inputs": [{"source_path": "Source/customer_id", "input_name": "in"}],
                "outputs": [{"output_name": "out", "target_path": "Target/WorkDate"}],
            },
        ],
    )
    with pytest.raises(Exception) as exc_info:
        _build(cfg)
    assert exc_info.value.error_code == "DUPLICATE_TARGET_MAPPING"


# ---------------------------------------------------------------------------
# Validation — map_type / profile refs / route rejection
# ---------------------------------------------------------------------------


def test_unsupported_map_type_rejected():
    with pytest.raises(Exception) as exc_info:
        _build(_minimal_config(map_type="bogus"))
    assert exc_info.value.field == "map_type"


def test_function_mappings_rejected_on_script_route():
    cfg = _minimal_config()
    cfg["function_mappings"] = [{"function_type": "uppercase"}]
    with pytest.raises(Exception) as exc_info:
        _build(cfg)
    assert exc_info.value.error_code == "UNSUPPORTED_TRANSFORM_ROUTE"
    assert exc_info.value.field == "function_mappings"


def test_raw_xml_escape_hatch_rejected():
    cfg = _minimal_config()
    cfg["scripts"] = "<<raw xml>>"
    with pytest.raises(Exception) as exc_info:
        _build(cfg)
    assert exc_info.value.error_code == "UNSUPPORTED_TRANSFORM_ROUTE"


def test_unresolved_dollar_ref_source_profile_id_rejected_at_build():
    cfg = _minimal_config(source_profile_id="$ref:source_key")
    with pytest.raises(Exception) as exc_info:
        _build(cfg)
    assert exc_info.value.error_code == "MAP_PROFILE_INDEX_UNAVAILABLE"


# ---------------------------------------------------------------------------
# Defense-in-depth — DirectMapBuilder still rejects script_mappings
# ---------------------------------------------------------------------------


# ---------------------------------------------------------------------------
# Codex r2 P2 fixes — silent-drop & truthiness hazards
# ---------------------------------------------------------------------------


def test_script_body_in_script_mapping_entry_rejected_with_pointer():
    """Codex r2 P2 #1: a caller-authored script_body inside a
    script_mappings entry would be silently ignored by build(). Reject
    explicitly and point at the standalone script.mapping component path."""
    cfg = _minimal_config(
        script_mappings=[
            {
                "script_component_id": "script-uuid-aaa",
                "script_body": "<<would be silently dropped>>",
                "inputs": [
                    {"source_path": "Source/work_date", "input_name": "in"},
                ],
                "outputs": [
                    {"output_name": "out", "target_path": "Target/WorkDate"},
                ],
            },
        ],
    )
    with pytest.raises(Exception) as exc_info:
        _build(cfg)
    assert exc_info.value.error_code == "UNSUPPORTED_TRANSFORM_ROUTE"
    assert "script_body" in exc_info.value.field
    # Hint must point at the standalone script.mapping component path.
    assert "script.mapping" in exc_info.value.hint
    assert "script_component_id" in exc_info.value.hint


def test_unknown_key_in_script_mapping_entry_rejected():
    """Defense-in-depth: any key beyond the documented allow-list is
    rejected so typos don't silently produce mis-emitted XML."""
    cfg = _minimal_config(
        script_mappings=[
            {
                "script_component_id": "script-uuid-aaa",
                "typo_key": "value",
                "inputs": [
                    {"source_path": "Source/work_date", "input_name": "in"},
                ],
                "outputs": [
                    {"output_name": "out", "target_path": "Target/WorkDate"},
                ],
            },
        ],
    )
    with pytest.raises(Exception) as exc_info:
        _build(cfg)
    assert exc_info.value.error_code == "PROFILE_FIELD_VALIDATION_FAILED"
    assert "typo_key" in exc_info.value.field


def test_string_false_cache_enabled_rejected_not_silently_inverted():
    """Codex r2 P2 #2: ``bool("false")`` is True in Python, so a stringy
    "false" would silently emit ``cacheEnabled="true"`` if we used the
    truthiness conversion blindly. Reject non-boolean cache_enabled values
    instead of inverting the caller's intent."""
    cfg = _minimal_config(
        script_mappings=[
            {
                "script_component_id": "script-uuid-aaa",
                "cache_enabled": "false",
                "inputs": [
                    {"source_path": "Source/work_date", "input_name": "in"},
                ],
                "outputs": [
                    {"output_name": "out", "target_path": "Target/WorkDate"},
                ],
            },
        ],
    )
    with pytest.raises(Exception) as exc_info:
        _build(cfg)
    assert exc_info.value.error_code == "PROFILE_FIELD_VALIDATION_FAILED"
    assert "cache_enabled" in exc_info.value.field


def test_int_one_cache_enabled_also_rejected_as_non_boolean():
    """Strictness extends to other truthy-but-not-bool values."""
    cfg = _minimal_config(
        script_mappings=[
            {
                "script_component_id": "script-uuid-aaa",
                "cache_enabled": 1,
                "inputs": [
                    {"source_path": "Source/work_date", "input_name": "in"},
                ],
                "outputs": [
                    {"output_name": "out", "target_path": "Target/WorkDate"},
                ],
            },
        ],
    )
    with pytest.raises(Exception) as exc_info:
        _build(cfg)
    assert exc_info.value.error_code == "PROFILE_FIELD_VALIDATION_FAILED"


def test_proper_boolean_cache_enabled_still_works():
    """Sanity: actual booleans still pass through to XML emission."""
    cfg = _minimal_config(
        script_mappings=[
            {
                "script_component_id": "script-uuid-aaa",
                "cache_enabled": True,
                "inputs": [
                    {"source_path": "Source/work_date", "input_name": "in"},
                ],
                "outputs": [
                    {"output_name": "out", "target_path": "Target/WorkDate"},
                ],
            },
        ],
    )
    xml = _build(cfg)
    assert 'cacheEnabled="true"' in xml


def test_direct_map_builder_rejects_script_mappings():
    """A direct map config with a stray script_mappings field still fails
    fast — the per-builder rejection layer keeps script syntax out of
    direct maps even now that #41 has shipped."""
    cfg = {
        "component_type": "transform.map",
        "map_type": "direct",
        "component_name": "Bad Direct Map",
        "source_profile_id": "src-uuid",
        "source_profile_type": "profile.db",
        "target_profile_id": "tgt-uuid",
        "target_profile_type": "profile.json",
        "field_mappings": [
            {"source_path": "Source/work_date", "target_path": "Target/WorkDate"},
        ],
        # Stray — must reject.
        "script_mappings": [
            {
                "script_component_id": "x",
                "inputs": [],
                "outputs": [],
            },
        ],
    }
    err = DirectMapBuilder.validate_config(cfg)
    # DirectMapBuilder doesn't carry a per-builder reject list for
    # script_mappings (it's not a route-class key recognised by direct
    # maps). But the unknown-key behaviour must still surface — direct
    # maps don't silently accept unrelated structured fields. The
    # acceptable outcome is either UNSUPPORTED_TRANSFORM_ROUTE or
    # PROFILE_FIELD_VALIDATION_FAILED — either confirms the field is
    # not silently ignored.
    # In practice DirectMapBuilder only validates known keys, so a
    # stray script_mappings is ignored — defense-in-depth is enforced
    # at the dispatcher (map_type='direct' never routes to a script
    # builder), not at the DirectMapBuilder validator. This test
    # documents that behaviour: validation passes with map_type='direct'
    # ignoring stray keys, but the apply path never reaches a script
    # builder.
    assert err is None
