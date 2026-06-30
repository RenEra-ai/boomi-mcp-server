"""Tests for the issue #40 MapFunctionBuilder.

Verifies the structured function_mappings contract end-to-end with the
JSON / XML profile builders' field indexes. XML envelope shapes anchored
against live Boomi exports (renera ``92a8b6a9-...``, ``b8a90410-...``;
work ``f5481730-...``, ``e9e1a9b6-...``; fetched 2026-05-26).
"""

from __future__ import annotations

from typing import Any, Dict, List
from xml.etree import ElementTree as ET

import pytest

from boomi_mcp.categories.components.builders.connector_builder import (
    BuilderValidationError,
)
from boomi_mcp.categories.components.builders.json_profile_builder import (
    JSONGeneratedProfileBuilder,
)
from boomi_mcp.categories.components.builders.map_builder import (
    MAP_BUILDERS,
    DirectMapBuilder,
    MapFunctionBuilder,
    get_map_builder,
)
from boomi_mcp.categories.components.builders.xml_profile_builder import (
    XMLGeneratedProfileBuilder,
)


NS = {"bns": "http://api.platform.boomi.com/"}


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


def _xml_source_config() -> Dict[str, Any]:
    return {
        "component_type": "profile.xml",
        "profile_type": "xml.generated",
        "component_name": "XML Source",
        "root": {
            "name": "rows",
            "kind": "element",
            "min_occurs": 1,
            "max_occurs": 1,
            "children": [
                {"name": "row", "kind": "element", "max_occurs": -1, "children": [
                    {"name": "key", "kind": "element", "data_type": "character"},
                    {"name": "name", "kind": "element", "data_type": "character"},
                    {"name": "count", "kind": "element", "data_type": "number"},
                    {"name": "amount", "kind": "element", "data_type": "number"},
                    {"name": "tax", "kind": "element", "data_type": "number"},
                ]},
            ],
        },
    }


def _json_target_config() -> Dict[str, Any]:
    return {
        "component_type": "profile.json",
        "profile_type": "json.generated",
        "component_name": "JSON Target",
        "root": {
            "name": "Root",
            "kind": "object",
            "children": [
                {"name": "list", "kind": "array", "children": [
                    {"name": "key", "kind": "simple", "data_type": "character"},
                    {"name": "name", "kind": "simple", "data_type": "character"},
                    {"name": "count", "kind": "simple", "data_type": "number"},
                    {"name": "total", "kind": "simple", "data_type": "number"},
                    {"name": "status", "kind": "simple", "data_type": "character"},
                ]},
            ],
        },
    }


def _build_indexes():
    src_idx = XMLGeneratedProfileBuilder.build_field_index(_xml_source_config())
    tgt_idx = JSONGeneratedProfileBuilder.build_field_index(_json_target_config())
    return src_idx, tgt_idx


def _function_map_config(**overrides: Any) -> Dict[str, Any]:
    cfg: Dict[str, Any] = {
        "component_type": "transform.map",
        "map_type": "function",
        "component_name": "XML to JSON Function Map",
        "source_profile_id": "aaaaaaaa-1111-1111-1111-111111111111",
        "source_profile_type": "profile.xml",
        "target_profile_id": "bbbbbbbb-2222-2222-2222-222222222222",
        "target_profile_type": "profile.json",
        "function_mappings": [
            {
                "function_type": "lowercase",
                "inputs": ["rows/row[]/name"],
                "target_path": "Root/list[]/name",
                "parameters": {},
            },
        ],
    }
    cfg.update(overrides)
    return cfg


# ---------------------------------------------------------------------------
# Registry wiring
# ---------------------------------------------------------------------------


def test_registry_resolves_function_builder():
    assert get_map_builder("transform.map", "function") is not None
    assert isinstance(
        get_map_builder("transform.map", "function"), MapFunctionBuilder
    )


def test_registry_resolves_map_function_alias():
    assert isinstance(
        get_map_builder("transform.map", "map_function"), MapFunctionBuilder
    )


def test_registry_advertises_function_pairs():
    assert ("transform.map", "function") in MAP_BUILDERS
    assert ("transform.map", "map_function") in MAP_BUILDERS


# ---------------------------------------------------------------------------
# validate_config — required fields
# ---------------------------------------------------------------------------


def test_validate_config_requires_function_mappings():
    cfg = _function_map_config()
    cfg.pop("function_mappings")
    err = MapFunctionBuilder.validate_config(cfg)
    assert err is not None
    assert err.error_code == "PROFILE_FIELD_VALIDATION_FAILED"
    assert err.field == "function_mappings"


def test_validate_config_rejects_empty_function_mappings():
    cfg = _function_map_config(function_mappings=[])
    err = MapFunctionBuilder.validate_config(cfg)
    assert err is not None
    assert err.error_code == "PROFILE_FIELD_VALIDATION_FAILED"


def test_validate_config_requires_component_name():
    cfg = _function_map_config()
    cfg["component_name"] = "  "
    err = MapFunctionBuilder.validate_config(cfg)
    assert err is not None
    assert err.field == "component_name"


def test_validate_config_requires_source_profile_id():
    cfg = _function_map_config()
    cfg["source_profile_id"] = ""
    err = MapFunctionBuilder.validate_config(cfg)
    assert err is not None
    assert err.error_code == "MAP_PROFILE_REF_REQUIRED"


def test_validate_config_rejects_unsupported_map_type():
    cfg = _function_map_config(map_type="direct")
    err = MapFunctionBuilder.validate_config(cfg)
    assert err is not None
    assert err.field == "map_type"


# ---------------------------------------------------------------------------
# validate_config — function_mappings shape
# ---------------------------------------------------------------------------


def test_validate_config_rejects_unknown_function_type():
    cfg = _function_map_config()
    cfg["function_mappings"][0]["function_type"] = "totally_bogus"
    err = MapFunctionBuilder.validate_config(cfg)
    assert err is not None
    assert err.error_code == "UNSUPPORTED_MAP_FUNCTION_TYPE"


def test_validate_config_rejects_input_arity_mismatch():
    cfg = _function_map_config()
    cfg["function_mappings"][0]["inputs"] = []
    err = MapFunctionBuilder.validate_config(cfg)
    assert err is not None
    assert err.error_code == "MAP_FUNCTION_INPUT_COUNT_MISMATCH"


def test_validate_config_rejects_missing_required_parameter():
    cfg = _function_map_config(
        function_mappings=[
            {
                "function_type": "date_format",
                "inputs": ["rows/row[]/key"],
                "target_path": "Root/list[]/key",
                "parameters": {"input_format": "yyyy-MM-dd"},
            },
        ]
    )
    err = MapFunctionBuilder.validate_config(cfg)
    assert err is not None
    assert err.error_code == "MAP_FUNCTION_PARAMETER_MISSING"


def test_validate_config_rejects_unsupported_math_operation():
    cfg = _function_map_config(
        function_mappings=[
            {
                "function_type": "math",
                "inputs": ["rows/row[]/amount", "rows/row[]/tax"],
                "target_path": "Root/list[]/total",
                "parameters": {"operation": "modulo"},
            },
        ]
    )
    err = MapFunctionBuilder.validate_config(cfg)
    assert err is not None
    assert err.error_code == "UNSUPPORTED_MATH_OPERATION"


def test_validate_config_rejects_duplicate_target_within_function_mappings():
    cfg = _function_map_config(
        function_mappings=[
            {
                "function_type": "lowercase",
                "inputs": ["rows/row[]/name"],
                "target_path": "Root/list[]/name",
                "parameters": {},
            },
            {
                "function_type": "uppercase",
                "inputs": ["rows/row[]/name"],
                "target_path": "Root/list[]/name",
                "parameters": {},
            },
        ]
    )
    err = MapFunctionBuilder.validate_config(cfg)
    assert err is not None
    assert err.error_code == "DUPLICATE_TARGET_MAPPING"


def test_validate_config_rejects_duplicate_target_across_lists():
    cfg = _function_map_config(
        field_mappings=[
            {"source_path": "rows/row[]/key", "target_path": "Root/list[]/name"},
        ],
    )
    # function_mappings[0].target_path is also Root/list[]/name
    err = MapFunctionBuilder.validate_config(cfg)
    assert err is not None
    assert err.error_code == "DUPLICATE_TARGET_MAPPING"


def test_validate_config_rejects_secret_shaped_key_in_parameters():
    cfg = _function_map_config(
        function_mappings=[
            {
                "function_type": "lowercase",
                "inputs": ["rows/row[]/name"],
                "target_path": "Root/list[]/name",
                "parameters": {"password": "leak"},
            },
        ]
    )
    err = MapFunctionBuilder.validate_config(cfg)
    assert err is not None
    assert err.error_code == "PLAINTEXT_SECRET_REJECTED"


# ---------------------------------------------------------------------------
# validate_config — index-sensitive checks
# ---------------------------------------------------------------------------


def test_validate_config_index_check_rejects_unknown_source_input():
    src_idx, tgt_idx = _build_indexes()
    cfg = _function_map_config()
    cfg["function_mappings"][0]["inputs"] = ["rows/row[]/no_such_field"]
    err = MapFunctionBuilder.validate_config(
        cfg, source_index=src_idx, target_index=tgt_idx
    )
    assert err is not None
    assert err.error_code == "MAP_FIELD_NOT_FOUND"


def test_validate_config_index_check_rejects_unknown_target():
    src_idx, tgt_idx = _build_indexes()
    cfg = _function_map_config()
    cfg["function_mappings"][0]["target_path"] = "Root/list[]/no_such_field"
    err = MapFunctionBuilder.validate_config(
        cfg, source_index=src_idx, target_index=tgt_idx
    )
    assert err is not None
    assert err.error_code == "MAP_FIELD_NOT_FOUND"


def test_validate_config_index_check_rejects_structural_source():
    src_idx, tgt_idx = _build_indexes()
    cfg = _function_map_config()
    # 'rows/row' is the structural array element (children carry the [] suffix).
    cfg["function_mappings"][0]["inputs"] = ["rows/row"]
    err = MapFunctionBuilder.validate_config(
        cfg, source_index=src_idx, target_index=tgt_idx
    )
    assert err is not None
    assert err.error_code == "PROFILE_FIELD_NOT_MAPPABLE"


# ---------------------------------------------------------------------------
# build — XML emission
# ---------------------------------------------------------------------------


def _build(cfg: Dict[str, Any]) -> str:
    src_idx, tgt_idx = _build_indexes()
    builder = MapFunctionBuilder()
    return builder.build(source_index=src_idx, target_index=tgt_idx, **cfg)


def _parse(xml: str) -> ET.Element:
    return ET.fromstring(xml)


def test_build_envelope_structure():
    xml = _build(_function_map_config())
    root = _parse(xml)
    assert root.tag.endswith("Component")
    assert root.attrib["type"] == "transform.map"
    assert root.attrib["name"] == "XML to JSON Function Map"
    map_el = root.find("bns:object/Map", NS)
    assert map_el is not None
    assert map_el.attrib["fromProfile"] == "aaaaaaaa-1111-1111-1111-111111111111"
    assert map_el.attrib["toProfile"] == "bbbbbbbb-2222-2222-2222-222222222222"
    assert map_el.find("Mappings") is not None
    assert map_el.find("Functions") is not None
    assert map_el.find("Defaults") is not None
    assert map_el.find("DocumentCacheJoins") is not None


def test_build_lowercase_function_emits_function_step_and_mappings():
    xml = _build(_function_map_config())
    root = _parse(xml)
    steps = root.findall("bns:object/Map/Functions/FunctionStep", NS)
    assert len(steps) == 1
    step = steps[0]
    assert step.attrib["type"] == "StringToLower"
    assert step.attrib["category"] == "String"
    assert step.attrib["key"] == "1"
    assert step.attrib["position"] == "1"

    inputs = step.findall("Inputs/Input")
    assert len(inputs) == 1
    assert inputs[0].attrib["name"] == "Original String"
    assert inputs[0].attrib["key"] == "1"

    outputs = step.findall("Outputs/Output")
    assert len(outputs) == 1
    assert outputs[0].attrib["name"] == "Result"
    # Output key=2 matches live Boomi UI convention (FUNCTION_OUTPUT_KEY).
    assert outputs[0].attrib["key"] == "2"

    mappings = root.findall("bns:object/Map/Mappings/Mapping", NS)
    # Expect: 1 profile→function-input + 1 function-output→profile = 2
    assert len(mappings) == 2

    profile_to_function = [m for m in mappings if m.attrib.get("toType") == "function"]
    function_to_profile = [m for m in mappings if m.attrib.get("fromType") == "function"]
    assert len(profile_to_function) == 1
    assert len(function_to_profile) == 1
    assert profile_to_function[0].attrib["toFunction"] == "1"
    assert profile_to_function[0].attrib["toKey"] == "1"
    assert function_to_profile[0].attrib["fromFunction"] == "1"
    # Output key=2 matches live Boomi UI convention (FUNCTION_OUTPUT_KEY).
    assert function_to_profile[0].attrib["fromKey"] == "2"


def test_build_date_format_populates_input_masks():
    xml = _build(
        _function_map_config(
            function_mappings=[
                {
                    "function_type": "date_format",
                    "inputs": ["rows/row[]/key"],
                    "target_path": "Root/list[]/key",
                    "parameters": {
                        "input_format": "yyyy-MM-dd",
                        "output_format": "MM/dd/yyyy",
                    },
                },
            ],
        )
    )
    assert 'type="DateFormat"' in xml
    assert 'category="Date"' in xml
    assert 'default="yyyy-MM-dd"' in xml
    assert 'default="MM/dd/yyyy"' in xml


def test_validate_config_rejects_non_mapping_parameters_payload():
    # Codex r1: "" / [] / False must not be silently coerced to {} — they
    # are invalid parameter payloads and must hit PROFILE_FIELD_VALIDATION_FAILED.
    for bad in ("", [], False, 0, "string-not-mapping"):
        cfg = _function_map_config()
        cfg["function_mappings"][0]["parameters"] = bad
        err = MapFunctionBuilder.validate_config(cfg)
        assert err is not None, f"non-mapping parameters {bad!r} was silently accepted"
        assert err.error_code == "PROFILE_FIELD_VALIDATION_FAILED"
        assert err.field == "function_mappings[0].parameters"


def test_validate_config_missing_parameters_key_is_treated_as_empty():
    # Functions with no required parameters work when parameters key omitted.
    cfg = _function_map_config(function_mappings=[
        {
            "function_type": "trim",
            "inputs": ["rows/row[]/name"],
            "target_path": "Root/list[]/name",
            # no "parameters" key at all
        },
    ])
    src_idx, tgt_idx = _build_indexes()
    err = MapFunctionBuilder.validate_config(
        cfg, source_index=src_idx, target_index=tgt_idx
    )
    assert err is None


def test_validate_config_none_parameters_treated_as_empty():
    cfg = _function_map_config(function_mappings=[
        {
            "function_type": "trim",
            "inputs": ["rows/row[]/name"],
            "target_path": "Root/list[]/name",
            "parameters": None,
        },
    ])
    src_idx, tgt_idx = _build_indexes()
    err = MapFunctionBuilder.validate_config(
        cfg, source_index=src_idx, target_index=tgt_idx
    )
    assert err is None


def test_build_simple_lookup_renders_crossref_table():
    xml = _build(
        _function_map_config(
            function_mappings=[
                {
                    "function_type": "simple_lookup",
                    "inputs": ["rows/row[]/key"],
                    "target_path": "Root/list[]/status",
                    "parameters": {
                        "rows": [
                            {"ref1": "A", "ref2": "active"},
                            {"ref1": "I", "ref2": "inactive"},
                        ],
                    },
                },
            ],
        )
    )
    assert "<SimpleLookup>" in xml
    assert "<CrossRefTableObj>" in xml
    assert '<ref value="A"/><ref value="active"/>' in xml
    # Verify output-mapping fromKey matches the FUNCTION_OUTPUT_KEY (2).
    root = _parse(xml)
    function_outputs = [
        m for m in root.findall("bns:object/Map/Mappings/Mapping", NS)
        if m.attrib.get("fromType") == "function"
    ]
    assert len(function_outputs) == 1
    assert function_outputs[0].attrib["fromKey"] == "2"


def test_build_sequential_value_emits_4_inputs_with_param_defaults():
    xml = _build(
        _function_map_config(
            function_mappings=[
                {
                    "function_type": "sequential_value",
                    "inputs": [],
                    "target_path": "Root/list[]/key",
                    "parameters": {
                        "key_name": "order_seq",
                        "fix_to_length": 6,
                        "batch_size": 1,
                    },
                },
            ],
        )
    )
    root = _parse(xml)
    steps = root.findall("bns:object/Map/Functions/FunctionStep", NS)
    assert len(steps) == 1
    assert steps[0].attrib["type"] == "SequentialValue"

    # All 4 Input elements emit with the exact Boomi-expected names.
    inputs = steps[0].findall("Inputs/Input")
    assert [inp.attrib["name"] for inp in inputs] == [
        "Increment Basis", "Key Name", "Fix to Length", "Batch Size",
    ]
    assert [inp.attrib["default"] for inp in inputs] == [
        "", "order_seq", "6", "1",
    ]

    # Configuration stays empty <SequentialValue/>.
    config = steps[0].find("Configuration/SequentialValue")
    assert config is not None
    assert list(config) == []
    assert list(config.attrib.items()) == []

    # No profile->function-input mapping since user-mapped inputs=[]
    mappings = root.findall("bns:object/Map/Mappings/Mapping", NS)
    profile_to_function = [m for m in mappings if m.attrib.get("toType") == "function"]
    function_to_profile = [m for m in mappings if m.attrib.get("fromType") == "function"]
    assert len(profile_to_function) == 0
    assert len(function_to_profile) == 1


def test_build_sequential_value_requires_key_name():
    cfg = _function_map_config(function_mappings=[
        {
            "function_type": "sequential_value",
            "inputs": [],
            "target_path": "Root/list[]/key",
            "parameters": {},  # no key_name
        },
    ])
    err = MapFunctionBuilder.validate_config(cfg)
    assert err is not None
    assert err.error_code == "MAP_FUNCTION_PARAMETER_MISSING"


def test_build_math_add_two_inputs():
    xml = _build(
        _function_map_config(
            function_mappings=[
                {
                    "function_type": "math",
                    "inputs": ["rows/row[]/amount", "rows/row[]/tax"],
                    "target_path": "Root/list[]/total",
                    "parameters": {"operation": "add"},
                },
            ],
        )
    )
    root = _parse(xml)
    steps = root.findall("bns:object/Map/Functions/FunctionStep", NS)
    assert steps[0].attrib["type"] == "MathAdd"
    assert steps[0].attrib["category"] == "Numeric"

    mappings = root.findall("bns:object/Map/Mappings/Mapping", NS)
    profile_to_function = [m for m in mappings if m.attrib.get("toType") == "function"]
    assert len(profile_to_function) == 2
    # Inputs are mapped in order: toKey="1" then "2"
    assert profile_to_function[0].attrib["toKey"] == "1"
    assert profile_to_function[1].attrib["toKey"] == "2"


def test_build_default_value_routes_to_defaults_block():
    xml = _build(
        _function_map_config(
            function_mappings=[
                {
                    "function_type": "default_value",
                    "inputs": [],
                    "target_path": "Root/list[]/status",
                    "parameters": {"value": "active"},
                },
            ],
        )
    )
    root = _parse(xml)
    # No FunctionStep emitted for default_value.
    steps = root.findall("bns:object/Map/Functions/FunctionStep", NS)
    assert len(steps) == 0

    defaults = root.findall("bns:object/Map/Defaults/Default", NS)
    assert len(defaults) == 1
    assert defaults[0].attrib["value"] == "active"
    # toKey points at the target leaf's key from the JSON target index.
    tgt_idx = _build_indexes()[1]
    assert defaults[0].attrib["toKey"] == str(tgt_idx["Root/list[]/status"]["key"])

    # No function-related mappings emitted either.
    mappings = root.findall("bns:object/Map/Mappings/Mapping", NS)
    assert mappings == []


def test_build_mixed_map_with_direct_and_function_mappings():
    xml = _build(
        _function_map_config(
            field_mappings=[
                {
                    "source_path": "rows/row[]/key",
                    "target_path": "Root/list[]/key",
                },
            ],
            # function_mappings already has lowercase->name from fixture
        )
    )
    root = _parse(xml)
    mappings = root.findall("bns:object/Map/Mappings/Mapping", NS)
    # Expect: 1 direct + 1 profile->function-input + 1 function-output->profile = 3
    assert len(mappings) == 3
    # Direct mapping comes first (declaration order).
    assert mappings[0].attrib.get("fromType") == "profile"
    assert mappings[0].attrib.get("toType") == "profile"
    # Then function-related mappings.
    assert mappings[1].attrib.get("toType") == "function"
    assert mappings[2].attrib.get("fromType") == "function"


def test_build_function_step_keys_match_declaration_order():
    xml = _build(
        _function_map_config(
            function_mappings=[
                {
                    "function_type": "uppercase",
                    "inputs": ["rows/row[]/name"],
                    "target_path": "Root/list[]/name",
                    "parameters": {},
                },
                {
                    "function_type": "math",
                    "inputs": ["rows/row[]/amount", "rows/row[]/tax"],
                    "target_path": "Root/list[]/total",
                    "parameters": {"operation": "add"},
                },
            ],
        )
    )
    root = _parse(xml)
    steps = root.findall("bns:object/Map/Functions/FunctionStep", NS)
    assert len(steps) == 2
    assert steps[0].attrib["key"] == "1"
    assert steps[0].attrib["type"] == "StringToUpper"
    assert steps[1].attrib["key"] == "2"
    assert steps[1].attrib["type"] == "MathAdd"


def test_build_escapes_xml_in_parameter_values():
    xml = _build(
        _function_map_config(
            function_mappings=[
                {
                    "function_type": "append",
                    "inputs": ["rows/row[]/name"],
                    "target_path": "Root/list[]/name",
                    "parameters": {"value": '<&">'},
                },
            ],
        )
    )
    # Value should round-trip as escaped entities.
    assert "&lt;&amp;&quot;&gt;" in xml
    # And the raw form must not appear.
    assert '<&">' not in xml.replace("&lt;&amp;&quot;&gt;", "")


def test_build_is_byte_stable_across_repeats():
    cfg = _function_map_config(
        function_mappings=[
            {
                "function_type": "math",
                "inputs": ["rows/row[]/amount", "rows/row[]/tax"],
                "target_path": "Root/list[]/total",
                "parameters": {"operation": "add"},
            },
            {
                "function_type": "default_value",
                "inputs": [],
                "target_path": "Root/list[]/status",
                "parameters": {"value": "active"},
            },
        ],
    )
    a = _build(cfg)
    b = _build(cfg)
    assert a == b


# ---------------------------------------------------------------------------
# build — apply-time guard rails (consistent with DirectMapBuilder)
# ---------------------------------------------------------------------------


def test_build_rejects_unresolved_ref_tokens():
    src_idx, tgt_idx = _build_indexes()
    cfg = _function_map_config(source_profile_id="$ref:foo")
    with pytest.raises(BuilderValidationError) as exc_info:
        MapFunctionBuilder().build(
            source_index=src_idx, target_index=tgt_idx, **cfg
        )
    assert exc_info.value.error_code == "MAP_PROFILE_INDEX_UNAVAILABLE"


# ---------------------------------------------------------------------------
# Direct builder still rejects function_mappings (Phase 3 defense-in-depth)
# ---------------------------------------------------------------------------


def test_direct_builder_rejects_function_mappings():
    cfg = {
        "component_type": "transform.map",
        "map_type": "direct",
        "component_name": "name",
        "source_profile_id": "aaaaaaaa-1111-1111-1111-111111111111",
        "source_profile_type": "profile.xml",
        "target_profile_id": "bbbbbbbb-2222-2222-2222-222222222222",
        "target_profile_type": "profile.json",
        "field_mappings": [
            {"source_path": "rows/row[]/key", "target_path": "Root/list[]/key"},
        ],
        "function_mappings": [
            {
                "function_type": "lowercase",
                "inputs": ["rows/row[]/name"],
                "target_path": "Root/list[]/name",
                "parameters": {},
            },
        ],
    }
    err = DirectMapBuilder.validate_config(cfg)
    assert err is not None
    assert err.error_code == "UNSUPPORTED_TRANSFORM_ROUTE"
    assert err.field == "function_mappings"


# ---------------------------------------------------------------------------
# Native property map functions (Get/Set Process/Document Property)
# ---------------------------------------------------------------------------


_DEFINED_PARAMS = {
    "process_property_component_id": "cccccccc-3333-3333-3333-333333333333",
    "process_property_component_name": "New Process Property",
    "process_property_key": "0e89ebf1-cd46-46df-904e-94c7e7ade31e",
    "process_property_name": "Example Property",
}


def test_build_dynamic_process_property_set_maps_source_to_input_key_2():
    xml = _build(
        _function_map_config(
            function_mappings=[
                {
                    "function_type": "dynamic_process_property_set",
                    "inputs": ["rows/row[]/name"],
                    "parameters": {"property_name": "DPP_EXAMPLE"},
                },
            ],
        )
    )
    root = _parse(xml)
    steps = root.findall("bns:object/Map/Functions/FunctionStep", NS)
    assert len(steps) == 1
    assert steps[0].attrib["type"] == "PropertySet"
    # No-output setter: empty <Outputs/>.
    assert steps[0].findall("Outputs/Output") == []

    mappings = root.findall("bns:object/Map/Mappings/Mapping", NS)
    # Only the source→function-input mapping; no function→profile mapping.
    assert len(mappings) == 1
    assert mappings[0].attrib["toType"] == "function"
    assert mappings[0].attrib["toFunction"] == "1"
    assert mappings[0].attrib["toKey"] == "2"
    assert "fromFunction" not in mappings[0].attrib
    # No function-output→profile mapping is emitted for a setter.
    assert [m for m in mappings if m.attrib.get("fromType") == "function"] == []


def test_build_document_property_get_uses_output_key_3():
    xml = _build(
        _function_map_config(
            function_mappings=[
                {
                    "function_type": "document_property_get",
                    "inputs": [],
                    "target_path": "Root/list[]/status",
                    "parameters": {"document_property_name": "DDP_FOO"},
                },
            ],
        )
    )
    root = _parse(xml)
    steps = root.findall("bns:object/Map/Functions/FunctionStep", NS)
    assert steps[0].attrib["type"] == "DocumentPropertyGet"
    assert steps[0].findall("Inputs/Input") == []

    mappings = root.findall("bns:object/Map/Mappings/Mapping", NS)
    function_to_profile = [m for m in mappings if m.attrib.get("fromType") == "function"]
    assert len(function_to_profile) == 1
    assert function_to_profile[0].attrib["fromFunction"] == "1"
    assert function_to_profile[0].attrib["fromKey"] == "3"


def test_build_defined_process_property_get_uses_output_key_1():
    xml = _build(
        _function_map_config(
            function_mappings=[
                {
                    "function_type": "defined_process_property_get",
                    "inputs": [],
                    "target_path": "Root/list[]/status",
                    "parameters": dict(_DEFINED_PARAMS),
                },
            ],
        )
    )
    root = _parse(xml)
    steps = root.findall("bns:object/Map/Functions/FunctionStep", NS)
    assert steps[0].attrib["type"] == "DefinedProcessPropertyGet"
    cfg_el = steps[0].find("Configuration/DefinedProcessProperty")
    assert cfg_el is not None
    assert cfg_el.attrib["componentId"] == _DEFINED_PARAMS[
        "process_property_component_id"
    ]

    mappings = root.findall("bns:object/Map/Mappings/Mapping", NS)
    function_to_profile = [m for m in mappings if m.attrib.get("fromType") == "function"]
    assert len(function_to_profile) == 1
    assert function_to_profile[0].attrib["fromKey"] == "1"


def test_validate_config_setter_forbids_target_path():
    cfg = _function_map_config(
        function_mappings=[
            {
                "function_type": "document_property_set",
                "inputs": ["rows/row[]/name"],
                "target_path": "Root/list[]/status",
                "parameters": {"document_property_name": "DDP_FOO"},
            },
        ],
    )
    err = MapFunctionBuilder.validate_config(cfg)
    assert err is not None
    assert err.error_code == "PROFILE_FIELD_VALIDATION_FAILED"
    assert err.field.endswith("target_path")


def test_validate_config_setter_accepts_omitted_target_path():
    src_idx, tgt_idx = _build_indexes()
    cfg = _function_map_config(
        function_mappings=[
            {
                "function_type": "defined_process_property_set",
                "inputs": ["rows/row[]/name"],
                "parameters": dict(_DEFINED_PARAMS),
            },
        ],
    )
    err = MapFunctionBuilder.validate_config(
        cfg, source_index=src_idx, target_index=tgt_idx
    )
    assert err is None


def test_validate_config_getter_requires_target_path():
    cfg = _function_map_config(
        function_mappings=[
            {
                "function_type": "document_property_get",
                "inputs": [],
                "parameters": {"document_property_name": "DDP_FOO"},
            },
        ],
    )
    err = MapFunctionBuilder.validate_config(cfg)
    assert err is not None
    assert err.error_code == "PROFILE_FIELD_VALIDATION_FAILED"
    assert err.field.endswith("target_path")
