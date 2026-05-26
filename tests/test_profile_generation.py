"""Tests for issue #43 profile field generation helpers."""

from __future__ import annotations

from typing import Any, Dict, List

import pytest

from boomi_mcp.categories.components.builders.connector_builder import (
    BuilderValidationError,
)
from boomi_mcp.categories.components.builders.profile_generation import (
    DUPLICATE_PROFILE_FIELD_PATH,
    DUPLICATE_TARGET_MAPPING,
    INVALID_PROFILE_FIELD_PATH,
    PROFILE_FIELD_NOT_FOUND,
    PROFILE_FIELD_NOT_MAPPABLE,
    PROFILE_GENERATION_VALIDATION_FAILED,
    UNSUPPORTED_PROFILE_FIELD_TYPE,
    UNSUPPORTED_PROFILE_GENERATION_SOURCE,
    build_profile_generation_artifacts,
    profile_from_db_read_fields,
    profile_from_json_schema,
    profile_from_xml_schema,
    reject_unsupported_generation_source,
    validate_field_mappings,
)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


def _db_fields() -> List[Dict[str, Any]]:
    return [
        {"name": "source_a", "data_type": "character", "required": True},
        {"name": "source_b", "data_type": "datetime"},
        {"name": "source_c", "data_type": "number"},
    ]


def _json_profile() -> Dict[str, Any]:
    return {
        "format": "json",
        "root": {
            "name": "Root",
            "kind": "object",
            "children": [
                {"name": "target_a", "kind": "simple", "data_type": "character"},
                {"name": "target_b", "kind": "simple", "data_type": "number"},
                {"name": "ts", "kind": "simple", "data_type": "datetime"},
                {"name": "flag", "kind": "simple", "data_type": "boolean"},
                {
                    "name": "child",
                    "kind": "object",
                    "children": [
                        {"name": "leaf", "kind": "simple", "data_type": "character"},
                    ],
                },
                {
                    "name": "list",
                    "kind": "array",
                    "children": [
                        {"name": "key", "kind": "simple", "data_type": "character"},
                    ],
                },
            ],
        },
    }


# ---------------------------------------------------------------------------
# profile_from_db_read_fields — happy paths
# ---------------------------------------------------------------------------


def test_profile_from_db_read_fields_happy_path():
    result = profile_from_db_read_fields(_db_fields())
    assert result["generation_mode"] == "profile_from_db_read_fields"
    assert result["component_type"] == "profile.db"
    assert result["profile_type"] == "database.read"
    assert result["component_name"] is None
    output_fields = result["profile_config"]["output_fields"]
    assert [f["name"] for f in output_fields] == ["source_a", "source_b", "source_c"]
    assert all(f["enforce_unique"] is False for f in output_fields)
    for path, entry in result["field_index_by_path"].items():
        assert entry["mappable"] is True
        assert entry["profile_component_type"] == "profile.db"
        assert entry["source"] == "db_read_fields"
        assert entry["path"] == path
    assert result["mappable_paths"] == ["source_a", "source_b", "source_c"]


def test_profile_from_db_read_fields_required_becomes_mandatory():
    result = profile_from_db_read_fields(_db_fields())
    by_name = {f["name"]: f for f in result["profile_config"]["output_fields"]}
    assert by_name["source_a"]["mandatory"] is True
    assert by_name["source_b"]["mandatory"] is False


def test_profile_from_db_read_fields_preserves_order():
    fields = [
        {"name": "z", "data_type": "character"},
        {"name": "a", "data_type": "number"},
        {"name": "m", "data_type": "datetime"},
    ]
    result = profile_from_db_read_fields(fields)
    assert [f["name"] for f in result["profile_config"]["output_fields"]] == ["z", "a", "m"]
    assert result["mappable_paths"] == ["z", "a", "m"]


def test_profile_from_db_read_fields_accepts_component_name():
    result = profile_from_db_read_fields(
        _db_fields(), component_name="DEMO Source Profile"
    )
    assert result["component_name"] == "DEMO Source Profile"


# ---------------------------------------------------------------------------
# profile_from_db_read_fields — error cases
# ---------------------------------------------------------------------------


def test_profile_from_db_read_fields_rejects_duplicate_name():
    fields = [
        {"name": "x", "data_type": "character"},
        {"name": "x", "data_type": "character"},
    ]
    with pytest.raises(BuilderValidationError) as excinfo:
        profile_from_db_read_fields(fields)
    assert excinfo.value.error_code == DUPLICATE_PROFILE_FIELD_PATH
    assert excinfo.value.field == "fields[1].name"
    assert excinfo.value.details == {
        "path": "x",
        "first_index": 0,
        "duplicate_index": 1,
    }


def test_profile_from_db_read_fields_rejects_unsupported_type():
    fields = [{"name": "x", "data_type": "blob"}]
    with pytest.raises(BuilderValidationError) as excinfo:
        profile_from_db_read_fields(fields)
    assert excinfo.value.error_code == UNSUPPORTED_PROFILE_FIELD_TYPE
    assert excinfo.value.field == "fields[0].data_type"
    assert excinfo.value.details["data_type"] == "blob"
    assert "character" in excinfo.value.details["supported"]


def test_profile_from_db_read_fields_rejects_reserved_path_char():
    fields = [{"name": "a/b", "data_type": "character"}]
    with pytest.raises(BuilderValidationError) as excinfo:
        profile_from_db_read_fields(fields)
    assert excinfo.value.error_code == INVALID_PROFILE_FIELD_PATH
    assert excinfo.value.details["reserved_char"] == "/"


def test_profile_from_db_read_fields_rejects_blank_name():
    fields = [{"name": "  ", "data_type": "character"}]
    with pytest.raises(BuilderValidationError) as excinfo:
        profile_from_db_read_fields(fields)
    assert excinfo.value.error_code == PROFILE_GENERATION_VALIDATION_FAILED
    assert excinfo.value.field == "fields[0].name"


def test_profile_from_db_read_fields_rejects_empty_iterable():
    with pytest.raises(BuilderValidationError) as excinfo:
        profile_from_db_read_fields([])
    assert excinfo.value.error_code == PROFILE_GENERATION_VALIDATION_FAILED
    assert excinfo.value.field == "fields"


def test_profile_from_db_read_fields_rejects_non_mapping_entry():
    with pytest.raises(BuilderValidationError) as excinfo:
        profile_from_db_read_fields([42])
    assert excinfo.value.error_code == PROFILE_GENERATION_VALIDATION_FAILED
    assert excinfo.value.field == "fields[0]"


# ---------------------------------------------------------------------------
# profile_from_json_schema — happy paths
# ---------------------------------------------------------------------------


def test_profile_from_json_schema_happy_path():
    result = profile_from_json_schema(_json_profile())
    assert result["generation_mode"] == "profile_from_json_schema"
    assert result["component_type"] == "profile.json"
    assert result["profile_type"] == "json.generated"

    index = result["field_index_by_path"]
    expected_paths = {
        "Root",
        "Root/target_a",
        "Root/target_b",
        "Root/ts",
        "Root/flag",
        "Root/child",
        "Root/child/leaf",
        "Root/list",
        "Root/list[]/key",
    }
    assert set(index.keys()) == expected_paths
    # All four JSON leaf data types are represented.
    leaf_types = {
        e["data_type"]
        for e in index.values()
        if e["kind"] == "simple"
    }
    assert leaf_types == {"character", "number", "datetime", "boolean"}
    # Every index entry carries the canonical metadata fields.
    for entry in index.values():
        assert entry["profile_component_type"] == "profile.json"
        assert entry["source"] == "json_schema"


def test_profile_from_json_schema_array_path_uses_brackets():
    result = profile_from_json_schema(_json_profile())
    assert "Root/list[]/key" in result["field_index_by_path"]
    # The array node itself does not have [] appended (only its children do).
    assert "Root/list" in result["field_index_by_path"]
    assert result["field_index_by_path"]["Root/list"]["kind"] == "array"


def test_profile_from_json_schema_mappable_paths_only_includes_simple_leaves():
    result = profile_from_json_schema(_json_profile())
    assert set(result["mappable_paths"]) == {
        "Root/target_a",
        "Root/target_b",
        "Root/ts",
        "Root/flag",
        "Root/child/leaf",
        "Root/list[]/key",
    }
    # Structural nodes are NOT in mappable_paths.
    assert "Root" not in result["mappable_paths"]
    assert "Root/list" not in result["mappable_paths"]
    assert "Root/child" not in result["mappable_paths"]


def test_profile_from_json_schema_preserves_child_order():
    result = profile_from_json_schema(_json_profile())
    root_children = result["profile_config"]["root"]["children"]
    assert [c["name"] for c in root_children] == [
        "target_a",
        "target_b",
        "ts",
        "flag",
        "child",
        "list",
    ]


def test_profile_from_json_schema_does_not_echo_description():
    profile = {
        "format": "json",
        "root": {
            "name": "Root",
            "kind": "object",
            "description": "<<should not be echoed>>",
            "children": [
                {
                    "name": "x",
                    "kind": "simple",
                    "data_type": "character",
                    "description": "<<should not be echoed either>>",
                },
            ],
        },
    }
    result = profile_from_json_schema(profile)
    import json as _json

    blob = _json.dumps(result)
    assert "should not be echoed" not in blob


# ---------------------------------------------------------------------------
# profile_from_json_schema — error cases
# ---------------------------------------------------------------------------


def test_profile_from_json_schema_rejects_duplicate_sibling():
    profile = {
        "format": "json",
        "root": {
            "name": "Root",
            "kind": "object",
            "children": [
                {"name": "x", "kind": "simple", "data_type": "character"},
                {"name": "x", "kind": "simple", "data_type": "character"},
            ],
        },
    }
    with pytest.raises(BuilderValidationError) as excinfo:
        profile_from_json_schema(profile)
    assert excinfo.value.error_code == DUPLICATE_PROFILE_FIELD_PATH
    assert excinfo.value.details["parent_path"] == "Root"
    assert excinfo.value.details["path"] == "Root/x"


def test_profile_from_json_schema_rejects_simple_without_data_type():
    profile = {
        "format": "json",
        "root": {
            "name": "Root",
            "kind": "object",
            "children": [
                {"name": "x", "kind": "simple"},
            ],
        },
    }
    with pytest.raises(BuilderValidationError) as excinfo:
        profile_from_json_schema(profile)
    assert excinfo.value.error_code == UNSUPPORTED_PROFILE_FIELD_TYPE


def test_profile_from_json_schema_rejects_object_with_data_type():
    profile = {
        "format": "json",
        "root": {
            "name": "Root",
            "kind": "object",
            "data_type": "character",
            "children": [
                {"name": "x", "kind": "simple", "data_type": "character"},
            ],
        },
    }
    with pytest.raises(BuilderValidationError) as excinfo:
        profile_from_json_schema(profile)
    assert excinfo.value.error_code == PROFILE_GENERATION_VALIDATION_FAILED
    assert excinfo.value.field == "payload_profile.root.data_type"


def test_profile_from_json_schema_rejects_object_with_empty_children():
    profile = {
        "format": "json",
        "root": {"name": "Root", "kind": "object", "children": []},
    }
    with pytest.raises(BuilderValidationError) as excinfo:
        profile_from_json_schema(profile)
    assert excinfo.value.error_code == PROFILE_GENERATION_VALIDATION_FAILED
    assert excinfo.value.field == "payload_profile.root.children"


def test_profile_from_json_schema_rejects_simple_root():
    profile = {
        "format": "json",
        "root": {"name": "Root", "kind": "simple", "data_type": "character"},
    }
    with pytest.raises(BuilderValidationError) as excinfo:
        profile_from_json_schema(profile)
    assert excinfo.value.error_code == PROFILE_GENERATION_VALIDATION_FAILED
    assert excinfo.value.field == "payload_profile.root.kind"


def test_profile_from_json_schema_rejects_non_json_format():
    profile = {"format": "xml", "root": {"name": "Root", "kind": "object"}}
    with pytest.raises(BuilderValidationError) as excinfo:
        profile_from_json_schema(profile)
    assert excinfo.value.error_code == PROFILE_GENERATION_VALIDATION_FAILED
    assert excinfo.value.field == "payload_profile.format"


def test_profile_from_json_schema_rejects_reserved_char_in_node_name():
    profile = {
        "format": "json",
        "root": {
            "name": "Root",
            "kind": "object",
            "children": [
                {"name": "a/b", "kind": "simple", "data_type": "character"},
            ],
        },
    }
    with pytest.raises(BuilderValidationError) as excinfo:
        profile_from_json_schema(profile)
    assert excinfo.value.error_code == INVALID_PROFILE_FIELD_PATH


# ---------------------------------------------------------------------------
# profile_from_xml_schema (issue #26 backfill)
# ---------------------------------------------------------------------------


def _xml_profile() -> Dict[str, Any]:
    return {
        "format": "xml",
        "root": {
            "name": "ShippingOrders",
            "kind": "element",
            "min_occurs": 1,
            "max_occurs": -1,
            "children": [
                {
                    "name": "ShippingOrder",
                    "kind": "element",
                    "max_occurs": -1,
                    "children": [
                        {"name": "OrderID", "kind": "element", "data_type": "character"},
                        {"name": "OrderDate", "kind": "element", "data_type": "datetime"},
                        {"name": "Total", "kind": "element", "data_type": "number"},
                        {"name": "Active", "kind": "element", "data_type": "boolean"},
                    ],
                },
            ],
        },
    }


def test_profile_from_xml_schema_happy_path():
    result = profile_from_xml_schema(_xml_profile())
    assert result["generation_mode"] == "profile_from_xml_schema"
    assert result["component_type"] == "profile.xml"
    assert result["profile_type"] == "xml.generated"

    index = result["field_index_by_path"]
    expected_paths = {
        "ShippingOrders",
        "ShippingOrders[]/ShippingOrder",
        "ShippingOrders[]/ShippingOrder[]/OrderID",
        "ShippingOrders[]/ShippingOrder[]/OrderDate",
        "ShippingOrders[]/ShippingOrder[]/Total",
        "ShippingOrders[]/ShippingOrder[]/Active",
    }
    assert set(index.keys()) == expected_paths
    # Every entry advertises profile.xml metadata.
    for entry in index.values():
        assert entry["profile_component_type"] == "profile.xml"
        assert entry["source"] == "xml_schema"
        assert entry["kind"] == "element"
    # Mappable_paths covers only leaves (elements without children).
    assert set(result["mappable_paths"]) == {
        "ShippingOrders[]/ShippingOrder[]/OrderID",
        "ShippingOrders[]/ShippingOrder[]/OrderDate",
        "ShippingOrders[]/ShippingOrder[]/Total",
        "ShippingOrders[]/ShippingOrder[]/Active",
    }


def test_profile_from_xml_schema_non_repeating_root():
    profile = {
        "format": "xml",
        "root": {
            "name": "Order",
            "kind": "element",
            "max_occurs": 1,
            "children": [
                {"name": "id", "kind": "element", "data_type": "character"},
            ],
        },
    }
    result = profile_from_xml_schema(profile)
    # max_occurs=1 means no [] appended to the segment for children.
    assert set(result["field_index_by_path"].keys()) == {"Order", "Order/id"}
    assert result["mappable_paths"] == ["Order/id"]


def test_profile_from_xml_schema_rejects_simple_root():
    profile = {
        "format": "xml",
        "root": {"name": "Root", "kind": "simple", "data_type": "character"},
    }
    with pytest.raises(BuilderValidationError) as excinfo:
        profile_from_xml_schema(profile)
    assert excinfo.value.error_code == PROFILE_GENERATION_VALIDATION_FAILED
    assert excinfo.value.field == "payload_profile.root.kind"


def test_profile_from_xml_schema_rejects_unsupported_data_type():
    profile = {
        "format": "xml",
        "root": {
            "name": "Root",
            "kind": "element",
            "children": [{"name": "x", "kind": "element", "data_type": "blob"}],
        },
    }
    with pytest.raises(BuilderValidationError) as excinfo:
        profile_from_xml_schema(profile)
    assert excinfo.value.error_code == UNSUPPORTED_PROFILE_FIELD_TYPE


def test_profile_from_xml_schema_rejects_duplicate_sibling():
    profile = {
        "format": "xml",
        "root": {
            "name": "Root",
            "kind": "element",
            "children": [
                {"name": "x", "kind": "element", "data_type": "character"},
                {"name": "x", "kind": "element", "data_type": "character"},
            ],
        },
    }
    with pytest.raises(BuilderValidationError) as excinfo:
        profile_from_xml_schema(profile)
    assert excinfo.value.error_code == DUPLICATE_PROFILE_FIELD_PATH


def test_profile_from_xml_schema_rejects_non_xml_format():
    with pytest.raises(BuilderValidationError) as excinfo:
        profile_from_xml_schema({"format": "json", "root": {"name": "R", "kind": "element"}})
    assert excinfo.value.error_code == PROFILE_GENERATION_VALIDATION_FAILED
    assert excinfo.value.field == "payload_profile.format"


def test_profile_from_xml_schema_rejects_invalid_max_occurs():
    profile = {
        "format": "xml",
        "root": {
            "name": "Root",
            "kind": "element",
            "max_occurs": 0,
            "children": [{"name": "x", "kind": "element", "data_type": "character"}],
        },
    }
    with pytest.raises(BuilderValidationError) as excinfo:
        profile_from_xml_schema(profile)
    assert excinfo.value.error_code == PROFILE_GENERATION_VALIDATION_FAILED
    assert excinfo.value.field == "payload_profile.root.max_occurs"


def test_profile_from_xml_schema_structural_element_rejects_data_type():
    profile = {
        "format": "xml",
        "root": {
            "name": "Root",
            "kind": "element",
            "data_type": "character",
            "children": [{"name": "x", "kind": "element", "data_type": "character"}],
        },
    }
    with pytest.raises(BuilderValidationError) as excinfo:
        profile_from_xml_schema(profile)
    assert excinfo.value.error_code == PROFILE_GENERATION_VALIDATION_FAILED


def test_profile_from_xml_schema_rejects_reserved_path_chars():
    profile = {
        "format": "xml",
        "root": {
            "name": "Root",
            "kind": "element",
            "children": [{"name": "a/b", "kind": "element", "data_type": "character"}],
        },
    }
    with pytest.raises(BuilderValidationError) as excinfo:
        profile_from_xml_schema(profile)
    assert excinfo.value.error_code == INVALID_PROFILE_FIELD_PATH


# ---------------------------------------------------------------------------
# validate_field_mappings — happy + error cases
# ---------------------------------------------------------------------------


def _indexes():
    src = profile_from_db_read_fields(_db_fields())["field_index_by_path"]
    tgt = profile_from_json_schema(_json_profile())["field_index_by_path"]
    return src, tgt


def test_validate_field_mappings_happy_path():
    src, tgt = _indexes()
    mappings = [
        {"source_field": "source_a", "target_path": "Root/target_a"},
        {"source_field": "source_c", "target_path": "Root/target_b"},
    ]
    result = validate_field_mappings(src, tgt, mappings)
    assert result == [
        {
            "route": "direct",
            "source_path": "source_a",
            "target_path": "Root/target_a",
            "source_data_type": "character",
            "target_data_type": "character",
        },
        {
            "route": "direct",
            "source_path": "source_c",
            "target_path": "Root/target_b",
            "source_data_type": "number",
            "target_data_type": "number",
        },
    ]


def test_validate_field_mappings_unknown_source_path():
    src, tgt = _indexes()
    with pytest.raises(BuilderValidationError) as excinfo:
        validate_field_mappings(
            src,
            tgt,
            [{"source_field": "missing", "target_path": "Root/target_a"}],
        )
    assert excinfo.value.error_code == PROFILE_FIELD_NOT_FOUND
    assert excinfo.value.details == {"path": "missing", "side": "source"}


def test_validate_field_mappings_unknown_target_path():
    src, tgt = _indexes()
    with pytest.raises(BuilderValidationError) as excinfo:
        validate_field_mappings(
            src,
            tgt,
            [{"source_field": "source_a", "target_path": "Root/ghost"}],
        )
    assert excinfo.value.error_code == PROFILE_FIELD_NOT_FOUND
    assert excinfo.value.details == {"path": "Root/ghost", "side": "target"}


def test_validate_field_mappings_object_target():
    src, tgt = _indexes()
    with pytest.raises(BuilderValidationError) as excinfo:
        validate_field_mappings(
            src,
            tgt,
            [{"source_field": "source_a", "target_path": "Root/child"}],
        )
    assert excinfo.value.error_code == PROFILE_FIELD_NOT_MAPPABLE
    assert excinfo.value.details == {"path": "Root/child", "kind": "object"}


def test_validate_field_mappings_array_target():
    src, tgt = _indexes()
    with pytest.raises(BuilderValidationError) as excinfo:
        validate_field_mappings(
            src,
            tgt,
            [{"source_field": "source_a", "target_path": "Root/list"}],
        )
    assert excinfo.value.error_code == PROFILE_FIELD_NOT_MAPPABLE
    assert excinfo.value.details == {"path": "Root/list", "kind": "array"}


def test_validate_field_mappings_duplicate_target():
    src, tgt = _indexes()
    mappings = [
        {"source_field": "source_a", "target_path": "Root/target_a"},
        {"source_field": "source_c", "target_path": "Root/target_a"},
    ]
    with pytest.raises(BuilderValidationError) as excinfo:
        validate_field_mappings(src, tgt, mappings)
    assert excinfo.value.error_code == DUPLICATE_TARGET_MAPPING
    assert excinfo.value.details == {
        "path": "Root/target_a",
        "first_index": 0,
        "duplicate_index": 1,
    }


def test_validate_field_mappings_same_source_different_targets_ok():
    src, tgt = _indexes()
    mappings = [
        {"source_field": "source_a", "target_path": "Root/target_a"},
        {"source_field": "source_a", "target_path": "Root/ts"},
    ]
    result = validate_field_mappings(src, tgt, mappings)
    assert len(result) == 2


def test_validate_field_mappings_rejects_blank_source_field():
    src, tgt = _indexes()
    with pytest.raises(BuilderValidationError) as excinfo:
        validate_field_mappings(
            src,
            tgt,
            [{"source_field": "  ", "target_path": "Root/target_a"}],
        )
    assert excinfo.value.error_code == PROFILE_GENERATION_VALIDATION_FAILED


# ---------------------------------------------------------------------------
# reject_unsupported_generation_source
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "mode",
    [
        "profile_from_db_metadata",
        "profile_from_sample_json",
        "profile_from_xsd",
        "profile_from_sample_xml",
    ],
)
def test_reject_unsupported_generation_source_known_deferred_modes(mode: str):
    with pytest.raises(BuilderValidationError) as excinfo:
        reject_unsupported_generation_source(mode)
    assert excinfo.value.error_code == UNSUPPORTED_PROFILE_GENERATION_SOURCE
    assert excinfo.value.field == "generation_mode"
    assert "#47" in (excinfo.value.hint or "")
    assert excinfo.value.details == {
        "mode": mode,
        "deferred_to_issue": "#47",
    }


def test_reject_unsupported_generation_source_unknown_mode():
    with pytest.raises(BuilderValidationError) as excinfo:
        reject_unsupported_generation_source("profile_from_unicorn")
    assert excinfo.value.error_code == UNSUPPORTED_PROFILE_GENERATION_SOURCE


# ---------------------------------------------------------------------------
# build_profile_generation_artifacts
# ---------------------------------------------------------------------------


def test_build_profile_generation_artifacts_aggregates():
    direct_ops = [
        {
            "operation_type": "direct",
            "source_field": "source_a",
            "target_path": "Root/target_a",
        },
    ]
    artifacts = build_profile_generation_artifacts(
        {"fields": _db_fields()},
        _json_profile(),
        direct_operations=direct_ops,
    )
    assert artifacts["source"]["component_type"] == "profile.db"
    assert artifacts["target"]["component_type"] == "profile.json"
    assert len(artifacts["direct_mappings"]) == 1
    assert artifacts["direct_mappings"][0]["target_path"] == "Root/target_a"

    unsupported_modes = {entry["mode"] for entry in artifacts["unsupported_sources"]}
    assert unsupported_modes == {
        "profile_from_db_metadata",
        "profile_from_sample_json",
        "profile_from_xsd",
        "profile_from_sample_xml",
    }
    for entry in artifacts["unsupported_sources"]:
        assert entry["deferred_to_issue"] == "#47"
        assert "#47" in entry["hint"]


def test_build_profile_generation_artifacts_accepts_empty_direct_ops():
    artifacts = build_profile_generation_artifacts(
        {"fields": _db_fields()},
        _json_profile(),
    )
    assert artifacts["direct_mappings"] == []


def test_build_profile_generation_artifacts_propagates_target_mismatch():
    direct_ops = [
        {
            "operation_type": "direct",
            "source_field": "source_a",
            "target_path": "Root/list",
        },
    ]
    with pytest.raises(BuilderValidationError) as excinfo:
        build_profile_generation_artifacts(
            {"fields": _db_fields()},
            _json_profile(),
            direct_operations=direct_ops,
        )
    assert excinfo.value.error_code == PROFILE_FIELD_NOT_MAPPABLE


def test_build_profile_generation_artifacts_rejects_missing_fields():
    with pytest.raises(BuilderValidationError) as excinfo:
        build_profile_generation_artifacts(
            {"fields": []},
            _json_profile(),
        )
    assert excinfo.value.error_code == PROFILE_GENERATION_VALIDATION_FAILED
    assert excinfo.value.field == "source_result_schema.fields"


# ---------------------------------------------------------------------------
# Anti-template hygiene — generation metadata must not echo SQL or payload
# ---------------------------------------------------------------------------


def test_artifacts_carry_no_sql_or_payload_body():
    import json as _json

    artifacts = build_profile_generation_artifacts(
        {"fields": _db_fields()},
        _json_profile(),
    )
    blob = _json.dumps(artifacts).lower()
    for marker in (
        "select ",
        " from ",
        "where ",
        "<?xml",
        "<process",
        "<connector",
        "<operation",
    ):
        assert marker not in blob, (
            f"generation artifacts unexpectedly contain template marker {marker!r}"
        )
