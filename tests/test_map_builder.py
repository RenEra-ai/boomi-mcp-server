"""Tests for issue #26 DirectMapBuilder.

Verifies the transform.map XML envelope and end-to-end integration with the
JSON / XML profile builders' field indexes. Anchored against live Boomi
reference shapes (renera ``77bb73d5-...`` Order DB→XML and work
``5aa8d537-...`` XML→JSON, fetched 2026-05-25).
"""

from __future__ import annotations

from typing import Any, Dict
from xml.etree import ElementTree as ET

import pytest

from boomi_mcp.categories.components.builders.connector_builder import (
    BuilderValidationError,
)
from boomi_mcp.categories.components.builders.json_profile_builder import (
    JSONGeneratedProfileBuilder,
)
from boomi_mcp.categories.components.builders.map_builder import (
    DirectMapBuilder,
    MAP_BUILDERS,
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
                ]},
            ],
        },
    }


def _build_indexes():
    src_idx = XMLGeneratedProfileBuilder.build_field_index(_xml_source_config())
    tgt_idx = JSONGeneratedProfileBuilder.build_field_index(_json_target_config())
    return src_idx, tgt_idx


def _direct_map_config(**overrides: Any) -> Dict[str, Any]:
    cfg = {
        "component_type": "transform.map",
        "map_type": "direct",
        "component_name": "XML to JSON",
        "source_profile_id": "aaaaaaaa-1111-1111-1111-111111111111",
        "source_profile_type": "profile.xml",
        "target_profile_id": "bbbbbbbb-2222-2222-2222-222222222222",
        "target_profile_type": "profile.json",
        "field_mappings": [
            {"source_path": "rows/row[]/key", "target_path": "Root/list[]/key"},
            {"source_path": "rows/row[]/name", "target_path": "Root/list[]/name"},
            {"source_path": "rows/row[]/count", "target_path": "Root/list[]/count"},
        ],
    }
    cfg.update(overrides)
    return cfg


# ---------------------------------------------------------------------------
# Registry
# ---------------------------------------------------------------------------


def test_registry_resolves_direct_map_builder():
    assert get_map_builder("transform.map", "direct") is not None
    assert isinstance(get_map_builder("transform.map", "direct"), DirectMapBuilder)


def test_registry_returns_none_for_unknown_map_type():
    # Note: ("transform.map", "function") is wired by #40 (MapFunctionBuilder);
    # this test only asserts truly-unknown pairs return None.
    assert get_map_builder("transform.unknown", "direct") is None
    assert get_map_builder("transform.map", "totally-bogus") is None
    assert get_map_builder("", "") is None


def test_registry_advertises_direct_map_pair():
    assert ("transform.map", "direct") in MAP_BUILDERS


# ---------------------------------------------------------------------------
# validate_config — required fields
# ---------------------------------------------------------------------------


def test_validate_config_accepts_minimal_direct_config():
    src_idx, tgt_idx = _build_indexes()
    err = DirectMapBuilder.validate_config(
        _direct_map_config(),
        source_index=src_idx,
        target_index=tgt_idx,
    )
    assert err is None


def test_validate_config_rejects_missing_source_profile_id():
    cfg = _direct_map_config()
    cfg.pop("source_profile_id")
    err = DirectMapBuilder.validate_config(cfg)
    assert err is not None
    assert err.error_code == "MAP_PROFILE_REF_REQUIRED"
    assert err.field == "source_profile_id"


def test_validate_config_rejects_missing_target_profile_id():
    cfg = _direct_map_config()
    cfg.pop("target_profile_id")
    err = DirectMapBuilder.validate_config(cfg)
    assert err is not None
    assert err.error_code == "MAP_PROFILE_REF_REQUIRED"
    assert err.field == "target_profile_id"


def test_validate_config_rejects_blank_profile_id():
    cfg = _direct_map_config()
    cfg["source_profile_id"] = "  "
    err = DirectMapBuilder.validate_config(cfg)
    assert err is not None
    assert err.error_code == "MAP_PROFILE_REF_REQUIRED"


def test_validate_config_rejects_missing_profile_type():
    cfg = _direct_map_config()
    cfg["source_profile_type"] = "unknown"
    err = DirectMapBuilder.validate_config(cfg)
    assert err is not None
    assert err.error_code == "PROFILE_FIELD_VALIDATION_FAILED"
    assert err.field == "source_profile_type"


def test_validate_config_rejects_wrong_map_type():
    cfg = _direct_map_config()
    cfg["map_type"] = "function"
    err = DirectMapBuilder.validate_config(cfg)
    assert err is not None
    assert err.error_code == "PROFILE_FIELD_VALIDATION_FAILED"
    assert err.field == "map_type"


def test_validate_config_rejects_missing_component_name():
    cfg = _direct_map_config()
    cfg["component_name"] = ""
    err = DirectMapBuilder.validate_config(cfg)
    assert err is not None
    assert err.error_code == "PROFILE_FIELD_VALIDATION_FAILED"
    assert err.field == "component_name"


def test_validate_config_rejects_missing_field_mappings():
    cfg = _direct_map_config()
    cfg.pop("field_mappings")
    err = DirectMapBuilder.validate_config(cfg)
    assert err is not None
    assert err.error_code == "PROFILE_FIELD_VALIDATION_FAILED"
    assert err.field == "field_mappings"


def test_validate_config_rejects_empty_field_mappings():
    cfg = _direct_map_config()
    cfg["field_mappings"] = []
    err = DirectMapBuilder.validate_config(cfg)
    assert err is not None
    assert err.error_code == "PROFILE_FIELD_VALIDATION_FAILED"


def test_validate_config_rejects_blank_source_or_target_path():
    cfg = _direct_map_config()
    cfg["field_mappings"][0]["source_path"] = "  "
    err = DirectMapBuilder.validate_config(cfg)
    assert err is not None
    assert err.error_code == "PROFILE_FIELD_VALIDATION_FAILED"


# ---------------------------------------------------------------------------
# validate_config — duplicate target binding
# ---------------------------------------------------------------------------


def test_validate_config_rejects_duplicate_target_path():
    cfg = _direct_map_config()
    cfg["field_mappings"].append(
        {"source_path": "rows/row[]/count", "target_path": "Root/list[]/name"}
    )
    err = DirectMapBuilder.validate_config(cfg)
    assert err is not None
    assert err.error_code == "DUPLICATE_TARGET_MAPPING"
    assert err.field == "field_mappings[3].target_path"


def test_validate_config_allows_same_source_to_multiple_targets():
    cfg = _direct_map_config()
    cfg["field_mappings"] = [
        {"source_path": "rows/row[]/key", "target_path": "Root/list[]/key"},
        {"source_path": "rows/row[]/key", "target_path": "Root/list[]/name"},
    ]
    src_idx, tgt_idx = _build_indexes()
    err = DirectMapBuilder.validate_config(
        cfg, source_index=src_idx, target_index=tgt_idx
    )
    assert err is None


# ---------------------------------------------------------------------------
# validate_config — UNSUPPORTED_TRANSFORM_ROUTE
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "key,expected_pointer",
    [
        # Raw-XML escape hatches still reject regardless of builder.
        ("functions", "structured function_mappings"),
        ("function_steps", "structured function_mappings"),
        ("scripts", "#41"),
        ("map_scripts", "#41"),
        ("xslt", "#42"),
        ("xslt_source", "#42"),
        ("expression", "#41"),
        ("expressions", "#41"),
        # Route-class keys: direct builder routes the caller at the function
        # builder instead of the bare future-issue pointer.
        ("function_mappings", "map_type='function'"),
        ("default_values", "function_type='default_value'"),
        ("defaults", "function_type='default_value'"),
        ("lookup", "function_type='simple_lookup'"),
        ("lookups", "function_type='simple_lookup'"),
    ],
)
def test_validate_config_rejects_unsupported_transform_route(key, expected_pointer):
    cfg = _direct_map_config()
    cfg[key] = ["something"]
    err = DirectMapBuilder.validate_config(cfg)
    assert err is not None
    assert err.error_code == "UNSUPPORTED_TRANSFORM_ROUTE"
    assert expected_pointer in (err.hint or "")


# ---------------------------------------------------------------------------
# validate_config — index-sensitive checks (apply-time-style)
# ---------------------------------------------------------------------------


def test_validate_config_rejects_unknown_source_path():
    src_idx, tgt_idx = _build_indexes()
    cfg = _direct_map_config()
    cfg["field_mappings"][0]["source_path"] = "rows/row[]/missing"
    err = DirectMapBuilder.validate_config(
        cfg, source_index=src_idx, target_index=tgt_idx
    )
    assert err is not None
    assert err.error_code == "MAP_FIELD_NOT_FOUND"
    assert err.details == {"path": "rows/row[]/missing", "side": "source"}


def test_validate_config_rejects_unknown_target_path():
    src_idx, tgt_idx = _build_indexes()
    cfg = _direct_map_config()
    cfg["field_mappings"][0]["target_path"] = "Root/list[]/missing"
    err = DirectMapBuilder.validate_config(
        cfg, source_index=src_idx, target_index=tgt_idx
    )
    assert err is not None
    assert err.error_code == "MAP_FIELD_NOT_FOUND"
    assert err.details == {"path": "Root/list[]/missing", "side": "target"}


def test_validate_config_rejects_mapping_to_structural_target_node():
    src_idx, tgt_idx = _build_indexes()
    cfg = _direct_map_config()
    cfg["field_mappings"][0]["target_path"] = "Root/list"
    err = DirectMapBuilder.validate_config(
        cfg, source_index=src_idx, target_index=tgt_idx
    )
    assert err is not None
    assert err.error_code == "PROFILE_FIELD_NOT_MAPPABLE"


def test_validate_config_skips_path_checks_when_indexes_absent():
    """When integration_builder hasn't yet resolved profile components,
    path-existence checks are deferred. Structural checks (duplicates,
    secret scan, unsupported routes) still fire."""
    cfg = _direct_map_config()
    cfg["field_mappings"][0]["source_path"] = "rows/row[]/missing"
    err = DirectMapBuilder.validate_config(cfg)  # no indexes
    assert err is None


# ---------------------------------------------------------------------------
# validate_config — secret scanning
# ---------------------------------------------------------------------------


def test_validate_config_rejects_secret_shaped_key():
    cfg = _direct_map_config()
    cfg["api_key"] = "leak"
    err = DirectMapBuilder.validate_config(cfg)
    assert err is not None
    assert err.error_code == "PLAINTEXT_SECRET_REJECTED"


def test_validate_config_rejects_nested_secret_shaped_key():
    cfg = _direct_map_config()
    cfg["field_mappings"][0]["bearer"] = "leak"
    err = DirectMapBuilder.validate_config(cfg)
    assert err is not None
    assert err.error_code == "PLAINTEXT_SECRET_REJECTED"


# ---------------------------------------------------------------------------
# build() — XML envelope shape
# ---------------------------------------------------------------------------


def _build_map_xml(**overrides):
    src_idx, tgt_idx = _build_indexes()
    return DirectMapBuilder().build(
        source_index=src_idx,
        target_index=tgt_idx,
        **_direct_map_config(**overrides),
    )


def test_build_emits_bns_component_envelope():
    xml = _build_map_xml()
    root = ET.fromstring(xml)
    assert root.tag == "{http://api.platform.boomi.com/}Component"
    assert root.attrib["type"] == "transform.map"
    assert root.attrib["name"] == "XML to JSON"


def test_build_emits_map_with_from_to_profile_uuids():
    xml = _build_map_xml()
    root = ET.fromstring(xml)
    map_el = root.find("bns:object/Map", NS)
    assert map_el is not None
    assert map_el.attrib["fromProfile"] == "aaaaaaaa-1111-1111-1111-111111111111"
    assert map_el.attrib["toProfile"] == "bbbbbbbb-2222-2222-2222-222222222222"


def test_build_emits_one_mapping_per_field_mapping_in_order():
    xml = _build_map_xml()
    root = ET.fromstring(xml)
    mappings = root.findall("bns:object/Map/Mappings/Mapping", NS)
    assert len(mappings) == 3
    # All are profile-to-profile direct mappings.
    for m in mappings:
        assert m.attrib["fromType"] == "profile"
        assert m.attrib["toType"] == "profile"


def test_build_mapping_uses_resolved_field_index_keys():
    """The fromKey/toKey + fromKeyPath/toKeyPath must come straight from the
    profile builders' field indexes."""
    xml = _build_map_xml()
    root = ET.fromstring(xml)
    mappings = root.findall("bns:object/Map/Mappings/Mapping", NS)

    src_idx, tgt_idx = _build_indexes()
    for mapping, cfg_mapping in zip(mappings, _direct_map_config()["field_mappings"]):
        src = src_idx[cfg_mapping["source_path"]]
        tgt = tgt_idx[cfg_mapping["target_path"]]
        assert int(mapping.attrib["fromKey"]) == src["key"]
        assert int(mapping.attrib["toKey"]) == tgt["key"]
        assert mapping.attrib["fromKeyPath"] == src["key_path"]
        assert mapping.attrib["toKeyPath"] == tgt["key_path"]
        assert mapping.attrib["fromNamePath"] == src["name_path"]
        assert mapping.attrib["toNamePath"] == tgt["name_path"]


def test_build_emits_empty_functions_defaults_document_cache_joins():
    """Per live reference, the map XML always carries these blocks even
    when no advanced route is requested. Boomi UI flags incomplete maps
    without them."""
    xml = _build_map_xml()
    root = ET.fromstring(xml)
    map_el = root.find("bns:object/Map", NS)
    functions = map_el.find("Functions")
    assert functions is not None
    assert functions.attrib["optimizeExecutionOrder"] == "true"
    # Functions has no children for direct maps.
    assert len(functions) == 0
    assert map_el.find("Defaults") is not None
    assert map_el.find("DocumentCacheJoins") is not None


def test_build_xml_is_deterministic_across_runs():
    xml1 = _build_map_xml()
    xml2 = _build_map_xml()
    assert xml1 == xml2


def test_build_with_folder_path_carries_folder_full_path():
    xml = _build_map_xml(folder_path="Test/Folder")
    root = ET.fromstring(xml)
    assert root.attrib.get("folderFullPath") == "Test/Folder"


def test_build_rejects_dollar_ref_profile_id_at_build_time():
    """$ref:KEY tokens must be resolved before build() is called. Otherwise
    the emitted XML would carry the literal '$ref:' string in fromProfile/
    toProfile, which Boomi would reject."""
    src_idx, tgt_idx = _build_indexes()
    cfg = _direct_map_config()
    cfg["source_profile_id"] = "$ref:source_profile"
    with pytest.raises(BuilderValidationError) as excinfo:
        DirectMapBuilder().build(
            source_index=src_idx, target_index=tgt_idx, **cfg
        )
    assert excinfo.value.error_code == "MAP_PROFILE_INDEX_UNAVAILABLE"


def test_build_raises_when_validation_fails():
    src_idx, tgt_idx = _build_indexes()
    cfg = _direct_map_config()
    cfg["field_mappings"][0]["source_path"] = "rows/row[]/missing"
    with pytest.raises(BuilderValidationError) as excinfo:
        DirectMapBuilder().build(
            source_index=src_idx, target_index=tgt_idx, **cfg
        )
    assert excinfo.value.error_code == "MAP_FIELD_NOT_FOUND"


def test_build_db_source_to_json_target_renders_correct_namepath():
    """End-to-end DB → JSON map. The DB profile field index is plug-compatible
    with the map builder."""
    # Stub a DB-style index in the shape DatabaseReadProfileBuilder would
    # eventually expose. (Issue #26 does not extend that builder; this test
    # confirms the map builder is profile-type-agnostic.)
    db_src_idx = {
        "Statement/Fields/OrderNum": {
            "key": 6,
            "key_path": "*[@key='2']/*[@key='3']/*[@key='6']",
            "name_path": "Statement/Fields/OrderNum",
            "data_type": "character",
            "kind": "simple",
            "mappable": True,
        },
        "Statement/Fields/OrderDate": {
            "key": 7,
            "key_path": "*[@key='2']/*[@key='3']/*[@key='7']",
            "name_path": "Statement/Fields/OrderDate",
            "data_type": "datetime",
            "kind": "simple",
            "mappable": True,
        },
    }
    _, json_tgt = _build_indexes()
    cfg = _direct_map_config(
        source_profile_type="profile.db",
        source_profile_id="cccccccc-3333-3333-3333-333333333333",
        field_mappings=[
            {
                "source_path": "Statement/Fields/OrderNum",
                "target_path": "Root/list[]/key",
            },
            {
                "source_path": "Statement/Fields/OrderDate",
                "target_path": "Root/list[]/name",
            },
        ],
    )
    xml = DirectMapBuilder().build(
        source_index=db_src_idx, target_index=json_tgt, **cfg
    )
    root = ET.fromstring(xml)
    mappings = root.findall("bns:object/Map/Mappings/Mapping", NS)
    assert mappings[0].attrib["fromNamePath"] == "Statement/Fields/OrderNum"
    # Live reference (renera 77bb73d5...) shows OrderNum at key=6 in DB
    # profile, mapping to OrderID at key=3 in XML target. Same envelope shape.
    assert mappings[0].attrib["fromKey"] == "6"
