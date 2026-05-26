"""Tests for issue #26 JSONGeneratedProfileBuilder.

Anchored against live Boomi reference shapes (Slack
``954783c1-443f-4efd-9f92-ad380d078216`` flat case + work CDS
``dbe1f2b9-e238-4da0-8211-65570781cf28`` nested array case, fetched
2026-05-25). XML is verified by parsing the emitted bytes — no canned
output snapshots are committed.
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


NS = {"bns": "http://api.platform.boomi.com/"}


def _flat_config() -> Dict[str, Any]:
    return {
        "component_type": "profile.json",
        "profile_type": "json.generated",
        "component_name": "Test Flat Root",
        "root": {
            "name": "Root",
            "kind": "object",
            "children": [
                {"name": "active", "kind": "simple", "data_type": "boolean"},
                {"name": "key", "kind": "simple", "data_type": "character", "required": True},
                {"name": "count", "kind": "simple", "data_type": "number"},
                {"name": "when", "kind": "simple", "data_type": "datetime"},
            ],
        },
    }


def _nested_array_config() -> Dict[str, Any]:
    return {
        "component_type": "profile.json",
        "profile_type": "json.generated",
        "component_name": "Test Nested Array",
        "root": {
            "name": "Root",
            "kind": "object",
            "children": [
                {"name": "list", "kind": "array", "children": [
                    {"name": "active", "kind": "simple", "data_type": "boolean"},
                    {"name": "key", "kind": "simple", "data_type": "character", "required": True},
                    {"name": "name", "kind": "simple", "data_type": "character"},
                ]},
            ],
        },
    }


def _build_root(**overrides: Any) -> ET.Element:
    cfg = _flat_config()
    cfg.update(overrides)
    xml = JSONGeneratedProfileBuilder().build(**cfg)
    return ET.fromstring(xml)


# ---------------------------------------------------------------------------
# Validate_config
# ---------------------------------------------------------------------------


def test_validate_config_accepts_minimal_flat_root():
    err = JSONGeneratedProfileBuilder.validate_config(_flat_config())
    assert err is None


def test_validate_config_accepts_nested_array():
    err = JSONGeneratedProfileBuilder.validate_config(_nested_array_config())
    assert err is None


def test_validate_config_rejects_wrong_profile_type():
    cfg = _flat_config()
    cfg["profile_type"] = "database.read"
    err = JSONGeneratedProfileBuilder.validate_config(cfg)
    assert err is not None
    assert err.error_code == "UNSUPPORTED_PROFILE_GENERATION_MODE"
    assert err.field == "profile_type"


def test_validate_config_rejects_missing_component_name():
    cfg = _flat_config()
    cfg["component_name"] = "   "
    err = JSONGeneratedProfileBuilder.validate_config(cfg)
    assert err is not None
    assert err.error_code == "PROFILE_FIELD_VALIDATION_FAILED"
    assert err.field == "component_name"


def test_validate_config_rejects_missing_root():
    cfg = _flat_config()
    cfg.pop("root")
    err = JSONGeneratedProfileBuilder.validate_config(cfg)
    assert err is not None
    assert err.error_code == "PROFILE_FIELD_VALIDATION_FAILED"


def test_validate_config_propagates_unsupported_data_type():
    cfg = _flat_config()
    cfg["root"]["children"][0]["data_type"] = "blob"
    err = JSONGeneratedProfileBuilder.validate_config(cfg)
    assert err is not None
    assert err.error_code == "UNSUPPORTED_PROFILE_FIELD_TYPE"


def test_validate_config_propagates_duplicate_sibling():
    cfg = _flat_config()
    cfg["root"]["children"].append(
        {"name": "active", "kind": "simple", "data_type": "boolean"}
    )
    err = JSONGeneratedProfileBuilder.validate_config(cfg)
    assert err is not None
    assert err.error_code == "DUPLICATE_PROFILE_FIELD_PATH"


def test_validate_config_rejects_secret_shaped_key():
    cfg = _flat_config()
    cfg["password"] = "leak"
    err = JSONGeneratedProfileBuilder.validate_config(cfg)
    assert err is not None
    assert err.error_code == "PLAINTEXT_SECRET_REJECTED"


# ---------------------------------------------------------------------------
# build() — XML envelope shape
# ---------------------------------------------------------------------------


def test_build_emits_bns_component_envelope():
    root = _build_root()
    assert root.tag == "{http://api.platform.boomi.com/}Component"
    assert root.attrib["type"] == "profile.json"
    assert root.attrib["name"] == "Test Flat Root"


def test_build_emits_json_profile_with_data_elements():
    root = _build_root()
    profile = root.find("bns:object/JSONProfile", NS)
    assert profile is not None
    assert profile.attrib["strict"] == "false"
    de = profile.find("DataElements")
    assert de is not None


def test_build_root_value_uses_dense_key_1():
    root = _build_root()
    rv = root.find("bns:object/JSONProfile/DataElements/JSONRootValue", NS)
    assert rv is not None
    assert rv.attrib["key"] == "1"
    assert rv.attrib["dataType"] == "character"
    assert rv.attrib["name"] == "Root"


def test_build_root_object_wrapper_uses_dense_key_2():
    root = _build_root()
    obj = root.find("bns:object/JSONProfile/DataElements/JSONRootValue/JSONObject", NS)
    assert obj is not None
    assert obj.attrib["key"] == "2"
    assert obj.attrib["name"] == "Object"
    assert obj.attrib["isMappable"] == "false"


def test_build_emits_one_entry_per_simple_leaf_in_order():
    root = _build_root()
    entries = root.findall(
        "bns:object/JSONProfile/DataElements/JSONRootValue/JSONObject/JSONObjectEntry",
        NS,
    )
    assert [e.attrib["name"] for e in entries] == ["active", "key", "count", "when"]


def test_build_boolean_leaf_uses_empty_data_format():
    root = _build_root()
    boolean_entry = root.find(
        "bns:object/JSONProfile/DataElements/JSONRootValue/JSONObject/"
        "JSONObjectEntry[@name='active']",
        NS,
    )
    assert boolean_entry is not None
    df = boolean_entry.find("DataFormat")
    assert df is not None
    assert len(df) == 0  # empty DataFormat


def test_build_required_leaf_carries_required_attr():
    root = _build_root()
    key_entry = root.find(
        "bns:object/JSONProfile/DataElements/JSONRootValue/JSONObject/"
        "JSONObjectEntry[@name='key']",
        NS,
    )
    assert key_entry is not None
    assert key_entry.attrib.get("required") == "true"


def test_build_number_leaf_uses_profile_number_format():
    root = _build_root()
    count_entry = root.find(
        "bns:object/JSONProfile/DataElements/JSONRootValue/JSONObject/"
        "JSONObjectEntry[@name='count']",
        NS,
    )
    df = count_entry.find("DataFormat/ProfileNumberFormat")
    assert df is not None
    assert df.attrib.get("numberFormat") == ""


def test_build_datetime_leaf_uses_profile_date_format():
    root = _build_root()
    when_entry = root.find(
        "bns:object/JSONProfile/DataElements/JSONRootValue/JSONObject/"
        "JSONObjectEntry[@name='when']",
        NS,
    )
    df = when_entry.find("DataFormat/ProfileDateFormat")
    assert df is not None
    assert df.attrib["dateFormat"] == "yyyy-MM-dd"


def test_build_nested_array_emits_full_wrapper_chain():
    """Array children should produce JSONObjectEntry → JSONArray → JSONArrayElement
    → JSONObject → entries, matching the work-profile CDS live shape."""
    cfg = _nested_array_config()
    xml = JSONGeneratedProfileBuilder().build(**cfg)
    root = ET.fromstring(xml)
    list_entry = root.find(
        "bns:object/JSONProfile/DataElements/JSONRootValue/JSONObject/"
        "JSONObjectEntry[@name='list']",
        NS,
    )
    assert list_entry is not None
    array = list_entry.find("JSONArray")
    assert array is not None
    assert array.attrib["name"] == "Array"
    assert array.attrib["elementType"] == "repeating"
    array_element = array.find("JSONArrayElement")
    assert array_element is not None
    # Boomi convention: element name mirrors the parent entry name.
    assert array_element.attrib["name"] == "list"
    assert array_element.attrib["maxOccurs"] == "-1"
    assert array_element.attrib["minOccurs"] == "0"
    element_object = array_element.find("JSONObject")
    assert element_object is not None
    assert element_object.attrib["name"] == "Object"
    nested_entries = element_object.findall("JSONObjectEntry")
    assert [e.attrib["name"] for e in nested_entries] == ["active", "key", "name"]


def test_build_qualifiers_terminator_placement_matches_live_shape():
    """Live Boomi reference shape (verified 2026-05-25 against work CDS
    profile dbe1f2b9): JSONArray has NO direct <Qualifiers> child — its
    only allowed child is <JSONArrayElement>. <Qualifiers> terminates
    <JSONArrayElement> and the enclosing <JSONObjectEntry>. Misplacing it
    inside <JSONArray> trips Boomi's schema:
    `cvc-complex-type.2.4.a: Invalid content was found starting with
    element 'Qualifiers'. One of '{JSONArrayElement}' is expected.`
    """
    cfg = _nested_array_config()
    xml = JSONGeneratedProfileBuilder().build(**cfg)
    root = ET.fromstring(xml)
    # JSONArray must NOT have a direct Qualifiers child.
    array = root.find(
        "bns:object/JSONProfile/DataElements/JSONRootValue/JSONObject/"
        "JSONObjectEntry[@name='list']/JSONArray",
        NS,
    )
    direct_qualifiers = array.find("Qualifiers")
    assert direct_qualifiers is None, (
        "JSONArray must NOT carry a direct <Qualifiers> child — Boomi schema "
        "rejects this with cvc-complex-type.2.4.a (only <JSONArrayElement> "
        "is allowed inside <JSONArray>)."
    )
    # JSONArrayElement MUST have a Qualifiers terminator.
    array_element = array.find("JSONArrayElement")
    assert array_element.find("Qualifiers") is not None, (
        "JSONArrayElement must end with <Qualifiers><QualifierList/></Qualifiers>"
    )
    # JSONObjectEntry wrapping an array MUST have a Qualifiers terminator
    # after </JSONArray>.
    list_entry = root.find(
        "bns:object/JSONProfile/DataElements/JSONRootValue/JSONObject/"
        "JSONObjectEntry[@name='list']",
        NS,
    )
    assert list_entry.find("Qualifiers") is not None, (
        "JSONObjectEntry wrapping a JSONArray must end with <Qualifiers>"
    )
    # JSONRootValue MUST have a Qualifiers terminator after </JSONObject>.
    root_value = root.find(
        "bns:object/JSONProfile/DataElements/JSONRootValue", NS
    )
    assert root_value.find("Qualifiers") is not None


def test_build_xml_is_deterministic_across_runs():
    cfg = _nested_array_config()
    xml1 = JSONGeneratedProfileBuilder().build(**cfg)
    xml2 = JSONGeneratedProfileBuilder().build(**cfg)
    assert xml1 == xml2


def test_build_emits_taglists_terminator():
    root = _build_root()
    profile = root.find("bns:object/JSONProfile", NS)
    assert profile.find("tagLists") is not None


def test_build_escapes_xml_special_chars_in_name():
    cfg = _flat_config()
    cfg["component_name"] = 'Test & "Quote" <Tag>'
    xml = JSONGeneratedProfileBuilder().build(**cfg)
    # Must parse successfully (raw special chars in attribute value would
    # break parsing).
    root = ET.fromstring(xml)
    assert root.attrib["name"] == 'Test & "Quote" <Tag>'


def test_build_with_folder_path_carries_folder_full_path():
    cfg = _flat_config()
    cfg["folder_path"] = "Test/Folder"
    xml = JSONGeneratedProfileBuilder().build(**cfg)
    root = ET.fromstring(xml)
    assert root.attrib.get("folderFullPath") == "Test/Folder"


# ---------------------------------------------------------------------------
# build_field_index — keys, key_paths, name_paths
# ---------------------------------------------------------------------------


def test_field_index_covers_root_and_each_leaf():
    idx = JSONGeneratedProfileBuilder.build_field_index(_flat_config())
    assert set(idx.keys()) == {"Root", "Root/active", "Root/key", "Root/count", "Root/when"}


def test_field_index_keys_are_dense_pre_order():
    idx = JSONGeneratedProfileBuilder.build_field_index(_flat_config())
    # Root=1, Object wrapper=2, active=3, key=4, count=5, when=6.
    assert idx["Root"]["key"] == 1
    assert idx["Root/active"]["key"] == 3
    assert idx["Root/key"]["key"] == 4
    assert idx["Root/count"]["key"] == 5
    assert idx["Root/when"]["key"] == 6


def test_field_index_name_path_uses_object_wrapper():
    idx = JSONGeneratedProfileBuilder.build_field_index(_flat_config())
    assert idx["Root/active"]["name_path"] == "Root/Object/active"
    assert idx["Root/key"]["name_path"] == "Root/Object/key"


def test_field_index_key_path_xpath_format():
    idx = JSONGeneratedProfileBuilder.build_field_index(_flat_config())
    assert idx["Root/active"]["key_path"] == "*[@key='1']/*[@key='2']/*[@key='3']"


def test_field_index_nested_array_name_path_mirrors_live_cds_shape():
    """For `Root → list[array] → {active, key, name}` the live work-CDS
    name_path for `Root/list[]/key` is
    `Root/Object/list/Array/list/Object/key`."""
    idx = JSONGeneratedProfileBuilder.build_field_index(_nested_array_config())
    assert idx["Root/list[]/key"]["name_path"] == "Root/Object/list/Array/list/Object/key"


def test_field_index_nested_array_key_path_descends_through_all_wrappers():
    idx = JSONGeneratedProfileBuilder.build_field_index(_nested_array_config())
    # Root=1, Object=2, list=3, Array=4, array element list=5, element Object=6, active=7, key=8, name=9
    entry = idx["Root/list[]/key"]
    assert entry["key"] == 8
    assert entry["key_path"] == (
        "*[@key='1']/*[@key='2']/*[@key='3']/*[@key='4']/*[@key='5']/*[@key='6']/*[@key='8']"
    )


def test_field_index_marks_structural_nodes_as_non_mappable():
    idx = JSONGeneratedProfileBuilder.build_field_index(_nested_array_config())
    assert idx["Root"]["mappable"] is False
    assert idx["Root/list"]["mappable"] is False
    assert idx["Root/list[]/key"]["mappable"] is True


def test_field_index_carries_data_type_for_leaves():
    idx = JSONGeneratedProfileBuilder.build_field_index(_flat_config())
    assert idx["Root/active"]["data_type"] == "boolean"
    assert idx["Root/key"]["data_type"] == "character"
    assert idx["Root/count"]["data_type"] == "number"
    assert idx["Root/when"]["data_type"] == "datetime"


def test_field_index_xml_keys_match_emitted_xml():
    """Defense-in-depth: the keys in the index must be exactly the keys
    emitted in the XML. Otherwise the map builder's <Mapping fromKey/toKey>
    would dangle."""
    cfg = _nested_array_config()
    idx = JSONGeneratedProfileBuilder.build_field_index(cfg)
    xml = JSONGeneratedProfileBuilder().build(**cfg)
    root = ET.fromstring(xml)
    # Walk all elements with a `key` attribute and confirm the index's keys
    # are a subset (some structural wrappers are not in the index).
    emitted_keys = {
        int(e.attrib["key"])
        for e in root.iter()
        if "key" in e.attrib
    }
    index_keys = {entry["key"] for entry in idx.values()}
    assert index_keys.issubset(emitted_keys)


# ---------------------------------------------------------------------------
# build() error paths
# ---------------------------------------------------------------------------


def test_build_raises_for_invalid_config():
    with pytest.raises(BuilderValidationError) as excinfo:
        JSONGeneratedProfileBuilder().build(
            **{
                "component_type": "profile.json",
                "profile_type": "json.generated",
                "component_name": "X",
                "root": {
                    "name": "Root",
                    "kind": "object",
                    "children": [
                        {"name": "x", "kind": "simple", "data_type": "blob"},
                    ],
                },
            }
        )
    assert excinfo.value.error_code == "UNSUPPORTED_PROFILE_FIELD_TYPE"
