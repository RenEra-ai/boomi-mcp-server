"""Tests for issue #26 XMLGeneratedProfileBuilder (element-only).

Anchored against live Boomi reference shapes (Shipping Order
``74f66e9e-fd30-470c-970e-397ee29fed73`` and work CDS
``9570b55c-993c-4715-9bc5-3d8d8353ff1e``, fetched 2026-05-25).
"""

from __future__ import annotations

from typing import Any, Dict
from xml.etree import ElementTree as ET

import pytest

from boomi_mcp.categories.components.builders.connector_builder import (
    BuilderValidationError,
)
from boomi_mcp.categories.components.builders.xml_profile_builder import (
    XMLGeneratedProfileBuilder,
)


NS = {"bns": "http://api.platform.boomi.com/"}


def _config() -> Dict[str, Any]:
    return {
        "component_type": "profile.xml",
        "profile_type": "xml.generated",
        "component_name": "Test XML",
        "root": {
            "name": "rows",
            "kind": "element",
            "min_occurs": 1,
            "max_occurs": 1,
            "children": [
                {"name": "row", "kind": "element", "max_occurs": -1, "children": [
                    {"name": "key", "kind": "element", "data_type": "character"},
                    {"name": "name", "kind": "element", "data_type": "character"},
                    {"name": "when", "kind": "element", "data_type": "datetime"},
                    {"name": "count", "kind": "element", "data_type": "number"},
                    {"name": "active", "kind": "element", "data_type": "boolean"},
                ]},
            ],
        },
    }


def _build_root(**overrides: Any) -> ET.Element:
    cfg = _config()
    cfg.update(overrides)
    xml = XMLGeneratedProfileBuilder().build(**cfg)
    return ET.fromstring(xml)


# ---------------------------------------------------------------------------
# validate_config
# ---------------------------------------------------------------------------


def test_validate_config_accepts_nested_element_tree():
    err = XMLGeneratedProfileBuilder.validate_config(_config())
    assert err is None


def test_validate_config_rejects_wrong_profile_type():
    cfg = _config()
    cfg["profile_type"] = "json.generated"
    err = XMLGeneratedProfileBuilder.validate_config(cfg)
    assert err is not None
    assert err.error_code == "UNSUPPORTED_PROFILE_GENERATION_MODE"


def test_validate_config_rejects_missing_component_name():
    cfg = _config()
    cfg["component_name"] = ""
    err = XMLGeneratedProfileBuilder.validate_config(cfg)
    assert err is not None
    assert err.error_code == "PROFILE_FIELD_VALIDATION_FAILED"


def test_validate_config_rejects_attributes_feature():
    cfg = _config()
    cfg["root"]["children"][0]["children"][0]["attributes"] = [{"name": "id"}]
    err = XMLGeneratedProfileBuilder.validate_config(cfg)
    assert err is not None
    assert err.error_code == "UNSUPPORTED_XML_PROFILE_FEATURE"
    assert "attributes" in (err.field or "")


def test_validate_config_rejects_namespaces_feature():
    cfg = _config()
    cfg["root"]["namespace_uri"] = "http://example.com/ns"
    err = XMLGeneratedProfileBuilder.validate_config(cfg)
    assert err is not None
    assert err.error_code == "UNSUPPORTED_XML_PROFILE_FEATURE"
    assert "namespace" in (err.field or "")


def test_validate_config_rejects_xsd_feature():
    cfg = _config()
    cfg["xsd"] = "<schema>...</schema>"
    err = XMLGeneratedProfileBuilder.validate_config(cfg)
    assert err is not None
    assert err.error_code == "UNSUPPORTED_XML_PROFILE_FEATURE"


def test_validate_config_propagates_unsupported_data_type():
    cfg = _config()
    cfg["root"]["children"][0]["children"][0]["data_type"] = "blob"
    err = XMLGeneratedProfileBuilder.validate_config(cfg)
    assert err is not None
    assert err.error_code == "UNSUPPORTED_PROFILE_FIELD_TYPE"


def test_validate_config_propagates_duplicate_sibling():
    cfg = _config()
    cfg["root"]["children"][0]["children"].append(
        {"name": "key", "kind": "element", "data_type": "character"}
    )
    err = XMLGeneratedProfileBuilder.validate_config(cfg)
    assert err is not None
    assert err.error_code == "DUPLICATE_PROFILE_FIELD_PATH"


def test_validate_config_rejects_simple_kind_in_xml_tree():
    cfg = _config()
    cfg["root"]["children"][0]["children"][0]["kind"] = "simple"
    err = XMLGeneratedProfileBuilder.validate_config(cfg)
    assert err is not None
    # profile_from_xml_schema raises PROFILE_GENERATION_VALIDATION_FAILED
    # for non-element kinds.
    assert err.error_code in (
        "PROFILE_GENERATION_VALIDATION_FAILED",
        "PROFILE_FIELD_VALIDATION_FAILED",
    )


def test_validate_config_rejects_secret_shaped_key():
    cfg = _config()
    cfg["api_key"] = "leak"
    err = XMLGeneratedProfileBuilder.validate_config(cfg)
    assert err is not None
    assert err.error_code == "PLAINTEXT_SECRET_REJECTED"


# ---------------------------------------------------------------------------
# build() — XML envelope shape
# ---------------------------------------------------------------------------


def test_build_emits_bns_component_envelope():
    root = _build_root()
    assert root.tag == "{http://api.platform.boomi.com/}Component"
    assert root.attrib["type"] == "profile.xml"


def test_build_emits_xml_profile_with_model_version():
    root = _build_root()
    profile = root.find("bns:object/XMLProfile", NS)
    assert profile is not None
    assert profile.attrib["modelVersion"] == "2"
    assert profile.attrib["strict"] == "true"


def test_build_emits_profile_properties_block():
    root = _build_root()
    pp = root.find("bns:object/XMLProfile/ProfileProperties", NS)
    assert pp is not None
    assert pp.find("XMLGeneralInfo") is not None
    opts = pp.find("XMLOptions")
    assert opts is not None
    assert opts.attrib["encoding"] == "utf8"


def test_build_root_element_has_is_root_attr_and_key_1():
    root = _build_root()
    root_el = root.find("bns:object/XMLProfile/DataElements/XMLElement", NS)
    assert root_el is not None
    assert root_el.attrib["isRoot"] == "true"
    assert root_el.attrib["key"] == "1"


def test_build_nested_element_does_not_carry_is_root_attr():
    root = _build_root()
    row_el = root.find(
        "bns:object/XMLProfile/DataElements/XMLElement/XMLElement[@name='row']",
        NS,
    )
    assert row_el is not None
    assert "isRoot" not in row_el.attrib


def test_build_emits_namespaces_block():
    root = _build_root()
    ns = root.find("bns:object/XMLProfile/Namespaces/XMLNamespace", NS)
    assert ns is not None
    assert ns.attrib["name"] == "Empty Namespace"
    assert ns.attrib["key"] == "-1"


def test_build_emits_taglists_terminator():
    root = _build_root()
    profile = root.find("bns:object/XMLProfile", NS)
    assert profile.find("tagLists") is not None


def test_build_emits_max_occurs_minus_one_for_repeating_row():
    root = _build_root()
    row_el = root.find(
        "bns:object/XMLProfile/DataElements/XMLElement/XMLElement[@name='row']",
        NS,
    )
    assert row_el.attrib["maxOccurs"] == "-1"
    assert row_el.attrib["minOccurs"] == "0"


def test_build_leaf_data_format_tags_match_data_types():
    root = _build_root()
    # character → ProfileCharacterFormat
    key_el = root.find(
        "bns:object/XMLProfile/DataElements/XMLElement/XMLElement/XMLElement[@name='key']",
        NS,
    )
    assert key_el.find("DataFormat/ProfileCharacterFormat") is not None
    # number → ProfileNumberFormat
    count_el = root.find(
        "bns:object/XMLProfile/DataElements/XMLElement/XMLElement/XMLElement[@name='count']",
        NS,
    )
    assert count_el.find("DataFormat/ProfileNumberFormat") is not None
    # datetime → ProfileDateFormat
    when_el = root.find(
        "bns:object/XMLProfile/DataElements/XMLElement/XMLElement/XMLElement[@name='when']",
        NS,
    )
    df = when_el.find("DataFormat/ProfileDateFormat")
    assert df is not None
    assert df.attrib["dateFormat"] == "yyyy-MM-dd"
    # boolean → ProfileCharacterFormat (Boomi XML profiles store boolean as
    # character format).
    active_el = root.find(
        "bns:object/XMLProfile/DataElements/XMLElement/XMLElement/XMLElement[@name='active']",
        NS,
    )
    assert active_el.find("DataFormat/ProfileCharacterFormat") is not None
    assert active_el.attrib["dataType"] == "boolean"


def test_build_every_element_has_qualifier_list():
    root = _build_root()
    elements = root.findall(".//XMLElement")
    for el in elements:
        # The element either contains a QualifierList directly, OR has a
        # nested structural shape where QualifierList sits among children.
        ql = el.find("QualifierList")
        assert ql is not None, f"missing QualifierList on element key={el.attrib['key']}"


def test_build_xml_is_deterministic_across_runs():
    cfg = _config()
    xml1 = XMLGeneratedProfileBuilder().build(**cfg)
    xml2 = XMLGeneratedProfileBuilder().build(**cfg)
    assert xml1 == xml2


def test_build_with_folder_path_carries_folder_full_path():
    cfg = _config()
    cfg["folder_path"] = "Test/Folder"
    xml = XMLGeneratedProfileBuilder().build(**cfg)
    root = ET.fromstring(xml)
    assert root.attrib.get("folderFullPath") == "Test/Folder"


# ---------------------------------------------------------------------------
# build_field_index — keys, key_paths, name_paths
# ---------------------------------------------------------------------------


def test_field_index_keys_are_dense_pre_order():
    idx = XMLGeneratedProfileBuilder.build_field_index(_config())
    assert idx["rows"]["key"] == 1
    assert idx["rows/row"]["key"] == 2
    assert idx["rows/row[]/key"]["key"] == 3


def test_field_index_repeating_segment_uses_brackets():
    idx = XMLGeneratedProfileBuilder.build_field_index(_config())
    # rows has max_occurs=1 (no [] on children), row has max_occurs=-1 ([] on children).
    assert "rows/row" in idx  # not rows[]/row
    assert "rows/row[]/key" in idx


def test_field_index_name_path_matches_logical_segments():
    idx = XMLGeneratedProfileBuilder.build_field_index(_config())
    # XML profile namePaths don't have synthetic wrappers like JSON's "Object".
    assert idx["rows/row[]/key"]["name_path"] == "rows/row/key"


def test_field_index_key_path_xpath_format():
    idx = XMLGeneratedProfileBuilder.build_field_index(_config())
    assert idx["rows/row[]/key"]["key_path"] == "*[@key='1']/*[@key='2']/*[@key='3']"


def test_field_index_marks_structural_nodes_as_non_mappable():
    idx = XMLGeneratedProfileBuilder.build_field_index(_config())
    assert idx["rows"]["mappable"] is False
    assert idx["rows/row"]["mappable"] is False
    assert idx["rows/row[]/key"]["mappable"] is True


def test_field_index_xml_keys_match_emitted_xml():
    cfg = _config()
    idx = XMLGeneratedProfileBuilder.build_field_index(cfg)
    xml = XMLGeneratedProfileBuilder().build(**cfg)
    root = ET.fromstring(xml)
    emitted_keys = {
        int(e.attrib["key"])
        for e in root.iter()
        if "key" in e.attrib and e.tag == "XMLElement"
    }
    index_keys = {entry["key"] for entry in idx.values()}
    assert index_keys == emitted_keys


def test_build_raises_for_invalid_config():
    with pytest.raises(BuilderValidationError) as excinfo:
        XMLGeneratedProfileBuilder().build(
            **{
                "component_type": "profile.xml",
                "profile_type": "xml.generated",
                "component_name": "X",
                "root": {
                    "name": "Root",
                    "kind": "element",
                    "children": [
                        {"name": "x", "kind": "element", "data_type": "blob"},
                    ],
                },
            }
        )
    assert excinfo.value.error_code == "UNSUPPORTED_PROFILE_FIELD_TYPE"


# ============================================================================
# Issue #45 — Component XML update preservation
# ============================================================================


def test_xml_profile_preservation_policy_attached():
    policy = XMLGeneratedProfileBuilder.PRESERVATION_POLICY
    assert policy.component_type == "profile.xml"
    paths = {op.path for op in policy.owned_paths}
    assert paths == {"bns:object/XMLProfile/DataElements"}


def test_xml_profile_update_preserves_namespaces_and_taglists():
    """XMLProfile siblings such as `Namespaces` and `tagLists` must
    survive a structured update — only `DataElements` is owned."""
    from boomi_mcp.categories.components.component_update_preservation import (
        merge_for_update,
    )

    desired_xml = XMLGeneratedProfileBuilder().build(**_config())
    current_xml = XMLGeneratedProfileBuilder().build(**_config())
    # Replace the empty default namespace with a customized one and
    # inject a future section
    current_xml = current_xml.replace(
        '<Namespaces><XMLNamespace key="-1" name="Empty Namespace"><Types/></XMLNamespace></Namespaces>',
        (
            '<Namespaces>'
            '<XMLNamespace key="-1" name="Empty Namespace"><Types/></XMLNamespace>'
            '<XMLNamespace key="42" name="custom" uri="urn:user:ns"><Types/></XMLNamespace>'
            '</Namespaces>'
        ),
    )
    current_xml = current_xml.replace(
        "<tagLists/>",
        '<tagLists><tagList key="future" name="user-added"/></tagLists>',
    )

    merged = merge_for_update(
        current_xml, desired_xml, XMLGeneratedProfileBuilder.PRESERVATION_POLICY
    )
    root = ET.fromstring(merged)
    profile = root.find("bns:object/XMLProfile", NS)
    namespaces = profile.findall("Namespaces/XMLNamespace")
    assert any(ns.attrib.get("name") == "custom" for ns in namespaces)
    tag_list = profile.find("tagLists/tagList")
    assert tag_list is not None
    assert tag_list.attrib["key"] == "future"
