"""Issue #122 (M11.3, epic #118) — DocumentCacheBuilder unit tests.

Byte-locks the builder's emission and structurally round-trips the owned
DocumentCache subtree against the live work-account capture
(tests/fixtures/live_xml/m11/documentcache_profile_key_component.xml).
Exercises the evidence gates: unsupported/gated profile types, gated
document-property key kind, and the id-zero silent-failure rejections.
"""

from __future__ import annotations

import sys
from pathlib import Path

import pytest
import xml.etree.ElementTree as ET

_project_root = str(Path(__file__).resolve().parent.parent)
if _project_root not in sys.path:
    sys.path.insert(0, _project_root)

from src.boomi_mcp.categories.components.builders.connector_builder import (
    BuilderValidationError,
)
from src.boomi_mcp.categories.components.builders.document_cache_builder import (
    DOCUMENT_CACHE_BUILDERS,
    DocumentCacheBuilder,
    get_document_cache_builder,
)

NS = {"bns": "http://api.platform.boomi.com/"}

_LIVE_FIXTURE = (
    Path(__file__).resolve().parent
    / "fixtures"
    / "live_xml"
    / "m11"
    / "documentcache_profile_key_component.xml"
)


def _live_equivalent_config(**overrides):
    """The config whose emission mirrors the live capture's DocumentCache."""
    cfg = {
        "component_name": "New Document Cache",
        "profile_type": "profile.json",
        "profile_id": "a8471eb5-adc7-4311-95c8-f27c2ea31808",
        "enforce_single_lucene": True,
        "indexes": [
            {
                "index_id": 1,
                "index_name": "by wallID",
                "keys": [
                    {
                        "id": 2,
                        "element_key": "7",
                        "name": "ID (Root/Object/wall/Object/ID)",
                    }
                ],
            }
        ],
    }
    cfg.update(overrides)
    return cfg


def _validate(**overrides):
    return DocumentCacheBuilder.validate_config(_live_equivalent_config(**overrides))


def test_registry_and_lookup():
    assert DOCUMENT_CACHE_BUILDERS == {"documentcache": DocumentCacheBuilder}
    assert get_document_cache_builder("documentcache") is DocumentCacheBuilder
    assert get_document_cache_builder("processproperty") is None


def test_preservation_policy_owns_document_cache():
    policy = DocumentCacheBuilder.PRESERVATION_POLICY
    assert policy.component_type == "documentcache"
    assert [p.path for p in policy.owned_paths] == ["bns:object/DocumentCache"]


def test_build_byte_lock():
    xml = DocumentCacheBuilder().build(**_live_equivalent_config())
    assert xml == (
        '<bns:Component xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" '
        'xmlns:bns="http://api.platform.boomi.com/" '
        'type="documentcache" '
        'name="New Document Cache">'
        "<bns:encryptedValues/>"
        "<bns:description></bns:description>"
        "<bns:object>"
        '<DocumentCache xmlns="" '
        'enforceSingleLucene="true" '
        'profile="a8471eb5-adc7-4311-95c8-f27c2ea31808" '
        'profileType="profile.json">'
        '<CacheIndex indexId="1" indexName="by wallID">'
        '<cacheKey alias="ID (Root/Object/wall/Object/ID)" '
        'elementKey="7" '
        'id="2" '
        'name="ID (Root/Object/wall/Object/ID)" '
        'taglistKey="0" '
        'xsi:type="ProfileElementKeyConfig"/>'
        "</CacheIndex>"
        "</DocumentCache>"
        "</bns:object>"
        "</bns:Component>"
    )


def _structure(element: ET.Element):
    return (
        element.tag,
        sorted(element.attrib.items()),
        (element.text or "").strip(),
        [_structure(child) for child in element],
    )


def test_build_round_trips_against_live_capture():
    built = ET.fromstring(DocumentCacheBuilder().build(**_live_equivalent_config()))
    live = ET.fromstring(_LIVE_FIXTURE.read_text(encoding="utf-8"))
    assert _structure(built.find("bns:object/DocumentCache", NS)) == _structure(
        live.find("bns:object/DocumentCache", NS)
    )
    assert "componentId" not in built.attrib
    assert "version" not in built.attrib


def test_alias_defaults_to_name_and_can_differ():
    cfg = _live_equivalent_config()
    cfg["indexes"][0]["keys"][0]["alias"] = "short"
    xml = DocumentCacheBuilder().build(**cfg)
    key = ET.fromstring(xml).find(
        "bns:object/DocumentCache/CacheIndex/cacheKey", NS
    )
    assert key.get("alias") == "short"
    assert key.get("name") == "ID (Root/Object/wall/Object/ID)"


def test_valid_config_passes():
    assert _validate() is None


def test_component_name_required():
    err = _validate(component_name="  ")
    assert err.error_code == "DOCUMENT_CACHE_NAME_REQUIRED"


def test_unknown_top_level_key_rejected():
    err = _validate(language="groovy2")
    assert err.error_code == "DOCUMENT_CACHE_VALIDATION_FAILED"


def test_raw_subtree_key_rejected():
    err = _validate(document_cache="<DocumentCache/>")
    assert err.error_code == "DOCUMENT_CACHE_RAW_XML_UNSUPPORTED"


def test_profile_type_required_and_gated_families_named():
    err = _validate(profile_type=None)
    assert err.error_code == "DOCUMENT_CACHE_PROFILE_TYPE_UNSUPPORTED"
    err = _validate(profile_type="profile.flatfile")
    assert err.error_code == "DOCUMENT_CACHE_PROFILE_TYPE_UNSUPPORTED"
    assert "gated" in (err.hint or "")
    err = _validate(profile_type="profile.none")
    assert err.error_code == "DOCUMENT_CACHE_PROFILE_TYPE_UNSUPPORTED"
    assert "gated" in (err.hint or "")


def test_profile_id_required():
    err = _validate(profile_id="")
    assert err.error_code == "DOCUMENT_CACHE_PROFILE_REQUIRED"


def test_indexes_required_non_empty():
    err = _validate(indexes=[])
    assert err.error_code == "DOCUMENT_CACHE_INDEX_REQUIRED"


def test_index_id_zero_rejected_with_silent_failure_hint():
    cfg = _live_equivalent_config()
    cfg["indexes"][0]["index_id"] = 0
    err = DocumentCacheBuilder.validate_config(cfg)
    assert err.error_code == "DOCUMENT_CACHE_INDEX_INVALID"
    assert "silently" in (err.hint or "")


def test_duplicate_index_ids_rejected():
    cfg = _live_equivalent_config()
    cfg["indexes"].append(
        {
            "index_id": 1,
            "index_name": "dup",
            "keys": [{"id": 3, "element_key": "9", "name": "X"}],
        }
    )
    err = DocumentCacheBuilder.validate_config(cfg)
    assert err.error_code == "DOCUMENT_CACHE_INDEX_INVALID"


def test_keys_required_and_key_id_zero_rejected():
    cfg = _live_equivalent_config()
    cfg["indexes"][0]["keys"] = []
    err = DocumentCacheBuilder.validate_config(cfg)
    assert err.error_code == "DOCUMENT_CACHE_INDEX_INVALID"
    cfg = _live_equivalent_config()
    cfg["indexes"][0]["keys"][0]["id"] = 0
    err = DocumentCacheBuilder.validate_config(cfg)
    assert err.error_code == "DOCUMENT_CACHE_KEY_INVALID"
    assert "silently" in (err.hint or "")


def test_document_property_key_kind_gated():
    cfg = _live_equivalent_config()
    cfg["indexes"][0]["keys"][0]["kind"] = "document_property"
    err = DocumentCacheBuilder.validate_config(cfg)
    assert err.error_code == "DOCUMENT_CACHE_KEY_KIND_GATED"


def test_element_key_and_name_required():
    for field, value in (("element_key", " "), ("name", "")):
        cfg = _live_equivalent_config()
        cfg["indexes"][0]["keys"][0][field] = value
        err = DocumentCacheBuilder.validate_config(cfg)
        assert err.error_code == "DOCUMENT_CACHE_KEY_INVALID", field


def test_secret_shaped_key_rejected():
    err = _validate(api_key="k")
    assert err.error_code == "PLAINTEXT_SECRET_REJECTED"


def test_build_raises_on_invalid_config():
    with pytest.raises(BuilderValidationError):
        DocumentCacheBuilder().build(component_name="X", profile_type="profile.json")
