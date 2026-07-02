"""M11.0 census fixture shape tests (issue #119 / #118).

Parses every sanitized live capture under tests/fixtures/live_xml/m11/ and
asserts the structural invariants later M11 children byte-lock against:
Document Cache component index/key shape, Process Property component
DefinedProcessProperties shape, Set Properties DDP/DPP propertyId prefixes,
cache step shapes, and the populated DocumentCacheJoins map shape.

Sources and gate outcomes are recorded in the local census doc
(.codex/plans/issue-119-cache-property-census.md, gitignored).
"""

from __future__ import annotations

import re
import sys
from pathlib import Path

import xml.etree.ElementTree as ET

_project_root = str(Path(__file__).resolve().parent.parent)
if _project_root not in sys.path:
    sys.path.insert(0, _project_root)

FIXTURE_DIR = Path(__file__).resolve().parent / "fixtures" / "live_xml" / "m11"

BNS = "{http://api.platform.boomi.com/}"

_UUID_RE = re.compile(
    r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$"
)


def _load(name: str) -> ET.Element:
    path = FIXTURE_DIR / name
    assert path.is_file(), f"missing fixture {path}"
    return ET.fromstring(path.read_text(encoding="utf-8"))


def _component_object(root: ET.Element) -> ET.Element:
    obj = root.find(f"{BNS}object")
    assert obj is not None, "fixture lacks bns:object"
    return obj


def test_fixture_dir_has_expected_captures():
    names = sorted(p.name for p in FIXTURE_DIR.glob("*.xml"))
    assert names == [
        "documentcache_profile_key_component.xml",
        "map_document_cache_joins.xml",
        "process_cache_branch_load_remove.xml",
        "process_doccacheretrieve_loadalldoc_variant.xml",
        "process_dpp_profile_decision_flow.xml",
        "process_setproperties_ddp_dpp_current_crossref.xml",
        "processproperty_allowed_values.xml",
        "processproperty_minimal.xml",
        "processproperty_persisted.xml",
    ]


def test_no_fixture_contains_scrubbed_vendor_string():
    # The scrubbed vendor name must never reappear in committed fixtures.
    scrubbed = "int" + "app"
    for path in FIXTURE_DIR.glob("*.xml"):
        text = path.read_text(encoding="utf-8").lower()
        assert scrubbed not in text, f"{path.name} contains scrubbed vendor string"


def test_documentcache_component_shape():
    root = _load("documentcache_profile_key_component.xml")
    assert root.get("type") == "documentcache"
    cache = _component_object(root).find("DocumentCache")
    assert cache is not None
    assert cache.get("enforceSingleLucene") == "true"
    assert cache.get("profileType") == "profile.json"
    assert _UUID_RE.match(cache.get("profile", ""))
    indexes = cache.findall("CacheIndex")
    assert indexes, "profiled cache must declare at least one CacheIndex"
    for index in indexes:
        assert int(index.get("indexId")) > 0
        assert index.get("indexName")
        keys = index.findall("cacheKey")
        assert keys, "CacheIndex must declare at least one cacheKey"
        for key in keys:
            assert int(key.get("id")) > 0
            xsi_type = key.get("{http://www.w3.org/2001/XMLSchema-instance}type")
            assert xsi_type == "ProfileElementKeyConfig"
            assert key.get("elementKey")
            assert key.get("taglistKey") == "0"


def _assert_processproperty_shape(root: ET.Element) -> list:
    assert root.get("type") == "processproperty"
    container = _component_object(root).find("DefinedProcessProperties")
    assert container is not None
    props = container.findall("definedProcessProperty")
    assert props, "component must define at least one property"
    for prop in props:
        assert _UUID_RE.match(prop.get("key", ""))
        children = [child.tag for child in prop]
        assert children == [
            "helpText",
            "label",
            "type",
            "defaultValue",
            "allowedValues",
            "persisted",
        ]
        assert prop.find("label").text
        assert prop.find("type").text in {"string", "number", "boolean"}
        assert prop.find("persisted").text in {"true", "false"}
    return props


def test_processproperty_minimal_shape():
    props = _assert_processproperty_shape(_load("processproperty_minimal.xml"))
    assert len(props) == 1
    assert props[0].find("label").text == "Example Property"
    assert props[0].find("persisted").text == "false"


def test_processproperty_allowed_values_shape():
    props = _assert_processproperty_shape(
        _load("processproperty_allowed_values.xml")
    )
    types = {p.find("type").text for p in props}
    assert types == {"string", "number", "boolean"}
    value_sets = [
        p for p in props if p.find("allowedValues").findall("allowedValueSet")
    ]
    assert value_sets, "capture must include populated allowedValueSet entries"
    for entry in value_sets[0].find("allowedValues").findall("allowedValueSet"):
        assert entry.get("label") is not None
        assert entry.get("value") is not None


def test_processproperty_persisted_shape():
    props = _assert_processproperty_shape(_load("processproperty_persisted.xml"))
    assert {p.find("persisted").text for p in props} == {"true"}
    assert {p.find("type").text for p in props} == {"string"}


def _shapes_by_type(root: ET.Element) -> dict:
    shapes: dict = {}
    for shape in _component_object(root).iter("shape"):
        shapes.setdefault(shape.get("shapetype"), []).append(shape)
    return shapes


def test_setproperties_ddp_dpp_prefixes_and_sources():
    root = _load("process_setproperties_ddp_dpp_current_crossref.xml")
    shapes = _shapes_by_type(root)
    setprops = shapes.get("documentproperties")
    assert setprops and len(setprops) == 2
    prop_ids = [
        prop.get("propertyId")
        for shape in setprops
        for prop in shape.iter("documentproperty")
    ]
    assert any(pid.startswith("dynamicdocument.") for pid in prop_ids)
    assert any(pid.startswith("process.") for pid in prop_ids)
    for shape in setprops:
        for prop in shape.iter("documentproperty"):
            assert prop.get("persist") in {"true", "false"}
            name = prop.get("name")
            pid = prop.get("propertyId")
            if pid.startswith("dynamicdocument."):
                assert name == (
                    "Dynamic Document Property - "
                    + pid[len("dynamicdocument."):]
                )
            else:
                assert name == (
                    "Dynamic Process Property - " + pid[len("process."):]
                )
    value_types = {
        pv.get("valueType") for pv in root.iter("parametervalue")
    }
    assert "current" in value_types
    assert "crossref" in value_types
    loads = shapes.get("doccacheload")
    assert loads and _UUID_RE.match(
        loads[0].find("configuration/doccacheload").get("docCache", "")
    )


def test_cache_branch_load_remove_shapes():
    root = _load("process_cache_branch_load_remove.xml")
    shapes = _shapes_by_type(root)
    branch_cfg = shapes["branch"][0].find("configuration/branch")
    assert branch_cfg.get("numBranches") == "5"
    removes = shapes.get("doccacheremove")
    assert removes and len(removes) == 5
    for shape in removes:
        cfg = shape.find("configuration/doccacheremove")
        assert _UUID_RE.match(cfg.get("docCache", ""))
        assert cfg.get("removeAllDocuments") == "true"


def test_doccacheretrieve_loadalldoc_attribute_variant():
    """Live variant fact: work-account retrieves use loadAllDoc +
    emptyCacheBehavior spellings (M10 goldens use loadAllDocuments +
    onNoDocuments) — both exist in the wild; new emission must be
    round-trip-verified before choosing a spelling."""
    root = _load("process_doccacheretrieve_loadalldoc_variant.xml")
    shapes = _shapes_by_type(root)
    retrieve = shapes["doccacheretrieve"][0].find(
        "configuration/doccacheretrieve"
    )
    assert _UUID_RE.match(retrieve.get("docCache", ""))
    assert retrieve.get("loadAllDoc") == "true"
    assert retrieve.get("emptyCacheBehavior") == "stopprocess"
    key_values = retrieve.find("cacheKeyValues")
    assert key_values is not None and len(list(key_values)) == 0
    value_types = {pv.get("valueType") for pv in root.iter("parametervalue")}
    assert "execution" in value_types


def test_dpp_profile_source_and_decision_reads():
    root = _load("process_dpp_profile_decision_flow.xml")
    profile_sources = [
        pv
        for pv in root.iter("parametervalue")
        if pv.get("valueType") == "profile"
    ]
    assert profile_sources
    element = profile_sources[0].find("profileelement")
    assert element.get("profileType") == "profile.json"
    assert _UUID_RE.match(element.get("profileId", ""))
    decision_dpp_reads = [
        pp
        for pp in root.iter("processparameter")
        if pp.get("processproperty", "").startswith("DPP_")
    ]
    assert decision_dpp_reads
    assert all(
        pp.get("processpropertydefaultvalue") is not None
        for pp in decision_dpp_reads
    )
    track_binds = [
        tp
        for tp in root.iter("trackparameter")
        if tp.get("propertyId", "").startswith("dynamicdocument.")
    ]
    assert track_binds, "connector params must bind DDPs via track source"
    overrides = root.find(
        f"{BNS}processOverrides/Overrides/DefinedProcessPropertyOverrides"
    )
    assert overrides is not None
    component = overrides.find("OverrideableDefinedProcessPropertyComponent")
    assert _UUID_RE.match(component.get("componentId", ""))
    value = component.find("OverrideableDefinedProcessPropertyValue")
    assert _UUID_RE.match(value.get("key", ""))


def test_map_document_cache_joins_shape():
    root = _load("map_document_cache_joins.xml")
    assert root.get("type") == "transform.map"
    map_el = _component_object(root).find("Map")
    joins = map_el.find("DocumentCacheJoins")
    assert joins is not None
    join = joins.find("DocumentCacheJoin")
    assert join is not None
    assert int(join.get("cacheIndex")) > 0
    assert _UUID_RE.match(join.get("docCache", ""))
    join_id = join.get("docCacheJoinId")
    assert join_id and int(join_id) > 0
    assert join.find("srcParentKey").get("key")
    assert join.find("srcParentKey").get("tagListKey") == "0"
    key_value = join.find("CacheKeyJoinValues/CacheKeyJoinValue")
    assert int(key_value.get("cacheKeyId")) > 0
    assert key_value.get("cacheKeyName")
    assert key_value.find("srcLinkKey").get("key")
    cache_mappings = [
        m
        for m in map_el.find("Mappings").findall("Mapping")
        if m.get("fromCacheJoinKey") is not None
    ]
    assert cache_mappings, "join must be referenced by Mapping@fromCacheJoinKey"
    for mapping in cache_mappings:
        assert mapping.get("fromCacheJoinKey") == join_id
        assert "*[@key='2147483639']" in mapping.get("fromKeyPath", "")
