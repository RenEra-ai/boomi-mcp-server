"""Issue #122 (M11.3, epic #118) — authored map-level DocumentCacheJoins.

The wire shape was live-captured in the #119 census (Outcome A):
tests/fixtures/live_xml/m11/map_document_cache_joins.xml. The rendering test
byte-locks the emitted <DocumentCacheJoins> section against that capture's
section verbatim; validation tests cover the structural contract plus the
$ref/depends_on and in-spec cache index/key cross-checks.
"""

from __future__ import annotations

import re
import sys
from pathlib import Path
from types import SimpleNamespace

import pytest

_project_root = str(Path(__file__).resolve().parent.parent)
if _project_root not in sys.path:
    sys.path.insert(0, _project_root)

from boomi_mcp.categories.components.builders.connector_builder import (
    BuilderValidationError,
)
from boomi_mcp.categories.components.builders.map_builder import (
    _render_document_cache_joins,
    validate_document_cache_joins_structure,
)
from boomi_mcp.categories.components.builders.transform_map_validation import (
    validate_transform_map,
)

_LIVE_FIXTURE = (
    Path(__file__).resolve().parent
    / "fixtures"
    / "live_xml"
    / "m11"
    / "map_document_cache_joins.xml"
)

# The join declared by the live capture, expressed as authored config.
_LIVE_JOIN = {
    "document_cache_id": "b1185483-2fa3-43ce-9ab7-6a3891667381",
    "cache_index": 1,
    "join_id": 8,
    "src_parent_key": "2",
    "key_values": [
        {
            "cache_key_id": 2,
            "cache_key_name": "wallID (Root/wall/wallID)",
            "src_link_key": "3",
        }
    ],
}


def _live_joins_section() -> str:
    text = _LIVE_FIXTURE.read_text(encoding="utf-8")
    match = re.search(r"<DocumentCacheJoins>.*</DocumentCacheJoins>", text, re.S)
    assert match, "live fixture must carry a populated DocumentCacheJoins"
    return match.group(0)


def test_rendered_section_byte_matches_live_capture():
    assert _render_document_cache_joins([_LIVE_JOIN]) == _live_joins_section()


def test_absent_or_empty_config_keeps_empty_element():
    assert _render_document_cache_joins(None) == "<DocumentCacheJoins/>"
    assert _render_document_cache_joins([]) == "<DocumentCacheJoins/>"


def test_render_raises_on_malformed_entry():
    with pytest.raises(BuilderValidationError):
        _render_document_cache_joins([{"document_cache_id": "x"}])


def test_structure_validation_matrix():
    ok = validate_document_cache_joins_structure
    assert ok(None) is None
    assert ok([_LIVE_JOIN]) is None
    assert ok("nope").error_code == "MAP_DOCUMENT_CACHE_JOINS_INVALID"
    assert ok([{}]).error_code == "MAP_DOCUMENT_CACHE_JOINS_INVALID"
    bad = dict(_LIVE_JOIN, cache_index=0)
    assert ok([bad]).field.endswith("cache_index")
    bad = dict(_LIVE_JOIN, join_id=0)
    assert ok([bad]).field.endswith("join_id")
    assert ok([_LIVE_JOIN, dict(_LIVE_JOIN)]).field.endswith("join_id")  # dup id
    bad = dict(_LIVE_JOIN, key_values=[])
    assert ok([bad]).field.endswith("key_values")
    bad = dict(_LIVE_JOIN, key_values=[{"cache_key_id": 0, "cache_key_name": "n", "src_link_key": "3"}])
    assert ok([bad]).field.endswith("cache_key_id")
    bad = dict(_LIVE_JOIN, extra_key=1)
    assert ok([bad]).error_code == "MAP_DOCUMENT_CACHE_JOINS_INVALID"


# --- plan-time cross-checks (validate_transform_map) ------------------------


def _comp(comp_type, config=None, name="C"):
    return SimpleNamespace(type=comp_type, name=name, config=config or {})


def _json_profile_config(name):
    return {
        "component_type": "profile.json",
        "profile_type": "json.generated",
        "component_name": name,
        "root": {
            "name": "Root",
            "kind": "object",
            "children": [
                {"name": "id", "kind": "simple", "data_type": "character"},
            ],
        },
    }


def _cache_config():
    return {
        "component_type": "documentcache",
        "component_name": "Lookup Cache",
        "profile_type": "profile.json",
        "profile_id": "$ref:src",
        "indexes": [
            {
                "index_id": 1,
                "index_name": "by id",
                "keys": [{"id": 2, "element_key": "3", "name": "id (Root/id)"}],
            }
        ],
    }


def _map_config(join, **overrides):
    cfg = {
        "component_type": "transform.map",
        "map_type": "direct",
        "component_name": "Join Map",
        "source_profile_id": "$ref:src",
        "source_profile_type": "profile.json",
        "target_profile_id": "$ref:tgt",
        "target_profile_type": "profile.json",
        "field_mappings": [{"source_path": "Root/id", "target_path": "Root/id"}],
        "document_cache_joins": [join],
    }
    cfg.update(overrides)
    return cfg


def _components():
    return {
        "src": _comp("profile.json", _json_profile_config("Src")),
        "tgt": _comp("profile.json", _json_profile_config("Tgt")),
        "cache": _comp("documentcache", _cache_config()),
    }


def _ref_join(**overrides):
    join = dict(_LIVE_JOIN, document_cache_id="$ref:cache", cache_index=1)
    join["key_values"] = [
        {"cache_key_id": 2, "cache_key_name": "id (Root/id)", "src_link_key": "3"}
    ]
    join.update(overrides)
    return join


def test_ref_join_with_in_spec_cache_passes():
    err = validate_transform_map(
        _map_config(_ref_join()), ["src", "tgt", "cache"], _components()
    )
    assert err is None


def test_literal_cache_id_join_passes_without_cross_checks():
    err = validate_transform_map(
        _map_config(_LIVE_JOIN), ["src", "tgt"], _components()
    )
    assert err is None


def test_ref_join_missing_from_depends_on_rejected():
    err = validate_transform_map(
        _map_config(_ref_join()), ["src", "tgt"], _components()
    )
    assert err is not None
    assert err.error_code == "MAP_DOCUMENT_CACHE_JOINS_INVALID"
    assert err.field == "depends_on"


def test_ref_join_to_non_cache_component_rejected():
    components = _components()
    components["cache"] = _comp("profile.json", _json_profile_config("P"))
    err = validate_transform_map(
        _map_config(_ref_join()), ["src", "tgt", "cache"], components
    )
    assert err is not None
    assert err.error_code == "MAP_DOCUMENT_CACHE_JOINS_INVALID"
    assert err.details.get("target_component_type") == "profile.json"


def test_ref_join_unknown_cache_index_rejected():
    err = validate_transform_map(
        _map_config(_ref_join(cache_index=9)),
        ["src", "tgt", "cache"],
        _components(),
    )
    assert err is not None
    assert err.field.endswith("cache_index")
    assert err.details.get("declared_indexes") == [1]


def test_ref_join_unknown_cache_key_id_rejected():
    join = _ref_join()
    join["key_values"][0]["cache_key_id"] = 7
    err = validate_transform_map(
        _map_config(join), ["src", "tgt", "cache"], _components()
    )
    assert err is not None
    assert err.field.endswith("cache_key_id")
    assert err.details.get("declared_key_ids") == [2]
