"""Issue #125 (M11.6, epic #118) — composed example specs prove M11 end-to-end.

Each example under examples/m11/ must (a) parse as IntegrationSpecV1, (b) plan
clean through _build_plan, and (c) emit the expected typed XML shapes. The
basic-flow process XML and the property-graft map XML are byte-locked as
goldens; the join example asserts the live-captured DocumentCacheJoins wire
section and demonstrates the #123 lineage contract (reversing the branch legs
must fail with the branch-order error).
"""

from __future__ import annotations

import json
import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

import xml.etree.ElementTree as ET

_project_root = str(Path(__file__).resolve().parent.parent)
if _project_root not in sys.path:
    sys.path.insert(0, _project_root)

from src.boomi_mcp.categories.integration_builder import _build_plan
from src.boomi_mcp.categories.components.builders import ProcessFlowBuilder

_EXAMPLES_DIR = Path(_project_root) / "examples" / "m11"
_GOLDEN_DIR = Path(__file__).resolve().parent / "fixtures" / "golden_xml"
_PATCH_TARGET = "src.boomi_mcp.categories.integration_builder.paginate_metadata"

_EXAMPLE_FILES = (
    "cache_property_authoring_basic.integration.json",
    "process_property_map_function.integration.json",
    "cache_property_authoring_join.integration.json",
)


def _load_example(name: str) -> dict:
    payload = json.loads((_EXAMPLES_DIR / name).read_text(encoding="utf-8"))
    assert payload["example_not_template"] is True
    assert payload["template_status"] == "example_only_not_reusable_template"
    return payload["integration_spec"]


def _plan(spec: dict) -> dict:
    with patch(_PATCH_TARGET, return_value=[]):
        return _build_plan(
            MagicMock(),
            {"integration_spec": spec, "conflict_policy": "reuse"},
        )


def test_all_examples_exist():
    assert sorted(p.name for p in _EXAMPLES_DIR.glob("*.json")) == sorted(
        _EXAMPLE_FILES
    )


def test_every_example_plans_clean():
    for name in _EXAMPLE_FILES:
        plan = _plan(_load_example(name))
        assert plan.get("_success", True) is not False, name
        for step in plan["steps"]:
            assert "validation_error" not in step, (name, step["key"], step.get("validation_error"))
            assert step["planned_action"] in ("create", "reuse"), (name, step["key"])


def test_basic_example_process_matches_golden():
    spec = _load_example("cache_property_authoring_basic.integration.json")
    process = next(c for c in spec["components"] if c["type"] == "process")
    xml = ProcessFlowBuilder.build(process["config"], name=process["name"])
    golden = _GOLDEN_DIR / "m11_cache_property_basic.xml"
    assert xml == golden.read_text()
    shapes = [s.get("shapetype") for s in ET.fromstring(xml).iter("shape")]
    assert shapes.count("documentproperties") == 2
    assert "decision" in shapes
    props = [
        p.get("propertyId")
        for p in ET.fromstring(xml).iter("documentproperty")
    ]
    assert props == [
        "dynamicdocument.DDP_ORDER_KEY",
        "process.DPP_LAST_ORDER_SEEN",
    ]


def test_property_map_example_components_match_golden():
    spec = _load_example("process_property_map_function.integration.json")
    from src.boomi_mcp.categories.components.builders.process_property_builder import (
        ProcessPropertyBuilder,
    )

    pp = next(c for c in spec["components"] if c["type"] == "processproperty")
    pp_xml = ProcessPropertyBuilder().build(**pp["config"])
    golden = _GOLDEN_DIR / "m11_processproperty_map_function.xml"
    assert pp_xml == golden.read_text()
    root = ET.fromstring(pp_xml)
    declared = root.find(
        "bns:object/DefinedProcessProperties/definedProcessProperty",
        {"bns": "http://api.platform.boomi.com/"},
    )
    map_comp = next(c for c in spec["components"] if c["type"] == "transform.map")
    params = map_comp["config"]["function_mappings"][0]["parameters"]
    # The verbatim key/label coupling the #131 contract requires.
    assert declared.get("key") == params["process_property_key"]
    assert declared.find("label").text == params["process_property_name"]
    assert "runtime_props" in map_comp["depends_on"]


def test_join_example_emits_live_captured_joins_section():
    spec = _load_example("cache_property_authoring_join.integration.json")
    from src.boomi_mcp.categories.components.builders.map_builder import (
        _render_document_cache_joins,
    )

    map_comp = next(c for c in spec["components"] if c["type"] == "transform.map")
    joins = map_comp["config"]["document_cache_joins"]
    rendered = _render_document_cache_joins(joins)
    assert rendered == (
        '<DocumentCacheJoins>'
        '<DocumentCacheJoin cacheIndex="1" docCache="$ref:handoff_cache" '
        'docCacheJoinId="8">'
        '<srcParentKey key="1" tagListKey="0"/>'
        '<CacheKeyJoinValues>'
        '<CacheKeyJoinValue cacheKeyId="2" cacheKeyName="id (Root/id)">'
        '<srcLinkKey key="2" tagListKey="0"/>'
        '</CacheKeyJoinValue>'
        '</CacheKeyJoinValues>'
        '</DocumentCacheJoin>'
        '</DocumentCacheJoins>'
    )


def test_join_example_reversed_legs_fail_lineage():
    # The #123 contract demonstrated on the composed example: consuming leg
    # before staging leg is a provable ordering bug, caught pre-mutation.
    spec = _load_example("cache_property_authoring_join.integration.json")
    process = next(c for c in spec["components"] if c["type"] == "process")
    config = json.loads(json.dumps(process["config"]))  # deep copy
    legs = config["flow_sequence"][0]["legs"]
    legs.reverse()
    err = ProcessFlowBuilder.validate_config(config, depends_on=["handoff_cache"])
    assert err is not None
    assert err.error_code == "PROCESS_LINEAGE_BRANCH_ORDER_INVALID"
