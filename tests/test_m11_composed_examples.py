"""Issue #125 (M11.6, epic #118) — composed example specs prove M11 end-to-end.

Each example under examples/m11/ must (a) parse as IntegrationSpecV1, (b) plan
clean through _build_plan, and (c) emit the expected typed XML shapes. The
process-bearing examples author their process as a DIRECT canonical unit
(``integration_spec.processes``, #159 L7). The basic-flow process XML and the
property-graft map XML are byte-locked as goldens, and the join process is
byte-locked against the legacy builder's frozen output; the join example also
asserts the live-captured DocumentCacheJoins wire section and demonstrates the
#123 lineage contract (reversing the branch legs must fail the canonical
semantic gate with the branch-order error).
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

from src.boomi_mcp.categories.integration_builder import _apply_plan, _build_plan

# The example definitions live in examples/m11/*.json; the corpus (#165) owns
# their one loader and this module CONSUMES it.
import _wave_gate_golden_corpus as _corpus
import _example_unit_support as _units

_EXAMPLES_DIR = Path(_project_root) / "examples" / "m11"
_GOLDEN_DIR = Path(__file__).resolve().parent / "fixtures" / "golden_xml"
_PATCH_TARGET = "src.boomi_mcp.categories.integration_builder.paginate_metadata"

_EXAMPLE_FILES = (
    "cache_property_authoring_basic.integration.json",
    "process_property_map_function.integration.json",
    "cache_property_authoring_join.integration.json",
)

_load_example = _corpus.load_m11_example


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
    # The process-bearing examples also enter the CANONICAL typed intake the S3 docs
    # steer to (QA-159-S3-r1-03): wrapped as an ``integration_spec`` intent they must
    # validate as IntegrationSpecV1 — the raw route above never required ``name``.
    from src.boomi_mcp.models.authoring_workflow import AuthoringRequestV1

    for name in (
        "cache_property_authoring_basic.integration.json",
        "cache_property_authoring_join.integration.json",
    ):
        spec = _load_example(name)
        request = AuthoringRequestV1.model_validate({
            "contract_version": "2",
            "intent": {"intent_kind": "integration_spec", "integration_spec": spec},
        })
        assert request.intent.integration_spec.name == spec["naming"]["integration_name"], name


def test_every_example_applies_clean_in_dry_run():
    """#151 (M12.14): the shipped examples must PLAN AND APPLY through the entry.

    The issue's acceptance criterion says "plan and apply", and planning alone was
    the coverage gap the architect review caught: `_build_plan` stops before the
    apply entry point, so a defect between the two would have been invisible here.
    Dry run, so this asserts the apply path is reached and refuses to mutate —
    `_apply_plan` returns before any create/update when `dry_run` is set.
    """
    for name in _EXAMPLE_FILES:
        spec = _load_example(name)
        client = MagicMock()
        with patch(_PATCH_TARGET, return_value=[]):
            planned = _build_plan(
                MagicMock(), {"integration_spec": spec, "conflict_policy": "reuse"}
            )
            applied = _apply_plan(
                client,
                "test-profile",
                {
                    "integration_spec": spec,
                    "conflict_policy": "reuse",
                    "dry_run": True,
                },
            )
        assert applied.get("_success") is not False, name
        assert applied["dry_run"] is True, name
        # The apply path must describe exactly what planning described — a
        # divergence here is the whole reason the criterion says "and apply".
        assert [s["planned_action"] for s in applied["steps"]] == [
            s["planned_action"] for s in planned["steps"]
        ], name
        assert [s["key"] for s in applied["steps"]] == [
            s["key"] for s in planned["steps"]
        ], name
        for step in applied["steps"]:
            assert "validation_error" not in step, (name, step["key"])
            assert step["planned_action"] in ("create", "reuse"), (name, step["key"])
        # A dry run must not touch the client AT ALL — asserted as "no calls",
        # not by matching method names. The real SDK writes are
        # `component.create_component(...)`, `update_component_raw(...)`,
        # `delete_component_metadata(...)`; none contains `.create(` / `.update(` /
        # `.delete(`, so a name matcher would have passed a genuine write.
        assert client.mock_calls == [], (
            name,
            [str(call) for call in client.mock_calls],
        )


def test_basic_example_process_matches_golden():
    spec = _load_example("cache_property_authoring_basic.integration.json")
    # The process is a DIRECT canonical unit: no component carries a legacy config.
    assert not [c for c in spec["components"] if c["type"] == "process"]
    assert [u["envelope"]["component_key"] for u in spec["processes"]] == ["demo_process"]
    rendered = _corpus.render_golden_case(
        "m11:cache_property_basic", "process-component-v1"
    )
    xml = rendered.decode("utf-8") if isinstance(rendered, bytes) else rendered
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


def test_join_example_process_matches_the_frozen_legacy_bytes():
    spec = _load_example("cache_property_authoring_join.integration.json")
    assert not [c for c in spec["components"] if c["type"] == "process"]
    xml = _units.render_process_xml(spec, _units.uuid5_registry(spec))
    frozen = (
        _units.LEGACY_ORACLE_DIR / "cache_property_authoring_join.process.legacy.xml"
    ).read_text(encoding="utf-8")
    assert xml == frozen
    shapes = [s.get("shapetype") for s in ET.fromstring(xml).iter("shape")]
    assert shapes.count("branch") == 1
    assert shapes.count("doccacheload") == 1
    assert shapes.count("doccacheretrieve") == 1


def _lineage_codes(plan_result):
    return {
        code
        for diagnostic in plan_result.errors
        for code in (diagnostic.code, *diagnostic.cause_codes)
    }


def test_join_example_reversed_legs_fail_lineage():
    # The #123 contract demonstrated on the composed example: consuming leg
    # before staging leg is a provable ordering bug, caught pre-mutation by the
    # canonical semantic gate (the same compile an apply binds to).
    spec = _load_example("cache_property_authoring_join.integration.json")
    clean, _ = _units.plan_example(spec)
    assert not _lineage_codes(clean)

    tampered = json.loads(json.dumps(spec))  # deep copy
    (branch,) = [
        step
        for step in tampered["processes"][0]["process_ir"]["body"]["steps"]
        if step["kind"] == "branch"
    ]
    branch["legs"].reverse()
    plan_result, _ = _units.plan_example(tampered)
    assert any("LINEAGE" in code for code in _lineage_codes(plan_result)), (
        [d.model_dump(mode="json") for d in plan_result.errors]
    )
