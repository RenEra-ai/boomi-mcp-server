"""Tests for the read-only ``plan_integration_design`` design-brief assembler
(issue #94, epic #85 / M4.5.9).

Pure-unit against ``boomi_mcp`` for the action logic — no SDK calls, no Boomi —
plus a small server-wrapper section that exercises the registered MCP tool's
annotations / output_schema / structured content. Covers both modes
(archetype-provided vs pre-selection), the anti-template / no-NL-parsing guards,
capability-gap fidelity against the source registries, the list_capabilities
entry + filtering, and the output-schema/payload lockstep.
"""

import asyncio
import inspect
import os
import sys
from pathlib import Path

_src = str(Path(__file__).resolve().parent.parent / "src")
if _src not in sys.path:
    sys.path.insert(0, _src)
_root = str(Path(__file__).resolve().parent.parent)
if _root not in sys.path:
    sys.path.insert(0, _root)

from boomi_mcp.categories import meta_tools
from boomi_mcp.categories.meta_tools import (
    PLAN_INTEGRATION_DESIGN_OUTPUT_SCHEMA,
    list_capabilities_action,
    plan_integration_design_action,
)
from boomi_mcp.errors import INVALID_INPUT, PATTERN_NOT_FOUND
from boomi_mcp.kb.account_governance import get_account_governance_catalog
from boomi_mcp.kb.design_doctrine import get_design_doctrine_catalog


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _archetype_names():
    from boomi_mcp import patterns as patterns_pkg
    from boomi_mcp.patterns import PatternKind, PatternRegistry

    registry = PatternRegistry.from_package(patterns_pkg)
    return [c.metadata.name for c in registry.list_patterns(kind=PatternKind.ARCHETYPE)]


def _status_by_name():
    statuses = {}
    for entry in get_design_doctrine_catalog()["entries"]:
        statuses[("design_doctrine", entry["name"])] = entry["capability_status"]
    for entry in get_account_governance_catalog()["entries"]:
        statuses[("account_governance", entry["name"])] = entry["capability_status"]
    return statuses


# ---------------------------------------------------------------------------
# Pre-selection mode (archetype omitted)
# ---------------------------------------------------------------------------

def test_pre_selection_mode_shape():
    r = plan_integration_design_action(intent_flags=["incremental", "retry"])
    assert r["_success"] is True
    assert r["mode"] == "pre_selection"
    assert r["archetype"] is None
    assert r["missing_inputs"] == ["archetype"]
    # The only decision is the archetype-selection missing-input item.
    assert len(r["required_user_decisions"]) == 1
    only = r["required_user_decisions"][0]
    assert only["from"] == "missing_input:archetype"
    assert only["field"] is None
    assert "select an archetype" in only["description"]
    # Discovery must steer toward archetype selection.
    discovery_tools = [s["tool"] for s in r["discovery_steps"]]
    assert "list_integration_archetypes" in discovery_tools
    assert discovery_tools[0] == "list_integration_archetypes"
    for expected in ("list_boomi_profiles", "query_components", "infer_profile_fields"):
        assert expected in discovery_tools
    # Read-only safety flags.
    assert r["read_only"] is True
    assert r["boomi_mutation"] is False
    assert r["raw_xml_exposed"] is False


def test_pre_selection_emits_no_schema_derived_decisions():
    """Acceptance criterion: with no archetype the response emits NO
    parameter-schema-derived decisions."""
    r = plan_integration_design_action(intent_flags=["incremental", "retry", "dlq"])
    froms = [d["from"] for d in r["required_user_decisions"]]
    assert "archetype_parameter_schema" not in froms
    assert froms == ["missing_input:archetype"]


def test_pre_selection_budget_caps():
    r = plan_integration_design_action(intent_flags=["incremental", "retry", "dlq", "notify"])
    assert r["doctrine_shown"] <= 10
    assert r["governance_shown"] <= 5
    assert r["doctrine_total"] == get_design_doctrine_catalog()["entry_count"]
    assert r["governance_total"] == get_account_governance_catalog()["entry_count"]
    assert "showing" in r["budget_note"]
    # Always surfaces governance even when nothing strongly matches.
    assert r["governance_shown"] >= 1


def test_pre_selection_no_flags_is_still_valid():
    r = plan_integration_design_action()
    assert r["_success"] is True
    assert r["mode"] == "pre_selection"
    assert r["intent_flags"] == []
    # No keywords -> doctrine may be empty, but governance still surfaces.
    assert r["doctrine_shown"] >= 0
    assert r["governance_shown"] >= 1


# ---------------------------------------------------------------------------
# Archetype mode (full brief)
# ---------------------------------------------------------------------------

def test_archetype_mode_schema_derived_decisions():
    r = plan_integration_design_action(
        archetype="database_to_api_sync",
        intent_flags=["incremental", "retry", "dlq", "notify"],
    )
    assert r["_success"] is True
    assert r["mode"] == "archetype"
    assert r["archetype"] == "database_to_api_sync"
    assert r["missing_inputs"] == []
    schema_decisions = [
        d for d in r["required_user_decisions"]
        if d["from"] == "archetype_parameter_schema"
    ]
    assert schema_decisions, "archetype mode must emit parameter-schema decisions"
    # At least one decision must surface a cross-cutting design choice.
    fields = " ".join((d["field"] or "") for d in schema_decisions).lower()
    assert any(k in fields for k in ("watermark", "dlq", "retry")), fields
    # Every schema decision carries a concrete field path.
    assert all(d["field"] for d in schema_decisions)


def test_archetype_mode_recommends_named_patterns_with_status():
    r = plan_integration_design_action(archetype="database_to_api_sync")
    patterns = r["recommended_doctrine_patterns"] + r["recommended_governance_patterns"]
    assert patterns, "a brief must recommend at least one pattern"
    for p in patterns:
        assert p["name"]
        assert p["capability_status"] in (
            "emittable_today", "gated", "guidance_only", "na"
        )
        assert p["source"] in ("design_doctrine", "account_governance")


# ---------------------------------------------------------------------------
# Capability-gap fidelity — gaps are COMPUTED from the registries
# ---------------------------------------------------------------------------

def test_capability_gaps_match_source_registries():
    r = plan_integration_design_action(
        archetype="database_to_api_sync",
        intent_flags=["incremental", "retry", "dlq", "notify"],
    )
    truth = _status_by_name()
    for gap in r["capability_gaps"]:
        key = (gap["source"], gap["name"])
        assert key in truth, f"gap names an entry absent from the registry: {key}"
        assert gap["capability_status"] == truth[key]
        # A gap is, by definition, NOT buildable today.
        assert gap["capability_status"] != "emittable_today"


# ---------------------------------------------------------------------------
# Error envelopes
# ---------------------------------------------------------------------------

def test_unknown_archetype_returns_small_error_envelope():
    r = plan_integration_design_action(archetype="__nope__")
    assert r["_success"] is False
    assert r["mode"] == "error"
    assert r["error_code"] == PATTERN_NOT_FOUND
    assert "__nope__" in r["error"]
    assert isinstance(r["valid_archetypes"], list) and r["valid_archetypes"]
    # Must NOT dump the doctrine/governance catalogs in the error path.
    assert "recommended_doctrine_patterns" not in r
    assert "recommended_governance_patterns" not in r


# ---------------------------------------------------------------------------
# Anti-template / no-NL-parsing guards
# ---------------------------------------------------------------------------

def test_freetext_intent_flag_is_rejected():
    r = plan_integration_design_action(intent_flags=["full sync"])
    assert r["_success"] is False
    assert r["mode"] == "error"
    assert r["error_code"] == INVALID_INPUT


def test_non_list_intent_flags_rejected():
    r = plan_integration_design_action(intent_flags="retry")  # type: ignore[arg-type]
    assert r["_success"] is False
    assert r["error_code"] == INVALID_INPUT


def test_archetype_like_flag_does_not_trigger_archetype_mode():
    """An archetype name passed as an intent flag must NOT be interpreted as
    selecting that archetype (no NL inference, no flag->archetype map)."""
    name = _archetype_names()[0]
    r = plan_integration_design_action(intent_flags=[name])
    assert r["mode"] == "pre_selection"
    assert r["archetype"] is None
    froms = [d["from"] for d in r["required_user_decisions"]]
    assert "archetype_parameter_schema" not in froms


def test_no_hardcoded_archetype_names_in_source():
    """Anti-template: the assembler and its helpers must contain no hard-coded
    archetype name literals (no canned task/flag -> archetype map)."""
    targets = [plan_integration_design_action] + [
        obj for name, obj in vars(meta_tools).items()
        if name.startswith("_plan_") and inspect.isfunction(obj)
    ]
    source = "\n".join(inspect.getsource(fn) for fn in targets)
    for archetype_name in _archetype_names():
        assert archetype_name not in source, (
            f"hard-coded archetype name {archetype_name!r} leaked into the assembler"
        )


# ---------------------------------------------------------------------------
# list_capabilities integration
# ---------------------------------------------------------------------------

def test_list_capabilities_includes_plan_integration_design():
    catalog = list_capabilities_action()
    entry = catalog["tools"].get("plan_integration_design")
    assert entry is not None
    assert entry["category"] == "Knowledge / Design"
    assert entry["read_only"] is True
    assert entry["no_boomi_mutation"] is True
    assert "archetype" in entry["parameters"]
    assert "intent_flags" in entry["parameters"]


def test_list_capabilities_filters_plan_integration_design():
    # Excluded from the live registry -> filtered out.
    without = list_capabilities_action(available_tools={"list_capabilities"})
    assert "plan_integration_design" not in without["tools"]
    # Present in the live registry -> retained.
    with_it = list_capabilities_action(
        available_tools={"list_capabilities", "plan_integration_design"}
    )
    assert "plan_integration_design" in with_it["tools"]


# ---------------------------------------------------------------------------
# Output-schema / payload lockstep
# ---------------------------------------------------------------------------

def test_payload_keys_declared_in_output_schema():
    props = set(PLAN_INTEGRATION_DESIGN_OUTPUT_SCHEMA["properties"].keys())
    required = set(PLAN_INTEGRATION_DESIGN_OUTPUT_SCHEMA["required"])
    for result in (
        plan_integration_design_action(intent_flags=["retry"]),
        plan_integration_design_action(archetype="database_to_api_sync"),
        plan_integration_design_action(archetype="__nope__"),
        plan_integration_design_action(intent_flags=["bad flag"]),
    ):
        extra = set(result.keys()) - props
        assert not extra, f"payload key(s) absent from output schema: {extra}"
        missing = required - set(result.keys())
        assert not missing, f"payload missing required schema key(s): {missing}"


# ---------------------------------------------------------------------------
# Server-wrapper metadata (registered MCP tool)
# ---------------------------------------------------------------------------

def _run_async(coro):
    loop = asyncio.new_event_loop()
    try:
        return loop.run_until_complete(coro)
    finally:
        loop.close()


def test_mcp_tool_metadata_and_structured_output():
    os.environ["BOOMI_LOCAL"] = "true"
    import server  # noqa: E402

    tool = _run_async(server.mcp.get_tool("plan_integration_design"))
    assert tool.title == "Plan Integration Design"
    assert tool.annotations.readOnlyHint is True
    assert tool.annotations.openWorldHint is False
    assert tool.output_schema, "output_schema must be defined on the registered tool"
    assert tool.output_schema is PLAN_INTEGRATION_DESIGN_OUTPUT_SCHEMA or (
        tool.output_schema["properties"].keys()
        == PLAN_INTEGRATION_DESIGN_OUTPUT_SCHEMA["properties"].keys()
    )

    # The wrapper returns a ToolResult: text fallback + structured content.
    result = tool.fn(intent_flags=["incremental", "retry"])
    assert result.structured_content["tool"] == "plan_integration_design"
    assert result.structured_content["mode"] == "pre_selection"
    text = result.content if isinstance(result.content, str) else result.content[0].text
    assert text and "Design brief" in text


def test_mcp_call_tool_roundtrip_structured_content():
    os.environ["BOOMI_LOCAL"] = "true"
    import server  # noqa: E402

    result = _run_async(
        server.mcp.call_tool(
            "plan_integration_design",
            {"archetype": "database_to_api_sync", "intent_flags": ["dlq"]},
        )
    )
    sc = result.structured_content
    assert sc["_success"] is True
    assert sc["mode"] == "archetype"
    assert sc["read_only"] is True
