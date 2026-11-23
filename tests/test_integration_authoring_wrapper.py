"""Wrapper tests for the recipe discovery MCP tools (Issues #18, #19; #159 S3).

Verifies that the server.py MCP wrappers:
- Register the two recipe-only discovery tools, and NOT the retired
  build_from_archetype / compose_archetypes tools (#159 S3).
- Keep review_transformation registered on its own (it is not an archetype tool).
- Carry read-only / closed-world annotations.
- Pass through to the action layer with no credential / Boomi() calls.
- Expose stable JSON-schema input contracts via mcp.list_tools().
- Are reachable end-to-end through mcp.call_tool() with structured success
  and failure payloads, all without credential reads or Boomi() construction.

Node ids of the retired build/compose tests are kept (the manifest pins them);
their bodies witness the retirement or the replacement discovery path.
"""

import asyncio
import json
import os
import sys
from pathlib import Path
from unittest.mock import patch

import pytest
from fastmcp.exceptions import NotFoundError

_project_root = str(Path(__file__).resolve().parent.parent)
if _project_root not in sys.path:
    sys.path.insert(0, _project_root)

# Force local mode before importing server (mirrors test_manage_deployment_wrapper.py)
os.environ["BOOMI_LOCAL"] = "true"

import server  # noqa: E402


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

TOOL_NAMES = (
    "list_integration_archetypes",
    "get_integration_archetype",
)

#: Retired in #159 S3 together with their action dispatchers.
RETIRED_TOOL_NAMES = ("build_from_archetype", "compose_archetypes")

_STUB = "boomi.archetype.stub_minimal"
_FANOUT = "boomi.compose.db_rest_fanout"
_DB_TO_API = "boomi.archetype.database_to_api_sync"


def _annotation_value(annotations, key):
    """Return annotations[key] across attribute, dict, or Pydantic shapes."""
    if annotations is None:
        return None
    if hasattr(annotations, key):
        return getattr(annotations, key)
    if isinstance(annotations, dict):
        return annotations.get(key)
    if hasattr(annotations, "model_dump"):
        return annotations.model_dump().get(key)
    raise AssertionError(f"Cannot read annotation {key!r} from {annotations!r}")


def _run_async(coro):
    # Mirrors tests/kb/_fixture_corpus.run_async: asyncio.run() clears the
    # thread's current event loop on exit, which poisons legacy modules that
    # still use asyncio.get_event_loop() (e.g. tests/test_verified_storage.py).
    # A throwaway loop that is never registered as current keeps that global
    # state untouched.
    loop = asyncio.new_event_loop()
    try:
        return loop.run_until_complete(coro)
    finally:
        loop.close()


def _resolve_tool(name):
    return _run_async(server.mcp.get_tool(name))


def _listed_tools():
    return _run_async(server.mcp.list_tools())


def _call_tool(name, args):
    return _run_async(server.mcp.call_tool(name, args))


def _payload(result):
    """Parse a ToolResult into the wrapper's JSON dict."""
    assert getattr(result, "content", None), f"call_tool returned no content: {result!r}"
    return json.loads(result.content[0].text)


# ---------------------------------------------------------------------------
# Registration + annotations
# ---------------------------------------------------------------------------


def test_tools_registered():
    for name in TOOL_NAMES:
        tool = _resolve_tool(name)
        assert tool is not None, f"{name} not registered"
    for name in RETIRED_TOOL_NAMES:
        assert _resolve_tool(name) is None, f"{name} is retired but still registered"
    # The step-4 prototype moved review_transformation under the archetype-tool
    # conditional; it is its own tool and must stay registered on its own.
    assert _resolve_tool("review_transformation") is not None


def test_tool_annotations_read_only_and_closed_world():
    for name in TOOL_NAMES:
        tool = _resolve_tool(name)
        ann = tool.annotations
        assert _annotation_value(ann, "readOnlyHint") is True, (
            f"{name} should have readOnlyHint=True"
        )
        assert _annotation_value(ann, "openWorldHint") is False, (
            f"{name} should have openWorldHint=False"
        )


# ---------------------------------------------------------------------------
# Direct wrapper invocation
# ---------------------------------------------------------------------------


def test_list_wrapper_returns_success():
    result = server.list_integration_archetypes()
    assert result["_success"] is True
    ids = [r["recipe_id"] for r in result["recipes"]]
    assert _STUB in ids


def test_get_wrapper_returns_success():
    result = server.get_integration_archetype(_STUB)
    assert result["_success"] is True
    assert result["descriptor"]["recipe_id"] == _STUB
    assert result["next_tool"] == "build_integration"


def test_build_wrapper_returns_success():
    """#159 S3: the build wrapper retired with its dispatcher (tombstone
    candidate); the server module no longer carries either."""
    assert not hasattr(server, "build_from_archetype")
    assert not hasattr(server, "build_from_archetype_action")
    assert not hasattr(server, "compose_archetypes")
    assert not hasattr(server, "compose_archetypes_action")


# ---------------------------------------------------------------------------
# No credential or Boomi() calls
# ---------------------------------------------------------------------------


def test_wrappers_do_not_call_boomi_or_credentials():
    with (
        patch.object(server, "get_current_user") as m_user,
        patch.object(server, "get_secret") as m_secret,
        patch.object(server, "Boomi") as m_boomi,
    ):
        r1 = server.list_integration_archetypes()
        r2 = server.get_integration_archetype(_STUB)
        r3 = server.get_integration_archetype(_FANOUT, "2.0.0")

    assert r1["_success"] is True
    assert r2["_success"] is True
    assert r3["_success"] is True

    m_user.assert_not_called()
    m_secret.assert_not_called()
    m_boomi.assert_not_called()


# ===========================================================================
# Issue #19 — list_tools() registration + schema hardening
# ===========================================================================


def test_list_tools_reports_all_authoring_tools():
    tools = _listed_tools()
    listed = {t.name for t in tools}
    for name in TOOL_NAMES:
        assert name in listed, f"{name} missing from mcp.list_tools()"
    assert "review_transformation" in listed
    for name in RETIRED_TOOL_NAMES:
        assert name not in listed, f"{name} is retired but still listed"


def test_list_tools_schemas_use_native_types_no_stale_args():
    by_name = {t.name: t for t in _listed_tools()}

    list_tool = by_name["list_integration_archetypes"]
    list_schema = list_tool.parameters
    assert list_schema["type"] == "object"
    list_props = list_schema["properties"]
    # #145 M12.10 added expected_recipe_registry (comparison-only). #159 S3
    # dropped the legacy tags filter: the recipe registry has no tag authority.
    assert set(list_props.keys()) == {"query", "expected_recipe_registry"}, (
        "list_integration_archetypes must expose only query + "
        "expected_recipe_registry — no tags / profile / boomi_client"
    )
    assert list_props["query"]["type"] == "string"

    get_tool = by_name["get_integration_archetype"]
    get_schema = get_tool.parameters
    assert get_schema["type"] == "object"
    # recipe_version pins an EXACT recipe version; it never falls forward or
    # backward.
    assert set(get_schema["properties"].keys()) == {"name", "recipe_version"}
    assert get_schema["properties"]["name"]["type"] == "string"
    assert "name" in get_schema.get("required", [])

    for name in RETIRED_TOOL_NAMES:
        assert name not in by_name


# ===========================================================================
# Issue #19 — call_tool() end-to-end success + failure payloads
# ===========================================================================


def test_call_tool_list_returns_structured_success():
    result = _call_tool("list_integration_archetypes", {})
    payload = _payload(result)
    assert payload["_success"] is True
    assert payload["raw_xml_exposed"] is False
    ids = [r["recipe_id"] for r in payload["recipes"]]
    assert _STUB in ids


def test_call_tool_get_returns_structured_success():
    result = _call_tool("get_integration_archetype", {"name": _STUB})
    payload = _payload(result)
    assert payload["_success"] is True
    assert payload["raw_xml_exposed"] is False
    assert payload["next_tool"] == "build_integration"
    assert payload["descriptor"]["recipe_id"] == _STUB
    assert payload["input_schema"]["additionalProperties"] is False


def test_call_tool_get_returns_enriched_describe_payload():
    """The MCP-facing call_tool path surfaces the same projection as the action,
    for EVERY row the list returns, fetched by its exact pin."""
    listed = _payload(_call_tool("list_integration_archetypes", {}))
    assert listed["count"] == len(listed["recipes"]) == 6
    for row in listed["recipes"]:
        payload = _payload(
            _call_tool(
                "get_integration_archetype",
                {"name": row["recipe_id"], "recipe_version": row["recipe_version"]},
            )
        )
        assert payload["_success"] is True, row
        assert payload["descriptor"] == row
        for key in ("capability_notes", "limitations", "examples"):
            assert payload["descriptor"][key], (row["recipe_id"], key)
        for example in payload["descriptor"]["examples"]:
            assert example["is_template"] is False


def test_call_tool_build_returns_structured_success():
    """#159 S3: build_from_archetype is not a tool any more (tombstone
    candidate) — calling it is an unknown-tool error, never a spec."""
    with pytest.raises(NotFoundError):
        _call_tool(
            "build_from_archetype",
            {"name": "stub_minimal_integration", "parameters": {"integration_name": "demo"}},
        )


def test_call_tool_get_missing_archetype_returns_structured_failure():
    result = _call_tool(
        "get_integration_archetype",
        {"name": "does-not-exist-xyzzy"},
    )
    payload = _payload(result)
    assert payload["_success"] is False
    assert payload["error_code"] == "RECIPE_NOT_FOUND"


def test_call_tool_build_invalid_parameters_returns_field_errors():
    """An exact pin that is not served — empty, unknown or retired — refuses
    with the registry's lookup diagnostic and the versions that ARE served."""
    for version in ("", "9.9.9", "1.0.0"):
        payload = _payload(
            _call_tool(
                "get_integration_archetype",
                {"name": _FANOUT, "recipe_version": version},
            )
        )
        assert payload["_success"] is False, version
        assert payload["error_code"] == "RECIPE_VERSION_UNAVAILABLE", version
        assert payload["recipe_diagnostics"][0]["available_versions"] == ["2.0.0"]


def test_call_tool_paths_do_not_call_boomi_or_credentials():
    with (
        patch.object(server, "get_current_user") as m_user,
        patch.object(server, "get_secret") as m_secret,
        patch.object(server, "Boomi") as m_boomi,
    ):
        r_list = _payload(_call_tool("list_integration_archetypes", {}))
        r_get = _payload(_call_tool("get_integration_archetype", {"name": _STUB}))
        r_pin = _payload(
            _call_tool(
                "get_integration_archetype",
                {"name": _FANOUT, "recipe_version": "2.0.0"},
            )
        )
        r_missing = _payload(
            _call_tool("get_integration_archetype", {"name": "nope-xyzzy"})
        )

    assert r_list["_success"] is True
    assert r_get["_success"] is True
    assert r_pin["_success"] is True
    assert r_missing["_success"] is False

    m_user.assert_not_called()
    m_secret.assert_not_called()
    m_boomi.assert_not_called()


# ===========================================================================
# Issue #14 (M8) — the fan-out is a recipe since #159 S3 (compose_archetypes retired)
# ===========================================================================


def test_call_tool_compose_returns_structured_success():
    """The composed fan-out is discovered as the default fan-out recipe: its
    input schema and its closed component-material schema are both served."""
    payload = _payload(_call_tool("get_integration_archetype", {"name": _FANOUT}))
    assert payload["_success"] is True
    assert payload["raw_xml_exposed"] is False
    assert payload["boomi_mutation"] is False
    assert payload["recipe_version"] == "2.0.0"
    assert {"source", "transform", "targets", "links"} <= set(
        payload["input_schema"]["properties"]
    )
    assert payload["component_material_schema"]["additionalProperties"] is False


def test_call_tool_compose_contract_mismatch_returns_structured_failure():
    """#159 S3: compose_archetypes is not a tool any more (tombstone candidate)."""
    with pytest.raises(NotFoundError):
        _call_tool("compose_archetypes", {"parts": [], "options": {}})


def test_compose_wrapper_does_not_call_boomi_or_credentials():
    with (
        patch.object(server, "get_current_user") as m_user,
        patch.object(server, "get_secret") as m_secret,
        patch.object(server, "Boomi") as m_boomi,
    ):
        payload = _payload(_call_tool("get_integration_archetype", {"name": _FANOUT}))

    assert payload["_success"] is True
    m_user.assert_not_called()
    m_secret.assert_not_called()
    m_boomi.assert_not_called()


# ===========================================================================
# Issue #21 — database_to_api_sync, MCP wrapper smoke (a recipe since #159 S3)
# ===========================================================================


def test_call_tool_list_includes_database_to_api_sync():
    result = _call_tool("list_integration_archetypes", {"query": "database_to_api"})
    payload = _payload(result)
    assert payload["_success"] is True
    assert [r["recipe_id"] for r in payload["recipes"]] == [_DB_TO_API]


def test_call_tool_get_database_to_api_sync_schema_is_strict():
    result = _call_tool("get_integration_archetype", {"name": _DB_TO_API})
    payload = _payload(result)
    assert payload["_success"] is True
    schema = payload["input_schema"]
    assert schema["additionalProperties"] is False
    assert payload["component_material_schema"]["additionalProperties"] is False


def test_call_tool_build_database_to_api_sync_minimal_succeeds():
    """#159 S3: the legacy archetype build retired (tombstone candidate); the
    discovered descriptor names build_integration and an exact pin instead."""
    payload = _payload(
        _call_tool(
            "get_integration_archetype",
            {"name": _DB_TO_API, "recipe_version": "1.0.0"},
        )
    )
    assert payload["_success"] is True
    assert payload["next_tool"] == "build_integration"
    assert f"'recipe_id': '{_DB_TO_API}'" in payload["next_steps"]
    assert "'recipe_version': '1.0.0'" in payload["next_steps"]
    assert "component_material" in payload["next_steps"]


# ---------------------------------------------------------------------------
# Import-path recipe provenance (#145 M12.10)
# ---------------------------------------------------------------------------


def test_the_import_recipe_ref_names_what_actually_executes():
    """"The EXACT recipe reference" has to include the code that runs.

    A migrated preset routes through TWO registry entries: the adapter it
    selects (``boomi.adapter.*``, a ``compatibility_adapter``) and the executable
    recipe that adapter targets (``boomi.archetype.*``). Returning only the
    adapter named the entry point and omitted the code, so a reader pinning a
    version from this payload pinned the wrong one
    (issue #145, §6 architect review).
    """
    #159 S3: the import's draft now PINS the executable recipe itself (no
    # compatibility adapter is selected any more), so the served reference IS the
    # code that runs: an executable registration, the exact pin the draft carries.
    # REST -> REST is drafted as a direct unit and names no recipe at all.
    from boomi_mcp.categories.integration_import import (
        _PRESET_RECIPES,
        _selected_recipe_ref,
    )
    from boomi_mcp.recipes import production_registry

    assert _selected_recipe_ref("api_to_api_sync") is None
    for preset in _PRESET_RECIPES:
        ref = _selected_recipe_ref(preset)
        assert "adapter_target" not in ref
        executable = production_registry().resolve(ref["recipe_id"], ref["recipe_version"])
        assert executable.entry_kind == ref["entry_kind"] == "executable_recipe"


def test_an_unmigrated_preset_still_reports_an_honest_absence():
    """The addition is additive: presets with no recipe still return ``None``
    rather than a plausible-looking reference to code they do not use."""
    from boomi_mcp.categories.integration_import import _selected_recipe_ref

    assert _selected_recipe_ref(None) is None
    assert _selected_recipe_ref("no_such_preset") is None
