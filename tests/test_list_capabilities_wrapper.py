"""Wrapper tests for the list_capabilities MCP tool (Bug #119 regression guard).

Bug #119: the previous wrapper read FastMCP's removed ``_tool_manager._tools``
attribute and returned ``{"_success": False, ...}`` on every MCP call, so the
entire catalog — including Issue #20's V3 Integration Authoring entries — was
invisible to MCP clients. These tests exercise the wrapper through the public
``mcp.call_tool`` path so any future API drift fails CI instead of shipping.
"""

import asyncio
import json
import os
import sys
from pathlib import Path
from unittest.mock import patch

_project_root = str(Path(__file__).resolve().parent.parent)
if _project_root not in sys.path:
    sys.path.insert(0, _project_root)

# Force local mode before importing server (mirrors test_integration_authoring_wrapper.py).
os.environ["BOOMI_LOCAL"] = "true"

import server  # noqa: E402


def _run_async(coro):
    # Mirrors tests/test_integration_authoring_wrapper._run_async: asyncio.run()
    # clears the thread's current event loop on exit, which poisons legacy
    # modules that still use asyncio.get_event_loop() (e.g.
    # tests/test_verified_storage.py). A throwaway loop never registered as
    # current keeps that global state untouched.
    loop = asyncio.new_event_loop()
    try:
        return loop.run_until_complete(coro)
    finally:
        loop.close()


def _payload(result):
    assert getattr(result, "content", None), f"call_tool returned no content: {result!r}"
    return json.loads(result.content[0].text)


def test_list_capabilities_wrapper_returns_success():
    """The MCP wrapper must not crash on FastMCP internals (Bug #119 regression guard)."""
    result = _run_async(server.mcp.call_tool("list_capabilities", {}))
    payload = _payload(result)
    assert payload["_success"] is True, payload
    assert payload.get("tools"), "catalog must be populated"


def test_list_capabilities_wrapper_filters_to_live_registry():
    """The filtered catalog must be a subset of the live FastMCP registry, and
    must include #20's three Integration Authoring tools."""
    result = _run_async(server.mcp.call_tool("list_capabilities", {}))
    payload = _payload(result)

    registered_names = {t.name for t in _run_async(server.mcp.list_tools())}
    catalog_names = set(payload["tools"].keys())

    assert catalog_names <= registered_names, (
        f"catalog leaked unregistered tools: {catalog_names - registered_names!r}"
    )
    for name in (
        "list_integration_archetypes",
        "get_integration_archetype",
        "build_from_archetype",
    ):
        assert name in catalog_names, f"{name!r} missing from filtered catalog"


def test_list_capabilities_wrapper_includes_orchestrate_deploy():
    """Issue #64: once orchestrate_deploy is registered, the live-filtered catalog
    must surface it (it is in the static catalog AND the FastMCP registry)."""
    result = _run_async(server.mcp.call_tool("list_capabilities", {}))
    payload = _payload(result)

    registered_names = {t.name for t in _run_async(server.mcp.list_tools())}
    assert "orchestrate_deploy" in registered_names, "orchestrate_deploy must be registered"
    assert "orchestrate_deploy" in payload["tools"], (
        "orchestrate_deploy missing from live-filtered catalog"
    )
    # The authoring workflow's step 8 routes apply -> orchestrate_deploy and must survive.
    wf = payload["workflows"].get("build_integration_from_description")
    assert wf is not None, "authoring workflow dropped — orchestrate_deploy step filtered it out"
    assert any("orchestrate_deploy" in s for s in wf["steps"]), (
        "authoring workflow must reference orchestrate_deploy"
    )


def test_list_capabilities_wrapper_surfaces_integration_authoring_workflow():
    """End-to-end: Issue #20's archetype-first workflow reaches MCP clients post-fix."""
    result = _run_async(server.mcp.call_tool("list_capabilities", {}))
    payload = _payload(result)
    wf = payload["workflows"].get("build_integration_from_description")
    assert wf is not None, "archetype-first workflow filtered out — wrapper regression"
    assert "list_boomi_profiles" in wf["steps"][0]
    # Issue #86: design_doctrine consult is interposed before archetype discovery.
    assert "design_doctrine" in wf["steps"][1]
    assert "list_integration_archetypes" in wf["steps"][2]


def test_list_capabilities_wrapper_does_not_call_boomi_or_credentials():
    """The wrapper is meta — it must never touch Boomi or credentials."""
    with (
        patch.object(server, "get_current_user") as m_user,
        patch.object(server, "get_secret") as m_secret,
        patch.object(server, "Boomi") as m_boomi,
    ):
        result = _run_async(server.mcp.call_tool("list_capabilities", {}))
    payload = _payload(result)

    assert payload["_success"] is True
    m_user.assert_not_called()
    m_secret.assert_not_called()
    m_boomi.assert_not_called()
