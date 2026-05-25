"""Wrapper tests for V3 archetype MCP tools (Issues #18, #19).

Verifies that the server.py MCP wrappers:
- Register all three tools.
- Carry read-only / closed-world annotations.
- Pass through to the action layer with no credential / Boomi() calls.
- Expose stable JSON-schema input contracts via mcp.list_tools().
- Are reachable end-to-end through mcp.call_tool() with structured success
  and failure payloads, all without credential reads or Boomi() construction.
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

# Force local mode before importing server (mirrors test_manage_deployment_wrapper.py)
os.environ["BOOMI_LOCAL"] = "true"

import server  # noqa: E402


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

TOOL_NAMES = (
    "list_integration_archetypes",
    "get_integration_archetype",
    "build_from_archetype",
)


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
    names = [a["name"] for a in result["archetypes"]]
    assert "stub_minimal_integration" in names


def test_get_wrapper_returns_success():
    result = server.get_integration_archetype("stub_minimal_integration")
    assert result["_success"] is True
    assert result["archetype"]["metadata"]["name"] == "stub_minimal_integration"


def test_build_wrapper_returns_success():
    result = server.build_from_archetype(
        "stub_minimal_integration",
        {"integration_name": "demo"},
    )
    assert result["_success"] is True
    assert result["integration_spec"]["name"] == "demo"
    assert result["boomi_mutation"] is False


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
        r2 = server.get_integration_archetype("stub_minimal_integration")
        r3 = server.build_from_archetype(
            "stub_minimal_integration",
            {"integration_name": "demo"},
        )

    assert r1["_success"] is True
    assert r2["_success"] is True
    assert r3["_success"] is True

    m_user.assert_not_called()
    m_secret.assert_not_called()
    m_boomi.assert_not_called()


# ===========================================================================
# Issue #19 — list_tools() registration + schema hardening
# ===========================================================================


def test_list_tools_reports_all_three_authoring_tools():
    tools = _listed_tools()
    listed = {t.name for t in tools}
    for name in TOOL_NAMES:
        assert name in listed, f"{name} missing from mcp.list_tools()"


def test_list_tools_schemas_use_native_types_no_stale_args():
    by_name = {t.name: t for t in _listed_tools()}

    list_tool = by_name["list_integration_archetypes"]
    list_schema = list_tool.parameters
    assert list_schema["type"] == "object"
    list_props = list_schema["properties"]
    assert set(list_props.keys()) == {"query", "tags"}, (
        "list_integration_archetypes must expose only query + tags — no profile / boomi_client"
    )
    assert list_props["tags"]["type"] == "array"
    assert list_props["tags"]["items"]["type"] == "string"
    assert list_props["query"]["type"] == "string"

    get_tool = by_name["get_integration_archetype"]
    get_schema = get_tool.parameters
    assert get_schema["type"] == "object"
    assert set(get_schema["properties"].keys()) == {"name"}
    assert get_schema["properties"]["name"]["type"] == "string"
    assert "name" in get_schema.get("required", [])

    build_tool = by_name["build_from_archetype"]
    build_schema = build_tool.parameters
    assert build_schema["type"] == "object"
    build_props = build_schema["properties"]
    assert set(build_props.keys()) == {"name", "parameters"}, (
        "build_from_archetype must expose only name + parameters — no profile arg"
    )
    assert build_props["name"]["type"] == "string"
    # parameters must be an object at the MCP boundary; the legacy JSON-string
    # form is intentionally only supported by the action helper for direct
    # Python callers.
    assert build_props["parameters"]["type"] == "object"
    assert "name" in build_schema.get("required", [])


# ===========================================================================
# Issue #19 — call_tool() end-to-end success + failure payloads
# ===========================================================================


def test_call_tool_list_returns_structured_success():
    result = _call_tool("list_integration_archetypes", {})
    payload = _payload(result)
    assert payload["_success"] is True
    assert payload["raw_xml_exposed"] is False
    names = [a["name"] for a in payload["archetypes"]]
    assert "stub_minimal_integration" in names


def test_call_tool_get_returns_structured_success():
    result = _call_tool(
        "get_integration_archetype",
        {"name": "stub_minimal_integration"},
    )
    payload = _payload(result)
    assert payload["_success"] is True
    assert payload["raw_xml_exposed"] is False
    assert payload["next_tool"] == "build_from_archetype"
    assert payload["archetype"]["metadata"]["name"] == "stub_minimal_integration"
    assert payload["archetype"]["parameter_schema"]["additionalProperties"] is False


def test_call_tool_get_returns_enriched_describe_payload():
    """The MCP-facing call_tool path surfaces the same enrichment as the direct action."""
    result = _call_tool(
        "get_integration_archetype",
        {"name": "stub_minimal_integration"},
    )
    payload = _payload(result)
    arch = payload["archetype"]
    for key in ("capability_notes", "limitations", "examples", "example_policy"):
        assert key in arch, f"call_tool archetype payload missing {key!r}"

    assert arch["example_policy"] == "example_only_not_reusable_template"
    assert arch["capability_notes"]
    assert arch["limitations"]
    assert arch["examples"]

    for example in arch["examples"]:
        assert example["is_template"] is False
        assert example["template_status"] == "example_only_not_reusable_template"

    props = arch["parameter_schema"]["properties"]
    for prop_name, prop_schema in props.items():
        assert prop_schema.get("description"), (
            f"property {prop_name!r} is missing a description"
        )


def test_call_tool_build_returns_structured_success():
    result = _call_tool(
        "build_from_archetype",
        {
            "name": "stub_minimal_integration",
            "parameters": {"integration_name": "demo"},
        },
    )
    payload = _payload(result)
    assert payload["_success"] is True
    assert payload["raw_xml_exposed"] is False
    assert payload["boomi_mutation"] is False
    assert payload["integration_spec"]["name"] == "demo"


def test_call_tool_get_missing_archetype_returns_structured_failure():
    result = _call_tool(
        "get_integration_archetype",
        {"name": "does-not-exist-xyzzy"},
    )
    payload = _payload(result)
    assert payload["_success"] is False
    assert payload["error_code"] == "PATTERN_NOT_FOUND"


def test_call_tool_build_invalid_parameters_returns_field_errors():
    result = _call_tool(
        "build_from_archetype",
        {"name": "stub_minimal_integration", "parameters": {}},
    )
    payload = _payload(result)
    assert payload["_success"] is False
    assert payload["error_code"] == "PARAM_VALIDATION_FAILED"
    paths = [fe["field_path"] for fe in payload["field_errors"]]
    assert "integration_name" in paths


def test_call_tool_paths_do_not_call_boomi_or_credentials():
    with (
        patch.object(server, "get_current_user") as m_user,
        patch.object(server, "get_secret") as m_secret,
        patch.object(server, "Boomi") as m_boomi,
    ):
        r_list = _payload(_call_tool("list_integration_archetypes", {}))
        r_get = _payload(
            _call_tool(
                "get_integration_archetype",
                {"name": "stub_minimal_integration"},
            )
        )
        r_build = _payload(
            _call_tool(
                "build_from_archetype",
                {
                    "name": "stub_minimal_integration",
                    "parameters": {"integration_name": "demo"},
                },
            )
        )
        r_missing = _payload(
            _call_tool("get_integration_archetype", {"name": "nope-xyzzy"})
        )

    assert r_list["_success"] is True
    assert r_get["_success"] is True
    assert r_build["_success"] is True
    assert r_missing["_success"] is False

    m_user.assert_not_called()
    m_secret.assert_not_called()
    m_boomi.assert_not_called()


# ===========================================================================
# Issue #21 — database_to_api_sync contract-only archetype, MCP wrapper smoke
# ===========================================================================


_DB_TO_API_SYNC_MINIMAL_PAYLOAD = {
    "naming": {
        "integration_name": "demo-db-to-api-sync",
        "component_prefix": "DEMO",
    },
    "source": {
        "binding": {
            "mode": "create",
            "settings": {
                "driver": "microsoft_jdbc",
                "auth_mode": "username_password",
                "host": "db.internal",
                "database": "AppDB",
                "username": "svc_sync",
                "credential_ref": "secrets/db/svc_sync",
            },
        },
        "read_operation": {
            "sql": "<<user-authored DB read statement>>",
            "result_schema": {
                "fields": [
                    {"name": "source_a", "data_type": "character"},
                ],
            },
        },
    },
    "target": {
        "binding": {
            "mode": "create",
            "settings": {
                "base_url": "https://api.example.com",
                "auth_mode": "none",
            },
        },
        "send_request": {"method": "POST", "path": "/v1/items"},
        "payload_profile": {
            "format": "json",
            "root": {
                "name": "Root",
                "kind": "object",
                "children": [
                    {
                        "name": "target_a",
                        "kind": "simple",
                        "data_type": "character",
                    },
                ],
            },
        },
    },
    "transform": {
        "operations": [
            {
                "operation_type": "direct",
                "source_field": "source_a",
                "target_path": "Root/target_a",
            },
        ],
    },
    "execution": {"trigger": {"mode": "manual"}},
    "reliability": {
        "retry": {"max_attempts": 1},
        "dlq": {"enabled": False},
        "error_classifier": {},
    },
}


def test_call_tool_list_includes_database_to_api_sync():
    result = _call_tool("list_integration_archetypes", {})
    payload = _payload(result)
    assert payload["_success"] is True
    names = [a["name"] for a in payload["archetypes"]]
    assert "database_to_api_sync" in names


def test_call_tool_get_database_to_api_sync_schema_is_strict():
    result = _call_tool(
        "get_integration_archetype",
        {"name": "database_to_api_sync"},
    )
    payload = _payload(result)
    assert payload["_success"] is True
    arch = payload["archetype"]
    schema = arch["parameter_schema"]
    assert schema["additionalProperties"] is False
    for prop_name, prop_schema in schema["properties"].items():
        assert prop_schema.get("description"), (
            f"top-level property {prop_name!r} is missing a description"
        )


def test_call_tool_build_database_to_api_sync_minimal_succeeds():
    result = _call_tool(
        "build_from_archetype",
        {
            "name": "database_to_api_sync",
            "parameters": _DB_TO_API_SYNC_MINIMAL_PAYLOAD,
        },
    )
    payload = _payload(result)
    assert payload["_success"] is True
    assert payload["boomi_mutation"] is False
    assert payload["raw_xml_exposed"] is False
    spec = payload["integration_spec"]
    assert spec["components"] == []
    assert spec["mode"] == "redesign"
    assert spec["validation_rules"]["contract_only"] is True
