"""Issue #83 (M7.3): MCP-wrapper tests for ``suggest_connection_reuse``.

Mirrors ``test_integration_import_wrapper.py``: forces local mode before
importing ``server``, resolves the registered tool via the FastMCP async API,
and proves the reuse-discovery tool is read-only, exposes exactly its documented
parameters, constructs a Boomi client for its (read-only) handler, and calls no
mutation surface.
"""

import asyncio
import json
import os
import sys
from pathlib import Path
from unittest.mock import patch, MagicMock

_project_root = str(Path(__file__).resolve().parent.parent)
if _project_root not in sys.path:
    sys.path.insert(0, _project_root)

os.environ["BOOMI_LOCAL"] = "true"

import server  # noqa: E402


def _annotation_value(annotations, key):
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
    loop = asyncio.new_event_loop()
    try:
        return loop.run_until_complete(coro)
    finally:
        loop.close()


def _resolve_tool(name):
    return _run_async(server.mcp.get_tool(name))


def _listed_tools():
    return _run_async(server.mcp.list_tools())


# ---------------------------------------------------------------------------
# Registration + schema
# ---------------------------------------------------------------------------


def test_reuse_tool_registered_and_readonly():
    t = _resolve_tool("suggest_connection_reuse")
    assert t is not None
    assert _annotation_value(t.annotations, "readOnlyHint") is True
    # Unlike the pure-meta import tool, this one queries the live account.
    assert _annotation_value(t.annotations, "openWorldHint") is True


def test_reuse_tool_in_list_tools():
    listed = {t.name for t in _listed_tools()}
    assert "suggest_connection_reuse" in listed


def test_reuse_schema_exposes_exact_params():
    by = {t.name: t for t in _listed_tools()}
    props = set(by["suggest_connection_reuse"].parameters["properties"])
    assert props == {"profile", "connector_type", "purpose", "endpoint_hint", "top_k"}


# ---------------------------------------------------------------------------
# Wrapper wiring: builds a Boomi client, delegates to the handler, no mutation
# ---------------------------------------------------------------------------


def test_reuse_wrapper_builds_client_and_delegates():
    sentinel_sdk = object()
    handler = MagicMock(return_value={"_success": True, "candidates": []})
    with (
        patch.object(server, "get_current_user", return_value="user@example.com"),
        patch.object(
            server,
            "get_secret",
            return_value={"account_id": "acct", "username": "u", "password": "p"},
        ),
        patch.object(server, "Boomi", return_value=sentinel_sdk) as m_boomi,
        patch.object(server, "suggest_connection_reuse_action", handler),
    ):
        result = server.suggest_connection_reuse(
            profile="prod",
            connector_type="database",
            purpose="orders",
            endpoint_hint="db.acme.com",
            top_k=3,
        )

    assert result == {"_success": True, "candidates": []}
    # A Boomi client was constructed and handed to the read-only handler.
    m_boomi.assert_called_once()
    handler.assert_called_once()
    args, kwargs = handler.call_args
    assert args[0] is sentinel_sdk
    assert args[1] == "prod"
    assert args[2] == "database"
    assert kwargs == {"purpose": "orders", "endpoint_hint": "db.acme.com", "top_k": 3}


def test_reuse_wrapper_error_keeps_readonly_flags_and_error_code():
    # Bug #152: a pre-handler failure (bad/disabled profile → get_secret raises)
    # must still return the full CONNECTION_REUSE_QUERY_FAILED envelope, not a
    # thinner one — matching the handler's own error contract and the docstring.
    with (
        patch.object(server, "get_current_user", side_effect=RuntimeError("no auth")),
    ):
        result = server.suggest_connection_reuse(profile="prod", connector_type="database")
    assert result["_success"] is False
    assert result["error_code"] == "CONNECTION_REUSE_QUERY_FAILED"
    assert result["profile"] == "prod"
    assert result["connector_type"] == "database"
    assert result["read_only"] is True
    assert result["boomi_mutation"] is False
    assert result["raw_xml_exposed"] is False
