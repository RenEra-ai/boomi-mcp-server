"""Issue #84 (M7.4): MCP-wrapper tests for ``search_marketplace_recipes``.

Mirrors ``test_suggest_connection_reuse_wrapper.py``: forces local mode before
importing ``server``, resolves the registered tool via the FastMCP async API,
and proves the marketplace search tool is read-only + open-world, exposes exactly
``{query, tags, top_k}`` (NO profile), delegates straight to the handler WITHOUT
constructing a Boomi client or reading credentials, and keeps a structured
read-only error envelope on an unexpected failure.
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

def test_marketplace_tool_registered_and_readonly_openworld():
    t = _resolve_tool("search_marketplace_recipes")
    assert t is not None
    assert _annotation_value(t.annotations, "readOnlyHint") is True
    # Reaches an external endpoint → open-world.
    assert _annotation_value(t.annotations, "openWorldHint") is True


def test_marketplace_tool_in_list_tools():
    listed = {t.name for t in _listed_tools()}
    assert "search_marketplace_recipes" in listed


def test_marketplace_schema_exposes_exact_params():
    by = {t.name: t for t in _listed_tools()}
    props = set(by["search_marketplace_recipes"].parameters["properties"])
    assert props == {"query", "tags", "top_k"}
    # Never a profile/account/credential/install surface.
    assert "profile" not in props


# ---------------------------------------------------------------------------
# Delegation: no Boomi client, no credentials
# ---------------------------------------------------------------------------

def test_wrapper_delegates_to_handler():
    handler = MagicMock(return_value={"_success": True, "recipes": []})
    with patch.object(server, "search_marketplace_recipes_action", handler):
        result = server.search_marketplace_recipes(
            query="orders", tags=["Salesforce"], top_k=3
        )
    assert result == {"_success": True, "recipes": []}
    handler.assert_called_once()
    assert handler.call_args.kwargs == {
        "query": "orders",
        "tags": ["Salesforce"],
        "top_k": 3,
    }


def test_wrapper_no_profile_no_credentials_no_client():
    handler = MagicMock(return_value={"_success": True, "recipes": []})
    with (
        patch.object(server, "get_current_user") as m_user,
        patch.object(server, "get_secret") as m_secret,
        patch.object(server, "Boomi") as m_boomi,
        patch.object(server, "search_marketplace_recipes_action", handler),
    ):
        _run_async(
            server.mcp.call_tool("search_marketplace_recipes", {"query": "orders"})
        )
    m_user.assert_not_called()
    m_secret.assert_not_called()
    m_boomi.assert_not_called()
    handler.assert_called_once()


def test_wrapper_unexpected_error_structured_envelope():
    def _boom(*args, **kwargs):
        raise RuntimeError("boom secret token")

    with patch.object(server, "search_marketplace_recipes_action", side_effect=_boom):
        result = server.search_marketplace_recipes(query="x")

    assert result["_success"] is False
    assert result["error_code"] == "MARKETPLACE_GRAPHQL_UNAVAILABLE"
    assert result["failure_kind"] == "unexpected"
    assert result["recipes"] == []
    assert result["read_only"] is True
    assert result["boomi_mutation"] is False
    assert result["open_world"] is True
    # The exception message must never leak into the envelope.
    assert "boom secret token" not in json.dumps(result)
