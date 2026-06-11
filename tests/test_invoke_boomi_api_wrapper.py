"""Wrapper tests for the invoke_boomi_api MCP tool in server.py (Issue #79).

Asserts the registration surface only: destructive annotations, confirm_write
exposure in the MCP input schema, and the raw-XML full-replacement docstring
warning. The gate behavior itself is covered by tests/test_invoke_boomi_api_guard.py.
"""

import asyncio
import os
import sys
from pathlib import Path

_project_root = str(Path(__file__).resolve().parent.parent)
if _project_root not in sys.path:
    sys.path.insert(0, _project_root)

# Force local mode before importing server.
os.environ["BOOMI_LOCAL"] = "true"

import server  # noqa: E402


def _run_async(coro):
    # Fresh loop per call — never poison the thread's global event loop.
    loop = asyncio.new_event_loop()
    try:
        return loop.run_until_complete(coro)
    finally:
        loop.close()


def test_registered_with_destructive_annotations():
    tool = _run_async(server.mcp.get_tool("invoke_boomi_api"))
    ann = tool.annotations
    assert ann is not None, "invoke_boomi_api must carry tool annotations"
    assert ann.readOnlyHint is False
    assert ann.destructiveHint is True
    assert ann.openWorldHint is True


def test_input_schema_exposes_confirm_write():
    tool = _run_async(server.mcp.get_tool("invoke_boomi_api"))
    schema = tool.parameters
    assert "confirm_write" in schema["properties"]
    assert schema["properties"]["confirm_write"].get("default") is False
    # confirm_write must not be required — reads need no confirmation.
    assert "confirm_write" not in schema.get("required", [])


def test_description_warns_raw_xml_full_replacement():
    tool = _run_async(server.mcp.get_tool("invoke_boomi_api"))
    description = tool.description or ""
    assert "FULL REPLACEMENT" in description
    assert "read-merge-write" in description
    assert "confirm_write" in description
