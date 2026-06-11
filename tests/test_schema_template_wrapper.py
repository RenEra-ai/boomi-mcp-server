"""Wrapper tests for the get_schema_template MCP tool (Issue #10).

Exercises the schema_name selector and the now-optional resource_type through
the public mcp.call_tool path. Mirrors tests/test_list_capabilities_wrapper.py
idioms (BOOMI_LOCAL before import, throwaway event loop, _payload parse).
"""

import asyncio
import json
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
    # Throwaway loop never registered as current — keeps the thread's global
    # event-loop state untouched (see test_list_capabilities_wrapper.py).
    loop = asyncio.new_event_loop()
    try:
        return loop.run_until_complete(coro)
    finally:
        loop.close()


def _payload(result):
    assert getattr(result, "content", None), f"call_tool returned no content: {result!r}"
    return json.loads(result.content[0].text)


def test_wrapper_schema_name_integration_spec_v1():
    result = _run_async(
        server.mcp.call_tool("get_schema_template", {"schema_name": "IntegrationSpecV1"})
    )
    payload = _payload(result)
    assert payload["_success"] is True, payload
    assert "components" in payload["json_schema"]["properties"]


def test_wrapper_legacy_resource_type_still_works():
    result = _run_async(
        server.mcp.call_tool("get_schema_template", {"resource_type": "process"})
    )
    payload = _payload(result)
    assert payload["_success"] is True, payload


def test_wrapper_input_schema_exposes_optional_schema_name():
    tool = _run_async(server.mcp.get_tool("get_schema_template"))
    properties = tool.parameters["properties"]
    assert "schema_name" in properties
    required = tool.parameters.get("required", [])
    assert "schema_name" not in required
    # resource_type is now optional too — schema_name-only calls must validate.
    assert "resource_type" not in required


def test_wrapper_docstring_documents_schema_name():
    tool = _run_async(server.mcp.get_tool("get_schema_template"))
    description = tool.description or ""
    assert 'schema_name="IntegrationSpecV1"' in description
    assert "SCHEMA_SELECTOR_REQUIRED" in description
