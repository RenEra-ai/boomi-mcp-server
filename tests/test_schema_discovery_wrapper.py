"""Wrapper tests for the M7 schema/spec discovery MCP tools (Issue #13).

Verifies the server.py wrappers for discover_openapi_spec / discover_soap_wsdl /
discover_odata_metadata / discover_db_schema:
- register in the FastMCP registry,
- carry the read-only annotations (and per-tool openWorldHint),
- expose the exact JSON-schema parameter contracts,
- delegate to the action layer (direct call + .fn()),
- never read credentials or construct a Boomi() client,
- return a leak-free structured envelope with the safety flags on unexpected
  handler failure.
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

os.environ["BOOMI_LOCAL"] = "true"

import server  # noqa: E402


TOOL_NAMES = (
    "discover_openapi_spec",
    "discover_soap_wsdl",
    "discover_odata_metadata",
    "discover_db_schema",
)

_EXPECTED_PARAMS = {
    "discover_openapi_spec": {"spec_url", "artifact", "options"},
    "discover_soap_wsdl": {"wsdl_url", "artifact", "options"},
    "discover_odata_metadata": {"metadata_url", "options"},
    "discover_db_schema": {"artifact", "options"},
}

_EXPECTED_OPEN_WORLD = {
    "discover_openapi_spec": True,
    "discover_soap_wsdl": True,
    "discover_odata_metadata": True,
    "discover_db_schema": False,
}


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
# Registration + annotations
# ---------------------------------------------------------------------------

def test_all_four_registered():
    listed = {t.name for t in _listed_tools()}
    for name in TOOL_NAMES:
        assert name in listed, f"{name} not registered"


def test_read_only_annotations():
    for name in TOOL_NAMES:
        tool = _resolve_tool(name)
        ann = tool.annotations
        assert _annotation_value(ann, "readOnlyHint") is True, name
        assert _annotation_value(ann, "destructiveHint") is False, name
        assert _annotation_value(ann, "idempotentHint") is True, name
        assert _annotation_value(ann, "openWorldHint") is _EXPECTED_OPEN_WORLD[name], name


def test_parameter_schemas_exact():
    by_name = {t.name: t for t in _listed_tools()}
    for name in TOOL_NAMES:
        schema = by_name[name].parameters
        assert schema["type"] == "object"
        assert set(schema["properties"].keys()) == _EXPECTED_PARAMS[name], name
    # required-ness of the mandatory params
    assert "metadata_url" in by_name["discover_odata_metadata"].parameters.get("required", [])
    assert "artifact" in by_name["discover_db_schema"].parameters.get("required", [])


# ---------------------------------------------------------------------------
# Delegation (direct + .fn())
# ---------------------------------------------------------------------------

def test_direct_wrapper_delegates_openapi():
    doc = {"openapi": "3.0.0", "info": {"title": "T"}, "paths": {"/x": {"get": {"responses": {}}}}}
    r = server.discover_openapi_spec(artifact=doc)
    assert r["_success"] is True and r["format"] == "openapi"
    assert r["boomi_mutation"] is False and r["raw_xml_exposed"] is False


def test_direct_wrapper_delegates_db():
    r = server.discover_db_schema({"columns": [{"table_name": "t", "column_name": "a", "data_type": "int"}]})
    assert r["_success"] is True and r["source_mode"] == "artifact"


def test_fn_delegation_matches_direct():
    doc = {"columns": [{"table_name": "t", "column_name": "a", "data_type": "int"}]}
    tool = _resolve_tool("discover_db_schema")
    fn_result = tool.fn(doc)
    assert fn_result["_success"] is True
    assert fn_result["counts"] == server.discover_db_schema(doc)["counts"]


# ---------------------------------------------------------------------------
# No credential / Boomi access
# ---------------------------------------------------------------------------

def test_wrappers_do_not_call_boomi_or_credentials():
    with (
        patch.object(server, "get_current_user") as m_user,
        patch.object(server, "get_secret") as m_secret,
        patch.object(server, "Boomi") as m_boomi,
    ):
        r1 = server.discover_openapi_spec(artifact={"openapi": "3.0.0", "info": {}, "paths": {}})
        r2 = server.discover_db_schema({"columns": [{"table_name": "t", "column_name": "a", "data_type": "int"}]})
        r3 = server.discover_soap_wsdl(artifact='<definitions xmlns="http://schemas.xmlsoap.org/wsdl/"/>')
    assert r1["_success"] and r2["_success"] and r3["_success"]
    m_user.assert_not_called()
    m_secret.assert_not_called()
    m_boomi.assert_not_called()


# ---------------------------------------------------------------------------
# Leak-free wrapper-level failure envelope
# ---------------------------------------------------------------------------

def test_unexpected_handler_exception_yields_leak_free_envelope():
    with patch.object(
        server, "discover_openapi_spec_action", side_effect=RuntimeError("boom secret-detail")
    ):
        r = server.discover_openapi_spec(artifact={"openapi": "3.0.0", "info": {}, "paths": {}})
    assert r["_success"] is False
    assert r["error_code"] == "OPENAPI_DISCOVERY_FAILED"
    assert r["read_only"] is True and r["boomi_mutation"] is False and r["raw_xml_exposed"] is False
    assert r["exception_type"] == "RuntimeError"
    assert "secret-detail" not in json.dumps(r)


def test_success_envelope_common_fields_present():
    r = server.discover_db_schema({"columns": [{"table_name": "t", "column_name": "a", "data_type": "int"}]})
    for key in ("read_only", "boomi_mutation", "raw_xml_exposed", "source_mode", "format",
                "version", "counts", "truncated", "truncation", "warnings", "tables"):
        assert key in r, key
