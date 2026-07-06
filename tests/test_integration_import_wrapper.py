"""Issue #48 (M7.2): MCP-wrapper tests for ``import_integration_draft``.

Mirrors ``test_infer_profile_fields_wrapper.py``: forces local mode before
importing ``server``, resolves/calls the registered tool via the FastMCP async
API, and proves the migration-import tool is read-only and never touches
Boomi / credentials.
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


# ---------------------------------------------------------------------------
# Helpers (copied from test_infer_profile_fields_wrapper.py)
# ---------------------------------------------------------------------------


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


def _call_tool(name, args):
    return _run_async(server.mcp.call_tool(name, args))


def _payload(result):
    assert getattr(result, "content", None), f"call_tool returned no content: {result!r}"
    return json.loads(result.content[0].text)


def _json_profile(children):
    return {
        "format": "json",
        "root": {"name": "Root", "kind": "object", "children": children},
    }


def _artifact():
    return {
        "name": "Wrapper Sync",
        "source": {
            "protocol": "rest",
            "base_url": "https://source.example.com",
            "path": "/v1/items",
            "schema": {
                "profile": _json_profile(
                    [{"name": "id", "kind": "simple", "data_type": "number", "required": True}]
                )
            },
        },
        "target": {
            "protocol": "rest",
            "base_url": "https://target.example.com",
            "path": "/v1/records",
            "schema": {
                "profile": _json_profile(
                    [{"name": "rid", "kind": "simple", "data_type": "number", "required": True}]
                )
            },
        },
        "mappings": [{"from": "id", "to": "rid"}],
    }


# ---------------------------------------------------------------------------
# Registration + schema
# ---------------------------------------------------------------------------


def test_import_tool_registered_and_readonly():
    t = _resolve_tool("import_integration_draft")
    assert t is not None
    assert _annotation_value(t.annotations, "readOnlyHint") is True
    assert _annotation_value(t.annotations, "openWorldHint") is False


def test_import_tool_in_list_tools():
    listed = {t.name for t in _listed_tools()}
    assert "import_integration_draft" in listed


def test_import_schema_has_no_profile_param():
    by = {t.name: t for t in _listed_tools()}
    props = set(by["import_integration_draft"].parameters["properties"])
    assert "profile" not in props
    assert props == {"source_type", "artifact", "options"}


def test_import_options_schema_is_constrained_not_any():
    by = {t.name: t for t in _listed_tools()}
    opt = by["import_integration_draft"].parameters["properties"]["options"]
    types = set()
    for variant in opt.get("anyOf", [opt]):
        if "type" in variant:
            types.add(variant["type"])
    assert {"object", "string", "null"} <= types, f"options schema not constrained: {opt}"


# ---------------------------------------------------------------------------
# call_tool paths
# ---------------------------------------------------------------------------


def test_import_call_tool_success_and_flags():
    p = _payload(
        _call_tool(
            "import_integration_draft",
            {"source_type": "generic_integration_description", "artifact": _artifact()},
        )
    )
    assert p["_success"] is True
    assert p["read_only"] is True
    assert p["boomi_mutation"] is False
    assert p["raw_xml_exposed"] is False
    assert p["ready_for_build"] is True
    assert isinstance(p["gaps"], list) and p["gaps"] == []
    assert p["selected_preset"] == "api_to_api_sync"
    assert [s["kind"] for s in p["pipeline_draft"]["stages"]] == ["fetch", "map", "send"]
    assert "integration_spec_draft" in p


def test_import_call_tool_blocked_analysis_keeps_flags_and_suppresses_draft():
    artifact = _artifact()
    artifact["source"]["auth"] = {"mode": "basic"}  # no credential_ref
    p = _payload(
        _call_tool(
            "import_integration_draft",
            {"source_type": "generic_integration_description", "artifact": artifact},
        )
    )
    assert p["_success"] is True
    assert p["read_only"] is True and p["boomi_mutation"] is False
    assert p["ready_for_build"] is False
    assert "MIGRATION_IMPORT_MISSING_CREDENTIAL" in [g["code"] for g in p["gaps"]]
    assert "integration_spec_draft" not in p


def test_import_call_tool_malformed_input_error_keeps_flags():
    p = _payload(
        _call_tool(
            "import_integration_draft",
            {"source_type": "generic_integration_description", "artifact": "free text"},
        )
    )
    assert p["_success"] is False
    assert p["code"] == "MIGRATION_IMPORT_INVALID_INPUT"
    assert p["read_only"] is True and p["boomi_mutation"] is False


def test_import_call_tool_options_as_dict_and_string():
    for options in ({"component_prefix": "MIGR"}, '{"component_prefix": "MIGR"}'):
        p = _payload(
            _call_tool(
                "import_integration_draft",
                {
                    "source_type": "generic_integration_description",
                    "artifact": _artifact(),
                    "options": options,
                },
            )
        )
        assert p["_success"] is True
        assert p["preset_parameters"]["naming"]["component_prefix"] == "MIGR"


# ---------------------------------------------------------------------------
# Read-only proof — no Boomi / credential access on either path
# ---------------------------------------------------------------------------


def test_import_wrapper_no_boomi_or_credentials():
    with (
        patch.object(server, "get_current_user") as m_user,
        patch.object(server, "get_secret") as m_secret,
        patch.object(server, "Boomi") as m_boomi,
    ):
        r = server.import_integration_draft(
            "generic_integration_description", _artifact()
        )
    assert r["_success"] is True
    m_user.assert_not_called()
    m_secret.assert_not_called()
    m_boomi.assert_not_called()


def test_import_call_tool_path_no_boomi_or_credentials():
    with (
        patch.object(server, "get_current_user") as m_user,
        patch.object(server, "get_secret") as m_secret,
        patch.object(server, "Boomi") as m_boomi,
    ):
        p = _payload(
            _call_tool(
                "import_integration_draft",
                {
                    "source_type": "source_tool_export_summary",
                    "artifact": {"product": "AnyTool", "flow": _artifact()},
                },
            )
        )
    assert p["_success"] is True
    assert p["input_provenance"]["product"] == "AnyTool"
    m_user.assert_not_called()
    m_secret.assert_not_called()
    m_boomi.assert_not_called()


def test_import_stable_output():
    args = {
        "source_type": "generic_integration_description",
        "artifact": _artifact(),
    }
    assert _payload(_call_tool("import_integration_draft", args)) == _payload(
        _call_tool("import_integration_draft", args)
    )
