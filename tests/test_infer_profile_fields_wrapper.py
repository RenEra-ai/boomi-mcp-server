"""Issue #47: action-layer + MCP-wrapper tests for ``infer_profile_fields``.

Mirrors the conventions of ``test_integration_authoring_wrapper.py``: forces
local mode before importing ``server``, resolves/calls the registered tool via
the FastMCP async API, and proves the discovery tool is read-only and never
touches Boomi / credentials.
"""

import asyncio
import json
import os
import sys
from pathlib import Path
from unittest.mock import patch

import pytest

_project_root = str(Path(__file__).resolve().parent.parent)
if _project_root not in sys.path:
    sys.path.insert(0, _project_root)

os.environ["BOOMI_LOCAL"] = "true"

import server  # noqa: E402

from boomi_mcp.categories.integration_authoring import (  # noqa: E402
    infer_profile_fields_action as act,
)


# ---------------------------------------------------------------------------
# Helpers (copied from test_integration_authoring_wrapper.py)
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


# ---------------------------------------------------------------------------
# Task 6 — action layer: envelope, options, dispatch, errors
# ---------------------------------------------------------------------------

_FLAG_KEYS = ("read_only", "boomi_mutation", "raw_xml_exposed")


def _assert_flags(env):
    assert env["read_only"] is True
    assert env["boomi_mutation"] is False
    assert env["raw_xml_exposed"] is False


def test_action_success_envelope_flags():
    r = act("profile_from_sample_json", '{"id":1}')
    assert r["_success"] is True
    _assert_flags(r)
    assert r["generation_mode"] == "profile_from_sample_json"
    assert r["mappable_paths"] == ["Root/id"]


def test_action_dispatch_db_metadata():
    r = act("profile_from_db_metadata", {"columns": [{"name": "a", "data_type": "varchar"}]})
    assert r["_success"] is True and r["component_type"] == "profile.db"


def test_action_dispatch_xsd_and_xml():
    xsd = '<xs:schema xmlns:xs="http://www.w3.org/2001/XMLSchema"><xs:element name="R" type="xs:string"/></xs:schema>'
    rx = act("profile_from_xsd", xsd)
    assert rx["_success"] is True and rx["component_type"] == "profile.xml"
    rs = act("profile_from_sample_xml", "<R><A>v</A></R>")
    assert rs["_success"] is True and rs["component_type"] == "profile.xml"


def test_action_options_json_string_ok():
    r = act("profile_from_sample_json", '{"id":1}', options='{"component_name":"Demo"}')
    assert r["_success"] is True and r["component_name"] == "Demo"


def test_action_options_dict_ok():
    r = act("profile_from_sample_json", '{"id":1}', options={"component_name": "Demo2"})
    assert r["component_name"] == "Demo2"


def test_action_bad_options_json():
    r = act("profile_from_sample_json", '{"id":1}', options="{bad")
    assert r["_success"] is False
    assert r["code"] == "PROFILE_INFERENCE_INVALID_INPUT"
    _assert_flags(r)


def test_action_options_non_object_json():
    r = act("profile_from_sample_json", '{"id":1}', options="[1,2,3]")
    assert r["_success"] is False and r["code"] == "PROFILE_INFERENCE_INVALID_INPUT"


def test_action_unknown_source_type():
    r = act("profile_from_unicorn", {})
    assert r["_success"] is False
    assert r["code"] == "PROFILE_INFERENCE_INVALID_INPUT"
    assert "profile_from_sample_json" in r["details"]["supported_source_types"]
    _assert_flags(r)


def test_action_oversize_input_char_limit():
    big = '{"a":"' + ("x" * 50) + '"}'
    r = act("profile_from_sample_json", big, options={"max_input_chars": 5})
    assert r["_success"] is False
    assert r["code"] == "PROFILE_INFERENCE_INPUT_TOO_LARGE"
    assert r["truncated"] is True
    assert r["ready_for_builder"] is False
    assert r["truncation"]["kind"] == "input_chars"
    # never echo the oversized artifact content
    assert "x" * 50 not in json.dumps(r)


def test_action_oversize_nodes_from_pure_layer():
    r = act("profile_from_sample_json", '{"a":1,"b":2,"c":3}', options={"max_fields": 2})
    assert r["_success"] is False and r["code"] == "PROFILE_INFERENCE_INPUT_TOO_LARGE"
    assert r["truncated"] is True and r["ready_for_builder"] is False


def test_action_error_envelope_keeps_flags():
    r = act("profile_from_sample_json", '"scalar root"')
    assert r["_success"] is False
    assert r["code"] == "PROFILE_INFERENCE_UNSUPPORTED_SHAPE"
    _assert_flags(r)


def test_action_propagated_43_error_keeps_flags():
    r = act(
        "profile_from_db_metadata",
        {"columns": [{"name": "a", "data_type": "varchar"}, {"name": "a", "data_type": "int"}]},
    )
    assert r["_success"] is False
    assert r["code"] == "DUPLICATE_PROFILE_FIELD_PATH"
    _assert_flags(r)


def test_action_ambiguous_marks_not_ready_but_succeeds():
    r = act("profile_from_db_metadata", {"columns": [{"name": "flag", "data_type": "bit"}]})
    assert r["_success"] is True
    assert r["ready_for_builder"] is False
    assert r["fields"][0]["confirmation_required"] is True


# ---------------------------------------------------------------------------
# Task 7 — MCP wrapper registration + read-only proof
# ---------------------------------------------------------------------------


def test_infer_tool_registered_and_readonly():
    t = _resolve_tool("infer_profile_fields")
    assert t is not None
    assert _annotation_value(t.annotations, "readOnlyHint") is True
    assert _annotation_value(t.annotations, "openWorldHint") is False


def test_infer_tool_in_list_tools():
    listed = {t.name for t in _listed_tools()}
    assert "infer_profile_fields" in listed


def test_infer_schema_has_no_profile_param():
    by = {t.name: t for t in _listed_tools()}
    props = set(by["infer_profile_fields"].parameters["properties"])
    assert "profile" not in props
    assert {"source_type", "artifact"} <= props


def test_infer_call_tool_success_and_flags():
    p = _payload(
        _call_tool(
            "infer_profile_fields",
            {"source_type": "profile_from_sample_json", "artifact": '{"id":1}'},
        )
    )
    assert p["_success"] is True
    assert p["read_only"] is True
    assert p["boomi_mutation"] is False
    assert p["raw_xml_exposed"] is False
    assert p["generation_mode"] == "profile_from_sample_json"


def test_infer_call_tool_structured_artifact():
    p = _payload(
        _call_tool(
            "infer_profile_fields",
            {
                "source_type": "profile_from_db_metadata",
                "artifact": {"columns": [{"name": "a", "data_type": "varchar"}]},
            },
        )
    )
    assert p["_success"] is True and p["component_type"] == "profile.db"


def test_infer_options_schema_is_constrained_not_any():
    # Architect/Codex review: options must advertise the dict|str|null contract
    # in list_tools (not an unconstrained Any that lets clients send arrays/numbers).
    by = {t.name: t for t in _listed_tools()}
    opt = by["infer_profile_fields"].parameters["properties"]["options"]
    types = set()
    for variant in opt.get("anyOf", [opt]):
        if "type" in variant:
            types.add(variant["type"])
    assert {"object", "string", "null"} <= types, f"options schema not constrained: {opt}"


def test_infer_call_tool_options_as_dict():
    # Architect review: the public contract advertises options: dict | str | None,
    # so a dict at the MCP boundary must reach the action, not be rejected by the
    # wrapper's type before the read-only envelope can be produced.
    p = _payload(
        _call_tool(
            "infer_profile_fields",
            {
                "source_type": "profile_from_sample_json",
                "artifact": '{"id":1}',
                "options": {"component_name": "DemoDict"},
            },
        )
    )
    assert p["_success"] is True and p["component_name"] == "DemoDict"


def test_infer_call_tool_options_json_string():
    p = _payload(
        _call_tool(
            "infer_profile_fields",
            {
                "source_type": "profile_from_sample_json",
                "artifact": '{"id":1}',
                "options": '{"component_name":"Demo"}',
            },
        )
    )
    assert p["_success"] is True and p["component_name"] == "Demo"


def test_infer_call_tool_error_keeps_flags():
    p = _payload(
        _call_tool(
            "infer_profile_fields",
            {"source_type": "profile_from_sample_json", "artifact": '"scalar root"'},
        )
    )
    assert p["_success"] is False
    assert p["code"] == "PROFILE_INFERENCE_UNSUPPORTED_SHAPE"
    assert p["read_only"] is True and p["boomi_mutation"] is False


def test_infer_wrapper_no_boomi_or_credentials():
    with (
        patch.object(server, "get_current_user") as m_user,
        patch.object(server, "get_secret") as m_secret,
        patch.object(server, "Boomi") as m_boomi,
    ):
        r = server.infer_profile_fields("profile_from_sample_json", '{"id":1}')
    assert r["_success"] is True
    m_user.assert_not_called()
    m_secret.assert_not_called()
    m_boomi.assert_not_called()


def test_infer_call_tool_path_no_boomi_or_credentials():
    with (
        patch.object(server, "get_current_user") as m_user,
        patch.object(server, "get_secret") as m_secret,
        patch.object(server, "Boomi") as m_boomi,
    ):
        p = _payload(
            _call_tool(
                "infer_profile_fields",
                {"source_type": "profile_from_xsd",
                 "artifact": '<xs:schema xmlns:xs="http://www.w3.org/2001/XMLSchema"><xs:element name="R" type="xs:string"/></xs:schema>'},
            )
        )
    assert p["_success"] is True
    m_user.assert_not_called()
    m_secret.assert_not_called()
    m_boomi.assert_not_called()


def test_infer_stable_output():
    a = {"source_type": "profile_from_sample_json", "artifact": '{"id":1,"name":"x"}'}
    assert _payload(_call_tool("infer_profile_fields", a)) == _payload(
        _call_tool("infer_profile_fields", a)
    )
