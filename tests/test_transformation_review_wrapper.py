"""MCP wrapper tests for review_transformation (issue #46).

Verifies the server.py registration (read-only, closed-world), that all five
actions are reachable through the wrapper, and that the wrapper never touches
the Boomi credential/SDK path (it is a fully static, read-only tool).
"""

import asyncio
import json
import os
import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

_project_root = str(Path(__file__).resolve().parent.parent)
if _project_root not in sys.path:
    sys.path.insert(0, _project_root)

os.environ["BOOMI_LOCAL"] = "true"

import server  # noqa: E402


def _contract_spec():
    return {
        "version": "1.0", "name": "demo", "flows": [
            {"key": "transform", "operation": "transform",
             "source_profile_generation": {"field_index_by_path": {
                 "customer_id": {"path": "customer_id", "name": "customer_id",
                                 "data_type": "number", "mappable": True}}},
             "target_profile_generation": {"field_index_by_path": {
                 "Root": {"path": "Root", "name": "Root", "kind": "object",
                          "data_type": None, "required": True, "mappable": False},
                 "Root/cust_id": {"path": "Root/cust_id", "name": "cust_id",
                                  "kind": "simple", "data_type": "number",
                                  "required": True, "mappable": True}}},
             "operations": [
                 {"operation_type": "direct", "source_field": "customer_id",
                  "target_path": "Root/cust_id"}]}
        ],
    }


# ---------------------------------------------------------------------------
# Registration
# ---------------------------------------------------------------------------


def _run_async(coro):
    # asyncio.run() clears the thread's current event loop on exit, which
    # poisons legacy modules that still use asyncio.get_event_loop() (e.g.
    # tests/test_verified_storage.py). A throwaway loop that is never
    # registered as current keeps that global state untouched.
    loop = asyncio.new_event_loop()
    try:
        return loop.run_until_complete(coro)
    finally:
        loop.close()


def _get_tool():
    tools = _run_async(server.mcp.list_tools())
    return {t.name: t for t in tools}.get("review_transformation")


def test_review_transformation_registered():
    assert _get_tool() is not None


def test_review_transformation_annotations_read_only_closed_world():
    tool = _get_tool()
    assert tool.annotations.readOnlyHint is True
    assert tool.annotations.openWorldHint is False


# ---------------------------------------------------------------------------
# All five actions reachable through the wrapper
# ---------------------------------------------------------------------------


def test_wrapper_all_actions_succeed():
    spec_cfg = json.dumps({"integration_spec": _contract_spec()})

    list_fields = server.review_transformation(action="list_fields", config=spec_cfg)
    assert list_fields["_success"] is True
    assert list_fields["read_only"] is True

    validate = server.review_transformation(action="validate_unmapped", config=spec_cfg)
    assert validate["_success"] is True and validate["valid"] is True

    diff = server.review_transformation(action="mapping_diff", config=spec_cfg)
    assert diff["_success"] is True and diff["comparison_available"] is False

    payload = server.review_transformation(action="generate_test_payload", config=spec_cfg)
    assert payload["_success"] is True
    assert payload["source_payload_skeleton"] == {"customer_id": 123}

    compare = server.review_transformation(
        action="compare_expected_actual",
        config=json.dumps({"expected_payload": {"a": 1}, "actual_payload": {"a": 1}}),
    )
    assert compare["_success"] is True and compare["match"] is True


def test_wrapper_invalid_action_returns_structured_error():
    r = server.review_transformation(action="nope", config=None)
    assert r["_success"] is False
    assert r["code"] == "TRANSFORM_REVIEW_INVALID_INPUT"


def _assert_safety_flags(r):
    assert r["read_only"] is True
    assert r["boomi_mutation"] is False
    assert r["raw_xml_exposed"] is False


def test_wrapper_rejects_non_json_config():
    r = server.review_transformation(action="list_fields", config="{not json")
    assert r["_success"] is False
    assert "Invalid config" in r["error"]
    _assert_safety_flags(r)  # parse errors must still carry the contract flags


def test_wrapper_rejects_non_object_config():
    r = server.review_transformation(action="list_fields", config="[1, 2, 3]")
    assert r["_success"] is False
    assert "JSON object" in r["error"]
    _assert_safety_flags(r)


# ---------------------------------------------------------------------------
# Static contract: no Boomi credential / SDK access
# ---------------------------------------------------------------------------


def test_wrapper_never_touches_boomi_credential_path():
    spec_cfg = json.dumps({"integration_spec": _contract_spec()})
    with (
        patch.object(server, "get_current_user", MagicMock(side_effect=AssertionError("get_current_user called"))),
        patch.object(server, "get_secret", MagicMock(side_effect=AssertionError("get_secret called"))),
        patch.object(server, "Boomi", MagicMock(side_effect=AssertionError("Boomi constructed"))),
    ):
        for action in ["list_fields", "validate_unmapped", "mapping_diff", "generate_test_payload"]:
            result = server.review_transformation(action=action, config=spec_cfg)
            assert result["_success"] is True
            assert result["boomi_mutation"] is False
            assert result["raw_xml_exposed"] is False
