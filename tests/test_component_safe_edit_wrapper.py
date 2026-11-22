"""Wrapper tests for the M9.7 safe-edit MCP tools (#97).

Exercises the FastMCP registration of prepare_component_edit / apply_component_edit:
annotations, parameter contract, JSON-before-credentials parsing, and the
confirm_apply gate firing before any credential access.
"""

import asyncio
import json
import os
import sys
from pathlib import Path

import pytest

_project_root = str(Path(__file__).resolve().parent.parent)
if _project_root not in sys.path:
    sys.path.insert(0, _project_root)

# Force local mode before importing server.
os.environ["BOOMI_LOCAL"] = "true"

import server  # noqa: E402


def _run_async(coro):
    loop = asyncio.new_event_loop()
    try:
        return loop.run_until_complete(coro)
    finally:
        loop.close()


def _payload(result):
    assert getattr(result, "content", None), f"call_tool returned no content: {result!r}"
    return json.loads(result.content[0].text)


def _boom_creds(*_args, **_kwargs):
    raise AssertionError("credentials were accessed before the input gate")


# ---------------------------------------------------------------------------
# Registration + annotations + parameter contract
# ---------------------------------------------------------------------------

def test_both_tools_registered_with_correct_annotations():
    prep = _run_async(server.mcp.get_tool("prepare_component_edit"))
    apply = _run_async(server.mcp.get_tool("apply_component_edit"))

    assert prep.annotations.readOnlyHint is True
    assert prep.annotations.openWorldHint is True

    assert apply.annotations.readOnlyHint is False
    assert apply.annotations.destructiveHint is True
    assert apply.annotations.openWorldHint is True


def test_parameter_contract():
    prep = _run_async(server.mcp.get_tool("prepare_component_edit"))
    prep_props = prep.parameters["properties"]
    assert {"profile", "component_id", "patch", "max_diff_lines"} <= set(prep_props)
    assert "max_diff_lines" not in prep.parameters.get("required", [])

    apply = _run_async(server.mcp.get_tool("apply_component_edit"))
    apply_props = apply.parameters["properties"]
    assert {"profile", "component_id", "patch", "confirmation_token", "confirm_apply"} <= set(apply_props)
    # confirm_apply defaults to false, so it is not required.
    assert "confirm_apply" not in apply.parameters.get("required", [])


def test_docstrings_document_workflow():
    prep = _run_async(server.mcp.get_tool("prepare_component_edit"))
    apply = _run_async(server.mcp.get_tool("apply_component_edit"))
    assert "confirmation_token" in (prep.description or "")
    assert "READ-ONLY" in (prep.description or "")
    assert "confirm_apply=true" in (apply.description or "")
    assert "drift" in (apply.description or "").lower()


# ---------------------------------------------------------------------------
# Input validation happens BEFORE any credential access
# ---------------------------------------------------------------------------

def test_prepare_invalid_json_before_credentials(monkeypatch):
    monkeypatch.setattr(server, "get_secret", _boom_creds)
    result = _run_async(
        server.mcp.call_tool(
            "prepare_component_edit",
            {"profile": "p", "component_id": "c", "patch": "{not json"},
        )
    )
    payload = _payload(result)
    assert payload["_success"] is False
    assert "Invalid patch" in payload["error"]
    assert payload["boomi_mutation"] is False


def test_prepare_non_object_patch_rejected(monkeypatch):
    monkeypatch.setattr(server, "get_secret", _boom_creds)
    result = _run_async(
        server.mcp.call_tool(
            "prepare_component_edit",
            {"profile": "p", "component_id": "c", "patch": "[1, 2, 3]"},
        )
    )
    payload = _payload(result)
    assert payload["_success"] is False
    assert "JSON object" in payload["error"]


def test_apply_confirmation_gate_before_credentials(monkeypatch):
    monkeypatch.setattr(server, "get_secret", _boom_creds)
    result = _run_async(
        server.mcp.call_tool(
            "apply_component_edit",
            {
                "profile": "p",
                "component_id": "c",
                "patch": json.dumps({"config": {"name": "x"}}),
                "confirmation_token": "tok",
                "confirm_apply": False,
            },
        )
    )
    payload = _payload(result)
    assert payload["_success"] is False
    assert payload["error_code"] == "COMPONENT_EDIT_CONFIRMATION_REQUIRED"
    assert payload["boomi_mutation"] is False


def test_apply_invalid_json_before_credentials(monkeypatch):
    monkeypatch.setattr(server, "get_secret", _boom_creds)
    result = _run_async(
        server.mcp.call_tool(
            "apply_component_edit",
            {
                "profile": "p",
                "component_id": "c",
                "patch": "{not json",
                "confirmation_token": "tok",
                "confirm_apply": True,
            },
        )
    )
    payload = _payload(result)
    assert payload["_success"] is False
    assert "Invalid patch" in payload["error"]
    assert payload["boomi_mutation"] is False


# ---------------------------------------------------------------------------
# Happy path: wrapper parses the patch and hands the dict to the action,
# returning the action's result (incl. confirmation_token).
# ---------------------------------------------------------------------------

def test_prepare_passes_parsed_patch_and_returns_token(monkeypatch):
    seen = {}

    def _fake_prepare(sdk, profile, component_id, patch_data, max_diff_lines):
        seen["patch_data"] = patch_data
        seen["max_diff_lines"] = max_diff_lines
        return {"_success": True, "confirmation_token": "TKN", "boomi_mutation": False}

    monkeypatch.setattr(server, "get_current_user", lambda: "tester")
    monkeypatch.setattr(server, "get_secret", lambda *a, **k: {
        "account_id": "acc", "username": "u", "password": "pw",
    })
    monkeypatch.setattr(server, "Boomi", lambda **kw: object())
    monkeypatch.setattr(server, "prepare_component_edit_action", _fake_prepare)

    result = _run_async(
        server.mcp.call_tool(
            "prepare_component_edit",
            {
                "profile": "p",
                "component_id": "c",
                "patch": json.dumps({"config": {"name": "x"}}),
            },
        )
    )
    payload = _payload(result)
    assert payload["_success"] is True
    assert payload["confirmation_token"] == "TKN"
    assert seen["patch_data"] == {"config": {"name": "x"}}
    assert seen["max_diff_lines"] == 200


# ---------------------------------------------------------------------------
# #159 S3: the canonical process update arm through the REGISTERED tools
# ---------------------------------------------------------------------------

_tests_dir = str(Path(__file__).resolve().parent)
if _tests_dir not in sys.path:
    sys.path.insert(0, _tests_dir)


def public_safe_edit(monkeypatch, account, patch, component_id):
    """prepare_component_edit then apply_component_edit, through ``server.mcp``.

    Only the account transport is faked: ``server.Boomi`` constructs ``account``
    (its component API answers reads and records writes); credentials are stubbed
    so the tool reaches it. Returns ``(prepared, pushes_after_prepare, applied)``.
    """
    monkeypatch.setattr(server, "get_current_user", lambda: "tester")
    monkeypatch.setattr(server, "get_secret", lambda *a, **k: {
        "account_id": account.account_id, "username": "u", "password": "pw",
    })
    monkeypatch.setattr(server, "Boomi", lambda **kw: account)
    prepared = _payload(_run_async(server.mcp.call_tool(
        "prepare_component_edit",
        {"profile": "p", "component_id": component_id, "patch": json.dumps(patch)},
    )))
    pushes_after_prepare = list(account.component.pushed)
    if not prepared.get("_success"):
        return prepared, pushes_after_prepare, None
    applied = _payload(_run_async(server.mcp.call_tool(
        "apply_component_edit",
        {"profile": "p", "component_id": component_id, "patch": json.dumps(patch),
         "confirmation_token": prepared["confirmation_token"], "confirm_apply": True},
    )))
    return prepared, pushes_after_prepare, applied


def test_the_public_safe_edit_applies_a_canonical_source_bound_update(monkeypatch):
    """The source-only canonical spelling (golden-000080's), bound to existing
    operations, end to end through the registered tools: zero writes on prepare, one
    update of the named process, the frozen golden's shapes, preservation intact."""
    from test_component_safe_edit import (
        CANONICAL_PROCESS_ID,
        CanonicalAccount,
        assert_live_process_preserved,
        canonical_account_store,
        canonical_patch,
        shapes_without_layout,
    )

    account = CanonicalAccount(canonical_account_store())
    prepared, pushes_after_prepare, applied = public_safe_edit(
        monkeypatch, account, canonical_patch(), CANONICAL_PROCESS_ID
    )
    assert prepared["_success"] is True, prepared
    assert prepared["boomi_mutation"] is False and pushes_after_prepare == []
    assert applied["_success"] is True, applied
    assert [cid for cid, _ in account.component.pushed] == [CANONICAL_PROCESS_ID]
    pushed = account.component.pushed[0][1]
    golden = (Path(_tests_dir) / "fixtures/golden_xml/issue184_source_dynamic_path_dpp.xml").read_text(
        encoding="utf-8"
    )
    assert shapes_without_layout(pushed) == shapes_without_layout(golden)
    assert_live_process_preserved(pushed)


def test_the_public_safe_edit_publishes_the_canonical_arm():
    prep = _run_async(server.mcp.get_tool("prepare_component_edit"))
    apply = _run_async(server.mcp.get_tool("apply_component_edit"))
    assert "authoring_request" in (prep.description or "")
    assert "authoring_request" in (apply.description or "")
