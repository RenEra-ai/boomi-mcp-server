"""Regression tests for BUG-28: process_id alias in list/get results.

list_processes() and get_process() must both include a ``process_id`` key
that mirrors ``component_id``.  Tests exercise the contract at both layers:
- manage_process_action() (internal router)
- server.manage_process.fn() (public MCP entrypoint)
"""

import os
import sys
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest

# Ensure project root is on sys.path so we can import server
_project_root = str(Path(__file__).resolve().parent.parent)
if _project_root not in sys.path:
    sys.path.insert(0, _project_root)

os.environ["BOOMI_LOCAL"] = "true"

import server  # noqa: E402
from boomi_mcp.categories.components.processes import manage_process_action

# ---------------------------------------------------------------------------
# helpers
# ---------------------------------------------------------------------------

COMP_ID = "abc-123-def"


def _make_component(**overrides):
    """Return a SimpleNamespace that looks like a component_metadata result."""
    defaults = dict(
        component_id=COMP_ID,
        id_="row-1",
        name="My Process",
        folder_name="Integrations",
        type="process",
        version="3",
        current_version="true",
        deleted="false",
        created_date="2026-01-01",
        modified_date="2026-04-01",
    )
    defaults.update(overrides)
    return SimpleNamespace(**defaults)


def _sdk_with_list_result(components):
    sdk = MagicMock()
    sdk.component_metadata.query_component_metadata.return_value = SimpleNamespace(
        result=components, query_token=None
    )
    return sdk


# ---------------------------------------------------------------------------
# list action
# ---------------------------------------------------------------------------


def test_list_returns_process_id():
    """list action must include process_id equal to component_id."""
    sdk = _sdk_with_list_result([_make_component()])
    result = manage_process_action(sdk, profile="dev", action="list")

    assert result["_success"] is True
    proc = result["processes"][0]
    assert "process_id" in proc, "process_id key missing from list result"
    assert proc["process_id"] == COMP_ID
    assert proc["process_id"] == proc["component_id"]


def test_list_process_id_matches_each_component():
    """Each listed process must carry its own process_id."""
    comps = [
        _make_component(component_id="id-1", name="P1"),
        _make_component(component_id="id-2", name="P2"),
    ]
    sdk = _sdk_with_list_result(comps)
    result = manage_process_action(sdk, profile="dev", action="list")

    assert result["_success"] is True
    for proc in result["processes"]:
        assert proc["process_id"] == proc["component_id"]


# ---------------------------------------------------------------------------
# get action
# ---------------------------------------------------------------------------


@patch("boomi_mcp.categories.components.processes._component_get_xml")
def test_get_returns_process_id(mock_xml):
    """get action must inject process_id equal to component_id."""
    mock_xml.return_value = {
        "component_id": COMP_ID,
        "name": "My Process",
        "type": "process",
    }
    sdk = MagicMock()
    result = manage_process_action(
        sdk, profile="dev", action="get", process_id=COMP_ID
    )

    assert result["_success"] is True
    proc = result["process"]
    assert "process_id" in proc, "process_id key missing from get result"
    assert proc["process_id"] == COMP_ID
    assert proc["process_id"] == proc["component_id"]


@patch("boomi_mcp.categories.components.processes._component_get_xml")
def test_get_process_id_fallback_when_component_id_absent(mock_xml):
    """If component_id is missing from XML, process_id falls back to the input ID."""
    mock_xml.return_value = {"name": "Orphan"}
    sdk = MagicMock()
    result = manage_process_action(
        sdk, profile="dev", action="get", process_id="fallback-id"
    )

    assert result["_success"] is True
    assert result["process"]["process_id"] == "fallback-id"


# ---------------------------------------------------------------------------
# wrapper-level tests (server.manage_process.fn)
# ---------------------------------------------------------------------------

FAKE_CREDS = {
    "account_id": "acct-test",
    "username": "user",
    "password": "pass",
}


@pytest.fixture()
def _mock_auth():
    """Patch auth helpers and SDK so the wrapper never hits real services."""
    with (
        patch.object(server, "get_current_user", return_value="test-user"),
        patch.object(server, "get_secret", return_value=FAKE_CREDS),
        patch.object(server, "Boomi", return_value=MagicMock()),
    ):
        yield


def _call_wrapper(**kwargs):
    return server.manage_process(**kwargs)


def test_wrapper_list_returns_process_id(_mock_auth):
    """Public manage_process list must include process_id in each result."""
    mock_action = MagicMock(return_value={
        "_success": True,
        "total_count": 1,
        "processes": [{"process_id": COMP_ID, "component_id": COMP_ID, "name": "P"}],
        "profile": "dev",
    })
    with patch.object(server, "manage_process_action", mock_action):
        result = _call_wrapper(profile="dev", action="list")

    assert result["_success"] is True
    proc = result["processes"][0]
    assert proc["process_id"] == proc["component_id"]


def test_wrapper_get_returns_process_id(_mock_auth):
    """Public manage_process get must include process_id in the result."""
    mock_action = MagicMock(return_value={
        "_success": True,
        "process": {"process_id": COMP_ID, "component_id": COMP_ID, "name": "P"},
        "profile": "dev",
    })
    with patch.object(server, "manage_process_action", mock_action):
        result = _call_wrapper(profile="dev", action="get", process_id=COMP_ID)

    assert result["_success"] is True
    proc = result["process"]
    assert proc["process_id"] == proc["component_id"]


def test_wrapper_list_end_to_end(_mock_auth):
    """End-to-end: wrapper -> action -> list_processes, assert process_id survives."""
    sdk = _sdk_with_list_result([_make_component()])
    with patch.object(server, "Boomi", return_value=sdk):
        result = _call_wrapper(profile="dev", action="list")

    assert result["_success"] is True
    proc = result["processes"][0]
    assert "process_id" in proc
    assert proc["process_id"] == COMP_ID


@patch("boomi_mcp.categories.components.processes._component_get_xml")
def test_wrapper_get_end_to_end(mock_xml, _mock_auth):
    """End-to-end: wrapper -> action -> get_process, assert process_id survives."""
    mock_xml.return_value = {"component_id": COMP_ID, "name": "P", "type": "process"}
    with patch.object(server, "Boomi", return_value=MagicMock()):
        result = _call_wrapper(profile="dev", action="get", process_id=COMP_ID)

    assert result["_success"] is True
    proc = result["process"]
    assert "process_id" in proc
    assert proc["process_id"] == COMP_ID


# ---------------------------------------------------------------------------
# manage_process is read-only: create/update/delete return ACTION_UNSUPPORTED.
# Legacy freeform process JSON authoring has been removed.
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("action", ["create", "update", "delete"])
def test_action_unsupported_at_router(action):
    """The router rejects authoring actions without touching the SDK."""
    sdk = MagicMock()
    result = manage_process_action(sdk, profile="dev", action=action)

    assert result["_success"] is False
    assert result["error_code"] == "ACTION_UNSUPPORTED"
    assert result["valid_actions"] == ["list", "get"]
    # No mutation attempted against the Boomi Component API.
    sdk.component.update_component.assert_not_called()
    sdk.component.create_component.assert_not_called()


def test_unknown_action_reports_action_unsupported():
    sdk = MagicMock()
    result = manage_process_action(sdk, profile="dev", action="frobnicate")
    assert result["_success"] is False
    assert result["error_code"] == "ACTION_UNSUPPORTED"
    assert result["valid_actions"] == ["list", "get"]


@pytest.mark.parametrize("action", ["create", "update", "delete"])
def test_wrapper_action_unsupported(action, _mock_auth):
    """Public manage_process wrapper surfaces the ACTION_UNSUPPORTED envelope."""
    result = _call_wrapper(profile="dev", action=action)
    assert result["_success"] is False
    assert result["error_code"] == "ACTION_UNSUPPORTED"
    assert result["valid_actions"] == ["list", "get"]


@pytest.mark.parametrize("action", ["create", "update", "delete"])
def test_wrapper_legacy_config_arg_reaches_action_unsupported(action, _mock_auth):
    """A legacy caller that still passes config/process_id must receive the
    ACTION_UNSUPPORTED envelope, not a TypeError on the dropped argument."""
    result = _call_wrapper(
        profile="dev",
        action=action,
        process_id="abc-123",
        config='{"name": "X", "shapes": []}',
    )
    assert result["_success"] is False
    assert result["error_code"] == "ACTION_UNSUPPORTED"
    assert result["valid_actions"] == ["list", "get"]
