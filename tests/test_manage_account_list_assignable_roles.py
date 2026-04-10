"""Regression tests for list_assignable_roles _note behavior (BUG-21).

Calls _action_list_assignable_roles directly with a mocked SDK so the real
handler logic — including the _note branch — actually executes.
Also verifies the server.py wrapper forwards resource_id correctly.
"""

import os
import sys
from unittest.mock import MagicMock, patch
from pathlib import Path

_project_root = str(Path(__file__).resolve().parent.parent)
if _project_root not in sys.path:
    sys.path.insert(0, _project_root)

os.environ["BOOMI_LOCAL"] = "true"

import server  # noqa: E402
from boomi_mcp.categories.account import _action_list_assignable_roles


def _make_sdk(roles=None):
    """Return a mock SDK whose get_assignable_roles returns the given roles."""
    sdk = MagicMock()
    result = MagicMock()
    if roles is not None:
        result.role = roles
    else:
        result.role = []
    sdk.get_assignable_roles.get_get_assignable_roles.return_value = result
    return sdk


def _make_role(id_="role-1", name="Admin", description="Admin role", account_id="acct-1"):
    role = MagicMock()
    role.id_ = id_
    role.name = name
    role.description = description
    role.account_id = account_id
    role.parent_id = None
    role.privileges = None
    return role


def test_without_resource_id_no_note():
    """Without resource_id the response should have no _note."""
    sdk = _make_sdk(roles=[_make_role()])
    result = _action_list_assignable_roles(sdk, profile="dev")
    assert result["_success"] is True
    assert result["total_count"] == 1
    assert "_note" not in result


def test_with_resource_id_adds_note():
    """Passing resource_id should succeed and include _note explaining it was ignored."""
    sdk = _make_sdk(roles=[_make_role()])
    result = _action_list_assignable_roles(sdk, profile="dev", resource_id="bogus-id")
    assert result["_success"] is True
    assert result["total_count"] == 1
    assert "_note" in result
    assert "ignored" in result["_note"]


def test_empty_roles_with_resource_id():
    """Even with zero roles, resource_id should still produce _note."""
    sdk = _make_sdk(roles=[])
    result = _action_list_assignable_roles(sdk, profile="dev", resource_id="anything")
    assert result["_success"] is True
    assert result["total_count"] == 0
    assert "_note" in result


# ---------------------------------------------------------------------------
# Wrapper-level test: verify server.py forwards resource_id to the action
# ---------------------------------------------------------------------------

FAKE_CREDS = {
    "account_id": "acct-test",
    "username": "user",
    "password": "pass",
}


def test_wrapper_forwards_resource_id():
    """manage_account.fn() must pass resource_id through to manage_account_action."""
    mock_action = MagicMock(return_value={"_success": True})
    with (
        patch.object(server, "get_current_user", return_value="test-user"),
        patch.object(server, "get_secret", return_value=FAKE_CREDS),
        patch.object(server, "Boomi", return_value=MagicMock()),
        patch.object(server, "manage_account_action", mock_action),
    ):
        server.manage_account.fn(
            profile="dev", action="list_assignable_roles", resource_id="test-id"
        )
    _, kwargs = mock_action.call_args
    assert kwargs.get("resource_id") == "test-id"
