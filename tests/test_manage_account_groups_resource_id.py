"""Regression tests for BUG-22: resource_id fallback in account group list handlers.

The three list association handlers (list_accounts, list_user_roles,
list_integration_packs) must accept resource_id as a fallback when
account_group_id is not provided in config.  Without the fix, passing only
resource_id would result in an unfiltered query returning all associations.

Tests call the action handlers directly with a mocked SDK and verify that
the resource_id value reaches the underlying query function as the filter.
Also verifies the server.py wrapper forwards resource_id correctly.
"""

import os
import sys
from unittest.mock import MagicMock, patch, call
from pathlib import Path

_project_root = str(Path(__file__).resolve().parent.parent)
if _project_root not in sys.path:
    sys.path.insert(0, _project_root)

os.environ["BOOMI_LOCAL"] = "true"

import server  # noqa: E402
from boomi_mcp.categories.account_groups import (
    _action_list_accounts,
    _action_list_user_roles,
    _action_list_integration_packs,
)


# ---------------------------------------------------------------------------
# Handler-level tests: resource_id fallback
# ---------------------------------------------------------------------------

GROUP_ID = "group-abc-123"


@patch("boomi_mcp.categories.account_groups._query_all_account_group_accounts", return_value=[])
def test_list_accounts_uses_resource_id(mock_query):
    """list_accounts must use resource_id when account_group_id is absent."""
    sdk = MagicMock()
    _action_list_accounts(sdk, profile="dev", resource_id=GROUP_ID)
    mock_query.assert_called_once_with(sdk, account_group_id_filter=GROUP_ID)


@patch("boomi_mcp.categories.account_groups._query_all_account_group_accounts", return_value=[])
def test_list_accounts_prefers_account_group_id(mock_query):
    """account_group_id in config takes precedence over resource_id."""
    sdk = MagicMock()
    _action_list_accounts(sdk, profile="dev", account_group_id="explicit-id", resource_id=GROUP_ID)
    mock_query.assert_called_once_with(sdk, account_group_id_filter="explicit-id")


@patch("boomi_mcp.categories.account_groups._query_all_account_group_user_roles", return_value=[])
def test_list_user_roles_uses_resource_id(mock_query):
    """list_user_roles must use resource_id when account_group_id is absent."""
    sdk = MagicMock()
    _action_list_user_roles(sdk, profile="dev", resource_id=GROUP_ID)
    mock_query.assert_called_once_with(sdk, account_group_id_filter=GROUP_ID)


@patch("boomi_mcp.categories.account_groups._query_all_account_group_user_roles", return_value=[])
def test_list_user_roles_prefers_account_group_id(mock_query):
    """account_group_id in config takes precedence over resource_id."""
    sdk = MagicMock()
    _action_list_user_roles(sdk, profile="dev", account_group_id="explicit-id", resource_id=GROUP_ID)
    mock_query.assert_called_once_with(sdk, account_group_id_filter="explicit-id")


@patch("boomi_mcp.categories.account_groups._query_all_account_group_integration_packs", return_value=[])
def test_list_integration_packs_uses_resource_id(mock_query):
    """list_integration_packs must use resource_id when account_group_id is absent."""
    sdk = MagicMock()
    _action_list_integration_packs(sdk, profile="dev", resource_id=GROUP_ID)
    mock_query.assert_called_once_with(sdk, account_group_id_filter=GROUP_ID)


@patch("boomi_mcp.categories.account_groups._query_all_account_group_integration_packs", return_value=[])
def test_list_integration_packs_prefers_account_group_id(mock_query):
    """account_group_id in config takes precedence over resource_id."""
    sdk = MagicMock()
    _action_list_integration_packs(sdk, profile="dev", account_group_id="explicit-id", resource_id=GROUP_ID)
    mock_query.assert_called_once_with(sdk, account_group_id_filter="explicit-id")


# ---------------------------------------------------------------------------
# Wrapper-level test: server.py forwards resource_id
# ---------------------------------------------------------------------------

FAKE_CREDS = {
    "account_id": "acct-test",
    "username": "user",
    "password": "pass",
}


def test_wrapper_forwards_resource_id():
    """manage_account_groups.fn() must pass resource_id through to the action handler."""
    mock_action = MagicMock(return_value={"_success": True})
    with (
        patch.object(server, "get_current_user", return_value="test-user"),
        patch.object(server, "get_secret", return_value=FAKE_CREDS),
        patch.object(server, "Boomi", return_value=MagicMock()),
        patch.object(server, "manage_account_groups_action", mock_action),
    ):
        server.manage_account_groups(
            profile="dev", action="list_accounts", resource_id=GROUP_ID
        )
    _, kwargs = mock_action.call_args
    assert kwargs.get("resource_id") == GROUP_ID


# ---------------------------------------------------------------------------
# End-to-end tests: fn() -> router -> handler -> query helper
# Mocks only at the query layer so the full dispatch chain executes.
# ---------------------------------------------------------------------------


@patch("boomi_mcp.categories.account_groups._query_all_account_group_accounts", return_value=[])
def test_e2e_list_accounts_resource_id_reaches_query(mock_query):
    """Full path: fn(resource_id) -> router merge -> handler fallback -> query filter."""
    with (
        patch.object(server, "get_current_user", return_value="test-user"),
        patch.object(server, "get_secret", return_value=FAKE_CREDS),
        patch.object(server, "Boomi", return_value=MagicMock()),
    ):
        result = server.manage_account_groups(
            profile="dev", action="list_accounts", resource_id=GROUP_ID
        )
    assert result["_success"] is True
    mock_query.assert_called_once()
    _, qkwargs = mock_query.call_args
    assert qkwargs["account_group_id_filter"] == GROUP_ID


@patch("boomi_mcp.categories.account_groups._query_all_account_group_user_roles", return_value=[])
def test_e2e_list_user_roles_resource_id_reaches_query(mock_query):
    """Full path: fn(resource_id) -> router merge -> handler fallback -> query filter."""
    with (
        patch.object(server, "get_current_user", return_value="test-user"),
        patch.object(server, "get_secret", return_value=FAKE_CREDS),
        patch.object(server, "Boomi", return_value=MagicMock()),
    ):
        result = server.manage_account_groups(
            profile="dev", action="list_user_roles", resource_id=GROUP_ID
        )
    assert result["_success"] is True
    _, qkwargs = mock_query.call_args
    assert qkwargs["account_group_id_filter"] == GROUP_ID


@patch("boomi_mcp.categories.account_groups._query_all_account_group_integration_packs", return_value=[])
def test_e2e_list_integration_packs_resource_id_reaches_query(mock_query):
    """Full path: fn(resource_id) -> router merge -> handler fallback -> query filter."""
    with (
        patch.object(server, "get_current_user", return_value="test-user"),
        patch.object(server, "get_secret", return_value=FAKE_CREDS),
        patch.object(server, "Boomi", return_value=MagicMock()),
    ):
        result = server.manage_account_groups(
            profile="dev", action="list_integration_packs", resource_id=GROUP_ID
        )
    assert result["_success"] is True
    _, qkwargs = mock_query.call_args
    assert qkwargs["account_group_id_filter"] == GROUP_ID
