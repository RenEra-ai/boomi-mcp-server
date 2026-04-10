"""Regression tests for BUG-40: remove_user_role safety gates.

Covers:
- P0: resource_id without user_id is blocked
- P1: critical-role lookup failure blocks the delete (fail closed)
- Existing gates: confirm_remove required, last-role blocked, critical-role blocked
"""

from unittest.mock import MagicMock, patch

from boomi_mcp.categories.account import _action_remove_user_role


def _make_sdk(user_roles=None, role_name="Custom Role"):
    """Build a mock SDK with configurable user-role query results and role lookup."""
    sdk = MagicMock()

    # Default: user has two roles
    if user_roles is None:
        user_roles = [
            MagicMock(id_="assoc-1", user_id="usr-1", role_id="role-1",
                      account_id="ACCT-1", first_name=None, last_name=None, notify_user=None),
            MagicMock(id_="assoc-2", user_id="usr-1", role_id="role-2",
                      account_id="ACCT-1", first_name=None, last_name=None, notify_user=None),
        ]

    query_resp = MagicMock()
    query_resp.result = user_roles
    query_resp.query_token = None
    sdk.account_user_role.query_account_user_role.return_value = query_resp

    role_obj = MagicMock()
    role_obj.name = role_name
    sdk.role.get_role.return_value = role_obj

    return sdk


# --- P0: resource_id-only bypasses safety gates ---

def test_resource_id_without_user_id_is_blocked():
    sdk = _make_sdk()
    result = _action_remove_user_role(
        sdk, profile="dev",
        resource_id="assoc-1", confirm_remove=True,
    )
    assert result["_success"] is False
    assert "user_id is required" in result["error"]
    sdk.account_user_role.delete_account_user_role.assert_not_called()


def test_resource_id_with_mismatched_user_id_is_blocked():
    # Simulate querying roles for the wrong user: returns roles that don't
    # include the target resource_id (assoc-target belongs to someone else)
    other_user_roles = [
        MagicMock(id_="assoc-X", user_id="usr-other", role_id="role-1",
                  account_id="ACCT-1", first_name=None, last_name=None, notify_user=None),
        MagicMock(id_="assoc-Y", user_id="usr-other", role_id="role-2",
                  account_id="ACCT-1", first_name=None, last_name=None, notify_user=None),
    ]
    sdk = _make_sdk(user_roles=other_user_roles)
    result = _action_remove_user_role(
        sdk, profile="dev",
        resource_id="assoc-target", user_id="usr-other", confirm_remove=True,
    )
    assert result["_success"] is False
    assert "does not belong to" in result["error"]
    sdk.account_user_role.delete_account_user_role.assert_not_called()


def test_resource_id_with_user_id_proceeds():
    sdk = _make_sdk(role_name="Custom Role")
    result = _action_remove_user_role(
        sdk, profile="dev",
        resource_id="assoc-1", user_id="usr-1", confirm_remove=True,
    )
    assert result["_success"] is True
    sdk.account_user_role.delete_account_user_role.assert_called_once_with(id_="assoc-1")


# --- P1: critical-role lookup failure fails closed ---

def test_role_lookup_failure_blocks_delete():
    sdk = _make_sdk()
    sdk.role.get_role.side_effect = Exception("API timeout")
    result = _action_remove_user_role(
        sdk, profile="dev",
        resource_id="assoc-1", user_id="usr-1", confirm_remove=True,
    )
    assert result["_success"] is False
    assert "lookup failed" in result["error"]
    sdk.account_user_role.delete_account_user_role.assert_not_called()


# --- Existing gates: confirm_remove, last-role, critical-role ---

def test_missing_confirm_remove_is_blocked():
    sdk = _make_sdk()
    result = _action_remove_user_role(
        sdk, profile="dev",
        user_id="usr-1", role_id="role-1",
    )
    assert result["_success"] is False
    assert "confirm_remove" in result["error"]
    sdk.account_user_role.delete_account_user_role.assert_not_called()


def test_last_role_removal_is_blocked():
    single_role = [
        MagicMock(id_="assoc-1", user_id="usr-1", role_id="role-1",
                  account_id="ACCT-1", first_name=None, last_name=None, notify_user=None),
    ]
    sdk = _make_sdk(user_roles=single_role, role_name="Custom Role")
    result = _action_remove_user_role(
        sdk, profile="dev",
        user_id="usr-1", role_id="role-1", confirm_remove=True,
    )
    assert result["_success"] is False
    assert "last remaining role" in result["error"]
    sdk.account_user_role.delete_account_user_role.assert_not_called()


def test_critical_role_removal_is_blocked():
    sdk = _make_sdk(role_name="Administrator")
    result = _action_remove_user_role(
        sdk, profile="dev",
        user_id="usr-1", role_id="role-1", confirm_remove=True,
    )
    assert result["_success"] is False
    assert "critical role" in result["error"].lower()
    sdk.account_user_role.delete_account_user_role.assert_not_called()


def test_forged_role_id_does_not_bypass_critical_gate():
    """BUG-40 P0: caller supplies a benign role_id to dodge the critical-role check.

    The association (assoc-1) actually maps to role-1 (Administrator).
    The caller forges role_id='normal-role' hoping the handler looks up that
    benign role instead.  The handler must derive the role from the resolved
    association and block the delete.
    """
    sdk = _make_sdk(role_name="Administrator")  # role-1 -> Administrator
    result = _action_remove_user_role(
        sdk, profile="dev",
        resource_id="assoc-1", user_id="usr-1",
        role_id="normal-role",  # forged benign role
        confirm_remove=True,
    )
    assert result["_success"] is False
    assert "critical role" in result["error"].lower()
    # Must look up role-1 (from association), not normal-role (from caller)
    sdk.role.get_role.assert_called_once_with(id_="role-1")
    sdk.account_user_role.delete_account_user_role.assert_not_called()
