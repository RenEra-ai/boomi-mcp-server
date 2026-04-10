"""Regression test for BUG-01: _action_assign_user_role request shape.

Verifies that AccountUserRole is constructed with user_id, role_id,
and account_id sourced from sdk._base_url_account_id.
"""

from unittest.mock import MagicMock, patch

from boomi.models import AccountUserRole

from boomi_mcp.categories.account import _action_assign_user_role


def _make_sdk(account_id="ACCT-123"):
    sdk = MagicMock()
    sdk._base_url_account_id = account_id
    sdk.account_user_role.create_account_user_role.return_value = MagicMock(
        user_id="user-1",
        role_id="role-1",
        account_id=account_id,
    )
    return sdk


def test_request_body_contains_required_fields():
    sdk = _make_sdk(account_id="ACCT-99")
    result = _action_assign_user_role(
        sdk, profile="dev", user_id="user-1", role_id="role-1"
    )

    assert result["_success"] is True
    call_args = sdk.account_user_role.create_account_user_role.call_args
    body = call_args.kwargs["request_body"]

    assert isinstance(body, AccountUserRole)
    assert body.user_id == "user-1"
    assert body.role_id == "role-1"
    assert body.account_id == "ACCT-99"


def test_missing_user_id():
    sdk = _make_sdk()
    result = _action_assign_user_role(sdk, profile="dev", role_id="role-1")
    assert result["_success"] is False
    assert "user_id" in result["error"]


def test_missing_role_id():
    sdk = _make_sdk()
    result = _action_assign_user_role(sdk, profile="dev", user_id="user-1")
    assert result["_success"] is False
    assert "role_id" in result["error"]
