"""Unit tests for top-level server.py account/profile tools (mocked SDK)."""

import json
import os
import sys
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

# Force local mode so server.py uses local secrets backend
os.environ["BOOMI_LOCAL"] = "true"

import server


# ── Helpers ──────────────────────────────────────────────────────────

def _fake_api_error(detail=None, body_message=None, message=None):
    """Build a fake ApiError-like exception with configurable attributes."""
    err = server.ApiError.__new__(server.ApiError)
    Exception.__init__(err, message or "raw repr fallback")
    err.error_detail = detail
    resp = MagicMock()
    resp.body = {"message": body_message} if body_message else {}
    err.response = resp
    err.message = message
    return err


FAKE_CREDS = {
    "account_id": "acct-test-123",
    "username": "BOOMI_TOKEN.user@example.com",
    "password": "tok-secret",
    "base_url": None,
}


def _call_tool(tool, **kwargs):
    """Call a FastMCP FunctionTool via .fn() or directly if plain function."""
    fn = getattr(tool, "fn", tool)
    return fn(**kwargs)


def _make_nested_account():
    """Build a fake account object with nested SDK-style child objects."""
    licensing = SimpleNamespace(type="enterprise", max_connections=100)
    molecule = SimpleNamespace(name="prod-molecule", status="running")
    account = SimpleNamespace(
        id_="acct-test-123",
        name="Test Account",
        status="active",
        licensing=licensing,
        molecule=molecule,
        date_created="2024-01-01",
        _private_field="should-be-excluded",
        empty_field=None,
    )
    return account


# ── set_boomi_credentials: ApiError sanitized ───────────────────────

class TestSetCredentialsApiError:
    @patch("server.put_secret")
    @patch("server.Boomi")
    @patch("server.get_current_user", return_value="test-user")
    def test_api_error_sanitized(self, _user, mock_boomi_cls, mock_put):
        mock_sdk = MagicMock()
        mock_sdk.account.get_account.side_effect = _fake_api_error(
            detail="Invalid credentials for this account"
        )
        mock_boomi_cls.return_value = mock_sdk

        result = _call_tool(
            server.set_boomi_credentials,
            profile="dev",
            account_id="acct-bad",
            username="BOOMI_TOKEN.bad",
            password="bad-tok",
        )

        assert result["_success"] is False
        mock_put.assert_not_called()

        error_msg = result["error"]
        assert "Invalid credentials for this account" in error_msg
        # Must not leak raw SDK repr patterns
        assert "ApiError(" not in error_msg
        assert "response=<" not in error_msg
        assert "object at 0x" not in error_msg


# ── boomi_account_info: success serialization ───────────────────────

class TestAccountInfoSuccessSerialization:
    @patch("server.Boomi")
    @patch("server.get_secret", return_value=FAKE_CREDS)
    @patch("server.get_current_user", return_value="test-user")
    def test_json_serializable(self, _user, _secret, mock_boomi_cls):
        mock_sdk = MagicMock()
        mock_sdk.account.get_account.return_value = _make_nested_account()
        mock_boomi_cls.return_value = mock_sdk

        result = _call_tool(server.boomi_account_info, profile="dev")

        assert result["_success"] is True
        # Must serialize without default=str
        serialized = json.dumps(result)
        assert isinstance(serialized, str)

    @patch("server.Boomi")
    @patch("server.get_secret", return_value=FAKE_CREDS)
    @patch("server.get_current_user", return_value="test-user")
    def test_nested_objects_are_dicts(self, _user, _secret, mock_boomi_cls):
        mock_sdk = MagicMock()
        mock_sdk.account.get_account.return_value = _make_nested_account()
        mock_boomi_cls.return_value = mock_sdk

        result = _call_tool(server.boomi_account_info, profile="dev")

        assert result["_success"] is True
        assert isinstance(result["licensing"], dict)
        assert result["licensing"]["type"] == "enterprise"
        assert isinstance(result["molecule"], dict)
        assert result["molecule"]["name"] == "prod-molecule"
        # Private and None fields excluded
        assert "_private_field" not in result
        assert "empty_field" not in result


# ── boomi_account_info: ApiError sanitized ───────────────────────────

class TestAccountInfoApiError:
    @patch("server.Boomi")
    @patch("server.get_secret", return_value=FAKE_CREDS)
    @patch("server.get_current_user", return_value="test-user")
    def test_api_error_sanitized(self, _user, _secret, mock_boomi_cls):
        mock_sdk = MagicMock()
        mock_sdk.account.get_account.side_effect = _fake_api_error(
            body_message="Unauthorized: invalid token"
        )
        mock_boomi_cls.return_value = mock_sdk

        result = _call_tool(server.boomi_account_info, profile="dev")

        assert result["_success"] is False
        assert result["account_id"] == "acct-test-123"

        error_msg = result["error"]
        assert "Unauthorized: invalid token" in error_msg
        assert "ApiError(" not in error_msg
        assert "response=<" not in error_msg
        assert "object at 0x" not in error_msg
