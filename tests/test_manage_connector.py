"""Unit tests for manage_connector batch-06 bugfixes (mocked SDK)."""

import json
import os
import sys
from unittest.mock import MagicMock, patch

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

os.environ["BOOMI_LOCAL"] = "true"

import server


# ── Helpers ──────────────────────────────────────────────────────────

FAKE_CREDS = {
    "account_id": "acct-test-123",
    "username": "BOOMI_TOKEN.user@example.com",
    "password": "tok-secret",
    "base_url": None,
}


def _call_tool(tool, **kwargs):
    fn = getattr(tool, "fn", tool)
    return fn(**kwargs)


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


def _assert_no_leak(error_msg):
    """Assert that an error message does not leak raw SDK repr patterns."""
    assert "ApiError(" not in error_msg
    assert "response=<" not in error_msg
    assert "object at 0x" not in error_msg


# ── QA-018: manage_connector get_type error cleanup ──────────────────


class TestConnectorGetTypeErrorCleanup:
    """get_type should return clean error messages, not raw ApiError repr."""

    @patch("server.get_secret", return_value=FAKE_CREDS)
    @patch("server.Boomi")
    @patch("server.get_current_user", return_value="test-user")
    def test_api_error_detail_surfaces(self, _user, mock_boomi_cls, _creds):
        """ApiError with error_detail returns the detail text, not repr."""
        mock_sdk = MagicMock()
        mock_boomi_cls.return_value = mock_sdk
        mock_sdk.connector.get_connector.side_effect = _fake_api_error(
            detail="Connector type 'nonexistent-xyz' not found"
        )

        result = _call_tool(
            server.manage_connector,
            profile="dev",
            action="get_type",
            config=json.dumps({"connector_type": "nonexistent-xyz"}),
        )

        assert result["_success"] is False
        assert "nonexistent-xyz" in result["error"]
        assert "hint" in result
        assert "list_types" in result["hint"]
        _assert_no_leak(result["error"])

    @patch("server.get_secret", return_value=FAKE_CREDS)
    @patch("server.Boomi")
    @patch("server.get_current_user", return_value="test-user")
    def test_api_error_body_message_surfaces(self, _user, mock_boomi_cls, _creds):
        """ApiError with response.body.message returns the message, not repr."""
        mock_sdk = MagicMock()
        mock_boomi_cls.return_value = mock_sdk
        mock_sdk.connector.get_connector.side_effect = _fake_api_error(
            body_message="400 Bad Request"
        )

        result = _call_tool(
            server.manage_connector,
            profile="dev",
            action="get_type",
            config=json.dumps({"connector_type": "bad-type"}),
        )

        assert result["_success"] is False
        assert "400 Bad Request" in result["error"]
        _assert_no_leak(result["error"])


# ── QA-019: manage_connector get error cleanup ───────────────────────


class TestConnectorGetErrorCleanup:
    """get should return clean error messages, not raw ApiError repr."""

    @patch("server.get_secret", return_value=FAKE_CREDS)
    @patch("server.Boomi")
    @patch("server.get_current_user", return_value="test-user")
    @patch("boomi_mcp.categories.components.connectors.component_get_xml")
    def test_api_error_detail_surfaces(self, mock_get_xml, _user, mock_boomi_cls, _creds):
        """ApiError with error_detail returns the detail text, not repr."""
        mock_boomi_cls.return_value = MagicMock()
        mock_get_xml.side_effect = _fake_api_error(
            detail="ComponentId fake-connector-id is invalid"
        )

        result = _call_tool(
            server.manage_connector,
            profile="dev",
            action="get",
            component_id="fake-connector-id",
        )

        assert result["_success"] is False
        assert "fake-connector-id" in result["error"]
        assert "hint" in result
        _assert_no_leak(result["error"])

    @patch("server.get_secret", return_value=FAKE_CREDS)
    @patch("server.Boomi")
    @patch("server.get_current_user", return_value="test-user")
    @patch("boomi_mcp.categories.components.connectors.component_get_xml")
    def test_wrapped_helper_failure(self, mock_get_xml, _user, mock_boomi_cls, _creds):
        """Exception from component_get_xml is cleaned, not leaked."""
        mock_boomi_cls.return_value = MagicMock()
        mock_get_xml.side_effect = Exception("GET failed: HTTP 400 — ComponentId is invalid")

        result = _call_tool(
            server.manage_connector,
            profile="dev",
            action="get",
            component_id="bad-id",
        )

        assert result["_success"] is False
        assert "bad-id" in result["error"]
        _assert_no_leak(result["error"])


# ── QA-020: manage_connector update error cleanup ────────────────────


class TestConnectorUpdateErrorCleanup:
    """update should return clean error messages, not raw ApiError repr."""

    @patch("server.get_secret", return_value=FAKE_CREDS)
    @patch("server.Boomi")
    @patch("server.get_current_user", return_value="test-user")
    @patch("boomi_mcp.categories.components.connectors.component_get_xml")
    def test_api_error_on_smart_merge(self, mock_get_xml, _user, mock_boomi_cls, _creds):
        """ApiError during smart-merge (component_get_xml) returns clean error."""
        mock_boomi_cls.return_value = MagicMock()
        mock_get_xml.side_effect = _fake_api_error(
            detail="ComponentId fake-id is invalid"
        )

        result = _call_tool(
            server.manage_connector,
            profile="dev",
            action="update",
            component_id="fake-id",
            config=json.dumps({"url": "https://example.com"}),
        )

        assert result["_success"] is False
        assert "fake-id" in result["error"]
        _assert_no_leak(result["error"])


# ── QA-021: manage_connector delete error cleanup ────────────────────


class TestConnectorDeleteErrorCleanup:
    """delete should return clean error messages, not raw ApiError repr."""

    @patch("server.get_secret", return_value=FAKE_CREDS)
    @patch("server.Boomi")
    @patch("server.get_current_user", return_value="test-user")
    @patch("boomi_mcp.categories.components.connectors.soft_delete_component")
    def test_api_error_detail_surfaces(self, mock_delete, _user, mock_boomi_cls, _creds):
        """ApiError with error_detail returns the detail text, not repr."""
        mock_boomi_cls.return_value = MagicMock()
        mock_delete.side_effect = _fake_api_error(
            detail="ComponentId fake-id is invalid"
        )

        result = _call_tool(
            server.manage_connector,
            profile="dev",
            action="delete",
            component_id="fake-id",
        )

        assert result["_success"] is False
        assert "fake-id" in result["error"]
        assert "hint" in result
        _assert_no_leak(result["error"])
