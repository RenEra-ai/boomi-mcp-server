"""Unit tests for troubleshoot_execution (batch-08 bugfixes + cancel action)."""

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
    fn = getattr(tool, "fn", tool)
    return fn(**kwargs)


def _assert_no_leak(error_msg):
    """Assert that an error message does not leak raw SDK repr patterns."""
    assert "ApiError(" not in error_msg
    assert "response=<" not in error_msg
    assert "object at 0x" not in error_msg


# ── QA-024: error_details ApiError sanitization ──────────────────────


class TestErrorDetailsApiError:
    @patch("server.get_secret", return_value=FAKE_CREDS)
    @patch("server.Boomi")
    @patch("server.get_current_user", return_value="test-user")
    def test_invalid_execution_id_clean_error(self, _user, mock_boomi_cls, _creds):
        mock_sdk = MagicMock()
        mock_sdk.execution_record.query_execution_record.side_effect = (
            _fake_api_error(detail="Execution not found")
        )
        mock_boomi_cls.return_value = mock_sdk

        result = _call_tool(
            server.troubleshoot_execution,
            profile="dev",
            action="error_details",
            execution_id="fake-exec-id-12345",
        )

        assert result["_success"] is False
        _assert_no_leak(result.get("error", ""))

    @patch("server.get_secret", return_value=FAKE_CREDS)
    @patch("server.Boomi")
    @patch("server.get_current_user", return_value="test-user")
    def test_body_message_extracted(self, _user, mock_boomi_cls, _creds):
        mock_sdk = MagicMock()
        mock_sdk.execution_record.query_execution_record.side_effect = (
            _fake_api_error(body_message="400 Bad Request: invalid execution ID")
        )
        mock_boomi_cls.return_value = mock_sdk

        result = _call_tool(
            server.troubleshoot_execution,
            profile="dev",
            action="error_details",
            execution_id="fake-exec-id-12345",
        )

        assert result["_success"] is False
        assert "400 Bad Request" in result.get("error", "")
        _assert_no_leak(result.get("error", ""))


# ── QA-025: retry ApiError sanitization ──────────────────────────────


class TestRetryApiError:
    @patch("server.get_secret", return_value=FAKE_CREDS)
    @patch("server.Boomi")
    @patch("server.get_current_user", return_value="test-user")
    def test_invalid_execution_id_clean_error(self, _user, mock_boomi_cls, _creds):
        mock_sdk = MagicMock()
        # retry calls _query_execution_record → sdk.execution_record.query_execution_record
        mock_sdk.execution_record.query_execution_record.side_effect = (
            _fake_api_error(detail="Execution record not found")
        )
        mock_boomi_cls.return_value = mock_sdk

        result = _call_tool(
            server.troubleshoot_execution,
            profile="dev",
            action="retry",
            execution_id="fake-exec-id-12345",
        )

        assert result["_success"] is False
        _assert_no_leak(result.get("error", ""))


# ── QA-026: reprocess ApiError sanitization ──────────────────────────


class TestReprocessApiError:
    @patch("server.get_secret", return_value=FAKE_CREDS)
    @patch("server.Boomi")
    @patch("server.get_current_user", return_value="test-user")
    def test_invalid_execution_id_clean_error(self, _user, mock_boomi_cls, _creds):
        mock_sdk = MagicMock()
        # reprocess calls _query_execution_record → sdk.execution_record.query_execution_record
        mock_sdk.execution_record.query_execution_record.side_effect = (
            _fake_api_error(detail="Execution record not found")
        )
        mock_boomi_cls.return_value = mock_sdk

        result = _call_tool(
            server.troubleshoot_execution,
            profile="dev",
            action="reprocess",
            execution_id="fake-exec-id-12345",
        )

        assert result["_success"] is False
        _assert_no_leak(result.get("error", ""))

    @patch("server.get_secret", return_value=FAKE_CREDS)
    @patch("server.Boomi")
    @patch("server.get_current_user", return_value="test-user")
    def test_body_message_extracted(self, _user, mock_boomi_cls, _creds):
        mock_sdk = MagicMock()
        mock_sdk.execution_record.query_execution_record.side_effect = (
            _fake_api_error(body_message="Resource does not exist")
        )
        mock_boomi_cls.return_value = mock_sdk

        result = _call_tool(
            server.troubleshoot_execution,
            profile="dev",
            action="reprocess",
            execution_id="fake-exec-id-12345",
        )

        assert result["_success"] is False
        assert "Resource does not exist" in result.get("error", "")
        _assert_no_leak(result.get("error", ""))


# ── Cancel action tests ──────────────────────────────────────────────

from src.boomi_mcp.categories.troubleshooting import handle_cancel


class TestCancelAction:
    def test_cancel_success(self):
        sdk = MagicMock()
        result = handle_cancel(sdk, execution_id="exec-123", config={})
        assert result["_success"] is True
        assert result["execution_id"] == "exec-123"
        assert result["message"] == "Cancel request submitted"
        sdk.cancel_execution.cancel_execution.assert_called_once_with(execution_id="exec-123")

    def test_cancel_missing_execution_id(self):
        sdk = MagicMock()
        result = handle_cancel(sdk, execution_id=None, config={})
        assert result["_success"] is False
        assert "execution_id is required" in result["error"]

    def test_cancel_empty_execution_id(self):
        sdk = MagicMock()
        result = handle_cancel(sdk, execution_id="", config={})
        assert result["_success"] is False
        assert "execution_id is required" in result["error"]

    def test_cancel_api_error(self):
        sdk = MagicMock()
        sdk.cancel_execution.cancel_execution.side_effect = (
            _fake_api_error(detail="Execution not found")
        )
        result = handle_cancel(sdk, execution_id="exec-123", config={})
        assert result["_success"] is False
        assert "Execution not found" in result["error"]
        _assert_no_leak(result["error"])

    def test_cancel_api_error_body_message(self):
        sdk = MagicMock()
        sdk.cancel_execution.cancel_execution.side_effect = (
            _fake_api_error(body_message="Cannot cancel completed execution")
        )
        result = handle_cancel(sdk, execution_id="exec-123", config={})
        assert result["_success"] is False
        assert "Cannot cancel completed execution" in result["error"]
        _assert_no_leak(result["error"])

    def test_cancel_general_exception(self):
        sdk = MagicMock()
        sdk.cancel_execution.cancel_execution.side_effect = RuntimeError("connection lost")
        result = handle_cancel(sdk, execution_id="exec-123", config={})
        assert result["_success"] is False
        assert "connection lost" in result["error"]

    @patch("server.get_secret", return_value=FAKE_CREDS)
    @patch("server.Boomi")
    @patch("server.get_current_user", return_value="test-user")
    def test_cancel_via_router(self, _user, mock_boomi_cls, _creds):
        """Test cancel action dispatched through the troubleshoot_execution tool."""
        mock_sdk = MagicMock()
        mock_boomi_cls.return_value = mock_sdk
        result = _call_tool(
            server.troubleshoot_execution,
            profile="dev",
            action="cancel",
            execution_id="exec-456",
        )
        assert result["_success"] is True
        assert result["execution_id"] == "exec-456"
        mock_sdk.cancel_execution.cancel_execution.assert_called_once_with(execution_id="exec-456")
