"""Unit tests for execute_process batch-08 bugfixes (QA-027/028)."""

import json
import os
import sys
from types import SimpleNamespace
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


# ── QA-027: atom_id without environment_id should not TypeError ──────


class TestExecuteProcessAtomOnly:
    @patch("server.get_secret", return_value=FAKE_CREDS)
    @patch("server.Boomi")
    @patch("server.get_current_user", return_value="test-user")
    def test_atom_id_only_reaches_sdk(self, _user, mock_boomi_cls, _creds):
        """Providing atom_id without environment_id should submit execution."""
        mock_sdk = MagicMock()
        mock_result = MagicMock()
        mock_result.request_id = "req-abc-123"
        mock_sdk.execution_request.create_execution_request.return_value = mock_result
        mock_boomi_cls.return_value = mock_sdk

        result = _call_tool(
            server.execute_process,
            profile="dev",
            process_id="proc-123",
            atom_id="atom-789",
        )

        assert result["_success"] is True
        assert result["request_id"] == "req-abc-123"
        assert result["atom_id"] == "atom-789"
        # Should NOT have called _resolve_atom_id
        mock_sdk.environment_atom_attachment.query_environment_atom_attachment.assert_not_called()

    @patch("server.get_secret", return_value=FAKE_CREDS)
    @patch("server.Boomi")
    @patch("server.get_current_user", return_value="test-user")
    def test_atom_id_only_no_type_error(self, _user, mock_boomi_cls, _creds):
        """Ensure no TypeError when environment_id is omitted."""
        mock_sdk = MagicMock()
        mock_result = MagicMock()
        mock_result.request_id = "req-abc-123"
        mock_sdk.execution_request.create_execution_request.return_value = mock_result
        mock_boomi_cls.return_value = mock_sdk

        # This previously raised TypeError
        result = _call_tool(
            server.execute_process,
            profile="dev",
            process_id="proc-123",
            atom_id="atom-789",
        )

        assert isinstance(result, dict)
        assert "TypeError" not in str(result)


# ── QA-028: missing both atom_id and environment_id ──────────────────


class TestExecuteProcessMissingBoth:
    @patch("server.get_secret", return_value=FAKE_CREDS)
    @patch("server.Boomi")
    @patch("server.get_current_user", return_value="test-user")
    def test_clean_validation_error(self, _user, mock_boomi_cls, _creds):
        """Missing both atom_id and environment_id returns clean error."""
        mock_sdk = MagicMock()
        mock_boomi_cls.return_value = mock_sdk

        result = _call_tool(
            server.execute_process,
            profile="dev",
            process_id="proc-123",
        )

        assert result["_success"] is False
        assert "atom_id" in result["error"]
        assert "environment_id" in result["error"]
        # Must not be a TypeError
        assert "TypeError" not in str(result)


# ── Existing auto-resolution path still works ────────────────────────


class TestExecuteProcessAutoResolve:
    @patch("server.get_secret", return_value=FAKE_CREDS)
    @patch("server.Boomi")
    @patch("server.get_current_user", return_value="test-user")
    def test_environment_id_resolves_atom(self, _user, mock_boomi_cls, _creds):
        """environment_id without atom_id triggers _resolve_atom_id."""
        mock_sdk = MagicMock()

        # Mock the atom attachment query for _resolve_atom_id
        attachment = MagicMock()
        attachment.atom_id = "resolved-atom-456"
        query_result = MagicMock()
        query_result.result = [attachment]
        query_result.query_token = None
        mock_sdk.environment_atom_attachment.query_environment_atom_attachment.return_value = query_result

        # Mock execution request
        exec_result = MagicMock()
        exec_result.request_id = "req-xyz-789"
        mock_sdk.execution_request.create_execution_request.return_value = exec_result

        mock_boomi_cls.return_value = mock_sdk

        result = _call_tool(
            server.execute_process,
            profile="dev",
            process_id="proc-123",
            environment_id="env-456",
        )

        assert result["_success"] is True
        assert result["atom_id"] == "resolved-atom-456"
        # Should have called the attachment query
        mock_sdk.environment_atom_attachment.query_environment_atom_attachment.assert_called_once()
