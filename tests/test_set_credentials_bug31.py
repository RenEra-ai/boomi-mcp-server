"""Regression tests for BUG-31: set_boomi_credentials validation and URL sanitization.

set_boomi_credentials must:
- Reject empty/whitespace-only parameters with a clear error.
- Reject account_id with invalid characters (e.g. slashes, spaces).
- Warn when username doesn't start with 'BOOMI_TOKEN.'.
- Never leak raw URLs or file paths in error responses.
"""

import os
import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

_project_root = str(Path(__file__).resolve().parent.parent)
if _project_root not in sys.path:
    sys.path.insert(0, _project_root)

os.environ["BOOMI_LOCAL"] = "true"

import server  # noqa: E402


# ---------------------------------------------------------------------------
# helpers
# ---------------------------------------------------------------------------

@pytest.fixture()
def _mock_auth():
    """Patch auth so the wrapper never hits real services."""
    with patch.object(server, "get_current_user", return_value="test-user"):
        yield


def _call(**kwargs):
    return server.set_boomi_credentials(**kwargs)


# ---------------------------------------------------------------------------
# empty / whitespace parameter rejection (plan step 1)
# ---------------------------------------------------------------------------


def test_empty_account_id_rejected(_mock_auth):
    result = _call(profile="dev", account_id="", username="BOOMI_TOKEN.u", password="pass")
    assert result["_success"] is False
    assert "account_id" in result["error"]


def test_whitespace_account_id_rejected(_mock_auth):
    result = _call(profile="dev", account_id="   ", username="BOOMI_TOKEN.u", password="pass")
    assert result["_success"] is False
    assert "account_id" in result["error"]


def test_empty_profile_rejected(_mock_auth):
    result = _call(profile="", account_id="acct-123", username="BOOMI_TOKEN.u", password="pass")
    assert result["_success"] is False
    assert "profile" in result["error"]


def test_empty_username_rejected(_mock_auth):
    result = _call(profile="dev", account_id="acct-123", username="", password="pass")
    assert result["_success"] is False
    assert "username" in result["error"]


def test_empty_password_rejected(_mock_auth):
    result = _call(profile="dev", account_id="acct-123", username="BOOMI_TOKEN.u", password="")
    assert result["_success"] is False
    assert "password" in result["error"]


def test_multiple_empty_params_all_listed(_mock_auth):
    result = _call(profile="", account_id="", username="", password="")
    assert result["_success"] is False
    for name in ("profile", "account_id", "username", "password"):
        assert name in result["error"]


# ---------------------------------------------------------------------------
# account_id format validation (plan step 2)
# ---------------------------------------------------------------------------


def test_account_id_with_slashes_rejected(_mock_auth):
    result = _call(profile="dev", account_id="acct/../../etc", username="BOOMI_TOKEN.u", password="pass")
    assert result["_success"] is False
    assert "invalid characters" in result["error"]


def test_account_id_with_spaces_rejected(_mock_auth):
    result = _call(profile="dev", account_id="acct 123", username="BOOMI_TOKEN.u", password="pass")
    assert result["_success"] is False
    assert "invalid characters" in result["error"]


def test_account_id_valid_formats_accepted(_mock_auth):
    """Valid account_ids should pass format check (may fail at SDK validation, which is fine)."""
    from boomi.net.transport.api_error import ApiError

    for acct_id in ("company-ABC123", "test_account", "Simple123"):
        with patch.object(server, "Boomi") as mock_boomi:
            mock_sdk = MagicMock()
            mock_boomi.return_value = mock_sdk
            # Simulate a successful credential validation
            mock_sdk.account.get_account.return_value = MagicMock()
            with patch.object(server, "put_secret"):
                result = _call(profile="dev", account_id=acct_id, username="BOOMI_TOKEN.u", password="pass")
        # Should not fail on format validation
        assert "invalid characters" not in result.get("error", "")


# ---------------------------------------------------------------------------
# BOOMI_TOKEN. username warning (plan step 3)
# ---------------------------------------------------------------------------


def test_username_without_boomi_token_prefix_warns(_mock_auth):
    """Should succeed but include a warning when username lacks BOOMI_TOKEN. prefix."""
    with patch.object(server, "Boomi") as mock_boomi:
        mock_sdk = MagicMock()
        mock_boomi.return_value = mock_sdk
        mock_sdk.account.get_account.return_value = MagicMock()
        with patch.object(server, "put_secret"):
            result = _call(profile="dev", account_id="acct-123", username="plain_user", password="pass")
    assert result["_success"] is True
    assert "_warning" in result
    assert "BOOMI_TOKEN." in result["_warning"]


def test_username_with_boomi_token_prefix_no_warning(_mock_auth):
    """Should succeed without warning when username has BOOMI_TOKEN. prefix."""
    with patch.object(server, "Boomi") as mock_boomi:
        mock_sdk = MagicMock()
        mock_boomi.return_value = mock_sdk
        mock_sdk.account.get_account.return_value = MagicMock()
        with patch.object(server, "put_secret"):
            result = _call(profile="dev", account_id="acct-123", username="BOOMI_TOKEN.mytoken", password="pass")
    assert result["_success"] is True
    assert "_warning" not in result


# ---------------------------------------------------------------------------
# URL / path sanitization in error messages (plan step 4)
# ---------------------------------------------------------------------------


def test_api_error_does_not_leak_url(_mock_auth):
    """ApiError containing a URL must have it redacted before returning."""
    from boomi.net.transport.api_error import ApiError

    fake_err = ApiError(message="Not Found: https://api.boomi.com/api/rest/v1//Account/")
    with patch.object(server, "Boomi") as mock_boomi:
        mock_sdk = MagicMock()
        mock_boomi.return_value = mock_sdk
        mock_sdk.account.get_account.side_effect = fake_err
        result = _call(profile="dev", account_id="acct-valid", username="BOOMI_TOKEN.u", password="pass")

    assert result["_success"] is False
    assert "https://" not in result["error"]
    assert "api.boomi.com" not in result["error"]


def test_generic_exception_does_not_leak_url(_mock_auth):
    """Generic exceptions containing URLs must also be sanitized."""
    with patch.object(server, "Boomi") as mock_boomi:
        mock_sdk = MagicMock()
        mock_boomi.return_value = mock_sdk
        mock_sdk.account.get_account.side_effect = ConnectionError(
            "Failed to connect to https://api.boomi.com/api/rest/v1/bad-account/Account/"
        )
        result = _call(profile="dev", account_id="acct-valid", username="BOOMI_TOKEN.u", password="pass")

    assert result["_success"] is False
    assert "https://" not in result["error"]
    assert "api.boomi.com" not in result["error"]


# ---------------------------------------------------------------------------
# _sanitize_error_msg unit tests
# ---------------------------------------------------------------------------


def test_sanitize_strips_https_urls():
    msg = "Error at https://api.boomi.com/api/rest/v1//Account/ was bad"
    result = server._sanitize_error_msg(msg)
    assert "https://" not in result
    assert "<redacted-url>" in result


def test_sanitize_strips_http_urls():
    msg = "Error at http://internal.host:8080/path was bad"
    result = server._sanitize_error_msg(msg)
    assert "http://" not in result
    assert "<redacted-url>" in result


def test_sanitize_strips_absolute_paths():
    msg = "Permission denied: /home/user/.boomi_mcp_local_secrets.json"
    result = server._sanitize_error_msg(msg)
    assert "/.boomi_mcp" not in result
    assert "<redacted-path>" in result


def test_sanitize_preserves_non_url_text():
    msg = "Authentication failed for user"
    result = server._sanitize_error_msg(msg)
    assert result == msg


# ---------------------------------------------------------------------------
# src/boomi_mcp/sanitize.py — shared sanitizer used by tools.py
# ---------------------------------------------------------------------------


def test_boomi_mcp_sanitize_strips_paths():
    """The shared sanitizer used by tools.py must redact absolute file paths."""
    from boomi_mcp.sanitize import sanitize_error_msg
    result = sanitize_error_msg("[Errno 13] Permission denied: '/home/user/.boomi_mcp_local_secrets.json'")
    assert "/.boomi_mcp" not in result
    assert "<redacted-path>" in result


def test_boomi_mcp_sanitize_strips_urls():
    """The shared sanitizer used by tools.py must redact URLs."""
    from boomi_mcp.sanitize import sanitize_error_msg
    result = sanitize_error_msg("Connection refused: https://vault.internal/v1/secrets")
    assert "https://" not in result
    assert "<redacted-url>" in result


def test_boomi_mcp_sanitize_preserves_clean_text():
    """Clean messages should pass through unchanged."""
    from boomi_mcp.sanitize import sanitize_error_msg
    msg = "Authentication failed for user"
    assert sanitize_error_msg(msg) == msg
