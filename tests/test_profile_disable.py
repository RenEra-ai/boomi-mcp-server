"""Tests for per-profile Disable/Enable (hide a profile from LLM/MCP without deleting it).

Covers the credential-resolution chokepoint and the LLM-facing listing/resolution
tools. The web routes (/api/profiles shape, the toggle endpoint, /web/logout) only
register in the OAuth branch and are validated live by the QA layer, not here.
"""

import os
import sys
from pathlib import Path
from unittest.mock import patch

import pytest

_project_root = str(Path(__file__).resolve().parent.parent)
if _project_root not in sys.path:
    sys.path.insert(0, _project_root)

os.environ["BOOMI_LOCAL"] = "true"

import server  # noqa: E402


def _call_tool(tool, **kwargs):
    """Call an MCP tool whether it's wrapped (FunctionTool.fn) or a plain function."""
    fn = getattr(tool, "fn", tool)
    return fn(**kwargs)


_ENABLED = {"username": "BOOMI_TOKEN.u@x.com", "password": "tok", "account_id": "acct-1"}
_DISABLED = {**_ENABLED, "disabled": True}
_EXPLICIT_ENABLED = {**_ENABLED, "disabled": False}


# --------------------------------------------------------------------------- #
# get_secret enforcement (the single chokepoint all MCP tools funnel through)
# --------------------------------------------------------------------------- #

def test_get_secret_raises_on_disabled():
    with patch.object(server.secrets_backend, "get_secret", return_value=dict(_DISABLED)):
        with pytest.raises(server.DisabledProfileError):
            server.get_secret("sub", "prod")


def test_get_secret_allow_disabled_reads_through():
    with patch.object(server.secrets_backend, "get_secret", return_value=dict(_DISABLED)):
        creds = server.get_secret("sub", "prod", allow_disabled=True)
    assert creds["account_id"] == "acct-1"
    assert creds["disabled"] is True


def test_get_secret_returns_enabled():
    with patch.object(server.secrets_backend, "get_secret", return_value=dict(_ENABLED)):
        creds = server.get_secret("sub", "prod")
    assert creds["account_id"] == "acct-1"


def test_disabled_profile_error_is_valueerror():
    # boomi_account_info and CredentialStore catch ValueError; subclassing keeps them working.
    assert issubclass(server.DisabledProfileError, ValueError)


# --------------------------------------------------------------------------- #
# _is_profile_disabled helper
# --------------------------------------------------------------------------- #

def test_is_profile_disabled_true_false_and_missing():
    with patch.object(server.secrets_backend, "get_secret", return_value=dict(_DISABLED)):
        assert server._is_profile_disabled("sub", "p") is True
    with patch.object(server.secrets_backend, "get_secret", return_value=dict(_EXPLICIT_ENABLED)):
        assert server._is_profile_disabled("sub", "p") is False
    with patch.object(server.secrets_backend, "get_secret", return_value=dict(_ENABLED)):
        # no "disabled" key → treated as enabled (backward compatible)
        assert server._is_profile_disabled("sub", "p") is False


def test_is_profile_disabled_swallows_read_errors():
    with patch.object(server.secrets_backend, "get_secret", side_effect=ValueError("not found")):
        # A transient read failure must never hide a profile.
        assert server._is_profile_disabled("sub", "p") is False


# --------------------------------------------------------------------------- #
# list_boomi_profiles (LLM tool) hides disabled profiles
# --------------------------------------------------------------------------- #

def _backend_get_secret_by_name(mapping):
    def _inner(sub, profile):
        return dict(mapping[profile])
    return _inner


def test_list_boomi_profiles_excludes_disabled():
    profiles = [{"profile": "alpha"}, {"profile": "beta"}]
    mapping = {"alpha": _ENABLED, "beta": _DISABLED}
    with patch.object(server, "get_current_user", return_value="sub"), \
         patch.object(server, "list_profiles", return_value=profiles), \
         patch.object(server.secrets_backend, "get_secret", side_effect=_backend_get_secret_by_name(mapping)):
        result = _call_tool(server.list_boomi_profiles)
    assert result["_success"] is True
    assert result["profiles"] == ["alpha"]
    assert result["count"] == 1


def test_list_boomi_profiles_includes_enabled_without_flag():
    profiles = [{"profile": "alpha"}, {"profile": "beta"}]
    mapping = {"alpha": _ENABLED, "beta": _EXPLICIT_ENABLED}
    with patch.object(server, "get_current_user", return_value="sub"), \
         patch.object(server, "list_profiles", return_value=profiles), \
         patch.object(server.secrets_backend, "get_secret", side_effect=_backend_get_secret_by_name(mapping)):
        result = _call_tool(server.list_boomi_profiles)
    assert sorted(result["profiles"]) == ["alpha", "beta"]
    assert result["count"] == 2


# --------------------------------------------------------------------------- #
# A tool rejects a disabled profile with a clean envelope (no crash)
# --------------------------------------------------------------------------- #

def test_boomi_account_info_rejects_disabled_profile():
    with patch.object(server, "get_current_user", return_value="sub"), \
         patch.object(server.secrets_backend, "get_secret", return_value=dict(_DISABLED)):
        result = _call_tool(server.boomi_account_info, profile="prod")
    assert result["_success"] is False
    assert "disabled" in result["error"].lower()
