"""Regression tests for BUG-24: config_data fallback for atom_id / environment_id.

execute_process_action must accept atom_id and environment_id from config_data
when the top-level arguments are absent.  Top-level args always take precedence.

Also verifies the type guard: non-dict config_data must not crash with
AttributeError — it should be treated as empty.
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
from boomi_mcp.categories.execution import execute_process_action


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_sdk():
    return MagicMock()


# ---------------------------------------------------------------------------
# config_data fallback tests
# ---------------------------------------------------------------------------

@patch("boomi_mcp.categories.execution._resolve_atom_id", return_value=("atom-resolved", None))
@patch("boomi_mcp.categories.execution._build_dynamic_properties", return_value=None)
@patch("boomi_mcp.categories.execution._build_process_properties", return_value=None)
def test_environment_id_from_config_data(mock_pp, mock_dp, mock_resolve):
    """environment_id in config_data is used when top-level arg is absent."""
    sdk = _make_sdk()
    sdk.execution_request.create_execution_request.return_value = MagicMock(request_id="req-1")

    result = execute_process_action(
        sdk, profile="dev", process_id="proc-1",
        environment_id=None, atom_id=None,
        config_data={"environment_id": "env-from-config"},
    )

    mock_resolve.assert_called_once_with(sdk, "env-from-config")
    assert result.get("_success") is not False


@patch("boomi_mcp.categories.execution._build_dynamic_properties", return_value=None)
@patch("boomi_mcp.categories.execution._build_process_properties", return_value=None)
def test_atom_id_from_config_data(mock_pp, mock_dp):
    """atom_id in config_data is used when top-level arg is absent."""
    sdk = _make_sdk()
    sdk.execution_request.create_execution_request.return_value = MagicMock(request_id="req-1")

    result = execute_process_action(
        sdk, profile="dev", process_id="proc-1",
        environment_id=None, atom_id=None,
        config_data={"atom_id": "atom-from-config"},
    )

    # Should NOT try to resolve — atom_id was provided via config
    assert result.get("_success") is not False


@patch("boomi_mcp.categories.execution.ExecutionRequest")
@patch("boomi_mcp.categories.execution._build_dynamic_properties", return_value=None)
@patch("boomi_mcp.categories.execution._build_process_properties", return_value=None)
def test_top_level_atom_id_takes_precedence(mock_pp, mock_dp, mock_er):
    """Top-level atom_id takes precedence over config_data.atom_id."""
    sdk = _make_sdk()
    sdk.execution_request.create_execution_request.return_value = MagicMock(request_id="req-1")

    result = execute_process_action(
        sdk, profile="dev", process_id="proc-1",
        environment_id=None, atom_id="atom-top-level",
        config_data={"atom_id": "atom-config-should-be-ignored"},
    )

    # ExecutionRequest must receive the top-level atom_id, not the config one
    mock_er.assert_called_once()
    call_kwargs = mock_er.call_args
    assert call_kwargs[1]["atom_id"] == "atom-top-level"


@patch("boomi_mcp.categories.execution._resolve_atom_id", return_value=("atom-resolved", None))
@patch("boomi_mcp.categories.execution._build_dynamic_properties", return_value=None)
@patch("boomi_mcp.categories.execution._build_process_properties", return_value=None)
def test_top_level_environment_id_takes_precedence(mock_pp, mock_dp, mock_resolve):
    """Top-level environment_id takes precedence over config_data.environment_id."""
    sdk = _make_sdk()
    sdk.execution_request.create_execution_request.return_value = MagicMock(request_id="req-1")

    result = execute_process_action(
        sdk, profile="dev", process_id="proc-1",
        environment_id="env-top-level", atom_id=None,
        config_data={"environment_id": "env-config-should-be-ignored"},
    )

    mock_resolve.assert_called_once_with(sdk, "env-top-level")
    assert result.get("_success") is not False


# ---------------------------------------------------------------------------
# Missing both atom_id and environment_id
# ---------------------------------------------------------------------------

def test_missing_both_returns_structured_error():
    """Missing both atom_id and environment_id returns a structured error."""
    sdk = _make_sdk()
    result = execute_process_action(
        sdk, profile="dev", process_id="proc-1",
        environment_id=None, atom_id=None,
        config_data={},
    )

    assert result["_success"] is False
    assert "atom_id" in result["error"]
    assert "environment_id" in result["error"]


# ---------------------------------------------------------------------------
# config_data type guard (non-dict must not raise AttributeError)
# ---------------------------------------------------------------------------

def test_non_dict_config_data_does_not_crash():
    """Non-dict config_data is treated as empty — returns structured error, not AttributeError."""
    sdk = _make_sdk()

    for bad_value in ["a string", 42, [1, 2], True]:
        result = execute_process_action(
            sdk, profile="dev", process_id="proc-1",
            environment_id=None, atom_id=None,
            config_data=bad_value,
        )
        assert result["_success"] is False, f"Expected structured error for config_data={bad_value!r}"
        assert "atom_id" in result["error"] or "environment_id" in result["error"]


def test_none_config_data_does_not_crash():
    """None config_data is treated as empty — returns structured error."""
    sdk = _make_sdk()
    result = execute_process_action(
        sdk, profile="dev", process_id="proc-1",
        environment_id=None, atom_id=None,
        config_data=None,
    )
    assert result["_success"] is False


# ---------------------------------------------------------------------------
# Wrapper-level test: server.py parses config JSON and forwards to action
# ---------------------------------------------------------------------------

FAKE_CREDS = {
    "account_id": "acct-test",
    "username": "user",
    "password": "pass",
}


def test_wrapper_forwards_config_atom_id_to_action():
    """execute_process.fn() must parse config JSON and forward config_data to the action."""
    mock_action = MagicMock(return_value={"_success": True, "request_id": "req-1"})
    with (
        patch.object(server, "get_current_user", return_value="test-user"),
        patch.object(server, "get_secret", return_value=FAKE_CREDS),
        patch.object(server, "Boomi", return_value=MagicMock()),
        patch.object(server, "execute_process_action", mock_action),
    ):
        result = server.execute_process.fn(
            profile="dev",
            process_id="proc-1",
            environment_id=None,
            atom_id=None,
            config='{"atom_id": "atom-from-config"}',
        )

    mock_action.assert_called_once()
    call_kwargs = mock_action.call_args
    # config_data must contain the parsed atom_id
    assert call_kwargs[1]["config_data"] == {"atom_id": "atom-from-config"}
    # top-level atom_id should be None (not extracted by wrapper)
    assert call_kwargs[1]["atom_id"] is None


def test_wrapper_forwards_config_environment_id_to_action():
    """execute_process.fn() must forward config.environment_id through config_data."""
    mock_action = MagicMock(return_value={"_success": True, "request_id": "req-1"})
    with (
        patch.object(server, "get_current_user", return_value="test-user"),
        patch.object(server, "get_secret", return_value=FAKE_CREDS),
        patch.object(server, "Boomi", return_value=MagicMock()),
        patch.object(server, "execute_process_action", mock_action),
    ):
        result = server.execute_process.fn(
            profile="dev",
            process_id="proc-1",
            environment_id=None,
            atom_id=None,
            config='{"environment_id": "env-from-config"}',
        )

    mock_action.assert_called_once()
    call_kwargs = mock_action.call_args
    assert call_kwargs[1]["config_data"] == {"environment_id": "env-from-config"}
