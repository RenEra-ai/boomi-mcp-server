"""Regression tests for the manage_deployment MCP wrapper in server.py.

Verifies that the public manage_deployment entrypoint correctly forwards
all parameters (especially resource_id) to manage_deployment_action.
"""

import os
import sys
import pytest
from unittest.mock import MagicMock, patch
from pathlib import Path

# Ensure project root is on sys.path so we can import server
_project_root = str(Path(__file__).resolve().parent.parent)
if _project_root not in sys.path:
    sys.path.insert(0, _project_root)

# Force local mode before importing server
os.environ["BOOMI_LOCAL"] = "true"

import server  # noqa: E402


FAKE_CREDS = {
    "account_id": "acct-test",
    "username": "user",
    "password": "pass",
}


@pytest.fixture(autouse=True)
def _mock_auth_and_sdk():
    """Patch auth helpers and SDK so the wrapper never hits real services."""
    with (
        patch.object(server, "get_current_user", return_value="test-user"),
        patch.object(server, "get_secret", return_value=FAKE_CREDS),
        patch.object(server, "Boomi", return_value=MagicMock()),
    ):
        yield


def _call_wrapper(**kwargs):
    """Call the underlying manage_deployment function (unwrapped from @mcp.tool)."""
    return server.manage_deployment.fn(**kwargs)


def test_resource_id_forwarded():
    """resource_id must reach manage_deployment_action as a keyword arg."""
    mock_action = MagicMock(return_value={"_success": True})
    with patch.object(server, "manage_deployment_action", mock_action):
        result = _call_wrapper(
            profile="dev",
            action="detach_process_atom",
            resource_id="att-123",
        )
    assert result["_success"] is True
    mock_action.assert_called_once()
    _, kwargs = mock_action.call_args
    assert kwargs["resource_id"] == "att-123"


def test_package_id_and_environment_id_forwarded():
    """package_id and environment_id must also be forwarded."""
    mock_action = MagicMock(return_value={"_success": True})
    with patch.object(server, "manage_deployment_action", mock_action):
        _call_wrapper(
            profile="dev",
            action="deploy",
            package_id="pkg-1",
            environment_id="env-2",
        )
    _, kwargs = mock_action.call_args
    assert kwargs["package_id"] == "pkg-1"
    assert kwargs["environment_id"] == "env-2"


def test_config_json_parsed_and_forwarded():
    """config string must be parsed as JSON and forwarded as config_data."""
    mock_action = MagicMock(return_value={"_success": True})
    with patch.object(server, "manage_deployment_action", mock_action):
        _call_wrapper(
            profile="dev",
            action="list_packages",
            config='{"component_id": "comp-1"}',
        )
    _, kwargs = mock_action.call_args
    assert kwargs["config_data"] == {"component_id": "comp-1"}


def test_invalid_config_returns_error():
    """Malformed config JSON must return an error without calling the action."""
    mock_action = MagicMock()
    with patch.object(server, "manage_deployment_action", mock_action):
        result = _call_wrapper(
            profile="dev",
            action="list_packages",
            config="not-json",
        )
    assert result["_success"] is False
    assert "Invalid config" in result["error"]
    mock_action.assert_not_called()


def test_none_params_not_forwarded():
    """When optional params are None they must not appear in the action kwargs."""
    mock_action = MagicMock(return_value={"_success": True})
    with patch.object(server, "manage_deployment_action", mock_action):
        _call_wrapper(profile="dev", action="list_packages")
    _, kwargs = mock_action.call_args
    assert "package_id" not in kwargs
    assert "environment_id" not in kwargs
    assert "resource_id" not in kwargs
