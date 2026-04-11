"""Regression tests for BUG-23: resource_id fallback in list_environment_roles.

The list_environment_roles handler must accept resource_id as a fallback when
environment_id is not provided in config.  Without the fix, passing only
resource_id would return an error requiring environment_id or role_id.

Tests call the action handler directly with a mocked SDK and verify that
the resource_id value is used as the environment_id filter.
Also verifies the server.py wrapper forwards resource_id correctly.
"""

import os
import sys
from unittest.mock import MagicMock, patch
from pathlib import Path

from boomi.models import EnvironmentRoleSimpleExpressionProperty

from boomi_mcp.categories.environments import _action_list_environment_roles


# ---------------------------------------------------------------------------
# Handler-level tests: resource_id fallback
# ---------------------------------------------------------------------------

ENV_ID = "env-abc-123"


def _mock_sdk_with_empty_query():
    sdk = MagicMock()
    mock_result = MagicMock()
    mock_result.result = []
    mock_result.query_token = None
    sdk.environment_role.query_environment_role.return_value = mock_result
    return sdk


def test_list_environment_roles_uses_resource_id():
    """list_environment_roles must use resource_id when environment_id is absent."""
    sdk = _mock_sdk_with_empty_query()
    result = _action_list_environment_roles(sdk, profile="dev", resource_id=ENV_ID)
    assert result["_success"] is True
    call_args = sdk.environment_role.query_environment_role.call_args
    query_config = call_args.kwargs.get("request_body") or call_args[1].get("request_body")
    expr = query_config.query_filter.expression
    assert expr.property == EnvironmentRoleSimpleExpressionProperty.ENVIRONMENTID
    assert expr.argument == [ENV_ID]


def test_list_environment_roles_prefers_environment_id():
    """environment_id takes precedence over resource_id."""
    sdk = _mock_sdk_with_empty_query()
    result = _action_list_environment_roles(
        sdk, profile="dev", environment_id="explicit-id", resource_id=ENV_ID
    )
    assert result["_success"] is True
    call_args = sdk.environment_role.query_environment_role.call_args
    query_config = call_args.kwargs.get("request_body") or call_args[1].get("request_body")
    expr = query_config.query_filter.expression
    assert expr.property == EnvironmentRoleSimpleExpressionProperty.ENVIRONMENTID
    assert expr.argument == ["explicit-id"]


def test_list_environment_roles_error_without_any_id():
    """list_environment_roles must error when neither environment_id, resource_id, nor role_id is provided."""
    sdk = MagicMock()
    result = _action_list_environment_roles(sdk, profile="dev")
    assert result["_success"] is False
    assert "environment_id or role_id is required" in result["error"]


# ---------------------------------------------------------------------------
# Wrapper-level test: server.py forwards resource_id
# ---------------------------------------------------------------------------

FAKE_CREDS = {
    "account_id": "acct-test",
    "username": "user",
    "password": "pass",
}


def test_wrapper_forwards_resource_id():
    """manage_environments.fn() must pass resource_id through to the action handler."""
    _project_root = str(Path(__file__).resolve().parent.parent)
    if _project_root not in sys.path:
        sys.path.insert(0, _project_root)
    os.environ["BOOMI_LOCAL"] = "true"

    import server  # noqa: E402

    mock_action = MagicMock(return_value={"_success": True})
    with (
        patch.object(server, "get_current_user", return_value="test-user"),
        patch.object(server, "get_secret", return_value=FAKE_CREDS),
        patch.object(server, "Boomi", return_value=MagicMock()),
        patch.object(server, "manage_environments_action", mock_action),
    ):
        server.manage_environments(
            profile="dev", action="list_environment_roles", resource_id=ENV_ID
        )
    _, kwargs = mock_action.call_args
    assert kwargs.get("resource_id") == ENV_ID
