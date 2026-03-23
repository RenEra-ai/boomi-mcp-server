"""Unit tests for manage_deployment batch-08 bugfixes (QA-029/030)."""

import json
import os
import sys
from types import SimpleNamespace
from unittest.mock import MagicMock, patch, call

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


def _mock_deployment(deployment_id="dep-001", package_id="pkg-001",
                     component_id="comp-001", environment_id="env-001"):
    """Create a mock deployment object."""
    dep = MagicMock()
    dep.deployment_id = deployment_id
    dep.package_id = package_id
    dep.component_id = component_id
    dep.environment_id = environment_id
    dep.component_type = "process"
    dep.component_name = "Test Process"
    dep.component_version = "1"
    dep.package_version = "1.0"
    dep.environment_name = "Test Env"
    dep.active = True
    dep.current = True
    dep.deployed_by = "user@test.com"
    dep.deployed_date = "2026-01-01"
    dep.notes = ""
    dep.version = 1
    return dep


# ── QA-029: deployment_id alias for get_deployment ───────────────────


class TestGetDeploymentAlias:
    @patch("server.get_secret", return_value=FAKE_CREDS)
    @patch("server.Boomi")
    @patch("server.get_current_user", return_value="test-user")
    def test_deployment_id_key_works(self, _user, mock_boomi_cls, _creds):
        """get_deployment should accept config.deployment_id as alias."""
        mock_sdk = MagicMock()
        mock_sdk.deployed_package.get_deployed_package.return_value = (
            _mock_deployment(deployment_id="dep-real-123")
        )
        mock_boomi_cls.return_value = mock_sdk

        result = _call_tool(
            server.manage_deployment,
            profile="dev",
            action="get_deployment",
            config=json.dumps({"deployment_id": "dep-real-123"}),
        )

        assert result["_success"] is True
        assert "deployment" in result
        mock_sdk.deployed_package.get_deployed_package.assert_called_once_with(
            id_="dep-real-123"
        )

    @patch("server.get_secret", return_value=FAKE_CREDS)
    @patch("server.Boomi")
    @patch("server.get_current_user", return_value="test-user")
    def test_package_id_still_works(self, _user, mock_boomi_cls, _creds):
        """get_deployment should still accept top-level package_id."""
        mock_sdk = MagicMock()
        mock_sdk.deployed_package.get_deployed_package.return_value = (
            _mock_deployment()
        )
        mock_boomi_cls.return_value = mock_sdk

        result = _call_tool(
            server.manage_deployment,
            profile="dev",
            action="get_deployment",
            package_id="dep-via-param",
        )

        assert result["_success"] is True
        mock_sdk.deployed_package.get_deployed_package.assert_called_once_with(
            id_="dep-via-param"
        )

    @patch("server.get_secret", return_value=FAKE_CREDS)
    @patch("server.Boomi")
    @patch("server.get_current_user", return_value="test-user")
    def test_package_id_takes_precedence(self, _user, mock_boomi_cls, _creds):
        """When both package_id and deployment_id are present, package_id wins."""
        mock_sdk = MagicMock()
        mock_sdk.deployed_package.get_deployed_package.return_value = (
            _mock_deployment()
        )
        mock_boomi_cls.return_value = mock_sdk

        result = _call_tool(
            server.manage_deployment,
            profile="dev",
            action="get_deployment",
            package_id="pkg-wins",
            config=json.dumps({"deployment_id": "dep-loses"}),
        )

        assert result["_success"] is True
        mock_sdk.deployed_package.get_deployed_package.assert_called_once_with(
            id_="pkg-wins"
        )


# ── QA-029: deployment_id alias for undeploy ─────────────────────────


class TestUndeployAlias:
    @patch("server.get_secret", return_value=FAKE_CREDS)
    @patch("server.Boomi")
    @patch("server.get_current_user", return_value="test-user")
    def test_deployment_id_key_works(self, _user, mock_boomi_cls, _creds):
        """undeploy should accept config.deployment_id as alias."""
        mock_sdk = MagicMock()
        mock_sdk.deployed_package.get_deployed_package.return_value = (
            _mock_deployment(deployment_id="dep-undeploy-123")
        )
        mock_boomi_cls.return_value = mock_sdk

        result = _call_tool(
            server.manage_deployment,
            profile="dev",
            action="undeploy",
            config=json.dumps({"deployment_id": "dep-undeploy-123"}),
        )

        assert result["_success"] is True
        assert "undeployed" in result
        mock_sdk.deployed_package.delete_deployed_package.assert_called_once_with(
            id_="dep-undeploy-123"
        )


# ── QA-030: list_deployments component_id filter ─────────────────────


class TestListDeploymentsComponentFilter:
    @patch("server.get_secret", return_value=FAKE_CREDS)
    @patch("server.Boomi")
    @patch("server.get_current_user", return_value="test-user")
    def test_component_id_filter_builds_expression(self, _user, mock_boomi_cls, _creds):
        """list_deployments with component_id should build COMPONENTID expression."""
        mock_sdk = MagicMock()
        query_result = MagicMock()
        query_result.result = [_mock_deployment(component_id="comp-target")]
        query_result.query_token = None
        mock_sdk.deployed_package.query_deployed_package.return_value = query_result
        mock_boomi_cls.return_value = mock_sdk

        result = _call_tool(
            server.manage_deployment,
            profile="dev",
            action="list_deployments",
            config=json.dumps({"component_id": "comp-target"}),
        )

        assert result["_success"] is True
        # Verify the SDK query was called with the right filter
        call_args = mock_sdk.deployed_package.query_deployed_package.call_args
        query_config = call_args.kwargs.get("request_body") or call_args[1].get("request_body") or call_args[0][0]
        # The query filter should contain a COMPONENTID expression
        qf = query_config.query_filter
        expr = qf.expression
        # Single expression: should be COMPONENTID EQUALS
        assert hasattr(expr, 'property')
        prop_val = expr.property
        prop_str = prop_val.value if hasattr(prop_val, 'value') else str(prop_val)
        assert prop_str == "componentId"

    @patch("server.get_secret", return_value=FAKE_CREDS)
    @patch("server.Boomi")
    @patch("server.get_current_user", return_value="test-user")
    def test_multiple_filters_use_and_grouping(self, _user, mock_boomi_cls, _creds):
        """Multiple filters (environment_id + component_id) should AND-group."""
        mock_sdk = MagicMock()
        query_result = MagicMock()
        query_result.result = []
        query_result.query_token = None
        mock_sdk.deployed_package.query_deployed_package.return_value = query_result
        mock_boomi_cls.return_value = mock_sdk

        result = _call_tool(
            server.manage_deployment,
            profile="dev",
            action="list_deployments",
            environment_id="env-filter",
            config=json.dumps({"component_id": "comp-filter"}),
        )

        assert result["_success"] is True
        call_args = mock_sdk.deployed_package.query_deployed_package.call_args
        query_config = call_args.kwargs.get("request_body") or call_args[1].get("request_body") or call_args[0][0]
        qf = query_config.query_filter
        expr = qf.expression
        # Multiple expressions → should be a grouping expression with AND
        assert hasattr(expr, 'nested_expression')
        op_val = expr.operator
        op_str = op_val.value if hasattr(op_val, 'value') else str(op_val)
        assert op_str == "and"
        assert len(expr.nested_expression) == 2
