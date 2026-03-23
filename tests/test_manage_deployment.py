"""Unit tests for manage_deployment actions including attachment actions."""

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


def _mock_attachment(id_="att-001", component_id="comp-001", atom_id="atom-001",
                     environment_id=None, process_id=None, component_type="process"):
    """Create a mock attachment object."""
    att = MagicMock()
    att.id_ = id_
    att.component_id = component_id if component_id else None
    att.atom_id = atom_id if atom_id else None
    att.environment_id = environment_id if environment_id else None
    att.process_id = process_id if process_id else None
    att.component_type = component_type
    return att


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
        # Multiple expressions -> should be a grouping expression with AND
        assert hasattr(expr, 'nested_expression')
        op_val = expr.operator
        op_str = op_val.value if hasattr(op_val, 'value') else str(op_val)
        assert op_str == "and"
        assert len(expr.nested_expression) == 2


# ── Component-Atom Attachment Tests ──────────────────────────────────


class TestListComponentAtomAttachments:
    @patch("server.get_secret", return_value=FAKE_CREDS)
    @patch("server.Boomi")
    @patch("server.get_current_user", return_value="test-user")
    def test_list_no_filter(self, _user, mock_boomi_cls, _creds):
        """list_component_atom_attachments without filter returns all."""
        mock_sdk = MagicMock()
        query_result = MagicMock()
        query_result.result = [
            _mock_attachment(id_="att-1", component_id="comp-1", atom_id="atom-1"),
            _mock_attachment(id_="att-2", component_id="comp-2", atom_id="atom-2"),
        ]
        query_result.query_token = None
        mock_sdk.component_atom_attachment.query_component_atom_attachment.return_value = query_result
        mock_boomi_cls.return_value = mock_sdk

        result = _call_tool(
            server.manage_deployment,
            profile="dev",
            action="list_component_atom_attachments",
        )

        assert result["_success"] is True
        assert result["total_count"] == 2
        assert len(result["attachments"]) == 2

    @patch("server.get_secret", return_value=FAKE_CREDS)
    @patch("server.Boomi")
    @patch("server.get_current_user", return_value="test-user")
    def test_list_filter_by_component_id(self, _user, mock_boomi_cls, _creds):
        """list_component_atom_attachments filters by component_id."""
        mock_sdk = MagicMock()
        query_result = MagicMock()
        query_result.result = [_mock_attachment(id_="att-1", component_id="comp-target")]
        query_result.query_token = None
        mock_sdk.component_atom_attachment.query_component_atom_attachment.return_value = query_result
        mock_boomi_cls.return_value = mock_sdk

        result = _call_tool(
            server.manage_deployment,
            profile="dev",
            action="list_component_atom_attachments",
            config=json.dumps({"component_id": "comp-target"}),
        )

        assert result["_success"] is True
        call_args = mock_sdk.component_atom_attachment.query_component_atom_attachment.call_args
        qc = call_args.kwargs.get("request_body") or call_args[1].get("request_body") or call_args[0][0]
        expr = qc.query_filter.expression
        prop_val = expr.property
        prop_str = prop_val.value if hasattr(prop_val, 'value') else str(prop_val)
        assert prop_str == "componentId"

    @patch("server.get_secret", return_value=FAKE_CREDS)
    @patch("server.Boomi")
    @patch("server.get_current_user", return_value="test-user")
    def test_list_filter_by_atom_id(self, _user, mock_boomi_cls, _creds):
        """list_component_atom_attachments filters by atom_id."""
        mock_sdk = MagicMock()
        query_result = MagicMock()
        query_result.result = []
        query_result.query_token = None
        mock_sdk.component_atom_attachment.query_component_atom_attachment.return_value = query_result
        mock_boomi_cls.return_value = mock_sdk

        result = _call_tool(
            server.manage_deployment,
            profile="dev",
            action="list_component_atom_attachments",
            config=json.dumps({"atom_id": "atom-target"}),
        )

        assert result["_success"] is True
        call_args = mock_sdk.component_atom_attachment.query_component_atom_attachment.call_args
        qc = call_args.kwargs.get("request_body") or call_args[1].get("request_body") or call_args[0][0]
        expr = qc.query_filter.expression
        prop_val = expr.property
        prop_str = prop_val.value if hasattr(prop_val, 'value') else str(prop_val)
        assert prop_str == "atomId"

    @patch("server.get_secret", return_value=FAKE_CREDS)
    @patch("server.Boomi")
    @patch("server.get_current_user", return_value="test-user")
    def test_list_paginates(self, _user, mock_boomi_cls, _creds):
        """list_component_atom_attachments follows query_token pagination."""
        mock_sdk = MagicMock()
        page1 = MagicMock()
        page1.result = [_mock_attachment(id_="att-1")]
        page1.query_token = "token-page-2"
        page2 = MagicMock()
        page2.result = [_mock_attachment(id_="att-2")]
        page2.query_token = None
        mock_sdk.component_atom_attachment.query_component_atom_attachment.return_value = page1
        mock_sdk.component_atom_attachment.query_more_component_atom_attachment.return_value = page2
        mock_boomi_cls.return_value = mock_sdk

        result = _call_tool(
            server.manage_deployment,
            profile="dev",
            action="list_component_atom_attachments",
        )

        assert result["_success"] is True
        assert result["total_count"] == 2
        mock_sdk.component_atom_attachment.query_more_component_atom_attachment.assert_called_once_with(
            request_body="token-page-2"
        )


class TestAttachComponentAtom:
    @patch("server.get_secret", return_value=FAKE_CREDS)
    @patch("server.Boomi")
    @patch("server.get_current_user", return_value="test-user")
    def test_attach_success(self, _user, mock_boomi_cls, _creds):
        """attach_component_atom creates attachment with component_id and atom_id."""
        mock_sdk = MagicMock()
        mock_sdk.component_atom_attachment.create_component_atom_attachment.return_value = (
            _mock_attachment(id_="new-att-1", component_id="comp-1", atom_id="atom-1")
        )
        mock_boomi_cls.return_value = mock_sdk

        result = _call_tool(
            server.manage_deployment,
            profile="dev",
            action="attach_component_atom",
            config=json.dumps({"component_id": "comp-1", "atom_id": "atom-1"}),
        )

        assert result["_success"] is True
        assert result["attachment"]["id"] == "new-att-1"
        mock_sdk.component_atom_attachment.create_component_atom_attachment.assert_called_once()

    @patch("server.get_secret", return_value=FAKE_CREDS)
    @patch("server.Boomi")
    @patch("server.get_current_user", return_value="test-user")
    def test_attach_missing_component_id(self, _user, mock_boomi_cls, _creds):
        """attach_component_atom fails without component_id."""
        mock_sdk = MagicMock()
        mock_boomi_cls.return_value = mock_sdk

        result = _call_tool(
            server.manage_deployment,
            profile="dev",
            action="attach_component_atom",
            config=json.dumps({"atom_id": "atom-1"}),
        )

        assert result["_success"] is False
        assert "component_id" in result["error"]

    @patch("server.get_secret", return_value=FAKE_CREDS)
    @patch("server.Boomi")
    @patch("server.get_current_user", return_value="test-user")
    def test_attach_missing_atom_id(self, _user, mock_boomi_cls, _creds):
        """attach_component_atom fails without atom_id."""
        mock_sdk = MagicMock()
        mock_boomi_cls.return_value = mock_sdk

        result = _call_tool(
            server.manage_deployment,
            profile="dev",
            action="attach_component_atom",
            config=json.dumps({"component_id": "comp-1"}),
        )

        assert result["_success"] is False
        assert "atom_id" in result["error"]


class TestDetachComponentAtom:
    @patch("server.get_secret", return_value=FAKE_CREDS)
    @patch("server.Boomi")
    @patch("server.get_current_user", return_value="test-user")
    def test_detach_success(self, _user, mock_boomi_cls, _creds):
        """detach_component_atom deletes by resource_id."""
        mock_sdk = MagicMock()
        mock_boomi_cls.return_value = mock_sdk

        result = _call_tool(
            server.manage_deployment,
            profile="dev",
            action="detach_component_atom",
            config=json.dumps({"resource_id": "att-del-1"}),
        )

        assert result["_success"] is True
        assert result["deleted_id"] == "att-del-1"
        mock_sdk.component_atom_attachment.delete_component_atom_attachment.assert_called_once_with(
            id_="att-del-1"
        )

    @patch("server.get_secret", return_value=FAKE_CREDS)
    @patch("server.Boomi")
    @patch("server.get_current_user", return_value="test-user")
    def test_detach_missing_resource_id(self, _user, mock_boomi_cls, _creds):
        """detach_component_atom fails without resource_id."""
        mock_sdk = MagicMock()
        mock_boomi_cls.return_value = mock_sdk

        result = _call_tool(
            server.manage_deployment,
            profile="dev",
            action="detach_component_atom",
        )

        assert result["_success"] is False
        assert "resource_id" in result["error"]


# ── Component-Environment Attachment Tests ───────────────────────────


class TestListComponentEnvironmentAttachments:
    @patch("server.get_secret", return_value=FAKE_CREDS)
    @patch("server.Boomi")
    @patch("server.get_current_user", return_value="test-user")
    def test_list_no_filter(self, _user, mock_boomi_cls, _creds):
        """list_component_environment_attachments without filter returns all."""
        mock_sdk = MagicMock()
        query_result = MagicMock()
        query_result.result = [
            _mock_attachment(id_="att-1", component_id="comp-1", atom_id=None, environment_id="env-1"),
        ]
        query_result.query_token = None
        mock_sdk.component_environment_attachment.query_component_environment_attachment.return_value = query_result
        mock_boomi_cls.return_value = mock_sdk

        result = _call_tool(
            server.manage_deployment,
            profile="dev",
            action="list_component_environment_attachments",
        )

        assert result["_success"] is True
        assert result["total_count"] == 1

    @patch("server.get_secret", return_value=FAKE_CREDS)
    @patch("server.Boomi")
    @patch("server.get_current_user", return_value="test-user")
    def test_list_filter_by_environment_id(self, _user, mock_boomi_cls, _creds):
        """list_component_environment_attachments filters by environment_id."""
        mock_sdk = MagicMock()
        query_result = MagicMock()
        query_result.result = []
        query_result.query_token = None
        mock_sdk.component_environment_attachment.query_component_environment_attachment.return_value = query_result
        mock_boomi_cls.return_value = mock_sdk

        result = _call_tool(
            server.manage_deployment,
            profile="dev",
            action="list_component_environment_attachments",
            environment_id="env-target",
        )

        assert result["_success"] is True
        call_args = mock_sdk.component_environment_attachment.query_component_environment_attachment.call_args
        qc = call_args.kwargs.get("request_body") or call_args[1].get("request_body") or call_args[0][0]
        expr = qc.query_filter.expression
        prop_val = expr.property
        prop_str = prop_val.value if hasattr(prop_val, 'value') else str(prop_val)
        assert prop_str == "environmentId"

    @patch("server.get_secret", return_value=FAKE_CREDS)
    @patch("server.Boomi")
    @patch("server.get_current_user", return_value="test-user")
    def test_list_paginates(self, _user, mock_boomi_cls, _creds):
        """list_component_environment_attachments follows pagination."""
        mock_sdk = MagicMock()
        page1 = MagicMock()
        page1.result = [_mock_attachment(id_="att-1")]
        page1.query_token = "token-2"
        page2 = MagicMock()
        page2.result = [_mock_attachment(id_="att-2")]
        page2.query_token = None
        mock_sdk.component_environment_attachment.query_component_environment_attachment.return_value = page1
        mock_sdk.component_environment_attachment.query_more_component_environment_attachment.return_value = page2
        mock_boomi_cls.return_value = mock_sdk

        result = _call_tool(
            server.manage_deployment,
            profile="dev",
            action="list_component_environment_attachments",
        )

        assert result["_success"] is True
        assert result["total_count"] == 2


class TestAttachComponentEnvironment:
    @patch("server.get_secret", return_value=FAKE_CREDS)
    @patch("server.Boomi")
    @patch("server.get_current_user", return_value="test-user")
    def test_attach_success(self, _user, mock_boomi_cls, _creds):
        """attach_component_environment creates attachment."""
        mock_sdk = MagicMock()
        mock_sdk.component_environment_attachment.create_component_environment_attachment.return_value = (
            _mock_attachment(id_="new-att", component_id="comp-1", atom_id=None, environment_id="env-1")
        )
        mock_boomi_cls.return_value = mock_sdk

        result = _call_tool(
            server.manage_deployment,
            profile="dev",
            action="attach_component_environment",
            environment_id="env-1",
            config=json.dumps({"component_id": "comp-1"}),
        )

        assert result["_success"] is True
        assert result["attachment"]["id"] == "new-att"

    @patch("server.get_secret", return_value=FAKE_CREDS)
    @patch("server.Boomi")
    @patch("server.get_current_user", return_value="test-user")
    def test_attach_missing_component_id(self, _user, mock_boomi_cls, _creds):
        """attach_component_environment fails without component_id."""
        mock_sdk = MagicMock()
        mock_boomi_cls.return_value = mock_sdk

        result = _call_tool(
            server.manage_deployment,
            profile="dev",
            action="attach_component_environment",
            environment_id="env-1",
        )

        assert result["_success"] is False
        assert "component_id" in result["error"]

    @patch("server.get_secret", return_value=FAKE_CREDS)
    @patch("server.Boomi")
    @patch("server.get_current_user", return_value="test-user")
    def test_attach_missing_environment_id(self, _user, mock_boomi_cls, _creds):
        """attach_component_environment fails without environment_id."""
        mock_sdk = MagicMock()
        mock_boomi_cls.return_value = mock_sdk

        result = _call_tool(
            server.manage_deployment,
            profile="dev",
            action="attach_component_environment",
            config=json.dumps({"component_id": "comp-1"}),
        )

        assert result["_success"] is False
        assert "environment_id" in result["error"]


class TestDetachComponentEnvironment:
    @patch("server.get_secret", return_value=FAKE_CREDS)
    @patch("server.Boomi")
    @patch("server.get_current_user", return_value="test-user")
    def test_detach_success(self, _user, mock_boomi_cls, _creds):
        """detach_component_environment deletes by resource_id."""
        mock_sdk = MagicMock()
        mock_boomi_cls.return_value = mock_sdk

        result = _call_tool(
            server.manage_deployment,
            profile="dev",
            action="detach_component_environment",
            config=json.dumps({"resource_id": "att-del-2"}),
        )

        assert result["_success"] is True
        assert result["deleted_id"] == "att-del-2"
        mock_sdk.component_environment_attachment.delete_component_environment_attachment.assert_called_once_with(
            id_="att-del-2"
        )

    @patch("server.get_secret", return_value=FAKE_CREDS)
    @patch("server.Boomi")
    @patch("server.get_current_user", return_value="test-user")
    def test_detach_missing_resource_id(self, _user, mock_boomi_cls, _creds):
        """detach_component_environment fails without resource_id."""
        mock_sdk = MagicMock()
        mock_boomi_cls.return_value = mock_sdk

        result = _call_tool(
            server.manage_deployment,
            profile="dev",
            action="detach_component_environment",
        )

        assert result["_success"] is False
        assert "resource_id" in result["error"]


# ── Process-Atom Attachment Tests ────────────────────────────────────


class TestListProcessAtomAttachments:
    @patch("server.get_secret", return_value=FAKE_CREDS)
    @patch("server.Boomi")
    @patch("server.get_current_user", return_value="test-user")
    def test_list_no_filter(self, _user, mock_boomi_cls, _creds):
        """list_process_atom_attachments without filter returns all."""
        mock_sdk = MagicMock()
        query_result = MagicMock()
        query_result.result = [
            _mock_attachment(id_="att-1", component_id=None, atom_id="atom-1", process_id="proc-1"),
        ]
        query_result.query_token = None
        mock_sdk.process_atom_attachment.query_process_atom_attachment.return_value = query_result
        mock_boomi_cls.return_value = mock_sdk

        result = _call_tool(
            server.manage_deployment,
            profile="dev",
            action="list_process_atom_attachments",
        )

        assert result["_success"] is True
        assert result["total_count"] == 1

    @patch("server.get_secret", return_value=FAKE_CREDS)
    @patch("server.Boomi")
    @patch("server.get_current_user", return_value="test-user")
    def test_list_filter_by_process_id(self, _user, mock_boomi_cls, _creds):
        """list_process_atom_attachments filters by process_id."""
        mock_sdk = MagicMock()
        query_result = MagicMock()
        query_result.result = []
        query_result.query_token = None
        mock_sdk.process_atom_attachment.query_process_atom_attachment.return_value = query_result
        mock_boomi_cls.return_value = mock_sdk

        result = _call_tool(
            server.manage_deployment,
            profile="dev",
            action="list_process_atom_attachments",
            config=json.dumps({"process_id": "proc-target"}),
        )

        assert result["_success"] is True
        call_args = mock_sdk.process_atom_attachment.query_process_atom_attachment.call_args
        qc = call_args.kwargs.get("request_body") or call_args[1].get("request_body") or call_args[0][0]
        expr = qc.query_filter.expression
        prop_val = expr.property
        prop_str = prop_val.value if hasattr(prop_val, 'value') else str(prop_val)
        assert prop_str == "processId"

    @patch("server.get_secret", return_value=FAKE_CREDS)
    @patch("server.Boomi")
    @patch("server.get_current_user", return_value="test-user")
    def test_list_filter_by_atom_id(self, _user, mock_boomi_cls, _creds):
        """list_process_atom_attachments filters by atom_id."""
        mock_sdk = MagicMock()
        query_result = MagicMock()
        query_result.result = []
        query_result.query_token = None
        mock_sdk.process_atom_attachment.query_process_atom_attachment.return_value = query_result
        mock_boomi_cls.return_value = mock_sdk

        result = _call_tool(
            server.manage_deployment,
            profile="dev",
            action="list_process_atom_attachments",
            config=json.dumps({"atom_id": "atom-target"}),
        )

        assert result["_success"] is True
        call_args = mock_sdk.process_atom_attachment.query_process_atom_attachment.call_args
        qc = call_args.kwargs.get("request_body") or call_args[1].get("request_body") or call_args[0][0]
        expr = qc.query_filter.expression
        prop_val = expr.property
        prop_str = prop_val.value if hasattr(prop_val, 'value') else str(prop_val)
        assert prop_str == "atomId"

    @patch("server.get_secret", return_value=FAKE_CREDS)
    @patch("server.Boomi")
    @patch("server.get_current_user", return_value="test-user")
    def test_list_paginates(self, _user, mock_boomi_cls, _creds):
        """list_process_atom_attachments follows pagination."""
        mock_sdk = MagicMock()
        page1 = MagicMock()
        page1.result = [_mock_attachment(id_="att-1")]
        page1.query_token = "token-2"
        page2 = MagicMock()
        page2.result = [_mock_attachment(id_="att-2")]
        page2.query_token = None
        mock_sdk.process_atom_attachment.query_process_atom_attachment.return_value = page1
        mock_sdk.process_atom_attachment.query_more_process_atom_attachment.return_value = page2
        mock_boomi_cls.return_value = mock_sdk

        result = _call_tool(
            server.manage_deployment,
            profile="dev",
            action="list_process_atom_attachments",
        )

        assert result["_success"] is True
        assert result["total_count"] == 2


class TestAttachProcessAtom:
    @patch("server.get_secret", return_value=FAKE_CREDS)
    @patch("server.Boomi")
    @patch("server.get_current_user", return_value="test-user")
    def test_attach_success(self, _user, mock_boomi_cls, _creds):
        """attach_process_atom creates attachment."""
        mock_sdk = MagicMock()
        mock_sdk.process_atom_attachment.create_process_atom_attachment.return_value = (
            _mock_attachment(id_="new-att", component_id=None, atom_id="atom-1", process_id="proc-1")
        )
        mock_boomi_cls.return_value = mock_sdk

        result = _call_tool(
            server.manage_deployment,
            profile="dev",
            action="attach_process_atom",
            config=json.dumps({"process_id": "proc-1", "atom_id": "atom-1"}),
        )

        assert result["_success"] is True
        assert result["attachment"]["id"] == "new-att"

    @patch("server.get_secret", return_value=FAKE_CREDS)
    @patch("server.Boomi")
    @patch("server.get_current_user", return_value="test-user")
    def test_attach_missing_process_id(self, _user, mock_boomi_cls, _creds):
        """attach_process_atom fails without process_id."""
        mock_sdk = MagicMock()
        mock_boomi_cls.return_value = mock_sdk

        result = _call_tool(
            server.manage_deployment,
            profile="dev",
            action="attach_process_atom",
            config=json.dumps({"atom_id": "atom-1"}),
        )

        assert result["_success"] is False
        assert "process_id" in result["error"]

    @patch("server.get_secret", return_value=FAKE_CREDS)
    @patch("server.Boomi")
    @patch("server.get_current_user", return_value="test-user")
    def test_attach_missing_atom_id(self, _user, mock_boomi_cls, _creds):
        """attach_process_atom fails without atom_id."""
        mock_sdk = MagicMock()
        mock_boomi_cls.return_value = mock_sdk

        result = _call_tool(
            server.manage_deployment,
            profile="dev",
            action="attach_process_atom",
            config=json.dumps({"process_id": "proc-1"}),
        )

        assert result["_success"] is False
        assert "atom_id" in result["error"]


class TestDetachProcessAtom:
    @patch("server.get_secret", return_value=FAKE_CREDS)
    @patch("server.Boomi")
    @patch("server.get_current_user", return_value="test-user")
    def test_detach_success(self, _user, mock_boomi_cls, _creds):
        """detach_process_atom deletes by resource_id."""
        mock_sdk = MagicMock()
        mock_boomi_cls.return_value = mock_sdk

        result = _call_tool(
            server.manage_deployment,
            profile="dev",
            action="detach_process_atom",
            config=json.dumps({"resource_id": "att-del-3"}),
        )

        assert result["_success"] is True
        assert result["deleted_id"] == "att-del-3"
        mock_sdk.process_atom_attachment.delete_process_atom_attachment.assert_called_once_with(
            id_="att-del-3"
        )

    @patch("server.get_secret", return_value=FAKE_CREDS)
    @patch("server.Boomi")
    @patch("server.get_current_user", return_value="test-user")
    def test_detach_missing_resource_id(self, _user, mock_boomi_cls, _creds):
        """detach_process_atom fails without resource_id."""
        mock_sdk = MagicMock()
        mock_boomi_cls.return_value = mock_sdk

        result = _call_tool(
            server.manage_deployment,
            profile="dev",
            action="detach_process_atom",
        )

        assert result["_success"] is False
        assert "resource_id" in result["error"]


# ── Process-Environment Attachment Tests ─────────────────────────────


class TestAttachProcessEnvironment:
    @patch("server.get_secret", return_value=FAKE_CREDS)
    @patch("server.Boomi")
    @patch("server.get_current_user", return_value="test-user")
    def test_attach_success(self, _user, mock_boomi_cls, _creds):
        """attach_process_environment creates attachment (CREATE only)."""
        mock_sdk = MagicMock()
        mock_sdk.process_environment_attachment.create_process_environment_attachment.return_value = (
            _mock_attachment(id_="new-att", component_id=None, atom_id=None,
                           environment_id="env-1", process_id="proc-1")
        )
        mock_boomi_cls.return_value = mock_sdk

        result = _call_tool(
            server.manage_deployment,
            profile="dev",
            action="attach_process_environment",
            environment_id="env-1",
            config=json.dumps({"process_id": "proc-1"}),
        )

        assert result["_success"] is True
        assert result["attachment"]["id"] == "new-att"
        mock_sdk.process_environment_attachment.create_process_environment_attachment.assert_called_once()

    @patch("server.get_secret", return_value=FAKE_CREDS)
    @patch("server.Boomi")
    @patch("server.get_current_user", return_value="test-user")
    def test_attach_missing_process_id(self, _user, mock_boomi_cls, _creds):
        """attach_process_environment fails without process_id."""
        mock_sdk = MagicMock()
        mock_boomi_cls.return_value = mock_sdk

        result = _call_tool(
            server.manage_deployment,
            profile="dev",
            action="attach_process_environment",
            environment_id="env-1",
        )

        assert result["_success"] is False
        assert "process_id" in result["error"]

    @patch("server.get_secret", return_value=FAKE_CREDS)
    @patch("server.Boomi")
    @patch("server.get_current_user", return_value="test-user")
    def test_attach_missing_environment_id(self, _user, mock_boomi_cls, _creds):
        """attach_process_environment fails without environment_id."""
        mock_sdk = MagicMock()
        mock_boomi_cls.return_value = mock_sdk

        result = _call_tool(
            server.manage_deployment,
            profile="dev",
            action="attach_process_environment",
            config=json.dumps({"process_id": "proc-1"}),
        )

        assert result["_success"] is False
        assert "environment_id" in result["error"]


# ── Router: unknown action test ──────────────────────────────────────


class TestRouterUnknownAction:
    @patch("server.get_secret", return_value=FAKE_CREDS)
    @patch("server.Boomi")
    @patch("server.get_current_user", return_value="test-user")
    def test_unknown_action_lists_all_valid(self, _user, mock_boomi_cls, _creds):
        """Unknown action returns error with all valid_actions including new ones."""
        mock_sdk = MagicMock()
        mock_boomi_cls.return_value = mock_sdk

        result = _call_tool(
            server.manage_deployment,
            profile="dev",
            action="nonexistent_action",
        )

        assert result["_success"] is False
        valid = result["valid_actions"]
        # Verify all 18 actions are listed
        assert "list_packages" in valid
        assert "attach_component_atom" in valid
        assert "detach_component_atom" in valid
        assert "list_component_atom_attachments" in valid
        assert "attach_component_environment" in valid
        assert "detach_component_environment" in valid
        assert "list_component_environment_attachments" in valid
        assert "attach_process_atom" in valid
        assert "detach_process_atom" in valid
        assert "list_process_atom_attachments" in valid
        assert "attach_process_environment" in valid
        assert "get_package_manifest" in valid
        assert len(valid) == 19


# ── Package Manifest Tests ───────────────────────────────────────────


class TestGetPackageManifest:
    @patch("server.get_secret", return_value=FAKE_CREDS)
    @patch("server.Boomi")
    @patch("server.get_current_user", return_value="test-user")
    def test_success(self, _user, mock_boomi_cls, _creds):
        """get_package_manifest returns manifest for a package."""
        mock_sdk = MagicMock()
        manifest_result = MagicMock()
        manifest_result.package_id = "pkg-001"
        manifest_result.component_id = "comp-001"
        # Simulate a list of component entries
        comp_entry = MagicMock()
        comp_entry.component_id = "comp-sub-1"
        comp_entry.component_name = "Sub Process"
        comp_entry.component_type = "process"
        manifest_result.components = [comp_entry]
        mock_sdk.packaged_component_manifest.get_packaged_component_manifest.return_value = manifest_result
        mock_boomi_cls.return_value = mock_sdk

        result = _call_tool(
            server.manage_deployment,
            profile="dev",
            action="get_package_manifest",
            config=json.dumps({"package_id": "pkg-001"}),
        )

        assert result["_success"] is True
        assert "manifest" in result
        mock_sdk.packaged_component_manifest.get_packaged_component_manifest.assert_called_once_with(
            package_id="pkg-001"
        )

    @patch("server.get_secret", return_value=FAKE_CREDS)
    @patch("server.Boomi")
    @patch("server.get_current_user", return_value="test-user")
    def test_missing_package_id(self, _user, mock_boomi_cls, _creds):
        """get_package_manifest fails without package_id."""
        mock_sdk = MagicMock()
        mock_boomi_cls.return_value = mock_sdk

        result = _call_tool(
            server.manage_deployment,
            profile="dev",
            action="get_package_manifest",
        )

        assert result["_success"] is False
        assert "package_id" in result["error"]
