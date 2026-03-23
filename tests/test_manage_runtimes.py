"""Unit tests for manage_runtimes list filters, detach validation, and new actions (mocked SDK)."""

import sys
import os
from unittest.mock import MagicMock, PropertyMock, patch

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from boomi.net.transport.api_error import ApiError

from src.boomi_mcp.categories.runtimes import (
    _action_list,
    _action_detach,
    _match_name_pattern,
    _action_get_release_schedule,
    _action_create_release_schedule,
    _action_update_release_schedule,
    _action_delete_release_schedule,
    _action_get_observability_settings,
    _action_update_observability_settings,
    _action_get_security_policies,
    _action_update_security_policies,
    _action_get_startup_properties,
    _action_reset_counters,
    _action_purge,
    _action_get_connector_versions,
    _action_offboard_node,
    _action_refresh_secrets_manager,
    _action_get_account_cloud_attachment_properties,
    _action_update_account_cloud_attachment_properties,
    _action_list_account_cloud_attachment_summaries,
    _action_get_account_cloud_attachment_summary,
    _action_list_account_cloud_attachment_quotas,
    _action_get_account_cloud_attachment_quota,
    _action_create_account_cloud_attachment_quota,
    _action_update_account_cloud_attachment_quota,
    _action_delete_account_cloud_attachment_quota,
    _action_get_cloud_attachment_properties,
    _action_update_cloud_attachment_properties,
    _action_get_account_cloud_attachment_defaults,
    _action_update_account_cloud_attachment_defaults,
)


# ── Fixtures ─────────────────────────────────────────────────────────


def _make_sdk():
    sdk = MagicMock()
    return sdk


def _make_runtime(id_="rt-1", name="Test Runtime", type_="ATOM", status="ONLINE"):
    """Create a mock SDK Atom object."""
    rt = MagicMock()
    rt.id_ = id_
    rt.name = name
    rt.type_ = MagicMock(value=type_)
    rt.status = MagicMock(value=status)
    rt.host_name = None
    rt.current_version = None
    rt.date_installed = None
    rt.created_by = None
    rt.cloud_id = None
    rt.cloud_name = None
    rt.cloud_molecule_id = None
    rt.cloud_molecule_name = None
    rt.cloud_owner_name = None
    rt.instance_id = None
    rt.status_detail = None
    rt.is_cloud_attachment = None
    rt.purge_history_days = None
    rt.purge_immediate = None
    rt.force_restart_time = None
    rt.capabilities = None
    return rt


def _make_query_result(items, query_token=None):
    """Create a mock query result with .result and .query_token."""
    result = MagicMock()
    result.result = items
    result.query_token = query_token
    return result


def _make_attachment(id_="att-1", atom_id="rt-1", environment_id="env-1"):
    """Create a mock EnvironmentAtomAttachment object."""
    att = MagicMock()
    att.id_ = id_
    att.atom_id = atom_id
    att.environment_id = environment_id
    return att


# ── TestMatchNamePattern ─────────────────────────────────────────────


class TestMatchNamePattern:
    """Unit tests for the _match_name_pattern helper."""

    def test_bare_text_matches_substring(self):
        assert _match_name_pattern("Production Atom", "Prod") is True

    def test_bare_text_no_match(self):
        assert _match_name_pattern("Production Atom", "Staging") is False

    def test_bare_text_case_sensitive(self):
        assert _match_name_pattern("Production Atom", "prod") is False

    def test_prefix_pattern(self):
        assert _match_name_pattern("Production Atom", "Prod%") is True
        assert _match_name_pattern("My Prod Atom", "Prod%") is False

    def test_suffix_pattern(self):
        assert _match_name_pattern("Production Atom", "%Atom") is True
        assert _match_name_pattern("Atom Server", "%Atom") is False

    def test_contains_pattern(self):
        assert _match_name_pattern("Production Atom", "%duct%") is True
        assert _match_name_pattern("Dev Atom", "%duct%") is False

    def test_wildcard_only(self):
        assert _match_name_pattern("Anything", "%") is True
        assert _match_name_pattern("", "%") is True

    def test_empty_pattern(self):
        assert _match_name_pattern("Anything", "") is True

    def test_explicit_percent_preserved(self):
        # %Prod% should match same as substring, not double-wrap
        assert _match_name_pattern("Production Atom", "%Prod%") is True
        assert _match_name_pattern("My Prod Server", "%Prod%") is True
        assert _match_name_pattern("Dev Atom", "%Prod%") is False


# ── TestActionListNameExact (QA-012) ─────────────────────────────────


class TestActionListNameExact:
    """Test config.name exact-match filter for list action."""

    def test_name_exact_match(self):
        sdk = _make_sdk()
        rt = _make_runtime(id_="rt-1", name="Production Atom")
        sdk.atom.query_atom.return_value = _make_query_result([rt])

        result = _action_list(sdk, "dev", name="Production Atom")

        assert result["_success"] is True
        assert result["total_count"] == 1
        assert result["runtimes"][0]["name"] == "Production Atom"
        # Verify SDK was called with EQUALS expression
        call_args = sdk.atom.query_atom.call_args
        qc = call_args.kwargs.get("request_body") or call_args[1].get("request_body")
        expr = qc.query_filter.expression
        assert expr.argument == ["Production Atom"]

    def test_name_exact_no_match(self):
        sdk = _make_sdk()
        sdk.atom.query_atom.return_value = _make_query_result([])

        result = _action_list(sdk, "dev", name="Nonexistent")

        assert result["_success"] is True
        assert result["total_count"] == 0
        assert result["runtimes"] == []

    def test_name_precedence_over_name_pattern(self):
        sdk = _make_sdk()
        rt = _make_runtime(id_="rt-1", name="Exact Name")
        sdk.atom.query_atom.return_value = _make_query_result([rt])

        result = _action_list(sdk, "dev", name="Exact Name", name_pattern="Pattern")

        assert result["_success"] is True
        assert result["total_count"] == 1
        # name_pattern should not have filtered further
        assert result["runtimes"][0]["name"] == "Exact Name"


# ── TestActionListNamePattern (QA-011) ───────────────────────────────


class TestActionListNamePattern:
    """Test config.name_pattern wrapper-side filtering for list action."""

    def _setup_runtimes(self, sdk):
        """Set up SDK with three runtimes for pattern tests."""
        runtimes = [
            _make_runtime(id_="rt-1", name="Prod Atom"),
            _make_runtime(id_="rt-2", name="Dev Atom"),
            _make_runtime(id_="rt-3", name="Prod Molecule"),
        ]
        sdk.atom.query_atom.return_value = _make_query_result(runtimes)
        return runtimes

    def test_bare_text_filters_locally(self):
        sdk = _make_sdk()
        self._setup_runtimes(sdk)

        result = _action_list(sdk, "dev", name_pattern="Prod")

        assert result["_success"] is True
        assert result["total_count"] == 2
        names = [r["name"] for r in result["runtimes"]]
        assert "Prod Atom" in names
        assert "Prod Molecule" in names
        assert "Dev Atom" not in names
        # SDK should have been called with no expression (fetching all)
        call_args = sdk.atom.query_atom.call_args
        qc = call_args.kwargs.get("request_body") or call_args[1].get("request_body")
        assert not hasattr(qc, 'query_filter') or qc.query_filter is None

    def test_prefix_pattern(self):
        sdk = _make_sdk()
        self._setup_runtimes(sdk)

        result = _action_list(sdk, "dev", name_pattern="Prod%")

        assert result["_success"] is True
        assert result["total_count"] == 2
        names = [r["name"] for r in result["runtimes"]]
        assert "Prod Atom" in names
        assert "Prod Molecule" in names

    def test_suffix_pattern(self):
        sdk = _make_sdk()
        self._setup_runtimes(sdk)

        result = _action_list(sdk, "dev", name_pattern="%Atom")

        assert result["_success"] is True
        assert result["total_count"] == 2
        names = [r["name"] for r in result["runtimes"]]
        assert "Prod Atom" in names
        assert "Dev Atom" in names

    def test_wildcard_returns_all(self):
        sdk = _make_sdk()
        self._setup_runtimes(sdk)

        result = _action_list(sdk, "dev", name_pattern="%")

        assert result["_success"] is True
        assert result["total_count"] == 3

    def test_no_match(self):
        sdk = _make_sdk()
        self._setup_runtimes(sdk)

        result = _action_list(sdk, "dev", name_pattern="Staging")

        assert result["_success"] is True
        assert result["total_count"] == 0


# ── TestActionDetachValidation (QA-013) ──────────────────────────────


class TestActionDetachValidation:
    """Test detach action validation for attachment_id vs runtime_id."""

    def test_detach_with_valid_attachment_id_no_env(self):
        """Direct detach with a valid attachment ID should succeed (no pre-flight query)."""
        sdk = _make_sdk()

        result = _action_detach(sdk, "dev", resource_id="att-1")

        assert result["_success"] is True
        assert result["detached_attachment_id"] == "att-1"
        sdk.environment_atom_attachment.delete_environment_atom_attachment.assert_called_once_with(
            id_="att-1"
        )
        # Should NOT query attachments for validation
        sdk.environment_atom_attachment.query_environment_atom_attachment.assert_not_called()

    def test_detach_with_runtime_id_and_env(self):
        """Lookup path: runtime_id + environment_id should find and delete attachment."""
        sdk = _make_sdk()
        att = _make_attachment(id_="att-1", atom_id="rt-1", environment_id="env-1")
        sdk.environment_atom_attachment.query_environment_atom_attachment.return_value = (
            _make_query_result([att])
        )

        result = _action_detach(sdk, "dev", resource_id="rt-1", environment_id="env-1")

        assert result["_success"] is True
        assert result["detached_attachment_id"] == "att-1"
        sdk.environment_atom_attachment.delete_environment_atom_attachment.assert_called_once_with(
            id_="att-1"
        )

    def test_detach_runtime_id_no_env_returns_friendly_error(self):
        """runtime_id without environment_id: catch Invalid compound id, return guidance."""
        sdk = _make_sdk()
        err = ApiError.__new__(ApiError)
        err.error_detail = "Invalid compound id 'rt-1'"
        sdk.environment_atom_attachment.delete_environment_atom_attachment.side_effect = err

        result = _action_detach(sdk, "dev", resource_id="rt-1")

        assert result["_success"] is False
        assert "environment_id is required" in result["error"]

    def test_detach_stale_attachment_id_no_env_raises(self):
        """Stale attachment ID without environment_id: non-compound-id error should propagate."""
        sdk = _make_sdk()
        err = ApiError.__new__(ApiError)
        err.error_detail = "Resource not found"
        sdk.environment_atom_attachment.delete_environment_atom_attachment.side_effect = err

        with pytest.raises(ApiError):
            _action_detach(sdk, "dev", resource_id="att-stale")

    def test_detach_missing_resource_id(self):
        sdk = _make_sdk()

        result = _action_detach(sdk, "dev")

        assert result["_success"] is False
        assert "resource_id is required" in result["error"]

    def test_detach_runtime_id_with_env_no_match(self):
        """runtime_id + environment_id but no matching attachment."""
        sdk = _make_sdk()
        att = _make_attachment(id_="att-1", atom_id="rt-other", environment_id="env-1")
        sdk.environment_atom_attachment.query_environment_atom_attachment.return_value = (
            _make_query_result([att])
        )

        result = _action_detach(sdk, "dev", resource_id="rt-1", environment_id="env-1")

        assert result["_success"] is False
        assert "No attachment found" in result["error"]
        sdk.environment_atom_attachment.delete_environment_atom_attachment.assert_not_called()


# ── Helpers for async mocks ──────────────────────────────────────────


def _make_async_token(token_str="tok-123"):
    """Create a mock async token result."""
    token_result = MagicMock()
    token_result.async_token.token = token_str
    return token_result


# ── TestReleaseSchedule ──────────────────────────────────────────────


class TestGetReleaseSchedule:
    """Tests for the get_release_schedule action."""

    def test_success(self):
        sdk = _make_sdk()
        sched = MagicMock()
        sched.atom_id = "rt-1"
        sched.schedule_type = MagicMock(value="FIRST")
        sched.day_of_week = "MONDAY"
        sched.hour_of_day = 10
        sched.time_zone = "America/New_York"
        sdk.runtime_release_schedule.get_runtime_release_schedule.return_value = sched

        result = _action_get_release_schedule(sdk, "dev", resource_id="rt-1")

        assert result["_success"] is True
        assert result["release_schedule"]["atom_id"] == "rt-1"
        assert result["release_schedule"]["schedule_type"] == "FIRST"
        assert result["release_schedule"]["day_of_week"] == "MONDAY"
        sdk.runtime_release_schedule.get_runtime_release_schedule.assert_called_once_with(id_="rt-1")

    def test_missing_resource_id(self):
        sdk = _make_sdk()
        result = _action_get_release_schedule(sdk, "dev")
        assert result["_success"] is False
        assert "resource_id is required" in result["error"]


class TestCreateReleaseSchedule:
    """Tests for the create_release_schedule action."""

    def test_success(self):
        sdk = _make_sdk()
        sched = MagicMock()
        sched.atom_id = "rt-1"
        sched.schedule_type = MagicMock(value="FIRST")
        sched.day_of_week = "MONDAY"
        sched.hour_of_day = 10
        sched.time_zone = "UTC"
        sdk.runtime_release_schedule.create_runtime_release_schedule.return_value = sched

        result = _action_create_release_schedule(
            sdk, "dev", resource_id="rt-1", schedule_type="FIRST",
            day_of_week="MONDAY", hour_of_day=10, time_zone="UTC"
        )

        assert result["_success"] is True
        assert result["release_schedule"]["schedule_type"] == "FIRST"
        sdk.runtime_release_schedule.create_runtime_release_schedule.assert_called_once()

    def test_missing_schedule_type(self):
        sdk = _make_sdk()
        result = _action_create_release_schedule(sdk, "dev", resource_id="rt-1")
        assert result["_success"] is False
        assert "schedule_type is required" in result["error"]

    def test_missing_resource_id(self):
        sdk = _make_sdk()
        result = _action_create_release_schedule(sdk, "dev", schedule_type="FIRST")
        assert result["_success"] is False
        assert "resource_id" in result["error"]

    def test_invalid_hour(self):
        sdk = _make_sdk()
        result = _action_create_release_schedule(
            sdk, "dev", resource_id="rt-1", schedule_type="FIRST", hour_of_day="abc"
        )
        assert result["_success"] is False
        assert "hour_of_day" in result["error"]


class TestUpdateReleaseSchedule:
    """Tests for the update_release_schedule action."""

    def test_success(self):
        sdk = _make_sdk()
        sched = MagicMock()
        sched.atom_id = "rt-1"
        sched.schedule_type = MagicMock(value="LAST")
        sched.day_of_week = "FRIDAY"
        sched.hour_of_day = 22
        sched.time_zone = "UTC"
        sdk.runtime_release_schedule.update_runtime_release_schedule.return_value = sched

        result = _action_update_release_schedule(
            sdk, "dev", resource_id="rt-1", schedule_type="LAST"
        )

        assert result["_success"] is True
        assert result["release_schedule"]["schedule_type"] == "LAST"
        sdk.runtime_release_schedule.update_runtime_release_schedule.assert_called_once()

    def test_missing_resource_id(self):
        sdk = _make_sdk()
        result = _action_update_release_schedule(sdk, "dev")
        assert result["_success"] is False
        assert "resource_id is required" in result["error"]


class TestDeleteReleaseSchedule:
    """Tests for the delete_release_schedule action."""

    def test_success(self):
        sdk = _make_sdk()
        result = _action_delete_release_schedule(sdk, "dev", resource_id="rt-1")
        assert result["_success"] is True
        assert "NEVER" in result["message"]
        sdk.runtime_release_schedule.delete_runtime_release_schedule.assert_called_once_with(id_="rt-1")

    def test_missing_resource_id(self):
        sdk = _make_sdk()
        result = _action_delete_release_schedule(sdk, "dev")
        assert result["_success"] is False
        assert "resource_id is required" in result["error"]


# ── TestObservabilitySettings ────────────────────────────────────────


class TestGetObservabilitySettings:
    """Tests for the get_observability_settings action (async raw polling)."""

    def test_success(self):
        import json as json_mod
        sdk = _make_sdk()

        token_result = MagicMock()
        token_result.async_token.token = "obs-token-123"
        sdk.runtime_observability_settings.async_get_runtime_observability_settings.return_value = token_result

        svc = sdk.runtime_observability_settings
        svc.base_url = "https://api.boomi.com/api/rest/v1/acct-123"
        svc.send_request.return_value = (
            json_mod.dumps({"result": [{"runtimeId": "rt-1", "generalSettings": {}}]}),
            200,
            "application/json",
        )
        svc.get_access_token.return_value = MagicMock()
        svc.get_basic_auth.return_value = MagicMock()

        result = _action_get_observability_settings(sdk, "dev", resource_id="rt-1")

        assert result["_success"] is True
        assert result["runtime_id"] == "rt-1"
        assert len(result["settings"]) == 1

    def test_missing_resource_id(self):
        sdk = _make_sdk()
        result = _action_get_observability_settings(sdk, "dev")
        assert result["_success"] is False
        assert "resource_id is required" in result["error"]


class TestUpdateObservabilitySettings:
    """Tests for the update_observability_settings action."""

    def test_success(self):
        sdk = _make_sdk()
        resp = MagicMock()
        resp.runtime_id = "rt-1"
        resp.general_settings = MagicMock()
        resp.log_settings = None
        resp.metric_settings = None
        resp.trace_settings = None
        sdk.runtime_observability_settings.update_runtime_observability_settings.return_value = resp

        result = _action_update_observability_settings(
            sdk, "dev", resource_id="rt-1", request_body=MagicMock()
        )

        assert result["_success"] is True
        sdk.runtime_observability_settings.update_runtime_observability_settings.assert_called_once()

    def test_missing_resource_id(self):
        sdk = _make_sdk()
        result = _action_update_observability_settings(sdk, "dev", request_body=MagicMock())
        assert result["_success"] is False
        assert "resource_id is required" in result["error"]

    def test_missing_request_body(self):
        sdk = _make_sdk()
        result = _action_update_observability_settings(sdk, "dev", resource_id="rt-1")
        assert result["_success"] is False
        assert "request_body is required" in result["error"]


# ── TestSecurityPolicies ─────────────────────────────────────────────


class TestGetSecurityPolicies:
    """Tests for the get_security_policies action (async)."""

    @patch("src.boomi_mcp.categories.runtimes.poll_async_result")
    def test_success(self, mock_poll):
        sdk = _make_sdk()
        item = MagicMock()
        item.atom_id = "rt-1"
        item.common = MagicMock()
        item.browser = None
        item.runner = None
        item.worker = None

        response = MagicMock()
        response.result = [item]
        mock_poll.return_value = response

        result = _action_get_security_policies(sdk, "dev", resource_id="rt-1")

        assert result["_success"] is True
        assert result["security_policies"]["atom_id"] == "rt-1"
        mock_poll.assert_called_once()

    def test_missing_resource_id(self):
        sdk = _make_sdk()
        result = _action_get_security_policies(sdk, "dev")
        assert result["_success"] is False
        assert "resource_id is required" in result["error"]


class TestUpdateSecurityPolicies:
    """Tests for the update_security_policies action."""

    def test_success(self):
        sdk = _make_sdk()
        resp = MagicMock()
        resp.atom_id = "rt-1"
        resp.common = MagicMock()
        resp.browser = None
        resp.runner = None
        resp.worker = None
        sdk.atom_security_policies.update_atom_security_policies.return_value = resp

        result = _action_update_security_policies(
            sdk, "dev", resource_id="rt-1", request_body=MagicMock()
        )

        assert result["_success"] is True
        sdk.atom_security_policies.update_atom_security_policies.assert_called_once()

    def test_missing_resource_id(self):
        sdk = _make_sdk()
        result = _action_update_security_policies(sdk, "dev", request_body=MagicMock())
        assert result["_success"] is False
        assert "resource_id is required" in result["error"]

    def test_missing_request_body(self):
        sdk = _make_sdk()
        result = _action_update_security_policies(sdk, "dev", resource_id="rt-1")
        assert result["_success"] is False
        assert "request_body is required" in result["error"]


# ── TestStartupProperties ────────────────────────────────────────────


class TestGetStartupProperties:
    """Tests for the get_startup_properties action."""

    def test_success(self):
        sdk = _make_sdk()
        prop1 = MagicMock()
        prop1.key = "com.boomi.container.maxMemory"
        prop1.value = "2048m"
        prop2 = MagicMock()
        prop2.key = "com.boomi.container.maxThreads"
        prop2.value = "200"
        resp = MagicMock()
        resp.id_ = "rt-1"
        resp.property = [prop1, prop2]
        sdk.atom_startup_properties.get_atom_startup_properties.return_value = resp

        result = _action_get_startup_properties(sdk, "dev", resource_id="rt-1")

        assert result["_success"] is True
        assert result["total_count"] == 2
        assert result["properties"][0]["key"] == "com.boomi.container.maxMemory"
        sdk.atom_startup_properties.get_atom_startup_properties.assert_called_once_with(id_="rt-1")

    def test_empty_properties(self):
        sdk = _make_sdk()
        resp = MagicMock()
        resp.id_ = "rt-1"
        resp.property = None
        sdk.atom_startup_properties.get_atom_startup_properties.return_value = resp

        result = _action_get_startup_properties(sdk, "dev", resource_id="rt-1")

        assert result["_success"] is True
        assert result["total_count"] == 0

    def test_missing_resource_id(self):
        sdk = _make_sdk()
        result = _action_get_startup_properties(sdk, "dev")
        assert result["_success"] is False
        assert "resource_id is required" in result["error"]


# ── TestResetCounters ────────────────────────────────────────────────


class TestResetCounters:
    """Tests for the reset_counters action."""

    def test_success(self):
        sdk = _make_sdk()
        sdk.atom_counters.update_atom_counters.return_value = MagicMock()

        result = _action_reset_counters(sdk, "dev", resource_id="rt-1")

        assert result["_success"] is True
        assert "reset" in result["message"].lower()
        sdk.atom_counters.update_atom_counters.assert_called_once()

    def test_missing_resource_id(self):
        sdk = _make_sdk()
        result = _action_reset_counters(sdk, "dev")
        assert result["_success"] is False
        assert "resource_id is required" in result["error"]


# ── TestPurge ────────────────────────────────────────────────────────


class TestPurge:
    """Tests for the purge action."""

    def test_success(self):
        sdk = _make_sdk()
        sdk.atom_purge.update_atom_purge.return_value = MagicMock()

        result = _action_purge(sdk, "dev", resource_id="rt-1")

        assert result["_success"] is True
        assert "purge" in result["message"].lower()
        sdk.atom_purge.update_atom_purge.assert_called_once()

    def test_missing_resource_id(self):
        sdk = _make_sdk()
        result = _action_purge(sdk, "dev")
        assert result["_success"] is False
        assert "resource_id is required" in result["error"]


# ── TestGetConnectorVersions ─────────────────────────────────────────


class TestGetConnectorVersions:
    """Tests for the get_connector_versions action."""

    def test_success(self):
        sdk = _make_sdk()
        cv1 = MagicMock()
        cv1.name = "HTTP Client"
        cv1.version = "2.5.0"
        cv2 = MagicMock()
        cv2.name = "Database"
        cv2.version = "1.3.0"
        resp = MagicMock()
        resp.id_ = "rt-1"
        resp.connector_version = [cv1, cv2]
        sdk.atom_connector_versions.get_atom_connector_versions.return_value = resp

        result = _action_get_connector_versions(sdk, "dev", resource_id="rt-1")

        assert result["_success"] is True
        assert result["total_count"] == 2
        assert result["connector_versions"][0]["name"] == "HTTP Client"
        sdk.atom_connector_versions.get_atom_connector_versions.assert_called_once_with(id_="rt-1")

    def test_empty_connectors(self):
        sdk = _make_sdk()
        resp = MagicMock()
        resp.id_ = "rt-1"
        resp.connector_version = None
        sdk.atom_connector_versions.get_atom_connector_versions.return_value = resp

        result = _action_get_connector_versions(sdk, "dev", resource_id="rt-1")

        assert result["_success"] is True
        assert result["total_count"] == 0

    def test_missing_resource_id(self):
        sdk = _make_sdk()
        result = _action_get_connector_versions(sdk, "dev")
        assert result["_success"] is False
        assert "resource_id is required" in result["error"]


# ── TestOffboardNode ─────────────────────────────────────────────────


class TestOffboardNode:
    """Tests for the offboard_node action."""

    def test_success_single_node(self):
        sdk = _make_sdk()
        sdk.node_offboard.create_node_offboard.return_value = MagicMock()

        result = _action_offboard_node(sdk, "dev", resource_id="rt-1", node_id="node-1")

        assert result["_success"] is True
        assert result["node_id"] == ["node-1"]
        sdk.node_offboard.create_node_offboard.assert_called_once()

    def test_success_multiple_nodes(self):
        sdk = _make_sdk()
        sdk.node_offboard.create_node_offboard.return_value = MagicMock()

        result = _action_offboard_node(
            sdk, "dev", resource_id="rt-1", node_id=["node-1", "node-2"]
        )

        assert result["_success"] is True
        assert result["node_id"] == ["node-1", "node-2"]

    def test_missing_resource_id(self):
        sdk = _make_sdk()
        result = _action_offboard_node(sdk, "dev", node_id="node-1")
        assert result["_success"] is False
        assert "resource_id" in result["error"]

    def test_missing_node_id(self):
        sdk = _make_sdk()
        result = _action_offboard_node(sdk, "dev", resource_id="rt-1")
        assert result["_success"] is False
        assert "node_id is required" in result["error"]


# ── TestRefreshSecretsManager ────────────────────────────────────────


class TestRefreshSecretsManager:
    """Tests for the refresh_secrets_manager action."""

    def test_success_with_provider(self):
        sdk = _make_sdk()
        resp = MagicMock()
        resp.message = "Cache refreshed successfully"
        sdk.refresh_secrets_manager.refresh_secrets_manager.return_value = resp

        result = _action_refresh_secrets_manager(sdk, "dev", provider="AWS")

        assert result["_success"] is True
        assert "refreshed" in result["message"].lower()
        sdk.refresh_secrets_manager.refresh_secrets_manager.assert_called_once()

    def test_success_without_provider(self):
        sdk = _make_sdk()
        resp = MagicMock()
        resp.message = None
        sdk.refresh_secrets_manager.refresh_secrets_manager.return_value = resp

        result = _action_refresh_secrets_manager(sdk, "dev")

        assert result["_success"] is True
        assert result["message"]  # Should have a default message
        sdk.refresh_secrets_manager.refresh_secrets_manager.assert_called_once()


# ── TestGetAccountCloudAttachmentProperties ──────────────────────────


class TestGetAccountCloudAttachmentProperties:
    """Tests for the get_account_cloud_attachment_properties action (async)."""

    @patch("src.boomi_mcp.categories.runtimes.poll_async_result")
    def test_success(self, mock_poll):
        sdk = _make_sdk()
        item = MagicMock()
        item.container_id = "cid-1"

        response = MagicMock()
        response.result = [item]
        mock_poll.return_value = response

        result = _action_get_account_cloud_attachment_properties(sdk, "dev", resource_id="cid-1")

        assert result["_success"] is True
        assert "properties" in result
        mock_poll.assert_called_once()

    def test_missing_resource_id(self):
        sdk = _make_sdk()
        result = _action_get_account_cloud_attachment_properties(sdk, "dev")
        assert result["_success"] is False
        assert "resource_id" in result["error"]


class TestUpdateAccountCloudAttachmentProperties:
    """Tests for the update_account_cloud_attachment_properties action."""

    def test_success(self):
        sdk = _make_sdk()
        resp = MagicMock()
        resp.container_id = "cid-1"
        sdk.account_cloud_attachment_properties.update_account_cloud_attachment_properties.return_value = resp

        result = _action_update_account_cloud_attachment_properties(
            sdk, "dev", resource_id="cid-1", request_body=MagicMock()
        )

        assert result["_success"] is True
        sdk.account_cloud_attachment_properties.update_account_cloud_attachment_properties.assert_called_once()

    def test_missing_resource_id(self):
        sdk = _make_sdk()
        result = _action_update_account_cloud_attachment_properties(sdk, "dev", request_body=MagicMock())
        assert result["_success"] is False
        assert "resource_id" in result["error"]

    def test_missing_request_body(self):
        sdk = _make_sdk()
        result = _action_update_account_cloud_attachment_properties(sdk, "dev", resource_id="cid-1")
        assert result["_success"] is False
        assert "request_body" in result["error"]


# ── TestListAccountCloudAttachmentSummaries ──────────────────────────


class TestListAccountCloudAttachmentSummaries:
    """Tests for the list_account_cloud_attachment_summaries action."""

    def test_success(self):
        sdk = _make_sdk()
        item = MagicMock()
        item.id_ = "sum-1"
        query_result = MagicMock()
        query_result.result = [item]
        query_result.query_token = None
        sdk.account_cloud_attachment_summary.query_account_cloud_attachment_summary.return_value = query_result

        result = _action_list_account_cloud_attachment_summaries(sdk, "dev")

        assert result["_success"] is True
        assert result["total_count"] == 1
        sdk.account_cloud_attachment_summary.query_account_cloud_attachment_summary.assert_called_once()

    def test_with_cloud_id_filter(self):
        sdk = _make_sdk()
        query_result = MagicMock()
        query_result.result = []
        query_result.query_token = None
        sdk.account_cloud_attachment_summary.query_account_cloud_attachment_summary.return_value = query_result

        result = _action_list_account_cloud_attachment_summaries(sdk, "dev", cloud_id="cloud-1")

        assert result["_success"] is True
        assert result["total_count"] == 0


# ── TestGetAccountCloudAttachmentSummary ─────────────────────────────


class TestGetAccountCloudAttachmentSummary:
    """Tests for the get_account_cloud_attachment_summary action."""

    def test_success(self):
        sdk = _make_sdk()
        resp = MagicMock()
        resp.id_ = "sum-1"
        sdk.account_cloud_attachment_summary.get_account_cloud_attachment_summary.return_value = resp

        result = _action_get_account_cloud_attachment_summary(sdk, "dev", resource_id="sum-1")

        assert result["_success"] is True
        assert "summary" in result
        sdk.account_cloud_attachment_summary.get_account_cloud_attachment_summary.assert_called_once_with(
            id_="sum-1"
        )

    def test_missing_resource_id(self):
        sdk = _make_sdk()
        result = _action_get_account_cloud_attachment_summary(sdk, "dev")
        assert result["_success"] is False
        assert "resource_id" in result["error"]


# ── TestListAccountCloudAttachmentQuotas ─────────────────────────────


class TestListAccountCloudAttachmentQuotas:
    """Tests for the list_account_cloud_attachment_quotas action (bulk)."""

    def test_success(self):
        sdk = _make_sdk()
        item = MagicMock()
        item.id_ = "q-1"
        bulk_result = MagicMock()
        bulk_result.result = [item]
        sdk.account_cloud_attachment_quota.bulk_account_cloud_attachment_quota.return_value = bulk_result

        result = _action_list_account_cloud_attachment_quotas(
            sdk, "dev", resource_ids=["q-1"]
        )

        assert result["_success"] is True
        assert result["total_count"] == 1
        sdk.account_cloud_attachment_quota.bulk_account_cloud_attachment_quota.assert_called_once()

    def test_missing_resource_ids(self):
        sdk = _make_sdk()
        result = _action_list_account_cloud_attachment_quotas(sdk, "dev")
        assert result["_success"] is False
        assert "resource_ids" in result["error"]

    def test_string_resource_ids_converted_to_list(self):
        sdk = _make_sdk()
        bulk_result = MagicMock()
        bulk_result.result = []
        sdk.account_cloud_attachment_quota.bulk_account_cloud_attachment_quota.return_value = bulk_result

        result = _action_list_account_cloud_attachment_quotas(
            sdk, "dev", resource_ids="q-single"
        )

        assert result["_success"] is True


# ── TestGetAccountCloudAttachmentQuota ───────────────────────────────


class TestGetAccountCloudAttachmentQuota:
    """Tests for the get_account_cloud_attachment_quota action."""

    def test_success(self):
        sdk = _make_sdk()
        resp = MagicMock()
        resp.id_ = "q-1"
        sdk.account_cloud_attachment_quota.get_account_cloud_attachment_quota.return_value = resp

        result = _action_get_account_cloud_attachment_quota(sdk, "dev", resource_id="q-1")

        assert result["_success"] is True
        assert "quota" in result
        sdk.account_cloud_attachment_quota.get_account_cloud_attachment_quota.assert_called_once_with(
            id_="q-1"
        )

    def test_missing_resource_id(self):
        sdk = _make_sdk()
        result = _action_get_account_cloud_attachment_quota(sdk, "dev")
        assert result["_success"] is False
        assert "resource_id" in result["error"]


# ── TestCreateAccountCloudAttachmentQuota ────────────────────────────


class TestCreateAccountCloudAttachmentQuota:
    """Tests for the create_account_cloud_attachment_quota action."""

    def test_success(self):
        sdk = _make_sdk()
        resp = MagicMock()
        resp.id_ = "q-new"
        sdk.account_cloud_attachment_quota.create_account_cloud_attachment_quota.return_value = resp

        result = _action_create_account_cloud_attachment_quota(
            sdk, "dev", request_body=MagicMock()
        )

        assert result["_success"] is True
        assert "quota" in result
        sdk.account_cloud_attachment_quota.create_account_cloud_attachment_quota.assert_called_once()

    def test_missing_request_body(self):
        sdk = _make_sdk()
        result = _action_create_account_cloud_attachment_quota(sdk, "dev")
        assert result["_success"] is False
        assert "request_body" in result["error"]


# ── TestUpdateAccountCloudAttachmentQuota ────────────────────────────


class TestUpdateAccountCloudAttachmentQuota:
    """Tests for the update_account_cloud_attachment_quota action."""

    def test_success(self):
        sdk = _make_sdk()
        resp = MagicMock()
        resp.id_ = "q-1"
        sdk.account_cloud_attachment_quota.update_account_cloud_attachment_quota.return_value = resp

        result = _action_update_account_cloud_attachment_quota(
            sdk, "dev", resource_id="q-1", request_body=MagicMock()
        )

        assert result["_success"] is True
        sdk.account_cloud_attachment_quota.update_account_cloud_attachment_quota.assert_called_once()

    def test_missing_resource_id(self):
        sdk = _make_sdk()
        result = _action_update_account_cloud_attachment_quota(sdk, "dev", request_body=MagicMock())
        assert result["_success"] is False
        assert "resource_id" in result["error"]

    def test_missing_request_body(self):
        sdk = _make_sdk()
        result = _action_update_account_cloud_attachment_quota(sdk, "dev", resource_id="q-1")
        assert result["_success"] is False
        assert "request_body" in result["error"]


# ── TestDeleteAccountCloudAttachmentQuota ────────────────────────────


class TestDeleteAccountCloudAttachmentQuota:
    """Tests for the delete_account_cloud_attachment_quota action."""

    def test_success(self):
        sdk = _make_sdk()
        result = _action_delete_account_cloud_attachment_quota(sdk, "dev", resource_id="q-1")

        assert result["_success"] is True
        assert result["deleted_id"] == "q-1"
        sdk.account_cloud_attachment_quota.delete_account_cloud_attachment_quota.assert_called_once_with(
            id_="q-1"
        )

    def test_missing_resource_id(self):
        sdk = _make_sdk()
        result = _action_delete_account_cloud_attachment_quota(sdk, "dev")
        assert result["_success"] is False
        assert "resource_id" in result["error"]


# ── TestGetCloudAttachmentProperties ─────────────────────────────────


class TestGetCloudAttachmentProperties:
    """Tests for the get_cloud_attachment_properties action (async)."""

    @patch("src.boomi_mcp.categories.runtimes.poll_async_result")
    def test_success(self, mock_poll):
        sdk = _make_sdk()
        item = MagicMock()
        item.runtime_id = "rt-1"

        response = MagicMock()
        response.result = [item]
        mock_poll.return_value = response

        result = _action_get_cloud_attachment_properties(sdk, "dev", resource_id="rt-1")

        assert result["_success"] is True
        assert "properties" in result
        mock_poll.assert_called_once()

    def test_missing_resource_id(self):
        sdk = _make_sdk()
        result = _action_get_cloud_attachment_properties(sdk, "dev")
        assert result["_success"] is False
        assert "resource_id" in result["error"]


class TestUpdateCloudAttachmentProperties:
    """Tests for the update_cloud_attachment_properties action."""

    def test_success(self):
        sdk = _make_sdk()
        resp = MagicMock()
        resp.runtime_id = "rt-1"
        sdk.cloud_attachment_properties.update_cloud_attachment_properties.return_value = resp

        result = _action_update_cloud_attachment_properties(
            sdk, "dev", resource_id="rt-1", request_body=MagicMock()
        )

        assert result["_success"] is True
        sdk.cloud_attachment_properties.update_cloud_attachment_properties.assert_called_once()

    def test_missing_resource_id(self):
        sdk = _make_sdk()
        result = _action_update_cloud_attachment_properties(sdk, "dev", request_body=MagicMock())
        assert result["_success"] is False
        assert "resource_id" in result["error"]

    def test_missing_request_body(self):
        sdk = _make_sdk()
        result = _action_update_cloud_attachment_properties(sdk, "dev", resource_id="rt-1")
        assert result["_success"] is False
        assert "request_body" in result["error"]


# ── TestGetAccountCloudAttachmentDefaults ────────────────────────────


class TestGetAccountCloudAttachmentDefaults:
    """Tests for the get_account_cloud_attachment_defaults action (async)."""

    @patch("src.boomi_mcp.categories.runtimes.poll_async_result")
    def test_success(self, mock_poll):
        sdk = _make_sdk()
        item = MagicMock()
        item.container_id = "cid-1"

        response = MagicMock()
        response.result = [item]
        mock_poll.return_value = response

        result = _action_get_account_cloud_attachment_defaults(sdk, "dev", resource_id="cid-1")

        assert result["_success"] is True
        assert "defaults" in result
        mock_poll.assert_called_once()

    def test_missing_resource_id(self):
        sdk = _make_sdk()
        result = _action_get_account_cloud_attachment_defaults(sdk, "dev")
        assert result["_success"] is False
        assert "resource_id" in result["error"]


class TestUpdateAccountCloudAttachmentDefaults:
    """Tests for the update_account_cloud_attachment_defaults action."""

    def test_success(self):
        sdk = _make_sdk()
        resp = MagicMock()
        resp.container_id = "cid-1"
        sdk.account_cloud_attachment_properties_default.update_account_cloud_attachment_properties_default.return_value = resp

        result = _action_update_account_cloud_attachment_defaults(
            sdk, "dev", resource_id="cid-1", request_body=MagicMock()
        )

        assert result["_success"] is True
        sdk.account_cloud_attachment_properties_default.update_account_cloud_attachment_properties_default.assert_called_once()

    def test_missing_resource_id(self):
        sdk = _make_sdk()
        result = _action_update_account_cloud_attachment_defaults(sdk, "dev", request_body=MagicMock())
        assert result["_success"] is False
        assert "resource_id" in result["error"]

    def test_missing_request_body(self):
        sdk = _make_sdk()
        result = _action_update_account_cloud_attachment_defaults(sdk, "dev", resource_id="cid-1")
        assert result["_success"] is False
        assert "request_body" in result["error"]
