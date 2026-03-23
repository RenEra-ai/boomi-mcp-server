"""Unit tests for manage_schedules actions (mocked SDK)."""

import base64
import json
import os
import sys
from types import SimpleNamespace
from unittest.mock import MagicMock, patch, call

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from boomi_mcp.categories.schedules import (
    manage_schedules_action,
    _schedule_id_from_ids,
    _schedule_status_to_dict,
)


# ── Helpers ──────────────────────────────────────────────────────────


def _mock_process_schedule(schedule_id="sched-001", process_id="proc-001",
                           atom_id="atom-001", schedules=None, retry=None):
    """Create a mock ProcessSchedules object."""
    ps = MagicMock()
    ps.id_ = schedule_id
    ps.process_id = process_id
    ps.atom_id = atom_id
    if schedules is None:
        sched = MagicMock()
        sched.minutes = "0"
        sched.hours = "9"
        sched.days_of_month = "*"
        sched.months = "*"
        sched.days_of_week = "*"
        sched.years = "*"
        ps.schedule = [sched]
    else:
        ps.schedule = schedules
    if retry is None:
        r = MagicMock()
        r.max_retry = 5
        ps.retry = r
    else:
        ps.retry = retry
    return ps


def _mock_schedule_status(status_id="sched-001", process_id="proc-001",
                          atom_id="atom-001", enabled=True):
    """Create a mock ProcessScheduleStatus object."""
    status = MagicMock()
    status.id_ = status_id
    status.process_id = process_id
    status.atom_id = atom_id
    status.enabled = enabled
    return status


def _mock_query_result(results, query_token=None):
    """Create a mock query response with result list and optional query_token."""
    qr = MagicMock()
    qr.result = results
    qr.query_token = query_token
    return qr


# ── Existing action: list ─────────────────────────────────────────────


class TestActionList:
    def test_list_no_filters(self):
        """list with no filters queries all schedules."""
        sdk = MagicMock()
        sdk.process_schedules.query_process_schedules.return_value = _mock_query_result(
            [_mock_process_schedule()]
        )

        result = manage_schedules_action(sdk, "dev", "list")

        assert result["_success"] is True
        assert result["total_count"] == 1
        assert result["active_count"] == 1
        sdk.process_schedules.query_process_schedules.assert_called_once()

    def test_list_with_process_id_filter(self):
        """list with process_id builds PROCESSID filter."""
        sdk = MagicMock()
        sdk.process_schedules.query_process_schedules.return_value = _mock_query_result([])

        result = manage_schedules_action(sdk, "dev", "list", config_data={"process_id": "proc-123"})

        assert result["_success"] is True
        assert result["total_count"] == 0
        call_args = sdk.process_schedules.query_process_schedules.call_args
        query_config = call_args.kwargs.get("request_body")
        assert query_config is not None

    def test_list_with_atom_id_filter(self):
        """list with atom_id builds ATOMID filter."""
        sdk = MagicMock()
        sdk.process_schedules.query_process_schedules.return_value = _mock_query_result([])

        result = manage_schedules_action(sdk, "dev", "list", config_data={"atom_id": "atom-123"})

        assert result["_success"] is True
        sdk.process_schedules.query_process_schedules.assert_called_once()

    def test_list_both_filters_rejected(self):
        """list with both process_id and atom_id returns error."""
        sdk = MagicMock()

        result = manage_schedules_action(
            sdk, "dev", "list",
            config_data={"process_id": "p1", "atom_id": "a1"},
        )

        assert result["_success"] is False
        assert "Cannot filter by both" in result["error"]


# ── Existing action: get ──────────────────────────────────────────────


class TestActionGet:
    def test_get_by_resource_id(self):
        """get with resource_id calls SDK directly."""
        sdk = MagicMock()
        sdk.process_schedules.get_process_schedules.return_value = _mock_process_schedule()

        result = manage_schedules_action(sdk, "dev", "get", resource_id="sched-b64")

        assert result["_success"] is True
        assert "schedule" in result
        sdk.process_schedules.get_process_schedules.assert_called_once_with(id_="sched-b64")

    def test_get_by_process_and_atom_id(self):
        """get with process_id + atom_id builds schedule ID."""
        sdk = MagicMock()
        sdk.process_schedules.get_process_schedules.return_value = _mock_process_schedule()

        result = manage_schedules_action(
            sdk, "dev", "get",
            config_data={"process_id": "proc-1", "atom_id": "atom-1"},
        )

        assert result["_success"] is True
        expected_id = _schedule_id_from_ids("atom-1", "proc-1")
        sdk.process_schedules.get_process_schedules.assert_called_once_with(id_=expected_id)

    def test_get_missing_ids_returns_error(self):
        """get without resource_id or both ids returns error."""
        sdk = MagicMock()

        result = manage_schedules_action(sdk, "dev", "get", config_data={"process_id": "p1"})

        assert result["_success"] is False
        assert "resource_id" in result["error"]


# ── Existing action: update ───────────────────────────────────────────


class TestActionUpdate:
    def test_update_with_cron(self):
        """update with valid cron and resource_id succeeds."""
        sdk = MagicMock()
        sdk.process_schedules.update_process_schedules.return_value = _mock_process_schedule()

        rid = _schedule_id_from_ids("atom-1", "proc-1")
        result = manage_schedules_action(
            sdk, "dev", "update",
            resource_id=rid,
            config_data={"cron": "0 9 * * *"},
        )

        assert result["_success"] is True
        assert result["cron_applied"] == "0 9 * * *"
        sdk.process_schedules.update_process_schedules.assert_called_once()

    def test_update_missing_cron_returns_error(self):
        """update without cron returns error."""
        sdk = MagicMock()

        result = manage_schedules_action(sdk, "dev", "update", resource_id="some-id")

        assert result["_success"] is False
        assert "cron" in result["error"]


# ── Existing action: delete ───────────────────────────────────────────


class TestActionDelete:
    def test_delete_clears_schedule(self):
        """delete sets empty schedule array."""
        sdk = MagicMock()
        cleared = _mock_process_schedule(schedules=[])
        sdk.process_schedules.update_process_schedules.return_value = cleared

        rid = _schedule_id_from_ids("atom-1", "proc-1")
        result = manage_schedules_action(sdk, "dev", "delete", resource_id=rid)

        assert result["_success"] is True
        assert "note" in result
        sdk.process_schedules.update_process_schedules.assert_called_once()

    def test_delete_missing_ids_returns_error(self):
        """delete without resource_id or both ids returns error."""
        sdk = MagicMock()

        result = manage_schedules_action(sdk, "dev", "delete")

        assert result["_success"] is False
        assert "resource_id" in result["error"]


# ── New action: list_status ───────────────────────────────────────────


class TestActionListStatus:
    def test_list_status_no_filters(self):
        """list_status with no filters queries all statuses."""
        sdk = MagicMock()
        sdk.process_schedule_status.query_process_schedule_status.return_value = (
            _mock_query_result([_mock_schedule_status()])
        )

        result = manage_schedules_action(sdk, "dev", "list_status")

        assert result["_success"] is True
        assert result["total_count"] == 1
        assert len(result["statuses"]) == 1
        assert result["statuses"][0]["enabled"] is True
        sdk.process_schedule_status.query_process_schedule_status.assert_called_once()

    def test_list_status_with_process_id_filter(self):
        """list_status with process_id builds PROCESSID filter."""
        sdk = MagicMock()
        sdk.process_schedule_status.query_process_schedule_status.return_value = (
            _mock_query_result([])
        )

        result = manage_schedules_action(
            sdk, "dev", "list_status",
            config_data={"process_id": "proc-123"},
        )

        assert result["_success"] is True
        assert result["total_count"] == 0
        call_args = sdk.process_schedule_status.query_process_schedule_status.call_args
        query_config = call_args.kwargs.get("request_body")
        assert query_config is not None
        # Verify the filter has process_id property
        qf = query_config.query_filter
        expr = qf.expression
        prop_val = expr.property
        prop_str = prop_val.value if hasattr(prop_val, 'value') else str(prop_val)
        assert prop_str == "processId"

    def test_list_status_with_atom_id_filter(self):
        """list_status with atom_id builds ATOMID filter."""
        sdk = MagicMock()
        sdk.process_schedule_status.query_process_schedule_status.return_value = (
            _mock_query_result([])
        )

        result = manage_schedules_action(
            sdk, "dev", "list_status",
            config_data={"atom_id": "atom-456"},
        )

        assert result["_success"] is True
        call_args = sdk.process_schedule_status.query_process_schedule_status.call_args
        query_config = call_args.kwargs.get("request_body")
        qf = query_config.query_filter
        expr = qf.expression
        prop_val = expr.property
        prop_str = prop_val.value if hasattr(prop_val, 'value') else str(prop_val)
        assert prop_str == "atomId"

    def test_list_status_pagination(self):
        """list_status paginates through query_more."""
        sdk = MagicMock()
        page1 = _mock_query_result(
            [_mock_schedule_status(status_id="s1")],
            query_token="token-page2",
        )
        page2 = _mock_query_result(
            [_mock_schedule_status(status_id="s2")],
        )
        sdk.process_schedule_status.query_process_schedule_status.return_value = page1
        sdk.process_schedule_status.query_more_process_schedule_status.return_value = page2

        result = manage_schedules_action(sdk, "dev", "list_status")

        assert result["_success"] is True
        assert result["total_count"] == 2
        sdk.process_schedule_status.query_more_process_schedule_status.assert_called_once_with(
            request_body="token-page2"
        )


# ── New action: get_status ────────────────────────────────────────────


class TestActionGetStatus:
    def test_get_status_by_resource_id(self):
        """get_status with resource_id calls SDK directly."""
        sdk = MagicMock()
        sdk.process_schedule_status.get_process_schedule_status.return_value = (
            _mock_schedule_status()
        )

        result = manage_schedules_action(sdk, "dev", "get_status", resource_id="sched-b64")

        assert result["_success"] is True
        assert "status" in result
        assert result["status"]["enabled"] is True
        sdk.process_schedule_status.get_process_schedule_status.assert_called_once_with(
            id_="sched-b64"
        )

    def test_get_status_by_process_and_atom_id(self):
        """get_status with process_id + atom_id builds schedule ID."""
        sdk = MagicMock()
        sdk.process_schedule_status.get_process_schedule_status.return_value = (
            _mock_schedule_status()
        )

        result = manage_schedules_action(
            sdk, "dev", "get_status",
            config_data={"process_id": "proc-1", "atom_id": "atom-1"},
        )

        assert result["_success"] is True
        expected_id = _schedule_id_from_ids("atom-1", "proc-1")
        sdk.process_schedule_status.get_process_schedule_status.assert_called_once_with(
            id_=expected_id
        )

    def test_get_status_missing_ids_returns_error(self):
        """get_status without resource_id or both ids returns error."""
        sdk = MagicMock()

        result = manage_schedules_action(
            sdk, "dev", "get_status",
            config_data={"process_id": "p1"},
        )

        assert result["_success"] is False
        assert "resource_id" in result["error"]


# ── New action: enable ────────────────────────────────────────────────


class TestActionEnable:
    def test_enable_by_resource_id(self):
        """enable with resource_id calls update with enabled=True."""
        sdk = MagicMock()
        sdk.process_schedule_status.update_process_schedule_status.return_value = (
            _mock_schedule_status(enabled=True)
        )

        # Use valid base64: CPSatom-1:process-1
        sched_id = "Q1BTYXRvbS0xOnByb2Nlc3MtMQ"
        result = manage_schedules_action(sdk, "dev", "enable", resource_id=sched_id)

        assert result["_success"] is True
        assert result["message"] == "Schedule enabled"
        assert result["status"]["enabled"] is True
        call_args = sdk.process_schedule_status.update_process_schedule_status.call_args
        assert call_args.kwargs["id_"] == sched_id
        body = call_args.kwargs["request_body"]
        assert body.enabled is True
        assert body.atom_id == "atom-1"
        assert body.process_id == "process-1"

    def test_enable_by_process_and_atom_id(self):
        """enable with process_id + atom_id builds schedule ID."""
        sdk = MagicMock()
        sdk.process_schedule_status.update_process_schedule_status.return_value = (
            _mock_schedule_status(enabled=True)
        )

        result = manage_schedules_action(
            sdk, "dev", "enable",
            config_data={"process_id": "proc-1", "atom_id": "atom-1"},
        )

        assert result["_success"] is True
        expected_id = _schedule_id_from_ids("atom-1", "proc-1")
        call_args = sdk.process_schedule_status.update_process_schedule_status.call_args
        assert call_args.kwargs["id_"] == expected_id

    def test_enable_missing_ids_returns_error(self):
        """enable without resource_id or both ids returns error."""
        sdk = MagicMock()

        result = manage_schedules_action(sdk, "dev", "enable")

        assert result["_success"] is False
        assert "resource_id" in result["error"]


# ── New action: disable ───────────────────────────────────────────────


class TestActionDisable:
    def test_disable_by_resource_id(self):
        """disable with resource_id calls update with enabled=False."""
        sdk = MagicMock()
        sdk.process_schedule_status.update_process_schedule_status.return_value = (
            _mock_schedule_status(enabled=False)
        )

        # Use valid base64: CPSatom-1:process-1
        sched_id = "Q1BTYXRvbS0xOnByb2Nlc3MtMQ"
        result = manage_schedules_action(sdk, "dev", "disable", resource_id=sched_id)

        assert result["_success"] is True
        assert result["message"] == "Schedule disabled"
        assert result["status"]["enabled"] is False
        call_args = sdk.process_schedule_status.update_process_schedule_status.call_args
        assert call_args.kwargs["id_"] == sched_id
        body = call_args.kwargs["request_body"]
        assert body.enabled is False
        assert body.atom_id == "atom-1"
        assert body.process_id == "process-1"

    def test_disable_by_process_and_atom_id(self):
        """disable with process_id + atom_id builds schedule ID."""
        sdk = MagicMock()
        sdk.process_schedule_status.update_process_schedule_status.return_value = (
            _mock_schedule_status(enabled=False)
        )

        result = manage_schedules_action(
            sdk, "dev", "disable",
            config_data={"process_id": "proc-1", "atom_id": "atom-1"},
        )

        assert result["_success"] is True
        expected_id = _schedule_id_from_ids("atom-1", "proc-1")
        call_args = sdk.process_schedule_status.update_process_schedule_status.call_args
        assert call_args.kwargs["id_"] == expected_id

    def test_disable_missing_ids_returns_error(self):
        """disable without resource_id or both ids returns error."""
        sdk = MagicMock()

        result = manage_schedules_action(sdk, "dev", "disable")

        assert result["_success"] is False
        assert "resource_id" in result["error"]


# ── Router edge cases ─────────────────────────────────────────────────


class TestRouter:
    def test_unknown_action_returns_error(self):
        """Unknown action returns error with valid_actions list."""
        sdk = MagicMock()

        result = manage_schedules_action(sdk, "dev", "bogus_action")

        assert result["_success"] is False
        assert "Unknown action" in result["error"]
        assert "list_status" in result["valid_actions"]
        assert "enable" in result["valid_actions"]
        assert "disable" in result["valid_actions"]

    def test_api_error_is_caught(self):
        """ApiError in handler returns clean error message."""
        from boomi.net.transport.api_error import ApiError

        sdk = MagicMock()
        err = ApiError.__new__(ApiError)
        Exception.__init__(err, "raw")
        err.error_detail = "Schedule not found"
        err.response = MagicMock()
        err.response.body = {}
        err.message = None
        sdk.process_schedule_status.get_process_schedule_status.side_effect = err

        result = manage_schedules_action(sdk, "dev", "get_status", resource_id="bad-id")

        assert result["_success"] is False
        assert "Schedule not found" in result["error"]
        assert result["exception_type"] == "ApiError"
