"""Unit tests for manage_listeners category module (mocked SDK)."""

import sys
import os
from unittest.mock import MagicMock, patch

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from boomi.net.transport.api_error import ApiError
from boomi.models import Action

from src.boomi_mcp.categories.listeners import (
    _action_status,
    _action_pause,
    _action_resume,
    _action_restart,
    _listener_status_to_dict,
    _extract_api_error_msg,
    manage_listeners_action,
)


# ── Fixtures ─────────────────────────────────────────────────────────


def _make_sdk():
    sdk = MagicMock()
    return sdk


def _make_token_result(token="test-token-123"):
    """Create a mock async token result."""
    token_result = MagicMock()
    token_result.async_token.token = token
    return token_result


def _make_listener(listener_id="proc-1", status="listening", connector_type="http"):
    """Create a mock ListenerStatus object."""
    ls = MagicMock()
    ls.listener_id = listener_id
    ls.status = status
    ls.connector_type = connector_type
    return ls


def _make_async_response(listeners):
    """Create a mock ListenerStatusAsyncResponse."""
    resp = MagicMock()
    resp.result = listeners
    resp.number_of_results = len(listeners)
    resp.response_status_code = 200
    return resp


# ── TestListenerStatusToDict ─────────────────────────────────────────


class TestListenerStatusToDict:
    """Tests for _listener_status_to_dict helper."""

    def test_basic_conversion(self):
        ls = _make_listener("proc-1", "listening", "http")
        result = _listener_status_to_dict(ls)
        assert result["listener_id"] == "proc-1"
        assert result["status"] == "listening"
        assert result["connector_type"] == "http"

    def test_missing_connector_type(self):
        ls = MagicMock(spec=[])
        ls.listener_id = "proc-2"
        ls.status = "paused"
        result = _listener_status_to_dict(ls)
        assert result["listener_id"] == "proc-2"
        assert result["status"] == "paused"
        assert result["connector_type"] is None


# ── TestExtractApiErrorMsg ───────────────────────────────────────────


class TestExtractApiErrorMsg:
    """Tests for _extract_api_error_msg helper."""

    def test_error_detail(self):
        e = MagicMock()
        e.error_detail = "Detailed error"
        assert _extract_api_error_msg(e) == "Detailed error"

    def test_response_body_message(self):
        e = MagicMock()
        e.error_detail = None
        e.response.body = {"message": "Body error"}
        assert _extract_api_error_msg(e) == "Body error"

    def test_fallback_str(self):
        e = MagicMock()
        e.error_detail = None
        e.response = None
        e.message = ""
        e.__str__ = lambda self: "Fallback error"
        assert _extract_api_error_msg(e) == "Fallback error"


# ── TestActionStatus ─────────────────────────────────────────────────


class TestActionStatus:
    """Tests for _action_status."""

    def test_missing_resource_id(self):
        sdk = _make_sdk()
        result = _action_status(sdk, "dev")
        assert result["_success"] is False
        assert "resource_id" in result["error"]

    def test_status_returns_listeners(self):
        sdk = _make_sdk()
        token_result = _make_token_result()
        listeners = [
            _make_listener("proc-1", "listening", "http"),
            _make_listener("proc-2", "paused", "jms"),
        ]
        async_resp = _make_async_response(listeners)

        sdk.listener_status.async_get_listener_status.return_value = token_result
        sdk.listener_status.async_token_listener_status.return_value = async_resp

        result = _action_status(sdk, "dev", resource_id="atom-123")
        assert result["_success"] is True
        assert result["container_id"] == "atom-123"
        assert result["total_count"] == 2
        assert len(result["listeners"]) == 2
        assert result["listeners"][0]["listener_id"] == "proc-1"
        assert result["listeners"][1]["status"] == "paused"

    def test_status_filter_by_listener_id(self):
        sdk = _make_sdk()
        token_result = _make_token_result()
        listeners = [
            _make_listener("proc-1", "listening", "http"),
            _make_listener("proc-2", "paused", "jms"),
        ]
        async_resp = _make_async_response(listeners)

        sdk.listener_status.async_get_listener_status.return_value = token_result
        sdk.listener_status.async_token_listener_status.return_value = async_resp

        result = _action_status(sdk, "dev", resource_id="atom-123", listener_id="proc-2")
        assert result["_success"] is True
        assert result["total_count"] == 1
        assert result["listeners"][0]["listener_id"] == "proc-2"

    def test_status_empty_result(self):
        sdk = _make_sdk()
        token_result = _make_token_result()
        async_resp = _make_async_response([])

        sdk.listener_status.async_get_listener_status.return_value = token_result
        sdk.listener_status.async_token_listener_status.return_value = async_resp

        result = _action_status(sdk, "dev", resource_id="atom-123")
        assert result["_success"] is True
        assert result["total_count"] == 0
        assert result["listeners"] == []

    def test_status_no_result_attr(self):
        """Response object has result=None."""
        sdk = _make_sdk()
        token_result = _make_token_result()
        async_resp = MagicMock()
        async_resp.result = None
        async_resp.response_status_code = 200

        sdk.listener_status.async_get_listener_status.return_value = token_result
        # poll_async_result checks for response.result — when None the polling
        # returns the object anyway (non-None, no .result list). Our handler
        # checks hasattr(response, 'result') and response.result, so empty list.
        sdk.listener_status.async_token_listener_status.return_value = async_resp

        result = _action_status(sdk, "dev", resource_id="atom-123")
        assert result["_success"] is True
        assert result["listeners"] == []


# ── TestActionPause ──────────────────────────────────────────────────


class TestActionPause:
    """Tests for _action_pause."""

    def test_missing_resource_id(self):
        sdk = _make_sdk()
        result = _action_pause(sdk, "dev")
        assert result["_success"] is False
        assert "resource_id" in result["error"]

    def test_pause_all_listeners(self):
        sdk = _make_sdk()
        sdk.change_listener_status.create_change_listener_status.return_value = None

        result = _action_pause(sdk, "dev", resource_id="atom-123")
        assert result["_success"] is True
        assert result["action"] == "pause"
        assert result["container_id"] == "atom-123"
        assert "submitted" in result["message"].lower()

        call_args = sdk.change_listener_status.create_change_listener_status.call_args
        request = call_args.kwargs.get("request_body") or call_args[1].get("request_body")
        assert request.container_id == "atom-123"
        # pause_all when no listener_id
        assert request.action == Action.PAUSEALL

    def test_pause_single_listener(self):
        sdk = _make_sdk()
        sdk.change_listener_status.create_change_listener_status.return_value = None

        result = _action_pause(sdk, "dev", resource_id="atom-123", listener_id="proc-1")
        assert result["_success"] is True

        call_args = sdk.change_listener_status.create_change_listener_status.call_args
        request = call_args.kwargs.get("request_body") or call_args[1].get("request_body")
        assert request.container_id == "atom-123"
        assert request.listener_id == "proc-1"
        assert request.action == Action.PAUSE


# ── TestActionResume ─────────────────────────────────────────────────


class TestActionResume:
    """Tests for _action_resume."""

    def test_missing_resource_id(self):
        sdk = _make_sdk()
        result = _action_resume(sdk, "dev")
        assert result["_success"] is False
        assert "resource_id" in result["error"]

    def test_resume_all_listeners(self):
        sdk = _make_sdk()
        sdk.change_listener_status.create_change_listener_status.return_value = None

        result = _action_resume(sdk, "dev", resource_id="atom-123")
        assert result["_success"] is True
        assert result["action"] == "resume"
        assert result["container_id"] == "atom-123"

        call_args = sdk.change_listener_status.create_change_listener_status.call_args
        request = call_args.kwargs.get("request_body") or call_args[1].get("request_body")
        assert request.action == Action.RESUMEALL

    def test_resume_single_listener(self):
        sdk = _make_sdk()
        sdk.change_listener_status.create_change_listener_status.return_value = None

        result = _action_resume(sdk, "dev", resource_id="atom-123", listener_id="proc-1")
        assert result["_success"] is True

        call_args = sdk.change_listener_status.create_change_listener_status.call_args
        request = call_args.kwargs.get("request_body") or call_args[1].get("request_body")
        assert request.listener_id == "proc-1"
        assert request.action == Action.RESUME


# ── TestActionRestart ────────────────────────────────────────────────


class TestActionRestart:
    """Tests for _action_restart."""

    def test_missing_resource_id(self):
        sdk = _make_sdk()
        result = _action_restart(sdk, "dev")
        assert result["_success"] is False
        assert "resource_id" in result["error"]

    def test_restart_all_listeners(self):
        sdk = _make_sdk()
        sdk.change_listener_status.create_change_listener_status.return_value = None

        result = _action_restart(sdk, "dev", resource_id="atom-123")
        assert result["_success"] is True
        assert result["action"] == "restart"
        assert result["container_id"] == "atom-123"

        call_args = sdk.change_listener_status.create_change_listener_status.call_args
        request = call_args.kwargs.get("request_body") or call_args[1].get("request_body")
        assert request.action == Action.RESTARTALL

    def test_restart_single_listener(self):
        sdk = _make_sdk()
        sdk.change_listener_status.create_change_listener_status.return_value = None

        result = _action_restart(sdk, "dev", resource_id="atom-123", listener_id="proc-1")
        assert result["_success"] is True

        call_args = sdk.change_listener_status.create_change_listener_status.call_args
        request = call_args.kwargs.get("request_body") or call_args[1].get("request_body")
        assert request.listener_id == "proc-1"
        assert request.action == Action.RESTART


# ── TestManageListenersAction (Router) ───────────────────────────────


class TestManageListenersAction:
    """Tests for the manage_listeners_action router."""

    def test_unknown_action(self):
        sdk = _make_sdk()
        result = manage_listeners_action(sdk, "dev", "explode")
        assert result["_success"] is False
        assert "Unknown action" in result["error"]
        assert "status" in result["valid_actions"]
        assert "pause" in result["valid_actions"]
        assert "resume" in result["valid_actions"]
        assert "restart" in result["valid_actions"]

    def test_routes_to_pause(self):
        sdk = _make_sdk()
        sdk.change_listener_status.create_change_listener_status.return_value = None

        result = manage_listeners_action(
            sdk, "dev", "pause", config_data={"resource_id": "atom-1"}
        )
        assert result["_success"] is True
        assert result["action"] == "pause"

    def test_routes_to_resume_with_kwargs(self):
        sdk = _make_sdk()
        sdk.change_listener_status.create_change_listener_status.return_value = None

        result = manage_listeners_action(
            sdk, "dev", "resume", resource_id="atom-1"
        )
        assert result["_success"] is True
        assert result["action"] == "resume"

    def test_routes_to_restart_with_config(self):
        sdk = _make_sdk()
        sdk.change_listener_status.create_change_listener_status.return_value = None

        result = manage_listeners_action(
            sdk, "dev", "restart",
            config_data={"resource_id": "atom-1", "listener_id": "proc-1"},
        )
        assert result["_success"] is True
        assert result["action"] == "restart"

    def test_api_error_caught(self):
        sdk = _make_sdk()
        err = ApiError("Boom", 500, "server error")
        sdk.change_listener_status.create_change_listener_status.side_effect = err

        result = manage_listeners_action(
            sdk, "dev", "pause", config_data={"resource_id": "atom-1"}
        )
        assert result["_success"] is False
        assert "pause" in result["error"]
        assert result["exception_type"] == "ApiError"

    def test_generic_exception_caught(self):
        sdk = _make_sdk()
        sdk.change_listener_status.create_change_listener_status.side_effect = RuntimeError("oops")

        result = manage_listeners_action(
            sdk, "dev", "pause", config_data={"resource_id": "atom-1"}
        )
        assert result["_success"] is False
        assert "oops" in result["error"]
        assert result["exception_type"] == "RuntimeError"

    def test_status_via_router(self):
        sdk = _make_sdk()
        token_result = _make_token_result()
        listeners = [_make_listener("proc-1", "listening", "http")]
        async_resp = _make_async_response(listeners)

        sdk.listener_status.async_get_listener_status.return_value = token_result
        sdk.listener_status.async_token_listener_status.return_value = async_resp

        result = manage_listeners_action(
            sdk, "dev", "status", config_data={"resource_id": "atom-1"}
        )
        assert result["_success"] is True
        assert result["total_count"] == 1

    def test_config_data_defaults_to_empty(self):
        """config_data=None should not raise."""
        sdk = _make_sdk()
        result = manage_listeners_action(sdk, "dev", "pause")
        assert result["_success"] is False
        assert "resource_id" in result["error"]
