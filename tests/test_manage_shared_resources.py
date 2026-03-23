"""Unit tests for manage_shared_resources category module (mocked SDK)."""

import sys
import os
from unittest.mock import MagicMock, patch

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from boomi.net.transport.api_error import ApiError

from boomi_mcp.categories.shared_resources import (
    manage_shared_resources_action,
    _action_update_channel,
    _action_delete_channel,
    _action_get_server_info,
    _action_update_server_info,
    _action_list_web_servers,
    _channel_to_dict,
    _server_info_to_dict,
    _extract_api_error_msg,
)


# ── Fixtures ─────────────────────────────────────────────────────────


def _make_sdk():
    return MagicMock()


def _make_channel(component_id="ch-001", component_name="Test Channel",
                  communication_type=None, folder_name="Home"):
    ch = MagicMock()
    ch.component_id = component_id
    ch.component_name = component_name
    ch.communication_type = communication_type
    ch.folder_name = folder_name
    ch.folder_id = None
    ch.folder_full_path = None
    ch.deleted = None
    ch.description = None
    ch.branch_id = None
    ch.branch_name = None
    return ch


def _make_server_info(atom_id="atom-001", api_type=None, auth=None,
                      http_port=9090, url="http://localhost:9090"):
    info = MagicMock()
    info.atom_id = atom_id
    info.api_type = api_type
    info.auth = auth
    info.auth_token = None
    info.check_forwarded_headers = None
    info.external_host = None
    info.external_http_port = None
    info.external_https_port = None
    info.http_port = http_port
    info.https_port = None
    info.internal_host = None
    info.max_threads = None
    info.min_auth = None
    info.override_url = None
    info.ssl_certificate_id = None
    info.url = url
    return info


# ── TestChannelToDict ────────────────────────────────────────────────


class TestChannelToDict:
    def test_basic_conversion(self):
        ch = _make_channel("ch-1", "My Channel")
        result = _channel_to_dict(ch)
        assert result["id"] == "ch-1"
        assert result["name"] == "My Channel"


# ── TestServerInfoToDict ─────────────────────────────────────────────


class TestServerInfoToDict:
    def test_basic_conversion(self):
        info = _make_server_info()
        result = _server_info_to_dict(info)
        assert result["atom_id"] == "atom-001"
        assert result["http_port"] == 9090
        assert result["url"] == "http://localhost:9090"

    def test_enum_value_extraction(self):
        info = _make_server_info()
        info.api_type = MagicMock()
        info.api_type.value = "intermediate"
        result = _server_info_to_dict(info)
        assert result["api_type"] == "intermediate"


# ── TestUpdateChannel ────────────────────────────────────────────────


class TestUpdateChannel:
    def test_missing_resource_id(self):
        sdk = _make_sdk()
        result = _action_update_channel(sdk, "dev")
        assert result["_success"] is False
        assert "resource_id" in result["error"]

    def test_update_success(self):
        sdk = _make_sdk()
        updated_ch = _make_channel("ch-001", "Updated Channel")
        sdk.shared_communication_channel_component.update_shared_communication_channel_component.return_value = updated_ch

        result = _action_update_channel(sdk, "dev", resource_id="ch-001", name="Updated Channel")
        assert result["_success"] is True
        assert result["channel"]["name"] == "Updated Channel"
        sdk.shared_communication_channel_component.update_shared_communication_channel_component.assert_called_once()


# ── TestDeleteChannel ────────────────────────────────────────────────


class TestDeleteChannel:
    def test_missing_resource_id(self):
        sdk = _make_sdk()
        result = _action_delete_channel(sdk, "dev")
        assert result["_success"] is False
        assert "resource_id" in result["error"]

    def test_delete_success(self):
        sdk = _make_sdk()
        result = _action_delete_channel(sdk, "dev", resource_id="ch-001")
        assert result["_success"] is True
        assert "deleted" in result["message"].lower()
        sdk.shared_communication_channel_component.delete_shared_communication_channel_component.assert_called_once_with(
            id_="ch-001"
        )


# ── TestGetServerInfo ────────────────────────────────────────────────


class TestGetServerInfo:
    def test_missing_resource_id(self):
        sdk = _make_sdk()
        result = _action_get_server_info(sdk, "dev")
        assert result["_success"] is False
        assert "resource_id" in result["error"]

    def test_get_success(self):
        sdk = _make_sdk()
        info = _make_server_info()
        sdk.shared_server_information.get_shared_server_information.return_value = info

        result = _action_get_server_info(sdk, "dev", resource_id="atom-001")
        assert result["_success"] is True
        assert result["server_info"]["atom_id"] == "atom-001"
        assert result["server_info"]["http_port"] == 9090


# ── TestUpdateServerInfo ─────────────────────────────────────────────


class TestUpdateServerInfo:
    def test_missing_resource_id(self):
        sdk = _make_sdk()
        result = _action_update_server_info(sdk, "dev")
        assert result["_success"] is False
        assert "resource_id" in result["error"]

    def test_no_update_fields(self):
        sdk = _make_sdk()
        result = _action_update_server_info(sdk, "dev", resource_id="atom-001")
        assert result["_success"] is False
        assert "No valid update fields" in result["error"]

    def test_update_success(self):
        sdk = _make_sdk()
        updated_info = _make_server_info(http_port=8080)
        sdk.shared_server_information.update_shared_server_information.return_value = updated_info

        result = _action_update_server_info(sdk, "dev", resource_id="atom-001", http_port=8080)
        assert result["_success"] is True
        assert result["server_info"]["http_port"] == 8080
        assert "http_port" in result["updated_fields"]


# ── TestGetWebServer (alias) ─────────────────────────────────────────


class TestGetWebServerAlias:
    def test_get_web_server_routes_to_list_web_servers(self):
        """get_web_server should be an alias for list_web_servers."""
        sdk = _make_sdk()
        # Patch _raw_web_server_request for the alias test
        with patch("boomi_mcp.categories.shared_resources._raw_web_server_request") as mock_raw:
            mock_raw.return_value = {"atomId": "atom-001", "generalSettings": {}}
            result = manage_shared_resources_action(sdk, "dev", "get_web_server",
                                                     resource_id="atom-001")
            assert result["_success"] is True
            assert "web_server" in result


# ── TestRouterUnknownAction ──────────────────────────────────────────


class TestRouterUnknownAction:
    def test_unknown_action(self):
        sdk = _make_sdk()
        result = manage_shared_resources_action(sdk, "dev", "nonexistent_action")
        assert result["_success"] is False
        assert "Unknown action" in result["error"]
        assert "valid_actions" in result


# ── TestRouterApiErrorHandling ───────────────────────────────────────


class TestRouterApiErrorHandling:
    def test_api_error_is_caught(self):
        sdk = _make_sdk()
        err = ApiError.__new__(ApiError)
        Exception.__init__(err, "test error")
        err.error_detail = "Channel not found"
        err.response = None
        err.message = None
        sdk.shared_communication_channel_component.update_shared_communication_channel_component.side_effect = err

        result = manage_shared_resources_action(
            sdk, "dev", "update_channel",
            config_data={"name": "New"},
            resource_id="bad-id",
        )
        assert result["_success"] is False
        assert "Channel not found" in result["error"]
        assert result["exception_type"] == "ApiError"


# ── TestExtractApiErrorMsg ───────────────────────────────────────────


class TestExtractApiErrorMsg:
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
