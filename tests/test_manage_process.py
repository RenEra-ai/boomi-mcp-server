"""Unit tests for manage_process batch-05 bugfixes (mocked SDK)."""

import json
import os
import sys
from unittest.mock import MagicMock, patch

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


def _fake_api_error(detail=None, body_message=None, message=None):
    """Build a fake ApiError-like exception with configurable attributes."""
    err = server.ApiError.__new__(server.ApiError)
    Exception.__init__(err, message or "raw repr fallback")
    err.error_detail = detail
    resp = MagicMock()
    resp.body = {"message": body_message} if body_message else {}
    err.response = resp
    err.message = message
    return err


def _assert_no_leak(error_msg):
    """Assert that an error message does not leak raw SDK repr patterns."""
    assert "ApiError(" not in error_msg
    assert "response=<" not in error_msg
    assert "object at 0x" not in error_msg


# ── QA-015: manage_process get error cleanup ──────────────────────────


class TestProcessGetErrorCleanup:
    """get_process should return clean error messages, not raw ApiError repr."""

    @patch("server.get_secret", return_value=FAKE_CREDS)
    @patch("server.Boomi")
    @patch("server.get_current_user", return_value="test-user")
    @patch(
        "boomi_mcp.categories.components.processes._component_get_xml"
    )
    def test_api_error_detail_surfaces(self, mock_get_xml, _user, mock_boomi_cls, _creds):
        """ApiError with error_detail returns the detail text, not repr."""
        mock_boomi_cls.return_value = MagicMock()
        mock_get_xml.side_effect = _fake_api_error(
            detail="ComponentId fake-process-id is invalid"
        )

        result = _call_tool(
            server.manage_process,
            profile="dev",
            action="get",
            process_id="fake-process-id",
        )

        assert result["_success"] is False
        assert "fake-process-id" in result["error"]
        assert "invalid" in result["error"].lower()
        _assert_no_leak(result["error"])

    @patch("server.get_secret", return_value=FAKE_CREDS)
    @patch("server.Boomi")
    @patch("server.get_current_user", return_value="test-user")
    @patch(
        "boomi_mcp.categories.components.processes._component_get_xml"
    )
    def test_api_error_body_message_surfaces(self, mock_get_xml, _user, mock_boomi_cls, _creds):
        """ApiError with response.body.message returns the message, not repr."""
        mock_boomi_cls.return_value = MagicMock()
        mock_get_xml.side_effect = _fake_api_error(
            body_message="Invalid compound id"
        )

        result = _call_tool(
            server.manage_process,
            profile="dev",
            action="get",
            process_id="bad-id",
        )

        assert result["_success"] is False
        assert "Invalid compound id" in result["error"]
        _assert_no_leak(result["error"])

    @patch("server.get_secret", return_value=FAKE_CREDS)
    @patch("server.Boomi")
    @patch("server.get_current_user", return_value="test-user")
    @patch(
        "boomi_mcp.categories.components.processes._component_get_xml"
    )
    def test_generic_exception_cleaned(self, mock_get_xml, _user, mock_boomi_cls, _creds):
        """Generic exception uses _extract_api_error_msg fallback (str)."""
        mock_boomi_cls.return_value = MagicMock()
        mock_get_xml.side_effect = Exception("GET failed: HTTP 404 — Not Found")

        result = _call_tool(
            server.manage_process,
            profile="dev",
            action="get",
            process_id="missing-id",
        )

        assert result["_success"] is False
        assert "missing-id" in result["error"]
        _assert_no_leak(result["error"])


class TestComponentGetXmlStatusCodePath:
    """component_get_xml status-code error path must not leak raw response."""

    @patch("boomi_mcp.categories.components._shared.Serializer")
    def test_http_error_json_body(self, mock_serializer_cls):
        """HTTP error with JSON body extracts the message field."""
        from boomi_mcp.categories.components._shared import component_get_xml

        # Wire up the Serializer chain mock
        mock_chain = MagicMock()
        mock_serializer_cls.return_value = mock_chain
        mock_chain.add_header.return_value = mock_chain
        mock_chain.serialize.return_value = mock_chain
        mock_chain.set_method.return_value = mock_chain

        mock_sdk = MagicMock()
        svc = mock_sdk.component

        import json as _json
        body = _json.dumps({"message": "ComponentId is invalid"})
        svc.send_request.return_value = (body, 400, None)

        with pytest.raises(Exception) as exc_info:
            component_get_xml(mock_sdk, "bad-id")

        msg = str(exc_info.value)
        assert "ComponentId is invalid" in msg
        assert "response=<" not in msg
        assert "object at 0x" not in msg

    @patch("boomi_mcp.categories.components._shared.Serializer")
    def test_http_error_plain_text_body(self, mock_serializer_cls):
        """HTTP error with plain text body uses first line, no raw dump."""
        from boomi_mcp.categories.components._shared import component_get_xml

        mock_chain = MagicMock()
        mock_serializer_cls.return_value = mock_chain
        mock_chain.add_header.return_value = mock_chain
        mock_chain.serialize.return_value = mock_chain
        mock_chain.set_method.return_value = mock_chain

        mock_sdk = MagicMock()
        svc = mock_sdk.component
        svc.send_request.return_value = ("Not Found", 404, None)

        with pytest.raises(Exception) as exc_info:
            component_get_xml(mock_sdk, "bad-id")

        msg = str(exc_info.value)
        assert "404" in msg
        assert "Not Found" in msg

    @patch("boomi_mcp.categories.components._shared.Serializer")
    def test_http_error_parsed_dict_body(self, mock_serializer_cls):
        """HTTP error where send_request returns a parsed dict extracts message."""
        from boomi_mcp.categories.components._shared import component_get_xml

        mock_chain = MagicMock()
        mock_serializer_cls.return_value = mock_chain
        mock_chain.add_header.return_value = mock_chain
        mock_chain.serialize.return_value = mock_chain
        mock_chain.set_method.return_value = mock_chain

        mock_sdk = MagicMock()
        svc = mock_sdk.component
        # send_request returns response.body as a parsed dict for JSON
        svc.send_request.return_value = (
            {"message": "ComponentId is invalid"},
            400,
            "application/json",
        )

        with pytest.raises(Exception) as exc_info:
            component_get_xml(mock_sdk, "bad-id")

        msg = str(exc_info.value)
        assert "ComponentId is invalid" in msg
        assert "400" in msg


# ── QA-016: single-process create top-level IDs ──────────────────────


class TestProcessCreateTopLevelIds:
    """create_process should add top-level process_id for single-process creates."""

    @patch("server.get_secret", return_value=FAKE_CREDS)
    @patch("server.Boomi")
    @patch("server.get_current_user", return_value="test-user")
    @patch(
        "boomi_mcp.categories.components.processes.ComponentOrchestrator"
    )
    @patch(
        "boomi_mcp.categories.components.processes.parse_json_to_specs"
    )
    def test_single_process_has_top_level_ids(
        self, mock_parse, mock_orch_cls, _user, mock_boomi_cls, _creds
    ):
        """Single-process create exposes process_id and component_id at top level."""
        mock_boomi_cls.return_value = MagicMock()
        mock_parse.return_value = [MagicMock()]

        created_id = "abc-123-def"
        mock_orch = MagicMock()
        mock_orch.build_with_dependencies.return_value = {
            "MyProcess": {
                "component_id": created_id,
                "id": created_id,
                "type": "process",
            }
        }
        mock_orch.warnings = []
        mock_orch_cls.return_value = mock_orch

        result = _call_tool(
            server.manage_process,
            profile="dev",
            action="create",
            config=json.dumps({"name": "MyProcess", "shapes": []}),
        )

        assert result["_success"] is True
        assert result["process_id"] == created_id
        assert result["component_id"] == created_id
        assert "components" in result
        assert "MyProcess" in result["components"]

    @patch("server.get_secret", return_value=FAKE_CREDS)
    @patch("server.Boomi")
    @patch("server.get_current_user", return_value="test-user")
    @patch(
        "boomi_mcp.categories.components.processes.ComponentOrchestrator"
    )
    @patch(
        "boomi_mcp.categories.components.processes.parse_json_to_specs"
    )
    def test_multi_component_no_top_level_ids(
        self, mock_parse, mock_orch_cls, _user, mock_boomi_cls, _creds
    ):
        """Multi-component create does NOT add top-level process_id."""
        mock_boomi_cls.return_value = MagicMock()
        mock_parse.return_value = [MagicMock(), MagicMock()]

        mock_orch = MagicMock()
        mock_orch.build_with_dependencies.return_value = {
            "MyProcess": {
                "component_id": "proc-1",
                "id": "proc-1",
                "type": "process",
            },
            "MyMap": {
                "component_id": "map-1",
                "id": "map-1",
                "type": "map",
            },
        }
        mock_orch.warnings = []
        mock_orch_cls.return_value = mock_orch

        result = _call_tool(
            server.manage_process,
            profile="dev",
            action="create",
            config=json.dumps({"name": "MyProcess", "shapes": []}),
        )

        assert result["_success"] is True
        assert "process_id" not in result
        assert "component_id" not in result
        assert "components" in result
        assert len(result["components"]) == 2
