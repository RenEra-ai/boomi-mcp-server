"""Unit tests for trading partner and organization batch-02 bugfixes (mocked SDK)."""

import json
import os
import sys
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

os.environ["BOOMI_LOCAL"] = "true"

import server


# ── Helpers ──────────────────────────────────────────────────────────


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


FAKE_CREDS = {
    "account_id": "acct-test-123",
    "username": "BOOMI_TOKEN.user@example.com",
    "password": "tok-secret",
    "base_url": None,
}


def _call_tool(tool, **kwargs):
    """Call a FastMCP FunctionTool via .fn() or directly if plain function."""
    fn = getattr(tool, "fn", tool)
    return fn(**kwargs)


def _assert_no_leak(error_msg):
    """Assert that an error message does not leak raw SDK repr patterns."""
    assert "ApiError(" not in error_msg
    assert "response=<" not in error_msg
    assert "object at 0x" not in error_msg


# ── QA-004: ApiError sanitization in trading partner operations ──────


class TestTradingPartnerGetApiError:
    @patch("server.get_secret", return_value=FAKE_CREDS)
    @patch("server.Boomi")
    @patch("server.get_current_user", return_value="test-user")
    def test_api_error_sanitized(self, _user, mock_boomi_cls, _creds):
        mock_sdk = MagicMock()
        mock_sdk.trading_partner_component.get_trading_partner_component.side_effect = (
            _fake_api_error(detail="The component could not be found")
        )
        mock_boomi_cls.return_value = mock_sdk

        result = _call_tool(
            server.manage_trading_partner,
            profile="dev",
            action="get",
            resource_id="bad-id-123",
        )

        assert result["_success"] is False
        assert "The component could not be found" in result["error"]
        _assert_no_leak(result["error"])


class TestTradingPartnerCreateApiError:
    @patch("server.get_secret", return_value=FAKE_CREDS)
    @patch("server.Boomi")
    @patch("server.get_current_user", return_value="test-user")
    def test_api_error_sanitized(self, _user, mock_boomi_cls, _creds):
        mock_sdk = MagicMock()
        mock_sdk.trading_partner_component.create_trading_partner_component.side_effect = (
            _fake_api_error(body_message="B2B feature not enabled for this account")
        )
        mock_boomi_cls.return_value = mock_sdk

        result = _call_tool(
            server.manage_trading_partner,
            profile="dev",
            action="create",
            config=json.dumps({
                "component_name": "Test Partner",
                "standard": "x12",
            }),
        )

        assert result["_success"] is False
        assert "B2B feature not enabled" in result["error"]
        _assert_no_leak(result["error"])


class TestTradingPartnerDeleteApiError:
    @patch("server.get_secret", return_value=FAKE_CREDS)
    @patch("server.Boomi")
    @patch("server.get_current_user", return_value="test-user")
    def test_api_error_sanitized(self, _user, mock_boomi_cls, _creds):
        mock_sdk = MagicMock()
        mock_sdk.trading_partner_component.delete_trading_partner_component.side_effect = (
            _fake_api_error(detail="Resource not found")
        )
        mock_boomi_cls.return_value = mock_sdk

        result = _call_tool(
            server.manage_trading_partner,
            profile="dev",
            action="delete",
            resource_id="bad-id-123",
        )

        assert result["_success"] is False
        assert "Resource not found" in result["error"]
        _assert_no_leak(result["error"])


# ── QA-004: ApiError sanitization in organization operations ─────────


class TestOrganizationGetApiError:
    @patch("server.get_secret", return_value=FAKE_CREDS)
    @patch("server.Boomi")
    @patch("server.get_current_user", return_value="test-user")
    def test_api_error_sanitized(self, _user, mock_boomi_cls, _creds):
        mock_sdk = MagicMock()
        mock_sdk.organization_component.get_organization_component.side_effect = (
            _fake_api_error(detail="Organization not found")
        )
        mock_boomi_cls.return_value = mock_sdk

        result = _call_tool(
            server.manage_trading_partner,
            profile="dev",
            action="org_get",
            resource_id="bad-org-id",
        )

        assert result["_success"] is False
        assert "Organization not found" in result["error"]
        _assert_no_leak(result["error"])


# ── QA-005: Organization list with sparse contact info ───────────────


class TestOrgListSparseContactInfo:
    @patch("server.get_secret", return_value=FAKE_CREDS)
    @patch("server.Boomi")
    @patch("server.get_current_user", return_value="test-user")
    def test_sparse_contact_info_succeeds(self, _user, mock_boomi_cls, _creds):
        """Org list should succeed even when OrganizationContactInfo is sparse/missing."""
        mock_sdk = MagicMock()

        # Mock the raw Serializer chain used by the rewritten list_organizations
        svc = mock_sdk.organization_component
        svc.base_url = "https://api.boomi.com/api/rest/v1/acct-test-123"
        svc.get_access_token.return_value = MagicMock()
        svc.get_basic_auth.return_value = MagicMock()

        # Return JSON with sparse org data (no contactInfo at all)
        raw_response = json.dumps({
            "result": [
                {
                    "componentId": "org-001",
                    "componentName": "Sparse Org 1",
                    "folderName": "Home",
                    "folderId": "folder-1",
                },
                {
                    "componentId": "org-002",
                    "componentName": "Sparse Org 2",
                    # Missing folderName and folderId entirely
                },
            ],
            "numberOfResults": 2,
        })
        svc.send_request.return_value = (raw_response, 200, None)

        mock_boomi_cls.return_value = mock_sdk

        result = _call_tool(
            server.manage_trading_partner,
            profile="dev",
            action="org_list",
        )

        assert result["_success"] is True
        assert result["total_count"] == 2
        assert len(result["organizations"]) == 2
        assert result["organizations"][0]["component_id"] == "org-001"
        assert result["organizations"][0]["name"] == "Sparse Org 1"
        assert result["organizations"][1]["component_id"] == "org-002"
        assert result["organizations"][1]["folder_name"] is None


# ── QA-006: X12 create defaults isa_qualifier to ZZ ──────────────────


class TestX12IsaQualifierDefault:
    def test_isa_id_only_defaults_qualifier_to_zz(self):
        """When isa_id is provided without isa_qualifier, qualifier should default to ZZ."""
        from boomi_mcp.models.trading_partner_builders import build_x12_partner_info

        result = build_x12_partner_info(isa_id="MYPARTNER")

        assert result is not None
        isa_ctrl = result.x12_control_info.isa_control_info
        assert isa_ctrl.interchange_id == "MYPARTNER"
        assert isa_ctrl.interchange_id_qualifier == "X12IDQUAL_ZZ"

    def test_explicit_qualifier_not_overridden(self):
        """Explicit isa_qualifier should not be overridden by the default."""
        from boomi_mcp.models.trading_partner_builders import build_x12_partner_info

        result = build_x12_partner_info(isa_id="MYPARTNER", isa_qualifier="01")

        assert result is not None
        isa_ctrl = result.x12_control_info.isa_control_info
        assert isa_ctrl.interchange_id_qualifier == "X12IDQUAL_01"

    def test_no_isa_fields_returns_none(self):
        """With no ISA or GS fields, build_x12_partner_info should return None."""
        from boomi_mcp.models.trading_partner_builders import build_x12_partner_info

        result = build_x12_partner_info()
        assert result is None


# ── QA-008: Create validation wording ────────────────────────────────


class TestCreateValidationWording:
    @patch("server.get_secret", return_value=FAKE_CREDS)
    @patch("server.Boomi")
    @patch("server.get_current_user", return_value="test-user")
    def test_empty_config_references_config(self, _user, mock_boomi_cls, _creds):
        """Empty config should produce error referencing 'config', not 'request_data'."""
        mock_boomi_cls.return_value = MagicMock()

        result = _call_tool(
            server.manage_trading_partner,
            profile="dev",
            action="create",
            config="{}",
        )

        assert result["_success"] is False
        assert "config" in result["error"].lower()
        assert "request_data" not in result.get("error", "")
        assert "component_name" in result.get("hint", "")
        assert "standard" in result.get("hint", "")

    @patch("server.get_secret", return_value=FAKE_CREDS)
    @patch("server.Boomi")
    @patch("server.get_current_user", return_value="test-user")
    def test_no_config_references_config(self, _user, mock_boomi_cls, _creds):
        """Missing config should produce error referencing 'config'."""
        mock_boomi_cls.return_value = MagicMock()

        result = _call_tool(
            server.manage_trading_partner,
            profile="dev",
            action="create",
        )

        assert result["_success"] is False
        assert "config" in result["error"].lower()


# ── QA-007 (excluded): List duplicates preserved ─────────────────────


class TestListDuplicatesPreserved:
    @patch("server.get_secret", return_value=FAKE_CREDS)
    @patch("server.Boomi")
    @patch("server.get_current_user", return_value="test-user")
    def test_duplicate_ids_preserved(self, _user, mock_boomi_cls, _creds):
        """Duplicate component_ids from the API should be preserved, not deduplicated."""
        mock_sdk = MagicMock()

        # Build query result with duplicate IDs
        dup_partner = SimpleNamespace(
            id_="tp-dup-001",
            name="Duplicate Partner",
            standard=SimpleNamespace(value="x12"),
            classification=SimpleNamespace(value="tradingpartner"),
            folder_name="Home",
            deleted=False,
        )
        query_result = SimpleNamespace(
            result=[dup_partner, dup_partner, dup_partner],
            query_token=None,
        )
        mock_sdk.trading_partner_component.query_trading_partner_component.return_value = query_result
        mock_boomi_cls.return_value = mock_sdk

        result = _call_tool(
            server.manage_trading_partner,
            profile="dev",
            action="list",
            config=json.dumps({"standard": "x12"}),
        )

        assert result["_success"] is True
        assert result["total_count"] == 3
        assert len(result["partners"]) == 3
        ids = [p["component_id"] for p in result["partners"]]
        assert ids == ["tp-dup-001", "tp-dup-001", "tp-dup-001"]
