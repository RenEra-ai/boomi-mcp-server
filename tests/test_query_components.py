"""Unit tests for query_components batch-05 bugfixes (mocked SDK)."""

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

FAKE_CREDS = {
    "account_id": "acct-test-123",
    "username": "BOOMI_TOKEN.user@example.com",
    "password": "tok-secret",
    "base_url": None,
}


def _call_tool(tool, **kwargs):
    fn = getattr(tool, "fn", tool)
    return fn(**kwargs)


def _fake_component_xml(component_id, name="Test Component"):
    """Return a dict mimicking component_get_xml output."""
    return {
        "component_id": component_id,
        "id": component_id,
        "name": name,
        "folder_name": "TestFolder",
        "folder_id": "folder-1",
        "folder_full_path": "TestFolder",
        "type": "process",
        "version": 1,
        "description": "",
        "xml": "<Component/>",
    }


def _fake_metadata(component_id, name="Test", deleted=False, current_version=True):
    """Return a SimpleNamespace mimicking a ComponentMetadata SDK object."""
    return SimpleNamespace(
        component_id=component_id,
        id_=component_id,
        name=name,
        folder_name="TestFolder",
        type_="process",
        version="1",
        current_version=current_version,
        deleted=deleted,
        created_date="2025-01-01",
        modified_date="2025-01-02",
        created_by="user@test.com",
        modified_by="user@test.com",
    )


# ── QA-014: bulk_get config routing ──────────────────────────────────


class TestBulkGetConfigRouting:
    """bulk_get should accept component_ids from config as well as direct param."""

    @patch("server.get_secret", return_value=FAKE_CREDS)
    @patch("server.Boomi")
    @patch("server.get_current_user", return_value="test-user")
    @patch(
        "boomi_mcp.categories.components.query_components.component_get_xml"
    )
    def test_config_component_ids(self, mock_get_xml, _user, mock_boomi_cls, _creds):
        """component_ids provided only via config reaches bulk_get handler."""
        ids = ["id-aaa", "id-bbb"]
        mock_get_xml.side_effect = lambda _sdk, cid: _fake_component_xml(cid)
        mock_boomi_cls.return_value = MagicMock()

        result = _call_tool(
            server.query_components,
            profile="dev",
            action="bulk_get",
            config=json.dumps({"component_ids": ids}),
        )

        assert result["_success"] is True
        assert result["total_count"] == 2
        returned_ids = [c["component_id"] for c in result["components"]]
        assert returned_ids == ids

    @patch("server.get_secret", return_value=FAKE_CREDS)
    @patch("server.Boomi")
    @patch("server.get_current_user", return_value="test-user")
    @patch(
        "boomi_mcp.categories.components.query_components.component_get_xml"
    )
    def test_direct_ids_take_precedence(self, mock_get_xml, _user, mock_boomi_cls, _creds):
        """Direct component_ids param wins over config-sourced IDs."""
        direct_ids = ["direct-1"]
        config_ids = ["config-1", "config-2"]
        mock_get_xml.side_effect = lambda _sdk, cid: _fake_component_xml(cid)
        mock_boomi_cls.return_value = MagicMock()

        result = _call_tool(
            server.query_components,
            profile="dev",
            action="bulk_get",
            component_ids=json.dumps(direct_ids),
            config=json.dumps({"component_ids": config_ids}),
        )

        assert result["_success"] is True
        returned_ids = [c["component_id"] for c in result["components"]]
        assert returned_ids == direct_ids

    @patch("server.get_secret", return_value=FAKE_CREDS)
    @patch("server.Boomi")
    @patch("server.get_current_user", return_value="test-user")
    def test_config_ids_validation_empty(self, _user, mock_boomi_cls, _creds):
        """Empty config component_ids list triggers validation error."""
        mock_boomi_cls.return_value = MagicMock()

        result = _call_tool(
            server.query_components,
            profile="dev",
            action="bulk_get",
            config=json.dumps({"component_ids": []}),
        )

        assert result["_success"] is False
        assert "empty" in result["error"].lower()

    @patch("server.get_secret", return_value=FAKE_CREDS)
    @patch("server.Boomi")
    @patch("server.get_current_user", return_value="test-user")
    def test_config_ids_validation_too_many(self, _user, mock_boomi_cls, _creds):
        """Config with >5 component_ids triggers max-limit error."""
        mock_boomi_cls.return_value = MagicMock()
        ids = [f"id-{i}" for i in range(6)]

        result = _call_tool(
            server.query_components,
            profile="dev",
            action="bulk_get",
            config=json.dumps({"component_ids": ids}),
        )

        assert result["_success"] is False
        assert "5" in result["error"]

    def test_no_ids_at_all(self):
        """bulk_get with neither direct nor config IDs returns required error."""
        from boomi_mcp.categories.components.query_components import (
            query_components_action,
        )

        sdk = MagicMock()
        result = query_components_action(sdk, "dev", "bulk_get")
        assert result["_success"] is False
        assert "required" in result["error"].lower()


# ── QA-017: deleted boolean normalization ─────────────────────────────


class TestDeletedBoolean:
    """list/search should return deleted as boolean, not string."""

    @patch("server.get_secret", return_value=FAKE_CREDS)
    @patch("server.Boomi")
    @patch("server.get_current_user", return_value="test-user")
    def test_list_deleted_is_boolean(self, _user, mock_boomi_cls, _creds):
        """query_components list returns deleted as a boolean."""
        mock_sdk = MagicMock()
        mock_boomi_cls.return_value = mock_sdk

        # Build a fake query result with one component
        fake_result = MagicMock()
        fake_result.result = [_fake_metadata("comp-1", deleted=False)]
        fake_result.query_token = None
        mock_sdk.component_metadata.query_component_metadata.return_value = fake_result

        result = _call_tool(
            server.query_components,
            profile="dev",
            action="list",
        )

        assert result["_success"] is True
        assert result["total_count"] >= 1
        comp = result["components"][0]
        assert comp["deleted"] is False
        assert isinstance(comp["deleted"], bool)

    @patch("server.get_secret", return_value=FAKE_CREDS)
    @patch("server.Boomi")
    @patch("server.get_current_user", return_value="test-user")
    def test_search_deleted_is_boolean(self, _user, mock_boomi_cls, _creds):
        """query_components search returns deleted as a boolean."""
        mock_sdk = MagicMock()
        mock_boomi_cls.return_value = mock_sdk

        fake_result = MagicMock()
        fake_result.result = [_fake_metadata("comp-2", deleted=False)]
        fake_result.query_token = None
        mock_sdk.component_metadata.query_component_metadata.return_value = fake_result

        result = _call_tool(
            server.query_components,
            profile="dev",
            action="search",
            config=json.dumps({"name": "%Test%"}),
        )

        assert result["_success"] is True
        assert result["total_count"] >= 1
        comp = result["components"][0]
        assert comp["deleted"] is False
        assert isinstance(comp["deleted"], bool)

    def test_metadata_to_dict_deleted_true(self):
        """metadata_to_dict converts deleted=True SDK value to boolean True."""
        from boomi_mcp.categories.components._shared import metadata_to_dict

        comp = _fake_metadata("comp-3", deleted=True)
        d = metadata_to_dict(comp)
        assert d["deleted"] is True
        assert isinstance(d["deleted"], bool)

    def test_metadata_to_dict_deleted_false(self):
        """metadata_to_dict converts deleted=False SDK value to boolean False."""
        from boomi_mcp.categories.components._shared import metadata_to_dict

        comp = _fake_metadata("comp-4", deleted=False)
        d = metadata_to_dict(comp)
        assert d["deleted"] is False
        assert isinstance(d["deleted"], bool)

    def test_metadata_to_dict_deleted_string_true(self):
        """metadata_to_dict handles string 'true' from SDK."""
        from boomi_mcp.categories.components._shared import metadata_to_dict

        comp = _fake_metadata("comp-5", deleted="true")
        d = metadata_to_dict(comp)
        assert d["deleted"] is True

    def test_metadata_to_dict_deleted_string_false(self):
        """metadata_to_dict handles string 'false' from SDK."""
        from boomi_mcp.categories.components._shared import metadata_to_dict

        comp = _fake_metadata("comp-6", deleted="false")
        d = metadata_to_dict(comp)
        assert d["deleted"] is False
