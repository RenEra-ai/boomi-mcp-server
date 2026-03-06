"""Unit tests for manage_account role CRUD (mocked SDK — no live Boomi dependency)."""

import sys
import os
from unittest.mock import MagicMock

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from src.boomi_mcp.categories.account import _action_manage_role


def _make_sdk():
    sdk = MagicMock()
    sdk._base_url_account_id = "acct-123"
    return sdk


def _make_current_role():
    role = MagicMock()
    role.name = "OldName"
    role.description = "OldDesc"
    role.privileges = MagicMock()
    role.privileges.privilege = []
    return role


# ── Update guard: clearing description to empty string ───────────────

class TestUpdateClearDescriptionEmptyString:
    def test_empty_description_passes_guard_and_reaches_sdk(self):
        sdk = _make_sdk()
        current = _make_current_role()
        sdk.role.get_role.return_value = current
        updated = MagicMock()
        updated.name = "OldName"
        updated.description = ""
        updated.privileges = current.privileges
        updated.id_ = "role-1"
        updated.account_id = "acct-123"
        sdk.role.update_role.return_value = updated

        result = _action_manage_role(
            sdk, "dev",
            operation="update",
            resource_id="role-1",
            description="",
        )

        assert result["_success"] is True
        call_kwargs = sdk.role.update_role.call_args
        request_body = call_kwargs.kwargs.get("request_body") or call_kwargs[1].get("request_body")
        assert request_body.description == ""


# ── Update guard: clearing privileges to empty list ──────────────────

class TestUpdateClearPrivilegesEmptyList:
    def test_empty_privileges_passes_guard_and_sends_empty(self):
        sdk = _make_sdk()
        current = _make_current_role()
        sdk.role.get_role.return_value = current
        updated = MagicMock()
        updated.name = "OldName"
        updated.description = "OldDesc"
        updated.privileges = MagicMock()
        updated.privileges.privilege = []
        updated.id_ = "role-1"
        updated.account_id = "acct-123"
        sdk.role.update_role.return_value = updated

        result = _action_manage_role(
            sdk, "dev",
            operation="update",
            resource_id="role-1",
            privileges=[],
        )

        assert result["_success"] is True
        call_kwargs = sdk.role.update_role.call_args
        request_body = call_kwargs.kwargs.get("request_body") or call_kwargs[1].get("request_body")
        # Should set privileges explicitly (not fall through to current.privileges)
        assert request_body.privileges.privilege == []


# ── Create: privileges as string rejected ────────────────────────────

class TestCreatePrivilegesStringRejected:
    def test_string_privileges_returns_error(self):
        sdk = _make_sdk()
        result = _action_manage_role(
            sdk, "dev",
            operation="create",
            name="TestRole",
            privileges="API",
        )

        assert result["_success"] is False
        assert "must be a list" in result["error"]
        sdk.role.create_role.assert_not_called()


# ── Update: privileges as string rejected ────────────────────────────

class TestUpdatePrivilegesStringRejected:
    def test_string_privileges_returns_error(self):
        sdk = _make_sdk()
        result = _action_manage_role(
            sdk, "dev",
            operation="update",
            resource_id="role-1",
            privileges="API",
        )

        assert result["_success"] is False
        assert "must be a list" in result["error"]
        sdk.role.get_role.assert_not_called()
        sdk.role.update_role.assert_not_called()


# ── Update: no fields still rejected ────────────────────────────────

class TestUpdateNoFieldsRejected:
    def test_no_fields_returns_error(self):
        sdk = _make_sdk()
        result = _action_manage_role(
            sdk, "dev",
            operation="update",
            resource_id="role-1",
        )

        assert result["_success"] is False
        assert "At least one of" in result["error"]
        sdk.role.get_role.assert_not_called()
