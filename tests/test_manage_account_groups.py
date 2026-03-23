"""Unit tests for manage_account_groups category module (mocked SDK)."""

import sys
import os
from unittest.mock import MagicMock, patch

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from src.boomi_mcp.categories.account_groups import (
    _account_group_to_dict,
    _account_group_account_to_dict,
    _account_group_user_role_to_dict,
    _account_group_integration_pack_to_dict,
    _action_list,
    _action_get,
    _action_create,
    _action_update,
    _action_delete,
    _action_list_accounts,
    _action_add_account,
    _action_remove_account,
    _action_list_user_roles,
    _action_assign_user_role,
    _action_remove_user_role,
    _action_list_integration_packs,
    _action_share_integration_pack,
    _action_unshare_integration_pack,
    manage_account_groups_action,
)


# ── Fixtures ─────────────────────────────────────────────────────────


def _make_sdk():
    sdk = MagicMock()
    return sdk


def _make_account_group(id_="ag-1", name="Test Group", account_id="acct-1",
                         default_group=False, auto_subscribe_alert_level=None):
    ag = MagicMock()
    ag.id_ = id_
    ag.name = name
    ag.account_id = account_id
    ag.default_group = default_group
    ag.auto_subscribe_alert_level = auto_subscribe_alert_level
    return ag


def _make_account_group_account(id_="aga-1", account_group_id="ag-1", account_id="acct-2"):
    aga = MagicMock()
    aga.id_ = id_
    aga.account_group_id = account_group_id
    aga.account_id = account_id
    return aga


def _make_user_role(id_="agur-1", account_group_id="ag-1", user_id="user-1",
                    role_id="role-1", first_name="John", last_name="Doe",
                    notify_user=True):
    ur = MagicMock()
    ur.id_ = id_
    ur.account_group_id = account_group_id
    ur.user_id = user_id
    ur.role_id = role_id
    ur.first_name = first_name
    ur.last_name = last_name
    ur.notify_user = notify_user
    return ur


def _make_integration_pack(id_="agip-1", account_group_id="ag-1",
                           integration_pack_id="ip-1",
                           integration_pack_name="Pack 1",
                           installation_type=None):
    ip = MagicMock()
    ip.id_ = id_
    ip.account_group_id = account_group_id
    ip.integration_pack_id = integration_pack_id
    ip.integration_pack_name = integration_pack_name
    ip.installation_type = installation_type
    return ip


def _make_query_response(results, query_token=None):
    resp = MagicMock()
    resp.result = results
    resp.query_token = query_token
    return resp


# ── _account_group_to_dict ───────────────────────────────────────────


class TestAccountGroupToDict:
    def test_basic_conversion(self):
        ag = _make_account_group()
        d = _account_group_to_dict(ag)
        assert d["id"] == "ag-1"
        assert d["name"] == "Test Group"
        assert d["account_id"] == "acct-1"
        assert d["default_group"] is False

    def test_alert_level_enum(self):
        level = MagicMock()
        level.value = "warning"
        ag = _make_account_group(auto_subscribe_alert_level=level)
        d = _account_group_to_dict(ag)
        assert d["auto_subscribe_alert_level"] == "warning"


# ── _action_list ─────────────────────────────────────────────────────


class TestActionList:
    def test_list_all(self):
        sdk = _make_sdk()
        ag1 = _make_account_group(id_="ag-1", name="Group 1")
        ag2 = _make_account_group(id_="ag-2", name="Group 2")
        sdk.account_group.query_account_group.return_value = _make_query_response([ag1, ag2])

        result = _action_list(sdk, "dev")
        assert result["_success"] is True
        assert result["total_count"] == 2
        assert len(result["account_groups"]) == 2

    def test_list_with_name_filter(self):
        sdk = _make_sdk()
        ag = _make_account_group(id_="ag-1", name="Filtered")
        sdk.account_group.query_account_group.return_value = _make_query_response([ag])

        result = _action_list(sdk, "dev", name="Filter*")
        assert result["_success"] is True
        assert result["total_count"] == 1

    def test_list_with_pagination(self):
        sdk = _make_sdk()
        ag1 = _make_account_group(id_="ag-1")
        ag2 = _make_account_group(id_="ag-2")
        page1 = _make_query_response([ag1], query_token="token-1")
        page2 = _make_query_response([ag2])

        sdk.account_group.query_account_group.return_value = page1
        sdk.account_group.query_more_account_group.return_value = page2

        result = _action_list(sdk, "dev")
        assert result["_success"] is True
        assert result["total_count"] == 2

    def test_list_empty(self):
        sdk = _make_sdk()
        sdk.account_group.query_account_group.return_value = _make_query_response([])

        result = _action_list(sdk, "dev")
        assert result["_success"] is True
        assert result["total_count"] == 0


# ── _action_get ──────────────────────────────────────────────────────


class TestActionGet:
    def test_get_by_id(self):
        sdk = _make_sdk()
        ag = _make_account_group()
        sdk.account_group.get_account_group.return_value = ag

        result = _action_get(sdk, "dev", resource_id="ag-1")
        assert result["_success"] is True
        assert result["account_group"]["id"] == "ag-1"
        sdk.account_group.get_account_group.assert_called_once_with(id_="ag-1")

    def test_get_requires_resource_id(self):
        sdk = _make_sdk()
        result = _action_get(sdk, "dev")
        assert result["_success"] is False
        assert "resource_id" in result["error"]


# ── _action_create ───────────────────────────────────────────────────


class TestActionCreate:
    def test_create_with_name(self):
        sdk = _make_sdk()
        ag = _make_account_group(id_="ag-new", name="New Group")
        sdk.account_group.create_account_group.return_value = ag

        result = _action_create(sdk, "dev", name="New Group")
        assert result["_success"] is True
        assert result["account_group"]["name"] == "New Group"

    def test_create_with_alert_level(self):
        sdk = _make_sdk()
        ag = _make_account_group()
        sdk.account_group.create_account_group.return_value = ag

        result = _action_create(sdk, "dev", name="G", auto_subscribe_alert_level="warning")
        assert result["_success"] is True

    def test_create_requires_name(self):
        sdk = _make_sdk()
        result = _action_create(sdk, "dev")
        assert result["_success"] is False
        assert "name" in result["error"]


# ── _action_update ───────────────────────────────────────────────────


class TestActionUpdate:
    def test_update_name(self):
        sdk = _make_sdk()
        ag = _make_account_group(id_="ag-1", name="Updated")
        sdk.account_group.update_account_group.return_value = ag

        result = _action_update(sdk, "dev", resource_id="ag-1", name="Updated")
        assert result["_success"] is True
        assert result["account_group"]["name"] == "Updated"
        sdk.account_group.update_account_group.assert_called_once()

    def test_update_requires_resource_id(self):
        sdk = _make_sdk()
        result = _action_update(sdk, "dev", name="X")
        assert result["_success"] is False
        assert "resource_id" in result["error"]


# ── _action_delete ───────────────────────────────────────────────────


class TestActionDelete:
    def test_delete_not_supported(self):
        sdk = _make_sdk()
        result = _action_delete(sdk, "dev", resource_id="ag-1")
        assert result["_success"] is False
        assert "does not provide a DELETE" in result["error"]


# ── _action_list_accounts ────────────────────────────────────────────


class TestActionListAccounts:
    def test_list_accounts_for_group(self):
        sdk = _make_sdk()
        aga = _make_account_group_account()
        sdk.account_group_account.query_account_group_account.return_value = (
            _make_query_response([aga])
        )

        result = _action_list_accounts(sdk, "dev", account_group_id="ag-1")
        assert result["_success"] is True
        assert result["total_count"] == 1
        assert result["accounts"][0]["account_group_id"] == "ag-1"

    def test_list_accounts_all(self):
        sdk = _make_sdk()
        sdk.account_group_account.query_account_group_account.return_value = (
            _make_query_response([])
        )

        result = _action_list_accounts(sdk, "dev")
        assert result["_success"] is True
        assert result["total_count"] == 0


# ── _action_add_account ──────────────────────────────────────────────


class TestActionAddAccount:
    def test_add_account(self):
        sdk = _make_sdk()
        aga = _make_account_group_account()
        sdk.account_group_account.create_account_group_account.return_value = aga

        result = _action_add_account(sdk, "dev", account_group_id="ag-1", account_id="acct-2")
        assert result["_success"] is True
        assert result["account_association"]["account_id"] == "acct-2"

    def test_add_account_missing_group_id(self):
        sdk = _make_sdk()
        result = _action_add_account(sdk, "dev", account_id="acct-2")
        assert result["_success"] is False

    def test_add_account_missing_account_id(self):
        sdk = _make_sdk()
        result = _action_add_account(sdk, "dev", account_group_id="ag-1")
        assert result["_success"] is False


# ── _action_remove_account ───────────────────────────────────────────


class TestActionRemoveAccount:
    def test_remove_by_resource_id(self):
        sdk = _make_sdk()
        result = _action_remove_account(sdk, "dev", resource_id="aga-1")
        assert result["_success"] is True
        assert result["deleted_id"] == "aga-1"
        sdk.account_group_account.delete_account_group_account.assert_called_once_with(
            id_="aga-1"
        )

    def test_remove_by_lookup(self):
        sdk = _make_sdk()
        aga = _make_account_group_account(id_="aga-99", account_group_id="ag-1", account_id="acct-2")
        sdk.account_group_account.query_account_group_account.return_value = (
            _make_query_response([aga])
        )

        result = _action_remove_account(
            sdk, "dev", account_group_id="ag-1", account_id="acct-2"
        )
        assert result["_success"] is True
        assert result["deleted_id"] == "aga-99"

    def test_remove_lookup_not_found(self):
        sdk = _make_sdk()
        sdk.account_group_account.query_account_group_account.return_value = (
            _make_query_response([])
        )

        result = _action_remove_account(
            sdk, "dev", account_group_id="ag-1", account_id="acct-missing"
        )
        assert result["_success"] is False
        assert "no association found" in result["error"].lower()

    def test_remove_missing_params(self):
        sdk = _make_sdk()
        result = _action_remove_account(sdk, "dev", account_group_id="ag-1")
        assert result["_success"] is False


# ── _action_list_user_roles ──────────────────────────────────────────


class TestActionListUserRoles:
    def test_list_user_roles(self):
        sdk = _make_sdk()
        ur = _make_user_role()
        sdk.account_group_user_role.query_account_group_user_role.return_value = (
            _make_query_response([ur])
        )

        result = _action_list_user_roles(sdk, "dev", account_group_id="ag-1")
        assert result["_success"] is True
        assert result["total_count"] == 1
        assert result["user_roles"][0]["user_id"] == "user-1"


# ── _action_assign_user_role ─────────────────────────────────────────


class TestActionAssignUserRole:
    def test_assign(self):
        sdk = _make_sdk()
        ur = _make_user_role()
        sdk.account_group_user_role.create_account_group_user_role.return_value = ur

        result = _action_assign_user_role(
            sdk, "dev", account_group_id="ag-1", user_id="user-1", role_id="role-1"
        )
        assert result["_success"] is True
        assert result["user_role"]["role_id"] == "role-1"

    def test_assign_missing_user_id(self):
        sdk = _make_sdk()
        result = _action_assign_user_role(
            sdk, "dev", account_group_id="ag-1", role_id="role-1"
        )
        assert result["_success"] is False

    def test_assign_missing_role_id(self):
        sdk = _make_sdk()
        result = _action_assign_user_role(
            sdk, "dev", account_group_id="ag-1", user_id="user-1"
        )
        assert result["_success"] is False

    def test_assign_missing_group_id(self):
        sdk = _make_sdk()
        result = _action_assign_user_role(
            sdk, "dev", user_id="user-1", role_id="role-1"
        )
        assert result["_success"] is False


# ── _action_remove_user_role ─────────────────────────────────────────


class TestActionRemoveUserRole:
    def test_remove_by_resource_id(self):
        sdk = _make_sdk()
        result = _action_remove_user_role(sdk, "dev", resource_id="agur-1")
        assert result["_success"] is True
        assert result["deleted_id"] == "agur-1"

    def test_remove_by_lookup(self):
        sdk = _make_sdk()
        ur = _make_user_role(id_="agur-99")
        sdk.account_group_user_role.query_account_group_user_role.return_value = (
            _make_query_response([ur])
        )

        result = _action_remove_user_role(
            sdk, "dev", account_group_id="ag-1", user_id="user-1", role_id="role-1"
        )
        assert result["_success"] is True
        assert result["deleted_id"] == "agur-99"

    def test_remove_lookup_not_found(self):
        sdk = _make_sdk()
        sdk.account_group_user_role.query_account_group_user_role.return_value = (
            _make_query_response([])
        )

        result = _action_remove_user_role(
            sdk, "dev", account_group_id="ag-1", user_id="user-1", role_id="role-missing"
        )
        assert result["_success"] is False

    def test_remove_missing_params(self):
        sdk = _make_sdk()
        result = _action_remove_user_role(
            sdk, "dev", account_group_id="ag-1", user_id="user-1"
        )
        assert result["_success"] is False


# ── _action_list_integration_packs ───────────────────────────────────


class TestActionListIntegrationPacks:
    def test_list_integration_packs(self):
        sdk = _make_sdk()
        ip = _make_integration_pack()
        sdk.account_group_integration_pack.query_account_group_integration_pack.return_value = (
            _make_query_response([ip])
        )

        result = _action_list_integration_packs(sdk, "dev", account_group_id="ag-1")
        assert result["_success"] is True
        assert result["total_count"] == 1
        assert result["integration_packs"][0]["integration_pack_id"] == "ip-1"


# ── _action_share_integration_pack ───────────────────────────────────


class TestActionShareIntegrationPack:
    def test_share(self):
        sdk = _make_sdk()
        ip = _make_integration_pack()
        sdk.account_group_integration_pack.create_account_group_integration_pack.return_value = ip

        result = _action_share_integration_pack(
            sdk, "dev", account_group_id="ag-1", integration_pack_id="ip-1"
        )
        assert result["_success"] is True
        assert result["integration_pack"]["integration_pack_id"] == "ip-1"

    def test_share_missing_group_id(self):
        sdk = _make_sdk()
        result = _action_share_integration_pack(
            sdk, "dev", integration_pack_id="ip-1"
        )
        assert result["_success"] is False

    def test_share_missing_pack_id(self):
        sdk = _make_sdk()
        result = _action_share_integration_pack(
            sdk, "dev", account_group_id="ag-1"
        )
        assert result["_success"] is False


# ── _action_unshare_integration_pack ─────────────────────────────────


class TestActionUnshareIntegrationPack:
    def test_unshare_by_resource_id(self):
        sdk = _make_sdk()
        result = _action_unshare_integration_pack(sdk, "dev", resource_id="agip-1")
        assert result["_success"] is True
        assert result["deleted_id"] == "agip-1"

    def test_unshare_by_lookup(self):
        sdk = _make_sdk()
        ip = _make_integration_pack(id_="agip-99", integration_pack_id="ip-1")
        sdk.account_group_integration_pack.query_account_group_integration_pack.return_value = (
            _make_query_response([ip])
        )

        result = _action_unshare_integration_pack(
            sdk, "dev", account_group_id="ag-1", integration_pack_id="ip-1"
        )
        assert result["_success"] is True
        assert result["deleted_id"] == "agip-99"

    def test_unshare_lookup_not_found(self):
        sdk = _make_sdk()
        sdk.account_group_integration_pack.query_account_group_integration_pack.return_value = (
            _make_query_response([])
        )

        result = _action_unshare_integration_pack(
            sdk, "dev", account_group_id="ag-1", integration_pack_id="ip-missing"
        )
        assert result["_success"] is False

    def test_unshare_missing_params(self):
        sdk = _make_sdk()
        result = _action_unshare_integration_pack(
            sdk, "dev", account_group_id="ag-1"
        )
        assert result["_success"] is False


# ── manage_account_groups_action (Router) ────────────────────────────


class TestRouter:
    def test_unknown_action(self):
        sdk = _make_sdk()
        result = manage_account_groups_action(sdk, "dev", "bogus")
        assert result["_success"] is False
        assert "Unknown action" in result["error"]
        assert "valid_actions" in result

    def test_routes_to_list(self):
        sdk = _make_sdk()
        sdk.account_group.query_account_group.return_value = _make_query_response([])

        result = manage_account_groups_action(sdk, "dev", "list")
        assert result["_success"] is True

    def test_routes_to_delete(self):
        sdk = _make_sdk()
        result = manage_account_groups_action(sdk, "dev", "delete", config_data={"resource_id": "ag-1"})
        assert result["_success"] is False
        assert "does not provide a DELETE" in result["error"]

    def test_api_error_handling(self):
        from boomi.net.transport.api_error import ApiError

        sdk = _make_sdk()
        sdk.account_group.get_account_group.side_effect = ApiError(
            "Test API error", 404, {}
        )

        result = manage_account_groups_action(
            sdk, "dev", "get", config_data={"resource_id": "ag-bad"}
        )
        assert result["_success"] is False
        assert "ApiError" in result["exception_type"]

    def test_generic_error_handling(self):
        sdk = _make_sdk()
        sdk.account_group.get_account_group.side_effect = RuntimeError("boom")

        result = manage_account_groups_action(
            sdk, "dev", "get", config_data={"resource_id": "ag-bad"}
        )
        assert result["_success"] is False
        assert "RuntimeError" in result["exception_type"]

    def test_config_data_merges_with_kwargs(self):
        sdk = _make_sdk()
        ag = _make_account_group()
        sdk.account_group.get_account_group.return_value = ag

        result = manage_account_groups_action(
            sdk, "dev", "get", config_data={"resource_id": "ag-1"}
        )
        assert result["_success"] is True
        assert result["account_group"]["id"] == "ag-1"

    def test_all_14_actions_registered(self):
        sdk = _make_sdk()
        result = manage_account_groups_action(sdk, "dev", "nonexistent_action")
        valid = result["valid_actions"]
        assert len(valid) == 14
        expected = [
            "list", "get", "create", "update", "delete",
            "list_accounts", "add_account", "remove_account",
            "list_user_roles", "assign_user_role", "remove_user_role",
            "list_integration_packs", "share_integration_pack", "unshare_integration_pack",
        ]
        assert sorted(valid) == sorted(expected)
