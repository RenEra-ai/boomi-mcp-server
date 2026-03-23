"""Unit tests for manage_account actions (mocked SDK — no live Boomi dependency)."""

import sys
import os
from unittest.mock import MagicMock, PropertyMock

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from src.boomi_mcp.categories.account import (
    _action_manage_role,
    _action_list_assignable_roles,
    _action_list_user_roles,
    _action_assign_user_role,
    _action_remove_user_role,
    _action_list_user_federations,
    _action_create_user_federation,
    _action_delete_user_federation,
    _action_get_sso_config,
    manage_account_action,
)


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


def _make_user_role(id_="ur-1", user_id="user@example.com", role_id="role-1",
                    account_id="acct-123", first_name=None, last_name=None):
    ur = MagicMock()
    ur.id_ = id_
    ur.user_id = user_id
    ur.role_id = role_id
    ur.account_id = account_id
    ur.first_name = first_name
    ur.last_name = last_name
    ur.notify_user = None
    return ur


def _make_user_federation(id_="uf-1", user_id="user@example.com",
                          federation_id="fed-abc", account_id="acct-123"):
    uf = MagicMock()
    uf.id_ = id_
    uf.user_id = user_id
    uf.federation_id = federation_id
    uf.account_id = account_id
    return uf


def _make_sso_config(account_id="acct-123", enabled=True, idp_url="https://idp.example.com"):
    sso = MagicMock()
    sso.account_id = account_id
    sso.enabled = enabled
    sso.idp_url = idp_url
    sso.cert_info = "CN=test"
    sso.assertion_encryption = False
    sso.authn_context = "PPT"
    sso.authn_context_comparison = "EXACT"
    sso.fed_id_from_name_id = True
    sso.name_id_policy = "TRANSIENT"
    sso.signout_redirect_url = "https://example.com/logout"
    sso.certificate = ["base64cert=="]
    return sso


def _make_query_result(items, query_token=None):
    result = MagicMock()
    result.result = items
    result.query_token = query_token
    return result


# ============================================================================
# Existing role tests (preserved from original)
# ============================================================================

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
        assert request_body.privileges.privilege == []


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


class TestUpdateInvalidNameRejected:
    def test_empty_name_returns_error(self):
        sdk = _make_sdk()
        result = _action_manage_role(
            sdk, "dev",
            operation="update",
            resource_id="role-1",
            name="",
        )

        assert result["_success"] is False
        assert "non-empty string" in result["error"]
        sdk.role.get_role.assert_not_called()
        sdk.role.update_role.assert_not_called()

    def test_whitespace_only_name_returns_error(self):
        sdk = _make_sdk()
        result = _action_manage_role(
            sdk, "dev",
            operation="update",
            resource_id="role-1",
            name="   ",
        )

        assert result["_success"] is False
        assert "non-empty string" in result["error"]
        sdk.role.get_role.assert_not_called()
        sdk.role.update_role.assert_not_called()

    def test_non_string_name_returns_error(self):
        sdk = _make_sdk()
        result = _action_manage_role(
            sdk, "dev",
            operation="update",
            resource_id="role-1",
            name=123,
        )

        assert result["_success"] is False
        assert "non-empty string" in result["error"]
        sdk.role.get_role.assert_not_called()
        sdk.role.update_role.assert_not_called()


class TestCreateInvalidNameRejected:
    def test_whitespace_only_name_returns_error(self):
        sdk = _make_sdk()
        result = _action_manage_role(
            sdk, "dev",
            operation="create",
            name="   ",
        )

        assert result["_success"] is False
        assert "non-empty string" in result["error"]
        sdk.role.create_role.assert_not_called()

    def test_non_string_name_returns_error(self):
        sdk = _make_sdk()
        result = _action_manage_role(
            sdk, "dev",
            operation="create",
            name=123,
        )

        assert result["_success"] is False
        assert "non-empty string" in result["error"]
        sdk.role.create_role.assert_not_called()


class TestCreatePrivilegesElementValidation:
    def test_integer_element_rejected(self):
        sdk = _make_sdk()
        result = _action_manage_role(
            sdk, "dev",
            operation="create",
            name="TestRole",
            privileges=[1, "API"],
        )

        assert result["_success"] is False
        assert "privileges[0]" in result["error"]
        sdk.role.create_role.assert_not_called()

    def test_empty_string_element_rejected(self):
        sdk = _make_sdk()
        result = _action_manage_role(
            sdk, "dev",
            operation="create",
            name="TestRole",
            privileges=["API", ""],
        )

        assert result["_success"] is False
        assert "privileges[1]" in result["error"]
        sdk.role.create_role.assert_not_called()

    def test_whitespace_only_element_rejected(self):
        sdk = _make_sdk()
        result = _action_manage_role(
            sdk, "dev",
            operation="create",
            name="TestRole",
            privileges=["API", "   "],
        )

        assert result["_success"] is False
        assert "privileges[1]" in result["error"]
        sdk.role.create_role.assert_not_called()


class TestUpdatePrivilegesElementValidation:
    def test_integer_element_rejected(self):
        sdk = _make_sdk()
        result = _action_manage_role(
            sdk, "dev",
            operation="update",
            resource_id="role-1",
            privileges=[1, "API"],
        )

        assert result["_success"] is False
        assert "privileges[0]" in result["error"]
        sdk.role.get_role.assert_not_called()
        sdk.role.update_role.assert_not_called()

    def test_empty_string_element_rejected(self):
        sdk = _make_sdk()
        result = _action_manage_role(
            sdk, "dev",
            operation="update",
            resource_id="role-1",
            privileges=["API", ""],
        )

        assert result["_success"] is False
        assert "privileges[1]" in result["error"]
        sdk.role.get_role.assert_not_called()
        sdk.role.update_role.assert_not_called()


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


# ============================================================================
# list_assignable_roles
# ============================================================================

class TestListAssignableRoles:
    def test_success_with_roles(self):
        sdk = _make_sdk()
        role1 = MagicMock()
        role1.id_ = "role-1"
        role1.name = "Admin"
        role1.description = "Administrator"
        role1.account_id = "acct-123"
        role1.parent_id = None
        role1.privileges = None

        roles_result = MagicMock()
        roles_result.role = [role1]
        sdk.get_assignable_roles.get_get_assignable_roles.return_value = roles_result

        result = _action_list_assignable_roles(sdk, "dev")

        assert result["_success"] is True
        assert result["total_count"] == 1
        assert result["roles"][0]["name"] == "Admin"

    def test_success_empty(self):
        sdk = _make_sdk()
        roles_result = MagicMock()
        roles_result.role = None
        sdk.get_assignable_roles.get_get_assignable_roles.return_value = roles_result

        result = _action_list_assignable_roles(sdk, "dev")

        assert result["_success"] is True
        assert result["total_count"] == 0
        assert result["roles"] == []

    def test_via_router(self):
        sdk = _make_sdk()
        roles_result = MagicMock()
        roles_result.role = []
        sdk.get_assignable_roles.get_get_assignable_roles.return_value = roles_result

        result = manage_account_action(sdk, "dev", "list_assignable_roles")

        assert result["_success"] is True
        assert result["total_count"] == 0


# ============================================================================
# list_user_roles
# ============================================================================

class TestListUserRoles:
    def test_success_all(self):
        sdk = _make_sdk()
        ur1 = _make_user_role(id_="ur-1", user_id="alice@test.com", role_id="role-1")
        ur2 = _make_user_role(id_="ur-2", user_id="bob@test.com", role_id="role-2")
        query_result = _make_query_result([ur1, ur2])
        sdk.account_user_role.query_account_user_role.return_value = query_result

        result = _action_list_user_roles(sdk, "dev")

        assert result["_success"] is True
        assert result["total_count"] == 2
        assert result["user_roles"][0]["user_id"] == "alice@test.com"
        assert result["user_roles"][1]["user_id"] == "bob@test.com"

    def test_success_with_user_id_filter(self):
        sdk = _make_sdk()
        ur1 = _make_user_role(id_="ur-1", user_id="alice@test.com", role_id="role-1")
        query_result = _make_query_result([ur1])
        sdk.account_user_role.query_account_user_role.return_value = query_result

        result = _action_list_user_roles(sdk, "dev", user_id="alice@test.com")

        assert result["_success"] is True
        assert result["total_count"] == 1

    def test_success_empty(self):
        sdk = _make_sdk()
        query_result = _make_query_result([])
        sdk.account_user_role.query_account_user_role.return_value = query_result

        result = _action_list_user_roles(sdk, "dev")

        assert result["_success"] is True
        assert result["total_count"] == 0

    def test_pagination(self):
        sdk = _make_sdk()
        ur1 = _make_user_role(id_="ur-1")
        ur2 = _make_user_role(id_="ur-2")
        page1 = _make_query_result([ur1], query_token="token123")
        page2 = _make_query_result([ur2])
        sdk.account_user_role.query_account_user_role.return_value = page1
        sdk.account_user_role.query_more_account_user_role.return_value = page2

        result = _action_list_user_roles(sdk, "dev")

        assert result["_success"] is True
        assert result["total_count"] == 2
        sdk.account_user_role.query_more_account_user_role.assert_called_once_with(
            request_body="token123"
        )


# ============================================================================
# assign_user_role
# ============================================================================

class TestAssignUserRole:
    def test_success(self):
        sdk = _make_sdk()
        created = _make_user_role(id_="ur-new", user_id="alice@test.com", role_id="role-1")
        sdk.account_user_role.create_account_user_role.return_value = created

        result = _action_assign_user_role(sdk, "dev",
                                          user_id="alice@test.com", role_id="role-1")

        assert result["_success"] is True
        assert result["user_role"]["user_id"] == "alice@test.com"
        assert result["user_role"]["role_id"] == "role-1"
        sdk.account_user_role.create_account_user_role.assert_called_once()

    def test_missing_user_id(self):
        sdk = _make_sdk()
        result = _action_assign_user_role(sdk, "dev", role_id="role-1")

        assert result["_success"] is False
        assert "user_id" in result["error"]
        sdk.account_user_role.create_account_user_role.assert_not_called()

    def test_missing_role_id(self):
        sdk = _make_sdk()
        result = _action_assign_user_role(sdk, "dev", user_id="alice@test.com")

        assert result["_success"] is False
        assert "role_id" in result["error"]
        sdk.account_user_role.create_account_user_role.assert_not_called()

    def test_with_optional_fields(self):
        sdk = _make_sdk()
        created = _make_user_role(id_="ur-new", user_id="alice@test.com", role_id="role-1",
                                  first_name="Alice", last_name="Smith")
        sdk.account_user_role.create_account_user_role.return_value = created

        result = _action_assign_user_role(
            sdk, "dev",
            user_id="alice@test.com", role_id="role-1",
            first_name="Alice", last_name="Smith", notify_user=True,
        )

        assert result["_success"] is True
        call_args = sdk.account_user_role.create_account_user_role.call_args
        request_body = call_args.kwargs.get("request_body") or call_args[1].get("request_body")
        assert request_body.first_name == "Alice"
        assert request_body.last_name == "Smith"
        assert request_body.notify_user is True


# ============================================================================
# remove_user_role
# ============================================================================

class TestRemoveUserRole:
    def test_success_by_resource_id(self):
        sdk = _make_sdk()

        result = _action_remove_user_role(sdk, "dev", resource_id="ur-1")

        assert result["_success"] is True
        assert result["deleted_id"] == "ur-1"
        sdk.account_user_role.delete_account_user_role.assert_called_once_with(id_="ur-1")

    def test_success_by_user_and_role_id(self):
        sdk = _make_sdk()
        ur1 = _make_user_role(id_="ur-1", user_id="alice@test.com", role_id="role-1")
        query_result = _make_query_result([ur1])
        sdk.account_user_role.query_account_user_role.return_value = query_result

        result = _action_remove_user_role(sdk, "dev",
                                          user_id="alice@test.com", role_id="role-1")

        assert result["_success"] is True
        assert result["deleted_id"] == "ur-1"
        sdk.account_user_role.delete_account_user_role.assert_called_once_with(id_="ur-1")

    def test_missing_both_resource_id_and_user_role_ids(self):
        sdk = _make_sdk()

        result = _action_remove_user_role(sdk, "dev")

        assert result["_success"] is False
        assert "resource_id" in result["error"]
        sdk.account_user_role.delete_account_user_role.assert_not_called()

    def test_missing_role_id_when_no_resource_id(self):
        sdk = _make_sdk()

        result = _action_remove_user_role(sdk, "dev", user_id="alice@test.com")

        assert result["_success"] is False
        assert "resource_id" in result["error"] or "role_id" in result["error"]
        sdk.account_user_role.delete_account_user_role.assert_not_called()

    def test_no_matching_assignment_found(self):
        sdk = _make_sdk()
        query_result = _make_query_result([])
        sdk.account_user_role.query_account_user_role.return_value = query_result

        result = _action_remove_user_role(sdk, "dev",
                                          user_id="alice@test.com", role_id="role-nonexist")

        assert result["_success"] is False
        assert "No user-role assignment found" in result["error"]
        sdk.account_user_role.delete_account_user_role.assert_not_called()


# ============================================================================
# list_user_federations
# ============================================================================

class TestListUserFederations:
    def test_success_all(self):
        sdk = _make_sdk()
        uf1 = _make_user_federation(id_="uf-1", user_id="alice@test.com", federation_id="fed-a")
        uf2 = _make_user_federation(id_="uf-2", user_id="bob@test.com", federation_id="fed-b")
        query_result = _make_query_result([uf1, uf2])
        sdk.account_user_federation.query_account_user_federation.return_value = query_result

        result = _action_list_user_federations(sdk, "dev")

        assert result["_success"] is True
        assert result["total_count"] == 2
        assert result["user_federations"][0]["federation_id"] == "fed-a"

    def test_success_with_user_id_filter(self):
        sdk = _make_sdk()
        uf1 = _make_user_federation(id_="uf-1", user_id="alice@test.com", federation_id="fed-a")
        query_result = _make_query_result([uf1])
        sdk.account_user_federation.query_account_user_federation.return_value = query_result

        result = _action_list_user_federations(sdk, "dev", user_id="alice@test.com")

        assert result["_success"] is True
        assert result["total_count"] == 1

    def test_success_empty(self):
        sdk = _make_sdk()
        query_result = _make_query_result([])
        sdk.account_user_federation.query_account_user_federation.return_value = query_result

        result = _action_list_user_federations(sdk, "dev")

        assert result["_success"] is True
        assert result["total_count"] == 0

    def test_pagination(self):
        sdk = _make_sdk()
        uf1 = _make_user_federation(id_="uf-1")
        uf2 = _make_user_federation(id_="uf-2")
        page1 = _make_query_result([uf1], query_token="tok-fed")
        page2 = _make_query_result([uf2])
        sdk.account_user_federation.query_account_user_federation.return_value = page1
        sdk.account_user_federation.query_more_account_user_federation.return_value = page2

        result = _action_list_user_federations(sdk, "dev")

        assert result["_success"] is True
        assert result["total_count"] == 2
        sdk.account_user_federation.query_more_account_user_federation.assert_called_once_with(
            request_body="tok-fed"
        )


# ============================================================================
# create_user_federation
# ============================================================================

class TestCreateUserFederation:
    def test_success(self):
        sdk = _make_sdk()
        created = _make_user_federation(id_="uf-new", user_id="alice@test.com",
                                        federation_id="fed-abc")
        sdk.account_user_federation.create_account_user_federation.return_value = created

        result = _action_create_user_federation(sdk, "dev",
                                                user_id="alice@test.com",
                                                federation_id="fed-abc")

        assert result["_success"] is True
        assert result["user_federation"]["federation_id"] == "fed-abc"
        sdk.account_user_federation.create_account_user_federation.assert_called_once()

    def test_missing_user_id(self):
        sdk = _make_sdk()
        result = _action_create_user_federation(sdk, "dev", federation_id="fed-abc")

        assert result["_success"] is False
        assert "user_id" in result["error"]
        sdk.account_user_federation.create_account_user_federation.assert_not_called()

    def test_missing_federation_id(self):
        sdk = _make_sdk()
        result = _action_create_user_federation(sdk, "dev", user_id="alice@test.com")

        assert result["_success"] is False
        assert "federation_id" in result["error"]
        sdk.account_user_federation.create_account_user_federation.assert_not_called()


# ============================================================================
# delete_user_federation
# ============================================================================

class TestDeleteUserFederation:
    def test_success_by_resource_id(self):
        sdk = _make_sdk()

        result = _action_delete_user_federation(sdk, "dev", resource_id="uf-1")

        assert result["_success"] is True
        assert result["deleted_id"] == "uf-1"
        sdk.account_user_federation.delete_account_user_federation.assert_called_once_with(id_="uf-1")

    def test_success_by_user_and_federation_id(self):
        sdk = _make_sdk()
        uf1 = _make_user_federation(id_="uf-1", user_id="alice@test.com", federation_id="fed-abc")
        query_result = _make_query_result([uf1])
        sdk.account_user_federation.query_account_user_federation.return_value = query_result

        result = _action_delete_user_federation(sdk, "dev",
                                                user_id="alice@test.com",
                                                federation_id="fed-abc")

        assert result["_success"] is True
        assert result["deleted_id"] == "uf-1"
        sdk.account_user_federation.delete_account_user_federation.assert_called_once_with(id_="uf-1")

    def test_missing_both_resource_id_and_user_federation_ids(self):
        sdk = _make_sdk()

        result = _action_delete_user_federation(sdk, "dev")

        assert result["_success"] is False
        assert "resource_id" in result["error"]
        sdk.account_user_federation.delete_account_user_federation.assert_not_called()

    def test_missing_federation_id_when_no_resource_id(self):
        sdk = _make_sdk()

        result = _action_delete_user_federation(sdk, "dev", user_id="alice@test.com")

        assert result["_success"] is False
        assert "resource_id" in result["error"] or "federation_id" in result["error"]
        sdk.account_user_federation.delete_account_user_federation.assert_not_called()

    def test_no_matching_federation_found(self):
        sdk = _make_sdk()
        query_result = _make_query_result([])
        sdk.account_user_federation.query_account_user_federation.return_value = query_result

        result = _action_delete_user_federation(sdk, "dev",
                                                user_id="alice@test.com",
                                                federation_id="fed-nonexist")

        assert result["_success"] is False
        assert "No user-federation mapping found" in result["error"]
        sdk.account_user_federation.delete_account_user_federation.assert_not_called()


# ============================================================================
# get_sso_config
# ============================================================================

class TestGetSsoConfig:
    def test_success(self):
        sdk = _make_sdk()
        sso = _make_sso_config()
        sdk.account_sso_config.get_account_sso_config.return_value = sso

        result = _action_get_sso_config(sdk, "dev")

        assert result["_success"] is True
        assert result["sso_config"]["account_id"] == "acct-123"
        assert result["sso_config"]["enabled"] is True
        assert result["sso_config"]["idp_url"] == "https://idp.example.com"
        assert result["sso_config"]["cert_info"] == "CN=test"
        assert result["sso_config"]["assertion_encryption"] is False
        assert result["sso_config"]["authn_context"] == "PPT"
        assert result["sso_config"]["name_id_policy"] == "TRANSIENT"
        assert result["sso_config"]["certificate"] == ["base64cert=="]
        assert "_note" in result
        sdk.account_sso_config.get_account_sso_config.assert_called_once_with(id_="acct-123")

    def test_uses_account_id_from_sdk(self):
        sdk = _make_sdk()
        sdk._base_url_account_id = "acct-999"
        sso = _make_sso_config(account_id="acct-999")
        sdk.account_sso_config.get_account_sso_config.return_value = sso

        result = _action_get_sso_config(sdk, "dev")

        assert result["_success"] is True
        sdk.account_sso_config.get_account_sso_config.assert_called_once_with(id_="acct-999")

    def test_via_router(self):
        sdk = _make_sdk()
        sso = _make_sso_config()
        sdk.account_sso_config.get_account_sso_config.return_value = sso

        result = manage_account_action(sdk, "dev", "get_sso_config")

        assert result["_success"] is True
        assert "sso_config" in result


# ============================================================================
# Router: unknown action
# ============================================================================

class TestRouterUnknownAction:
    def test_unknown_action_returns_error(self):
        sdk = _make_sdk()

        result = manage_account_action(sdk, "dev", "nonexistent_action")

        assert result["_success"] is False
        assert "Unknown action" in result["error"]
        assert "list_assignable_roles" in result["valid_actions"]
        assert "get_sso_config" in result["valid_actions"]

    def test_all_new_actions_in_valid_actions(self):
        sdk = _make_sdk()

        result = manage_account_action(sdk, "dev", "nonexistent_action")

        expected_actions = [
            "list_roles", "manage_role", "list_branches", "manage_branch",
            "list_assignable_roles", "list_user_roles", "assign_user_role",
            "remove_user_role", "list_user_federations", "create_user_federation",
            "delete_user_federation", "get_sso_config",
        ]
        for action in expected_actions:
            assert action in result["valid_actions"]


# ============================================================================
# Router: ApiError handling
# ============================================================================

class TestRouterApiErrorHandling:
    def test_api_error_caught_for_new_actions(self):
        from boomi.net.transport.api_error import ApiError

        sdk = _make_sdk()
        # list_user_roles uses SDK directly, so ApiError is caught by router
        sdk.account_user_role.query_account_user_role.side_effect = ApiError(
            "test error", 500, "internal"
        )

        result = manage_account_action(sdk, "dev", "list_user_roles")

        assert result["_success"] is False
        assert "list_user_roles" in result["error"]
        assert result["exception_type"] == "ApiError"
