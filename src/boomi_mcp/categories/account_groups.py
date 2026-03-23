"""
Account Group Management MCP Tool for Boomi Platform.

Provides 14 account group management actions:
- list / get / create / update / delete: Account group CRUD
- list_accounts / add_account / remove_account: Account associations
- list_user_roles / assign_user_role / remove_user_role: User role associations
- list_integration_packs / share_integration_pack / unshare_integration_pack: Integration pack sharing
"""

from typing import Dict, Any, Optional, List

from boomi import Boomi
from boomi.net.transport.api_error import ApiError
from boomi.models import (
    AccountGroup,
    AccountGroupQueryConfig,
    AccountGroupQueryConfigQueryFilter,
    AccountGroupSimpleExpression,
    AccountGroupSimpleExpressionOperator,
    AccountGroupSimpleExpressionProperty,
    AccountGroupAccount,
    AccountGroupAccountQueryConfig,
    AccountGroupAccountQueryConfigQueryFilter,
    AccountGroupAccountSimpleExpression,
    AccountGroupAccountSimpleExpressionOperator,
    AccountGroupAccountSimpleExpressionProperty,
    AccountGroupUserRole,
    AccountGroupUserRoleQueryConfig,
    AccountGroupUserRoleQueryConfigQueryFilter,
    AccountGroupUserRoleSimpleExpression,
    AccountGroupUserRoleSimpleExpressionOperator,
    AccountGroupUserRoleSimpleExpressionProperty,
    AccountGroupIntegrationPack,
    AccountGroupIntegrationPackQueryConfig,
    AccountGroupIntegrationPackQueryConfigQueryFilter,
    AccountGroupIntegrationPackExpression,
)


# ============================================================================
# Helpers
# ============================================================================

def _extract_api_error_msg(e) -> str:
    """Extract user-friendly error message from ApiError."""
    detail = getattr(e, "error_detail", None)
    if detail:
        return detail
    resp = getattr(e, "response", None)
    if resp:
        body = getattr(resp, "body", None)
        if isinstance(body, dict):
            msg = body.get("message", "")
            if msg:
                return msg
    return getattr(e, "message", "") or str(e)


def _account_group_to_dict(ag) -> Dict[str, Any]:
    """Convert SDK AccountGroup object to plain dict."""
    return {
        "id": getattr(ag, "id_", ""),
        "name": getattr(ag, "name", ""),
        "account_id": getattr(ag, "account_id", ""),
        "default_group": getattr(ag, "default_group", None),
        "auto_subscribe_alert_level": (
            getattr(ag.auto_subscribe_alert_level, "value", None)
            if hasattr(ag, "auto_subscribe_alert_level") and ag.auto_subscribe_alert_level is not None
            else None
        ),
    }


def _account_group_account_to_dict(aga) -> Dict[str, Any]:
    """Convert SDK AccountGroupAccount object to plain dict."""
    return {
        "id": getattr(aga, "id_", ""),
        "account_group_id": getattr(aga, "account_group_id", ""),
        "account_id": getattr(aga, "account_id", ""),
    }


def _account_group_user_role_to_dict(agur) -> Dict[str, Any]:
    """Convert SDK AccountGroupUserRole object to plain dict."""
    return {
        "id": getattr(agur, "id_", ""),
        "account_group_id": getattr(agur, "account_group_id", ""),
        "user_id": getattr(agur, "user_id", ""),
        "role_id": getattr(agur, "role_id", ""),
        "first_name": getattr(agur, "first_name", ""),
        "last_name": getattr(agur, "last_name", ""),
        "notify_user": getattr(agur, "notify_user", None),
    }


def _account_group_integration_pack_to_dict(agip) -> Dict[str, Any]:
    """Convert SDK AccountGroupIntegrationPack object to plain dict."""
    return {
        "id": getattr(agip, "id_", ""),
        "account_group_id": getattr(agip, "account_group_id", ""),
        "integration_pack_id": getattr(agip, "integration_pack_id", ""),
        "integration_pack_name": getattr(agip, "integration_pack_name", ""),
        "installation_type": (
            getattr(agip.installation_type, "value", None)
            if hasattr(agip, "installation_type") and agip.installation_type is not None
            else None
        ),
    }


def _query_all_account_groups(sdk: Boomi, query_config) -> List[Dict[str, Any]]:
    """Execute an account group query with pagination, return list of dicts."""
    result = sdk.account_group.query_account_group(request_body=query_config)

    items = []
    if hasattr(result, "result") and result.result:
        items.extend([_account_group_to_dict(r) for r in result.result])

    while hasattr(result, "query_token") and result.query_token:
        result = sdk.account_group.query_more_account_group(
            request_body=result.query_token
        )
        if hasattr(result, "result") and result.result:
            items.extend([_account_group_to_dict(r) for r in result.result])

    return items


def _query_all_account_group_accounts(
    sdk: Boomi, account_group_id_filter: str = None
) -> List[Dict[str, Any]]:
    """Execute an AccountGroupAccount query with pagination."""
    if account_group_id_filter:
        expression = AccountGroupAccountSimpleExpression(
            operator=AccountGroupAccountSimpleExpressionOperator.EQUALS,
            property=AccountGroupAccountSimpleExpressionProperty.ACCOUNTGROUPID,
            argument=[account_group_id_filter],
        )
        query_filter = AccountGroupAccountQueryConfigQueryFilter(expression=expression)
        query_config = AccountGroupAccountQueryConfig(query_filter=query_filter)
    else:
        expression = AccountGroupAccountSimpleExpression(
            operator=AccountGroupAccountSimpleExpressionOperator.ISNOTNULL,
            property=AccountGroupAccountSimpleExpressionProperty.ACCOUNTGROUPID,
            argument=[],
        )
        query_filter = AccountGroupAccountQueryConfigQueryFilter(expression=expression)
        query_config = AccountGroupAccountQueryConfig(query_filter=query_filter)

    result = sdk.account_group_account.query_account_group_account(
        request_body=query_config
    )

    items = []
    if hasattr(result, "result") and result.result:
        items.extend([_account_group_account_to_dict(r) for r in result.result])

    while hasattr(result, "query_token") and result.query_token:
        result = sdk.account_group_account.query_more_account_group_account(
            request_body=result.query_token
        )
        if hasattr(result, "result") and result.result:
            items.extend([_account_group_account_to_dict(r) for r in result.result])

    return items


def _query_all_account_group_user_roles(
    sdk: Boomi, account_group_id_filter: str = None
) -> List[Dict[str, Any]]:
    """Execute an AccountGroupUserRole query with pagination."""
    if account_group_id_filter:
        expression = AccountGroupUserRoleSimpleExpression(
            operator=AccountGroupUserRoleSimpleExpressionOperator.EQUALS,
            property=AccountGroupUserRoleSimpleExpressionProperty.ACCOUNTGROUPID,
            argument=[account_group_id_filter],
        )
        query_filter = AccountGroupUserRoleQueryConfigQueryFilter(expression=expression)
        query_config = AccountGroupUserRoleQueryConfig(query_filter=query_filter)
    else:
        expression = AccountGroupUserRoleSimpleExpression(
            operator=AccountGroupUserRoleSimpleExpressionOperator.ISNOTNULL,
            property=AccountGroupUserRoleSimpleExpressionProperty.ACCOUNTGROUPID,
            argument=[],
        )
        query_filter = AccountGroupUserRoleQueryConfigQueryFilter(expression=expression)
        query_config = AccountGroupUserRoleQueryConfig(query_filter=query_filter)

    result = sdk.account_group_user_role.query_account_group_user_role(
        request_body=query_config
    )

    items = []
    if hasattr(result, "result") and result.result:
        items.extend([_account_group_user_role_to_dict(r) for r in result.result])

    while hasattr(result, "query_token") and result.query_token:
        result = sdk.account_group_user_role.query_more_account_group_user_role(
            request_body=result.query_token
        )
        if hasattr(result, "result") and result.result:
            items.extend([_account_group_user_role_to_dict(r) for r in result.result])

    return items


def _query_all_account_group_integration_packs(
    sdk: Boomi, account_group_id_filter: str = None
) -> List[Dict[str, Any]]:
    """Execute an AccountGroupIntegrationPack query with pagination."""
    if account_group_id_filter:
        expression = AccountGroupIntegrationPackExpression(
            id_=account_group_id_filter,
        )
        query_filter = AccountGroupIntegrationPackQueryConfigQueryFilter(
            expression=expression
        )
        query_config = AccountGroupIntegrationPackQueryConfig(
            query_filter=query_filter
        )
    else:
        query_config = AccountGroupIntegrationPackQueryConfig(
            query_filter=AccountGroupIntegrationPackQueryConfigQueryFilter(
                expression=AccountGroupIntegrationPackExpression()
            )
        )

    result = sdk.account_group_integration_pack.query_account_group_integration_pack(
        request_body=query_config
    )

    items = []
    if hasattr(result, "result") and result.result:
        items.extend(
            [_account_group_integration_pack_to_dict(r) for r in result.result]
        )

    while hasattr(result, "query_token") and result.query_token:
        result = sdk.account_group_integration_pack.query_more_account_group_integration_pack(
            request_body=result.query_token
        )
        if hasattr(result, "result") and result.result:
            items.extend(
                [_account_group_integration_pack_to_dict(r) for r in result.result]
            )

    return items


# ============================================================================
# Action Handlers — Account Group CRUD
# ============================================================================

def _action_list(sdk: Boomi, profile: str, **kwargs) -> Dict[str, Any]:
    """List all account groups with optional name filter."""
    name_filter = kwargs.get("name")

    if name_filter:
        expression = AccountGroupSimpleExpression(
            operator=AccountGroupSimpleExpressionOperator.LIKE,
            property=AccountGroupSimpleExpressionProperty.NAME,
            argument=[name_filter],
        )
        query_filter = AccountGroupQueryConfigQueryFilter(expression=expression)
        query_config = AccountGroupQueryConfig(query_filter=query_filter)
    else:
        expression = AccountGroupSimpleExpression(
            operator=AccountGroupSimpleExpressionOperator.ISNOTNULL,
            property=AccountGroupSimpleExpressionProperty.NAME,
            argument=[],
        )
        query_filter = AccountGroupQueryConfigQueryFilter(expression=expression)
        query_config = AccountGroupQueryConfig(query_filter=query_filter)

    groups = _query_all_account_groups(sdk, query_config)

    return {
        "_success": True,
        "account_groups": groups,
        "total_count": len(groups),
    }


def _action_get(sdk: Boomi, profile: str, **kwargs) -> Dict[str, Any]:
    """Get a specific account group by ID."""
    resource_id = kwargs.get("resource_id")
    if not resource_id:
        return {
            "_success": False,
            "error": "resource_id is required for 'get' action.",
        }

    result = sdk.account_group.get_account_group(id_=resource_id)
    return {
        "_success": True,
        "account_group": _account_group_to_dict(result),
    }


def _action_create(sdk: Boomi, profile: str, **kwargs) -> Dict[str, Any]:
    """Create a new account group."""
    name = kwargs.get("name")
    if not name:
        return {
            "_success": False,
            "error": "name is required for 'create' action.",
        }

    ag_kwargs = {"name": name}

    auto_subscribe_alert_level = kwargs.get("auto_subscribe_alert_level")
    if auto_subscribe_alert_level:
        ag_kwargs["auto_subscribe_alert_level"] = auto_subscribe_alert_level

    ag = AccountGroup(**ag_kwargs)
    result = sdk.account_group.create_account_group(request_body=ag)
    return {
        "_success": True,
        "account_group": _account_group_to_dict(result),
    }


def _action_update(sdk: Boomi, profile: str, **kwargs) -> Dict[str, Any]:
    """Update an existing account group."""
    resource_id = kwargs.get("resource_id")
    if not resource_id:
        return {
            "_success": False,
            "error": "resource_id is required for 'update' action.",
        }

    ag_kwargs = {"id_": resource_id}

    name = kwargs.get("name")
    if name:
        ag_kwargs["name"] = name

    auto_subscribe_alert_level = kwargs.get("auto_subscribe_alert_level")
    if auto_subscribe_alert_level:
        ag_kwargs["auto_subscribe_alert_level"] = auto_subscribe_alert_level

    ag = AccountGroup(**ag_kwargs)
    result = sdk.account_group.update_account_group(
        id_=resource_id, request_body=ag
    )
    return {
        "_success": True,
        "account_group": _account_group_to_dict(result),
    }


def _action_delete(sdk: Boomi, profile: str, **kwargs) -> Dict[str, Any]:
    """Delete an account group — not supported by the Boomi API."""
    return {
        "_success": False,
        "error": (
            "The Boomi API does not provide a DELETE operation for Account Groups. "
            "Account groups can only be deleted through the Boomi platform UI."
        ),
    }


# ============================================================================
# Action Handlers — Account Associations
# ============================================================================

def _action_list_accounts(sdk: Boomi, profile: str, **kwargs) -> Dict[str, Any]:
    """List accounts in an account group."""
    account_group_id = kwargs.get("account_group_id")

    items = _query_all_account_group_accounts(
        sdk, account_group_id_filter=account_group_id
    )

    return {
        "_success": True,
        "accounts": items,
        "total_count": len(items),
    }


def _action_add_account(sdk: Boomi, profile: str, **kwargs) -> Dict[str, Any]:
    """Add an account to an account group."""
    account_group_id = kwargs.get("account_group_id")
    account_id = kwargs.get("account_id")

    if not account_group_id or not account_id:
        return {
            "_success": False,
            "error": "Both account_group_id and account_id are required for 'add_account' action.",
        }

    aga = AccountGroupAccount(
        account_group_id=account_group_id,
        account_id=account_id,
    )
    result = sdk.account_group_account.create_account_group_account(request_body=aga)
    return {
        "_success": True,
        "account_association": _account_group_account_to_dict(result),
    }


def _action_remove_account(sdk: Boomi, profile: str, **kwargs) -> Dict[str, Any]:
    """Remove an account from an account group."""
    resource_id = kwargs.get("resource_id")

    if not resource_id:
        account_group_id = kwargs.get("account_group_id")
        account_id = kwargs.get("account_id")

        if not account_group_id or not account_id:
            return {
                "_success": False,
                "error": (
                    "Provide resource_id or both account_group_id and account_id "
                    "for 'remove_account' action."
                ),
            }

        # Query to find the association
        results = _query_all_account_group_accounts(
            sdk, account_group_id_filter=account_group_id
        )
        match = [r for r in results if r.get("account_id") == account_id]
        if not match:
            return {
                "_success": False,
                "error": f"No association found for account_group_id={account_group_id} and account_id={account_id}.",
            }
        resource_id = match[0]["id"]

    sdk.account_group_account.delete_account_group_account(id_=resource_id)
    return {
        "_success": True,
        "deleted_id": resource_id,
        "message": "Account removed from account group.",
    }


# ============================================================================
# Action Handlers — User Role Associations
# ============================================================================

def _action_list_user_roles(sdk: Boomi, profile: str, **kwargs) -> Dict[str, Any]:
    """List user roles in an account group."""
    account_group_id = kwargs.get("account_group_id")

    items = _query_all_account_group_user_roles(
        sdk, account_group_id_filter=account_group_id
    )

    return {
        "_success": True,
        "user_roles": items,
        "total_count": len(items),
    }


def _action_assign_user_role(sdk: Boomi, profile: str, **kwargs) -> Dict[str, Any]:
    """Assign a user role in an account group."""
    account_group_id = kwargs.get("account_group_id")
    user_id = kwargs.get("user_id")
    role_id = kwargs.get("role_id")

    if not account_group_id or not user_id or not role_id:
        return {
            "_success": False,
            "error": "account_group_id, user_id, and role_id are all required for 'assign_user_role' action.",
        }

    agur_kwargs = {
        "account_group_id": account_group_id,
        "user_id": user_id,
        "role_id": role_id,
    }

    notify_user = kwargs.get("notify_user")
    if notify_user is not None:
        agur_kwargs["notify_user"] = notify_user

    agur = AccountGroupUserRole(**agur_kwargs)
    result = sdk.account_group_user_role.create_account_group_user_role(
        request_body=agur
    )
    return {
        "_success": True,
        "user_role": _account_group_user_role_to_dict(result),
    }


def _action_remove_user_role(sdk: Boomi, profile: str, **kwargs) -> Dict[str, Any]:
    """Remove a user role from an account group."""
    resource_id = kwargs.get("resource_id")

    if not resource_id:
        account_group_id = kwargs.get("account_group_id")
        user_id = kwargs.get("user_id")
        role_id = kwargs.get("role_id")

        if not account_group_id or not user_id or not role_id:
            return {
                "_success": False,
                "error": (
                    "Provide resource_id or all of account_group_id, user_id, and role_id "
                    "for 'remove_user_role' action."
                ),
            }

        # Query to find the association
        results = _query_all_account_group_user_roles(
            sdk, account_group_id_filter=account_group_id
        )
        match = [
            r
            for r in results
            if r.get("user_id") == user_id and r.get("role_id") == role_id
        ]
        if not match:
            return {
                "_success": False,
                "error": (
                    f"No user role association found for account_group_id={account_group_id}, "
                    f"user_id={user_id}, role_id={role_id}."
                ),
            }
        resource_id = match[0]["id"]

    sdk.account_group_user_role.delete_account_group_user_role(id_=resource_id)
    return {
        "_success": True,
        "deleted_id": resource_id,
        "message": "User role removed from account group.",
    }


# ============================================================================
# Action Handlers — Integration Pack Sharing
# ============================================================================

def _action_list_integration_packs(
    sdk: Boomi, profile: str, **kwargs
) -> Dict[str, Any]:
    """List integration packs shared with an account group."""
    account_group_id = kwargs.get("account_group_id")

    items = _query_all_account_group_integration_packs(
        sdk, account_group_id_filter=account_group_id
    )

    return {
        "_success": True,
        "integration_packs": items,
        "total_count": len(items),
    }


def _action_share_integration_pack(
    sdk: Boomi, profile: str, **kwargs
) -> Dict[str, Any]:
    """Share an integration pack with an account group."""
    account_group_id = kwargs.get("account_group_id")
    integration_pack_id = kwargs.get("integration_pack_id")

    if not account_group_id or not integration_pack_id:
        return {
            "_success": False,
            "error": (
                "Both account_group_id and integration_pack_id are required "
                "for 'share_integration_pack' action."
            ),
        }

    agip_kwargs = {
        "account_group_id": account_group_id,
        "integration_pack_id": integration_pack_id,
    }

    installation_type = kwargs.get("installation_type")
    if installation_type:
        agip_kwargs["installation_type"] = installation_type

    agip = AccountGroupIntegrationPack(**agip_kwargs)
    result = sdk.account_group_integration_pack.create_account_group_integration_pack(
        request_body=agip
    )
    return {
        "_success": True,
        "integration_pack": _account_group_integration_pack_to_dict(result),
    }


def _action_unshare_integration_pack(
    sdk: Boomi, profile: str, **kwargs
) -> Dict[str, Any]:
    """Remove an integration pack from an account group."""
    resource_id = kwargs.get("resource_id")

    if not resource_id:
        account_group_id = kwargs.get("account_group_id")
        integration_pack_id = kwargs.get("integration_pack_id")

        if not account_group_id or not integration_pack_id:
            return {
                "_success": False,
                "error": (
                    "Provide resource_id or both account_group_id and integration_pack_id "
                    "for 'unshare_integration_pack' action."
                ),
            }

        # Query to find the association
        results = _query_all_account_group_integration_packs(
            sdk, account_group_id_filter=account_group_id
        )
        match = [
            r
            for r in results
            if r.get("integration_pack_id") == integration_pack_id
        ]
        if not match:
            return {
                "_success": False,
                "error": (
                    f"No integration pack association found for "
                    f"account_group_id={account_group_id}, "
                    f"integration_pack_id={integration_pack_id}."
                ),
            }
        resource_id = match[0]["id"]

    sdk.account_group_integration_pack.delete_account_group_integration_pack(
        id_=resource_id
    )
    return {
        "_success": True,
        "deleted_id": resource_id,
        "message": "Integration pack removed from account group.",
    }


# ============================================================================
# Action Router
# ============================================================================

def manage_account_groups_action(
    sdk: Boomi,
    profile: str,
    action: str,
    config_data: Optional[Dict[str, Any]] = None,
    **kwargs,
) -> Dict[str, Any]:
    """
    Route to the appropriate account group action handler.

    Args:
        sdk: Authenticated Boomi SDK client
        profile: Profile name
        action: One of: list, get, create, update, delete,
                list_accounts, add_account, remove_account,
                list_user_roles, assign_user_role, remove_user_role,
                list_integration_packs, share_integration_pack, unshare_integration_pack
        config_data: Action-specific configuration dict
        **kwargs: Additional parameters (resource_id, etc.)
    """
    if config_data is None:
        config_data = {}

    merged = {**config_data, **kwargs}

    actions = {
        "list": _action_list,
        "get": _action_get,
        "create": _action_create,
        "update": _action_update,
        "delete": _action_delete,
        "list_accounts": _action_list_accounts,
        "add_account": _action_add_account,
        "remove_account": _action_remove_account,
        "list_user_roles": _action_list_user_roles,
        "assign_user_role": _action_assign_user_role,
        "remove_user_role": _action_remove_user_role,
        "list_integration_packs": _action_list_integration_packs,
        "share_integration_pack": _action_share_integration_pack,
        "unshare_integration_pack": _action_unshare_integration_pack,
    }

    handler = actions.get(action)
    if not handler:
        return {
            "_success": False,
            "error": f"Unknown action: {action}",
            "valid_actions": list(actions.keys()),
        }

    try:
        return handler(sdk, profile, **merged)
    except ApiError as e:
        return {
            "_success": False,
            "error": f"Action '{action}' failed: {_extract_api_error_msg(e)}",
            "exception_type": "ApiError",
        }
    except Exception as e:
        return {
            "_success": False,
            "error": f"Action '{action}' failed: {str(e)}",
            "exception_type": type(e).__name__,
        }
