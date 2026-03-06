"""
Account Administration MCP Tools for Boomi Platform.

Provides 4 account management actions:
- list_roles: List all roles with optional name filter
- manage_role: Create, get, update, or delete a role (via config.operation)
- list_branches: List all component branches
- manage_branch: Create, get, or delete a branch (via config.operation)
"""

from typing import Dict, Any, Optional, List

from boomi import Boomi
from boomi.models import (
    Role,
    RoleQueryConfig,
    RoleQueryConfigQueryFilter,
    RoleSimpleExpression,
    RoleSimpleExpressionOperator,
    RoleSimpleExpressionProperty,
    Privileges,
    Privilege,
    Branch,
    BranchQueryConfig,
    BranchQueryConfigQueryFilter,
    BranchSimpleExpression,
    BranchSimpleExpressionOperator,
)
from boomi.net.transport.api_error import ApiError
from boomi.net.transport.serializer import Serializer
from boomi.net.environment.environment import Environment


# ============================================================================
# Helpers
# ============================================================================

def _role_to_dict(role) -> Dict[str, Any]:
    """Convert SDK Role object to plain dict."""
    result = {
        "id": getattr(role, 'id_', ''),
        "name": getattr(role, 'name', ''),
        "description": getattr(role, 'description', ''),
        "account_id": getattr(role, 'account_id', ''),
    }
    # Include parent_id if present
    parent_id = getattr(role, 'parent_id', None)
    if parent_id:
        result["parent_id"] = parent_id
    # Include privileges summary
    if hasattr(role, 'privileges') and role.privileges:
        if hasattr(role.privileges, 'privilege') and role.privileges.privilege:
            privs = role.privileges.privilege
            if isinstance(privs, list):
                result["privileges"] = [getattr(p, 'name', str(p)) for p in privs]
            else:
                result["privileges"] = []
        else:
            result["privileges"] = []
    return result


def _branch_to_dict(branch) -> Dict[str, Any]:
    """Convert SDK Branch object to plain dict."""
    result = {
        "id": getattr(branch, 'id_', ''),
        "name": getattr(branch, 'name', ''),
        "description": getattr(branch, 'description', ''),
    }
    for field in ('created_by', 'created_date', 'modified_by', 'modified_date',
                  'stage', 'parent_id', 'package_id', 'deployment_id'):
        val = getattr(branch, field, None)
        if val is not None:
            result[field] = str(val)
    ready = getattr(branch, 'ready', None)
    if ready is not None:
        result["ready"] = ready
    deleted = getattr(branch, 'deleted', None)
    if deleted is not None:
        result["deleted"] = deleted
    return result


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


def _query_all_roles_raw(sdk: Boomi) -> List[Dict[str, Any]]:
    """List all roles using raw API call (empty QueryFilter).

    The Role API rejects IS_NOT_NULL and LIKE operators, so we bypass
    the SDK query builder and POST an empty QueryFilter directly.
    """
    import json as json_mod
    svc = sdk.role
    base = svc.base_url or Environment.DEFAULT.url
    url = f"{base.rstrip('/')}/Role/query"

    ser = Serializer(url, [svc.get_access_token(), svc.get_basic_auth()])
    ser = ser.add_header("Accept", "application/json")
    serialized = ser.serialize().set_method("POST")
    serialized = serialized.set_body({"QueryFilter": {}}, "application/json")

    response, status, _ = svc.send_request(serialized)
    if isinstance(response, (bytes, bytearray)):
        response = response.decode("utf-8")
    data = json_mod.loads(response) if isinstance(response, str) else response

    roles = []
    if "result" in data:
        roles.extend([_role_to_dict_raw(r) for r in data["result"]])

    # Handle pagination via queryToken
    while data.get("queryToken"):
        token = data["queryToken"]
        url_more = f"{base.rstrip('/')}/Role/queryMore"
        ser2 = Serializer(url_more, [svc.get_access_token(), svc.get_basic_auth()])
        ser2 = ser2.add_header("Accept", "application/json")
        serialized2 = ser2.serialize().set_method("POST")
        serialized2 = serialized2.set_body(token, "text/plain")
        response2, _, _ = svc.send_request(serialized2)
        if isinstance(response2, (bytes, bytearray)):
            response2 = response2.decode("utf-8")
        data = json_mod.loads(response2) if isinstance(response2, str) else response2
        if "result" in data:
            roles.extend([_role_to_dict_raw(r) for r in data["result"]])

    return roles


def _role_to_dict_raw(role_data: dict) -> Dict[str, Any]:
    """Convert raw JSON role dict to our standard output format."""
    result = {
        "id": role_data.get("id", ""),
        "name": role_data.get("name", ""),
        "description": role_data.get("Description", ""),
        "account_id": role_data.get("accountId", ""),
    }
    parent_id = role_data.get("parentId")
    if parent_id:
        result["parent_id"] = parent_id
    privileges = role_data.get("Privileges", {})
    if privileges:
        privs = privileges.get("Privilege", [])
        if isinstance(privs, dict):
            privs = [privs]
        result["privileges"] = [p.get("name", str(p)) for p in privs]
    return result


def _query_all_roles(sdk: Boomi, expression) -> List[Dict[str, Any]]:
    """Execute a role query with pagination, return list of dicts."""
    query_filter = RoleQueryConfigQueryFilter(expression=expression)
    query_config = RoleQueryConfig(query_filter=query_filter)
    result = sdk.role.query_role(request_body=query_config)

    roles = []
    if hasattr(result, 'result') and result.result:
        roles.extend([_role_to_dict(r) for r in result.result])

    while hasattr(result, 'query_token') and result.query_token:
        result = sdk.role.query_more_role(request_body=result.query_token)
        if hasattr(result, 'result') and result.result:
            roles.extend([_role_to_dict(r) for r in result.result])

    return roles


def _query_all_branches(sdk: Boomi, expression) -> List[Dict[str, Any]]:
    """Execute a branch query with pagination, return list of dicts."""
    query_filter = BranchQueryConfigQueryFilter(expression=expression)
    query_config = BranchQueryConfig(query_filter=query_filter)
    result = sdk.branch.query_branch(request_body=query_config)

    branches = []
    if hasattr(result, 'result') and result.result:
        branches.extend([_branch_to_dict(b) for b in result.result])

    while hasattr(result, 'query_token') and result.query_token:
        result = sdk.branch.query_more_branch(request_body=result.query_token)
        if hasattr(result, 'result') and result.result:
            branches.extend([_branch_to_dict(b) for b in result.result])

    return branches


# ============================================================================
# Action Handlers — Roles
# ============================================================================

def _action_list_roles(sdk: Boomi, profile: str, **kwargs) -> Dict[str, Any]:
    """List all roles with optional exact name filter."""
    name = kwargs.get("name")

    if name:
        # Role API only supports EQUALS on name
        expression = RoleSimpleExpression(
            operator=RoleSimpleExpressionOperator.EQUALS,
            property=RoleSimpleExpressionProperty.NAME,
            argument=[name],
        )
        roles = _query_all_roles(sdk, expression)
    else:
        # Empty QueryFilter via raw API (SDK operators not supported)
        roles = _query_all_roles_raw(sdk)

    return {
        "_success": True,
        "roles": roles,
        "total_count": len(roles),
    }


def _action_manage_role(sdk: Boomi, profile: str, **kwargs) -> Dict[str, Any]:
    """Create, get, update, or delete a role via config.operation."""
    operation = kwargs.get("operation")
    resource_id = kwargs.get("resource_id")

    if not operation:
        return {
            "_success": False,
            "error": "config.operation is required for manage_role. "
                     "Valid operations: create, get, update, delete",
        }

    operation = operation.lower()

    if operation == "create":
        name = kwargs.get("name")
        if not name:
            return {"_success": False, "error": "config.name is required for create operation"}

        description = kwargs.get("description")
        privileges_list = kwargs.get("privileges")

        role_privileges = None
        if privileges_list and isinstance(privileges_list, list):
            privilege_objects = [Privilege(name=p) for p in privileges_list]
            role_privileges = Privileges(privilege=privilege_objects)

        account_id = sdk._base_url_account_id
        new_role = Role(
            name=name,
            description=description,
            privileges=role_privileges,
            account_id=account_id,
        )
        created = sdk.role.create_role(request_body=new_role)

        return {
            "_success": True,
            "role": _role_to_dict(created),
        }

    elif operation == "get":
        if not resource_id:
            return {"_success": False, "error": "resource_id is required for get operation"}
        role = sdk.role.get_role(id_=resource_id)
        return {
            "_success": True,
            "role": _role_to_dict(role),
        }

    elif operation == "update":
        if not resource_id:
            return {"_success": False, "error": "resource_id is required for update operation"}

        name = kwargs.get("name")
        description = kwargs.get("description")
        privileges_list = kwargs.get("privileges")

        if not name and not description and not privileges_list:
            return {
                "_success": False,
                "error": "At least one of name, description, or privileges is required for update",
            }

        # Get current role to preserve fields
        current = sdk.role.get_role(id_=resource_id)

        account_id = sdk._base_url_account_id
        update_role = Role(id_=resource_id, account_id=account_id)
        update_role.name = name if name else getattr(current, 'name', None)
        if description is not None:
            update_role.description = description
        elif hasattr(current, 'description'):
            update_role.description = current.description

        if privileges_list and isinstance(privileges_list, list):
            privilege_objects = [Privilege(name=p) for p in privileges_list]
            update_role.privileges = Privileges(privilege=privilege_objects)
        elif hasattr(current, 'privileges'):
            update_role.privileges = current.privileges

        updated = sdk.role.update_role(id_=resource_id, request_body=update_role)

        return {
            "_success": True,
            "role": _role_to_dict(updated),
        }

    elif operation == "delete":
        if not resource_id:
            return {"_success": False, "error": "resource_id is required for delete operation"}

        # Get role info before deletion
        role = sdk.role.get_role(id_=resource_id)
        role_dict = _role_to_dict(role)

        sdk.role.delete_role(id_=resource_id)

        return {
            "_success": True,
            "deleted_role": role_dict,
            "warning": "Role deletion is permanent and cannot be undone.",
        }

    else:
        return {
            "_success": False,
            "error": f"Unknown operation: {operation}",
            "valid_operations": ["create", "get", "update", "delete"],
        }


# ============================================================================
# Action Handlers — Branches
# ============================================================================

def _action_list_branches(sdk: Boomi, profile: str, **kwargs) -> Dict[str, Any]:
    """List all component branches with optional name filter."""
    name_pattern = kwargs.get("name_pattern")

    if name_pattern:
        expression = BranchSimpleExpression(
            operator=BranchSimpleExpressionOperator.LIKE,
            property="name",
            argument=[name_pattern],
        )
    else:
        expression = BranchSimpleExpression(
            operator=BranchSimpleExpressionOperator.ISNOTNULL,
            property="id",
            argument=[],
        )

    branches = _query_all_branches(sdk, expression)

    return {
        "_success": True,
        "branches": branches,
        "total_count": len(branches),
    }


def _action_manage_branch(sdk: Boomi, profile: str, **kwargs) -> Dict[str, Any]:
    """Create, get, or delete a branch via config.operation."""
    operation = kwargs.get("operation")
    resource_id = kwargs.get("resource_id")

    if not operation:
        return {
            "_success": False,
            "error": "config.operation is required for manage_branch. "
                     "Valid operations: create, get, delete",
        }

    operation = operation.lower()

    if operation == "create":
        name = kwargs.get("name")
        if not name:
            return {"_success": False, "error": "config.name is required for create operation"}

        description = kwargs.get("description")
        parent_id = kwargs.get("parent_id")

        new_branch = Branch(name=name)
        if description:
            new_branch.description = description
        if parent_id:
            new_branch.parent_id = parent_id

        created = sdk.branch.create_branch(request_body=new_branch)

        return {
            "_success": True,
            "branch": _branch_to_dict(created),
        }

    elif operation == "get":
        if not resource_id:
            return {"_success": False, "error": "resource_id is required for get operation"}
        branch = sdk.branch.get_branch(id_=resource_id)
        return {
            "_success": True,
            "branch": _branch_to_dict(branch),
        }

    elif operation == "delete":
        if not resource_id:
            return {"_success": False, "error": "resource_id is required for delete operation"}

        # Get branch info before deletion
        branch = sdk.branch.get_branch(id_=resource_id)
        branch_dict = _branch_to_dict(branch)

        sdk.branch.delete_branch(id_=resource_id)

        return {
            "_success": True,
            "deleted_branch": branch_dict,
            "warning": "Branch deletion is permanent and cannot be undone.",
        }

    else:
        return {
            "_success": False,
            "error": f"Unknown operation: {operation}",
            "valid_operations": ["create", "get", "delete"],
        }


# ============================================================================
# Action Router
# ============================================================================

def manage_account_action(
    sdk: Boomi,
    profile: str,
    action: str,
    config_data: Optional[Dict[str, Any]] = None,
    **kwargs,
) -> Dict[str, Any]:
    """
    Route to the appropriate account administration action handler.

    Args:
        sdk: Authenticated Boomi SDK client
        profile: Profile name
        action: One of: list_roles, manage_role, list_branches, manage_branch
        config_data: Action-specific configuration dict
        **kwargs: Additional parameters (resource_id, etc.)
    """
    if config_data is None:
        config_data = {}

    # Merge config_data into kwargs
    merged = {**config_data, **kwargs}

    actions = {
        "list_roles": _action_list_roles,
        "manage_role": _action_manage_role,
        "list_branches": _action_list_branches,
        "manage_branch": _action_manage_branch,
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
