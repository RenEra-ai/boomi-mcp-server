"""
Deployment Package & Deploy Management for Boomi Platform.

Provides 8 deployment management actions:
- list_packages: List packages with optional component_id filter
- get_package: Get single package details
- create_package: Create versioned package from component
- delete_package: Delete package (fails if deployed)
- deploy: Deploy package to environment
- undeploy: Remove deployment from environment
- list_deployments: List deployments with optional filters
- get_deployment: Get single deployment details
"""

from typing import Dict, Any, Optional

from boomi import Boomi
from boomi.net.transport.api_error import ApiError
from boomi.models import (
    PackagedComponent,
    PackagedComponentQueryConfig,
    PackagedComponentQueryConfigQueryFilter,
    PackagedComponentSimpleExpression,
    PackagedComponentSimpleExpressionOperator,
    DeployedPackage,
    DeployedPackageQueryConfig,
    DeployedPackageQueryConfigQueryFilter,
    DeployedPackageSimpleExpression,
    DeployedPackageSimpleExpressionOperator,
    DeployedPackageSimpleExpressionProperty,
    DeployedPackageGroupingExpression,
    DeployedPackageGroupingExpressionOperator,
)


# ============================================================================
# Helpers
# ============================================================================

def _enum_str(val) -> str:
    """Extract plain string from a value that may be an enum."""
    if hasattr(val, 'value'):
        return str(val.value)
    return str(val) if val else ''


def _parse_bool(val) -> bool:
    """Parse a boolean value, handling string inputs correctly."""
    if isinstance(val, bool):
        return val
    if isinstance(val, str):
        return val.lower() in ("true", "1", "yes")
    return bool(val)


def _package_to_dict(pkg) -> Dict[str, Any]:
    """Convert SDK PackagedComponent to plain dict."""
    result = {}
    for sdk_attr, dict_key in [
        ('package_id', 'package_id'),
        ('component_id', 'component_id'),
        ('component_type', 'component_type'),
        ('package_version', 'package_version'),
        ('component_version', 'component_version'),
        ('branch_name', 'branch_name'),
        ('notes', 'notes'),
        ('created_by', 'created_by'),
        ('created_date', 'created_date'),
    ]:
        val = getattr(pkg, sdk_attr, None)
        if val is not None:
            result[dict_key] = _enum_str(val) if hasattr(val, 'value') else val
    return result


def _deployment_to_dict(dep) -> Dict[str, Any]:
    """Convert SDK DeployedPackage to plain dict."""
    result = {}
    for sdk_attr, dict_key in [
        ('deployment_id', 'deployment_id'),
        ('package_id', 'package_id'),
        ('component_id', 'component_id'),
        ('component_type', 'component_type'),
        ('environment_id', 'environment_id'),
        ('package_version', 'package_version'),
        ('deployed_date', 'deployed_date'),
        ('deployed_by', 'deployed_by'),
        ('listener_status', 'listener_status'),
        ('active', 'active'),
        ('version', 'version'),
        ('notes', 'notes'),
        ('branch_name', 'branch_name'),
        ('current_version', 'current_version'),
    ]:
        val = getattr(dep, sdk_attr, None)
        if val is not None:
            if hasattr(val, 'value'):
                result[dict_key] = str(val.value)
            elif isinstance(val, str) and dict_key == 'active':
                result[dict_key] = val.lower() in ('true', '1', 'yes')
            else:
                result[dict_key] = val
    # Fallback: some responses use id_ for deployment ID
    if 'deployment_id' not in result:
        id_val = getattr(dep, 'id_', None) or getattr(dep, 'id', None)
        if id_val:
            result['deployment_id'] = id_val
    return result


def _extract_api_error_msg(e: ApiError) -> str:
    """Extract user-friendly error message from ApiError."""
    detail = getattr(e, 'error_detail', None)
    if detail:
        return detail
    resp = getattr(e, 'response', None)
    if resp:
        body = getattr(resp, 'body', None)
        if isinstance(body, dict):
            msg = body.get("message", "")
            if msg:
                return msg
    return getattr(e, 'message', '') or str(e)


# ============================================================================
# Package Actions
# ============================================================================

def _action_list_packages(sdk: Boomi, profile: str, **kwargs) -> Dict[str, Any]:
    component_id = kwargs.get("component_id")

    if component_id:
        expression = PackagedComponentSimpleExpression(
            operator=PackagedComponentSimpleExpressionOperator.EQUALS,
            property="componentId",
            argument=[component_id],
        )
    else:
        expression = PackagedComponentSimpleExpression(
            operator=PackagedComponentSimpleExpressionOperator.ISNOTNULL,
            property="componentId",
            argument=[],
        )

    query_filter = PackagedComponentQueryConfigQueryFilter(expression=expression)
    query_config = PackagedComponentQueryConfig(query_filter=query_filter)
    result = sdk.packaged_component.query_packaged_component(request_body=query_config)

    packages = []
    if hasattr(result, 'result') and result.result:
        items = result.result if isinstance(result.result, list) else [result.result]
        packages.extend([_package_to_dict(p) for p in items])

    while hasattr(result, 'query_token') and result.query_token:
        result = sdk.packaged_component.query_more_packaged_component(
            request_body=result.query_token
        )
        if hasattr(result, 'result') and result.result:
            items = result.result if isinstance(result.result, list) else [result.result]
            packages.extend([_package_to_dict(p) for p in items])

    return {"_success": True, "packages": packages, "total_count": len(packages)}


def _action_get_package(sdk: Boomi, profile: str, **kwargs) -> Dict[str, Any]:
    package_id = kwargs.get("package_id")
    if not package_id:
        return {"_success": False, "error": "package_id is required for 'get_package' action"}

    result = sdk.packaged_component.get_packaged_component(id_=package_id)
    return {"_success": True, "package": _package_to_dict(result)}


def _action_create_package(sdk: Boomi, profile: str, **kwargs) -> Dict[str, Any]:
    component_id = kwargs.get("component_id")
    package_version = kwargs.get("package_version")

    if not component_id:
        return {"_success": False, "error": "config.component_id is required for 'create_package'"}
    if not package_version:
        return {"_success": False, "error": "config.package_version is required for 'create_package'"}

    pkg_kwargs = {
        "component_id": component_id,
        "package_version": package_version,
    }
    for key in ("notes", "branch_name", "component_type"):
        val = kwargs.get(key)
        if val:
            pkg_kwargs[key] = val

    packaged_component = PackagedComponent(**pkg_kwargs)
    result = sdk.packaged_component.create_packaged_component(request_body=packaged_component)

    return {
        "_success": True,
        "package": _package_to_dict(result),
        "hint": "Use action='deploy' with the returned package_id to deploy to an environment.",
    }


def _action_delete_package(sdk: Boomi, profile: str, **kwargs) -> Dict[str, Any]:
    package_id = kwargs.get("package_id")
    if not package_id:
        return {"_success": False, "error": "package_id is required for 'delete_package'"}

    try:
        pkg = sdk.packaged_component.get_packaged_component(id_=package_id)
        pkg_dict = _package_to_dict(pkg)
    except Exception:
        pkg_dict = {"package_id": package_id}

    try:
        sdk.packaged_component.delete_packaged_component(id_=package_id)
    except ApiError as e:
        status = getattr(e, 'status', None)
        if status == 409:
            return {
                "_success": False,
                "error": "Package is currently deployed. Undeploy it first using action='undeploy'.",
            }
        raise

    return {
        "_success": True,
        "deleted_package": pkg_dict,
        "warning": "Package deletion is permanent and cannot be undone.",
    }


# ============================================================================
# Deployment Actions
# ============================================================================

def _action_deploy(sdk: Boomi, profile: str, **kwargs) -> Dict[str, Any]:
    package_id = kwargs.get("package_id")
    environment_id = kwargs.get("environment_id")

    if not package_id:
        return {"_success": False, "error": "package_id is required for 'deploy' action"}
    if not environment_id:
        return {"_success": False, "error": "environment_id is required for 'deploy' action"}

    deploy_kwargs = {
        "package_id": package_id,
        "environment_id": environment_id,
    }
    listener_status = kwargs.get("listener_status")
    if listener_status:
        upper = listener_status.upper()
        if upper not in ("RUNNING", "PAUSED"):
            return {
                "_success": False,
                "error": f"Invalid listener_status: '{listener_status}'. Must be 'RUNNING' or 'PAUSED'.",
            }
        deploy_kwargs["listener_status"] = upper

    notes = kwargs.get("notes")
    if notes:
        deploy_kwargs["notes"] = notes

    deployed_package = DeployedPackage(**deploy_kwargs)
    result = sdk.deployed_package.create_deployed_package(request_body=deployed_package)

    return {
        "_success": True,
        "deployment": _deployment_to_dict(result),
    }


def _action_undeploy(sdk: Boomi, profile: str, **kwargs) -> Dict[str, Any]:
    package_id = kwargs.get("package_id")  # actually deployment_id passed via package_id param
    if not package_id:
        return {"_success": False, "error": "package_id (deployment_id) is required for 'undeploy'"}

    try:
        deployment = sdk.deployed_package.get_deployed_package(id_=package_id)
        dep_dict = _deployment_to_dict(deployment)
    except Exception:
        dep_dict = {"id": package_id}

    sdk.deployed_package.delete_deployed_package(id_=package_id)

    return {
        "_success": True,
        "undeployed": dep_dict,
        "message": "Package successfully undeployed from environment.",
    }


def _action_list_deployments(sdk: Boomi, profile: str, **kwargs) -> Dict[str, Any]:
    environment_id = kwargs.get("environment_id")
    filter_package_id = kwargs.get("package_id")
    active_only = _parse_bool(kwargs.get("active_only", False))

    expressions = []

    if environment_id:
        expressions.append(DeployedPackageSimpleExpression(
            operator=DeployedPackageSimpleExpressionOperator.EQUALS,
            property=DeployedPackageSimpleExpressionProperty.ENVIRONMENTID,
            argument=[environment_id],
        ))
    if filter_package_id:
        expressions.append(DeployedPackageSimpleExpression(
            operator=DeployedPackageSimpleExpressionOperator.EQUALS,
            property=DeployedPackageSimpleExpressionProperty.PACKAGEID,
            argument=[filter_package_id],
        ))

    if not expressions:
        expressions.append(DeployedPackageSimpleExpression(
            operator=DeployedPackageSimpleExpressionOperator.ISNOTNULL,
            property=DeployedPackageSimpleExpressionProperty.PACKAGEID,
            argument=[],
        ))

    if len(expressions) == 1:
        query_filter = DeployedPackageQueryConfigQueryFilter(expression=expressions[0])
    else:
        combined = DeployedPackageGroupingExpression(
            operator=DeployedPackageGroupingExpressionOperator.AND,
            nested_expression=expressions,
        )
        query_filter = DeployedPackageQueryConfigQueryFilter(expression=combined)

    query_config = DeployedPackageQueryConfig(query_filter=query_filter)
    result = sdk.deployed_package.query_deployed_package(request_body=query_config)

    deployments = []
    if hasattr(result, 'result') and result.result:
        items = result.result if isinstance(result.result, list) else [result.result]
        deployments.extend([_deployment_to_dict(d) for d in items])

    while hasattr(result, 'query_token') and result.query_token:
        result = sdk.deployed_package.query_more_deployed_package(
            request_body=result.query_token
        )
        if hasattr(result, 'result') and result.result:
            items = result.result if isinstance(result.result, list) else [result.result]
            deployments.extend([_deployment_to_dict(d) for d in items])

    if active_only:
        deployments = [d for d in deployments if d.get("active")]

    return {"_success": True, "deployments": deployments, "total_count": len(deployments)}


def _action_get_deployment(sdk: Boomi, profile: str, **kwargs) -> Dict[str, Any]:
    package_id = kwargs.get("package_id")  # actually deployment_id
    if not package_id:
        return {"_success": False, "error": "package_id (deployment_id) is required for 'get_deployment'"}

    result = sdk.deployed_package.get_deployed_package(id_=package_id)
    return {"_success": True, "deployment": _deployment_to_dict(result)}


# ============================================================================
# Action Router
# ============================================================================

def manage_deployment_action(
    sdk: Boomi,
    profile: str,
    action: str,
    config_data: Optional[Dict[str, Any]] = None,
    **kwargs,
) -> Dict[str, Any]:
    """Route to the appropriate deployment action handler."""
    if config_data is None:
        config_data = {}

    merged = {**config_data, **kwargs}

    actions = {
        "list_packages": _action_list_packages,
        "get_package": _action_get_package,
        "create_package": _action_create_package,
        "delete_package": _action_delete_package,
        "deploy": _action_deploy,
        "undeploy": _action_undeploy,
        "list_deployments": _action_list_deployments,
        "get_deployment": _action_get_deployment,
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
