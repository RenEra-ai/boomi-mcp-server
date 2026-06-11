"""
Deployment Package & Deploy Management for Boomi Platform.

Provides 21 deployment management actions:
- list_packages: List packages with optional component_id filter
- get_package: Get single package details
- create_package: Create versioned package from component
- delete_package: Delete package (fails if deployed)
- get_package_manifest: Get package manifest (included components and metadata)
- deploy: Deploy package to environment
- undeploy: Remove deployment from environment
- list_deployments: List deployments with optional filters
- get_deployment: Get single deployment details
- list_component_atom_attachments: List component-atom attachments (DEPRECATED — env-enabled accounts: use *_environment actions)
- attach_component_atom: Attach component to a runtime (DEPRECATED — env-enabled accounts: use *_environment actions)
- detach_component_atom: Detach component from a runtime (DEPRECATED — env-enabled accounts: use *_environment actions)
- list_component_environment_attachments: List component-environment attachments
- attach_component_environment: Attach component to an environment
- detach_component_environment: Detach component from an environment
- list_process_atom_attachments: List process-atom attachments (DEPRECATED — env-enabled accounts: use *_environment actions)
- attach_process_atom: Attach process to a runtime (DEPRECATED — env-enabled accounts: use *_environment actions)
- detach_process_atom: Detach process from a runtime (DEPRECATED — env-enabled accounts: use *_environment actions)
- list_process_environment_attachments: List process-environment attachments
- attach_process_environment: Attach process to an environment
- detach_process_environment: Detach process from an environment
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
    ComponentAtomAttachment,
    ComponentAtomAttachmentQueryConfig,
    ComponentAtomAttachmentQueryConfigQueryFilter,
    ComponentAtomAttachmentSimpleExpression,
    ComponentAtomAttachmentSimpleExpressionOperator,
    ComponentAtomAttachmentSimpleExpressionProperty,
    ComponentEnvironmentAttachment,
    ComponentEnvironmentAttachmentQueryConfig,
    ComponentEnvironmentAttachmentQueryConfigQueryFilter,
    ComponentEnvironmentAttachmentSimpleExpression,
    ComponentEnvironmentAttachmentSimpleExpressionOperator,
    ComponentEnvironmentAttachmentSimpleExpressionProperty,
    ProcessAtomAttachment,
    ProcessAtomAttachmentQueryConfig,
    ProcessAtomAttachmentQueryConfigQueryFilter,
    ProcessAtomAttachmentSimpleExpression,
    ProcessAtomAttachmentSimpleExpressionOperator,
    ProcessAtomAttachmentSimpleExpressionProperty,
    ProcessEnvironmentAttachment,
    ProcessEnvironmentAttachmentQueryConfig,
    ProcessEnvironmentAttachmentQueryConfigQueryFilter,
    ProcessEnvironmentAttachmentSimpleExpression,
    ProcessEnvironmentAttachmentSimpleExpressionOperator,
    ProcessEnvironmentAttachmentSimpleExpressionProperty,
)

from .deployment_utils import (
    ENVIRONMENT_ACCOUNT_ATOM_ATTACHMENT_UNSUPPORTED,
    DEPRECATED_ATOM_ATTACHMENT_ACTION,
    atom_attachment_deprecation_metadata,
    environment_account_remediation,
    is_environment_account_signal,
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
    # Ensure notes is always present for consistent dict shape
    if 'notes' not in result:
        result['notes'] = ''
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


def _attachment_to_dict(att) -> Dict[str, Any]:
    """Convert an SDK attachment object to plain dict.

    Works for ComponentAtomAttachment, ComponentEnvironmentAttachment,
    ProcessAtomAttachment, and ProcessEnvironmentAttachment.
    """
    result = {}
    for sdk_attr in (
        'id_', 'component_id', 'component_type', 'atom_id',
        'environment_id', 'process_id',
    ):
        val = getattr(att, sdk_attr, None)
        if val is not None:
            # Map id_ -> id in output for readability
            dict_key = 'id' if sdk_attr == 'id_' else sdk_attr
            result[dict_key] = _enum_str(val) if hasattr(val, 'value') else val
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


def _with_atom_deprecation(action: str, payload: Dict[str, Any]) -> Dict[str, Any]:
    """Merge deprecation metadata into an atom-attachment action response (no-op otherwise)."""
    meta = atom_attachment_deprecation_metadata(action)
    if meta:
        payload.update(meta)
    return payload


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
    component_type = kwargs.get("component_type")
    package_version = kwargs.get("package_version")

    missing = []
    if not component_id:
        missing.append('component_id')
    if not component_type:
        missing.append('component_type')
    if not package_version:
        missing.append('package_version')
    if missing:
        return {
            "_success": False,
            "error": f"Missing required config fields for 'create_package': {', '.join(missing)}",
            "hint": "Valid component_type values: process, certificate, customlibrary, flowservice, processroute, tpgroup, webservice",
            "required_fields": {
                "component_id": "Boomi component ID",
                "component_type": "process | certificate | customlibrary | flowservice | processroute | tpgroup | webservice",
                "package_version": "Semantic version string (e.g., 1.0.0)",
            },
        }

    pkg_kwargs = {
        "component_id": component_id,
        "component_type": component_type,
        "package_version": package_version,
    }
    for key in ("notes", "branch_name"):
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


def _action_get_package_manifest(sdk: Boomi, profile: str, **kwargs) -> Dict[str, Any]:
    package_id = kwargs.get("package_id")
    if not package_id:
        return {"_success": False, "error": "package_id is required for 'get_package_manifest' action"}

    result = sdk.packaged_component_manifest.get_packaged_component_manifest(package_id=package_id)

    manifest = {}
    for attr in dir(result):
        if not attr.startswith('_'):
            val = getattr(result, attr, None)
            if val is not None and not callable(val):
                if hasattr(val, 'value'):
                    manifest[attr] = str(val.value)
                elif isinstance(val, list):
                    manifest[attr] = []
                    for item in val:
                        if hasattr(item, '__dict__'):
                            entry = {}
                            for sub_attr in dir(item):
                                if not sub_attr.startswith('_'):
                                    sub_val = getattr(item, sub_attr, None)
                                    if sub_val is not None and not callable(sub_val):
                                        dict_key = 'id' if sub_attr == 'id_' else sub_attr
                                        entry[dict_key] = _enum_str(sub_val) if hasattr(sub_val, 'value') else sub_val
                            manifest[attr].append(entry)
                        else:
                            manifest[attr].append(item)
                else:
                    manifest[attr] = val

    return {"_success": True, "manifest": manifest}


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


def _lookup_deployment_id(sdk: Boomi, package_id: str, environment_id: str, *, active_only: bool = False) -> Optional[str]:
    """Look up a deployment_id by querying DeployedPackage with package_id + environment_id.

    When multiple deployments match, prefers active deployments and selects the
    most recently deployed one (by deployed_date).  When *active_only* is True,
    only active deployments are considered (returns None if none are active).
    """
    expressions = [
        DeployedPackageSimpleExpression(
            operator=DeployedPackageSimpleExpressionOperator.EQUALS,
            property=DeployedPackageSimpleExpressionProperty.PACKAGEID,
            argument=[package_id],
        ),
        DeployedPackageSimpleExpression(
            operator=DeployedPackageSimpleExpressionOperator.EQUALS,
            property=DeployedPackageSimpleExpressionProperty.ENVIRONMENTID,
            argument=[environment_id],
        ),
    ]
    combined = DeployedPackageGroupingExpression(
        operator=DeployedPackageGroupingExpressionOperator.AND,
        nested_expression=expressions,
    )
    query_filter = DeployedPackageQueryConfigQueryFilter(expression=combined)
    query_config = DeployedPackageQueryConfig(query_filter=query_filter)
    result = sdk.deployed_package.query_deployed_package(request_body=query_config)

    # Collect all pages of results
    all_deps = []
    if hasattr(result, 'result') and result.result:
        items = result.result if isinstance(result.result, list) else [result.result]
        all_deps.extend(items)
    while hasattr(result, 'query_token') and result.query_token:
        result = sdk.deployed_package.query_more_deployed_package(
            request_body=result.query_token
        )
        if hasattr(result, 'result') and result.result:
            items = result.result if isinstance(result.result, list) else [result.result]
            all_deps.extend(items)

    if not all_deps:
        return None

    def _dep_id(dep):
        return getattr(dep, 'deployment_id', None) or getattr(dep, 'id_', None) or getattr(dep, 'id', None)

    def _is_active(dep):
        active_raw = getattr(dep, 'active', None)
        if isinstance(active_raw, str):
            return active_raw.lower() in ('true', '1', 'yes')
        return bool(active_raw) if active_raw is not None else False

    if active_only:
        all_deps = [d for d in all_deps if _is_active(d)]
        if not all_deps:
            return None

    # Prefer active deployments; among ties sort by deployed_date descending
    def _sort_key(dep):
        deployed_date = getattr(dep, 'deployed_date', '') or ''
        return (_is_active(dep), deployed_date)

    all_deps.sort(key=_sort_key, reverse=True)
    return _dep_id(all_deps[0])


def _action_undeploy(sdk: Boomi, profile: str, **kwargs) -> Dict[str, Any]:
    deployment_id = kwargs.get("deployment_id")
    package_id = kwargs.get("package_id")
    environment_id = kwargs.get("environment_id")

    # Resolve deployment_id: accept directly, or look up via package_id + environment_id
    if not deployment_id:
        if package_id and environment_id:
            deployment_id = _lookup_deployment_id(sdk, package_id, environment_id, active_only=True)
            if not deployment_id:
                return {
                    "_success": False,
                    "error": f"No active deployment found for package_id={package_id} in environment_id={environment_id}",
                }
        elif package_id or environment_id:
            return {
                "_success": False,
                "error": "undeploy requires either deployment_id, or both package_id and environment_id",
            }
        else:
            return {
                "_success": False,
                "error": "undeploy requires either deployment_id, or both package_id and environment_id",
            }

    try:
        deployment = sdk.deployed_package.get_deployed_package(id_=deployment_id)
        dep_dict = _deployment_to_dict(deployment)
    except Exception:
        dep_dict = {"deployment_id": deployment_id}

    sdk.deployed_package.delete_deployed_package(id_=deployment_id)

    return {
        "_success": True,
        "undeployed": dep_dict,
        "message": "Package successfully undeployed from environment.",
    }


def _action_list_deployments(sdk: Boomi, profile: str, **kwargs) -> Dict[str, Any]:
    environment_id = kwargs.get("environment_id")
    filter_package_id = kwargs.get("package_id")
    component_id = kwargs.get("component_id")
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
    if component_id:
        expressions.append(DeployedPackageSimpleExpression(
            operator=DeployedPackageSimpleExpressionOperator.EQUALS,
            property=DeployedPackageSimpleExpressionProperty.COMPONENTID,
            argument=[component_id],
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
    deployment_id = kwargs.get("deployment_id")
    package_id = kwargs.get("package_id")
    environment_id = kwargs.get("environment_id")

    # Resolve deployment_id: accept directly, or look up via package_id + environment_id
    if not deployment_id:
        if package_id and environment_id:
            deployment_id = _lookup_deployment_id(sdk, package_id, environment_id)
            if not deployment_id:
                return {
                    "_success": False,
                    "error": f"No deployment found for package_id={package_id} in environment_id={environment_id}",
                }
        elif package_id or environment_id:
            return {
                "_success": False,
                "error": "get_deployment requires either deployment_id, or both package_id and environment_id",
            }
        else:
            return {
                "_success": False,
                "error": "get_deployment requires either deployment_id, or both package_id and environment_id",
            }

    result = sdk.deployed_package.get_deployed_package(id_=deployment_id)
    return {"_success": True, "deployment": _deployment_to_dict(result)}


# ============================================================================
# Component-Atom Attachment Actions
# ============================================================================

def _action_list_component_atom_attachments(sdk: Boomi, profile: str, **kwargs) -> Dict[str, Any]:
    """List component-atom attachments. Optional filter by component_id or atom_id."""
    component_id = kwargs.get("component_id")
    atom_id = kwargs.get("atom_id")

    if component_id:
        expression = ComponentAtomAttachmentSimpleExpression(
            operator=ComponentAtomAttachmentSimpleExpressionOperator.EQUALS,
            property=ComponentAtomAttachmentSimpleExpressionProperty.COMPONENTID,
            argument=[component_id],
        )
    elif atom_id:
        expression = ComponentAtomAttachmentSimpleExpression(
            operator=ComponentAtomAttachmentSimpleExpressionOperator.EQUALS,
            property=ComponentAtomAttachmentSimpleExpressionProperty.ATOMID,
            argument=[atom_id],
        )
    else:
        expression = ComponentAtomAttachmentSimpleExpression(
            operator=ComponentAtomAttachmentSimpleExpressionOperator.ISNOTNULL,
            property=ComponentAtomAttachmentSimpleExpressionProperty.COMPONENTID,
            argument=[],
        )

    query_filter = ComponentAtomAttachmentQueryConfigQueryFilter(expression=expression)
    query_config = ComponentAtomAttachmentQueryConfig(query_filter=query_filter)
    result = sdk.component_atom_attachment.query_component_atom_attachment(
        request_body=query_config
    )

    attachments = []
    if hasattr(result, 'result') and result.result:
        items = result.result if isinstance(result.result, list) else [result.result]
        attachments.extend([_attachment_to_dict(a) for a in items])

    while hasattr(result, 'query_token') and result.query_token:
        result = sdk.component_atom_attachment.query_more_component_atom_attachment(
            request_body=result.query_token
        )
        if hasattr(result, 'result') and result.result:
            items = result.result if isinstance(result.result, list) else [result.result]
            attachments.extend([_attachment_to_dict(a) for a in items])

    response = {"_success": True, "attachments": attachments, "total_count": len(attachments)}
    if not attachments:
        response["warning"] = (
            "Deprecated atom endpoint: environment-enabled accounts return empty results "
            "here even when bindings exist."
        )
        response["hint"] = (
            "Use list_component_environment_attachments and "
            "manage_runtimes(action='list_attachments') to see the real bindings."
        )
    return _with_atom_deprecation("list_component_atom_attachments", response)


def _action_attach_component_atom(sdk: Boomi, profile: str, **kwargs) -> Dict[str, Any]:
    """Attach a component to a runtime (atom). DEPRECATED — rejected on env-enabled accounts."""
    component_id = kwargs.get("component_id")
    atom_id = kwargs.get("atom_id")

    if not component_id:
        return _with_atom_deprecation("attach_component_atom", {
            "_success": False,
            "error": "component_id is required for 'attach_component_atom'",
            "error_code": DEPRECATED_ATOM_ATTACHMENT_ACTION,
        })
    if not atom_id:
        return _with_atom_deprecation("attach_component_atom", {
            "_success": False,
            "error": "atom_id is required for 'attach_component_atom'",
            "error_code": DEPRECATED_ATOM_ATTACHMENT_ACTION,
        })

    attachment = ComponentAtomAttachment(component_id=component_id, atom_id=atom_id)
    result = sdk.component_atom_attachment.create_component_atom_attachment(
        request_body=attachment
    )

    return _with_atom_deprecation(
        "attach_component_atom",
        {"_success": True, "attachment": _attachment_to_dict(result)},
    )


def _action_detach_component_atom(sdk: Boomi, profile: str, **kwargs) -> Dict[str, Any]:
    """Detach a component from a runtime by attachment resource_id. DEPRECATED."""
    resource_id = kwargs.get("resource_id")
    if not resource_id:
        return _with_atom_deprecation("detach_component_atom", {
            "_success": False,
            "error": "resource_id is required for 'detach_component_atom'",
            "error_code": DEPRECATED_ATOM_ATTACHMENT_ACTION,
        })

    sdk.component_atom_attachment.delete_component_atom_attachment(id_=resource_id)

    return _with_atom_deprecation("detach_component_atom", {
        "_success": True,
        "deleted_id": resource_id,
        "message": "Component-atom attachment deleted.",
    })


# ============================================================================
# Component-Environment Attachment Actions
# ============================================================================

def _action_list_component_environment_attachments(sdk: Boomi, profile: str, **kwargs) -> Dict[str, Any]:
    """List component-environment attachments. Optional filter by component_id or environment_id."""
    component_id = kwargs.get("component_id")
    environment_id = kwargs.get("environment_id")

    if component_id:
        expression = ComponentEnvironmentAttachmentSimpleExpression(
            operator=ComponentEnvironmentAttachmentSimpleExpressionOperator.EQUALS,
            property=ComponentEnvironmentAttachmentSimpleExpressionProperty.COMPONENTID,
            argument=[component_id],
        )
    elif environment_id:
        expression = ComponentEnvironmentAttachmentSimpleExpression(
            operator=ComponentEnvironmentAttachmentSimpleExpressionOperator.EQUALS,
            property=ComponentEnvironmentAttachmentSimpleExpressionProperty.ENVIRONMENTID,
            argument=[environment_id],
        )
    else:
        expression = ComponentEnvironmentAttachmentSimpleExpression(
            operator=ComponentEnvironmentAttachmentSimpleExpressionOperator.ISNOTNULL,
            property=ComponentEnvironmentAttachmentSimpleExpressionProperty.COMPONENTID,
            argument=[],
        )

    query_filter = ComponentEnvironmentAttachmentQueryConfigQueryFilter(expression=expression)
    query_config = ComponentEnvironmentAttachmentQueryConfig(query_filter=query_filter)
    result = sdk.component_environment_attachment.query_component_environment_attachment(
        request_body=query_config
    )

    attachments = []
    if hasattr(result, 'result') and result.result:
        items = result.result if isinstance(result.result, list) else [result.result]
        attachments.extend([_attachment_to_dict(a) for a in items])

    while hasattr(result, 'query_token') and result.query_token:
        result = sdk.component_environment_attachment.query_more_component_environment_attachment(
            request_body=result.query_token
        )
        if hasattr(result, 'result') and result.result:
            items = result.result if isinstance(result.result, list) else [result.result]
            attachments.extend([_attachment_to_dict(a) for a in items])

    return {"_success": True, "attachments": attachments, "total_count": len(attachments)}


def _action_attach_component_environment(sdk: Boomi, profile: str, **kwargs) -> Dict[str, Any]:
    """Attach a component to an environment."""
    component_id = kwargs.get("component_id")
    environment_id = kwargs.get("environment_id")

    if not component_id:
        return {"_success": False, "error": "component_id is required for 'attach_component_environment'"}
    if not environment_id:
        return {"_success": False, "error": "environment_id is required for 'attach_component_environment'"}

    attachment = ComponentEnvironmentAttachment(
        component_id=component_id, environment_id=environment_id
    )
    result = sdk.component_environment_attachment.create_component_environment_attachment(
        request_body=attachment
    )

    return {"_success": True, "attachment": _attachment_to_dict(result)}


def _action_detach_component_environment(sdk: Boomi, profile: str, **kwargs) -> Dict[str, Any]:
    """Detach a component from an environment by attachment resource_id."""
    resource_id = kwargs.get("resource_id")
    if not resource_id:
        return {"_success": False, "error": "resource_id is required for 'detach_component_environment'"}

    sdk.component_environment_attachment.delete_component_environment_attachment(
        id_=resource_id
    )

    return {
        "_success": True,
        "deleted_id": resource_id,
        "message": "Component-environment attachment deleted.",
    }


# ============================================================================
# Process-Atom Attachment Actions
# ============================================================================

def _action_list_process_atom_attachments(sdk: Boomi, profile: str, **kwargs) -> Dict[str, Any]:
    """List process-atom attachments. Optional filter by process_id or atom_id."""
    process_id = kwargs.get("process_id")
    atom_id = kwargs.get("atom_id")

    if process_id:
        expression = ProcessAtomAttachmentSimpleExpression(
            operator=ProcessAtomAttachmentSimpleExpressionOperator.EQUALS,
            property=ProcessAtomAttachmentSimpleExpressionProperty.PROCESSID,
            argument=[process_id],
        )
    elif atom_id:
        expression = ProcessAtomAttachmentSimpleExpression(
            operator=ProcessAtomAttachmentSimpleExpressionOperator.EQUALS,
            property=ProcessAtomAttachmentSimpleExpressionProperty.ATOMID,
            argument=[atom_id],
        )
    else:
        expression = ProcessAtomAttachmentSimpleExpression(
            operator=ProcessAtomAttachmentSimpleExpressionOperator.ISNOTNULL,
            property=ProcessAtomAttachmentSimpleExpressionProperty.PROCESSID,
            argument=[],
        )

    query_filter = ProcessAtomAttachmentQueryConfigQueryFilter(expression=expression)
    query_config = ProcessAtomAttachmentQueryConfig(query_filter=query_filter)
    result = sdk.process_atom_attachment.query_process_atom_attachment(
        request_body=query_config
    )

    attachments = []
    if hasattr(result, 'result') and result.result:
        items = result.result if isinstance(result.result, list) else [result.result]
        attachments.extend([_attachment_to_dict(a) for a in items])

    while hasattr(result, 'query_token') and result.query_token:
        result = sdk.process_atom_attachment.query_more_process_atom_attachment(
            request_body=result.query_token
        )
        if hasattr(result, 'result') and result.result:
            items = result.result if isinstance(result.result, list) else [result.result]
            attachments.extend([_attachment_to_dict(a) for a in items])

    response = {"_success": True, "attachments": attachments, "total_count": len(attachments)}
    if not attachments:
        response["warning"] = (
            "Deprecated atom endpoint: environment-enabled accounts return empty results "
            "here even when bindings exist."
        )
        response["hint"] = (
            "Use list_process_environment_attachments and "
            "manage_runtimes(action='list_attachments') to see the real bindings."
        )
    return _with_atom_deprecation("list_process_atom_attachments", response)


def _action_attach_process_atom(sdk: Boomi, profile: str, **kwargs) -> Dict[str, Any]:
    """Attach a process to a runtime (atom). DEPRECATED — rejected on env-enabled accounts."""
    process_id = kwargs.get("process_id")
    atom_id = kwargs.get("atom_id")

    if not process_id:
        return _with_atom_deprecation("attach_process_atom", {
            "_success": False,
            "error": "process_id is required for 'attach_process_atom'",
            "error_code": DEPRECATED_ATOM_ATTACHMENT_ACTION,
        })
    if not atom_id:
        return _with_atom_deprecation("attach_process_atom", {
            "_success": False,
            "error": "atom_id is required for 'attach_process_atom'",
            "error_code": DEPRECATED_ATOM_ATTACHMENT_ACTION,
        })

    attachment = ProcessAtomAttachment(process_id=process_id, atom_id=atom_id)
    result = sdk.process_atom_attachment.create_process_atom_attachment(
        request_body=attachment
    )

    return _with_atom_deprecation(
        "attach_process_atom",
        {"_success": True, "attachment": _attachment_to_dict(result)},
    )


def _action_detach_process_atom(sdk: Boomi, profile: str, **kwargs) -> Dict[str, Any]:
    """Detach a process from a runtime by attachment resource_id. DEPRECATED."""
    resource_id = kwargs.get("resource_id")
    if not resource_id:
        return _with_atom_deprecation("detach_process_atom", {
            "_success": False,
            "error": "resource_id is required for 'detach_process_atom'",
            "error_code": DEPRECATED_ATOM_ATTACHMENT_ACTION,
        })

    sdk.process_atom_attachment.delete_process_atom_attachment(id_=resource_id)

    return _with_atom_deprecation("detach_process_atom", {
        "_success": True,
        "deleted_id": resource_id,
        "message": "Process-atom attachment deleted.",
    })


# ============================================================================
# Process-Environment Attachment Actions
# ============================================================================

def _action_list_process_environment_attachments(sdk: Boomi, profile: str, **kwargs) -> Dict[str, Any]:
    """List process-environment attachments. Optional filter by process_id or environment_id."""
    process_id = kwargs.get("process_id")
    environment_id = kwargs.get("environment_id")

    if process_id:
        expression = ProcessEnvironmentAttachmentSimpleExpression(
            operator=ProcessEnvironmentAttachmentSimpleExpressionOperator.EQUALS,
            property=ProcessEnvironmentAttachmentSimpleExpressionProperty.PROCESSID,
            argument=[process_id],
        )
    elif environment_id:
        expression = ProcessEnvironmentAttachmentSimpleExpression(
            operator=ProcessEnvironmentAttachmentSimpleExpressionOperator.EQUALS,
            property=ProcessEnvironmentAttachmentSimpleExpressionProperty.ENVIRONMENTID,
            argument=[environment_id],
        )
    else:
        expression = ProcessEnvironmentAttachmentSimpleExpression(
            operator=ProcessEnvironmentAttachmentSimpleExpressionOperator.ISNOTNULL,
            property=ProcessEnvironmentAttachmentSimpleExpressionProperty.PROCESSID,
            argument=[],
        )

    query_filter = ProcessEnvironmentAttachmentQueryConfigQueryFilter(expression=expression)
    query_config = ProcessEnvironmentAttachmentQueryConfig(query_filter=query_filter)
    result = sdk.deployment.query_process_environment_attachment(
        request_body=query_config
    )

    attachments = []
    if hasattr(result, 'result') and result.result:
        items = result.result if isinstance(result.result, list) else [result.result]
        attachments.extend([_attachment_to_dict(a) for a in items])

    while hasattr(result, 'query_token') and result.query_token:
        result = sdk.deployment.query_more_process_environment_attachment(
            request_body=result.query_token
        )
        if hasattr(result, 'result') and result.result:
            items = result.result if isinstance(result.result, list) else [result.result]
            attachments.extend([_attachment_to_dict(a) for a in items])

    return {"_success": True, "attachments": attachments, "total_count": len(attachments)}


def _action_attach_process_environment(sdk: Boomi, profile: str, **kwargs) -> Dict[str, Any]:
    """Attach a process to an environment."""
    process_id = kwargs.get("process_id")
    environment_id = kwargs.get("environment_id")

    if not process_id:
        return {"_success": False, "error": "process_id is required for 'attach_process_environment'"}
    if not environment_id:
        return {"_success": False, "error": "environment_id is required for 'attach_process_environment'"}

    attachment = ProcessEnvironmentAttachment(
        process_id=process_id, environment_id=environment_id
    )
    result = sdk.process_environment_attachment.create_process_environment_attachment(
        request_body=attachment
    )

    return {"_success": True, "attachment": _attachment_to_dict(result)}


def _action_detach_process_environment(sdk: Boomi, profile: str, **kwargs) -> Dict[str, Any]:
    """Detach a process from an environment by attachment resource_id."""
    resource_id = kwargs.get("resource_id")
    if not resource_id:
        return {"_success": False, "error": "resource_id is required for 'detach_process_environment'"}

    sdk.deployment.delete_process_environment_attachment(id_=resource_id)

    return {
        "_success": True,
        "deleted_id": resource_id,
        "message": "Process-environment attachment deleted.",
    }


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
        "get_package_manifest": _action_get_package_manifest,
        "deploy": _action_deploy,
        "undeploy": _action_undeploy,
        "list_deployments": _action_list_deployments,
        "get_deployment": _action_get_deployment,
        "list_component_atom_attachments": _action_list_component_atom_attachments,
        "attach_component_atom": _action_attach_component_atom,
        "detach_component_atom": _action_detach_component_atom,
        "list_component_environment_attachments": _action_list_component_environment_attachments,
        "attach_component_environment": _action_attach_component_environment,
        "detach_component_environment": _action_detach_component_environment,
        "list_process_atom_attachments": _action_list_process_atom_attachments,
        "attach_process_atom": _action_attach_process_atom,
        "detach_process_atom": _action_detach_process_atom,
        "list_process_environment_attachments": _action_list_process_environment_attachments,
        "attach_process_environment": _action_attach_process_environment,
        "detach_process_environment": _action_detach_process_environment,
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
        msg = _extract_api_error_msg(e)
        response = {
            "_success": False,
            "error": f"Action '{action}' failed: {msg}",
            "exception_type": "ApiError",
        }
        # Environment-enabled accounts reject the deprecated atom actions wholesale — the
        # attach creates AND the list queries themselves. Fail closed with remediation:
        # unlike orchestrate_deploy (whose env+runtime legs already bound the process), a
        # standalone atom call has no compensating bindings. The "Action '...' failed:
        # <original message>" format is load-bearing — orchestrate_deploy's leg-3 handling
        # re-detects the env signal from this text.
        remediation = environment_account_remediation(action)
        if remediation and is_environment_account_signal(msg):
            response["error_code"] = ENVIRONMENT_ACCOUNT_ATOM_ATTACHMENT_UNSUPPORTED
            response["retryable"] = False
            response["remediation"] = remediation
        return _with_atom_deprecation(action, response)
    except Exception as e:
        return _with_atom_deprecation(action, {
            "_success": False,
            "error": f"Action '{action}' failed: {str(e)}",
            "exception_type": type(e).__name__,
        })
