"""
Runtime Management MCP Tools for Boomi Platform.

Provides 15 runtime management actions:
- list: List runtimes with optional type/status/name filters
- get: Get single runtime details
- update: Update runtime name
- delete: Delete runtime (permanent)
- attach: Attach runtime to environment
- detach: Detach runtime from environment
- list_attachments: List environment-runtime attachments
- restart: Restart runtime
- configure_java: Upgrade or rollback Java version
- create_installer_token: Create installer token for new runtime installation
- cloud_list: List private runtime clouds
- cloud_get: Get private runtime cloud details
- cloud_create: Create private runtime cloud (PROD or TEST)
- cloud_update: Update private runtime cloud settings
- cloud_delete: Delete private runtime cloud
"""

from typing import Dict, Any, Optional, List

from boomi import Boomi
from boomi.net.transport.api_error import ApiError
from boomi.models import (
    Atom,
    AtomQueryConfig,
    AtomQueryConfigQueryFilter,
    AtomSimpleExpression,
    AtomSimpleExpressionOperator,
    AtomSimpleExpressionProperty,
    EnvironmentAtomAttachment,
    EnvironmentAtomAttachmentQueryConfig,
    EnvironmentAtomAttachmentQueryConfigQueryFilter,
    EnvironmentAtomAttachmentSimpleExpression,
    EnvironmentAtomAttachmentSimpleExpressionOperator,
    EnvironmentAtomAttachmentSimpleExpressionProperty,
    RuntimeRestartRequest,
    JavaUpgrade,
    JavaRollback,
    InstallerToken,
    InstallType,
    RuntimeCloud,
    RuntimeCloudQueryConfig,
    RuntimeCloudQueryConfigQueryFilter,
    RuntimeCloudSimpleExpression,
    RuntimeCloudSimpleExpressionOperator,
    RuntimeCloudSimpleExpressionProperty,
)


# ============================================================================
# Constants
# ============================================================================

VALID_RUNTIME_TYPES = {"ATOM", "MOLECULE", "CLOUD"}
VALID_STATUSES = {"ONLINE", "OFFLINE"}
VALID_INSTALL_TYPES = {"ATOM", "MOLECULE", "CLOUD", "BROKER", "GATEWAY"}
VALID_CLASSIFICATIONS = {"PROD", "TEST"}

JAVA_VERSIONS = {
    '8': '1.8.0',
    '11': '11.0',
    '17': '17.0',
    '21': '21.0',
}


# ============================================================================
# Helpers
# ============================================================================

def _enum_str(val) -> str:
    """Extract plain string from a value that may be an enum."""
    if hasattr(val, 'value'):
        return str(val.value)
    return str(val) if val else ''


def _runtime_to_dict(runtime) -> Dict[str, Any]:
    """Convert SDK Atom object to plain dict."""
    result = {
        "id": getattr(runtime, 'id_', ''),
        "name": getattr(runtime, 'name', ''),
        "type": _enum_str(getattr(runtime, 'type_', '')),
        "status": _enum_str(getattr(runtime, 'status', '')),
    }
    # Include optional fields only when present
    for sdk_attr, dict_key in [
        ('host_name', 'hostname'),
        ('current_version', 'version'),
        ('date_installed', 'date_installed'),
        ('date_created', 'date_created'),
        ('created_by', 'created_by'),
        ('description', 'description'),
    ]:
        val = getattr(runtime, sdk_attr, None)
        if val and str(val) != 'N/A':
            result[dict_key] = str(val)

    capabilities = getattr(runtime, 'capabilities', None)
    if capabilities:
        if isinstance(capabilities, list):
            result['capabilities'] = [_enum_str(c) for c in capabilities]
        else:
            result['capabilities'] = [_enum_str(capabilities)]

    return result


def _attachment_to_dict(attachment) -> Dict[str, Any]:
    """Convert SDK EnvironmentAtomAttachment to dict."""
    return {
        "id": getattr(attachment, 'id_', ''),
        "atom_id": getattr(attachment, 'atom_id', ''),
        "environment_id": getattr(attachment, 'environment_id', ''),
    }


def _query_all_runtimes(sdk: Boomi, expression=None) -> List[Dict[str, Any]]:
    """Execute query_atom with pagination, return list of dicts."""
    if expression:
        query_filter = AtomQueryConfigQueryFilter(expression=expression)
        query_config = AtomQueryConfig(query_filter=query_filter)
    else:
        query_config = AtomQueryConfig()

    result = sdk.atom.query_atom(request_body=query_config)

    runtimes = []
    if hasattr(result, 'result') and result.result:
        items = result.result if isinstance(result.result, list) else [result.result]
        runtimes.extend([_runtime_to_dict(r) for r in items])

    while hasattr(result, 'query_token') and result.query_token:
        result = sdk.atom.query_more_atom(request_body=result.query_token)
        if hasattr(result, 'result') and result.result:
            items = result.result if isinstance(result.result, list) else [result.result]
            runtimes.extend([_runtime_to_dict(r) for r in items])

    return runtimes


def _query_all_attachments(sdk: Boomi, expression=None) -> List[Dict[str, Any]]:
    """Execute query_environment_atom_attachment with pagination."""
    if expression:
        query_filter = EnvironmentAtomAttachmentQueryConfigQueryFilter(expression=expression)
        query_config = EnvironmentAtomAttachmentQueryConfig(query_filter=query_filter)
    else:
        # List all: use CONTAINS with empty string on ENVIRONMENTID
        expression = EnvironmentAtomAttachmentSimpleExpression(
            operator=EnvironmentAtomAttachmentSimpleExpressionOperator.CONTAINS,
            property=EnvironmentAtomAttachmentSimpleExpressionProperty.ENVIRONMENTID,
            argument=[""],
        )
        query_filter = EnvironmentAtomAttachmentQueryConfigQueryFilter(expression=expression)
        query_config = EnvironmentAtomAttachmentQueryConfig(query_filter=query_filter)

    result = sdk.environment_atom_attachment.query_environment_atom_attachment(
        request_body=query_config
    )

    attachments = []
    if hasattr(result, 'result') and result.result:
        items = result.result if isinstance(result.result, list) else [result.result]
        attachments.extend([_attachment_to_dict(a) for a in items])

    while hasattr(result, 'query_token') and result.query_token:
        result = sdk.environment_atom_attachment.query_more_environment_atom_attachment(
            request_body=result.query_token
        )
        if hasattr(result, 'result') and result.result:
            items = result.result if isinstance(result.result, list) else [result.result]
            attachments.extend([_attachment_to_dict(a) for a in items])

    return attachments


# ============================================================================
# Action Handlers
# ============================================================================

def _action_list(sdk: Boomi, profile: str, **kwargs) -> Dict[str, Any]:
    """List runtimes with optional type/status/name filters."""
    runtime_type = kwargs.get("runtime_type")
    status = kwargs.get("status")
    name_pattern = kwargs.get("name_pattern")

    if runtime_type:
        upper = runtime_type.upper()
        if upper not in VALID_RUNTIME_TYPES:
            return {
                "_success": False,
                "error": f"Invalid runtime_type: '{runtime_type}'. "
                         f"Valid values: {', '.join(sorted(VALID_RUNTIME_TYPES))}",
            }
        expression = AtomSimpleExpression(
            operator=AtomSimpleExpressionOperator.EQUALS,
            property=AtomSimpleExpressionProperty.TYPE,
            argument=[upper],
        )
    elif status:
        upper = status.upper()
        if upper not in VALID_STATUSES:
            return {
                "_success": False,
                "error": f"Invalid status: '{status}'. "
                         f"Valid values: {', '.join(sorted(VALID_STATUSES))}",
            }
        expression = AtomSimpleExpression(
            operator=AtomSimpleExpressionOperator.EQUALS,
            property=AtomSimpleExpressionProperty.STATUS,
            argument=[upper],
        )
    elif name_pattern:
        # CONTAINS does substring matching; strip any SQL-style % wildcards
        clean_pattern = name_pattern.strip("%")
        expression = AtomSimpleExpression(
            operator=AtomSimpleExpressionOperator.CONTAINS,
            property=AtomSimpleExpressionProperty.NAME,
            argument=[clean_pattern],
        )
    else:
        expression = None

    runtimes = _query_all_runtimes(sdk, expression)

    return {
        "_success": True,
        "runtimes": runtimes,
        "total_count": len(runtimes),
    }


def _action_get(sdk: Boomi, profile: str, **kwargs) -> Dict[str, Any]:
    """Get a single runtime by ID."""
    resource_id = kwargs.get("resource_id")
    if not resource_id:
        return {"_success": False, "error": "resource_id is required for 'get' action"}

    runtime = sdk.atom.get_atom(id_=resource_id)
    return {
        "_success": True,
        "runtime": _runtime_to_dict(runtime),
    }


def _action_update(sdk: Boomi, profile: str, **kwargs) -> Dict[str, Any]:
    """Update runtime name (GET first to preserve required fields)."""
    resource_id = kwargs.get("resource_id")
    name = kwargs.get("name")

    if not resource_id:
        return {"_success": False, "error": "resource_id is required for 'update' action"}
    if not name:
        return {"_success": False, "error": "config.name is required for 'update' action"}

    # GET current atom to preserve required fields
    current_atom = sdk.atom.get_atom(id_=resource_id)

    update_data = {
        'id_': resource_id,
        'name': name,
        'purge_history_days': getattr(current_atom, 'purge_history_days', 30),
        'purge_immediate': getattr(current_atom, 'purge_immediate', False),
        'force_restart_time': getattr(current_atom, 'force_restart_time', 0),
    }
    runtime_update = Atom(**update_data)

    result = sdk.atom.update_atom(id_=resource_id, request_body=runtime_update)

    return {
        "_success": True,
        "runtime": _runtime_to_dict(result),
    }


def _action_delete(sdk: Boomi, profile: str, **kwargs) -> Dict[str, Any]:
    """Delete a runtime (permanent)."""
    resource_id = kwargs.get("resource_id")
    if not resource_id:
        return {"_success": False, "error": "resource_id is required for 'delete' action"}

    # Get info first for the response
    try:
        runtime = sdk.atom.get_atom(id_=resource_id)
        runtime_dict = _runtime_to_dict(runtime)
    except Exception:
        runtime_dict = {"id": resource_id}

    try:
        sdk.atom.delete_atom(id_=resource_id)
    except ApiError as e:
        status = getattr(e, 'status', None)
        if status == 409:
            return {
                "_success": False,
                "error": "Runtime is attached to one or more environments. "
                         "Detach it first using action='detach'.",
            }
        raise

    return {
        "_success": True,
        "deleted_runtime": runtime_dict,
        "warning": "Runtime deletion is permanent and cannot be undone.",
    }


def _action_attach(sdk: Boomi, profile: str, **kwargs) -> Dict[str, Any]:
    """Attach runtime to environment."""
    resource_id = kwargs.get("resource_id")
    environment_id = kwargs.get("environment_id")

    if not resource_id:
        return {"_success": False, "error": "resource_id (runtime_id) is required for 'attach' action"}
    if not environment_id:
        return {"_success": False, "error": "environment_id is required for 'attach' action"}

    attachment_request = EnvironmentAtomAttachment(
        atom_id=resource_id,
        environment_id=environment_id,
    )
    result = sdk.environment_atom_attachment.create_environment_atom_attachment(
        attachment_request
    )

    return {
        "_success": True,
        "attachment": _attachment_to_dict(result),
    }


def _action_detach(sdk: Boomi, profile: str, **kwargs) -> Dict[str, Any]:
    """Detach runtime from environment.

    Supports two calling patterns:
    1. resource_id=attachment_id (direct)
    2. resource_id=runtime_id + environment_id (auto-lookup)
    """
    resource_id = kwargs.get("resource_id")
    environment_id = kwargs.get("environment_id")

    if not resource_id:
        return {"_success": False, "error": "resource_id is required for 'detach' action"}

    attachment_id = resource_id

    # If environment_id is provided, treat resource_id as runtime_id and look up attachment
    if environment_id:
        expression = EnvironmentAtomAttachmentSimpleExpression(
            operator=EnvironmentAtomAttachmentSimpleExpressionOperator.EQUALS,
            property=EnvironmentAtomAttachmentSimpleExpressionProperty.ENVIRONMENTID,
            argument=[environment_id],
        )
        attachments = _query_all_attachments(sdk, expression)

        # Find the attachment matching both runtime_id and environment_id
        matching = [a for a in attachments if a["atom_id"] == resource_id]
        if not matching:
            return {
                "_success": False,
                "error": f"No attachment found for runtime '{resource_id}' "
                         f"in environment '{environment_id}'.",
            }
        attachment_id = matching[0]["id"]

    sdk.environment_atom_attachment.delete_environment_atom_attachment(id_=attachment_id)

    return {
        "_success": True,
        "detached_attachment_id": attachment_id,
        "message": "Runtime successfully detached from environment.",
    }


def _action_list_attachments(sdk: Boomi, profile: str, **kwargs) -> Dict[str, Any]:
    """List environment-runtime attachments with optional filters."""
    environment_id = kwargs.get("environment_id")
    resource_id = kwargs.get("resource_id")

    if environment_id:
        expression = EnvironmentAtomAttachmentSimpleExpression(
            operator=EnvironmentAtomAttachmentSimpleExpressionOperator.EQUALS,
            property=EnvironmentAtomAttachmentSimpleExpressionProperty.ENVIRONMENTID,
            argument=[environment_id],
        )
    elif resource_id:
        expression = EnvironmentAtomAttachmentSimpleExpression(
            operator=EnvironmentAtomAttachmentSimpleExpressionOperator.EQUALS,
            property=EnvironmentAtomAttachmentSimpleExpressionProperty.ATOMID,
            argument=[resource_id],
        )
    else:
        expression = None

    attachments = _query_all_attachments(sdk, expression)

    return {
        "_success": True,
        "attachments": attachments,
        "total_count": len(attachments),
    }


def _action_restart(sdk: Boomi, profile: str, **kwargs) -> Dict[str, Any]:
    """Restart runtime."""
    resource_id = kwargs.get("resource_id")
    if not resource_id:
        return {"_success": False, "error": "resource_id is required for 'restart' action"}

    restart_request = RuntimeRestartRequest(
        runtime_id=resource_id,
        message="Restart initiated via MCP",
    )

    try:
        result = sdk.runtime_restart_request.create_runtime_restart_request(
            request_body=restart_request
        )
    except ApiError as e:
        status = getattr(e, 'status', None)
        if status == 400:
            return {
                "_success": False,
                "error": "Cannot restart this runtime via API. "
                         "Cloud runtimes are managed by Boomi and restart automatically.",
            }
        raise

    # Handle response — may be string, object with message, or dict
    message = "Restart command sent successfully"
    if result:
        if isinstance(result, str):
            message = result if 'RuntimeRestartRequest' not in result else message
        elif hasattr(result, 'message') and result.message:
            message = result.message
        elif isinstance(result, dict) and 'message' in result:
            message = result['message']

    return {
        "_success": True,
        "runtime_id": resource_id,
        "message": message,
    }


def _action_configure_java(sdk: Boomi, profile: str, **kwargs) -> Dict[str, Any]:
    """Upgrade or rollback Java version on a runtime."""
    resource_id = kwargs.get("resource_id")
    java_action = kwargs.get("java_action")
    target_version = kwargs.get("target_version")

    if not resource_id:
        return {"_success": False, "error": "resource_id is required for 'configure_java' action"}
    if not java_action:
        return {
            "_success": False,
            "error": "config.java_action is required ('upgrade' or 'rollback')",
        }

    java_action = java_action.lower()

    if java_action == "upgrade":
        if not target_version:
            return {
                "_success": False,
                "error": "config.target_version is required for upgrade "
                         f"(valid: {', '.join(sorted(JAVA_VERSIONS.keys()))})",
            }
        target_version = str(target_version)
        if target_version not in JAVA_VERSIONS:
            return {
                "_success": False,
                "error": f"Invalid target_version: '{target_version}'. "
                         f"Valid values: {', '.join(sorted(JAVA_VERSIONS.keys()))}",
            }

        sdk_version = JAVA_VERSIONS[target_version]
        upgrade_request = JavaUpgrade(
            atom_id=resource_id,
            target_version=sdk_version,
        )
        result = sdk.java_upgrade.create_java_upgrade(request_body=upgrade_request)

        return {
            "_success": True,
            "runtime_id": resource_id,
            "java_action": "upgrade",
            "target_version": target_version,
            "sdk_version": sdk_version,
            "message": f"Java upgrade to {target_version} ({sdk_version}) initiated",
        }

    elif java_action == "rollback":
        sdk.java_rollback.execute_java_rollback(id_=resource_id)

        return {
            "_success": True,
            "runtime_id": resource_id,
            "java_action": "rollback",
            "message": "Java rollback initiated",
        }

    else:
        return {
            "_success": False,
            "error": f"Invalid java_action: '{java_action}'. Must be 'upgrade' or 'rollback'.",
        }


def _action_create_installer_token(sdk: Boomi, profile: str, **kwargs) -> Dict[str, Any]:
    """Create installer token for new runtime installation."""
    install_type = kwargs.get("install_type", "ATOM").upper()
    duration_minutes = kwargs.get("duration_minutes", 60)
    cloud_id = kwargs.get("cloud_id")

    if install_type not in VALID_INSTALL_TYPES:
        return {
            "_success": False,
            "error": f"Invalid install_type: '{install_type}'. "
                     f"Valid values: {', '.join(sorted(VALID_INSTALL_TYPES))}",
        }

    if install_type == "CLOUD" and not cloud_id:
        return {
            "_success": False,
            "error": "cloud_id is required when install_type is CLOUD. "
                     "Use action='list' with runtime_type='CLOUD' to find cloud IDs.",
        }

    try:
        duration_minutes = int(duration_minutes)
    except (TypeError, ValueError):
        return {"_success": False, "error": f"duration_minutes must be a number, got: {duration_minutes}"}

    if duration_minutes < 30 or duration_minutes > 1440:
        return {
            "_success": False,
            "error": f"duration_minutes must be between 30 and 1440, got: {duration_minutes}",
        }

    install_type_enum = getattr(InstallType, install_type)
    token_kwargs = {
        "install_type": install_type_enum,
        "duration_minutes": duration_minutes,
    }
    if install_type == "CLOUD" and cloud_id:
        token_kwargs["cloud_id"] = cloud_id
    token_request = InstallerToken(**token_kwargs)
    result = sdk.installer_token.create_installer_token(token_request)

    # Parse response — may be object, wrapped in _kwargs, or dict
    token_data = {}
    if hasattr(result, 'token'):
        token_data = {
            "token": getattr(result, 'token', ''),
            "install_type": _enum_str(getattr(result, 'install_type', install_type)),
            "account_id": getattr(result, 'account_id', ''),
            "created": str(getattr(result, 'created', '')),
            "expiration": str(getattr(result, 'expiration', '')),
            "duration_minutes": getattr(result, 'duration_minutes', duration_minutes),
        }
    elif hasattr(result, '_kwargs') and 'InstallerToken' in result._kwargs:
        raw = result._kwargs['InstallerToken']
        token_data = {
            "token": raw.get('@token', raw.get('token', '')),
            "install_type": raw.get('@installType', raw.get('installType', install_type)),
            "account_id": raw.get('@accountId', raw.get('accountId', '')),
            "created": raw.get('@created', raw.get('created', '')),
            "expiration": raw.get('@expiration', raw.get('expiration', '')),
            "duration_minutes": duration_minutes,
        }
    elif isinstance(result, dict):
        token_data = result
    else:
        token_data = {"raw_response": str(result)}

    return {
        "_success": True,
        **token_data,
    }


# ============================================================================
# RuntimeCloud Helpers & Actions
# ============================================================================

def _cloud_to_dict(cloud) -> Dict[str, Any]:
    """Convert SDK RuntimeCloud object to plain dict."""
    result = {
        "id": getattr(cloud, 'id_', ''),
        "name": getattr(cloud, 'name', ''),
        "classification": getattr(cloud, 'classification', ''),
    }
    for sdk_attr, dict_key in [
        ('allow_deployments', 'allow_deployments'),
        ('allow_browsing', 'allow_browsing'),
        ('allow_test_executions', 'allow_test_executions'),
        ('max_attachments_per_account', 'max_attachments_per_account'),
        ('created_by', 'created_by'),
        ('created_date', 'created_date'),
        ('modified_by', 'modified_by'),
        ('modified_date', 'modified_date'),
    ]:
        val = getattr(cloud, sdk_attr, None)
        if val is not None:
            result[dict_key] = val if isinstance(val, (bool, int)) else str(val)
    return result


def _query_all_clouds(sdk: Boomi, expression=None) -> List[Dict[str, Any]]:
    """Execute query_runtime_cloud with pagination, return list of dicts."""
    query_filter = RuntimeCloudQueryConfigQueryFilter(expression=expression)
    query_config = RuntimeCloudQueryConfig(query_filter=query_filter)

    result = sdk.runtime_cloud.query_runtime_cloud(request_body=query_config)

    clouds = []
    if hasattr(result, 'result') and result.result:
        items = result.result if isinstance(result.result, list) else [result.result]
        clouds.extend([_cloud_to_dict(c) for c in items])

    while hasattr(result, 'query_token') and result.query_token:
        result = sdk.runtime_cloud.query_more_runtime_cloud(request_body=result.query_token)
        if hasattr(result, 'result') and result.result:
            items = result.result if isinstance(result.result, list) else [result.result]
            clouds.extend([_cloud_to_dict(c) for c in items])

    return clouds


def _action_cloud_list(sdk: Boomi, profile: str, **kwargs) -> Dict[str, Any]:
    """List private runtime clouds with optional classification filter."""
    classification = kwargs.get("classification")

    if classification:
        upper = classification.upper()
        if upper not in VALID_CLASSIFICATIONS:
            return {
                "_success": False,
                "error": f"Invalid classification: '{classification}'. "
                         f"Valid values: {', '.join(sorted(VALID_CLASSIFICATIONS))}",
            }
        expression = RuntimeCloudSimpleExpression(
            operator=RuntimeCloudSimpleExpressionOperator.EQUALS,
            property=RuntimeCloudSimpleExpressionProperty.CLASSIFICATION,
            argument=[upper],
        )
    else:
        # List all: use CONTAINS with empty string on name
        expression = RuntimeCloudSimpleExpression(
            operator=RuntimeCloudSimpleExpressionOperator.CONTAINS,
            property=RuntimeCloudSimpleExpressionProperty.NAME,
            argument=[""],
        )

    clouds = _query_all_clouds(sdk, expression)

    return {
        "_success": True,
        "clouds": clouds,
        "total_count": len(clouds),
    }


def _action_cloud_get(sdk: Boomi, profile: str, **kwargs) -> Dict[str, Any]:
    """Get a single private runtime cloud by ID."""
    resource_id = kwargs.get("resource_id")
    if not resource_id:
        return {"_success": False, "error": "resource_id is required for 'cloud_get' action"}

    cloud = sdk.runtime_cloud.get_runtime_cloud(id_=resource_id)
    return {
        "_success": True,
        "cloud": _cloud_to_dict(cloud),
    }


def _action_cloud_create(sdk: Boomi, profile: str, **kwargs) -> Dict[str, Any]:
    """Create a private runtime cloud."""
    name = kwargs.get("name")
    classification = kwargs.get("classification")

    if not name:
        return {"_success": False, "error": "config.name is required for 'cloud_create' action"}
    if not classification:
        return {
            "_success": False,
            "error": "config.classification is required for 'cloud_create' action (PROD or TEST)",
        }

    upper = classification.upper()
    if upper not in VALID_CLASSIFICATIONS:
        return {
            "_success": False,
            "error": f"Invalid classification: '{classification}'. "
                     f"Valid values: {', '.join(sorted(VALID_CLASSIFICATIONS))}",
        }

    cloud_kwargs = {"name": name, "classification": upper}
    for key in ("allow_deployments", "allow_browsing", "allow_test_executions"):
        val = kwargs.get(key)
        if val is not None:
            cloud_kwargs[key] = bool(val)
    max_attach = kwargs.get("max_attachments_per_account")
    if max_attach is not None:
        cloud_kwargs["max_attachments_per_account"] = int(max_attach)

    cloud_request = RuntimeCloud(**cloud_kwargs)
    result = sdk.runtime_cloud.create_runtime_cloud(request_body=cloud_request)

    return {
        "_success": True,
        "cloud": _cloud_to_dict(result),
    }


def _action_cloud_update(sdk: Boomi, profile: str, **kwargs) -> Dict[str, Any]:
    """Update a private runtime cloud (name, permissions, max attachments)."""
    resource_id = kwargs.get("resource_id")
    if not resource_id:
        return {"_success": False, "error": "resource_id is required for 'cloud_update' action"}

    # GET current cloud to preserve required fields
    current = sdk.runtime_cloud.get_runtime_cloud(id_=resource_id)

    update_kwargs = {
        "name": kwargs.get("name", getattr(current, 'name', '')),
        "classification": getattr(current, 'classification', 'PROD'),
    }
    for key in ("allow_deployments", "allow_browsing", "allow_test_executions"):
        val = kwargs.get(key)
        if val is not None:
            update_kwargs[key] = bool(val)
        else:
            existing = getattr(current, key, None)
            if existing is not None:
                update_kwargs[key] = existing
    max_attach = kwargs.get("max_attachments_per_account")
    if max_attach is not None:
        update_kwargs["max_attachments_per_account"] = int(max_attach)
    else:
        existing = getattr(current, 'max_attachments_per_account', None)
        if existing is not None:
            update_kwargs["max_attachments_per_account"] = existing

    cloud_update = RuntimeCloud(**update_kwargs)
    result = sdk.runtime_cloud.update_runtime_cloud(id_=resource_id, request_body=cloud_update)

    return {
        "_success": True,
        "cloud": _cloud_to_dict(result),
    }


def _action_cloud_delete(sdk: Boomi, profile: str, **kwargs) -> Dict[str, Any]:
    """Delete a private runtime cloud (permanent)."""
    resource_id = kwargs.get("resource_id")
    if not resource_id:
        return {"_success": False, "error": "resource_id is required for 'cloud_delete' action"}

    # Get info first for the response
    try:
        cloud = sdk.runtime_cloud.get_runtime_cloud(id_=resource_id)
        cloud_dict = _cloud_to_dict(cloud)
    except Exception:
        cloud_dict = {"id": resource_id}

    sdk.runtime_cloud.delete_runtime_cloud(id_=resource_id)

    return {
        "_success": True,
        "deleted_cloud": cloud_dict,
        "warning": "Private runtime cloud deletion is permanent and cannot be undone.",
    }


# ============================================================================
# Action Router
# ============================================================================

def manage_runtimes_action(
    sdk: Boomi,
    profile: str,
    action: str,
    config_data: Optional[Dict[str, Any]] = None,
    **kwargs,
) -> Dict[str, Any]:
    """Route to the appropriate runtime action handler."""
    if config_data is None:
        config_data = {}

    # Merge config_data into kwargs
    merged = {**config_data, **kwargs}

    actions = {
        "list": _action_list,
        "get": _action_get,
        "update": _action_update,
        "delete": _action_delete,
        "attach": _action_attach,
        "detach": _action_detach,
        "list_attachments": _action_list_attachments,
        "restart": _action_restart,
        "configure_java": _action_configure_java,
        "create_installer_token": _action_create_installer_token,
        "cloud_list": _action_cloud_list,
        "cloud_get": _action_cloud_get,
        "cloud_create": _action_cloud_create,
        "cloud_update": _action_cloud_update,
        "cloud_delete": _action_cloud_delete,
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
    except Exception as e:
        return {
            "_success": False,
            "error": f"Action '{action}' failed: {str(e)}",
            "exception_type": type(e).__name__,
        }
