"""
Runtime Management MCP Tools for Boomi Platform.

Provides 45 runtime management actions:
- list: List runtimes with optional type/status/name filters
- get: Get single runtime details
- create: Create a cloud attachment (requires cloud_id from available_clouds or cloud_list)
- update: Update runtime name
- delete: Delete runtime (permanent)
- attach: Attach runtime to environment
- detach: Detach runtime from environment
- list_attachments: List environment-runtime attachments
- restart: Restart runtime
- configure_java: Upgrade or rollback Java version
- create_installer_token: Create installer token for new runtime installation
- available_clouds: List Boomi-managed public clouds (PCS/DCS/MCS) your account can use for cloud attachments
- cloud_list: List private runtime clouds your account owns (requires Cloud Management privilege)
- cloud_get: Get private runtime cloud details
- cloud_create: Create private runtime cloud (PROD or TEST)
- cloud_update: Update private runtime cloud settings
- cloud_delete: Delete private runtime cloud
- diagnostics: Get combined runtime diagnostics (counters, disk space, listener status)
- get_release_schedule: Get the release schedule for a runtime
- create_release_schedule: Create a release schedule for a runtime
- update_release_schedule: Update the release schedule for a runtime
- delete_release_schedule: Delete the release schedule for a runtime
- get_observability_settings: Get observability settings for a runtime (async)
- update_observability_settings: Update observability settings for a runtime
- get_security_policies: Get security policies for a runtime cluster/cloud (async)
- update_security_policies: Update security policies for a runtime cluster/cloud
- get_startup_properties: Get startup properties for a runtime
- reset_counters: Reset counters for a runtime
- purge: Purge processed data from a runtime cloud attachment
- get_connector_versions: Get connector versions for a runtime
- offboard_node: Offboard a node from a runtime cluster/cloud
- refresh_secrets_manager: Refresh secrets manager cache
- get_account_cloud_attachment_properties: Get account cloud attachment properties (async)
- update_account_cloud_attachment_properties: Update account cloud attachment properties
- list_account_cloud_attachment_summaries: Query account cloud attachment summaries
- get_account_cloud_attachment_summary: Get a single account cloud attachment summary
- list_account_cloud_attachment_quotas: Bulk-get account cloud attachment quotas
- get_account_cloud_attachment_quota: Get a single account cloud attachment quota
- create_account_cloud_attachment_quota: Create an account cloud attachment quota
- update_account_cloud_attachment_quota: Update an account cloud attachment quota
- delete_account_cloud_attachment_quota: Delete an account cloud attachment quota
- get_cloud_attachment_properties: Get cloud attachment properties (async)
- update_cloud_attachment_properties: Update cloud attachment properties
- get_account_cloud_attachment_defaults: Get account cloud attachment property defaults (async)
- update_account_cloud_attachment_defaults: Update account cloud attachment property defaults
"""

import re
import time
from typing import Dict, Any, Optional, List

from boomi import Boomi
from boomi.net.transport.api_error import ApiError
from boomi.net.transport.serializer import Serializer
from boomi.net.environment.environment import Environment


def _get_env_url():
    """Get the default Boomi API base URL."""
    return Environment.DEFAULT.url
from boomi.models import (
    Atom,
    AtomQueryConfig,
    AtomQueryConfigQueryFilter,
    AtomSimpleExpression,
    AtomSimpleExpressionOperator,
    AtomSimpleExpressionProperty,
    CloudQueryConfig,
    CloudQueryConfigQueryFilter,
    CloudSimpleExpression,
    CloudSimpleExpressionOperator,
    CloudSimpleExpressionProperty,
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
    ListenerStatusQueryConfig,
    ListenerStatusQueryConfigQueryFilter,
    ListenerStatusSimpleExpression,
    ListenerStatusSimpleExpressionOperator,
    ListenerStatusSimpleExpressionProperty,
    RuntimeReleaseSchedule,
    AtomCounters,
    AtomPurge,
    NodeOffboard,
    SecretsManagerRefreshRequest,
)
from boomi_mcp.utils.async_polling import poll_async_result


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


def _parse_bool(val) -> bool:
    """Parse a boolean value, handling string inputs correctly."""
    if isinstance(val, bool):
        return val
    if isinstance(val, str):
        return val.lower() in ("true", "1", "yes")
    return bool(val)


def _parse_int(val, field_name: str) -> tuple:
    """Parse an integer value with validation. Returns (value, error_string)."""
    try:
        return int(val), None
    except (TypeError, ValueError):
        return None, f"config.{field_name} must be a number, got: {val!r}"


def _match_name_pattern(name: str, pattern: str) -> bool:
    """Match a runtime name against a %-wildcard pattern.

    - Bare text (no %) -> substring match (like %text%)
    - % is the only wildcard char, converted to .* regex
    - Case-sensitive matching
    """
    if not pattern or pattern == "%":
        return True
    if "%" not in pattern:
        return pattern in name
    # Split on %, escape each literal segment, join with .*
    parts = pattern.split("%")
    regex = ".*".join(re.escape(p) for p in parts)
    return bool(re.fullmatch(regex, name))


def _runtime_to_dict(runtime) -> Dict[str, Any]:
    """Convert SDK Atom object to plain dict."""
    result = {
        "id": getattr(runtime, 'id_', ''),
        "name": getattr(runtime, 'name', ''),
        "type": _enum_str(getattr(runtime, 'type_', '')),
        "status": _enum_str(getattr(runtime, 'status', '')),
    }
    # Include optional string fields only when present
    for sdk_attr, dict_key in [
        ('host_name', 'hostname'),
        ('current_version', 'version'),
        ('date_installed', 'date_installed'),
        ('created_by', 'created_by'),
        ('cloud_id', 'cloud_id'),
        ('cloud_name', 'cloud_name'),
        ('cloud_molecule_id', 'cloud_molecule_id'),
        ('cloud_molecule_name', 'cloud_molecule_name'),
        ('cloud_owner_name', 'cloud_owner_name'),
        ('instance_id', 'instance_id'),
        ('status_detail', 'status_detail'),
    ]:
        val = getattr(runtime, sdk_attr, None)
        if val and str(val) != 'N/A':
            result[dict_key] = str(val)

    # Bool/int fields need explicit None check (0/False are valid values)
    for sdk_attr, dict_key in [
        ('is_cloud_attachment', 'is_cloud_attachment'),
        ('purge_history_days', 'purge_history_days'),
        ('purge_immediate', 'purge_immediate'),
        ('force_restart_time', 'force_restart_time'),
    ]:
        val = getattr(runtime, sdk_attr, None)
        if val is not None:
            result[dict_key] = val

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
    """List runtimes with optional filters.

    Filter precedence: runtime_type > status > name > name_pattern.
    runtime_type and status use SDK query expressions.
    name uses SDK EQUALS for exact match.
    name_pattern fetches all runtimes and filters wrapper-side.
    """
    runtime_type = kwargs.get("runtime_type")
    status = kwargs.get("status")
    name = kwargs.get("name")
    name_pattern = kwargs.get("name_pattern")

    expression = None

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
    elif name:
        expression = AtomSimpleExpression(
            operator=AtomSimpleExpressionOperator.EQUALS,
            property=AtomSimpleExpressionProperty.NAME,
            argument=[name],
        )

    runtimes = _query_all_runtimes(sdk, expression)

    # Wrapper-side pattern filtering (only when no higher-precedence filter)
    if name_pattern and not runtime_type and not status and not name:
        runtimes = [
            r for r in runtimes
            if _match_name_pattern(r.get("name", ""), name_pattern)
        ]

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

    if environment_id:
        # Lookup path: treat resource_id as runtime_id, find attachment
        expression = EnvironmentAtomAttachmentSimpleExpression(
            operator=EnvironmentAtomAttachmentSimpleExpressionOperator.EQUALS,
            property=EnvironmentAtomAttachmentSimpleExpressionProperty.ENVIRONMENTID,
            argument=[environment_id],
        )
        attachments = _query_all_attachments(sdk, expression)

        matching = [a for a in attachments if a["atom_id"] == resource_id]
        if not matching:
            return {
                "_success": False,
                "error": f"No attachment found for runtime '{resource_id}' "
                         f"in environment '{environment_id}'.",
            }
        attachment_id = matching[0]["id"]
    try:
        sdk.environment_atom_attachment.delete_environment_atom_attachment(id_=attachment_id)
    except ApiError as e:
        msg = _extract_api_error_msg(e)
        if "Invalid compound id" in msg and not environment_id:
            return {
                "_success": False,
                "error": "environment_id is required when resource_id is a runtime_id. "
                         "Provide environment_id or pass an attachment_id directly.",
            }
        raise

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
        msg = _extract_api_error_msg(e)
        return {
            "_success": False,
            "error": msg,
        }

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
        rollback_request = JavaRollback(atom_id=resource_id)
        sdk.java_rollback.execute_java_rollback(id_=resource_id, request_body=rollback_request)

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


def _action_create(sdk: Boomi, profile: str, **kwargs) -> Dict[str, Any]:
    """Create a cloud attachment on a Boomi-managed or private runtime cloud.

    Requires cloud_id — use available_clouds to find Boomi-managed cloud IDs,
    or cloud_list for private runtime cloud IDs.

    Note: Local atoms cannot be created via API. Use create_installer_token
    to get a token, then install the runtime manually.
    """
    name = kwargs.get("name")
    cloud_id = kwargs.get("cloud_id")

    if not name:
        return {"_success": False, "error": "config.name is required for 'create' action"}

    if not cloud_id:
        return {
            "_success": False,
            "error": "config.cloud_id is required for 'create' action. "
                     "The Atom CREATE API only creates cloud attachments. "
                     "Use action='available_clouds' to find Boomi-managed cloud IDs, "
                     "or action='cloud_list' for private runtime cloud IDs. "
                     "For local atoms, use action='create_installer_token' instead.",
        }

    atom_kwargs = {"name": name, "cloud_id": cloud_id}

    for key in ("purge_history_days", "force_restart_time"):
        val = kwargs.get(key)
        if val is not None:
            parsed, err = _parse_int(val, key)
            if err:
                return {"_success": False, "error": err}
            atom_kwargs[key] = parsed

    atom_request = Atom(**atom_kwargs)
    try:
        result = sdk.atom.create_atom(request_body=atom_request)
    except ApiError as e:
        msg = _extract_api_error_msg(e)
        return {
            "_success": False,
            "error": f"{msg} Use action='available_clouds' to find Boomi-managed cloud IDs, "
                     f"or action='cloud_list' for private runtime cloud IDs.",
        }

    return {
        "_success": True,
        "runtime": _runtime_to_dict(result),
        "note": "Cloud attachment created successfully.",
    }


def _action_available_clouds(sdk: Boomi, profile: str, **kwargs) -> Dict[str, Any]:
    """List Boomi-managed clouds available for cloud atom creation."""
    name_pattern = kwargs.get("name_pattern")

    if name_pattern:
        like_pattern = name_pattern if "%" in name_pattern else f"%{name_pattern}%"
        expression = CloudSimpleExpression(
            operator=CloudSimpleExpressionOperator.LIKE,
            property=CloudSimpleExpressionProperty.NAME,
            argument=[like_pattern],
        )
    else:
        expression = CloudSimpleExpression(
            operator=CloudSimpleExpressionOperator.LIKE,
            property=CloudSimpleExpressionProperty.NAME,
            argument=["%"],
        )

    query_filter = CloudQueryConfigQueryFilter(expression=expression)
    query_config = CloudQueryConfig(query_filter=query_filter)
    result = sdk.cloud.query_cloud(request_body=query_config)

    clouds = []
    def _parse_clouds(res):
        if hasattr(res, 'result') and res.result:
            items = res.result if isinstance(res.result, list) else [res.result]
            for c in items:
                cloud_dict = {
                    "id": getattr(c, 'id_', ''),
                    "name": getattr(c, 'name', ''),
                }
                atoms = getattr(c, 'atom', None)
                if atoms:
                    atom_list = atoms if isinstance(atoms, list) else [atoms]
                    cloud_dict["atoms"] = [
                        {"atom_id": getattr(a, 'atom_id', ''), "deleted": getattr(a, 'deleted', False)}
                        for a in atom_list
                    ]
                clouds.append(cloud_dict)

    _parse_clouds(result)
    while hasattr(result, 'query_token') and result.query_token:
        result = sdk.cloud.query_more_cloud(request_body=result.query_token)
        _parse_clouds(result)

    if not clouds:
        return {
            "_success": True,
            "clouds": [],
            "total_count": 0,
            "hint": "No Boomi-managed public clouds found. "
                    "If your account uses partner or test clouds, "
                    "use action='get' on an existing runtime to find its cloud_id, "
                    "or action='cloud_list' for private runtime clouds.",
        }

    return {
        "_success": True,
        "clouds": clouds,
        "total_count": len(clouds),
        "hint": "Use a cloud 'id' as cloud_id in action='create' to create a cloud attachment. "
                "These are Boomi-managed public clouds (PCS/DCS/MCS). "
                "For private runtime clouds, use action='cloud_list' instead.",
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
            cloud_kwargs[key] = _parse_bool(val)
    max_attach = kwargs.get("max_attachments_per_account")
    if max_attach is not None:
        parsed, err = _parse_int(max_attach, "max_attachments_per_account")
        if err:
            return {"_success": False, "error": err}
        cloud_kwargs["max_attachments_per_account"] = parsed

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
            update_kwargs[key] = _parse_bool(val)
        else:
            existing = getattr(current, key, None)
            if existing is not None:
                update_kwargs[key] = existing
    max_attach = kwargs.get("max_attachments_per_account")
    if max_attach is not None:
        parsed, err = _parse_int(max_attach, "max_attachments_per_account")
        if err:
            return {"_success": False, "error": err}
        update_kwargs["max_attachments_per_account"] = parsed
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
# Async Polling Helper
# ============================================================================

def _poll_async_token(poll_fn, token: str, poll_interval: int = 2,
                      max_attempts: int = 15) -> Any:
    """Poll an async token until result is ready or timeout.

    Args:
        poll_fn: Callable that takes token= and returns the result.
        token: The async token string.
        poll_interval: Seconds between polls.
        max_attempts: Maximum number of poll attempts.

    Returns:
        The result object, or None on timeout.
    """
    for attempt in range(max_attempts):
        if attempt > 0:
            time.sleep(poll_interval)
        try:
            result = poll_fn(token=token)
            if result:
                if hasattr(result, 'response_status_code'):
                    code = result.response_status_code
                    if code == 202 or getattr(code, 'value', None) == 202:
                        continue
                return result
        except Exception as e:
            if '202' in str(e) or 'still processing' in str(e).lower():
                continue
            raise
    return None


def _run_single_async(start_fn, poll_fn) -> Any:
    """Start an async operation and poll until complete.

    Args:
        start_fn: No-arg callable that returns an object with async_token.
        poll_fn: Callable that takes token= and returns the result.

    Returns:
        The final result object, or a dict with error info.
    """
    try:
        initial = start_fn()
        if not hasattr(initial, 'async_token') or not initial.async_token:
            return {"error": "No async token returned"}
        token = initial.async_token.token
    except Exception as e:
        msg = _extract_api_error_msg(e) if isinstance(e, ApiError) else str(e)
        return {"error": f"Failed to start async operation: {msg}"}

    try:
        result = _poll_async_token(poll_fn, token)
        if result is None:
            return {"error": "Operation timed out waiting for results"}
        return result
    except Exception as e:
        msg = _extract_api_error_msg(e) if isinstance(e, ApiError) else str(e)
        return {"error": f"Failed polling async result: {msg}"}


# ============================================================================
# Diagnostics Action
# ============================================================================

def _action_diagnostics(sdk: Boomi, profile: str, **kwargs) -> Dict[str, Any]:
    """Get combined runtime diagnostics: counters, disk space, listener status."""
    resource_id = kwargs.get("resource_id")
    if not resource_id:
        return {"_success": False, "error": "resource_id (atom_id) is required for 'diagnostics' action"}

    report = {"_success": True, "atom_id": resource_id}

    # 1. Atom counters
    counters_result = _run_single_async(
        lambda: sdk.atom.async_get_atom_counters(id_=resource_id),
        sdk.atom.async_token_atom_counters,
    )
    if isinstance(counters_result, dict) and "error" in counters_result:
        report["counters"] = counters_result
    else:
        counters_data = {}
        if hasattr(counters_result, 'result') and counters_result.result:
            raw = counters_result.result[0] if isinstance(counters_result.result, list) else counters_result.result
            if hasattr(raw, 'counter_group') and raw.counter_group:
                for group in raw.counter_group:
                    group_name = getattr(group, 'name', 'Unknown')
                    counters_data[group_name] = {}
                    if hasattr(group, 'counter') and group.counter:
                        for counter in group.counter:
                            c_name = getattr(counter, 'name', 'Unknown')
                            c_value = getattr(counter, 'value', None)
                            counters_data[group_name][c_name] = c_value
        report["counters"] = counters_data

    # 2. Disk space
    disk_result = _run_single_async(
        lambda: sdk.atom_disk_space.async_get_atom_disk_space(id_=resource_id),
        sdk.atom_disk_space.async_token_atom_disk_space,
    )
    if isinstance(disk_result, dict) and "error" in disk_result:
        report["disk_space"] = disk_result
    else:
        disk_data = {}
        if hasattr(disk_result, 'disk_partition') and disk_result.disk_partition:
            for partition in disk_result.disk_partition:
                name = getattr(partition, 'name', 'Unknown')
                disk_data[name] = {
                    "total_space": getattr(partition, 'total_space', 0),
                    "used_space": getattr(partition, 'used_space', 0),
                    "free_space": getattr(partition, 'free_space', 0),
                }
        report["disk_space"] = disk_data

    # 3. Listener status
    listener_query = ListenerStatusQueryConfig(
        query_filter=ListenerStatusQueryConfigQueryFilter(
            expression=ListenerStatusSimpleExpression(
                operator=ListenerStatusSimpleExpressionOperator.EQUALS,
                property=ListenerStatusSimpleExpressionProperty.CONTAINERID,
                argument=[resource_id],
            )
        )
    )
    listener_result = _run_single_async(
        lambda: sdk.listener_status.async_get_listener_status(request_body=listener_query),
        sdk.listener_status.async_token_listener_status,
    )
    if isinstance(listener_result, dict) and "error" in listener_result:
        report["listener_status"] = listener_result
    else:
        listener_data = {}
        if hasattr(listener_result, 'result') and listener_result.result:
            items = listener_result.result if isinstance(listener_result.result, list) else [listener_result.result]
            for listener in items:
                name = getattr(listener, 'listener_name', 'Unknown')
                status = getattr(listener, 'status', 'Unknown')
                listener_data[name] = _enum_str(status)
        report["listener_status"] = listener_data

    # Check sub-operation failures
    sub_ops = ["counters", "disk_space", "listener_status"]
    failed = [op for op in sub_ops if isinstance(report.get(op), dict) and "error" in report[op]]
    if len(failed) == len(sub_ops):
        report["_success"] = False
        report["error"] = "All diagnostics sub-operations failed"
    elif failed:
        report["warnings"] = [f"{op} failed: {report[op]['error']}" for op in failed]

    return report


# ============================================================================
# Release Schedule Actions
# ============================================================================

def _action_get_release_schedule(sdk: Boomi, profile: str, **kwargs) -> Dict[str, Any]:
    """Get the release schedule for a runtime."""
    resource_id = kwargs.get("resource_id")
    if not resource_id:
        return {"_success": False, "error": "resource_id is required for 'get_release_schedule' action"}

    result = sdk.runtime_release_schedule.get_runtime_release_schedule(id_=resource_id)

    schedule = {
        "atom_id": getattr(result, 'atom_id', ''),
        "schedule_type": _enum_str(getattr(result, 'schedule_type', '')),
    }
    for attr, key in [('day_of_week', 'day_of_week'), ('hour_of_day', 'hour_of_day'), ('time_zone', 'time_zone')]:
        val = getattr(result, attr, None)
        if val is not None:
            schedule[key] = val

    return {"_success": True, "release_schedule": schedule}


def _action_create_release_schedule(sdk: Boomi, profile: str, **kwargs) -> Dict[str, Any]:
    """Create a release schedule for a runtime."""
    resource_id = kwargs.get("resource_id")
    schedule_type = kwargs.get("schedule_type")

    if not resource_id:
        return {"_success": False, "error": "resource_id (atom_id) is required for 'create_release_schedule' action"}
    if not schedule_type:
        return {
            "_success": False,
            "error": "config.schedule_type is required (NEVER, FIRST, or LAST)",
        }

    schedule_kwargs = {"atom_id": resource_id, "schedule_type": schedule_type.upper()}

    for key in ("day_of_week", "time_zone"):
        val = kwargs.get(key)
        if val:
            schedule_kwargs[key] = val

    hour = kwargs.get("hour_of_day")
    if hour is not None:
        parsed, err = _parse_int(hour, "hour_of_day")
        if err:
            return {"_success": False, "error": err}
        schedule_kwargs["hour_of_day"] = parsed

    request = RuntimeReleaseSchedule(**schedule_kwargs)
    result = sdk.runtime_release_schedule.create_runtime_release_schedule(request_body=request)

    schedule = {
        "atom_id": getattr(result, 'atom_id', ''),
        "schedule_type": _enum_str(getattr(result, 'schedule_type', '')),
    }
    for attr, key in [('day_of_week', 'day_of_week'), ('hour_of_day', 'hour_of_day'), ('time_zone', 'time_zone')]:
        val = getattr(result, attr, None)
        if val is not None:
            schedule[key] = val

    return {"_success": True, "release_schedule": schedule}


def _action_update_release_schedule(sdk: Boomi, profile: str, **kwargs) -> Dict[str, Any]:
    """Update the release schedule for a runtime."""
    resource_id = kwargs.get("resource_id")
    if not resource_id:
        return {"_success": False, "error": "resource_id is required for 'update_release_schedule' action"}

    schedule_kwargs = {"atom_id": resource_id}

    schedule_type = kwargs.get("schedule_type")
    if schedule_type:
        schedule_kwargs["schedule_type"] = schedule_type.upper()

    for key in ("day_of_week", "time_zone"):
        val = kwargs.get(key)
        if val:
            schedule_kwargs[key] = val

    hour = kwargs.get("hour_of_day")
    if hour is not None:
        parsed, err = _parse_int(hour, "hour_of_day")
        if err:
            return {"_success": False, "error": err}
        schedule_kwargs["hour_of_day"] = parsed

    request = RuntimeReleaseSchedule(**schedule_kwargs)
    result = sdk.runtime_release_schedule.update_runtime_release_schedule(
        id_=resource_id, request_body=request
    )

    schedule = {
        "atom_id": getattr(result, 'atom_id', ''),
        "schedule_type": _enum_str(getattr(result, 'schedule_type', '')),
    }
    for attr, key in [('day_of_week', 'day_of_week'), ('hour_of_day', 'hour_of_day'), ('time_zone', 'time_zone')]:
        val = getattr(result, attr, None)
        if val is not None:
            schedule[key] = val

    return {"_success": True, "release_schedule": schedule}


def _action_delete_release_schedule(sdk: Boomi, profile: str, **kwargs) -> Dict[str, Any]:
    """Delete the release schedule for a runtime (resets to NEVER)."""
    resource_id = kwargs.get("resource_id")
    if not resource_id:
        return {"_success": False, "error": "resource_id is required for 'delete_release_schedule' action"}

    sdk.runtime_release_schedule.delete_runtime_release_schedule(id_=resource_id)

    return {
        "_success": True,
        "runtime_id": resource_id,
        "message": "Release schedule deleted (reset to NEVER).",
    }


# ============================================================================
# Observability Settings Actions (async reads)
# ============================================================================

def _action_get_observability_settings(sdk: Boomi, profile: str, **kwargs) -> Dict[str, Any]:
    """Get observability settings for a runtime (async operation).

    Uses raw API call for the token poll because the SDK model deserialization
    can fail when RuntimeObservabilitySettings requires runtime_id.
    """
    import json as json_mod

    resource_id = kwargs.get("resource_id")
    if not resource_id:
        return {"_success": False, "error": "resource_id is required for 'get_observability_settings' action"}

    timeout = kwargs.get("timeout", 60)

    # Step 1: Get async token
    token_result = sdk.runtime_observability_settings.async_get_runtime_observability_settings(
        id_=resource_id
    )

    if not hasattr(token_result, 'async_token') or not token_result.async_token:
        return {"_success": False, "error": "Failed to get async token for observability settings"}

    token = token_result.async_token.token

    # Step 2: Poll with raw API to avoid SDK deserialization errors
    svc = sdk.runtime_observability_settings
    base = svc.base_url or _get_env_url()
    poll_url = f"{base.rstrip('/')}/async/RuntimeObservabilitySettings/response/{token}"

    start_time = time.time()
    while time.time() - start_time < timeout:
        try:
            ser = Serializer(poll_url, [svc.get_access_token(), svc.get_basic_auth()])
            ser = ser.add_header("Accept", "application/json")
            serialized = ser.serialize().set_method("GET")
            response, status, _ = svc.send_request(serialized)

            if isinstance(response, (bytes, bytearray)):
                response = response.decode("utf-8")
            data = json_mod.loads(response) if isinstance(response, str) else response

            if data and isinstance(data, dict) and data.get("result"):
                settings_list = data["result"] if isinstance(data["result"], list) else [data["result"]]
                return {
                    "_success": True,
                    "runtime_id": resource_id,
                    "settings": settings_list,
                    "total_count": len(settings_list),
                }
            elif data and isinstance(data, dict) and not data.get("@type", "").endswith("AsyncOperationTokenResult"):
                # Direct response, not a token
                return {
                    "_success": True,
                    "runtime_id": resource_id,
                    "settings": [data],
                    "total_count": 1,
                }
        except Exception as e:
            err_str = str(e).lower()
            if "202" in err_str or "still processing" in err_str:
                time.sleep(2)
                continue
            return {"_success": False, "error": f"Error polling observability settings: {e}"}

        time.sleep(2)

    return {"_success": False, "error": f"Timeout after {timeout}s waiting for observability settings"}


def _action_update_observability_settings(sdk: Boomi, profile: str, **kwargs) -> Dict[str, Any]:
    """Update observability settings for a runtime."""
    resource_id = kwargs.get("resource_id")
    request_body = kwargs.get("request_body")

    if not resource_id:
        return {"_success": False, "error": "resource_id is required for 'update_observability_settings' action"}
    if not request_body:
        return {
            "_success": False,
            "error": "config.request_body is required for 'update_observability_settings' action. "
                     "Must include general_settings, log_settings, metric_settings, and trace_settings.",
        }

    result = sdk.runtime_observability_settings.update_runtime_observability_settings(
        id_=resource_id, request_body=request_body
    )

    settings = {"runtime_id": getattr(result, 'runtime_id', resource_id)}
    for attr in ('general_settings', 'log_settings', 'metric_settings', 'trace_settings'):
        val = getattr(result, attr, None)
        if val is not None:
            settings[attr] = str(val)

    return {"_success": True, "observability_settings": settings}


# ============================================================================
# Security Policies Actions (async reads)
# ============================================================================

def _action_get_security_policies(sdk: Boomi, profile: str, **kwargs) -> Dict[str, Any]:
    """Get security policies for a runtime cluster or cloud (async operation)."""
    resource_id = kwargs.get("resource_id")
    if not resource_id:
        return {"_success": False, "error": "resource_id is required for 'get_security_policies' action"}

    def initiate_fn():
        return sdk.atom_security_policies.async_get_atom_security_policies(id_=resource_id)

    def poll_fn(token):
        return sdk.atom_security_policies.async_token_atom_security_policies(token=token)

    response = poll_async_result(
        initiate_fn=initiate_fn,
        poll_fn=poll_fn,
        timeout=60,
        interval=2,
        resource_label="security policies",
    )

    policies_list = []
    if hasattr(response, 'result') and response.result:
        items = response.result if isinstance(response.result, list) else [response.result]
        for item in items:
            entry = {"atom_id": getattr(item, 'atom_id', '')}
            for section in ('common', 'browser', 'runner', 'worker'):
                val = getattr(item, section, None)
                if val is not None:
                    entry[section] = str(val)
            policies_list.append(entry)

    return {
        "_success": True,
        "security_policies": policies_list[0] if len(policies_list) == 1 else policies_list,
    }


def _action_update_security_policies(sdk: Boomi, profile: str, **kwargs) -> Dict[str, Any]:
    """Update security policies for a runtime cluster or cloud."""
    resource_id = kwargs.get("resource_id")
    request_body = kwargs.get("request_body")

    if not resource_id:
        return {"_success": False, "error": "resource_id is required for 'update_security_policies' action"}
    if not request_body:
        return {
            "_success": False,
            "error": "config.request_body is required for 'update_security_policies' action. "
                     "Must include atom_id and common section at minimum.",
        }

    result = sdk.atom_security_policies.update_atom_security_policies(
        id_=resource_id, request_body=request_body
    )

    policies = {"atom_id": getattr(result, 'atom_id', resource_id)}
    for section in ('common', 'browser', 'runner', 'worker'):
        val = getattr(result, section, None)
        if val is not None:
            policies[section] = str(val)

    return {"_success": True, "security_policies": policies}


# ============================================================================
# Other Runtime Operations
# ============================================================================

def _action_get_startup_properties(sdk: Boomi, profile: str, **kwargs) -> Dict[str, Any]:
    """Get startup properties for a runtime."""
    resource_id = kwargs.get("resource_id")
    if not resource_id:
        return {"_success": False, "error": "resource_id is required for 'get_startup_properties' action"}

    result = sdk.atom_startup_properties.get_atom_startup_properties(id_=resource_id)

    properties = []
    if hasattr(result, 'property') and result.property:
        items = result.property if isinstance(result.property, list) else [result.property]
        for prop in items:
            # SDK model may use 'key' or 'name' for the property identifier
            key = getattr(prop, 'key', '') or getattr(prop, 'name', '')
            properties.append({
                "key": key,
                "value": getattr(prop, 'value', ''),
            })

    return {
        "_success": True,
        "runtime_id": getattr(result, 'id_', resource_id),
        "properties": properties,
        "total_count": len(properties),
    }


def _action_reset_counters(sdk: Boomi, profile: str, **kwargs) -> Dict[str, Any]:
    """Reset counters for a runtime."""
    resource_id = kwargs.get("resource_id")
    if not resource_id:
        return {"_success": False, "error": "resource_id is required for 'reset_counters' action"}

    # UPDATE with empty counters resets them
    request = AtomCounters(atom_id=resource_id)
    result = sdk.atom_counters.update_atom_counters(id_=resource_id, request_body=request)

    return {
        "_success": True,
        "runtime_id": resource_id,
        "message": "Counters reset successfully.",
    }


def _action_purge(sdk: Boomi, profile: str, **kwargs) -> Dict[str, Any]:
    """Purge processed data from a runtime cloud attachment."""
    resource_id = kwargs.get("resource_id")
    if not resource_id:
        return {"_success": False, "error": "resource_id is required for 'purge' action"}

    request = AtomPurge(atom_id=resource_id)
    result = sdk.atom_purge.update_atom_purge(id_=resource_id, request_body=request)

    return {
        "_success": True,
        "runtime_id": resource_id,
        "message": "Purge initiated for runtime cloud attachment.",
    }


def _action_get_connector_versions(sdk: Boomi, profile: str, **kwargs) -> Dict[str, Any]:
    """Get connector versions for a runtime."""
    resource_id = kwargs.get("resource_id")
    if not resource_id:
        return {"_success": False, "error": "resource_id is required for 'get_connector_versions' action"}

    result = sdk.atom_connector_versions.get_atom_connector_versions(id_=resource_id)

    connectors = []
    if hasattr(result, 'connector_version') and result.connector_version:
        items = result.connector_version if isinstance(result.connector_version, list) else [result.connector_version]
        for cv in items:
            connectors.append({
                "name": getattr(cv, 'name', ''),
                "version": getattr(cv, 'version', ''),
            })

    return {
        "_success": True,
        "runtime_id": getattr(result, 'id_', resource_id),
        "connector_versions": connectors,
        "total_count": len(connectors),
    }


def _action_offboard_node(sdk: Boomi, profile: str, **kwargs) -> Dict[str, Any]:
    """Offboard a node from a runtime cluster or cloud."""
    resource_id = kwargs.get("resource_id")
    node_id = kwargs.get("node_id")

    if not resource_id:
        return {"_success": False, "error": "resource_id (atom_id) is required for 'offboard_node' action"}
    if not node_id:
        return {"_success": False, "error": "config.node_id is required for 'offboard_node' action"}

    # node_id can be a single string or a list
    if isinstance(node_id, str):
        node_id = [node_id]

    request = NodeOffboard(atom_id=resource_id, node_id=node_id)
    result = sdk.node_offboard.create_node_offboard(request_body=request)

    return {
        "_success": True,
        "runtime_id": resource_id,
        "node_id": node_id,
        "message": "Node offboard initiated. Node status will change to 'Deleting'.",
    }


def _action_refresh_secrets_manager(sdk: Boomi, profile: str, **kwargs) -> Dict[str, Any]:
    """Refresh secrets manager cache."""
    provider = kwargs.get("provider")

    req_kwargs = {}
    if provider:
        req_kwargs["provider"] = provider.upper()

    request = SecretsManagerRefreshRequest(**req_kwargs)
    result = sdk.refresh_secrets_manager.refresh_secrets_manager(request_body=request)

    message = getattr(result, 'message', None) or "Secrets manager refresh initiated."

    return {
        "_success": True,
        "message": message,
    }


# ============================================================================
# Cloud Attachment Management Actions
# ============================================================================

def _action_get_account_cloud_attachment_properties(sdk: Boomi, profile: str, **kwargs) -> Dict[str, Any]:
    """Get account cloud attachment properties (async operation)."""
    resource_id = kwargs.get("resource_id")
    if not resource_id:
        return {"_success": False, "error": "resource_id (container_id) is required for 'get_account_cloud_attachment_properties' action"}

    def initiate_fn():
        return sdk.account_cloud_attachment_properties.async_get_account_cloud_attachment_properties(
            id_=resource_id
        )

    def poll_fn(token):
        return sdk.account_cloud_attachment_properties.async_token_account_cloud_attachment_properties(
            token=token
        )

    response = poll_async_result(
        initiate_fn=initiate_fn,
        poll_fn=poll_fn,
        timeout=60,
        interval=2,
        resource_label="account cloud attachment properties",
    )

    # Extract properties from the async response
    properties = {}
    if hasattr(response, 'result') and response.result:
        items = response.result if isinstance(response.result, list) else [response.result]
        if items:
            item = items[0]
            # Convert object attributes to dict
            for attr in dir(item):
                if not attr.startswith('_'):
                    val = getattr(item, attr, None)
                    if val is not None and not callable(val):
                        properties[attr] = val

    return {"_success": True, "properties": properties}


def _action_update_account_cloud_attachment_properties(sdk: Boomi, profile: str, **kwargs) -> Dict[str, Any]:
    """Update account cloud attachment properties."""
    resource_id = kwargs.get("resource_id")
    request_body = kwargs.get("request_body")

    if not resource_id:
        return {"_success": False, "error": "resource_id (container_id) is required for 'update_account_cloud_attachment_properties' action"}
    if not request_body:
        return {"_success": False, "error": "config.request_body is required for 'update_account_cloud_attachment_properties' action"}

    result = sdk.account_cloud_attachment_properties.update_account_cloud_attachment_properties(
        id_=resource_id, request_body=request_body
    )

    properties = {}
    for attr in dir(result):
        if not attr.startswith('_'):
            val = getattr(result, attr, None)
            if val is not None and not callable(val):
                properties[attr] = val

    return {"_success": True, "properties": properties}


def _action_list_account_cloud_attachment_summaries(sdk: Boomi, profile: str, **kwargs) -> Dict[str, Any]:
    """Query account cloud attachment summaries with optional cloud_id filter."""
    from boomi.models import (
        AccountCloudAttachmentSummaryQueryConfig,
        AccountCloudAttachmentSummaryQueryConfigQueryFilter,
        AccountCloudAttachmentSummarySimpleExpression,
        AccountCloudAttachmentSummarySimpleExpressionOperator,
        AccountCloudAttachmentSummarySimpleExpressionProperty,
    )

    cloud_id = kwargs.get("cloud_id")

    if cloud_id:
        expression = AccountCloudAttachmentSummarySimpleExpression(
            operator=AccountCloudAttachmentSummarySimpleExpressionOperator.EQUALS,
            property=AccountCloudAttachmentSummarySimpleExpressionProperty.CLOUDID,
            argument=[cloud_id],
        )
    else:
        expression = AccountCloudAttachmentSummarySimpleExpression(
            operator=AccountCloudAttachmentSummarySimpleExpressionOperator.ISNOTNULL,
            property=AccountCloudAttachmentSummarySimpleExpressionProperty.CLOUDID,
            argument=[],
        )

    query_filter = AccountCloudAttachmentSummaryQueryConfigQueryFilter(expression=expression)
    query_config = AccountCloudAttachmentSummaryQueryConfig(query_filter=query_filter)
    result = sdk.account_cloud_attachment_summary.query_account_cloud_attachment_summary(
        request_body=query_config
    )

    summaries = []
    if hasattr(result, 'result') and result.result:
        items = result.result if isinstance(result.result, list) else [result.result]
        for item in items:
            entry = {}
            for attr in dir(item):
                if not attr.startswith('_'):
                    val = getattr(item, attr, None)
                    if val is not None and not callable(val):
                        entry[attr] = _enum_str(val) if hasattr(val, 'value') else val
            summaries.append(entry)

    while hasattr(result, 'query_token') and result.query_token:
        result = sdk.account_cloud_attachment_summary.query_more_account_cloud_attachment_summary(
            request_body=result.query_token
        )
        if hasattr(result, 'result') and result.result:
            items = result.result if isinstance(result.result, list) else [result.result]
            for item in items:
                entry = {}
                for attr in dir(item):
                    if not attr.startswith('_'):
                        val = getattr(item, attr, None)
                        if val is not None and not callable(val):
                            entry[attr] = _enum_str(val) if hasattr(val, 'value') else val
                summaries.append(entry)

    return {"_success": True, "summaries": summaries, "total_count": len(summaries)}


def _action_get_account_cloud_attachment_summary(sdk: Boomi, profile: str, **kwargs) -> Dict[str, Any]:
    """Get a single account cloud attachment summary by resource_id."""
    resource_id = kwargs.get("resource_id")
    if not resource_id:
        return {"_success": False, "error": "resource_id is required for 'get_account_cloud_attachment_summary' action"}

    result = sdk.account_cloud_attachment_summary.get_account_cloud_attachment_summary(
        id_=resource_id
    )

    summary = {}
    for attr in dir(result):
        if not attr.startswith('_'):
            val = getattr(result, attr, None)
            if val is not None and not callable(val):
                summary[attr] = _enum_str(val) if hasattr(val, 'value') else val

    return {"_success": True, "summary": summary}


def _action_list_account_cloud_attachment_quotas(sdk: Boomi, profile: str, **kwargs) -> Dict[str, Any]:
    """Bulk-get account cloud attachment quotas by IDs."""
    from boomi.models import AccountCloudAttachmentQuotaBulkRequest

    resource_ids = kwargs.get("resource_ids")
    if not resource_ids:
        return {"_success": False, "error": "config.resource_ids (list of quota IDs) is required for 'list_account_cloud_attachment_quotas' action"}

    if isinstance(resource_ids, str):
        resource_ids = [resource_ids]

    request = AccountCloudAttachmentQuotaBulkRequest(id_=resource_ids)
    result = sdk.account_cloud_attachment_quota.bulk_account_cloud_attachment_quota(
        request_body=request
    )

    quotas = []
    if hasattr(result, 'result') and result.result:
        items = result.result if isinstance(result.result, list) else [result.result]
        for item in items:
            entry = {}
            for attr in dir(item):
                if not attr.startswith('_'):
                    val = getattr(item, attr, None)
                    if val is not None and not callable(val):
                        entry[attr] = _enum_str(val) if hasattr(val, 'value') else val
            quotas.append(entry)

    return {"_success": True, "quotas": quotas, "total_count": len(quotas)}


def _action_get_account_cloud_attachment_quota(sdk: Boomi, profile: str, **kwargs) -> Dict[str, Any]:
    """Get a single account cloud attachment quota by resource_id."""
    resource_id = kwargs.get("resource_id")
    if not resource_id:
        return {"_success": False, "error": "resource_id is required for 'get_account_cloud_attachment_quota' action"}

    result = sdk.account_cloud_attachment_quota.get_account_cloud_attachment_quota(
        id_=resource_id
    )

    quota = {}
    for attr in dir(result):
        if not attr.startswith('_'):
            val = getattr(result, attr, None)
            if val is not None and not callable(val):
                quota[attr] = _enum_str(val) if hasattr(val, 'value') else val

    return {"_success": True, "quota": quota}


def _action_create_account_cloud_attachment_quota(sdk: Boomi, profile: str, **kwargs) -> Dict[str, Any]:
    """Create an account cloud attachment quota."""
    request_body = kwargs.get("request_body")
    if not request_body:
        return {"_success": False, "error": "config.request_body is required for 'create_account_cloud_attachment_quota' action"}

    result = sdk.account_cloud_attachment_quota.create_account_cloud_attachment_quota(
        request_body=request_body
    )

    quota = {}
    for attr in dir(result):
        if not attr.startswith('_'):
            val = getattr(result, attr, None)
            if val is not None and not callable(val):
                quota[attr] = _enum_str(val) if hasattr(val, 'value') else val

    return {"_success": True, "quota": quota}


def _action_update_account_cloud_attachment_quota(sdk: Boomi, profile: str, **kwargs) -> Dict[str, Any]:
    """Update an account cloud attachment quota."""
    resource_id = kwargs.get("resource_id")
    request_body = kwargs.get("request_body")

    if not resource_id:
        return {"_success": False, "error": "resource_id is required for 'update_account_cloud_attachment_quota' action"}
    if not request_body:
        return {"_success": False, "error": "config.request_body is required for 'update_account_cloud_attachment_quota' action"}

    result = sdk.account_cloud_attachment_quota.update_account_cloud_attachment_quota(
        id_=resource_id, request_body=request_body
    )

    quota = {}
    for attr in dir(result):
        if not attr.startswith('_'):
            val = getattr(result, attr, None)
            if val is not None and not callable(val):
                quota[attr] = _enum_str(val) if hasattr(val, 'value') else val

    return {"_success": True, "quota": quota}


def _action_delete_account_cloud_attachment_quota(sdk: Boomi, profile: str, **kwargs) -> Dict[str, Any]:
    """Delete an account cloud attachment quota."""
    resource_id = kwargs.get("resource_id")
    if not resource_id:
        return {"_success": False, "error": "resource_id is required for 'delete_account_cloud_attachment_quota' action"}

    sdk.account_cloud_attachment_quota.delete_account_cloud_attachment_quota(
        id_=resource_id
    )

    return {
        "_success": True,
        "deleted_id": resource_id,
        "message": "Account cloud attachment quota deleted.",
    }


def _action_get_cloud_attachment_properties(sdk: Boomi, profile: str, **kwargs) -> Dict[str, Any]:
    """Get cloud attachment properties (async operation)."""
    resource_id = kwargs.get("resource_id")
    if not resource_id:
        return {"_success": False, "error": "resource_id is required for 'get_cloud_attachment_properties' action"}

    def initiate_fn():
        return sdk.cloud_attachment_properties.async_get_cloud_attachment_properties(
            id_=resource_id
        )

    def poll_fn(token):
        return sdk.cloud_attachment_properties.async_token_cloud_attachment_properties(
            token=token
        )

    response = poll_async_result(
        initiate_fn=initiate_fn,
        poll_fn=poll_fn,
        timeout=60,
        interval=2,
        resource_label="cloud attachment properties",
    )

    properties = {}
    if hasattr(response, 'result') and response.result:
        items = response.result if isinstance(response.result, list) else [response.result]
        if items:
            item = items[0]
            for attr in dir(item):
                if not attr.startswith('_'):
                    val = getattr(item, attr, None)
                    if val is not None and not callable(val):
                        properties[attr] = val

    return {"_success": True, "properties": properties}


def _action_update_cloud_attachment_properties(sdk: Boomi, profile: str, **kwargs) -> Dict[str, Any]:
    """Update cloud attachment properties."""
    resource_id = kwargs.get("resource_id")
    request_body = kwargs.get("request_body")

    if not resource_id:
        return {"_success": False, "error": "resource_id is required for 'update_cloud_attachment_properties' action"}
    if not request_body:
        return {"_success": False, "error": "config.request_body is required for 'update_cloud_attachment_properties' action"}

    result = sdk.cloud_attachment_properties.update_cloud_attachment_properties(
        id_=resource_id, request_body=request_body
    )

    properties = {}
    for attr in dir(result):
        if not attr.startswith('_'):
            val = getattr(result, attr, None)
            if val is not None and not callable(val):
                properties[attr] = val

    return {"_success": True, "properties": properties}


def _action_get_account_cloud_attachment_defaults(sdk: Boomi, profile: str, **kwargs) -> Dict[str, Any]:
    """Get account cloud attachment property defaults (async operation)."""
    resource_id = kwargs.get("resource_id")
    if not resource_id:
        return {"_success": False, "error": "resource_id is required for 'get_account_cloud_attachment_defaults' action"}

    def initiate_fn():
        return sdk.account_cloud_attachment_properties_default.async_get_account_cloud_attachment_properties_default(
            id_=resource_id
        )

    def poll_fn(token):
        return sdk.account_cloud_attachment_properties_default.async_token_account_cloud_attachment_properties_default(
            token=token
        )

    response = poll_async_result(
        initiate_fn=initiate_fn,
        poll_fn=poll_fn,
        timeout=60,
        interval=2,
        resource_label="account cloud attachment property defaults",
    )

    defaults = {}
    if hasattr(response, 'result') and response.result:
        items = response.result if isinstance(response.result, list) else [response.result]
        if items:
            item = items[0]
            for attr in dir(item):
                if not attr.startswith('_'):
                    val = getattr(item, attr, None)
                    if val is not None and not callable(val):
                        defaults[attr] = val

    return {"_success": True, "defaults": defaults}


def _action_update_account_cloud_attachment_defaults(sdk: Boomi, profile: str, **kwargs) -> Dict[str, Any]:
    """Update account cloud attachment property defaults."""
    resource_id = kwargs.get("resource_id")
    request_body = kwargs.get("request_body")

    if not resource_id:
        return {"_success": False, "error": "resource_id is required for 'update_account_cloud_attachment_defaults' action"}
    if not request_body:
        return {"_success": False, "error": "config.request_body is required for 'update_account_cloud_attachment_defaults' action"}

    result = sdk.account_cloud_attachment_properties_default.update_account_cloud_attachment_properties_default(
        id_=resource_id, request_body=request_body
    )

    defaults = {}
    for attr in dir(result):
        if not attr.startswith('_'):
            val = getattr(result, attr, None)
            if val is not None and not callable(val):
                defaults[attr] = val

    return {"_success": True, "defaults": defaults}


# ============================================================================
# Error Helpers
# ============================================================================

def _extract_api_error_msg(e: ApiError) -> str:
    """Extract user-friendly error message from ApiError."""
    # 1. SDK's pre-parsed XML error detail
    detail = getattr(e, 'error_detail', None)
    if detail:
        return detail
    # 2. JSON response body with "message" key
    resp = getattr(e, 'response', None)
    if resp:
        body = getattr(resp, 'body', None)
        if isinstance(body, dict):
            msg = body.get("message", "")
            if msg:
                return msg
    # 3. Fallback to ApiError.message (contains URL + status)
    return getattr(e, 'message', '') or str(e)


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
        "create": _action_create,
        "update": _action_update,
        "delete": _action_delete,
        "attach": _action_attach,
        "detach": _action_detach,
        "list_attachments": _action_list_attachments,
        "restart": _action_restart,
        "configure_java": _action_configure_java,
        "create_installer_token": _action_create_installer_token,
        "available_clouds": _action_available_clouds,
        "cloud_list": _action_cloud_list,
        "cloud_get": _action_cloud_get,
        "cloud_create": _action_cloud_create,
        "cloud_update": _action_cloud_update,
        "cloud_delete": _action_cloud_delete,
        "diagnostics": _action_diagnostics,
        "get_release_schedule": _action_get_release_schedule,
        "create_release_schedule": _action_create_release_schedule,
        "update_release_schedule": _action_update_release_schedule,
        "delete_release_schedule": _action_delete_release_schedule,
        "get_observability_settings": _action_get_observability_settings,
        "update_observability_settings": _action_update_observability_settings,
        "get_security_policies": _action_get_security_policies,
        "update_security_policies": _action_update_security_policies,
        "get_startup_properties": _action_get_startup_properties,
        "reset_counters": _action_reset_counters,
        "purge": _action_purge,
        "get_connector_versions": _action_get_connector_versions,
        "offboard_node": _action_offboard_node,
        "refresh_secrets_manager": _action_refresh_secrets_manager,
        "get_account_cloud_attachment_properties": _action_get_account_cloud_attachment_properties,
        "update_account_cloud_attachment_properties": _action_update_account_cloud_attachment_properties,
        "list_account_cloud_attachment_summaries": _action_list_account_cloud_attachment_summaries,
        "get_account_cloud_attachment_summary": _action_get_account_cloud_attachment_summary,
        "list_account_cloud_attachment_quotas": _action_list_account_cloud_attachment_quotas,
        "get_account_cloud_attachment_quota": _action_get_account_cloud_attachment_quota,
        "create_account_cloud_attachment_quota": _action_create_account_cloud_attachment_quota,
        "update_account_cloud_attachment_quota": _action_update_account_cloud_attachment_quota,
        "delete_account_cloud_attachment_quota": _action_delete_account_cloud_attachment_quota,
        "get_cloud_attachment_properties": _action_get_cloud_attachment_properties,
        "update_cloud_attachment_properties": _action_update_cloud_attachment_properties,
        "get_account_cloud_attachment_defaults": _action_get_account_cloud_attachment_defaults,
        "update_account_cloud_attachment_defaults": _action_update_account_cloud_attachment_defaults,
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
