"""
Shared Resources Management MCP Tools for Boomi Platform.

Provides 5 shared resource management actions:
- list_web_servers: Get shared web server configuration for an atom
- update_web_server: Update shared web server settings
- list_channels: List shared communication channels
- get_channel: Get a specific communication channel by ID
- create_channel: Create a new communication channel
"""

from typing import Dict, Any, Optional, List

from boomi import Boomi
from boomi.net.transport.api_error import ApiError
from boomi.models import (
    SharedWebServer,
    SharedCommunicationChannelComponent,
    SharedCommunicationChannelComponentQueryConfig,
    SharedCommunicationChannelComponentSimpleExpression,
    SharedCommunicationChannelComponentSimpleExpressionOperator,
    SharedCommunicationChannelComponentQueryConfigQueryFilter,
    PartnerArchiving,
    PartnerCommunication,
)


# ============================================================================
# Helpers
# ============================================================================

def _web_server_to_dict(server) -> Dict[str, Any]:
    """Convert SDK SharedWebServer object to plain dict."""
    result = {
        "id": getattr(server, 'id_', ''),
    }
    for attr in (
        'url', 'base_url', 'port', 'ssl_port', 'api_type',
        'auth_type', 'max_connections', 'max_threads',
        'external_host', 'external_port', 'external_ssl_port',
    ):
        val = getattr(server, attr, None)
        if val is not None:
            result[attr] = val if isinstance(val, (bool, int)) else str(val)
    return result


def _channel_to_dict(channel) -> Dict[str, Any]:
    """Convert SDK SharedCommunicationChannelComponent to plain dict."""
    result = {
        "id": getattr(channel, 'id_', ''),
        "name": getattr(channel, 'component_name', '') or getattr(channel, 'name', ''),
        "type": getattr(channel, 'communication_type', None) or getattr(channel, 'type', None),
    }
    # Remove type if still None
    if result["type"] is None:
        del result["type"]
    elif hasattr(result["type"], 'value'):
        result["type"] = str(result["type"].value)
    else:
        result["type"] = str(result["type"])
    for attr in (
        'folder_id', 'folder_full_path',
        'created_date', 'modified_date', 'created_by', 'modified_by',
        'component_id', 'version', 'current_version', 'deleted',
    ):
        val = getattr(channel, attr, None)
        if val is not None:
            if hasattr(val, 'value'):
                result[attr] = str(val.value)
            elif isinstance(val, (bool, int)):
                result[attr] = val
            else:
                result[attr] = str(val)
    return result


def _query_all_channels(sdk: Boomi, expression=None) -> List[Dict[str, Any]]:
    """Execute channel query with pagination, return list of dicts."""
    if expression:
        query_filter = SharedCommunicationChannelComponentQueryConfigQueryFilter(
            expression=expression
        )
    else:
        # Default: list all channels using wildcard
        expression = SharedCommunicationChannelComponentSimpleExpression(
            operator=SharedCommunicationChannelComponentSimpleExpressionOperator.LIKE,
            property="name",
            argument=["%"],
        )
        query_filter = SharedCommunicationChannelComponentQueryConfigQueryFilter(
            expression=expression
        )

    query_config = SharedCommunicationChannelComponentQueryConfig(
        query_filter=query_filter
    )
    result = sdk.shared_communication_channel_component.query_shared_communication_channel_component(
        request_body=query_config
    )

    channels = []
    if hasattr(result, 'result') and result.result:
        items = result.result if isinstance(result.result, list) else [result.result]
        channels.extend([_channel_to_dict(c) for c in items])

    while hasattr(result, 'query_token') and result.query_token:
        result = sdk.shared_communication_channel_component.query_more_shared_communication_channel_component(
            request_body=result.query_token
        )
        if hasattr(result, 'result') and result.result:
            items = result.result if isinstance(result.result, list) else [result.result]
            channels.extend([_channel_to_dict(c) for c in items])

    return channels


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
# Action Handlers
# ============================================================================

def _action_list_web_servers(sdk: Boomi, profile: str, **kwargs) -> Dict[str, Any]:
    """Get shared web server configuration for an atom."""
    resource_id = kwargs.get("resource_id")
    if not resource_id:
        return {"_success": False, "error": "resource_id (atom_id) is required for 'list_web_servers' action"}

    server = sdk.shared_web_server.get_shared_web_server(id_=resource_id)
    return {
        "_success": True,
        "web_server": _web_server_to_dict(server),
    }


def _action_update_web_server(sdk: Boomi, profile: str, **kwargs) -> Dict[str, Any]:
    """Update shared web server settings."""
    resource_id = kwargs.get("resource_id")
    if not resource_id:
        return {"_success": False, "error": "resource_id (atom_id) is required for 'update_web_server' action"}

    # Get current config first
    current = sdk.shared_web_server.get_shared_web_server(id_=resource_id)

    # Apply updates from config
    update_fields = {
        k: v for k, v in kwargs.items()
        if k not in ("resource_id",) and hasattr(current, k)
    }

    if not update_fields:
        return {
            "_success": False,
            "error": "No valid update fields provided in config. "
                     "Provide fields like: url, port, ssl_port, max_connections, max_threads, "
                     "auth_type, api_type, external_host, external_port, external_ssl_port",
        }

    for key, value in update_fields.items():
        setattr(current, key, value)

    updated = sdk.shared_web_server.update_shared_web_server(
        id_=resource_id,
        request_body=current,
    )

    return {
        "_success": True,
        "web_server": _web_server_to_dict(updated),
        "updated_fields": list(update_fields.keys()),
    }


def _action_list_channels(sdk: Boomi, profile: str, **kwargs) -> Dict[str, Any]:
    """List shared communication channels."""
    name_pattern = kwargs.get("name_pattern")

    if name_pattern:
        like_pattern = name_pattern if "%" in name_pattern else f"%{name_pattern}%"
        expression = SharedCommunicationChannelComponentSimpleExpression(
            operator=SharedCommunicationChannelComponentSimpleExpressionOperator.LIKE,
            property="name",
            argument=[like_pattern],
        )
    else:
        expression = None  # _query_all_channels handles default

    channels = _query_all_channels(sdk, expression)

    return {
        "_success": True,
        "channels": channels,
        "total_count": len(channels),
    }


def _action_get_channel(sdk: Boomi, profile: str, **kwargs) -> Dict[str, Any]:
    """Get a specific communication channel by ID."""
    resource_id = kwargs.get("resource_id")
    if not resource_id:
        return {"_success": False, "error": "resource_id (channel_id) is required for 'get_channel' action"}

    channel = sdk.shared_communication_channel_component.get_shared_communication_channel_component(
        id_=resource_id
    )
    return {
        "_success": True,
        "channel": _channel_to_dict(channel),
    }


def _action_create_channel(sdk: Boomi, profile: str, **kwargs) -> Dict[str, Any]:
    """Create a new communication channel."""
    name = kwargs.get("name")
    channel_type = kwargs.get("channel_type") or kwargs.get("type")

    if not name:
        return {"_success": False, "error": "config.name is required for 'create_channel' action"}

    # Map user-facing names to SDK constructor params
    channel_kwargs = {"component_name": name}
    if channel_type:
        channel_kwargs["communication_type"] = channel_type

    # Pass through additional SDK-recognized fields
    for key in ("folder_id", "folder_name", "description"):
        val = kwargs.get(key)
        if val is not None:
            channel_kwargs[key] = val

    channel = SharedCommunicationChannelComponent(
        partner_archiving=PartnerArchiving(),
        partner_communication=PartnerCommunication(),
        **channel_kwargs,
    )
    created = sdk.shared_communication_channel_component.create_shared_communication_channel_component(
        request_body=channel
    )

    return {
        "_success": True,
        "channel": _channel_to_dict(created),
    }


# ============================================================================
# Action Router
# ============================================================================

def manage_shared_resources_action(
    sdk: Boomi,
    profile: str,
    action: str,
    config_data: Optional[Dict[str, Any]] = None,
    **kwargs,
) -> Dict[str, Any]:
    """
    Route to the appropriate shared resources action handler.

    Args:
        sdk: Authenticated Boomi SDK client
        profile: Profile name
        action: One of: list_web_servers, update_web_server, list_channels, get_channel, create_channel
        config_data: Action-specific configuration dict
        **kwargs: Additional parameters (resource_id, etc.)
    """
    if config_data is None:
        config_data = {}

    # Merge config_data into kwargs
    merged = {**config_data, **kwargs}

    actions = {
        "list_web_servers": _action_list_web_servers,
        "update_web_server": _action_update_web_server,
        "list_channels": _action_list_channels,
        "get_channel": _action_get_channel,
        "create_channel": _action_create_channel,
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
