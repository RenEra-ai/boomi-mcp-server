"""
Shared Resources Management MCP Tools for Boomi Platform.

Provides 10 shared resource management actions:
- list_web_servers: Get shared web server configuration for an atom
- update_web_server: Update shared web server settings
- get_web_server: Alias for list_web_servers (single-runtime GET)
- list_channels: List shared communication channels
- get_channel: Get a specific communication channel by ID
- create_channel: Create a new communication channel
- update_channel: Update a shared communication channel by ID
- delete_channel: Delete a shared communication channel by ID
- get_server_info: Get shared server information for a runtime
- update_server_info: Update shared server information for a runtime
"""

from typing import Dict, Any, Optional, List

from boomi import Boomi
from boomi.net.transport.api_error import ApiError
# Serializer/Environment are used only by the documented SharedWebServer JSON
# transport (_raw_web_server_request); component-family create/get/update use the
# shared component_family_json_request helper.
from boomi.net.transport.serializer import Serializer
from boomi.net.environment import Environment
from boomi_mcp.categories.components._shared import (
    component_family_json_request,
    _json_error_message,
)
from boomi.models import (
    SharedWebServer,
    SharedCommunicationChannelComponent,
    SharedCommunicationChannelComponentQueryConfig,
    SharedCommunicationChannelComponentSimpleExpression,
    SharedCommunicationChannelComponentSimpleExpressionOperator,
    SharedCommunicationChannelComponentQueryConfigQueryFilter,
    PartnerArchiving,
    PartnerCommunication,
    SharedServerInformation,
    FtpCommunicationOptions,
    SftpCommunicationOptions,
    HttpCommunicationOptions,
    As2CommunicationOptions,
    DiskCommunicationOptions,
    MllpCommunicationOptions,
    OftpCommunicationOptions,
)

# Map channel types to their PartnerCommunication kwarg and options class
_CHANNEL_TYPE_COMM_OPTIONS = {
    'FTP': ('ftp_communication_options', FtpCommunicationOptions),
    'SFTP': ('sftp_communication_options', SftpCommunicationOptions),
    'HTTP': ('http_communication_options', HttpCommunicationOptions),
    'AS2': ('as2_communication_options', As2CommunicationOptions),
    'DISK': ('disk_communication_options', DiskCommunicationOptions),
    'MLLP': ('mllp_communication_options', MllpCommunicationOptions),
    'OFTP': ('oftp_communication_options', OftpCommunicationOptions),
}


# ============================================================================
# Helpers
# ============================================================================

def _raw_web_server_request(sdk: Boomi, resource_id: str, method: str = "GET",
                            body: dict = None) -> dict:
    """Documented JSON transport for the SharedWebServer endpoint.

    SharedWebServer supports JSON, and updates require POSTing the full modified
    settings JSON. The SDK's typed ``update_shared_web_server`` takes a
    ``SharedWebServer`` model whose nested settings (``SharedWebServerGeneral`` /
    cloud-tenant / listener-port / authentication) have strict constructors, so
    rebuilding them from a sparse cloud-atom GET is impractical and lossy. We
    therefore GET/modify/POST the JSON directly here. This is a deliberate JSON
    path for a JSON endpoint — not one of the stale raw-XML SDK bypasses; only the
    XML-only generic /Component endpoint uses the SDK's raw-XML methods.
    """
    svc = sdk.shared_web_server
    base = svc.base_url or Environment.DEFAULT.url
    url = f"{base}/SharedWebServer/{resource_id}"

    ser = Serializer(url, [svc.get_access_token(), svc.get_basic_auth()])  # sdk-bypass-ok: SharedWebServer JSON transport (typed update needs strict nested models)
    ser = ser.add_header("Accept", "application/json")
    serialized = ser.serialize().set_method(method)

    if body is not None:
        serialized = serialized.set_body(body, "application/json")

    response, status, _ = svc.send_request(serialized)  # sdk-bypass-ok: SharedWebServer JSON transport
    return response


def _web_server_to_dict(data: dict) -> Dict[str, Any]:
    """Convert raw SharedWebServer JSON response to a clean dict.

    Handles both cloud atoms (settings in cloudTennantGeneral) and
    local atoms (settings in generalSettings).
    """
    result = {"atom_id": data.get("atomId", "")}

    # Cloud atoms use cloudTennantGeneral, local atoms use generalSettings
    gs = data.get("generalSettings") or data.get("cloudTennantGeneral") or {}

    for key in ('apiType', 'baseUrl', 'externalHost', 'internalHost', 'sslCertificate'):
        val = gs.get(key)
        if val is not None:
            # camelCase → snake_case for user-facing output
            snake = ''.join(f'_{c.lower()}' if c.isupper() else c for c in key)
            result[snake] = val

    mnt = gs.get('maxNumberOfThreads')
    if mnt is not None:
        result['max_number_of_threads'] = mnt

    auth_type = gs.get('authType')
    if auth_type is None:
        # Local atoms nest authType under authentication object
        auth_obj = gs.get('authentication')
        if auth_obj and isinstance(auth_obj, dict):
            auth_type = auth_obj.get('authType')
    if auth_type is not None:
        result['auth_type'] = auth_type

    lp = gs.get("listenerPorts") or {}
    ports_list = lp.get("port", [])
    if ports_list:
        result['ports'] = []
        for p in ports_list:
            port_dict = {}
            for camel, snake in (('authType', 'auth_type'), ('baseUrlForRequest', 'base_url_for_request')):
                v = p.get(camel)
                if v is not None:
                    port_dict[snake] = v
            for camel, snake in (('port', 'port'), ('externalPort', 'external_port')):
                v = p.get(camel)
                if v is not None:
                    port_dict[snake] = v
            for camel, snake in (('ssl', 'ssl'), ('externalSSL', 'external_ssl'),
                                  ('enablePort', 'enable_port'), ('defaultPort', 'default_port')):
                v = p.get(camel)
                if v is not None:
                    port_dict[snake] = v
            result['ports'].append(port_dict)

    return result


def _channel_to_dict(channel) -> Dict[str, Any]:
    """Convert a SharedCommunicationChannelComponent to a plain dict.

    Accepts either a JSON dict (from the create/get/update JSON transport) or a
    typed SDK model (from the query/list path), since SDK 3.0.0 made only the
    create/get/update methods XML-only while query still returns typed models.
    """
    if isinstance(channel, dict):
        result: Dict[str, Any] = {
            "id": channel.get('componentId', ''),
            "name": channel.get('componentName', ''),
        }
        ctype = channel.get('communicationType')
        if ctype is not None:
            result["type"] = str(ctype)
        for wire, snake in (
            ('folderId', 'folder_id'), ('folderFullPath', 'folder_full_path'),
            ('componentId', 'component_id'), ('deleted', 'deleted'),
            ('description', 'description'), ('folderName', 'folder_name'),
            ('branchId', 'branch_id'), ('branchName', 'branch_name'),
        ):
            val = channel.get(wire)
            if val is not None:
                result[snake] = val if isinstance(val, (bool, int)) else str(val)
        return result

    result = {
        "id": getattr(channel, 'component_id', ''),
        "name": getattr(channel, 'component_name', ''),
        "type": getattr(channel, 'communication_type', None),
    }
    # Remove type if still None
    if result["type"] is None:
        del result["type"]
    elif hasattr(result["type"], 'value'):
        result["type"] = str(result["type"].value)
    else:
        result["type"] = str(result["type"])
    for attr in (
        'folder_id', 'folder_full_path', 'component_id', 'deleted',
        'description', 'folder_name', 'branch_id', 'branch_name',
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

    data = _raw_web_server_request(sdk, resource_id)
    return {
        "_success": True,
        "web_server": _web_server_to_dict(data),
    }


def _action_update_web_server(sdk: Boomi, profile: str, **kwargs) -> Dict[str, Any]:
    """Update shared web server settings."""
    resource_id = kwargs.get("resource_id")
    if not resource_id:
        return {"_success": False, "error": "resource_id (atom_id) is required for 'update_web_server' action"}

    # snake_case config keys → camelCase JSON keys
    GENERAL_MAP = {
        'base_url': 'baseUrl', 'api_type': 'apiType',
        'external_host': 'externalHost', 'internal_host': 'internalHost',
        'ssl_certificate': 'sslCertificate', 'max_number_of_threads': 'maxNumberOfThreads',
        'max_threads': 'maxNumberOfThreads',
    }
    PORT_MAP = {
        'port': 'port', 'ssl': 'ssl', 'external_port': 'externalPort',
        'external_ssl': 'externalSSL', 'auth_type': 'authType', 'enable_port': 'enablePort',
    }

    general_updates = {GENERAL_MAP[k]: v for k, v in kwargs.items() if k in GENERAL_MAP}
    port_updates = {PORT_MAP[k]: v for k, v in kwargs.items() if k in PORT_MAP}
    port_index = kwargs.get('port_index')
    auth_type_value = kwargs.get('auth_type')

    if not general_updates and not port_updates and auth_type_value is None:
        return {
            "_success": False,
            "error": "No valid update fields provided in config. "
                     "General fields: base_url, api_type, external_host, internal_host, "
                     "ssl_certificate, max_number_of_threads (or max_threads), auth_type. "
                     "Port fields: port, ssl, external_port, external_ssl, auth_type, enable_port",
        }

    # GET JSON, modify, POST back via the documented SharedWebServer JSON transport
    current = _raw_web_server_request(sdk, resource_id)

    # Cloud atoms use cloudTennantGeneral, local atoms use generalSettings
    gs = current.get("generalSettings") or current.get("cloudTennantGeneral")
    is_local = "generalSettings" in current and current["generalSettings"]

    if general_updates:
        if not gs:
            return {"_success": False, "error": "Server has no settings section to update"}
        gs.update(general_updates)

    # auth_type at general level: cloud stores directly, local nests under authentication
    if auth_type_value is not None:
        if not gs:
            return {"_success": False, "error": "Server has no settings section to update"}
        if is_local:
            auth_obj = gs.setdefault("authentication", {})
            auth_obj["authType"] = auth_type_value
        else:
            gs["authType"] = auth_type_value

    if port_updates:
        lp = gs.get("listenerPorts") if gs else None
        ports = lp.get("port") if lp else None
        if not ports:
            return {"_success": False, "error": "Server has no listener ports to update"}
        if port_index is not None:
            if not isinstance(port_index, int) or port_index < 0 or port_index >= len(ports):
                return {
                    "_success": False,
                    "error": f"port_index {port_index} is out of range (valid: 0–{len(ports) - 1})",
                }
        targets = [ports[port_index]] if port_index is not None else ports
        for p in targets:
            p.update(port_updates)

    updated = _raw_web_server_request(sdk, resource_id, method="POST", body=current)

    # Collect user-facing field names for the response
    updated_names = [k for k in kwargs if k in {*GENERAL_MAP, *PORT_MAP}]
    return {
        "_success": True,
        "web_server": _web_server_to_dict(updated),
        "updated_fields": updated_names,
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

    # The SharedCommunicationChannelComponent endpoint accepts JSON; SDK 3.0.0's
    # typed get is XML-only, so transport JSON and read the response dict.
    resp, status = component_family_json_request(
        sdk.shared_communication_channel_component,
        f"SharedCommunicationChannelComponent/{resource_id}", "GET"
    )
    if status and status >= 400:
        return {"_success": False, "error": _json_error_message(resp)}
    return {
        "_success": True,
        "channel": _channel_to_dict(resp),
    }


def _action_create_channel(sdk: Boomi, profile: str, **kwargs) -> Dict[str, Any]:
    """Create a new communication channel."""
    name = kwargs.get("name")
    channel_type = kwargs.get("channel_type") or kwargs.get("type")

    if not name:
        return {"_success": False, "error": "config.name is required for 'create_channel' action"}
    if not channel_type:
        return {"_success": False, "error": "config.channel_type is required (FTP, SFTP, HTTP, AS2, DISK, MLLP, OFTP)"}

    # Normalize to uppercase for Boomi API
    channel_type = channel_type.upper()
    valid_types = ('FTP', 'SFTP', 'HTTP', 'AS2', 'DISK', 'MLLP', 'OFTP')
    if channel_type not in valid_types:
        return {"_success": False, "error": f"Invalid channel_type: {channel_type}. Valid types: {', '.join(valid_types)}"}

    # Map user-facing names to SDK constructor params
    channel_kwargs = {"component_name": name, "communication_type": channel_type}

    # Pass through additional SDK-recognized fields
    for key in ("folder_id", "folder_name", "description"):
        val = kwargs.get(key)
        if val is not None:
            channel_kwargs[key] = val

    # Build PartnerCommunication with type-appropriate options so it serializes correctly
    comm_entry = _CHANNEL_TYPE_COMM_OPTIONS.get(channel_type)
    if comm_entry:
        kwarg_name, opts_cls = comm_entry
        pc = PartnerCommunication(**{kwarg_name: opts_cls()})
    else:
        pc = PartnerCommunication()

    channel = SharedCommunicationChannelComponent(
        partner_archiving=PartnerArchiving(),
        partner_communication=pc,
        **channel_kwargs,
    )
    # Transport the typed model as JSON (the SDK's typed create is XML-only).
    resp, status = component_family_json_request(
        sdk.shared_communication_channel_component,
        "SharedCommunicationChannelComponent", "POST", body=channel
    )
    if status and status >= 400:
        return {"_success": False, "error": _json_error_message(resp)}

    return {
        "_success": True,
        "channel": _channel_to_dict(resp),
    }


def _action_update_channel(sdk: Boomi, profile: str, **kwargs) -> Dict[str, Any]:
    """Update a shared communication channel by ID.

    Performs a GET-then-merge to preserve existing channel config (partner_archiving,
    partner_communication, etc.) when only metadata fields are being updated.
    """
    resource_id = kwargs.get("resource_id")
    if not resource_id:
        return {"_success": False, "error": "resource_id (channel_id) is required for 'update_channel' action"}

    # Fetch existing channel (JSON) to preserve its full config; merge on the
    # dict and POST it back (the SDK's typed get/update are XML-only).
    resp, status = component_family_json_request(
        sdk.shared_communication_channel_component,
        f"SharedCommunicationChannelComponent/{resource_id}", "GET"
    )
    if status and status >= 400:
        return {"_success": False, "error": _json_error_message(resp)}
    existing = resp if isinstance(resp, dict) else {}

    # Normalize channel_type / communication_type the same way as create
    new_type = kwargs.get("channel_type") or kwargs.get("communication_type")
    if new_type is not None:
        new_type = new_type.upper()
        valid_types = ('FTP', 'SFTP', 'HTTP', 'AS2', 'DISK', 'MLLP', 'OFTP')
        if new_type not in valid_types:
            return {"_success": False, "error": f"Invalid channel_type: {new_type}. Valid types: {', '.join(valid_types)}"}
        old_type = (existing.get("communicationType") or "").upper()
        existing["communicationType"] = new_type
        # Only rebuild partner_communication when the protocol actually changes;
        # a same-type update (e.g. "ftp" → "FTP") must preserve existing settings.
        # The typed model renders the correct PartnerCommunication wire dict.
        if new_type != old_type:
            comm_entry = _CHANNEL_TYPE_COMM_OPTIONS.get(new_type)
            if comm_entry:
                kwarg_name, opts_cls = comm_entry
                existing["PartnerCommunication"] = PartnerCommunication(**{kwarg_name: opts_cls()})._map()
            else:
                existing["PartnerCommunication"] = PartnerCommunication()._map()

    # Apply remaining user-provided overrides onto the existing dict (wire keys)
    for key, wire in (
        ("component_name", "componentName"), ("name", "componentName"),
        ("folder_id", "folderId"), ("folder_name", "folderName"),
        ("description", "description"),
    ):
        val = kwargs.get(key)
        if val is not None:
            existing[wire] = val

    resp2, status2 = component_family_json_request(
        sdk.shared_communication_channel_component,
        f"SharedCommunicationChannelComponent/{resource_id}", "POST", body=existing
    )
    if status2 and status2 >= 400:
        return {"_success": False, "error": _json_error_message(resp2)}
    return {
        "_success": True,
        "channel": _channel_to_dict(resp2),
    }


def _action_delete_channel(sdk: Boomi, profile: str, **kwargs) -> Dict[str, Any]:
    """Delete a shared communication channel by ID."""
    resource_id = kwargs.get("resource_id")
    if not resource_id:
        return {"_success": False, "error": "resource_id (channel_id) is required for 'delete_channel' action"}

    sdk.shared_communication_channel_component.delete_shared_communication_channel_component(
        id_=resource_id
    )
    return {
        "_success": True,
        "message": f"Channel {resource_id} deleted successfully",
    }


def _server_info_to_dict(info) -> Dict[str, Any]:
    """Convert SDK SharedServerInformation to plain dict."""
    result = {}
    for attr in (
        'api_type', 'atom_id', 'auth', 'auth_token',
        'check_forwarded_headers', 'external_host',
        'external_http_port', 'external_https_port',
        'http_port', 'https_port', 'internal_host',
        'max_threads', 'min_auth', 'override_url',
        'ssl_certificate_id', 'url',
    ):
        val = getattr(info, attr, None)
        if val is not None:
            if hasattr(val, 'value'):
                result[attr] = str(val.value)
            elif isinstance(val, (bool, int)):
                result[attr] = val
            else:
                result[attr] = str(val)
    return result


def _action_get_server_info(sdk: Boomi, profile: str, **kwargs) -> Dict[str, Any]:
    """Get shared server information for a runtime."""
    resource_id = kwargs.get("resource_id")
    if not resource_id:
        return {"_success": False, "error": "resource_id (atom_id) is required for 'get_server_info' action"}

    info = sdk.shared_server_information.get_shared_server_information(id_=resource_id)
    return {
        "_success": True,
        "server_info": _server_info_to_dict(info),
    }


def _action_update_server_info(sdk: Boomi, profile: str, **kwargs) -> Dict[str, Any]:
    """Update shared server information for a runtime."""
    resource_id = kwargs.get("resource_id")
    if not resource_id:
        return {"_success": False, "error": "resource_id (atom_id) is required for 'update_server_info' action"}

    # Build SharedServerInformation from kwargs
    info_kwargs = {}
    FIELD_MAP = {
        'api_type': 'api_type', 'auth': 'auth', 'min_auth': 'min_auth',
        'external_host': 'external_host', 'internal_host': 'internal_host',
        'http_port': 'http_port', 'https_port': 'https_port',
        'external_http_port': 'external_http_port',
        'external_https_port': 'external_https_port',
        'max_threads': 'max_threads', 'override_url': 'override_url',
        'check_forwarded_headers': 'check_forwarded_headers',
        'ssl_certificate_id': 'ssl_certificate_id', 'url': 'url',
    }
    for user_key, sdk_key in FIELD_MAP.items():
        val = kwargs.get(user_key)
        if val is not None:
            info_kwargs[sdk_key] = val

    if not info_kwargs:
        return {
            "_success": False,
            "error": "No valid update fields provided in config. "
                     "Valid fields: " + ", ".join(sorted(FIELD_MAP.keys())),
        }

    info = SharedServerInformation(**info_kwargs)
    info.atom_id = resource_id
    updated = sdk.shared_server_information.update_shared_server_information(
        id_=resource_id, request_body=info
    )
    return {
        "_success": True,
        "server_info": _server_info_to_dict(updated),
        "updated_fields": list(info_kwargs.keys()),
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
        action: One of: list_web_servers, get_web_server, update_web_server, list_channels,
            get_channel, create_channel, update_channel, delete_channel,
            get_server_info, update_server_info
        config_data: Action-specific configuration dict
        **kwargs: Additional parameters (resource_id, etc.)
    """
    if config_data is None:
        config_data = {}

    # Merge config_data into kwargs
    merged = {**config_data, **kwargs}

    actions = {
        "list_web_servers": _action_list_web_servers,
        "get_web_server": _action_list_web_servers,
        "update_web_server": _action_update_web_server,
        "list_channels": _action_list_channels,
        "get_channel": _action_get_channel,
        "create_channel": _action_create_channel,
        "update_channel": _action_update_channel,
        "delete_channel": _action_delete_channel,
        "get_server_info": _action_get_server_info,
        "update_server_info": _action_update_server_info,
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
