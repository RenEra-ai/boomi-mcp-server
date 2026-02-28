"""
Connector Component MCP Tools for Boomi API Integration.

Actions:
- list_types: List available connector types in the Boomi account
- get_type: Get field definitions for a specific connector type
- list: List connector components (connections and operations)
- get: Get a single connector component with full XML
- create: Create a new connector from builder config or raw XML
- update: Update connector fields or replace with raw XML
- delete: Soft-delete a connector component
"""

from typing import Dict, Any, Optional
import xml.etree.ElementTree as ET

from boomi import Boomi
from boomi.models import (
    ComponentMetadataQueryConfig,
    ComponentMetadataQueryConfigQueryFilter,
    ComponentMetadataSimpleExpression,
    ComponentMetadataSimpleExpressionOperator,
    ComponentMetadataSimpleExpressionProperty,
    ComponentMetadataGroupingExpression,
    ComponentMetadataGroupingExpressionOperator,
    ConnectorQueryConfig,
    ConnectorQueryConfigQueryFilter,
    ConnectorSimpleExpression,
    ConnectorSimpleExpressionOperator,
    ConnectorSimpleExpressionProperty,
)
from boomi.net.transport.serializer import Serializer
from boomi.net.environment.environment import Environment

from ._shared import component_get_xml, set_description_element, soft_delete_component, paginate_metadata
from .builders.connector_builder import (
    get_connector_builder, CONNECTOR_BUILDERS,
    find_http_settings, update_http_settings_fields,
)


# ============================================================================
# Read Actions
# ============================================================================

def list_connector_types(
    boomi_client: Boomi,
) -> Dict[str, Any]:
    """List all available connector types in the Boomi account.

    Uses sdk.connector.query_connector() with a LIKE '%' query.
    """
    try:
        expression = ConnectorSimpleExpression(
            operator=ConnectorSimpleExpressionOperator.LIKE,
            property=ConnectorSimpleExpressionProperty.NAME,
            argument=["%"],
        )
        query_filter = ConnectorQueryConfigQueryFilter(expression=expression)
        query_config = ConnectorQueryConfig(query_filter=query_filter)

        result = boomi_client.connector.query_connector(request_body=query_config)

        types = []
        if hasattr(result, 'result') and result.result:
            for conn in result.result:
                types.append({
                    'name': getattr(conn, 'name', ''),
                    'type': getattr(conn, 'type_', '') or getattr(conn, 'type', ''),
                })

        # Paginate
        while hasattr(result, 'query_token') and result.query_token:
            result = boomi_client.connector.query_more_connector(
                request_body=result.query_token
            )
            if hasattr(result, 'result') and result.result:
                for conn in result.result:
                    types.append({
                        'name': getattr(conn, 'name', ''),
                        'type': getattr(conn, 'type_', '') or getattr(conn, 'type', ''),
                    })

        return {
            "_success": True,
            "total_count": len(types),
            "types": types,
        }

    except Exception as e:
        return {
            "_success": False,
            "error": f"Failed to list connector types: {str(e)}",
            "exception_type": type(e).__name__,
        }


def get_connector_type(
    boomi_client: Boomi,
    connector_type: str,
) -> Dict[str, Any]:
    """Get field definitions for a specific connector type.

    Uses sdk.connector.get_connector(connector_type).
    """
    try:
        result = boomi_client.connector.get_connector(connector_type)

        type_info = {
            'name': getattr(result, 'name', ''),
            'type': getattr(result, 'type_', '') or getattr(result, 'type', ''),
        }

        # Include field definitions if available
        if hasattr(result, 'field') and result.field:
            type_info['fields'] = []
            for field in result.field:
                field_dict = {
                    'id': getattr(field, 'id_', '') or getattr(field, 'id', ''),
                    'label': getattr(field, 'label', ''),
                    'type': getattr(field, 'type_', '') or getattr(field, 'type', ''),
                    'help_text': getattr(field, 'help_text', '') or getattr(field, 'helpText', ''),
                }
                if hasattr(field, 'default_value') and field.default_value:
                    field_dict['default_value'] = field.default_value
                if hasattr(field, 'allowed_value') and field.allowed_value:
                    field_dict['allowed_values'] = [
                        {'label': getattr(av, 'label', ''), 'value': getattr(av, 'value', '')}
                        for av in field.allowed_value
                    ]
                type_info['fields'].append(field_dict)

        # Include operation types if available
        if hasattr(result, 'operation_type') and result.operation_type:
            type_info['operation_types'] = []
            for op in result.operation_type:
                type_info['operation_types'].append({
                    'type': getattr(op, 'type_', '') or getattr(op, 'type', ''),
                })

        return {
            "_success": True,
            "connector_type": type_info,
        }

    except Exception as e:
        return {
            "_success": False,
            "error": f"Failed to get connector type '{connector_type}': {str(e)}",
            "exception_type": type(e).__name__,
            "hint": "Use action='list_types' to see available connector types",
        }


def list_connectors(
    boomi_client: Boomi,
    profile: str,
    filters: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """List connector components (connections and/or operations).

    Filters:
        component_type: "connection" (connector-settings) or "operation" (connector-action)
        connector_type: sub-type filter (e.g., "http", "database", "sftp")
        folder_name: client-side folder filter
        show_all: include deleted/non-current versions
    """
    try:
        show_all = False
        if filters:
            show_all = filters.get('show_all', False)

        # Determine component type(s) to query
        comp_type = None
        if filters and filters.get('component_type'):
            ct = filters['component_type'].lower()
            if ct in ('connection', 'connector-settings'):
                comp_type = 'connector-settings'
            elif ct in ('operation', 'connector-action'):
                comp_type = 'connector-action'
            else:
                return {
                    "_success": False,
                    "error": f"Invalid component_type: '{filters['component_type']}'",
                    "hint": "Valid values: 'connection' (connector-settings) or 'operation' (connector-action)",
                }

        # Build expressions
        expressions = []

        if comp_type:
            expressions.append(ComponentMetadataSimpleExpression(
                operator=ComponentMetadataSimpleExpressionOperator.EQUALS,
                property=ComponentMetadataSimpleExpressionProperty.TYPE,
                argument=[comp_type],
            ))
        else:
            # Query both connector types
            expressions.append(ComponentMetadataGroupingExpression(
                operator=ComponentMetadataGroupingExpressionOperator.OR,
                nested_expression=[
                    ComponentMetadataSimpleExpression(
                        operator=ComponentMetadataSimpleExpressionOperator.EQUALS,
                        property=ComponentMetadataSimpleExpressionProperty.TYPE,
                        argument=['connector-settings'],
                    ),
                    ComponentMetadataSimpleExpression(
                        operator=ComponentMetadataSimpleExpressionOperator.EQUALS,
                        property=ComponentMetadataSimpleExpressionProperty.TYPE,
                        argument=['connector-action'],
                    ),
                ],
            ))

        # Sub-type filter for connector_type (e.g., "http")
        if filters and filters.get('connector_type'):
            expressions.append(ComponentMetadataSimpleExpression(
                operator=ComponentMetadataSimpleExpressionOperator.EQUALS,
                property=ComponentMetadataSimpleExpressionProperty.SUBTYPE,
                argument=[filters['connector_type']],
            ))

        # Combine expressions
        if len(expressions) == 1:
            root_expr = expressions[0]
        else:
            root_expr = ComponentMetadataGroupingExpression(
                operator=ComponentMetadataGroupingExpressionOperator.AND,
                nested_expression=expressions,
            )

        query_filter = ComponentMetadataQueryConfigQueryFilter(expression=root_expr)
        query_config = ComponentMetadataQueryConfig(query_filter=query_filter)

        components = paginate_metadata(boomi_client, query_config, show_all=show_all)

        # Client-side folder filter
        if filters and filters.get('folder_name'):
            folder = filters['folder_name']
            components = [c for c in components if c.get('folder_name') == folder]

        return {
            "_success": True,
            "total_count": len(components),
            "connectors": components,
            "profile": profile,
        }

    except Exception as e:
        return {
            "_success": False,
            "error": f"Failed to list connectors: {str(e)}",
            "exception_type": type(e).__name__,
        }


def get_connector(
    boomi_client: Boomi,
    profile: str,
    component_id: str,
) -> Dict[str, Any]:
    """Get a single connector component by ID with full XML."""
    try:
        comp_data = component_get_xml(boomi_client, component_id)
        return {
            "_success": True,
            "connector": comp_data,
            "profile": profile,
        }
    except Exception as e:
        return {
            "_success": False,
            "error": f"Failed to get connector '{component_id}': {str(e)}",
            "exception_type": type(e).__name__,
            "hint": "Verify the component ID exists and is a connector component",
        }


# ============================================================================
# Write Actions
# ============================================================================

def _create_component_raw(boomi_client: Boomi, xml: str) -> Dict[str, Any]:
    """Create a component via raw POST, returning parsed XML response.

    The SDK's create_component() fails to parse GenericConnectionConfig responses,
    so we use the Serializer directly (same approach as component_get_xml).
    """
    svc = boomi_client.component
    serialized_request = (
        Serializer(
            f"{svc.base_url or Environment.DEFAULT.url}/Component",
            [svc.get_access_token(), svc.get_basic_auth()],
        )
        .add_header("Accept", "application/xml")
        .add_header("Content-Type", "application/xml")
        .serialize()
        .set_method("POST")
    )
    serialized_request.body = xml.encode('utf-8') if isinstance(xml, str) else xml
    response, status, content = svc.send_request(serialized_request)

    if status >= 400:
        raw = response if isinstance(response, str) else response.decode('utf-8')
        raise Exception(f"Create failed: HTTP {status} — {raw}")

    raw_xml = response if isinstance(response, str) else response.decode('utf-8')
    root = ET.fromstring(raw_xml)

    return {
        'component_id': root.attrib.get('componentId', ''),
        'name': root.attrib.get('name', ''),
        'type': root.attrib.get('type', ''),
        'sub_type': root.attrib.get('subType', ''),
        'folder_name': root.attrib.get('folderName', ''),
        'version': root.attrib.get('version', ''),
    }


def create_connector(
    boomi_client: Boomi,
    profile: str,
    config: Dict[str, Any],
) -> Dict[str, Any]:
    """Create a new connector component.

    Two paths:
    1. Raw XML — if config["xml"] provided, POST directly (any connector type)
    2. Builder — if config["connector_type"] provided (e.g., "http"), use builder
    """
    try:
        # Path 1: raw XML
        if config.get('xml'):
            result = _create_component_raw(boomi_client, config['xml'])
            return {
                "_success": True,
                "message": f"Created connector '{result['name']}'",
                "component_id": result['component_id'],
                "name": result['name'],
                "type": result['type'],
                "sub_type": result['sub_type'],
                "profile": profile,
            }

        # Path 2: builder
        connector_type = config.get('connector_type')
        if not connector_type:
            supported = ', '.join(CONNECTOR_BUILDERS.keys())
            return {
                "_success": False,
                "error": "Either 'xml' or 'connector_type' is required in config",
                "hint": (
                    f"Supported builder types: {supported}. "
                    "Or provide raw XML from an existing connector "
                    "(use action='get' to export XML template)."
                ),
            }

        builder = get_connector_builder(connector_type)
        if not builder:
            supported = ', '.join(CONNECTOR_BUILDERS.keys())
            return {
                "_success": False,
                "error": f"No builder available for connector type '{connector_type}'",
                "hint": (
                    f"Supported builder types: {supported}. "
                    "For unsupported types, use action='get' on an existing connector "
                    "to export XML, then pass as config.xml."
                ),
            }

        if not config.get('component_name'):
            return {
                "_success": False,
                "error": "component_name is required for builder-based creation",
                "hint": f'Provide config: {{"connector_type": "{connector_type}", "component_name": "My Connection", "url": "https://..."}}',
            }

        xml = builder.build(**config)
        result = _create_component_raw(boomi_client, xml)

        return {
            "_success": True,
            "message": f"Created {connector_type} connector '{result['name']}'",
            "component_id": result['component_id'],
            "name": result['name'],
            "type": result['type'],
            "sub_type": result['sub_type'],
            "profile": profile,
        }

    except ValueError as e:
        return {
            "_success": False,
            "error": f"Validation error: {str(e)}",
        }
    except Exception as e:
        return {
            "_success": False,
            "error": f"Failed to create connector: {str(e)}",
            "exception_type": type(e).__name__,
        }


def update_connector(
    boomi_client: Boomi,
    profile: str,
    component_id: str,
    config: Dict[str, Any],
) -> Dict[str, Any]:
    """Update an existing connector component.

    Two paths:
    1. Raw XML — if config["xml"] provided, full replacement
    2. Smart merge — update specific fields in existing XML
       - Component-level: name, description, folder_name
       - Connector-specific: HttpSettings attributes (url, auth_type, etc.)
    """
    try:
        # Path 1: raw XML replacement
        if config.get('xml'):
            boomi_client.component.update_component_raw(component_id, config['xml'])
            return {
                "_success": True,
                "message": f"Updated connector '{component_id}' with provided XML",
                "component_id": component_id,
                "profile": profile,
            }

        # Path 2: smart merge
        current = component_get_xml(boomi_client, component_id)
        raw_xml = current['xml']
        root = ET.fromstring(raw_xml)

        changed = False

        # Component-level updates
        if config.get('name') or config.get('component_name'):
            root.set('name', config.get('name') or config['component_name'])
            changed = True
        if config.get('folder_name'):
            root.set('folderName', config['folder_name'])
            changed = True
        if config.get('folder_id'):
            root.set('folderId', config['folder_id'])
            changed = True
        if 'description' in config:
            set_description_element(root, config['description'])
            changed = True

        # Connector-specific field updates (HttpSettings)
        ns = {'bns': 'http://api.platform.boomi.com/'}
        obj_elem = root.find('bns:object', ns)
        if obj_elem is None:
            obj_elem = root.find('object')

        if obj_elem is not None:
            http_settings = find_http_settings(obj_elem)
            if http_settings is not None:
                if update_http_settings_fields(http_settings, config):
                    changed = True

        if not changed:
            return {
                "_success": False,
                "error": "No updatable fields provided in config",
                "hint": "Provide name, description, folder_name, url, auth_type, username, trust_all_certs, or xml",
            }

        modified_xml = ET.tostring(root, encoding='unicode', xml_declaration=True)
        boomi_client.component.update_component_raw(component_id, modified_xml)

        return {
            "_success": True,
            "message": f"Updated connector '{current['name']}'",
            "component_id": component_id,
            "profile": profile,
        }

    except Exception as e:
        return {
            "_success": False,
            "error": f"Failed to update connector '{component_id}': {str(e)}",
            "exception_type": type(e).__name__,
        }


def delete_connector(
    boomi_client: Boomi,
    profile: str,
    component_id: str,
) -> Dict[str, Any]:
    """Soft-delete a connector component."""
    try:
        result = soft_delete_component(boomi_client, component_id)
        warning = "Dependent components (operations, processes) are NOT automatically deleted."
        if result.get("verify_warning"):
            warning += f" {result['verify_warning']}. Verify in Boomi Platform UI."
        resp = {
            "_success": True,
            "message": f"Deleted connector '{result['component_name']}'",
            "component_id": component_id,
            "profile": profile,
            "method": result["method"],
            "warning": warning,
        }
        if result["method"] == "metadata_delete":
            resp["warning"] = "Used metadata API delete (not soft-delete). This may be irreversible."
        return resp

    except Exception as e:
        return {
            "_success": False,
            "error": f"Failed to delete connector '{component_id}': {str(e)}",
            "exception_type": type(e).__name__,
            "hint": "Retry or use Boomi Platform UI to delete this component.",
        }


# ============================================================================
# Action Router
# ============================================================================

def manage_connector_action(
    boomi_client: Boomi,
    profile: str,
    action: str,
    **params,
) -> Dict[str, Any]:
    """Route manage_connector actions."""
    try:
        if action == "list_types":
            return list_connector_types(boomi_client)

        elif action == "get_type":
            connector_type = params.get("connector_type")
            if not connector_type:
                return {
                    "_success": False,
                    "error": "connector_type is required for 'get_type' action",
                    "hint": 'Provide config=\'{"connector_type": "http"}\'. '
                            "Use action='list_types' to see available types.",
                }
            return get_connector_type(boomi_client, connector_type)

        elif action == "list":
            filters = params.get("filters", None)
            return list_connectors(boomi_client, profile, filters)

        elif action == "get":
            component_id = params.get("component_id")
            if not component_id:
                return {
                    "_success": False,
                    "error": "component_id is required for 'get' action",
                    "hint": "Provide the connector component ID to retrieve",
                }
            return get_connector(boomi_client, profile, component_id)

        elif action == "create":
            config = params.get("config")
            if not config:
                supported = ', '.join(CONNECTOR_BUILDERS.keys())
                return {
                    "_success": False,
                    "error": "config is required for 'create' action",
                    "hint": (
                        f'Provide config with connector_type and fields: '
                        f'{{"connector_type": "http", "component_name": "My Connection", "url": "https://..."}}'
                        f' Supported builder types: {supported}. Or provide raw XML as config.xml.'
                    ),
                }
            return create_connector(boomi_client, profile, config)

        elif action == "update":
            component_id = params.get("component_id")
            if not component_id:
                return {
                    "_success": False,
                    "error": "component_id is required for 'update' action",
                    "hint": "Provide the connector component ID to update",
                }
            config = params.get("config")
            if not config:
                return {
                    "_success": False,
                    "error": "config is required for 'update' action",
                    "hint": 'Provide config with fields to update: {"url": "https://new-url.com"}',
                }
            return update_connector(boomi_client, profile, component_id, config)

        elif action == "delete":
            component_id = params.get("component_id")
            if not component_id:
                return {
                    "_success": False,
                    "error": "component_id is required for 'delete' action",
                    "hint": "Provide the connector component ID to delete",
                }
            return delete_connector(boomi_client, profile, component_id)

        else:
            return {
                "_success": False,
                "error": f"Unknown action: {action}",
                "hint": "Valid actions are: list_types, get_type, list, get, create, update, delete",
            }

    except Exception as e:
        return {
            "_success": False,
            "error": f"Action '{action}' failed: {str(e)}",
            "exception_type": type(e).__name__,
        }


__all__ = ['manage_connector_action']
