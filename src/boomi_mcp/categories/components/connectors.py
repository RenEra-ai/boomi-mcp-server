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
from boomi.net.transport.api_error import ApiError
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
from ._shared import (
    component_get_xml, set_description_element, soft_delete_component,
    paginate_metadata, _create_component_raw, _extract_api_error_msg,
)
from .builders.connector_builder import (
    BuilderValidationError,
    DatabaseGetOperationBuilder,
    RestClientOperationBuilder,
    _resolve_rest_connector_type,
    get_connector_builder, CONNECTOR_BUILDERS,
    get_connector_action_builder, CONNECTOR_ACTION_BUILDERS,
    find_http_settings, update_http_settings_fields,
)


def _missing_component_name_hint(connector_type: str) -> str:
    """Build a connector_type-aware 'config example' hint for the
    component_name-missing error path. Steers REST callers toward the
    base_url/auth/oauth2 shape (not the legacy HTTP url/auth_type shape)."""
    if _resolve_rest_connector_type(connector_type) is not None:
        return (
            f'Provide config: {{"connector_type": "{connector_type}", '
            '"component_name": "My REST Connection", '
            '"base_url": "https://api.example.com", '
            '"auth": "OAUTH2", "oauth2": {"grant_type": "client_credentials", '
            '"client_id": "<<client id>>", '
            '"client_secret_ref": "credential://<<vendor>>/oauth-client-secret", '
            '"access_token_url": "https://api.example.com/oauth/token"}}'
        )
    if connector_type and connector_type.lower() == "database":
        return (
            f'Provide config: {{"connector_type": "{connector_type}", '
            '"component_name": "My DB Connection", '
            '"driver_id": "sqlserver", "auth_mode": "username_password", '
            '"host": "host.docker.internal", "dbname": "MyDB", '
            '"username": "sa", '
            '"credential_ref": "credential://<<vendor>>/sqlserver/password"}}'
        )
    return (
        f'Provide config with connector_type and required fields. '
        f'connector_type="{connector_type}" — call '
        'get_schema_template(resource_type="component", operation="create", '
        f'component_type="connector-settings") for the protocol-specific shape.'
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

    Uses sdk.connector.get_connector(connector_type). REST Client aliases
    ('rest' / 'rest_client') are normalized to the canonical Boomi subtype
    `officialboomi-X3979C-rest-prod` before the SDK call — Boomi's
    catalog doesn't know our local aliases.
    """
    rest_canonical = _resolve_rest_connector_type(connector_type)
    if rest_canonical is not None:
        connector_type = rest_canonical
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

    except ApiError as e:
        return {
            "_success": False,
            "error": f"Failed to get connector type '{connector_type}': {_extract_api_error_msg(e)}",
            "exception_type": type(e).__name__,
            "hint": "Use action='list_types' to see available connector types",
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

        # Sub-type filter for connector_type. REST Client aliases ('rest' /
        # 'rest_client') resolve to the canonical Boomi subtype before the
        # query is built — Boomi components carry the canonical subType
        # value, not our local alias.
        if filters and filters.get('connector_type'):
            connector_type_filter = filters['connector_type']
            rest_canonical = _resolve_rest_connector_type(connector_type_filter)
            if rest_canonical is not None:
                connector_type_filter = rest_canonical
            expressions.append(ComponentMetadataSimpleExpression(
                operator=ComponentMetadataSimpleExpressionOperator.EQUALS,
                property=ComponentMetadataSimpleExpressionProperty.SUBTYPE,
                argument=[connector_type_filter],
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
    except ApiError as e:
        return {
            "_success": False,
            "error": f"Failed to get connector '{component_id}': {_extract_api_error_msg(e)}",
            "exception_type": type(e).__name__,
            "hint": "Verify the component ID exists and is a connector component",
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

        # 2a) connector-action dispatch (e.g. database Get operation).
        # Keyed by (connector_type, operation_mode) so a single connector
        # family can host multiple action shapes (get, send, etc.).
        component_type = (config.get('component_type') or '').lower()
        if component_type == 'connector-action':
            operation_mode = config.get('operation_mode') or ''
            action_builder = get_connector_action_builder(
                connector_type, operation_mode
            )
            if not action_builder:
                # For known connector families (e.g. database, rest), let the
                # family's validator surface the proper structured error —
                # otherwise a deliberate `operation_mode="get"` on a REST
                # action ends up with a generic "no builder" message instead
                # of the documented UNSUPPORTED_REST_OPERATION_MODE hint.
                if connector_type.lower() == 'database':
                    db_err = DatabaseGetOperationBuilder.validate_config(config)
                    if db_err is not None:
                        raise db_err
                if _resolve_rest_connector_type(connector_type) is not None:
                    rest_err = RestClientOperationBuilder.validate_config(config)
                    if rest_err is not None:
                        raise rest_err
                supported_pairs = ', '.join(
                    f"{ct}.{om}" for (ct, om) in sorted(CONNECTOR_ACTION_BUILDERS.keys())
                )
                return {
                    "_success": False,
                    "error": (
                        f"No connector-action builder for connector_type="
                        f"{connector_type!r} operation_mode={operation_mode!r}"
                    ),
                    "hint": (
                        f"Supported (connector_type, operation_mode) pairs: "
                        f"{supported_pairs}. For unsupported pairs, use "
                        "action='get' on an existing connector-action to "
                        "export XML, then pass as config.xml."
                    ),
                }
            if not config.get('component_name'):
                return {
                    "_success": False,
                    "error": "component_name is required for builder-based creation",
                    "hint": (
                        f'Provide config: {{"component_type": "connector-action", '
                        f'"connector_type": "{connector_type}", "operation_mode": '
                        f'"{operation_mode}", "component_name": "My Operation", ...}}'
                    ),
                }
            xml = action_builder.build(**config)
            result = _create_component_raw(boomi_client, xml)
            return {
                "_success": True,
                "message": (
                    f"Created {connector_type} {operation_mode} operation "
                    f"'{result['name']}'"
                ),
                "component_id": result['component_id'],
                "name": result['name'],
                "type": result['type'],
                "sub_type": result['sub_type'],
                "profile": profile,
            }

        # 2b) connector-settings dispatch (existing behavior).
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
                "hint": _missing_component_name_hint(connector_type),
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

    except BuilderValidationError as e:
        return {
            "_success": False,
            "error_code": e.error_code,
            "error": str(e),
            "field": e.field,
            "hint": e.hint,
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

    except ApiError as e:
        return {
            "_success": False,
            "error": f"Failed to update connector '{component_id}': {_extract_api_error_msg(e)}",
            "exception_type": type(e).__name__,
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
        return {
            "_success": True,
            "message": f"Deleted connector '{result['component_name']}'",
            "component_id": component_id,
            "profile": profile,
            "method": result["method"],
            "warning": "Dependent components (operations, processes) are NOT automatically deleted.",
        }

    except ApiError as e:
        return {
            "_success": False,
            "error": f"Failed to delete connector '{component_id}': {_extract_api_error_msg(e)}",
            "exception_type": type(e).__name__,
            "hint": "Retry or use Boomi Platform UI to delete this component.",
        }
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
                    "hint": (
                        'Provide config=\'{"connector_type": "rest"}\' '
                        "(REST Client, the M2 target) or "
                        'config=\'{"connector_type": "officialboomi-X3979C-rest-prod"}\' '
                        "for the canonical subtype. Use action='list_types' "
                        "to see all available connector types in this account."
                    ),
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
                        'Provide config with connector_type and fields. '
                        'Example (REST Client, the M2 target): '
                        '{"connector_type": "rest", "component_name": "Target REST Connection", '
                        '"base_url": "https://api.example.com", "auth": "OAUTH2", '
                        '"oauth2": {"grant_type": "client_credentials", '
                        '"client_id": "<<client id>>", '
                        '"client_secret_ref": "credential://<<vendor>>/oauth-client-secret", '
                        '"access_token_url": "https://api.example.com/oauth/token"}}. '
                        f'Supported builder types: {supported}. '
                        'Or provide raw XML as config.xml.'
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
                    "hint": (
                        "Provide config with fields to update. Universally "
                        "supported component-level fields: name, description, "
                        "folder_name, folder_id. For HTTP Client connections, "
                        "smart-merge also handles HttpSettings attributes "
                        "(url, auth_type, username, trust_all_certs, "
                        "client_ssl_alias). For REST Client / database / "
                        "other field-level edits, use the raw-XML escape "
                        "hatch: config={\"xml\": \"<bns:Component ...full XML...>\"}."
                    ),
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
