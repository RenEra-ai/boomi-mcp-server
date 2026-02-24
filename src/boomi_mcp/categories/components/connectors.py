"""
Connector Component MCP Tools for Boomi API Integration.

Phase 1 (MVP) — read-only actions for connector discovery and retrieval:
- list_types: List available connector types in the Boomi account
- get_type: Get field definitions for a specific connector type
- list: List connector components (connections and operations)
- get: Get a single connector component with full XML
"""

from typing import Dict, Any, Optional

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

from ._shared import component_get_xml, paginate_metadata


# ============================================================================
# Actions
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

        else:
            return {
                "_success": False,
                "error": f"Unknown action: {action}",
                "hint": "Valid actions are: list_types, get_type, list, get",
            }

    except Exception as e:
        return {
            "_success": False,
            "error": f"Action '{action}' failed: {str(e)}",
            "exception_type": type(e).__name__,
        }


__all__ = ['manage_connector_action']
