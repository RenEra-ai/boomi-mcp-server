"""
Component Query MCP Tools for Boomi API Integration.

Provides component discovery and retrieval capabilities:
- list: List all components (optionally filtered by type)
- get: Get a single component by ID with full XML
- search: Multi-field search with AND logic
- bulk_get: Retrieve up to 5 components in one call
"""

from typing import Dict, Any, List, Optional
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
)

from ._shared import component_get_xml


# ============================================================================
# Helper: paginate component metadata queries
# ============================================================================

def _paginate_metadata(boomi_client: Boomi, query_config, show_all: bool = False) -> List[Dict[str, Any]]:
    """Execute a metadata query with pagination. Returns list of component dicts."""
    result = boomi_client.component_metadata.query_component_metadata(
        request_body=query_config
    )

    components = []
    if hasattr(result, 'result') and result.result:
        for comp in result.result:
            components.append(_metadata_to_dict(comp))

    # Paginate
    while hasattr(result, 'query_token') and result.query_token:
        result = boomi_client.component_metadata.query_more_component_metadata(
            request_body=result.query_token
        )
        if hasattr(result, 'result') and result.result:
            for comp in result.result:
                components.append(_metadata_to_dict(comp))

    # Client-side filter: current version, not deleted (unless show_all)
    if not show_all:
        components = [
            c for c in components
            if str(c.get('current_version', 'false')).lower() == 'true'
            and str(c.get('deleted', 'true')).lower() == 'false'
        ]

    return components


def _metadata_to_dict(comp) -> Dict[str, Any]:
    """Convert a ComponentMetadata SDK object to a plain dict."""
    return {
        'component_id': getattr(comp, 'component_id', ''),
        'id': getattr(comp, 'id_', ''),
        'name': getattr(comp, 'name', ''),
        'folder_name': getattr(comp, 'folder_name', ''),
        'type': getattr(comp, 'type_', ''),
        'version': getattr(comp, 'version', ''),
        'current_version': str(getattr(comp, 'current_version', 'false')),
        'deleted': str(getattr(comp, 'deleted', 'false')),
        'created_date': getattr(comp, 'created_date', ''),
        'modified_date': getattr(comp, 'modified_date', ''),
        'created_by': getattr(comp, 'created_by', ''),
        'modified_by': getattr(comp, 'modified_by', ''),
    }


# ============================================================================
# Actions
# ============================================================================

def list_components(
    boomi_client: Boomi,
    profile: str,
    filters: Optional[Dict[str, Any]] = None
) -> Dict[str, Any]:
    """List all components, optionally filtered by type."""
    try:
        show_all = False
        if filters:
            show_all = filters.get('show_all', False)

        comp_type = filters.get('type') if filters else None

        if comp_type:
            expression = ComponentMetadataSimpleExpression(
                operator=ComponentMetadataSimpleExpressionOperator.EQUALS,
                property=ComponentMetadataSimpleExpressionProperty.TYPE,
                argument=[comp_type]
            )
        else:
            expression = ComponentMetadataSimpleExpression(
                operator=ComponentMetadataSimpleExpressionOperator.LIKE,
                property=ComponentMetadataSimpleExpressionProperty.NAME,
                argument=["%"]
            )

        query_filter = ComponentMetadataQueryConfigQueryFilter(expression=expression)
        query_config = ComponentMetadataQueryConfig(query_filter=query_filter)

        components = _paginate_metadata(boomi_client, query_config, show_all=show_all)

        # Client-side folder filter
        if filters and filters.get('folder_name'):
            folder = filters['folder_name']
            components = [c for c in components if c.get('folder_name') == folder]

        return {
            "_success": True,
            "total_count": len(components),
            "components": components,
            "profile": profile,
        }

    except Exception as e:
        return {
            "_success": False,
            "error": f"Failed to list components: {str(e)}",
            "exception_type": type(e).__name__,
        }


def get_component(
    boomi_client: Boomi,
    profile: str,
    component_id: str
) -> Dict[str, Any]:
    """Get a single component by ID with full XML."""
    try:
        comp_data = component_get_xml(boomi_client, component_id)
        return {
            "_success": True,
            "component": comp_data,
            "profile": profile,
        }
    except Exception as e:
        return {
            "_success": False,
            "error": f"Failed to get component '{component_id}': {str(e)}",
            "exception_type": type(e).__name__,
            "hint": "Verify the component ID exists and is accessible",
        }


def search_components(
    boomi_client: Boomi,
    profile: str,
    filters: Dict[str, Any]
) -> Dict[str, Any]:
    """Multi-field component search with AND logic."""
    try:
        expressions = []

        if filters.get('name'):
            expressions.append(ComponentMetadataSimpleExpression(
                operator=ComponentMetadataSimpleExpressionOperator.LIKE,
                property=ComponentMetadataSimpleExpressionProperty.NAME,
                argument=[filters['name']]
            ))

        if filters.get('type'):
            expressions.append(ComponentMetadataSimpleExpression(
                operator=ComponentMetadataSimpleExpressionOperator.EQUALS,
                property=ComponentMetadataSimpleExpressionProperty.TYPE,
                argument=[filters['type']]
            ))

        if not expressions:
            # Fallback: match all
            expressions.append(ComponentMetadataSimpleExpression(
                operator=ComponentMetadataSimpleExpressionOperator.LIKE,
                property=ComponentMetadataSimpleExpressionProperty.NAME,
                argument=["%"]
            ))

        if len(expressions) == 1:
            root_expr = expressions[0]
        else:
            root_expr = ComponentMetadataGroupingExpression(
                operator=ComponentMetadataGroupingExpressionOperator.AND,
                nested_expression=expressions
            )

        query_filter = ComponentMetadataQueryConfigQueryFilter(expression=root_expr)
        query_config = ComponentMetadataQueryConfig(query_filter=query_filter)

        show_all = filters.get('show_all', False)
        components = _paginate_metadata(boomi_client, query_config, show_all=show_all)

        # Client-side folder filter
        if filters.get('folder_name'):
            folder = filters['folder_name']
            components = [c for c in components if c.get('folder_name') == folder]

        return {
            "_success": True,
            "total_count": len(components),
            "components": components,
            "profile": profile,
            "filters_applied": {k: v for k, v in filters.items() if v},
        }

    except Exception as e:
        return {
            "_success": False,
            "error": f"Failed to search components: {str(e)}",
            "exception_type": type(e).__name__,
        }


def bulk_get_components(
    boomi_client: Boomi,
    profile: str,
    component_ids: List[str]
) -> Dict[str, Any]:
    """Retrieve up to 5 components by their IDs.

    Note: Boomi's bulk Component endpoint returns 406 when SDK sends
    Accept: application/json. We use individual component_get_xml() calls
    instead, which is still efficient for up to 5 components.
    """
    try:
        if not component_ids:
            return {"_success": False, "error": "component_ids list is empty"}

        if len(component_ids) > 5:
            return {
                "_success": False,
                "error": f"Maximum 5 components per bulk request (got {len(component_ids)})",
                "hint": "Split into multiple bulk_get calls of 5 or fewer IDs",
            }

        components = []
        errors = []
        for cid in component_ids:
            try:
                comp = component_get_xml(boomi_client, cid)
                # Remove full XML from bulk response to keep it lighter
                comp_summary = {k: v for k, v in comp.items() if k != 'xml'}
                components.append(comp_summary)
            except Exception as e:
                errors.append({'component_id': cid, 'error': str(e)})

        result = {
            "_success": True,
            "total_count": len(components),
            "components": components,
            "profile": profile,
        }
        if errors:
            result["errors"] = errors

        return result

    except Exception as e:
        return {
            "_success": False,
            "error": f"Failed to bulk get components: {str(e)}",
            "exception_type": type(e).__name__,
        }


# ============================================================================
# Action Router
# ============================================================================

def query_components_action(
    boomi_client: Boomi,
    profile: str,
    action: str,
    **params
) -> Dict[str, Any]:
    """Route query_components actions."""
    try:
        if action == "list":
            filters = params.get("filters", None)
            return list_components(boomi_client, profile, filters)

        elif action == "get":
            component_id = params.get("component_id")
            if not component_id:
                return {
                    "_success": False,
                    "error": "component_id is required for 'get' action",
                    "hint": "Provide the component ID to retrieve",
                }
            return get_component(boomi_client, profile, component_id)

        elif action == "search":
            filters = params.get("filters")
            if not filters:
                return {
                    "_success": False,
                    "error": "config with search filters is required for 'search' action",
                    "hint": 'Provide config like: {"name": "%Test%", "type": "process"}',
                }
            return search_components(boomi_client, profile, filters)

        elif action == "bulk_get":
            component_ids = params.get("component_ids")
            if not component_ids:
                return {
                    "_success": False,
                    "error": "component_ids is required for 'bulk_get' action",
                    "hint": 'Provide component_ids as a JSON array: ["id1", "id2"]',
                }
            return bulk_get_components(boomi_client, profile, component_ids)

        else:
            return {
                "_success": False,
                "error": f"Unknown action: {action}",
                "hint": "Valid actions are: list, get, search, bulk_get",
            }

    except Exception as e:
        return {
            "_success": False,
            "error": f"Action '{action}' failed: {str(e)}",
            "exception_type": type(e).__name__,
        }


__all__ = ['query_components_action']
