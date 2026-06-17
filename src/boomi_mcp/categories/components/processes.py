#!/usr/bin/env python3
"""
Process Component MCP Tools for Boomi API Integration.

This module provides read-only process component inspection: listing process
components in an account and getting a single process by ID.

Process authoring (create/update) is no longer offered here. The legacy
freeform JSON-to-process-XML compiler has been removed; typed process
authoring lives in build_integration (config.process_kind) and the
archetype tooling. manage_process therefore supports list/get only.

Features:
- List process components (with optional folder filter)
- Get a single process component by ID
"""

from typing import Dict, Any, Optional

from boomi import Boomi
from boomi.net.transport.api_error import ApiError

# Import typed models for query operations
from boomi.models import (
    ComponentMetadataQueryConfig,
    ComponentMetadataQueryConfigQueryFilter,
    ComponentMetadataSimpleExpression,
    ComponentMetadataSimpleExpressionOperator,
    ComponentMetadataSimpleExpressionProperty
)

# Import shared helper
from ._shared import (
    component_get_xml as _component_get_xml,
    _extract_api_error_msg,
    ComponentGetDeadlineExceeded,
    component_get_deadline_envelope,
)


def list_processes(
    boomi_client: Boomi,
    profile: str,
    filters: Optional[Dict[str, Any]] = None
) -> Dict[str, Any]:
    """
    List all process components in the account.

    Args:
        boomi_client: Authenticated Boomi SDK client
        profile: Profile name (for context)
        filters: Optional filters (folder, name pattern, etc.)

    Returns:
        Dict with success status and list of processes

    Example:
        result = list_processes(sdk, "production")
        for process in result["processes"]:
            print(f"{process['name']} ({process['folder_name']})")
    """
    try:
        # Build query for process type
        expression = ComponentMetadataSimpleExpression(
            operator=ComponentMetadataSimpleExpressionOperator.EQUALS,
            property=ComponentMetadataSimpleExpressionProperty.TYPE,
            argument=["process"]
        )

        query_filter = ComponentMetadataQueryConfigQueryFilter(expression=expression)
        query_config = ComponentMetadataQueryConfig(query_filter=query_filter)

        # Query API
        result = boomi_client.component_metadata.query_component_metadata(
            request_body=query_config
        )

        # Parse results
        processes = []
        if hasattr(result, 'result') and result.result:
            for comp in result.result:
                # Filter to current, non-deleted versions
                if (str(getattr(comp, 'current_version', 'false')).lower() == 'true'
                        and str(getattr(comp, 'deleted', 'true')).lower() == 'false'):

                    # Apply user filters if provided
                    if filters:
                        folder_filter = filters.get('folder_name')
                        if folder_filter and getattr(comp, 'folder_name', '') != folder_filter:
                            continue

                    processes.append({
                        'process_id': getattr(comp, 'component_id', ''),
                        'component_id': getattr(comp, 'component_id', ''),
                        'id': getattr(comp, 'id_', ''),
                        'name': getattr(comp, 'name', ''),
                        'folder_name': getattr(comp, 'folder_name', ''),
                        'type': getattr(comp, 'type', ''),
                        'version': getattr(comp, 'version', ''),
                        'created_date': getattr(comp, 'created_date', ''),
                        'modified_date': getattr(comp, 'modified_date', '')
                    })

        return {
            "_success": True,
            "total_count": len(processes),
            "processes": processes,
            "profile": profile
        }

    except ApiError as e:
        return {
            "_success": False,
            "error": f"Failed to list processes: {_extract_api_error_msg(e)}",
            "exception_type": "ApiError"
        }
    except Exception as e:
        return {
            "_success": False,
            "error": f"Failed to list processes: {str(e)}",
            "exception_type": type(e).__name__
        }


def get_process(
    boomi_client: Boomi,
    profile: str,
    process_id: str
) -> Dict[str, Any]:
    """
    Get specific process component by ID.

    Args:
        boomi_client: Authenticated Boomi SDK client
        profile: Profile name (for context)
        process_id: Process component ID

    Returns:
        Dict with success status and process details

    Example:
        result = get_process(sdk, "production", "abc-123-def")
        print(result["process"]["name"])
    """
    try:
        # Use raw XML helper — SDK's get_component() returns 406 (JSON-only Accept header)
        process_data = _component_get_xml(boomi_client, process_id)
        process_data['process_id'] = process_data.get('component_id', process_id)

        return {
            "_success": True,
            "process": process_data,
            "profile": profile
        }

    except ComponentGetDeadlineExceeded as e:
        return component_get_deadline_envelope(e)
    except Exception as e:
        return {
            "_success": False,
            "error": f"Failed to get process '{process_id}': {_extract_api_error_msg(e)}",
            "exception_type": type(e).__name__,
            "hint": "Verify the process ID exists and is accessible"
        }


def manage_process_action(
    boomi_client: Boomi,
    profile: str,
    action: str,
    **params
) -> Dict[str, Any]:
    """
    Consolidated read-only process inspection function.

    Routes to the appropriate reader based on action parameter.
    manage_process is read-only: create/update/delete are no longer
    supported (legacy freeform process JSON authoring has been removed).

    Args:
        boomi_client: Authenticated Boomi SDK client
        profile: Profile name for authentication
        action: Action to perform (list, get)
        **params: Action-specific parameters

    Actions:
        - list: List process components with optional filters
          Params: filters (optional dict)

        - get: Get specific process by ID
          Params: process_id (required str)

    Returns:
        Action result dict with success status and data/error.
        Unsupported actions (create/update/delete) return an
        ACTION_UNSUPPORTED envelope.

    Examples:
        # List processes
        result = manage_process_action(sdk, "prod", "list")

        # Get process
        result = manage_process_action(
            sdk, "prod", "get",
            process_id="abc-123"
        )
    """
    try:
        if action == "list":
            filters = params.get("filters", None)
            return list_processes(boomi_client, profile, filters)

        elif action == "get":
            process_id = params.get("process_id")
            if not process_id:
                return {
                    "_success": False,
                    "error": "process_id is required for 'get' action",
                    "hint": "Provide the process component ID to retrieve"
                }
            return get_process(boomi_client, profile, process_id)

        elif action in ("create", "update", "delete"):
            return {
                "_success": False,
                "error_code": "ACTION_UNSUPPORTED",
                "error": (
                    f"manage_process(action='{action}') is no longer supported. "
                    "Legacy freeform process JSON authoring has been removed; "
                    "manage_process is read-only."
                ),
                "valid_actions": ["list", "get"],
                "hint": (
                    "For typed process authoring use list_integration_archetypes()/"
                    "build_from_archetype()/build_integration. For an explicit raw "
                    "XML component escape hatch use manage_component. Components are "
                    "deleted via manage_component(action='delete') or the Boomi "
                    "Platform Build page."
                ),
            }

        else:
            return {
                "_success": False,
                "error_code": "ACTION_UNSUPPORTED",
                "error": f"Unknown action: {action}",
                "valid_actions": ["list", "get"],
                "hint": "Valid actions are: list, get"
            }

    except ApiError as e:
        return {
            "_success": False,
            "error": f"Action '{action}' failed: {_extract_api_error_msg(e)}",
            "exception_type": "ApiError"
        }
    except Exception as e:
        return {
            "_success": False,
            "error": f"Action '{action}' failed: {str(e)}",
            "exception_type": type(e).__name__
        }


__all__ = [
    'list_processes',
    'get_process',
    'manage_process_action'
]
