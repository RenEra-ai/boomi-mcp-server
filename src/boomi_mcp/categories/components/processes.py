#!/usr/bin/env python3
"""
Process Component MCP Tools for Boomi API Integration.

This module provides process component management capabilities including
CRUD operations and orchestrated multi-component workflows.

Features:
- Simple process creation (single component, no dependencies)
- Complex workflows (multi-component with dependency management)
- Fuzzy ID resolution (component names → IDs)
- YAML configuration support
- Topological sorting for dependencies
"""

from typing import Dict, Any, List, Optional
import xml.etree.ElementTree as ET

from boomi import Boomi
from boomi.net.transport.serializer import Serializer
from boomi.net.environment.environment import Environment

# Import our modules
from ...xml_builders.builders.orchestrator import ComponentOrchestrator
from ...xml_builders.yaml_parser import parse_yaml_to_specs
from ...models.process_models import ComponentSpec, ProcessConfig

# Import typed models for query operations
from boomi.models import (
    ComponentMetadataQueryConfig,
    ComponentMetadataQueryConfigQueryFilter,
    ComponentMetadataSimpleExpression,
    ComponentMetadataSimpleExpressionOperator,
    ComponentMetadataSimpleExpressionProperty
)


# ============================================================================
# SDK workaround helpers (Component API lacks delete; GET returns XML only)
# ============================================================================

def _component_get_xml(boomi_client: Boomi, component_id: str) -> Dict[str, Any]:
    """GET component as parsed XML dict — bypasses SDK 406 bug on get_component().

    The SDK's HttpHandler auto-sets Accept: application/json, but Boomi's
    Component GET endpoint only supports application/xml (returns 406 otherwise).
    We use the Serializer directly with an explicit Accept header.
    """
    svc = boomi_client.component
    serialized_request = (
        Serializer(
            f"{svc.base_url or Environment.DEFAULT.url}/Component/{component_id}",
            [svc.get_access_token(), svc.get_basic_auth()],
        )
        .add_header("Accept", "application/xml")
        .serialize()
        .set_method("GET")
    )
    response, status, content = svc.send_request(serialized_request)
    if status >= 400:
        raise Exception(f"GET failed: HTTP {status} — {response}")

    raw_xml = response if isinstance(response, str) else response.decode('utf-8')
    root = ET.fromstring(raw_xml)

    return {
        'component_id': root.attrib.get('componentId', component_id),
        'id': root.attrib.get('componentId', ''),
        'name': root.attrib.get('name', ''),
        'folder_name': root.attrib.get('folderName', ''),
        'folder_id': root.attrib.get('folderId', ''),
        'type': root.attrib.get('type', ''),
        'version': root.attrib.get('version', ''),
        'xml': raw_xml,
    }


def _component_delete(boomi_client: Boomi, component_id: str) -> None:
    """Delete component — NOT supported by Boomi's Component REST API.

    The Component API (used for processes) does not support HTTP DELETE,
    nor does updating with deleted='true' work (the flag is silently ignored).
    Components can only be deleted through the Boomi Platform UI.
    """
    raise NotImplementedError(
        "Boomi's Component REST API does not support deletion. "
        "Process components can only be deleted through the Boomi Platform UI (Build page)."
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
            "count": len(processes),
            "processes": processes,
            "profile": profile
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

        return {
            "_success": True,
            "process": process_data,
            "profile": profile
        }

    except Exception as e:
        return {
            "_success": False,
            "error": f"Failed to get process '{process_id}': {str(e)}",
            "exception_type": type(e).__name__,
            "hint": "Verify the process ID exists and is accessible"
        }


def create_process(
    boomi_client: Boomi,
    profile: str,
    config_yaml: str
) -> Dict[str, Any]:
    """
    Create process component(s) from YAML configuration.

    Supports both single process and multi-component workflows with dependencies.

    Args:
        boomi_client: Authenticated Boomi SDK client
        profile: Profile name (for context)
        config_yaml: YAML configuration string

    Returns:
        Dict with success status and created component IDs

    Example (single process):
        yaml_config = '''
        name: "Hello World"
        shapes:
          - type: start
            name: start
          - type: message
            name: msg
            config:
              message_text: "Hello from Boomi!"
          - type: stop
            name: end
        '''
        result = create_process(sdk, "production", yaml_config)

    Example (multi-component):
        yaml_config = '''
        components:
          - name: "Transform Map"
            type: map
            dependencies: []
          - name: "Main Process"
            type: process
            dependencies: ["Transform Map"]
            config:
              name: "Main Process"
              shapes:
                - type: start
                  name: start
                - type: map
                  name: transform
                  config:
                    map_ref: "Transform Map"
                - type: stop
                  name: end
        '''
        result = create_process(sdk, "production", yaml_config)
    """
    try:
        # Parse YAML to ComponentSpec list
        specs = parse_yaml_to_specs(config_yaml)

        # Create orchestrator
        orchestrator = ComponentOrchestrator(boomi_client)

        # Build with dependencies
        registry = orchestrator.build_with_dependencies(specs)

        # Format results
        created_components = {}
        for name, info in registry.items():
            created_components[name] = {
                'component_id': info['component_id'],
                'id': info['id'],
                'type': info['type']
            }

        result = {
            "_success": True,
            "message": f"Created {len(created_components)} component(s)",
            "components": created_components,
            "profile": profile
        }
        if orchestrator.warnings:
            result["warnings"] = orchestrator.warnings
        return result

    except ValueError as e:
        # Validation or parsing errors
        return {
            "_success": False,
            "error": f"Configuration error: {str(e)}",
            "exception_type": "ValidationError",
            "hint": "Check YAML syntax and required fields. Ensure dependencies are declared correctly."
        }

    except Exception as e:
        return {
            "_success": False,
            "error": f"Failed to create process: {str(e)}",
            "exception_type": type(e).__name__
        }


def update_process(
    boomi_client: Boomi,
    profile: str,
    process_id: str,
    config_yaml: str
) -> Dict[str, Any]:
    """
    Update existing process component.

    Note: This performs a full rebuild of the process XML from the YAML config.
    Partial updates are not supported by Boomi's Component API.

    Args:
        boomi_client: Authenticated Boomi SDK client
        profile: Profile name (for context)
        process_id: Process component ID to update
        config_yaml: New YAML configuration

    Returns:
        Dict with success status

    Example:
        yaml_config = '''
        name: "Updated Process"
        shapes:
          - type: start
            name: start
          - type: message
            name: msg
            config:
              message_text: "Updated message"
          - type: stop
            name: end
        '''
        result = update_process(sdk, "production", "abc-123", yaml_config)
    """
    try:
        # Parse YAML
        specs = parse_yaml_to_specs(config_yaml)

        if len(specs) > 1:
            return {
                "_success": False,
                "error": "Update only supports single process configuration",
                "hint": "Use create action for multi-component workflows"
            }

        spec = specs[0]

        # Create orchestrator to build XML
        orchestrator = ComponentOrchestrator(boomi_client)
        xml = orchestrator._build_process(spec)

        # Update via API
        result = boomi_client.component.update_component(
            component_id=process_id,
            request_body=xml
        )

        return {
            "_success": True,
            "message": f"Updated process '{spec.name}'",
            "process_id": process_id,
            "component_id": getattr(result, 'component_id', process_id),
            "profile": profile
        }

    except ValueError as e:
        return {
            "_success": False,
            "error": f"Configuration error: {str(e)}",
            "exception_type": "ValidationError"
        }

    except Exception as e:
        return {
            "_success": False,
            "error": f"Failed to update process '{process_id}': {str(e)}",
            "exception_type": type(e).__name__
        }


def delete_process(
    boomi_client: Boomi,
    profile: str,
    process_id: str
) -> Dict[str, Any]:
    """
    Delete process component.

    Args:
        boomi_client: Authenticated Boomi SDK client
        profile: Profile name (for context)
        process_id: Process component ID to delete

    Returns:
        Dict with success status

    Example:
        result = delete_process(sdk, "production", "abc-123-def")
    """
    try:
        # Use direct HTTP DELETE — ComponentService lacks delete_component method
        _component_delete(boomi_client, process_id)

        return {
            "_success": True,
            "message": f"Deleted process '{process_id}'",
            "process_id": process_id,
            "profile": profile
        }

    except NotImplementedError as e:
        return {
            "_success": False,
            "error": str(e),
            "exception_type": "NotSupported",
            "hint": "Delete process components from the Boomi Platform Build page instead."
        }

    except Exception as e:
        return {
            "_success": False,
            "error": f"Failed to delete process '{process_id}': {str(e)}",
            "exception_type": type(e).__name__
        }


def manage_process_action(
    boomi_client: Boomi,
    profile: str,
    action: str,
    **params
) -> Dict[str, Any]:
    """
    Consolidated process management function.

    Routes to appropriate function based on action parameter.
    This enables consolidation of multiple operations into 1 MCP tool.

    Args:
        boomi_client: Authenticated Boomi SDK client
        profile: Profile name for authentication
        action: Action to perform (list, get, create, update, delete)
        **params: Action-specific parameters

    Actions:
        - list: List process components with optional filters
          Params: filters (optional dict)

        - get: Get specific process by ID
          Params: process_id (required str)

        - create: Create new process(es) from YAML
          Params: config_yaml (required str)

        - update: Update existing process
          Params: process_id (required str), config_yaml (required str)

        - delete: Delete process
          Params: process_id (required str)

    Returns:
        Action result dict with success status and data/error

    Examples:
        # List processes
        result = manage_process_action(sdk, "prod", "list")

        # Create simple process
        result = manage_process_action(
            sdk, "prod", "create",
            config_yaml="name: Test\\nshapes: [...]"
        )

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

        elif action == "create":
            config_yaml = params.get("config_yaml")
            if not config_yaml:
                return {
                    "_success": False,
                    "error": "config_yaml is required for 'create' action",
                    "hint": "Provide YAML configuration with process structure. See documentation for examples."
                }
            return create_process(boomi_client, profile, config_yaml)

        elif action == "update":
            process_id = params.get("process_id")
            config_yaml = params.get("config_yaml")
            if not process_id:
                return {
                    "_success": False,
                    "error": "process_id is required for 'update' action",
                    "hint": "Provide the process component ID to update"
                }
            if not config_yaml:
                return {
                    "_success": False,
                    "error": "config_yaml is required for 'update' action",
                    "hint": "Provide new YAML configuration for the process"
                }
            return update_process(boomi_client, profile, process_id, config_yaml)

        elif action == "delete":
            process_id = params.get("process_id")
            if not process_id:
                return {
                    "_success": False,
                    "error": "process_id is required for 'delete' action",
                    "hint": "Provide the process component ID to delete"
                }
            return delete_process(boomi_client, profile, process_id)

        else:
            return {
                "_success": False,
                "error": f"Unknown action: {action}",
                "hint": "Valid actions are: list, get, create, update, delete"
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
    'create_process',
    'update_process',
    'delete_process',
    'manage_process_action'
]
