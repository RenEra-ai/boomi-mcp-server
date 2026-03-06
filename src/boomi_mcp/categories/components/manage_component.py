"""
Component Management MCP Tools for Boomi API Integration.

Provides component CRUD operations:
- create: Create a component from XML or delegate process creation to processes.py
- update: Update component metadata or full XML
- clone: Clone an existing component with a new name
- delete: Soft-delete a component (mark deleted=true)
"""

from typing import Dict, Any, Optional
import xml.etree.ElementTree as ET

from boomi import Boomi
from boomi.net.transport.api_error import ApiError

from ._shared import (
    component_get_xml, set_description_element, soft_delete_component,
    _create_component_raw, _extract_api_error_msg,
)


# ============================================================================
# Actions
# ============================================================================

def create_component(
    boomi_client: Boomi,
    profile: str,
    config: Dict[str, Any],
    config_yaml: Optional[str] = None
) -> Dict[str, Any]:
    """Create a new component.

    If type is 'process' and config_yaml is provided, delegates to processes.py.
    Otherwise requires raw XML in config['xml'].

    Note: Boomi's Component API requires type-specific XML structures with proper
    namespaces and object elements. Minimal XML without these is rejected with 400.
    Use query_components get action on an existing component to obtain a valid XML
    template, or use manage_process with config_yaml for process components.
    """
    try:
        comp_type = config.get('type', '')

        # Delegate process creation to processes.py if YAML provided
        if config_yaml:
            if not comp_type:
                comp_type = 'process'
            if comp_type == 'process':
                from .processes import create_process
                return create_process(boomi_client, profile, config_yaml)

        # Create from raw XML
        if config.get('xml'):
            result = _create_component_raw(boomi_client, config['xml'])
            return {
                "_success": True,
                "message": f"Created component '{result['name']}'",
                "component_id": result['component_id'],
                "name": result['name'],
                "type": result['type'],
                "profile": profile,
            }

        # No XML provided - cannot create without valid component XML
        return {
            "_success": False,
            "error": "xml is required in config for component creation",
            "hint": (
                "Boomi requires type-specific XML with proper namespaces. "
                "Use query_components get action on an existing component to obtain "
                "a valid XML template, then modify and pass as config.xml. "
                "For processes, use manage_process with config_yaml instead. "
                "For connectors (connector-settings, connector-action), use "
                "manage_connector action='get' on a similar connector to obtain XML, "
                "then modify and pass as config.xml."
            ),
        }

    except ApiError as e:
        return {
            "_success": False,
            "error": f"Failed to create component: {_extract_api_error_msg(e)}",
            "exception_type": "ApiError",
        }
    except Exception as e:
        return {
            "_success": False,
            "error": f"Failed to create component: {str(e)}",
            "exception_type": type(e).__name__,
        }


def update_component(
    boomi_client: Boomi,
    profile: str,
    component_id: str,
    config: Dict[str, Any]
) -> Dict[str, Any]:
    """Update an existing component.

    If config contains 'xml', does full XML replacement.
    Otherwise, updates metadata fields (name, description) in existing XML.
    """
    try:
        if config.get('xml'):
            # Full XML replacement
            result = boomi_client.component.update_component_raw(
                component_id, config['xml']
            )
            return {
                "_success": True,
                "message": f"Updated component '{component_id}' with provided XML",
                "component_id": component_id,
                "profile": profile,
            }

        # Partial update: get current XML, modify, put back
        current = component_get_xml(boomi_client, component_id)
        raw_xml = current['xml']
        root = ET.fromstring(raw_xml)

        changed = False
        if config.get('name'):
            root.set('name', config['name'])
            changed = True
        if config.get('folder_id'):
            root.set('folderId', config['folder_id'])
            changed = True
        if config.get('folder_name'):
            root.set('folderName', config['folder_name'])
            changed = True
        if 'description' in config:
            set_description_element(root, config['description'])
            changed = True

        if not changed:
            return {
                "_success": False,
                "error": "No updatable fields provided in config",
                "hint": "Provide name, folder_id, folder_name, description, or xml",
            }

        modified_xml = ET.tostring(root, encoding='unicode')
        result = boomi_client.component.update_component_raw(
            component_id, modified_xml
        )

        return {
            "_success": True,
            "message": f"Updated component '{current['name']}'",
            "component_id": component_id,
            "profile": profile,
        }

    except ApiError as e:
        return {
            "_success": False,
            "error": f"Failed to update component '{component_id}': {_extract_api_error_msg(e)}",
            "exception_type": "ApiError",
        }
    except Exception as e:
        return {
            "_success": False,
            "error": f"Failed to update component '{component_id}': {str(e)}",
            "exception_type": type(e).__name__,
        }


def clone_component(
    boomi_client: Boomi,
    profile: str,
    component_id: str,
    config: Dict[str, Any]
) -> Dict[str, Any]:
    """Clone an existing component with a new name."""
    try:
        new_name = config.get('name')
        if not new_name:
            return {
                "_success": False,
                "error": "name is required in config for clone action",
                "hint": 'Provide config: {"name": "Cloned Component Name"}',
            }

        # Get source component XML
        source = component_get_xml(boomi_client, component_id)
        raw_xml = source['xml']
        root = ET.fromstring(raw_xml)

        # Set new name
        root.set('name', new_name)

        # Remove identity attributes
        for attr in ['componentId', 'version', 'currentVersion', 'deleted',
                     'createdDate', 'createdBy', 'modifiedDate', 'modifiedBy']:
            if attr in root.attrib:
                del root.attrib[attr]

        # Optionally set folder
        if config.get('folder_name'):
            root.set('folderName', config['folder_name'])
        if config.get('folder_id'):
            root.set('folderId', config['folder_id'])

        # Optionally set description
        if config.get('description'):
            set_description_element(root, config['description'])

        new_xml = ET.tostring(root, encoding='unicode')
        result = _create_component_raw(boomi_client, new_xml)

        return {
            "_success": True,
            "message": f"Cloned '{source['name']}' as '{new_name}'",
            "component_id": result['component_id'],
            "name": result['name'],
            "type": result['type'],
            "source_component_id": component_id,
            "profile": profile,
        }

    except ApiError as e:
        return {
            "_success": False,
            "error": f"Failed to clone component '{component_id}': {_extract_api_error_msg(e)}",
            "exception_type": "ApiError",
        }
    except Exception as e:
        return {
            "_success": False,
            "error": f"Failed to clone component '{component_id}': {str(e)}",
            "exception_type": type(e).__name__,
        }


def delete_component(
    boomi_client: Boomi,
    profile: str,
    component_id: str
) -> Dict[str, Any]:
    """Delete a component via the metadata API."""
    try:
        result = soft_delete_component(boomi_client, component_id)
        return {
            "_success": True,
            "message": f"Deleted component '{result['component_name']}'",
            "component_id": component_id,
            "profile": profile,
            "method": result["method"],
            "warning": "Dependent components are NOT automatically deleted. Check references first.",
        }

    except ApiError as e:
        return {
            "_success": False,
            "error": f"Delete failed: {_extract_api_error_msg(e)}",
            "exception_type": "ApiError",
            "hint": "Retry or use Boomi Platform UI to delete this component.",
        }
    except Exception as e:
        return {
            "_success": False,
            "error": f"Delete failed: {str(e)}",
            "exception_type": type(e).__name__,
            "hint": "Retry or use Boomi Platform UI to delete this component.",
        }


# ============================================================================
# Action Router
# ============================================================================

def manage_component_action(
    boomi_client: Boomi,
    profile: str,
    action: str,
    **params
) -> Dict[str, Any]:
    """Route manage_component actions."""
    try:
        if action == "create":
            config = params.get("config", {})
            config_yaml = params.get("config_yaml")
            if not config and not config_yaml:
                return {
                    "_success": False,
                    "error": "config or config_yaml is required for 'create' action",
                    "hint": 'Provide config: {"name": "My Component", "type": "process"} or config_yaml for process creation',
                }
            return create_component(boomi_client, profile, config or {}, config_yaml)

        elif action == "update":
            component_id = params.get("component_id")
            if not component_id:
                return {
                    "_success": False,
                    "error": "component_id is required for 'update' action",
                }
            config = params.get("config", {})
            if not config:
                return {
                    "_success": False,
                    "error": "config is required for 'update' action",
                    "hint": 'Provide config with fields to update: {"name": "New Name"}',
                }
            return update_component(boomi_client, profile, component_id, config)

        elif action == "clone":
            component_id = params.get("component_id")
            if not component_id:
                return {
                    "_success": False,
                    "error": "component_id is required for 'clone' action",
                }
            config = params.get("config", {})
            if not config or not config.get('name'):
                return {
                    "_success": False,
                    "error": "config with 'name' is required for 'clone' action",
                    "hint": 'Provide config: {"name": "Cloned Component Name"}',
                }
            return clone_component(boomi_client, profile, component_id, config)

        elif action == "delete":
            component_id = params.get("component_id")
            if not component_id:
                return {
                    "_success": False,
                    "error": "component_id is required for 'delete' action",
                }
            return delete_component(boomi_client, profile, component_id)

        else:
            return {
                "_success": False,
                "error": f"Unknown action: {action}",
                "hint": "Valid actions are: create, update, clone, delete",
            }

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


__all__ = ['manage_component_action']
