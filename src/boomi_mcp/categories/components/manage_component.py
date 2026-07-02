"""
Component Management MCP Tools for Boomi API Integration.

Provides component CRUD operations:
- create: Create a component from XML
- update: Update component metadata or full XML
- clone: Clone an existing component with a new name
- delete: Delete a component via metadata API
"""

from typing import Dict, Any
import xml.etree.ElementTree as ET

from boomi import Boomi
from boomi.net.transport.api_error import ApiError

from ._shared import (
    component_get_xml, set_description_element, soft_delete_component,
    _create_component_raw, _extract_api_error_msg,
    ComponentGetDeadlineExceeded, component_get_deadline_envelope,
)
from .builders import (
    BuilderValidationError,
    PROFILE_BUILDERS,
    get_profile_builder,
)
from .builders.process_property_builder import get_process_property_builder
from .builders.script_mapping_builder import get_script_mapping_builder


# ============================================================================
# Actions
# ============================================================================

def create_component(
    boomi_client: Boomi,
    profile: str,
    config: Dict[str, Any]
) -> Dict[str, Any]:
    """Create a new component.

    Two paths:
    1. Builder — for known component_type + profile_type pairs (e.g. profile.db
       database.read for Select-statement Read profiles, profile.db
       database.stored_procedure_read for Stored Procedure Read profiles),
       dispatch through PROFILE_BUILDERS to emit XML from structured caller
       config.
    2. Raw XML — if config['xml'] is provided, POST directly (escape hatch).

    Boomi's Component API requires type-specific XML with proper namespaces;
    the builder path handles namespaces for the types it knows. For other
    types, use query_components get action on an existing component to
    obtain a valid XML template.
    """
    try:
        # Path 1: profile builder dispatch (when no raw XML override).
        component_type = config.get('component_type')
        profile_type = config.get('profile_type')
        if not config.get('xml') and component_type:
            builder = get_profile_builder(component_type, profile_type or "")
            if builder is not None:
                xml = builder.build(**config)
                result = _create_component_raw(boomi_client, xml)
                return {
                    "_success": True,
                    "message": f"Created {component_type} '{result['name']}'",
                    "component_id": result['component_id'],
                    "name": result['name'],
                    "type": result['type'],
                    "profile": profile,
                }
            # Issue #41: standalone script.mapping components also dispatch
            # through manage_component (the component is profile-agnostic so
            # it doesn't need build_integration's profile-index threading).
            # This mirrors the schema template's "tool: manage_component
            # (action='create')" advertisement.
            sm_builder_cls = get_script_mapping_builder(component_type)
            if sm_builder_cls is not None:
                xml = sm_builder_cls().build(**config)
                result = _create_component_raw(boomi_client, xml)
                return {
                    "_success": True,
                    "message": f"Created {component_type} '{result['name']}'",
                    "component_id": result['component_id'],
                    "name": result['name'],
                    "type": result['type'],
                    "profile": profile,
                }
            # Issue #131 M11.7: standalone processproperty components dispatch
            # the same way (profile-agnostic, single-key builder registry).
            pp_builder_cls = get_process_property_builder(component_type)
            if pp_builder_cls is not None:
                xml = pp_builder_cls().build(**config)
                result = _create_component_raw(boomi_client, xml)
                return {
                    "_success": True,
                    "message": f"Created {component_type} '{result['name']}'",
                    "component_id": result['component_id'],
                    "name": result['name'],
                    "type": result['type'],
                    "profile": profile,
                }
            # No builder matched. For component_types that have a registered
            # builder family (e.g. profile.db), surface a structured
            # UNSUPPORTED_DB_PROFILE_MODE envelope listing the supported
            # protocols — matches the dispatch contract that
            # integration_builder._build_plan uses for the same payload.
            valid_profile_types = sorted({
                pt for (ct, pt) in PROFILE_BUILDERS if ct == component_type.lower()
            })
            if valid_profile_types:
                return {
                    "_success": False,
                    "error_code": "UNSUPPORTED_DB_PROFILE_MODE",
                    "error": (
                        f"profile_type {profile_type!r} is not supported "
                        f"for {component_type}. Supported: "
                        f"{', '.join(valid_profile_types)}."
                    ),
                    "field": "profile_type",
                    "hint": (
                        f"Pass profile_type as one of {valid_profile_types} "
                        f"and supply the matching structured config "
                        "(query+output_fields for database.read, "
                        "procedure_name+output_fields for "
                        "database.stored_procedure_read, "
                        "statement_type+fields/conditions for database.write)."
                    ),
                    "profile": profile,
                }
            # No registered builder family for this component_type at all →
            # fall through to the raw-XML escape-hatch error.

        # Path 2: raw XML
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
                "For connectors (connector-settings, connector-action), use "
                "manage_connector action='create' with structured config. "
                "For Select-statement database read profiles, pass "
                "component_type='profile.db' + profile_type='database.read' + "
                "query + output_fields. For Stored Procedure Read profiles, "
                "use profile_type='database.stored_procedure_read' + "
                "procedure_name + output_fields."
            ),
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
        # `name` is the canonical key; `component_name` is the schema-template
        # field name used by build_integration configs. Accept either so a
        # metadata-only structured update (e.g. profile.db rename via
        # build_integration which only carries `component_name`) actually
        # renames the component. Codex r4 P2 follow-up.
        new_name = config.get('name') or config.get('component_name')
        if new_name:
            root.set('name', new_name)
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

    except ComponentGetDeadlineExceeded as e:
        return component_get_deadline_envelope(e)
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

    except ComponentGetDeadlineExceeded as e:
        return component_get_deadline_envelope(e)
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

    except ComponentGetDeadlineExceeded as e:
        return component_get_deadline_envelope(e)
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
            if not config:
                return {
                    "_success": False,
                    "error": "config is required for 'create' action",
                    "hint": 'Provide config: {"xml": "<Component ...>"} with valid Boomi XML. Use query_components get action on an existing component to obtain an XML template.',
                }
            return create_component(boomi_client, profile, config)

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
