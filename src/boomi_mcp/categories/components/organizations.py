#!/usr/bin/env python3
"""
Organization MCP Tools for Boomi API Integration.

This module provides organization management capabilities including CRUD operations
for Organization components that can be linked to Trading Partners.

Organizations provide shared contact information that can be reused across
multiple trading partners via the organization_id field.
"""

from typing import Dict, Any, Optional

# Import typed models for query operations
from boomi.models import (
    OrganizationComponent,
    OrganizationContactInfo,
    OrganizationComponentQueryConfig,
    OrganizationComponentQueryConfigQueryFilter,
    OrganizationComponentSimpleExpression,
    OrganizationComponentSimpleExpressionOperator,
    OrganizationComponentSimpleExpressionProperty
)
from boomi.net.transport.api_error import ApiError
from boomi_mcp.categories.components._shared import (
    _extract_api_error_msg,
    component_family_json_request,
    _json_error_message,
)
import json as json_mod


def build_organization_contact_info(**kwargs) -> OrganizationContactInfo:
    """
    Build OrganizationContactInfo model from flat parameters.

    All 11 fields are REQUIRED by the SDK model, so we use empty strings as defaults.

    Args:
        contact_name: Name of the contact person
        contact_email: Email address
        contact_phone: Phone number
        contact_fax: Fax number
        contact_url: Contact URL/website
        contact_address: Street address line 1
        contact_address2: Street address line 2
        contact_city: City
        contact_state: State/Province
        contact_country: Country
        contact_postalcode: Postal/ZIP code

    Returns:
        OrganizationContactInfo model
    """
    return OrganizationContactInfo(
        contact_name=kwargs.get('contact_name', '') or '',
        email=kwargs.get('contact_email', '') or '',
        phone=kwargs.get('contact_phone', '') or '',
        fax=kwargs.get('contact_fax', '') or '',
        contact_url=kwargs.get('contact_url', '') or '',
        address1=kwargs.get('contact_address', '') or '',
        address2=kwargs.get('contact_address2', '') or '',
        city=kwargs.get('contact_city', '') or '',
        state=kwargs.get('contact_state', '') or '',
        country=kwargs.get('contact_country', '') or '',
        postalcode=kwargs.get('contact_postalcode', '') or ''
    )


# ============================================================================
# Organization CRUD Operations
# ============================================================================

def create_organization(boomi_client, profile: str, request_data: Dict[str, Any]) -> Dict[str, Any]:
    """
    Create a new organization component in Boomi.

    Args:
        boomi_client: Authenticated Boomi SDK client
        profile: Profile name for authentication
        request_data: Organization configuration including:
            - component_name: Name of the organization (required)
            - folder_name: Folder name (default: Home)
            - contact_name, contact_email, contact_phone, contact_fax, contact_url
            - contact_address, contact_address2, contact_city, contact_state, contact_country, contact_postalcode

    Returns:
        Created organization details or error
    """
    try:
        if not request_data.get("component_name"):
            return {
                "_success": False,
                "error": "component_name is required",
                "message": "Organization name (component_name) is required"
            }

        # Build contact info
        contact_info = build_organization_contact_info(**request_data)

        # Build organization model
        org_model = OrganizationComponent(
            organization_contact_info=contact_info,
            component_name=request_data.get("component_name"),
            folder_name=request_data.get("folder_name", "Home")
        )

        # Create organization. The OrganizationComponent endpoint accepts JSON;
        # SDK 3.0.0's typed create is XML-only, so transport the typed model as
        # JSON and hydrate the response back into a model for field access.
        resp, status = component_family_json_request(
            boomi_client.organization_component, "OrganizationComponent", "POST", body=org_model
        )
        if status and status >= 400:
            msg = _json_error_message(resp)
            return {
                "_success": False,
                "error": msg,
                "message": f"Failed to create organization: {msg}"
            }
        # Read the JSON response as a dict (the strict typed model rejects sparse
        # OrganizationContactInfo, so we never re-hydrate it).
        result = resp if isinstance(resp, dict) else {}
        component_id = result.get("componentId") or result.get("id")

        return {
            "_success": True,
            "organization": {
                "component_id": component_id,
                "name": result.get("componentName") or request_data.get("component_name"),
                "folder_name": result.get("folderName") or request_data.get("folder_name", "Home")
            },
            "message": f"Successfully created organization: {request_data.get('component_name')}"
        }

    except ApiError as e:
        return {
            "_success": False,
            "error": _extract_api_error_msg(e),
            "message": f"Failed to create organization: {_extract_api_error_msg(e)}"
        }
    except Exception as e:
        return {
            "_success": False,
            "error": str(e),
            "message": f"Failed to create organization: {str(e)}"
        }


def get_organization(boomi_client, profile: str, organization_id: str) -> Dict[str, Any]:
    """
    Get details of a specific organization by ID.

    Args:
        boomi_client: Authenticated Boomi SDK client
        profile: Profile name for authentication
        organization_id: Organization component ID

    Returns:
        Organization details or error
    """
    try:
        resp, status = component_family_json_request(
            boomi_client.organization_component, f"OrganizationComponent/{organization_id}", "GET"
        )
        if status and status >= 400:
            msg = _json_error_message(resp)
            return {
                "_success": False,
                "error": msg,
                "message": f"Failed to get organization: {msg}"
            }
        # Read the JSON response as a dict (avoids the strict typed model, which
        # rejects sparse OrganizationContactInfo payloads).
        result = resp if isinstance(resp, dict) else {}

        # Extract contact info with normalized contact_* field names (matches input config format)
        org_data = {
            "component_id": result.get("componentId") or organization_id,
            "name": result.get("componentName"),
            "folder_id": result.get("folderId"),
            "folder_name": result.get("folderName"),
            "deleted": result.get("deleted", False),
        }

        contact = result.get("OrganizationContactInfo") or {}
        if isinstance(contact, dict) and contact:
            contact_fields = {
                "contact_name": contact.get("contactName"),
                "contact_email": contact.get("email"),
                "contact_phone": contact.get("phone"),
                "contact_fax": contact.get("fax"),
                "contact_url": contact.get("contactUrl"),
                "contact_address": contact.get("address1"),
                "contact_address2": contact.get("address2"),
                "contact_city": contact.get("city"),
                "contact_state": contact.get("state"),
                "contact_country": contact.get("country"),
                "contact_postalcode": contact.get("postalcode"),
            }
            # Flatten into org_data, skip empty values
            for k, v in contact_fields.items():
                if v:
                    org_data[k] = v

        return {
            "_success": True,
            "organization": org_data
        }

    except ApiError as e:
        return {
            "_success": False,
            "error": _extract_api_error_msg(e),
            "message": f"Failed to get organization: {_extract_api_error_msg(e)}"
        }
    except Exception as e:
        return {
            "_success": False,
            "error": str(e),
            "message": f"Failed to get organization: {str(e)}"
        }


def list_organizations(boomi_client, profile: str, filters: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    """
    List all organizations with optional filtering.

    Args:
        boomi_client: Authenticated Boomi SDK client
        profile: Profile name for authentication
        filters: Optional filters including:
            - name_pattern: Filter by name pattern (supports % wildcard)

    Returns:
        List of organizations or error
    """
    try:
        # Build query body
        name_pattern = '%'
        if filters and "name_pattern" in filters:
            name_pattern = filters["name_pattern"]

        query_body = {
            "QueryFilter": {
                "expression": {
                    "operator": "LIKE",
                    "property": "name",
                    "argument": [name_pattern]
                }
            }
        }

        # OrganizationComponent query accepts JSON; transport it via the shared
        # component-family JSON helper (SDK 3.0.0's typed query path is fine too,
        # but keeping JSON preserves this list's sparse-tolerant field handling).
        response, status = component_family_json_request(
            boomi_client.organization_component, "OrganizationComponent/query", "POST", body=query_body
        )
        if status and status >= 400:
            msg = _json_error_message(response)
            return {
                "_success": False,
                "error": msg,
                "message": f"Failed to list organizations: {msg}"
            }
        if isinstance(response, (bytes, bytearray)):
            response = response.decode("utf-8")
        data = json_mod.loads(response) if isinstance(response, str) else response

        organizations = []
        seen = {}  # component_id -> index in organizations list
        raw_total = 0

        def _ingest(rows):
            nonlocal raw_total
            for org in rows:
                raw_total += 1
                cid = org.get("componentId") or org.get("id")
                entry = {
                    "component_id": cid,
                    "name": org.get("componentName"),
                    "folder_name": org.get("folderName"),
                    "folder_id": org.get("folderId")
                }
                if cid in seen:
                    # Backfill missing fields from later duplicate
                    existing = organizations[seen[cid]]
                    for k, v in entry.items():
                        if v is not None and existing.get(k) is None:
                            existing[k] = v
                else:
                    seen[cid] = len(organizations)
                    organizations.append(entry)

        if "result" in data:
            _ingest(data["result"])

        # Handle pagination
        while data.get("queryToken"):
            token = data["queryToken"]
            response2, _ = component_family_json_request(
                boomi_client.organization_component, "OrganizationComponent/queryMore",
                "POST", body=token, body_content_type="text/plain"
            )
            if isinstance(response2, (bytes, bytearray)):
                response2 = response2.decode("utf-8")
            data = json_mod.loads(response2) if isinstance(response2, str) else response2
            if "result" in data:
                _ingest(data["result"])

        dupes_removed = raw_total - len(organizations)
        result = {
            "_success": True,
            "total_count": len(organizations),
            "organizations": organizations
        }
        if dupes_removed > 0:
            result["raw_total_count"] = raw_total
            result["duplicates_removed"] = dupes_removed
        return result

    except ApiError as e:
        return {
            "_success": False,
            "error": _extract_api_error_msg(e),
            "message": f"Failed to list organizations: {_extract_api_error_msg(e)}"
        }
    except Exception as e:
        return {
            "_success": False,
            "error": str(e),
            "message": f"Failed to list organizations: {str(e)}"
        }


def update_organization(boomi_client, profile: str, organization_id: str, updates: Dict[str, Any]) -> Dict[str, Any]:
    """
    Update an existing organization component.

    Args:
        boomi_client: Authenticated Boomi SDK client
        profile: Profile name for authentication
        organization_id: Organization component ID to update
        updates: Fields to update including:
            - component_name: Organization name
            - folder_name: Folder location
            - contact_name, contact_email, contact_phone, contact_fax, contact_url
            - contact_address, contact_address2, contact_city, contact_state, contact_country, contact_postalcode

    Returns:
        Updated organization details or error
    """
    try:
        # Get existing organization (JSON; hydrate into a typed model to merge).
        resp, status = component_family_json_request(
            boomi_client.organization_component, f"OrganizationComponent/{organization_id}", "GET"
        )
        if status and status >= 400:
            msg = _json_error_message(resp)
            return {
                "_success": False,
                "error": msg,
                "message": f"Failed to update organization: {msg}"
            }
        # Work on the JSON dict directly (avoids the strict typed model) and POST
        # it back, preserving any fields the MCP doesn't own.
        existing_org = resp if isinstance(resp, dict) else {}

        # Update component name / folder if provided (wire keys are camelCase)
        if "component_name" in updates:
            existing_org["componentName"] = updates["component_name"]
        if "folder_name" in updates:
            existing_org["folderName"] = updates["folder_name"]

        # Boomi API REQUIRES OrganizationContactInfo to be present in the update
        # payload. Merge existing contact values with any contact_* updates,
        # mapped to their wire keys.
        existing_contact = existing_org.get("OrganizationContactInfo") or {}
        contact_params = {
            'contactName': existing_contact.get('contactName', ''),
            'email': existing_contact.get('email', ''),
            'phone': existing_contact.get('phone', ''),
            'fax': existing_contact.get('fax', ''),
            'contactUrl': existing_contact.get('contactUrl', ''),
            'address1': existing_contact.get('address1', ''),
            'address2': existing_contact.get('address2', ''),
            'city': existing_contact.get('city', ''),
            'state': existing_contact.get('state', ''),
            'country': existing_contact.get('country', ''),
            'postalcode': existing_contact.get('postalcode', ''),
        }
        _contact_wire_keys = {
            'contact_name': 'contactName', 'contact_email': 'email', 'contact_phone': 'phone',
            'contact_fax': 'fax', 'contact_url': 'contactUrl', 'contact_address': 'address1',
            'contact_address2': 'address2', 'contact_city': 'city', 'contact_state': 'state',
            'contact_country': 'country', 'contact_postalcode': 'postalcode',
        }
        for k, v in updates.items():
            if k in _contact_wire_keys:
                contact_params[_contact_wire_keys[k]] = v
        existing_org["OrganizationContactInfo"] = contact_params

        # Update organization (JSON POST to the dedicated endpoint).
        resp2, status2 = component_family_json_request(
            boomi_client.organization_component, f"OrganizationComponent/{organization_id}",
            "POST", body=existing_org
        )
        if status2 and status2 >= 400:
            msg = _json_error_message(resp2)
            return {
                "_success": False,
                "error": msg,
                "message": f"Failed to update organization: {msg}"
            }

        return {
            "_success": True,
            "organization": {
                "component_id": organization_id,
                "name": updates.get("component_name", existing_org.get("componentName")),
                "updated_fields": list(updates.keys())
            },
            "message": f"Successfully updated organization: {organization_id}"
        }

    except ApiError as e:
        return {
            "_success": False,
            "error": _extract_api_error_msg(e),
            "message": f"Failed to update organization: {_extract_api_error_msg(e)}"
        }
    except Exception as e:
        return {
            "_success": False,
            "error": str(e),
            "message": f"Failed to update organization: {str(e)}"
        }


def delete_organization(boomi_client, profile: str, organization_id: str) -> Dict[str, Any]:
    """
    Delete an organization component.

    Args:
        boomi_client: Authenticated Boomi SDK client
        profile: Profile name for authentication
        organization_id: Organization component ID to delete

    Returns:
        Deletion confirmation or error
    """
    try:
        boomi_client.organization_component.delete_organization_component(organization_id)

        return {
            "_success": True,
            "component_id": organization_id,
            "deleted": True,
            "message": f"Successfully deleted organization: {organization_id}"
        }

    except ApiError as e:
        return {
            "_success": False,
            "error": _extract_api_error_msg(e),
            "message": f"Failed to delete organization: {_extract_api_error_msg(e)}"
        }
    except Exception as e:
        return {
            "_success": False,
            "error": str(e),
            "message": f"Failed to delete organization: {str(e)}"
        }


# ============================================================================
# Consolidated Action Router (for MCP tool consolidation)
# ============================================================================

def manage_organization_action(
    boomi_client,
    profile: str,
    action: str,
    **params
) -> Dict[str, Any]:
    """
    Consolidated organization management function.

    Routes to appropriate function based on action parameter.

    Args:
        boomi_client: Authenticated Boomi SDK client
        profile: Profile name for authentication
        action: Action to perform (list, get, create, update, delete)
        **params: Action-specific parameters

    Actions:
        - list: List organizations with optional filters
          Params: filters (optional dict)

        - get: Get specific organization by ID
          Params: organization_id (required str)

        - create: Create new organization
          Params: request_data (required dict)

        - update: Update existing organization
          Params: organization_id (required str), updates (required dict)

        - delete: Delete organization
          Params: organization_id (required str)

    Returns:
        Action result dict with success status and data/error
    """
    try:
        if action == "list":
            filters = params.get("filters", None)
            return list_organizations(boomi_client, profile, filters)

        elif action == "get":
            organization_id = params.get("organization_id")
            if not organization_id:
                return {
                    "_success": False,
                    "error": "organization_id is required for 'get' action",
                    "hint": "Provide the organization component ID to retrieve"
                }
            return get_organization(boomi_client, profile, organization_id)

        elif action == "create":
            request_data = params.get("request_data")
            if not request_data:
                return {
                    "_success": False,
                    "error": "config is required for 'create' action",
                    "hint": "config must include at least component_name. Provide organization configuration including contact info."
                }
            return create_organization(boomi_client, profile, request_data)

        elif action == "update":
            organization_id = params.get("organization_id")
            updates = params.get("updates")
            if not organization_id:
                return {
                    "_success": False,
                    "error": "organization_id is required for 'update' action",
                    "hint": "Provide the organization component ID to update"
                }
            if not updates:
                return {
                    "_success": False,
                    "error": "updates dict is required for 'update' action",
                    "hint": "Provide the fields to update"
                }
            return update_organization(boomi_client, profile, organization_id, updates)

        elif action == "delete":
            organization_id = params.get("organization_id")
            if not organization_id:
                return {
                    "_success": False,
                    "error": "organization_id is required for 'delete' action",
                    "hint": "Provide the organization component ID to delete"
                }
            return delete_organization(boomi_client, profile, organization_id)

        else:
            return {
                "_success": False,
                "error": f"Unknown action: {action}",
                "hint": "Valid actions are: list, get, create, update, delete"
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
