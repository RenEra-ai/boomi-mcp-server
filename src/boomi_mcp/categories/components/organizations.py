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

        # Create organization
        result = boomi_client.organization_component.create_organization_component(
            request_body=org_model
        )

        # Extract component ID
        component_id = getattr(result, 'component_id', None) or getattr(result, 'id_', None)

        return {
            "_success": True,
            "organization": {
                "component_id": component_id,
                "name": getattr(result, 'component_name', request_data.get("component_name")),
                "folder_name": getattr(result, 'folder_name', request_data.get("folder_name", "Home"))
            },
            "message": f"Successfully created organization: {request_data.get('component_name')}"
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
        result = boomi_client.organization_component.get_organization_component(
            id_=organization_id
        )

        # Extract contact info with normalized contact_* field names (matches input config format)
        org_data = {
            "component_id": getattr(result, 'component_id', None) or organization_id,
            "name": getattr(result, 'component_name', None),
            "folder_id": getattr(result, 'folder_id', None),
            "folder_name": getattr(result, 'folder_name', None),
            "deleted": getattr(result, 'deleted', False),
        }

        contact = getattr(result, 'organization_contact_info', None)
        if contact:
            contact_fields = {
                "contact_name": getattr(contact, 'contact_name', None),
                "contact_email": getattr(contact, 'email', None),
                "contact_phone": getattr(contact, 'phone', None),
                "contact_fax": getattr(contact, 'fax', None),
                "contact_url": getattr(contact, 'contact_url', None),
                "contact_address": getattr(contact, 'address1', None),
                "contact_address2": getattr(contact, 'address2', None),
                "contact_city": getattr(contact, 'city', None),
                "contact_state": getattr(contact, 'state', None),
                "contact_country": getattr(contact, 'country', None),
                "contact_postalcode": getattr(contact, 'postalcode', None),
            }
            # Flatten into org_data, skip empty values
            for k, v in contact_fields.items():
                if v:
                    org_data[k] = v

        return {
            "_success": True,
            "organization": org_data
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
        # Build query expression
        if filters and "name_pattern" in filters:
            expression = OrganizationComponentSimpleExpression(
                operator=OrganizationComponentSimpleExpressionOperator.LIKE,
                property=OrganizationComponentSimpleExpressionProperty.NAME,
                argument=[filters["name_pattern"]]
            )
        else:
            # Get all organizations
            expression = OrganizationComponentSimpleExpression(
                operator=OrganizationComponentSimpleExpressionOperator.LIKE,
                property=OrganizationComponentSimpleExpressionProperty.NAME,
                argument=['%']
            )

        query_filter = OrganizationComponentQueryConfigQueryFilter(expression=expression)
        query_config = OrganizationComponentQueryConfig(query_filter=query_filter)

        result = boomi_client.organization_component.query_organization_component(
            request_body=query_config
        )

        organizations = []
        if hasattr(result, 'result') and result.result:
            for org in result.result:
                org_id = getattr(org, 'component_id', None) or getattr(org, 'id_', None)
                organizations.append({
                    "component_id": org_id,
                    "name": getattr(org, 'component_name', None),
                    "folder_name": getattr(org, 'folder_name', None),
                    "folder_id": getattr(org, 'folder_id', None)
                })

        return {
            "_success": True,
            "total_count": len(organizations),
            "organizations": organizations
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
        # Get existing organization
        existing_org = boomi_client.organization_component.get_organization_component(
            id_=organization_id
        )

        # Update component name if provided
        if "component_name" in updates:
            existing_org.component_name = updates["component_name"]

        if "folder_name" in updates:
            existing_org.folder_name = updates["folder_name"]

        # Boomi API REQUIRES OrganizationContactInfo to be present in update payload
        # Build contact info from existing values merged with any updates
        existing_contact = getattr(existing_org, 'organization_contact_info', None)

        # Get existing values or defaults
        contact_params = {
            'contact_name': getattr(existing_contact, 'contact_name', '') if existing_contact else '',
            'contact_email': getattr(existing_contact, 'email', '') if existing_contact else '',
            'contact_phone': getattr(existing_contact, 'phone', '') if existing_contact else '',
            'contact_fax': getattr(existing_contact, 'fax', '') if existing_contact else '',
            'contact_url': getattr(existing_contact, 'contact_url', '') if existing_contact else '',
            'contact_address': getattr(existing_contact, 'address1', '') if existing_contact else '',
            'contact_address2': getattr(existing_contact, 'address2', '') if existing_contact else '',
            'contact_city': getattr(existing_contact, 'city', '') if existing_contact else '',
            'contact_state': getattr(existing_contact, 'state', '') if existing_contact else '',
            'contact_country': getattr(existing_contact, 'country', '') if existing_contact else '',
            'contact_postalcode': getattr(existing_contact, 'postalcode', '') if existing_contact else ''
        }

        # Override with any contact updates
        contact_updates = {k: v for k, v in updates.items() if k.startswith('contact_')}
        contact_params.update(contact_updates)

        # Always set contact info (required by Boomi API)
        existing_org.organization_contact_info = build_organization_contact_info(**contact_params)

        # Update organization
        result = boomi_client.organization_component.update_organization_component(
            id_=organization_id,
            request_body=existing_org
        )

        return {
            "_success": True,
            "organization": {
                "component_id": organization_id,
                "name": updates.get("component_name", getattr(existing_org, 'component_name', None)),
                "updated_fields": list(updates.keys())
            },
            "message": f"Successfully updated organization: {organization_id}"
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
                    "error": "request_data is required for 'create' action",
                    "hint": "Provide organization configuration including component_name and contact info"
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

    except Exception as e:
        return {
            "_success": False,
            "error": f"Action '{action}' failed: {str(e)}",
            "exception_type": type(e).__name__
        }
