#!/usr/bin/env python3
"""
Trading Partner MCP Tools for Boomi API Integration.

This module provides comprehensive trading partner management capabilities
including CRUD operations, bulk operations, and querying for B2B/EDI partners.

Supported Standards:
- X12 (EDI)
- EDIFACT
- HL7 (Healthcare)
- RosettaNet
- TRADACOMS
- ODETTE
- Custom formats
"""

from typing import Dict, Any, List, Optional
import json
from datetime import datetime
import xml.etree.ElementTree as ET

# Import typed models for query operations
from boomi.models import (
    TradingPartnerComponentQueryConfig,
    TradingPartnerComponentQueryConfigQueryFilter,
    TradingPartnerComponentSimpleExpression,
    TradingPartnerComponentSimpleExpressionOperator,
    TradingPartnerComponentSimpleExpressionProperty
)


# ============================================================================
# Trading Partner CRUD Operations
# ============================================================================

def create_trading_partner(boomi_client, profile: str, request_data: Dict[str, Any]) -> Dict[str, Any]:
    """
    Create a new trading partner component in Boomi using JSON-based TradingPartnerComponent API.

    This implementation uses the typed JSON models from the boomi-python SDK
    instead of XML templates, providing better type safety and maintainability.

    Args:
        boomi_client: Authenticated Boomi SDK client
        profile: Profile name for authentication
        request_data: Trading partner configuration including:
            - component_name: Name of the trading partner (required)
            - standard: Trading standard - x12, edifact, hl7, rosettanet, custom, tradacoms, or odette (default: x12)
            - classification: Classification type (default: tradingpartner)
            - folder_name: Folder name (default: Home)
            - description: Component description (optional)

            # Contact Information (10 fields)
            - contact_name, contact_email, contact_phone, contact_fax
            - contact_address, contact_address2, contact_city, contact_state, contact_country, contact_postalcode

            # Communication Protocols
            - communication_protocols: Comma-separated list or list of protocols (ftp, sftp, http, as2, mllp, oftp, disk)

            # Protocol-specific fields (see trading_partner_builders.py for details)
            - disk_*, ftp_*, sftp_*, http_*, as2_*, oftp_*

            # Standard-specific fields (see trading_partner_builders.py for details)
            - isa_id, isa_qualifier, gs_id (X12)
            - unb_* (EDIFACT)
            - sending_*, receiving_* (HL7)
            - duns_number, global_location_number (RosettaNet)
            - sender_code, recipient_code (TRADACOMS)
            - originator_code, destination_code (ODETTE)
            - custom_partner_info (dict for custom standard)

    Returns:
        Created trading partner details or error

    Example:
        request_data = {
            "component_name": "My Trading Partner",
            "standard": "x12",
            "classification": "tradingpartner",
            "folder_name": "Home",
            "contact_email": "partner@example.com",
            "isa_id": "MYPARTNER",
            "isa_qualifier": "01",
            "communication_protocols": "http",
            "http_url": "https://partner.example.com/edi"
        }
    """
    try:
        # Import the JSON model builder
        import sys
        import os
        sys.path.insert(0, os.path.join(os.path.dirname(__file__), '../../../'))
        from boomi_mcp.models.trading_partner_builders import build_trading_partner_model

        # Validate required fields
        if not request_data.get("component_name"):
            return {
                "_success": False,
                "error": "component_name is required",
                "message": "Trading partner name (component_name) is required"
            }

        # Extract main fields and pass remaining fields as kwargs
        component_name = request_data.get("component_name")
        standard = request_data.get("standard", "x12")
        classification = request_data.get("classification", "tradingpartner")
        folder_name = request_data.get("folder_name", "Home")
        description = request_data.get("description", "")

        # Remove main fields from request_data to avoid duplicate kwargs
        other_params = {k: v for k, v in request_data.items()
                       if k not in ["component_name", "standard", "classification", "folder_name", "description"]}

        # Use SDK models for all protocols
        try:
            tp_model = build_trading_partner_model(
                component_name=component_name,
                standard=standard,
                classification=classification,
                folder_name=folder_name,
                description=description,
                **other_params  # Pass all other parameters
            )
        except ValueError as ve:
            return {
                "_success": False,
                "error": str(ve),
                "message": f"Invalid trading partner configuration: {str(ve)}"
            }

        # Create trading partner using TradingPartnerComponent API (JSON-based)
        result = boomi_client.trading_partner_component.create_trading_partner_component(
            request_body=tp_model
        )

        # Extract component ID using the same pattern as SDK example
        # SDK uses 'id_' attribute, not 'component_id'
        component_id = None
        if hasattr(result, 'id_'):
            component_id = result.id_
        elif hasattr(result, 'component_id'):
            component_id = result.component_id
        elif hasattr(result, 'id'):
            component_id = result.id

        return {
            "_success": True,
            "trading_partner": {
                "component_id": component_id,
                "name": getattr(result, 'name', request_data.get("component_name")),
                "standard": request_data.get("standard", "x12"),
                "classification": request_data.get("classification", "tradingpartner"),
                "folder_name": request_data.get("folder_name", "Home")
            },
            "message": f"Successfully created trading partner: {request_data.get('component_name')}"
        }

    except Exception as e:
        error_msg = str(e)
        # Provide helpful error messages for common issues
        if "B2B" in error_msg or "EDI" in error_msg:
            error_msg = f"{error_msg}. Note: Account must have B2B/EDI feature enabled for trading partner creation."

        return {
            "_success": False,
            "error": str(e),
            "message": f"Failed to create trading partner: {error_msg}"
        }


def get_trading_partner(boomi_client, profile: str, component_id: str) -> Dict[str, Any]:
    """
    Get details of a specific trading partner by ID.

    This implementation aligns with the boomi-python SDK example pattern,
    using id_ parameter and proper attribute access.

    Args:
        boomi_client: Authenticated Boomi SDK client
        profile: Profile name for authentication
        component_id: Trading partner component ID

    Returns:
        Trading partner details or error
    """
    try:
        # Use SDK directly - model deserialization is now fixed
        result = boomi_client.trading_partner_component.get_trading_partner_component(
            id_=component_id
        )

        # Extract using SDK model attributes
        retrieved_id = None
        if hasattr(result, 'id_'):
            retrieved_id = result.id_
        elif hasattr(result, 'id'):
            retrieved_id = result.id
        elif hasattr(result, 'component_id'):
            retrieved_id = result.component_id
        else:
            retrieved_id = component_id

        # Extract partner details (use snake_case for JSON API attributes)
        partner_info = {}
        info = getattr(result, 'partner_info', None)
        if info:
            # X12 partner info
            x12_info = getattr(info, 'x12_partner_info', None)
            if x12_info:
                x12_ctrl = getattr(x12_info, 'x12_control_info', None)
                if x12_ctrl:
                    isa_ctrl = getattr(x12_ctrl, 'isa_control_info', None)
                    gs_ctrl = getattr(x12_ctrl, 'gs_control_info', None)
                    if isa_ctrl:
                        partner_info["isa_id"] = getattr(isa_ctrl, 'interchange_id', None)
                        partner_info["isa_qualifier"] = getattr(isa_ctrl, 'interchange_id_qualifier', None)
                    if gs_ctrl:
                        partner_info["gs_id"] = getattr(gs_ctrl, 'applicationcode', None)

        contact_info = {}
        communication_protocols = []

        # Use object attributes for SDK model
        contact = getattr(result, 'contact_info', None)
        if contact:
            raw_contact = {
                "name": getattr(contact, 'contact_name', None),
                "email": getattr(contact, 'email', None),
                "phone": getattr(contact, 'phone', None),
                "address1": getattr(contact, 'address1', None),
                "address2": getattr(contact, 'address2', None),
                "city": getattr(contact, 'city', None),
                "state": getattr(contact, 'state', None),
                "country": getattr(contact, 'country', None),
                "postalcode": getattr(contact, 'postalcode', None),
                "fax": getattr(contact, 'fax', None)
            }
            contact_info = {k: v for k, v in raw_contact.items() if v}

        # Parse partner_communication for communication protocols
        comm = getattr(result, 'partner_communication', None)
        if comm:
            # Disk protocol
            if getattr(comm, 'disk_communication_options', None):
                disk_opts = comm.disk_communication_options
                disk_info = {"protocol": "disk"}
                get_opts = getattr(disk_opts, 'disk_get_options', None)
                send_opts = getattr(disk_opts, 'disk_send_options', None)
                if get_opts:
                    disk_info["get_directory"] = getattr(get_opts, 'get_directory', None)
                    disk_info["file_filter"] = getattr(get_opts, 'file_filter', None)
                if send_opts:
                    disk_info["send_directory"] = getattr(send_opts, 'send_directory', None)
                communication_protocols.append(disk_info)

            # FTP protocol
            if getattr(comm, 'ftp_communication_options', None):
                ftp_opts = comm.ftp_communication_options
                ftp_info = {"protocol": "ftp"}
                settings = getattr(ftp_opts, 'ftp_settings', None)
                if settings:
                    ftp_info["host"] = getattr(settings, 'host', None)
                    ftp_info["port"] = getattr(settings, 'port', None)
                    ftp_info["user"] = getattr(settings, 'user', None)
                    ftp_info["connection_mode"] = getattr(settings, 'connection_mode', None)
                    # Extract FTP SSL options
                    ftpssl_opts = getattr(settings, 'ftpssl_options', None)
                    if ftpssl_opts:
                        ftp_info["ssl_mode"] = getattr(ftpssl_opts, 'sslmode', None)
                        ftp_info["use_client_authentication"] = getattr(ftpssl_opts, 'use_client_authentication', None)
                get_opts = getattr(ftp_opts, 'ftp_get_options', None)
                if get_opts:
                    ftp_info["remote_directory"] = getattr(get_opts, 'remote_directory', None)
                # Filter out None values
                ftp_info = {k: v for k, v in ftp_info.items() if v is not None}
                communication_protocols.append(ftp_info)

            # SFTP protocol
            if getattr(comm, 'sftp_communication_options', None):
                sftp_opts = comm.sftp_communication_options
                sftp_info = {"protocol": "sftp"}
                settings = getattr(sftp_opts, 'sftp_settings', None)
                if settings:
                    sftp_info["host"] = getattr(settings, 'host', None)
                    sftp_info["port"] = getattr(settings, 'port', None)
                    sftp_info["user"] = getattr(settings, 'user', None)
                    # Extract SFTP SSH options
                    sftpssh_opts = getattr(settings, 'sftpssh_options', None)
                    if sftpssh_opts:
                        sftp_info["ssh_key_auth"] = getattr(sftpssh_opts, 'sshkeyauth', None)
                        sftp_info["known_host_entry"] = getattr(sftpssh_opts, 'known_host_entry', None)
                        sftp_info["ssh_key_path"] = getattr(sftpssh_opts, 'sshkeypath', None)
                    # Extract SFTP proxy settings
                    proxy_settings = getattr(settings, 'sftp_proxy_settings', None)
                    if proxy_settings:
                        sftp_info["proxy_host"] = getattr(proxy_settings, 'host', None)
                        sftp_info["proxy_port"] = getattr(proxy_settings, 'port', None)
                        sftp_info["proxy_type"] = getattr(proxy_settings, 'type_', None)
                get_opts = getattr(sftp_opts, 'sftp_get_options', None)
                if get_opts:
                    sftp_info["remote_directory"] = getattr(get_opts, 'remote_directory', None)
                # Filter out None values
                sftp_info = {k: v for k, v in sftp_info.items() if v is not None}
                communication_protocols.append(sftp_info)

            # HTTP protocol
            if getattr(comm, 'http_communication_options', None):
                http_opts = comm.http_communication_options
                http_info = {"protocol": "http"}
                settings = getattr(http_opts, 'http_settings', None)
                if settings:
                    http_info["url"] = getattr(settings, 'url', None)
                    http_info["authentication_type"] = getattr(settings, 'authentication_type', None)
                    http_info["connect_timeout"] = getattr(settings, 'connect_timeout', None)
                    http_info["read_timeout"] = getattr(settings, 'read_timeout', None)
                    # Extract HTTP auth settings
                    http_auth = getattr(settings, 'http_auth_settings', None)
                    if http_auth:
                        http_info["username"] = getattr(http_auth, 'user', None)
                    # Extract HTTP SSL options
                    httpssl_opts = getattr(settings, 'httpssl_options', None)
                    if httpssl_opts:
                        http_info["client_auth"] = getattr(httpssl_opts, 'clientauth', None)
                        http_info["trust_server_cert"] = getattr(httpssl_opts, 'trust_server_cert', None)
                # Extract HTTP send options
                send_opts = getattr(http_opts, 'http_send_options', None)
                if send_opts:
                    http_info["method_type"] = getattr(send_opts, 'method_type', None)
                    http_info["data_content_type"] = getattr(send_opts, 'data_content_type', None)
                    http_info["follow_redirects"] = getattr(send_opts, 'follow_redirects', None)
                    http_info["return_errors"] = getattr(send_opts, 'return_errors', None)
                # Filter out None values
                http_info = {k: v for k, v in http_info.items() if v is not None}
                communication_protocols.append(http_info)

            # AS2 protocol
            if getattr(comm, 'as2_communication_options', None):
                as2_opts = comm.as2_communication_options
                as2_info = {"protocol": "as2"}

                # Extract AS2SendSettings
                settings = getattr(as2_opts, 'as2_send_settings', None)
                if settings:
                    as2_info["url"] = getattr(settings, 'url', None)
                    as2_info["authentication_type"] = getattr(settings, 'authentication_type', None)
                    as2_info["verify_hostname"] = getattr(settings, 'verify_hostname', None)
                    # Extract basic auth info
                    auth_settings = getattr(settings, 'auth_settings', None)
                    if auth_settings:
                        as2_info["username"] = getattr(auth_settings, 'username', None)

                # Extract AS2SendOptions
                send_options = getattr(as2_opts, 'as2_send_options', None)
                if send_options:
                    # Partner info (as2_id)
                    partner_info = getattr(send_options, 'as2_partner_info', None)
                    if partner_info:
                        as2_info["as2_partner_id"] = getattr(partner_info, 'as2_id', None)

                    # Message options
                    msg_opts = getattr(send_options, 'as2_message_options', None)
                    if msg_opts:
                        as2_info["signed"] = getattr(msg_opts, 'signed', None)
                        as2_info["encrypted"] = getattr(msg_opts, 'encrypted', None)
                        as2_info["compressed"] = getattr(msg_opts, 'compressed', None)
                        as2_info["encryption_algorithm"] = getattr(msg_opts, 'encryption_algorithm', None)
                        as2_info["signing_digest_alg"] = getattr(msg_opts, 'signing_digest_alg', None)

                    # MDN options
                    mdn_opts = getattr(send_options, 'as2_mdn_options', None)
                    if mdn_opts:
                        as2_info["request_mdn"] = getattr(mdn_opts, 'request_mdn', None)
                        as2_info["mdn_signed"] = getattr(mdn_opts, 'signed', None)
                        as2_info["synchronous_mdn"] = getattr(mdn_opts, 'synchronous', None)

                # Filter out None values
                as2_info = {k: v for k, v in as2_info.items() if v is not None}
                communication_protocols.append(as2_info)

            # MLLP protocol
            if getattr(comm, 'mllp_communication_options', None):
                mllp_opts = comm.mllp_communication_options
                mllp_info = {"protocol": "mllp"}
                settings = getattr(mllp_opts, 'mllp_send_settings', None)
                if settings:
                    mllp_info["host"] = getattr(settings, 'host', None)
                    mllp_info["port"] = getattr(settings, 'port', None)
                    mllp_info["persistent"] = getattr(settings, 'persistent', None)
                    mllp_info["receive_timeout"] = getattr(settings, 'receive_timeout', None)
                    mllp_info["send_timeout"] = getattr(settings, 'send_timeout', None)
                    mllp_info["max_connections"] = getattr(settings, 'max_connections', None)
                    mllp_info["inactivity_timeout"] = getattr(settings, 'inactivity_timeout', None)
                    mllp_info["max_retry"] = getattr(settings, 'max_retry', None)
                    # Extract MLLP SSL options
                    mllpssl_opts = getattr(settings, 'mllpssl_options', None)
                    if mllpssl_opts:
                        mllp_info["use_ssl"] = getattr(mllpssl_opts, 'use_ssl', None)
                        mllp_info["use_client_ssl"] = getattr(mllpssl_opts, 'use_client_ssl', None)
                        mllp_info["client_ssl_alias"] = getattr(mllpssl_opts, 'client_ssl_alias', None)
                        mllp_info["ssl_alias"] = getattr(mllpssl_opts, 'ssl_alias', None)
                # Filter out None values
                mllp_info = {k: v for k, v in mllp_info.items() if v is not None}
                communication_protocols.append(mllp_info)

            # OFTP protocol
            if getattr(comm, 'oftp_communication_options', None):
                oftp_opts = comm.oftp_communication_options
                oftp_info = {"protocol": "oftp"}
                conn_settings = getattr(oftp_opts, 'oftp_connection_settings', None)
                if conn_settings:
                    # Check both direct attrs and default_oftp_connection_settings
                    default_settings = getattr(conn_settings, 'default_oftp_connection_settings', None)
                    # Try direct attributes first, fall back to default settings
                    oftp_info["host"] = getattr(conn_settings, 'host', None) or (getattr(default_settings, 'host', None) if default_settings else None)
                    oftp_info["port"] = getattr(conn_settings, 'port', None) or (getattr(default_settings, 'port', None) if default_settings else None)
                    oftp_info["tls"] = getattr(conn_settings, 'tls', None) if hasattr(conn_settings, 'tls') else (getattr(default_settings, 'tls', None) if default_settings else None)
                    oftp_info["ssid_auth"] = getattr(conn_settings, 'ssidauth', None) if hasattr(conn_settings, 'ssidauth') else (getattr(default_settings, 'ssidauth', None) if default_settings else None)
                    oftp_info["sfid_cipher"] = getattr(conn_settings, 'sfidciph', None) if hasattr(conn_settings, 'sfidciph') else (getattr(default_settings, 'sfidciph', None) if default_settings else None)
                    oftp_info["use_gateway"] = getattr(conn_settings, 'use_gateway', None) if hasattr(conn_settings, 'use_gateway') else (getattr(default_settings, 'use_gateway', None) if default_settings else None)
                    oftp_info["use_client_ssl"] = getattr(conn_settings, 'use_client_ssl', None) if hasattr(conn_settings, 'use_client_ssl') else (getattr(default_settings, 'use_client_ssl', None) if default_settings else None)
                    oftp_info["client_ssl_alias"] = getattr(conn_settings, 'client_ssl_alias', None) or (getattr(default_settings, 'client_ssl_alias', None) if default_settings else None)
                    # Extract partner info from both locations
                    partner_info = getattr(conn_settings, 'my_partner_info', None) or (getattr(default_settings, 'my_partner_info', None) if default_settings else None)
                    if partner_info:
                        oftp_info["ssid_code"] = getattr(partner_info, 'ssidcode', None)
                        oftp_info["compress"] = getattr(partner_info, 'ssidcmpr', None)
                # Filter out None values
                oftp_info = {k: v for k, v in oftp_info.items() if v is not None}
                communication_protocols.append(oftp_info)

        return {
            "_success": True,
            "trading_partner": {
                "component_id": retrieved_id,
                "name": getattr(result, 'name', getattr(result, 'component_name', None)),
                "standard": getattr(result, 'standard', None),
                "classification": getattr(result, 'classification', None),
                "folder_id": getattr(result, 'folder_id', None),
                "folder_name": getattr(result, 'folder_name', None),
                "organization_id": getattr(result, 'organization_id', None),
                "deleted": getattr(result, 'deleted', False),
                "partner_info": partner_info if partner_info else None,
                "contact_info": contact_info if contact_info else None,
                "communication_protocols": communication_protocols if communication_protocols else []
            }
        }

    except Exception as e:
        return {
            "_success": False,
            "error": str(e),
            "message": f"Failed to get trading partner: {str(e)}"
        }


def list_trading_partners(boomi_client, profile: str, filters: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    """
    List all trading partners with optional filtering using typed query models.

    This implementation follows the boomi-python SDK example pattern of using
    typed query model classes instead of dictionary-based queries.

    Args:
        boomi_client: Authenticated Boomi SDK client
        profile: Profile name for authentication
        filters: Optional filters including:
            - standard: Filter by standard (x12, edifact, hl7, etc.)
            - classification: Filter by classification (tradingpartner, mycompany)
            - folder_name: Filter by folder
            - name_pattern: Filter by name pattern (supports % wildcard)
            - include_deleted: Include deleted partners (default: False)

    Returns:
        List of trading partners or error
    """
    try:
        # Build query expression using typed models (as shown in SDK example)
        expressions = []

        if filters:
            # Filter by standard
            if "standard" in filters:
                expr = TradingPartnerComponentSimpleExpression(
                    operator=TradingPartnerComponentSimpleExpressionOperator.EQUALS,
                    property=TradingPartnerComponentSimpleExpressionProperty.STANDARD,
                    argument=[filters["standard"].lower()]
                )
                expressions.append(expr)

            # Filter by classification
            if "classification" in filters:
                expr = TradingPartnerComponentSimpleExpression(
                    operator=TradingPartnerComponentSimpleExpressionOperator.EQUALS,
                    property=TradingPartnerComponentSimpleExpressionProperty.CLASSIFICATION,
                    argument=[filters["classification"].lower()]
                )
                expressions.append(expr)

            # Filter by name pattern
            if "name_pattern" in filters:
                expr = TradingPartnerComponentSimpleExpression(
                    operator=TradingPartnerComponentSimpleExpressionOperator.LIKE,
                    property=TradingPartnerComponentSimpleExpressionProperty.NAME,
                    argument=[filters["name_pattern"]]
                )
                expressions.append(expr)

            # Note: NOT_EQUALS operator not available in typed models
            # Deleted filtering would need to be done client-side if needed

        # If no filters provided, get all trading partners
        if not expressions:
            expression = TradingPartnerComponentSimpleExpression(
                operator=TradingPartnerComponentSimpleExpressionOperator.LIKE,
                property=TradingPartnerComponentSimpleExpressionProperty.NAME,
                argument=['%']
            )
        elif len(expressions) == 1:
            expression = expressions[0]
        else:
            # Multiple expressions - use first one (compound expressions not yet supported)
            expression = expressions[0]

        # Build typed query config
        query_filter = TradingPartnerComponentQueryConfigQueryFilter(expression=expression)
        query_config = TradingPartnerComponentQueryConfig(query_filter=query_filter)

        # Query trading partners using typed config
        result = boomi_client.trading_partner_component.query_trading_partner_component(
            request_body=query_config
        )

        partners = []
        if hasattr(result, 'result') and result.result:
            for partner in result.result:
                # Extract ID using SDK pattern (id_ attribute)
                partner_id = None
                if hasattr(partner, 'id_'):
                    partner_id = partner.id_
                elif hasattr(partner, 'id'):
                    partner_id = partner.id
                elif hasattr(partner, 'component_id'):
                    partner_id = partner.component_id

                partners.append({
                    "component_id": partner_id,
                    "name": getattr(partner, 'name', getattr(partner, 'component_name', None)),
                    "standard": getattr(partner, 'standard', None),
                    "classification": getattr(partner, 'classification', None),
                    "folder_name": getattr(partner, 'folder_name', None),
                    "deleted": getattr(partner, 'deleted', False)
                })

        # Group partners by standard
        grouped = {}
        for partner in partners:
            standard = partner.get("standard", "unknown")
            if standard:
                standard_upper = standard.upper()
                if standard_upper not in grouped:
                    grouped[standard_upper] = []
                grouped[standard_upper].append(partner)

        return {
            "_success": True,
            "total_count": len(partners),
            "partners": partners,
            "by_standard": grouped,
            "summary": {
                "x12": len(grouped.get("X12", [])),
                "edifact": len(grouped.get("EDIFACT", [])),
                "hl7": len(grouped.get("HL7", [])),
                "custom": len(grouped.get("CUSTOM", [])),
                "rosettanet": len(grouped.get("ROSETTANET", [])),
                "tradacoms": len(grouped.get("TRADACOMS", [])),
                "odette": len(grouped.get("ODETTE", []))
            }
        }

    except Exception as e:
        return {
            "_success": False,
            "error": str(e),
            "message": f"Failed to list trading partners: {str(e)}"
        }


def update_trading_partner(boomi_client, profile: str, component_id: str, updates: Dict[str, Any]) -> Dict[str, Any]:
    """
    Update an existing trading partner component using JSON-based TradingPartnerComponent API.

    This implementation uses the typed JSON models for a much simpler update process:
    1. Get existing trading partner using trading_partner_component.get_trading_partner_component()
    2. Update the model fields based on the updates dict
    3. Call trading_partner_component.update_trading_partner_component() with updated model

    Args:
        boomi_client: Authenticated Boomi SDK client
        profile: Profile name for authentication
        component_id: Trading partner component ID to update
        updates: Fields to update including:
            - component_name: Trading partner name
            - description: Component description
            - classification: Partner type (tradingpartner or mycompany)
            - folder_name: Folder location

            # Contact Information
            - contact_name, contact_email, contact_phone, contact_fax
            - contact_address, contact_address2, contact_city, contact_state, contact_country, contact_postalcode

            # Communication Protocols (not yet fully implemented)
            - communication_protocols: List of protocols
            - Protocol-specific fields (disk_*, ftp_*, sftp_*, http_*, as2_*, oftp_*)

            # Standard-specific fields (not yet fully implemented)
            - isa_id, isa_qualifier, gs_id (X12)
            - unb_* (EDIFACT)
            - sending_*, receiving_* (HL7)
            - etc.

    Returns:
        Updated trading partner details or error

    Note:
        Full implementation of protocol-specific and standard-specific updates
        will be added in future iterations. Currently supports basic fields.
    """
    try:
        # Import the JSON model builder
        import sys
        import os
        sys.path.insert(0, os.path.join(os.path.dirname(__file__), '../../../'))
        from boomi_mcp.models.trading_partner_builders import build_contact_info
        from boomi.models import ContactInfo

        # Step 1: Get the existing trading partner using JSON-based API
        try:
            existing_tp = boomi_client.trading_partner_component.get_trading_partner_component(
                id_=component_id
            )
        except Exception as e:
            return {
                "_success": False,
                "error": f"Component not found: {str(e)}",
                "message": f"Trading partner {component_id} not found or could not be retrieved"
            }

        # Step 2: Update model fields based on updates dict
        # The SDK's PartnerCommunication._map() now produces minimal structure that the API accepts,
        # so communications are automatically preserved during updates.

        # Check if protocol updates were specified (these will REPLACE existing communications)
        # Support both nested format (*_settings) and flat format (*_host, *_url, etc.)
        from boomi_mcp.models.trading_partner_builders import PartnerCommunicationDict
        flat_protocol_prefixes = ["ftp_", "sftp_", "http_", "as2_", "disk_", "mllp_", "oftp_"]
        has_flat_protocol_updates = any(
            any(key.startswith(prefix) for prefix in flat_protocol_prefixes)
            for key in updates
        )
        has_nested_protocol_updates = any(key in updates for key in [
            "as2_settings", "http_settings", "sftp_settings", "ftp_settings", "disk_settings"
        ])
        has_protocol_updates = has_flat_protocol_updates or has_nested_protocol_updates or "communication_protocols" in updates

        # Update basic component fields
        if "component_name" in updates:
            existing_tp.component_name = updates["component_name"]

        if "description" in updates:
            existing_tp.description = updates["description"]

        if "classification" in updates:
            from boomi.models import TradingPartnerComponentClassification
            classification = updates["classification"]
            if isinstance(classification, str):
                if classification.lower() == "mycompany":
                    existing_tp.classification = TradingPartnerComponentClassification.MYCOMPANY
                else:
                    existing_tp.classification = TradingPartnerComponentClassification.TRADINGPARTNER
            else:
                existing_tp.classification = classification

        if "folder_name" in updates:
            existing_tp.folder_name = updates["folder_name"]

        # Update contact information
        # Support both nested dict format and flat parameter format
        # IMPORTANT: Merge with existing contact info to preserve unchanged fields
        contact_updates = {}
        if "contact_info" in updates:
            # Nested format
            contact_updates = updates["contact_info"]
        else:
            # Flat format - extract contact_* parameters
            for key in updates:
                if key.startswith('contact_'):
                    contact_updates[key] = updates[key]

        if contact_updates:
            # First, get existing contact info values to preserve unchanged fields
            existing_contact = getattr(existing_tp, 'contact_info', None)
            merged_contact = {}

            if existing_contact:
                # Extract existing values
                merged_contact = {
                    'contact_name': getattr(existing_contact, 'contact_name', None) or getattr(existing_contact, 'name', None) or '',
                    'contact_email': getattr(existing_contact, 'email', '') or '',
                    'contact_phone': getattr(existing_contact, 'phone', '') or '',
                    'contact_fax': getattr(existing_contact, 'fax', '') or '',
                    'contact_address': getattr(existing_contact, 'address1', '') or '',
                    'contact_address2': getattr(existing_contact, 'address2', '') or '',
                    'contact_city': getattr(existing_contact, 'city', '') or '',
                    'contact_state': getattr(existing_contact, 'state', '') or '',
                    'contact_country': getattr(existing_contact, 'country', '') or '',
                    'contact_postalcode': getattr(existing_contact, 'postalcode', '') or '',
                }

            # Merge updates on top of existing values
            merged_contact.update(contact_updates)

            # Build ContactInfo model from merged values
            contact_info = build_contact_info(**merged_contact)
            if contact_info:
                existing_tp.contact_info = contact_info

        # Protocol-specific updates - PRESERVE existing protocols and merge with updates
        if has_protocol_updates:
            from boomi_mcp.models.trading_partner_builders import (
                build_as2_communication_options,
                build_http_communication_options,
                build_sftp_communication_options,
                build_ftp_communication_options,
                build_disk_communication_options,
                build_mllp_communication_options,
                build_oftp_communication_options
            )

            comm_dict = {}

            # First, preserve ALL existing protocols using PartnerCommunication._map()
            # This produces the minimal structure that the API accepts
            existing_comm = getattr(existing_tp, 'partner_communication', None)
            if existing_comm and hasattr(existing_comm, '_map'):
                # Use the SDK's _map() which properly filters to minimal structure
                preserved = existing_comm._map()
                if preserved:
                    # Fix BigInteger format returned by API (e.g., ['BigInteger', 2575] -> 2575)
                    def fix_biginteger_format(obj):
                        if isinstance(obj, dict):
                            return {k: fix_biginteger_format(v) for k, v in obj.items()}
                        elif isinstance(obj, list):
                            if len(obj) == 2 and obj[0] == 'BigInteger':
                                return obj[1]
                            return [fix_biginteger_format(item) for item in obj]
                        return obj
                    preserved = fix_biginteger_format(preserved)
                    comm_dict.update(preserved)

            # Handle flat parameters (preferred format from server.py)
            # These will UPDATE or ADD protocols on top of preserved ones
            if has_flat_protocol_updates:
                # Extract flat params by prefix
                as2_params = {k: v for k, v in updates.items() if k.startswith('as2_')}
                http_params = {k: v for k, v in updates.items() if k.startswith('http_')}
                sftp_params = {k: v for k, v in updates.items() if k.startswith('sftp_')}
                ftp_params = {k: v for k, v in updates.items() if k.startswith('ftp_')}
                disk_params = {k: v for k, v in updates.items() if k.startswith('disk_')}

                if as2_params:
                    # For updates, merge with existing AS2 values if as2_url not provided
                    if 'as2_url' not in as2_params:
                        # Try to get existing AS2 URL from the trading partner
                        existing_comm = getattr(existing_tp, 'partner_communication', None)
                        if existing_comm:
                            existing_as2 = getattr(existing_comm, 'as2_communication_options', None)
                            if existing_as2:
                                existing_send_settings = getattr(existing_as2, 'as2_send_settings', None)
                                if existing_send_settings:
                                    existing_url = getattr(existing_send_settings, 'url', None)
                                    if existing_url:
                                        as2_params['as2_url'] = existing_url
                                    existing_auth = getattr(existing_send_settings, 'authentication_type', None)
                                    if existing_auth and 'as2_authentication_type' not in as2_params:
                                        as2_params['as2_authentication_type'] = existing_auth
                                # Also get existing partner ID if not provided
                                existing_send_opts = getattr(existing_as2, 'as2_send_options', None)
                                if existing_send_opts:
                                    existing_partner_info = getattr(existing_send_opts, 'as2_partner_info', None)
                                    if existing_partner_info and 'as2_partner_identifier' not in as2_params:
                                        existing_partner_id = getattr(existing_partner_info, 'as2_id', None)
                                        if existing_partner_id:
                                            as2_params['as2_partner_identifier'] = existing_partner_id

                    as2_opts = build_as2_communication_options(**as2_params)
                    if as2_opts:
                        comm_dict["AS2CommunicationOptions"] = as2_opts

                if http_params:
                    # Merge with existing HTTP values for partial updates
                    if 'http_url' not in http_params:
                        existing_comm = getattr(existing_tp, 'partner_communication', None)
                        if existing_comm:
                            existing_http = getattr(existing_comm, 'http_communication_options', None)
                            if existing_http:
                                existing_settings = getattr(existing_http, 'http_settings', None)
                                if existing_settings:
                                    existing_url = getattr(existing_settings, 'url', None)
                                    if existing_url:
                                        http_params['http_url'] = existing_url
                                    if 'http_authentication_type' not in http_params:
                                        existing_auth = getattr(existing_settings, 'authentication_type', None)
                                        if existing_auth:
                                            http_params['http_authentication_type'] = existing_auth
                    http_opts = build_http_communication_options(**http_params)
                    if http_opts:
                        comm_dict["HTTPCommunicationOptions"] = http_opts

                if sftp_params:
                    # Merge with existing SFTP values for partial updates
                    if 'sftp_host' not in sftp_params:
                        existing_comm = getattr(existing_tp, 'partner_communication', None)
                        if existing_comm:
                            existing_sftp = getattr(existing_comm, 'sftp_communication_options', None)
                            if existing_sftp:
                                existing_settings = getattr(existing_sftp, 'sftp_settings', None)
                                if existing_settings:
                                    existing_host = getattr(existing_settings, 'host', None)
                                    if existing_host:
                                        sftp_params['sftp_host'] = existing_host
                                    if 'sftp_port' not in sftp_params:
                                        existing_port = getattr(existing_settings, 'port', None)
                                        if existing_port:
                                            sftp_params['sftp_port'] = existing_port
                                    if 'sftp_username' not in sftp_params:
                                        existing_user = getattr(existing_settings, 'user', None)
                                        if existing_user:
                                            sftp_params['sftp_username'] = existing_user
                    sftp_opts = build_sftp_communication_options(**sftp_params)
                    if sftp_opts:
                        comm_dict["SFTPCommunicationOptions"] = sftp_opts

                if ftp_params:
                    # Merge with existing FTP values for partial updates
                    existing_comm = getattr(existing_tp, 'partner_communication', None)
                    if existing_comm:
                        existing_ftp = getattr(existing_comm, 'ftp_communication_options', None)
                        if existing_ftp:
                            existing_settings = getattr(existing_ftp, 'ftp_settings', None)
                            if existing_settings:
                                if 'ftp_host' not in ftp_params:
                                    existing_host = getattr(existing_settings, 'host', None)
                                    if existing_host:
                                        ftp_params['ftp_host'] = existing_host
                                if 'ftp_port' not in ftp_params:
                                    existing_port = getattr(existing_settings, 'port', None)
                                    if existing_port:
                                        ftp_params['ftp_port'] = existing_port
                                if 'ftp_username' not in ftp_params:
                                    existing_user = getattr(existing_settings, 'user', None)
                                    if existing_user:
                                        ftp_params['ftp_username'] = existing_user
                                if 'ftp_password' not in ftp_params:
                                    existing_pass = getattr(existing_settings, 'password', None)
                                    if existing_pass:
                                        ftp_params['ftp_password'] = existing_pass
                                if 'ftp_connection_mode' not in ftp_params:
                                    existing_mode = getattr(existing_settings, 'connection_mode', None)
                                    if existing_mode:
                                        ftp_params['ftp_connection_mode'] = existing_mode
                                # Preserve SSL mode from existing SSL options
                                if 'ftp_ssl_mode' not in ftp_params:
                                    existing_ssl = getattr(existing_settings, 'ftpssl_options', None)
                                    if existing_ssl:
                                        existing_ssl_mode = getattr(existing_ssl, 'sslmode', None)
                                        if existing_ssl_mode:
                                            ftp_params['ftp_ssl_mode'] = existing_ssl_mode
                    ftp_opts = build_ftp_communication_options(**ftp_params)
                    if ftp_opts:
                        comm_dict["FTPCommunicationOptions"] = ftp_opts

                if disk_params:
                    # Merge with existing Disk values for partial updates
                    existing_comm = getattr(existing_tp, 'partner_communication', None)
                    if existing_comm:
                        existing_disk = getattr(existing_comm, 'disk_communication_options', None)
                        if existing_disk:
                            if 'disk_get_directory' not in disk_params:
                                existing_get = getattr(existing_disk, 'disk_get_options', None)
                                if existing_get:
                                    existing_dir = getattr(existing_get, 'get_directory', None)
                                    if existing_dir:
                                        disk_params['disk_get_directory'] = existing_dir
                            if 'disk_send_directory' not in disk_params:
                                existing_send = getattr(existing_disk, 'disk_send_options', None)
                                if existing_send:
                                    existing_dir = getattr(existing_send, 'send_directory', None)
                                    if existing_dir:
                                        disk_params['disk_send_directory'] = existing_dir
                    disk_opts = build_disk_communication_options(**disk_params)
                    if disk_opts:
                        comm_dict["DiskCommunicationOptions"] = disk_opts

                # MLLP protocol
                mllp_params = {k: v for k, v in updates.items() if k.startswith('mllp_')}
                if mllp_params:
                    # Merge with existing MLLP values for partial updates
                    existing_comm = getattr(existing_tp, 'partner_communication', None)
                    if existing_comm:
                        existing_mllp = getattr(existing_comm, 'mllp_communication_options', None)
                        if existing_mllp:
                            existing_settings = getattr(existing_mllp, 'mllp_send_settings', None)
                            if existing_settings:
                                if 'mllp_host' not in mllp_params:
                                    existing_host = getattr(existing_settings, 'host', None)
                                    if existing_host:
                                        mllp_params['mllp_host'] = existing_host
                                if 'mllp_port' not in mllp_params:
                                    existing_port = getattr(existing_settings, 'port', None)
                                    if existing_port:
                                        mllp_params['mllp_port'] = existing_port
                                if 'mllp_persistent' not in mllp_params:
                                    existing_persistent = getattr(existing_settings, 'persistent', None)
                                    if existing_persistent is not None:
                                        mllp_params['mllp_persistent'] = existing_persistent
                                if 'mllp_send_timeout' not in mllp_params:
                                    existing_timeout = getattr(existing_settings, 'send_timeout', None)
                                    if existing_timeout:
                                        mllp_params['mllp_send_timeout'] = existing_timeout
                                if 'mllp_receive_timeout' not in mllp_params:
                                    existing_timeout = getattr(existing_settings, 'receive_timeout', None)
                                    if existing_timeout:
                                        mllp_params['mllp_receive_timeout'] = existing_timeout
                    mllp_opts = build_mllp_communication_options(**mllp_params)
                    if mllp_opts:
                        comm_dict["MLLPCommunicationOptions"] = mllp_opts

                # OFTP protocol
                oftp_params = {k: v for k, v in updates.items() if k.startswith('oftp_')}
                if oftp_params:
                    # Merge with existing OFTP values for partial updates
                    existing_comm = getattr(existing_tp, 'partner_communication', None)
                    if existing_comm:
                        existing_oftp = getattr(existing_comm, 'oftp_communication_options', None)
                        if existing_oftp:
                            existing_settings = getattr(existing_oftp, 'oftp_connection_settings', None)
                            # OFTP values are in default_oftp_connection_settings
                            default_settings = getattr(existing_settings, 'default_oftp_connection_settings', None) if existing_settings else None
                            if default_settings:
                                if 'oftp_host' not in oftp_params:
                                    existing_host = getattr(default_settings, 'host', None)
                                    if existing_host:
                                        oftp_params['oftp_host'] = existing_host
                                if 'oftp_port' not in oftp_params:
                                    existing_port = getattr(default_settings, 'port', None)
                                    if existing_port:
                                        oftp_params['oftp_port'] = existing_port
                                if 'oftp_tls' not in oftp_params:
                                    existing_tls = getattr(default_settings, 'tls', None)
                                    if existing_tls is not None:
                                        oftp_params['oftp_tls'] = existing_tls
                                if 'oftp_ssid_auth' not in oftp_params:
                                    existing_auth = getattr(default_settings, 'ssidauth', None)
                                    if existing_auth is not None:
                                        oftp_params['oftp_ssid_auth'] = existing_auth
                                if 'oftp_sfid_cipher' not in oftp_params:
                                    existing_cipher = getattr(default_settings, 'sfidciph', None)
                                    if existing_cipher is not None:
                                        oftp_params['oftp_sfid_cipher'] = existing_cipher
                                if 'oftp_use_gateway' not in oftp_params:
                                    existing_gateway = getattr(default_settings, 'use_gateway', None)
                                    if existing_gateway is not None:
                                        oftp_params['oftp_use_gateway'] = existing_gateway
                                if 'oftp_use_client_ssl' not in oftp_params:
                                    existing_client_ssl = getattr(default_settings, 'use_client_ssl', None)
                                    if existing_client_ssl is not None:
                                        oftp_params['oftp_use_client_ssl'] = existing_client_ssl
                                if 'oftp_client_ssl_alias' not in oftp_params:
                                    existing_alias = getattr(default_settings, 'client_ssl_alias', None)
                                    if existing_alias:
                                        oftp_params['oftp_client_ssl_alias'] = existing_alias
                                # Get partner info from default_settings
                                partner_info = getattr(default_settings, 'my_partner_info', None)
                                if partner_info:
                                    if 'oftp_ssid_code' not in oftp_params:
                                        existing_code = getattr(partner_info, 'ssidcode', None)
                                        if existing_code:
                                            oftp_params['oftp_ssid_code'] = existing_code
                                    if 'oftp_ssid_password' not in oftp_params:
                                        existing_pwd = getattr(partner_info, 'ssidpswd', None)
                                        if existing_pwd:
                                            oftp_params['oftp_ssid_password'] = existing_pwd
                                    if 'oftp_compress' not in oftp_params:
                                        existing_compress = getattr(partner_info, 'ssidcmpr', None)
                                        if existing_compress is not None:
                                            oftp_params['oftp_compress'] = existing_compress
                                    if 'oftp_sfid_sign' not in oftp_params:
                                        existing_sign = getattr(partner_info, 'sfidsign', None)
                                        if existing_sign is not None:
                                            oftp_params['oftp_sfid_sign'] = existing_sign
                                    if 'oftp_sfid_encrypt' not in oftp_params:
                                        existing_encrypt = getattr(partner_info, 'sfidsec-encrypt', None)
                                        if existing_encrypt is not None:
                                            oftp_params['oftp_sfid_encrypt'] = existing_encrypt
                    oftp_opts = build_oftp_communication_options(**oftp_params)
                    if oftp_opts:
                        comm_dict["OFTPCommunicationOptions"] = oftp_opts

            # Handle nested format (legacy support)
            elif has_nested_protocol_updates:
                # AS2 settings
                if "as2_settings" in updates:
                    as2 = updates["as2_settings"]
                    as2_params = {}
                    if "url" in as2:
                        as2_params["as2_url"] = as2["url"]
                    if "as2_identifier" in as2:
                        as2_params["as2_identifier"] = as2["as2_identifier"]
                    if "partner_as2_identifier" in as2:
                        as2_params["as2_partner_identifier"] = as2["partner_as2_identifier"]
                    if "authentication_type" in as2:
                        as2_params["as2_authentication_type"] = as2["authentication_type"]
                    if "username" in as2:
                        as2_params["as2_username"] = as2["username"]
                    if as2_params:
                        as2_opts = build_as2_communication_options(**as2_params)
                        if as2_opts:
                            comm_dict["AS2CommunicationOptions"] = as2_opts

                # HTTP settings
                if "http_settings" in updates:
                    http = updates["http_settings"]
                    http_params = {}
                    if "url" in http:
                        http_params["http_url"] = http["url"]
                    if "authentication_type" in http:
                        http_params["http_authentication_type"] = http["authentication_type"]
                    if "username" in http:
                        http_params["http_username"] = http["username"]
                    if http_params:
                        http_opts = build_http_communication_options(**http_params)
                        if http_opts:
                            comm_dict["HTTPCommunicationOptions"] = http_opts

                # SFTP settings
                if "sftp_settings" in updates:
                    sftp = updates["sftp_settings"]
                    sftp_params = {}
                    if "host" in sftp:
                        sftp_params["sftp_host"] = sftp["host"]
                    if "port" in sftp:
                        sftp_params["sftp_port"] = sftp["port"]
                    if "username" in sftp:
                        sftp_params["sftp_username"] = sftp["username"]
                    if sftp_params:
                        sftp_opts = build_sftp_communication_options(**sftp_params)
                        if sftp_opts:
                            comm_dict["SFTPCommunicationOptions"] = sftp_opts

                # FTP settings
                if "ftp_settings" in updates:
                    ftp = updates["ftp_settings"]
                    ftp_params = {}
                    if "host" in ftp:
                        ftp_params["ftp_host"] = ftp["host"]
                    if "port" in ftp:
                        ftp_params["ftp_port"] = ftp["port"]
                    if "username" in ftp:
                        ftp_params["ftp_username"] = ftp["username"]
                    if ftp_params:
                        ftp_opts = build_ftp_communication_options(**ftp_params)
                        if ftp_opts:
                            comm_dict["FTPCommunicationOptions"] = ftp_opts

                # Disk settings
                if "disk_settings" in updates:
                    disk = updates["disk_settings"]
                    disk_params = {}
                    if "get_directory" in disk:
                        disk_params["disk_get_directory"] = disk["get_directory"]
                    if "send_directory" in disk:
                        disk_params["disk_send_directory"] = disk["send_directory"]
                    if disk_params:
                        disk_opts = build_disk_communication_options(**disk_params)
                        if disk_opts:
                            comm_dict["DiskCommunicationOptions"] = disk_opts

            # Assign new communications (replaces existing)
            if comm_dict:
                existing_tp.partner_communication = PartnerCommunicationDict(comm_dict)

        # Organization linking
        if "organization_id" in updates:
            existing_tp.organization_id = updates["organization_id"]

        # Fix BigInteger format in existing partner_communication (e.g., MLLP port)
        # This is needed even when there are no protocol updates
        if not has_protocol_updates:
            existing_comm = getattr(existing_tp, 'partner_communication', None)
            if existing_comm and hasattr(existing_comm, '_map'):
                preserved = existing_comm._map()
                if preserved:
                    def fix_biginteger_format(obj):
                        if isinstance(obj, dict):
                            return {k: fix_biginteger_format(v) for k, v in obj.items()}
                        elif isinstance(obj, list):
                            if len(obj) == 2 and obj[0] == 'BigInteger':
                                return obj[1]
                            return [fix_biginteger_format(item) for item in obj]
                        return obj
                    preserved = fix_biginteger_format(preserved)
                    from boomi_mcp.models.trading_partner_builders import PartnerCommunicationDict
                    existing_tp.partner_communication = PartnerCommunicationDict(preserved)

        # Step 3: Update the trading partner using JSON-based API
        result = boomi_client.trading_partner_component.update_trading_partner_component(
            id_=component_id,
            request_body=existing_tp
        )

        return {
            "_success": True,
            "trading_partner": {
                "component_id": component_id,
                "name": updates.get("component_name", getattr(existing_tp, 'component_name', None)),
                "updated_fields": list(updates.keys())
            },
            "message": f"Successfully updated trading partner: {component_id}"
        }

    except Exception as e:
        return {
            "_success": False,
            "error": str(e),
            "message": f"Failed to update trading partner: {str(e)}"
        }


def delete_trading_partner(boomi_client, profile: str, component_id: str) -> Dict[str, Any]:
    """
    Delete a trading partner component.

    Args:
        boomi_client: Authenticated Boomi SDK client
        profile: Profile name for authentication
        component_id: Trading partner component ID to delete

    Returns:
        Deletion confirmation or error
    """
    try:
        result = boomi_client.trading_partner_component.delete_trading_partner_component(component_id)

        return {
            "_success": True,
            "component_id": component_id,
            "deleted": True,
            "message": f"Successfully deleted trading partner: {component_id}"
        }

    except Exception as e:
        return {
            "_success": False,
            "error": str(e),
            "message": f"Failed to delete trading partner: {str(e)}"
        }


def bulk_create_trading_partners(boomi_client, profile: str, partners: List[Dict[str, Any]]) -> Dict[str, Any]:
    """
    Create multiple trading partners in a single operation.

    Args:
        boomi_client: Authenticated Boomi SDK client
        profile: Profile name for authentication
        partners: List of trading partner configurations

    Returns:
        Bulk creation results or error
    """
    try:
        # Prepare bulk request
        bulk_data = {
            "TradingPartnerComponent": []
        }

        for partner_data in partners:
            partner_component = {
                "componentName": partner_data.get("component_name"),
                "standard": partner_data.get("standard", "x12").lower(),
                "classification": partner_data.get("classification", "tradingpartner").lower()
            }

            # Add optional fields
            if "folder_name" in partner_data:
                partner_component["folderName"] = partner_data["folder_name"]
            if "contact_info" in partner_data:
                partner_component["ContactInfo"] = partner_data["contact_info"]
            if "partner_info" in partner_data:
                partner_component["PartnerInfo"] = partner_data["partner_info"]

            bulk_data["TradingPartnerComponent"].append(partner_component)

        # Execute bulk create
        result = boomi_client.trading_partner_component.bulk_create_trading_partner_component(bulk_data)

        created_partners = []
        if hasattr(result, 'TradingPartnerComponent'):
            for partner in result.TradingPartnerComponent:
                created_partners.append({
                    "component_id": getattr(partner, 'component_id', None),
                    "name": getattr(partner, 'component_name', None),
                    "standard": getattr(partner, 'standard', None)
                })

        return {
            "_success": True,
            "created_count": len(created_partners),
            "partners": created_partners,
            "message": f"Successfully created {len(created_partners)} trading partners"
        }

    except Exception as e:
        return {
            "_success": False,
            "error": str(e),
            "message": f"Failed to bulk create trading partners: {str(e)}"
        }


def analyze_trading_partner_usage(boomi_client, profile: str, component_id: str) -> Dict[str, Any]:
    """
    Analyze where a trading partner is used in processes and configurations.

    Uses the ComponentReference API to find all components that reference this trading partner.
    Note: Returns immediate references (one level), not recursive like UI's "Show Where Used".

    Args:
        boomi_client: Authenticated Boomi SDK client
        profile: Profile name for authentication
        component_id: Trading partner component ID

    Returns:
        Usage analysis including processes, connections, and dependencies
    """
    try:
        # Get the trading partner details first using Component API (avoids ContactInfo parsing issues)
        partner = boomi_client.component.get_component(component_id=component_id)
        partner_name = getattr(partner, 'name', 'Unknown')

        # Query for component references using the QUERY endpoint (returns 200 with empty results, not 400)
        from boomi.models import (
            ComponentReferenceQueryConfig,
            ComponentReferenceQueryConfigQueryFilter,
            ComponentReferenceSimpleExpression,
            ComponentReferenceSimpleExpressionOperator,
            ComponentReferenceSimpleExpressionProperty
        )

        # Build query to find all components that reference this trading partner
        expression = ComponentReferenceSimpleExpression(
            operator=ComponentReferenceSimpleExpressionOperator.EQUALS,
            property=ComponentReferenceSimpleExpressionProperty.COMPONENTID,
            argument=[component_id]
        )
        query_filter = ComponentReferenceQueryConfigQueryFilter(expression=expression)
        query_config = ComponentReferenceQueryConfig(query_filter=query_filter)

        # Execute query
        query_result = boomi_client.component_reference.query_component_reference(request_body=query_config)

        # Collect all referenced components
        referenced_by = []

        # Extract references from query results
        if hasattr(query_result, 'result') and query_result.result:
            for result_item in query_result.result:
                # Each result item has a 'references' array
                refs = getattr(result_item, 'references', [])
                if not refs:
                    continue

                for ref in refs:
                    parent_id = getattr(ref, 'parent_component_id', None)
                    parent_version = getattr(ref, 'parent_version', None)

                    if parent_id:
                        # Try to get component metadata
                        try:
                            parent_comp = boomi_client.component.get_component(component_id=parent_id)
                            comp_type = getattr(parent_comp, 'type', 'unknown')
                            comp_name = getattr(parent_comp, 'name', 'Unknown')

                            referenced_by.append({
                                "component_id": parent_id,
                                "name": comp_name,
                                "type": comp_type,
                                "version": str(parent_version)
                            })
                        except Exception as e:
                            # If we can't get parent component details, still include the reference
                            referenced_by.append({
                                "component_id": parent_id,
                                "name": "Unknown",
                                "type": "unknown",
                                "version": str(parent_version),
                                "error": str(e)
                            })

        analysis = {
            "_success": True,
            "trading_partner": {
                "component_id": component_id,
                "name": partner_name,
                "standard": getattr(partner, 'standard', None)
            },
            "referenced_by": referenced_by,
            "total_references": len(referenced_by),
            "can_safely_delete": len(referenced_by) == 0,
            "_note": "Shows immediate references (one level). UI's 'Show Where Used' does recursive tracing."
        }

        return analysis

    except Exception as e:
        return {
            "_success": False,
            "error": str(e),
            "message": f"Failed to analyze trading partner usage: {str(e)}"
        }


# ============================================================================
# Consolidated Action Router (for MCP tool consolidation)
# ============================================================================

def manage_trading_partner_action(
    boomi_client,
    profile: str,
    action: str,
    **params
) -> Dict[str, Any]:
    """
    Consolidated trading partner management function.

    Routes to appropriate function based on action parameter.
    This enables consolidation of 6 separate MCP tools into 1.

    Args:
        boomi_client: Authenticated Boomi SDK client
        profile: Profile name for authentication
        action: Action to perform (list, get, create, update, delete, analyze_usage)
        **params: Action-specific parameters

    Actions:
        - list: List trading partners with optional filters
          Params: filters (optional dict)

        - get: Get specific trading partner by ID
          Params: partner_id (required str)

        - create: Create new trading partner
          Params: request_data (required dict with standard, name, etc.)

        - update: Update existing trading partner
          Params: partner_id (required str), updates (required dict)

        - delete: Delete trading partner
          Params: partner_id (required str)

        - analyze_usage: Analyze where trading partner is used
          Params: partner_id (required str)

    Returns:
        Action result dict with success status and data/error
    """
    try:
        if action == "list":
            filters = params.get("filters", None)
            return list_trading_partners(boomi_client, profile, filters)

        elif action == "get":
            partner_id = params.get("partner_id")
            if not partner_id:
                return {
                    "_success": False,
                    "error": "partner_id is required for 'get' action",
                    "hint": "Provide the trading partner component ID to retrieve"
                }
            return get_trading_partner(boomi_client, profile, partner_id)

        elif action == "create":
            request_data = params.get("request_data")
            if not request_data:
                return {
                    "_success": False,
                    "error": "request_data is required for 'create' action",
                    "hint": "Provide trading partner configuration including standard, name, and standard-specific parameters. Use get_schema_template for expected format."
                }
            return create_trading_partner(boomi_client, profile, request_data)

        elif action == "update":
            partner_id = params.get("partner_id")
            updates = params.get("updates")
            if not partner_id:
                return {
                    "_success": False,
                    "error": "partner_id is required for 'update' action",
                    "hint": "Provide the trading partner component ID to update"
                }
            if not updates:
                return {
                    "_success": False,
                    "error": "updates dict is required for 'update' action",
                    "hint": "Provide the fields to update in the trading partner configuration"
                }
            return update_trading_partner(boomi_client, profile, partner_id, updates)

        elif action == "delete":
            partner_id = params.get("partner_id")
            if not partner_id:
                return {
                    "_success": False,
                    "error": "partner_id is required for 'delete' action",
                    "hint": "Provide the trading partner component ID to delete"
                }
            return delete_trading_partner(boomi_client, profile, partner_id)

        elif action == "analyze_usage":
            partner_id = params.get("partner_id")
            if not partner_id:
                return {
                    "_success": False,
                    "error": "partner_id is required for 'analyze_usage' action",
                    "hint": "Provide the trading partner component ID to analyze"
                }
            return analyze_trading_partner_usage(boomi_client, profile, partner_id)

        else:
            return {
                "_success": False,
                "error": f"Unknown action: {action}",
                "hint": "Valid actions are: list, get, create, update, delete, analyze_usage"
            }

    except Exception as e:
        return {
            "_success": False,
            "error": f"Action '{action}' failed: {str(e)}",
            "exception_type": type(e).__name__
        }