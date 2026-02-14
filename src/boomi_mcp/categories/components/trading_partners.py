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

        # Collect warnings for potentially problematic values
        warnings = []
        ftp_get_action = request_data.get('ftp_get_action', '')
        if ftp_get_action and ftp_get_action.lower() == 'actiongetmove':
            warnings.append(
                "FTP get_action 'actiongetmove' may not be supported by the Boomi API "
                "and could be silently reverted to 'actionget'. Consider using 'actiongetdelete' instead."
            )

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
            "message": f"Successfully created trading partner: {request_data.get('component_name')}",
            "warnings": warnings if warnings else None
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

            # EDIFACT partner info
            edifact_info = getattr(info, 'edifact_partner_info', None)
            if edifact_info:
                edifact_ctrl = getattr(edifact_info, 'edifact_control_info', None)
                if edifact_ctrl:
                    unb_ctrl = getattr(edifact_ctrl, 'unb_control_info', None)
                    if unb_ctrl:
                        partner_info["edifact_interchange_id"] = getattr(unb_ctrl, 'interchange_id', None)
                        raw_qual = getattr(unb_ctrl, 'interchange_id_qual', None)
                        partner_info["edifact_interchange_id_qual"] = raw_qual.value if hasattr(raw_qual, 'value') else raw_qual
                        raw_syntax = getattr(unb_ctrl, 'syntax_id', None)
                        partner_info["edifact_syntax_id"] = raw_syntax.value if hasattr(raw_syntax, 'value') else raw_syntax
                        raw_version = getattr(unb_ctrl, 'syntax_version', None)
                        partner_info["edifact_syntax_version"] = raw_version.value if hasattr(raw_version, 'value') else raw_version
                        raw_test = getattr(unb_ctrl, 'test_indicator', None)
                        partner_info["edifact_test_indicator"] = raw_test.value if hasattr(raw_test, 'value') else raw_test

            # HL7 partner info
            hl7_info = getattr(info, 'hl7_partner_info', None)
            if hl7_info:
                hl7_ctrl = getattr(hl7_info, 'hl7_control_info', None)
                if hl7_ctrl:
                    msh_ctrl = getattr(hl7_ctrl, 'msh_control_info', None)
                    if msh_ctrl:
                        app = getattr(msh_ctrl, 'application', None)
                        if app:
                            partner_info["hl7_application"] = getattr(app, 'namespace_id', None)
                        fac = getattr(msh_ctrl, 'facility', None)
                        if fac:
                            partner_info["hl7_facility"] = getattr(fac, 'namespace_id', None)

            # RosettaNet partner info
            rosettanet_info = getattr(info, 'rosetta_net_partner_info', None)
            if rosettanet_info:
                rn_ctrl = getattr(rosettanet_info, 'rosetta_net_control_info', None)
                if rn_ctrl:
                    partner_info["rosettanet_partner_id"] = getattr(rn_ctrl, 'partner_id', None)
                    partner_info["rosettanet_partner_location"] = getattr(rn_ctrl, 'partner_location', None)
                    raw_usage = getattr(rn_ctrl, 'global_usage_code', None)
                    partner_info["rosettanet_global_usage_code"] = raw_usage.value if hasattr(raw_usage, 'value') else raw_usage
                    partner_info["rosettanet_supply_chain_code"] = getattr(rn_ctrl, 'supply_chain_code', None)
                    partner_info["rosettanet_classification_code"] = getattr(rn_ctrl, 'global_partner_classification_code', None)

            # TRADACOMS partner info
            tradacoms_info = getattr(info, 'tradacoms_partner_info', None)
            if tradacoms_info:
                tradacoms_ctrl = getattr(tradacoms_info, 'tradacoms_control_info', None)
                if tradacoms_ctrl:
                    stx_ctrl = getattr(tradacoms_ctrl, 'stx_control_info', None)
                    if stx_ctrl:
                        partner_info["tradacoms_interchange_id"] = getattr(stx_ctrl, 'interchange_id', None)
                        partner_info["tradacoms_interchange_id_qualifier"] = getattr(stx_ctrl, 'interchange_id_qualifier', None)

            # ODETTE partner info
            odette_info = getattr(info, 'odette_partner_info', None)
            if odette_info:
                odette_ctrl = getattr(odette_info, 'odette_control_info', None)
                if odette_ctrl:
                    odette_unb = getattr(odette_ctrl, 'odette_unb_control_info', None)
                    if odette_unb:
                        partner_info["odette_interchange_id"] = getattr(odette_unb, 'interchange_id', None)
                        raw_qual = getattr(odette_unb, 'interchange_id_qual', None)
                        partner_info["odette_interchange_id_qual"] = raw_qual.value if hasattr(raw_qual, 'value') else raw_qual
                        raw_syntax = getattr(odette_unb, 'syntax_id', None)
                        partner_info["odette_syntax_id"] = raw_syntax.value if hasattr(raw_syntax, 'value') else raw_syntax
                        raw_version = getattr(odette_unb, 'syntax_version', None)
                        partner_info["odette_syntax_version"] = raw_version.value if hasattr(raw_version, 'value') else raw_version
                        raw_test = getattr(odette_unb, 'test_indicator', None)
                        partner_info["odette_test_indicator"] = raw_test.value if hasattr(raw_test, 'value') else raw_test

        # Clean up None values from partner_info
        partner_info = {k: v for k, v in partner_info.items() if v is not None}

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
                    disk_info["get_directory"] = getattr(get_opts, 'get_directory', None) or getattr(get_opts, 'getDirectory', None)
                    disk_info["file_filter"] = getattr(get_opts, 'file_filter', None) or getattr(get_opts, 'fileFilter', None)
                    disk_info["filter_match_type"] = getattr(get_opts, 'filter_match_type', None) or getattr(get_opts, 'filterMatchType', None)
                    disk_info["delete_after_read"] = getattr(get_opts, 'delete_after_read', None) or getattr(get_opts, 'deleteAfterRead', None)
                    disk_info["max_file_count"] = getattr(get_opts, 'max_file_count', None) or getattr(get_opts, 'maxFileCount', None)
                if send_opts:
                    disk_info["send_directory"] = getattr(send_opts, 'send_directory', None) or getattr(send_opts, 'sendDirectory', None)
                    disk_info["create_directory"] = getattr(send_opts, 'create_directory', None) or getattr(send_opts, 'createDirectory', None)
                    disk_info["write_option"] = getattr(send_opts, 'write_option', None) or getattr(send_opts, 'writeOption', None)
                # Filter out None values
                disk_info = {k: v for k, v in disk_info.items() if v is not None}
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
                        # Extract client SSL certificate alias
                        client_ssl_cert = getattr(ftpssl_opts, 'client_ssl_certificate', None) or getattr(ftpssl_opts, 'clientSSLCertificate', None)
                        if client_ssl_cert:
                            ftp_info["client_ssl_alias"] = getattr(client_ssl_cert, 'alias', None)
                # Extract FTP get options
                get_opts = getattr(ftp_opts, 'ftp_get_options', None)
                if get_opts:
                    ftp_info["remote_directory"] = getattr(get_opts, 'remote_directory', None)
                    ftp_info["get_transfer_type"] = getattr(get_opts, 'transfer_type', None)
                    ftp_info["get_action"] = getattr(get_opts, 'ftp_action', None) or getattr(get_opts, 'ftpAction', None)
                    ftp_info["max_file_count"] = getattr(get_opts, 'max_file_count', None) or getattr(get_opts, 'maxFileCount', None)
                    ftp_info["file_to_move"] = getattr(get_opts, 'file_to_move', None) or getattr(get_opts, 'fileToMove', None)
                # Extract FTP send options
                send_opts = getattr(ftp_opts, 'ftp_send_options', None)
                if send_opts:
                    ftp_info["send_remote_directory"] = getattr(send_opts, 'remote_directory', None)
                    ftp_info["send_transfer_type"] = getattr(send_opts, 'transfer_type', None)
                    ftp_info["send_action"] = getattr(send_opts, 'ftp_action', None) or getattr(send_opts, 'ftpAction', None)
                    ftp_info["move_to_directory"] = getattr(send_opts, 'move_to_directory', None) or getattr(send_opts, 'moveToDirectory', None)
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
                        sftp_info["known_host_entry"] = getattr(sftpssh_opts, 'known_host_entry', None) or getattr(sftpssh_opts, 'knownHostEntry', None)
                        sftp_info["ssh_key_path"] = getattr(sftpssh_opts, 'sshkeypath', None)
                        sftp_info["dh_key_max_1024"] = getattr(sftpssh_opts, 'dh_key_size_max1024', None) or getattr(sftpssh_opts, 'dhKeySizeMax1024', None)
                    # Extract SFTP proxy settings
                    proxy_settings = getattr(settings, 'sftp_proxy_settings', None)
                    if proxy_settings:
                        sftp_info["proxy_enabled"] = getattr(proxy_settings, 'proxy_enabled', None) or getattr(proxy_settings, 'proxyEnabled', None)
                        sftp_info["proxy_host"] = getattr(proxy_settings, 'host', None)
                        sftp_info["proxy_port"] = getattr(proxy_settings, 'port', None)
                        sftp_info["proxy_type"] = getattr(proxy_settings, 'type_', None) or getattr(proxy_settings, 'type', None)
                        sftp_info["proxy_user"] = getattr(proxy_settings, 'user', None)
                # Extract SFTP get options
                get_opts = getattr(sftp_opts, 'sftp_get_options', None)
                if get_opts:
                    sftp_info["remote_directory"] = getattr(get_opts, 'remote_directory', None) or getattr(get_opts, 'remoteDirectory', None)
                    sftp_info["get_action"] = getattr(get_opts, 'ftp_action', None) or getattr(get_opts, 'ftpAction', None)
                    sftp_info["max_file_count"] = getattr(get_opts, 'max_file_count', None) or getattr(get_opts, 'maxFileCount', None)
                    sftp_info["file_to_move"] = getattr(get_opts, 'file_to_move', None) or getattr(get_opts, 'fileToMove', None)
                    sftp_info["move_to_directory"] = getattr(get_opts, 'move_to_directory', None) or getattr(get_opts, 'moveToDirectory', None)
                    sftp_info["move_force_override"] = getattr(get_opts, 'move_to_force_override', None) or getattr(get_opts, 'moveToForceOverride', None)
                # Extract SFTP send options
                send_opts = getattr(sftp_opts, 'sftp_send_options', None)
                if send_opts:
                    sftp_info["send_remote_directory"] = getattr(send_opts, 'remote_directory', None) or getattr(send_opts, 'remoteDirectory', None)
                    sftp_info["send_action"] = getattr(send_opts, 'ftp_action', None) or getattr(send_opts, 'ftpAction', None)
                    sftp_info["send_move_to_directory"] = getattr(send_opts, 'move_to_directory', None) or getattr(send_opts, 'moveToDirectory', None)
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
                    http_info["authentication_type"] = getattr(settings, 'authentication_type', None) or getattr(settings, 'authenticationType', None)
                    http_info["connect_timeout"] = getattr(settings, 'connect_timeout', None) or getattr(settings, 'connectTimeout', None)
                    http_info["read_timeout"] = getattr(settings, 'read_timeout', None) or getattr(settings, 'readTimeout', None)
                    http_info["cookie_scope"] = getattr(settings, 'cookie_scope', None) or getattr(settings, 'cookieScope', None)
                    # Extract HTTP auth settings
                    http_auth = getattr(settings, 'http_auth_settings', None) or getattr(settings, 'HTTPAuthSettings', None)
                    if http_auth:
                        http_info["username"] = getattr(http_auth, 'user', None)
                    # Extract HTTP OAuth2 settings
                    oauth2_settings = getattr(settings, 'http_oauth2_settings', None) or getattr(settings, 'HTTPOAuth2Settings', None)
                    if oauth2_settings:
                        http_info["oauth_scope"] = getattr(oauth2_settings, 'scope', None)
                        http_info["oauth_grant_type"] = getattr(oauth2_settings, 'grant_type', None) or getattr(oauth2_settings, 'grantType', None)
                        # Extract token endpoint
                        token_endpoint = getattr(oauth2_settings, 'access_token_endpoint', None) or getattr(oauth2_settings, 'accessTokenEndpoint', None)
                        if token_endpoint:
                            http_info["oauth_token_url"] = getattr(token_endpoint, 'url', None)
                        # Extract credentials
                        credentials = getattr(oauth2_settings, 'credentials', None)
                        if credentials:
                            http_info["oauth_client_id"] = getattr(credentials, 'client_id', None) or getattr(credentials, 'clientId', None)
                    # Extract HTTP SSL options
                    httpssl_opts = getattr(settings, 'httpssl_options', None) or getattr(settings, 'HTTPSSLOptions', None)
                    if httpssl_opts:
                        http_info["client_auth"] = getattr(httpssl_opts, 'clientauth', None)
                        http_info["trust_server_cert"] = getattr(httpssl_opts, 'trust_server_cert', None) or getattr(httpssl_opts, 'trustServerCert', None)
                        http_info["client_ssl_alias"] = getattr(httpssl_opts, 'clientsslalias', None)
                        http_info["trusted_cert_alias"] = getattr(httpssl_opts, 'trustedcertalias', None)
                # Extract HTTP send options
                send_opts = getattr(http_opts, 'http_send_options', None) or getattr(http_opts, 'HTTPSendOptions', None)
                if send_opts:
                    http_info["method_type"] = getattr(send_opts, 'method_type', None) or getattr(send_opts, 'methodType', None)
                    http_info["data_content_type"] = getattr(send_opts, 'data_content_type', None) or getattr(send_opts, 'dataContentType', None)
                    http_info["follow_redirects"] = getattr(send_opts, 'follow_redirects', None) or getattr(send_opts, 'followRedirects', None)
                    http_info["return_errors"] = getattr(send_opts, 'return_errors', None) or getattr(send_opts, 'returnErrors', None)
                    http_info["return_responses"] = getattr(send_opts, 'return_responses', None) or getattr(send_opts, 'returnResponses', None)
                    http_info["request_profile"] = getattr(send_opts, 'request_profile', None) or getattr(send_opts, 'requestProfile', None)
                    http_info["request_profile_type"] = getattr(send_opts, 'request_profile_type', None) or getattr(send_opts, 'requestProfileType', None)
                    http_info["response_profile"] = getattr(send_opts, 'response_profile', None) or getattr(send_opts, 'responseProfile', None)
                    http_info["response_profile_type"] = getattr(send_opts, 'response_profile_type', None) or getattr(send_opts, 'responseProfileType', None)
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
                    as2_info["authentication_type"] = getattr(settings, 'authentication_type', None) or getattr(settings, 'authenticationType', None)
                    as2_info["verify_hostname"] = getattr(settings, 'verify_hostname', None) or getattr(settings, 'verifyHostname', None)
                    # Extract basic auth info
                    auth_settings = getattr(settings, 'auth_settings', None) or getattr(settings, 'AuthSettings', None)
                    if auth_settings:
                        as2_info["username"] = getattr(auth_settings, 'username', None) or getattr(auth_settings, 'user', None)
                    # Extract SSL settings
                    ssl_settings = getattr(settings, 'as2ssl_options', None) or getattr(settings, 'AS2SSLOptions', None)
                    if ssl_settings:
                        as2_info["client_ssl_alias"] = getattr(ssl_settings, 'clientsslalias', None) or getattr(ssl_settings, 'clientSSLAlias', None)

                # Extract AS2SendOptions
                send_options = getattr(as2_opts, 'as2_send_options', None) or getattr(as2_opts, 'AS2SendOptions', None)
                if send_options:
                    # Partner info (as2_id)
                    partner_info = getattr(send_options, 'as2_partner_info', None) or getattr(send_options, 'AS2PartnerInfo', None)
                    if partner_info:
                        as2_info["as2_partner_id"] = getattr(partner_info, 'as2_id', None) or getattr(partner_info, 'as2Id', None)
                        as2_info["reject_duplicates"] = getattr(partner_info, 'reject_duplicates', None) or getattr(partner_info, 'rejectDuplicates', None)
                        as2_info["duplicate_check_count"] = getattr(partner_info, 'duplicate_check_count', None) or getattr(partner_info, 'duplicateCheckCount', None)
                        as2_info["legacy_smime"] = getattr(partner_info, 'legacy_smime', None) or getattr(partner_info, 'legacySMIME', None)

                    # Message options
                    msg_opts = getattr(send_options, 'as2_message_options', None) or getattr(send_options, 'AS2MessageOptions', None)
                    if msg_opts:
                        as2_info["signed"] = getattr(msg_opts, 'signed', None)
                        as2_info["encrypted"] = getattr(msg_opts, 'encrypted', None)
                        as2_info["compressed"] = getattr(msg_opts, 'compressed', None)
                        as2_info["encryption_algorithm"] = getattr(msg_opts, 'encryption_algorithm', None) or getattr(msg_opts, 'encryptionAlgorithm', None)
                        as2_info["signing_digest_alg"] = getattr(msg_opts, 'signing_digest_alg', None) or getattr(msg_opts, 'signingDigestAlg', None)
                        as2_info["data_content_type"] = getattr(msg_opts, 'data_content_type', None) or getattr(msg_opts, 'dataContentType', None)
                        as2_info["subject"] = getattr(msg_opts, 'subject', None)
                        as2_info["multiple_attachments"] = getattr(msg_opts, 'multiple_attachments', None) or getattr(msg_opts, 'multipleAttachments', None)
                        as2_info["max_document_count"] = getattr(msg_opts, 'max_document_count', None) or getattr(msg_opts, 'maxDocumentCount', None)
                        as2_info["attachment_option"] = getattr(msg_opts, 'attachment_option', None) or getattr(msg_opts, 'attachmentOption', None)
                        as2_info["attachment_cache"] = getattr(msg_opts, 'attachment_cache', None) or getattr(msg_opts, 'attachmentCache', None)
                        # Certificate aliases
                        encrypt_cert = getattr(msg_opts, 'encrypt_cert', None) or getattr(msg_opts, 'encryptCert', None)
                        if encrypt_cert:
                            as2_info["encrypt_alias"] = getattr(encrypt_cert, 'alias', None)
                        sign_cert = getattr(msg_opts, 'sign_cert', None) or getattr(msg_opts, 'signCert', None)
                        if sign_cert:
                            as2_info["sign_alias"] = getattr(sign_cert, 'alias', None)

                    # MDN options
                    mdn_opts = getattr(send_options, 'as2_mdn_options', None) or getattr(send_options, 'AS2MDNOptions', None)
                    if mdn_opts:
                        as2_info["request_mdn"] = getattr(mdn_opts, 'request_mdn', None) or getattr(mdn_opts, 'requestMDN', None)
                        as2_info["mdn_signed"] = getattr(mdn_opts, 'signed', None)
                        as2_info["mdn_digest_alg"] = getattr(mdn_opts, 'mdn_digest_alg', None) or getattr(mdn_opts, 'mdnDigestAlg', None)
                        as2_info["synchronous_mdn"] = getattr(mdn_opts, 'synchronous', None)
                        as2_info["fail_on_negative_mdn"] = getattr(mdn_opts, 'fail_on_negative_mdn', None) or getattr(mdn_opts, 'failOnNegativeMDN', None)
                        as2_info["mdn_external_url"] = getattr(mdn_opts, 'external_url', None) or getattr(mdn_opts, 'externalURL', None)
                        as2_info["mdn_use_external_url"] = getattr(mdn_opts, 'use_external_url', None) or getattr(mdn_opts, 'useExternalURL', None)
                        as2_info["mdn_use_ssl"] = getattr(mdn_opts, 'use_ssl', None) or getattr(mdn_opts, 'useSSL', None)
                        # MDN certificate aliases
                        mdn_cert = getattr(mdn_opts, 'mdn_cert', None) or getattr(mdn_opts, 'mdnCert', None)
                        if mdn_cert:
                            as2_info["mdn_alias"] = getattr(mdn_cert, 'alias', None)

                # Filter out None values
                as2_info = {k: v for k, v in as2_info.items() if v is not None}
                communication_protocols.append(as2_info)

            # MLLP protocol
            if getattr(comm, 'mllp_communication_options', None):
                mllp_opts = comm.mllp_communication_options
                mllp_info = {"protocol": "mllp"}
                settings = getattr(mllp_opts, 'mllp_send_settings', None) or getattr(mllp_opts, 'MLLPSendSettings', None)
                if settings:
                    mllp_info["host"] = getattr(settings, 'host', None)
                    mllp_info["port"] = getattr(settings, 'port', None)
                    mllp_info["persistent"] = getattr(settings, 'persistent', None)
                    mllp_info["receive_timeout"] = getattr(settings, 'receive_timeout', None) or getattr(settings, 'receiveTimeout', None)
                    mllp_info["send_timeout"] = getattr(settings, 'send_timeout', None) or getattr(settings, 'sendTimeout', None)
                    mllp_info["max_connections"] = getattr(settings, 'max_connections', None) or getattr(settings, 'maxConnections', None)
                    mllp_info["inactivity_timeout"] = getattr(settings, 'inactivity_timeout', None) or getattr(settings, 'inactivityTimeout', None)
                    mllp_info["max_retry"] = getattr(settings, 'max_retry', None) or getattr(settings, 'maxRetry', None)
                    mllp_info["halt_timeout"] = getattr(settings, 'halt_timeout', None) or getattr(settings, 'haltTimeout', None)
                    # Extract MLLP SSL options
                    mllpssl_opts = getattr(settings, 'mllpssl_options', None) or getattr(settings, 'MLLPSSLOptions', None)
                    if mllpssl_opts:
                        mllp_info["use_ssl"] = getattr(mllpssl_opts, 'use_ssl', None) or getattr(mllpssl_opts, 'useSSL', None)
                        mllp_info["use_client_ssl"] = getattr(mllpssl_opts, 'use_client_ssl', None) or getattr(mllpssl_opts, 'useClientSSL', None)
                        mllp_info["client_ssl_alias"] = getattr(mllpssl_opts, 'client_ssl_alias', None) or getattr(mllpssl_opts, 'clientSSLAlias', None)
                        mllp_info["ssl_alias"] = getattr(mllpssl_opts, 'ssl_alias', None) or getattr(mllpssl_opts, 'sslAlias', None)
                # Filter out None values
                mllp_info = {k: v for k, v in mllp_info.items() if v is not None}
                communication_protocols.append(mllp_info)

            # OFTP protocol
            if getattr(comm, 'oftp_communication_options', None):
                oftp_opts = comm.oftp_communication_options
                oftp_info = {"protocol": "oftp"}
                conn_settings = getattr(oftp_opts, 'oftp_connection_settings', None) or getattr(oftp_opts, 'OFTPConnectionSettings', None)
                if conn_settings:
                    # Check both direct attrs and default_oftp_connection_settings
                    default_settings = getattr(conn_settings, 'default_oftp_connection_settings', None) or getattr(conn_settings, 'defaultOFTPConnectionSettings', None)
                    # Try direct attributes first, fall back to default settings
                    oftp_info["host"] = getattr(conn_settings, 'host', None) or (getattr(default_settings, 'host', None) if default_settings else None)
                    oftp_info["port"] = getattr(conn_settings, 'port', None) or (getattr(default_settings, 'port', None) if default_settings else None)
                    oftp_info["tls"] = getattr(conn_settings, 'tls', None) if hasattr(conn_settings, 'tls') else (getattr(default_settings, 'tls', None) if default_settings else None)
                    oftp_info["ssid_auth"] = getattr(conn_settings, 'ssidauth', None) if hasattr(conn_settings, 'ssidauth') else (getattr(default_settings, 'ssidauth', None) if default_settings else None)
                    oftp_info["sfid_cipher"] = getattr(conn_settings, 'sfidciph', None) if hasattr(conn_settings, 'sfidciph') else (getattr(default_settings, 'sfidciph', None) if default_settings else None)
                    oftp_info["use_gateway"] = getattr(conn_settings, 'use_gateway', None) or getattr(conn_settings, 'useGateway', None) if hasattr(conn_settings, 'use_gateway') or hasattr(conn_settings, 'useGateway') else (getattr(default_settings, 'use_gateway', None) or getattr(default_settings, 'useGateway', None) if default_settings else None)
                    oftp_info["use_client_ssl"] = getattr(conn_settings, 'use_client_ssl', None) or getattr(conn_settings, 'useClientSSL', None) if hasattr(conn_settings, 'use_client_ssl') or hasattr(conn_settings, 'useClientSSL') else (getattr(default_settings, 'use_client_ssl', None) or getattr(default_settings, 'useClientSSL', None) if default_settings else None)
                    oftp_info["client_ssl_alias"] = getattr(conn_settings, 'client_ssl_alias', None) or getattr(conn_settings, 'clientSSLAlias', None) or (getattr(default_settings, 'client_ssl_alias', None) or getattr(default_settings, 'clientSSLAlias', None) if default_settings else None)
                    # Extract partner info from both locations
                    partner_info = getattr(conn_settings, 'my_partner_info', None) or getattr(conn_settings, 'myPartnerInfo', None) or (getattr(default_settings, 'my_partner_info', None) or getattr(default_settings, 'myPartnerInfo', None) if default_settings else None)
                    if partner_info:
                        oftp_info["ssid_code"] = getattr(partner_info, 'ssidcode', None)
                        oftp_info["compress"] = getattr(partner_info, 'ssidcmpr', None)
                        oftp_info["sfid_sign"] = getattr(partner_info, 'sfidsign', None)
                        oftp_info["sfid_encrypt"] = getattr(partner_info, 'sfidsec_encrypt', None) or getattr(partner_info, 'sfidsec-encrypt', None)
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

        # Collect warnings for potentially problematic values
        warnings = []
        ftp_get_action = updates.get('ftp_get_action', '')
        if ftp_get_action and ftp_get_action.lower() == 'actiongetmove':
            warnings.append(
                "FTP get_action 'actiongetmove' may not be supported by the Boomi API "
                "and could be silently reverted to 'actionget'. Consider using 'actiongetdelete' instead."
            )

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
                    # For updates, merge with existing AS2 values for partial updates
                    existing_comm = getattr(existing_tp, 'partner_communication', None)
                    if existing_comm:
                        existing_as2 = getattr(existing_comm, 'as2_communication_options', None)
                        if existing_as2:
                            # Preserve AS2 Send Settings (connection)
                            existing_send_settings = getattr(existing_as2, 'as2_send_settings', None)
                            if existing_send_settings:
                                if 'as2_url' not in as2_params:
                                    existing_url = getattr(existing_send_settings, 'url', None)
                                    if existing_url:
                                        as2_params['as2_url'] = existing_url
                                if 'as2_authentication_type' not in as2_params:
                                    existing_auth = getattr(existing_send_settings, 'authentication_type', None)
                                    if existing_auth:
                                        as2_params['as2_authentication_type'] = existing_auth
                                if 'as2_username' not in as2_params:
                                    existing_user = getattr(existing_send_settings, 'user', None)
                                    if existing_user:
                                        as2_params['as2_username'] = existing_user
                                if 'as2_password' not in as2_params:
                                    existing_pass = getattr(existing_send_settings, 'password', None)
                                    if existing_pass:
                                        as2_params['as2_password'] = existing_pass
                                if 'as2_verify_hostname' not in as2_params:
                                    existing_verify = getattr(existing_send_settings, 'verify_hostname', None) or getattr(existing_send_settings, 'verifyHostname', None)
                                    if existing_verify is not None:
                                        as2_params['as2_verify_hostname'] = str(existing_verify).lower()
                                if 'as2_client_ssl_alias' not in as2_params:
                                    client_ssl = getattr(existing_send_settings, 'client_ssl_certificate', None)
                                    if client_ssl:
                                        existing_alias = getattr(client_ssl, 'alias', None)
                                        if existing_alias:
                                            as2_params['as2_client_ssl_alias'] = existing_alias

                            # Preserve AS2 Send Options (message settings)
                            existing_send_opts = getattr(existing_as2, 'as2_send_options', None)
                            if existing_send_opts:
                                # Partner info
                                existing_partner_info = getattr(existing_send_opts, 'as2_partner_info', None)
                                if existing_partner_info:
                                    if 'as2_partner_identifier' not in as2_params:
                                        existing_partner_id = getattr(existing_partner_info, 'as2_id', None) or getattr(existing_partner_info, 'as2Id', None)
                                        if existing_partner_id:
                                            as2_params['as2_partner_identifier'] = existing_partner_id
                                # Signing and encryption certificates
                                if 'as2_encrypt_alias' not in as2_params:
                                    encrypt_cert = getattr(existing_send_opts, 'encrypt_certificate', None) or getattr(existing_send_opts, 'encryptCertificate', None)
                                    if encrypt_cert:
                                        existing_alias = getattr(encrypt_cert, 'alias', None)
                                        if existing_alias:
                                            as2_params['as2_encrypt_alias'] = existing_alias
                                if 'as2_sign_alias' not in as2_params:
                                    sign_cert = getattr(existing_send_opts, 'sign_certificate', None) or getattr(existing_send_opts, 'signCertificate', None)
                                    if sign_cert:
                                        existing_alias = getattr(sign_cert, 'alias', None)
                                        if existing_alias:
                                            as2_params['as2_sign_alias'] = existing_alias
                                # Message options
                                if 'as2_signed' not in as2_params:
                                    existing_signed = getattr(existing_send_opts, 'signed', None)
                                    if existing_signed is not None:
                                        as2_params['as2_signed'] = str(existing_signed).lower()
                                if 'as2_encrypted' not in as2_params:
                                    existing_encrypted = getattr(existing_send_opts, 'encrypted', None)
                                    if existing_encrypted is not None:
                                        as2_params['as2_encrypted'] = str(existing_encrypted).lower()
                                if 'as2_compressed' not in as2_params:
                                    existing_compressed = getattr(existing_send_opts, 'compressed', None)
                                    if existing_compressed is not None:
                                        as2_params['as2_compressed'] = str(existing_compressed).lower()
                                if 'as2_encryption_algorithm' not in as2_params:
                                    existing_algo = getattr(existing_send_opts, 'encryption_algorithm', None) or getattr(existing_send_opts, 'encryptionAlgorithm', None)
                                    if existing_algo:
                                        as2_params['as2_encryption_algorithm'] = existing_algo
                                if 'as2_signing_digest_alg' not in as2_params:
                                    existing_digest = getattr(existing_send_opts, 'signing_digest_algorithm', None) or getattr(existing_send_opts, 'signingDigestAlgorithm', None)
                                    if existing_digest:
                                        as2_params['as2_signing_digest_alg'] = existing_digest
                                if 'as2_data_content_type' not in as2_params:
                                    existing_content = getattr(existing_send_opts, 'data_content_type', None) or getattr(existing_send_opts, 'dataContentType', None)
                                    if existing_content:
                                        as2_params['as2_data_content_type'] = existing_content
                                if 'as2_subject' not in as2_params:
                                    existing_subject = getattr(existing_send_opts, 'subject', None)
                                    if existing_subject:
                                        as2_params['as2_subject'] = existing_subject
                                # MDN options
                                if 'as2_request_mdn' not in as2_params:
                                    existing_req_mdn = getattr(existing_send_opts, 'request_mdn', None) or getattr(existing_send_opts, 'requestMdn', None)
                                    if existing_req_mdn is not None:
                                        as2_params['as2_request_mdn'] = str(existing_req_mdn).lower()
                                if 'as2_mdn_signed' not in as2_params:
                                    existing_mdn_signed = getattr(existing_send_opts, 'mdn_signed', None) or getattr(existing_send_opts, 'mdnSigned', None)
                                    if existing_mdn_signed is not None:
                                        as2_params['as2_mdn_signed'] = str(existing_mdn_signed).lower()
                                if 'as2_mdn_digest_alg' not in as2_params:
                                    existing_mdn_digest = getattr(existing_send_opts, 'mdn_digest_algorithm', None) or getattr(existing_send_opts, 'mdnDigestAlgorithm', None)
                                    if existing_mdn_digest:
                                        as2_params['as2_mdn_digest_alg'] = existing_mdn_digest
                                if 'as2_synchronous_mdn' not in as2_params:
                                    existing_sync_mdn = getattr(existing_send_opts, 'synchronous_mdn', None) or getattr(existing_send_opts, 'synchronousMdn', None)
                                    if existing_sync_mdn is not None:
                                        as2_params['as2_synchronous_mdn'] = str(existing_sync_mdn).lower()
                                if 'as2_fail_on_negative_mdn' not in as2_params:
                                    existing_fail_mdn = getattr(existing_send_opts, 'fail_on_negative_mdn', None) or getattr(existing_send_opts, 'failOnNegativeMdn', None)
                                    if existing_fail_mdn is not None:
                                        as2_params['as2_fail_on_negative_mdn'] = str(existing_fail_mdn).lower()
                                # Attachments
                                if 'as2_multiple_attachments' not in as2_params:
                                    existing_multi = getattr(existing_send_opts, 'multiple_attachments', None) or getattr(existing_send_opts, 'multipleAttachments', None)
                                    if existing_multi is not None:
                                        as2_params['as2_multiple_attachments'] = str(existing_multi).lower()
                                if 'as2_max_document_count' not in as2_params:
                                    existing_max = getattr(existing_send_opts, 'max_document_count', None) or getattr(existing_send_opts, 'maxDocumentCount', None)
                                    if existing_max:
                                        as2_params['as2_max_document_count'] = existing_max
                                if 'as2_legacy_smime' not in as2_params:
                                    existing_legacy = getattr(existing_send_opts, 'legacy_smime', None) or getattr(existing_send_opts, 'legacySMIME', None)
                                    if existing_legacy is not None:
                                        as2_params['as2_legacy_smime'] = str(existing_legacy).lower()

                            # Preserve AS2 Receive Options (MDN delivery)
                            existing_recv_opts = getattr(existing_as2, 'as2_receive_options', None)
                            if existing_recv_opts:
                                if 'as2_mdn_alias' not in as2_params:
                                    mdn_cert = getattr(existing_recv_opts, 'mdn_certificate', None) or getattr(existing_recv_opts, 'mdnCertificate', None)
                                    if mdn_cert:
                                        existing_alias = getattr(mdn_cert, 'alias', None)
                                        if existing_alias:
                                            as2_params['as2_mdn_alias'] = existing_alias
                                if 'as2_reject_duplicates' not in as2_params:
                                    existing_reject = getattr(existing_recv_opts, 'reject_duplicates', None) or getattr(existing_recv_opts, 'rejectDuplicates', None)
                                    if existing_reject is not None:
                                        as2_params['as2_reject_duplicates'] = str(existing_reject).lower()
                                if 'as2_duplicate_check_count' not in as2_params:
                                    existing_check = getattr(existing_recv_opts, 'duplicate_check_count', None) or getattr(existing_recv_opts, 'duplicateCheckCount', None)
                                    if existing_check:
                                        as2_params['as2_duplicate_check_count'] = existing_check

                    as2_opts = build_as2_communication_options(**as2_params)
                    if as2_opts:
                        comm_dict["AS2CommunicationOptions"] = as2_opts

                if http_params:
                    # Merge with existing HTTP values for partial updates
                    existing_comm = getattr(existing_tp, 'partner_communication', None)
                    if existing_comm:
                        existing_http = getattr(existing_comm, 'http_communication_options', None)
                        if existing_http:
                            existing_settings = getattr(existing_http, 'http_settings', None)
                            if existing_settings:
                                # Basic connection settings
                                if 'http_url' not in http_params:
                                    existing_url = getattr(existing_settings, 'url', None)
                                    if existing_url:
                                        http_params['http_url'] = existing_url
                                if 'http_authentication_type' not in http_params:
                                    existing_auth = getattr(existing_settings, 'authentication_type', None)
                                    if existing_auth:
                                        http_params['http_authentication_type'] = existing_auth
                                if 'http_username' not in http_params:
                                    existing_user = getattr(existing_settings, 'user', None)
                                    if existing_user:
                                        http_params['http_username'] = existing_user
                                if 'http_password' not in http_params:
                                    existing_pass = getattr(existing_settings, 'password', None)
                                    if existing_pass:
                                        http_params['http_password'] = existing_pass
                                # Timeout settings
                                if 'http_connect_timeout' not in http_params:
                                    existing_timeout = getattr(existing_settings, 'connect_timeout', None) or getattr(existing_settings, 'connectTimeout', None)
                                    if existing_timeout:
                                        http_params['http_connect_timeout'] = str(existing_timeout)
                                if 'http_read_timeout' not in http_params:
                                    existing_timeout = getattr(existing_settings, 'read_timeout', None) or getattr(existing_settings, 'readTimeout', None)
                                    if existing_timeout:
                                        http_params['http_read_timeout'] = str(existing_timeout)
                                # Method and content settings
                                if 'http_method_type' not in http_params:
                                    existing_method = getattr(existing_settings, 'method_type', None) or getattr(existing_settings, 'methodType', None)
                                    if existing_method:
                                        http_params['http_method_type'] = existing_method
                                if 'http_data_content_type' not in http_params:
                                    existing_content = getattr(existing_settings, 'data_content_type', None) or getattr(existing_settings, 'dataContentType', None)
                                    if existing_content:
                                        http_params['http_data_content_type'] = existing_content
                                # SSL settings
                                if 'http_client_auth' not in http_params:
                                    existing_client_auth = getattr(existing_settings, 'use_client_authentication', None)
                                    if existing_client_auth is not None:
                                        http_params['http_client_auth'] = str(existing_client_auth).lower()
                                if 'http_trust_server_cert' not in http_params:
                                    existing_trust = getattr(existing_settings, 'trust_ssl_server_certificate', None)
                                    if existing_trust is not None:
                                        http_params['http_trust_server_cert'] = str(existing_trust).lower()
                                if 'http_client_ssl_alias' not in http_params:
                                    client_ssl = getattr(existing_settings, 'client_ssl_certificate', None)
                                    if client_ssl:
                                        existing_alias = getattr(client_ssl, 'alias', None)
                                        if existing_alias:
                                            http_params['http_client_ssl_alias'] = existing_alias
                                if 'http_trusted_cert_alias' not in http_params:
                                    trusted_ssl = getattr(existing_settings, 'trusted_ssl_certificate', None)
                                    if trusted_ssl:
                                        existing_alias = getattr(trusted_ssl, 'alias', None)
                                        if existing_alias:
                                            http_params['http_trusted_cert_alias'] = existing_alias
                                # Behavior settings
                                if 'http_follow_redirects' not in http_params:
                                    existing_follow = getattr(existing_settings, 'follow_redirects', None) or getattr(existing_settings, 'followRedirects', None)
                                    if existing_follow is not None:
                                        http_params['http_follow_redirects'] = str(existing_follow).lower()
                                if 'http_return_errors' not in http_params:
                                    existing_errors = getattr(existing_settings, 'return_error_response_payload', None)
                                    if existing_errors is not None:
                                        http_params['http_return_errors'] = str(existing_errors).lower()
                                if 'http_return_responses' not in http_params:
                                    existing_responses = getattr(existing_settings, 'return_response_payload', None)
                                    if existing_responses is not None:
                                        http_params['http_return_responses'] = str(existing_responses).lower()
                                if 'http_cookie_scope' not in http_params:
                                    existing_cookie = getattr(existing_settings, 'cookie_scope', None) or getattr(existing_settings, 'cookieScope', None)
                                    if existing_cookie:
                                        http_params['http_cookie_scope'] = existing_cookie
                                # Request/Response profiles
                                if 'http_request_profile_type' not in http_params:
                                    existing_req_type = getattr(existing_settings, 'request_profile_type', None) or getattr(existing_settings, 'requestProfileType', None)
                                    if existing_req_type:
                                        http_params['http_request_profile_type'] = existing_req_type
                                if 'http_request_profile' not in http_params:
                                    req_profile = getattr(existing_settings, 'request_profile', None) or getattr(existing_settings, 'requestProfile', None)
                                    if req_profile:
                                        existing_id = getattr(req_profile, 'component_id', None) or getattr(req_profile, 'componentId', None)
                                        if existing_id:
                                            http_params['http_request_profile'] = existing_id
                                if 'http_response_profile_type' not in http_params:
                                    existing_resp_type = getattr(existing_settings, 'response_profile_type', None) or getattr(existing_settings, 'responseProfileType', None)
                                    if existing_resp_type:
                                        http_params['http_response_profile_type'] = existing_resp_type
                                if 'http_response_profile' not in http_params:
                                    resp_profile = getattr(existing_settings, 'response_profile', None) or getattr(existing_settings, 'responseProfile', None)
                                    if resp_profile:
                                        existing_id = getattr(resp_profile, 'component_id', None) or getattr(resp_profile, 'componentId', None)
                                        if existing_id:
                                            http_params['http_response_profile'] = existing_id
                                # OAuth2 settings
                                oauth = getattr(existing_settings, 'oauth2_settings', None) or getattr(existing_settings, 'oAuth2Settings', None)
                                if oauth:
                                    if 'http_oauth_token_url' not in http_params:
                                        existing_token_url = getattr(oauth, 'access_token_url', None) or getattr(oauth, 'accessTokenUrl', None)
                                        if existing_token_url:
                                            http_params['http_oauth_token_url'] = existing_token_url
                                    if 'http_oauth_client_id' not in http_params:
                                        existing_client = getattr(oauth, 'client_id', None) or getattr(oauth, 'clientId', None)
                                        if existing_client:
                                            http_params['http_oauth_client_id'] = existing_client
                                    if 'http_oauth_client_secret' not in http_params:
                                        existing_secret = getattr(oauth, 'client_secret', None) or getattr(oauth, 'clientSecret', None)
                                        if existing_secret:
                                            http_params['http_oauth_client_secret'] = existing_secret
                                    if 'http_oauth_scope' not in http_params:
                                        existing_scope = getattr(oauth, 'scope', None)
                                        if existing_scope:
                                            http_params['http_oauth_scope'] = existing_scope
                    http_opts = build_http_communication_options(**http_params)
                    if http_opts:
                        comm_dict["HTTPCommunicationOptions"] = http_opts

                if sftp_params:
                    # Merge with existing SFTP values for partial updates
                    existing_comm = getattr(existing_tp, 'partner_communication', None)
                    if existing_comm:
                        existing_sftp = getattr(existing_comm, 'sftp_communication_options', None)
                        if existing_sftp:
                            # Preserve SFTP Settings (connection parameters)
                            existing_settings = getattr(existing_sftp, 'sftp_settings', None)
                            if existing_settings:
                                if 'sftp_host' not in sftp_params:
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
                                if 'sftp_password' not in sftp_params:
                                    existing_pass = getattr(existing_settings, 'password', None)
                                    if existing_pass:
                                        sftp_params['sftp_password'] = existing_pass
                                if 'sftp_known_host_entry' not in sftp_params:
                                    existing_known_host = getattr(existing_settings, 'known_host_entry', None)
                                    if existing_known_host:
                                        sftp_params['sftp_known_host_entry'] = existing_known_host
                                if 'sftp_dh_key_max_1024' not in sftp_params:
                                    existing_dh = getattr(existing_settings, 'dh_key_max1024', None) or getattr(existing_settings, 'dhKeyMax1024', None)
                                    if existing_dh is not None:
                                        sftp_params['sftp_dh_key_max_1024'] = str(existing_dh).lower()
                                # Preserve SSH key settings
                                if 'sftp_ssh_key_auth' not in sftp_params:
                                    existing_ssh_auth = getattr(existing_settings, 'use_ssh_key_authentication', None)
                                    if existing_ssh_auth is not None:
                                        sftp_params['sftp_ssh_key_auth'] = str(existing_ssh_auth).lower()
                                if 'sftp_ssh_key_path' not in sftp_params:
                                    existing_ssh_path = getattr(existing_settings, 'ssh_key_file_path', None)
                                    if existing_ssh_path:
                                        sftp_params['sftp_ssh_key_path'] = existing_ssh_path
                                if 'sftp_ssh_key_password' not in sftp_params:
                                    existing_ssh_pass = getattr(existing_settings, 'ssh_key_password', None)
                                    if existing_ssh_pass:
                                        sftp_params['sftp_ssh_key_password'] = existing_ssh_pass
                                # Preserve proxy settings
                                if 'sftp_proxy_enabled' not in sftp_params:
                                    existing_proxy = getattr(existing_settings, 'proxy_enabled', None)
                                    if existing_proxy is not None:
                                        sftp_params['sftp_proxy_enabled'] = str(existing_proxy).lower()
                                if 'sftp_proxy_type' not in sftp_params:
                                    existing_proxy_type = getattr(existing_settings, 'proxy_type', None)
                                    if existing_proxy_type:
                                        sftp_params['sftp_proxy_type'] = existing_proxy_type
                                if 'sftp_proxy_host' not in sftp_params:
                                    existing_proxy_host = getattr(existing_settings, 'proxy_host', None)
                                    if existing_proxy_host:
                                        sftp_params['sftp_proxy_host'] = existing_proxy_host
                                if 'sftp_proxy_port' not in sftp_params:
                                    existing_proxy_port = getattr(existing_settings, 'proxy_port', None)
                                    if existing_proxy_port:
                                        sftp_params['sftp_proxy_port'] = str(existing_proxy_port)
                                if 'sftp_proxy_user' not in sftp_params:
                                    existing_proxy_user = getattr(existing_settings, 'proxy_user', None)
                                    if existing_proxy_user:
                                        sftp_params['sftp_proxy_user'] = existing_proxy_user
                                if 'sftp_proxy_password' not in sftp_params:
                                    existing_proxy_pass = getattr(existing_settings, 'proxy_password', None)
                                    if existing_proxy_pass:
                                        sftp_params['sftp_proxy_password'] = existing_proxy_pass

                            # Preserve SFTP Get Options (download settings)
                            existing_get_opts = getattr(existing_sftp, 'sftp_get_options', None)
                            if existing_get_opts:
                                if 'sftp_remote_directory' not in sftp_params:
                                    existing_dir = getattr(existing_get_opts, 'remote_directory', None)
                                    if existing_dir:
                                        sftp_params['sftp_remote_directory'] = existing_dir
                                if 'sftp_get_action' not in sftp_params:
                                    existing_action = getattr(existing_get_opts, 'ftp_action', None) or getattr(existing_get_opts, 'ftpAction', None)
                                    if existing_action:
                                        sftp_params['sftp_get_action'] = existing_action
                                if 'sftp_max_file_count' not in sftp_params:
                                    existing_count = getattr(existing_get_opts, 'max_file_count', None) or getattr(existing_get_opts, 'maxFileCount', None)
                                    if existing_count:
                                        sftp_params['sftp_max_file_count'] = str(existing_count)
                                if 'sftp_file_to_move' not in sftp_params:
                                    existing_file = getattr(existing_get_opts, 'file_to_move', None) or getattr(existing_get_opts, 'fileToMove', None)
                                    if existing_file:
                                        sftp_params['sftp_file_to_move'] = existing_file
                                if 'sftp_move_to_directory' not in sftp_params:
                                    existing_move_dir = getattr(existing_get_opts, 'move_to_directory', None) or getattr(existing_get_opts, 'moveToDirectory', None)
                                    if existing_move_dir:
                                        sftp_params['sftp_move_to_directory'] = existing_move_dir
                                if 'sftp_move_force_override' not in sftp_params:
                                    existing_force = getattr(existing_get_opts, 'move_force_override', None) or getattr(existing_get_opts, 'moveForceOverride', None)
                                    if existing_force is not None:
                                        sftp_params['sftp_move_force_override'] = str(existing_force).lower()

                            # Preserve SFTP Send Options (upload settings)
                            existing_send_opts = getattr(existing_sftp, 'sftp_send_options', None)
                            if existing_send_opts:
                                if 'sftp_send_action' not in sftp_params:
                                    existing_action = getattr(existing_send_opts, 'ftp_action', None) or getattr(existing_send_opts, 'ftpAction', None)
                                    if existing_action:
                                        sftp_params['sftp_send_action'] = existing_action
                    sftp_opts = build_sftp_communication_options(**sftp_params)
                    if sftp_opts:
                        comm_dict["SFTPCommunicationOptions"] = sftp_opts

                if ftp_params:
                    # Merge with existing FTP values for partial updates
                    existing_comm = getattr(existing_tp, 'partner_communication', None)
                    if existing_comm:
                        existing_ftp = getattr(existing_comm, 'ftp_communication_options', None)
                        if existing_ftp:
                            # Preserve FTP Settings (connection parameters)
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
                                # Preserve SSL options
                                existing_ssl = getattr(existing_settings, 'ftpssl_options', None)
                                if existing_ssl:
                                    if 'ftp_ssl_mode' not in ftp_params:
                                        existing_ssl_mode = getattr(existing_ssl, 'sslmode', None)
                                        if existing_ssl_mode:
                                            ftp_params['ftp_ssl_mode'] = existing_ssl_mode
                                    if 'ftp_client_ssl_alias' not in ftp_params:
                                        client_ssl_cert = getattr(existing_ssl, 'client_ssl_certificate', None) or getattr(existing_ssl, 'clientSSLCertificate', None)
                                        if client_ssl_cert:
                                            existing_alias = getattr(client_ssl_cert, 'alias', None)
                                            if existing_alias:
                                                ftp_params['ftp_client_ssl_alias'] = existing_alias

                            # Preserve FTP Get Options (download settings)
                            existing_get_opts = getattr(existing_ftp, 'ftp_get_options', None)
                            if existing_get_opts:
                                if 'ftp_remote_directory' not in ftp_params:
                                    existing_dir = getattr(existing_get_opts, 'remote_directory', None)
                                    if existing_dir:
                                        ftp_params['ftp_remote_directory'] = existing_dir
                                if 'ftp_transfer_type' not in ftp_params:
                                    existing_type = getattr(existing_get_opts, 'transfer_type', None)
                                    if existing_type:
                                        ftp_params['ftp_transfer_type'] = existing_type
                                if 'ftp_get_action' not in ftp_params:
                                    existing_action = getattr(existing_get_opts, 'ftp_action', None) or getattr(existing_get_opts, 'ftpAction', None)
                                    if existing_action:
                                        ftp_params['ftp_get_action'] = existing_action
                                if 'ftp_max_file_count' not in ftp_params:
                                    existing_count = getattr(existing_get_opts, 'max_file_count', None) or getattr(existing_get_opts, 'maxFileCount', None)
                                    if existing_count:
                                        ftp_params['ftp_max_file_count'] = str(existing_count)
                                if 'ftp_file_to_move' not in ftp_params:
                                    existing_file = getattr(existing_get_opts, 'file_to_move', None) or getattr(existing_get_opts, 'fileToMove', None)
                                    if existing_file:
                                        ftp_params['ftp_file_to_move'] = existing_file
                                if 'ftp_move_to_directory' not in ftp_params:
                                    existing_move_dir = getattr(existing_get_opts, 'move_to_directory', None) or getattr(existing_get_opts, 'moveToDirectory', None)
                                    if existing_move_dir:
                                        ftp_params['ftp_move_to_directory'] = existing_move_dir

                            # Preserve FTP Send Options (upload settings)
                            existing_send_opts = getattr(existing_ftp, 'ftp_send_options', None)
                            if existing_send_opts:
                                if 'ftp_send_action' not in ftp_params:
                                    existing_action = getattr(existing_send_opts, 'ftp_action', None) or getattr(existing_send_opts, 'ftpAction', None)
                                    if existing_action:
                                        ftp_params['ftp_send_action'] = existing_action
                                if 'ftp_move_to_directory' not in ftp_params:
                                    existing_move_dir = getattr(existing_send_opts, 'move_to_directory', None) or getattr(existing_send_opts, 'moveToDirectory', None)
                                    if existing_move_dir:
                                        ftp_params['ftp_move_to_directory'] = existing_move_dir
                                if 'ftp_remote_directory' not in ftp_params:
                                    existing_dir = getattr(existing_send_opts, 'remote_directory', None) or getattr(existing_send_opts, 'remoteDirectory', None)
                                    if existing_dir:
                                        ftp_params['ftp_remote_directory'] = existing_dir
                                if 'ftp_transfer_type' not in ftp_params:
                                    existing_type = getattr(existing_send_opts, 'transfer_type', None) or getattr(existing_send_opts, 'transferType', None)
                                    if existing_type:
                                        ftp_params['ftp_transfer_type'] = existing_type
                    ftp_opts = build_ftp_communication_options(**ftp_params)
                    if ftp_opts:
                        comm_dict["FTPCommunicationOptions"] = ftp_opts

                if disk_params:
                    # Merge with existing Disk values for partial updates
                    existing_comm = getattr(existing_tp, 'partner_communication', None)
                    if existing_comm:
                        existing_disk = getattr(existing_comm, 'disk_communication_options', None)
                        if existing_disk:
                            # Preserve Disk Get Options (read settings)
                            existing_get = getattr(existing_disk, 'disk_get_options', None)
                            if existing_get:
                                if 'disk_get_directory' not in disk_params:
                                    existing_dir = getattr(existing_get, 'get_directory', None)
                                    if existing_dir:
                                        disk_params['disk_get_directory'] = existing_dir
                                if 'disk_file_filter' not in disk_params:
                                    existing_filter = getattr(existing_get, 'file_filter', None) or getattr(existing_get, 'fileFilter', None)
                                    if existing_filter:
                                        disk_params['disk_file_filter'] = existing_filter
                                if 'disk_filter_match_type' not in disk_params:
                                    existing_match = getattr(existing_get, 'filter_match_type', None) or getattr(existing_get, 'filterMatchType', None)
                                    if existing_match:
                                        disk_params['disk_filter_match_type'] = existing_match
                                if 'disk_delete_after_read' not in disk_params:
                                    existing_delete = getattr(existing_get, 'delete_after_read', None) or getattr(existing_get, 'deleteAfterRead', None)
                                    if existing_delete is not None:
                                        disk_params['disk_delete_after_read'] = str(existing_delete).lower()
                                if 'disk_max_file_count' not in disk_params:
                                    existing_count = getattr(existing_get, 'max_file_count', None) or getattr(existing_get, 'maxFileCount', None)
                                    if existing_count:
                                        disk_params['disk_max_file_count'] = str(existing_count)

                            # Preserve Disk Send Options (write settings)
                            existing_send = getattr(existing_disk, 'disk_send_options', None)
                            if existing_send:
                                if 'disk_send_directory' not in disk_params:
                                    existing_dir = getattr(existing_send, 'send_directory', None)
                                    if existing_dir:
                                        disk_params['disk_send_directory'] = existing_dir
                                if 'disk_create_directory' not in disk_params:
                                    existing_create = getattr(existing_send, 'create_directory', None) or getattr(existing_send, 'createDirectory', None)
                                    if existing_create is not None:
                                        disk_params['disk_create_directory'] = str(existing_create).lower()
                                if 'disk_write_option' not in disk_params:
                                    existing_option = getattr(existing_send, 'write_option', None) or getattr(existing_send, 'writeOption', None)
                                    if existing_option:
                                        disk_params['disk_write_option'] = existing_option
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
                                # Basic connection settings
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
                                        mllp_params['mllp_persistent'] = str(existing_persistent).lower()
                                # Timeout settings
                                if 'mllp_send_timeout' not in mllp_params:
                                    existing_timeout = getattr(existing_settings, 'send_timeout', None) or getattr(existing_settings, 'sendTimeout', None)
                                    if existing_timeout:
                                        mllp_params['mllp_send_timeout'] = str(existing_timeout)
                                if 'mllp_receive_timeout' not in mllp_params:
                                    existing_timeout = getattr(existing_settings, 'receive_timeout', None) or getattr(existing_settings, 'receiveTimeout', None)
                                    if existing_timeout:
                                        mllp_params['mllp_receive_timeout'] = str(existing_timeout)
                                if 'mllp_halt_timeout' not in mllp_params:
                                    existing_timeout = getattr(existing_settings, 'halt_timeout', None) or getattr(existing_settings, 'haltTimeout', None)
                                    if existing_timeout:
                                        mllp_params['mllp_halt_timeout'] = str(existing_timeout)
                                # Connection settings
                                if 'mllp_max_connections' not in mllp_params:
                                    existing_max = getattr(existing_settings, 'max_connections', None) or getattr(existing_settings, 'maxConnections', None)
                                    if existing_max:
                                        mllp_params['mllp_max_connections'] = str(existing_max)
                                if 'mllp_max_retry' not in mllp_params:
                                    existing_retry = getattr(existing_settings, 'max_retry', None) or getattr(existing_settings, 'maxRetry', None)
                                    if existing_retry:
                                        mllp_params['mllp_max_retry'] = existing_retry
                                if 'mllp_inactivity_timeout' not in mllp_params:
                                    existing_inactivity = getattr(existing_settings, 'inactivity_timeout', None) or getattr(existing_settings, 'inactivityTimeout', None)
                                    if existing_inactivity:
                                        mllp_params['mllp_inactivity_timeout'] = existing_inactivity
                                # SSL settings
                                if 'mllp_use_ssl' not in mllp_params:
                                    existing_ssl = getattr(existing_settings, 'use_ssl', None) or getattr(existing_settings, 'useSsl', None)
                                    if existing_ssl is not None:
                                        mllp_params['mllp_use_ssl'] = str(existing_ssl).lower()
                                if 'mllp_ssl_alias' not in mllp_params:
                                    ssl_cert = getattr(existing_settings, 'ssl_certificate', None) or getattr(existing_settings, 'sslCertificate', None)
                                    if ssl_cert:
                                        existing_alias = getattr(ssl_cert, 'alias', None)
                                        if existing_alias:
                                            mllp_params['mllp_ssl_alias'] = existing_alias
                                if 'mllp_use_client_ssl' not in mllp_params:
                                    existing_client_ssl = getattr(existing_settings, 'use_client_ssl', None) or getattr(existing_settings, 'useClientSsl', None)
                                    if existing_client_ssl is not None:
                                        mllp_params['mllp_use_client_ssl'] = str(existing_client_ssl).lower()
                                if 'mllp_client_ssl_alias' not in mllp_params:
                                    client_ssl = getattr(existing_settings, 'client_ssl_certificate', None) or getattr(existing_settings, 'clientSslCertificate', None)
                                    if client_ssl:
                                        existing_alias = getattr(client_ssl, 'alias', None)
                                        if existing_alias:
                                            mllp_params['mllp_client_ssl_alias'] = existing_alias
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

        # Sanitize partner_info for Custom standard to prevent 400 errors
        # The API rejects empty CustomPartnerInfo structures on UPDATE
        # The SDK returns {'@type': 'CustomPartnerInfo'} for empty custom partners
        existing_standard = getattr(existing_tp, 'standard', None)
        std_val = existing_standard.value if hasattr(existing_standard, 'value') else str(existing_standard) if existing_standard else None
        if std_val and std_val.lower() == 'custom':
            existing_pi = getattr(existing_tp, 'partner_info', None)
            if existing_pi:
                custom_pi = getattr(existing_pi, 'custom_partner_info', None)
                # Empty custom partner info: None, {}, or just {'@type': 'CustomPartnerInfo'}
                is_empty = (
                    custom_pi is None
                    or custom_pi == {}
                    or (isinstance(custom_pi, dict) and set(custom_pi.keys()) <= {'@type'})
                )
                if is_empty:
                    existing_tp.partner_info = None

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
            "message": f"Successfully updated trading partner: {component_id}",
            "warnings": warnings if warnings else None
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