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


def _ga(obj, *attrs):
    """Get first non-None attribute value, safely handling False/0."""
    for attr in attrs:
        val = getattr(obj, attr, None)
        if val is not None:
            return val
    return None


def _header_to_dict(h):
    """Convert SDK Header model object to dict with 4-level fallback."""
    kw = getattr(h, '_kwargs', {})
    return {
        "headerName": _ga(h, 'header_name', 'headerName') or kw.get('headerName') or _ga(h, 'header_field_name', 'headerFieldName') or kw.get('headerFieldName'),
        "headerValue": _ga(h, 'header_value', 'headerValue') or kw.get('headerValue') or _ga(h, 'target_property_name', 'targetPropertyName') or kw.get('targetPropertyName')
    }


def _element_to_dict(e):
    """Convert SDK Element model object to dict."""
    return {"name": getattr(e, 'name', None)}


def _enum_val(v):
    """Extract .value from SDK enum objects; pass through plain strings/ints."""
    if v is None:
        return None
    return getattr(v, 'value', v)


def _strip_enum_prefix(val):
    """Strip SDK enum prefixes like X12IDQUAL_, EDIFACTIDQUAL_, etc. from values."""
    if val is None:
        return None
    s = getattr(val, 'value', val)  # extract .value from enum if needed
    if isinstance(s, str) and '_' in s:
        # Known prefixes: X12IDQUAL_, EDIFACTIDQUAL_, EDIFACTSYNTAXVERSION_, EDIFACTTEST_,
        # ODETTEIDQUAL_, ODETTESYNTAXVERSION_, ODETTETEST_
        for prefix in ('X12IDQUAL_', 'EDIFACTIDQUAL_', 'EDIFACTSYNTAXVERSION_', 'EDIFACTTEST_',
                       'ODETTEIDQUAL_', 'ODETTESYNTAXVERSION_', 'ODETTETEST_'):
            if s.startswith(prefix):
                return s[len(prefix):]
    return s


# AS2 content type: SDK enum string → human-readable display
_AS2_CONTENT_TYPE_DISPLAY = {
    "textplain": "text/plain",
    "textxml": "text/xml",
    "applicationxml": "application/xml",
    "edix12": "application/edi-x12",
    "edifact": "application/edifact",
    "applicationoctetstream": "application/octet-stream",
}


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
        from boomi_mcp.models.trading_partner_builders import build_trading_partner_model, normalize_config_aliases

        # Normalize user-friendly aliases to internal field names
        request_data = normalize_config_aliases(request_data)

        # Validate required fields
        if not request_data.get("component_name"):
            return {
                "_success": False,
                "error": "component_name is required",
                "message": "Trading partner name (component_name) is required"
            }

        # Collect warnings for potentially problematic values
        warnings = []
        # Collect alias normalization warnings
        alias_warnings = request_data.pop("_alias_warnings", None)
        if alias_warnings:
            warnings.extend(alias_warnings)

        ftp_get_action = request_data.get('ftp_get_action', '')
        if ftp_get_action and ftp_get_action.lower() == 'actiongetmove':
            if not request_data.get('ftp_file_to_move'):
                warnings.append(
                    "FTP get_action 'actiongetmove' requires ftp_file_to_move (target directory). "
                    "Also consider setting ftp_move_force_override='true' if target may already exist."
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
                        partner_info["isa_qualifier"] = _strip_enum_prefix(getattr(isa_ctrl, 'interchange_id_qualifier', None))
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
                        partner_info["edifact_interchange_id_qual"] = _strip_enum_prefix(getattr(unb_ctrl, 'interchange_id_qual', None))
                        raw_syntax = getattr(unb_ctrl, 'syntax_id', None)
                        partner_info["edifact_syntax_id"] = raw_syntax.value if hasattr(raw_syntax, 'value') else raw_syntax
                        partner_info["edifact_syntax_version"] = _strip_enum_prefix(getattr(unb_ctrl, 'syntax_version', None))
                        partner_info["edifact_test_indicator"] = _strip_enum_prefix(getattr(unb_ctrl, 'test_indicator', None))

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
                        partner_info["odette_interchange_id_qual"] = _strip_enum_prefix(getattr(odette_unb, 'interchange_id_qual', None))
                        raw_syntax = getattr(odette_unb, 'syntax_id', None)
                        partner_info["odette_syntax_id"] = raw_syntax.value if hasattr(raw_syntax, 'value') else raw_syntax
                        partner_info["odette_syntax_version"] = _strip_enum_prefix(getattr(odette_unb, 'syntax_version', None))
                        partner_info["odette_test_indicator"] = _strip_enum_prefix(getattr(odette_unb, 'test_indicator', None))

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
                    disk_info["get_directory"] = _ga(get_opts, 'get_directory', 'getDirectory')
                    disk_info["file_filter"] = _ga(get_opts, 'file_filter', 'fileFilter')
                    disk_info["filter_match_type"] = _ga(get_opts, 'filter_match_type', 'filterMatchType')
                    disk_info["delete_after_read"] = _ga(get_opts, 'delete_after_read', 'deleteAfterRead')
                    disk_info["max_file_count"] = _ga(get_opts, 'max_file_count', 'maxFileCount')
                if send_opts:
                    disk_info["send_directory"] = _ga(send_opts, 'send_directory', 'sendDirectory')
                    disk_info["create_directory"] = _ga(send_opts, 'create_directory', 'createDirectory')
                    disk_info["write_option"] = _ga(send_opts, 'write_option', 'writeOption')
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
                        # Extract client SSL certificate (componentId is the correct identifier)
                        client_ssl_cert = _ga(ftpssl_opts, 'client_ssl_certificate', 'clientSSLCertificate')
                        if client_ssl_cert:
                            ftp_info["client_ssl_alias"] = _ga(client_ssl_cert, 'component_id', 'componentId') or getattr(client_ssl_cert, 'alias', None)
                # Extract FTP get options
                get_opts = getattr(ftp_opts, 'ftp_get_options', None)
                if get_opts:
                    ftp_info["remote_directory"] = getattr(get_opts, 'remote_directory', None)
                    ftp_info["get_transfer_type"] = getattr(get_opts, 'transfer_type', None)
                    ftp_action = _ga(get_opts, 'ftp_action', 'ftpAction')
                    file_to_move = _ga(get_opts, 'file_to_move', 'fileToMove')
                    # Boomi normalizes actiongetmove → actionget + fileToMove; reconstruct
                    ftp_action_str = getattr(ftp_action, 'value', ftp_action) if ftp_action else ftp_action
                    if ftp_action_str == 'actionget' and file_to_move:
                        ftp_action_str = 'actiongetmove'
                    ftp_info["get_action"] = ftp_action_str
                    ftp_info["max_file_count"] = _ga(get_opts, 'max_file_count', 'maxFileCount')
                    ftp_info["file_to_move"] = file_to_move
                    ftp_info["move_to_directory"] = _ga(get_opts, 'move_to_directory', 'moveToDirectory')
                    ftp_info["move_force_override"] = _ga(get_opts, 'move_to_force_override', 'moveToForceOverride')
                # Extract FTP send options
                send_opts = getattr(ftp_opts, 'ftp_send_options', None)
                if send_opts:
                    ftp_info["send_remote_directory"] = getattr(send_opts, 'remote_directory', None)
                    ftp_info["send_transfer_type"] = getattr(send_opts, 'transfer_type', None)
                    ftp_info["send_action"] = _ga(send_opts, 'ftp_action', 'ftpAction')
                    ftp_info["send_move_to_directory"] = _ga(send_opts, 'move_to_directory', 'moveToDirectory')
                    ftp_info["send_move_force_override"] = _ga(send_opts, 'move_to_force_override', 'moveToForceOverride')
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
                        sftp_info["known_host_entry"] = _ga(sftpssh_opts, 'known_host_entry', 'knownHostEntry')
                        sftp_info["ssh_key_path"] = getattr(sftpssh_opts, 'sshkeypath', None)
                        sftp_info["dh_key_max_1024"] = _ga(sftpssh_opts, 'dh_key_size_max1024', 'dhKeySizeMax1024')
                    # Extract SFTP proxy settings
                    proxy_settings = getattr(settings, 'sftp_proxy_settings', None)
                    if proxy_settings:
                        sftp_info["proxy_enabled"] = _ga(proxy_settings, 'proxy_enabled', 'proxyEnabled')
                        sftp_info["proxy_host"] = getattr(proxy_settings, 'host', None)
                        sftp_info["proxy_port"] = getattr(proxy_settings, 'port', None)
                        sftp_info["proxy_type"] = _ga(proxy_settings, 'type_', 'type')
                        sftp_info["proxy_user"] = getattr(proxy_settings, 'user', None)
                # Extract SFTP get options
                get_opts = getattr(sftp_opts, 'sftp_get_options', None)
                if get_opts:
                    sftp_info["remote_directory"] = _ga(get_opts, 'remote_directory', 'remoteDirectory')
                    sftp_action = _ga(get_opts, 'ftp_action', 'ftpAction')
                    file_to_move = _ga(get_opts, 'file_to_move', 'fileToMove')
                    # Boomi normalizes actiongetmove → actionget + fileToMove; reconstruct
                    sftp_action_str = getattr(sftp_action, 'value', sftp_action) if sftp_action else sftp_action
                    if sftp_action_str == 'actionget' and file_to_move:
                        sftp_action_str = 'actiongetmove'
                    sftp_info["get_action"] = sftp_action_str
                    sftp_info["max_file_count"] = _ga(get_opts, 'max_file_count', 'maxFileCount')
                    sftp_info["file_to_move"] = file_to_move
                    sftp_info["move_to_directory"] = _ga(get_opts, 'move_to_directory', 'moveToDirectory')
                    sftp_info["move_force_override"] = _ga(get_opts, 'move_to_force_override', 'moveToForceOverride')
                # Extract SFTP send options
                send_opts = getattr(sftp_opts, 'sftp_send_options', None)
                if send_opts:
                    sftp_info["send_remote_directory"] = _ga(send_opts, 'remote_directory', 'remoteDirectory')
                    sftp_info["send_action"] = _ga(send_opts, 'ftp_action', 'ftpAction')
                    sftp_info["send_move_to_directory"] = _ga(send_opts, 'move_to_directory', 'moveToDirectory')
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
                    http_info["authentication_type"] = _ga(settings, 'authentication_type', 'authenticationType')
                    http_info["connect_timeout"] = _ga(settings, 'connect_timeout', 'connectTimeout')
                    http_info["read_timeout"] = _ga(settings, 'read_timeout', 'readTimeout')
                    http_info["cookie_scope"] = _ga(settings, 'cookie_scope', 'cookieScope')
                    # Settings flags
                    http_info["use_custom_auth"] = _ga(settings, 'use_custom_auth', 'useCustomAuth')
                    http_info["use_basic_auth"] = _ga(settings, 'use_basic_auth', 'useBasicAuth')
                    http_info["use_default_settings"] = _ga(settings, 'use_default_settings', 'useDefaultSettings')
                    # Extract HTTP auth settings
                    http_auth = _ga(settings, 'http_auth_settings', 'HTTPAuthSettings')
                    if http_auth:
                        http_info["username"] = getattr(http_auth, 'user', None)
                    # Extract HTTP OAuth 1.0 settings
                    oauth1_settings = _ga(settings, 'httpo_auth_settings', 'HTTPOAuthSettings')
                    if oauth1_settings:
                        http_info["oauth1_consumer_key"] = _ga(oauth1_settings, 'consumer_key', 'consumerKey')
                        http_info["oauth1_consumer_secret"] = _ga(oauth1_settings, 'consumer_secret', 'consumerSecret')
                        http_info["oauth1_access_token"] = _ga(oauth1_settings, 'access_token', 'accessToken')
                        http_info["oauth1_token_secret"] = _ga(oauth1_settings, 'token_secret', 'tokenSecret')
                        http_info["oauth1_realm"] = getattr(oauth1_settings, 'realm', None)
                        http_info["oauth1_signature_method"] = _ga(oauth1_settings, 'signature_method', 'signatureMethod')
                        http_info["oauth1_request_token_url"] = _ga(oauth1_settings, 'request_token_url', 'requestTokenURL')
                        http_info["oauth1_access_token_url"] = _ga(oauth1_settings, 'access_token_url', 'accessTokenURL')
                        http_info["oauth1_authorization_url"] = _ga(oauth1_settings, 'authorization_url', 'authorizationURL')
                        http_info["oauth1_suppress_blank_access_token"] = _ga(oauth1_settings, 'suppress_blank_access_token', 'suppressBlankAccessToken')
                    # Extract HTTP OAuth2 settings
                    oauth2_settings = _ga(settings, 'http_oauth2_settings', 'HTTPOAuth2Settings')
                    if oauth2_settings:
                        http_info["oauth_scope"] = getattr(oauth2_settings, 'scope', None)
                        http_info["oauth_grant_type"] = _ga(oauth2_settings, 'grant_type', 'grantType')
                        # Extract token endpoint
                        token_endpoint = _ga(oauth2_settings, 'access_token_endpoint', 'accessTokenEndpoint')
                        if token_endpoint:
                            http_info["oauth_token_url"] = getattr(token_endpoint, 'url', None)
                        # Extract authorization token endpoint
                        auth_token_endpoint = _ga(oauth2_settings, 'authorization_token_endpoint', 'authorizationTokenEndpoint')
                        if auth_token_endpoint:
                            http_info["oauth2_authorization_token_url"] = getattr(auth_token_endpoint, 'url', None)
                        # Extract credentials
                        credentials = getattr(oauth2_settings, 'credentials', None)
                        if credentials:
                            http_info["oauth_client_id"] = _ga(credentials, 'client_id', 'clientId')
                            http_info["oauth2_access_token"] = _ga(credentials, 'access_token', 'accessToken')
                            http_info["oauth2_use_refresh_token"] = _ga(credentials, 'use_refresh_token', 'useRefreshToken')
                        # Extract OAuth2 parameter sets
                        access_params = _ga(oauth2_settings, 'access_token_parameters', 'accessTokenParameters')
                        if access_params:
                            http_info["oauth2_access_token_params"] = access_params
                        auth_params = _ga(oauth2_settings, 'authorization_parameters', 'authorizationParameters')
                        if auth_params:
                            http_info["oauth2_authorization_params"] = auth_params
                    # Extract HTTP SSL options
                    httpssl_opts = _ga(settings, 'httpssl_options', 'HTTPSSLOptions')
                    if httpssl_opts:
                        http_info["client_auth"] = getattr(httpssl_opts, 'clientauth', None)
                        http_info["trust_server_cert"] = _ga(httpssl_opts, 'trust_server_cert', 'trustServerCert')
                        http_info["client_ssl_alias"] = getattr(httpssl_opts, 'clientsslalias', None)
                        http_info["trusted_cert_alias"] = getattr(httpssl_opts, 'trustedcertalias', None)
                # Extract HTTP send options
                send_opts = _ga(http_opts, 'http_send_options', 'HTTPSendOptions')
                if send_opts:
                    http_info["method_type"] = _ga(send_opts, 'method_type', 'methodType')
                    http_info["data_content_type"] = _ga(send_opts, 'data_content_type', 'dataContentType')
                    http_info["follow_redirects"] = _ga(send_opts, 'follow_redirects', 'followRedirects')
                    http_info["return_errors"] = _ga(send_opts, 'return_errors', 'returnErrors')
                    http_info["return_responses"] = _ga(send_opts, 'return_responses', 'returnResponses')
                    http_info["request_profile"] = _ga(send_opts, 'request_profile', 'requestProfile')
                    http_info["request_profile_type"] = _ga(send_opts, 'request_profile_type', 'requestProfileType')
                    http_info["response_profile"] = _ga(send_opts, 'response_profile', 'responseProfile')
                    http_info["response_profile_type"] = _ga(send_opts, 'response_profile_type', 'responseProfileType')
                    # Extract headers/path elements from send options
                    # SDK returns model objects; convert to dicts via module-level helpers
                    req_headers = _ga(send_opts, 'request_headers', 'requestHeaders')
                    if req_headers:
                        header_list = getattr(req_headers, 'header', None)
                        if header_list:
                            http_info["request_headers"] = [_header_to_dict(h) for h in header_list]
                    resp_header_map = _ga(send_opts, 'response_header_mapping', 'responseHeaderMapping')
                    if resp_header_map:
                        header_list = getattr(resp_header_map, 'header', None)
                        if header_list:
                            http_info["response_header_mapping"] = [_header_to_dict(h) for h in header_list]
                    reflect_hdrs = _ga(send_opts, 'reflect_headers', 'reflectHeaders')
                    if reflect_hdrs:
                        elem_list = getattr(reflect_hdrs, 'element', None)
                        if elem_list:
                            http_info["reflect_headers"] = [_element_to_dict(e) for e in elem_list]
                    path_elems = _ga(send_opts, 'path_elements', 'pathElements')
                    if path_elems:
                        elem_list = getattr(path_elems, 'element', None)
                        if elem_list:
                            http_info["path_elements"] = [_element_to_dict(e) for e in elem_list]
                # Extract HTTP get options
                get_opts = _ga(http_opts, 'http_get_options', 'HTTPGetOptions')
                if get_opts:
                    http_info["get_method_type"] = _ga(get_opts, 'method_type', 'methodType')
                    http_info["get_content_type"] = _ga(get_opts, 'data_content_type', 'dataContentType')
                    http_info["get_follow_redirects"] = _ga(get_opts, 'follow_redirects', 'followRedirects')
                    http_info["get_return_errors"] = _ga(get_opts, 'return_errors', 'returnErrors')
                    http_info["get_request_profile"] = _ga(get_opts, 'request_profile', 'requestProfile')
                    http_info["get_request_profile_type"] = _ga(get_opts, 'request_profile_type', 'requestProfileType')
                    http_info["get_response_profile"] = _ga(get_opts, 'response_profile', 'responseProfile')
                    http_info["get_response_profile_type"] = _ga(get_opts, 'response_profile_type', 'responseProfileType')
                    get_req_headers = _ga(get_opts, 'request_headers', 'requestHeaders')
                    if get_req_headers:
                        get_header_list = getattr(get_req_headers, 'header', None)
                        if get_header_list:
                            http_info["get_request_headers"] = [_header_to_dict(h) for h in get_header_list]
                # Extract HTTP listen options
                listen_opts = _ga(http_opts, 'http_listen_options', 'HTTPListenOptions')
                if listen_opts:
                    http_info["listen_mime_passthrough"] = _ga(listen_opts, 'mime_passthrough', 'mimePassthrough')
                    http_info["listen_object_name"] = _ga(listen_opts, 'object_name', 'objectName')
                    http_info["listen_operation_type"] = _ga(listen_opts, 'operation_type', 'operationType')
                    http_info["listen_password"] = getattr(listen_opts, 'password', None)
                    http_info["listen_use_default"] = _ga(listen_opts, 'use_default_listen_options', 'useDefaultListenOptions')
                    http_info["listen_username"] = getattr(listen_opts, 'username', None)
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
                    as2_info["authentication_type"] = _enum_val(_ga(settings, 'authentication_type', 'authenticationType'))
                    as2_info["verify_hostname"] = _ga(settings, 'verify_hostname', 'verifyHostname')
                    # Extract basic auth info
                    auth_settings = _ga(settings, 'auth_settings', 'AuthSettings')
                    if auth_settings:
                        as2_info["username"] = _ga(auth_settings, 'username', 'user')
                    # Extract SSL settings
                    ssl_settings = _ga(settings, 'as2ssl_options', 'AS2SSLOptions')
                    if ssl_settings:
                        as2_info["client_ssl_alias"] = _ga(ssl_settings, 'clientsslalias', 'clientSSLAlias')

                # Extract AS2SendOptions
                send_options = _ga(as2_opts, 'as2_send_options', 'AS2SendOptions')
                if send_options:
                    # Partner info (as2_id + certificates stored here on create)
                    as2_pi = _ga(send_options, 'as2_partner_info', 'AS2PartnerInfo')
                    if as2_pi:
                        as2_info["as2_partner_id"] = _ga(as2_pi, 'as2_id', 'as2Id')
                        as2_info["reject_duplicates"] = _ga(as2_pi, 'reject_duplicates', 'rejectDuplicates')
                        as2_info["duplicate_check_count"] = _ga(as2_pi, 'duplicate_check_count', 'duplicateCheckCount')
                        as2_info["legacy_smime"] = _ga(as2_pi, 'legacy_smime', 'legacySMIME')
                        # Certificates stored in PartnerInfo (CREATE stores them here)
                        enc_cert = _ga(as2_pi, 'encryption_public_certificate', 'encryptionPublicCertificate')
                        if enc_cert:
                            as2_info.setdefault("encrypt_alias", _ga(enc_cert, 'component_id', 'componentId') or getattr(enc_cert, 'alias', None))
                        sign_cert = _ga(as2_pi, 'signing_public_certificate', 'signingPublicCertificate')
                        if sign_cert:
                            as2_info.setdefault("sign_alias", _ga(sign_cert, 'component_id', 'componentId') or getattr(sign_cert, 'alias', None))
                        mdn_cert = _ga(as2_pi, 'mdn_signature_public_certificate', 'mdnSignaturePublicCertificate')
                        if mdn_cert:
                            as2_info.setdefault("mdn_alias", _ga(mdn_cert, 'component_id', 'componentId') or getattr(mdn_cert, 'alias', None))

                    # Message options
                    msg_opts = _ga(send_options, 'as2_message_options', 'AS2MessageOptions')
                    if msg_opts:
                        as2_info["signed"] = getattr(msg_opts, 'signed', None)
                        as2_info["encrypted"] = getattr(msg_opts, 'encrypted', None)
                        as2_info["compressed"] = getattr(msg_opts, 'compressed', None)
                        as2_info["encryption_algorithm"] = _enum_val(_ga(msg_opts, 'encryption_algorithm', 'encryptionAlgorithm'))
                        as2_info["signing_digest_alg"] = _enum_val(_ga(msg_opts, 'signing_digest_alg', 'signingDigestAlg'))
                        raw_ct = _enum_val(_ga(msg_opts, 'data_content_type', 'dataContentType'))
                        as2_info["data_content_type"] = _AS2_CONTENT_TYPE_DISPLAY.get(raw_ct, raw_ct) if raw_ct else None
                        as2_info["subject"] = getattr(msg_opts, 'subject', None)
                        as2_info["multiple_attachments"] = _ga(msg_opts, 'multiple_attachments', 'multipleAttachments')
                        as2_info["max_document_count"] = _ga(msg_opts, 'max_document_count', 'maxDocumentCount')
                        as2_info["attachment_option"] = _ga(msg_opts, 'attachment_option', 'attachmentOption')
                        as2_info["attachment_cache"] = _ga(msg_opts, 'attachment_cache', 'attachmentCache')
                        # Certificate aliases
                        encrypt_cert = _ga(msg_opts, 'encrypt_cert', 'encryptCert')
                        if encrypt_cert:
                            as2_info["encrypt_alias"] = _ga(encrypt_cert, 'component_id', 'componentId') or getattr(encrypt_cert, 'alias', None)
                        sign_cert = _ga(msg_opts, 'sign_cert', 'signCert')
                        if sign_cert:
                            as2_info["sign_alias"] = _ga(sign_cert, 'component_id', 'componentId') or getattr(sign_cert, 'alias', None)

                    # MDN options
                    mdn_opts = _ga(send_options, 'as2_mdn_options', 'AS2MDNOptions')
                    if mdn_opts:
                        as2_info["request_mdn"] = _ga(mdn_opts, 'request_mdn', 'requestMDN')
                        as2_info["mdn_signed"] = getattr(mdn_opts, 'signed', None)
                        as2_info["mdn_digest_alg"] = _enum_val(_ga(mdn_opts, 'mdn_digest_alg', 'mdnDigestAlg'))
                        as2_info["synchronous_mdn"] = _enum_val(getattr(mdn_opts, 'synchronous', None))
                        as2_info["fail_on_negative_mdn"] = _ga(mdn_opts, 'fail_on_negative_mdn', 'failOnNegativeMDN')
                        as2_info["mdn_external_url"] = _ga(mdn_opts, 'external_url', 'externalURL')
                        as2_info["mdn_use_external_url"] = _ga(mdn_opts, 'use_external_url', 'useExternalURL')
                        as2_info["mdn_use_ssl"] = _ga(mdn_opts, 'use_ssl', 'useSSL')
                        # MDN certificate aliases
                        mdn_cert = _ga(mdn_opts, 'mdn_cert', 'mdnCert')
                        if mdn_cert:
                            as2_info["mdn_alias"] = _ga(mdn_cert, 'component_id', 'componentId') or getattr(mdn_cert, 'alias', None)

                # --- MyCompany fallback: receive-side attributes ---
                # For mycompany classification, Boomi populates receive-side attributes
                # instead of send-side. Use setdefault() so send-side always takes priority.

                # AS2DefaultPartnerSettings (like AS2SendSettings for mycompany)
                default_partner = _ga(as2_opts, 'as2_default_partner_settings', 'AS2DefaultPartnerSettings')
                if default_partner:
                    as2_info.setdefault("url", getattr(default_partner, 'url', None))
                    dp_auth = _enum_val(_ga(default_partner, 'authentication_type', 'authenticationType'))
                    if dp_auth is not None:
                        as2_info.setdefault("authentication_type", dp_auth)
                    as2_info.setdefault("verify_hostname", _ga(default_partner, 'verify_hostname', 'verifyHostname'))
                    dp_auth_settings = _ga(default_partner, 'auth_settings', 'AuthSettings')
                    if dp_auth_settings:
                        as2_info.setdefault("username", _ga(dp_auth_settings, 'username', 'user'))
                    dp_ssl = _ga(default_partner, 'as2ssl_options', 'AS2SSLOptions')
                    if dp_ssl:
                        as2_info.setdefault("client_ssl_alias", _ga(dp_ssl, 'clientsslalias', 'clientSSLAlias'))

                # AS2ReceiveOptions (mycompany info, default partner MDN/message options)
                recv_opts = _ga(as2_opts, 'as2_receive_options', 'AS2ReceiveOptions')
                if recv_opts:
                    # AS2MyCompanyInfo — as2_id, legacy_smime, private certificates
                    my_info = _ga(recv_opts, 'as2_my_company_info', 'AS2MyCompanyInfo')
                    if my_info:
                        as2_info.setdefault("as2_partner_id", _ga(my_info, 'as2_id', 'as2Id'))
                        as2_info.setdefault("legacy_smime", _ga(my_info, 'legacy_smime', 'legacySMIME'))
                        enc_cert = _ga(my_info, 'encryption_private_certificate', 'encryptionPrivateCertificate')
                        if enc_cert:
                            as2_info.setdefault("encrypt_alias", _ga(enc_cert, 'component_id', 'componentId') or getattr(enc_cert, 'alias', None))
                        sign_cert = _ga(my_info, 'signing_private_certificate', 'signingPrivateCertificate')
                        if sign_cert:
                            as2_info.setdefault("sign_alias", _ga(sign_cert, 'component_id', 'componentId') or getattr(sign_cert, 'alias', None))
                        mdn_cert = _ga(my_info, 'mdn_signature_private_certificate', 'mdnSignaturePrivateCertificate')
                        if mdn_cert:
                            as2_info.setdefault("mdn_alias", _ga(mdn_cert, 'component_id', 'componentId') or getattr(mdn_cert, 'alias', None))

                    # Default partner MDN options
                    dp_mdn = _ga(recv_opts, 'as2_default_partner_mdn_options', 'AS2DefaultPartnerMDNOptions')
                    if not dp_mdn:
                        dp_mdn = _ga(recv_opts, 'as2_mdn_options', 'AS2MDNOptions')
                    if dp_mdn:
                        as2_info.setdefault("request_mdn", _ga(dp_mdn, 'request_mdn', 'requestMDN'))
                        as2_info.setdefault("mdn_signed", getattr(dp_mdn, 'signed', None))
                        mdn_dig = _enum_val(_ga(dp_mdn, 'mdn_digest_alg', 'mdnDigestAlg'))
                        if mdn_dig is not None:
                            as2_info.setdefault("mdn_digest_alg", mdn_dig)
                        sync_val = _enum_val(getattr(dp_mdn, 'synchronous', None))
                        if sync_val is not None:
                            as2_info.setdefault("synchronous_mdn", sync_val)
                        as2_info.setdefault("fail_on_negative_mdn", _ga(dp_mdn, 'fail_on_negative_mdn', 'failOnNegativeMDN'))

                    # Default partner message options
                    dp_msg = _ga(recv_opts, 'as2_default_partner_message_options', 'AS2DefaultPartnerMessageOptions')
                    if not dp_msg:
                        dp_msg = _ga(recv_opts, 'as2_message_options', 'AS2MessageOptions')
                    if dp_msg:
                        as2_info.setdefault("signed", getattr(dp_msg, 'signed', None))
                        as2_info.setdefault("encrypted", getattr(dp_msg, 'encrypted', None))
                        as2_info.setdefault("compressed", getattr(dp_msg, 'compressed', None))
                        enc_alg = _enum_val(_ga(dp_msg, 'encryption_algorithm', 'encryptionAlgorithm'))
                        if enc_alg is not None:
                            as2_info.setdefault("encryption_algorithm", enc_alg)
                        sign_dig = _enum_val(_ga(dp_msg, 'signing_digest_alg', 'signingDigestAlg'))
                        if sign_dig is not None:
                            as2_info.setdefault("signing_digest_alg", sign_dig)
                        raw_ct = _enum_val(_ga(dp_msg, 'data_content_type', 'dataContentType'))
                        if raw_ct is not None:
                            as2_info.setdefault("data_content_type", _AS2_CONTENT_TYPE_DISPLAY.get(raw_ct, raw_ct))
                        as2_info.setdefault("subject", getattr(dp_msg, 'subject', None))

                # Filter out None values
                as2_info = {k: v for k, v in as2_info.items() if v is not None}
                communication_protocols.append(as2_info)

            # MLLP protocol
            if getattr(comm, 'mllp_communication_options', None):
                mllp_opts = comm.mllp_communication_options
                mllp_info = {"protocol": "mllp"}
                settings = _ga(mllp_opts, 'mllp_send_settings', 'MLLPSendSettings')
                if settings:
                    mllp_info["host"] = getattr(settings, 'host', None)
                    mllp_info["port"] = getattr(settings, 'port', None)
                    mllp_info["persistent"] = getattr(settings, 'persistent', None)
                    mllp_info["receive_timeout"] = _ga(settings, 'receive_timeout', 'receiveTimeout')
                    mllp_info["send_timeout"] = _ga(settings, 'send_timeout', 'sendTimeout')
                    mllp_info["max_connections"] = _ga(settings, 'max_connections', 'maxConnections')
                    mllp_info["inactivity_timeout"] = _ga(settings, 'inactivity_timeout', 'inactivityTimeout')
                    mllp_info["max_retry"] = _ga(settings, 'max_retry', 'maxRetry')
                    mllp_info["halt_timeout"] = _ga(settings, 'halt_timeout', 'haltTimeout')
                    # Extract MLLP SSL options
                    mllpssl_opts = _ga(settings, 'mllpssl_options', 'MLLPSSLOptions')
                    if mllpssl_opts:
                        mllp_info["use_ssl"] = _ga(mllpssl_opts, 'use_ssl', 'useSSL')
                        mllp_info["use_client_ssl"] = _ga(mllpssl_opts, 'use_client_ssl', 'useClientSSL')
                        mllp_info["client_ssl_alias"] = _ga(mllpssl_opts, 'client_ssl_alias', 'clientSSLAlias')
                        mllp_info["ssl_alias"] = _ga(mllpssl_opts, 'ssl_alias', 'sslAlias')
                # --- Fallback: check _kwargs for raw dict data if SDK didn't deserialize ---
                if not settings:
                    kw = getattr(mllp_opts, '_kwargs', {})
                    raw_send = kw.get('MLLPSendSettings') or kw.get('mllpSendSettings')
                    if raw_send and isinstance(raw_send, dict):
                        mllp_info["host"] = raw_send.get('host')
                        mllp_info["port"] = raw_send.get('port')
                        mllp_info["persistent"] = raw_send.get('persistent')
                        mllp_info["receive_timeout"] = raw_send.get('receiveTimeout')
                        mllp_info["send_timeout"] = raw_send.get('sendTimeout')
                        mllp_info["max_connections"] = raw_send.get('maxConnections')
                        mllp_info["inactivity_timeout"] = raw_send.get('inactivityTimeout')
                        mllp_info["max_retry"] = raw_send.get('maxRetry')
                        mllp_info["halt_timeout"] = raw_send.get('haltTimeout')
                        ssl_data = raw_send.get('MLLPSSLOptions') or raw_send.get('mllpsslOptions')
                        if ssl_data and isinstance(ssl_data, dict):
                            mllp_info["use_ssl"] = ssl_data.get('useSSL')
                            mllp_info["use_client_ssl"] = ssl_data.get('useClientSSL')
                            mllp_info["client_ssl_alias"] = ssl_data.get('clientSSLAlias')
                            mllp_info["ssl_alias"] = ssl_data.get('sslAlias')
                        settings = True  # Mark as found to skip listen fallback
                # --- MyCompany fallback: listen-side attributes ---
                # For mycompany, MLLP data may be in _kwargs under MLLPListenSettings
                # when mllp_send_settings yields no data.
                if not settings:
                    kw = getattr(mllp_opts, '_kwargs', {})
                    listen = kw.get('MLLPListenSettings') or kw.get('mllpListenSettings')
                    if listen and isinstance(listen, dict):
                        mllp_info["host"] = listen.get('host')
                        mllp_info["port"] = listen.get('port')
                        mllp_info["persistent"] = listen.get('persistent')
                        mllp_info["receive_timeout"] = listen.get('receiveTimeout')
                        mllp_info["send_timeout"] = listen.get('sendTimeout')
                        mllp_info["max_connections"] = listen.get('maxConnections')
                        mllp_info["inactivity_timeout"] = listen.get('inactivityTimeout')
                        mllp_info["max_retry"] = listen.get('maxRetry')
                        mllp_info["halt_timeout"] = listen.get('haltTimeout')
                        ssl_data = listen.get('MLLPSSLOptions') or listen.get('mllpsslOptions')
                        if ssl_data and isinstance(ssl_data, dict):
                            mllp_info["use_ssl"] = ssl_data.get('useSSL')
                            mllp_info["use_client_ssl"] = ssl_data.get('useClientSSL')
                            mllp_info["client_ssl_alias"] = ssl_data.get('clientSSLAlias')
                            mllp_info["ssl_alias"] = ssl_data.get('sslAlias')
                    elif hasattr(mllp_opts, '__dict__'):
                        # Try attribute-based access for SDK model fallback
                        listen_obj = _ga(mllp_opts, 'mllp_listen_settings', 'MLLPListenSettings')
                        if listen_obj:
                            mllp_info["host"] = getattr(listen_obj, 'host', None)
                            mllp_info["port"] = getattr(listen_obj, 'port', None)
                            mllp_info["persistent"] = getattr(listen_obj, 'persistent', None)
                            mllp_info["receive_timeout"] = _ga(listen_obj, 'receive_timeout', 'receiveTimeout')
                            mllp_info["send_timeout"] = _ga(listen_obj, 'send_timeout', 'sendTimeout')
                            mllp_info["max_connections"] = _ga(listen_obj, 'max_connections', 'maxConnections')
                            mllp_info["inactivity_timeout"] = _ga(listen_obj, 'inactivity_timeout', 'inactivityTimeout')
                            mllp_info["max_retry"] = _ga(listen_obj, 'max_retry', 'maxRetry')
                            mllp_info["halt_timeout"] = _ga(listen_obj, 'halt_timeout', 'haltTimeout')
                            mllpssl = _ga(listen_obj, 'mllpssl_options', 'MLLPSSLOptions')
                            if mllpssl:
                                mllp_info["use_ssl"] = _ga(mllpssl, 'use_ssl', 'useSSL')
                                mllp_info["use_client_ssl"] = _ga(mllpssl, 'use_client_ssl', 'useClientSSL')
                                mllp_info["client_ssl_alias"] = _ga(mllpssl, 'client_ssl_alias', 'clientSSLAlias')
                                mllp_info["ssl_alias"] = _ga(mllpssl, 'ssl_alias', 'sslAlias')

                # Filter out None values
                mllp_info = {k: v for k, v in mllp_info.items() if v is not None}
                communication_protocols.append(mllp_info)

            # OFTP protocol
            if getattr(comm, 'oftp_communication_options', None):
                oftp_opts = comm.oftp_communication_options
                oftp_info = {"protocol": "oftp"}
                conn_settings = _ga(oftp_opts, 'oftp_connection_settings', 'OFTPConnectionSettings')
                if conn_settings:
                    # Old partners nest fields under defaultOFTPConnectionSettings;
                    # new partners put them directly in conn_settings.
                    # Check default_settings first for each field, fall back to conn_settings.
                    default_settings = _ga(conn_settings, 'default_oftp_connection_settings', 'defaultOFTPConnectionSettings')
                    def _oftp_val(*attrs):
                        if default_settings:
                            val = _ga(default_settings, *attrs)
                            if val is not None:
                                return val
                        return _ga(conn_settings, *attrs)
                    oftp_info["host"] = _oftp_val('host')
                    oftp_info["port"] = _oftp_val('port')
                    oftp_info["tls"] = _oftp_val('tls')
                    oftp_info["ssid_auth"] = _oftp_val('ssidauth')
                    oftp_info["sfid_cipher"] = _oftp_val('sfidciph')
                    oftp_info["use_gateway"] = _oftp_val('use_gateway', 'useGateway')
                    oftp_info["use_client_ssl"] = _oftp_val('use_client_ssl', 'useClientSSL')
                    oftp_info["client_ssl_alias"] = _oftp_val('client_ssl_alias', 'clientSSLAlias')
                    # Extract partner info - per-field fallback across both levels
                    default_partner = _ga(default_settings, 'my_partner_info', 'myPartnerInfo') if default_settings else None
                    direct_partner = _ga(conn_settings, 'my_partner_info', 'myPartnerInfo')
                    if default_partner or direct_partner:
                        def _partner_val(attr, alt=None):
                            for obj in (default_partner, direct_partner):
                                if obj:
                                    val = _ga(obj, attr, alt) if alt else getattr(obj, attr, None)
                                    if val is not None:
                                        return val
                            return None
                        oftp_info["ssid_code"] = _partner_val('ssidcode')
                        oftp_info["compress"] = _partner_val('ssidcmpr')
                        oftp_info["sfid_sign"] = _partner_val('sfidsign')
                        oftp_info["sfid_encrypt"] = _partner_val('sfidsec_encrypt', 'sfidsec-encrypt')
                # --- MyCompany fallback: server listen-side attributes ---
                # For mycompany, OFTP data may be in server listen options instead of connection settings.
                listen_opts = _ga(oftp_opts, 'oftp_server_listen_options', 'OFTPServerListenOptions')
                if listen_opts:
                    oftp_info.setdefault("listen_operation", _ga(listen_opts, 'listen_operation', 'listenOperation'))
                    partner_group = _ga(listen_opts, 'partner_group', 'partnerGroup')
                    if partner_group is not None:
                        oftp_info.setdefault("partner_group", partner_group)
                    # Local certificates for mycompany listener
                    local_certs = _ga(listen_opts, 'local_certificates', 'localCertificates')
                    if local_certs:
                        oftp_info.setdefault("local_certificates", local_certs)

                # Parse oftp_get_options and oftp_send_options if present
                get_opts = _ga(oftp_opts, 'oftp_get_options', 'OFTPGetOptions')
                if get_opts:
                    oftp_info.setdefault("get_use_default", _ga(get_opts, 'use_default_get_options', 'useDefaultGetOptions'))
                send_opts = _ga(oftp_opts, 'oftp_send_options', 'OFTPSendOptions')
                if send_opts:
                    oftp_info.setdefault("send_use_default", _ga(send_opts, 'use_default_send_options', 'useDefaultSendOptions'))

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

        # Query trading partners using typed config with pagination
        all_results = []
        result = boomi_client.trading_partner_component.query_trading_partner_component(
            request_body=query_config
        )
        if hasattr(result, 'result') and result.result:
            all_results.extend(result.result)
        query_token = getattr(result, 'query_token', None)
        while query_token and len(all_results) < 1000:
            try:
                result = boomi_client.trading_partner_component.query_more_trading_partner_component(
                    request_body=query_token
                )
                if hasattr(result, 'result') and result.result:
                    all_results.extend(result.result)
                query_token = getattr(result, 'query_token', None)
            except Exception:
                break

        # Fallback standard from filter (Boomi QUERY API omits standard for some types like odette)
        filter_standard = filters.get("standard") if filters else None

        partners = []
        for partner in all_results:
                # Extract ID using SDK pattern (id_ attribute)
                partner_id = None
                if hasattr(partner, 'id_'):
                    partner_id = partner.id_
                elif hasattr(partner, 'id'):
                    partner_id = partner.id
                elif hasattr(partner, 'component_id'):
                    partner_id = partner.component_id

                raw_std = getattr(partner, 'standard', None)
                raw_cls = getattr(partner, 'classification', None)
                std_val = raw_std.value if hasattr(raw_std, 'value') else raw_std
                # Boomi QUERY API omits standard for some types (e.g., odette); use filter as fallback
                if std_val is None and filter_standard:
                    std_val = filter_standard.lower()
                # If still None, retrieve standard via GET (lightweight per-partner call)
                if std_val is None and partner_id:
                    try:
                        full_tp = boomi_client.trading_partner_component.get_trading_partner_component(id_=partner_id)
                        fetched_std = getattr(full_tp, 'standard', None)
                        std_val = fetched_std.value if hasattr(fetched_std, 'value') else fetched_std
                    except Exception:
                        pass  # leave as None if GET fails
                partners.append({
                    "component_id": partner_id,
                    "name": getattr(partner, 'name', getattr(partner, 'component_name', None)),
                    "standard": std_val,
                    "classification": raw_cls.value if hasattr(raw_cls, 'value') else raw_cls,
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


HTTP_UPDATE_DENYLIST = {"http_cookie_scope"}


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
        from boomi_mcp.models.trading_partner_builders import build_contact_info, normalize_config_aliases
        from boomi.models import ContactInfo

        # Normalize user-friendly aliases to internal field names
        updates = normalize_config_aliases(updates)

        # Collect warnings for potentially problematic values
        warnings = []
        # Collect alias normalization warnings
        alias_warnings = updates.pop("_alias_warnings", None)
        if alias_warnings:
            warnings.extend(alias_warnings)

        ftp_get_action = updates.get('ftp_get_action', '')
        if ftp_get_action and ftp_get_action.lower() == 'actiongetmove':
            if not updates.get('ftp_file_to_move'):
                warnings.append(
                    "FTP get_action 'actiongetmove' requires ftp_file_to_move (target directory). "
                    "Also consider setting ftp_move_force_override='true' if target may already exist."
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
                    'contact_name': _ga(existing_contact, 'contact_name', 'name') or '',
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

        # Standard-specific partner_info update
        partner_info_fields = {
            'x12': ['isa_id', 'isa_qualifier', 'gs_id'],
            'edifact': ['edifact_interchange_id', 'edifact_interchange_id_qual', 'edifact_syntax_id',
                        'edifact_syntax_version', 'edifact_test_indicator'],
            'hl7': ['hl7_application', 'hl7_facility'],
            'rosettanet': ['rosettanet_partner_id', 'rosettanet_partner_location',
                           'rosettanet_global_usage_code', 'rosettanet_supply_chain_code',
                           'rosettanet_classification_code'],
            'tradacoms': ['tradacoms_interchange_id', 'tradacoms_interchange_id_qualifier'],
            'odette': ['odette_interchange_id', 'odette_interchange_id_qual', 'odette_syntax_id',
                       'odette_syntax_version', 'odette_test_indicator'],
        }
        all_pi_fields = [f for fields in partner_info_fields.values() for f in fields]
        pi_updates = {k: v for k, v in updates.items() if k in all_pi_fields}

        if pi_updates:
            from boomi_mcp.models.trading_partner_builders import build_partner_info

            existing_standard = getattr(existing_tp, 'standard', None)
            std_val = existing_standard.value if hasattr(existing_standard, 'value') else str(existing_standard) if existing_standard else None

            if std_val:
                std_lower = std_val.lower()
                # Extract existing partner_info values to merge with updates
                existing_pi_values = {}
                existing_pi = getattr(existing_tp, 'partner_info', None)
                if existing_pi:
                    if std_lower == 'x12':
                        x12_info = getattr(existing_pi, 'x12_partner_info', None)
                        if x12_info:
                            x12_ctrl = getattr(x12_info, 'x12_control_info', None)
                            if x12_ctrl:
                                isa_ctrl = getattr(x12_ctrl, 'isa_control_info', None)
                                gs_ctrl = getattr(x12_ctrl, 'gs_control_info', None)
                                if isa_ctrl:
                                    existing_pi_values['isa_id'] = getattr(isa_ctrl, 'interchange_id', None)
                                    existing_pi_values['isa_qualifier'] = _strip_enum_prefix(getattr(isa_ctrl, 'interchange_id_qualifier', None))
                                if gs_ctrl:
                                    existing_pi_values['gs_id'] = getattr(gs_ctrl, 'applicationcode', None)
                    elif std_lower == 'edifact':
                        edifact_info = getattr(existing_pi, 'edifact_partner_info', None)
                        if edifact_info:
                            edifact_ctrl = getattr(edifact_info, 'edifact_control_info', None)
                            if edifact_ctrl:
                                unb_ctrl = getattr(edifact_ctrl, 'unb_control_info', None)
                                if unb_ctrl:
                                    existing_pi_values['edifact_interchange_id'] = getattr(unb_ctrl, 'interchange_id', None)
                                    existing_pi_values['edifact_interchange_id_qual'] = _strip_enum_prefix(getattr(unb_ctrl, 'interchange_id_qual', None))
                                    raw = getattr(unb_ctrl, 'syntax_id', None)
                                    existing_pi_values['edifact_syntax_id'] = raw.value if hasattr(raw, 'value') else raw
                                    existing_pi_values['edifact_syntax_version'] = _strip_enum_prefix(getattr(unb_ctrl, 'syntax_version', None))
                                    existing_pi_values['edifact_test_indicator'] = _strip_enum_prefix(getattr(unb_ctrl, 'test_indicator', None))
                    elif std_lower == 'hl7':
                        hl7_info = getattr(existing_pi, 'hl7_partner_info', None)
                        if hl7_info:
                            hl7_ctrl = getattr(hl7_info, 'hl7_control_info', None)
                            if hl7_ctrl:
                                msh_ctrl = getattr(hl7_ctrl, 'msh_control_info', None)
                                if msh_ctrl:
                                    app = getattr(msh_ctrl, 'application', None)
                                    if app:
                                        existing_pi_values['hl7_application'] = getattr(app, 'namespace_id', None)
                                    fac = getattr(msh_ctrl, 'facility', None)
                                    if fac:
                                        existing_pi_values['hl7_facility'] = getattr(fac, 'namespace_id', None)
                    elif std_lower == 'rosettanet':
                        rn_info = getattr(existing_pi, 'rosetta_net_partner_info', None)
                        if rn_info:
                            rn_ctrl = getattr(rn_info, 'rosetta_net_control_info', None)
                            if rn_ctrl:
                                existing_pi_values['rosettanet_partner_id'] = getattr(rn_ctrl, 'partner_id', None)
                                existing_pi_values['rosettanet_partner_location'] = getattr(rn_ctrl, 'partner_location', None)
                                raw = getattr(rn_ctrl, 'global_usage_code', None)
                                existing_pi_values['rosettanet_global_usage_code'] = raw.value if hasattr(raw, 'value') else raw
                                existing_pi_values['rosettanet_supply_chain_code'] = getattr(rn_ctrl, 'supply_chain_code', None)
                                existing_pi_values['rosettanet_classification_code'] = getattr(rn_ctrl, 'global_partner_classification_code', None)
                    elif std_lower == 'tradacoms':
                        tc_info = getattr(existing_pi, 'tradacoms_partner_info', None)
                        if tc_info:
                            tc_ctrl = getattr(tc_info, 'tradacoms_control_info', None)
                            if tc_ctrl:
                                stx_ctrl = getattr(tc_ctrl, 'stx_control_info', None)
                                if stx_ctrl:
                                    existing_pi_values['tradacoms_interchange_id'] = getattr(stx_ctrl, 'interchange_id', None)
                                    existing_pi_values['tradacoms_interchange_id_qualifier'] = getattr(stx_ctrl, 'interchange_id_qualifier', None)
                    elif std_lower == 'odette':
                        od_info = getattr(existing_pi, 'odette_partner_info', None)
                        if od_info:
                            od_ctrl = getattr(od_info, 'odette_control_info', None)
                            if od_ctrl:
                                od_unb = getattr(od_ctrl, 'odette_unb_control_info', None)
                                if od_unb:
                                    existing_pi_values['odette_interchange_id'] = getattr(od_unb, 'interchange_id', None)
                                    existing_pi_values['odette_interchange_id_qual'] = _strip_enum_prefix(getattr(od_unb, 'interchange_id_qual', None))
                                    raw = getattr(od_unb, 'syntax_id', None)
                                    existing_pi_values['odette_syntax_id'] = raw.value if hasattr(raw, 'value') else raw
                                    existing_pi_values['odette_syntax_version'] = _strip_enum_prefix(getattr(od_unb, 'syntax_version', None))
                                    existing_pi_values['odette_test_indicator'] = _strip_enum_prefix(getattr(od_unb, 'test_indicator', None))

                # Remove None values from existing, merge with updates
                existing_pi_values = {k: v for k, v in existing_pi_values.items() if v is not None}
                merged_pi = {**existing_pi_values, **pi_updates}

                new_partner_info = build_partner_info(std_lower, **merged_pi)
                if new_partner_info:
                    existing_tp.partner_info = new_partner_info

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
                # Strip create-only HTTP fields to prevent Boomi 400 errors
                for field in HTTP_UPDATE_DENYLIST:
                    if field in http_params:
                        del http_params[field]
                        warnings.append(
                            f"{field} is not supported on update and was ignored to prevent Boomi 400 error"
                        )
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
                                if 'as2_username' not in as2_params or 'as2_password' not in as2_params:
                                    auth_settings = _ga(existing_send_settings, 'auth_settings', 'AuthSettings')
                                    if auth_settings:
                                        if 'as2_username' not in as2_params:
                                            existing_user = _ga(auth_settings, 'username', 'user')
                                            if existing_user:
                                                as2_params['as2_username'] = existing_user
                                        if 'as2_password' not in as2_params:
                                            existing_pass = getattr(auth_settings, 'password', None)
                                            if existing_pass:
                                                as2_params['as2_password'] = existing_pass
                                if 'as2_verify_hostname' not in as2_params:
                                    existing_verify = _ga(existing_send_settings, 'verify_hostname', 'verifyHostname')
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
                                    if 'as2_reject_duplicates' not in as2_params:
                                        existing_reject = _ga(existing_partner_info, 'reject_duplicates', 'rejectDuplicates')
                                        if existing_reject is not None:
                                            as2_params['as2_reject_duplicates'] = str(existing_reject).lower()
                                    if 'as2_duplicate_check_count' not in as2_params:
                                        existing_check = _ga(existing_partner_info, 'duplicate_check_count', 'duplicateCheckCount')
                                        if existing_check is not None:
                                            as2_params['as2_duplicate_check_count'] = existing_check
                                # Navigate to sub-objects matching GET extraction paths
                                existing_msg_opts = _ga(existing_send_opts, 'as2_message_options', 'AS2MessageOptions')
                                existing_mdn_opts = _ga(existing_send_opts, 'as2_mdn_options', 'AS2MDNOptions')

                                # Certs and message options (under AS2MessageOptions)
                                if existing_msg_opts:
                                    if 'as2_encrypt_alias' not in as2_params:
                                        encrypt_cert = _ga(existing_msg_opts, 'encrypt_cert', 'encryptCert')
                                        if encrypt_cert:
                                            existing_alias = _ga(encrypt_cert, 'component_id', 'componentId') or getattr(encrypt_cert, 'alias', None)
                                            if existing_alias:
                                                as2_params['as2_encrypt_alias'] = existing_alias
                                    if 'as2_sign_alias' not in as2_params:
                                        sign_cert = _ga(existing_msg_opts, 'sign_cert', 'signCert')
                                        if sign_cert:
                                            existing_alias = _ga(sign_cert, 'component_id', 'componentId') or getattr(sign_cert, 'alias', None)
                                            if existing_alias:
                                                as2_params['as2_sign_alias'] = existing_alias
                                    if 'as2_signed' not in as2_params:
                                        existing_signed = getattr(existing_msg_opts, 'signed', None)
                                        if existing_signed is not None:
                                            as2_params['as2_signed'] = str(existing_signed).lower()
                                    if 'as2_encrypted' not in as2_params:
                                        existing_encrypted = getattr(existing_msg_opts, 'encrypted', None)
                                        if existing_encrypted is not None:
                                            as2_params['as2_encrypted'] = str(existing_encrypted).lower()
                                    if 'as2_compressed' not in as2_params:
                                        existing_compressed = getattr(existing_msg_opts, 'compressed', None)
                                        if existing_compressed is not None:
                                            as2_params['as2_compressed'] = str(existing_compressed).lower()
                                    if 'as2_encryption_algorithm' not in as2_params:
                                        existing_algo = _ga(existing_msg_opts, 'encryption_algorithm', 'encryptionAlgorithm')
                                        if existing_algo:
                                            as2_params['as2_encryption_algorithm'] = existing_algo
                                    if 'as2_signing_digest_alg' not in as2_params:
                                        existing_digest = _ga(existing_msg_opts, 'signing_digest_alg', 'signingDigestAlg')
                                        if existing_digest:
                                            as2_params['as2_signing_digest_alg'] = existing_digest
                                    if 'as2_data_content_type' not in as2_params:
                                        existing_content = _ga(existing_msg_opts, 'data_content_type', 'dataContentType')
                                        if existing_content:
                                            as2_params['as2_data_content_type'] = existing_content
                                    if 'as2_subject' not in as2_params:
                                        existing_subject = getattr(existing_msg_opts, 'subject', None)
                                        if existing_subject:
                                            as2_params['as2_subject'] = existing_subject
                                    if 'as2_multiple_attachments' not in as2_params:
                                        existing_multi = _ga(existing_msg_opts, 'multiple_attachments', 'multipleAttachments')
                                        if existing_multi is not None:
                                            as2_params['as2_multiple_attachments'] = str(existing_multi).lower()
                                    if 'as2_max_document_count' not in as2_params:
                                        existing_max = _ga(existing_msg_opts, 'max_document_count', 'maxDocumentCount')
                                        if existing_max:
                                            as2_params['as2_max_document_count'] = existing_max

                                # MDN options (under AS2MDNOptions)
                                if existing_mdn_opts:
                                    if 'as2_request_mdn' not in as2_params:
                                        existing_req_mdn = _ga(existing_mdn_opts, 'request_mdn', 'requestMDN')
                                        if existing_req_mdn is not None:
                                            as2_params['as2_request_mdn'] = str(existing_req_mdn).lower()
                                    if 'as2_mdn_signed' not in as2_params:
                                        existing_mdn_signed = getattr(existing_mdn_opts, 'signed', None)
                                        if existing_mdn_signed is not None:
                                            as2_params['as2_mdn_signed'] = str(existing_mdn_signed).lower()
                                    if 'as2_mdn_digest_alg' not in as2_params:
                                        existing_mdn_digest = _ga(existing_mdn_opts, 'mdn_digest_alg', 'mdnDigestAlg')
                                        if existing_mdn_digest:
                                            as2_params['as2_mdn_digest_alg'] = existing_mdn_digest
                                    if 'as2_synchronous_mdn' not in as2_params:
                                        existing_sync_mdn = getattr(existing_mdn_opts, 'synchronous', None)
                                        if existing_sync_mdn is not None:
                                            # API returns 'sync'/'async' but builder expects 'true'/'false'
                                            as2_params['as2_synchronous_mdn'] = 'true' if str(existing_sync_mdn).lower() == 'sync' else 'false'
                                    if 'as2_mdn_external_url' not in as2_params:
                                        existing_ext_url = _ga(existing_mdn_opts, 'external_url', 'externalURL')
                                        if existing_ext_url:
                                            as2_params['as2_mdn_external_url'] = existing_ext_url
                                    if 'as2_mdn_use_external_url' not in as2_params:
                                        existing_use_ext = _ga(existing_mdn_opts, 'use_external_url', 'useExternalURL')
                                        if existing_use_ext is not None:
                                            as2_params['as2_mdn_use_external_url'] = str(existing_use_ext).lower()
                                    if 'as2_mdn_use_ssl' not in as2_params:
                                        existing_use_ssl = _ga(existing_mdn_opts, 'use_ssl', 'useSSL')
                                        if existing_use_ssl is not None:
                                            as2_params['as2_mdn_use_ssl'] = str(existing_use_ssl).lower()
                                    if 'as2_fail_on_negative_mdn' not in as2_params:
                                        existing_fail = _ga(existing_mdn_opts, 'fail_on_negative_mdn', 'failOnNegativeMDN')
                                        if existing_fail is not None:
                                            as2_params['as2_fail_on_negative_mdn'] = str(existing_fail).lower()

                                # Legacy S/MIME (under partner info, not send options)
                                if existing_partner_info:
                                    if 'as2_legacy_smime' not in as2_params:
                                        existing_legacy = _ga(existing_partner_info, 'legacy_smime', 'legacySMIME')
                                        if existing_legacy is not None:
                                            as2_params['as2_legacy_smime'] = str(existing_legacy).lower()

                            # Preserve AS2 Receive Options (MDN delivery)
                            existing_recv_opts = getattr(existing_as2, 'as2_receive_options', None)
                            if existing_recv_opts:
                                if 'as2_mdn_alias' not in as2_params:
                                    mdn_cert = _ga(existing_recv_opts, 'mdn_certificate', 'mdnCertificate')
                                    if mdn_cert:
                                        existing_alias = getattr(mdn_cert, 'alias', None)
                                        if existing_alias:
                                            as2_params['as2_mdn_alias'] = existing_alias

                    cls = updates.get('classification', None)
                    # Normalize enum to string (e.g. TradingPartnerComponentClassification.MYCOMPANY -> 'mycompany')
                    if cls and hasattr(cls, 'value'):
                        cls = cls.value
                    if not cls:
                        raw_cls = getattr(existing_tp, 'classification', None)
                        cls = raw_cls.value if hasattr(raw_cls, 'value') else raw_cls
                    if cls:
                        as2_params['classification'] = cls
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
                                if 'http_username' not in http_params or 'http_password' not in http_params:
                                    http_auth = _ga(existing_settings, 'http_auth_settings', 'HTTPAuthSettings')
                                    if http_auth:
                                        if 'http_username' not in http_params:
                                            existing_user = getattr(http_auth, 'user', None)
                                            if existing_user:
                                                http_params['http_username'] = existing_user
                                        if 'http_password' not in http_params:
                                            existing_pass = getattr(http_auth, 'password', None)
                                            if existing_pass:
                                                http_params['http_password'] = existing_pass
                                # Timeout settings
                                if 'http_connect_timeout' not in http_params:
                                    existing_timeout = _ga(existing_settings, 'connect_timeout', 'connectTimeout')
                                    if existing_timeout:
                                        http_params['http_connect_timeout'] = str(existing_timeout)
                                if 'http_read_timeout' not in http_params:
                                    existing_timeout = _ga(existing_settings, 'read_timeout', 'readTimeout')
                                    if existing_timeout:
                                        http_params['http_read_timeout'] = str(existing_timeout)
                                # SSL settings (nested under HTTPSSLOptions)
                                existing_ssl_opts = _ga(existing_settings, 'httpssl_options', 'HTTPSSLOptions')
                                if existing_ssl_opts:
                                    if 'http_client_auth' not in http_params:
                                        existing_client_auth = getattr(existing_ssl_opts, 'clientauth', None)
                                        if existing_client_auth is not None:
                                            http_params['http_client_auth'] = str(existing_client_auth).lower()
                                    if 'http_trust_server_cert' not in http_params:
                                        existing_trust = _ga(existing_ssl_opts, 'trust_server_cert', 'trustServerCert')
                                        if existing_trust is not None:
                                            http_params['http_trust_server_cert'] = str(existing_trust).lower()
                                    if 'http_client_ssl_alias' not in http_params:
                                        existing_alias = getattr(existing_ssl_opts, 'clientsslalias', None)
                                        if existing_alias:
                                            http_params['http_client_ssl_alias'] = existing_alias
                                    if 'http_trusted_cert_alias' not in http_params:
                                        existing_alias = getattr(existing_ssl_opts, 'trustedcertalias', None)
                                        if existing_alias:
                                            http_params['http_trusted_cert_alias'] = existing_alias
                                if 'http_cookie_scope' not in http_params and 'http_cookie_scope' not in HTTP_UPDATE_DENYLIST:
                                    existing_cookie = _ga(existing_settings, 'cookie_scope', 'cookieScope')
                                    if existing_cookie:
                                        http_params['http_cookie_scope'] = existing_cookie
                                # Settings flags
                                if 'http_use_custom_auth' not in http_params:
                                    existing_val = _ga(existing_settings, 'use_custom_auth', 'useCustomAuth')
                                    if existing_val is not None:
                                        http_params['http_use_custom_auth'] = str(existing_val).lower()
                                if 'http_use_basic_auth' not in http_params:
                                    existing_val = _ga(existing_settings, 'use_basic_auth', 'useBasicAuth')
                                    if existing_val is not None:
                                        http_params['http_use_basic_auth'] = str(existing_val).lower()
                                if 'http_use_default_settings' not in http_params:
                                    existing_val = _ga(existing_settings, 'use_default_settings', 'useDefaultSettings')
                                    if existing_val is not None:
                                        http_params['http_use_default_settings'] = str(existing_val).lower()
                                # OAuth 1.0 settings
                                oauth1 = _ga(existing_settings, 'httpo_auth_settings', 'HTTPOAuthSettings')
                                if oauth1:
                                    if 'http_oauth1_consumer_key' not in http_params:
                                        existing_val = _ga(oauth1, 'consumer_key', 'consumerKey')
                                        if existing_val:
                                            http_params['http_oauth1_consumer_key'] = existing_val
                                    if 'http_oauth1_consumer_secret' not in http_params:
                                        existing_val = _ga(oauth1, 'consumer_secret', 'consumerSecret')
                                        if existing_val:
                                            http_params['http_oauth1_consumer_secret'] = existing_val
                                    if 'http_oauth1_access_token' not in http_params:
                                        existing_val = _ga(oauth1, 'access_token', 'accessToken')
                                        if existing_val:
                                            http_params['http_oauth1_access_token'] = existing_val
                                    if 'http_oauth1_token_secret' not in http_params:
                                        existing_val = _ga(oauth1, 'token_secret', 'tokenSecret')
                                        if existing_val:
                                            http_params['http_oauth1_token_secret'] = existing_val
                                    if 'http_oauth1_realm' not in http_params:
                                        existing_val = getattr(oauth1, 'realm', None)
                                        if existing_val:
                                            http_params['http_oauth1_realm'] = existing_val
                                    if 'http_oauth1_signature_method' not in http_params:
                                        existing_val = _ga(oauth1, 'signature_method', 'signatureMethod')
                                        if existing_val:
                                            http_params['http_oauth1_signature_method'] = existing_val
                                    if 'http_oauth1_request_token_url' not in http_params:
                                        existing_val = _ga(oauth1, 'request_token_url', 'requestTokenUrl')
                                        if existing_val:
                                            http_params['http_oauth1_request_token_url'] = existing_val
                                    if 'http_oauth1_access_token_url' not in http_params:
                                        existing_val = _ga(oauth1, 'access_token_url', 'accessTokenUrl')
                                        if existing_val:
                                            http_params['http_oauth1_access_token_url'] = existing_val
                                    if 'http_oauth1_authorization_url' not in http_params:
                                        existing_val = _ga(oauth1, 'authorization_url', 'authorizationUrl')
                                        if existing_val:
                                            http_params['http_oauth1_authorization_url'] = existing_val
                                    if 'http_oauth1_suppress_blank_access_token' not in http_params:
                                        existing_val = _ga(oauth1, 'suppress_blank_access_token', 'suppressBlankAccessToken')
                                        if existing_val is not None:
                                            http_params['http_oauth1_suppress_blank_access_token'] = str(existing_val).lower()
                                # OAuth2 settings
                                oauth = _ga(existing_settings, 'http_oauth2_settings', 'HTTPOAuth2Settings')
                                if oauth:
                                    if 'http_oauth_token_url' not in http_params:
                                        token_ep = _ga(oauth, 'access_token_endpoint', 'accessTokenEndpoint')
                                        if token_ep:
                                            existing_url = getattr(token_ep, 'url', None)
                                            if existing_url:
                                                http_params['http_oauth_token_url'] = existing_url
                                    if 'http_oauth2_authorization_token_url' not in http_params:
                                        auth_ep = _ga(oauth, 'authorization_token_endpoint', 'authorizationTokenEndpoint')
                                        if auth_ep:
                                            existing_url = getattr(auth_ep, 'url', None)
                                            if existing_url:
                                                http_params['http_oauth2_authorization_token_url'] = existing_url
                                    creds = getattr(oauth, 'credentials', None)
                                    if creds:
                                        if 'http_oauth_client_id' not in http_params:
                                            existing_val = _ga(creds, 'client_id', 'clientId')
                                            if existing_val:
                                                http_params['http_oauth_client_id'] = existing_val
                                        if 'http_oauth_client_secret' not in http_params:
                                            existing_val = _ga(creds, 'client_secret', 'clientSecret')
                                            if existing_val:
                                                http_params['http_oauth_client_secret'] = existing_val
                                        if 'http_oauth2_access_token' not in http_params:
                                            existing_val = _ga(creds, 'access_token', 'accessToken')
                                            if existing_val:
                                                http_params['http_oauth2_access_token'] = existing_val
                                        if 'http_oauth2_use_refresh_token' not in http_params:
                                            existing_val = _ga(creds, 'use_refresh_token', 'useRefreshToken')
                                            if existing_val is not None:
                                                http_params['http_oauth2_use_refresh_token'] = str(existing_val).lower()
                                    if 'http_oauth_scope' not in http_params:
                                        existing_scope = getattr(oauth, 'scope', None)
                                        if existing_scope:
                                            http_params['http_oauth_scope'] = existing_scope
                                    if 'http_oauth_grant_type' not in http_params:
                                        existing_grant = _ga(oauth, 'grant_type', 'grantType')
                                        if existing_grant:
                                            http_params['http_oauth_grant_type'] = existing_grant
                            # Preserve Listen options
                            existing_listen = _ga(existing_http, 'http_listen_options', 'HTTPListenOptions')
                            if existing_listen:
                                if 'http_listen_mime_passthrough' not in http_params:
                                    existing_val = _ga(existing_listen, 'mime_passthrough', 'mimePassthrough')
                                    if existing_val is not None:
                                        http_params['http_listen_mime_passthrough'] = str(existing_val).lower()
                                if 'http_listen_object_name' not in http_params:
                                    existing_val = _ga(existing_listen, 'object_name', 'objectName')
                                    if existing_val:
                                        http_params['http_listen_object_name'] = existing_val
                                if 'http_listen_operation_type' not in http_params:
                                    existing_val = _ga(existing_listen, 'operation_type', 'operationType')
                                    if existing_val:
                                        http_params['http_listen_operation_type'] = existing_val
                                if 'http_listen_password' not in http_params:
                                    existing_val = getattr(existing_listen, 'password', None)
                                    if existing_val:
                                        http_params['http_listen_password'] = existing_val
                                if 'http_listen_use_default' not in http_params:
                                    existing_val = _ga(existing_listen, 'use_default_listen_options', 'useDefaultListenOptions')
                                    if existing_val is not None:
                                        http_params['http_listen_use_default'] = str(existing_val).lower()
                                if 'http_listen_username' not in http_params:
                                    existing_val = getattr(existing_listen, 'username', None)
                                    if existing_val:
                                        http_params['http_listen_username'] = existing_val
                            # Helpers for serializing SDK header/element objects
                            import json as _json
                            def _serialize_headers(items):
                                """Serialize SDK Header objects using _header_to_dict."""
                                return _json.dumps([_header_to_dict(h) for h in items])
                            def _serialize_elements(items):
                                """Serialize SDK Element objects using _element_to_dict."""
                                return _json.dumps([_element_to_dict(e) for e in items])
                            # Preserve Send options headers/path elements
                            existing_send = _ga(existing_http, 'http_send_options', 'HTTPSendOptions')
                            if existing_send:
                                if 'http_request_headers' not in http_params:
                                    req_hdrs = _ga(existing_send, 'request_headers', 'requestHeaders')
                                    if req_hdrs:
                                        hdr_list = getattr(req_hdrs, 'header', None)
                                        if hdr_list:
                                            http_params['http_request_headers'] = _serialize_headers(hdr_list)
                                if 'http_response_header_mapping' not in http_params:
                                    resp_hdrs = _ga(existing_send, 'response_header_mapping', 'responseHeaderMapping')
                                    if resp_hdrs:
                                        hdr_list = getattr(resp_hdrs, 'header', None)
                                        if hdr_list:
                                            http_params['http_response_header_mapping'] = _serialize_headers(hdr_list)
                                if 'http_reflect_headers' not in http_params:
                                    reflect = _ga(existing_send, 'reflect_headers', 'reflectHeaders')
                                    if reflect:
                                        elem_list = getattr(reflect, 'element', None)
                                        if elem_list:
                                            http_params['http_reflect_headers'] = _serialize_elements(elem_list)
                                if 'http_path_elements' not in http_params:
                                    path_elems = _ga(existing_send, 'path_elements', 'pathElements')
                                    if path_elems:
                                        elem_list = getattr(path_elems, 'element', None)
                                        if elem_list:
                                            http_params['http_path_elements'] = _serialize_elements(elem_list)
                                # Preserve send-level fields (method, content, follow, profiles)
                                if 'http_method_type' not in http_params:
                                    existing_method = _ga(existing_send, 'method_type', 'methodType')
                                    if existing_method:
                                        http_params['http_method_type'] = existing_method
                                if 'http_data_content_type' not in http_params:
                                    existing_content = _ga(existing_send, 'data_content_type', 'dataContentType')
                                    if existing_content:
                                        http_params['http_data_content_type'] = existing_content
                                if 'http_follow_redirects' not in http_params:
                                    existing_follow = _ga(existing_send, 'follow_redirects', 'followRedirects')
                                    if existing_follow is not None:
                                        http_params['http_follow_redirects'] = str(existing_follow).lower()
                                if 'http_return_errors' not in http_params:
                                    existing_val = _ga(existing_send, 'return_errors', 'returnErrors')
                                    if existing_val is not None:
                                        http_params['http_return_errors'] = str(existing_val).lower()
                                if 'http_return_responses' not in http_params:
                                    existing_val = _ga(existing_send, 'return_responses', 'returnResponses')
                                    if existing_val is not None:
                                        http_params['http_return_responses'] = str(existing_val).lower()
                                if 'http_request_profile_type' not in http_params:
                                    existing_req_type = _ga(existing_send, 'request_profile_type', 'requestProfileType')
                                    if existing_req_type:
                                        http_params['http_request_profile_type'] = existing_req_type
                                if 'http_request_profile' not in http_params:
                                    req_profile = _ga(existing_send, 'request_profile', 'requestProfile')
                                    if req_profile:
                                        existing_id = _ga(req_profile, 'component_id', 'componentId')
                                        if existing_id:
                                            http_params['http_request_profile'] = existing_id
                                if 'http_response_profile_type' not in http_params:
                                    existing_resp_type = _ga(existing_send, 'response_profile_type', 'responseProfileType')
                                    if existing_resp_type:
                                        http_params['http_response_profile_type'] = existing_resp_type
                                if 'http_response_profile' not in http_params:
                                    resp_profile = _ga(existing_send, 'response_profile', 'responseProfile')
                                    if resp_profile:
                                        existing_id = _ga(resp_profile, 'component_id', 'componentId')
                                        if existing_id:
                                            http_params['http_response_profile'] = existing_id
                            # Preserve Get options (separate from send)
                            existing_get = _ga(existing_http, 'http_get_options', 'HTTPGetOptions')
                            if existing_get:
                                if 'http_get_method_type' not in http_params:
                                    existing_val = _ga(existing_get, 'method_type', 'methodType')
                                    if existing_val:
                                        http_params['http_get_method_type'] = existing_val
                                if 'http_get_content_type' not in http_params:
                                    existing_val = _ga(existing_get, 'data_content_type', 'dataContentType')
                                    if existing_val:
                                        http_params['http_get_content_type'] = existing_val
                                if 'http_get_follow_redirects' not in http_params:
                                    existing_val = _ga(existing_get, 'follow_redirects', 'followRedirects')
                                    if existing_val is not None:
                                        http_params['http_get_follow_redirects'] = str(existing_val).lower()
                                if 'http_get_return_errors' not in http_params:
                                    existing_val = _ga(existing_get, 'return_errors', 'returnErrors')
                                    if existing_val is not None:
                                        http_params['http_get_return_errors'] = str(existing_val).lower()
                                if 'http_get_request_profile' not in http_params:
                                    existing_val = _ga(existing_get, 'request_profile', 'requestProfile')
                                    if existing_val:
                                        http_params['http_get_request_profile'] = existing_val
                                if 'http_get_request_profile_type' not in http_params:
                                    existing_val = _ga(existing_get, 'request_profile_type', 'requestProfileType')
                                    if existing_val:
                                        http_params['http_get_request_profile_type'] = existing_val
                                if 'http_get_response_profile' not in http_params:
                                    existing_val = _ga(existing_get, 'response_profile', 'responseProfile')
                                    if existing_val:
                                        http_params['http_get_response_profile'] = existing_val
                                if 'http_get_response_profile_type' not in http_params:
                                    existing_val = _ga(existing_get, 'response_profile_type', 'responseProfileType')
                                    if existing_val:
                                        http_params['http_get_response_profile_type'] = existing_val
                                if 'http_get_request_headers' not in http_params:
                                    req_hdrs = _ga(existing_get, 'request_headers', 'requestHeaders')
                                    if req_hdrs:
                                        hdr_list = getattr(req_hdrs, 'header', None)
                                        if hdr_list:
                                            http_params['http_get_request_headers'] = _serialize_headers(hdr_list)
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
                                # Preserve SSH settings (nested under SFTPSSHOptions)
                                sftpssh = getattr(existing_settings, 'sftpssh_options', None)
                                if sftpssh:
                                    if 'sftp_known_host_entry' not in sftp_params:
                                        existing_known_host = _ga(sftpssh, 'known_host_entry', 'knownHostEntry')
                                        if existing_known_host:
                                            sftp_params['sftp_known_host_entry'] = existing_known_host
                                    if 'sftp_dh_key_max_1024' not in sftp_params:
                                        existing_dh = _ga(sftpssh, 'dh_key_size_max1024', 'dhKeySizeMax1024')
                                        if existing_dh is not None:
                                            sftp_params['sftp_dh_key_max_1024'] = str(existing_dh).lower()
                                    if 'sftp_ssh_key_auth' not in sftp_params:
                                        existing_ssh_auth = getattr(sftpssh, 'sshkeyauth', None)
                                        if existing_ssh_auth is not None:
                                            sftp_params['sftp_ssh_key_auth'] = str(existing_ssh_auth).lower()
                                    if 'sftp_ssh_key_path' not in sftp_params:
                                        existing_ssh_path = getattr(sftpssh, 'sshkeypath', None)
                                        if existing_ssh_path:
                                            sftp_params['sftp_ssh_key_path'] = existing_ssh_path
                                    if 'sftp_ssh_key_password' not in sftp_params:
                                        existing_ssh_pass = getattr(sftpssh, 'sshkeypassword', None)
                                        if existing_ssh_pass:
                                            sftp_params['sftp_ssh_key_password'] = existing_ssh_pass
                                # Preserve proxy settings (nested under SFTPProxySettings)
                                existing_proxy = getattr(existing_settings, 'sftp_proxy_settings', None)
                                if existing_proxy:
                                    if 'sftp_proxy_enabled' not in sftp_params:
                                        val = _ga(existing_proxy, 'proxy_enabled', 'proxyEnabled')
                                        if val is not None:
                                            sftp_params['sftp_proxy_enabled'] = str(val).lower()
                                    if 'sftp_proxy_type' not in sftp_params:
                                        val = _ga(existing_proxy, 'type_', 'type')
                                        if val:
                                            sftp_params['sftp_proxy_type'] = val
                                    if 'sftp_proxy_host' not in sftp_params:
                                        val = getattr(existing_proxy, 'host', None)
                                        if val:
                                            sftp_params['sftp_proxy_host'] = val
                                    if 'sftp_proxy_port' not in sftp_params:
                                        val = getattr(existing_proxy, 'port', None)
                                        if val:
                                            sftp_params['sftp_proxy_port'] = str(val)
                                    if 'sftp_proxy_user' not in sftp_params:
                                        val = getattr(existing_proxy, 'user', None)
                                        if val:
                                            sftp_params['sftp_proxy_user'] = val
                                    if 'sftp_proxy_password' not in sftp_params:
                                        val = getattr(existing_proxy, 'password', None)
                                        if val:
                                            sftp_params['sftp_proxy_password'] = val

                            # Preserve SFTP Get Options (download settings)
                            existing_get_opts = getattr(existing_sftp, 'sftp_get_options', None)
                            if existing_get_opts:
                                if 'sftp_remote_directory' not in sftp_params:
                                    existing_dir = getattr(existing_get_opts, 'remote_directory', None)
                                    if existing_dir:
                                        sftp_params['sftp_remote_directory'] = existing_dir
                                if 'sftp_get_action' not in sftp_params:
                                    existing_action = _ga(existing_get_opts, 'ftp_action', 'ftpAction')
                                    if existing_action:
                                        sftp_params['sftp_get_action'] = existing_action
                                if 'sftp_max_file_count' not in sftp_params:
                                    existing_count = _ga(existing_get_opts, 'max_file_count', 'maxFileCount')
                                    if existing_count:
                                        sftp_params['sftp_max_file_count'] = str(existing_count)
                                if 'sftp_file_to_move' not in sftp_params:
                                    existing_file = _ga(existing_get_opts, 'file_to_move', 'fileToMove')
                                    if existing_file:
                                        sftp_params['sftp_file_to_move'] = existing_file
                                if 'sftp_move_to_directory' not in sftp_params:
                                    existing_move_dir = _ga(existing_get_opts, 'move_to_directory', 'moveToDirectory')
                                    if existing_move_dir:
                                        sftp_params['sftp_move_to_directory'] = existing_move_dir
                                if 'sftp_move_force_override' not in sftp_params:
                                    existing_force = _ga(existing_get_opts, 'move_to_force_override', 'moveToForceOverride')
                                    if existing_force is not None:
                                        sftp_params['sftp_move_force_override'] = str(existing_force).lower()

                            # Preserve SFTP Send Options (upload settings)
                            existing_send_opts = getattr(existing_sftp, 'sftp_send_options', None)
                            if existing_send_opts:
                                if 'sftp_send_action' not in sftp_params:
                                    existing_action = _ga(existing_send_opts, 'ftp_action', 'ftpAction')
                                    if existing_action:
                                        sftp_params['sftp_send_action'] = existing_action
                                if 'sftp_send_remote_directory' not in sftp_params:
                                    existing_dir = _ga(existing_send_opts, 'remote_directory', 'remoteDirectory')
                                    if existing_dir:
                                        sftp_params['sftp_send_remote_directory'] = existing_dir
                    sftp_opts = build_sftp_communication_options(**sftp_params)
                    if sftp_opts:
                        comm_dict["SFTPCommunicationOptions"] = sftp_opts

                if ftp_params:
                    # Map alternative parameter names to builder-expected names
                    if 'ftp_passive_mode' in ftp_params and 'ftp_connection_mode' not in ftp_params:
                        ftp_params['ftp_connection_mode'] = 'passive' if str(ftp_params.pop('ftp_passive_mode')).lower() == 'true' else 'active'
                    elif 'ftp_passive_mode' in ftp_params:
                        ftp_params.pop('ftp_passive_mode')
                    if 'ftp_binary_transfer' in ftp_params and 'ftp_transfer_type' not in ftp_params:
                        ftp_params['ftp_transfer_type'] = 'binary' if str(ftp_params.pop('ftp_binary_transfer')).lower() == 'true' else 'ascii'
                    elif 'ftp_binary_transfer' in ftp_params:
                        ftp_params.pop('ftp_binary_transfer')

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
                                        ftp_params['ftp_connection_mode'] = existing_mode.value if hasattr(existing_mode, 'value') else existing_mode
                                # Preserve SSL options
                                existing_ssl = getattr(existing_settings, 'ftpssl_options', None)
                                if existing_ssl:
                                    if 'ftp_ssl_mode' not in ftp_params:
                                        existing_ssl_mode = getattr(existing_ssl, 'sslmode', None)
                                        if existing_ssl_mode:
                                            ftp_params['ftp_ssl_mode'] = existing_ssl_mode
                                    if 'ftp_client_ssl_alias' not in ftp_params:
                                        client_ssl_cert = _ga(existing_ssl, 'client_ssl_certificate', 'clientSSLCertificate')
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
                                        ftp_params['ftp_transfer_type'] = existing_type.value if hasattr(existing_type, 'value') else existing_type
                                if 'ftp_get_action' not in ftp_params:
                                    existing_action = _ga(existing_get_opts, 'ftp_action', 'ftpAction')
                                    if existing_action:
                                        ftp_params['ftp_get_action'] = existing_action
                                if 'ftp_max_file_count' not in ftp_params:
                                    existing_count = _ga(existing_get_opts, 'max_file_count', 'maxFileCount')
                                    if existing_count:
                                        ftp_params['ftp_max_file_count'] = str(existing_count)
                                if 'ftp_file_to_move' not in ftp_params:
                                    existing_file = _ga(existing_get_opts, 'file_to_move', 'fileToMove')
                                    if existing_file:
                                        ftp_params['ftp_file_to_move'] = existing_file
                                if 'ftp_move_to_directory' not in ftp_params:
                                    existing_move_dir = _ga(existing_get_opts, 'move_to_directory', 'moveToDirectory')
                                    if existing_move_dir:
                                        ftp_params['ftp_move_to_directory'] = existing_move_dir
                                if 'ftp_move_force_override' not in ftp_params:
                                    existing_force = _ga(existing_get_opts, 'move_to_force_override', 'moveToForceOverride')
                                    if existing_force is not None:
                                        ftp_params['ftp_move_force_override'] = str(existing_force).lower()

                            # Preserve FTP Send Options (upload settings)
                            existing_send_opts = getattr(existing_ftp, 'ftp_send_options', None)
                            if existing_send_opts:
                                if 'ftp_send_action' not in ftp_params:
                                    existing_action = _ga(existing_send_opts, 'ftp_action', 'ftpAction')
                                    if existing_action:
                                        ftp_params['ftp_send_action'] = existing_action
                                if 'ftp_move_to_directory' not in ftp_params:
                                    existing_move_dir = _ga(existing_send_opts, 'move_to_directory', 'moveToDirectory')
                                    if existing_move_dir:
                                        ftp_params['ftp_move_to_directory'] = existing_move_dir
                                if 'ftp_remote_directory' not in ftp_params:
                                    existing_dir = _ga(existing_send_opts, 'remote_directory', 'remoteDirectory')
                                    if existing_dir:
                                        ftp_params['ftp_remote_directory'] = existing_dir
                                if 'ftp_transfer_type' not in ftp_params:
                                    existing_type = _ga(existing_send_opts, 'transfer_type', 'transferType')
                                    if existing_type:
                                        ftp_params['ftp_transfer_type'] = existing_type.value if hasattr(existing_type, 'value') else existing_type
                                if 'ftp_send_remote_directory' not in ftp_params:
                                    existing_dir = _ga(existing_send_opts, 'remote_directory', 'remoteDirectory')
                                    if existing_dir:
                                        ftp_params['ftp_send_remote_directory'] = existing_dir
                                if 'ftp_send_transfer_type' not in ftp_params:
                                    existing_type = _ga(existing_send_opts, 'transfer_type', 'transferType')
                                    if existing_type:
                                        ftp_params['ftp_send_transfer_type'] = existing_type.value if hasattr(existing_type, 'value') else existing_type
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
                                    existing_filter = _ga(existing_get, 'file_filter', 'fileFilter')
                                    if existing_filter:
                                        disk_params['disk_file_filter'] = existing_filter
                                if 'disk_filter_match_type' not in disk_params:
                                    existing_match = _ga(existing_get, 'filter_match_type', 'filterMatchType')
                                    if existing_match:
                                        disk_params['disk_filter_match_type'] = existing_match
                                if 'disk_delete_after_read' not in disk_params:
                                    existing_delete = _ga(existing_get, 'delete_after_read', 'deleteAfterRead')
                                    if existing_delete is not None:
                                        disk_params['disk_delete_after_read'] = str(existing_delete).lower()
                                if 'disk_max_file_count' not in disk_params:
                                    existing_count = _ga(existing_get, 'max_file_count', 'maxFileCount')
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
                                    existing_create = _ga(existing_send, 'create_directory', 'createDirectory')
                                    if existing_create is not None:
                                        disk_params['disk_create_directory'] = str(existing_create).lower()
                                if 'disk_write_option' not in disk_params:
                                    existing_option = _ga(existing_send, 'write_option', 'writeOption')
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
                                    existing_timeout = _ga(existing_settings, 'send_timeout', 'sendTimeout')
                                    if existing_timeout:
                                        mllp_params['mllp_send_timeout'] = str(existing_timeout)
                                if 'mllp_receive_timeout' not in mllp_params:
                                    existing_timeout = _ga(existing_settings, 'receive_timeout', 'receiveTimeout')
                                    if existing_timeout:
                                        mllp_params['mllp_receive_timeout'] = str(existing_timeout)
                                if 'mllp_halt_timeout' not in mllp_params:
                                    existing_timeout = _ga(existing_settings, 'halt_timeout', 'haltTimeout')
                                    if existing_timeout:
                                        mllp_params['mllp_halt_timeout'] = str(existing_timeout)
                                # Connection settings
                                if 'mllp_max_connections' not in mllp_params:
                                    existing_max = _ga(existing_settings, 'max_connections', 'maxConnections')
                                    if existing_max is not None:
                                        mllp_params['mllp_max_connections'] = str(existing_max)
                                if 'mllp_max_retry' not in mllp_params:
                                    existing_retry = _ga(existing_settings, 'max_retry', 'maxRetry')
                                    if existing_retry:
                                        mllp_params['mllp_max_retry'] = existing_retry
                                if 'mllp_inactivity_timeout' not in mllp_params:
                                    existing_inactivity = _ga(existing_settings, 'inactivity_timeout', 'inactivityTimeout')
                                    if existing_inactivity:
                                        mllp_params['mllp_inactivity_timeout'] = existing_inactivity
                                # SSL settings
                                if 'mllp_use_ssl' not in mllp_params:
                                    existing_ssl = _ga(existing_settings, 'use_ssl', 'useSsl')
                                    if existing_ssl is not None:
                                        mllp_params['mllp_use_ssl'] = str(existing_ssl).lower()
                                if 'mllp_ssl_alias' not in mllp_params:
                                    ssl_cert = _ga(existing_settings, 'ssl_certificate', 'sslCertificate')
                                    if ssl_cert:
                                        existing_alias = getattr(ssl_cert, 'alias', None)
                                        if existing_alias:
                                            mllp_params['mllp_ssl_alias'] = existing_alias
                                if 'mllp_use_client_ssl' not in mllp_params:
                                    existing_client_ssl = _ga(existing_settings, 'use_client_ssl', 'useClientSsl')
                                    if existing_client_ssl is not None:
                                        mllp_params['mllp_use_client_ssl'] = str(existing_client_ssl).lower()
                                if 'mllp_client_ssl_alias' not in mllp_params:
                                    client_ssl = _ga(existing_settings, 'client_ssl_certificate', 'clientSslCertificate')
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
                            # Old partners nest under defaultOFTPConnectionSettings;
                            # new partners put fields directly in existing_settings.
                            # Check default_settings first for each field, fall back to existing_settings.
                            default_settings = _ga(existing_settings, 'default_oftp_connection_settings', 'defaultOFTPConnectionSettings') if existing_settings else None
                            def _existing_oftp_val(attr, alt_attr=None):
                                if default_settings:
                                    val = _ga(default_settings, attr, alt_attr) if alt_attr else getattr(default_settings, attr, None)
                                    if val is not None:
                                        return val
                                if existing_settings:
                                    return _ga(existing_settings, attr, alt_attr) if alt_attr else getattr(existing_settings, attr, None)
                                return None
                            if existing_settings:
                                if 'oftp_host' not in oftp_params:
                                    existing_host = _existing_oftp_val('host')
                                    if existing_host:
                                        oftp_params['oftp_host'] = existing_host
                                if 'oftp_port' not in oftp_params:
                                    existing_port = _existing_oftp_val('port')
                                    if existing_port:
                                        oftp_params['oftp_port'] = existing_port
                                if 'oftp_tls' not in oftp_params:
                                    existing_tls = _existing_oftp_val('tls')
                                    if existing_tls is not None:
                                        oftp_params['oftp_tls'] = existing_tls
                                if 'oftp_ssid_auth' not in oftp_params:
                                    existing_auth = _existing_oftp_val('ssidauth')
                                    if existing_auth is not None:
                                        oftp_params['oftp_ssid_auth'] = existing_auth
                                if 'oftp_sfid_cipher' not in oftp_params:
                                    existing_cipher = _existing_oftp_val('sfidciph')
                                    if existing_cipher is not None:
                                        oftp_params['oftp_sfid_cipher'] = existing_cipher
                                if 'oftp_use_gateway' not in oftp_params:
                                    existing_gateway = _existing_oftp_val('use_gateway')
                                    if existing_gateway is not None:
                                        oftp_params['oftp_use_gateway'] = existing_gateway
                                if 'oftp_use_client_ssl' not in oftp_params:
                                    existing_client_ssl = _existing_oftp_val('use_client_ssl')
                                    if existing_client_ssl is not None:
                                        oftp_params['oftp_use_client_ssl'] = existing_client_ssl
                                if 'oftp_client_ssl_alias' not in oftp_params:
                                    existing_alias = _existing_oftp_val('client_ssl_alias')
                                    if existing_alias:
                                        oftp_params['oftp_client_ssl_alias'] = existing_alias
                                # Get partner info - per-field fallback across both levels
                                default_partner = _ga(default_settings, 'my_partner_info', 'myPartnerInfo') if default_settings else None
                                direct_partner = _ga(existing_settings, 'my_partner_info', 'myPartnerInfo') if existing_settings else None
                                def _partner_val(attr, alt=None):
                                    for obj in (default_partner, direct_partner):
                                        if obj:
                                            val = _ga(obj, attr, alt) if alt else getattr(obj, attr, None)
                                            if val is not None:
                                                return val
                                    return None
                                if default_partner or direct_partner:
                                    if 'oftp_ssid_code' not in oftp_params:
                                        existing_code = _partner_val('ssidcode')
                                        if existing_code:
                                            oftp_params['oftp_ssid_code'] = existing_code
                                    if 'oftp_ssid_password' not in oftp_params:
                                        existing_pwd = _partner_val('ssidpswd')
                                        if existing_pwd:
                                            oftp_params['oftp_ssid_password'] = existing_pwd
                                    if 'oftp_compress' not in oftp_params:
                                        existing_compress = _partner_val('ssidcmpr')
                                        if existing_compress is not None:
                                            oftp_params['oftp_compress'] = existing_compress
                                    if 'oftp_sfid_sign' not in oftp_params:
                                        existing_sign = _partner_val('sfidsign')
                                        if existing_sign is not None:
                                            oftp_params['oftp_sfid_sign'] = existing_sign
                                    if 'oftp_sfid_encrypt' not in oftp_params:
                                        existing_encrypt = _partner_val('sfidsec_encrypt', 'sfidsec-encrypt')
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
                    if "authentication_type" in as2:
                        as2_params["as2_authentication_type"] = as2["authentication_type"]
                    if "username" in as2:
                        as2_params["as2_username"] = as2["username"]
                    if as2_params:
                        cls = updates.get('classification', None)
                        # Normalize enum to string (e.g. TradingPartnerComponentClassification.MYCOMPANY -> 'mycompany')
                        if cls and hasattr(cls, 'value'):
                            cls = cls.value
                        if not cls:
                            raw_cls = getattr(existing_tp, 'classification', None)
                            cls = raw_cls.value if hasattr(raw_cls, 'value') else raw_cls
                        if cls:
                            as2_params['classification'] = cls
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
                # Empty custom partner info: None, empty dict, dict with only @type,
                # or a CustomPartnerInfo model with no meaningful attributes set
                is_empty = (
                    custom_pi is None
                    or custom_pi == {}
                    or (isinstance(custom_pi, dict) and set(custom_pi.keys()) <= {'@type'})
                    or (hasattr(custom_pi, '__dict__') and not any(
                        v for k, v in vars(custom_pi).items()
                        if not k.startswith('_')
                    ))
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