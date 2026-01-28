#!/usr/bin/env python3
"""
Boomi MCP Server - LOCAL DEVELOPMENT VERSION

This is a simplified version for local testing without OAuth or Docker.
Use this for fast iteration when developing new MCP tools.

Features:
- No OAuth authentication (for local testing only)
- Local file-based credential storage (~/.boomi_mcp_local_secrets.json)
- No Docker build required
- Fast startup for rapid development

NOT FOR PRODUCTION USE - Use server.py with OAuth for production.
"""

import os
import sys
from typing import Dict
from pathlib import Path

from fastmcp import FastMCP

# --- Add boomi-python to path ---
boomi_python_path = Path(__file__).parent.parent / "boomi-python" / "src"
if boomi_python_path.exists():
    sys.path.insert(0, str(boomi_python_path))

# --- Add src to path for local_secrets ---
src_path = Path(__file__).parent / "src"
if src_path.exists():
    sys.path.insert(0, str(src_path))

try:
    from boomi import Boomi
except ImportError as e:
    print(f"ERROR: Failed to import Boomi SDK: {e}")
    print(f"       Boomi-python path: {boomi_python_path}")
    print(f"       Run: pip install git+https://github.com/RenEra-ai/boomi-python.git")
    sys.exit(1)

# --- Local Secrets Storage ---
try:
    from boomi_mcp.local_secrets import LocalSecretsBackend
    secrets_backend = LocalSecretsBackend()
    print(f"[INFO] Using local file-based secrets storage")
    print(f"[INFO] Storage file: {secrets_backend.storage_file}")
except ImportError as e:
    print(f"ERROR: Failed to import local_secrets: {e}")
    print(f"       Make sure src/boomi_mcp/local_secrets.py exists")
    sys.exit(1)

# --- Trading Partner Tools ---
try:
    from boomi_mcp.categories.components.trading_partners import manage_trading_partner_action
    print(f"[INFO] Trading partner tools loaded successfully")
except ImportError as e:
    print(f"[WARNING] Failed to import trading_partner_tools: {e}")
    manage_trading_partner_action = None

# --- Process Tools ---
try:
    from boomi_mcp.categories.components.processes import manage_process_action
    print(f"[INFO] Process tools loaded successfully")
except ImportError as e:
    print(f"[WARNING] Failed to import process_tools: {e}")
    manage_process_action = None

# --- Organization Tools ---
try:
    from boomi_mcp.categories.components.organizations import manage_organization_action
    print(f"[INFO] Organization tools loaded successfully")
except ImportError as e:
    print(f"[WARNING] Failed to import organization_tools: {e}")
    manage_organization_action = None


def put_secret(subject: str, profile: str, payload: Dict[str, str]):
    """Store credentials for a user profile."""
    secrets_backend.put_secret(subject, profile, payload)
    print(f"[INFO] Stored credentials for {subject}:{profile} (username: {payload.get('username', '')[:10]}***)")


def get_secret(subject: str, profile: str) -> Dict[str, str]:
    """Retrieve credentials for a user profile."""
    return secrets_backend.get_secret(subject, profile)


def list_profiles(subject: str):
    """List all profiles for a user."""
    return secrets_backend.list_profiles(subject)


def delete_profile(subject: str, profile: str):
    """Delete a user profile."""
    secrets_backend.delete_profile(subject, profile)


# --- Create FastMCP server WITHOUT authentication (local dev only) ---
mcp = FastMCP(
    name="Boomi MCP Server (Local Dev)"
)

# Hardcoded test user for local development
# In production, this comes from OAuth
TEST_USER = "local-dev-user"


# --- Tools ---

@mcp.tool()
def list_boomi_profiles():
    """
    List all saved Boomi credential profiles for the local test user.

    Returns a list of profile names that can be used with boomi_account_info().
    Use this tool first to see which profiles are available before requesting account info.

    Returns:
        List of profile objects with 'profile' name and metadata
    """
    try:
        subject = TEST_USER
        print(f"[INFO] list_boomi_profiles called for local user: {subject}")

        profiles = list_profiles(subject)
        print(f"[INFO] Found {len(profiles)} profiles for {subject}")

        if not profiles:
            return {
                "_success": True,
                "profiles": [],
                "message": "No profiles found. Use set_boomi_credentials tool to add credentials.",
                "_note": "This is the local development version"
            }

        return {
            "_success": True,
            "profiles": [p["profile"] for p in profiles],
            "count": len(profiles),
            "_note": "This is the local development version"
        }
    except Exception as e:
        print(f"[ERROR] Failed to list profiles: {e}")
        return {
            "_success": False,
            "error": f"Failed to list profiles: {str(e)}"
        }


@mcp.tool()
def set_boomi_credentials(
    profile: str,
    account_id: str,
    username: str,
    password: str
):
    """
    Store Boomi API credentials for local testing.

    This tool is only available in the local development version.
    In production, credentials are managed via the web UI.

    Args:
        profile: Profile name (e.g., 'production', 'sandbox', 'dev')
        account_id: Boomi account ID
        username: Boomi API username (should start with BOOMI_TOKEN.)
        password: Boomi API password/token

    Returns:
        Success confirmation or error details
    """
    try:
        subject = TEST_USER
        print(f"[INFO] set_boomi_credentials called for profile: {profile}")

        # Validate credentials by making a test API call
        try:
            test_sdk = Boomi(
                account_id=account_id,
                username=username,
                password=password,
                timeout=10000,
            )
            test_sdk.account.get_account(id_=account_id)
            print(f"[INFO] Credentials validated successfully for {account_id}")
        except Exception as e:
            print(f"[ERROR] Credential validation failed: {e}")
            return {
                "_success": False,
                "error": f"Credential validation failed: {str(e)}",
                "_note": "Please check your account_id, username, and password"
            }

        # Store credentials
        put_secret(subject, profile, {
            "username": username,
            "password": password,
            "account_id": account_id,
        })

        return {
            "_success": True,
            "message": f"Credentials saved for profile '{profile}'",
            "profile": profile,
            "_note": "Credentials stored locally in ~/.boomi_mcp_local_secrets.json"
        }
    except Exception as e:
        print(f"[ERROR] Failed to set credentials: {e}")
        return {
            "_success": False,
            "error": str(e)
        }


@mcp.tool()
def delete_boomi_profile(profile: str):
    """
    Delete a stored Boomi credential profile.

    This tool is only available in the local development version.
    In production, profiles are managed via the web UI.

    Args:
        profile: Profile name to delete

    Returns:
        Success confirmation or error details
    """
    try:
        subject = TEST_USER
        print(f"[INFO] delete_boomi_profile called for profile: {profile}")

        delete_profile(subject, profile)

        return {
            "_success": True,
            "message": f"Profile '{profile}' deleted successfully",
            "_note": "This is the local development version"
        }
    except Exception as e:
        print(f"[ERROR] Failed to delete profile: {e}")
        return {
            "_success": False,
            "error": str(e)
        }


@mcp.tool()
def boomi_account_info(profile: str):
    """
    Get Boomi account information from a specific profile.

    MULTI-ACCOUNT SUPPORT:
    - Users can store multiple Boomi accounts (unlimited in local dev)
    - Each profile has a unique name (e.g., 'production', 'sandbox', 'dev')
    - Profile name is REQUIRED - there is no default profile
    - If user has multiple profiles, ASK which one to use for the task
    - Once user specifies a profile, continue using it for subsequent calls
    - Don't repeatedly ask if already working with a selected profile

    WORKFLOW:
    1. First call: Use list_boomi_profiles to see available profiles
    2. If multiple profiles exist, ask user which one to use
    3. If only one profile exists, use that one
    4. Use the selected profile for all subsequent Boomi API calls in this conversation
    5. Only ask again if user explicitly wants to switch accounts

    LOCAL DEV VERSION:
    - Store credentials using set_boomi_credentials tool
    - No web UI available in local dev mode
    - Credentials stored in ~/.boomi_mcp_local_secrets.json

    Args:
        profile: Profile name to use (REQUIRED - no default)

    Returns:
        Account information including name, status, licensing details, or error details
    """
    try:
        subject = TEST_USER
        print(f"[INFO] boomi_account_info called for profile: {profile}")
    except Exception as e:
        print(f"[ERROR] Failed to get user subject: {e}")
        return {
            "_success": False,
            "error": f"Failed: {str(e)}"
        }

    # Try to get stored credentials
    try:
        creds = get_secret(subject, profile)
        print(f"[INFO] Successfully retrieved stored credentials for profile: {profile}")
        print(f"[INFO] Account ID: {creds.get('account_id')}, Username: {creds.get('username', '')[:20]}...")
    except ValueError as e:
        print(f"[ERROR] Profile '{profile}' not found: {e}")

        # List available profiles
        available_profiles = list_profiles(subject)
        print(f"[INFO] Available profiles: {[p['profile'] for p in available_profiles]}")

        return {
            "_success": False,
            "error": f"Profile '{profile}' not found. Use set_boomi_credentials to add credentials.",
            "available_profiles": [p["profile"] for p in available_profiles],
            "_note": "Use set_boomi_credentials tool to store credentials for this profile"
        }
    except Exception as e:
        print(f"[ERROR] Unexpected error retrieving credentials: {e}")
        return {
            "_success": False,
            "error": f"Failed to retrieve credentials: {str(e)}"
        }

    print(f"[INFO] Calling Boomi API for profile: {profile} (account: {creds['account_id']})")

    # Initialize Boomi SDK
    try:
        sdk_params = {
            "account_id": creds["account_id"],
            "username": creds["username"],
            "password": creds["password"],
            "timeout": 30000,  # 30 seconds
        }

        # Only add base_url if explicitly provided
        if creds.get("base_url"):
            sdk_params["base_url"] = creds["base_url"]

        sdk = Boomi(**sdk_params)

        # Call API
        result = sdk.account.get_account(id_=creds["account_id"])

        # Convert to plain dict for transport
        if hasattr(result, "__dict__"):
            out = {
                k: v for k, v in result.__dict__.items()
                if not k.startswith("_") and v is not None
            }
            out["_success"] = True
            out["_note"] = "Account data retrieved successfully (local dev version)"
            print(f"[INFO] Successfully retrieved account info for {creds['account_id']}")
            return out

        return {
            "_success": True,
            "message": "Account object created; minimal data returned.",
            "_note": "This indicates successful authentication (local dev version)."
        }

    except Exception as e:
        print(f"[ERROR] Boomi API call failed: {e}")
        return {
            "_success": False,
            "error": str(e),
            "account_id": creds["account_id"],
            "_note": "Check credentials and API access permissions"
        }


# --- Trading Partner MCP Tools (Local) ---
if manage_trading_partner_action:
    @mcp.tool()
    def manage_trading_partner(
        profile: str,
        action: str,
        partner_id: str = None,
        component_name: str = None,
        standard: str = None,
        classification: str = None,
        folder_name: str = None,
        isa_id: str = None,
        isa_qualifier: str = None,
        gs_id: str = None,
        contact_name: str = None,
        contact_email: str = None,
        contact_phone: str = None,
        contact_fax: str = None,
        contact_address: str = None,
        contact_address2: str = None,
        contact_city: str = None,
        contact_state: str = None,
        contact_country: str = None,
        contact_postalcode: str = None,
        communication_protocols: str = None,
        disk_directory: str = None,
        disk_get_directory: str = None,
        disk_send_directory: str = None,
        disk_file_filter: str = None,
        disk_filter_match_type: str = None,
        disk_delete_after_read: str = None,
        disk_max_file_count: str = None,
        disk_create_directory: str = None,
        disk_write_option: str = None,
        ftp_host: str = None,
        ftp_port: str = None,
        ftp_username: str = None,
        ftp_password: str = None,
        ftp_remote_directory: str = None,
        ftp_ssl_mode: str = None,
        ftp_connection_mode: str = None,
        ftp_transfer_type: str = None,
        ftp_get_action: str = None,
        ftp_send_action: str = None,
        ftp_max_file_count: str = None,
        ftp_file_to_move: str = None,
        ftp_move_to_directory: str = None,
        ftp_client_ssl_alias: str = None,
        sftp_host: str = None,
        sftp_port: str = None,
        sftp_username: str = None,
        sftp_password: str = None,
        sftp_remote_directory: str = None,
        http_url: str = None,
        http_password: str = None,
        as2_url: str = None,
        as2_identifier: str = None,
        as2_partner_identifier: str = None,
        as2_password: str = None,
        oftp_host: str = None,
        oftp_tls: str = None,
        http_authentication_type: str = None,
        http_connect_timeout: str = None,
        http_read_timeout: str = None,
        http_username: str = None,
        http_client_auth: str = None,
        http_trust_server_cert: str = None,
        http_method_type: str = None,
        http_data_content_type: str = None,
        http_follow_redirects: str = None,
        http_return_errors: str = None,
        http_return_responses: str = None,
        http_cookie_scope: str = None,
        http_client_ssl_alias: str = None,
        http_trusted_cert_alias: str = None,
        http_request_profile: str = None,
        http_request_profile_type: str = None,
        http_response_profile: str = None,
        http_response_profile_type: str = None,
        http_oauth_token_url: str = None,
        http_oauth_client_id: str = None,
        http_oauth_client_secret: str = None,
        http_oauth_scope: str = None,
        as2_authentication_type: str = None,
        as2_verify_hostname: str = None,
        as2_client_ssl_alias: str = None,
        as2_username: str = None,
        as2_encrypt_alias: str = None,
        as2_sign_alias: str = None,
        as2_mdn_alias: str = None,
        as2_signed: str = None,
        as2_encrypted: str = None,
        as2_compressed: str = None,
        as2_encryption_algorithm: str = None,
        as2_signing_digest_alg: str = None,
        as2_data_content_type: str = None,
        as2_request_mdn: str = None,
        as2_mdn_signed: str = None,
        as2_mdn_digest_alg: str = None,
        as2_synchronous_mdn: str = None,
        as2_fail_on_negative_mdn: str = None,
        as2_subject: str = None,
        as2_multiple_attachments: str = None,
        as2_max_document_count: int = None,
        as2_attachment_option: str = None,
        as2_attachment_cache: str = None,
        as2_mdn_external_url: str = None,
        as2_mdn_use_external_url: str = None,
        as2_mdn_use_ssl: str = None,
        as2_mdn_client_ssl_cert: str = None,
        as2_mdn_ssl_cert: str = None,
        as2_reject_duplicates: str = None,
        as2_duplicate_check_count: int = None,
        as2_legacy_smime: str = None,
        # New standard-specific fields
        edifact_interchange_id: str = None,
        edifact_interchange_id_qual: str = None,
        edifact_syntax_id: str = None,
        edifact_syntax_version: str = None,
        edifact_test_indicator: str = None,
        hl7_sending_application: str = None,
        hl7_sending_facility: str = None,
        hl7_receiving_application: str = None,
        hl7_receiving_facility: str = None,
        rosettanet_partner_id: str = None,
        rosettanet_partner_location: str = None,
        rosettanet_global_usage_code: str = None,
        rosettanet_supply_chain_code: str = None,
        rosettanet_classification_code: str = None,
        tradacoms_interchange_id: str = None,
        tradacoms_interchange_id_qualifier: str = None,
        odette_interchange_id: str = None,
        odette_interchange_id_qual: str = None,
        odette_syntax_id: str = None,
        odette_syntax_version: str = None,
        odette_test_indicator: str = None,
        # New protocol-specific fields (FTP params moved to main FTP section above)
        sftp_ssh_key_auth: str = None,
        sftp_known_host_entry: str = None,
        sftp_ssh_key_path: str = None,
        sftp_ssh_key_password: str = None,
        sftp_dh_key_max_1024: str = None,
        sftp_get_action: str = None,
        sftp_send_action: str = None,
        sftp_max_file_count: str = None,
        sftp_file_to_move: str = None,
        sftp_move_to_directory: str = None,
        sftp_move_force_override: str = None,
        sftp_proxy_enabled: str = None,
        sftp_proxy_host: str = None,
        sftp_proxy_port: str = None,
        sftp_proxy_user: str = None,
        sftp_proxy_password: str = None,
        sftp_proxy_type: str = None,
        mllp_host: str = None,
        mllp_port: str = None,
        mllp_use_ssl: str = None,
        mllp_persistent: str = None,
        mllp_receive_timeout: str = None,
        mllp_send_timeout: str = None,
        mllp_max_connections: str = None,
        mllp_inactivity_timeout: int = None,
        mllp_max_retry: int = None,
        mllp_halt_timeout: str = None,
        mllp_use_client_ssl: str = None,
        mllp_client_ssl_alias: str = None,
        mllp_ssl_alias: str = None,
        oftp_port: str = None,
        oftp_ssid_code: str = None,
        oftp_ssid_password: str = None,
        oftp_compress: str = None,
        oftp_ssid_auth: str = None,
        oftp_sfid_cipher: int = None,
        oftp_use_gateway: str = None,
        oftp_use_client_ssl: str = None,
        oftp_client_ssl_alias: str = None,
        oftp_sfid_sign: str = None,
        oftp_sfid_encrypt: str = None,
        # Organization linking
        organization_id: str = None
    ):
        """
        Manage B2B/EDI trading partners (all 7 standards).

        Consolidated tool for all trading partner operations (local dev mode).
        Now uses JSON-based TradingPartnerComponent API for cleaner, type-safe operations.

        Args:
            profile: Boomi profile name (required)
            action: Action to perform - must be one of: list, get, create, update, delete, analyze_usage
            partner_id: Trading partner component ID (required for get, update, delete, analyze_usage)
            component_name: Trading partner name (required for create, optional for update)
            standard: Trading standard (required for create, optional filter for list)
                      Options: x12, edifact, hl7, rosettanet, custom, tradacoms, odette
            classification: Partner classification (optional for create/list)
                           Options: tradingpartner, mycompany
            folder_name: Folder to place partner in (optional for create/list)

            # Standard-specific fields (X12, EDIFACT, HL7, RosettaNet, TRADACOMS, ODETTE)
            isa_id: ISA ID for X12 partners (X12 only)
            isa_qualifier: ISA Qualifier for X12 partners (X12 only)
            gs_id: GS ID for X12 partners (X12 only)

            # Contact Information (10 fields)
            contact_name: Contact person name (optional)
            contact_email: Contact email address (optional)
            contact_phone: Contact phone number (optional)
            contact_fax: Contact fax number (optional)
            contact_address: Contact street address line 1 (optional)
            contact_address2: Contact street address line 2 (optional)
            contact_city: Contact city (optional)
            contact_state: Contact state/province (optional)
            contact_country: Contact country (optional)
            contact_postalcode: Contact postal/zip code (optional)

            # Communication Protocols
            communication_protocols: Comma-separated list of communication protocols to enable (optional for create)
                                    Available: ftp, sftp, http, as2, mllp, oftp, disk
                                    Example: "ftp,http" or "as2,sftp"
                                    If not provided, creates partner with no communication configured

            # Protocol-specific fields (Note: Full protocol support coming in future update)
            # DISK Protocol
            disk_directory: Main directory for Disk protocol
            disk_get_directory: Get/Receive directory for Disk protocol
            disk_send_directory: Send directory for Disk protocol
            disk_file_filter: File filter pattern (default: *)
            disk_filter_match_type: Filter type - wildcard or regex (default: wildcard)
            disk_delete_after_read: Delete files after reading - "true" or "false"
            disk_max_file_count: Maximum files to retrieve per poll
            disk_create_directory: Create directory if not exists - "true" or "false"
            disk_write_option: Write option - unique, over, append, abort (default: unique)
            # FTP Protocol
            ftp_host: FTP server hostname/IP
            ftp_port: FTP server port
            ftp_username: FTP username
            ftp_get_action: Get action - actionget, actiongetdelete, actiongetmove
            ftp_send_action: Send action - actionputrename, actionputappend, actionputerror, actionputoverwrite
            ftp_connection_mode: Connection mode - passive, active
            ftp_ssl_mode: SSL mode - none, implicit, explicit
            # SFTP Protocol
            sftp_host: SFTP server hostname/IP
            sftp_port: SFTP server port
            sftp_username: SFTP username
            sftp_get_action: Get action - actionget, actiongetdelete, actiongetmove
            sftp_send_action: Send action - actionputrename, actionputappend, actionputerror, actionputoverwrite
            # HTTP Protocol
            http_url: HTTP/HTTPS URL
            # AS2 Protocol
            as2_url: AS2 endpoint URL
            as2_identifier: Local AS2 identifier
            as2_partner_identifier: Partner AS2 identifier
            # OFTP Protocol
            oftp_host: OFTP server hostname/IP
            oftp_tls: Enable TLS for OFTP - "true" or "false"
            oftp_ssid_auth: Enable SSID authentication - "true" or "false"
            oftp_sfid_cipher: SFID cipher strength (0=none, 1=3DES, 2=AES-128, 3=AES-192, 4=AES-256)
            oftp_use_gateway: Use OFTP gateway - "true" or "false"
            oftp_use_client_ssl: Use client SSL certificate - "true" or "false"
            oftp_client_ssl_alias: Client SSL certificate alias
            oftp_sfid_sign: Sign files - "true" or "false"
            oftp_sfid_encrypt: Encrypt files - "true" or "false"
            http_authentication_type: HTTP authentication type - NONE, BASIC, OAUTH2
            http_connect_timeout: HTTP connection timeout in ms
            http_read_timeout: HTTP read timeout in ms
            http_username: HTTP username
            http_client_auth: Enable client SSL authentication - "true" or "false"
            http_trust_server_cert: Trust server certificate - "true" or "false"
            http_method_type: HTTP method - GET, POST, PUT, DELETE, PATCH
            http_data_content_type: HTTP content type
            http_follow_redirects: Follow redirects - "true" or "false"
            http_return_errors: Return errors in response - "true" or "false"
            http_return_responses: Return response body - "true" or "false"
            http_cookie_scope: Cookie handling - IGNORED, GLOBAL, CONNECTOR_SHAPE
            http_client_ssl_alias: Client SSL certificate alias
            http_trusted_cert_alias: Trusted server certificate alias
            http_request_profile: Request profile component ID
            http_request_profile_type: Request profile type - NONE, XML, JSON
            http_response_profile: Response profile component ID
            http_response_profile_type: Response profile type - NONE, XML, JSON
            http_oauth_token_url: OAuth2 token endpoint URL
            http_oauth_client_id: OAuth2 client ID
            http_oauth_client_secret: OAuth2 client secret
            http_oauth_scope: OAuth2 scope
            as2_authentication_type: AS2 authentication type - NONE, BASIC
            as2_verify_hostname: Verify SSL hostname - "true" or "false"
            as2_client_ssl_alias: Client SSL certificate alias
            as2_username: AS2 username
            as2_encrypt_alias: AS2 encryption certificate alias
            as2_sign_alias: AS2 signing certificate alias
            as2_mdn_alias: AS2 MDN certificate alias
            as2_signed: Sign AS2 messages - "true" or "false"
            as2_encrypted: Encrypt AS2 messages - "true" or "false"
            as2_compressed: Compress AS2 messages - "true" or "false"
            as2_encryption_algorithm: Encryption algorithm - tripledes, rc2, aes128, aes192, aes256
            as2_signing_digest_alg: Signing digest algorithm - SHA1, SHA256, SHA384, SHA512
            as2_data_content_type: AS2 content type - textplain, binary, edifact, edix12, applicationxml, textxml
            as2_request_mdn: Request MDN - "true" or "false"
            as2_mdn_signed: Signed MDN - "true" or "false"
            as2_mdn_digest_alg: MDN digest algorithm - SHA1, SHA256, SHA384, SHA512
            as2_synchronous_mdn: Synchronous MDN - "true" or "false"
            as2_fail_on_negative_mdn: Fail on negative MDN - "true" or "false"
            as2_subject: AS2 message subject header
            as2_multiple_attachments: Enable multiple attachments - "true" or "false"
            as2_max_document_count: Maximum documents per message
            as2_attachment_option: Attachment handling - BATCH, DOCUMENT_CACHE
            as2_attachment_cache: Attachment cache component ID
            as2_mdn_external_url: External URL for async MDN delivery
            as2_mdn_use_external_url: Use external URL for MDN - "true" or "false"
            as2_mdn_use_ssl: Use SSL for MDN delivery - "true" or "false"
            as2_mdn_client_ssl_cert: Client SSL certificate alias for MDN
            as2_mdn_ssl_cert: Server SSL certificate alias for MDN
            as2_reject_duplicates: Reject duplicate messages - "true" or "false"
            as2_duplicate_check_count: Number of messages to check for duplicates
            as2_legacy_smime: Enable legacy S/MIME compatibility - "true" or "false"

        Returns:
            Action result with success status and data/error

        Implementation Note:
            This tool now uses JSON-based TradingPartnerComponent models internally,
            providing better type safety and maintainability compared to the previous
            XML-based approach. Protocol-specific and standard-specific field support
            is currently limited to basic fields and will be expanded in future updates.
        """
        try:
            subject = TEST_USER
            print(f"[INFO] manage_trading_partner called for local user: {subject}, profile: {profile}, action: {action}")

            # Get credentials
            creds = get_secret(subject, profile)

            # Initialize Boomi SDK
            sdk = Boomi(
                account_id=creds["account_id"],
                username=creds["username"],
                password=creds["password"]
            )

            # Build parameters based on action
            params = {}

            if action == "list":
                filters = {}
                if standard:
                    filters["standard"] = standard
                if classification:
                    filters["classification"] = classification
                if folder_name:
                    filters["folder_name"] = folder_name
                params["filters"] = filters

            elif action == "get":
                params["partner_id"] = partner_id

            elif action == "create":
                request_data = {}
                if component_name:
                    request_data["component_name"] = component_name
                if standard:
                    request_data["standard"] = standard
                if classification:
                    request_data["classification"] = classification
                if folder_name:
                    request_data["folder_name"] = folder_name

                # Pass X12 fields flat (builder expects flat kwargs, not nested)
                if isa_id:
                    request_data["isa_id"] = isa_id
                if isa_qualifier:
                    request_data["isa_qualifier"] = isa_qualifier
                if gs_id:
                    request_data["gs_id"] = gs_id

                # Pass contact fields flat (builder expects flat kwargs)
                if contact_name:
                    request_data["contact_name"] = contact_name
                if contact_email:
                    request_data["contact_email"] = contact_email
                if contact_phone:
                    request_data["contact_phone"] = contact_phone
                if contact_fax:
                    request_data["contact_fax"] = contact_fax
                if contact_address:
                    request_data["contact_address"] = contact_address
                if contact_address2:
                    request_data["contact_address2"] = contact_address2
                if contact_city:
                    request_data["contact_city"] = contact_city
                if contact_state:
                    request_data["contact_state"] = contact_state
                if contact_country:
                    request_data["contact_country"] = contact_country
                if contact_postalcode:
                    request_data["contact_postalcode"] = contact_postalcode

                # Add communication protocols if provided
                if communication_protocols:
                    protocols_list = [p.strip() for p in communication_protocols.split(',')]
                    request_data["communication_protocols"] = protocols_list

                # Pass disk fields flat
                if disk_directory:
                    request_data["disk_directory"] = disk_directory
                if disk_get_directory:
                    request_data["disk_get_directory"] = disk_get_directory
                if disk_send_directory:
                    request_data["disk_send_directory"] = disk_send_directory
                if disk_file_filter:
                    request_data["disk_file_filter"] = disk_file_filter
                if disk_filter_match_type:
                    request_data["disk_filter_match_type"] = disk_filter_match_type
                if disk_delete_after_read:
                    request_data["disk_delete_after_read"] = disk_delete_after_read
                if disk_max_file_count:
                    request_data["disk_max_file_count"] = disk_max_file_count
                if disk_create_directory:
                    request_data["disk_create_directory"] = disk_create_directory
                if disk_write_option:
                    request_data["disk_write_option"] = disk_write_option

                # Pass FTP fields flat
                if ftp_host:
                    request_data["ftp_host"] = ftp_host
                if ftp_port:
                    request_data["ftp_port"] = ftp_port
                if ftp_username:
                    request_data["ftp_username"] = ftp_username
                if ftp_password:
                    request_data["ftp_password"] = ftp_password
                if ftp_remote_directory:
                    request_data["ftp_remote_directory"] = ftp_remote_directory

                # Pass SFTP fields flat
                if sftp_host:
                    request_data["sftp_host"] = sftp_host
                if sftp_port:
                    request_data["sftp_port"] = sftp_port
                if sftp_username:
                    request_data["sftp_username"] = sftp_username
                if sftp_password:
                    request_data["sftp_password"] = sftp_password
                if sftp_remote_directory:
                    request_data["sftp_remote_directory"] = sftp_remote_directory

                # Pass HTTP fields flat
                if http_url:
                    request_data["http_url"] = http_url
                if http_username:
                    request_data["http_username"] = http_username
                if http_password:
                    request_data["http_password"] = http_password
                if http_connect_timeout:
                    request_data["http_connect_timeout"] = http_connect_timeout
                if http_read_timeout:
                    request_data["http_read_timeout"] = http_read_timeout

                # Pass AS2 fields flat
                if as2_url:
                    request_data["as2_url"] = as2_url
                if as2_username:
                    request_data["as2_username"] = as2_username
                if as2_password:
                    request_data["as2_password"] = as2_password
                if as2_identifier:
                    request_data["as2_identifier"] = as2_identifier
                if as2_partner_identifier:
                    request_data["as2_partner_identifier"] = as2_partner_identifier
                if as2_signed:
                    request_data["as2_signed"] = as2_signed
                if as2_encrypted:
                    request_data["as2_encrypted"] = as2_encrypted
                if as2_compressed:
                    request_data["as2_compressed"] = as2_compressed
                if as2_encryption_algorithm:
                    request_data["as2_encryption_algorithm"] = as2_encryption_algorithm
                if as2_signing_digest_alg:
                    request_data["as2_signing_digest_alg"] = as2_signing_digest_alg
                if as2_request_mdn:
                    request_data["as2_request_mdn"] = as2_request_mdn
                if as2_mdn_signed:
                    request_data["as2_mdn_signed"] = as2_mdn_signed
                if as2_synchronous_mdn:
                    request_data["as2_synchronous_mdn"] = as2_synchronous_mdn
                if as2_authentication_type:
                    request_data["as2_authentication_type"] = as2_authentication_type
                if as2_verify_hostname:
                    request_data["as2_verify_hostname"] = as2_verify_hostname
                if as2_client_ssl_alias:
                    request_data["as2_client_ssl_alias"] = as2_client_ssl_alias
                if as2_encrypt_alias:
                    request_data["as2_encrypt_alias"] = as2_encrypt_alias
                if as2_sign_alias:
                    request_data["as2_sign_alias"] = as2_sign_alias
                if as2_mdn_alias:
                    request_data["as2_mdn_alias"] = as2_mdn_alias
                if as2_data_content_type:
                    request_data["as2_data_content_type"] = as2_data_content_type
                if as2_mdn_digest_alg:
                    request_data["as2_mdn_digest_alg"] = as2_mdn_digest_alg
                if as2_fail_on_negative_mdn:
                    request_data["as2_fail_on_negative_mdn"] = as2_fail_on_negative_mdn
                if as2_subject:
                    request_data["as2_subject"] = as2_subject
                if as2_multiple_attachments:
                    request_data["as2_multiple_attachments"] = as2_multiple_attachments
                if as2_max_document_count:
                    request_data["as2_max_document_count"] = as2_max_document_count
                if as2_attachment_option:
                    request_data["as2_attachment_option"] = as2_attachment_option
                if as2_attachment_cache:
                    request_data["as2_attachment_cache"] = as2_attachment_cache
                if as2_mdn_external_url:
                    request_data["as2_mdn_external_url"] = as2_mdn_external_url
                if as2_mdn_use_external_url:
                    request_data["as2_mdn_use_external_url"] = as2_mdn_use_external_url
                if as2_mdn_use_ssl:
                    request_data["as2_mdn_use_ssl"] = as2_mdn_use_ssl
                if as2_mdn_client_ssl_cert:
                    request_data["as2_mdn_client_ssl_cert"] = as2_mdn_client_ssl_cert
                if as2_mdn_ssl_cert:
                    request_data["as2_mdn_ssl_cert"] = as2_mdn_ssl_cert
                if as2_reject_duplicates:
                    request_data["as2_reject_duplicates"] = as2_reject_duplicates
                if as2_duplicate_check_count:
                    request_data["as2_duplicate_check_count"] = as2_duplicate_check_count
                if as2_legacy_smime:
                    request_data["as2_legacy_smime"] = as2_legacy_smime

                # Pass EDIFACT fields flat
                if edifact_interchange_id:
                    request_data["edifact_interchange_id"] = edifact_interchange_id
                if edifact_interchange_id_qual:
                    request_data["edifact_interchange_id_qual"] = edifact_interchange_id_qual
                if edifact_syntax_id:
                    request_data["edifact_syntax_id"] = edifact_syntax_id
                if edifact_syntax_version:
                    request_data["edifact_syntax_version"] = edifact_syntax_version
                if edifact_test_indicator:
                    request_data["edifact_test_indicator"] = edifact_test_indicator

                # Pass HL7 fields flat
                if hl7_sending_application:
                    request_data["hl7_sending_application"] = hl7_sending_application
                if hl7_sending_facility:
                    request_data["hl7_sending_facility"] = hl7_sending_facility
                if hl7_receiving_application:
                    request_data["hl7_receiving_application"] = hl7_receiving_application
                if hl7_receiving_facility:
                    request_data["hl7_receiving_facility"] = hl7_receiving_facility

                # Pass RosettaNet fields flat
                if rosettanet_partner_id:
                    request_data["rosettanet_partner_id"] = rosettanet_partner_id
                if rosettanet_partner_location:
                    request_data["rosettanet_partner_location"] = rosettanet_partner_location
                if rosettanet_global_usage_code:
                    request_data["rosettanet_global_usage_code"] = rosettanet_global_usage_code
                if rosettanet_supply_chain_code:
                    request_data["rosettanet_supply_chain_code"] = rosettanet_supply_chain_code
                if rosettanet_classification_code:
                    request_data["rosettanet_classification_code"] = rosettanet_classification_code

                # Pass TRADACOMS fields flat
                if tradacoms_interchange_id:
                    request_data["tradacoms_interchange_id"] = tradacoms_interchange_id
                if tradacoms_interchange_id_qualifier:
                    request_data["tradacoms_interchange_id_qualifier"] = tradacoms_interchange_id_qualifier

                # Pass ODETTE fields flat
                if odette_interchange_id:
                    request_data["odette_interchange_id"] = odette_interchange_id
                if odette_interchange_id_qual:
                    request_data["odette_interchange_id_qual"] = odette_interchange_id_qual
                if odette_syntax_id:
                    request_data["odette_syntax_id"] = odette_syntax_id
                if odette_syntax_version:
                    request_data["odette_syntax_version"] = odette_syntax_version
                if odette_test_indicator:
                    request_data["odette_test_indicator"] = odette_test_indicator

                # Pass enhanced FTP fields flat
                if ftp_ssl_mode:
                    request_data["ftp_ssl_mode"] = ftp_ssl_mode
                if ftp_connection_mode:
                    request_data["ftp_connection_mode"] = ftp_connection_mode
                if ftp_transfer_type:
                    request_data["ftp_transfer_type"] = ftp_transfer_type
                if ftp_get_action:
                    request_data["ftp_get_action"] = ftp_get_action
                if ftp_send_action:
                    request_data["ftp_send_action"] = ftp_send_action
                if ftp_max_file_count:
                    request_data["ftp_max_file_count"] = ftp_max_file_count
                if ftp_file_to_move:
                    request_data["ftp_file_to_move"] = ftp_file_to_move
                if ftp_move_to_directory:
                    request_data["ftp_move_to_directory"] = ftp_move_to_directory
                if ftp_client_ssl_alias:
                    request_data["ftp_client_ssl_alias"] = ftp_client_ssl_alias

                # Pass enhanced SFTP fields flat
                if sftp_ssh_key_auth:
                    request_data["sftp_ssh_key_auth"] = sftp_ssh_key_auth
                if sftp_known_host_entry:
                    request_data["sftp_known_host_entry"] = sftp_known_host_entry
                if sftp_ssh_key_path:
                    request_data["sftp_ssh_key_path"] = sftp_ssh_key_path
                if sftp_ssh_key_password:
                    request_data["sftp_ssh_key_password"] = sftp_ssh_key_password
                if sftp_dh_key_max_1024:
                    request_data["sftp_dh_key_max_1024"] = sftp_dh_key_max_1024
                if sftp_get_action:
                    request_data["sftp_get_action"] = sftp_get_action
                if sftp_send_action:
                    request_data["sftp_send_action"] = sftp_send_action
                if sftp_max_file_count:
                    request_data["sftp_max_file_count"] = sftp_max_file_count
                if sftp_file_to_move:
                    request_data["sftp_file_to_move"] = sftp_file_to_move
                if sftp_move_to_directory:
                    request_data["sftp_move_to_directory"] = sftp_move_to_directory
                if sftp_move_force_override:
                    request_data["sftp_move_force_override"] = sftp_move_force_override
                if sftp_proxy_enabled:
                    request_data["sftp_proxy_enabled"] = sftp_proxy_enabled
                if sftp_proxy_host:
                    request_data["sftp_proxy_host"] = sftp_proxy_host
                if sftp_proxy_port:
                    request_data["sftp_proxy_port"] = sftp_proxy_port
                if sftp_proxy_user:
                    request_data["sftp_proxy_user"] = sftp_proxy_user
                if sftp_proxy_password:
                    request_data["sftp_proxy_password"] = sftp_proxy_password
                if sftp_proxy_type:
                    request_data["sftp_proxy_type"] = sftp_proxy_type

                # Pass HTTP additional fields flat
                if http_authentication_type:
                    request_data["http_authentication_type"] = http_authentication_type
                if http_client_auth:
                    request_data["http_client_auth"] = http_client_auth
                if http_trust_server_cert:
                    request_data["http_trust_server_cert"] = http_trust_server_cert
                if http_method_type:
                    request_data["http_method_type"] = http_method_type
                if http_data_content_type:
                    request_data["http_data_content_type"] = http_data_content_type
                if http_follow_redirects:
                    request_data["http_follow_redirects"] = http_follow_redirects
                if http_return_errors:
                    request_data["http_return_errors"] = http_return_errors
                if http_return_responses:
                    request_data["http_return_responses"] = http_return_responses
                if http_cookie_scope:
                    request_data["http_cookie_scope"] = http_cookie_scope
                if http_client_ssl_alias:
                    request_data["http_client_ssl_alias"] = http_client_ssl_alias
                if http_trusted_cert_alias:
                    request_data["http_trusted_cert_alias"] = http_trusted_cert_alias
                if http_request_profile:
                    request_data["http_request_profile"] = http_request_profile
                if http_request_profile_type:
                    request_data["http_request_profile_type"] = http_request_profile_type
                if http_response_profile:
                    request_data["http_response_profile"] = http_response_profile
                if http_response_profile_type:
                    request_data["http_response_profile_type"] = http_response_profile_type
                if http_oauth_token_url:
                    request_data["http_oauth_token_url"] = http_oauth_token_url
                if http_oauth_client_id:
                    request_data["http_oauth_client_id"] = http_oauth_client_id
                if http_oauth_client_secret:
                    request_data["http_oauth_client_secret"] = http_oauth_client_secret
                if http_oauth_scope:
                    request_data["http_oauth_scope"] = http_oauth_scope

                # Pass MLLP fields flat
                if mllp_host:
                    request_data["mllp_host"] = mllp_host
                if mllp_port:
                    request_data["mllp_port"] = mllp_port
                if mllp_use_ssl:
                    request_data["mllp_use_ssl"] = mllp_use_ssl
                if mllp_persistent:
                    request_data["mllp_persistent"] = mllp_persistent
                if mllp_receive_timeout:
                    request_data["mllp_receive_timeout"] = mllp_receive_timeout
                if mllp_send_timeout:
                    request_data["mllp_send_timeout"] = mllp_send_timeout
                if mllp_max_connections:
                    request_data["mllp_max_connections"] = mllp_max_connections
                if mllp_inactivity_timeout:
                    request_data["mllp_inactivity_timeout"] = mllp_inactivity_timeout
                if mllp_max_retry:
                    request_data["mllp_max_retry"] = mllp_max_retry
                if mllp_halt_timeout:
                    request_data["mllp_halt_timeout"] = mllp_halt_timeout
                if mllp_use_client_ssl:
                    request_data["mllp_use_client_ssl"] = mllp_use_client_ssl
                if mllp_client_ssl_alias:
                    request_data["mllp_client_ssl_alias"] = mllp_client_ssl_alias
                if mllp_ssl_alias:
                    request_data["mllp_ssl_alias"] = mllp_ssl_alias

                # Pass OFTP fields flat
                if oftp_host:
                    request_data["oftp_host"] = oftp_host
                if oftp_port:
                    request_data["oftp_port"] = oftp_port
                if oftp_tls:
                    request_data["oftp_tls"] = oftp_tls
                if oftp_ssid_code:
                    request_data["oftp_ssid_code"] = oftp_ssid_code
                if oftp_ssid_password:
                    request_data["oftp_ssid_password"] = oftp_ssid_password
                if oftp_compress:
                    request_data["oftp_compress"] = oftp_compress
                if oftp_ssid_auth:
                    request_data["oftp_ssid_auth"] = oftp_ssid_auth
                if oftp_sfid_cipher:
                    request_data["oftp_sfid_cipher"] = oftp_sfid_cipher
                if oftp_use_gateway:
                    request_data["oftp_use_gateway"] = oftp_use_gateway
                if oftp_use_client_ssl:
                    request_data["oftp_use_client_ssl"] = oftp_use_client_ssl
                if oftp_client_ssl_alias:
                    request_data["oftp_client_ssl_alias"] = oftp_client_ssl_alias
                if oftp_sfid_sign:
                    request_data["oftp_sfid_sign"] = oftp_sfid_sign
                if oftp_sfid_encrypt:
                    request_data["oftp_sfid_encrypt"] = oftp_sfid_encrypt

                # Organization linking
                if organization_id:
                    request_data["organization_id"] = organization_id

                params["request_data"] = request_data

            elif action == "update":
                params["partner_id"] = partner_id
                updates = {}
                if component_name:
                    updates["component_name"] = component_name

                # Pass contact fields flat (builder expects flat kwargs)
                if contact_name:
                    updates["contact_name"] = contact_name
                if contact_email:
                    updates["contact_email"] = contact_email
                if contact_phone:
                    updates["contact_phone"] = contact_phone
                if contact_fax:
                    updates["contact_fax"] = contact_fax
                if contact_address:
                    updates["contact_address"] = contact_address
                if contact_address2:
                    updates["contact_address2"] = contact_address2
                if contact_city:
                    updates["contact_city"] = contact_city
                if contact_state:
                    updates["contact_state"] = contact_state
                if contact_country:
                    updates["contact_country"] = contact_country
                if contact_postalcode:
                    updates["contact_postalcode"] = contact_postalcode

                # Pass X12 fields flat
                if isa_id:
                    updates["isa_id"] = isa_id
                if isa_qualifier:
                    updates["isa_qualifier"] = isa_qualifier
                if gs_id:
                    updates["gs_id"] = gs_id

                # Communication protocols (flat params - same as create)
                if communication_protocols:
                    protocols_list = [p.strip() for p in communication_protocols.split(',')]
                    updates["communication_protocols"] = protocols_list

                # Disk protocol fields (flat)
                if disk_directory:
                    updates["disk_directory"] = disk_directory
                if disk_get_directory:
                    updates["disk_get_directory"] = disk_get_directory
                if disk_send_directory:
                    updates["disk_send_directory"] = disk_send_directory
                if disk_file_filter:
                    updates["disk_file_filter"] = disk_file_filter
                if disk_filter_match_type:
                    updates["disk_filter_match_type"] = disk_filter_match_type
                if disk_delete_after_read:
                    updates["disk_delete_after_read"] = disk_delete_after_read
                if disk_max_file_count:
                    updates["disk_max_file_count"] = disk_max_file_count
                if disk_create_directory:
                    updates["disk_create_directory"] = disk_create_directory
                if disk_write_option:
                    updates["disk_write_option"] = disk_write_option

                # FTP protocol fields (flat)
                if ftp_host:
                    updates["ftp_host"] = ftp_host
                if ftp_port:
                    updates["ftp_port"] = ftp_port
                if ftp_username:
                    updates["ftp_username"] = ftp_username
                if ftp_password:
                    updates["ftp_password"] = ftp_password
                if ftp_remote_directory:
                    updates["ftp_remote_directory"] = ftp_remote_directory
                if ftp_connection_mode:
                    updates["ftp_connection_mode"] = ftp_connection_mode
                if ftp_ssl_mode:
                    updates["ftp_ssl_mode"] = ftp_ssl_mode
                if ftp_transfer_type:
                    updates["ftp_transfer_type"] = ftp_transfer_type
                if ftp_get_action:
                    updates["ftp_get_action"] = ftp_get_action
                if ftp_send_action:
                    updates["ftp_send_action"] = ftp_send_action
                if ftp_max_file_count:
                    updates["ftp_max_file_count"] = ftp_max_file_count
                if ftp_file_to_move:
                    updates["ftp_file_to_move"] = ftp_file_to_move
                if ftp_move_to_directory:
                    updates["ftp_move_to_directory"] = ftp_move_to_directory
                if ftp_client_ssl_alias:
                    updates["ftp_client_ssl_alias"] = ftp_client_ssl_alias

                # SFTP protocol fields (flat)
                if sftp_host:
                    updates["sftp_host"] = sftp_host
                if sftp_port:
                    updates["sftp_port"] = sftp_port
                if sftp_username:
                    updates["sftp_username"] = sftp_username
                if sftp_password:
                    updates["sftp_password"] = sftp_password
                if sftp_remote_directory:
                    updates["sftp_remote_directory"] = sftp_remote_directory
                if sftp_ssh_key_auth:
                    updates["sftp_ssh_key_auth"] = sftp_ssh_key_auth
                if sftp_known_host_entry:
                    updates["sftp_known_host_entry"] = sftp_known_host_entry
                if sftp_ssh_key_path:
                    updates["sftp_ssh_key_path"] = sftp_ssh_key_path
                if sftp_ssh_key_password:
                    updates["sftp_ssh_key_password"] = sftp_ssh_key_password
                if sftp_dh_key_max_1024:
                    updates["sftp_dh_key_max_1024"] = sftp_dh_key_max_1024
                if sftp_get_action:
                    updates["sftp_get_action"] = sftp_get_action
                if sftp_send_action:
                    updates["sftp_send_action"] = sftp_send_action
                if sftp_max_file_count:
                    updates["sftp_max_file_count"] = sftp_max_file_count
                if sftp_file_to_move:
                    updates["sftp_file_to_move"] = sftp_file_to_move
                if sftp_move_to_directory:
                    updates["sftp_move_to_directory"] = sftp_move_to_directory
                if sftp_move_force_override:
                    updates["sftp_move_force_override"] = sftp_move_force_override
                if sftp_proxy_enabled:
                    updates["sftp_proxy_enabled"] = sftp_proxy_enabled
                if sftp_proxy_host:
                    updates["sftp_proxy_host"] = sftp_proxy_host
                if sftp_proxy_port:
                    updates["sftp_proxy_port"] = sftp_proxy_port
                if sftp_proxy_type:
                    updates["sftp_proxy_type"] = sftp_proxy_type
                if sftp_proxy_user:
                    updates["sftp_proxy_user"] = sftp_proxy_user
                if sftp_proxy_password:
                    updates["sftp_proxy_password"] = sftp_proxy_password

                # HTTP protocol fields (flat)
                if http_url:
                    updates["http_url"] = http_url
                if http_username:
                    updates["http_username"] = http_username
                if http_password:
                    updates["http_password"] = http_password
                if http_authentication_type:
                    updates["http_authentication_type"] = http_authentication_type
                if http_connect_timeout:
                    updates["http_connect_timeout"] = http_connect_timeout
                if http_read_timeout:
                    updates["http_read_timeout"] = http_read_timeout
                if http_client_auth:
                    updates["http_client_auth"] = http_client_auth
                if http_trust_server_cert:
                    updates["http_trust_server_cert"] = http_trust_server_cert
                if http_method_type:
                    updates["http_method_type"] = http_method_type
                if http_data_content_type:
                    updates["http_data_content_type"] = http_data_content_type
                if http_follow_redirects:
                    updates["http_follow_redirects"] = http_follow_redirects
                if http_return_errors:
                    updates["http_return_errors"] = http_return_errors
                if http_return_responses:
                    updates["http_return_responses"] = http_return_responses
                if http_cookie_scope:
                    updates["http_cookie_scope"] = http_cookie_scope
                if http_client_ssl_alias:
                    updates["http_client_ssl_alias"] = http_client_ssl_alias
                if http_trusted_cert_alias:
                    updates["http_trusted_cert_alias"] = http_trusted_cert_alias
                if http_request_profile:
                    updates["http_request_profile"] = http_request_profile
                if http_request_profile_type:
                    updates["http_request_profile_type"] = http_request_profile_type
                if http_response_profile:
                    updates["http_response_profile"] = http_response_profile
                if http_response_profile_type:
                    updates["http_response_profile_type"] = http_response_profile_type
                if http_oauth_token_url:
                    updates["http_oauth_token_url"] = http_oauth_token_url
                if http_oauth_client_id:
                    updates["http_oauth_client_id"] = http_oauth_client_id
                if http_oauth_client_secret:
                    updates["http_oauth_client_secret"] = http_oauth_client_secret
                if http_oauth_scope:
                    updates["http_oauth_scope"] = http_oauth_scope

                # AS2 protocol fields (flat)
                if as2_url:
                    updates["as2_url"] = as2_url
                if as2_identifier:
                    updates["as2_identifier"] = as2_identifier
                if as2_partner_identifier:
                    updates["as2_partner_identifier"] = as2_partner_identifier
                if as2_authentication_type:
                    updates["as2_authentication_type"] = as2_authentication_type
                if as2_verify_hostname:
                    updates["as2_verify_hostname"] = as2_verify_hostname
                if as2_username:
                    updates["as2_username"] = as2_username
                if as2_password:
                    updates["as2_password"] = as2_password
                if as2_client_ssl_alias:
                    updates["as2_client_ssl_alias"] = as2_client_ssl_alias
                if as2_encrypt_alias:
                    updates["as2_encrypt_alias"] = as2_encrypt_alias
                if as2_sign_alias:
                    updates["as2_sign_alias"] = as2_sign_alias
                if as2_mdn_alias:
                    updates["as2_mdn_alias"] = as2_mdn_alias
                if as2_signed:
                    updates["as2_signed"] = as2_signed
                if as2_encrypted:
                    updates["as2_encrypted"] = as2_encrypted
                if as2_compressed:
                    updates["as2_compressed"] = as2_compressed
                if as2_encryption_algorithm:
                    updates["as2_encryption_algorithm"] = as2_encryption_algorithm
                if as2_signing_digest_alg:
                    updates["as2_signing_digest_alg"] = as2_signing_digest_alg
                if as2_data_content_type:
                    updates["as2_data_content_type"] = as2_data_content_type
                if as2_request_mdn:
                    updates["as2_request_mdn"] = as2_request_mdn
                if as2_mdn_signed:
                    updates["as2_mdn_signed"] = as2_mdn_signed
                if as2_mdn_digest_alg:
                    updates["as2_mdn_digest_alg"] = as2_mdn_digest_alg
                if as2_synchronous_mdn:
                    updates["as2_synchronous_mdn"] = as2_synchronous_mdn
                if as2_fail_on_negative_mdn:
                    updates["as2_fail_on_negative_mdn"] = as2_fail_on_negative_mdn
                if as2_subject:
                    updates["as2_subject"] = as2_subject
                if as2_multiple_attachments:
                    updates["as2_multiple_attachments"] = as2_multiple_attachments
                if as2_max_document_count:
                    updates["as2_max_document_count"] = as2_max_document_count
                if as2_attachment_option:
                    updates["as2_attachment_option"] = as2_attachment_option
                if as2_attachment_cache:
                    updates["as2_attachment_cache"] = as2_attachment_cache
                if as2_mdn_external_url:
                    updates["as2_mdn_external_url"] = as2_mdn_external_url
                if as2_mdn_use_external_url:
                    updates["as2_mdn_use_external_url"] = as2_mdn_use_external_url
                if as2_mdn_use_ssl:
                    updates["as2_mdn_use_ssl"] = as2_mdn_use_ssl
                if as2_mdn_client_ssl_cert:
                    updates["as2_mdn_client_ssl_cert"] = as2_mdn_client_ssl_cert
                if as2_mdn_ssl_cert:
                    updates["as2_mdn_ssl_cert"] = as2_mdn_ssl_cert
                if as2_reject_duplicates:
                    updates["as2_reject_duplicates"] = as2_reject_duplicates
                if as2_duplicate_check_count:
                    updates["as2_duplicate_check_count"] = as2_duplicate_check_count
                if as2_legacy_smime:
                    updates["as2_legacy_smime"] = as2_legacy_smime

                # MLLP protocol fields (flat)
                if mllp_host:
                    updates["mllp_host"] = mllp_host
                if mllp_port:
                    updates["mllp_port"] = mllp_port
                if mllp_use_ssl:
                    updates["mllp_use_ssl"] = mllp_use_ssl
                if mllp_persistent:
                    updates["mllp_persistent"] = mllp_persistent
                if mllp_send_timeout:
                    updates["mllp_send_timeout"] = mllp_send_timeout
                if mllp_receive_timeout:
                    updates["mllp_receive_timeout"] = mllp_receive_timeout
                if mllp_max_connections:
                    updates["mllp_max_connections"] = mllp_max_connections
                if mllp_inactivity_timeout:
                    updates["mllp_inactivity_timeout"] = mllp_inactivity_timeout
                if mllp_max_retry:
                    updates["mllp_max_retry"] = mllp_max_retry
                if mllp_halt_timeout:
                    updates["mllp_halt_timeout"] = mllp_halt_timeout
                if mllp_use_client_ssl:
                    updates["mllp_use_client_ssl"] = mllp_use_client_ssl
                if mllp_client_ssl_alias:
                    updates["mllp_client_ssl_alias"] = mllp_client_ssl_alias
                if mllp_ssl_alias:
                    updates["mllp_ssl_alias"] = mllp_ssl_alias

                # OFTP protocol fields (flat)
                if oftp_host:
                    updates["oftp_host"] = oftp_host
                if oftp_port:
                    updates["oftp_port"] = oftp_port
                if oftp_tls:
                    updates["oftp_tls"] = oftp_tls
                if oftp_ssid_code:
                    updates["oftp_ssid_code"] = oftp_ssid_code
                if oftp_ssid_password:
                    updates["oftp_ssid_password"] = oftp_ssid_password
                if oftp_compress:
                    updates["oftp_compress"] = oftp_compress
                if oftp_ssid_auth:
                    updates["oftp_ssid_auth"] = oftp_ssid_auth
                if oftp_sfid_cipher:
                    updates["oftp_sfid_cipher"] = oftp_sfid_cipher
                if oftp_use_gateway:
                    updates["oftp_use_gateway"] = oftp_use_gateway
                if oftp_use_client_ssl:
                    updates["oftp_use_client_ssl"] = oftp_use_client_ssl
                if oftp_client_ssl_alias:
                    updates["oftp_client_ssl_alias"] = oftp_client_ssl_alias
                if oftp_sfid_sign:
                    updates["oftp_sfid_sign"] = oftp_sfid_sign
                if oftp_sfid_encrypt:
                    updates["oftp_sfid_encrypt"] = oftp_sfid_encrypt

                # EDIFACT standard fields
                if edifact_interchange_id:
                    updates["edifact_interchange_id"] = edifact_interchange_id
                if edifact_interchange_id_qual:
                    updates["edifact_interchange_id_qual"] = edifact_interchange_id_qual
                if edifact_syntax_id:
                    updates["edifact_syntax_id"] = edifact_syntax_id
                if edifact_syntax_version:
                    updates["edifact_syntax_version"] = edifact_syntax_version
                if edifact_test_indicator:
                    updates["edifact_test_indicator"] = edifact_test_indicator

                # HL7 standard fields
                if hl7_sending_application:
                    updates["hl7_sending_application"] = hl7_sending_application
                if hl7_sending_facility:
                    updates["hl7_sending_facility"] = hl7_sending_facility
                if hl7_receiving_application:
                    updates["hl7_receiving_application"] = hl7_receiving_application
                if hl7_receiving_facility:
                    updates["hl7_receiving_facility"] = hl7_receiving_facility

                # RosettaNet standard fields
                if rosettanet_partner_id:
                    updates["rosettanet_partner_id"] = rosettanet_partner_id
                if rosettanet_partner_location:
                    updates["rosettanet_partner_location"] = rosettanet_partner_location
                if rosettanet_global_usage_code:
                    updates["rosettanet_global_usage_code"] = rosettanet_global_usage_code
                if rosettanet_supply_chain_code:
                    updates["rosettanet_supply_chain_code"] = rosettanet_supply_chain_code
                if rosettanet_classification_code:
                    updates["rosettanet_classification_code"] = rosettanet_classification_code

                # TRADACOMS standard fields
                if tradacoms_interchange_id:
                    updates["tradacoms_interchange_id"] = tradacoms_interchange_id
                if tradacoms_interchange_id_qualifier:
                    updates["tradacoms_interchange_id_qualifier"] = tradacoms_interchange_id_qualifier

                # ODETTE standard fields
                if odette_interchange_id:
                    updates["odette_interchange_id"] = odette_interchange_id
                if odette_interchange_id_qual:
                    updates["odette_interchange_id_qual"] = odette_interchange_id_qual
                if odette_syntax_id:
                    updates["odette_syntax_id"] = odette_syntax_id
                if odette_syntax_version:
                    updates["odette_syntax_version"] = odette_syntax_version
                if odette_test_indicator:
                    updates["odette_test_indicator"] = odette_test_indicator

                # Organization linking
                if organization_id:
                    updates["organization_id"] = organization_id

                params["updates"] = updates

            elif action == "delete":
                params["partner_id"] = partner_id

            elif action == "analyze_usage":
                params["partner_id"] = partner_id

            return manage_trading_partner_action(sdk, profile, action, **params)

        except Exception as e:
            print(f"[ERROR] Failed to {action} trading partner: {e}")
            return {"_success": False, "error": str(e)}

    print("[INFO] Trading partner tool registered successfully (1 consolidated tool, local)")


# --- Process MCP Tools (Local) ---
if manage_process_action:
    @mcp.tool()
    def manage_process(
        profile: str,
        action: str,
        process_id: str = None,
        config_yaml: str = None,
        filters: str = None
    ):
        """
        Manage Boomi process components with AI-friendly YAML configuration.

        This tool enables creation of simple processes or complex multi-component
        workflows with automatic dependency management and ID resolution.

        Args:
            profile: Boomi profile name (required)
            action: Action to perform - must be one of: list, get, create, update, delete
            process_id: Process component ID (required for get, update, delete)
            config_yaml: YAML configuration string (required for create, update)
            filters: JSON string with filters for list action (optional)

        Actions:
            - list: List all process components
                Example: action="list"
                Example with filter: action="list", filters='{"folder_name": "Integrations"}'

            - get: Get specific process by ID
                Example: action="get", process_id="abc-123-def"

            - create: Create new process(es) from YAML
                Single process example:
                    config_yaml = '''
                    name: "Hello World"
                    folder_name: "Test"
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

                Multi-component with dependencies:
                    config_yaml = '''
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

            - update: Update existing process
                Example: action="update", process_id="abc-123", config_yaml="..."

            - delete: Delete process
                Example: action="delete", process_id="abc-123-def"

        YAML Shape Types:
            - start: Process start (required first shape)
            - stop: Process termination (can be last shape)
            - return: Return documents (alternative last shape)
            - message: Debug/logging messages
            - map: Data transformation (requires map_id or map_ref)
            - connector: External system integration (requires connector_id, operation)
            - decision: Conditional branching (requires expression)
            - branch: Parallel branches (requires num_branches)
            - note: Documentation annotation

        Returns:
            Dict with success status and result data

        Examples:
            # List all processes
            result = manage_process(profile="prod", action="list")

            # Create simple process
            result = manage_process(
                profile="prod",
                action="create",
                config_yaml="name: Test\\nshapes: [...]"
            )

            # Get process details
            result = manage_process(
                profile="prod",
                action="get",
                process_id="abc-123-def"
            )
        """
        try:
            subject = TEST_USER
            print(f"[INFO] manage_process called for local user: {subject}, profile: {profile}, action: {action}")

            # Get credentials
            creds = get_secret(subject, profile)

            # Initialize Boomi SDK
            sdk = Boomi(
                account_id=creds["account_id"],
                username=creds["username"],
                password=creds["password"]
            )

            # Build parameters based on action
            params = {}

            if action == "list":
                if filters:
                    import json
                    params["filters"] = json.loads(filters)

            elif action == "get":
                params["process_id"] = process_id

            elif action == "create":
                params["config_yaml"] = config_yaml

            elif action == "update":
                params["process_id"] = process_id
                params["config_yaml"] = config_yaml

            elif action == "delete":
                params["process_id"] = process_id

            # Call the action function
            return manage_process_action(sdk, profile, action, **params)

        except Exception as e:
            print(f"[ERROR] Failed to {action} process: {e}")
            import traceback
            traceback.print_exc()
            return {"_success": False, "error": str(e), "exception_type": type(e).__name__}

    print("[INFO] Process tool registered successfully (1 consolidated tool, local)")


# --- Organization MCP Tools (Local) ---
if manage_organization_action:
    @mcp.tool()
    def manage_organization(
        profile: str,
        action: str,
        organization_id: str = None,
        component_name: str = None,
        folder_name: str = None,
        contact_name: str = None,
        contact_email: str = None,
        contact_phone: str = None,
        contact_fax: str = None,
        contact_url: str = None,
        contact_address: str = None,
        contact_address2: str = None,
        contact_city: str = None,
        contact_state: str = None,
        contact_country: str = None,
        contact_postalcode: str = None
    ):
        """
        Manage Boomi organizations (shared contact info for trading partners).

        Organizations provide centralized contact information that can be linked
        to multiple trading partners via the organization_id field.

        Args:
            profile: Boomi profile name (required)
            action: Action to perform - must be one of: list, get, create, update, delete
            organization_id: Organization component ID (required for get, update, delete)
            component_name: Organization name (required for create)
            folder_name: Folder to place organization in (default: Home)

            # Contact Information (all fields used for create/update)
            contact_name: Contact person name
            contact_email: Contact email address
            contact_phone: Contact phone number
            contact_fax: Contact fax number
            contact_url: Contact URL/website
            contact_address: Street address line 1
            contact_address2: Street address line 2
            contact_city: City
            contact_state: State/Province
            contact_country: Country
            contact_postalcode: Postal/ZIP code

        Returns:
            Action result with success status and data/error

        Examples:
            # List all organizations
            manage_organization(profile="sandbox", action="list")

            # Create organization with contact info
            manage_organization(
                profile="sandbox",
                action="create",
                component_name="Acme Corp",
                folder_name="Home/Organizations",
                contact_name="John Doe",
                contact_email="john@acme.com",
                contact_phone="555-1234",
                contact_address="123 Main St",
                contact_city="New York",
                contact_state="NY",
                contact_country="USA",
                contact_postalcode="10001"
            )

            # Link trading partner to organization
            # Use manage_trading_partner with organization_id parameter
        """
        try:
            subject = TEST_USER
            print(f"[INFO] manage_organization called for local user: {subject}, profile: {profile}, action: {action}")

            # Get credentials
            creds = get_secret(subject, profile)

            # Initialize Boomi SDK
            sdk = Boomi(
                account_id=creds["account_id"],
                username=creds["username"],
                password=creds["password"]
            )

            # Build parameters based on action
            params = {}

            if action == "list":
                filters = {}
                if folder_name:
                    filters["folder_name"] = folder_name
                params["filters"] = filters if filters else None

            elif action == "get":
                params["organization_id"] = organization_id

            elif action == "create":
                request_data = {}
                if component_name:
                    request_data["component_name"] = component_name
                if folder_name:
                    request_data["folder_name"] = folder_name

                # Contact fields
                if contact_name:
                    request_data["contact_name"] = contact_name
                if contact_email:
                    request_data["contact_email"] = contact_email
                if contact_phone:
                    request_data["contact_phone"] = contact_phone
                if contact_fax:
                    request_data["contact_fax"] = contact_fax
                if contact_url:
                    request_data["contact_url"] = contact_url
                if contact_address:
                    request_data["contact_address"] = contact_address
                if contact_address2:
                    request_data["contact_address2"] = contact_address2
                if contact_city:
                    request_data["contact_city"] = contact_city
                if contact_state:
                    request_data["contact_state"] = contact_state
                if contact_country:
                    request_data["contact_country"] = contact_country
                if contact_postalcode:
                    request_data["contact_postalcode"] = contact_postalcode

                params["request_data"] = request_data

            elif action == "update":
                params["organization_id"] = organization_id
                updates = {}
                if component_name:
                    updates["component_name"] = component_name
                if folder_name:
                    updates["folder_name"] = folder_name

                # Contact fields
                if contact_name:
                    updates["contact_name"] = contact_name
                if contact_email:
                    updates["contact_email"] = contact_email
                if contact_phone:
                    updates["contact_phone"] = contact_phone
                if contact_fax:
                    updates["contact_fax"] = contact_fax
                if contact_url:
                    updates["contact_url"] = contact_url
                if contact_address:
                    updates["contact_address"] = contact_address
                if contact_address2:
                    updates["contact_address2"] = contact_address2
                if contact_city:
                    updates["contact_city"] = contact_city
                if contact_state:
                    updates["contact_state"] = contact_state
                if contact_country:
                    updates["contact_country"] = contact_country
                if contact_postalcode:
                    updates["contact_postalcode"] = contact_postalcode

                params["updates"] = updates

            elif action == "delete":
                params["organization_id"] = organization_id

            return manage_organization_action(sdk, profile, action, **params)

        except Exception as e:
            print(f"[ERROR] Failed to {action} organization: {e}")
            import traceback
            traceback.print_exc()
            return {"_success": False, "error": str(e)}

    print("[INFO] Organization tool registered successfully (1 consolidated tool, local)")


if __name__ == "__main__":
    # Print startup info
    print("\n" + "=" * 60)
    print("🚀 Boomi MCP Server - LOCAL DEVELOPMENT MODE")
    print("=" * 60)
    print("⚠️  WARNING: This is for LOCAL TESTING ONLY")
    print("⚠️  No OAuth authentication - DO NOT use in production")
    print("=" * 60)
    print(f"Auth Mode:     None (local dev)")
    print(f"Storage:       Local file (~/.boomi_mcp_local_secrets.json)")
    print(f"Test User:     {TEST_USER}")
    print("=" * 60)
    print("\n🔧 MCP Tools available:")
    print("  • list_boomi_profiles - List saved credential profiles")
    print("  • set_boomi_credentials - Store Boomi credentials")
    print("  • delete_boomi_profile - Delete a credential profile")
    print("  • boomi_account_info - Get account information from Boomi API")
    if manage_trading_partner_action:
        print("\n  🤝 Trading Partner Management (CONSOLIDATED):")
        print("  • manage_trading_partner - Unified tool for all trading partner operations")
        print("    Actions: list, get, create, update, delete, analyze_usage")
        print("    Standards: X12, EDIFACT, HL7, RosettaNet, Custom, Tradacoms, Odette")
    print("\n📝 Quick Start:")
    print("  1. Connect with: claude mcp add boomi-local stdio -- python server_local.py")
    print("  2. Use set_boomi_credentials to store your Boomi API credentials")
    print("  3. Use boomi_account_info to test API calls")
    print("=" * 60 + "\n")

    # Run in stdio mode for fast local testing
    mcp.run(transport="stdio")
