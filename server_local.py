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

import json
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
        resource_id: str = None,
        config: str = None,
    ):
        """
        Manage B2B/EDI trading partners (all 7 standards) via JSON config.

        Args:
            profile: Boomi profile name (required)
            action: One of: list, get, create, update, delete, analyze_usage
            resource_id: Trading partner component ID (required for get, update, delete, analyze_usage)
            config: JSON string with action-specific configuration (see examples below)

        Tip: Use action="get" with a known resource_id to retrieve the full structure,
        then use that output as a template for create/update config.

        Actions and config examples:

            list - List trading partners, optional filters:
                config='{"standard": "x12", "classification": "tradingpartner", "folder_name": "Partners"}'

            get - Get partner by ID (no config needed):
                resource_id="abc-123-def"

            create - Create new partner (config required):
                config='{
                    "component_name": "Acme Corp",
                    "standard": "x12",
                    "classification": "tradingpartner",
                    "folder_name": "Partners",
                    "isa_id": "ACME",
                    "isa_qualifier": "ZZ",
                    "gs_id": "ACMECORP",
                    "contact_name": "John Doe",
                    "contact_email": "john@acme.com",
                    "communication_protocols": ["http", "as2"],
                    "http_url": "https://api.acme.com/edi",
                    "as2_url": "https://as2.acme.com",
                    "as2_identifier": "ACME-AS2"
                }'

            update - Update existing partner (config required):
                resource_id="abc-123-def"
                config='{"contact_email": "new@acme.com", "http_url": "https://new.acme.com"}'

            delete - Delete partner (no config needed):
                resource_id="abc-123-def"

            analyze_usage - Analyze partner usage (no config needed):
                resource_id="abc-123-def"

        Config field reference (all optional, grouped by category):

            Basic: component_name, standard (x12|edifact|hl7|rosettanet|custom|tradacoms|odette),
                   classification (tradingpartner|mycompany), folder_name

            X12: isa_id, isa_qualifier, gs_id
            EDIFACT: edifact_interchange_id, edifact_interchange_id_qual, edifact_syntax_id,
                     edifact_syntax_version, edifact_test_indicator
            HL7: hl7_application, hl7_facility
            RosettaNet: rosettanet_partner_id, rosettanet_partner_location,
                        rosettanet_global_usage_code, rosettanet_supply_chain_code,
                        rosettanet_classification_code
            TRADACOMS: tradacoms_interchange_id, tradacoms_interchange_id_qualifier
            ODETTE: odette_interchange_id, odette_interchange_id_qual, odette_syntax_id,
                    odette_syntax_version, odette_test_indicator

            Contact: contact_name, contact_email, contact_phone, contact_fax,
                     contact_address, contact_address2, contact_city, contact_state,
                     contact_country, contact_postalcode

            Protocols: communication_protocols (JSON array: ["http", "as2", "ftp", "sftp", "disk", "mllp", "oftp"])
            Organization: organization_id

            Protocol-specific keys (use action="get" to see all fields for a protocol):
                Disk: disk_directory, disk_get_directory, disk_send_directory, ... (9 fields)
                FTP: ftp_host, ftp_port, ftp_username, ftp_password, ... (17 fields)
                SFTP: sftp_host, sftp_port, sftp_username, sftp_password, ... (22 fields)
                HTTP: http_url, http_username, http_authentication_type, ... (40+ fields incl. OAuth)
                AS2: as2_url, as2_identifier, as2_signed, as2_encrypted, ... (30 fields)
                MLLP: mllp_host, mllp_port, mllp_use_ssl, ... (13 fields)
                OFTP: oftp_host, oftp_port, oftp_tls, ... (14 fields)

        Returns:
            Action result with success status and data/error
        """
        # Parse config JSON
        config_data = {}
        if config:
            try:
                config_data = json.loads(config)
            except json.JSONDecodeError as e:
                return {"_success": False, "error": f"Invalid JSON in config: {e}"}

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
                if config_data:
                    params["filters"] = config_data

            elif action == "get":
                params["partner_id"] = resource_id

            elif action == "create":
                params["request_data"] = config_data

            elif action == "update":
                params["partner_id"] = resource_id
                params["updates"] = config_data

            elif action == "delete":
                params["partner_id"] = resource_id

            elif action == "analyze_usage":
                params["partner_id"] = resource_id

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
        resource_id: str = None,
        config: str = None,
    ):
        """
        Manage Boomi organizations (shared contact info for trading partners) via JSON config.

        Args:
            profile: Boomi profile name (required)
            action: One of: list, get, create, update, delete
            resource_id: Organization component ID (required for get, update, delete)
            config: JSON string with action-specific configuration (see examples below)

        Tip: Use action="get" with a known resource_id to retrieve the full structure,
        then use that output as a template for create/update config.

        Actions and config examples:

            list - List organizations, optional filters:
                config='{"folder_name": "Home/Organizations"}'

            get - Get organization by ID (no config needed):
                resource_id="abc-123-def"

            create - Create new organization (config required):
                config='{
                    "component_name": "Acme Corp",
                    "folder_name": "Home/Organizations",
                    "contact_name": "John Doe",
                    "contact_email": "john@acme.com",
                    "contact_phone": "555-1234",
                    "contact_address": "123 Main St",
                    "contact_city": "New York",
                    "contact_state": "NY",
                    "contact_country": "USA",
                    "contact_postalcode": "10001"
                }'

            update - Update existing organization (config required):
                resource_id="abc-123-def"
                config='{"contact_email": "new@acme.com", "contact_phone": "555-5678"}'

            delete - Delete organization (no config needed):
                resource_id="abc-123-def"

        Config field reference:
            component_name, folder_name,
            contact_name, contact_email, contact_phone, contact_fax, contact_url,
            contact_address, contact_address2, contact_city, contact_state,
            contact_country, contact_postalcode

        Returns:
            Action result with success status and data/error
        """
        # Parse config JSON
        config_data = {}
        if config:
            try:
                config_data = json.loads(config)
            except json.JSONDecodeError as e:
                return {"_success": False, "error": f"Invalid JSON in config: {e}"}

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
                if config_data:
                    params["filters"] = config_data

            elif action == "get":
                params["organization_id"] = resource_id

            elif action == "create":
                params["request_data"] = config_data

            elif action == "update":
                params["organization_id"] = resource_id
                params["updates"] = config_data

            elif action == "delete":
                params["organization_id"] = resource_id

            return manage_organization_action(sdk, profile, action, **params)

        except Exception as e:
            print(f"[ERROR] Failed to {action} organization: {e}")
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
