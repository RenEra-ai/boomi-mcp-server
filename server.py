#!/usr/bin/env python3
"""
Boomi MCP Server - Unified server for local dev and production.

When BOOMI_LOCAL=true (local development):
- Uses local file-based credential storage (~/.boomi_mcp_local_secrets.json)
- No OAuth authentication
- Credential management tools (set/delete) are active
- Runs in stdio mode

When BOOMI_LOCAL is not set (production):
- Uses GCP Secret Manager for credentials
- OAuth 2.0 authentication (Google) with refresh token support
- Web UI for credential management
- Runs in HTTP mode via server_http.py
"""

import json
import os
import sys
from typing import Dict
from pathlib import Path

from fastmcp import FastMCP

# --- Mode Detection ---
LOCAL_MODE = os.getenv("BOOMI_LOCAL", "").lower() in ("true", "1", "yes")

# Additional imports for production mode
if not LOCAL_MODE:
    import secrets
    import hashlib
    import base64
    from typing import Optional
    from fastmcp.server.dependencies import get_access_token

# --- Add boomi-python to path ---
boomi_python_path = Path(__file__).parent.parent / "boomi-python" / "src"
if boomi_python_path.exists():
    sys.path.insert(0, str(boomi_python_path))

# --- Add src to path ---
src_path = Path(__file__).parent / "src"
if src_path.exists():
    sys.path.insert(0, str(src_path))

try:
    from boomi import Boomi
    from boomi.net.transport.api_error import ApiError
except ImportError as e:
    print(f"ERROR: Failed to import Boomi SDK: {e}")
    print(f"       Boomi-python path: {boomi_python_path}")
    print(f"       Run: pip install git+https://github.com/RenEra-ai/boomi-python.git")
    sys.exit(1)

# --- Secrets Backend (conditional) ---
if LOCAL_MODE:
    try:
        from boomi_mcp.local_secrets import LocalSecretsBackend
        secrets_backend = LocalSecretsBackend()
        print(f"[INFO] Using local file-based secrets storage")
        print(f"[INFO] Storage file: {secrets_backend.storage_file}")
    except ImportError as e:
        print(f"ERROR: Failed to import local_secrets: {e}")
        print(f"       Make sure src/boomi_mcp/local_secrets.py exists")
        sys.exit(1)
else:
    try:
        from boomi_mcp.cloud_secrets import get_secrets_backend
        secrets_backend = get_secrets_backend()
        backend_type = os.getenv("SECRETS_BACKEND", "gcp")
        print(f"[INFO] Using secrets backend: {backend_type}")
        if backend_type == "gcp":
            project_id = os.getenv("GCP_PROJECT_ID", "boomimcp")
            print(f"[INFO] GCP Project: {project_id}")
    except ImportError as e:
        print(f"ERROR: Failed to import cloud_secrets: {e}")
        print(f"       Make sure src/boomi_mcp/cloud_secrets.py exists")
        sys.exit(1)

# --- Trading Partner Tools ---
try:
    from boomi_mcp.categories.components.trading_partners import manage_trading_partner_action
    print(f"[INFO] Trading partner tools loaded successfully")
except ImportError as e:
    print(f"[WARNING] Failed to import trading partner tools: {e}")
    manage_trading_partner_action = None

# --- Process Tools ---
try:
    from boomi_mcp.categories.components.processes import manage_process_action
    print(f"[INFO] Process tools loaded successfully")
except ImportError as e:
    print(f"[WARNING] Failed to import process tools: {e}")
    manage_process_action = None

# --- Organization Tools ---
try:
    from boomi_mcp.categories.components.organizations import manage_organization_action
    print(f"[INFO] Organization tools loaded successfully")
except ImportError as e:
    print(f"[WARNING] Failed to import organization tools: {e}")
    manage_organization_action = None

# --- Component Query Tools ---
try:
    from boomi_mcp.categories.components.query_components import query_components_action
    print(f"[INFO] Component query tools loaded successfully")
except ImportError as e:
    print(f"[WARNING] Failed to import component query tools: {e}")
    query_components_action = None

# --- Component Management Tools ---
try:
    from boomi_mcp.categories.components.manage_component import manage_component_action
    print(f"[INFO] Component management tools loaded successfully")
except ImportError as e:
    print(f"[WARNING] Failed to import component management tools: {e}")
    manage_component_action = None

# --- Component Analysis Tools ---
try:
    from boomi_mcp.categories.components.analyze_component import analyze_component_action
    print(f"[INFO] Component analysis tools loaded successfully")
except ImportError as e:
    print(f"[WARNING] Failed to import component analysis tools: {e}")
    analyze_component_action = None

# --- Connector Tools ---
try:
    from boomi_mcp.categories.components.connectors import manage_connector_action
    print(f"[INFO] Connector tools loaded successfully")
except ImportError as e:
    print(f"[WARNING] Failed to import connector tools: {e}")
    manage_connector_action = None

# --- Folder Tools ---
try:
    from boomi_mcp.categories.folders import manage_folders_action
    print(f"[INFO] Folder tools loaded successfully")
except ImportError as e:
    print(f"[WARNING] Failed to import folder tools: {e}")
    manage_folders_action = None

# --- Monitoring Tools ---
try:
    from boomi_mcp.categories.monitoring import monitor_platform_action
    print(f"[INFO] Monitoring tools loaded successfully")
except ImportError as e:
    print(f"[WARNING] Failed to import monitoring tools: {e}")
    monitor_platform_action = None

# --- Schema Template Tools ---
try:
    from boomi_mcp.categories.meta_tools import get_schema_template_action
    print(f"[INFO] Schema template tools loaded successfully")
except ImportError as e:
    print(f"[WARNING] Failed to import schema template tools: {e}")
    get_schema_template_action = None

# --- Generic API Invoker ---
try:
    from boomi_mcp.categories.meta_tools import invoke_api
    print(f"[INFO] Generic API invoker loaded successfully")
except ImportError as e:
    print(f"[WARNING] Failed to import generic API invoker: {e}")
    invoke_api = None

# --- List Capabilities ---
try:
    from boomi_mcp.categories.meta_tools import list_capabilities_action
    print(f"[INFO] List capabilities loaded successfully")
except ImportError as e:
    print(f"[WARNING] Failed to import list capabilities: {e}")
    list_capabilities_action = None

# --- Environment Tools ---
try:
    from boomi_mcp.categories.environments import manage_environments_action
    print(f"[INFO] Environment tools loaded successfully")
except ImportError as e:
    print(f"[WARNING] Failed to import environment tools: {e}")
    manage_environments_action = None

# --- Runtime Tools ---
try:
    from boomi_mcp.categories.runtimes import manage_runtimes_action
    print(f"[INFO] Runtime tools loaded successfully")
except ImportError as e:
    print(f"[WARNING] Failed to import runtime tools: {e}")
    manage_runtimes_action = None

# --- Deployment Tools ---
try:
    from boomi_mcp.categories.deployment.packages import manage_deployment_action
    print(f"[INFO] Deployment tools loaded successfully")
except ImportError as e:
    print(f"[WARNING] Failed to import deployment tools: {e}")
    manage_deployment_action = None

# --- Execution Tools ---
try:
    from boomi_mcp.categories.execution import execute_process_action
    print(f"[INFO] Execution tools loaded successfully")
except ImportError as e:
    print(f"[WARNING] Failed to import execution tools: {e}")
    execute_process_action = None

# --- Shared Resources Tools ---
try:
    from boomi_mcp.categories.shared_resources import manage_shared_resources_action
    print(f"[INFO] Shared resources tools loaded successfully")
except ImportError as e:
    print(f"[WARNING] Failed to import shared resources tools: {e}")
    manage_shared_resources_action = None

# --- Troubleshooting Tools ---
try:
    from boomi_mcp.categories.troubleshooting import troubleshoot_execution_action
    print(f"[INFO] Troubleshooting tools loaded successfully")
except ImportError as e:
    print(f"[WARNING] Failed to import troubleshooting tools: {e}")
    troubleshoot_execution_action = None

# --- Schedule Tools ---
try:
    from boomi_mcp.categories.schedules import manage_schedules_action
    print(f"[INFO] Schedule tools loaded successfully")
except ImportError as e:
    print(f"[WARNING] Failed to import schedule tools: {e}")
    manage_schedules_action = None

# --- Account Tools ---
try:
    from boomi_mcp.categories.account import manage_account_action
    print(f"[INFO] Account tools loaded successfully")
except ImportError as e:
    print(f"[WARNING] Failed to import account tools: {e}")
    manage_account_action = None

# --- Integration Builder Tool ---
try:
    from boomi_mcp.categories.integration_builder import build_integration_action
    print(f"[INFO] Integration builder tool loaded successfully")
except ImportError as e:
    print(f"[WARNING] Failed to import integration builder tool: {e}")
    build_integration_action = None


def _extract_api_error_msg(e) -> str:
    """Extract user-friendly message from ApiError."""
    detail = getattr(e, "error_detail", None)
    if detail:
        return detail
    resp = getattr(e, "response", None)
    if resp:
        body = getattr(resp, "body", None)
        if isinstance(body, dict):
            msg = body.get("message", "")
            if msg:
                return msg
    return getattr(e, "message", "") or str(e)


def _serialize_sdk_object(obj):
    """Recursively convert SDK objects to JSON-safe dicts."""
    if obj is None or isinstance(obj, (str, int, float, bool)):
        return obj
    if isinstance(obj, list):
        return [_serialize_sdk_object(item) for item in obj]
    if isinstance(obj, dict):
        return {k: _serialize_sdk_object(v) for k, v in obj.items()
                if not k.startswith("_") and v is not None}
    if hasattr(obj, "__dict__"):
        return {k: _serialize_sdk_object(v) for k, v in obj.__dict__.items()
                if not k.startswith("_") and v is not None}
    return str(obj)


def put_secret(sub: str, profile: str, payload: Dict[str, str]):
    """Store credentials for a user profile."""
    secrets_backend.put_secret(sub, profile, payload)
    print(f"[INFO] Stored credentials for {sub}:{profile} (username: {payload.get('username', '')[:10]}***)")


def get_secret(sub: str, profile: str) -> Dict[str, str]:
    """Retrieve credentials for a user profile."""
    return secrets_backend.get_secret(sub, profile)


def list_profiles(sub: str):
    """List all profiles for a user."""
    return secrets_backend.list_profiles(sub)


def delete_profile(sub: str, profile: str):
    """Delete a user profile."""
    secrets_backend.delete_profile(sub, profile)


# --- OAuth Setup (production only) ---
if not LOCAL_MODE:
    from fastmcp.server.auth.providers.google import GoogleProvider
    from key_value.aio.stores.mongodb import MongoDBStore
    from key_value.aio.wrappers.encryption import FernetEncryptionWrapper
    from cryptography.fernet import Fernet

    # Create Google OAuth provider
    try:
        client_id = os.getenv("OIDC_CLIENT_ID")
        client_secret = os.getenv("OIDC_CLIENT_SECRET")
        base_url = os.getenv("OIDC_BASE_URL", "http://localhost:8000")

        if not client_id or not client_secret:
            raise ValueError("OIDC_CLIENT_ID and OIDC_CLIENT_SECRET must be set")

        # Get MongoDB connection and encryption keys
        mongodb_uri = os.getenv("MONGODB_URI")
        jwt_signing_key = os.getenv("JWT_SIGNING_KEY")
        storage_encryption_key = os.getenv("STORAGE_ENCRYPTION_KEY")

        if not mongodb_uri:
            raise ValueError("MONGODB_URI must be set for production deployment")
        if not jwt_signing_key:
            raise ValueError("JWT_SIGNING_KEY must be set for production deployment")
        if not storage_encryption_key:
            raise ValueError("STORAGE_ENCRYPTION_KEY must be set for production deployment")

        # Create MongoDB storage with Fernet encryption (production-ready)
        # Using MongoDB Atlas free tier (512MB, persistent)
        mongodb_storage = MongoDBStore(
            url=mongodb_uri,
            db_name="boomi_mcp",
            coll_name="oauth_tokens"
        )

        encrypted_storage = FernetEncryptionWrapper(
            key_value=mongodb_storage,
            fernet=Fernet(storage_encryption_key.encode())
        )

        print(f"[INFO] OAuth tokens will be stored in MongoDB Atlas")
        print(f"[INFO] Token storage encrypted with Fernet")

        # Create GoogleProvider with encrypted MongoDB storage
        auth = GoogleProvider(
            client_id=client_id,
            client_secret=client_secret,
            base_url=base_url,
            jwt_signing_key=jwt_signing_key,  # Explicit JWT signing key (production requirement)
            client_storage=encrypted_storage,  # Encrypted MongoDB storage
            extra_authorize_params={
                "access_type": "offline",  # Request refresh tokens from Google
                "prompt": "consent",       # Force consent to ensure refresh token is issued
            },
        )

        # FIX: Patch register_client to clear client_secret when token_endpoint_auth_method="none"
        # This fixes a bug where MCP clients send a secret during registration but don't send it
        # during token exchange when using auth_method="none". The MCP SDK's ClientAuthenticator
        # incorrectly requires the secret if it's stored, regardless of auth_method.
        original_register_client = auth.register_client
        async def patched_register_client(client_info):
            if hasattr(client_info, 'client_secret') and client_info.client_secret:
                client_info = client_info.model_copy(update={"client_secret": None})
            return await original_register_client(client_info)
        auth.register_client = patched_register_client

        print(f"[INFO] Google OAuth 2.0 configured")
        print(f"[INFO] Base URL: {base_url}")
        print(f"[INFO] All authenticated Google users have full access to all tools")
        print(f"[INFO] OAuth endpoints:")
        print(f"       - Authorize: {base_url}/authorize")
        print(f"       - Callback: {base_url}/auth/callback")
        print(f"       - Token: {base_url}/token")
    except Exception as e:
        print(f"[ERROR] Failed to configure OAuth: {e}")
        print(f"[ERROR] Please ensure these environment variables are set:")
        print(f"       - OIDC_CLIENT_ID")
        print(f"       - OIDC_CLIENT_SECRET")
        print(f"       - OIDC_BASE_URL")
        sys.exit(1)

    # Create FastMCP server with auth
    mcp = FastMCP(
        name="Boomi MCP Server",
        auth=auth
    )

    # Add SessionMiddleware for web UI OAuth flow
    session_secret = os.getenv("SESSION_SECRET")
    if not session_secret:
        print("[ERROR] SESSION_SECRET environment variable must be set for web UI")
        sys.exit(1)

    if hasattr(mcp, '_app'):
        mcp._app.add_middleware(SessionMiddleware, secret_key=session_secret, max_age=3600)
        print(f"[INFO] SessionMiddleware configured for web UI")
    elif hasattr(mcp, 'app'):
        mcp.app.add_middleware(SessionMiddleware, secret_key=session_secret, max_age=3600)
        print(f"[INFO] SessionMiddleware configured for web UI")

    # --- Helper: get authenticated user info ---
    def get_user_subject() -> str:
        """Get authenticated user subject from access token."""
        token = get_access_token()
        if not token:
            raise PermissionError("Authentication required")

        # Get subject from JWT claims (Google email)
        subject = token.claims.get("sub") if hasattr(token, "claims") else token.client_id
        if not subject:
            # Try email as fallback
            subject = token.claims.get("email") if hasattr(token, "claims") else None
        if not subject:
            raise PermissionError("Token missing 'sub' or 'email' claim")

        return subject

else:
    # Local dev mode - no auth
    mcp = FastMCP(
        name="Boomi MCP Server (Local Dev)"
    )


# --- User Identity ---
def get_current_user() -> str:
    """Get current user identity (local dev user or OAuth subject)."""
    if LOCAL_MODE:
        return "local-dev-user"
    return get_user_subject()


# --- MCP Tools ---

@mcp.tool(
    annotations={
        "readOnlyHint": True,
        "openWorldHint": True
    }
)
def list_boomi_profiles():
    """
    List all saved Boomi credential profiles for the current user.

    Returns a list of profile names that can be used with boomi_account_info().
    Use this tool first to see which profiles are available before requesting account info.

    Returns:
        List of profile objects with 'profile' name and metadata
    """
    try:
        subject = get_current_user()
        print(f"[INFO] list_boomi_profiles called by user: {subject}")

        profiles = list_profiles(subject)
        print(f"[INFO] Found {len(profiles)} profiles for {subject}")

        if not profiles:
            result = {
                "_success": True,
                "profiles": [],
                "message": "No profiles found. Use set_boomi_credentials tool to add credentials." if LOCAL_MODE else "No profiles found. Add credentials at https://boomi.renera.ai/",
            }
            if not LOCAL_MODE:
                result["web_portal"] = "https://boomi.renera.ai/"
            return result

        result = {
            "_success": True,
            "profiles": [p["profile"] for p in profiles],
            "count": len(profiles),
        }
        if not LOCAL_MODE:
            result["web_portal"] = "https://boomi.renera.ai/"
        return result
    except Exception as e:
        print(f"[ERROR] Failed to list profiles: {e}")
        return {
            "_success": False,
            "error": f"Failed to list profiles: {str(e)}"
        }


@mcp.tool(
    annotations={
        "readOnlyHint": True,
        "openWorldHint": True
    }
)
def boomi_account_info(profile: str):
    """
    Get Boomi account information from a specific profile.

    MULTI-ACCOUNT SUPPORT:
    - Users can store multiple Boomi accounts (up to 10 profiles)
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

    WEB PORTAL:
    - Store credentials at: https://boomi.renera.ai/
    - Each credential set is stored as a named profile
    - Profile name is required when adding credentials
    - Users can add, delete, and switch between profiles

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
        subject = get_current_user()
        print(f"[INFO] boomi_account_info called by user: {subject}, profile: {profile}")
    except Exception as e:
        print(f"[ERROR] Failed to get user subject: {e}")
        return {
            "_success": False,
            "error": f"Authentication failed: {str(e)}"
        }

    # Try to get stored credentials
    try:
        creds = get_secret(subject, profile)
        print(f"[INFO] Successfully retrieved stored credentials for {subject}:{profile}")
        print(f"[INFO] Account ID: {creds.get('account_id')}, Username: {creds.get('username', '')[:20]}...")
    except ValueError as e:
        print(f"[ERROR] Profile '{profile}' not found for user {subject}: {e}")

        # List available profiles
        available_profiles = list_profiles(subject)
        print(f"[INFO] Available profiles for {subject}: {[p['profile'] for p in available_profiles]}")

        result = {
            "_success": False,
            "error": f"Profile '{profile}' not found. {'Use set_boomi_credentials to add credentials.' if LOCAL_MODE else 'Please store credentials at the web portal first.'}",
            "available_profiles": [p["profile"] for p in available_profiles],
        }
        if not LOCAL_MODE:
            result["web_portal"] = "https://boomi-mcp-server-126964451821.us-central1.run.app/"
        return result
    except Exception as e:
        print(f"[ERROR] Unexpected error retrieving credentials: {e}")
        return {
            "_success": False,
            "error": f"Failed to retrieve credentials: {str(e)}"
        }

    print(f"[INFO] Calling Boomi API for {subject}:{profile} (account: {creds['account_id']})")

    # Initialize Boomi SDK
    try:
        sdk_params = {
            "account_id": creds["account_id"],
            "username": creds["username"],
            "password": creds["password"],
            "timeout": 30000,  # 30 seconds (SDK uses milliseconds)
        }

        # Only add base_url if explicitly provided (not None)
        if creds.get("base_url"):
            sdk_params["base_url"] = creds["base_url"]

        sdk = Boomi(**sdk_params)

        # Call the same endpoint the sample demonstrates
        result = sdk.account.get_account(id_=creds["account_id"])

        # Convert to plain dict for transport (recursive for nested SDK objects)
        if hasattr(result, "__dict__"):
            out = _serialize_sdk_object(result)
            out["_success"] = True
            out["_note"] = "Account data retrieved successfully"
            print(f"[INFO] Successfully retrieved account info for {creds['account_id']}")
            return out

        return {
            "_success": True,
            "message": "Account object created; minimal data returned.",
            "_note": "This indicates successful authentication."
        }

    except ApiError as e:
        print(f"[ERROR] Boomi API call failed: {e}")
        return {
            "_success": False,
            "error": _extract_api_error_msg(e),
            "account_id": creds["account_id"],
            "_note": "Check credentials and API access permissions"
        }
    except Exception as e:
        print(f"[ERROR] Boomi API call failed: {e}")
        return {
            "_success": False,
            "error": str(e),
            "account_id": creds["account_id"],
            "_note": "Check credentials and API access permissions"
        }


# --- Trading Partner MCP Tools ---
if manage_trading_partner_action:
    @mcp.tool()
    def manage_trading_partner(
        profile: str,
        action: str,
        resource_id: str = None,
        config: str = None,
    ):
        """
        Manage B2B/EDI trading partners (all 7 standards) and organizations via JSON config.

        Args:
            profile: Boomi profile name (required)
            action: One of: list, get, create, update, delete, analyze_usage, list_options, org_list, org_get, org_create, org_update, org_delete
            resource_id: Trading partner component ID (required for get, update, delete, analyze_usage) or organization ID (required for org_get, org_update, org_delete)
            config: JSON string with action-specific configuration (see examples below)

        RECOMMENDED WORKFLOW for create/update:
          1. Call action="list_options" first to get all valid enum values
          2. Present the full set of options to the user
          3. Then call action="create" or action="update" with user selections

        Tip: Use action="get" with a known resource_id to retrieve the full structure,
        then use that output as a template for create/update config.

        Actions and config examples:

            list_options - Get all valid enum values (no config needed, no API call):
                Returns standards, classifications, protocols, and protocol-specific enums

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
                    "as2_url": "https://as2.acme.com"
                }'

            update - Update existing partner (config required):
                resource_id="abc-123-def"
                config='{"contact_email": "new@acme.com", "http_url": "https://new.acme.com"}'

            delete - Delete partner (no config needed):
                resource_id="abc-123-def"

            analyze_usage - Analyze partner usage (no config needed):
                resource_id="abc-123-def"

            org_list - List organizations, optional filters:
                config='{"folder_name": "Home/Organizations"}'

            org_get - Get organization by ID (no config needed):
                resource_id="abc-123-def"

            org_create - Create new organization (config required):
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

            org_update - Update existing organization (config required):
                resource_id="abc-123-def"
                config='{"contact_email": "new@acme.com", "contact_phone": "555-5678"}'

            org_delete - Delete organization (no config needed):
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
            Organization: organization_id (use org_list to find IDs)

            Organization fields (for org_* actions):
                component_name, folder_name,
                contact_name, contact_email, contact_phone, contact_fax, contact_url,
                contact_address, contact_address2, contact_city, contact_state,
                contact_country, contact_postalcode

            Protocol-specific keys (use action="get" to see all fields for a protocol):
                Disk: disk_directory (alias: sets both get+send), disk_get_directory, disk_send_directory, ... (9 fields)
                FTP: ftp_host, ftp_port, ftp_username, ftp_password, ftp_remote_directory (alias: ftp_directory),
                     ftp_ssl_mode (NONE|EXPLICIT|IMPLICIT; alias: ftp_use_ssl true→EXPLICIT), ... (17 fields)
                SFTP: sftp_host, sftp_port, sftp_username, sftp_password, sftp_remote_directory (alias: sftp_directory),
                      sftp_ssh_key_auth (alias: sftp_use_key_auth), sftp_known_host_entry (alias: sftp_known_hosts_file), ... (22 fields)
                HTTP: http_url, http_username, http_authentication_type (NONE|BASIC|PASSWORD_DIGEST|CUSTOM|OAUTH|OAUTH2),
                      http_data_content_type (alias: http_content_type), http_connect_timeout (alias: http_connection_timeout),
                      http_method_type (alias: http_send_method), http_client_ssl_alias (alias: http_ssl_cert_id), ... (40+ fields incl. OAuth)
                AS2: as2_url, as2_signed, as2_encrypted, as2_signing_digest_alg (alias: as2_sign_algorithm; SHA1|SHA256|SHA384|SHA512),
                     as2_request_mdn (alias: as2_mdn_required), as2_sign_alias (alias: as2_signing_cert_id),
                     as2_encrypt_alias (alias: as2_encryption_cert_id), as2_data_content_type (alias: as2_content_type), ... (30 fields)
                MLLP: mllp_host, mllp_port, mllp_use_ssl, ... (13 fields)
                OFTP: oftp_host, oftp_port, oftp_tls, ... (14 fields)

            Enum values:
                edifact_test_indicator / odette_test_indicator: "1" (test), "NA" (production). "0" is auto-mapped to "NA".
                ftp_ssl_mode: "NONE", "EXPLICIT", "IMPLICIT"
                as2_signing_digest_alg: "SHA1", "SHA256", "SHA384", "SHA512"
                as2_encryption_algorithm: "tripledes", "rc2", "aes128", "aes192", "aes256"

            Dropped fields (no Boomi API equivalent):
                as2_from, as2_to (use as2_partner_id instead), http_max_redirects, http_response_content_type

        Returns:
            Action result with success status and data/error
        """
        # Static actions (no API call needed)
        if action == "list_options":
            return manage_trading_partner_action(None, profile, action)

        # Parse config JSON
        config_data = {}
        if config:
            try:
                config_data = json.loads(config)
            except (json.JSONDecodeError, TypeError) as e:
                return {"_success": False, "error": f"Invalid config (must be a JSON string): {e}"}
            if not isinstance(config_data, dict):
                return {"_success": False, "error": "config must be a JSON object, not " + type(config_data).__name__}

        try:
            subject = get_current_user()
            print(f"[INFO] manage_trading_partner called by user: {subject}, profile: {profile}, action: {action}")

            # Get credentials
            creds = get_secret(subject, profile)

            # Initialize Boomi SDK
            sdk_params = {
                "account_id": creds["account_id"],
                "username": creds["username"],
                "password": creds["password"],
                "timeout": 30000,
            }
            if creds.get("base_url"):
                sdk_params["base_url"] = creds["base_url"]
            sdk = Boomi(**sdk_params)

            # Organization sub-actions
            if action.startswith("org_"):
                if not manage_organization_action:
                    return {"_success": False, "error": "Organization module not available"}
                org_action = action[4:]  # "org_list" -> "list"
                org_params = {}
                if org_action == "list":
                    if config_data:
                        org_params["filters"] = config_data
                elif org_action == "get":
                    org_params["organization_id"] = resource_id
                elif org_action == "create":
                    org_params["request_data"] = config_data
                elif org_action == "update":
                    org_params["organization_id"] = resource_id
                    org_params["updates"] = config_data
                elif org_action == "delete":
                    org_params["organization_id"] = resource_id
                return manage_organization_action(sdk, profile, org_action, **org_params)

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

    print("[INFO] Trading partner tool registered successfully (1 consolidated tool)")


# --- Process MCP Tools ---
if manage_process_action:
    @mcp.tool()
    def manage_process(
        profile: str,
        action: str,
        process_id: str = None,
        config: str = None,
        filters: str = None
    ):
        """
        Manage Boomi process components with JSON configuration.

        Args:
            profile: Boomi profile name (required)
            action: Action to perform - must be one of: list, get, create, update, delete
            process_id: Process component ID (required for get, update, delete)
            config: JSON configuration string (required for create, update)
            filters: JSON string with filters for list action (optional)

        Actions:
            - list: List all process components
                Example: action="list"
                Example with filter: action="list", filters='{"folder_name": "Integrations"}'

            - get: Get specific process by ID
                Example: action="get", process_id="abc-123-def"

            - create: Create new process(es) from JSON config
                Single process:
                    config='{"name":"Hello World","folder_name":"Test","shapes":[{"type":"start","name":"start"},{"type":"message","name":"msg","config":{"message_text":"Hello from Boomi!"}},{"type":"stop","name":"end"}]}'
                Multi-component:
                    config='{"components":[{"name":"Transform Map","type":"map","dependencies":[]},{"name":"Main Process","type":"process","dependencies":["Transform Map"],"config":{"name":"Main Process","shapes":[{"type":"start","name":"start"},{"type":"map","name":"transform","config":{"map_ref":"Transform Map"}},{"type":"stop","name":"end"}]}}]}'

            - update: Update existing process
                Example: action="update", process_id="abc-123", config='{"name":"Updated Process","shapes":[...]}'

            - delete: Delete process
                Example: action="delete", process_id="abc-123-def"

        Shape Types:
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
                config='{"name":"Test","shapes":[...]}'
            )

            # Get process details
            result = manage_process(
                profile="prod",
                action="get",
                process_id="abc-123-def"
            )
        """
        try:
            subject = get_current_user()
            print(f"[INFO] manage_process called by user: {subject}, profile: {profile}, action: {action}")

            # Get credentials
            creds = get_secret(subject, profile)

            # Initialize Boomi SDK
            sdk_params = {
                "account_id": creds["account_id"],
                "username": creds["username"],
                "password": creds["password"],
                "timeout": 30000,
            }
            if creds.get("base_url"):
                sdk_params["base_url"] = creds["base_url"]
            sdk = Boomi(**sdk_params)

            # Build parameters based on action
            params = {}

            if action == "list":
                if filters:
                    try:
                        params["filters"] = json.loads(filters)
                    except (json.JSONDecodeError, TypeError) as e:
                        return {"_success": False, "error": f"Invalid filters (must be a JSON string): {e}"}
                    if not isinstance(params["filters"], dict):
                        return {"_success": False, "error": "filters must be a JSON object, not " + type(params["filters"]).__name__}

            elif action == "get":
                params["process_id"] = process_id

            elif action == "create":
                if not config:
                    return {"_success": False, "error": "config is required for create action"}
                try:
                    params["config"] = json.loads(config)
                except (json.JSONDecodeError, TypeError) as e:
                    return {"_success": False, "error": f"Invalid config (must be a JSON string): {e}"}
                if not isinstance(params["config"], dict):
                    return {"_success": False, "error": "config must be a JSON object, not " + type(params["config"]).__name__}

            elif action == "update":
                params["process_id"] = process_id
                if not config:
                    return {"_success": False, "error": "config is required for update action"}
                try:
                    params["config"] = json.loads(config)
                except (json.JSONDecodeError, TypeError) as e:
                    return {"_success": False, "error": f"Invalid config (must be a JSON string): {e}"}
                if not isinstance(params["config"], dict):
                    return {"_success": False, "error": "config must be a JSON object, not " + type(params["config"]).__name__}

            elif action == "delete":
                params["process_id"] = process_id

            # Call the action function
            return manage_process_action(sdk, profile, action, **params)

        except Exception as e:
            print(f"[ERROR] Failed to {action} process: {e}")
            import traceback
            traceback.print_exc()
            return {"_success": False, "error": str(e), "exception_type": type(e).__name__}

    print("[INFO] Process tool registered successfully (1 consolidated tool)")



# --- Monitoring MCP Tools ---
if monitor_platform_action:
    @mcp.tool(annotations={"openWorldHint": True})
    def monitor_platform(
        profile: str,
        action: str,
        config: str = None,
    ):
        """
        Monitor Boomi platform: execution history, logs, artifacts, audit trail, events, certificates, throughput, metrics, and connector document downloads.

        Args:
            profile: Boomi profile name (required)
            action: One of: execution_records, execution_logs, execution_artifacts, audit_logs, events, certificates, throughput, execution_metrics, connector_documents, download_connector_document
            config: JSON string with action-specific configuration (see examples below)

        Actions and config examples:

            execution_records - Query execution history (like Process Reporting):
                config='{"start_date": "2025-01-01T00:00:00Z", "end_date": "2025-01-31T23:59:59Z", "status": "ERROR", "process_name": "My Process", "atom_name": "Production Atom", "limit": 50}'
                Filter fields (at least one required): start_date, end_date, status, process_name, process_id, atom_name, atom_id, execution_id
                Status values: COMPLETE, ERROR, ABORTED, COMPLETE_WARN, INPROCESS

            execution_logs - Download and return process logs inline:
                config='{"execution_id": "abc-123-def", "log_level": "ALL"}'
                log_level values: SEVERE, WARNING, INFO, CONFIG, FINE, FINER, FINEST, ALL (default: ALL)
                Log content is automatically downloaded and returned inline (ZIP extracted).
                Set "fetch_content": false to get only the download URL instead.

            execution_artifacts - Download and return execution artifacts inline:
                config='{"execution_id": "abc-123-def"}'
                Artifact content is automatically downloaded and returned inline (ZIP extracted).
                Set "fetch_content": false to get only the download URL instead.

            audit_logs - Query audit trail with filters:
                config='{
                    "start_date": "2025-01-01T00:00:00Z",
                    "end_date": "2025-01-31T23:59:59Z",
                    "user": "user@example.com",
                    "action": "Deploy",
                    "type": "Process",
                    "level": "INFO",
                    "source": "API",
                    "limit": 100
                }'

            events - Query platform events with filters:
                config='{
                    "start_date": "2025-01-01T00:00:00Z",
                    "end_date": "2025-12-31T23:59:59Z",
                    "event_level": "ERROR",
                    "event_type": "process.error",
                    "process_name": "My Process",
                    "atom_name": "Production Atom",
                    "execution_id": "abc-123-def",
                    "limit": 100
                }'

            certificates - Query expiring/expired deployed certificates:
                config='{"days_ahead": 30, "limit": 50}'
                days_ahead: days to look ahead for expiration (default: 30)

            throughput - Account-level throughput metrics by date range:
                config='{"start_date": "2025-01-01", "end_date": "2025-01-31", "atom_id": "abc-123", "limit": 100}'
                At least one filter required: start_date, end_date, atom_id

            execution_metrics - Aggregated execution statistics (success rate, avg duration, top failures):
                config='{"start_date": "2025-01-01T00:00:00Z", "end_date": "2025-01-31T23:59:59Z", "top_failures": 5, "limit": 200}'
                Same filters as execution_records. Returns aggregated stats instead of raw records.

            connector_documents - Document-level tracking for connector operations:
                config='{"execution_id": "abc-123-def", "connector_type": "http", "status": "SUCCESS", "action_type": "GET", "limit": 50}'
                execution_id is REQUIRED. Optional: connector_type, status (SUCCESS/ERROR), action_type.

            download_connector_document - Download actual connector document content:
                config='{"generic_connector_record_id": "rec-123"}'
                generic_connector_record_id is REQUIRED (from connector_documents result).
                Set "fetch_content": false to get only the download URL without fetching content.
                Text content is returned inline; binary content is returned as base64.

        Returns:
            Action result with success status and data/error
        """
        # Parse config JSON
        config_data = {}
        if config:
            try:
                config_data = json.loads(config)
            except (json.JSONDecodeError, TypeError) as e:
                return {"_success": False, "error": f"Invalid config (must be a JSON string): {e}"}
            if not isinstance(config_data, dict):
                return {"_success": False, "error": "config must be a JSON object, not " + type(config_data).__name__}

        try:
            subject = get_current_user()
            print(f"[INFO] monitor_platform called by user: {subject}, profile: {profile}, action: {action}")

            # Get credentials
            creds = get_secret(subject, profile)

            # Initialize Boomi SDK
            sdk_params = {
                "account_id": creds["account_id"],
                "username": creds["username"],
                "password": creds["password"],
                "timeout": 30000,
            }
            if creds.get("base_url"):
                sdk_params["base_url"] = creds["base_url"]
            sdk = Boomi(**sdk_params)

            return monitor_platform_action(sdk, profile, action, config_data=config_data, creds=creds)

        except Exception as e:
            print(f"[ERROR] Failed to {action} monitor_platform: {e}")
            return {"_success": False, "error": str(e)}

    print("[INFO] Monitoring tool registered successfully (1 consolidated tool)")


# --- Component Query MCP Tools ---
if query_components_action:
    @mcp.tool(annotations={"readOnlyHint": True, "openWorldHint": True})
    def query_components(
        profile: str,
        action: str,
        component_id: str = None,
        component_ids: str = None,
        config: str = None,
    ):
        """
        Discover and retrieve Boomi components across all types.

        Args:
            profile: Boomi profile name (required)
            action: One of: list, get, search, bulk_get
            component_id: Component ID (required for get)
            component_ids: JSON array of component IDs (required for bulk_get, max 5)
            config: JSON string with action-specific configuration

        Actions and config examples:

            list - List all components, optionally filtered:
                config='{"type": "process"}'
                config='{"folder_name": "Integrations"}'
                config='{"type": "transform.map", "show_all": true}'

            get - Get a single component with full XML:
                component_id="abc-123-def"

            search - Multi-field search with AND logic:
                config='{"name": "%Test%", "type": "process"}'
                config='{"name": "%Map%", "folder_name": "Production"}'
                config='{"sub_type": "some_value"}'
                config='{"created_by": "user@example.com"}'
                config='{"modified_by": "user@example.com"}'
                config='{"component_id": "abc-123"}'

            bulk_get - Retrieve up to 5 components by ID (metadata only, no XML):
                component_ids='["id1", "id2", "id3"]'

        Component types:
            Processes: process, processproperty, processroute
            Connectors: connector-settings, connector-action
            Profiles: profile.db, profile.edi, profile.flatfile, profile.json, profile.xml
            Trading Partners: tradingpartner, tpgroup, tporganization, tpcommoptions
            Transforms: transform.map, transform.function, xslt, script.processing, script.mapping
            Services: flowservice, webservice, webservice.external
            Other: certificate, certificate.pgp, crossref, customlibrary, documentcache,
                edistandard, queue

        Returns:
            Action result with success status and component data
        """
        # Parse config JSON
        config_data = {}
        if config:
            try:
                config_data = json.loads(config)
            except (json.JSONDecodeError, TypeError) as e:
                return {"_success": False, "error": f"Invalid config (must be a JSON string): {e}"}
            if not isinstance(config_data, dict):
                return {"_success": False, "error": "config must be a JSON object, not " + type(config_data).__name__}

        # Parse component_ids JSON
        ids_list = None
        if component_ids:
            try:
                ids_list = json.loads(component_ids)
            except (json.JSONDecodeError, TypeError) as e:
                return {"_success": False, "error": f"Invalid component_ids (must be a JSON array): {e}"}
            if not isinstance(ids_list, list):
                return {"_success": False, "error": "component_ids must be a JSON array"}

        try:
            subject = get_current_user()
            print(f"[INFO] query_components called by user: {subject}, profile: {profile}, action: {action}")

            creds = get_secret(subject, profile)

            sdk_params = {
                "account_id": creds["account_id"],
                "username": creds["username"],
                "password": creds["password"],
                "timeout": 30000,
            }
            if creds.get("base_url"):
                sdk_params["base_url"] = creds["base_url"]
            sdk = Boomi(**sdk_params)

            params = {}
            if action == "list":
                if config_data:
                    params["filters"] = config_data
            elif action == "get":
                params["component_id"] = component_id
            elif action == "search":
                params["filters"] = config_data
            elif action == "bulk_get":
                params["component_ids"] = ids_list

            return query_components_action(sdk, profile, action, **params)

        except Exception as e:
            print(f"[ERROR] Failed to {action} query_components: {e}")
            return {"_success": False, "error": str(e)}

    print("[INFO] Component query tool registered successfully (1 consolidated tool)")


# --- Component Management MCP Tools ---
if manage_component_action:
    @mcp.tool()
    def manage_component(
        profile: str,
        action: str,
        component_id: str = None,
        config: str = None,
    ):
        """
        Create, update, clone, and delete Boomi components.

        Args:
            profile: Boomi profile name (required)
            action: One of: create, update, clone, delete
            component_id: Component ID (required for update, clone, delete)
            config: JSON string with action-specific configuration

        Actions and config examples:

            create - Create a component from XML template:
                config='{"xml": "<full-component-xml>...</full-component-xml>"}'
                For processes, use manage_process with config (JSON) instead.
                For connectors (connector-settings, connector-action), use manage_connector.
                Tip: Use query_components get on a similar component to obtain an XML template.

            update - Update an existing component:
                component_id="abc-123-def"
                config='{"name": "Renamed Component"}'
                config='{"description": "Updated description"}'
                config='{"xml": "<full-component-xml>...</full-component-xml>"}'

            clone - Clone a component with a new name:
                component_id="abc-123-def"
                config='{"name": "Cloned Component"}'
                config='{"name": "Cloned", "folder_name": "Test Folder"}'

            delete - Delete a component:
                component_id="abc-123-def"

        Returns:
            Action result with success status and component data
        """
        # Parse config JSON
        config_data = {}
        if config:
            try:
                config_data = json.loads(config)
            except (json.JSONDecodeError, TypeError) as e:
                return {"_success": False, "error": f"Invalid config (must be a JSON string): {e}"}
            if not isinstance(config_data, dict):
                return {"_success": False, "error": "config must be a JSON object, not " + type(config_data).__name__}

        try:
            subject = get_current_user()
            print(f"[INFO] manage_component called by user: {subject}, profile: {profile}, action: {action}")

            creds = get_secret(subject, profile)

            sdk_params = {
                "account_id": creds["account_id"],
                "username": creds["username"],
                "password": creds["password"],
                "timeout": 30000,
            }
            if creds.get("base_url"):
                sdk_params["base_url"] = creds["base_url"]
            sdk = Boomi(**sdk_params)

            params = {}
            if action == "create":
                params["config"] = config_data
            elif action == "update":
                params["component_id"] = component_id
                params["config"] = config_data
            elif action == "clone":
                params["component_id"] = component_id
                params["config"] = config_data
            elif action == "delete":
                params["component_id"] = component_id

            return manage_component_action(sdk, profile, action, **params)

        except Exception as e:
            print(f"[ERROR] Failed to {action} manage_component: {e}")
            return {"_success": False, "error": str(e)}

    print("[INFO] Component management tool registered successfully (1 consolidated tool)")


# --- Component Analysis MCP Tools ---
if analyze_component_action:
    @mcp.tool(annotations={"readOnlyHint": False, "openWorldHint": True})
    def analyze_component(
        profile: str,
        action: str,
        component_id: str = None,
        config: str = None,
    ):
        """
        Analyze component dependencies, compare versions, and merge.

        Args:
            profile: Boomi profile name (required)
            action: One of: where_used, dependencies, compare_versions, merge
            component_id: Component ID (required for all actions)
            config: JSON string with action-specific configuration

        Actions and config examples:

            where_used - Find all components that reference this component (inbound):
                component_id="abc-123-def"
                config='{"type": "DEPENDENT"}'  (optional: filter — DEPENDENT or INDEPENDENT)

            dependencies - Find all components this component references (outbound):
                component_id="abc-123-def"
                config='{"type": "DEPENDENT"}'  (optional: filter — DEPENDENT or INDEPENDENT)

            compare_versions - Compare two versions of a component:
                component_id="abc-123-def"
                config='{"source_version": 1, "target_version": 2}'

            merge - Merge component content across branches or versions:
                Branch merge: config='{"source_branch": "branch-id", "target_branch": "branch-id"}'
                Version merge: config='{"source_version": 1, "target_version": 2}'

        Notes:
            - where_used and dependencies show immediate references only (one level)
            - compare_versions requires both version numbers to exist for the component
            - merge supports branch-aware merging (componentId~branchId) or version-based merging
            - Use manage_account action="list_branches" to find branch IDs
            - Use query_components action="get" to find a component's current version

        Returns:
            Action result with success status and analysis data
        """
        # Parse config JSON
        config_data = {}
        if config:
            try:
                config_data = json.loads(config)
            except (json.JSONDecodeError, TypeError) as e:
                return {"_success": False, "error": f"Invalid config (must be a JSON string): {e}"}
            if not isinstance(config_data, dict):
                return {"_success": False, "error": "config must be a JSON object, not " + type(config_data).__name__}

        try:
            subject = get_current_user()
            print(f"[INFO] analyze_component called by user: {subject}, profile: {profile}, action: {action}")

            creds = get_secret(subject, profile)

            sdk_params = {
                "account_id": creds["account_id"],
                "username": creds["username"],
                "password": creds["password"],
                "timeout": 30000,
            }
            if creds.get("base_url"):
                sdk_params["base_url"] = creds["base_url"]
            sdk = Boomi(**sdk_params)

            params = {}
            if action == "where_used":
                params["component_id"] = component_id
                if config_data:
                    params["filters"] = config_data
            elif action == "dependencies":
                params["component_id"] = component_id
                if config_data:
                    params["filters"] = config_data
            elif action == "compare_versions":
                params["component_id"] = component_id
                params["config"] = config_data
            elif action == "merge":
                params["component_id"] = component_id
                params["config"] = config_data

            return analyze_component_action(sdk, profile, action, **params)

        except Exception as e:
            print(f"[ERROR] Failed to {action} analyze_component: {e}")
            return {"_success": False, "error": str(e)}

    print("[INFO] Component analysis tool registered successfully (1 consolidated tool)")


# --- Connector MCP Tools ---
if manage_connector_action:
    @mcp.tool(annotations={"openWorldHint": True})
    def manage_connector(
        profile: str,
        action: str,
        component_id: str = None,
        config: str = None,
    ):
        """Manage connector components (connections and operations) with catalog discovery.

        Args:
            profile: Boomi profile name (required)
            action: One of: list_types, get_type, list, get, create, update, delete
            component_id: Component ID (required for get, update, delete)
            config: JSON string with action-specific configuration

        Actions and config examples:

            list_types - List available connector types in the Boomi account:
                (no config needed)

            get_type - Get field definitions for a connector type:
                config='{"connector_type": "http"}'

            list - List connector components (connections and/or operations):
                config='{"component_type": "connection"}'
                config='{"component_type": "operation", "connector_type": "http"}'
                config='{"connector_type": "database"}'
                component_type values: "connection" (connector-settings) or "operation" (connector-action)

            get - Get a connector component with full XML:
                component_id="abc-123-def"

            create - Create new connector (builder or raw XML):
                config='{"connector_type": "http", "component_name": "My HTTP", "url": "https://api.example.com", "auth_type": "NONE"}'
                config='{"connector_type": "http", "component_name": "OAuth API", "url": "https://api.example.com", "auth_type": "OAUTH2", "oauth2_token_url": "https://auth.example.com/token", "oauth2_client_id": "my-client"}'
                config='{"xml": "<bns:Component ...>...</bns:Component>"}'

            update - Update existing connector:
                component_id="abc-123-def", config='{"url": "https://new-url.com"}'
                component_id="abc-123-def", config='{"name": "Renamed Connection", "auth_type": "BASIC"}'
                component_id="abc-123-def", config='{"xml": "<full-component-xml>...</full-component-xml>"}'

            delete - Delete connector:
                component_id="abc-123-def"

        Returns:
            Action result with success status and connector data
        """
        # Parse config JSON
        config_data = {}
        if config:
            try:
                config_data = json.loads(config)
            except (json.JSONDecodeError, TypeError) as e:
                return {"_success": False, "error": f"Invalid config (must be a JSON string): {e}"}
            if not isinstance(config_data, dict):
                return {"_success": False, "error": "config must be a JSON object, not " + type(config_data).__name__}

        try:
            subject = get_current_user()
            print(f"[INFO] manage_connector called by user: {subject}, profile: {profile}, action: {action}")

            creds = get_secret(subject, profile)

            sdk_params = {
                "account_id": creds["account_id"],
                "username": creds["username"],
                "password": creds["password"],
                "timeout": 30000,
            }
            if creds.get("base_url"):
                sdk_params["base_url"] = creds["base_url"]
            sdk = Boomi(**sdk_params)

            params = {}
            if action == "list_types":
                pass  # no extra params
            elif action == "get_type":
                params["connector_type"] = config_data.get("connector_type")
            elif action == "list":
                params["filters"] = config_data if config_data else None
            elif action == "get":
                params["component_id"] = component_id
            elif action == "create":
                params["config"] = config_data if config_data else None
            elif action == "update":
                params["component_id"] = component_id
                params["config"] = config_data if config_data else None
            elif action == "delete":
                params["component_id"] = component_id

            return manage_connector_action(sdk, profile, action, **params)

        except Exception as e:
            print(f"[ERROR] Failed to {action} manage_connector: {e}")
            return {"_success": False, "error": str(e)}

    print("[INFO] Connector tool registered successfully (1 consolidated tool)")


# --- Integration Builder MCP Tool ---
if build_integration_action:
    @mcp.tool(annotations={"openWorldHint": True})
    def build_integration(
        profile: str,
        action: str,
        config: str = None,
    ):
        """Build Boomi integrations from component-oriented JSON specs.

        Args:
            profile: Boomi profile name (required)
            action: One of: plan, apply, verify
            config: JSON string with action-specific payload

        Actions:
            plan:
                - Normalize and validate integration spec
                - Build deterministic execution order
                - Resolve current component existence for conflict handling
                Example:
                    config='{"mode":"lift_shift","components":[{"key":"conn","type":"connector-settings","name":"HTTP API","action":"create","config":{"connector_type":"http","component_name":"HTTP API","url":"https://api.example.com"}}]}'

            apply:
                - Execute plan in dependency order
                - Honors conflict_policy: reuse | clone | fail
                - dry_run defaults to true
                Example:
                    config='{"dry_run":false,"conflict_policy":"reuse","integration_spec":{"name":"Order Sync","mode":"lift_shift","components":[...]}}'

            verify:
                - Verify components exist and declared dependencies were resolved
                Example:
                    config='{"build_id":"<uuid-from-apply>"}'
        """
        config_data = {}
        if config:
            try:
                config_data = json.loads(config)
            except (json.JSONDecodeError, TypeError) as e:
                return {"_success": False, "error": f"Invalid config (must be a JSON string): {e}"}
            if not isinstance(config_data, dict):
                return {"_success": False, "error": "config must be a JSON object, not " + type(config_data).__name__}

        try:
            subject = get_current_user()
            print(f"[INFO] build_integration called by user: {subject}, profile: {profile}, action: {action}")

            creds = get_secret(subject, profile)
            sdk_params = {
                "account_id": creds["account_id"],
                "username": creds["username"],
                "password": creds["password"],
                "timeout": 30000,
            }
            if creds.get("base_url"):
                sdk_params["base_url"] = creds["base_url"]
            sdk = Boomi(**sdk_params)

            return build_integration_action(sdk, profile, action, config=config_data)

        except Exception as e:
            print(f"[ERROR] Failed to {action} build_integration: {e}")
            return {"_success": False, "error": str(e)}

    print("[INFO] Integration builder tool registered successfully")


# --- Folder Management MCP Tools ---
if manage_folders_action:
    @mcp.tool()
    def manage_folders(
        profile: str,
        action: str,
        folder_id: str = None,
        config: str = None,
    ):
        """
        Manage folder hierarchy for organizing Boomi components.

        Args:
            profile: Boomi profile name (required)
            action: One of: list, get, create, move, delete, restore, contents
            folder_id: Folder ID (required for get, delete, restore, contents)
            config: JSON string with action-specific configuration (see examples below)

        Actions and config examples:

            list - List all folders with tree view:
                config='{"include_deleted": true}'
                config='{"folder_name": "Production"}'
                config='{"folder_path": "Production/APIs"}'

            get - Get folder by ID (no config needed):
                folder_id="abc-123-def"

            create - Create folder or hierarchy:
                config='{"folder_name": "Production/APIs/v2"}'
                config='{"folder_name": "NewFolder", "parent_folder_id": "abc-123"}'

            move - Move a component to a folder:
                config='{"component_id": "comp-123", "target_folder_id": "folder-456"}'

            delete - Delete an empty folder:
                folder_id="abc-123-def"

            restore - Restore a deleted folder:
                folder_id="abc-123-def"

            contents - List components and sub-folders in a folder:
                folder_id="abc-123-def"
                config='{"folder_name": "Production"}'

        Returns:
            Action result with success status and data/error
        """
        # Parse config JSON
        config_data = {}
        if config:
            try:
                config_data = json.loads(config)
            except (json.JSONDecodeError, TypeError) as e:
                return {"_success": False, "error": f"Invalid config (must be a JSON string): {e}"}
            if not isinstance(config_data, dict):
                return {"_success": False, "error": "config must be a JSON object, not " + type(config_data).__name__}

        try:
            subject = get_current_user()
            print(f"[INFO] manage_folders called by user: {subject}, profile: {profile}, action: {action}")

            creds = get_secret(subject, profile)

            sdk_params = {
                "account_id": creds["account_id"],
                "username": creds["username"],
                "password": creds["password"],
                "timeout": 30000,
            }
            if creds.get("base_url"):
                sdk_params["base_url"] = creds["base_url"]
            sdk = Boomi(**sdk_params)

            params = {}
            if folder_id:
                params["folder_id"] = folder_id

            return manage_folders_action(sdk, profile, action, config_data=config_data, **params)

        except Exception as e:
            print(f"[ERROR] Failed to {action} manage_folders: {e}")
            import traceback
            traceback.print_exc()
            return {"_success": False, "error": str(e), "exception_type": type(e).__name__}

    print("[INFO] Folder management tool registered successfully (1 consolidated tool)")


# --- Schema Template MCP Tool ---
if get_schema_template_action:
    @mcp.tool(annotations={"readOnlyHint": True})
    def get_schema_template(
        resource_type: str,
        operation: str = None,
        standard: str = None,
        component_type: str = None,
        protocol: str = None,
    ):
        """Get JSON template and enum values for constructing tool requests.

        Returns example payloads, required/optional fields, and valid enum values.
        No API calls — pure reference data. Use before create/update operations.

        Args:
            resource_type: One of: trading_partner, process, integration, component, environment, package, execution_request, organization, folder, monitoring
            operation: Optional action context: create, update, list, execute, search, clone, compare_versions, execution_records, execution_logs, execution_artifacts, audit_logs, events
            standard: For trading_partner create: x12, edifact, hl7, rosettanet, tradacoms, odette, custom
            component_type: For component: process, connector-settings, transform.map, etc.
            protocol: For trading_partner protocols: http, as2, ftp, sftp, disk, mllp, oftp

        Examples:
            get_schema_template("trading_partner") → overview of all actions/standards
            get_schema_template("trading_partner", "create", standard="x12") → X12 create template
            get_schema_template("trading_partner", protocol="as2") → AS2 protocol fields
            get_schema_template("process", "create") → JSON process template
            get_schema_template("integration", "plan") → IntegrationSpecV1 planner template
            get_schema_template("component") → overview of component tools
            get_schema_template("monitoring", "execution_records") → execution query template
            get_schema_template("organization", "create") → organization create template
        """
        return get_schema_template_action(
            resource_type=resource_type,
            operation=operation,
            standard=standard,
            component_type=component_type,
            protocol=protocol,
        )

    print("[INFO] Schema template tool registered successfully")


# --- Generic API Invoker MCP Tool ---
if invoke_api:
    @mcp.tool()
    def invoke_boomi_api(
        profile: str,
        endpoint: str,
        method: str = "GET",
        payload: str = None,
        content_type: str = "json",
        accept: str = "json",
        confirm_delete: bool = False,
    ):
        """Direct Boomi API access for operations not covered by other tools.

        Generic escape hatch for any Boomi REST API endpoint.

        Args:
            profile: Boomi profile name (required for authentication)
            endpoint: API endpoint path (appended to base URL).
                      Format: "Resource" or "Resource/id" or "Resource/query"
            method: HTTP method - GET, POST, PUT, DELETE
            payload: JSON string request body for POST/PUT (parsed and sent as-is)
            content_type: Request body format - "json" (default) or "xml"
            accept: Response format - "json" (default) or "xml"
            confirm_delete: Set to true to confirm DELETE operations (safety gate)

        Common endpoints:
            # Roles — see /mnt/examples/04_environment_setup/manage_roles.py
            - "Role/query" POST with filter body → list roles
            - "Role" POST with role body → create role
            - "Role/{id}" GET → get role
            - "Role/{id}" DELETE → delete role

            # Branches — see /mnt/examples/02_organize_structure/manage_branches.py
            - "Branch/query" POST with filter body → list branches
            - "Branch" POST → create branch
            - "Branch/{id}" DELETE → delete branch

            # Folders — see /mnt/examples/02_organize_structure/manage_folders.py
            - "Folder/query" POST with filter body → list folders
            - "Folder" POST → create folder
            - "Folder/{id}" GET → get folder

            # Shared Web Server
            - "SharedWebServer/{id}" GET → get web server config

            # Persisted Properties (async — returns token, poll for result)
            - "PersistedProcessProperties/{atomId}" GET → initiate async get

            # Queues (async)
            - "ListQueues/{atomId}" GET → initiate async queue list
            - "ClearQueue" POST → clear queue messages
            - "MoveQueue" POST → move messages between queues

            # Secrets
            - "SecretsManagerRefreshRequest" POST → refresh secrets

            # Packages & Deployments
            - "PackagedComponent/query" POST → list packages
            - "DeployedPackage/query" POST → list deployments
            - "DeployedPackage" POST → deploy package

        Examples:
            # List all roles
            invoke_boomi_api(profile="prod", endpoint="Role/query",
                method="POST", payload='{"QueryFilter":{"expression":{"operator":"EQUALS","property":"name","argument":["Administrator"]}}}')

            # Get a specific folder
            invoke_boomi_api(profile="prod", endpoint="Folder/12345", method="GET")

            # Get component as XML
            invoke_boomi_api(profile="prod", endpoint="Component/abc-123",
                method="GET", accept="xml")
        """
        try:
            subject = get_current_user()
            creds = get_secret(subject, profile)

            sdk_params = {
                "account_id": creds["account_id"],
                "username": creds["username"],
                "password": creds["password"],
                "timeout": 30000,
            }
            if creds.get("base_url"):
                sdk_params["base_url"] = creds["base_url"]

            sdk = Boomi(**sdk_params)

            return invoke_api(
                boomi_client=sdk,
                profile=profile,
                endpoint=endpoint,
                method=method,
                payload=payload,
                content_type=content_type,
                accept=accept,
                confirm_delete=confirm_delete,
            )

        except Exception as e:
            return {"_success": False, "error": str(e)}

    print("[INFO] Generic API invoker tool registered successfully")


# --- List Capabilities ---
if list_capabilities_action:
    @mcp.tool(annotations={"readOnlyHint": True})
    def list_capabilities():
        """List all available MCP tools and their capabilities.

        Returns summary of:
        - All available tools with descriptions and actions
        - Implementation status (which tools are ready)
        - Workflow suggestions for common multi-step tasks
        - Coverage statistics (SDK example coverage)
        - Quick-start hints

        No parameters needed. Call this to understand what operations are possible.

        Helps AI agent:
        - Select the right tool for any user request
        - Understand multi-step workflows (e.g., create → package → deploy → monitor)
        - Know when to use invoke_boomi_api for uncovered APIs
        - Find the right get_schema_template call before creating resources
        """
        try:
            return list_capabilities_action()
        except Exception as e:
            return {"_success": False, "error": str(e)}

    print("[INFO] List capabilities tool registered successfully")


# --- Environment Management MCP Tools ---
if manage_environments_action:
    @mcp.tool()
    def manage_environments(
        profile: str,
        action: str,
        resource_id: str = None,
        config: str = None,
    ):
        """
        Manage Boomi environments and their configuration extensions.

        Args:
            profile: Boomi profile name (required)
            action: One of: list, get, create, update, delete, get_extensions, update_extensions, query_extensions, stats, get_properties, update_properties
            resource_id: Environment ID (required for get, update, delete, get_extensions, update_extensions, query_extensions)
            config: JSON string with action-specific configuration (see examples below)

        Actions and config examples:

            list - List all environments, optional filters:
                config='{"classification": "PROD"}'
                config='{"name_pattern": "%test%"}'

            get - Get environment by ID (no config needed):
                resource_id="abc-123-def"

            create - Create new environment:
                config='{"name": "My Test Env", "classification": "TEST"}'

            update - Update environment name:
                resource_id="abc-123-def"
                config='{"name": "Renamed Environment"}'

            delete - Delete environment (permanent!):
                resource_id="abc-123-def"

            get_extensions - Get environment config overrides (returns ALL override points including defaults):
                resource_id="abc-123-def"
                Returns all deployed extension entries. Fields with useDefault=true are available
                override points that still use the component default. Use the returned IDs verbatim
                in update_extensions.

            update_extensions - Update environment extensions (performs verification read after update):
                resource_id="abc-123-def"
                config='{"extensions": {"connections": {"connection": [{"id": "<componentId>", "field": [{"id": "host", "value": "myhost", "useDefault": false}]}]}}}'
                Full workflow: get_extensions → copy IDs → update_extensions → verify with get_extensions.
                The tool performs an automatic verification GET after each update to confirm persistence.

            query_extensions - Check if environment has extensions:
                resource_id="abc-123-def"

            stats - Environment summary by classification (no params needed)

            get_properties - Get persisted process properties for a runtime (async):
                resource_id="<atom_id>"

            update_properties - Update persisted process properties:
                resource_id="<atom_id>"
                config='{"properties": {"Process": [{"processId": "<proc_id>", "ProcessProperties": {"ProcessProperty": [{"ProcessPropertyValue": [{"key": "prop1", "value": "val1"}]}]}}]}}'

        Classification values: TEST, PROD
        Note: Classification is immutable after creation. Only name can be updated.

        Extension types (8 total):
            connections, operations, properties, cross_references,
            trading_partners, pgp_certificates, process_properties, data_maps

        Extension lifecycle (3 phases):
            1. DEFINE: Mark components as extensible in process canvas (Extensions dialog).
               Stored in process component XML — handled by manage_process / manage_component.
            2. DEPLOY: Package and deploy process to an environment. Extension entries are
               auto-generated from deployed process XML (no API to create/delete them).
            3. CONFIGURE (this tool): Use get_extensions to read current values, update_extensions
               to override field values per environment (e.g., connection URLs, credentials).

        update_extensions caveats:
            - partial=true (default): merges only the extension types you send; others are untouched.
            - partial=false: replaces ALL extensions — any type omitted is reset to defaults.
            - Encrypted fields (passwords, tokens) are returned as empty strings on read.
              To preserve them, omit them from the update payload or resend the actual value.
            - Each extension item supports a "useDefault" attribute (true/false) to control
              whether it uses the component default or the environment-level override.

        Returns:
            Action result with success status and data/error
        """
        # Parse config JSON
        config_data = {}
        if config:
            try:
                config_data = json.loads(config)
            except (json.JSONDecodeError, TypeError) as e:
                return {"_success": False, "error": f"Invalid config (must be a JSON string): {e}"}
            if not isinstance(config_data, dict):
                return {"_success": False, "error": "config must be a JSON object, not " + type(config_data).__name__}

        try:
            subject = get_current_user()
            print(f"[INFO] manage_environments called by user: {subject}, profile: {profile}, action: {action}")

            creds = get_secret(subject, profile)

            sdk_params = {
                "account_id": creds["account_id"],
                "username": creds["username"],
                "password": creds["password"],
                "timeout": 30000,
            }
            if creds.get("base_url"):
                sdk_params["base_url"] = creds["base_url"]
            sdk = Boomi(**sdk_params)

            params = {}
            if resource_id:
                params["resource_id"] = resource_id

            return manage_environments_action(sdk, profile, action, config_data=config_data, **params)

        except Exception as e:
            print(f"[ERROR] Failed to {action} manage_environments: {e}")
            import traceback
            traceback.print_exc()
            return {"_success": False, "error": str(e), "exception_type": type(e).__name__}

    print("[INFO] Environment management tool registered successfully (1 consolidated tool)")


# --- Runtime Management MCP Tools ---
if manage_runtimes_action:
    @mcp.tool()
    def manage_runtimes(
        profile: str,
        action: str,
        resource_id: str = None,
        environment_id: str = None,
        config: str = None,
    ):
        """Manage Boomi runtimes (Atoms, Molecules, Clouds), attachments, and provisioning.

        Args:
            profile: Boomi profile name (required)
            action: One of: list, get, create, update, delete, attach, detach, list_attachments, restart, configure_java, create_installer_token, available_clouds, cloud_list, cloud_get, cloud_create, cloud_update, cloud_delete, diagnostics
            resource_id: Runtime ID (most actions) or attachment ID (detach) or cloud ID (cloud_get, cloud_update, cloud_delete)
            environment_id: Environment ID (for attach, detach, list_attachments)
            config: JSON string with action-specific parameters

        Actions and config examples:

            list - List all runtimes, optional filters:
                config='{"runtime_type": "ATOM"}'
                config='{"status": "ONLINE"}'
                config='{"name_pattern": "%prod%"}'

            get - Get runtime by ID (no config needed):
                resource_id="abc-123-def"

            create - Create a cloud attachment (requires cloud_id):
                config='{"name": "My Cloud Atom", "cloud_id": "abc-123-def"}'
                Optional: purge_history_days, force_restart_time
                Use available_clouds to find Boomi-managed cloud IDs, or cloud_list for private clouds.
                Note: Local atoms cannot be created via API — use create_installer_token instead.

            update - Update runtime name:
                resource_id="abc-123-def"
                config='{"name": "Production Atom"}'

            delete - Delete runtime (permanent!):
                resource_id="abc-123-def"

            attach - Attach runtime to environment:
                resource_id="abc-123-def"           (runtime_id)
                environment_id="env-456-ghi"

            detach - Detach runtime from environment:
                resource_id="attachment-789-jkl"    (attachment_id)
                OR:
                resource_id="abc-123-def"           (runtime_id)
                environment_id="env-456-ghi"        (auto-lookup attachment_id)

            list_attachments - List environment-runtime attachments:
                environment_id="env-456-ghi"        (all runtimes in this env)
                resource_id="abc-123-def"           (all envs for this runtime)
                (neither = list all attachments)

            restart - Restart runtime:
                resource_id="abc-123-def"

            configure_java - Upgrade or rollback Java:
                resource_id="abc-123-def"
                config='{"java_action": "upgrade", "target_version": "17"}'
                config='{"java_action": "rollback"}'

            create_installer_token - Create installer token for local runtime:
                config='{"install_type": "ATOM", "duration_minutes": 120}'

            available_clouds - List Boomi-managed public clouds (PCS/DCS/MCS) your account can attach to:
                (no config needed - lists all available clouds)
                config='{"name_pattern": "%US%"}'
                Use these cloud IDs with action='create' to create cloud attachments.

            cloud_list - List private runtime clouds your account owns (requires Cloud Management privilege):
                config='{"classification": "PROD"}'
                For accounts that manage their own runtime clouds (enterprise feature).

            cloud_get - Get private runtime cloud by ID:
                resource_id="abc-123-def"

            cloud_create - Create private runtime cloud:
                config='{"name": "My Cloud", "classification": "PROD"}'
                config='{"name": "Test Cloud", "classification": "TEST", "allow_deployments": true, "allow_browsing": true, "allow_test_executions": true}'

            cloud_update - Update private runtime cloud:
                resource_id="abc-123-def"
                config='{"name": "Renamed Cloud", "allow_deployments": false}'

            cloud_delete - Delete private runtime cloud (permanent!):
                resource_id="abc-123-def"

            diagnostics - Get combined runtime diagnostics (counters, disk space, listener status):
                resource_id="<atom_id>"

        Runtime types: ATOM, MOLECULE, CLOUD
        Install types: ATOM, MOLECULE, CLOUD, BROKER, GATEWAY
        Cloud classifications: PROD, TEST (immutable after creation)
        Java versions: 8, 11, 17, 21

        Returns:
            Action result with success status and data/error
        """
        # Parse config JSON
        config_data = {}
        if config:
            try:
                config_data = json.loads(config)
            except (json.JSONDecodeError, TypeError) as e:
                return {"_success": False, "error": f"Invalid config (must be a JSON string): {e}"}
            if not isinstance(config_data, dict):
                return {"_success": False, "error": "config must be a JSON object, not " + type(config_data).__name__}

        try:
            subject = get_current_user()
            print(f"[INFO] manage_runtimes called by user: {subject}, profile: {profile}, action: {action}")

            creds = get_secret(subject, profile)

            sdk_params = {
                "account_id": creds["account_id"],
                "username": creds["username"],
                "password": creds["password"],
                "timeout": 30000,
            }
            if creds.get("base_url"):
                sdk_params["base_url"] = creds["base_url"]
            sdk = Boomi(**sdk_params)

            params = {}
            if resource_id:
                params["resource_id"] = resource_id
            if environment_id:
                params["environment_id"] = environment_id

            return manage_runtimes_action(sdk, profile, action, config_data=config_data, **params)

        except Exception as e:
            print(f"[ERROR] Failed to {action} manage_runtimes: {e}")
            import traceback
            traceback.print_exc()
            return {"_success": False, "error": str(e), "exception_type": type(e).__name__}

    print("[INFO] Runtime management tool registered successfully (1 consolidated tool)")


# --- Deployment Management MCP Tools ---
if manage_deployment_action:
    @mcp.tool()
    def manage_deployment(
        profile: str,
        action: str,
        package_id: str = None,
        environment_id: str = None,
        config: str = None,
    ):
        """Manage deployment packages and deploy to environments.

        Args:
            profile: Boomi profile name (required)
            action: One of: list_packages, get_package, create_package, delete_package,
                    deploy, undeploy, list_deployments, get_deployment
            package_id: Package ID (get/delete/deploy) or deployment ID (undeploy/get_deployment)
            environment_id: Target environment (deploy) or filter (list_deployments)
            config: JSON string with action-specific parameters

        RECOMMENDED WORKFLOW:
          1. Create a package: action="create_package", config='{"component_id":"...", "component_type":"process", "package_version":"1.0"}'
          2. Deploy to env: action="deploy", package_id="<from step 1>", environment_id="..."
          3. Promote to prod: action="deploy", package_id="<same>", environment_id="<prod-env-id>"

        Actions and config examples:

            list_packages - List all packages, optional component filter:
                config='{"component_id": "abc-123"}'

            get_package - Get package details:
                package_id="pkg-123"

            create_package - Create versioned package from component:
                config='{"component_id": "abc-123", "component_type": "process", "package_version": "1.0.0"}'
                config='{"component_id": "abc-123", "component_type": "process", "package_version": "2.0", "notes": "Release", "branch_name": "main"}'
                component_type: process, certificate, customlibrary, flowservice, processroute, tpgroup, webservice

            delete_package - Delete package (fails if deployed):
                package_id="pkg-123"

            deploy - Deploy package to environment:
                package_id="pkg-123"
                environment_id="env-456"
                config='{"listener_status": "RUNNING", "notes": "Production deploy"}'

            undeploy - Remove deployment from environment:
                package_id="deployment-789"   (this is the deployment_id)

            list_deployments - List deployments with optional filters:
                environment_id="env-456"
                config='{"package_id": "pkg-123", "active_only": true}'

            get_deployment - Get deployment details:
                package_id="deployment-789"   (this is the deployment_id)

        Listener statuses: RUNNING, PAUSED

        Returns:
            Action result with success status and data/error
        """
        # Parse config JSON
        config_data = {}
        if config:
            try:
                config_data = json.loads(config)
            except (json.JSONDecodeError, TypeError) as e:
                return {"_success": False, "error": f"Invalid config (must be a JSON string): {e}"}
            if not isinstance(config_data, dict):
                return {"_success": False, "error": "config must be a JSON object, not " + type(config_data).__name__}

        try:
            subject = get_current_user()
            print(f"[INFO] manage_deployment called by user: {subject}, profile: {profile}, action: {action}")

            creds = get_secret(subject, profile)

            sdk_params = {
                "account_id": creds["account_id"],
                "username": creds["username"],
                "password": creds["password"],
                "timeout": 30000,
            }
            if creds.get("base_url"):
                sdk_params["base_url"] = creds["base_url"]
            sdk = Boomi(**sdk_params)

            params = {}
            if package_id:
                params["package_id"] = package_id
            if environment_id:
                params["environment_id"] = environment_id

            return manage_deployment_action(sdk, profile, action, config_data=config_data, **params)

        except Exception as e:
            print(f"[ERROR] manage_deployment {action} failed: {e}")
            import traceback
            traceback.print_exc()
            return {"_success": False, "error": str(e), "exception_type": type(e).__name__}

    print("[INFO] Deployment management tool registered successfully")


# ============================================================
# execute_process — Execute a Boomi process on a runtime
# ============================================================
if execute_process_action:
    @mcp.tool(annotations={"destructiveHint": True, "openWorldHint": True})
    def execute_process(
        profile: str,
        process_id: str,
        environment_id: str,
        atom_id: str = None,
        config: str = None,
    ):
        """Execute a Boomi process on a runtime.

        Args:
            profile: Boomi profile name (required)
            process_id: Process component ID to execute (required)
            environment_id: Environment to execute in (required)
            atom_id: Runtime ID (auto-detected if only one attached to environment)
            config: JSON string with optional parameters

        RECOMMENDED WORKFLOW:
          1. execute_process(...) → returns request_id
          2. monitor_platform(action="execution_records", config='{"execution_id":"<request_id>"}')
          3. If ERROR: monitor_platform(action="execution_logs", config='{"execution_id":"<request_id>"}')

        OR use wait mode to poll automatically:
          execute_process(..., config='{"wait": true}')

        Examples:

            Basic (fire and forget):
                process_id="abc-123", environment_id="env-456"

            Wait for completion (up to 5 min):
                process_id="abc-123", environment_id="env-456",
                config='{"wait": true}'

            Wait with custom timeout (120 seconds):
                process_id="abc-123", environment_id="env-456",
                config='{"wait": true, "timeout": 120}'

            With dynamic properties:
                process_id="abc-123", environment_id="env-456",
                config='{"dynamic_properties": {"inputFile": "/data/orders.csv"}}'

        Config fields:
            wait: If true, poll execution_record until completion or timeout (default: false)
            timeout: Max seconds to wait when wait=true (default: 300)
            dynamic_properties: Dict of key-value pairs (e.g. {"key": "value"})
            process_properties: Dict of component overrides
                Format: {"componentId": {"key": "value", ...}}

        Returns:
            request_id (and execution_result with status details when wait=true)
        """
        config_data = {}
        if config:
            try:
                config_data = json.loads(config)
            except (json.JSONDecodeError, TypeError) as e:
                return {"_success": False, "error": f"Invalid config JSON: {e}"}
            if not isinstance(config_data, dict):
                return {"_success": False, "error": "config must be a JSON object"}

        try:
            subject = get_current_user()
            print(f"[INFO] execute_process called by user: {subject}, profile: {profile}")
            creds = get_secret(subject, profile)

            sdk_params = {
                "account_id": creds["account_id"],
                "username": creds["username"],
                "password": creds["password"],
                "timeout": 30000,
            }
            if creds.get("base_url"):
                sdk_params["base_url"] = creds["base_url"]
            sdk = Boomi(**sdk_params)

            return execute_process_action(
                sdk, profile, process_id, environment_id,
                atom_id=atom_id, config_data=config_data,
            )

        except Exception as e:
            print(f"[ERROR] execute_process failed: {e}")
            import traceback
            traceback.print_exc()
            return {"_success": False, "error": str(e), "exception_type": type(e).__name__}

    print("[INFO] Execute process tool registered successfully")


# ============================================================
# troubleshoot_execution — Troubleshoot failed executions
# ============================================================
if troubleshoot_execution_action:
    @mcp.tool(annotations={"destructiveHint": True, "openWorldHint": True})
    def troubleshoot_execution(
        profile: str,
        action: str,
        execution_id: str = None,
        process_id: str = None,
        environment_id: str = None,
        config: str = None,
    ):
        """Troubleshoot failed executions: get error details, retry, reprocess documents, manage queues.

        Actions: error_details, retry, reprocess, list_queues, clear_queue, move_queue

        Args:
            profile: Boomi profile name (required)
            action: Action to perform (required)
            execution_id: Execution ID (for error_details, retry, reprocess)
            process_id: Process ID (for error_details filtering, reprocess)
            environment_id: Environment ID (for reprocess when no atom_id)
            config: JSON string with action-specific parameters

        Action details:

            error_details — Get error info from failed executions
                execution_id: specific execution to analyze
                process_id: filter recent errors by process
                config: {"days": 7, "limit": 10, "fetch_logs": true, "log_level": "ALL"}

            retry — Retry a failed execution with same process/atom
                execution_id: required - the failed execution to retry
                config: {"dynamic_properties": {"key": "value"}}

            reprocess — Re-execute a process (from failed execution or fresh)
                execution_id: get process_id/atom_id from failed execution
                process_id + environment_id: specify directly
                config: {"atom_id": "...", "dynamic_properties": {"key": "value"}}

            list_queues — List all queues for a runtime (async)
                config: {"atom_id": "required", "timeout": 60}

            clear_queue — Clear messages from a stuck queue
                config: {"atom_id": "required", "queue_name": "required", "dlq": false, "subscriber_name": "..."}

            move_queue — Move messages between queues
                config: {"atom_id": "required", "source_queue": "required", "dest_queue": "required",
                         "source_dlq": false, "dest_dlq": false, "source_subscriber": "...", "dest_subscriber": "..."}
        """
        cfg = {}
        if config:
            try:
                cfg = json.loads(config)
            except (json.JSONDecodeError, TypeError) as e:
                return {"_success": False, "error": f"Invalid config JSON: {e}"}
            if not isinstance(cfg, dict):
                return {"_success": False, "error": "config must be a JSON object"}

        try:
            subject = get_current_user()
            print(f"[INFO] troubleshoot_execution called by user: {subject}, profile: {profile}, action: {action}")
            creds = get_secret(subject, profile)

            sdk_params = {
                "account_id": creds["account_id"],
                "username": creds["username"],
                "password": creds["password"],
                "timeout": 30000,
            }
            if creds.get("base_url"):
                sdk_params["base_url"] = creds["base_url"]
            sdk = Boomi(**sdk_params)

            return troubleshoot_execution_action(
                sdk, action,
                execution_id=execution_id,
                process_id=process_id,
                environment_id=environment_id,
                config=cfg,
            )

        except Exception as e:
            print(f"[ERROR] troubleshoot_execution failed: {e}")
            import traceback
            traceback.print_exc()
            return {"_success": False, "error": str(e), "exception_type": type(e).__name__}

    print("[INFO] Troubleshoot execution tool registered successfully")


# --- Shared Resources Management MCP Tools ---
if manage_shared_resources_action:
    @mcp.tool()
    def manage_shared_resources(
        profile: str,
        action: str,
        resource_id: str = None,
        config: str = None,
    ):
        """Manage shared resources: web servers and communication channels on Boomi runtimes.

        Args:
            profile: Boomi profile name (required)
            action: One of: list_web_servers, update_web_server, list_channels, get_channel, create_channel
            resource_id: Atom ID (web server actions) or channel ID (get_channel)
            config: JSON string with action-specific parameters

        Actions and config examples:

            list_web_servers - Get shared web server config for an atom:
                resource_id="<atom_id>"

            update_web_server - Update web server settings:
                resource_id="<atom_id>"
                config='{"base_url": "/ws", "max_number_of_threads": 200}'
                General fields: base_url, api_type, external_host, internal_host,
                    ssl_certificate, max_number_of_threads, auth_type
                Port fields (applied to all ports, or specify port_index):
                    port, ssl, external_port, external_ssl, auth_type, enable_port

            list_channels - List shared communication channels:
                (no config needed - lists all channels)
                config='{"name_pattern": "%HTTP%"}'

            get_channel - Get a specific channel by ID:
                resource_id="<channel_id>"

            create_channel - Create a new communication channel:
                config='{"name": "My Channel", "channel_type": "HTTP"}'
                Optional: folder_id

        Returns:
            Action result with success status and data/error
        """
        config_data = {}
        if config:
            try:
                config_data = json.loads(config)
            except (json.JSONDecodeError, TypeError) as e:
                return {"_success": False, "error": f"Invalid config (must be a JSON string): {e}"}
            if not isinstance(config_data, dict):
                return {"_success": False, "error": "config must be a JSON object, not " + type(config_data).__name__}

        try:
            subject = get_current_user()
            print(f"[INFO] manage_shared_resources called by user: {subject}, profile: {profile}, action: {action}")

            creds = get_secret(subject, profile)

            sdk_params = {
                "account_id": creds["account_id"],
                "username": creds["username"],
                "password": creds["password"],
                "timeout": 30000,
            }
            if creds.get("base_url"):
                sdk_params["base_url"] = creds["base_url"]
            sdk = Boomi(**sdk_params)

            params = {}
            if resource_id:
                params["resource_id"] = resource_id

            return manage_shared_resources_action(sdk, profile, action, config_data=config_data, **params)

        except Exception as e:
            print(f"[ERROR] Failed to {action} manage_shared_resources: {e}")
            import traceback
            traceback.print_exc()
            return {"_success": False, "error": str(e), "exception_type": type(e).__name__}

    print("[INFO] Shared resources management tool registered successfully")


# --- Account Administration MCP Tools ---
if manage_account_action:
    @mcp.tool()
    def manage_account(
        profile: str,
        action: str,
        resource_id: str = None,
        config: str = None,
    ):
        """Manage Boomi account administration: roles and component branches.

        Args:
            profile: Boomi profile name (required)
            action: One of: list_roles, manage_role, list_branches, manage_branch
            resource_id: Role or Branch ID (required for get/update/delete operations)
            config: JSON string with action-specific configuration (see examples below)

        Actions and config examples:

            list_roles - List all roles, optional exact name filter:
                (no config needed — lists all)
                config='{"name": "Administrator"}'

            manage_role - Create, get, update, or delete a role:
                Create: config='{"operation": "create", "name": "API Developer", "description": "API access role", "privileges": ["API", "BUILD", "EXECUTE"]}'
                Get:    resource_id="role-id", config='{"operation": "get"}'
                Update: resource_id="role-id", config='{"operation": "update", "name": "New Name", "privileges": ["API", "EXECUTE"]}'
                Delete: resource_id="role-id", config='{"operation": "delete"}'

            list_branches - List all component branches, optional name filter:
                (no config needed — lists all)
                config='{"name_pattern": "%feature%"}'

            manage_branch - Create, get, or delete a branch:
                Create: config='{"operation": "create", "name": "feature-branch", "description": "New feature"}'
                Get:    resource_id="branch-id", config='{"operation": "get"}'
                Delete: resource_id="branch-id", config='{"operation": "delete"}'

        Common privileges: EXECUTE, VIEW_RESULT, BUILD, DEVELOPER, ACCOUNT_ADMIN,
            USER_MANAGEMENT, ENV_MANAGEMENT, ATOM_MANAGEMENT, DEPLOY, API

        Note: Branch functionality requires Enterprise accounts with Git integration.

        Returns:
            Action result with success status and data/error
        """
        # Parse config JSON
        config_data = {}
        if config:
            try:
                config_data = json.loads(config)
            except (json.JSONDecodeError, TypeError) as e:
                return {"_success": False, "error": f"Invalid config (must be a JSON string): {e}"}
            if not isinstance(config_data, dict):
                return {"_success": False, "error": "config must be a JSON object, not " + type(config_data).__name__}

        try:
            subject = get_current_user()
            print(f"[INFO] manage_account called by user: {subject}, profile: {profile}, action: {action}")

            creds = get_secret(subject, profile)

            sdk_params = {
                "account_id": creds["account_id"],
                "username": creds["username"],
                "password": creds["password"],
                "timeout": 30000,
            }
            if creds.get("base_url"):
                sdk_params["base_url"] = creds["base_url"]
            sdk = Boomi(**sdk_params)

            params = {}
            if resource_id:
                params["resource_id"] = resource_id

            return manage_account_action(sdk, profile, action, config_data=config_data, **params)

        except Exception as e:
            print(f"[ERROR] Failed to {action} manage_account: {e}")
            import traceback
            traceback.print_exc()
            return {"_success": False, "error": str(e), "exception_type": type(e).__name__}

    print("[INFO] Account management tool registered successfully (1 consolidated tool)")



# --- Schedule Management MCP Tools ---
# ============================================================
if manage_schedules_action:
    @mcp.tool()
    def manage_schedules(
        profile: str,
        action: str,
        resource_id: str = None,
        config: str = None,
    ):
        """Manage process schedules — when and how often Boomi processes run automatically.

        Args:
            profile: Boomi profile name (required)
            action: One of: list, get, update, delete
            resource_id: Schedule ID (base64-encoded, from list/get results)
            config: JSON string with action-specific parameters

        Actions and config examples:

            list - List all schedules, optional filters:
                (no config) — list all schedules
                config='{"process_id": "abc-123"}'
                config='{"atom_id": "def-456"}'

            get - Get specific schedule:
                resource_id="Q1BTMmQ0ZDVkYTQtMGRmZS00MWY4..."
                OR config='{"process_id": "abc-123", "atom_id": "def-456"}'

            update - Set schedule with cron expression:
                config='{"process_id": "abc-123", "atom_id": "def-456", "cron": "0 9 * * *"}'
                config='{"process_id": "abc-123", "atom_id": "def-456", "cron": "*/15 9-17 * * 1-5", "max_retry": 3}'

            delete - Clear/disable schedule (process stops running automatically):
                resource_id="Q1BTMmQ0ZDVkYTQtMGRmZS00MWY4..."
                OR config='{"process_id": "abc-123", "atom_id": "def-456"}'

        Cron format: minute hour day_of_month month day_of_week
            "0 9 * * *"         — Daily at 9:00 AM
            "*/15 * * * *"      — Every 15 minutes
            "0 */6 * * *"       — Every 6 hours
            "0 9 * * 1-5"       — Weekdays at 9:00 AM
            "*/30 9-17 * * 1-5" — Every 30 min during business hours

        Schedule ID: Base64-encoded from "CPS{atomId}:{processId}".
        Use list or get to discover schedule IDs.

        Note: Listener processes cannot be scheduled (returns 400).
        Requires Runtime Management + Scheduling privileges.

        Returns:
            Action result with success status and schedule data
        """
        config_data = {}
        if config:
            try:
                config_data = json.loads(config)
            except (json.JSONDecodeError, TypeError) as e:
                return {"_success": False, "error": f"Invalid config JSON: {e}"}
            if not isinstance(config_data, dict):
                return {"_success": False, "error": "config must be a JSON object"}

        try:
            subject = get_current_user()
            print(f"[INFO] manage_schedules called by user: {subject}, profile: {profile}, action: {action}")

            creds = get_secret(subject, profile)

            sdk_params = {
                "account_id": creds["account_id"],
                "username": creds["username"],
                "password": creds["password"],
                "timeout": 30000,
            }
            if creds.get("base_url"):
                sdk_params["base_url"] = creds["base_url"]
            sdk = Boomi(**sdk_params)

            params = {}
            if resource_id:
                params["resource_id"] = resource_id

            return manage_schedules_action(sdk, profile, action, config_data=config_data, **params)

        except Exception as e:
            print(f"[ERROR] Failed to {action} manage_schedules: {e}")
            import traceback
            traceback.print_exc()
            return {"_success": False, "error": str(e), "exception_type": type(e).__name__}

    print("[INFO] Schedule management tool registered successfully")


# --- Credential Management Tools (local dev only) ---
if LOCAL_MODE:
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
            subject = get_current_user()
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
            except ApiError as e:
                print(f"[ERROR] Credential validation failed: {e}")
                return {
                    "_success": False,
                    "error": f"Credential validation failed: {_extract_api_error_msg(e)}",
                    "_note": "Please check your account_id, username, and password"
                }
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
            subject = get_current_user()
            print(f"[INFO] delete_boomi_profile called for profile: {profile}")

            delete_profile(subject, profile)

            return {
                "_success": True,
                "message": f"Profile '{profile}' deleted successfully",
            }
        except Exception as e:
            print(f"[ERROR] Failed to delete profile: {e}")
            return {
                "_success": False,
                "error": str(e)
            }


# --- Web UI Routes (production only) ---
if not LOCAL_MODE:
    from starlette.responses import HTMLResponse, JSONResponse, RedirectResponse
    from starlette.requests import Request
    from starlette.middleware.sessions import SessionMiddleware
    import urllib.parse
    import httpx

    def generate_pkce_pair():
        """Generate PKCE code_verifier and code_challenge."""
        code_verifier = base64.urlsafe_b64encode(secrets.token_bytes(32)).decode('utf-8').rstrip('=')
        code_challenge = base64.urlsafe_b64encode(
            hashlib.sha256(code_verifier.encode('utf-8')).digest()
        ).decode('utf-8').rstrip('=')
        return code_verifier, code_challenge

    def get_authenticated_user(request: Request) -> Optional[str]:
        """Extract authenticated user from request (works with OAuth middleware and sessions)."""
        # Try session first (web portal authentication)
        # Use 'sub' (Google user ID) for consistency with MCP OAuth
        if hasattr(request, "session") and request.session.get("user_sub"):
            return request.session.get("user_sub")

        # Try request.state (FastMCP/Starlette OAuth pattern for MCP clients)
        if hasattr(request.state, "user"):
            user = request.state.user
            if isinstance(user, dict):
                return user.get("sub") or user.get("email")
            if hasattr(user, "sub"):
                return user.sub
            if hasattr(user, "email"):
                return user.email
            return str(user)

        # No authenticated user found
        return None

    @mcp.custom_route("/web/login", methods=["GET"])
    async def web_login(request: Request):
        """Initiate OAuth login with PKCE for web portal."""
        # Get Google OAuth configuration
        client_id = os.getenv("OIDC_CLIENT_ID")
        base_url = os.getenv("OIDC_BASE_URL", str(request.base_url).rstrip('/'))

        if not client_id:
            return JSONResponse({"error": "OAuth not configured"}, status_code=500)

        # Generate PKCE parameters
        code_verifier, code_challenge = generate_pkce_pair()
        state = secrets.token_urlsafe(32)

        # Store code_verifier and state in session
        request.session["oauth_state"] = state
        request.session["code_verifier"] = code_verifier

        print(f"[DEBUG] Stored in session: oauth_state={state[:20]}..., code_verifier={code_verifier[:20]}...")
        print(f"[DEBUG] Session after store: {dict(request.session)}")

        # Build Google OAuth authorization URL with PKCE
        redirect_uri = f"{base_url}/web/callback"
        auth_params = {
            "client_id": client_id,
            "response_type": "code",
            "scope": "openid email profile",
            "redirect_uri": redirect_uri,
            "state": state,
            "code_challenge": code_challenge,
            "code_challenge_method": "S256",
        }

        auth_url = "https://accounts.google.com/o/oauth2/v2/auth?" + urllib.parse.urlencode(auth_params)

        print(f"[INFO] Initiating OAuth login for web portal")
        print(f"[INFO] Redirect URI: {redirect_uri}")

        return RedirectResponse(auth_url)

    @mcp.custom_route("/web/callback", methods=["GET"])
    async def web_callback(request: Request):
        """Handle OAuth callback for web portal."""
        # Get parameters from callback
        code = request.query_params.get("code")
        state = request.query_params.get("state")
        error = request.query_params.get("error")

        print(f"[DEBUG] Callback received - state from URL: {state[:20] if state else 'None'}...")
        print(f"[DEBUG] Session contents: {dict(request.session)}")
        print(f"[DEBUG] Session ID: {id(request.session)}")
        print(f"[DEBUG] Has session attr: {hasattr(request, 'session')}")

        if error:
            return HTMLResponse(f"<html><body><h1>OAuth Error</h1><p>{error}</p></body></html>", status_code=400)

        if not code or not state:
            return HTMLResponse("<html><body><h1>OAuth Error</h1><p>Missing code or state</p></body></html>", status_code=400)

        # Verify state
        stored_state = request.session.get("oauth_state")
        print(f"[DEBUG] Stored state from session: {stored_state[:20] if stored_state else 'None'}...")
        print(f"[DEBUG] State match: {state == stored_state}")

        if not stored_state or state != stored_state:
            return HTMLResponse(
                f"<html><body><h1>OAuth Error</h1>"
                f"<p>Invalid state</p>"
                f"<p>Debug: Expected state in session but got empty session</p>"
                f"<p>Session keys: {list(request.session.keys())}</p>"
                f"</body></html>",
                status_code=400
            )

        # Get stored code_verifier
        code_verifier = request.session.get("code_verifier")
        if not code_verifier:
            return HTMLResponse("<html><body><h1>OAuth Error</h1><p>Missing code_verifier</p></body></html>", status_code=400)

        # Exchange code for tokens
        client_id = os.getenv("OIDC_CLIENT_ID")
        client_secret = os.getenv("OIDC_CLIENT_SECRET")
        base_url = os.getenv("OIDC_BASE_URL", str(request.base_url).rstrip('/'))
        redirect_uri = f"{base_url}/web/callback"

        token_url = "https://oauth2.googleapis.com/token"
        token_data = {
            "client_id": client_id,
            "client_secret": client_secret,
            "code": code,
            "code_verifier": code_verifier,
            "grant_type": "authorization_code",
            "redirect_uri": redirect_uri,
        }

        try:
            async with httpx.AsyncClient() as client:
                response = await client.post(token_url, data=token_data)
                response.raise_for_status()
                tokens = response.json()

            # Decode ID token to get user info (we don't verify signature here since we got it directly from Google)
            import jwt
            id_token = tokens.get("id_token")
            user_info = jwt.decode(id_token, options={"verify_signature": False})

            # Store user info in session
            request.session["user_email"] = user_info.get("email")
            request.session["user_sub"] = user_info.get("sub")

            # Clear OAuth state
            request.session.pop("oauth_state", None)
            request.session.pop("code_verifier", None)

            print(f"[INFO] Web portal login successful for {user_info.get('email')}")

            # Redirect to main page
            return RedirectResponse("/")

        except Exception as e:
            print(f"[ERROR] OAuth token exchange failed: {e}")
            return HTMLResponse(f"<html><body><h1>OAuth Error</h1><p>Token exchange failed: {str(e)}</p></body></html>", status_code=500)

    @mcp.custom_route("/", methods=["GET"])
    async def web_ui(request: Request):
        """Serve the credential management web UI (requires authentication)."""
        # Get authenticated user
        subject = get_authenticated_user(request)
        if not subject:
            # Show login page (no template variables needed - uses /web/login endpoint)
            template_path = Path(__file__).parent / "templates" / "login.html"
            html = template_path.read_text()
            return HTMLResponse(html)

        # Read and render template
        template_path = Path(__file__).parent / "templates" / "credentials.html"
        html = template_path.read_text()

        # Get server URL from environment or request
        base_url = os.getenv("OIDC_BASE_URL")
        if not base_url:
            # Fallback to request base URL
            base_url = str(request.base_url).rstrip('/')

        server_url = f"{base_url}/mcp"

        # Replace template variables
        html = html.replace("{{ user_email }}", subject)
        html = html.replace("{{ server_url }}", server_url)

        return HTMLResponse(html)

    @mcp.custom_route("/api/credentials/validate", methods=["POST"])
    async def api_validate_credentials(request: Request):
        """API endpoint to validate Boomi credentials before saving."""
        subject = get_authenticated_user(request)
        if not subject:
            return JSONResponse({"error": "Authentication required"}, status_code=401)

        try:
            data = await request.json()

            print(f"[DEBUG] Validating credentials for account_id: {data['account_id']}, username: {data['username'][:30]}...")

            # Test credentials by attempting to initialize Boomi SDK and make a simple API call
            test_sdk = Boomi(
                account_id=data["account_id"],
                username=data["username"],
                password=data["password"],
                timeout=10000,
            )

            # Try to get account info - this will fail if credentials are invalid
            print(f"[DEBUG] Calling Boomi API: account.get_account(id_={data['account_id']})")
            result = test_sdk.account.get_account(id_=data["account_id"])

            if result:
                print(f"[DEBUG] Validation successful for {data['account_id']}")
                return JSONResponse({
                    "success": True,
                    "message": "Credentials validated successfully"
                })
            else:
                print(f"[ERROR] Validation returned no result for {data['account_id']}")
                return JSONResponse({"error": "Failed to validate credentials"}, status_code=400)

        except Exception as e:
            error_msg = str(e)
            print(f"[ERROR] Validation exception: {error_msg}")
            print(f"[ERROR] Exception type: {type(e).__name__}")

            # Provide user-friendly error messages
            if "401" in error_msg or "Unauthorized" in error_msg:
                error_msg = "Invalid username or password"
            elif "403" in error_msg or "Forbidden" in error_msg:
                error_msg = "Access denied - check your account permissions"
            elif "404" in error_msg or "Not Found" in error_msg:
                error_msg = "Account ID not found"
            elif "timeout" in error_msg.lower():
                error_msg = "Connection timeout - please try again"

            return JSONResponse({"error": f"Validation failed: {error_msg}"}, status_code=400)

    @mcp.custom_route("/api/credentials", methods=["POST"])
    async def api_set_credentials(request: Request):
        """API endpoint to save credentials."""
        subject = get_authenticated_user(request)
        if not subject:
            return JSONResponse({"error": "Authentication required"}, status_code=401)

        try:
            data = await request.json()

            # Check profile limit (10 profiles per user)
            existing_profiles = list_profiles(subject)
            profile_name = data["profile"]

            # Allow updating existing profile, but limit new profiles to 10
            is_new_profile = profile_name not in [p["profile"] for p in existing_profiles]
            if is_new_profile and len(existing_profiles) >= 10:
                return JSONResponse({
                    "error": "Profile limit reached. You can store up to 10 Boomi account profiles. Please delete an existing profile before adding a new one."
                }, status_code=400)

            put_secret(subject, profile_name, {
                "username": data["username"],
                "password": data["password"],
                "account_id": data["account_id"],
            })

            return JSONResponse({
                "success": True,
                "message": f"Credentials saved for profile '{profile_name}'"
            })
        except Exception as e:
            return JSONResponse({"error": str(e)}, status_code=400)

    @mcp.custom_route("/api/profiles", methods=["GET"])
    async def api_list_profiles(request: Request):
        """API endpoint to list profiles."""
        subject = get_authenticated_user(request)
        if not subject:
            return JSONResponse({"error": "Authentication required"}, status_code=401)

        profiles_data = list_profiles(subject)
        profile_names = [p["profile"] for p in profiles_data]

        return JSONResponse({"profiles": profile_names})

    @mcp.custom_route("/api/profiles/{profile}", methods=["DELETE"])
    async def api_delete_profile(request: Request):
        """API endpoint to delete a profile."""
        subject = get_authenticated_user(request)
        if not subject:
            return JSONResponse({"error": "Authentication required"}, status_code=401)

        profile = request.path_params["profile"]

        try:
            delete_profile(subject, profile)
            return JSONResponse({
                "success": True,
                "message": f"Profile '{profile}' deleted"
            })
        except Exception as e:
            return JSONResponse({"error": str(e)}, status_code=400)


if __name__ == "__main__":
    if LOCAL_MODE:
        print("\n" + "=" * 60)
        print("Boomi MCP Server - LOCAL DEVELOPMENT MODE")
        print("=" * 60)
        print("  WARNING: This is for LOCAL TESTING ONLY")
        print("  No OAuth authentication - DO NOT use in production")
        print("=" * 60)
        print(f"Auth Mode:     None (local dev)")
        print(f"Storage:       Local file (~/.boomi_mcp_local_secrets.json)")
        print(f"Test User:     local-dev-user")
        print("=" * 60)
        print("\nMCP Tools available:")
        print("  list_boomi_profiles - List saved credential profiles")
        print("  set_boomi_credentials - Store Boomi credentials")
        print("  delete_boomi_profile - Delete a credential profile")
        print("  boomi_account_info - Get account information from Boomi API")
        if manage_trading_partner_action:
            print("\n  Trading Partner & Organization Management:")
            tp_desc = "trading partners and organizations" if manage_organization_action else "trading partners"
            print(f"  manage_trading_partner - Unified tool for {tp_desc}")
            print("    Actions: list, get, create, update, delete, analyze_usage")
            if manage_organization_action:
                print("    Org actions: org_list, org_get, org_create, org_update, org_delete")
            print("    Standards: X12, EDIFACT, HL7, RosettaNet, Custom, Tradacoms, Odette")
        if manage_process_action:
            print("\n  Process Management:")
            print("  manage_process - Unified tool for all process operations")
        if build_integration_action:
            print("\n  Integration Builder:")
            print("  build_integration - Plan/apply/verify full integration builds")
            print("    Actions: plan, apply, verify")
        if query_components_action:
            print("\n  Component Discovery:")
            print("  query_components - List, get, search, bulk_get components")
            print("    Actions: list, get, search, bulk_get")
        if manage_component_action:
            print("\n  Component Management:")
            print("  manage_component - Create, update, clone, delete components")
            print("    Actions: create, update, clone, delete")
        if analyze_component_action:
            print("\n  Component Analysis:")
            print("  analyze_component - Dependencies, version comparison, and merge")
            print("    Actions: where_used, dependencies, compare_versions, merge")
        if monitor_platform_action:
            print("\n  Platform Monitoring:")
            print("  monitor_platform - Logs, artifacts, audit trail, and events")
            print("    Actions: execution_logs, execution_artifacts, audit_logs, events")
        if manage_runtimes_action:
            print("\n  Runtime Management:")
            print("  manage_runtimes - Manage runtimes, attachments, restart, Java, tokens")
            print("    Actions: list, get, update, delete, attach, detach, list_attachments,")
            print("             restart, configure_java, create_installer_token")
        if manage_deployment_action:
            print("\n  Deployment Management:")
            print("  manage_deployment - Packages and deployment lifecycle")
            print("    Actions: list_packages, get_package, create_package, delete_package,")
            print("             deploy, undeploy, list_deployments, get_deployment")
        if execute_process_action:
            print("\n  Process Execution:")
            print("  execute_process - Execute a process on a runtime")
            print("    Returns request_id for polling via monitor_platform(action='execution_records')")
        if manage_shared_resources_action:
            print("\n  Shared Resources:")
            print("  manage_shared_resources - Web servers and communication channels")
            print("    Actions: list_web_servers, update_web_server, list_channels, get_channel, create_channel")
        if invoke_api:
            print("\n  Generic API Access:")
            print("  invoke_boomi_api - Direct access to any Boomi REST API endpoint")
            print("    Covers: Roles, Branches, Folders, Packages, Deployments, etc.")
        print("=" * 60 + "\n")

        mcp.run(transport="stdio")
    else:
        # Print startup info
        print("\n" + "=" * 60)
        print("Boomi MCP Server")
        print("=" * 60)

        provider_type = os.getenv("OIDC_PROVIDER", "google")
        base_url = os.getenv("OIDC_BASE_URL", "http://localhost:8000")
        backend_type = os.getenv("SECRETS_BACKEND", "gcp")
        print(f"Auth Mode:     OAuth 2.0 ({provider_type})")
        print(f"Base URL:      {base_url}")
        print(f"Login URL:     {base_url}/auth/login")
        print(f"Secrets:       {backend_type.upper()}")
        if backend_type == "gcp":
            print(f"GCP Project:   {os.getenv('GCP_PROJECT_ID', 'boomimcp')}")
        print("=" * 60)

        host = os.getenv("MCP_HOST", "127.0.0.1")
        port = int(os.getenv("MCP_PORT", "8000"))

        print(f"Starting server on http://{host}:{port}")
        print(f"MCP endpoint: /mcp")
        print(f"OAuth endpoints: /authorize, /auth/callback, /token")
        print("\nPress Ctrl+C to stop\n")

        mcp.run(transport="http", host=host, port=port)
