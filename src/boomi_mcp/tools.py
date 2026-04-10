"""MCP tools for Boomi API integration with secure credential management."""

import sys
from pathlib import Path
from typing import Dict, List, Optional
from pydantic import BaseModel, Field
import logging

# Add boomi-python to path
boomi_python_path = Path(__file__).parent.parent.parent.parent / "boomi-python"
if boomi_python_path.exists():
    sys.path.insert(0, str(boomi_python_path))

try:
    from boomi import Boomi
except ImportError as e:
    logging.error(f"Failed to import Boomi SDK: {e}")
    logging.error(f"Boomi-python path: {boomi_python_path}")
    logging.error(f"Python path: {sys.path}")
    raise

from .credentials import CredentialStore

logger = logging.getLogger(__name__)


# Pydantic models for tool inputs/outputs
class SetBoomiCredentialsInput(BaseModel):
    """Input schema for setting Boomi credentials."""
    profile: str = Field(..., description="Profile name (e.g., 'sandbox', 'prod')")
    username: str = Field(..., description="Boomi username (e.g., BOOMI_TOKEN.user@example.com)")
    password: str = Field(..., description="Boomi password or API token")
    account_id: str = Field(..., description="Boomi account ID")
    base_url: str = Field(
        default="https://api.boomi.com/api/rest/v1",
        description="Boomi API base URL"
    )


class SetBoomiCredentialsOutput(BaseModel):
    """Output schema for setting Boomi credentials."""
    success: bool
    message: str
    profile: str


class ListBoomiProfilesOutput(BaseModel):
    """Output schema for listing Boomi profiles."""
    profiles: List[str] = Field(description="List of available profile names")
    count: int = Field(description="Number of profiles")


class DeleteBoomiProfileInput(BaseModel):
    """Input schema for deleting a Boomi profile."""
    profile: str = Field(..., description="Profile name to delete")


class DeleteBoomiProfileOutput(BaseModel):
    """Output schema for deleting a Boomi profile."""
    success: bool
    message: str
    profile: str


class BoomiAccountInfoInput(BaseModel):
    """Input schema for getting Boomi account information."""
    profile: str = Field(..., description="Profile name to use for API call")


class BoomiAccountInfoOutput(BaseModel):
    """Output schema for Boomi account information."""
    success: bool
    account_id: str
    account_name: Optional[str] = None
    status: Optional[str] = None
    type: Optional[str] = None
    date_created: Optional[str] = None
    expiration_date: Optional[str] = None
    raw_data: Optional[Dict] = Field(default=None, description="Full account data from API")
    error: Optional[str] = None


class BoomiMCPTools:
    """
    MCP tools for Boomi API integration.

    Implements credential management and Boomi API calls with proper error handling.
    """

    def __init__(self, credential_store: CredentialStore):
        """
        Initialize Boomi MCP tools.

        Args:
            credential_store: Secure credential storage instance
        """
        self.credential_store = credential_store

    def set_boomi_credentials(
        self,
        subject: str,
        profile: str,
        username: str,
        password: str,
        account_id: str,
        base_url: str = "https://api.boomi.com/api/rest/v1"
    ) -> SetBoomiCredentialsOutput:
        """
        Store Boomi credentials for a profile (requires 'secrets:write' scope).

        Args:
            subject: JWT subject (user identifier)
            profile: Profile name (e.g., 'sandbox', 'prod')
            username: Boomi username
            password: Boomi password (will be encrypted)
            account_id: Boomi account ID
            base_url: Boomi API base URL

        Returns:
            Success status and message
        """
        import re as _re
        try:
            # Validate required parameters are not empty/whitespace
            validation_errors = []
            for param_name, param_val in [("profile", profile), ("account_id", account_id), ("username", username), ("password", password)]:
                if not param_val or not param_val.strip():
                    validation_errors.append(param_name)
            if validation_errors:
                return SetBoomiCredentialsOutput(
                    success=False,
                    message=f"Required parameter(s) cannot be empty: {', '.join(validation_errors)}",
                    profile=profile or ""
                )

            # Strip whitespace
            profile = profile.strip()
            account_id = account_id.strip()
            username = username.strip()
            password = password.strip()

            # Validate account_id format
            if not _re.fullmatch(r'[A-Za-z0-9_-]+', account_id):
                return SetBoomiCredentialsOutput(
                    success=False,
                    message="account_id contains invalid characters. Expected alphanumeric, hyphens, or underscores only.",
                    profile=profile
                )

            self.credential_store.store_credentials(
                subject=subject,
                profile=profile,
                username=username,
                password=password,
                account_id=account_id,
                base_url=base_url
            )

            # Never log passwords
            logger.info(f"Stored credentials for {subject}:{profile} (username: {username[:10]}***)")

            msg = f"Credentials stored successfully for profile '{profile}'"
            if not username.startswith("BOOMI_TOKEN."):
                msg += " (warning: username does not start with 'BOOMI_TOKEN.' — Boomi API tokens typically use this prefix)"

            return SetBoomiCredentialsOutput(
                success=True,
                message=msg,
                profile=profile
            )
        except Exception as e:
            logger.error(f"Failed to store credentials for {subject}:{profile}: {e}")
            # Sanitize error message to avoid leaking URLs/paths
            from .sanitize import sanitize_error_msg
            raw = sanitize_error_msg(str(e))
            return SetBoomiCredentialsOutput(
                success=False,
                message=f"Failed to store credentials: {raw}",
                profile=profile
            )

    def list_boomi_profiles(self, subject: str) -> ListBoomiProfilesOutput:
        """
        List all Boomi profiles for the authenticated user (requires 'secrets:read' scope).

        Args:
            subject: JWT subject (user identifier)

        Returns:
            List of profile names
        """
        try:
            profiles = self.credential_store.list_profiles(subject)
            logger.info(f"Listed {len(profiles)} profiles for {subject}")

            return ListBoomiProfilesOutput(
                profiles=profiles,
                count=len(profiles)
            )
        except Exception as e:
            logger.error(f"Failed to list profiles for {subject}: {e}")
            return ListBoomiProfilesOutput(
                profiles=[],
                count=0
            )

    def delete_boomi_profile(
        self,
        subject: str,
        profile: str
    ) -> DeleteBoomiProfileOutput:
        """
        Delete a Boomi profile (requires 'secrets:write' scope).

        Args:
            subject: JWT subject (user identifier)
            profile: Profile name to delete

        Returns:
            Success status and message
        """
        try:
            deleted = self.credential_store.delete_profile(subject, profile)

            if deleted:
                logger.info(f"Deleted profile {subject}:{profile}")
                return DeleteBoomiProfileOutput(
                    success=True,
                    message=f"Profile '{profile}' deleted successfully",
                    profile=profile
                )
            else:
                logger.warning(f"Profile {subject}:{profile} not found")
                return DeleteBoomiProfileOutput(
                    success=False,
                    message=f"Profile '{profile}' not found",
                    profile=profile
                )
        except Exception as e:
            logger.error(f"Failed to delete profile {subject}:{profile}: {e}")
            return DeleteBoomiProfileOutput(
                success=False,
                message=f"Failed to delete profile: {str(e)}",
                profile=profile
            )

    def boomi_account_info(
        self,
        subject: str,
        profile: str,
        timeout: int = 30
    ) -> BoomiAccountInfoOutput:
        """
        Get Boomi account information (requires 'boomi:read' scope).

        This tool implements the core logic from boomi-python/examples/12_utilities/sample.py:
        1. Retrieve encrypted credentials for the profile
        2. Initialize Boomi SDK client
        3. Call account.get_account() to retrieve account information
        4. Return structured account data

        Args:
            subject: JWT subject (user identifier)
            profile: Profile name to use
            timeout: API request timeout in seconds

        Returns:
            Account information or error message
        """
        try:
            # 1. Retrieve credentials
            creds = self.credential_store.get_credentials(subject, profile)
            if not creds:
                logger.warning(f"Profile {subject}:{profile} not found")
                return BoomiAccountInfoOutput(
                    success=False,
                    account_id="",
                    error=f"Profile '{profile}' not found. Use set_boomi_credentials first."
                )

            logger.info(f"Retrieved credentials for {subject}:{profile}")

            # 2. Initialize Boomi SDK (same as sample.py)
            sdk = Boomi(
                account_id=creds["account_id"],
                username=creds["username"],
                password=creds["password"],
                base_url=creds.get("base_url", "https://api.boomi.com/api/rest/v1"),
                timeout=timeout * 1000,  # Boomi SDK uses milliseconds
            )

            logger.info(f"Initialized Boomi SDK for account {creds['account_id']}")

            # 3. Call get_account (same as sample.py line 31)
            result = sdk.account.get_account(id_=creds["account_id"])

            logger.info(f"Retrieved account info for {creds['account_id']}")

            # 4. Parse and return structured data
            account_data = {}
            if hasattr(result, '__dict__'):
                for key, value in result.__dict__.items():
                    if not key.startswith('_') and value is not None:
                        account_data[key] = str(value)

            return BoomiAccountInfoOutput(
                success=True,
                account_id=creds["account_id"],
                account_name=account_data.get("name"),
                status=account_data.get("status"),
                type=account_data.get("type"),
                date_created=account_data.get("dateCreated"),
                expiration_date=account_data.get("expirationDate"),
                raw_data=account_data,
                error=None
            )

        except Exception as e:
            logger.error(f"Failed to get account info for {subject}:{profile}: {e}", exc_info=True)
            return BoomiAccountInfoOutput(
                success=False,
                account_id=creds["account_id"] if creds else "",
                error=f"API call failed: {str(e)}"
            )


# Scope requirements for each tool
TOOL_SCOPES = {
    "set_boomi_credentials": ["secrets:write"],
    "list_boomi_profiles": ["secrets:read"],
    "delete_boomi_profile": ["secrets:write"],
    "boomi_account_info": ["boomi:read"],
}
