"""
Local file-based secrets storage for development.
Stores credentials in a local JSON file instead of cloud secret manager.
"""

import json
import os
from pathlib import Path
from typing import Dict, List


class LocalSecretsBackend:
    """Local file-based credential storage for development."""

    def __init__(self, storage_file: str = None):
        """Initialize local secrets backend.

        Args:
            storage_file: Path to JSON file for storing credentials.
                         Defaults to ~/.boomi_mcp_local_secrets.json
        """
        if storage_file is None:
            storage_file = os.path.expanduser("~/.boomi_mcp_local_secrets.json")

        self.storage_file = Path(storage_file)
        self._ensure_storage_exists()

    def _ensure_storage_exists(self):
        """Create storage file if it doesn't exist."""
        if not self.storage_file.exists():
            self.storage_file.parent.mkdir(parents=True, exist_ok=True)
            self._write_data({})

    def _read_data(self) -> Dict:
        """Read all data from storage file."""
        try:
            with open(self.storage_file, 'r') as f:
                return json.load(f)
        except (json.JSONDecodeError, FileNotFoundError):
            return {}

    def _write_data(self, data: Dict):
        """Write all data to storage file."""
        with open(self.storage_file, 'w') as f:
            json.dump(data, f, indent=2)

    def put_secret(self, subject: str, profile: str, payload: Dict[str, str]):
        """Store credentials for a user profile.

        Args:
            subject: User identifier (email or user ID)
            profile: Profile name
            payload: Credentials dictionary with username, password, account_id
        """
        data = self._read_data()

        # Create user entry if doesn't exist
        if subject not in data:
            data[subject] = {}

        # Store profile credentials
        data[subject][profile] = payload
        self._write_data(data)

    def get_secret(self, subject: str, profile: str) -> Dict[str, str]:
        """Retrieve credentials for a user profile.

        Args:
            subject: User identifier
            profile: Profile name

        Returns:
            Credentials dictionary

        Raises:
            ValueError: If profile not found
        """
        data = self._read_data()

        if subject not in data:
            raise ValueError(f"No credentials found for user: {subject}")

        if profile not in data[subject]:
            available = list(data[subject].keys())
            raise ValueError(
                f"Profile '{profile}' not found for user {subject}. "
                f"Available profiles: {available}"
            )

        return data[subject][profile]

    def list_profiles(self, subject: str) -> List[Dict]:
        """List all profiles for a user.

        Args:
            subject: User identifier

        Returns:
            List of profile dictionaries with 'profile' name
        """
        data = self._read_data()

        if subject not in data:
            return []

        return [{"profile": profile} for profile in data[subject].keys()]

    def delete_profile(self, subject: str, profile: str):
        """Delete a user profile.

        Args:
            subject: User identifier
            profile: Profile name

        Raises:
            ValueError: If profile not found
        """
        data = self._read_data()

        if subject not in data or profile not in data[subject]:
            raise ValueError(f"Profile '{profile}' not found for user {subject}")

        del data[subject][profile]

        # Remove user entry if no profiles left
        if not data[subject]:
            del data[subject]

        self._write_data(data)
