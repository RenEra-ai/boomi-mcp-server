"""Compatibility wrapper that unifies local and cloud secret backends
behind the CredentialStore interface expected by tools.py and server.py."""

import os
import logging
from typing import Dict, List, Optional

logger = logging.getLogger(__name__)


class CredentialStore:
    """Thin adapter over LocalSecretsBackend / cloud SecretsBackend.

    Selects the backend at init time:
    - BOOMI_LOCAL=true or db_path provided  ->  LocalSecretsBackend
    - Otherwise                             ->  get_secrets_backend()
    """

    def __init__(self, db_path: Optional[str] = None):
        local = os.getenv("BOOMI_LOCAL", "").lower() in ("true", "1", "yes")
        if local:
            from .local_secrets import LocalSecretsBackend
            self._backend = LocalSecretsBackend(storage_file=db_path)
        else:
            from .cloud_secrets import get_secrets_backend
            self._backend = get_secrets_backend()

    def store_credentials(
        self,
        subject: str,
        profile: str,
        username: str,
        password: str,
        account_id: str,
        base_url: str = "https://api.boomi.com/api/rest/v1",
    ) -> None:
        payload = {
            "username": username,
            "password": password,
            "account_id": account_id,
            "base_url": base_url,
        }
        self._backend.put_secret(subject, profile, payload)

    def get_credentials(self, subject: str, profile: str) -> Optional[Dict[str, str]]:
        try:
            return self._backend.get_secret(subject, profile)
        except ValueError:
            return None

    def list_profiles(self, subject: str) -> List[str]:
        raw = self._backend.list_profiles(subject)
        return [entry["profile"] for entry in raw]

    def delete_profile(self, subject: str, profile: str) -> bool:
        try:
            self._backend.delete_profile(subject, profile)
            return True
        except ValueError:
            return False
