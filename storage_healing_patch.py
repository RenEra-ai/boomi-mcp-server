"""
Storage self-healing for corrupted OAuth client documents (Fix C.2).

Today the chain is:

    ClientAuthenticator.authenticate_request
      -> OAuthProxy.get_client
        -> PydanticAdapter.get
          -> VerifiedStorage.get (passthrough)
            -> FernetEncryptionWrapper.get  (raises InvalidToken on bad ciphertext)
              -> MongoDBStore.get

When the ciphertext for a `mcp-oauth-proxy-clients` document can no longer
be decrypted (encryption key rotation that lost the old key, corrupted
write, or a mixed-vintage row from the April-11 migration), the exception
bubbles up to ClientAuthenticator and turns into a permanent 401 for the
affected client_id. The client cannot recover without a manual operator
delete.

This patch wraps `auth_provider.get_client` (after diagnostic_logging has
already wrapped it for ERROR logging) and on `InvalidToken` /
`pydantic.ValidationError`:

  1. Logs an ERROR with the truncated client_id + exception class.
  2. Calls `_client_store.delete(key=client_id)` to evict the corrupted
     document — single targeted call, no scans.
  3. Returns None, which surfaces to the SDK as the existing 401
     `unauthorized_client`. The client falls into Dynamic Client
     Registration on its next attempt and gets a fresh, properly-
     encrypted entry.

Disable with `BOOMI_AUTH_HEAL_CORRUPT_CLIENTS=false`. When disabled,
the ERROR log still fires but the corrupted document is left in place
for forensic inspection; the wrapper still returns None so the user
sees the same 401, just without the self-heal step.
"""

from __future__ import annotations

import logging
import os
import types

logger = logging.getLogger("boomi.storage_healing")

CORRUPT_CLIENT_COLLECTION = "mcp-oauth-proxy-clients"


def apply_storage_healing_patch(auth_provider) -> None:
    """Wrap auth_provider.get_client to self-heal corrupted client docs.

    Call this AFTER `diagnostic_logging.apply_all_patches(...)` so that
    the diagnostic logging wrapper sits inside the healing wrapper:
    the inner wrapper's ERROR log still fires before this outer wrapper
    catches the exception.
    """
    heal_enabled = os.getenv(
        "BOOMI_AUTH_HEAL_CORRUPT_CLIENTS", "true"
    ).lower() in ("true", "1", "yes")

    # Imports deferred so this module can be imported in LOCAL_MODE without
    # pulling in cryptography/pydantic.
    from cryptography.fernet import InvalidToken
    from pydantic import ValidationError

    original_get_client = auth_provider.get_client

    async def patched_get_client(self, client_id):
        try:
            return await original_get_client(client_id)
        except (InvalidToken, ValidationError) as exc:
            client_id_repr = (
                client_id[:16] + "..."
                if client_id and len(client_id) > 16
                else client_id
            )
            logger.error(
                "Corrupted oauth client document detected: client_id=%s "
                "collection=%s error=%s: %s",
                client_id_repr,
                CORRUPT_CLIENT_COLLECTION,
                type(exc).__name__,
                exc,
            )
            if heal_enabled:
                try:
                    await self._client_store.delete(key=client_id)
                    logger.error(
                        "Deleted corrupted client document client_id=%s "
                        "(client will be re-registered on next DCR attempt)",
                        client_id_repr,
                    )
                except Exception as delete_exc:  # pragma: no cover - defensive
                    logger.error(
                        "Failed to delete corrupted client_id=%s: %s: %s",
                        client_id_repr,
                        type(delete_exc).__name__,
                        delete_exc,
                    )
            return None

    auth_provider.get_client = types.MethodType(patched_get_client, auth_provider)
    logger.info(
        "Storage self-healing ENABLED (heal_on_corrupt=%s)", heal_enabled
    )
