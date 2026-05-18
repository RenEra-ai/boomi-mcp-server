"""
Shared backend for the refresh-token grace cache (Fix D of the cache plan).

Fix A (refresh_token_grace_patch.py, shipped in PR #33) keeps a per-process
LRU cache of rotated tokens for ~60s so a client retry-after-blip or
parallel-tab refresh on the same Python process still receives the same
new tokens. That works inside one Cloud Run replica. With
`k8s/deployment.yaml replicas: 2`, the per-process map is invisible to
the other replica: a refresh that lands on replica B after replica A has
already rotated the RT still gets `Refresh token mapping not found` 401
because B's local cache is empty AND the FastMCP RT row + JTI mapping
were deleted by A.

This module exposes a Fernet-encrypted, MongoDB-backed shared layer that
refresh_token_grace_patch.py uses as a cross-instance L2 behind its
in-process L1. The leader writes the rotated OAuthToken payload through
to this shared backend after `orig_exchange` succeeds; followers on the
same or other replicas read it back during the grace window.

Reuses the same MongoDBStore + FernetEncryptionWrapper + STORAGE_ENCRYPTION_KEY
stack that server.py already wires for OAuth state. New collection name
defaults to `mcp-rt-grace` (the lock collection used by the optional
Fix D.2 distributed singleflight is `mcp-rt-inflight-locks`, owned by
this module too).

Disable with BOOMI_RT_GRACE_SHARED=false (the helper returns None and the
caller falls back to PR #33's per-process-only behavior).
"""

from __future__ import annotations

import logging
import os
from typing import Any, Optional

logger = logging.getLogger("boomi.rt_grace_shared")

DEFAULT_COLLECTION = "mcp-rt-grace"
DEFAULT_LOCK_COLLECTION = "mcp-rt-inflight-locks"


class SharedGraceBackend:
    """Async-safe wrapper around an AsyncKeyValue store.

    Surface kept minimal: get / put / delete. The optional lock primitives
    for Fix D.2 will be added in Phase 4.

    The wrapped store is expected to encrypt at rest (production wires
    FernetEncryptionWrapper). All exceptions on read are converted to
    `None` (corrupted grace entry is best-effort, never a hard 401).
    All exceptions on write are logged WARNING and swallowed (the
    in-process L1 cache still serves the local caller).
    """

    def __init__(self, key_value_store) -> None:
        # Deferred import so this module can be imported without the
        # encryption stack present (tests can pass in any AsyncKeyValue).
        from key_value.aio.errors import DecryptionError, DeserializationError

        self._store = key_value_store
        self._read_swallowed = (DecryptionError, DeserializationError)

    async def get(self, key: str) -> Optional[dict[str, Any]]:
        try:
            return await self._store.get(key=key)
        except self._read_swallowed as exc:
            logger.warning(
                "Shared grace cache read swallowed for key=%s: %s: %s",
                key[:16] + "..." if key and len(key) > 16 else key,
                type(exc).__name__,
                exc,
            )
            return None

    async def put(self, key: str, value: dict[str, Any], ttl_seconds: int) -> None:
        try:
            await self._store.put(key=key, value=value, ttl=ttl_seconds)
        except Exception as exc:  # noqa: BLE001 — fire-and-forget by design
            logger.warning(
                "GRACE_SHARED_PUT_FAILED key=%s ttl=%ds: %s: %s",
                key[:16] + "..." if key and len(key) > 16 else key,
                ttl_seconds,
                type(exc).__name__,
                exc,
            )

    async def delete(self, key: str) -> None:
        try:
            await self._store.delete(key=key)
        except Exception as exc:  # noqa: BLE001 — best-effort cleanup
            logger.warning(
                "Shared grace cache delete failed for key=%s: %s: %s",
                key[:16] + "..." if key and len(key) > 16 else key,
                type(exc).__name__,
                exc,
            )


def initialize_shared_grace_backend(
    mongodb_uri: str,
    fernet,
    *,
    db_name: str = "boomi_mcp",
) -> Optional[SharedGraceBackend]:
    """Build the production SharedGraceBackend or return None if disabled.

    Args:
        mongodb_uri: Same URI as server.py uses for OAuth state.
        fernet: The Fernet or MultiFernet instance built by server.py
            (lines 424-438 today). Reusing it makes the grace cache
            participate in the existing STORAGE_ENCRYPTION_KEY rotation.
        db_name: Database name (default `boomi_mcp` matches OAuth state).

    Returns:
        SharedGraceBackend on success, or None when
        BOOMI_RT_GRACE_SHARED=false (operator off-switch).

    Caller-facing failures:
        Raises ValueError if any required input is missing or empty.
        Underlying MongoDB connection errors at first use of the returned
        backend are caught by the backend's `put`/`get` wrappers and
        logged WARNING — they do not crash the server.
    """
    if os.getenv("BOOMI_RT_GRACE_SHARED", "true").lower() in ("false", "0", "no"):
        logger.info("Shared grace cache backend DISABLED (BOOMI_RT_GRACE_SHARED=false)")
        return None
    if not mongodb_uri:
        raise ValueError("initialize_shared_grace_backend requires mongodb_uri")
    if fernet is None:
        raise ValueError("initialize_shared_grace_backend requires a Fernet/MultiFernet instance")

    collection = os.getenv("BOOMI_RT_GRACE_SHARED_COLLECTION", DEFAULT_COLLECTION)

    # Deferred imports keep this module importable in LOCAL_MODE.
    from key_value.aio.stores.mongodb import MongoDBStore
    from key_value.aio.wrappers.encryption import FernetEncryptionWrapper

    mongo_store = MongoDBStore(url=mongodb_uri, db_name=db_name, coll_name=collection)
    encrypted = FernetEncryptionWrapper(key_value=mongo_store, fernet=fernet)
    backend = SharedGraceBackend(encrypted)
    logger.info(
        "Shared grace cache backend ENABLED (collection=%s, encryption=fernet)",
        collection,
    )
    return backend
