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

    Surface: get / put / delete for the L2 grace cache, plus optional
    lock primitives for Fix D.2 (cross-instance singleflight).

    The wrapped store is expected to encrypt at rest (production wires
    FernetEncryptionWrapper). All exceptions on read are converted to
    `None` (corrupted grace entry is best-effort, never a hard 401).
    All exceptions on write are logged WARNING and swallowed (the
    in-process L1 cache still serves the local caller).

    The optional `lock_collection` argument is a motor collection (raw
    MongoDB client) used by `try_claim_lock`/`release_lock` for atomic
    insert-or-fail semantics. When None, lock methods raise — callers
    must check `supports_locks` before invoking them. Locks are only
    needed when Fix D.2 is enabled (env BOOMI_RT_GRACE_DISTRIBUTED_LOCK).
    """

    def __init__(self, key_value_store, lock_collection=None) -> None:
        # Deferred import so this module can be imported without the
        # encryption stack present (tests can pass in any AsyncKeyValue).
        from key_value.aio.errors import DecryptionError, DeserializationError

        self._store = key_value_store
        self._read_swallowed = (DecryptionError, DeserializationError)
        self._lock_collection = lock_collection

    @property
    def supports_locks(self) -> bool:
        return self._lock_collection is not None

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

    # ---- Fix D.2: distributed singleflight via Mongo upsert-as-lock ----

    async def try_claim_lock(
        self, key: str, ttl_seconds: int, instance: str
    ) -> bool:
        """Best-effort claim of an inflight-refresh lock for `key`.

        Returns True if this caller got the lock (became leader);
        False if another instance already holds it.

        Failure modes: any unexpected exception is logged WARNING and
        we return True (degrade to "no lock held" rather than
        deadlocking the system). The lock collection has a TTL index on
        `expires_at`, so a crashed leader auto-releases after at most
        ttl_seconds + one Mongo TTL-sweep cycle.
        """
        if self._lock_collection is None:
            raise RuntimeError(
                "try_claim_lock called but SharedGraceBackend has no lock_collection. "
                "Enable Fix D.2 via BOOMI_RT_GRACE_DISTRIBUTED_LOCK=true."
            )
        from datetime import datetime, timedelta, timezone
        from pymongo.errors import DuplicateKeyError
        try:
            await self._lock_collection.insert_one({
                "_id": key,
                "instance": instance,
                "expires_at": datetime.now(timezone.utc) + timedelta(seconds=ttl_seconds),
            })
            return True
        except DuplicateKeyError:
            return False
        except Exception as exc:  # noqa: BLE001 — degrade to "no lock"
            logger.warning(
                "try_claim_lock fallthrough for key=%s: %s: %s",
                key[:16] + "..." if key and len(key) > 16 else key,
                type(exc).__name__,
                exc,
            )
            return True

    async def release_lock(self, key: str) -> None:
        """Best-effort lock release. Safe to call even if we never held it."""
        if self._lock_collection is None:
            return
        try:
            await self._lock_collection.delete_one({"_id": key})
        except Exception as exc:  # noqa: BLE001
            logger.warning(
                "release_lock failed for key=%s: %s: %s",
                key[:16] + "..." if key and len(key) > 16 else key,
                type(exc).__name__,
                exc,
            )

    async def write_failure_marker(
        self, key: str, error_type: str, short_ttl: int = 5
    ) -> None:
        """Write a small failure marker so polling followers see the
        leader's exception within ~short_ttl seconds rather than the
        full lock TTL. The marker is detected by callers via the
        sentinel `error` field in the payload."""
        await self.put(
            key,
            {"error": error_type},
            short_ttl,
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
    distributed_lock_enabled = os.getenv(
        "BOOMI_RT_GRACE_DISTRIBUTED_LOCK", "false"
    ).lower() in ("true", "1", "yes")

    # Deferred imports keep this module importable in LOCAL_MODE.
    from key_value.aio.stores.mongodb import MongoDBStore
    from key_value.aio.wrappers.encryption import FernetEncryptionWrapper

    mongo_store = MongoDBStore(url=mongodb_uri, db_name=db_name, coll_name=collection)
    encrypted = FernetEncryptionWrapper(key_value=mongo_store, fernet=fernet)

    lock_collection = None
    if distributed_lock_enabled:
        # Fix D.2: stand up a raw motor collection for atomic upsert-as-lock
        # semantics that the key_value abstraction doesn't expose. Best
        # effort -- if motor can't be imported or the index can't be
        # created, log WARNING and downgrade to grace-cache-only.
        try:
            from motor.motor_asyncio import AsyncIOMotorClient
            client = AsyncIOMotorClient(mongodb_uri)
            db = client[db_name]
            lock_collection = db[DEFAULT_LOCK_COLLECTION]
            # Ensure TTL index on expires_at so crashed leaders auto-release.
            # expireAfterSeconds=0 means "expire when expires_at is in the past".
            import asyncio as _asyncio
            try:
                _asyncio.get_event_loop().run_until_complete(
                    lock_collection.create_index(
                        "expires_at", expireAfterSeconds=0, background=True
                    )
                )
            except RuntimeError:
                # No running event loop at startup; the index is created
                # opportunistically on the first lock attempt instead.
                pass
            logger.info(
                "Distributed grace lock ENABLED (collection=%s)",
                DEFAULT_LOCK_COLLECTION,
            )
        except Exception as exc:  # noqa: BLE001 — degrade gracefully
            logger.warning(
                "Distributed grace lock init failed (%s: %s); "
                "falling back to L2-cache-only",
                type(exc).__name__,
                exc,
            )
            lock_collection = None
    else:
        logger.info("Distributed grace lock DISABLED")

    backend = SharedGraceBackend(encrypted, lock_collection=lock_collection)
    logger.info(
        "Shared grace cache backend ENABLED (collection=%s, encryption=fernet)",
        collection,
    )
    return backend
