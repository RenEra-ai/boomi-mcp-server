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

import asyncio
import logging
import os
from typing import Any, Optional

logger = logging.getLogger("boomi.rt_grace_shared")

DEFAULT_COLLECTION = "mcp-rt-grace"
DEFAULT_LOCK_COLLECTION = "mcp-rt-inflight-locks"


def _default_lock_client_factory(mongodb_uri: str):
    """Build the async MongoDB client used for the distributed lock.

    Uses pymongo's native ``AsyncMongoClient`` (4.9+) rather than Motor: the
    native client does not carry Motor's event-loop-binding footguns, so it can
    be constructed lazily on the running serving loop. Module-level so tests can
    monkeypatch it with a fake (no real Mongo).
    """
    from pymongo import AsyncMongoClient

    return AsyncMongoClient(mongodb_uri)


class SharedGraceBackend:
    """Async-safe wrapper around an AsyncKeyValue store.

    Surface: get / put / delete for the L2 grace cache, plus optional
    lock primitives for Fix D.2 (cross-instance singleflight).

    The wrapped store is expected to encrypt at rest (production wires
    FernetEncryptionWrapper). Store failures on read are converted to
    `None` (corrupted/unavailable grace entry is best-effort, never a hard
    401). Task cancellation is still allowed to propagate. All exceptions
    on write are logged WARNING and swallowed (the in-process L1 cache still
    serves the local caller).

    Distributed locking (Fix D.2) is configured one of two ways:

    * `lock_collection` — a ready async collection (raw MongoDB client),
      used directly. This back-compat path is mainly for tests that inject
      a fake collection.
    * `lock_uri` / `lock_db_name` / `lock_collection_name` — lazy config.
      The async client + collection + TTL index are built on FIRST USE inside
      `try_claim_lock`/`release_lock`, on the running serving event loop. This
      avoids binding a client to the wrong/closed loop at startup (the bug this
      module previously had with a startup-time AsyncIOMotorClient). An
      optional `lock_client_factory(uri)` overrides the default client builder
      (tests inject a fake).

    When neither is configured, lock methods raise — callers must check
    `supports_locks` first. Locks are only needed when Fix D.2 is enabled
    (env BOOMI_RT_GRACE_DISTRIBUTED_LOCK).
    """

    def __init__(
        self,
        key_value_store,
        lock_collection=None,
        *,
        lock_uri: Optional[str] = None,
        lock_db_name: Optional[str] = None,
        lock_collection_name: Optional[str] = None,
        lock_client_factory=None,
    ) -> None:
        # Deferred import so this module can be imported without the
        # encryption stack present (tests can pass in any AsyncKeyValue).
        from key_value.aio.errors import DecryptionError, DeserializationError

        self._store = key_value_store
        self._read_swallowed = (DecryptionError, DeserializationError)
        self._lock_collection = lock_collection
        self._lock_uri = lock_uri
        self._lock_db_name = lock_db_name
        self._lock_collection_name = lock_collection_name
        self._lock_client_factory = lock_client_factory
        self._lock_init_lock: Optional[asyncio.Lock] = None

    @property
    def supports_locks(self) -> bool:
        return self._lock_collection is not None or self._lock_uri is not None

    async def _ensure_lock_collection(self):
        """Return the lock collection, lazily building it on the running loop.

        Builds the async client + collection + TTL index exactly once, on first
        use, single-flighted by an asyncio.Lock created on the running loop.
        Index-creation failures are logged and tolerated (slower TTL reap, not a
        crash). A directly-injected `lock_collection` is returned as-is (no index
        creation — preserves the test/back-compat path).
        """
        if self._lock_collection is not None:
            return self._lock_collection
        if self._lock_uri is None:
            raise RuntimeError(
                "Distributed lock requested but SharedGraceBackend has no lock "
                "configuration. Enable Fix D.2 via BOOMI_RT_GRACE_DISTRIBUTED_LOCK=true."
            )
        if self._lock_init_lock is None:
            # asyncio.Lock() creation is synchronous; no await between the None
            # check and assignment, so this is race-free under cooperative
            # single-threaded asyncio.
            self._lock_init_lock = asyncio.Lock()
        async with self._lock_init_lock:
            if self._lock_collection is None:
                factory = self._lock_client_factory or _default_lock_client_factory
                client = factory(self._lock_uri)
                coll = client[self._lock_db_name][self._lock_collection_name]
                # TTL index so crashed leaders auto-release. expireAfterSeconds=0
                # means "expire when expires_at is in the past".
                try:
                    await coll.create_index(
                        "expires_at", expireAfterSeconds=0, background=True
                    )
                except Exception as exc:  # noqa: BLE001 — TTL index is mandatory
                    # Without the TTL index a crashed leader's lock row would
                    # never auto-release. Refuse to use this collection: leave
                    # _lock_collection unset and raise so try_claim_lock degrades
                    # to no-lock for this attempt (no row inserted) instead of
                    # leaving an unmanaged lock behind.
                    logger.error(
                        "lazy lock TTL index creation failed (%s: %s); skipping "
                        "lock-row insert to avoid unmanaged locks",
                        type(exc).__name__,
                        exc,
                    )
                    raise
                self._lock_collection = coll
        return self._lock_collection

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
        except Exception as exc:  # noqa: BLE001 — best-effort cache read
            logger.warning(
                "GRACE_SHARED_GET_FAILED key=%s: %s: %s",
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

    async def probe(self) -> None:
        """Strict-startup health probe: a real write+read+delete round-trip to
        the shared grace collection.

        The shared grace cache DEPENDS on writes (``put``), so the probe
        exercises the write path -- a read-only check would pass on a collection
        the credentials can read but not write, while live cache writes are
        silently dropped. Unlike ``get``/``put``/``delete`` (which swallow errors
        so a cache blip never fails a live exchange), this lets exceptions
        PROPAGATE so a strict production startup fails fast when the collection is
        unreachable, missing, or not writable. Uses a short-TTL sentinel key and
        cleans it up.
        """
        probe_key = "__rt_grace_startup_probe__"
        await self._store.put(key=probe_key, value={"probe": True}, ttl=60)
        await self._store.get(key=probe_key)
        await self._store.delete(key=probe_key)

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
        if not self.supports_locks:
            raise RuntimeError(
                "try_claim_lock called but SharedGraceBackend has no lock_collection. "
                "Enable Fix D.2 via BOOMI_RT_GRACE_DISTRIBUTED_LOCK=true."
            )
        try:
            lock_collection = await self._ensure_lock_collection()
        except Exception as exc:  # noqa: BLE001 — degrade to "no lock held"
            logger.warning(
                "lock collection init failed for key=%s (%s: %s); degrading to no-lock",
                key[:16] + "..." if key and len(key) > 16 else key,
                type(exc).__name__,
                exc,
            )
            return True
        from datetime import datetime, timedelta, timezone
        from pymongo.errors import DuplicateKeyError
        try:
            await lock_collection.insert_one({
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

    async def release_lock(self, key: str, instance: str) -> None:
        """Best-effort lock release. Safe to call even if we never held it.

        Owner-scoped: the delete filter requires both the key AND the
        instance label stamped into the lock at claim time. Without
        that scoping, a stale leader that ran past `ttl_seconds`
        (Mongo TTL-expired the lock, a peer claimed a fresh one) would
        delete the new leader's lock in its `finally` block, re-opening
        the duplicate-refresh race the lock is meant to prevent. The
        same hazard exists when `try_claim_lock` returns True after an
        ambiguous insert failure (the degrade-to-no-lock fallthrough):
        the non-owner caller would otherwise unlock a real owner.
        """
        if not self.supports_locks:
            return
        try:
            lock_collection = await self._ensure_lock_collection()
        except Exception:  # noqa: BLE001 — nothing to release if init fails
            return
        try:
            await lock_collection.delete_one(
                {"_id": key, "instance": instance}
            )
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

    # Route every grace record to `collection` (mcp-rt-grace). MongoDBStore
    # selects the physical Mongo collection from the per-operation `collection`
    # argument, falling back to `default_collection` when none is supplied --
    # and SharedGraceBackend.get/put/delete deliberately pass none. The
    # `coll_name` constructor arg is inert in this key_value version, so
    # passing the name there silently routed every grace record into a
    # collection literally named "default_collection" instead of mcp-rt-grace.
    mongo_store = MongoDBStore(
        url=mongodb_uri, db_name=db_name, default_collection=collection
    )
    encrypted = FernetEncryptionWrapper(key_value=mongo_store, fernet=fernet)

    lock_kwargs: dict[str, Any] = {}
    if distributed_lock_enabled:
        # Fix D.2: configure the distributed lock LAZILY. The async client +
        # collection + TTL index are built on first use inside try_claim_lock,
        # on the running serving event loop -- never at startup, which would
        # bind the client to the wrong/closed loop ("Future attached to a
        # different loop"). Only inert config strings are stored here.
        lock_kwargs = dict(
            lock_uri=mongodb_uri,
            lock_db_name=db_name,
            lock_collection_name=DEFAULT_LOCK_COLLECTION,
        )
        logger.info(
            "Distributed grace lock ENABLED (lazy init; collection=%s)",
            DEFAULT_LOCK_COLLECTION,
        )
    else:
        logger.info("Distributed grace lock DISABLED")

    backend = SharedGraceBackend(encrypted, **lock_kwargs)
    logger.info(
        "Shared grace cache backend ENABLED (collection=%s, encryption=fernet)",
        collection,
    )
    return backend
