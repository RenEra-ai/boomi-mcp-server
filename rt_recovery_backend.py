"""
Durable refresh-token recovery backend (stale-token recovery plan).

`refresh_token_grace_patch.py` (Fix A) only protects *immediate* refresh-token
replays: it caches the rotated ``OAuthToken`` for ~60s. That cache cannot be
stretched to hours/days because its cached access token expires after ~1 hour.

When an MCP client comes back hours or days later presenting a FastMCP refresh
JWT that is still cryptographically valid but whose ``_jti_mapping_store`` row
and ``_refresh_token_store`` row were already deleted (by one-time-use rotation
or TTL expiry), FastMCP raises ``invalid_grant`` and the user must fully
re-authenticate.

This module exposes a Fernet-encrypted, MongoDB-backed *alias ledger* keyed by
``sha256(old_refresh_token)``. Each alias records enough metadata to find the
*latest live successor* of a rotated token: the successor's refresh-token hash,
its JTI, and the upstream-token id. ``refresh_token_recovery_patch.py`` walks
this ledger to recover a stale token into a fresh access/refresh pair WITHOUT
replaying any old cached ``OAuthToken`` (the access token would be expired).

Reuses the same MongoDBStore + FernetEncryptionWrapper + STORAGE_ENCRYPTION_KEY
stack server.py already wires for OAuth state (mirrors rt_grace_shared_backend).
Collection defaults to ``mcp-rt-recovery``.

Disable with ``BOOMI_RT_RECOVERY_ENABLED=false`` (the initializer returns None
and the patch is skipped). Records are never stored unencrypted; raw tokens are
never stored at all (only hashes).
"""

from __future__ import annotations

import logging
import os
import time
from typing import Any, Optional

logger = logging.getLogger("boomi.refresh_token_recovery")

DEFAULT_RECOVERY_COLLECTION = "mcp-rt-recovery"

# Alias record schema version (stamped into every written record so a future
# format change can detect/skip old records).
ALIAS_RECORD_VERSION = 1


def alias_ttl_seconds(successor_expires_at: float, max_age_seconds: int, now: float | None = None) -> int:
    """Compute the durable alias TTL.

    The alias must never outlive the successor it points to, nor exceed the
    operator-configured durable recovery window. Floored at 1 second so a live
    alias is always written with a positive TTL.
    """
    if now is None:
        now = time.time()
    remaining = int(successor_expires_at - now)
    return max(1, min(int(max_age_seconds), remaining))


class RefreshTokenRecoveryBackend:
    """Async-safe alias ledger over an encrypted AsyncKeyValue store.

    Surface: ``get`` / ``put_alias`` / ``delete`` for individual aliases, plus
    ``resolve_latest`` which walks ``successor_rt_hash`` links to find the
    newest still-live successor of a stale token.

    The wrapped store is expected to encrypt at rest (production wires
    ``FernetEncryptionWrapper``). Read failures (decryption/deserialization or
    any storage error) are converted to ``None`` and logged WARNING -- a
    corrupted/unavailable alias is best-effort, never a hard 401. Task
    cancellation still propagates. Writes and deletes log WARNING and are
    swallowed so an alias-ledger blip never fails an otherwise-successful
    token rotation.
    """

    def __init__(self, key_value_store) -> None:
        # Deferred import so this module can be imported without the encryption
        # stack present (tests can pass in any AsyncKeyValue).
        from key_value.aio.errors import DecryptionError, DeserializationError

        self._store = key_value_store
        self._read_swallowed = (DecryptionError, DeserializationError)

    @staticmethod
    def _key_repr(key: str) -> str:
        return key[:16] + "..." if key and len(key) > 16 else key

    async def get(self, old_hash: str) -> Optional[dict[str, Any]]:
        try:
            return await self._store.get(key=old_hash)
        except self._read_swallowed as exc:
            logger.warning(
                "Recovery alias read swallowed for key=%s: %s: %s",
                self._key_repr(old_hash),
                type(exc).__name__,
                exc,
            )
            return None
        except Exception as exc:  # noqa: BLE001 — best-effort ledger read
            logger.warning(
                "RT_RECOVERY_GET_FAILED key=%s: %s: %s",
                self._key_repr(old_hash),
                type(exc).__name__,
                exc,
            )
            return None

    async def put_alias(self, old_hash: str, record: dict[str, Any], ttl_seconds: int) -> None:
        try:
            await self._store.put(key=old_hash, value=record, ttl=ttl_seconds)
        except Exception as exc:  # noqa: BLE001 — fire-and-forget by design
            logger.warning(
                "RT_RECOVERY_PUT_FAILED key=%s ttl=%ds: %s: %s",
                self._key_repr(old_hash),
                ttl_seconds,
                type(exc).__name__,
                exc,
            )

    async def delete(self, old_hash: str) -> None:
        try:
            await self._store.delete(key=old_hash)
        except Exception as exc:  # noqa: BLE001 — best-effort cleanup
            logger.warning(
                "RT_RECOVERY_DELETE_FAILED key=%s: %s: %s",
                self._key_repr(old_hash),
                type(exc).__name__,
                exc,
            )

    async def resolve_latest(
        self, old_hash: str, max_hops: int
    ) -> tuple[Optional[dict[str, Any]], list[str]]:
        """Follow ``successor_rt_hash`` links to the newest live successor.

        Returns ``(latest_record, visited_hashes)`` where ``latest_record`` is
        the deepest alias whose successor is still live (its
        ``successor_refresh_jti`` / ``upstream_token_id`` identify the live
        token to rotate), and ``visited_hashes`` is the ordered list of stale
        hashes traversed (used by the patch to compact the chain by pointing
        every visited hash directly at the freshly-issued successor).

        Stops -- returning the best resolvable ``last_good`` so far -- on a
        cycle, a missing alias (terminal: the successor has no further alias
        and is the live token), a ``client_id`` change mid-chain, an expired
        successor, a malformed (linkless) record, or exceeding ``max_hops``.
        Returns ``(None, [])`` when ``old_hash`` has no alias at all.
        """
        now = time.time()
        visited: list[str] = []
        seen: set[str] = set()
        cursor = old_hash
        first_client_id: str | None = None
        last_good: Optional[dict[str, Any]] = None

        for _ in range(max_hops + 1):
            if cursor in seen:
                logger.warning(
                    "RT_DIAG event=rt_recovery_chain_cycle key=%s",
                    self._key_repr(cursor),
                )
                break
            seen.add(cursor)

            record = await self.get(cursor)
            if record is None:
                # Terminal: `cursor` has no alias, so the successor pointed at
                # by `last_good` is the live token. (When cursor == old_hash on
                # the first lookup, last_good is still None -> no recovery.)
                break

            client_id = record.get("client_id")
            if first_client_id is None:
                first_client_id = client_id
            elif client_id != first_client_id:
                logger.warning(
                    "RT_DIAG event=rt_recovery_client_drift key=%s expected=%s got=%s",
                    self._key_repr(cursor),
                    first_client_id,
                    client_id,
                )
                break

            successor_expires_at = record.get("successor_expires_at") or 0
            if successor_expires_at <= now:
                logger.warning(
                    "RT_DIAG event=rt_recovery_chain_expired key=%s",
                    self._key_repr(cursor),
                )
                break

            successor_hash = record.get("successor_rt_hash")
            if not successor_hash:
                # Malformed record with no link; treat its successor metadata
                # as terminal-live so recovery can still use it.
                visited.append(cursor)
                last_good = record
                break

            visited.append(cursor)
            last_good = record
            cursor = successor_hash
        else:
            # Loop exhausted max_hops+1 lookups with a live link still to
            # follow: bound the walk and use the best resolvable so far.
            logger.warning(
                "RT_DIAG event=rt_recovery_max_hops key=%s max_hops=%d",
                self._key_repr(old_hash),
                max_hops,
            )

        return last_good, visited


def initialize_refresh_token_recovery_backend(
    mongodb_uri: str,
    fernet,
    *,
    db_name: str = "boomi_mcp",
) -> Optional[RefreshTokenRecoveryBackend]:
    """Build the production RefreshTokenRecoveryBackend or return None if disabled.

    Args:
        mongodb_uri: Same URI server.py uses for OAuth state.
        fernet: The Fernet or MultiFernet instance built by server.py. Reusing
            it makes the recovery ledger participate in the existing
            STORAGE_ENCRYPTION_KEY rotation.
        db_name: Database name (default ``boomi_mcp`` matches OAuth state).

    Returns:
        RefreshTokenRecoveryBackend on success, or None when
        ``BOOMI_RT_RECOVERY_ENABLED=false`` (operator off-switch).

    Caller-facing failures:
        Raises ValueError if any required input is missing or empty. Underlying
        MongoDB errors at first use are caught by the backend's get/put/delete
        wrappers and logged WARNING -- they do not crash the server.
    """
    if os.getenv("BOOMI_RT_RECOVERY_ENABLED", "true").lower() in ("false", "0", "no"):
        logger.info("Durable refresh-token recovery DISABLED (BOOMI_RT_RECOVERY_ENABLED=false)")
        return None
    if not mongodb_uri:
        raise ValueError("initialize_refresh_token_recovery_backend requires mongodb_uri")
    if fernet is None:
        raise ValueError(
            "initialize_refresh_token_recovery_backend requires a Fernet/MultiFernet instance"
        )

    collection = os.getenv("BOOMI_RT_RECOVERY_COLLECTION", DEFAULT_RECOVERY_COLLECTION)

    # Deferred imports keep this module importable in LOCAL_MODE.
    from key_value.aio.stores.mongodb import MongoDBStore
    from key_value.aio.wrappers.encryption import FernetEncryptionWrapper

    # Route every alias to `collection` (mcp-rt-recovery). MongoDBStore selects
    # the physical collection from `default_collection` when get/put/delete pass
    # no per-operation collection (which this backend deliberately does not).
    mongo_store = MongoDBStore(
        url=mongodb_uri, db_name=db_name, default_collection=collection
    )
    encrypted = FernetEncryptionWrapper(key_value=mongo_store, fernet=fernet)

    backend = RefreshTokenRecoveryBackend(encrypted)
    logger.info(
        "Durable refresh-token recovery ENABLED (collection=%s, encryption=fernet)",
        collection,
    )
    return backend
