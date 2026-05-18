"""
Refresh-token rotation grace window (Fix A of the OAuth hardening plan).

Upstream FastMCP v3.1.1 enforces one-time-use refresh tokens: each
successful exchange deletes the old refresh-token row and JTI mapping
(see fastmcp/server/auth/oauth_proxy/proxy.py around lines 1333 and
1351). Any client retry-after-blip, parallel-tab refresh, or persistence
hiccup that presents the old refresh token a second time returns
`Refresh token mapping not found` -> 401 invalid_grant -> the user must
fully re-authenticate.

This patch keeps the old refresh token usable for a short grace window
(default 60s) by returning the *same* new tokens that the first exchange
produced. Same pattern used by Auth0 ("absolute refresh-token reuse
interval") and AWS Cognito ("reuse grace"). Google's own refresh token
is still rotated only when Google rotates it -- this patch only covers
the FastMCP-side rotation.

Disable with `BOOMI_RT_GRACE_SECONDS=0`. Capacity bounded by
`BOOMI_RT_GRACE_MAX_SIZE` (default 512).
"""

from __future__ import annotations

import asyncio
import logging
import os
import time
from collections import OrderedDict
from typing import Any

logger = logging.getLogger("boomi.refresh_token_grace")


class _TTLCache:
    """Bounded LRU dict with per-entry expiry. Single asyncio.Lock; O(1)."""

    def __init__(self, max_size: int) -> None:
        self._data: "OrderedDict[str, tuple[float, Any]]" = OrderedDict()
        self._max_size = max_size
        self._lock = asyncio.Lock()

    async def get(self, key: str) -> Any | None:
        async with self._lock:
            entry = self._data.get(key)
            if entry is None:
                return None
            expires_at, value = entry
            if expires_at <= time.time():
                self._data.pop(key, None)
                return None
            self._data.move_to_end(key)
            return value

    async def set(self, key: str, value: Any, expires_at: float) -> None:
        async with self._lock:
            self._data[key] = (expires_at, value)
            self._data.move_to_end(key)
            while len(self._data) > self._max_size:
                self._data.popitem(last=False)


def apply_refresh_token_grace_patch(*, shared_backend=None) -> None:
    """Install the grace-window monkey-patches on OAuthProxy.

    Idempotent at import time: applies once per Python process. Calling
    twice is harmless because the second call overwrites the same two
    methods with functionally identical wrappers (the inner cache is
    re-created, which is acceptable -- there are no live tokens at
    server startup).

    Args:
        shared_backend: Optional rt_grace_shared_backend.SharedGraceBackend
            instance. When provided, the leader's rotated tokens are
            written through to the shared backend after orig_exchange
            succeeds, and followers on the same OR other Cloud Run
            replicas consult the shared backend on local cache miss.
            When None, behavior matches PR #33 (per-process only).
    """
    grace_seconds = int(os.getenv("BOOMI_RT_GRACE_SECONDS", "60"))
    if grace_seconds <= 0:
        logger.info("Refresh-token grace window DISABLED (BOOMI_RT_GRACE_SECONDS=0)")
        return

    max_size = int(os.getenv("BOOMI_RT_GRACE_MAX_SIZE", "512"))
    cache = _TTLCache(max_size=max_size)

    # Per-token singleflight: ensures only one call into the underlying
    # OAuthProxy.exchange_refresh_token runs for each old refresh-token
    # hash, even under concurrent (parallel-tab / network-retry) load.
    # Without this, two concurrent refreshes both observe a cache miss
    # and both enter the original exchange path; the first deletes the
    # FastMCP refresh-token row + JTI mapping (proxy.py:1333,1351), the
    # second then fails with `invalid_grant` "Refresh token mapping not
    # found" -- the exact 401 this patch advertises that it prevents.
    inflight: dict[str, asyncio.Future] = {}
    inflight_lock = asyncio.Lock()

    # Imports are deferred so this module can be imported in LOCAL_MODE
    # without pulling in the full FastMCP auth stack.
    from fastmcp.server.auth.oauth_proxy.models import _hash_token
    from fastmcp.server.auth.oauth_proxy.proxy import OAuthProxy
    from mcp.server.auth.provider import RefreshToken
    from mcp.shared.auth import OAuthToken

    orig_load_refresh_token = OAuthProxy.load_refresh_token
    orig_exchange_refresh_token = OAuthProxy.exchange_refresh_token

    def _serialize(oauth_token, client_id, scopes_list):
        """OAuthToken + identity -> JSON-friendly dict for the shared backend."""
        return {
            "access_token": oauth_token.access_token,
            "token_type": oauth_token.token_type,
            "expires_in": oauth_token.expires_in,
            "refresh_token": oauth_token.refresh_token,
            "scope": oauth_token.scope,
            "client_id": client_id,
            "scopes": list(scopes_list),
        }

    def _deserialize(payload):
        """Shared-backend payload -> (OAuthToken, client_id, scopes_list)."""
        return (
            OAuthToken(
                access_token=payload["access_token"],
                token_type=payload.get("token_type", "Bearer"),
                expires_in=payload.get("expires_in"),
                refresh_token=payload.get("refresh_token"),
                scope=payload.get("scope"),
            ),
            payload.get("client_id"),
            payload.get("scopes") or [],
        )

    async def _shared_get(old_hash):
        """Return (OAuthToken, client_id, scopes) from the shared backend, or None."""
        if shared_backend is None:
            return None
        payload = await shared_backend.get(old_hash)
        if not payload or "access_token" not in payload:
            return None
        return _deserialize(payload)

    async def patched_load_refresh_token(self, client, refresh_token):
        result = await orig_load_refresh_token(self, client, refresh_token)
        if result is not None:
            return result

        # Original lookup missed. There are two ways this can happen
        # within the grace window:
        #   (a) The exchange has already completed and the cache holds the
        #       rotated tokens -- check cache and synthesize.
        #   (b) A concurrent exchange is in flight and has already deleted
        #       the storage row but hasn't populated the cache yet -- wait
        #       on the in-flight Future, then re-check the cache.
        old_hash = _hash_token(refresh_token)

        async with inflight_lock:
            future = inflight.get(old_hash)
        if future is not None:
            try:
                await future  # Wait for the leader to finish.
            except Exception:
                # Leader failed; let the cache miss decide below.
                pass

        cached = await cache.get(old_hash)
        if cached is None:
            # Local L1 miss. Consult the shared L2 (Fix D) -- a leader on
            # ANOTHER Cloud Run replica may have already rotated this RT.
            cached = await _shared_get(old_hash)
            if cached is None:
                return None
            logger.info(
                "Refresh-token grace SHARED HIT in load (client=%s)",
                (client.client_id or "<none>")[:8],
            )

        oauth_token, cached_client_id, cached_scopes = cached
        # Cross-client reuse defense: only honor the grace entry for the
        # original client_id.
        if cached_client_id != client.client_id:
            logger.warning(
                "Refresh-token grace cache client_id mismatch: expected %s, got %s",
                cached_client_id,
                client.client_id,
            )
            return None

        # expires_at on RefreshToken is the upstream RT expiry; we don't
        # know the exact value here, so use the OAuthToken.expires_in as a
        # lower-bound proxy. The framework only uses expires_at for the
        # not-expired check, which we want to pass.
        expires_at = int(time.time() + (oauth_token.expires_in or 3600))
        return RefreshToken(
            token=refresh_token,
            client_id=cached_client_id,
            scopes=cached_scopes,
            expires_at=expires_at,
        )

    async def patched_exchange_refresh_token(self, client, refresh_token, scopes):
        old_hash = _hash_token(refresh_token.token)
        client_id_repr = (client.client_id or "<none>")[:8]

        # Fast path: someone has already rotated this RT within the grace
        # window. Skip both the singleflight and the upstream call.
        cached = await cache.get(old_hash)
        if cached is not None:
            oauth_token, _cached_client_id, _cached_scopes = cached
            logger.info(
                "Refresh-token grace cache HIT (client=%s) -- returning cached rotated tokens",
                client_id_repr,
            )
            return oauth_token

        # Shared L2 fast path: a leader on ANOTHER replica may have
        # already rotated. If so, return their result and skip both the
        # local singleflight and any upstream call.
        shared_cached = await _shared_get(old_hash)
        if shared_cached is not None:
            oauth_token, _cid, _scp = shared_cached
            # Warm the local L1 cache so subsequent calls on this replica
            # don't pay the shared-get latency again.
            await cache.set(
                old_hash, shared_cached, time.time() + grace_seconds
            )
            logger.info(
                "Refresh-token grace SHARED HIT in exchange (client=%s)",
                client_id_repr,
            )
            return oauth_token

        # Singleflight: claim the slot or piggyback on the in-flight leader.
        is_leader = False
        async with inflight_lock:
            # Re-check the local cache under the lock to close the obvious
            # check-then-act race against a leader that just finished.
            cached = await cache.get(old_hash)
            if cached is not None:
                return cached[0]
            # Also re-check the shared cache under the lock so a write from
            # another replica between the fast-path check and lock entry
            # is observed before we needlessly claim a singleflight slot.
            shared_cached = await _shared_get(old_hash)
            if shared_cached is not None:
                await cache.set(
                    old_hash, shared_cached, time.time() + grace_seconds
                )
                return shared_cached[0]
            future = inflight.get(old_hash)
            if future is None:
                future = asyncio.get_event_loop().create_future()
                inflight[old_hash] = future
                is_leader = True

        if not is_leader:
            logger.info(
                "Refresh-token grace singleflight WAIT (client=%s) -- joining in-flight rotation",
                client_id_repr,
            )
            # Re-raise the leader's exception so the framework returns the
            # same TokenError to all racing callers.
            return await future

        # Leader path: run the real exchange, populate the cache, and
        # release the singleflight slot regardless of outcome.
        try:
            result = await orig_exchange_refresh_token(self, client, refresh_token, scopes)
        except BaseException as exc:
            async with inflight_lock:
                inflight.pop(old_hash, None)
            if not future.done():
                future.set_exception(exc)
            raise
        await cache.set(
            old_hash,
            (result, client.client_id, list(scopes)),
            time.time() + grace_seconds,
        )
        # Write through to the shared backend (Fix D). The backend
        # swallows storage errors and logs WARNING, so the local caller
        # is never blocked by a Mongo blip.
        if shared_backend is not None:
            await shared_backend.put(
                old_hash,
                _serialize(result, client.client_id, list(scopes)),
                grace_seconds,
            )
        async with inflight_lock:
            inflight.pop(old_hash, None)
        if not future.done():
            future.set_result(result)
        return result

    OAuthProxy.load_refresh_token = patched_load_refresh_token
    OAuthProxy.exchange_refresh_token = patched_exchange_refresh_token
    logger.info(
        "Refresh-token grace window ENABLED (%ds, max_size=%d, singleflight=on, shared_backend=%s)",
        grace_seconds,
        max_size,
        "on" if shared_backend is not None else "off",
    )
