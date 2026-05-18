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


def apply_refresh_token_grace_patch() -> None:
    """Install the grace-window monkey-patches on OAuthProxy.

    Idempotent at import time: applies once per Python process. Calling
    twice is harmless because the second call overwrites the same two
    methods with functionally identical wrappers (the inner cache is
    re-created, which is acceptable -- there are no live tokens at
    server startup).
    """
    grace_seconds = int(os.getenv("BOOMI_RT_GRACE_SECONDS", "60"))
    if grace_seconds <= 0:
        logger.info("Refresh-token grace window DISABLED (BOOMI_RT_GRACE_SECONDS=0)")
        return

    max_size = int(os.getenv("BOOMI_RT_GRACE_MAX_SIZE", "512"))
    cache = _TTLCache(max_size=max_size)

    # Imports are deferred so this module can be imported in LOCAL_MODE
    # without pulling in the full FastMCP auth stack.
    from fastmcp.server.auth.oauth_proxy.models import _hash_token
    from fastmcp.server.auth.oauth_proxy.proxy import OAuthProxy
    from mcp.server.auth.provider import RefreshToken

    orig_load_refresh_token = OAuthProxy.load_refresh_token
    orig_exchange_refresh_token = OAuthProxy.exchange_refresh_token

    async def patched_load_refresh_token(self, client, refresh_token):
        result = await orig_load_refresh_token(self, client, refresh_token)
        if result is not None:
            return result

        # Original lookup missed. Check the grace cache -- the same RT may
        # have been rotated within the grace window and we still want the
        # framework to proceed to exchange (which will short-circuit
        # using the cached OAuthToken).
        cached = await cache.get(_hash_token(refresh_token))
        if cached is None:
            return None

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

        cached = await cache.get(old_hash)
        if cached is not None:
            oauth_token, _cached_client_id, _cached_scopes = cached
            client_id_repr = (client.client_id or "<none>")[:8]
            logger.info(
                "Refresh-token grace cache HIT (client=%s) -- returning cached rotated tokens",
                client_id_repr,
            )
            return oauth_token

        result = await orig_exchange_refresh_token(self, client, refresh_token, scopes)
        # Store before returning so a concurrent retry on the same RT
        # sees the same new tokens. The original has already deleted the
        # old RT row and JTI; the grace cache fills the gap for
        # `grace_seconds`.
        await cache.set(
            old_hash,
            (result, client.client_id, list(scopes)),
            time.time() + grace_seconds,
        )
        return result

    OAuthProxy.load_refresh_token = patched_load_refresh_token
    OAuthProxy.exchange_refresh_token = patched_exchange_refresh_token
    logger.info(
        "Refresh-token grace window ENABLED (%ds, max_size=%d)",
        grace_seconds,
        max_size,
    )
