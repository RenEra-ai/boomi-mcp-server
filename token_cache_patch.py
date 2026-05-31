"""
Per-process LRU TTL cache for GoogleTokenVerifier.verify_token (Fix B).

Every MCP tool call goes through OAuthProxy.load_access_token, which calls
GoogleTokenVerifier.verify_token (fastmcp/server/auth/providers/google.py
lines 67-156). That method issues two synchronous HTTP requests to Google
on every invocation:
  GET https://www.googleapis.com/oauth2/v1/tokeninfo?access_token=...
  GET https://www.googleapis.com/oauth2/v2/userinfo

Consequences without this cache:
  - ~150-400 ms latency tax on every MCP tool call.
  - Google rate-limits both endpoints per OAuth client and per source IP.
    A burst trips the limit; every tool call then 401s because
    GoogleTokenVerifier returns None on HTTPX errors (google.py:160-162)
    and OAuthProxy.load_access_token silently turns None into a 401
    (proxy.py:1452-1454) with no log line.

This patch wraps verify_token with a bounded LRU TTL cache keyed by
sha256(token)[:32]. Cached AccessToken instances are returned for the
remainder of their declared expiry, capped at BOOMI_TOKEN_CACHE_TTL_SECONDS
(default 300 s -- caps the window in which Google-side revocation is not
honored). Negative results (None) are NEVER cached: a transient Google
rate-limit must not lock a user out for the TTL.

Disable with BOOMI_TOKEN_CACHE_DISABLE=true. Capacity bounded by
BOOMI_TOKEN_CACHE_MAX_SIZE (default 256).

Stale-while-revalidate (opt-in, BOOMI_TOKEN_CACHE_SWR=true): when a hit's
remaining TTL drops below BOOMI_TOKEN_CACHE_SWR_WINDOW (default 30 s),
return the cached value immediately and schedule an asyncio.create_task
background refresh. Defends against short Google outages.

Stale-if-error (opt-in, BOOMI_TOKEN_CACHE_STALE_IF_ERROR_SECONDS > 0): when a
cache entry has already expired and the live Google verifier then returns None
(a transient tokeninfo/userinfo failure), the last positive AccessToken is
served for up to that many seconds past the entry's expiry -- but ONLY while the
token's own declared expiry is still in the future. Negative verifier results
are still NEVER cached, and a token with an explicitly past or missing expiry is
never served stale. Emits a TOKEN_CACHE event=token_cache_stale_if_error line.
"""

from __future__ import annotations

import asyncio
import hashlib
import logging
import os
import time
from collections import OrderedDict
from typing import Any

logger = logging.getLogger("boomi.token_cache")


def _log_token_cache_event(event: str, *, level: str = "warning", **fields: Any) -> None:
    """Emit one structured diagnostic line: ``TOKEN_CACHE event=<name> k=v ...``.

    Only ever passed cache-key prefixes (never raw token material).
    """
    parts = [f"event={event}"]
    for key, value in fields.items():
        if value is None:
            continue
        parts.append(f"{key}={value}")
    getattr(logger, level, logger.warning)("TOKEN_CACHE " + " ".join(parts))


class _TTLCache:
    """Bounded LRU dict with per-entry expiry. Single asyncio.Lock; O(1).

    When ``stale_if_error_seconds`` > 0 the cache also remembers each positive
    entry for that many seconds past its expiry, so a transient verifier failure
    can be ridden out by serving the recently-expired-but-still-valid token.
    """

    def __init__(self, max_size: int, stale_if_error_seconds: float = 0.0) -> None:
        self._data: "OrderedDict[str, tuple[float, Any]]" = OrderedDict()
        # key -> (stale_deadline, value); only populated when stale-if-error is on.
        self._stale: "OrderedDict[str, tuple[float, Any]]" = OrderedDict()
        self._max_size = max_size
        self._stale_if_error = float(stale_if_error_seconds)
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

    async def peek_remaining(self, key: str) -> float | None:
        """Return remaining seconds for an entry, or None if missing/expired.

        Does not mutate LRU order. Used by the SWR refresh-trigger check.
        """
        async with self._lock:
            entry = self._data.get(key)
            if entry is None:
                return None
            expires_at, _ = entry
            remaining = expires_at - time.time()
            return remaining if remaining > 0 else None

    async def set(self, key: str, value: Any, expires_at: float) -> None:
        async with self._lock:
            self._data[key] = (expires_at, value)
            self._data.move_to_end(key)
            while len(self._data) > self._max_size:
                self._data.popitem(last=False)
            if self._stale_if_error > 0:
                # Remember this positive entry so a later verifier failure can be
                # ridden out while the token itself is still valid.
                self._stale[key] = (expires_at + self._stale_if_error, value)
                self._stale.move_to_end(key)
                while len(self._stale) > self._max_size:
                    self._stale.popitem(last=False)

    async def get_stale(self, key: str) -> "tuple[Any | None, str | None]":
        """Stale-if-error lookup. Returns ``(value, action)``.

        ``value`` is the cached AccessToken to serve, or None. ``action`` is a
        short diagnostic reason: ``used`` (serve it), ``window_elapsed`` (past the
        stale window), ``token_expired`` (token's own expiry is in the past),
        ``no_expiry`` (cannot prove validity), or None when no positive entry is
        remembered for this key.
        """
        if self._stale_if_error <= 0:
            return None, None
        async with self._lock:
            entry = self._stale.get(key)
            if entry is None:
                return None, None
            stale_deadline, value = entry
            now = time.time()
            if now > stale_deadline:
                self._stale.pop(key, None)
                return None, "window_elapsed"
            token_exp = getattr(value, "expires_at", None)
            if not token_exp:
                return None, "no_expiry"
            if token_exp <= now:
                return None, "token_expired"
            return value, "used"


def apply_token_verifier_cache_patch() -> None:
    """Install the verifier cache monkey-patch on GoogleTokenVerifier.

    Idempotent at import time: applying twice overwrites the same method
    with a functionally identical wrapper, re-creating the cache (no live
    tokens at startup).
    """
    if os.getenv("BOOMI_TOKEN_CACHE_DISABLE", "").lower() in ("true", "1", "yes"):
        logger.info("Token verifier cache DISABLED (BOOMI_TOKEN_CACHE_DISABLE)")
        return

    ttl_cap = int(os.getenv("BOOMI_TOKEN_CACHE_TTL_SECONDS", "300"))
    max_size = int(os.getenv("BOOMI_TOKEN_CACHE_MAX_SIZE", "256"))
    swr_enabled = os.getenv("BOOMI_TOKEN_CACHE_SWR", "").lower() in ("true", "1", "yes")
    swr_window = int(os.getenv("BOOMI_TOKEN_CACHE_SWR_WINDOW", "30"))
    stale_if_error_seconds = int(os.getenv("BOOMI_TOKEN_CACHE_STALE_IF_ERROR_SECONDS", "0"))

    cache = _TTLCache(max_size=max_size, stale_if_error_seconds=stale_if_error_seconds)

    # Track in-flight SWR refreshes so a slow Google response doesn't
    # spawn one new task per request during the SWR window.
    swr_inflight: set[str] = set()

    # Imports are deferred so this module can be imported in LOCAL_MODE
    # without pulling in the FastMCP auth stack.
    from fastmcp.server.auth.providers.google import GoogleTokenVerifier

    original_verify_token = GoogleTokenVerifier.verify_token

    def _cache_ttl(result_expires_at: float | None) -> float:
        """Compute TTL = min(token's remaining lifetime, ttl_cap)."""
        if not result_expires_at:
            return float(ttl_cap)
        return min(float(result_expires_at) - time.time(), float(ttl_cap))

    async def _refresh_in_background(self, token: str, key: str) -> None:
        """SWR: re-run verify_token and update the cache; never raise."""
        try:
            result = await original_verify_token(self, token)
        except Exception as exc:  # pragma: no cover - defensive
            logger.debug("SWR background refresh raised: %s", exc)
            return
        finally:
            swr_inflight.discard(key)
        if result is None:
            # Negative result: leave the existing cache entry alone; the
            # original 401 path will trigger on the next hit AFTER the
            # existing entry expires naturally. Do not poison the cache.
            return
        ttl = _cache_ttl(getattr(result, "expires_at", None))
        if ttl > 0:
            await cache.set(key, result, time.time() + ttl)

    async def cached_verify_token(self, token: str):
        key = hashlib.sha256(token.encode("utf-8")).hexdigest()[:32]
        hit = await cache.get(key)
        if hit is not None:
            if swr_enabled:
                remaining = await cache.peek_remaining(key)
                if remaining is not None and remaining < swr_window and key not in swr_inflight:
                    swr_inflight.add(key)
                    asyncio.create_task(_refresh_in_background(self, token, key))
            return hit
        logger.debug("Token cache MISS key=%s...", key[:8])
        result = await original_verify_token(self, token)
        if result is None:
            # Negative result: never cache. But if stale-if-error is on, ride out
            # a transient verifier failure by serving the last positive token
            # while it is still valid and inside the stale window.
            if stale_if_error_seconds > 0:
                stale_value, action = await cache.get_stale(key)
                if stale_value is not None:
                    _log_token_cache_event(
                        "token_cache_stale_if_error", action="used", key=key[:8]
                    )
                    return stale_value
                if action is not None:
                    _log_token_cache_event(
                        "token_cache_stale_if_error",
                        level="debug",
                        action=action,
                        key=key[:8],
                    )
            return None
        ttl = _cache_ttl(getattr(result, "expires_at", None))
        if ttl > 0:
            await cache.set(key, result, time.time() + ttl)
        return result

    GoogleTokenVerifier.verify_token = cached_verify_token
    logger.info(
        "Token verifier cache ENABLED (ttl_cap=%ds, max_size=%d, swr=%s, "
        "stale_if_error=%ds)",
        ttl_cap,
        max_size,
        "on" if swr_enabled else "off",
        stale_if_error_seconds,
    )
