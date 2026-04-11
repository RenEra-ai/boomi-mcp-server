"""
Verified storage wrapper for OAuth token persistence.

Wraps an AsyncKeyValue store to verify that writes can be read back
with the correct value, catching storage backend failures before
issuing tokens based on unverified or stale writes.

Preserves the safeguard from the vendored FastMCP fork (commit 4dbabd3)
as a project-local wrapper instead of maintaining a full fork.
"""

import json
import logging

from mcp.server.auth.provider import TokenError

logger = logging.getLogger(__name__)


class VerifiedStorage:
    """AsyncKeyValue wrapper that verifies writes persist correctly.

    Intercepts put() calls and immediately reads back the written value
    to confirm the storage backend accepted the write and the new value
    matches what was written. Raises TokenError if verification fails,
    preventing tokens from being issued against unverified or stale
    storage state.

    All other operations (get, delete, list, etc.) pass through unchanged.
    """

    def __init__(self, key_value):
        self._kv = key_value

    async def put(self, *, key, value, **kwargs):
        await self._kv.put(key=key, value=value, **kwargs)
        get_kwargs = {k: v for k, v in kwargs.items() if k in ("collection",)}
        verified = await self._kv.get(key=key, **get_kwargs)
        key_prefix = key[:8] if key else key
        if verified is None:
            logger.error(
                "Storage verification failed: could not read back key %s",
                key_prefix,
            )
            raise TokenError(
                "invalid_request",
                f"Failed to persist value for key {key_prefix}...",
            )
        if json.dumps(verified, sort_keys=True) != json.dumps(
            dict(value) if not isinstance(value, dict) else value,
            sort_keys=True,
        ):
            logger.error(
                "Storage verification failed: stale value for key %s",
                key_prefix,
            )
            raise TokenError(
                "invalid_request",
                "Token storage verification failed: written value does not match read-back",
            )
        logger.debug(
            "Storage verification passed for key %s",
            key_prefix,
        )

    async def get(self, *, key, **kwargs):
        return await self._kv.get(key=key, **kwargs)

    async def delete(self, *, key, **kwargs):
        return await self._kv.delete(key=key, **kwargs)

    async def list(self, **kwargs):
        return await self._kv.list(**kwargs)

    def __getattr__(self, name):
        return getattr(self._kv, name)
