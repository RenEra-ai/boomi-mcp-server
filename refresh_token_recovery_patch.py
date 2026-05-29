"""
Durable stale refresh-token recovery + sliding refresh expiry (recovery plan).

This patch sits BETWEEN refresh_token_grace_patch.py (Fix A, the 60s immediate
replay cache -- applied AFTER this one so it stays the OUTER fast path) and the
real FastMCP OAuthProxy rotation. It adds two capabilities:

1. Durable recovery. When a client returns hours/days later presenting a
   FastMCP refresh JWT that still verifies but whose ``_jti_mapping_store`` row
   and ``_refresh_token_store`` row were already deleted, we resolve the stale
   token through rt_recovery_backend's encrypted alias ledger to the latest
   live successor, run a fresh rotation, and return brand-new access+refresh
   tokens. We NEVER replay an old cached ``OAuthToken`` (its access token would
   be expired).

2. Sliding refresh expiry. FastMCP freezes the FastMCP refresh-token lifetime
   to the original auth-code deadline (proxy.py:1250/1267 read
   ``upstream_token_set.refresh_token_expires_at``, set once to now+30d at
   auth-code time, and count *down*). The normal path fixes this by pre-seeding
   ``refresh_token_expires_at = now + SLIDING_TTL`` before delegating to FastMCP
   -- which then reads exactly that value -- so the refresh window slides.

Design: HYBRID. The hot/normal path delegates to FastMCP's own
``exchange_refresh_token`` (pre-seed + delegate + write alias) so it tracks the
vendored rotation exactly. Only the cold recovery path reimplements rotation,
confining FastMCP-3.1.1 private-attr coupling there. A patch-time
``_compat_check`` and per-call runtime guards degrade gracefully (never 500) if
the SDK changes.

Env:
  BOOMI_RT_RECOVERY_ENABLED (default true; the backend honors it -- recovery is
    on only when a backend is passed)
  BOOMI_RT_RECOVERY_MAX_AGE_SECONDS (default 604800 = 7d)
  BOOMI_RT_RECOVERY_MAX_HOPS (default 16)
  BOOMI_RT_SLIDING_REFRESH_EXPIRY (default true)
  BOOMI_RT_SLIDING_REFRESH_TTL_SECONDS (default 2592000 = 30d)
"""

from __future__ import annotations

import asyncio
import logging
import os
import secrets
import time
from typing import Any

# Imported at module scope so tests can monkeypatch
# refresh_token_recovery_patch.AsyncOAuth2Client with a no-HTTP fake.
from authlib.integrations.httpx_client import AsyncOAuth2Client

from rt_recovery_backend import ALIAS_RECORD_VERSION, alias_ttl_seconds

logger = logging.getLogger("boomi.refresh_token_recovery")

_THIRTY_DAYS = 60 * 60 * 24 * 30

# Instance attributes the recovery reimplementation and sliding pre-seed touch.
# Checked at runtime (they are set per-instance, not on the class) so a FastMCP
# change degrades to the original exchange instead of 500-ing.
_REQUIRED_INSTANCE_ATTRS = (
    "_jti_mapping_store",
    "_upstream_token_store",
    "_refresh_token_store",
    "_upstream_client_id",
    "_upstream_client_secret",
    "_upstream_token_endpoint",
)

# Class-level methods/properties the patch overrides or calls.
_REQUIRED_CLASS_ATTRS = (
    "load_refresh_token",
    "exchange_refresh_token",
    "revoke_token",
    "_prepare_scopes_for_upstream_refresh",
    "_extract_upstream_claims",
    "jwt_issuer",
)


def _log_rt_event(event: str, *, level: str = "warning", **fields: Any) -> None:
    """Emit one structured diagnostic line: ``RT_DIAG event=<name> k=v ...``.

    NEVER pass raw token material: ``rt_hash`` values are truncated and every
    value is length-capped.
    """
    parts = [f"event={event}"]
    for key, value in fields.items():
        if value is None:
            continue
        text = str(value)
        if key.endswith("hash"):
            text = text[:16]
        elif len(text) > 200:
            text = text[:200] + "…"
        parts.append(f"{key}={text}")
    getattr(logger, level, logger.warning)("RT_DIAG " + " ".join(parts))


def _compat_check(oauth_proxy_cls) -> bool:
    """Verify the class-level surface the patch depends on still exists."""
    missing = [n for n in _REQUIRED_CLASS_ATTRS if not hasattr(oauth_proxy_cls, n)]
    if missing:
        _log_rt_event(
            "rt_recovery_compat_disabled",
            level="error",
            reason="missing_class_attrs",
            missing=",".join(missing),
        )
        return False
    return True


def apply_refresh_token_recovery_patch(*, recovery_backend=None) -> None:
    """Install durable recovery + sliding-expiry monkey-patches on OAuthProxy.

    Must be applied BEFORE apply_refresh_token_grace_patch so the grace 60s
    replay cache wraps this patch as the outer fast-path.

    Args:
        recovery_backend: Optional rt_recovery_backend.RefreshTokenRecoveryBackend.
            When None, durable recovery is disabled (the sliding-expiry fix still
            applies if enabled).
    """
    sliding_enabled = os.getenv("BOOMI_RT_SLIDING_REFRESH_EXPIRY", "true").lower() not in (
        "false",
        "0",
        "no",
    )
    sliding_ttl = int(os.getenv("BOOMI_RT_SLIDING_REFRESH_TTL_SECONDS", str(_THIRTY_DAYS)))
    max_age = int(os.getenv("BOOMI_RT_RECOVERY_MAX_AGE_SECONDS", "604800"))
    max_hops = int(os.getenv("BOOMI_RT_RECOVERY_MAX_HOPS", "16"))
    recovery_enabled = recovery_backend is not None

    if not recovery_enabled and not sliding_enabled:
        logger.info(
            "Refresh-token recovery patch NO-OP (recovery disabled, sliding disabled)"
        )
        return

    # Deferred imports keep this module's apply() aligned with the LOCAL-mode
    # convention used by the grace patch.
    from fastmcp.server.auth.oauth_proxy.models import (
        DEFAULT_ACCESS_TOKEN_EXPIRY_SECONDS,
        HTTP_TIMEOUT_SECONDS,
        JTIMapping,
        RefreshTokenMetadata,
        _hash_token,
    )
    from fastmcp.server.auth.oauth_proxy.proxy import OAuthProxy
    from mcp.server.auth.provider import RefreshToken, TokenError
    from mcp.shared.auth import OAuthToken

    if not _compat_check(OAuthProxy):
        # Leave methods unpatched: the grace patch + original FastMCP still
        # work; only durable recovery and sliding expiry are lost.
        return

    orig_load_refresh_token = OAuthProxy.load_refresh_token
    orig_exchange_refresh_token = OAuthProxy.exchange_refresh_token
    orig_revoke_token = OAuthProxy.revoke_token

    _compat_warned = {"done": False}

    def _instance_compatible(self) -> bool:
        missing = [n for n in _REQUIRED_INSTANCE_ATTRS if not hasattr(self, n)]
        if missing:
            if not _compat_warned["done"]:
                _compat_warned["done"] = True
                _log_rt_event(
                    "rt_recovery_compat_disabled",
                    level="error",
                    reason="missing_instance_attrs",
                    missing=",".join(missing),
                )
            return False
        return True

    def _safe_jti(self, token: str) -> str | None:
        """Return the refresh JWT's jti, or None if it does not verify."""
        try:
            return self.jwt_issuer.verify_token(token).get("jti")
        except Exception:  # noqa: BLE001 — unverifiable token => no jti
            return None

    async def _refresh_upstream(self, upstream_token_set, scopes):
        oauth_client = AsyncOAuth2Client(
            client_id=self._upstream_client_id,
            client_secret=self._upstream_client_secret.get_secret_value(),
            token_endpoint_auth_method=self._token_endpoint_auth_method,
            timeout=HTTP_TIMEOUT_SECONDS,
        )
        upstream_scopes = self._prepare_scopes_for_upstream_refresh(scopes)
        return await oauth_client.refresh_token(
            url=self._upstream_token_endpoint,
            refresh_token=upstream_token_set.refresh_token,
            scope=" ".join(upstream_scopes) if upstream_scopes else None,
            **self._extra_token_params,
        )

    def _compute_ttls(self, token_response, upstream_token_set):
        """Mirror FastMCP's TTL computation with the sliding-expiry override.

        Returns (new_expires_in, refresh_ttl, new_refresh_expires_in) and mutates
        upstream_token_set in place (access_token, expires_at, refresh_token,
        refresh_token_expires_at, raw_token_data) -- exactly like proxy.py.
        """
        now = time.time()
        if "expires_in" in token_response:
            new_expires_in = int(token_response["expires_in"])
        elif self._fallback_access_token_expiry_seconds is not None:
            new_expires_in = self._fallback_access_token_expiry_seconds
        else:
            new_expires_in = DEFAULT_ACCESS_TOKEN_EXPIRY_SECONDS
        upstream_token_set.access_token = token_response["access_token"]
        upstream_token_set.expires_at = now + new_expires_in

        new_refresh_expires_in = None
        new_upstream_refresh = token_response.get("refresh_token")
        if new_upstream_refresh:
            if new_upstream_refresh != upstream_token_set.refresh_token:
                upstream_token_set.refresh_token = new_upstream_refresh
            if "refresh_expires_in" in token_response:
                # Honor upstream's explicit value verbatim (overrides sliding).
                new_refresh_expires_in = int(token_response["refresh_expires_in"])
                upstream_token_set.refresh_token_expires_at = now + new_refresh_expires_in
            elif sliding_enabled:
                new_refresh_expires_in = sliding_ttl
                upstream_token_set.refresh_token_expires_at = now + sliding_ttl
            elif upstream_token_set.refresh_token_expires_at:
                new_refresh_expires_in = int(
                    upstream_token_set.refresh_token_expires_at - now
                )
            else:
                new_refresh_expires_in = _THIRTY_DAYS
                upstream_token_set.refresh_token_expires_at = now + _THIRTY_DAYS
        elif sliding_enabled:
            # Google commonly returns no refresh_token on refresh; still slide.
            new_refresh_expires_in = sliding_ttl
            upstream_token_set.refresh_token_expires_at = now + sliding_ttl

        upstream_token_set.raw_token_data = {
            **upstream_token_set.raw_token_data,
            **token_response,
        }
        refresh_ttl = new_refresh_expires_in or (
            int(upstream_token_set.refresh_token_expires_at - now)
            if upstream_token_set.refresh_token_expires_at
            else _THIRTY_DAYS
        )
        return new_expires_in, refresh_ttl, new_refresh_expires_in

    def _build_alias_record(client_id, scopes, *, successor_hash, successor_jti, upstream_token_id, successor_expires_at, reason):
        now = time.time()
        return {
            "version": ALIAS_RECORD_VERSION,
            "client_id": client_id,
            "scopes": list(scopes),
            "successor_rt_hash": successor_hash,
            "successor_refresh_jti": successor_jti,
            "upstream_token_id": upstream_token_id,
            "successor_expires_at": int(successor_expires_at),
            "created_at": now,
            "updated_at": now,
            "reason": reason,
        }

    async def _write_aliases(hashes, record):
        """Best-effort write of `record` under every hash in `hashes`."""
        if recovery_backend is None:
            return
        ttl = alias_ttl_seconds(record["successor_expires_at"], max_age)
        for h in dict.fromkeys(h for h in hashes if h):  # dedupe, keep order
            await recovery_backend.put_alias(h, dict(record), ttl)

    async def _preseed_sliding(self, upstream_token_id):
        """Pre-seed refresh_token_expires_at so FastMCP's own refresh_ttl slides."""
        upstream_token_set = await self._upstream_token_store.get(key=upstream_token_id)
        if upstream_token_set is None:
            return
        now = time.time()
        upstream_token_set.refresh_token_expires_at = now + sliding_ttl
        await self._upstream_token_store.put(
            key=upstream_token_id,
            value=upstream_token_set,
            ttl=max(sliding_ttl, 1),
        )

    async def _normal_exchange(self, client, refresh_token, scopes, jti_mapping):
        """Delegate to FastMCP rotation; pre-seed sliding, then write an alias."""
        upstream_token_id = jti_mapping.upstream_token_id
        if sliding_enabled:
            try:
                await _preseed_sliding(self, upstream_token_id)
            except Exception as exc:  # noqa: BLE001 — sliding is best-effort
                _log_rt_event(
                    "rt_sliding_preseed_failed",
                    reason=f"{type(exc).__name__}: {exc}",
                )

        # FastMCP rotation. Its TokenError/exceptions propagate unchanged.
        result = await orig_exchange_refresh_token(self, client, refresh_token, scopes)

        # Durable alias: consumed presented token -> newly issued successor.
        if recovery_enabled and result is not None and result.refresh_token:
            try:
                payload = self.jwt_issuer.verify_token(result.refresh_token)
                new_hash = _hash_token(result.refresh_token)
                record = _build_alias_record(
                    client.client_id,
                    scopes,
                    successor_hash=new_hash,
                    successor_jti=payload.get("jti"),
                    upstream_token_id=upstream_token_id,
                    successor_expires_at=payload.get("exp") or (time.time() + _THIRTY_DAYS),
                    reason="normal_rotation",
                )
                await _write_aliases([_hash_token(refresh_token.token)], record)
            except Exception as exc:  # noqa: BLE001 — alias write is best-effort
                _log_rt_event(
                    "rt_recovery_alias_write_failed",
                    reason=f"{type(exc).__name__}: {exc}",
                )
        return result

    async def _recover_and_rotate(self, client, refresh_token, scopes, record, visited):
        """Cold path: mint fresh tokens from the resolved live successor."""
        client_repr = (client.client_id or "<none>")[:8]
        if record.get("client_id") != client.client_id:
            _log_rt_event(
                "rt_recovery_reject", reason="client_mismatch", client=client_repr
            )
            raise TokenError("invalid_grant", "Invalid refresh token")

        successor_jti = record.get("successor_refresh_jti")
        successor_hash = record.get("successor_rt_hash")
        upstream_token_id = record.get("upstream_token_id")

        successor_mapping = (
            await self._jti_mapping_store.get(key=successor_jti) if successor_jti else None
        )
        if successor_mapping is None:
            _log_rt_event(
                "rt_recovery_miss", reason="dead_successor_jti", client=client_repr
            )
            raise TokenError("invalid_grant", "Refresh token mapping not found")
        # The live JTI mapping is authoritative for the upstream token id.
        upstream_token_id = successor_mapping.upstream_token_id or upstream_token_id

        upstream_token_set = await self._upstream_token_store.get(key=upstream_token_id)
        if upstream_token_set is None:
            _log_rt_event("rt_recovery_miss", reason="dead_upstream", client=client_repr)
            raise TokenError("invalid_grant", "Upstream token not found")
        if not upstream_token_set.refresh_token:
            _log_rt_event(
                "rt_recovery_miss", reason="no_upstream_refresh", client=client_repr
            )
            raise TokenError("invalid_grant", "Refresh not supported for this token")

        try:
            token_response = await _refresh_upstream(self, upstream_token_set, scopes)
        except Exception as exc:  # noqa: BLE001
            _log_rt_event(
                "rt_recovery_miss",
                reason="upstream_refresh_failed",
                detail=f"{type(exc).__name__}",
                client=client_repr,
            )
            raise TokenError("invalid_grant", f"Upstream refresh failed: {exc}") from exc

        new_expires_in, refresh_ttl, _new_refresh_expires_in = _compute_ttls(
            self, token_response, upstream_token_set
        )
        now = time.time()
        await self._upstream_token_store.put(
            key=upstream_token_set.upstream_token_id,
            value=upstream_token_set,
            ttl=max(refresh_ttl, new_expires_in, 1),
        )

        upstream_claims = await self._extract_upstream_claims(
            upstream_token_set.raw_token_data
        )

        new_access_jti = secrets.token_urlsafe(32)
        new_access = self.jwt_issuer.issue_access_token(
            client_id=client.client_id,
            scopes=scopes,
            jti=new_access_jti,
            expires_in=new_expires_in,
            upstream_claims=upstream_claims,
        )
        await self._jti_mapping_store.put(
            key=new_access_jti,
            value=JTIMapping(
                jti=new_access_jti,
                upstream_token_id=upstream_token_set.upstream_token_id,
                created_at=now,
            ),
            ttl=new_expires_in,
        )

        new_refresh_jti = secrets.token_urlsafe(32)
        new_refresh = self.jwt_issuer.issue_refresh_token(
            client_id=client.client_id,
            scopes=scopes,
            jti=new_refresh_jti,
            expires_in=refresh_ttl,
            upstream_claims=upstream_claims,
        )
        await self._jti_mapping_store.put(
            key=new_refresh_jti,
            value=JTIMapping(
                jti=new_refresh_jti,
                upstream_token_id=upstream_token_set.upstream_token_id,
                created_at=now,
            ),
            ttl=refresh_ttl,
        )

        # One-time-use: consume the LATEST LIVE successor (its jti + hash), not
        # the stale presented token (whose row was already gone).
        await self._jti_mapping_store.delete(key=successor_jti)
        if successor_hash:
            await self._refresh_token_store.delete(key=successor_hash)

        new_hash = _hash_token(new_refresh)
        await self._refresh_token_store.put(
            key=new_hash,
            value=RefreshTokenMetadata(
                client_id=client.client_id,
                scopes=scopes,
                expires_at=int(now) + refresh_ttl,
                created_at=now,
            ),
            ttl=refresh_ttl,
        )

        # Compact the chain: point every visited stale hash AND the consumed
        # successor hash directly at the freshly issued successor.
        alias_record = _build_alias_record(
            client.client_id,
            scopes,
            successor_hash=new_hash,
            successor_jti=new_refresh_jti,
            upstream_token_id=upstream_token_set.upstream_token_id,
            successor_expires_at=int(now) + refresh_ttl,
            reason="recovery_rotation",
        )
        await _write_aliases(list(visited) + [successor_hash], alias_record)

        _log_rt_event("rt_recovery_hit", level="info", client=client_repr)
        return OAuthToken(
            access_token=new_access,
            token_type="Bearer",
            expires_in=new_expires_in,
            refresh_token=new_refresh,
            scope=" ".join(scopes),
        )

    async def patched_load_refresh_token(self, client, refresh_token):
        result = await orig_load_refresh_token(self, client, refresh_token)
        if result is not None:
            # A present-but-expired metadata row is the frozen-TTL window
            # before Mongo physically sweeps it. If returned as-is, the MCP
            # token handler rejects it ("refresh token has expired") BEFORE
            # exchange_refresh_token runs (token.py refresh-grant path) -- so
            # recovery would never fire for the exact stale-but-valid case it
            # targets. When recovery is enabled, fall through to the alias path
            # for that case; otherwise return unchanged (prior behavior).
            expired = bool(result.expires_at and result.expires_at < time.time())
            if not (recovery_enabled and expired):
                return result
            _log_rt_event("rt_recovery_expired_metadata", level="info")
        if not recovery_enabled:
            return None
        try:
            if not _instance_compatible(self):
                return None
            try:
                payload = self.jwt_issuer.verify_token(refresh_token)
            except Exception:  # noqa: BLE001 — invalid/expired/forged
                _log_rt_event("rt_recovery_reject", level="info", reason="jwt_invalid")
                return None
            if payload.get("token_use") != "refresh":
                _log_rt_event("rt_recovery_reject", reason="token_use")
                return None
            if payload.get("client_id") != client.client_id:
                _log_rt_event("rt_recovery_reject", reason="client_mismatch")
                return None
            exp = payload.get("exp")
            if exp is not None and exp < time.time():
                _log_rt_event("rt_recovery_reject", level="info", reason="jwt_expired")
                return None
            record, _visited = await recovery_backend.resolve_latest(
                _hash_token(refresh_token), max_hops
            )
            if record is None:
                _log_rt_event("rt_recovery_miss", reason="no_alias")
                return None
            if record.get("client_id") != client.client_id:
                _log_rt_event("rt_recovery_reject", reason="alias_client_mismatch")
                return None
            _log_rt_event("rt_recovery_load_ok", level="info")
            return RefreshToken(
                token=refresh_token,
                client_id=record["client_id"],
                scopes=record.get("scopes") or [],
                expires_at=record.get("successor_expires_at"),
            )
        except asyncio.CancelledError:
            raise
        except Exception as exc:  # noqa: BLE001 — degrade to plain miss
            _log_rt_event(
                "rt_recovery_backend_unavailable",
                reason=f"{type(exc).__name__}: {exc}",
            )
            return None

    async def patched_exchange_refresh_token(self, client, refresh_token, scopes):
        # ---- Path detection (guarded; failures degrade to plain orig) ----
        try:
            if not _instance_compatible(self):
                return await orig_exchange_refresh_token(self, client, refresh_token, scopes)
            refresh_jti = _safe_jti(self, refresh_token.token)
            jti_mapping = (
                await self._jti_mapping_store.get(key=refresh_jti) if refresh_jti else None
            )
            record = None
            visited: list[str] = []
            if jti_mapping is None and recovery_enabled:
                record, visited = await recovery_backend.resolve_latest(
                    _hash_token(refresh_token.token), max_hops
                )
        except asyncio.CancelledError:
            raise
        except Exception as exc:  # noqa: BLE001 — detection failed; degrade
            _log_rt_event(
                "rt_recovery_unavailable",
                reason=f"detect_failed:{type(exc).__name__}",
            )
            return await orig_exchange_refresh_token(self, client, refresh_token, scopes)

        # ---- Committed paths (TokenError/exceptions propagate) ----
        # Both-exist => NORMAL wins: a live jti mapping means the presented
        # token is current and unconsumed; honoring an alias here could
        # double-rotate the live successor.
        if jti_mapping is not None:
            return await _normal_exchange(self, client, refresh_token, scopes, jti_mapping)
        if record is not None:
            return await _recover_and_rotate(
                self, client, refresh_token, scopes, record, visited
            )
        # Neither path applies: let orig produce the canonical invalid_grant.
        return await orig_exchange_refresh_token(self, client, refresh_token, scopes)

    async def patched_revoke_token(self, token):
        # Clean up recovery aliases + jti mapping before delegating. Best-effort:
        # cleanup failures never block the original revoke.
        try:
            if isinstance(token, RefreshToken):
                if recovery_enabled:
                    await recovery_backend.delete(_hash_token(token.token))
                try:
                    payload = self.jwt_issuer.verify_token(token.token)
                    jti = payload.get("jti")
                    if jti and hasattr(self, "_jti_mapping_store"):
                        await self._jti_mapping_store.delete(key=jti)
                except Exception:  # noqa: BLE001 — unverifiable token, skip jti delete
                    pass
        except asyncio.CancelledError:
            raise
        except Exception as exc:  # noqa: BLE001
            _log_rt_event(
                "rt_recovery_revoke_cleanup_failed",
                reason=f"{type(exc).__name__}: {exc}",
            )
        return await orig_revoke_token(self, token)

    OAuthProxy.load_refresh_token = patched_load_refresh_token
    OAuthProxy.exchange_refresh_token = patched_exchange_refresh_token
    OAuthProxy.revoke_token = patched_revoke_token
    logger.info(
        "Refresh-token recovery patch ENABLED (recovery=%s, sliding=%s, "
        "sliding_ttl=%ds, max_age=%ds, max_hops=%d)",
        "on" if recovery_enabled else "off",
        "on" if sliding_enabled else "off",
        sliding_ttl,
        max_age,
        max_hops,
    )
