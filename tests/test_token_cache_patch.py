"""Tests for the Google verifier cache patch (Fix B)."""

from __future__ import annotations

import asyncio
import importlib
import sys
import time
from types import SimpleNamespace

import pytest


# ---------- helpers ----------

def _fresh_module():
    sys.modules.pop("token_cache_patch", None)
    return importlib.import_module("token_cache_patch")


def _run(coro):
    return asyncio.new_event_loop().run_until_complete(coro)


def _make_token(label: str = "AT") -> str:
    """Synthesize a unique token string (cache key is sha256 of this)."""
    return f"{label}-{time.time_ns()}"


# ---------- _TTLCache primitive ----------

def test_ttlcache_hit_and_miss():
    mod = _fresh_module()
    cache = mod._TTLCache(max_size=4)
    _run(cache.set("k1", "v1", time.time() + 60))
    assert _run(cache.get("k1")) == "v1"
    assert _run(cache.get("missing")) is None


def test_ttlcache_expiry_evicts():
    mod = _fresh_module()
    cache = mod._TTLCache(max_size=4)
    _run(cache.set("k1", "v1", time.time() - 1))
    assert _run(cache.get("k1")) is None
    assert "k1" not in cache._data


def test_ttlcache_lru_evicts_at_capacity():
    mod = _fresh_module()
    cache = mod._TTLCache(max_size=2)
    far = time.time() + 60
    _run(cache.set("k1", "v1", far))
    _run(cache.set("k2", "v2", far))
    assert _run(cache.get("k1")) == "v1"  # touch k1 → MRU
    _run(cache.set("k3", "v3", far))
    assert _run(cache.get("k2")) is None
    assert _run(cache.get("k1")) == "v1"
    assert _run(cache.get("k3")) == "v3"


def test_ttlcache_peek_remaining():
    mod = _fresh_module()
    cache = mod._TTLCache(max_size=4)
    _run(cache.set("k1", "v1", time.time() + 30))
    remaining = _run(cache.peek_remaining("k1"))
    assert remaining is not None and 25 < remaining <= 30
    assert _run(cache.peek_remaining("missing")) is None


# ---------- apply_token_verifier_cache_patch ----------

@pytest.fixture
def restore_verifier():
    """Snapshot GoogleTokenVerifier.verify_token, yield, then restore."""
    from fastmcp.server.auth.providers.google import GoogleTokenVerifier
    saved = GoogleTokenVerifier.verify_token
    yield GoogleTokenVerifier
    GoogleTokenVerifier.verify_token = saved


def test_disabled_via_env_is_noop(monkeypatch, restore_verifier):
    monkeypatch.setenv("BOOMI_TOKEN_CACHE_DISABLE", "true")
    GoogleTokenVerifier = restore_verifier
    pre = GoogleTokenVerifier.verify_token

    mod = _fresh_module()
    mod.apply_token_verifier_cache_patch()
    assert GoogleTokenVerifier.verify_token is pre


def test_cache_serves_repeat_calls_for_same_token(monkeypatch, restore_verifier):
    """5 sequential calls with the same token → original invoked once."""
    monkeypatch.setenv("BOOMI_TOKEN_CACHE_DISABLE", "false")
    monkeypatch.setenv("BOOMI_TOKEN_CACHE_TTL_SECONDS", "300")
    GoogleTokenVerifier = restore_verifier

    call_count = {"n": 0}
    from fastmcp.server.auth.auth import AccessToken

    async def stubbed(self, token):
        call_count["n"] += 1
        return AccessToken(
            token=token,
            client_id="cid",
            scopes=["openid"],
            expires_at=int(time.time() + 60),
            claims={"sub": "user-1"},
        )

    GoogleTokenVerifier.verify_token = stubbed

    mod = _fresh_module()
    mod.apply_token_verifier_cache_patch()

    verifier = SimpleNamespace()
    tok = _make_token()

    async def _run_many():
        return [await GoogleTokenVerifier.verify_token(verifier, tok) for _ in range(5)]

    results = _run(_run_many())
    assert call_count["n"] == 1
    assert all(r.token == tok for r in results)
    assert all(r.claims["sub"] == "user-1" for r in results)


def test_negative_results_not_cached(monkeypatch, restore_verifier):
    """Stubbed verify_token returning None: every call invokes the original."""
    monkeypatch.setenv("BOOMI_TOKEN_CACHE_DISABLE", "false")
    GoogleTokenVerifier = restore_verifier

    call_count = {"n": 0}

    async def stubbed_none(self, token):
        call_count["n"] += 1
        return None

    GoogleTokenVerifier.verify_token = stubbed_none

    mod = _fresh_module()
    mod.apply_token_verifier_cache_patch()

    verifier = SimpleNamespace()
    tok = _make_token()

    async def _run_many():
        return [await GoogleTokenVerifier.verify_token(verifier, tok) for _ in range(5)]

    results = _run(_run_many())
    assert call_count["n"] == 5
    assert all(r is None for r in results)


def test_ttl_cap_overrides_long_token_lifetime(monkeypatch, restore_verifier):
    """expires_at=now+3600 with cap=1: cache entry evicts at the cap."""
    monkeypatch.setenv("BOOMI_TOKEN_CACHE_DISABLE", "false")
    monkeypatch.setenv("BOOMI_TOKEN_CACHE_TTL_SECONDS", "1")
    GoogleTokenVerifier = restore_verifier

    call_count = {"n": 0}
    from fastmcp.server.auth.auth import AccessToken

    async def stubbed(self, token):
        call_count["n"] += 1
        return AccessToken(
            token=token,
            client_id="cid",
            scopes=["openid"],
            expires_at=int(time.time() + 3600),  # long lifetime
            claims={},
        )

    GoogleTokenVerifier.verify_token = stubbed

    mod = _fresh_module()
    mod.apply_token_verifier_cache_patch()

    verifier = SimpleNamespace()
    tok = _make_token()

    async def _scenario():
        await GoogleTokenVerifier.verify_token(verifier, tok)  # n=1
        await GoogleTokenVerifier.verify_token(verifier, tok)  # cache hit
        await asyncio.sleep(1.2)  # exceed the 1s TTL cap
        await GoogleTokenVerifier.verify_token(verifier, tok)  # n=2

    _run(_scenario())
    assert call_count["n"] == 2


def test_different_tokens_do_not_collide(monkeypatch, restore_verifier):
    monkeypatch.setenv("BOOMI_TOKEN_CACHE_DISABLE", "false")
    GoogleTokenVerifier = restore_verifier

    call_log: list[str] = []
    from fastmcp.server.auth.auth import AccessToken

    async def stubbed(self, token):
        call_log.append(token)
        return AccessToken(
            token=token, client_id="cid", scopes=["openid"],
            expires_at=int(time.time() + 60), claims={},
        )

    GoogleTokenVerifier.verify_token = stubbed

    mod = _fresh_module()
    mod.apply_token_verifier_cache_patch()

    verifier = SimpleNamespace()
    tok_a, tok_b = _make_token("A"), _make_token("B")

    async def _scenario():
        await GoogleTokenVerifier.verify_token(verifier, tok_a)
        await GoogleTokenVerifier.verify_token(verifier, tok_b)
        await GoogleTokenVerifier.verify_token(verifier, tok_a)  # cache hit
        await GoogleTokenVerifier.verify_token(verifier, tok_b)  # cache hit

    _run(_scenario())
    assert call_log == [tok_a, tok_b]


def test_cache_key_is_sha256_prefix(monkeypatch, restore_verifier):
    """The stored cache key never contains the plaintext token."""
    monkeypatch.setenv("BOOMI_TOKEN_CACHE_DISABLE", "false")
    GoogleTokenVerifier = restore_verifier

    from fastmcp.server.auth.auth import AccessToken

    async def stubbed(self, token):
        return AccessToken(
            token=token, client_id="cid", scopes=["openid"],
            expires_at=int(time.time() + 60), claims={},
        )

    GoogleTokenVerifier.verify_token = stubbed

    mod = _fresh_module()
    mod.apply_token_verifier_cache_patch()

    # Grab the closure's cache reference via the patched function
    patched_fn = GoogleTokenVerifier.verify_token
    closure_vars = {
        cell_name: cell.cell_contents
        for cell_name, cell in zip(patched_fn.__code__.co_freevars, patched_fn.__closure__)
    }
    cache = closure_vars["cache"]

    verifier = SimpleNamespace()
    plaintext_token = "MY-SECRET-TOKEN-PLAINTEXT"

    _run(GoogleTokenVerifier.verify_token(verifier, plaintext_token))

    cache_keys = list(cache._data.keys())
    assert plaintext_token not in cache_keys
    assert all(len(k) == 32 for k in cache_keys)  # sha256 prefix length


def test_swr_returns_stale_and_triggers_background_refresh(monkeypatch, restore_verifier):
    """With SWR enabled, an entry inside the SWR window returns immediately
    and schedules a background refresh."""
    monkeypatch.setenv("BOOMI_TOKEN_CACHE_DISABLE", "false")
    monkeypatch.setenv("BOOMI_TOKEN_CACHE_SWR", "true")
    monkeypatch.setenv("BOOMI_TOKEN_CACHE_SWR_WINDOW", "60")  # always within window
    monkeypatch.setenv("BOOMI_TOKEN_CACHE_TTL_SECONDS", "50")
    GoogleTokenVerifier = restore_verifier

    call_count = {"n": 0}
    from fastmcp.server.auth.auth import AccessToken

    async def stubbed(self, token):
        call_count["n"] += 1
        return AccessToken(
            token=token, client_id="cid", scopes=["openid"],
            expires_at=int(time.time() + 50), claims={"call": call_count["n"]},
        )

    GoogleTokenVerifier.verify_token = stubbed

    mod = _fresh_module()
    mod.apply_token_verifier_cache_patch()

    verifier = SimpleNamespace()
    tok = _make_token()

    async def _scenario():
        first = await GoogleTokenVerifier.verify_token(verifier, tok)  # n=1, cache
        # Subsequent call: hit (returns stale immediately) + schedules refresh
        second = await GoogleTokenVerifier.verify_token(verifier, tok)
        # Let the background task run
        await asyncio.sleep(0.05)
        return first, second

    first, second = _run(_scenario())
    # The second call returned the cached value (call=1) BEFORE the refresh
    # completed, but the background refresh fired (call_count went to 2).
    assert first.claims["call"] == 1
    assert second.claims["call"] == 1  # stale-while-revalidate
    assert call_count["n"] == 2  # background refresh ran


# ---------- stale-if-error ----------

from types import SimpleNamespace as _NS  # noqa: E402


def test_ttlcache_stale_serves_recent_positive():
    """A recently-expired positive entry is served inside the stale window while
    the token's own expiry is still in the future."""
    mod = _fresh_module()
    cache = mod._TTLCache(max_size=4, stale_if_error_seconds=60)
    now = time.time()
    value = _NS(expires_at=now + 300)  # token still valid for 5 min
    _run(cache.set("k1", value, now - 1))  # cache entry already expired
    assert _run(cache.get("k1")) is None  # gone from the live cache
    served, action = _run(cache.get_stale("k1"))
    assert served is value
    assert action == "used"


def test_ttlcache_stale_window_elapsed_not_served():
    mod = _fresh_module()
    cache = mod._TTLCache(max_size=4, stale_if_error_seconds=60)
    now = time.time()
    value = _NS(expires_at=now + 300)
    _run(cache.set("k1", value, now - 100))  # stale_deadline = now-40 (elapsed)
    served, action = _run(cache.get_stale("k1"))
    assert served is None
    assert action == "window_elapsed"


def test_ttlcache_stale_not_served_when_token_expired():
    mod = _fresh_module()
    cache = mod._TTLCache(max_size=4, stale_if_error_seconds=60)
    now = time.time()
    value = _NS(expires_at=now - 1)  # token itself already expired
    _run(cache.set("k1", value, now - 1))  # within stale window, but token dead
    served, action = _run(cache.get_stale("k1"))
    assert served is None
    assert action == "token_expired"


def test_ttlcache_stale_disabled_when_zero():
    mod = _fresh_module()
    cache = mod._TTLCache(max_size=4, stale_if_error_seconds=0)
    now = time.time()
    _run(cache.set("k1", _NS(expires_at=now + 300), now - 1))
    # No stale tracking when the window is 0 -> nothing recorded, nothing served.
    assert cache._stale == {}
    served, action = _run(cache.get_stale("k1"))
    assert served is None and action is None


def test_stale_if_error_serves_last_positive_on_verifier_failure(monkeypatch, restore_verifier):
    """End-to-end: positive cached, cache TTL elapses, verifier then fails ->
    the still-valid token is served stale instead of a 401."""
    monkeypatch.setenv("BOOMI_TOKEN_CACHE_DISABLE", "false")
    monkeypatch.setenv("BOOMI_TOKEN_CACHE_TTL_SECONDS", "1")  # cache entry ~1s
    monkeypatch.setenv("BOOMI_TOKEN_CACHE_STALE_IF_ERROR_SECONDS", "60")
    GoogleTokenVerifier = restore_verifier

    state = {"n": 0}
    from fastmcp.server.auth.auth import AccessToken

    async def stubbed(self, token):
        state["n"] += 1
        if state["n"] == 1:
            return AccessToken(
                token=token, client_id="cid", scopes=["openid"],
                expires_at=int(time.time() + 300), claims={"sub": "u"},
            )
        return None  # verifier now failing transiently

    GoogleTokenVerifier.verify_token = stubbed
    mod = _fresh_module()
    mod.apply_token_verifier_cache_patch()
    verifier = SimpleNamespace()
    tok = _make_token()

    async def _scenario():
        first = await GoogleTokenVerifier.verify_token(verifier, tok)   # positive, cached
        await asyncio.sleep(1.1)                                        # cache entry expires
        second = await GoogleTokenVerifier.verify_token(verifier, tok)  # None -> stale served
        return first, second

    first, second = _run(_scenario())
    assert first is not None
    assert second is not None and second.token == tok
    assert state["n"] == 2


def test_stale_if_error_disabled_by_default(monkeypatch, restore_verifier):
    """Without the stale window set, a verifier failure after expiry 401s as before."""
    monkeypatch.setenv("BOOMI_TOKEN_CACHE_DISABLE", "false")
    monkeypatch.setenv("BOOMI_TOKEN_CACHE_TTL_SECONDS", "1")
    monkeypatch.delenv("BOOMI_TOKEN_CACHE_STALE_IF_ERROR_SECONDS", raising=False)
    GoogleTokenVerifier = restore_verifier

    state = {"n": 0}
    from fastmcp.server.auth.auth import AccessToken

    async def stubbed(self, token):
        state["n"] += 1
        if state["n"] == 1:
            return AccessToken(
                token=token, client_id="cid", scopes=["openid"],
                expires_at=int(time.time() + 300), claims={"sub": "u"},
            )
        return None

    GoogleTokenVerifier.verify_token = stubbed
    mod = _fresh_module()
    mod.apply_token_verifier_cache_patch()
    verifier = SimpleNamespace()
    tok = _make_token()

    async def _scenario():
        first = await GoogleTokenVerifier.verify_token(verifier, tok)
        await asyncio.sleep(1.1)
        second = await GoogleTokenVerifier.verify_token(verifier, tok)
        return first, second

    first, second = _run(_scenario())
    assert first is not None
    assert second is None  # no stale-if-error -> normal 401 path
