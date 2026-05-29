"""Tests for refresh_token_recovery_patch (durable recovery + sliding expiry).

Harness mirrors the existing OAuth patch tests: plain asyncio via _run(), a
restore fixture that saves/restores the patched OAuthProxy class methods, and
SimpleNamespace/dict fakes. The patch captures whatever OAuthProxy.* methods are
installed at apply time as its `orig_*`, so each test sets fake originals BEFORE
applying. No real HTTP: refresh_token_recovery_patch.AsyncOAuth2Client is
monkeypatched with a fake.
"""

from __future__ import annotations

import asyncio
import logging
import time
from types import SimpleNamespace

import pytest
from cryptography.fernet import Fernet
from key_value.aio.stores.memory import MemoryStore
from key_value.aio.wrappers.encryption import FernetEncryptionWrapper

from fastmcp.server.auth.oauth_proxy.models import _hash_token
from mcp.server.auth.provider import RefreshToken, TokenError
from mcp.shared.auth import OAuthToken

import refresh_token_recovery_patch as patch_mod
from rt_recovery_backend import RefreshTokenRecoveryBackend


def _run(coro):
    return asyncio.new_event_loop().run_until_complete(coro)


@pytest.fixture
def restore_oauth_proxy():
    from fastmcp.server.auth.oauth_proxy.proxy import OAuthProxy

    saved = (
        OAuthProxy.load_refresh_token,
        OAuthProxy.exchange_refresh_token,
        OAuthProxy.revoke_token,
    )
    yield OAuthProxy
    (
        OAuthProxy.load_refresh_token,
        OAuthProxy.exchange_refresh_token,
        OAuthProxy.revoke_token,
    ) = saved


# ----------------------------- fakes -----------------------------

class _FakeStore:
    def __init__(self):
        self.data: dict = {}
        self.deleted: list = []
        self.put_keys: list = []

    async def get(self, *, key):
        return self.data.get(key)

    async def put(self, *, key, value, ttl=None):
        self.put_keys.append(key)
        self.data[key] = value

    async def delete(self, *, key):
        self.deleted.append(key)
        self.data.pop(key, None)


class _FakeIssuer:
    def __init__(self):
        self.payloads: dict = {}   # token -> payload dict OR Exception
        self.issued: list = []     # (kind, jti, expires_in)

    def verify_token(self, token):
        p = self.payloads.get(token)
        if isinstance(p, Exception):
            raise p
        if p is None:
            raise ValueError(f"unverifiable token: {token!r}")
        return p

    def issue_access_token(self, *, client_id, scopes, jti, expires_in, upstream_claims=None):
        self.issued.append(("access", jti, expires_in))
        return f"AT-{jti}"

    def issue_refresh_token(self, *, client_id, scopes, jti, expires_in, upstream_claims=None):
        self.issued.append(("refresh", jti, expires_in))
        tok = f"RT-{jti}"
        self.payloads[tok] = {
            "jti": jti,
            "client_id": client_id,
            "token_use": "refresh",
            "exp": time.time() + expires_in,
            "scope": " ".join(scopes),
        }
        return tok


def _set_fake_oauth(monkeypatch, *, response=None, raise_exc=None):
    resp = response or {"access_token": "up-AT", "expires_in": 3600}

    class _FakeOAuthClient:
        def __init__(self, **kwargs):
            pass

        async def refresh_token(self, **kwargs):
            if raise_exc is not None:
                raise raise_exc
            return dict(resp)

    monkeypatch.setattr(patch_mod, "AsyncOAuth2Client", _FakeOAuthClient)


def _make_self(issuer, *, jti=None, upstream=None, refresh=None, fallback=None,
               omit=()):
    self = SimpleNamespace()
    if "jwt_issuer" not in omit:
        self.jwt_issuer = issuer
    if "_jti_mapping_store" not in omit:
        self._jti_mapping_store = jti if jti is not None else _FakeStore()
    if "_upstream_token_store" not in omit:
        self._upstream_token_store = upstream if upstream is not None else _FakeStore()
    if "_refresh_token_store" not in omit:
        self._refresh_token_store = refresh if refresh is not None else _FakeStore()
    self._upstream_client_id = "up-cid"
    self._upstream_client_secret = SimpleNamespace(get_secret_value=lambda: "up-secret")
    self._upstream_token_endpoint = "https://up.example/token"
    self._token_endpoint_auth_method = None
    self._extra_token_params = {}
    self._fallback_access_token_expiry_seconds = fallback
    self._prepare_scopes_for_upstream_refresh = lambda scopes: scopes

    async def _extract(raw):
        return None

    self._extract_upstream_claims = _extract
    return self


def _upstream_set(*, upstream_token_id="ut-1", refresh_token="up-rt", refresh_token_expires_at=None):
    if refresh_token_expires_at is None:
        refresh_token_expires_at = time.time() + 1000
    return SimpleNamespace(
        upstream_token_id=upstream_token_id,
        access_token="up-old-at",
        refresh_token=refresh_token,
        refresh_token_expires_at=refresh_token_expires_at,
        expires_at=time.time() + 3600,
        token_type="Bearer",
        scope="openid",
        client_id="client-A",
        created_at=time.time(),
        raw_token_data={},
    )


def _mem_recovery_backend():
    backing = MemoryStore()
    wrapped = FernetEncryptionWrapper(key_value=backing, fernet=Fernet(Fernet.generate_key()))
    return RefreshTokenRecoveryBackend(wrapped)


def _alias(*, client_id="client-A", successor_hash="live-hash", successor_jti="jti-live",
           upstream_token_id="ut-1", successor_expires_at=None, scopes=("openid",)):
    if successor_expires_at is None:
        successor_expires_at = int(time.time()) + 3600
    return {
        "version": 1,
        "client_id": client_id,
        "scopes": list(scopes),
        "successor_rt_hash": successor_hash,
        "successor_refresh_jti": successor_jti,
        "upstream_token_id": upstream_token_id,
        "successor_expires_at": successor_expires_at,
        "created_at": time.time(),
        "updated_at": time.time(),
        "reason": "test",
    }


CLIENT = SimpleNamespace(client_id="client-A")


def _apply(monkeypatch, OAuthProxy, recovery_backend, *, orig_load=None, orig_exchange=None,
           orig_revoke=None, env=None):
    for k, v in (env or {}).items():
        monkeypatch.setenv(k, v)

    async def _default_load(self, client, refresh_token):
        return None

    async def _default_exchange(self, client, refresh_token, scopes):
        raise AssertionError("orig exchange must not be called in this test")

    async def _default_revoke(self, token):
        return None

    OAuthProxy.load_refresh_token = orig_load or _default_load
    OAuthProxy.exchange_refresh_token = orig_exchange or _default_exchange
    OAuthProxy.revoke_token = orig_revoke or _default_revoke
    patch_mod.apply_refresh_token_recovery_patch(recovery_backend=recovery_backend)


def _diag(caplog):
    return [r.message for r in caplog.records if "RT_DIAG" in r.message]


# ----------------------------- patch gating -----------------------------

def test_disabled_when_recovery_off_and_sliding_off(monkeypatch, restore_oauth_proxy):
    OAuthProxy = restore_oauth_proxy
    sentinel = OAuthProxy.exchange_refresh_token
    _apply(
        monkeypatch, OAuthProxy, None,
        orig_exchange=sentinel,
        env={"BOOMI_RT_SLIDING_REFRESH_EXPIRY": "false"},
    )
    # Nothing patched: the method is still the sentinel we installed.
    assert OAuthProxy.exchange_refresh_token is sentinel


# ----------------------------- load_refresh_token -----------------------------

def test_load_passthrough_when_orig_hits(monkeypatch, restore_oauth_proxy):
    OAuthProxy = restore_oauth_proxy
    issuer = _FakeIssuer()
    expected = RefreshToken(token="rt", client_id="client-A", scopes=["openid"], expires_at=None)

    async def orig_load(self, client, refresh_token):
        return expected

    _apply(monkeypatch, OAuthProxy, _mem_recovery_backend(), orig_load=orig_load)
    self = _make_self(issuer)
    out = _run(OAuthProxy.load_refresh_token(self, CLIENT, "rt"))
    assert out is expected


def test_load_miss_no_alias_returns_none(monkeypatch, restore_oauth_proxy, caplog):
    OAuthProxy = restore_oauth_proxy
    issuer = _FakeIssuer()
    issuer.payloads["STALE"] = {"jti": "j", "client_id": "client-A", "token_use": "refresh", "exp": time.time() + 99}
    _apply(monkeypatch, OAuthProxy, _mem_recovery_backend())
    self = _make_self(issuer)
    with caplog.at_level(logging.WARNING, logger="boomi.refresh_token_recovery"):
        out = _run(OAuthProxy.load_refresh_token(self, CLIENT, "STALE"))
    assert out is None
    assert any("rt_recovery_miss" in m and "no_alias" in m for m in _diag(caplog))


def test_load_reject_invalid_jwt(monkeypatch, restore_oauth_proxy, caplog):
    OAuthProxy = restore_oauth_proxy
    issuer = _FakeIssuer()
    issuer.payloads["BAD"] = ValueError("nope")
    _apply(monkeypatch, OAuthProxy, _mem_recovery_backend())
    self = _make_self(issuer)
    with caplog.at_level(logging.INFO, logger="boomi.refresh_token_recovery"):
        out = _run(OAuthProxy.load_refresh_token(self, CLIENT, "BAD"))
    assert out is None
    assert any("rt_recovery_reject" in m and "jwt_invalid" in m for m in _diag(caplog))


def test_load_reject_wrong_token_use(monkeypatch, restore_oauth_proxy, caplog):
    OAuthProxy = restore_oauth_proxy
    issuer = _FakeIssuer()
    issuer.payloads["ACC"] = {"jti": "j", "client_id": "client-A", "exp": time.time() + 99}  # no token_use
    _apply(monkeypatch, OAuthProxy, _mem_recovery_backend())
    self = _make_self(issuer)
    with caplog.at_level(logging.WARNING, logger="boomi.refresh_token_recovery"):
        out = _run(OAuthProxy.load_refresh_token(self, CLIENT, "ACC"))
    assert out is None
    assert any("rt_recovery_reject" in m and "token_use" in m for m in _diag(caplog))


def test_load_reject_client_mismatch(monkeypatch, restore_oauth_proxy, caplog):
    OAuthProxy = restore_oauth_proxy
    issuer = _FakeIssuer()
    issuer.payloads["X"] = {"jti": "j", "client_id": "other", "token_use": "refresh", "exp": time.time() + 99}
    _apply(monkeypatch, OAuthProxy, _mem_recovery_backend())
    self = _make_self(issuer)
    with caplog.at_level(logging.WARNING, logger="boomi.refresh_token_recovery"):
        out = _run(OAuthProxy.load_refresh_token(self, CLIENT, "X"))
    assert out is None
    assert any("rt_recovery_reject" in m and "client_mismatch" in m for m in _diag(caplog))


def test_load_reject_expired_jwt(monkeypatch, restore_oauth_proxy, caplog):
    OAuthProxy = restore_oauth_proxy
    issuer = _FakeIssuer()
    # verify_token does NOT raise but exp is in the past -> our explicit check.
    issuer.payloads["EXP"] = {"jti": "j", "client_id": "client-A", "token_use": "refresh", "exp": time.time() - 5}
    _apply(monkeypatch, OAuthProxy, _mem_recovery_backend())
    self = _make_self(issuer)
    with caplog.at_level(logging.INFO, logger="boomi.refresh_token_recovery"):
        out = _run(OAuthProxy.load_refresh_token(self, CLIENT, "EXP"))
    assert out is None
    assert any("rt_recovery_reject" in m and "jwt_expired" in m for m in _diag(caplog))


def test_load_synthesizes_refresh_token_on_valid_alias(monkeypatch, restore_oauth_proxy, caplog):
    OAuthProxy = restore_oauth_proxy
    issuer = _FakeIssuer()
    issuer.payloads["STALE"] = {"jti": "j", "client_id": "client-A", "token_use": "refresh", "exp": time.time() + 99}
    backend = _mem_recovery_backend()
    exp = int(time.time()) + 1234
    _run(backend.put_alias(_hash_token("STALE"), _alias(successor_expires_at=exp, scopes=("openid", "email")), 60))
    _apply(monkeypatch, OAuthProxy, backend)
    self = _make_self(issuer)
    with caplog.at_level(logging.INFO, logger="boomi.refresh_token_recovery"):
        out = _run(OAuthProxy.load_refresh_token(self, CLIENT, "STALE"))
    assert isinstance(out, RefreshToken)
    assert out.token == "STALE"
    assert out.client_id == "client-A"
    assert list(out.scopes) == ["openid", "email"]
    assert out.expires_at == exp
    assert any("rt_recovery_load_ok" in m for m in _diag(caplog))


def test_load_backend_error_degrades_to_none(monkeypatch, restore_oauth_proxy, caplog):
    OAuthProxy = restore_oauth_proxy
    issuer = _FakeIssuer()
    issuer.payloads["STALE"] = {"jti": "j", "client_id": "client-A", "token_use": "refresh", "exp": time.time() + 99}

    class _BoomBackend:
        async def resolve_latest(self, old_hash, max_hops):
            raise RuntimeError("mongo down")

    _apply(monkeypatch, OAuthProxy, _BoomBackend())
    self = _make_self(issuer)
    with caplog.at_level(logging.WARNING, logger="boomi.refresh_token_recovery"):
        out = _run(OAuthProxy.load_refresh_token(self, CLIENT, "STALE"))
    assert out is None
    assert any("rt_recovery_backend_unavailable" in m for m in _diag(caplog))


# ----------------------------- exchange: normal path -----------------------------

def test_exchange_normal_delegates_and_writes_alias(monkeypatch, restore_oauth_proxy):
    OAuthProxy = restore_oauth_proxy
    issuer = _FakeIssuer()
    issuer.payloads["PRESENTED"] = {"jti": "jti-pres", "client_id": "client-A", "token_use": "refresh", "exp": time.time() + 99}
    issuer.payloads["RT-NEW"] = {"jti": "jti-new", "client_id": "client-A", "token_use": "refresh", "exp": time.time() + 5000}

    calls = {"n": 0}

    async def orig_exchange(self, client, refresh_token, scopes):
        calls["n"] += 1
        return OAuthToken(access_token="AT-x", token_type="Bearer", expires_in=3600,
                          refresh_token="RT-NEW", scope=" ".join(scopes))

    jti = _FakeStore()
    jti.data["jti-pres"] = SimpleNamespace(upstream_token_id="ut-1")
    upstream = _FakeStore()
    upstream.data["ut-1"] = _upstream_set()
    backend = _mem_recovery_backend()
    _apply(monkeypatch, OAuthProxy, backend, orig_exchange=orig_exchange)
    self = _make_self(issuer, jti=jti, upstream=upstream)
    rt = SimpleNamespace(token="PRESENTED")
    result = _run(OAuthProxy.exchange_refresh_token(self, CLIENT, rt, ["openid"]))
    assert calls["n"] == 1
    assert result.refresh_token == "RT-NEW"
    # Alias written: presented-hash -> new successor hash.
    alias = _run(backend.get(_hash_token("PRESENTED")))
    assert alias is not None
    assert alias["successor_rt_hash"] == _hash_token("RT-NEW")
    assert alias["successor_refresh_jti"] == "jti-new"


def test_exchange_normal_preseeds_sliding(monkeypatch, restore_oauth_proxy):
    OAuthProxy = restore_oauth_proxy
    issuer = _FakeIssuer()
    issuer.payloads["PRESENTED"] = {"jti": "jti-pres", "client_id": "client-A", "token_use": "refresh", "exp": time.time() + 99}
    issuer.payloads["RT-NEW"] = {"jti": "jti-new", "client_id": "client-A", "token_use": "refresh", "exp": time.time() + 5000}

    async def orig_exchange(self, client, refresh_token, scopes):
        return OAuthToken(access_token="AT-x", token_type="Bearer", expires_in=3600,
                          refresh_token="RT-NEW", scope=" ".join(scopes))

    jti = _FakeStore()
    jti.data["jti-pres"] = SimpleNamespace(upstream_token_id="ut-1")
    upstream = _FakeStore()
    old_exp = time.time() + 10  # near expiry, frozen
    upstream.data["ut-1"] = _upstream_set(refresh_token_expires_at=old_exp)
    _apply(monkeypatch, OAuthProxy, _mem_recovery_backend(), orig_exchange=orig_exchange,
           env={"BOOMI_RT_SLIDING_REFRESH_TTL_SECONDS": "1000"})
    self = _make_self(issuer, jti=jti, upstream=upstream)
    rt = SimpleNamespace(token="PRESENTED")
    _run(OAuthProxy.exchange_refresh_token(self, CLIENT, rt, ["openid"]))
    # Pre-seed slid the upstream refresh window to ~now+1000 before delegating.
    assert upstream.data["ut-1"].refresh_token_expires_at > time.time() + 900


def test_exchange_sliding_off_does_not_preseed(monkeypatch, restore_oauth_proxy):
    OAuthProxy = restore_oauth_proxy
    issuer = _FakeIssuer()
    issuer.payloads["PRESENTED"] = {"jti": "jti-pres", "client_id": "client-A", "token_use": "refresh", "exp": time.time() + 99}
    issuer.payloads["RT-NEW"] = {"jti": "jti-new", "client_id": "client-A", "token_use": "refresh", "exp": time.time() + 5000}

    async def orig_exchange(self, client, refresh_token, scopes):
        return OAuthToken(access_token="AT-x", token_type="Bearer", expires_in=3600,
                          refresh_token="RT-NEW", scope=" ".join(scopes))

    jti = _FakeStore()
    jti.data["jti-pres"] = SimpleNamespace(upstream_token_id="ut-1")
    upstream = _FakeStore()
    frozen = time.time() + 10
    upstream.data["ut-1"] = _upstream_set(refresh_token_expires_at=frozen)
    # recovery still enabled (so alias write path runs), but sliding OFF.
    _apply(monkeypatch, OAuthProxy, _mem_recovery_backend(), orig_exchange=orig_exchange,
           env={"BOOMI_RT_SLIDING_REFRESH_EXPIRY": "false"})
    self = _make_self(issuer, jti=jti, upstream=upstream)
    rt = SimpleNamespace(token="PRESENTED")
    _run(OAuthProxy.exchange_refresh_token(self, CLIENT, rt, ["openid"]))
    assert upstream.data["ut-1"].refresh_token_expires_at == frozen  # untouched


# ----------------------------- exchange: recovery path -----------------------------

def _setup_recovery(monkeypatch, OAuthProxy, *, response=None, raise_exc=None,
                    successor_jti_live=True, upstream_live=True, env=None):
    issuer = _FakeIssuer()
    issuer.payloads["STALE"] = {"jti": "jti-stale", "client_id": "client-A", "token_use": "refresh", "exp": time.time() + 99}
    backend = _mem_recovery_backend()
    alias = _alias(successor_hash="live-hash", successor_jti="jti-live", upstream_token_id="ut-1")
    _run(backend.put_alias(_hash_token("STALE"), alias, 600))
    jti = _FakeStore()
    if successor_jti_live:
        jti.data["jti-live"] = SimpleNamespace(upstream_token_id="ut-1")
    upstream = _FakeStore()
    if upstream_live:
        upstream.data["ut-1"] = _upstream_set()
    refresh = _FakeStore()
    refresh.data["live-hash"] = SimpleNamespace()
    _set_fake_oauth(monkeypatch, response=response, raise_exc=raise_exc)
    _apply(monkeypatch, OAuthProxy, backend, env=env)
    self = _make_self(issuer, jti=jti, upstream=upstream, refresh=refresh)
    return issuer, backend, jti, upstream, refresh, self


def test_exchange_recovery_mints_fresh_tokens(monkeypatch, restore_oauth_proxy, caplog):
    OAuthProxy = restore_oauth_proxy
    issuer, backend, jti, upstream, refresh, self = _setup_recovery(monkeypatch, OAuthProxy)
    rt = SimpleNamespace(token="STALE")
    with caplog.at_level(logging.INFO, logger="boomi.refresh_token_recovery"):
        result = _run(OAuthProxy.exchange_refresh_token(self, CLIENT, rt, ["openid"]))
    assert isinstance(result, OAuthToken)
    assert result.access_token.startswith("AT-")
    assert result.refresh_token.startswith("RT-")
    assert any("rt_recovery_hit" in m for m in _diag(caplog))


def test_exchange_recovery_consumes_successor_not_stale(monkeypatch, restore_oauth_proxy):
    OAuthProxy = restore_oauth_proxy
    issuer, backend, jti, upstream, refresh, self = _setup_recovery(monkeypatch, OAuthProxy)
    rt = SimpleNamespace(token="STALE")
    _run(OAuthProxy.exchange_refresh_token(self, CLIENT, rt, ["openid"]))
    # The LIVE successor's jti + hash are deleted; the stale token's are not.
    assert "jti-live" in jti.deleted
    assert "live-hash" in refresh.deleted
    assert _hash_token("STALE") not in refresh.deleted


def test_exchange_recovery_writes_compacted_aliases(monkeypatch, restore_oauth_proxy):
    OAuthProxy = restore_oauth_proxy
    issuer, backend, jti, upstream, refresh, self = _setup_recovery(monkeypatch, OAuthProxy)
    rt = SimpleNamespace(token="STALE")
    result = _run(OAuthProxy.exchange_refresh_token(self, CLIENT, rt, ["openid"]))
    new_hash = _hash_token(result.refresh_token)
    # Visited stale hash AND consumed successor hash now point at the new successor.
    for h in (_hash_token("STALE"), "live-hash"):
        alias = _run(backend.get(h))
        assert alias is not None and alias["successor_rt_hash"] == new_hash


def test_exchange_recovery_honors_google_refresh_expires_in(monkeypatch, restore_oauth_proxy):
    OAuthProxy = restore_oauth_proxy
    issuer, backend, jti, upstream, refresh, self = _setup_recovery(
        monkeypatch, OAuthProxy,
        response={"access_token": "up-AT", "expires_in": 3600,
                  "refresh_token": "up-rt-new", "refresh_expires_in": 12345},
    )
    rt = SimpleNamespace(token="STALE")
    _run(OAuthProxy.exchange_refresh_token(self, CLIENT, rt, ["openid"]))
    # The new refresh JWT is issued with the upstream's exact value, not sliding.
    refresh_issues = [e for e in issuer.issued if e[0] == "refresh"]
    assert refresh_issues and refresh_issues[-1][2] == 12345


def test_exchange_recovery_dead_successor_jti_invalid_grant(monkeypatch, restore_oauth_proxy, caplog):
    OAuthProxy = restore_oauth_proxy
    issuer, backend, jti, upstream, refresh, self = _setup_recovery(
        monkeypatch, OAuthProxy, successor_jti_live=False
    )
    rt = SimpleNamespace(token="STALE")
    with caplog.at_level(logging.WARNING, logger="boomi.refresh_token_recovery"):
        with pytest.raises(TokenError):
            _run(OAuthProxy.exchange_refresh_token(self, CLIENT, rt, ["openid"]))
    assert any("rt_recovery_miss" in m and "dead_successor_jti" in m for m in _diag(caplog))


def test_exchange_recovery_dead_upstream_invalid_grant(monkeypatch, restore_oauth_proxy, caplog):
    OAuthProxy = restore_oauth_proxy
    issuer, backend, jti, upstream, refresh, self = _setup_recovery(
        monkeypatch, OAuthProxy, upstream_live=False
    )
    rt = SimpleNamespace(token="STALE")
    with caplog.at_level(logging.WARNING, logger="boomi.refresh_token_recovery"):
        with pytest.raises(TokenError):
            _run(OAuthProxy.exchange_refresh_token(self, CLIENT, rt, ["openid"]))
    assert any("rt_recovery_miss" in m and "dead_upstream" in m for m in _diag(caplog))


def test_exchange_recovery_upstream_refresh_failure_invalid_grant(monkeypatch, restore_oauth_proxy, caplog):
    OAuthProxy = restore_oauth_proxy
    issuer, backend, jti, upstream, refresh, self = _setup_recovery(
        monkeypatch, OAuthProxy, raise_exc=RuntimeError("google said no")
    )
    rt = SimpleNamespace(token="STALE")
    with caplog.at_level(logging.WARNING, logger="boomi.refresh_token_recovery"):
        with pytest.raises(TokenError):
            _run(OAuthProxy.exchange_refresh_token(self, CLIENT, rt, ["openid"]))
    assert any("rt_recovery_miss" in m and "upstream_refresh_failed" in m for m in _diag(caplog))


def test_exchange_both_jti_and_alias_normal_wins(monkeypatch, restore_oauth_proxy):
    OAuthProxy = restore_oauth_proxy
    issuer = _FakeIssuer()
    issuer.payloads["PRESENTED"] = {"jti": "jti-pres", "client_id": "client-A", "token_use": "refresh", "exp": time.time() + 99}
    issuer.payloads["RT-NEW"] = {"jti": "jti-new", "client_id": "client-A", "token_use": "refresh", "exp": time.time() + 5000}

    calls = {"n": 0}

    async def orig_exchange(self, client, refresh_token, scopes):
        calls["n"] += 1
        return OAuthToken(access_token="AT-x", token_type="Bearer", expires_in=3600,
                          refresh_token="RT-NEW", scope=" ".join(scopes))

    jti = _FakeStore()
    jti.data["jti-pres"] = SimpleNamespace(upstream_token_id="ut-1")
    upstream = _FakeStore()
    upstream.data["ut-1"] = _upstream_set()
    backend = _mem_recovery_backend()
    # An alias also exists for the presented token's hash.
    _run(backend.put_alias(_hash_token("PRESENTED"), _alias(), 600))
    _apply(monkeypatch, OAuthProxy, backend, orig_exchange=orig_exchange)
    self = _make_self(issuer, jti=jti, upstream=upstream)
    rt = SimpleNamespace(token="PRESENTED")
    result = _run(OAuthProxy.exchange_refresh_token(self, CLIENT, rt, ["openid"]))
    # Normal path won: orig delegated, recovery did not re-mint.
    assert calls["n"] == 1
    assert result.refresh_token == "RT-NEW"


def test_exchange_detection_failure_degrades_to_orig(monkeypatch, restore_oauth_proxy, caplog):
    OAuthProxy = restore_oauth_proxy
    issuer = _FakeIssuer()
    issuer.payloads["STALE"] = {"jti": "jti-stale", "client_id": "client-A", "token_use": "refresh", "exp": time.time() + 99}

    class _BoomBackend:
        async def resolve_latest(self, old_hash, max_hops):
            raise RuntimeError("ledger down")

    sentinel = {"called": 0}

    async def orig_exchange(self, client, refresh_token, scopes):
        sentinel["called"] += 1
        raise TokenError("invalid_grant", "Refresh token mapping not found")

    jti = _FakeStore()  # no mapping for jti-stale -> recovery attempted
    _apply(monkeypatch, OAuthProxy, _BoomBackend(), orig_exchange=orig_exchange)
    self = _make_self(issuer, jti=jti)
    rt = SimpleNamespace(token="STALE")
    with caplog.at_level(logging.WARNING, logger="boomi.refresh_token_recovery"):
        with pytest.raises(TokenError):
            _run(OAuthProxy.exchange_refresh_token(self, CLIENT, rt, ["openid"]))
    assert sentinel["called"] == 1  # degraded to orig
    assert any("rt_recovery_unavailable" in m for m in _diag(caplog))


def test_exchange_compat_disabled_when_instance_attr_missing(monkeypatch, restore_oauth_proxy, caplog):
    OAuthProxy = restore_oauth_proxy
    issuer = _FakeIssuer()
    sentinel = {"called": 0}

    async def orig_exchange(self, client, refresh_token, scopes):
        sentinel["called"] += 1
        return OAuthToken(access_token="AT-orig", token_type="Bearer", expires_in=3600,
                          refresh_token="RT-orig", scope="openid")

    _apply(monkeypatch, OAuthProxy, _mem_recovery_backend(), orig_exchange=orig_exchange)
    # Self is missing _jti_mapping_store -> instance-incompatible.
    self = _make_self(issuer, omit=("_jti_mapping_store",))
    rt = SimpleNamespace(token="PRESENTED")
    with caplog.at_level(logging.ERROR, logger="boomi.refresh_token_recovery"):
        result = _run(OAuthProxy.exchange_refresh_token(self, CLIENT, rt, ["openid"]))
    assert sentinel["called"] == 1
    assert result.refresh_token == "RT-orig"
    assert any("rt_recovery_compat_disabled" in m for m in _diag(caplog))


# ----------------------------- revoke -----------------------------

def test_revoke_deletes_recovery_record_and_jti(monkeypatch, restore_oauth_proxy):
    OAuthProxy = restore_oauth_proxy
    issuer = _FakeIssuer()
    issuer.payloads["REV"] = {"jti": "jti-rev", "client_id": "client-A", "token_use": "refresh", "exp": time.time() + 99}
    backend = _mem_recovery_backend()
    _run(backend.put_alias(_hash_token("REV"), _alias(), 600))
    revoked = {"called": 0}

    async def orig_revoke(self, token):
        revoked["called"] += 1

    jti = _FakeStore()
    jti.data["jti-rev"] = SimpleNamespace(upstream_token_id="ut-1")
    _apply(monkeypatch, OAuthProxy, backend, orig_revoke=orig_revoke)
    self = _make_self(issuer, jti=jti)
    token = RefreshToken(token="REV", client_id="client-A", scopes=["openid"], expires_at=None)
    _run(OAuthProxy.revoke_token(self, token))
    assert revoked["called"] == 1
    assert _run(backend.get(_hash_token("REV"))) is None  # alias deleted
    assert "jti-rev" in jti.deleted
