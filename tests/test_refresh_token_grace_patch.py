"""Tests for the refresh-token rotation grace window patch (Fix A)."""

from __future__ import annotations

import asyncio
import importlib
import sys
import time
from types import SimpleNamespace

import pytest


# ---------- _TTLCache primitive ----------

def _fresh_module():
    """Drop any cached import and return a fresh refresh_token_grace_patch."""
    sys.modules.pop("refresh_token_grace_patch", None)
    return importlib.import_module("refresh_token_grace_patch")


def _run(coro):
    return asyncio.new_event_loop().run_until_complete(coro)


def test_ttlcache_hit_and_miss():
    mod = _fresh_module()
    cache = mod._TTLCache(max_size=4)
    _run(cache.set("k1", "v1", time.time() + 60))
    assert _run(cache.get("k1")) == "v1"
    assert _run(cache.get("missing")) is None


def test_ttlcache_expiry_evicts():
    mod = _fresh_module()
    cache = mod._TTLCache(max_size=4)
    _run(cache.set("k1", "v1", time.time() - 1))  # already past expiry
    assert _run(cache.get("k1")) is None
    # And expired entries are purged on access so the dict shrinks
    assert "k1" not in cache._data


def test_ttlcache_lru_evicts_at_capacity():
    mod = _fresh_module()
    cache = mod._TTLCache(max_size=2)
    far_future = time.time() + 60
    _run(cache.set("k1", "v1", far_future))
    _run(cache.set("k2", "v2", far_future))
    # touch k1 so it moves to MRU
    assert _run(cache.get("k1")) == "v1"
    _run(cache.set("k3", "v3", far_future))
    # k2 is LRU and must be evicted, k1 stays
    assert _run(cache.get("k2")) is None
    assert _run(cache.get("k1")) == "v1"
    assert _run(cache.get("k3")) == "v3"


# ---------- apply_refresh_token_grace_patch() ----------

@pytest.fixture
def restore_oauth_proxy():
    """Snapshot OAuthProxy methods, yield, then restore. Always runs."""
    from fastmcp.server.auth.oauth_proxy.proxy import OAuthProxy
    saved_load = OAuthProxy.load_refresh_token
    saved_exchange = OAuthProxy.exchange_refresh_token
    yield OAuthProxy
    OAuthProxy.load_refresh_token = saved_load
    OAuthProxy.exchange_refresh_token = saved_exchange


def test_disabled_when_grace_seconds_zero(monkeypatch, restore_oauth_proxy):
    """BOOMI_RT_GRACE_SECONDS=0 short-circuits without monkey-patching."""
    monkeypatch.setenv("BOOMI_RT_GRACE_SECONDS", "0")
    OAuthProxy = restore_oauth_proxy
    pre_load = OAuthProxy.load_refresh_token
    pre_exchange = OAuthProxy.exchange_refresh_token

    mod = _fresh_module()
    mod.apply_refresh_token_grace_patch()

    assert OAuthProxy.load_refresh_token is pre_load
    assert OAuthProxy.exchange_refresh_token is pre_exchange


def test_exchange_caches_and_short_circuits_on_replay(monkeypatch, restore_oauth_proxy):
    """Same RT presented twice within the window returns the cached OAuthToken."""
    monkeypatch.setenv("BOOMI_RT_GRACE_SECONDS", "60")
    OAuthProxy = restore_oauth_proxy

    call_count = {"n": 0}

    from mcp.shared.auth import OAuthToken

    async def fake_exchange(self, client, refresh_token, scopes):
        call_count["n"] += 1
        return OAuthToken(
            access_token=f"AT-{call_count['n']}",
            token_type="Bearer",
            expires_in=3600,
            refresh_token=f"RT-{call_count['n']}",
            scope=" ".join(scopes),
        )

    OAuthProxy.exchange_refresh_token = fake_exchange

    mod = _fresh_module()
    mod.apply_refresh_token_grace_patch()

    proxy = SimpleNamespace()  # `self` placeholder
    client = SimpleNamespace(client_id="client-abc")
    rt = SimpleNamespace(token="rt-original")
    scopes = ["openid"]

    first = _run(OAuthProxy.exchange_refresh_token(proxy, client, rt, scopes))
    second = _run(OAuthProxy.exchange_refresh_token(proxy, client, rt, scopes))

    assert call_count["n"] == 1, "underlying exchange must be called only once"
    assert first.access_token == "AT-1"
    assert second.access_token == "AT-1", "grace-cache hit must return the same OAuthToken"
    assert second.refresh_token == "RT-1"


def test_exchange_grace_window_expires(monkeypatch, restore_oauth_proxy):
    """After the grace window passes, the underlying exchange runs again."""
    monkeypatch.setenv("BOOMI_RT_GRACE_SECONDS", "1")
    OAuthProxy = restore_oauth_proxy

    call_count = {"n": 0}
    from mcp.shared.auth import OAuthToken

    async def fake_exchange(self, client, refresh_token, scopes):
        call_count["n"] += 1
        return OAuthToken(access_token=f"AT-{call_count['n']}", token_type="Bearer", expires_in=3600)

    OAuthProxy.exchange_refresh_token = fake_exchange

    mod = _fresh_module()
    mod.apply_refresh_token_grace_patch()

    proxy = SimpleNamespace()
    client = SimpleNamespace(client_id="client-abc")
    rt = SimpleNamespace(token="rt-original")

    async def _scenario():
        await OAuthProxy.exchange_refresh_token(proxy, client, rt, ["s"])  # n=1
        await OAuthProxy.exchange_refresh_token(proxy, client, rt, ["s"])  # cache hit, n=1
        await asyncio.sleep(1.2)
        await OAuthProxy.exchange_refresh_token(proxy, client, rt, ["s"])  # n=2

    _run(_scenario())
    assert call_count["n"] == 2, "expired entry must trigger a fresh underlying call"


def test_load_refresh_token_synthesizes_from_grace_cache(monkeypatch, restore_oauth_proxy):
    """When original load returns None, patched load synthesizes from cache."""
    monkeypatch.setenv("BOOMI_RT_GRACE_SECONDS", "60")
    OAuthProxy = restore_oauth_proxy

    from mcp.shared.auth import OAuthToken

    async def fake_exchange(self, client, refresh_token, scopes):
        return OAuthToken(access_token="AT", token_type="Bearer", expires_in=3600)

    async def fake_load_returning_none(self, client, refresh_token):
        return None

    OAuthProxy.exchange_refresh_token = fake_exchange
    OAuthProxy.load_refresh_token = fake_load_returning_none

    mod = _fresh_module()
    mod.apply_refresh_token_grace_patch()

    proxy = SimpleNamespace()
    client = SimpleNamespace(client_id="client-abc")
    rt = SimpleNamespace(token="rt-original")

    # First exchange populates the grace cache
    _run(OAuthProxy.exchange_refresh_token(proxy, client, rt, ["openid"]))

    # Now ask load_refresh_token for the same RT string -> should be synthesized
    synthesized = _run(OAuthProxy.load_refresh_token(proxy, client, "rt-original"))
    assert synthesized is not None
    assert synthesized.client_id == "client-abc"
    assert "openid" in synthesized.scopes
    # And a totally-unknown RT still misses
    miss = _run(OAuthProxy.load_refresh_token(proxy, client, "rt-never-issued"))
    assert miss is None


def test_load_refresh_token_passes_through_when_original_returns(monkeypatch, restore_oauth_proxy):
    """If original load returns a RefreshToken, patched load returns it unchanged."""
    monkeypatch.setenv("BOOMI_RT_GRACE_SECONDS", "60")
    OAuthProxy = restore_oauth_proxy

    from mcp.server.auth.provider import RefreshToken

    sentinel = RefreshToken(
        token="rt-x", client_id="client-x", scopes=["s"], expires_at=int(time.time() + 600)
    )

    async def fake_load(self, client, refresh_token):
        return sentinel

    OAuthProxy.load_refresh_token = fake_load

    mod = _fresh_module()
    mod.apply_refresh_token_grace_patch()

    proxy = SimpleNamespace()
    client = SimpleNamespace(client_id="client-x")
    out = _run(OAuthProxy.load_refresh_token(proxy, client, "rt-x"))
    assert out is sentinel


def test_singleflight_coalesces_concurrent_exchanges(monkeypatch, restore_oauth_proxy):
    """Two concurrent exchanges on the same RT must hit orig exactly once.

    Reproduces the parallel-tab race that Codex flagged: without
    singleflight, both coroutines observe a cache MISS and both enter
    orig_exchange. The first call mutates the upstream FastMCP state
    (deletes _refresh_token_store row + JTI mapping), and the second
    would fail with `invalid_grant`. Singleflight ensures the second
    awaits the first's Future and returns the same OAuthToken.
    """
    monkeypatch.setenv("BOOMI_RT_GRACE_SECONDS", "60")
    OAuthProxy = restore_oauth_proxy

    call_count = {"n": 0}
    from mcp.shared.auth import OAuthToken

    async def slow_exchange(self, client, refresh_token, scopes):
        # Sleep long enough that a second coroutine can definitely enter
        # patched_exchange while we're still in flight here.
        call_count["n"] += 1
        my_n = call_count["n"]
        await asyncio.sleep(0.2)
        return OAuthToken(
            access_token=f"AT-{my_n}",
            token_type="Bearer",
            expires_in=3600,
            refresh_token=f"RT-{my_n}",
            scope=" ".join(scopes),
        )

    OAuthProxy.exchange_refresh_token = slow_exchange

    mod = _fresh_module()
    mod.apply_refresh_token_grace_patch()

    proxy = SimpleNamespace()
    client = SimpleNamespace(client_id="client-abc")
    rt = SimpleNamespace(token="rt-shared")

    async def _race():
        return await asyncio.gather(
            OAuthProxy.exchange_refresh_token(proxy, client, rt, ["openid"]),
            OAuthProxy.exchange_refresh_token(proxy, client, rt, ["openid"]),
            OAuthProxy.exchange_refresh_token(proxy, client, rt, ["openid"]),
        )

    a, b, c = _run(_race())

    assert call_count["n"] == 1, "underlying exchange must run exactly once under concurrency"
    assert a.access_token == b.access_token == c.access_token == "AT-1"
    assert a.refresh_token == b.refresh_token == c.refresh_token == "RT-1"


def test_singleflight_propagates_leader_exception(monkeypatch, restore_oauth_proxy):
    """If the leader's orig_exchange raises, followers must see the same error."""
    monkeypatch.setenv("BOOMI_RT_GRACE_SECONDS", "60")
    OAuthProxy = restore_oauth_proxy

    call_count = {"n": 0}

    async def slow_failing_exchange(self, client, refresh_token, scopes):
        call_count["n"] += 1
        await asyncio.sleep(0.1)
        raise RuntimeError("upstream blew up")

    OAuthProxy.exchange_refresh_token = slow_failing_exchange

    mod = _fresh_module()
    mod.apply_refresh_token_grace_patch()

    proxy = SimpleNamespace()
    client = SimpleNamespace(client_id="client-abc")
    rt = SimpleNamespace(token="rt-shared")

    async def _race():
        return await asyncio.gather(
            OAuthProxy.exchange_refresh_token(proxy, client, rt, ["openid"]),
            OAuthProxy.exchange_refresh_token(proxy, client, rt, ["openid"]),
            return_exceptions=True,
        )

    a, b = _run(_race())

    assert call_count["n"] == 1, "underlying exchange must run only once even on failure"
    assert isinstance(a, RuntimeError) and "upstream blew up" in str(a)
    assert isinstance(b, RuntimeError) and "upstream blew up" in str(b)


def test_singleflight_releases_slot_after_completion(monkeypatch, restore_oauth_proxy):
    """After leader finishes, a fresh request (cache evicted) must run orig again."""
    monkeypatch.setenv("BOOMI_RT_GRACE_SECONDS", "1")  # short window
    OAuthProxy = restore_oauth_proxy

    call_count = {"n": 0}
    from mcp.shared.auth import OAuthToken

    async def quick_exchange(self, client, refresh_token, scopes):
        call_count["n"] += 1
        return OAuthToken(access_token=f"AT-{call_count['n']}", token_type="Bearer", expires_in=3600)

    OAuthProxy.exchange_refresh_token = quick_exchange

    mod = _fresh_module()
    mod.apply_refresh_token_grace_patch()

    proxy = SimpleNamespace()
    client = SimpleNamespace(client_id="client-abc")
    rt = SimpleNamespace(token="rt-shared")

    async def _scenario():
        await OAuthProxy.exchange_refresh_token(proxy, client, rt, ["s"])  # n=1, populates cache
        await asyncio.sleep(1.2)  # cache expires
        await OAuthProxy.exchange_refresh_token(proxy, client, rt, ["s"])  # n=2, slot must be free

    _run(_scenario())
    assert call_count["n"] == 2


class _FakeSharedBackend:
    """In-memory stand-in for SharedGraceBackend used in unit tests.

    Supports both the basic L2 cache surface (get/put/delete) and the
    Fix D.2 lock primitives. `supports_locks` toggles whether the patch
    enters the distributed-singleflight branch.
    """

    def __init__(self, *, supports_locks: bool = False):
        self.store: dict[str, dict] = {}
        self.get_exc: Exception | None = None
        self.put_exc: Exception | None = None
        self.get_calls = 0
        self.put_calls = 0
        self.supports_locks = supports_locks
        self.lock_holder: dict[str, str] = {}
        self.claim_calls = 0
        self.release_calls = 0
        self.failure_marker_calls = 0

    async def get(self, key):
        self.get_calls += 1
        if self.get_exc is not None:
            raise self.get_exc
        return self.store.get(key)

    async def put(self, key, value, ttl_seconds):
        self.put_calls += 1
        if self.put_exc is not None:
            return  # real backend swallows; mirror that here
        self.store[key] = value

    async def delete(self, key):
        self.store.pop(key, None)

    async def try_claim_lock(self, key, ttl_seconds, instance):
        self.claim_calls += 1
        if key in self.lock_holder:
            return False
        self.lock_holder[key] = instance
        return True

    async def release_lock(self, key):
        self.release_calls += 1
        self.lock_holder.pop(key, None)

    async def write_failure_marker(self, key, error_type, short_ttl=5):
        self.failure_marker_calls += 1
        self.store[key] = {"error": error_type}


def test_shared_backend_leader_writes_through(monkeypatch, restore_oauth_proxy):
    monkeypatch.setenv("BOOMI_RT_GRACE_SECONDS", "60")
    OAuthProxy = restore_oauth_proxy
    from mcp.shared.auth import OAuthToken

    async def fake_exchange(self, client, refresh_token, scopes):
        return OAuthToken(
            access_token="AT-leader",
            token_type="Bearer",
            expires_in=3600,
            refresh_token="RT-leader",
            scope=" ".join(scopes),
        )

    OAuthProxy.exchange_refresh_token = fake_exchange

    shared = _FakeSharedBackend()
    mod = _fresh_module()
    mod.apply_refresh_token_grace_patch(shared_backend=shared)

    proxy = SimpleNamespace()
    client = SimpleNamespace(client_id="client-abc")
    rt = SimpleNamespace(token="rt-shared")
    _run(OAuthProxy.exchange_refresh_token(proxy, client, rt, ["openid"]))

    from fastmcp.server.auth.oauth_proxy.models import _hash_token
    h = _hash_token("rt-shared")
    assert h in shared.store
    payload = shared.store[h]
    assert payload["access_token"] == "AT-leader"
    assert payload["refresh_token"] == "RT-leader"
    assert payload["client_id"] == "client-abc"
    assert payload["scopes"] == ["openid"]


def test_shared_backend_visible_to_second_instance(monkeypatch, restore_oauth_proxy):
    """Simulates two Cloud Run replicas sharing one Mongo-backed grace.

    Verifies the cross-replica visibility property: a rotation on
    instance A is observed by instance B's load AND exchange without
    re-running orig_exchange. This is the property that closes the
    multi-instance gap left over from PR #33.
    """
    monkeypatch.setenv("BOOMI_RT_GRACE_SECONDS", "60")
    OAuthProxy = restore_oauth_proxy
    from mcp.shared.auth import OAuthToken

    exchange_calls = {"n": 0}

    async def fake_exchange(self, client, refresh_token, scopes):
        exchange_calls["n"] += 1
        return OAuthToken(
            access_token=f"AT-{exchange_calls['n']}",
            token_type="Bearer",
            expires_in=3600,
            refresh_token=f"RT-{exchange_calls['n']}",
            scope=" ".join(scopes),
        )

    async def fake_load_returning_none(self, client, refresh_token):
        return None

    OAuthProxy.exchange_refresh_token = fake_exchange
    OAuthProxy.load_refresh_token = fake_load_returning_none

    shared = _FakeSharedBackend()

    # "Instance 1": apply with the shared backend.
    mod_a = _fresh_module()
    mod_a.apply_refresh_token_grace_patch(shared_backend=shared)

    proxy = SimpleNamespace()
    client = SimpleNamespace(client_id="client-abc")
    rt = SimpleNamespace(token="rt-shared")
    first_result = _run(OAuthProxy.exchange_refresh_token(proxy, client, rt, ["openid"]))

    # "Instance 2": re-apply with a fresh module load (= fresh local
    # _TTLCache and inflight dict). Same shared backend.
    mod_b = _fresh_module()
    mod_b.apply_refresh_token_grace_patch(shared_backend=shared)

    rt_from_b = _run(OAuthProxy.load_refresh_token(proxy, client, "rt-shared"))
    assert rt_from_b is not None
    assert rt_from_b.client_id == "client-abc"

    second_result = _run(OAuthProxy.exchange_refresh_token(proxy, client, rt, ["openid"]))
    assert exchange_calls["n"] == 1, "second instance must NOT re-call orig_exchange"
    assert second_result.access_token == first_result.access_token
    assert second_result.refresh_token == first_result.refresh_token


def test_shared_backend_put_failure_does_not_break_local_exchange(monkeypatch, restore_oauth_proxy):
    monkeypatch.setenv("BOOMI_RT_GRACE_SECONDS", "60")
    OAuthProxy = restore_oauth_proxy
    from mcp.shared.auth import OAuthToken

    async def fake_exchange(self, client, refresh_token, scopes):
        return OAuthToken(access_token="AT", token_type="Bearer", expires_in=3600)

    OAuthProxy.exchange_refresh_token = fake_exchange

    shared = _FakeSharedBackend()
    shared.put_exc = RuntimeError("simulated mongo outage")
    mod = _fresh_module()
    mod.apply_refresh_token_grace_patch(shared_backend=shared)

    proxy = SimpleNamespace()
    client = SimpleNamespace(client_id="client-abc")
    rt = SimpleNamespace(token="rt-shared")
    result = _run(OAuthProxy.exchange_refresh_token(proxy, client, rt, ["openid"]))
    # Local caller still gets the OAuthToken even though shared put "failed".
    assert result.access_token == "AT"


def test_shared_backend_get_failure_does_not_break_local_exchange(monkeypatch, restore_oauth_proxy):
    """A shared-cache read outage must degrade to the normal local exchange."""
    monkeypatch.setenv("BOOMI_RT_GRACE_SECONDS", "60")
    OAuthProxy = restore_oauth_proxy
    from mcp.shared.auth import OAuthToken

    call_count = {"n": 0}

    async def fake_exchange(self, client, refresh_token, scopes):
        call_count["n"] += 1
        return OAuthToken(access_token="AT-local", token_type="Bearer", expires_in=3600)

    OAuthProxy.exchange_refresh_token = fake_exchange

    shared = _FakeSharedBackend()
    shared.get_exc = RuntimeError("simulated mongo read outage")
    mod = _fresh_module()
    mod.apply_refresh_token_grace_patch(shared_backend=shared)

    proxy = SimpleNamespace()
    client = SimpleNamespace(client_id="client-abc")
    rt = SimpleNamespace(token="rt-shared")
    result = _run(OAuthProxy.exchange_refresh_token(proxy, client, rt, ["openid"]))

    assert call_count["n"] == 1
    assert result.access_token == "AT-local"


def test_grace_cache_rejects_cross_client_replay(monkeypatch, restore_oauth_proxy):
    """A cached entry must only be honored for the original client_id."""
    monkeypatch.setenv("BOOMI_RT_GRACE_SECONDS", "60")
    OAuthProxy = restore_oauth_proxy

    from mcp.shared.auth import OAuthToken

    async def fake_exchange(self, client, refresh_token, scopes):
        return OAuthToken(access_token="AT", token_type="Bearer", expires_in=3600)

    async def fake_load_returning_none(self, client, refresh_token):
        return None

    OAuthProxy.exchange_refresh_token = fake_exchange
    OAuthProxy.load_refresh_token = fake_load_returning_none

    mod = _fresh_module()
    mod.apply_refresh_token_grace_patch()

    proxy = SimpleNamespace()
    client_a = SimpleNamespace(client_id="client-A")
    client_b = SimpleNamespace(client_id="client-B")
    rt = SimpleNamespace(token="rt-shared")

    # Client A exchanges; cache holds (OAuthToken, "client-A", ["openid"])
    _run(OAuthProxy.exchange_refresh_token(proxy, client_a, rt, ["openid"]))

    # Client B presents the same RT string -> grace cache must NOT synthesize
    out = _run(OAuthProxy.load_refresh_token(proxy, client_b, "rt-shared"))
    assert out is None


# ---------- Fix D.2: cross-instance distributed singleflight ----------

def test_d2_distributed_follower_returns_remote_leader_result(monkeypatch, restore_oauth_proxy):
    """When this replica loses the distributed-lock race, it polls the
    shared cache for the leader's result and returns it without calling
    orig_exchange."""
    monkeypatch.setenv("BOOMI_RT_GRACE_SECONDS", "60")
    monkeypatch.setenv("BOOMI_RT_GRACE_LOCK_POLL_MS", "20")
    OAuthProxy = restore_oauth_proxy
    from fastmcp.server.auth.oauth_proxy.models import _hash_token

    orig_called = {"n": 0}

    async def never_call(self, *args, **kwargs):
        orig_called["n"] += 1
        raise AssertionError("orig must not be called when remote leader produces result")

    OAuthProxy.exchange_refresh_token = never_call

    shared = _FakeSharedBackend(supports_locks=True)
    # Simulate the remote leader already holding the lock.
    shared.lock_holder[_hash_token("rt-shared")] = "remote-replica"

    mod = _fresh_module()
    mod.apply_refresh_token_grace_patch(shared_backend=shared)

    proxy = SimpleNamespace()
    client = SimpleNamespace(client_id="client-abc")
    rt = SimpleNamespace(token="rt-shared")

    async def _scenario():
        # Schedule the "remote leader" to write the result after 50ms.
        async def remote_leader_writes():
            await asyncio.sleep(0.05)
            shared.store[_hash_token("rt-shared")] = {
                "access_token": "AT-from-remote",
                "token_type": "Bearer",
                "expires_in": 3600,
                "refresh_token": "RT-from-remote",
                "scope": "openid",
                "client_id": "client-abc",
                "scopes": ["openid"],
            }
        asyncio.create_task(remote_leader_writes())
        return await OAuthProxy.exchange_refresh_token(proxy, client, rt, ["openid"])

    result = _run(_scenario())
    assert orig_called["n"] == 0
    assert result.access_token == "AT-from-remote"
    assert result.refresh_token == "RT-from-remote"


def test_d2_follower_sees_failure_marker_and_raises_token_error(monkeypatch, restore_oauth_proxy):
    monkeypatch.setenv("BOOMI_RT_GRACE_SECONDS", "60")
    monkeypatch.setenv("BOOMI_RT_GRACE_LOCK_POLL_MS", "20")
    OAuthProxy = restore_oauth_proxy
    from fastmcp.server.auth.oauth_proxy.models import _hash_token
    from mcp.server.auth.provider import TokenError

    async def never_call(self, *args, **kwargs):
        raise AssertionError("orig must not run on follower path")

    OAuthProxy.exchange_refresh_token = never_call

    shared = _FakeSharedBackend(supports_locks=True)
    h = _hash_token("rt-shared")
    shared.lock_holder[h] = "remote-replica"

    mod = _fresh_module()
    mod.apply_refresh_token_grace_patch(shared_backend=shared)

    proxy = SimpleNamespace()
    client = SimpleNamespace(client_id="client-abc")
    rt = SimpleNamespace(token="rt-shared")

    async def _scenario():
        async def remote_leader_fails():
            await asyncio.sleep(0.05)
            shared.store[h] = {"error": "RuntimeError"}
        asyncio.create_task(remote_leader_fails())
        return await OAuthProxy.exchange_refresh_token(proxy, client, rt, ["openid"])

    with pytest.raises(TokenError) as excinfo:
        _run(_scenario())
    assert "Refresh rotation failed on peer instance" in str(excinfo.value.error_description or "")


def test_d2_leader_holds_lock_releases_in_finally(monkeypatch, restore_oauth_proxy):
    """The leader claims the distributed lock and releases it whether the
    underlying exchange succeeds or fails."""
    monkeypatch.setenv("BOOMI_RT_GRACE_SECONDS", "60")
    OAuthProxy = restore_oauth_proxy
    from mcp.shared.auth import OAuthToken
    from fastmcp.server.auth.oauth_proxy.models import _hash_token

    async def fake_exchange(self, client, refresh_token, scopes):
        return OAuthToken(access_token="AT", token_type="Bearer", expires_in=3600)

    OAuthProxy.exchange_refresh_token = fake_exchange

    shared = _FakeSharedBackend(supports_locks=True)
    mod = _fresh_module()
    mod.apply_refresh_token_grace_patch(shared_backend=shared)

    proxy = SimpleNamespace()
    client = SimpleNamespace(client_id="client-abc")
    rt = SimpleNamespace(token="rt-shared")
    _run(OAuthProxy.exchange_refresh_token(proxy, client, rt, ["openid"]))

    h = _hash_token("rt-shared")
    assert shared.claim_calls == 1
    assert shared.release_calls == 1
    assert h not in shared.lock_holder


def test_d2_leader_failure_writes_marker_and_releases_lock(monkeypatch, restore_oauth_proxy):
    monkeypatch.setenv("BOOMI_RT_GRACE_SECONDS", "60")
    OAuthProxy = restore_oauth_proxy
    from fastmcp.server.auth.oauth_proxy.models import _hash_token

    async def fake_exchange_fails(self, client, refresh_token, scopes):
        raise RuntimeError("upstream blew up")

    OAuthProxy.exchange_refresh_token = fake_exchange_fails

    shared = _FakeSharedBackend(supports_locks=True)
    mod = _fresh_module()
    mod.apply_refresh_token_grace_patch(shared_backend=shared)

    proxy = SimpleNamespace()
    client = SimpleNamespace(client_id="client-abc")
    rt = SimpleNamespace(token="rt-shared")
    with pytest.raises(RuntimeError, match="upstream blew up"):
        _run(OAuthProxy.exchange_refresh_token(proxy, client, rt, ["openid"]))

    h = _hash_token("rt-shared")
    assert shared.failure_marker_calls == 1
    assert shared.store[h] == {"error": "RuntimeError"}
    assert shared.release_calls == 1
    assert h not in shared.lock_holder


def test_d2_disabled_when_backend_does_not_support_locks(monkeypatch, restore_oauth_proxy):
    """`supports_locks=False` keeps the patch on the Phase-3 code path."""
    monkeypatch.setenv("BOOMI_RT_GRACE_SECONDS", "60")
    OAuthProxy = restore_oauth_proxy
    from mcp.shared.auth import OAuthToken

    async def fake_exchange(self, client, refresh_token, scopes):
        return OAuthToken(access_token="AT", token_type="Bearer", expires_in=3600)

    OAuthProxy.exchange_refresh_token = fake_exchange

    shared = _FakeSharedBackend(supports_locks=False)  # explicit
    mod = _fresh_module()
    mod.apply_refresh_token_grace_patch(shared_backend=shared)

    proxy = SimpleNamespace()
    client = SimpleNamespace(client_id="client-abc")
    rt = SimpleNamespace(token="rt-shared")
    _run(OAuthProxy.exchange_refresh_token(proxy, client, rt, ["openid"]))

    # Locks were NOT consulted.
    assert shared.claim_calls == 0
    assert shared.release_calls == 0
