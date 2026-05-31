"""Tests for RefreshTokenRecoveryBackend (durable stale-token alias ledger)."""

from __future__ import annotations

import asyncio
import logging
import time

import pytest
from cryptography.fernet import Fernet
from key_value.aio.errors import DecryptionError, DeserializationError
from key_value.aio.stores.memory import MemoryStore
from key_value.aio.wrappers.encryption import FernetEncryptionWrapper

from rt_recovery_backend import (
    DEFAULT_RECOVERY_COLLECTION,
    RefreshTokenRecoveryBackend,
    alias_ttl_seconds,
    initialize_refresh_token_recovery_backend,
)


def _run(coro):
    return asyncio.new_event_loop().run_until_complete(coro)


def _new_key() -> bytes:
    return Fernet.generate_key()


def _mem_backend() -> RefreshTokenRecoveryBackend:
    backing = MemoryStore()
    wrapped = FernetEncryptionWrapper(key_value=backing, fernet=Fernet(_new_key()))
    return RefreshTokenRecoveryBackend(wrapped)


def _record(*, client_id="client-A", successor_hash="next", successor_jti="jti-next",
            upstream_token_id="ut-1", successor_expires_at=None):
    if successor_expires_at is None:
        successor_expires_at = int(time.time()) + 3600
    return {
        "version": 1,
        "client_id": client_id,
        "scopes": ["openid"],
        "successor_rt_hash": successor_hash,
        "successor_refresh_jti": successor_jti,
        "upstream_token_id": upstream_token_id,
        "successor_expires_at": successor_expires_at,
        "created_at": time.time(),
        "updated_at": time.time(),
        "reason": "test",
    }


# ---------- alias_ttl_seconds ----------

def test_alias_ttl_is_min_of_max_age_and_remaining():
    assert alias_ttl_seconds(1000, 604800, now=400) == 600  # remaining wins
    assert alias_ttl_seconds(10_000_000, 604800, now=0) == 604800  # max_age wins


def test_alias_ttl_floored_at_one_second():
    # Successor already expired -> still write a positive (1s) TTL.
    assert alias_ttl_seconds(100, 604800, now=1000) == 1


# ---------- get / put_alias round-trip ----------

def test_put_get_alias_round_trip():
    backend = _mem_backend()
    rec = _record()
    _run(backend.put_alias("oldhash", rec, ttl_seconds=60))
    out = _run(backend.get("oldhash"))
    assert out == rec


def test_ttl_expiry_returns_none():
    backend = _mem_backend()

    async def _scenario():
        await backend.put_alias("k", _record(), ttl_seconds=1)
        first = await backend.get("k")
        await asyncio.sleep(1.3)
        second = await backend.get("k")
        return first, second

    first, second = _run(_scenario())
    assert first is not None
    assert second is None


# ---------- failure swallowing ----------

class _StubStore:
    def __init__(self, *, get_exc=None, put_exc=None, delete_exc=None):
        self._get_exc = get_exc
        self._put_exc = put_exc
        self._delete_exc = delete_exc

    async def get(self, *, key):
        if self._get_exc:
            raise self._get_exc
        return None

    async def put(self, *, key, value, ttl):
        if self._put_exc:
            raise self._put_exc

    async def delete(self, *, key):
        if self._delete_exc:
            raise self._delete_exc


def test_get_swallows_decryption_error(caplog):
    backend = RefreshTokenRecoveryBackend(_StubStore(get_exc=DecryptionError("bad")))
    with caplog.at_level(logging.WARNING, logger="boomi.refresh_token_recovery"):
        out = _run(backend.get("k"))
    assert out is None
    assert any("read swallowed" in r.message for r in caplog.records)


def test_get_swallows_deserialization_error(caplog):
    backend = RefreshTokenRecoveryBackend(_StubStore(get_exc=DeserializationError("x")))
    with caplog.at_level(logging.WARNING, logger="boomi.refresh_token_recovery"):
        out = _run(backend.get("k"))
    assert out is None
    assert any("read swallowed" in r.message for r in caplog.records)


def test_get_swallows_runtime_error(caplog):
    backend = RefreshTokenRecoveryBackend(_StubStore(get_exc=RuntimeError("mongo down")))
    with caplog.at_level(logging.WARNING, logger="boomi.refresh_token_recovery"):
        out = _run(backend.get("k"))
    assert out is None
    assert any("RT_RECOVERY_GET_FAILED" in r.message for r in caplog.records)


def test_get_does_not_swallow_cancelled():
    backend = RefreshTokenRecoveryBackend(_StubStore(get_exc=asyncio.CancelledError()))
    with pytest.raises(asyncio.CancelledError):
        _run(backend.get("k"))


def test_put_alias_never_raises_on_backend_error(caplog):
    backend = RefreshTokenRecoveryBackend(_StubStore(put_exc=RuntimeError("down")))
    with caplog.at_level(logging.WARNING, logger="boomi.refresh_token_recovery"):
        _run(backend.put_alias("k", _record(), ttl_seconds=60))  # must not raise
    assert any("RT_RECOVERY_PUT_FAILED" in r.message for r in caplog.records)


def test_delete_never_raises_on_backend_error(caplog):
    backend = RefreshTokenRecoveryBackend(_StubStore(delete_exc=RuntimeError("down")))
    with caplog.at_level(logging.WARNING, logger="boomi.refresh_token_recovery"):
        _run(backend.delete("k"))  # must not raise
    assert any("RT_RECOVERY_DELETE_FAILED" in r.message for r in caplog.records)


# ---------- resolve_latest ----------

def test_resolve_latest_no_alias_returns_none_empty():
    backend = _mem_backend()
    record, visited = _run(backend.resolve_latest("missing", max_hops=16))
    assert record is None
    assert visited == []


def test_resolve_latest_single_hop_terminal():
    backend = _mem_backend()
    rec = _record(successor_hash="liveB")
    _run(backend.put_alias("A", rec, ttl_seconds=60))
    # "liveB" has no alias -> terminal; rec is the latest, visited == [A].
    record, visited = _run(backend.resolve_latest("A", max_hops=16))
    assert record == rec
    assert visited == ["A"]


def test_resolve_latest_multi_hop_compaction_list():
    backend = _mem_backend()
    recA = _record(successor_hash="B", successor_jti="jti-B")
    recB = _record(successor_hash="C", successor_jti="jti-C")
    _run(backend.put_alias("A", recA, ttl_seconds=60))
    _run(backend.put_alias("B", recB, ttl_seconds=60))
    # C has no alias -> terminal; latest record is recB, visited == [A, B].
    record, visited = _run(backend.resolve_latest("A", max_hops=16))
    assert record == recB
    assert visited == ["A", "B"]


def test_resolve_latest_cycle_detection(caplog):
    backend = _mem_backend()
    _run(backend.put_alias("A", _record(successor_hash="B"), ttl_seconds=60))
    _run(backend.put_alias("B", _record(successor_hash="A"), ttl_seconds=60))
    with caplog.at_level(logging.WARNING, logger="boomi.refresh_token_recovery"):
        record, visited = _run(backend.resolve_latest("A", max_hops=16))
    # Stops on cycle; returns best resolvable so far without infinite loop.
    assert record is not None
    assert any("rt_recovery_chain_cycle" in r.message for r in caplog.records)


def test_resolve_latest_stops_at_max_hops(caplog):
    backend = _mem_backend()
    # Chain longer than max_hops: A->B->C->D... each links to the next.
    for name, nxt in [("A", "B"), ("B", "C"), ("C", "D"), ("D", "E")]:
        _run(backend.put_alias(name, _record(successor_hash=nxt), ttl_seconds=60))
    with caplog.at_level(logging.WARNING, logger="boomi.refresh_token_recovery"):
        record, visited = _run(backend.resolve_latest("A", max_hops=2))
    assert record is not None  # best resolvable so far
    assert len(visited) <= 3
    assert any("rt_recovery_max_hops" in r.message for r in caplog.records)


def test_resolve_latest_rejects_client_drift(caplog):
    backend = _mem_backend()
    _run(backend.put_alias("A", _record(client_id="client-A", successor_hash="B"), ttl_seconds=60))
    _run(backend.put_alias("B", _record(client_id="client-OTHER", successor_hash="C"), ttl_seconds=60))
    with caplog.at_level(logging.WARNING, logger="boomi.refresh_token_recovery"):
        record, visited = _run(backend.resolve_latest("A", max_hops=16))
    # Stops at the drift boundary; only the consistent prefix is used.
    assert visited == ["A"]
    assert any("rt_recovery_client_drift" in r.message for r in caplog.records)


def test_resolve_latest_stops_on_expired_successor(caplog):
    backend = _mem_backend()
    expired = _record(successor_expires_at=int(time.time()) - 100)
    _run(backend.put_alias("A", expired, ttl_seconds=60))
    with caplog.at_level(logging.WARNING, logger="boomi.refresh_token_recovery"):
        record, visited = _run(backend.resolve_latest("A", max_hops=16))
    assert record is None  # first record already expired -> nothing live
    assert visited == []
    assert any("rt_recovery_chain_expired" in r.message for r in caplog.records)


# ---------- initialize_refresh_token_recovery_backend ----------

def test_initialize_disabled_returns_none(monkeypatch):
    monkeypatch.setenv("BOOMI_RT_RECOVERY_ENABLED", "false")
    assert initialize_refresh_token_recovery_backend("mongodb://x", Fernet(_new_key())) is None


def test_initialize_missing_uri_raises(monkeypatch):
    monkeypatch.setenv("BOOMI_RT_RECOVERY_ENABLED", "true")
    with pytest.raises(ValueError, match="mongodb_uri"):
        initialize_refresh_token_recovery_backend("", Fernet(_new_key()))


def test_initialize_missing_fernet_raises(monkeypatch):
    monkeypatch.setenv("BOOMI_RT_RECOVERY_ENABLED", "true")
    with pytest.raises(ValueError, match="Fernet"):
        initialize_refresh_token_recovery_backend("mongodb://x", None)


def test_initialize_routes_to_recovery_collection(monkeypatch):
    monkeypatch.setenv("BOOMI_RT_RECOVERY_ENABLED", "true")
    monkeypatch.delenv("BOOMI_RT_RECOVERY_COLLECTION", raising=False)

    recorded: dict = {}

    class _RecordingStore(MemoryStore):
        def __init__(self, **kwargs):
            recorded.update(kwargs)
            super().__init__()

    monkeypatch.setattr("key_value.aio.stores.mongodb.MongoDBStore", _RecordingStore)
    backend = initialize_refresh_token_recovery_backend("mongodb://x", Fernet(_new_key()))
    assert backend is not None
    assert recorded.get("default_collection") == DEFAULT_RECOVERY_COLLECTION


# ---------- strict-startup probe ----------

def test_probe_succeeds_against_live_store():
    """A reachable, writable store: probe round-trips and cleans up its sentinel."""
    backend = _mem_backend()
    assert _run(backend.probe()) is None
    # The probe must not leave its sentinel behind.
    assert _run(backend.get("__rt_recovery_startup_probe__")) is None


def test_probe_propagates_write_failure():
    """Read-only/unwritable store: the probe must NOT swallow -- it raises for
    fail-fast, even though reads alone would succeed (the write path is what
    durable recovery actually depends on)."""
    class _WriteDeniedStore:
        async def put(self, *, key, value, ttl):
            raise RuntimeError("write denied")

        async def get(self, *, key):
            return None

        async def delete(self, *, key):
            pass

    backend = RefreshTokenRecoveryBackend(_WriteDeniedStore())
    with pytest.raises(RuntimeError, match="write denied"):
        _run(backend.probe())
