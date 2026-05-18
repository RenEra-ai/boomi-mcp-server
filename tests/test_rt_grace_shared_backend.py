"""Tests for SharedGraceBackend (Fix D, shared mongo+fernet layer)."""

from __future__ import annotations

import asyncio
import logging

import pytest
from cryptography.fernet import Fernet, MultiFernet
from key_value.aio.errors import DecryptionError, DeserializationError
from key_value.aio.stores.memory import MemoryStore
from key_value.aio.wrappers.encryption import FernetEncryptionWrapper

from rt_grace_shared_backend import SharedGraceBackend, initialize_shared_grace_backend


def _run(coro):
    return asyncio.new_event_loop().run_until_complete(coro)


def _new_key() -> bytes:
    return Fernet.generate_key()


# ---------- core SharedGraceBackend behavior ----------

def test_round_trip_via_in_memory_fernet_store():
    backing = MemoryStore()
    wrapped = FernetEncryptionWrapper(key_value=backing, fernet=Fernet(_new_key()))
    backend = SharedGraceBackend(wrapped)

    payload = {"access_token": "AT-1", "refresh_token": "RT-1", "scope": "openid"}
    _run(backend.put("k1", payload, ttl_seconds=60))
    out = _run(backend.get("k1"))
    assert out == payload


def test_ttl_evicts_after_expiry():
    backing = MemoryStore()
    wrapped = FernetEncryptionWrapper(key_value=backing, fernet=Fernet(_new_key()))
    backend = SharedGraceBackend(wrapped)

    async def _scenario():
        await backend.put("k1", {"x": 1}, ttl_seconds=1)
        first = await backend.get("k1")
        await asyncio.sleep(1.3)
        second = await backend.get("k1")
        return first, second

    first, second = _run(_scenario())
    assert first == {"x": 1}
    assert second is None


def test_two_backends_sharing_one_store_see_each_other_writes():
    backing = MemoryStore()
    key = _new_key()
    backend_a = SharedGraceBackend(FernetEncryptionWrapper(key_value=backing, fernet=Fernet(key)))
    backend_b = SharedGraceBackend(FernetEncryptionWrapper(key_value=backing, fernet=Fernet(key)))

    _run(backend_a.put("shared", {"from": "A"}, ttl_seconds=60))
    assert _run(backend_b.get("shared")) == {"from": "A"}


def test_multifernet_can_read_old_key_ciphertext():
    """A value written under OLD-key wrapper is readable through MultiFernet wrapper."""
    backing = MemoryStore()
    old_key = _new_key()
    new_key = _new_key()

    old_backend = SharedGraceBackend(
        FernetEncryptionWrapper(key_value=backing, fernet=Fernet(old_key))
    )
    _run(old_backend.put("rotating", {"v": "old-era"}, ttl_seconds=60))

    mf = MultiFernet([Fernet(new_key), Fernet(old_key)])
    mf_backend = SharedGraceBackend(
        FernetEncryptionWrapper(key_value=backing, fernet=mf)
    )
    assert _run(mf_backend.get("rotating")) == {"v": "old-era"}


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


def test_get_swallows_decryption_error_returns_none(caplog):
    backend = SharedGraceBackend(_StubStore(get_exc=DecryptionError("bad ciphertext")))
    with caplog.at_level(logging.WARNING, logger="boomi.rt_grace_shared"):
        out = _run(backend.get("k1"))
    assert out is None
    assert any("read swallowed" in rec.message for rec in caplog.records)


def test_get_swallows_deserialization_error_returns_none(caplog):
    backend = SharedGraceBackend(_StubStore(get_exc=DeserializationError("not a dict")))
    with caplog.at_level(logging.WARNING, logger="boomi.rt_grace_shared"):
        out = _run(backend.get("k1"))
    assert out is None
    assert any("read swallowed" in rec.message for rec in caplog.records)


def test_get_swallows_runtime_storage_errors_returns_none(caplog):
    backend = SharedGraceBackend(_StubStore(get_exc=RuntimeError("mongo down")))
    with caplog.at_level(logging.WARNING, logger="boomi.rt_grace_shared"):
        out = _run(backend.get("k1"))
    assert out is None
    assert any("GRACE_SHARED_GET_FAILED" in rec.message for rec in caplog.records)


def test_get_does_not_swallow_cancelled_error():
    """Task cancellation is control flow, not a storage cache miss."""
    backend = SharedGraceBackend(_StubStore(get_exc=asyncio.CancelledError()))
    with pytest.raises(asyncio.CancelledError):
        _run(backend.get("k1"))


def test_put_swallows_storage_errors_and_logs(caplog):
    backend = SharedGraceBackend(_StubStore(put_exc=RuntimeError("mongo down")))
    with caplog.at_level(logging.WARNING, logger="boomi.rt_grace_shared"):
        # Must not raise — the in-process L1 cache covers the local caller
        _run(backend.put("k1", {"x": 1}, ttl_seconds=60))
    assert any("GRACE_SHARED_PUT_FAILED" in rec.message for rec in caplog.records)


# ---------- initialize_shared_grace_backend ----------

def test_initialize_disabled_returns_none(monkeypatch):
    monkeypatch.setenv("BOOMI_RT_GRACE_SHARED", "false")
    backend = initialize_shared_grace_backend("mongodb://x", Fernet(_new_key()))
    assert backend is None


def test_initialize_missing_uri_raises(monkeypatch):
    monkeypatch.setenv("BOOMI_RT_GRACE_SHARED", "true")
    with pytest.raises(ValueError, match="mongodb_uri"):
        initialize_shared_grace_backend("", Fernet(_new_key()))


def test_initialize_missing_fernet_raises(monkeypatch):
    monkeypatch.setenv("BOOMI_RT_GRACE_SHARED", "true")
    with pytest.raises(ValueError, match="Fernet"):
        initialize_shared_grace_backend("mongodb://x", None)


# ---------- Fix D.2 lock primitives ----------

class _FakeLockCollection:
    """In-memory motor-shaped collection used to test lock methods.

    Mimics pymongo's DuplicateKeyError on insert_one for an existing _id.
    """

    def __init__(self):
        self.docs: dict[str, dict] = {}
        self.insert_calls = 0
        self.delete_calls = 0

    async def insert_one(self, doc):
        self.insert_calls += 1
        key = doc["_id"]
        if key in self.docs:
            from pymongo.errors import DuplicateKeyError
            raise DuplicateKeyError("duplicate _id")
        self.docs[key] = doc

    async def delete_one(self, filt):
        self.delete_calls += 1
        self.docs.pop(filt["_id"], None)
        return None


def test_supports_locks_property():
    backend_a = SharedGraceBackend(_StubStore())
    assert backend_a.supports_locks is False
    backend_b = SharedGraceBackend(_StubStore(), lock_collection=_FakeLockCollection())
    assert backend_b.supports_locks is True


def test_try_claim_lock_first_wins_second_loses():
    lock_coll = _FakeLockCollection()
    backend = SharedGraceBackend(_StubStore(), lock_collection=lock_coll)
    first = _run(backend.try_claim_lock("k1", 30, "instance-A"))
    second = _run(backend.try_claim_lock("k1", 30, "instance-B"))
    assert first is True
    assert second is False
    assert lock_coll.docs["k1"]["instance"] == "instance-A"


def test_release_lock_lets_next_claim_succeed():
    lock_coll = _FakeLockCollection()
    backend = SharedGraceBackend(_StubStore(), lock_collection=lock_coll)
    assert _run(backend.try_claim_lock("k1", 30, "A")) is True
    _run(backend.release_lock("k1"))
    assert _run(backend.try_claim_lock("k1", 30, "B")) is True


def test_try_claim_lock_raises_when_locks_unsupported():
    backend = SharedGraceBackend(_StubStore())  # no lock_collection
    with pytest.raises(RuntimeError, match="BOOMI_RT_GRACE_DISTRIBUTED_LOCK"):
        _run(backend.try_claim_lock("k1", 30, "A"))


def test_write_failure_marker_lands_in_shared_cache():
    backing = MemoryStore()
    wrapped = FernetEncryptionWrapper(key_value=backing, fernet=Fernet(_new_key()))
    backend = SharedGraceBackend(wrapped)
    _run(backend.write_failure_marker("k1", "RuntimeError"))
    payload = _run(backend.get("k1"))
    assert payload == {"error": "RuntimeError"}
