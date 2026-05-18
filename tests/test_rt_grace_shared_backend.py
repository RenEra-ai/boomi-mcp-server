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


def test_get_does_not_swallow_unrelated_errors():
    """A non-storage exception (e.g., asyncio.CancelledError) must propagate."""
    backend = SharedGraceBackend(_StubStore(get_exc=RuntimeError("mongo down")))
    with pytest.raises(RuntimeError, match="mongo down"):
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
