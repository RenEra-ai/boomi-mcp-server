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


def test_initialize_routes_grace_records_to_mcp_rt_grace(monkeypatch):
    """initialize_shared_grace_backend must build the MongoDB store so that
    collection-less get/put/delete land in `mcp-rt-grace` -- not the fallback
    `default_collection`. SharedGraceBackend passes no per-call collection, so
    the store's `default_collection` IS the grace collection.
    """
    monkeypatch.setenv("BOOMI_RT_GRACE_SHARED", "true")
    monkeypatch.setenv("BOOMI_RT_GRACE_DISTRIBUTED_LOCK", "false")
    monkeypatch.delenv("BOOMI_RT_GRACE_SHARED_COLLECTION", raising=False)

    recorded: dict = {}

    class _RecordingStore(MemoryStore):
        """A real AsyncKeyValue store that also records its constructor kwargs."""

        def __init__(self, **kwargs):
            recorded.update(kwargs)
            super().__init__()

    monkeypatch.setattr(
        "key_value.aio.stores.mongodb.MongoDBStore", _RecordingStore
    )

    backend = initialize_shared_grace_backend("mongodb://x", Fernet(_new_key()))

    assert backend is not None
    assert recorded.get("default_collection") == "mcp-rt-grace"
    # The inert `coll_name` arg must not be used to carry the collection name.
    assert recorded.get("coll_name") != "mcp-rt-grace"


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
        key = filt["_id"]
        existing = self.docs.get(key)
        if existing is None:
            return None
        # Match motor/pymongo semantics: only delete when every filter
        # field matches the stored document. Owner-scoped release_lock
        # passes both _id and instance, so the wrong instance is a no-op.
        if all(existing.get(k) == v for k, v in filt.items() if k != "_id"):
            del self.docs[key]
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
    _run(backend.release_lock("k1", "A"))
    assert _run(backend.try_claim_lock("k1", 30, "B")) is True


def test_release_lock_wrong_instance_does_not_delete():
    """A non-owner caller must not be able to delete another owner's lock.

    This is the hazard when try_claim_lock returns True after an
    ambiguous insert failure (the degrade-to-no-lock fallthrough):
    the non-owner caller would otherwise unlock the real owner.
    """
    lock_coll = _FakeLockCollection()
    backend = SharedGraceBackend(_StubStore(), lock_collection=lock_coll)
    assert _run(backend.try_claim_lock("k1", 30, "A")) is True
    _run(backend.release_lock("k1", "B"))  # wrong owner attempts release
    # A's lock must survive — a fresh claim still loses.
    assert _run(backend.try_claim_lock("k1", 30, "C")) is False
    assert lock_coll.docs["k1"]["instance"] == "A"


def test_stale_leader_release_does_not_evict_fresh_leader():
    """The TTL-expiry race: a slow leader runs past the lock TTL,
    Mongo's TTL sweep removes the lock, and a peer claims a fresh one.
    The original leader's finally block must NOT delete the new
    leader's lock — that would reopen the duplicate-refresh race the
    lock exists to prevent.
    """
    lock_coll = _FakeLockCollection()
    backend = SharedGraceBackend(_StubStore(), lock_collection=lock_coll)
    # Stale leader claims, lock TTL-expires, peer claims fresh:
    assert _run(backend.try_claim_lock("k1", 30, "stale-A")) is True
    lock_coll.docs.pop("k1")  # simulate Mongo TTL eviction
    assert _run(backend.try_claim_lock("k1", 30, "fresh-B")) is True
    # Stale leader now returns from its slow exchange and releases:
    _run(backend.release_lock("k1", "stale-A"))
    # Fresh leader's lock must still be intact; a third peer still loses.
    assert "k1" in lock_coll.docs
    assert lock_coll.docs["k1"]["instance"] == "fresh-B"
    assert _run(backend.try_claim_lock("k1", 30, "C")) is False


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


# ---------- Fix D.2 lazy lock init (Motor event-loop fix) ----------

class _RecordingLockCollection:
    """Records the running loop at create_index/insert time (no real Mongo)."""

    def __init__(self, *, index_exc=None):
        self.docs: dict[str, dict] = {}
        self.index_exc = index_exc
        self.create_index_loop = None
        self.create_index_calls = 0
        self.insert_calls = 0
        self.delete_calls = 0

    async def create_index(self, *args, **kwargs):
        self.create_index_calls += 1
        self.create_index_loop = id(asyncio.get_running_loop())
        if self.index_exc is not None:
            raise self.index_exc

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


def _make_recording_factory(collection):
    state = {"count": 0, "loop": None}

    class _Client:
        def __getitem__(self, _db):
            return _DB()

    class _DB:
        def __getitem__(self, _name):
            return collection

    def factory(uri):
        state["count"] += 1
        try:
            state["loop"] = id(asyncio.get_running_loop())
        except RuntimeError:
            state["loop"] = None
        return _Client()

    factory.state = state
    return factory


def test_distributed_lock_builds_no_client_at_init(monkeypatch):
    monkeypatch.setenv("BOOMI_RT_GRACE_SHARED", "true")
    monkeypatch.setenv("BOOMI_RT_GRACE_DISTRIBUTED_LOCK", "true")
    monkeypatch.delenv("BOOMI_RT_GRACE_SHARED_COLLECTION", raising=False)

    class _MemStore(MemoryStore):
        def __init__(self, **kwargs):
            super().__init__()

    monkeypatch.setattr("key_value.aio.stores.mongodb.MongoDBStore", _MemStore)

    coll = _RecordingLockCollection()
    factory = _make_recording_factory(coll)
    monkeypatch.setattr("rt_grace_shared_backend._default_lock_client_factory", factory)

    backend = initialize_shared_grace_backend("mongodb://x", Fernet(_new_key()))
    assert backend is not None
    assert backend.supports_locks is True
    # No client built at init time -- this is the event-loop-bug fix.
    assert factory.state["count"] == 0

    # First lock use lazily builds the client on the running serving loop.
    _run(backend.try_claim_lock("k1", 30, "A"))
    assert factory.state["count"] == 1
    assert coll.insert_calls == 1
    assert coll.create_index_calls == 1


def test_lock_collection_lazily_created_on_serving_loop():
    coll = _RecordingLockCollection()
    factory = _make_recording_factory(coll)
    backend = SharedGraceBackend(
        _StubStore(),
        lock_uri="mongodb://x",
        lock_db_name="boomi_mcp",
        lock_collection_name="mcp-rt-inflight-locks",
        lock_client_factory=factory,
    )

    async def _scenario():
        running = id(asyncio.get_running_loop())
        ok = await backend.try_claim_lock("k1", 30, "A")
        return running, ok

    running_loop, ok = _run(_scenario())
    assert ok is True
    # Client + index were created on the loop that ran the lock call, not at
    # construction time (which had no running loop).
    assert factory.state["loop"] == running_loop
    assert coll.create_index_loop == running_loop


def test_lazy_index_creation_failure_degrades_not_crash():
    coll = _RecordingLockCollection(index_exc=RuntimeError("index boom"))
    factory = _make_recording_factory(coll)
    backend = SharedGraceBackend(
        _StubStore(),
        lock_uri="mongodb://x",
        lock_db_name="boomi_mcp",
        lock_collection_name="locks",
        lock_client_factory=factory,
    )
    # Index creation raises, but the claim still proceeds.
    assert _run(backend.try_claim_lock("k1", 30, "A")) is True
    assert coll.insert_calls == 1


def test_supports_locks_reflects_lazy_config_presence():
    backend = SharedGraceBackend(_StubStore(), lock_uri="mongodb://x", lock_db_name="d", lock_collection_name="c")
    assert backend.supports_locks is True  # before any lazy init


def test_release_lock_lazily_inits_if_claim_never_called():
    coll = _RecordingLockCollection()
    factory = _make_recording_factory(coll)
    backend = SharedGraceBackend(
        _StubStore(),
        lock_uri="mongodb://x",
        lock_db_name="boomi_mcp",
        lock_collection_name="locks",
        lock_client_factory=factory,
    )
    # release alone (no prior claim) must still lazily build the collection.
    _run(backend.release_lock("k1", "A"))
    assert factory.state["count"] == 1
    assert coll.delete_calls == 1
