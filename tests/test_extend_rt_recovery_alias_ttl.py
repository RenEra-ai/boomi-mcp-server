"""Tests for the recovery-alias TTL extension admin script."""

from __future__ import annotations

import asyncio
import sys
import time
from pathlib import Path

import pytest
from cryptography.fernet import Fernet, MultiFernet
from key_value.aio.errors import DecryptionError
from key_value.aio.stores.memory import MemoryStore
from key_value.aio.wrappers.encryption import FernetEncryptionWrapper

# The script lives under scripts/; make it importable as a module.
_SCRIPTS = Path(__file__).resolve().parent.parent / "scripts"
if str(_SCRIPTS) not in sys.path:
    sys.path.insert(0, str(_SCRIPTS))

import extend_rt_recovery_alias_ttl as ext  # noqa: E402


_THIRTY_DAYS = 60 * 60 * 24 * 30


def _run(coro):
    return asyncio.new_event_loop().run_until_complete(coro)


def _new_key() -> bytes:
    return Fernet.generate_key()


def _alias(successor_expires_at, *, client_id="client-1", successor_hash="succ-hash") -> dict:
    """Build an alias record matching refresh_token_recovery_patch's schema."""
    now = time.time()
    return {
        "version": 1,
        "client_id": client_id,
        "scopes": ["openid"],
        "successor_rt_hash": successor_hash,
        "successor_refresh_jti": "succ-jti",
        "upstream_token_id": "ut-1",
        "successor_expires_at": int(successor_expires_at),
        "created_at": now,
        "updated_at": now,
        "reason": "normal_rotation",
    }


def test_dry_run_reports_counts_without_writing():
    """Dry run tallies disposition and never mutates the store."""
    backing = MemoryStore()
    wrapped = FernetEncryptionWrapper(key_value=backing, fernet=Fernet(_new_key()))
    now = time.time()

    async def _scenario():
        # 1 live, 1 expired, 1 malformed (no successor_expires_at).
        await wrapped.put(key="live", value=_alias(now + 20 * 86400), ttl=600)
        await wrapped.put(key="dead", value=_alias(now - 100), ttl=600)
        malformed = _alias(now + 20 * 86400)
        malformed.pop("successor_expires_at")
        await wrapped.put(key="bad", value=malformed, ttl=600)

        counts = await ext.extend_aliases(
            wrapped, ["live", "dead", "bad"], max_age=_THIRTY_DAYS, apply=False, now=now
        )
        # The live alias's stored TTL must be untouched by a dry run.
        _, live_ttl = await backing.ttl(key="live")
        return counts, live_ttl

    counts, live_ttl = _run(_scenario())
    assert counts["total_scanned"] == 3
    assert counts["would_extend"] == 1
    assert counts["skipped_expired"] == 1
    assert counts["skipped_malformed"] == 1
    assert counts["decryptable"] == 2  # live + dead (both have successor_expires_at)
    assert counts["extended"] == 0
    # No write happened -> original 600s ttl is still in force (not bumped to 30d).
    assert live_ttl is not None and live_ttl <= 600


def test_apply_extends_live_alias():
    """--apply re-writes a live alias with the extended (capped) TTL."""
    backing = MemoryStore()
    wrapped = FernetEncryptionWrapper(key_value=backing, fernet=Fernet(_new_key()))
    now = time.time()
    successor_expires_at = int(now + 20 * 86400)  # 20 days out -> under the 30d cap

    async def _scenario():
        await wrapped.put(key="live", value=_alias(successor_expires_at), ttl=600)
        counts = await ext.extend_aliases(
            wrapped, ["live"], max_age=_THIRTY_DAYS, apply=True, now=now
        )
        value, ttl = await backing.ttl(key="live")
        return counts, value, ttl

    counts, _value, ttl = _run(_scenario())
    assert counts["extended"] == 1
    assert counts["write_failed"] == 0
    expected = ext.alias_ttl_seconds(successor_expires_at, _THIRTY_DAYS, now)
    assert ttl is not None
    # Re-put bumped the TTL from 600s toward the ~20-day successor window.
    assert ttl > 600
    assert expected - 5 <= ttl <= expected + 1


def test_apply_ttl_capped_at_max_age():
    """A far-future successor caps the new TTL at max_age, not the full remaining."""
    backing = MemoryStore()
    wrapped = FernetEncryptionWrapper(key_value=backing, fernet=Fernet(_new_key()))
    now = time.time()
    successor_expires_at = int(now + 100 * 86400)  # 100 days out -> exceeds 30d cap

    async def _scenario():
        await wrapped.put(key="live", value=_alias(successor_expires_at), ttl=600)
        await ext.extend_aliases(wrapped, ["live"], max_age=_THIRTY_DAYS, apply=True, now=now)
        _, ttl = await backing.ttl(key="live")
        return ttl

    ttl = _run(_scenario())
    assert ttl is not None
    assert _THIRTY_DAYS - 5 <= ttl <= _THIRTY_DAYS + 1


def test_skips_expired_alias_on_apply():
    """An alias whose successor already expired is never extended."""
    backing = MemoryStore()
    wrapped = FernetEncryptionWrapper(key_value=backing, fernet=Fernet(_new_key()))
    now = time.time()

    async def _scenario():
        await wrapped.put(key="dead", value=_alias(now - 100), ttl=600)
        return await ext.extend_aliases(
            wrapped, ["dead"], max_age=_THIRTY_DAYS, apply=True, now=now
        )

    counts = _run(_scenario())
    assert counts["skipped_expired"] == 1
    assert counts["extended"] == 0
    assert counts["would_extend"] == 0


def test_skips_malformed_record_on_apply():
    """A record lacking successor_expires_at is skipped, not extended."""
    backing = MemoryStore()
    wrapped = FernetEncryptionWrapper(key_value=backing, fernet=Fernet(_new_key()))
    now = time.time()

    async def _scenario():
        malformed = _alias(now + 20 * 86400)
        malformed.pop("successor_expires_at")
        await wrapped.put(key="bad", value=malformed, ttl=600)
        return await ext.extend_aliases(
            wrapped, ["bad"], max_age=_THIRTY_DAYS, apply=True, now=now
        )

    counts = _run(_scenario())
    assert counts["skipped_malformed"] == 1
    assert counts["extended"] == 0
    assert counts["decryptable"] == 0


def test_decrypt_failure_counted_as_malformed():
    """A doc encrypted under a non-listed key surfaces as a skip, never a crash."""
    backing = MemoryStore()
    now = time.time()

    async def _scenario():
        orphan = FernetEncryptionWrapper(key_value=backing, fernet=Fernet(_new_key()))
        await orphan.put(key="orphan", value=_alias(now + 20 * 86400), ttl=600)
        # Reader holds a different key -> get() raises DecryptionError.
        reader = FernetEncryptionWrapper(key_value=backing, fernet=Fernet(_new_key()))
        return await ext.extend_aliases(
            reader, ["orphan"], max_age=_THIRTY_DAYS, apply=True, now=now
        )

    counts = _run(_scenario())
    assert counts["skipped_malformed"] == 1
    assert counts["extended"] == 0
    assert counts["decryptable"] == 0


def test_apply_reencrypts_under_newest_multifernet_key():
    """--apply re-writes the alias under the newest key during rotation."""
    old_key = _new_key()
    new_key = _new_key()
    backing = MemoryStore()
    now = time.time()

    async def _scenario():
        # Written under OLD.
        old_wrapper = FernetEncryptionWrapper(key_value=backing, fernet=Fernet(old_key))
        await old_wrapper.put(key="k1", value=_alias(now + 20 * 86400), ttl=600)

        # Extend through MultiFernet([NEW, OLD]) -> re-encrypts under NEW.
        mf = MultiFernet([Fernet(new_key), Fernet(old_key)])
        wrapped = FernetEncryptionWrapper(key_value=backing, fernet=mf)
        counts = await ext.extend_aliases(
            wrapped, ["k1"], max_age=_THIRTY_DAYS, apply=True, now=now
        )

        new_only = FernetEncryptionWrapper(key_value=backing, fernet=Fernet(new_key))
        new_value = await new_only.get(key="k1")

        old_only = FernetEncryptionWrapper(key_value=backing, fernet=Fernet(old_key))
        return counts, new_value, old_only

    counts, new_value, old_only = _run(_scenario())
    assert counts["extended"] == 1
    assert new_value is not None and new_value["client_id"] == "client-1"
    with pytest.raises(DecryptionError):
        _run(old_only.get(key="k1"))


def test_never_logs_token_material(capsys):
    """No raw alias key or successor hash appears in output -- only fingerprints."""
    backing = MemoryStore()
    wrapped = FernetEncryptionWrapper(key_value=backing, fernet=Fernet(_new_key()))
    now = time.time()
    raw_key = "a" * 64  # looks like a sha256 token hash
    secret_hash = "SUPER_SECRET_SUCCESSOR_HASH"

    async def _scenario():
        # One malformed (forces a per-record log line) carrying the secret hash.
        malformed = _alias(now + 20 * 86400, successor_hash=secret_hash)
        malformed.pop("successor_expires_at")
        await wrapped.put(key=raw_key, value=malformed, ttl=600)
        return await ext.extend_aliases(
            wrapped, [raw_key], max_age=_THIRTY_DAYS, apply=True, now=now
        )

    counts = _run(_scenario())
    out = capsys.readouterr().out
    assert counts["skipped_malformed"] == 1
    assert raw_key not in out
    assert secret_hash not in out
    # The fingerprint (hash-of-key) IS allowed and present.
    assert ext._fingerprint(raw_key) in out
