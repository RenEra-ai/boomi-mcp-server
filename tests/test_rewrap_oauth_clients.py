"""Tests for the OAuth client doc rewrap helper used in key rotation."""

from __future__ import annotations

import asyncio

import pytest
from cryptography.fernet import Fernet, InvalidToken, MultiFernet
from key_value.aio.stores.memory import MemoryStore
from key_value.aio.wrappers.encryption import FernetEncryptionWrapper


def _run(coro):
    return asyncio.new_event_loop().run_until_complete(coro)


def _new_key() -> bytes:
    return Fernet.generate_key()


def test_rewrap_round_trip_with_multifernet_moves_ciphertext_to_newest_key():
    """End-to-end model of what the rewrap script does in production.

    1. Write a client doc encrypted with OLD key.
    2. Open a MultiFernet([NEW, OLD]) wrapper. Get returns the cleartext.
    3. Put the same value back -- now stored under NEW.
    4. Verify the new ciphertext is decryptable by NEW alone and
       NOT by OLD alone.
    """
    old_key = _new_key()
    new_key = _new_key()
    backing = MemoryStore()

    async def _scenario():
        # 1. Original write under OLD key.
        old_wrapper = FernetEncryptionWrapper(key_value=backing, fernet=Fernet(old_key))
        await old_wrapper.put(key="client-abc", value={"client_id": "client-abc", "name": "x"})

        # 2. & 3. Rewrap pass: open MultiFernet wrapper, read+write.
        mf = MultiFernet([Fernet(new_key), Fernet(old_key)])
        mf_wrapper = FernetEncryptionWrapper(key_value=backing, fernet=mf)
        value = await mf_wrapper.get(key="client-abc")
        assert value == {"client_id": "client-abc", "name": "x"}
        await mf_wrapper.put(key="client-abc", value=value)

        # 4. Post-rewrap: NEW-only wrapper succeeds; OLD-only fails.
        new_only = FernetEncryptionWrapper(key_value=backing, fernet=Fernet(new_key))
        assert await new_only.get(key="client-abc") == {"client_id": "client-abc", "name": "x"}

        old_only = FernetEncryptionWrapper(key_value=backing, fernet=Fernet(old_key))
        from key_value.aio.errors import DecryptionError
        with pytest.raises(DecryptionError):
            await old_only.get(key="client-abc")

    _run(_scenario())


def test_rewrap_skips_docs_unreadable_by_any_listed_key():
    """A doc encrypted with a never-listed key surfaces as DecryptionError.

    The rewrap script logs and continues; this test just confirms the
    underlying stack raises so the script's failure path triggers.
    """
    orphan_key = _new_key()
    new_key = _new_key()
    backing = MemoryStore()

    async def _scenario():
        orphan_wrapper = FernetEncryptionWrapper(key_value=backing, fernet=Fernet(orphan_key))
        await orphan_wrapper.put(key="orphan", value={"x": 1})

        mf = MultiFernet([Fernet(new_key)])  # orphan_key NOT listed
        mf_wrapper = FernetEncryptionWrapper(key_value=backing, fernet=mf)
        from key_value.aio.errors import DecryptionError
        with pytest.raises(DecryptionError):
            await mf_wrapper.get(key="orphan")

    _run(_scenario())
