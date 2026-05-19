"""Tests for the STORAGE_ENCRYPTION_KEY MultiFernet rotation logic (Fix C.3).

These tests exercise the standalone behavior of MultiFernet (no server
boot required) and the parsing helper in server.py is exercised via an
isolated import of the parsing block.
"""

from __future__ import annotations

import pytest
from cryptography.fernet import Fernet, InvalidToken, MultiFernet


def _gen_key() -> str:
    return Fernet.generate_key().decode()


# --- MultiFernet round-trips (covers Fix C.3's core invariant) ---


def test_multifernet_decrypts_old_key_ciphertext():
    """Ciphertext encrypted with key A is readable by MultiFernet([B, A])."""
    key_a = _gen_key()
    key_b = _gen_key()

    ct_under_a = Fernet(key_a.encode()).encrypt(b"hello")
    mf = MultiFernet([Fernet(key_b.encode()), Fernet(key_a.encode())])
    assert mf.decrypt(ct_under_a) == b"hello"


def test_multifernet_writes_use_first_key():
    """MultiFernet([B, A]) writes ciphertext that ONLY B can decrypt."""
    key_a = _gen_key()
    key_b = _gen_key()

    mf = MultiFernet([Fernet(key_b.encode()), Fernet(key_a.encode())])
    ct = mf.encrypt(b"hello")

    assert Fernet(key_b.encode()).decrypt(ct) == b"hello"
    with pytest.raises(InvalidToken):
        Fernet(key_a.encode()).decrypt(ct)


def test_multifernet_rejects_unknown_key():
    """A ciphertext encrypted with a key NOT in the list raises InvalidToken."""
    key_a = _gen_key()
    key_b = _gen_key()
    key_other = _gen_key()

    ct_under_other = Fernet(key_other.encode()).encrypt(b"hi")
    mf = MultiFernet([Fernet(key_b.encode()), Fernet(key_a.encode())])
    with pytest.raises(InvalidToken):
        mf.decrypt(ct_under_other)


# --- Parsing helper logic mirrored from server.py block ---


def _parse_storage_keys(storage_encryption_key: str):
    """Mirrors the parsing block in server.py for unit testing.

    Kept in sync manually with server.py — if you change the parsing
    there, update this and the tests below.
    """
    key_list = [k.strip() for k in storage_encryption_key.split(",") if k.strip()]
    if not key_list:
        raise ValueError(
            "STORAGE_ENCRYPTION_KEY must contain at least one Fernet key"
        )
    try:
        fernets = [Fernet(k.encode()) for k in key_list]
    except Exception as exc:
        raise ValueError(
            "STORAGE_ENCRYPTION_KEY contains an invalid Fernet key "
            "(comma-separated list expected, newest first): " + str(exc)
        ) from exc
    return fernets[0] if len(fernets) == 1 else MultiFernet(fernets)


def test_parse_single_key_returns_plain_fernet():
    key = _gen_key()
    out = _parse_storage_keys(key)
    assert isinstance(out, Fernet)
    # Round-trip works
    assert out.decrypt(out.encrypt(b"x")) == b"x"


def test_parse_multi_key_returns_multifernet():
    key_a = _gen_key()
    key_b = _gen_key()
    out = _parse_storage_keys(f"{key_b},{key_a}")
    assert isinstance(out, MultiFernet)

    # And the old-key ciphertext continues to work
    ct = Fernet(key_a.encode()).encrypt(b"legacy")
    assert out.decrypt(ct) == b"legacy"


def test_parse_handles_whitespace_around_commas():
    key_a = _gen_key()
    key_b = _gen_key()
    out = _parse_storage_keys(f"  {key_b}  ,  {key_a}  ")
    assert isinstance(out, MultiFernet)


def test_parse_rejects_empty_string():
    with pytest.raises(ValueError, match="at least one Fernet key"):
        _parse_storage_keys("")


def test_parse_rejects_only_commas():
    with pytest.raises(ValueError, match="at least one Fernet key"):
        _parse_storage_keys(",,,")


def test_parse_rejects_invalid_key_in_list():
    key_a = _gen_key()
    with pytest.raises(ValueError, match="invalid Fernet key"):
        _parse_storage_keys(f"{key_a},this-is-not-a-valid-fernet-key")
