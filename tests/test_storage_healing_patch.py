"""Tests for the storage self-healing patch (Fix C.2)."""

from __future__ import annotations

import asyncio
import importlib
import logging
import sys
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest


def _fresh_module():
    sys.modules.pop("storage_healing_patch", None)
    return importlib.import_module("storage_healing_patch")


def _run(coro):
    return asyncio.new_event_loop().run_until_complete(coro)


def _make_provider(get_client_impl):
    """Build a stub OAuthProxy-shaped object."""
    provider = SimpleNamespace()
    provider.get_client = get_client_impl  # bound-method-ish; the patch
    #                                       captures this and rebinds via
    #                                       types.MethodType.
    provider._client_store = SimpleNamespace(delete=AsyncMock(return_value=True))
    return provider


def test_passthrough_when_get_client_succeeds(monkeypatch):
    monkeypatch.setenv("BOOMI_AUTH_HEAL_CORRUPT_CLIENTS", "true")
    sentinel = object()

    async def ok(client_id):
        return sentinel

    provider = _make_provider(ok)
    _fresh_module().apply_storage_healing_patch(provider)

    out = _run(provider.get_client("client-abc"))
    assert out is sentinel
    provider._client_store.delete.assert_not_called()


def test_heals_on_real_decryption_error_from_keyvalue_wrapper(monkeypatch, caplog):
    """Primary prod path: FernetEncryptionWrapper raises DecryptionError."""
    monkeypatch.setenv("BOOMI_AUTH_HEAL_CORRUPT_CLIENTS", "true")
    from key_value.aio.errors import DecryptionError

    async def boom(client_id):
        raise DecryptionError("Failed to decrypt value")

    provider = _make_provider(boom)
    _fresh_module().apply_storage_healing_patch(provider)

    with caplog.at_level(logging.ERROR, logger="boomi.storage_healing"):
        out = _run(provider.get_client("client-abc-with-very-long-id-string"))

    assert out is None
    provider._client_store.delete.assert_awaited_once_with(
        key="client-abc-with-very-long-id-string"
    )
    assert any(
        "Corrupted oauth client document detected" in rec.message
        for rec in caplog.records
    )


def test_heals_on_real_deserialization_error_from_pydantic_adapter(monkeypatch):
    """Primary prod path: PydanticAdapter raises DeserializationError."""
    monkeypatch.setenv("BOOMI_AUTH_HEAL_CORRUPT_CLIENTS", "true")
    from key_value.aio.errors import DeserializationError

    async def boom(client_id):
        raise DeserializationError("Invalid ProxyDCRClient: [...]")

    provider = _make_provider(boom)
    _fresh_module().apply_storage_healing_patch(provider)

    out = _run(provider.get_client("client-xyz"))
    assert out is None
    provider._client_store.delete.assert_awaited_once_with(key="client-xyz")


def test_defense_in_depth_invalid_token(monkeypatch):
    """Defensive catch: bare cryptography.InvalidToken (in case wrapper changes)."""
    monkeypatch.setenv("BOOMI_AUTH_HEAL_CORRUPT_CLIENTS", "true")
    from cryptography.fernet import InvalidToken

    async def boom(client_id):
        raise InvalidToken("ciphertext can't be decrypted")

    provider = _make_provider(boom)
    _fresh_module().apply_storage_healing_patch(provider)

    out = _run(provider.get_client("client-xyz"))
    assert out is None
    provider._client_store.delete.assert_awaited_once_with(key="client-xyz")


def test_defense_in_depth_pydantic_validation_error(monkeypatch):
    """Defensive catch: bare pydantic.ValidationError."""
    monkeypatch.setenv("BOOMI_AUTH_HEAL_CORRUPT_CLIENTS", "true")
    from pydantic import BaseModel, ValidationError

    class _T(BaseModel):
        x: int

    try:
        _T(x="not-an-int")
    except ValidationError as captured:
        validation_error = captured

    async def boom(client_id):
        raise validation_error

    provider = _make_provider(boom)
    _fresh_module().apply_storage_healing_patch(provider)

    out = _run(provider.get_client("client-xyz"))
    assert out is None
    provider._client_store.delete.assert_awaited_once_with(key="client-xyz")


def test_heal_disabled_skips_delete_but_still_returns_none(monkeypatch, caplog):
    monkeypatch.setenv("BOOMI_AUTH_HEAL_CORRUPT_CLIENTS", "false")
    from key_value.aio.errors import DecryptionError

    async def boom(client_id):
        raise DecryptionError("nope")

    provider = _make_provider(boom)
    _fresh_module().apply_storage_healing_patch(provider)

    with caplog.at_level(logging.ERROR, logger="boomi.storage_healing"):
        out = _run(provider.get_client("client-xyz"))

    assert out is None
    provider._client_store.delete.assert_not_called()
    # ERROR log still fires so operators see the corruption.
    assert any(
        "Corrupted oauth client document detected" in rec.message
        for rec in caplog.records
    )


def test_non_decryption_exceptions_propagate(monkeypatch):
    """Random errors (network, code bugs) must NOT be swallowed by healing."""
    monkeypatch.setenv("BOOMI_AUTH_HEAL_CORRUPT_CLIENTS", "true")

    async def boom(client_id):
        raise RuntimeError("mongodb unreachable")

    provider = _make_provider(boom)
    _fresh_module().apply_storage_healing_patch(provider)

    with pytest.raises(RuntimeError, match="mongodb unreachable"):
        _run(provider.get_client("client-abc"))
    provider._client_store.delete.assert_not_called()
