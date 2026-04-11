"""Tests for the VerifiedStorage wrapper and consent CSP patch."""

import asyncio
import os
import sys
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock

import pytest

_project_root = str(Path(__file__).resolve().parent.parent)
if _project_root not in sys.path:
    sys.path.insert(0, _project_root)

from mcp.server.auth.provider import TokenError
from verified_storage import VerifiedStorage

TOKEN_DICT = {"access_token": "at-123", "refresh_token": "rt-456", "expires_in": 3600}
STALE_DICT = {"access_token": "at-OLD", "refresh_token": "rt-OLD", "expires_in": 3600}


def _run(coro):
    """Run an async coroutine synchronously."""
    return asyncio.get_event_loop().run_until_complete(coro)


class TestVerifiedStorage:

    @pytest.fixture
    def mock_kv(self):
        kv = AsyncMock()
        kv.put = AsyncMock()
        kv.get = AsyncMock()
        kv.delete = AsyncMock()
        kv.list = AsyncMock(return_value=[])
        return kv

    def test_put_succeeds_when_readback_matches(self, mock_kv):
        mock_kv.get.return_value = TOKEN_DICT.copy()
        store = VerifiedStorage(mock_kv)

        _run(store.put(key="token-123", value=TOKEN_DICT))

        mock_kv.put.assert_awaited_once_with(key="token-123", value=TOKEN_DICT)
        mock_kv.get.assert_awaited_once_with(key="token-123")

    def test_put_raises_token_error_when_readback_returns_none(self, mock_kv):
        mock_kv.get.return_value = None
        store = VerifiedStorage(mock_kv)

        with pytest.raises(TokenError, match="Failed to persist") as exc_info:
            _run(store.put(key="token-456", value=TOKEN_DICT))
        assert exc_info.value.error == "invalid_request"

    def test_put_raises_token_error_when_readback_is_stale(self, mock_kv):
        mock_kv.get.return_value = STALE_DICT
        store = VerifiedStorage(mock_kv)

        with pytest.raises(TokenError, match="does not match read-back") as exc_info:
            _run(store.put(key="token-789", value=TOKEN_DICT))
        assert exc_info.value.error == "invalid_request"

    def test_token_error_code_serializes_to_token_error_response(self, mock_kv):
        """TokenErrorResponse rejects codes outside its Literal type.
        Verify the error code we raise survives serialization."""
        from mcp.server.auth.handlers.token import TokenErrorResponse
        mock_kv.get.return_value = None
        store = VerifiedStorage(mock_kv)

        with pytest.raises(TokenError) as exc_info:
            _run(store.put(key="k1", value=TOKEN_DICT))
        resp = TokenErrorResponse(
            error=exc_info.value.error,
            error_description=exc_info.value.error_description,
        )
        assert resp.error == "invalid_request"

    def test_token_error_not_rewritten_to_401(self, mock_kv):
        """invalid_grant gets rewritten to 401 by FastMCP auth.py (line 99).
        Storage failures must not trigger that path — verify we use a code
        that stays as 400 through the real TokenHandler response path."""
        from mcp.server.auth.handlers.token import TokenErrorResponse, TokenHandler
        mock_kv.get.return_value = None
        store = VerifiedStorage(mock_kv)

        with pytest.raises(TokenError) as exc_info:
            _run(store.put(key="k1", value=TOKEN_DICT))

        error_resp = TokenErrorResponse(
            error=exc_info.value.error,
            error_description=exc_info.value.error_description,
        )
        # TokenHandler.response() returns 400 for error responses,
        # but auth.py upgrades invalid_grant to 401. Verify our code
        # is not invalid_grant so it won't be upgraded.
        assert error_resp.error != "invalid_grant"
        handler = TokenHandler.__new__(TokenHandler)
        http_response = handler.response(error_resp)
        assert http_response.status_code == 400

    def test_get_passes_through(self, mock_kv):
        mock_kv.get.return_value = TOKEN_DICT
        store = VerifiedStorage(mock_kv)

        result = _run(store.get(key="k1"))
        assert result == TOKEN_DICT
        mock_kv.get.assert_awaited_once_with(key="k1")

    def test_delete_passes_through(self, mock_kv):
        store = VerifiedStorage(mock_kv)
        _run(store.delete(key="k1"))
        mock_kv.delete.assert_awaited_once_with(key="k1")

    def test_put_forwards_collection_kwarg_to_get(self, mock_kv):
        mock_kv.get.return_value = TOKEN_DICT.copy()
        store = VerifiedStorage(mock_kv)

        _run(store.put(key="k1", value=TOKEN_DICT, collection="mcp-upstream-tokens"))

        mock_kv.put.assert_awaited_once_with(
            key="k1", value=TOKEN_DICT, collection="mcp-upstream-tokens"
        )
        mock_kv.get.assert_awaited_once_with(key="k1", collection="mcp-upstream-tokens")

    def test_getattr_delegates_unknown_attributes(self, mock_kv):
        mock_kv.some_property = "hello"
        store = VerifiedStorage(mock_kv)
        assert store.some_property == "hello"


class TestConsentCspPatch:

    def test_claude_desktop_gets_no_form_action(self):
        from fastmcp.server.auth.oauth_proxy import consent
        original_fn = consent.create_consent_html

        original_html = (
            '<meta http-equiv="Content-Security-Policy" '
            'content="default-src \'none\'; style-src \'unsafe-inline\'"'
            ' />'
        )

        def mock_create_consent_html(*args, **kwargs):
            return original_html

        consent.create_consent_html = mock_create_consent_html

        try:
            from consent_csp_patch import apply_consent_csp_patch
            apply_consent_csp_patch()
            result = consent.create_consent_html(
                client_id="test",
                redirect_uri="https://claude.ai/callback",
                scopes=["read"],
                txn_id="txn1",
                csrf_token="csrf1",
            )
            assert "form-action" not in result
            assert "base-uri 'none'" in result
        finally:
            consent.create_consent_html = original_fn

    def test_chatgpt_gets_form_action_with_self(self):
        from fastmcp.server.auth.oauth_proxy import consent
        original_fn = consent.create_consent_html

        original_html = (
            '<meta http-equiv="Content-Security-Policy" '
            'content="default-src \'none\'; style-src \'unsafe-inline\'"'
            ' />'
        )

        def mock_create_consent_html(*args, **kwargs):
            return original_html

        consent.create_consent_html = mock_create_consent_html

        try:
            from consent_csp_patch import apply_consent_csp_patch
            apply_consent_csp_patch()
            result = consent.create_consent_html(
                client_id="test",
                redirect_uri="https://chatgpt.com/callback",
                scopes=["read"],
                txn_id="txn1",
                csrf_token="csrf1",
            )
            assert "form-action 'self' https: http:" in result
        finally:
            consent.create_consent_html = original_fn

    def test_custom_scheme_appended(self):
        from fastmcp.server.auth.oauth_proxy import consent
        original_fn = consent.create_consent_html

        original_html = (
            '<meta http-equiv="Content-Security-Policy" '
            'content="default-src \'none\'; style-src \'unsafe-inline\'"'
            ' />'
        )

        def mock_create_consent_html(*args, **kwargs):
            return original_html

        consent.create_consent_html = mock_create_consent_html

        try:
            from consent_csp_patch import apply_consent_csp_patch
            apply_consent_csp_patch()
            result = consent.create_consent_html(
                client_id="test",
                redirect_uri="cursor://callback",
                scopes=["read"],
                txn_id="txn1",
                csrf_token="csrf1",
            )
            assert "cursor:" in result
            assert "form-action" in result
        finally:
            consent.create_consent_html = original_fn
