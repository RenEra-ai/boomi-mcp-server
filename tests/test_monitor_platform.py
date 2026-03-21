"""Unit tests for monitor_platform download_connector_document (mocked SDK)."""

import sys
import os
import base64
from unittest.mock import MagicMock, patch

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from src.boomi_mcp.categories.monitoring import (
    handle_download_connector_document,
    _download_content,
)


CREDS = {"username": "user@account", "password": "secret"}


# ── _download_content ────────────────────────────────────────────────


class TestDownloadContent:
    @patch("src.boomi_mcp.categories.monitoring.httpx.Client")
    def test_text_content_returned_inline(self, mock_client_cls):
        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.content = b"Hello, World!"
        mock_resp.headers = {"content-type": "text/plain"}
        mock_client = MagicMock()
        mock_client.__enter__ = MagicMock(return_value=mock_client)
        mock_client.__exit__ = MagicMock(return_value=False)
        mock_client.get.return_value = mock_resp
        mock_client_cls.return_value = mock_client

        result = _download_content("https://platform.boomi.com/download/123", CREDS)
        assert result["_downloaded"] is True
        assert result["content"] == "Hello, World!"
        assert result["content_type"] == "text/plain"

    @patch("src.boomi_mcp.categories.monitoring.httpx.Client")
    def test_binary_content_returned_as_base64(self, mock_client_cls):
        binary_data = bytes(range(256))
        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.content = binary_data
        mock_resp.headers = {"content-type": "application/octet-stream"}
        mock_client = MagicMock()
        mock_client.__enter__ = MagicMock(return_value=mock_client)
        mock_client.__exit__ = MagicMock(return_value=False)
        mock_client.get.return_value = mock_resp
        mock_client_cls.return_value = mock_client

        result = _download_content("https://platform.boomi.com/download/123", CREDS)
        assert result["_downloaded"] is True
        assert "content_base64" in result
        assert base64.b64decode(result["content_base64"]) == binary_data
        assert result["size_bytes"] == 256

    @patch("src.boomi_mcp.categories.monitoring.httpx.Client")
    def test_polls_202_then_200(self, mock_client_cls):
        resp_202 = MagicMock()
        resp_202.status_code = 202
        resp_200 = MagicMock()
        resp_200.status_code = 200
        resp_200.content = b"data"
        resp_200.headers = {"content-type": "text/plain"}

        mock_client = MagicMock()
        mock_client.__enter__ = MagicMock(return_value=mock_client)
        mock_client.__exit__ = MagicMock(return_value=False)
        mock_client.get.side_effect = [resp_202, resp_202, resp_200]
        mock_client_cls.return_value = mock_client

        result = _download_content("https://url", CREDS)
        assert result["_downloaded"] is True
        assert mock_client.get.call_count == 3

    @patch("src.boomi_mcp.categories.monitoring.httpx.Client")
    def test_timeout_after_continuous_202(self, mock_client_cls):
        resp_202 = MagicMock()
        resp_202.status_code = 202

        mock_client = MagicMock()
        mock_client.__enter__ = MagicMock(return_value=mock_client)
        mock_client.__exit__ = MagicMock(return_value=False)
        mock_client.get.return_value = resp_202
        mock_client_cls.return_value = mock_client

        result = _download_content("https://url", CREDS)
        assert result["_downloaded"] is False
        assert "202" in str(result.get("http_status", "")) or "failed" in result.get("error", "").lower()

    @patch("src.boomi_mcp.categories.monitoring.httpx.Client")
    def test_size_guard(self, mock_client_cls):
        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.content = b"x" * (11 * 1024 * 1024)  # 11 MB
        mock_resp.headers = {"content-type": "text/plain"}

        mock_client = MagicMock()
        mock_client.__enter__ = MagicMock(return_value=mock_client)
        mock_client.__exit__ = MagicMock(return_value=False)
        mock_client.get.return_value = mock_resp
        mock_client_cls.return_value = mock_client

        result = _download_content("https://url", CREDS)
        assert result["_downloaded"] is False
        assert "too large" in result.get("error", "").lower() or "limit" in result.get("error", "").lower()


# ── handle_download_connector_document ───────────────────────────────


class TestHandleDownloadConnectorDocument:
    def test_requires_generic_connector_record_id(self):
        sdk = MagicMock()
        result = handle_download_connector_document(sdk, {})
        assert result["_success"] is False
        assert "generic_connector_record_id" in result["error"]

    def test_url_only_mode(self):
        sdk = MagicMock()
        mock_result = MagicMock()
        mock_result._map.return_value = {
            "url": "https://platform.boomi.com/download/abc",
            "statusCode": "202",
            "message": "Processing",
        }
        sdk.connector_document.create_connector_document.return_value = mock_result

        result = handle_download_connector_document(
            sdk,
            {"generic_connector_record_id": "rec-123", "fetch_content": False},
            creds=CREDS,
        )
        assert result["_success"] is True
        assert result["download_url"] == "https://platform.boomi.com/download/abc"
        assert "fetch_content=false" in result.get("note", "")

    @patch("src.boomi_mcp.categories.monitoring._download_content")
    def test_fetches_content_when_enabled(self, mock_download):
        sdk = MagicMock()
        mock_result = MagicMock()
        mock_result._map.return_value = {
            "url": "https://platform.boomi.com/download/abc",
            "statusCode": "202",
        }
        sdk.connector_document.create_connector_document.return_value = mock_result
        mock_download.return_value = {
            "_downloaded": True,
            "content": "document data",
            "content_type": "text/xml",
            "size_bytes": 13,
        }

        result = handle_download_connector_document(
            sdk,
            {"generic_connector_record_id": "rec-123"},
            creds=CREDS,
        )
        assert result["_success"] is True
        assert result["_downloaded"] is True
        assert result["content"] == "document data"
        mock_download.assert_called_once_with(
            "https://platform.boomi.com/download/abc", CREDS
        )

    def test_no_creds_returns_url_with_note(self):
        sdk = MagicMock()
        mock_result = MagicMock()
        mock_result._map.return_value = {
            "url": "https://platform.boomi.com/download/abc",
        }
        sdk.connector_document.create_connector_document.return_value = mock_result

        result = handle_download_connector_document(
            sdk,
            {"generic_connector_record_id": "rec-123"},
            creds=None,
        )
        assert result["_success"] is True
        assert "Basic auth" in result.get("note", "")
