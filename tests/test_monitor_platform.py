"""Unit tests for monitor_platform actions (mocked SDK)."""

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
    handle_execution_summary,
    handle_document_counts,
    handle_execution_counts,
    handle_api_usage_counts,
    handle_connection_licensing_report,
    handle_custom_tracked_fields,
    handle_edi_connector_records,
    monitor_platform_action,
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


# ── Helper: build a mock query response ──────────────────────────────


def _mock_query_response(entries, query_token=None):
    """Build a MagicMock that looks like an SDK query response."""
    resp = MagicMock()
    resp.result = entries
    resp.query_token = query_token
    return resp


def _mock_entry(**fields):
    """Build a MagicMock SDK entry with given attributes and a _map() method."""
    entry = MagicMock()
    for k, v in fields.items():
        setattr(entry, k, v)
    entry._map.return_value = fields
    return entry


# ── handle_execution_summary ─────────────────────────────────────────


class TestHandleExecutionSummary:
    def test_requires_at_least_one_filter(self):
        sdk = MagicMock()
        result = handle_execution_summary(sdk, {})
        assert result["_success"] is False
        assert "filter" in result["error"].lower()

    def test_query_by_process_id(self):
        sdk = MagicMock()
        entry = _mock_entry(process_id="pid-1", process_name="Test", execution_count=5)
        sdk.execution_summary_record.query_execution_summary_record.return_value = (
            _mock_query_response([entry])
        )

        result = handle_execution_summary(sdk, {"process_id": "pid-1"})
        assert result["_success"] is True
        assert result["total_count"] == 1
        assert len(result["execution_summary_records"]) == 1
        sdk.execution_summary_record.query_execution_summary_record.assert_called_once()

    def test_pagination(self):
        sdk = MagicMock()
        e1 = _mock_entry(process_id="p1", execution_count=3)
        e2 = _mock_entry(process_id="p2", execution_count=7)
        page1 = _mock_query_response([e1], query_token="tok-1")
        page2 = _mock_query_response([e2])
        sdk.execution_summary_record.query_execution_summary_record.return_value = page1
        sdk.execution_summary_record.query_more_execution_summary_record.return_value = page2

        result = handle_execution_summary(sdk, {"atom_id": "atom-1"})
        assert result["_success"] is True
        assert result["total_count"] == 2
        sdk.execution_summary_record.query_more_execution_summary_record.assert_called_once_with(
            request_body="tok-1"
        )

    def test_date_range_filter(self):
        sdk = MagicMock()
        sdk.execution_summary_record.query_execution_summary_record.return_value = (
            _mock_query_response([])
        )

        result = handle_execution_summary(sdk, {
            "start_date": "2025-01-01T00:00:00Z",
            "end_date": "2025-01-31T23:59:59Z"
        })
        assert result["_success"] is True
        sdk.execution_summary_record.query_execution_summary_record.assert_called_once()


# ── handle_document_counts ───────────────────────────────────────────


class TestHandleDocumentCounts:
    def test_requires_at_least_one_filter(self):
        sdk = MagicMock()
        result = handle_document_counts(sdk, {})
        assert result["_success"] is False
        assert "filter" in result["error"].lower()

    def test_account_scope(self):
        sdk = MagicMock()
        entry = _mock_entry(processDate="2025-01-15", value=42)
        sdk.document_count_account.query_document_count_account.return_value = (
            _mock_query_response([entry])
        )

        result = handle_document_counts(sdk, {"start_date": "2025-01-01"})
        assert result["_success"] is True
        assert result["scope"] == "account"
        assert result["total_count"] == 1
        sdk.document_count_account.query_document_count_account.assert_called_once()

    def test_group_scope(self):
        sdk = MagicMock()
        entry = _mock_entry(processDate="2025-01-15", value=10)
        sdk.document_count_account_group.query_document_count_account_group.return_value = (
            _mock_query_response([entry])
        )

        result = handle_document_counts(sdk, {
            "account_group_id": "grp-1",
            "start_date": "2025-01-01",
        })
        assert result["_success"] is True
        assert result["scope"] == "account_group"
        assert result["account_group_id"] == "grp-1"
        sdk.document_count_account_group.query_document_count_account_group.assert_called_once()

    def test_group_pagination(self):
        sdk = MagicMock()
        e1 = _mock_entry(processDate="2025-01-15")
        e2 = _mock_entry(processDate="2025-01-16")
        page1 = _mock_query_response([e1], query_token="tok-g")
        page2 = _mock_query_response([e2])
        sdk.document_count_account_group.query_document_count_account_group.return_value = page1
        sdk.document_count_account_group.query_more_document_count_account_group.return_value = page2

        result = handle_document_counts(sdk, {"account_group_id": "grp-1"})
        assert result["_success"] is True
        assert result["total_count"] == 2


# ── handle_execution_counts ──────────────────────────────────────────


class TestHandleExecutionCounts:
    def test_requires_at_least_one_filter(self):
        sdk = MagicMock()
        result = handle_execution_counts(sdk, {})
        assert result["_success"] is False

    def test_account_scope(self):
        sdk = MagicMock()
        entry = _mock_entry(processDate="2025-01-15", value=100)
        sdk.execution_count_account.query_execution_count_account.return_value = (
            _mock_query_response([entry])
        )

        result = handle_execution_counts(sdk, {"start_date": "2025-01-01"})
        assert result["_success"] is True
        assert result["scope"] == "account"
        assert result["total_count"] == 1

    def test_group_scope(self):
        sdk = MagicMock()
        entry = _mock_entry(processDate="2025-01-15", value=50)
        sdk.execution_count_account_group.query_execution_count_account_group.return_value = (
            _mock_query_response([entry])
        )

        result = handle_execution_counts(sdk, {
            "account_group_id": "grp-2",
            "start_date": "2025-01-01",
        })
        assert result["_success"] is True
        assert result["scope"] == "account_group"


# ── handle_api_usage_counts ──────────────────────────────────────────


class TestHandleApiUsageCounts:
    def test_requires_at_least_one_filter(self):
        sdk = MagicMock()
        result = handle_api_usage_counts(sdk, {})
        assert result["_success"] is False

    def test_query_by_date_range(self):
        sdk = MagicMock()
        entry = _mock_entry(processDate="2025-01-15", classification="API", successCount=10, errorCount=2)
        sdk.api_usage_count.query_api_usage_count.return_value = (
            _mock_query_response([entry])
        )

        result = handle_api_usage_counts(sdk, {
            "start_date": "2025-01-01",
            "end_date": "2025-01-31",
        })
        assert result["_success"] is True
        assert result["total_count"] == 1
        assert len(result["api_usage_counts"]) == 1

    def test_pagination(self):
        sdk = MagicMock()
        e1 = _mock_entry(processDate="2025-01-15")
        e2 = _mock_entry(processDate="2025-01-16")
        page1 = _mock_query_response([e1], query_token="tok-api")
        page2 = _mock_query_response([e2])
        sdk.api_usage_count.query_api_usage_count.return_value = page1
        sdk.api_usage_count.query_more_api_usage_count.return_value = page2

        result = handle_api_usage_counts(sdk, {"start_date": "2025-01-01"})
        assert result["_success"] is True
        assert result["total_count"] == 2


# ── handle_connection_licensing_report ───────────────────────────────


class TestHandleConnectionLicensingReport:
    def test_returns_download_url(self):
        sdk = MagicMock()
        mock_result = MagicMock()
        mock_result.url = "https://platform.boomi.com/download/lic-123"
        mock_result.status_code = "202"
        mock_result.message = "Report generating"
        sdk.connection_licensing_report.create_connection_licensing_report.return_value = mock_result

        result = handle_connection_licensing_report(sdk, {})
        assert result["_success"] is True
        assert result["url"] == "https://platform.boomi.com/download/lic-123"
        assert "hint" in result

    def test_empty_config_passes_none(self):
        sdk = MagicMock()
        mock_result = MagicMock()
        mock_result.url = "https://example.com"
        mock_result.status_code = None
        mock_result.message = None
        sdk.connection_licensing_report.create_connection_licensing_report.return_value = mock_result

        handle_connection_licensing_report(sdk, {})
        sdk.connection_licensing_report.create_connection_licensing_report.assert_called_once_with(
            request_body=None
        )


# ── handle_custom_tracked_fields ─────────────────────────────────────


class TestHandleCustomTrackedFields:
    def test_returns_fields(self):
        sdk = MagicMock()
        entry = _mock_entry(name="CustomField1", displayName="Custom Field 1")
        sdk.custom_tracked_field.query_custom_tracked_field.return_value = (
            _mock_query_response([entry])
        )

        result = handle_custom_tracked_fields(sdk, {})
        assert result["_success"] is True
        assert result["total_count"] == 1
        assert len(result["custom_tracked_fields"]) == 1

    def test_passes_none_as_body(self):
        sdk = MagicMock()
        sdk.custom_tracked_field.query_custom_tracked_field.return_value = (
            _mock_query_response([])
        )

        handle_custom_tracked_fields(sdk, {})
        sdk.custom_tracked_field.query_custom_tracked_field.assert_called_once_with(
            request_body=None
        )

    def test_pagination(self):
        sdk = MagicMock()
        e1 = _mock_entry(name="Field1")
        e2 = _mock_entry(name="Field2")
        page1 = _mock_query_response([e1], query_token="tok-ctf")
        page2 = _mock_query_response([e2])
        sdk.custom_tracked_field.query_custom_tracked_field.return_value = page1
        sdk.custom_tracked_field.query_more_custom_tracked_field.return_value = page2

        result = handle_custom_tracked_fields(sdk, {})
        assert result["_success"] is True
        assert result["total_count"] == 2


# ── handle_edi_connector_records ─────────────────────────────────────


class TestHandleEdiConnectorRecords:
    def test_requires_standard(self):
        sdk = MagicMock()
        result = handle_edi_connector_records(sdk, {})
        assert result["_success"] is False
        assert "standard" in result["error"].lower()
        assert "valid_standards" in result

    def test_rejects_unknown_standard(self):
        sdk = MagicMock()
        result = handle_edi_connector_records(sdk, {"standard": "bogus"})
        assert result["_success"] is False
        assert "bogus" in result["error"]

    def test_x12_dispatch(self):
        sdk = MagicMock()
        entry = _mock_entry(id_="rec-1", standard="x12")
        sdk.x12_connector_record.query_x12_connector_record.return_value = (
            _mock_query_response([entry])
        )

        result = handle_edi_connector_records(sdk, {"standard": "x12"})
        assert result["_success"] is True
        assert result["standard"] == "x12"
        assert result["total_count"] == 1
        sdk.x12_connector_record.query_x12_connector_record.assert_called_once()

    def test_as2_dispatch(self):
        sdk = MagicMock()
        entry = _mock_entry(id_="rec-2")
        sdk.as2_connector_record.query_as2_connector_record.return_value = (
            _mock_query_response([entry])
        )

        result = handle_edi_connector_records(sdk, {"standard": "as2"})
        assert result["_success"] is True
        assert result["standard"] == "as2"
        sdk.as2_connector_record.query_as2_connector_record.assert_called_once()

    def test_edifact_dispatch(self):
        sdk = MagicMock()
        sdk.edifact_connector_record.query_edifact_connector_record.return_value = (
            _mock_query_response([])
        )

        result = handle_edi_connector_records(sdk, {"standard": "edifact"})
        assert result["_success"] is True
        assert result["standard"] == "edifact"

    def test_rosettanet_dispatch(self):
        sdk = MagicMock()
        sdk.rosetta_net_connector_record.query_rosetta_net_connector_record.return_value = (
            _mock_query_response([])
        )

        result = handle_edi_connector_records(sdk, {"standard": "rosettanet"})
        assert result["_success"] is True
        assert result["standard"] == "rosettanet"

    def test_pagination(self):
        sdk = MagicMock()
        e1 = _mock_entry(id_="r1")
        e2 = _mock_entry(id_="r2")
        page1 = _mock_query_response([e1], query_token="tok-edi")
        page2 = _mock_query_response([e2])
        sdk.x12_connector_record.query_x12_connector_record.return_value = page1
        sdk.x12_connector_record.query_more_x12_connector_record.return_value = page2

        result = handle_edi_connector_records(sdk, {"standard": "x12"})
        assert result["_success"] is True
        assert result["total_count"] == 2

    def test_case_insensitive_standard(self):
        sdk = MagicMock()
        sdk.hl7_connector_record.query_hl7_connector_record.return_value = (
            _mock_query_response([])
        )

        result = handle_edi_connector_records(sdk, {"standard": "HL7"})
        assert result["_success"] is True
        assert result["standard"] == "hl7"


# ── monitor_platform_action router ──────────────────────────────────


class TestMonitorPlatformActionRouter:
    """Test that the router dispatches new actions correctly."""

    def test_unknown_action_lists_all_valid(self):
        sdk = MagicMock()
        result = monitor_platform_action(sdk, "dev", "nonexistent_action")
        assert result["_success"] is False
        valid = result["valid_actions"]
        for action in [
            "execution_summary", "document_counts", "execution_counts",
            "api_usage_counts", "connection_licensing_report",
            "custom_tracked_fields", "edi_connector_records",
        ]:
            assert action in valid

    def test_routes_execution_summary(self):
        sdk = MagicMock()
        sdk.execution_summary_record.query_execution_summary_record.return_value = (
            _mock_query_response([])
        )
        result = monitor_platform_action(sdk, "dev", "execution_summary", {"process_id": "p1"})
        assert result["_success"] is True

    def test_routes_custom_tracked_fields(self):
        sdk = MagicMock()
        sdk.custom_tracked_field.query_custom_tracked_field.return_value = (
            _mock_query_response([])
        )
        result = monitor_platform_action(sdk, "dev", "custom_tracked_fields")
        assert result["_success"] is True

    def test_api_error_caught(self):
        from boomi.net.transport.api_error import ApiError
        sdk = MagicMock()
        sdk.custom_tracked_field.query_custom_tracked_field.side_effect = ApiError(
            "test error", 400, "bad request"
        )
        result = monitor_platform_action(sdk, "dev", "custom_tracked_fields")
        assert result["_success"] is False
        assert "ApiError" in result["exception_type"]
