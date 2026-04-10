"""Regression tests for BUG-34: fetch_logs must be normalized via _parse_bool().

When fetch_logs is the string "false", it must be treated as False — not as a
truthy string.  The hint should appear when fetch_logs is false (or "false"),
and process_log should only be fetched when fetch_logs is true (or "true").
"""

import os
import sys
from unittest.mock import MagicMock, patch
from pathlib import Path

_project_root = str(Path(__file__).resolve().parent.parent)
if _project_root not in sys.path:
    sys.path.insert(0, _project_root)
_src_dir = str(Path(__file__).resolve().parent.parent / "src")
if _src_dir not in sys.path:
    sys.path.insert(0, _src_dir)

os.environ["BOOMI_LOCAL"] = "true"

import server  # noqa: E402 — bootstraps the app
from boomi_mcp.categories.troubleshooting import handle_error_details

HINT_TEXT = "Use fetch_logs=true in config to download process logs for detailed stack traces"


def _make_sdk_with_record():
    sdk = MagicMock()
    record = MagicMock()
    record.execution_id = "exec-1"
    record.status = "ERROR"
    record.process_name = "TestProcess"
    record.process_id = "proc-1"
    record.atom_name = "TestAtom"
    record.atom_id = "atom-1"
    record.execution_time = None
    record.execution_duration = None
    record.message = "Something failed"
    record.inbound_document_count = 1
    record.outbound_document_count = 0
    record.inbound_error_document_count = 1
    return sdk, record


# ---------------------------------------------------------------------------
# fetch_logs=false (boolean) — hint present, no log fetch
# ---------------------------------------------------------------------------

@patch("boomi_mcp.categories.troubleshooting._fetch_process_log")
@patch("boomi_mcp.categories.troubleshooting._query_execution_record")
def test_fetch_logs_false_bool_shows_hint(mock_query, mock_fetch_log):
    sdk, record = _make_sdk_with_record()
    mock_query.return_value = record

    result = handle_error_details(sdk, execution_id="exec-1", config={"fetch_logs": False})

    assert result["_success"] is True
    assert HINT_TEXT in result["error_analysis"]["troubleshooting_tips"]
    mock_fetch_log.assert_not_called()


# ---------------------------------------------------------------------------
# fetch_logs="false" (string) — must behave identically to False
# ---------------------------------------------------------------------------

@patch("boomi_mcp.categories.troubleshooting._fetch_process_log")
@patch("boomi_mcp.categories.troubleshooting._query_execution_record")
def test_fetch_logs_string_false_shows_hint(mock_query, mock_fetch_log):
    """Regression: string 'false' was truthy, skipping hint and fetching logs."""
    sdk, record = _make_sdk_with_record()
    mock_query.return_value = record

    result = handle_error_details(sdk, execution_id="exec-1", config={"fetch_logs": "false"})

    assert result["_success"] is True
    assert HINT_TEXT in result["error_analysis"]["troubleshooting_tips"]
    mock_fetch_log.assert_not_called()


# ---------------------------------------------------------------------------
# fetch_logs=true (boolean) — no hint, log fetched
# ---------------------------------------------------------------------------

@patch("boomi_mcp.categories.troubleshooting._fetch_process_log", return_value={"log": "data"})
@patch("boomi_mcp.categories.troubleshooting._query_execution_record")
def test_fetch_logs_true_bool_fetches_logs(mock_query, mock_fetch_log):
    sdk, record = _make_sdk_with_record()
    mock_query.return_value = record

    result = handle_error_details(sdk, execution_id="exec-1", config={"fetch_logs": True})

    assert result["_success"] is True
    assert HINT_TEXT not in result["error_analysis"]["troubleshooting_tips"]
    assert "process_log" in result
    mock_fetch_log.assert_called_once()


# ---------------------------------------------------------------------------
# fetch_logs="true" (string) — must behave identically to True
# ---------------------------------------------------------------------------

@patch("boomi_mcp.categories.troubleshooting._fetch_process_log", return_value={"log": "data"})
@patch("boomi_mcp.categories.troubleshooting._query_execution_record")
def test_fetch_logs_string_true_fetches_logs(mock_query, mock_fetch_log):
    sdk, record = _make_sdk_with_record()
    mock_query.return_value = record

    result = handle_error_details(sdk, execution_id="exec-1", config={"fetch_logs": "true"})

    assert result["_success"] is True
    assert HINT_TEXT not in result["error_analysis"]["troubleshooting_tips"]
    assert "process_log" in result
    mock_fetch_log.assert_called_once()


# ---------------------------------------------------------------------------
# fetch_logs omitted — default False, hint present
# ---------------------------------------------------------------------------

@patch("boomi_mcp.categories.troubleshooting._fetch_process_log")
@patch("boomi_mcp.categories.troubleshooting._query_execution_record")
def test_fetch_logs_omitted_shows_hint(mock_query, mock_fetch_log):
    sdk, record = _make_sdk_with_record()
    mock_query.return_value = record

    result = handle_error_details(sdk, execution_id="exec-1", config={})

    assert result["_success"] is True
    assert HINT_TEXT in result["error_analysis"]["troubleshooting_tips"]
    mock_fetch_log.assert_not_called()
