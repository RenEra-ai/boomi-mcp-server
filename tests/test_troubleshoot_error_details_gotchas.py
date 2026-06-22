"""Tests that error_details attaches gotcha_matches when symptoms route (issue #78).

Exercises handle_error_details directly with a stubbed execution record, covering:
- match via execution message (gotchas_enabled=True) → gotcha_matches present
- match via config.observed_symptoms → gotcha_matches present
- no symptom match → key omitted
- gotchas_enabled=False → key omitted even when a symptom would match
- non-error actions never carry gotcha_matches (routed through the dispatcher)
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
from boomi_mcp.categories.troubleshooting import (  # noqa: E402
    handle_error_details,
    troubleshoot_execution_action,
)


def _make_record(message="Something failed"):
    record = MagicMock()
    record.execution_id = "exec-1"
    record.status = "ERROR"
    record.process_name = "TestProcess"
    record.process_id = "proc-1"
    record.atom_name = "TestAtom"
    record.atom_id = "atom-1"
    record.execution_time = None
    record.execution_duration = None
    record.message = message
    record.inbound_document_count = 1
    record.outbound_document_count = 0
    record.inbound_error_document_count = 1
    return record


# ---------------------------------------------------------------------------
# Match: symptom carried in the execution message
# ---------------------------------------------------------------------------

@patch("boomi_mcp.categories.troubleshooting._query_execution_record")
def test_gotcha_match_from_execution_message(mock_query):
    mock_query.return_value = _make_record(message="HTTP 404 returned from deployed API")
    sdk = MagicMock()

    result = handle_error_details(sdk, execution_id="exec-1", config={}, gotchas_enabled=True)

    assert result["_success"] is True
    assert "gotcha_matches" in result
    ids = [m["id"] for m in result["gotcha_matches"]]
    assert "wss_path_objectname_verbatim" in ids
    match = result["gotcha_matches"][0]
    assert set(match) == {"id", "title", "remediation", "lookup"}


# ---------------------------------------------------------------------------
# Match: symptom supplied via config.observed_symptoms (string and list)
# ---------------------------------------------------------------------------

@patch("boomi_mcp.categories.troubleshooting._query_execution_record")
def test_gotcha_match_from_observed_symptoms_list(mock_query):
    mock_query.return_value = _make_record(message="generic failure")
    sdk = MagicMock()

    result = handle_error_details(
        sdk, execution_id="exec-1",
        config={"observed_symptoms": ["extension values disappearing after deploy"]},
        gotchas_enabled=True,
    )

    ids = [m["id"] for m in result["gotcha_matches"]]
    assert "empty_process_overrides_hides_extensions" in ids


@patch("boomi_mcp.categories.troubleshooting._query_execution_record")
def test_gotcha_match_from_observed_symptoms_string(mock_query):
    mock_query.return_value = _make_record(message="generic failure")
    sdk = MagicMock()

    result = handle_error_details(
        sdk, execution_id="exec-1",
        config={"observed_symptoms": "the variable appears literally in the output"},
        gotchas_enabled=True,
    )

    ids = [m["id"] for m in result["gotcha_matches"]]
    assert "env_var_literal_in_component_xml" in ids


# ---------------------------------------------------------------------------
# No match → key omitted cleanly
# ---------------------------------------------------------------------------

@patch("boomi_mcp.categories.troubleshooting._query_execution_record")
def test_no_gotcha_match_omits_key(mock_query):
    mock_query.return_value = _make_record(message="generic failure with no routed symptom")
    sdk = MagicMock()

    result = handle_error_details(sdk, execution_id="exec-1", config={}, gotchas_enabled=True)

    assert result["_success"] is True
    assert "gotcha_matches" not in result


# ---------------------------------------------------------------------------
# Disabled → key omitted even when a symptom would match
# ---------------------------------------------------------------------------

@patch("boomi_mcp.categories.troubleshooting._query_execution_record")
def test_gotchas_disabled_omits_key(mock_query):
    mock_query.return_value = _make_record(message="HTTP 404 returned from deployed API")
    sdk = MagicMock()

    # Default gotchas_enabled=False
    result = handle_error_details(sdk, execution_id="exec-1", config={})

    assert result["_success"] is True
    assert "gotcha_matches" not in result


# ---------------------------------------------------------------------------
# matches survive alongside process_log when fetch_logs=true
# ---------------------------------------------------------------------------

@patch("boomi_mcp.categories.troubleshooting._fetch_process_log", return_value={"log": "data"})
@patch("boomi_mcp.categories.troubleshooting._query_execution_record")
def test_gotcha_matches_coexist_with_fetch_logs(mock_query, mock_fetch_log):
    mock_query.return_value = _make_record(message="HTTP 404 returned from deployed API")
    sdk = MagicMock()

    result = handle_error_details(
        sdk, execution_id="exec-1", config={"fetch_logs": True}, gotchas_enabled=True
    )

    assert "process_log" in result
    assert "gotcha_matches" in result


# ---------------------------------------------------------------------------
# Non-error actions never carry gotcha_matches (dispatcher level)
# ---------------------------------------------------------------------------

@patch("boomi_mcp.categories.troubleshooting._create_execution_request")
@patch("boomi_mcp.categories.troubleshooting._query_execution_record")
def test_retry_action_never_carries_gotcha_matches(mock_query, mock_create):
    mock_query.return_value = _make_record(message="HTTP 404 returned from deployed API")
    mock_create.return_value = {"_success": True, "context": "retry"}
    sdk = MagicMock()

    result = troubleshoot_execution_action(
        sdk, "retry", execution_id="exec-1", config={}, gotchas_enabled=True
    )

    assert "gotcha_matches" not in result
