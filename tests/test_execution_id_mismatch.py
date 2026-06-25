"""Regression tests for the request_id vs execution_id mismatch (Bug #19/#51).

execute_process surfaces the Boomi async tracking id as top-level request_id
(format 'executionrecord-<uuid>'). That id is rejected by the ProcessLog/
ExecutionArtifacts/ConnectorDocument platform APIs, which require the distinct
execution_id (form 'execution-<uuid>-<date>', surfaced in execution_result).

These tests pin the additive guardrail: the log/artifact/document handlers
short-circuit with a structured, actionable failure when an 'executionrecord-'
prefix is supplied, while valid 'execution-...' ids still flow through unchanged.
They also pin the clarifying note execute_process adds on the non-wait path.
"""

import os
import sys
from unittest.mock import MagicMock, patch
from pathlib import Path

_project_root = str(Path(__file__).resolve().parent.parent)
if _project_root not in sys.path:
    sys.path.insert(0, _project_root)
_src = str(Path(__file__).resolve().parent.parent / "src")
if _src not in sys.path:
    sys.path.insert(0, _src)

os.environ["BOOMI_LOCAL"] = "true"

import server  # noqa: E402,F401
from boomi_mcp.categories.monitoring import (  # noqa: E402
    handle_execution_logs,
    handle_execution_artifacts,
    handle_connector_documents,
)
from boomi_mcp.categories.troubleshooting import (  # noqa: E402
    _fetch_process_log,
    handle_error_details,
)
from boomi_mcp.categories.execution import execute_process_action  # noqa: E402


_REQUEST_ID = "executionrecord-8e811200-fb2f-44f0-a6d9-ca4211b67c0c"
_EXECUTION_ID = "execution-8e811200-fb2f-44f0-a6d9-ca4211b67c0c-2026.06.06"


_EXPECTED_ERROR = (
    "execution_id looks like a request_id (executionrecord- prefix). "
    "Use the execution_id from execute_process execution_result.execution_id instead."
)
_EXPECTED_HINT = (
    "execute_process returns two ids: request_id (executionrecord-..., for polling only) "
    "and execution_id (inside execution_result.execution_id, for logs/artifacts). "
    "Pass execution_result.execution_id here."
)


def _assert_request_id_rejection(result):
    """Common assertions for the structured request_id-as-execution_id failure.

    Pins the full user-facing contract (exact error AND hint) — the hint literal is
    duplicated across the monitoring.py and troubleshooting.py helpers, so exact equality
    also guards against the two copies drifting apart.
    """
    assert result["_success"] is False
    assert result["error"] == _EXPECTED_ERROR
    assert result["hint"] == _EXPECTED_HINT


# ---------------------------------------------------------------------------
# Rejection paths — a request_id must never reach the platform
# ---------------------------------------------------------------------------

def test_execution_logs_rejects_request_id():
    client = MagicMock()
    result = handle_execution_logs(client, {"execution_id": _REQUEST_ID})
    _assert_request_id_rejection(result)
    # Guard short-circuits before any SDK call
    client.process_log.create_process_log.assert_not_called()


def test_execution_artifacts_rejects_request_id():
    client = MagicMock()
    result = handle_execution_artifacts(client, {"execution_id": _REQUEST_ID})
    _assert_request_id_rejection(result)
    client.execution_artifacts.create_execution_artifacts.assert_not_called()


def test_fetch_process_log_rejects_request_id():
    sdk = MagicMock()
    result = _fetch_process_log(sdk, _REQUEST_ID)
    _assert_request_id_rejection(result)
    sdk.process_log.create_process_log.assert_not_called()


def test_connector_documents_rejects_request_id():
    client = MagicMock()
    result = handle_connector_documents(client, {"execution_id": _REQUEST_ID})
    _assert_request_id_rejection(result)
    client.generic_connector_record.query_generic_connector_record.assert_not_called()


def test_error_details_rejects_request_id_before_server_query():
    """error_details must reject a request_id BEFORE the server-side ExecutionRecord query.

    The guard in _fetch_process_log is too late for this path: handle_error_details
    queries the execution record first, which the platform rejects with a raw
    'Unknown execution id format'. The guard must short-circuit at the top.
    """
    sdk = MagicMock()
    result = handle_error_details(sdk, execution_id=_REQUEST_ID, config={"fetch_logs": True})
    _assert_request_id_rejection(result)
    # Never reaches the server-side execution-record query
    sdk.execution_record.query_execution_record.assert_not_called()


# ---------------------------------------------------------------------------
# Golden path — a valid execution_id is NOT rejected by the guard
# ---------------------------------------------------------------------------

def test_execution_logs_accepts_valid_execution_id():
    client = MagicMock()
    sdk_result = MagicMock()
    sdk_result.status_code = 202
    sdk_result.message = ""
    sdk_result.url = "https://platform.boomi.com/download/log.zip"
    client.process_log.create_process_log.return_value = sdk_result

    result = handle_execution_logs(
        client,
        {"execution_id": _EXECUTION_ID, "fetch_content": False},
    )

    assert result["_success"] is True
    assert result["download_url"] == "https://platform.boomi.com/download/log.zip"
    # The valid id flowed through to the SDK exactly once, unchanged
    client.process_log.create_process_log.assert_called_once()
    process_log_arg = client.process_log.create_process_log.call_args.kwargs["request_body"]
    assert process_log_arg.execution_id == _EXECUTION_ID


# ---------------------------------------------------------------------------
# execute_process contract clarity — non-wait note warns against reusing request_id
# ---------------------------------------------------------------------------

@patch(
    "boomi_mcp.categories.execution._resolve_execution_id",
    return_value=(None, "Execution still running; execution_id not yet available"),
)
def test_execute_process_nonwait_includes_request_id_note(mock_resolve):
    sdk = MagicMock()
    create_result = MagicMock()
    create_result.request_id = _REQUEST_ID
    sdk.execution_request.create_execution_request.return_value = create_result

    response = execute_process_action(
        sdk,
        profile="dev",
        process_id="proc-1",
        environment_id=None,
        atom_id="atom-1",
        config_data={},  # non-wait
    )

    # request_id stays surfaced (the pollers legitimately consume it)
    assert response["request_id"] == _REQUEST_ID
    # ...but the response now warns it is not valid for the log/artifact endpoints
    assert "note" in response
    note = response["note"]
    assert "execution_logs" in note
    assert "execution_artifacts" in note
    assert "connector_documents" in note
    assert "execution_result.execution_id" in note
