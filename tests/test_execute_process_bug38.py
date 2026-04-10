"""Regression tests for BUG-38: cloud auto-detachment hint.

_check_cloud_detachment must extract runtime_id (not atom_id/id_) from
AccountCloudAttachmentSummary objects so that execute_process_action returns
a useful hint when the environment has no attached runtime but cloud runtimes
exist in the account.
"""

import os
import sys
from unittest.mock import MagicMock, patch
from pathlib import Path

_project_root = str(Path(__file__).resolve().parent.parent)
if _project_root not in sys.path:
    sys.path.insert(0, _project_root)

os.environ["BOOMI_LOCAL"] = "true"

import server  # noqa: E402
from boomi_mcp.categories.execution import (
    _check_cloud_detachment,
    execute_process_action,
)


# ---------------------------------------------------------------------------
# _check_cloud_detachment unit tests
# ---------------------------------------------------------------------------

def _make_cloud_summary(runtime_id=None):
    """Return a mock matching the real AccountCloudAttachmentSummary shape."""
    summary = MagicMock()
    summary.runtime_id = runtime_id
    # Ensure the old wrong fields are NOT present (match real SDK model)
    del summary.atom_id
    del summary.id_
    return summary


def test_check_cloud_detachment_extracts_runtime_id():
    """runtime_id is extracted correctly from cloud attachment summaries."""
    sdk = MagicMock()
    query_result = MagicMock()
    query_result.result = [
        _make_cloud_summary(runtime_id="rt-aaa"),
        _make_cloud_summary(runtime_id="rt-bbb"),
    ]
    sdk.account_cloud_attachment_summary.query_account_cloud_attachment_summary.return_value = query_result

    hint = _check_cloud_detachment(sdk, "env-123")

    assert hint is not None
    assert "cloud test runtimes" in hint.lower()
    # Hint must NOT embed specific runtime_ids (account-wide results are not
    # correlated to the target environment, so suggesting a specific one is
    # misleading).  Instead it should direct the user to list runtimes.
    assert "rt-aaa" not in hint
    assert "rt-bbb" not in hint
    assert "manage_runtimes(action='list')" in hint


def test_check_cloud_detachment_no_results():
    """Returns None when no cloud attachment summaries exist."""
    sdk = MagicMock()
    query_result = MagicMock()
    query_result.result = []
    sdk.account_cloud_attachment_summary.query_account_cloud_attachment_summary.return_value = query_result

    hint = _check_cloud_detachment(sdk, "env-123")
    assert hint is None


def test_check_cloud_detachment_none_runtime_ids_skipped():
    """Summaries with None runtime_id are ignored."""
    sdk = MagicMock()
    query_result = MagicMock()
    query_result.result = [
        _make_cloud_summary(runtime_id=None),
        _make_cloud_summary(runtime_id="rt-only"),
    ]
    sdk.account_cloud_attachment_summary.query_account_cloud_attachment_summary.return_value = query_result

    hint = _check_cloud_detachment(sdk, "env-123")

    assert hint is not None
    assert "manage_runtimes(action='list')" in hint


def test_check_cloud_detachment_sdk_error_returns_none():
    """SDK errors are swallowed — the hint is best-effort."""
    sdk = MagicMock()
    sdk.account_cloud_attachment_summary.query_account_cloud_attachment_summary.side_effect = Exception("boom")

    hint = _check_cloud_detachment(sdk, "env-123")
    assert hint is None


# ---------------------------------------------------------------------------
# Integration: execute_process_action surfaces the hint
# ---------------------------------------------------------------------------

@patch("boomi_mcp.categories.execution._check_cloud_detachment",
       return_value="Your account has cloud test runtimes that auto-detach after execution.")
@patch("boomi_mcp.categories.execution._resolve_atom_id",
       return_value=(None, "No runtime attached to environment 'env-123'. Attach a runtime first using manage_runtimes(action='attach')."))
def test_execute_process_surfaces_hint_on_no_runtime(mock_resolve, mock_cloud):
    """execute_process_action includes a hint key when cloud detachment is detected."""
    sdk = MagicMock()
    result = execute_process_action(
        sdk, profile="dev", process_id="proc-1",
        environment_id="env-123", atom_id=None,
        config_data={},
    )

    assert result["_success"] is False
    assert "No runtime attached" in result["error"]
    assert "hint" in result
    assert "cloud test runtimes" in result["hint"].lower()


@patch("boomi_mcp.categories.execution._check_cloud_detachment", return_value=None)
@patch("boomi_mcp.categories.execution._resolve_atom_id",
       return_value=(None, "No runtime attached to environment 'env-123'. Attach a runtime first using manage_runtimes(action='attach')."))
def test_execute_process_no_hint_when_no_cloud_atoms(mock_resolve, mock_cloud):
    """No hint key when _check_cloud_detachment returns None."""
    sdk = MagicMock()
    result = execute_process_action(
        sdk, profile="dev", process_id="proc-1",
        environment_id="env-123", atom_id=None,
        config_data={},
    )

    assert result["_success"] is False
    assert "hint" not in result


# ---------------------------------------------------------------------------
# False-positive guard: hint must not embed unrelated runtime_ids
# ---------------------------------------------------------------------------

def test_check_cloud_detachment_does_not_embed_runtime_ids():
    """Hint must not suggest a specific runtime_id from account-wide results.

    Cloud attachment summaries are account-wide, so the runtime_ids found
    may belong to environments unrelated to the one that triggered the error.
    Embedding a specific runtime_id in a 're-attach' command is misleading
    for on-prem environments that happen to share an account with cloud runtimes.
    """
    sdk = MagicMock()
    query_result = MagicMock()
    query_result.result = [
        _make_cloud_summary(runtime_id="rt-unrelated"),
    ]
    sdk.account_cloud_attachment_summary.query_account_cloud_attachment_summary.return_value = query_result

    hint = _check_cloud_detachment(sdk, "env-onprem")

    assert hint is not None
    # Must NOT contain the specific runtime_id from an unrelated environment
    assert "rt-unrelated" not in hint
    # Must direct user to discover runtimes themselves
    assert "manage_runtimes(action='list')" in hint
    # Must include the target environment_id for context
    assert "env-onprem" in hint
