"""Ingest: verified bytes in, conservative rows out."""

from __future__ import annotations

import shutil
import tempfile
from pathlib import Path

import pytest

from boomi_mcp.connector_replay.capture import summarize
from boomi_mcp.connector_replay.ingest import (
    IngestRefused,
    classify,
    ingest,
    verify_archive,
)
from boomi_mcp.connector_replay.models import RetrySafetyV1, SideEffectV1

_REPO = Path(__file__).resolve().parents[1]
_ROOT = _REPO / "docs" / "architecture" / "evidence" / "issue-155"
_C = _ROOT / "captures"

_ACTIONS = {
    "cap155-e4-head-status": "HEAD",
    "cap155-e4-options-status": "OPTIONS",
    "cap155-e4-trace-status": "TRACE",
    "cap155-e4-negative-control": "DELETE",
}


def test_archived_captures_verify_against_the_manifest():
    for name in _ACTIONS:
        verify_archive(_ROOT, _C / name)


def test_a_tampered_capture_is_refused():
    """The whole point of verifying: a changed byte must not ingest."""
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp) / "archive"
        shutil.copytree(_ROOT, root)
        victim = root / "captures" / "cap155-e4-head-status" / "mock_access_log.txt"
        victim.write_text(victim.read_text().replace(" 200 OK", " 999 NOPE"))
        with pytest.raises(IngestRefused) as err:
            verify_archive(root, root / "captures" / "cap155-e4-head-status")
        assert "digest mismatch" in str(err.value)


def test_an_extra_file_is_refused_rather_than_ignored():
    """An unlisted file is a capture nobody archived."""
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp) / "archive"
        shutil.copytree(_ROOT, root)
        (root / "captures" / "cap155-e4-head-status" / "planted.txt").write_text("x")
        with pytest.raises(IngestRefused) as err:
            verify_archive(root, root / "captures" / "cap155-e4-head-status")
        assert "unlisted" in str(err.value)


def test_a_missing_manifest_is_refused():
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        (root / "captures" / "x").mkdir(parents=True)
        with pytest.raises(IngestRefused):
            verify_archive(root, root / "captures" / "x")


def test_the_read_verbs_ingest_as_idempotent_reads():
    rows = {r.action: r for r in ingest(
        _ROOT, [_C / n for n in _ACTIONS if n != "cap155-e4-negative-control"],
        family="rest", actions=_ACTIONS,
    )}
    assert set(rows) == {"HEAD", "OPTIONS", "TRACE"}
    for row in rows.values():
        assert row.side_effect is SideEffectV1.READ
        assert row.retry_safety is RetrySafetyV1.IDEMPOTENT
        assert row.execution_ids


def test_a_refused_request_does_not_become_a_safe_read():
    """The classifier's most important property.

    The negative control's readback is unchanged and its execution reads COMPLETE
    with zero inbound errors — identical, on every platform-visible field, to the
    three genuine successes beside it. Only the counterparty's 405 distinguishes
    it. A classifier reading platform fields would call a REFUSED DELETE an
    idempotent read, which is precisely the wrong direction to be wrong in.
    """
    summary = summarize(_C / "cap155-e4-negative-control", "DELETE")
    assert {r.status for r in summary.runs} == {"COMPLETE"}
    assert {r.state_changed for r in summary.runs} == {False}
    side_effect, retry_safety = classify(summary)
    assert side_effect is SideEffectV1.UNKNOWN
    assert retry_safety is RetrySafetyV1.UNVERIFIED


def test_a_capture_with_no_counterparty_attestation_proves_nothing():
    """Unattested is UNKNOWN, never a default of 'read'."""
    summary = summarize(_C / "cap155-e2-post")
    assert not summary.has_counterparty_attestation
    side_effect, retry_safety = classify(summary)
    assert side_effect is SideEffectV1.UNKNOWN
    assert retry_safety is RetrySafetyV1.UNVERIFIED


def test_an_undeclared_action_is_refused():
    """A directory name is a convention, not evidence of what ran."""
    with pytest.raises(IngestRefused) as err:
        ingest(_ROOT, [_C / "cap155-e4-head-status"], family="rest", actions={})
    assert "not inferred" in str(err.value)


def test_ingest_of_the_whole_read_set_is_deterministic():
    a = ingest(_ROOT, [_C / n for n in _ACTIONS], family="rest", actions=_ACTIONS)
    b = ingest(_ROOT, [_C / n for n in reversed(list(_ACTIONS))], family="rest", actions=_ACTIONS)
    assert a == b, "row order or content depends on input order"
