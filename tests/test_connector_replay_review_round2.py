"""Regression pins for the correction re-review.

Every case here binds evidence to the WRONG thing, or manufactures evidence from a
failure. They are the same defect wearing different clothes: reading the first thing
that loosely matches, instead of the thing correlated to what was actually exercised.
"""

from __future__ import annotations

import json
import shutil
import tempfile
from pathlib import Path

import pytest

from boomi_mcp.connector_replay.capture import CaptureRefused, summarize
from boomi_mcp.connector_replay.ingest import IngestRefused, ingest

_REPO = Path(__file__).resolve().parents[1]
_ROOT = _REPO / "docs" / "architecture" / "evidence" / "issue-155"
_C = _ROOT / "captures"


def test_the_verdict_names_the_operation_that_was_exercised():
    """A capture holds a source operation AND the target under test.

    `component_op_src.xml` sorts first, so returning the first match attached the
    conditionally-idempotent verdict to the GET source. A verdict naming the wrong
    operation is worse than none: it authorises replaying something never replayed.
    """
    row = ingest(_ROOT, [_C / "cap155-e5-patch-attested"], family="rest",
                 actions={"cap155-e5-patch-attested": "PATCH"})[0]
    target = _C / "cap155-e5-patch-attested" / "component_op_tgt.xml"
    source = _C / "cap155-e5-patch-attested" / "component_op_src.xml"
    assert row.operation_component_id in target.read_text()
    assert row.operation_component_id not in source.read_text()


def test_placement_comes_from_the_connector_under_test():
    """The archived captures put a `nodata` sentinel first, and it IS the start shape.

    Reading whichever flag came first therefore reported entry placement for a
    connector that runs downstream — and the record order differs between the two
    executions, so the answer was not even stable.
    """
    for name, action in (("cap155-e5-patch-attested", "PATCH"),
                         ("cap155-e5-delete-attested", "DELETE")):
        assert summarize(_C / name, action).is_start_shape is False


def test_a_failed_readback_is_not_an_absent_resource():
    """404 says the resource is gone; 500 says the observation failed.

    Conflating them let a 200 followed by two 500s read as a clean first effect
    with no replay effect — an affirmative verdict built on two failed observations.
    """
    with tempfile.TemporaryDirectory() as tmp:
        d = Path(tmp) / "cap"
        shutil.copytree(_C / "cap155-e5-delete-attested", d)
        for stage in ("R1_between", "R2_after"):
            f = d / f"readback_{stage}_target.json"
            payload = json.loads(f.read_text())
            payload["status"] = 500
            f.write_text(json.dumps(payload))
        with pytest.raises(CaptureRefused) as err:
            summarize(d, "DELETE")
        assert "neither observes" in str(err.value)


def test_surplus_log_lines_drop_attribution_too():
    """Only a shortfall used to drop it; a surplus left one status copied to all runs.

    A third unrelated request of the same method could therefore turn a refused
    replay into an attested success.
    """
    with tempfile.TemporaryDirectory() as tmp:
        d = Path(tmp) / "cap"
        shutil.copytree(_C / "cap155-e5-delete-attested", d)
        log = d / "mock_access_log.txt"
        extra = next(l for l in log.read_text().splitlines() if '"DELETE ' in l)
        log.write_text(log.read_text() + "\n" + extra + "\n")
        summary = summarize(d, "DELETE")
        assert {r.counterparty_status for r in summary.runs} == {None}


def test_a_capture_without_an_account_is_refused():
    """Hashing the empty string gave every accountless capture the SAME scope.

    Evidence from two different unknown accounts then satisfied the
    account-consistency check that the hash exists to enforce.
    """
    from boomi_mcp.connector_replay.ingest import _account_scope_hash

    summary = summarize(_C / "cap155-e5-patch-attested", "PATCH")
    assert summary.account_id, "the real capture must carry an account, or this is vacuous"
    assert _account_scope_hash(summary)

    stripped = summary.model_copy(update={"account_id": None})
    with pytest.raises(IngestRefused) as err:
        _account_scope_hash(stripped)
    assert "account" in str(err.value)

    # And the empty string must not slip through as "an account".
    with pytest.raises(IngestRefused):
        _account_scope_hash(summary.model_copy(update={"account_id": ""}))


def test_the_effect_follows_what_the_replay_did():
    """`state_unchanged_after_replay` beside `duplicate_effect` is a contradiction.

    Labelling any write with a convergence tuple as unchanged-after-replay emitted
    exactly that pair for a non-idempotent double execution.
    """
    row = ingest(_ROOT, [_C / "cap155-e5-patch-attested"], family="rest",
                 actions={"cap155-e5-patch-attested": "PATCH"})[0]
    summary = row.capture.summary
    if summary.replay.value == "duplicate_effect":
        assert summary.effect.value != "state_unchanged_after_replay"
    else:
        assert summary.effect.value == "state_unchanged_after_replay"
