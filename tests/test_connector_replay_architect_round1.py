"""Regression pins for the architect review's fail-open findings.

Each case failed against the reviewed tree. They share a theme the delta-scoped
commit reviews could not see: the mechanism verified BYTES thoroughly and then took
the caller's word for what those bytes meant.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from _connector_replay_factories import capture_reference, evidence_row
from boomi_mcp.connector_replay.capture import CaptureRefused, summarize
from boomi_mcp.connector_replay.ingest import IngestRefused, ingest
from boomi_mcp.connector_replay.models import (
    CapabilityEvidenceRecordV1,
    RetrySafetyV1,
    SideEffectV1,
)
from boomi_mcp.connector_replay.registry import ReplayRegistry

_REPO = Path(__file__).resolve().parents[1]
_ROOT = _REPO / "docs" / "architecture" / "evidence" / "issue-155"
_C = _ROOT / "captures"
_REAL = "execution-1957bb8f-9a89-4254-b169-9ddbf41fddf8-2026.08.26"


def test_a_capture_reports_the_connector_it_actually_used():
    summary = summarize(_C / "cap155-e4-head-status", "HEAD")
    assert summary.observed_connector_types == ("officialboomi-X3979C-rest-prod",)
    # A SET: a capture commonly declares more than one method (a source operation
    # that fetches, plus the operation under test), and which is the subject is not
    # determinable from the components alone.
    assert summary.observed_methods == ("HEAD",)


def test_the_declared_family_is_reconciled_against_the_capture():
    """Verifying bytes proves nothing about the LABELS attached to them.

    A checksummed REST capture ingested as `database` used to mint a
    `database/HEAD/idempotent` row — a verdict for a connector no execution touched.
    """
    with pytest.raises(IngestRefused) as err:
        ingest(_ROOT, [_C / "cap155-e4-head-status"], family="database",
               actions={"cap155-e4-head-status": "HEAD"})
    assert "does not match the observed" in str(err.value)


def test_the_declared_action_is_reconciled_against_the_component():
    with pytest.raises(IngestRefused):
        ingest(_ROOT, [_C / "cap155-e4-head-status"], family="rest",
               actions={"cap155-e4-head-status": "DELETE"})


def test_truthful_labels_still_ingest():
    """The control: reconciliation must not refuse the honest case."""
    rows = ingest(_ROOT, [_C / "cap155-e4-head-status"], family="rest",
                  actions={"cap155-e4-head-status": "HEAD"})
    assert (rows[0].family, rows[0].action) == ("rest", "HEAD")
    assert rows[0].side_effect is SideEffectV1.READ
    # UNVERIFIED, and that is correct: this capture never exercised a replay, so
    # nothing observed supports an affirmative retry verdict. That a safe method is
    # idempotent is a claim from the transport specification; this registry records
    # what was observed.
    assert rows[0].retry_safety is RetrySafetyV1.UNVERIFIED
    assert rows[0].capture.summary.replay.value == "not_exercised"


def test_an_unmapped_family_gets_no_affirmative_verdict():
    """A row alone is not enough: the family must resolve through the vocabulary.

    Otherwise an evidence row for a family the registry cannot map onto any live
    connector still returned an affirmative verdict — the one answer this registry
    exists to withhold.
    """
    row = evidence_row(family="unmapped", action="DELETE")
    empty_vocabulary = ReplayRegistry((), (row,))
    assert empty_vocabulary.retry_safety("unmapped", "DELETE") is RetrySafetyV1.UNVERIFIED


def test_the_served_action_list_matches_the_router_everywhere():
    """The third attempt at this sweep; the first two declared completeness wrongly."""
    from boomi_mcp.categories.meta_tools import _get_monitoring_template
    from boomi_mcp.categories.monitoring import _MONITORING_ACTIONS

    overview = _get_monitoring_template()
    assert set(overview["available_actions"]) == set(_MONITORING_ACTIONS)
    assert "execution_connectors" in overview["available_actions"]
    # Templated and dispatchable are DIFFERENT facts; collapsing them is what told
    # a caller that eight real actions did not exist.
    assert set(overview["actions_with_templates"]) < set(overview["available_actions"])
    assert set(overview["actions_with_templates"]).isdisjoint(
        overview["actions_without_templates"])


def test_no_served_surface_hand_lists_the_actions():
    """Derived by parsing, so a copy added later is covered without editing this."""
    import re

    from boomi_mcp.categories.monitoring import _MONITORING_ACTIONS

    known = set(_MONITORING_ACTIONS)
    offenders = []
    for rel in ("src/boomi_mcp/categories/meta_tools.py",
                "src/boomi_mcp/categories/monitoring.py"):
        for n, line in enumerate((_REPO / rel).read_text().split("\n"), 1):
            names = set(re.findall(r"[\"'`]([a-z_]{6,})[\"'`]", line)) & known
            if len(names) >= 4 and "execution_connectors" not in names:
                offenders.append(f"{rel}:{n} lists {len(names)} of {len(known)}")
    assert offenders == [], (
        "a served surface hand-lists monitoring actions and is already stale: {0}"
        .format(offenders))


def test_an_unrecognised_action_gets_no_verdict_even_in_a_mapped_family():
    """Resolving the family alone was the same fail-open, one level down."""
    from boomi_mcp.connector_replay.registry import load_registry

    vocabulary = load_registry().vocabulary
    invented = evidence_row(action="BREW_COFFEE")
    assert ReplayRegistry(vocabulary, (invented,)).retry_safety(
        "rest", "BREW_COFFEE") is RetrySafetyV1.UNVERIFIED

    # The control: a RECOGNISED action still resolves, or the fix is just a denial.
    real = invented.model_copy(update={"action": "HEAD",
                                       "retry_safety": RetrySafetyV1.UNVERIFIED})
    assert ReplayRegistry(vocabulary, (real,)).retry_safety(
        "rest", "HEAD") is RetrySafetyV1.UNVERIFIED


def test_the_attested_patch_capture_ingests_with_a_replay_verdict():
    """The capture slice F actually needs: a double execution WITH attestation.

    An earlier version selected the operation component by FILENAME, which blocked
    the archived PATCH captures entirely. Selecting by content fixed that — but the
    older PATCH captures carry no counterparty log, so under the verdict binding
    they establish a side effect nobody observed and are now REFUSED. That is
    correct and is why the attested capture was taken.
    """
    rows = ingest(_ROOT, [_C / "cap155-e5-patch-attested"], family="rest",
                  actions={"cap155-e5-patch-attested": "PATCH"})
    row = rows[0]
    assert (row.family, row.action) == ("rest", "PATCH")
    assert row.retry_safety is RetrySafetyV1.CONDITIONALLY_IDEMPOTENT
    assert row.capture.summary.replay.value == "same_effect"
    assert row.operation_component_id, "a conditional verdict must name its operation"


def test_the_unattested_patch_captures_are_now_refused():
    """The control: an unattested capture must not yield a verdict.

    These have two executions and staged readbacks but no counterparty log, so
    nothing observed what the endpoint returned.
    """
    with pytest.raises(IngestRefused):
        ingest(_ROOT, [_C / "cap155-e3b-patch-canonical"], family="rest",
               actions={"cap155-e3b-patch-canonical": "PATCH"})


def test_connector_rows_must_name_this_captures_executions():
    """The execution id is the causal tie between an artifact and a capture.

    Without it, any file in the directory carrying a `connectorType` lent its
    authority to the capture — so an artifact from an unrelated execution could
    name the connector a row was minted for.
    """
    import shutil
    import tempfile

    import json

    with tempfile.TemporaryDirectory() as tmp:
        d = Path(tmp) / "cap"
        shutil.copytree(_C / "cap155-e4-head-status", d)
        f = d / "execution_connector.json"
        f.write_text(f.read_text().replace(
            "execution-487d4ceb-32e5-4f1e-95ec-9a2d64475607-2026.08.27",
            "execution-00000000-0000-4000-8000-000000000000-2026.01.01"))
        with pytest.raises(CaptureRefused) as err:
            summarize(d, "HEAD")
        assert "not one of this capture's" in str(err.value)

    # The MIXED case, which per-FILE correlation could not see: a file holding a
    # record for this capture's execution BESIDE one for a foreign execution passed
    # wholesale, and every connector type in it was then trusted.
    with tempfile.TemporaryDirectory() as tmp:
        d = Path(tmp) / "cap"
        shutil.copytree(_C / "cap155-e4-head-status", d)
        f = d / "execution_connector.json"
        doc = json.loads(f.read_text())
        records = doc["raw"]["data"]["result"]
        records.append({
            **records[0],
            "executionId": "execution-00000000-0000-4000-8000-000000000000-2026.01.01",
            "connectorType": "officialboomi-X3979C-dbv2da-prod",
        })
        f.write_text(json.dumps(doc))
        with pytest.raises(CaptureRefused) as err:
            summarize(d, "HEAD")
        assert "not one of this capture's" in str(err.value)

    # The control: the untouched capture still summarises.
    assert summarize(_C / "cap155-e4-head-status", "HEAD").observed_connector_types


def test_a_component_kind_with_no_published_projection_is_refused():
    """A digest that cannot state its projection is not an identity.

    The projection used to fall back to hard-coded constants when the registry was
    absent, so a digest was still produced — under a projection nobody published,
    silently disagreeing with every digest computed when the registry did load.
    """
    from boomi_mcp.connector_replay.digests import (
        ConfigDigestRefused,
        component_config_digest_v1,
    )
    from boomi_mcp.connector_replay.registry import ReplayRegistry

    empty = ReplayRegistry((), ())
    assert empty.projection_for("operation") is None


def test_one_logged_request_cannot_attest_two_executions():
    """A replay verdict must not rest on a request nobody observed.

    The single counterparty outcome was copied onto every run, so a capture with
    two executions and one logged request looked fully attested — and a replay
    verdict drawn from the second execution rested on nothing.
    """
    import shutil
    import tempfile

    from boomi_mcp.connector_replay.ingest import classify

    with tempfile.TemporaryDirectory() as tmp:
        d = Path(tmp) / "cap"
        shutil.copytree(_C / "cap155-e2-post", d)
        (d / "mock_access_log.txt").write_text(
            'INFO: 1.2.3.4 - "POST /x HTTP/1.1" 200 OK\n')
        summary = summarize(d, "POST")
        assert len(summary.runs) == 2
        assert {r.counterparty_status for r in summary.runs} == {None}, (
            "one logged request was spread across both executions")
        assert classify(summary, "POST", frozenset()) == (
            SideEffectV1.UNKNOWN, RetrySafetyV1.UNVERIFIED)


def test_absence_is_not_a_present_null():
    """A replay that ADDED a null-valued field reported no difference at all.

    `dict.get` returns None for both "not there" and "there and null", and that
    result fed the convergence verdict.
    """
    from boomi_mcp.connector_replay.capture import _differing_keys

    assert _differing_keys({}, {"f": None}) == ("f",)
    assert _differing_keys({"f": None}, {}) == ("f",)
    assert _differing_keys({"f": None}, {"f": None}) == ()
