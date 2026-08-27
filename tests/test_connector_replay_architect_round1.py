"""Regression pins for the architect review's fail-open findings.

Each case failed against the reviewed tree. They share a theme the delta-scoped
commit reviews could not see: the mechanism verified BYTES thoroughly and then took
the caller's word for what those bytes meant.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from boomi_mcp.connector_replay.capture import summarize
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
    assert rows[0].retry_safety is RetrySafetyV1.IDEMPOTENT


def test_an_unmapped_family_gets_no_affirmative_verdict():
    """A row alone is not enough: the family must resolve through the vocabulary.

    Otherwise an evidence row for a family the registry cannot map onto any live
    connector still returned an affirmative verdict — the one answer this registry
    exists to withhold.
    """
    row = CapabilityEvidenceRecordV1(
        family="unmapped", action="DELETE", side_effect=SideEffectV1.READ,
        retry_safety=RetrySafetyV1.IDEMPOTENT, capture_digest="a" * 64,
        execution_ids=(_REAL,),
    )
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
    invented = CapabilityEvidenceRecordV1(
        family="rest", action="BREW_COFFEE", side_effect=SideEffectV1.READ,
        retry_safety=RetrySafetyV1.IDEMPOTENT, capture_digest="a" * 64,
        execution_ids=(_REAL,),
    )
    assert ReplayRegistry(vocabulary, (invented,)).retry_safety(
        "rest", "BREW_COFFEE") is RetrySafetyV1.UNVERIFIED

    # The control: a RECOGNISED action still resolves, or the fix is just a denial.
    real = invented.model_copy(update={"action": "HEAD"})
    assert ReplayRegistry(vocabulary, (real,)).retry_safety(
        "rest", "HEAD") is RetrySafetyV1.IDEMPOTENT


def test_the_banked_patch_captures_can_still_be_ingested():
    """Slice F depends on these; a reconciliation that blocks them is a regression.

    An earlier version selected the operation component by FILENAME, and the PATCH
    captures are named `component_op_patch.xml` — so their method read as absent and
    reconciliation refused the very evidence the next slice needs.
    """
    rows = ingest(_ROOT, [_C / "cap155-e3b-patch-canonical"], family="rest",
                  actions={"cap155-e3b-patch-canonical": "PATCH"})
    assert (rows[0].family, rows[0].action) == ("rest", "PATCH")
