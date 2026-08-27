"""Summarising archived captures, against every capture actually in the archive."""

from __future__ import annotations

from pathlib import Path

import pytest

from boomi_mcp.connector_replay.capture import CaptureRefused, summarize

_REPO = Path(__file__).resolve().parents[1]
_CAPTURES = _REPO / "docs" / "architecture" / "evidence" / "issue-155" / "captures"

#: The verb each read-verb capture is about. Used to attribute a log line when a
#: capture's log carries more than one request.
_HINTS = {
    "cap155-e4-head-status": "HEAD",
    "cap155-e4-options-status": "OPTIONS",
    "cap155-e4-trace-status": "TRACE",
    "cap155-e4-negative-control": "DELETE",
}


def _dirs() -> list[Path]:
    return sorted(p for p in _CAPTURES.iterdir() if p.is_dir())


def test_the_archive_has_captures_to_summarise():
    assert len(_dirs()) >= 20


def test_every_capture_either_summarises_or_refuses_with_a_reason():
    """No capture may crash the summariser, and no refusal may be silent."""
    for d in _dirs():
        try:
            summary = summarize(d, _HINTS.get(d.name))
        except CaptureRefused as exc:
            assert str(exc), "a refusal must say why"
            continue
        assert summary.scenario == d.name
        assert summary.runs
        assert summary.file_count >= 1


def test_a_capture_without_an_execution_record_is_refused():
    """Refused, not summarised as empty — an empty summary reads as 'nothing happened'."""
    candidates = [d for d in _dirs() if not any(p.name.endswith("execution_record.json") for p in d.iterdir())]
    assert candidates, "expected the archive to contain non-execution captures"
    with pytest.raises(CaptureRefused):
        summarize(candidates[0])


@pytest.mark.parametrize(
    "scenario,expected_status",
    [
        ("cap155-e4-head-status", 200),
        ("cap155-e4-options-status", 204),
        ("cap155-e4-trace-status", 200),
        ("cap155-e4-negative-control", 405),
    ],
)
def test_the_counterparty_status_is_read_from_the_counterpartys_own_log(scenario, expected_status):
    """The only field that distinguishes a success from a refusal.

    The negative control is the one that matters: its execution reports COMPLETE
    with zero inbound errors, exactly like the three successes, and only the
    counterparty's log says 405.
    """
    summary = summarize(_CAPTURES / scenario, _HINTS[scenario])
    statuses = {r.counterparty_status for r in summary.runs}
    assert statuses == {expected_status}


def test_the_platform_status_does_not_distinguish_the_refusal():
    """The trap, asserted rather than described.

    If this ever fails because the platform started reporting the refusal, the
    counterparty-log machinery could be simplified — so it is worth knowing.
    """
    control = summarize(_CAPTURES / "cap155-e4-negative-control", "DELETE")
    success = summarize(_CAPTURES / "cap155-e4-head-status", "HEAD")
    assert {r.status for r in control.runs} == {r.status for r in success.runs}
    assert {r.inbound_error_documents for r in control.runs} == \
           {r.inbound_error_documents for r in success.runs}
    # ...and yet they are distinguishable, from the counterparty side only.
    assert {r.counterparty_status for r in control.runs} != \
           {r.counterparty_status for r in success.runs}


def test_the_capture_digest_covers_names_as_well_as_bytes():
    """Two captures differing only in which file held which payload must differ."""
    a = summarize(_CAPTURES / "cap155-e4-head-status", "HEAD")
    b = summarize(_CAPTURES / "cap155-e4-options-status", "OPTIONS")
    assert a.capture_digest != b.capture_digest
    again = summarize(_CAPTURES / "cap155-e4-head-status", "HEAD")
    assert a.capture_digest == again.capture_digest


def test_a_double_execution_yields_two_runs():
    summary = summarize(_CAPTURES / "cap155-e2-post")
    assert len(summary.runs) == 2
    assert len(summary.execution_ids) == 2


def test_counterparty_attestation_is_reported_honestly():
    """A capture with no log must not claim one."""
    with_log = summarize(_CAPTURES / "cap155-e4-head-status", "HEAD")
    without = summarize(_CAPTURES / "cap155-e2-post")
    assert with_log.has_counterparty_attestation
    assert not without.has_counterparty_attestation


def test_an_ambiguous_log_attribution_is_refused_rather_than_guessed():
    """Constructed case: the same verb twice in one log cannot be attributed."""
    import shutil
    import tempfile

    src = _CAPTURES / "cap155-e4-head-status"
    with tempfile.TemporaryDirectory() as tmp:
        dst = Path(tmp) / src.name
        shutil.copytree(src, dst)
        log = dst / "mock_access_log.txt"
        line = next(l for l in log.read_text().splitlines() if '"HEAD ' in l)
        log.write_text(log.read_text() + "\n" + line + "\n")
        with pytest.raises(CaptureRefused) as err:
            summarize(dst, "HEAD")
        assert "unambiguously" in str(err.value)


def test_convergence_is_derived_from_the_raw_bodies():
    """The PATCH double-execution, recomputed rather than trusted.

    The capture tooling writes its own analysis beside these files. This derives
    the same facts from the platform's returned bodies, so the two agree
    independently — and if the tooling's judgement about volatile fields were ever
    wrong, this would disagree with it rather than inherit it.
    """
    summary = summarize(_CAPTURES / "cap155-e3b-patch-canonical")
    by_subject = {c.subject: c for c in summary.convergence}
    assert set(by_subject) == {"target", "template"}

    target = by_subject["target"]
    # The positive control: the first call must actually have done something,
    # otherwise "the replay changed nothing" is meaningless.
    assert target.first_call_changed_state is True
    # The replay moved ONLY the server-maintained timestamp — which is what
    # conditionally-idempotent means for this endpoint.
    assert target.fields_differing_on_replay == ("modifiedOn",)


def test_the_untouched_subject_is_a_negative_control():
    """A resource the calls never targeted must move at neither step.

    If this one had changed, the target's convergence would be evidence of
    nothing — it would mean something other than the call under test was moving
    state during the capture window.
    """
    summary = summarize(_CAPTURES / "cap155-e3b-patch-canonical")
    template = {c.subject: c for c in summary.convergence}["template"]
    assert template.first_call_changed_state is False
    assert template.replay_changed_state is False
    assert template.fields_differing_on_replay == ()


def test_both_patch_capture_generations_agree():
    """The superseded and corrected capture sets must tell the same story."""
    stories = {}
    for name in ("cap155-e3-patch-canonical", "cap155-e3b-patch-canonical"):
        target = {c.subject: c for c in summarize(_CAPTURES / name).convergence}["target"]
        stories[name] = (target.first_call_changed_state, target.fields_differing_on_replay)
    assert len(set(stories.values())) == 1, stories


def test_captures_without_staged_readbacks_report_no_convergence():
    """Absence is reported as absence, never as 'converged'."""
    assert summarize(_CAPTURES / "cap155-e4-head-status", "HEAD").convergence == ()
