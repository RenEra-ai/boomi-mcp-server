"""DC-178-D structural fix: the ledger's review accounting is DERIVED, not typed.

Three findings in one slice had the same shape — a ledger fact maintained by hand
that the durable evidence already knows:

* `QA-178-r3-02` — a baseline was described by "the tree you tested last round"
  instead of by its actual diff, and silently carried three unvalidated corrections;
* `L2R3-02` — a review round was archived but never added to the evaluation table;
* `L2R3-01` — three evaluations accrued in each loop with the checkpoint table
  still empty.

Second instance triggers the structural-fix rule, so the enumeration is replaced
by an invariant read from the runtime authority. That authority is
`docs/architecture/evidence/issue-178/index.jsonl`: the collector writes one row
per COLLECTED review round, and a round that is not collected does not exist as
far as the workflow is concerned. Prose can now disagree with the archive only by
failing a test.

SCOPE, stated rather than implied. This is enforced for #178 and not repo-wide,
because repo-wide it is false TODAY and retrofitting it would be churn rather than
correctness — measured: `issue-152` has 65 archived runs its ledger never cites,
`issue-153` has 11, `issue-175` has 1. Those predate the convention. Narrowing the
guard to the slice that owns it is the honest option; silently asserting nothing
is not.
"""

import json
import re
import sys
from pathlib import Path

import pytest

_ROOT = Path(__file__).resolve().parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

LEDGER = _ROOT / "docs" / "architecture" / "ISSUE_178_AUDIT_LEDGER.md"
INDEX = _ROOT / "docs" / "architecture" / "evidence" / "issue-178" / "index.jsonl"

#: Every third evaluation of a loop forces a recorded checkpoint decision.
CHECKPOINT_EVERY = 3


def _archive_rows():
    rows = [json.loads(line) for line in INDEX.read_text().splitlines() if line.strip()]
    return rows[0], rows[1:]


def _collected_review_runs():
    """Run-dir names of every COLLECTED commit-review round, from the archive."""
    _header, runs = _archive_rows()
    return [
        row["durable_dir"].split("/")[-1]
        for row in runs
        if row.get("collector") == "commit-review-collect"
        and row.get("status") == "completed"
    ]


def test_the_archive_is_the_authority_and_is_not_empty():
    """A derivation over an empty set proves nothing about the prose it guards."""
    header, runs = _archive_rows()
    assert header["issue"] == 178, header
    assert runs, "no archived rows — every assertion below would be vacuous"
    assert _collected_review_runs(), "no collected review rounds in the archive"


def test_every_archived_review_round_appears_in_the_ledger():
    """`L2R3-02`'s defect, made unwritable.

    A round that was collected but never entered in the evaluation table corrupts
    the cumulative count the checkpoint rule keys on — which is how a mandatory
    third-evaluation checkpoint gets skipped without anyone noticing.
    """
    text = LEDGER.read_text(encoding="utf-8")
    missing = [name for name in _collected_review_runs() if name not in text]
    assert missing == [], (
        "archived review rounds absent from the ledger: {0}".format(sorted(missing))
    )


def test_the_ledger_cites_no_review_round_the_archive_lacks():
    """The other direction. A cited run with no archived evidence is a fabricated
    attestation — the failure #152 drove through and the reason the repo-wide
    scanner exists; asserted here too so both directions hold for this slice."""
    text = LEDGER.read_text(encoding="utf-8")
    cited = set(re.findall(r"cdx(?:-gate)?-review\.[A-Za-z0-9_-]+", text))
    _header, runs = _archive_rows()
    archived = {
        row["durable_dir"].split("/")[-1] for row in runs if "durable_dir" in row
    }
    assert cited - archived == set(), sorted(cited - archived)


def test_a_checkpoint_row_exists_for_every_third_review_evaluation():
    """`L2R3-01`'s defect, made unwritable.

    The obligation is DERIVED from how many rounds the archive holds, so it
    appears the moment a third round is collected rather than when someone
    remembers the rule.
    """
    count = len(_collected_review_runs())
    due = count // CHECKPOINT_EVERY
    if due == 0:
        pytest.skip("fewer than {0} collected rounds".format(CHECKPOINT_EVERY))
    text = LEDGER.read_text(encoding="utf-8")
    checkpoint_rows = [
        line
        for line in text.splitlines()
        if line.startswith("| L2 ") and "Stage-2" in line
    ]
    assert len(checkpoint_rows) >= due, (
        "{0} collected review rounds require {1} Stage-2 checkpoint row(s); "
        "found {2}".format(count, due, len(checkpoint_rows))
    )


def test_a_checkpoint_states_its_decision_and_rules_out_the_alternatives():
    """A checkpoint that records only an outcome is not a decision.

    The workflow requires per-tier counts, a trend, explicit rule-outs of the
    other outcomes and a named finite next correction — and `L2R4-01` showed the
    failure mode is describing the POST-correction state, which makes any decision
    look safe. So the rows must also disclose when they were not written in
    advance.
    """
    text = LEDGER.read_text(encoding="utf-8")
    rows = [line for line in text.splitlines() if re.match(r"\| L[12] — ", line)]
    assert rows, "no checkpoint rows found"
    for row in rows:
        for token in ("Per-tier", "Trend vector", "Rule-outs", "Named finite next"):
            assert token in row, "checkpoint row missing {0!r}: {1}".format(
                token, row[:120]
            )
        assert "DECISION POINT" in row.upper(), (
            "checkpoint row does not state the state it decided on: {0}".format(
                row[:120]
            )
        )


def test_the_derivation_would_notice_a_missing_row():
    """NON-VACUITY. The guard above passes today; this proves it can still fail.

    A checker that cannot be made to go red is not a checker — this repo has four
    recorded instances of exactly that.
    """
    text = LEDGER.read_text(encoding="utf-8")
    real = _collected_review_runs()
    assert real, "no runs to perturb"

    fabricated = real + ["cdx-review.NOTARCHIVED"]
    missing = [name for name in fabricated if name not in text]
    assert missing == ["cdx-review.NOTARCHIVED"], (
        "the citation check cannot detect an uncited round"
    )

    # ...and the checkpoint obligation really does scale with the round count.
    assert (len(real) + CHECKPOINT_EVERY) // CHECKPOINT_EVERY > len(
        real
    ) // CHECKPOINT_EVERY
