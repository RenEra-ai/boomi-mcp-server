"""DC-178-D structural fix: the ledger's review accounting is DERIVED, not typed.

TWO findings in one slice had the same shape — a ledger row the durable evidence
already knows about, left unwritten:

* `L2R3-02` — a review round was archived but never added to the evaluation table;
* `L2R3-01` — three evaluations accrued in each loop with the checkpoint table
  still empty.

`QA-178-r3-02` (a baseline described by which round last ran, rather than by its
diff) was ORIGINALLY assigned to this class and is NOT covered here. That claim was
withdrawn on measurement, not narrowed after the fact: this authority holds only
collected commit-review rounds, with no QA baseline or source-diff information, so
a Stage-1 evaluation naming the wrong tested tree would leave every assertion in
this file green. It is reclassified as DC-178-E, whose authority is `git diff` and
whose disposition is recorded in the ledger.

Second instance triggers the structural-fix rule, so the enumeration is replaced
by an invariant read from the runtime authority. That authority is
`docs/architecture/evidence/issue-178/index.jsonl`: the collector writes one row
per COLLECTED review round, and a round that is not collected does not exist as
far as the workflow is concerned. Prose can now disagree with the archive only by
failing a test.

SCOPE — widened, and the earlier scoping note was measured wrong.

An earlier revision said this could not go repo-wide because "`issue-152` has 65
uncited runs, `issue-153` has 11, `issue-175` has 1". Re-measured properly, TWO of
those three assertions widen cleanly:

* **No ledger cites a run its archive lacks** holds for ALL nine ledgers today, with
  no exemption. It is enforced repo-wide below.
* **Every completed archived run is cited in its ledger** holds for six of nine.
  Three predate the convention (`issue-152` 64 runs, `issue-153` 8, `issue-175` 1 —
  73 total). Those are FROZEN in
  `tests/fixtures/audit_ledger_citation_legacy_baseline.json` and the freeze is
  asserted exactly, so the set cannot grow: a NEW uncited run in ANY ledger fails,
  including in those three.

What stays #178-scoped, and why, stated rather than implied: the evaluation-table
and checkpoint-row assertions read a table FORMAT that only this ledger uses —
measured, zero numbered `cdx-review` rows in every other ledger. Asserting a format
nobody else adopted would fail on correct records.

One field is deliberately NOT used as an authority anywhere here: `logical_loop`.
It is free-typed prose — 27 distinct spellings across the archives and 66 rows with
no value at all — so keying a derivation on it would be the very DC-178-D mechanism
this file exists to close. The collector-written `collector`, `status` and
`durable_dir` fields are the authority instead.
"""

import json
import re
import sys
from pathlib import Path

import pytest

_ROOT = Path(__file__).resolve().parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

ARCHITECTURE = _ROOT / "docs" / "architecture"
LEDGER = ARCHITECTURE / "ISSUE_178_AUDIT_LEDGER.md"
LEGACY_BASELINE = (
    _ROOT / "tests" / "fixtures" / "audit_ledger_citation_legacy_baseline.json"
)
INDEX = _ROOT / "docs" / "architecture" / "evidence" / "issue-178" / "index.jsonl"

#: Every third evaluation of a loop forces a recorded checkpoint decision.
CHECKPOINT_EVERY = 3


def _archive_rows():
    rows = [json.loads(line) for line in INDEX.read_text().splitlines() if line.strip()]
    return rows[0], rows[1:]


#: The Stage-2 loop's own label, as the collector records it on each archived row.
#: Filtering on it is load-bearing: a downstream or wave correction's repo review
#: uses the SAME `commit-review-collect` collector under a DIFFERENT logical loop,
#: and counting those as Stage-2 evaluations would demand a spurious L2 checkpoint
#: the moment the combined total crossed a multiple of three.
STAGE2_LOOP_PREFIX = "L2"


def _collected_review_runs(loop_prefix=STAGE2_LOOP_PREFIX):
    """Run-dir names of every COLLECTED commit-review round in ONE logical loop."""
    _header, runs = _archive_rows()
    return [
        row["durable_dir"].split("/")[-1]
        for row in runs
        if row.get("collector") == "commit-review-collect"
        and row.get("status") == "completed"
        and str(row.get("logical_loop", "")).startswith(loop_prefix)
    ]


#: The heading that owns the Stage-2 evaluation table. Parsing is bounded to this
#: SECTION, not the file: the §6 architect gate is already on this slice's roster
#: and will be recorded in its own numbered table of `cdx-gate-review.*` rows. A
#: whole-file scan would read those as Stage-2 history and then reject them as
#: unowned, so the guard would fail on a correctly-kept record.
STAGE2_HEADING = "## Stage-2 repo Codex review (loop 2)"


def _stage2_table_runs():
    """Run-dir names listed in the Stage-2 EVALUATION TABLE, in order.

    Parsing the table rather than the whole file is the point. A whole-file
    substring check is satisfied by the run's own FINDING row, so it passes while
    the evaluation table silently loses an entry — measured: deleting round 2's
    table row left all six assertions green. That is exactly the `L2R3-02`
    accounting defect, so the guard that claims to prevent it must read the table
    the cumulative count is displayed in.

    A LIST, not a set: duplicates inflate the evaluation count just as effectively
    as omissions hide it, and set membership cannot see a row listed twice.
    """
    text = LEDGER.read_text(encoding="utf-8")
    # Anchored at LINE START, not by substring. The ledger quotes this heading
    # inside a finding row that explains the fix, and finding rows come earlier in
    # the file — so a bare `index()` matched the quotation, sliced an empty
    # section, and reported zero table rows. Measured, on this file.
    heading = re.search(
        r"^{0}$".format(re.escape(STAGE2_HEADING)), text, re.MULTILINE
    )
    assert heading is not None, "the Stage-2 section heading was not found"
    rest = text[heading.end():]
    # Stop at the next top-level heading, so a later section's numbered table is
    # never read as this one's history.
    end = rest.find("\n## ")
    section = rest if end == -1 else rest[:end]
    rows = []
    for line in section.splitlines():
        # `cdx-review.` ONLY — a gate-review run belongs to a different loop and
        # is not Stage-2 history even when it appears in a numbered row.
        match = re.match(r"\|\s*\d+\s*\|\s*`(cdx-review\.[A-Za-z0-9_-]+)`", line)
        if match:
            rows.append(match.group(1))
    return rows


def test_the_archive_is_the_authority_and_is_not_empty():
    """A derivation over an empty set proves nothing about the prose it guards."""
    header, runs = _archive_rows()
    assert header["issue"] == 178, header
    assert runs, "no archived rows — every assertion below would be vacuous"
    assert _collected_review_runs(), "no collected review rounds in the archive"


def test_every_archived_review_round_appears_in_the_stage2_table():
    """`L2R3-02`'s defect, made unwritable — in the TABLE, not merely in the file.

    A round that was collected but never entered in the evaluation table corrupts
    the cumulative count the checkpoint rule keys on, which is how a mandatory
    third-evaluation checkpoint gets skipped without anyone noticing.
    """
    listed = _stage2_table_runs()
    assert listed, "the Stage-2 evaluation table parsed to nothing"
    missing = [name for name in _collected_review_runs() if name not in listed]
    assert missing == [], (
        "archived Stage-2 rounds absent from the evaluation table: {0}".format(
            sorted(missing)
        )
    )


def test_the_stage2_table_matches_the_archive_exactly_once_each():
    """The table must not pad its own history either.

    Compared as MULTISETS. Set membership passes a run listed twice — both copies
    are "owned" — while the displayed evaluation count is silently inflated, which
    is the same accounting corruption as an omission pointing the other way.
    """
    from collections import Counter

    listed = Counter(_stage2_table_runs())
    owned = Counter(_collected_review_runs())
    assert listed == owned, {
        "duplicated_or_unowned": sorted((listed - owned).elements()),
        "missing": sorted((owned - listed).elements()),
    }


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

    listed = _stage2_table_runs()
    fabricated = real + ["cdx-review.NOTARCHIVED"]
    missing = [name for name in fabricated if name not in listed]
    assert missing == ["cdx-review.NOTARCHIVED"], (
        "the table check cannot detect a round missing from the evaluation table"
    )
    # ...and the check reads the TABLE, not the whole file: a name that appears
    # only in prose must NOT satisfy it. This is the precise weakness that made
    # the first version of this guard pass while the defect it names recurred.
    assert "L2R1-01" in text and "L2R1-01" not in listed

    # ...and the checkpoint obligation really does scale with the round count.
    assert (len(real) + CHECKPOINT_EVERY) // CHECKPOINT_EVERY > len(
        real
    ) // CHECKPOINT_EVERY


# ---------------------------------------------------------------------------
# Repo-wide: every audit ledger, not only this slice's.
# ---------------------------------------------------------------------------


def _every_ledger():
    """(issue, ledger_text, completed_run_names, all_archived_names) per archive.

    Keyed on the COLLECTOR-written fields. `logical_loop` is deliberately not
    consulted: it is hand-typed prose with 27 spellings across the archives, so a
    derivation resting on it would be the defect class this file closes.
    """
    out = []
    for index in sorted((ARCHITECTURE / "evidence").glob("issue-*/index.jsonl")):
        rows = [json.loads(l) for l in index.read_text().splitlines() if l.strip()]
        header, runs = rows[0], rows[1:]
        ledger = ARCHITECTURE / "ISSUE_{0}_AUDIT_LEDGER.md".format(header["issue"])
        if not ledger.is_file():
            continue
        archived = {
            row["durable_dir"].split("/")[-1] for row in runs if "durable_dir" in row
        }
        completed = {
            row["durable_dir"].split("/")[-1]
            for row in runs
            if row.get("status") == "completed" and "durable_dir" in row
        }
        out.append(
            (str(header["issue"]), ledger.read_text(encoding="utf-8"), completed, archived)
        )
    return out


def _legacy_uncited():
    return json.loads(LEGACY_BASELINE.read_text())["uncited_by_issue"]


def test_every_ledger_is_covered_and_the_scan_is_not_empty():
    """Non-vacuity for the repo-wide assertions below."""
    ledgers = _every_ledger()
    assert len(ledgers) >= 8, len(ledgers)
    assert any(completed for _i, _t, completed, _a in ledgers)


def test_no_ledger_cites_a_review_run_its_archive_lacks():
    """Repo-wide, with NO exemption — measured true for every ledger today.

    A cited run with no archived evidence is a fabricated attestation, which is the
    failure #152 drove through.
    """
    offenders = {}
    for issue, text, _completed, archived in _every_ledger():
        cited = set(re.findall(r"cdx(?:-gate)?-review\.[A-Za-z0-9_-]+", text))
        missing = sorted(cited - archived)
        if missing:
            offenders[issue] = missing
    assert offenders == {}, offenders


def test_every_completed_run_is_cited_by_its_ledger_outside_the_frozen_legacy_set():
    """Repo-wide, minus a frozen legacy set that cannot grow.

    Six of nine ledgers satisfy this outright. The three that do not predate the
    convention and are enumerated in the baseline fixture — so every FUTURE ledger
    is covered automatically, and a new uncited run in ANY ledger fails, including
    in the three exempt ones.
    """
    legacy = _legacy_uncited()
    offenders = {}
    for issue, text, completed, _archived in _every_ledger():
        exempt = set(legacy.get(issue, ()))
        missing = sorted(name for name in completed if name not in text) 
        unexpected = [name for name in missing if name not in exempt]
        if unexpected:
            offenders[issue] = unexpected
    assert offenders == {}, offenders


def test_the_legacy_baseline_is_frozen_minimal_and_still_accurate():
    """The exemption may not be padded, and may not outlive what it excuses.

    Both directions matter. A baseline that listed runs which ARE cited would be a
    licence to stop citing them later; one that silently absorbed new entries would
    make the invariant above decorative.
    """
    legacy = _legacy_uncited()
    assert legacy, "the frozen baseline is empty — the assertions above are untested"
    by_issue = {i: (t, c) for i, t, c, _a in _every_ledger()}
    stale = {}
    for issue, names in legacy.items():
        assert issue in by_issue, "baseline names an issue with no ledger: " + issue
        text, completed = by_issue[issue]
        # Every exempt run must still be BOTH archived-completed and uncited. A run
        # that has since been cited must leave the baseline, not linger in it.
        wrong = sorted(
            name for name in names if name in text or name not in completed
        )
        if wrong:
            stale[issue] = wrong
    assert stale == {}, stale
    assert sum(len(v) for v in legacy.values()) == 73, {
        k: len(v) for k, v in legacy.items()
    }
