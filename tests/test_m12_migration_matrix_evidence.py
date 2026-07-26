"""Every test cited as regression evidence in the M12 migration matrix exists.

Added after QA Bug #183 (issue #143). §7 of the compatibility inventory opens by
declaring that it discharges the acceptance criterion

    "each existing validator is accounted for in a migration matrix with
     soundness decision and regression evidence"

— so the "Regression evidence" column is not documentation, it IS the artifact
that discharges the criterion. Seven of its cells named tests that had never
been written, six of them prefixed "New " (a promise about this issue's own
deliverable) on rows whose own "Sound?/Complete?" cell conceded the coverage was
missing. The reference did not fail loudly because the cited FILE existed; only
the test names inside it did not.

A doc-only fix would have left the same trap for the next issue that extends the
matrix, so this is the structural guard instead: it re-derives the citations from
the document and resolves each against the real suite. A future row citing a
nonexistent test fails here rather than at some later audit.
"""

from __future__ import annotations

import re
import subprocess
import sys
from pathlib import Path

import pytest

_ROOT = Path(__file__).resolve().parent.parent
_INVENTORY = _ROOT / "docs" / "architecture" / "M12_COMPATIBILITY_INVENTORY.md"

#: ``tests/<file>.py::<name>`` where <name> may be a test function OR a class.
#: A citation may be ``file::test``, ``file::Class`` or ``file::Class::method``.
#: Capturing only the FIRST ``::`` segment reproduces the exact trap this guard
#: exists to close (QA Bug #196): a three-part citation would verify the CLASS
#: exists and never check the method — and one such citation was broken.
_CITATION = re.compile(r"tests/[A-Za-z0-9_/]+\.py(?:::[A-Za-z0-9_]+)+")


def _cited_ids() -> list:
    if not _INVENTORY.exists():  # pragma: no cover - the doc is tracked
        pytest.skip("compatibility inventory not present")
    return sorted(set(_CITATION.findall(_INVENTORY.read_text())))


def _collected_ids() -> set:
    """Every collectible node id, plus each id's file::class and file::function.

    Collected once via ``--collect-only`` rather than by grepping for ``def`` —
    a grep would accept a test that exists but cannot be collected (an import
    error, a skipped module), which is not evidence of anything.
    """
    result = subprocess.run(
        [
            sys.executable,
            "-m",
            "pytest",
            "--collect-only",
            "-q",
            "-p",
            "no:randomly",
            str(_ROOT / "tests"),
        ],
        capture_output=True,
        text=True,
        cwd=str(_ROOT),
        env={"PYTHONPATH": "src", "PATH": "/usr/bin:/bin:/usr/sbin:/sbin"},
    )
    ids = set()
    for line in result.stdout.splitlines():
        line = line.strip()
        if "::" not in line or not line.startswith("tests/"):
            continue
        ids.add(line)
        parts = line.split("::")
        # tests/f.py::TestClass::test_x  ->  also index ::TestClass and ::test_x
        for part in parts[1:]:
            ids.add("{0}::{1}".format(parts[0], part.split("[")[0]))
        ids.add(line.split("[")[0])
    return ids


def test_the_matrix_actually_cites_evidence():
    """Guard the guard: if the regex stops matching, every other assertion here
    passes vacuously."""
    cited = _cited_ids()
    assert len(cited) > 100, len(cited)


def test_every_cited_regression_test_resolves():
    cited = _cited_ids()
    collected = _collected_ids()
    missing = [item for item in cited if item not in collected]
    assert missing == [], (
        "migration-matrix rows cite tests that do not exist: {0}".format(missing)
    )


# ---------------------------------------------------------------------------
# The §7.2 verdict tally is DERIVED from the table, not asserted in prose.
#
# Added after QA Bug #188 — the ninth consecutive round to find the same class
# of defect: a hand-maintained claim about the tree that was true when written
# and quietly falsified by a later edit. Bugs #182, #186, #187 and #188 were all
# that shape, and each was found one round after the change that broke it.
#
# The fix that worked for Bug #183 was to stop asserting and start deriving, so
# the same treatment applies here: the vocabulary table states a row count per
# verdict, and this test re-counts the actual column. A future edit that adds a
# row without updating the tally fails here rather than at the next review.
# ---------------------------------------------------------------------------

_LEDGER_START = "### 7.2 Error-code ledger"
_LEDGER_END = "### 7.3"


def _ledger_rows() -> list:
    text = _INVENTORY.read_text()
    body = text.split(_LEDGER_START, 1)[1].split(_LEDGER_END, 1)[0]
    rows = []
    for line in body.split("\n"):
        line = line.strip()
        # a ledger row starts with a code cell and ends with its verdict
        if not line.startswith("| `") or not line.endswith(" |"):
            continue
        cells = [c.strip() for c in line.strip("|").split(" | ")]
        if len(cells) < 5:
            continue
        rows.append(cells)
    return rows


def _declared_counts() -> dict:
    """The per-verdict counts the vocabulary table claims."""
    text = _INVENTORY.read_text()
    body = text.split(_LEDGER_START, 1)[1].split(_LEDGER_END, 1)[0]
    declared = {}
    for line in body.split("\n"):
        m = re.match(r"^\|\s*`([a-z-]+)`\s*\|\s*(\d+)\s*\|", line.strip())
        if m:
            declared[m.group(1)] = int(m.group(2))
    return declared


def test_the_ledger_verdict_tally_matches_the_table():
    rows = _ledger_rows()
    actual = {}
    for cells in rows:
        actual[cells[-1]] = actual.get(cells[-1], 0) + 1
    declared = _declared_counts()
    assert declared, "the §7.2 vocabulary table declares no counts"
    assert declared == actual, (
        "§7.2 vocabulary claims {0} but the column contains {1}".format(declared, actual)
    )


def test_every_ledger_verdict_is_from_the_declared_vocabulary():
    """A verdict value absent from the vocabulary table is undefined for a
    reader — which is precisely how `delegated` shipped in a header that still
    read '(no / re-homed / new)'."""
    declared = set(_declared_counts())
    used = {cells[-1] for cells in _ledger_rows()}
    assert used <= declared, sorted(used - declared)
