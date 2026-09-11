"""#158 SELF-158-r26-01: every finding of the #158 audit ledger is visible to its scanner.

Six rows were minted with a round segment (`s2r1`, `s2r3b`, ...) outside the
finding-id grammar the ledger scanners share, so no append-only, disposition,
supersession or class-tally check read them — and one was edited after its
first commit without any check objecting. Their originals stay byte-frozen
under the ids their gates reported; each has a declared scanner-visible record.
Scoped to this ledger: widening the grammar across ledgers surfaces historical
renames that are not this slice's to reconcile (#155 SELF-155-r65-01).
"""

import re
from pathlib import Path

_LEDGER = Path(__file__).resolve().parents[1] / "docs" / "architecture" / "ISSUE_158_AUDIT_LEDGER.md"


def _findings_table_ids(text):
    section = text.split("## Findings ledger", 1)[1].split("## Checkpoint records", 1)[0]
    ids = []
    for line in section.splitlines():
        if not line.startswith("| "):
            continue
        first = line.split("|")[1].strip()
        if first in ("ID", "---"):
            continue
        ids.append(first)
    return ids


def test_every_finding_row_of_the_158_ledger_is_visible_to_its_scanner():
    from test_wave_gate import _finding_rows

    text = _LEDGER.read_text(encoding="utf-8")
    table_ids = _findings_table_ids(text)
    visible = set(_finding_rows(text))
    # The extraction and the scanner read the same table: they agree on rows.
    assert set(table_ids) & visible, (table_ids, sorted(visible))
    # The declaration is its own paragraph (a row's prose may name it).
    block = re.search(r"^\*\*Scanner-visible records\*\*(.+?)(?:\n\n|\Z)", text, re.S | re.M)
    declared = {
        original: record
        for record, original in re.findall(r"`([^`]+)` records `([^`]+)`", block.group(1) if block else "")
    }
    invisible = sorted(i for i in table_ids if i not in visible)
    # Every row the scanner cannot parse is a declared original, and every
    # declared original is one it cannot parse (a stale declaration fails too).
    assert invisible == sorted(declared), (invisible, sorted(declared))
    # Each declared original has its record present, parsed, and naming it.
    for original, record in declared.items():
        assert record in visible, (original, record)
        assert "`{0}`".format(original) in _record_line(text, record), (original, record)


def _record_line(text, record):
    return next(line for line in text.splitlines() if line.startswith("| {0} |".format(record)))
