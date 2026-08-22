"""#177: the defect-class table must AGREE with the finding rows it claims to derive from.

The table's own header says "Instances (derived from rows)", and the ledger states the
derivation rule explicitly. It went stale twice anyway — once as `A6-07` (the table listed
one class while the rows carried four) and again one round later, after new rows were
appended without regenerating it. Two instances of one mechanism is this repo's trigger for
replacing the enumeration with an invariant, and a table a human regenerates by hand is an
enumeration.

So the arithmetic is checked here instead of promised in prose. This does NOT verify that a
class assignment is CORRECT — only running the reconciliation establishes that — it verifies
that the published counts and IDs are the ones the rows actually yield, which is the part a
machine can own.
"""

import pathlib
import re

_LEDGER = (
    pathlib.Path(__file__).resolve().parent.parent
    / "docs"
    / "architecture"
    / "ISSUE_177_AUDIT_LEDGER.md"
)

#: Rows that are NOT instances found inside this slice: the inherited class-level deviations
#: the slice exists to discharge. The ledger's derivation rule names them explicitly, and
#: this list is the same rule in executable form.
_EXCLUDED_PREFIX = "INH-"


#: A row's cells, split on unescaped pipes only. A verbatim summary may legally contain a
#: Markdown-escaped pipe, and a naive `split("|")` then shifts every later cell — the defect
#: class stops being `cells[5]` and the row is silently DROPPED, which would let someone
#: append such a row and leave the table stale with this check still green.
#: A pipe is a DELIMITER when the backslash run immediately before it is even — `\\|` is a
#: literal backslash followed by a real separator, while `\|` is an escaped pipe. A negative
#: lookbehind cannot tell those apart, so the run is counted modulo two.
_PIPE_RUN = re.compile(r"(\\*)\|")


def _cells(line):
    body = line.strip()
    if body.startswith("|"):
        body = body[1:]
    cells, start, last = [], 0, 0
    for match in _PIPE_RUN.finditer(body):
        if len(match.group(1)) % 2:
            continue  # odd run -> the pipe is escaped, not a delimiter
        cells.append(body[start : match.end() - 1])
        start = match.end()
        last = match.end()
    tail = body[start:]
    # A trailing delimiter leaves an empty tail, which is not a cell.
    if tail.strip() or not last:
        cells.append(tail)
    return [cell.strip() for cell in cells]


def _finding_rows(text):
    """Every finding row as `(id, defect_class)`, read from the one contiguous table.

    Rows dispositioned `finding-refuted` are EXCLUDED, because the ledger's own derivation
    rule excludes them: a refuted finding is not an instance of anything. Reading only the
    id and class would have counted them and forced a wrong published total the first time
    a refutation landed.
    """
    rows = []
    for line in text.splitlines():
        if not line.startswith("| "):
            continue
        cells = _cells(line)
        if len(cells) < 9 or cells[0] in {"ID", "---"}:
            continue
        match = re.search(r"DC-177-[A-Z]", cells[5])
        if match is None:
            continue
        if cells[8].startswith("`finding-refuted`"):
            continue
        rows.append((cells[0], match.group(0)))
    return rows


def _published_table(text):
    """`{class: (count, [ids])}` as the defect-class table PUBLISHES it."""
    published = {}
    for line in text.splitlines():
        if not line.startswith("| **DC-177-"):
            continue
        cells = _cells(line)
        name = re.search(r"DC-177-[A-Z]", cells[0]).group(0)
        count = re.search(r"\*\*(\d+) rows\*\*", cells[3])
        assert count is not None, cells[3]
        ids = re.findall(r"\b((?:QA|A6|L2|INH)-[A-Za-z0-9-]+)", cells[3])
        published[name] = (int(count.group(1)), ids)
    return published


def test_the_defect_class_table_matches_the_rows_it_is_derived_from():
    text = _LEDGER.read_text(encoding="utf-8")

    rows = _finding_rows(text)
    assert rows, "no finding rows parsed — this check would be vacuous"

    derived = {}
    for finding_id, cls in rows:
        if finding_id.startswith(_EXCLUDED_PREFIX):
            continue
        derived.setdefault(cls, []).append(finding_id)

    published = _published_table(text)
    assert published, "no defect-class table parsed — this check would be vacuous"

    assert set(published) == set(derived), {
        "published but not derived": sorted(set(published) - set(derived)),
        "derived but not published": sorted(set(derived) - set(published)),
    }

    wrong_counts = sorted(
        (cls, published[cls][0], len(derived[cls]))
        for cls in derived
        if published[cls][0] != len(derived[cls])
    )
    assert wrong_counts == [], wrong_counts

    # EXACT multisets, both directions. A one-way subtraction let a class row keep the right
    # count while publishing an extra or duplicated ID, so the enumeration could disagree
    # with the rows and still pass.
    import collections

    mismatched = sorted(
        (
            cls,
            sorted((collections.Counter(published[cls][1]) - collections.Counter(derived[cls])).elements()),
            sorted((collections.Counter(derived[cls]) - collections.Counter(published[cls][1])).elements()),
        )
        for cls in derived
        if collections.Counter(published[cls][1]) != collections.Counter(derived[cls])
    )
    assert mismatched == [], mismatched

    duplicated = sorted(
        (cls, sorted(i for i, n in collections.Counter(published[cls][1]).items() if n > 1))
        for cls in published
        if any(n > 1 for n in collections.Counter(published[cls][1]).values())
    )
    assert duplicated == [], duplicated
