#!/usr/bin/env python3
"""Rebaseline the M12.12 reachability inventory and re-emit its markdown tables.

Editing a module the recipe layer scans moves the capability revision, and five
served artifacts bind that revision. The freeze test then fails and names this
procedure. Doing it by hand has gone wrong three separate ways in this
repository, each time differently, which is why it is a script:

* the emitter writes startup log lines to stdout, so pasting its output whole
  splices ``[INFO]`` text into a tracked document — that destroyed a heading once
  (collecting only lines that begin with a pipe is what actually prevents it; the
  explicit log filter below is redundant belt-and-braces, and is kept as
  documentation of the failure rather than as the defence against it);
* replacing everything between two headings deletes the PROSE a section keeps
  between its tables;
* treating a table as "lines beginning with a pipe and a space" ends the run at
  the ``|---|`` separator, so only the header is replaced and the old rows are
  orphaned underneath the new one.

Each generated table is therefore matched to the document run whose HEADER row
equals it, and the result is graded by the property all three mistakes violated:
no line that is not a table row may be added or removed.
"""

from __future__ import annotations

import argparse
import pathlib
import re
import subprocess
import sys

ROOT = pathlib.Path(__file__).resolve().parents[1]
INVENTORY = ROOT / "tests/fixtures/m12_12/legacy_reachability_inventory.json"
DOCUMENT = ROOT / "docs/architecture/M12_COMPATIBILITY_INVENTORY.md"
EMITTER = ROOT / "tests/_m12_12_legacy_inventory.py"

#: Which generated block belongs under which section heading, in document order.
#: A section may also hold hand-authored tables, which is why the match is by
#: header row rather than by position.
PLACEMENT = {
    "11.2": ["11.2"],
    "11.3": ["11.3"],
    "11.4": ["11.4-routes", "11.4-sites"],
    "11.5": ["11.5"],
    "11.6": ["11.6"],
}


def _run(*args: str) -> str:
    env = {"PYTHONPATH": "src", "PYTHONDONTWRITEBYTECODE": "1", "BOOMI_LOCAL": "true"}
    import os

    result = subprocess.run(
        [sys.executable, *args],
        cwd=ROOT,
        capture_output=True,
        text=True,
        env={**os.environ, **env},
    )
    if result.returncode != 0:
        raise SystemExit(f"{args[0]} failed:\n{result.stderr}")
    return result.stdout


def _emitted_blocks() -> dict[str, list[str]]:
    """The generated tables, with the emitter's own log lines removed."""
    blocks: dict[str, list[str]] = {}
    key = None
    for line in _run(str(EMITTER), "--emit-markdown").split("\n"):
        if re.match(r"^\[(INFO|WARN|ERROR)\]", line):
            continue
        marker = re.match(r"<!-- generated: (\S+) -->", line)
        if marker:
            key = marker.group(1)
            blocks[key] = []
        elif key is not None and line.startswith("|"):
            blocks[key].append(line)
    return blocks


def _table_runs(lines: list[str], start: int, end: int) -> list[tuple[int, int]]:
    """Contiguous runs of table lines. A separator is a table line."""
    runs, i = [], start
    while i < end:
        if lines[i].startswith("|"):
            j = i
            while j < end and lines[j].startswith("|"):
                j += 1
            runs.append((i, j))
            i = j
        else:
            i += 1
    return runs


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--check",
        action="store_true",
        help="report what would change without writing anything",
    )
    args = parser.parse_args()

    before_doc = DOCUMENT.read_text()
    if not args.check:
        _run(str(EMITTER), "--write", str(INVENTORY))

    blocks = _emitted_blocks()
    missing = [k for keys in PLACEMENT.values() for k in keys if k not in blocks]
    if missing:
        raise SystemExit(f"the emitter produced no block for: {missing}")

    lines = DOCUMENT.read_text().split("\n")
    for section, keys in PLACEMENT.items():
        for key in reversed(keys):  # back to front keeps earlier indices valid
            start = next(
                i for i, l in enumerate(lines) if l.startswith(f"## {section} ")
            )
            end = next(
                i for i in range(start + 1, len(lines)) if lines[i].startswith("## ")
            )
            rows = blocks[key]
            hits = [
                (a, b) for a, b in _table_runs(lines, start, end) if lines[a] == rows[0]
            ]
            if len(hits) != 1:
                raise SystemExit(
                    f"{section}/{key}: {len(hits)} document tables match the "
                    "generated header; the placement map needs updating"
                )
            a, b = hits[0]
            lines[a:b] = rows
            print(f"  {section}/{key}: {b - a} rows -> {len(rows)}")

            # NO ORPHANS. The run detector once ended at the `|---|` separator,
            # so a ten-row table replaced a one-line "run" and the old rows
            # survived underneath the new ones. The non-table grade below cannot
            # see that — every line involved is a table line — so it is caught
            # here, where the shape is still known.
            after_splice = a + len(rows)
            if after_splice < len(lines) and lines[after_splice].startswith("|"):
                raise SystemExit(
                    f"{section}/{key}: table rows survive below the replacement at "
                    f"line {after_splice + 1}; the run detector ended early"
                )

    after = "\n".join(lines)

    # THE GRADE. Every mistake this script exists to prevent shows up as a
    # non-table line appearing or disappearing.
    def _non_table(text: str) -> list[str]:
        return [l for l in text.split("\n") if not l.startswith("|")]

    lost = set(_non_table(before_doc)) - set(_non_table(after))
    gained = set(_non_table(after)) - set(_non_table(before_doc))
    if lost or gained:
        raise SystemExit(
            "refusing to write: the splice changed non-table lines.\n"
            f"  removed: {sorted(lost)[:5]}\n  added: {sorted(gained)[:5]}"
        )

    if args.check:
        print("check only: no files written")
        return 0
    DOCUMENT.write_text(after)
    print(f"rebaselined {INVENTORY.name} and re-emitted section 11")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
