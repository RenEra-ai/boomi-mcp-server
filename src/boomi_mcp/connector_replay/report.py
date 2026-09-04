"""The human-readable view of what the registry actually knows.

Generated, never hand-edited, and tracked in the repository so that the registry's
claims are reviewable without running anything. Two properties matter:

* **Deterministic.** The same registry renders the same bytes. A report that
  reordered rows between runs would produce diff noise that trains reviewers to
  skim exactly the artifact they are meant to read.
* **Honest about emptiness.** The registry ships with no rows, and the report says
  so in those words. A summary that rendered an empty table with no explanation
  reads like a rendering bug; one that says "nothing is verified, so every write
  refuses a retry" states the operative consequence.
"""

from __future__ import annotations

from pathlib import Path
from typing import Final

from .registry import ReplayRegistry, load_registry

__all__ = ["REPORT_RELATIVE_PATH", "parse", "render", "write_report"]

#: Where the tracked report lives, relative to the repository root.
REPORT_RELATIVE_PATH: Final[str] = "docs/evidence/connector-replay-captures.md"

_HEADER: Final[str] = "# Connector replay evidence"

_PREAMBLE: Final[tuple[str, ...]] = (
    "*Generated from the packaged registry. Do not edit by hand — regenerate it.*",
    "",
    "This is the complete record of what has been OBSERVED about re-executing a",
    "connector action. Nothing here is derived from documentation or from the shape",
    "of a component: a row exists because an execution produced it.",
    "",
    "An action with no row is `unverified`, and `unverified` refuses a retry. That is",
    "the safe direction: a registry that failed to load, or loaded empty, denies.",
)


def render(registry: ReplayRegistry | None = None) -> str:
    """Render the report. Deterministic for a given registry."""
    reg = registry if registry is not None else load_registry()
    lines: list[str] = [_HEADER, "", *_PREAMBLE, "", "## Connector vocabulary", ""]

    if reg.vocabulary:
        lines += [
            "| platform connector type | family | action read from |",
            "| --- | --- | --- |",
        ]
        for entry in sorted(reg.vocabulary, key=lambda e: e.platform_connector_type):
            lines.append(
                f"| `{entry.platform_connector_type}` | {entry.family} "
                f"| {entry.action_source.value} |"
            )
    else:
        lines.append("No connector types are mapped, so no capture can be attributed.")

    lines += ["", "## Observed actions", ""]
    if reg.evidence_records:
        lines += [
            "| family | action | side effect | retry safety | executions | capture |",
            "| --- | --- | --- | --- | --- | --- |",
        ]
        for row in sorted(reg.evidence_records, key=lambda r: (r.family, r.action)):
            lines.append(
                f"| {row.family} | {row.action} | {row.side_effect.value} "
                f"| {row.retry_safety.value} | {len(row.execution_ids)} "
                f"| `{row.capture_digest[:12]}` |"
            )
    else:
        lines += [
            "**No actions have been verified.** The registry ships empty: the mechanism",
            "that ingests captures is in place, but the rows are minted in a later step.",
            "",
            "The operative consequence is that every write currently refuses a retry.",
        ]

    # THE PACKAGED OPERATION RECORDS, which the report called itself complete
    # without ever publishing. A reader was told what the class-level evidence
    # says and not which specific operations carry a contract — the rows that
    # actually authorise a retry.
    lines += ["", "## Operation contract records", ""]
    if reg.operation_records:
        lines += [
            "| contract reference | family | action | semantics | revision |",
            "| --- | --- | --- | --- | --- |",
        ]
        for record in sorted(reg.operation_records, key=lambda r: r.contract_ref):
            lines.append(
                f"| `{record.contract_ref}` | {record.family} | {record.action} "
                f"| {record.semantics_id} | {record.semantics_revision} |"
            )
    else:
        lines.append(
            "No operation contract record is packaged, so no specific operation "
            "can authorise a retry."
        )

    lines.append("")
    return "\n".join(lines)


def parse(text: str) -> dict:
    """Read a rendered report back into the facts it states, or refuse.

    STRICT, and the strictness is the point. A renderer with no inverse can drift
    from the registry it claims to describe and nothing notices, because the only
    check available is that the file equals the renderer's own output — which is
    true no matter what the renderer says. Parsing the served text back gives a
    test something to compare AGAINST the registry rather than against the
    renderer.

    Refuses rather than skipping: a row this cannot read is a row a reader cannot
    read either, and returning a partial view of a report is how a missing
    section becomes an empty one.
    """
    import re

    sections: dict = {"vocabulary": [], "operation_records": []}
    current = None
    for line in text.splitlines():
        heading = re.match(r"^## (.+?)\s*$", line)
        if heading:
            current = heading.group(1)
            continue
        if not line.startswith("|") or set(line) <= set("| -"):
            continue
        cells = [c.strip() for c in line.strip().strip("|").split("|")]
        if current == "Connector vocabulary":
            if cells[0] == "platform connector type":
                continue
            if len(cells) != 3:
                raise ValueError(f"vocabulary row is not three columns: {line!r}")
            sections["vocabulary"].append({
                "platform_connector_type": cells[0].strip("`"),
                "family": cells[1],
                "action_source": cells[2],
            })
        elif current == "Operation contract records":
            if cells[0] == "contract reference":
                continue
            if len(cells) != 5:
                raise ValueError(f"operation-record row is not five columns: {line!r}")
            if not cells[4].isdigit():
                raise ValueError(f"revision is not an integer: {line!r}")
            sections["operation_records"].append({
                "contract_ref": cells[0].strip("`"),
                "family": cells[1],
                "action": cells[2],
                "semantics_id": cells[3],
                "semantics_revision": int(cells[4]),
            })
    return sections


def write_report(repo_root: Path) -> Path:
    """Write the report to its tracked location and return the path."""
    target = Path(repo_root) / REPORT_RELATIVE_PATH
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(render())
    return target
