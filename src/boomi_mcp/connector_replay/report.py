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

__all__ = ["REPORT_RELATIVE_PATH", "render", "write_report"]

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

    lines.append("")
    return "\n".join(lines)


def write_report(repo_root: Path) -> Path:
    """Write the report to its tracked location and return the path."""
    target = Path(repo_root) / REPORT_RELATIVE_PATH
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(render())
    return target
