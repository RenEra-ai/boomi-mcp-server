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

    # THE KEY SEMANTICS EACH CONTRACT CITES. An operation record names a
    # semantics id and a revision; without the definitions a reader is told
    # which contract applies and not what it MEANS — the mechanism, the scope a
    # key is unique within, and what a duplicate is guaranteed to do. Those are
    # the terms on which a retry is safe, so publishing the citation without the
    # definition published the reference and withheld the contract.
    lines += ["", "## Contract key semantics", ""]
    if reg.semantics_definitions:
        lines += [
            "| semantics | revision | mechanism | key scope | duplicate guarantee |",
            "| --- | --- | --- | --- | --- |",
        ]
        for spec in sorted(reg.semantics_definitions,
                           key=lambda s: (s.semantics_id, s.revision)):
            lines.append(
                f"| `{spec.semantics_id}` | {spec.revision} | {spec.mechanism.value} "
                f"| {spec.key_scope.value} | {spec.duplicate_guarantee.value} |"
            )
    else:
        lines.append(
            "No key semantics are defined, so no contract reference can be resolved "
            "to the terms it names."
        )

    # THE PROJECTION EACH DIGEST IS TAKEN OVER. A configuration digest decides
    # whether the component a capture describes is still the component in front
    # of us, and it is computed over a CLOSED projection: what that projection
    # includes and excludes is the whole meaning of "unchanged". A report that
    # published the digests' verdicts while withholding their domain asked the
    # reader to trust a comparison whose terms were not stated.
    lines += ["", "## Component projection allowlists", ""]
    if reg.projection_allowlists:
        lines += [
            "| family | component kind | projection | included | excluded |",
            "| --- | --- | --- | --- | --- |",
        ]
        for spec in sorted(reg.projection_allowlists,
                           key=lambda s: (s.family, s.component_kind,
                                          s.projection_version)):
            # COUNTS, not the members. The members are long, unstable in width and
            # already byte-pinned in the packaged registry; what a reader needs
            # from the served text is that a projection is CLOSED and how wide it
            # is. A count that moves is a projection that moved.
            included = (len(spec.included_attributes) + len(spec.included_value_fields)
                        + len(spec.included_property_fields) + len(spec.included_elements)
                        + len(spec.included_scope_attributes))
            excluded = len(spec.excluded_fields) + len(spec.excluded_scope_attributes)
            lines.append(
                f"| {spec.family} | `{spec.component_kind}` "
                f"| v{spec.projection_version} | {included} | {excluded} |"
            )
    else:
        lines.append(
            "No projection allowlist is packaged, so no configuration digest can "
            "state the domain it was taken over."
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

    required_headings = {"Connector vocabulary", "Observed actions",
                         "Operation contract records", "Contract key semantics",
                         "Component projection allowlists"}
    sections: dict = {"vocabulary": [], "observed_actions": [], "operation_records": [],
                      "semantics_definitions": [], "projection_allowlists": []}
    seen_headings: set = set()
    current = None
    for line in text.splitlines():
        heading = re.match(r"^## (.+?)\s*$", line)
        if heading:
            current = heading.group(1)
            seen_headings.add(current)
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
        elif current == "Observed actions":
            # PARSED, not skipped. Every row under this heading was silently
            # ignored, so a renderer that dropped or corrupted the evidence table
            # still satisfied a round-trip check — the section that says what was
            # actually observed, unread by the thing verifying the report.
            if cells[0] in ("family", "connector family"):
                continue
            # EXACTLY SIX, NAMED. "At least three, remainder opaque" meant the
            # side effect, the retry safety, the execution count and the capture
            # could each be corrupted or truncated while a round-trip check still
            # passed — the four fields that say what was actually observed,
            # carried as an unread blob.
            if len(cells) != 6:
                raise ValueError(
                    f"observed-actions row is not six columns: {line!r}")
            if not cells[4].isdigit():
                raise ValueError(f"execution count is not an integer: {line!r}")
            sections["observed_actions"].append({
                "family": cells[0].strip("`"),
                "action": cells[1].strip("`"),
                "side_effect": cells[2].strip("`"),
                "retry_safety": cells[3].strip("`"),
                "executions": int(cells[4]),
                "capture": cells[5].strip("`"),
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
        elif current == "Contract key semantics":
            if cells[0] == "semantics":
                continue
            # EXACTLY FIVE, NAMED, for the same reason the observed-actions row is:
            # the mechanism, the scope and the duplicate guarantee ARE the contract,
            # so carrying them as an unread remainder would let the terms of a retry
            # drift while a round-trip check kept passing.
            if len(cells) != 5:
                raise ValueError(f"semantics row is not five columns: {line!r}")
            if not cells[1].isdigit():
                raise ValueError(f"semantics revision is not an integer: {line!r}")
            sections["semantics_definitions"].append({
                "semantics_id": cells[0].strip("`"),
                "revision": int(cells[1]),
                "mechanism": cells[2],
                "key_scope": cells[3],
                "duplicate_guarantee": cells[4],
            })
        elif current == "Component projection allowlists":
            if cells[0] == "family":
                continue
            if len(cells) != 5:
                raise ValueError(f"projection row is not five columns: {line!r}")
            if not cells[2].startswith("v") or not cells[2][1:].isdigit():
                raise ValueError(f"projection version is not a version: {line!r}")
            if not (cells[3].isdigit() and cells[4].isdigit()):
                raise ValueError(f"projection width is not a count: {line!r}")
            sections["projection_allowlists"].append({
                "family": cells[0],
                "component_kind": cells[1].strip("`"),
                "projection_version": int(cells[2][1:]),
                "included": int(cells[3]),
                "excluded": int(cells[4]),
            })
    # EVERY REQUIRED HEADING, or the report is not one. Succeeding on an empty
    # document meant a missing section and an empty section were the same result,
    # which is the failure this parser exists to make visible.
    absent = sorted(required_headings - seen_headings)
    if absent:
        raise ValueError(f"the report is missing required sections: {absent}")
    return sections


def write_report(repo_root: Path) -> Path:
    """Write the report to its tracked location and return the path."""
    target = Path(repo_root) / REPORT_RELATIVE_PATH
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(render())
    return target
