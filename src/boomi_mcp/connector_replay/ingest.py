"""Turning archived captures into registry rows.

This module lands DARK. It is exercised by tests against the real archive, but the
packaged registry it would write ships empty, and the step that actually fills it
belongs to a later slice. That ordering is deliberate: a mechanism that mints rows
is worth reviewing before it has minted any.

**Integrity comes from one place.** Every candidate file is verified against the
archive's single top-level checksum manifest before a byte of it is interpreted.
Not against per-record digest fields inside the captures — only some capture
generations carry those, they are written by the same tooling that wrote the file
they attest, and a digest that travels with its own payload attests nothing.

**A capture that cannot be verified is REFUSED, never skipped.** Skipping would turn
a tampered or truncated capture into a silently smaller registry, and a smaller
registry fails closed — which looks like success. The failure would be invisible in
exactly the direction that matters.
"""

from __future__ import annotations

import hashlib
from pathlib import Path
from typing import Iterable

from .capture import CaptureRefused, CaptureSummaryV1, summarize
from .models import (
    CapabilityEvidenceRecordV1,
    RetrySafetyV1,
    SideEffectV1,
)

__all__ = [
    "IngestRefused",
    "VerifiedCapture",
    "verify_archive",
    "classify",
    "ingest",
]

_MANIFEST = "SHA256SUMS"

#: Statuses the counterparty may return that mean "the request was understood and
#: acted on". A 4xx or 5xx means the action was NOT observed, whatever the platform
#: reported about the execution.
_SUCCESS_RANGE = range(200, 300)


class IngestRefused(Exception):
    """A capture could not be turned into a registry row."""


class VerifiedCapture:
    """A capture directory whose bytes matched the archive manifest."""

    __slots__ = ("directory", "summary")

    def __init__(self, directory: Path, summary: CaptureSummaryV1) -> None:
        self.directory = directory
        self.summary = summary


def _manifest_digests(archive_root: Path) -> dict[str, str]:
    manifest = archive_root / _MANIFEST
    if not manifest.is_file():
        raise IngestRefused(
            f"no {_MANIFEST} at {archive_root}; without the archive manifest there "
            "is nothing to verify these captures against, and an unverified capture "
            "is not evidence"
        )
    digests: dict[str, str] = {}
    for line in manifest.read_text().splitlines():
        line = line.strip()
        if not line:
            continue
        # `sha256  relative/path` — the separator is two spaces, but a path may
        # itself contain spaces, so split once from the left only.
        parts = line.split(None, 1)
        if len(parts) != 2:
            raise IngestRefused(f"{_MANIFEST}: unparseable line {line!r}")
        digest, rel = parts
        digests[rel.lstrip("*").strip()] = digest.lower()
    if not digests:
        raise IngestRefused(f"{_MANIFEST} lists no files")
    return digests


def verify_archive(archive_root: Path, capture_dir: Path) -> None:
    """Refuse unless every file in ``capture_dir`` matches the archive manifest."""
    archive_root = Path(archive_root)
    capture_dir = Path(capture_dir)
    digests = _manifest_digests(archive_root)

    unlisted: list[str] = []
    mismatched: list[str] = []
    for path in sorted(p for p in capture_dir.rglob("*") if p.is_file()):
        rel = str(path.relative_to(archive_root))
        expected = digests.get(rel)
        if expected is None:
            unlisted.append(rel)
            continue
        actual = hashlib.sha256(path.read_bytes()).hexdigest()
        if actual != expected:
            mismatched.append(rel)

    if unlisted or mismatched:
        raise IngestRefused(
            "capture {0} does not match the archive manifest — unlisted: {1}; "
            "digest mismatch: {2}. A capture is evidence only while its bytes are "
            "the bytes that were archived.".format(capture_dir.name, unlisted, mismatched)
        )


def classify(summary: CaptureSummaryV1) -> tuple[SideEffectV1, RetrySafetyV1]:
    """Decide what a capture proves, from its derived facts alone.

    The rules, and why each is conservative:

    * A capture with no counterparty attestation proves nothing about the action.
      The platform reports an execution as complete with zero errors even when the
      counterparty refused the request — reproduced live — so an execution status
      cannot stand in for an outcome.
    * A non-2xx status means the action was not performed. It is not evidence that
      the action is safe; it is evidence that nothing happened.
    * A state change observed at the target means WRITE. No state change means READ
      only when there is an attested success to go with it — otherwise the absence
      of change is equally consistent with the call never landing.
    * Replay safety is claimed only from a real double execution WITH a positive
      control. Without the control, "the replay changed nothing" is unfalsifiable.
    """
    statuses = [r.counterparty_status for r in summary.runs if r.counterparty_status is not None]
    if not statuses:
        return SideEffectV1.UNKNOWN, RetrySafetyV1.UNVERIFIED
    if any(s not in _SUCCESS_RANGE for s in statuses):
        return SideEffectV1.UNKNOWN, RetrySafetyV1.UNVERIFIED

    changed = [r.state_changed for r in summary.runs if r.state_changed is not None]
    convergence = [c for c in summary.convergence if c.first_call_changed_state]

    if convergence:
        # A double execution with a positive control: the first call moved state,
        # so the second one's behaviour is meaningful.
        only_volatile = all(not c.replay_changed_state or set(c.fields_differing_on_replay) <= {"modifiedOn"}
                            for c in convergence)
        return (
            SideEffectV1.WRITE,
            RetrySafetyV1.CONDITIONALLY_IDEMPOTENT if only_volatile else RetrySafetyV1.NON_IDEMPOTENT,
        )
    if any(changed):
        return SideEffectV1.WRITE, RetrySafetyV1.UNVERIFIED
    if changed:
        return SideEffectV1.READ, RetrySafetyV1.IDEMPOTENT
    return SideEffectV1.UNKNOWN, RetrySafetyV1.UNVERIFIED


def ingest(
    archive_root: Path,
    capture_dirs: Iterable[Path],
    *,
    family: str,
    actions: dict[str, str],
) -> tuple[CapabilityEvidenceRecordV1, ...]:
    """Verify, summarise and classify captures into evidence rows.

    ``actions`` maps a capture directory name to the action it exercised. It is
    required rather than inferred from the directory name: a naming convention is
    not evidence, and a row that named the wrong action would authorise retrying
    something never observed.
    """
    archive_root = Path(archive_root)
    rows: list[CapabilityEvidenceRecordV1] = []
    for directory in sorted(Path(d) for d in capture_dirs):
        action = actions.get(directory.name)
        if action is None:
            raise IngestRefused(
                f"{directory.name}: no action declared. The action is not inferred "
                "from the directory name — a naming convention is not evidence"
            )
        verify_archive(archive_root, directory)
        try:
            summary = summarize(directory, method_hint=action)
        except CaptureRefused as exc:
            raise IngestRefused(f"{directory.name}: {exc}") from exc

        side_effect, retry_safety = classify(summary)
        operation_id = None
        if retry_safety is RetrySafetyV1.CONDITIONALLY_IDEMPOTENT:
            # The model requires it, and rightly: convergence was observed against
            # one operation and does not transfer to another.
            operation_id = _operation_component_id(directory)
            if operation_id is None:
                raise IngestRefused(
                    f"{directory.name}: convergence was observed but the operation "
                    "component it holds for could not be identified"
                )
        rows.append(
            CapabilityEvidenceRecordV1(
                family=family,
                action=action,
                side_effect=side_effect,
                retry_safety=retry_safety,
                capture_digest=summary.capture_digest,
                execution_ids=summary.execution_ids,
                operation_component_id=operation_id,
            )
        )
    return tuple(rows)


def _operation_component_id(directory: Path) -> str | None:
    """The component id of the operation a capture exercised, from its XML."""
    import re

    from .ids import is_boomi_component_id

    for path in sorted(directory.glob("*operation*.xml")):
        for match in re.finditer(r'componentId="([^"]+)"|\bid="([0-9a-fA-F-]{36})"', path.read_text()):
            value = match.group(1) or match.group(2)
            if is_boomi_component_id(value):
                return value
    return None
