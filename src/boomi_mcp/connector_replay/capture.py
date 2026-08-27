"""Summarising an executed capture into facts a registry row can rest on.

**Why this reads artifacts and not the sidecar.** Each capture directory carries a
provenance sidecar written by the capture tooling. Measured across 31 directories,
that sidecar has EIGHT distinct shapes — one object form in twelve of them and six
different list forms elsewhere, agreeing on almost no keys. Teaching this reader all
eight would work until a ninth capture generation, and would be a hand-model of a
file whose authority is a script that has already changed six times.

The evidence is the platform's own artifacts beside it: the execution record, the
generic connector record, the readback delta, and — where one was taken — the
counterparty's access log. Those shapes are defined by the platform and by the
counterparty, not by our tooling, and they do not drift between capture rounds.

**Run labels.** Capture generations differ in one small, well-defined way: some
write ``execution_record.json`` while others write ``run1_execution_record.json``
and ``replay_execution_record.json`` for a double execution. That is a label prefix,
not a schema, and it is handled by grouping on the prefix rather than by listing the
prefixes that have been seen.
"""

from __future__ import annotations

import hashlib
import json
import re
from pathlib import Path
from typing import Any, Final

from pydantic import Field

from .ids import is_execution_id
from .models import ReplayRegistryModel

__all__ = [
    "CaptureRefused",
    "CaptureRunV1",
    "CaptureSummaryV1",
    "summarize",
]

#: Artifacts the summariser reads, by suffix. A capture may carry more; anything not
#: named here contributes to the digest but not to the derived facts.
_EXECUTION_RECORD: Final[str] = "execution_record.json"
_READBACK_DELTA: Final[str] = "readback_delta.json"
_ACCESS_LOG: Final[str] = "mock_access_log.txt"

_RUN_PREFIX: Final[re.Pattern[str]] = re.compile(r"^(?P<label>[a-z0-9]+)_(?P<rest>.+)$")

#: A uvicorn-style access line: method, target, protocol, then the status.
_ACCESS_LINE: Final[re.Pattern[str]] = re.compile(
    r'"(?P<method>[A-Z]+)\s+(?P<target>\S+)\s+HTTP/[0-9.]+"\s+(?P<status>\d{3})'
)


class CaptureRefused(Exception):
    """A capture directory could not be summarised into usable facts."""


class CaptureRunV1(ReplayRegistryModel):
    """One execution within a capture."""

    label: str = Field(min_length=1)
    execution_id: str
    status: str = Field(min_length=1)
    inbound_error_documents: int | None = None
    #: True when the readback showed the counterparty's state actually moved.
    state_changed: bool | None = None
    #: The status the COUNTERPARTY logged, when a log was captured.
    #:
    #: This is the only field that can tell a success from a refusal. The platform
    #: reports an execution as complete with zero errors even when the counterparty
    #: answered 405 — reproduced live — so ``status`` above says nothing about the
    #: HTTP outcome and must never be read as if it did.
    counterparty_status: int | None = None
    counterparty_method: str | None = None


class CaptureSummaryV1(ReplayRegistryModel):
    """Everything a registry row may be derived from, for one capture."""

    scenario: str = Field(min_length=1)
    runs: tuple[CaptureRunV1, ...] = Field(min_length=1)
    #: sha256 over every archived file's bytes, in sorted-name order.
    capture_digest: str = Field(pattern=r"^[0-9a-f]{64}$")
    file_count: int = Field(ge=1)

    @property
    def execution_ids(self) -> tuple[str, ...]:
        seen: list[str] = []
        for run in self.runs:
            if run.execution_id not in seen:
                seen.append(run.execution_id)
        return tuple(seen)

    @property
    def has_counterparty_attestation(self) -> bool:
        """True when at least one run recorded the counterparty's own status."""
        return any(run.counterparty_status is not None for run in self.runs)


def _load_json(path: Path) -> Any:
    try:
        return json.loads(path.read_text())
    except (OSError, json.JSONDecodeError) as exc:
        raise CaptureRefused(f"{path.name}: unreadable ({exc})") from exc


def _first(node: Any, key: str) -> Any:
    """Depth-first search for ``key``.

    The platform nests its query results differently across endpoints (a bare
    object here, ``data.result[0]`` there). Searching rather than pinning a path
    keeps this working across those without modelling each envelope — and the keys
    being searched for are the platform's own field names, not ours.
    """
    if isinstance(node, dict):
        if key in node:
            return node[key]
        for value in node.values():
            found = _first(value, key)
            if found is not None:
                return found
    elif isinstance(node, list):
        for value in node:
            found = _first(value, key)
            if found is not None:
                return found
    return None


def _run_label(name: str, suffix: str) -> str:
    """``run1_execution_record.json`` -> ``run1``; ``execution_record.json`` -> ``run``."""
    stem = name[: -len(suffix)].rstrip("_")
    return stem or "run"


def _counterparty_outcome(log: Path, method_hint: str | None) -> tuple[int | None, str | None]:
    """Read the status the counterparty itself logged.

    When the log carries several requests, a hint is used to pick the one for the
    verb under test. If the hint appears more than once the outcome is refused
    rather than guessed: an ambiguous attribution is not an attestation, and this
    value is what decides whether an action counts as observed at all.
    """
    try:
        lines = log.read_text().splitlines()
    except OSError as exc:
        raise CaptureRefused(f"{log.name}: unreadable ({exc})") from exc
    hits = [m for line in lines if (m := _ACCESS_LINE.search(line))]
    if not hits:
        return None, None
    if method_hint:
        for_method = [m for m in hits if m.group("method") == method_hint]
        if len(for_method) == 1:
            return int(for_method[0].group("status")), for_method[0].group("method")
        if len(for_method) > 1:
            raise CaptureRefused(
                f"{log.name}: {method_hint} appears {len(for_method)} times; the "
                "request this capture is about cannot be attributed unambiguously"
            )
    if len(hits) == 1:
        return int(hits[0].group("status")), hits[0].group("method")
    return None, None


def summarize(capture_dir: Path, method_hint: str | None = None) -> CaptureSummaryV1:
    """Derive registry-usable facts from one archived capture directory."""
    capture_dir = Path(capture_dir)
    if not capture_dir.is_dir():
        raise CaptureRefused(f"{capture_dir} is not a directory")

    files = sorted(p for p in capture_dir.iterdir() if p.is_file())
    if not files:
        raise CaptureRefused(f"{capture_dir.name} contains no files")

    digest = hashlib.sha256()
    for path in files:
        # Name AND bytes: two captures differing only in which file held which
        # payload would otherwise digest identically.
        digest.update(path.name.encode("utf-8"))
        digest.update(b"\0")
        digest.update(path.read_bytes())
        digest.update(b"\0")

    execution_files = [p for p in files if p.name.endswith(_EXECUTION_RECORD)]
    if not execution_files:
        raise CaptureRefused(
            f"{capture_dir.name}: no execution record; a capture without one records "
            "no execution and cannot support an evidence row"
        )

    deltas = {
        _run_label(p.name, _READBACK_DELTA): p
        for p in files
        if p.name.endswith(_READBACK_DELTA)
    }
    logs = [p for p in files if p.name.endswith(_ACCESS_LOG)]
    counterparty_status, counterparty_method = (None, None)
    if logs:
        counterparty_status, counterparty_method = _counterparty_outcome(logs[0], method_hint)

    runs: list[CaptureRunV1] = []
    for path in execution_files:
        label = _run_label(path.name, _EXECUTION_RECORD)
        payload = _load_json(path)
        execution_id = _first(payload, "executionId")
        if not is_execution_id(execution_id):
            raise CaptureRefused(
                f"{path.name}: no usable execution id (found {execution_id!r})"
            )
        status = _first(payload, "status")
        if not isinstance(status, str) or not status:
            raise CaptureRefused(f"{path.name}: no execution status")

        state_changed: bool | None = None
        delta_path = deltas.get(label) or (next(iter(deltas.values())) if len(deltas) == 1 else None)
        if delta_path is not None:
            changed = _first(_load_json(delta_path), "raw_changed")
            if isinstance(changed, bool):
                state_changed = changed

        runs.append(
            CaptureRunV1(
                label=label,
                execution_id=execution_id,
                status=status,
                inbound_error_documents=_first(payload, "inboundErrorDocumentCount"),
                state_changed=state_changed,
                counterparty_status=counterparty_status,
                counterparty_method=counterparty_method,
            )
        )

    return CaptureSummaryV1(
        scenario=capture_dir.name,
        runs=tuple(sorted(runs, key=lambda r: r.label)),
        capture_digest=digest.hexdigest(),
        file_count=len(files),
    )
