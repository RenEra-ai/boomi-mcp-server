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
    "ConvergenceV1",
    "CaptureRunV1",
    "CaptureSummaryV1",
    "summarize",
]

#: Artifacts the summariser reads, by suffix. A capture may carry more; anything not
#: named here contributes to the digest but not to the derived facts.
_EXECUTION_RECORD: Final[str] = "execution_record.json"
_READBACK_DELTA: Final[str] = "readback_delta.json"
_ACCESS_LOG: Final[str] = "mock_access_log.txt"
_EXECUTION_CONNECTOR: Final[str] = "execution_connector.json"

#: Staged readbacks from a double-execution capture: the state BEFORE the first
#: call, BETWEEN the two, and AFTER the second. Discovered by this shape rather
#: than by a list of stage names, so a capture adding a fourth stage is ordered
#: correctly instead of ignored.
_STAGE_READBACK: Final[re.Pattern[str]] = re.compile(
    r"^readback_(?P<stage>R\d+)_(?P<moment>[a-z]+)_(?P<subject>[a-z]+)\.json$"
)

_RUN_PREFIX: Final[re.Pattern[str]] = re.compile(r"^(?P<label>[a-z0-9]+)_(?P<rest>.+)$")

#: A uvicorn-style access line: method, target, protocol, then the status.
_ACCESS_LINE: Final[re.Pattern[str]] = re.compile(
    r'"(?P<method>[A-Z]+)\s+(?P<target>\S+)\s+HTTP/[0-9.]+"\s+(?P<status>\d{3})'
)


class CaptureRefused(Exception):
    """A capture directory could not be summarised into usable facts."""


class ConvergenceV1(ReplayRegistryModel):
    """What two identical calls did to the counterparty's state.

    Derived from the platform's own returned bodies at each staged readback, NOT
    from the capture tooling's precomputed digest beside them. The tooling's digest
    already embeds a judgement about which fields are volatile; recomputing from the
    body keeps that judgement out here, where it would be invisible.

    This reports FACTS and deliberately stops short of a verdict. Whether a replay
    that changed only a timestamp counts as idempotent is a policy question the
    registry answers with the differing field names in hand — not one this
    summariser should settle by hard-coding which fields are allowed to move.
    """

    subject: str = Field(min_length=1)
    stages: tuple[str, ...] = Field(min_length=2)
    #: The positive control: the FIRST call must actually have done something.
    #: Without this a broken call that changed nothing would look perfectly
    #: idempotent, which is the failure mode this field exists to expose.
    first_call_changed_state: bool
    #: Whether the second, identical call moved the state again.
    replay_changed_state: bool
    #: Exactly which top-level fields differed across the replay.
    fields_differing_on_replay: tuple[str, ...] = ()


class CaptureRunV1(ReplayRegistryModel):
    """One execution within a capture."""

    label: str = Field(min_length=1)
    execution_id: str
    status: str = Field(min_length=1)
    inbound_error_documents: int | None = None
    #: Documents CONSUMED — distinct from the error count, which is what an earlier
    #: version read and which is zero for a healthy input-consuming run.
    inbound_documents: int | None = None
    #: Documents the connector produced. A counterparty log proves a request
    #: reached the endpoint; only this proves anything came back.
    outbound_documents: int | None = None
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
    #: Connector types the PLATFORM recorded for these executions, sentinels excluded.
    #: This is what a capture observed, as distinct from what a caller says it is.
    observed_connector_types: tuple[str, ...] = ()
    #: Every HTTP method the capture's components declare. A capture commonly holds
    #: more than one — a source operation that fetches, and the operation under
    #: test — so this is a SET, and which member is the subject is not determinable
    #: from the components alone. Reconciliation therefore requires a declared
    #: action to be a member, and requires exact agreement with the counterparty log
    #: wherever one was taken. Evidence-proportional: strong where the evidence is,
    #: still non-vacuous where it is not.
    observed_methods: tuple[str, ...] = ()
    #: Provenance READ FROM THE CAPTURE rather than from its directory name.
    captured_at: str | None = None
    account_id: str | None = None
    #: Whether the connector ran as the process entry. Observed, not assumed: the
    #: archived double-execution target runs downstream of a fetch.
    is_start_shape: bool | None = None
    #: sha256 over every archived file's bytes, in sorted-name order.
    capture_digest: str = Field(pattern=r"^[0-9a-f]{64}$")
    file_count: int = Field(ge=1)
    #: Present only when the capture staged readbacks around a double execution.
    convergence: tuple[ConvergenceV1, ...] = ()

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


def _first_across(files: list[Path], key: str, want: type | tuple = object) -> Any:
    """The first value for ``key`` across the capture's JSON artifacts.

    Used for provenance the capture RECORDS — the execution's time, its account,
    whether the connector was the entry shape — rather than values inferred from a
    directory name or assumed by the reader.
    """
    for path in sorted(files):
        if path.suffix != ".json":
            continue
        try:
            found = _first(_load_json(path), key)
        except CaptureRefused:
            continue
        # TYPE-CHECKED. The platform reuses key names at different shapes — an
        # `account` appears both as an id string and as a nested object — so taking
        # the first match of any shape picked up the wrong one.
        if found is not None and isinstance(found, want):
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
            # NOT an error. A double-execution capture NECESSARILY logs the method
            # once per execution, and refusing on multiplicity made the very
            # evidence a replay verdict requires unusable. The count is reconciled
            # against the number of executions by the caller; here we report the
            # FIRST outcome and let per-run attribution place the rest.
            return int(for_method[0].group("status")), for_method[0].group("method")
        # ZERO matches. Previously this fell through, and a log holding a single
        # UNRELATED request returned that request's status — which the ingest then
        # paired with the DECLARED action. A successful read could therefore mint an
        # idempotent row for a delete that never ran. A declared method with no
        # matching request is not weak evidence; it is evidence of the wrong thing.
        raise CaptureRefused(
            f"{log.name}: no {method_hint} request appears in the counterparty log, "
            "so this capture attests nothing about that method. Another request's "
            "status must never be borrowed for it"
        )
    if len(hits) == 1:
        return int(hits[0].group("status")), hits[0].group("method")
    return None, None


def _status_of(payload: Any) -> int | None:
    """The readback's HTTP status, when it recorded one."""
    value = payload.get("status") if isinstance(payload, dict) else None
    return value if isinstance(value, int) else None


def _body_of(payload: Any) -> dict[str, Any] | None:
    body = payload.get("body") if isinstance(payload, dict) else None
    return body if isinstance(body, dict) else None


_ABSENT = object()


def _differing_keys(a: dict[str, Any], b: dict[str, Any]) -> tuple[str, ...]:
    """Keys whose value differs, counting ABSENCE as different from a present null.

    `a.get(k)` returns None for both "not there" and "there and null", so a replay
    that ADDED a null-valued field reported no difference at all — and that result
    feeds the convergence verdict. A sentinel keeps the two apart.
    """
    return tuple(sorted(
        k for k in set(a) | set(b) if a.get(k, _ABSENT) != b.get(k, _ABSENT)
    ))


def _convergence(files: list[Path]) -> tuple[ConvergenceV1, ...]:
    """Derive what a replay did, per subject, from the staged readbacks."""
    by_subject: dict[str, list[tuple[str, Path]]] = {}
    for path in files:
        m = _STAGE_READBACK.match(path.name)
        if m:
            by_subject.setdefault(m.group("subject"), []).append((m.group("stage"), path))

    results: list[ConvergenceV1] = []
    for subject, staged in sorted(by_subject.items()):
        # Order by the stage NUMBER, not lexically: R10 must not sort before R2.
        staged.sort(key=lambda pair: int(pair[0][1:]))
        if len(staged) < 3:
            continue
        bodies = [(stage, _body_of(_load_json(path))) for stage, path in staged]
        if any(body is None for _, body in bodies):
            raise CaptureRefused(
                f"{subject}: a staged readback carries no body object, so what the "
                "replay did cannot be derived"
            )
        before, between, after = bodies[0][1], bodies[-2][1], bodies[-1][1]
        statuses = [_status_of(_load_json(path)) for _, path in staged]

        # ABSENCE is a state, and two absences are the same state. When a readback
        # is non-2xx the resource is not there, and comparing the two error BODIES
        # then reports a difference that is not about the resource at all — the
        # archived delete capture returns 404 at both stages with only the error's
        # own `timestamp` differing, which read as a second effect and turned an
        # effect-idempotent delete into a non-idempotent one.
        #
        # Deliberately narrower than widening the volatile-field list: that list
        # governs fields on a resource that EXISTS, and widening it is how a real
        # side effect gets reclassified as noise.
        def moved(a_status, a_body, b_status, b_body):
            if a_status is not None and b_status is not None:
                a_present = 200 <= a_status < 300
                b_present = 200 <= b_status < 300
                if a_present != b_present:
                    return True, ("<resource presence>",)
                if not a_present and not b_present:
                    return False, ()
            keys = _differing_keys(a_body, b_body)
            return bool(keys), keys

        first_moved, _ = moved(statuses[0], before, statuses[-2], between)
        replay_moved, replay_keys = moved(statuses[-2], between, statuses[-1], after)
        results.append(
            ConvergenceV1(
                subject=subject,
                stages=tuple(stage for stage, _ in staged),
                first_call_changed_state=first_moved,
                replay_changed_state=replay_moved,
                fields_differing_on_replay=replay_keys,
            )
        )
    return tuple(results)


def _platform_connector_records(payload: Any) -> list[dict]:
    """The platform's own connector records inside a captured query response.

    Found by SHAPE — an object carrying both `executionId` and `connectorType` —
    rather than by a fixed path, because the capture generations wrap the platform
    response differently. The shape is the platform's; the wrapper is ours.
    """
    found: list[dict] = []

    def walk(node: Any) -> None:
        if isinstance(node, dict):
            if "connectorType" in node and "executionId" in node:
                found.append(node)
                return
            for child in node.values():
                walk(child)
        elif isinstance(node, list):
            for child in node:
                walk(child)

    walk(payload)
    return found


def _observed_connector_types(
    files: list[Path], execution_ids: frozenset[str]
) -> tuple[str, ...]:
    """Connector types the platform recorded FOR THIS CAPTURE'S EXECUTIONS.

    The execution id is the causal tie, and requiring it is the point: without it
    any file in the directory carrying a `connectorType` lent its authority to the
    capture, so an artifact from an unrelated execution — or a bare
    ``{"connectorType": ...}`` object — could name the connector a row was minted
    for. A record that does not mention this capture's executions is not evidence
    about them.
    """
    from .models import EXECUTION_SENTINELS

    seen: set[str] = set()
    for path in files:
        # By CONTENT, not by filename. A suffix check missed
        # `run1_execution_connector_raw.json` — the shape the double-execution
        # captures use — so the records this correlation depends on were skipped
        # entirely and the capture refused. Third time filename coupling has broken
        # a reader in this slice; the file's NAME is our tooling's, its CONTENT is
        # the platform's.
        if path.suffix != ".json":
            continue
        try:
            payload = _load_json(path)
        except CaptureRefused:
            continue
        if not _platform_connector_records(payload):
            continue

        # PER RECORD, from the PLATFORM's own records. The capture tooling also
        # writes a flattened `rows` array, and that array DROPS `executionId` —
        # the very field that ties a row to an execution. Reading it would have
        # made per-record correlation impossible and per-file correlation the only
        # option, which is what allowed a file holding an own-execution row beside
        # a foreign-execution row to pass wholesale.
        #
        # A record is evidence about the execution it names, and about no other.
        records = _platform_connector_records(payload)
        if not records:
            raise CaptureRefused(
                f"{path.name} carries no platform connector records, so there is "
                "nothing whose execution can be checked"
            )
        for record in records:
            execution = record.get("executionId")
            if not isinstance(execution, str):
                raise CaptureRefused(
                    f"{path.name}: a connector record names no execution, so "
                    "nothing ties it to this capture"
                )
            if execution not in execution_ids:
                raise CaptureRefused(
                    f"{path.name}: a connector record belongs to execution "
                    f"{execution!r}, which is not one of this capture's "
                    f"{sorted(execution_ids)!r}"
                )
            value = record.get("connectorType")
            if isinstance(value, str) and value not in EXECUTION_SENTINELS:
                seen.add(value)
    return tuple(sorted(seen))


def _observed_methods(files: list[Path]) -> tuple[str, ...]:
    """Every verb the capture's components declare.

    Read from the component rather than from the execution record, because the
    platform reports one generic action for all eight verbs — measured across 95
    rows. A capture's method is therefore only knowable from the component.
    """
    import re

    # Selected by CONTENT, not by filename. The first version required "operation"
    # in the name, which silently excluded the archived PATCH captures — they use
    # `component_op_patch.xml` — so their method read as absent and the
    # reconciliation this feeds REFUSED the very captures slice F depends on. A
    # naming convention is not evidence; that sentence is already written a few
    # lines below about the action, and it applies here too.
    found: set[str] = set()
    for path in sorted(files):
        if path.suffix != ".xml":
            continue
        found.update(re.findall(r'customOperationType="([^"]+)"', path.read_text()))
    return tuple(sorted(found))


def _counterparty_outcomes(log: Path, method_hint: str | None) -> list[tuple[int, str]]:
    """Every logged outcome for the method under test, in chronological order."""
    hits = [m for line in log.read_text().splitlines() if (m := _ACCESS_LINE.search(line))]
    if method_hint:
        hits = [m for m in hits if m.group("method") == method_hint]
    return [(int(m.group("status")), m.group("method")) for m in hits]


def _counterparty_request_count(log: Path, method_hint: str | None) -> int:
    """How many requests the counterparty actually logged for the method under test."""
    hits = [m for line in log.read_text().splitlines() if (m := _ACCESS_LINE.search(line))]
    if method_hint:
        return sum(1 for m in hits if m.group("method") == method_hint)
    return len(hits)


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
    if len(logs) > 1:
        raise CaptureRefused(
            "the capture carries more than one counterparty log and there is no "
            "rule for which execution each belongs to"
        )
    counterparty_status, counterparty_method = (None, None)
    logged_request_count = 0
    if logs:
        counterparty_status, counterparty_method = _counterparty_outcome(logs[0], method_hint)
        logged_request_count = _counterparty_request_count(logs[0], method_hint)

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
                inbound_documents=_first(payload, "inboundDocumentCount"),
                outbound_documents=_first(payload, "outboundDocumentCount"),
                state_changed=state_changed,
                counterparty_status=counterparty_status,
                counterparty_method=counterparty_method,
            )
        )

    # ONE observed request cannot attest TWO executions. The outcome above is
    # copied onto every run, which is correct only when the counterparty logged as
    # many requests as there were executions. Otherwise the second execution is
    # unattested, and a replay verdict drawn from it would rest on a request nobody
    # observed. Drop the attribution rather than spread it.
    if logs and logged_request_count < len(runs):
        runs = [
            run.model_copy(update={"counterparty_status": None, "counterparty_method": None})
            for run in runs
        ]
    elif logs and logged_request_count == len(runs) and len(runs) > 1:
        # The counts agree, so each execution has its own observed request. Attribute
        # them IN ORDER — the log is chronological and the runs are label-sorted,
        # which is the same order the executions ran in. Copying one outcome onto
        # every run would have claimed the second execution returned what the first
        # did, and for the archived DELETE capture that is false: 204 then 404.
        outcomes = _counterparty_outcomes(logs[0], method_hint)
        if len(outcomes) == len(runs):
            runs = [
                run.model_copy(update={"counterparty_status": status,
                                       "counterparty_method": method})
                for run, (status, method) in zip(runs, outcomes)
            ]

    return CaptureSummaryV1(
        scenario=capture_dir.name,
        runs=tuple(sorted(runs, key=lambda r: r.label)),
        capture_digest=digest.hexdigest(),
        file_count=len(files),
        convergence=_convergence(files),
        observed_connector_types=_observed_connector_types(
            files, frozenset(run.execution_id for run in runs)),
        observed_methods=_observed_methods(files),
        captured_at=_first_across(files, "executionTime", str),
        account_id=_first_across(files, "account", str),
        is_start_shape=_first_across(files, "isStartShape", bool),
    )
