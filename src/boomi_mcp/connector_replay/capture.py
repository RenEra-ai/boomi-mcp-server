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
import xml.etree.ElementTree as ET
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
#: call, BETWEEN the two, and AFTER the second. Discovered by this shape rather than
#: by a list of stage names — but a capture adding a FOURTH stage is now refused, not
#: "ordered correctly": the moments below are a closed set, and this comment promised
#: the opposite of what the reader does for two rounds after that changed.
_STAGE_READBACK: Final[re.Pattern[str]] = re.compile(
    r"^readback_(?P<stage>R\d+)_(?P<moment>[a-z]+)_(?P<subject>[a-z]+)\.json$"
)

#: The moments a convergence comparison is placed on, and the order they run in. A
#: closed set on purpose: these are the boundaries of the two comparison windows, so
#: a capture that records a different set has not recorded THIS measurement.
_CONVERGENCE_ORDER: Final[tuple[str, ...]] = ("before", "between", "after")
_CONVERGENCE_MOMENTS: Final[frozenset[str]] = frozenset(_CONVERGENCE_ORDER)

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
    #: Documents the CONNECTOR UNDER TEST processed, and the bytes it moved, summed
    #: over its own correlated platform rows. Distinct from the execution's counts,
    #: which are process-level sums over every connector that ran: the archived
    #: captures report `outboundDocumentCount` 2 for an execution in which the
    #: connector under test reports `successCount` 1, the other document being the
    #: source read's. A connector-level claim derived from the process-level sum is
    #: a claim about the wrong subject. Counted in DOCUMENTS, because the vocabulary
    #: these feed is document-based and a successful call can return zero bytes.
    connector_documents: int | None = Field(default=None, ge=0)
    connector_successful_documents: int | None = Field(default=None, ge=0)
    #: Documents a Return Documents shape received. A DIFFERENT SUBJECT again: the
    #: connector completing successfully does not mean a receiver got its output, and
    #: the vocabulary has a separate value for each. The archived read-verb captures
    #: carry no receiver row, and the two attested write captures do.
    return_documents: int | None = Field(default=None, ge=0)
    #: Whether the operation under test PRODUCED what that receiver counted, per the
    #: archived process graph: it reaches the receiver with no other connector action
    #: in between. Merely reaching it is not enough — in a linear process every
    #: upstream operation reaches the receiver. None means the archive cannot say.
    receiver_is_downstream: bool | None = None
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


#: The fields every archived connector row is READ for. Two copies of one platform row
#: must agree on exactly these: reconciling fields nobody reads would refuse captures
#: over differences that change no observation, and reconciling fewer would let a read
#: field vary by filename. Hand-listing it is what let `connectorType` fall out — the
#: family reconciliation consumes it — so the set is PINNED to the module's own reads
#: by a guard that derives them from this file, rather than trusted to stay in step.
_CORRELATED_ROW_FIELDS: Final = (
    "connectorType",
    "errorCount",
    "executionConnector",
    "executionId",
    "isStartShape",
    "successCount",
)


def _component_name_under_test(files: list[Path], method_hint: str | None) -> str | None:
    """The name half of the resolution below, for the row join."""
    resolved = _component_under_test(files, method_hint)
    return None if resolved is None else resolved[1]


def _component_under_test(
    files: list[Path], method_hint: str | None
) -> tuple[str, str] | None:
    """The NAME of the operation component whose verb is the one under test.

    THE JOIN KEY, and the reason this function exists at all. A capture holds more
    than one connector component — every archived one pairs a source read with the
    verb being tested — and the platform's own connector rows are printed per step,
    so any observation taken by scanning rows describes whichever component the scan
    happened to reach. That is not an observation of the connector under test.

    The component is resolved by its DECLARED VERB, the same authority
    ``_observed_methods`` reads, never by filename and never by a naming convention:
    the step names in the archive happen to spell their verb, and reading that would
    be a hand-model of a test harness's habit rather than a fact the platform
    published. Zero or several components declaring the verb is an unresolved
    correlation and returns None — the callers refuse rather than pick.
    """
    if not method_hint:
        return None
    # Keyed on the COMPONENT ID, not on the name. Deduping by name would collapse
    # two distinct components that happen to share one — and they can: measured live
    # on the account, component CREATE uniquifies a duplicate name but component
    # UPDATE does not, so two ids can carry the same name. Counting names would then
    # report "uniquely resolved" for a genuinely ambiguous capture, which is the
    # fail-open direction. Counting components refuses it.
    by_component: dict[str, str] = {}
    owners: dict[str, set[str]] = {}
    for path in sorted(files):
        if path.suffix != ".xml":
            continue
        try:
            root = ET.parse(path).getroot()
        except (OSError, ET.ParseError):
            continue
        name = root.get("name")
        component_id = root.get("componentId")
        if not name or not component_id:
            continue
        # Ownership is contested only among components that can PRODUCE a connector
        # row — those declaring an operation type. A process or profile archived
        # beside them cannot appear in the platform's connector rows, so counting it
        # as a rival owner refused captures it could never have corrupted.
        declared = [el.get("customOperationType") for el in root.iter()
                    if el.get("customOperationType")]
        if not declared:
            continue
        owners.setdefault(name, set()).add(component_id)
        if method_hint in declared:
            by_component[component_id] = name
    if len(by_component) != 1:
        return None
    component_id, name = next(iter(by_component.items()))
    # The name must be owned by exactly ONE component across the WHOLE capture, not
    # merely one among those declaring this verb. Guarding only the verb's own
    # components protects the RESOLUTION and leaves the JOIN wide: the rows below
    # match on the name alone, so a source component sharing the target's name — the
    # platform's UPDATE path permits it — would lend its documents to the target's
    # observations, and placement would not refuse because both normally run
    # downstream. That is the same uncorrelated read one level out.
    if len(owners.get(name, ())) != 1:
        return None
    return component_id, name


def _connector_rows_under_test(
    files: list[Path], execution_ids: frozenset[str], method_hint: str | None
) -> tuple[dict, ...] | None:
    """The platform's connector rows FOR THE CONNECTOR UNDER TEST, or None.

    The single correlated selector every per-connector observation reads from.
    Rows are matched on the platform's own ``executionConnector`` field against the
    name of the component resolved above, so the selection is a join between two
    artifacts the platform authored rather than a filter that admits whatever else
    ran in the same execution.

    None means the correlation could not be made — no method hint, no uniquely
    resolvable component, or no row bearing its name. Callers must refuse on None;
    none of them may substitute a default, because a default here is a machine-served
    claim about a connector nobody observed.
    """
    step_name = _component_name_under_test(files, method_hint)
    if not step_name:
        return None
    rows = _reconciled_platform_rows(files, execution_ids)
    selected = [row for row in rows if row.get("executionConnector") == step_name]
    return tuple(selected) or None


def _reconciled_platform_rows(
    files: list[Path], execution_ids: frozenset[str]
) -> tuple[dict, ...]:
    """Every archived platform connector row, deduped by id and reconciled.

    RECONCILED BEFORE THE JOIN, and the order is the whole point. Doing this after
    filtering made two of the reconciled fields structurally unreachable — a row is
    only compared against another row that already matched the same execution and the
    same step, so two copies disagreeing about WHICH step a row belongs to could never
    meet, and the disagreement resolved silently toward the connector under test. That
    is the reconciler failing at exactly the case it was added for.

    Captures archive the same query twice — a raw copy beside a requery — and the
    manifest validates each file on its own, so nothing else would notice the two
    disagreeing; keeping whichever file sorted first would make a served observation
    depend on a filename.

    Rows naming a FOREIGN execution are skipped rather than reconciled. A record that
    does not mention this capture's executions is not evidence about them, so it is
    not a rival copy of anything here — and the rule that refuses it belongs to the
    connector-type reconciliation, which reports it in those terms. Reconciling it
    here would answer a question about causal ties with a message about disagreeing
    copies.
    """
    by_row: dict[str, dict] = {}
    for path in sorted(files):
        if path.suffix != ".json":
            continue
        try:
            payload = _load_json(path)
        except CaptureRefused:
            continue
        for record in _platform_connector_records(payload):
            if record.get("executionId") not in execution_ids:
                continue
            key = record.get("id")
            if not isinstance(key, str):
                # No platform id to reconcile on: keep it, but keyed so two such
                # rows from one file cannot collapse into one.
                key = f"{path.name}#{len(by_row)}"
            seen = by_row.get(key)
            if seen is None:
                by_row[key] = record
                continue
            differing = sorted(
                field for field in _CORRELATED_ROW_FIELDS
                if seen.get(field) != record.get(field)
            )
            if differing:
                raise CaptureRefused(
                    f"{path.name}: two archived copies of platform row {key!r} "
                    f"disagree on {differing!r}; which one describes the execution "
                    "is not decidable from the archive"
                )
    return tuple(by_row.values())


def _connector_document_counts(
    files: list[Path], execution_ids: frozenset[str], method_hint: str | None
) -> tuple[int, int] | None:
    """(documents handled, documents handled successfully) for the connector under test.

    DOCUMENTS, not bytes. An earlier form of this read `size`, which misreads exactly
    the read verbs this slice exists to evidence: the archived HEAD capture records a
    successful connector document of zero bytes — the platform even archives its
    download — so a byte test published "no output observed" for a call that did
    return a document. The A9 vocabulary the value feeds is document-based.

    Every correlated row must carry both counts as non-negative integers. A missing
    field is REFUSED rather than skipped: dropping it left the sum None, which the
    observations then read as zero and published as an affirmative absence — an
    incomplete capture asserting that nothing was consumed. A negative is refused for
    the same reason, since these feed `> 0` decisions and a corrupt row could sum
    back under the threshold.
    """
    rows = _connector_rows_under_test(files, execution_ids, method_hint)
    if rows is None:
        return None
    handled = succeeded = 0
    for row in rows:
        counts = {}
        for field in ("successCount", "errorCount"):
            value = row.get(field)
            if not isinstance(value, int) or isinstance(value, bool):
                raise CaptureRefused(
                    f"connector row {row.get('id')!r} carries no usable {field!r} "
                    f"({value!r}); a connector-level observation cannot be derived "
                    "from a row that does not report it"
                )
            if value < 0:
                raise CaptureRefused(
                    f"connector row {row.get('id')!r} reports a negative {field!r} "
                    f"({value}); the count feeds an absence decision and a negative "
                    "would sum back under it"
                )
            counts[field] = value
        handled += counts["successCount"] + counts["errorCount"]
        succeeded += counts["successCount"]
    return handled, succeeded


def _reachable_receivers(
    files: list[Path], method_hint: str | None, process_id: str | None
) -> frozenset[str] | None:
    """The Return Documents shapes the operation under test PRODUCES for.

    Three corrections live in this one function and each was a different wrong
    question, so they are worth keeping named.

    THE EXECUTED GRAPH, not the first one on disk. A capture may archive more than one
    process — the archive already holds a directory carrying an emitted graph beside
    the stored one, and the emitted copy has no component id and sorts first, so
    taking the first match read a graph that never ran. The execution record names the
    process it ran, and the process component carries that id.

    PRODUCED, not merely REACHED. The traversal stops at any other connector action:
    documents entering a receiver were produced by the last connector on the path in,
    and in a linear process every upstream operation reaches the receiver otherwise.

    WHICH receivers, not whether any. Returning a bare yes let the caller count rows
    from every receiver in the execution, so a reachable receiver reporting nothing
    plus an unrelated branch reporting documents read as a delivery. The shape names
    are carried out so the counting binds to the same receivers this walk found.

    None means the archive cannot say: no execution names a process, no archived
    process component is the one that ran, or its graph does not contain this
    operation. Empty means it ran and produces for no receiver.
    """
    resolved = _component_under_test(files, method_hint)
    if resolved is None or not process_id:
        return None
    component_id, _ = resolved

    # RECONCILED, like the connector rows are. Two archived copies of ONE process
    # component that disagree would otherwise let filename sort order decide a served
    # answer — the same defect the row reader closed, in the artifact beside it, which
    # is where the sweep for that fix should have reached.
    graphs: dict[str, tuple[Path, str]] = {}
    for path in sorted(files):
        if path.suffix != ".xml":
            continue
        try:
            body = path.read_bytes()
            root = ET.parse(path).getroot()
        except (OSError, ET.ParseError):
            continue
        if root.get("type") != "process" or root.get("componentId") != process_id:
            continue
        canonical = ET.canonicalize(body.decode("utf-8", "replace"), strip_text=True)
        seen = graphs.get(process_id)
        if seen is not None and seen[1] != canonical:
            raise CaptureRefused(
                f"{path.name}: two archived copies of process {process_id!r} differ, "
                f"and {seen[0].name} sorts first; which graph ran is not decidable "
                "from the archive"
            )
        graphs[process_id] = (path, canonical)

    for path, _canonical in graphs.values():
        root = ET.parse(path).getroot()

        shapes: dict[str, dict] = {}
        for element in root.iter():
            if element.tag.split("}")[-1] != "shape":
                continue
            name = element.get("name")
            if not name:
                continue
            successors, operations = [], []
            for child in element.iter():
                local = child.tag.split("}")[-1]
                if local == "dragpoint" and child.get("toShape"):
                    successors.append(child.get("toShape"))
                elif local == "connectoraction" and child.get("operationId"):
                    operations.append(child.get("operationId"))
            shapes[name] = {
                "type": element.get("shapetype"),
                "successors": successors,
                "operations": operations,
            }

        origins = [n for n, sh in shapes.items() if component_id in sh["operations"]]
        if not origins:
            return None

        receivers: set[str] = set()
        seen, frontier = set(origins), list(origins)
        while frontier:  # cycle-guarded: an authored graph need not be acyclic
            current = frontier.pop()
            for successor in shapes.get(current, {}).get("successors", ()):
                if successor in seen:
                    continue
                seen.add(successor)
                successor_type = shapes.get(successor, {}).get("type")
                if successor_type == "returndocuments":
                    receivers.add(successor)
                    continue
                if successor_type == "connectoraction":
                    continue
                frontier.append(successor)
        return frozenset(receivers)
    return None


def _return_documents(
    files: list[Path],
    execution_ids: frozenset[str],
    receivers: frozenset[str] | None,
) -> int | None:
    """Documents THIS operation's receivers reported, or None when unknowable.

    Bound to the receivers the graph walk actually found. Summing every receiver in
    the execution let another branch's delivery stand in for this operation's, which
    is the same substitution the walk above was corrected for, one step later.
    """
    present = [row for row in _reconciled_platform_rows(files, execution_ids)
               if row.get("connectorType") == "return"]
    if not present:
        return 0
    if receivers is None:
        # Receivers ran, and the archive cannot say which are this operation's. An
        # unknown, not a zero — the caller refuses.
        return None
    ours = [row for row in present if row.get("executionConnector") in receivers]
    if not ours:
        return 0
    counted = [row for row in ours
               if isinstance(row.get("successCount"), int)
               and not isinstance(row.get("successCount"), bool)]
    if not counted:
        return None
    for row in counted:
        if row["successCount"] < 0:
            raise CaptureRefused(
                f"receiver row {row.get('id')!r} reports a negative document count "
                f"({row['successCount']}); the value feeds a delivery decision"
            )
    return sum(row["successCount"] for row in counted)


def _start_shape_of_the_exercised_connector(
    files: list[Path], execution_ids: frozenset[str], method_hint: str | None = None
) -> bool | None:
    """Whether the CONNECTOR UNDER TEST ran as the process entry.

    Reads the flag off the correlated rows only. Two earlier shapes of this
    function were wrong in the same way and are worth naming, because the second
    looked like a fix for the first: reading whichever flag came first reported the
    `nodata` sentinel's placement, and aggregating every non-sentinel row in the
    execution reported a set spanning the source read AND the verb under test. Both
    happened to agree with the archive, which is why both passed — every archived
    capture runs its connector downstream of a start shape. Neither had correlated
    anything.

    Disagreement among the correlated rows, or no correlation at all, returns None
    and the placement is refused downstream.
    """
    rows = _connector_rows_under_test(files, execution_ids, method_hint)
    if rows is None:
        return None
    flags = {r["isStartShape"] for r in rows if isinstance(r.get("isStartShape"), bool)}
    if len(flags) == 1:
        return next(iter(flags))
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


def _state_change_for_run(
    own: Path | None,
    deltas: dict[str, Path],
    label: str,
    execution_id: str,
) -> bool | None:
    """Whether THIS run changed state, from the delta entry that names it.

    Two capture generations, one rule. Some write a delta file per run, whose label
    matches the run's; others write one file holding a list of entries, each carrying
    its own ``label`` and ``execution_id``. Matching on those is the same correlation
    the connector rows get, and it is what the previous reader skipped: it took entry
    zero from the shared file for every run.
    """
    candidates = [own] if own is not None else list(deltas.values())
    for path in candidates:
        try:
            payload = _load_json(path)
        except CaptureRefused:
            continue
        entries = payload if isinstance(payload, list) else [payload]
        # THE EXECUTION ID WINS WHEREVER IT IS PRESENT. Matching on either key let a
        # coincidental label override a CONFLICTING id and bind another execution's
        # verdict to this run — the weaker key overruling the stronger one, which is
        # the same substitution this whole correlation exists to prevent.
        # PER ENTRY, not per file. Scoping the precedence to the file let one keyed
        # STRANGER deny label matching to an entry carrying no identifier at all,
        # turning a served row into a refusal on the strength of an unrelated
        # neighbour. An entry that names an execution is judged on that name; an entry
        # that names none is judged on its label.
        # A keyed entry claiming the same label PROVES that label is not unique here,
        # so the unkeyed entry beside it identifies nothing. Unrelated keyed
        # neighbours still leave the fallback alone — it is the collision that
        # disqualifies it, not the presence of keys.
        reused = {
            entry.get("label") for entry in entries
            if isinstance(entry, dict) and entry.get("execution_id")
            and entry.get("execution_id") != execution_id
        }
        matching = [
            entry for entry in entries
            if isinstance(entry, dict)
            and (entry["execution_id"] == execution_id
                 if entry.get("execution_id")
                 else (entry.get("label") == label and label not in reused))
        ]
        if len(matching) == 1:
            changed = matching[0].get("raw_changed")
            return changed if isinstance(changed, bool) else None
        if (not matching and own is not None and len(entries) == 1
                and not any(isinstance(e, dict) and e.get("execution_id") for e in entries)):
            # A per-run file whose single entry names NEITHER key: the filename is the
            # only correlation that generation offers. An entry carrying an id that
            # does not match is a disagreement, not a fallback.
            changed = _first(entries[0], "raw_changed")
            return changed if isinstance(changed, bool) else None
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
            # NOT an error here. A double-execution capture NECESSARILY logs the
            # method once per execution. The COUNT is reconciled against the number
            # of executions by the caller, and ANY mismatch — surplus as well as
            # shortfall — drops attribution entirely: a third unrelated request of
            # the same method would otherwise leave the first status copied onto
            # both runs, which can turn a refused replay into an attested success.
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


#: Statuses that VERIFY a resource is absent. Deliberately tiny: everything else
#: non-2xx means the readback failed, which is not the same as the resource being
#: gone, and conflating them manufactures evidence from failure.
_ABSENCE_STATUSES: Final[frozenset[int]] = frozenset({404, 410})

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


def _observed_resource(path: Path) -> str | None:
    """The counterparty's OWN name for what a staged readback observed."""
    try:
        payload = _load_json(path)
    except CaptureRefused:
        return None
    resource = payload.get("path") if isinstance(payload, dict) else None
    return resource if isinstance(resource, str) and resource else None


def _body_label(path: Path) -> str | None:
    """The place a staged readback claims for itself, if it claims one."""
    try:
        payload = _load_json(path)
    except CaptureRefused:
        return None
    label = payload.get("label") if isinstance(payload, dict) else None
    return label if isinstance(label, str) and label else None


def _convergence(files: list[Path]) -> tuple[ConvergenceV1, ...]:
    """Derive what a replay did, per subject, from the staged readbacks."""
    # KEYED ON THE MOMENT EACH READBACK STATES, not on where it falls in a sorted
    # list. Every staged file names its moment twice — in its filename and again in
    # its own payload label — and the reader discarded both, admitting any subject
    # with three or more files and then taking the first, second-to-last and last.
    # Two archived-capture shapes defeat that: one file fewer and the subject leaves
    # the tuple entirely, silently narrowing the set the verdict quantifies over; one
    # file MORE and both comparison windows slide past the effect. Either way an
    # archive that recorded a second effect serves the clean verdict, and the served
    # row carries no stage count for a consumer to notice with.
    by_subject: dict[str, dict[str, list[tuple[str, Path]]]] = {}
    for path in files:
        m = _STAGE_READBACK.match(path.name)
        if m:
            by_subject.setdefault(m.group("subject"), {}).setdefault(
                m.group("moment"), []
            ).append((m.group("stage"), path))

    results: list[ConvergenceV1] = []
    resources: dict[str, str] = {}
    for subject, moments in sorted(by_subject.items()):
        # Incomplete or duplicated moments REFUSE. Dropping the subject was the
        # fail-open half: the neighbouring check already refuses a readback with no
        # body, so missing evidence one line over was failing the opposite way.
        if set(moments) != _CONVERGENCE_MOMENTS:
            raise CaptureRefused(
                f"{subject}: staged readbacks cover {sorted(moments)!r} rather than "
                f"{sorted(_CONVERGENCE_MOMENTS)!r}; a comparison window cannot be "
                "placed on a moment the capture did not record"
            )
        duplicated = sorted(m for m, entries in moments.items() if len(entries) != 1)
        if duplicated:
            raise CaptureRefused(
                f"{subject}: more than one staged readback claims {duplicated!r}, so "
                "which one bounds the comparison is not decidable from the archive"
            )
        staged = [moments[m][0] for m in _CONVERGENCE_ORDER]

        # THE TWO RECORDED ORDERS MUST AGREE. A moment set can be complete while every
        # moment contradicts its own stage number — an archive naming R0 the `between`
        # and R1 the `before` has a complete set and a reversed sequence, so the two
        # comparison windows get placed on a replay that ran the other way round. The
        # stage number is a sequence and the moment is a position in it; if sorting by
        # one disagrees with the other, the capture states two different orders and
        # neither is authoritative.
        by_stage = sorted(staged, key=lambda pair: int(pair[0][1:]))
        if [stage for stage, _ in by_stage] != [stage for stage, _ in staged]:
            raise CaptureRefused(
                f"{subject}: stage order {[s for s, _ in by_stage]!r} disagrees with "
                f"moment order {[s for s, _ in staged]!r}; the capture records two "
                "different sequences for one replay"
            )

        # And where a readback's own payload names its place, that name must agree
        # with the filename. Two capture generations write this label differently —
        # one states subject, stage and moment, the other only subject and stage — so
        # the moment half is checked only where it is actually stated. A fact recorded
        # twice with no pin between the copies is a class this ledger already carries.
        for stage, path in staged:
            label = _body_label(path)
            if label is None:
                continue
            expected_stage = f"{subject} {stage}"
            if not label.startswith(expected_stage):
                raise CaptureRefused(
                    f"{path.name}: its own label {label!r} does not agree with the "
                    f"subject and stage its filename states ({expected_stage!r})"
                )
            stated_moment = label[len(expected_stage):].lstrip("_").strip()
            moment = next(m for m in _CONVERGENCE_ORDER if moments[m][0][0] == stage)
            if stated_moment and stated_moment != moment:
                raise CaptureRefused(
                    f"{path.name}: its own label states the {stated_moment!r} moment "
                    f"while its filename states {moment!r}"
                )
        # WHAT was observed, not only WHEN. Every readback records the counterparty's
        # own name for the resource it read, and the reader took the subject from a
        # token in a filename OUR capture harness chose — so substituting one staged
        # readback with an observation of a DIFFERENT resource, leaving filename,
        # stage, moment and payload label untouched, manufactured the positive control
        # the verdict calls essential and turned a capture that correctly refuses into
        # a served duplicate-effect row.
        observed = {}
        for stage, path in staged:
            resource = _observed_resource(path)
            if resource is None:
                raise CaptureRefused(
                    f"{path.name}: records no resource, so what it observed is not "
                    "established and it cannot bound a comparison"
                )
            observed[stage] = resource
        if len(set(observed.values())) != 1:
            raise CaptureRefused(
                f"{subject}: its staged readbacks observed {sorted(set(observed.values()))!r} "
                "rather than one resource; a before-and-after comparison across two "
                "resources measures nothing about either"
            )
        resources[subject] = next(iter(observed.values()))

        bodies = [(stage, _body_of(_load_json(path))) for stage, path in staged]
        if any(body is None for _, body in bodies):
            raise CaptureRefused(
                f"{subject}: a staged readback carries no body object, so what the "
                "replay did cannot be derived"
            )
        before, between, after = bodies[0][1], bodies[1][1], bodies[2][1]
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
                    # ONLY a verified absence. A 404 says the resource is not
                    # there; a 401, 429 or 500 says the readback FAILED and the
                    # state is unknown. Treating those as "absent" made a 200
                    # followed by two 500s read as a clean first effect and no
                    # replay effect — an affirmative verdict built on two failed
                    # observations.
                    if a_status in _ABSENCE_STATUSES and b_status in _ABSENCE_STATUSES:
                        return False, ()
                    raise CaptureRefused(
                        f"readbacks returned {a_status} and {b_status}; neither "
                        "observes the resource's state, so whether the call "
                        "changed anything is unknown"
                    )
            keys = _differing_keys(a_body, b_body)
            return bool(keys), keys

        first_moved, _ = moved(statuses[0], before, statuses[1], between)
        replay_moved, replay_keys = moved(statuses[1], between, statuses[2], after)
        results.append(
            ConvergenceV1(
                subject=subject,
                stages=tuple(stage for stage, _ in staged),
                first_call_changed_state=first_moved,
                replay_changed_state=replay_moved,
                fields_differing_on_replay=replay_keys,
            )
        )

    # And two subjects must not be the same resource under two names: the negative
    # control only controls for something if it is somewhere else.
    duplicated = {r for r in resources.values() if list(resources.values()).count(r) > 1}
    if duplicated:
        raise CaptureRefused(
            f"subjects {sorted(k for k, v in resources.items() if v in duplicated)!r} "
            "observed the same resource, so one is not a control on the other"
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

    SWEPT, and deliberately left uncorrelated. This is the one observation where
    reading every row is the STRONGER check: its consumer requires the whole
    observed set to equal the declared family, so a capture whose source and target
    are different families is refused outright rather than attributed to whichever
    one was correlated. Narrowing this to the connector under test would turn a
    refusal into an attribution.

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

    SWEPT, and left as a MEMBERSHIP test on purpose: it answers "could this capture
    have exercised the declared verb", which is the weaker question, and it is
    backstopped by two correlated checks that answer the stronger one — the
    counterparty log must agree exactly where one exists, and the placement
    correlation refuses any declared action that does not resolve to a unique
    component carrying a platform row.
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

    _run_started_at: dict[str, str] = {}
    execution_ids_seen: set[str] = set()
    process_ids: set[str] = set()
    execution_times: list[str] = []
    accounts: set[str] = set()
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

        # Read HERE, from the record of an execution this capture owns, rather than
        # by searching the directory for the key. A directory-wide search takes the
        # first file that happens to carry one — and the archive holds a capture whose
        # earliest such file is a by-atom listing of OTHER executions entirely.
        process_id = _first(payload, "processId")
        if isinstance(process_id, str) and process_id:
            process_ids.add(process_id)
        # THE SAME SWEEP, and it was owed when the process id moved here. These two
        # feed served values — the recorded capture time and the account the scope
        # hash is built from — and both were still taking whatever the directory's
        # first matching artifact said. Measured on the archive: dropping in its own
        # by-atom execution listing moved a capture's served timestamp to an execution
        # a month earlier from a different capture, and ingest still succeeded.
        executed_at = _first(payload, "executionTime")
        if not (isinstance(executed_at, str) and executed_at):
            # The platform stamps the record twice, and the second stamp is present
            # and distinct in every archived multi-run capture. Refusing to order runs
            # because ONE of two recorded times is missing discards an order the
            # artifact does record — the defect being avoided is inventing an order,
            # not reading a second one the platform wrote.
            executed_at = _first(payload, "recordedDate")
        if isinstance(executed_at, str) and executed_at:
            execution_times.append(executed_at)
        account = _first(payload, "account")
        if isinstance(account, str) and account:
            accounts.add(account)

        state_changed: bool | None = None
        # CORRELATED TO THIS RUN. A single delta file covering several runs is a LIST
        # whose entries each name the run and the execution they describe, and the
        # reader used to take the first entry's verdict and hand it to every run — the
        # archived post capture serves a replay `state_changed` of true from its first
        # call's entry while its own second entry says false. The filename label is a
        # fallback for the one-entry-per-file generation, never a way to spread one
        # entry over several runs.
        state_changed = _state_change_for_run(
            deltas.get(label), deltas, label, execution_id
        )

        if isinstance(executed_at, str) and executed_at:
            _run_started_at[label] = executed_at
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

    # ORDERED BY WHEN THEY RAN, or NOT ORDERED AT ALL. The attribution below pairs the
    # counterparty's chronological log against this sequence, and it used to sort by
    # the run's filename LABEL on the stated grounds that the two orders agree —
    # measured, they disagree in ten of the thirteen multi-run captures by LABEL order, which is
    # what that fallback would use; eleven is the count by filename order.
    #
    # Sorting by timestamp with a fallback for missing ones was the first correction
    # and was itself wrong: an untimed run sorted ahead of every timed one, so removing
    # one timestamp from the archived delete capture moved the first call's 204 onto
    # the second execution and served that execution's id as the capture's. A partial
    # chronology is not a chronology.
    #
    # So the order is established or it is not. Established means every run carries a
    # timestamp and no two share one; then the runs are ordered by it. Otherwise they
    # keep a deterministic label order that asserts nothing about sequence, and the
    # per-run attribution below is dropped rather than paired against an order the
    # archive did not establish. Falling back to label ORDER for the pairing would
    # reinstate exactly the unestablished key this replaced.
    _times = [_run_started_at.get(run.label) for run in runs]
    # No `len(runs) == 1` special case: one run trivially satisfies both conditions,
    # and a disjunct that cannot change an outcome is a clause this module has already
    # been told twice not to write.
    chronology_established = all(_times) and len(set(_times)) == len(_times)
    if chronology_established and len(runs) > 1:
        runs.sort(key=lambda r: _run_started_at[r.label])
    else:
        runs.sort(key=lambda r: r.label)

    # ONE observed request cannot attest TWO executions. The outcome above is
    # copied onto every run, which is correct only when the counterparty logged as
    # many requests as there were executions. Otherwise the second execution is
    # unattested, and a replay verdict drawn from it would rest on a request nobody
    # observed. Drop the attribution rather than spread it.
    if logs and logged_request_count != len(runs):
        runs = [
            run.model_copy(update={"counterparty_status": None, "counterparty_method": None})
            for run in runs
        ]
    elif logs and len(runs) > 1 and not chronology_established:
        # The counts agree, so each execution has its own observed request — but which
        # request belongs to which execution is exactly what an unestablished order
        # cannot say. Dropping the attribution costs the replay verdict, which then
        # falls back to unverified; keeping it would mint a per-execution outcome from
        # an order nobody recorded.
        runs = [
            run.model_copy(update={"counterparty_status": None, "counterparty_method": None})
            for run in runs
        ]
    elif logs and len(runs) > 1:
        # The counts agree and the chronology is established, so each execution has
        # its own observed request and the two sequences can be paired. The runs are
        # ordered by when they RAN — this comment said "label-sorted" for two rounds
        # after that stopped being true, in the one branch where the distinction
        # decides the pairing. Copying one outcome onto every run would have claimed
        # the second execution returned what the first did, and for the archived
        # DELETE capture that is false: 204 then 404.
        #
        # No `len(outcomes) == len(runs)` guard: both were filtered from the same log
        # by the same predicate that produced the count above, so the branch could not
        # be false, and an unreachable clause here is a shape this module has been
        # told about three times.
        outcomes = _counterparty_outcomes(logs[0], method_hint)
        runs = [
            run.model_copy(update={"counterparty_status": status,
                                   "counterparty_method": method})
            for run, (status, method) in zip(runs, outcomes)
        ]

    execution_ids = frozenset(run.execution_id for run in runs)
    counts = _connector_document_counts(files, execution_ids, method_hint)
    # Exactly one, or none. Executions naming DIFFERENT processes do not share a
    # topology, and the archive holds such a capture — picking either graph would
    # attribute one execution's shape to the other's documents. Unresolved here means
    # the attribution refuses rather than guesses.
    receivers = _reachable_receivers(
        files, method_hint, process_ids.pop() if len(process_ids) == 1 else None
    )
    return CaptureSummaryV1(
        scenario=capture_dir.name,
        runs=tuple(runs),
        capture_digest=digest.hexdigest(),
        file_count=len(files),
        convergence=_convergence(files),
        observed_connector_types=_observed_connector_types(files, execution_ids),
        observed_methods=_observed_methods(files),
        captured_at=min(execution_times) if execution_times else None,
        # One account or none: executions from two accounts share no scope, and a
        # scope hash built from either would place the capture in the wrong one.
        account_id=accounts.pop() if len(accounts) == 1 else None,
        is_start_shape=_start_shape_of_the_exercised_connector(
            files, execution_ids, method_hint
        ),
        return_documents=_return_documents(files, execution_ids, receivers),
        receiver_is_downstream=None if receivers is None else bool(receivers),
        connector_documents=None if counts is None else counts[0],
        connector_successful_documents=None if counts is None else counts[1],
    )
