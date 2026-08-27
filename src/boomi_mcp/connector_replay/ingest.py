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
from typing import Final, Iterable

from .capture import CaptureRefused, CaptureSummaryV1, summarize
from .models import (
    CapabilityEvidenceRecordV1,
    CaptureReferenceV1,
    ClosedCaptureObservationsV1,
    EffectObservationV1,
    EvidenceScopeV1,
    EvidenceSourceV1,
    InputObservationV1,
    OutputObservationV1,
    PlacementObservationV1,
    ReplayObservationV1,
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

#: Fields the counterparty maintains itself, which moving does not make a replay
#: unsafe. Deliberately tiny and named here rather than inline: widening it is how
#: a genuine side effect gets reclassified as noise, so it should be an obvious
#: edit with a reviewer's eyes on it.
_VOLATILE_FIELDS: Final[frozenset[str]] = frozenset({"modifiedOn"})


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

    on_disk = sorted(p for p in capture_dir.rglob("*") if p.is_file())
    unlisted: list[str] = []
    mismatched: list[str] = []
    for path in on_disk:
        rel = str(path.relative_to(archive_root))
        expected = digests.get(rel)
        if expected is None:
            unlisted.append(rel)
            continue
        actual = hashlib.sha256(path.read_bytes()).hexdigest()
        if actual != expected:
            mismatched.append(rel)

    # MISSING is the third direction, and the one the first version could not see.
    # Walking only what exists finds a changed byte and an extra file, but a
    # DELETED artifact produces neither — it is not on disk to mismatch and not
    # unlisted because it is not there at all. A capture that quietly lost its
    # replay execution record or a readback would then verify clean and be
    # classified on the evidence that remained.
    prefix = str(capture_dir.relative_to(archive_root)) + "/"
    present = {str(p.relative_to(archive_root)) for p in on_disk}
    missing = sorted(rel for rel in digests if rel.startswith(prefix) and rel not in present)

    if unlisted or mismatched or missing:
        raise IngestRefused(
            "capture {0} does not match the archive manifest — unlisted: {1}; "
            "digest mismatch: {2}; MISSING (listed but absent): {3}. A capture is "
            "evidence only while its bytes are the bytes that were archived, and "
            "only while all of them are still there.".format(
                capture_dir.name, unlisted, mismatched, missing)
        )


def classify(summary: CaptureSummaryV1, action: str | None = None,
             safe_actions: frozenset[str] | None = None) -> tuple[SideEffectV1, RetrySafetyV1]:
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
    # The FIRST execution must have succeeded — otherwise the action never
    # happened and nothing about it was observed.
    if statuses[0] not in _SUCCESS_RANGE:
        return SideEffectV1.UNKNOWN, RetrySafetyV1.UNVERIFIED
    replay_statuses = statuses[1:]
    if replay_statuses and any(s not in _SUCCESS_RANGE for s in replay_statuses):
        # A REPLAY that is refused is not an absence of evidence — it is evidence,
        # and often the most useful kind. A delete that returns 204 then 404 while
        # the resource stays deleted has an idempotent EFFECT and a
        # non-idempotent RESPONSE, which is exactly the conflict-without-second-
        # effect outcome the model defines. Refusing every non-2xx made that
        # outcome unreachable from any capture.
        #
        # The condition is narrow: the state must not have moved on the replay. A
        # refused replay that ALSO changed something is not a conflict, it is a
        # second effect.
        moved = [c for c in summary.convergence
                 if c.replay_changed_state
                 and not set(c.fields_differing_on_replay) <= _VOLATILE_FIELDS]
        if moved:
            return SideEffectV1.WRITE, RetrySafetyV1.NON_IDEMPOTENT
        return SideEffectV1.WRITE, RetrySafetyV1.CONDITIONALLY_IDEMPOTENT

    changed = [r.state_changed for r in summary.runs if r.state_changed is not None]
    # The positive control: at least one subject the FIRST call actually moved.
    # Without it, "the replay changed nothing" is unfalsifiable — a call that did
    # nothing at all looks perfectly idempotent.
    acted = [c for c in summary.convergence if c.first_call_changed_state]

    if acted:
        # A replay verdict requires two DISTINCT executions. Staged readbacks alone
        # do not make a double execution: a malformed or truncated capture could
        # carry them beside a single run and be handed a replay verdict it never
        # earned.
        if len(summary.execution_ids) < 2:
            return SideEffectV1.WRITE, RetrySafetyV1.UNVERIFIED

        # Evaluate the replay across EVERY captured subject, not only the ones the
        # first call touched. Filtering to those discarded the negative controls —
        # so a second call that unexpectedly moved a previously untouched resource
        # was invisible, and the action was still called conditionally idempotent.
        # A side effect on a subject nobody targeted is the strongest possible
        # evidence AGAINST replay safety.
        only_volatile = all(
            not c.replay_changed_state
            or set(c.fields_differing_on_replay) <= _VOLATILE_FIELDS
            for c in summary.convergence
        )
        return (
            SideEffectV1.WRITE,
            RetrySafetyV1.CONDITIONALLY_IDEMPOTENT if only_volatile else RetrySafetyV1.NON_IDEMPOTENT,
        )
    if any(changed):
        return SideEffectV1.WRITE, RetrySafetyV1.UNVERIFIED
    if changed:
        # No observable state change. That is consistent with a READ *and* with a
        # write that happened to be a no-op — a PATCH setting a field to the value
        # it already held changes nothing and is not therefore idempotent.
        #
        # The verb is what separates them, and the verb is now VERIFIED evidence:
        # it was reconciled against the capture's components and against the
        # counterparty log. Without it, the honest answer is unknown.
        if action is None or safe_actions is None or action not in safe_actions:
            return SideEffectV1.UNKNOWN, RetrySafetyV1.UNVERIFIED
        # READ, but retry safety stays UNVERIFIED — and the distinction is the
        # whole point of this registry. That a safe method is idempotent is a
        # claim from the transport SPECIFICATION; that replaying THIS action
        # against THIS counterparty is safe is a claim about an observation, and
        # no replay was exercised here. The registry records what was observed.
        # An affirmative retry verdict needs a double execution, which is exactly
        # what the capture set for the read verbs does not yet contain.
        return SideEffectV1.READ, RetrySafetyV1.UNVERIFIED
    return SideEffectV1.UNKNOWN, RetrySafetyV1.UNVERIFIED


def ingest(
    archive_root: Path,
    capture_dirs: Iterable[Path],
    *,
    family: str,
    actions: dict[str, str],
    registry=None,
) -> tuple[CapabilityEvidenceRecordV1, ...]:
    """Verify, summarise and classify captures into evidence rows.

    ``actions`` maps a capture directory name to the action it exercised, and
    ``family`` names the connector family. BOTH are RECONCILED against what the
    capture observed — they are the caller's claim about the evidence, not the
    evidence.

    Without that reconciliation the caller's word became the row: a checksummed
    REST HEAD capture ingested as ``family="database"`` produced a
    ``database/HEAD/idempotent`` row, minting a verdict for a connector no
    execution touched. Verifying the bytes proves the capture was not altered; it
    says nothing about whether the labels attached to it are true.

    ``registry`` supplies the vocabulary that maps an observed platform connector
    type to a family. It defaults to the packaged one.
    """
    if registry is None:
        from .registry import load_registry

        registry = load_registry()
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

        # RECONCILE the caller's labels against the capture's own observations.
        if not summary.observed_connector_types:
            raise IngestRefused(
                f"{directory.name}: no connector type was observed, so the family "
                "cannot be reconciled and the caller's claim would stand unchecked"
            )
        observed_families = {
            registry.family_for(t) for t in summary.observed_connector_types
        }
        if None in observed_families:
            unmapped = [t for t in summary.observed_connector_types
                        if registry.family_for(t) is None]
            raise IngestRefused(
                f"{directory.name}: observed connector type(s) {unmapped!r} are not "
                "in the registry vocabulary. A capture whose connector cannot be "
                "resolved cannot be attributed to a family"
            )
        if observed_families != {family}:
            raise IngestRefused(
                f"{directory.name}: declared family {family!r} does not match the "
                f"observed {sorted(observed_families)!r}. The declared family is a "
                "claim about the evidence, not the evidence"
            )
        if not summary.observed_methods:
            raise IngestRefused(
                f"{directory.name}: no component declares a method, so the action "
                "cannot be reconciled against anything"
            )
        if action not in summary.observed_methods:
            raise IngestRefused(
                f"{directory.name}: declared action {action!r} is not among the "
                f"methods this capture's components declare {list(summary.observed_methods)!r}. "
                "Classification would otherwise describe a method the capture never "
                "exercised"
            )
        # Where a counterparty log exists it is the STRONGER evidence, and it must
        # agree exactly: membership says the capture COULD have exercised this
        # method, the log says it DID.
        logged = {r.counterparty_method for r in summary.runs if r.counterparty_method}
        if logged and logged != {action}:
            raise IngestRefused(
                f"{directory.name}: declared action {action!r} disagrees with the "
                f"counterparty log, which recorded {sorted(logged)!r}"
            )

        safe = frozenset(
            a for entry in registry.vocabulary if entry.family == family
            for a in entry.safe_actions
        )
        side_effect, retry_safety = classify(summary, action, safe)
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
                accepts_input=_input_observation(summary),
                produces_output=_output_observation(summary),
                side_effect=side_effect,
                retry_safety=retry_safety,
                capture=_capture_reference(summary, side_effect),
                capture_digest=summary.capture_digest,
                execution_ids=summary.execution_ids,
                operation_component_id=operation_id,
            )
        )
    return tuple(rows)


def _input_observation(summary: CaptureSummaryV1) -> InputObservationV1:
    """Whether the connector consumed documents, from the execution's own counts."""
    # `inboundDocumentCount`, not `inboundErrorDocumentCount` — the latter counts
    # ERRONEOUS inbound documents, so a successful input-consuming run has zero of
    # them and was being published as consuming nothing.
    consumed = any((run.inbound_documents or 0) > 0 for run in summary.runs)
    return (InputObservationV1.DOCUMENTS_CONSUMED if consumed
            else InputObservationV1.NO_INBOUND_DOCUMENTS)


def _output_observation(summary: CaptureSummaryV1) -> OutputObservationV1:
    """What the capture saw come back.

    Conservative: without a counterparty attestation nothing is claimed about
    output, because the platform reports a complete execution either way.
    """
    # A counterparty log proves a REQUEST reached the endpoint. It says nothing
    # about a response document reaching the process, and a 204 carries no body at
    # all — so claiming documents were returned from the log alone was asserting
    # something never observed.
    if any((run.outbound_documents or 0) > 0 for run in summary.runs):
        return OutputObservationV1.RETURN_DOCUMENTS_RECEIVED
    return OutputObservationV1.NO_OUTPUT_OBSERVED


def _replay_observation(summary: CaptureSummaryV1) -> ReplayObservationV1:
    """What a second identical execution did — or that none was attempted.

    `not_exercised` is the default and the honest one: a capture with no positive
    control, or with fewer than two attested executions, has not observed a replay
    at all. Reporting anything else would let an unexercised path acquire a verdict.
    """
    acted = [c for c in summary.convergence if c.first_call_changed_state]
    attested = [r for r in summary.runs if r.counterparty_status is not None]
    if not acted or len(summary.execution_ids) < 2 or len(attested) < 2:
        return ReplayObservationV1.NOT_EXERCISED
    moved = [c for c in summary.convergence
             if c.replay_changed_state
             and not set(c.fields_differing_on_replay) <= _VOLATILE_FIELDS]
    if moved:
        return ReplayObservationV1.DUPLICATE_EFFECT
    # A replay REFUSED by the counterparty, with the state unmoved, is the conflict
    # outcome rather than a plain repeat — and the distinction matters to a caller
    # deciding whether a retry is safe to attempt at all.
    replay_statuses = [r.counterparty_status for r in summary.runs[1:]
                       if r.counterparty_status is not None]
    if replay_statuses and any(s not in _SUCCESS_RANGE for s in replay_statuses):
        return ReplayObservationV1.CONFLICT_WITHOUT_SECOND_EFFECT
    return ReplayObservationV1.SAME_EFFECT


def _capture_reference(summary: CaptureSummaryV1, side_effect: SideEffectV1) -> CaptureReferenceV1:
    """Assemble the typed capture the evidence row is bound to.

    Every observation here is DERIVED from what the capture recorded. The scope is
    `single_operation` because that is what one capture observes — an action-wide
    claim needs evidence gathered across operations, and asserting it from one would
    be the overreach the scope field exists to prevent.
    """
    if side_effect is SideEffectV1.READ:
        effect = EffectObservationV1.READ_ONLY
    elif side_effect is SideEffectV1.WRITE:
        effect = (EffectObservationV1.STATE_UNCHANGED_AFTER_REPLAY
                  if summary.convergence else EffectObservationV1.STATE_CHANGED)
    else:
        # UNKNOWN has no honest observation. Defaulting to `read_only` fabricated a
        # POSITIVE finding out of missing or failed evidence, and let the outer side
        # effect contradict the summary it is supposed to rest on.
        raise IngestRefused(
            f"{summary.scenario}: the side effect could not be determined, so there "
            "is no observation to record. A capture that establishes nothing must "
            "not be given a reference that claims something"
        )
    sources = [EvidenceSourceV1.EXECUTION_RECORD, EvidenceSourceV1.EXECUTION_CONNECTOR]
    if any(r.state_changed is not None for r in summary.runs) or summary.convergence:
        sources.append(EvidenceSourceV1.ENDPOINT_READBACK)
    if effect is EffectObservationV1.STATE_CHANGED and \
            EvidenceSourceV1.ENDPOINT_READBACK not in sources:
        # The model refuses this anyway; failing here names the capture.
        raise IngestRefused(
            f"{summary.scenario}: a state-change effect has no readback behind it"
        )
    return CaptureReferenceV1(
        execution_id=summary.execution_ids[0],
        # From the capture, not from its DIRECTORY NAME. A scenario label is neither
        # a timestamp nor an account: hashing it made two captures from one account
        # look unrelated and two identically-named scenarios from different accounts
        # collide — which silently defeats the account-consistency check that
        # consumes this hash.
        captured_at=summary.captured_at or summary.execution_ids[0].rsplit("-", 1)[1],
        account_scope_hash=hashlib.sha256(
            (summary.account_id or "").encode("utf-8")).hexdigest(),
        summary=ClosedCaptureObservationsV1(
            placement=(PlacementObservationV1.ENTRY if summary.is_start_shape
                       else PlacementObservationV1.DOWNSTREAM),
            input_observation=_input_observation(summary),
            output_observation=_output_observation(summary),
            effect=effect,
            replay=_replay_observation(summary),
            scope=EvidenceScopeV1.SINGLE_OPERATION,
            sources=tuple(sorted(sources, key=lambda s: s.value)),
        ),
        capture_digest=summary.capture_digest,
    )


def _operation_component_id(directory: Path) -> str | None:
    """The component id of the operation a capture exercised, from its XML."""
    import re

    from .ids import is_boomi_component_id

    # By CONTENT. The double-execution captures name their components
    # `component_op_src.xml` and `component_op_tgt.xml`, which a `*operation*` glob
    # misses — so the operation a conditional verdict must name was unfindable.
    for path in sorted(directory.glob("*.xml")):
        if "customOperationType" not in path.read_text():
            continue
        for match in re.finditer(r'componentId="([^"]+)"|\bid="([0-9a-fA-F-]{36})"', path.read_text()):
            value = match.group(1) or match.group(2)
            if is_boomi_component_id(value):
                return value
    return None
