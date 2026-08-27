"""Constructors for evidence rows in tests.

Centralised because an evidence row is now BOUND to a typed capture: the verdict
must be one the capture's replay observation supports. Building that by hand at
every call site invites each site to drift into a combination the model forbids —
or worse, into one it allows for the wrong reason.
"""

from __future__ import annotations

from boomi_mcp.connector_replay.models import (
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

REAL_EXECUTION = "execution-1957bb8f-9a89-4254-b169-9ddbf41fddf8-2026.08.26"


def capture_reference(
    *,
    replay: ReplayObservationV1 = ReplayObservationV1.NOT_EXERCISED,
    effect: EffectObservationV1 = EffectObservationV1.READ_ONLY,
    scope: EvidenceScopeV1 = EvidenceScopeV1.SINGLE_OPERATION,
    sources: tuple = (EvidenceSourceV1.ENDPOINT_READBACK, EvidenceSourceV1.EXECUTION_RECORD),
    execution_id: str = REAL_EXECUTION,
) -> CaptureReferenceV1:
    return CaptureReferenceV1(
        execution_id=execution_id,
        captured_at="2026-08-26T00:00:00Z",
        account_scope_hash="b" * 64,
        capture_digest="c" * 64,
        summary=ClosedCaptureObservationsV1(
            placement=PlacementObservationV1.ENTRY,
            input_observation=InputObservationV1.NO_INBOUND_DOCUMENTS,
            output_observation=OutputObservationV1.RETURN_DOCUMENTS_RECEIVED,
            effect=effect, replay=replay, scope=scope,
            sources=tuple(sorted(sources, key=lambda s: s.value)),
        ),
    )


def evidence_row(
    *,
    family: str = "rest",
    action: str = "GET",
    side_effect: SideEffectV1 = SideEffectV1.READ,
    retry_safety: RetrySafetyV1 = RetrySafetyV1.UNVERIFIED,
    capture: CaptureReferenceV1 | None = None,
    execution_ids: tuple = (REAL_EXECUTION,),
    **overrides,
) -> CapabilityEvidenceRecordV1:
    return CapabilityEvidenceRecordV1(
        family=family, action=action,
        accepts_input=InputObservationV1.NO_INBOUND_DOCUMENTS,
        produces_output=OutputObservationV1.RETURN_DOCUMENTS_RECEIVED,
        side_effect=side_effect, retry_safety=retry_safety,
        capture=capture if capture is not None else capture_reference(),
        capture_digest="a" * 64, execution_ids=execution_ids, **overrides,
    )


def evidence_row_dict(**kw) -> dict:
    """The same row as a JSON-ready dict, for registry-parsing tests."""
    return evidence_row(**kw).model_dump(mode="json")
