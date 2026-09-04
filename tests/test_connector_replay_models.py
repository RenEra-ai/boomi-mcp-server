"""Registry rows: closed, frozen, and unable to record an unobserved claim.

Provenance: execution ids and connector types come from the archived live captures;
the rejected execution id is the platform's own published example.
"""

from __future__ import annotations

import json
import re
from pathlib import Path

import pytest
from pydantic import ValidationError

from _connector_replay_factories import capture_reference, evidence_row
from boomi_mcp.connector_replay.models import (
    ActionSourceV1,
    CapabilityEvidenceRecordV1,
    ConnectorVocabularyMappingV1,
    RetrySafetyV1,
    SideEffectV1,
)

_REPO = Path(__file__).resolve().parents[1]
_CAPTURES = _REPO / "docs" / "architecture" / "evidence" / "issue-155" / "captures"
_EXEC_ID = re.compile(r"execution-[0-9a-fA-F-]+-\d{4}\.\d{2}\.\d{2}")


def _a_real_execution_id() -> str:
    for path in _CAPTURES.rglob("*execution_record*.json"):
        found = _EXEC_ID.findall(path.read_text())
        if found:
            return found[0]
    raise AssertionError("no execution id in the archive")


def _real_connector_types() -> set[str]:
    seen: set[str] = set()
    for path in _CAPTURES.rglob("*execution_connector*.json"):
        seen.update(re.findall(r'"connectorType":\s*"([^"]+)"', path.read_text()))
    return seen


def test_rows_are_frozen_and_closed():
    row = evidence_row(execution_ids=(_a_real_execution_id(),))
    with pytest.raises(ValidationError):
        row.family = "other"
    with pytest.raises(ValidationError):
        evidence_row(execution_ids=(_a_real_execution_id(),), surprise="x")


def test_the_platforms_published_example_cannot_be_cited_as_evidence():
    """The undated documentation example is exactly what a fixture author pastes."""
    with pytest.raises(ValidationError) as err:
        evidence_row(execution_ids=("execution-110b23f4-567a-8d90-1234-56789e0b123d",))
    assert "execution ids" in str(err.value)


def test_a_row_cannot_cite_the_same_execution_twice():
    eid = _a_real_execution_id()
    with pytest.raises(ValidationError):
        evidence_row(execution_ids=(eid, eid))


def test_conditionally_idempotent_must_name_its_operation():
    with pytest.raises(ValidationError) as err:
        evidence_row(action="PATCH", side_effect=SideEffectV1.WRITE,
                     retry_safety=RetrySafetyV1.CONDITIONALLY_IDEMPOTENT,
                     capture=_converged(), execution_ids=(_a_real_execution_id(),))
    assert "not transferable" in str(err.value)


def test_conditionally_idempotent_is_accepted_when_it_names_one():
    row = evidence_row(
        action="PATCH", side_effect=SideEffectV1.WRITE,
        retry_safety=RetrySafetyV1.CONDITIONALLY_IDEMPOTENT, capture=_converged(),
        execution_ids=(_a_real_execution_id(),),
        operation_component_id="c4016c66-1234-4abc-9def-0123456789ab",
    )
    assert row.retry_safety is RetrySafetyV1.CONDITIONALLY_IDEMPOTENT


def test_unknown_side_effect_cannot_carry_a_verdict():
    with pytest.raises(ValidationError):
        evidence_row(action="TRACE", side_effect=SideEffectV1.UNKNOWN,
                     retry_safety=RetrySafetyV1.IDEMPOTENT)


def test_the_execution_sentinels_are_present_in_real_data_and_are_refused():
    """Non-vacuity: prove the sentinels this validator refuses actually occur."""
    real = _real_connector_types()
    assert {"nodata", "return"} <= real, (
        "the sentinels this validator refuses were not found in the archive, so the "
        "refusal is guarding against something that does not happen: {0}".format(sorted(real))
    )
    for sentinel in ("nodata", "return"):
        with pytest.raises(ValidationError):
            ConnectorVocabularyMappingV1(
                platform_connector_type=sentinel, family="rest",
                action_source=ActionSourceV1.OPERATION_COMPONENT,
                recognised_actions=("GET",),
                action_ids=(("GET", "get"),),
            )


def test_a_genuine_connector_type_maps():
    genuine = sorted(t for t in _real_connector_types() if t not in ("nodata", "return"))
    assert genuine, "no genuine connector type in the archive"
    m = ConnectorVocabularyMappingV1(
        platform_connector_type=genuine[0], family="rest",
        action_source=ActionSourceV1.OPERATION_COMPONENT,
        recognised_actions=("GET", "HEAD"),
        action_ids=(("GET", "get"), ("HEAD", "head")),
    )
    assert m.family == "rest"


def test_a_mapping_must_recognise_at_least_one_action():
    """Resolving a family alone let an invented action inherit its authority."""
    with pytest.raises(ValidationError):
        ConnectorVocabularyMappingV1(
            platform_connector_type="officialboomi-X3979C-rest-prod", family="rest",
            action_source=ActionSourceV1.OPERATION_COMPONENT, recognised_actions=(),
        )


def test_recognised_actions_have_one_canonical_form():
    for bad in (("HEAD", "GET"), ("GET", "GET")):
        with pytest.raises(ValidationError):
            ConnectorVocabularyMappingV1(
                platform_connector_type="x", family="rest",
                action_source=ActionSourceV1.OPERATION_COMPONENT, recognised_actions=bad,
            )


def test_the_action_may_not_be_sourced_from_the_execution_record():
    """The measured finding, encoded as a refusal rather than a comment."""
    with pytest.raises(ValidationError) as err:
        ConnectorVocabularyMappingV1(
            platform_connector_type="officialboomi-X3979C-rest-prod", family="rest",
            action_source=ActionSourceV1.EXECUTION_RECORD, recognised_actions=("GET",),
            # Mapped, so this reaches the rule it is about. An unmapped entry is
            # refused earlier now, and the earlier refusal would have masked the
            # one under test — a probe graded by the wrong guard.
            action_ids=(("GET", "get"),),
        )
    assert "one generic action" in str(err.value)


def test_the_closed_vocabularies_are_the_designs_not_invented_ones():
    """A closed vocabulary is a contract with the slices that consume it.

    An earlier version invented plausible near-synonyms — `client_supplied_key`,
    `rejects_duplicate`, a `connection` key scope. They read sensibly and were not
    the published contract, which means the consuming slice could not express what
    it was designed to express.
    """
    from boomi_mcp.connector_replay.models import (
        DuplicateGuaranteeV1,
        EffectObservationV1,
        KeyMechanismV1,
        KeyScopeV1,
        ReplayObservationV1,
    )

    assert {e.value for e in KeyMechanismV1} == {
        "request_key_deduplication", "resource_identity_upsert"}
    assert {e.value for e in KeyScopeV1} == {"operation", "static_route", "service"}
    assert {e.value for e in DuplicateGuaranteeV1} == {
        "same_effect", "same_result", "conflict_without_second_effect"}
    assert {e.value for e in EffectObservationV1} == {
        "read_only", "state_created", "state_changed", "state_deleted",
        "state_unchanged_after_replay"}
    assert {e.value for e in ReplayObservationV1} == {
        "not_exercised", "same_effect", "same_result",
        "conflict_without_second_effect", "duplicate_effect"}


def test_a_state_claim_requires_an_endpoint_readback():
    """The platform reports an execution complete even when the counterparty refused.

    So an effect claim about the counterparty's state rests on nothing unless a
    readback observed it. `not_exercised` exists for the same reason: a boolean
    cannot distinguish "the replay produced the same effect" from "no replay was
    attempted", and collapsing those is how an unexercised path acquires a verdict.
    """
    from boomi_mcp.connector_replay.models import (
        ClosedCaptureObservationsV1,
        EffectObservationV1,
        EvidenceScopeV1,
        EvidenceSourceV1,
        InputObservationV1,
        OutputObservationV1,
        PlacementObservationV1,
        ReplayObservationV1,
    )

    kw = dict(
        placement=PlacementObservationV1.ENTRY,
        input_observation=InputObservationV1.NO_INBOUND_DOCUMENTS,
        output_observation=OutputObservationV1.RETURN_DOCUMENTS_RECEIVED,
        replay=ReplayObservationV1.NOT_EXERCISED,
        scope=EvidenceScopeV1.SINGLE_OPERATION,
    )
    with pytest.raises(ValidationError) as err:
        ClosedCaptureObservationsV1(
            effect=EffectObservationV1.STATE_CHANGED,
            sources=(EvidenceSourceV1.EXECUTION_RECORD,), **kw)
    assert "endpoint readback" in str(err.value)

    # With the readback, the same claim stands.
    ok = ClosedCaptureObservationsV1(
        effect=EffectObservationV1.STATE_CHANGED,
        sources=(EvidenceSourceV1.ENDPOINT_READBACK, EvidenceSourceV1.EXECUTION_RECORD),
        **kw)
    assert ok.effect is EffectObservationV1.STATE_CHANGED

    # And a read-only claim needs no readback.
    assert ClosedCaptureObservationsV1(
        effect=EffectObservationV1.READ_ONLY,
        sources=(EvidenceSourceV1.EXECUTION_RECORD,), **kw)


def _converged():
    """A capture whose replay WAS exercised and converged.

    A conditional verdict needs one: the model now requires the verdict to be
    supported by the capture's replay observation, and `not_exercised` supports
    nothing affirmative.
    """
    from boomi_mcp.connector_replay.models import (
        EffectObservationV1, EvidenceSourceV1, ReplayObservationV1)

    return capture_reference(
        replay=ReplayObservationV1.SAME_EFFECT,
        effect=EffectObservationV1.STATE_CHANGED,
        sources=(EvidenceSourceV1.ENDPOINT_READBACK, EvidenceSourceV1.EXECUTION_RECORD),
    )
