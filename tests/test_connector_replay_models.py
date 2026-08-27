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
    row = CapabilityEvidenceRecordV1(
        family="rest", action="GET", side_effect=SideEffectV1.READ,
        retry_safety=RetrySafetyV1.IDEMPOTENT, capture_digest="a" * 64,
        execution_ids=(_a_real_execution_id(),),
    )
    with pytest.raises(ValidationError):
        row.family = "other"
    with pytest.raises(ValidationError):
        CapabilityEvidenceRecordV1(
            family="rest", action="GET", side_effect=SideEffectV1.READ,
            retry_safety=RetrySafetyV1.IDEMPOTENT, capture_digest="a" * 64,
            execution_ids=(_a_real_execution_id(),), surprise="x",
        )


def test_the_platforms_published_example_cannot_be_cited_as_evidence():
    """The undated documentation example is exactly what a fixture author pastes."""
    with pytest.raises(ValidationError) as err:
        CapabilityEvidenceRecordV1(
            family="rest", action="GET", side_effect=SideEffectV1.READ,
            retry_safety=RetrySafetyV1.IDEMPOTENT, capture_digest="a" * 64,
            execution_ids=("execution-110b23f4-567a-8d90-1234-56789e0b123d",),
        )
    assert "execution ids" in str(err.value)


def test_a_row_cannot_cite_the_same_execution_twice():
    eid = _a_real_execution_id()
    with pytest.raises(ValidationError):
        CapabilityEvidenceRecordV1(
            family="rest", action="GET", side_effect=SideEffectV1.READ,
            retry_safety=RetrySafetyV1.IDEMPOTENT, capture_digest="a" * 64,
            execution_ids=(eid, eid),
        )


def test_conditionally_idempotent_must_name_its_operation():
    with pytest.raises(ValidationError) as err:
        CapabilityEvidenceRecordV1(
            family="rest", action="PATCH", side_effect=SideEffectV1.WRITE,
            retry_safety=RetrySafetyV1.CONDITIONALLY_IDEMPOTENT,
            capture_digest="a" * 64, execution_ids=(_a_real_execution_id(),),
        )
    assert "not transferable" in str(err.value)


def test_conditionally_idempotent_is_accepted_when_it_names_one():
    row = CapabilityEvidenceRecordV1(
        family="rest", action="PATCH", side_effect=SideEffectV1.WRITE,
        retry_safety=RetrySafetyV1.CONDITIONALLY_IDEMPOTENT,
        capture_digest="a" * 64, execution_ids=(_a_real_execution_id(),),
        operation_component_id="c4016c66-1234-4abc-9def-0123456789ab",
    )
    assert row.retry_safety is RetrySafetyV1.CONDITIONALLY_IDEMPOTENT


def test_unknown_side_effect_cannot_carry_a_verdict():
    with pytest.raises(ValidationError):
        CapabilityEvidenceRecordV1(
            family="rest", action="TRACE", side_effect=SideEffectV1.UNKNOWN,
            retry_safety=RetrySafetyV1.IDEMPOTENT, capture_digest="a" * 64,
            execution_ids=(_a_real_execution_id(),),
        )


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
            )


def test_a_genuine_connector_type_maps():
    genuine = sorted(t for t in _real_connector_types() if t not in ("nodata", "return"))
    assert genuine, "no genuine connector type in the archive"
    m = ConnectorVocabularyMappingV1(
        platform_connector_type=genuine[0], family="rest",
        action_source=ActionSourceV1.OPERATION_COMPONENT,
    )
    assert m.family == "rest"


def test_the_action_may_not_be_sourced_from_the_execution_record():
    """The measured finding, encoded as a refusal rather than a comment."""
    with pytest.raises(ValidationError) as err:
        ConnectorVocabularyMappingV1(
            platform_connector_type="officialboomi-X3979C-rest-prod", family="rest",
            action_source=ActionSourceV1.EXECUTION_RECORD,
        )
    assert "one generic action" in str(err.value)
