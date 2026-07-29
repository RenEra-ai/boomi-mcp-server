"""Capability-registry tests (issue #144, M12.9).

The registry is where this issue's central claim lives: every modeled object and
relation is tied to evidence and carries a state. These tests pin that the
registry cannot drift from the schema, that the five states behave differently
from one another, and — most importantly — that the four honest evidence gaps
stay honest.
"""

import sys
from pathlib import Path

import pytest

_project_root = Path(__file__).resolve().parent.parent
_src = str(_project_root / "src")
if _src not in sys.path:
    sys.path.insert(0, _src)

from boomi_mcp.models.system_topology import (
    TOPOLOGY_OBJECT_KINDS,
    TOPOLOGY_RELATION_KINDS,
    parse_system_topology_v1,
)
from boomi_mcp.compiler.system_topology import capabilities as caps
from boomi_mcp.compiler.system_topology.capabilities import (
    GATED_SUBJECTS,
    SYSTEM_TOPOLOGY_CAPABILITIES,
    SYSTEM_TOPOLOGY_CAPABILITY_REVISION,
    build_capability_report,
    capability_for,
    collect_capability_findings,
)

_STATES = {
    "emittable",
    "plannable-only",
    "guidance-only",
    "gated-no-evidence",
    "unsupported",
}


def _spec(objects, relations=()):
    return parse_system_topology_v1(
        {
            "version": "1",
            "profile_ref": "p",
            "objects": list(objects),
            "relations": list(relations),
        }
    )


_PROCESS = {"kind": "process", "key": "p", "component_ref": "$ref:pk"}


# ---------------------------------------------------------------------------
# Coverage — the registry cannot drift from the unions
# ---------------------------------------------------------------------------


def test_registry_covers_every_object_kind_exactly():
    registered = {
        row.subject
        for row in SYSTEM_TOPOLOGY_CAPABILITIES.values()
        if row.subject_kind == "object"
    }
    assert registered == set(TOPOLOGY_OBJECT_KINDS)


def test_registry_covers_every_relation_kind_exactly():
    registered = {
        row.subject
        for row in SYSTEM_TOPOLOGY_CAPABILITIES.values()
        if row.subject_kind == "relation"
    }
    assert registered == set(TOPOLOGY_RELATION_KINDS)


def test_coverage_check_rejects_a_missing_registration():
    """Positive control: a check nobody can make fail proves nothing.

    Driven through the private ``_validate_coverage`` with a deliberately short
    row set, which is why that function is exposed at all rather than inlined at
    module scope.
    """
    short = tuple(
        row for row in caps._ROWS if row.subject != TOPOLOGY_OBJECT_KINDS[0]
    )
    with pytest.raises(ValueError) as exc:
        caps._validate_coverage(TOPOLOGY_OBJECT_KINDS, TOPOLOGY_RELATION_KINDS, short)
    assert "coverage mismatch" in str(exc.value)
    assert TOPOLOGY_OBJECT_KINDS[0] in str(exc.value)


def test_coverage_check_rejects_an_unknown_registration():
    extra = caps._ROWS + (
        caps.CapabilityRegistrationV1(
            subject="not_a_real_kind",
            subject_kind="object",
            state="emittable",
            source=caps._leg("source", "verified", "x"),
            documentation=caps._leg("documentation", "verified", "x"),
            live=caps._leg("live", "verified", "x"),
        ),
    )
    with pytest.raises(ValueError) as exc:
        caps._validate_coverage(TOPOLOGY_OBJECT_KINDS, TOPOLOGY_RELATION_KINDS, extra)
    assert "not_a_real_kind" in str(exc.value)


def test_coverage_check_rejects_a_duplicate_subject():
    duplicated = caps._ROWS + (caps._ROWS[0],)
    with pytest.raises(ValueError) as exc:
        caps._validate_coverage(
            TOPOLOGY_OBJECT_KINDS, TOPOLOGY_RELATION_KINDS, duplicated
        )
    assert "duplicate subject" in str(exc.value)


def test_registry_is_immutable():
    with pytest.raises(TypeError):
        SYSTEM_TOPOLOGY_CAPABILITIES["process"] = None  # type: ignore[index]


def test_every_registration_declares_a_state_and_three_evidence_legs():
    for subject, row in SYSTEM_TOPOLOGY_CAPABILITIES.items():
        assert row.state in _STATES, subject
        assert row.source.leg == "source", subject
        assert row.documentation.leg == "documentation", subject
        assert row.live.leg == "live", subject
        assert row.source.reference, subject
        assert row.documentation.reference, subject
        assert row.live.reference, subject


def test_capability_for_refuses_an_unregistered_subject():
    with pytest.raises(ValueError):
        capability_for("no_such_subject")


# ---------------------------------------------------------------------------
# The four honest evidence gaps
# ---------------------------------------------------------------------------


def test_api_service_is_emittable_but_its_live_leg_is_unavailable():
    """The capture observed no ``webservice`` components.

    The typed builder is real, so the object stays emittable — but claiming the
    live leg satisfied would be the exact overclaim this issue forbids.
    """
    row = capability_for("api_service")
    assert row.state == "emittable"
    assert row.live.status == "unavailable"
    route = capability_for("api_service_route")
    assert route.state == "plannable-only"
    assert route.live.status == "unavailable"


def test_dependency_api_is_registered_as_an_unsupported_process_call_witness():
    """A flat one-level mixed-type list cannot witness an edge kind it never carries."""
    row = capability_for("dependency_api_as_process_call_witness")
    assert row.state == "unsupported"
    assert row.live.status == "conflicting"
    # And ProcessCall itself is therefore only corroborated live, never verified.
    assert capability_for("process_call").live.status == "corroborating_only"


def test_schedule_content_is_guidance_only_with_no_live_evidence():
    """Every schedule body in the capture was empty, so cron/interval shape
    has no evidence to model from."""
    row = capability_for("schedule_content")
    assert row.state == "guidance-only"
    assert row.live.status == "unavailable"


def test_account_capability_limits_were_never_captured():
    row = capability_for("account_capability_limits")
    assert row.state == "gated-no-evidence"
    assert row.source.status == "not_captured"
    assert row.documentation.status == "not_captured"
    assert row.live.status == "not_captured"


def test_deployment_evidence_does_not_support_an_apply_claim():
    """Reads establish that records exist and can be listed — not that one can be made.

    Deliberately NOT justified by "every live record is inactive": that was
    measured on one profile, is false on the other, and was never what the
    verdict rested on. Listing a deployment and creating one are different
    capabilities, and only the first was ever observed.
    """
    assert capability_for("deployment_unit").state == "plannable-only"
    assert capability_for("deployment_binding").state == "plannable-only"
    assert capability_for("topology_apply").state == "unsupported"
    assert capability_for("atomic_multi_process_deployment").state == "unsupported"


def test_schedule_is_bound_to_a_runtime_not_an_environment():
    assert capability_for("schedule_environment_binding").state == "unsupported"
    assert capability_for("schedule_binding").live.status == "verified"


def test_listener_status_is_not_accepted_as_a_route_witness():
    row = capability_for("listener_status_as_api_route_witness")
    assert row.state == "unsupported"
    # A conflicting source is REPORTED, never resolved by precedence.
    assert row.source.status == "conflicting"
    assert row.documentation.status == "conflicting"


# ---------------------------------------------------------------------------
# Gating behavior
# ---------------------------------------------------------------------------


def test_queues_and_event_streams_are_the_gated_subjects():
    assert GATED_SUBJECTS == {
        "external_queue",
        "external_event_stream",
        "queue_reference",
        "event_stream_reference",
        "account_capability_limits",
    }


@pytest.mark.parametrize("kind", ["external_queue", "external_event_stream"])
def test_a_gated_object_blocks_by_being_present_at_all(kind):
    """No relation need reference it — the OBJECT is the intent."""
    spec = _spec([_PROCESS, {"kind": kind, "key": "g", "resource_ref": "r"}])
    findings = collect_capability_findings(spec)
    assert [f.code for f in findings] == ["TOPOLOGY_CAPABILITY_GATED"]
    assert findings[0].phase == "capability"
    assert findings[0].subject == kind
    assert findings[0].path == "/objects/1"


@pytest.mark.parametrize(
    "obj_kind,rel_kind,role",
    [
        ("external_queue", "queue_reference", "external_queue"),
        ("external_event_stream", "event_stream_reference", "external_event_stream"),
    ],
)
def test_a_gated_relation_blocks_too(obj_kind, rel_kind, role):
    spec = _spec(
        [_PROCESS, {"kind": obj_kind, "key": "g", "resource_ref": "r"}],
        [{"kind": rel_kind, "key": "rr", "process": "p", role: "g"}],
    )
    findings = collect_capability_findings(spec)
    assert len(findings) == 2
    assert {f.path for f in findings} == {"/objects/1", "/relations/0"}
    assert all(f.severity == "error" for f in findings)


def test_a_gated_finding_carries_its_evidence_provenance():
    spec = _spec([_PROCESS, {"kind": "external_queue", "key": "q", "resource_ref": "r"}])
    finding = collect_capability_findings(spec)[0]
    assert finding.provenance == (capability_for("external_queue").live.reference,)


def test_a_spec_with_no_gated_kinds_produces_no_capability_findings():
    assert collect_capability_findings(_spec([_PROCESS])) == ()


# ---------------------------------------------------------------------------
# The report
# ---------------------------------------------------------------------------


def test_the_report_lists_every_registered_subject_including_absent_ones():
    """A report showing only what was authored would hide the gates entirely."""
    report = build_capability_report(_spec([_PROCESS]))
    assert len(report.entries) == len(SYSTEM_TOPOLOGY_CAPABILITIES)
    subjects = {e.subject for e in report.entries}
    assert "external_queue" in subjects
    assert "queue_mutation" in subjects


def test_the_report_marks_which_subjects_the_spec_actually_uses():
    report = build_capability_report(
        _spec(
            [_PROCESS, {"kind": "process", "key": "p2", "component_ref": "$ref:p2"}],
            [
                {
                    "kind": "process_call",
                    "key": "r",
                    "caller_process": "p",
                    "callee_process": "p2",
                }
            ],
        )
    )
    present = {e.subject for e in report.entries if e.present_in_spec}
    assert present == {"process", "process_call"}


def test_the_report_is_deterministically_ordered():
    report = build_capability_report(_spec([_PROCESS]))
    keys = [e.sort_key() for e in report.entries]
    assert keys == sorted(keys)


def test_the_report_records_the_registry_revision():
    report = build_capability_report(_spec([_PROCESS]))
    assert report.revision == SYSTEM_TOPOLOGY_CAPABILITY_REVISION


def test_a_capability_report_carries_no_authored_value():
    spec = _spec([{"kind": "process", "key": "secret-key", "component_ref": "$ref:secret"}])
    blob = build_capability_report(spec).model_dump_json()
    assert "secret-key" not in blob
    assert "secret" not in blob.replace("client_secret", "")
