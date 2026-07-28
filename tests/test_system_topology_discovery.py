"""Read-only discovery boundary tests (issue #144, M12.9).

The discovery port is the only place live data enters. These tests pin that it
reads exactly seven things, forwards exactly one profile, records pagination
honestly, and lets no raw XML or secret-shaped source field survive into a
snapshot.

The fake port below is deliberately hostile: every attribute that is NOT one of
the seven reads raises. A port that silently tolerated a ``deploy`` call would
make the whole no-mutation claim untestable.
"""

import json
import sys
import unicodedata
from pathlib import Path

import pytest

_project_root = Path(__file__).resolve().parent.parent
_src = str(_project_root / "src")
if _src not in sys.path:
    sys.path.insert(0, _src)

from boomi_mcp.models.system_topology import parse_system_topology_v1
from boomi_mcp.compiler.system_topology.capabilities import capability_for
from boomi_mcp.compiler.system_topology.discovery import (
    DISCOVERY_COMPONENT_TYPES,
    ReadOnlyTopologyDiscoveryPort,
    TopologyDiscoveryError,
    capture_topology_discovery_snapshot,
)
from boomi_mcp.compiler.system_topology.evidence import (
    normalize_dependency_corroboration,
    parse_api_service_component_evidence,
    parse_process_component_evidence,
)

_READ_METHODS = frozenset(
    {
        "list_profiles",
        "query_components",
        "read_component_xml",
        "read_component_dependencies",
        "list_environments",
        "list_schedules",
        "list_deployments",
    }
)


class HostileFakePort:
    """Implements the seven reads; explodes on anything else."""

    def __init__(self, *, profiles=("prof-a", "prof-b"), components=None,
                 environments=(), schedules=(), deployments=()):
        self._profiles = profiles
        self._components = components or {}
        self._environments = environments
        self._schedules = schedules
        self._deployments = deployments
        self.calls = []

    def __getattr__(self, name):
        # Only reached for attributes not defined below.
        raise AssertionError(f"discovery touched a non-read attribute: {name}")

    def list_profiles(self):
        self.calls.append(("list_profiles", None))
        return self._profiles

    def query_components(self, profile, component_type):
        self.calls.append(("query_components", profile, component_type))
        return self._components.get(component_type, {"components": []})

    def read_component_xml(self, profile, component_ref):
        self.calls.append(("read_component_xml", profile, component_ref))
        return None

    def read_component_dependencies(self, profile, component_ref):
        self.calls.append(("read_component_dependencies", profile, component_ref))
        return []

    def list_environments(self, profile):
        self.calls.append(("list_environments", profile))
        return self._environments

    def list_schedules(self, profile):
        self.calls.append(("list_schedules", profile))
        return self._schedules

    def list_deployments(self, profile):
        self.calls.append(("list_deployments", profile))
        return self._deployments


def _spec(profile="prof-a"):
    return parse_system_topology_v1(
        {
            "version": "1",
            "profile_ref": profile,
            "objects": [{"kind": "process", "key": "p", "component_ref": "$ref:k"}],
            "relations": [],
        }
    )


def _capture(port, profile="prof-a"):
    return capture_topology_discovery_snapshot(
        _spec(profile),
        port,
        captured_at="2026-01-01T00:00:00Z",
        source_revision="rev-1",
        service_release="rel-1",
    )


# ---------------------------------------------------------------------------
# The read ledger
# ---------------------------------------------------------------------------


def test_the_port_protocol_declares_exactly_seven_reads():
    declared = {
        name
        for name in dir(ReadOnlyTopologyDiscoveryPort)
        if not name.startswith("_")
    }
    assert declared == _READ_METHODS


def test_every_port_method_is_a_read_verb():
    """Checked by the VERB, not by substring.

    A substring hunt would flag ``list_deployments`` for containing "deploy" —
    which is a read whose noun happens to name a mutation. The precise claim is
    that every method's leading verb is a read, so the check is on the prefix.
    """
    read_prefixes = ("list_", "query_", "read_", "get_")
    for name in _READ_METHODS:
        assert name.startswith(read_prefixes), name


def test_no_port_method_leads_with_a_mutating_verb():
    mutating = (
        "create_",
        "update_",
        "delete_",
        "deploy_",
        "undeploy_",
        "attach_",
        "detach_",
        "execute_",
        "enable_",
        "disable_",
        "clear_",
        "move_",
        "write_",
        "set_",
        "put_",
        "post_",
        "patch_",
    )
    for name in _READ_METHODS:
        assert not name.startswith(mutating), name


def test_the_read_verb_check_would_actually_catch_a_mutation():
    """Positive control — the two checks above must not pass vacuously."""
    read_prefixes = ("list_", "query_", "read_", "get_")
    assert not "deploy_package".startswith(read_prefixes)
    assert "deploy_package".startswith(("deploy_",))


def test_discovery_calls_only_the_declared_reads():
    port = HostileFakePort()
    _capture(port)
    assert {call[0] for call in port.calls} <= _READ_METHODS


def test_the_profile_is_validated_before_anything_else_is_read():
    port = HostileFakePort()
    _capture(port)
    assert port.calls[0][0] == "list_profiles"


def test_an_unknown_profile_is_a_hard_error_not_an_empty_snapshot():
    """An empty snapshot reads as 'this account has nothing' — a typo must not
    become a confident claim that no queues exist."""
    port = HostileFakePort(profiles=("prof-b",))
    with pytest.raises(TopologyDiscoveryError):
        _capture(port, profile="prof-a")


def test_exactly_the_spec_profile_is_forwarded_to_every_call():
    port = HostileFakePort()
    _capture(port, profile="prof-a")
    for call in port.calls:
        if call[0] == "list_profiles":
            continue
        assert call[1] == "prof-a", call


def test_every_declared_component_type_is_queried():
    port = HostileFakePort()
    _capture(port)
    queried = [c[2] for c in port.calls if c[0] == "query_components"]
    assert queried == list(DISCOVERY_COMPONENT_TYPES)


def test_two_profiles_produce_isolated_facts():
    port_a = HostileFakePort(
        components={"process": {"components": [{"component_id": "a1", "type": "process"}]}}
    )
    port_b = HostileFakePort(
        components={"process": {"components": [{"component_id": "b1", "type": "process"}]}}
    )
    snap_a = _capture(port_a, "prof-a")
    snap_b = _capture(port_b, "prof-b")
    assert {c.component_id for c in snap_a.components} == {"a1"}
    assert {c.component_id for c in snap_b.components} == {"b1"}
    assert all(c.profile == "prof-a" for c in snap_a.components)
    assert all(c.profile == "prof-b" for c in snap_b.components)


# ---------------------------------------------------------------------------
# Pagination provenance
# ---------------------------------------------------------------------------


def test_pagination_records_returned_total_and_truncation():
    """The live census that motivated this: 100 returned, 186 available."""
    port = HostileFakePort(
        components={
            "documentcache": {
                "components": [
                    {"component_id": f"c{i}", "type": "documentcache"} for i in range(100)
                ],
                "total_available": 186,
                "has_more": True,
            }
        }
    )
    snapshot = _capture(port)
    page = [p for p in snapshot.pagination if p.component_type == "documentcache"][0]
    assert page.returned_count == 100
    assert page.total_available == 186
    assert page.has_more is True
    assert page.truncated is True


def test_a_complete_page_is_not_truncated():
    port = HostileFakePort(
        components={
            "process": {
                "components": [{"component_id": "p1", "type": "process"}],
                "total_available": 1,
                "has_more": False,
            }
        }
    )
    snapshot = _capture(port)
    page = [p for p in snapshot.pagination if p.component_type == "process"][0]
    assert page.truncated is False


def test_a_short_page_without_has_more_is_still_truncated():
    """``total_available`` exceeding the returned count is truncation on its own."""
    port = HostileFakePort(
        components={
            "process": {
                "components": [{"component_id": "p1", "type": "process"}],
                "total_available": 50,
            }
        }
    )
    snapshot = _capture(port)
    page = [p for p in snapshot.pagination if p.component_type == "process"][0]
    assert page.truncated is True


def test_every_component_query_gets_a_provenance_row():
    port = HostileFakePort()
    snapshot = _capture(port)
    assert {p.component_type for p in snapshot.pagination} == set(
        DISCOVERY_COMPONENT_TYPES
    )


# ---------------------------------------------------------------------------
# The four live-evidence facts, as behavior
# ---------------------------------------------------------------------------


def test_zero_webservice_components_leaves_the_asc_evidence_unavailable():
    port = HostileFakePort(components={"webservice": {"components": []}})
    snapshot = _capture(port)
    assert not [c for c in snapshot.components if c.component_type == "webservice"]
    assert capability_for("api_service").live.status == "unavailable"


def test_zero_queue_components_leaves_queues_gated():
    port = HostileFakePort(components={"queue": {"components": []}})
    snapshot = _capture(port)
    assert not [c for c in snapshot.components if c.component_type == "queue"]
    assert capability_for("external_queue").state == "gated-no-evidence"


def test_an_empty_schedule_body_is_recorded_without_inventing_content():
    """Every live schedule looked like this. No cron field exists to fill."""
    port = HostileFakePort(
        schedules=[
            {
                "process_id": "proc-1",
                "atom_id": "atom-1",
                "schedules": [],
                "retry": {"max_retry": 5},
                "active": False,
            }
        ]
    )
    snapshot = _capture(port)
    binding = snapshot.schedule_bindings[0]
    assert binding.process_id == "proc-1"
    assert binding.runtime_id == "atom-1"
    assert binding.active is False
    assert binding.has_schedule_body is False
    # No cron/interval/retry field survives anywhere in the snapshot.
    blob = snapshot.model_dump_json()
    for forbidden in ("cron", "interval", "max_retry"):
        assert forbidden not in blob


def test_a_schedule_binds_a_runtime_never_an_environment():
    port = HostileFakePort(
        schedules=[{"process_id": "proc-1", "atom_id": "atom-1", "schedules": []}],
        environments=[{"id": "env-1", "classification": "TEST"}],
    )
    snapshot = _capture(port)
    assert snapshot.runtimes[0].runtime_id == "atom-1"
    binding_fields = set(snapshot.schedule_bindings[0].model_dump())
    assert "environment_id" not in binding_fields


def test_inactive_deployments_do_not_imply_an_apply_capability():
    port = HostileFakePort(
        deployments=[
            {"component_id": "c1", "environment_id": "env-1", "active": False},
            {"component_id": "c2", "environment_id": "env-1", "active": False},
        ]
    )
    snapshot = _capture(port)
    assert all(d.active is False for d in snapshot.deployments)
    assert capability_for("topology_apply").state == "unsupported"


def test_environment_classification_is_captured_verbatim():
    port = HostileFakePort(
        environments=[
            {"id": "env-1", "classification": "TEST"},
            {"id": "env-2", "classification": "PROD"},
        ]
    )
    snapshot = _capture(port)
    assert {e.classification for e in snapshot.environments} == {"TEST", "PROD"}


def test_a_profile_with_no_prod_environment_is_representable():
    """One of the two live profiles has two TEST environments and no PROD."""
    port = HostileFakePort(
        environments=[
            {"id": "env-1", "classification": "TEST"},
            {"id": "env-2", "classification": "TEST"},
        ]
    )
    snapshot = _capture(port)
    assert {e.classification for e in snapshot.environments} == {"TEST"}


# ---------------------------------------------------------------------------
# Witnesses vs corroboration
# ---------------------------------------------------------------------------


def test_a_process_dependency_without_processcall_xml_is_only_corroboration():
    """The central evidence correction, at the boundary that produces it."""
    rows = normalize_dependency_corroboration(
        "caller-1",
        [("callee-1", "process"), ("profile-1", "profile.json"), ("cache-1", "documentcache")],
    )
    assert len(rows) == 3
    # A corroboration row is a DIFFERENT type from every witness, so promoting
    # one is a type error rather than a judgement call at a call site.
    from boomi_mcp.compiler.system_topology.context import (
        DependencyCorroborationV1,
        ProcessCallEvidenceV1,
    )

    assert all(isinstance(row, DependencyCorroborationV1) for row in rows)
    assert not any(isinstance(row, ProcessCallEvidenceV1) for row in rows)
    # It preserves the mixed types faithfully — the API really does return them.
    assert {row.child_component_type for row in rows} == {
        "process",
        "profile.json",
        "documentcache",
    }


def test_processcall_xml_does_create_a_witness():
    calls, uses = parse_process_component_evidence(
        "caller-1",
        '<process><processcall processId="callee-1"/></process>',
    )
    assert len(calls) == 1
    assert calls[0].callee_component_ref == "callee-1"
    assert calls[0].witness == "component_xml"
    assert uses == ()


def test_shared_resource_xml_creates_typed_use_witnesses():
    calls, uses = parse_process_component_evidence(
        "proc-1",
        '<process><documentcache documentCacheId="cache-1"/>'
        '<processproperty componentId="prop-1"/></process>',
    )
    assert calls == ()
    kinds = {(u.resource_kind, u.resource_component_ref) for u in uses}
    assert kinds == {("document_cache", "cache-1"), ("process_property", "prop-1")}


def test_a_wss_api_service_creates_a_route_witness():
    routes = parse_api_service_component_evidence(
        "asc-1",
        '<webservice><wss listen="true"/><operation processId="listener-1"/></webservice>',
    )
    assert len(routes) == 1
    assert routes[0].listener_component_ref == "listener-1"
    assert routes[0].witness == "component_xml"


def test_a_non_listen_api_service_creates_no_route_witness():
    """An ASC whose operations are not listen-shaped does not make targets listeners."""
    routes = parse_api_service_component_evidence(
        "asc-1", '<webservice><operation processId="listener-1"/></webservice>'
    )
    assert routes == ()


def test_route_witnesses_carry_no_path_method_or_endpoint_detail():
    routes = parse_api_service_component_evidence(
        "asc-1",
        '<webservice><wss listen="true"/>'
        '<operation processId="listener-1" path="/secret/path" method="POST"/></webservice>',
    )
    blob = json.dumps([r.model_dump() for r in routes])
    assert "/secret/path" not in blob
    assert "POST" not in blob


def test_a_duplicate_target_in_xml_yields_one_edge():
    calls, _ = parse_process_component_evidence(
        "caller-1",
        '<process><processcall processId="x"/><processcall processId="x"/></process>',
    )
    assert len(calls) == 1


def test_oversized_xml_is_refused_rather_than_truncated():
    """A half-parsed document produces confidently wrong witnesses.

    The fixture must be WELL-FORMED and oversized, or it proves nothing: a
    malformed one is refused by the parser regardless, so the size bound could
    be deleted with the test still green. This document parses cleanly and is
    rejected only because of its length.
    """
    from boomi_mcp.compiler.system_topology.evidence import _MAX_XML_CHARS

    # Sized from the constant, not a magic number: a hardcoded length silently
    # stops exceeding the bound the day the bound is raised, and the test goes
    # back to proving nothing.
    padding = "<pad/>" * ((_MAX_XML_CHARS // 6) + 1000)
    huge = f'<process><processcall processId="x"/>{padding}</process>'
    assert len(huge) > _MAX_XML_CHARS, (len(huge), _MAX_XML_CHARS)
    # Well-formed: the same document under the bound DOES yield a witness.
    small = '<process><processcall processId="x"/><pad/></process>'
    assert parse_process_component_evidence("caller-1", small)[0]

    calls, uses = parse_process_component_evidence("caller-1", huge)
    assert calls == ()
    assert uses == ()


# ---------------------------------------------------------------------------
# Redaction
# ---------------------------------------------------------------------------


def test_raw_xml_never_survives_into_a_witness():
    xml = (
        '<process><processcall processId="callee-1"/>'
        '<connection password="hunter2" host="internal.example"/></process>'
    )
    calls, uses = parse_process_component_evidence("caller-1", xml)
    blob = json.dumps([c.model_dump() for c in calls] + [u.model_dump() for u in uses])
    assert "hunter2" not in blob
    assert "internal.example" not in blob
    assert "<process>" not in blob


def test_a_secret_bearing_source_field_never_reaches_the_snapshot():
    port = HostileFakePort(
        components={
            "process": {
                "components": [
                    {
                        "component_id": "p1",
                        "type": "process",
                        "password": "hunter2",
                        "connection_properties": {"host": "internal.example"},
                    }
                ]
            }
        },
        environments=[
            {"id": "env-1", "classification": "TEST", "extensions": {"token": "abc"}}
        ],
    )
    snapshot = _capture(port)
    blob = snapshot.model_dump_json()
    for secret in ("hunter2", "internal.example", "abc", "extensions"):
        assert secret not in blob, secret


def test_the_snapshot_records_release_and_source_provenance():
    """The live service runs behind the checkout; a claim that cannot say which
    release it observed is not a claim anyone can check."""
    snapshot = _capture(HostileFakePort())
    assert snapshot.source_revision == "rev-1"
    assert snapshot.service_release == "rel-1"
    assert snapshot.captured_at == "2026-01-01T00:00:00Z"


def test_discovery_reads_no_account_limits_listener_status_or_extensions():
    """Deliberately absent surfaces, asserted against the declared read set.

    Each omission has a stated reason: no limit capture exists, listener status
    conflicts with its own documentation, environment extensions carry override
    VALUES, and the shared-resource module exposes unrelated secret-bearing
    resources with mixed write verbs.
    """
    declared = " ".join(sorted(_READ_METHODS)).lower()
    for forbidden in (
        "limit",
        "quota",
        "listener",
        "extension",
        "shared_resource",
        "web_server",
        "channel",
        "queue_message",
        "execution",
    ):
        assert forbidden not in declared, forbidden


def test_the_snapshot_golden_pin():
    committed = json.loads(
        (_project_root / "tests" / "fixtures" / "system_topology" / "topology_discovery_v1.json").read_text()
    )
    port = HostileFakePort(
        components={
            "process": {
                "components": [{"component_id": "component-placeholder-1", "type": "process"}],
                "total_available": 2,
                "has_more": True,
            }
        },
        environments=[{"id": "environment-placeholder-1", "classification": "TEST"}],
        schedules=[
            {"process_id": "component-placeholder-1", "atom_id": "runtime-placeholder-1", "schedules": []}
        ],
        deployments=[
            {
                "component_id": "component-placeholder-1",
                "environment_id": "environment-placeholder-1",
                "active": False,
            }
        ],
    )
    snapshot = _capture(port)
    assert json.loads(snapshot.model_dump_json()) == committed


# ---------------------------------------------------------------------------
# QA #207/#208/#209 — three claims that were wrong, now pinned
# ---------------------------------------------------------------------------


def test_an_active_deployment_is_recorded_faithfully():
    """QA #207. Active deployment records DO exist in the live account.

    An earlier draft justified the deployment verdict with "every live
    deployment record is inactive" — measured on one profile and false on the
    other. The verdict (`plannable-only`, no apply path) was right; the reason
    was not. This pins that an active record is recorded without drama and
    changes no capability.
    """
    port = HostileFakePort(
        deployments=[
            {"component_id": "c1", "environment_id": "env-1", "active": True},
            {"component_id": "c2", "environment_id": "env-1", "active": False},
        ]
    )
    snapshot = _capture(port)
    assert {d.active for d in snapshot.deployments} == {True, False}
    # The verdict is unmoved: listing a deployment and creating one are
    # different capabilities, and only the first was ever observed.
    assert capability_for("deployment_unit").state == "plannable-only"
    assert capability_for("topology_apply").state == "unsupported"


def test_no_published_string_claims_every_deployment_is_inactive():
    """QA #207. The false universal reached callers through ``derive_guidance``.

    Asserted against the caller-visible text, not the source, because that is
    where it did damage: a plan document emitted "Every deployment record
    observed live is inactive" while its own embedded snapshot carried an
    active record.
    """
    from boomi_mcp.compiler.system_topology.capabilities import (
        SYSTEM_TOPOLOGY_CAPABILITIES,
    )
    from boomi_mcp.compiler.system_topology.relations import derive_guidance
    from boomi_mcp.compiler.system_topology.context import (
        TopologyResolutionContextV1,
    )
    from boomi_mcp.compiler.system_topology.context import prepare_topology_context

    spec = parse_system_topology_v1(
        {
            "version": "1",
            "profile_ref": "prof",
            "objects": [
                {"kind": "process", "key": "p", "component_ref": "$ref:k"},
                {"kind": "environment", "key": "e", "environment_ref": "env-1"},
                {"kind": "deployment_unit", "key": "u"},
            ],
            "relations": [
                {
                    "kind": "deployment_binding",
                    "key": "rd",
                    "deployment_unit": "u",
                    "process": "p",
                    "environment": "e",
                }
            ],
        }
    )
    prepared = prepare_topology_context(TopologyResolutionContextV1(profile="prof"))
    text = " ".join(g.message for g in derive_guidance(spec, prepared)).lower()
    for false_universal in (
        "every deployment record",
        "all deployments are inactive",
        "no deployment is active",
    ):
        assert false_universal not in text, false_universal
    # And no evidence token asserts it either.
    tokens = " ".join(
        leg.reference
        for row in SYSTEM_TOPOLOGY_CAPABILITIES.values()
        for leg in (row.source, row.documentation, row.live)
    )
    assert "all-inactive" not in tokens


def test_a_failed_query_is_not_recorded_as_a_genuine_zero():
    """QA #208. "The query did not answer" must not read as "there are none".

    This lands on the claim the registry is most exposed on: all four queue and
    Event Streams gates cite an empty ``query_components`` result as their live
    evidence.
    """
    port = HostileFakePort(
        components={"queue": {"_success": False, "error": "upstream unavailable"}}
    )
    snapshot = _capture(port)
    page = [p for p in snapshot.pagination if p.component_type == "queue"][0]
    assert page.observed is False
    # Fail closed: unknown is treated as truncated, so absence is not evidence.
    assert page.truncated is True
    assert page.total_available is None


def test_a_genuine_empty_listing_is_still_observed():
    """The counterpart: a real zero must stay distinguishable from a failure."""
    port = HostileFakePort(components={"queue": {"_success": True, "components": []}})
    snapshot = _capture(port)
    page = [p for p in snapshot.pagination if p.component_type == "queue"][0]
    assert page.observed is True
    assert page.returned_count == 0
    assert page.truncated is False


@pytest.mark.parametrize(
    "payload",
    [None, {}, {"_success": False}, {"error": "boom"}, "not a mapping", []],
)
def test_every_non_answer_shape_is_marked_unobserved(payload):
    port = HostileFakePort(components={"process": payload})
    snapshot = _capture(port)
    page = [p for p in snapshot.pagination if p.component_type == "process"][0]
    assert page.observed is False, payload


def test_an_unobserved_query_raises_its_own_unresolved_decision():
    """Reported separately from truncation — different problem, different action."""
    from boomi_mcp.compiler.system_topology import plan_system_topology
    from boomi_mcp.compiler.system_topology.context import (
        TopologyResolutionContextV1,
    )

    port = HostileFakePort(components={"queue": {"_success": False}})
    snapshot = _capture(port)
    spec = _spec("prof-a")
    plan = plan_system_topology(
        spec,
        TopologyResolutionContextV1(profile="prof-a", snapshot=snapshot),
    )
    subjects = {d.subject for d in plan.unresolved_decisions}
    assert "discovery_unobserved_query" in subjects


def test_a_missing_environment_classification_is_never_defaulted():
    """QA #209. Defaulting to TEST manufactures an observation.

    Its proven consequence: an author correctly declaring PROD gets blocked by
    TOPOLOGY_ENVIRONMENT_MISMATCH against a value nobody read — despite the
    environment collector being written so only a real CONTRADICTION is a
    finding.
    """
    port = HostileFakePort(environments=[{"id": "env-1"}])
    snapshot = _capture(port)
    assert snapshot.environments[0].classification is None


def test_an_unclassified_environment_does_not_contradict_an_authored_prod():
    from boomi_mcp.compiler.system_topology import validate_system_topology
    from boomi_mcp.compiler.system_topology.context import (
        TopologyResolutionContextV1,
    )

    port = HostileFakePort(environments=[{"id": "env-1"}])
    snapshot = _capture(port)
    spec = parse_system_topology_v1(
        {
            "version": "1",
            "profile_ref": "prof-a",
            "objects": [
                {"kind": "process", "key": "p", "component_ref": "$ref:k"},
                {
                    "kind": "environment",
                    "key": "e",
                    "environment_ref": "env-1",
                    "classification": "PROD",
                },
            ],
            "relations": [],
        }
    )
    report = validate_system_topology(
        spec, TopologyResolutionContextV1(profile="prof-a", snapshot=snapshot)
    )
    codes = [d.code for d in report.errors]
    assert "TOPOLOGY_ENVIRONMENT_MISMATCH" not in codes, codes


def test_an_observed_classification_still_contradicts_a_wrong_authored_one():
    """The guard must not have disabled the real check."""
    from boomi_mcp.compiler.system_topology import validate_system_topology
    from boomi_mcp.compiler.system_topology.context import (
        TopologyResolutionContextV1,
    )

    port = HostileFakePort(environments=[{"id": "env-1", "classification": "TEST"}])
    snapshot = _capture(port)
    spec = parse_system_topology_v1(
        {
            "version": "1",
            "profile_ref": "prof-a",
            "objects": [
                {"kind": "process", "key": "p", "component_ref": "$ref:k"},
                {
                    "kind": "environment",
                    "key": "e",
                    "environment_ref": "env-1",
                    "classification": "PROD",
                },
            ],
            "relations": [],
        }
    )
    report = validate_system_topology(
        spec, TopologyResolutionContextV1(profile="prof-a", snapshot=snapshot)
    )
    assert "TOPOLOGY_ENVIRONMENT_MISMATCH" in [d.code for d in report.errors]


def test_an_unrecognized_classification_value_is_dropped_not_coerced():
    port = HostileFakePort(environments=[{"id": "env-1", "classification": "STAGING"}])
    snapshot = _capture(port)
    assert snapshot.environments[0].classification is None


_RETRACTED_DEPLOYMENT_CLAIMS = (
    "every live deployment record",
    "deployment record observed is inactive",
    "every deployment record observed",
    # Keyed on the PREDICATE, not just the subject. The claim minus one word —
    # "every deployment record is inactive" — is the most plausible rewrite from
    # memory, and a subject-only list publishes it cleanly.
    "deployment record is inactive",
    "all deployments are inactive",
    "no deployment is active",
    "all-inactive",
)

#: Markers that identify a QUOTATION of the retracted claim rather than an
#: assertion of it. Matched in a window AROUND the phrase, never file-wide: an
#: exemption keyed on the whole file would permanently un-scan the very module
#: that records the retraction, which is also the module the claim came from.
_RETRACTION_MARKERS = (
    "false as stated",
    "was wrong",
    "retracted",
    "earlier draft",
    "no longer",
)

#: Characters either side of a match to search for a retraction marker. Wide
#: enough to span the sentence that frames a quotation, narrow enough that a
#: bare assertion elsewhere in the same file is not covered by it.
_RETRACTION_WINDOW = 400

#: Strips Unicode category ``Cf`` (format): U+200B/200C/200D/FEFF, U+2060 WORD
#: JOINER, U+00AD SOFT HYPHEN, U+180E. An enumerated tuple closes only the
#: members someone thought of — the first version of this omitted U+2060, the
#: documented successor to a codepoint it did include — so the check is by
#: category instead.
#:
#: It does NOT close the whole invisible-character class, and saying so here
#: would be a false universal in the one file whose job is catching those.
#: ``Default_Ignorable_Code_Point`` also spans ``Mn`` (U+034F CGJ, the U+FE00
#: variation selectors) and ``Lo`` (U+3164, U+115F/1160, U+FFA0), and none of
#: those is stripped. They are adversarial-only, and the same accepted limit
#: applies as to a hyphenated line break: this is a tripwire against a revert,
#: not a proof about prose.
#:
#: (U+00A0 and U+2009 are ``Zs`` and DO split, so whitespace collapsing already
#: handles them — verified, not assumed.)
def _is_invisible(char):
    return unicodedata.category(char) == "Cf"


def _normalized(text):
    """Lowercased, zero-width stripped, every whitespace run collapsed to one space.

    Load-bearing, not cosmetic. Every guarded site is prose wrapped at ~79
    columns, so the phrases straddle newlines in the source: the genuine
    quotation in ``capabilities.py`` reads ``every\n   live deployment record``
    and a raw substring scan does not see it. An earlier version of these guards
    matched un-normalized text, which meant the phrase list produced ZERO hits
    across all scanned files and the exemption branch never executed once —
    the tests passed vacuously, and the claim could be reintroduced verbatim
    simply by wrapping it one word earlier.

    Scope note, stated rather than implied: a substring list is a TRIPWIRE
    against a revert, not a proof about prose. A hyphenated line break
    (``deployment re-\ncord``) or a free rewording still evades it. Those are
    caught by review, not by this test — which is why the guard also runs over
    the published schema, where a reviewer can see the whole description at once.
    """
    text = "".join(char for char in text if not _is_invisible(char))
    return " ".join(text.lower().split())


def test_the_published_schema_makes_no_false_deployment_universal():
    """QA #210. A pydantic class docstring IS published schema.

    ``model_json_schema()`` emits a class docstring verbatim as the ``$def``
    description, so a retracted claim left in one reaches every consumer of the
    contract — and the committed schema golden then pins it. The earlier #207
    regression test scanned only ``derive_guidance`` output and evidence tokens,
    which is why this site survived.
    """
    from boomi_mcp.models.system_topology import (
        canonical_system_topology_schema_json,
        system_topology_v1_json_schema,
    )

    # Walk the PARSED schema, not the serialized string. In JSON a docstring's
    # newlines are escaped as a literal backslash-n, which survives whitespace
    # normalization intact — so a wrapped claim would slip past a scan of the
    # serialized bytes while sitting in plain sight in the description a
    # consumer actually reads.
    descriptions = []

    def walk(node):
        if isinstance(node, dict):
            for key, value in node.items():
                if key in ("description", "title") and isinstance(value, str):
                    descriptions.append(value)
                walk(value)
        elif isinstance(node, list):
            for item in node:
                walk(item)

    walk(system_topology_v1_json_schema())
    assert descriptions, "no descriptions found — the walk is broken"
    for description in descriptions:
        normalized = _normalized(description)
        for false_universal in _RETRACTED_DEPLOYMENT_CLAIMS:
            assert false_universal not in normalized, (false_universal, description[:80])

    # Belt and braces on the serialized form, with JSON escapes unwrapped.
    serialized = _normalized(
        canonical_system_topology_schema_json().replace("\\n", " ").replace("\\t", " ")
    )
    for false_universal in _RETRACTED_DEPLOYMENT_CLAIMS:
        assert false_universal not in serialized, false_universal


def _scanned_files():
    roots = (
        _project_root / "src" / "boomi_mcp" / "compiler" / "system_topology",
        _project_root / "src" / "boomi_mcp" / "models",
        _project_root / "docs" / "architecture",
    )
    for root in roots:
        for path in sorted(root.rglob("*")):
            if path.is_file() and path.suffix in (".py", ".md"):
                yield path


def _retraction_offenders(paths):
    """THE guard implementation. Both the guard and its controls call this.

    Hoisted deliberately. An earlier version inlined this logic in the guard and
    had the controls re-implement the window arithmetic on strings they built
    themselves — which graded the ingredients, not the guard. Mutation testing
    showed the consequence: reverting the windowed exemption to a file-wide one
    (the original #211 defect, character for character) survived the entire
    suite, because no control ever invoked the code being reverted.
    """
    offenders = []
    for path in paths:
        text = _normalized(path.read_text())
        for phrase in _RETRACTED_DEPLOYMENT_CLAIMS:
            start = 0
            while True:
                index = text.find(phrase, start)
                if index == -1:
                    break
                window = text[
                    max(0, index - _RETRACTION_WINDOW) : index
                    + len(phrase)
                    + _RETRACTION_WINDOW
                ]
                if not any(marker in window for marker in _RETRACTION_MARKERS):
                    offenders.append((path.name, phrase))
                start = index + len(phrase)
    return offenders


def test_no_source_file_in_the_feature_asserts_the_retracted_claim():
    """The same class of miss, checked across every file #144 touches.

    Source-wide rather than per-surface: the claim escaped once by living in a
    module the fix delta did not include, so the check must not be scoped to the
    modules anyone remembered to look at.

    A quotation is allowed; an assertion is not.
    """
    assert _retraction_offenders(_scanned_files()) == []


def test_the_scan_visits_the_module_the_claim_came_from():
    """QA #213/M9. Dropping a scan root must not silently shrink coverage."""
    scanned = {path.resolve() for path in _scanned_files()}
    for required in (
        _project_root
        / "src"
        / "boomi_mcp"
        / "compiler"
        / "system_topology"
        / "capabilities.py",
        _project_root / "src" / "boomi_mcp" / "models" / "system_topology.py",
        _project_root / "docs" / "architecture" / "SYSTEM_TOPOLOGY_V1.md",
    ):
        assert required.resolve() in scanned, required.name


def test_the_retraction_scan_is_not_vacuous():
    """Positive control: the scan must FIND the one genuine quotation there is.

    Reached through ``_scanned_files()`` rather than a hard-coded path, so this
    also fails if the file stops being visited.
    """
    target = (
        _project_root
        / "src"
        / "boomi_mcp"
        / "compiler"
        / "system_topology"
        / "capabilities.py"
    )
    assert target.resolve() in {p.resolve() for p in _scanned_files()}
    text = _normalized(target.read_text())
    found = [p for p in _RETRACTED_DEPLOYMENT_CLAIMS if p in text]
    assert found, "the matcher no longer sees the known wrapped quotation"


def _write(tmp_path, name, body):
    path = tmp_path / name
    path.write_text(body)
    return path


def test_the_guard_reports_an_assertion_far_from_a_marker(tmp_path):
    """QA #213/M6. Reverting the exemption to file-wide must fail HERE.

    Driven through ``_retraction_offenders`` — the guard's own body — with a
    marker present in the file but far outside the window. A file-wide
    exemption returns no offenders and this goes red.
    """
    path = _write(
        tmp_path,
        "reverted.py",
        '"""An earlier draft was wrong about this.\n\n'
        + ("padding padding padding padding padding.\n" * 40)
        + '\nEvery live deployment record observed is inactive.\n"""\n',
    )
    offenders = _retraction_offenders([path])
    assert offenders, "a file-wide exemption would return nothing here"
    assert all(name == "reverted.py" for name, _ in offenders)


def test_the_guard_allows_a_quotation_beside_its_retraction(tmp_path):
    """The exemption must still work, or the guard forbids documenting the fix."""
    path = _write(
        tmp_path,
        "documented.py",
        '"""An earlier draft justified this with "every live deployment record\n'
        'is inactive" — measured on one profile, and false on the other."""\n',
    )
    assert _retraction_offenders([path]) == []


def test_the_guard_is_case_insensitive(tmp_path):
    """QA #213/M5b. Dropping ``.lower()`` must fail here."""
    path = _write(
        tmp_path, "shouty.py", "# EVERY LIVE DEPLOYMENT RECORD OBSERVED IS INACTIVE.\n"
    )
    assert _retraction_offenders([path])


def test_the_guard_sees_through_line_wrapping(tmp_path):
    """QA #212. The defect that made the first version vacuous."""
    path = _write(
        tmp_path,
        "wrapped.py",
        '"""Creating a package is a mutation, and every\n'
        "live deployment record observed is inactive, so no apply capability is\n"
        'inferable."""\n',
    )
    assert _retraction_offenders([path])


def test_the_guard_sees_through_a_zero_width_space(tmp_path):
    """QA #214. ``str.split()`` does not treat U+200B as whitespace.

    The zero-width character sits INSIDE a word, so it breaks every phrase in
    the list at once. Placing it between phrases would leave one of them
    contiguous and the control would pass without the stripping doing any work —
    which is what a first attempt at this test did.
    """
    path = _write(
        tmp_path,
        "sneaky.py",
        "# every live deployment re\u200bcord observed is inactive.\n",
    )
    assert _retraction_offenders([path])


def test_the_zero_width_control_is_not_satisfied_by_a_neighbouring_phrase(tmp_path):
    """Guards the guard: without stripping, this file must be invisible.

    Asserted against the un-normalized text so the control states its own
    premise — if some future phrase happens to survive the splice, this fails
    and the test above stops being a real control.
    """
    body = "# every live deployment re\u200bcord observed is inactive.\n"
    raw = " ".join(body.lower().split())  # normalization WITHOUT zero-width stripping
    assert not any(phrase in raw for phrase in _RETRACTED_DEPLOYMENT_CLAIMS)


def test_the_guard_catches_the_claim_minus_one_word(tmp_path):
    """QA #214. The likeliest rewrite from memory drops "live"."""
    path = _write(tmp_path, "rewritten.py", "# Every deployment record is inactive.\n")
    assert _retraction_offenders([path])


def test_a_clean_file_produces_no_offenders(tmp_path):
    """Negative control: the guard must not flag ordinary deployment prose."""
    path = _write(
        tmp_path,
        "clean.py",
        "# Live reads establish that deployment records exist and can be listed;\n"
        "# they establish nothing about creating one.\n",
    )
    assert _retraction_offenders([path]) == []



# ---------------------------------------------------------------------------
# QA #215/#216/#217 — the guard's own machinery, mutation-tested
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("phrase", _RETRACTED_DEPLOYMENT_CLAIMS)
def test_every_listed_phrase_is_individually_enforced(tmp_path, phrase):
    """QA #215. Each entry must be deletable ONLY by failing a test.

    A rewrite of these guards dropped the one control that pinned specific
    phrases, and six of seven entries then became free to delete with the whole
    suite green — which let a mutant prune two phrases and republish the claim
    into the pydantic docstring undetected. Parametrizing over the list means
    the list pins itself: removing an entry removes its test case, and the
    remaining cases still fail if the matcher stops finding it.
    """
    path = _write(tmp_path, "asserted.py", f"# Note: {phrase} in this account.\n")
    offenders = _retraction_offenders([path])
    assert (path.name, phrase) in offenders, offenders


#: The sentences that were actually retracted, written out in full and
#: INDEPENDENTLY of ``_RETRACTED_DEPLOYMENT_CLAIMS``. This is the contract; the
#: phrase list is one implementation of it.
#:
#: The distinction is load-bearing. A control that parametrizes over the phrase
#: list can only ever pin the list's cardinality: swap an entry for an inert
#: string and the count still passes, while the sentence that entry existed to
#: catch sails into the published ``$def`` description. Grading behavior against
#: fixed sentences closes deletion and substitution in one control — and still
#: lets someone legitimately refactor the phrase list, because the only thing
#: asserted is that these sentences remain catchable. (Reordering is NOT closed,
#: and should not be: tuple order only permutes append order in
#: ``_retraction_offenders``, so a permutation is a genuine no-op and a test that
#: went red on one would be brittle.)
_RETRACTED_SENTENCES = (
    "Every live deployment record observed is inactive.",
    "Deliberately not a package: creating a package is a mutation, and every\n"
    "live deployment record observed is inactive, so no apply capability is\n"
    "inferable.",
    "Every deployment record observed is inactive, so no apply or "
    "active-lifecycle capability is inferable from it.",
    "Every deployment record is inactive.",
    "All deployments are inactive.",
    "No deployment is active.",
    "Deployment intent is planning-only. Every deployment record observed live "
    "is inactive.",
    # One sentence per phrase that NO sibling phrase also catches. Without
    # these the corpus grades the list as a whole: a phrase whose every
    # sentence is redundantly covered could be deleted with the suite green,
    # and the tripwire would quietly narrow one wording at a time.
    "Every live deployment record was inactive at capture time.",
    "The deployment record observed is inactive.",
    "Evidence token: capture:manage_deployment/all-inactive.",
)


def test_each_phrase_has_a_sentence_only_it_catches():
    """Guards the corpus: redundancy must not hide a phrase's deletion.

    For every entry in the phrase list there must be at least one corpus
    sentence that entry alone matches. Otherwise deleting the entry changes no
    test outcome, and the list can be narrowed one wording at a time without
    anything going red.
    """
    uncovered = []
    for phrase in _RETRACTED_DEPLOYMENT_CLAIMS:
        others = [p for p in _RETRACTED_DEPLOYMENT_CLAIMS if p != phrase]
        exclusive = [
            sentence
            for sentence in _RETRACTED_SENTENCES
            if phrase in _normalized(sentence)
            and not any(other in _normalized(sentence) for other in others)
        ]
        if not exclusive:
            uncovered.append(phrase)
    assert uncovered == [], uncovered


@pytest.mark.parametrize("sentence", _RETRACTED_SENTENCES)
def test_every_retracted_sentence_is_still_caught(tmp_path, sentence):
    """QA #218. The phrase list must catch the sentences it exists to catch.

    Parametrized over a hardcoded corpus rather than over the phrase list, so a
    mutation that edits only the list — deleting or substituting an entry —
    fails here even though every test body is untouched. Reordering is a no-op
    and deliberately not caught.
    """
    path = _write(tmp_path, "republished.py", f'"""{sentence}"""\n')
    assert _retraction_offenders([path]), sentence


def test_the_retracted_sentence_corpus_is_not_empty():
    """A parametrized test over an empty corpus passes zero cases, silently."""
    assert len(_RETRACTED_SENTENCES) >= 7


def test_the_guard_judges_every_occurrence_not_just_the_first(tmp_path):
    """QA #216. Stopping at the first match restores #211 for the one file that mattered.

    ``capabilities.py`` carries the exempt quotation near the top; a bare
    assertion added anywhere below it would then be invisible, because the
    guard would have already accepted the file's first occurrence and moved on.

    Every other control passes a file with a single occurrence, so none of them
    can tell "first" from "all". This one deliberately mirrors the real file's
    shape: an exempt quotation, then padding, then an assertion.

    Both occurrences use the SAME phrase, and one that no other entry in the
    list overlaps. A first attempt at this test used the two different
    deployment-record wordings, and it passed under the ``break`` mutation for
    the wrong reason: the second occurrence happened to be the FIRST occurrence
    of a different phrase, so that phrase reported it and the multi-occurrence
    behavior was never exercised.
    """
    phrase = "all deployments are inactive"
    assert phrase in _RETRACTED_DEPLOYMENT_CLAIMS
    # No other listed phrase may appear in this file, or the test can pass
    # without ever judging a second occurrence.
    overlapping = [
        other
        for other in _RETRACTED_DEPLOYMENT_CLAIMS
        if other != phrase and (other in phrase or phrase in other)
    ]
    assert overlapping == [], overlapping

    path = _write(
        tmp_path,
        "two_occurrences.py",
        f'"""An earlier draft was wrong to say "{phrase}" here.\n\n'
        + ("padding padding padding padding padding.\n" * 40)
        + f"\n{phrase.capitalize()}.\n\"\"\"\n",
    )
    body = _normalized(path.read_text())
    assert body.count(phrase) == 2, body.count(phrase)

    offenders = _retraction_offenders([path])
    # The first occurrence is exempt; the second is not. A guard that judged
    # only the first would return nothing at all.
    assert offenders == [(path.name, phrase)], offenders


def test_the_guard_still_exempts_a_file_whose_every_occurrence_is_quoted(tmp_path):
    """The counterpart: judging all occurrences must not break the real file."""
    path = _write(
        tmp_path,
        "all_quoted.py",
        '"""An earlier draft was wrong: "every live deployment record is inactive".\n'
        'A later draft was wrong the same way: "all deployments are inactive"."""\n',
    )
    assert _retraction_offenders([path]) == []


@pytest.mark.parametrize(
    "invisible", ["\u200b", "\u200c", "\u200d", "\ufeff", "\u2060", "\u00ad", "\u180e"]
)
def test_the_guard_sees_through_every_invisible_format_character(tmp_path, invisible):
    """QA #217. An enumerated list closes only what someone thought of.

    The first version omitted U+2060 WORD JOINER — the documented successor to
    U+FEFF, which it did include. Category ``Cf`` closes the class.
    """
    path = _write(
        tmp_path,
        "spliced.py",
        f"# every live deployment re{invisible}cord observed is inactive.\n",
    )
    assert _retraction_offenders([path]), invisible


def test_a_visible_space_character_is_handled_by_whitespace_collapsing(tmp_path):
    """U+00A0 and U+2009 are ``Zs`` and DO split — no stripping needed."""
    for space in ("\u00a0", "\u2009"):
        path = _write(
            tmp_path,
            "spaced.py",
            f"# every{space}live deployment record observed is inactive.\n",
        )
        assert _retraction_offenders([path]), repr(space)


# ---------------------------------------------------------------------------
# Codex review round 1 — XML extraction must parse, not pattern-match
# ---------------------------------------------------------------------------


def test_a_commented_out_shape_creates_no_witness():
    """A regex has no idea what a comment is.

    This is the sharp case: a witness AUTHORIZES a planning relation, so a
    commented-out sub-process call would establish an edge the process does not
    have. ``iter()`` yields elements only, so comments contribute nothing
    without any special handling.
    """
    calls, uses = parse_process_component_evidence(
        "lit-1", '<process><!-- <processcall processId="ghost"/> --></process>'
    )
    assert calls == ()
    assert uses == ()


def test_a_real_shape_beside_a_commented_one_still_witnesses():
    """Negative control: the fix must not stop seeing genuine shapes."""
    calls, _ = parse_process_component_evidence(
        "lit-1",
        '<process><!-- <processcall processId="ghost"/> -->'
        '<processcall processId="real"/></process>',
    )
    assert [c.callee_component_ref for c in calls] == ["real"]


def test_malformed_xml_creates_no_witness():
    """Fail closed: an unreadable document means no witness, hence gated."""
    calls, uses = parse_process_component_evidence(
        "lit-1", '<process><processcall processId="y"'
    )
    assert calls == ()
    assert uses == ()


def test_a_dtd_bearing_document_is_refused():
    """XXE / billion-laughs: DOCTYPE and ENTITY are rejected before parsing."""
    for hostile in (
        '<!DOCTYPE f [<!ENTITY a "b">]><process><processcall processId="z"/></process>',
        '<!ENTITY x "y"><process><processcall processId="z"/></process>',
    ):
        calls, _ = parse_process_component_evidence("lit-1", hostile)
        assert calls == (), hostile


def test_a_namespaced_component_still_witnesses():
    """Boomi XML carries namespaces; a raw tag comparison would stop matching."""
    calls, _ = parse_process_component_evidence(
        "lit-1",
        '<p:process xmlns:p="urn:x"><p:processcall processId="nsok"/></p:process>',
    )
    assert [c.callee_component_ref for c in calls] == ["nsok"]


def test_an_api_service_route_requires_a_real_wss_listen_element():
    """A comment mentioning wss/listen must not make a target a listener."""
    assert (
        parse_api_service_component_evidence(
            "asc-1",
            '<webservice><!-- <wss listen="true"/> -->'
            '<operation processId="L"/></webservice>',
        )
        == ()
    )
    assert parse_api_service_component_evidence(
        "asc-1", '<webservice><wss listen="true"/><operation processId="L"/></webservice>'
    )


def test_xml_target_extraction_is_linear_not_quadratic():
    """The 8 MiB bound admits enough distinct targets for O(n^2) to bite.

    Timed rather than asserted structurally: the defect is a performance
    property, and a membership test against a list is the only way to fail it.
    Ratio, not absolute time, so the test is not machine-speed dependent.
    """
    import time

    def elapsed(count):
        xml = (
            "<process>"
            + "".join(f'<processcall processId="p{i}"/>' for i in range(count))
            + "</process>"
        )
        start = time.perf_counter()
        parse_process_component_evidence("x", xml)
        return time.perf_counter() - start

    small = elapsed(4000)
    large = elapsed(16000)
    # 4x the input. Linear is ~4x; the quadratic version was ~16x. A generous
    # ceiling keeps this from flaking on a loaded machine while still failing
    # decisively if the set is removed.
    assert large < small * 9, (small, large)


# ---------------------------------------------------------------------------
# QA #223 — the element-name filter, pinned against REAL Boomi component XML
# ---------------------------------------------------------------------------

_LIVE_XML = _project_root / "tests" / "fixtures" / "live_xml"


def _live_process_xmls():
    return sorted(
        path
        for path in _LIVE_XML.rglob("*.xml")
        if path.name.startswith("process_")
    )


def test_the_live_xml_corpus_is_present():
    """A parametrized scan over an empty corpus passes zero cases, silently."""
    assert len(_live_process_xmls()) >= 3


@pytest.mark.parametrize(
    "path", _live_process_xmls(), ids=lambda p: p.name
)
def test_real_component_xml_fabricates_no_witness(path):
    """QA #223. The element-name filter is load-bearing on REAL documents.

    Every Boomi component's ROOT is ``<bns:Component … componentId="…">``, and
    ``componentId`` is one of the attributes a Process Property is matched by.
    Without the tag filter every real component would fabricate a
    ``process_property`` witness for itself — a planning edge invented from the
    document's own identity.

    Parametrized over the real captures rather than a synthetic root, because
    the point is that the filter holds against documents nobody wrote for it.
    """
    calls, uses = parse_process_component_evidence("lit-1", path.read_text())
    fabricated = [
        u for u in uses if u.resource_component_ref == u.process_component_ref
    ]
    assert fabricated == [], (path.name, fabricated)
    # Nothing may claim the component calls itself, either.
    assert [c for c in calls if c.callee_component_ref == "lit-1"] == []


def test_a_component_root_alone_yields_no_process_property_witness():
    """The minimal shape of the same defect, stated directly."""
    root_only = (
        '<bns:Component xmlns:bns="http://api.platform.boomi.com/" '
        'componentId="abc-123" type="process" name="X"/>'
    )
    calls, uses = parse_process_component_evidence("lit-1", root_only)
    assert calls == ()
    assert uses == ()


def test_a_genuine_processproperty_element_still_witnesses():
    """Negative control: the filter must not stop seeing real shapes."""
    xml = (
        '<bns:Component xmlns:bns="http://api.platform.boomi.com/" '
        'componentId="self-id"><processproperty componentId="prop-1"/></bns:Component>'
    )
    _, uses = parse_process_component_evidence("lit-1", xml)
    assert [(u.resource_kind, u.resource_component_ref) for u in uses] == [
        ("process_property", "prop-1")
    ]


@pytest.mark.parametrize(
    "tag,attr,kind",
    [
        ("processcall", "processId", "call"),
        ("documentcache", "documentCacheId", "document_cache"),
        ("cache", "cacheId", "document_cache"),
        ("processproperty", "componentId", "process_property"),
        ("property", "processPropertyId", "process_property"),
    ],
)
def test_every_declared_tag_and_attribute_is_exercised(tag, attr, kind):
    """QA #223. Each vocabulary constant must be individually pinned.

    Three sibling constants were unpinned, so any of them could be deleted with
    the suite green — silently narrowing what the planner can witness.
    """
    xml = f'<process><{tag} {attr}="target-1"/></process>'
    calls, uses = parse_process_component_evidence("lit-1", xml)
    if kind == "call":
        assert [c.callee_component_ref for c in calls] == ["target-1"], (tag, attr)
    else:
        assert [
            u.resource_component_ref for u in uses if u.resource_kind == kind
        ] == ["target-1"], (tag, attr)


@pytest.mark.parametrize("tag", ["operation", "route"])
def test_every_declared_route_tag_is_exercised(tag):
    """``route`` is the tag the repo's own real ASC capture uses."""
    xml = f'<webservice><wss listen="true"/><{tag} processId="L-1"/></webservice>'
    assert [
        r.listener_component_ref
        for r in parse_api_service_component_evidence("asc-1", xml)
    ] == ["L-1"], tag


def test_the_real_api_service_capture_yields_no_route_witness_today():
    """A recorded fact, not an aspiration.

    The repo's real ASC capture carries no ``<wss>`` element at all — the WSS
    Listen configuration lives on the linked PROCESS's start shape, not on the
    API Service Component. So this returns nothing, and the api_service_route
    relation stays witness-gated on a real ASC. That is pre-existing platform
    shape, consistent with the registry's ``unavailable`` live leg for
    ``api_service`` — and it is pinned here so a future change to the extraction
    cannot quietly start claiming routes it cannot actually see.
    """
    capture = _LIVE_XML / "m6" / "api_service_minimal.xml"
    if not capture.exists():
        pytest.skip("the m6 ASC capture is not present")
    assert parse_api_service_component_evidence("asc-1", capture.read_text()) == ()
