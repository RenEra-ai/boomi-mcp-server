"""Reference, lifecycle, environment and cycle validation (issue #144, M12.9).

Negative tests, mostly. Each pins that a specific wrong topology is caught
deterministically, with a pointer a caller can act on and no authored value in
the diagnostic.
"""

import sys
from pathlib import Path

import pytest

_project_root = Path(__file__).resolve().parent.parent
_src = str(_project_root / "src")
if _src not in sys.path:
    sys.path.insert(0, _src)

from boomi_mcp.models.system_topology import (
    TOPOLOGY_RELATION_KINDS,
    TOPOLOGY_RELATION_ROLES,
    parse_system_topology_v1,
)
from boomi_mcp.compiler.system_topology import validate_system_topology
from boomi_mcp.compiler.system_topology.context import (
    ApiServiceRouteEvidenceV1,
    ComponentFactV1,
    ComponentPlanSymbolV1,
    EnvironmentFactV1,
    ProcessCallEvidenceV1,
    RuntimeFactV1,
    SharedResourceUseEvidenceV1,
    TopologyDiscoverySnapshotV1,
    TopologyResolutionContextV1,
)
from boomi_mcp.compiler.system_topology.references import TOPOLOGY_ENDPOINT_MATRIX

_ALL_OBJECTS = [
    {"kind": "process", "key": "p", "component_ref": "$ref:pk"},
    {"kind": "process", "key": "p2", "component_ref": "$ref:p2k"},
    {"kind": "api_service", "key": "a", "component_ref": "$ref:ak"},
    {"kind": "document_cache", "key": "c", "component_ref": "$ref:ck"},
    {"kind": "process_property", "key": "pp", "component_ref": "$ref:ppk"},
    {"kind": "runtime", "key": "rt", "runtime_ref": "runtime-1"},
    {"kind": "environment", "key": "e", "environment_ref": "env-1"},
    {"kind": "schedule", "key": "s"},
    {"kind": "deployment_unit", "key": "u"},
]

_BINDINGS = [
    {"kind": "schedule_binding", "key": "rs", "schedule": "s", "process": "p", "runtime": "rt"},
    {
        "kind": "deployment_binding",
        "key": "rd",
        "deployment_unit": "u",
        "process": "p",
        "environment": "e",
    },
]

_SYMBOLS = tuple(
    ComponentPlanSymbolV1(component_key=k, component_type=t)
    for k, t in (
        ("pk", "process"),
        ("p2k", "process"),
        ("ak", "webservice"),
        ("ck", "documentcache"),
        ("ppk", "processproperty"),
    )
)

_WITNESSED = TopologyResolutionContextV1(
    profile="p-alpha",
    component_plan_symbols=_SYMBOLS,
    process_call_evidence=(
        ProcessCallEvidenceV1(
            caller_component_ref="$ref:pk",
            callee_component_ref="$ref:p2k",
            witness="process_ir",
        ),
    ),
    api_service_route_evidence=(
        ApiServiceRouteEvidenceV1(
            api_service_component_ref="$ref:ak",
            listener_component_ref="$ref:pk",
            witness="typed_builder",
        ),
    ),
    shared_resource_use_evidence=(
        SharedResourceUseEvidenceV1(
            process_component_ref="$ref:pk",
            resource_component_ref="$ref:ck",
            resource_kind="document_cache",
            witness="process_ir",
        ),
        SharedResourceUseEvidenceV1(
            process_component_ref="$ref:pk",
            resource_component_ref="$ref:ppk",
            resource_kind="process_property",
            witness="process_ir",
        ),
    ),
)


def _spec(relations, objects=None, profile="p-alpha"):
    return parse_system_topology_v1(
        {
            "version": "1",
            "profile_ref": profile,
            "objects": list(objects if objects is not None else _ALL_OBJECTS),
            "relations": list(relations) + list(_BINDINGS)
            if objects is None
            else list(relations),
        }
    )


def _codes(report):
    return [d.code for d in report.errors]


# ---------------------------------------------------------------------------
# The endpoint matrix
# ---------------------------------------------------------------------------


def test_endpoint_matrix_covers_every_relation_role():
    """A role absent from the matrix would raise KeyError on the happy path.

    Checked as coverage rather than left to a crash: the matrix is derived-adjacent
    (roles come from the models, expectations are hand-written), so this is the
    one place the two can drift.
    """
    expected = {
        (kind, role)
        for kind in TOPOLOGY_RELATION_KINDS
        for role in TOPOLOGY_RELATION_ROLES[kind]
    }
    assert set(TOPOLOGY_ENDPOINT_MATRIX) == expected


def test_a_fully_witnessed_topology_validates_clean():
    report = validate_system_topology(
        _spec(
            [
                {"kind": "process_call", "key": "r1", "caller_process": "p", "callee_process": "p2"},
                {"kind": "document_cache_use", "key": "r2", "process": "p", "document_cache": "c"},
                {"kind": "process_property_use", "key": "r3", "process": "p", "process_property": "pp"},
            ]
        ),
        _WITNESSED,
    )
    assert report.is_valid, _codes(report)
    assert report.errors == ()


# ---------------------------------------------------------------------------
# References
# ---------------------------------------------------------------------------


def test_a_relation_role_naming_an_undeclared_object_is_not_found():
    report = validate_system_topology(
        _spec(
            [
                {
                    "kind": "process_call",
                    "key": "r1",
                    "caller_process": "p",
                    "callee_process": "nobody",
                }
            ]
        ),
        _WITNESSED,
    )
    assert "TOPOLOGY_REFERENCE_NOT_FOUND" in _codes(report)
    finding = [d for d in report.errors if d.code == "TOPOLOGY_REFERENCE_NOT_FOUND"][0]
    assert finding.path == "/relations/0/callee_process"
    assert finding.subject == "process"


def _key_for(kind):
    return {
        "process": "p",
        "api_service": "a",
        "document_cache": "c",
        "process_property": "pp",
        "runtime": "rt",
        "environment": "e",
        "schedule": "s",
        "deployment_unit": "u",
    }[kind]


def _mismatch_cases():
    """Every (relation, role) whose accepted kind is not ``process``.

    Those are the only roles a process key can actually mis-fill; a role that
    already accepts a process cannot be given a wrong-kinded process. Gated
    kinds are excluded because their capability blocker fires first and the
    reference phase never gets to speak.
    """
    return [
        (kind, role)
        for (kind, role), expected in sorted(TOPOLOGY_ENDPOINT_MATRIX.items())
        if expected != "process"
        and kind not in ("queue_reference", "event_stream_reference")
    ]


def test_the_mismatch_case_list_is_not_empty():
    """Positive control: an empty parametrization passes vacuously."""
    assert len(_mismatch_cases()) >= 6


@pytest.mark.parametrize("kind,role", _mismatch_cases())
def test_every_endpoint_type_mismatch_is_caught(kind, role):
    """Point one role at a PROCESS, which it does not accept."""
    sample = {
        "kind": kind,
        "key": "rx",
        **{
            r: _key_for(TOPOLOGY_ENDPOINT_MATRIX[(kind, r)])
            for r in TOPOLOGY_RELATION_ROLES[kind]
        },
    }
    sample[role] = "p"

    # Every schedule and deployment unit must still be bound, or the cardinality
    # rule fires at parse time and the reference phase never runs. Add only the
    # bindings ``sample`` does not already supply — adding one it does would
    # bind the same schedule twice, which is a different (also real) error.
    relations = [sample]
    if not (kind == "schedule_binding" and sample["schedule"] == "s"):
        relations.append(_BINDINGS[0])
    if not (kind == "deployment_binding" and sample["deployment_unit"] == "u"):
        relations.append(_BINDINGS[1])

    spec = parse_system_topology_v1(
        {
            "version": "1",
            "profile_ref": "p-alpha",
            "objects": _ALL_OBJECTS,
            "relations": relations,
        }
    )
    report = validate_system_topology(spec, _WITNESSED)
    mismatches = [
        d for d in report.errors if d.code == "TOPOLOGY_REFERENCE_TYPE_MISMATCH"
    ]
    assert mismatches, (kind, role, _codes(report))
    assert any(d.path == f"/relations/0/{role}" for d in mismatches), (
        kind,
        role,
        [d.path for d in mismatches],
    )


def test_a_component_ref_with_no_symbol_or_live_fact_is_not_found():
    spec = _spec(
        [],
        objects=[{"kind": "process", "key": "p", "component_ref": "$ref:unknown"}],
    )
    report = validate_system_topology(spec, _WITNESSED)
    codes = _codes(report)
    assert "TOPOLOGY_REFERENCE_NOT_FOUND" in codes


def test_all_reference_failures_accumulate_and_sort_deterministically():
    spec = _spec(
        [
            {"kind": "process_call", "key": "r1", "caller_process": "ghost1", "callee_process": "ghost2"},
            {"kind": "document_cache_use", "key": "r2", "process": "ghost3", "document_cache": "c"},
        ]
    )
    report = validate_system_topology(spec, _WITNESSED)
    not_found = [d for d in report.errors if d.code == "TOPOLOGY_REFERENCE_NOT_FOUND"]
    assert len(not_found) == 3
    keys = [d.sort_key() for d in report.errors]
    assert keys == sorted(keys)


def test_reference_diagnostics_never_carry_the_failed_key():
    spec = _spec(
        [
            {
                "kind": "process_call",
                "key": "r1",
                "caller_process": "p",
                "callee_process": "a-very-distinctive-missing-key",
            }
        ]
    )
    report = validate_system_topology(spec, _WITNESSED)
    blob = report.model_dump_json()
    assert "a-very-distinctive-missing-key" not in blob


# ---------------------------------------------------------------------------
# Lifecycle shapes
# ---------------------------------------------------------------------------


def test_scheduling_a_listener_process_is_unsupported():
    """A listener is invoked by its API service; a schedule is a second trigger."""
    spec = parse_system_topology_v1(
        {
            "version": "1",
            "profile_ref": "p-alpha",
            "objects": _ALL_OBJECTS,
            "relations": [
                {"kind": "api_service_route", "key": "rr", "api_service": "a", "listener_process": "p"},
                _BINDINGS[0],
                _BINDINGS[1],
            ],
        }
    )
    report = validate_system_topology(spec, _WITNESSED)
    unsupported = [d for d in report.errors if d.code == "TOPOLOGY_RELATION_UNSUPPORTED"]
    assert unsupported, _codes(report)
    assert unsupported[0].path.endswith("/process")
    assert unsupported[0].subject == "schedule_binding"


def test_a_schedule_bound_twice_is_unsupported():
    spec = parse_system_topology_v1(
        {
            "version": "1",
            "profile_ref": "p-alpha",
            "objects": _ALL_OBJECTS,
            "relations": [
                _BINDINGS[0],
                {"kind": "schedule_binding", "key": "rs2", "schedule": "s", "process": "p2", "runtime": "rt"},
                _BINDINGS[1],
            ],
        }
    )
    report = validate_system_topology(spec, _WITNESSED)
    assert "TOPOLOGY_RELATION_UNSUPPORTED" in _codes(report)


def test_a_deployment_unit_targeting_two_processes_is_unsupported():
    """``orchestrate_deploy`` requires exactly one process; no atomicity exists."""
    spec = parse_system_topology_v1(
        {
            "version": "1",
            "profile_ref": "p-alpha",
            "objects": _ALL_OBJECTS,
            "relations": [
                _BINDINGS[0],
                _BINDINGS[1],
                {
                    "kind": "deployment_binding",
                    "key": "rd2",
                    "deployment_unit": "u",
                    "process": "p2",
                    "environment": "e",
                },
            ],
        }
    )
    report = validate_system_topology(spec, _WITNESSED)
    unsupported = [d for d in report.errors if d.code == "TOPOLOGY_RELATION_UNSUPPORTED"]
    assert unsupported
    assert unsupported[0].subject == "deployment_binding"


def test_a_self_call_is_unsupported():
    spec = _spec(
        [{"kind": "process_call", "key": "r1", "caller_process": "p", "callee_process": "p"}]
    )
    report = validate_system_topology(spec, _WITNESSED)
    assert "TOPOLOGY_RELATION_UNSUPPORTED" in _codes(report)


# ---------------------------------------------------------------------------
# Witness gating
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "relation",
    [
        {"kind": "process_call", "key": "r1", "caller_process": "p", "callee_process": "p2"},
        {"kind": "api_service_route", "key": "r1", "api_service": "a", "listener_process": "p2"},
        {"kind": "document_cache_use", "key": "r1", "process": "p2", "document_cache": "c"},
        {"kind": "process_property_use", "key": "r1", "process": "p2", "process_property": "pp"},
    ],
)
def test_a_relation_with_no_trusted_witness_is_gated(relation):
    """A supported KIND still needs evidence for the specific edge."""
    bare = TopologyResolutionContextV1(
        profile="p-alpha", component_plan_symbols=_SYMBOLS
    )
    report = validate_system_topology(_spec([relation]), bare)
    gated = [d for d in report.errors if d.code == "TOPOLOGY_CAPABILITY_GATED"]
    assert gated, _codes(report)
    # Witness-level gating lives in the ``relation`` phase, not ``capability``.
    assert gated[0].phase == "relation"
    assert gated[0].subject == relation["kind"]


def test_dependency_corroboration_alone_does_not_witness_a_process_call():
    """The central evidence correction, asserted as behavior.

    A dependency row saying "p2 appears in p's dependency list" must not promote
    ``process_call`` — the API's response carries no edge kind, so the same row
    would appear for a JSON profile reference.
    """
    from boomi_mcp.compiler.system_topology.evidence import (
        normalize_dependency_corroboration,
    )

    corroborated = TopologyResolutionContextV1(
        profile="p-alpha",
        component_plan_symbols=_SYMBOLS,
        dependency_corroboration=normalize_dependency_corroboration(
            "$ref:pk", [("$ref:p2k", "process")]
        ),
    )
    report = validate_system_topology(
        _spec(
            [{"kind": "process_call", "key": "r1", "caller_process": "p", "callee_process": "p2"}]
        ),
        corroborated,
    )
    assert "TOPOLOGY_CAPABILITY_GATED" in _codes(report)


def test_kind_level_and_witness_level_gating_use_different_phases():
    """Same code, two phases — so each is independently testable and orderable."""
    spec = parse_system_topology_v1(
        {
            "version": "1",
            "profile_ref": "p-alpha",
            "objects": _ALL_OBJECTS
            + [{"kind": "external_queue", "key": "q", "resource_ref": "queue-1"}],
            "relations": [
                {"kind": "process_call", "key": "r1", "caller_process": "p", "callee_process": "p2"}
            ]
            + _BINDINGS,
        }
    )
    bare = TopologyResolutionContextV1(
        profile="p-alpha", component_plan_symbols=_SYMBOLS
    )
    report = validate_system_topology(spec, bare)
    phases = {
        d.phase for d in report.errors if d.code == "TOPOLOGY_CAPABILITY_GATED"
    }
    assert phases == {"capability", "relation"}
    # Phase rank precedes path in the total order, so capability sorts first.
    ordered = [d.phase for d in report.errors if d.code == "TOPOLOGY_CAPABILITY_GATED"]
    assert ordered.index("capability") < ordered.index("relation")


# ---------------------------------------------------------------------------
# Profile isolation and environment classification
# ---------------------------------------------------------------------------


def test_a_context_from_another_profile_is_an_environment_mismatch():
    """A fact from another account is not weaker evidence — it is other evidence."""
    other = TopologyResolutionContextV1(
        profile="p-beta", component_plan_symbols=_SYMBOLS
    )
    report = validate_system_topology(_spec([]), other)
    mismatch = [d for d in report.errors if d.code == "TOPOLOGY_ENVIRONMENT_MISMATCH"]
    assert mismatch
    assert mismatch[0].path == "/profile_ref"


def test_a_snapshot_from_another_profile_is_an_environment_mismatch():
    ctx = TopologyResolutionContextV1(
        profile="p-alpha",
        component_plan_symbols=_SYMBOLS,
        snapshot=TopologyDiscoverySnapshotV1(
            profile="p-beta",
            captured_at="2026-01-01T00:00:00Z",
            source_revision="rev",
            service_release="rel",
        ),
    )
    report = validate_system_topology(_spec([]), ctx)
    assert "TOPOLOGY_ENVIRONMENT_MISMATCH" in _codes(report)


def test_a_contradicted_environment_classification_is_a_mismatch():
    ctx = TopologyResolutionContextV1(
        profile="p-alpha",
        component_plan_symbols=_SYMBOLS,
        snapshot=TopologyDiscoverySnapshotV1(
            profile="p-alpha",
            captured_at="2026-01-01T00:00:00Z",
            source_revision="rev",
            service_release="rel",
            components=(
                ComponentFactV1(profile="p-alpha", component_id="x", component_type="process"),
            ),
            environments=(
                EnvironmentFactV1(
                    profile="p-alpha", environment_id="env-1", classification="PROD"
                ),
            ),
            runtimes=(RuntimeFactV1(profile="p-alpha", runtime_id="runtime-1"),),
        ),
    )
    objects = [
        o if o["key"] != "e" else dict(o, classification="TEST") for o in _ALL_OBJECTS
    ]
    spec = parse_system_topology_v1(
        {
            "version": "1",
            "profile_ref": "p-alpha",
            "objects": objects,
            "relations": _BINDINGS,
        }
    )
    report = validate_system_topology(spec, ctx)
    mismatch = [d for d in report.errors if d.code == "TOPOLOGY_ENVIRONMENT_MISMATCH"]
    assert mismatch
    assert mismatch[0].path.endswith("/classification")


def test_an_unauthored_classification_is_never_a_mismatch():
    """A profile may legitimately have no PROD environment; requiring one would
    make a real account unmodelable."""
    ctx = TopologyResolutionContextV1(
        profile="p-alpha",
        component_plan_symbols=_SYMBOLS,
        snapshot=TopologyDiscoverySnapshotV1(
            profile="p-alpha",
            captured_at="2026-01-01T00:00:00Z",
            source_revision="rev",
            service_release="rel",
            environments=(
                EnvironmentFactV1(
                    profile="p-alpha", environment_id="env-1", classification="TEST"
                ),
            ),
            runtimes=(RuntimeFactV1(profile="p-alpha", runtime_id="runtime-1"),),
        ),
    )
    report = validate_system_topology(_spec([]), ctx)
    assert "TOPOLOGY_ENVIRONMENT_MISMATCH" not in _codes(report)


# ---------------------------------------------------------------------------
# Cycles
# ---------------------------------------------------------------------------


def _cycle_ctx(pairs):
    return TopologyResolutionContextV1(
        profile="p-alpha",
        component_plan_symbols=tuple(
            ComponentPlanSymbolV1(component_key=f"k{i}", component_type="process")
            for i in range(6)
        ),
        process_call_evidence=tuple(
            ProcessCallEvidenceV1(
                caller_component_ref=f"$ref:k{a}",
                callee_component_ref=f"$ref:k{b}",
                witness="process_ir",
            )
            for a, b in pairs
        ),
    )


def _cycle_spec(pairs):
    nodes = sorted({n for pair in pairs for n in pair})
    return parse_system_topology_v1(
        {
            "version": "1",
            "profile_ref": "p-alpha",
            "objects": [
                {"kind": "process", "key": f"n{i}", "component_ref": f"$ref:k{i}"}
                for i in nodes
            ],
            "relations": [
                {
                    "kind": "process_call",
                    "key": f"r{index}",
                    "caller_process": f"n{a}",
                    "callee_process": f"n{b}",
                }
                for index, (a, b) in enumerate(pairs)
            ],
        }
    )


@pytest.mark.parametrize(
    "pairs",
    [
        [(0, 1), (1, 0)],
        [(0, 1), (1, 2), (2, 0)],
        [(0, 1), (1, 2), (2, 3), (3, 1)],
    ],
    ids=["two-node", "three-node", "tail-into-cycle"],
)
def test_a_process_call_cycle_is_reported(pairs):
    report = validate_system_topology(_cycle_spec(pairs), _cycle_ctx(pairs))
    assert "TOPOLOGY_DEPENDENCY_CYCLE" in _codes(report)


def test_the_cycle_pointer_is_the_earliest_participating_relation():
    """A cycle has no natural first element, so the pointer must be canonical."""
    pairs = [(0, 1), (1, 2), (2, 1)]
    report = validate_system_topology(_cycle_spec(pairs), _cycle_ctx(pairs))
    cycle = [d for d in report.errors if d.code == "TOPOLOGY_DEPENDENCY_CYCLE"][0]
    # Relation 0 (n0->n1) merely feeds the cycle; the cycle is relations 1 and 2.
    assert cycle.path == "/relations/1"


def test_an_acyclic_graph_reports_no_cycle():
    pairs = [(0, 1), (1, 2), (0, 2)]
    report = validate_system_topology(_cycle_spec(pairs), _cycle_ctx(pairs))
    assert "TOPOLOGY_DEPENDENCY_CYCLE" not in _codes(report)


def test_the_cycle_check_is_skipped_when_references_do_not_resolve():
    """A cycle among relations that point at nothing is true and useless."""
    spec = parse_system_topology_v1(
        {
            "version": "1",
            "profile_ref": "p-alpha",
            "objects": [{"kind": "process", "key": "n0", "component_ref": "$ref:k0"}],
            "relations": [
                {"kind": "process_call", "key": "r0", "caller_process": "n0", "callee_process": "ghost"},
            ],
        }
    )
    report = validate_system_topology(spec, _cycle_ctx([]))
    assert "TOPOLOGY_REFERENCE_NOT_FOUND" in _codes(report)
    assert "TOPOLOGY_DEPENDENCY_CYCLE" not in _codes(report)


# ---------------------------------------------------------------------------
# Report hygiene
# ---------------------------------------------------------------------------


def test_a_report_carries_only_topology_codes():
    report = validate_system_topology(_spec([]), _WITNESSED)
    for bucket in (report.errors, report.warnings, report.advisories):
        for diagnostic in bucket:
            assert diagnostic.code.startswith("TOPOLOGY_")


def test_a_diagnostic_cannot_be_built_with_a_foreign_code():
    from boomi_mcp.compiler.system_topology.contracts import TopologyDiagnosticV1

    with pytest.raises(ValueError):
        TopologyDiagnosticV1(
            code="PROCESS_IR_SCHEMA_INVALID",
            severity="error",
            phase="model",
            path="/",
            message="m",
            remediation="r",
        )


def test_a_diagnostic_subject_must_be_a_structural_token():
    from boomi_mcp.compiler.system_topology.contracts import TopologyDiagnosticV1

    with pytest.raises(ValueError):
        TopologyDiagnosticV1(
            code="TOPOLOGY_SCHEMA_INVALID",
            severity="error",
            phase="model",
            path="/",
            subject="Some Component Name",
            message="m",
            remediation="r",
        )


def test_validation_never_raises_on_a_bad_topology():
    """Validation REPORTS. Raising would make accumulation impossible."""
    report = validate_system_topology(
        _spec([{"kind": "process_call", "key": "r", "caller_process": "x", "callee_process": "y"}]),
        _WITNESSED,
    )
    assert not report.is_valid



# ---------------------------------------------------------------------------
# Codex review round 1 — eight defects, each pinned by the case that found it
# ---------------------------------------------------------------------------


def _snapshot(**kwargs):
    from boomi_mcp.compiler.system_topology.context import TopologyDiscoverySnapshotV1

    base = dict(
        profile="p-alpha",
        captured_at="2026-01-01T00:00:00Z",
        source_revision="rev",
        service_release="rel",
    )
    base.update(kwargs)
    return TopologyDiscoverySnapshotV1(**base)


def _complete(*types):
    from boomi_mcp.compiler.system_topology.context import DiscoveryPageProvenanceV1

    return tuple(
        DiscoveryPageProvenanceV1(component_type=t, returned_count=1, total_available=1)
        for t in types
    )


def test_a_wrong_typed_component_reference_is_a_type_mismatch():
    """An id-only index resolves a Document Cache as a process, silently."""
    spec = parse_system_topology_v1(
        {
            "version": "1",
            "profile_ref": "p-alpha",
            "objects": [{"kind": "process", "key": "p", "component_ref": "cache-1"}],
            "relations": [],
        }
    )
    ctx = TopologyResolutionContextV1(
        profile="p-alpha",
        snapshot=_snapshot(
            components=(
                ComponentFactV1(
                    profile="p-alpha", component_id="cache-1", component_type="documentcache"
                ),
            ),
            pagination=_complete("process"),
        ),
    )
    report = validate_system_topology(spec, ctx)
    assert "TOPOLOGY_REFERENCE_TYPE_MISMATCH" in _codes(report), _codes(report)


def test_a_wrong_typed_component_plan_symbol_is_a_type_mismatch():
    spec = parse_system_topology_v1(
        {
            "version": "1",
            "profile_ref": "p-alpha",
            "objects": [{"kind": "process", "key": "p", "component_ref": "$ref:ck"}],
            "relations": [],
        }
    )
    ctx = TopologyResolutionContextV1(
        profile="p-alpha",
        component_plan_symbols=(
            ComponentPlanSymbolV1(component_key="ck", component_type="documentcache"),
        ),
    )
    assert "TOPOLOGY_REFERENCE_TYPE_MISMATCH" in _codes(
        validate_system_topology(spec, ctx)
    )


def test_a_correctly_typed_reference_still_resolves():
    """Negative control: the type check must not reject correct references."""
    spec = parse_system_topology_v1(
        {
            "version": "1",
            "profile_ref": "p-alpha",
            "objects": [{"kind": "document_cache", "key": "c", "component_ref": "cache-1"}],
            "relations": [],
        }
    )
    ctx = TopologyResolutionContextV1(
        profile="p-alpha",
        snapshot=_snapshot(
            components=(
                ComponentFactV1(
                    profile="p-alpha", component_id="cache-1", component_type="documentcache"
                ),
            ),
            pagination=_complete("documentcache"),
        ),
    )
    assert validate_system_topology(spec, ctx).errors == ()


def test_absence_from_a_truncated_page_is_not_a_not_found():
    """The documented 100-of-186 capture: a later page is not evidence of absence.

    Reporting NOT_FOUND here would contradict the pagination provenance this
    contract records precisely so absence is not over-read.
    """
    spec = parse_system_topology_v1(
        {
            "version": "1",
            "profile_ref": "p-alpha",
            "objects": [
                {"kind": "document_cache", "key": "c", "component_ref": "on-page-two"}
            ],
            "relations": [],
        }
    )
    from boomi_mcp.compiler.system_topology.context import DiscoveryPageProvenanceV1

    ctx = TopologyResolutionContextV1(
        profile="p-alpha",
        snapshot=_snapshot(
            components=(
                ComponentFactV1(
                    profile="p-alpha", component_id="on-page-one", component_type="documentcache"
                ),
            ),
            pagination=(
                DiscoveryPageProvenanceV1(
                    component_type="documentcache",
                    returned_count=100,
                    total_available=186,
                    has_more=True,
                ),
            ),
        ),
    )
    assert "TOPOLOGY_REFERENCE_NOT_FOUND" not in _codes(
        validate_system_topology(spec, ctx)
    )


def test_absence_from_a_complete_page_is_still_a_not_found():
    """The counterpart: a complete listing DOES witness absence."""
    spec = parse_system_topology_v1(
        {
            "version": "1",
            "profile_ref": "p-alpha",
            "objects": [{"kind": "document_cache", "key": "c", "component_ref": "ghost"}],
            "relations": [],
        }
    )
    ctx = TopologyResolutionContextV1(
        profile="p-alpha",
        snapshot=_snapshot(
            components=(
                ComponentFactV1(
                    profile="p-alpha", component_id="real", component_type="documentcache"
                ),
            ),
            pagination=_complete("documentcache"),
        ),
    )
    assert "TOPOLOGY_REFERENCE_NOT_FOUND" in _codes(validate_system_topology(spec, ctx))


def test_an_unobserved_page_never_witnesses_absence():
    from boomi_mcp.compiler.system_topology.context import DiscoveryPageProvenanceV1

    spec = parse_system_topology_v1(
        {
            "version": "1",
            "profile_ref": "p-alpha",
            "objects": [{"kind": "document_cache", "key": "c", "component_ref": "ghost"}],
            "relations": [],
        }
    )
    ctx = TopologyResolutionContextV1(
        profile="p-alpha",
        snapshot=_snapshot(
            components=(
                ComponentFactV1(
                    profile="p-alpha", component_id="x", component_type="process"
                ),
            ),
            pagination=(
                DiscoveryPageProvenanceV1(
                    component_type="documentcache", returned_count=0, observed=False
                ),
            ),
        ),
    )
    assert "TOPOLOGY_REFERENCE_NOT_FOUND" not in _codes(
        validate_system_topology(spec, ctx)
    )


def test_a_foreign_profile_fact_inside_a_snapshot_cannot_resolve():
    """Profile isolation is enforced per FACT, not just on the envelope.

    A snapshot may carry the right profile while a fact inside it names another
    account; an index built without checking lets a foreign component id resolve
    with no mismatch reported at all.
    """
    from boomi_mcp.compiler.system_topology import plan_system_topology

    spec = parse_system_topology_v1(
        {
            "version": "1",
            "profile_ref": "p-alpha",
            "objects": [{"kind": "process", "key": "p", "component_ref": "foreign"}],
            "relations": [],
        }
    )
    ctx = TopologyResolutionContextV1(
        profile="p-alpha",
        snapshot=_snapshot(
            components=(
                ComponentFactV1(
                    profile="OTHER-ACCOUNT", component_id="foreign", component_type="process"
                ),
            )
        ),
    )
    plan = plan_system_topology(spec, ctx)
    assert "TOPOLOGY_ENVIRONMENT_MISMATCH" in [b.code for b in plan.blockers]
    assert plan.resolved_references == ()


def test_a_relation_is_withdrawn_when_an_endpoint_object_is_blocked():
    """An endpoint's failure is reported under /objects/N, a different path.

    A filter that looked only at /relations/N left a structural binding in
    ``planning_only_relations`` while it pointed at an unresolvable object.
    """
    from boomi_mcp.compiler.system_topology import plan_system_topology

    spec = parse_system_topology_v1(
        {
            "version": "1",
            "profile_ref": "p-alpha",
            "objects": [
                {"kind": "process", "key": "proc", "component_ref": "ghost"},
                {"kind": "runtime", "key": "rt", "runtime_ref": "runtime-1"},
                {"kind": "schedule", "key": "s"},
            ],
            "relations": [
                {
                    "kind": "schedule_binding",
                    "key": "rs",
                    "schedule": "s",
                    "process": "proc",
                    "runtime": "rt",
                }
            ],
        }
    )
    ctx = TopologyResolutionContextV1(
        profile="p-alpha",
        snapshot=_snapshot(
            components=(
                ComponentFactV1(
                    profile="p-alpha", component_id="real", component_type="process"
                ),
            ),
            runtimes=(RuntimeFactV1(profile="p-alpha", runtime_id="runtime-1"),),
            pagination=_complete("process"),
        ),
    )
    plan = plan_system_topology(spec, ctx)
    assert plan.blockers
    assert plan.planning_only_relations == ()


def test_a_process_ir_witness_is_rejected_for_an_existing_process():
    """The context is a public input; the constructor's rule must hold at use.

    An authored ProcessIR for an EXISTING process may describe an intended
    future shape, so accepting it would let a plan assert an edge the deployed
    component does not have.
    """
    spec = parse_system_topology_v1(
        {
            "version": "1",
            "profile_ref": "p-alpha",
            "objects": [
                {"kind": "process", "key": "a", "component_ref": "literal-caller"},
                {"kind": "process", "key": "b", "component_ref": "literal-callee"},
            ],
            "relations": [
                {"kind": "process_call", "key": "r", "caller_process": "a", "callee_process": "b"}
            ],
        }
    )
    ctx = TopologyResolutionContextV1(
        profile="p-alpha",
        process_call_evidence=(
            ProcessCallEvidenceV1(
                caller_component_ref="literal-caller",
                callee_component_ref="literal-callee",
                witness="process_ir",
            ),
        ),
    )
    assert "TOPOLOGY_CAPABILITY_GATED" in _codes(validate_system_topology(spec, ctx))


def test_a_component_xml_witness_is_accepted_for_an_existing_process():
    """Negative control: the form check must not reject the correct witness."""
    spec = parse_system_topology_v1(
        {
            "version": "1",
            "profile_ref": "p-alpha",
            "objects": [
                {"kind": "process", "key": "a", "component_ref": "literal-caller"},
                {"kind": "process", "key": "b", "component_ref": "literal-callee"},
            ],
            "relations": [
                {"kind": "process_call", "key": "r", "caller_process": "a", "callee_process": "b"}
            ],
        }
    )
    ctx = TopologyResolutionContextV1(
        profile="p-alpha",
        process_call_evidence=(
            ProcessCallEvidenceV1(
                caller_component_ref="literal-caller",
                callee_component_ref="literal-callee",
                witness="component_xml",
            ),
        ),
    )
    assert validate_system_topology(spec, ctx).errors == ()


def test_a_component_xml_witness_is_rejected_for_a_planned_process():
    """The inverse mismatch: a planned component has no deployed artifact."""
    spec = parse_system_topology_v1(
        {
            "version": "1",
            "profile_ref": "p-alpha",
            "objects": [
                {"kind": "process", "key": "a", "component_ref": "$ref:ka"},
                {"kind": "process", "key": "b", "component_ref": "$ref:kb"},
            ],
            "relations": [
                {"kind": "process_call", "key": "r", "caller_process": "a", "callee_process": "b"}
            ],
        }
    )
    ctx = TopologyResolutionContextV1(
        profile="p-alpha",
        component_plan_symbols=(
            ComponentPlanSymbolV1(component_key="ka", component_type="process"),
            ComponentPlanSymbolV1(component_key="kb", component_type="process"),
        ),
        process_call_evidence=(
            ProcessCallEvidenceV1(
                caller_component_ref="$ref:ka",
                callee_component_ref="$ref:kb",
                witness="component_xml",
            ),
        ),
    )
    assert "TOPOLOGY_CAPABILITY_GATED" in _codes(validate_system_topology(spec, ctx))


def test_binding_the_first_schedule_to_a_runtime_is_not_blocked():
    """Discovery derives runtimes from SCHEDULE rows, so it sees only runtimes
    that already have one. Treating that as an inventory reports every
    unscheduled runtime as not-found and blocks the primary use case."""
    spec = parse_system_topology_v1(
        {
            "version": "1",
            "profile_ref": "p-alpha",
            "objects": [
                {"kind": "process", "key": "proc", "component_ref": "c1"},
                {"kind": "runtime", "key": "newrt", "runtime_ref": "runtime-NEW"},
                {"kind": "schedule", "key": "s"},
            ],
            "relations": [
                {
                    "kind": "schedule_binding",
                    "key": "rs",
                    "schedule": "s",
                    "process": "proc",
                    "runtime": "newrt",
                }
            ],
        }
    )
    ctx = TopologyResolutionContextV1(
        profile="p-alpha",
        snapshot=_snapshot(
            components=(
                ComponentFactV1(
                    profile="p-alpha", component_id="c1", component_type="process"
                ),
            ),
            runtimes=(
                RuntimeFactV1(profile="p-alpha", runtime_id="runtime-ALREADY-SCHEDULED"),
            ),
            pagination=_complete("process"),
        ),
    )
    assert validate_system_topology(spec, ctx).errors == ()


def test_an_authoritative_runtime_inventory_does_witness_absence():
    """The counterpart: when an inventory IS complete, absence is conclusive."""
    spec = parse_system_topology_v1(
        {
            "version": "1",
            "profile_ref": "p-alpha",
            "objects": [
                {"kind": "process", "key": "proc", "component_ref": "c1"},
                {"kind": "runtime", "key": "ghost", "runtime_ref": "runtime-GHOST"},
                {"kind": "schedule", "key": "s"},
            ],
            "relations": [
                {
                    "kind": "schedule_binding",
                    "key": "rs",
                    "schedule": "s",
                    "process": "proc",
                    "runtime": "ghost",
                }
            ],
        }
    )
    ctx = TopologyResolutionContextV1(
        profile="p-alpha",
        snapshot=_snapshot(
            components=(
                ComponentFactV1(
                    profile="p-alpha", component_id="c1", component_type="process"
                ),
            ),
            runtimes=(RuntimeFactV1(profile="p-alpha", runtime_id="runtime-REAL"),),
            pagination=_complete("process"),
            runtime_inventory_complete=True,
        ),
    )
    assert "TOPOLOGY_REFERENCE_NOT_FOUND" in _codes(validate_system_topology(spec, ctx))


def test_a_cycle_pointer_is_never_an_acyclic_bridge():
    """Two cycles joined by a bridge: source/sink pruning leaves the bridge.

    Every node keeps an in-edge and an out-edge, so nothing prunes and the
    bridge survives as if it were cyclic. Following a remediation that names it
    removes neither cycle.
    """
    from boomi_mcp.compiler.system_topology.dependencies import (
        collect_dependency_findings,
    )

    spec = parse_system_topology_v1(
        {
            "version": "1",
            "profile_ref": "p-alpha",
            "objects": [
                {"kind": "process", "key": f"n{i}", "component_ref": f"$ref:k{i}"}
                for i in range(4)
            ],
            # relation 0 is the BRIDGE n1->n2, in neither cycle.
            "relations": [
                {"kind": "process_call", "key": "r0", "caller_process": "n1", "callee_process": "n2"},
                {"kind": "process_call", "key": "r1", "caller_process": "n0", "callee_process": "n1"},
                {"kind": "process_call", "key": "r2", "caller_process": "n1", "callee_process": "n0"},
                {"kind": "process_call", "key": "r3", "caller_process": "n2", "callee_process": "n3"},
                {"kind": "process_call", "key": "r4", "caller_process": "n3", "callee_process": "n2"},
            ],
        }
    )
    findings = collect_dependency_findings(spec)
    assert findings
    assert findings[0].path != "/relations/0", "the bridge is in no cycle"
    assert findings[0].path in ("/relations/1", "/relations/2", "/relations/3", "/relations/4")


def test_a_tail_feeding_a_cycle_is_not_blamed_for_it():
    from boomi_mcp.compiler.system_topology.dependencies import (
        collect_dependency_findings,
    )

    spec = parse_system_topology_v1(
        {
            "version": "1",
            "profile_ref": "p-alpha",
            "objects": [
                {"kind": "process", "key": f"n{i}", "component_ref": f"$ref:k{i}"}
                for i in range(3)
            ],
            "relations": [
                {"kind": "process_call", "key": "r0", "caller_process": "n0", "callee_process": "n1"},
                {"kind": "process_call", "key": "r1", "caller_process": "n1", "callee_process": "n2"},
                {"kind": "process_call", "key": "r2", "caller_process": "n2", "callee_process": "n1"},
            ],
        }
    )
    findings = collect_dependency_findings(spec)
    assert findings and findings[0].path == "/relations/1"


# ---------------------------------------------------------------------------
# QA round 8 — the fixes' own pins were thinner than the fixes
# ---------------------------------------------------------------------------


def test_an_unsatisfiable_ref_is_judged_with_an_empty_symbol_table():
    """QA #221. The verdict must not depend on an UNRELATED symbol's presence.

    Guarding the ``$ref`` rule on "is the symbol table non-empty" made the same
    document flip from "no blockers, binding plannable" to "blocked, binding
    withdrawn" when one unrelated symbol was added — and left the endpoint
    *unjudged* rather than blocked, which defeats endpoint withdrawal.
    """
    spec = parse_system_topology_v1(
        {
            "version": "1",
            "profile_ref": "p-alpha",
            "objects": [{"kind": "process", "key": "a", "component_ref": "$ref:missing"}],
            "relations": [],
        }
    )
    empty = validate_system_topology(
        spec, TopologyResolutionContextV1(profile="p-alpha")
    )
    unrelated = validate_system_topology(
        spec,
        TopologyResolutionContextV1(
            profile="p-alpha",
            component_plan_symbols=(
                ComponentPlanSymbolV1(component_key="UNRELATED", component_type="process"),
            ),
        ),
    )
    assert _codes(empty) == ["TOPOLOGY_REFERENCE_NOT_FOUND"], _codes(empty)
    assert _codes(empty) == _codes(unrelated), (_codes(empty), _codes(unrelated))


def test_an_unsatisfiable_ref_also_withdraws_its_relation():
    """The endpoint must be BLOCKED, not merely unjudged, or fix 3 cannot fire."""
    from boomi_mcp.compiler.system_topology import plan_system_topology

    spec = parse_system_topology_v1(
        {
            "version": "1",
            "profile_ref": "p-alpha",
            "objects": [
                {"kind": "process", "key": "proc", "component_ref": "$ref:missing"},
                {"kind": "runtime", "key": "rt", "runtime_ref": "runtime-1"},
                {"kind": "schedule", "key": "s"},
            ],
            "relations": [
                {
                    "kind": "schedule_binding",
                    "key": "rs",
                    "schedule": "s",
                    "process": "proc",
                    "runtime": "rt",
                }
            ],
        }
    )
    plan = plan_system_topology(spec, TopologyResolutionContextV1(profile="p-alpha"))
    assert plan.blockers
    assert plan.planning_only_relations == ()


def test_the_resolution_table_is_type_aware_for_both_reference_forms():
    """QA #222. ``resolve_topology_references`` was pinned by nothing.

    Type-blindness could revert in BOTH branches with the suite green, producing
    exactly the plan self-contradiction its own source comment names: a Document
    Cache listed in ``resolved_references`` as a process, beside a type-mismatch
    blocker about it.
    """
    from boomi_mcp.compiler.system_topology.references import (
        resolve_topology_references,
    )
    from boomi_mcp.compiler.system_topology.context import prepare_topology_context

    # $ref branch: symbol is the wrong type.
    spec_ref = parse_system_topology_v1(
        {
            "version": "1",
            "profile_ref": "p-alpha",
            "objects": [{"kind": "process", "key": "p", "component_ref": "$ref:ck"}],
            "relations": [],
        }
    )
    prepared_ref = prepare_topology_context(
        TopologyResolutionContextV1(
            profile="p-alpha",
            component_plan_symbols=(
                ComponentPlanSymbolV1(component_key="ck", component_type="documentcache"),
            ),
        )
    )
    assert resolve_topology_references(spec_ref, prepared_ref) == ()

    # literal branch: live component is the wrong type.
    spec_lit = parse_system_topology_v1(
        {
            "version": "1",
            "profile_ref": "p-alpha",
            "objects": [{"kind": "process", "key": "p", "component_ref": "cache-1"}],
            "relations": [],
        }
    )
    prepared_lit = prepare_topology_context(
        TopologyResolutionContextV1(
            profile="p-alpha",
            snapshot=_snapshot(
                components=(
                    ComponentFactV1(
                        profile="p-alpha",
                        component_id="cache-1",
                        component_type="documentcache",
                    ),
                )
            ),
        )
    )
    assert resolve_topology_references(spec_lit, prepared_lit) == ()

    # Negative control: correct types DO resolve, in both branches.
    spec_ok = parse_system_topology_v1(
        {
            "version": "1",
            "profile_ref": "p-alpha",
            "objects": [
                {"kind": "process", "key": "p", "component_ref": "$ref:pk"},
                {"kind": "document_cache", "key": "c", "component_ref": "cache-1"},
            ],
            "relations": [],
        }
    )
    prepared_ok = prepare_topology_context(
        TopologyResolutionContextV1(
            profile="p-alpha",
            component_plan_symbols=(
                ComponentPlanSymbolV1(component_key="pk", component_type="process"),
            ),
            snapshot=_snapshot(
                components=(
                    ComponentFactV1(
                        profile="p-alpha",
                        component_id="cache-1",
                        component_type="documentcache",
                    ),
                )
            ),
        )
    )
    assert len(resolve_topology_references(spec_ok, prepared_ok)) == 2


@pytest.mark.parametrize(
    "family,fact",
    [
        (
            "components",
            lambda: ComponentFactV1(
                profile="OTHER", component_id="x", component_type="process"
            ),
        ),
        (
            "environments",
            lambda: EnvironmentFactV1(
                profile="OTHER", environment_id="e", classification="TEST"
            ),
        ),
        ("runtimes", lambda: RuntimeFactV1(profile="OTHER", runtime_id="r")),
    ],
)
def test_every_fact_family_is_profile_filtered(family, fact):
    """QA #225. Fix 4 was pinned for ONE of the fact families.

    Un-filtering environments let a foreign environment resolve with
    ``foreign_count: 0`` and no error at all.
    """
    from boomi_mcp.compiler.system_topology.context import prepare_topology_context

    prepared = prepare_topology_context(
        TopologyResolutionContextV1(
            profile="p-alpha", snapshot=_snapshot(**{family: (fact(),)})
        )
    )
    assert prepared.foreign_profile_fact_count == 1, family
    assert prepared.components == ()
    assert prepared.environment_ids == ()
    assert prepared.runtime_ids == ()


def test_schedule_and_deployment_facts_are_counted_as_foreign_too():
    from boomi_mcp.compiler.system_topology.context import (
        DeploymentFactV1,
        ScheduleBindingFactV1,
        prepare_topology_context,
    )

    prepared = prepare_topology_context(
        TopologyResolutionContextV1(
            profile="p-alpha",
            snapshot=_snapshot(
                schedule_bindings=(
                    ScheduleBindingFactV1(
                        profile="OTHER", process_id="p", runtime_id="r"
                    ),
                ),
                deployments=(
                    DeploymentFactV1(
                        profile="OTHER", component_id="c", environment_id="e"
                    ),
                ),
            ),
        )
    )
    assert prepared.foreign_profile_fact_count == 2


def test_same_profile_facts_are_kept():
    """Negative control: the filter must not discard legitimate facts."""
    from boomi_mcp.compiler.system_topology.context import prepare_topology_context

    prepared = prepare_topology_context(
        TopologyResolutionContextV1(
            profile="p-alpha",
            snapshot=_snapshot(
                components=(
                    ComponentFactV1(
                        profile="p-alpha", component_id="x", component_type="process"
                    ),
                ),
                environments=(
                    EnvironmentFactV1(
                        profile="p-alpha", environment_id="e", classification="TEST"
                    ),
                ),
                runtimes=(RuntimeFactV1(profile="p-alpha", runtime_id="r"),),
            ),
        )
    )
    assert prepared.foreign_profile_fact_count == 0
    assert prepared.components == (("x", "process"),)
    assert prepared.environment_ids == ("e",)
    assert prepared.runtime_ids == ("r",)


@pytest.mark.parametrize(
    "authored,expected",
    [
        ("api_service", "webservice"),
        ("api.service", "webservice"),
        ("API_SERVICE", "webservice"),
        ("Process", "process"),
        ("webservice", "webservice"),
        ("documentcache", "documentcache"),
        ("processproperty", "processproperty"),
    ],
)
def test_a_builder_component_type_alias_does_not_become_a_type_mismatch(
    authored, expected
):
    """QA #226. The projection takes a RAW spec and cannot assume normalization.

    ``build_integration`` normalizes ``api_service`` to ``webservice`` before
    planning, but ``project_component_plan_symbols`` is given the spec directly.
    Passing the type through verbatim made a perfectly valid authored alias
    report a TYPE_MISMATCH against its own object.
    """
    from boomi_mcp.models.integration_models import IntegrationSpecV1
    from boomi_mcp.compiler.system_topology.context import (
        project_component_plan_symbols,
    )

    spec = IntegrationSpecV1(
        name="alias fixture", components=[{"key": "k", "type": authored}]
    )
    symbols = project_component_plan_symbols(spec)
    assert symbols[0].component_type == expected, (authored, symbols[0].component_type)


def test_the_topology_alias_map_agrees_with_the_builders():
    """The alias map is a COPY; drift between the two is the failure mode."""
    from boomi_mcp.categories.integration_builder import _normalize_component_type
    from boomi_mcp.compiler.system_topology.context import (
        _COMPONENT_TYPE_ALIASES,
        _normalize_component_type as topology_normalize,
    )

    for alias in _COMPONENT_TYPE_ALIASES:
        assert topology_normalize(alias) == _normalize_component_type(alias), alias
    # And every Boomi type the topology contract requires is reachable.
    from boomi_mcp.compiler.system_topology.references import _COMPONENT_BACKED

    for required in set(_COMPONENT_BACKED.values()):
        assert topology_normalize(required) == required, required
        assert _normalize_component_type(required) == required, required


# ---------------------------------------------------------------------------
# Codex review round 2 — three P2 defects in the round-1 fixes
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("alias", ["api_service", "api.service", "API_SERVICE"])
def test_an_alias_supplied_through_the_public_context_is_normalized(alias):
    """R2. ``TopologyResolutionContextV1`` is a public input.

    A caller can assemble ``component_plan_symbols`` directly and never touch
    ``project_component_plan_symbols``, so normalizing only in the projection
    left a builder-legal alias reporting a type mismatch against its own object.
    ``prepare_topology_context`` is the one gate every path passes through.
    """
    spec = parse_system_topology_v1(
        {
            "version": "1",
            "profile_ref": "p-alpha",
            "objects": [{"kind": "api_service", "key": "a", "component_ref": "$ref:ak"}],
            "relations": [],
        }
    )
    ctx = TopologyResolutionContextV1(
        profile="p-alpha",
        component_plan_symbols=(
            ComponentPlanSymbolV1(component_key="ak", component_type=alias),
        ),
    )
    assert validate_system_topology(spec, ctx).errors == (), alias


def test_a_live_component_fact_type_is_normalized_too():
    spec = parse_system_topology_v1(
        {
            "version": "1",
            "profile_ref": "p-alpha",
            "objects": [{"kind": "api_service", "key": "a", "component_ref": "asc-1"}],
            "relations": [],
        }
    )
    ctx = TopologyResolutionContextV1(
        profile="p-alpha",
        snapshot=_snapshot(
            components=(
                ComponentFactV1(
                    profile="p-alpha", component_id="asc-1", component_type="API_Service"
                ),
            ),
            pagination=_complete("webservice"),
        ),
    )
    assert validate_system_topology(spec, ctx).errors == ()


def _ordered_witness_verdict(evidence):
    spec = parse_system_topology_v1(
        {
            "version": "1",
            "profile_ref": "p-alpha",
            "objects": [
                {"kind": "process", "key": "a", "component_ref": "$ref:ka"},
                {"kind": "process", "key": "b", "component_ref": "$ref:kb"},
            ],
            "relations": [
                {"kind": "process_call", "key": "r", "caller_process": "a", "callee_process": "b"}
            ],
        }
    )
    ctx = TopologyResolutionContextV1(
        profile="p-alpha",
        component_plan_symbols=(
            ComponentPlanSymbolV1(component_key="ka", component_type="process"),
            ComponentPlanSymbolV1(component_key="kb", component_type="process"),
        ),
        process_call_evidence=evidence,
    )
    return _codes(validate_system_topology(spec, ctx))


def test_the_witness_verdict_does_not_depend_on_evidence_order():
    """R2. The same evidence SET gave two different verdicts.

    A ``{key: row.witness for row in ...}`` comprehension keeps the LAST row for
    a duplicated key, and nothing constrains the evidence tuple to be unique —
    so a valid ``process_ir`` beside a stale ``component_xml`` gated or passed
    purely on their order. A contract whose central claim is determinism cannot
    decide on input order.
    """
    good = ProcessCallEvidenceV1(
        caller_component_ref="$ref:ka",
        callee_component_ref="$ref:kb",
        witness="process_ir",
    )
    stale = ProcessCallEvidenceV1(
        caller_component_ref="$ref:ka",
        callee_component_ref="$ref:kb",
        witness="component_xml",
    )
    forward = _ordered_witness_verdict((good, stale))
    reverse = _ordered_witness_verdict((stale, good))
    assert forward == reverse, (forward, reverse)
    assert forward == [], forward


def test_adding_evidence_never_removes_a_witness():
    """Monotonicity: more evidence must not gate a previously-witnessed edge."""
    good = ProcessCallEvidenceV1(
        caller_component_ref="$ref:ka",
        callee_component_ref="$ref:kb",
        witness="process_ir",
    )
    stale = ProcessCallEvidenceV1(
        caller_component_ref="$ref:ka",
        callee_component_ref="$ref:kb",
        witness="component_xml",
    )
    assert _ordered_witness_verdict((good,)) == []
    assert _ordered_witness_verdict((good, stale)) == []
    assert _ordered_witness_verdict((good, stale, stale)) == []


def test_only_wrong_form_evidence_still_gates():
    """Negative control: the form rule must still bite when nothing fits."""
    stale = ProcessCallEvidenceV1(
        caller_component_ref="$ref:ka",
        callee_component_ref="$ref:kb",
        witness="component_xml",
    )
    assert _ordered_witness_verdict((stale, stale)) == ["TOPOLOGY_CAPABILITY_GATED"]


def test_the_planned_witness_reported_is_deterministic():
    """The witness recorded in the plan must not depend on order either."""
    from boomi_mcp.compiler.system_topology import plan_system_topology

    spec = parse_system_topology_v1(
        {
            "version": "1",
            "profile_ref": "p-alpha",
            "objects": [
                {"kind": "process", "key": "a", "component_ref": "$ref:ka"},
                {"kind": "process", "key": "b", "component_ref": "$ref:kb"},
            ],
            "relations": [
                {"kind": "process_call", "key": "r", "caller_process": "a", "callee_process": "b"}
            ],
        }
    )
    symbols = (
        ComponentPlanSymbolV1(component_key="ka", component_type="process"),
        ComponentPlanSymbolV1(component_key="kb", component_type="process"),
    )
    rows = [
        ProcessCallEvidenceV1(
            caller_component_ref="$ref:ka",
            callee_component_ref="$ref:kb",
            witness=w,
        )
        for w in ("process_ir", "component_xml")
    ]
    seen = set()
    for order in (tuple(rows), tuple(reversed(rows))):
        plan = plan_system_topology(
            spec,
            TopologyResolutionContextV1(
                profile="p-alpha",
                component_plan_symbols=symbols,
                process_call_evidence=order,
            ),
        )
        seen.add(tuple((r.relation_key, r.witness) for r in plan.planning_only_relations))
    assert len(seen) == 1, seen


def test_cycle_detection_is_linear_in_the_number_of_components():
    """R2. One self-call per process makes components == arcs.

    Scanning every SCC per arc is O(arcs x components); ``SystemTopologySpecV1``
    bounds neither objects nor relations, and self-calls reach this collector
    even though the relation phase also flags them. Ratio-based so the test is
    not machine-speed dependent.
    """
    import time

    from boomi_mcp.compiler.system_topology.dependencies import (
        collect_dependency_findings,
    )

    def build(n):
        return parse_system_topology_v1(
            {
                "version": "1",
                "profile_ref": "p-alpha",
                "objects": [
                    {"kind": "process", "key": f"n{i}", "component_ref": f"$ref:k{i}"}
                    for i in range(n)
                ],
                "relations": [
                    {
                        "kind": "process_call",
                        "key": f"r{i}",
                        "caller_process": f"n{i}",
                        "callee_process": f"n{i}",
                    }
                    for i in range(n)
                ],
            }
        )

    def elapsed(spec):
        start = time.perf_counter()
        collect_dependency_findings(spec)
        return time.perf_counter() - start

    small_spec, large_spec = build(600), build(2400)
    # Warm any lazy import so it is not charged to the first measurement.
    collect_dependency_findings(build(10))
    small = min(elapsed(small_spec) for _ in range(3))
    large = min(elapsed(large_spec) for _ in range(3))
    # 4x the input. Linear is ~4x; the quadratic version was ~16x.
    assert large < small * 9, (small, large)


def test_at_most_one_witness_kind_is_accepted_per_form():
    """Why the first-vs-last choice in ``_accepted_witness`` is unobservable.

    Each evidence model's ``witness`` Literal overlaps each form's accepted set
    in exactly one value, so the accepted list never holds two distinct kinds
    and the tie-break cannot be observed. That is a property of the current
    Literals, not a guarantee — pinned here so widening one surfaces the
    ambiguity instead of quietly making plan output depend on evidence order
    again, which is the defect the sort exists to prevent.
    """
    from typing import get_args

    from boomi_mcp.compiler.system_topology.context import (
        ApiServiceRouteEvidenceV1,
        ProcessCallEvidenceV1,
        SharedResourceUseEvidenceV1,
    )
    from boomi_mcp.compiler.system_topology.relations import _WITNESS_FORMS

    models = (
        ProcessCallEvidenceV1,
        ApiServiceRouteEvidenceV1,
        SharedResourceUseEvidenceV1,
    )
    for model in models:
        declared = set(get_args(model.model_fields["witness"].annotation))
        assert declared, model.__name__
        for form, accepted in _WITNESS_FORMS.items():
            overlap = declared & set(accepted)
            assert len(overlap) <= 1, (model.__name__, form, sorted(overlap))
        # And every form is reachable — a model no form accepts would make its
        # relation permanently ungateable-into-plannable.
        assert any(declared & set(a) for a in _WITNESS_FORMS.values()), model.__name__


# ---------------------------------------------------------------------------
# QA round 9 — one product defect the round-2 fix unmasked, two pin gaps
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "page_vocab", ["webservice", "api_service", "api.service", "Webservice", "API_Service"]
)
def test_a_pagination_vocabulary_alias_still_witnesses_absence(page_vocab):
    """QA #227. The THIRD type-bearing field needed normalizing too.

    ``complete_component_types`` is compared against the canonical expected
    type, so a page recorded in any alias vocabulary never matched — absence
    stopped being conclusive and a ghost component went UNJUDGED rather than
    blocked. Pre-fix this was unreachable because the symbol type mismatch
    stopped the caller first; normalizing the other two fields unmasked it.
    """
    spec = parse_system_topology_v1(
        {
            "version": "1",
            "profile_ref": "p-alpha",
            "objects": [
                {"kind": "api_service", "key": "real", "component_ref": "real-asc"},
                {"kind": "api_service", "key": "ghost", "component_ref": "does-not-exist"},
            ],
            "relations": [],
        }
    )
    from boomi_mcp.compiler.system_topology.context import DiscoveryPageProvenanceV1

    ctx = TopologyResolutionContextV1(
        profile="p-alpha",
        snapshot=_snapshot(
            components=(
                ComponentFactV1(
                    profile="p-alpha", component_id="real-asc", component_type="webservice"
                ),
            ),
            pagination=(
                DiscoveryPageProvenanceV1(
                    component_type=page_vocab, returned_count=1, total_available=1
                ),
            ),
        ),
    )
    codes = _codes(validate_system_topology(spec, ctx))
    assert codes == ["TOPOLOGY_REFERENCE_NOT_FOUND"], (page_vocab, codes)


def test_all_three_type_bearing_fields_are_normalized_together():
    """The invariant behind #226/#227: every type that reaches a comparison.

    Stated as one test so a fourth type-bearing field added later has an
    obvious home, rather than being normalized in two places out of three
    again.
    """
    from boomi_mcp.compiler.system_topology.context import (
        DiscoveryPageProvenanceV1,
        prepare_topology_context,
    )

    prepared = prepare_topology_context(
        TopologyResolutionContextV1(
            profile="p-alpha",
            component_plan_symbols=(
                ComponentPlanSymbolV1(component_key="k", component_type="API_Service"),
            ),
            snapshot=_snapshot(
                components=(
                    ComponentFactV1(
                        profile="p-alpha", component_id="c", component_type="api.service"
                    ),
                ),
                pagination=(
                    DiscoveryPageProvenanceV1(
                        component_type="api_service", returned_count=1, total_available=1
                    ),
                ),
            ),
        )
    )
    assert prepared.symbols == (("k", "webservice"),)
    assert prepared.components == (("c", "webservice"),)
    assert prepared.complete_component_types == ("webservice",)


def test_duplicate_accepted_witnesses_still_witness():
    """QA #228. The monotonicity pin only ever appended REJECTED-form rows.

    Those are filtered before ``accepted`` is built, so it never held two
    entries and ``accepted[0] if len(accepted) == 1 else None`` survived —
    which is not equivalent: two identical valid rows would gate a relation the
    real code correctly witnesses. Nothing constrains the evidence tuple to be
    unique, which is the whole premise of the order-independence fix.
    """
    good = ProcessCallEvidenceV1(
        caller_component_ref="$ref:ka",
        callee_component_ref="$ref:kb",
        witness="process_ir",
    )
    assert _ordered_witness_verdict((good, good)) == []
    assert _ordered_witness_verdict((good, good, good)) == []


def test_the_accepted_witness_helper_handles_repeats_directly():
    """The same property at the unit boundary, so the cause is unambiguous."""
    from boomi_mcp.compiler.system_topology.relations import _accepted_witness

    assert _accepted_witness("$ref:k", ["process_ir"]) == "process_ir"
    assert _accepted_witness("$ref:k", ["process_ir", "process_ir"]) == "process_ir"
    assert (
        _accepted_witness("$ref:k", ["process_ir", "component_xml", "process_ir"])
        == "process_ir"
    )
    assert _accepted_witness("literal", ["component_xml", "component_xml"]) == "component_xml"
    assert _accepted_witness("$ref:k", ["component_xml"]) is None
    assert _accepted_witness("$ref:k", []) is None


def test_the_cycle_index_covers_every_component_not_just_the_first():
    """QA #229. The existing pointer test's fixture has ONE cyclic component.

    With a single component, which components get indexed cannot matter, so
    ``enumerate(cyclic[:1])`` survived.

    Two disjoint cycles are necessary but not sufficient: Tarjan emits
    components in TRAVERSAL order (node-sorted), while the pointer is the
    earliest AUTHORED index. The two orders must disagree, or indexing only the
    first component still lands on the same answer by accident — which is what
    a first attempt at this test did. So the cycle Tarjan finds LAST is the one
    authored FIRST.
    """
    from boomi_mcp.compiler.system_topology.dependencies import (
        _arcs,
        _cyclic_sccs,
        collect_dependency_findings,
    )

    spec = parse_system_topology_v1(
        {
            "version": "1",
            "profile_ref": "p-alpha",
            "objects": [
                {"kind": "process", "key": f"n{i}", "component_ref": f"$ref:k{i}"}
                for i in range(4)
            ],
            "relations": [
                # cycle B (n2<->n3) authored FIRST, but traversal reaches it SECOND.
                {"kind": "process_call", "key": "r0", "caller_process": "n2", "callee_process": "n3"},
                {"kind": "process_call", "key": "r1", "caller_process": "n3", "callee_process": "n2"},
                # cycle A (n0<->n1) authored second, traversal reaches it FIRST.
                {"kind": "process_call", "key": "r2", "caller_process": "n0", "callee_process": "n1"},
                {"kind": "process_call", "key": "r3", "caller_process": "n1", "callee_process": "n0"},
            ],
        }
    )
    # The premise, asserted rather than assumed: the two orders really disagree.
    components = _cyclic_sccs(tuple(sorted(f"n{i}" for i in range(4))), _arcs(spec))
    assert len(components) == 2
    assert components[0] == frozenset({"n0", "n1"}), components
    # ...so a first-component-only index would answer /relations/2.

    findings = collect_dependency_findings(spec)
    assert findings
    # The EARLIEST authored internal edge, across BOTH components.
    assert findings[0].path == "/relations/0", findings[0].path


def test_two_disjoint_cycles_are_both_indexed():
    """The same coverage, asserted on membership rather than the pointer."""
    from boomi_mcp.compiler.system_topology.dependencies import _arcs, _cyclic_sccs

    spec = parse_system_topology_v1(
        {
            "version": "1",
            "profile_ref": "p-alpha",
            "objects": [
                {"kind": "process", "key": f"n{i}", "component_ref": f"$ref:k{i}"}
                for i in range(4)
            ],
            "relations": [
                {"kind": "process_call", "key": "r0", "caller_process": "n0", "callee_process": "n1"},
                {"kind": "process_call", "key": "r1", "caller_process": "n1", "callee_process": "n0"},
                {"kind": "process_call", "key": "r2", "caller_process": "n2", "callee_process": "n3"},
                {"kind": "process_call", "key": "r3", "caller_process": "n3", "callee_process": "n2"},
            ],
        }
    )
    nodes = tuple(sorted(f"n{i}" for i in range(4)))
    components = _cyclic_sccs(nodes, _arcs(spec))
    assert len(components) == 2, components
    assert set().union(*components) == set(nodes)


# ---------------------------------------------------------------------------
# QA round 10 — the last-wins class at its final site, and two ungraded pins
# ---------------------------------------------------------------------------


def _classification_verdict(env_facts, authored="TEST"):
    """Drive the classification rule for a given AUTHORED value.

    Parametrized on ``authored`` because hard-coding ``TEST`` left half the
    rule's domain ungraded: a comparison written as ``sorted(seen)[0] !=
    authored`` reintroduces the self-contradiction fail-open for authored
    ``PROD`` only, and one written to also fire on ``PROD`` reports every
    correctly-authored production environment as a mismatch — a blocking false
    positive on a legitimate topology. Both survived the whole suite while the
    mirrored edit, which breaks the ``TEST`` direction, was caught immediately.
    """
    spec = parse_system_topology_v1(
        {
            "version": "1",
            "profile_ref": "p-alpha",
            "objects": [
                {"kind": "process", "key": "x", "component_ref": "$ref:k"},
                {
                    "kind": "environment",
                    "key": "e",
                    "environment_ref": "env-1",
                    "classification": authored,
                },
            ],
            "relations": [],
        }
    )
    ctx = TopologyResolutionContextV1(
        profile="p-alpha",
        component_plan_symbols=(
            ComponentPlanSymbolV1(component_key="k", component_type="process"),
        ),
        snapshot=_snapshot(environments=env_facts),
    )
    return [
        d
        for d in validate_system_topology(spec, ctx).errors
        if d.code == "TOPOLOGY_ENVIRONMENT_MISMATCH"
    ]


_REAL = EnvironmentFactV1(
    profile="p-alpha", environment_id="env-1", classification="PROD"
)
_BLANK = EnvironmentFactV1(
    profile="p-alpha", environment_id="env-1", classification=None
)
_AGREES = EnvironmentFactV1(
    profile="p-alpha", environment_id="env-1", classification="TEST"
)
#: The same fact from the other side of the closed domain, so every rule below
#: can be asserted in both directions rather than only the one that happens to
#: match the fixture's authored value.
_PROD_AGREES = EnvironmentFactV1(
    profile="p-alpha", environment_id="env-1", classification="PROD"
)


def test_a_blank_duplicate_cannot_erase_an_observed_contradiction():
    """QA #230. A last-wins comprehension failed OPEN, not merely order-dependent.

    ``classification=None`` is the designed output of ``_opt_classification``
    for a missing or mis-cased field, so a second row carrying one overwrote a
    real ``PROD`` observation and a blocked plan came back valid. Unobserved
    rows must contribute nothing rather than overwrite.
    """
    assert _classification_verdict((_REAL,)), "baseline: the contradiction is seen"
    assert _classification_verdict((_REAL, _BLANK)), "a blank must not erase it"
    assert _classification_verdict((_BLANK, _REAL))
    assert _classification_verdict((_BLANK, _REAL, _BLANK))


def test_the_classification_verdict_is_order_independent():
    forward = _classification_verdict((_REAL, _BLANK))
    reverse = _classification_verdict((_BLANK, _REAL))
    assert [d.path for d in forward] == [d.path for d in reverse]


def test_an_agreeing_or_unobserved_classification_is_not_a_finding():
    """Negative control: only a genuine contradiction may fire."""
    assert _classification_verdict((_AGREES,)) == []
    assert _classification_verdict((_BLANK,)) == []
    assert _classification_verdict(()) == []
    assert _classification_verdict((_AGREES, _BLANK)) == []


def test_a_foreign_profile_row_never_supplies_a_classification():
    """The collector read the RAW snapshot, so another account's row decided this.

    The mixed-profile snapshot is still reported — at ``/profile_ref``, which is
    what it actually is — but no classification claim is built on it.
    """
    foreign = EnvironmentFactV1(
        profile="OTHER-ACCOUNT", environment_id="env-1", classification="PROD"
    )
    findings = _classification_verdict((foreign,))
    assert findings, "the mixed-profile snapshot itself must still be reported"
    assert all(d.path == "/profile_ref" for d in findings), [d.path for d in findings]
    assert all(
        "mixed-profile-snapshot" in " ".join(d.provenance) for d in findings
    ), findings


def test_no_last_wins_dict_survives_over_a_raw_caller_supplied_collection():
    """The CLASS, not the instance.

    Rounds 2, 3 and this one were all the same defect: a dict comprehension over
    a caller-supplied collection that nothing constrains to be unique. Scanning
    the source keeps a fourth instance from being written; the comment on each
    surviving comprehension has to say why its key is unique.
    """
    import ast

    package = (
        _project_root / "src" / "boomi_mcp" / "compiler" / "system_topology"
    )
    offenders = []
    for path in sorted(package.glob("*.py")):
        tree = ast.parse(path.read_text())
        for node in ast.walk(tree):
            if not isinstance(node, ast.DictComp):
                continue
            source = ast.get_source_segment(path.read_text(), node) or ""
            # A comprehension over the PREPARED index is fine: it is normalized,
            # deduplicated and sorted by construction.
            if "prepared.symbols" in source or "prepared.components" in source:
                continue
            # Over spec.objects/relations is fine: duplicate keys are already a
            # schema error, and those sites use setdefault deliberately.
            if "spec.objects" in source or "spec.relations" in source:
                continue
            if "snapshot." in source or "ctx." in source or "context." in source:
                offenders.append((path.name, source.split("\n")[0]))
    assert offenders == [], offenders


def test_self_contradicting_duplicate_classifications_are_reported():
    """R4. ``not in seen`` was too weak.

    With both TEST and PROD observed for one environment, an authored TEST is
    "in" the set and passed silently — the plan came back valid on evidence
    that contradicts itself. Every observation must agree.
    """
    conflicting = (
        EnvironmentFactV1(
            profile="p-alpha", environment_id="env-1", classification="TEST"
        ),
        EnvironmentFactV1(
            profile="p-alpha", environment_id="env-1", classification="PROD"
        ),
    )
    assert _classification_verdict(conflicting), "self-contradicting evidence"
    # Either order, same verdict.
    assert _classification_verdict(tuple(reversed(conflicting)))


def test_unanimous_agreeing_duplicates_are_still_not_a_finding():
    """Negative control: agreement must stay silent however many rows say it."""
    agreeing = (_AGREES, _AGREES, _AGREES)
    assert _classification_verdict(agreeing) == []
    # And a blank alongside unanimous agreement changes nothing.
    assert _classification_verdict((_AGREES, _BLANK, _AGREES)) == []



# ---------------------------------------------------------------------------
# QA #233 — the classification rule, graded in BOTH authored directions
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("authored", ["TEST", "PROD"])
def test_self_contradicting_duplicates_are_reported_for_either_authored_value(
    authored,
):
    """``classification`` is a closed two-value domain, so both sides are cheap.

    Grading only ``TEST`` left a comparison that fails open for ``PROD``
    indistinguishable from the correct one.
    """
    conflicting = (_AGREES, _PROD_AGREES)
    assert _classification_verdict(conflicting, authored)
    assert _classification_verdict(tuple(reversed(conflicting)), authored)


@pytest.mark.parametrize(
    "authored,agreeing",
    [("TEST", _AGREES), ("PROD", _PROD_AGREES)],
)
def test_unanimous_agreement_is_silent_for_either_authored_value(authored, agreeing):
    """The false-positive direction.

    A rule that also fired on a correctly-authored value would block every
    legitimate production topology — worse than the fail-open it replaced,
    because it rejects correct input.
    """
    assert _classification_verdict((agreeing,), authored) == []
    assert _classification_verdict((agreeing, agreeing), authored) == []
    assert _classification_verdict((agreeing, _BLANK), authored) == []


@pytest.mark.parametrize(
    "authored,observed",
    [("TEST", _PROD_AGREES), ("PROD", _AGREES)],
)
def test_a_single_contradicting_observation_fires_either_way(authored, observed):
    assert _classification_verdict((observed,), authored)


@pytest.mark.parametrize("authored", ["TEST", "PROD"])
def test_blank_and_missing_stay_silent_for_either_authored_value(authored):
    assert _classification_verdict((), authored) == []
    assert _classification_verdict((_BLANK,), authored) == []
    assert _classification_verdict((_BLANK, _BLANK), authored) == []


#: Every distinct CAUSE each code is emitted for, with a phrase that must
#: appear in its remediation. Hand-maintained on purpose: the point is that
#: adding an emit site forces someone to decide what to tell the caller.
#:
#: The counts are checked against the source, so a new ``topology_finding``
#: call for one of these codes fails until this map and the remediation catch
#: up. That is the class behind #234, #235, #236 and #237 — a remediation that
#: enumerates causes but omits one it actually fires for, sending a caller to
#: fix something they do not have.
_EMIT_SITE_CAUSES = {
    "TOPOLOGY_ENVIRONMENT_MISMATCH": (
        "context names a different profile",
        "snapshot envelope does",
        "inside the snapshot",
        "more than one classification",
    ),
    "TOPOLOGY_RELATION_UNSUPPORTED": (
        "invoked by its api service",
        "binds one process to one runtime",
        "exactly one process and one environment",
        "cannot call itself",
    ),
    "TOPOLOGY_CAPABILITY_GATED": (
        "kind is supported",
        "kind itself is gated",
        "capability report",
    ),
}


def _emit_site_counts():
    """How many times each code is passed to ``topology_finding`` in the package."""
    import ast
    import collections

    package = _project_root / "src" / "boomi_mcp" / "compiler" / "system_topology"
    counts = collections.Counter()
    for path in sorted(package.glob("*.py")):
        for node in ast.walk(ast.parse(path.read_text())):
            if (
                isinstance(node, ast.Call)
                and getattr(node.func, "id", "") == "topology_finding"
                and node.args
                and isinstance(node.args[0], ast.Name)
                and node.args[0].id.startswith("TOPOLOGY_")
            ):
                counts[node.args[0].id] += 1
    return counts


def test_the_emit_site_scan_finds_real_call_sites():
    """Positive control: a scanner returning nothing passes every test below."""
    counts = _emit_site_counts()
    assert counts, counts
    assert counts["TOPOLOGY_ENVIRONMENT_MISMATCH"] >= 4, counts


@pytest.mark.parametrize("code", sorted(_EMIT_SITE_CAUSES))
def test_every_emit_site_has_a_cause_named_in_its_remediation(code):
    """QA #235/#236/#237. A remediation must describe every case it is served for.

    ``topology_finding`` attaches the static remediation to EVERY finding for a
    code, so a string that enumerates three causes while the collector emits it
    for four sends one caller in four to fix something they do not have — and
    for the mixed-profile-snapshot case the omitted instruction was actively
    harmful, because aligning the profile trips a different site.
    """
    from boomi_mcp.compiler.system_topology.findings import _REMEDIATION

    remediation = _REMEDIATION[code].lower()
    causes = _EMIT_SITE_CAUSES[code]
    for phrase in causes:
        assert phrase in remediation, (code, phrase)
    # The map must not fall behind the source.
    assert _emit_site_counts()[code] == len(causes), (
        code,
        _emit_site_counts()[code],
        len(causes),
    )


def test_the_environment_mismatch_remediation_offers_a_reachable_action():
    """QA #234/#235. Two of its four causes admit no authored fix at all."""
    from boomi_mcp.compiler.system_topology.findings import _REMEDIATION
    from boomi_mcp.errors import TOPOLOGY_ENVIRONMENT_MISMATCH

    remediation = _REMEDIATION[TOPOLOGY_ENVIRONMENT_MISMATCH].lower()
    assert "align the profile" in remediation
    assert "re-capture the snapshot" in remediation


def test_a_foreign_row_inside_an_agreeing_snapshot_is_a_described_cause():
    """#235's exact shape: every enumerated alignment already holds.

    The context, the snapshot envelope and the topology all name one profile,
    and the environment classification agrees — yet the code fires, for a row
    inside the snapshot. Telling this caller to "align the profile" would send
    them to change ``profile_ref`` and trip two other sites.
    """
    spec = parse_system_topology_v1(
        {
            "version": "1",
            "profile_ref": "p-alpha",
            "objects": [
                {"kind": "process", "key": "x", "component_ref": "$ref:k"},
                {
                    "kind": "environment",
                    "key": "e",
                    "environment_ref": "env-1",
                    "classification": "TEST",
                },
            ],
            "relations": [],
        }
    )
    ctx = TopologyResolutionContextV1(
        profile="p-alpha",
        component_plan_symbols=(
            ComponentPlanSymbolV1(component_key="k", component_type="process"),
        ),
        snapshot=_snapshot(
            components=(
                ComponentFactV1(
                    profile="p-omega", component_id="c", component_type="process"
                ),
            ),
            environments=(_AGREES,),
        ),
    )
    findings = [
        d
        for d in validate_system_topology(spec, ctx).errors
        if d.code == "TOPOLOGY_ENVIRONMENT_MISMATCH"
    ]
    assert findings
    assert any(
        "mixed-profile-snapshot" in " ".join(d.provenance) for d in findings
    ), findings
    assert "inside the snapshot" in findings[0].remediation.lower()


def test_a_gated_relation_kind_and_a_witness_less_one_get_usable_text():
    """QA #237. One string serves two categorically different cases.

    A ``document_cache_use`` with no witness is ``plannable-only`` — supplying
    evidence clears it, and there is no issue to file. Telling that caller
    "adding support requires a separate evidence-backed issue" is the
    most-served misdirect the table had.
    """
    from boomi_mcp.compiler.system_topology.capabilities import capability_for

    assert capability_for("document_cache_use").state == "plannable-only"
    assert capability_for("external_queue").state == "gated-no-evidence"

    spec = parse_system_topology_v1(
        {
            "version": "1",
            "profile_ref": "p-alpha",
            "objects": [
                {"kind": "process", "key": "p", "component_ref": "$ref:pk"},
                {"kind": "document_cache", "key": "c", "component_ref": "$ref:ck"},
            ],
            "relations": [
                {"kind": "document_cache_use", "key": "r", "process": "p", "document_cache": "c"}
            ],
        }
    )
    ctx = TopologyResolutionContextV1(
        profile="p-alpha",
        component_plan_symbols=(
            ComponentPlanSymbolV1(component_key="pk", component_type="process"),
            ComponentPlanSymbolV1(component_key="ck", component_type="documentcache"),
        ),
    )
    gated = [
        d
        for d in validate_system_topology(spec, ctx).errors
        if d.code == "TOPOLOGY_CAPABILITY_GATED"
    ]
    assert gated
    text = gated[0].remediation.lower()
    # The supported-kind branch must be present and reachable...
    assert "kind is supported" in text
    assert "witness" in text
    # ...and supplying the witness must actually clear it.
    from boomi_mcp.compiler.system_topology.context import SharedResourceUseEvidenceV1

    witnessed = TopologyResolutionContextV1(
        profile="p-alpha",
        component_plan_symbols=ctx.component_plan_symbols,
        shared_resource_use_evidence=(
            SharedResourceUseEvidenceV1(
                process_component_ref="$ref:pk",
                resource_component_ref="$ref:ck",
                resource_kind="document_cache",
                witness="process_ir",
            ),
        ),
    )
    assert validate_system_topology(spec, witnessed).errors == ()


def test_a_self_call_finding_names_self_recursion():
    """QA #236. The cycle collector is skipped when references do not resolve.

    In that case the self-call finding is the ONLY one a caller gets, and its
    text is the one that omitted the cause — despite the source comment saying
    this check exists so the finding names the offending role rather than a
    graph.
    """
    spec = parse_system_topology_v1(
        {
            "version": "1",
            "profile_ref": "p-alpha",
            "objects": [{"kind": "process", "key": "a", "component_ref": "$ref:missing"}],
            "relations": [
                {"kind": "process_call", "key": "r", "caller_process": "a", "callee_process": "a"}
            ],
        }
    )
    report = validate_system_topology(spec, TopologyResolutionContextV1(profile="p-alpha"))
    codes = _codes(report)
    assert "TOPOLOGY_RELATION_UNSUPPORTED" in codes
    assert "TOPOLOGY_DEPENDENCY_CYCLE" not in codes, "the cycle collector is skipped"
    unsupported = [d for d in report.errors if d.code == "TOPOLOGY_RELATION_UNSUPPORTED"][0]
    assert "cannot call itself" in unsupported.remediation.lower()


def test_the_self_contradicting_case_has_a_reachable_remedy():
    """No authored value satisfies it, so the remedy cannot be an authored value."""
    findings = _classification_verdict((_AGREES, _PROD_AGREES), "TEST")
    other = _classification_verdict((_AGREES, _PROD_AGREES), "PROD")
    assert findings and other, "both authored values fire, as the domain is closed"
    assert findings[0].remediation == other[0].remediation
    assert "re-captur" in findings[0].remediation.lower()
