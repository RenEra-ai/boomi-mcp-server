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
