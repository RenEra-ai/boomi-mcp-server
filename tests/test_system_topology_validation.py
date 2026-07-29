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

#: ``has_process_ir`` is set for the process symbols because these fixtures
#: supply ``witness="process_ir"`` evidence for them. The planner now consumes
#: the flag — a ProcessIR witness for a planned process whose symbol declares no
#: ProcessIR root is a claim about nothing, and is gated.
_SYMBOLS = tuple(
    ComponentPlanSymbolV1(
        component_key=k, component_type=t, has_process_ir=(t == "process")
    )
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


def test_a_schedule_bound_twice_is_a_cardinality_violation():
    """The SHAPE is fine; there is simply one binding too many.

    Reporting it as an unsupported lifecycle sent a caller looking for a shape
    problem that is not there — the plan specifies the cardinality code at the
    later binding.
    """
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
    cardinality = [
        d for d in report.errors if d.code == "TOPOLOGY_SCHEMA_INVALID_CARDINALITY"
    ]
    assert cardinality, _codes(report)
    assert cardinality[0].path == "/relations/1/schedule"


def test_a_deployment_unit_bound_twice_is_a_cardinality_violation():
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
    assert "TOPOLOGY_SCHEMA_INVALID_CARDINALITY" in _codes(report)


def _superseded_test_a_schedule_bound_twice_is_unsupported():
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


def _superseded_test_a_deployment_unit_targeting_two_processes_is_unsupported():
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
    # Witness-level gating lives in the normative ``lifecycle`` phase, not
    # ``capability`` (which is kind-level) and not ``relation`` (shape rules).
    assert gated[0].phase == "lifecycle"
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
    """Same code, two phases — so each is independently testable and orderable.

    ``capability`` for a gated KIND, ``lifecycle`` for a supported kind missing
    its witness. ``lifecycle`` is in the plan's normative phase order and was
    previously unused, with witness failures folded into ``relation`` alongside
    the shape rules.
    """
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
    assert phases == {"capability", "lifecycle"}
    # Phase rank precedes path in the total order, so capability sorts first.
    ordered = [d.phase for d in report.errors if d.code == "TOPOLOGY_CAPABILITY_GATED"]
    assert ordered.index("capability") < ordered.index("lifecycle")


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
            ComponentPlanSymbolV1(
                component_key=f"k{i}", component_type="process", has_process_ir=True
            )
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
            ComponentPlanSymbolV1(
                component_key="ka", component_type="process", has_process_ir=True
            ),
            ComponentPlanSymbolV1(
                component_key="kb", component_type="process", has_process_ir=True
            ),
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
            ComponentPlanSymbolV1(
                component_key="ka", component_type="process", has_process_ir=True
            ),
            ComponentPlanSymbolV1(
                component_key="kb", component_type="process", has_process_ir=True
            ),
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


def _env_spec(classification="TEST", profile_ref="p-alpha"):
    return parse_system_topology_v1(
        {
            "version": "1",
            "profile_ref": profile_ref,
            "objects": [
                {"kind": "process", "key": "x", "component_ref": "$ref:k"},
                {
                    "kind": "environment",
                    "key": "e",
                    "environment_ref": "env-1",
                    "classification": classification,
                },
            ],
            "relations": [],
        }
    )


_K_SYMBOL = (ComponentPlanSymbolV1(component_key="k", component_type="process"),)


def _cause_context_profile():
    return _env_spec(), TopologyResolutionContextV1(
        profile="p-other", component_plan_symbols=_K_SYMBOL
    )


def _cause_snapshot_envelope_profile():
    return _env_spec(), TopologyResolutionContextV1(
        profile="p-alpha",
        component_plan_symbols=_K_SYMBOL,
        snapshot=_snapshot(profile="p-other"),
    )


def _cause_foreign_row_inside_snapshot():
    return _env_spec(), TopologyResolutionContextV1(
        profile="p-alpha",
        component_plan_symbols=_K_SYMBOL,
        snapshot=_snapshot(
            components=(
                ComponentFactV1(
                    profile="p-omega", component_id="c", component_type="process"
                ),
            ),
            environments=(_AGREES,),
        ),
    )


def _cause_unanimous_classification_mismatch():
    return _env_spec("TEST"), TopologyResolutionContextV1(
        profile="p-alpha",
        component_plan_symbols=_K_SYMBOL,
        snapshot=_snapshot(environments=(_PROD_AGREES,)),
    )


def _cause_self_contradicting_discovery():
    return _env_spec("TEST"), TopologyResolutionContextV1(
        profile="p-alpha",
        component_plan_symbols=_K_SYMBOL,
        snapshot=_snapshot(environments=(_AGREES, _PROD_AGREES)),
    )


#: One BEHAVIORAL case per cause: a context that actually provokes it, and the
#: phrase its remediation must contain.
#:
#: The previous version of this pin counted ``topology_finding`` CALL SITES via
#: the AST and compared that to the length of a hand-written cause list. That
#: equates syntax with meaning and gave false assurance: the final
#: environment-mismatch call site serves TWO logical causes — an authored value
#: disagreeing with unanimous discovery, and discovery disagreeing with itself —
#: so a four-entry list matched four call sites while the remediation had
#: dropped one of the five real causes, and the omitted one's offered action
#: was wrong for it. Provoking each cause is the only thing that grades the
#: text a caller in that situation actually receives.
_ENVIRONMENT_MISMATCH_CAUSES = (
    ("context profile", _cause_context_profile, "context names a different profile"),
    ("snapshot envelope profile", _cause_snapshot_envelope_profile, "snapshot envelope does"),
    ("foreign row inside snapshot", _cause_foreign_row_inside_snapshot, "inside the snapshot"),
    (
        "unanimous classification mismatch",
        _cause_unanimous_classification_mismatch,
        "update the authored classification",
    ),
    (
        "self-contradicting discovery",
        _cause_self_contradicting_discovery,
        "more than one classification",
    ),
)


@pytest.mark.parametrize(
    "label,build,phrase", _ENVIRONMENT_MISMATCH_CAUSES, ids=[c[0] for c in _ENVIRONMENT_MISMATCH_CAUSES]
)
def test_every_environment_mismatch_cause_is_described_in_its_remediation(
    label, build, phrase
):
    """QA #235 / R7. A remediation must describe every case it is served for.

    ``topology_finding`` attaches one static string to EVERY finding for a code,
    so a caller in an undescribed case is told to fix something they do not
    have — and for two of these five, the instruction the old text offered was
    actively wrong.
    """
    spec, ctx = build()
    findings = [
        d
        for d in validate_system_topology(spec, ctx).errors
        if d.code == "TOPOLOGY_ENVIRONMENT_MISMATCH"
    ]
    assert findings, f"{label} must actually provoke the code"
    assert phrase in findings[0].remediation.lower(), (label, phrase)


def test_each_environment_mismatch_cause_is_a_distinct_input():
    """The cases must not collapse — otherwise the parametrization is padding.

    Keyed on a fingerprint of the generated SPEC AND CONTEXT, deliberately
    excluding the label. Including it made the assertion vacuous: labels are
    unique by construction, so ``{(label, findings)}`` had one entry per case
    however identical the inputs were, and two builders returning the very same
    context passed.
    """
    from boomi_mcp.models.system_topology import canonical_system_topology_json

    fingerprints = []
    for _label, build, _phrase in _ENVIRONMENT_MISMATCH_CAUSES:
        spec, ctx = build()
        fingerprints.append(
            (canonical_system_topology_json(spec), ctx.model_dump_json())
        )
    assert len(set(fingerprints)) == len(_ENVIRONMENT_MISMATCH_CAUSES), [
        c[0] for c in _ENVIRONMENT_MISMATCH_CAUSES
    ]


def test_the_distinctness_check_would_notice_a_collapse():
    """Positive control for the test above, since its first version was vacuous."""
    from boomi_mcp.models.system_topology import canonical_system_topology_json

    duplicated = (
        _cause_context_profile,
        _cause_context_profile,
    )
    fingerprints = []
    for build in duplicated:
        spec, ctx = build()
        fingerprints.append(
            (canonical_system_topology_json(spec), ctx.model_dump_json())
        )
    assert len(set(fingerprints)) == 1, "identical builders must collapse to one key"


def _relation_unsupported_cases():
    """One spec per lifecycle rule the collector rejects."""
    base_objects = [
        {"kind": "process", "key": "p", "component_ref": "$ref:pk"},
        {"kind": "process", "key": "p2", "component_ref": "$ref:p2k"},
        {"kind": "api_service", "key": "a", "component_ref": "$ref:ak"},
        {"kind": "runtime", "key": "rt", "runtime_ref": "runtime-1"},
        {"kind": "environment", "key": "e", "environment_ref": "env-1"},
        {"kind": "schedule", "key": "s"},
        {"kind": "schedule", "key": "s2"},
        {"kind": "deployment_unit", "key": "u"},
    ]
    sched = {"kind": "schedule_binding", "key": "rs", "schedule": "s", "process": "p", "runtime": "rt"}
    sched2 = {"kind": "schedule_binding", "key": "rs2", "schedule": "s2", "process": "p2", "runtime": "rt"}
    dep = {
        "kind": "deployment_binding",
        "key": "rd",
        "deployment_unit": "u",
        "process": "p",
        "environment": "e",
    }
    return (
        (
            "scheduled listener",
            base_objects,
            [
                {"kind": "api_service_route", "key": "rr", "api_service": "a", "listener_process": "p"},
                sched,
                sched2,
                dep,
            ],
            "invoked by its api service",
        ),
        (
            "process self-call",
            base_objects,
            [
                {"kind": "process_call", "key": "rc", "caller_process": "p", "callee_process": "p"},
                sched,
                sched2,
                dep,
            ],
            "cannot call itself",
        ),
    )


@pytest.mark.parametrize(
    "label,objects,relations,phrase",
    _relation_unsupported_cases(),
    ids=[c[0] for c in _relation_unsupported_cases()],
)
def test_every_relation_unsupported_cause_is_described(label, objects, relations, phrase):
    """QA #236. Four lifecycle rules, one string that listed three."""
    spec = parse_system_topology_v1(
        {
            "version": "1",
            "profile_ref": "p-alpha",
            "objects": objects,
            "relations": relations,
        }
    )
    findings = [
        d
        for d in validate_system_topology(spec, _WITNESSED).errors
        if d.code == "TOPOLOGY_RELATION_UNSUPPORTED"
    ]
    assert findings, f"{label} must actually provoke the code"
    assert phrase in findings[0].remediation.lower(), (label, phrase)


def _gated_cases():
    process_objs = [
        {"kind": "process", "key": "p", "component_ref": "$ref:pk"},
        {"kind": "process", "key": "p2", "component_ref": "$ref:p2k"},
        {"kind": "api_service", "key": "a", "component_ref": "$ref:ak"},
        {"kind": "document_cache", "key": "c", "component_ref": "$ref:ck"},
    ]
    return (
        (
            "supported kind, no witness (ProcessCall)",
            process_objs,
            [{"kind": "process_call", "key": "r", "caller_process": "p", "callee_process": "p2"}],
            "processir node",
        ),
        (
            "supported kind, no witness (API route)",
            process_objs,
            [{"kind": "api_service_route", "key": "r", "api_service": "a", "listener_process": "p"}],
            "typed builder projection",
        ),
        (
            "gated kind",
            process_objs + [{"kind": "external_queue", "key": "q", "resource_ref": "queue-1"}],
            [{"kind": "queue_reference", "key": "r", "process": "p", "external_queue": "q"}],
            "kind itself is gated",
        ),
    )


@pytest.mark.parametrize(
    "label,objects,relations,phrase", _gated_cases(), ids=[c[0] for c in _gated_cases()]
)
def test_every_capability_gated_cause_is_described(label, objects, relations, phrase):
    """QA #237 / R7. One string, categorically different callers.

    An ``api_service_route`` cannot be witnessed by a ProcessIR node —
    ``ApiServiceRouteEvidenceV1`` does not permit it — so telling every planned
    relation to supply one cannot clear that supported case.
    """
    spec = parse_system_topology_v1(
        {
            "version": "1",
            "profile_ref": "p-alpha",
            "objects": objects,
            "relations": relations,
        }
    )
    bare = TopologyResolutionContextV1(
        profile="p-alpha", component_plan_symbols=_SYMBOLS
    )
    findings = [
        d
        for d in validate_system_topology(spec, bare).errors
        if d.code == "TOPOLOGY_CAPABILITY_GATED"
    ]
    assert findings, f"{label} must actually provoke the code"
    assert phrase in findings[0].remediation.lower(), (label, phrase)


def test_the_witness_a_remediation_names_is_the_one_the_model_accepts():
    """The instruction must be satisfiable by the type system, not just readable."""
    from typing import get_args

    from boomi_mcp.compiler.system_topology.context import (
        ApiServiceRouteEvidenceV1,
        ProcessCallEvidenceV1,
    )
    from boomi_mcp.compiler.system_topology.findings import _REMEDIATION

    text = _REMEDIATION["TOPOLOGY_CAPABILITY_GATED"].lower()
    assert "process_ir" in get_args(ProcessCallEvidenceV1.model_fields["witness"].annotation)
    assert "processir node" in text
    route_witnesses = get_args(ApiServiceRouteEvidenceV1.model_fields["witness"].annotation)
    assert "process_ir" not in route_witnesses
    assert "typed_builder" in route_witnesses
    assert "typed builder projection" in text


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
            ComponentPlanSymbolV1(
                component_key="pk", component_type="process", has_process_ir=True
            ),
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


def test_the_gated_remediation_names_a_discriminator_the_caller_actually_has():
    """QA #238. ``validate_system_topology`` returns no capability report.

    Its ``TopologyValidationReportV1`` has only the three severity buckets, and
    the package exports the capability report TYPE but no builder — so a caller
    on that path could neither receive nor construct the artifact the text told
    them to consult. ``phase`` is on every finding on both paths and is the real
    discriminator, which the design doc already documents.
    """
    from boomi_mcp.compiler.system_topology import TopologyValidationReportV1
    from boomi_mcp.compiler.system_topology.findings import _REMEDIATION

    text = _REMEDIATION["TOPOLOGY_CAPABILITY_GATED"].lower()
    assert "phase" in text
    assert "'capability'" in text and "'lifecycle'" in text
    # The named discriminator must be a field the caller actually holds...
    assert "capability_report" not in set(TopologyValidationReportV1.model_fields)

    # ...and it must genuinely discriminate the two cases.
    spec = parse_system_topology_v1(
        {
            "version": "1",
            "profile_ref": "p-alpha",
            "objects": [
                {"kind": "process", "key": "p", "component_ref": "$ref:pk"},
                {"kind": "process", "key": "p2", "component_ref": "$ref:p2k"},
                {"kind": "external_queue", "key": "q", "resource_ref": "queue-1"},
            ],
            "relations": [
                {"kind": "process_call", "key": "r1", "caller_process": "p", "callee_process": "p2"},
                {"kind": "queue_reference", "key": "r2", "process": "p", "external_queue": "q"},
            ],
        }
    )
    report = validate_system_topology(
        spec,
        TopologyResolutionContextV1(
            profile="p-alpha", component_plan_symbols=_SYMBOLS
        ),
    )
    gated = [d for d in report.errors if d.code == "TOPOLOGY_CAPABILITY_GATED"]
    phases = {d.subject: d.phase for d in gated}
    assert phases["external_queue"] == "capability", phases
    assert phases["process_call"] == "lifecycle", phases


# ---------------------------------------------------------------------------
# Architect review round 1 — plan-vs-code gaps the commit-review gate could not
# see, because it judges code on its own merits rather than against the plan
# ---------------------------------------------------------------------------


def test_document_rules_run_on_the_planner_not_only_the_parser():
    """A6-F4. Duplicate keys escaped the planner entirely.

    ``_check_document_rules`` was reachable only through
    ``parse_system_topology_v1``, so a caller who built the spec with
    ``model_validate`` — the ordinary way — and handed it to the planner got no
    duplicate-key error at all. That fails the issue's "duplicate ... errors are
    caught deterministically" criterion on the planner's own surface.
    """
    from boomi_mcp.models.system_topology import SystemTopologySpecV1

    spec = SystemTopologySpecV1.model_validate(
        {
            "version": "1",
            "profile_ref": "p-alpha",
            "objects": [
                {"kind": "process", "key": "dup", "component_ref": "$ref:k1"},
                {"kind": "process", "key": "dup", "component_ref": "$ref:k2"},
            ],
            "relations": [],
        }
    )
    ctx = TopologyResolutionContextV1(
        profile="p-alpha",
        component_plan_symbols=(
            ComponentPlanSymbolV1(component_key="k1", component_type="process"),
            ComponentPlanSymbolV1(component_key="k2", component_type="process"),
        ),
    )
    report = validate_system_topology(spec, ctx)
    duplicates = [d for d in report.errors if d.code == "TOPOLOGY_SCHEMA_DUPLICATE_KEY"]
    assert duplicates, _codes(report)
    assert duplicates[0].phase == "model"
    assert duplicates[0].path == "/objects/1/key"


def test_an_unbound_schedule_is_caught_on_the_planner_path_too():
    from boomi_mcp.models.system_topology import SystemTopologySpecV1

    spec = SystemTopologySpecV1.model_validate(
        {
            "version": "1",
            "profile_ref": "p-alpha",
            "objects": [
                {"kind": "process", "key": "p", "component_ref": "$ref:pk"},
                {"kind": "schedule", "key": "s"},
            ],
            "relations": [],
        }
    )
    report = validate_system_topology(
        spec, TopologyResolutionContextV1(profile="p-alpha")
    )
    assert "TOPOLOGY_SCHEMA_INVALID_CARDINALITY" in _codes(report)


def test_a_process_ir_witness_requires_a_symbol_that_declares_one():
    """A6-F1. ``has_process_ir`` was carried and consumed by nothing.

    A caller could label evidence ``witness="process_ir"`` for a planned process
    whose own ComponentPlan symbol says it has no ProcessIR root, and the
    relation planned clean — the label was trusted on its own word. There was
    nothing for the claim to be true OF.
    """
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
    evidence = (
        ProcessCallEvidenceV1(
            caller_component_ref="$ref:ka",
            callee_component_ref="$ref:kb",
            witness="process_ir",
        ),
    )
    without = TopologyResolutionContextV1(
        profile="p-alpha",
        component_plan_symbols=(
            ComponentPlanSymbolV1(component_key="ka", component_type="process"),
            ComponentPlanSymbolV1(component_key="kb", component_type="process"),
        ),
        process_call_evidence=evidence,
    )
    assert "TOPOLOGY_CAPABILITY_GATED" in _codes(validate_system_topology(spec, without))

    with_ir = TopologyResolutionContextV1(
        profile="p-alpha",
        component_plan_symbols=(
            ComponentPlanSymbolV1(
                component_key="ka", component_type="process", has_process_ir=True
            ),
            ComponentPlanSymbolV1(
                component_key="kb", component_type="process", has_process_ir=True
            ),
        ),
        process_call_evidence=evidence,
    )
    assert validate_system_topology(spec, with_ir).errors == ()


def test_per_fact_filtering_anchors_on_the_context_profile():
    """A6-F3. The envelope is itself caller-supplied.

    Filtering against ``snapshot.profile`` meant a snapshot stamped with the
    wrong profile kept every fact inside it — the one arrangement the filter
    exists to stop.
    """
    from boomi_mcp.compiler.system_topology.context import prepare_topology_context

    prepared = prepare_topology_context(
        TopologyResolutionContextV1(
            profile="p-alpha",
            snapshot=_snapshot(
                profile="p-omega",
                components=(
                    ComponentFactV1(
                        profile="p-omega", component_id="c", component_type="process"
                    ),
                ),
            ),
        )
    )
    # Anchored on the CONTEXT: the omega fact is foreign and is discarded.
    assert prepared.components == ()
    # ...but the CAPTURE is coherent — an omega row inside an omega envelope —
    # so it is not accused of being mixed on top of being the wrong account.
    assert prepared.foreign_profile_fact_count == 0


def test_a_foreign_snapshot_cannot_donate_its_observation_flag():
    """The envelope leg of ``environment_inventory_observed``, at its own level.

    Both current consumers independently require the profiles to agree, so
    dropping this leg is behaviourally invisible today — an equivalent mutant,
    verified as such. That is exactly why it is pinned HERE, on
    ``prepare_topology_context``, rather than through a consumer whose own gate
    masks it: the leg is what stops a THIRD consumer from inheriting a foreign
    account's claim to have observed this one's environments, and a guarantee
    graded only by proxy stops being graded the moment the proxy changes.
    """
    from boomi_mcp.compiler.system_topology.context import prepare_topology_context

    prepared = prepare_topology_context(
        TopologyResolutionContextV1(
            profile="p-alpha",
            snapshot=_snapshot(
                profile="p-omega", environment_inventory_observed=True
            ),
        )
    )
    assert prepared.environment_inventory_observed is False

    # ...and the same snapshot in its own account keeps the flag, so the
    # assertion above is the envelope check and not a constant.
    kept = prepare_topology_context(
        TopologyResolutionContextV1(
            profile="p-omega",
            snapshot=_snapshot(
                profile="p-omega", environment_inventory_observed=True
            ),
        )
    )
    assert kept.environment_inventory_observed is True


def test_an_empty_environment_inventory_still_witnesses_absence():
    """A6-F5b. ``list_environments`` is not paged; an OBSERVED empty result is a fact.

    Guarding the check on the id set being non-empty let an empty inventory wave
    every authored environment through, and its deployment binding with it.

    ``environment_inventory_observed=True`` is what makes the emptiness mean
    something: it says the listing answered. Without it an empty tuple is
    ambiguous, and the companion test below pins the other reading.
    """
    spec = parse_system_topology_v1(
        {
            "version": "1",
            "profile_ref": "p-alpha",
            "objects": [
                {"kind": "process", "key": "pr", "component_ref": "$ref:kp"},
                {"kind": "environment", "key": "e", "environment_ref": "ghost-env"},
                {"kind": "deployment_unit", "key": "u"},
            ],
            "relations": [
                {
                    "kind": "deployment_binding",
                    "key": "rd",
                    "deployment_unit": "u",
                    "process": "pr",
                    "environment": "e",
                }
            ],
        }
    )
    ctx = TopologyResolutionContextV1(
        profile="p-alpha",
        component_plan_symbols=(
            ComponentPlanSymbolV1(component_key="kp", component_type="process"),
        ),
        snapshot=_snapshot(
            environments=(),
            pagination=_complete("process"),
            environment_inventory_observed=True,
        ),
    )
    from boomi_mcp.compiler.system_topology import plan_system_topology

    plan = plan_system_topology(spec, ctx)
    assert "TOPOLOGY_REFERENCE_NOT_FOUND" in [b.code for b in plan.blockers]
    # ...and its binding is withdrawn with it.
    assert plan.planning_only_relations == ()


def test_an_unobserved_environment_listing_does_not_witness_absence():
    """QA #243. An outage is not an inventory.

    ``environments=()`` is produced BOTH by an account with no environments and
    by a ``list_environments`` call that failed. The first is conclusive; the
    second is nothing at all. Deriving the observation flag from the snapshot's
    ENVELOPE made every hand-built or failed capture claim the first reading,
    turning a transient outage into a confident ``TOPOLOGY_REFERENCE_NOT_FOUND``
    against every authored environment.

    The identical outage on a COMPONENT query is correctly left unjudged —
    ``_is_successful_listing`` guards it — so this is the environment path
    catching up to the control that already existed beside it.
    """
    spec = parse_system_topology_v1(
        {
            "version": "1",
            "profile_ref": "p-alpha",
            "objects": [
                {"kind": "process", "key": "pr", "component_ref": "$ref:kp"},
                {"kind": "environment", "key": "e", "environment_ref": "env-real"},
                {"kind": "deployment_unit", "key": "u"},
            ],
            "relations": [
                {
                    "kind": "deployment_binding",
                    "key": "rd",
                    "deployment_unit": "u",
                    "process": "pr",
                    "environment": "e",
                }
            ],
        }
    )
    ctx = TopologyResolutionContextV1(
        profile="p-alpha",
        component_plan_symbols=(
            ComponentPlanSymbolV1(component_key="kp", component_type="process"),
        ),
        # A snapshot whose envelope is perfectly in order — same profile, real
        # pagination — but whose environment listing never answered.
        snapshot=_snapshot(
            environments=(),
            pagination=_complete("process"),
            environment_inventory_observed=False,
        ),
    )
    from boomi_mcp.compiler.system_topology import plan_system_topology

    plan = plan_system_topology(spec, ctx)
    assert [b.code for b in plan.blockers] == []
    # Unjudged, not waved through as verified: the binding is still planned,
    # but nothing in the plan claims the environment was confirmed to exist.
    assert [r.relation_key for r in plan.planning_only_relations] == ["rd"]


def test_the_environment_observation_flag_requires_a_real_listing():
    """QA #243, at the boundary. The flag is earned by capture, never assumed."""
    from boomi_mcp.compiler.system_topology.discovery import (
        capture_topology_discovery_snapshot,
    )

    spec = parse_system_topology_v1(
        {
            "version": "1",
            "profile_ref": "prof-a",
            "objects": [
                {"kind": "environment", "key": "e", "environment_ref": "env-1"}
            ],
        }
    )

    class _Port:
        def __init__(self, envelope):
            self._envelope = envelope

        def list_profiles(self):
            return ("prof-a",)

        def query_components(self, profile, component_type):
            return {"components": []}

        def read_component_xml(self, profile, component_ref):
            return None

        def read_component_dependencies(self, profile, component_ref):
            return []

        def list_environments(self, profile):
            return self._envelope

        def list_schedules(self, profile):
            return ()

        def list_deployments(self, profile):
            return ()

    def _capture(envelope):
        return capture_topology_discovery_snapshot(
            spec,
            _Port(envelope),
            captured_at="2026-01-01T00:00:00Z",
            source_revision="rev",
            service_release="rel",
        )

    answered = _capture({"environments": [{"id": "env-1", "classification": "TEST"}]})
    assert answered.environment_inventory_observed is True
    assert len(answered.environments) == 1

    empty_but_answered = _capture({"environments": []})
    assert empty_but_answered.environment_inventory_observed is True
    assert empty_but_answered.environments == ()

    # A tuple of rows answers just as well as a list. The type test replaced a
    # membership test to reject null and strings, not to start caring which
    # sequence an adapter hands back — narrowing it to ``list`` alone would fail
    # shut on a perfectly good listing.
    as_tuple = _capture({"environments": ({"id": "env-1"},)})
    assert as_tuple.environment_inventory_observed is True
    assert len(as_tuple.environments) == 1

    # Five ways a listing can fail to answer. None of them may claim otherwise,
    # and none may let rows out of a failed envelope.
    #
    # The last two are QA #245: ``key in payload`` is a MEMBERSHIP test, so a
    # null result read as an observed empty inventory — the exact confident
    # NOT_FOUND the flag exists to prevent — and a string result reached the row
    # walk and raised ``AttributeError`` out of a pure function.
    for failed in (
        {"_success": False, "environments": [{"id": "env-1"}]},
        {"error": "timeout", "environments": [{"id": "env-1"}]},
        {"message": "no environments key"},
        {"_success": True, "environments": None},
        {"_success": True, "environments": "env-1"},
    ):
        snapshot = _capture(failed)
        assert snapshot.environment_inventory_observed is False
        assert snapshot.environments == ()


def test_a_null_component_listing_is_not_an_observed_empty_page():
    """QA #245 on the sibling path. One check guards both; both are pinned."""
    from boomi_mcp.compiler.system_topology.discovery import (
        capture_topology_discovery_snapshot,
    )

    spec = parse_system_topology_v1(
        {
            "version": "1",
            "profile_ref": "prof-a",
            "objects": [{"kind": "process", "key": "p", "component_ref": "lit-1"}],
        }
    )

    class _Port:
        def list_profiles(self):
            return ("prof-a",)

        def query_components(self, profile, component_type):
            return {"_success": True, "components": None}

        def read_component_xml(self, profile, component_ref):
            return None

        def read_component_dependencies(self, profile, component_ref):
            return []

        def list_environments(self, profile):
            return {"environments": []}

        def list_schedules(self, profile):
            return ()

        def list_deployments(self, profile):
            return ()

    snapshot = capture_topology_discovery_snapshot(
        spec,
        _Port(),
        captured_at="2026-01-01T00:00:00Z",
        source_revision="rev",
        service_release="rel",
    )
    # Not one page may claim to have been observed, so no component type is
    # complete and the literal id is left unjudged rather than declared missing.
    assert [page.observed for page in snapshot.pagination] == [False] * 5
    ctx = TopologyResolutionContextV1(profile="prof-a", snapshot=snapshot)
    assert "TOPOLOGY_REFERENCE_NOT_FOUND" not in _codes(
        validate_system_topology(spec, ctx)
    )


def test_an_unobserved_environment_listing_is_announced_not_merely_survived():
    """QA #244. The new third state had no published trace.

    ``observed`` / ``observed-empty`` / ``did-not-answer`` are three different
    situations, and the third showed up only as one absent row in
    ``resolved_references`` — ``is_valid`` true, no blocker, no warning, no
    string anywhere naming it. The component path already refuses that silence
    via ``discovery_unobserved_query``; the environment listing gets the same
    treatment under its own subject, because the component decision's text is
    about queue listings and is not the action to take here.
    """
    from boomi_mcp.compiler.system_topology import plan_system_topology

    spec = parse_system_topology_v1(
        {
            "version": "1",
            "profile_ref": "p-alpha",
            "objects": [
                {"kind": "environment", "key": "e", "environment_ref": "env-1"}
            ],
            "relations": [],
        }
    )
    plan = plan_system_topology(
        spec,
        TopologyResolutionContextV1(
            profile="p-alpha",
            # A usable, same-account snapshot whose environment listing did not
            # answer. Nothing else about it is wrong.
            snapshot=_snapshot(
                profile="p-alpha", environment_inventory_observed=False
            ),
        ),
    )
    assert plan.blockers == (), [b.code for b in plan.blockers]
    assert "environment_inventory_unobserved" in {
        d.subject for d in plan.unresolved_decisions
    }
    # And an observed listing says nothing, so the notice stays meaningful.
    quiet = plan_system_topology(
        spec,
        TopologyResolutionContextV1(
            profile="p-alpha",
            snapshot=_snapshot(
                profile="p-alpha",
                environments=(),
                environment_inventory_observed=True,
            ),
        ),
    )
    assert "environment_inventory_unobserved" not in {
        d.subject for d in quiet.unresolved_decisions
    }
    # And a plan with NO snapshot says nothing about an environment listing that
    # was never attempted — that case is already `live_revalidation`'s, and
    # stacking a second notice on it would tell the caller to re-run a discovery
    # they have not run once. This pins the notice's placement inside the
    # snapshot-usable branch, not merely its condition.
    none = plan_system_topology(
        spec, TopologyResolutionContextV1(profile="p-alpha")
    )
    subjects = {d.subject for d in none.unresolved_decisions}
    assert "live_revalidation" in subjects
    assert "environment_inventory_unobserved" not in subjects, subjects


def test_the_unobserved_notice_never_contradicts_a_resolved_environment():
    """The notice claims less than the plan knows, not more.

    A snapshot assembled by hand — or by an adapter written before
    ``list_environments`` returned an envelope — can carry environment rows and
    still claim no observation. Those rows resolve, and correctly so: a PRESENT
    row is positive evidence that something saw it, and withdrawing it would
    make a real environment unresolvable to protect a claim about absence. So
    the notice must be scoped to absence, like its component sibling. Saying
    "no environment reference was judged against it" put a false sentence
    beside a `platform_resource` resolution in the same plan.
    """
    from boomi_mcp.compiler.system_topology import plan_system_topology
    from boomi_mcp.compiler.system_topology.context import EnvironmentFactV1

    spec = parse_system_topology_v1(
        {
            "version": "1",
            "profile_ref": "p-alpha",
            "objects": [
                {"kind": "environment", "key": "e", "environment_ref": "env-1"},
                {"kind": "environment", "key": "g", "environment_ref": "ghost"},
            ],
            "relations": [],
        }
    )
    plan = plan_system_topology(
        spec,
        TopologyResolutionContextV1(
            profile="p-alpha",
            snapshot=_snapshot(
                profile="p-alpha",
                environments=(
                    EnvironmentFactV1(profile="p-alpha", environment_id="env-1"),
                ),
                # Rows present, observation NOT claimed.
                environment_inventory_observed=False,
            ),
        ),
    )
    # The present row resolves...
    assert [
        (r.object_key, r.resolution) for r in plan.resolved_references
    ] == [("e", "platform_resource")]
    # ...the absent one is left unjudged rather than reported not-found...
    assert plan.blockers == (), [b.code for b in plan.blockers]
    # ...and the notice is there, saying only what is true.
    notice = next(
        d
        for d in plan.unresolved_decisions
        if d.subject == "environment_inventory_unobserved"
    )
    assert "absence" in notice.question.lower()
    assert _overclaiming_decisions(plan, snapshot_supplied=True) == []


# --- the overclaim guard, and its controls -------------------------------

#: A published decision may not assert a UNIVERSAL NEGATIVE about what was
#: judged while the same plan publishes rows that resolved. Matching the CLASS
#: rather than one retracted sentence: an identity guard that forbids one string
#: and requires one word passes any reworded version of the same overclaim, and
#: rewording is exactly how this defect keeps coming back.
#: Claims about what the plan JUDGED. False once anything resolved.
_JUDGEMENT_OVERCLAIMS = (
    # "no reference was judged", "nothing was checked", "none were verified"
    r"\b(no|none|nothing|neither)\b[^.]{0,80}"
    r"\b(judged|checked|verified|examined|resolved|considered)\b",
    # "presence and absence are both unknown" — a both-directions claim, where
    # only the absence direction is actually unsupported.
    r"\bpresence and absence\b",
    # "cannot say whether X exists" — same both-directions overreach.
    r"cannot say whether\b[^.]{0,60}\bexist",
)

#: Claims about whether a snapshot EXISTS. False once one was supplied — even
#: when it was then refused for naming another account. A separate precondition
#: on purpose: this class contradicts the blocker text, not the resolution
#: table, and the shapes that trigger it resolve nothing at all. Folding it in
#: under the judgement precondition made it unreachable, which is how a guard
#: comes to forbid a sentence it can never see.
_SNAPSHOT_EXISTENCE_OVERCLAIMS = (r"\bwithout a live discovery snapshot\b",)


def _normalized_question(text):
    import re as _re

    return _re.sub(r"\s+", " ", text).strip().lower()


def _overclaiming_decisions(plan, *, snapshot_supplied=False):
    """Decisions whose text asserts more than the plan itself supports.

    Runs against the plan, so the controls below drive this same function —
    a guard graded only by its ingredients is a guard nothing grades.
    """
    import re as _re

    active = []
    if plan.resolved_references:
        active.extend(_JUDGEMENT_OVERCLAIMS)
    if snapshot_supplied:
        active.extend(_SNAPSHOT_EXISTENCE_OVERCLAIMS)
    offenders = []
    for decision in plan.unresolved_decisions:
        question = _normalized_question(decision.question)
        for pattern in active:
            if _re.search(pattern, question):
                offenders.append((decision.subject, pattern))
    return offenders


def test_a_foreign_context_cannot_change_which_findings_are_reported():
    """§6 R4-F1. The profile gate reached the buckets and not the collectors.

    A context for another account was still consulted by reference resolution,
    witness gating and guidance derivation, so what the WRONG account happened
    to contain decided which findings appeared: an omega context carrying the
    symbols and the ProcessCall witness silently removed alpha's two
    ``TOPOLOGY_REFERENCE_NOT_FOUND`` findings and its
    ``TOPOLOGY_CAPABILITY_GATED``, and published dependency guidance besides.

    Under a mismatch the only honest report is the mismatch itself. Judging
    everything as not-found instead would over-claim absence from evidence that
    was never about this account — the rule the delta established for snapshots,
    applied to the context.
    """
    from boomi_mcp.compiler.system_topology import plan_system_topology
    from boomi_mcp.compiler.system_topology.context import (
        DependencyCorroborationV1,
        ProcessCallEvidenceV1,
    )

    spec = parse_system_topology_v1(
        {
            "version": "1",
            "profile_ref": "p-alpha",
            "objects": [
                {"kind": "process", "key": "a", "component_ref": "$ref:ka"},
                {"kind": "process", "key": "b", "component_ref": "$ref:kb"},
            ],
            "relations": [
                {
                    "kind": "process_call",
                    "key": "r",
                    "caller_process": "a",
                    "callee_process": "b",
                }
            ],
        }
    )
    symbols = (
        ComponentPlanSymbolV1(
            component_key="ka", component_type="process", has_process_ir=True
        ),
        ComponentPlanSymbolV1(
            component_key="kb", component_type="process", has_process_ir=True
        ),
    )
    witness = ProcessCallEvidenceV1(
        caller_component_ref="$ref:ka",
        callee_component_ref="$ref:kb",
        witness="process_ir",
    )
    bare = TopologyResolutionContextV1(profile="p-omega")
    loaded = TopologyResolutionContextV1(
        profile="p-omega",
        component_plan_symbols=symbols,
        process_call_evidence=(witness,),
        dependency_corroboration=(
            DependencyCorroborationV1(
                parent_component_ref="$ref:ka",
                child_component_ref="$ref:kb",
                child_component_type="process",
            ),
        ),
    )
    reports = [_codes(validate_system_topology(spec, c)) for c in (bare, loaded)]
    # Identical, and containing only the mismatch: what the foreign account
    # holds may not change this plan's verdict in either direction.
    assert reports[0] == reports[1] == ["TOPOLOGY_ENVIRONMENT_MISMATCH"], reports
    for context in (bare, loaded):
        plan = plan_system_topology(spec, context)
        assert plan.guidance == (), [g.subject for g in plan.guidance]
        assert plan.planning_only_relations == ()
        assert plan.executable_component_prerequisites == ()

    # The control: the same evidence in the AUTHORED account still does its job,
    # so the silence above is the profile gate and not a lost rule.
    same = plan_system_topology(
        spec,
        TopologyResolutionContextV1(
            profile="p-alpha",
            component_plan_symbols=symbols,
            process_call_evidence=(witness,),
        ),
    )
    assert same.blockers == (), [b.code for b in same.blockers]
    assert [r.relation_key for r in same.planning_only_relations] == ["r"]


def test_the_reference_gate_answers_to_the_context_not_the_snapshot():
    """QA #277. ``same_account`` is a CONJUNCTION, and this loop is not.

    Gating the object loop on ``same_account`` — context AND snapshot envelope —
    silenced it whenever merely the SNAPSHOT was foreign, even though the
    ``$ref`` branch reads ``prepared.symbols``, which arrives on the context and
    is qualified by the context's profile. A plan whose context matched the spec
    exactly then lost a real ``TOPOLOGY_REFERENCE_TYPE_MISMATCH``, planned a
    relation through the blocked endpoint, and emitted a prerequisite telling a
    consumer to build a ``documentcache`` for an object declared a ``process``.

    The invariant checker certified it, because it re-derives suppression from
    the plan's own blockers — and the blocker was the thing that had vanished.
    Two gates agreeing on the wrong answer is why this is pinned as a table:
    only the middle row moves, and only against the fix.
    """
    from boomi_mcp.compiler.system_topology import plan_system_topology
    from boomi_mcp.compiler.system_topology.context import ProcessCallEvidenceV1

    spec = parse_system_topology_v1(
        {
            "version": "1",
            "profile_ref": "p-a",
            "objects": [
                {"kind": "process", "key": "a", "component_ref": "$ref:ka"},
                {"kind": "process", "key": "x", "component_ref": "$ref:kx"},
            ],
            "relations": [
                {
                    "kind": "process_call",
                    "key": "call",
                    "caller_process": "a",
                    "callee_process": "x",
                }
            ],
        }
    )
    symbols = (
        ComponentPlanSymbolV1(
            component_key="ka", component_type="process", has_process_ir=True
        ),
        # Deliberately the WRONG type for the object kind that names it.
        ComponentPlanSymbolV1(
            component_key="kx", component_type="documentcache", has_process_ir=True
        ),
    )
    witness = (
        ProcessCallEvidenceV1(
            caller_component_ref="$ref:ka",
            callee_component_ref="$ref:kx",
            witness="process_ir",
        ),
    )

    def _plan(ctx_profile, snapshot_profile):
        return plan_system_topology(
            spec,
            TopologyResolutionContextV1(
                profile=ctx_profile,
                component_plan_symbols=symbols,
                process_call_evidence=witness,
                snapshot=_snapshot(profile=snapshot_profile),
            ),
        )

    mismatch = "TOPOLOGY_ENVIRONMENT_MISMATCH"
    type_mismatch = "TOPOLOGY_REFERENCE_TYPE_MISMATCH"

    # ctx and snapshot both this account: the type mismatch is reported.
    agreed = _plan("p-a", "p-a")
    assert [b.code for b in agreed.blockers] == [type_mismatch]

    # Only the SNAPSHOT is foreign. The symbols are still this account's, so the
    # type mismatch is STILL reported — beside the envelope mismatch.
    snapshot_only = _plan("p-a", "p-omega")
    assert sorted(b.code for b in snapshot_only.blockers) == sorted(
        [mismatch, type_mismatch]
    )
    assert snapshot_only.planning_only_relations == ()
    assert [p.component_key for p in snapshot_only.executable_component_prerequisites] == [
        "ka"
    ]

    # The CONTEXT is foreign: nothing is judged from it, in either arrangement.
    for snapshot_profile in ("p-omega", "p-a"):
        foreign = _plan("p-omega", snapshot_profile)
        assert [b.code for b in foreign.blockers] == [mismatch], snapshot_profile
        assert foreign.planning_only_relations == ()
        assert foreign.executable_component_prerequisites == ()


def test_the_relation_bucket_invariant_holds_when_blockers_are_not_relation_local():
    """§6 R4-F2. The checker failed open in exactly the states worth checking.

    Completeness was gated on the plan having NO blockers, so one unrelated
    gated queue disabled it entirely; and permissibility read only
    ``/relations/N`` paths, so a relation withdrawn because an endpoint object
    is blocked — or because the context names another account — could be
    injected straight back. Both directions, in three states.
    """
    from boomi_mcp.compiler.system_topology import plan_system_topology
    from boomi_mcp.compiler.system_topology.context import (
        ComponentFactV1,
        DiscoveryPageProvenanceV1,
        ProcessCallEvidenceV1,
        prepare_topology_context,
    )
    from boomi_mcp.compiler.system_topology.contracts import (
        ComponentPlanPrerequisiteV1,
        PlannedTopologyRelationV1,
    )
    from boomi_mcp.compiler.system_topology.invariants import (
        TopologyPlanningInvariantError,
        check_topology_plan_invariants,
    )

    def _rejects(plan, context, spec, **update):
        with pytest.raises(TopologyPlanningInvariantError):
            check_topology_plan_invariants(
                plan.model_copy(update=update),
                prepare_topology_context(context),
                spec,
            )

    # 1. A valid witnessed call beside an UNRELATED gated-queue blocker. The
    #    call is planned; dropping it must not be accepted just because some
    #    other object blocked.
    gated = parse_system_topology_v1(
        {
            "version": "1",
            "profile_ref": "p-a",
            "objects": [
                {"kind": "process", "key": "a", "component_ref": "$ref:ka"},
                {"kind": "process", "key": "b", "component_ref": "$ref:kb"},
                {"kind": "external_queue", "key": "q", "resource_ref": "qr"},
            ],
            "relations": [
                {
                    "kind": "process_call",
                    "key": "r",
                    "caller_process": "a",
                    "callee_process": "b",
                },
                {
                    "kind": "queue_reference",
                    "key": "rq",
                    "process": "a",
                    "external_queue": "q",
                },
            ],
        }
    )
    gated_ctx = TopologyResolutionContextV1(
        profile="p-a",
        component_plan_symbols=(
            ComponentPlanSymbolV1(
                component_key="ka", component_type="process", has_process_ir=True
            ),
            ComponentPlanSymbolV1(
                component_key="kb", component_type="process", has_process_ir=True
            ),
        ),
        process_call_evidence=(
            ProcessCallEvidenceV1(
                caller_component_ref="$ref:ka",
                callee_component_ref="$ref:kb",
                witness="process_ir",
            ),
        ),
    )
    gated_plan = plan_system_topology(gated, gated_ctx)
    assert [r.relation_key for r in gated_plan.planning_only_relations] == ["r"]
    assert {b.code for b in gated_plan.blockers} == {"TOPOLOGY_CAPABILITY_GATED"}
    _rejects(gated_plan, gated_ctx, gated, planning_only_relations=())

    # 2. A relation withdrawn ONLY because an endpoint object is blocked, under
    #    a path the old scan never read.
    endpoint = parse_system_topology_v1(
        {
            "version": "1",
            "profile_ref": "p-a",
            "objects": [
                {"kind": "process", "key": "a", "component_ref": "lit-a"},
                {"kind": "process", "key": "b", "component_ref": "lit-b"},
            ],
            "relations": [
                {
                    "kind": "process_call",
                    "key": "r",
                    "caller_process": "a",
                    "callee_process": "b",
                }
            ],
        }
    )
    endpoint_ctx = TopologyResolutionContextV1(
        profile="p-a",
        snapshot=_snapshot(
            profile="p-a",
            components=(
                ComponentFactV1(
                    profile="p-a", component_id="lit-a", component_type="process"
                ),
                ComponentFactV1(
                    profile="p-a",
                    component_id="lit-b",
                    component_type="documentcache",
                ),
            ),
            pagination=(
                DiscoveryPageProvenanceV1(
                    component_type="process", returned_count=2, total_available=2
                ),
            ),
        ),
        process_call_evidence=(
            ProcessCallEvidenceV1(
                caller_component_ref="lit-a",
                callee_component_ref="lit-b",
                witness="component_xml",
            ),
        ),
    )
    endpoint_plan = plan_system_topology(endpoint, endpoint_ctx)
    assert [b.path for b in endpoint_plan.blockers] == ["/objects/1/component_ref"]
    assert endpoint_plan.planning_only_relations == ()
    _rejects(
        endpoint_plan,
        endpoint_ctx,
        endpoint,
        planning_only_relations=(
            PlannedTopologyRelationV1(
                relation_key="r", relation_kind="process_call", witness="component_xml"
            ),
        ),
    )

    # 3. A context-profile mismatch: neither a relation nor a prerequisite may
    #    be injected back into a plan whose only blocker is the mismatch.
    foreign_ctx = TopologyResolutionContextV1(
        profile="p-omega",
        component_plan_symbols=(
            ComponentPlanSymbolV1(
                component_key="ka", component_type="process", has_process_ir=True
            ),
        ),
    )
    foreign_plan = plan_system_topology(endpoint, foreign_ctx)
    assert [b.code for b in foreign_plan.blockers] == ["TOPOLOGY_ENVIRONMENT_MISMATCH"]
    _rejects(
        foreign_plan,
        foreign_ctx,
        endpoint,
        planning_only_relations=(
            PlannedTopologyRelationV1(
                relation_key="r", relation_kind="process_call", witness="process_ir"
            ),
        ),
    )
    _rejects(
        foreign_plan,
        foreign_ctx,
        endpoint,
        executable_component_prerequisites=(
            ComponentPlanPrerequisiteV1(component_key="ka", component_type="process"),
        ),
    )


def test_the_pagination_notice_does_not_speak_for_a_non_component_blocker():
    """QA #250. Pagination witnesses components; it was talking about all absence.

    A plan can block on ``/objects/N/environment_ref`` — absence judged from an
    OBSERVED environment listing, which pagination has nothing to do with —
    while a truncated COMPONENT page publishes "absence from this snapshot is
    not evidence of absence in the account. Page through fully before treating
    a not-found reference as real." Against that blocker the remedy is inert:
    paging fully retires the notice and leaves the blocker untouched.
    """
    from boomi_mcp.compiler.system_topology import plan_system_topology
    from boomi_mcp.compiler.system_topology.context import (
        ComponentFactV1,
        DiscoveryPageProvenanceV1,
        EnvironmentFactV1,
    )

    spec = parse_system_topology_v1(
        {
            "version": "1",
            "profile_ref": "p-alpha",
            "objects": [
                {"kind": "process", "key": "p", "component_ref": "lit-1"},
                {"kind": "environment", "key": "e", "environment_ref": "ghost"},
            ],
            "relations": [],
        }
    )
    plan = plan_system_topology(
        spec,
        TopologyResolutionContextV1(
            profile="p-alpha",
            snapshot=_snapshot(
                profile="p-alpha",
                components=(
                    ComponentFactV1(
                        profile="p-alpha",
                        component_id="lit-1",
                        component_type="process",
                    ),
                ),
                environments=(
                    EnvironmentFactV1(profile="p-alpha", environment_id="real"),
                ),
                environment_inventory_observed=True,
                pagination=(
                    DiscoveryPageProvenanceV1(
                        component_type="process",
                        returned_count=1,
                        total_available=9,
                        has_more=True,
                    ),
                ),
            ),
        ),
    )
    # The two coexist — the environment blocker is real, the truncation is real.
    assert [b.path for b in plan.blockers] == ["/objects/1/environment_ref"]
    notice = next(
        d for d in plan.unresolved_decisions if d.subject == "discovery_pagination"
    )
    question = _normalized_question(notice.question)
    # ...and the notice says which references it speaks for, so its remedy is
    # not read as applying to a blocker paging cannot retire.
    assert "environment" in question and "runtime" in question
    # QA #252. Scoping to COMPONENT references was not enough, and a guard that
    # only checked vocabulary passed a strictly WORSE sentence. No published
    # component not-found is a paging artifact either: a literal id is reported
    # missing only when its type is in ``complete`` — observed and untruncated —
    # and a ``$ref`` comes from the symbol table. Paging is anti-monotone, so
    # the notice may not offer it as a way to retire ANY blocker, and must say
    # what truncation actually costs: coverage.
    assert "unjudged" in question
    assert "coverage" in question
    assert "before treating a not-found" not in question
    # QA #257/#261/#262. Two successive attempts to say what a re-run does to
    # existing findings were both false of the planner, so the notice no longer
    # says anything about it. "Paging can only add findings" is refuted by
    # ``_collect`` skipping the dependency phase once a reference finding
    # exists; "a not-found came from a complete listing" is refuted by
    # ``component_ids`` being keyed by id across every type. A coverage notice
    # may not predict a re-run.
    for forbidden in (
        "can only add findings",
        "will not retire",
        "never remove one",
        "came from a complete listing",
    ):
        assert forbidden not in question, forbidden


def test_the_two_reference_remediations_name_the_table_that_fixes_them():
    """QA #258/#259. A remedy pointing at the wrong table does not terminate.

    A ``$ref`` not-found is judged against the ComponentPlan symbol table, so
    "declare the referenced object in this document" was advice about a document
    where the object is already declared, and "supply the component fact" was
    about a listing the ``$ref`` never consults. A type mismatch reported at
    ``/objects/N/component_ref`` involves no relation role at all, so the
    endpoint matrix answered nothing.
    """
    from boomi_mcp.compiler.system_topology.findings import topology_finding

    not_found = topology_finding(
        "TOPOLOGY_REFERENCE_NOT_FOUND",
        severity="error",
        phase="reference",
        path="/objects/0/component_ref",
    ).remediation.lower()
    # QA #263: the first correction deleted the one arm that was already right.
    # This code has FOUR emit sites across THREE fix locations, and the
    # relation-role arm is judged against this document's objects — which the
    # replacement text explicitly denied.
    assert "/relations/n/role" in not_found
    assert "declare that object here" in not_found
    assert "componentplan" in not_found
    assert "literal id" in not_found and "credential profile" in not_found
    # And the wrong-table advice may not come back ALONGSIDE the right advice:
    # the object referenced by a failing '$ref' is already declared here, so
    # "declare it in this document" is a step that changes nothing.
    assert "declare the referenced object in this document" not in not_found

    mismatch = topology_finding(
        "TOPOLOGY_REFERENCE_TYPE_MISMATCH",
        severity="error",
        phase="reference",
        path="/objects/0/component_ref",
    ).remediation.lower()
    # Both arms, since one code serves both paths.
    assert "on an object" in mismatch and "on a relation" in mismatch
    assert "endpoint matrix" in mismatch


def test_paging_can_remove_a_finding_which_is_why_nothing_claims_otherwise():
    """QA #261/#262, pinned as BEHAVIOUR so the retracted claim cannot return.

    Two counterexamples, each in a cell the earlier paging pins excluded by
    construction — they used zero relations, one component type, and compared
    ``{path}`` rather than ``{(code, path)}``.
    """
    from boomi_mcp.compiler.system_topology import plan_system_topology
    from boomi_mcp.compiler.system_topology.context import (
        ComponentFactV1,
        DiscoveryPageProvenanceV1,
    )

    def _pages(**by_type):
        return tuple(
            DiscoveryPageProvenanceV1(
                component_type=name,
                returned_count=1,
                total_available=9 if truncated else 1,
                has_more=truncated,
            )
            for name, truncated in by_type.items()
        )

    def _findings(spec, facts, pages):
        return {
            (b.code, b.path)
            for b in plan_system_topology(
                spec,
                TopologyResolutionContextV1(
                    profile="p-alpha",
                    snapshot=_snapshot(
                        profile="p-alpha", components=facts, pagination=pages
                    ),
                ),
            ).blockers
        }

    # 1. A reference finding SUPPRESSES the dependency phase, so revealing one
    #    by paging removes an already-reported cycle.
    cyclic = parse_system_topology_v1(
        {
            "version": "1",
            "profile_ref": "p-alpha",
            "objects": [
                {"kind": "process", "key": "a", "component_ref": "id-a"},
                {"kind": "process", "key": "b", "component_ref": "id-b"},
                {"kind": "process", "key": "c", "component_ref": "ghost"},
            ],
            "relations": [
                {
                    "kind": "process_call",
                    "key": "r1",
                    "caller_process": "a",
                    "callee_process": "b",
                },
                {
                    "kind": "process_call",
                    "key": "r2",
                    "caller_process": "b",
                    "callee_process": "a",
                },
            ],
        }
    )
    facts = (
        ComponentFactV1(
            profile="p-alpha", component_id="id-a", component_type="process"
        ),
        ComponentFactV1(
            profile="p-alpha", component_id="id-b", component_type="process"
        ),
    )
    truncated = _findings(cyclic, facts, _pages(process=True))
    paged = _findings(cyclic, facts, _pages(process=False))
    assert any(code == "TOPOLOGY_DEPENDENCY_CYCLE" for code, _ in truncated)
    assert not any(code == "TOPOLOGY_DEPENDENCY_CYCLE" for code, _ in paged)

    # 2. ``component_ids`` is keyed by id across ALL types, so paging a
    #    DIFFERENT type turns a not-found into a type mismatch and retires it.
    mistyped = parse_system_topology_v1(
        {
            "version": "1",
            "profile_ref": "p-alpha",
            "objects": [
                {"kind": "document_cache", "key": "d", "component_ref": "id-a"}
            ],
            "relations": [],
        }
    )
    before = _findings(mistyped, (), _pages(documentcache=False, process=True))
    after = _findings(mistyped, facts[:1], _pages(documentcache=False, process=False))
    assert ("TOPOLOGY_REFERENCE_NOT_FOUND", "/objects/0/component_ref") in before
    assert ("TOPOLOGY_REFERENCE_NOT_FOUND", "/objects/0/component_ref") not in after
    assert ("TOPOLOGY_REFERENCE_TYPE_MISMATCH", "/objects/0/component_ref") in after


def test_a_type_is_complete_only_when_every_one_of_its_pages_is():
    """QA #256. The completeness set was an EXISTENTIAL over pages.

    Two pages normalizing to one type — a raw duplicate, or any pair from the
    alias set ``_normalize_component_type`` collapses — put that type in
    ``complete_component_types`` as long as ONE of them was untruncated. Absence
    was then conclusive from a demonstrably partial listing, and paging through
    REMOVED the resulting not-found: exactly the outcome the field promises
    cannot happen, and the one the pagination notice now tells callers to expect.
    """
    from boomi_mcp.compiler.system_topology.context import (
        DiscoveryPageProvenanceV1,
        prepare_topology_context,
    )

    whole = DiscoveryPageProvenanceV1(
        component_type="process", returned_count=1, total_available=1
    )
    partial = DiscoveryPageProvenanceV1(
        component_type="process", returned_count=1, total_available=9, has_more=True
    )
    unanswered = DiscoveryPageProvenanceV1(
        component_type="process", returned_count=0, observed=False
    )

    def _complete(*pages):
        return prepare_topology_context(
            TopologyResolutionContextV1(
                profile="p-alpha",
                snapshot=_snapshot(profile="p-alpha", pagination=pages),
            )
        ).complete_component_types

    assert _complete(whole) == ("process",)
    # One partial page anywhere disqualifies the type, in either order...
    assert _complete(whole, partial) == ()
    assert _complete(partial, whole) == ()
    # ...and an unanswered page does too.
    assert _complete(whole, unanswered) == ()
    # The alias route, which is how this is reached without a duplicate: both
    # names normalize to one type, so one truncated page poisons the other.
    assert _complete(
        DiscoveryPageProvenanceV1(
            component_type="webservice", returned_count=1, total_available=1
        ),
        DiscoveryPageProvenanceV1(
            component_type="Webservice",
            returned_count=1,
            total_available=9,
            has_more=True,
        ),
    ) == ()


def test_a_partially_paged_type_claims_nothing_until_it_is_paged():
    """QA #256's behavioural half, in the shape the old pin could not reach.

    ``test_paging_fully_never_retires_a_component_blocker`` passes a single page
    per type, so it graded the claim only where the bug could not appear.
    """
    from boomi_mcp.compiler.system_topology import plan_system_topology
    from boomi_mcp.compiler.system_topology.context import (
        ComponentFactV1,
        DiscoveryPageProvenanceV1,
    )

    spec = parse_system_topology_v1(
        {
            "version": "1",
            "profile_ref": "p-alpha",
            "objects": [{"kind": "process", "key": "b", "component_ref": "ghost"}],
            "relations": [],
        }
    )
    facts = (
        ComponentFactV1(
            profile="p-alpha", component_id="present", component_type="process"
        ),
    )

    def _blockers(*pages):
        return {
            b.path
            for b in plan_system_topology(
                spec,
                TopologyResolutionContextV1(
                    profile="p-alpha",
                    snapshot=_snapshot(
                        profile="p-alpha", components=facts, pagination=pages
                    ),
                ),
            ).blockers
        }

    mixed = _blockers(
        DiscoveryPageProvenanceV1(
            component_type="process", returned_count=1, total_available=1
        ),
        DiscoveryPageProvenanceV1(
            component_type="process",
            returned_count=1,
            total_available=9,
            has_more=True,
        ),
    )
    paged = _blockers(
        DiscoveryPageProvenanceV1(
            component_type="process", returned_count=2, total_available=2
        )
    )
    # Nothing was claimed while a page was still outstanding, and paging ADDED
    # the finding rather than removing one.
    assert mixed == set()
    assert paged == {"/objects/0/component_ref"}


def test_paging_a_single_type_adds_the_blocker_it_reveals():
    """The behavioural claim the #252 wording rests on, measured not asserted."""
    from boomi_mcp.compiler.system_topology import plan_system_topology
    from boomi_mcp.compiler.system_topology.context import (
        ComponentFactV1,
        DiscoveryPageProvenanceV1,
    )

    spec = parse_system_topology_v1(
        {
            "version": "1",
            "profile_ref": "p-alpha",
            "objects": [
                {"kind": "process", "key": "a", "component_ref": "present"},
                {"kind": "process", "key": "b", "component_ref": "ghost"},
            ],
            "relations": [],
        }
    )
    facts = (
        ComponentFactV1(
            profile="p-alpha", component_id="present", component_type="process"
        ),
    )

    def _plan(page):
        return plan_system_topology(
            spec,
            TopologyResolutionContextV1(
                profile="p-alpha",
                snapshot=_snapshot(
                    profile="p-alpha", components=facts, pagination=(page,)
                ),
            ),
        )

    truncated = _plan(
        DiscoveryPageProvenanceV1(
            component_type="process",
            returned_count=1,
            total_available=9,
            has_more=True,
        )
    )
    paged = _plan(
        DiscoveryPageProvenanceV1(
            component_type="process", returned_count=1, total_available=1
        )
    )
    before = {b.path for b in truncated.blockers}
    after = {b.path for b in paged.blockers}
    # Paging retired the NOTICE and added a blocker. It removed nothing — which
    # is why the notice may not present paging as a way to clear one.
    assert "discovery_pagination" in {d.subject for d in truncated.unresolved_decisions}
    assert "discovery_pagination" not in {d.subject for d in paged.unresolved_decisions}
    assert before <= after and after - before == {"/objects/1/component_ref"}


def test_guidance_never_asserts_a_universal_the_plan_itself_refutes():
    """QA #253/#254. Six rounds censused decisions; guidance was never read.

    ``TopologyGuidanceV1`` carries no provenance and no revision stamp, so a
    live universal in the present tense is unfalsifiable to a reader and
    refutable by the very payload beside it. Both offenders asserted one.
    """
    from boomi_mcp.compiler.system_topology import plan_system_topology
    from boomi_mcp.compiler.system_topology.context import ComponentFactV1

    spec = parse_system_topology_v1(
        {
            "version": "1",
            "profile_ref": "p-alpha",
            "objects": [
                {"kind": "api_service", "key": "a", "component_ref": "asc-1"},
                {"kind": "process", "key": "p", "component_ref": "$ref:kp"},
                {"kind": "runtime", "key": "rt", "runtime_ref": "rt-1"},
                {"kind": "schedule", "key": "s"},
            ],
            "relations": [
                {
                    "kind": "schedule_binding",
                    "key": "rs",
                    "schedule": "s",
                    "process": "p",
                    "runtime": "rt",
                }
            ],
        }
    )
    plan = plan_system_topology(
        spec,
        TopologyResolutionContextV1(
            profile="p-alpha",
            snapshot=_snapshot(
                profile="p-alpha",
                components=(
                    ComponentFactV1(
                        profile="p-alpha",
                        component_id="asc-1",
                        component_type="webservice",
                    ),
                ),
            ),
        ),
    )
    # The plan resolves an API Service Component out of the shipped capture...
    assert ("a", "existing_component") in [
        (r.object_key, r.resolution) for r in plan.resolved_references
    ]
    messages = {g.subject: _normalized_question(g.message) for g in plan.guidance}
    # ...so no guidance beside it may say none exists.
    assert "no api service component exists" not in messages["api_service"]
    assert "either live profile" not in messages["api_service"]
    # And retry has live evidence, which the snapshot model records; guidance
    # may say it is unmodeled, never that nothing about it was observed.
    assert "no shape has evidence" not in messages["schedule_content"]
    assert "retry" in messages["schedule_content"]

    # QA #273. ``derive_guidance`` gates this string on the spec alone, so it
    # publishes with no snapshot at all — beside `live_revalidation`'s "no live
    # discovery snapshot applies to this plan", in one document. It therefore
    # states a property of the CONTRACT ("where a capture observes one..."),
    # never an observation this plan may not have. `observed_max_retry` also
    # defaults to None, so even a snapshot does not guarantee the claim.
    bare = plan_system_topology(
        spec, TopologyResolutionContextV1(profile="p-alpha")
    )
    bare_messages = {g.subject: _normalized_question(g.message) for g in bare.guidance}
    assert "live_revalidation" in {d.subject for d in bare.unresolved_decisions}
    for forbidden in (
        "a max-retry value is observed",
        "is observed, and is recorded on the snapshot",
        "is observed on the snapshot",
    ):
        assert forbidden not in bare_messages["schedule_content"], forbidden
    assert "where a capture observes" in bare_messages["schedule_content"]


def test_a_foreign_capture_may_not_refute_a_reference_type():
    """QA #251. The one consumer `same_account` did not gate.

    A coherent capture of another account may not confirm this account's
    reference, witness its absence, or supply its classification — and yet it
    could REFUTE the reference's type, because "a wrong type is conclusive from
    the fact alone" was applied across an account boundary. It is not: this
    package's own context docstring records that two profiles legitimately hold
    the same component ids for different things.
    """
    spec = parse_system_topology_v1(
        {
            "version": "1",
            "profile_ref": "p-alpha",
            "objects": [
                {"kind": "process", "key": "x", "component_ref": "shared-id"}
            ],
            "relations": [],
        }
    )
    foreign = validate_system_topology(
        spec,
        TopologyResolutionContextV1(
            profile="p-omega",
            snapshot=_snapshot(
                profile="p-omega",
                components=(
                    ComponentFactV1(
                        profile="p-omega",
                        component_id="shared-id",
                        component_type="documentcache",
                    ),
                ),
            ),
        ),
    )
    codes = _codes(foreign)
    # The account mismatch is reported, once, and nothing is claimed about the
    # authored reference on another account's evidence.
    assert "TOPOLOGY_ENVIRONMENT_MISMATCH" in codes
    assert "TOPOLOGY_REFERENCE_TYPE_MISMATCH" not in codes, codes

    # The control: the same disagreement inside the authored account IS a type
    # mismatch, so the silence above is the account gate and not a lost rule.
    same = validate_system_topology(
        spec,
        TopologyResolutionContextV1(
            profile="p-alpha",
            snapshot=_snapshot(
                profile="p-alpha",
                components=(
                    ComponentFactV1(
                        profile="p-alpha",
                        component_id="shared-id",
                        component_type="documentcache",
                    ),
                ),
            ),
        ),
    )
    assert "TOPOLOGY_REFERENCE_TYPE_MISMATCH" in _codes(same)


def test_live_revalidation_is_true_in_the_trigger_no_earlier_wording_covered():
    """QA #248. The third trigger, where both published disjuncts were false.

    ``snapshot.profile != prepared.context.profile`` fires ALONE exactly when
    the snapshot matches the spec — a genuine capture of precisely the account
    being planned. There, "none was supplied" and "belongs to a different
    account than the one being planned" are both false, and "re-run with a
    snapshot for this account" is inert: following it reproduces the same plan.
    The only true account of that state involves the resolution CONTEXT, so the
    sentence has to name it. Both retracted wordings fail that, which is what
    makes this a check and not a restatement.
    """
    from boomi_mcp.compiler.system_topology import plan_system_topology

    spec = parse_system_topology_v1(
        {
            "version": "1",
            "profile_ref": "p-alpha",
            "objects": [{"kind": "process", "key": "x", "component_ref": "lit-1"}],
            "relations": [],
        }
    )
    plan = plan_system_topology(
        spec,
        TopologyResolutionContextV1(
            profile="p-beta",
            # Matches the SPEC exactly; only the context differs.
            snapshot=_snapshot(profile="p-alpha"),
        ),
    )
    notice = next(
        d for d in plan.unresolved_decisions if d.subject == "live_revalidation"
    )
    question = _normalized_question(notice.question)
    assert "context" in question, notice.question
    # The two false accounts of this state, neither of which may return.
    assert "without a live discovery snapshot" not in question
    assert "different account than the one being planned" not in question


def test_the_overclaim_guard_catches_every_retracted_sentence():
    """The control. A guard that fires on nothing forbids nothing.

    Each string below is a sentence this contract actually published and
    retracted, or a reworded variant of one that survived the identity-based
    pin. All of them must trip the guard.
    """
    from boomi_mcp.compiler.system_topology.contracts import TopologyDecisionV1

    class _FakePlan:
        resolved_references = ("one row, so a universal negative is a lie",)

        def __init__(self, question):
            self.unresolved_decisions = (
                TopologyDecisionV1(subject="s", question=question),
            )

    retracted = (
        "No environment reference was judged against it.",
        "Nothing in this snapshot was checked against your environment "
        "references: presence and absence are both unknown.",
        "None of the authored references were verified.",
        "This snapshot cannot say whether those components exist.",
        "This plan was produced without a live discovery snapshot, so "
        "existing-component references are unverified.",
    )
    for question in retracted:
        assert _overclaiming_decisions(
            _FakePlan(question), snapshot_supplied=True
        ), question

    # The guard's KNOWN false-positive class, recorded rather than left as a
    # trap. Every one of these sentences is TRUE, and every one trips the
    # pattern — a negative quantifier followed by a judgement verb reads the
    # same whether it denies a judgement or merely limits a scope.
    #
    # The honest account of why the pattern stays: a narrowing DOES exist at no
    # measured recall cost (exempt a negative quantifier followed within ~30
    # characters by "about"/"applies to"/"in scope"/…), so "narrowing would
    # lose coverage" is not the reason. The reason is that such an exemption is
    # another keyword list whose false-NEGATIVE surface nobody has measured,
    # and it hands the next rewording a ready-made bypass — "no reference was
    # judged, which affects nothing here". What the pattern really encodes is a
    # style rule: state a scope limit POSITIVELY. Its whole blast radius is the
    # handful of decision strings this contract publishes, and every member of
    # the class below has an accepted rewrite that is also the clearer sentence.
    # A failure here means "say it positively", not "your sentence is false".
    for question in (
        "This says nothing about environment or runtime references, whose "
        "absence is judged from their own listings.",
        "None of this applies to references the plan already resolved.",
        "Nothing here changes what was checked.",
    ):
        assert _overclaiming_decisions(
            _FakePlan(question), snapshot_supplied=True
        ), question

    # ...and the wording actually shipped does not trip it, so the guard is
    # discriminating rather than merely loud.
    for question in (
        "The environment listing did not answer, so this snapshot cannot be "
        "read as a complete environment inventory. Rows it does carry still "
        "resolve; what it cannot witness is ABSENCE.",
        "At least one component query did not answer. Rows the snapshot does "
        "carry still resolve; what it cannot witness is ABSENCE.",
        "No live discovery snapshot applies to this plan: either none was "
        "supplied, or the one supplied belongs to a different account.",
    ):
        assert _overclaiming_decisions(
            _FakePlan(question), snapshot_supplied=True
        ) == [], question


def test_no_published_decision_overclaims_in_any_reachable_plan():
    """The guard applied where it counts: every plan the matrix can produce."""
    from boomi_mcp.compiler.system_topology import plan_system_topology
    from boomi_mcp.compiler.system_topology.context import (
        DiscoveryPageProvenanceV1,
        EnvironmentFactV1,
    )

    spec = parse_system_topology_v1(
        {
            "version": "1",
            "profile_ref": "p-alpha",
            "objects": [
                {"kind": "process", "key": "x", "component_ref": "lit-1"},
                {"kind": "environment", "key": "e", "environment_ref": "env-1"},
                {"kind": "deployment_unit", "key": "u"},
            ],
            # Bound, so the spec is legal — and so `topology_apply` is one of
            # the decisions the sweep actually reads.
            "relations": [
                {
                    "kind": "deployment_binding",
                    "key": "rd",
                    "deployment_unit": "u",
                    "process": "x",
                    "environment": "e",
                }
            ],
        }
    )
    row = EnvironmentFactV1(profile="p-alpha", environment_id="env-1")
    truncated = DiscoveryPageProvenanceV1(
        component_type="process", returned_count=1, total_available=2, has_more=True
    )
    unobserved = DiscoveryPageProvenanceV1(
        component_type="process", returned_count=0, observed=False
    )
    for ctx_profile in ("p-alpha", "p-beta"):
        for snapshot_profile in ("p-alpha", "p-beta"):
            for observed in (True, False):
                for pagination in ((), (truncated,), (unobserved,)):
                    plan = plan_system_topology(
                        spec,
                        TopologyResolutionContextV1(
                            profile=ctx_profile,
                            snapshot=_snapshot(
                                profile=snapshot_profile,
                                environments=(row,),
                                pagination=pagination,
                                environment_inventory_observed=observed,
                            ),
                        ),
                    )
                    assert _overclaiming_decisions(
                        plan, snapshot_supplied=True
                    ) == [], (
                        ctx_profile,
                        snapshot_profile,
                        observed,
                        [p.component_type for p in pagination],
                    )


def test_the_observation_flag_defaults_to_claiming_nothing():
    """QA #243, the default itself. A hand-built snapshot earns no authority.

    Nothing graded the ``False`` default: every test that cared passed the flag
    explicitly, so flipping the model default to ``True`` changed no test while
    silently restoring the bug for every context assembled by hand — which is
    precisely the pre-delta adapter's shape.
    """
    spec = parse_system_topology_v1(
        {
            "version": "1",
            "profile_ref": "p-alpha",
            "objects": [
                {"kind": "environment", "key": "e", "environment_ref": "ghost-env"}
            ],
            "relations": [],
        }
    )
    ctx = TopologyResolutionContextV1(
        profile="p-alpha",
        # The flag is DELIBERATELY not passed. An empty inventory it never
        # claims to have observed may not witness absence.
        snapshot=_snapshot(profile="p-alpha", environments=()),
    )
    assert "TOPOLOGY_REFERENCE_NOT_FOUND" not in _codes(
        validate_system_topology(spec, ctx)
    )


def test_no_snapshot_means_no_live_fact_claim():
    """A6-F5c. A structural binding rested on the caller saying so.

    Labelling it ``live_fact`` with no snapshot present put a corroboration the
    planner never had into the plan's own output.
    """
    from boomi_mcp.compiler.system_topology import plan_system_topology

    spec = parse_system_topology_v1(
        {
            "version": "1",
            "profile_ref": "p-alpha",
            "objects": [
                {"kind": "process", "key": "pr", "component_ref": "$ref:kp"},
                {"kind": "runtime", "key": "rt", "runtime_ref": "rt-1"},
                {"kind": "schedule", "key": "s"},
            ],
            "relations": [
                {"kind": "schedule_binding", "key": "rs", "schedule": "s", "process": "pr", "runtime": "rt"}
            ],
        }
    )
    plan = plan_system_topology(
        spec,
        TopologyResolutionContextV1(
            profile="p-alpha",
            component_plan_symbols=(
                ComponentPlanSymbolV1(component_key="kp", component_type="process"),
            ),
        ),
    )
    assert [r.witness for r in plan.planning_only_relations] == ["declared_intent"]


def test_a_blocked_object_yields_no_prerequisite():
    """A6-F5a. The plan emitted a prerequisite for a component it rejected."""
    from boomi_mcp.compiler.system_topology import plan_system_topology

    spec = parse_system_topology_v1(
        {
            "version": "1",
            "profile_ref": "p-alpha",
            "objects": [{"kind": "process", "key": "a", "component_ref": "$ref:k"}],
            "relations": [],
        }
    )
    plan = plan_system_topology(
        spec,
        TopologyResolutionContextV1(
            profile="p-alpha",
            component_plan_symbols=(
                ComponentPlanSymbolV1(component_key="k", component_type="documentcache"),
            ),
        ),
    )
    assert [b.code for b in plan.blockers] == ["TOPOLOGY_REFERENCE_TYPE_MISMATCH"]
    assert plan.executable_component_prerequisites == ()


def test_the_invariant_checker_verifies_membership_against_the_spec():
    """A6-MED-9 / R2. Driven BEHAVIORALLY, not by grepping the source.

    Grepping for a message proves the string exists, not that the check runs or
    that it can fail. Each case below hands the checker a plan that is wrong in
    exactly one way and asserts it raises.
    """
    import inspect

    import pytest as _pytest

    from boomi_mcp.compiler.system_topology import plan_system_topology
    from boomi_mcp.compiler.system_topology.context import prepare_topology_context
    from boomi_mcp.compiler.system_topology.contracts import (
        PlannedTopologyRelationV1,
        TopologyRuntimeOrderV1,
    )
    from boomi_mcp.compiler.system_topology.invariants import (
        TopologyPlanningInvariantError,
        check_topology_plan_invariants,
    )

    # ``spec`` is REQUIRED. A default made every spec-dependent check skippable
    # by omitting an argument — the same fail-open shape they were added to close.
    parameter = inspect.signature(check_topology_plan_invariants).parameters["spec"]
    assert parameter.default is inspect.Parameter.empty

    spec = parse_system_topology_v1(
        {
            "version": "1",
            "profile_ref": "p-alpha",
            "objects": [
                {"kind": "process", "key": "top", "component_ref": "$ref:kt"},
                {"kind": "process", "key": "leaf", "component_ref": "$ref:kl"},
            ],
            "relations": [
                {"kind": "process_call", "key": "r", "caller_process": "top", "callee_process": "leaf"}
            ],
        }
    )
    ctx = TopologyResolutionContextV1(
        profile="p-alpha",
        component_plan_symbols=(
            ComponentPlanSymbolV1(
                component_key="kt", component_type="process", has_process_ir=True
            ),
            ComponentPlanSymbolV1(
                component_key="kl", component_type="process", has_process_ir=True
            ),
        ),
        process_call_evidence=(
            ProcessCallEvidenceV1(
                caller_component_ref="$ref:kt",
                callee_component_ref="$ref:kl",
                witness="process_ir",
            ),
        ),
    )
    plan = plan_system_topology(spec, ctx)
    prepared = prepare_topology_context(ctx)
    assert plan.blockers == ()
    # The real plan passes its own checker.
    check_topology_plan_invariants(plan, prepared, spec)

    # 1. A relation the spec never declared.
    invented = plan.model_copy(
        update={
            "planning_only_relations": plan.planning_only_relations
            + (
                PlannedTopologyRelationV1(
                    relation_key="invented",
                    relation_kind="process_call",
                    witness="process_ir",
                ),
            )
        }
    )
    with _pytest.raises(TopologyPlanningInvariantError):
        check_topology_plan_invariants(invented, prepared, spec)

    # 2. An order that does not linearize the ProcessCall graph.
    reversed_order = plan.model_copy(
        update={
            "runtime_process_order": TopologyRuntimeOrderV1(order=("top", "leaf"))
        }
    )
    with _pytest.raises(TopologyPlanningInvariantError):
        check_topology_plan_invariants(reversed_order, prepared, spec)

    # 3. A clean plan that silently dropped its order entirely. Checking only
    #    the edges whose endpoints already appear let this pass vacuously —
    #    there were no positions to compare.
    emptied = plan.model_copy(
        update={"runtime_process_order": TopologyRuntimeOrderV1(order=())}
    )
    with _pytest.raises(TopologyPlanningInvariantError):
        check_topology_plan_invariants(emptied, prepared, spec)

    # 4. An order naming a process the spec does not declare.
    foreign = plan.model_copy(
        update={
            "runtime_process_order": TopologyRuntimeOrderV1(
                order=("leaf", "top", "ghost")
            )
        }
    )
    with _pytest.raises(TopologyPlanningInvariantError):
        check_topology_plan_invariants(foreign, prepared, spec)


def test_the_unknown_discriminator_pointer_names_the_kind_field():
    """A6-MED-7a. Pointing at the member position made a caller hunt the field."""
    from boomi_mcp.models.system_topology import SystemTopologyValidationError

    with pytest.raises(SystemTopologyValidationError) as exc:
        parse_system_topology_v1(
            {
                "version": "1",
                "profile_ref": "p-alpha",
                "objects": [{"kind": "not_a_kind", "key": "k"}],
                "relations": [],
            }
        )
    assert [d.path for d in exc.value.diagnostics] == ["/objects/0/kind"]


def test_a_conflicting_symbol_row_cannot_authorize_a_process_ir_witness():
    """Duplicate ComponentPlan rows are permitted; ``any()`` was too generous.

    Resolution deterministically selects the sorted-last type, so a
    ``documentcache`` row marked ``has_process_ir=True`` could authorize a
    ProcessIR witness for the ``process`` row actually selected, which says it
    has none.
    """
    from boomi_mcp.compiler.system_topology.relations import _process_ir_available

    conflicting = TopologyResolutionContextV1(
        profile="p-alpha",
        component_plan_symbols=(
            ComponentPlanSymbolV1(
                component_key="k", component_type="process", has_process_ir=False
            ),
            ComponentPlanSymbolV1(
                component_key="k", component_type="documentcache", has_process_ir=True
            ),
        ),
    )
    assert _process_ir_available("$ref:k", conflicting) is False

    # The selected row saying True is still honoured.
    agreeing = TopologyResolutionContextV1(
        profile="p-alpha",
        component_plan_symbols=(
            ComponentPlanSymbolV1(
                component_key="k", component_type="process", has_process_ir=True
            ),
        ),
    )
    assert _process_ir_available("$ref:k", agreeing) is True


def _binding_plan(schedule_facts, snapshot_profile="p-alpha"):
    from boomi_mcp.compiler.system_topology import plan_system_topology

    spec = parse_system_topology_v1(
        {
            "version": "1",
            "profile_ref": "p-alpha",
            "objects": [
                {"kind": "process", "key": "pr", "component_ref": "proc-1"},
                {"kind": "runtime", "key": "rt", "runtime_ref": "rt-1"},
                {"kind": "schedule", "key": "s"},
            ],
            "relations": [
                {"kind": "schedule_binding", "key": "rs", "schedule": "s", "process": "pr", "runtime": "rt"}
            ],
        }
    )
    return plan_system_topology(
        spec,
        TopologyResolutionContextV1(
            profile="p-alpha",
            snapshot=_snapshot(
                profile=snapshot_profile,
                components=(
                    ComponentFactV1(
                        profile=snapshot_profile,
                        component_id="proc-1",
                        component_type="process",
                    ),
                ),
                runtimes=(
                    RuntimeFactV1(profile=snapshot_profile, runtime_id="rt-1"),
                ),
                schedule_bindings=schedule_facts,
                pagination=_complete("process"),
            ),
        ),
    )


def test_a_foreign_profile_fact_cannot_corroborate_a_binding():
    """A foreign FACT is reported and refused as corroboration — not more.

    It raises the mixed-profile ``/profile_ref`` blocker, and the corroboration
    scan refuses it, so the binding stays ``declared_intent``. It does NOT
    invalidate the whole context: the ComponentPlan symbols and the authored
    relation are still this account's, and erasing them would delete valid
    output over one bad row. Only a CONTEXT-level profile mismatch — the
    evidence set as a whole being about another account — empties the buckets.
    """
    from boomi_mcp.compiler.system_topology.context import ScheduleBindingFactV1

    plan = _binding_plan(
        (
            ScheduleBindingFactV1(
                profile="FOREIGN", process_id="proc-1", runtime_id="rt-1"
            ),
        )
    )
    assert "TOPOLOGY_ENVIRONMENT_MISMATCH" in [b.code for b in plan.blockers]
    assert [r.witness for r in plan.planning_only_relations] == ["declared_intent"]


def test_a_context_level_profile_mismatch_does_empty_the_buckets():
    """The grade above: the evidence set as a whole is about another account."""
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
    plan = plan_system_topology(
        spec,
        TopologyResolutionContextV1(
            profile="p-omega",
            component_plan_symbols=(
                ComponentPlanSymbolV1(
                    component_key="ka", component_type="process", has_process_ir=True
                ),
                ComponentPlanSymbolV1(
                    component_key="kb", component_type="process", has_process_ir=True
                ),
            ),
            process_call_evidence=(
                ProcessCallEvidenceV1(
                    caller_component_ref="$ref:ka",
                    callee_component_ref="$ref:kb",
                    witness="process_ir",
                ),
            ),
        ),
    )
    assert "TOPOLOGY_ENVIRONMENT_MISMATCH" in [b.code for b in plan.blockers]
    assert plan.planning_only_relations == ()
    assert plan.executable_component_prerequisites == ()
    assert plan.resolved_references == ()


def test_a_snapshot_only_mismatch_keeps_valid_symbol_backed_evidence():
    """R3. Emptying everything deleted perfectly valid prerequisites.

    The ComponentPlan symbols and ProcessIR evidence are still this account's;
    only the snapshot is foreign. A blocker must not suppress the parts of a
    plan that are fine — the same rule that keeps one gated queue from emptying
    an otherwise-complete plan.
    """
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
    plan = plan_system_topology(
        spec,
        TopologyResolutionContextV1(
            profile="p-alpha",
            component_plan_symbols=(
                ComponentPlanSymbolV1(
                    component_key="ka", component_type="process", has_process_ir=True
                ),
                ComponentPlanSymbolV1(
                    component_key="kb", component_type="process", has_process_ir=True
                ),
            ),
            process_call_evidence=(
                ProcessCallEvidenceV1(
                    caller_component_ref="$ref:ka",
                    callee_component_ref="$ref:kb",
                    witness="process_ir",
                ),
            ),
            snapshot=_snapshot(profile="p-omega"),
        ),
    )
    assert "TOPOLOGY_ENVIRONMENT_MISMATCH" in [b.code for b in plan.blockers]
    assert len(plan.executable_component_prerequisites) == 2
    assert [r.relation_kind for r in plan.planning_only_relations] == ["process_call"]
    # ...and the caller is told the snapshot proved nothing about this account.
    assert "live_revalidation" in {d.subject for d in plan.unresolved_decisions}


def test_a_coherent_foreign_context_never_proves_absence():
    """R3-P1. Agreement among the WRONG sources is not evidence.

    A context and snapshot that agree with each other but not with the spec
    still describe a different account, and their empty listing was allowed to
    prove that the spec's components do not exist.
    """
    spec = parse_system_topology_v1(
        {
            "version": "1",
            "profile_ref": "p-alpha",
            "objects": [{"kind": "process", "key": "x", "component_ref": "alpha-lit"}],
            "relations": [],
        }
    )
    ctx = TopologyResolutionContextV1(
        profile="p-omega",
        snapshot=_snapshot(profile="p-omega", pagination=_complete("process")),
    )
    codes = _codes(validate_system_topology(spec, ctx))
    assert "TOPOLOGY_ENVIRONMENT_MISMATCH" in codes
    assert "TOPOLOGY_REFERENCE_NOT_FOUND" not in codes, codes


def test_a_coherent_foreign_context_never_contradicts_a_classification():
    """QA #241. The other half of the same rule, on the same evidence.

    Absence authority was qualified to the AUTHORED profile while the
    environment-classification scan stayed on the context's. With a context and
    snapshot that agree with each other but not with the spec, one report then
    said both "this beta snapshot cannot prove your alpha component is missing"
    and "this beta snapshot proves your alpha environment's classification is
    wrong" — the second carrying a remediation that says to align the document
    with the other account's data.

    One evidence set may not be authoritative in one direction and refused in
    the other.
    """
    spec = parse_system_topology_v1(
        {
            "version": "1",
            "profile_ref": "p-alpha",
            "objects": [
                {
                    "kind": "environment",
                    "key": "e",
                    "environment_ref": "env-1",
                    "classification": "PROD",
                }
            ],
            "relations": [],
        }
    )
    from boomi_mcp.compiler.system_topology.context import EnvironmentFactV1

    ctx = TopologyResolutionContextV1(
        profile="p-beta",
        snapshot=_snapshot(
            profile="p-beta",
            environments=(
                EnvironmentFactV1(
                    profile="p-beta", environment_id="env-1", classification="TEST"
                ),
            ),
            environment_inventory_observed=True,
        ),
    )
    report = validate_system_topology(spec, ctx)
    # The profile mismatch itself is reported — loudly, and at ``/profile_ref``.
    assert "TOPOLOGY_ENVIRONMENT_MISMATCH" in _codes(report)
    # But nothing is said about the authored classification, because nothing
    # this context holds is evidence about p-alpha's environments.
    assert [d.path for d in report.errors if d.path.endswith("/classification")] == []


def test_every_leg_of_the_classification_gate_is_load_bearing():
    """QA #241, graded leg by leg. Three profiles must AGREE, not merely pair up.

    The gate was fixed at two mutually-redundant sites, so the obvious test
    input — a foreign fact in a foreign snapshot — is caught by either half
    alone and therefore grades neither. Each arrangement below makes exactly ONE
    leg of ``snapshot == ctx == spec`` disagree while the fact itself carries
    the authored profile, so a gate that dropped that leg would fire and the
    correct gate stays silent.
    """
    from boomi_mcp.compiler.system_topology.context import EnvironmentFactV1

    spec = parse_system_topology_v1(
        {
            "version": "1",
            "profile_ref": "p-alpha",
            "objects": [
                {
                    "kind": "environment",
                    "key": "e",
                    "environment_ref": "env-1",
                    "classification": "PROD",
                }
            ],
            "relations": [],
        }
    )
    # The fact always names the AUTHORED account, so the per-fact filter never
    # masks the envelope gate under test.
    fact = EnvironmentFactV1(
        profile="p-alpha", environment_id="env-1", classification="TEST"
    )
    arrangements = (
        # snapshot == ctx, both foreign to the spec — the coherent-foreign case.
        ("p-beta", "p-beta"),
        # ctx == spec, snapshot foreign.
        ("p-alpha", "p-beta"),
        # snapshot == spec, ctx foreign.
        ("p-beta", "p-alpha"),
    )
    for ctx_profile, snapshot_profile in arrangements:
        report = validate_system_topology(
            spec,
            TopologyResolutionContextV1(
                profile=ctx_profile,
                snapshot=_snapshot(
                    profile=snapshot_profile,
                    environments=(fact,),
                    environment_inventory_observed=True,
                ),
            ),
        )
        offending = [
            d.path for d in report.errors if d.path.endswith("/classification")
        ]
        assert offending == [], (ctx_profile, snapshot_profile, offending)

    # The control: with all three in agreement the contradiction IS reported,
    # so the assertions above are silence earned by the gate, not by the input.
    agreed = validate_system_topology(
        spec,
        TopologyResolutionContextV1(
            profile="p-alpha",
            snapshot=_snapshot(
                profile="p-alpha",
                environments=(fact,),
                environment_inventory_observed=True,
            ),
        ),
    )
    assert [d.path for d in agreed.errors if d.path.endswith("/classification")] == [
        "/objects/0/classification"
    ]


def test_a_coherent_foreign_snapshot_is_not_revalidation_either():
    """QA #242, the spec leg. ``snapshot == ctx`` is not enough.

    The companion test below exercises the CONTEXT leg (snapshot matches the
    spec but not the context). This one makes the snapshot agree with the
    context and disagree with the authored spec, so a check that had kept only
    the context anchor would fall through to the else branch and publish that
    foreign capture's paging notice.
    """
    from boomi_mcp.compiler.system_topology.context import DiscoveryPageProvenanceV1
    from boomi_mcp.compiler.system_topology import plan_system_topology

    spec = parse_system_topology_v1(
        {
            "version": "1",
            "profile_ref": "p-alpha",
            "objects": [{"kind": "process", "key": "x", "component_ref": "lit-1"}],
            "relations": [],
        }
    )
    plan = plan_system_topology(
        spec,
        TopologyResolutionContextV1(
            profile="p-beta",
            snapshot=_snapshot(
                profile="p-beta",
                pagination=(
                    DiscoveryPageProvenanceV1(
                        component_type="process",
                        returned_count=1,
                        total_available=2,
                        has_more=True,
                    ),
                ),
            ),
        ),
    )
    subjects = {d.subject for d in plan.unresolved_decisions}
    assert "live_revalidation" in subjects
    assert "discovery_pagination" not in subjects, subjects


def test_a_snapshot_the_context_discarded_is_not_revalidation():
    """QA #242. Usability and relevance are two different anchors.

    ``prepare_topology_context`` discards every row of an envelope-mismatched
    snapshot, so a snapshot matching the SPEC but not the CONTEXT contributes
    nothing at all. Anchoring the decision on the spec alone left that third
    shape reporting the discarded capture's pagination notice — advising the
    caller to page through a snapshot the planner had already thrown away —
    instead of telling them to re-run with one the context can use.
    """
    from boomi_mcp.compiler.system_topology.context import DiscoveryPageProvenanceV1

    spec = parse_system_topology_v1(
        {
            "version": "1",
            "profile_ref": "p-alpha",
            "objects": [{"kind": "process", "key": "x", "component_ref": "lit-1"}],
            "relations": [],
        }
    )
    from boomi_mcp.compiler.system_topology import plan_system_topology

    truncated = (
        DiscoveryPageProvenanceV1(
            component_type="process",
            returned_count=1,
            total_available=2,
            has_more=True,
        ),
    )
    plan = plan_system_topology(
        spec,
        TopologyResolutionContextV1(
            profile="p-beta",
            snapshot=_snapshot(profile="p-alpha", pagination=truncated),
        ),
    )
    subjects = {d.subject for d in plan.unresolved_decisions}
    assert "live_revalidation" in subjects
    assert "discovery_pagination" not in subjects, subjects


def test_a_usable_same_account_snapshot_still_reports_its_pagination():
    """The control for #242: the fix must not silence a genuine paging notice."""
    from boomi_mcp.compiler.system_topology.context import DiscoveryPageProvenanceV1

    spec = parse_system_topology_v1(
        {
            "version": "1",
            "profile_ref": "p-alpha",
            "objects": [{"kind": "process", "key": "x", "component_ref": "lit-1"}],
            "relations": [],
        }
    )
    from boomi_mcp.compiler.system_topology import plan_system_topology

    plan = plan_system_topology(
        spec,
        TopologyResolutionContextV1(
            profile="p-alpha",
            snapshot=_snapshot(
                profile="p-alpha",
                pagination=(
                    DiscoveryPageProvenanceV1(
                        component_type="process",
                        returned_count=1,
                        total_available=2,
                        has_more=True,
                    ),
                ),
            ),
        ),
    )
    subjects = {d.subject for d in plan.unresolved_decisions}
    assert "discovery_pagination" in subjects
    assert "live_revalidation" not in subjects, subjects


def test_a_same_profile_fact_that_does_not_match_leaves_the_binding_declared():
    """The corroboration rule in isolation: right account, wrong pair."""
    from boomi_mcp.compiler.system_topology.context import ScheduleBindingFactV1

    plan = _binding_plan(
        (
            ScheduleBindingFactV1(
                profile="p-alpha", process_id="proc-1", runtime_id="OTHER-RUNTIME"
            ),
        )
    )
    assert plan.blockers == (), [b.code for b in plan.blockers]
    assert [r.witness for r in plan.planning_only_relations] == ["declared_intent"]


def test_a_same_profile_matching_fact_earns_live_fact():
    from boomi_mcp.compiler.system_topology.context import ScheduleBindingFactV1

    plan = _binding_plan(
        (
            ScheduleBindingFactV1(
                profile="p-alpha", process_id="proc-1", runtime_id="rt-1"
            ),
        )
    )
    assert plan.blockers == (), [b.code for b in plan.blockers]
    assert [r.witness for r in plan.planning_only_relations] == ["live_fact"]


def test_a_planned_ref_subject_is_never_corroborated():
    """R2. The docstring said literal ids; the code compared what it was handed.

    A snapshot fact whose ``process_id`` literally read ``$ref:kp`` matched by
    string equality and promoted a planned binding to ``live_fact`` — live data
    corroborating a component that does not exist yet.
    """
    from boomi_mcp.compiler.system_topology import plan_system_topology
    from boomi_mcp.compiler.system_topology.context import ScheduleBindingFactV1

    spec = parse_system_topology_v1(
        {
            "version": "1",
            "profile_ref": "p-alpha",
            "objects": [
                {"kind": "process", "key": "pr", "component_ref": "$ref:kp"},
                {"kind": "runtime", "key": "rt", "runtime_ref": "rt-1"},
                {"kind": "schedule", "key": "s"},
            ],
            "relations": [
                {"kind": "schedule_binding", "key": "rs", "schedule": "s", "process": "pr", "runtime": "rt"}
            ],
        }
    )
    plan = plan_system_topology(
        spec,
        TopologyResolutionContextV1(
            profile="p-alpha",
            component_plan_symbols=(
                ComponentPlanSymbolV1(component_key="kp", component_type="process"),
            ),
            snapshot=_snapshot(
                runtimes=(RuntimeFactV1(profile="p-alpha", runtime_id="rt-1"),),
                schedule_bindings=(
                    ScheduleBindingFactV1(
                        profile="p-alpha", process_id="$ref:kp", runtime_id="rt-1"
                    ),
                ),
                pagination=_complete("process"),
            ),
        ),
    )
    assert [r.witness for r in plan.planning_only_relations] == ["declared_intent"]


@pytest.mark.parametrize("spelling", ["process", "PROCESS", "Process"])
def test_the_processir_symbol_selection_matches_resolutions(spelling):
    """The two must agree on WHICH duplicate row was selected.

    Sorting raw types picks a different row than sorting normalized ones
    whenever a duplicate uses a builder-legal case variant — ``PROCESS`` sorts
    before ``documentcache`` while ``process`` sorts after it — so a valid
    planned relation was gated on a row resolution had not selected.
    """
    from boomi_mcp.compiler.system_topology.context import prepare_topology_context
    from boomi_mcp.compiler.system_topology.relations import _process_ir_available

    ctx = TopologyResolutionContextV1(
        profile="p-alpha",
        component_plan_symbols=(
            ComponentPlanSymbolV1(
                component_key="k", component_type=spelling, has_process_ir=True
            ),
            ComponentPlanSymbolV1(
                component_key="k", component_type="documentcache", has_process_ir=False
            ),
        ),
    )
    selected_type = dict(prepare_topology_context(ctx).symbols)["k"]
    # Resolution picks the process row; the ProcessIR check must agree.
    assert selected_type == "process", spelling
    assert _process_ir_available("$ref:k", ctx) is True, spelling


def test_a_case_variant_planned_process_call_is_not_gated():
    """The same divergence, at the level a caller actually feels it."""
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
            ComponentPlanSymbolV1(
                component_key="ka", component_type="PROCESS", has_process_ir=True
            ),
            ComponentPlanSymbolV1(
                component_key="kb", component_type="Process", has_process_ir=True
            ),
        ),
        process_call_evidence=(
            ProcessCallEvidenceV1(
                caller_component_ref="$ref:ka",
                callee_component_ref="$ref:kb",
                witness="process_ir",
            ),
        ),
    )
    assert validate_system_topology(spec, ctx).errors == ()


@pytest.mark.parametrize(
    "rows,expected",
    [
        # Conflicting alias rows: unresolvable evidence, fails closed.
        ((("PROCESS", True), ("process", False)), False),
        ((("process", False), ("PROCESS", True)), False),
        # Agreeing alias rows: honoured.
        ((("PROCESS", True), ("process", True)), True),
        # A different TYPE loses the type selection outright; the flag on the
        # selected type is what counts.
        ((("PROCESS", True), ("documentcache", False)), True),
        ((("documentcache", True), ("process", False)), False),
        # Single rows.
        ((("process", True),), True),
        ((("process", False),), False),
    ],
)
def test_conflicting_processir_flags_fail_closed(rows, expected):
    """Sorting ``(type, flag)`` together made the boolean a TIE-BREAKER.

    Two alias rows that normalize alike but disagree on the flag therefore
    always yielded ``True`` — fail-open, in the one helper whose whole job is to
    stop a witness being authorized by something that is not there. Resolution
    makes no such choice: it selects a type and nothing else.
    """
    from boomi_mcp.compiler.system_topology.relations import _process_ir_available

    ctx = TopologyResolutionContextV1(
        profile="p-alpha",
        component_plan_symbols=tuple(
            ComponentPlanSymbolV1(
                component_key="k", component_type=component_type, has_process_ir=flag
            )
            for component_type, flag in rows
        ),
    )
    assert _process_ir_available("$ref:k", ctx) is expected, rows


def test_conflicting_processir_flags_gate_the_relation():
    """The same property where a caller feels it."""
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
            ComponentPlanSymbolV1(
                component_key="ka", component_type="PROCESS", has_process_ir=True
            ),
            ComponentPlanSymbolV1(
                component_key="ka", component_type="process", has_process_ir=False
            ),
            ComponentPlanSymbolV1(
                component_key="kb", component_type="process", has_process_ir=True
            ),
        ),
        process_call_evidence=(
            ProcessCallEvidenceV1(
                caller_component_ref="$ref:ka",
                callee_component_ref="$ref:kb",
                witness="process_ir",
            ),
        ),
    )
    assert "TOPOLOGY_CAPABILITY_GATED" in _codes(validate_system_topology(spec, ctx))


def test_a_duplicate_object_key_does_not_crash_the_planner():
    """QA #240. The diagnostic was computed, then thrown away by a crash.

    A duplicate object KEY produced two resolution rows for one key and tripped
    the uniqueness invariant, so the planner emitted the right diagnostic and
    then raised ``TopologyPlanningInvariantError`` on top of it — an exception
    whose own contract is that it is never raised because of authored input. A
    duplicate key is authored input.
    """
    from boomi_mcp.models.system_topology import SystemTopologySpecV1
    from boomi_mcp.compiler.system_topology import plan_system_topology

    spec = SystemTopologySpecV1.model_validate(
        {
            "version": "1",
            "profile_ref": "p-alpha",
            "objects": [
                {"kind": "process", "key": "dup", "component_ref": "$ref:k1"},
                {"kind": "process", "key": "dup", "component_ref": "$ref:k2"},
            ],
            "relations": [],
        }
    )
    plan = plan_system_topology(
        spec,
        TopologyResolutionContextV1(
            profile="p-alpha",
            component_plan_symbols=(
                ComponentPlanSymbolV1(component_key="k1", component_type="process"),
                ComponentPlanSymbolV1(component_key="k2", component_type="process"),
            ),
        ),
    )
    assert [b.code for b in plan.blockers] == ["TOPOLOGY_SCHEMA_DUPLICATE_KEY"]
    # One row per key, so the uniqueness invariant holds.
    assert len({r.object_key for r in plan.resolved_references}) == len(
        plan.resolved_references
    )


def test_all_three_per_fact_profile_filters_share_one_anchor():
    """QA #239. The re-anchor moved two of three sites, creating a divergence.

    A fact foreign to the CONTEXT was discarded for resolution yet still
    supplied that same environment's classification — one report saying both
    "this environment does not resolve in your context" and "your
    classification for it disagrees with what discovery observed".

    Scoped to the three per-fact FILTERS. Capture self-consistency is a
    different question with a different correct anchor (QA #249), so it lives
    in ``_internally_mixed_fact_count`` — the one sanctioned envelope-anchored
    comparison, asserted below to still be envelope-anchored so this guard
    cannot be sidestepped by moving a filter into it.
    """
    import inspect

    from boomi_mcp.compiler.system_topology import context as ctx_mod
    from boomi_mcp.compiler.system_topology import relations as rel_mod

    sources = (
        inspect.getsource(ctx_mod.prepare_topology_context),
        inspect.getsource(rel_mod._binding_corroborated),
        inspect.getsource(rel_mod.collect_environment_findings),
    )
    for source in sources:
        assert "!= snapshot.profile" not in source, source[:120]
        assert "== snapshot.profile" not in source, source[:120]

    # The sanctioned exception: envelope-anchored by construction, and counting
    # only — it indexes nothing, so it cannot become a filter without this
    # assertion failing.
    mixed = inspect.getsource(ctx_mod._internally_mixed_fact_count)
    assert "envelope = snapshot.profile" in mixed
    # It COUNTS. A filter would have to build a collection to be of any use to
    # a caller, and this returns an int, so it cannot quietly become one.
    assert "-> int:" in mixed
    body = mixed.split('"""')[-1]
    assert body.count("sum(1 for") == 5 and "[" not in body


def test_a_context_foreign_fact_supplies_no_classification():
    """The behavioral half of #239."""
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
    # The snapshot envelope agrees with the context; the FACT inside does not.
    ctx = TopologyResolutionContextV1(
        profile="p-alpha",
        component_plan_symbols=(
            ComponentPlanSymbolV1(component_key="k", component_type="process"),
        ),
        snapshot=_snapshot(
            environments=(
                EnvironmentFactV1(
                    profile="p-omega", environment_id="env-1", classification="PROD"
                ),
            )
        ),
    )
    findings = [
        d
        for d in validate_system_topology(spec, ctx).errors
        if d.code == "TOPOLOGY_ENVIRONMENT_MISMATCH"
    ]
    # Reported as a mixed-profile snapshot, never as a classification claim
    # built on another account's row.
    assert findings
    assert all(d.path == "/profile_ref" for d in findings), [d.path for d in findings]


def test_the_invariant_checker_enforces_relation_bucket_permissibility():
    """R3-F4. The ordering checks alone did not give the bucket rule.

    The checker accepted a blocked, witness-less plan with that relation
    INJECTED into ``planning_only_relations`` — the second half of "each
    declared relation occupies only its permitted bucket".
    """
    import pytest as _pytest

    from boomi_mcp.compiler.system_topology import plan_system_topology
    from boomi_mcp.compiler.system_topology.context import prepare_topology_context
    from boomi_mcp.compiler.system_topology.contracts import PlannedTopologyRelationV1
    from boomi_mcp.compiler.system_topology.invariants import (
        TopologyPlanningInvariantError,
        check_topology_plan_invariants,
    )

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
    # No witness -> the relation is gated, and its blocker sits at /relations/0.
    bare = TopologyResolutionContextV1(
        profile="p-alpha",
        component_plan_symbols=(
            ComponentPlanSymbolV1(component_key="ka", component_type="process"),
            ComponentPlanSymbolV1(component_key="kb", component_type="process"),
        ),
    )
    plan = plan_system_topology(spec, bare)
    prepared = prepare_topology_context(bare)
    assert plan.blockers
    assert plan.planning_only_relations == ()
    check_topology_plan_invariants(plan, prepared, spec)

    injected = plan.model_copy(
        update={
            "planning_only_relations": (
                PlannedTopologyRelationV1(
                    relation_key="r",
                    relation_kind="process_call",
                    witness="process_ir",
                ),
            )
        }
    )
    with _pytest.raises(TopologyPlanningInvariantError):
        check_topology_plan_invariants(injected, prepared, spec)


def test_a_mismatched_snapshot_envelope_invalidates_every_row_inside_it():
    """A snapshot cannot be more trustworthy than the account it says it came from.

    Filtering row-by-row let an omega-envelope capture whose rows happened to be
    stamped alpha supply resolution and ``live_fact`` corroboration for alpha —
    while ``/profile_ref`` was blocked and ``live_revalidation`` simultaneously
    said the snapshot proved nothing.
    """
    from boomi_mcp.compiler.system_topology import plan_system_topology
    from boomi_mcp.compiler.system_topology.context import ScheduleBindingFactV1

    spec = parse_system_topology_v1(
        {
            "version": "1",
            "profile_ref": "p-alpha",
            "objects": [
                {"kind": "process", "key": "pr", "component_ref": "lit-1"},
                {"kind": "runtime", "key": "rt", "runtime_ref": "rt-1"},
                {"kind": "schedule", "key": "s"},
            ],
            "relations": [
                {"kind": "schedule_binding", "key": "rs", "schedule": "s", "process": "pr", "runtime": "rt"}
            ],
        }
    )
    inner_rows = dict(
        components=(
            ComponentFactV1(
                profile="p-alpha", component_id="lit-1", component_type="process"
            ),
        ),
        runtimes=(RuntimeFactV1(profile="p-alpha", runtime_id="rt-1"),),
        schedule_bindings=(
            ScheduleBindingFactV1(
                profile="p-alpha", process_id="lit-1", runtime_id="rt-1"
            ),
        ),
        pagination=_complete("process"),
    )

    foreign_envelope = plan_system_topology(
        spec,
        TopologyResolutionContextV1(
            profile="p-alpha", snapshot=_snapshot(profile="p-omega", **inner_rows)
        ),
    )
    assert "TOPOLOGY_ENVIRONMENT_MISMATCH" in [b.code for b in foreign_envelope.blockers]
    assert foreign_envelope.resolved_references == ()
    assert [r.witness for r in foreign_envelope.planning_only_relations] == [
        "declared_intent"
    ]

    # The same rows under a MATCHING envelope do everything they should.
    matching = plan_system_topology(
        spec,
        TopologyResolutionContextV1(
            profile="p-alpha", snapshot=_snapshot(profile="p-alpha", **inner_rows)
        ),
    )
    assert matching.blockers == (), [b.code for b in matching.blockers]
    assert matching.resolved_references
    assert [r.witness for r in matching.planning_only_relations] == ["live_fact"]


def test_the_invariant_checker_enforces_relation_bucket_completeness():
    """The other half of the bucket rule.

    Checking only the blocked case left a blocker-free plan free to drop its
    relations entirely and still pass — the guarantee the permissibility check
    was documented as closing.
    """
    import pytest as _pytest

    from boomi_mcp.compiler.system_topology import plan_system_topology
    from boomi_mcp.compiler.system_topology.context import prepare_topology_context
    from boomi_mcp.compiler.system_topology.invariants import (
        TopologyPlanningInvariantError,
        check_topology_plan_invariants,
    )

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
            ComponentPlanSymbolV1(
                component_key="ka", component_type="process", has_process_ir=True
            ),
            ComponentPlanSymbolV1(
                component_key="kb", component_type="process", has_process_ir=True
            ),
        ),
        process_call_evidence=(
            ProcessCallEvidenceV1(
                caller_component_ref="$ref:ka",
                callee_component_ref="$ref:kb",
                witness="process_ir",
            ),
        ),
    )
    plan = plan_system_topology(spec, ctx)
    prepared = prepare_topology_context(ctx)
    assert plan.blockers == ()
    assert plan.planning_only_relations
    check_topology_plan_invariants(plan, prepared, spec)

    dropped = plan.model_copy(update={"planning_only_relations": ()})
    with _pytest.raises(TopologyPlanningInvariantError):
        check_topology_plan_invariants(dropped, prepared, spec)


def test_a_mismatched_envelope_also_blocks_the_classification_scan():
    """The envelope gate applies to every snapshot read, not just corroboration.

    The classification scan read the raw snapshot, so a row stamped with the
    context's profile inside a foreign-envelope capture still produced a
    contradiction — from the very snapshot the planner had just declared
    untrustworthy.
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
            profile="p-omega",
            environments=(
                EnvironmentFactV1(
                    profile="p-alpha", environment_id="env-1", classification="PROD"
                ),
            ),
        ),
    )
    findings = [
        d
        for d in validate_system_topology(spec, ctx).errors
        if d.code == "TOPOLOGY_ENVIRONMENT_MISMATCH"
    ]
    assert findings, "the envelope mismatch itself is still reported"
    assert all(d.path == "/profile_ref" for d in findings), [d.path for d in findings]


def test_the_foreign_row_count_describes_the_capture_not_the_context():
    """"Mixed" is a claim about the CAPTURE, so it is measured against itself.

    Deriving the count from the kept-list lengths made an envelope mismatch
    report every correctly-stamped row as foreign; re-deriving it per row but
    against the CONTEXT left the same falsehood reachable by the other route —
    QA #249, where all 61 rows of a coherent single-account live capture were
    counted foreign purely because the context named another profile.
    """
    from boomi_mcp.compiler.system_topology.context import prepare_topology_context

    # QA #249: coherent capture, foreign context. Unusable here, but not mixed.
    coherent_but_foreign = prepare_topology_context(
        TopologyResolutionContextV1(
            profile="p-alpha",
            snapshot=_snapshot(
                profile="p-omega",
                components=(
                    ComponentFactV1(
                        profile="p-omega", component_id="c", component_type="process"
                    ),
                ),
                environments=(
                    EnvironmentFactV1(
                        profile="p-omega", environment_id="e"
                    ),
                ),
            ),
        )
    )
    assert coherent_but_foreign.foreign_profile_fact_count == 0
    # ...and nothing from it is indexed, because it is the wrong account.
    assert coherent_but_foreign.components == ()

    # A row that disagrees with its OWN envelope IS mixed, whichever account
    # the context names — that is a defect in whatever produced the capture.
    row_disagrees_with_envelope = prepare_topology_context(
        TopologyResolutionContextV1(
            profile="p-alpha",
            snapshot=_snapshot(
                profile="p-omega",
                components=(
                    ComponentFactV1(
                        profile="p-alpha", component_id="c", component_type="process"
                    ),
                ),
            ),
        )
    )
    assert row_disagrees_with_envelope.foreign_profile_fact_count == 1
    assert row_disagrees_with_envelope.components == ()

    genuinely_foreign = prepare_topology_context(
        TopologyResolutionContextV1(
            profile="p-alpha",
            snapshot=_snapshot(
                profile="p-alpha",
                components=(
                    ComponentFactV1(
                        profile="p-omega", component_id="c", component_type="process"
                    ),
                ),
            ),
        )
    )
    assert genuinely_foreign.foreign_profile_fact_count == 1
