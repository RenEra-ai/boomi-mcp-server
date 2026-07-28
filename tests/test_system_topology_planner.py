"""Deterministic plan assembly (issue #144, M12.9).

The plan's five buckets are the issue's headline deliverable, so these tests pin
what lands in each and — just as importantly — what never does.
"""

import json
import os
import subprocess
import sys
from pathlib import Path

import pytest

_project_root = Path(__file__).resolve().parent.parent
_src = str(_project_root / "src")
if _src not in sys.path:
    sys.path.insert(0, _src)

from boomi_mcp.models.system_topology import parse_system_topology_v1
from boomi_mcp.compiler.system_topology import (
    canonical_topology_plan_json,
    plan_system_topology,
)
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

_FIXTURES = _project_root / "tests" / "fixtures" / "system_topology"


def _fixture(name):
    return json.loads((_FIXTURES / name).read_text())


_SYMBOLS = tuple(
    ComponentPlanSymbolV1(
        component_key=key, component_type=kind, has_process_ir=kind == "process"
    )
    for key, kind in (
        ("proc_order_sync_main", "process"),
        ("proc_order_sync_child", "process"),
        ("proc_order_intake_listener", "process"),
        ("asc_order_api", "webservice"),
        ("cache_order_lookup", "documentcache"),
        ("prop_order_settings", "processproperty"),
    )
)

_SNAPSHOT = TopologyDiscoverySnapshotV1(
    profile="profile-placeholder",
    captured_at="2026-01-01T00:00:00Z",
    source_revision="rev-placeholder",
    service_release="release-placeholder",
    environments=(
        EnvironmentFactV1(
            profile="profile-placeholder",
            environment_id="environment-placeholder-1",
            classification="TEST",
        ),
    ),
    runtimes=(
        RuntimeFactV1(
            profile="profile-placeholder", runtime_id="runtime-placeholder-1"
        ),
    ),
)

_CONTEXT = TopologyResolutionContextV1(
    profile="profile-placeholder",
    component_plan_symbols=_SYMBOLS,
    snapshot=_SNAPSHOT,
    process_call_evidence=(
        ProcessCallEvidenceV1(
            caller_component_ref="$ref:proc_order_sync_main",
            callee_component_ref="$ref:proc_order_sync_child",
            witness="process_ir",
        ),
    ),
    api_service_route_evidence=(
        ApiServiceRouteEvidenceV1(
            api_service_component_ref="$ref:asc_order_api",
            listener_component_ref="$ref:proc_order_intake_listener",
            witness="typed_builder",
        ),
    ),
    shared_resource_use_evidence=(
        SharedResourceUseEvidenceV1(
            process_component_ref="$ref:proc_order_sync_main",
            resource_component_ref="$ref:cache_order_lookup",
            resource_kind="document_cache",
            witness="process_ir",
        ),
        SharedResourceUseEvidenceV1(
            process_component_ref="$ref:proc_order_sync_main",
            resource_component_ref="$ref:prop_order_settings",
            resource_kind="process_property",
            witness="process_ir",
        ),
    ),
)


@pytest.fixture(scope="module")
def representative_plan():
    spec = parse_system_topology_v1(_fixture("system_topology_v1.json"))
    return plan_system_topology(spec, _CONTEXT)


# ---------------------------------------------------------------------------
# The representative multi-process fixture
# ---------------------------------------------------------------------------


def test_the_fixture_describes_every_concept_the_issue_requires(representative_plan):
    """Multiple processes, ProcessCall, listener binding, shared cache/property,
    schedule intent, deployment intent — and a gated queue alongside them."""
    spec = parse_system_topology_v1(_fixture("system_topology_v1.json"))
    kinds = {o.kind for o in spec.objects} | {r.kind for r in spec.relations}
    assert {
        "process",
        "api_service",
        "document_cache",
        "process_property",
        "schedule",
        "deployment_unit",
        "external_queue",
        "process_call",
        "api_service_route",
        "document_cache_use",
        "process_property_use",
        "schedule_binding",
        "deployment_binding",
        "queue_reference",
    } <= kinds
    assert len([o for o in spec.objects if o.kind == "process"]) >= 3


def test_apply_is_never_supported(representative_plan):
    assert representative_plan.apply_supported is False


def test_the_queue_is_the_only_blocker(representative_plan):
    """Gated intent blocks; nothing else in the fixture does."""
    codes = {b.code for b in representative_plan.blockers}
    assert codes == {"TOPOLOGY_CAPABILITY_GATED"}
    assert {b.subject for b in representative_plan.blockers} == {
        "external_queue",
        "queue_reference",
    }


def test_gated_subjects_never_enter_the_planning_bucket(representative_plan):
    planned = {r.relation_kind for r in representative_plan.planning_only_relations}
    assert "queue_reference" not in planned
    assert "event_stream_reference" not in planned


def test_unaffected_prerequisites_are_still_reported_beside_a_blocker(
    representative_plan,
):
    """A blocker must not suppress the parts of the plan that are fine.

    Otherwise one gated queue would make an otherwise-complete plan look empty,
    and a caller would have no idea what they had.
    """
    assert representative_plan.blockers
    assert representative_plan.executable_component_prerequisites
    assert representative_plan.planning_only_relations


def test_every_witnessed_relation_is_planned_with_its_witness(representative_plan):
    by_kind = {
        r.relation_kind: r.witness for r in representative_plan.planning_only_relations
    }
    assert by_kind["process_call"] == "process_ir"
    assert by_kind["api_service_route"] == "typed_builder"
    assert by_kind["document_cache_use"] == "process_ir"
    assert by_kind["process_property_use"] == "process_ir"
    # Structural bindings the caller declares outright.
    assert by_kind["schedule_binding"] == "live_fact"
    assert by_kind["deployment_binding"] == "live_fact"


def test_the_plan_separates_guidance_from_blockers_and_decisions(representative_plan):
    guidance = {g.subject for g in representative_plan.guidance}
    decisions = {d.subject for d in representative_plan.unresolved_decisions}
    assert "schedule_content" in guidance
    assert "api_service" in guidance
    assert "account_capability_limits" in decisions
    assert "topology_apply" in decisions
    # Three distinct buckets — nothing appears in two of them.
    assert guidance.isdisjoint({b.subject for b in representative_plan.blockers})
    assert decisions.isdisjoint({b.subject for b in representative_plan.blockers})


def test_the_capability_report_travels_with_the_plan(representative_plan):
    assert representative_plan.capability_report.entries
    assert representative_plan.capability_report.revision == "1"


# ---------------------------------------------------------------------------
# Prerequisites
# ---------------------------------------------------------------------------


def test_only_ref_backed_objects_become_prerequisites(representative_plan):
    keys = {p.component_key for p in representative_plan.executable_component_prerequisites}
    assert keys == {s.component_key for s in _SYMBOLS}
    for prerequisite in representative_plan.executable_component_prerequisites:
        assert prerequisite.owner == "component_plan"


def test_a_literal_component_id_is_resolved_but_never_a_prerequisite():
    """An existing component is not something to build."""
    spec = parse_system_topology_v1(
        {
            "version": "1",
            "profile_ref": "prof",
            "objects": [
                {"kind": "process", "key": "existing", "component_ref": "component-id-1"}
            ],
            "relations": [],
        }
    )
    context = TopologyResolutionContextV1(
        profile="prof",
        snapshot=TopologyDiscoverySnapshotV1(
            profile="prof",
            captured_at="t",
            source_revision="r",
            service_release="s",
            components=(
                ComponentFactV1(
                    profile="prof", component_id="component-id-1", component_type="process"
                ),
            ),
        ),
    )
    plan = plan_system_topology(spec, context)
    assert plan.executable_component_prerequisites == ()
    assert [r.resolution for r in plan.resolved_references] == ["existing_component"]


def test_prerequisites_are_deduplicated_and_ordered(representative_plan):
    keys = [p.component_key for p in representative_plan.executable_component_prerequisites]
    assert keys == sorted(keys)
    assert len(keys) == len(set(keys))


# ---------------------------------------------------------------------------
# Runtime order
# ---------------------------------------------------------------------------


def test_a_blocked_plan_reports_no_runtime_order(representative_plan):
    """An order a caller may not act on would read as authoritative."""
    assert representative_plan.blockers
    assert representative_plan.runtime_process_order.order == ()


def test_a_clean_plan_orders_callees_before_callers():
    spec = parse_system_topology_v1(
        {
            "version": "1",
            "profile_ref": "prof",
            "objects": [
                {"kind": "process", "key": "top", "component_ref": "$ref:k_top"},
                {"kind": "process", "key": "mid", "component_ref": "$ref:k_mid"},
                {"kind": "process", "key": "leaf", "component_ref": "$ref:k_leaf"},
            ],
            "relations": [
                {"kind": "process_call", "key": "r1", "caller_process": "top", "callee_process": "mid"},
                {"kind": "process_call", "key": "r2", "caller_process": "mid", "callee_process": "leaf"},
            ],
        }
    )
    context = TopologyResolutionContextV1(
        profile="prof",
        component_plan_symbols=tuple(
            ComponentPlanSymbolV1(component_key=k, component_type="process")
            for k in ("k_top", "k_mid", "k_leaf")
        ),
        process_call_evidence=(
            ProcessCallEvidenceV1(
                caller_component_ref="$ref:k_top",
                callee_component_ref="$ref:k_mid",
                witness="process_ir",
            ),
            ProcessCallEvidenceV1(
                caller_component_ref="$ref:k_mid",
                callee_component_ref="$ref:k_leaf",
                witness="process_ir",
            ),
        ),
    )
    plan = plan_system_topology(spec, context)
    assert plan.blockers == ()
    assert plan.runtime_process_order.order == ("leaf", "mid", "top")
    assert plan.runtime_process_order.namespace == "topology_runtime"
    assert plan.runtime_process_order.basis == "process_call"


def test_runtime_order_is_stable_under_relation_permutation():
    """Authored order is a formatting choice and must not reach the plan."""
    import itertools

    relations = [
        {"kind": "process_call", "key": "r1", "caller_process": "top", "callee_process": "mid"},
        {"kind": "process_call", "key": "r2", "caller_process": "mid", "callee_process": "leaf"},
    ]
    context = TopologyResolutionContextV1(
        profile="prof",
        component_plan_symbols=tuple(
            ComponentPlanSymbolV1(component_key=k, component_type="process")
            for k in ("k_top", "k_mid", "k_leaf")
        ),
        process_call_evidence=(
            ProcessCallEvidenceV1(
                caller_component_ref="$ref:k_top",
                callee_component_ref="$ref:k_mid",
                witness="process_ir",
            ),
            ProcessCallEvidenceV1(
                caller_component_ref="$ref:k_mid",
                callee_component_ref="$ref:k_leaf",
                witness="process_ir",
            ),
        ),
    )
    seen = set()
    for permutation in itertools.permutations(relations):
        spec = parse_system_topology_v1(
            {
                "version": "1",
                "profile_ref": "prof",
                "objects": [
                    {"kind": "process", "key": "top", "component_ref": "$ref:k_top"},
                    {"kind": "process", "key": "mid", "component_ref": "$ref:k_mid"},
                    {"kind": "process", "key": "leaf", "component_ref": "$ref:k_leaf"},
                ],
                "relations": list(permutation),
            }
        )
        seen.add(plan_system_topology(spec, context).runtime_process_order.order)
    assert seen == {("leaf", "mid", "top")}


# ---------------------------------------------------------------------------
# Apply refusal
# ---------------------------------------------------------------------------


def test_a_non_plan_operation_is_refused():
    spec = parse_system_topology_v1(_fixture("system_topology_v1.json"))
    plan = plan_system_topology(spec, _CONTEXT, "apply")
    assert [b.code for b in plan.blockers] == ["TOPOLOGY_APPLY_NOT_SUPPORTED"]
    assert plan.blockers[0].path == "/operation"
    assert plan.apply_supported is False
    assert plan.executable_component_prerequisites == ()
    assert plan.planning_only_relations == ()


def test_the_apply_refusal_happens_before_the_context_is_read():
    """A spy that explodes on access must still get a clean refusal.

    Otherwise "refused" would only mean "refused eventually", after every other
    collector had already run against live data.
    """

    class Exploding:
        def __getattribute__(self, name):
            raise AssertionError(f"context was read: {name}")

    spec = parse_system_topology_v1(_fixture("system_topology_v1.json"))
    plan = plan_system_topology(spec, Exploding(), "deploy")  # type: ignore[arg-type]
    assert [b.code for b in plan.blockers] == ["TOPOLOGY_APPLY_NOT_SUPPORTED"]


# ---------------------------------------------------------------------------
# Determinism
# ---------------------------------------------------------------------------


def test_the_plan_is_byte_identical_across_repeated_runs(representative_plan):
    spec = parse_system_topology_v1(_fixture("system_topology_v1.json"))
    first = canonical_topology_plan_json(plan_system_topology(spec, _CONTEXT))
    second = canonical_topology_plan_json(plan_system_topology(spec, _CONTEXT))
    assert first == second


def test_the_spec_hash_is_the_canonical_spec_digest(representative_plan):
    import hashlib

    from boomi_mcp.models.system_topology import canonical_system_topology_json

    spec = parse_system_topology_v1(_fixture("system_topology_v1.json"))
    expected = hashlib.sha256(
        canonical_system_topology_json(spec).encode("utf-8")
    ).hexdigest()
    assert representative_plan.spec_sha256 == expected


def test_planning_does_not_mutate_its_inputs():
    spec = parse_system_topology_v1(_fixture("system_topology_v1.json"))
    from boomi_mcp.models.system_topology import canonical_system_topology_json

    before_spec = canonical_system_topology_json(spec)
    before_ctx = _CONTEXT.model_dump_json()
    plan_system_topology(spec, _CONTEXT)
    assert canonical_system_topology_json(spec) == before_spec
    assert _CONTEXT.model_dump_json() == before_ctx


_PLAN_SCRIPT = """
import json, sys
sys.path.insert(0, {src!r})
sys.path.insert(0, {tests!r})
from test_system_topology_planner import _CONTEXT
from boomi_mcp.models.system_topology import parse_system_topology_v1
from boomi_mcp.compiler.system_topology import (
    canonical_topology_plan_json, plan_system_topology,
)
spec = parse_system_topology_v1(json.load(open({fixture!r})))
print('PLAN:' + canonical_topology_plan_json(plan_system_topology(spec, _CONTEXT)))
"""


@pytest.mark.parametrize("seed", ["0", "1", "12345"])
def test_the_plan_is_identical_across_hash_seeds(seed, representative_plan):
    """Determinism ACROSS processes — the only way the claim is real.

    ``PYTHONHASHSEED`` is fixed at interpreter startup, so an in-process loop
    cannot vary it. Set iteration order in the capability report and the
    dedup/bucket helpers is exactly the kind of thing it perturbs.
    """
    code = _PLAN_SCRIPT.format(
        src=_src,
        tests=str(_project_root / "tests"),
        fixture=str(_FIXTURES / "system_topology_v1.json"),
    )
    result = subprocess.run(
        [sys.executable, "-c", code],
        capture_output=True,
        text=True,
        env=dict(os.environ, PYTHONPATH=_src, PYTHONHASHSEED=seed),
        cwd=_project_root,
    )
    assert result.returncode == 0, result.stderr
    line = [l for l in result.stdout.splitlines() if l.startswith("PLAN:")][0]
    assert line[len("PLAN:") :] == canonical_topology_plan_json(representative_plan)


def test_canonical_plan_golden_pin(representative_plan):
    committed = (_FIXTURES / "system_topology_plan_v1.json").read_text().strip()
    assert canonical_topology_plan_json(representative_plan) == committed


def test_the_plan_golden_contains_no_real_account_identifier():
    """Fixtures use placeholders only — a golden is a committed artifact."""
    text = (_FIXTURES / "system_topology_plan_v1.json").read_text()
    # A Boomi component id is a lowercase UUID; none may appear.
    import re

    assert not re.search(
        r"[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}", text
    )
