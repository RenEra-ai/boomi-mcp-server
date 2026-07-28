"""Three graphs, three namespaces (issue #144, M12.9).

The acceptance criterion is that a topology RUNTIME edge cannot be confused with
a ComponentPlan BUILD dependency or a ProcessIR CFG edge. Asserting that in
prose is easy and worthless, so it is proved three independent ways:

1. **Vocabulary** — the authored schema has no field a caller could use to say
   "build dependency" or "CFG edge".
2. **Import isolation** — the runtime-graph module cannot reach the compiler's
   ordering code, so it cannot start sharing it by accident.
3. **Byte independence** — perturbing one graph leaves the other's output
   byte-identical. This is the one that would actually catch a real leak; the
   first two catch the ways a leak usually gets introduced.
"""

import json
import os
import subprocess
import sys
from pathlib import Path

_project_root = Path(__file__).resolve().parent.parent
_src = str(_project_root / "src")
if _src not in sys.path:
    sys.path.insert(0, _src)

from boomi_mcp.models.system_topology import (
    parse_system_topology_v1,
    system_topology_v1_json_schema,
)
from boomi_mcp.compiler.system_topology import (
    canonical_topology_plan_json,
    plan_system_topology,
)
from boomi_mcp.compiler.system_topology.context import (
    ComponentPlanSymbolV1,
    ProcessCallEvidenceV1,
    TopologyResolutionContextV1,
)

_TOPOLOGY_PKG = _project_root / "src" / "boomi_mcp" / "compiler" / "system_topology"


def _spec(relations):
    return parse_system_topology_v1(
        {
            "version": "1",
            "profile_ref": "prof",
            "objects": [
                {"kind": "process", "key": "top", "component_ref": "$ref:k_top"},
                {"kind": "process", "key": "mid", "component_ref": "$ref:k_mid"},
                {"kind": "process", "key": "leaf", "component_ref": "$ref:k_leaf"},
            ],
            "relations": list(relations),
        }
    )


def _context(build_dependencies):
    """A context whose only variable is the COMPONENT PLAN's build graph."""
    return TopologyResolutionContextV1(
        profile="prof",
        component_plan_symbols=(
            ComponentPlanSymbolV1(
                component_key="k_top",
                component_type="process",
                has_process_ir=True,
                materialization_dependencies=build_dependencies,
            ),
            ComponentPlanSymbolV1(
                component_key="k_mid", component_type="process", has_process_ir=True
            ),
            ComponentPlanSymbolV1(
                component_key="k_leaf", component_type="process", has_process_ir=True
            ),
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


_CALLS = [
    {"kind": "process_call", "key": "r1", "caller_process": "top", "callee_process": "mid"},
    {"kind": "process_call", "key": "r2", "caller_process": "mid", "callee_process": "leaf"},
]


# ---------------------------------------------------------------------------
# 1. Vocabulary
# ---------------------------------------------------------------------------


def _property_names(node, out=None):
    out = set() if out is None else out
    if isinstance(node, dict):
        for key, value in node.items():
            if key == "properties" and isinstance(value, dict):
                out.update(str(k).lower() for k in value)
            if key != "description":
                _property_names(value, out)
    elif isinstance(node, list):
        for item in node:
            _property_names(item, out)
    return out


def test_the_authored_schema_has_no_build_dependency_field():
    """A caller cannot express a ComponentPlan build edge in a topology document."""
    names = _property_names(system_topology_v1_json_schema())
    for forbidden in ("depends_on", "dependencies", "materialization_dependencies"):
        assert forbidden not in names, forbidden


def test_the_authored_schema_has_no_cfg_or_layout_field():
    names = _property_names(system_topology_v1_json_schema())
    for forbidden in (
        "edge_id",
        "source_node_id",
        "target_node_id",
        "cfg_node_id",
        "internal_node_id",
        "dragpoint",
        "layout",
        "shape_id",
        "steps",
        "body",
        "terminal",
    ):
        assert forbidden not in names, forbidden


def test_the_vocabulary_scan_sees_the_fields_that_do_exist():
    """Positive control: a scanner returning nothing passes every test above."""
    names = _property_names(system_topology_v1_json_schema())
    assert {"kind", "key", "component_ref", "caller_process", "callee_process"} <= names


def test_a_processir_payload_fails_topology_validation():
    """A CFG-shaped document is not a topology document, structurally."""
    from boomi_mcp.models.system_topology import SystemTopologyValidationError

    import pytest

    with pytest.raises(SystemTopologyValidationError):
        parse_system_topology_v1(
            {
                "version": "1",
                "profile_ref": "prof",
                "objects": [
                    {"kind": "sequence", "steps": [{"kind": "message", "text": "x"}]}
                ],
                "relations": [],
            }
        )


# ---------------------------------------------------------------------------
# 2. Import isolation
# ---------------------------------------------------------------------------


def test_the_runtime_graph_module_does_not_import_the_processir_compiler():
    source = (_TOPOLOGY_PKG / "dependencies.py").read_text()
    for line in source.splitlines():
        stripped = line.strip()
        if stripped.startswith(("import ", "from ")):
            assert "process_ir" not in stripped, stripped


def test_no_topology_module_imports_the_processir_compiler():
    """Sibling packages, not layers — checked across the whole package."""
    for path in sorted(_TOPOLOGY_PKG.glob("*.py")):
        for line in path.read_text().splitlines():
            stripped = line.strip()
            if stripped.startswith(("import ", "from ")):
                assert "compiler.process_ir" not in stripped, (path.name, stripped)
                assert not stripped.startswith("from ..process_ir"), (
                    path.name,
                    stripped,
                )


def test_importing_the_topology_planner_does_not_load_the_processir_compiler():
    """Checked in a FRESH subprocess — this process already imported both."""
    code = (
        "import sys; import boomi_mcp.compiler.system_topology as t; "
        "mods=[m for m in sys.modules if 'compiler.process_ir' in m]; "
        "print('RESULT:' + repr(mods))"
    )
    result = subprocess.run(
        [sys.executable, "-c", code],
        capture_output=True,
        text=True,
        env=dict(os.environ, PYTHONPATH=_src),
        cwd=_project_root,
    )
    assert result.returncode == 0, result.stderr
    assert "RESULT:[]" in result.stdout, result.stdout


def test_importing_the_topology_planner_does_not_import_the_server():
    code = (
        "import sys; import boomi_mcp.compiler.system_topology; "
        "print('RESULT:' + repr('server' in sys.modules))"
    )
    result = subprocess.run(
        [sys.executable, "-c", code],
        capture_output=True,
        text=True,
        env=dict(os.environ, PYTHONPATH=_src),
        cwd=_project_root,
    )
    assert result.returncode == 0, result.stderr
    assert "RESULT:False" in result.stdout, result.stdout


# ---------------------------------------------------------------------------
# 3. Byte independence — the proof that would catch a real leak
# ---------------------------------------------------------------------------


def test_changing_the_build_graph_leaves_the_runtime_order_byte_identical():
    """ComponentPlan build edges must not reach the runtime order.

    Two contexts differing ONLY in ``materialization_dependencies``. If the
    planner ever started folding build edges into the ProcessCall graph, this is
    where it would show.
    """
    spec = _spec(_CALLS)
    plan_a = plan_system_topology(spec, _context(()))
    plan_b = plan_system_topology(spec, _context(("k_leaf", "k_mid")))
    assert plan_a.runtime_process_order == plan_b.runtime_process_order
    assert (
        json.dumps(plan_a.runtime_process_order.model_dump(mode="json"), sort_keys=True)
        == json.dumps(
            plan_b.runtime_process_order.model_dump(mode="json"), sort_keys=True
        )
    )


def test_changing_the_call_graph_leaves_the_prerequisites_byte_identical():
    """ProcessCall edges must not reach the build prerequisites."""
    context = _context(())
    plan_a = plan_system_topology(_spec(_CALLS), context)
    plan_b = plan_system_topology(_spec([]), context)
    assert (
        plan_a.executable_component_prerequisites
        == plan_b.executable_component_prerequisites
    )


def test_a_build_dependency_never_appears_in_a_plan_payload():
    """``materialization_dependencies`` is carried in the CONTEXT and reported
    nowhere: a prerequisite states what to build, not in what order."""
    plan = plan_system_topology(_spec(_CALLS), _context(("k_leaf", "k_mid")))
    blob = canonical_topology_plan_json(plan)
    assert "materialization_dependencies" not in blob
    assert "depends_on" not in blob


def test_the_three_namespaces_are_structurally_distinct():
    plan = plan_system_topology(_spec(_CALLS), _context(()))
    assert plan.runtime_process_order.namespace == "topology_runtime"
    assert plan.runtime_process_order.basis == "process_call"
    for relation in plan.planning_only_relations:
        assert relation.namespace == "system_topology"
    for prerequisite in plan.executable_component_prerequisites:
        assert prerequisite.owner == "component_plan"


def test_the_namespace_literals_cannot_be_overridden():
    """``Literal`` types, not defaulted strings — unconstructible otherwise."""
    import pytest
    from pydantic import ValidationError

    from boomi_mcp.compiler.system_topology.contracts import (
        ComponentPlanPrerequisiteV1,
        PlannedTopologyRelationV1,
        TopologyRuntimeOrderV1,
    )

    with pytest.raises(ValidationError):
        TopologyRuntimeOrderV1(namespace="system_topology")  # type: ignore[arg-type]
    with pytest.raises(ValidationError):
        TopologyRuntimeOrderV1(basis="document_cache_use")  # type: ignore[arg-type]
    with pytest.raises(ValidationError):
        PlannedTopologyRelationV1(
            namespace="topology_runtime",  # type: ignore[arg-type]
            relation_key="k",
            relation_kind="process_call",
            witness="process_ir",
        )
    with pytest.raises(ValidationError):
        ComponentPlanPrerequisiteV1(
            owner="system_topology",  # type: ignore[arg-type]
            component_key="k",
            component_type="process",
        )


def test_non_call_relations_never_enter_the_runtime_graph():
    """Cache/property/schedule/deployment/route edges are real but not orderings."""
    spec = parse_system_topology_v1(
        {
            "version": "1",
            "profile_ref": "prof",
            "objects": [
                {"kind": "process", "key": "top", "component_ref": "$ref:k_top"},
                {"kind": "process", "key": "mid", "component_ref": "$ref:k_mid"},
                {"kind": "process", "key": "leaf", "component_ref": "$ref:k_leaf"},
                {"kind": "document_cache", "key": "c", "component_ref": "$ref:k_cache"},
            ],
            "relations": [
                # leaf and top are connected only by a CACHE edge, so neither
                # constrains the other's position.
                {"kind": "document_cache_use", "key": "rc1", "process": "top", "document_cache": "c"},
                {"kind": "document_cache_use", "key": "rc2", "process": "leaf", "document_cache": "c"},
            ],
        }
    )
    from boomi_mcp.compiler.system_topology.context import SharedResourceUseEvidenceV1

    context = TopologyResolutionContextV1(
        profile="prof",
        component_plan_symbols=(
            ComponentPlanSymbolV1(
                component_key="k_top", component_type="process", has_process_ir=True
            ),
            ComponentPlanSymbolV1(
                component_key="k_mid", component_type="process", has_process_ir=True
            ),
            ComponentPlanSymbolV1(
                component_key="k_leaf", component_type="process", has_process_ir=True
            ),
            ComponentPlanSymbolV1(component_key="k_cache", component_type="documentcache"),
        ),
        shared_resource_use_evidence=(
            SharedResourceUseEvidenceV1(
                process_component_ref="$ref:k_top",
                resource_component_ref="$ref:k_cache",
                resource_kind="document_cache",
                witness="process_ir",
            ),
            SharedResourceUseEvidenceV1(
                process_component_ref="$ref:k_leaf",
                resource_component_ref="$ref:k_cache",
                resource_kind="document_cache",
                witness="process_ir",
            ),
        ),
    )
    plan = plan_system_topology(spec, context)
    assert plan.blockers == ()
    # With no ProcessCall at all, the order is a pure lexical listing — nothing
    # in it was derived from the cache edges.
    assert plan.runtime_process_order.order == ("leaf", "mid", "top")


def test_a_cache_cycle_is_not_a_dependency_cycle():
    """Two processes sharing a cache is normal, not a cycle."""
    from boomi_mcp.compiler.system_topology.dependencies import (
        collect_dependency_findings,
    )

    spec = parse_system_topology_v1(
        {
            "version": "1",
            "profile_ref": "prof",
            "objects": [
                {"kind": "process", "key": "a", "component_ref": "$ref:ka"},
                {"kind": "process", "key": "b", "component_ref": "$ref:kb"},
                {"kind": "document_cache", "key": "c", "component_ref": "$ref:kc"},
            ],
            "relations": [
                {"kind": "document_cache_use", "key": "r1", "process": "a", "document_cache": "c"},
                {"kind": "document_cache_use", "key": "r2", "process": "b", "document_cache": "c"},
            ],
        }
    )
    assert collect_dependency_findings(spec) == ()
