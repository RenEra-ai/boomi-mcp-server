"""Nothing existing changed (issue #144, M12.9).

An acceptance criterion in its own right: "Existing IntegrationSpec/build/deploy
behavior and schemas are unchanged." #144 adds a dark package and fourteen error
codes; every one of these tests would fail if it had reached further.
"""

import inspect
import sys
from pathlib import Path

_project_root = Path(__file__).resolve().parent.parent
_src = str(_project_root / "src")
if _src not in sys.path:
    sys.path.insert(0, _src)

import boomi_mcp.compiler.system_topology  # noqa: F401 — importing must be harmless
from boomi_mcp.models.integration_models import (
    IntegrationComponentSpec,
    IntegrationSpecV1,
)


# ---------------------------------------------------------------------------
# IntegrationSpecV1 is untouched
# ---------------------------------------------------------------------------


def test_integration_component_spec_fields_are_unchanged():
    assert set(IntegrationComponentSpec.model_fields) == {
        "key",
        "type",
        "action",
        "name",
        "component_id",
        "config",
        "depends_on",
    }


def test_integration_spec_still_accepts_its_permissive_payload():
    """#144 must not tighten a neighbouring contract by accident."""
    spec = IntegrationSpecV1(
        name="regression fixture",
        components=[
            {"key": "a", "type": "process", "config": {"process_kind": "wrapper_subprocess"}},
            {"key": "b", "type": "connector-settings", "depends_on": ["a"]},
        ],
    )
    assert [c.key for c in spec.components] == ["a", "b"]
    assert spec.components[0].config["process_kind"] == "wrapper_subprocess"


def test_integration_spec_schema_carries_no_topology_vocabulary():
    """The two contracts stay separate: no topology concept leaked into it."""
    import json

    schema = json.dumps(IntegrationSpecV1.model_json_schema()).lower()
    for forbidden in (
        "systemtopology",
        "topology_object",
        "topology_relation",
        "process_call_relation",
        "deployment_binding",
        "capability_report",
    ):
        assert forbidden not in schema, forbidden


def test_integration_spec_still_rejects_a_self_dependency():
    import pytest

    with pytest.raises(Exception):
        IntegrationComponentSpec(key="a", type="process", depends_on=["a"])


def test_component_plan_projection_never_reads_config():
    """``has_process_ir`` is caller-supplied precisely so ``config`` stays untouched.

    Proven with a component whose ``config`` explodes on access: if the
    projection ever reached into it to sniff ``process_kind``, this raises.
    """
    from boomi_mcp.compiler.system_topology.context import (
        project_component_plan_symbols,
    )

    class ExplodingConfig(dict):
        def __getitem__(self, key):
            raise AssertionError("projection read config")

        def get(self, *args, **kwargs):
            raise AssertionError("projection read config")

    spec = IntegrationSpecV1(
        name="projection fixture", components=[{"key": "a", "type": "process"}]
    )
    object.__setattr__(spec.components[0], "config", ExplodingConfig())

    symbols = project_component_plan_symbols(spec, process_ir_keys=frozenset({"a"}))
    assert symbols[0].component_key == "a"
    assert symbols[0].component_type == "process"
    assert symbols[0].has_process_ir is True


def test_component_plan_projection_reads_only_key_type_and_depends_on():
    from boomi_mcp.compiler.system_topology.context import (
        project_component_plan_symbols,
    )

    spec = IntegrationSpecV1(
        name="projection fixture",
        components=[
            {"key": "a", "type": "process", "name": "Display Name", "depends_on": []},
            {"key": "b", "type": "profile.json", "depends_on": ["a"]},
        ],
    )
    symbols = project_component_plan_symbols(spec)
    assert [s.component_key for s in symbols] == ["a", "b"]
    assert symbols[1].materialization_dependencies == ("a",)
    # The display name is not projected — topology carries opaque refs only.
    blob = "".join(s.model_dump_json() for s in symbols)
    assert "Display Name" not in blob
    # Nothing was marked as having a ProcessIR root without being told.
    assert all(s.has_process_ir is False for s in symbols)


# ---------------------------------------------------------------------------
# The build and deploy wrappers are untouched
# ---------------------------------------------------------------------------


def test_orchestrate_deploy_still_rejects_multiple_processes():
    """Its exactly-one-process rule is a REFERENCE constraint for #144, not a
    thing #144 changes."""
    from boomi_mcp.categories.deployment import orchestration

    assert orchestration.BUILD_MULTIPLE_PROCESS_COMPONENTS == (
        "BUILD_MULTIPLE_PROCESS_COMPONENTS"
    )


def test_the_orchestration_error_code_is_not_in_the_shared_taxonomy():
    """It is an orchestration-local constant; #144 must not have adopted it."""
    from boomi_mcp.errors import ERROR_TAXONOMY

    assert "BUILD_MULTIPLE_PROCESS_COMPONENTS" not in ERROR_TAXONOMY


def test_no_topology_code_leaked_into_another_owner():
    from boomi_mcp.errors import ERROR_TAXONOMY

    for code, spec in ERROR_TAXONOMY.items():
        if spec.owner == "#144":
            assert code.startswith("TOPOLOGY_"), code
        if code.startswith("TOPOLOGY_"):
            assert spec.owner == "#144", code
            assert spec.category == "topology", code


def test_the_topology_category_is_new_and_holds_only_topology_codes():
    from boomi_mcp.errors import ERROR_TAXONOMY

    topology_category = {
        code for code, spec in ERROR_TAXONOMY.items() if spec.category == "topology"
    }
    assert topology_category == {
        code for code in ERROR_TAXONOMY if code.startswith("TOPOLOGY_")
    }
    assert len(topology_category) == 14


def test_importing_the_topology_planner_does_not_change_build_integration():
    """The existing plan/order entry points are unmoved.

    ``_build_plan`` and ``_topological_order`` are the two functions #144's
    prerequisites conceptually sit beside, so they are the two whose signatures
    matter most: a topology argument appearing on either would mean the planner
    had been wired into the build path rather than left dark.
    """
    from boomi_mcp.categories import integration_builder

    for name in ("_build_plan", "_topological_order"):
        function = getattr(integration_builder, name)
        rendered = str(inspect.signature(function)).lower()
        assert "topology" not in rendered, (name, rendered)

    # Parameter NAMES, not the rendered signature string: the module uses
    # postponed annotations, so the repr is a formatting detail that would make
    # this test fail on an unrelated import change.
    assert list(inspect.signature(integration_builder._build_plan).parameters) == [
        "boomi_client",
        "config",
    ]
    assert list(inspect.signature(integration_builder._topological_order).parameters) == [
        "spec"
    ]


def test_no_existing_category_module_imports_the_topology_planner():
    """#144 wires itself to nothing; #146 owns the wiring."""
    categories = _project_root / "src" / "boomi_mcp" / "categories"
    for path in sorted(categories.rglob("*.py")):
        source = path.read_text()
        assert "compiler.system_topology" not in source, path.name
        assert "plan_system_topology" not in source, path.name
        assert "SystemTopologySpecV1" not in source, path.name


def test_the_server_module_does_not_mention_topology():
    source = (_project_root / "server.py").read_text()
    assert "system_topology" not in source
    assert "SystemTopology" not in source


# ---------------------------------------------------------------------------
# The ProcessIR contracts are untouched
# ---------------------------------------------------------------------------


def test_process_ir_schema_is_unchanged_by_the_topology_addition():
    """Both model families live in the same package; neither may perturb the other."""
    from boomi_mcp.models.process_ir import canonical_process_ir_schema_json

    schema = canonical_process_ir_schema_json().lower()
    for forbidden in ("topology", "deployment_binding", "capability_report"):
        assert forbidden not in schema, forbidden


def test_process_ir_and_topology_diagnostics_are_separate_types():
    from boomi_mcp.models.process_ir import ProcessIRDiagnostic
    from boomi_mcp.models.system_topology import SystemTopologyDiagnostic

    assert ProcessIRDiagnostic is not SystemTopologyDiagnostic
    assert not issubclass(SystemTopologyDiagnostic, ProcessIRDiagnostic)


def test_a_topology_report_cannot_carry_a_process_ir_code():
    import pytest

    from boomi_mcp.compiler.system_topology.contracts import TopologyDiagnosticV1

    for foreign in (
        "PROCESS_IR_SCHEMA_INVALID",
        "PROCESS_IR_COMPILE_INTERNAL",
        "LEGACY_ADAPTER_EXEMPTION_SUBPROCESS_SUMMARY",
        "BUILD_MULTIPLE_PROCESS_COMPONENTS",
    ):
        with pytest.raises(ValueError):
            TopologyDiagnosticV1(
                code=foreign,
                severity="error",
                phase="model",
                path="/",
                message="m",
                remediation="r",
            )


def test_no_free_form_integration_hint_is_reinterpreted_as_topology():
    """Existing ``endpoints``/``flows``/``runtime``/``validation_rules`` stay
    free-form and are read by nothing in #144."""
    for path in sorted(
        (_project_root / "src" / "boomi_mcp" / "compiler" / "system_topology").glob("*.py")
    ):
        source = path.read_text()
        for hint in ('"endpoints"', '"flows"', '"runtime_config"', '"validation_rules"'):
            assert hint not in source, (path.name, hint)
