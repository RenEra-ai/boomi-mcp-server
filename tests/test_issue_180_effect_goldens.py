"""Issue #180 — the effect-declaration channel, emitted and pinned.

#154 shipped the channel and proved it at the resolver, the compiler and the
emitter. What it did not have was a byte golden in which a DECLARATION is the
subject, and building one is what found the defect these goldens now pin: the
apply-time recompile in ``materialize_canonical_process_xml`` was never given
the trusted context, so a root whose declaration turned a blocking finding into
a warning planned clean, compiled clean, and then failed at materialization.
The channel stopped at compile and never reached apply.

Each case is chosen so it cannot compile at all without its declaration — a map
whose write a later step reads, a Branch leg reading what an earlier leg's
``process_call`` established, and a ``cache_get`` over a cache nothing writes.
Remove the declaration and there are no bytes to freeze, which is what makes
these goldens about the channel rather than about the emitter.

Oracle provenance is recorded in
``tests/fixtures/process_ir/issue180/PROVENANCE.md``. The bytes are regression
pins, not oracles: what stands behind them is graph verification, the compiler's
own invariants, determinism across isolated renders, and — uniquely here — the
mutation control in ``test_the_apply_recompile_needs_the_context_the_plan_carries``,
which shows the pre-fix spelling still fails on the very document the golden
freezes.
"""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

_HERE = Path(__file__).resolve().parent
if str(_HERE) not in sys.path:
    sys.path.insert(0, str(_HERE))
_SRC = str(_HERE.parent / "src")
if _SRC not in sys.path:
    sys.path.insert(0, _SRC)

import _wave_gate_golden_corpus as corpus  # noqa: E402

from boomi_mcp.categories.components.process_graph_verifier import (  # noqa: E402
    verify_process_graph,
)

_CASES = (
    "map_declared_effect",
    "subprocess_declared_effect",
    "external_writer_declared_effect",
)
_RENDERER = "process-component-v1"
_GOLDENS = _HERE / "fixtures" / "golden_xml"


def _render(case):
    rendered = corpus.render_golden_case("issue180:" + case, _RENDERER)
    return rendered if isinstance(rendered, bytes) else rendered.encode("utf-8")


@pytest.mark.parametrize("case", _CASES)
def test_golden_bytes_are_exact(case):
    """RAW bytes. No canonicalisation, no re-parse — a comparison that
    normalised first could not see an ordering or whitespace regression."""
    assert _render(case) == (
        _GOLDENS / "issue180_{0}.xml".format(case)).read_bytes()


@pytest.mark.parametrize("case", _CASES)
def test_emitted_graph_verifies(case):
    report = verify_process_graph(_render(case).decode("utf-8"))
    assert report["errors"] == [], (case, report["errors"])


@pytest.mark.parametrize("case", _CASES)
def test_emission_is_deterministic(case):
    """Two renders in one process. The wave gate additionally renders every
    case in two isolated children under different hash seeds; this is the cheap
    per-commit half of the same property."""
    assert _render(case) == _render(case)


@pytest.mark.parametrize("case", _CASES)
def test_every_case_is_registered_in_the_golden_manifest(case):
    """A golden the manifest does not declare is not gated by the wave gate.

    Read from the manifest through the gate's OWN parser rather than
    `json.loads` per line, so this cannot pass on a file the gate would reject.
    """
    import importlib.util

    root = _HERE.parent
    spec = importlib.util.spec_from_file_location(
        "_wave_gate_module_issue180", root / "scripts" / "wave_gate.py")
    gate = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(gate)
    manifest = gate.parse_manifest(
        (root / gate.GOLDENS_MANIFEST).read_bytes(), "goldens")

    rows = [row for row in manifest.active
            if row["input_case"] == "issue180:" + case]
    assert len(rows) == 1, (case, rows)
    assert rows[0]["expected_file"] == (
        "tests/fixtures/golden_xml/issue180_{0}.xml".format(case))
    assert rows[0]["renderer"] == _RENDERER
    assert rows[0]["owner"] == "#180"


def _declaration_gated_plan():
    """The map case's request, compiled through the PUBLIC entry.

    Returns `(plan, bound_symbols)` — the exact pair
    `materialize_canonical_process_xml` works with, so the mutation below is of
    the real call and not of a re-creation of it.
    """
    from boomi_mcp.authoring.workflow import (
        _connector_metadata_from_components,
        _normalize_intent,
        compile_authoring_request_v1,
    )
    from boomi_mcp.categories.components.canonical_process_apply import (
        _ref_key,
        bind_symbols_to_applied_ids,
    )
    from boomi_mcp.models.authoring_workflow import (
        AuthoringRequestV1,
        ProcessIRAuthoringIntentV1,
        ProcessIREffectDeclarationsV1,
        ProcessIRMapEffectDeclarationV1,
        ProcessIRStateEffectDeclarationV1,
        ProcessIRStateReferenceV1,
    )
    from boomi_mcp.models.process_component import (
        ProcessAuthoringUnitV1,
        ProcessComponentEnvelopeV1,
    )
    from boomi_mcp.models.process_ir import parse_process_ir_v1
    from boomi_mcp.recipes.materialization import build_symbol_table

    conn = {"key": "conn", "type": "connector-settings", "name": "M12.16 conn",
            "action": "create",
            "config": {"connector_type": "rest", "component_name": "M12.16 conn",
                       "base_url": "https://orders.example.invalid", "auth": "NONE"}}
    op = {"key": "op", "type": "connector-action", "name": "M12.16 op",
          "action": "create", "depends_on": ["conn"],
          "config": {"connector_type": "rest", "operation_mode": "execute",
                     "component_name": "M12.16 op", "connection_ref_key": "conn",
                     "method": "GET", "path": "/v1/things"}}

    def profile(key):
        return {"key": key, "type": "profile.json", "name": key, "action": "create",
                "config": {"component_type": "profile.json",
                           "profile_type": "json.generated", "component_name": key,
                           "root": {"name": "root", "kind": "object", "children": [
                               {"name": "a", "kind": "simple",
                                "data_type": "character"}]}}}

    unit = ProcessAuthoringUnitV1(
        envelope=ProcessComponentEnvelopeV1(
            component_key="proc", name="M12.16 Effect Map", action="create",
            depends_on=("conn", "map", "op", "sp", "tp")),
        process_ir=parse_process_ir_v1({
            "version": "1", "body": {"kind": "sequence", "steps": [
                {"kind": "source", "connection_ref": "$ref:conn",
                 "operation_ref": "$ref:op"},
                {"kind": "map_ref", "map_ref": "$ref:map"},
                {"kind": "set_dpp", "name": "ECHO", "source_values": [
                    {"value_type": "dpp", "property_name": "OUT"}]},
                {"kind": "return_documents"}]}}))
    request = AuthoringRequestV1(
        intent=ProcessIRAuthoringIntentV1(
            integration_name="M12.16 Effect Integration", units=(unit,),
            components=(conn, op, {
                "key": "map", "type": "transform.map", "name": "M12.16 map",
                "action": "create", "depends_on": ["sp", "tp"],
                "config": {"component_name": "M12.16 map", "map_type": "function",
                           "source_profile_id": "$ref:sp",
                           "source_profile_type": "profile.json",
                           "target_profile_id": "$ref:tp",
                           "target_profile_type": "profile.json",
                           "function_mappings": [{
                               "function_type": "dynamic_process_property_set",
                               "inputs": ["root/a"],
                               "parameters": {"property_name": "OUT"}}]}},
                profile("sp"), profile("tp")),
            conflict_policy="fail"),
        effect_declarations=ProcessIREffectDeclarationsV1(map_effects=(
            ProcessIRMapEffectDeclarationV1(
                map_ref="$ref:map",
                effect=ProcessIRStateEffectDeclarationV1(
                    reads=(),
                    writes=(ProcessIRStateReferenceV1(scope="dpp", name="OUT"),),
                    replay_safe=True)),)))

    _result, internals = compile_authoring_request_v1(
        request, profile="golden", account_id="golden")
    plan = internals.materialization_plans["proc"]
    spec = _normalize_intent(request).integration_spec
    symbols = build_symbol_table(
        list(spec.components),
        process_keys=[u.envelope.component_key for u in spec.processes],
        connector_metadata=_connector_metadata_from_components(spec.components))
    bound = bind_symbols_to_applied_ids(
        symbols,
        {"conn": "golden-conn-id", "op": "golden-op-id", "map": "golden-map-id",
         "sp": "golden-sp-id", "tp": "golden-tp-id"},
        "proc",
        required_keys={_ref_key(slot.ref) for slot in plan.unresolved_symbol_slots})
    return plan, bound


def test_the_apply_recompile_needs_the_context_the_plan_carries():
    """The mutation control for the #180 production fix.

    `materialize_canonical_process_xml` recompiles the plan's root against real
    ids. Before this slice that recompile passed no capabilities, so it asked a
    strict question the plan's own compile had not been asked — and every
    declaration-gated root died there. The mutant IS the pre-fix line, run on
    the very document the map golden freezes.

    The plan is asserted to actually CARRY a context first: without that the
    "mutant" and the fixed call would be the same call, and this test would pass
    while proving nothing.
    """
    from boomi_mcp.compiler.process_ir.diagnostics import ProcessIRCompileError
    from boomi_mcp.compiler.process_ir.pipeline import compile_process_ir_v1

    plan, bound = _declaration_gated_plan()

    # The mutation can only take effect if there is a context to withhold.
    assert plan.effect_capabilities is not None
    assert plan.effect_capabilities.map_effect("$ref:map").writes == (
        ("dpp", "OUT"),), plan.effect_capabilities

    # MUTANT — the pre-fix spelling.
    with pytest.raises(ProcessIRCompileError) as caught:
        compile_process_ir_v1(plan.process_ir, bound)
    assert [d.code for d in caught.value.diagnostics] == [
        "PROCESS_IR_SEMANTIC_LINEAGE_PROPERTY_READ_BEFORE_WRITE"], caught.value

    # ...and the shipped spelling, which is what the golden renders through.
    cfg, emission_plan = compile_process_ir_v1(
        plan.process_ir, bound, capabilities=plan.effect_capabilities)
    assert cfg is not None and emission_plan is not None


def test_the_recorded_context_is_covered_by_the_plan_fingerprint():
    """A plan compiled under a declaration is not the same plan as one compiled
    without, and the digest has to say so.

    Excluding the field would have kept every existing fingerprint stable, which
    is exactly why it is worth asserting: the cheap choice is the wrong one
    here. Withholding the context changes the material, so a swapped plan is
    caught by the fingerprint check apply already runs.
    """
    from boomi_mcp.authoring.process_materialization import (
        canonical_plan_material,
        covered_plan_fields,
        process_plan_fingerprint,
    )

    assert "effect_capabilities" in covered_plan_fields()

    plan, _bound = _declaration_gated_plan()
    recomputed, material = process_plan_fingerprint(plan)
    assert recomputed == plan.plan_fingerprint
    assert b"effect_capabilities" in material

    stripped = plan.model_copy(update={"effect_capabilities": None})
    assert canonical_plan_material(stripped) != material
    assert process_plan_fingerprint(stripped)[0] != plan.plan_fingerprint
