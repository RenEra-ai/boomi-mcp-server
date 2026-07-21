"""Unit tests for the typed ProcessIR process-emitter registry (issue #138 M12.3).

These exercise the registry's *contract* — completeness against the closed
``EmitterInputV1`` discriminator, fail-closed preflight (unknown kind, bad input,
missing/wrong-type symbols, outgoing cardinality), determinism, and the isolation
guarantees (no legacy config, no integration-builder coupling). Byte parity
against the legacy builder lives in ``test_process_emitter_parity.py``.
"""

from __future__ import annotations

import ast
import sys
from pathlib import Path

import pytest

_ROOT = str(Path(__file__).resolve().parent.parent)
if _ROOT not in sys.path:
    sys.path.insert(0, _ROOT)

from boomi_mcp.compiler.process_ir import emitter_registry as R
from boomi_mcp.compiler.process_ir.contracts import (
    ConnectorActionInputV1,
    DataProcessInputV1,
    DocCacheRetrieveInputV1,
    EmissionLayoutV1,
    EmissionNodeV1,
    EmissionPlanV1,
    EmissionTransitionV1,
    ComponentSymbolV1,
    MapInputV1,
    StartNoActionInputV1,
    StopInputV1,
    SymbolTableV1,
)
from boomi_mcp.compiler.process_ir.diagnostics import ProcessIRCompileError
from boomi_mcp.errors import (
    PROCESS_IR_COMPILE_EMITTER_INPUT_INVALID,
    PROCESS_IR_COMPILE_INTERNAL,
    PROCESS_IR_COMPILE_SYMBOL_UNRESOLVED,
)

_MODULE_SRC = (
    Path(R.__file__).read_text()
)


# ---------------------------------------------------------------------------
# Minimal plan builders
# ---------------------------------------------------------------------------


def _t(shape_index, point, to_shape, *, identifier=None, text=None, y=104.0):
    return EmissionTransitionV1(
        local_ordinal=point,
        dragpoint_name=f"shape{shape_index}.dragpoint{point}",
        to_shape_id=to_shape,
        x=96.0 + (shape_index - 1) * 160.0 + 144.0,
        y=y,
        identifier=identifier,
        text=text,
        provenance="cfg_edge",
    )


def _node(ordinal, emitter_input, *, outgoing=(), origin="ir", role=None):
    return EmissionNodeV1(
        ordinal=ordinal,
        shape_id=f"shape{ordinal}",
        origin=origin,
        synthetic_role=role,
        emitter_input=emitter_input,
        layout=EmissionLayoutV1(x=96.0 + (ordinal - 1) * 160.0, y=96.0),
        outgoing=tuple(outgoing),
    )


def _linear_map_plan(map_id="MAP", *, map_outgoing=None):
    """start -> map -> stop, with the map's single outgoing configurable."""
    if map_outgoing is None:
        map_outgoing = (_t(2, 1, "shape3"),)
    start = _node(1, StartNoActionInputV1(), outgoing=(_t(1, 1, "shape2"),), origin="synthetic", role="start")
    mapp = _node(2, MapInputV1(map_id=map_id, userlabel="m"), outgoing=map_outgoing)
    stop = _node(3, StopInputV1(continue_=True), outgoing=(), origin="synthetic", role="terminal_stop")
    return EmissionPlanV1(
        entry_shape_id="shape1",
        nodes=(start, mapp, stop),
        terminal_shape_ids=("shape3",),
    )


def _map_symbols(component_id="MAP", component_type="transform.map"):
    return SymbolTableV1(
        symbols=(ComponentSymbolV1(ref="m", component_id=component_id, component_type=component_type),)
    )


# ---------------------------------------------------------------------------
# Completeness / registration metadata
# ---------------------------------------------------------------------------


def test_registry_covers_discriminator_exactly():
    # 16 model classes, 17 discriminator keys (connector source + target share a model).
    assert R.registry_keys() == R.discriminator_keys()
    assert len(R.registry_keys()) == 17


def test_connector_roles_share_one_renderer():
    src = R.registration_for("connectoraction_source")
    tgt = R.registration_for("connectoraction_target")
    assert src.emit is tgt.emit
    assert src.input_type is ConnectorActionInputV1 is tgt.input_type
    assert src.produced_shape_type == tgt.produced_shape_type == "connectoraction"


def test_every_registration_declares_capability_and_shape():
    for kind in R.registry_keys():
        reg = R.registration_for(kind)
        assert reg.supported_capability == R.CAPABILITY_PROCESS_IR_V1
        assert reg.produced_shape_type
        assert reg.emitter_kind == kind


def test_duplicate_registration_rejected():
    reg = R.registration_for("map")
    with pytest.raises(ValueError, match="duplicate emitter registration"):
        R._build_registry((reg, reg))


def test_registration_for_unknown_kind_is_none():
    assert R.registration_for("no_such_kind") is None
    assert R.registration_for("emit_fragment") is None


def test_non_ir_shapes_are_absent_from_the_registry():
    # emit_fragment stays out of the canonical path; listener-start, catcherrors,
    # notify and route are legacy-only / verifier-only, never registry kinds.
    for absent in ("emit_fragment", "start_listen", "catcherrors", "notify", "route"):
        assert absent not in R.registry_keys()


# ---------------------------------------------------------------------------
# Happy path + determinism
# ---------------------------------------------------------------------------


def test_emit_process_linear_map_ok():
    art = R.emit_process(_linear_map_plan(), _map_symbols())
    assert len(art.shape_xml_parts) == 3
    assert art.shape_xml_parts[1].startswith('<shape image="map_icon" name="shape2"')
    assert art.verifier.errors == ()
    assert art.verifier.shapes_checked == 3


def test_emit_process_is_deterministic():
    plan, symbols = _linear_map_plan(), _map_symbols()
    a = R.emit_process(plan, symbols)
    b = R.emit_process(plan, symbols)
    assert a.shape_xml_parts == b.shape_xml_parts
    assert a.process_xml == b.process_xml


def test_output_order_is_plan_order_not_registry_order():
    art = R.emit_process(_linear_map_plan(), _map_symbols())
    names = [p.split('name="')[1].split('"')[0] for p in art.shape_xml_parts]
    assert names == ["shape1", "shape2", "shape3"]


def test_symbol_table_input_order_does_not_affect_output():
    plan = _linear_map_plan()
    forward = SymbolTableV1(symbols=(
        ComponentSymbolV1(ref="a", component_id="OTHER", component_type="process"),
        ComponentSymbolV1(ref="m", component_id="MAP", component_type="transform.map"),
    ))
    reverse = SymbolTableV1(symbols=(
        ComponentSymbolV1(ref="m", component_id="MAP", component_type="transform.map"),
        ComponentSymbolV1(ref="a", component_id="OTHER", component_type="process"),
    ))
    assert R.emit_process(plan, forward).process_xml == R.emit_process(plan, reverse).process_xml


# ---------------------------------------------------------------------------
# Symbol resolution
# ---------------------------------------------------------------------------


def test_missing_symbol_fails_closed():
    with pytest.raises(ProcessIRCompileError) as exc:
        R.emit_process(_linear_map_plan(), SymbolTableV1(symbols=()))
    codes = [d.code for d in exc.value.diagnostics]
    assert PROCESS_IR_COMPILE_SYMBOL_UNRESOLVED in codes
    assert all(d.phase == "reference_resolution" for d in exc.value.diagnostics)


def test_wrong_type_symbol_fails_closed():
    with pytest.raises(ProcessIRCompileError) as exc:
        R.emit_process(_linear_map_plan(), _map_symbols(component_type="process"))
    assert [d.code for d in exc.value.diagnostics] == [PROCESS_IR_COMPILE_SYMBOL_UNRESOLVED]


def test_duplicate_component_id_with_one_compatible_alias_resolves():
    # Two refs share one component id; only one carries the required type.
    symbols = SymbolTableV1(symbols=(
        ComponentSymbolV1(ref="m", component_id="MAP", component_type="other"),
        ComponentSymbolV1(ref="m2", component_id="MAP", component_type="transform.map"),
    ))
    art = R.emit_process(_linear_map_plan(), symbols)
    assert art.verifier.errors == ()


# ---------------------------------------------------------------------------
# Outgoing cardinality + preconditions
# ---------------------------------------------------------------------------


def test_linear_shape_requires_exactly_one_outgoing():
    plan = _linear_map_plan(map_outgoing=())  # map with zero outgoing
    with pytest.raises(ProcessIRCompileError) as exc:
        R.emit_process(plan, _map_symbols())
    assert PROCESS_IR_COMPILE_EMITTER_INPUT_INVALID in [d.code for d in exc.value.diagnostics]


def test_linear_shape_rejects_two_outgoing():
    plan = _linear_map_plan(map_outgoing=(_t(2, 1, "shape3"), _t(2, 2, "shape3")))
    with pytest.raises(ProcessIRCompileError) as exc:
        R.emit_process(plan, _map_symbols())
    assert PROCESS_IR_COMPILE_EMITTER_INPUT_INVALID in [d.code for d in exc.value.diagnostics]


def test_doccacheretrieve_precondition_load_all_documents():
    node = _node(
        2,
        DocCacheRetrieveInputV1(
            document_cache_id="CID",
            empty_cache_behavior="stopprocess",
            load_all_documents=False,  # violates the precondition
        ),
        outgoing=(_t(2, 1, "shape3"),),
    )
    start = _node(1, StartNoActionInputV1(), outgoing=(_t(1, 1, "shape2"),), origin="synthetic", role="start")
    stop = _node(3, StopInputV1(), origin="synthetic", role="terminal_stop")
    plan = EmissionPlanV1(entry_shape_id="shape1", nodes=(start, node, stop), terminal_shape_ids=("shape3",))
    symbols = SymbolTableV1(symbols=(ComponentSymbolV1(ref="c", component_id="CID", component_type="documentcache"),))
    with pytest.raises(ProcessIRCompileError) as exc:
        R.emit_process(plan, symbols)
    assert PROCESS_IR_COMPILE_EMITTER_INPUT_INVALID in [d.code for d in exc.value.diagnostics]


# ---------------------------------------------------------------------------
# Data Process: non-empty steps + profile-kind must match the resolved symbol
# ---------------------------------------------------------------------------


def _dataprocess_plan(dp_input, *, outgoing=None):
    if outgoing is None:
        outgoing = (_t(2, 1, "shape3"),)
    start = _node(1, StartNoActionInputV1(), outgoing=(_t(1, 1, "shape2"),), origin="synthetic", role="start")
    dp = _node(2, dp_input, outgoing=outgoing)
    stop = _node(3, StopInputV1(), origin="synthetic", role="terminal_stop")
    return EmissionPlanV1(entry_shape_id="shape1", nodes=(start, dp, stop), terminal_shape_ids=("shape3",))


_SPLIT_XML_STEP = {
    "operation": "split_documents",
    "key": 1,
    "index": 1,
    "profile_type": "xml",
    "profile_id": "PROF",
    "link_element_key": "k",
    "link_element_name": "n",
}


def test_empty_dataprocess_is_rejected():
    plan = _dataprocess_plan(DataProcessInputV1(steps=(), userlabel="dp"))
    with pytest.raises(ProcessIRCompileError) as exc:
        R.emit_process(plan, SymbolTableV1(symbols=()))
    assert PROCESS_IR_COMPILE_EMITTER_INPUT_INVALID in [d.code for d in exc.value.diagnostics]


def test_dataprocess_profile_kind_must_match_symbol_type():
    plan = _dataprocess_plan(DataProcessInputV1(steps=(_SPLIT_XML_STEP,), userlabel="dp"))
    # An xml-declared step whose symbol is only profile.json must NOT resolve.
    json_syms = SymbolTableV1(symbols=(
        ComponentSymbolV1(ref="p", component_id="PROF", component_type="profile.json"),
    ))
    with pytest.raises(ProcessIRCompileError) as exc:
        R.emit_process(plan, json_syms)
    assert PROCESS_IR_COMPILE_SYMBOL_UNRESOLVED in [d.code for d in exc.value.diagnostics]
    # The matching profile.xml symbol resolves and emits clean.
    xml_syms = SymbolTableV1(symbols=(
        ComponentSymbolV1(ref="p", component_id="PROF", component_type="profile.xml"),
    ))
    assert R.emit_process(plan, xml_syms).verifier.errors == ()


def test_resolved_symbols_are_actual_component_symbols():
    # EmitterContext.resolved_symbols must carry the resolved ComponentSymbolV1
    # objects, not the SymbolRequirement descriptors.
    idx = R._component_symbol_index(_map_symbols())
    diags, reg, narrowed = R._preflight_node(
        _linear_map_plan().nodes[1], idx, R.CAPABILITY_PROCESS_IR_V1
    )
    assert diags == []
    assert narrowed and all(isinstance(s, ComponentSymbolV1) for s in narrowed)
    assert [s.component_id for s in narrowed] == ["MAP"]


# ---------------------------------------------------------------------------
# Whole-plan fail-closed: a bad later node blocks ALL emission
# ---------------------------------------------------------------------------


def test_bad_later_node_blocks_all_rendering(monkeypatch):
    calls = []
    real = R.rendering.render_start_noaction
    monkeypatch.setattr(
        R.rendering, "render_start_noaction", lambda ctx: calls.append("start") or real(ctx)
    )
    # A valid start, then a map whose symbol is missing → whole plan aborts in
    # preflight, so NO renderer runs at all.
    with pytest.raises(ProcessIRCompileError):
        R.emit_process(_linear_map_plan(), SymbolTableV1(symbols=()))
    assert calls == []


def test_renderer_exception_becomes_internal(monkeypatch):
    def boom(*a, **k):
        raise RuntimeError("secret internal detail")

    monkeypatch.setattr(R.rendering, "render_map", boom)
    with pytest.raises(ProcessIRCompileError) as exc:
        R.emit_process(_linear_map_plan(), _map_symbols())
    assert [d.code for d in exc.value.diagnostics] == [PROCESS_IR_COMPILE_INTERNAL]
    # value-free: the secret text never leaks into the diagnostic
    assert "secret" not in str(exc.value)


# ---------------------------------------------------------------------------
# Isolation guarantees (AST/import guards)
# ---------------------------------------------------------------------------


def test_registry_does_not_import_legacy_config_or_integration_builder():
    tree = ast.parse(_MODULE_SRC)
    imported = []
    for node in ast.walk(tree):
        if isinstance(node, ast.ImportFrom):
            imported.append(node.module or "")
            imported.extend(a.name for a in node.names)
        elif isinstance(node, ast.Import):
            imported.extend(a.name for a in node.names)
    joined = " ".join(imported)
    for forbidden in (
        "process_flow_builder",
        "integration_builder",
        "IntegrationSpecV1",
        "PipelineSpec",
        "pipeline_models",
    ):
        assert forbidden not in joined, f"registry must not import {forbidden!r}"


def test_registry_reuses_the_shared_renderers_not_a_second_copy():
    # The registry emits through process_emitters.rendering — the SAME module the
    # legacy builder's adapters call — so there is exactly one template copy.
    assert "process_emitters" in _MODULE_SRC
    assert R.rendering.__name__.endswith("process_emitters.rendering")
