"""Flow-phase collectors (issue #143, M12.8) — slice 3. Still DARK.

The point of these tests is the difference between the collector and the
compiler assertion it re-derives: ``check_cfg_invariants`` raises on the FIRST
violation, this accumulates ALL of them. A test that only proves "bad input is
rejected" would pass against either, so the discriminating assertions here are
about COUNT and about which families are delegated rather than re-derived.
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

import pytest

_ROOT = Path(__file__).resolve().parent.parent
for _p in (str(_ROOT), str(_ROOT / "src")):
    if _p not in sys.path:
        sys.path.insert(0, _p)

from boomi_mcp.compiler.process_ir.contracts import (
    CfgEdgeV1,
    CfgNodeV1,
    SemanticCfgV1,
    SymbolTableV1,
)
from boomi_mcp.compiler.process_ir.diagnostics import ProcessIRCompileError
from boomi_mcp.compiler.process_ir.semantic_validation.context import (
    PreparedProcessValidationV1,
    prepare_validation_context,
    _edge_index,
)
from boomi_mcp.compiler.process_ir.semantic_validation.flow import (
    collect_connector_flow_findings,
    collect_flow_findings,
    collect_reachability_findings,
    collect_terminal_findings,
)
from boomi_mcp.errors import (
    PROCESS_IR_SEMANTIC_MISSING_TERMINAL,
    PROCESS_IR_SEMANTIC_UNREACHABLE,
)
from boomi_mcp.models.process_ir import parse_process_ir_v1

_FIXTURES = _ROOT / "tests" / "fixtures" / "process_ir"
GOLDEN_DOCS = json.loads((_FIXTURES / "process_ir_v1.json").read_text())


def _prepared(name="linear_flow"):
    return prepare_validation_context(
        parse_process_ir_v1(GOLDEN_DOCS[name]), SymbolTableV1(symbols=())
    )


def _synthetic(nodes, edges, entry="n1", exits=()):
    """A hand-built CFG.

    Built directly rather than lowered because the interesting inputs here are
    malformed, and lowering deliberately cannot produce them. The collectors must
    survive input the compiler would reject — a checker that crashes on a broken
    graph is not a checker.
    """
    cfg = SemanticCfgV1(
        entry_node_id=entry, nodes=tuple(nodes), edges=tuple(edges), exit_node_ids=exits
    )
    return PreparedProcessValidationV1(
        ir=parse_process_ir_v1(GOLDEN_DOCS["linear_flow"]),
        cfg=cfg,
        symbols=SymbolTableV1(symbols=()),
        node_by_id={n.node_id: n for n in cfg.nodes},
        outgoing=_edge_index(cfg.edges, "source_node_id"),
        incoming=_edge_index(cfg.edges, "target_node_id"),
        symbol_by_ref={},
    )


def _node(node_id, ordinal, *, kind="message", exit_role=None, path=None):
    semantic = {"semantic_kind": "message", "text": "t"} if kind == "message" else None
    return CfgNodeV1(
        node_id=node_id,
        ordinal=ordinal,
        source_path=path or "/body/steps/{0}".format(ordinal - 1),
        semantic=semantic,
        exit_role=exit_role,
    )


def _edge(edge_id, ordinal, src, dst, local=1):
    return CfgEdgeV1(
        edge_id=edge_id,
        ordinal=ordinal,
        source_node_id=src,
        target_node_id=dst,
        kind="ordering",
        local_ordinal=local,
        provenance_path="/body/steps/0",
    )


# ---------------------------------------------------------------------------
# reachability — accumulation is the whole point
# ---------------------------------------------------------------------------


def test_a_healthy_flow_has_no_reachability_findings():
    assert collect_reachability_findings(_prepared()) == ()


def test_every_unreachable_node_is_reported_not_just_the_first():
    """The discriminating test: the compiler assertion raises on node #1 and
    stops. A report exists to show all three at once."""
    nodes = [
        _node("n1", 1, exit_role="stop"),
        _node("n2", 2),
        _node("n3", 3),
        _node("n4", 4),
    ]
    findings = collect_reachability_findings(_synthetic(nodes, []))
    assert len(findings) == 3
    assert {f.code for f in findings} == {PROCESS_IR_SEMANTIC_UNREACHABLE}
    assert {f.internal_node_id for f in findings} == {"n2", "n3", "n4"}


def test_reachability_terminates_on_a_cyclic_graph():
    """A cycle is the compiler's diagnostic to report, not this collector's —
    but this collector must not HANG on one before the compiler gets there."""
    nodes = [_node("n1", 1), _node("n2", 2)]
    edges = [_edge("e1", 1, "n1", "n2"), _edge("e2", 2, "n2", "n1")]
    assert collect_reachability_findings(_synthetic(nodes, edges)) == ()


def test_reachability_findings_name_the_authored_path():
    nodes = [_node("n1", 1, exit_role="stop"), _node("n2", 2, path="/body/steps/7")]
    findings = collect_reachability_findings(_synthetic(nodes, []))
    assert findings[0].path == "/body/steps/7"
    assert findings[0].phase == "reachability"


# ---------------------------------------------------------------------------
# terminals
# ---------------------------------------------------------------------------


def test_a_healthy_flow_has_no_terminal_findings():
    assert collect_terminal_findings(_prepared()) == ()


def test_a_leaf_without_an_exit_role_is_reported():
    nodes = [_node("n1", 1), _node("n2", 2)]
    findings = collect_terminal_findings(_synthetic(nodes, [_edge("e1", 1, "n1", "n2")]))
    assert [f.code for f in findings] == [PROCESS_IR_SEMANTIC_MISSING_TERMINAL]
    assert findings[0].internal_node_id == "n2"


def test_every_dangling_leaf_is_reported_not_just_one():
    nodes = [_node("n1", 1), _node("n2", 2), _node("n3", 3)]
    edges = [_edge("e1", 1, "n1", "n2", local=1), _edge("e2", 2, "n1", "n3", local=2)]
    findings = collect_terminal_findings(_synthetic(nodes, edges))
    assert len(findings) == 2


def test_a_leaf_carrying_an_exit_role_is_accepted():
    nodes = [_node("n1", 1, exit_role="stop")]
    assert collect_terminal_findings(_synthetic(nodes, [])) == ()


def test_control_flow_golden_produces_no_terminal_findings():
    """Real Branch/Decision bodies must stay clean — this is the regression
    guard against the per-control-path rule over-firing."""
    assert collect_terminal_findings(_prepared("control_flow")) == ()


# ---------------------------------------------------------------------------
# delegation to #140 / #142
# ---------------------------------------------------------------------------


def test_a_flow_with_no_connector_call_delegates_cleanly():
    assert collect_connector_flow_findings(_prepared()) == ()


def test_delegated_diagnostics_keep_their_codes_verbatim():
    """Codes are the stable contract. Delegation may re-file the PHASE, but a
    caller keying on PROCESS_IR_SEMANTIC_PROFILE_MISMATCH must still see it."""
    import boomi_mcp.compiler.process_ir.semantic_validation.flow as flow_mod
    from boomi_mcp.compiler.process_ir.diagnostics import diagnostic

    raised = ProcessIRCompileError(
        [diagnostic("PROCESS_IR_SEMANTIC_PROFILE_MISMATCH", "semantic_lowering", "/a")]
    )

    def _boom(cfg, symbols):
        raise raised

    original = flow_mod.validate_connector_calls
    flow_mod.validate_connector_calls = _boom
    try:
        findings = collect_connector_flow_findings(_prepared())
    finally:
        flow_mod.validate_connector_calls = original

    assert [f.code for f in findings] == ["PROCESS_IR_SEMANTIC_PROFILE_MISMATCH"]
    assert findings[0].phase == "profile"


def test_a_compile_family_diagnostic_is_reraised_never_translated():
    """ADR-001 §7: a ValidationReport must not be able to carry a COMPILE_*
    code. Translating one would invite a caller to 'fix' a compiler bug."""
    import boomi_mcp.compiler.process_ir.semantic_validation.flow as flow_mod
    from boomi_mcp.compiler.process_ir.diagnostics import diagnostic

    raised = ProcessIRCompileError(
        [diagnostic("PROCESS_IR_COMPILE_CONNECTOR_BINDING_INVALID", "emission_planning", "/a")]
    )

    def _boom(cfg, symbols):
        raise raised

    original = flow_mod.validate_connector_calls
    flow_mod.validate_connector_calls = _boom
    try:
        with pytest.raises(ProcessIRCompileError):
            collect_connector_flow_findings(_prepared())
    finally:
        flow_mod.validate_connector_calls = original


def test_a_mixed_diagnostic_set_containing_a_compile_code_is_reraised_whole():
    """Fail closed: if ANY diagnostic in the set blames the compiler, the whole
    set is re-raised rather than partially laundered into a report."""
    import boomi_mcp.compiler.process_ir.semantic_validation.flow as flow_mod
    from boomi_mcp.compiler.process_ir.diagnostics import diagnostic

    raised = ProcessIRCompileError(
        [
            diagnostic("PROCESS_IR_SEMANTIC_PROFILE_MISMATCH", "semantic_lowering", "/a"),
            diagnostic("PROCESS_IR_COMPILE_INTERNAL", "emission_planning", "/b"),
        ]
    )

    def _boom(cfg, symbols):
        raise raised

    original = flow_mod.validate_connector_calls
    flow_mod.validate_connector_calls = _boom
    try:
        with pytest.raises(ProcessIRCompileError):
            collect_connector_flow_findings(_prepared())
    finally:
        flow_mod.validate_connector_calls = original


# ---------------------------------------------------------------------------
# aggregate
# ---------------------------------------------------------------------------


def test_collect_flow_findings_is_clean_on_every_golden_doc():
    for name in GOLDEN_DOCS:
        prepared = prepare_validation_context(
            parse_process_ir_v1(GOLDEN_DOCS[name]), SymbolTableV1(symbols=())
        )
        assert collect_flow_findings(prepared) == (), name


def test_collect_flow_findings_merges_families():
    nodes = [_node("n1", 1), _node("n2", 2), _node("n3", 3)]
    findings = collect_flow_findings(_synthetic(nodes, [_edge("e1", 1, "n1", "n2")]))
    codes = {f.code for f in findings}
    assert PROCESS_IR_SEMANTIC_UNREACHABLE in codes  # n3
    assert PROCESS_IR_SEMANTIC_MISSING_TERMINAL in codes  # n2
