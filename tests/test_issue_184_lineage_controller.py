"""#184 D12: one ordered lineage controller, iterative, with no depth bound.

The recursive visit this replaced stopped at depth 256 and reported the walk as
truncated. Once #184 moved the stream-profile proof into that walk, a truncated
walk no longer only lost state facts: a profile consumer past the cutoff went
unchecked. The connector walk that used to check maps had no depth bound, so that
was a regression the controller change removes.

Expected codes and pointers come from the plan of record (Claude plan D12: "No depth
cutoff. A visit counter above len(cfg.nodes) raises PROCESS_IR_COMPILE_INTERNAL")
and from the stream-profile rule the pointers are already pinned by
(`tests/test_issue_184_stream_profiles.py`). The before/after differential over every
committed IR fixture is archived under `docs/architecture/evidence/issue-184/sweeps/`.
"""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

_ROOT = Path(__file__).resolve().parent.parent
for _p in (str(_ROOT), str(_ROOT / "src")):
    if _p not in sys.path:
        sys.path.insert(0, _p)

from boomi_mcp.compiler.process_ir import connector_capabilities as CC  # noqa: E402
from boomi_mcp.compiler.process_ir.contracts import (  # noqa: E402
    CfgEdgeV1,
    CfgNodeV1,
    ComponentSymbolV1,
    MessageSemanticV1,
    SemanticCfgV1,
    StopSemanticV1,
    SymbolTableV1,
)
from boomi_mcp.compiler.process_ir.diagnostics import ProcessIRCompileError  # noqa: E402
from boomi_mcp.compiler.process_ir.pipeline import (  # noqa: E402
    compile_process_ir_v1,
    parse_and_compile_process_ir_v1,
)
from boomi_mcp.errors import (  # noqa: E402
    PROCESS_IR_COMPILE_INTERNAL,
    PROCESS_IR_SEMANTIC_LINEAGE_PROPERTY_READ_BEFORE_WRITE,
    PROCESS_IR_SEMANTIC_PROFILE_MISMATCH,
)
from boomi_mcp.models.process_ir import parse_process_ir_v1  # noqa: E402

#: Deeper than the withdrawn recursive bound of 256 nodes on one path.
_PAST_THE_OLD_BOUND = 300


def _symbols():
    rest = CC.REST_FAMILY

    def sym(ref, cid, ctype, **kw):
        return ComponentSymbolV1(ref="$ref:" + ref, component_id=cid, component_type=ctype, **kw)

    return SymbolTableV1(symbols=(
        sym("RCONN", "RCONN", "connector-settings", connector_type=rest),
        sym("GET", "GETOP", "connector-action", connector_type=rest, action_type="GET",
            connection_ref="$ref:RCONN", output_profile_ref="$ref:P1"),
        sym("PATCH", "PATCHOP", "connector-action", connector_type=rest, action_type="PATCH",
            connection_ref="$ref:RCONN", input_profile_ref="$ref:P2"),
        sym("P1", "PROFILE-ONE", "profile.json"),
        sym("P2", "PROFILE-TWO", "profile.json"),
        sym("CACHE", "CACHE", "documentcache"),
    ))


def _filler(count):
    return [
        {"kind": "set_dpp", "name": "FILLER%d" % index,
         "source_values": [{"value_type": "static", "value": "v"}]}
        for index in range(count)
    ]


def _both_routes(payload):
    routes = []
    for compile_route in ("parse_and_compile", "model"):
        try:
            if compile_route == "parse_and_compile":
                parse_and_compile_process_ir_v1(payload, _symbols())
            else:
                compile_process_ir_v1(parse_process_ir_v1(payload), _symbols())
        except ProcessIRCompileError as exc:
            routes.append(tuple((item.code, item.path) for item in exc.diagnostics))
        else:
            routes.append(())
    assert routes[0] == routes[1], routes
    return routes[0]


def test_a_profile_consumer_past_the_old_depth_bound_is_checked():
    """A profile source 301 nodes down a root connector sequence is still compared
    with the stream reaching it: GET hands on `P1`, and the writer reads `P2`."""
    writer = {"kind": "set_ddp", "name": "X", "source_values": [
        {"value_type": "profile", "profile_ref": "$ref:P2", "profile_type": "json",
         "element_id": "3", "element_name": "id"}]}
    steps = ([{"kind": "connector_call", "operation_ref": "$ref:GET"}]
             + _filler(_PAST_THE_OLD_BOUND)
             + [writer, {"kind": "connector_call", "operation_ref": "$ref:PATCH"}, {"kind": "stop"}])
    pointer = "/body/steps/{0}/source_values/0/profile_ref".format(1 + _PAST_THE_OLD_BOUND)
    diagnostics = _both_routes({"version": "1", "body": {"kind": "sequence", "steps": steps}})
    assert (PROCESS_IR_SEMANTIC_PROFILE_MISMATCH, pointer) in diagnostics, diagnostics


def test_a_stale_document_property_past_the_old_depth_bound_is_reported():
    """The #184 A5 read-before-write, with the reader past the withdrawn bound."""
    steps = ([{"kind": "connector_call", "operation_ref": "$ref:GET"},
              {"kind": "set_ddp", "name": "X", "source_values": [{"value_type": "static", "value": "v"}]},
              {"kind": "cache_put", "cache_ref": "$ref:CACHE"},
              {"kind": "cache_get", "cache_ref": "$ref:CACHE"}]
             + _filler(_PAST_THE_OLD_BOUND)
             + [{"kind": "set_dpp", "name": "Y", "source_values": [{"value_type": "ddp", "property_name": "X"}]},
                {"kind": "connector_call", "operation_ref": "$ref:PATCH"}, {"kind": "stop"}])
    pointer = "/body/steps/{0}".format(4 + _PAST_THE_OLD_BOUND)
    diagnostics = _both_routes({"version": "1", "body": {"kind": "sequence", "steps": steps}})
    assert (PROCESS_IR_SEMANTIC_LINEAGE_PROPERTY_READ_BEFORE_WRITE, pointer) in diagnostics, diagnostics


def test_a_graph_that_is_not_a_tree_is_a_compiler_defect_not_a_loop():
    """The visit counter's refusal, witnessed. Lowering only produces trees, and the
    CFG invariants reject a cycle before the gate, so this walk can meet a cycle only
    through a hand-assembled context — and there it must refuse rather than spin."""
    from boomi_mcp.compiler.process_ir.semantic_validation.context import (
        PreparedProcessValidationV1,
        _edge_index,
    )
    from boomi_mcp.compiler.process_ir.semantic_validation.lineage import walk_lineage

    ir = parse_process_ir_v1({"version": "1", "body": {"kind": "sequence", "steps": [
        {"kind": "message", "text": "m"},
        {"kind": "connector_call", "operation_ref": "$ref:GET"},
        {"kind": "stop"}]}})
    nodes = (
        CfgNodeV1(node_id="n1", ordinal=1, source_path="/body/steps/0", semantic=MessageSemanticV1(text="m")),
        CfgNodeV1(node_id="n2", ordinal=2, source_path="/body/steps/1", semantic=StopSemanticV1(), exit_role="stop"),
    )
    edges = (
        CfgEdgeV1(edge_id="e1", ordinal=1, source_node_id="n1", target_node_id="n2",
                  kind="terminal", local_ordinal=1, provenance_path="/body/steps/1"),
        CfgEdgeV1(edge_id="e2", ordinal=2, source_node_id="n2", target_node_id="n1",
                  kind="ordering", local_ordinal=1, provenance_path="/body/steps/0"),
    )
    cfg = SemanticCfgV1(entry_node_id="n1", nodes=nodes, edges=edges, exit_node_ids=("n2",))
    prepared = PreparedProcessValidationV1(
        ir=ir, cfg=cfg, symbols=SymbolTableV1(symbols=()),
        node_by_id={node.node_id: node for node in nodes},
        outgoing=_edge_index(edges, "source_node_id"),
        incoming=_edge_index(edges, "target_node_id"),
        symbol_by_ref={},
    )
    with pytest.raises(ProcessIRCompileError) as excinfo:
        walk_lineage(prepared)
    assert [item.code for item in excinfo.value.diagnostics] == [PROCESS_IR_COMPILE_INTERNAL]


def test_the_truncation_reason_is_withdrawn_from_the_served_contract():
    """A closed limit is withdrawn, not reworded: the token, its served row and the
    sentence clause composed from that row are all gone. The literal below is the
    withdrawn served wording, copied from the pre-D12 row."""
    from boomi_mcp.authoring import process_ir_effects as effects
    from boomi_mcp.authoring.process_ir_projection import _SEMANTIC_RULES

    assert "walk_truncated" not in {token for token, _wording in effects.subprocess_inert_reasons()}
    assert not hasattr(effects, "INERT_WALK_TRUNCATED")
    rule = next(row for row in _SEMANTIC_RULES if row[0] == "semantic_rule.effect.subprocess_inspection")
    assert "the walk stops at its bound" not in rule[3], rule[3]
