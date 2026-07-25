"""Rich Branch/Decision bodies (issue #141, M12.6).

The representative DIVERGENT flow: a Branch whose sibling legs run different
connector families with their own maps, and a Decision whose outcomes nest.

Every placement admitted here is live-attested in
``.codex/plans/issue-141-live-captures.md``; every placement rejected here is
either absent from that capture (fail-closed) or an explicitly gated capability.

**Golden oracle.** As in #140, the legacy builder cannot express these flows, so
there is no legacy-parity oracle. Four independent things stand in:

1. ``verify_process_graph`` accepts the emitted XML;
2. the emission-plan invariants (wiring, geometry, terminal sets, recomputed
   emitter inputs) hold — ``compile_process_ir_v1`` runs them;
3. the bytes are stable across repeated compiles and shuffled symbol order;
4. the Branch/Decision connector labels match the LIVE capture verbatim
   (``identifier="1"``/``text="1"``; ``identifier="true"``/``text="True"``).
"""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

_ROOT = Path(__file__).resolve().parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))
if str(_ROOT / "src") not in sys.path:
    sys.path.insert(0, str(_ROOT / "src"))

from boomi_mcp.categories.components.process_graph_verifier import verify_process_graph
from boomi_mcp.compiler.process_ir import body_capabilities as bodycaps
from boomi_mcp.compiler.process_ir.contracts import ComponentSymbolV1, SymbolTableV1
from boomi_mcp.compiler.process_ir.diagnostics import ProcessIRCompileError
from boomi_mcp.compiler.process_ir.emitter_registry import emit_process
from boomi_mcp.compiler.process_ir.pipeline import compile_process_ir_v1
from boomi_mcp.errors import (
    PROCESS_IR_CAPABILITY_NODE_NOT_ALLOWED_IN_BODY,
    PROCESS_IR_SEMANTIC_CARDINALITY_MISMATCH,
    PROCESS_IR_SEMANTIC_NESTING_LIMIT,
    PROCESS_IR_SEMANTIC_PROFILE_MISMATCH,
)
from boomi_mcp.models import process_ir as ir_module
from boomi_mcp.models.process_ir import (
    PROCESS_IR_V1_MAX_CONTROL_DEPTH,
    ProcessIRValidationError,
    parse_process_ir_v1,
)

_FIXTURES = _ROOT / "tests" / "fixtures" / "process_ir" / "rich_control"


def _symbol(ref, component_type, **extra):
    return ComponentSymbolV1(
        ref=ref, component_id="id_" + ref, component_type=component_type, **extra
    )


def rich_symbols() -> SymbolTableV1:
    """Two connector families, each with its own map boundary."""
    return SymbolTableV1(
        symbols=(
            _symbol("conn_rest", "connector-settings", connector_type="rest"),
            _symbol("conn_soap", "connector-settings", connector_type="soap_client"),
            _symbol("conn_db", "connector-settings", connector_type="database"),
            _symbol("prof_rest_out", "profile.json"),
            _symbol("prof_soap_in", "profile.xml"),
            _symbol("prof_soap_out", "profile.xml"),
            _symbol("prof_db_write", "profile.db"),
            _symbol("prof_patch_in", "profile.json"),
            _symbol("prof_patch_out", "profile.json"),
            _symbol(
                "op_rest_get",
                "connector-action",
                connector_type="rest",
                action_type="GET",
                connection_ref="conn_rest",
                output_profile_ref="prof_rest_out",
            ),
            _symbol(
                "map_rest_to_soap",
                "transform.map",
                input_profile_ref="prof_rest_out",
                output_profile_ref="prof_soap_in",
            ),
            _symbol(
                "map_rest_to_patch",
                "transform.map",
                input_profile_ref="prof_rest_out",
                output_profile_ref="prof_patch_in",
            ),
            _symbol(
                "op_soap_execute",
                "connector-action",
                connector_type="soap_client",
                action_type="EXECUTE",
                connection_ref="conn_soap",
                input_profile_ref="prof_soap_in",
                output_profile_ref="prof_soap_out",
            ),
            _symbol(
                "op_rest_patch",
                "connector-action",
                connector_type="rest",
                action_type="PATCH",
                connection_ref="conn_rest",
                input_profile_ref="prof_patch_in",
                output_profile_ref="prof_patch_out",
            ),
            _symbol(
                "op_db_send",
                "connector-action",
                connector_type="database",
                action_type="Send",
                connection_ref="conn_db",
                input_profile_ref="prof_db_write",
            ),
            _symbol("child_process", "process"),
        )
    )


def call(ref, **extra):
    step = {"kind": "connector_call", "operation_ref": ref}
    step.update(extra)
    return step


#: The divergent fixture the issue asks for: sibling legs run DIFFERENT connector
#: families with DIFFERENT maps, and each terminates independently.
BRANCH_MIXED_DOC = {
    "version": "1",
    "body": {
        "kind": "sequence",
        "steps": [
            call("op_rest_get", action="GET", label="Read orders"),
            {
                "kind": "branch",
                "legs": [
                    {
                        "steps": [
                            {"kind": "map_ref", "map_ref": "map_rest_to_soap"},
                            call("op_soap_execute", action="EXECUTE"),
                        ],
                        "terminal": {"kind": "stop"},
                    },
                    {
                        "steps": [
                            {"kind": "map_ref", "map_ref": "map_rest_to_patch"},
                            call("op_rest_patch", action="PATCH"),
                        ],
                        "terminal": {"kind": "stop"},
                    },
                ],
            },
        ],
    },
}


def compile_doc(doc, symbols=None):
    table = symbols or rich_symbols()
    return compile_process_ir_v1(parse_process_ir_v1(doc), table), table


def codes_for(doc, symbols=None):
    with pytest.raises(ProcessIRCompileError) as excinfo:
        compile_process_ir_v1(parse_process_ir_v1(doc), symbols or rich_symbols())
    return [(item.code, item.path) for item in excinfo.value.diagnostics]


# ---------------------------------------------------------------------------
# AC: a divergent fixture with mixed connector actions/mappings per path
# ---------------------------------------------------------------------------


def test_divergent_mixed_connector_branch_compiles():
    (cfg, plan), table = compile_doc(BRANCH_MIXED_DOC)
    kinds = [node.emitter_input.emitter_kind for node in plan.nodes]
    assert kinds[0] == "start_noaction"
    # One source shape (the entry call) and three target shapes (SOAP, PATCH).
    assert kinds.count("connectoraction_source") == 1
    assert kinds.count("connectoraction_target") == 2
    assert kinds.count("branch") == 1
    assert kinds.count("map") == 2


def test_divergent_branch_emits_valid_graph_and_live_verified_labels():
    (_cfg, plan), table = compile_doc(BRANCH_MIXED_DOC)
    artifact = emit_process(plan, table)
    text = artifact.process_xml
    report = verify_process_graph(text)
    assert not report.get("errors"), report
    # Live-captured Branch connector labels (capture §2.1): the 1-based leg
    # number in BOTH attributes.
    assert 'identifier="1"' in text and 'text="1"' in text
    assert 'identifier="2"' in text and 'text="2"' in text


def test_divergent_branch_bytes_are_deterministic():
    """Repeated compilation, and a shuffled symbol table, must not move a byte."""
    (_c1, plan1), table1 = compile_doc(BRANCH_MIXED_DOC)
    first_text = emit_process(plan1, table1).process_xml

    shuffled = SymbolTableV1(symbols=tuple(reversed(rich_symbols().symbols)))
    (_c2, plan2), _ = compile_doc(BRANCH_MIXED_DOC, shuffled)
    second_text = emit_process(plan2, shuffled).process_xml

    assert first_text == second_text
    # ...and a second compile of the very same input, too.
    (_c3, plan3), table3 = compile_doc(BRANCH_MIXED_DOC)
    assert emit_process(plan3, table3).process_xml == first_text


def test_authored_leg_order_is_preserved_end_to_end():
    (cfg, plan), _ = compile_doc(BRANCH_MIXED_DOC)
    branch = next(n for n in cfg.nodes if n.semantic.semantic_kind == "branch")
    leg_edges = [e for e in cfg.edges if e.source_node_id == branch.node_id]
    assert [e.leg_ordinal for e in leg_edges] == [1, 2]
    # ...and the same order survives into the plan's wiring.
    plan_branch = next(n for n in plan.nodes if n.emitter_input.emitter_kind == "branch")
    assert [t.identifier for t in plan_branch.outgoing] == ["1", "2"]


# ---------------------------------------------------------------------------
# AC: sibling paths are independent — no state leaks between legs
# ---------------------------------------------------------------------------


def test_a_maps_profiles_are_checked_against_its_own_leg_only():
    """Leg 2's map must be compared with leg 2's call, never leg 1's.

    Before #141 the checker walked one flattened list of every call and map in
    CFG order, which interleaves independent legs — leg 2's map would have been
    judged against leg 1's SOAP call and wrongly rejected.
    """
    (cfg, _plan), _ = compile_doc(BRANCH_MIXED_DOC)
    assert cfg is not None  # compiling at all is the assertion


def test_a_send_in_one_leg_does_not_block_its_sibling():
    """A non-producing Send terminalizes ITS path, not the whole process."""
    doc = {
        "version": "1",
        "body": {
            "kind": "sequence",
            "steps": [
                call("op_rest_get", action="GET"),
                {
                    "kind": "branch",
                    "legs": [
                        # A Send, then nothing — legal, it ends its own path.
                        {"steps": [call("op_db_send", action="Send")], "terminal": {"kind": "stop"}},
                        # The sibling still gets documents from the entry call.
                        {
                            "steps": [
                                {"kind": "map_ref", "map_ref": "map_rest_to_patch"},
                                call("op_rest_patch", action="PATCH"),
                            ],
                            "terminal": {"kind": "stop"},
                        },
                    ],
                },
            ],
        },
    }
    (cfg, plan), _ = compile_doc(doc)
    assert plan is not None


def test_a_call_after_a_send_inside_one_leg_is_still_rejected():
    """The Send gate is per-path, not abandoned."""
    doc = {
        "version": "1",
        "body": {
            "kind": "sequence",
            "steps": [
                call("op_rest_get", action="GET"),
                {
                    "kind": "branch",
                    "legs": [
                        {
                            "steps": [
                                call("op_db_send", action="Send"),
                                call("op_rest_patch", action="PATCH"),
                            ],
                            "terminal": {"kind": "stop"},
                        },
                        {"steps": [{"kind": "message", "text": "m"}], "terminal": {"kind": "stop"}},
                    ],
                },
            ],
        },
    }
    codes = codes_for(doc)
    assert codes[0][0] == PROCESS_IR_SEMANTIC_CARDINALITY_MISMATCH
    # Blames the SEND — the node whose position is wrong — not the follower.
    assert codes[0][1] == "/body/steps/1/legs/0/steps/0/operation_ref"


def test_a_documents_required_call_with_no_producer_on_its_path_is_rejected():
    doc = {
        "version": "1",
        "body": {
            "kind": "sequence",
            "steps": [
                {
                    "kind": "branch",
                    "legs": [
                        # No producer anywhere on this path.
                        {"steps": [call("op_rest_patch", action="PATCH")], "terminal": {"kind": "stop"}},
                        {"steps": [{"kind": "message", "text": "m"}], "terminal": {"kind": "stop"}},
                    ],
                }
            ],
        },
    }
    codes = codes_for(doc)
    assert codes[0][0] == PROCESS_IR_SEMANTIC_CARDINALITY_MISMATCH


def test_a_legacy_source_endpoint_feeds_a_call_inside_a_control_body():
    """A legacy ``source`` genuinely produces documents.

    Rejecting a ``documents_required`` call in a leg beneath one would be wrong,
    and this shape is new in #141 (a pure legacy flow yields no bindings at all).
    """
    doc = {
        "version": "1",
        "body": {
            "kind": "sequence",
            "steps": [
                {"kind": "source", "connection_ref": "conn_rest", "operation_ref": "op_rest_get"},
                {
                    "kind": "branch",
                    "legs": [
                        {
                            "steps": [
                                {"kind": "map_ref", "map_ref": "map_rest_to_patch"},
                                call("op_rest_patch", action="PATCH"),
                            ],
                            "terminal": {"kind": "stop"},
                        },
                        {"steps": [{"kind": "message", "text": "m"}], "terminal": {"kind": "stop"}},
                    ],
                },
            ],
        },
    }
    (cfg, plan), _ = compile_doc(doc)
    assert plan is not None


# ---------------------------------------------------------------------------
# AC: the nesting maximum is enforced at limit and limit+1
# ---------------------------------------------------------------------------


def _decision(true_terminal, false_terminal=None):
    return {
        "kind": "decision",
        "comparison": "equals",
        "left": {"value_type": "static", "static_value": "a"},
        "right": {"value_type": "static", "static_value": "b"},
        "true_arm": {"steps": [{"kind": "message", "text": "t"}], "terminal": true_terminal},
        "false_arm": {"steps": [], "terminal": false_terminal or {"kind": "stop"}},
    }


def test_depth_at_the_limit_compiles():
    doc = {
        "version": "1",
        "body": {"kind": "sequence", "steps": [_decision(_decision({"kind": "stop"}))]},
    }
    (cfg, plan), _ = compile_doc(doc)
    assert sum(1 for n in cfg.nodes if n.semantic.semantic_kind == "decision") == 2


def test_depth_at_limit_plus_one_is_rejected_before_any_cfg_exists():
    doc = {
        "version": "1",
        "body": {
            "kind": "sequence",
            "steps": [_decision(_decision(_decision({"kind": "stop"})))],
        },
    }
    # The model rejects it at PARSE time — before a CFG, plan or emitter exists,
    # and therefore before anything could mutate a component.
    with pytest.raises(ProcessIRValidationError) as excinfo:
        parse_process_ir_v1(doc)
    assert excinfo.value.diagnostics[0].code == PROCESS_IR_SEMANTIC_NESTING_LIMIT


def test_the_depth_bound_is_re_derived_from_the_cfg():
    """The compiler-side check is computed from a DIFFERENT representation.

    ``models`` walks the authored tree; ``invariants`` walks the lowered graph.
    A lowering defect that flattened nesting could otherwise slip a document past
    a rule that only ever looked at the authored form.
    """
    from boomi_mcp.compiler.process_ir.invariants import check_cfg_invariants
    from boomi_mcp.compiler.process_ir.lowering import lower_process_ir_to_cfg

    ok = parse_process_ir_v1(
        {"version": "1", "body": {"kind": "sequence", "steps": [_decision(_decision({"kind": "stop"}))]}}
    )
    check_cfg_invariants(lower_process_ir_to_cfg(ok))  # depth 2 passes

    import boomi_mcp.compiler.process_ir.invariants as inv

    original = inv.PROCESS_IR_V1_MAX_CONTROL_DEPTH
    try:
        inv.PROCESS_IR_V1_MAX_CONTROL_DEPTH = 1
        with pytest.raises(ProcessIRCompileError) as excinfo:
            check_cfg_invariants(lower_process_ir_to_cfg(ok))
        assert excinfo.value.diagnostics[0].code == PROCESS_IR_SEMANTIC_NESTING_LIMIT
    finally:
        inv.PROCESS_IR_V1_MAX_CONTROL_DEPTH = original


def test_the_documented_depth_is_two():
    """Pinned so raising it is a deliberate, reviewed act.

    Two is a COMPILER bound chosen on test cost, not a platform cap: the capture
    records a real production Decision chain six deep inside one Branch leg
    (§2.1), which this bound rejects. The documentation must say so.
    """
    assert PROCESS_IR_V1_MAX_CONTROL_DEPTH == 2


# ---------------------------------------------------------------------------
# Nested Decision + bare false Stop — the dominant live shape
# ---------------------------------------------------------------------------


DECISION_NESTED_DOC = {
    "version": "1",
    "body": {
        "kind": "sequence",
        "steps": [
            call("op_rest_get", action="GET"),
            _decision(
                # TRUE routes into a second Decision (live-attested, capture §2.1);
                # its own outcomes end in an exception and a BARE Stop.
                _decision({"kind": "exception", "message_template": "failed: {1}"}),
                # FALSE routes straight to a Stop with zero intervening steps —
                # the rule #141 removed as legacy-builder parity.
                {"kind": "stop"},
            ),
        ],
    },
}


def test_nested_decision_with_bare_false_stop_compiles_and_emits():
    (cfg, plan), table = compile_doc(DECISION_NESTED_DOC)
    assert sum(1 for n in cfg.nodes if n.semantic.semantic_kind == "decision") == 2
    artifact = emit_process(plan, table)
    report = verify_process_graph(artifact.process_xml)
    assert not report.get("errors"), report
    # Live-captured Decision connector labels (capture §2.1) — note the case
    # asymmetry: lowercase identifier, title-case text.
    assert 'identifier="true"' in artifact.process_xml
    assert 'text="True"' in artifact.process_xml
    assert 'identifier="false"' in artifact.process_xml
    assert 'text="False"' in artifact.process_xml


def test_true_outcome_precedes_false_everywhere():
    """Documented ordering: True documents are processed to completion first."""
    (cfg, plan), _ = compile_doc(DECISION_NESTED_DOC)
    for node in cfg.nodes:
        if node.semantic.semantic_kind != "decision":
            continue
        outs = [e for e in cfg.edges if e.source_node_id == node.node_id]
        assert [e.outcome for e in outs] == ["true", "false"]
    for node in plan.nodes:
        if node.emitter_input.emitter_kind == "decision":
            assert [t.identifier for t in node.outgoing] == ["true", "false"]




# ---------------------------------------------------------------------------
# AC: control-only root (what makes a ProcessCall-only path constructible)
# ---------------------------------------------------------------------------


PROCESS_CALL_BRANCH_DOC = {
    "version": "1",
    "body": {
        "kind": "sequence",
        "steps": [
            {
                "kind": "branch",
                "legs": [
                    {
                        "steps": [{"kind": "process_call", "process_ref": "child_process"}],
                        "terminal": {"kind": "stop"},
                    },
                    {
                        "steps": [{"kind": "process_call", "process_ref": "child_process"}],
                        "terminal": {"kind": "stop"},
                    },
                ],
            }
        ],
    },
}


def test_control_only_root_with_process_call_legs_compiles_and_emits():
    (cfg, plan), table = compile_doc(PROCESS_CALL_BRANCH_DOC)
    # The Branch IS the control-flow entry; synthetic Start wires straight to it.
    assert cfg.entry_node_id == cfg.nodes[0].node_id
    assert cfg.nodes[0].semantic.semantic_kind == "branch"
    assert plan.nodes[0].synthetic_role == "start"
    assert plan.nodes[0].outgoing[0].to_shape_id == plan.nodes[1].shape_id

    artifact = emit_process(plan, table)
    report = verify_process_graph(artifact.process_xml)
    assert not report.get("errors"), report


def test_a_process_call_path_carries_no_connector():
    """``process_call_connector_mixing`` stays honestly gated.

    The control-only root is what makes this true rather than vacuous: any other
    root that can hold a Branch starts with a connector, which would sit on every
    leg's root-to-leaf path.
    """
    (cfg, _plan), _ = compile_doc(PROCESS_CALL_BRANCH_DOC)
    assert not [
        n for n in cfg.nodes if n.semantic.semantic_kind in ("connector", "connector_call")
    ]


def test_a_control_only_root_admits_nothing_beside_the_control():
    for extra in ({"kind": "message", "text": "m"}, {"kind": "stop"}):
        doc = {
            "version": "1",
            "body": {
                "kind": "sequence",
                "steps": [PROCESS_CALL_BRANCH_DOC["body"]["steps"][0], extra],
            },
        }
        with pytest.raises(ProcessIRValidationError):
            parse_process_ir_v1(doc)


# ---------------------------------------------------------------------------
# AC: the capability registry is closed and cannot drift from the unions
# ---------------------------------------------------------------------------


def _union_kinds(alias):
    return set(ir_module._kinds_of(alias))


@pytest.mark.parametrize(
    "context,slot,alias_name",
    [
        (bodycaps.BRANCH_LEG, bodycaps.STEP_SLOT, "BranchLegStepV1"),
        (bodycaps.DECISION_TRUE_ARM, bodycaps.STEP_SLOT, "DecisionTrueArmStepV1"),
        (bodycaps.DECISION_FALSE_ARM, bodycaps.STEP_SLOT, "DecisionFalseArmStepV1"),
    ],
)
def test_registry_step_rows_match_the_model_unions(context, slot, alias_name):
    """BOTH directions.

    A union that gains a kind without a deliberate registry change fails here,
    and so does a registry row that admits something the union cannot express —
    which is the whole reason the registry is worth having as data.
    """
    assert bodycaps.BODY_CAPABILITIES_V1[(context, slot)] == _union_kinds(
        getattr(ir_module, alias_name)
    )


def test_registry_terminal_rows_are_the_shipped_matrix():
    assert bodycaps.BODY_CAPABILITIES_V1[(bodycaps.BRANCH_LEG, bodycaps.TERMINAL_SLOT)] == {
        "target",
        "cache_put",
        "stop",
        "decision",
    }
    assert bodycaps.BODY_CAPABILITIES_V1[
        (bodycaps.DECISION_TRUE_ARM, bodycaps.TERMINAL_SLOT)
    ] == {"target", "stop", "exception", "branch", "decision"}
    assert bodycaps.BODY_CAPABILITIES_V1[
        (bodycaps.DECISION_FALSE_ARM, bodycaps.TERMINAL_SLOT)
    ] == {"stop", "exception", "branch", "decision"}


def test_registry_lookup_is_absence_as_denial():
    assert not bodycaps.is_allowed("branch_leg", "terminal", "branch")
    assert not bodycaps.is_allowed("decision_false_arm", "step", "process_call")
    assert not bodycaps.is_allowed("branch_leg", "step", "return_documents")
    assert not bodycaps.is_allowed("no_such_context", "step", "message")
    assert not bodycaps.is_allowed("branch_leg", "no_such_slot", "message")


def test_registry_rejects_a_wrong_context_node_before_lowering():
    """The registry is a real gate, not documentation.

    Driven through the compiler with a hand-built model that bypasses the union
    (the unions and the registry agree, so a payload cannot normally reach it) —
    proving the second, independent enforcement point exists.
    """
    ir = parse_process_ir_v1(BRANCH_MIXED_DOC)
    leg = ir.body.steps[1].legs[0]
    object.__setattr__(leg, "terminal", ir_module.ReturnDocumentsNodeV1(kind="return_documents"))
    with pytest.raises(ProcessIRCompileError) as excinfo:
        compile_process_ir_v1(ir, rich_symbols())
    assert excinfo.value.diagnostics[0].code == PROCESS_IR_CAPABILITY_NODE_NOT_ALLOWED_IN_BODY
    assert excinfo.value.diagnostics[0].path == "/body/steps/1/legs/0/terminal"


# ---------------------------------------------------------------------------
# Security: diagnostics never echo authored values
# ---------------------------------------------------------------------------


SENTINEL = "SENTINEL-DO-NOT-LEAK"


def test_diagnostics_never_echo_labels_or_operands():
    doc = {
        "version": "1",
        "body": {
            "kind": "sequence",
            "steps": [
                call("op_rest_get", action="GET", label=SENTINEL),
                {
                    "kind": "branch",
                    "legs": [
                        {
                            "steps": [
                                call("op_db_send", action="Send", label=SENTINEL),
                                call("op_rest_patch", action="PATCH"),
                            ],
                            "terminal": {"kind": "stop"},
                        },
                        {"steps": [{"kind": "message", "text": SENTINEL}], "terminal": {"kind": "stop"}},
                    ],
                },
            ],
        },
    }
    with pytest.raises(ProcessIRCompileError) as excinfo:
        compile_process_ir_v1(parse_process_ir_v1(doc), rich_symbols())
    rendered = str(excinfo.value) + "".join(
        d.message + d.remediation + d.path for d in excinfo.value.diagnostics
    )
    assert SENTINEL not in rendered


# ---------------------------------------------------------------------------
# The committed fixtures are the real inputs, not decoration
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "name,doc",
    [
        ("branch_mixed_connectors.json", BRANCH_MIXED_DOC),
        ("branch_process_call.json", PROCESS_CALL_BRANCH_DOC),
    ],
)
def test_committed_fixture_matches_the_compiled_document(name, doc):
    import json

    assert json.loads((_FIXTURES / name).read_text()) == doc


@pytest.mark.parametrize(
    "name", ["branch_mixed_connectors.json", "branch_process_call.json"]
)
def test_fixtures_carry_only_opaque_sentinel_references(name):
    text = (_FIXTURES / name).read_text().lower()
    for forbidden in ("password", "secret", "token", "http://", "https://", "@"):
        assert forbidden not in text
