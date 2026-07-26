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
    PROCESS_IR_SEMANTIC_AMBIGUOUS_FLOW,
    PROCESS_IR_SEMANTIC_CARDINALITY_MISMATCH,
    PROCESS_IR_SEMANTIC_NESTING_LIMIT,
    PROCESS_IR_SEMANTIC_PROFILE_MISMATCH,
    PROCESS_IR_SEMANTIC_UNTERMINATED_PATH,
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


# ---------------------------------------------------------------------------
# Codex review round 1 — regressions for four real holes in the first cut
# ---------------------------------------------------------------------------


def test_process_call_body_requires_a_control_only_root():
    """R1/F1: path mode is body-local, so it could not see a connector ANCESTOR.

    `[connector_call(GET), branch(process_call -> stop)]` used to compile, which
    is exactly the `process_call_connector_mixing` the manifest still reports as
    gated — the root's connector sits on that leg's root-to-leaf path.
    """
    doc = {
        "version": "1",
        "body": {"kind": "sequence", "steps": [
            call("op_rest_get", action="GET"),
            {"kind": "branch", "legs": [
                {"steps": [{"kind": "process_call", "process_ref": "child_process"}],
                 "terminal": {"kind": "stop"}},
                {"steps": [{"kind": "message", "text": "m"}], "terminal": {"kind": "stop"}},
            ]},
        ]},
    }
    with pytest.raises(ProcessIRValidationError) as excinfo:
        parse_process_ir_v1(doc)
    assert excinfo.value.diagnostics[0].code == PROCESS_IR_CAPABILITY_NODE_NOT_ALLOWED_IN_BODY
    # ...and the legitimate shape is untouched.
    compile_doc(PROCESS_CALL_BRANCH_DOC)


@pytest.mark.parametrize(
    "follower,label",
    [
        ({"kind": "message", "text": "after"}, "message"),
        ({"kind": "set_ddp", "name": "X", "source_values": [{"value_type": "static", "value": "v"}]}, "set_ddp"),
        ({"kind": "flow_control", "for_each_count": 1}, "flow_control"),
        # `cache_put` is deliberately absent: an existing model rule already
        # rejects a trailing cache_put in a leg's steps (it belongs in the leg
        # terminal), so the Send gate never sees that shape.
    ],
)
def test_the_send_gate_covers_every_downstream_node(follower, label):
    """R1/F2: the gate only rejected a following call or map.

    Before #141 nothing else COULD follow a call (a root connector_call sequence
    admits only calls and maps), so those two kinds were the whole story. Rich
    bodies broke that: a leg may put any linear node after a Send, and none of
    them can ever run.
    """
    doc = {
        "version": "1",
        "body": {"kind": "sequence", "steps": [
            call("op_rest_get", action="GET"),
            {"kind": "branch", "legs": [
                {"steps": [call("op_db_send", action="Send"), follower],
                 "terminal": {"kind": "stop"}},
                {"steps": [{"kind": "message", "text": "m"}], "terminal": {"kind": "stop"}},
            ]},
        ]},
    }
    codes = codes_for(doc)
    assert codes[0][0] == PROCESS_IR_SEMANTIC_CARDINALITY_MISMATCH, label
    # Blames the SEND, not the follower — the Send is what is mispositioned.
    assert codes[0][1] == "/body/steps/1/legs/0/steps/0/operation_ref"


def test_a_send_may_still_be_followed_by_a_stop():
    """The gate must not reject the one legal follower."""
    doc = {
        "version": "1",
        "body": {"kind": "sequence", "steps": [
            call("op_rest_get", action="GET"),
            {"kind": "branch", "legs": [
                {"steps": [call("op_db_send", action="Send")], "terminal": {"kind": "stop"}},
                {"steps": [{"kind": "message", "text": "m"}], "terminal": {"kind": "stop"}},
            ]},
        ]},
    }
    compile_doc(doc)


def test_a_send_may_be_followed_by_a_stream_replacing_cache_read():
    """A cache read supplies its own documents, so it genuinely restarts the stream."""
    doc = {
        "version": "1",
        "body": {"kind": "sequence", "steps": [
            call("op_rest_get", action="GET"),
            {"kind": "branch", "legs": [
                {"steps": [call("op_db_send", action="Send"),
                           {"kind": "cache_get", "cache_ref": "child_process"}],
                 "terminal": {"kind": "stop"}},
                {"steps": [{"kind": "message", "text": "m"}], "terminal": {"kind": "stop"}},
            ]},
        ]},
    }
    compile_doc(doc)


def test_every_map_in_a_body_is_validated_or_rejected():
    """R1/F3: a second map silently overwrote the first, and a map before a
    terminal was dropped — either way the compiler claimed to have "verified
    profiles" for a map it never compared.

    #140 states this rule in the MODEL for a root connector_call sequence ("a
    map_ref must be immediately followed by a connector_call"); inside a control
    body a map is an ordinary linear step, so the walk has to state it.
    """
    def leg_steps(steps):
        return {
            "version": "1",
            "body": {"kind": "sequence", "steps": [
                call("op_rest_get", action="GET"),
                {"kind": "branch", "legs": [
                    {"steps": steps, "terminal": {"kind": "stop"}},
                    {"steps": [{"kind": "message", "text": "m"}], "terminal": {"kind": "stop"}},
                ]},
            ]},
        }

    # two maps in a row: the first has no call to be checked against
    codes = codes_for(leg_steps([
        {"kind": "map_ref", "map_ref": "map_rest_to_soap"},
        {"kind": "map_ref", "map_ref": "map_rest_to_patch"},
        call("op_rest_patch", action="PATCH"),
    ]))
    assert codes[0][0] == PROCESS_IR_SEMANTIC_PROFILE_MISMATCH

    # a map whose successor is the terminal, never a call
    codes = codes_for(leg_steps([{"kind": "map_ref", "map_ref": "map_rest_to_soap"}]))
    assert codes[0][0] == PROCESS_IR_SEMANTIC_PROFILE_MISMATCH

    # a map whose successor is some other linear node
    codes = codes_for(leg_steps([
        {"kind": "map_ref", "map_ref": "map_rest_to_soap"},
        {"kind": "message", "text": "m"},
        call("op_soap_execute", action="EXECUTE"),
    ]))
    assert codes[0][0] == PROCESS_IR_SEMANTIC_PROFILE_MISMATCH


def test_a_legacy_map_beside_non_call_neighbours_stays_unchecked():
    """The bracketing rule applies only where an upstream CALL exists.

    A map whose upstream is a legacy `source` endpoint carries no call-to-call
    profile pair, so it is left alone exactly as before #141 — tightening it
    would reject the existing branch-leg shape (`steps:[map_ref]` + target).
    """
    doc = {
        "version": "1",
        "body": {"kind": "sequence", "steps": [
            {"kind": "source", "connection_ref": "conn_rest", "operation_ref": "op_rest_get"},
            {"kind": "branch", "legs": [
                {"steps": [{"kind": "map_ref", "map_ref": "map_rest_to_soap"}],
                 "terminal": {"kind": "target", "connection_ref": "conn_rest",
                              "operation_ref": "op_rest_patch"}},
                {"steps": [{"kind": "message", "text": "m"}], "terminal": {"kind": "stop"}},
            ]},
        ]},
    }
    compile_doc(doc)


def test_an_unterminated_control_leg_reports_its_own_code():
    """R1/F4: the diagnostic existed but could never fire.

    In a finite acyclic join-free CFG a leg that reaches no exit ALWAYS ends on a
    non-terminal leaf, so the generic leaf check ran first and reported
    PROCESS_IR_SEMANTIC_MISSING_TERMINAL. The specific check now runs before it.
    A code with no reachable path is not a check — and the reason this shipped is
    that the first cut added the code without a test that exercised it.
    """
    from boomi_mcp.compiler.process_ir.contracts import (
        BranchSemanticV1, CfgEdgeV1, CfgNodeV1, MessageSemanticV1,
        SemanticCfgV1, StopSemanticV1,
    )
    from boomi_mcp.compiler.process_ir.invariants import check_cfg_invariants

    def node(o, s, p, e=None):
        return CfgNodeV1(node_id="n%d" % o, ordinal=o, source_path=p, semantic=s, exit_role=e)

    def edge(o, a, b, k, l, p, leg=None):
        return CfgEdgeV1(edge_id="e%d" % o, ordinal=o, source_node_id="n%d" % a,
                         target_node_id="n%d" % b, kind=k, local_ordinal=l,
                         provenance_path=p, leg_ordinal=leg)

    B = "/body/steps/0"
    cfg = SemanticCfgV1(
        entry_node_id="n1",
        nodes=(node(1, BranchSemanticV1(leg_count=2), B),
               node(2, MessageSemanticV1(text="x"), B + "/legs/0/steps/0"),   # leg 1 reaches no exit
               node(3, MessageSemanticV1(text="y"), B + "/legs/1/steps/0"),
               node(4, StopSemanticV1(), B + "/legs/1/terminal", e="stop")),
        edges=(edge(1, 1, 2, "branch_leg", 1, B + "/legs/0", 1),
               edge(2, 1, 3, "branch_leg", 2, B + "/legs/1", 2),
               edge(3, 3, 4, "terminal", 1, B + "/legs/1/terminal")),
        exit_node_ids=("n4",),
    )
    with pytest.raises(ProcessIRCompileError) as excinfo:
        check_cfg_invariants(cfg)
    assert excinfo.value.diagnostics[0].code == PROCESS_IR_SEMANTIC_UNTERMINATED_PATH
    assert excinfo.value.diagnostics[0].path == B + "/legs/0"


# ---------------------------------------------------------------------------
# Codex review round 2
# ---------------------------------------------------------------------------


def _dec(true_body, false_body):
    return {"kind": "decision", "comparison": "equals",
            "left": {"value_type": "static", "static_value": "a"},
            "right": {"value_type": "static", "static_value": "b"},
            "true_arm": true_body, "false_arm": false_body}


def test_process_call_mixing_is_checked_along_the_whole_path_not_just_the_root():
    """R2/F1: the first fix asked "is the root control-only?" and returned early.

    A connector can sit in an OUTER control body while the process_call sits in a
    NESTED one — both on one root-to-leaf path, under a control-only root. The
    rule is about the PATH, so it has to walk the path.
    """
    PC = {"kind": "process_call", "process_ref": "child_process"}
    MSG = {"kind": "message", "text": "m"}
    doc = {"version": "1", "body": {"kind": "sequence", "steps": [
        {"kind": "branch", "legs": [
            {"steps": [call("op_rest_get", action="GET")],
             "terminal": _dec({"steps": [PC], "terminal": {"kind": "stop"}},
                              {"steps": [], "terminal": {"kind": "stop"}})},
            {"steps": [MSG], "terminal": {"kind": "stop"}},
        ]},
    ]}}
    with pytest.raises(ProcessIRValidationError) as excinfo:
        parse_process_ir_v1(doc)
    assert excinfo.value.diagnostics[0].code == PROCESS_IR_CAPABILITY_NODE_NOT_ALLOWED_IN_BODY

    # The same nesting WITHOUT a connector upstream stays legal — the rule bans
    # the mix, not the depth.
    ok = {"version": "1", "body": {"kind": "sequence", "steps": [
        {"kind": "branch", "legs": [
            {"steps": [MSG],
             "terminal": _dec({"steps": [PC], "terminal": {"kind": "stop"}},
                              {"steps": [], "terminal": {"kind": "stop"}})},
            {"steps": [MSG], "terminal": {"kind": "stop"}},
        ]},
    ]}}
    compile_doc(ok)


def test_the_termination_prepass_does_not_steal_non_control_diagnostics():
    """R2/F2: moving the check earlier let it judge edges it does not own.

    A malformed CFG can hang a ``branch_leg`` edge off a LINEAR node. That is an
    invalid successor, not an unterminated control path, and the per-node rule
    that says so must still be the one that fires.
    """
    from boomi_mcp.compiler.process_ir.contracts import (
        CfgEdgeV1, CfgNodeV1, MessageSemanticV1, SemanticCfgV1,
    )
    from boomi_mcp.compiler.process_ir.invariants import check_cfg_invariants

    def node(o, p):
        return CfgNodeV1(node_id="n%d" % o, ordinal=o, source_path=p,
                         semantic=MessageSemanticV1(text="x"))

    cfg = SemanticCfgV1(
        entry_node_id="n1",
        nodes=(node(1, "/body/steps/0"), node(2, "/body/steps/1")),
        edges=(CfgEdgeV1(edge_id="e1", ordinal=1, source_node_id="n1", target_node_id="n2",
                         kind="branch_leg", local_ordinal=1,
                         provenance_path="/body/steps/0", leg_ordinal=1),),
        exit_node_ids=(),
    )
    with pytest.raises(ProcessIRCompileError) as excinfo:
        check_cfg_invariants(cfg)
    assert excinfo.value.diagnostics[0].code == PROCESS_IR_SEMANTIC_AMBIGUOUS_FLOW


# ---------------------------------------------------------------------------
# Architect impl-review round 1 — completed gates
# ---------------------------------------------------------------------------


def test_a_decision_arm_admits_at_most_one_process_call():
    """The capture attests exactly one `decision ->true-> processcall` (twice).

    A CHAIN of process calls on an arm is unproven, so it stays closed even
    though the Branch-leg rule is deliberately plural.
    """
    PC = {"kind": "process_call", "process_ref": "child_process"}
    MSG = {"kind": "message", "text": "m"}
    ok = {"version": "1", "body": {"kind": "sequence", "steps": [
        {"kind": "branch", "legs": [
            {"steps": [MSG], "terminal": _dec({"steps": [PC], "terminal": {"kind": "stop"}},
                                              {"steps": [], "terminal": {"kind": "stop"}})},
            {"steps": [MSG], "terminal": {"kind": "stop"}},
        ]},
    ]}}
    compile_doc(ok)

    too_many = {"version": "1", "body": {"kind": "sequence", "steps": [
        {"kind": "branch", "legs": [
            {"steps": [MSG], "terminal": _dec({"steps": [PC, PC], "terminal": {"kind": "stop"}},
                                              {"steps": [], "terminal": {"kind": "stop"}})},
            {"steps": [MSG], "terminal": {"kind": "stop"}},
        ]},
    ]}}
    with pytest.raises(ProcessIRValidationError) as excinfo:
        parse_process_ir_v1(too_many)
    assert excinfo.value.diagnostics[0].code == PROCESS_IR_CAPABILITY_NODE_NOT_ALLOWED_IN_BODY


def test_a_map_must_follow_its_call_immediately():
    """`call -> message -> map -> call` is NOT bracketed.

    A Message REPLACES the document, so the upstream call's response profile is
    no longer what feeds the map. Branch/Decision stay transparent — they route
    documents without altering them.
    """
    MSG = {"kind": "message", "text": "m"}
    doc = {"version": "1", "body": {"kind": "sequence", "steps": [
        call("op_rest_get", action="GET"),
        {"kind": "branch", "legs": [
            {"steps": [MSG, {"kind": "map_ref", "map_ref": "map_rest_to_patch"},
                       call("op_rest_patch", action="PATCH")],
             "terminal": {"kind": "stop"}},
            {"steps": [MSG], "terminal": {"kind": "stop"}},
        ]},
    ]}}
    assert codes_for(doc)[0][0] == PROCESS_IR_SEMANTIC_PROFILE_MISMATCH
    # ...while the branch itself remains transparent (the mixed fixture relies on
    # the root call still pairing with each leg's map).
    compile_doc(BRANCH_MIXED_DOC)


def test_whole_document_diagnostics_name_the_offending_node():
    """Depth and mixing are PATH properties; a model validator could only ever
    report the document root, which is true and useless."""
    MSG = {"kind": "message", "text": "m"}
    deep = {"version": "1", "body": {"kind": "sequence", "steps": [
        _dec({"steps": [MSG], "terminal": _dec(
            {"steps": [MSG], "terminal": _dec({"steps": [MSG], "terminal": {"kind": "stop"}},
                                              {"steps": [], "terminal": {"kind": "stop"}})},
            {"steps": [], "terminal": {"kind": "stop"}})},
             {"steps": [], "terminal": {"kind": "stop"}}),
    ]}}
    with pytest.raises(ProcessIRValidationError) as excinfo:
        parse_process_ir_v1(deep)
    d = excinfo.value.diagnostics[0]
    assert d.code == PROCESS_IR_SEMANTIC_NESTING_LIMIT
    assert d.path == "/body/steps/0/true_arm/terminal/true_arm/terminal"


def test_a_control_subtree_may_not_escape_its_own_region():
    """The per-node rule binds only an edge's FIRST target. A subtree that
    escapes into a sibling region one node later is the same cross-wiring
    defect, just deeper."""
    from boomi_mcp.compiler.process_ir.contracts import (
        BranchSemanticV1, CfgEdgeV1, CfgNodeV1, MessageSemanticV1,
        SemanticCfgV1, StopSemanticV1,
    )
    from boomi_mcp.compiler.process_ir.invariants import check_cfg_invariants

    B = "/body/steps/0"
    def n(o, s, p, e=None):
        return CfgNodeV1(node_id="n%d" % o, ordinal=o, source_path=p, semantic=s, exit_role=e)
    def e(o, a, b, k, l, p, leg=None):
        return CfgEdgeV1(edge_id="e%d" % o, ordinal=o, source_node_id="n%d" % a,
                         target_node_id="n%d" % b, kind=k, local_ordinal=l,
                         provenance_path=p, leg_ordinal=leg)
    cfg = SemanticCfgV1(
        entry_node_id="n1",
        nodes=(n(1, BranchSemanticV1(leg_count=2), B),
               n(2, MessageSemanticV1(text="a"), B + "/legs/0/steps/0"),
               # leg 1's own step routes into LEG 2's region
               n(3, StopSemanticV1(), B + "/legs/1/terminal", e="stop"),
               n(4, MessageSemanticV1(text="b"), B + "/legs/1/steps/0"),
               n(5, StopSemanticV1(), B + "/legs/1/steps/1", e="stop")),
        edges=(e(1, 1, 2, "branch_leg", 1, B + "/legs/0", 1),
               e(2, 1, 4, "branch_leg", 2, B + "/legs/1", 2),
               e(3, 2, 3, "terminal", 1, B + "/legs/0/terminal"),
               e(4, 4, 5, "terminal", 1, B + "/legs/1/steps/1")),
        exit_node_ids=("n3", "n5"),
    )
    with pytest.raises(ProcessIRCompileError) as excinfo:
        check_cfg_invariants(cfg)
    assert excinfo.value.diagnostics[0].code == PROCESS_IR_SEMANTIC_AMBIGUOUS_FLOW
    assert "escapes its own" in excinfo.value.diagnostics[0].message


def test_twenty_five_legs_compile_with_order_preserved_end_to_end():
    """The upper Branch bound, exercised through CFG, plan and XML — not just
    the schema bound."""
    legs = [{"steps": [{"kind": "message", "text": "leg%d" % i}],
             "terminal": {"kind": "stop"}} for i in range(25)]
    doc = {"version": "1", "body": {"kind": "sequence", "steps": [
        {"kind": "branch", "legs": legs},
    ]}}
    (cfg, plan), table = compile_doc(doc)
    branch_cfg = next(n for n in cfg.nodes if n.semantic.semantic_kind == "branch")
    edges = [e for e in cfg.edges if e.source_node_id == branch_cfg.node_id]
    assert [e.leg_ordinal for e in edges] == list(range(1, 26))
    plan_branch = next(n for n in plan.nodes if n.emitter_input.emitter_kind == "branch")
    assert [t.identifier for t in plan_branch.outgoing] == [str(i) for i in range(1, 26)]
    assert plan_branch.emitter_input.num_branches == 25
    artifact = emit_process(plan, table)
    assert 'numBranches="25"' in artifact.process_xml
    assert not verify_process_graph(artifact.process_xml).get("errors")


def test_terminal_registry_rows_match_the_model_terminal_unions():
    """Bidirectional, like the step rows — a terminal union that gains a kind
    without a deliberate registry change must fail the build."""
    import typing
    def terminal_kinds(model):
        ann = model.model_fields["terminal"].annotation
        args = typing.get_args(ann)
        members = typing.get_args(args[0]) if args and typing.get_args(args[0]) else args
        return {typing.get_args(m.model_fields["kind"].annotation)[0] for m in members}

    assert bodycaps.BODY_CAPABILITIES_V1[(bodycaps.BRANCH_LEG, bodycaps.TERMINAL_SLOT)] == \
        terminal_kinds(ir_module.BranchLegV1)
    assert bodycaps.BODY_CAPABILITIES_V1[(bodycaps.DECISION_TRUE_ARM, bodycaps.TERMINAL_SLOT)] == \
        terminal_kinds(ir_module.DecisionTrueArmV1)
    assert bodycaps.BODY_CAPABILITIES_V1[(bodycaps.DECISION_FALSE_ARM, bodycaps.TERMINAL_SLOT)] == \
        terminal_kinds(ir_module.DecisionFalseArmV1)


def test_a_bare_false_stop_is_a_verifier_WARNING_not_an_error():
    """Intentional document dropping is operationally notable but legal — the
    graph verifier must not treat it as a failure."""
    (_cfg, plan), table = compile_doc(DECISION_NESTED_DOC)
    report = verify_process_graph(emit_process(plan, table).process_xml)
    assert not report.get("errors"), report


_GOLDEN_XML = _ROOT / "tests" / "fixtures" / "golden_xml"


@pytest.mark.parametrize(
    "doc,golden",
    [
        (BRANCH_MIXED_DOC, "process_ir_rich_branch_mixed_connectors.xml"),
        (DECISION_NESTED_DOC, "process_ir_rich_decision_nested_bare_false_stop.xml"),
        (PROCESS_CALL_BRANCH_DOC, "process_ir_rich_branch_process_call.xml"),
    ],
)
def test_rich_fixtures_match_their_frozen_xml_golden(doc, golden):
    """A FROZEN golden, not two fresh emissions compared with each other.

    Comparing one emission against another proves determinism but not that the
    bytes are the intended ones — any change would move both sides together.
    These files are the byte contract; a diff here is a deliberate review item.
    """
    (_cfg, plan), table = compile_doc(doc)
    assert emit_process(plan, table).process_xml == (_GOLDEN_XML / golden).read_text()


def test_the_committed_decision_fixture_matches_the_compiled_document():
    import json
    assert json.loads(
        (_FIXTURES / "decision_nested_bare_false_stop.json").read_text()
    ) == DECISION_NESTED_DOC


# ---------------------------------------------------------------------------
# Codex review round 4 (repo gate) — both public entry points, both fields
# ---------------------------------------------------------------------------


def test_the_mixing_gate_holds_on_the_COMPILER_entry_point_too():
    """``ProcessIRV1`` is exported, so a caller can skip ``parse_process_ir_v1``.

    Moving the check into the parser (for a precise pointer) left the compiler
    path unguarded — a gate that only one of two public entry points enforces is
    not a gate. Both now hold, from two independent walks.
    """
    from boomi_mcp.models.process_ir import ProcessIRV1

    PC = {"kind": "process_call", "process_ref": "child_process"}
    MSG = {"kind": "message", "text": "m"}
    doc = {"version": "1", "body": {"kind": "sequence", "steps": [
        {"kind": "branch", "legs": [
            {"steps": [call("op_rest_get", action="GET")],
             "terminal": _dec({"steps": [PC], "terminal": {"kind": "stop"}},
                              {"steps": [], "terminal": {"kind": "stop"}})},
            {"steps": [MSG], "terminal": {"kind": "stop"}},
        ]},
    ]}}
    # Bypass the parser entirely.
    ir = ProcessIRV1.model_validate(doc)
    with pytest.raises(ProcessIRCompileError) as excinfo:
        compile_process_ir_v1(ir, rich_symbols())
    assert excinfo.value.diagnostics[0].code == PROCESS_IR_CAPABILITY_NODE_NOT_ALLOWED_IN_BODY
    # ...and the parser still gives its precise diagnostic.
    with pytest.raises(ProcessIRValidationError):
        parse_process_ir_v1(doc)


def test_control_wiring_code_does_not_depend_on_which_field_was_corrupted():
    """Corrupting only ``to_shape_id`` used to fall through to the generic code."""
    from boomi_mcp.compiler.process_ir.contracts import EmissionPlanV1
    from boomi_mcp.compiler.process_ir.invariants import check_emission_plan_invariants
    from boomi_mcp.errors import PROCESS_IR_COMPILE_CONTROL_WIRING_INVALID

    (cfg, plan), table = compile_doc(BRANCH_MIXED_DOC)
    branch_index = next(
        i for i, n in enumerate(plan.nodes) if n.emitter_input.emitter_kind == "branch"
    )
    branch = plan.nodes[branch_index]
    # Keep the cfg_edge_id sequence intact; corrupt ONLY the target.
    wires = list(branch.outgoing)
    wires[0] = wires[0].model_copy(update={"to_shape_id": wires[1].to_shape_id})
    nodes = list(plan.nodes)
    nodes[branch_index] = branch.model_copy(update={"outgoing": tuple(wires)})
    broken = plan.model_copy(update={"nodes": tuple(nodes)})

    with pytest.raises(ProcessIRCompileError) as excinfo:
        check_emission_plan_invariants(broken, cfg, table)
    assert excinfo.value.diagnostics[0].code == PROCESS_IR_COMPILE_CONTROL_WIRING_INVALID


def test_the_compiler_gate_catches_a_connector_APPENDED_to_a_process_call_body():
    """``ProcessIRV1`` is exported AND mutable.

    A caller can validate a legal ProcessCall-only leg, append a connector call
    to it, and hand the model to the compiler — so the compiler gate must look at
    the CURRENT body, not only at ancestors. Trusting the model's own path-mode
    invariant is what made the "independent enforcement point" not independent.
    """
    from boomi_mcp.compiler.process_ir.body_capabilities import validate_body_capabilities

    ir = parse_process_ir_v1(PROCESS_CALL_BRANCH_DOC)          # legal when parsed
    leg = ir.body.steps[0].legs[0]
    leg.steps.append(ir_module.ConnectorCallNodeV1(            # ...then mutated
        kind="connector_call", operation_ref="op_rest_get"
    ))
    with pytest.raises(ProcessIRCompileError) as excinfo:
        validate_body_capabilities(ir)
    assert excinfo.value.diagnostics[0].code == PROCESS_IR_CAPABILITY_NODE_NOT_ALLOWED_IN_BODY


def test_control_wiring_code_is_independent_of_the_corrupted_TARGET_value():
    """Corrupting a control target to a NONEXISTENT shape must classify the same
    as corrupting it to another real one — the code follows the node kind, not
    the value that happened to be written."""
    from boomi_mcp.compiler.process_ir.invariants import check_emission_plan_invariants
    from boomi_mcp.errors import PROCESS_IR_COMPILE_CONTROL_WIRING_INVALID

    (cfg, plan), table = compile_doc(BRANCH_MIXED_DOC)
    i = next(i for i, n in enumerate(plan.nodes) if n.emitter_input.emitter_kind == "branch")
    branch = plan.nodes[i]
    for bogus in ("shape999", plan.nodes[-1].shape_id):
        wires = list(branch.outgoing)
        wires[0] = wires[0].model_copy(update={"to_shape_id": bogus})
        nodes = list(plan.nodes)
        nodes[i] = branch.model_copy(update={"outgoing": tuple(wires)})
        with pytest.raises(ProcessIRCompileError) as excinfo:
            check_emission_plan_invariants(
                plan.model_copy(update={"nodes": tuple(nodes)}), cfg, table
            )
        assert excinfo.value.diagnostics[0].code == PROCESS_IR_COMPILE_CONTROL_WIRING_INVALID, bogus


def test_the_mixing_gate_propagates_in_BOTH_directions():
    """The rule is symmetric: "these two may not share a path".

    Carrying only the connector direction caught `connector -> ... -> process_call`
    but not `process_call -> ... -> connector`, which a mutable model expresses
    just as easily.
    """
    from boomi_mcp.compiler.process_ir.body_capabilities import validate_body_capabilities

    ir = parse_process_ir_v1(PROCESS_CALL_BRANCH_DOC)
    leg = ir.body.steps[0].legs[0]
    # process_call above -> nested decision -> connector_call below
    leg.terminal = ir_module.DecisionNodeV1.model_validate({
        "kind": "decision", "comparison": "equals",
        "left": {"value_type": "static", "static_value": "a"},
        "right": {"value_type": "static", "static_value": "b"},
        "true_arm": {"steps": [{"kind": "connector_call", "operation_ref": "op_rest_get"}],
                     "terminal": {"kind": "stop"}},
        "false_arm": {"steps": [], "terminal": {"kind": "stop"}},
    })
    with pytest.raises(ProcessIRCompileError) as excinfo:
        validate_body_capabilities(ir)
    assert excinfo.value.diagnostics[0].code == PROCESS_IR_CAPABILITY_NODE_NOT_ALLOWED_IN_BODY


def test_control_wiring_code_covers_transition_ROLE_corruption():
    """The plan lists role alongside count, order and target."""
    from boomi_mcp.compiler.process_ir.invariants import check_emission_plan_invariants
    from boomi_mcp.errors import PROCESS_IR_COMPILE_CONTROL_WIRING_INVALID

    (cfg, plan), table = compile_doc(BRANCH_MIXED_DOC)
    i = next(i for i, n in enumerate(plan.nodes) if n.emitter_input.emitter_kind == "branch")
    branch = plan.nodes[i]
    wires = list(branch.outgoing)
    # Corrupt ONLY provenance. Nulling cfg_edge_id as well would trip the
    # earlier ordered-edge-id check, which ALREADY returned the control-wiring
    # code — the test would then pass even with the role classification
    # reverted, i.e. it would not test what it claims.
    wires[0] = wires[0].model_copy(update={"provenance": "synthetic"})
    nodes = list(plan.nodes)
    nodes[i] = branch.model_copy(update={"outgoing": tuple(wires)})
    with pytest.raises(ProcessIRCompileError) as excinfo:
        check_emission_plan_invariants(plan.model_copy(update={"nodes": tuple(nodes)}), cfg, table)
    assert excinfo.value.diagnostics[0].code == PROCESS_IR_COMPILE_CONTROL_WIRING_INVALID
