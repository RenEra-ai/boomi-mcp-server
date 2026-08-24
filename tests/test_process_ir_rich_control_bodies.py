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

import copy

from boomi_mcp.categories.components.process_graph_verifier import verify_process_graph
from boomi_mcp.compiler.process_ir import body_capabilities as bodycaps
from boomi_mcp.compiler.process_ir.contracts import SymbolTableV1
from boomi_mcp.compiler.process_ir.diagnostics import ProcessIRCompileError
from boomi_mcp.compiler.process_ir.emitter_registry import emit_process
from boomi_mcp.compiler.process_ir.pipeline import compile_process_ir_v1
from boomi_mcp.errors import (
    PROCESS_IR_CAPABILITY_NODE_NOT_ALLOWED_IN_BODY,
    PROCESS_IR_CAPABILITY_PROCESS_CALL_RETURN_PATH_BINDING_UNSUPPORTED,
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

# The symbol table and compile harness live in the corpus (#165); this module
# CONSUMES them. The three *_DOC literals below stay here as WITNESSES, pinned
# byte-identically to the committed rich_control/*.json definitions by the
# fixture-equality tests in this file.
import _wave_gate_golden_corpus as _corpus

_symbol = _corpus.rich_symbol
rich_symbols = _corpus.rich_symbols


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


compile_doc = _corpus.rich_compile_doc


def external_writer_for(cache_ref):
    """#143: an authored `external_writer` flag DECLARES an outside writer; a
    typed contract must CONFIRM it. The flag alone no longer downgrades the
    blocking finding — that would be a free-form assertion suppressing a fatal
    rule, which the issue excludes by name."""
    from boomi_mcp.compiler.process_ir.semantic_validation import (
        ExternalWriterContractV1,
        ProcessIRValidationCapabilitiesV1,
    )

    return ProcessIRValidationCapabilitiesV1(
        external_writers=(ExternalWriterContractV1(cache_ref=cache_ref),)
    )


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


# #175: the calls are the leg TERMINALS, with no trailing stop. This document is
# the case definition that owns `process_ir_rich_branch_process_call.xml`, so the
# golden follows from it — the bytes are never hand-edited.
PROCESS_CALL_BRANCH_DOC = {
    "version": "1",
    "body": {
        "kind": "sequence",
        "steps": [
            {
                "kind": "branch",
                "legs": [
                    {
                        "steps": [],
                        "terminal": {"kind": "process_call", "process_ref": "child_process"},
                    },
                    {
                        "steps": [],
                        "terminal": {"kind": "process_call", "process_ref": "child_process"},
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
        # #154 brought the Try/Catch bodies under the same pin. They were the
        # slots that had DRIFTED while unpinned.
        (bodycaps.TRY_BODY, bodycaps.STEP_SLOT, "TryCatchBodyStepV1"),
        (bodycaps.CATCH_BODY, bodycaps.STEP_SLOT, "TryCatchBodyStepV1"),
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


#: The matrix this slice ships, spelled out. This is a REVIEW TRIPWIRE, not a
#: second authority, and the distinction is what makes it safe to write down.
#:
#: Before #154 the production matrix ALSO hand-listed these sets, so a union and
#: its registry row could drift together and no test could tell — which is how
#: ``(try_body, terminal)`` stayed ``{"stop"}`` and the Try/Catch step vocabulary
#: silently lost ``flow_control``/``data_process``. Production now DERIVES every
#: row from the model field (``derive_body_capabilities_v1``), so drift of that
#: kind is no longer expressible. What remains worth having is a diff: a union
#: edit that widens a placement should not be able to reach callers without a
#: human seeing the widened row in a reviewable change. That is the same argument
#: ``test_the_whole_contract_is_frozen_in_a_committed_snapshot`` makes for served
#: prose, and it is why this list may be written by hand while the production one
#: may not.
_SHIPPED_MATRIX_V1 = {
    (bodycaps.BRANCH_LEG, bodycaps.STEP_SLOT): {
        "cache_get", "cache_put", "cache_remove", "connector_call", "data_process",
        "document_cache_retrieve", "flow_control", "map_ref", "message", "set_ddp", "set_dpp",
    },
    # #175 moved `process_call` from the STEP row into the TERMINAL rows.
    (bodycaps.BRANCH_LEG, bodycaps.TERMINAL_SLOT): {
        "target", "cache_put", "stop", "process_call", "decision",
    },
    (bodycaps.DECISION_TRUE_ARM, bodycaps.STEP_SLOT): {
        "cache_get", "cache_put", "cache_remove", "connector_call", "data_process",
        "document_cache_retrieve", "flow_control", "map_ref", "message", "set_ddp", "set_dpp",
    },
    (bodycaps.DECISION_TRUE_ARM, bodycaps.TERMINAL_SLOT): {
        "target", "stop", "exception", "process_call", "branch", "decision",
    },
    (bodycaps.DECISION_FALSE_ARM, bodycaps.STEP_SLOT): {
        "cache_get", "cache_put", "cache_remove", "connector_call", "data_process",
        "document_cache_retrieve", "flow_control", "map_ref", "message", "set_ddp", "set_dpp",
    },
    (bodycaps.DECISION_FALSE_ARM, bodycaps.TERMINAL_SLOT): {
        "stop", "exception", "branch", "decision",
    },
    (bodycaps.TRY_BODY, bodycaps.STEP_SLOT): {
        "cache_get", "cache_put", "cache_remove", "connector_call", "data_process",
        "document_cache_retrieve", "flow_control", "map_ref", "message", "set_ddp", "set_dpp",
    },
    # #154 item 2: the legacy builder wraps a Return Documents terminal inside
    # its process-scoped Try/Catch, so the protected path may end on one.
    (bodycaps.TRY_BODY, bodycaps.TERMINAL_SLOT): {"stop", "return_documents"},
    (bodycaps.CATCH_BODY, bodycaps.STEP_SLOT): {
        "cache_get", "cache_put", "cache_remove", "connector_call", "data_process",
        "document_cache_retrieve", "flow_control", "map_ref", "message", "set_ddp", "set_dpp",
    },
    (bodycaps.CATCH_BODY, bodycaps.TERMINAL_SLOT): {"stop", "exception", "cache_put"},
}


def test_shipped_matrix_review_tripwire():
    """Every widened row lands in a diff a reviewer must approve."""
    assert dict(bodycaps.BODY_CAPABILITIES_V1) == _SHIPPED_MATRIX_V1


def _independent_kinds(annotation):
    """A SECOND reader of a slot annotation, written independently of production.

    Deliberately not ``bodycaps._kinds_from_annotation``. An expectation computed
    by the helper it is meant to verify agrees with that helper by construction
    and proves nothing — the exact vacuity #149 recorded. This walker is written
    against ``typing`` directly so the two implementations can DISAGREE, which is
    the only way the comparison carries information.
    """
    import typing

    origin = typing.get_origin(annotation)
    if origin is not None and str(origin).endswith("Annotated"):
        return _independent_kinds(typing.get_args(annotation)[0])
    args = typing.get_args(annotation)
    if origin in (list, tuple, set, frozenset) and args:
        return _independent_kinds(args[0])
    if origin is typing.Union:
        out = set()
        for member in args:
            out |= _independent_kinds(member)
        return out
    if hasattr(annotation, "model_fields") and "kind" in annotation.model_fields:
        return {typing.get_args(annotation.model_fields["kind"].annotation)[0]}
    # Annotated[X, FieldInfo] where get_origin() did not report Annotated.
    if args:
        return _independent_kinds(args[0])
    return set()


@pytest.mark.parametrize("key", sorted(bodycaps.BODY_SLOT_AUTHORITIES_V1))
def test_every_matrix_row_is_derived_from_its_slot_authority(key):
    """BOTH directions, for all TEN slots, via an independent reader.

    #154. Before this, only the three Branch/Decision step rows and the three
    Branch/Decision terminal rows were pinned to their unions; the Try/Catch rows
    were pinned to nothing, which is why they were the ones that drifted.
    """
    model, field_name = bodycaps.BODY_SLOT_AUTHORITIES_V1[key]
    expected = _independent_kinds(model.model_fields[field_name].annotation)
    assert expected, f"independent reader found no kinds for {key} — the test would be vacuous"
    assert set(bodycaps.BODY_CAPABILITIES_V1[key]) == expected


def test_authority_table_covers_every_body_model_in_the_schema():
    """A NEW body context cannot silently lack placement rows.

    The authority is the model set itself: every ProcessIR model carrying BOTH a
    ``steps`` and a ``terminal`` field is a control body, and every control body
    owes two rows. Enumerating the models we happen to remember would re-create
    the omission this test exists to catch.
    """
    bodies = _control_body_models(ir_module.ProcessIRV1)
    assert bodies, "model walk found no control bodies — the test would be vacuous"
    covered = {model for model, _field in bodycaps.BODY_SLOT_AUTHORITIES_V1.values()}
    assert covered == bodies, (
        "every control body owes a step row and a terminal row; "
        f"uncovered={sorted(m.__name__ for m in bodies - covered)} "
        f"unknown={sorted(m.__name__ for m in covered - bodies)}"
    )
    for model in bodies:
        slots = {field for m, field in bodycaps.BODY_SLOT_AUTHORITIES_V1.values() if m is model}
        assert slots == {"steps", "terminal"}, model.__name__


def _control_body_models(root):
    """Every control body reachable from ``root`` — the authority's case set.

    Extracted from the coverage test above (#180) so a witness can run the SAME
    walk over a SYNTHETIC root. Inline, the walk could only ever be pointed at
    the real schema, so nothing could show that the comparison it feeds would
    actually fail on an uncovered body context.
    """
    bodies = set()
    stack, seen = [root], set()
    while stack:
        model = stack.pop()
        if model in seen:
            continue
        seen.add(model)
        fields = getattr(model, "model_fields", {})
        if "steps" in fields and "terminal" in fields:
            bodies.add(model)
        for field in fields.values():
            for nested in _reachable_models(field.annotation):
                if nested not in seen:
                    stack.append(nested)
    return bodies


def _reachable_models(annotation):
    import typing

    out = []
    stack = [annotation]
    while stack:
        current = stack.pop()
        if hasattr(current, "model_fields"):
            out.append(current)
            continue
        stack.extend(typing.get_args(current))
    return out


def test_no_derived_row_is_empty():
    """An empty row denies every placement — fail LOUD, never ship it."""
    for key, kinds in bodycaps.BODY_CAPABILITIES_V1.items():
        assert kinds, key


# --- non-vacuity witnesses: prove the derivation actually tracks the models ---


def test_mutant_terminal_union_moves_the_derived_row():
    """The witness for the row #154 had to widen.

    Constructs a try-body model whose terminal union additionally admits
    ``exception``, PROVES the mutant took effect, then proves the derived row
    follows it — so a matrix that had stayed ``{"stop"}`` would be caught.
    """
    import typing
    from typing import Annotated, Union
    from pydantic import Field, create_model

    mutant = create_model(
        "MutantTryBodyV1",
        steps=(ir_module.TryCatchTryBodyV1.model_fields["steps"].annotation, ...),
        terminal=(
            Annotated[
                Union[ir_module.StopNodeV1, ir_module.ExceptionNodeV1],
                Field(discriminator="kind"),
            ],
            ...,
        ),
    )
    # The mutation took effect: the model really does admit `exception` now.
    assert "exception" in _independent_kinds(mutant.model_fields["terminal"].annotation)

    derived = bodycaps.derive_body_capabilities_v1(
        {(bodycaps.TRY_BODY, bodycaps.TERMINAL_SLOT): (mutant, "terminal")}
    )
    assert derived[(bodycaps.TRY_BODY, bodycaps.TERMINAL_SLOT)] == {"stop", "exception"}
    # ...and the shipped row would therefore no longer match the tripwire.
    assert derived[(bodycaps.TRY_BODY, bodycaps.TERMINAL_SLOT)] != _SHIPPED_MATRIX_V1[
        (bodycaps.TRY_BODY, bodycaps.TERMINAL_SLOT)
    ]


def test_mutant_step_union_moves_the_derived_row():
    """Same witness in the STEP slot — the drift #154 actually found."""
    from typing import Annotated, List, Union
    from pydantic import Field, create_model

    narrowed = Annotated[
        Union[ir_module.MessageNodeV1, ir_module.MapRefNodeV1], Field(discriminator="kind")
    ]
    mutant = create_model(
        "MutantBodyV1",
        steps=(List[narrowed], ...),
        terminal=(ir_module.TryCatchTryBodyV1.model_fields["terminal"].annotation, ...),
    )
    assert _independent_kinds(mutant.model_fields["steps"].annotation) == {"message", "map_ref"}

    derived = bodycaps.derive_body_capabilities_v1(
        {(bodycaps.TRY_BODY, bodycaps.STEP_SLOT): (mutant, "steps")}
    )
    assert derived[(bodycaps.TRY_BODY, bodycaps.STEP_SLOT)] == {"message", "map_ref"}
    assert "flow_control" not in derived[(bodycaps.TRY_BODY, bodycaps.STEP_SLOT)]


def test_derivation_refuses_an_empty_row_rather_than_denying_everything():
    """A slot whose annotation yields nothing must RAISE, not ship {}.

    ``is_allowed`` reads absence as denial, so an empty derived row would silently
    reject every placement in that slot — a fail-closed-looking result produced by
    a broken derivation rather than by a decision.
    """
    from pydantic import create_model

    mutant = create_model("NoKindBodyV1", steps=(list, ...), terminal=(int, ...))
    assert _independent_kinds(mutant.model_fields["terminal"].annotation) == set()
    with pytest.raises(RuntimeError, match="EMPTY kind set"):
        bodycaps.derive_body_capabilities_v1(
            {(bodycaps.TRY_BODY, bodycaps.TERMINAL_SLOT): (mutant, "terminal")}
        )


def test_derivation_refuses_an_authority_naming_a_missing_field():
    with pytest.raises(RuntimeError, match="names no field"):
        bodycaps.derive_body_capabilities_v1(
            {(bodycaps.TRY_BODY, bodycaps.TERMINAL_SLOT): (ir_module.TryCatchTryBodyV1, "nope")}
        )


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
    # #175: the call is now the leg TERMINAL, which is precisely why the ancestor
    # walk had to learn to look at terminals — checking only `steps` would let
    # this mixed path through the moment the kind moved slots.
    doc = {
        "version": "1",
        "body": {"kind": "sequence", "steps": [
            call("op_rest_get", action="GET"),
            {"kind": "branch", "legs": [
                {"steps": [],
                 "terminal": {"kind": "process_call", "process_ref": "child_process"}},
                {"steps": [{"kind": "message", "text": "m"}], "terminal": {"kind": "stop"}},
            ]},
        ]},
    }
    with pytest.raises(ProcessIRValidationError) as excinfo:
        parse_process_ir_v1(doc)
    assert excinfo.value.diagnostics[0].code == PROCESS_IR_CAPABILITY_NODE_NOT_ALLOWED_IN_BODY
    assert excinfo.value.diagnostics[0].path.endswith("/terminal"), (
        excinfo.value.diagnostics[0].path
    )
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
                           {"kind": "cache_get", "cache_ref": "doc_cache",
                            "empty_cache_behavior": "stopprocess",
                            "external_writer": True}],
                 "terminal": {"kind": "stop"}},
                {"steps": [{"kind": "message", "text": "m"}], "terminal": {"kind": "stop"}},
            ]},
        ]},
    }
    compile_doc(doc, capabilities=external_writer_for("doc_cache"))


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
             "terminal": _dec({"steps": [], "terminal": PC},
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
             "terminal": _dec({"steps": [], "terminal": PC},
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


def test_a_decision_arm_admits_exactly_one_terminal_process_call():
    """The capture attests exactly one `decision ->true-> processcall` (twice).

    #141 enforced "at most one" with an explicit `max_calls=1` bound on a list of
    STEPS. #175 makes the bound structural instead: the call is the arm's single
    TERMINAL, so a chain has nowhere to be written and the counting rule is gone
    rather than merely satisfied. A rule you cannot express is stronger than a
    rule you have to remember to check.
    """
    PC = {"kind": "process_call", "process_ref": "child_process"}
    MSG = {"kind": "message", "text": "m"}
    ok = {"version": "1", "body": {"kind": "sequence", "steps": [
        {"kind": "branch", "legs": [
            {"steps": [MSG], "terminal": _dec({"steps": [], "terminal": PC},
                                              {"steps": [], "terminal": {"kind": "stop"}})},
            {"steps": [MSG], "terminal": {"kind": "stop"}},
        ]},
    ]}}
    compile_doc(ok)

    # A chain: the second call would need the first child's return paths to be
    # reachable at all, so it is a continuation request, not a cardinality slip.
    too_many = {"version": "1", "body": {"kind": "sequence", "steps": [
        {"kind": "branch", "legs": [
            {"steps": [MSG], "terminal": _dec({"steps": [PC], "terminal": PC},
                                              {"steps": [], "terminal": {"kind": "stop"}})},
            {"steps": [MSG], "terminal": {"kind": "stop"}},
        ]},
    ]}}
    with pytest.raises(ProcessIRValidationError) as excinfo:
        parse_process_ir_v1(too_many)
    assert (
        excinfo.value.diagnostics[0].code
        == PROCESS_IR_CAPABILITY_PROCESS_CALL_RETURN_PATH_BINDING_UNSUPPORTED
    ), [(d.code, d.path) for d in excinfo.value.diagnostics]


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
             "terminal": _dec({"steps": [], "terminal": PC},
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


def test_the_call_above_connector_direction_is_shadowed_not_silently_dropped():
    """The symmetric mixing direction is UNREACHABLE after #175 — pinned, not assumed.

    `_walk_body` still carries `process_call_above` so that "these two may not
    share a path" propagates both ways. But #175 made a call a TERMINAL, and a
    terminal ends its body — so the only way a call can sit ABOVE anything is in
    a `steps` slot, which is now itself a continuation request and is caught
    first. The symmetric branch is therefore shadowed.

    This test exists because the previous version of it was VACUOUS: it mutated a
    leg to hold a call in `steps` with a connector-bearing Decision below, and
    asserted `PROCESS_IR_CAPABILITY_NODE_NOT_ALLOWED_IN_BODY` — which the generic
    slot check produced the moment `process_call` left the step union, never
    reaching the mixing propagation it claimed to prove. A guard nothing can
    reach passes whether or not the property holds, so the honest thing is to
    pin the SHADOWING: if a later change makes the call-above direction
    expressible again, this fails and forces the symmetric guard to be re-proven
    rather than quietly trusted.
    """
    from boomi_mcp.compiler.process_ir.body_capabilities import validate_body_capabilities

    ir = parse_process_ir_v1(PROCESS_CALL_BRANCH_DOC)
    leg = ir.body.steps[0].legs[0]
    leg.steps = [ir_module.ProcessCallNodeV1.model_validate(
        {"kind": "process_call", "process_ref": "child_process"}
    )]
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
    # The CONTINUATION is what this document actually gets wrong first: a call in
    # a step slot is invalid regardless of what sits below it.
    assert (
        excinfo.value.diagnostics[0].code
        == PROCESS_IR_CAPABILITY_PROCESS_CALL_RETURN_PATH_BINDING_UNSUPPORTED
    ), [(d.code, d.path) for d in excinfo.value.diagnostics]
    assert excinfo.value.diagnostics[0].path.endswith("/legs/0/steps/0")


def test_the_connector_above_call_direction_is_still_live_and_enforced():
    """The direction that IS reachable must stay proven, or removing the vacuous
    test above would have left the mixing gate with no compiler-side witness at
    all. A connector in the leg's steps with the call as the leg TERMINAL is
    expressible by authoring, needs no mutation, and must report mixing."""
    from boomi_mcp.compiler.process_ir.body_capabilities import validate_body_capabilities

    # The MODEL refuses this shape at construction (its own connector-precedence
    # check), so the compiler-side gate is reached the same way every other
    # mutable-model test reaches it: build a legal document, then append the
    # connector afterwards. That is precisely the bypass the compiler gate
    # exists for — an exported model handed straight to `compile_process_ir_v1`.
    ir = parse_process_ir_v1(PROCESS_CALL_BRANCH_DOC)
    ir.body.steps[0].legs[0].steps = [
        ir_module.ConnectorCallNodeV1.model_validate(
            {"kind": "connector_call", "operation_ref": "op_rest_get", "action": "GET"}
        )
    ]
    with pytest.raises(ProcessIRCompileError) as excinfo:
        validate_body_capabilities(ir)
    assert excinfo.value.diagnostics[0].code == PROCESS_IR_CAPABILITY_NODE_NOT_ALLOWED_IN_BODY
    assert "process_call_connector_mixing" in excinfo.value.diagnostics[0].message


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


# ---------------------------------------------------------------------------
# #175 round 3 — ONE authority for process_call placement, rendered twice.
#
# Both public entry points enforce this rule: authoring through
# `parse_process_ir_v1`, and a mutated exported model handed straight to
# `compile_process_ir_v1`. While each carried its own copy of the decision the
# two disagreed on FOUR documents — the prefix pointer, the
# connector-with-call-terminal pointer, and two orderings where one path
# reported mixing and the other the return-path rule. The whole suite was green
# throughout: nothing pinned the agreement, which is why the divergence
# survived three review rounds. This is that pin.
# ---------------------------------------------------------------------------

_PLACEMENT_DPP = {
    "kind": "set_dpp", "name": "DPP_P", "source_values": [{"value_type": "current"}],
}
_PLACEMENT_CONN = {"kind": "connector_call", "operation_ref": "op_rest_get", "action": "GET"}
_PLACEMENT_CALL = {"kind": "process_call", "process_ref": "child_process"}
_PLACEMENT_STOP = {"kind": "stop"}

#: Every same-body placement a Branch leg can express, legal and illegal. The
#: legal row is load-bearing: without it a verdict that simply raised on every
#: body would satisfy every other row.
_PLACEMENT_CASES = [
    ("call_in_steps", [_PLACEMENT_CALL], _PLACEMENT_STOP),
    ("prefix_then_call_terminal", [_PLACEMENT_DPP], _PLACEMENT_CALL),
    ("call_in_steps_and_call_terminal", [_PLACEMENT_CALL], _PLACEMENT_CALL),
    ("connector_then_call_terminal", [_PLACEMENT_CONN], _PLACEMENT_CALL),
    ("call_then_connector", [_PLACEMENT_CALL, _PLACEMENT_CONN], _PLACEMENT_STOP),
    ("connector_then_call", [_PLACEMENT_CONN, _PLACEMENT_CALL], _PLACEMENT_STOP),
    ("bare_call_terminal_is_legal", [], _PLACEMENT_CALL),
]


def _placement_doc(steps, terminal):
    return {"version": "1", "body": {"kind": "sequence", "steps": [{
        "kind": "branch", "legs": [
            {"steps": copy.deepcopy(steps), "terminal": copy.deepcopy(terminal)},
            {"steps": [copy.deepcopy(_PLACEMENT_DPP)], "terminal": {"kind": "stop"}},
        ]}]}}


def _placement_node(payload):
    name = {
        "set_dpp": "SetDppNodeV1", "connector_call": "ConnectorCallNodeV1",
        "process_call": "ProcessCallNodeV1", "stop": "StopNodeV1",
    }[payload["kind"]]
    return getattr(ir_module, name).model_validate(copy.deepcopy(payload))


def _placement_via_parser(steps, terminal, doc=None):
    try:
        parse_process_ir_v1(doc if doc is not None else _placement_doc(steps, terminal))
    except ProcessIRValidationError as exc:
        first = exc.diagnostics[0]
        return (first.code, first.path, first.message)
    return None


def _placement_via_compiler(steps, terminal):
    """Reach the compiler with a model the parser would have refused: build a
    legal leg, then overwrite its slots the way an exported model can be mutated.
    That bypass is precisely what the compiler-side check exists for."""
    from boomi_mcp.compiler.process_ir.body_capabilities import validate_body_capabilities

    ir = parse_process_ir_v1(_placement_doc([_PLACEMENT_DPP], {"kind": "stop"}))
    leg = ir.body.steps[0].legs[0]
    leg.steps = [_placement_node(s) for s in steps]
    leg.terminal = _placement_node(terminal)
    try:
        validate_body_capabilities(ir)
    except ProcessIRCompileError as exc:
        first = exc.diagnostics[0]
        return (first.code, first.path, first.message)
    return None


@pytest.mark.parametrize(
    "label,steps,terminal", _PLACEMENT_CASES, ids=[c[0] for c in _PLACEMENT_CASES]
)
def test_both_entry_points_agree_on_process_call_placement(label, steps, terminal):
    """Same document, same answer — CODE, POINTER **and MESSAGE**.

    Each of the three has been the broken one. Round 3 found the paths serving
    `/terminal` and `/steps/0` for one body, so a caller who fixed what the
    compiler pointed at would still be refused by the parser. Round 5 then found
    that comparing only code and pointer was itself the gap: for a call in a step
    slot the two agreed on both and still handed the caller DIFFERENT WORDS,
    because the union rejection is translated in one place and the verdict is
    rendered in another. Comparing the whole diagnostic is what makes that
    unwritable.
    """
    assert _placement_via_parser(steps, terminal) == _placement_via_compiler(steps, terminal)


def test_the_legal_terminal_form_is_actually_accepted_by_both():
    """Non-vacuity for the row above: if the verdict refused every body, every
    parity assertion would still hold. Both paths must ACCEPT the bare terminal
    call — the one form #175 ships."""
    assert _placement_via_parser([], _PLACEMENT_CALL) is None
    assert _placement_via_compiler([], _PLACEMENT_CALL) is None


def test_every_body_context_has_a_placement_label():
    """Coverage derived from the authority's full case set, not restated.

    The compiler indexes the label table by body context, so a context added
    without a label is a KeyError raised while DIAGNOSING — a crash on the error
    path, which is the worst place to find one.
    """
    from boomi_mcp.compiler.process_ir.body_capabilities import PUBLIC_BODY_CONTEXTS

    assert set(ir_module.PROCESS_CALL_PLACEMENT_CONTEXT_LABELS) == set(PUBLIC_BODY_CONTEXTS)


def test_the_placement_verdict_is_the_only_copy_of_the_decision():
    """The structural claim itself: the compiler RENDERS the verdict, it does not
    re-derive it. A second copy is how the four divergences arose, so its return
    is what the compiler must branch on."""
    import inspect
    from boomi_mcp.compiler.process_ir import body_capabilities

    source = inspect.getsource(body_capabilities._walk_body)
    assert "process_call_placement_verdict(" in source
    # The reasons are compared by NAME, never by a re-spelled literal.
    assert '"prefix"' not in source and '"step_form"' not in source


def _decision_placement_doc(false_steps, false_terminal):
    """A Decision FALSE arm admits NO process_call in any slot, so it exercises
    the branch a Branch leg cannot: what the two entry points say when the kind
    was never admitted here at all."""
    return {"version": "1", "body": {"kind": "sequence", "steps": [{
        "kind": "decision", "comparison": "equals",
        "left": {"value_type": "static", "static_value": "a"},
        "right": {"value_type": "static", "static_value": "b"},
        "true_arm": {"steps": [copy.deepcopy(_PLACEMENT_DPP)],
                     "terminal": {"kind": "stop"}},
        "false_arm": {"steps": copy.deepcopy(false_steps),
                      "terminal": copy.deepcopy(false_terminal)},
    }]}}


def _false_arm_via_compiler(false_steps, false_terminal):
    from boomi_mcp.compiler.process_ir.body_capabilities import validate_body_capabilities

    ir = parse_process_ir_v1(_decision_placement_doc([_PLACEMENT_DPP], {"kind": "stop"}))
    arm = ir.body.steps[0].false_arm
    arm.steps = [_placement_node(s) for s in false_steps]
    arm.terminal = _placement_node(false_terminal)
    try:
        validate_body_capabilities(ir)
    except ProcessIRCompileError as exc:
        first = exc.diagnostics[0]
        return (first.code, first.path, first.message)
    return None


@pytest.mark.parametrize("steps,terminal,label", [
    ([_PLACEMENT_CONN], _PLACEMENT_CALL, "connector step + call terminal"),
    ([_PLACEMENT_DPP], _PLACEMENT_CALL, "prefix + call terminal"),
    ([], _PLACEMENT_CALL, "bare call terminal"),
], ids=["connector_then_call", "prefix_then_call", "bare_call"])
def test_a_slot_that_admits_no_call_reports_the_slot_check_on_both_paths(steps, terminal, label):
    """Where the kind was never admitted, the SLOT check is the true diagnosis.

    Round 5 measured the compiler answering a body-local placement question the
    caller never got to ask: for a FALSE arm holding a legal connector step and a
    call terminal, it pointed at the LEGAL connector while the parser pointed at
    the illegal terminal. The verdict is body-local and cannot know the slot was
    closed, so the guard belongs at the call site — and this pins it for every
    placement, not just the one that was reported.
    """
    doc = _decision_placement_doc(steps, terminal)
    assert _placement_via_parser(None, None, doc=doc) == \
        _false_arm_via_compiler(steps, terminal), label


# ---------------------------------------------------------------------------
# #175 round 5 — the ROOT sequence gets the same treatment as a body.
#
# Round 3 gave the BODY rule one authority and left the root rule as two
# hand-written copies. Live QA's own sibling sweep found them disagreeing on all
# five illegal root shapes — on the pointer for three, and on the CODE itself
# when a connector is present, where one path served a capability refusal at
# `/body` and the other a return-path refusal at a step index. A machine consumer
# branching on the code took a different branch depending on the entry point.
# ---------------------------------------------------------------------------

_ROOT_CASES = [
    ("call_then_stop", [_PLACEMENT_CALL, _PLACEMENT_STOP]),
    ("step_then_call", [_PLACEMENT_DPP, _PLACEMENT_CALL]),
    ("call_then_step", [_PLACEMENT_CALL, _PLACEMENT_DPP]),
    ("connector_then_call", [_PLACEMENT_CONN, _PLACEMENT_CALL]),
    ("call_then_connector", [_PLACEMENT_CALL, _PLACEMENT_CONN]),
    ("two_calls", [_PLACEMENT_CALL, _PLACEMENT_CALL]),
    ("singleton_is_legal", [_PLACEMENT_CALL]),
]


def _root_doc(steps):
    return {"version": "1", "body": {"kind": "sequence", "steps": copy.deepcopy(steps)}}


def _root_via_parser(steps):
    try:
        parse_process_ir_v1(_root_doc(steps))
    except ProcessIRValidationError as exc:
        first = exc.diagnostics[0]
        return (first.code, first.path, first.message)
    return None


def _root_via_compiler(steps):
    from boomi_mcp.compiler.process_ir.body_capabilities import validate_body_capabilities

    ir = parse_process_ir_v1(_root_doc([_PLACEMENT_CALL]))
    ir.body.steps = [_placement_node(s) for s in steps]
    try:
        validate_body_capabilities(ir)
    except ProcessIRCompileError as exc:
        first = exc.diagnostics[0]
        return (first.code, first.path, first.message)
    return None


@pytest.mark.parametrize("label,steps", _ROOT_CASES, ids=[c[0] for c in _ROOT_CASES])
def test_both_entry_points_agree_on_root_process_call_placement(label, steps):
    """One document, one code, one pointer, one message — at the ROOT too.

    `two_calls` is here because it is the shape that once raised StopIteration
    out of the validator: the offending index is written against the first call's
    INDEX precisely so an all-call chain still has an answer.
    """
    assert _root_via_parser(steps) == _root_via_compiler(steps)


def test_the_legal_root_singleton_is_accepted_by_both():
    """Non-vacuity: a verdict that refused every root would satisfy every parity
    assertion above. The exact singleton is the one root form #175 ships."""
    assert _root_via_parser([_PLACEMENT_CALL]) is None
    assert _root_via_compiler([_PLACEMENT_CALL]) is None


# ---------------------------------------------------------------------------
# #175 round 6 — the two remaining ways a placement answer could still diverge.
# ---------------------------------------------------------------------------

def test_an_ancestor_connector_still_yields_to_the_slot_check():
    """Cross-nesting mixing is subject to the same precedence as everything else.

    A root connector above a Decision whose FALSE-arm terminal is mutated to a
    call: both paths already agreed on code and pointer, and served DIFFERENT
    MESSAGES — ancestor mixing from the compiler, generic slot admission from the
    parser. Comparing whole diagnostics is what surfaced it, which is why the
    parity assertions compare messages too.
    """
    from boomi_mcp.compiler.process_ir.body_capabilities import validate_body_capabilities

    def decision(false_terminal):
        return {"kind": "decision", "comparison": "equals",
                "left": {"value_type": "static", "static_value": "a"},
                "right": {"value_type": "static", "static_value": "b"},
                "true_arm": {"steps": [copy.deepcopy(_PLACEMENT_DPP)],
                             "terminal": {"kind": "stop"}},
                "false_arm": {"steps": [copy.deepcopy(_PLACEMENT_DPP)],
                              "terminal": copy.deepcopy(false_terminal)}}

    authored = {"version": "1", "body": {"kind": "sequence", "steps": [
        copy.deepcopy(_PLACEMENT_CONN), decision(_PLACEMENT_CALL)]}}
    try:
        parse_process_ir_v1(authored)
        parser = None
    except ProcessIRValidationError as exc:
        d = exc.diagnostics[0]
        parser = (d.code, d.path, d.message)

    ir = parse_process_ir_v1({"version": "1", "body": {"kind": "sequence", "steps": [
        copy.deepcopy(_PLACEMENT_CONN), decision({"kind": "stop"})]}})
    ir.body.steps[1].false_arm.terminal = _placement_node(_PLACEMENT_CALL)
    try:
        validate_body_capabilities(ir)
        compiler = None
    except ProcessIRCompileError as exc:
        d = exc.diagnostics[0]
        compiler = (d.code, d.path, d.message)

    assert parser == compiler


def test_the_root_verdict_yields_to_the_control_continuation_rule():
    """`[branch, process_call]` is a control-continuation defect, not a
    process-call one — `[branch, set_dpp]` is the same defect with a different
    kind, and the verdict must not claim the first while ignoring the second.

    Two assertions, because yielding must not open a hole: the placement check
    stays silent, and the FULL pipeline still refuses the mutated model.
    """
    from boomi_mcp.compiler.process_ir.body_capabilities import validate_body_capabilities
    from boomi_mcp.compiler.process_ir.pipeline import compile_process_ir_v1

    branch = {"kind": "branch", "legs": [
        {"steps": [copy.deepcopy(_PLACEMENT_DPP)], "terminal": {"kind": "stop"}},
        {"steps": [copy.deepcopy(_PLACEMENT_DPP)], "terminal": {"kind": "stop"}}]}
    ir = parse_process_ir_v1(
        {"version": "1", "body": {"kind": "sequence", "steps": [copy.deepcopy(branch)]}})
    ir.body.steps = [ir.body.steps[0], _placement_node(_PLACEMENT_CALL)]

    validate_body_capabilities(ir)  # yields — the continuation rule owns this

    with pytest.raises(ProcessIRCompileError) as excinfo:
        compile_process_ir_v1(ir, None)
    # #178 SUPERSESSION. Until #178 this asserted the compiler's own
    # `PROCESS_IR_SEMANTIC_AMBIGUOUS_FLOW` at `/body/steps/0` — the CFG
    # invariant that fired once the document had been lowered. Public compile now
    # re-parses through the parser authority first, so the document is refused by
    # the rule this test's own docstring already named as its owner, one stage
    # earlier and with the authored pointer intact. The old code is recorded in
    # the #178 ledger; it is SUPERSEDED, not broken, and no `src/` consumer keyed
    # on it for this document.
    #
    # Still fail-closed, and still NAMED: "some nonempty code exists" proves
    # nothing about WHICH rule owns the document — a reviewer rejected exactly
    # that assertion. The refusal must come from a kind-agnostic rule, and it must
    # NOT be the process-call placement code, whose whole point here is to have
    # yielded.
    from boomi_mcp.errors import (
        PROCESS_IR_CAPABILITY_PROCESS_CALL_RETURN_PATH_BINDING_UNSUPPORTED,
        PROCESS_IR_SEMANTIC_AMBIGUOUS_FLOW,
        PROCESS_IR_SEMANTIC_CONTROL_CONTINUATION_UNSUPPORTED,
    )
    served = excinfo.value.diagnostics[0]
    # The whole triple, not just the code: #178 exists because the two entry
    # points agreed on the decision and disagreed on the diagnostic, so pinning
    # the code alone would leave the pointer and message free to drift again.
    assert (served.code, served.path, served.message) == (
        PROCESS_IR_SEMANTIC_CONTROL_CONTINUATION_UNSUPPORTED,
        "/body",
        "no step may follow a branch or decision — control nodes are terminal "
        "fan-out in ProcessIR v1 (continuation_after_branch_or_decision is gated)",
    ), [(d.code, d.path) for d in excinfo.value.diagnostics]
    assert served.code != PROCESS_IR_SEMANTIC_AMBIGUOUS_FLOW
    assert (
        served.code
        != PROCESS_IR_CAPABILITY_PROCESS_CALL_RETURN_PATH_BINDING_UNSUPPORTED
    )
    # The parser's finding is served through the compile error family, with the
    # schema phase — a raw ProcessIRValidationError here would defeat every
    # production handler, all of which catch only ProcessIRCompileError.
    assert served.phase == "schema"

    # ...and the SAME document with an ordinary kind is refused the same way,
    # which is the claim "the continuation rule owns this" actually rests on.
    # This peer is what makes the assertion above a statement about the
    # CONTINUATION rule rather than about process calls, so it survives #178
    # unchanged in intent — only the expected code moved.
    other = parse_process_ir_v1(
        {"version": "1", "body": {"kind": "sequence", "steps": [copy.deepcopy(branch)]}})
    other.body.steps = [other.body.steps[0], _placement_node(_PLACEMENT_DPP)]
    with pytest.raises(ProcessIRCompileError) as other_exc:
        compile_process_ir_v1(other, None)
    peer = other_exc.value.diagnostics[0]
    assert (peer.code, peer.path, peer.message) == (
        served.code,
        served.path,
        served.message,
    )


def test_a_synthetic_additional_body_context_is_reported_as_uncovered():
    """#180 witness (i) — the non-vacuity control for the coverage rule above.

    `test_authority_table_covers_every_body_model_in_the_schema` asserts the
    authority table equals the schema's control-body set, and floors the walk
    with `assert bodies`. That floor proves the walk found SOMETHING; it never
    proved the comparison would FAIL on a body context nobody added rows for —
    which is the entire property. A table that happened to be a superset, or a
    walk that silently missed a nesting shape, would pass either way.

    So: build a root carrying one EXTRA control body, assert the walk sees it
    (the mutation took effect), then show it is uncovered by the real authority
    table. The real root is re-checked at the end as the opposite-direction
    control, so a walk that reported everything as uncovered could not pass.
    """
    from pydantic import create_model

    synthetic_body = create_model(
        "SyntheticExtraBodyV1",
        steps=(ir_module.TryCatchTryBodyV1.model_fields["steps"].annotation, ...),
        terminal=(ir_module.TryCatchTryBodyV1.model_fields["terminal"].annotation, ...),
    )
    synthetic_root = create_model(
        "SyntheticRootV1",
        body=(ir_module.ProcessIRV1.model_fields["body"].annotation, ...),
        extra_body=(synthetic_body, ...),
    )

    real = _control_body_models(ir_module.ProcessIRV1)
    mutant = _control_body_models(synthetic_root)

    # THE MUTATION TOOK EFFECT: the walk really does see the new body context.
    assert synthetic_body in mutant, sorted(m.__name__ for m in mutant)
    assert synthetic_body not in real
    assert mutant == real | {synthetic_body}, sorted(
        m.__name__ for m in mutant ^ (real | {synthetic_body}))

    covered = {model for model, _field in bodycaps.BODY_SLOT_AUTHORITIES_V1.values()}
    # ...and the comparison the real test makes would FAIL on it.
    assert mutant - covered == {synthetic_body}
    assert covered != mutant

    # CONTROL: the real schema is still fully covered, so the check above is
    # about the synthetic context and not about a walk that over-reports.
    assert covered == real
