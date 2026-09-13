"""#184 workstream A: the Data Passthrough entry, end to end.

A ``passthrough`` node is the third explicit entry form beside the scheduled
default and the #158 listener: root-only, first step, exactly once, label-only,
fused with the compiler-synthesized Start, and the source of the ``passthrough``
execution profile. This file pins it at every seam the listener entry pinned:

* the root GRAMMAR, every admitted and refused form asserted at BOTH public entry
  points — the parser, and ``compile_process_ir_v1`` on an exported model mutated
  after a legal parse — through ``tests/_process_ir_entrypoint_differential.py``,
  plus ``validate_body_capabilities`` called DIRECTLY, so the compiler-side
  rendering is proven on its own rather than shadowed by the compile entry's
  re-parse;
* three-way ENTRY classification on really lowered CFGs, with a decoy Web
  Services Server Listen operation in the symbol table;
* the plan and CFG INVARIANTS, re-derived for the fused passthrough Start;
* the emitted Start and the materialized ``<process>`` option bytes;
* entry RECOGNITION for deployment, and the served revision oracle.

**Expected-value provenance** (the clean-room rule — nothing below is read off the
implementation under test):

* the Start's ``<configuration>`` and the six ``<process>`` option attributes are
  READ from ``tests/fixtures/live_xml/m11/process_doccacheretrieve_loadalldoc_variant.xml``,
  a UI-built passthrough process committed long before #184 and recorded as
  UI-built and causally independent in
  ``docs/architecture/evidence/issue-175/stage1-qa-round-4.md``;
* the grammar codes and pointers are the #184 workstream-A contract's stated
  identities: a misplaced entry is ``PROCESS_IR_SCHEMA_INVALID_CARDINALITY`` at the
  entry step (the listener's existing position identity), a construct a
  passthrough root does not compose with is ``PROCESS_IR_CAPABILITY_UNSUPPORTED``
  at that step, and a prefix before a root process call keeps the existing root
  process-call placement identity;
* the invariant adversaries reuse the #158 suite's own site messages and derived
  per-field case set, imported rather than copied.
"""

from __future__ import annotations

import copy
import re
import xml.etree.ElementTree as ET
from pathlib import Path

import pytest

from boomi_mcp.authoring import contract as authoring_contract
from boomi_mcp.authoring.contract import get_authoring_revisions
from boomi_mcp.authoring.process_entry import ProcessEntryV1, canonical_root_entry
from boomi_mcp.authoring.process_materialization import (
    build_materialization_plan,
    process_plan_fingerprint,
)
from boomi_mcp.categories.components import canonical_process_apply as cpa
from boomi_mcp.categories.components import process_component_materializer as pcm
from boomi_mcp.compiler.process_ir import body_capabilities as bc
from boomi_mcp.compiler.process_ir import execution_profile as ep
from boomi_mcp.compiler.process_ir.contracts import (
    ComponentSymbolV1,
    ListenerSemanticV1,
    PassthroughSemanticV1,
    StartListenInputV1,
    StartNoActionInputV1,
    StartPassthroughInputV1,
    SymbolTableV1,
)
from boomi_mcp.compiler.process_ir.diagnostics import ProcessIRCompileError
from boomi_mcp.compiler.process_ir.emitter_registry import (
    emit_process,
    emitter_revision,
    registration_for,
)
from boomi_mcp.compiler.process_ir.entry_policy import (
    ENTRY_FORMS,
    FUSED_ENTRY_SEMANTIC_KINDS,
    classify_entry,
    derive_process_entry,
)
from boomi_mcp.compiler.process_ir.invariants import (
    check_cfg_invariants,
    check_emission_plan_invariants,
)
from boomi_mcp.compiler.process_ir.lowering import (
    _emitter_input_for,
    lower_process_ir_to_cfg,
)
from boomi_mcp.compiler.process_ir.pipeline import compile_process_ir_v1
from boomi_mcp.errors import (
    PROCESS_IR_CAPABILITY_NODE_NOT_ALLOWED_IN_BODY,
    PROCESS_IR_CAPABILITY_PROCESS_CALL_RETURN_PATH_BINDING_UNSUPPORTED,
    PROCESS_IR_CAPABILITY_UNSUPPORTED,
    PROCESS_IR_COMPILE_EMISSION_PLAN_INVALID,
    PROCESS_IR_COMPILE_INTERNAL,
    PROCESS_IR_COMPILE_NONDETERMINISTIC,
    PROCESS_IR_SCHEMA_INVALID_CARDINALITY,
    PROCESS_IR_SEMANTIC_CONTROL_CONTINUATION_UNSUPPORTED,
)
from boomi_mcp.models.process_component import ProcessComponentEnvelopeV1
from boomi_mcp.models.process_ir import (
    EXPLICIT_ENTRY_KINDS,
    LINEAR_BODY_KINDS,
    PROCESS_IR_V1_CAPABILITIES,
    ProcessIRValidationError,
    parse_process_ir_v1,
    process_call_root_verdict,
    process_ir_v1_json_schema,
)
from boomi_mcp.recipes.materialization import build_symbol_table

from _process_ir_entrypoint_differential import diagnostic_vector, measure_entrypoints
from test_issue_158_entry_invariants import (
    _SITE_ENTRY_ID,
    _SITE_FORM,
    _SITE_INPUT,
    _SITE_ONE_START,
    _SITE_ONE_WIRE,
    _SITE_START_GEOMETRY,
    _SITE_SUCCESSOR,
    _listener_doc,
    _plan_field_cases,
    _refusal,
    _scheduled_doc,
    _site,
    _symbols as _entry_invariant_symbols,
    _with_start,
    _with_start_wires,
)
from test_process_ir_entrypoint_diagnostic_parity import NODE, _atom

# The #158 deployment suite's two autouse fixtures, imported so they apply to this
# module too: the build registry is restored after each test, and the metadata
# pager is stubbed — unstubbed over a MagicMock client it never terminates.
from test_issue_158_listener_deployment import (  # noqa: F401
    _no_live_metadata_queries,
    _registry_restored,
)

_ROOT = Path(__file__).resolve().parent.parent
_PLAN_INVALID = PROCESS_IR_COMPILE_EMISSION_PLAN_INVALID

# ---------------------------------------------------------------------------
# The UI-built capture: the byte authority, READ, never retyped
# ---------------------------------------------------------------------------

_CAPTURE = (
    _ROOT / "tests" / "fixtures" / "live_xml" / "m11"
    / "process_doccacheretrieve_loadalldoc_variant.xml"
).read_text(encoding="utf-8")
_CAPTURED_START = re.search(
    r'<shape image="start" name="shape1" shapetype="start" userlabel="([^"]*)"[^>]*>'
    r"(<configuration>.*?</configuration>)",
    _CAPTURE,
)
_CAPTURED_OPTIONS = re.search(r'<process xmlns="" ([^>]*)>', _CAPTURE)
assert _CAPTURED_START and _CAPTURED_OPTIONS, "the UI capture no longer has the expected shape"
#: The capture's own Start label and configuration element.
CAPTURED_LABEL = _CAPTURED_START.group(1)
CAPTURED_START_CONFIGURATION = _CAPTURED_START.group(2)
#: The capture's `<process>` open-tag attribute string.
CAPTURED_PROCESS_OPTIONS = _CAPTURED_OPTIONS.group(1)


def test_the_capture_is_a_passthrough_process_with_six_options():
    """NON-VACUITY of the authority itself: it IS a passthrough Start, and its
    open tag carries six attributes with no stopProcessingIfZeroDocuments."""
    assert "passthroughaction" in CAPTURED_START_CONFIGURATION
    assert len(re.findall(r'\w+="[^"]*"', CAPTURED_PROCESS_OPTIONS)) == 6
    assert "stopProcessingIfZeroDocuments" not in CAPTURED_PROCESS_OPTIONS


# ---------------------------------------------------------------------------
# Authored nodes
# ---------------------------------------------------------------------------

P = {"kind": "passthrough"}
S = _atom("stop")
RD = _atom("return_documents")
MSG = _atom("message")
MAP = _atom("map_ref")
CALL = _atom("connector_call")
PC = _atom("process_call")
BR = _atom("branch")
DEC = _atom("decision")
TRY = _atom("try_catch")
EXC = _atom("exception")
NOTIFY = _atom("notify")
CONT = _atom("continue")
SRC = _atom("source")
TGT = _atom("target")
LSN = _atom("listener")
SET_DDP = _atom("set_ddp")
CACHE_PUT = _atom("cache_put")
CACHE_GET = _atom("cache_get")


def _doc(steps):
    return {"version": "1", "body": {"kind": "sequence", "steps": copy.deepcopy(steps)}}


def _every_linear_kind():
    """Every linear kind, in a run the cache rules accept: a cache_put is
    immediately followed by a stream-replacing read. DERIVED from the model's
    linear union, so a linear kind added later is exercised here too."""
    run = []
    for kind in LINEAR_BODY_KINDS:
        run.append(_atom(kind))
        if kind == "cache_put":
            run.append(_atom("cache_get"))
    return run


def _carrier(steps):
    """A model parsed LEGAL and then mutated to hold ``steps`` — each node validated
    on its own, never as part of a root, so no root validator runs on it."""
    carrier = parse_process_ir_v1(_doc([P, MSG, S]))
    carrier.body.steps = [NODE.validate_python(copy.deepcopy(step)) for step in steps]
    return carrier


def _parser_diagnostic(steps):
    with pytest.raises(ProcessIRValidationError) as excinfo:
        parse_process_ir_v1(_doc(steps))
    return excinfo.value.diagnostics[0]


def _body_capabilities_outcome(model):
    try:
        bc.validate_body_capabilities(model)
    except ProcessIRCompileError as exc:
        return ("REFUSED",) + diagnostic_vector(exc)
    return ("ACCEPTED",)


# ---------------------------------------------------------------------------
# 1. The grammar matrix, at both entry points
# ---------------------------------------------------------------------------

_ADMITTED = [
    ("process-call", [P, PC]),
    ("branch", [P, BR]),
    ("decision", [P, DEC]),
    ("stop", [P, S]),
    ("return-documents", [P, RD]),
    ("consecutive-maps-no-producer", [P, MAP, MAP, S]),
    ("call-then-trailing-map", [P, CALL, MAP, S]),
    ("map-then-call-return", [P, MAP, CALL, RD]),
    ("every-linear-kind", [P] + _every_linear_kind() + [S]),
    ("linear-and-call-then-branch", [P, SET_DDP, CALL, BR]),
    ("map-then-decision", [P, MAP, DEC]),
    ("labelled", [dict(P, label="Receive <prepared> & \"docs\""), S]),
]


@pytest.mark.parametrize(
    "steps", [row[1] for row in _ADMITTED], ids=[row[0] for row in _ADMITTED]
)
def test_admitted_passthrough_forms_are_accepted_at_both_entry_points(steps):
    parsed = parse_process_ir_v1(_doc(steps))
    assert parsed.body.steps[0].kind == "passthrough"
    carrier = _carrier(steps)
    assert measure_entrypoints(carrier) == (("ACCEPTED",), ("ACCEPTED",))
    assert _body_capabilities_outcome(carrier) == ("ACCEPTED",)


_CARD = PROCESS_IR_SCHEMA_INVALID_CARDINALITY
_CAP = PROCESS_IR_CAPABILITY_UNSUPPORTED
_CONTINUATION = PROCESS_IR_SEMANTIC_CONTROL_CONTINUATION_UNSUPPORTED
_ROOT_CALL = PROCESS_IR_CAPABILITY_PROCESS_CALL_RETURN_PATH_BINDING_UNSUPPORTED

_REFUSED = [
    # (id, steps, code, pointer)
    # -- entry position: the shared explicit-entry verdict -------------------
    ("alone", [P], _CARD, "/body"),
    ("after-a-step", [MSG, P, S], _CARD, "/body/steps/1"),
    ("twice", [P, P, S], _CARD, "/body/steps/1"),
    ("listener-after-passthrough", [P, LSN, S], _CARD, "/body/steps/1"),
    ("passthrough-after-listener", [LSN, P, TGT, S], _CARD, "/body/steps/1"),
    # -- composition ---------------------------------------------------------
    ("source", [P, SRC, S], _CAP, "/body/steps/1"),
    ("target", [P, MSG, TGT, S], _CAP, "/body/steps/2"),
    ("try-catch", [P, TRY], _CAP, "/body/steps/1"),
    ("exception-terminal", [P, MSG, EXC], _CAP, "/body/steps/2"),
    ("notify", [P, NOTIFY, S], PROCESS_IR_CAPABILITY_NODE_NOT_ALLOWED_IN_BODY,
     "/body/steps/1"),
    ("continue", [P, CONT], _CONTINUATION, "/body"),
    # -- control is terminal fan-out -----------------------------------------
    ("step-after-branch", [P, BR, S], _CONTINUATION, "/body"),
    ("step-after-decision", [P, DEC, MSG, S], _CONTINUATION, "/body"),
    ("call-after-branch", [P, BR, PC], _CONTINUATION, "/body"),
    # -- the root process-call verdict ---------------------------------------
    ("prefix-before-call", [P, MSG, PC], _ROOT_CALL, "/body/steps/1"),
    ("two-step-prefix-before-call", [P, SET_DDP, MSG, PC], _ROOT_CALL, "/body/steps/1"),
    ("suffix-after-call", [P, PC, S], _ROOT_CALL, "/body/steps/2"),
    ("two-calls", [P, PC, PC], _ROOT_CALL, "/body/steps/2"),
    ("connector-beside-call", [P, CALL, PC], _CAP, "/body"),
    # -- terminal and step vocabulary ----------------------------------------
    ("no-terminal", [P, MSG], _CARD, "/body"),
    ("ends-on-a-call", [P, CALL], _CARD, "/body"),
    ("stop-mid-run", [P, S, MSG, S], _CAP, "/body/steps/1"),
    ("return-documents-mid-run", [P, RD, S], _CAP, "/body/steps/1"),
    # -- the cache rules, unchanged ------------------------------------------
    ("cache-put-not-followed-by-read", [P, CACHE_PUT, MSG, S], _CARD, "/body"),
    ("trailing-cache-put", [P, MSG, CACHE_PUT, S], _CARD, "/body"),
    ("trailing-cache-put-before-branch", [P, CACHE_PUT, BR], _CARD, "/body"),
]


@pytest.mark.parametrize(
    "steps,code,pointer",
    [row[1:] for row in _REFUSED],
    ids=[row[0] for row in _REFUSED],
)
def test_refused_passthrough_forms_serve_one_identity_at_both_entry_points(
    steps, code, pointer
):
    """The parser serves the stated code and pointer; the compile entry serves the
    SAME full diagnostic vector for the mutated model; and the compiler-side
    rendering, called directly past the compile entry's re-parse, serves the same
    code, pointer and message.

    The direct rendering's REMEDIATION is the compiler layer's own served text for
    the code, as it is for every shared model rule ``body_capabilities`` translates
    (the serialized-chain grammar included); the public compile entry serves the
    parser's, because it re-parses first — which the full-vector equality above pins.
    """
    parsed = _parser_diagnostic(steps)
    assert (parsed.code, parsed.path) == (code, pointer), parsed.message

    carrier = _carrier(steps)
    parser_outcome, compiler_outcome = measure_entrypoints(carrier)
    assert parser_outcome == compiler_outcome, (parser_outcome, compiler_outcome)
    assert parser_outcome[:2] == ("REFUSED", (code, pointer, parsed.message, parsed.remediation))

    direct = _body_capabilities_outcome(carrier)
    assert direct[0] == "REFUSED", direct
    assert direct[1][:3] == (code, pointer, parsed.message), direct


def test_the_compiler_side_passthrough_check_is_load_bearing(monkeypatch):
    """MUTATION CONTROL for ``_check_passthrough_placement``. With it disabled the
    compiler's own body pass lets the composition and grammar refusals through —
    so the direct refusals above are this check's, not a later rule's."""
    load_bearing = [row for row in _REFUSED if row[0] in (
        "source", "target", "no-terminal", "stop-mid-run", "trailing-cache-put",
        "step-after-branch",
    )]
    monkeypatch.setattr(bc, "_check_passthrough_placement", lambda ir: None)
    for row_id, steps, _code, _pointer in load_bearing:
        assert _body_capabilities_outcome(_carrier(steps)) == ("ACCEPTED",), row_id


def test_the_process_call_singleton_skips_the_entry_and_names_authored_steps():
    """The root process-call verdict treats a leading passthrough as NOT a step on
    the call's path: the singleton is admitted, and a refusal's pointer and its
    message name the same AUTHORED step. Without the entry offset the singleton
    would be refused at the entry itself."""
    assert process_call_root_verdict(["passthrough", "process_call"]) is None
    reason, at, message = process_call_root_verdict(
        ["passthrough", "message", "process_call"]
    )
    assert at == ("steps", 1)
    assert "(step 1)" in message
    # Every other root is judged exactly as before.
    assert process_call_root_verdict(["message", "process_call"])[1] == ("steps", 0)
    assert process_call_root_verdict(["process_call"]) is None


@pytest.mark.parametrize("slot", ["steps", "terminal"])
def test_a_nested_passthrough_is_a_body_slot_refusal_at_both_entry_points(slot):
    """Root-only: no control-body union admits it. The raw payload is refused by
    the parser, and the same mistake on a mutated model by the compiler."""
    leg = {"steps": [MSG], "terminal": S}
    if slot == "steps":
        leg = {"steps": [P], "terminal": S}
        pointer = "/body/steps/0/legs/0/steps/0"
    else:
        leg = {"steps": [MSG], "terminal": P}
        pointer = "/body/steps/0/legs/0/terminal"
    payload_branch = {"kind": "branch", "legs": [leg, {"steps": [MSG], "terminal": S}]}
    diagnostic = _parser_diagnostic([payload_branch])
    assert (diagnostic.code, diagnostic.path) == (
        PROCESS_IR_CAPABILITY_NODE_NOT_ALLOWED_IN_BODY, pointer,
    )

    model = parse_process_ir_v1(_doc([BR]))
    entry = NODE.validate_python(copy.deepcopy(P))
    if slot == "steps":
        model.body.steps[0].legs[0].steps = [entry]
    else:
        model.body.steps[0].legs[0].terminal = entry
    direct = _body_capabilities_outcome(model)
    assert direct[0] == "REFUSED" and direct[1][:2] == (
        PROCESS_IR_CAPABILITY_NODE_NOT_ALLOWED_IN_BODY, pointer,
    ), direct


def test_the_explicit_entry_kinds_and_the_policy_agree_in_both_directions():
    """The model's explicit-entry vocabulary and the entry policy's fused kinds
    are two layers' views of one fact, pinned against each other."""
    assert EXPLICIT_ENTRY_KINDS == FUSED_ENTRY_SEMANTIC_KINDS == {"listener", "passthrough"}
    assert {row.absorbed_semantic_kind for row in ENTRY_FORMS.values()} - {None} == (
        FUSED_ENTRY_SEMANTIC_KINDS
    )


def test_the_served_schema_describes_the_passthrough_in_caller_vocabulary():
    node = process_ir_v1_json_schema()["$defs"]["PassthroughEntryNodeV1"]
    text = node["description"].lower()
    for word in ("start_passthrough", "passthroughaction", "emitter", "cfg", "shape1",
                 "synthetic", "dragpoint"):
        assert word not in text, word
    for fact in ("one group", "wait", "empty document", "first root step"):
        assert fact in text, fact
    assert set(node["properties"]) == {"kind", "label"}
    assert node["required"] == ["kind"]


def test_the_passthrough_entry_is_published_supported_and_served():
    from boomi_mcp.authoring.process_ir_projection import build_process_ir_authoring_entries

    assert PROCESS_IR_V1_CAPABILITIES["passthrough_entry"] == "supported"
    entries = {e.contract_entry_id: e for e in build_process_ir_authoring_entries()}
    node = entries["node.passthrough"]
    assert node.category == "entry"
    assert "capability.passthrough_entry" in node.related_entry_ids
    for related in node.related_entry_ids:
        assert related in entries, related
    facts = " ".join(node.ordering_facts).lower()
    for fact in ("first step", "one group", "wait=true", "single empty document",
                 "exactly one process_call"):
        assert fact in facts, fact
    authority = entries["semantic_rule.listener.entry_authority"].summary.lower()
    for form in ENTRY_FORMS:
        assert form in authority, form


# ---------------------------------------------------------------------------
# 2. Three-way entry classification on real lowered CFGs
# ---------------------------------------------------------------------------


def _decoyed_symbols():
    """A symbol table carrying a genuine WSS Listen operation no passthrough or
    scheduled root references — the decoy that must not classify anything."""
    return SymbolTableV1(symbols=_entry_invariant_symbols().symbols + (
        ComponentSymbolV1(ref="$ref:rc", component_id="REST-CONN",
                          component_type="connector-settings", connector_type="rest"),
        ComponentSymbolV1(ref="$ref:get_op", component_id="GET-OP",
                          component_type="connector-action", connector_type="rest",
                          action_type="GET", connection_ref="$ref:rc"),
    ))


_GET = {"kind": "connector_call", "operation_ref": "$ref:get_op"}


def _passthrough_call_doc(label="In"):
    return _doc([dict(P, label=label), MSG, _GET, S])


@pytest.mark.parametrize(
    "doc,expected",
    [
        pytest.param(_doc([_GET, S]), "scheduled", id="scheduled"),
        pytest.param(_listener_doc(), "listener", id="listener"),
        pytest.param(_passthrough_call_doc(), "passthrough", id="passthrough"),
    ],
)
def test_classify_entry_is_three_way_on_compiled_graphs(doc, expected):
    symbols = _decoyed_symbols()
    assert any(s.action_type == "Listen" for s in symbols.symbols)  # the decoy is real
    cfg, plan = compile_process_ir_v1(parse_process_ir_v1(doc), symbols)
    assert classify_entry(cfg) == expected
    assert classify_entry(lower_process_ir_to_cfg(parse_process_ir_v1(doc))) == expected
    assert ep.derive_process_execution_profile(cfg, symbols) == expected
    assert plan.nodes[0].emitter_input.emitter_kind == ENTRY_FORMS[expected].start_emitter_kind
    assert canonical_root_entry(doc).form == expected


# ---------------------------------------------------------------------------
# 3. Invariants: the fused passthrough Start, re-derived
# ---------------------------------------------------------------------------


def _compiled_passthrough(label="In"):
    symbols = _decoyed_symbols()
    cfg, plan = compile_process_ir_v1(parse_process_ir_v1(_passthrough_call_doc(label)), symbols)
    return cfg, plan, symbols


def test_the_passthrough_is_fused_once_at_shape1():
    cfg, plan, symbols = _compiled_passthrough()
    check_emission_plan_invariants(plan, cfg, symbols)  # the baseline is legal
    start = plan.nodes[0]
    assert (start.shape_id, start.origin, start.synthetic_role) == ("shape1", "synthetic", "start")
    assert start.emitter_input == StartPassthroughInputV1(userlabel="In")
    assert cfg.nodes[0].semantic == PassthroughSemanticV1(label="In")
    ir_nodes = [n for n in plan.nodes if n.origin == "ir"]
    assert [n.cfg_node_id for n in ir_nodes] == [n.node_id for n in cfg.nodes[1:]]
    assert [t.to_shape_id for t in start.outgoing] == ["shape2"]
    # Every call under a passthrough is downstream: none is the connector entry.
    call = next(n for n in cfg.nodes if n.semantic.semantic_kind == "connector_call")
    assert call.semantic.role == "downstream"
    assert next(
        n for n in plan.nodes if n.cfg_node_id == call.node_id
    ).emitter_input.emitter_kind == "connectoraction_target"


def test_the_passthrough_registration_is_symbol_free_and_shaped_like_a_start():
    reg = registration_for("start_passthrough")
    assert reg.input_type is StartPassthroughInputV1
    assert reg.produced_shape_type == "start"
    assert reg.outgoing == registration_for("start_noaction").outgoing
    assert tuple(reg.requirements(StartPassthroughInputV1(userlabel="x"))) == ()
    assert set(StartPassthroughInputV1.model_fields) == {"emitter_kind", "userlabel"}


def test_entry_rejects_every_wrong_start_form():
    cfg, plan, symbols = _compiled_passthrough()
    for wrong in (StartNoActionInputV1(), StartListenInputV1(operation_id="WSSOP-1", userlabel="In")):
        mutated = _with_start(plan, emitter_input=wrong)
        assert mutated != plan
        _site(_refusal(mutated, cfg, symbols), _SITE_FORM)
    # ...and the passthrough form on the other two roots.
    for doc in (_listener_doc(), _scheduled_doc()):
        o_cfg, o_plan = compile_process_ir_v1(parse_process_ir_v1(doc), _decoyed_symbols())
        check_emission_plan_invariants(o_plan, o_cfg, _decoyed_symbols())
        _site(_refusal(
            _with_start(o_plan, emitter_input=StartPassthroughInputV1(userlabel="")),
            o_cfg, _decoyed_symbols(),
        ), _SITE_FORM)


def test_entry_rejects_a_forged_label():
    cfg, plan, symbols = _compiled_passthrough(label="In")
    forged = _with_start(plan, emitter_input=StartPassthroughInputV1(userlabel="Other"))
    _site(_refusal(forged, cfg, symbols), _SITE_INPUT)


@pytest.mark.parametrize("target", ["shape3", "shape1"])
def test_entry_rejects_a_wrong_successor(target):
    cfg, plan, symbols = _compiled_passthrough()
    wire = plan.nodes[0].outgoing[0].model_copy(update={"to_shape_id": target})
    _site(_refusal(_with_start_wires(plan, [wire]), cfg, symbols), _SITE_SUCCESSOR)


@pytest.mark.parametrize("axis", ["x", "y"])
def test_entry_rejects_authored_geometry(axis):
    cfg, plan, symbols = _compiled_passthrough()
    layout = plan.nodes[0].layout.model_copy(
        update={axis: getattr(plan.nodes[0].layout, axis) + 8.0}
    )
    _site(_refusal(_with_start(plan, layout=layout), cfg, symbols), _SITE_START_GEOMETRY)


def test_entry_rejects_a_retargeted_entry_id_a_second_start_and_a_claimed_wire():
    cfg, plan, symbols = _compiled_passthrough()
    _site(_refusal(plan.model_copy(update={"entry_shape_id": "shape2"}), cfg, symbols),
          _SITE_ENTRY_ID)
    last = plan.nodes[-1].ordinal + 1
    second = plan.nodes[0].model_copy(update={
        "ordinal": last, "shape_id": "shape%d" % last, "outgoing": (),
    })
    _site(_refusal(plan.model_copy(update={"nodes": tuple(plan.nodes) + (second,)}),
                   cfg, symbols), _SITE_ONE_START)
    claimed = plan.nodes[0].outgoing[0].model_copy(
        update={"provenance": "cfg_edge", "cfg_edge_id": "e1"}
    )
    _site(_refusal(_with_start_wires(plan, [claimed]), cfg, symbols), _SITE_ONE_WIRE)


def test_entry_rejects_claimed_ownership():
    """Synthetic ownership: the Start may claim neither IR origin nor provenance."""
    cfg, plan, symbols = _compiled_passthrough()
    _site(_refusal(
        _with_start(plan, origin="ir", synthetic_role=None,
                    cfg_node_id=cfg.entry_node_id, source_path="/body/steps/0"),
        cfg, symbols,
    ), _SITE_FORM)
    assert _refusal(
        _with_start(plan, source_path="/body/steps/0"), cfg, symbols
    ).code == _PLAN_INVALID


def test_the_absorbed_passthrough_cannot_reappear_as_a_plan_node():
    """Duplicate absorption: an IR node for the absorbed entry is refused, and
    lowering refuses to derive an emitter input for it at all."""
    cfg, plan, symbols = _compiled_passthrough()
    duplicated = plan.nodes[1].model_copy(update={
        "cfg_node_id": cfg.entry_node_id, "source_path": "/body/steps/0",
    })
    forged = plan.model_copy(update={"nodes": (plan.nodes[0], duplicated) + tuple(plan.nodes[2:])})
    assert _refusal(forged, cfg, symbols).code in (_PLAN_INVALID, PROCESS_IR_COMPILE_INTERNAL)
    with pytest.raises(ProcessIRCompileError) as excinfo:
        _emitter_input_for(cfg.nodes[0], symbols.build_index())
    assert excinfo.value.diagnostics[0].code == _PLAN_INVALID
    assert "passthrough" in excinfo.value.diagnostics[0].message


def test_cfg_rejects_a_passthrough_off_entry_relocated_twice_or_beside_a_listener():
    cfg, _plan, _symbols = _compiled_passthrough()
    check_cfg_invariants(cfg)  # the baseline is legal
    entry = cfg.nodes[0]

    def _internal(mutated):
        with pytest.raises(ProcessIRCompileError) as excinfo:
            check_cfg_invariants(mutated)
        return excinfo.value.diagnostics[0]

    assert _internal(cfg.model_copy(update={"entry_node_id": cfg.nodes[1].node_id})).code
    # A root step path no other node holds, so the shared-source-path check cannot
    # answer first and the explicit-entry position rule is the one measured.
    relocated = cfg.model_copy(update={"nodes": (
        entry.model_copy(update={"source_path": "/body/steps/9"}),
    ) + tuple(cfg.nodes[1:])})
    diagnostic = _internal(relocated)
    assert diagnostic.code == PROCESS_IR_COMPILE_INTERNAL
    assert "passthrough" in diagnostic.message
    twice = cfg.model_copy(update={"nodes": (entry, cfg.nodes[1].model_copy(update={
        "semantic": entry.semantic}),) + tuple(cfg.nodes[2:])})
    assert _internal(twice).code == PROCESS_IR_COMPILE_INTERNAL
    beside = cfg.model_copy(update={"nodes": (entry, cfg.nodes[1].model_copy(update={
        "semantic": ListenerSemanticV1(operation_ref="$ref:wss_op")}),) + tuple(cfg.nodes[2:])})
    diagnostic = _internal(beside)
    assert diagnostic.code == PROCESS_IR_COMPILE_INTERNAL
    assert "listener" in diagnostic.message
    with pytest.raises(ProcessIRCompileError):
        derive_process_entry(beside)
    fanout = cfg.edges[0].model_copy(update={
        "edge_id": "e9", "ordinal": 9, "local_ordinal": 2,
        "target_node_id": cfg.nodes[2].node_id,
    })
    with pytest.raises(ProcessIRCompileError) as excinfo:
        derive_process_entry(cfg.model_copy(update={"edges": tuple(cfg.edges) + (fanout,)}))
    assert excinfo.value.diagnostics[0].code == PROCESS_IR_COMPILE_INTERNAL


def test_no_connector_call_may_carry_the_entry_role_under_a_passthrough():
    """Under a passthrough every call is downstream. CONTROL: on a scheduled root
    the same call legitimately carries the entry role, so the refusal below is the
    passthrough rule firing, not a blanket ban on the role."""
    s_cfg, _s_plan = compile_process_ir_v1(parse_process_ir_v1(_doc([_GET, S])), _decoyed_symbols())
    assert s_cfg.nodes[0].semantic.role == "entry"
    check_cfg_invariants(s_cfg)

    cfg, _plan, _symbols = _compiled_passthrough()
    index, call = next(
        (i, n) for i, n in enumerate(cfg.nodes) if n.semantic.semantic_kind == "connector_call"
    )
    forged_call = call.model_copy(update={
        "semantic": call.semantic.model_copy(update={"role": "entry"})})
    forged = cfg.model_copy(update={
        "nodes": cfg.nodes[:index] + (forged_call,) + cfg.nodes[index + 1:]})
    with pytest.raises(ProcessIRCompileError) as excinfo:
        check_cfg_invariants(forged)
    diagnostic = excinfo.value.diagnostics[0]
    assert diagnostic.code == PROCESS_IR_COMPILE_INTERNAL
    assert "enters on a passthrough" in diagnostic.message


_PT_CFG, _PT_PLAN, _PT_SYMBOLS, _PT_FIELD_CASES = _plan_field_cases(
    _doc([dict(P, label="In"), MSG, S])
)


@pytest.mark.parametrize(
    "label,index,field,value",
    _PT_FIELD_CASES,
    ids=[case[0] for case in _PT_FIELD_CASES],
)
def test_every_plan_field_of_a_passthrough_root_refuses_a_perturbation(label, index, field, value):
    """The #158 per-FIELD adversarial set, derived from the plan models, over a
    fused passthrough Start and the body shape beside it."""
    assert _PT_PLAN.nodes[0].emitter_input.emitter_kind == "start_passthrough"
    node = _PT_PLAN.nodes[index].model_copy(update={field: value})
    nodes = _PT_PLAN.nodes[:index] + (node,) + _PT_PLAN.nodes[index + 1:]
    diagnostic = _refusal(_PT_PLAN.model_copy(update={"nodes": nodes}), _PT_CFG, _PT_SYMBOLS)
    assert diagnostic.code in (_PLAN_INVALID, PROCESS_IR_COMPILE_NONDETERMINISTIC), (
        label, diagnostic.code, diagnostic.message,
    )


# ---------------------------------------------------------------------------
# 4. Emission bytes and process options, against the UI capture
# ---------------------------------------------------------------------------


def _emitted_passthrough(label):
    symbols = SymbolTableV1(symbols=())
    cfg, plan = compile_process_ir_v1(parse_process_ir_v1(_doc([dict(P, label=label), MSG, S])), symbols)
    return cfg, emit_process(plan, symbols).shape_xml_parts


def test_the_emitted_start_configuration_is_the_captured_one():
    _cfg, shapes = _emitted_passthrough(CAPTURED_LABEL)
    emitted = re.match(
        r'<shape image="start" name="shape1" shapetype="start" userlabel="([^"]*)"[^>]*>'
        r"(<configuration>.*?</configuration>)",
        shapes[0],
    )
    assert emitted, shapes[0]
    assert emitted.group(2) == CAPTURED_START_CONFIGURATION
    assert emitted.group(1) == CAPTURED_LABEL


def test_the_emitted_start_escapes_its_label():
    """Round-tripped through an XML parser, so the check does not restate the
    escaping implementation it is checking."""
    label = 'Receive <prepared> & "docs"'
    _cfg, shapes = _emitted_passthrough(label)
    assert ET.fromstring(shapes[0]).get("userlabel") == label


def test_the_passthrough_profile_selects_the_captured_option_bytes():
    assert pcm.process_options_for_profile("passthrough") == CAPTURED_PROCESS_OPTIONS
    # The unknown-profile refusal is unchanged.
    with pytest.raises(Exception) as excinfo:
        pcm.process_options_for_profile("cron")
    assert getattr(excinfo.value, "error_code", "") == (
        "PROCESS_MATERIALIZATION_EXECUTION_PROFILE_INVALID"
    )


def test_the_materialized_process_open_tag_is_the_captured_one():
    cfg, shapes = _emitted_passthrough(CAPTURED_LABEL)
    profile = ep.derive_process_execution_profile(cfg, SymbolTableV1(symbols=()))
    xml = pcm.ProcessComponentMaterializer().materialize(
        shapes, name="E184 passthrough", execution_profile=profile
    )
    assert re.search(r'<process xmlns="" ([^>]*)>', xml).group(1) == CAPTURED_PROCESS_OPTIONS


def _passthrough_plan():
    symbols = build_symbol_table([], connector_metadata={})
    plan = build_materialization_plan(
        envelope=ProcessComponentEnvelopeV1(
            component_key="root", name="Passthrough Root", action="create", depends_on=(),
        ),
        process_ir=parse_process_ir_v1(_doc([dict(P, label=CAPTURED_LABEL), MSG, S])),
        symbols=symbols,
        conflict_policy="reuse",
        compiler_revision=get_authoring_revisions()["compiler_revision"],
        emitter_revision=emitter_revision(),
        materializer_revision="sha256:" + "a" * 64,
    )
    return plan, symbols


def test_the_plan_records_the_passthrough_profile_and_apply_emits_its_options():
    plan, symbols = _passthrough_plan()
    assert plan.execution_profile == "passthrough"
    xml = cpa.materialize_canonical_process_xml(plan=plan, id_registry={}, symbols=symbols)
    assert '<process xmlns="" ' + CAPTURED_PROCESS_OPTIONS + ">" in xml
    assert CAPTURED_START_CONFIGURATION in xml


@pytest.mark.parametrize("forged_profile", ["scheduled", "listener"])
def test_a_forged_profile_on_a_passthrough_plan_is_refused_before_emission(forged_profile):
    """Re-fingerprinted so the fingerprint guard passes; apply's re-derivation
    from the recompiled entry refuses it before a byte is emitted."""
    plan, symbols = _passthrough_plan()
    forged = plan.model_copy(update={"execution_profile": forged_profile})
    digest, _material = process_plan_fingerprint(
        forged.model_copy(update={"plan_fingerprint": "sha256:" + "0" * 64})
    )
    forged = forged.model_copy(update={"plan_fingerprint": digest})

    from boomi_mcp.compiler.process_ir import emitter_registry as _er

    emitted = []
    real_emit = _er.emit_process

    def _tripwire(*args, **kwargs):
        emitted.append(1)
        return real_emit(*args, **kwargs)

    _er.emit_process = _tripwire
    try:
        with pytest.raises(cpa.CanonicalProcessApplyError) as excinfo:
            cpa.materialize_canonical_process_xml(plan=forged, id_registry={}, symbols=symbols)
    finally:
        _er.emit_process = real_emit
    assert excinfo.value.error_code == "PROCESS_MATERIALIZATION_EXECUTION_PROFILE_INVALID"
    assert emitted == []


# ---------------------------------------------------------------------------
# 5. The served revision oracle covers the third profile
# ---------------------------------------------------------------------------


def test_the_oracle_covers_three_profiles_and_their_option_bytes():
    oracle = authoring_contract._execution_profile_behaviour_oracle()
    assert oracle["profiles"] == ["listener", "passthrough", "scheduled"]
    assert set(oracle["cases"].values()) == {"scheduled", "listener", "passthrough"}
    passthrough_rows = sorted(k for k, v in oracle["cases"].items() if v == "passthrough")
    assert passthrough_rows and all("passthrough" in row for row in passthrough_rows)
    assert oracle["cases"]["entry-kind-passthrough"] == "passthrough"
    assert oracle["cases"]["passthrough-entry-listener-table"] == "passthrough"
    assert oracle["cases"]["passthrough-entry-id-names-no-node"] == "scheduled"
    assert oracle["process_options"]["passthrough"] == CAPTURED_PROCESS_OPTIONS


def test_the_revision_moves_with_passthrough_classification_or_bytes(monkeypatch):
    baseline = authoring_contract._compiler_revision()
    original = ep.derive_process_execution_profile

    def _two_forms(cfg, symbols):
        answer = original(cfg, symbols)
        return ep.SCHEDULED if answer == ep.PASSTHROUGH else answer

    with monkeypatch.context() as patched:
        patched.setattr(ep, "derive_process_execution_profile", _two_forms)
        assert authoring_contract._compiler_revision() != baseline
    with monkeypatch.context() as patched:
        patched.setitem(
            pcm._PROFILE_OPTIONS, "passthrough",
            pcm.PASSTHROUGH_PROCESS_OPTIONS + ' stopProcessingIfZeroDocuments="true"',
        )
        assert authoring_contract._compiler_revision() != baseline
    assert authoring_contract._compiler_revision() == baseline


# ---------------------------------------------------------------------------
# 6. Entry recognition
# ---------------------------------------------------------------------------


_PASSTHROUGH_ROOT_DOC = _doc([dict(P, label="E184 prepared requests"), MSG, S])


def test_a_passthrough_root_deploys_and_is_never_published_or_probed_as_a_listener(
    monkeypatch,
):
    """Through the public route — plan → compile → apply, then ``orchestrate_deploy``
    dry and real over the #158 suite's recording account — with a genuine WSS Listen
    operation in the same build as a decoy. The root is recognised as a passthrough,
    gets no listener endpoint and no probe, and deploys like any other root.

    CONTROL: the #158 matrix row ``listener-alone`` drives the SAME account surface
    and records a probe, an endpoint and execution-record polls, so the empty
    recordings below are this root's classification, not a surface that records
    nothing.
    """
    from test_issue_158_listener_deployment import (
        _AccountSurface,
        _ROOT_ID,
        _WSS_LISTEN_OP,
        _deploy,
        _error_codes,
        _recorded,
        _request,
        _typed_build,
        _unit,
    )
    from boomi_mcp.categories.deployment import orchestration

    applied, _boundary = _typed_build(
        _request([_unit(_PASSTHROUGH_ROOT_DOC, ())], [_WSS_LISTEN_OP])
    )
    build_id = applied["build_id"]
    assert applied["results"]["root"]["component_id"] == _ROOT_ID
    _entry, components, processes = _recorded(build_id)
    assert _WSS_LISTEN_OP["key"] in [comp["key"] for comp in components]  # the decoy is in
    assert canonical_root_entry(processes[0]["process_ir"]) == ProcessEntryV1("passthrough")

    target, error = orchestration._resolve_build_deployment_target(build_id)
    assert error is None, error
    assert target.process_component_id == _ROOT_ID
    assert orchestration._resolve_listener_metadata(build_id, target) is None
    assert orchestration._target_listener_operation_id(build_id, target) is None

    planned = _deploy(build_id, dry_run=True)
    assert planned["_success"] is True, planned.get("errors")
    assert _error_codes(planned) == []
    assert planned["listener_verify"]["status"] == "not_required"

    surface = _AccountSurface().install(monkeypatch)
    real = _deploy(build_id, dry_run=False)
    assert real["_success"] is True, (real.get("errors"), real.get("error"))
    assert surface.package_boundary == [{"component_id": _ROOT_ID, "package_version": build_id}]
    assert real["listener_verify"]["status"] == "not_required"
    assert surface.probes == []
    assert [call for call in surface.calls if call[0] == "shared_resources"] == []
    assert surface.actions("execution_records") == []


def test_an_api_service_route_to_a_passthrough_root_is_refused():
    """An API Service Component routes only to a listener root; a passthrough root
    is recognised by the same entry authority and refused as a route target."""
    from unittest.mock import MagicMock

    from test_issue_158_listener_deployment import (
        _PROFILE,
        _ApplyBoundary,
        _asc,
        _cause_codes,
        _request,
        _unit,
    )
    from boomi_mcp.categories.integration_builder import build_integration_action

    raw = _request([_unit(_PASSTHROUGH_ROOT_DOC, ())], [_asc("root")]).model_dump(mode="json")
    with _ApplyBoundary().installed():
        planned = build_integration_action(
            MagicMock(), _PROFILE, "plan", config={"authoring_request": raw}
        )
    assert "API_SERVICE_ROUTE_PROCESS_NOT_LISTEN" in _cause_codes(planned), planned


def test_canonical_root_entry_returns_the_real_passthrough_form():
    doc = _doc([dict(P, label="In"), MSG, S])
    for recorded in (doc, parse_process_ir_v1(doc)):
        entry = canonical_root_entry(recorded)
        assert entry == ProcessEntryV1("passthrough")
        assert entry.operation_ref is None
        assert entry.is_listener is False
    assert ProcessEntryV1("listener").is_listener is True
