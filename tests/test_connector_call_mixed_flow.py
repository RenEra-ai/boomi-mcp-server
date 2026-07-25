"""The representative mixed linear flow (issue #140, M12.5).

The issue's goal flow — five connector calls across three connector families with
one map between them — realized as:

    REST GET -> MapRef -> SOAP EXECUTE -> REST PATCH -> Database Send -> Stop

The Database Send is LAST because official Boomi documentation states a ``Send``
action "does not return any data to the process for further processing" and
Database (Legacy) declares only a Write profile (an input). Placing it mid-flow
would emit a shape that can never run; the evidence and the decision are recorded
in ``.codex/plans/issue-140-live-captures.md``. A call after a Send is a hard
error here, pinned below.

**Golden oracle.** The legacy builder cannot express a multi-connector flow at
all, so there is no legacy-parity oracle for this XML. Four independent things
stand in for one, and each is asserted:

1. every ``connectoraction`` shape is byte-identical in STRUCTURE to the shipped,
   live-QA-verified ``sync_pipeline_*`` goldens, because both come from the same
   unmodified ``render_connectoraction``;
2. ``verify_process_graph`` accepts the emitted XML;
3. the emission-plan invariants (wiring, geometry, terminal sets, recomputed
   emitter inputs) hold;
4. the bytes are stable across repeated compiles and shuffled symbol order.
"""

from __future__ import annotations

import re
import sys
from pathlib import Path

import pytest

_ROOT = Path(__file__).resolve().parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))
if str(_ROOT / "src") not in sys.path:
    sys.path.insert(0, str(_ROOT / "src"))

from boomi_mcp.categories.components.process_graph_verifier import verify_process_graph
from boomi_mcp.compiler.process_ir import connector_capabilities as caps
from boomi_mcp.compiler.process_ir.contracts import ComponentSymbolV1, SymbolTableV1
from boomi_mcp.compiler.process_ir.diagnostics import ProcessIRCompileError
from boomi_mcp.compiler.process_ir.emitter_registry import emit_process
from boomi_mcp.compiler.process_ir.pipeline import compile_process_ir_v1
from boomi_mcp.errors import (
    PROCESS_IR_SEMANTIC_CARDINALITY_MISMATCH,
    PROCESS_IR_SEMANTIC_PROFILE_MISMATCH,
)
from boomi_mcp.models.process_ir import parse_process_ir_v1

_FIXTURES = _ROOT / "tests" / "fixtures" / "process_ir"
_GOLDEN = _FIXTURES / "emitter_parity" / "connector_call_mixed.process.xml"
_IR_FIXTURE = _FIXTURES / "connector_call_mixed.json"


# ---------------------------------------------------------------------------
# The symbol table — the compiler-supplied resolution context (ADR-001 §6).
# Sentinel refs/ids only; profile data is schema metadata, never content.
# ---------------------------------------------------------------------------


def _symbol(ref, component_type, **extra):
    return ComponentSymbolV1(
        ref=ref, component_id="id_" + ref, component_type=component_type, **extra
    )


def mixed_symbols():
    return SymbolTableV1(
        symbols=(
            _symbol("conn_rest", "connector-settings", connector_type="rest"),
            _symbol("conn_soap", "connector-settings", connector_type="soap_client"),
            _symbol("conn_db", "connector-settings", connector_type="database"),
            _symbol("prof_get_response", "profile.json"),
            _symbol("prof_soap_request", "profile.xml"),
            _symbol("prof_soap_response", "profile.xml"),
            _symbol("prof_patch_response", "profile.json"),
            _symbol("prof_db_write", "profile.db"),
            _symbol(
                "op_rest_get",
                "connector-action",
                connector_type="rest",
                action_type="GET",
                connection_ref="conn_rest",
                output_profile_ref="prof_get_response",
            ),
            _symbol(
                "map_get_to_soap",
                "transform.map",
                input_profile_ref="prof_get_response",
                output_profile_ref="prof_soap_request",
            ),
            _symbol(
                "op_soap_execute",
                "connector-action",
                connector_type="soap_client",
                action_type="EXECUTE",
                connection_ref="conn_soap",
                input_profile_ref="prof_soap_request",
                output_profile_ref="prof_soap_response",
            ),
            _symbol(
                "op_rest_patch",
                "connector-action",
                connector_type="rest",
                action_type="PATCH",
                connection_ref="conn_rest",
                input_profile_ref="prof_soap_response",
                output_profile_ref="prof_patch_response",
            ),
            _symbol(
                "op_db_send",
                "connector-action",
                connector_type="database",
                action_type="Send",
                connection_ref="conn_db",
                input_profile_ref="prof_patch_response",
            ),
        )
    )


MIXED_DOC = {
    "version": "1",
    "body": {
        "kind": "sequence",
        "steps": [
            {
                "kind": "connector_call",
                "operation_ref": "op_rest_get",
                "action": "GET",
                "label": "Read orders",
            },
            {"kind": "map_ref", "map_ref": "map_get_to_soap"},
            {
                "kind": "connector_call",
                "operation_ref": "op_soap_execute",
                "action": "EXECUTE",
            },
            {"kind": "connector_call", "operation_ref": "op_rest_patch", "action": "PATCH"},
            {"kind": "connector_call", "operation_ref": "op_db_send", "action": "Send"},
            {"kind": "stop"},
        ],
    },
}


def compile_mixed(symbols=None):
    return compile_process_ir_v1(parse_process_ir_v1(MIXED_DOC), symbols or mixed_symbols())


def emit_mixed(symbols=None):
    table = symbols or mixed_symbols()
    _cfg, plan = compile_mixed(table)
    return emit_process(plan, table)


def mutated(doc, index, **changes):
    """A copy of ``doc`` with one step changed — keeps each negative isolated."""
    steps = [dict(step) for step in doc["body"]["steps"]]
    steps[index].update(changes)
    return {"version": doc["version"], "body": {"kind": "sequence", "steps": steps}}


def codes_for(doc, table=None):
    with pytest.raises(ProcessIRCompileError) as excinfo:
        compile_process_ir_v1(parse_process_ir_v1(doc), table or mixed_symbols())
    return [(item.code, item.path, item.phase) for item in excinfo.value.diagnostics]


# ---------------------------------------------------------------------------
# The fixture itself
# ---------------------------------------------------------------------------


def test_ir_fixture_matches_the_document_under_test():
    """The committed fixture is the same document the tests compile, so a reader
    of the fixture is reading what actually ships."""
    import json

    assert json.loads(_IR_FIXTURE.read_text()) == MIXED_DOC


def test_the_fixture_carries_only_opaque_sentinel_references():
    text = _IR_FIXTURE.read_text().lower()
    for forbidden in ("password", "secret", "token", "http://", "https://", "@"):
        assert forbidden not in text


# ---------------------------------------------------------------------------
# Compilation
# ---------------------------------------------------------------------------


def test_the_mixed_flow_compiles_to_the_expected_shapes_in_order():
    _cfg, plan = compile_mixed()
    assert [
        (node.ordinal, node.shape_id, node.emitter_input.emitter_kind)
        for node in plan.nodes
    ] == [
        (1, "shape1", "start_noaction"),
        (2, "shape2", "connectoraction_source"),
        (3, "shape3", "map"),
        (4, "shape4", "connectoraction_target"),
        (5, "shape5", "connectoraction_target"),
        (6, "shape6", "connectoraction_target"),
        (7, "shape7", "stop"),
    ]


def test_only_the_entry_call_is_a_source_shape():
    """The entry/downstream split is DERIVED from position. The two emitter keys
    share one renderer, so this selects a key without changing output — but it
    must still be exactly one source, or the plan would claim two entries."""
    _cfg, plan = compile_mixed()
    sources = [
        node.shape_id
        for node in plan.nodes
        if node.emitter_input.emitter_kind == "connectoraction_source"
    ]
    assert sources == ["shape2"]


def test_each_call_binds_its_own_family_action_connection_and_operation():
    _cfg, plan = compile_mixed()
    bound = [
        (
            node.emitter_input.connector_type,
            node.emitter_input.action_type,
            node.emitter_input.connection_id,
            node.emitter_input.operation_id,
        )
        for node in plan.nodes
        if node.emitter_input.emitter_kind.startswith("connectoraction")
    ]
    assert bound == [
        (caps.REST_FAMILY, "GET", "id_conn_rest", "id_op_rest_get"),
        (caps.SOAP_FAMILY, "EXECUTE", "id_conn_soap", "id_op_soap_execute"),
        (caps.REST_FAMILY, "PATCH", "id_conn_rest", "id_op_rest_patch"),
        # NOT "SEND": the database family keeps its authoritative mixed-case verb.
        (caps.DATABASE_FAMILY, "Send", "id_conn_db", "id_op_db_send"),
    ]


def test_two_calls_reusing_one_connection_stay_distinct_operations():
    """REST GET and REST PATCH share ``conn_rest``. One connection reused across
    two calls must not collapse them into one binding."""
    _cfg, plan = compile_mixed()
    rest = [
        node.emitter_input
        for node in plan.nodes
        if getattr(node.emitter_input, "connector_type", None) == caps.REST_FAMILY
    ]
    assert len({item.connection_id for item in rest}) == 1
    assert len({item.operation_id for item in rest}) == 2


def test_the_flow_is_wired_as_one_forward_chain():
    _cfg, plan = compile_mixed()
    wiring = {
        node.shape_id: tuple(t.to_shape_id for t in node.outgoing) for node in plan.nodes
    }
    assert wiring == {
        "shape1": ("shape2",),
        "shape2": ("shape3",),
        "shape3": ("shape4",),
        "shape4": ("shape5",),
        "shape5": ("shape6",),
        "shape6": ("shape7",),
        "shape7": (),
    }
    assert plan.terminal_shape_ids == ("shape7",)


# ---------------------------------------------------------------------------
# Emission, verification, and the golden
# ---------------------------------------------------------------------------


def test_emitted_xml_matches_the_committed_golden_byte_for_byte():
    artifact = emit_mixed()
    assert "".join(artifact.shape_xml_parts) == _GOLDEN.read_text()


def test_the_emitted_process_passes_the_graph_verifier():
    """Oracle (2). ``emit_process`` verifies its own output, and re-running the
    verifier independently on the same wrapped XML must agree — a mixed flow of
    five connector shapes has no dead end, no unwired branch, and one terminal."""
    artifact = emit_mixed()
    assert artifact.verifier.errors == ()
    report = verify_process_graph(artifact.process_xml)
    assert report["errors"] == [], report
    assert report["shapes_checked"] == 7


def test_connector_shapes_are_structurally_identical_to_the_shipped_goldens():
    """Oracle (1). The one thing that CAN be cross-checked against verified
    output: every attribute name and order on a ``connectoraction`` here must
    match the live-QA-verified #139 sync_pipeline golden, because both are
    produced by the same unmodified renderer."""
    shipped = (
        _ROOT / "tests" / "fixtures" / "golden_xml" / "sync_pipeline_db_read_map_rest_send.xml"
    ).read_text()
    pattern = re.compile(r"<connectoraction ([^>]*?)>")

    def attribute_names(xml):
        return {
            tuple(re.findall(r'(\w[\w-]*)="', match))
            for match in pattern.findall(xml)
        }

    ours = attribute_names(_GOLDEN.read_text())
    theirs = attribute_names(shipped)
    assert ours, "the golden must contain connectoraction shapes"
    assert ours <= theirs, (ours, theirs)


def test_the_golden_carries_no_dynamic_path_or_parameter_binding():
    """#140 enables ONLY the simple binding. A ``parameter-profile`` attribute or
    a populated ``dynamicProperties`` would mean a gated capability leaked in."""
    xml = _GOLDEN.read_text()
    assert "parameter-profile" not in xml
    assert "<dynamicProperties/>" in xml
    assert "<parameters/>" in xml


# ---------------------------------------------------------------------------
# Determinism
# ---------------------------------------------------------------------------


def test_repeated_compilation_is_byte_identical():
    first = "".join(emit_mixed().shape_xml_parts)
    second = "".join(emit_mixed().shape_xml_parts)
    assert first == second


def test_shuffled_symbol_insertion_order_cannot_change_output():
    table = mixed_symbols()
    shuffled = SymbolTableV1(symbols=tuple(reversed(table.symbols)))
    assert "".join(emit_mixed(table).shape_xml_parts) == "".join(
        emit_mixed(shuffled).shape_xml_parts
    )


def test_compilation_yields_an_identical_plan_object_each_time():
    _cfg_a, plan_a = compile_mixed()
    _cfg_b, plan_b = compile_mixed()
    assert plan_a == plan_b


# ---------------------------------------------------------------------------
# Negatives — cardinality
# ---------------------------------------------------------------------------


def test_a_call_after_a_non_producing_send_is_rejected():
    """The Send gate. This is the literal step order the issue body names
    (``... -> DB Send -> REST PATCH``); it is rejected, with a stable code,
    rather than emitted as a shape that could never run."""
    doc = {
        "version": "1",
        "body": {
            "kind": "sequence",
            "steps": [
                {"kind": "connector_call", "operation_ref": "op_rest_get"},
                {"kind": "map_ref", "map_ref": "map_get_to_soap"},
                {"kind": "connector_call", "operation_ref": "op_soap_execute"},
                {"kind": "connector_call", "operation_ref": "op_db_send"},
                {"kind": "connector_call", "operation_ref": "op_rest_patch"},
                {"kind": "stop"},
            ],
        },
    }
    assert codes_for(doc) == [
        (
            PROCESS_IR_SEMANTIC_CARDINALITY_MISMATCH,
            "/body/steps/3/operation_ref",
            "semantic_lowering",
        )
    ]


def test_a_document_consumer_cannot_be_the_entry_call():
    doc = {
        "version": "1",
        "body": {
            "kind": "sequence",
            "steps": [
                {"kind": "connector_call", "operation_ref": "op_db_send"},
                {"kind": "stop"},
            ],
        },
    }
    assert codes_for(doc) == [
        (
            PROCESS_IR_SEMANTIC_CARDINALITY_MISMATCH,
            "/body/steps/0/operation_ref",
            "semantic_lowering",
        )
    ]


def test_a_producer_only_flow_is_fine_without_any_consumer():
    doc = {
        "version": "1",
        "body": {
            "kind": "sequence",
            "steps": [
                {"kind": "connector_call", "operation_ref": "op_rest_get"},
                {"kind": "stop"},
            ],
        },
    }
    _cfg, plan = compile_process_ir_v1(parse_process_ir_v1(doc), mixed_symbols())
    assert len(plan.nodes) == 3


def test_return_documents_may_not_follow_a_non_producing_call():
    """``stop`` and ``return_documents`` are NOT interchangeable here. ``stop``
    consumes nothing and merely ends the path; ``return_documents`` returns the
    current document stream to the caller, so after a Send — which returns no
    documents to the process — it would emit a shape that can never return
    anything."""
    doc = {
        "version": "1",
        "body": {
            "kind": "sequence",
            "steps": [
                {"kind": "connector_call", "operation_ref": "op_rest_get"},
                {"kind": "connector_call", "operation_ref": "op_db_send"},
                {"kind": "return_documents"},
            ],
        },
    }
    assert codes_for(doc) == [
        (
            PROCESS_IR_SEMANTIC_CARDINALITY_MISMATCH,
            "/body/steps/2",
            "semantic_lowering",
        )
    ]


def test_return_documents_after_a_producing_call_is_accepted():
    """Guard the guard: the rule above must key on the CALL's output, not simply
    ban ``return_documents`` from connector-call flows."""
    doc = {
        "version": "1",
        "body": {
            "kind": "sequence",
            "steps": [
                {"kind": "connector_call", "operation_ref": "op_rest_get"},
                {"kind": "return_documents"},
            ],
        },
    }
    _cfg, plan = compile_process_ir_v1(parse_process_ir_v1(doc), mixed_symbols())
    assert plan.nodes[-1].emitter_input.emitter_kind == "returndocuments"


def test_a_terminal_send_preceded_by_a_producer_is_accepted():
    """The whole point of the gate is that Send LAST is fine."""
    doc = {
        "version": "1",
        "body": {
            "kind": "sequence",
            "steps": [
                {"kind": "connector_call", "operation_ref": "op_rest_get"},
                {"kind": "connector_call", "operation_ref": "op_db_send"},
                {"kind": "stop"},
            ],
        },
    }
    _cfg, plan = compile_process_ir_v1(parse_process_ir_v1(doc), mixed_symbols())
    assert plan.nodes[-1].emitter_input.emitter_kind == "stop"


# ---------------------------------------------------------------------------
# Negatives — map profile continuity
# ---------------------------------------------------------------------------


def test_map_source_profile_must_match_the_preceding_calls_output():
    table = SymbolTableV1(
        symbols=tuple(
            symbol.model_copy(update={"input_profile_ref": "prof_soap_response"})
            if symbol.ref == "map_get_to_soap"
            else symbol
            for symbol in mixed_symbols().symbols
        )
    )
    assert codes_for(MIXED_DOC, table) == [
        (PROCESS_IR_SEMANTIC_PROFILE_MISMATCH, "/body/steps/1/map_ref", "semantic_lowering")
    ]


def test_map_target_profile_must_match_the_following_calls_input():
    table = SymbolTableV1(
        symbols=tuple(
            symbol.model_copy(update={"output_profile_ref": "prof_patch_response"})
            if symbol.ref == "map_get_to_soap"
            else symbol
            for symbol in mixed_symbols().symbols
        )
    )
    assert codes_for(MIXED_DOC, table)[0][0] == PROCESS_IR_SEMANTIC_PROFILE_MISMATCH


def test_a_map_ref_that_is_not_a_map_component_is_rejected():
    table = SymbolTableV1(
        symbols=tuple(
            symbol.model_copy(update={"component_type": "profile.json"})
            if symbol.ref == "map_get_to_soap"
            else symbol
            for symbol in mixed_symbols().symbols
        )
    )
    assert codes_for(MIXED_DOC, table)[0][0] == PROCESS_IR_SEMANTIC_PROFILE_MISMATCH


def test_an_absent_profile_on_either_side_of_a_map_is_a_mismatch():
    """A map's source/destination profiles are hard component requirements, so
    "not declared" cannot satisfy one. (Connector-to-connector adjacency is
    deliberately NOT gated this way — the platform documents connector profiles
    as non-validating.)"""
    table = SymbolTableV1(
        symbols=tuple(
            symbol.model_copy(update={"output_profile_ref": None})
            if symbol.ref == "op_rest_get"
            else symbol
            for symbol in mixed_symbols().symbols
        )
    )
    assert codes_for(MIXED_DOC, table)[0][0] == PROCESS_IR_SEMANTIC_PROFILE_MISMATCH


def test_profile_refs_must_resolve_to_actual_profile_components():
    """Without a type check the continuity test is self-fulfilling: point BOTH
    sides of a map boundary at the same non-profile component and the identities
    compare equal, so an invalid map "matches" while neither side is a profile."""
    base = mixed_symbols().symbols
    table = SymbolTableV1(
        symbols=tuple(
            symbol.model_copy(update={"output_profile_ref": "conn_rest"})
            if symbol.ref == "op_rest_get"
            else symbol.model_copy(update={"input_profile_ref": "conn_rest"})
            if symbol.ref == "map_get_to_soap"
            else symbol
            for symbol in base
        )
    )
    assert codes_for(MIXED_DOC, table) == [
        (PROCESS_IR_SEMANTIC_PROFILE_MISMATCH, "/body/steps/1/map_ref", "semantic_lowering")
    ]


@pytest.mark.parametrize(
    "profile_type", ["profile.db", "profile.edi", "profile.flatfile", "profile.json", "profile.xml"]
)
def test_every_real_boomi_profile_kind_is_accepted_as_a_profile(profile_type):
    """The type check must not narrow to the three kinds the Data Process emitter
    validates — a map may legitimately read an EDI or flat-file profile, and
    rejecting one would be a false failure."""
    from boomi_mcp.compiler.process_ir.connector_resolution import (
        PROFILE_COMPONENT_TYPES,
    )

    assert profile_type in PROFILE_COMPONENT_TYPES


def test_two_refs_pointing_at_one_profile_component_agree():
    """Continuity compares resolved COMPONENT IDS, not ref tokens — #139B's
    occurrence-scoped aliases legitimately produce two refs for one component."""
    base = mixed_symbols().symbols
    alias = ComponentSymbolV1(
        ref="prof_get_response_alias",
        component_id="id_prof_get_response",
        component_type="profile.json",
    )
    table = SymbolTableV1(
        symbols=tuple(
            symbol.model_copy(update={"input_profile_ref": "prof_get_response_alias"})
            if symbol.ref == "map_get_to_soap"
            else symbol
            for symbol in base
        )
        + (alias,)
    )
    _cfg, plan = compile_process_ir_v1(parse_process_ir_v1(MIXED_DOC), table)
    assert len(plan.nodes) == 7


# ---------------------------------------------------------------------------
# Validation happens before emission
# ---------------------------------------------------------------------------


def test_every_rejection_happens_before_an_emission_plan_exists(monkeypatch):
    """Acceptance criterion: unsupported/invalid combinations fail BEFORE any
    component mutation. The nearest observable proxy in the compiler is that the
    emission-plan lowering is never entered."""
    from boomi_mcp.compiler.process_ir import pipeline as pipeline_module

    calls = []

    def _tripwire(*args, **kwargs):  # pragma: no cover - must never run
        calls.append(args)
        raise AssertionError("emission planning must not run for a rejected payload")

    monkeypatch.setattr(pipeline_module, "lower_cfg_to_emission_plan", _tripwire)
    bad = mutated(MIXED_DOC, 4, action="Get")  # asserts the wrong action
    with pytest.raises(ProcessIRCompileError):
        compile_process_ir_v1(parse_process_ir_v1(bad), mixed_symbols())
    assert calls == []
