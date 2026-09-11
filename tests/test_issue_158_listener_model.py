"""#158: the ``listener`` node's authoring contract, at both public entry points.

A listener is the first root step, exactly once, authoring ONLY its operation —
the exact inverse of ``SourceEndpointV1``, whose connection is required. It
composes with linear steps followed by a target and a stop, or with
connector_call steps ending on a call before a stop, and with nothing else.

Every refusal is asserted at BOTH entry points a caller can reach: the parser
(``parse_process_ir_v1``) and the compiler handed an already-parsed model that
was mutated afterwards (``compile_process_ir_v1``, which re-parses since #178).
The two must serve one identity for one mistake. The full body-slot grid — a
listener in every control-body slot, both entry points — is covered by the
generated matrix in ``tests/test_process_ir_entrypoint_diagnostic_parity.py``;
this file pins the ROOT half, which that matrix does not reach.
"""

from __future__ import annotations

import copy

import pytest

from boomi_mcp.compiler.process_ir.contracts import SymbolTableV1
from boomi_mcp.compiler.process_ir.diagnostics import ProcessIRCompileError
from boomi_mcp.compiler.process_ir.pipeline import compile_process_ir_v1
from boomi_mcp.errors import (
    PROCESS_IR_CAPABILITY_LISTENER_COMPOSITION_UNSUPPORTED,
    PROCESS_IR_CAPABILITY_NODE_NOT_ALLOWED_IN_BODY,
    PROCESS_IR_CAPABILITY_UNSUPPORTED,
    PROCESS_IR_SCHEMA_INVALID,
    PROCESS_IR_SCHEMA_INVALID_CARDINALITY,
    PROCESS_IR_SCHEMA_LISTENER_CONNECTION_FORBIDDEN,
)
from boomi_mcp.models.process_ir import (
    PROCESS_IR_V1_CAPABILITIES,
    ProcessIRValidationError,
    listener_root_verdict,
    parse_process_ir_v1,
    process_ir_v1_json_schema,
)

from test_process_ir_entrypoint_diagnostic_parity import _atom  # noqa: E402

_LISTENER = {"kind": "listener", "operation_ref": "$ref:wss_op"}
_TARGET = {"kind": "target", "connection_ref": "$ref:tc", "operation_ref": "$ref:to"}
_CALL = {"kind": "connector_call", "operation_ref": "$ref:get_op"}
_MAP = {"kind": "map_ref", "map_ref": "$ref:map"}
_STOP = {"kind": "stop"}
_MESSAGE = {"kind": "message", "text": "hello"}


def _doc(steps):
    return {"version": "1", "body": {"kind": "sequence", "steps": copy.deepcopy(steps)}}


def _parse_refusal(steps):
    with pytest.raises(ProcessIRValidationError) as excinfo:
        parse_process_ir_v1(_doc(steps))
    return excinfo.value.diagnostics[0]


# ---------------------------------------------------------------------------
# Admitted forms
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "steps",
    [
        [_LISTENER, _TARGET, _STOP],
        [_LISTENER, _MAP, _TARGET, _STOP],
        [_LISTENER, _MESSAGE, _MAP, _TARGET, _STOP],
        [_LISTENER, _CALL, _STOP],
        [_LISTENER, _MESSAGE, _CALL, _MAP, _CALL, _STOP],
        [dict(_LISTENER, label="Receive <orders> & \"more\""), _TARGET, _STOP],
        [dict(_LISTENER, inbound_validation={"mode": "profile_bound"}), _TARGET, _STOP],
    ],
    ids=[
        "endpoint", "endpoint-map", "endpoint-linear-map", "call", "call-map-call",
        "label", "inbound-validation",
    ],
)
def test_admitted_listener_forms_parse(steps):
    ir = parse_process_ir_v1(_doc(steps))
    assert ir.body.steps[0].kind == "listener"
    assert listener_root_verdict([step["kind"] for step in steps]) is None


def test_listener_entry_is_published_supported_and_its_error_scope_stays_gated():
    assert PROCESS_IR_V1_CAPABILITIES["listener_entry"] == "supported"
    assert PROCESS_IR_V1_CAPABILITIES["listener_error_scope"] == "gated"


# ---------------------------------------------------------------------------
# The connection is FORBIDDEN, not ignored
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("value", ["$ref:conn", None], ids=["value", "null"])
def test_connection_ref_is_refused_as_a_value_and_as_null(value):
    diagnostic = _parse_refusal([dict(_LISTENER, connection_ref=value), _TARGET, _STOP])
    assert diagnostic.code == PROCESS_IR_SCHEMA_LISTENER_CONNECTION_FORBIDDEN
    assert diagnostic.path == "/body/steps/0/connection_ref"


def test_connection_ref_on_another_node_keeps_its_own_diagnosis():
    """Matched on the IMMEDIATE owner: an unknown field elsewhere is not a
    listener connection, and must not be told it is."""
    diagnostic = _parse_refusal(
        [_LISTENER, dict(_MESSAGE, connection_ref="$ref:conn"), _TARGET, _STOP]
    )
    assert diagnostic.code != PROCESS_IR_SCHEMA_LISTENER_CONNECTION_FORBIDDEN


def test_inbound_validation_mode_is_closed():
    diagnostic = _parse_refusal(
        [dict(_LISTENER, inbound_validation={"mode": "strict"}), _TARGET, _STOP]
    )
    assert diagnostic.code == PROCESS_IR_SCHEMA_INVALID


# ---------------------------------------------------------------------------
# Placement and composition — ONE verdict, both entry points
# ---------------------------------------------------------------------------


_PLACEMENT_CASES = [
    # (id, steps, code, pointer)
    ("after-prefix", [_MESSAGE, _LISTENER, _TARGET, _STOP],
     PROCESS_IR_SCHEMA_INVALID_CARDINALITY, "/body/steps/1"),
    ("twice", [_LISTENER, _LISTENER, _TARGET, _STOP],
     PROCESS_IR_SCHEMA_INVALID_CARDINALITY, "/body/steps/1"),
    ("with-source", [_LISTENER, {"kind": "source", "connection_ref": "$ref:sc",
                                 "operation_ref": "$ref:so"}, _TARGET, _STOP],
     PROCESS_IR_CAPABILITY_LISTENER_COMPOSITION_UNSUPPORTED, "/body/steps/1"),
    ("with-process-call", [_LISTENER, _atom("process_call")],
     PROCESS_IR_CAPABILITY_LISTENER_COMPOSITION_UNSUPPORTED, "/body/steps/1"),
    ("with-try-catch", [_LISTENER, _atom("try_catch")],
     PROCESS_IR_CAPABILITY_LISTENER_COMPOSITION_UNSUPPORTED, "/body/steps/1"),
    ("with-branch", [_LISTENER, _atom("branch")],
     PROCESS_IR_CAPABILITY_LISTENER_COMPOSITION_UNSUPPORTED, "/body/steps/1"),
    ("with-decision", [_LISTENER, _atom("decision")],
     PROCESS_IR_CAPABILITY_LISTENER_COMPOSITION_UNSUPPORTED, "/body/steps/1"),
    ("with-flow-control", [_LISTENER, _atom("flow_control"), _TARGET, _STOP],
     PROCESS_IR_CAPABILITY_LISTENER_COMPOSITION_UNSUPPORTED, "/body/steps/1"),
    ("with-return-documents", [_LISTENER, _TARGET, _atom("return_documents")],
     PROCESS_IR_CAPABILITY_LISTENER_COMPOSITION_UNSUPPORTED, "/body/steps/2"),
    ("with-exception", [_LISTENER, _atom("exception")],
     PROCESS_IR_CAPABILITY_LISTENER_COMPOSITION_UNSUPPORTED, "/body/steps/1"),
]


@pytest.mark.parametrize(
    "steps,code,pointer",
    [case[1:] for case in _PLACEMENT_CASES],
    ids=[case[0] for case in _PLACEMENT_CASES],
)
def test_placement_and_composition_refusals_serve_one_identity(steps, code, pointer):
    """Parser and compiler agree on the code AND the pointer for each mistake.

    The compiler path hands over a model parsed LEGAL and mutated afterwards —
    the route a caller holding a model can take — so the refusal is served from
    the compile boundary rather than asserted twice from the parser.
    """
    parsed = _parse_refusal(steps)
    assert (parsed.code, parsed.path) == (code, pointer)

    # A LEGAL carrier whose step list is then replaced by individually valid
    # nodes of the illegal document's kinds — no validator runs on assignment.
    carrier = parse_process_ir_v1(_doc([_LISTENER, _TARGET, _STOP]))
    carrier.body.steps = [_parsed_member(step) for step in steps]
    with pytest.raises(ProcessIRCompileError) as excinfo:
        compile_process_ir_v1(carrier, SymbolTableV1(symbols=()))
    served = excinfo.value.diagnostics[0]
    assert (served.code, served.path) == (code, pointer)


def _parsed_member(step):
    """One node, parsed on its own in a root where it is LEGAL, then detached."""
    kind = step["kind"]
    if kind == "listener":
        return parse_process_ir_v1(_doc([step, _TARGET, _STOP])).body.steps[0]
    if kind in ("target",):
        return parse_process_ir_v1(_doc([_LISTENER, step, _STOP])).body.steps[1]
    if kind == "stop":
        return parse_process_ir_v1(_doc([_LISTENER, _TARGET, step])).body.steps[2]
    if kind == "message":
        return parse_process_ir_v1(_doc([_LISTENER, step, _TARGET, _STOP])).body.steps[1]
    if kind == "source":
        return parse_process_ir_v1(_doc([step, _TARGET, _STOP])).body.steps[0]
    if kind == "flow_control":
        return parse_process_ir_v1(
            _doc([{"kind": "source", "connection_ref": "$ref:sc",
                   "operation_ref": "$ref:so"}, step, _TARGET, _STOP])
        ).body.steps[1]
    if kind == "return_documents":
        return parse_process_ir_v1(
            _doc([{"kind": "source", "connection_ref": "$ref:sc",
                   "operation_ref": "$ref:so"}, step])
        ).body.steps[1]
    if kind == "exception":
        return parse_process_ir_v1(
            _doc([{"kind": "source", "connection_ref": "$ref:sc",
                   "operation_ref": "$ref:so"}, step])
        ).body.steps[1]
    # Controls and process_call are legal as a lone root.
    return parse_process_ir_v1(_doc([step])).body.steps[0]


@pytest.mark.parametrize(
    "steps,code",
    [
        ([_LISTENER, _STOP], PROCESS_IR_SCHEMA_INVALID_CARDINALITY),
        ([_LISTENER], PROCESS_IR_SCHEMA_INVALID_CARDINALITY),
        ([_LISTENER, _MAP, _CALL, _STOP], PROCESS_IR_SCHEMA_INVALID_CARDINALITY),
        ([_LISTENER, _CALL, _MAP, _STOP], PROCESS_IR_SCHEMA_INVALID_CARDINALITY),
        ([_LISTENER, _CALL, _TARGET, _STOP], PROCESS_IR_CAPABILITY_UNSUPPORTED),
        ([_LISTENER, _CALL, _MESSAGE, _STOP], PROCESS_IR_CAPABILITY_UNSUPPORTED),
        ([_LISTENER, _TARGET, _MESSAGE, _STOP], PROCESS_IR_SCHEMA_INVALID_CARDINALITY),
    ],
    ids=[
        "no-target", "alone", "map-after-listener", "trailing-map", "mixed-forms",
        "linear-suffix", "target-not-last",
    ],
)
def test_listener_flow_grammar_refusals(steps, code):
    assert _parse_refusal(steps).code == code


def test_a_listener_in_a_control_body_is_a_body_capability_failure():
    """Root-only: the body unions do not admit it, and the diagnostic says so
    rather than calling a documented kind "unknown"."""
    diagnostic = _parse_refusal([{
        "kind": "branch",
        "legs": [
            {"steps": [_LISTENER], "terminal": _STOP},
            {"steps": [_MESSAGE], "terminal": _STOP},
        ],
    }])
    assert diagnostic.code == PROCESS_IR_CAPABILITY_NODE_NOT_ALLOWED_IN_BODY


def test_the_served_schema_describes_the_listener_in_caller_vocabulary():
    """The model docstring IS served schema: it may not leak compiler words."""
    listener = process_ir_v1_json_schema()["$defs"]["ListenerEntryNodeV1"]
    text = listener["description"].lower()
    for word in ("start_listen", "emitter", "cfg", "shape1", "synthetic"):
        assert word not in text, word
    assert set(listener["properties"]) == {
        "kind", "operation_ref", "label", "inbound_validation",
    }
    assert listener["required"] == ["kind", "operation_ref"]


def test_the_served_listener_entries_agree_with_their_runtime_authorities():
    """The served projection's listener facts, checked against what enforces them.

    Hand-written prose is served, so it is parity-pinned: the entry-authority rule
    must name exactly the two forms the entry policy admits; the node entry must
    cite the capability rows the manifest actually carries; and every new code must
    resolve to a served repair entry.
    """
    from boomi_mcp.authoring.process_ir_projection import (
        authoring_contract_entry_ids_for_diagnostic,
        build_process_ir_authoring_entries,
    )
    from boomi_mcp.compiler.process_ir.entry_policy import ENTRY_FORMS, entry_form_rows

    entries = {e.contract_entry_id: e for e in build_process_ir_authoring_entries()}
    for entry_id in (
        "node.listener", "capability.listener_entry",
        "semantic_rule.listener.entry_authority",
        "semantic_rule.listener.composition",
        "semantic_rule.listener.inbound_validation",
    ):
        assert entry_id in entries, entry_id

    assert set(ENTRY_FORMS) == {"scheduled", "listener"}
    assert entry_form_rows() == (("listener", "listener"), ("scheduled", "scheduled"))
    authority = entries["semantic_rule.listener.entry_authority"].summary.lower()
    assert "exactly one" in authority
    for form in ENTRY_FORMS:
        assert form in authority, form
    for word in ("start_listen", "start_noaction", "emitter", "shape1"):
        assert word not in authority, word

    node = entries["node.listener"]
    cited = [r[len("capability."):] for r in node.related_entry_ids if r.startswith("capability.")]
    assert set(cited) == {"listener_entry", "listener_error_scope"}
    assert all(capability in PROCESS_IR_V1_CAPABILITIES for capability in cited)
    for related in node.related_entry_ids:
        assert related in entries, related
    for code in (
        PROCESS_IR_SCHEMA_LISTENER_CONNECTION_FORBIDDEN,
        PROCESS_IR_CAPABILITY_LISTENER_COMPOSITION_UNSUPPORTED,
        "PROCESS_IR_REFERENCE_LISTENER_OPERATION_INVALID",
        "PROCESS_IR_SEMANTIC_LISTENER_INBOUND_CONTRACT_UNSATISFIED",
    ):
        assert authoring_contract_entry_ids_for_diagnostic(code), code
