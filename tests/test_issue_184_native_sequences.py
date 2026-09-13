"""#184 A1/A6/D8/D10: the native sequence grammar and its evidence bound.

Every admission here was refused at the branch point `cbab28f`, with the code and
pointer recorded in the issue's own "Current predicates" table and re-measured in
`docs/architecture/evidence/issue-184/predicates/probe_cbab28f.jsonl`. Each positive
case sits beside a negative neighbour that stays refused.

The attested prefix table is bound, in both directions, to the live capture
`cap184-prefix-predecessors`. The expected rows are derived from that capture's own
MANIFEST, never restated here.
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

from boomi_mcp.compiler.process_ir import connector_resolution  # noqa: E402
from boomi_mcp.errors import (  # noqa: E402
    PROCESS_IR_CAPABILITY_NODE_NOT_ALLOWED_IN_BODY,
    PROCESS_IR_CAPABILITY_PROCESS_CALL_PLACEMENT_UNSUPPORTED,
    PROCESS_IR_CAPABILITY_PROCESS_CALL_RETURN_PATH_BINDING_UNSUPPORTED,
    PROCESS_IR_CAPABILITY_UNSUPPORTED,
    PROCESS_IR_SCHEMA_INVALID_CARDINALITY,
)
from boomi_mcp.models import process_ir as model  # noqa: E402
from boomi_mcp.models.process_ir import ProcessIRValidationError, parse_process_ir_v1  # noqa: E402

_CAPTURE = _ROOT / "docs/architecture/evidence/issue-184/captures/cap184-prefix-predecessors/MANIFEST.json"
_PROBE = _ROOT / "docs/architecture/evidence/issue-184/predicates/probe_cbab28f.jsonl"

#: The capture names its runs `pfx_<kind>_<br|dec>`; these map a run's names onto the
#: authored vocabulary. A retrieve row covers both authored cache-read kinds, which
#: lower to the same platform step.
_CAPTURE_KIND_TO_AUTHORED = {
    "map": ("map_ref",),
    "set_ddp": ("set_ddp",),
    "set_dpp": ("set_dpp",),
    "cache_retrieve": ("cache_get", "document_cache_retrieve"),
    "flow_control": ("flow_control",),
    "message": ("message",),
    "data_process": ("data_process",),
    "cache_remove": ("cache_remove",),
    "cache_remove_primed": ("cache_remove",),
}
_CAPTURE_CONTEXT = {"br": "branch_leg", "dec": "decision_true_arm"}


def _captured_rows():
    """`{(context, authored kind): verdict}` read off the capture MANIFEST."""
    manifest = json.loads(_CAPTURE.read_text())
    rows = {}
    for row in manifest["verdict"]["rows"]:
        runs = [item for item in row["evidence"] if item.startswith("runs/pfx_")]
        if not runs:
            continue  # the added connector-successor control is not a prefix row
        run = runs[0].split("/")[1].rsplit("-", 1)[0]  # pfx_<kind>_<ctx>
        body, context = run[len("pfx_"):].rsplit("_", 1)
        for kind in _CAPTURE_KIND_TO_AUTHORED[body]:
            key = (_CAPTURE_CONTEXT[context], kind)
            verdicts = rows.setdefault(key, set())
            verdicts.add(row["verdict"])
    return rows


def test_the_attested_prefix_table_is_the_captured_admissions_exactly():
    """Both directions: every admitted row has an ADMIT run, and every ADMIT run is a row."""
    captured = _captured_rows()
    assert captured, "the capture MANIFEST yielded no prefix rows — the binding would be vacuous"
    admitted_by_capture = {key for key, verdicts in captured.items() if verdicts == {"ADMIT"}}
    refused_by_capture = {key for key, verdicts in captured.items() if "REFUSE" in verdicts}
    assert refused_by_capture, "no refused row — the negative half of the binding would be vacuous"

    table = {(context, kind) for context, kind, _form, _wait in model.PROCESS_CALL_ATTESTED_PREDECESSORS}
    assert table == admitted_by_capture, {
        "admitted_without_a_capture": sorted(table - admitted_by_capture),
        "captured_but_not_admitted": sorted(admitted_by_capture - table),
    }
    assert not (table & refused_by_capture), sorted(table & refused_by_capture)
    # The capture's child is a Data Passthrough child called with wait=true: only that
    # form and that wait are admitted (#184 amendment 3 §8).
    assert {(form, wait) for _c, _k, form, wait in model.PROCESS_CALL_ATTESTED_PREDECESSORS} == {("passthrough", True)}
    assert {context for context, _kind in table} == set(model.PROCESS_CALL_PREFIX_CONTEXTS)


def _leg_with_prefix(*steps):
    return {"version": "1", "body": {"kind": "sequence", "steps": [{"kind": "branch", "legs": [
        {"steps": list(steps), "terminal": {"kind": "process_call", "process_ref": "$ref:CHILD"}},
        {"steps": [{"kind": "message", "text": "m"}], "terminal": {"kind": "stop"}},
    ]}]}}


def _arm_with_prefix(*steps):
    return {"version": "1", "body": {"kind": "sequence", "steps": [{
        "kind": "decision", "comparison": "equals",
        "left": {"value_type": "static", "static_value": "a"},
        "right": {"value_type": "static", "static_value": "a"},
        "true_arm": {"steps": list(steps), "terminal": {"kind": "process_call", "process_ref": "$ref:CHILD"}},
        "false_arm": {"steps": [], "terminal": {"kind": "stop"}},
    }]}}


_SET_DPP = {"kind": "set_dpp", "name": "Z", "source_values": [{"value_type": "static", "value": "v"}]}
_SET_DDP = {"kind": "set_ddp", "name": "X", "source_values": [{"value_type": "static", "value": "v"}]}


def _diagnostics(payload):
    try:
        parse_process_ir_v1(payload)
    except ProcessIRValidationError as exc:
        return [(item.code, item.path) for item in exc.diagnostics]
    return []


@pytest.mark.parametrize("builder,label", [(_leg_with_prefix, "branch leg"), (_arm_with_prefix, "true arm")])
@pytest.mark.parametrize("prefix", [
    pytest.param((_SET_DDP,), id="set_ddp"),
    pytest.param((_SET_DPP,), id="set_dpp"),
    pytest.param(({"kind": "message", "text": "m"},), id="message"),
    pytest.param(({"kind": "cache_get", "cache_ref": "$ref:C", "external_writer": True},
                  {"kind": "map_ref", "map_ref": "$ref:M"}, _SET_DDP), id="read_map_writer"),
])
def test_an_attested_prefix_before_a_terminal_call_parses(builder, label, prefix):
    assert _diagnostics(builder(*prefix)) == []


@pytest.mark.parametrize("builder,terminal_pointer,cache_pointer", [
    # Explicit ids keep the node ids the wave gate registered for the two-value form.
    pytest.param(_leg_with_prefix, "/body/steps/0/legs/0/terminal",
                 "/body/steps/0/legs/0/steps/0/cache_ref",
                 id="_leg_with_prefix-/body/steps/0/legs/0/terminal"),
    pytest.param(_arm_with_prefix, "/body/steps/0/true_arm/terminal",
                 "/body/steps/0/true_arm/steps/0/cache_ref",
                 id="_arm_with_prefix-/body/steps/0/true_arm/terminal"),
])
def test_a_prefix_ending_on_an_unattested_predecessor_is_refused_at_the_terminal(
    builder, terminal_pointer, cache_pointer
):
    """A cache remove hands the call nothing (measured, ledger E0-184-01).

    #184 amendment 3: an all-document Remove from Cache hands on ZERO documents, so
    ANY authored successor on its path — the terminal process call included — never
    runs. That is now refused by the zero-emission rule at the remove's own
    ``/cache_ref``, reported BEFORE the process-call prefix rule could answer at the
    terminal. The prefix stays out of the attested table either way (the capture's
    cache-remove rows are REFUSE).
    """
    diagnostics = _diagnostics(builder({"kind": "cache_remove", "cache_ref": "$ref:C"}))
    assert diagnostics[:1] == [(PROCESS_IR_SCHEMA_INVALID_CARDINALITY, cache_pointer)], diagnostics
    assert (PROCESS_IR_CAPABILITY_PROCESS_CALL_PLACEMENT_UNSUPPORTED, terminal_pointer) not in diagnostics[:1]
    assert not any(
        kind == "cache_remove" for _context, kind, _form, _wait in model.PROCESS_CALL_ATTESTED_PREDECESSORS
    )


def test_a_connector_in_the_prefix_keeps_the_mixing_refusal():
    diagnostics = _diagnostics(_leg_with_prefix({"kind": "connector_call", "operation_ref": "$ref:OP"}))
    assert diagnostics[:1] == [(PROCESS_IR_CAPABILITY_NODE_NOT_ALLOWED_IN_BODY, "/body/steps/0/legs/0/steps/0")], diagnostics


def test_a_connector_above_a_prefixed_terminal_call_answers_at_the_terminal():
    """With the prefix admitted, the whole-document mixing walk owns the diagnosis."""
    payload = {"version": "1", "body": {"kind": "sequence", "steps": [
        {"kind": "connector_call", "operation_ref": "$ref:OP"},
        {"kind": "branch", "legs": [
            {"steps": [_SET_DPP], "terminal": {"kind": "process_call", "process_ref": "$ref:CHILD"}},
            {"steps": [{"kind": "message", "text": "m"}], "terminal": {"kind": "stop"}},
        ]}]}}
    diagnostics = _diagnostics(payload)
    assert diagnostics[:1] == [(PROCESS_IR_CAPABILITY_NODE_NOT_ALLOWED_IN_BODY, "/body/steps/1/legs/0/terminal")], diagnostics


def test_a_root_prefix_before_a_call_moves_to_the_placement_code_and_a_successor_keeps_its_code():
    prefix = _diagnostics({"version": "1", "body": {"kind": "sequence", "steps": [
        _SET_DPP, {"kind": "process_call", "process_ref": "$ref:CHILD"}]}})
    assert prefix[:1] == [(PROCESS_IR_CAPABILITY_PROCESS_CALL_PLACEMENT_UNSUPPORTED, "/body/steps/0")], prefix
    successor = _diagnostics({"version": "1", "body": {"kind": "sequence", "steps": [
        {"kind": "process_call", "process_ref": "$ref:CHILD"}, {"kind": "stop"}]}})
    assert successor[:1] == [(PROCESS_IR_CAPABILITY_PROCESS_CALL_RETURN_PATH_BINDING_UNSUPPORTED, "/body/steps/1")], successor


def _root(*steps):
    return {"version": "1", "body": {"kind": "sequence", "steps": list(steps)}}


_CALL = {"kind": "connector_call", "operation_ref": "$ref:OP"}
_MAP = {"kind": "map_ref", "map_ref": "$ref:M"}
_STOP = {"kind": "stop"}


@pytest.mark.parametrize("payload", [
    pytest.param(_root(_CALL, _MAP, _MAP, _CALL, _STOP), id="consecutive_root_maps"),
    pytest.param(_root(_CALL, _MAP, _STOP), id="root_map_suffix"),
    pytest.param(_root(_CALL, _SET_DDP, _STOP), id="root_property_suffix"),
    pytest.param(_root({"kind": "cache_get", "cache_ref": "$ref:C", "external_writer": True}, _MAP, _STOP),
                 id="call_free_root_led_by_a_cache_read"),
    pytest.param(_root({"kind": "document_cache_retrieve", "cache_ref": "$ref:C"}, _SET_DPP,
                       {"kind": "return_documents"}), id="call_free_root_returning_documents"),
])
def test_a_native_root_the_branch_point_refused_now_parses(payload):
    assert _diagnostics(payload) == []


@pytest.mark.parametrize("payload,code", [
    pytest.param(_root({"kind": "message", "text": "m"}, _STOP), PROCESS_IR_SCHEMA_INVALID_CARDINALITY,
                 id="call_free_root_with_no_producer"),
    pytest.param(_root(_SET_DPP, {"kind": "cache_get", "cache_ref": "$ref:C"}, _STOP),
                 PROCESS_IR_SCHEMA_INVALID_CARDINALITY, id="call_free_root_not_led_by_the_read"),
    pytest.param(_root(_CALL, {"kind": "cache_put", "cache_ref": "$ref:C"}, _CALL, _STOP),
                 PROCESS_IR_SCHEMA_INVALID_CARDINALITY, id="cache_write_adjacency_unchanged"),
    pytest.param(_root({"kind": "cache_get", "cache_ref": "$ref:C"}, _CALL, _MAP, _STOP), None,
                 id="producer_prefix_before_a_call_still_admitted"),
])
def test_the_negative_neighbours_keep_their_refusals(payload, code):
    diagnostics = _diagnostics(payload)
    if code is None:
        assert diagnostics == [], diagnostics
    else:
        assert diagnostics and diagnostics[0][0] == code, diagnostics


def test_a_try_catch_terminated_root_keeps_both_old_rules():
    """D11: the error-context compositions are not widened."""
    # A VALID handler, so each refusal below is the root rule under test and not the
    # handler's own catch-body rule (a first draft left the catch body empty, which is
    # refused at `/catch_body` and made the map assertion pass for the wrong reason).
    handler = {"kind": "try_catch", "scope": "connector",
               "try_body": {"steps": [_CALL], "terminal": _STOP},
               "catch_body": {"steps": [{"kind": "message", "text": "m"}], "terminal": _STOP}}
    assert _diagnostics(_root(_CALL, handler)) == [], "the handler itself must be valid"
    mapped = _diagnostics(_root(_CALL, _MAP, handler))
    assert mapped and mapped[0][0] == PROCESS_IR_SCHEMA_INVALID_CARDINALITY, mapped
    suffixed = _diagnostics(_root(_CALL, _SET_DDP, handler))
    assert suffixed and suffixed[0][0] == PROCESS_IR_CAPABILITY_UNSUPPORTED, suffixed


def test_the_branch_point_refused_the_admitted_root_and_prefix_forms():
    """The 'before' half, read from the archived branch-point measurement."""
    rows = {json.loads(line)["form"]: json.loads(line) for line in _PROBE.read_text().splitlines() if line}
    for form in ("root_call_map_map_call", "root_call_map_stop", "root_call_setddp_stop",
                 "root_linear_cacheget_map_stop", "leg_prefix_cget_map_ddp_pc", "leg_prefix_ddp_pc",
                 "true_arm_prefix_cget_pc"):
        assert rows[form].get("compile") != "ok+emit", rows[form]


def test_the_root_read_kinds_are_the_connector_walks_document_producers():
    """ONE statement of which reads may lead a call-free root, pinned both ways.

    #184 amendment 3 withdrew the connector walk's own ``_STREAM_PRODUCING_KINDS``:
    the model's root-read kinds and the walk's triggered reads are both read from
    the document-emission authority, and that authority is pinned to the served
    contract's all-document stream replacers.
    """
    from boomi_mcp.authoring.process_ir_projection import process_ir_authoring_revision_payload
    from boomi_mcp.models import process_ir_document_semantics as emission

    assert model.ROOT_ENTRY_READ_KINDS is emission.TRIGGERED_REPLACEMENT_KINDS
    assert model.ROOT_ENTRY_READ_KINDS == emission.TRIGGERED_REPLACEMENT_KINDS
    served = {
        entry["subject"]
        for entry in process_ir_authoring_revision_payload()["entries"]
        if entry.get("entry_type") == "node"
        and (entry.get("document_semantics") or {}).get("output_documents") == "stream_replacing"
        and (entry.get("document_semantics") or {}).get("grouping") == "all_documents"
    }
    assert served, "the served contract published no all-document stream replacer — the pin would be vacuous"
    assert model.ROOT_ENTRY_READ_KINDS == served, {
        "only_in_model": sorted(model.ROOT_ENTRY_READ_KINDS - served),
        "only_in_served_contract": sorted(served - model.ROOT_ENTRY_READ_KINDS),
    }
    # The connector walk's triggered reads are the same rows' semantic kinds.
    assert connector_resolution.TRIGGERED_REPLACEMENT_SEMANTIC_KINDS == {
        emission.DOCUMENT_EMISSION_V1[kind].semantic_kind for kind in model.ROOT_ENTRY_READ_KINDS
    }
