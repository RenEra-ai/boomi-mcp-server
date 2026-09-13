"""#184 A4: a map or a cache write on an absent document stream is refused.

Before #184 only a ``documents_required`` connector call was gated on something
upstream on its path having produced documents. A map or a ``cache_put`` in a
call-free Branch leg or Decision arm under a control-only root compiled and
emitted (archived branch-point probe rows ``absent_stream_maps_in_legs`` and
``absent_stream_map_put``), although the empty start document carries nothing to
transform or stage.

Document existence has ONE derived authority: a kind supplies documents regardless
of its upstream iff its served ``document_semantics`` pair is
``output_documents == "stream_replacing"`` AND ``grouping == "all_documents"``.
``message`` and ``data_process`` are served per-document and are NOT from-nothing
producers. The pin below reads the served contract, never a list written here.

Expected codes and pointers come from the issue's own acceptance text (A4: "refused
with a stable code at the consumer's pointer") and the branch-point measurement.
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

from boomi_mcp.compiler.process_ir import connector_capabilities as CC  # noqa: E402
from boomi_mcp.compiler.process_ir import connector_resolution  # noqa: E402
from boomi_mcp.compiler.process_ir.contracts import (  # noqa: E402
    ComponentSymbolV1,
    SymbolTableV1,
)
from boomi_mcp.compiler.process_ir.diagnostics import ProcessIRCompileError  # noqa: E402
from boomi_mcp.compiler.process_ir.pipeline import compile_process_ir_v1  # noqa: E402
from boomi_mcp.errors import PROCESS_IR_SEMANTIC_CARDINALITY_MISMATCH  # noqa: E402
from boomi_mcp.models.process_ir import parse_process_ir_v1  # noqa: E402

_CARD = PROCESS_IR_SEMANTIC_CARDINALITY_MISMATCH


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
        sym("MAP", "MAP", "transform.map", input_profile_ref="$ref:P1", output_profile_ref="$ref:P2"),
        sym("P1", "P1", "profile.json"),
        sym("P2", "P2", "profile.json"),
        sym("CACHE", "CACHE", "documentcache"),
        # legacy source/target endpoints
        sym("conn", "LCONN", "connector-settings", connector_type=rest),
        sym("op", "LOP", "connector-action", connector_type=rest, action_type="GET"),
        sym("tconn", "LTCONN", "connector-settings", connector_type=rest),
        sym("top", "LTOP", "connector-action", connector_type=rest, action_type="PATCH"),
    ))


_GET = {"kind": "connector_call", "operation_ref": "$ref:GET"}
_PATCH = {"kind": "connector_call", "operation_ref": "$ref:PATCH"}
_MAP = {"kind": "map_ref", "map_ref": "$ref:MAP"}
_PUT = {"kind": "cache_put", "cache_ref": "$ref:CACHE"}
_CGET = {"kind": "cache_get", "cache_ref": "$ref:CACHE"}
_STOP = {"kind": "stop"}
_DPP = {"kind": "set_dpp", "name": "Z", "source_values": [{"value_type": "static", "value": "v"}]}
#: A sibling leg that is valid on its own, so a probe has exactly one offending consumer.
_OK_LEG = {"steps": [_GET, _DPP], "terminal": _STOP}


def _doc(*steps):
    return {"version": "1", "body": {"kind": "sequence", "steps": list(steps)}}


def _diagnostics(payload):
    try:
        compile_process_ir_v1(parse_process_ir_v1(payload), _symbols())
    except ProcessIRCompileError as exc:
        return tuple((item.code, item.path) for item in exc.diagnostics)
    return ()


_REFUSED = [
    pytest.param(
        _doc({"kind": "branch", "legs": [{"steps": [_MAP], "terminal": _STOP}, _OK_LEG]}),
        "/body/steps/0/legs/0/steps/0/map_ref", id="map_in_call_free_leg"),
    pytest.param(
        _doc({"kind": "branch", "legs": [{"steps": [_MAP], "terminal": _PUT}, _OK_LEG]}),
        "/body/steps/0/legs/0/steps/0/map_ref", id="map_feeding_a_staging_cache_put"),
    pytest.param(
        _doc({"kind": "branch", "legs": [{"steps": [_DPP], "terminal": _PUT}, _OK_LEG]}),
        "/body/steps/0/legs/0/terminal/cache_ref", id="staging_cache_put_on_absent_stream"),
    pytest.param(
        _doc({"kind": "decision", "comparison": "equals",
              "left": {"value_type": "static", "static_value": "a"},
              "right": {"value_type": "static", "static_value": "a"},
              "true_arm": {"steps": [_MAP], "terminal": _STOP},
              "false_arm": {"steps": [], "terminal": _STOP}}),
        "/body/steps/0/true_arm/steps/0/map_ref", id="map_in_decision_true_arm"),
    pytest.param(
        _doc({"kind": "branch", "legs": [
            {"steps": [{"kind": "message", "text": "x"}, _MAP], "terminal": _STOP}, _OK_LEG]}),
        "/body/steps/0/legs/0/steps/1/map_ref", id="message_is_not_a_from_nothing_producer"),
]


@pytest.mark.parametrize("payload,pointer", _REFUSED)
def test_a_consumer_on_an_absent_stream_is_refused_at_its_pointer(payload, pointer):
    diagnostics = _diagnostics(payload)
    assert (_CARD, pointer) in diagnostics, diagnostics


_SATISFIED = [
    pytest.param(
        _doc({"kind": "branch", "legs": [
            {"steps": [_GET], "terminal": _PUT},
            {"steps": [_CGET, _MAP], "terminal": _STOP}]}),
        id="cache_read_supplies_documents"),
    pytest.param(
        _doc({"kind": "branch", "legs": [
            {"steps": [_GET, _MAP, _PATCH], "terminal": _STOP}, _OK_LEG]}),
        id="producing_call_before_a_bracketed_map"),
    pytest.param(
        _doc({"kind": "source", "connection_ref": "$ref:conn", "operation_ref": "$ref:op"},
             _MAP,
             {"kind": "target", "connection_ref": "$ref:tconn", "operation_ref": "$ref:top"},
             _STOP),
        id="legacy_source_supplies_documents"),
]


@pytest.mark.parametrize("payload", _SATISFIED)
def test_a_consumer_downstream_of_a_producer_is_not_refused_for_cardinality(payload):
    codes = {code for code, _path in _diagnostics(payload)}
    assert _CARD not in codes, _diagnostics(payload)


def test_the_branch_point_compiled_the_absent_stream_probes():
    """The 'before' half, read from the archived pre-baseline measurement."""
    probe = _ROOT / "docs/architecture/evidence/issue-184/predicates/probe_cbab28f.jsonl"
    rows = {json.loads(line)["form"]: json.loads(line) for line in probe.read_text().splitlines() if line}
    for form in ("absent_stream_maps_in_legs", "absent_stream_map_put"):
        assert rows[form].get("compile") == "ok+emit", rows[form]


def test_the_producer_set_is_the_served_all_document_stream_replacers():
    """ONE document-existence authority, pinned to the served contract in BOTH directions.

    The negative witness makes the exclusion meaningful: `message` and
    `data_process` ARE served as stream-replacing, so a pin on stream replacement
    alone would admit them as from-nothing producers. Their served grouping is
    per-document, and that is what keeps them out.
    """
    from boomi_mcp.authoring.process_ir_projection import process_ir_authoring_revision_payload

    nodes = {
        entry["subject"]: entry.get("document_semantics") or {}
        for entry in process_ir_authoring_revision_payload()["entries"]
        if entry.get("entry_type") == "node"
    }
    served = {
        kind for kind, docs in nodes.items()
        if docs.get("output_documents") == "stream_replacing"
        and docs.get("grouping") == "all_documents"
    }
    assert served, "the served contract published no all-document stream replacer — the pin would be vacuous"
    assert connector_resolution._STREAM_PRODUCING_KINDS == served, {
        "only_in_compiler": sorted(connector_resolution._STREAM_PRODUCING_KINDS - served),
        "only_in_served_contract": sorted(served - connector_resolution._STREAM_PRODUCING_KINDS),
    }
    assert connector_resolution._MAY_FOLLOW_NON_PRODUCER == frozenset({"stop"}) | served

    for per_document in ("message", "data_process"):
        assert nodes[per_document].get("output_documents") == "stream_replacing", nodes[per_document]
        assert nodes[per_document].get("grouping") != "all_documents", nodes[per_document]
        assert per_document not in connector_resolution._STREAM_PRODUCING_KINDS


def test_the_producer_set_is_load_bearing_for_the_refusal(monkeypatch):
    """Non-vacuity: with no from-nothing producer, the cache-read leg's map is refused."""
    payload = _doc({"kind": "branch", "legs": [
        {"steps": [_GET], "terminal": _PUT},
        {"steps": [_CGET, _MAP], "terminal": _STOP}]})
    assert _CARD not in {code for code, _path in _diagnostics(payload)}
    monkeypatch.setattr(connector_resolution, "_STREAM_PRODUCING_KINDS", frozenset())
    monkeypatch.setattr(connector_resolution, "_MAY_FOLLOW_NON_PRODUCER", frozenset({"stop"}))
    assert (_CARD, "/body/steps/0/legs/1/steps/1/map_ref") in _diagnostics(payload)
