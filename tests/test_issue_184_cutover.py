"""#184 A8: the mapped both-sides dynamic path has a canonical spelling that emits the
legacy renderer's `<shapes>` byte for byte.

`EVAL-155-02` found this composition unrepresentable on the canonical chain: the root
map-adjacency rule refused it at parse time. The legacy oracle was frozen BEFORE the
step-0 baseline (`tests/fixtures/golden_xml/dynamic_path_both_sides.xml`,
golden-000079, provenance in `tests/fixtures/process_ir/issue184/PROVENANCE.md`).
Every expected value below is read from that frozen file or from the issue and plan
text, never from the canonical compiler.

What does NOT discharge A8 (issue text): a green compile of the hoisted-writer or
no-map form; a target writer that reads the pre-map profile; expected bytes
regenerated from the new compiler.
"""

from __future__ import annotations

import sys
import xml.etree.ElementTree as ET
from pathlib import Path

import pytest

_ROOT = Path(__file__).resolve().parent.parent
for _p in (str(_ROOT / "src"), str(_ROOT / "tests")):
    if _p not in sys.path:
        sys.path.insert(0, _p)

import _wave_gate_golden_corpus as corpus  # noqa: E402

from boomi_mcp.compiler.process_ir.diagnostics import ProcessIRCompileError  # noqa: E402
from boomi_mcp.compiler.process_ir.emitter_registry import emit_process  # noqa: E402
from boomi_mcp.compiler.process_ir.pipeline import parse_and_compile_process_ir_v1  # noqa: E402
from boomi_mcp.errors import PROCESS_IR_SEMANTIC_PROFILE_MISMATCH  # noqa: E402

_ORACLE = _ROOT / "tests/fixtures/golden_xml/dynamic_path_both_sides.xml"
_CANONICAL_GOLDEN = _ROOT / "tests/fixtures/golden_xml/issue184_both_sides_dynamic_path.xml"
_CASE = "issue184:both_sides_dynamic_path"


def _shapes(text: str) -> str:
    return text[text.index("<shapes>"):text.index("</shapes>") + len("</shapes>")]


def test_the_canonical_render_emits_the_frozen_oracles_shapes_byte_for_byte():
    oracle = _shapes(_ORACLE.read_text(encoding="utf-8"))
    rendered = corpus.render_golden_case(_CASE, "process-xml-v1").decode("utf-8")
    assert _shapes(rendered) == oracle
    # ...and the committed canonical golden is that slice, wrapped, not a render.
    assert _CANONICAL_GOLDEN.read_text(encoding="utf-8") == '<process xmlns="">' + oracle + "</process>"


def _oracle_shapes():
    root = ET.fromstring(_ORACLE.read_text(encoding="utf-8"))
    return [shape for shape in root.iter("shape")]


def test_the_oracle_binds_a_path_on_both_connectors_with_the_target_writer_after_the_map():
    shapes = _oracle_shapes()
    kinds = [shape.get("shapetype") for shape in shapes]
    assert kinds == ["start", "documentproperties", "connectoraction", "map",
                     "documentproperties", "connectoraction", "stop"], kinds
    bound = [
        shape for shape in shapes
        if shape.get("shapetype") == "connectoraction"
        and shape.find("configuration/connectoraction/dynamicProperties/propertyvalue[@key='path']") is not None
    ]
    assert len(bound) == 2, [shape.get("name") for shape in shapes]
    assert kinds.index("map") < len(kinds) - 1 - kinds[::-1].index("documentproperties")


def test_the_map_transforms_the_read_profile_into_the_request_profile():
    table = corpus.issue184_symbols()
    by_ref = {symbol.ref: symbol for symbol in table.symbols}
    mapping = by_ref["$ref:MAP"]
    assert by_ref[mapping.input_profile_ref].component_id != by_ref[mapping.output_profile_ref].component_id
    assert by_ref["$ref:ROP"].output_profile_ref == mapping.input_profile_ref
    assert by_ref["$ref:OP"].input_profile_ref == mapping.output_profile_ref


def _writer(name, second):
    return {"kind": "set_ddp", "name": name, "source_values": [
        {"value_type": "static", "value": "/v1/prefix/"}, second]}


_SOURCE_SEGMENT = {"value_type": "dpp", "property_name": "seed_id", "default_value": ""}


def _target_segment(profile_ref):
    return {"value_type": "profile", "profile_ref": profile_ref, "profile_type": "profile.json",
            "element_id": "3", "element_name": "requestId (Root/Object/requestId)"}


def test_the_hoisted_writer_form_is_refused_on_the_empty_scheduled_entry():
    """Amendment 2 §6: a target writer hoisted before the GET reads a profile element off
    the empty No Data document, and is refused at that source (the hoisted form was
    expected to COMPILE before amendment 2, which is withdrawn)."""
    doc = {"version": "1", "body": {"kind": "sequence", "steps": [
        _writer("DDP_PATH_SOURCE", _SOURCE_SEGMENT),
        _writer("DDP_PATH_TARGET", _target_segment("$ref:PREQ")),
        {"kind": "connector_call", "operation_ref": "$ref:ROP", "path_binding": {"property_name": "DDP_PATH_SOURCE"}},
        {"kind": "map_ref", "map_ref": "$ref:MAP"},
        {"kind": "connector_call", "operation_ref": "$ref:OP",
         "path_binding": {"property_name": "DDP_PATH_TARGET", "request_profile_ref": "$ref:PREQ"}},
        {"kind": "stop"}]}}
    with pytest.raises(ProcessIRCompileError) as excinfo:
        parse_and_compile_process_ir_v1(doc, corpus.issue184_symbols())
    diagnostics = [(item.code, item.path) for item in excinfo.value.diagnostics]
    assert (PROCESS_IR_SEMANTIC_PROFILE_MISMATCH, "/body/steps/1/source_values/1/profile_ref") in diagnostics, diagnostics


def test_the_no_map_both_bound_form_compiles_but_is_not_the_oracle():
    """Binding both paths is not the composition A8 names: without the map the PATCH
    receives the GET's documents, so its writer reads the READ profile, and the shapes
    lack the map the oracle carries."""
    doc = {"version": "1", "body": {"kind": "sequence", "steps": [
        _writer("DDP_PATH_SOURCE", _SOURCE_SEGMENT),
        {"kind": "connector_call", "operation_ref": "$ref:ROP", "path_binding": {"property_name": "DDP_PATH_SOURCE"}},
        _writer("DDP_PATH_TARGET", _target_segment("$ref:PREAD")),
        {"kind": "connector_call", "operation_ref": "$ref:OP",
         "path_binding": {"property_name": "DDP_PATH_TARGET", "request_profile_ref": "$ref:PREAD"}},
        {"kind": "stop"}]}}
    symbols = corpus.issue184_symbols()
    _ir, _cfg, plan = parse_and_compile_process_ir_v1(doc, symbols)
    xml = emit_process(plan, symbols).process_xml
    rendered = xml if isinstance(xml, str) else xml.decode("utf-8")
    assert _shapes(rendered) != _shapes(_ORACLE.read_text(encoding="utf-8"))
    assert 'shapetype="map"' not in _shapes(rendered)


def test_a_target_writer_reading_the_pre_map_profile_is_refused():
    """The issue's excluded spelling: the target writer after the map must read the
    map's OUTPUT profile. Reading the GET's profile there contradicts the stream."""
    doc = {"version": "1", "body": {"kind": "sequence", "steps": [
        _writer("DDP_PATH_SOURCE", _SOURCE_SEGMENT),
        {"kind": "connector_call", "operation_ref": "$ref:ROP", "path_binding": {"property_name": "DDP_PATH_SOURCE"}},
        {"kind": "map_ref", "map_ref": "$ref:MAP"},
        _writer("DDP_PATH_TARGET", _target_segment("$ref:PREAD")),
        {"kind": "connector_call", "operation_ref": "$ref:OP",
         "path_binding": {"property_name": "DDP_PATH_TARGET", "request_profile_ref": "$ref:PREAD"}},
        {"kind": "stop"}]}}
    with pytest.raises(ProcessIRCompileError) as excinfo:
        parse_and_compile_process_ir_v1(doc, corpus.issue184_symbols())
    diagnostics = [(item.code, item.path) for item in excinfo.value.diagnostics]
    assert (PROCESS_IR_SEMANTIC_PROFILE_MISMATCH, "/body/steps/3/source_values/1/profile_ref") in diagnostics, diagnostics
