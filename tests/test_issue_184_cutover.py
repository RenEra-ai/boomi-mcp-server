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


# ---------------------------------------------------------------------------
# #159 caller census (L9) — EVAL-155-02 / EVAL-155-02a. TRANSITIONAL witnesses: they
# drive the retained legacy oracle input (`legacy_both_sides_config.json`, golden-000079's
# frozen input) through the two serving routes that can hand a hand-authored
# `database_to_api_sync` config to the legacy renderer, so the census row for each route
# is executable rather than asserted. When #159 migrates or retires a route, rewrite the
# matching body in place (keep the node id); #160 deletes them with the legacy renderer.
# ---------------------------------------------------------------------------

_ORACLE_INPUT = _ROOT / "tests/fixtures/process_ir/issue184/legacy_both_sides_config.json"


def _shape_tree(text: str):
    """The `<shapes>` element as a comparable tree, ignoring only the x/y layout
    attributes (the safe-edit route arranges the canvas, #188)."""
    def canon(element):
        return (element.tag,
                tuple(sorted((k, v) for k, v in element.attrib.items() if k not in ("x", "y"))),
                (element.text or "").strip(), tuple(canon(child) for child in element))
    return canon(ET.fromstring(_shapes(text)))


def test_the_build_integration_plan_refuses_the_retained_oracle_input():
    """The create/reuse and literal-reference spellings of the legacy both-bound config
    are refused on the build_integration route.

    #159 S3 (cutover decision 1): the refusal is now admission's — a flat
    `database_to_api_sync` body is refused before normalization, any validator, preview,
    lookup or write, with the registered unit-required code at the body's selector
    pointer, and no plan steps are produced. (Before the cutover the step-level builder
    validator refused it with PROCESS_CONNECTOR_BINDING_INVALID at source.connector_type;
    that validator is no longer reached.) Apply re-runs this plan, so nothing reaches a
    renderer."""
    import json
    from unittest.mock import MagicMock, patch

    from boomi_mcp.authoring.process_admission import unit_required_envelope
    from boomi_mcp.categories import integration_builder as ib
    from boomi_mcp.errors import PROCESS_COMPONENT_AUTHORING_UNIT_REQUIRED

    literal = json.loads(_ORACLE_INPUT.read_text(encoding="utf-8"))
    referenced = json.loads(_ORACLE_INPUT.read_text(encoding="utf-8"))
    referenced["source"].update(connection_id="$ref:rconn", operation_id="$ref:rop")
    referenced["target"].update(connection_id="$ref:conn", operation_id="$ref:op")
    referenced["transform"]["map_ref"] = "$ref:map"
    # In-spec dependencies on the update path (existing ids) — would-write siblings
    # AHEAD of the refused process.
    dependencies = [
        {"key": key, "type": kind, "action": "update", "name": key,
         "component_id": "00000000-0000-0000-0000-00000000000%d" % index, "config": config}
        for index, (key, kind, config) in enumerate((
            ("rconn", "connector-settings", {"connector_type": "rest", "base_url": "http://x"}),
            ("rop", "connector-action", {"connector_type": "rest", "method": "GET"}),
            ("conn", "connector-settings", {"connector_type": "rest", "base_url": "http://x"}),
            ("op", "connector-action", {"connector_type": "rest", "method": "PATCH"}),
            ("map", "transform.map", {})))]
    for process_config, others in ((literal, []), (referenced, dependencies)):
        spec = {"conflict_policy": "reuse", "integration_spec": {"version": "1.0", "name": "l9", "components": [
            *others, {"key": "proc", "type": "process", "action": "create", "name": "P",
                      "config": process_config, "depends_on": [other["key"] for other in others]}]}}
        with patch.object(ib, "paginate_metadata", return_value=[]) as lookups:
            plan = ib._build_plan(MagicMock(), spec)
        assert lookups.call_count == 0
        assert plan["_success"] is False, plan
        assert plan["error_code"] == PROCESS_COMPONENT_AUTHORING_UNIT_REQUIRED, plan
        assert plan["field"] == "integration_spec.components.%d.config.process_kind" % len(others)
        assert set(plan) == set(unit_required_envelope("x"))
        assert "steps" not in plan
        assert "PROCESS_CONNECTOR_BINDING_INVALID" not in json.dumps(plan)


def test_the_safe_edit_route_builds_the_retained_oracle_input_into_the_oracles_shapes():
    """The census's former working caller: prepare_component_edit / apply_component_edit
    handed a structured process body straight to the legacy builder, so the retained
    oracle input was built and PUSHED with golden-000079's shapes.

    #159 S3 (cutover decision 2): that legacy body arm is refused — prepare answers with
    the unit-required code, pointing at `config` and at `patch.authoring_request`, and
    nothing is written; apply of the same body cannot write either. The transfer this
    route takes is the canonical arm, witnessed by
    `test_the_public_safe_edit_applies_the_canonical_mapped_both_sides_update` below
    (golden-000081's canonical spelling, the oracle's shapes). Offline: only the live
    fetch and the SDK client are stand-ins."""
    import json
    from unittest.mock import MagicMock, patch

    from boomi_mcp.categories.components import safe_edit_component as safe_edit
    from boomi_mcp.errors import PROCESS_COMPONENT_AUTHORING_UNIT_REQUIRED

    current = {"type": "process", "version": 3, "xml": (
        _ROOT / "tests/fixtures/golden_xml/dynamic_path_target_profile.xml").read_text(encoding="utf-8")}
    body = {"config": {**json.loads(_ORACLE_INPUT.read_text(encoding="utf-8")), "name": "Edited"}}
    client = MagicMock()
    with patch.object(safe_edit, "component_get_xml", return_value=current):
        prepared = safe_edit.prepare_component_edit_action(client, "prof", "PROC-ID", body)
        assert prepared["_success"] is False, prepared
        assert prepared["error_code"] == PROCESS_COMPONENT_AUTHORING_UNIT_REQUIRED, prepared
        assert str(prepared.get("field") or "").startswith("config")
        assert "patch.authoring_request" in prepared["hint"]
        assert prepared.get("confirmation_token") is None
        assert prepared["boomi_mutation"] is False
        blob = json.dumps(prepared)
        assert "Edited" not in blob and "DDP_PATH" not in blob
        # No token can be prepared for this body, so apply is refused at its token
        # check here — this leg proves only that nothing is written, not which
        # refusal apply itself would give the body (lane A's safe-edit matrix owns that).
        applied = safe_edit.apply_component_edit_action(
            client, "prof", "PROC-ID", body,
            confirmation_token="not-a-prepared-token", confirm_apply=True)
    assert applied["_success"] is False, applied
    assert applied.get("boomi_mutation") is not True
    client.component.update_component_raw.assert_not_called()
    client.component.update_component.assert_not_called()


def test_the_public_safe_edit_applies_the_canonical_mapped_both_sides_update(monkeypatch):
    """EVAL-155-02 / -02a: the one working mapped both-bound caller (safe edit), on the
    canonical arm. The #184 canonical fixture (golden-000081's input) is submitted
    through the REGISTERED prepare/apply tools with every reference BOUND to a
    proven-operable capture (`replay_identity/cap159-s3-bound-profiles/`, executed green:
    a GET storing `responseProfile`, a PATCH storing `requestProfile`, and the map between
    them; provenance in `tests/test_component_safe_edit.py`, "#159 S3 (lane S3b)"). Only the
    account transport is faked, and it serves the captured bytes verbatim.

    The map's profile continuity is proved from the STORED bindings (#159 S3 lane S3b:
    `authoring.stored_profile_bindings`). Expected values come from the frozen legacy
    oracle golden-000079 only; the comparison substitutes the documented captured->oracle
    ids (`ORACLE_IDS_BY_CAPTURED_ID`) and drops layout coordinates, nothing else."""
    from test_component_safe_edit import (
        CANONICAL_PROCESS_ID,
        CAPTURED_GET_RESPONSE_PROFILE,
        CAPTURED_MAP,
        CAPTURED_PATCH_REQUEST_PROFILE,
        CanonicalAccount,
        assert_live_process_preserved,
        captured_bound_profiles_patch,
        captured_bound_profiles_store,
        shapes_without_layout,
        with_oracle_ids,
    )
    from test_component_safe_edit_wrapper import public_safe_edit

    account = CanonicalAccount(captured_bound_profiles_store())
    prepared, pushes_after_prepare, applied = public_safe_edit(
        monkeypatch, account, captured_bound_profiles_patch(), CANONICAL_PROCESS_ID
    )
    assert prepared["_success"] is True, prepared
    assert pushes_after_prepare == []
    assert applied["_success"] is True, applied
    # Exactly one write, of the named process — no supporting component is written.
    assert [cid for cid, _ in account.component.pushed] == [CANONICAL_PROCESS_ID]
    # The stored map and both of its linked profiles were read from the account.
    assert {CAPTURED_MAP, CAPTURED_GET_RESPONSE_PROFILE, CAPTURED_PATCH_REQUEST_PROFILE} <= set(
        account.component.reads)
    pushed = account.component.pushed[0][1]
    # The oracle's <shapes> tree, ignoring only layout coordinates and the documented ids.
    assert shapes_without_layout(with_oracle_ids(pushed)) == shapes_without_layout(
        _ORACLE.read_text(encoding="utf-8"))
    # The target writer after the map reads the map's OUTPUT profile.
    root = ET.fromstring(pushed)
    shapes = [shape for shape in root.iter("shape")]
    kinds = [shape.get("shapetype") for shape in shapes]
    target_writer = shapes[len(kinds) - 1 - kinds[::-1].index("documentproperties")]
    assert kinds.index("map") < shapes.index(target_writer)
    assert {element.get("profileId") for element in target_writer.iter("profileelement")} == {
        CAPTURED_PATCH_REQUEST_PROFILE
    }
    assert_live_process_preserved(pushed)


def test_the_public_typed_apply_writes_the_captured_bound_profile_update_once(monkeypatch):
    """#159 S3 (lane S3b apply hunk): the same captured all-bound request, through the
    REGISTERED `build_integration` compile then confirmed apply. Apply reads the stored
    profile links of the map and both REST operations it does not write through its own
    phase budget, so the map's profile continuity is judged on fresh stored bytes and the
    apply writes exactly one component — the named process — and nothing else.

    Same-case control (measured at b998588, before the hunk): this apply refused
    `PROCESS_IR_SEMANTIC_PROFILE_MISMATCH` at `/body/steps/2/map_ref` with zero writes,
    while compile of the same request succeeded."""
    import json

    import server
    from test_component_safe_edit import (
        CANONICAL_PROCESS_ID,
        CAPTURED_GET_RESPONSE_PROFILE,
        CAPTURED_MAP,
        CAPTURED_PATCH_REQUEST_PROFILE,
        CanonicalAccount,
        captured_bound_profiles_patch,
        captured_bound_profiles_store,
    )
    from test_component_safe_edit_wrapper import _payload, _run_async

    account = CanonicalAccount(captured_bound_profiles_store())
    monkeypatch.setattr(server, "get_current_user", lambda: "tester")
    monkeypatch.setattr(server, "get_secret", lambda *a, **k: {
        "account_id": account.account_id, "username": "u", "password": "pw"})
    monkeypatch.setattr(server, "Boomi", lambda **kw: account)

    def call(action, request, **extra):
        return _payload(_run_async(server.mcp.call_tool("build_integration", {
            "profile": "p", "action": action,
            "config": json.dumps(dict({"authoring_request": request}, **extra))})))

    request = captured_bound_profiles_patch()["authoring_request"]
    compiled = call("compile", request)
    assert compiled["_success"] is True, compiled
    assert account.component.pushed == []
    binding = compiled["authoring_result"]["revision_binding"]
    applied = call("apply", dict(
        request, expected_compile_hash=binding["compile_hash"],
        expected_capability_revision=binding["capability_revision"]), dry_run=False)
    assert applied["_success"] is True, applied
    assert [cid for cid, _ in account.component.pushed] == [CANONICAL_PROCESS_ID]
    assert {CAPTURED_MAP, CAPTURED_GET_RESPONSE_PROFILE, CAPTURED_PATCH_REQUEST_PROFILE} <= set(
        account.component.reads)
