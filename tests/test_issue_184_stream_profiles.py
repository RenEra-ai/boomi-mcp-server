"""#184 A1/A3: a map, a cache write and a profile-valued property source are proved
against the profile of the stream that actually reaches them.

Before #184 a map's profiles were checked only when a connector call sat on BOTH
sides of it, and a body map without that bracket was refused outright. So two
correctly profiled maps in a row, a map staging into a cache, and a map ending a
leg were refused, while a map fed by a cache read was never compared with
anything (issue probe: the cache read cleared the producer binding).

The stream profile is carried per path by the lineage controller:
- a producing call hands on its declared output profile;
- a map hands on its target profile;
- a cache read hands on the single profile every write reaching it stored.
  If the writes disagree, or one is unknown, the profile is unknown.
- a Message or Data Process erases what was known.

Expected codes and pointers come from the issue text and the attested plans, never
from this implementation's output:
- the design plan's §2 A1 matrix;
- the Claude plan's D2 consumer table;
- amendment 2 §4: the empty-entry profile source at
  `/body/steps/0/source_values/1/profile_ref`.
"""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

_ROOT = Path(__file__).resolve().parent.parent
for _p in (str(_ROOT), str(_ROOT / "src")):
    if _p not in sys.path:
        sys.path.insert(0, _p)

from boomi_mcp.compiler.process_ir import connector_capabilities as CC  # noqa: E402
from boomi_mcp.compiler.process_ir.contracts import (  # noqa: E402
    ComponentSymbolV1,
    SymbolTableV1,
)
from boomi_mcp.compiler.process_ir.diagnostics import ProcessIRCompileError  # noqa: E402
from boomi_mcp.compiler.process_ir.pipeline import (  # noqa: E402
    compile_process_ir_v1,
    parse_and_compile_process_ir_v1,
)
from boomi_mcp.compiler.process_ir.semantic_validation import lineage  # noqa: E402
from boomi_mcp.compiler.process_ir.semantic_validation.contracts import (  # noqa: E402
    ExternalWriterContractV1,
    ProcessIRValidationCapabilitiesV1,
)
from boomi_mcp.errors import PROCESS_IR_SEMANTIC_PROFILE_MISMATCH  # noqa: E402
from boomi_mcp.models.process_ir import parse_process_ir_v1  # noqa: E402

_PROFILE = PROCESS_IR_SEMANTIC_PROFILE_MISMATCH


def _symbols():
    rest = CC.REST_FAMILY

    def sym(ref, cid, ctype, **kw):
        return ComponentSymbolV1(ref="$ref:" + ref, component_id=cid, component_type=ctype, **kw)

    return SymbolTableV1(symbols=(
        sym("RCONN", "RCONN", "connector-settings", connector_type=rest),
        sym("GET", "GETOP", "connector-action", connector_type=rest, action_type="GET",
            connection_ref="$ref:RCONN", output_profile_ref="$ref:P1"),
        sym("GET_UNDECLARED", "GETOP2", "connector-action", connector_type=rest, action_type="GET",
            connection_ref="$ref:RCONN"),
        sym("PATCH", "PATCHOP", "connector-action", connector_type=rest, action_type="PATCH",
            connection_ref="$ref:RCONN", input_profile_ref="$ref:P2"),
        sym("PATCH_UNDECLARED", "PATCHOP2", "connector-action", connector_type=rest,
            action_type="PATCH", connection_ref="$ref:RCONN"),
        sym("M12", "M12", "transform.map", input_profile_ref="$ref:P1", output_profile_ref="$ref:P2"),
        sym("M21", "M21", "transform.map", input_profile_ref="$ref:P2", output_profile_ref="$ref:P1"),
        sym("M11", "M11", "transform.map", input_profile_ref="$ref:P1", output_profile_ref="$ref:P1"),
        sym("M32", "M32", "transform.map", input_profile_ref="$ref:P3", output_profile_ref="$ref:P2"),
        sym("M1_", "M1_", "transform.map", input_profile_ref="$ref:P1"),
        # Two refs, one component: an alias of P1 is the same profile identity.
        sym("M12_ALIAS", "M12A", "transform.map", input_profile_ref="$ref:P1_ALIAS",
            output_profile_ref="$ref:P2"),
        sym("P1", "PROFILE-ONE", "profile.json"),
        sym("P1_ALIAS", "PROFILE-ONE", "profile.json"),
        sym("P2", "PROFILE-TWO", "profile.json"),
        sym("P3", "PROFILE-THREE", "profile.json"),
        sym("CACHE", "CACHE", "documentcache"),
        sym("CACHE_P1", "CACHEP1", "documentcache", cache_profile_ref="$ref:P1"),
        sym("CACHE_P2", "CACHEP2", "documentcache", cache_profile_ref="$ref:P2"),
        # #158 listener operation declaring a request profile
        sym("LISTEN", "LISTENOP", "connector-action", connector_type="wss", action_type="Listen",
            input_profile_ref="$ref:P1", input_document_type="singlejson"),
        # legacy source/target endpoints
        sym("conn", "LCONN", "connector-settings", connector_type=rest),
        sym("op", "LOP", "connector-action", connector_type=rest, action_type="GET"),
        sym("tconn", "LTCONN", "connector-settings", connector_type=rest),
        sym("top", "LTOP", "connector-action", connector_type=rest, action_type="PATCH"),
    ))


def _call(ref):
    return {"kind": "connector_call", "operation_ref": "$ref:" + ref}


def _map(ref):
    return {"kind": "map_ref", "map_ref": "$ref:" + ref}


def _put(ref="CACHE"):
    return {"kind": "cache_put", "cache_ref": "$ref:" + ref}


def _read(ref="CACHE", **kw):
    return {"kind": "cache_get", "cache_ref": "$ref:" + ref, **kw}


def _profile_writer(profile_ref):
    return {"kind": "set_ddp", "name": "X", "source_values": [
        {"value_type": "profile", "profile_ref": "$ref:" + profile_ref,
         "profile_type": "json", "element_id": "3", "element_name": "id"},
    ]}


_STOP = {"kind": "stop"}
_OK_LEG = {"steps": [_call("GET"), {"kind": "set_dpp", "name": "Z", "source_values": [
    {"value_type": "static", "value": "v"}]}], "terminal": _STOP}


def _leg(steps, terminal=None):
    return {"steps": list(steps), "terminal": terminal or _STOP}


def _branch(*legs):
    return {"version": "1", "body": {"kind": "sequence", "steps": [
        {"kind": "branch", "legs": list(legs)}]}}


#: A leg whose TERMINAL removes every document from `CACHE` (its only legal placement).
_REMOVAL_LEG = _leg([], {"kind": "cache_remove", "cache_ref": "$ref:CACHE"})


def _both_routes(payload, capabilities=None):
    """(code, path) pairs from BOTH compile entry points, asserted identical."""
    routes = []
    for compile_route in ("parse_and_compile", "model"):
        try:
            if compile_route == "parse_and_compile":
                parse_and_compile_process_ir_v1(payload, _symbols(), capabilities=capabilities)
            else:
                compile_process_ir_v1(parse_process_ir_v1(payload), _symbols(), capabilities=capabilities)
        except ProcessIRCompileError as exc:
            routes.append(tuple((item.code, item.path) for item in exc.diagnostics))
        else:
            routes.append(())
    assert routes[0] == routes[1], routes
    return routes[0]


_REFUSED = [
    pytest.param(
        _branch(_leg([_call("GET")], _put()), _leg([_read(), _map("M32")])),
        "/body/steps/0/legs/1/steps/1/map_ref", None, id="cache_origin_wrong_map"),
    pytest.param(
        _branch(_leg([_call("GET"), _map("M32"), _call("PATCH")]), _OK_LEG),
        "/body/steps/0/legs/0/steps/1/map_ref", None, id="first_map_input_mismatch"),
    pytest.param(
        _branch(_leg([_call("GET"), _map("M12"), _map("M12")]), _OK_LEG),
        "/body/steps/0/legs/0/steps/2/map_ref", None, id="between_maps_mismatch"),
    pytest.param(
        _branch(_leg([_call("GET"), _map("M11"), _call("PATCH")]), _OK_LEG),
        "/body/steps/0/legs/0/steps/1/map_ref", None, id="map_target_differs_from_call_input"),
    pytest.param(
        _branch(_leg([_call("GET"), _map("M12"), _call("PATCH_UNDECLARED")]), _OK_LEG),
        "/body/steps/0/legs/0/steps/1/map_ref", None, id="mapped_call_declares_no_input"),
    pytest.param(
        _branch(_leg([_call("GET"), _map("M12")], _put("CACHE_P1")), _OK_LEG),
        "/body/steps/0/legs/0/terminal/cache_ref", None, id="cache_declaration_differs_from_map_output"),
    pytest.param(
        _branch(_leg([_call("GET"), _profile_writer("P2")]), _OK_LEG),
        "/body/steps/0/legs/0/steps/1/source_values/0/profile_ref", None,
        id="profile_source_differs_from_known_stream"),
    pytest.param(
        _branch(_leg([_call("GET")], _put()), _leg([_call("GET"), _map("M12")], _put()),
                _leg([_read(), _map("M12")])),
        "/body/steps/0/legs/2/steps/1/map_ref", None, id="disagreeing_cache_writers_are_unknown"),
    pytest.param(
        _branch(_leg([_read(external_writer=True), _map("M12")]), _OK_LEG),
        "/body/steps/0/legs/0/steps/1/map_ref",
        ProcessIRValidationCapabilitiesV1(external_writers=(ExternalWriterContractV1(cache_ref="$ref:CACHE"),)),
        id="external_writer_content_proves_no_profile"),
    pytest.param(
        _branch(_leg([_call("GET"), {"kind": "message", "text": "m"}, _map("M12")]), _OK_LEG),
        "/body/steps/0/legs/0/steps/2/map_ref", None, id="message_erases_the_profile"),
    # #184 amendment 3 (measured): an all-document Remove from Cache hands on ZERO
    # documents, so it TERMINATES its own leg and a LATER leg reads the cache.
    pytest.param(
        _branch(_leg([_call("GET")], _put()), _REMOVAL_LEG, _leg([_read(), _map("M12")])),
        "/body/steps/0/legs/2/steps/1/map_ref", None, id="whole_cache_removal_erases_content"),
    pytest.param(
        _branch(_leg([_call("GET"), _map("M1_")]), _OK_LEG),
        "/body/steps/0/legs/0/steps/1/map_ref", None, id="map_without_a_target_profile"),
    pytest.param(
        _branch(_leg([_call("GET_UNDECLARED"), _map("M12")]), _OK_LEG),
        "/body/steps/0/legs/0/steps/1/map_ref", None, id="upstream_call_declares_no_output"),
]


@pytest.mark.parametrize("payload,pointer,capabilities", _REFUSED)
def test_a_consumer_the_reaching_stream_contradicts_is_refused_at_its_pointer(payload, pointer, capabilities):
    diagnostics = _both_routes(payload, capabilities)
    assert (_PROFILE, pointer) in diagnostics, diagnostics


_SATISFIED = [
    pytest.param(_branch(_leg([_call("GET"), _map("M12"), _call("PATCH")]), _OK_LEG),
                 id="bracketed_map"),
    pytest.param(_branch(_leg([_call("GET"), _map("M12"), _map("M21")]), _OK_LEG),
                 id="consecutive_matching_maps"),
    pytest.param(_branch(_leg([_call("GET"), _map("M12")]), _OK_LEG),
                 id="map_ending_a_leg"),
    pytest.param(_branch(_leg([_call("GET"), _map("M12")], _put("CACHE_P2")), _OK_LEG),
                 id="map_staging_into_a_declared_cache"),
    pytest.param(_branch(_leg([_call("GET")], _put()), _leg([_read(), _map("M12")])),
                 id="cache_origin_matching_map"),
    pytest.param(_branch(_leg([_call("GET"), _map("M12"), _profile_writer("P2"), _call("PATCH")]), _OK_LEG),
                 id="profile_source_after_its_map"),
    pytest.param(_branch(_leg([_call("GET"), _map("M12_ALIAS"), _call("PATCH")]), _OK_LEG),
                 id="aliased_profile_refs"),
    pytest.param(_branch(_leg([_call("GET"), _map("M12")], {
        "kind": "decision", "comparison": "equals",
        "left": {"value_type": "static", "static_value": "a"},
        "right": {"value_type": "static", "static_value": "a"},
        "true_arm": {"steps": [_call("PATCH")], "terminal": _STOP},
        "false_arm": {"steps": [], "terminal": _STOP}}), _OK_LEG),
        id="mapped_stream_enters_each_decision_arm"),
    pytest.param({"version": "1", "body": {"kind": "sequence", "steps": [
        {"kind": "source", "connection_ref": "$ref:conn", "operation_ref": "$ref:op"},
        _map("M32"), _map("M32"),
        {"kind": "target", "connection_ref": "$ref:tconn", "operation_ref": "$ref:top"},
        _STOP]}}, id="legacy_source_maps_stay_unchecked"),
]


@pytest.mark.parametrize("payload", _SATISFIED)
def test_a_consumer_the_reaching_stream_agrees_with_is_not_refused(payload):
    diagnostics = _both_routes(payload)
    assert _PROFILE not in {code for code, _path in diagnostics}, diagnostics


def test_a_mapped_stream_is_checked_again_on_each_decision_arm():
    """D7: each path checks its own first consumer, so one wrong arm is refused alone."""
    payload = _branch(_leg([_call("GET"), _map("M12")], {
        "kind": "decision", "comparison": "equals",
        "left": {"value_type": "static", "static_value": "a"},
        "right": {"value_type": "static", "static_value": "a"},
        "true_arm": {"steps": [_call("PATCH")], "terminal": _STOP},
        "false_arm": {"steps": [_map("M12")], "terminal": _STOP}}), _OK_LEG)
    diagnostics = _both_routes(payload)
    assert (_PROFILE, "/body/steps/0/legs/0/terminal/false_arm/steps/0/map_ref") in diagnostics, diagnostics
    assert (_PROFILE, "/body/steps/0/legs/0/steps/1/map_ref") not in diagnostics, diagnostics


def test_several_mismatching_sources_on_one_step_are_each_reported():
    """The dedup key carries the sub-path: one step, two wrong sources, two findings."""
    writer = {"kind": "set_ddp", "name": "X", "source_values": [
        {"value_type": "profile", "profile_ref": "$ref:P2", "profile_type": "json",
         "element_id": "3", "element_name": "id"},
        {"value_type": "profile", "profile_ref": "$ref:P3", "profile_type": "json",
         "element_id": "4", "element_name": "other"},
    ]}
    diagnostics = _both_routes(_branch(_leg([_call("GET"), writer]), _OK_LEG))
    for position in (0, 1):
        pointer = "/body/steps/0/legs/0/steps/1/source_values/{0}/profile_ref".format(position)
        assert (_PROFILE, pointer) in diagnostics, diagnostics


def test_the_removal_leg_is_what_erases_the_cache_content():
    """Non-vacuity of `whole_cache_removal_erases_content`: the same write leg and
    read leg WITHOUT the terminal-removal leg between them prove the map, so the
    refusal is the removal's erasure of the stored content and nothing else."""
    pointer = "/body/steps/0/legs/2/steps/1/map_ref"
    with_removal = _both_routes(
        _branch(_leg([_call("GET")], _put()), _REMOVAL_LEG, _leg([_read(), _map("M12")])))
    assert (_PROFILE, pointer) in with_removal, with_removal
    without_removal = _both_routes(_branch(_leg([_call("GET")], _put()), _leg([_read(), _map("M12")])))
    assert _PROFILE not in {code for code, _path in without_removal}, without_removal


def test_the_cache_content_fact_is_load_bearing(monkeypatch):
    """Non-vacuity, both directions: with no content tracked, a correctly profiled
    cache-origin map is refused; with it, only the wrong map is."""
    matching = _branch(_leg([_call("GET")], _put()), _leg([_read(), _map("M12")]))
    assert _PROFILE not in {code for code, _path in _both_routes(matching)}
    monkeypatch.setattr(lineage._State, "content_of", lambda self, cache_ref: frozenset())
    assert (_PROFILE, "/body/steps/0/legs/1/steps/1/map_ref") in _both_routes(matching)


def _probe_rows():
    import json

    probe = _ROOT / "docs/architecture/evidence/issue-184/predicates/probe_cbab28f.jsonl"
    return {
        row["form"]: row
        for row in (json.loads(line) for line in probe.read_text().splitlines() if line)
    }


def test_the_branch_point_compiled_the_wrong_cache_origin_map():
    """The 'before' half: at cbab28f a cache read cleared the producer binding, so a
    wrong map in leg 2 compiled (A1 row `Cache-origin wrong map profile`)."""
    rows = _probe_rows()
    assert rows["cache_origin_map_wrong_profile"].get("compile") == "ok+emit", rows["cache_origin_map_wrong_profile"]


@pytest.mark.parametrize("form", ["leg_call_map_map_put", "leg_call_map_put", "leg_call_map_call_map_stop"])
def test_the_branch_point_refused_correctly_profiled_leg_chains(form):
    """The 'before' half of the admitted chains: at cbab28f the bracketing rule refused
    each of them at a map, whatever its profiles."""
    row = _probe_rows()[form]
    assert row.get("compile") != "ok+emit", row
    assert [item[0] for item in row["compile"]] == [_PROFILE], row


def test_a_profile_source_on_the_empty_scheduled_entry_is_refused():
    """Amendment 2 §4: golden-000072's first writer reads a profile element before
    anything produces a document. Under a scheduled start the entry document is the
    empty No Data document, so the read addresses nothing. Pointer and code are the
    amendment's."""
    import json

    from _wave_gate_golden_corpus import issue155_symbols

    payload = json.loads(
        (_ROOT / "tests/fixtures/process_ir/issue155/source_dynamic_path_profile.json").read_text()
    )
    diagnostics = []
    try:
        parse_and_compile_process_ir_v1(payload, issue155_symbols())
    except ProcessIRCompileError as exc:
        diagnostics = [(item.code, item.path, item.phase) for item in exc.diagnostics]
    assert (_PROFILE, "/body/steps/0/source_values/1/profile_ref", "semantic_lowering") in diagnostics, diagnostics


# ---------------------------------------------------------------------------
# The named legacy exemption is the dialect's, and is scoped per path
# ---------------------------------------------------------------------------

_TARGET = {"kind": "target", "connection_ref": "$ref:tconn", "operation_ref": "$ref:top"}


def test_the_legacy_listener_target_spine_stays_unchecked():
    """#158 shipped `[listener, map_ref, target, stop]` with the map unchecked (goldens
    000022/040/041/078). A legacy `target` ends every document the map handles, so the
    dialect's exemption applies even with a listener that declares a request profile
    the map's source contradicts."""
    payload = {"version": "1", "body": {"kind": "sequence", "steps": [
        {"kind": "listener", "operation_ref": "$ref:LISTEN"}, _map("M32"), _TARGET, _STOP]}}
    diagnostics = _both_routes(payload)
    assert _PROFILE not in {code for code, _path in diagnostics}, diagnostics


def test_a_call_after_a_legacy_source_restores_the_check():
    """A first-class call's output is what reaches the next map, so a legacy source
    further upstream does not exempt it."""
    payload = {"version": "1", "body": {"kind": "sequence", "steps": [
        {"kind": "source", "connection_ref": "$ref:conn", "operation_ref": "$ref:op"},
        {"kind": "branch", "legs": [
            _leg([_call("GET"), _map("M32"), _call("PATCH")]),
            _leg([{"kind": "message", "text": "m"}])]}]}}
    diagnostics = _both_routes(payload)
    assert (_PROFILE, "/body/steps/1/legs/0/steps/1/map_ref") in diagnostics, diagnostics


def test_a_call_leg_beside_a_legacy_target_leg_is_still_checked():
    """The model admits a call leg and a target leg side by side under a control-only
    root. The target leg's exemption must not leak into its sibling."""
    payload = _branch(_leg([_call("GET"), _map("M32")]), _leg([], _TARGET))
    diagnostics = _both_routes(payload)
    assert (_PROFILE, "/body/steps/0/legs/0/steps/1/map_ref") in diagnostics, diagnostics
