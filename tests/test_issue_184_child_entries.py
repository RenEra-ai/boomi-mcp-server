"""#184 amendment 3 §8: child entry contracts, derived without declarations and discharged per call.

Expected codes, pointers and behaviour come from amendment 1's diagnostic policy as
amended by amendment 3 §8 (ledger rows C14, C19-C21) and from the archived captures,
never from this implementation's output:

- `cap184-passthrough-group`: a waited Data Passthrough child receives the arriving
  documents as one group, so what it consumes is a requirement of the caller's stream.
- `cap184-passthrough-wait-true`: wait=false is OPEN, so it stays refused at `/wait`.
- `cap184-nodata-per-document`: a No Data child runs once per arriving document, on an
  empty document of its own.
- `cap184-passthrough-ddp-handoff`: a passthrough child's bound path composes from its
  caller's writer; a No Data child receives no parent document property.
- `cap184-prefix-predecessors`: prefix rows admit a passthrough child with wait=true only.
- `cap184-shared-cache`, `cap184-dpp-both-ways`: execution state crosses both forms.
- `cap184-passthrough-standalone`: run directly, a passthrough process runs as No Data.
"""

from __future__ import annotations

import sys
from pathlib import Path
from types import SimpleNamespace

import pytest

_HERE = Path(__file__).resolve().parent
_ROOT = _HERE.parent
for _p in (str(_ROOT), str(_ROOT / "src"), str(_HERE)):
    if _p not in sys.path:
        sys.path.insert(0, _p)

from boomi_mcp.authoring.process_ir_effects import (  # noqa: E402
    resolve_process_ir_effect_declarations,
)
from boomi_mcp.compiler.process_ir import connector_capabilities as CC  # noqa: E402
from boomi_mcp.compiler.process_ir.contracts import (  # noqa: E402
    ComponentSymbolV1,
    SymbolTableV1,
)
from boomi_mcp.compiler.process_ir.diagnostics import ProcessIRCompileError  # noqa: E402
from boomi_mcp.compiler.process_ir.pipeline import compile_process_ir_v1  # noqa: E402
from boomi_mcp.compiler.process_ir.semantic_validation import lineage  # noqa: E402
from boomi_mcp.compiler.process_ir.semantic_validation.contracts import (  # noqa: E402
    DEFAULT_VALIDATION_CAPABILITIES,
    ChildEntryContractV1,
    ProcessIRValidationCapabilitiesV1,
)
from boomi_mcp.compiler.process_ir.semantic_validation.pipeline import (  # noqa: E402
    validate_process_ir,
)
from boomi_mcp.errors import (  # noqa: E402
    PROCESS_IR_CAPABILITY_EFFECT_CONTRACT_INVALID,
    PROCESS_IR_CAPABILITY_ENTRY_CONTEXT_UNSUPPORTED,
    PROCESS_IR_CAPABILITY_PROCESS_CALL_PLACEMENT_UNSUPPORTED,
    PROCESS_IR_CAPABILITY_PROCESS_CALL_REPEATED_RUN_UNSTABLE,
    PROCESS_IR_SEMANTIC_DYNAMIC_PATH_DDP_NOT_ESTABLISHED,
    PROCESS_IR_SEMANTIC_DYNAMIC_PATH_NO_DYNAMIC_SEGMENT,
    PROCESS_IR_SEMANTIC_LINEAGE_PROPERTY_READ_BEFORE_WRITE,
    PROCESS_IR_SEMANTIC_PROFILE_MISMATCH,
)
from boomi_mcp.models import process_ir as model  # noqa: E402
from boomi_mcp.models.process_ir import parse_process_ir_v1  # noqa: E402

# The #158 deployment suite's two autouse fixtures, imported so they apply to this
# module too: the build registry is restored after each test, and the metadata pager is
# stubbed — unstubbed over a MagicMock client it never terminates.
from test_issue_158_listener_deployment import (  # noqa: E402,F401
    _no_live_metadata_queries,
    _registry_restored,
)

_PLACEMENT = PROCESS_IR_CAPABILITY_PROCESS_CALL_PLACEMENT_UNSUPPORTED
_PROCESS_KEYS = (
    "PARENT", "OTHER_PARENT", "CHILD", "CHILD_P1", "MID", "BOUND", "BOUND_SPLIT",
    "NODATA", "NEEDS_K", "MUTATES_K", "READS_X", "LOOP_A", "LOOP_B", "EXTERNAL",
    "CACHE_CHILD", "CACHE_CHILD_P1", "MIDP", "EXTCHILD", "WRITER", "BOUND_XY", "ENRICH", "HIDES",
)


def _symbols():
    rest = CC.REST_FAMILY

    def sym(ref, cid, ctype, **kw):
        return ComponentSymbolV1(ref="$ref:" + ref, component_id=cid, component_type=ctype, **kw)

    return SymbolTableV1(symbols=(
        sym("RCONN", "RCONN", "connector-settings", connector_type=rest),
        sym("GET", "GETOP", "connector-action", connector_type=rest, action_type="GET",
            connection_ref="$ref:RCONN"),
        sym("GETP1", "GETP1OP", "connector-action", connector_type=rest, action_type="GET",
            connection_ref="$ref:RCONN", output_profile_ref="$ref:P1"),
        sym("PATCH", "PATCHOP", "connector-action", connector_type=rest, action_type="PATCH",
            connection_ref="$ref:RCONN", input_profile_ref="$ref:P2"),
        # A call that consumes the documents and declares no input profile (D2).
        sym("PATCH_UNDECLARED", "PATCHOP2", "connector-action", connector_type=rest,
            action_type="PATCH", connection_ref="$ref:RCONN"),
        sym("M12", "M12", "transform.map", input_profile_ref="$ref:P1", output_profile_ref="$ref:P2"),
        sym("M22", "M22", "transform.map", input_profile_ref="$ref:P2", output_profile_ref="$ref:P2"),
        sym("P1", "PROFILE-ONE", "profile.json"),
        sym("P2", "PROFILE-TWO", "profile.json"),
        sym("CACHE", "CACHE", "documentcache"),
        sym("CACHE_ALIAS", "CACHE", "documentcache"),
        sym("CACHE2", "CACHE2", "documentcache"),
        sym("CHILD_ALIAS", "CHILD-PROC", "process"),
    ) + tuple(sym(key, key + "-PROC", "process") for key in _PROCESS_KEYS))


_ENTRY = {"kind": "passthrough", "label": "Receive"}
_STOP = {"kind": "stop"}
_MSG = {"kind": "message", "text": "m"}
_MAP = {"kind": "map_ref", "map_ref": "$ref:M12"}
_PATCH = {"kind": "connector_call", "operation_ref": "$ref:PATCH"}
_SET_K = {"kind": "set_dpp", "name": "K", "source_values": [{"value_type": "static", "value": "v"}]}
_SET_Z = {"kind": "set_dpp", "name": "Z", "source_values": [{"value_type": "static", "value": "v"}]}
_STATIC_X = {"kind": "set_ddp", "name": "X", "source_values": [{"value_type": "static", "value": "/c/1"}]}
_DYNAMIC_X = {"kind": "set_ddp", "name": "X", "source_values": [
    {"value_type": "static", "value": "/clients/"},
    {"value_type": "dpp", "property_name": "key", "default_value": ""},
]}
_BOUND_GET = {"kind": "connector_call", "operation_ref": "$ref:GET", "path_binding": {"property_name": "X"}}
_SPLIT = {"kind": "data_process", "steps": [{
    "operation": "split_documents", "profile_type": "json", "profile_ref": "$ref:P1",
    "link_element_key": "1", "link_element_name": "root"}]}


def _doc(*steps):
    return {"version": "1", "body": {"kind": "sequence", "steps": list(steps)}}


def _call(key, **extra):
    return dict({"kind": "process_call", "process_ref": "$ref:" + key}, **extra)


def _branch(prefix, terminal):
    """A Branch whose FIRST leg ends in ``terminal`` after ``prefix``; the second stops."""
    return {"kind": "branch", "legs": [
        {"steps": list(prefix), "terminal": terminal},
        {"steps": [_MSG], "terminal": _STOP},
    ]}


def _parent(prefix, terminal):
    """A Data Passthrough parent: its caller's documents reach the Branch."""
    return _doc(_ENTRY, _branch(prefix, terminal))


def _decision(true_terminal):
    return {"kind": "decision", "comparison": "equals",
            "left": {"value_type": "static", "static_value": "a"},
            "right": {"value_type": "static", "static_value": "a"},
            "true_arm": {"steps": [], "terminal": true_terminal},
            "false_arm": {"steps": [], "terminal": _STOP}}


def _reads_k(*extra_steps):
    return _doc(_decision_steps([
        {"kind": "set_dpp", "name": "OUT", "source_values": [{"value_type": "dpp", "property_name": "K"}]},
        *extra_steps,
    ]))


def _decision_steps(steps):
    decision = _decision(_STOP)
    decision["true_arm"] = {"steps": list(steps), "terminal": _STOP}
    return decision


_CHILD = _doc(_ENTRY, _PATCH, _STOP)                 # consumes P2 off its caller's documents
_CHILD_P1 = _doc(_ENTRY, _MAP, _PATCH, _STOP)        # consumes P1
_BOUND = _doc(_ENTRY, _BOUND_GET, _STOP)             # a request path only its caller composes
_BOUND_SPLIT = _doc(_ENTRY, _SPLIT, _BOUND_GET, _STOP)
_NODATA = _doc(_decision_steps([_MSG]))          # a scheduled (No Data) root
_NEEDS_K = _reads_k()
_MUTATES_K = _reads_k(_SET_K)
_READS_X = _doc(_decision_steps([
    {"kind": "set_dpp", "name": "Y", "source_values": [{"value_type": "ddp", "property_name": "X"}]}]))
_P2_PREFIX = [_MAP]                                  # the caller's documents become P2


def _resolve(roots):
    parsed = [(key, parse_process_ir_v1(doc)) for key, doc in roots]
    resolution = resolve_process_ir_effect_declarations(
        parsed, None, _symbols(), [], child_roots={"$ref:" + key: ir for key, ir in parsed})
    assert resolution.ok, resolution.findings
    return dict(parsed), resolution


def _capabilities(resolution, key):
    return resolution.capabilities_by_root[key] or DEFAULT_VALIDATION_CAPABILITIES


def _errors(roots, key):
    irs, resolution = _resolve(roots)
    report = validate_process_ir(irs[key], _symbols(), capabilities=_capabilities(resolution, key))
    return [(item.code, item.path) for item in report.errors]


def _compile_errors(roots, key):
    irs, resolution = _resolve(roots)
    try:
        compile_process_ir_v1(irs[key], _symbols(), capabilities=_capabilities(resolution, key))
    except ProcessIRCompileError as exc:
        return [(item.code, item.path) for item in exc.diagnostics]
    return []


_LEG = "/body/steps/1/legs/0/terminal"


# ---------------------------------------------------------------------------
# derivation
# ---------------------------------------------------------------------------


def test_a_contract_is_derived_without_any_declaration():
    _irs, resolution = _resolve([("PARENT", _parent(_P2_PREFIX, _call("CHILD"))), ("CHILD", _CHILD)])
    row = resolution.capabilities_by_root["PARENT"].child_entry_contract("$ref:CHILD")
    assert (row.entry_form, row.document_requirements) == ("passthrough", ("$ref:P2",)), row
    own = resolution.capabilities_by_root["CHILD"].entry_contract
    assert own is not None and own.document_requirements == ("$ref:P2",), own


def test_a_request_with_no_call_and_no_passthrough_root_keeps_no_context():
    _irs, resolution = _resolve([("NODATA", _NODATA)])
    assert resolution.capabilities_by_root == {"NODATA": None}


def test_an_alias_of_the_child_binds_the_same_contract():
    roots = [("PARENT", _parent(_P2_PREFIX, {"kind": "process_call", "process_ref": "$ref:CHILD_ALIAS"})),
             ("CHILD", _CHILD)]
    _irs, resolution = _resolve(roots)
    assert resolution.capabilities_by_root["PARENT"].child_entry_contract("$ref:CHILD_ALIAS") is not None
    assert _errors(roots, "PARENT") == []


def test_a_grandchild_contract_reaches_its_parents_derivation():
    """MID calls the P2-consuming child after its own map, and requires P1 of its caller."""
    mid = _parent(_P2_PREFIX, _call("CHILD"))
    roots = [("PARENT", _parent(_P2_PREFIX, _call("MID"))), ("MID", mid), ("CHILD", _CHILD)]
    _irs, resolution = _resolve(roots)
    assert resolution.capabilities_by_root["PARENT"].child_entry_contract("$ref:MID").document_requirements == ("$ref:P1",)
    assert _errors(roots, "MID") == []
    assert (PROCESS_IR_SEMANTIC_PROFILE_MISMATCH, _LEG + "/process_ref") in _errors(roots, "PARENT")


def test_a_call_cycle_has_no_derivable_contract():
    """THE NAME IS THE CLAIM'S HISTORY, AND THE NODE ID THE WAVE GATE PINS: a cycle member's
    contract is no longer empty, but this node stays under its original name because
    `tests/fixtures/wave_gate/test_nodes.jsonl` requires it and that manifest is append-only
    (B21A-R6-V6-03).

    What holds now: a cycle member cannot be derived IN ORDER, so it is seeded with the
    contract an underivable call gets and then derived against those seeds to a fixed point
    (B21A-R5-CYC-01, correction batch 21a rounds 6 and 7). Its own entry form and obligations
    are real — they come off its own CFG, and what one member owes reaches the member that
    calls it — while everything the seeding hid stays unknown, so a supplied-but-cyclic
    ProcessIR is never weaker evidence than an absent one."""
    roots = [("LOOP_A", _parent(_P2_PREFIX, _call("LOOP_B"))), ("LOOP_B", _parent(_P2_PREFIX, _call("LOOP_A")))]
    _irs, resolution = _resolve(roots)
    row = resolution.capabilities_by_root["LOOP_A"].child_entry_contract("$ref:LOOP_B")
    assert row.entry_form == "passthrough"
    assert (row.state_known, row.cache_writes_known) == (False, False)
    assert row.unwaited_writes_of_an_unknown_cache is True
    assert row.required_caches_retain_nothing_it_stored is False
    # Each member is still refused at its own call — what the seed hides is the profile its
    # callee consumes, so the caller cannot prove what it hands over (before round 6 the
    # vacuous contract made the same call a placement refusal instead).
    for key in ("LOOP_A", "LOOP_B"):
        assert (PROCESS_IR_SEMANTIC_PROFILE_MISMATCH, _LEG + "/process_ref") in _errors(roots, key)


# ---------------------------------------------------------------------------
# per-call discharge: admission, profile, wait
# ---------------------------------------------------------------------------


def test_a_waiting_passthrough_call_admits_an_attested_prefix_on_both_routes():
    roots = [("PARENT", _parent(_P2_PREFIX, _call("CHILD"))), ("CHILD", _CHILD)]
    assert _errors(roots, "PARENT") == []
    assert _compile_errors(roots, "PARENT") == []


def test_the_profile_the_child_consumes_is_checked_at_each_call():
    roots = [("PARENT", _parent(_P2_PREFIX, _call("CHILD_P1"))), ("CHILD_P1", _CHILD_P1)]
    assert (PROCESS_IR_SEMANTIC_PROFILE_MISMATCH, _LEG + "/process_ref") in _errors(roots, "PARENT")


def test_each_caller_discharges_the_contract_independently():
    """One parent hands over P2; the other hands over documents a Message rewrote."""
    roots = [("PARENT", _parent(_P2_PREFIX, _call("CHILD"))),
             ("OTHER_PARENT", _parent([_MSG], _call("CHILD"))),
             ("CHILD", _CHILD)]
    assert _errors(roots, "PARENT") == []
    assert (PROCESS_IR_SEMANTIC_PROFILE_MISMATCH, _LEG + "/process_ref") in _errors(roots, "OTHER_PARENT")


@pytest.mark.parametrize("prefix", [pytest.param(_P2_PREFIX, id="prefixed"), pytest.param([], id="empty")])
def test_a_passthrough_call_without_waiting_is_refused_at_wait(prefix):
    roots = [("PARENT", _parent(prefix, _call("CHILD", wait=False))), ("CHILD", _CHILD)]
    assert (PROCESS_IR_CAPABILITY_ENTRY_CONTEXT_UNSUPPORTED, _LEG + "/wait") in _errors(roots, "PARENT")
    assert (PROCESS_IR_CAPABILITY_ENTRY_CONTEXT_UNSUPPORTED, _LEG + "/wait") in _compile_errors(roots, "PARENT")


def test_a_prefix_into_a_child_with_no_derivable_contract_is_refused_at_the_terminal():
    """EXTERNAL is a process symbol but no root of this request, so nothing states its entry.
    An empty-prefix call keeps the legacy compatibility placement."""
    assert (_PLACEMENT, _LEG) in _errors([("PARENT", _parent(_P2_PREFIX, _call("EXTERNAL")))], "PARENT")
    assert (_PLACEMENT, _LEG) not in _errors([("PARENT", _parent([], _call("EXTERNAL")))], "PARENT")


def test_a_no_data_child_after_a_prefix_is_refused_and_its_empty_prefix_call_is_not():
    assert (_PLACEMENT, _LEG) in _errors([("PARENT", _parent(_P2_PREFIX, _call("NODATA"))), ("NODATA", _NODATA)], "PARENT")
    assert (_PLACEMENT, _LEG) not in _errors([("PARENT", _parent([], _call("NODATA"))), ("NODATA", _NODATA)], "PARENT")


def test_an_interposed_decision_keeps_the_prefix_obligation():
    """Native work, then a Decision, then a call whose arm authors no step (ARCH-184-r1-03).

    A Data Passthrough parent carries its native-work marker across the control
    (amendment 1 rule 3), and newly authored passthrough entry cannot use the legacy
    empty-prefix placement (rule 4). The call is keyed on the marker: a child with no
    derivable contract and a No Data child are refused at the call, a passthrough child
    is admitted, and a known child's contract is still discharged there. The same holds
    at the root, behind a Branch, and behind two Decisions."""
    arm = _LEG + "/true_arm/terminal"
    refused = (("EXTERNAL", []), ("NODATA", [("NODATA", _NODATA)]))
    for key, extra in refused:
        roots = [("PARENT", _parent(_P2_PREFIX, _decision(_call(key))))] + extra
        assert (_PLACEMENT, arm) in _errors(roots, "PARENT"), key
        assert (_PLACEMENT, arm) in _compile_errors(roots, "PARENT"), key
    admitted = [("PARENT", _parent(_P2_PREFIX, _decision(_call("CHILD")))), ("CHILD", _CHILD)]
    assert _errors(admitted, "PARENT") == []
    assert _compile_errors(admitted, "PARENT") == []
    mismatched = [("PARENT", _parent(_P2_PREFIX, _decision(_call("CHILD_P1")))), ("CHILD_P1", _CHILD_P1)]
    assert (PROCESS_IR_SEMANTIC_PROFILE_MISMATCH, arm + "/process_ref") in _errors(mismatched, "PARENT")
    spellings = (
        (lambda call: _doc(_ENTRY, _MAP, _decision(call)), "/body/steps/2/true_arm/terminal"),
        (lambda call: _doc(_ENTRY, _MAP, _branch([], call)), "/body/steps/2/legs/0/terminal"),
        (lambda call: _doc(_ENTRY, _MAP, _decision(_decision(call))),
         "/body/steps/2/true_arm/terminal/true_arm/terminal"),
    )
    for spell, pointer in spellings:
        for key, extra in refused:
            assert (_PLACEMENT, pointer) in _errors([("PARENT", spell(_call(key)))] + extra, "PARENT"), (pointer, key)
        assert _errors([("PARENT", spell(_call("CHILD"))), ("CHILD", _CHILD)], "PARENT") == [], pointer


def test_the_prefix_key_admits_only_the_captured_form_and_wait():
    assert model.process_call_prefix_admitted("branch_leg", "map_ref", "passthrough", True)
    for form, wait in (("passthrough", False), ("scheduled", True), ("unknown", True), (None, True)):
        assert not model.process_call_prefix_admitted("branch_leg", "map_ref", form, wait), (form, wait)


def test_the_whole_key_is_load_bearing(monkeypatch):
    """Non-vacuity: admit every key and the No Data prefix is no longer refused."""
    roots = [("PARENT", _parent(_P2_PREFIX, _call("NODATA"))), ("NODATA", _NODATA)]
    assert (_PLACEMENT, _LEG) in _errors(roots, "PARENT")
    monkeypatch.setattr(lineage, "process_call_prefix_admitted", lambda *args: True)
    assert (_PLACEMENT, _LEG) not in _errors(roots, "PARENT")


# ---------------------------------------------------------------------------
# per-call discharge: writers, execution state, repeated No Data runs
# ---------------------------------------------------------------------------


def test_a_childs_bound_path_is_proved_against_each_callers_writer():
    child = ("BOUND", _BOUND)
    _irs, resolution = _resolve([("PARENT", _parent([_DYNAMIC_X], _call("BOUND"))), child])
    assert resolution.capabilities_by_root["PARENT"].child_entry_contract("$ref:BOUND").required_writers == (("X", None),)
    assert [c for c in _errors([("PARENT", _parent([_DYNAMIC_X], _call("BOUND"))), child], "PARENT")
            if "DYNAMIC_PATH" in c[0]] == []
    assert (PROCESS_IR_SEMANTIC_DYNAMIC_PATH_NO_DYNAMIC_SEGMENT, _LEG + "/process_ref") in _errors(
        [("PARENT", _parent([_STATIC_X], _call("BOUND"))), child], "PARENT")
    assert (PROCESS_IR_SEMANTIC_DYNAMIC_PATH_DDP_NOT_ESTABLISHED, _LEG + "/process_ref") in _errors(
        [("PARENT", _parent([_SET_Z], _call("BOUND"))), child], "PARENT")


def test_a_called_child_rests_its_binding_on_the_callers_writer_and_a_standalone_one_does_not():
    roots = [("PARENT", _parent([_DYNAMIC_X], _call("BOUND"))), ("BOUND", _BOUND)]
    refusal = (PROCESS_IR_SEMANTIC_DYNAMIC_PATH_DDP_NOT_ESTABLISHED, "/body/steps/1/path_binding")
    assert refusal not in _errors(roots, "BOUND")
    irs, _resolution = _resolve(roots)
    standalone = validate_process_ir(irs["BOUND"], _symbols())
    assert refusal in [(item.code, item.path) for item in standalone.errors]


def test_a_step_that_hands_on_other_documents_keeps_the_childs_own_refusal():
    """The derivation is measured on the child's walk: a split before the binding drops
    the caller's writer, so nothing is demanded of the caller and the child stays refused."""
    roots = [("PARENT", _parent([_DYNAMIC_X], _call("BOUND_SPLIT"))), ("BOUND_SPLIT", _BOUND_SPLIT)]
    _irs, resolution = _resolve(roots)
    assert resolution.capabilities_by_root["PARENT"].child_entry_contract("$ref:BOUND_SPLIT").required_writers == ()
    assert (PROCESS_IR_SEMANTIC_DYNAMIC_PATH_DDP_NOT_ESTABLISHED, "/body/steps/2/path_binding") in _errors(roots, "BOUND_SPLIT")


def test_a_childs_required_process_property_is_demanded_of_each_caller():
    child = ("NEEDS_K", _NEEDS_K)
    missing = [("PARENT", _doc(_branch([], _call("NEEDS_K")))), child]
    assert (PROCESS_IR_SEMANTIC_LINEAGE_PROPERTY_READ_BEFORE_WRITE, "/body/steps/0/legs/0/terminal") in _errors(missing, "PARENT")
    supplied = [("PARENT", _doc({"kind": "branch", "legs": [
        {"steps": [_SET_K], "terminal": _STOP},
        {"steps": [], "terminal": _call("NEEDS_K")},
    ]})), child]
    assert _errors(supplied, "PARENT") == []
    assert _errors(supplied, "NEEDS_K") == []
    irs, _resolution = _resolve(supplied)
    standalone = validate_process_ir(irs["NEEDS_K"], _symbols())
    assert PROCESS_IR_SEMANTIC_LINEAGE_PROPERTY_READ_BEFORE_WRITE in {item.code for item in standalone.errors}


def test_a_no_data_child_does_not_owe_its_document_property_reads_to_a_caller():
    roots = [("PARENT", _doc(_branch([], _call("READS_X")))), ("READS_X", _READS_X)]
    _irs, resolution = _resolve(roots)
    assert resolution.capabilities_by_root["PARENT"].child_entry_contract("$ref:READS_X").required_reads == ()
    assert (PROCESS_IR_SEMANTIC_LINEAGE_PROPERTY_READ_BEFORE_WRITE, "/body/steps/0/true_arm/steps/0") in _errors(roots, "READS_X")


def test_a_no_data_child_run_per_document_may_not_change_the_state_it_requires(monkeypatch):
    """Amendment 1 rule 8, answered by the lattice per component (ledger row C20a).

    The passthrough parent's stream may carry several documents, so the No Data child may
    run several times. A process property is never un-established, so a child that reads
    K and rewrites it leaves K established for its next run. A cache it requires and may
    write or remove is refused, because the lattice cannot express a possible removal.

    Refused under its own code since QA-184-s1-r21-01, which the placement code's step-prefix
    text could not explain; the refusal, its pointer and both mutant answers are unchanged.
    Where a verdict is meant to be clean the WHOLE verdict is asserted, so no other code at this
    pointer — the placement code the step-prefix rule still raises included — can hide behind a
    check for one code (TI-184-21a-03)."""
    def parent(child_key, first=None):
        return _doc(_ENTRY, {"kind": "branch", "legs": [
            first or {"steps": [_SET_K], "terminal": _STOP},
            {"steps": [], "terminal": _call(child_key)},
        ]})

    placement = (PROCESS_IR_CAPABILITY_PROCESS_CALL_REPEATED_RUN_UNSTABLE, "/body/steps/1/legs/1/terminal")
    rewrites_k = [("PARENT", parent("MUTATES_K")), ("MUTATES_K", _MUTATES_K)]
    assert _errors(rewrites_k, "PARENT") == []
    stable = [("PARENT", parent("NEEDS_K")), ("NEEDS_K", _NEEDS_K)]
    assert _errors(stable, "PARENT") == []

    staged = {"steps": [_MAP], "terminal": _put("$ref:CACHE")}
    reads_then_removes = _legs(
        {"steps": [_read_cache("$ref:CACHE"), _MSG], "terminal": _STOP},
        {"steps": [], "terminal": {"kind": "cache_remove", "cache_ref": "$ref:CACHE"}},
    )
    removal = [("PARENT", parent("CACHE_CHILD", staged)), ("CACHE_CHILD", reads_then_removes)]
    assert placement in _errors(removal, "PARENT")
    # A typed consumer of the cache (PATCH declares P2) and a later leg appending to it.
    # No map or script, so the child's state is known and only the cache term refuses it.
    appends_to_its_typed_cache = _legs(
        {"steps": [_read_cache("$ref:CACHE"), _PATCH], "terminal": _STOP},
        {"steps": [{"kind": "connector_call", "operation_ref": "$ref:GET"}], "terminal": _put("$ref:CACHE")},
    )
    typed = [("PARENT", parent("CACHE_CHILD_P1", staged)), ("CACHE_CHILD_P1", appends_to_its_typed_cache)]
    assert placement in _errors(typed, "PARENT")
    # Mutant: without the cache term only the unknown-state refusal is left, and both
    # children that may change the cache they read are admitted.
    monkeypatch.setattr(lineage, "_repetition_unstable_caches", lambda contract, cache_refs, semantic: ())
    assert _errors(removal, "PARENT") == []
    assert _errors(typed, "PARENT") == []


def test_a_child_may_leave_unknown_content_in_a_shared_cache():
    """A later leg's typed consumer of a cache stays unproved once a child could have
    written that cache; with a Message in the child's place it is proved."""
    def parent(middle_leg):
        return _doc(_ENTRY, {"kind": "branch", "legs": [
            {"steps": [_MAP], "terminal": {"kind": "cache_put", "cache_ref": "$ref:CACHE"}},
            middle_leg,
            {"steps": [{"kind": "cache_get", "cache_ref": "$ref:CACHE"},
                       {"kind": "map_ref", "map_ref": "$ref:M22"}], "terminal": _STOP},
        ]})

    consumer = (PROCESS_IR_SEMANTIC_PROFILE_MISMATCH, "/body/steps/1/legs/2/steps/1/map_ref")
    assert consumer in _errors([("PARENT", parent({"steps": [], "terminal": _call("EXTERNAL")}))], "PARENT")
    assert consumer not in _errors([("PARENT", parent({"steps": [_MSG], "terminal": _STOP}))], "PARENT")


def test_an_unbound_child_contract_is_a_caller_error():
    capabilities = ProcessIRValidationCapabilitiesV1(child_entry_contracts=(
        ChildEntryContractV1(process_ref="$ref:CHILD", entry_form="passthrough"),))
    report = validate_process_ir(parse_process_ir_v1(_NODATA), _symbols(), capabilities=capabilities)
    assert (PROCESS_IR_CAPABILITY_EFFECT_CONTRACT_INVALID, "/capabilities/child_entry_contracts/0") in [
        (item.code, item.path) for item in report.errors]


# ---------------------------------------------------------------------------
# public route and standalone use
# ---------------------------------------------------------------------------


def test_the_public_plan_derives_and_discharges_the_contract_without_declarations():
    from unittest.mock import MagicMock

    from test_issue_158_listener_deployment import (
        _PROFILE,
        _ApplyBoundary,
        _cause_codes,
        _request,
        _unit,
    )
    from boomi_mcp.categories.integration_builder import build_integration_action

    parent = _doc(_branch([_MSG], {"kind": "process_call", "process_ref": "$ref:child"}))

    def plan(child_doc):
        raw = _request(
            [_unit(parent, ("child",), key="root"), _unit(child_doc, (), key="child", name="E184 Child")],
            [],
        ).model_dump(mode="json")
        with _ApplyBoundary().installed():
            return build_integration_action(MagicMock(), _PROFILE, "plan", config={"authoring_request": raw})

    admitted = plan(_doc(dict(_ENTRY, label="E184 child"), _MSG, _STOP))
    assert admitted["authoring_result"]["validation_report"]["is_valid"] is True, _cause_codes(admitted)
    refused = plan(_NODATA)
    assert _PLACEMENT in _cause_codes(refused), _cause_codes(refused)


def test_a_passthrough_root_records_what_a_direct_run_would_lack():
    from boomi_mcp.categories.integration_builder import _standalone_entry_records

    capabilities = ProcessIRValidationCapabilitiesV1(entry_contract=ChildEntryContractV1(
        process_ref="$ref:root", entry_form="passthrough", document_requirements=("$ref:P2",),
        required_reads=(("cache", "$ref:CACHE"), ("dpp", "K")), required_writers=(("X", None),)))
    bundle = SimpleNamespace(materialization_plans={
        "root": SimpleNamespace(execution_profile="passthrough", effect_capabilities=capabilities),
        "other": SimpleNamespace(execution_profile="scheduled", effect_capabilities=None),
    })
    assert _standalone_entry_records(bundle) == {"root": {
        "derived": True, "consumes_caller_documents": True, "caller_composed_paths": 1,
        "caller_document_properties": 0, "caller_cache_contents": 1,
        "dynamic_process_properties": ["K"],
    }}


def test_a_direct_run_of_a_passthrough_root_that_needs_a_caller_is_refused_before_mutation():
    """The gate reads the requirements recorded with the typed build. The record is
    edited below to isolate the gate; its derivation is pinned by the test above."""
    from test_issue_158_listener_deployment import _deploy, _error_codes, _request, _typed_build, _unit
    from boomi_mcp.categories.integration_builder import _BUILD_REGISTRY

    applied, _boundary = _typed_build(_request([_unit(_doc(dict(_ENTRY, label="E184 standalone"), _MSG, _STOP), ())], []))
    build_id = applied["build_id"]
    record = _BUILD_REGISTRY[build_id]["authoring"]["standalone_entry"]["root"]
    assert record == {"derived": True, "consumes_caller_documents": False, "caller_composed_paths": 0,
                      "caller_document_properties": 0, "caller_cache_contents": 0,
                      "dynamic_process_properties": []}, record
    schedule = {"mode": "scheduled", "cron": "0 * * * *", "enabled": True, "max_retry": 0}
    code = PROCESS_IR_CAPABILITY_ENTRY_CONTEXT_UNSUPPORTED

    assert code not in _error_codes(_deploy(build_id, dry_run=True, run_test=True))
    record["dynamic_process_properties"] = ["K"]
    assert _error_codes(_deploy(build_id, dry_run=True, run_test=True)) == [code]
    assert code not in _error_codes(_deploy(build_id, dry_run=True, run_test=True, test_dynamic_properties={"K": "v"}))
    assert _error_codes(_deploy(build_id, dry_run=True, schedule_override=schedule)) == [code]
    record["dynamic_process_properties"] = []
    record["consumes_caller_documents"] = True
    refused = _deploy(build_id, dry_run=True, run_test=True)
    assert _error_codes(refused) == [code], refused
    assert refused["errors"][0]["details"]["requirements"] == ["caller_documents"]
    assert code not in _error_codes(_deploy(build_id, dry_run=True))
    del _BUILD_REGISTRY[build_id]["authoring"]["standalone_entry"]
    unrecorded = _deploy(build_id, dry_run=True, run_test=True)
    assert unrecorded["errors"][0]["details"]["requirements"] == ["entry_contract_not_recorded"]


def test_the_revision_moves_with_child_contract_emission_and_survival_behaviour(monkeypatch):
    """Amendment 3 §10: a BEHAVIOUR change moves the compiler revision.

    Each perturbation changes what the server accepts, not a sentence: admitting every
    prefix key, marking a split as keeping document properties, dropping a
    document-emission row, and four changes to the retrieve overlay (ARCH-184-r1-09):
    dropping the cached writer alternatives, dropping the carried writers, freezing no
    writer at a cache write, and assuming the one-current/one-cached singleton. Each of
    the four flips a staged graph's verdict inside the property-survival row, for every
    read kind. The revision rows read their authorities at call time, so each
    perturbation is visible, and the baseline returns afterwards.
    """
    from types import MappingProxyType

    from boomi_mcp.authoring import contract as authoring_contract
    from boomi_mcp.models import process_ir_document_semantics as emission

    baseline_payload = authoring_contract._compiler_revision_payload()
    baseline = authoring_contract.sha256_fingerprint(baseline_payload)
    with monkeypatch.context() as patched:
        patched.setattr(lineage, "process_call_prefix_admitted", lambda *args: True)
        assert authoring_contract._compiler_revision_moved(baseline_payload)[0]
    with monkeypatch.context() as patched:
        cells = dict(lineage.PROPERTY_SURVIVAL_V1)
        cells[("data_process", "split_documents")] = "survives"
        patched.setattr(lineage, "PROPERTY_SURVIVAL_V1", MappingProxyType(cells))
        assert authoring_contract._compiler_revision_moved(baseline_payload)[0]
    with monkeypatch.context() as patched:
        rows = dict(emission.DOCUMENT_EMISSION_V1)
        rows.pop("stop")
        patched.setattr(emission, "DOCUMENT_EMISSION_V1", MappingProxyType(rows))
        assert authoring_contract._compiler_revision_moved(baseline_payload)[0]

    real_overlay, real_freeze = lineage._overlay_cache_read, lineage._cohort_at_write

    def with_cohorts(state, cohorts):
        return lineage._State(state.document, state.execution, state.content, frozenset(cohorts))

    def without_cached_alternatives(semantic, state, writers, on_documents, invalidated, stream):
        stripped = with_cohorts(
            state, ((ref, cohort._replace(alternatives=frozenset())) for ref, cohort in state.cohorts))
        after, carried, documents, dropped, count = real_overlay(
            semantic, stripped, writers, on_documents, invalidated, stream)
        return with_cohorts(after, state.cohorts), carried, documents, dropped, count

    def without_carried_writers(*args):
        after, carried, documents, dropped, count = real_overlay(*args)
        return after, {key: value for key, value in carried.items() if key[0] != lineage.DDP}, documents, dropped, count

    def assuming_the_singleton(semantic, state, writers, on_documents, invalidated, stream):
        ones = with_cohorts(state, ((ref, cohort._replace(count=lineage.COUNT_ONE)) for ref, cohort in state.cohorts))
        after, carried, documents, dropped, count = real_overlay(
            semantic, ones, writers, on_documents, invalidated, stream._replace(count=lineage.COUNT_ONE))
        return with_cohorts(after, state.cohorts), carried, documents, dropped, count

    def freezing_no_writer(on_documents, writers, stream):
        return real_freeze(on_documents, writers, stream)._replace(alternatives=frozenset())

    def overlay_graphs(payload):
        row = payload["property_survival"]
        assert row != "unavailable"
        return row["overlay_verdicts"]["graphs"]

    kinds = sorted(lineage.TRIGGERED_REPLACEMENT_SEMANTIC_KINDS)
    unperturbed = overlay_graphs(baseline_payload)
    # Non-vacuity: before anything is perturbed, the cached writer admits its bound path
    # and a current-only write proves nothing past the read.
    for kind in kinds:
        assert unperturbed[kind + ":cached_dynamic_writer_bound"] == [], kind
        assert unperturbed[kind + ":current_only_ordinary_read"] != [], kind
    for name, replacement, flipped in (
        ("_overlay_cache_read", without_cached_alternatives, "cached_dynamic_writer_bound"),
        ("_overlay_cache_read", without_carried_writers, "cached_dynamic_writer_bound"),
        ("_cohort_at_write", freezing_no_writer, "cached_dynamic_writer_bound"),
        ("_overlay_cache_read", assuming_the_singleton, "current_only_ordinary_read"),
    ):
        with monkeypatch.context() as patched:
            patched.setattr(lineage, name, replacement)
            moved, perturbed = authoring_contract._compiler_revision_moved(baseline_payload)
            graphs = overlay_graphs(perturbed)
            assert all(graphs[kind + ":" + flipped] != unperturbed[kind + ":" + flipped] for kind in kinds), (
                replacement.__name__, graphs)
            assert moved, replacement.__name__
    assert authoring_contract._compiler_revision() == baseline

# ---------------------------------------------------------------------------
# QA-184-s1-r1-02: typed cache requirements across the child boundary (amendment 1 rule 6)
# ---------------------------------------------------------------------------


def _stage_then_call(child_key):
    """A scheduled parent: one leg stages P2 documents in CACHE, the next calls the child."""
    return _doc({"kind": "branch", "legs": [
        {"steps": [{"kind": "connector_call", "operation_ref": "$ref:GETP1"}, _MAP],
         "terminal": {"kind": "cache_put", "cache_ref": "$ref:CACHE"}},
        {"steps": [], "terminal": _call(child_key)},
    ]})


def _cache_child(map_ref):
    return _doc({"kind": "cache_get", "cache_ref": "$ref:CACHE"},
                {"kind": "map_ref", "map_ref": map_ref}, _STOP)


def test_a_childs_cache_consumer_is_proved_against_each_callers_writes():
    """The QA reproduction: a No Data child `[cache_get, map_ref, stop]` whose caller's
    writes store the map's source profile is admitted. Alone it stays refused: nothing in
    it proves what the cache holds (amendment 3's matrix row for that root)."""
    roots = [("PARENT", _stage_then_call("CACHE_CHILD")), ("CACHE_CHILD", _cache_child("$ref:M22"))]
    irs, resolution = _resolve(roots)
    row = resolution.capabilities_by_root["PARENT"].child_entry_contract("$ref:CACHE_CHILD")
    assert row.cache_requirements == (("$ref:CACHE", "$ref:P2"),), row
    assert resolution.capabilities_by_root["CACHE_CHILD"].caller_cache_contents == (("$ref:CACHE", "$ref:P2"),)
    assert _errors(roots, "PARENT") == []
    assert _errors(roots, "CACHE_CHILD") == []
    assert _compile_errors(roots, "CACHE_CHILD") == []
    standalone = validate_process_ir(irs["CACHE_CHILD"], _symbols())
    assert (PROCESS_IR_SEMANTIC_PROFILE_MISMATCH, "/body/steps/1/map_ref") in [
        (item.code, item.path) for item in standalone.errors]


def test_a_caller_whose_writes_store_another_profile_is_refused_at_its_call():
    """The child needs P1 of the cache and this caller stored P2. The child is valid under
    its requirement; the call that cannot meet it is the one refused."""
    roots = [("PARENT", _stage_then_call("CACHE_CHILD_P1")), ("CACHE_CHILD_P1", _cache_child("$ref:M12"))]
    assert (PROCESS_IR_SEMANTIC_PROFILE_MISMATCH, "/body/steps/0/legs/1/terminal/process_ref") in _errors(roots, "PARENT")
    assert _errors(roots, "CACHE_CHILD_P1") == []


def test_the_callers_cache_proof_is_load_bearing(monkeypatch):
    """Non-vacuity: without the seeded content the called child is refused at its map."""
    roots = [("PARENT", _stage_then_call("CACHE_CHILD")), ("CACHE_CHILD", _cache_child("$ref:M22"))]
    assert _errors(roots, "CACHE_CHILD") == []
    from boomi_mcp.authoring import process_ir_effects

    monkeypatch.setattr(process_ir_effects, "_caller_cache_seeds", lambda requirements, symbols: ())
    assert (PROCESS_IR_SEMANTIC_PROFILE_MISMATCH, "/body/steps/1/map_ref") in _errors(roots, "CACHE_CHILD")


def test_one_rule_judges_cache_content_in_process_and_across_a_call():
    """ARCH-184-r1-02: a consumer in the caller and the same consumer in a child are judged alike.

    The caller stages GETP1's P1 output in CACHE, and `[cache_get, PATCH]` needs P2 of it. In
    process it is refused at the call's own `/operation_ref`; as a No Data child it is refused at
    the caller's `/process_ref`, from the same content. A caller staging P2 through the map
    satisfies both. Called, the child is valid under its requirement; run alone it proves nothing
    about the cache, so it is refused at its call, exactly as its map would be."""
    code = PROCESS_IR_SEMANTIC_PROFILE_MISMATCH
    consumer = [{"kind": "cache_get", "cache_ref": "$ref:CACHE"}, _PATCH]
    for stage, refused in (([], True), ([_MAP], False)):
        staging = {"steps": [{"kind": "connector_call", "operation_ref": "$ref:GETP1"}] + stage,
                   "terminal": {"kind": "cache_put", "cache_ref": "$ref:CACHE"}}
        in_process = [("PARENT", _doc({"kind": "branch", "legs": [
            staging, {"steps": consumer, "terminal": _STOP}]}))]
        called = [("PARENT", _doc({"kind": "branch", "legs": [
            staging, {"steps": [], "terminal": _call("CACHE_CHILD")}]})),
            ("CACHE_CHILD", _doc(*consumer, _STOP))]
        in_process_pointer = (code, "/body/steps/0/legs/1/steps/1/operation_ref")
        assert (in_process_pointer in _errors(in_process, "PARENT")) == refused
        assert (in_process_pointer in _compile_errors(in_process, "PARENT")) == refused
        assert ((code, "/body/steps/0/legs/1/terminal/process_ref") in _errors(called, "PARENT")) == refused
        assert _errors(called, "CACHE_CHILD") == []
    irs, _resolution = _resolve(called)
    standalone = validate_process_ir(irs["CACHE_CHILD"], _symbols())
    assert (code, "/body/steps/1/operation_ref") in [(item.code, item.path) for item in standalone.errors]


def _entry_by_id(value, entry_id):
    """The served contract entry with this id, wherever the payload nests it."""
    if isinstance(value, dict):
        if value.get("contract_entry_id") == entry_id:
            return value
        value = list(value.values())
    if isinstance(value, list):
        for item in value:
            found = _entry_by_id(item, entry_id)
            if found is not None:
                return found
    return None


def test_the_process_call_page_states_the_passthrough_standalone_refusal():
    """QA-184-s1-r1-01: the node page a caller reads for calls states it too."""
    from boomi_mcp.authoring.process_ir_projection import process_ir_authoring_revision_payload

    node = _entry_by_id(process_ir_authoring_revision_payload(), "node.process_call")
    assert node is not None
    facts = " ".join(node.get("ordering_facts") or ())
    assert "a direct run of one that requires what only a caller supplies is refused" in facts

# ---------------------------------------------------------------------------
# Stage-2 review round r1 (`cdx-review.PJotK5`), correction batch 2
# ---------------------------------------------------------------------------

_GETP1 = {"kind": "connector_call", "operation_ref": "$ref:GETP1"}


def _legs(*legs):
    return _doc({"kind": "branch", "legs": list(legs)})


def _put(ref):
    return {"kind": "cache_put", "cache_ref": ref}


def _read_cache(ref):
    return {"kind": "cache_get", "cache_ref": ref}


_THROUGH_ALIAS = [("PARENT", _legs(
    {"steps": [_GETP1, _MAP], "terminal": _put("$ref:CACHE_ALIAS")},
    {"steps": [_read_cache("$ref:CACHE"), {"kind": "map_ref", "map_ref": "$ref:M22"}], "terminal": _STOP},
))]


def test_a_cache_alias_is_the_same_cache_for_every_cache_fact():
    """F1: both refs name one documentcache component. A P2 write through the alias
    makes a P1 map over the other spelling unproved, and a write through the alias
    alone establishes that read, with its profile."""
    mixed = [("PARENT", _legs(
        {"steps": [_GETP1], "terminal": _put("$ref:CACHE")},
        {"steps": [_GETP1, _MAP], "terminal": _put("$ref:CACHE_ALIAS")},
        {"steps": [_read_cache("$ref:CACHE"), {"kind": "map_ref", "map_ref": "$ref:M12"}], "terminal": _STOP},
    ))]
    assert (PROCESS_IR_SEMANTIC_PROFILE_MISMATCH, "/body/steps/0/legs/2/steps/1/map_ref") in _errors(mixed, "PARENT")
    assert _errors(_THROUGH_ALIAS, "PARENT") == []


def test_the_canonical_cache_spelling_is_load_bearing(monkeypatch):
    """Non-vacuity: with no canonical spelling, the alias write no longer reaches the read."""
    from boomi_mcp.compiler.process_ir.semantic_validation import context, pipeline

    assert _errors(_THROUGH_ALIAS, "PARENT") == []
    for module in (context, pipeline, lineage):
        monkeypatch.setattr(module, "canonical_cache_refs", lambda symbols: {})
    codes = {code for code, _path in _errors(_THROUGH_ALIAS, "PARENT")}
    assert "PROCESS_IR_SEMANTIC_LINEAGE_CACHE_WRITER_MISSING" in codes, codes


def test_a_child_whose_declared_external_writer_fills_its_cache_owes_its_caller_nothing():
    """F2: the child's contract is derived under its own trusted context, so the external
    writer contract satisfying its read keeps that read off every caller."""
    from boomi_mcp.models.authoring_workflow import (
        ProcessIREffectDeclarationsV1,
        ProcessIRExternalWriterDeclarationV1,
    )

    child = _doc({"kind": "cache_get", "cache_ref": "$ref:CACHE", "external_writer": True}, _SET_Z, _STOP)
    parsed = [
        ("PARENT", parse_process_ir_v1(_doc(_branch([], _call("EXTCHILD"))))),
        ("EXTCHILD", parse_process_ir_v1(child)),
    ]
    declarations = ProcessIREffectDeclarationsV1(external_writers=(
        ProcessIRExternalWriterDeclarationV1(cache_ref="$ref:CACHE"),))
    resolution = resolve_process_ir_effect_declarations(
        parsed, declarations, _symbols(), [], child_roots={"$ref:" + key: ir for key, ir in parsed})
    assert resolution.ok, resolution.findings
    row = resolution.capabilities_by_root["PARENT"].child_entry_contract("$ref:EXTCHILD")
    assert ("cache", "$ref:CACHE") not in row.required_reads, row
    for key, ir in parsed:
        report = validate_process_ir(ir, _symbols(), capabilities=_capabilities(resolution, key))
        assert [(item.code, item.path) for item in report.errors] == [], key


def test_a_forwarding_call_passes_its_childs_cache_requirement_to_its_caller():
    """F3: MID reads nothing itself, so the profile its child needs of the cache is MID's
    own caller's obligation, carried up and seeded like a consumer's. A caller whose
    writes store another profile is refused at its own call."""
    def mid(child_key):
        return _legs({"steps": [_MSG], "terminal": _STOP}, {"steps": [], "terminal": _call(child_key)})

    roots = [("PARENT", _stage_then_call("MID")), ("MID", mid("CACHE_CHILD")), ("CACHE_CHILD", _cache_child("$ref:M22"))]
    _irs, resolution = _resolve(roots)
    assert resolution.capabilities_by_root["PARENT"].child_entry_contract("$ref:MID").cache_requirements == (
        ("$ref:CACHE", "$ref:P2"),)
    for key in ("PARENT", "MID", "CACHE_CHILD"):
        assert _errors(roots, key) == [], key
    mismatched = [("PARENT", _stage_then_call("MID")), ("MID", mid("CACHE_CHILD_P1")),
                  ("CACHE_CHILD_P1", _cache_child("$ref:M12"))]
    assert (PROCESS_IR_SEMANTIC_PROFILE_MISMATCH, "/body/steps/0/legs/1/terminal/process_ref") in _errors(mismatched, "PARENT")


def test_a_forwarding_passthrough_passes_its_childs_bound_path_writer_to_its_caller():
    """F4: MIDP only forwards its caller's documents to BOUND, so the writer BOUND's path
    needs is MIDP's caller's obligation, carried up and seeded like a binding's own."""
    midp = _doc(_ENTRY, _call("BOUND"))
    roots = [("PARENT", _parent([_DYNAMIC_X], _call("MIDP"))), ("MIDP", midp), ("BOUND", _BOUND)]
    _irs, resolution = _resolve(roots)
    assert resolution.capabilities_by_root["PARENT"].child_entry_contract("$ref:MIDP").required_writers == (("X", None),)
    for key in ("PARENT", "MIDP", "BOUND"):
        assert _errors(roots, key) == [], key
    missing = [("PARENT", _parent([_SET_Z], _call("MIDP"))), ("MIDP", midp), ("BOUND", _BOUND)]
    assert (PROCESS_IR_SEMANTIC_DYNAMIC_PATH_DDP_NOT_ESTABLISHED, _LEG + "/process_ref") in _errors(missing, "PARENT")


# ---------------------------------------------------------------------------
# Stage-2 review round r2 (`cdx-review.i7DdUD`), correction batch 3
# ---------------------------------------------------------------------------


def _forwarder(first):
    return _legs({"steps": [], "terminal": _call(first)}, {"steps": [], "terminal": _call("CACHE_CHILD")})


_APPENDS_P1 = _legs({"steps": [_GETP1], "terminal": _put("$ref:CACHE")}, {"steps": [_MSG], "terminal": _STOP})
_FORWARDED_READ = (PROCESS_IR_SEMANTIC_PROFILE_MISMATCH, "/body/steps/0/legs/1/terminal/process_ref")


def test_a_call_that_appends_to_a_forwarded_cache_leaves_it_unproved(monkeypatch):
    """CDX-184-r2-01: MID names no cache. Its first call appends P1 to the cache its caller
    staged with P2, so the P2 map its second call runs is unproved at that call. A child
    that is not a root of the request is unknown, and may append just the same."""
    def roots(first, *extra):
        return [("PARENT", _stage_then_call("MID")), ("MID", _forwarder(first)),
                ("CACHE_CHILD", _cache_child("$ref:M22"))] + list(extra)

    assert _FORWARDED_READ in _errors(roots("WRITER", ("WRITER", _APPENDS_P1)), "MID")
    assert _FORWARDED_READ in _errors(roots("NODATA"), "MID")
    # CONTROL: a first call that writes nothing leaves the caller's proof standing.
    assert _errors(roots("NODATA", ("NODATA", _NODATA)), "MID") == []
    # Non-vacuity: with no cache marked after a call, the appended P1 is invisible again.
    monkeypatch.setattr(lineage, "_caches_a_call_may_write", lambda cache_refs, contract: ())
    assert _errors(roots("WRITER", ("WRITER", _APPENDS_P1)), "MID") == []


def test_a_cache_declaration_on_either_reference_binds_the_component():
    """CDX-184-r2-02: the profile is declared on the alias that sorts after the canonical
    spelling. A P1 write through either reference is refused at the write."""
    declared = SymbolTableV1(symbols=_symbols().symbols + (ComponentSymbolV1(
        ref="$ref:CACHE_ZDECL", component_id="CACHE", component_type="documentcache",
        cache_profile_ref="$ref:P2"),))

    def errors(table, ref, steps):
        ir = parse_process_ir_v1(_legs(
            {"steps": steps, "terminal": _put(ref)}, {"steps": [_MSG], "terminal": _STOP}))
        return [(item.code, item.path) for item in validate_process_ir(ir, table).errors]

    refused = (PROCESS_IR_SEMANTIC_PROFILE_MISMATCH, "/body/steps/0/legs/0/terminal/cache_ref")
    assert refused in errors(declared, "$ref:CACHE_ZDECL", [_GETP1])
    assert refused in errors(declared, "$ref:CACHE", [_GETP1])
    assert errors(declared, "$ref:CACHE", [_GETP1, _MAP]) == []  # CONTROL: P2 documents
    # Declarations that disagree: a write matching one of them is refused against the other.
    split = SymbolTableV1(symbols=declared.symbols + (ComponentSymbolV1(
        ref="$ref:CACHE_ZP1", component_id="CACHE", component_type="documentcache",
        cache_profile_ref="$ref:P1"),))
    assert refused in errors(split, "$ref:CACHE", [_GETP1])
    assert refused in errors(split, "$ref:CACHE", [_GETP1, _MAP])


def test_a_forwarder_owes_its_caller_only_the_bound_writers_it_does_not_establish():
    """CDX-184-r2-03: MIDP composes X itself and calls a child whose paths need X and Y.
    Its caller owes Y alone, so a caller composing only Y is admitted."""
    bound_xy = _doc(_ENTRY, {"kind": "branch", "legs": [
        {"steps": [_BOUND_GET], "terminal": _STOP},
        {"steps": [dict(_BOUND_GET, path_binding={"property_name": "Y"})], "terminal": _STOP},
    ]})
    dynamic_y = dict(_DYNAMIC_X, name="Y")
    midp = _parent([_DYNAMIC_X], _call("BOUND_XY"))
    roots = [("PARENT", _parent([dynamic_y], _call("MIDP"))), ("MIDP", midp), ("BOUND_XY", bound_xy)]
    _irs, resolution = _resolve(roots)
    assert resolution.capabilities_by_root["PARENT"].child_entry_contract("$ref:MIDP").required_writers == (
        ("Y", None),)
    for key in ("PARENT", "MIDP", "BOUND_XY"):
        assert _errors(roots, key) == [], key
    # CONTROL: a caller composing only X leaves Y unestablished at its call.
    only_x = [("PARENT", _parent([_DYNAMIC_X], _call("MIDP"))), ("MIDP", midp), ("BOUND_XY", bound_xy)]
    assert (PROCESS_IR_SEMANTIC_DYNAMIC_PATH_DDP_NOT_ESTABLISHED, _LEG + "/process_ref") in _errors(only_x, "PARENT")


def test_the_revision_moves_with_component_identity_and_forwarding_behaviour(monkeypatch):
    """CDX-184-r2-04: each perturbation changes a verdict the server returns, and the two
    oracle rows read their authorities at call time, so each moves the revision."""
    from boomi_mcp.authoring import contract as authoring_contract
    from boomi_mcp.authoring import process_ir_effects
    import collections

    from boomi_mcp.categories import integration_builder
    from boomi_mcp.recipes import materialization

    payload = authoring_contract._compiler_revision_payload()
    for row in ("component_identity", "child_forwarding"):
        assert payload[row] != "unavailable", row
    baseline = authoring_contract.sha256_fingerprint(payload)
    perturbations = (
        (integration_builder, "declared_bindings_for_components",
         lambda components, conflict_policy="reuse", existing_ids=None: {}),
        (lineage, "canonical_cache_profiles", lambda symbols: {}),
        (lineage, "_caches_a_call_may_write", lambda cache_refs, contract: ()),
        (process_ir_effects, "_caller_cache_seeds", lambda requirements, symbols: ()),
        (process_ir_effects, "_caller_composed_paths", lambda prepared, capabilities, walk: ()),
        # Stage-2 review round r3: the written spec and the facts projected from it.
        (process_ir_effects, "_written_map_effect",
         lambda aliases, components, conflict_policy, derive, canonical: (None, False)),
        # QA round r7: the canonical component id every binding and write-conflict check reads.
        (integration_builder, "canonical_component_id",
         lambda value: value.strip() if isinstance(value, str) and value.strip() else None),
        # Stage-2 review round r5: the write-conflict refusal and the canonical effect comparison.
        (integration_builder, "component_write_conflicts",
         lambda components, conflict_policy="reuse", existing_ids=None: {}),
        (integration_builder, "component_writes_existing", lambda comp, existing_ids=None: False),
        # Correction batch 10: the binding a route resolved with the account, ignored for the declared one.
        (integration_builder, "_bound_existing_id",
         lambda comp, existing_ids=None: integration_builder.resolve_planner_binding(
             None, comp, declared_only=True).existing_id),
        (integration_builder, "apply_writes_component_config", lambda comp, conflict_policy, existing_ids=None: True),
        (integration_builder, "_binds_as_metadata_only_connector_update", lambda component_type, config: False),
        (integration_builder, "smart_merge_would_change", lambda config: False),
        (process_ir_effects, "_canonical_effect", lambda effect, canonical: effect),
        # Correction batch 12 (CDX-184-r11-01): the plan route's identity readings. The guard
        # `test_every_identity_decision_of_the_plan_route_moves_the_compiler_revision` requires every one.
        (integration_builder, "reused_keys_for_components",
         lambda components, conflict_policy="reuse", existing_ids=None: set()),
        (integration_builder, "planned_existing_ids", lambda planned: None),
    )
    for module, name, replacement in perturbations:
        with monkeypatch.context() as patched:
            patched.setattr(module, name, replacement)
            # Non-vacuity: a replacement the oracle cannot call would read "unavailable" and move
            # the revision without changing any verdict.
            moved, perturbed = authoring_contract._compiler_revision_moved(payload)
            assert all(perturbed[row] != "unavailable" for row in ("component_identity", "child_forwarding")), name
            assert moved, name
    assert authoring_contract._compiler_revision() == baseline


def test_a_call_whose_unknown_effects_are_a_map_writes_no_unlisted_cache():
    """SELF-184-03: only a cache step writes a cache, so a child that is opaque only
    because of a map lists every cache it may write, and a forwarded cache stays proved.
    Only a call to a child whose own cache writes are unknown can hide one."""
    enrich = _doc(_GETP1, _MAP, _STOP)
    roots = [("PARENT", _stage_then_call("MID")), ("MID", _forwarder("ENRICH")), ("ENRICH", enrich),
             ("CACHE_CHILD", _cache_child("$ref:M22"))]
    _irs, resolution = _resolve(roots)
    row = resolution.capabilities_by_root["MID"].child_entry_contract("$ref:ENRICH")
    assert (row.state_known, row.cache_writes_known) == (False, True), row
    assert _errors(roots, "MID") == []
    hides = [("PARENT", _stage_then_call("MID")), ("MID", _forwarder("HIDES")),
             ("HIDES", _doc(_decision(_call("NODATA")))), ("CACHE_CHILD", _cache_child("$ref:M22"))]
    _irs, resolution = _resolve(hides)
    assert resolution.capabilities_by_root["MID"].child_entry_contract("$ref:HIDES").cache_writes_known is False
    assert _FORWARDED_READ in _errors(hides, "MID")


# ---------------------------------------------------------------------------
# Architect evaluation 1 (ARCH-184-r1-03, ARCH-184-r1-04), correction batch 18
# ---------------------------------------------------------------------------


def test_a_scheduled_parent_keeps_the_legacy_empty_prefix_placement_after_a_decision():
    """Rule 4's compatibility path stays open for a scheduled parent: the #141 capture
    attests `decision -> true -> processcall` after a leg step, whatever the child's form.
    Typed native work before the Decision keeps it too, as C21a scopes the finding."""
    arm = "/body/steps/0/legs/0/terminal/true_arm/terminal"
    for prefix in ([_MSG], [_read_cache("$ref:CACHE"), _MAP]):
        for key, extra in (("EXTERNAL", []), ("NODATA", [("NODATA", _NODATA)])):
            roots = [("PARENT", _doc(_branch(prefix, _decision(_call(key)))))] + extra
            assert (_PLACEMENT, arm) not in _errors(roots, "PARENT"), (prefix, key)
            assert (_PLACEMENT, arm) not in _compile_errors(roots, "PARENT"), (prefix, key)


def test_the_native_marker_is_load_bearing(monkeypatch):
    """Non-vacuity, both halves: key the call on its own body alone, or carry no marker
    past a step, and the interposed Decision hides the native work again."""
    roots = [("PARENT", _parent(_P2_PREFIX, _decision(_call("EXTERNAL"))))]
    refusal = (_PLACEMENT, _LEG + "/true_arm/terminal")
    assert refusal in _errors(roots, "PARENT")
    with monkeypatch.context() as patched:
        patched.setattr(lineage, "_prefix_predecessor", lambda context, own, marker, parent_form: own)
        assert refusal not in _errors(roots, "PARENT")
    with monkeypatch.context() as patched:
        patched.setattr(lineage, "_native_work_marker", lambda incoming, semantic_kind, authored_kind: incoming)
        assert refusal not in _errors(roots, "PARENT")
    assert refusal in _errors(roots, "PARENT")


def test_the_prefix_predecessor_reads_the_marker_only_where_a_prefix_key_exists():
    """The whole decision, row by row. The own body's step wins; an empty prefix body keys
    a Data Passthrough parent's call on the marker; every other parent keeps rule 4's
    legacy placement; and a call outside a prefix context (a root call, a catch body's
    recovery call) has no key, so no marker stands in. No public graph reaches that last
    row after native work today (a root call takes no step before it, and no try_catch
    follows a passthrough entry), so it is pinned here, on the predicate."""
    rows = (
        (("branch_leg", "message", "map_ref", "passthrough"), "message"),
        (("branch_leg", "message", "map_ref", "scheduled"), "message"),
        (("decision_true_arm", None, "map_ref", "passthrough"), "map_ref"),
        (("branch_leg", None, "map_ref", "passthrough"), "map_ref"),
        (("decision_true_arm", None, None, "passthrough"), None),
        (("decision_true_arm", None, "map_ref", "scheduled"), None),
        (("branch_leg", None, "map_ref", "listener"), None),
        ((None, None, "notify", "passthrough"), None),
    )
    for arguments, expected in rows:
        assert lineage._prefix_predecessor(*arguments) == expected, arguments


def test_the_process_call_page_states_the_forwarder_and_interposition_placement():
    """Served contract: the node page a caller reads for calls states both new refusals
    (ARCH-184-r1-03, -04) and the parents rule 4 keeps on the legacy placement."""
    from boomi_mcp.authoring.process_ir_projection import process_ir_authoring_revision_payload

    node = _entry_by_id(process_ir_authoring_revision_payload(), "node.process_call")
    assert node is not None
    facts = " ".join(node.get("ordering_facts") or ())
    for clause in (
        "or into a Data Passthrough process that hands the documents on to a process whose "
        "entry cannot be derived are refused",
        "In a Data Passthrough process these rules hold across a Branch or Decision after "
        "native work, with the last step ahead of it counting as the step before the call",
        "any other process keeps the empty-prefix placement there",
    ):
        assert clause in facts, clause


def _typed_request(units, components):
    """A typed authoring request: ``units`` are ``(key, ProcessIR payload, depends_on)``."""
    from boomi_mcp.models.authoring_workflow import AuthoringRequestV1

    return AuthoringRequestV1.model_validate({"contract_version": "2", "intent": {
        "intent_kind": "process_ir", "integration_name": "E184 forwarding",
        "units": [{"envelope": {"component_key": key, "name": "E184 " + key, "action": "create",
                                "depends_on": list(depends_on)}, "process_ir": payload}
                  for key, payload, depends_on in units],
        "components": components, "conflict_policy": "reuse"}})


def test_an_unknown_grandchild_leaves_its_forwarders_requirement_unknown():
    """ARCH-184-r1-04: MID only hands its caller's documents to EXTERNAL, which no root of
    the request states, so MID requires an unknown consumption of them (amendment 1 §2:
    an undeclared consumption never becomes NONE). The caller's prefix is refused exactly
    as the same prefix calling EXTERNAL directly, on every route; MID stays valid."""
    from test_issue_158_listener_deployment import _reference_child
    from test_issue_184_component_identity import _map, _profile
    from boomi_mcp.authoring.workflow import (
        AuthoringWorkflowError,
        compile_authoring_request_v1,
        plan_authoring_request_v1,
    )

    roots = [("PARENT", _parent(_P2_PREFIX, _call("MID"))), ("MID", _doc(_ENTRY, _call("EXTERNAL")))]
    _irs, resolution = _resolve(roots)
    assert resolution.capabilities_by_root["PARENT"].child_entry_contract("$ref:MID").document_requirements == (None,)
    assert (_PLACEMENT, _LEG) in _errors(roots, "PARENT")
    assert (_PLACEMENT, _LEG) in _compile_errors(roots, "PARENT")
    assert _errors(roots, "MID") == []
    assert _compile_errors(roots, "MID") == []

    request = _typed_request(
        [("root", _parent([{"kind": "map_ref", "map_ref": "$ref:m12"}], _call("mid")), ("mid", "p1", "p2", "m12")),
         ("mid", _doc(_ENTRY, _call("external")), ("external",))],
        [_profile("p1", "a1"), _profile("p2", "b2"), _map("m12", "p1", "p2", "a1", "b2"),
         _reference_child("external", "reference-external-cid-184")])
    refusal = (_PLACEMENT, _LEG, "root")
    planned = plan_authoring_request_v1(request, boomi_client=None, profile="qa_profile", account_id="qa_account")[0]
    causes = {(cause, item.path, item.subject_id) for item in planned.errors for cause in item.cause_codes}
    assert refusal in causes, causes
    assert not any(subject == "mid" for _cause, _path, subject in causes), causes
    with pytest.raises(AuthoringWorkflowError) as excinfo:
        compile_authoring_request_v1(request, boomi_client=None, profile="qa_profile", account_id="qa_account")
    assert refusal in {
        (cause, item.path, item.subject_id) for item in excinfo.value.diagnostics for cause in item.cause_codes
    }, excinfo.value.diagnostics


def test_a_known_childs_unstated_consumer_is_forwarded_too():
    """ARCH-184-r1-04: ENRICH states nothing about what its split reads, so MID, which only
    hands it the documents, owes its caller the same unknown. Controls: the prefix calling an
    unknown child directly is refused, the legacy empty-prefix call is kept, and a No Data
    grandchild receives none of the documents, so its forwarder owes nothing of them."""
    mid, enrich = ("MID", _doc(_ENTRY, _call("ENRICH"))), ("ENRICH", _doc(_ENTRY, _SPLIT, _STOP))
    roots = [("PARENT", _parent(_P2_PREFIX, _call("MID"))), mid, enrich]
    _irs, resolution = _resolve(roots)
    assert resolution.capabilities_by_root["PARENT"].child_entry_contract("$ref:MID").document_requirements == (None,)
    assert (_PLACEMENT, _LEG) in _errors(roots, "PARENT")
    assert (_PLACEMENT, _LEG) in _compile_errors(roots, "PARENT")
    assert _errors(roots, "MID") == []
    # CONTROLS
    assert (_PLACEMENT, _LEG) in _errors([("PARENT", _parent(_P2_PREFIX, _call("EXTERNAL")))], "PARENT")
    assert (_PLACEMENT, _LEG) not in _errors([("PARENT", _parent([], _call("MID"))), mid, enrich], "PARENT")
    nodata = [("PARENT", _parent(_P2_PREFIX, _call("MID"))), ("MID", _doc(_ENTRY, _call("NODATA"))),
              ("NODATA", _NODATA)]
    _irs, resolution = _resolve(nodata)
    assert resolution.capabilities_by_root["PARENT"].child_entry_contract("$ref:MID").document_requirements == ()
    assert (_PLACEMENT, _LEG) not in _errors(nodata, "PARENT")


#: `ChildEntryContractV1` fields that are not requirements a caller discharges, by name and
#: with the reason. Every other field is one, so a pure forwarder must present it.
_CONTRACT_FIELDS_NOT_FORWARDED = {
    "process_ref": "the row's key, naming the child the row describes",
    "entry_form": "the forwarder's own entry, read off its own root",
    "mutated_state": "effect side: what the child may change, applied by each call's own rules",
    "state_known": "effect side: whether the child's state effects are all known",
    "cache_writes_known": "effect side: whether the child's cache writes are all listed",
    "guaranteed_state": (
        "effect side: what every normal completion of the child establishes, applied by each "
        "call's own guarantee rule"),
    "removed_caches": (
        "effect side: which caches the child may empty, applied by each call's own rule for "
        "what a call un-establishes"),
    "required_caches_retain_nothing_it_stored": (
        "effect side: what no completion of the child leaves in the caches it requires, read "
        "by the repetition check for a later run of that child, off its own walk"),
    "unwaited_cache_writes": (
        "effect side: the caches a completion of the child may STILL be writing, unioned into "
        "the caller's own set at the call site whether or not the call is waited"),
    "unwaited_writes_of_an_unknown_cache": (
        "effect side: whether one of those pending writes is to a cache the child cannot name; "
        "the call site then reads it against the CALLER's own observable caches"),
}

#: Per requirement field, one child whose own contract states a value other than the field's
#: default, so what the forwarder presents is compared against something.
_FORWARDING_CASES = {
    "document_requirements": ("CHILD", _CHILD),
    "required_reads": ("NEEDS_K", _NEEDS_K),
    "required_writers": ("BOUND", _BOUND),
    "cache_requirements": ("CACHE_CHILD", _cache_child("$ref:M22")),
    # A No Data child binding its request path to X of the documents it retrieves from a
    # cache nothing in it fills (ARCH-184-r1-05).
    "cache_property_requirements": ("CACHE_CHILD", _doc(_read_cache("$ref:CACHE"), _BOUND_GET, _STOP)),
}

#: What a direct call demands of a child whose entry nothing derives: a consumption of the
#: documents it is handed that nothing states, and nothing else a caller could discharge.
_UNKNOWN_CHILD_REQUIRES = {"document_requirements": (None,)}


def test_every_requirement_field_has_a_forwarding_case():
    """The coverage claim, derived from the contract model: a field added later fails here
    until it has a forwarding case or a named reason it is not a requirement."""
    fields = set(ChildEntryContractV1.model_fields)
    assert set(_CONTRACT_FIELDS_NOT_FORWARDED) <= fields, sorted(set(_CONTRACT_FIELDS_NOT_FORWARDED) - fields)
    assert fields - set(_CONTRACT_FIELDS_NOT_FORWARDED) == set(_FORWARDING_CASES), {
        "without_a_case": sorted(fields - set(_CONTRACT_FIELDS_NOT_FORWARDED) - set(_FORWARDING_CASES)),
        "case_for_no_requirement": sorted(set(_FORWARDING_CASES) - (fields - set(_CONTRACT_FIELDS_NOT_FORWARDED))),
    }
    assert set(_UNKNOWN_CHILD_REQUIRES) <= set(_FORWARDING_CASES)


def test_a_pure_forwarder_presents_every_requirement_of_its_child():
    """ARCH-184-r1-04's invariant: MID only hands its caller's documents to one child, so for
    every requirement field MID's row equals its child's, and an unknown child's is exactly
    what a direct call demands of it. Each case's child states a non-default value."""
    fields = sorted(set(ChildEntryContractV1.model_fields) - set(_CONTRACT_FIELDS_NOT_FORWARDED))
    defaults = {name: ChildEntryContractV1.model_fields[name].default for name in fields}
    drift = {}
    for field, (key, child) in sorted(_FORWARDING_CASES.items()):
        roots = [("PARENT", _parent([], _call("MID"))), ("MID", _parent([], _call(key))), (key, child)]
        _irs, resolution = _resolve(roots)
        own = resolution.capabilities_by_root["MID"].child_entry_contract("$ref:" + key)
        presented = resolution.capabilities_by_root["PARENT"].child_entry_contract("$ref:MID")
        assert getattr(own, field) != defaults[field], (field, key)
        for name in fields:
            if getattr(presented, name) != getattr(own, name):
                drift[(key, name)] = {"child": getattr(own, name), "forwarder": getattr(presented, name)}
    roots = [("PARENT", _parent([], _call("MID"))), ("MID", _parent([], _call("EXTERNAL")))]
    _irs, resolution = _resolve(roots)
    presented = resolution.capabilities_by_root["PARENT"].child_entry_contract("$ref:MID")
    for name in fields:
        expected = _UNKNOWN_CHILD_REQUIRES.get(name, defaults[name])
        if getattr(presented, name) != expected:
            drift[("EXTERNAL", name)] = {"child": expected, "forwarder": getattr(presented, name)}
    assert drift == {}, drift


def test_a_forwarder_to_an_unknown_child_is_refused_a_direct_run_before_mutation(monkeypatch):
    """ARCH-184-r1-04 on the typed build: the forwarder's recorded entry says it consumes
    its caller's documents, so a direct test run of it is refused before anything reaches
    the account. Deploying it without a test run stays available."""
    from test_issue_158_listener_deployment import (
        _AccountSurface,
        _deploy,
        _error_codes,
        _reference_child,
        _request,
        _typed_build,
        _unit,
    )
    from boomi_mcp.categories.integration_builder import _BUILD_REGISTRY

    applied, _boundary = _typed_build(_request(
        [_unit(_doc(dict(_ENTRY, label="E184 forwarder"), _call("external")), ("external",),
               key="mid", name="E184 Forwarder")],
        [_reference_child("external", "reference-external-cid-184")],
    ))
    build_id = applied["build_id"]
    record = _BUILD_REGISTRY[build_id]["authoring"]["standalone_entry"]["mid"]
    assert record["consumes_caller_documents"] is True, record
    code = PROCESS_IR_CAPABILITY_ENTRY_CONTEXT_UNSUPPORTED
    refused = _deploy(build_id, dry_run=True, run_test=True)
    assert _error_codes(refused) == [code], refused
    assert refused["errors"][0]["details"]["requirements"] == ["caller_documents"]
    assert code not in _error_codes(_deploy(build_id, dry_run=True))
    surface = _AccountSurface().install(monkeypatch)
    real = _deploy(build_id, dry_run=False, run_test=True)
    assert _error_codes(real) == [code], real
    assert (surface.calls, surface.package_boundary, surface.reads) == ([], [], [])


# ---------------------------------------------------------------------------
# Correction batch 18, pre-commit verification: what voids a caller's cache seed
# ---------------------------------------------------------------------------

#: Consumers of a caller-filled cache that state NO profile. Each records a
#: ``(cache, None)`` requirement: a call that declares no input consumes the documents in
#: a way nothing states (D2), and so does a data process.
_STATES_NO_PROFILE = {
    "an_undeclared_input_call": {"kind": "connector_call", "operation_ref": "$ref:PATCH_UNDECLARED"},
    "a_data_process": _SPLIT,
}
#: Typed consumers of the same read, each requiring P2, with the pointer each is refused at.
_STATES_A_PROFILE = {
    "a_declared_input_call": (_PATCH, "/operation_ref"),
    "a_map": ({"kind": "map_ref", "map_ref": "$ref:M22"}, "/map_ref"),
    "a_profile_source": ({"kind": "set_ddp", "name": "S", "source_values": [
        {"value_type": "profile", "profile_ref": "$ref:P2", "profile_type": "json",
         "element_id": "3", "element_name": "id"}]}, "/source_values/0/profile_ref"),
}


def _two_consumers(first, second):
    """A No Data child: one leg reads CACHE and runs ``first``, the next runs ``second``."""
    return _legs({"steps": [_read_cache("$ref:CACHE"), first], "terminal": _STOP},
                 {"steps": [_read_cache("$ref:CACHE"), second], "terminal": _STOP})


@pytest.mark.parametrize("typed", sorted(_STATES_A_PROFILE))
@pytest.mark.parametrize("unstated", sorted(_STATES_NO_PROFILE))
def test_a_consumer_that_states_no_profile_does_not_void_the_callers_cache_seed(unstated, typed):
    """Amendment 1 rule 6: the seed is what the cache's consumers NAME.

    A consumer naming no profile states nothing a caller could store or fail to store —
    every call skips it for exactly that reason — so it is not a disagreement. Voiding the
    seed refused a called child's map, profile source or declared-input call beside such a
    consumer of the same cache, while the identical in-process graph compiled and the
    caller staged exactly the declared profile."""
    consumer, _tail = _STATES_A_PROFILE[typed]
    other = _STATES_NO_PROFILE[unstated]
    roots = [("PARENT", _stage_then_call("CACHE_CHILD")),
             ("CACHE_CHILD", _two_consumers(other, consumer))]
    _irs, resolution = _resolve(roots)
    assert resolution.capabilities_by_root["CACHE_CHILD"].caller_cache_contents == (("$ref:CACHE", "$ref:P2"),)
    for key in ("PARENT", "CACHE_CHILD"):
        assert _errors(roots, key) == [], key
        assert _compile_errors(roots, key) == [], key
    # The in-process twin, admitted before this batch and still admitted.
    in_process = _legs({"steps": [_GETP1, _MAP], "terminal": _put("$ref:CACHE")},
                       {"steps": [_read_cache("$ref:CACHE"), other], "terminal": _STOP},
                       {"steps": [_read_cache("$ref:CACHE"), consumer], "terminal": _STOP})
    assert _errors([("PARENT", in_process)], "PARENT") == []


@pytest.mark.parametrize("typed", sorted(_STATES_A_PROFILE))
def test_the_seed_still_needs_one_named_profile_and_the_writes_that_store_it(typed):
    """The refusals that stand: a caller whose writes store another profile is refused at
    its own call, and consumers of one cache naming DIFFERENT profiles seed nothing, so no
    caller could satisfy both and the child keeps its own refusal."""
    consumer, tail = _STATES_A_PROFILE[typed]
    wrong_profile = [("PARENT", _legs({"steps": [_GETP1], "terminal": _put("$ref:CACHE")},
                                      {"steps": [], "terminal": _call("CACHE_CHILD")})),
                     ("CACHE_CHILD", _two_consumers(_STATES_NO_PROFILE["a_data_process"], consumer))]
    assert (PROCESS_IR_SEMANTIC_PROFILE_MISMATCH, "/body/steps/0/legs/1/terminal/process_ref") in _errors(
        wrong_profile, "PARENT")
    disagreeing = [("PARENT", _stage_then_call("CACHE_CHILD")),
                   ("CACHE_CHILD", _two_consumers(_MAP, consumer))]
    refused = {(code, path) for code, path in _errors(disagreeing, "CACHE_CHILD")}
    assert (PROCESS_IR_SEMANTIC_PROFILE_MISMATCH, "/body/steps/0/legs/1/steps/1" + tail) in refused, refused


def test_the_unstated_consumer_rule_is_load_bearing(monkeypatch):
    """Non-vacuity: with the pre-batch seed rule in its place — any consumer naming no
    profile voids the seed — the called child is refused at its own consumer again."""
    from boomi_mcp.authoring import process_ir_effects
    from boomi_mcp.compiler.process_ir.contracts import component_identity

    def voided_by_an_unstated_consumer(requirements, symbols):
        by_cache = {}
        for cache_ref, profile_ref in requirements:
            by_cache.setdefault(cache_ref, []).append(profile_ref)
        seeds = []
        for cache_ref, refs in sorted(by_cache.items()):
            if None in refs:
                continue
            identities = {component_identity(process_ir_effects._symbol(symbols, ref)) or ref for ref in refs}
            if len(identities) == 1:
                seeds.append((cache_ref, sorted(refs)[0]))
        return tuple(seeds)

    roots = [("PARENT", _stage_then_call("CACHE_CHILD")),
             ("CACHE_CHILD", _two_consumers(_STATES_NO_PROFILE["an_undeclared_input_call"], _PATCH))]
    assert _errors(roots, "CACHE_CHILD") == []
    monkeypatch.setattr(process_ir_effects, "_caller_cache_seeds", voided_by_an_unstated_consumer)
    assert (PROCESS_IR_SEMANTIC_PROFILE_MISMATCH, "/body/steps/0/legs/1/steps/1/operation_ref") in _errors(
        roots, "CACHE_CHILD")
