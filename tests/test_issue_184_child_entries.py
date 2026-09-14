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
        sym("M12", "M12", "transform.map", input_profile_ref="$ref:P1", output_profile_ref="$ref:P2"),
        sym("M22", "M22", "transform.map", input_profile_ref="$ref:P2", output_profile_ref="$ref:P2"),
        sym("P1", "PROFILE-ONE", "profile.json"),
        sym("P2", "PROFILE-TWO", "profile.json"),
        sym("CACHE", "CACHE", "documentcache"),
        sym("CACHE_ALIAS", "CACHE", "documentcache"),
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
    roots = [("LOOP_A", _parent(_P2_PREFIX, _call("LOOP_B"))), ("LOOP_B", _parent(_P2_PREFIX, _call("LOOP_A")))]
    _irs, resolution = _resolve(roots)
    assert resolution.capabilities_by_root["LOOP_A"].child_entry_contract("$ref:LOOP_B").entry_form == "unknown"
    for key in ("LOOP_A", "LOOP_B"):
        assert (_PLACEMENT, _LEG) in _errors(roots, key)


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
    """Native work, then a Decision, then a call whose arm authors no step.

    The call keeps the legacy empty-prefix placement (amendment 1 rule 4): the #141
    capture attests `decision -> true -> processcall` after a leg step, so even a
    child with no derivable contract is not refused. The Decision erases no
    obligation (rule 3): a known child's contract is discharged at that call."""
    unknown = [("PARENT", _parent(_P2_PREFIX, _decision(_call("EXTERNAL"))))]
    assert (_PLACEMENT, _LEG + "/true_arm/terminal") not in _errors(unknown, "PARENT")
    mismatched = [("PARENT", _parent(_P2_PREFIX, _decision(_call("CHILD_P1")))), ("CHILD_P1", _CHILD_P1)]
    assert (PROCESS_IR_SEMANTIC_PROFILE_MISMATCH, _LEG + "/true_arm/terminal/process_ref") in _errors(mismatched, "PARENT")


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


def test_a_no_data_child_run_per_document_may_not_change_the_state_it_requires():
    def parent(child_key):
        return _doc(_ENTRY, {"kind": "branch", "legs": [
            {"steps": [_SET_K], "terminal": _STOP},
            {"steps": [], "terminal": _call(child_key)},
        ]})

    refused = [("PARENT", parent("MUTATES_K")), ("MUTATES_K", _MUTATES_K)]
    assert (_PLACEMENT, "/body/steps/1/legs/1/terminal") in _errors(refused, "PARENT")
    stable = [("PARENT", parent("NEEDS_K")), ("NEEDS_K", _NEEDS_K)]
    assert (_PLACEMENT, "/body/steps/1/legs/1/terminal") not in _errors(stable, "PARENT")


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
    prefix key, marking a split as keeping document properties, and dropping a
    document-emission row. The three revision rows read their authorities at call
    time, so each perturbation is visible, and the baseline returns afterwards.
    """
    from types import MappingProxyType

    from boomi_mcp.authoring import contract as authoring_contract
    from boomi_mcp.models import process_ir_document_semantics as emission

    baseline = authoring_contract._compiler_revision()
    with monkeypatch.context() as patched:
        patched.setattr(lineage, "process_call_prefix_admitted", lambda *args: True)
        assert authoring_contract._compiler_revision() != baseline
    with monkeypatch.context() as patched:
        cells = dict(lineage.PROPERTY_SURVIVAL_V1)
        cells[("data_process", "split_documents")] = "survives"
        patched.setattr(lineage, "PROPERTY_SURVIVAL_V1", MappingProxyType(cells))
        assert authoring_contract._compiler_revision() != baseline
    with monkeypatch.context() as patched:
        rows = dict(emission.DOCUMENT_EMISSION_V1)
        rows.pop("stop")
        patched.setattr(emission, "DOCUMENT_EMISSION_V1", MappingProxyType(rows))
        assert authoring_contract._compiler_revision() != baseline
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
    baseline = authoring_contract._compiler_revision()
    perturbations = (
        (integration_builder, "declared_bindings_for_components", lambda components, conflict_policy="reuse": {}),
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
        # Pre-commit verification of batch 7: which spec describes a reference.
        (materialization, "_fact_source", lambda component, bindings, writers, writer_for: component),
        # Stage-2 review round r7: a writer's unstated fact leaves the reference its own.
        (materialization, "_overlay", lambda stated, own: tuple(stated)),
        # Stage-2 review round r5: the write-conflict refusal and the canonical effect comparison.
        (integration_builder, "component_write_conflicts", lambda components: {}),
        (integration_builder, "component_writes_existing", lambda comp: False),
        (integration_builder, "apply_writes_component_config", lambda comp, conflict_policy: True),
        (integration_builder, "_binds_as_metadata_only_connector_update", lambda component_type, config: False),
        (integration_builder, "smart_merge_would_change", lambda config: False),
        (process_ir_effects, "_canonical_effect", lambda effect, canonical: effect),
    )
    for module, name, replacement in perturbations:
        with monkeypatch.context() as patched:
            patched.setattr(module, name, replacement)
            # Non-vacuity: a replacement the oracle cannot call would read "unavailable" and move
            # the revision without changing any verdict.
            perturbed = authoring_contract._compiler_revision_payload()
            assert all(perturbed[row] != "unavailable" for row in ("component_identity", "child_forwarding")), name
            assert authoring_contract._compiler_revision() != baseline, name
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
