"""#184 correction batch 18: the execution state a call hands its child and takes back.

Architect evaluation 1, findings 5 and 6 (ledger rows ARCH-184-r1-05 and -06, plan
corrections C20a and C21a). The child entry contract is a projection of the child's own
lineage walk at its boundary, over every component of its state. Expected codes, pointers
and verdicts come from amendment 1 rules 7 and 8, amendment 3 §7-§8 and the archived
captures, never from this implementation's output:

- `ddp-cache-1`: a passthrough child retrieves the documents its caller cached, and their
  document properties compose its request paths. `cap184-shared-cache`: a No Data child
  retrieves them too; the property half of that form is owed to live QA.
- `cap184-dpp-both-ways`: a waited, abort-on-error child's process property reaches the
  parent's later Branch leg, for both entry forms.
"""

from __future__ import annotations

import sys
from pathlib import Path

import pytest
from pydantic import ValidationError

_HERE = Path(__file__).resolve().parent
_ROOT = _HERE.parent
for _p in (str(_ROOT), str(_ROOT / "src"), str(_HERE)):
    if _p not in sys.path:
        sys.path.insert(0, _p)

from boomi_mcp.authoring.process_ir_effects import (  # noqa: E402
    resolve_process_ir_effect_declarations,
)
from boomi_mcp.compiler.process_ir.semantic_validation import lineage  # noqa: E402
from boomi_mcp.compiler.process_ir.semantic_validation.context import (  # noqa: E402
    prepare_validation_context,
)
from boomi_mcp.compiler.process_ir.semantic_validation.contracts import (  # noqa: E402
    DEFAULT_VALIDATION_CAPABILITIES,
    ChildEntryContractV1,
)
from boomi_mcp.compiler.process_ir.semantic_validation.pipeline import (  # noqa: E402
    validate_process_ir,
)
from boomi_mcp.errors import (  # noqa: E402
    PROCESS_IR_CAPABILITY_PROCESS_CALL_PLACEMENT_UNSUPPORTED,
    PROCESS_IR_SEMANTIC_DYNAMIC_PATH_DDP_NOT_ESTABLISHED,
    PROCESS_IR_SEMANTIC_DYNAMIC_PATH_NO_DYNAMIC_SEGMENT,
    PROCESS_IR_SEMANTIC_LINEAGE_CACHE_WRITER_MISSING,
    PROCESS_IR_SEMANTIC_LINEAGE_DDP_SCOPE_INVALID,
    PROCESS_IR_SEMANTIC_LINEAGE_EXTERNAL_WRITER_ASSUMED,
    PROCESS_IR_SEMANTIC_LINEAGE_PROPERTY_READ_BEFORE_WRITE,
    PROCESS_IR_SEMANTIC_SIDE_EFFECT_ORDERING_UNSAFE,
)
from boomi_mcp.models.process_ir import parse_process_ir_v1  # noqa: E402

# The #158 deployment suite's two autouse fixtures, imported so they apply to this module
# too: the build registry is restored after each test, and the metadata pager is stubbed.
from test_issue_158_listener_deployment import (  # noqa: E402,F401
    _no_live_metadata_queries,
    _registry_restored,
)
from test_issue_184_child_entries import (  # noqa: E402
    _BOUND,
    _BOUND_GET,
    _CHILD,
    _DYNAMIC_X,
    _ENTRY,
    _MSG,
    _NEEDS_K,
    _NODATA,
    _P2_PREFIX,
    _SET_K,
    _STATIC_X,
    _STOP,
    _cache_child,
    _call,
    _compile_errors,
    _decision,
    _doc,
    _errors,
    _legs,
    _parent,
    _resolve,
    _stage_then_call,
    _symbols,
)

_NOT_ESTABLISHED = PROCESS_IR_SEMANTIC_DYNAMIC_PATH_DDP_NOT_ESTABLISHED
_READ_BEFORE_WRITE = PROCESS_IR_SEMANTIC_LINEAGE_PROPERTY_READ_BEFORE_WRITE

_READ = {"kind": "cache_get", "cache_ref": "$ref:CACHE"}
_PUT = {"kind": "cache_put", "cache_ref": "$ref:CACHE"}
_GET = {"kind": "connector_call", "operation_ref": "$ref:GET"}
_READS_X = {"kind": "set_dpp", "name": "Y", "source_values": [{"value_type": "ddp", "property_name": "X"}]}
#: A staging leg: documents carrying X, composed from a process property, go to the cache.
_STAGES_X = {"steps": [_GET, _DYNAMIC_X], "terminal": _PUT}
#: The CONTENT channel's twin of `_STAGES_X`: documents of a stated profile go to the same
#: cache, for a child whose map states what it consumes from it.
_STAGES_P2 = {"steps": [{"kind": "connector_call", "operation_ref": "$ref:GETP1"},
                        {"kind": "map_ref", "map_ref": "$ref:M12"}], "terminal": _PUT}
_TYPED_CACHE_CHILD = _legs(
    {"steps": [_READ, {"kind": "map_ref", "map_ref": "$ref:M22"}], "terminal": _STOP},
    {"steps": [_MSG], "terminal": _STOP})

#: Each child uses the cached X after reading the cache first: ``(child, bound, where it
#: reports X unproved when run alone)``.
_USES = {
    "no_data_bound": (_doc(_READ, _BOUND_GET, _STOP), True, "/body/steps/1/path_binding"),
    "no_data_read": (_doc(_READ, _READS_X, _STOP), False, "/body/steps/1"),
    "passthrough_bound": (_doc(_ENTRY, _READ, _BOUND_GET, _STOP), True, "/body/steps/2/path_binding"),
    "passthrough_read": (_doc(_ENTRY, _READ, _READS_X, _STOP), False, "/body/steps/2"),
}


def _stage_and_call(*legs, child="CACHE_CHILD"):
    """A scheduled parent: ``legs`` run first, then a last leg calls ``child`` with no prefix."""
    return _legs(*legs, {"steps": [], "terminal": _call(child)})


def _row(roots, parent, child):
    _irs, resolution = _resolve(roots)
    return resolution.capabilities_by_root[parent].child_entry_contract("$ref:" + child)


# ---------------------------------------------------------------------------
# ARCH-184-r1-05: the cached document properties and their writers cross the boundary
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("use", sorted(_USES))
def test_a_called_child_binds_a_path_to_a_cached_property_its_caller_stored(use):
    """Amendment 3 §7-§8: both forms receive the execution's cache facts, and the cached
    documents keep the properties they were stored with. The in-process twin of each
    graph compiles; so does the call, for a bound path and an ordinary read alike."""
    child, bound, _pointer = _USES[use]
    roots = [("PARENT", _stage_and_call(_STAGES_X)), ("CACHE_CHILD", child)]
    _irs, resolution = _resolve(roots)
    row = resolution.capabilities_by_root["PARENT"].child_entry_contract("$ref:CACHE_CHILD")
    assert row.cache_property_requirements == (("$ref:CACHE", "X", None, bound),), row
    assert resolution.capabilities_by_root["CACHE_CHILD"].caller_cache_cohorts == (("$ref:CACHE", "X"),)
    for key in ("PARENT", "CACHE_CHILD"):
        assert _errors(roots, key) == [], key
        assert _compile_errors(roots, key) == [], key


#: What reaches the cache before the call, and which leg holds the call.
_STAGINGS = {
    "no_writer_of_x": (({"steps": [_GET], "terminal": _PUT},), 1),
    "a_literal_x": (({"steps": [_GET, _STATIC_X], "terminal": _PUT},), 1),
    "two_writes_disagree": ((_STAGES_X, {"steps": [_GET], "terminal": _PUT}), 2),
    "a_child_may_append": ((_STAGES_X, {"steps": [], "terminal": _call("EXTERNAL")}), 2),
}


@pytest.mark.parametrize("staging", sorted(_STAGINGS))
@pytest.mark.parametrize("use", sorted(_USES))
def test_a_call_proves_a_cached_property_against_every_write_that_reached_the_cache(use, staging):
    """Every cohort reaching the call meets: a write without X, a second write that
    disagrees, and what an unknown child may append all leave X unproved. A bound path
    also needs each cached writer to compose it (C19's pointer, the call's
    `/process_ref`); an ordinary read needs X established, whoever wrote it."""
    child, bound, _pointer = _USES[use]
    legs, call_leg = _STAGINGS[staging]
    call = "/body/steps/0/legs/{0}/terminal".format(call_leg)
    if not bound:
        expected = [] if staging == "a_literal_x" else [(_READ_BEFORE_WRITE, call)]
    elif staging == "a_literal_x":
        expected = [(PROCESS_IR_SEMANTIC_DYNAMIC_PATH_NO_DYNAMIC_SEGMENT, call + "/process_ref")]
    else:
        expected = [(_NOT_ESTABLISHED, call + "/process_ref")]
    assert _errors([("PARENT", _stage_and_call(*legs)), ("CACHE_CHILD", child)], "PARENT") == expected


@pytest.mark.parametrize("use", sorted(_USES))
def test_an_outside_writer_of_the_cache_leaves_a_cached_property_unproved(use):
    """Base D6: an outside writer's documents carry nothing this process can see, so the
    cache a verified external writer also fills proves no cached property at a call."""
    from boomi_mcp.models.authoring_workflow import (
        ProcessIREffectDeclarationsV1,
        ProcessIRExternalWriterDeclarationV1,
    )

    child, bound, _pointer = _USES[use]
    outside = {"steps": [dict(_READ, external_writer=True), _MSG], "terminal": _STOP}
    parsed = [
        ("PARENT", parse_process_ir_v1(_stage_and_call(_STAGES_X, outside))),
        ("CACHE_CHILD", parse_process_ir_v1(child)),
    ]
    declarations = ProcessIREffectDeclarationsV1(external_writers=(
        ProcessIRExternalWriterDeclarationV1(cache_ref="$ref:CACHE"),))
    resolution = resolve_process_ir_effect_declarations(
        parsed, declarations, _symbols(), [], child_roots={"$ref:" + key: ir for key, ir in parsed})
    assert resolution.ok, resolution.findings
    report = validate_process_ir(
        dict(parsed)["PARENT"], _symbols(),
        capabilities=resolution.capabilities_by_root["PARENT"] or DEFAULT_VALIDATION_CAPABILITIES)
    call = "/body/steps/0/legs/2/terminal"
    expected = (_NOT_ESTABLISHED, call + "/process_ref") if bound else (_READ_BEFORE_WRITE, call)
    assert [(item.code, item.path) for item in report.errors] == [expected]


@pytest.mark.parametrize("use", sorted(_USES))
def test_the_same_child_run_alone_keeps_its_own_refusal(use):
    """Nothing seeds a caller cohort for a process nobody calls (amendment 3's matrix row
    for a standalone root), so its use of the cached X stays refused where it is."""
    child, bound, pointer = _USES[use]
    irs, _resolution = _resolve([("PARENT", _stage_and_call(_STAGES_X)), ("CACHE_CHILD", child)])
    standalone = validate_process_ir(irs["CACHE_CHILD"], _symbols())
    code = _NOT_ESTABLISHED if bound else _READ_BEFORE_WRITE
    assert (code, pointer) in [(item.code, item.path) for item in standalone.errors]


def test_the_caller_cohort_never_becomes_a_current_document_overlay():
    """Amendment 3 §7: current-only carry is admitted only for one current and one cached
    document, and a caller's cache proves no count. X the child writes on its own
    document before the retrieve is therefore not carried, and the caller still owes it."""
    own_x_first = _legs({"steps": [_DYNAMIC_X, _READ, _BOUND_GET], "terminal": _STOP},
                        {"steps": [_MSG], "terminal": _STOP})
    staged = [("PARENT", _stage_and_call(_STAGES_X)), ("CACHE_CHILD", own_x_first)]
    assert _row(staged, "PARENT", "CACHE_CHILD").cache_property_requirements == (
        ("$ref:CACHE", "X", None, True),)
    assert _errors(staged, "CACHE_CHILD") == []
    unstaged = [("PARENT", _stage_and_call({"steps": [_GET], "terminal": _PUT})), ("CACHE_CHILD", own_x_first)]
    assert _errors(unstaged, "PARENT") == [(_NOT_ESTABLISHED, "/body/steps/0/legs/1/terminal/process_ref")]


def test_a_forwarder_inherits_its_childs_cached_property_requirement():
    """MID writes nothing to the cache, so what its child needs of the cache is MID's own
    caller's obligation, carried up and proved at that caller's call."""
    mid = _legs({"steps": [_MSG], "terminal": _STOP}, {"steps": [], "terminal": _call("CACHE_CHILD")})
    child = _USES["no_data_bound"][0]
    roots = [("PARENT", _stage_and_call(_STAGES_X, child="MID")), ("MID", mid), ("CACHE_CHILD", child)]
    assert _row(roots, "PARENT", "MID").cache_property_requirements == (("$ref:CACHE", "X", None, True),)
    for key in ("PARENT", "MID", "CACHE_CHILD"):
        assert _errors(roots, key) == [], key
    missing = [("PARENT", _stage_and_call({"steps": [_GET], "terminal": _PUT}, child="MID")),
               ("MID", mid), ("CACHE_CHILD", child)]
    assert _errors(missing, "PARENT") == [(_NOT_ESTABLISHED, "/body/steps/0/legs/1/terminal/process_ref")]


def test_a_no_data_child_run_per_document_may_not_append_to_the_cache_it_needs_properties_of():
    """Amendment 1 rule 8: under a passthrough parent the child may run once per arriving
    document, and a later run's retrieve may find what an earlier run appended. Under a
    scheduled parent it runs once, and the same child is admitted."""
    appends = _legs({"steps": [_READ, _BOUND_GET], "terminal": _STOP},
                    {"steps": [_GET, _DYNAMIC_X], "terminal": _PUT})
    per_document = _doc(_ENTRY, {"kind": "branch", "legs": [
        {"steps": [_DYNAMIC_X], "terminal": _PUT}, {"steps": [], "terminal": _call("CACHE_CHILD")}]})
    placement = (PROCESS_IR_CAPABILITY_PROCESS_CALL_PLACEMENT_UNSUPPORTED, "/body/steps/1/legs/1/terminal")
    assert placement in _errors([("PARENT", per_document), ("CACHE_CHILD", appends)], "PARENT")
    assert _errors([("PARENT", _stage_and_call(_STAGES_X)), ("CACHE_CHILD", appends)], "PARENT") == []


def test_the_caller_cohort_seed_is_load_bearing(monkeypatch):
    """Non-vacuity: with an unknown cohort in the caller cohort's place, the called child
    is refused at its own binding again."""
    child, _bound, pointer = _USES["no_data_bound"]
    roots = [("PARENT", _stage_and_call(_STAGES_X)), ("CACHE_CHILD", child)]
    assert _errors(roots, "CACHE_CHILD") == []
    monkeypatch.setattr(lineage, "_caller_cohort", lambda names: lineage.UNKNOWN_COHORT)
    assert (_NOT_ESTABLISHED, pointer) in _errors(roots, "CACHE_CHILD")


def test_every_cohort_fact_crosses_the_boundary():
    """Coverage over the facts an executing cache write freezes (`_Cohort._fields`) plus
    the content's profile: each one crosses to a called child or is withheld for a stated
    reason, and a fact added to the cohort fails here until it is placed."""
    key = ("ddp", "X")
    seeded = lineage._caller_cohort(("X",))
    _irs, typed = _resolve([("PARENT", _stage_then_call("CACHE_CHILD")), ("CACHE_CHILD", _cache_child("$ref:M22"))])
    crossing = {
        # the properties the child's contract names; proved at each call above
        "guaranteed": seeded.guaranteed == frozenset({key}),
        # the caller writes them, and each call checks every writer that reached the cache
        "alternatives": seeded.alternatives == frozenset({(key, lineage.CALLER_WRITER)}),
        # withheld: a caller may store properties the child names nowhere
        "possible": seeded.possible is None,
        # withheld: a caller's cache proves no count, so no current-document overlay
        "count": seeded.count == lineage.COUNT_UNKNOWN,
        # amendment 1 rule 6, carried by `cache_requirements`
        "content": typed.capabilities_by_root["CACHE_CHILD"].caller_cache_contents == (("$ref:CACHE", "$ref:P2"),),
    }
    assert set(crossing) == set(lineage._Cohort._fields) | {"content"}
    assert all(crossing.values()), crossing


# ---------------------------------------------------------------------------
# The contract is the child's walk at its boundary: component x side, pinned both ways
# ---------------------------------------------------------------------------

_SIDES = ("requirement", "possible_effect", "guaranteed_effect")

#: Every cell of the child's state at its boundary, by `_State` component and side: the
#: contract fields that carry it, or why nothing crosses.
_BOUNDARY = {
    ("document", "requirement"): ("document_requirements", "required_writers", "required_reads"),
    ("document", "possible_effect"):
        "withheld: a child's document properties never land on its caller's sibling copies (rule 7)",
    ("document", "guaranteed_effect"):
        "withheld: rule 7 again, and the contract refuses a document property as a guarantee",
    ("execution", "requirement"): ("required_reads",),
    # `mutated_state` records a write and a removal as one fact, so the removals are
    # named beside it: only they un-establish the cache at a call (amendment 3 §8).
    ("execution", "possible_effect"): ("mutated_state", "removed_caches", "state_known"),
    ("execution", "guaranteed_effect"): ("guaranteed_state",),
    ("content", "requirement"): ("cache_requirements",),
    ("content", "possible_effect"): ("cache_writes_known",),
    ("content", "guaranteed_effect"):
        "withheld: what a child stores stays an unknown possibility at every later read",
    ("cohorts", "requirement"): ("cache_property_requirements",),
    ("cohorts", "possible_effect"):
        "carried by the content cells: every cache a child may write gets an unknown cohort",
    ("cohorts", "guaranteed_effect"):
        "withheld: the properties a child stores stay an unknown possibility too",
}

_WRITER = [("PARENT", _legs({"steps": [], "terminal": _call("WRITER", wait=True, abort_on_error=True)},
                            {"steps": [_MSG], "terminal": _STOP})),
           ("WRITER", _doc(_decision(_STOP) | {"true_arm": {"steps": [_SET_K], "terminal": _STOP},
                                               "false_arm": {"steps": [_SET_K], "terminal": _STOP}}))]

#: A child that fills the cache on one leg and empties it on the next, called by a parent
#: that reads the cache afterwards (amendment 3 §8).
_REMOVES_THE_CACHE = [
    ("PARENT", _legs({"steps": [], "terminal": _call("WRITER", wait=True, abort_on_error=True)},
                     {"steps": [_READ, _MSG], "terminal": _STOP})),
    ("WRITER", _legs({"steps": [_GET], "terminal": _PUT},
                     {"steps": [], "terminal": {"kind": "cache_remove", "cache_ref": "$ref:CACHE"}})),
]

#: One derivation per contract field that makes it differ from its default.
_NON_DEFAULT = {
    "removed_caches": (_REMOVES_THE_CACHE, "WRITER"),
    "document_requirements": ([("PARENT", _parent(_P2_PREFIX, _call("CHILD"))), ("CHILD", _CHILD)], "CHILD"),
    "required_writers": ([("PARENT", _parent([_DYNAMIC_X], _call("BOUND"))), ("BOUND", _BOUND)], "BOUND"),
    "required_reads": ([("PARENT", _legs({"steps": [_SET_K], "terminal": _STOP},
                                         {"steps": [], "terminal": _call("NEEDS_K")})),
                        ("NEEDS_K", _NEEDS_K)], "NEEDS_K"),
    "cache_requirements": ([("PARENT", _stage_then_call("CACHE_CHILD")),
                            ("CACHE_CHILD", _cache_child("$ref:M22"))], "CACHE_CHILD"),
    "cache_property_requirements": ([("PARENT", _stage_and_call(_STAGES_X)),
                                     ("CACHE_CHILD", _USES["no_data_bound"][0])], "CACHE_CHILD"),
    "mutated_state": (_WRITER, "WRITER"),
    "guaranteed_state": (_WRITER, "WRITER"),
    "state_known": (_WRITER, "WRITER"),
    "cache_writes_known": (_WRITER, "WRITER"),
}


def test_the_child_contract_carries_every_lattice_component():
    """The structural fix for `child-contract-omits-cache-content`: the coverage table is
    derived from the lattice (`_State` components) on one side and the contract's fields on
    the other, so a component or a field nobody placed fails here. Every carried field has
    a derivation that makes it non-default, so a field no derivation fills fails too."""
    assert {component for component, _side in _BOUNDARY} == set(lineage._State.__slots__)
    assert set(_BOUNDARY) == {(component, side) for component in lineage._State.__slots__ for side in _SIDES}
    carried = {field for cell in _BOUNDARY.values() if isinstance(cell, tuple) for field in cell}
    identity = {"process_ref", "entry_form"}
    assert carried == set(ChildEntryContractV1.model_fields) - identity
    assert set(_NON_DEFAULT) == carried
    for field, (roots, child) in sorted(_NON_DEFAULT.items()):
        row = _row(roots, "PARENT", child)
        assert getattr(row, field) != ChildEntryContractV1.model_fields[field].default, (field, row)


# ---------------------------------------------------------------------------
# ARCH-184-r1-06: guarantees, and the exit meet they are read from
# ---------------------------------------------------------------------------

_READS_K_LATER = {"steps": [{"kind": "set_dpp", "name": "OUT", "source_values": [
    {"value_type": "dpp", "property_name": "K"}]}], "terminal": _STOP}
_READS_D_LATER = {"steps": [{"kind": "set_dpp", "name": "OUT", "source_values": [
    {"value_type": "ddp", "property_name": "D"}]}], "terminal": _STOP}
_WAITS_AND_ABORTS = {"wait": True, "abort_on_error": True}
_LATER_READ = "/body/steps/0/legs/1/steps/0"


def _arms(true_steps, false_steps, true_terminal=None):
    decision = _decision(_STOP)
    decision["true_arm"] = {"steps": list(true_steps), "terminal": true_terminal or _STOP}
    decision["false_arm"] = {"steps": list(false_steps), "terminal": _STOP}
    return decision


def _call_then_read(later=_READS_K_LATER, passthrough=False, **call):
    """A parent whose first Branch leg calls WRITER and whose second reads what it wrote."""
    body = {"kind": "branch", "legs": [{"steps": [], "terminal": _call("WRITER", **call)}, later]}
    return _doc(_ENTRY, body) if passthrough else _doc(body)


_SETS_K = _doc(_arms([_SET_K], [_SET_K]))
_SETS_D = _doc(_arms([dict(_STATIC_X, name="D")], [dict(_STATIC_X, name="D")]))
_ENDS_IN_A_CALL = _doc(_arms([], [_SET_K], true_terminal=_call("NODATA", **_WAITS_AND_ABORTS)))


@pytest.mark.parametrize("child", [
    pytest.param(_SETS_K, id="no_data"),
    pytest.param(_doc(_ENTRY, _arms([_SET_K], [_SET_K])), id="passthrough"),
])
def test_a_waited_abort_on_error_child_guarantees_its_writes_to_a_later_leg(child):
    """Capture `cap184-dpp-both-ways`: both parents call with wait=true and
    abort_on_error=true, and the parent's post-call Branch leg carries the child-set
    process property, for both forms (amendment 1 rule 7, amendment 3 §8)."""
    roots = [("PARENT", _call_then_read(**_WAITS_AND_ABORTS)), ("WRITER", child)]
    assert _row(roots, "PARENT", "WRITER").guaranteed_state == (("dpp", "K"),)
    assert _errors(roots, "PARENT") == []
    assert _compile_errors(roots, "PARENT") == []


#: ``(parent, child, guaranteed_state, errors)``: none of these establishes the later read.
_UNGUARANTEED = {
    # the call returns at once; the child may still be running
    "no_wait": (_call_then_read(wait=False, abort_on_error=True), _SETS_K, (("dpp", "K"),),
                {(_READ_BEFORE_WRITE, _LATER_READ), (PROCESS_IR_SEMANTIC_SIDE_EFFECT_ORDERING_UNSAFE, _LATER_READ)}),
    # the parent continues after a child that failed part-way
    "no_abort": (_call_then_read(wait=True, abort_on_error=False), _SETS_K, (("dpp", "K"),),
                 {(_READ_BEFORE_WRITE, _LATER_READ)}),
    "one_arm_only": (_call_then_read(**_WAITS_AND_ABORTS), _doc(_arms([_SET_K], [_MSG])), (),
                     {(_READ_BEFORE_WRITE, _LATER_READ)}),
    # the exit meet: a terminal call completes its path without K
    "an_arm_ends_in_a_call": (_call_then_read(**_WAITS_AND_ABORTS), _ENDS_IN_A_CALL, (),
                              {(_READ_BEFORE_WRITE, _LATER_READ)}),
    # a passthrough parent proves no count, so the child may run no times at all
    "several_documents": (_call_then_read(passthrough=True, **_WAITS_AND_ABORTS), _SETS_K, (("dpp", "K"),),
                          {(_READ_BEFORE_WRITE, "/body/steps/1/legs/1/steps/0")}),
    # rule 7: a child's document property never lands on the parent's copies
    "a_document_property": (_call_then_read(later=_READS_D_LATER, **_WAITS_AND_ABORTS), _SETS_D, (),
                            {(_READ_BEFORE_WRITE, _LATER_READ)}),
}


@pytest.mark.parametrize("case", sorted(_UNGUARANTEED))
def test_a_guarantee_needs_a_waited_abort_on_error_call_that_provably_runs_a_writing_child(case):
    parent, child, guaranteed, expected = _UNGUARANTEED[case]
    roots = [("PARENT", parent), ("WRITER", child), ("NODATA", _NODATA)]
    assert _row(roots, "PARENT", "WRITER").guaranteed_state == guaranteed
    assert set(_errors(roots, "PARENT")) == expected
    assert set(_compile_errors(roots, "PARENT")) == expected


def test_a_terminal_call_completes_its_path_in_the_must_set():
    """Amendment 1 rule 7's exit meet: one arm ends in a call to a child that writes
    nothing, so K is not established on every normal completion."""
    walk = lineage.walk_lineage(prepare_validation_context(parse_process_ir_v1(_ENDS_IN_A_CALL), _symbols()))
    assert walk.established_at_exit == ()


def test_a_child_guarantees_no_document_property_and_nothing_it_does_not_write():
    ChildEntryContractV1(process_ref="$ref:W", entry_form="scheduled",
                         mutated_state=(("dpp", "K"),), guaranteed_state=(("dpp", "K"),))
    with pytest.raises(ValidationError):
        ChildEntryContractV1(process_ref="$ref:W", entry_form="scheduled",
                             mutated_state=(("ddp", "D"),), guaranteed_state=(("ddp", "D"),))
    with pytest.raises(ValidationError):
        ChildEntryContractV1(process_ref="$ref:W", entry_form="scheduled", guaranteed_state=(("dpp", "K"),))


def test_each_guarantee_gate_is_load_bearing(monkeypatch):
    """Mutation witnesses: dropping the abort gate, the path's proof of a run, or the exit
    meet's treatment of a terminal call each admits a case the tests above refuse."""
    def refused(case):
        parent, child, _guaranteed, _expected = _UNGUARANTEED[case]
        return _errors([("PARENT", parent), ("WRITER", child), ("NODATA", _NODATA)], "PARENT") != []

    for case in ("no_abort", "several_documents", "an_arm_ends_in_a_call"):
        assert refused(case), case

    def without_abort(semantic, contract):
        if contract is None or contract.entry_form not in ("scheduled", "passthrough") or not semantic.wait:
            return ()
        return tuple((key[0], key[1]) for key in contract.guaranteed_state)

    with monkeypatch.context() as patched:
        patched.setattr(lineage, "_awaited_guarantee", without_abort)
        assert not refused("no_abort")
    with monkeypatch.context() as patched:
        patched.setattr(lineage, "_child_guarantee",
                        lambda semantic, contract, stream: lineage._awaited_guarantee(semantic, contract))
        assert not refused("several_documents")
    with monkeypatch.context() as patched:
        patched.setattr(lineage, "_ABNORMAL_EXIT_ROLES", frozenset({"exception", "process_call"}))
        assert not refused("an_arm_ends_in_a_call")


def test_the_public_plan_admits_a_later_legs_read_of_a_guaranteed_property():
    """The typed route derives the same contract: the `cap184-dpp-both-ways` shape plans
    clean, and the same call with abort_on_error=false leaves the read unproved."""
    from unittest.mock import MagicMock

    from test_issue_158_listener_deployment import (
        _PROFILE,
        _ApplyBoundary,
        _cause_codes,
        _request,
        _unit,
    )
    from boomi_mcp.categories.integration_builder import build_integration_action

    def plan(abort_on_error):
        parent = _doc({"kind": "branch", "legs": [
            {"steps": [], "terminal": {"kind": "process_call", "process_ref": "$ref:child",
                                       "wait": True, "abort_on_error": abort_on_error}},
            _READS_K_LATER,
        ]})
        raw = _request(
            [_unit(parent, ("child",), key="root"), _unit(_SETS_K, (), key="child", name="E184 Child")],
            [],
        ).model_dump(mode="json")
        with _ApplyBoundary().installed():
            return build_integration_action(MagicMock(), _PROFILE, "plan", config={"authoring_request": raw})

    admitted = plan(True)
    assert admitted["authoring_result"]["validation_report"]["is_valid"] is True, _cause_codes(admitted)
    refused = plan(False)
    assert _READ_BEFORE_WRITE in _cause_codes(refused), _cause_codes(refused)


# ---------------------------------------------------------------------------
# Correction batch 18, pre-commit verification: every use a caller cohort clears
# ---------------------------------------------------------------------------

_NO_DYNAMIC_SEGMENT = PROCESS_IR_SEMANTIC_DYNAMIC_PATH_NO_DYNAMIC_SEGMENT
_AT_THE_CALL = "/body/steps/0/legs/1/terminal/process_ref"
_STAGES_LITERAL_X = {"steps": [_GET, _STATIC_X], "terminal": _PUT}
_READ2 = {"kind": "cache_get", "cache_ref": "$ref:CACHE2"}

#: A bound request path the caller's cached X composes, reached where the stream no longer
#: carries the caller-cache marker: a Message rebuilds it, and so does a second cache.
_ESCAPED_BOUND_USES = {
    "past_a_message": _doc(_READ, _READS_X, _MSG, _BOUND_GET, _STOP),
    "past_a_recache": _legs({"steps": [_READ, _READS_X], "terminal": {
        "kind": "cache_put", "cache_ref": "$ref:CACHE2"}},
        {"steps": [_READ2, _BOUND_GET], "terminal": _STOP}),
}


@pytest.mark.parametrize("use", sorted(_ESCAPED_BOUND_USES))
def test_a_bound_use_the_caller_cohort_clears_is_a_bound_requirement(use):
    """Amendment 3 §7-§8: a property is proved at the call against every write that
    reached the cache, and a BOUND use needs each of those writers to compose the path.

    The binding's own refusal records no cached row here, because the marker rides on the
    stream, so the ordinary read's row was the only one and the call proved establishment
    alone — a literal segment the caller stored then reached a bound request path. The row
    a seeded cohort clears is the bound one whichever refusal recorded it."""
    child = _ESCAPED_BOUND_USES[use]
    roots = [("PARENT", _stage_and_call(_STAGES_X)), ("CACHE_CHILD", child)]
    row = _row(roots, "PARENT", "CACHE_CHILD")
    assert ("$ref:CACHE", "X", None, True) in row.cache_property_requirements, row
    # CONTROL: the dynamic writer composes it, so the whole chain still compiles.
    for key in ("PARENT", "CACHE_CHILD"):
        assert _errors(roots, key) == [], key
        assert _compile_errors(roots, key) == [], key
    literal = [("PARENT", _stage_and_call(_STAGES_LITERAL_X)), ("CACHE_CHILD", child)]
    assert (_NO_DYNAMIC_SEGMENT, _AT_THE_CALL) in _errors(literal, "PARENT")
    assert (_NO_DYNAMIC_SEGMENT, _AT_THE_CALL) in _compile_errors(literal, "PARENT")


def test_a_bound_use_no_cohort_clears_keeps_the_childs_own_refusal():
    """The conservative limit, fail-closed: with no use of X on the retrieve's own stream
    there is nothing for a seeded cohort to clear, so no row crosses the boundary and the
    child keeps its own refusal wherever it binds the path."""
    child = _doc(_READ, _MSG, _BOUND_GET, _STOP)
    roots = [("PARENT", _stage_and_call(_STAGES_X)), ("CACHE_CHILD", child)]
    assert _row(roots, "PARENT", "CACHE_CHILD").cache_property_requirements == ()
    assert (_NOT_ESTABLISHED, "/body/steps/2/path_binding") in _errors(roots, "CACHE_CHILD")


def test_the_cleared_binding_rule_is_load_bearing(monkeypatch):
    """Non-vacuity: with the pre-batch measurement in its place — only a refusal that
    recorded a cached row of its own counts — the literal parent is admitted again."""
    from boomi_mcp.authoring import process_ir_effects

    def cached_rows_only(prepared, capabilities, walk):
        candidates = sorted(set(walk.unestablished_cached_keys),
                            key=lambda row: (row[0], row[1], row[2], row[3] or "", row[4]))
        if not candidates:
            return ()
        seeded = capabilities.model_copy(update={"caller_cache_cohorts": tuple(sorted(
            {(cache, name) for _p, cache, name, _r, _b in candidates}))})
        still = {(row[0], row[2]) for row in lineage.walk_lineage(prepared, seeded).unestablished_cached_keys}
        return tuple(sorted({(cache, name, ref, bound)
                             for pointer, cache, name, ref, bound in candidates
                             if (pointer, name) not in still},
                            key=lambda row: (row[0], row[1], row[2] or "", row[3])))

    literal = [("PARENT", _stage_and_call(_STAGES_LITERAL_X)),
               ("CACHE_CHILD", _ESCAPED_BOUND_USES["past_a_message"])]
    assert (_NO_DYNAMIC_SEGMENT, _AT_THE_CALL) in _errors(literal, "PARENT")
    monkeypatch.setattr(process_ir_effects, "_caller_cached_properties", cached_rows_only)
    assert _errors(literal, "PARENT") == []


def _chain(outer_leg, use="no_data_bound"):
    """GRANDPARENT -> MID -> CACHE_CHILD, where MID stages X into the cache MID's caller
    also writes and the grandchild uses X on the documents it retrieves."""
    return [("PARENT", _legs(outer_leg, {"steps": [], "terminal": _call("MID")})),
            ("MID", _stage_and_call(_STAGES_X)), ("CACHE_CHILD", _USES[use][0])]


def test_a_caller_that_stores_nothing_in_the_shared_cache_satisfies_the_row_vacuously():
    """What a call CHECKS is the documents THIS process put in the cache. A caller that put
    none there stores nothing wrong: it satisfies the row vacuously, exactly as it does at
    runtime, where every document the grandchild retrieves came from deeper in the chain and
    was proved at the call that stored it. So a pure forwarder compiles — at one level of
    forwarding and at two — exactly as the flattened graph of the same legs does, while the
    row it cannot check keeps travelling up to the callers that can.

    The other rule, `_caller_owes_a_cached_property`, decides that travelling: the row is
    owed upward whatever this call proved, because the cache is shared and no process can
    prove it holds only its own documents. Measured on this tree before the correction:
    inheriting the row without this vacuous case refused PARENT at its call
    (`…DDP_NOT_ESTABLISHED` bound, `…PROPERTY_READ_BEFORE_WRITE` ordinary) while the twin
    compiled (FIX-A re-verification new_defects[0]), and dropping the row instead admitted
    the X-less grandparent in the test below (FIX-D re-verification new_defects[1])."""
    for use in ("no_data_bound", "no_data_read"):
        nothing = _chain({"steps": [_MSG], "terminal": _STOP}, use)
        # Nothing is refused, and the row is still owed upward: a caller of PARENT may fill
        # the cache, and PARENT is the last place that would be checked if it did not travel.
        assert _row(nothing, "PARENT", "MID").cache_property_requirements == (
            ("$ref:CACHE", "X", None, use == "no_data_bound"),), use
        for key in ("PARENT", "MID", "CACHE_CHILD"):
            assert _errors(nothing, key) == [], (use, key)
            assert _compile_errors(nothing, key) == [], (use, key)
        use_steps = [_READ, _BOUND_GET] if use == "no_data_bound" else [_READ, _READS_X]
        twin = _legs({"steps": [_MSG], "terminal": _STOP}, _STAGES_X,
                     {"steps": use_steps, "terminal": _STOP})
        assert _errors([("PARENT", twin)], "PARENT") == [], use
    # Two levels of forwarding, neither touching the cache, compile the same way.
    deep = [("PARENT", _legs({"steps": [_MSG], "terminal": _STOP},
                             {"steps": [], "terminal": _call("MIDP")}))] + [
        ("MIDP", _legs({"steps": [_MSG], "terminal": _STOP},
                       {"steps": [], "terminal": _call("MID")}))] + _chain(
        {"steps": [_MSG], "terminal": _STOP})[1:]
    assert _row(deep, "PARENT", "MIDP").cache_property_requirements == (
        ("$ref:CACHE", "X", None, True),)
    assert _errors(deep, "PARENT") == []
    assert _compile_errors(deep, "PARENT") == []
    # CONTROL: an outer caller whose own writes carry X is admitted throughout.
    with_x = _chain(_STAGES_X)
    for key in ("PARENT", "MID", "CACHE_CHILD"):
        assert _errors(with_x, key) == [], key
        assert _compile_errors(with_x, key) == [], key
    # CONTROL: a middle process whose own writes do NOT reach the cache proves nothing
    # either, so its refusal carries the row up and the X-less caller is still refused.
    forwards = [("PARENT", _legs({"steps": [_GET], "terminal": _PUT},
                                 {"steps": [], "terminal": _call("MID")})),
                ("MID", _legs({"steps": [_MSG], "terminal": _STOP},
                              {"steps": [], "terminal": _call("CACHE_CHILD")})),
                ("CACHE_CHILD", _USES["no_data_bound"][0])]
    assert _row(forwards, "PARENT", "MID").cache_property_requirements == (
        ("$ref:CACHE", "X", None, True),)
    assert (_NOT_ESTABLISHED, _AT_THE_CALL) in _errors(forwards, "PARENT")


def test_an_outer_callers_x_less_documents_in_the_same_cache_are_refused_at_the_call():
    """The other shape of the same rule: a middle process that stages X proves the row for
    the writes that reach its own call and for nothing else, so the row travels on and the
    outer caller that stores X-LESS documents in the same shared cache is refused AT ITS
    CALL — where the flattened graph of the same legs refuses the binding.

    An execution-scoped cache is shared with every caller (capture `cap184-shared-cache`),
    so "did this call prove it" is the wrong question and "can documents this process did
    not write reach the cache" is the right one; nothing inside a process can answer the
    second with no, so `_caller_owes_a_cached_property` fails closed. Recording only
    refusals admitted this composition on validate and compile at every root, while the
    model's own flattening refused it (FIX-D re-verification new_defects[1])."""
    x_less = _chain({"steps": [_GET], "terminal": _PUT})
    assert _row(x_less, "PARENT", "MID").cache_property_requirements == (
        ("$ref:CACHE", "X", None, True),)
    assert (_NOT_ESTABLISHED, _AT_THE_CALL) in _errors(x_less, "PARENT")
    assert (_NOT_ESTABLISHED, _AT_THE_CALL) in _compile_errors(x_less, "PARENT")
    for key in ("MID", "CACHE_CHILD"):
        assert _errors(x_less, key) == [], key
    flattened = _legs({"steps": [_GET], "terminal": _PUT}, _STAGES_X,
                      {"steps": [_READ, _BOUND_GET], "terminal": _STOP})
    assert (_NOT_ESTABLISHED, "/body/steps/0/legs/2/steps/1/path_binding") in _errors(
        [("PARENT", flattened)], "PARENT")
    # THE CONTENT CHANNEL beside it still answers the other way at the same shape, and the
    # difference is measured here rather than assumed away: `cache_requirements` inherits
    # only what this process's own writes do not prove, so MID's staging ends that
    # obligation and PARENT's profile-less documents in the same cache are not checked.
    # Fail-open, unchanged from 80bdd30, and out of scope for this correction.
    typed = [("PARENT", _legs({"steps": [_GET], "terminal": _PUT},
                              {"steps": [], "terminal": _call("MID")})),
             ("MID", _stage_and_call(_STAGES_P2)), ("CACHE_CHILD", _TYPED_CACHE_CHILD)]
    assert _row(typed, "PARENT", "MID").cache_requirements == ()
    assert _errors(typed, "PARENT") == []


def test_the_vacuous_cache_rule_is_load_bearing(monkeypatch):
    """Non-vacuity of the first half: with `_call_stores_nothing_in` answering False — the
    rule of the round that inherited unconditionally — the forwarder that stores nothing is
    refused again at its call, while its flattened twin still compiles."""
    nothing = _chain({"steps": [_MSG], "terminal": _STOP})
    assert _errors(nothing, "PARENT") == []
    monkeypatch.setattr(lineage, "_call_stores_nothing_in",
                        lambda state, cache_ref, external_writer: False)
    assert (_NOT_ESTABLISHED, _AT_THE_CALL) in _errors(nothing, "PARENT")
    twin = _legs({"steps": [_MSG], "terminal": _STOP}, _STAGES_X,
                 {"steps": [_READ, _BOUND_GET], "terminal": _STOP})
    assert _errors([("PARENT", twin)], "PARENT") == []


def test_the_shared_cache_obligation_travelling_upward_is_load_bearing(monkeypatch):
    """Non-vacuity of the second half: with `_caller_owes_a_cached_property` answering False
    — the rule of the round that recorded refusals only — the middle process's row goes back
    to () and the outer caller's X-LESS documents in the same cache are admitted again."""
    x_less = _chain({"steps": [_GET], "terminal": _PUT})
    assert (_NOT_ESTABLISHED, _AT_THE_CALL) in _errors(x_less, "PARENT")
    monkeypatch.setattr(lineage, "_caller_owes_a_cached_property",
                        lambda cache_ref, name, capabilities: False)
    assert _row(x_less, "PARENT", "MID").cache_property_requirements == ()
    assert _errors(x_less, "PARENT") == []


_PUT2 = {"kind": "cache_put", "cache_ref": "$ref:CACHE2"}
_AT_THE_THIRD_LEGS_CALL = "/body/steps/0/legs/2/terminal/process_ref"
#: One child, two caches, one bound use. Which cache the binding rides on is the only
#: difference, and the caller writes X dynamically into CACHE and literally into CACHE2.
_BINDING_RIDES_ON = {
    "the_cache_it_reads": (_READ, ()),
    "a_second_cache": (_READ2, ((_NO_DYNAMIC_SEGMENT, _AT_THE_THIRD_LEGS_CALL),)),
}


def _two_cache_child(bound_read):
    return _legs({"steps": [_READ, _READS_X], "terminal": _STOP},
                 {"steps": [_READ2, _READS_X], "terminal": _STOP},
                 {"steps": [bound_read, _MSG, _BOUND_GET], "terminal": _STOP})


_WRITES_BOTH_CACHES = _legs(_STAGES_X, {"steps": [_GET, _STATIC_X], "terminal": _PUT2},
                            {"steps": [], "terminal": _call("CACHE_CHILD")})


@pytest.mark.parametrize("rides_on", sorted(_BINDING_RIDES_ON))
def test_a_bound_use_is_attributed_to_the_cache_the_binding_rode_on(rides_on):
    """A cleared binding is a bound use of the cache whose OWN seed cleared it, never of
    every cache that happens to need a property of that name. The child reads X ordinarily
    from both caches and binds a request path on one of them; the caller composes X
    dynamically into that one and literally into the other. Exactly the cache the binding
    rides on decides, as it does in the flattened graph.

    Before the correction both spellings were refused at the same pointer with the same four
    rows, so a caller whose second cache stored a literal X was refused although no bound
    path ever read those documents (FIX-A re-verification new_defects[1])."""
    bound_read, expected = _BINDING_RIDES_ON[rides_on]
    roots = [("PARENT", _WRITES_BOTH_CACHES), ("CACHE_CHILD", _two_cache_child(bound_read))]
    assert _errors(roots, "PARENT") == list(expected)
    assert _compile_errors(roots, "PARENT") == list(expected)
    rows = _row(roots, "PARENT", "CACHE_CHILD").cache_property_requirements
    on = bound_read["cache_ref"]
    beside = "$ref:CACHE2" if on == "$ref:CACHE" else "$ref:CACHE"
    assert (on, "X", None, True) in rows, rows
    assert (beside, "X", None, True) not in rows, rows
    # Both ordinary reads stay the caller's obligation whichever cache binds.
    assert {(on, "X", None, False), (beside, "X", None, False)} <= set(rows), rows


def test_the_cache_a_cleared_binding_is_credited_to_is_load_bearing(monkeypatch):
    """Non-vacuity: with the pre-correction attribution — every seeded cache sharing the
    property NAME — the caller of the child that binds on CACHE is refused again for what
    it stored in CACHE2."""
    from boomi_mcp.authoring import process_ir_effects

    def by_name_across_every_cache(prepared, capabilities, walk):
        candidates = sorted(set(walk.unestablished_cached_keys),
                            key=lambda row: (row[0], row[1], row[2], row[3] or "", row[4]))
        if not candidates:
            return ()
        seeds = tuple(sorted({(cache, name) for _p, cache, name, _r, _b in candidates}))
        after = lineage.walk_lineage(prepared, capabilities.model_copy(
            update={"caller_cache_cohorts": seeds}))
        still = {(row[0], row[2]) for row in after.unestablished_cached_keys}
        rows = {(cache, name, ref, bound) for pointer, cache, name, ref, bound in candidates
                if (pointer, name) not in still}
        rows |= {
            (cache, name, request_profile_ref, True)
            for _pointer, name, request_profile_ref in
            set(walk.unestablished_bindings) - set(after.unestablished_bindings)
            for cache in {seeded for seeded, seeded_name in seeds if seeded_name == name}
        }
        return tuple(sorted(rows, key=lambda row: (row[0], row[1], row[2] or "", row[3])))

    roots = [("PARENT", _WRITES_BOTH_CACHES), ("CACHE_CHILD", _two_cache_child(_READ))]
    assert _errors(roots, "PARENT") == []
    monkeypatch.setattr(process_ir_effects, "_caller_cached_properties", by_name_across_every_cache)
    assert (_NO_DYNAMIC_SEGMENT, _AT_THE_THIRD_LEGS_CALL) in _errors(roots, "PARENT")


#: The child of the re-cache shape: it reads one cache, uses X on those documents, stores
#: them in a SECOND cache, then retrieves that one and binds a request path on X. At runtime
#: the second cache holds the child's re-cached documents AND whatever its caller stored
#: there, so both caches carry the caller's documents to the binding.
_RECACHES_THEN_BINDS = _legs({"steps": [_READ, _READS_X], "terminal": _PUT2},
                             {"steps": [_READ2, _BOUND_GET], "terminal": _STOP})
_DYNAMIC_INTO_CACHE2 = {"steps": [_GET, _DYNAMIC_X], "terminal": _PUT2}
_LITERAL_INTO_CACHE2 = {"steps": [_GET, _STATIC_X], "terminal": _PUT2}
#: What the caller stores in each cache, and the verdict it earns.
_RECACHE_CALLERS = {
    "dynamic_in_both": (_STAGES_X, _DYNAMIC_INTO_CACHE2, ()),
    "a_literal_in_the_cache_the_binding_rides_on": (
        _STAGES_X, _LITERAL_INTO_CACHE2,
        ((_NO_DYNAMIC_SEGMENT, _AT_THE_THIRD_LEGS_CALL),)),
    "a_literal_in_the_cache_the_property_came_from": (
        _STAGES_LITERAL_X, _DYNAMIC_INTO_CACHE2,
        ((_NO_DYNAMIC_SEGMENT, _AT_THE_THIRD_LEGS_CALL),)),
}


@pytest.mark.parametrize("caller", sorted(_RECACHE_CALLERS))
def test_a_bound_use_past_a_recache_is_owed_to_the_cache_it_rides_on_too(caller):
    """A cleared binding is a bound use of the cache the documents are RETRIEVED from as
    well as of the cache whose seed cleared it. Where nothing re-caches between the retrieve
    and the binding those are one cache; where the child re-caches them they are two, and
    both hold documents its caller may have stored — so a literal path segment in either one
    reaches the bound request path and is refused at the call, exactly as the flattened graph
    of the same legs refuses it.

    Crediting the seed alone named the cache the property came FROM, so the caller's literal
    writes to the cache the binding actually rides on were never checked and the composition
    was admitted although 80bdd30 refused it at the child (FIX-D re-verification
    new_defects[2]) — the class ARCH-184-r1-05 exists to close."""
    into_cache, into_cache2, expected = _RECACHE_CALLERS[caller]
    parent = _legs(into_cache, into_cache2, {"steps": [], "terminal": _call("CACHE_CHILD")})
    roots = [("PARENT", parent), ("CACHE_CHILD", _RECACHES_THEN_BINDS)]
    assert _errors(roots, "PARENT") == list(expected)
    assert _compile_errors(roots, "PARENT") == list(expected)
    assert set(_row(roots, "PARENT", "CACHE_CHILD").cache_property_requirements) == {
        ("$ref:CACHE", "X", None, False),
        ("$ref:CACHE", "X", None, True),
        ("$ref:CACHE2", "X", None, True),
    }
    # The model's own flattening of the same legs agrees, both ways.
    flat = list(parent["body"]["steps"][0]["legs"])[:-1] + list(
        _RECACHES_THEN_BINDS["body"]["steps"][0]["legs"])
    twin = _errors([("PARENT", _legs(*flat))], "PARENT")
    assert bool(twin) == bool(expected), (twin, expected)
    # CONTROL: a caller that stores nothing in the cache the binding rides on is admitted —
    # the row it cannot check is satisfied vacuously, not refused.
    only_one = [("PARENT", _legs(into_cache, {"steps": [], "terminal": _call("CACHE_CHILD")})),
                ("CACHE_CHILD", _RECACHES_THEN_BINDS)]
    assert _errors(only_one, "PARENT") == list(
        () if caller != "a_literal_in_the_cache_the_property_came_from"
        else ((_NO_DYNAMIC_SEGMENT, _AT_THE_CALL),))


def test_the_cache_a_binding_rides_on_is_load_bearing(monkeypatch):
    """Non-vacuity: with the pre-correction attribution — a cleared binding credited to the
    seeded cache alone — the row loses its second cache and the caller that stored a literal
    segment in the cache the binding rides on is admitted again."""
    from boomi_mcp.authoring import process_ir_effects

    def the_seeded_cache_only(prepared, capabilities, walk):
        candidates = sorted(set(walk.unestablished_cached_keys),
                            key=lambda row: (row[0], row[1], row[2], row[3] or "", row[4]))
        if not candidates:
            return ()
        seeds = tuple(sorted({(cache, name) for _p, cache, name, _r, _b in candidates}))
        after = lineage.walk_lineage(prepared, capabilities.model_copy(
            update={"caller_cache_cohorts": seeds}))
        still = {(row[0], row[2]) for row in after.unestablished_cached_keys}
        rows = {(cache, name, ref, bound) for pointer, cache, name, ref, bound in candidates
                if (pointer, name) not in still}
        cleared = set(walk.unestablished_bindings) - set(after.unestablished_bindings)
        for cache, name in seeds:
            alone = lineage.walk_lineage(prepared, capabilities.model_copy(
                update={"caller_cache_cohorts": ((cache, name),)}))
            rows |= {(cache, cleared_name, ref, True)
                     for _pointer, cleared_name, ref in cleared - set(alone.unestablished_bindings)
                     if cleared_name == name}
        return tuple(sorted(rows, key=lambda row: (row[0], row[1], row[2] or "", row[3])))

    roots = [("PARENT", _legs(_STAGES_X, _LITERAL_INTO_CACHE2,
                              {"steps": [], "terminal": _call("CACHE_CHILD")})),
             ("CACHE_CHILD", _RECACHES_THEN_BINDS)]
    assert (_NO_DYNAMIC_SEGMENT, _AT_THE_THIRD_LEGS_CALL) in _errors(roots, "PARENT")
    monkeypatch.setattr(process_ir_effects, "_caller_cached_properties", the_seeded_cache_only)
    assert _errors(roots, "PARENT") == []
    assert ("$ref:CACHE2", "X", None, True) not in _row(
        roots, "PARENT", "CACHE_CHILD").cache_property_requirements


#: A child that binds a request path past EACH of two caches: two cached-property rows of
#: ONE property name, both reported at the caller's single call pointer.
_BINDS_PAST_EACH_CACHE = _legs({"steps": [_READ, _BOUND_GET], "terminal": _STOP},
                               {"steps": [_READ2, _BOUND_GET], "terminal": _STOP})
_X_LESS_INTO_CACHE2 = {"steps": [_GET], "terminal": _PUT2}
_STORES_NOTHING = {"steps": [_MSG], "terminal": _STOP}
#: What the middle stores in the second cache before forwarding, and the rows it then owes
#: ITS OWN callers: a row survives exactly where a seeded caller cohort for THAT cache
#: clears the middle's refusal, so the one the middle itself broke drops and the other stays.
_MIDDLES_INTO_CACHE2 = {
    "x_less_documents": (_X_LESS_INTO_CACHE2, (("$ref:CACHE", "X", None, True),)),
    "nothing_at_all": (_STORES_NOTHING, (("$ref:CACHE", "X", None, True),
                                         ("$ref:CACHE2", "X", None, True))),
    "a_literal_X": (_LITERAL_INTO_CACHE2, (("$ref:CACHE", "X", None, True),)),
}


def _colliding_chain(mid_leg, into_cache=_STAGES_LITERAL_X, into_cache2=_LITERAL_INTO_CACHE2):
    """PARENT -> MID -> the child that binds past each cache. PARENT fills both caches
    before calling; MID runs `mid_leg` against the second one and forwards."""
    return [("PARENT", _legs(into_cache, into_cache2,
                             {"steps": [], "terminal": _call("MID")})),
            ("MID", _legs(mid_leg, {"steps": [], "terminal": _call("CACHE_CHILD")})),
            ("CACHE_CHILD", _BINDS_PAST_EACH_CACHE)]


@pytest.mark.parametrize("middle", sorted(_MIDDLES_INTO_CACHE2))
def test_two_cached_rows_of_one_name_at_one_call_are_measured_per_cache(middle):
    """Two caches needing a property of the same name at ONE call pointer are TWO
    obligations. The measurement that decides which of them a caller owes is keyed by
    cache, so a refusal the seeded cohort cannot clear for one cache leaves the other row
    where it is.

    Keyed on (pointer, property name) alone, a middle that broke EITHER row dropped BOTH:
    it published `cache_property_requirements = ()` for a child that genuinely has two, and
    the caller that stored a literal path segment in the cache the grandchild still binds on
    was admitted — fail-open, and the second measured instance of the class ARCH-184-r1-05
    exists to close (the first was the bound use credited to one cache only).

    The two middles that break nothing at that pointer are the control: their rows and their
    callers' verdicts are the same under either keying."""
    mid_leg, expected_rows = _MIDDLES_INTO_CACHE2[middle]
    roots = _colliding_chain(mid_leg)
    # The child's own contract is unchanged by what the middle does: two rows, one per cache.
    assert set(_row(roots, "MID", "CACHE_CHILD").cache_property_requirements) == {
        ("$ref:CACHE", "X", None, True), ("$ref:CACHE2", "X", None, True)}
    assert _row(roots, "PARENT", "MID").cache_property_requirements == expected_rows
    # PARENT stored a LITERAL X in the cache every surviving row names, so it is refused.
    assert _errors(roots, "PARENT") == [(_NO_DYNAMIC_SEGMENT, _AT_THE_THIRD_LEGS_CALL)]
    assert _compile_errors(roots, "PARENT") == [(_NO_DYNAMIC_SEGMENT, _AT_THE_THIRD_LEGS_CALL)]


#: What the caller stores in each cache past a middle that breaks the SECOND one, and the
#: verdict its own call then earns. The row for the broken cache is the middle's defect and
#: is not charged upward; the row for the cache the middle left alone still is.
_COLLIDING_CALLERS = {
    "a_literal_in_both": (_STAGES_LITERAL_X, _LITERAL_INTO_CACHE2,
                          ((_NO_DYNAMIC_SEGMENT, _AT_THE_THIRD_LEGS_CALL),)),
    "a_literal_in_the_cache_the_middle_left_alone": (
        _STAGES_LITERAL_X, _DYNAMIC_INTO_CACHE2,
        ((_NO_DYNAMIC_SEGMENT, _AT_THE_THIRD_LEGS_CALL),)),
    "a_literal_only_in_the_cache_the_middle_broke": (_STAGES_X, _LITERAL_INTO_CACHE2, ()),
    "dynamic_in_both": (_STAGES_X, _DYNAMIC_INTO_CACHE2, ()),
}


@pytest.mark.parametrize("caller", sorted(_COLLIDING_CALLERS))
def test_the_surviving_row_charges_the_caller_for_that_cache_and_no_other(caller):
    """The other direction, so the per-cache key is not read as "keep every row": a caller
    whose only literal segment sits in the cache the middle ALREADY broke is not refused at
    its call — that row is the middle's own defect, measured by the seed failing to clear
    it — while a literal in the cache the middle left alone is refused.

    The middle is refused either way, so no such composition ships: the request is blocked at
    MID's own call, which is where the child's unmet obligation belongs. The flattened graph
    of the same legs is refused in every one of these shapes, including the two whose caller
    is admitted, and that is the point — the refusal is relocated to the middle, not lost."""
    into_cache, into_cache2, expected = _COLLIDING_CALLERS[caller]
    roots = _colliding_chain(_X_LESS_INTO_CACHE2, into_cache, into_cache2)
    assert _errors(roots, "PARENT") == list(expected)
    assert _compile_errors(roots, "PARENT") == list(expected)
    # The middle owns the second cache's defect, at its own call, on both routes.
    assert (_NOT_ESTABLISHED, _AT_THE_CALL) in _errors(roots, "MID")
    assert (_NOT_ESTABLISHED, _AT_THE_CALL) in _compile_errors(roots, "MID")
    # The model's own flattening of the whole chain refuses it, so nothing here is admitted
    # as a complete composition.
    flat = [into_cache, into_cache2, _X_LESS_INTO_CACHE2] + list(
        _BINDS_PAST_EACH_CACHE["body"]["steps"][0]["legs"])
    assert _errors([("PARENT", _legs(*flat))], "PARENT") != []


def test_the_cache_a_refused_cached_row_belongs_to_is_load_bearing(monkeypatch):
    """Non-vacuity: with the pre-correction key — (pointer, property name), ignoring the
    cache — the middle's refusal of the second cache swallows the first cache's row too, the
    contract goes back to (), and the caller that stored a literal X in the cache the
    grandchild binds on is admitted again."""
    from boomi_mcp.authoring import process_ir_effects

    def keyed_on_the_property_name_alone(prepared, capabilities, walk):
        candidates = sorted(set(walk.unestablished_cached_keys),
                            key=lambda row: (row[0], row[1], row[2], row[3] or "", row[4]))
        if not candidates:
            return ()
        seeds = tuple(sorted({(cache, name) for _p, cache, name, _r, _b in candidates}))
        after = lineage.walk_lineage(prepared, capabilities.model_copy(
            update={"caller_cache_cohorts": seeds}))
        still = {(row[0], row[2]) for row in after.unestablished_cached_keys}
        rows = {(cache, name, ref, bound) for pointer, cache, name, ref, bound in candidates
                if (pointer, name) not in still}
        cleared_bindings = set(walk.unestablished_bindings) - set(after.unestablished_bindings)
        if cleared_bindings:
            rides_on = {}
            for pointer, cache in walk.binding_cache_origins:
                rides_on.setdefault(pointer, set()).add(cache)
            for cache, name in seeds:
                alone = lineage.walk_lineage(prepared, capabilities.model_copy(
                    update={"caller_cache_cohorts": ((cache, name),)}))
                for pointer, cleared_name, ref in (
                    cleared_bindings - set(alone.unestablished_bindings)
                ):
                    if cleared_name != name:
                        continue
                    rows.add((cache, cleared_name, ref, True))
                    rows |= {(ridden, cleared_name, ref, True)
                             for ridden in rides_on.get(pointer, ())}
        return tuple(sorted(rows, key=lambda row: (row[0], row[1], row[2] or "", row[3])))

    roots = _colliding_chain(_X_LESS_INTO_CACHE2)
    assert _errors(roots, "PARENT") == [(_NO_DYNAMIC_SEGMENT, _AT_THE_THIRD_LEGS_CALL)]
    monkeypatch.setattr(process_ir_effects, "_caller_cached_properties",
                        keyed_on_the_property_name_alone)
    assert _row(roots, "PARENT", "MID").cache_property_requirements == ()
    assert _errors(roots, "PARENT") == []


# ---------------------------------------------------------------------------
# Correction batch 18, pre-commit verification: a removal invalidates a guarantee
# ---------------------------------------------------------------------------

_CACHE_WRITER_MISSING = PROCESS_IR_SEMANTIC_LINEAGE_CACHE_WRITER_MISSING
_REMOVE = {"kind": "cache_remove", "cache_ref": "$ref:CACHE"}
_FILLS_THE_CACHE = {"steps": [_GET], "terminal": _PUT}
_EMPTIES_IT = {"steps": [], "terminal": _REMOVE}
#: fills the cache on one leg, empties it on the next
_PUTS_THEN_EMPTIES = _legs(_FILLS_THE_CACHE, _EMPTIES_IT)
#: empties it first and fills it again, so every completion leaves it filled
_EMPTIES_THEN_PUTS = _legs(_EMPTIES_IT, _FILLS_THE_CACHE)
_ONLY_EMPTIES = _legs(_EMPTIES_IT, {"steps": [_MSG], "terminal": _STOP})
_AT_THE_CACHE_READ = "/body/steps/0/legs/1/steps/0"


def _calls_then_reads_the_cache(child_key, **call):
    """A scheduled parent: one leg calls the child, the next reads the cache it wrote."""
    return _legs({"steps": [], "terminal": _call(child_key, **dict(_WAITS_AND_ABORTS, **call))},
                 {"steps": [_READ, _MSG], "terminal": _STOP})


def _removal_chain(**inner):
    """PARENT -> MID -> HIDES, where MID fills the cache and HIDES empties it."""
    return [("PARENT", _calls_then_reads_the_cache("MID")),
            ("MID", _legs(_FILLS_THE_CACHE,
                          {"steps": [], "terminal": _call("HIDES", **dict(_WAITS_AND_ABORTS, **inner))})),
            ("HIDES", _ONLY_EMPTIES)]


def test_a_child_that_empties_the_cache_guarantees_it_to_nobody():
    """Amendment 3 §8: a terminal remove invalidates execution-cache guarantees even though
    it emits no documents. The child fills the cache on one leg and empties it on the next,
    so the caller's later read of that cache is refused again — the guarantee this batch
    added must not outlive the removal that undid the write behind it."""
    roots = [("PARENT", _calls_then_reads_the_cache("WRITER")), ("WRITER", _PUTS_THEN_EMPTIES)]
    row = _row(roots, "PARENT", "WRITER")
    assert row.removed_caches == ("$ref:CACHE",)
    assert row.guaranteed_state == ()
    assert (_CACHE_WRITER_MISSING, _AT_THE_CACHE_READ) in _errors(roots, "PARENT")
    assert (_CACHE_WRITER_MISSING, _AT_THE_CACHE_READ) in _compile_errors(roots, "PARENT")
    # CONTROL: the same child that empties the cache and fills it again on every completion
    # still guarantees it, so the correction refuses the removal and not the removal step.
    refilled = [("PARENT", _calls_then_reads_the_cache("WRITER")), ("WRITER", _EMPTIES_THEN_PUTS)]
    assert _row(refilled, "PARENT", "WRITER").guaranteed_state == (("cache", "$ref:CACHE"),)
    for key in ("PARENT", "WRITER"):
        assert _errors(refilled, key) == [], key
        assert _compile_errors(refilled, key) == [], key


def test_a_removal_a_called_grandchild_makes_reaches_the_outer_caller():
    """The removal is inherited: MID fills the cache and hands it to a child that empties
    it, so MID guarantees nothing to ITS caller either. Not gated on wait or
    abort_on_error, which decide whether a child's WRITES can be relied on and never
    whether its removal can be ruled out."""
    for inner in ({}, {"abort_on_error": False}, {"wait": False}):
        roots = _removal_chain(**inner)
        row = _row(roots, "PARENT", "MID")
        assert row.removed_caches == ("$ref:CACHE",), inner
        assert row.guaranteed_state == (), inner
        assert (_CACHE_WRITER_MISSING, _AT_THE_CACHE_READ) in _errors(roots, "PARENT"), inner
        # MID and the grandchild are each valid on their own: only the caller's read moves.
        assert _errors(roots, "HIDES") == [], inner
    assert (_CACHE_WRITER_MISSING, _AT_THE_CACHE_READ) in _compile_errors(_removal_chain(), "PARENT")


def test_each_removal_gate_is_load_bearing(monkeypatch):
    """Mutation witnesses, each neutralising one hunk: the pre-batch removal that cleared
    the content and left the cache established, a call that inherits no removal from its
    child, and the primitive both paths share."""
    own = [("PARENT", _calls_then_reads_the_cache("WRITER")), ("WRITER", _PUTS_THEN_EMPTIES)]
    chain = _removal_chain()
    assert _errors(own, "PARENT") != []
    assert _errors(chain, "PARENT") != []

    with monkeypatch.context() as patched:
        patched.setattr(lineage, "_after_a_whole_cache_removal",
                        lambda state, cache_ref: state.without_content(cache_ref))
        assert _errors(own, "PARENT") == []
    with monkeypatch.context() as patched:
        patched.setattr(lineage, "_caches_a_call_may_remove", lambda cache_refs, contract: ())
        assert _errors(chain, "PARENT") == []
    with monkeypatch.context() as patched:
        patched.setattr(lineage, "_without_cache_establishment", lambda state, cache_ref: state)
        assert _errors(own, "PARENT") == []
        assert _errors(chain, "PARENT") == []


#: A derived child that only hands its caller's execution on to a process nothing derives.
_CALLS_UNKNOWN = _legs({"steps": [], "terminal": _call("EXTERNAL", **_WAITS_AND_ABORTS)},
                       {"steps": [_MSG], "terminal": _STOP})
_AT_A_LATER_LEGS_READ = "/body/steps/0/legs/2/steps/0"


def _fills_calls_then_reads(child_key):
    """A scheduled parent: fill the cache, call the child, then read that cache in a later leg."""
    return _legs(_FILLS_THE_CACHE,
                 {"steps": [], "terminal": _call(child_key, **_WAITS_AND_ABORTS)},
                 {"steps": [_READ, _MSG], "terminal": _STOP})


def test_a_call_un_establishes_only_the_caches_the_walk_proves_it_may_remove():
    """A removal the walk PROVES un-establishes a cache; not knowing what a child does never
    does. That is the rule on the write side beside it — `_caches_a_call_may_write` records
    an unknown child's writes as an unknown possibility and leaves the establishment alone —
    and it is what amendment 3 states for these summaries: "Whole-cache removal clears these
    summaries. Possible opaque/external/child writes introduce unknown possibilities."

    So the verdict no longer depends on how far away the underivable process sits: a derived
    child that merely calls a process nothing derives leaves its caller's cache established,
    exactly as that same process called DIRECTLY does, one level down (measured identical at
    80bdd30 and here; FIX-B re-verification new_defects[1])."""
    derived = [("PARENT", _fills_calls_then_reads("MID")), ("MID", _CALLS_UNKNOWN)]
    row = _row(derived, "PARENT", "MID")
    assert (row.cache_writes_known, row.removed_caches) == (False, ())
    for key in ("PARENT", "MID"):
        assert _errors(derived, key) == [], key
        assert _compile_errors(derived, key) == [], key
    # One level down: the same unknowable process called directly.
    assert _errors([("PARENT", _fills_calls_then_reads("EXTERNAL"))], "PARENT") == []
    # CONTROL: a derivable grandchild that removes nothing keeps the cache established.
    known = [("PARENT", _fills_calls_then_reads("MID")),
             ("MID", _legs({"steps": [], "terminal": _call("NODATA", **_WAITS_AND_ABORTS)},
                           {"steps": [_MSG], "terminal": _STOP})),
             ("NODATA", _NODATA)]
    assert _errors(known, "PARENT") == []
    # THE REFUSALS THIS BATCH INTENDS, unmoved: a removal the walk proves still takes the
    # establishment away, on the child's own path and through a derived grandchild alike.
    proved = ([("PARENT", _fills_calls_then_reads("WRITER")), ("WRITER", _PUTS_THEN_EMPTIES)],
              [("PARENT", _fills_calls_then_reads("MID")),
               ("MID", _legs(_FILLS_THE_CACHE,
                             {"steps": [], "terminal": _call("HIDES", **_WAITS_AND_ABORTS)})),
               ("HIDES", _ONLY_EMPTIES)])
    for roots in proved:
        assert _row(roots, "PARENT", roots[1][0]).removed_caches == ("$ref:CACHE",)
        assert (_CACHE_WRITER_MISSING, _AT_A_LATER_LEGS_READ) in _errors(roots, "PARENT")
        assert (_CACHE_WRITER_MISSING, _AT_A_LATER_LEGS_READ) in _compile_errors(roots, "PARENT")


def test_the_removal_fallback_is_load_bearing(monkeypatch):
    """Non-vacuity: with the pre-correction fallback in its place — a child whose cache
    writes are not all known may have removed EVERY cache this process can observe — the
    derived chain is refused again, while the identical unknowability called directly stays
    admitted. That difference is the defect the correction removes."""
    derived = [("PARENT", _fills_calls_then_reads("MID")), ("MID", _CALLS_UNKNOWN)]
    direct = [("PARENT", _fills_calls_then_reads("EXTERNAL"))]
    assert _errors(derived, "PARENT") == []

    def every_observable_cache(cache_refs, contract):
        if contract is not None and (contract.state_known or contract.cache_writes_known):
            return tuple(contract.removed_caches)
        return tuple(cache_refs)

    monkeypatch.setattr(lineage, "_caches_a_call_may_remove", every_observable_cache)
    assert (_CACHE_WRITER_MISSING, _AT_A_LATER_LEGS_READ) in _errors(derived, "PARENT")
    assert _errors(direct, "PARENT") == []


#: A child that PROVABLY empties the cache on one leg and, on the next, calls a process
#: nothing derives. The removal is authored in its own body; the call hides other removals
#: but erases none.
_EMPTIES_AND_CALLS_UNKNOWN = _legs(
    _EMPTIES_IT, {"steps": [], "terminal": _call("EXTERNAL", **_WAITS_AND_ABORTS)})


def test_a_removal_the_walk_proves_survives_an_underivable_call_beside_it():
    """`removed_caches` carries what the walk PROVES, independently of what it cannot know
    about a call it could not derive. `mutated_state` is a completeness claim — "writes
    nothing else" — and is rightly blanked when a call hides writes; a removal list is an
    existence claim, and blanking it with `mutated_state` erased a `cache_remove` authored
    in the child's own body the moment the child also called a process nothing derives, so
    a parent that filled that cache, called the child and read the cache afterwards was
    admitted on validate and compile (FIX-D re-verification new_defects[0])."""
    roots = [("PARENT", _fills_calls_then_reads("MID")), ("MID", _EMPTIES_AND_CALLS_UNKNOWN)]
    row = _row(roots, "PARENT", "MID")
    assert (row.cache_writes_known, row.removed_caches) == (False, ("$ref:CACHE",))
    assert (_CACHE_WRITER_MISSING, _AT_A_LATER_LEGS_READ) in _errors(roots, "PARENT")
    assert (_CACHE_WRITER_MISSING, _AT_A_LATER_LEGS_READ) in _compile_errors(roots, "PARENT")
    # One level deeper: the proved removal sits in a derived grandchild, beside the same
    # underivable call, and still reaches the outer caller.
    deeper = [("PARENT", _fills_calls_then_reads("MID")),
              ("MID", _legs({"steps": [], "terminal": _call("HIDES", **_WAITS_AND_ABORTS)},
                            {"steps": [], "terminal": _call("EXTERNAL", **_WAITS_AND_ABORTS)})),
              ("HIDES", _ONLY_EMPTIES)]
    assert _row(deeper, "PARENT", "MID").removed_caches == ("$ref:CACHE",)
    assert (_CACHE_WRITER_MISSING, _AT_A_LATER_LEGS_READ) in _errors(deeper, "PARENT")
    # CONTROLS, unmoved: unknowability ALONE still removes nothing, one level up and one
    # level down alike — the rule the round before this one established.
    derived = [("PARENT", _fills_calls_then_reads("MID")), ("MID", _CALLS_UNKNOWN)]
    assert _row(derived, "PARENT", "MID").removed_caches == ()
    assert _errors(derived, "PARENT") == []
    assert _errors([("PARENT", _fills_calls_then_reads("EXTERNAL"))], "PARENT") == []


def test_the_proved_removal_surviving_an_unknown_call_is_load_bearing(monkeypatch):
    """Non-vacuity: with the pre-correction gate in its place — the removal list blanked
    whenever the child's cache writes are not all known — the authored removal disappears
    from the contract and the parent's later read is admitted again."""
    from boomi_mcp.authoring import process_ir_effects

    real = process_ir_effects.derive_child_entry_facts

    def blanked_with_mutated_state(child_ir, symbols, capabilities=None):
        facts = real(child_ir, symbols, capabilities)
        if not (facts.get("state_known") or facts.get("cache_writes_known")):
            facts = dict(facts, removed_caches=())
        return facts

    roots = [("PARENT", _fills_calls_then_reads("MID")), ("MID", _EMPTIES_AND_CALLS_UNKNOWN)]
    assert (_CACHE_WRITER_MISSING, _AT_A_LATER_LEGS_READ) in _errors(roots, "PARENT")
    monkeypatch.setattr(process_ir_effects, "derive_child_entry_facts", blanked_with_mutated_state)
    assert _row(roots, "PARENT", "MID").removed_caches == ()
    assert _errors(roots, "PARENT") == []


_DDP_SCOPE_INVALID = PROCESS_IR_SEMANTIC_LINEAGE_DDP_SCOPE_INVALID
_EXTERNAL_WRITER_ASSUMED = PROCESS_IR_SEMANTIC_LINEAGE_EXTERNAL_WRITER_ASSUMED
_ORDERING_UNSAFE = PROCESS_IR_SEMANTIC_SIDE_EFFECT_ORDERING_UNSAFE
#: The authority row whose prose is folded into the served boundary rule.
_AUTHORITY = "none:every-strict-finding-stands"

#: The four caller shapes ONE walked child is called from. NO verdict is pinned here: the
#: witness exists to compare a served sentence with what the compiler actually does, and a
#: table of expected codes beside it would be a second hand-model of the same gates.
_BOUNDARY_SHAPES = {
    "gated": dict(_WAITS_AND_ABORTS),
    "no_wait": {"wait": False, "abort_on_error": True},
    "no_abort": {"wait": True, "abort_on_error": False},
    "several_documents": dict(_WAITS_AND_ABORTS, passthrough=True),
}
_UNGATED = tuple(shape for shape in _BOUNDARY_SHAPES if shape != "gated")

#: One child per WRITE SCOPE the served sentence quantifies over — each proving its write
#: on every normal completion — with the caller's later leg that consumes it and the
#: replay-safety a declaration of it must claim:
#: ``(child, the declared write, the later leg, replay_safe)``.
#:
#: The cache child carries no connector on purpose: `cache_put` already makes it
#: replay-UNSAFE, so a declaration claiming replay_safe=True is rejected as a content
#: mismatch before any verdict — which is how the first measurement of this scope was
#: misread as "a cache can never be declared".
_BOUNDARY_SCOPES = {
    "dpp": (_SETS_K, ("dpp", "K"), _READS_K_LATER, True),
    "cache": (_legs({"steps": [_MSG], "terminal": _PUT}, {"steps": [_MSG], "terminal": _STOP}),
              ("cache", "$ref:CACHE"), {"steps": [_READ, _MSG], "terminal": _STOP}, False),
    "ddp": (_SETS_D, ("ddp", "D"), _READS_D_LATER, True),
}


def _subprocess_declaration(process_ref, write, replay_safe):
    from boomi_mcp.models.authoring_workflow import (
        ProcessIREffectDeclarationsV1,
        ProcessIRStateEffectDeclarationV1,
        ProcessIRStateReferenceV1,
        ProcessIRSubprocessEffectDeclarationV1,
    )

    return ProcessIREffectDeclarationsV1(subprocess_effects=(
        ProcessIRSubprocessEffectDeclarationV1(
            process_ref=process_ref,
            effect=ProcessIRStateEffectDeclarationV1(
                reads=(),
                writes=(ProcessIRStateReferenceV1(scope=write[0], name=write[1]),),
                replay_safe=replay_safe)),))


def _external_writer_declaration():
    from boomi_mcp.models.authoring_workflow import (
        ProcessIREffectDeclarationsV1,
        ProcessIRExternalWriterDeclarationV1,
    )

    return ProcessIREffectDeclarationsV1(external_writers=(
        ProcessIRExternalWriterDeclarationV1(cache_ref="$ref:CACHE"),))


def _cell(roots, declarations=None, walkable=True, key="PARENT"):
    """ONE measured verdict: what the resolver says, and what validate and compile report.

    `ok` is the resolver's; `inert` is the server's own word for a declaration it has no
    authority for, which is what the served sentence claims to describe.
    """
    from boomi_mcp.compiler.process_ir.diagnostics import ProcessIRCompileError
    from boomi_mcp.compiler.process_ir.pipeline import compile_process_ir_v1

    parsed = [(name, parse_process_ir_v1(doc)) for name, doc in roots]
    resolution = resolve_process_ir_effect_declarations(
        parsed, declarations, _symbols(), [],
        child_roots={"$ref:" + name: ir for name, ir in parsed} if walkable else None)
    if not resolution.ok:
        return {"ok": False, "rejected": tuple(f.reason for f in resolution.findings),
                "inert": (), "errors": frozenset(), "warnings": frozenset(),
                "compile_errors": frozenset()}
    capabilities = (resolution.capabilities_by_root.get(key)
                    or DEFAULT_VALIDATION_CAPABILITIES)
    report = validate_process_ir(dict(parsed)[key], _symbols(), capabilities=capabilities)
    try:
        compile_process_ir_v1(dict(parsed)[key], _symbols(), capabilities=capabilities)
        compiled = frozenset()
    except ProcessIRCompileError as exc:
        compiled = frozenset((item.code, item.path) for item in exc.diagnostics)
    return {"ok": True, "rejected": (), "inert": tuple(resolution.inert or ()),
            "errors": frozenset((item.code, item.path) for item in report.errors),
            "warnings": frozenset(
                (item.code, item.path) for item in getattr(report, "warnings", ())),
            "compile_errors": compiled}


def _boundary_measurements():
    """Every shape the served rule quantifies over, measured once.

    The keys are the distinctions the SENTENCE makes: a write scope crossed with a caller
    shape, on the walk alone and with a verified declaration; a declaration for a child
    this request carries no definition for; an external-writer declaration with and
    without the flag its read must author; and a declaration whose reference does not
    resolve to a process at all.
    """
    measured = {}
    for scope, (child, write, later, replay_safe) in _BOUNDARY_SCOPES.items():
        for shape, call in _BOUNDARY_SHAPES.items():
            call = dict(call)
            parent = _call_then_read(
                later=later, passthrough=call.pop("passthrough", False), **call)
            roots = [("PARENT", parent), ("WRITER", child)]
            measured[(scope, shape, "walk")] = _cell(roots)
            measured[(scope, shape, "declared")] = _cell(
                roots, _subprocess_declaration("$ref:WRITER", write, replay_safe))
    for shape, call in _BOUNDARY_SHAPES.items():
        call = dict(call)
        passthrough = call.pop("passthrough", False)
        body = {"kind": "branch", "legs": [
            {"steps": [], "terminal": _call("EXTERNAL", **call)}, _READS_K_LATER]}
        root = _doc(_ENTRY, body) if passthrough else _doc(body)
        measured[("unwalkable", shape, "declared")] = _cell(
            [("PARENT", root)],
            _subprocess_declaration("$ref:EXTERNAL", ("dpp", "K"), True), walkable=False)
    for flagged in (True, False):
        read = {"kind": "cache_get", "cache_ref": "$ref:CACHE"}
        roots = [("PARENT", _doc(dict(read, external_writer=True) if flagged else read,
                                 _MSG, _STOP))]
        where = "flagged" if flagged else "unflagged"
        measured[("external_writer", where, "walk")] = _cell(roots)
        measured[("external_writer", where, "declared")] = _cell(
            roots, _external_writer_declaration())
    measured[("identity", "a map", "declared")] = _cell(
        [("PARENT", _call_then_read(**_WAITS_AND_ABORTS)), ("WRITER", _SETS_K)],
        _subprocess_declaration("$ref:M12", ("dpp", "K"), True))
    return measured


def _refused(cell):
    """The composition does not ship: the resolver rejected it, or a finding stands."""
    return not cell["ok"] or bool(cell["errors"])


def _identity_must_resolve(m):
    return not m[("identity", "a map", "declared")]["ok"]


def _content_comes_from_an_authority(m):
    """A declaration alone never supplies content: where the server has no authority the
    finding stands, and the one declaration with no content authority names an assumption
    instead of establishing the write."""
    return (all(bool(m[("unwalkable", shape, "declared")]["inert"])
                and _refused(m[("unwalkable", shape, "declared")])
                for shape in _BOUNDARY_SHAPES)
            and _EXTERNAL_WRITER_ASSUMED in {
                code for code, _ in m[("external_writer", "flagged", "declared")]["warnings"]})


def _uncorroborated_establishes_nothing(m):
    return all(_refused(m[("unwalkable", shape, "declared")]) for shape in _BOUNDARY_SHAPES)


def _inert_with_one_exception(m):
    """INERT is the server's own word, and it has exactly one measured exception: the
    external-writer declaration on a read that authors the flag, which is NOT reported
    inert and replaces the blocking error with a warning."""
    flagged = m[("external_writer", "flagged", "declared")]
    unflagged = m[("external_writer", "unflagged", "declared")]
    return (bool(m[("unwalkable", "gated", "declared")]["inert"])
            and bool(unflagged["inert"]) and _refused(unflagged)
            and not flagged["inert"] and not _refused(flagged)
            and _EXTERNAL_WRITER_ASSUMED in {code for code, _ in flagged["warnings"]}
            and _refused(m[("external_writer", "flagged", "walk")]))


def _the_gated_call_establishes(m):
    """The call `node.process_call` says establishes the child's PROCESS PROPERTIES AND
    CACHES: both scopes clear there, with no declaration anywhere."""
    return all(not _refused(m[(scope, "gated", "walk")]) for scope in ("dpp", "cache"))


def _every_other_call_carries_nothing(m):
    """The walk carries nothing past the other three calls, and a verified declaration is
    what carries it past a WAITED one — never past wait=false, which is why the served
    form is a necessary condition and not "whenever"."""
    return (all(_refused(m[(scope, shape, "walk")])
                for scope in ("dpp", "cache") for shape in _UNGATED)
            and not _refused(m[("dpp", "no_abort", "declared")])
            and not _refused(m[("cache", "no_abort", "declared")])
            and not _refused(m[("dpp", "several_documents", "declared")])
            and _refused(m[("dpp", "no_wait", "declared")])
            and _refused(m[("cache", "no_wait", "declared")]))


def _a_document_property_never_reaches(m):
    """Rule 7's half of the boundary: at EVERY caller shape, on both routes. A verified
    declaration only changes which code refuses it."""
    return all(_refused(m[("ddp", shape, route)])
               for shape in _BOUNDARY_SHAPES for route in ("walk", "declared"))


def _inert_is_not_an_error(m):
    return all(m[("unwalkable", shape, "declared")]["ok"]
               and _refused(m[("unwalkable", shape, "declared")])
               for shape in _BOUNDARY_SHAPES)


#: Every sentence of the served boundary rule, with the MEASUREMENT that makes it true.
#: ``(claim id, the served words, a predicate over the measured verdicts)``.
#:
#: This replaces a hand-list of retired strings plus required clauses. That shape could
#: only catch a universal somebody had already found to be false: a NEW one passed it
#: while #177 stayed green, and only the byte-frozen snapshot noticed — as "text changed",
#: never as "text false". Here a sentence has to name a claim, and a claim has to hold of
#: the compiler, so an added or reworded universal fails at the accounting below or at its
#: own predicate.
_BOUNDARY_CLAIMS = (
    ("identity-resolves",
     "the reference must resolve to a component of the right type",
     _identity_must_resolve),
    ("content-has-an-authority",
     "What the effect IS always comes from a server-side authority.",
     _content_comes_from_an_authority),
    ("uncorroborated-establishes-nothing",
     "one the server cannot corroborate establishes nothing",
     _uncorroborated_establishes_nothing),
    ("no-read-and-no-write",
     "establishes no read and no write: the content of an effect never comes from a "
     "declaration, so one about a step the server cannot inspect establishes nothing "
     "about that step",
     _content_comes_from_an_authority),
    ("inert-with-one-exception",
     "Such a declaration is reported INERT, with one exception that still supplies no "
     "content: an external-writer declaration, on a cache_get that authors "
     "external_writer, replaces that read's blocking missing-writer error with a named "
     "warning; on a read without the flag it is inert like the rest.",
     _inert_with_one_exception),
    ("the-gated-call-establishes",
     "at a call the process call entry says establishes the process properties and "
     "caches the child writes, those writes are established and the strict findings "
     "they clear are cleared",
     _the_gated_call_establishes),
    ("every-other-call-carries-nothing",
     "At every other call the walk carries nothing past the call, and those writes reach "
     "later paths only when a verified declaration states them.",
     _every_other_call_carries_nothing),
    ("a-document-property-never",
     "A document property a child writes is never carried past the call, by the walk or "
     "by a declaration.",
     _a_document_property_never_reaches),
    ("inert-is-not-an-error",
     "Inert is not an error — an unregistered script is a legal thing to author, it just "
     "proves nothing.",
     _inert_is_not_an_error),
)

#: The same three facts as `node.process_call` states them. The boundary rule DEFERS to
#: that entry, so the two must agree on the same payload: these reuse the boundary rule's
#: own predicates rather than a second measurement of the same gates.
_PROCESS_CALL_CLAIMS = (
    ("the-gated-call-establishes",
     "A call with wait=true and abort_on_error=true on a path that provably carries "
     "exactly one document establishes, for later paths, the process properties and "
     "caches its child writes on every normal completion.",
     _the_gated_call_establishes),
    ("every-other-call-carries-nothing",
     "After any other call with wait=true those writes reach later paths only when a "
     "verified subprocess effect declaration states them; after wait=false nothing "
     "carries them, declared or not.",
     _every_other_call_carries_nothing),
    ("a-document-property-never",
     "A document property a child writes never reaches later paths, with or without a "
     "declaration.",
     _a_document_property_never_reaches),
)

#: The text this round replaced, kept VERBATIM as a mutant rather than as a banned string.
#: Its third sentence is false at the external-writer shape measured above.
_THE_TEXT_THIS_ROUND_REPLACED = (
    "A declaration that is omitted, or that passes identity with no server-side "
    "authority behind its content, is INERT: it adds nothing to the analysis and "
    "clears no finding of its own. What a declaration buys is never the content of "
    "an effect — the server takes that from an authority of its own or from nowhere "
    "at all, so a declaration about a step it cannot inspect stays inert — and what "
    "an omitted one withholds is never a finding the server clears on its own "
    "authority. Where it has such an authority — a called child whose process "
    "definition it walks — that walk is used with no declaration anywhere: at a call "
    "the process call entry says establishes the child's writes for later paths, "
    "those writes are established there, and the strict findings they clear are "
    "cleared. At every other call the walk carries nothing past the call, and those "
    "writes reach later paths only when a verified declaration states them. Inert is "
    "not an error — an unregistered script is a legal thing to author, it just "
    "proves nothing.")


def _sentences(text):
    import re

    return [sentence for sentence in re.split(r"(?<=\.)\s+", text) if sentence]


def _served(entry_id):
    from boomi_mcp.authoring.process_ir_projection import (
        process_ir_authoring_revision_payload,
    )
    from test_issue_184_child_entries import _entry_by_id

    entry = _entry_by_id(process_ir_authoring_revision_payload(), entry_id)
    assert entry is not None, entry_id
    return entry


def test_the_declaration_boundary_says_what_an_omitted_declaration_does_not_withhold():
    """The served rule has to be true of the server that serves it, AT EVERY SHAPE IT
    QUANTIFIES OVER (ARCH-184-r1-06 and the correction-batch-18 re-verifications of it).

    Three universals have died in this row, each measured below:

    * "every strict finding it might have cleared still fires" is false at the gated
      caller, where the walk clears the read with no declaration anywhere;
    * "the findings it clears are cleared at every caller" is false at the other three
      callers, where the walk carries nothing past the call;
    * "is INERT: it adds nothing to the analysis and clears no finding of its own" is
      false of an external-writer declaration, which passes identity with no server-side
      content authority and still replaces a blocking error with a named warning — the
      server's own resolver reports it inert only WITHOUT the flag its read authors.

    Each surviving sentence now names a claim, and each claim is checked against what the
    compiler does rather than against a list of the wordings already known to be wrong.
    """
    entry = _served("semantic_rule.effect.declaration_boundary")
    summary = entry["summary"]
    measured = _boundary_measurements()

    # 1. Every claim is stated exactly once, and is TRUE of the server that serves it.
    for claim_id, fragment, predicate in _BOUNDARY_CLAIMS:
        assert summary.count(fragment) == 1, claim_id
        assert predicate(measured), claim_id

    # 2. ACCOUNTING: every served sentence is one of those claims. A sentence nobody
    #    measured cannot be served — which is what makes a NEW universal fail here.
    for sentence in _sentences(summary):
        owners = [claim_id for claim_id, fragment, _ in _BOUNDARY_CLAIMS
                  if fragment in sentence]
        assert len(owners) == 1, (sentence, owners)

    # 3. The rule points at the entry whose gates it defers to instead of copying them.
    assert "node.process_call" in entry["related_entry_ids"], entry["related_entry_ids"]


def test_the_boundary_rule_and_the_process_call_page_agree_on_what_a_call_establishes():
    """The deferral is only sound if the entry deferred TO is true on the same payload.

    The sentence it used to rest on — "Any other call, and every document property a child
    writes, establishes nothing after the call unless a verified subprocess effect
    declaration says so" — over-promised twice: measured, a verified declaration carries
    nothing past wait=false, and carries a DOCUMENT property nowhere at all. Both entries
    are now checked by the SAME predicates, so they cannot drift apart silently.
    """
    import re

    facts = " ".join(_served("node.process_call")["ordering_facts"])
    measured = _boundary_measurements()
    for claim_id, fragment, predicate in _PROCESS_CALL_CLAIMS:
        assert facts.count(fragment) == 1, claim_id
        assert predicate(measured), claim_id
    # Accounting, scoped to the sentences that say what reaches a later path: a new
    # over-promise about it has to be a claim measured above.
    for sentence in _sentences(facts):
        if not re.search(r"for later paths|reach(es)? later paths", sentence):
            continue
        owners = [claim_id for claim_id, fragment, _ in _PROCESS_CALL_CLAIMS
                  if fragment in sentence]
        assert len(owners) == 1, (sentence, owners)


def test_every_boundary_claim_is_falsifiable_by_its_own_measurement():
    """No predicate may be a tautology over the measurement it reads.

    Each is checked against two perturbed worlds — one where every shape is admitted and
    one where every shape is refused and every declaration inert — and must reject at
    least one. A claim that passes both is measuring nothing.
    """
    measured = _boundary_measurements()
    admitted = {"ok": True, "rejected": (), "inert": (), "errors": frozenset(),
                "warnings": frozenset(), "compile_errors": frozenset()}
    refused = {"ok": True, "rejected": (), "inert": ("/effect_declarations/x/0",),
               "errors": frozenset({("SOME_CODE", "/body")}), "warnings": frozenset(),
               "compile_errors": frozenset({("SOME_CODE", "/body")})}
    all_admitted = {key: admitted for key in measured}
    all_refused = {key: refused for key in measured}
    for claim_id, _fragment, predicate in _BOUNDARY_CLAIMS + _PROCESS_CALL_CLAIMS:
        assert predicate(measured), claim_id
        assert not (predicate(all_admitted) and predicate(all_refused)), claim_id


def _fails_the_witness(monkeypatch, prose, marker):
    """Serve `prose` as the boundary rule's authority text and re-run the witness.

    `marker` is a fragment UNIQUE to the mutant, asserted to be served before the witness
    runs. A tail comparison would not do: the replaced wording and its replacement share a
    closing sentence, so it would pass whether or not the mutant ever reached the payload
    — which is exactly how an earlier attempt at this measurement proved nothing, the
    entries being memoized behind `reset_process_ir_authoring_cache`.
    """
    from boomi_mcp.authoring import process_ir_projection as projection

    monkeypatch.setattr(
        projection, "_EFFECT_AUTHORITY_PROSE",
        dict(projection._EFFECT_AUTHORITY_PROSE, **{_AUTHORITY: prose}))
    projection.reset_process_ir_authoring_cache()
    try:
        assert marker in _served("semantic_rule.effect.declaration_boundary")["summary"]
        with pytest.raises(AssertionError) as caught:
            test_the_declaration_boundary_says_what_an_omitted_declaration_does_not_withhold()
        return str(caught.value)
    finally:
        monkeypatch.undo()
        projection.reset_process_ir_authoring_cache()


def test_a_new_false_universal_in_the_boundary_rule_fails_the_witness(monkeypatch):
    """The re-verification's own mutant: the previous witness stayed GREEN when this
    sentence was added to the served text, and so did #177 — only the byte-frozen snapshot
    moved, and regenerating it silences that without anyone evaluating the claim.

    It is measurably false: an external-writer declaration is exactly a declaration the
    server cannot corroborate, and it changes a blocking error into a warning.
    """
    measured = _boundary_measurements()
    flagged = measured[("external_writer", "flagged", "declared")]
    assert _refused(measured[("external_writer", "flagged", "walk")])
    assert not _refused(flagged) and flagged["warnings"] and not flagged["inert"]

    marker = "never changes the severity of any finding"
    assert marker not in _served("semantic_rule.effect.declaration_boundary")["summary"]
    message = _fails_the_witness(
        monkeypatch,
        _EFFECT_AUTHORITY_PROSE_FOR_TEST()
        + " A declaration the server cannot corroborate " + marker + ".",
        marker)
    # It fails as an UNACCOUNTED sentence: nothing measured it, so it cannot be served.
    assert marker in message, message


def test_the_text_this_round_replaced_fails_the_witness(monkeypatch):
    """Neutralising the correction: the previous round's served wording, verbatim.

    This is the measurement a served-string fix is allowed instead of a monkeypatched
    decision, and it is a real in-process one. It also has to fail for the RIGHT reason —
    the claim the old text never stated — rather than because some string moved.
    """
    marker = "is INERT: it adds nothing to the analysis and clears no finding of its own"
    assert marker not in _served("semantic_rule.effect.declaration_boundary")["summary"]
    message = _fails_the_witness(monkeypatch, _THE_TEXT_THIS_ROUND_REPLACED, marker)
    # The RIGHT reason, DERIVED rather than hand-picked: the failure names one of the
    # claims the replaced wording never stated. Naming a single id here would pin the
    # order the claims happen to be checked in, not the defect being measured.
    unstated = [claim_id for claim_id, fragment, _ in _BOUNDARY_CLAIMS
                if fragment not in _THE_TEXT_THIS_ROUND_REPLACED]
    assert unstated, "the replaced wording would satisfy every claim"
    assert any(claim_id in message for claim_id in unstated), (message, unstated)


def _EFFECT_AUTHORITY_PROSE_FOR_TEST():
    from boomi_mcp.authoring.process_ir_projection import _EFFECT_AUTHORITY_PROSE

    return _EFFECT_AUTHORITY_PROSE[_AUTHORITY]


def test_the_boundary_shapes_still_get_the_verdicts_the_rule_was_written_against():
    """Control: the verdicts behind the claims, pinned once, so a behaviour drift that
    makes a claim vacuously true is caught here rather than passing quietly.

    Validate and compile must agree at every cell: the served rule makes no distinction
    between them, so a divergence would make it true of one route and false of the other.
    """
    measured = _boundary_measurements()
    for key, cell in sorted(measured.items()):
        assert cell["errors"] == cell["compile_errors"], key
    assert measured[("dpp", "gated", "walk")]["errors"] == frozenset()
    assert measured[("cache", "gated", "walk")]["errors"] == frozenset()
    assert measured[("dpp", "no_wait", "walk")]["errors"] == {
        (_READ_BEFORE_WRITE, _LATER_READ), (_ORDERING_UNSAFE, _LATER_READ)}
    assert measured[("dpp", "no_abort", "walk")]["errors"] == {
        (_READ_BEFORE_WRITE, _LATER_READ)}
    assert measured[("dpp", "several_documents", "walk")]["errors"] == {
        (_READ_BEFORE_WRITE, "/body/steps/1/legs/1/steps/0")}
    assert measured[("cache", "no_abort", "walk")]["errors"] == {
        (_CACHE_WRITER_MISSING, _LATER_READ)}
    # rule 7: the gated call is the shape a document property most looks established at,
    # and it is refused there too — a verified declaration only changes the code.
    assert measured[("ddp", "gated", "walk")]["errors"] == {
        (_READ_BEFORE_WRITE, _LATER_READ)}
    assert measured[("ddp", "gated", "declared")]["errors"] == {
        (_DDP_SCOPE_INVALID, _LATER_READ)}
    assert measured[("external_writer", "flagged", "declared")]["warnings"] == {
        (_EXTERNAL_WRITER_ASSUMED, "/body/steps/0")}
    assert measured[("external_writer", "unflagged", "declared")]["inert"] == (
        "/effect_declarations/external_writers/0",)
    assert measured[("identity", "a map", "declared")]["rejected"] == ("unbound",)


# ---------------------------------------------------------------------------
# Correction batch 18, pre-commit verification: the coverage cells that were
# asserted and never exercised
# ---------------------------------------------------------------------------

_GETP1 = {"kind": "connector_call", "operation_ref": "$ref:GETP1"}
_TO_P2 = {"kind": "map_ref", "map_ref": "$ref:M12"}
_CONSUMES_P2 = {"kind": "map_ref", "map_ref": "$ref:M22"}
#: ONE staging leg storing both a profile a consumer needs and a property a path binds
_STAGES_CONTENT_AND_X = {"steps": [_GETP1, _TO_P2, _DYNAMIC_X], "terminal": _PUT}
_USES_BOTH = _legs({"steps": [_READ, _CONSUMES_P2], "terminal": _STOP},
                   {"steps": [_READ, _BOUND_GET], "terminal": _STOP})
_DUAL_SEED = [("PARENT", _legs(_STAGES_CONTENT_AND_X,
                               {"steps": [], "terminal": _call("CACHE_CHILD")})),
              ("CACHE_CHILD", _USES_BOTH)]
_AT_THE_CHILDS_BINDING = "/body/steps/0/legs/1/steps/1/path_binding"

_BINDS_X = _doc(_ENTRY, _BOUND_GET, _STOP)
_READS_X_CHILD = _doc(_ENTRY, _READS_X, _STOP)


def _cached_origin_chain(child_key, child):
    """PARENT stages X into the cache; MID retrieves it and hands the documents on."""
    return [("PARENT", _legs({"steps": [_GETP1, _DYNAMIC_X], "terminal": _PUT},
                             {"steps": [], "terminal": _call("MID")})),
            ("MID", _legs({"steps": [_READ], "terminal": _call(child_key, wait=True)},
                          {"steps": [_MSG], "terminal": _STOP})),
            (child_key, child)]


def test_one_caller_filled_cache_carries_content_and_a_cached_property_at_once():
    """The cell the entry seed's cohort guard implements: ONE cache the child needs both a
    profile and a document property of. The content seed and the cohort seed name the same
    cache, and an unknown cohort seeded beside the named one would be met with it — cohorts
    are a MAY set — un-proving the property the contract just carried across."""
    row = _row(_DUAL_SEED, "PARENT", "CACHE_CHILD")
    assert row.cache_requirements == (("$ref:CACHE", "$ref:P2"),)
    assert row.cache_property_requirements == (("$ref:CACHE", "X", None, True),)
    for key in ("PARENT", "CACHE_CHILD"):
        assert _errors(_DUAL_SEED, key) == [], key
        assert _compile_errors(_DUAL_SEED, key) == [], key


def test_the_cohort_seed_guard_is_load_bearing(monkeypatch):
    """Non-vacuity: seeding the unknown cohort unconditionally refuses the bound path of a
    child whose caller stored exactly the property it binds."""
    assert _errors(_DUAL_SEED, "CACHE_CHILD") == []
    monkeypatch.setattr(lineage, "_seeds_an_unknown_cohort",
                        lambda cache_ref, cohort_names: True)
    assert (_NOT_ESTABLISHED, _AT_THE_CHILDS_BINDING) in _errors(_DUAL_SEED, "CACHE_CHILD")


def test_a_call_owes_a_bound_writer_of_retrieved_documents_to_the_cache():
    """The cell the call-side origin argument implements for a child's bound path: the
    writer MID owes is not MID's own, it is whatever stored the documents it retrieved, so
    the obligation crosses as that cache's requirement instead of MID's own defect."""
    roots = _cached_origin_chain("BOUND", _BINDS_X)
    assert _row(roots, "PARENT", "MID").cache_property_requirements == (
        ("$ref:CACHE", "X", None, True),)
    for key in ("PARENT", "MID", "BOUND"):
        assert _errors(roots, key) == [], key


def test_a_call_owes_an_unmet_read_of_retrieved_documents_to_the_cache():
    """The same cell for a child's ordinary read, which needs establishment only, so the
    row it records is the unbound one."""
    roots = _cached_origin_chain("READS_X", _READS_X_CHILD)
    assert _row(roots, "PARENT", "MID").cache_property_requirements == (
        ("$ref:CACHE", "X", None, False),)
    for key in ("PARENT", "MID", "READS_X"):
        assert _errors(roots, key) == [], key


#: A SCHEDULED child that reads a document property. `derive_child_entry_facts` drops every
#: document-property read from a scheduled child's `required_reads`, so its contract carries
#: this read nowhere: only a verified subprocess summary states it at the call.
from test_issue_184_child_entries import _READS_X as _SCHEDULED_READS_X  # noqa: E402

#: The middle process: it retrieves a cache no write of its own reached, then calls that
#: child — the shape in which the summary's declared read is the ONLY thing that can make
#: the property an obligation of whoever filled the cache.
_CALLS_A_SUMMARISED_CHILD = _legs(
    {"steps": [_READ], "terminal": _call("CHILD", **_WAITS_AND_ABORTS)},
    {"steps": [_MSG], "terminal": _STOP})
_SUMMARISED_CALLERS = {
    "a_scheduled_middle": _CALLS_A_SUMMARISED_CHILD,
    "a_passthrough_middle": _parent([_READ], _call("CHILD", **_WAITS_AND_ABORTS)),
}
#: The control: this process's own write reached the cache, so nothing is inherited.
_FILLS_IT_THEN_CALLS = _legs(
    _STAGES_X, {"steps": [_READ], "terminal": _call("CHILD", **_WAITS_AND_ABORTS)})
#: One level up, against the row the middle process derived.
_STORES_X_THEN_CALLS_MID = _legs(_STAGES_X, {"steps": [], "terminal": _call("MID")})
_STORES_NO_X_THEN_CALLS_MID = _legs(
    {"steps": [_GET], "terminal": _PUT}, {"steps": [], "terminal": _call("MID")})


def _summarised(child_doc):
    """The child's effect as the SERVER derives it, handed over as trusted context.

    Never a hand-written effect: `derive_subprocess_effect` is the authority a caller's
    subprocess declaration is verified against, so this is the summary the public declared
    route produces for this child and nothing here is the test's own invention.
    """
    from boomi_mcp.authoring.process_ir_effects import derive_subprocess_effect
    from boomi_mcp.compiler.process_ir.semantic_validation.contracts import (
        ProcessIRValidationCapabilitiesV1,
        StateEffectV1,
        SubprocessSummaryV1,
    )

    reads, writes, _replay_safe = derive_subprocess_effect(
        parse_process_ir_v1(child_doc), capabilities=None, symbols=_symbols()).effect
    return ProcessIRValidationCapabilitiesV1(subprocess_summaries=(
        SubprocessSummaryV1(process_ref="$ref:CHILD",
                            effect=StateEffectV1(reads=tuple(reads), writes=tuple(writes))),))


def _derived_facts(caller_doc, capabilities):
    from boomi_mcp.authoring.process_ir_effects import derive_child_entry_facts

    return derive_child_entry_facts(
        parse_process_ir_v1(caller_doc), _symbols(), capabilities)


def _errors_against(caller_doc, capabilities):
    report = validate_process_ir(
        parse_process_ir_v1(caller_doc), _symbols(), capabilities=capabilities)
    return [(item.code, item.path) for item in report.errors]


@pytest.mark.parametrize("caller", sorted(_SUMMARISED_CALLERS))
def test_a_declared_read_of_retrieved_documents_is_owed_to_the_cache_that_filled_them(caller):
    """The third cached-origin channel, and the only one a SCHEDULED child has.

    A trusted contract's declared read is checked at the call against the path's stream. Past
    a retrieve of a cache no write of this process reached, the property can only have been
    stored by whoever filled that cache, so it is that cache's requirement — exactly as the
    bound and the ordinary-read channels record their own.
    """
    facts = _derived_facts(_SUMMARISED_CALLERS[caller], _summarised(_SCHEDULED_READS_X))
    assert facts["cache_property_requirements"] == (("$ref:CACHE", "X", None, False),)
    # It is the CACHE's requirement, never a demand that the caller establish the property
    # as its own state: the property rides on the cached documents.
    assert ("ddp", "X") not in facts.get("required_reads", ())


def test_a_middles_own_write_to_the_cache_leaves_a_declared_read_nothing_to_inherit():
    """The control the channel rests on: when this process's own write reached the cache,
    the documents are its own, so the declared read inherits nothing."""
    facts = _derived_facts(_FILLS_IT_THEN_CALLS, _summarised(_SCHEDULED_READS_X))
    assert facts["cache_property_requirements"] == ()


def test_what_a_declared_reads_cache_row_is_worth_at_the_caller():
    """What the row buys, measured one level up against the contract the server derived:
    the caller that stored the property is admitted, and the caller that stored documents
    without it is refused at its call rather than at the grandchild."""
    row = ChildEntryContractV1(process_ref="$ref:MID", **_derived_facts(
        _CALLS_A_SUMMARISED_CHILD, _summarised(_SCHEDULED_READS_X)))
    assert row.cache_property_requirements == (("$ref:CACHE", "X", None, False),)
    from boomi_mcp.compiler.process_ir.semantic_validation.contracts import (
        ProcessIRValidationCapabilitiesV1,
    )

    held = ProcessIRValidationCapabilitiesV1(child_entry_contracts=(row,))
    assert _errors_against(_STORES_X_THEN_CALLS_MID, held) == []
    assert _READ_BEFORE_WRITE in {
        code for code, _path in _errors_against(_STORES_NO_X_THEN_CALLS_MID, held)}


def test_the_declared_read_channel_is_load_bearing(monkeypatch):
    """Non-vacuity, and the measurement that fixes WHICH failure the rule prevents: with the
    cached-origin rule neutralised the obligation does not simply move. For a scheduled
    middle it disappears — its contract drops document-property reads, so nothing is owed to
    anybody — and for a passthrough middle it becomes a demand that the caller establish the
    property as its own state, which no caller of a cache-filling chain can satisfy."""
    capabilities = _summarised(_SCHEDULED_READS_X)
    monkeypatch.setattr(lineage, "_caller_cached_origin",
                        lambda key, stream, invalidated: None)
    scheduled = _derived_facts(_CALLS_A_SUMMARISED_CHILD, capabilities)
    passthrough = _derived_facts(_SUMMARISED_CALLERS["a_passthrough_middle"], capabilities)
    assert scheduled["cache_property_requirements"] == ()
    assert ("ddp", "X") not in scheduled.get("required_reads", ())
    assert passthrough["cache_property_requirements"] == ()
    assert ("ddp", "X") in passthrough["required_reads"]


def test_a_catch_carried_stream_never_reaches_a_call_that_consults_its_marker():
    """Why the catch edge's native-work reset cannot change a verdict, measured rather than
    asserted. The marker's only consumer is a terminal call's prefix evidence key, which
    exists solely in a Branch leg or a Decision true arm — and the catch body's step
    vocabulary carries no control kind at all, so neither can be authored inside one. The
    one call a catch body admits is its own terminal, whose body names no prefix context.

    The ordinary-path twin is measured beside it, so this is not a claim about a rule that
    never runs: there the same marker is consulted and answered.
    """
    from boomi_mcp.models import process_ir as model

    assert set(model.CATCH_BODY_KINDS) & {"branch", "decision", "try_catch"} == set()
    assert set(model.PROCESS_CALL_PREFIX_CONTEXTS) == {"branch_leg", "decision_true_arm"}

    leg_call = {"kind": "branch", "legs": [
        {"steps": [], "terminal": _call("CHILD", **_WAITS_AND_ABORTS)},
        {"steps": [_MSG], "terminal": _STOP}]}
    try:
        parse_process_ir_v1(_doc(_ENTRY, {
            "kind": "try_catch", "scope": "process",
            "try_body": {"steps": [_GET], "terminal": _STOP},
            "catch_body": {"steps": [leg_call], "terminal": _STOP}}))
        raise AssertionError("a Branch inside a catch body must be refused")
    except Exception as exc:                                   # the model's own refusal
        assert "NODE_NOT_ALLOWED_IN_BODY" in str(exc), exc

    recorded = []
    real = lineage._prefix_predecessor

    def recording(context, own, marker, parent_form):
        value = real(context, own, marker, parent_form)
        recorded.append((context, marker, value))
        return value

    ordinary = _doc(_ENTRY, _P2_PREFIX[0], leg_call)
    recovery = _doc({"kind": "try_catch", "scope": "process",
                     "try_body": {"steps": [_GET], "terminal": _STOP},
                     "catch_body": {"steps": [],
                                    "terminal": _call("CHILD", **_WAITS_AND_ABORTS)}})
    try:
        lineage._prefix_predecessor = recording
        for roots, key in ((( ("PARENT", ordinary), ("CHILD", _CHILD)), "PARENT"),
                           ((("PARENT", recovery), ("CHILD", _CHILD)), "PARENT")):
            recorded.append(("--", None, None))
            _errors(list(roots), key)
    finally:
        lineage._prefix_predecessor = real

    keys = [row for row in recorded if row[0] != "--"]
    boundary = recorded.index(("--", None, None), 1)
    on_the_path = [row for row in recorded[:boundary] if row[0] != "--"]
    in_the_catch = [row for row in recorded[boundary:] if row[0] != "--"]
    assert keys, "no call discharged a prefix key at all"
    # On the ordinary path the marker is consulted AND answered...
    assert [row for row in on_the_path if row[0] == "branch_leg" and row[1] is not None]
    # ...while the recovery call sits outside every prefix context, so whatever the catch
    # edge left in the marker is never read.
    assert in_the_catch and all(row[0] is None and row[2] is None for row in in_the_catch)


def test_each_call_side_cached_origin_is_load_bearing(monkeypatch):
    """Non-vacuity, one witness per argument: with the origin rule neutralised each site
    keeps a DIFFERENT refusal — the inherited binding at the call's `/process_ref`, the
    inherited read at the call's terminal — so neither row rests on the other's argument."""
    bound = _cached_origin_chain("BOUND", _BINDS_X)
    ordinary = _cached_origin_chain("READS_X", _READS_X_CHILD)
    assert _errors(bound, "MID") == []
    assert _errors(ordinary, "MID") == []
    monkeypatch.setattr(lineage, "_caller_cached_origin",
                        lambda key, stream, invalidated: None)
    assert (_NOT_ESTABLISHED, "/body/steps/0/legs/0/terminal/process_ref") in _errors(bound, "MID")
    assert (_READ_BEFORE_WRITE, "/body/steps/0/legs/0/terminal") in _errors(ordinary, "MID")
    assert _row(bound, "PARENT", "MID").cache_property_requirements == ()
    assert _row(ordinary, "PARENT", "MID").cache_property_requirements == ()
