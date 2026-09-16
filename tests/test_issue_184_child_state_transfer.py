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
    PROCESS_IR_SEMANTIC_LINEAGE_BRANCH_ORDER_INVALID,
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
    ("sealed", "requirement"):
        "withheld: a caller owes nothing for a cache the CHILD emptied — the seal decides "
        "whose obligation a cached property is inside one process, at its own calls",
    ("sealed", "possible_effect"):
        "carried by `removed_caches`: what crosses the boundary is the removal itself, and "
        "each caller seals its own path from it",
    ("sealed", "guaranteed_effect"):
        "withheld: a caller's own later writes can refill the cache, so a child's removal "
        "guarantees its caller no such exclusivity",
    ("proved", "requirement"):
        "withheld: a child requires no proof of its caller's, only the state itself — what "
        "it needs before the call is `required_reads`",
    ("proved", "possible_effect"):
        "carried by `mutated_state`: a POSSIBLE write is recorded whether or not the path "
        "proves it runs, which is what makes this compartment a separate question",
    # The guarantee is the MEET of the two execution-scoped compartments: established at
    # every normal exit, and established there by a write the path proved would run.
    ("proved", "guaranteed_effect"): ("guaranteed_state",),
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
                        lambda cache_ref, name, capabilities, state: False)
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
#: empties it first and fills it again — but BEHIND a retrieve, so the refill may not run
_EMPTIES_THEN_PUTS = _legs(_EMPTIES_IT, _FILLS_THE_CACHE)
#: A cache write on a path the walk proves runs. The documents in front of it are the
#: PASSTHROUGH entry's — its caller's group, which the gate at that call proved non-empty —
#: handed on one for one by the Message. Every root below that uses this leg is therefore a
#: passthrough root: on a scheduled root an Add to Cache has no documents to store and is
#: refused A4 CARDINALITY_MISMATCH, so a scheduled "proved fill" is not authorable at all
#: and the witnesses that used one pinned caller verdicts against a child that cannot ship
#: (round r19b).
_FILLS_ON_A_PROVED_PATH = {"steps": [_MSG], "terminal": _PUT}


def _passthrough_root(*legs):
    """A Data Passthrough root whose Branch runs ``legs``."""
    return _doc(_ENTRY, {"kind": "branch", "legs": list(legs)})
#: The same empty-then-refill authored as a PASSTHROUGH child, so the CHILD ITSELF is
#: valid. An Add to Cache on the bare scheduled entry is refused A4 CARDINALITY_MISMATCH,
#: which is the only reason round r18's control dropped the child from its clean-verdict
#: loop rather than fixing its fixture. A passthrough entry hands the leg its caller's
#: documents, so the put is legal AND on a path the walk proves — and the control can
#: assert what it asserted before r18: the caller and the child are both clean (round r19).
_PASSTHROUGH_EMPTIES_THEN_REFILLS = _doc(_ENTRY, {"kind": "branch", "legs": [
    _EMPTIES_IT, {"steps": [], "terminal": _PUT}, {"steps": [_MSG], "terminal": _STOP}]})
#: fills the cache where the walk proves the write runs, then empties it on the next leg.
#: The removal is then the ONLY reason the guarantee does not cross, which is what the
#: witnesses below neutralise one hunk at a time (round r18).
_PUTS_ON_A_PROVED_PATH_THEN_EMPTIES = _passthrough_root(_FILLS_ON_A_PROVED_PATH, _EMPTIES_IT)
_ONLY_EMPTIES = _legs(_EMPTIES_IT, {"steps": [_MSG], "terminal": _STOP})
_AT_THE_CACHE_READ = "/body/steps/0/legs/1/steps/0"


def _calls_then_reads_the_cache(child_key, **call):
    """A scheduled parent: one leg calls the child, the next reads the cache it wrote."""
    return _legs({"steps": [], "terminal": _call(child_key, **dict(_WAITS_AND_ABORTS, **call))},
                 {"steps": [_READ, _MSG], "terminal": _STOP})


def _removal_chain(**inner):
    """PARENT -> MID -> HIDES, where MID fills the cache and HIDES empties it."""
    return [("PARENT", _calls_then_reads_the_cache("MID")),
            ("MID", _passthrough_root(
                _FILLS_ON_A_PROVED_PATH,
                {"steps": [], "terminal": _call("HIDES", **dict(_WAITS_AND_ABORTS, **inner))})),
            ("HIDES", _ONLY_EMPTIES)]


def test_a_child_that_empties_the_cache_guarantees_it_to_nobody():
    """Amendment 3 §8: a terminal remove invalidates execution-cache guarantees even though
    it emits no documents. The child fills the cache on one leg and empties it on the next,
    so the caller's later read of that cache is refused again — the guarantee this batch
    added must not outlive the removal that undid the write behind it."""
    roots = [("PARENT", _calls_then_reads_the_cache("WRITER")),
             ("WRITER", _PUTS_ON_A_PROVED_PATH_THEN_EMPTIES)]
    row = _row(roots, "PARENT", "WRITER")
    assert row.removed_caches == ("$ref:CACHE",)
    assert row.guaranteed_state == ()
    assert (_CACHE_WRITER_MISSING, _AT_THE_CACHE_READ) in _errors(roots, "PARENT")
    assert (_CACHE_WRITER_MISSING, _AT_THE_CACHE_READ) in _compile_errors(roots, "PARENT")
    # A refill BEHIND A RETRIEVE does not bring the guarantee back: a retrieve that returns
    # nothing leaves the Add to Cache unrun while the child still completes normally, so the
    # caller's later read is refused for that child too (amendment 1 rule 7). Round r18
    # corrected this control, which pinned the opposite; the defect it witnesses is
    # unchanged, and the refill's own case is measured just below.
    behind_a_retrieve = [("PARENT", _calls_then_reads_the_cache("WRITER")),
                         ("WRITER", _EMPTIES_THEN_PUTS)]
    assert _row(behind_a_retrieve, "PARENT", "WRITER").guaranteed_state == ()
    assert _row(behind_a_retrieve, "PARENT", "WRITER").removed_caches == ("$ref:CACHE",)
    assert (_CACHE_WRITER_MISSING, _AT_THE_CACHE_READ) in _errors(behind_a_retrieve, "PARENT")
    # CONTROL: the same child whose refill runs on a path the walk PROVES still guarantees
    # the cache, so the correction refuses the removal and not the removal step. The child
    # is a PASSTHROUGH root so that it is valid on its own, and BOTH roots are asserted
    # clean — round r18 repaired this control by deleting the child half of that assertion,
    # its scheduled fixture being refused A4 CARDINALITY_MISMATCH; a fixture satisfying the
    # original assertion exists, so the assertion is restored rather than dropped (r19).
    refilled = [("PARENT", _calls_then_reads_the_cache("WRITER")),
                ("WRITER", _PASSTHROUGH_EMPTIES_THEN_REFILLS)]
    assert _row(refilled, "PARENT", "WRITER").guaranteed_state == (("cache", "$ref:CACHE"),)
    assert _row(refilled, "PARENT", "WRITER").removed_caches == ("$ref:CACHE",)
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
    own = [("PARENT", _calls_then_reads_the_cache("WRITER")),
           ("WRITER", _PUTS_ON_A_PROVED_PATH_THEN_EMPTIES)]
    chain = _removal_chain()
    assert _errors(own, "PARENT") != []
    assert _errors(chain, "PARENT") != []

    with monkeypatch.context() as patched:
        patched.setattr(lineage, "_after_a_whole_cache_removal",
                        lambda state, cache_ref, proved_to_run: state.without_content(cache_ref))
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


# ---------------------------------------------------------------------------
# Stage-2 review round r18: ONE rule for a proved removal, at every site
# ---------------------------------------------------------------------------


def _forwards_to(key):
    """A pure forwarder: one leg calls ``key`` and the other touches no cache."""
    return _legs({"steps": [], "terminal": _call(key, **_WAITS_AND_ABORTS)},
                 {"steps": [_MSG], "terminal": _STOP})


#: Every way a proved removal reaches a caller: the child's own body, and one or two pure
#: forwarding hops. The removing leaf is `_EMPTIES_AND_CALLS_UNKNOWN` in each, so its cache
#: writes are NOT all known — which is the point: `mutated_state` is rightly blanked for it
#: and the removal beside it must survive anyway, at whatever distance.
_REMOVAL_REACHES = {
    "the_childs_own_body": [("PARENT", _fills_calls_then_reads("MID")),
                            ("MID", _EMPTIES_AND_CALLS_UNKNOWN)],
    "one_forwarding_hop": [("PARENT", _fills_calls_then_reads("MID")),
                           ("MID", _forwards_to("HIDES")),
                           ("HIDES", _EMPTIES_AND_CALLS_UNKNOWN)],
    "two_forwarding_hops": [("PARENT", _fills_calls_then_reads("MIDP")),
                            ("MIDP", _forwards_to("MID")),
                            ("MID", _forwards_to("HIDES")),
                            ("HIDES", _EMPTIES_AND_CALLS_UNKNOWN)],
}


@pytest.mark.parametrize("channel", sorted(_REMOVAL_REACHES))
def test_a_proved_removal_reaches_every_caller_it_travels_to(channel):
    """Amendment 3 §8: a removal the walk proves is an EXISTENCE claim, so every hop carries
    it whether or not that hop's child has all its cache writes known.

    Batch 18 ungated the child's own body and the call that consumes the list, and left the
    inheritance hop inside the write-completeness gate — so a process that only FORWARDED
    the call dropped the removal, and a parent that filled the cache, called the forwarder
    and read the cache afterwards was admitted while the identical composition one hop
    closer was refused. It compounded: each further forwarder dropped it again."""
    roots = _REMOVAL_REACHES[channel]
    keys = [key for key, _ir in roots]
    for index in range(len(keys) - 1):
        assert _row(roots, keys[index], keys[index + 1]).removed_caches == ("$ref:CACHE",), (
            channel, keys[index])
    assert (_CACHE_WRITER_MISSING, _AT_A_LATER_LEGS_READ) in _errors(roots, "PARENT"), channel
    assert (_CACHE_WRITER_MISSING, _AT_A_LATER_LEGS_READ) in _compile_errors(roots, "PARENT")


def test_unknowability_alone_still_removes_nothing_through_a_forwarder():
    """The control the one rule must not break: `contract is None` contributes no removal, so
    a chain of forwarders ending at a process nothing derives leaves its caller's cache
    established — the rule the round before this one established, at a new distance."""
    nothing = [("PARENT", _fills_calls_then_reads("MID")), ("MID", _forwards_to("HIDES")),
               ("HIDES", _forwards_to("EXTERNAL"))]
    assert _row(nothing, "PARENT", "MID").removed_caches == ()
    assert _row(nothing, "MID", "HIDES").removed_caches == ()
    assert _errors(nothing, "PARENT") == []
    assert _compile_errors(nothing, "PARENT") == []
    # ... while a forwarded remover whose own cache writes ARE all known is refused as before
    plain = [("PARENT", _fills_calls_then_reads("MID")), ("MID", _forwards_to("HIDES")),
             ("HIDES", _ONLY_EMPTIES)]
    assert _row(plain, "PARENT", "MID").removed_caches == ("$ref:CACHE",)
    assert (_CACHE_WRITER_MISSING, _AT_A_LATER_LEGS_READ) in _errors(plain, "PARENT")


def test_the_one_rule_every_removal_site_reads_is_load_bearing(monkeypatch):
    """Non-vacuity of the structural fix: with the write-completeness gate restored INSIDE
    the one rule, the forwarded removal disappears from the contract AND the direct one stops
    reaching its consumer — both, from one perturbation, because there is only one site left
    to gate. That is what replacing the enumeration with an invariant bought."""
    forwarded = _REMOVAL_REACHES["one_forwarding_hop"]
    direct = _REMOVAL_REACHES["the_childs_own_body"]
    assert _errors(forwarded, "PARENT") != []
    assert _errors(direct, "PARENT") != []

    def gated_by_write_completeness(contract):
        if contract is None or not (contract.state_known or contract.cache_writes_known):
            return ()
        return tuple(contract.removed_caches)

    monkeypatch.setattr(lineage, "proved_removals", gated_by_write_completeness)
    assert _row(forwarded, "PARENT", "MID").removed_caches == ()
    assert _errors(forwarded, "PARENT") == []
    assert _errors(direct, "PARENT") == []


#: Every place in the server that READS a child contract's `removed_caches`, by file and
#: enclosing function, with why it is the only one there.
_REMOVAL_READERS = {
    ("compiler/process_ir/semantic_validation/lineage.py", "proved_removals"):
        "THE rule: every producer and consumer of a proved removal asks it",
    ("compiler/process_ir/semantic_validation/context.py", "contract"):
        "not a reader: the cache-identity canonicalization REWRITES the field into one "
        "spelling per component and decides nothing about it",
}


def test_every_proved_removal_is_read_through_one_rule():
    """The structural fix for `call-side-unknowability-modelled-as-a-removal`, whose second
    instance was the forwarding hop. An enumeration — one gate per site, each free to
    disagree with the next — is replaced by an invariant stated once, and this is what keeps
    it one: every attribute read of `removed_caches` in the server is either that rule or a
    justified non-decision, so a fourth site cannot quietly gate it differently.

    The sibling sweep it enumerates is the three ways a proved removal reaches a caller —
    the child's own body and a child it calls, which produce the field in
    `derive_child_entry_facts`, and the call that consumes it in `_caches_a_call_may_remove`
    — each measured end to end by the channel test above."""
    import ast

    source_root = _ROOT / "src" / "boomi_mcp"
    found = set()

    def walk(node, owner, relative):
        for child in ast.iter_child_nodes(node):
            if isinstance(child, ast.Attribute) and child.attr == "removed_caches":
                found.add((relative, owner))
            walk(child,
                 child.name if isinstance(child, (ast.FunctionDef, ast.AsyncFunctionDef)) else owner,
                 relative)

    for path in sorted(source_root.rglob("*.py")):
        walk(ast.parse(path.read_text(encoding="utf-8")), None, str(path.relative_to(source_root)))
    assert found == set(_REMOVAL_READERS), {
        "unjustified": sorted(found - set(_REMOVAL_READERS)),
        "justified_but_absent": sorted(set(_REMOVAL_READERS) - found),
    }


# ---------------------------------------------------------------------------
# Stage-2 review round r18: a guarantee only for writes the path proves it runs
# ---------------------------------------------------------------------------

#: The finding's own child: a connector retrieve, then the writer whose value its caller reads.
_BEHIND_A_RETRIEVE = _doc(_GET, _SET_K, _STOP)


def test_a_child_guarantees_only_what_its_own_path_proves_it_runs():
    """Amendment 1 rule 7: "Child state changes need conservative transfer". A retrieve that
    returns no documents leaves the steps behind it unrun while the process still completes
    NORMALLY, so a write behind one is established on no completion a caller can rely on and
    no guarantee crosses the boundary for it. The lattice's in-process stance is unchanged —
    inside one process a read is still assumed to produce documents for the steps behind
    it — and the child is still valid on its own; only what it EXPORTS narrows."""
    behind = [("PARENT", _call_then_read(**_WAITS_AND_ABORTS)), ("WRITER", _BEHIND_A_RETRIEVE)]
    assert _row(behind, "PARENT", "WRITER").guaranteed_state == ()
    assert _errors(behind, "WRITER") == []
    assert (_READ_BEFORE_WRITE, _LATER_READ) in _errors(behind, "PARENT")
    assert (_READ_BEFORE_WRITE, _LATER_READ) in _compile_errors(behind, "PARENT")
    # CONTROL: the same write with nothing in front of it keeps its guarantee.
    proved = [("PARENT", _call_then_read(**_WAITS_AND_ABORTS)), ("WRITER", _SETS_K)]
    assert _row(proved, "PARENT", "WRITER").guaranteed_state == (("dpp", "K"),)
    assert _errors(proved, "PARENT") == []
    # CONTROL, capture `cap184-dpp-both-ways`: a passthrough child receives its caller's
    # documents as one group, which the gate at that call already proved non-empty, so
    # everything behind its entry runs and the property still reaches the parent's later leg.
    passthrough = [("PARENT", _call_then_read(**_WAITS_AND_ABORTS)),
                   ("WRITER", _doc(_ENTRY, _arms([_SET_K], [_SET_K])))]
    assert _row(passthrough, "PARENT", "WRITER").guaranteed_state == (("dpp", "K"),)
    assert _errors(passthrough, "PARENT") == []


#: Two legs that BOTH write K, one on a proved path and one behind a retrieve. Every normal
#: completion writes K — leg 1 always runs — so the guarantee holds; a rule that subtracted
#: one union over all paths from the meet over exits withheld it (round r19).
_WRITES_K_PROVED_AND_BEHIND_A_GET = _legs({"steps": [_MSG, _SET_K], "terminal": _STOP},
                                          {"steps": [_GET, _SET_K], "terminal": _STOP})


def test_a_guarantee_is_a_meet_over_paths_not_a_subtraction_across_them():
    """Round r19: the proof is carried PER PATH and met over the normal exits — never one
    union over all paths subtracted from that meet.

    Subtracting withheld a key every normal completion establishes, the moment any ONE other
    path also wrote that key behind a possibly-empty step, and refused a caller's later read
    of a property the child always sets. The meet keeps that key, because every completion
    proved a write of it, and still withholds a key no completion proves.
    """
    admitted = [("PARENT", _call_then_read(**_WAITS_AND_ABORTS)),
                ("WRITER", _WRITES_K_PROVED_AND_BEHIND_A_GET)]
    assert _row(admitted, "PARENT", "WRITER").guaranteed_state == (("dpp", "K"),)
    assert _errors(admitted, "PARENT") == []
    assert _compile_errors(admitted, "PARENT") == []
    # A meet does not depend on which leg is written first; the subtraction did not either,
    # and withheld both orders.
    reversed_legs = [("PARENT", _call_then_read(**_WAITS_AND_ABORTS)),
                     ("WRITER", _legs({"steps": [_GET, _SET_K], "terminal": _STOP},
                                      {"steps": [_MSG, _SET_K], "terminal": _STOP}))]
    assert _row(reversed_legs, "PARENT", "WRITER").guaranteed_state == (("dpp", "K"),)
    assert _errors(reversed_legs, "PARENT") == []
    # CDX-184-r18-02 STAYS FIXED: where the ONLY path writes behind a possibly-empty step,
    # no completion proves the write and nothing crosses.
    only = [("PARENT", _call_then_read(**_WAITS_AND_ABORTS)), ("WRITER", _BEHIND_A_RETRIEVE)]
    assert _row(only, "PARENT", "WRITER").guaranteed_state == ()
    assert (_READ_BEFORE_WRITE, _LATER_READ) in _errors(only, "PARENT")
    # ... and so does the Decision whose arms disagree. Arms are EXCLUSIVE, so a completion
    # that took the unproved arm never made the write: crediting a key because SOME path
    # proved it — the other way to stop subtracting — would be unsound here, and the meet
    # is what refuses it.
    arms = [("PARENT", _call_then_read(**_WAITS_AND_ABORTS)),
            ("WRITER", _doc(_arms([_SET_K], [_GET, _SET_K])))]
    assert _row(arms, "PARENT", "WRITER").guaranteed_state == ()
    assert (_READ_BEFORE_WRITE, _LATER_READ) in _errors(arms, "PARENT")


def test_the_per_path_meet_on_a_guarantee_is_load_bearing(monkeypatch):
    """Non-vacuity: with an unproved write UN-proving the key for every path — the
    union-over-paths stance this correction replaced, expressed on the lattice — the child
    that writes K on every normal completion exports nothing again and its caller is
    refused. The lever is the rule, not an assertion: nothing below is weakened."""
    roots = [("PARENT", _call_then_read(**_WAITS_AND_ABORTS)),
             ("WRITER", _WRITES_K_PROVED_AND_BEHIND_A_GET)]
    assert _errors(roots, "PARENT") == []
    real = lineage._State.with_write

    def un_proves(self, key, proved=False):
        after = real(self, key, proved=proved)
        if proved or key[0] in lineage._DOCUMENT_LIFETIME_SCOPES:
            return after
        return lineage._State(after.document, after.execution, after.content, after.cohorts,
                              after.sealed, after.proved - {key})

    monkeypatch.setattr(lineage._State, "with_write", un_proves)
    assert _row(roots, "PARENT", "WRITER").guaranteed_state == ()
    assert (_READ_BEFORE_WRITE, _LATER_READ) in _errors(roots, "PARENT")


def test_the_proved_path_gate_on_a_guarantee_is_load_bearing(monkeypatch):
    """Non-vacuity: with every path counted as proved — the rule before this correction — the
    child exports a guarantee for a writer a zero-document retrieve skips, and the parent's
    later read of it is admitted again."""
    behind = [("PARENT", _call_then_read(**_WAITS_AND_ABORTS)), ("WRITER", _BEHIND_A_RETRIEVE)]
    assert (_READ_BEFORE_WRITE, _LATER_READ) in _errors(behind, "PARENT")
    monkeypatch.setattr(lineage, "_path_provably_runs", lambda stream: True)
    assert _row(behind, "PARENT", "WRITER").guaranteed_state == (("dpp", "K"),)
    assert _errors(behind, "PARENT") == []


# ---------------------------------------------------------------------------
# Stage-2 review round r18: a proved removal and refill ends the upward obligation
# ---------------------------------------------------------------------------

_X_LESS_INTO_THE_CACHE = {"steps": [_GET], "terminal": _PUT}
_FORWARDS_TO_THE_CACHE_CHILD = {"steps": [], "terminal": _call("CACHE_CHILD")}
_REMOVES_THEN_STAGES = _legs(_EMPTIES_IT, _STAGES_X, _FORWARDS_TO_THE_CACHE_CHILD)
_STAGES_ONLY = _legs(_STAGES_X, _FORWARDS_TO_THE_CACHE_CHILD)
_WRITES_THE_CACHE = ("MIDP", _passthrough_root(_FILLS_ON_A_PROVED_PATH,
                                               {"steps": [_MSG], "terminal": _STOP}))


def _outer_caller_of(mid, use="no_data_bound"):
    """PARENT stores documents WITHOUT X in the shared cache, then calls MID."""
    return [("PARENT", _legs(_X_LESS_INTO_THE_CACHE, {"steps": [], "terminal": _call("MID")})),
            ("MID", mid), ("CACHE_CHILD", _USES[use][0])]


def test_a_whole_cache_removal_and_refill_ends_the_upward_obligation():
    """The row a grandchild's cached property puts on the callers above it asks one question:
    can a document THIS process did not write reach that cache? Usually nothing inside a
    process can answer no. Here the walk proves it: the whole cache was emptied on this path
    and refilled only from this process's own writes, each judged at this same call.

    Without it the parent was refused at its call while the process it called, the grandchild
    and the flattened graph of the same legs all validated."""
    removes = _outer_caller_of(_REMOVES_THEN_STAGES)
    assert _row(removes, "PARENT", "MID").cache_property_requirements == ()
    for key in ("PARENT", "MID", "CACHE_CHILD"):
        assert _errors(removes, key) == [], key
        assert _compile_errors(removes, key) == [], key
    # The model's own flattening of the same legs agrees, as it did not before.
    flat = _legs(_X_LESS_INTO_THE_CACHE, _EMPTIES_IT, _STAGES_X,
                 {"steps": [_READ, _BOUND_GET], "terminal": _STOP})
    assert _errors([("PARENT", flat)], "PARENT") == []
    # The ordinary-read channel answers the same way.
    read = _outer_caller_of(_REMOVES_THEN_STAGES, "no_data_read")
    assert _row(read, "PARENT", "MID").cache_property_requirements == ()
    assert _errors(read, "PARENT") == []
    # CONTROL, the fail-open batch 18 closed, unmoved: with no removal the row still travels
    # up and the X-LESS caller is still refused AT ITS CALL, in both channels and on both
    # routes, exactly as its flattened twin is.
    stages = _outer_caller_of(_STAGES_ONLY)
    assert _row(stages, "PARENT", "MID").cache_property_requirements == (
        ("$ref:CACHE", "X", None, True),)
    assert (_NOT_ESTABLISHED, _AT_THE_CALL) in _errors(stages, "PARENT")
    assert (_NOT_ESTABLISHED, _AT_THE_CALL) in _compile_errors(stages, "PARENT")
    stages_read = _outer_caller_of(_STAGES_ONLY, "no_data_read")
    assert _row(stages_read, "PARENT", "MID").cache_property_requirements == (
        ("$ref:CACHE", "X", None, False),)
    assert (_READ_BEFORE_WRITE, "/body/steps/0/legs/1/terminal") in _errors(stages_read, "PARENT")
    twin = _legs(_X_LESS_INTO_THE_CACHE, _STAGES_X,
                 {"steps": [_READ, _BOUND_GET], "terminal": _STOP})
    assert _errors([("PARENT", twin)], "PARENT") != []


#: A foreign document can reach the cache again after the refill, so the obligation stays
#: where it was: ``(the middle, the pointer its OWN refusal carries)``.
_FOREIGN_AFTER_THE_REFILL = {
    "a_child_that_may_write_the_cache": _legs(
        _EMPTIES_IT, _STAGES_X, {"steps": [], "terminal": _call("MIDP", **_WAITS_AND_ABORTS)},
        _FORWARDS_TO_THE_CACHE_CHILD),
    "a_child_nothing_derives": _legs(
        _EMPTIES_IT, _STAGES_X, {"steps": [], "terminal": _call("EXTERNAL", **_WAITS_AND_ABORTS)},
        _FORWARDS_TO_THE_CACHE_CHILD),
}


@pytest.mark.parametrize("shape", sorted(_FOREIGN_AFTER_THE_REFILL))
def test_a_foreign_write_after_the_refill_ends_the_proof(shape):
    """Each of these lets a document nobody here wrote back into the cache between the refill
    and the call, so the middle process cannot prove the grandchild's property of what it
    retrieves and is refused at its own call — which is where an unmet obligation of its
    child belongs. Nothing ships either way."""
    roots = _outer_caller_of(_FOREIGN_AFTER_THE_REFILL[shape]) + [_WRITES_THE_CACHE]
    pointer = "/body/steps/0/legs/3/terminal/process_ref"
    assert (_NOT_ESTABLISHED, pointer) in _errors(roots, "MID"), shape
    assert (_NOT_ESTABLISHED, pointer) in _compile_errors(roots, "MID"), shape


def test_a_removal_with_nothing_refilled_leaves_the_caller_nothing_to_prove():
    """The cache is EMPTY at the call, so no document of the caller's can reach the
    grandchild and the row ends here too — while the middle is refused for its own read of a
    cache nothing filled, so again nothing ships. ``(the middle, its own pointer)``."""
    for mid, pointer in ((_legs(_STAGES_X, _EMPTIES_IT, _FORWARDS_TO_THE_CACHE_CHILD),
                          "/body/steps/0/legs/2/terminal"),
                         (_legs(_EMPTIES_IT, _FORWARDS_TO_THE_CACHE_CHILD),
                          "/body/steps/0/legs/1/terminal")):
        roots = _outer_caller_of(mid)
        assert _row(roots, "PARENT", "MID").cache_property_requirements == ()
        assert (_CACHE_WRITER_MISSING, pointer) in _errors(roots, "MID"), pointer
        assert _errors(roots, "PARENT") == [], pointer


def test_the_removed_and_refilled_proof_is_load_bearing(monkeypatch):
    """Non-vacuity: with nothing ever sealed — the rule before this correction, where a
    removal proved nothing about who filled the cache afterwards — the row travels up again
    and the parent is refused at its call for documents that cannot reach the grandchild."""
    removes = _outer_caller_of(_REMOVES_THEN_STAGES)
    assert _errors(removes, "PARENT") == []
    monkeypatch.setattr(lineage._State, "with_sealed_cache", lambda self, cache_ref: self)
    assert _row(removes, "PARENT", "MID").cache_property_requirements == (
        ("$ref:CACHE", "X", None, True),)
    assert (_NOT_ESTABLISHED, _AT_THE_CALL) in _errors(removes, "PARENT")


#: The same chain with ONE leg changed: the whole-cache removal sits behind a connector call
#: that may return no rows, so the removal may never execute while MID still completes
#: normally (round r19).
_REMOVES_BEHIND_A_RETRIEVE = _legs({"steps": [_GET], "terminal": _REMOVE},
                                   _STAGES_X, _FORWARDS_TO_THE_CACHE_CHILD)


def test_the_seal_needs_the_same_proof_the_writes_need():
    """Round r19: the seal may not rest on a removal the same walk marks as possibly skipped.

    The two halves of a whole-cache removal take OPPOSITE directions from the same doubt.
    Un-establishing is fail-closed and stays unconditional. The seal is the only PERMISSIVE
    consumer — it stops charging a caller for documents it declares gone — so it needs the
    proof the writes on this path need. Behind a connector call that may return no rows the
    terminal remove never runs on that execution, the caller's documents are all still in the
    shared cache, and the grandchild's cached-property row must still travel up.
    """
    may_skip = _outer_caller_of(_REMOVES_BEHIND_A_RETRIEVE)
    assert _row(may_skip, "PARENT", "MID").cache_property_requirements == (
        ("$ref:CACHE", "X", None, True),)
    assert (_NOT_ESTABLISHED, _AT_THE_CALL) in _errors(may_skip, "PARENT")
    assert (_NOT_ESTABLISHED, _AT_THE_CALL) in _compile_errors(may_skip, "PARENT")
    # The removal itself still crosses: a removal that MAY run is one nobody above can rule
    # out, which is the fail-closed half and is not gated.
    assert _row(may_skip, "PARENT", "MID").removed_caches == ("$ref:CACHE",)
    # CONTROL: the PROVED removal still ends the obligation, so what is refused is the
    # unproved removal and not the shape.
    proved = _outer_caller_of(_REMOVES_THEN_STAGES)
    assert _row(proved, "PARENT", "MID").cache_property_requirements == ()
    assert _errors(proved, "PARENT") == []


def test_the_proof_gate_on_the_seal_is_load_bearing(monkeypatch):
    """Non-vacuity: sealing whatever the proof says — the rule before this correction — drops
    the row and re-admits the whole chain for a removal that may never have run."""
    may_skip = _outer_caller_of(_REMOVES_BEHIND_A_RETRIEVE)
    assert (_NOT_ESTABLISHED, _AT_THE_CALL) in _errors(may_skip, "PARENT")

    def always_seals(state, cache_ref, proved_to_run):
        return lineage._without_cache_establishment(
            state.without_content(cache_ref), cache_ref).with_sealed_cache(cache_ref)

    monkeypatch.setattr(lineage, "_after_a_whole_cache_removal", always_seals)
    assert _row(may_skip, "PARENT", "MID").cache_property_requirements == ()
    assert _errors(may_skip, "PARENT") == []


def test_a_foreign_cohort_and_the_meet_each_end_the_removal_proof():
    """The two ways the proof ends, measured where they are decided.

    The meet is witnessed at the lattice rather than through a graph because no graph can
    OBSERVE it: a one-arm whole-cache removal can never be followed by a call that reads the
    seal. Two of its three spellings are refused outright — a `cache_remove` true-arm
    terminal inside a Branch leg is `…NODE_NOT_ALLOWED_IN_BODY` and the nested-branch form
    there is `…NESTING_LIMIT` — but the third, a root-sequence Decision whose true arm ends
    in a nested Branch carrying the removal, PARSES AND VALIDATES CLEAN; what stops it is
    that a Decision cannot be followed by further steps (`…CONTROL_CONTINUATION_UNSUPPORTED`),
    so the forward call never exists. The earlier docstring gave the first reason for all
    three spellings, which is false as written (round r19b). The lattice below is therefore
    the instrument, and it is the same `merged_with` every verdict above rests on."""
    empty = lineage._State()
    sealed = empty.with_sealed_cache("$ref:CACHE")
    assert sealed.sealed == frozenset({"$ref:CACHE"})
    # This process's own executing write keeps the proof; every other cohort ends it.
    ours = sealed.with_cohort("$ref:CACHE", lineage._caller_cohort(("X",)), ours=True)
    assert ours.sealed == frozenset({"$ref:CACHE"})
    foreign = sealed.with_cohort("$ref:CACHE", lineage.UNKNOWN_COHORT)
    assert foreign.sealed == frozenset()
    # The meet intersects it, so a removal on one arm of a Decision never counts.
    assert sealed.merged_with(empty).sealed == frozenset()
    assert sealed.merged_with(sealed).sealed == frozenset({"$ref:CACHE"})
    # And the gate reads exactly those three answers. The declared external writer is NOT a
    # fourth: that case never reaches this gate, and is measured where it is decided by
    # `test_a_declared_external_writer_is_refused_before_the_seal_is_consulted`.
    plain = DEFAULT_VALIDATION_CAPABILITIES
    owes = lineage._caller_owes_a_cached_property
    assert owes("$ref:CACHE", "X", plain, empty) is True
    assert owes("$ref:CACHE", "X", plain, sealed) is False
    assert owes("$ref:CACHE", "X", plain, foreign) is True


def test_a_declared_external_writer_is_refused_before_the_seal_is_consulted(monkeypatch):
    """Round r19b: what keeps a proved removal safe against an OUTSIDE writer, measured.

    It is not an escape clause in `_caller_owes_a_cached_property`. That clause — "unless a
    declared external writer writes this cache" — could never fire at the gate's only call
    site, and the batch cited it as the reason the seal was safe. A declared external writer
    makes `_call_stores_nothing_in` false, so the call takes the retrieve overlay, which
    seeds an unknown cohort; the meet there is empty, nothing at that call is proved, and the
    gate is never consulted at all. The middle process is refused at its own forward instead,
    so nothing ships. The clause is gone and this is what replaces the claim."""
    flagged = {"steps": [dict(_READ, external_writer=True), _READS_X], "terminal": _STOP}
    roots = _outer_caller_of(
        _legs(flagged, _EMPTIES_IT, _STAGES_X, _FORWARDS_TO_THE_CACHE_CHILD))
    asked = []
    real = lineage._caller_owes_a_cached_property

    def recording(cache_ref, name, capabilities, state):
        asked.append((cache_ref in state.sealed,
                      bool(capabilities.writes_cache_externally(cache_ref))))
        return real(cache_ref, name, capabilities, state)

    monkeypatch.setattr(lineage, "_caller_owes_a_cached_property", recording)
    declared = _cell(roots, _external_writer_declaration(), key="MID")
    at_the_forward = "/body/steps/0/legs/3/terminal/process_ref"
    assert (_NOT_ESTABLISHED, at_the_forward) in declared["errors"]
    assert (_NOT_ESTABLISHED, at_the_forward) in declared["compile_errors"]
    assert asked == [], asked
    # With no external writer declared the gate IS consulted — and only ever about a cache
    # no external writer touches, which is exactly why the removed clause could not fire.
    asked.clear()
    for key in ("PARENT", "MID"):
        _cell(roots, None, key=key)
    assert asked, "the gate was never consulted, so the control proves nothing"
    assert all(external is False for _sealed, external in asked), asked


# ---------------------------------------------------------------------------
# Stage-2 review round r19b: the proof is about documents, not about a marker
# ---------------------------------------------------------------------------

#: One body, authored under both entry forms: a Message — which the walk's own
#: `_COUNT_PRESERVING_KINDS` calls a step that hands on exactly the documents it received —
#: standing in front of the write whose value the caller reads in a later leg.
_A_MESSAGE_THEN_THE_WRITE = {"kind": "branch", "legs": [
    {"steps": [_MSG, _SET_K], "terminal": _STOP}, {"steps": [_MSG], "terminal": _STOP}]}
#: The same body with a step that MAY hand on no documents in front of the write.
_A_RETRIEVE_THEN_THE_WRITE = {"kind": "branch", "legs": [
    {"steps": [_GET, _SET_K], "terminal": _STOP}, {"steps": [_MSG], "terminal": _STOP}]}


def test_a_count_preserving_step_keeps_the_proof_its_entry_gave_it():
    """Round r19b: the proof a guarantee rests on is about the DOCUMENTS in front of the
    write — may they be zero — and not about the stream's state token.

    Read off the token, the proof named the Data Passthrough entry itself, so any
    stream-replacing step destroyed it: a Message withdrew the guarantee for a passthrough
    child and kept it for the No Data twin, whose proof rides the count. Same steps, same
    write, opposite answers — and the caller of the passthrough form was refused a
    composition that always sets the key at runtime, on validate, on compile and on the
    public typed-plan route, in both blocking scopes."""
    passthrough = [("PARENT", _call_then_read(**_WAITS_AND_ABORTS)),
                   ("WRITER", _doc(_ENTRY, _A_MESSAGE_THEN_THE_WRITE))]
    assert _row(passthrough, "PARENT", "WRITER").guaranteed_state == (("dpp", "K"),)
    for key in ("PARENT", "WRITER"):
        assert _errors(passthrough, key) == [], key
        assert _compile_errors(passthrough, key) == [], key
    # The No Data twin answers the same, as it did before: one rule, both entry forms
    # (capture `cap184-dpp-both-ways`).
    scheduled = [("PARENT", _call_then_read(**_WAITS_AND_ABORTS)),
                 ("WRITER", _doc(_A_MESSAGE_THEN_THE_WRITE))]
    assert _row(scheduled, "PARENT", "WRITER").guaranteed_state == (("dpp", "K"),)
    assert _errors(scheduled, "PARENT") == []
    # The CACHE scope of the same shape, the other blocking scope the withholding reached.
    cached = [("PARENT", _calls_then_reads_the_cache("WRITER")),
              ("WRITER", _passthrough_root(_FILLS_ON_A_PROVED_PATH,
                                           {"steps": [_MSG], "terminal": _STOP}))]
    assert _row(cached, "PARENT", "WRITER").guaranteed_state == (("cache", "$ref:CACHE"),)
    for key in ("PARENT", "WRITER"):
        assert _errors(cached, key) == [], key
        assert _compile_errors(cached, key) == [], key
    # THE OTHER DIRECTION, unmoved: a step that may hand on NO documents still ends the
    # proof — in the passthrough form too, where the entry no longer carries it past one.
    behind = [("PARENT", _call_then_read(**_WAITS_AND_ABORTS)),
              ("WRITER", _doc(_ENTRY, _A_RETRIEVE_THEN_THE_WRITE))]
    assert _row(behind, "PARENT", "WRITER").guaranteed_state == ()
    assert (_READ_BEFORE_WRITE, _LATER_READ) in _errors(behind, "PARENT")
    assert (_READ_BEFORE_WRITE, _LATER_READ) in _compile_errors(behind, "PARENT")


def test_the_carried_non_emptiness_proof_is_load_bearing(monkeypatch):
    """Non-vacuity: with the proof read off the stream's STATE again — the rule this
    correction replaced, restored verbatim — the passthrough child's guarantee disappears
    the moment a Message stands in front of the write, and its caller is refused."""
    roots = [("PARENT", _call_then_read(**_WAITS_AND_ABORTS)),
             ("WRITER", _doc(_ENTRY, _A_MESSAGE_THEN_THE_WRITE))]
    assert _errors(roots, "PARENT") == []

    def from_the_stream_state(stream):
        if stream.state == lineage.STREAM_ABSENT:
            return False
        return stream.count == lineage.COUNT_ONE or stream.state == lineage.STREAM_CALLER_ENTRY

    monkeypatch.setattr(lineage, "_path_provably_runs", from_the_stream_state)
    assert _row(roots, "PARENT", "WRITER").guaranteed_state == ()
    assert (_READ_BEFORE_WRITE, _LATER_READ) in _errors(roots, "PARENT")


# ---------------------------------------------------------------------------
# Stage-2 review round r19b: what a call contributes to the leg-order diagnostic
# ---------------------------------------------------------------------------

_BRANCH_ORDER_INVALID = PROCESS_IR_SEMANTIC_LINEAGE_BRANCH_ORDER_INVALID
_SCRIPT = {"kind": "data_process", "steps": [
    {"operation": "custom_scripting", "script": "// nothing states what this does"}]}
#: The served oracle's `writes_a_property_after_an_uninspectable_step`.
_WRITES_K_AFTER_AN_UNINSPECTABLE_STEP = _doc(_GET, _SCRIPT, _SET_K, _STOP)
#: A NON-STRICT read: a Decision operand tolerates a property nobody wrote (it is a defined
#: empty string on the wire), but not a writer it can never see.
_TRACKS_K = {"steps": [], "terminal": {
    "kind": "decision", "comparison": "equals",
    "left": {"value_type": "track", "property_id": "process.K"},
    "right": {"value_type": "static", "static_value": "x"},
    "true_arm": {"steps": [_MSG], "terminal": _STOP},
    "false_arm": {"steps": [], "terminal": _STOP}}}


def _reads_then_calls(read, made):
    """A caller that reads in its FIRST leg and calls in its second — the leg-order case."""
    return _legs(read, {"steps": [], "terminal": made})


def test_a_calls_later_leg_write_is_what_it_guarantees_not_what_it_may_write():
    """The one row of the served child-call oracle that moved from a REFUSAL to an
    ADMISSION, pinned with the reason that is true of it.

    `_leg_write_index` asks `_awaited_guarantee` for a call's contribution, so what a later
    leg "writes" is what the call ESTABLISHES for later paths — never the child's
    `mutated_state`. When the guarantee is correctly withheld there is no later-leg write for
    the earlier leg to be ordered against, and a non-strict read is admitted. That is the
    model's standing stance and not a hole this batch opened: the unwaited control below,
    whose child's `mutated_state` carries K and whose guarantee does not cross, was admitted
    at 6ac8031 too. The recorded justification it replaces — "there is no later-leg write" —
    was false as stated, because the child's write of K is still in the contract."""
    waited = _call("WRITER", **_WAITS_AND_ABORTS)
    uninspectable = [("PARENT", _reads_then_calls(_TRACKS_K, waited)),
                     ("WRITER", _WRITES_K_AFTER_AN_UNINSPECTABLE_STEP)]
    row = _row(uninspectable, "PARENT", "WRITER")
    assert row.guaranteed_state == ()
    assert ("dpp", "K") in row.mutated_state
    assert _errors(uninspectable, "PARENT") == []
    assert _compile_errors(uninspectable, "PARENT") == []
    # The hazard is NOT undiagnosed: the strict twin of the same graph is still refused.
    strict = [("PARENT", _reads_then_calls(_READS_K_LATER, waited)),
              ("WRITER", _WRITES_K_AFTER_AN_UNINSPECTABLE_STEP)]
    assert (_READ_BEFORE_WRITE, "/body/steps/0/legs/0/steps/0") in _errors(strict, "PARENT")
    assert (_READ_BEFORE_WRITE, "/body/steps/0/legs/0/steps/0") in _compile_errors(strict, "PARENT")
    # A call whose guarantee DOES cross is still ordered ...
    guaranteed = [("PARENT", _reads_then_calls(_TRACKS_K, waited)), ("WRITER", _SETS_K)]
    assert (_BRANCH_ORDER_INVALID, "/body/steps/0/legs/0/terminal") in _errors(guaranteed, "PARENT")
    # ... and one whose guarantee does not was never ordered, whatever it may write.
    unwaited = [("PARENT", _reads_then_calls(_TRACKS_K, _call("WRITER", wait=False,
                                                              abort_on_error=False))),
                ("WRITER", _SETS_K)]
    assert ("dpp", "K") in _row(unwaited, "PARENT", "WRITER").mutated_state
    assert _errors(unwaited, "PARENT") == []
    # A DIRECT later-leg write is still ordered, so the rule itself is alive.
    direct = [("PARENT", _legs(_TRACKS_K, {"steps": [_SET_K], "terminal": _STOP}))]
    assert (_BRANCH_ORDER_INVALID, "/body/steps/0/legs/0/terminal") in _errors(direct, "PARENT")


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
    # Round r19: the sentence's own narrowing, at the gated shape it narrows — the same
    # caller as ("dpp", "gated", "walk") with the write moved behind a possibly-empty step.
    measured[("dpp_behind_a_possibly_empty_step", "gated", "walk")] = _cell(
        [("PARENT", _call_then_read(**_BOUNDARY_SHAPES["gated"])),
         ("WRITER", _BEHIND_A_RETRIEVE)])
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


def _a_write_behind_a_possibly_empty_step_establishes_nothing(m):
    """Round r19's narrowing of the served sentence, measured at the shape it narrows: at the
    SAME gated call, a child whose write stands behind a step that may hand on no documents
    establishes nothing, while the child that proves its write on every completion does."""
    return (_refused(m[("dpp_behind_a_possibly_empty_step", "gated", "walk")])
            and not _refused(m[("dpp", "gated", "walk")]))


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
     "caches the child writes on every normal completion at a step its own documents "
     "provably reach.",
     _the_gated_call_establishes),
    ("a-write-behind-a-possibly-empty-step-establishes-nothing",
     "A write the child makes behind a step that may hand on no documents — a connector "
     "call that may return no rows, a retrieve of a cache that may be empty — establishes "
     "nothing for the caller, because a completion that skipped it is still a normal "
     "completion.",
     _a_write_behind_a_possibly_empty_step_establishes_nothing),
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


# ---------------------------------------------------------------------------
# Stage-2 review round r19c: a retrieve carries the fill proof the walk already has
# ---------------------------------------------------------------------------

#: Leg 0 fills the cache on a path the passthrough entry proves; leg 1 retrieves THAT cache
#: and writes K behind it. Legs run in order, so the retrieve provably hands on a document
#: and K is written on every normal completion.
_PROVED_FILL_THEN_RETRIEVE_THEN_WRITE = _passthrough_root(
    _FILLS_ON_A_PROVED_PATH,
    {"steps": [_READ, _SET_K], "terminal": _STOP},
    {"steps": [_MSG], "terminal": _STOP})
#: The same shape whose fill stands behind a producer that may return no rows, so the cache
#: itself may be empty at the retrieve ...
_UNPROVED_FILL_THEN_RETRIEVE_THEN_WRITE = _passthrough_root(
    {"steps": [_GET], "terminal": _PUT},
    {"steps": [_READ, _SET_K], "terminal": _STOP},
    {"steps": [_MSG], "terminal": _STOP})
#: ... and the same shape with no fill at all, where only a caller can have stored anything.
_NO_FILL_THEN_RETRIEVE_THEN_WRITE = _passthrough_root(
    {"steps": [_MSG], "terminal": _STOP},
    {"steps": [_READ, _SET_K], "terminal": _STOP},
    {"steps": [_MSG], "terminal": _STOP})


def test_a_retrieve_of_a_cache_this_path_proved_it_filled_keeps_the_proof():
    """Round r19c: the non-emptiness proof is carried across the one other step that can be
    PROVED to hand on a document — a retrieve of a cache this path's own proved write filled.

    Dropping it at every step that is not count-preserving is right for a producer, whose
    rows may be zero, and wrong here: the walk already holds the fill proof in
    `_State.proved` and the retrieve did not consult it, so a caller composition the server
    admitted before the carry existed was refused on validate AND compile — and only in the
    Data Passthrough form, because the No Data twin's proof rides `count`. That is the same
    one-rule-both-entry-forms asymmetry the carry exists to remove (capture
    `cap184-dpp-both-ways`), reappearing on the cache channel."""
    proved = [("PARENT", _call_then_read(**_WAITS_AND_ABORTS)),
              ("WRITER", _PROVED_FILL_THEN_RETRIEVE_THEN_WRITE)]
    row = _row(proved, "PARENT", "WRITER")
    assert ("dpp", "K") in row.guaranteed_state, row.guaranteed_state
    assert ("cache", "$ref:CACHE") in row.guaranteed_state, row.guaranteed_state
    for key in ("PARENT", "WRITER"):
        assert _errors(proved, key) == [], key
        assert _compile_errors(proved, key) == [], key
    # WITHHELD, and the discriminator is the FILL's own proof: behind a producer that may
    # return no rows the cache may be empty at the retrieve, so nothing behind it is proved.
    unproved = [("PARENT", _call_then_read(**_WAITS_AND_ABORTS)),
                ("WRITER", _UNPROVED_FILL_THEN_RETRIEVE_THEN_WRITE)]
    assert _row(unproved, "PARENT", "WRITER").guaranteed_state == ()
    assert (_READ_BEFORE_WRITE, _LATER_READ) in _errors(unproved, "PARENT")
    assert (_READ_BEFORE_WRITE, _LATER_READ) in _compile_errors(unproved, "PARENT")
    # WITHHELD for the same reason where nothing filled the cache at all: a retrieve of a
    # cache nobody here proved filled proves nothing, which is the served sentence's own
    # second exclusion.
    unfilled = [("PARENT", _call_then_read(**_WAITS_AND_ABORTS)),
                ("WRITER", _NO_FILL_THEN_RETRIEVE_THEN_WRITE)]
    assert _row(unfilled, "PARENT", "WRITER").guaranteed_state == ()
    assert (_READ_BEFORE_WRITE, _LATER_READ) in _errors(unfilled, "PARENT")
    assert (_READ_BEFORE_WRITE, _LATER_READ) in _compile_errors(unfilled, "PARENT")


def test_the_retrieve_fill_proof_is_load_bearing(monkeypatch):
    """Non-vacuity, bracketed in both directions, with the rule as the lever and no assertion
    weakened. Consulting nothing at a retrieve — the rule this correction replaced — withdraws
    the guarantee from a write every normal completion makes; treating EVERY retrieve as
    proved restores it for the cache nothing filled, which must stay withheld. So the recorded
    answers rest on the fill proof itself and not on the shape of the graph."""
    proved = [("PARENT", _call_then_read(**_WAITS_AND_ABORTS)),
              ("WRITER", _PROVED_FILL_THEN_RETRIEVE_THEN_WRITE)]
    unfilled = [("PARENT", _call_then_read(**_WAITS_AND_ABORTS)),
                ("WRITER", _NO_FILL_THEN_RETRIEVE_THEN_WRITE)]
    assert _errors(proved, "PARENT") == []
    with monkeypatch.context() as patched:
        patched.setattr(lineage, "_retrieve_of_a_proved_cache",
                        lambda semantic, state: False)
        assert ("dpp", "K") not in _row(proved, "PARENT", "WRITER").guaranteed_state
        assert (_READ_BEFORE_WRITE, _LATER_READ) in _errors(proved, "PARENT")
    with monkeypatch.context() as patched:
        patched.setattr(lineage, "_retrieve_of_a_proved_cache",
                        lambda semantic, state: True)
        assert ("dpp", "K") in _row(unfilled, "PARENT", "WRITER").guaranteed_state
    assert _errors(proved, "PARENT") == []
    assert _row(unfilled, "PARENT", "WRITER").guaranteed_state == ()
