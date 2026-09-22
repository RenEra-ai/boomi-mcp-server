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
    PROCESS_IR_CAPABILITY_PROCESS_CALL_REPEATED_RUN_UNSTABLE,
    PROCESS_IR_SEMANTIC_DYNAMIC_PATH_DDP_NOT_ESTABLISHED,
    PROCESS_IR_SEMANTIC_DYNAMIC_PATH_NO_DYNAMIC_SEGMENT,
    PROCESS_IR_SEMANTIC_LINEAGE_BRANCH_ORDER_INVALID,
    PROCESS_IR_SEMANTIC_LINEAGE_CACHE_WRITER_MISSING,
    PROCESS_IR_SEMANTIC_LINEAGE_DDP_SCOPE_INVALID,
    PROCESS_IR_SEMANTIC_LINEAGE_EXTERNAL_WRITER_ASSUMED,
    PROCESS_IR_SEMANTIC_LINEAGE_PROPERTY_READ_BEFORE_WRITE,
    PROCESS_IR_SEMANTIC_PROFILE_MISMATCH,
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
    scheduled parent it runs once, and the same child is admitted. Refused under its own code
    since QA-184-s1-r21-01; the verdict and the pointer are unchanged."""
    appends = _legs({"steps": [_READ, _BOUND_GET], "terminal": _STOP},
                    {"steps": [_GET, _DYNAMIC_X], "terminal": _PUT})
    per_document = _doc(_ENTRY, {"kind": "branch", "legs": [
        {"steps": [_DYNAMIC_X], "terminal": _PUT}, {"steps": [], "terminal": _call("CACHE_CHILD")}]})
    placement = (PROCESS_IR_CAPABILITY_PROCESS_CALL_REPEATED_RUN_UNSTABLE, "/body/steps/1/legs/1/terminal")
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
    # What a completion of the child may write, and — because a write it did not wait for is
    # still in flight when it returns — which of those writes its caller must go on treating
    # as possible after the call, whether or not the caller waits (amendment 1 rule 7). The
    # second of the two says the child cannot NAME them, so the caller reads it against its
    # own observable caches; deciding this per process made the identical composition depend
    # on how deeply it was nested (B21A-R4-SOUND-01).
    ("content", "possible_effect"): (
        "cache_writes_known", "unwaited_cache_writes", "unwaited_writes_of_an_unknown_cache"),
    # What no completion leaves in the caches the child requires of its callers — read only by
    # the repetition check for a LATER RUN of the same child (amendment 1 rule 8). A caller's
    # own later reads still meet what a child may store as an unknown possibility.
    ("content", "guaranteed_effect"): ("required_caches_retain_nothing_it_stored",),
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
    # Fills CACHE2, binds on what it retrieves, and empties CACHE2 on a later leg the walk proves.
    "required_caches_retain_nothing_it_stored": ([
        ("PARENT", _legs({"steps": [], "terminal": _call("CACHE_CHILD", wait=True, abort_on_error=True)},
                         {"steps": [_MSG], "terminal": _STOP})),
        ("CACHE_CHILD", _legs(
            {"steps": [_GET, _DYNAMIC_X], "terminal": {"kind": "cache_put", "cache_ref": "$ref:CACHE2"}},
            {"steps": [{"kind": "cache_get", "cache_ref": "$ref:CACHE2"}, _BOUND_GET], "terminal": _STOP},
            {"steps": [], "terminal": {"kind": "cache_remove", "cache_ref": "$ref:CACHE2"}}))], "CACHE_CHILD"),
    # Hands its documents to a call it does not WAIT for: that call's write into CACHE2 is
    # still in flight when this child returns, so its callers carry it too.
    "unwaited_cache_writes": ([
        ("PARENT", _legs({"steps": [], "terminal": _call("CACHE_CHILD", wait=True, abort_on_error=True)},
                         {"steps": [_MSG], "terminal": _STOP})),
        ("CACHE_CHILD", _legs({"steps": [], "terminal": _call("WRITER2", wait=False, abort_on_error=False)},
                              {"steps": [_MSG], "terminal": _STOP})),
        ("WRITER2", _legs({"steps": [_GET], "terminal": {"kind": "cache_put", "cache_ref": "$ref:CACHE2"}},
                          {"steps": [_MSG], "terminal": _STOP}))], "CACHE_CHILD"),
    # ... and the same call to a process nothing derives: this child cannot name what that
    # call may still be writing, so it says so and its callers use their own vocabulary.
    "unwaited_writes_of_an_unknown_cache": ([
        ("PARENT", _legs({"steps": [], "terminal": _call("CACHE_CHILD", wait=True, abort_on_error=True)},
                         {"steps": [_MSG], "terminal": _STOP})),
        ("CACHE_CHILD", _legs({"steps": [], "terminal": _call("EXTERNAL", wait=False, abort_on_error=False)},
                              {"steps": [_MSG], "terminal": _STOP}))], "CACHE_CHILD"),
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
    # THE CONTENT CHANNEL beside it now answers the same way at the same shape. Until
    # correction batch 21a `cache_requirements` inherited only what this process's own writes
    # did not reach, so MID's staging ended the obligation and PARENT's profile-less documents
    # in the same cache were never checked (the limit SELF-184-43 recorded), while the
    # flattened graph refused the map. Amendment 3 §7: "Append possible cohorts; do not treat
    # the last cache write as replacing earlier contents" — MID's P2 documents sit BESIDE
    # PARENT's, so the row travels up and PARENT is refused at its call, as its twin is.
    # Measured before the correction: row `()`, PARENT clean on validate and compile.
    typed = [("PARENT", _legs({"steps": [_GET], "terminal": _PUT},
                              {"steps": [], "terminal": _call("MID")})),
             ("MID", _stage_and_call(_STAGES_P2)), ("CACHE_CHILD", _TYPED_CACHE_CHILD)]
    assert _row(typed, "PARENT", "MID").cache_requirements == (("$ref:CACHE", "$ref:P2"),)
    content_refusal = (PROCESS_IR_SEMANTIC_PROFILE_MISMATCH, "/body/steps/0/legs/1/terminal/process_ref")
    assert _errors(typed, "PARENT") == [content_refusal]
    assert _compile_errors(typed, "PARENT") == [content_refusal]
    typed_twin = _legs({"steps": [_GET], "terminal": _PUT}, _STAGES_P2,
                       {"steps": [_READ, {"kind": "map_ref", "map_ref": "$ref:M22"}], "terminal": _STOP})
    assert (PROCESS_IR_SEMANTIC_PROFILE_MISMATCH, "/body/steps/0/legs/2/steps/1/map_ref") in _errors(
        [("PARENT", typed_twin)], "PARENT")
    # CONTROL, over-correction: a caller that stores NOTHING in that cache is still admitted —
    # nothing it stored is of the wrong profile — and so is its twin.
    stores_nothing = [("PARENT", _legs({"steps": [_MSG], "terminal": _STOP},
                                       {"steps": [], "terminal": _call("MID")}))] + typed[1:]
    assert _errors(stores_nothing, "PARENT") == []
    assert _compile_errors(stores_nothing, "PARENT") == []


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
    segment in the cache the binding rides on is admitted again.

    Measured where the ride-on credit is the ONLY carrier (correction batch 21a). With the
    binding right behind the retrieve, the retrieve's own marker now names the second cache
    too — a caller's documents may share it beside the child's re-cached ones (amendment 3
    §7: "Append possible cohorts; do not treat the last cache write as replacing earlier
    contents") — so that refusal records the second cache's row directly and the mutant
    leaves it refused; that second carrier is asserted below rather than assumed. Behind a
    Message the marker is withheld, as it always was, and the credit alone carries the row:
    there the mutant re-admits the literal caller, which is the claim."""
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

    parent = _legs(_STAGES_X, _LITERAL_INTO_CACHE2, {"steps": [], "terminal": _call("CACHE_CHILD")})
    roots = [("PARENT", parent), ("CACHE_CHILD", _RECACHES_THEN_BINDS_PAST_A_MESSAGE)]
    beside = [("PARENT", parent), ("CACHE_CHILD", _RECACHES_THEN_BINDS)]
    assert (_NO_DYNAMIC_SEGMENT, _AT_THE_THIRD_LEGS_CALL) in _errors(roots, "PARENT")
    assert (_NO_DYNAMIC_SEGMENT, _AT_THE_THIRD_LEGS_CALL) in _errors(beside, "PARENT")
    monkeypatch.setattr(process_ir_effects, "_caller_cached_properties", the_seeded_cache_only)
    assert _errors(roots, "PARENT") == []
    assert _compile_errors(roots, "PARENT") == []
    assert ("$ref:CACHE2", "X", None, True) not in _row(
        roots, "PARENT", "CACHE_CHILD").cache_property_requirements
    # The second carrier, with no Message: the binding's own refusal names the second cache,
    # so the row survives the mutant and the literal caller stays refused.
    assert ("$ref:CACHE2", "X", None, True) in _row(
        beside, "PARENT", "CACHE_CHILD").cache_property_requirements
    assert (_NO_DYNAMIC_SEGMENT, _AT_THE_THIRD_LEGS_CALL) in _errors(beside, "PARENT")


#: The same re-cache child with a Message between the retrieve and the binding. A Message
#: hands on exactly the documents it received, so the binding still rides on the documents
#: the child retrieved from the second cache — and every verdict below is the one the shape
#: without it already earns.
_RECACHES_THEN_BINDS_PAST_A_MESSAGE = _legs(
    {"steps": [_READ, _READS_X], "terminal": _PUT2},
    {"steps": [_READ2, _MSG, _BOUND_GET], "terminal": _STOP})


@pytest.mark.parametrize("caller", sorted(_RECACHE_CALLERS))
def test_a_bound_use_past_a_recache_rides_on_that_cache_past_a_message_too(caller):
    """Amendment 3 §7: "A bound path must pass for every possible selected writer. Never
    discard an inconvenient writer alternative."

    A step that hands on exactly the documents it received cannot change WHICH cache those
    documents were retrieved from, so it cannot change which caller writes reach the binding
    riding on them. The stream-replacement branch rebuilt the stream as opaque and dropped
    `retrieved_from` with it, so the ride-on attribution — and only the attribution, never
    the seeded cohort's clearing of the binding — was lost one Message wide: the caller's
    LITERAL X in the cache the binding rides on escaped validation and both roots compiled,
    while the flattened twin of the same legs was refused NO_DYNAMIC_SEGMENT (ARCH-184-r2-01,
    the limit recorded at SELF-184-42 whose stated blocker — a marker that survives a stream
    replacement — round r19's count-preserving carrier removed)."""
    into_cache, into_cache2, expected = _RECACHE_CALLERS[caller]
    parent = _legs(into_cache, into_cache2, {"steps": [], "terminal": _call("CACHE_CHILD")})
    roots = [("PARENT", parent), ("CACHE_CHILD", _RECACHES_THEN_BINDS_PAST_A_MESSAGE)]
    assert _errors(roots, "PARENT") == list(expected)
    assert _compile_errors(roots, "PARENT") == list(expected)
    assert set(_row(roots, "PARENT", "CACHE_CHILD").cache_property_requirements) == {
        ("$ref:CACHE", "X", None, False),
        ("$ref:CACHE", "X", None, True),
        ("$ref:CACHE2", "X", None, True),
    }
    # The Message moves nothing: the same legs without it earn the same verdict, which is
    # the whole claim — including for the caller that composes X dynamically into both
    # caches, which stays ADMITTED either way rather than being refused by a fix that
    # over-corrects.
    without = [("PARENT", parent), ("CACHE_CHILD", _RECACHES_THEN_BINDS)]
    assert _errors(without, "PARENT") == list(expected)
    assert _compile_errors(without, "PARENT") == list(expected)
    # The model's own flattening of the same legs agrees, as it does without the Message.
    flat = list(parent["body"]["steps"][0]["legs"])[:-1] + list(
        _RECACHES_THEN_BINDS_PAST_A_MESSAGE["body"]["steps"][0]["legs"])
    twin = _errors([("PARENT", _legs(*flat))], "PARENT")
    assert bool(twin) == bool(expected), (twin, expected)
    # The refusal belongs to the CALL, not to the child: the child alone is still clean.
    assert _errors(roots, "CACHE_CHILD") == []


def test_the_ride_on_cache_surviving_a_stream_replacement_is_load_bearing(monkeypatch):
    """Non-vacuity: with the carry neutralised — an opaque stream rebuilt WITHOUT the cache
    its documents were retrieved from, exactly what the branch returned before this
    correction — the caller that stored a literal path segment in the cache the binding
    rides on is admitted again and the row it was refused for disappears.

    The mutant reaches only the stream a replacement rebuilds, so the same shape without the
    Message keeps its refusal: what it neutralises is the carry, not the ride-on channel."""
    into_cache, into_cache2, expected = _RECACHE_CALLERS[
        "a_literal_in_the_cache_the_binding_rides_on"]
    parent = _legs(into_cache, into_cache2, {"steps": [], "terminal": _call("CACHE_CHILD")})
    roots = [("PARENT", parent), ("CACHE_CHILD", _RECACHES_THEN_BINDS_PAST_A_MESSAGE)]
    assert _errors(roots, "PARENT") == list(expected)

    real_stream = lineage._Stream

    def without_the_ride_on_marker(*args, **kwargs):
        if kwargs.get("origin") == "opaque":
            kwargs.pop("retrieved_from", None)
        return real_stream(*args, **kwargs)

    monkeypatch.setattr(lineage, "_Stream", without_the_ride_on_marker)
    assert _errors(roots, "PARENT") == []
    assert _compile_errors(roots, "PARENT") == []
    assert ("$ref:CACHE2", "X", None, True) not in _row(
        roots, "PARENT", "CACHE_CHILD").cache_property_requirements
    assert _errors([("PARENT", parent), ("CACHE_CHILD", _RECACHES_THEN_BINDS)],
                   "PARENT") == list(expected)


def test_a_per_document_caller_of_the_recache_child_answers_the_same_past_a_message():
    """The restored attribution's OTHER consequence, pinned where it is newly reachable.

    A Data Passthrough caller hands the call a group of a size no child can know, so this No
    Data child runs once per document — and it both requires CACHE2 and appends to it, which
    amendment 1 rule 8 (`_repetition_unstable_caches`) refuses at the call. That verdict is
    not new: the Message-free twin has earned it since correction batch 18, because the
    ride-on row is what makes the child require the cache it also writes. What IS new is that
    the shape WITH the Message now answers the same instead of compiling clean, which is the
    claim itself — a step that hands on exactly the documents it received moves nothing.

    Pinned on the caller that composes X dynamically into both caches, the cell that moved:
    it is refused here for the repetition, not for its writers, so a later change that made
    this admit again while the twin stayed refused would be the same hole reopened. Refused
    under its own code since QA-184-s1-r21-01; the verdict is unchanged."""
    call_leg = {"steps": [], "terminal": _call("CACHE_CHILD")}
    parent = _passthrough_root(_STAGES_X, _DYNAMIC_INTO_CACHE2, call_leg)
    past_a_message = _errors(
        [("PARENT", parent), ("CACHE_CHILD", _RECACHES_THEN_BINDS_PAST_A_MESSAGE)], "PARENT")
    assert [code for code, _pointer in past_a_message] == [
        PROCESS_IR_CAPABILITY_PROCESS_CALL_REPEATED_RUN_UNSTABLE]
    assert past_a_message == _errors(
        [("PARENT", parent), ("CACHE_CHILD", _RECACHES_THEN_BINDS)], "PARENT")


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
    so nothing ships. The clause is gone and this is what replaces the claim.

    Measured AT THE CALL, which is the site the claim is about. Since correction batch 21a a
    retrieve asks the same gate too, per property it hands on, whether a caller of the
    retrieving process is one more writer alternative (amendment 3 §7: "possible presence
    retains both alternatives, including unknown provenance"); those asks are recorded
    separately, and none of them pairs a sealed cache with a declared external writer
    either."""
    import sys as _sys

    flagged = {"steps": [dict(_READ, external_writer=True), _READS_X], "terminal": _STOP}
    roots = _outer_caller_of(
        _legs(flagged, _EMPTIES_IT, _STAGES_X, _FORWARDS_TO_THE_CACHE_CHILD))
    asked = []
    real = lineage._caller_owes_a_cached_property

    def _asking_site():
        """The function that asked, not the comprehension it asked from.

        One of the gate's call sites sits inside a set comprehension, which is its own
        frame before 3.12 and is inlined from 3.12 on (PEP 709), so the raw caller name is
        `<setcomp>` on 3.11 and the enclosing function on 3.12. Walking out of the
        synthetic `<...>` frames records the same site on both interpreters; reading frame 1
        directly failed only under the 3.11 CI gate.
        """
        frame = _sys._getframe(2)
        while frame is not None and frame.f_code.co_name.startswith("<"):
            frame = frame.f_back
        return frame.f_code.co_name if frame is not None else "<unknown>"

    def recording(cache_ref, name, capabilities, state):
        asked.append((_asking_site(), cache_ref in state.sealed,
                      bool(capabilities.writes_cache_externally(cache_ref))))
        return real(cache_ref, name, capabilities, state)

    def at_the_call():
        return [(sealed, external) for site, sealed, external in asked
                if site == "_discharge_child_contract"]

    monkeypatch.setattr(lineage, "_caller_owes_a_cached_property", recording)
    declared = _cell(roots, _external_writer_declaration(), key="MID")
    at_the_forward = "/body/steps/0/legs/3/terminal/process_ref"
    assert (_NOT_ESTABLISHED, at_the_forward) in declared["errors"]
    assert (_NOT_ESTABLISHED, at_the_forward) in declared["compile_errors"]
    assert at_the_call() == [], asked
    assert {site for site, _sealed, _external in asked} <= {"_discharge_child_contract", "_transfer"}, asked
    assert not any(sealed and external for _site, sealed, external in asked), asked
    # With no external writer declared the gate IS consulted — and only ever about a cache
    # no external writer touches, which is exactly why the removed clause could not fire.
    asked.clear()
    for key in ("PARENT", "MID"):
        _cell(roots, None, key=key)
    assert at_the_call(), "the gate was never consulted, so the control proves nothing"
    assert all(external is False for _sealed, external in at_the_call()), asked
    # ... and at every site that asks, the retrieve's per-property ask included: no ask anywhere
    # is about a cache a declared external writer touches (the pre-21a strength, TI-184-21a-02).
    assert all(external is False for _site, _sealed, external in asked), asked


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
    """REVISED in correction batch 21a; the name is the retired claim, kept because the node
    id is pinned by the wave gate's frozen node list.

    The retired claim: "when this process's own write reached the cache, the documents are
    its own, so the declared read inherits nothing". A document cache is shared with every
    caller and Add to Cache APPENDS, so the middle's own write puts its documents BESIDE any
    its caller stored there; the retrieve hands on both, and the declared read needs X on
    every one. Amendment 3 §7: "Append possible cohorts; do not treat the last cache write as
    replacing earlier contents." The row therefore travels up exactly as it does for a middle
    that wrote nothing, and each caller proves it against what IT stored.

    Measured before the correction: row `()`, so a caller that stored X-less documents in the
    same cache was admitted while the same legs in one process are refused
    `…PROPERTY_READ_BEFORE_WRITE`. The controls keep the callers the row must not refuse."""
    facts = _derived_facts(_FILLS_IT_THEN_CALLS, _summarised(_SCHEDULED_READS_X))
    assert facts["cache_property_requirements"] == (("$ref:CACHE", "X", None, False),)
    from boomi_mcp.compiler.process_ir.semantic_validation.contracts import (
        ProcessIRValidationCapabilitiesV1,
    )

    held = ProcessIRValidationCapabilitiesV1(child_entry_contracts=(
        ChildEntryContractV1(process_ref="$ref:MID", **facts),))
    assert _READ_BEFORE_WRITE in {
        code for code, _path in _errors_against(_STORES_NO_X_THEN_CALLS_MID, held)}
    twin = _legs({"steps": [_GET], "terminal": _PUT}, _STAGES_X,
                 {"steps": [_READ, _READS_X], "terminal": _STOP})
    assert (_READ_BEFORE_WRITE, "/body/steps/0/legs/2/steps/1") in _errors([("PARENT", twin)], "PARENT")
    # CONTROLS: a caller whose documents carry X, and one that stores nothing, are admitted.
    assert _errors_against(_STORES_X_THEN_CALLS_MID, held) == []
    stores_nothing = _legs({"steps": [_MSG], "terminal": _STOP}, {"steps": [], "terminal": _call("MID")})
    assert _errors_against(stores_nothing, held) == []


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
    cached-origin rule neutralised the obligation does not move anywhere — it disappears, for
    both entry forms. A scheduled middle's contract drops document-property reads; a passthrough
    middle's read is of documents it RETRIEVED, which the documents its caller hands it never
    reach, so it is no entry requirement either (correction batch 21a round 2 — it used to
    become a demand that the caller establish the property on its own documents, which no
    caller of a cache-filling chain can satisfy and which let none of them through). So the
    cached-origin row is the only thing that charges any caller for it."""
    capabilities = _summarised(_SCHEDULED_READS_X)
    real = (_derived_facts(_CALLS_A_SUMMARISED_CHILD, capabilities),
            _derived_facts(_SUMMARISED_CALLERS["a_passthrough_middle"], capabilities))
    assert all(("$ref:CACHE", "X", None, False) in facts["cache_property_requirements"] for facts in real)
    monkeypatch.setattr(lineage, "_caller_cached_origin",
                        lambda key, stream, invalidated: None)
    scheduled = _derived_facts(_CALLS_A_SUMMARISED_CHILD, capabilities)
    passthrough = _derived_facts(_SUMMARISED_CALLERS["a_passthrough_middle"], capabilities)
    assert scheduled["cache_property_requirements"] == ()
    assert ("ddp", "X") not in scheduled.get("required_reads", ())
    assert passthrough["cache_property_requirements"] == ()
    assert ("ddp", "X") not in passthrough["required_reads"]


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


def test_the_public_plan_admits_a_retrieve_of_a_cache_this_path_proved_it_filled(monkeypatch):
    """QA round r20 reported that the carry's ADMISSION direction has no authorable
    composition at the public tool boundary. It has one — the two rows above, carried out to
    `build_integration(action="plan"|"compile")` unchanged — and the whole of the difference
    is the Message in front of the Add to Cache: it makes the write consume the CHILD's own
    documents, so the child records no entry requirement and demands no profile of its
    caller. QA's own spelling writes the caller's ENTRY documents to the cache, which is what
    made that call a PROFILE_MISMATCH and was read as a universal rule about profiled caches.

    The cache here is genuinely profiled — a `$ref` to an in-spec profile.json, the only
    spelling that sets `cache_profile_ref` — so the admission is measured against the same
    profile fact QA's refusal rested on rather than around it. The refs are the module's own,
    bound to declared components keyed exactly as `_symbols()` names them, so the fixtures
    reach the public route as they are."""
    from unittest.mock import MagicMock

    from _m12_11_support import APPLIABLE_CONN, APPLIABLE_OP
    from test_issue_158_listener_deployment import (
        _PROFILE,
        _ApplyBoundary,
        _cause_codes,
        _request,
        _unit,
    )
    from boomi_mcp.categories.integration_builder import build_integration_action
    from boomi_mcp.models.integration_models import IntegrationComponentSpec
    from boomi_mcp.recipes.materialization import build_symbol_table

    profile = {"key": "P2", "type": "profile.json", "name": "E184 P2", "action": "create",
               "config": {"component_type": "profile.json", "profile_type": "json.generated",
                          "component_name": "E184 P2", "root": {
                              "name": "Root", "kind": "object", "children": [
                                  {"name": "id", "kind": "simple", "data_type": "character"}]}}}
    cache = {"key": "CACHE", "type": "documentcache", "name": "E184 CACHE", "action": "create",
             "depends_on": ["P2"], "config": {
                 "component_type": "documentcache", "component_name": "E184 CACHE",
                 "profile_type": "profile.json", "profile_id": "$ref:P2",
                 "indexes": [{"index_id": 1, "index_name": "by id", "keys": [
                     {"id": 1, "element_key": "3", "name": "id (Root/id)"}]}]}}
    components = [profile, cache, dict(APPLIABLE_CONN, key="RCONN"),
                  dict(APPLIABLE_OP, key="GET", depends_on=["RCONN"],
                       config=dict(APPLIABLE_OP["config"], connection_ref_key="RCONN"))]
    # The premise the row rests on, read off the DERIVED table rather than asserted of the
    # literal above: this cache really does declare a profile, so an admission here is not
    # an admission of an unprofiled cache.
    table = build_symbol_table([IntegrationComponentSpec(**spec) for spec in components])
    assert [symbol.cache_profile_ref for symbol in table.symbols
            if symbol.ref == "$ref:CACHE"] == ["$ref:P2"]

    def verdicts(child):
        """The caller and one child through the public dispatcher, on both read actions."""
        raw = _request(
            [_unit(_call_then_read(**_WAITS_AND_ABORTS), ("WRITER",), key="root",
                   name="E184 Root"),
             _unit(child, tuple(spec["key"] for spec in components), key="WRITER",
                   name="E184 Child")],
            components,
        ).model_dump(mode="json")
        results = {}
        for action in ("plan", "compile"):
            with _ApplyBoundary().installed():
                results[action] = build_integration_action(
                    MagicMock(), _PROFILE, action, config={"authoring_request": raw})
        return results

    def blamed(result, code):
        """Every pointer the route blames for ``code``, wherever the envelope carries it:
        plan REPORTS it in the validation payload, compile REFUSES with diagnostics."""
        reported = list((result.get("authoring_result") or {}).get("errors") or ())
        reported += list(result.get("authoring_diagnostics") or ())
        return sorted({item.get("path") or "" for item in reported
                       if code in ({item.get("code")} | set(item.get("cause_codes") or ()))})

    # the caller's first Branch leg, whose terminal is the call itself
    at_the_call = "/body/steps/0/legs/0/terminal"

    # ADMITTED on both actions, with neither lineage refusal anywhere in the envelope.
    admitted = verdicts(_PROVED_FILL_THEN_RETRIEVE_THEN_WRITE)
    assert admitted["plan"]["authoring_result"]["validation_report"]["is_valid"] is True, (
        _cause_codes(admitted["plan"]))
    assert admitted["compile"]["_success"] is True, _cause_codes(admitted["compile"])
    for action, result in admitted.items():
        for code in (_READ_BEFORE_WRITE, _CACHE_WRITER_MISSING):
            assert blamed(result, code) == [], (action, code, _cause_codes(result))

    # CONTROL, the fill unproved: behind a producer that may return no rows the cache may be
    # empty at the retrieve, so K is not guaranteed and the caller's later read is refused.
    unproved = verdicts(_UNPROVED_FILL_THEN_RETRIEVE_THEN_WRITE)
    assert unproved["plan"]["authoring_result"]["validation_report"]["is_valid"] is False
    assert unproved["compile"]["_success"] is False
    for action, result in unproved.items():
        assert blamed(result, _READ_BEFORE_WRITE) == [_LATER_READ], (action, _cause_codes(result))

    # CONTROL, nothing fills it at all: the same refusal, plus the cache's own missing writer
    # at the call that would have had to bring one.
    unfilled = verdicts(_NO_FILL_THEN_RETRIEVE_THEN_WRITE)
    assert unfilled["plan"]["authoring_result"]["validation_report"]["is_valid"] is False
    assert unfilled["compile"]["_success"] is False
    for action, result in unfilled.items():
        assert blamed(result, _READ_BEFORE_WRITE) == [_LATER_READ], (action, _cause_codes(result))
        assert blamed(result, _CACHE_WRITER_MISSING) == [at_the_call], (
            action, _cause_codes(result))

    # NON-VACUITY: with the carry neutralised the ADMITTED witness is refused exactly where
    # its controls are, so this row measures the CARRY and not the composition.
    with monkeypatch.context() as patched:
        patched.setattr(lineage, "_retrieve_of_a_proved_cache", lambda semantic, state: False)
        neutralised = verdicts(_PROVED_FILL_THEN_RETRIEVE_THEN_WRITE)
    assert neutralised["plan"]["authoring_result"]["validation_report"]["is_valid"] is False
    assert neutralised["compile"]["_success"] is False
    for action, result in neutralised.items():
        assert blamed(result, _READ_BEFORE_WRITE) == [_LATER_READ], (action, _cause_codes(result))


# ---------------------------------------------------------------------------
# Correction batch 21a: ONE authority on whether a caller's documents may share a cache
# ---------------------------------------------------------------------------
#
# `lineage._caller_documents_may_reach` answers one question — may documents this process did
# not write still be in that cache here? — and every site that turns a cache attribution into
# an obligation of the process's callers asks it (CDX-184-r20-01, the second instance of
# `obligation-ignores-what-the-walk-proves-about-the-cache`). Expected verdicts come from
# amendment 3 §7's cache sentences and the runtime they describe, never from this
# implementation's output: a document cache is shared with every caller and Add to Cache
# APPENDS ("Append possible cohorts; do not treat the last cache write as replacing earlier
# contents"); a whole-cache removal that runs empties it ("Whole-cache removal clears these
# summaries"); a removal that may be skipped leaves what it may have left ("Never discard an
# inconvenient writer alternative"). Each composition is compared with the flattened graph of
# the same legs, which after this correction agrees in every cell measured here.

_REMOVE2 = {"kind": "cache_remove", "cache_ref": "$ref:CACHE2"}
#: A whole-cache removal of CACHE2 the walk proves runs, and one behind a producer that may
#: return no rows, so the walk proves nothing about it.
_EMPTIES2 = {"steps": [], "terminal": _REMOVE2}
_EMPTIES2_BEHIND_A_GET = {"steps": [_GET], "terminal": _REMOVE2}
_REMOVALS_OF_CACHE2 = {"proved": [_EMPTIES2], "unproved": [_EMPTIES2_BEHIND_A_GET], "none": []}
#: Reads the caller's CACHE, uses X on those documents, and re-caches them into CACHE2.
_RECACHE_LEG = {"steps": [_READ, _READS_X], "terminal": _PUT2}
_BINDS2 = {"steps": [_READ2, _BOUND_GET], "terminal": _STOP}
_READS2 = {"steps": [_READ2, _READS_X], "terminal": _STOP}
_AT_THE_SECOND_LEGS_CALL = "/body/steps/0/legs/1/terminal"
_AT_THE_THIRD_LEGS_TERMINAL = "/body/steps/0/legs/2/terminal"


def _branch_legs(document):
    """The legs of the first Branch a root authors, whichever entry form the root has."""
    return list(next(step for step in document["body"]["steps"] if step.get("kind") == "branch")["legs"])


def _flattened(caller, *callees):
    """The same work in ONE process: the caller's legs before its call, each middle's legs
    before ITS call, then the last callee's legs. Every call is its root's last leg."""
    legs = _branch_legs(caller)[:-1]
    for position, callee in enumerate(callees):
        own = _branch_legs(callee)
        legs += own if position == len(callees) - 1 else own[:-1]
    return _legs(*legs)


def _both_routes(roots, key, declared_external_writers=()):
    """The same verdict on the validate and the compile entry point, returned once.

    ``declared_external_writers`` names the caches the REQUEST declares an outside writer
    for. Such a declaration is a fact only the caller can state, so it travels the public
    effect-declaration path — the same one `resolve_process_ir_effect_declarations` serves —
    rather than a hand-built capability, and a root whose own retrieve does not author the
    `external_writer` flag receives nothing from it (`process_ir_effects` §external writers).
    """
    if not declared_external_writers:
        validated = _errors(roots, key)
        assert sorted(_compile_errors(roots, key)) == sorted(validated), (key, validated)
        return validated
    from boomi_mcp.compiler.process_ir.diagnostics import ProcessIRCompileError
    from boomi_mcp.compiler.process_ir.pipeline import compile_process_ir_v1
    from boomi_mcp.models.authoring_workflow import (
        ProcessIREffectDeclarationsV1,
        ProcessIRExternalWriterDeclarationV1,
    )

    parsed = [(name, parse_process_ir_v1(document)) for name, document in roots]
    resolution = resolve_process_ir_effect_declarations(
        parsed,
        ProcessIREffectDeclarationsV1(external_writers=tuple(
            ProcessIRExternalWriterDeclarationV1(cache_ref=ref)
            for ref in declared_external_writers)),
        _symbols(), [], child_roots={"$ref:" + name: ir for name, ir in parsed})
    assert resolution.ok, resolution.findings
    irs = dict(parsed)
    capabilities = resolution.capabilities_by_root[key] or DEFAULT_VALIDATION_CAPABILITIES
    validated = [(item.code, item.path) for item in
                 validate_process_ir(irs[key], _symbols(), capabilities=capabilities).errors]
    try:
        compile_process_ir_v1(irs[key], _symbols(), capabilities=capabilities)
        compiled = []
    except ProcessIRCompileError as exc:
        compiled = [(item.code, item.path) for item in exc.diagnostics]
    assert sorted(compiled) == sorted(validated), (key, validated, compiled)
    return validated


def _root(form, *legs):
    return _passthrough_root(*legs) if form == "passthrough" else _legs(*legs)


@pytest.mark.parametrize("past_a_message", (False, True), ids=("no_step", "message"))
@pytest.mark.parametrize("form", ("scheduled", "passthrough"))
@pytest.mark.parametrize("removal", sorted(_REMOVALS_OF_CACHE2))
@pytest.mark.parametrize("caller", sorted(_RECACHE_CALLERS))
def test_the_ride_on_obligation_ends_exactly_where_the_removal_is_proved(caller, removal, form, past_a_message):
    """CDX-184-r20-01. The child empties CACHE2, re-caches what it read from CACHE into it, and
    binds a request path on the documents it retrieves from CACHE2. Where the walk PROVES the
    removal ran, whatever the caller stored in CACHE2 is gone before the re-cache, so the
    caller's literal segment there cannot reach the binding: it is admitted, as the flattened
    graph of the same legs is. The ride-on credit charged it anyway, on both forms, with and
    without a Message — the charge was written without asking whether a caller's document can
    still be in that cache.

    Where the removal may be skipped, or there is none, the literal still reaches the binding
    and stays refused at the call, exactly as before; a literal in the cache the property came
    FROM is refused whatever happens to CACHE2."""
    into_cache, into_cache2, refused_without_removal = _RECACHE_CALLERS[caller]
    child = _root(form, *(_REMOVALS_OF_CACHE2[removal] + [
        _RECACHE_LEG,
        {"steps": [_READ2] + ([_MSG] if past_a_message else []) + [_BOUND_GET], "terminal": _STOP}]))
    parent = _legs(into_cache, into_cache2, {"steps": [], "terminal": _call("CACHE_CHILD")})
    roots = [("PARENT", parent), ("CACHE_CHILD", child)]
    literal_in_cache2 = caller == "a_literal_in_the_cache_the_binding_rides_on"
    expected = [] if (removal == "proved" and literal_in_cache2) else list(refused_without_removal)
    assert _both_routes(roots, "PARENT") == expected
    assert _both_routes(roots, "CACHE_CHILD") == []
    rows = set(_row(roots, "PARENT", "CACHE_CHILD").cache_property_requirements)
    assert (("$ref:CACHE2", "X", None, True) in rows) == (removal != "proved"), rows
    twin = _errors([("PARENT", _flattened(parent, child))], "PARENT")
    assert bool(twin) == bool(expected), (twin, expected)


@pytest.mark.parametrize("past_a_message", (False, True), ids=("no_step", "message"))
@pytest.mark.parametrize("mid_form", ("scheduled", "passthrough"))
@pytest.mark.parametrize("removal", sorted(_REMOVALS_OF_CACHE2))
def test_a_passthrough_call_discharges_the_ride_on_obligation_through_the_same_authority(
        removal, mid_form, past_a_message):
    """The same function serves a call discharging a passthrough child's bound path: MID
    retrieves the re-cached documents and hands them to BOUND, whose binding its caller's
    writers compose. The verdict follows the removal's proof exactly as the child's own binding
    does above."""
    mid = _root(mid_form, *(_REMOVALS_OF_CACHE2[removal] + [
        _RECACHE_LEG,
        {"steps": [_READ2] + ([_MSG] if past_a_message else []), "terminal": _call("BOUND")}]))
    expected = [] if removal == "proved" else [(_NO_DYNAMIC_SEGMENT, _AT_THE_THIRD_LEGS_CALL)]
    literal = _legs(_STAGES_X, _LITERAL_INTO_CACHE2, {"steps": [], "terminal": _call("MID")})
    roots = [("PARENT", literal), ("MID", mid), ("BOUND", _BOUND)]
    assert _both_routes(roots, "PARENT") == expected
    assert _both_routes(roots, "MID") == []
    inline = _legs(*(_REMOVALS_OF_CACHE2[removal] + [
        _RECACHE_LEG,
        {"steps": [_READ2] + ([_MSG] if past_a_message else []) + [_BOUND_GET], "terminal": _STOP}]))
    twin = _errors([("PARENT", _flattened(literal, inline))], "PARENT")
    assert bool(twin) == bool(expected), (twin, expected)
    # CONTROL: the caller that composes X dynamically into both caches is admitted either way.
    dynamic = [("PARENT", _legs(_STAGES_X, _DYNAMIC_INTO_CACHE2, {"steps": [], "terminal": _call("MID")})),
               ("MID", mid), ("BOUND", _BOUND)]
    assert _both_routes(dynamic, "PARENT") == []


@pytest.mark.parametrize("use", ("bound", "ordinary"))
@pytest.mark.parametrize("removal", sorted(_REMOVALS_OF_CACHE2))
def test_a_middle_that_provably_emptied_the_cache_charges_its_caller_nothing_for_it(use, removal):
    """The inherited-row site. MID empties CACHE2, re-caches its caller's CACHE into it and
    calls a grandchild that uses X on what it retrieves from CACHE2. MID's own walk cannot
    prove that use (only a seeded caller cohort for CACHE can), so its refusal recorded a row
    for CACHE2 — and that refusal attributed the row without asking the question the proved
    channel beside it already asked, charging the caller for a cache the walk proved it had
    emptied. Measured before the correction: the literal (bound) and X-less (ordinary) caller
    refused at its call although the flattened graph of the same legs is clean."""
    grandchild = _legs(_BINDS2 if use == "bound" else _READS2, {"steps": [_MSG], "terminal": _STOP})
    mid = _legs(*(_REMOVALS_OF_CACHE2[removal] + [
        _RECACHE_LEG, {"steps": [], "terminal": _call("CACHE_CHILD")}]))
    bad = _LITERAL_INTO_CACHE2 if use == "bound" else _X_LESS_INTO_CACHE2
    refusal = ((_NO_DYNAMIC_SEGMENT, _AT_THE_THIRD_LEGS_CALL) if use == "bound"
               else (_READ_BEFORE_WRITE, _AT_THE_THIRD_LEGS_TERMINAL))
    parent = _legs(_STAGES_X, bad, {"steps": [], "terminal": _call("MID")})
    roots = [("PARENT", parent), ("MID", mid), ("CACHE_CHILD", grandchild)]
    expected = [] if removal == "proved" else [refusal]
    assert _both_routes(roots, "PARENT") == expected
    for key in ("MID", "CACHE_CHILD"):
        assert _both_routes(roots, key) == [], key
    rows = set(_row(roots, "PARENT", "MID").cache_property_requirements)
    assert any(row[0] == "$ref:CACHE2" for row in rows) == (removal != "proved"), rows
    twin = _errors([("PARENT", _flattened(parent, mid, grandchild))], "PARENT")
    assert bool(twin) == bool(expected), (twin, expected)


def test_the_one_authority_is_load_bearing_for_every_ride_on_site(monkeypatch):
    """Non-vacuity for CDX-184-r20-01: with `_caller_documents_may_reach` answering yes
    everywhere — no proved removal ends anything — the caller whose literal segment the proved
    removal discarded is refused again at each of the three sites, and CACHE2's row returns."""
    literal_parent = _legs(_STAGES_X, _LITERAL_INTO_CACHE2, {"steps": [], "terminal": _call("CACHE_CHILD")})
    sites = {
        "own binding": [("PARENT", literal_parent), ("CACHE_CHILD", _legs(
            _EMPTIES2, _RECACHE_LEG, _BINDS2))],
        "own binding past a Message": [("PARENT", literal_parent), ("CACHE_CHILD", _legs(
            _EMPTIES2, _RECACHE_LEG, {"steps": [_READ2, _MSG, _BOUND_GET], "terminal": _STOP}))],
        "passthrough call discharge": [
            ("PARENT", _legs(_STAGES_X, _LITERAL_INTO_CACHE2, {"steps": [], "terminal": _call("MID")})),
            ("MID", _legs(_EMPTIES2, _RECACHE_LEG, {"steps": [_READ2], "terminal": _call("BOUND")})),
            ("BOUND", _BOUND)],
        "inherited row": [
            ("PARENT", _legs(_STAGES_X, _LITERAL_INTO_CACHE2, {"steps": [], "terminal": _call("MID")})),
            ("MID", _legs(_EMPTIES2, _RECACHE_LEG, {"steps": [], "terminal": _call("CACHE_CHILD")})),
            ("CACHE_CHILD", _legs(_BINDS2, {"steps": [_MSG], "terminal": _STOP}))],
    }
    for site, roots in sites.items():
        assert _errors(roots, "PARENT") == [], site
    monkeypatch.setattr(lineage, "_caller_documents_may_reach", lambda state, cache_ref: True)
    for site, roots in sites.items():
        assert _errors(roots, "PARENT") == [(_NO_DYNAMIC_SEGMENT, _AT_THE_THIRD_LEGS_CALL)], site
        callee = roots[1][0]
        assert any(row[0] == "$ref:CACHE2" for row in _row(
            roots, "PARENT", callee).cache_property_requirements), site


#: A child that FILLS the cache itself, then retrieves it and uses X: ``(its fill, the steps
#: after the retrieve, what a caller stores that breaks the use, what satisfies it, the
#: refusal at the caller's call)``.
_OWN_FILL_USES = {
    "bound": (_DYNAMIC_INTO_CACHE2, [_BOUND_GET], _LITERAL_INTO_CACHE2, _DYNAMIC_INTO_CACHE2,
              (_NO_DYNAMIC_SEGMENT, _AT_THE_CALL)),
    "bound_past_a_message": (_DYNAMIC_INTO_CACHE2, [_MSG, _BOUND_GET], _LITERAL_INTO_CACHE2,
                             _DYNAMIC_INTO_CACHE2, (_NO_DYNAMIC_SEGMENT, _AT_THE_CALL)),
    "bound_past_a_connector_call": (_DYNAMIC_INTO_CACHE2, [_GET, _BOUND_GET], _LITERAL_INTO_CACHE2,
                                    _DYNAMIC_INTO_CACHE2, (_NO_DYNAMIC_SEGMENT, _AT_THE_CALL)),
    "bound_past_a_map": ({"steps": [_GETP1, _TO_P2, _DYNAMIC_X], "terminal": _PUT2},
                         [_CONSUMES_P2, _MSG, _BOUND_GET],
                         {"steps": [_GETP1, _TO_P2, _STATIC_X], "terminal": _PUT2},
                         {"steps": [_GETP1, _TO_P2, _DYNAMIC_X], "terminal": _PUT2},
                         (_NO_DYNAMIC_SEGMENT, _AT_THE_CALL)),
    "ordinary": (_DYNAMIC_INTO_CACHE2, [_READS_X], _X_LESS_INTO_CACHE2, _DYNAMIC_INTO_CACHE2,
                 (_READ_BEFORE_WRITE, _AT_THE_SECOND_LEGS_CALL)),
    "ordinary_past_a_message": (_DYNAMIC_INTO_CACHE2, [_MSG, _READS_X], _X_LESS_INTO_CACHE2,
                                _DYNAMIC_INTO_CACHE2, (_READ_BEFORE_WRITE, _AT_THE_SECOND_LEGS_CALL)),
}


@pytest.mark.parametrize("form", ("scheduled", "passthrough"))
@pytest.mark.parametrize("use", sorted(_OWN_FILL_USES))
def test_a_child_that_fills_the_cache_itself_still_owes_the_callers_that_share_it(use, form):
    """Finding O of correction batch 21a. The child stores documents carrying a dynamic X in
    CACHE2 and uses X on what it retrieves from CACHE2. At runtime the cache is shared and
    appends, so the retrieve hands on the child's documents AND whatever its caller stored
    there before the call: a caller's literal X reaches the bound path, and a caller's X-less
    documents reach the read. Amendment 3 §7: "Append possible cohorts; do not treat the last
    cache write as replacing earlier contents."

    Every row was empty, because the retrieve's caller marker was keyed on "no write of this
    process reached the cache": the child's own write was read as replacing its caller's
    documents. Measured before the correction: every breaking caller below admitted on validate
    and compile, for both child forms, while the flattened graph refused it. CONTROLS, against
    over-correction: a caller that stores satisfying documents, and one that stores nothing in
    the cache, are admitted, as their twins are."""
    fill, after_the_retrieve, breaks, satisfies, refusal = _OWN_FILL_USES[use]
    child = _root(form, fill, {"steps": [_READ2] + after_the_retrieve, "terminal": _STOP})
    for caller_leg, expected in ((breaks, [refusal]), (satisfies, []), (_STORES_NOTHING, [])):
        parent = _legs(caller_leg, {"steps": [], "terminal": _call("CACHE_CHILD")})
        roots = [("PARENT", parent), ("CACHE_CHILD", child)]
        assert _both_routes(roots, "PARENT") == expected, caller_leg
        assert _both_routes(roots, "CACHE_CHILD") == [], caller_leg
        twin = _errors([("PARENT", _flattened(parent, child))], "PARENT")
        assert bool(twin) == bool(expected), (caller_leg, twin)
    bound = use.startswith("bound")
    assert ("$ref:CACHE2", "X", None, bound) in _row(roots, "PARENT", "CACHE_CHILD").cache_property_requirements


def test_a_forwarder_hands_its_caller_the_obligation_of_a_child_that_fills_the_cache_itself():
    """Finding O through a middle that stores nothing: the row travels up and the caller that
    stored a literal X in the shared cache is refused at ITS call, as the flattened graph is."""
    child = _legs(_DYNAMIC_INTO_CACHE2, _BINDS2)
    mid = _legs(_STORES_NOTHING, {"steps": [], "terminal": _call("CACHE_CHILD")})
    for caller_leg, expected in ((_LITERAL_INTO_CACHE2, [(_NO_DYNAMIC_SEGMENT, _AT_THE_CALL)]),
                                 (_DYNAMIC_INTO_CACHE2, [])):
        parent = _legs(caller_leg, {"steps": [], "terminal": _call("MID")})
        roots = [("PARENT", parent), ("MID", mid), ("CACHE_CHILD", child)]
        assert _both_routes(roots, "PARENT") == expected
        assert _both_routes(roots, "MID") == []
        assert _row(roots, "PARENT", "MID").cache_property_requirements == (("$ref:CACHE2", "X", None, True),)
        twin = _errors([("PARENT", _flattened(parent, mid, child))], "PARENT")
        assert bool(twin) == bool(expected), twin


def test_the_retired_own_write_proxy_is_load_bearing(monkeypatch):
    """Non-vacuity for finding O: with the one authority answering the retired proxy — "a
    caller's documents may be there only when no write of this process reached the cache" —
    the caller whose literal X shares the cache the child filled is admitted again, on the
    bound and the ordinary channel, and the row is gone."""
    cells = {
        "bound": ([("PARENT", _legs(_LITERAL_INTO_CACHE2, {"steps": [], "terminal": _call("CACHE_CHILD")})),
                   ("CACHE_CHILD", _legs(_DYNAMIC_INTO_CACHE2, _BINDS2))],
                  (_NO_DYNAMIC_SEGMENT, _AT_THE_CALL)),
        "ordinary": ([("PARENT", _legs(_X_LESS_INTO_CACHE2, {"steps": [], "terminal": _call("CACHE_CHILD")})),
                      ("CACHE_CHILD", _legs(_DYNAMIC_INTO_CACHE2, _READS2))],
                     (_READ_BEFORE_WRITE, _AT_THE_SECOND_LEGS_CALL)),
    }
    for use, (roots, refusal) in cells.items():
        assert _errors(roots, "PARENT") == [refusal], use
    monkeypatch.setattr(lineage, "_caller_documents_may_reach",
                        lambda state, cache_ref: not state.content_of(cache_ref))
    for use, (roots, _refusal) in cells.items():
        assert _errors(roots, "PARENT") == [], use
        assert _row(roots, "PARENT", "CACHE_CHILD").cache_property_requirements == (), use


#: A child that stages P2 documents carrying X in CACHE itself and maps what it retrieves, so
#: its contract owes a CONTENT row for CACHE, then uses X in a way no cached-property refusal
#: ever recorded: a Decision that tracks X, and a writer that re-composes X from its `current`
#: value for a bound path.
_STAGES_P2_WITH_X = {"steps": [_GETP1, _TO_P2, _DYNAMIC_X], "terminal": _PUT}
_DECIDES_ON_X = {"kind": "decision", "comparison": "equals",
                 "left": {"value_type": "track", "property_id": "dynamicdocument.X"},
                 "right": {"value_type": "static", "static_value": "a"},
                 "true_arm": {"steps": [_MSG], "terminal": _STOP}, "false_arm": {"steps": [], "terminal": _STOP}}
_RECOMPOSES_X = {"kind": "set_ddp", "name": "X", "source_values": [
    {"value_type": "current"}, {"value_type": "static", "value": "/tail"}]}
_CONTENT_AND_AN_UNRECORDED_USE = {
    "a_decision_tracking_x": _legs(
        _STAGES_P2_WITH_X, {"steps": [_READ, _CONSUMES_P2], "terminal": _DECIDES_ON_X}),
    "a_current_recomposition_bound": _legs(_STAGES_P2_WITH_X, {
        "steps": [_READ, _CONSUMES_P2, _MSG, _RECOMPOSES_X, _BOUND_GET], "terminal": _STOP}),
}


@pytest.mark.parametrize("use", sorted(_CONTENT_AND_AN_UNRECORDED_USE))
def test_a_use_that_owes_nothing_by_a_refusal_still_names_the_cache_it_owes(use):
    """Over-correction guard, found by this correction's own verification. The content row makes
    every caller seed CACHE's content for the child, and a cache seeded for content that no
    cached-property row names enters with an UNKNOWN cohort. A use of X no refusal ever recorded
    a row for — a non-strict Decision operand of a property the child writes, and a writer that
    re-uses X's `current` value for a bound path — then met the child's own cohort with the
    unknown one, and the called child was refused under every caller, including one that stores
    nothing, while it is clean on its own and so is every flattened twin but the breaking one.

    Each such use is refused unestablished, so each owes its callers exactly what a refusal would
    have recorded: the row names CACHE, the seed carries X, and the child is admitted under its
    callers again. The `current` recomposition was also a fail-open of finding O's kind before
    the correction: a caller whose documents in CACHE lack X was admitted while its twin is
    refused. It is now refused at its call (as an establishment row: the recomposing writer's own
    non-static source composes the segment, so a caller's literal X is admitted, as its twin is)."""
    child = _CONTENT_AND_AN_UNRECORDED_USE[use]
    call_leg = {"steps": [], "terminal": _call("CACHE_CHILD")}
    assert _both_routes([("CACHE_CHILD", child)], "CACHE_CHILD") == []
    x_less = {"steps": [_GETP1, _TO_P2], "terminal": _PUT}
    literal = {"steps": [_GETP1, _TO_P2, _STATIC_X], "terminal": _PUT}
    for caller_leg, refused in ((_STORES_NOTHING, False), (_STAGES_P2_WITH_X, False), (literal, False),
                                (x_less, True)):
        parent = _legs(caller_leg, call_leg)
        roots = [("PARENT", parent), ("CACHE_CHILD", child)]
        assert _both_routes(roots, "CACHE_CHILD") == [], caller_leg
        expected = [(_READ_BEFORE_WRITE, _AT_THE_SECOND_LEGS_CALL)] if refused else []
        assert _both_routes(roots, "PARENT") == expected, caller_leg
        twin = _errors([("PARENT", _flattened(parent, child))], "PARENT")
        assert bool(twin) == refused, (caller_leg, twin)
    row = _row(roots, "PARENT", "CACHE_CHILD")
    assert ("$ref:CACHE", "X", None, False) in row.cache_property_requirements, row
    assert ("$ref:CACHE", "$ref:P2") in row.cache_requirements, row


#: ``(what the caller or the first leg stores, the use leg, the refusal code, the use's
#: sub-pointer)`` for the unproved-removal shapes.
_UNPROVED_REMOVAL_USES = {
    "bound": (_LITERAL_INTO_CACHE2, _BINDS2, _NO_DYNAMIC_SEGMENT, "/steps/1/path_binding"),
    "ordinary": (_X_LESS_INTO_CACHE2, _READS2, _READ_BEFORE_WRITE, "/steps/1"),
}


@pytest.mark.parametrize("use", sorted(_UNPROVED_REMOVAL_USES))
def test_a_removal_the_walk_does_not_prove_keeps_what_the_cache_may_hold(use):
    """Finding S7 of correction batch 21a, in one process and across a call.

    A whole-cache removal behind a producer that may return no rows may never run while the
    process still completes normally, and the producer in front of a later leg's write is
    independent of it. After it the cache is the meet of "it ran" and "it was skipped": its
    guarantees clear, and what it may hold survives — amendment 3 §7 unions possible cohorts at
    convergence and says "Never discard an inconvenient writer alternative". The removal
    cleared the cohorts unconditionally, so a literal (bound) or X-less (ordinary) document
    stored before it reached the use unrefused, while the same legs with no removal are refused.
    Measured before the correction: the unproved cells below clean on validate and compile, in
    process and at the caller's call. The proved removal still clears them.

    Across a call the shape is also finding O's — the child fills the cache itself after the
    removal — so there the removal's proof decides through the seal: an unproved removal grants
    none, and the child still owes the callers whose documents may have survived it."""
    before, use_leg, code, sub_path = _UNPROVED_REMOVAL_USES[use]
    unproved = _legs(before, _EMPTIES2_BEHIND_A_GET, _DYNAMIC_INTO_CACHE2, use_leg)
    assert _both_routes([("PARENT", unproved)], "PARENT") == [(code, "/body/steps/0/legs/3" + sub_path)]
    no_removal = _legs(before, _DYNAMIC_INTO_CACHE2, use_leg)
    assert _both_routes([("PARENT", no_removal)], "PARENT") == [(code, "/body/steps/0/legs/2" + sub_path)]
    proved = _legs(before, _EMPTIES2, _DYNAMIC_INTO_CACHE2, use_leg)
    assert _both_routes([("PARENT", proved)], "PARENT") == []
    # Across a call: the removal and the refill are the child's, the breaking document is its caller's.
    at_the_call = _AT_THE_CALL if use == "bound" else _AT_THE_SECOND_LEGS_CALL
    for removal, expected in (("unproved", [(code, at_the_call)]), ("proved", [])):
        child = _legs(*(_REMOVALS_OF_CACHE2[removal] + [_DYNAMIC_INTO_CACHE2, use_leg]))
        parent = _legs(before, {"steps": [], "terminal": _call("CACHE_CHILD")})
        roots = [("PARENT", parent), ("CACHE_CHILD", child)]
        assert _both_routes(roots, "PARENT") == expected, removal
        assert _both_routes(roots, "CACHE_CHILD") == [], removal
        twin = _errors([("PARENT", _flattened(parent, child))], "PARENT")
        assert bool(twin) == bool(expected), (removal, twin)


def test_the_proof_gate_on_the_cohort_clear_is_load_bearing(monkeypatch):
    """Non-vacuity for S7: with the pre-correction removal — content and cohorts cleared
    whatever the proof, only the seal gated — the literal (bound) and the X-less document
    (ordinary) stored before an unproved removal are admitted again in one process.

    Measured, and stated so the witness is not over-read: the same shape ACROSS a call stays
    refused under this mutant, because there it is also finding O's shape and the unproved
    removal grants no seal — so that cell is O's to witness (above), not this one's."""
    cells = {
        "bound": ([("PARENT", _legs(_LITERAL_INTO_CACHE2, _EMPTIES2_BEHIND_A_GET, _DYNAMIC_INTO_CACHE2, _BINDS2))],
                  [(_NO_DYNAMIC_SEGMENT, "/body/steps/0/legs/3/steps/1/path_binding")]),
        "ordinary": ([("PARENT", _legs(_X_LESS_INTO_CACHE2, _EMPTIES2_BEHIND_A_GET, _DYNAMIC_INTO_CACHE2, _READS2))],
                     [(_READ_BEFORE_WRITE, "/body/steps/0/legs/3/steps/1")]),
    }
    called = [("PARENT", _legs(_LITERAL_INTO_CACHE2, {"steps": [], "terminal": _call("CACHE_CHILD")})),
              ("CACHE_CHILD", _legs(_EMPTIES2_BEHIND_A_GET, _DYNAMIC_INTO_CACHE2, _BINDS2))]
    for use, (roots, refusal) in cells.items():
        assert _errors(roots, "PARENT") == refusal, use

    def clears_whatever_the_proof(state, cache_ref, proved_to_run):
        after = lineage._without_cache_establishment(state.without_content(cache_ref), cache_ref)
        return after.with_sealed_cache(cache_ref) if proved_to_run else after

    monkeypatch.setattr(lineage, "_after_a_whole_cache_removal", clears_whatever_the_proof)
    for use, (roots, _refusal) in cells.items():
        assert _errors(roots, "PARENT") == [], use
    assert _errors(called, "PARENT") == [(_NO_DYNAMIC_SEGMENT, _AT_THE_CALL)]


#: A process that STAGES P2 documents in CACHE itself, behind each kind of removal.
_STAGES_P2_BEHIND = {
    "proved": [_EMPTIES_IT],
    "unproved": [{"steps": [_GET], "terminal": _REMOVE}],
    "none": [],
}


@pytest.mark.parametrize("removal", sorted(_STAGES_P2_BEHIND))
@pytest.mark.parametrize("site", ("inherited", "own"))
def test_a_process_that_stages_the_profile_itself_still_owes_the_content_its_callers_share(site, removal):
    """The content channel, the limit SELF-184-43 recorded. A process that staged P2 in the
    cache itself inherited nothing (at a call) and recorded nothing (at its own map), because
    both were keyed on "no write of this process reaches the cache". The cache appends, so a
    caller that stored P1 documents there before the call has them retrieved beside the
    process's P2 ones and mapped as P2 (amendment 3 §7). Measured before the correction: the P1
    caller admitted at every removal while the flattened graph refused the map, except at the
    proved removal, whose flattened graph is clean too.

    CONTROLS, against over-correction: a caller that stores nothing in the cache stores nothing
    of the wrong profile and is admitted — the vacuous rule the cached-property rows already
    apply, now applied to the content rows the same way — and so is a P2 caller."""
    process_legs = _STAGES_P2_BEHIND[removal] + [_STAGES_P2]
    if site == "inherited":
        callee = [("MID", _legs(*(process_legs + [{"steps": [], "terminal": _call("CACHE_CHILD")}]))),
                  ("CACHE_CHILD", _TYPED_CACHE_CHILD)]
    else:
        callee = [("CACHE_CHILD", _legs(*(process_legs + [
            {"steps": [_READ, _CONSUMES_P2], "terminal": _STOP}])))]
    called = callee[0][0]
    p1 = {"steps": [_GETP1], "terminal": _PUT}
    for caller_leg, expected in (
        (p1, [] if removal == "proved" else [(PROCESS_IR_SEMANTIC_PROFILE_MISMATCH, _AT_THE_CALL)]),
        (_STAGES_P2, []),
        (_STORES_NOTHING, []),
    ):
        parent = _legs(caller_leg, {"steps": [], "terminal": _call(called)})
        roots = [("PARENT", parent)] + callee
        assert _both_routes(roots, "PARENT") == expected, caller_leg
        for key, _document in callee:
            assert _both_routes(roots, key) == [], (caller_leg, key)
        twin = _errors([("PARENT", _flattened(parent, *[document for _key, document in callee]))], "PARENT")
        assert bool(twin) == bool(expected), (caller_leg, twin)
    rows = _row(roots, "PARENT", called).cache_requirements
    assert (("$ref:CACHE", "$ref:P2") in rows) == (removal != "proved"), rows


def test_the_content_channel_asks_the_one_authority_and_the_vacuous_rule(monkeypatch):
    """Non-vacuity for the content channel, one witness per half. With the retired proxy in the
    authority's place the P1 caller of the self-staging middle is admitted again; with the
    vacuous rule gone the caller that stores nothing in the cache is refused for it."""
    mid = _legs(_STAGES_P2, {"steps": [], "terminal": _call("CACHE_CHILD")})

    def chain(caller_leg):
        return [("PARENT", _legs(caller_leg, {"steps": [], "terminal": _call("MID")})),
                ("MID", mid), ("CACHE_CHILD", _TYPED_CACHE_CHILD)]

    p1, nothing = chain({"steps": [_GETP1], "terminal": _PUT}), chain(_STORES_NOTHING)
    refusal = (PROCESS_IR_SEMANTIC_PROFILE_MISMATCH, _AT_THE_CALL)
    assert _errors(p1, "PARENT") == [refusal]
    assert _errors(nothing, "PARENT") == []
    with monkeypatch.context() as patched:
        patched.setattr(lineage, "_caller_documents_may_reach",
                        lambda state, cache_ref: not state.content_of(cache_ref))
        assert _errors(p1, "PARENT") == []
    with monkeypatch.context() as patched:
        patched.setattr(lineage, "_call_stores_nothing_in", lambda state, cache_ref, external_writer: False)
        assert _errors(nothing, "PARENT") == [refusal]


_RECACHES_THEN_READS = _legs(_RECACHE_LEG, _READS2)
_RECACHES_THEN_READS_PAST_A_MESSAGE = _legs(
    _RECACHE_LEG, {"steps": [_READ2, _MSG, _READS_X], "terminal": _STOP})
#: What the caller stores in each cache, and the verdict the read past the re-cache earns.
_RECACHE_READ_CALLERS = {
    "dynamic_in_both": (_STAGES_X, _DYNAMIC_INTO_CACHE2, ()),
    "x_less_in_the_cache_the_read_rides_on": (
        _STAGES_X, _X_LESS_INTO_CACHE2, ((_READ_BEFORE_WRITE, _AT_THE_THIRD_LEGS_TERMINAL),)),
    "x_less_in_the_cache_the_property_came_from": (
        {"steps": [_GET], "terminal": _PUT}, _DYNAMIC_INTO_CACHE2,
        ((_READ_BEFORE_WRITE, _AT_THE_THIRD_LEGS_TERMINAL),)),
    "nothing_in_the_cache_the_read_rides_on": (_STAGES_X, _STORES_NOTHING, ()),
}


@pytest.mark.parametrize("child", ("no_step", "message"))
@pytest.mark.parametrize("caller", sorted(_RECACHE_READ_CALLERS))
def test_an_ordinary_read_past_a_recache_rides_on_that_cache_past_a_message_too(caller, child):
    """The ordinary-read twin of ARCH-184-r2-01, reached by this correction's coverage matrix.
    A read of X on documents retrieved from the re-cache is a use of that cache's documents, and
    a Message between the retrieve and the read hands on exactly the documents it received, so
    it changes nothing about which caller documents reach the read. The binding already carried
    the cache it rides on past a Message; the read did not. Once the retrieve's own marker
    charged the second cache (a caller's documents may share it beside the re-cached ones), the
    same read one Message later still charged nothing, and a caller's X-less documents in that
    cache escaped it. Measured before the correction: both child spellings admitted the X-less
    caller of the second cache while the flattened graph refused it."""
    into_cache, into_cache2, expected = _RECACHE_READ_CALLERS[caller]
    document = _RECACHES_THEN_READS_PAST_A_MESSAGE if child == "message" else _RECACHES_THEN_READS
    parent = _legs(into_cache, into_cache2, {"steps": [], "terminal": _call("CACHE_CHILD")})
    roots = [("PARENT", parent), ("CACHE_CHILD", document)]
    assert _both_routes(roots, "PARENT") == list(expected)
    assert _both_routes(roots, "CACHE_CHILD") == []
    assert ("$ref:CACHE2", "X", None, False) in _row(roots, "PARENT", "CACHE_CHILD").cache_property_requirements
    twin = _errors([("PARENT", _flattened(parent, document))], "PARENT")
    assert bool(twin) == bool(expected), (twin, expected)


def test_the_ride_on_credit_for_an_ordinary_read_is_load_bearing(monkeypatch):
    """Non-vacuity: with the walk's ordinary-read ride-on record dropped, the X-less caller of
    the second cache is admitted again behind the Message, while the read with no step keeps its
    refusal through the retrieve's own marker."""
    into_cache, into_cache2, expected = _RECACHE_READ_CALLERS["x_less_in_the_cache_the_read_rides_on"]
    parent = _legs(into_cache, into_cache2, {"steps": [], "terminal": _call("CACHE_CHILD")})
    past = [("PARENT", parent), ("CACHE_CHILD", _RECACHES_THEN_READS_PAST_A_MESSAGE)]
    beside = [("PARENT", parent), ("CACHE_CHILD", _RECACHES_THEN_READS)]
    assert _errors(past, "PARENT") == list(expected)
    real = lineage.walk_lineage

    def without_the_read_ride_on(prepared, capabilities=DEFAULT_VALIDATION_CAPABILITIES):
        return real(prepared, capabilities)._replace(read_cache_origins=())

    monkeypatch.setattr(lineage, "walk_lineage", without_the_read_ride_on)
    assert _errors(past, "PARENT") == []
    assert _errors(beside, "PARENT") == list(expected)


@pytest.mark.parametrize("use", ("bound", "ordinary", "content"))
def test_a_per_document_caller_of_a_child_that_fills_and_reads_its_own_shared_cache_is_refused(use):
    """Amendment 1 rule 8, newly reachable. A No Data child a Data Passthrough caller runs once
    per document appends to the cache on every run, so each later run's retrieve also hands on
    what the earlier runs stored: "A later invocation whose required cache may have been removed
    or changed is refused as a verified composition." The child now requires something of that
    cache's documents — its callers' documents are owed (finding O) — so rule 8 sees the change.
    A scheduled caller runs it once and is admitted, and so is a per-document caller of the same
    child behind a proved removal, which empties the cache on every run before refilling it."""
    fills = {"bound": (_DYNAMIC_INTO_CACHE2, [_BOUND_GET]),
             "ordinary": (_DYNAMIC_INTO_CACHE2, [_READS_X]),
             "content": ({"steps": [_GETP1, _TO_P2], "terminal": _PUT2}, [_CONSUMES_P2])}
    fill, after_the_retrieve = fills[use]
    use_leg = {"steps": [_READ2] + after_the_retrieve, "terminal": _STOP}
    call_leg = {"steps": [], "terminal": _call("CACHE_CHILD")}
    per_document = _passthrough_root(_STORES_NOTHING, call_leg)
    scheduled = _legs(_STORES_NOTHING, call_leg)
    child = _legs(fill, use_leg)
    placement = [(PROCESS_IR_CAPABILITY_PROCESS_CALL_REPEATED_RUN_UNSTABLE, "/body/steps/1/legs/1/terminal")]
    assert _both_routes([("PARENT", per_document), ("CACHE_CHILD", child)], "PARENT") == placement
    assert _both_routes([("PARENT", scheduled), ("CACHE_CHILD", child)], "PARENT") == []
    emptied_each_run = _legs(_EMPTIES2, fill, use_leg)
    assert _both_routes([("PARENT", per_document), ("CACHE_CHILD", emptied_each_run)], "PARENT") == []


def _cache_mutation_kinds():
    """Every authored node kind that changes a document cache, read off the model: a kind that
    names a cache and is not a retrieve (`TRIGGERED_REPLACEMENT_KINDS`)."""
    import typing

    from pydantic import BaseModel

    from boomi_mcp.models import process_ir as model

    named = {
        typing.get_args(member.model_fields["kind"].annotation)[0]
        for member in vars(model).values()
        if isinstance(member, type) and issubclass(member, BaseModel)
        and "cache_ref" in member.model_fields and "kind" in member.model_fields
    }
    return named - set(model.TRIGGERED_REPLACEMENT_KINDS)


#: QA-184-s1-r21-01 and B21A-TXT-01. The children a per-document caller is refused for under
#: amendment 1 rule 8, per KIND of change a child may make to the cache it requires: each
#: `(child, what its caller stores first)`. An Add to Cache: the re-cache child, the same with a
#: Message before its binding, and a child that fills CACHE2 and reads it back. A Remove from
#: Cache: a child that binds on what its caller stored in CACHE2 and then empties CACHE2, adding
#: nothing — so the cause may not claim that a later run reads what an earlier one added.
_REPEATED_RUN_CHILDREN = {
    "cache_put": {
        "recache": (_RECACHES_THEN_BINDS, [_STAGES_X, _DYNAMIC_INTO_CACHE2]),
        "recache_past_a_message": (_RECACHES_THEN_BINDS_PAST_A_MESSAGE, [_STAGES_X, _DYNAMIC_INTO_CACHE2]),
        "fills_and_reads_its_own_cache": (_legs(_DYNAMIC_INTO_CACHE2, _BINDS2), [_STORES_NOTHING]),
    },
    "cache_remove": {
        "reads_its_callers_cache_then_empties_it": (_legs(_BINDS2, _EMPTIES2), [_DYNAMIC_INTO_CACHE2]),
    },
}
_REPEATED_RUN_CASES = sorted((kind, name) for kind, children in _REPEATED_RUN_CHILDREN.items() for name in children)


def test_the_repeated_run_witnesses_cover_every_kind_of_cache_change():
    """The enumeration is derived, not hand-picked: one entry per node kind that can change a
    cache, each child's own contract says it may change the cache it requires, and the child
    changes that cache with that kind and no other."""
    assert set(_REPEATED_RUN_CHILDREN) == _cache_mutation_kinds() == {"cache_put", "cache_remove"}
    for kind, children in _REPEATED_RUN_CHILDREN.items():
        for name, (document, caller_prefix) in children.items():
            roots = [("PARENT", _passthrough_root(*(caller_prefix + [{"steps": [], "terminal": _call("CACHE_CHILD")}]))),
                     ("CACHE_CHILD", document)]
            row = _row(roots, "PARENT", "CACHE_CHILD")
            assert ("cache", "$ref:CACHE2") in row.mutated_state, (kind, name, row)
            changes = {leg["terminal"]["kind"] for leg in _branch_legs(document)
                       if leg["terminal"].get("cache_ref") == "$ref:CACHE2"} & _cache_mutation_kinds()
            assert changes == {kind}, (kind, name, changes)


@pytest.mark.parametrize("kind, child", _REPEATED_RUN_CASES, ids=["-".join(case) for case in _REPEATED_RUN_CASES])
def test_the_repeated_run_refusal_states_its_cause_and_its_way_out(kind, child):
    """QA-184-s1-r21-01, measured live at 5b5038c: this refusal served the placement code, whose
    message ("a process call is placed after a composition ProcessIR v1 does not admit") names
    no cause and whose remediation describes a step prefix the call does not have. It now has
    its own code: the message says the child runs once for each document and requires a cache it
    may also add to OR EMPTY, so a later run may find what an earlier run added or emptied
    (B21A-TXT-01: the refusal fires for a child that only removes the cache too, and "reads what
    an earlier run added" was false for it); the evidence names the state scope, `cache`; and
    the remediation names the alternatives measured admitted — a Data Passthrough entry called
    wait=true, a path handing the child exactly one document, and, for a child that never reads
    the cache before writing it, a removal after its last write with the call waiting and
    aborting on error. The finding names no cache reference: served findings carry no authored
    text.

    Both general alternatives are measured here too, on the same child, for each kind of cache
    change: rewritten with a passthrough step first and called wait=true, and called from a
    process with no explicit entry, each is admitted."""
    from boomi_mcp.compiler.process_ir import diagnostics as compiler_diagnostics
    from boomi_mcp.compiler.process_ir.diagnostics import ProcessIRCompileError
    from boomi_mcp.compiler.process_ir.pipeline import compile_process_ir_v1

    document, caller_prefix = _REPEATED_RUN_CHILDREN[kind][child]
    call_leg = {"steps": [], "terminal": _call("CACHE_CHILD")}
    roots = [("PARENT", _passthrough_root(*(caller_prefix + [call_leg]))), ("CACHE_CHILD", document)]
    pointer = "/body/steps/1/legs/{0}/terminal".format(len(caller_prefix))
    irs, resolution = _resolve(roots)
    capabilities = resolution.capabilities_by_root["PARENT"]
    validated = [item for item in validate_process_ir(irs["PARENT"], _symbols(), capabilities=capabilities).errors]
    assert [(item.code, item.path) for item in validated] == [
        (PROCESS_IR_CAPABILITY_PROCESS_CALL_REPEATED_RUN_UNSTABLE, pointer)]
    with pytest.raises(ProcessIRCompileError) as caught:
        compile_process_ir_v1(irs["PARENT"], _symbols(), capabilities=capabilities)
    compiled = list(caught.value.diagnostics)
    assert [(item.code, item.path) for item in compiled] == [
        (PROCESS_IR_CAPABILITY_PROCESS_CALL_REPEATED_RUN_UNSTABLE, pointer)]
    for served in (validated[0], compiled[0]):
        message, remediation = served.message, served.remediation
        for cause in ("runs once for each document", "document cache", "add to or empty",
                      "a later run may find what an earlier run added or emptied"):
            assert cause in message, (cause, message)
        assert "a later run reads what an earlier run added" not in message
        for way_out in ("Data Passthrough entry", "wait=true", "exactly one document",
                        "abort_on_error=true", "node_kind='process_call'"):
            assert way_out in remediation, (way_out, remediation)
        # NON-VACUITY: the generic placement text no longer serves for this cause.
        placement = PROCESS_IR_CAPABILITY_PROCESS_CALL_PLACEMENT_UNSUPPORTED
        assert message != compiler_diagnostics._MESSAGES[placement]
        assert remediation != compiler_diagnostics._REMEDIATION[placement]
        assert "direct predecessor" not in remediation
    assert [(evidence.key, evidence.value) for evidence in validated[0].evidence] == [("state_scope", "cache")]
    # The two ways out, measured on the same child.
    as_passthrough = _passthrough_root(*_branch_legs(document))
    waited = [("PARENT", _passthrough_root(*(caller_prefix + [
        {"steps": [], "terminal": _call("CACHE_CHILD", wait=True)}]))), ("CACHE_CHILD", as_passthrough)]
    assert _both_routes(waited, "PARENT") == []
    one_document = [("PARENT", _legs(*(caller_prefix + [call_leg]))), ("CACHE_CHILD", document)]
    assert _both_routes(one_document, "PARENT") == []


def test_the_repeated_run_text_is_what_each_table_serves_for_the_code(monkeypatch):
    """Non-vacuity of the table entry: with this code's text removed from the validator's
    tables the finding falls back to the generic text, which states no cause and names neither
    way out — so the witness above measures the served entry, not a string the finding
    builds."""
    from boomi_mcp.compiler.process_ir.semantic_validation import findings

    code = PROCESS_IR_CAPABILITY_PROCESS_CALL_REPEATED_RUN_UNSTABLE
    served = findings.finding(code, "error", "capability", "/body")
    assert "runs once for each document" in served.message
    assert "Data Passthrough entry" in served.remediation
    assert {row["code"]: row for row in findings.finding_specs()}[code]["message"] == served.message
    monkeypatch.setattr(findings, "_MESSAGES", {k: v for k, v in findings._MESSAGES.items() if k != code})
    monkeypatch.setattr(findings, "_REMEDIATION", {k: v for k, v in findings._REMEDIATION.items() if k != code})
    fallback = findings.finding(code, "error", "capability", "/body")
    assert "runs once for each document" not in fallback.message
    assert "Data Passthrough entry" not in fallback.remediation


#: The middle process for the declared-read channel: ``form -> (its legs before the retrieve,
#: the retrieve, the removal legs by kind, what a caller stores that breaks the read, the
#: caller's other legs)``.
_DECLARED_READ_MIDDLES = {
    "own_fill": ([_STAGES_X], _READ,
                 {"proved": [_EMPTIES_IT], "unproved": [{"steps": [_GET], "terminal": _REMOVE}], "none": []},
                 {"steps": [_GET], "terminal": _PUT}, []),
    "recache": ([_RECACHE_LEG], _READ2, _REMOVALS_OF_CACHE2, _X_LESS_INTO_CACHE2, [_STAGES_X]),
}


@pytest.mark.parametrize("past_a_message", (False, True), ids=("no_step", "message"))
@pytest.mark.parametrize("removal", ("proved", "unproved", "none"))
@pytest.mark.parametrize("form", sorted(_DECLARED_READ_MIDDLES))
def test_a_declared_read_owes_the_callers_that_share_its_cache_unless_the_removal_is_proved(
        form, removal, past_a_message):
    """The declared-read channel over the same case set: a verified subprocess summary's read
    of X at a call, on documents the middle retrieved from a cache it filled or re-cached into.
    The row is owed exactly while a caller's documents may be in that cache, and a caller whose
    documents there lack X is refused at its call — on validate and compile — exactly then; a
    caller that stores nothing there is admitted throughout."""
    from boomi_mcp.compiler.process_ir.diagnostics import ProcessIRCompileError
    from boomi_mcp.compiler.process_ir.pipeline import compile_process_ir_v1
    from boomi_mcp.compiler.process_ir.semantic_validation.contracts import (
        ProcessIRValidationCapabilitiesV1,
    )

    before, retrieve, removals, breaks, caller_prefix = _DECLARED_READ_MIDDLES[form]
    call = _call("CHILD", **_WAITS_AND_ABORTS)
    middle = _legs(*(removals[removal] + before + [
        {"steps": [retrieve] + ([_MSG] if past_a_message else []), "terminal": call}]))
    facts = _derived_facts(middle, _summarised(_SCHEDULED_READS_X))
    owed = removal != "proved"
    assert ((retrieve["cache_ref"], "X", None, False) in facts["cache_property_requirements"]) == owed, facts
    held = ProcessIRValidationCapabilitiesV1(child_entry_contracts=(
        ChildEntryContractV1(process_ref="$ref:MID", **facts),))

    def codes(caller_legs):
        document = parse_process_ir_v1(_legs(*(caller_legs + [{"steps": [], "terminal": _call("MID")}])))
        validated = [(item.code, item.path) for item in
                     validate_process_ir(document, _symbols(), capabilities=held).errors]
        try:
            compile_process_ir_v1(document, _symbols(), capabilities=held)
            compiled = []
        except ProcessIRCompileError as exc:
            compiled = [(item.code, item.path) for item in exc.diagnostics]
        assert sorted(compiled) == sorted(validated), (validated, compiled)
        return {code for code, _path in validated}

    assert (_READ_BEFORE_WRITE in codes(caller_prefix + [breaks])) == owed
    assert codes(caller_prefix + [_STORES_NOTHING]) == set()


# ---------------------------------------------------------------------------
# Correction batch 21a follow-up: a removal proved relative to what can observe it
# ---------------------------------------------------------------------------

from test_issue_184_child_entries import _SPLIT as _SPLITS_THE_DOCUMENTS  # noqa: E402


def _decision_whose_true_arm_is(terminal):
    return {"kind": "decision", "comparison": "equals",
            "left": {"value_type": "static", "static_value": "a"},
            "right": {"value_type": "static", "static_value": "a"},
            "true_arm": {"steps": [], "terminal": terminal},
            "false_arm": {"steps": [], "terminal": _STOP}}


#: Roots whose Branch is fed by ONE producer in front of it, so every leg receives that
#: producer's documents: ``form -> (build the root from the legs, the Branch's own pointer)``.
_SHARED_TRIGGER_ROOTS = {
    "a_get_before_the_branch": (
        lambda legs: _doc(_GET, {"kind": "branch", "legs": legs}), "/body/steps/1"),
    "a_get_and_a_message_before_the_branch": (
        lambda legs: _doc(_GET, _MSG, {"kind": "branch", "legs": legs}), "/body/steps/2"),
    "a_passthrough_entry_and_a_get_before_the_branch": (
        lambda legs: _doc(_ENTRY, _GET, {"kind": "branch", "legs": legs}), "/body/steps/2"),
    "a_decision_arm_branch_behind_a_get": (
        lambda legs: _doc(_GET, _decision_whose_true_arm_is({"kind": "branch", "legs": legs})),
        "/body/steps/1/true_arm/terminal"),
}
#: The removal leg's steps: ones that hand on exactly the documents the leg received, and ones
#: that may not.
_LEG_INPUT_KEPT = {"no_steps": [], "a_message": [_MSG], "a_process_property": [_SET_K]}
_LEG_INPUT_REDUCED = {"its_own_get": [_GET], "a_split": [_SPLITS_THE_DOCUMENTS]}
#: ``use -> (what an earlier leg stores that breaks it, the use leg, the refusal code, sub-pointer)``
_SHARED_TRIGGER_USES = {
    "bound": ([_STATIC_X], {"steps": [_READ2, _BOUND_GET], "terminal": _STOP}, _NO_DYNAMIC_SEGMENT,
              "/legs/3/steps/1/path_binding"),
    "ordinary": ([], {"steps": [_READ2, _READS_X], "terminal": _STOP}, _READ_BEFORE_WRITE, "/legs/3/steps/1"),
}


@pytest.mark.parametrize("use", sorted(_SHARED_TRIGGER_USES))
@pytest.mark.parametrize("kept", sorted(_LEG_INPUT_KEPT))
@pytest.mark.parametrize("root", sorted(_SHARED_TRIGGER_ROOTS))
def test_a_removal_on_the_documents_its_branch_hands_every_leg_has_run_for_every_later_leg(root, kept, use):
    """The over-refusal correction batch 21a introduced, and its bound.

    A producer in front of a Branch feeds every leg the same documents, and a leg that receives
    none runs nothing. A removal whose leg does nothing to those documents but hand them on
    therefore runs whenever a later leg runs at all — so for that later leg the cache WAS
    emptied, and a document an earlier leg stored cannot reach it. Gating the removal's
    MAY-set clear on the whole-run proof alone refused these on validate and compile, although
    every one was admitted before the batch and the runtime cannot reach the breaking document.

    CONTROLS, and they are the bound: a removal leg with a producer of its own (finding S7's
    shape) or a step that may reduce the documents stays refused, because the removal may be
    skipped while the later legs run; and the same legs with no removal are refused."""
    build, branch_pointer = _SHARED_TRIGGER_ROOTS[root]
    breaks, use_leg, code, sub_path = _SHARED_TRIGGER_USES[use]

    def legs(removal_steps):
        return [{"steps": list(breaks), "terminal": _PUT2},
                {"steps": list(removal_steps), "terminal": _REMOVE2},
                {"steps": [_DYNAMIC_X], "terminal": _PUT2},
                use_leg]

    refused = [(code, branch_pointer + sub_path)]
    assert _both_routes([("PARENT", build(legs(_LEG_INPUT_KEPT[kept])))], "PARENT") == []
    for reduced, steps in sorted(_LEG_INPUT_REDUCED.items()):
        assert _both_routes([("PARENT", build(legs(steps)))], "PARENT") == refused, reduced
    no_removal = [legs([])[0], legs([])[2], dict(use_leg)]
    no_removal_pointer = branch_pointer + sub_path.replace("/legs/3/", "/legs/2/")
    assert _both_routes([("PARENT", build(no_removal))], "PARENT") == [(code, no_removal_pointer)]


@pytest.mark.parametrize("use", sorted(_SHARED_TRIGGER_USES))
def test_a_called_child_whose_removal_shares_its_branchs_trigger_owes_nothing_for_the_emptied_cache(use):
    """The same bound across a call. The child's GET feeds its whole Branch; the removal leg
    hands those documents on untouched, so whenever the child's later legs run the caller's
    documents in CACHE2 are gone, and the caller whose breaking document sits there is admitted,
    with no row for CACHE2 — as the flattened graph is. With the removal behind a GET of its own
    leg the caller is refused at its call, exactly as before."""
    breaks, use_leg, code, _sub_path = _SHARED_TRIGGER_USES[use]
    parent = _legs({"steps": [_GET] + list(breaks), "terminal": _PUT2}, {"steps": [], "terminal": _call("CACHE_CHILD")})
    at_the_call = _AT_THE_CALL if use == "bound" else _AT_THE_SECOND_LEGS_CALL
    for removal_steps, expected in (([], []), ([_GET], [(code, at_the_call)])):
        child_legs = [{"steps": list(removal_steps), "terminal": _REMOVE2},
                      {"steps": [_DYNAMIC_X], "terminal": _PUT2}, use_leg]
        child = _doc(_GET, {"kind": "branch", "legs": child_legs})
        roots = [("PARENT", parent), ("CACHE_CHILD", child)]
        assert _both_routes(roots, "PARENT") == expected, removal_steps
        assert _both_routes(roots, "CACHE_CHILD") == [], removal_steps
        rows = _row(roots, "PARENT", "CACHE_CHILD").cache_property_requirements
        assert any(row[0] == "$ref:CACHE2" for row in rows) == bool(expected), rows
        twin = _doc(_GET, {"kind": "branch", "legs": _branch_legs(parent)[:-1] + child_legs})
        assert bool(_errors([("PARENT", twin)], "PARENT")) == bool(expected), removal_steps


def test_the_relative_removal_proof_is_load_bearing(monkeypatch):
    """Non-vacuity, one witness per half. With the removal's proof reduced to the whole-run
    proof, the shared-trigger removal is read as possibly skipped again and both the in-process
    and the called shape are refused. With the Message no longer counted as handing on the
    documents it received, only the removal leg that carries one is refused again: the leg
    input is carried by that authority and nothing else."""
    in_process = [("PARENT", _doc(_GET, {"kind": "branch", "legs": [
        {"steps": [_STATIC_X], "terminal": _PUT2}, {"steps": [], "terminal": _REMOVE2},
        {"steps": [_DYNAMIC_X], "terminal": _PUT2}, _BINDS2]}))]
    past_a_message = [("PARENT", _doc(_GET, {"kind": "branch", "legs": [
        {"steps": [_STATIC_X], "terminal": _PUT2}, {"steps": [_MSG], "terminal": _REMOVE2},
        {"steps": [_DYNAMIC_X], "terminal": _PUT2}, _BINDS2]}))]
    called = [("PARENT", _legs({"steps": [_GET, _STATIC_X], "terminal": _PUT2},
                               {"steps": [], "terminal": _call("CACHE_CHILD")})),
              ("CACHE_CHILD", _doc(_GET, {"kind": "branch", "legs": [
                  {"steps": [], "terminal": _REMOVE2}, {"steps": [_DYNAMIC_X], "terminal": _PUT2}, _BINDS2]}))]
    for roots in (in_process, past_a_message, called):
        assert _errors(roots, "PARENT") == []
    with monkeypatch.context() as patched:
        patched.setattr(lineage, "_removal_runs_before_anything_after_it",
                        lambda stream: lineage._path_provably_runs(stream))
        assert _errors(in_process, "PARENT") == [(_NO_DYNAMIC_SEGMENT, "/body/steps/1/legs/3/steps/1/path_binding")]
        assert _errors(called, "PARENT") == [(_NO_DYNAMIC_SEGMENT, _AT_THE_CALL)]
    with monkeypatch.context() as patched:
        patched.setattr(lineage, "_COUNT_PRESERVING_KINDS", lineage._COUNT_PRESERVING_KINDS - {"message"})
        assert _errors(in_process, "PARENT") == []
        assert _errors(past_a_message, "PARENT") == [
            (_NO_DYNAMIC_SEGMENT, "/body/steps/1/legs/3/steps/1/path_binding")]


def test_what_can_observe_a_removal_is_only_the_later_legs_of_its_own_branch():
    """The premise `_removal_runs_before_anything_after_it` rests on, pinned where the model
    decides it rather than assumed: a whole-cache removal is only ever a Branch-leg terminal;
    nothing follows a Branch; and no Branch sits below another at any depth. So the effect of a
    removal proved relative to its Branch can reach the later legs of that Branch and nothing
    else — a document a removal left behind can never meet a step outside it."""
    from boomi_mcp.models.process_ir import ProcessIRValidationError

    inner = {"kind": "branch", "legs": [{"steps": [], "terminal": _REMOVE2}, _STORES_NOTHING]}
    refused = {
        "PROCESS_IR_SEMANTIC_CONTROL_CONTINUATION_UNSUPPORTED": _doc(
            _GET, {"kind": "branch", "legs": [{"steps": [], "terminal": _PUT2}, _STORES_NOTHING]}, _MSG, _STOP),
        "PROCESS_IR_SEMANTIC_NESTING_LIMIT": _doc(
            _GET, {"kind": "branch", "legs": [{"steps": [], "terminal": _decision_whose_true_arm_is(inner)},
                                              _STORES_NOTHING]}),
        "PROCESS_IR_SCHEMA_INVALID_CARDINALITY": _doc(
            _GET, {"kind": "branch", "legs": [{"steps": [_REMOVE2], "terminal": _STOP}, _STORES_NOTHING]}),
    }
    for code, document in refused.items():
        with pytest.raises(ProcessIRValidationError) as caught:
            parse_process_ir_v1(document)
        assert code in str(caught.value), (code, str(caught.value))
    # CONTROL: the same removal as the terminal of a leg parses.
    parse_process_ir_v1(_doc(_GET, {"kind": "branch", "legs": [
        {"steps": [], "terminal": _REMOVE2}, _STORES_NOTHING]}))


# ---------------------------------------------------------------------------
# Correction batch 21a: the coverage claim over the authority's case set
# ---------------------------------------------------------------------------
#
# {removal: proved, unproved, a shared trigger, none} x {ride-on cache: the seeded cache, a
# re-cache into a second cache that USES the property first, a re-cache that does not} x
# {between the retrieve and the use: nothing, a Message} x {site: the retrieving
# process's own use, a passthrough call's discharge, a grandchild's row a middle inherits} x
# {the retrieving process fills the ride-on cache itself: yes, no} x {use: a bound path, an
# ordinary read, a profile consumer, a bound path whose writer composes from a DEFAULTED ddp
# source}; the declared read is measured above over the removal and step axes. Each cell has three callers: one that stores a breaking document in the ride-on
# cache beside satisfying ones, one that stores only satisfying documents, and one that stores
# nothing there.
#
# A PROVED removal runs on every run (its leg is fed by the scheduled start); an UNPROVED one
# stands behind a GET of its own leg, which may return no rows while the later legs run; a
# SHARED-TRIGGER one stands on the documents a step in front of the whole Branch hands every leg,
# so it has run whenever a later leg runs at all. That step is a GET where the process retrieves
# for its own use, and a Split behind a passthrough entry at the two call sites: a process call
# is not an admitted leg terminal of a Branch a connector call feeds, while one a Split feeds
# parses (measured by `test_the_shared_trigger_spellings_are_what_each_site_can_author`).
#
# The verdict is DERIVED from the runtime the plan describes, not read off this implementation:
# the breaking caller is refused exactly when its document can reach the use — an unproved
# removal or none — and the other two are admitted, each on both entry points and in agreement with
# the flattened graph. A cell is DETERMINED when the processes the caller calls are themselves
# admitted; the undetermined families are those processes' OWN refusals, named by
# `_matrix_undetermined` with the limit each rests on, and for them the claim is that nothing
# ships.

#: A bound path composed from X with a default, which does NOT discharge a read that becomes a
#: request path (`DdpPropertySourceV1`): the X-less document a caller stored reaches the path.
_Y_FROM_DEFAULTED_X = {"kind": "set_ddp", "name": "Y", "source_values": [
    {"value_type": "static", "value": "/clients/"},
    {"value_type": "ddp", "property_name": "X", "default_value": "0"}]}
_BOUND_Y_GET = {"kind": "connector_call", "operation_ref": "$ref:GET", "path_binding": {"property_name": "Y"}}

_MATRIX_USES = {
    # (the use, what satisfies it, what breaks it, the passthrough child that uses it, refusal
    #  code, what the retrieving process does before a passthrough call, the pointer suffix)
    "bound": ([_BOUND_GET], [_GET, _DYNAMIC_X], [_GET, _STATIC_X], _doc(_ENTRY, _BOUND_GET, _STOP),
              _NO_DYNAMIC_SEGMENT, [], "/process_ref"),
    "ordinary": ([_READS_X], [_GET, _DYNAMIC_X], [_GET], _doc(_ENTRY, _READS_X, _STOP), _READ_BEFORE_WRITE,
                 [], ""),
    "content": ([_CONSUMES_P2], [_GETP1, _TO_P2], [_GETP1], _doc(_ENTRY, _CONSUMES_P2, _STOP),
                PROCESS_IR_SEMANTIC_PROFILE_MISMATCH, [], "/process_ref"),
    # SOUND-21a-01: what the callers owe is the SOURCE property, an establishment row, so it is
    # refused as an unmet read at the call.
    "defaulted_writer": ([_Y_FROM_DEFAULTED_X, _BOUND_Y_GET], [_GET, _DYNAMIC_X], [_GET],
                         _doc(_ENTRY, _BOUND_Y_GET, _STOP), _READ_BEFORE_WRITE, [_Y_FROM_DEFAULTED_X], ""),
}
_MATRIX_CELLS = [
    (removal, form, step, site, fills, use)
    for removal in ("proved", "unproved", "shared_trigger", "none")
    for form in ("same", "recache", "recache_no_use")
    for step in ("none", "message")
    for site in ("own", "passthrough", "inherited")
    for fills in (True, False)
    for use in sorted(_MATRIX_USES)
]


#: How each use is refused INSIDE the process that makes it, which is what an undetermined
#: family measures. The call-side code in `_MATRIX_USES` is what a CALLER is refused with, and
#: for a path writer composing from a defaulted source the two differ.
_IN_CHILD_REFUSAL = {
    "bound": _NOT_ESTABLISHED,
    "ordinary": _READ_BEFORE_WRITE,
    "content": PROCESS_IR_SEMANTIC_PROFILE_MISMATCH,
    "defaulted_writer": _NOT_ESTABLISHED,
}


def _matrix_undetermined(removal, form, step, site, fills, use):
    """Why the processes the caller calls refuse the SATISFYING caller too, with the codes
    that refusal carries — or None where the cell is determined.

    The codes are part of the record (TI2-184-21a-02): a family that starts refusing for a
    different reason is a limit that moved, and the cell must say so rather than stay green
    because SOMETHING refused.

    - A profile consumer reached only by re-cached or Message-rebuilt documents: a cache write
      is not a consumer, so the source cache owes no content row and the re-cache proves no
      profile; a Message rebuilds the payload the map would consume.
    - A retrieve of the seeded cache after a removal nothing refilled: a read of a cache that may
      be empty (amendment 3 §8, `CACHE_WRITER_MISSING`).
    - A property used only behind a Message, with no use on the retrieve's own stream and no fill
      of the process's own to prove it: the fail-closed limit
      `test_a_bound_use_no_cohort_clears_keeps_the_childs_own_refusal` records. At the inherited
      site the grandchild never fills, so every Message cell there is in it.

    - A grandchild a middle calls behind a Split: the Split is native work in front of the call,
      and no attested hand-off admits it into a No Data child (the placement refusal). That is
      the inherited site's only shared-trigger spelling that parses, so it ships nothing; the
      passthrough site's call follows a retrieve and IS measured.
    """
    in_the_child = _IN_CHILD_REFUSAL[use]
    families = []
    if removal == "shared_trigger" and site == "inherited":
        # The Split is native work in front of the call (the placement refusal) AND it leaves
        # the count unknown, so a No Data grandchild requiring anything of a shared cache is
        # refused under amendment 1 rule 8 at the same pointer.
        families.append(("a call whose Branch a Split feeds, into a No Data child",
                         frozenset({PROCESS_IR_CAPABILITY_PROCESS_CALL_PLACEMENT_UNSUPPORTED,
                                    PROCESS_IR_CAPABILITY_PROCESS_CALL_REPEATED_RUN_UNSTABLE})))
    if use == "content" and (form.startswith("recache") or step == "message"):
        families.append(("a profile consumer of re-cached or rebuilt documents",
                         frozenset({PROCESS_IR_SEMANTIC_PROFILE_MISMATCH})))
    if form == "same" and not fills and removal != "none":
        # The retrieve finds a cache nothing filled, and what it hands on establishes the use
        # no better — so the family carries the empty-cache refusal and the use's own.
        families.append(("a read of a cache the removal may have emptied",
                         frozenset({_CACHE_WRITER_MISSING, in_the_child})))
    if step == "message" and (site == "inherited" or (form == "same" and not fills)):
        families.append(("a use behind a Message that no cohort seed clears", frozenset({in_the_child})))
    if not families:
        return None
    # A cell can sit in more than one recorded limit, and then it carries all of them: the
    # codes it may raise are their union, and dropping the ones it does not name first would
    # make the check depend on the order they are listed in.
    return (tuple(reason for reason, _codes in families),
            frozenset().union(*(codes for _reason, codes in families)))


def _matrix_roots(removal, form, step, site, fills, use):
    """``(callers, callees, the use leg, the callee legs before it, the refusal code)``."""
    uses, satisfies, breaks, passthrough_user, code, before_the_call, _suffix = _MATRIX_USES[use]
    ride_put, ride_read, ride_remove = (_PUT, _READ, _REMOVE) if form == "same" else (_PUT2, _READ2, _REMOVE2)
    before = {"proved": [{"steps": [], "terminal": ride_remove}],
              "unproved": [{"steps": [_GET], "terminal": ride_remove}],
              "shared_trigger": [{"steps": [], "terminal": ride_remove}],
              "none": []}[removal]
    if form == "recache":
        before = before + [{"steps": [_READ] + ([] if use == "content" else [_READS_X]), "terminal": _PUT2}]
    if form == "recache_no_use":
        # The same re-cache with NOTHING reading the property on the way: what the documents
        # carry is unproved, so the cache they came OUT of is the only thing that can carry the
        # obligation (`_cohort_at_write`'s origin alternative, correction batch 21a round 3).
        before = before + [{"steps": [_READ], "terminal": _PUT2}]
    if fills:
        before = before + [{"steps": list(satisfies), "terminal": ride_put}]
    message = [_MSG] if step == "message" else []
    use_leg = {"steps": [ride_read] + message + list(uses), "terminal": _STOP}

    def branch(*legs, calls=False):
        # A Branch needs two legs; a lone leg gets a Message leg beside it, which stores nothing.
        # A shared trigger is a step in front of the whole Branch: a GET where the process uses
        # the documents itself, a Split behind a passthrough entry where a leg ends in a call.
        legs = legs if len(legs) > 1 else legs + (_STORES_NOTHING,)
        body = {"kind": "branch", "legs": list(legs)}
        if removal != "shared_trigger":
            return _doc(body)
        return _doc(_ENTRY, _SPLITS_THE_DOCUMENTS, body) if calls else _doc(_GET, body)

    if site == "own":
        callees = [("CACHE_CHILD", branch(*(before + [use_leg])))]
    elif site == "passthrough":
        callees = [("CACHE_CHILD", branch(*(before + [
            {"steps": [ride_read] + message + list(before_the_call), "terminal": _call("BOUND")}]), calls=True)),
                   ("BOUND", passthrough_user)]
    else:
        callees = [("CACHE_CHILD", branch(*(before + [{"steps": [], "terminal": _call("CACHE_CHILD_P1")}]),
                                          calls=True)),
                   ("CACHE_CHILD_P1", _legs(use_leg, _STORES_NOTHING))]
    into_the_seeded_cache = {"steps": list(satisfies), "terminal": _PUT}
    callers = {
        "breaking": [into_the_seeded_cache, {"steps": list(breaks), "terminal": ride_put}],
        "satisfying": [into_the_seeded_cache] + (
            [{"steps": list(satisfies), "terminal": _PUT2}] if form == "recache" else []),
        "storing_nothing_there": [_STORES_NOTHING] if form == "same" else [into_the_seeded_cache],
    }
    return callers, callees, use_leg, before, code, branch


def _matrix_id(cell):
    removal, form, step, site, fills, use = cell
    return "-".join((removal, form, "msg" if step == "message" else "nostep", site,
                     "fills" if fills else "nofill", use))


@pytest.mark.parametrize("cell", _MATRIX_CELLS, ids=[_matrix_id(cell) for cell in _MATRIX_CELLS])
def test_every_cell_of_the_cache_obligation_case_set_answers_as_the_runtime_does(cell):
    """The coverage claim (see the section comment above): every cell, every caller, on the
    validate and the compile entry point, against the flattened graph of the same legs."""
    removal, form, _step, site, fills, use = cell
    callers, callees, use_leg, before, code, branch = _matrix_roots(*cell)
    suffix = _MATRIX_USES[use][6]
    undetermined = _matrix_undetermined(*cell)
    for caller, caller_legs in callers.items():
        parent = _legs(*(caller_legs + [{"steps": [], "terminal": _call("CACHE_CHILD")}]))
        roots = [("PARENT", parent)] + callees
        callee_verdicts = {key: _errors(roots, key) for key, _document in callees}
        if undetermined is not None:
            reasons, expected_codes = undetermined
            raised = {found for verdict in callee_verdicts.values() for found, _path in verdict}
            assert raised and raised <= expected_codes, (reasons, caller, callee_verdicts)
            continue
        assert callee_verdicts == {key: [] for key, _document in callees}, (caller, callee_verdicts)
        call = "/body/steps/0/legs/{0}/terminal".format(len(caller_legs))
        if caller == "breaking" and removal in ("unproved", "none"):
            refused = True
            expected = [(code, call + suffix)]
        elif caller == "storing_nothing_there" and form == "same" and not fills:
            # The callee reads the cache before anything of its own fills it, so a caller that
            # stores nothing there leaves it empty: a read before any write, at the call.
            refused = True
            expected = [(_CACHE_WRITER_MISSING, call)]
        else:
            refused = False
            expected = []
        assert _both_routes(roots, "PARENT") == expected, caller
        # The flattened graph: a shared trigger is the GET spelling there, which parses in a process
        # that uses the documents itself.
        twin = _errors([("PARENT", branch(*(caller_legs + before + [use_leg])))], "PARENT")
        assert bool(twin) == refused, (caller, twin)


def test_the_case_set_is_the_whole_product_and_every_axis_value_is_determined_somewhere():
    """Non-vacuity of the claim itself: the parametrized set is the full product of its axes,
    every undetermined family is inhabited, and what they leave still spans every value of every
    axis — so no axis value is covered only by the "nothing ships" half."""
    axes = (("proved", "unproved", "shared_trigger", "none"), ("same", "recache", "recache_no_use"),
            ("none", "message"),
            ("own", "passthrough", "inherited"), (True, False), tuple(sorted(_MATRIX_USES)))
    product = 1
    for values in axes:
        product *= len(values)
    assert len(_MATRIX_CELLS) == product == len(set(_MATRIX_CELLS))
    recorded = [_matrix_undetermined(*cell) for cell in _MATRIX_CELLS]
    families = {reason for entry in recorded if entry is not None for reason in entry[0]}
    assert len(families) == 4, families
    # Every recorded limit names the codes it is a limit OF (TI2-184-21a-02), and every one of
    # the four is inhabited on its own, not only in combination with another.
    assert all(entry[1] for entry in recorded if entry is not None)
    assert {entry[0][0] for entry in recorded if entry is not None and len(entry[0]) == 1} == families
    determined = [cell for cell in _MATRIX_CELLS if _matrix_undetermined(*cell) is None]
    # The shared trigger is measured at a CALL site, not only in the process that uses the
    # documents: the passthrough discharge's cells are determined.
    assert any(cell[0] == "shared_trigger" and cell[3] == "passthrough" for cell in determined)
    for axis, values in enumerate(axes):
        assert {cell[axis] for cell in determined} == set(values), axis


# ---------------------------------------------------------------------------
# Correction batch 21a round 2: the verification's findings, each with its witness
# ---------------------------------------------------------------------------

_WAITS_AND_ABORTS_ON_ERROR = {"wait": True, "abort_on_error": True}


def _waited(key, **extra):
    return {"steps": [], "terminal": _call(key, **dict(_WAITS_AND_ABORTS_ON_ERROR, **extra))}


def _lineage_with_source(monkeypatch, old, new, occurrences=1):
    """Run every walk through `lineage.py` with ONE source edit, and nothing else changed.

    A source mutant, for a rule that lives inside the walk's own closures and so has no
    module-level name to replace: the module's source is executed afresh with ``old`` replaced by
    ``new``, and only `_walk_lineage` — which `walk_lineage` and `collect_lineage_findings` both
    call through the module — is rebound to the mutant's."""
    source = Path(lineage.__file__).read_text(encoding="utf-8")
    assert source.count(old) == occurrences, (old, source.count(old))
    namespace = {"__name__": lineage.__name__, "__package__": lineage.__package__, "__file__": lineage.__file__}
    exec(compile(source.replace(old, new), lineage.__file__, "exec"), namespace)  # noqa: S102
    monkeypatch.setattr(lineage, "_walk_lineage", namespace["_walk_lineage"])


def _source_mutants():
    """Every source mutant this module applies, read off its own call sites.

    Machine-readable because the call sites are: `_lineage_with_source(monkeypatch, old, new)`
    takes the two source strings as literals, so the pairs can be collected without a second
    list anyone has to keep in step. A call site whose strings are not literals fails the test
    below rather than being skipped — a mutant this sweep cannot see is a mutant nothing
    checks (B21A-R4-REV-01).
    """
    import ast

    found = []
    tree = ast.parse(Path(__file__).read_text(encoding="utf-8"))
    for node in ast.walk(tree):
        if not (isinstance(node, ast.Call) and isinstance(node.func, ast.Name)
                and node.func.id == "_lineage_with_source"):
            continue
        occurrences = next(
            (keyword.value.value for keyword in node.keywords
             if keyword.arg == "occurrences" and isinstance(keyword.value, ast.Constant)), 1)
        pair = tuple(argument.value if isinstance(argument, ast.Constant) else None
                     for argument in node.args[1:3])
        found.append((node.lineno, pair, occurrences))
    return sorted(found)


@pytest.mark.parametrize("mutant", _source_mutants(), ids=lambda mutant: "line%d" % mutant[0])
def test_every_source_mutant_this_module_applies_moves_the_served_revision(mutant, monkeypatch):
    """B21A-R4-REV-01. Each of these mutants is a claim that some rule inside the walk's
    closures is load-bearing. A mutant that changed nothing the served compiler revision
    records would be a claim the revision cannot keep: the rule could then be reverted with
    the served payload standing still, which is the property `test_issue_184_revision_coverage`
    exists to give for module-level decisions and this test gives for the closures.

    The pairs are read off this module's own call sites, so a mutant added later is measured
    the moment it is written.

    WHAT CARRIES THE CLAIM, measured rather than assumed. The revision's hand-built oracles
    reach six of these twelve mutants; the other six change no verdict any of them records,
    because the rules they perturb only speak on graphs no oracle builds. Batch 21b's
    behaviour corpus DOES record those graphs — it harvests the compiler's real inputs from
    this very suite — and with it in the payload all twelve move (measured on a merged copy of
    both batches with the corpus regenerated: 12 of 12, against 6 of 12 on this batch's tree
    alone; the counts are kept in `b21a/round6/{m21a_r6,mmerged_r6}.json`). So the strict claim
    is asserted wherever the corpus row is in the payload, which is
    where it is true; without it the mutant is still applied and the payload still has to
    build, and the six are covered by the witness tests they sit in."""
    from boomi_mcp.authoring import contract as authoring_contract

    line, (old, new), occurrences = mutant
    assert old is not None and new is not None, ("not a literal pair", line)
    baseline = authoring_contract._compiler_revision()
    assert authoring_contract.sha256_fingerprint(
        authoring_contract._compiler_revision_payload()) == baseline
    # Applied through a REFERENCE, not a call of the name: the sweep above matches calls, and
    # this one is the driver rather than a mutant claim of its own.
    apply_the_mutant = _lineage_with_source
    with monkeypatch.context() as patched:
        apply_the_mutant(patched, old, new, occurrences=occurrences)
        payload = authoring_contract._compiler_revision_payload()
    assert sorted(row for row, value in payload.items() if value == "unavailable") == [], line
    # BOOTSTRAP SEAM, and only over this one branch. The corpus that makes the strict claim
    # true is produced by a harvest pass that RUNS this test, and the producer refuses to
    # write unless the covered tests are clean — so against a stale corpus the claim and its
    # own evidence deadlock, and nobody following the documented command can regenerate it
    # (V5-05, measured on a merged copy). The harvest pass sets `REVISION_CORPUS_BOOTSTRAP`;
    # everything else here — the mutant applying, the payload building, no row going
    # unavailable, the served revision standing still — is asserted in that pass too.
    import os

    bootstrapping = bool(os.environ.get("REVISION_CORPUS_BOOTSTRAP"))
    if "behaviour_corpus" in payload and not bootstrapping:
        assert authoring_contract.sha256_fingerprint(payload) != baseline, (line, old)
    assert authoring_contract._compiler_revision() == baseline


# --- SOUND-21a-01: a path writer's defaulted DDP source ------------------------------------

#: What each child does with the documents it retrieves, and the cache they come from:
#: ``(the child's legs, the cache the caller's documents share)``. Every use composes a bound
#: request path from X with a default, which does NOT discharge a read that becomes a request
#: path (`DdpPropertySourceV1`).
_DEFAULTED_SOURCE_CHILDREN = {
    "composes_y_from_x": ([_DYNAMIC_INTO_CACHE2, {"steps": [_READ2, _Y_FROM_DEFAULTED_X, _BOUND_Y_GET],
                                                  "terminal": _STOP}], "CACHE2"),
    "past_a_message": ([_DYNAMIC_INTO_CACHE2, {"steps": [_READ2, _MSG, _Y_FROM_DEFAULTED_X, _BOUND_Y_GET],
                                               "terminal": _STOP}], "CACHE2"),
    "recomposes_x_from_x": ([_DYNAMIC_INTO_CACHE2, {"steps": [_READ2, {
        "kind": "set_ddp", "name": "X", "source_values": [
            {"value_type": "static", "value": "/clients/"},
            {"value_type": "ddp", "property_name": "X", "default_value": "0"}]}, _BOUND_GET],
        "terminal": _STOP}], "CACHE2"),
    # The passthrough call-discharge site: the binding lives in the passthrough child.
    "a_passthrough_child_binds": ([_DYNAMIC_INTO_CACHE2, {"steps": [_READ2, _Y_FROM_DEFAULTED_X],
                                                          "terminal": _call("BOUND_XY")}], "CACHE2"),
    "the_same_cache": ([_STAGES_X, {"steps": [_READ, _Y_FROM_DEFAULTED_X, _BOUND_Y_GET], "terminal": _STOP}],
                       "CACHE"),
    # No fill of its own: the over-refusal the same missing row caused (p4 cell d8).
    "no_fill_of_its_own": ([{"steps": [_READ, _Y_FROM_DEFAULTED_X, _BOUND_Y_GET], "terminal": _STOP},
                            _STORES_NOTHING], "CACHE"),
}
_DEFAULTED_SOURCE_CALLERS = ("x_less", "dynamic_x", "stores_nothing")


def _defaulted_source_roots(child, caller):
    legs, shared = _DEFAULTED_SOURCE_CHILDREN[child]
    put = _PUT if shared == "CACHE" else _PUT2
    other = _DYNAMIC_INTO_CACHE2 if shared == "CACHE" else _STAGES_X
    caller_legs = {
        "x_less": [other, {"steps": [_GET], "terminal": put}],
        "dynamic_x": [other, {"steps": [_GET, _DYNAMIC_X], "terminal": put}],
        "stores_nothing": [other, _STORES_NOTHING],
    }[caller]
    roots = [("PARENT", _legs(*(caller_legs + [_waited("CACHE_CHILD")]))), ("CACHE_CHILD", _legs(*legs)),
             ("BOUND_XY", _doc(_ENTRY, _BOUND_Y_GET, _STOP))]
    flat = [({"steps": leg["steps"] + [_BOUND_Y_GET], "terminal": _STOP}
             if leg["terminal"].get("kind") == "process_call" else leg) for leg in legs]
    return roots, _legs(*(caller_legs + flat))


@pytest.mark.parametrize("caller", _DEFAULTED_SOURCE_CALLERS)
@pytest.mark.parametrize("child", sorted(_DEFAULTED_SOURCE_CHILDREN))
def test_a_path_writer_owes_the_callers_that_share_its_cache_the_source_it_composes_from(child, caller):
    """SOUND-21a-01, a sibling of finding O the batch's sweep missed. A child that fills CACHE2
    itself, retrieves it and composes a bound request path from X WITH A DEFAULT admitted a
    caller whose X-less documents shared CACHE2: the default composed the path from "0", and the
    GET went to another resource, while the flattened graph was refused. The published
    `DdpPropertySourceV1` says a default does NOT discharge a read that becomes a request path,
    and amendment 3 §7: "A bound path must pass for every possible selected writer."

    The obligation is recorded where the in-process refusal lives — at the writer, per SOURCE
    property, default or not — so the child's contract owes each caller X on what it stored in
    the shared cache: an X-less caller is refused at its call, one storing X or storing nothing
    there is admitted, and the flattened graph agrees in every cell. The same recording clears
    the mirrored over-refusal (p4 cell d8): a child with no fill of its own was refused for every
    caller, because its contract named nothing a caller could satisfy."""
    roots, flat = _defaulted_source_roots(child, caller)
    call = "/body/steps/0/legs/2/terminal"
    if caller == "x_less":
        expected = [(_READ_BEFORE_WRITE, call)]
    elif caller == "stores_nothing" and child == "no_fill_of_its_own":
        expected = [(_CACHE_WRITER_MISSING, call)]
    else:
        expected = []
    assert _both_routes(roots, "PARENT") == expected
    assert _both_routes(roots, "CACHE_CHILD") == []
    shared = _DEFAULTED_SOURCE_CHILDREN[child][1]
    assert ("$ref:" + shared, "X", None, False) in _row(roots, "PARENT", "CACHE_CHILD").cache_property_requirements
    assert bool(_errors([("PARENT", flat)], "PARENT")) == bool(expected)


def test_the_per_source_writer_capture_is_load_bearing(monkeypatch):
    """Non-vacuity, one mutant per half, each a source edit of the writer capture.

    - The capture restored to the `current` recomposition only: the X-less caller of the child
      that composes Y from a defaulted X is admitted again, while the flattened graph is refused.
    - A writer's unmet source recorded nowhere: the child with no fill of its own is refused for
      the caller storing X again (the over-refusal)."""
    x_less, x_less_flat = _defaulted_source_roots("composes_y_from_x", "x_less")
    dynamic, _flat = _defaulted_source_roots("no_fill_of_its_own", "dynamic_x")
    assert _errors(x_less, "PARENT") == [(_READ_BEFORE_WRITE, "/body/steps/0/legs/2/terminal")]
    assert _errors(dynamic, "PARENT") == [] and _errors(dynamic, "CACHE_CHILD") == []
    with monkeypatch.context() as patched:
        _lineage_with_source(
            patched,
            "                sources = {read_key for read_key, _has_default, _strict in _reads_of(semantic)\n"
            "                           if read_key[0] == DDP}\n"
            "                if recomposes:\n"
            "                    sources.add(key)\n",
            "                sources = {key} if recomposes and key in on_documents else set()\n")
        assert _errors(x_less, "PARENT") == []
        assert _errors([("PARENT", x_less_flat)], "PARENT") != []
    with monkeypatch.context() as patched:
        _lineage_with_source(
            patched,
            "            for source_name, cache_ref, ridden, origins in unmet_origins:\n",
            "            for source_name, cache_ref, ridden, origins in ():\n")
        assert _errors(dynamic, "CACHE_CHILD") == [
            (_NOT_ESTABLISHED, "/body/steps/0/legs/0/steps/2/path_binding")]


# --- B21A-OVR-02: a run that leaves nothing it stored is repetition-stable -----------------

#: Children a No Data caller's per-document call runs once per document, each filling CACHE2,
#: using what it retrieves and emptying CACHE2 again on a leg the walk proves runs.
_CLEANS_UP_EVERY_RUN = {
    "bound": _legs(_DYNAMIC_INTO_CACHE2, _BINDS2, _EMPTIES2),
    "ordinary": _legs(_DYNAMIC_INTO_CACHE2, _READS2, _EMPTIES2),
    "content": _legs({"steps": [_GETP1, _TO_P2], "terminal": _PUT2},
                     {"steps": [_READ2, _CONSUMES_P2], "terminal": _STOP}, _EMPTIES2),
    "a_message_before_the_removal": _legs(_DYNAMIC_INTO_CACHE2, _BINDS2, {"steps": [_MSG], "terminal": _REMOVE2}),
    "a_shared_trigger": _doc(_GET, {"kind": "branch", "legs": [
        {"steps": [_DYNAMIC_X], "terminal": _PUT2}, _BINDS2, _EMPTIES2]}),
    "two_cycles": _legs(_DYNAMIC_INTO_CACHE2, _BINDS2, _EMPTIES2, _DYNAMIC_INTO_CACHE2, _BINDS2, _EMPTIES2),
}
_PER_DOCUMENT_CALL = "/body/steps/1/legs/1/terminal"


def _per_document(child, **extra):
    return [("PARENT", _passthrough_root(_STORES_NOTHING, {"steps": [], "terminal": _call("CACHE_CHILD", **extra)})),
            ("CACHE_CHILD", child)]


@pytest.mark.parametrize("child", sorted(_CLEANS_UP_EVERY_RUN))
def test_a_run_that_leaves_nothing_it_stored_in_the_cache_is_stable_under_repetition(child):
    """B21A-OVR-02. Amendment 1 rule 8 says to "analyze possible subsequent invocations using the
    same finite state/profile lattice until stable", and amendment 3 repeats it ("Repeated No Data
    invocation checks use the same finite lattice until stable"). A child that fills CACHE2, uses
    what it retrieves and then empties CACHE2 on a leg the walk proves runs leaves nothing it
    stored at the end of any run, so every later run's uses retrieve only what the callers stored
    — which the call proves — or nothing: stable after one step. 5b5038c admitted it; the first
    handback refused it, because the child's own fill no longer ended its callers' obligation.

    Admitted for a call that waits and aborts on error, on both entry points; the contract says
    why (`required_caches_retain_nothing_it_stored`, read off the walk's exit)."""
    roots = _per_document(_CLEANS_UP_EVERY_RUN[child], **_WAITS_AND_ABORTS_ON_ERROR)
    assert _both_routes(roots, "PARENT") == []
    assert _both_routes(roots, "CACHE_CHILD") == []
    row = _row(roots, "PARENT", "CACHE_CHILD")
    assert row.required_caches_retain_nothing_it_stored is True
    assert any(requirement[0] == "$ref:CACHE2" for requirement in
               row.cache_property_requirements + row.cache_requirements), row
    # A scheduled caller is the NEGATIVE control, and it is one for the exemption too: its bare
    # Branch leg carries the proved singleton, so rule 8 is not entered at all there and the same
    # cell is clean for a child the exemption does NOT cover (R8-TI-01 — asserted here as the
    # gate it is, rather than as an exemption witness it cannot be).
    for scheduled_child in (_CLEANS_UP_EVERY_RUN[child], _legs(_DYNAMIC_INTO_CACHE2, _BINDS2)):
        scheduled = [("PARENT", _legs(_STORES_NOTHING, _waited("CACHE_CHILD"))),
                     ("CACHE_CHILD", scheduled_child)]
        assert _both_routes(scheduled, "PARENT") == []


def test_a_per_document_run_that_may_leave_what_it_stored_stays_refused():
    """The controls, each the reason a later run may find what an earlier run added or emptied.

    - Nothing removes CACHE2, or a removal the walk does not prove runs (behind its own GET), or a
      refill after the removal: a run may end with what it stored still there.
    - The cleanup child called without abort_on_error: amendment 1 rule 7, "An asynchronous call
      or a call allowed to continue after child failure must not establish normal-exit
      guarantees" — a run that fails between its fill and its removal leaves the fill behind, and
      the next run starts from it.
    - A child that READS CACHE2 before writing it and then empties it: the next run finds the
      cache the first emptied, so the establishment the call proved is gone.
    - A forwarder between the per-document caller and the cleanup child: the forwarder's own run
      meets its child's possible writes as unknown possibilities (amendment 3 §7, "Possible
      opaque/external/child writes introduce unknown possibilities"), so its exit may hold them.
    And the admitted control: a child that empties CACHE2 first owes its callers nothing there."""
    refused = [(PROCESS_IR_CAPABILITY_PROCESS_CALL_REPEATED_RUN_UNSTABLE, _PER_DOCUMENT_CALL)]
    fill_use = _legs(_DYNAMIC_INTO_CACHE2, _BINDS2)
    for child in (fill_use,
                  _legs(_DYNAMIC_INTO_CACHE2, _BINDS2, _EMPTIES2_BEHIND_A_GET),
                  _legs(_DYNAMIC_INTO_CACHE2, _BINDS2, _EMPTIES2, _DYNAMIC_INTO_CACHE2)):
        roots = _per_document(child, **_WAITS_AND_ABORTS_ON_ERROR)
        assert _both_routes(roots, "PARENT") == refused, child
        assert _row(roots, "PARENT", "CACHE_CHILD").required_caches_retain_nothing_it_stored is False
    assert _both_routes(_per_document(_CLEANS_UP_EVERY_RUN["bound"], wait=True), "PARENT") == refused
    reads_then_empties = [
        ("PARENT", _passthrough_root(_DYNAMIC_INTO_CACHE2, {"steps": [], "terminal": _call(
            "CACHE_CHILD", **_WAITS_AND_ABORTS_ON_ERROR)})), ("CACHE_CHILD", _legs(_BINDS2, _EMPTIES2))]
    assert _both_routes(reads_then_empties, "PARENT") == refused
    via_a_forwarder = [
        ("PARENT", _passthrough_root(_STORES_NOTHING, {"steps": [], "terminal": _call(
            "MID", **_WAITS_AND_ABORTS_ON_ERROR)})),
        ("MID", _legs(_STORES_NOTHING, _waited("CACHE_CHILD"))), ("CACHE_CHILD", _CLEANS_UP_EVERY_RUN["bound"])]
    assert _both_routes(via_a_forwarder, "PARENT") == refused
    assert _both_routes(_per_document(_legs(_EMPTIES2, _DYNAMIC_INTO_CACHE2, _BINDS2)), "PARENT") == []


def test_the_exit_state_and_the_abort_gate_decide_the_repeated_run_exemption(monkeypatch):
    """Non-vacuity in both directions, and of the gate. The walk's exit MAY set decides: forced to
    name CACHE2, the cleanup child is refused again; forced empty, the child whose removal the walk
    does not prove is admitted. And the abort gate decides: with every call treated as waiting and
    aborting on error, the cleanup child called without abort_on_error is admitted."""
    import types

    cleans_up = _per_document(_CLEANS_UP_EVERY_RUN["bound"], **_WAITS_AND_ABORTS_ON_ERROR)
    unproved = _per_document(_legs(_DYNAMIC_INTO_CACHE2, _BINDS2, _EMPTIES2_BEHIND_A_GET), **_WAITS_AND_ABORTS_ON_ERROR)
    no_abort = _per_document(_CLEANS_UP_EVERY_RUN["bound"], wait=True)
    refused = [(PROCESS_IR_CAPABILITY_PROCESS_CALL_REPEATED_RUN_UNSTABLE, _PER_DOCUMENT_CALL)]
    assert (_errors(cleans_up, "PARENT"), _errors(unproved, "PARENT"), _errors(no_abort, "PARENT")) == ([], refused, refused)
    real_walk = lineage.walk_lineage
    for forced, roots, expected in ((("$ref:CACHE2",), cleans_up, refused), ((), unproved, [])):
        with monkeypatch.context() as patched:
            patched.setattr(lineage, "walk_lineage", lambda prepared, capabilities=DEFAULT_VALIDATION_CAPABILITIES,
                            forced=forced: real_walk(prepared, capabilities)._replace(may_hold_at_exit=forced))
            assert _errors(roots, "PARENT") == expected, forced
    real_rule = lineage._repetition_unstable_caches
    with monkeypatch.context() as patched:
        patched.setattr(lineage, "_repetition_unstable_caches", lambda contract, cache_refs, semantic: real_rule(
            contract, cache_refs, types.SimpleNamespace(wait=True, abort_on_error=True)))
        assert _errors(no_abort, "PARENT") == []


# --- B21A-OVR-01 and SOUND-21a-02: a child's possible writes are unknown possibilities -------

_SELF_FILLING_USES = {
    # (the fill, the use after the retrieve, the refusal at the second call)
    "bound": (_DYNAMIC_INTO_CACHE2, [_BOUND_GET], _NOT_ESTABLISHED, "/process_ref"),
    "ordinary": (_DYNAMIC_INTO_CACHE2, [_READS_X], _READ_BEFORE_WRITE, ""),
    "content": ({"steps": [_GETP1, _TO_P2], "terminal": _PUT2}, [_CONSUMES_P2], PROCESS_IR_SEMANTIC_PROFILE_MISMATCH,
                "/process_ref"),
}


@pytest.mark.parametrize("use", sorted(_SELF_FILLING_USES))
def test_a_later_waited_call_meets_what_an_earlier_child_may_have_stored_as_an_unknown_possibility(use):
    """B21A-OVR-01, a DELIBERATE refusal: an admission of 5b5038c (masked there by finding O) that
    this batch turns into a refusal, listed and pinned here so the verdict is a decision.

    Two waited calls into a child that fills CACHE2 and uses what it retrieves; the same with a
    WRITER child that fills CACHE2 first; and the same with the parent filling CACHE2 between the
    two calls. The second call is refused, while the flattened graph of the same legs is admitted:
    at runtime the second run retrieves the first run's documents, and they carry what the child's
    own writer composed. The reason is the plan's, not the runtime's: amendment 3 §7, "Whole-cache
    removal clears these summaries. Possible opaque/external/child writes introduce unknown
    possibilities" (ledger C20: every call adds unknown content and an unknown property cohort to
    the caches its child may write). A call does not carry what its child stored into the caller's
    cache state, and this batch does not build that transfer. Since finding O the child owes its
    callers what they stored in CACHE2, and the first call's unknown possibility is such a
    document, so the second call cannot prove the row.

    The admitted way out: the child empties CACHE2 first, on a leg the walk proves runs, so no
    earlier call's document reaches its use."""
    fill, after, code, suffix = _SELF_FILLING_USES[use]
    use_leg = {"steps": [_READ2] + after, "terminal": _STOP}
    child = _legs(fill, use_leg)
    twice = [("PARENT", _legs(_waited("CACHE_CHILD"), _waited("CACHE_CHILD"))), ("CACHE_CHILD", child)]
    assert _both_routes(twice, "PARENT") == [(code, "/body/steps/0/legs/1/terminal" + suffix)]
    assert _both_routes(twice, "CACHE_CHILD") == []
    assert _errors([("PARENT", _legs(fill, use_leg, fill, use_leg))], "PARENT") == []
    writer_first = [("PARENT", _legs(_waited("WRITER"), _waited("CACHE_CHILD"))),
                    ("WRITER", _legs(fill, _STORES_NOTHING)), ("CACHE_CHILD", child)]
    assert _both_routes(writer_first, "PARENT") == [(code, "/body/steps/0/legs/1/terminal" + suffix)]
    parent_fills_between = [("PARENT", _legs(_waited("CACHE_CHILD"), fill, _waited("CACHE_CHILD"))),
                            ("CACHE_CHILD", child)]
    assert _both_routes(parent_fills_between, "PARENT") == [(code, "/body/steps/0/legs/2/terminal" + suffix)]
    empties_first = _legs(_EMPTIES2, fill, use_leg)
    for roots in ([("PARENT", _legs(_waited("CACHE_CHILD"), _waited("CACHE_CHILD"))), ("CACHE_CHILD", empties_first)],
                  [("PARENT", _legs(_waited("WRITER"), _waited("CACHE_CHILD"))),
                   ("WRITER", _legs(fill, _STORES_NOTHING)), ("CACHE_CHILD", empties_first)]):
        assert _both_routes(roots, "PARENT") == []


@pytest.mark.parametrize("caller", ("stores_the_profile", "stores_nothing"))
def test_a_middle_that_calls_a_cache_writer_before_a_self_filling_consumer_meets_an_unknown_possibility(caller):
    """SOUND-21a-02, a DELIBERATE refusal, listed and pinned like the one above. A middle calls a
    WRITER child that stores P2 in CACHE, then a child that stores P2 in CACHE itself and consumes
    what it retrieves. The middle is refused PROFILE_MISMATCH at its second call while the
    flattened graph is admitted: the writer's content crosses the call as unknown (amendment 3
    §7, "Possible opaque/external/child writes introduce unknown possibilities"), and unknown
    content cannot satisfy a typed consumer (amendment 3, A3). The consumer's own fill no longer
    ends the requirement, so the middle's check meets that unknown content.

    The admitted ways out: the middle without the writer call, and a consumer that empties CACHE
    first on a leg the walk proves runs."""
    stores_p2 = {"steps": [_GETP1, _TO_P2], "terminal": _PUT}
    consumes = {"steps": [_READ, _CONSUMES_P2], "terminal": _STOP}
    caller_leg = stores_p2 if caller == "stores_the_profile" else _STORES_NOTHING
    roots = [("PARENT", _legs(caller_leg, _waited("MID"))),
             ("MID", _legs(_waited("WRITER"), _waited("CACHE_CHILD"))),
             ("WRITER", _legs(stores_p2, _STORES_NOTHING)), ("CACHE_CHILD", _legs(stores_p2, consumes))]
    assert _both_routes(roots, "MID") == [(PROCESS_IR_SEMANTIC_PROFILE_MISMATCH, "/body/steps/0/legs/1/terminal/process_ref")]
    assert _both_routes(roots, "CACHE_CHILD") == []
    assert _errors([("PARENT", _legs(caller_leg, stores_p2, _STORES_NOTHING, stores_p2, consumes))], "PARENT") == []
    without_the_writer = [("PARENT", _legs(caller_leg, _waited("MID"))),
                          ("MID", _legs(_STORES_NOTHING, _waited("CACHE_CHILD"))),
                          ("CACHE_CHILD", _legs(stores_p2, consumes))]
    empties_first = roots[:3] + [("CACHE_CHILD", _legs(_EMPTIES_IT, stores_p2, consumes))]
    for admitted in (without_the_writer, empties_first):
        assert _both_routes(admitted, "PARENT") == []
        assert _both_routes(admitted, "MID") == []


# --- TI-184-21a-01: the shared trigger at the call sites ---------------------------------------

def test_the_shared_trigger_spellings_are_what_each_site_can_author():
    """What the coverage matrix's shared-trigger cells rest on, measured. A connector call in front
    of a Branch whose leg ends in a process call does not parse
    (`PROCESS_IR_CAPABILITY_NODE_NOT_ALLOWED_IN_BODY`); a Split behind a passthrough entry does.
    At the passthrough call-discharge site that spelling is admitted, so its cells are measured.
    At the inherited site the middle's call has only the Split in front of it, and the Split is a
    native step prefix no attested hand-off admits into the No Data grandchild, so the middle is
    refused under the placement code: that site's shared-trigger cells ship nothing."""
    from boomi_mcp.models.process_ir import ProcessIRValidationError

    def removal_then(call_leg):
        return {"kind": "branch", "legs": [_EMPTIES2, _RECACHE_LEG, call_leg]}

    with pytest.raises(ProcessIRValidationError) as caught:
        parse_process_ir_v1(_doc(_GET, removal_then({"steps": [_READ2], "terminal": _call("BOUND")})))
    assert "PROCESS_IR_CAPABILITY_NODE_NOT_ALLOWED_IN_BODY" in str(caught.value)
    caller = _legs(_STAGES_X, _LITERAL_INTO_CACHE2, {"steps": [], "terminal": _call("MID")})
    discharge = [("PARENT", caller),
                 ("MID", _doc(_ENTRY, _SPLITS_THE_DOCUMENTS, removal_then({"steps": [_READ2], "terminal": _call("BOUND")}))),
                 ("BOUND", _BOUND)]
    assert _both_routes(discharge, "MID") == []
    assert _both_routes(discharge, "PARENT") == []
    inherited = [("PARENT", caller),
                 ("MID", _doc(_ENTRY, _SPLITS_THE_DOCUMENTS, removal_then({"steps": [], "terminal": _call("CACHE_CHILD")}))),
                 ("CACHE_CHILD", _legs(_BINDS2, _STORES_NOTHING))]
    assert _both_routes(inherited, "MID") == [
        (PROCESS_IR_CAPABILITY_PROCESS_CALL_PLACEMENT_UNSUPPORTED, "/body/steps/2/legs/2/terminal")]


def test_the_relative_removal_proof_decides_the_passthrough_call_site_too(monkeypatch):
    """Non-vacuity of the matrix's shared-trigger cells at the passthrough call-discharge site:
    with the removal's proof reduced to the whole-run proof, the literal segment the caller stored
    in CACHE2 is read as possibly surviving the Split-fed removal, and the caller is refused at its
    call — the cell's own verdict, flipped."""
    cell = ("shared_trigger", "recache", "none", "passthrough", True, "bound")
    callers, callees, _use_leg, _before, code, _branch = _matrix_roots(*cell)
    roots = [("PARENT", _legs(*(callers["breaking"] + [{"steps": [], "terminal": _call("CACHE_CHILD")}])))] + callees
    assert _both_routes(roots, "PARENT") == []
    monkeypatch.setattr(lineage, "_removal_runs_before_anything_after_it",
                        lambda stream: lineage._path_provably_runs(stream))
    assert _both_routes(roots, "PARENT") == [(code, "/body/steps/0/legs/2/terminal/process_ref")]


# --- the entry requirement of a passthrough process that reads what it retrieved --------------

_READS_WHAT_IT_RETRIEVED = {
    "its_own_read": {"steps": [_READ2, _READS_X], "terminal": _STOP},
    "a_passthrough_child_reads": {"steps": [_READ2], "terminal": _call("READS_X")},
}


@pytest.mark.parametrize("trigger", ("a_passthrough_entry", "a_split"))
@pytest.mark.parametrize("use", sorted(_READS_WHAT_IT_RETRIEVED))
def test_a_passthrough_process_asks_its_callers_nothing_of_their_documents_for_what_it_retrieved(use, trigger):
    """Found by building the matrix's call-site shared-trigger cells from the Split spelling
    (TI-184-21a-01), and a sibling of the batch's own authority sweep: every channel that turns a
    read into an obligation of the callers must ask whether a caller's document can reach it.

    A Data Passthrough process empties CACHE2 on a leg the walk proves runs, re-caches what its
    caller stored in CACHE (reading X there) into CACHE2, and reads X on what it retrieves from
    CACHE2. The walk records that read unestablished until a caller cohort is seeded, and it used
    to record it as an ENTRY requirement too: every caller was asked to establish X on the
    documents it hands over — documents the retrieve replaced, which never reach the read — and
    was refused (`PROCESS_IR_SEMANTIC_LINEAGE_DDP_SCOPE_INVALID`) while the flattened graph ran.
    Measured at 5b5038c too. Past a retrieve on its path a document property read is a read of the
    retrieved documents; the cached-property row is what charges a caller, and it still does."""
    entry = [_ENTRY, _SPLITS_THE_DOCUMENTS] if trigger == "a_split" else [_ENTRY]
    mid = _doc(*(entry + [{"kind": "branch", "legs": [_EMPTIES2, _RECACHE_LEG, _READS_WHAT_IT_RETRIEVED[use]]}]))
    callers = {
        "stores_x": [_STAGES_X],
        "stores_x_less_documents_in_the_emptied_cache": [_STAGES_X, _X_LESS_INTO_CACHE2],
        "stores_x_less_documents_in_the_cache_it_re_caches": [{"steps": [_GET], "terminal": _PUT}],
    }
    for caller, caller_legs in callers.items():
        roots = [("PARENT", _legs(*(caller_legs + [{"steps": [], "terminal": _call("MID")}]))), ("MID", mid),
                 ("READS_X", _doc(_ENTRY, _READS_X, _STOP))]
        breaks = caller.endswith("the_cache_it_re_caches")
        expected = [(_READ_BEFORE_WRITE, "/body/steps/0/legs/{0}/terminal".format(len(caller_legs)))] if breaks else []
        assert _both_routes(roots, "PARENT") == expected, caller
        assert _both_routes(roots, "MID") == []
        assert ("ddp", "X") not in _row(roots, "PARENT", "MID").required_reads


def test_a_read_of_retrieved_documents_is_no_entry_requirement_by_that_rule_alone(monkeypatch):
    """Non-vacuity: with the rule removed from `_classify_unmet_read`, the caller storing X is
    asked for X on its own documents again and refused."""
    mid = _doc(_ENTRY, {"kind": "branch", "legs": [_EMPTIES2, _RECACHE_LEG, _READS_WHAT_IT_RETRIEVED["its_own_read"]]})
    roots = [("PARENT", _legs(_STAGES_X, {"steps": [], "terminal": _call("MID")})), ("MID", mid)]
    assert _errors(roots, "PARENT") == []
    _lineage_with_source(monkeypatch, "        elif not externally_satisfied and upward and not of_retrieved_documents:\n",
                         "        elif not externally_satisfied and upward:\n")
    assert ("ddp", "X") in _row(roots, "PARENT", "MID").required_reads
    assert _errors(roots, "PARENT") != []


# ---------------------------------------------------------------------------
# Correction batch 21a round 3: verification round 2's findings
# ---------------------------------------------------------------------------

# --- R8-SOUND-01: rule 8 asks the child's own vocabulary ----------------------------------

def test_rule_eight_asks_the_childs_own_vocabulary_when_its_cache_writes_are_unknown(monkeypatch):
    """R8-SOUND-01. `_caches_a_call_may_write` answers a child whose cache writes are unknown
    with the CALLER's observable caches, which is the right answer for what a call leaves behind
    for the caller's own later steps and the wrong one for amendment 1 rule 8: what a later run
    of the CHILD may find is a fact about the child. Intersecting with the caller's vocabulary
    made the same child admitted under a caller that names no cache and refused under one that
    does — and appending a call nothing can derive to an otherwise refused child turned the
    refusal into an admission, because the child's own `mutated_state` goes empty with it.

    The child here fills CACHE2, binds on what it retrieves, empties CACHE2 on a proved leg and
    then calls a process nothing in the request derives. Nothing states that process does not
    add to CACHE2, so run 2's retrieve may hand on its documents."""
    child = _legs(_DYNAMIC_INTO_CACHE2, _BINDS2, _EMPTIES2, _waited("EXTERNAL"))
    refused = [(PROCESS_IR_CAPABILITY_PROCESS_CALL_REPEATED_RUN_UNSTABLE, _PER_DOCUMENT_CALL)]
    names_no_cache = _per_document(child, **_WAITS_AND_ABORTS_ON_ERROR)
    names_the_cache = [("PARENT", _passthrough_root(
        _DYNAMIC_INTO_CACHE2, {"steps": [], "terminal": _call("CACHE_CHILD", **_WAITS_AND_ABORTS_ON_ERROR)})),
        ("CACHE_CHILD", child)]
    assert _both_routes(names_no_cache, "PARENT") == refused
    assert _both_routes(names_the_cache, "PARENT") == refused
    assert _row(names_no_cache, "PARENT", "CACHE_CHILD").mutated_state == ()
    # THE INVERSION: appending the underivable call to a refused child leaves it refused.
    fill_use = _legs(_DYNAMIC_INTO_CACHE2, _BINDS2)
    assert _both_routes(_per_document(fill_use, **_WAITS_AND_ABORTS_ON_ERROR), "PARENT") == refused
    assert _both_routes(_per_document(_legs(_DYNAMIC_INTO_CACHE2, _BINDS2, _waited("EXTERNAL")),
                                      **_WAITS_AND_ABORTS_ON_ERROR), "PARENT") == refused
    # CONTROL: the same cleanup child without the underivable call keeps its exemption.
    assert _both_routes(_per_document(_legs(_DYNAMIC_INTO_CACHE2, _BINDS2, _EMPTIES2),
                                      **_WAITS_AND_ABORTS_ON_ERROR), "PARENT") == []
    # NON-VACUITY: with the caller's vocabulary back in the unknown-writes branch, the child is
    # admitted under the caller that names no cache and refused under the one that does.
    _lineage_with_source(
        monkeypatch,
        "        return tuple(sorted(required))\n",
        "        return tuple(sorted(required & set(_caches_a_call_may_write(cache_refs, contract))))\n")
    assert _errors(names_no_cache, "PARENT") == []
    assert _errors(names_the_cache, "PARENT") == refused


# --- B21A-R2-FO-01 and W21A-01: what a re-cache carries -----------------------------------

#: A use of a property of documents that came out of one cache and were stored in another, by
#: kind: ``(the steps after the retrieve, the pointer suffix of the call-side refusal, the code)``.
_USES_OFF_A_RE_CACHE = {
    "ordinary": ([_READS_X], "", _READ_BEFORE_WRITE),
    "bound": ([_BOUND_GET], "/process_ref", _NOT_ESTABLISHED),
    "defaulted_writer": ([_Y_FROM_DEFAULTED_X, _BOUND_Y_GET], "", _READ_BEFORE_WRITE),
}
#: The re-cache itself: it reads the caller's CACHE and stores what it read into CACHE2,
#: with NOTHING reading the property on the way — so nothing in the child names it.
_RE_CACHES_WITHOUT_A_USE = {"steps": [_READ], "terminal": _PUT2}


@pytest.mark.parametrize("through_a_middle", (False, True), ids=("direct", "through_a_middle"))
@pytest.mark.parametrize("emptied_first", (False, True), ids=("no_removal", "emptied_first"))
@pytest.mark.parametrize("use", sorted(_USES_OFF_A_RE_CACHE))
def test_a_use_off_a_re_cache_owes_the_cache_the_documents_came_from(use, emptied_first, through_a_middle):
    """W21A-01 and B21A-R2-FO-01, one mechanism with two faces, both measured at 5b5038c.

    A child retrieves its caller's CACHE, stores what it retrieved into CACHE2 without reading
    the property on the way, and then uses that property on what it retrieves from CACHE2.

    - Without the removal the child was refused for EVERY caller and its contract named nothing
      a caller could satisfy, while the flattened graph of the same legs ran for a caller that
      stores the property (the over-refusal).
    - With CACHE2 emptied first on a proved leg the call was ADMITTED for a caller whose
      documents in CACHE lack the property, while the flattened twin was refused: those
      documents are exactly what the re-cache stores and the use reads (the fail-open).

    Both are the same gap: the documents changed caches, and the caller-cache attribution did
    not travel with them. It travels now on the carrier every other caller-cache obligation
    uses — the `CALLER_CACHE_WRITER` alternative, under the key no authored property can spell —
    so the use records its row against the cache the documents came OUT of. The child is its own
    process again, the caller that stores the property is admitted, and the one that does not is
    refused at its call, on both entry points and in agreement with the flattened graph."""
    steps, suffix, code = _USES_OFF_A_RE_CACHE[use]
    removal = [_EMPTIES2] if emptied_first else []
    child_legs = removal + [_RE_CACHES_WITHOUT_A_USE, {"steps": [_READ2] + steps, "terminal": _STOP}]
    callers = {"stores_the_property": _STAGES_X, "stores_documents_without_it": {"steps": [_GET], "terminal": _PUT}}
    for caller, caller_leg in callers.items():
        if through_a_middle:
            roots = [("PARENT", _legs(caller_leg, _waited("MID"))),
                     ("MID", _legs(_STORES_NOTHING, _waited("CACHE_CHILD"))),
                     ("CACHE_CHILD", _legs(*child_legs))]
            call = "/body/steps/0/legs/1/terminal"
        else:
            roots = [("PARENT", _legs(caller_leg, _waited("CACHE_CHILD"))), ("CACHE_CHILD", _legs(*child_legs))]
            call = "/body/steps/0/legs/1/terminal"
        breaks = caller == "stores_documents_without_it"
        assert _both_routes(roots, "CACHE_CHILD") == [], (caller, use)
        assert _both_routes(roots, "PARENT") == ([(code, call + suffix)] if breaks else []), (caller, use)
        assert ("$ref:CACHE", "X") in {
            (row[0], row[1]) for row in _row(roots, "PARENT", roots[1][0]).cache_property_requirements}
        twin = _errors([("PARENT", _legs(*([caller_leg] + child_legs)))], "PARENT")
        assert bool(twin) == breaks, (caller, use, twin)


#: Every site in the server that builds a `_Cohort` — what a cache write leaves behind — and
#: what each says about where those documents came from. The sibling sweep for
#: `attribution-lost-where-documents-change-hands`, whose first instance was a bound use past a
#: Message (ARCH-184-r2-01) and whose second is a use off a re-cache (W21A-01, B21A-R2-FO-01).
_COHORT_BUILDERS = {
    ("compiler/process_ir/semantic_validation/lineage.py", "_cohort"): (
        "THE factory every builder below goes through: it takes the origins explicitly and is "
        "the one place they become the alternative a later retrieve reads back, so a builder "
        "that carries none says so by passing none (B21A-R4-TI-03)"),
    # The two namedtuple copies in the tree, named so the sweep that now matches `_replace`
    # and `_make` stays closed: neither is a cohort, and a cohort copied in either function
    # would be a key of its own.
    ("compiler/process_ir/semantic_validation/lineage.py", "_transfer via stream._replace"): (
        "not a cohort: the per-path `_Stream` after a step, whose markers the cohort factory "
        "is handed as `origins` where the write happens"),
    ("compiler/process_ir/semantic_validation/lineage.py", "_edge_stream via stream._replace"): (
        "not a cohort: the same `_Stream`, weakened across an edge the walk cannot count"),
    ("compiler/process_ir/semantic_validation/lineage.py", "_cohort_at_write"): (
        "the executing Add to Cache: it carries every cache these documents came out of, the "
        "retrieve that handed them on and the ones that retrieve inherited"),
    ("compiler/process_ir/semantic_validation/lineage.py", "UNKNOWN_COHORT"): (
        "a write nothing here can inspect (a child, a contract, an outside writer): it claims "
        "no origin, and no use can be proved against it at all"),
    ("compiler/process_ir/semantic_validation/lineage.py", "_caller_cohort"): (
        "the caller's own documents, seeded per cache from the contract: the cache IS the row's "
        "key, so there is nothing further back to name"),
    ("authoring/contract.py", "_retrieve_overlay_behaviour_oracle"): (
        "not a write: hand-built inputs to `_overlay_cache_read` for the served revision's "
        "retrieve-overlay row (B21A-R3-TI-01). One of them now CARRIES an origin, because the "
        "row does distinguish one — the earlier justification asserted it did not, and the "
        "measurement said otherwise (B21A-R4-TI-04). What is true, and measured by "
        "`test_the_served_overlay_row_distinguishes_a_cohort_that_carries_an_origin`, is that "
        "`_overlay_cache_read`'s answer about a property key does not depend on the origin: the "
        "origin travels at the retrieve site, into `_Stream.retrieved_origins`"),
}


def _cohort_builders_in(source_root):
    """Every construction of a `_Cohort` under ``source_root``, as ``(file, owner)``.

    WHAT IS MATCHED, exactly — the claim is this list, not "every spelling" (V5-04 measured
    five that passed the round-5 version):

    * `_Cohort(...)` and `_cohort(...)`, plain or attribute-qualified (`lineage._Cohort(...)`),
      the factory being the one place a WRITE builds one;
    * `<anything>._replace(...)` and `<anything>._make(...)`, keyed by what they copy;
    * a call through a name bound to either in the SAME file, resolved transitively, and the
      same through `functools.partial` over either;
    * a call to a class declared in that file whose bases name either;
    * `getattr(<anything>, "_Cohort"|"_cohort")(...)` and `tuple.__new__(_Cohort, ...)`.

    STILL OPEN, named rather than implied: an alias bound in ANOTHER file and imported under
    a new name, a constructor kept in a container (`builders["cohort"](...)`), and anything
    reached only at runtime (`globals()[...]`). The sweep reads one file at a time.
    """
    import ast

    built = set()

    def owner_of(node, owner, relative):
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
            return node.name
        if isinstance(node, ast.Assign) and owner is None:
            names = [target.id for target in node.targets if isinstance(target, ast.Name)]
            return names[0] if names else owner
        return owner if owner is not None else relative

    def spelled(node):
        """The dotted name this call is spelled with, for a key that names the copy too."""
        if isinstance(node, ast.Name):
            return node.id
        if isinstance(node, ast.Attribute):
            return spelled(node.value) + "." + node.attr
        return "<expression>"

    def constructors_in(tree):
        """The names that BUILD a cohort in this file: the two real ones, every alias of
        them (including through `functools.partial`), and every class declared over one."""
        known = {"_Cohort", "_cohort"}
        growing = True
        while growing:
            growing = False
            for node in ast.walk(tree):
                found = set()
                if isinstance(node, ast.Assign):
                    value = node.value
                    if isinstance(value, ast.Call) and spelled(value.func).endswith("partial"):
                        value = value.args[0] if value.args else None
                    if value is not None and isinstance(value, (ast.Name, ast.Attribute)):
                        spelling = spelled(value)
                        if spelling in known or spelling.split(".")[-1] in known:
                            found = {target.id for target in node.targets
                                     if isinstance(target, ast.Name)}
                if isinstance(node, ast.ClassDef) and any(
                        spelled(base).split(".")[-1] in known for base in node.bases):
                    found = {node.name}
                if found - known:
                    known |= found
                    growing = True
        return known

    def builds_a_cohort(node, known):
        """Every spelling that can produce a cohort, not just the constructor's name.

        The compiler builds them through ONE checked factory that takes the origins
        explicitly (`_cohort`), so the sweep looks for that; the NamedTuple itself is still
        matched, qualified or not, for the served revision's hand-built inputs; and
        `_replace`/`_make` are matched WHEREVER they appear, because a cohort spelled either
        way carries whatever the copy carried and a sweep blind to them would have let a
        builder drop the origins with nothing failing (B21A-R4-TI-03). Round 6 added the
        indirections V5-04 measured: an alias, a `partial`, a subclass, a `getattr` and
        `tuple.__new__`. A copy is keyed by what it copies as well as by its owner, so a
        cohort copied inside a function that already copies something else still needs its
        own justification.
        """
        if not isinstance(node, ast.Call):
            return None
        func = node.func
        spelling = spelled(func)
        if spelling in known or spelling.split(".")[-1] in known:
            return ""
        if isinstance(func, ast.Attribute) and func.attr in ("_replace", "_make"):
            return " via " + spelled(func)
        # `tuple.__new__(_Cohort, ...)` and `type(...)(...)`: the cohort is an ARGUMENT.
        if any(isinstance(argument, (ast.Name, ast.Attribute))
               and spelled(argument).split(".")[-1] in known for argument in node.args):
            return " via " + spelling
        # `getattr(x, "_Cohort")(...)`: the constructor is named by a constant.
        if (isinstance(func, ast.Call) and spelled(func.func) == "getattr"
                and any(isinstance(argument, ast.Constant) and argument.value in known
                        for argument in func.args)):
            return " via getattr"
        if (spelling == "getattr"
                and any(isinstance(argument, ast.Constant) and argument.value in known
                        for argument in node.args)):
            return " via getattr"
        return None

    def walk(node, owner, relative, known):
        for child in ast.iter_child_nodes(node):
            how = builds_a_cohort(child, known)
            if how is not None:
                built.add((relative, (owner if owner is not None else relative) + how))
            walk(child, owner_of(child, owner, relative), relative, known)

    for path in sorted(Path(source_root).rglob("*.py")):
        tree = ast.parse(path.read_text(encoding="utf-8"))
        walk(tree, None, str(path.relative_to(source_root)), constructors_in(tree))
    return built


def _obligation_sites_in(source, recorded):
    """Every place one of ``recorded`` could be added to in ``source``, as ``(owner, list)``.

    WHAT IS MATCHED, exactly — the claim is this list and not "any spelling", which is what
    round 5's wording said while four spellings were checked (V5-03, measured):

    * `x += [row]`, and the same through a name bound to `x`;
    * ANY method call on the list — `append`, `extend`, `insert`, `__iadd__`, whatever comes
      next — including through an attribute-qualified owner (`self.rows.append`);
    * an assignment into the list, `x[...] = ...`, which is how a slice insert is written;
    * the list handed to a call that is not a builtin, by position or by keyword, since the
      callee may record into it;
    * the list's bound method handed to anything at all (`map(x.append, rows)`), a builtin
      included;
    * each of the above through a LOCAL ALIAS of the list, resolved transitively.

    STILL OPEN, named rather than implied: a recorded list stored in a container or an
    attribute (`box["rows"] = x`) and reached from somewhere else entirely; a list reached
    through `globals()`; and a mutation made in another module against a list passed there —
    the last is bounded by the sweep's own scope, which is this one module's source.
    """
    import ast
    import builtins

    tree = ast.parse(source)
    parents = {}
    for node in ast.walk(tree):
        for child in ast.iter_child_nodes(node):
            parents[child] = node
    found = set()

    def owner_of(node):
        while node is not None:
            if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
                return node.name
            node = parents.get(node)
        return None

    # Local aliases: `_a = read_cache_origins` makes `_a` the same list. Resolved to a fixed
    # point, so an alias of an alias is one too.
    aliases = {}
    growing = True
    while growing:
        growing = False
        for node in ast.walk(tree):
            if not (isinstance(node, ast.Assign) and len(node.targets) == 1
                    and isinstance(node.targets[0], ast.Name) and isinstance(node.value, ast.Name)):
                continue
            target, value = node.targets[0].id, node.value.id
            named = value if value in recorded else aliases.get(value)
            if named is not None and aliases.get(target) != named:
                aliases[target] = named
                growing = True

    def names(node):
        """The recorded list this expression stands for, directly or through an alias."""
        if isinstance(node, ast.Attribute) and node.attr in recorded:
            return node.attr
        while isinstance(node, ast.Attribute):
            node = node.value
        if not isinstance(node, ast.Name):
            return None
        return node.id if node.id in recorded else aliases.get(node.id)

    for node in ast.walk(tree):
        if isinstance(node, ast.AugAssign):
            name = names(node.target)
            if name is not None:
                found.add((owner_of(node), name))
        if isinstance(node, ast.Assign):
            for target in node.targets:
                if isinstance(target, ast.Subscript):
                    name = names(target.value)
                    if name is not None:
                        found.add((owner_of(node), name))
        if not isinstance(node, ast.Call):
            continue
        if isinstance(node.func, ast.Attribute):
            name = names(node.func.value)
            if name is not None:
                found.add((owner_of(node), name))
        handed = list(node.args) + [keyword.value for keyword in node.keywords]
        builtin = isinstance(node.func, ast.Name) and hasattr(builtins, node.func.id)
        for argument in handed:
            name = names(argument)
            if name is None:
                continue
            if isinstance(argument, ast.Attribute) or not builtin:
                found.add((owner_of(node), name))
    return found


def test_the_two_derived_sweeps_fail_closed(tmp_path, monkeypatch):
    """Non-vacuity of the two guards this round rebuilt (B21A-R4-TI-02, B21A-R4-TI-03).

    The cohort sweep is measured on a file written for it, with a cohort built in every
    spelling this guard claims — the constructor plain and qualified, the factory, the two
    NamedTuple copies, and the five indirections round 6 added after they were measured
    passing: an alias, an alias of the factory, a `functools.partial`, a subclass, a `getattr`
    and `tuple.__new__` (V5-04). The obligation sweep is measured on mutated copies of the
    derivation's own source, one per way it could quietly measure nothing: a requirement field
    nobody assigns, one whose expression reaches no walk field, a call that IS handed the walk
    and cannot be followed by name, the same call spelled through an attribute, a builtin that
    reads an attribute for you, and a second assignment to the same requirement key (V5-02).
    Two controls keep it honest: a call the walk is not handed, and a plain builtin over the
    walk, neither of which may raise."""
    from boomi_mcp.authoring import process_ir_effects

    spellings = tmp_path / "spellings"
    spellings.mkdir()
    (spellings / "builders.py").write_text(
        "import functools\n"
        "_C = _Cohort\n"
        "_c2 = _cohort\n"
        "_P = functools.partial(_Cohort)\n"
        "class Sub(_Cohort):\n    pass\n"
        "def a():\n    return _Cohort(1, 2, 3, 4)\n"
        "def b():\n    return lineage._Cohort(1, 2, 3, 4)\n"
        "def c():\n    return _cohort(1, 2, 3, 4, origins=())\n"
        "def d(x):\n    return x._replace(alternatives=frozenset())\n"
        "def e(rows):\n    return _Cohort._make(rows)\n"
        "def f():\n    return _C(1, 2, 3, 4)\n"
        "def g():\n    return _c2(1, 2, 3, 4)\n"
        "def h():\n    return _P(1, 2, 3, 4)\n"
        "def i():\n    return Sub(1, 2, 3, 4)\n"
        "def j(m):\n    return getattr(m, '_Cohort')(1, 2, 3, 4)\n"
        "def k():\n    return tuple.__new__(_Cohort, (1, 2, 3, 4))\n"
        "def nothing():\n    return _Stream('known')\n",
        encoding="utf-8")
    assert sorted(owner for _file, owner in _cohort_builders_in(spellings)) == [
        "_P via functools.partial", "a", "b", "c", "d via x._replace", "e via _Cohort._make",
        "f", "g", "h", "i", "j via getattr", "k via tuple.__new__"]

    source = Path(process_ir_effects.__file__).read_text(encoding="utf-8")
    assigns = '    facts["cache_property_requirements"] = _caller_cached_properties(prepared, base, walk)'
    assert source.count(assigns) == 1
    mutants = {
        "unassigned": "    pass",
        "reaches_no_walk_field": '    facts["cache_property_requirements"] = ()',
        "an_unfollowable_call_handed_the_walk": assigns + " + elsewhere_helper(walk)",
        "a_qualified_call_handed_the_walk": assigns + " + _t.dumps(walk)",
        "a_builtin_that_reads_an_attribute": assigns + ' + tuple(getattr(walk, "may_hold_at_exit"))',
        "a_second_assignment_to_the_same_key": (
            assigns + '\n    facts["cache_property_requirements"] = elsewhere_helper(walk)'),
    }
    silent = {
        "a_call_the_walk_is_not_handed": assigns + " + _t.dumps(prepared)",
        "a_plain_builtin_over_the_walk": assigns + " + tuple(walk.may_hold_at_exit)",
    }
    for label, replacement in sorted(silent.items()):
        quiet = tmp_path / (label + ".py")
        quiet.write_text(source.replace(assigns, replacement, 1), encoding="utf-8")
        with monkeypatch.context() as patched:
            patched.setattr(process_ir_effects, "__file__", str(quiet))
            assert _requirement_walk_fields(), label
    assert _requirement_walk_fields(), "the sweep must find something before it is mutated"
    for label, replacement in sorted(mutants.items()):
        mutated = tmp_path / (label + ".py")
        mutated.write_text(source.replace(assigns, replacement, 1), encoding="utf-8")
        with monkeypatch.context() as patched:
            patched.setattr(process_ir_effects, "__file__", str(mutated))
            with pytest.raises(AssertionError):
                _requirement_walk_fields()


def test_every_cohort_a_write_stores_says_where_its_documents_came_from():
    """The sibling sweep, derived: a cohort is the only thing a later retrieve hands on, so every
    construction of one either carries the caches its documents came out of or states why it
    cannot. A fourth builder fails here until it does one or the other."""
    import ast

    built = _cohort_builders_in(_ROOT / "src" / "boomi_mcp")
    assert built == set(_COHORT_BUILDERS), {
        "unjustified": sorted(built - set(_COHORT_BUILDERS)),
        "justified_but_absent": sorted(set(_COHORT_BUILDERS) - built),
    }
    # ... and the one that carries origins really does, measured rather than read: a cohort
    # written from retrieved documents names that cache; one written from the entry names none.
    from boomi_mcp.compiler.process_ir.semantic_validation.lineage import (
        CACHE_TRANSFER_UNPROVED,
        CALLER_CACHE_WRITER,
        _cohort_at_write,
        _Stream,
    )

    retrieved = _cohort_at_write(frozenset(), {}, _Stream("unknown", retrieved_from="$ref:CACHE"))
    assert (CACHE_TRANSFER_UNPROVED, CALLER_CACHE_WRITER + "$ref:CACHE") in retrieved.alternatives
    from_the_entry = _cohort_at_write(frozenset(), {}, _Stream("empty_entry"))
    assert not [token for key, token in from_the_entry.alternatives if key == CACHE_TRANSFER_UNPROVED]
    # ... and the factory is where the origins become that alternative, so a builder that
    # passes none carries none however it spells the rest.
    from boomi_mcp.compiler.process_ir.semantic_validation.lineage import _cohort

    assert _cohort(frozenset(), None, frozenset(), "one").alternatives == frozenset()
    assert _cohort(frozenset(), None, frozenset(), "one", origins=("$ref:CACHE",)).alternatives == \
        frozenset({(CACHE_TRANSFER_UNPROVED, CALLER_CACHE_WRITER + "$ref:CACHE")})


#: The served overlay row's columns, in the order `_retrieve_overlay_behaviour_oracle` records
#: them. Read from the oracle's own source so a column inserted later moves this table too.
_OVERLAY_ROW_COLUMNS = (
    "cohorts", "carried", "stream_count", "external", "current", "establishes_x", "writers_of_x",
    "x_on_documents", "x_invalidated", "origin_invalidated", "writers_of_the_origin",
    "count_out", "state",
)


def test_the_served_overlay_row_distinguishes_a_cohort_that_carries_an_origin():
    """B21A-R4-TI-04. The `_COHORT_BUILDERS` justification for the served revision's overlay
    oracle used to ASSERT that the row never reads an origin. Measured, that was wrong twice
    over: the row records the state it hands on, which holds the cohorts, so an origin-bearing
    cohort does move it — and the `shapes` table carried none, so the origin columns were
    constants and the claim could not have been measured either way.

    Both halves are measured here, against the two shapes that differ ONLY in the origin. The
    row distinguishes them; `_overlay_cache_read`'s answer about a property key does not, which
    is the accurate version of the old claim — the origin is carried at the retrieve site, into
    `_Stream.retrieved_origins` (pinned above and by the re-cache witnesses), not by the
    overlay's per-key answer."""
    from boomi_mcp.authoring import contract as authoring_contract

    rows = authoring_contract._retrieve_overlay_behaviour_oracle()["transfer"]
    assert {len(row) for row in rows} == {len(_OVERLAY_ROW_COLUMNS)}, sorted({len(row) for row in rows})
    column = {name: index for index, name in enumerate(_OVERLAY_ROW_COLUMNS)}
    by_case = {(tuple(row[column["cohorts"]]), row[column["carried"]], row[column["stream_count"]],
                row[column["external"]], row[column["current"]]): row for row in rows}
    with_an_origin = [case for case in by_case if case[0] == ("retrieved_from_a_caller_cache@one",)]
    assert with_an_origin, sorted({label for case in by_case for label in case[0]})
    moved, same_answer = 0, 0
    for case in with_an_origin:
        twin = ((("unknown_properties@one",),) + case[1:])
        origin_row, plain_row = by_case[case], by_case[twin]
        assert origin_row != plain_row, case
        moved += 1
        for name in ("establishes_x", "writers_of_x", "x_on_documents", "x_invalidated",
                     "origin_invalidated", "writers_of_the_origin", "count_out"):
            assert origin_row[column[name]] == plain_row[column[name]], (case, name)
        same_answer += 1
        assert origin_row[column["state"]] != plain_row[column["state"]], case
    assert moved == same_answer == 16, moved


def test_the_re_cache_attribution_is_load_bearing(monkeypatch):
    """Non-vacuity, one mutant per face, each a source edit of the one carrier.

    - The cohort stores no origin: the child that re-caches without a use is refused for every
      caller again, with a contract naming nothing (the over-refusal returns).
    - The retrieve does not read the origins back: the caller whose documents lack the property
      is admitted again while the flattened twin refuses it (the fail-open returns)."""
    child = _legs(_RE_CACHES_WITHOUT_A_USE, {"steps": [_READ2, _BOUND_GET], "terminal": _STOP})
    emptied = _legs(_EMPTIES2, _RE_CACHES_WITHOUT_A_USE, {"steps": [_READ2, _BOUND_GET], "terminal": _STOP})
    satisfying = [("PARENT", _legs(_STAGES_X, _waited("CACHE_CHILD"))), ("CACHE_CHILD", child)]
    breaking = [("PARENT", _legs({"steps": [_GET], "terminal": _PUT}, _waited("CACHE_CHILD"))),
                ("CACHE_CHILD", emptied)]
    assert _errors(satisfying, "CACHE_CHILD") == [] and _errors(satisfying, "PARENT") == []
    assert _errors(breaking, "PARENT") == [(_NOT_ESTABLISHED, "/body/steps/0/legs/1/terminal/process_ref")]
    twin = [("PARENT", _legs({"steps": [_GET], "terminal": _PUT}, _EMPTIES2, _RE_CACHES_WITHOUT_A_USE,
                             {"steps": [_READ2, _BOUND_GET], "terminal": _STOP}))]
    assert _errors(twin, "PARENT") != []
    with monkeypatch.context() as patched:
        # The write stores no origin at all.
        _lineage_with_source(
            patched,
            "            ({stream.retrieved_from} - {None}) | set(stream.retrieved_origins)\n",
            "            ()\n")
        assert _errors(satisfying, "CACHE_CHILD") != []
        assert _errors(breaking, "PARENT") == []
        assert _errors(twin, "PARENT") != []
    with monkeypatch.context() as patched:
        # The write stores them and the retrieve does not read them back.
        _lineage_with_source(patched, "retrieved_origins=inherited", "retrieved_origins=()", occurrences=2)
        assert _errors(satisfying, "CACHE_CHILD") != []
        assert _errors(breaking, "PARENT") == []


#: Every site in the server that records a row a CALLER must discharge, and what makes each one
#: ask the one authority (`_caller_documents_may_reach`) before it does, or why the authority
#: does not apply to it. Keyed on the function that records, in the style of `_REMOVAL_READERS`.
#: The hand sweep missed a channel in each of the two earlier rounds (TI2-184-21a-01) and the
#: hand-chosen LIST SET missed the content and entry channels in the third (B21A-R3-TI-01), so
#: both the set of lists and the set of sites are now derived from source.
_OBLIGATION_SITES = {
    ("_report", "findings"): (
        "the walk's own findings, which are a requirement channel because "
        "`_caller_composed_paths` re-walks the child with `caller_supplied_writers` seeded and "
        "reads which DYNAMIC_PATH_DDP_NOT_ESTABLISHED refusals CLEAR: a refusal that a caller's "
        "writer removes IS the `required_writers` row. One place by construction — every "
        "finding the walk reports goes through `_report` — and the fail-closed sweep is what "
        "surfaced this channel (B21A-R4-TI-02); measured by the passthrough `required_writers` "
        "forwarding case and by the bound cells of the matrix"),
    ("_check_bound_key", "unestablished_bindings"): (
        "a bound path this process cannot compose: the row is about the CALLER'S OWN DOCUMENTS, "
        "not about a cache, so the cache authority does not apply — `_caller_composed_paths` "
        "measures it by seeding `caller_supplied_writers`; measured by the passthrough "
        "`required_writers` forwarding case and the bound cells of the matrix"),
    ("_classify_unmet_read", "unmet"): (
        "what the caller must ESTABLISH (`required_reads`): the authority is asked in the "
        "negative — a document property read past a retrieve is never an entry requirement "
        "(`of_retrieved_documents`) — and a cache read is establishment, not attribution; "
        "measured by `test_a_passthrough_process_asks_its_callers_nothing_of_their_documents_"
        "for_what_it_retrieved` and by the matrix's storing-nothing caller"),
    ("_requires", "cache_requirement_refs"): (
        "the content row a consumer of retrieved documents records: the cache is "
        "`_Stream.caller_cache`, which IS the retrieve's own `_caller_documents_may_reach` "
        "answer; measured by the matrix's content use at the own site"),
    ("_requires", "entry_requirement_refs"): (
        "what a passthrough child requires of the documents its caller hands over: the entry "
        "group, not a cache, so the authority does not apply; measured by the "
        "`document_requirements` forwarding case"),
    ("_discharge_child_contract", "cache_requirement_refs"): (
        "the content row a call inherits from its child, recorded under a direct "
        "`_caller_documents_may_reach(state, cache_ref)`; measured by the matrix's content use "
        "at the inherited site and by "
        "`test_a_process_that_stages_the_profile_itself_still_owes_the_content_its_callers_share`"),
    ("_classify_unmet_read", "read_cache_origins"): (
        "the ride-on credit for an unmet ordinary read: the caches come from `_Stream`'s "
        "`retrieved_from` and `retrieved_origins`, both set at a retrieve that asked the "
        "authority; measured by the matrix's ordinary and defaulted_writer uses at the own site"),
    ("_classify_unmet_read", "unestablished_cached_keys"): (
        "the cached row for an unmet read: `cached_from` is `_caller_cached_origin`, which reads "
        "the stream's `caller_cache` — the authority's own answer at the retrieve — and the "
        "origin rows come from `retrieved_origins`; measured by the matrix's own and inherited "
        "sites and by the re-cache witnesses"),
    ("_check_bound_key", "binding_cache_origins"): (
        "the same credit for a bound path, off the same two stream markers; measured by the "
        "matrix's bound use at every site"),
    ("_check_bound_key", "unestablished_cached_keys"): (
        "the cached row a refused binding records, and the rows a PROVED binding owes through "
        "its writer's captured `CALLER_CACHE_WRITER` alternatives, which exist only where "
        "`_caller_owes_a_cached_property` said yes at the retrieve"),
    ("owe_the_retrieve_on_failure", "unestablished_cached_keys"): (
        "THE row of a binding whose check failed on documents a call retrieved for its child: "
        "one rule inside `_check_bound_key`, fed by the three call sites, and the cache is the "
        "site's own `cached_from` — `_caller_documents_may_reach` asked where that retrieve "
        "happens (B21A-R4-FO-01); measured by "
        "`test_a_failed_binding_on_documents_a_call_retrieved_travels_to_the_caller_that_"
        "stored_them` — the three-level graph, with "
        "`test_the_inherited_rows_obligation_on_a_failed_writer_is_load_bearing` neutralising "
        "this rule and nothing else (the two-level test passes with it reverted, V5-01) — and "
        "by the matrix's bound and defaulted_writer uses at the inherited site"),
    ("_check_one_writer", "read_cache_origins"): (
        "a path writer's unmet DDP source, credited to the caches its documents came out of, off "
        "the same markers captured with the writer; measured by the defaulted_writer use"),
    ("_check_one_writer", "unestablished_cached_keys"): (
        "the same source's cached and origin rows; measured by the defaulted_writer use and by "
        "`test_a_path_writer_owes_the_callers_that_share_its_cache_the_source_it_composes_from`"),
    ("_owe_the_caches_behind", "unestablished_cached_keys"): (
        "what a PROVED use owes: one row per `CALLER_CACHE_WRITER` alternative, and the retrieve "
        "adds that alternative only where `_caller_owes_a_cached_property` — the authority plus "
        "the seeded-pair bookkeeping — said yes"),
    ("_discharge_child_contract", "unestablished_cached_keys"): (
        "the row a call inherits from its child, recorded only when `_caller_owes_a_cached_"
        "property` says this process's callers may share the cache; measured by the matrix's "
        "inherited site"),
}


def _requirement_walk_fields():
    """The walk fields the child-contract derivation turns into a REQUIREMENT field.

    Derived twice over, so neither half is a hand list: the requirement fields come from the
    lattice table above (`_BOUNDARY`, requirement side), and which walk field feeds each comes
    from `derive_child_entry_facts`'s own source — the expression assigned to that fact, with
    local names resolved and the module-level helpers it calls followed.

    FAIL-CLOSED, every way it could quietly measure nothing (B21A-R4-TI-02, widened in round 6
    by V5-02): a requirement field the derivation never assigns, a field whose expression
    reaches no walk attribute at all, and any call handed the walk that this sweep cannot
    follow. EVERY assignment to a requirement key is swept, not the first one found: the
    derivation assigns some of them once per entry form, and a second assignment on one path
    would otherwise decide the runtime value while this sweep read another expression.

    The resolution rule is closed and derived rather than listed. A call is followable when its
    func is a NAME defined in this module (followed into), or the walk's own constructor (its
    result IS the walk, so attributes off it count like `walk.<field>`), or a builtin that
    cannot read an attribute for you. `getattr`, `vars` and `map` are builtins that CAN, so
    they count as unfollowable the moment the walk reaches them. Every other spelling — an
    attribute-qualified helper from another module, a call through a subscript, a lambda's
    result — is unfollowable too, with one hand-modelled exception, `facts.update(...)`, whose
    keywords this sweep reads directly. A call the walk is NOT handed cannot read a walk field
    whatever it does, which is what makes `prepare_validation_context(child_ir, symbols)` and
    the canonicalization of the trusted context safe to pass over without naming them.

    STILL OPEN, named rather than implied: the walk could be stashed in a container or an
    attribute first (`box["w"] = walk`) and read back out of it later; this sweep follows names
    and calls, not containers.
    """
    import ast
    import builtins

    from boomi_mcp.authoring import process_ir_effects
    from boomi_mcp.compiler.process_ir.semantic_validation.lineage import walk_lineage

    #: The one call whose RESULT is a walk. Named from the runtime object, so renaming it in
    #: the derivation's import list makes this sweep fail rather than silently miss a channel.
    walking = walk_lineage.__name__
    wanted = {field for (_component, side), cell in _BOUNDARY.items()
              if side == "requirement" and isinstance(cell, tuple) for field in cell}
    tree = ast.parse(Path(process_ir_effects.__file__).read_text(encoding="utf-8"))
    functions = {node.name: node for node in ast.walk(tree) if isinstance(node, ast.FunctionDef)}
    derive = functions["derive_child_entry_facts"]
    #: name/key -> every expression assigned to it, in source order. A list, not a first
    #: writer: two assignments to one requirement key mean the sweep would read an
    #: expression the runtime value does not come from (V5-02).
    assigned: "dict" = {}

    def record(key, value):
        assigned.setdefault(key, []).append(value)

    for node in ast.walk(derive):
        if isinstance(node, ast.Assign):
            for target in node.targets:
                if isinstance(target, ast.Name):
                    record(target.id, node.value)
                if isinstance(target, ast.Subscript) and isinstance(target.slice, ast.Constant):
                    record(target.slice.value, node.value)
        if isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute) and node.func.attr == "update":
            for keyword in node.keywords:
                record(keyword.arg, keyword.value)
    unfollowed = set()
    #: Builtins that read an attribute or apply a callable for you, so a walk handed to one
    #: goes somewhere this sweep cannot see. Everything else in `builtins` is inert here.
    opaque_builtins = {"getattr", "vars", "map", "filter", "eval", "exec", "next", "iter"}

    def is_a_walk(node):
        """Whether this expression IS the walk: the name it is bound to, or a fresh one."""
        if isinstance(node, ast.Name):
            return node.id in ("walk", "after")
        return (isinstance(node, ast.Call) and isinstance(node.func, ast.Name)
                and node.func.id == walking)

    def is_handed_the_walk(call):
        """Whether the walk reaches this call — the only way it could read a walk field."""
        return any(is_a_walk(inner)
                   for argument in list(call.args) + [keyword.value for keyword in call.keywords]
                   for inner in ast.walk(argument))

    def spelling(func):
        """How a call is written, for naming what could not be followed."""
        if isinstance(func, ast.Name):
            return func.id
        if isinstance(func, ast.Attribute):
            return spelling(func.value) + "." + func.attr
        return type(func).__name__

    def collect(node, seen):
        found = set()
        for child in ast.walk(node):
            if isinstance(child, ast.Attribute) and is_a_walk(child.value):
                found.add(child.attr)
            if isinstance(child, ast.Name) and child.id in assigned and child.id not in seen:
                for value in assigned[child.id]:
                    found |= collect(value, seen | {child.id})
            if not isinstance(child, ast.Call):
                continue
            func = child.func
            if isinstance(func, ast.Name) and func.id in functions:
                if func.id not in seen:
                    found |= collect(functions[func.id], seen | {func.id})
                continue
            if not is_handed_the_walk(child):
                # Whatever it is, it cannot read a field of a walk it was never handed.
                continue
            if isinstance(func, ast.Name) and func.id == walking:
                continue
            if (isinstance(func, ast.Name) and hasattr(builtins, func.id)
                    and func.id not in opaque_builtins):
                continue
            if isinstance(func, ast.Attribute) and spelling(func) == "facts.update":
                # Hand-modelled above: its keywords are read as assignments.
                continue
            unfollowed.add(spelling(func))
        return found

    fields = set()
    for field in sorted(wanted):
        assert field in assigned, (
            "the derivation assigns no %s, so this sweep would measure nothing for it" % field)
        # EVERY assignment to the key, not the first: the derivation assigns some of them
        # once per entry form, and a second assignment on one path would otherwise decide
        # the runtime value while the sweep read another expression (V5-02).
        reached = set()
        for value in assigned[field]:
            reached |= collect(value, {field})
        assert reached, ("%s reaches no walk field" % field, sorted(fields))
        fields |= reached
    assert not unfollowed, sorted(unfollowed)
    return fields


def test_every_caller_obligation_is_recorded_through_the_one_authority():
    """The derived sweep for the batch's structural fix, in the style of
    `test_every_proved_removal_is_read_through_one_rule` and for the same reason: an enumeration
    of sites, each free to decide for itself, is what the fix replaced, and a hand-written
    coverage claim missed a channel in each round — the writer's defaulted source
    (SOUND-21a-01), the entry-requirement channel (round 2), and the content channel, which the
    guard's own hand-chosen list set hid (B21A-R3-TI-01).

    Both halves are derived now. The LISTS come from the lattice table's requirement cells
    through `derive_child_entry_facts`'s own source; the SITES are every mutation of those
    lists in the spellings `_obligation_sites_in` enumerates — a method call on the list, an
    augmented assignment, an assignment into it, the list or its bound method handed to a
    call, each of those through a local alias — with what that sweep does NOT see named in
    its own docstring rather than implied here (V5-03)."""
    import ast

    lineage_source = Path(lineage.__file__).read_text(encoding="utf-8")
    tree = ast.parse(lineage_source)
    construction = next(node for node in ast.walk(tree) if isinstance(node, ast.Call)
                        and isinstance(node.func, ast.Name) and node.func.id == "LineageWalkV1")
    declared = {node.target.id for node in ast.walk(tree)
                if isinstance(node, ast.AnnAssign) and isinstance(node.target, ast.Name)
                and isinstance(node.value, (ast.List, ast.Set))}
    requirement_fields = _requirement_walk_fields()
    recorded = {name
                for keyword in construction.keywords if keyword.arg in requirement_fields
                for name in {inner.id for inner in ast.walk(keyword.value) if isinstance(inner, ast.Name)}
                if name in declared}
    # The derivation must reach the three channels the batch works through, the content channel
    # the guard used to miss, and the entry one — otherwise the guard would be measuring less
    # than it says while still passing.
    assert {"unestablished_cached_keys", "read_cache_origins", "binding_cache_origins",
            "cache_requirement_refs", "entry_requirement_refs", "unmet"} <= recorded, sorted(recorded)
    found = _obligation_sites_in(lineage_source, recorded)
    assert found == set(_OBLIGATION_SITES), {
        "unjustified": sorted(found - set(_OBLIGATION_SITES)),
        "justified_but_absent": sorted(set(_OBLIGATION_SITES) - found),
    }
    # The lists are the walk's own locals, so a row recorded anywhere else reaches no contract.
    assert recorded <= set(lineage.LineageWalkV1._fields) | {"unmet"}, sorted(recorded)


# --- R8-TXT-01 and B21A-R2-TXT-01/-02: what the three surfaces say ------------------------

def test_the_repeated_run_exemption_is_scoped_to_the_cause_it_belongs_to():
    """R8-TXT-01. The exemption — every run ends with nothing it stored left in the cache, and
    the call waits and aborts on error — applies to the cause that USES what it retrieves, never
    to a child that reads the cache before writing it: for that one the establishment the call
    proved is gone whatever the child does afterwards. Measured on the child the two readings
    disagree about: it reads CACHE2 before writing it, its contract's
    `required_caches_retain_nothing_it_stored` is True and the call waits and aborts, and it is
    refused. The served sentence must therefore attach the exemption to the second cause only,
    which is where the other two surfaces already put it."""
    from boomi_mcp.compiler.process_ir.semantic_validation import findings

    reads_then_empties = [
        ("PARENT", _passthrough_root(_DYNAMIC_INTO_CACHE2, {"steps": [], "terminal": _call(
            "CACHE_CHILD", **_WAITS_AND_ABORTS_ON_ERROR)})),
        ("CACHE_CHILD", _legs(_BINDS2, _EMPTIES2))]
    assert _both_routes(reads_then_empties, "PARENT") == [
        (PROCESS_IR_CAPABILITY_PROCESS_CALL_REPEATED_RUN_UNSTABLE, _PER_DOCUMENT_CALL)]
    row = _row(reads_then_empties, "PARENT", "CACHE_CHILD")
    assert row.required_caches_retain_nothing_it_stored is True
    assert ("cache", "$ref:CACHE2") in row.required_reads
    message = findings.finding(
        PROCESS_IR_CAPABILITY_PROCESS_CALL_REPEATED_RUN_UNSTABLE, "error", "capability", "/body").message
    first, second = message.index("it reads that cache before writing it"), message.index("; or it consumes")
    exemption = message.index("unless every run ends with nothing it stored left in that cache")
    assert first < second < exemption, message


#: B21A-R2-TXT-01: what the remediation's second way out describes, and the near misses. The
#: caller is a No Data root whose Branch leg ends in the call; the child is the refused shape.
_WAY_TWO_LEG_PREFIXES = {
    "no steps of its own": ([], True),
    "a message": ([_MSG], False),
    "a map": ([_TO_P2], False),
    "a process property write": ([_SET_K], False),
    "flow control": ([{"kind": "flow_control", "for_each_count": 1}], False),
}
_WAY_TWO_ROOT_PREFIXES = {
    "a message": [_MSG],
    "a process property write": [_SET_K],
    "flow control": [{"kind": "flow_control", "for_each_count": 1}],
    "a map": [_TO_P2],
    "a cache retrieve": [_READ],
    "a data process": [_SPLITS_THE_DOCUMENTS],
    "a connector call": [_GET],
    "a message and a connector call": [_MSG, _GET],
}


def test_the_second_way_out_of_the_repeated_run_refusal_is_a_recipe():
    """B21A-R2-TXT-01. Ways 1 and 3 are sufficient recipes — author this, and the call is
    admitted — and way 2 was not: "a path that runs no connector call, cache retrieve or data
    process before the call" describes a Message, a map, a property write and flow control
    before the call, and each of those is refused under the placement code, which the same
    served catalog's `node.process_call` entry states. It now names what it means, the empty leg
    with nothing in front of its Branch, and this measures every near miss: exactly the shape
    the sentence describes is admitted."""
    from boomi_mcp.compiler.process_ir.semantic_validation import findings
    from boomi_mcp.models.process_ir import ProcessIRValidationError

    remediation = findings.finding(
        PROCESS_IR_CAPABILITY_PROCESS_CALL_REPEATED_RUN_UNSTABLE, "error", "capability", "/body").remediation
    assert "as the terminal of a Branch leg that has no steps of its own and no step in front " \
           "of its Branch" in remediation
    child = _legs(_DYNAMIC_INTO_CACHE2, _BINDS2)
    for label, (prefix, admitted) in _WAY_TWO_LEG_PREFIXES.items():
        roots = [("PARENT", _legs(_STORES_NOTHING, {"steps": list(prefix), "terminal": _call("CACHE_CHILD")})),
                 ("CACHE_CHILD", child)]
        verdict = _both_routes(roots, "PARENT")
        if admitted:
            assert verdict == [], label
        else:
            # The placement refusal, beside whatever else the prefix itself is refused for (a
            # map of documents the leg does not carry is its own cardinality defect).
            assert (PROCESS_IR_CAPABILITY_PROCESS_CALL_PLACEMENT_UNSUPPORTED,
                    "/body/steps/0/legs/1/terminal") in verdict, (label, verdict)
    for label, prefix in _WAY_TWO_ROOT_PREFIXES.items():
        document = _doc(*(list(prefix) + [{"kind": "branch", "legs": [
            _STORES_NOTHING, {"steps": [], "terminal": _call("CACHE_CHILD")}]}]))
        try:
            parsed = [("PARENT", document), ("CACHE_CHILD", child)]
            assert _both_routes(parsed, "PARENT") != [], label
        except ProcessIRValidationError as refused:
            assert "PROCESS_IR" in str(refused), label


def test_every_served_surface_that_states_rule_eight_states_its_gate():
    """B21A-R2-TXT-02. Four surfaces state amendment 1 rule 8 — the finding's message, the
    authoring entry, the architecture doc's §3e row and the error taxonomy's summary — and the
    call's wait/abort gate decides a measured verdict, so a summary that leaves it out describes
    a refusal no listed cause explains."""
    from boomi_mcp.compiler.process_ir.semantic_validation import findings
    from boomi_mcp.errors import ERROR_TAXONOMY

    summary = ERROR_TAXONOMY[PROCESS_IR_CAPABILITY_PROCESS_CALL_REPEATED_RUN_UNSTABLE].summary
    message = findings.finding(
        PROCESS_IR_CAPABILITY_PROCESS_CALL_REPEATED_RUN_UNSTABLE, "error", "capability", "/body").message
    from boomi_mcp.authoring.process_ir_projection import process_ir_authoring_revision_payload

    from test_issue_184_child_entries import _entry_by_id

    entry = _entry_by_id(process_ir_authoring_revision_payload(), "node.process_call")
    doc_text = (_ROOT / "docs" / "architecture" / "PROCESS_IR_V1.md").read_text(encoding="utf-8")
    assert "wait=true and abort_on_error=true" in message
    assert "wait=true and abort_on_error=true" in " ".join(entry["ordering_facts"])
    assert "wait and abort on error" in doc_text
    # The rule is a CONJUNCTION — nothing retained AND the call waits and aborts — and a summary
    # that keeps one conjunct exempts shapes the server refuses (B21A-R3-TXT-01). Measured on
    # the three cells that tell the conjuncts apart, rather than on a substring.
    assert "unless every run ends with nothing it stored there" in summary, summary
    assert "the call both waits and aborts on error" in summary, summary
    # ... and "nothing it stored there" is itself a conjunction, whose other conjuncts the
    # summary omitted while A10's shape — a declared outside writer on a flagged retrieve —
    # is refused (B21A-R6-V6-02, the third instance on this surface).
    for clause in ("no call it makes may write that cache after its removal or without being "
                   "waited for",
                   "no outside writer is declared for that cache on a retrieve that names one"):
        assert clause in summary, summary
    refused = [(PROCESS_IR_CAPABILITY_PROCESS_CALL_REPEATED_RUN_UNSTABLE, _PER_DOCUMENT_CALL)]
    cleanup = _legs(_DYNAMIC_INTO_CACHE2, _BINDS2, _EMPTIES2)
    no_removal = _legs(_DYNAMIC_INTO_CACHE2, _BINDS2)
    cells = {
        # retains what it stored, and the call waits and aborts: the first conjunct is false.
        "retains_what_it_stored": (no_removal, _WAITS_AND_ABORTS_ON_ERROR, refused),
        # retains nothing, but the call does not abort on error: the second conjunct is false.
        "the_call_does_not_abort": (cleanup, {"wait": True}, refused),
        # both conjuncts hold: admitted.
        "both_hold": (cleanup, _WAITS_AND_ABORTS_ON_ERROR, []),
    }
    for label, (child, flags, expected) in cells.items():
        roots = _per_document(child, **flags)
        assert _both_routes(roots, "PARENT") == expected, label
        retained = _row(roots, "PARENT", "CACHE_CHILD").required_caches_retain_nothing_it_stored
        assert retained is (label != "retains_what_it_stored"), label


# ---------------------------------------------------------------------------
# Correction batch 21a round 4: verification round 3's findings
# ---------------------------------------------------------------------------

# --- B21A-R3-FO-01: a failed WRITER is a failed BINDING ------------------------------------

#: The child of the fail-open: it re-caches its caller's CACHE into CACHE2 after composing Y
#: from a DEFAULTED X, then binds a request path to Y on what it retrieves from CACHE2.
_COMPOSES_Y_THEN_RE_CACHES = {"steps": [_READ, _Y_FROM_DEFAULTED_X], "terminal": _PUT2}
#: The same writer with no unmet source: its value comes from a process property the execution
#: supplies. This one was charged before, and is the non-vacuity pair for the fix.
_COMPOSES_Y_FROM_A_PROCESS_PROPERTY = {"kind": "set_ddp", "name": "Y", "source_values": [
    {"value_type": "static", "value": "/clients/"},
    {"value_type": "dpp", "property_name": "key", "default_value": ""}]}


@pytest.mark.parametrize("writer", ("an_unmet_source", "a_process_property"))
def test_a_binding_whose_writer_fails_owes_the_cache_it_retrieves_from(writer):
    """B21A-R3-FO-01, a fail-open round 3's carrier exposed: at 5b5038c this child was refused on
    its own walk (the W21A-01 over-refusal), and making it admissible turned a masked gap into a
    live one.

    The binding's establishment test PASSES — the child's own writer guarantees Y on what it
    re-caches — and the composition then fails inside `_check_one_writer` because the writer's
    own source is unmet. That failure recorded only the SOURCE property's caches, so the cache
    the BINDING retrieves from was never charged and a caller that stored documents there
    without Y was admitted while the flattened graph refused it. A composition failure is a
    failure of the binding, so it now records what an establishment failure records.

    The control keeps the pair honest: with a writer that has no unmet source the same row was
    already published, so the fix adds the missing half rather than the whole rule."""
    steps = _COMPOSES_Y_THEN_RE_CACHES if writer == "an_unmet_source" else {
        "steps": [_READ, _COMPOSES_Y_FROM_A_PROCESS_PROPERTY], "terminal": _PUT2}
    child_legs = [steps, {"steps": [_READ2, _BOUND_Y_GET], "terminal": _STOP}]
    bare_documents = {"steps": [_GET], "terminal": _PUT2}
    caller_legs = [_STAGES_X, bare_documents]
    roots = [("PARENT", _legs(*(caller_legs + [_waited("CACHE_CHILD")]))),
             ("CACHE_CHILD", _legs(*child_legs))]
    assert _both_routes(roots, "CACHE_CHILD") == []
    assert _both_routes(roots, "PARENT") == [(_NOT_ESTABLISHED, "/body/steps/0/legs/2/terminal/process_ref")]
    rows = {(row[0], row[1], row[3]) for row in _row(roots, "PARENT", "CACHE_CHILD").cache_property_requirements}
    assert ("$ref:CACHE2", "Y", True) in rows, rows
    if writer == "an_unmet_source":
        # ... beside the SOURCE row the round-1 fix publishes, which names the other cache.
        assert ("$ref:CACHE", "X", False) in rows, rows
    # The flattened graph of the same legs refuses the same caller.
    assert _errors([("PARENT", _legs(*(caller_legs + child_legs)))], "PARENT") != []
    # CONTROL: a caller that stores nothing in the cache the binding retrieves from is admitted.
    admitted = [("PARENT", _legs(_STAGES_X, _STORES_NOTHING, _waited("CACHE_CHILD"))),
                ("CACHE_CHILD", _legs(*child_legs))]
    assert _both_routes(admitted, "PARENT") == []


def test_the_bindings_own_obligation_on_a_failed_writer_is_load_bearing(monkeypatch):
    """Non-vacuity: with the binding's own rows dropped from the writer-failure path, the
    bare-documents caller is admitted again while the flattened twin still refuses it."""
    child = _legs(_COMPOSES_Y_THEN_RE_CACHES, {"steps": [_READ2, _BOUND_Y_GET], "terminal": _STOP})
    caller_legs = [_STAGES_X, {"steps": [_GET], "terminal": _PUT2}]
    roots = [("PARENT", _legs(*(caller_legs + [_waited("CACHE_CHILD")]))), ("CACHE_CHILD", child)]
    assert _errors(roots, "PARENT") != []
    # The mutant: a failed binding owes nothing, which is what it did before the fix.
    _lineage_with_source(
        monkeypatch,
        "                _owe_the_caches_behind(\n"
        "                    node.source_path + sub_path, key, binding.request_profile_ref, True, writers)\n"
        "                owe_the_retrieve_on_failure()\n"
        "                return False\n",
        "                return False\n")
    assert _errors(roots, "PARENT") == []
    assert _errors([("PARENT", _legs(*(caller_legs + _branch_legs(child))))], "PARENT") != []


# --- B21A-R4-FO-01: the same failure ONE CALL FURTHER OUT ----------------------------------

#: The three-level shape round 5's rule exists for. The grandparent stages X into CACHE and
#: BARE documents into CACHE2; the middle retrieves CACHE, composes Y from a defaulted X and
#: re-caches into CACHE2; the child retrieves CACHE2 and binds a request path to Y. The
#: middle's writer fails inside `_check_one_writer`, so the row the child charged the middle
#: has to travel one further call — to the grandparent that stored the Y-less documents.
_THREE_LEVEL_RE_CACHE = (
    [_STAGES_X, {"steps": [_GET], "terminal": _PUT2}],                      # the grandparent's
    [{"steps": [_READ, _Y_FROM_DEFAULTED_X], "terminal": _PUT2}],           # the middle's
    [{"steps": [_READ2, _BOUND_Y_GET], "terminal": _STOP}, _STORES_NOTHING],  # the child's
)


def _three_level_roots():
    caller, middle, child = _THREE_LEVEL_RE_CACHE
    return [("PARENT", _legs(*(list(caller) + [_waited("MID")]))),
            ("MID", _legs(*(list(middle) + [_waited("CACHE_CHILD")]))),
            ("CACHE_CHILD", _legs(*child))]


def test_a_failed_binding_on_documents_a_call_retrieved_travels_to_the_caller_that_stored_them():
    """B21A-R4-FO-01, the shape the rule is FOR, at the depth it is for.

    Round 4 fixed a failed writer's obligation at the in-process binding site; the identical
    failure at the CALL-DISCHARGE site — the row a call inherits from its child, on documents
    the call itself retrieved — was still dropped, so the middle was served clean and its
    caller was never charged while the flattened graph of the same legs refused. The middle's
    own walk is clean here (its writer's refusal is its caller's to discharge), which is what
    makes the grandparent's verdict the only place the defect is visible.

    The two-level twin above passes with the rule reverted — it is the control, not the
    witness (V5-01). This graph is the witness: the pointer, the row and the flattened twin."""
    roots = _three_level_roots()
    assert _both_routes(roots, "MID") == []
    assert _both_routes(roots, "CACHE_CHILD") == []
    assert _both_routes(roots, "PARENT") == [
        (_NOT_ESTABLISHED, "/body/steps/0/legs/2/terminal/process_ref")]
    # THE row: the middle charges its own callers the bound property, on the cache it
    # retrieved the documents from.
    assert ("$ref:CACHE2", "Y", None, True) in _row(roots, "PARENT", "MID").cache_property_requirements
    # ... and the child's own row, one level in, is what the middle inherited it from.
    assert ("$ref:CACHE2", "Y", None, True) in _row(roots, "MID", "CACHE_CHILD").cache_property_requirements
    caller, middle, child = _THREE_LEVEL_RE_CACHE
    flattened = _legs(*(list(caller) + list(middle) + list(child)))
    assert _errors([("PARENT", flattened)], "PARENT") != []
    # CONTROL: the same three processes, with the grandparent storing nothing in CACHE2.
    admitted = [("PARENT", _legs(_STAGES_X, _STORES_NOTHING, _waited("MID")))] + roots[1:]
    assert _both_routes(admitted, "PARENT") == []


def test_the_inherited_rows_obligation_on_a_failed_writer_is_load_bearing(monkeypatch):
    """Non-vacuity of round 5's rule ALONE: with only
    `if owes_the_retrieve_on_failure and cached_from is not None:` neutralised — round 4's
    `_owe_the_caches_behind` call left intact — the grandparent above is served clean, the row
    disappears from the middle's contract, and the flattened twin still refuses. 2149 tests
    stayed green under exactly this mutant before this test existed (V5-01)."""
    roots = _three_level_roots()
    caller, middle, child = _THREE_LEVEL_RE_CACHE
    flattened = _legs(*(list(caller) + list(middle) + list(child)))
    assert _errors(roots, "PARENT") != []
    _lineage_with_source(
        monkeypatch,
        "            if owes_the_retrieve_on_failure and cached_from is not None:",
        "            if False:")
    assert _errors(roots, "PARENT") == []
    assert ("$ref:CACHE2", "Y", None, True) not in _row(
        roots, "PARENT", "MID").cache_property_requirements
    assert _errors([("PARENT", flattened)], "PARENT") != []


# --- B21A-R3-SOUND-01: an unwaited call is not ordered before the removal -------------------

@pytest.mark.parametrize("call_flags, admitted", (
    ({"wait": True, "abort_on_error": True}, True),
    ({"wait": True}, True),
    ({"wait": False}, False),
), ids=("waited_and_aborting", "waited_without_abort", "not_waited"))
def test_the_repeat_exemption_asks_whether_the_childs_own_calls_are_waited_for(call_flags, admitted):
    """B21A-R3-SOUND-01. The exemption is a normal-exit guarantee about a cache, and amendment 1
    rule 7 withholds those across an asynchronous call: a child that is not waited for may still
    be running when this process finishes, so a whole-cache removal authored after it does not
    provably follow its writes, and the next run may find them.

    `abort_on_error` is deliberately NOT part of this: a child this process continues past has
    still finished before the next leg runs, so the removal still follows it. The pair measured
    here is the one the finding asked for — the waited twin stays admitted, the asynchronous one
    is refused — plus that third spelling, which distinguishes the two questions."""
    child = _legs(_DYNAMIC_INTO_CACHE2, _BINDS2,
                  {"steps": [], "terminal": _call("EXTERNAL", **call_flags)}, _EMPTIES2)
    roots = _per_document(child, **_WAITS_AND_ABORTS_ON_ERROR)
    expected = [] if admitted else [
        (PROCESS_IR_CAPABILITY_PROCESS_CALL_REPEATED_RUN_UNSTABLE, _PER_DOCUMENT_CALL)]
    assert _both_routes(roots, "PARENT") == expected
    assert _row(roots, "PARENT", "CACHE_CHILD").required_caches_retain_nothing_it_stored is admitted


# --- B21A-R4-SOUND-01: the same composition, however deeply the async call is nested --------

#: A process whose one leg calls ``key``, with the flags given. The middle of the chains below.
def _a_middle_that_calls(key, **flags):
    return _legs({"steps": [], "terminal": _call(key, **flags)}, {"steps": [_MSG], "terminal": _STOP})


#: Writes CACHE2 on the documents it is handed. The call nobody waits for, in every depth.
_WRITES_CACHE2 = _legs({"steps": [_GET], "terminal": _PUT2}, {"steps": [_MSG], "terminal": _STOP})
_ASYNC = {"wait": False, "abort_on_error": False}

#: One asynchronous write into CACHE2, at four depths below the child that empties CACHE2 —
#: and the synchronous twin of the nested one, which nothing leaves in flight.
_ASYNC_DEPTHS = {
    "flat": ([("WRITER2", _WRITES_CACHE2)], {"steps": [], "terminal": _call("WRITER2", **_ASYNC)}),
    "nested": ([("MID", _a_middle_that_calls("WRITER2", **_ASYNC)), ("WRITER2", _WRITES_CACHE2)],
               _waited("MID")),
    "three_deep": ([("MID", _a_middle_that_calls("MIDP", **_WAITS_AND_ABORTS_ON_ERROR)),
                    ("MIDP", _a_middle_that_calls("WRITER2", **_ASYNC)),
                    ("WRITER2", _WRITES_CACHE2)], _waited("MID")),
    # The middle cannot NAME what it may still be writing — nothing derives EXTERNAL — and it
    # names no cache of its own either, so only the "an unknown cache" bit crosses and the
    # child reads it against its own vocabulary.
    "an_underivable_call": ([("MID", _a_middle_that_calls("EXTERNAL", **_ASYNC))], _waited("MID")),
}


@pytest.mark.parametrize("depth", sorted(_ASYNC_DEPTHS))
def test_an_asynchronous_write_is_carried_across_the_calls_that_hid_it(depth):
    """B21A-R4-SOUND-01. Round 4 decided this per process: the child's OWN unwaited call kept
    the cache in `may_hold_at_exit`, but the identical call one process deeper was invisible to
    it, so the same composition was refused flat and admitted nested — the depth of the nesting,
    not the behaviour, decided it.

    The fact now crosses the boundary: `ChildEntryContractV1.unwaited_cache_writes` states what
    a completion of a child may still be writing (its own unwaited calls, unioned with the same
    field of every child IT calls), and a caller unions it at the call site whether or not it
    waits — waiting for a process whose own write is in flight orders nothing. A child that
    cannot name those caches says so instead (`unwaited_writes_of_an_unknown_cache`), and its
    caller then takes its own observable caches, exactly as an underivable call is treated.

    Every depth is refused, like the flat spelling; the synchronous control below is admitted."""
    extra, call_leg = _ASYNC_DEPTHS[depth]
    child = _legs(_DYNAMIC_INTO_CACHE2, _BINDS2, call_leg, _EMPTIES2)
    roots = _per_document(child, **_WAITS_AND_ABORTS_ON_ERROR) + extra
    assert _both_routes(roots, "PARENT") == [
        (PROCESS_IR_CAPABILITY_PROCESS_CALL_REPEATED_RUN_UNSTABLE, _PER_DOCUMENT_CALL)]
    assert _row(roots, "PARENT", "CACHE_CHILD").required_caches_retain_nothing_it_stored is False


def test_a_nested_call_that_is_waited_for_leaves_nothing_in_flight():
    """The control the refusals above are measured against: the same three processes, with the
    middle WAITING for the writer. Nothing is in flight when the middle returns, the child's
    removal provably follows the write, and the composition stays admitted — so the carry is
    about the asynchrony and not about the nesting."""
    roots = _per_document(
        _legs(_DYNAMIC_INTO_CACHE2, _BINDS2, _waited("MID"), _EMPTIES2), **_WAITS_AND_ABORTS_ON_ERROR
    ) + [("MID", _a_middle_that_calls("WRITER2", **_WAITS_AND_ABORTS_ON_ERROR)),
         ("WRITER2", _WRITES_CACHE2)]
    assert _both_routes(roots, "PARENT") == []
    assert _row(roots, "PARENT", "CACHE_CHILD").required_caches_retain_nothing_it_stored is True
    assert _row(roots, "CACHE_CHILD", "MID").unwaited_cache_writes == ()


def test_the_carry_across_the_boundary_is_what_refuses_the_nested_spelling(monkeypatch):
    """Non-vacuity, both halves: with the union of the CHILD's own pending writes dropped, the
    nested spelling is admitted again while the flat one still refuses — the exact disagreement
    the finding measured — and with the unknown-cache bit dropped, the underivable middle is."""
    nested = _per_document(_legs(_DYNAMIC_INTO_CACHE2, _BINDS2, _waited("MID"), _EMPTIES2),
                           **_WAITS_AND_ABORTS_ON_ERROR) + _ASYNC_DEPTHS["nested"][0]
    flat = _per_document(
        _legs(_DYNAMIC_INTO_CACHE2, _BINDS2, _ASYNC_DEPTHS["flat"][1], _EMPTIES2),
        **_WAITS_AND_ABORTS_ON_ERROR) + _ASYNC_DEPTHS["flat"][0]
    underivable = _per_document(_legs(_DYNAMIC_INTO_CACHE2, _BINDS2, _waited("MID"), _EMPTIES2),
                                **_WAITS_AND_ABORTS_ON_ERROR) + _ASYNC_DEPTHS["an_underivable_call"][0]
    refused = [(PROCESS_IR_CAPABILITY_PROCESS_CALL_REPEATED_RUN_UNSTABLE, _PER_DOCUMENT_CALL)]
    with monkeypatch.context() as patch:
        _lineage_with_source(patch, "                unwaited_cache_writes.update(called.unwaited_cache_writes)",
                             "                pass")
        assert _errors(nested, "PARENT") == []
        assert _errors(flat, "PARENT") == refused
    with monkeypatch.context() as patch:
        _lineage_with_source(patch, "                    unwaited_cache_writes.update(cache_refs)",
                             "                    pass")
        assert _errors(underivable, "PARENT") == []
        assert _errors(flat, "PARENT") == refused
    assert _errors(nested, "PARENT") == refused
    assert _errors(underivable, "PARENT") == refused


# --- B21A-R5-CYC-01: a call cycle is not a way out of the rule ------------------------------

#: The cycle: a middle whose guarded second leg calls a process that calls the middle back.
#: Nothing can ORDER either of them, so neither could be derived before round 6.
def _a_cycle_through(key, back_to):
    return ([(key, _legs({"steps": [], "terminal": _call("WRITER2", **_ASYNC)},
                         {"steps": [], "terminal": _call("LOOP_B", **_WAITS_AND_ABORTS_ON_ERROR)},
                         {"steps": [_MSG], "terminal": _STOP})),
             ("LOOP_B", _legs({"steps": [], "terminal": _call(back_to, **_WAITS_AND_ABORTS_ON_ERROR)},
                              {"steps": [_MSG], "terminal": _STOP})),
             ("WRITER2", _WRITES_CACHE2)])


#: The decisive triple, plus the control: the SAME child (the published cleanup recipe) calling
#: the SAME middle, with the cycle present, absent, and replaced by an extra leg that closes no
#: cycle. Only the cycle differs.
_CYCLE_CELLS = {
    "no_cycle": [("MID", _a_middle_that_calls("WRITER2", **_ASYNC)), ("WRITER2", _WRITES_CACHE2)],
    "a_cycle_below_the_middle": _a_cycle_through("MID", "MID"),
    "an_extra_leg_that_closes_no_cycle": [
        ("MID", _legs({"steps": [], "terminal": _call("WRITER2", **_ASYNC)},
                      {"steps": [], "terminal": _call("LOOP_B", **_WAITS_AND_ABORTS_ON_ERROR)},
                      {"steps": [_MSG], "terminal": _STOP})),
        ("LOOP_B", _legs({"steps": [_MSG], "terminal": _STOP}, {"steps": [_MSG], "terminal": _STOP})),
        ("WRITER2", _WRITES_CACHE2)],
}


@pytest.mark.parametrize("cell", sorted(_CYCLE_CELLS))
def test_a_call_cycle_below_a_child_does_not_buy_it_the_repeat_exemption(cell):
    """B21A-R5-CYC-01. A root nothing can order — a cycle member, or anything that
    transitively calls one — derived no facts at all, so its contract said it requires
    nothing, writes nothing it cannot list and leaves nothing in flight. That is WEAKER than
    the row an absent ProcessIR gets, and adding one decision-guarded leg calling a process
    that calls back therefore admitted the composition amendment 1 rule 7 refuses.

    Such a root is now seeded with the contract an underivable call gets and derived once
    against those seeds, so its own obligations and entry form are real while everything the
    seeding hid stays unknown. All three cells are refused; the extra leg that closes no cycle
    shows the cycle is the only difference, and the control below keeps an ordinary chain
    untouched."""
    roots = _per_document(
        _legs(_DYNAMIC_INTO_CACHE2, _BINDS2, _waited("MID"), _EMPTIES2), **_WAITS_AND_ABORTS_ON_ERROR
    ) + _CYCLE_CELLS[cell]
    assert _both_routes(roots, "PARENT") == [
        (PROCESS_IR_CAPABILITY_PROCESS_CALL_REPEATED_RUN_UNSTABLE, _PER_DOCUMENT_CALL)]


def test_a_cycle_member_still_states_what_it_requires_of_its_callers():
    """The other half of the same rule, pre-existing and closed with it (§7): a child that
    retrieves a cache only its caller fills and binds a request path on what it retrieves owes
    that caller the property — and a call cycle underneath it used to erase the row. The
    caller that stores a document without the property is refused again, and the caller that
    stores it stays admitted, so the seeding restored the obligation rather than refusing
    everything."""
    child = _legs({"steps": [_READ2, _BOUND_GET], "terminal": _STOP},
                  {"steps": [], "terminal": _call("LOOP_B", **_WAITS_AND_ABORTS_ON_ERROR)})
    cycle = [("CACHE_CHILD", child),
             ("LOOP_B", _legs({"steps": [], "terminal": _call("CACHE_CHILD", **_WAITS_AND_ABORTS_ON_ERROR)},
                              {"steps": [_MSG], "terminal": _STOP}))]
    bare = {"steps": [_GET], "terminal": _PUT2}
    stores_x = {"steps": [_GET, _DYNAMIC_X], "terminal": _PUT2}
    refused = [("PARENT", _legs(bare, _waited("CACHE_CHILD")))] + cycle
    admitted = [("PARENT", _legs(stores_x, _waited("CACHE_CHILD")))] + cycle
    assert _errors(refused, "PARENT") == [(_NOT_ESTABLISHED, "/body/steps/0/legs/1/terminal/process_ref")]
    assert _errors(admitted, "PARENT") == []


def test_the_cycle_seed_is_what_refuses_it(monkeypatch):
    """Non-vacuity of the seed's four CLAIMS, measured on a cycle where nothing else refuses:
    every call waits and aborts, no process writes a cache asynchronously, and the members
    require nothing of their callers, so the only reason the caller cannot take the repeat
    exemption is that a cycle member's state and cache writes are not known. Emptying the
    claims admits it again; the acyclic twin, which is refused for the asynchronous write, is
    unaffected."""
    from boomi_mcp.authoring import process_ir_effects

    all_waited = [("MID", _a_middle_that_calls("LOOP_B", **_WAITS_AND_ABORTS_ON_ERROR)),
                  ("LOOP_B", _a_middle_that_calls("MID", **_WAITS_AND_ABORTS_ON_ERROR))]
    cyclic = _per_document(
        _legs(_DYNAMIC_INTO_CACHE2, _BINDS2, _waited("MID"), _EMPTIES2), **_WAITS_AND_ABORTS_ON_ERROR
    ) + all_waited
    acyclic = _per_document(
        _legs(_DYNAMIC_INTO_CACHE2, _BINDS2, _waited("MID"), _EMPTIES2), **_WAITS_AND_ABORTS_ON_ERROR
    ) + _CYCLE_CELLS["no_cycle"]
    assert _errors(cyclic, "PARENT") == [
        (PROCESS_IR_CAPABILITY_PROCESS_CALL_REPEATED_RUN_UNSTABLE, _PER_DOCUMENT_CALL)]
    monkeypatch.setattr(process_ir_effects, "_A_CYCLE_MEMBERS_CLAIMS", {})
    assert _errors(cyclic, "PARENT") == []
    assert _errors(acyclic, "PARENT") != []


def _effects_with_source(monkeypatch, old, new, occurrences=1):
    """Resolve every graph through `process_ir_effects` with ONE source edit and nothing else.

    The twin of `_lineage_with_source` for the derivation module: its source is executed
    afresh with ``old`` replaced by ``new``, and the resolver every helper here calls is
    rebound to the mutant's, so a rule that lives inside `_entry_contract_bindings` — which
    has no module-level name of its own — can be reverted and measured.
    """
    from boomi_mcp.authoring import process_ir_effects

    import test_issue_184_child_entries as entries

    source = Path(process_ir_effects.__file__).read_text(encoding="utf-8")
    assert source.count(old) == occurrences, (old, source.count(old))
    namespace = {"__name__": process_ir_effects.__name__, "__package__": process_ir_effects.__package__,
                 "__file__": process_ir_effects.__file__}
    exec(compile(source.replace(old, new), process_ir_effects.__file__, "exec"), namespace)  # noqa: S102
    monkeypatch.setattr(
        entries, "resolve_process_ir_effect_declarations",
        namespace["resolve_process_ir_effect_declarations"])


#: The CYC6-01 graph, as the verifier spelled it: the grandparent stores X-LESS documents in
#: CACHE2 and calls LOOP_A; LOOP_A fills CACHE2 with X-carrying documents and calls LOOP_B;
#: LOOP_B retrieves CACHE2, binds a request path on X, and calls LOOP_A back. The obligation
#: is between two members of ONE cycle, so it is only charged if a member's derived row
#: reaches the member that calls it.
def _an_obligation_inside_a_cycle(back_edge):
    loop_b_legs = [{"steps": [_READ2, _BOUND_GET], "terminal": _STOP}, _STORES_NOTHING]
    if back_edge:
        loop_b_legs.append(_waited("LOOP_A"))
    return [("PARENT", _passthrough_root({"steps": [_GET], "terminal": _PUT2},
                                         {"steps": [], "terminal": _call("LOOP_A", **_WAITS_AND_ABORTS_ON_ERROR)})),
            ("LOOP_A", _legs(_DYNAMIC_INTO_CACHE2, _waited("LOOP_B"), _STORES_NOTHING)),
            ("LOOP_B", _legs(*loop_b_legs))]


def test_an_obligation_between_two_members_of_one_cycle_reaches_the_caller():
    """CYC6-01. Round 6 derived each unordered root ONCE against a frozen seed map, so two
    members of one cycle saw each other as the `unknown` seed and `_discharge_child_contract`
    returned early: LOOP_A inherited none of LOOP_B's requirement, the row PARENT was handed
    was empty, and the whole request — which 5b5038c refused and whose flattened twin is
    refused — validated and compiled clean.

    The unordered set is now derived to a FIXED POINT: each pass reads the previous pass's
    rows, and what a member owes its callers only grows. The cyclic graph is now refused
    exactly where its acyclic twin is, with the row travelling."""
    cyclic, acyclic = _an_obligation_inside_a_cycle(True), _an_obligation_inside_a_cycle(False)
    refused = [(PROCESS_IR_CAPABILITY_PROCESS_CALL_REPEATED_RUN_UNSTABLE, "/body/steps/1/legs/1/terminal"),
               (_NOT_ESTABLISHED, "/body/steps/1/legs/1/terminal/process_ref")]
    for roots in (cyclic, acyclic):
        assert sorted(_both_routes(roots, "PARENT")) == sorted(refused)
        assert ("$ref:CACHE2", "X", None, True) in _row(roots, "PARENT", "LOOP_A").cache_property_requirements
        assert _both_routes(roots, "LOOP_A") == [] and _both_routes(roots, "LOOP_B") == []
    # CONTROL: a grandparent that stores the property is admitted, so the fixpoint charges the
    # obligation rather than refusing every caller of a cycle.
    satisfying = [("PARENT", _passthrough_root(
        {"steps": [_GET, _DYNAMIC_X], "terminal": _PUT2},
        {"steps": [], "terminal": _call("LOOP_A", **_WAITS_AND_ABORTS_ON_ERROR)}))] + cyclic[1:]
    assert _NOT_ESTABLISHED not in {code for code, _path in _both_routes(satisfying, "PARENT")}


def test_the_fixpoint_is_what_carries_the_obligation_out_of_the_cycle(monkeypatch):
    """Non-vacuity of the fixpoint itself: stop after the first pass — exactly what round 6
    did — and the cyclic graph is admitted again while its acyclic twin stays refused."""
    cyclic, acyclic = _an_obligation_inside_a_cycle(True), _an_obligation_inside_a_cycle(False)
    assert _errors(cyclic, "PARENT") != []
    _effects_with_source(
        monkeypatch,
        "        if all(previous[key] == facts[key] for key in unordered):\n            break\n",
        "        break\n")
    assert _errors(cyclic, "PARENT") == []
    assert _errors(acyclic, "PARENT") != []


# --- B21A-R3-TXT-01: what the published recipe leaves out -----------------------------------

def test_the_published_cleanup_recipe_is_what_the_server_admits():
    """B21A-R3-TXT-01. The third way out promises "every run then ends with nothing it stored
    left there", and the shape that follows it verbatim is refused when it ALSO calls a process
    the request carries no ProcessIR for after the removal: such a call may add to the cache
    after it. The refusal is right and the text was not, so both now name the condition, and
    both ways out of it are measured here — supply the called process's ProcessIR, or call it
    before the removal and wait for it."""
    from boomi_mcp.compiler.process_ir.semantic_validation import findings

    fill, use, removal = _DYNAMIC_INTO_CACHE2, _BINDS2, _EMPTIES2
    recipe = _legs(fill, use, removal)
    refused = [(PROCESS_IR_CAPABILITY_PROCESS_CALL_REPEATED_RUN_UNSTABLE, _PER_DOCUMENT_CALL)]
    assert _both_routes(_per_document(recipe, **_WAITS_AND_ABORTS_ON_ERROR), "PARENT") == []
    after = _legs(fill, use, removal, _waited("EXTERNAL"))
    assert _both_routes(_per_document(after, **_WAITS_AND_ABORTS_ON_ERROR), "PARENT") == refused
    before = _legs(fill, use, _waited("EXTERNAL"), removal)
    assert _both_routes(_per_document(before, **_WAITS_AND_ABORTS_ON_ERROR), "PARENT") == []
    derivable = [("PARENT", _per_document(after, **_WAITS_AND_ABORTS_ON_ERROR)[0][1]),
                 ("CACHE_CHILD", _legs(fill, use, removal, _waited("MID"))),
                 ("MID", _legs(_STORES_NOTHING, {"steps": [_MSG], "terminal": _STOP}))]
    assert _both_routes(derivable, "PARENT") == []
    served = findings.finding(
        PROCESS_IR_CAPABILITY_PROCESS_CALL_REPEATED_RUN_UNSTABLE, "error", "capability", "/body")
    for clause in ("no call it makes after that removal may write that cache",
                   "no call it makes without waiting may write that cache",
                   # The two round-6 corrections: the removal's own proof qualifier
                   # (B21A-R5-TXT-02) and the declared outside writer (B21A-R5-TXT-01).
                   "a whole-cache removal that runs whenever its leg does",
                   "no outside writer is declared for that cache"):
        assert clause in served.message, served.message
    for clause in ("move a call that stands after the removal to before it and wait for it",
                   "shows that process does not write that cache",
                   "takes no such exemption at all",
                   # B21A-R5-TXT-03: the way out names the entry change it needs, because a
                   # No Data child never receives its caller's documents.
                   "give it a Data Passthrough entry and take what it needs from the documents"):
        assert clause in served.remediation, served.remediation


# --- B21A-R4-TXT-01: the recipe states the predicate, clause by clause ----------------------

#: Writes the OTHER cache, so the contract derived from it names a cache the recipe is not
#: about. Supplying such a process's ProcessIR is what "helps" means in the served text.
_WRITES_THE_OTHER_CACHE = _legs({"steps": [_GET], "terminal": _PUT},
                                {"steps": [_MSG], "terminal": _STOP})

#: A retrieve of CACHE2 that AUTHORS the outside-writer flag, consumed by a Message. Only a
#: root whose own retrieve authors it receives the declaration's contract, so this leg is what
#: makes a declared writer of CACHE2 visible to the child at all.
_READS_CACHE2_AS_AN_OUTSIDE_WRITERS = {
    "steps": [{"kind": "cache_get", "cache_ref": "$ref:CACHE2", "external_writer": True}, _MSG],
    "terminal": _STOP}

#: Per clause of the served recipe, a request that satisfies it and one that breaks it, each
#: varying only that clause. The child always fills CACHE2, binds on what it retrieves and —
#: except in the first row — empties CACHE2 on a leg the walk proves runs.
_RECIPE_CLAUSES = {
    "a whole-cache removal follows its last write to it": (
        ((_EMPTIES2,), ()), ((), ())),
    "no call it makes after that removal may write that cache/a derivable writer": (
        ((_waited("WRITER2"), _EMPTIES2), (("WRITER2", _WRITES_CACHE2),)),
        ((_EMPTIES2, _waited("WRITER2")), (("WRITER2", _WRITES_CACHE2),))),
    "no call it makes after that removal may write that cache/one nothing derives": (
        ((_waited("EXTERNAL"), _EMPTIES2), ()),
        ((_EMPTIES2, _waited("EXTERNAL")), ())),
    # The ProcessIR is supplied in BOTH directions here: what decides the verdict is what the
    # contract derived from it says the process writes, not that it was supplied.
    "a call may write that cache when its contract says it writes it": (
        ((_EMPTIES2, _waited("OTHER")), (("OTHER", _WRITES_THE_OTHER_CACHE),)),
        ((_EMPTIES2, _waited("WRITER2")), (("WRITER2", _WRITES_CACHE2),))),
    "no call it makes without waiting may write that cache/its own": (
        (({"steps": [], "terminal": _call("OTHER", **_ASYNC)}, _EMPTIES2),
         (("OTHER", _WRITES_THE_OTHER_CACHE),)),
        (({"steps": [], "terminal": _call("WRITER2", **_ASYNC)}, _EMPTIES2),
         (("WRITER2", _WRITES_CACHE2),))),
    "no call it makes without waiting may write that cache/inside a process it calls": (
        ((_waited("MID"), _EMPTIES2),
         (("MID", _a_middle_that_calls("WRITER2", **_WAITS_AND_ABORTS_ON_ERROR)),
          ("WRITER2", _WRITES_CACHE2))),
        ((_waited("MID"), _EMPTIES2),
         (("MID", _a_middle_that_calls("WRITER2", **_ASYNC)), ("WRITER2", _WRITES_CACHE2)))),
    # The seventh condition, which no surface stated before round 6 (B21A-R5-TXT-01): a
    # DECLARED outside writer of that cache may refill it between runs, so the exemption is
    # withheld whatever the child's own calls do. Both sides author the same four legs — the
    # third retrieves CACHE2 with `external_writer` — and differ only in whether the request
    # declares the writer, which is what the walk reads.
    "no outside writer is declared for that cache": (
        ((_READS_CACHE2_AS_AN_OUTSIDE_WRITERS, _EMPTIES2), (), ()),
        ((_READS_CACHE2_AS_AN_OUTSIDE_WRITERS, _EMPTIES2), (), ("$ref:CACHE2",))),
    # ... and the half of that predicate the first row holds fixed: the DECLARATION is made in
    # both directions here and only the retrieve's `external_writer` flag varies, because a
    # declaration no retrieve names is recorded inert and establishes nothing
    # (B21A-R6-V6-01, measured).
    "no outside writer is declared for that cache/on a retrieve that names one": (
        (({"steps": [_READ2, _MSG], "terminal": _STOP}, _EMPTIES2), (), ("$ref:CACHE2",)),
        ((_READS_CACHE2_AS_AN_OUTSIDE_WRITERS, _EMPTIES2), (), ("$ref:CACHE2",))),
}


@pytest.mark.parametrize("clause", sorted(_RECIPE_CLAUSES))
def test_every_clause_of_the_published_recipe_is_pinned_in_both_directions(clause):
    """B21A-R4-TXT-01. The recipe used to name DERIVABILITY — "nothing this request cannot
    derive may write that cache", "every process it calls ... must have its ProcessIR in this
    request" — where the code asks whether the call MAY WRITE that cache. The two differ in
    both directions, and each direction is a request an author would be told to build: a
    derivable process whose contract says it writes the cache is refused although its ProcessIR
    was supplied, and one whose contract names another cache is admitted although it is called
    after the removal, or without waiting.

    Second instance of "a hand-written served recipe versus the predicate the code evaluates"
    (the first was B21A-R3-TXT-01), so the sentence is written from
    `_repetition_unstable_caches` and `_caches_a_call_may_write` and every clause of it is
    pinned here by a constructed request, admitted and refused."""
    admitted_side, refused_side = _RECIPE_CLAUSES[clause]
    # A side is `(the child's extra legs, the extra roots)`, and optionally the caches the
    # REQUEST declares an outside writer for — the seventh clause's only dimension.
    admitted_legs, admitted_roots = admitted_side[0], admitted_side[1]
    refused_legs, refused_roots = refused_side[0], refused_side[1]
    admitted_declared = admitted_side[2] if len(admitted_side) > 2 else ()
    refused_declared = refused_side[2] if len(refused_side) > 2 else ()
    refused = [(PROCESS_IR_CAPABILITY_PROCESS_CALL_REPEATED_RUN_UNSTABLE, _PER_DOCUMENT_CALL)]
    admitted_child = _legs(_DYNAMIC_INTO_CACHE2, _BINDS2, *admitted_legs)
    refused_child = _legs(_DYNAMIC_INTO_CACHE2, _BINDS2, *refused_legs)
    assert _both_routes(
        _per_document(admitted_child, **_WAITS_AND_ABORTS_ON_ERROR) + list(admitted_roots),
        "PARENT", admitted_declared) == []
    assert _both_routes(
        _per_document(refused_child, **_WAITS_AND_ABORTS_ON_ERROR) + list(refused_roots),
        "PARENT", refused_declared) == refused


def test_the_three_surfaces_state_the_predicate_the_code_evaluates():
    """B21A-R4-TXT-01, the other half: the served finding, the served `node.process_call`
    paragraph and PROCESS_IR_V1.md §3e all say what the walk asks — whether a call MAY WRITE
    that cache, answered by the contract derived from the called process — rather than whether
    the request can derive it, and each gives the first cause its own way out."""
    from boomi_mcp.compiler.process_ir.semantic_validation import findings

    served = findings.finding(
        PROCESS_IR_CAPABILITY_PROCESS_CALL_REPEATED_RUN_UNSTABLE, "error", "capability", "/body")
    paragraphs = " ".join(_served("node.process_call")["ordering_facts"])
    document = (_ROOT / "docs" / "architecture" / "PROCESS_IR_V1.md").read_text(encoding="utf-8")
    for surface, text in (("finding", served.message + " " + served.remediation),
                          ("node.process_call", paragraphs), ("PROCESS_IR_V1.md", document)):
        assert "may write that cache" in text or "may write it" in text, surface
        assert "cannot derive may write any cache" in text, surface
        # The first cause gets its own way out on every surface, and it is not the removal.
        assert "reads the cache before writing it" in text or \
            "reads that cache before writing it" in text, surface
    assert "no removal exempts that one" in served.remediation
    assert "has no such way out" in paragraphs
    assert "has no removal that exempts it" in document
    # ... and the declared outside writer is stated wherever the exemption is (B21A-R5-TXT-01).
    assert "no outside writer declared for that cache" in paragraphs
    assert "declares an outside writer of that cache" in document


# --- B21A-R3-OR-01: what the origin carry does NOT survive ---------------------------------

@pytest.mark.parametrize("between", ("a_message", "a_connector_call", "a_map"),)
def test_the_origin_of_a_re_cache_survives_only_a_step_that_hands_the_documents_on(between):
    """B21A-R3-OR-01, recorded rather than changed. The cohort's origin travels with the
    documents, and `_advance_stream` rebuilds the stream at a connector call and at a map before
    the count-preserving carry is reached, so both drop it — `map` in particular is in
    `_COUNT_PRESERVING_KINDS` and still loses it.

    The direction is fail-closed and identical at 5b5038c: the child is refused on its own walk
    with no row a caller could satisfy, while the flattened graph of the same legs runs for the
    caller that stores the property. Moving the carry past a connector call is a behaviour
    change — the documents there are the connector's output, not the caller's — so this pins
    what the walk does today rather than asserting what it should do."""
    step = {"a_message": _MSG, "a_connector_call": _GET, "a_map": _TO_P2}[between]
    child = _legs({"steps": [_READ, step], "terminal": _PUT2},
                  {"steps": [_READ2, _READS_X], "terminal": _STOP})
    roots = [("PARENT", _legs(_STAGES_X, _waited("CACHE_CHILD"))), ("CACHE_CHILD", child)]
    rows = {(row[0], row[1]) for row in _row(roots, "PARENT", "CACHE_CHILD").cache_property_requirements}
    twin = _errors([("PARENT", _legs(_STAGES_X, *_branch_legs(child)))], "PARENT")
    if between == "a_message":
        assert _both_routes(roots, "CACHE_CHILD") == []
        assert ("$ref:CACHE", "X") in rows and ("$ref:CACHE2", "X") in rows
        assert _both_routes(roots, "PARENT") == [] and twin == []
    else:
        # The recorded limit: nothing carries the origin across a step that rebuilds the stream,
        # so the child keeps its own refusal and names no cache to its callers. (A map of
        # documents whose profile nothing proves is refused for that too, which is its own
        # rule and not what this cell is about.)
        assert (_READ_BEFORE_WRITE, "/body/steps/0/legs/1/steps/1") in _both_routes(roots, "CACHE_CHILD")
        assert rows == set()
        # The flattened graph of the same legs reads the property fine: the disagreement is the
        # recorded limit. (A map of documents whose profile nothing proves is refused for THAT
        # in both graphs, which is its own rule and not what this cell is about.)
        assert _READ_BEFORE_WRITE not in {code for code, _path in twin}, twin


# --- B21A-R3-TI-01/-02: the guards, and the revision oracle --------------------------------

def test_the_two_derived_guards_see_every_spelling(tmp_path):
    """Non-vacuity for both sweeps, each against the spelling that used to escape it: a cohort
    built through a module attribute, an obligation recorded with `+=`, and a content-channel
    row appended by a site the table does not name."""
    module = tmp_path / "mutant.py"
    module.write_text("import lineage\n\n\ndef _fourth_builder():\n"
                      "    return lineage._Cohort(frozenset(), None, frozenset(), 'unknown')\n",
                      encoding="utf-8")
    assert _cohort_builders_in(tmp_path) == {("mutant.py", "_fourth_builder")}
    augmented = ("def _somewhere(node):\n"
                 "    read_cache_origins += [(node.source_path, 'X', '$ref:CACHE')]\n")
    assert _obligation_sites_in(augmented, {"read_cache_origins"}) == {("_somewhere", "read_cache_origins")}
    extended = ("def _elsewhere(node):\n"
                "    cache_requirement_refs.extend([('$ref:ESCAPED', None)])\n")
    assert _obligation_sites_in(extended, {"cache_requirement_refs"}) == {
        ("_elsewhere", "cache_requirement_refs")}
    # ... and a spelling that is NOT a mutation of one of them stays invisible.
    assert _obligation_sites_in("def _reader():\n    return read_cache_origins[0]\n",
                                {"read_cache_origins"}) == set()


def test_the_re_cache_carrier_moves_the_served_compiler_revision(monkeypatch):
    """B21A-R3-TI-02. The revision oracle's `child_forwarding` row now carries the mechanism this
    batch added, so an edit to the carrier cannot change what the server accepts while the served
    revision stands still — the defect class `revision-oracle-omits-changed-behaviour` the
    revision suite exists for. Measured with the two mutants the verifier used: neither moved the
    revision before the row gained its cases."""
    from boomi_mcp.authoring import contract as authoring_contract

    baseline = authoring_contract._compiler_revision()
    with monkeypatch.context() as patched:
        _lineage_with_source(
            patched,
            "        for token in writers.get(key) or ():\n",
            "        for token in ():\n")
        assert authoring_contract._compiler_revision() != baseline
    with monkeypatch.context() as patched:
        _lineage_with_source(
            patched,
            "        for origin in (origins if scope == DDP else ()):\n",
            "        for origin in ():\n")
        assert authoring_contract._compiler_revision() != baseline
    assert authoring_contract._compiler_revision() == baseline


# --- CDX-184-r21-01: a recursive document requirement is finite -----------------------------

#: A map that consumes the caller's documents: the passthrough child's own consumer, whose
#: profile is what its callers must hand over.
_CONSUMES_P2 = {"kind": "map_ref", "map_ref": "$ref:M22"}


def _recursive_passthrough_child(consumer=_CONSUMES_P2):
    """The reviewer's shape: a Data Passthrough child with one Branch leg that CONSUMES a
    profile and another, Decision-guarded, that calls the child back."""
    return _passthrough_root(
        {"steps": [consumer], "terminal": _STOP},
        {"steps": [], "terminal": _decision(_call("CHILD", **_WAITS_AND_ABORTS_ON_ERROR))},
        {"steps": [_MSG], "terminal": _STOP},
    )


def _recursive_roots(consumer=_CONSUMES_P2):
    return [("PARENT", _parent([], _call("CHILD", **_WAITS_AND_ABORTS_ON_ERROR))),
            ("CHILD", _recursive_passthrough_child(consumer))]


def test_a_recursive_document_requirement_settles_on_the_distinct_consumptions():
    """CDX-184-r21-01. `document_requirements` is POSITIONAL — one entry per consumer of the
    caller's documents — so it cannot join the set-union the other obligation fields carry, and
    round 7 left it out of the carry entirely. For a child that calls ITSELF that never
    settles: each pass appends the child's own consumer requirement and then everything the
    self-call hands back, so the tuple grew by one entry per pass and the fixpoint's bound —
    a backstop against a rule that does not converge — was reached on a request the parser
    accepts. Resolution raised `RuntimeError` where 5b5038c returned an unknown contract.

    The finite representation is the DISTINCT requirements in first-seen order: each entry is
    a profile the caller's documents must match, so the same profile twice is the same
    obligation. This asserts the SERVED OUTCOME — a contract with those entries, and the
    verdicts every root receives — not merely the absence of a crash."""
    roots = _recursive_roots()
    row = _row(roots, "PARENT", "CHILD")
    assert row.entry_form == "passthrough"
    # P2, because the child's own leg consumes it; then `None`, the consumption the self-call
    # states nothing about — kept once, so the fail-closed reading survives deduplication.
    assert row.document_requirements == ("$ref:P2", None), row.document_requirements
    assert _both_routes(roots, "PARENT") == []
    assert _both_routes(roots, "CHILD") == []
    # ... and the acyclic twin of the same child states the same single consumption.
    acyclic = [("PARENT", _parent([], _call("CHILD", **_WAITS_AND_ABORTS_ON_ERROR))),
               ("CHILD", _passthrough_root({"steps": [_CONSUMES_P2], "terminal": _STOP},
                                           {"steps": [_MSG], "terminal": _STOP}))]
    assert _row(acyclic, "PARENT", "CHILD").document_requirements == ("$ref:P2",)
    assert _both_routes(acyclic, "PARENT") == []


@pytest.mark.parametrize("shape", ("a_self_call", "a_two_member_cycle", "an_unguarded_self_call"))
def test_every_recursive_spelling_resolves_to_a_contract(shape):
    """The siblings, each of which raised before: the back edge spelled as a Decision arm, as a
    plain leg terminal, and through a partner. Every one resolves and every root is judged."""
    if shape == "a_two_member_cycle":
        roots = [("PARENT", _parent([], _call("LOOP_A", **_WAITS_AND_ABORTS_ON_ERROR))),
                 ("LOOP_A", _passthrough_root(
                     {"steps": [_CONSUMES_P2], "terminal": _STOP},
                     {"steps": [], "terminal": _decision(_call("LOOP_B", **_WAITS_AND_ABORTS_ON_ERROR))},
                     {"steps": [_MSG], "terminal": _STOP})),
                 ("LOOP_B", _passthrough_root(
                     {"steps": [_CONSUMES_P2], "terminal": _STOP},
                     {"steps": [], "terminal": _decision(_call("LOOP_A", **_WAITS_AND_ABORTS_ON_ERROR))},
                     {"steps": [_MSG], "terminal": _STOP}))]
        child = "LOOP_A"
    elif shape == "an_unguarded_self_call":
        roots = [("PARENT", _parent([], _call("CHILD", **_WAITS_AND_ABORTS_ON_ERROR))),
                 ("CHILD", _passthrough_root(
                     {"steps": [_CONSUMES_P2], "terminal": _STOP},
                     {"steps": [], "terminal": _call("CHILD", **_WAITS_AND_ABORTS_ON_ERROR)},
                     {"steps": [_MSG], "terminal": _STOP}))]
        child = "CHILD"
    else:
        roots, child = _recursive_roots(), "CHILD"
    assert _row(roots, "PARENT", child).document_requirements == ("$ref:P2", None)
    for key, _document in roots:
        assert _both_routes(roots, key) == [], key


def test_the_finite_representation_is_what_makes_the_fixpoint_settle(monkeypatch):
    """Non-vacuity: with `document_requirements` taken from the latest derivation instead of
    carried as its distinct entries — round 7's state — the recursive child re-states its
    requirement with one more repeat on every pass: the same entries as a set, a longer tuple.
    That used to run into the fixpoint's pass count. Correction batch 22 removed the count, so
    it is now refused the moment it happens, by the check that every pass only GROWS a row,
    naming the field — never answered, and never looped on."""
    from boomi_mcp.authoring import process_ir_effects

    roots = _recursive_roots()
    assert _row(roots, "PARENT", "CHILD").document_requirements == ("$ref:P2", None)
    monkeypatch.setattr(process_ir_effects, "_OBLIGATION_FIELDS", tuple(
        field for field in process_ir_effects._OBLIGATION_FIELDS
        if field != "document_requirements"))
    monkeypatch.setattr(process_ir_effects, "_PERMISSIVE_FIELDS",
                        process_ir_effects._PERMISSIVE_FIELDS + ("document_requirements",))
    # A WATCHDOG, not a bound on the code under test: with no pass count left, a regression
    # in the check this pins would loop forever, and a hung run is a poor way to fail. Forty
    # derivations is far beyond the two passes the check needs to fire here.
    real, derivations = process_ir_effects.derive_child_entry_facts, []

    def watched(child_ir, symbols, capabilities=None):
        derivations.append(child_ir)
        assert len(derivations) < 40, "the fixpoint kept looping: the growth check did not fire"
        return real(child_ir, symbols, capabilities)

    monkeypatch.setattr(process_ir_effects, "derive_child_entry_facts", watched)
    with pytest.raises(RuntimeError, match=r"not monotone: pass 2, root 'CHILD', field 'document_requirements'"):
        _row(roots, "PARENT", "CHILD")


# --- correction batch 22: the fixpoint ends on growth, never on a pass count ------------------

def _relay_symbols():
    """The module's symbols plus enough document caches for the longest relay below."""
    from boomi_mcp.compiler.process_ir.contracts import ComponentSymbolV1, SymbolTableV1

    return SymbolTableV1(symbols=tuple(_symbols().symbols) + tuple(
        ComponentSymbolV1(ref="$ref:RC%d" % index, component_id="RC%d" % index, component_type="documentcache")
        for index in range(21)))


def _re_caches(indexes):
    """One Branch leg per index: retrieve cache RC(i) and store what it holds in RC(i-1)."""
    return [{"steps": [{"kind": "cache_get", "cache_ref": "$ref:RC%d" % index}],
             "terminal": {"kind": "cache_put", "cache_ref": "$ref:RC%d" % (index - 1)}} for index in indexes]


#: The use at the end of every relay: an ordinary read of X on documents retrieved from RC0.
_READS_X_FROM_RC0 = {"steps": [{"kind": "cache_get", "cache_ref": "$ref:RC0"}, _READS_X], "terminal": _STOP}


def _relay(shape, k):
    """A re-cache relay over caches RC0..RC(k). Each re-cache leg turns an obligation its callee
    states on RC(i-1) into one on RC(i), so the obligation travels ONE cache per pass of the
    fixpoint — and once more around the ring for every member it has to cross."""
    if shape == "a_guarded_passthrough_self_relay":  # the reviewer's spelling
        return [("LOOP_A", _passthrough_root(
            _READS_X_FROM_RC0, *_re_caches(range(1, k + 1)),
            {"steps": [], "terminal": _decision(_call("LOOP_A", **_WAITS_AND_ABORTS_ON_ERROR))}))]
    if shape == "a_self_relay":
        return [("LOOP_A", _legs(_READS_X_FROM_RC0, *_re_caches(range(1, k + 1)), _waited("LOOP_A")))]
    if shape == "a_two_member_relay":  # LOOP_A re-caches the odd caches, LOOP_B the even ones
        return [("LOOP_A", _legs(*_re_caches(range(1, k + 1, 2)), _waited("LOOP_B"))),
                ("LOOP_B", _legs(_READS_X_FROM_RC0, *_re_caches(range(2, k + 1, 2)), _waited("LOOP_A")))]
    assert shape == "a_three_member_ring"  # LOOP_A -> LOOP_B -> MID -> LOOP_A; only LOOP_A re-caches
    return [("LOOP_A", _legs(_READS_X_FROM_RC0, *_re_caches(range(1, k + 1)), _waited("LOOP_B"))),
            ("LOOP_B", _legs({"steps": [_MSG], "terminal": _STOP}, _waited("MID"))),
            ("MID", _legs({"steps": [_MSG], "terminal": _STOP}, _waited("LOOP_A")))]


#: ``shape -> (caches re-cached, passes the fixpoint takes, the root whose row for LOOP_A is read)``.
#: The pass counts are the verifiers' measurement with ONLY the old bound lifted (correction batch
#: 22 pre-commit verification, static lens E4/E5): 10, 22, 16 and 25, against old bounds of 9, 9,
#: 15 and 21.
_WITNESSED_RELAYS = {
    "a_guarded_passthrough_self_relay": (8, 10, "LOOP_A"),
    "a_self_relay": (20, 22, "LOOP_A"),
    "a_two_member_relay": (13, 16, "LOOP_B"),
    "a_three_member_ring": (7, 25, "MID"),
}
#: The longest relay of each shape the old pass counter still let settle.
_THE_OLD_COUNTER_LAST_SETTLED = {
    "a_guarded_passthrough_self_relay": 7, "a_self_relay": 7, "a_two_member_relay": 12, "a_three_member_ring": 5,
}
#: The counter this batch removed, put back into the working source: 3 plus 6 per unordered root.
_THE_OLD_PASS_COUNTER = (
    "    passes = 0\n    while unordered:\n        passes += 1\n",
    "    passes = 0\n    while unordered:\n        passes += 1\n"
    "        if passes > 3 + len(unordered) * 6:\n"
    "            raise RuntimeError('did not settle')\n",
)


def _relay_verdicts(roots, declarations=None):
    """``(resolution, {root: errors})`` under the relay symbols, every root validated AND compiled
    under the context the resolver serves it — through whichever resolver `_effects_with_source`
    has installed."""
    import test_issue_184_child_entries as entries
    from boomi_mcp.compiler.process_ir.diagnostics import ProcessIRCompileError
    from boomi_mcp.compiler.process_ir.pipeline import compile_process_ir_v1

    symbols = _relay_symbols()
    parsed = [(key, parse_process_ir_v1(document)) for key, document in roots]
    resolution = entries.resolve_process_ir_effect_declarations(
        parsed, declarations, symbols, [], child_roots={"$ref:" + key: ir for key, ir in parsed})
    assert resolution.ok, resolution.findings
    verdicts = {}
    for key, ir in parsed:
        capabilities = resolution.capabilities_by_root[key] or DEFAULT_VALIDATION_CAPABILITIES
        validated = sorted((item.code, item.path)
                           for item in validate_process_ir(ir, symbols, capabilities=capabilities).errors)
        try:
            compile_process_ir_v1(ir, symbols, capabilities=capabilities)
            compiled = []
        except ProcessIRCompileError as exc:
            compiled = sorted((item.code, item.path) for item in exc.diagnostics)
        assert compiled == validated, (key, validated, compiled)
        verdicts[key] = validated
    return resolution, verdicts


def _counting_derivations(monkeypatch):
    """How many times each process is derived — one per pass for a root nothing could order."""
    from collections import Counter

    from boomi_mcp.authoring import process_ir_effects

    counts, real = Counter(), process_ir_effects.derive_child_entry_facts

    def counting(child_ir, symbols, capabilities=None):
        counts[id(child_ir)] += 1
        return real(child_ir, symbols, capabilities)

    monkeypatch.setattr(process_ir_effects, "derive_child_entry_facts", counting)
    return counts


@pytest.mark.parametrize("shape", sorted(_WITNESSED_RELAYS))
def test_a_re_cache_relay_settles_however_many_caches_it_names(shape, monkeypatch):
    """Correction batch 22. The fixpoint stopped at a fixed 3 + 6 passes per unordered root and
    raised. A re-cache relay moves an obligation one cache per pass, so the passes it needs grow
    with the CACHES a request names — times the ring it travels — not with its roots: each of
    these parser-valid requests raised `RuntimeError` (so did `plan_authoring_request_v1`, on a
    nine-cache self-relay and an eight-cache two-member relay), while the same loop left to run
    settles and every root validates clean. Termination now rests on the rows only growing over
    the finite set of entries the request can name.

    Asserted: the SERVED outcome, on both resolver entry points — a derived contract (not the
    unknown seed) stating one cached-property row per cache the relay names; every root
    validated and compiled clean; and the pass count the verifiers measured with the bound
    lifted, so the answer is the lifted-bound answer and not merely an absence of a crash."""
    from boomi_mcp.models.authoring_workflow import ProcessIREffectDeclarationsV1

    k, passes, caller = _WITNESSED_RELAYS[shape]
    roots = _relay(shape, k)
    derivations = _counting_derivations(monkeypatch)
    resolution, verdicts = _relay_verdicts(roots)
    assert max(derivations.values()) == passes, dict(derivations)
    assert verdicts == {key: [] for key, _document in roots}
    row = resolution.capabilities_by_root[caller].child_entry_contract("$ref:LOOP_A")
    assert row.entry_form == ("passthrough" if "passthrough" in shape else "scheduled")
    assert sorted(row.cache_property_requirements) == sorted(
        ("$ref:RC%d" % index, "X", None, False) for index in range(k + 1))
    declared, declared_verdicts = _relay_verdicts(roots, ProcessIREffectDeclarationsV1())
    assert declared.capabilities_by_root == resolution.capabilities_by_root
    assert declared_verdicts == verdicts


@pytest.mark.parametrize("shape", sorted(_WITNESSED_RELAYS))
def test_the_pass_counter_this_replaced_refused_every_witnessed_relay(shape, monkeypatch):
    """Non-vacuity of the witness above: the old counter, put back into the working source,
    refuses each relay — and still settles the longest one it used to, with the very answer the
    working source gives, so the counter is the only thing that refused."""
    k, _passes, _caller = _WITNESSED_RELAYS[shape]
    shorter = _relay(shape, _THE_OLD_COUNTER_LAST_SETTLED[shape])
    expected = _relay_verdicts(shorter)
    _effects_with_source(monkeypatch, *_THE_OLD_PASS_COUNTER)
    with pytest.raises(RuntimeError, match="did not settle"):
        _relay_verdicts(_relay(shape, k))
    settled = _relay_verdicts(shorter)
    assert settled[1] == expected[1]
    assert settled[0].capabilities_by_root == expected[0].capabilities_by_root


def _a_self_calling_member():
    return [("LOOP_A", _legs({"steps": [_MSG], "terminal": _STOP}, _waited("LOOP_A")))]


#: A derivation made deliberately NON-MONOTONE in one field: what its first pass states, a later
#: pass takes back. ``field -> (first pass's facts, every later pass's facts)``.
_TAKEN_BACK = {
    "removed_caches": ({"removed_caches": ("$ref:CACHE2",)}, {"removed_caches": ()}),
    "guaranteed_state": ({"mutated_state": (("dpp", "K"),), "guaranteed_state": (("dpp", "K"),)},
                         {"mutated_state": (("dpp", "K"),), "guaranteed_state": ()}),
    "mutated_state": ({"mutated_state": (("dpp", "K"),)}, {"mutated_state": ()}),
    "entry_form": ({"entry_form": "passthrough"}, {"entry_form": "scheduled"}),
}


@pytest.mark.parametrize("field", sorted(_TAKEN_BACK))
def test_a_derivation_that_takes_a_fact_back_is_refused_naming_it(field, monkeypatch):
    """The invariant the termination argument rests on, shown to be CHECKED rather than assumed:
    a pass that shrinks a permissive field or changes a scalar is a derivation rule gone
    non-monotone — a code defect no parser-valid request reaches — and the fixpoint raises on
    the pass it happens, naming the root, the field and both values. It never answers with the
    unknown seed (which `_discharge_child_contract` skips, dropping every derived obligation)
    or with the last pass's rows (an under-approximation), and without the injected rule the
    same request settles."""
    from boomi_mcp.authoring import process_ir_effects

    roots = _a_self_calling_member()
    assert _relay_verdicts(roots)[1] == {"LOOP_A": []}
    first, later = _TAKEN_BACK[field]
    real, passes = process_ir_effects.derive_child_entry_facts, []

    def taking_back(child_ir, symbols, capabilities=None):
        # Keyed on the INPUT, never on a call count, so the behaviour corpus's harvest — which
        # replays the call under this very patch — sees the same rule: the first pass is the one
        # that still sees the member it calls as the `unknown` seed.
        called = capabilities.child_entry_contract("$ref:LOOP_A")
        passes.append("first" if called.entry_form == "unknown" else "later")
        return dict(real(child_ir, symbols, capabilities), **(first if passes[-1] == "first" else later))

    monkeypatch.setattr(process_ir_effects, "derive_child_entry_facts", taking_back)
    with pytest.raises(RuntimeError) as raised:
        _relay_verdicts(roots)
    assert str(raised.value) == "the entry-contract fixpoint is not monotone: pass 2, root 'LOOP_A', field {0!r}: " \
        "{1!r} -> {2!r}".format(field, first[field], later[field])
    assert passes[:2] == ["first", "later"]


def test_every_contract_field_moves_by_exactly_one_rule():
    """The classification the termination argument needs, pinned against the contract model
    itself: every tuple-valued field is carried as a union (LARGER is fail-closed) or taken
    latest and asserted to grow (LARGER is permissive), and every other field is a scalar the
    seed pins or the entry form, asserted equal from pass to pass."""
    from typing import get_origin

    from boomi_mcp.authoring import process_ir_effects

    fields = {name: field for name, field in ChildEntryContractV1.model_fields.items() if name != "process_ref"}
    tuples = {name for name, field in fields.items() if get_origin(field.annotation) is tuple}
    carried = set(process_ir_effects._OBLIGATION_FIELDS)
    permissive = set(process_ir_effects._PERMISSIVE_FIELDS)
    assert not carried & permissive
    assert carried | permissive == tuples
    assert permissive == {"removed_caches", "guaranteed_state", "mutated_state"}
    assert set(fields) - tuples == {"entry_form"} | set(process_ir_effects._A_CYCLE_MEMBERS_CLAIMS)


@pytest.mark.parametrize("mutant", ("a_field_left_unclassified", "a_field_classified_twice"))
def test_a_classification_that_misses_a_field_is_refused_before_the_first_pass(mutant, monkeypatch):
    """Non-vacuity of the classification check: a contract field no rule names — or two rules
    name — is refused on the first cyclic request rather than moving by an accident of which
    list forgot it."""
    from boomi_mcp.authoring import process_ir_effects

    if mutant == "a_field_left_unclassified":
        monkeypatch.setattr(process_ir_effects, "_PERMISSIVE_FIELDS", ("removed_caches", "guaranteed_state"))
        named = "mutated_state"
    else:
        monkeypatch.setattr(process_ir_effects, "_PERMISSIVE_FIELDS",
                            process_ir_effects._PERMISSIVE_FIELDS + ("unwaited_cache_writes",))
        named = "unwaited_cache_writes"
    with pytest.raises(RuntimeError, match=r"the contract fields \['{0}'\] are not each classified".format(named)):
        _row(_recursive_roots(), "PARENT", "CHILD")


# --- correction batch 22c: what the termination argument rests on ----------------------------

_REMOVES_CACHE = {"steps": [], "terminal": {"kind": "cache_remove", "cache_ref": "$ref:CACHE"}}
_FILLS_CACHE_ON_A_PROVED_PATH = {"steps": [_MSG], "terminal": _PUT}

#: Two rings in which a member FILLS a cache on a path its walk proves and then calls into a
#: partner that REMOVES it — the removal arriving one hop per pass (the verification lenses'
#: `ring2_fill_then_partner_removes` and `ring3_removal_arrives_late`; MID stands in for the third
#: member). ``shape -> (roots, the pass on which the relaxed pin's guarantee is taken back)``.
_A_PARTNER_REMOVES_WHAT_A_MEMBER_FILLED = {
    "ring2_fill_then_partner_removes": ([
        ("LOOP_A", _passthrough_root(_FILLS_CACHE_ON_A_PROVED_PATH, _waited("LOOP_B"))),
        ("LOOP_B", _legs(_REMOVES_CACHE, _waited("LOOP_A")))], 2),
    "ring3_removal_arrives_late": ([
        ("LOOP_A", _passthrough_root(_FILLS_CACHE_ON_A_PROVED_PATH, _waited("LOOP_B"))),
        ("LOOP_B", _legs({"steps": [_MSG], "terminal": _STOP}, _waited("MID"))),
        ("MID", _legs(_REMOVES_CACHE, _waited("LOOP_A")))], 3),
}


@pytest.mark.parametrize("shape", sorted(_A_PARTNER_REMOVES_WHAT_A_MEMBER_FILLED))
def test_the_pinned_claims_are_what_keep_a_cycle_members_guarantee_monotone(shape, monkeypatch):
    """Correction batch 22c. The fixpoint's termination argument needs `guaranteed_state` to
    only grow, and the raw derivation does NOT give that: a waited call un-establishes every
    cache its callee's `removed_caches` names before applying the callee's guarantee, and a
    member's removals grow from the seed's () outward — so a caller's guarantee of a cache it
    filled is taken back on the pass its partner's removal arrives. It stays () for every cycle
    member only because each one calls a member pinned `cache_writes_known=False`, which blanks
    its `mutated_state` and with it the guarantee filter.

    Both halves: on the real module each ring settles, every member guarantees and lists
    nothing, and every root validates and compiles alike; with that ONE pin relaxed in memory,
    the same request is refused by the growth check naming `guaranteed_state` on the pass the
    removal arrives. A change that derives the claim for a cycle member fails here rather than
    in production."""
    from boomi_mcp.authoring import process_ir_effects

    roots, taken_back_on = _A_PARTNER_REMOVES_WHAT_A_MEMBER_FILLED[shape]
    resolution, _verdicts = _relay_verdicts(roots)
    members = [key for key, _document in roots]
    for caller in members:
        for row in resolution.capabilities_by_root[caller].child_entry_contracts:
            assert (row.mutated_state, row.guaranteed_state) == ((), ()), (caller, row)
    assert any(row.removed_caches for caller in members
               for row in resolution.capabilities_by_root[caller].child_entry_contracts)
    monkeypatch.setattr(process_ir_effects, "_A_CYCLE_MEMBERS_CLAIMS",
                        dict(process_ir_effects._A_CYCLE_MEMBERS_CLAIMS, cache_writes_known=True))
    with pytest.raises(RuntimeError) as raised:
        _relay_verdicts(roots)
    assert str(raised.value) == (
        "the entry-contract fixpoint is not monotone: pass {0}, root 'LOOP_A', field 'guaranteed_state': "
        "(('cache', '$ref:CACHE'),) -> ()".format(taken_back_on)), str(raised.value)


def test_the_re_applied_pin_is_what_keeps_the_retention_claim_still(monkeypatch):
    """The same dependency for a SCALAR. `required_caches_retain_nothing_it_stored` is asserted
    equal from pass to pass, and its raw derivation is not constant: over a two-member re-cache
    relay it flips from True to False between passes. Only the pin re-applied after every
    derivation holds it still — drop that re-application and the growth check names the field."""
    roots = _relay("a_two_member_relay", 13)
    assert _relay_verdicts(roots)[1] == {"LOOP_A": [], "LOOP_B": []}
    _effects_with_source(monkeypatch, "                **_A_CYCLE_MEMBERS_CLAIMS\n            )\n", "            )\n")
    with pytest.raises(RuntimeError, match=r"not monotone: pass \d+, root '\w+', field "
                                           r"'required_caches_retain_nothing_it_stored': True -> False"):
        _relay_verdicts(roots)


#: The source mutant the oracle measured and refused: seed a root the fixpoint cannot order with
#: its REAL entry form — read off its own CFG through the derivation itself — instead of `unknown`.
_A_REAL_FORM_SEED = (
    '        facts[key] = dict(_A_CYCLE_MEMBERS_CLAIMS, entry_form="unknown")\n',
    "        facts[key] = dict(_A_CYCLE_MEMBERS_CLAIMS, entry_form=derive_child_entry_facts(\n"
    "            roots[key], symbols_for(key, roots[key]), ProcessIRValidationCapabilitiesV1())['entry_form'])\n",
)


def _rc(index):
    return "$ref:RC%d" % index


def _a_self_calling_member_that_re_reads_what_it_stores():
    """Adversarial fuzz seed 1648 of the batch-22 termination lens, in this module's builders: a No
    Data member that stores documents carrying X in RC5 and in RC2, calls itself without waiting
    and without aborting, re-reads RC5 into a per-document self-call, and binds a request path on
    X off RC5. PARENT stores in RC1 and calls it once per arriving document."""
    member = _legs(
        {"steps": [_GET, _STATIC_X], "terminal": {"kind": "cache_put", "cache_ref": _rc(5)}},
        {"steps": [], "terminal": _call("LOOP_A", wait=False, abort_on_error=False)},
        {"steps": [_GET, _STATIC_X], "terminal": {"kind": "cache_put", "cache_ref": _rc(2)}},
        {"steps": [], "terminal": _call("LOOP_A", wait=True, abort_on_error=False)},
        {"steps": [{"kind": "cache_get", "cache_ref": _rc(5)}], "terminal": _call("LOOP_A", **_WAITS_AND_ABORTS_ON_ERROR)},
        {"steps": [{"kind": "cache_get", "cache_ref": _rc(5)}, _BOUND_GET], "terminal": _STOP})
    parent = _passthrough_root({"steps": [_GET, _DYNAMIC_X], "terminal": {"kind": "cache_put", "cache_ref": _rc(1)}},
                               {"steps": [], "terminal": _call("LOOP_A", **_WAITS_AND_ABORTS_ON_ERROR)})
    return [("LOOP_A", member), ("PARENT", parent)]


def _with_the_back_calls_removed(roots):
    """The acyclic twin: every call a member makes to itself spelled Message -> Stop instead."""
    import copy

    twin = copy.deepcopy(roots)
    for leg in twin[0][1]["body"]["steps"][0]["legs"]:
        if leg["terminal"].get("process_ref") == "$ref:LOOP_A":
            leg["terminal"], leg["steps"] = _STOP, leg["steps"] or [_MSG]
    return twin


def test_the_unknown_seed_is_what_refuses_a_caller_its_acyclic_twin_refuses(monkeypatch):
    """Correction batch 22c, the measurement that kept the seed as it is. Seeding a root the
    fixpoint cannot order with its real entry form removes the recorded over-refusal (the next
    witness) — but the `unknown` seed's first pass also states an obligation a real-form seed
    never grounds, and here it is the only thing refusing the member's caller. PARENT calls the
    member once per document and the member's own row asks nothing of RC5 once its seed is real,
    so the repeated-run question finds nothing unstable and PARENT is ADMITTED — while the acyclic
    twin, whose member states the cached-property use as its callers' obligation, refuses PARENT
    `PROCESS_IR_CAPABILITY_PROCESS_CALL_REPEATED_RUN_UNSTABLE` exactly as the real module does. An
    admission the twin refuses is the unsound direction, so the change was reverted
    (oracle `b22c/impl/oracle.py`, one such request among the adversarial corpus's 3,008)."""
    roots = _a_self_calling_member_that_re_reads_what_it_stores()
    refused = [(PROCESS_IR_CAPABILITY_PROCESS_CALL_REPEATED_RUN_UNSTABLE, "/body/steps/1/legs/1/terminal")]
    assert _relay_verdicts(roots)[1]["PARENT"] == refused
    assert _relay_verdicts(_with_the_back_calls_removed(roots))[1]["PARENT"] == refused
    _effects_with_source(monkeypatch, *_A_REAL_FORM_SEED)
    assert _relay_verdicts(roots)[1]["PARENT"] == []


def test_the_unknown_seed_s_recorded_over_refusal():
    """The limit the unknown seed keeps, pinned so a change to it is a diff (verification lens
    `b22c/other/b_seed_artifact.py`). A member whose partner is a No Data process states one real
    requirement from its second pass on, but its first pass read the partner as the `unknown`
    seed — an unknown CONSUMPTION of what it hands over — and the union carry keeps that `None`,
    so PARENT's prefixed call is refused `PROCESS_IR_CAPABILITY_PROCESS_CALL_PLACEMENT_UNSUPPORTED`
    while the acyclic twin, in which the partner does not call back, is admitted. Fail-closed:
    refused where it could ship, never shipped where it must not; 260eb8c admitted it, and
    `5b5038c` refused it with the same code."""
    parent = ("PARENT", _passthrough_root({"steps": [{"kind": "map_ref", "map_ref": "$ref:M12"}],
                                           "terminal": _call("LOOP_A", **_WAITS_AND_ABORTS_ON_ERROR)},
                                          {"steps": [_MSG], "terminal": _STOP}))
    member = ("LOOP_A", _passthrough_root(_waited("LOOP_B"),
                                          {"steps": [{"kind": "map_ref", "map_ref": "$ref:M22"}], "terminal": _STOP}))
    cyclic = [parent, member, ("LOOP_B", _legs(_waited("LOOP_A"), {"steps": [_MSG], "terminal": _STOP}))]
    acyclic = [parent, member, ("LOOP_B", _legs({"steps": [_MSG], "terminal": _STOP},
                                                {"steps": [_MSG], "terminal": _STOP}))]
    assert _row(cyclic, "PARENT", "LOOP_A").document_requirements == (None, "$ref:P2")
    assert _both_routes(cyclic, "PARENT") == [
        (PROCESS_IR_CAPABILITY_PROCESS_CALL_PLACEMENT_UNSUPPORTED, "/body/steps/1/legs/0/terminal")]
    assert _row(acyclic, "PARENT", "LOOP_A").document_requirements == ("$ref:P2",)
    assert _both_routes(acyclic, "PARENT") == []
