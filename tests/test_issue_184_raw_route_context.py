"""#184 architect finding 7: the raw ``integration_spec`` route compiles under the per-root trusted context.

Base plan §4 and amendment 1 §3 require the same derived child capabilities "through raw
canonical preflight and dry emission" and build recording; amendment 3 §8 keeps that
obligation and the passthrough ``wait=false`` refusal at ``/…/wait`` (A6). The typed route
derived the context; the raw route's plan builder compiled the same roots strict. One
two-root request was therefore admitted typed and refused raw, and an unwaited passthrough
call was refused typed and CREATED raw.

The structural fix is one derivation, ``authoring.workflow.resolve_root_context``, which
both routes call: the typed route from ``_validate_processes``, the raw route from inside
``_build_canonical_plan`` (through ``derive_root_capabilities``), so neither of its two
callers can omit it. #180's source-derived sweep enforces the call now that its stale
exemption is gone, and the derivation sweep below pins every other resolver call in ``src/``.

Expected codes and pointers come from the plan text and the captures the child-entries
module cites, and the parity sweep compares against the TYPED route's verdicts, never this
route's own output. The requests are the finding's own spelling and the child-entries
module's fixtures:

- `cap184-prefix-predecessors`: a prefix admits a passthrough child with wait=true only.
- `cap184-passthrough-wait-true`: wait=false stays refused at `/wait`.
- `cap184-passthrough-standalone`: run directly, a passthrough process runs as No Data.
"""

from __future__ import annotations

import ast
import copy
import sys
from pathlib import Path
from unittest.mock import MagicMock

import pytest

_HERE = Path(__file__).resolve().parent
_ROOT = _HERE.parent
for _p in (str(_ROOT), str(_ROOT / "src"), str(_HERE)):
    if _p not in sys.path:
        sys.path.insert(0, _p)

from boomi_mcp.authoring import workflow  # noqa: E402
from boomi_mcp.categories import integration_builder  # noqa: E402
from boomi_mcp.categories.integration_builder import (  # noqa: E402
    _BUILD_REGISTRY,
    build_integration_action,
)
from boomi_mcp.compiler.process_ir.diagnostics import ProcessIRCompileError  # noqa: E402
from boomi_mcp.errors import (  # noqa: E402
    PROCESS_IR_CAPABILITY_ENTRY_CONTEXT_UNSUPPORTED,
    PROCESS_IR_CAPABILITY_PROCESS_CALL_PLACEMENT_UNSUPPORTED,
)
from boomi_mcp.recipes import materialization  # noqa: E402

# The #158 deployment suite's two autouse fixtures, imported so they apply here too: the
# build registry is restored after each test, and the metadata pager is stubbed, because
# unstubbed over a MagicMock client it never terminates.
from test_issue_158_listener_deployment import (  # noqa: E402,F401
    _PROFILE,
    _ApplyBoundary,
    _deploy,
    _error_codes,
    _no_live_metadata_queries,
    _registry_restored,
    _request,
    _unit,
)
from test_issue_184_child_entries import (  # noqa: E402
    _BOUND,
    _BOUND_GET,
    _CHILD,
    _CHILD_P1,
    _DYNAMIC_X,
    _ENTRY,
    _MAP,
    _MSG,
    _MUTATES_K,
    _NEEDS_K,
    _NODATA,
    _P2_PREFIX,
    _SET_K,
    _SPLIT,
    _STATIC_X,
    _STOP,
    _branch,
    _call,
    _decision,
    _doc,
    _legs,
    _parent,
    _put,
    _read_cache,
    _symbols,
)

_PLACEMENT = PROCESS_IR_CAPABILITY_PROCESS_CALL_PLACEMENT_UNSUPPORTED
_ENTRY_CONTEXT = PROCESS_IR_CAPABILITY_ENTRY_CONTEXT_UNSUPPORTED

#: The finding's child: Data Passthrough, then a Message, then Stop. It needs nothing.
_ENRICH = _doc(dict(_ENTRY, label="E184 child"), _MSG, _STOP)
#: A Data Passthrough root that reads a process property only a caller sets.
_READS_K = _doc(dict(_ENTRY, label="E184 reads K"), {
    "kind": "set_dpp", "name": "OUT", "source_values": [{"value_type": "dpp", "property_name": "K"}]}, _STOP)


def _scheduled(prefix, terminal):
    """A SCHEDULED parent: no entry step, so step 0 is the Branch (the finding's parent)."""
    return _doc(_branch(prefix, terminal))


def _per_document(child_key):
    """A passthrough parent whose second leg calls ``child_key`` once per arriving document."""
    return _doc(_ENTRY, {"kind": "branch", "legs": [
        {"steps": [_SET_K], "terminal": _STOP}, {"steps": [], "terminal": _call(child_key)}]})


# ---------------------------------------------------------------------------
# the public raw route
# ---------------------------------------------------------------------------


def _raw_spec(*roots):
    """An ``integration_spec`` of canonical roots, each ``(key, document, depends_on)``."""
    return {"name": "E184 raw route", "components": [], "processes": [
        _unit(doc, depends_on, key=key, name="E184 " + key).model_dump(mode="json")
        for key, doc, depends_on in roots]}


#: The finding's request: a scheduled parent's Branch leg hands a Message to the child.
_ATTESTED = _raw_spec(("root", _scheduled([_MSG], _call("child")), ("child",)), ("child", _ENRICH, ()))
#: The same child called without waiting, through an empty leg.
_UNWAITED = _raw_spec(("root", _scheduled([], _call("child", wait=False)), ("child",)), ("child", _ENRICH, ()))


def _raw_apply(spec, boundary):
    """plan, then a wet apply, of one ``integration_spec`` through the public dispatcher."""
    with boundary.installed():
        planned = build_integration_action(
            MagicMock(), _PROFILE, "plan", config={"integration_spec": copy.deepcopy(spec)})
        applied = build_integration_action(
            MagicMock(), _PROFILE, "apply",
            config={"integration_spec": copy.deepcopy(spec), "dry_run": False})
    return planned, applied


def _two_roots():
    return _ApplyBoundary(root_ids=("e184-child-cid", "e184-root-cid"))


def test_the_raw_route_admits_an_attested_passthrough_prefix():
    boundary = _two_roots()
    planned, applied = _raw_apply(_ATTESTED, boundary)
    assert planned["_success"] is True, planned.get("error")
    assert applied["_success"] is True, (applied.get("error_code"), applied.get("hint"))
    assert boundary.created == ["e184-child-cid", "e184-root-cid"], boundary.created
    assert {key: step["component_id"] for key, step in applied["results"].items()} == {
        "child": "e184-child-cid", "root": "e184-root-cid"}, applied["results"]


def test_the_raw_route_refuses_an_unwaited_passthrough_call_before_any_write():
    boundary = _two_roots()
    _planned, applied = _raw_apply(_UNWAITED, boundary)
    assert applied["_success"] is False, applied
    assert (applied["error_code"], applied["failed_step"]) == (_ENTRY_CONTEXT, "root"), applied
    assert "Offending path: /body/steps/0/legs/0/terminal/wait." in applied["hint"], applied["hint"]
    assert boundary.created == [] and boundary.executed == {}, (boundary.created, boundary.executed)


def test_the_plan_both_raw_callers_consume_records_the_derived_context():
    """The pre-write pass and the in-loop fallback both build through `_build_canonical_plan`,
    so the context rides on the plan either way, where the dry emit and the wet apply
    recompile with it. A root nothing binds in keeps the strict compile."""
    from boomi_mcp.models.integration_models import IntegrationSpecV1

    spec = IntegrationSpecV1(**copy.deepcopy(_ATTESTED))
    units = {unit.envelope.component_key: unit for unit in spec.processes}
    resolution = integration_builder._request_only_resolution(spec)

    def plan(key):
        return integration_builder._build_canonical_plan(
            spec=spec, unit=units[key], conflict_policy="reuse", resolution=resolution, existing_ids={})

    row = plan("root").effect_capabilities.child_entry_contract("$ref:child")
    assert (row.entry_form, row.document_requirements) == ("passthrough", ()), row
    assert plan("child").effect_capabilities.entry_contract.entry_form == "passthrough"
    scheduled = IntegrationSpecV1(**_raw_spec(("root", _scheduled([_MSG], _STOP), ())))
    alone = integration_builder._build_canonical_plan(
        spec=scheduled, unit=scheduled.processes[0], conflict_policy="reuse",
        resolution=integration_builder._request_only_resolution(scheduled), existing_ids={})
    assert alone.effect_capabilities is None


# ---------------------------------------------------------------------------
# typed / raw parity over the child-entries matrix
# ---------------------------------------------------------------------------

#: A Data Passthrough child that splits its caller's documents: nothing states what it reads.
_OPAQUE = _doc(_ENTRY, _SPLIT, _STOP)
_GET = {"kind": "connector_call", "operation_ref": "$ref:GET"}
#: Staging legs: documents carrying X composed from a process property, documents without
#: X, and P2 documents, each stored in CACHE.
_STAGES_X = {"steps": [_GET, _DYNAMIC_X], "terminal": _put("$ref:CACHE")}
_STAGES_NO_X = {"steps": [_GET], "terminal": _put("$ref:CACHE")}
_STAGES_P2 = {"steps": [_MAP], "terminal": _put("$ref:CACHE")}
_CALLS_CACHE_CHILD = {"steps": [], "terminal": _call("CACHE_CHILD")}
#: A No Data child binding its request path to X of the documents it retrieves from CACHE.
_CACHED_X_CHILD = _doc(_read_cache("$ref:CACHE"), _BOUND_GET, _STOP)
#: A No Data child that reads CACHE in one leg and removes all of it in the next.
_READS_THEN_REMOVES = _legs(
    {"steps": [_read_cache("$ref:CACHE"), _MSG], "terminal": _STOP},
    {"steps": [], "terminal": {"kind": "cache_remove", "cache_ref": "$ref:CACHE"}},
)
#: A No Data child that sets K on both arms of a Decision, so every completion sets it.
_SETS_K_ON_EVERY_ARM = _doc(dict(
    _decision(_STOP),
    true_arm={"steps": [_SET_K], "terminal": _STOP},
    false_arm={"steps": [_SET_K], "terminal": _STOP},
))


def _per_document_after(first_leg, child_key):
    """A passthrough parent: ``first_leg`` runs, then a leg calls ``child_key`` per document."""
    return _doc(_ENTRY, {"kind": "branch", "legs": [first_leg, {"steps": [], "terminal": _call(child_key)}]})


def _calls_then_reads_k(abort_on_error):
    """A scheduled parent whose first leg calls WRITER and whose second reads K."""
    return _legs(
        {"steps": [], "terminal": _call("WRITER", wait=True, abort_on_error=abort_on_error)},
        {"steps": [{"kind": "set_dpp", "name": "OUT",
                    "source_values": [{"value_type": "dpp", "property_name": "K"}]}], "terminal": _STOP},
    )


#: ONE case list for the parity sweep and its load-bearing check: ``(id, roots)``, each root
#: ``(key, document)`` over the child-entries symbol table. A further case is one line.
_CHILD_MATRIX = [
    ("finding_scheduled_message_prefix", [("PARENT", _scheduled([_MSG], _call("ENRICH"))), ("ENRICH", _ENRICH)]),
    ("attested_prefix", [("PARENT", _parent(_P2_PREFIX, _call("CHILD"))), ("CHILD", _CHILD)]),
    ("profile_mismatch_at_the_call", [("PARENT", _parent(_P2_PREFIX, _call("CHILD_P1"))), ("CHILD_P1", _CHILD_P1)]),
    ("unwaited_passthrough_after_a_prefix", [("PARENT", _parent(_P2_PREFIX, _call("CHILD", wait=False))), ("CHILD", _CHILD)]),
    ("unwaited_passthrough_empty_prefix", [("PARENT", _scheduled([], _call("ENRICH", wait=False))), ("ENRICH", _ENRICH)]),
    ("unknown_child_after_a_prefix", [("PARENT", _parent(_P2_PREFIX, _call("EXTERNAL")))]),
    ("no_data_child_after_a_prefix", [("PARENT", _parent(_P2_PREFIX, _call("NODATA"))), ("NODATA", _NODATA)]),
    ("interposed_decision", [("PARENT", _parent(_P2_PREFIX, _decision(_call("CHILD_P1")))), ("CHILD_P1", _CHILD_P1)]),
    ("scheduled_empty_prefix_p2_requirement", [("PARENT", _scheduled([], _call("CHILD"))), ("CHILD", _CHILD)]),
    ("bound_child_dynamic_writer", [("PARENT", _parent([_DYNAMIC_X], _call("BOUND"))), ("BOUND", _BOUND)]),
    ("bound_child_static_writer", [("PARENT", _parent([_STATIC_X], _call("BOUND"))), ("BOUND", _BOUND)]),
    ("bound_child_without_writer", [("PARENT", _parent([], _call("BOUND"))), ("BOUND", _BOUND)]),
    ("no_data_mutation_per_document", [("PARENT", _per_document("MUTATES_K")), ("MUTATES_K", _MUTATES_K)]),
    ("no_data_stable_per_document", [("PARENT", _per_document("NEEDS_K")), ("NEEDS_K", _NEEDS_K)]),
    ("required_process_property", [("PARENT", _scheduled([], _call("NEEDS_K"))), ("NEEDS_K", _NEEDS_K)]),
    ("grandchild_chain", [("PARENT", _parent(_P2_PREFIX, _call("MID"))), ("MID", _parent(_P2_PREFIX, _call("CHILD"))), ("CHILD", _CHILD)]),
    # ARCH-184-r1-03: a passthrough parent keys the call behind an interposed control on
    # its native work; a scheduled parent keeps the legacy empty-prefix placement.
    ("interposed_decision_passthrough_child", [("PARENT", _parent(_P2_PREFIX, _decision(_call("CHILD")))), ("CHILD", _CHILD)]),
    ("interposed_decision_unknown_child", [("PARENT", _parent(_P2_PREFIX, _decision(_call("EXTERNAL"))))]),
    ("interposed_decision_no_data_child", [("PARENT", _parent(_P2_PREFIX, _decision(_call("NODATA")))), ("NODATA", _NODATA)]),
    ("interposed_branch_unknown_child", [("PARENT", _doc(_ENTRY, _MAP, _branch([], _call("EXTERNAL"))))]),
    ("scheduled_interposed_decision_no_data_child", [("PARENT", _scheduled([_MSG], _decision(_call("NODATA")))), ("NODATA", _NODATA)]),
    # ARCH-184-r1-04: a forwarder presents what its child's consumption leaves unstated.
    ("forwarder_to_an_unknown_child", [("PARENT", _parent(_P2_PREFIX, _call("MID"))), ("MID", _doc(_ENTRY, _call("EXTERNAL")))]),
    ("forwarder_to_an_opaque_child", [("PARENT", _parent(_P2_PREFIX, _call("MID"))), ("MID", _doc(_ENTRY, _call("ENRICH"))), ("ENRICH", _OPAQUE)]),
    # ARCH-184-r1-05: the document properties a caller stored with cached documents.
    ("cached_property_chain", [("PARENT", _legs(_STAGES_X, _CALLS_CACHE_CHILD)), ("CACHE_CHILD", _CACHED_X_CHILD)]),
    ("cached_property_chain_without_x", [("PARENT", _legs(_STAGES_NO_X, _CALLS_CACHE_CHILD)), ("CACHE_CHILD", _CACHED_X_CHILD)]),
    ("no_data_cache_removal_per_document", [("PARENT", _per_document_after(_STAGES_P2, "CACHE_CHILD")), ("CACHE_CHILD", _READS_THEN_REMOVES)]),
    # ARCH-184-r1-06: a waited abort-on-error child's guarantee reaches a later leg.
    ("guarantee_chain", [("PARENT", _calls_then_reads_k(abort_on_error=True)), ("WRITER", _SETS_K_ON_EVERY_ARM)]),
    ("guarantee_chain_without_abort", [("PARENT", _calls_then_reads_k(abort_on_error=False)), ("WRITER", _SETS_K_ON_EVERY_ARM)]),
]


def _matrix_request(roots):
    return _request([_unit(doc, (), key=key, name="E184 " + key) for key, doc in roots], [])


def _typed_verdicts(roots, monkeypatch):
    """The typed route's judge, `_validate_processes`: ``{root: {(code, path)}}`` of its errors."""
    normalized = workflow._normalize_intent(_matrix_request(roots))
    monkeypatch.setattr(materialization, "build_symbol_table", lambda *args, **kwargs: _symbols())
    diagnostics = workflow._validate_processes(normalized)[1]
    verdicts = {key: set() for key, _doc in roots}
    for item in diagnostics:
        if item.severity == "error":
            code = item.cause_codes[0] if item.cause_codes else item.code
            verdicts.setdefault(item.subject_id, set()).add((code, item.path))
    return verdicts


def _raw_verdicts(roots, monkeypatch):
    """The raw route's judge, the pre-write `_build_canonical_plan`: the same shape."""
    spec = workflow._normalize_intent(_matrix_request(roots)).integration_spec
    monkeypatch.setattr(integration_builder, "_build_canonical_symbols", lambda **kwargs: _symbols())
    resolution = integration_builder._request_only_resolution(spec)
    verdicts = {}
    for unit in spec.processes:
        key = unit.envelope.component_key
        try:
            integration_builder._build_canonical_plan(
                spec=spec, unit=unit, conflict_policy="reuse", resolution=resolution, existing_ids={})
        except ProcessIRCompileError as exc:
            verdicts[key] = {(item.code, item.path) for item in exc.diagnostics}
        else:
            verdicts[key] = set()
    return verdicts


@pytest.mark.parametrize("roots", [pytest.param(roots, id=case_id) for case_id, roots in _CHILD_MATRIX])
def test_the_raw_and_typed_routes_judge_the_child_matrix_alike(roots, monkeypatch):
    assert _raw_verdicts(roots, monkeypatch) == _typed_verdicts(roots, monkeypatch)


def test_the_child_matrix_admits_and_refuses(monkeypatch):
    """Coverage: parity over a matrix that only admits, or only refuses, would prove little."""
    refused = {case_id for case_id, roots in _CHILD_MATRIX
               if any(_typed_verdicts(roots, monkeypatch).values())}
    assert refused and refused != {case_id for case_id, _roots in _CHILD_MATRIX}, refused


def test_the_raw_route_context_is_load_bearing(monkeypatch):
    """Non-vacuity: with the derivation answering nothing, the raw route flips both public
    verdicts and parts from the typed route wherever a child's contract decides."""
    monkeypatch.setattr(workflow, "derive_root_capabilities", lambda *args, **kwargs: {})

    boundary = _two_roots()
    _planned, attested = _raw_apply(_ATTESTED, boundary)
    assert (attested["_success"], attested["error_code"]) == (False, _PLACEMENT), attested
    assert "Offending path: /body/steps/0/legs/0/terminal." in attested["hint"], attested["hint"]
    assert boundary.created == []

    boundary = _two_roots()
    _planned, unwaited = _raw_apply(_UNWAITED, boundary)
    assert unwaited["_success"] is True, unwaited
    assert len(boundary.created) == 2, boundary.created

    parted = {case_id for case_id, roots in _CHILD_MATRIX
              if _raw_verdicts(roots, monkeypatch) != _typed_verdicts(roots, monkeypatch)}
    assert {
        "finding_scheduled_message_prefix",
        "unwaited_passthrough_empty_prefix",
        "interposed_decision",
        "scheduled_empty_prefix_p2_requirement",
        "bound_child_without_writer",
        "no_data_mutation_per_document",
        # The architect-round cases reach the raw route only through its derived context.
        "interposed_decision_passthrough_child",
        "cached_property_chain",
        "no_data_cache_removal_per_document",
        "guarantee_chain",
    } <= parted, parted


# ---------------------------------------------------------------------------
# build recording (amendment 1 §3) and the standalone gate
# ---------------------------------------------------------------------------

_SCHEDULE = {"mode": "scheduled", "cron": "0 * * * *", "enabled": True, "max_retry": 0}
#: A scheduled parent that sets K in one Branch leg and calls the child in the next, so the
#: child's read of K rests on its caller: compiled in this build, lacking K run directly.
_CALLER_SETS_K = (
    ("root", _doc({"kind": "branch", "legs": [
        {"steps": [_SET_K], "terminal": _STOP}, {"steps": [], "terminal": _call("child")}]}), ("child",)),
    ("child", _READS_K, ()),
)
#: What a direct run of that child lacks, as every build records it.
_LACKS_K = {
    "derived": True, "consumes_caller_documents": False, "caller_composed_paths": 0,
    "caller_document_properties": 0, "caller_cache_contents": 0,
    "dynamic_process_properties": ["K"],
}


def _raw_build(spec, boundary):
    planned, applied = _raw_apply(spec, boundary)
    assert planned["_success"] is True, planned.get("error")
    assert applied["_success"] is True, (applied.get("error_code"), applied.get("hint"))
    return applied["build_id"]


def test_a_raw_build_records_what_a_direct_run_of_its_passthrough_root_would_lack():
    """Read off the plans the pre-write pass compiled, and equal to what the typed build of
    the same request records under `authoring`."""
    from test_issue_158_listener_deployment import _typed_build

    raw = _BUILD_REGISTRY[_raw_build(_raw_spec(*_CALLER_SETS_K), _two_roots())]
    assert "authoring" not in raw
    assert raw["standalone_entry"] == {"child": _LACKS_K}, raw.get("standalone_entry")

    units = [_unit(doc, depends_on, key=key, name="E184 " + key) for key, doc, depends_on in _CALLER_SETS_K]
    typed, _boundary = _typed_build(_request(units, []), _two_roots())
    assert _BUILD_REGISTRY[typed["build_id"]]["authoring"]["standalone_entry"] == raw["standalone_entry"]


def test_orchestration_judges_a_direct_run_by_the_raw_builds_record():
    """One authored root, so a deploy reaches the standalone gate. The record is edited to
    isolate the gate, as the typed gate's test does; its derivation is pinned above."""
    build_id = _raw_build(_raw_spec(("root", _ENRICH, ())), _ApplyBoundary())
    record = _BUILD_REGISTRY[build_id]["standalone_entry"]["root"]
    assert record == dict(_LACKS_K, dynamic_process_properties=[]), record
    assert _ENTRY_CONTEXT not in _error_codes(_deploy(build_id, dry_run=True, run_test=True))

    record["dynamic_process_properties"] = ["K"]
    refused = _deploy(build_id, dry_run=True, run_test=True)
    assert _error_codes(refused) == [_ENTRY_CONTEXT], refused
    assert refused["errors"][0]["details"]["requirements"] == ["dynamic_process_properties"]
    assert _ENTRY_CONTEXT not in _error_codes(
        _deploy(build_id, dry_run=True, run_test=True, test_dynamic_properties={"K": "v"}))
    assert _error_codes(_deploy(build_id, dry_run=True, schedule_override=_SCHEDULE)) == [_ENTRY_CONTEXT]
    assert _ENTRY_CONTEXT not in _error_codes(_deploy(build_id, dry_run=True))

    del _BUILD_REGISTRY[build_id]["standalone_entry"]
    unrecorded = _deploy(build_id, dry_run=True, run_test=True)
    assert unrecorded["errors"][0]["details"]["requirements"] == ["entry_contract_not_recorded"]


def test_a_raw_build_without_a_passthrough_root_records_no_standalone_field():
    build_id = _raw_build(_raw_spec(("root", _scheduled([_MSG], _STOP), ())), _ApplyBoundary())
    assert "standalone_entry" not in _BUILD_REGISTRY[build_id]


# ---------------------------------------------------------------------------
# sibling sweep: every derivation of the per-root context in src/
# ---------------------------------------------------------------------------

#: The one derivation every route that compiles a canonical root for a caller shares.
_SHARED_DERIVATION = ("src/boomi_mcp/authoring/workflow.py", "resolve_root_context")
#: Every OTHER call of the resolver in `src/`, with why it may stand. Keyed by
#: ``(path, enclosing top-level function)``; a function of None covers the whole module.
_OTHER_DERIVATIONS = {
    ("src/boomi_mcp/recipes/engine.py", "_compile_processes"): (
        "the recipe route: the same resolver with child roots and symbols_for, projecting each "
        "child exactly as its own compile does, since that route has no resolution snapshot"),
    ("src/boomi_mcp/authoring/contract.py", None): (
        "revision material: behaviour oracles over fixed in-memory graphs that fingerprint what "
        "the server accepts, never a caller's root"),
}
#: The two routes the finding names, and the function each derives the context in.
_ROUTE_CALLERS = {
    ("src/boomi_mcp/authoring/workflow.py", "_validate_processes"): "resolve_root_context",
    ("src/boomi_mcp/categories/integration_builder.py", "_build_canonical_plan"): "derive_root_capabilities",
}


def _call_name(node):
    func = node.func
    return func.id if isinstance(func, ast.Name) else getattr(func, "attr", None)


def _sites_in(source, relative, callee):
    """``(path, enclosing top-level function)`` of every call of ``callee`` in ``source``."""
    sites = set()
    for top in ast.parse(source).body:
        for node in ast.walk(top):
            if isinstance(node, ast.Call) and _call_name(node) == callee:
                sites.add((relative, getattr(top, "name", None)))
    return sites


def _src_sites(callee):
    sites = set()
    for path in sorted((_ROOT / "src").rglob("*.py")):
        sites |= _sites_in(path.read_text(encoding="utf-8"), path.relative_to(_ROOT).as_posix(), callee)
    return sites


def _unexplained(sites):
    return sorted(
        site for site in sites
        if site != _SHARED_DERIVATION
        and site not in _OTHER_DERIVATIONS and (site[0], None) not in _OTHER_DERIVATIONS)


def test_every_derivation_of_the_per_root_context_is_the_shared_one_or_says_why_not():
    sites = _src_sites("resolve_process_ir_effect_declarations")
    assert _SHARED_DERIVATION in sites, sorted(sites)
    assert _unexplained(sites) == [], _unexplained(sites)
    stale = sorted(key for key in _OTHER_DERIVATIONS
                   if not any(site[0] == key[0] and key[1] in (None, site[1]) for site in sites))
    assert stale == [], stale
    for (path, function), helper in sorted(_ROUTE_CALLERS.items()):
        assert (path, function) in _src_sites(helper), (path, function, helper)

    # Non-vacuity: a derivation written anywhere else is reported.
    witness = _sites_in(
        "def route():\n    return resolve_process_ir_effect_declarations(roots, None, s)\n",
        "src/synthetic_witness.py", "resolve_process_ir_effect_declarations")
    assert _unexplained(witness) == [("src/synthetic_witness.py", "route")]


@pytest.mark.parametrize("key,reason", sorted(_OTHER_DERIVATIONS.items(), key=lambda item: (item[0][0], item[0][1] or "")))
def test_every_other_derivation_states_a_reason(key, reason):
    assert len(reason.split()) >= 8, (key, reason)
