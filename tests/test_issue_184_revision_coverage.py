"""#184 correction batch 18 (ARCH-184-r1-09): every lineage decision moves the compiler revision.

The defect class `revision-oracle-omits-changed-behaviour` recurred after batch 12's
structural fix, whose set came from the typed plan route's builder imports alone. The
retrieve overlay's writer selection then changed what the server accepts while the served
compiler revision stood still, because nothing in the lineage module was in that set. The
set is now derived from the lineage module and the child-contract derivation themselves,
and each member needs a perturbation that changes a verdict and moves the revision.
"""

from __future__ import annotations

import ast
import json
import sys
from pathlib import Path
from types import MappingProxyType

import pytest

_ROOT = Path(__file__).resolve().parent.parent
for _p in (str(_ROOT), str(_ROOT / "src")):
    if _p not in sys.path:
        sys.path.insert(0, _p)

from boomi_mcp.authoring import contract as authoring_contract  # noqa: E402
from boomi_mcp.authoring import process_ir_effects  # noqa: E402
from boomi_mcp.compiler.process_ir.semantic_validation import lineage  # noqa: E402

#: Per source module: ``(module, the functions the set starts from, whether they are members)``.
#: `_walk_lineage` is the walk itself and not a member: perturbing it replaces the whole
#: analysis. The three effect functions build a root's entry-contract binding, decide
#: whether it applies and apply it, so they are members.
_SOURCES = {
    "lineage": (lineage, ("_walk_lineage",), False),
    "effects": (process_ir_effects, ("_entry_contract_bindings", "_binds", "_with_entry_contracts"), True),
}


def _is_table(value) -> bool:
    """A computed module-level value: a set, a mapping or a record instance.

    A literal token, a tuple of tokens and a type alias are vocabulary, not decisions: the
    function that compares one is a member in its own right.
    """
    return isinstance(value, (ast.Call, ast.Set, ast.Dict, ast.SetComp, ast.DictComp, ast.ListComp,
                              ast.GeneratorExp))


def _decisions_in(source: str, roots, include_roots: bool):
    """Every module-level decision ``roots`` reach, transitively through module-level functions.

    A decision is a module-level function, a method of a module-level class (a class a
    reached body names decides through its methods), or a module-level table. A name is
    followed wherever a reached body mentions it, closures included, so a rule nested in
    `_walk_lineage` contributes every module-level predicate it asks.
    """
    tree = ast.parse(source)
    functions = {node.name: node for node in tree.body if isinstance(node, ast.FunctionDef)}
    classes = {node.name: node for node in tree.body if isinstance(node, ast.ClassDef)}
    tables = {
        target.id
        for node in tree.body
        if isinstance(node, (ast.Assign, ast.AnnAssign)) and node.value is not None and _is_table(node.value)
        for target in (node.targets if isinstance(node, ast.Assign) else [node.target])
        if isinstance(target, ast.Name)
    }
    found, seen, frontier = set(), set(), list(roots)
    while frontier:
        name = frontier.pop()
        if name in seen:
            continue
        seen.add(name)
        bodies = []
        if name in functions:
            if include_roots or name not in roots:
                found.add(name)
            bodies.append(functions[name])
        elif name in classes:
            for member in classes[name].body:
                if isinstance(member, ast.FunctionDef) and not member.name.startswith("__"):
                    found.add(name + "." + member.name)
                    bodies.append(member)
        elif name in tables:
            found.add(name)
        for body in bodies:
            frontier.extend(inner.id for inner in ast.walk(body) if isinstance(inner, ast.Name))
    return found


def _derived_decisions():
    derived = set()
    for label, (module, roots, include_roots) in _SOURCES.items():
        source = Path(module.__file__).read_text(encoding="utf-8")
        derived |= {(label, name) for name in _decisions_in(source, roots, include_roots)}
    return derived


def _perturbations():
    """One behaviour change per decision: ``(module label, name) -> replacement``.

    Each drops, inverts or widens what the decision answers, never raises: a replacement
    that raised would read "unavailable" and move the revision without changing a verdict.
    """
    real_overlay = lineage._overlay_cache_read
    real_freeze = lineage._cohort_at_write
    real_state = lineage._State
    x = (lineage.DDP, "X")

    def without_cached_alternatives(semantic, state, writers, on_documents, invalidated, stream):
        stripped = frozenset((ref, cohort._replace(alternatives=frozenset())) for ref, cohort in state.cohorts)
        after, carried, documents, dropped, count = real_overlay(
            semantic, real_state(state.document, state.execution, state.content, stripped),
            writers, on_documents, invalidated, stream)
        return real_state(after.document, after.execution, after.content, state.cohorts), carried, documents, \
            dropped, count

    def without_the_abort_gate(semantic, contract):
        if contract is None or contract.entry_form not in ("scheduled", "passthrough") or not semantic.wait:
            return ()
        return tuple((key[0], key[1]) for key in contract.guaranteed_state)

    survival = dict(lineage.PROPERTY_SURVIVAL_V1)
    survival[("message", None)] = "unmeasured"
    occurrences = ("map_ref", "process_ref", "cache_get_ref", "external_writer_ref", "scripts")
    return {
        # --- lineage: functions ---------------------------------------------------------
        ("lineage", "_authored_at"): lambda ir, pointer: None,
        ("lineage", "_call_prefix"): lambda ir, call_path: (None, None),
        ("lineage", "_native_work_marker"): lambda incoming, semantic_kind, authored_kind: incoming,
        ("lineage", "_prefix_predecessor"): lambda context, own, marker, parent_form: own,
        ("lineage", "_handed_document_requirements"): lambda contract, wait: (),
        ("lineage", "_caller_cohort"): lambda names: lineage.UNKNOWN_COHORT,
        ("lineage", "cache_content_judgement"):
            lambda stream, identity: None if stream.origin != "cache" else True,
        ("lineage", "_tracked_property_key"):
            lambda property_id, fallback_name: (lineage.DPP, fallback_name or property_id),
        ("lineage", "_reads_of"): lambda semantic: (),
        ("lineage", "_replacing_step_operation"): lambda semantic: "split_documents",
        ("lineage", "_discards_document_properties"): lambda semantic: False,
        ("lineage", "_drop_replaced_document_keys"): lambda state, keep: (state, frozenset()),
        ("lineage", "_replaces_document_stream"): lambda semantic: False,
        ("lineage", "_cohort_at_write"):
            lambda on_documents, writers, stream: real_freeze(on_documents, writers, stream)._replace(
                alternatives=frozenset()),
        ("lineage", "_overlay_cache_read"): without_cached_alternatives,
        ("lineage", "_writes_of"): lambda semantic: (),
        ("lineage", "_trusted_effects"): lambda semantic, capabilities: (),
        ("lineage", "_establishes_downstream"): lambda semantic: False,
        ("lineage", "_awaited_guarantee"): without_the_abort_gate,
        ("lineage", "_child_guarantee"):
            lambda semantic, contract, stream: lineage._awaited_guarantee(semantic, contract),
        ("lineage", "_caller_cached_origin"): lambda key, stream, invalidated: None,
        ("lineage", "_nonstrict_read_can_fail"): lambda prepared, key, capabilities: True,
        ("lineage", "_established_anywhere"): lambda prepared, key, capabilities: False,
        ("lineage", "_opaque_reason"): lambda semantic, capabilities: None,
        ("lineage", "_caches_a_call_may_write"): lambda cache_refs, contract: (),
        ("lineage", "_caches_a_call_may_remove"): lambda cache_refs, contract: (),
        # The one rule every site reads a proved removal through, and the proved-path gate
        # on what a child may guarantee at all.
        ("lineage", "proved_removals"): lambda contract: (),
        ("lineage", "_path_provably_runs"): lambda stream: True,
        # The other carrier of non-emptiness: the walk's own proof that an earlier write
        # filled the cache this retrieve reads. Dropping it withholds the guarantee behind
        # every such retrieve again.
        ("lineage", "_retrieve_of_a_proved_cache"): lambda semantic, state: False,
        # The two halves of the shared-cache obligation: what a call can check, and what it
        # owes its own callers. Dropping either re-opens one of the two shapes.
        ("lineage", "_call_stores_nothing_in"):
            lambda state, cache_ref, external_writer: False,
        ("lineage", "_caller_owes_a_cached_property"):
            lambda cache_ref, name, capabilities, state: False,
        ("lineage", "_without_cache_establishment"): lambda state, cache_ref: state,
        ("lineage", "_after_a_whole_cache_removal"):
            lambda state, cache_ref, proved_to_run: state.without_content(cache_ref),
        ("lineage", "_seeds_an_unknown_cohort"): lambda cache_ref, cohort_names: True,
        ("lineage", "_repetition_unstable_caches"): lambda contract, cache_refs: (),
        ("lineage", "_leg_member_index"): lambda prepared: {},
        ("lineage", "_leg_write_index"): lambda prepared, capabilities=None: {},
        ("lineage", "_written_in_a_later_leg"): lambda leg_writes, leg, key: False,
        ("lineage", "_written_anywhere"): lambda prepared, key, capabilities=None: False,
        # --- lineage: the lattice's transfer and meet -----------------------------------
        ("lineage", "_State.with_write"): lambda self, key, proved=False: self,
        ("lineage", "_State.with_content"): lambda self, cache_ref, identity: self,
        ("lineage", "_State.with_cohort"): lambda self, cache_ref, cohort, ours=False: self,
        ("lineage", "_State.cohorts_of"): lambda self, cache_ref: frozenset(),
        ("lineage", "_State.without_content"): lambda self, cache_ref: self,
        # Never sealing re-opens the false refusal a proved removal and refill ends. The
        # opposite direction — clearing the seal when a foreign cohort enters — is a guard
        # inside `_State.with_cohort`, not a decision of its own, and is witnessed by
        # `test_issue_184_child_state_transfer.py::test_a_foreign_cohort_and_the_meet_each_end_the_removal_proof`.
        ("lineage", "_State.with_sealed_cache"): lambda self, cache_ref: self,
        ("lineage", "_State.content_of"): lambda self, cache_ref: frozenset(),
        ("lineage", "_State.establishes"): lambda self, key: False,
        ("lineage", "_State.entering_branch_leg"):
            lambda self: real_state(frozenset(), self.execution, self.content, self.cohorts),
        ("lineage", "_State.merged_with"): lambda self, other: real_state(
            self.document | other.document, self.execution | other.execution,
            self.content | other.content, self.cohorts | other.cohorts),
        # --- lineage: tables ------------------------------------------------------------
        ("lineage", "NO_PRODUCER_STREAMS"): frozenset(),
        ("lineage", "PROPERTY_SURVIVAL_V1"): MappingProxyType(survival),
        ("lineage", "DOCUMENT_STREAM_REPLACING_KINDS"): frozenset(),
        ("lineage", "_ABNORMAL_EXIT_ROLES"): frozenset({"exception", "process_call"}),
        ("lineage", "_COUNT_PRESERVING_KINDS"): frozenset(),
        ("lineage", "_DERIVED_ENTRY_FORMS"): frozenset(),
        ("lineage", "_DOCUMENT_LIFETIME_SCOPES"): frozenset(),
        ("lineage", "UNKNOWN_COHORT"): lineage._Cohort(
            frozenset({x}), None, frozenset({(x, lineage.UNKNOWN_WRITER)}), lineage.COUNT_UNKNOWN),
        # --- the child-contract derivation ----------------------------------------------
        ("effects", "derive_child_entry_facts"):
            lambda child_ir, symbols, capabilities=None: {"entry_form": "unknown"},
        ("effects", "_caller_composed_paths"): lambda prepared, capabilities, walk: (),
        ("effects", "_caller_cached_properties"): lambda prepared, capabilities, walk: (),
        ("effects", "_caller_cache_seeds"): lambda requirements, symbols: (),
        ("effects", "_entry_contract_bindings"): lambda process_roots, symbols, symbols_for, base_for=None: {
            key: ((), (), (), None, "unknown", (), ()) for key, _ir in process_roots},
        ("effects", "_binds"): lambda binding: False,
        ("effects", "_with_entry_contracts"): lambda capabilities, binding: capabilities,
        ("effects", "_aliases"): lambda symbols, ref: frozenset({ref}),
        ("effects", "_symbol"): lambda symbols, ref: None,
        ("effects", "_occurrences"): lambda ir: {bucket: set() for bucket in occurrences},
        ("effects", "_iter_nodes"): lambda node: iter(()),
        ("effects", "_node_is_inspectable"): lambda semantic, capabilities=None: False,
        ("effects", "CONTRACT_GATED_CHILD_KINDS"): frozenset(),
    }


def _patch(patched, key, replacement):
    label, name = key
    target = _SOURCES[label][0]
    if "." in name:
        owner, name = name.split(".")
        target = getattr(target, owner)
    # `raising=False`: a key the module does not define still applies, so the guard runs
    # against an older tree and reports each decision there that moves nothing.
    patched.setattr(target, name, replacement, raising=False)


def test_every_lineage_decision_moves_the_compiler_revision(monkeypatch):
    """The coverage claim. The set is every module-level function and table of
    `semantic_validation/lineage.py` that `_walk_lineage` references, transitively through
    module-level functions, and the methods of the module-level classes it names
    (`_State`'s transfer and meet); plus every module-level function and table of
    `authoring/process_ir_effects.py` the child-contract binding reaches from
    `_entry_contract_bindings`, `_binds` and `_with_entry_contracts`. The set is derived
    from source, and the perturbation map's keys must equal it: a decision added later
    fails here until it has a perturbation, and a perturbation of something no longer
    reached fails too. Each perturbation must move `_compiler_revision()` with no row
    reading "unavailable".

    Literal tokens (a scope name, a stream state, a verdict label) are vocabulary: the
    function comparing one is in the set. Rules nested in `_walk_lineage`'s closures (the
    transfer, the stream step, the call discharge, the controller) are not module-level.
    They are covered through the module-level predicates they ask, which are in the set,
    and through the closed-vocabulary matrices the revision rows validate
    (`property_survival`, `lineage_reads`, `cache_content_consumers`,
    `child_entry_contract`, `child_forwarding`), never rule by rule.

    THE BOUND, stated because a coverage claim that does not say what it excludes gets read
    as one that excludes nothing. The unit of this claim is a module-level DECISION, not a
    source line. An edit INSIDE a reached decision — one call site's argument, a tightened
    branch, an added guard clause — has no perturbation of its own here and need not move
    the revision; perturbing the decision it lives in exercises the decision, not that line.
    Measured on correction batch 18 with a per-hunk reversion harness: at zero context 53 of
    144 sub-hunks were revision-silent, including three fail-closed guards of the batch
    itself. Those are covered by TESTS, which is the other half of the claim and the half
    that must be kept honest — each of the three fails when its guard is reverted:

    * the child-contract model validator, by
      `test_issue_184_child_state_transfer.py::test_a_child_guarantees_no_document_property_and_nothing_it_does_not_write`;
    * the map cache-content judgement, by
      `test_issue_184_stream_profiles.py::test_the_cache_content_judgement_is_load_bearing`;
    * the zero-emission outgoing-wire refusal, by
      `test_issue_184_document_emission.py::test_an_outgoing_cache_wire_is_refused_at_emitter_preflight`.

    Served contract PROSE is not in this set either, and does not need to be: it is carried
    by the `process_ir_authoring_contract` row of the same payload — reverting an authority
    sentence moves the revision — and pinned byte-for-byte besides, by
    `test_process_ir_authoring_contract_parity.py::test_the_whole_contract_is_frozen_in_a_committed_snapshot`
    and its page-schema twin, so a served sentence cannot change without a diff someone approves.

    So the guarantee this guard gives is: no module-level decision the walk reaches can be
    changed with the served compiler revision standing still. It is NOT "no edit to
    lineage.py can be". A reviewer who needs the second property runs the reversion harness;
    a reviewer who needs the first runs this test."""
    derived = _derived_decisions()
    perturbations = _perturbations()
    payload = authoring_contract._compiler_revision_payload()
    assert sorted(row for row, value in payload.items() if value == "unavailable") == []
    baseline = authoring_contract._compiler_revision()
    assert authoring_contract.sha256_fingerprint(payload) == baseline
    unmoved, unavailable = [], {}
    for key, replacement in sorted(perturbations.items()):
        with monkeypatch.context() as patched:
            _patch(patched, key, replacement)
            perturbed = authoring_contract._compiler_revision_payload()
        rows = sorted(row for row, value in perturbed.items() if value == "unavailable")
        if rows:
            unavailable[key] = rows
        if authoring_contract.sha256_fingerprint(perturbed) == baseline:
            unmoved.append(key)
    assert authoring_contract._compiler_revision() == baseline
    assert {
        "unperturbed": sorted(derived - set(perturbations)),
        "not_a_reached_decision": sorted(set(perturbations) - derived),
        "unmoved": unmoved,
        "unavailable": unavailable,
    } == {"unperturbed": [], "not_a_reached_decision": [], "unmoved": [], "unavailable": {}}
    # Non-vacuity of the derivation: it reaches this batch's predicates, the overlay, the
    # lattice's meet and a table, and both sources contribute.
    assert {
        ("lineage", "_overlay_cache_read"), ("lineage", "_cohort_at_write"),
        ("lineage", "cache_content_judgement"), ("lineage", "_prefix_predecessor"),
        ("lineage", "_child_guarantee"), ("lineage", "_repetition_unstable_caches"),
        ("lineage", "_State.merged_with"), ("lineage", "_ABNORMAL_EXIT_ROLES"),
        ("effects", "_caller_cached_properties"), ("effects", "derive_child_entry_facts"),
    } <= derived


def test_the_derivation_follows_functions_closures_classes_and_tables_but_not_tokens():
    """Non-vacuity of the derivation itself, on a module built for it: a helper reached only
    from a closure, a class method, a table, and none of a token, a tuple of tokens, a
    constructor or a table nothing reached mentions."""
    source = "\n".join((
        "TOKEN = 't'",
        "PAIR = (TOKEN, None)",
        "TABLE = frozenset({'a'})",
        "UNUSED = frozenset({'b'})",
        "class _Box:",
        "    def __init__(self):",
        "        self.value = UNUSED",
        "    def decide(self):",
        "        return TABLE",
        "def _helper(value):",
        "    return value in TABLE and TOKEN",
        "def _unreached():",
        "    return UNUSED",
        "def _root():",
        "    def closure():",
        "        return _helper(PAIR)",
        "    return _Box().decide() and closure()",
    ))
    assert _decisions_in(source, ("_root",), False) == {"_helper", "TABLE", "_Box.decide"}
    assert _decisions_in(source, ("_root",), True) == {"_root", "_helper", "TABLE", "_Box.decide"}


def test_an_equivalent_decision_leaves_the_revision_where_it_is(monkeypatch):
    """The control: a replacement that answers exactly as the original does moves nothing,
    so a moved revision above is a changed verdict, never a changed function object."""
    baseline = authoring_contract._compiler_revision()
    real_overlay, real_merge = lineage._overlay_cache_read, lineage._State.merged_with
    monkeypatch.setattr(lineage, "_overlay_cache_read", lambda *args: real_overlay(*args))
    monkeypatch.setattr(lineage._State, "merged_with", lambda self, other: real_merge(self, other))
    assert authoring_contract._compiler_revision() == baseline


def test_the_overlay_read_kinds_are_the_survival_tables_overlay_cells():
    """Both ways: every kind the walk routes to the retrieve overlay is a cache-overlay cell
    of the survival table, keyed on no step operation, and every such cell is one of those
    kinds. The revision row validates its staged graphs once per kind."""
    cells = {cell for cell, verdict in lineage.PROPERTY_SURVIVAL_V1.items() if verdict == lineage._CACHE_OVERLAY}
    assert cells == {(kind, None) for kind in lineage.TRIGGERED_REPLACEMENT_SEMANTIC_KINDS}
    graphs = authoring_contract._compiler_revision_payload()["property_survival"]["overlay_verdicts"]["graphs"]
    assert {name.split(":")[0] for name in graphs} == set(lineage.TRIGGERED_REPLACEMENT_SEMANTIC_KINDS)


# ---------------------------------------------------------------------------
# ARCH-184-r1-09, the residual half: a retrieve answers with a whole state
# ---------------------------------------------------------------------------


def _overlay_row():
    payload = authoring_contract._compiler_revision_payload()
    assert payload["property_survival"] != "unavailable"
    return payload["property_survival"]["overlay_verdicts"]


def _dropping(component):
    """A retrieve that answers exactly as the real one does, minus one component."""
    real_overlay = lineage._overlay_cache_read

    def overlay(semantic, state, writers, on_documents, invalidated, stream):
        after, carried, documents, invalid, count = real_overlay(
            semantic, state, writers, on_documents, invalidated, stream)
        components = {slot: getattr(after, slot) for slot in lineage._State.__slots__}
        components[component] = frozenset()
        return lineage._State(**components), carried, documents, invalid, count

    return overlay


@pytest.mark.parametrize("component", sorted(lineage._State.__slots__))
def test_a_retrieve_that_drops_one_component_of_its_answer_moves_the_revision(component, monkeypatch):
    """The coverage claim for the retrieve overlay, both ways against the lattice: the
    overlay answers with a whole state, so EVERY component of that state is revision
    material. The finding's own mutation moved only the document keys; a retrieve that
    kept the documents and dropped the cached cohorts, or the execution state, changed
    public plan and compile verdicts with the served revision standing still."""
    baseline = authoring_contract._compiler_revision()
    with monkeypatch.context() as patched:
        patched.setattr(lineage, "_overlay_cache_read", _dropping(component))
        payload = authoring_contract._compiler_revision_payload()
    assert sorted(row for row, value in payload.items() if value == "unavailable") == []
    assert authoring_contract.sha256_fingerprint(payload) != baseline, component
    assert authoring_contract._compiler_revision() == baseline


def test_the_overlay_row_records_every_component_of_the_state_and_varies_each_one():
    """Both ways against the authority. Every component of `_State` is projected — a
    component added to the lattice with no projection raises, which reads "unavailable"
    and fails the guard above — and the recorded case space VARIES every one of them, so
    no projection is a constant nothing can move."""
    projections = [case[-1] for case in _overlay_row()["transfer"]]
    assert projections, "the transfer half recorded no case"
    assert {frozenset(projection) for projection in projections} == {
        frozenset(lineage._State.__slots__)}
    for component in lineage._State.__slots__:
        values = {json.dumps(projection[component], sort_keys=True) for projection in projections}
        assert len(values) > 1, component


def test_the_overlay_graphs_read_the_cache_twice_and_read_a_property_after_it():
    """The public half of the same claim, per read kind the walk routes to the overlay: a
    second read still carries the cohorts the cache holds, the cache's own establishment
    survives its retrieve and so do the process properties — and a whole-cache removal
    takes that establishment away (amendment 3 §7-§8)."""
    graphs = _overlay_row()["graphs"]
    for kind in sorted(lineage.TRIGGERED_REPLACEMENT_SEMANTIC_KINDS):
        for name in ("second_read_bound", "second_read_ordinary", "read_twice_over_one_write",
                     "property_read_after_the_read"):
            assert graphs[kind + ":" + name] == [], (kind, name)
        assert graphs[kind + ":removed_then_read"] != [], kind


@pytest.mark.parametrize("component,flipped", sorted((
    ("cohorts", "second_read_bound"),
    ("execution", "property_read_after_the_read"),
)))
def test_dropping_a_carried_component_flips_a_public_verdict(component, flipped, monkeypatch):
    """Non-vacuity of the graphs above: each is admitted today and refused by a retrieve
    that drops that component, so the recorded verdict is a measurement, not a label."""
    before = _overlay_row()["graphs"]
    with monkeypatch.context() as patched:
        patched.setattr(lineage, "_overlay_cache_read", _dropping(component))
        after = _overlay_row()["graphs"]
    for kind in sorted(lineage.TRIGGERED_REPLACEMENT_SEMANTIC_KINDS):
        name = kind + ":" + flipped
        assert before[name] == [] and after[name] != [], (name, after[name])


# ---------------------------------------------------------------------------
# The same class inside this batch's own hunks: what a call does to the caller
# ---------------------------------------------------------------------------


def _still_opaque(semantic, capabilities):
    """The pre-batch rule: a called process the caller declares nothing about is opaque,
    whatever the server's own walk of it proves."""
    if semantic.semantic_kind == "process_call":
        return None if capabilities.subprocess_effect(semantic.process_ref) else "subprocess"
    return _REAL_OPAQUE_REASON(semantic, capabilities)


_REAL_OPAQUE_REASON = lineage._opaque_reason
_REAL_WITH_WRITE = lineage._State.with_write
_REAL_AFTER_A_WHOLE_CACHE_REMOVAL = lineage._after_a_whole_cache_removal


def _seals_whatever_the_walk_proved(state, cache_ref, proved_to_run):
    """The PERMISSIVE half of the seal gate, neutralised: a whole-cache removal the walk
    marks as possibly skipped seals the cache all the same — the rule before round r19.

    Its mirror (never sealing) is already a perturbation of `_State.with_sealed_cache`, and
    that direction moved the revision. This one did not: the section added for the seal used
    a provably-running removal in every case, so the gate inside the function the
    `a_whole_cache_removal_only_clears_the_content` rule perturbs was unbound (round r19c).
    """
    return _REAL_AFTER_A_WHOLE_CACHE_REMOVAL(state, cache_ref, True)


def _a_union_over_paths(self, key, proved=False):
    """The stance the per-path meet replaced: ONE unproved write un-proves the key for every
    path, so a key every normal completion establishes is withheld the moment some other path
    also wrote it behind a possibly-empty step."""
    after = _REAL_WITH_WRITE(self, key, proved=proved)
    if proved or key[0] in lineage._DOCUMENT_LIFETIME_SCOPES:
        return after
    return lineage._State(after.document, after.execution, after.content, after.cohorts,
                          after.sealed, after.proved - {key})

#: One per rule of this batch that decides what a call does to its caller's state and
#: that no table states: the row it must move, and the pre-batch answer that neutralises
#: it. Each was revertible with the served revision standing still.
_CALL_STATE_RULES = {
    "a_call_guarantees_nothing_to_a_later_reader": (
        "child_call_state", "_awaited_guarantee", lambda semantic, contract: ()),
    "a_derived_child_is_opaque_all_the_same": (
        "child_call_state", "_opaque_reason", _still_opaque),
    "a_cached_property_is_the_childs_own_defect": (
        "child_call_state", "_caller_cached_origin", lambda key, stream, invalidated: None),
    "a_whole_cache_removal_only_clears_the_content": (
        "property_survival", "_after_a_whole_cache_removal",
        lambda state, cache_ref, proved_to_run: state.without_content(cache_ref)),
    # Round r19b: the per-path meet was revision-SILENT — reverting it left the compiler
    # revision byte-identical, because the oracle's child vocabulary had no child writing one
    # key on both a proved and an unproved path. The shape the rule exists for is now a case,
    # so the rule cannot be reverted with this payload standing still.
    "a_guarantee_is_a_union_over_paths_not_a_meet": (
        "child_call_state", "_State.with_write", _a_union_over_paths),
    # Round r19c: the seal's own proof gate was revision-silent in the direction it exists
    # to block. The `a_whole_cache_removal_only_clears_the_content` row above drops the
    # WHOLE transform and moves the revision; removing only the gate inside it moved a real
    # caller from refused to admitted with the served bytes unchanged.
    "an_unproved_removal_seals_all_the_same": (
        "child_call_state", "_after_a_whole_cache_removal", _seals_whatever_the_walk_proved),
}


@pytest.mark.parametrize("rule", sorted(_CALL_STATE_RULES))
def test_each_call_state_rule_moves_the_revision_through_its_own_row(rule, monkeypatch):
    """`revision-oracle-omits-changed-behaviour` recurred INSIDE its own fix: four rules
    of this batch could be reverted with the compiler revision unchanged. Each is now
    recorded as the verdicts a product of closed vocabularies receives, and each must
    move both that row and the fingerprint."""
    row_name, target, replacement = _CALL_STATE_RULES[rule]
    baseline_payload = authoring_contract._compiler_revision_payload()
    baseline = authoring_contract.sha256_fingerprint(baseline_payload)
    with monkeypatch.context() as patched:
        # `_patch` rather than a bare setattr: a rule may live on a method of the lattice
        # (`_State.with_write`), which only the dotted form reaches.
        _patch(patched, ("lineage", target), replacement)
        payload = authoring_contract._compiler_revision_payload()
    assert sorted(row for row, value in payload.items() if value == "unavailable") == []
    assert payload[row_name] != baseline_payload[row_name], rule
    assert authoring_contract.sha256_fingerprint(payload) != baseline, rule
    assert authoring_contract._compiler_revision() == baseline


def _update_keys(node):
    """The literal keys of the ``update=`` mapping this ``model_copy`` call is handed."""
    for keyword in node.keywords:
        if keyword.arg == "update" and isinstance(keyword.value, ast.Dict):
            return {key.value for key in keyword.value.keys if isinstance(key, ast.Constant)}
    return set()


def _canonicalized_contract_fields():
    """Every fact the cache-identity canonicalization rewrites, read off its source: the
    child-contract fields its inner `contract` rewrites, and the capability fields it
    rewrites itself. The nested effect copies are not facts of their own — they carry
    the same state keys under a different owner — so only these two updates count."""
    from boomi_mcp.compiler.process_ir.semantic_validation import context

    tree = ast.parse(Path(context.__file__).read_text(encoding="utf-8"))
    canonicalizer = next(
        node for node in ast.walk(tree)
        if isinstance(node, ast.FunctionDef) and node.name == "canonical_cache_capabilities"
    )
    row_copy = next(
        node for node in ast.walk(next(
            inner for inner in ast.walk(canonicalizer)
            if isinstance(inner, ast.FunctionDef) and inner.name == "contract"))
        if isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute)
        and node.func.attr == "model_copy"
    )
    capability_copy = next(
        node for node in ast.walk(canonicalizer)
        if isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute)
        and node.func.attr == "model_copy" and isinstance(node.func.value, ast.Name)
        and node.func.value.id == "capabilities"
    )
    return _update_keys(row_copy) | _update_keys(capability_copy)


#: Canonicalized before this batch, and pinned by the #184 component-identity suite.
_CANONICAL_BEFORE_BATCH_18 = {
    "required_reads", "mutated_state", "cache_requirements", "map_effects", "script_effects",
    "subprocess_summaries", "external_writers", "established_at_entry", "child_entry_contracts",
    "caller_cache_contents", "entry_contract",
}


def test_every_cache_fact_this_batch_added_is_measured_through_two_references():
    """The cache-identity class's guard, one level up: each cache-keyed contract fact the
    batch added is recorded as the verdicts a graph receives with one reference to the
    cache and with a second one in the place that states the fact. The two must agree —
    that is what "one component" means — and a field added to the canonicalization later
    fails here until it has a case."""
    cases = authoring_contract._compiler_revision_payload()["child_call_state"]["one_cache_two_refs"]
    assert set(cases) == _canonicalized_contract_fields() - _CANONICAL_BEFORE_BATCH_18
    for field, halves in sorted(cases.items()):
        assert set(halves) == {"derived_from_the_child", "stated_by_a_caller"}, field
        for half, spellings in sorted(halves.items()):
            assert set(spellings) == {"through_one_reference", "through_two_references"}, (field, half)
            assert spellings["through_one_reference"] == spellings["through_two_references"], (
                field, half, spellings)
    # Non-vacuity: the agreement is not [] == [] everywhere. One chain's read follows a
    # removal, so the canonical spelling refuses it and the aliased one must too.
    assert any(
        answer["errors"]
        for halves in cases.values()
        for answer in halves["derived_from_the_child"]["through_one_reference"].values()
    )


def test_the_two_reference_cases_rest_on_the_cache_identity_canonicalization(monkeypatch):
    """Non-vacuity of that row: with the canonicalization neutralised the two spellings
    disagree, and the revision moves — so the row measures the identity rule rather than
    recording two copies of one verdict."""
    from boomi_mcp.compiler.process_ir.semantic_validation import pipeline as validation_pipeline

    baseline = authoring_contract._compiler_revision()
    for module in (validation_pipeline, lineage):
        monkeypatch.setattr(module, "canonical_cache_capabilities",
                            lambda capabilities, canonical: capabilities)
    payload = authoring_contract._compiler_revision_payload()
    cases = payload["child_call_state"]["one_cache_two_refs"]
    assert any(
        halves["stated_by_a_caller"]["through_one_reference"]
        != halves["stated_by_a_caller"]["through_two_references"]
        for halves in cases.values()
    ), cases
    assert authoring_contract.sha256_fingerprint(payload) != baseline


def _recorded_root_verdicts(row):
    """Every root verdict anywhere in the child-call-state row, split by leaf and caller.

    Found by walking the payload rather than by naming the sections: a section added later
    is covered the moment its roots go through `verdicts_of`, which is what records the
    `calls` list each verdict carries.
    """
    leaves, callers, stack = [], [], [(("child_call_state",), row)]
    while stack:
        path, node = stack.pop()
        if isinstance(node, dict):
            if "calls" in node and "errors" in node:
                (leaves if node["calls"] == [] else callers).append((path, node))
                continue
            stack.extend((path + (str(key),), value) for key, value in node.items())
        elif isinstance(node, list):
            stack.extend((path + (str(index),), value) for index, value in enumerate(node))
    return leaves, callers


def test_every_child_any_call_state_section_measures_is_a_graph_that_can_ship():
    """The structural half of `an oracle case built on a graph the validator refuses`.

    The class appeared three times in this slice — the `fills_the_cache_on_a_proved_path`
    child, the `one_cache_two_refs` guarantee child, and the test fixtures that copied their
    shape — because the row recorded only the CALLER's verdict, so a child refused
    `…CARDINALITY_MISMATCH` still contributed a `guaranteed_state` the served compiler and
    capability revisions were computed over. The first fix put the child's verdict into the
    `calls` rows; that left the mechanism alive in every OTHER section, and the batch's own
    `a_removed_and_refilled_cache` resolved four roots while recording two — so swapping its
    writer back to the refused shape moved no served byte and tripped nothing (round r19c).

    The enumeration is therefore gone on both sides. Every root a section resolves is
    recorded, because the reported set IS the resolver's root list; and this guard reads
    every recorded verdict anywhere in the row, keyed on what each root CALLS rather than on
    its name. A root that calls nobody is a leaf child, whose contract facts the verdicts
    above it are derived from, and it must be a graph that can ship. A root that calls
    another is the caller under measurement, and its refusal is often the recorded answer."""
    row = authoring_contract._compiler_revision_payload()["child_call_state"]
    assert all(len(case) == 6 for case in row["calls"]), "a case carries no child verdict"
    leaves, callers = _recorded_root_verdicts(row)
    refused = sorted(path for path, verdict in leaves if verdict["errors"])
    assert refused == [], refused
    # Non-vacuity, three ways: the walk reaches leaves in more than one section, the `calls`
    # rows' own child verdicts are among them, and the SAME measurement reports errors where
    # there are any — so an empty leaf-error list is a measured fact, not an empty shape.
    assert len({path[1] for path, _verdict in leaves}) > 1, sorted(
        {path[1] for path, _verdict in leaves})
    assert len(leaves) >= len(row["calls"]), (len(leaves), len(row["calls"]))
    assert any(case[3]["errors"] for case in row["calls"])
    assert any(verdict["errors"] for _path, verdict in callers)


def test_the_call_state_row_reaches_every_case_of_its_vocabularies():
    """Non-vacuity of the product: every child, caller and call form contributes a case,
    the verdicts are not all the same, and every case names a derived child contract."""
    row = authoring_contract._compiler_revision_payload()["child_call_state"]
    children, callers, forms = zip(*[(case[0], case[1], case[2]) for case in row["calls"]])
    assert len(row["calls"]) == len(set(children)) * len(set(callers)) * len(set(forms))
    assert len({json.dumps(case[3], sort_keys=True) for case in row["calls"]}) > 1
    assert all(case[4] is not None for case in row["calls"])
    assert set(row["forwarded_cached_properties"]) == {
        "a_bound_request_path", "an_ordinary_read", "a_declared_read"}
    # The declared channel records what its row is WORTH, not merely that it exists: the
    # caller that stored the property is admitted and the one that did not is refused, so
    # the rule that produces the row cannot be reverted with this payload standing still.
    declared = row["forwarded_cached_properties"]["a_declared_read"]["roots"]
    assert declared["a_caller_that_stored_it"]["errors"] == []
    assert declared["a_caller_that_stored_documents_without_it"]["errors"] != []
    # Each channel of the contract carries its own row, and the caller holds it: a
    # property the child uses on retrieved documents is the CACHE's requirement.
    assert all(
        case["row_at_the_caller"]["cache_property_requirements"]
        for case in row["forwarded_cached_properties"].values()
    ), row["forwarded_cached_properties"]
