"""#184 A5, amended by amendment 3 §7: document properties across a stream-replacing step.

Two measured facts shape this file.

1. **Add to Cache hands on no documents.** The platform skips a step wired after it
   and still reads COMPLETE (captures `cap184-cache-put-successor`,
   `cap184-passthrough-ddp-handoff`). So the literal A5 probe
   `[GET, set_ddp X, cache_put, cache_get, set_dpp Y <- ddp X, PATCH, stop]` is not
   evidence that a retrieve loses X. Its retrieve never ran. It is now refused for
   its dead successor, at the cache write's `/cache_ref`.
2. **A triggered retrieve CARRIES the cached documents' properties**, overlaid on
   the current document's, with cached winning a collision (capture
   `cap184-retrieve-ddp-replacement` R1–R4, measured for exactly one current and
   one cached document). The table cells for both reads therefore read
   `cache_overlay`, and the positive probe is the legal STAGED form: a terminal cache
   write in one Branch leg and a separately triggered retrieve in a later one.

Split, combine and generic scripting keep the conservative invalidation. Those cells
are asserted exactly as before, derived from `PROPERTY_SURVIVAL_V1`, never listed
here.

Expected codes and pointers come from the amendment's acceptance text and the
measured captures, never from this implementation's output.
"""

from __future__ import annotations

import json
import sys
from pathlib import Path
from types import SimpleNamespace

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
from boomi_mcp.compiler.process_ir.pipeline import compile_process_ir_v1  # noqa: E402
from boomi_mcp.compiler.process_ir.semantic_validation import lineage  # noqa: E402
from boomi_mcp.errors import (  # noqa: E402
    PROCESS_IR_SCHEMA_INVALID_CARDINALITY,
    PROCESS_IR_SEMANTIC_DYNAMIC_PATH_DDP_NOT_ESTABLISHED,
    PROCESS_IR_SEMANTIC_DYNAMIC_PATH_NO_DYNAMIC_SEGMENT,
    PROCESS_IR_SEMANTIC_LINEAGE_DDP_SCOPE_INVALID,
    PROCESS_IR_SEMANTIC_LINEAGE_PROPERTY_READ_BEFORE_WRITE,
)
from boomi_mcp.models.process_ir import (  # noqa: E402
    ProcessIRValidationError,
    parse_process_ir_v1,
)


def _symbols():
    rest = CC.REST_FAMILY
    return SymbolTableV1(symbols=(
        ComponentSymbolV1(ref="$ref:RCONN", component_id="RCONN",
                          component_type="connector-settings", connector_type=rest),
        ComponentSymbolV1(ref="$ref:GET", component_id="GETOP",
                          component_type="connector-action", connector_type=rest,
                          action_type="GET", connection_ref="$ref:RCONN"),
        ComponentSymbolV1(ref="$ref:PATCH", component_id="PATCHOP",
                          component_type="connector-action", connector_type=rest,
                          action_type="PATCH", connection_ref="$ref:RCONN"),
        ComponentSymbolV1(ref="$ref:PROF", component_id="PROF",
                          component_type="profile.json"),
        ComponentSymbolV1(ref="$ref:CACHE", component_id="CACHE",
                          component_type="documentcache"),
    ))


_GET = {"kind": "connector_call", "operation_ref": "$ref:GET"}
_PATCH = {"kind": "connector_call", "operation_ref": "$ref:PATCH"}
_STOP = {"kind": "stop"}
_PUT = {"kind": "cache_put", "cache_ref": "$ref:CACHE"}
_REMOVE = {"kind": "cache_remove", "cache_ref": "$ref:CACHE"}
_WRITE_X = {"kind": "set_ddp", "name": "X",
            "source_values": [{"value_type": "static", "value": "v"}]}
#: A writer whose value has a dynamic segment, so it can compose a request path.
_DYNAMIC_X = {"kind": "set_ddp", "name": "X", "source_values": [
    {"value_type": "static", "value": "/clients/"},
    {"value_type": "dpp", "property_name": "key", "default_value": ""},
]}
_BOUND_X = {"kind": "connector_call", "operation_ref": "$ref:GET",
            "path_binding": {"property_name": "X"}}


def _read(kind):
    return {"kind": kind, "cache_ref": "$ref:CACHE"}


def _read_x(default=None):
    source = {"value_type": "ddp", "property_name": "X"}
    if default is not None:
        source["default_value"] = default
    return {"kind": "set_dpp", "name": "Y", "source_values": [source]}


def _profile_step(operation):
    return {"operation": operation, "profile_type": "json", "profile_ref": "$ref:PROF",
            "link_element_key": "1", "link_element_name": "root"}


#: `(semantic kind, step operation)` -> the authored node(s) that realise it. A cache
#: retrieval is realised as the READ alone: its write belongs in an earlier Branch leg
#: (see `_staged`), because nothing may follow a cache write on its own path.
_REPLACING_NODE_FACTORIES = {
    ("message", None): lambda: [{"kind": "message", "text": "hello"}],
    ("cache_get", None): lambda: [_read("cache_get")],
    ("document_cache_retrieve", None): lambda: [_read("document_cache_retrieve")],
    ("data_process", "split_documents"): lambda: [
        {"kind": "data_process", "steps": [_profile_step("split_documents")]}],
    ("data_process", "combine_documents"): lambda: [
        {"kind": "data_process", "steps": [_profile_step("combine_documents")]}],
    ("data_process", "custom_scripting"): lambda: [
        {"kind": "data_process", "steps": [
            {"operation": "custom_scripting", "language": "groovy2",
             "script": "// emits its own documents"}]}],
}


def _cells(verdict_is_survives):
    return [
        pytest.param(cell, id="{0}.{1}".format(cell[0], cell[1]))
        for cell, verdict in sorted(
            lineage.PROPERTY_SURVIVAL_V1.items(), key=lambda item: (item[0][0], str(item[0][1]))
        )
        if (verdict == "survives") == verdict_is_survives
    ]


def _diagnostics(steps):
    doc = {"version": "1", "body": {"kind": "sequence", "steps": steps}}
    try:
        compile_process_ir_v1(parse_process_ir_v1(doc), _symbols())
    except (ProcessIRCompileError, ProcessIRValidationError) as exc:
        return tuple((item.code, item.path) for item in exc.diagnostics)
    return ()


def _codes(steps):
    return {code for code, _path in _diagnostics(steps)}


def _staged(*legs):
    """`[GET, branch[legs]]`: every leg receives its own copy of the GET's documents."""
    return [_GET, {"kind": "branch", "legs": list(legs)}]


def _leg(steps, terminal):
    return {"steps": list(steps), "terminal": terminal}


# ---------------------------------------------------------------------------
# The table and the refuted probe
# ---------------------------------------------------------------------------


def test_every_survival_table_row_has_a_node_factory():
    """The coverage claim is the table's own row set, in both directions."""
    assert set(_REPLACING_NODE_FACTORIES) == set(lineage.PROPERTY_SURVIVAL_V1), {
        "rows_without_a_factory": sorted(
            map(str, set(lineage.PROPERTY_SURVIVAL_V1) - set(_REPLACING_NODE_FACTORIES))),
        "factories_without_a_row": sorted(
            map(str, set(_REPLACING_NODE_FACTORIES) - set(lineage.PROPERTY_SURVIVAL_V1))),
    }
    assert _cells(True) and _cells(False), "a side of the table is empty — the tests would be vacuous"


def test_the_issue_probe_is_refused_as_a_read_before_write():
    """A5's literal probe is refused, but for its DEAD SUCCESSOR, not for property loss.

    The name is kept (a registered node id). Amendment 3 §7: the probe's retrieve
    never executed on the platform, because Add to Cache hands on no documents. It
    was never evidence about property loss, and the compiler now refuses the write
    that ends the path. Neither lineage code is served beside that refusal: the
    document never reaches lineage validation.
    """
    steps = [_GET, _WRITE_X, _PUT, _read("cache_get"), _read_x(), _PATCH, _STOP]
    diagnostics = _diagnostics(steps)
    assert (PROCESS_IR_SCHEMA_INVALID_CARDINALITY, "/body/steps/2/cache_ref") in diagnostics, diagnostics
    codes = {c for c, _ in diagnostics}
    assert PROCESS_IR_SEMANTIC_LINEAGE_PROPERTY_READ_BEFORE_WRITE not in codes, diagnostics
    assert PROCESS_IR_SEMANTIC_LINEAGE_DDP_SCOPE_INVALID not in codes, diagnostics


def test_the_branch_point_compiled_the_probe():
    """The 'before' half of the pair, read from the archived pre-baseline measurement."""
    probe = _ROOT / "docs/architecture/evidence/issue-184/predicates/probe_cbab28f.jsonl"
    rows = {json.loads(line)["form"]: json.loads(line) for line in probe.read_text().splitlines() if line}
    assert rows["stale_ddp_after_cacheget"].get("compile") == "ok+emit", rows["stale_ddp_after_cacheget"]


@pytest.mark.parametrize("cell", _cells(False))
def test_every_kind_the_table_does_not_mark_survives_invalidates_the_read(cell):
    """Every cell not measured to survive, by its verdict.

    - `cache_overlay` (both reads): the STAGED read carries the cached X, and without
      the cached write the same read is a read-before-write at the reader.
    - Every other non-surviving cell keeps the conservative invalidation on its linear
      form.
    """
    verdict = lineage.PROPERTY_SURVIVAL_V1[cell]
    between = _REPLACING_NODE_FACTORIES[cell]()
    if verdict == "cache_overlay":
        carried = _staged(_leg([_WRITE_X], _PUT), _leg(between + [_read_x(), _PATCH], _STOP))
        assert _diagnostics(carried) == (), _diagnostics(carried)
        reader = "/body/steps/1/legs/1/steps/{0}".format(len(between))
        uncached = _staged(_leg([], _PUT), _leg(between + [_read_x(), _PATCH], _STOP))
        diagnostics = _diagnostics(uncached)
        assert (PROCESS_IR_SEMANTIC_LINEAGE_PROPERTY_READ_BEFORE_WRITE, reader) in diagnostics, diagnostics
        return
    steps = [_GET, _WRITE_X] + between + [_read_x(), _PATCH, _STOP]
    reader = "/body/steps/{0}".format(2 + len(between))
    diagnostics = _diagnostics(steps)
    assert (PROCESS_IR_SEMANTIC_LINEAGE_PROPERTY_READ_BEFORE_WRITE, reader) in diagnostics, diagnostics


@pytest.mark.parametrize("cell", _cells(True))
def test_a_kind_the_table_marks_survives_keeps_the_value(cell):
    between = _REPLACING_NODE_FACTORIES[cell]()
    steps = [_GET, _WRITE_X] + between + [_read_x(), _PATCH, _STOP]
    assert PROCESS_IR_SEMANTIC_LINEAGE_PROPERTY_READ_BEFORE_WRITE not in _codes(steps)


def test_a_defaulted_read_is_still_satisfied():
    steps = _staged(_leg([], _PUT), _leg([_read("cache_get"), _read_x(default="d"), _PATCH], _STOP))
    assert _diagnostics(steps) == ()


def test_a_rewrite_after_the_replacement_satisfies_the_read():
    steps = _staged(_leg([], _PUT), _leg([_read("cache_get"), _WRITE_X, _read_x(), _PATCH], _STOP))
    assert _diagnostics(steps) == ()


def test_a_sibling_leg_write_keeps_its_scope_diagnostic():
    """No replacement on the reader's path: the different-copy code still applies."""
    doc_steps = [{"kind": "branch", "legs": [
        {"steps": [_GET, _WRITE_X], "terminal": _STOP},
        {"steps": [_GET, _read_x()], "terminal": _STOP},
    ]}]
    diagnostics = _diagnostics(doc_steps)
    assert (PROCESS_IR_SEMANTIC_LINEAGE_DDP_SCOPE_INVALID, "/body/steps/0/legs/1/steps/1") in diagnostics, diagnostics


def test_the_lattice_drop_is_load_bearing(monkeypatch):
    """Non-vacuity for the cells that still invalidate: with the drop disabled, a split
    between the writer and the reader is no longer reported."""
    steps = [_GET, _WRITE_X] + _REPLACING_NODE_FACTORIES[("data_process", "split_documents")]() + [
        _read_x(), _PATCH, _STOP]
    assert PROCESS_IR_SEMANTIC_LINEAGE_PROPERTY_READ_BEFORE_WRITE in _codes(steps)
    monkeypatch.setattr(lineage, "_drop_replaced_document_keys", lambda state, keep: (state, frozenset()))
    assert PROCESS_IR_SEMANTIC_LINEAGE_PROPERTY_READ_BEFORE_WRITE not in _codes(steps)


# ---------------------------------------------------------------------------
# #184 amendment 3 §7 — the retrieve overlay through the public compile route
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("read_kind", ["cache_get", "document_cache_retrieve"])
def test_a_bound_request_path_rests_on_the_cached_writer(read_kind):
    """R1/R3: the retrieved document carries the cached X, so a path bound to X is
    composed by the writer that ran on the ORIGINAL cached document."""
    ok = _staged(_leg([_DYNAMIC_X], _PUT), _leg([_read(read_kind), _BOUND_X], _STOP))
    assert _diagnostics(ok) == (), _diagnostics(ok)
    missing = _staged(_leg([], _PUT), _leg([_read(read_kind), _BOUND_X], _STOP))
    assert PROCESS_IR_SEMANTIC_DYNAMIC_PATH_DDP_NOT_ESTABLISHED in _codes(missing)


def test_a_current_only_property_is_not_carried_without_a_singleton_proof():
    """R2 is attested for ONE current and ONE cached document only. A GET's output count
    is not proved, so a property written on the current documents before the read is
    not established after it: the fix is to write it after the read."""
    steps = _staged(_leg([], _PUT), _leg([_WRITE_X, _read("cache_get"), _read_x(), _PATCH], _STOP))
    diagnostics = _diagnostics(steps)
    assert (PROCESS_IR_SEMANTIC_LINEAGE_PROPERTY_READ_BEFORE_WRITE, "/body/steps/1/legs/1/steps/2") in diagnostics, diagnostics


def test_several_cached_cohorts_guarantee_only_what_every_one_carries():
    """Add to Cache APPENDS. The retrieved documents may come from either staging leg,
    so a key only one leg wrote is not guaranteed."""
    one_sided = _staged(
        _leg([_WRITE_X], _PUT), _leg([], _PUT),
        _leg([_read("cache_get"), _read_x(), _PATCH], _STOP),
    )
    diagnostics = _diagnostics(one_sided)
    assert (PROCESS_IR_SEMANTIC_LINEAGE_PROPERTY_READ_BEFORE_WRITE, "/body/steps/1/legs/2/steps/1") in diagnostics, diagnostics
    both = _staged(
        _leg([_WRITE_X], _PUT), _leg([_WRITE_X], _PUT),
        _leg([_read("cache_get"), _read_x(), _PATCH], _STOP),
    )
    assert _diagnostics(both) == (), _diagnostics(both)


def test_a_bound_path_must_be_sound_for_every_possible_cached_writer():
    """Two cohorts, two writers of X: one composes a dynamic path, the other a static
    one. The retrieved document may carry either, so the binding is refused."""
    mixed = _staged(
        _leg([_DYNAMIC_X], _PUT), _leg([_WRITE_X], _PUT),
        _leg([_read("cache_get"), _BOUND_X], _STOP),
    )
    assert PROCESS_IR_SEMANTIC_DYNAMIC_PATH_NO_DYNAMIC_SEGMENT in _codes(mixed), _diagnostics(mixed)
    sound = _staged(
        _leg([_DYNAMIC_X], _PUT), _leg([_DYNAMIC_X], _PUT),
        _leg([_read("cache_get"), _BOUND_X], _STOP),
    )
    assert _diagnostics(sound) == (), _diagnostics(sound)


def test_a_whole_cache_removal_clears_the_cached_properties():
    """Amendment 3 §7: "Whole-cache removal clears these summaries" — for a removal that has run
    whenever what reads the cache after it runs.

    The first assertion is this test as it stood before correction batch 21a. The removal
    stands behind the GET in front of the Branch, so the walk cannot prove it runs at all; but
    that GET feeds EVERY leg, so the later leg that reads the cache runs only when the removal
    leg did (`lineage._removal_runs_before_anything_after_it`), and the cohort it cleared is
    gone for that read. Batch 21a first gated the clear on the whole-run proof alone and this
    shape came back clean — an over-refusal of its own, reversed there.

    A removal the later legs do NOT depend on keeps what the cache may hold: behind a GET of
    its own leg it may be skipped while the reading leg still runs, so the cache after it is
    the meet of "it ran" and "it was skipped" — §7 unions possible cohorts and says "Never
    discard an inconvenient writer alternative". Measured before the correction: that
    leg-local form was clean, while the same legs with no removal are refused."""
    steps = _staged(
        _leg([_WRITE_X], _PUT), _leg([], _REMOVE),
        _leg([_read("cache_get"), _read_x(), _PATCH], _STOP),
    )
    diagnostics = _diagnostics(steps)
    assert (PROCESS_IR_SEMANTIC_LINEAGE_PROPERTY_READ_BEFORE_WRITE, "/body/steps/1/legs/2/steps/1") in diagnostics, diagnostics
    reads = _leg([_read("cache_get"), _read_x(), _PATCH], _STOP)
    refused = (PROCESS_IR_SEMANTIC_LINEAGE_PROPERTY_READ_BEFORE_WRITE, "/body/steps/1/legs/3/steps/1")
    # The shared trigger: X-less documents, the removal, an X refill, the read. The removal ran
    # whenever the read runs, so only the refill reaches it.
    assert _diagnostics(_staged(_leg([], _PUT), _leg([], _REMOVE), _leg([_WRITE_X], _PUT), reads)) == ()
    # The removal behind its OWN leg's GET may be skipped while the read runs: refused ...
    assert _diagnostics(_staged(_leg([], _PUT), _leg([_GET], _REMOVE), _leg([_WRITE_X], _PUT), reads)) == (refused,)
    # ... exactly as the same legs with no removal at all.
    assert refused[0] in _codes(_staged(_leg([], _PUT), _leg([_WRITE_X], _PUT), reads))


def test_the_cohort_meet_is_load_bearing(monkeypatch):
    """Non-vacuity: with every cohort claiming to carry X, the one-sided staging case
    above is accepted — so its refusal comes from the recorded cohorts."""
    steps = _staged(
        _leg([_WRITE_X], _PUT), _leg([], _PUT),
        _leg([_read("cache_get"), _read_x(), _PATCH], _STOP),
    )
    assert PROCESS_IR_SEMANTIC_LINEAGE_PROPERTY_READ_BEFORE_WRITE in _codes(steps)
    real = lineage._cohort_at_write

    def claims_x(on_documents, writers, stream):
        cohort = real(on_documents, writers, stream)
        return cohort._replace(guaranteed=cohort.guaranteed | {("ddp", "X")})

    monkeypatch.setattr(lineage, "_cohort_at_write", claims_x)
    assert PROCESS_IR_SEMANTIC_LINEAGE_PROPERTY_READ_BEFORE_WRITE not in _codes(steps)


# ---------------------------------------------------------------------------
# #184 amendment 3 §7 — the overlay policy as a pure transfer
# ---------------------------------------------------------------------------
#
# These are TRANSFER tests, not public-route admission evidence (amendment 3 §7): a
# public graph cannot yet prove a singleton cached document, because every document
# producer's count is unproved.

X = ("ddp", "X")


def _read_semantic(external_writer=False):
    return SimpleNamespace(cache_ref="$ref:CACHE", external_writer=external_writer)


def _state(cohorts=(), document=frozenset({X})):
    return lineage._State(
        document=document,
        cohorts=frozenset(("$ref:CACHE", cohort) for cohort in cohorts),
    )


def _stream(count):
    return lineage._Stream(lineage.STREAM_KNOWN, count=count)


def _cohort(guaranteed=(), possible=(), alternatives=(), count=lineage.COUNT_ONE):
    return lineage._Cohort(
        frozenset(guaranteed),
        None if possible is None else frozenset(possible),
        frozenset(alternatives),
        count,
    )


def _overlay(state, writers, count, external_writer=False):
    on_documents = frozenset(key for key in state.document if key[0] == "ddp")
    return lineage._overlay_cache_read(
        _read_semantic(external_writer), state, writers, on_documents, frozenset(), _stream(count)
    )


def test_definite_cached_absence_carries_the_current_writer_only_under_a_singleton():
    """R2: the cached document has no X, the current one has X, and X survives."""
    cohort = _cohort(guaranteed=(), possible=())
    state, writers, on_documents, invalidated, _count = _overlay(
        _state([cohort]), {X: ("current-writer",)}, lineage.COUNT_ONE)
    assert X in on_documents and state.establishes(X)
    assert writers[X] == ("current-writer",)
    # unknown multiplicity: nothing current is carried
    state, writers, on_documents, invalidated, _count = _overlay(
        _state([cohort]), {X: ("current-writer",)}, lineage.COUNT_UNKNOWN)
    assert X not in on_documents and not state.establishes(X)
    assert X in invalidated and X not in writers


def test_definite_cached_presence_selects_the_cached_writer():
    """R1/R4: a collision is won by the cached value, so the cached writer composes it."""
    cohort = _cohort(guaranteed=(X,), possible=(X,), alternatives=((X, "cached-writer"),))
    _state_after, writers, on_documents, _inv, count = _overlay(
        _state([cohort]), {X: ("current-writer",)}, lineage.COUNT_ONE)
    assert X in on_documents
    assert writers[X] == ("cached-writer",)
    assert count == lineage.COUNT_ONE


def test_possible_cached_presence_keeps_both_writers_and_unknown_provenance():
    cohort = _cohort(guaranteed=(), possible=None)
    _state_after, writers, on_documents, _inv, _count = _overlay(
        _state([cohort]), {X: ("current-writer",)}, lineage.COUNT_ONE)
    assert X in on_documents
    assert set(writers[X]) == {"current-writer", lineage.UNKNOWN_WRITER}


def test_several_cohorts_meet_their_guarantees_and_union_their_writers():
    first = _cohort(guaranteed=(X,), possible=(X,), alternatives=((X, "writer-a"),))
    second = _cohort(guaranteed=(X,), possible=(X,), alternatives=((X, "writer-b"),))
    _state_after, writers, on_documents, _inv, count = _overlay(
        _state([first, second]), {}, lineage.COUNT_ONE)
    assert X in on_documents
    assert set(writers[X]) == {"writer-a", "writer-b"}
    assert count == lineage.COUNT_UNKNOWN


def test_an_external_writer_contributes_an_unknown_cohort():
    cohort = _cohort(guaranteed=(X,), possible=(X,), alternatives=((X, "cached-writer"),))
    _state_after, writers, on_documents, _inv, _count = _overlay(
        _state([cohort]), {}, lineage.COUNT_ONE, external_writer=True)
    assert X not in on_documents and X not in writers


def test_an_empty_cache_guarantees_nothing():
    state, writers, on_documents, invalidated, count = _overlay(
        _state([]), {X: ("current-writer",)}, lineage.COUNT_ONE)
    assert on_documents == frozenset() and not state.establishes(X)
    assert X in invalidated and count == lineage.COUNT_UNKNOWN
