"""Prepared context + reference collector (issue #143, M12.8) — slice 2.

Still DARK: nothing in the compiler, adapters, builders or MCP tools calls these.

The two properties worth the most here are the ones a later slice would be
expensive to retrofit:

``the snapshot is genuinely isolated``
    ``ProcessIRV1`` is strict but not frozen, so a caller keeps a mutable model.
    Validation exists to gate a mutation, which makes "payload changed after it
    was validated" the single most consequential failure this layer could have.
    It is tested by mutating the caller's object after preparing and proving the
    prepared context did not move.

``connector refs are NOT claimed by the generic codes``
    #140's operation/connection diagnostics distinguish three conditions the
    generic "did not resolve" code cannot. Unification must not quietly downgrade
    them, so a connector-only flow must produce no reference findings here at all.
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

import pytest

_ROOT = Path(__file__).resolve().parent.parent
for _p in (str(_ROOT), str(_ROOT / "src")):
    if _p not in sys.path:
        sys.path.insert(0, _p)

from boomi_mcp.compiler.process_ir.contracts import ComponentSymbolV1, SymbolTableV1
from boomi_mcp.compiler.process_ir.semantic_validation.context import (
    prepare_validation_context,
)
from boomi_mcp.compiler.process_ir.semantic_validation.references import (
    collect_reference_findings,
)
from boomi_mcp.errors import (
    PROCESS_IR_REFERENCE_COMPONENT_NOT_FOUND,
    PROCESS_IR_REFERENCE_COMPONENT_TYPE_MISMATCH,
)
from boomi_mcp.models.process_ir import parse_process_ir_v1

_FIXTURES = _ROOT / "tests" / "fixtures" / "process_ir"
GOLDEN_DOCS = json.loads((_FIXTURES / "process_ir_v1.json").read_text())

# Component type per ref, matching the golden doc's roles.
_TYPE_BY_REF = {
    "$ref:db_conn": "connector-settings",
    "$ref:db_op": "connector-action",
    "$ref:map": "transform.map",
    "$ref:cache": "documentcache",
    "$ref:profile": "profile.json",
    "$ref:profile2": "profile.xml",
    "$ref:profile3": "profile.json",
    "$ref:sub": "process",
}


def _linear_ir():
    return parse_process_ir_v1(GOLDEN_DOCS["linear_flow"])


def _symbols_for(cfg, *, omit=(), retype=None):
    """Build a symbol table covering every ref the CFG mentions.

    ``omit`` drops refs (to exercise "not found"); ``retype`` rewrites one ref's
    component type (to exercise "type mismatch").
    """
    retype = retype or {}
    refs = set()

    def _collect(semantic):
        for role in ("connection_ref", "operation_ref", "map_ref", "cache_ref", "process_ref"):
            value = getattr(semantic, role, None)
            if isinstance(value, str) and value:
                refs.add(value)
        for container in ("steps", "source_values"):
            for item in getattr(semantic, container, ()) or ():
                nested = getattr(item, "profile_ref", None)
                if isinstance(nested, str) and nested:
                    refs.add(nested)

    for node in cfg.nodes:
        _collect(node.semantic)

    return SymbolTableV1(
        symbols=tuple(
            ComponentSymbolV1(
                ref=ref,
                component_id="id" + str(index),
                component_type=retype.get(ref, _TYPE_BY_REF.get(ref, "profile.json")),
            )
            for index, ref in enumerate(sorted(refs))
            if ref not in omit
        )
    )


def _prepared(**kwargs):
    ir = _linear_ir()
    bare = prepare_validation_context(ir, SymbolTableV1(symbols=()))
    return prepare_validation_context(ir, _symbols_for(bare.cfg, **kwargs))


# ---------------------------------------------------------------------------
# snapshot isolation — the time-of-check/time-of-use guard
# ---------------------------------------------------------------------------


def test_the_prepared_ir_is_a_snapshot_not_the_callers_object():
    ir = _linear_ir()
    prepared = prepare_validation_context(ir, SymbolTableV1(symbols=()))
    assert prepared.ir is not ir


def test_mutating_the_callers_ir_after_preparing_does_not_move_the_context():
    """The whole point of snapshotting: validation gates a mutation, so a
    payload that changes after it was checked must not reach emission."""
    ir = _linear_ir()
    prepared = prepare_validation_context(ir, SymbolTableV1(symbols=()))
    before = len(prepared.cfg.nodes)

    # Mutate the caller's model in place, after validation prepared its context.
    ir.body.steps = tuple(ir.body.steps[:2])

    assert len(prepared.cfg.nodes) == before
    assert len(prepared.ir.body.steps) != len(ir.body.steps)


def test_preparation_is_pure_for_the_same_input():
    first = prepare_validation_context(_linear_ir(), SymbolTableV1(symbols=()))
    second = prepare_validation_context(_linear_ir(), SymbolTableV1(symbols=()))
    assert first.cfg == second.cfg


# ---------------------------------------------------------------------------
# indexes
# ---------------------------------------------------------------------------


def test_every_cfg_node_is_indexed_by_id():
    prepared = prepare_validation_context(_linear_ir(), SymbolTableV1(symbols=()))
    assert set(prepared.node_by_id) == {n.node_id for n in prepared.cfg.nodes}
    for node in prepared.cfg.nodes:
        assert prepared.node(node.node_id) is node


def test_an_unknown_node_id_resolves_to_none_rather_than_raising():
    prepared = prepare_validation_context(_linear_ir(), SymbolTableV1(symbols=()))
    assert prepared.node("n9999") is None
    assert prepared.successors("n9999") == ()
    assert prepared.predecessors("n9999") == ()


def test_outgoing_and_incoming_cover_every_edge_exactly_once():
    prepared = prepare_validation_context(_linear_ir(), SymbolTableV1(symbols=()))
    out = [e for edges in prepared.outgoing.values() for e in edges]
    inc = [e for edges in prepared.incoming.values() for e in edges]
    assert sorted(e.edge_id for e in out) == sorted(e.edge_id for e in prepared.cfg.edges)
    assert sorted(e.edge_id for e in inc) == sorted(e.edge_id for e in prepared.cfg.edges)


def test_outgoing_edges_are_ordered_by_local_ordinal():
    """Leg order is MEANING — the lineage phase reads earlier legs as visible to
    later ones, so an arbitrary grouping order would change which leg is
    'earlier'."""
    control = parse_process_ir_v1(GOLDEN_DOCS["control_flow"])
    prepared = prepare_validation_context(control, SymbolTableV1(symbols=()))
    for edges in prepared.outgoing.values():
        ordinals = [e.local_ordinal for e in edges]
        assert ordinals == sorted(ordinals)


def test_edge_id_tiebreak_is_numeric_not_lexical():
    """``e10`` must sort after ``e2``. Pinned directly on the helper because a
    real CFG may not happen to produce a colliding local ordinal."""
    from boomi_mcp.compiler.process_ir.semantic_validation.context import _edge_sort_id

    assert sorted(["e10", "e2", "e1"], key=_edge_sort_id) == ["e1", "e2", "e10"]


def test_symbols_are_indexed_by_ref():
    prepared = _prepared()
    for symbol in prepared.symbols.symbols:
        assert prepared.symbol(symbol.ref) is symbol
    assert prepared.symbol(None) is None
    assert prepared.symbol("$ref:absent") is None


# ---------------------------------------------------------------------------
# preparation defects are NOT report entries
# ---------------------------------------------------------------------------


def test_a_preparation_defect_escapes_rather_than_becoming_a_finding():
    """ADR-001 §7: PROCESS_IR_COMPILE_* is not a family this issue owns, so a
    snapshot/lowering failure must reach the compiler's own _guarded boundary
    instead of being relabelled as a user-facing validation finding."""
    with pytest.raises(Exception) as excinfo:
        prepare_validation_context(object(), SymbolTableV1(symbols=()))
    # whatever it is, it is not a ValidationReport being returned quietly
    assert excinfo.value is not None


# ---------------------------------------------------------------------------
# reference collector
# ---------------------------------------------------------------------------


def test_a_fully_resolved_flow_produces_no_reference_findings():
    findings, facts = collect_reference_findings(_prepared())
    assert findings == ()
    assert facts.unresolved == set()


def test_nested_profile_refs_are_walked_not_just_top_level_roles():
    """A data_process step and a set-property source each carry their own
    profile_ref. Walking only top-level fields would skip every one of them."""
    _findings, facts = collect_reference_findings(_prepared())
    profile_refs = {r for r in facts.resolved if "profile" in r}
    assert profile_refs, "no nested profile ref was resolved"


def test_a_missing_map_reference_is_reported_not_found():
    findings, facts = collect_reference_findings(_prepared(omit={"$ref:map"}))
    codes = {f.code for f in findings}
    assert PROCESS_IR_REFERENCE_COMPONENT_NOT_FOUND in codes
    assert "$ref:map" in facts.unresolved


def test_a_wrong_typed_cache_reference_is_reported_as_a_type_mismatch():
    findings, _facts = collect_reference_findings(
        _prepared(retype={"$ref:cache": "transform.map"})
    )
    codes = {f.code for f in findings}
    assert PROCESS_IR_REFERENCE_COMPONENT_TYPE_MISMATCH in codes


def test_connector_operation_and_connection_refs_are_left_to_the_140_codes():
    """Dropping BOTH connector symbols must produce no finding here — those
    conditions keep their specialized #140 diagnostics, which say strictly more
    than 'a ref did not resolve'."""
    findings, _facts = collect_reference_findings(
        _prepared(omit={"$ref:db_conn", "$ref:db_op"})
    )
    assert findings == ()


def test_collection_accumulates_rather_than_stopping_at_the_first_bad_ref():
    findings, facts = collect_reference_findings(
        _prepared(omit={"$ref:map", "$ref:cache"})
    )
    assert len({f.internal_node_id for f in findings}) >= 2
    assert {"$ref:map", "$ref:cache"} <= facts.unresolved


def test_findings_are_reference_phase_errors_and_carry_a_type_class():
    findings, _facts = collect_reference_findings(_prepared(omit={"$ref:map"}))
    for item in findings:
        assert item.phase == "reference"
        assert item.severity == "error"
        assert [e.key for e in item.evidence] == ["component_type_class"]


def test_reference_findings_carry_no_authored_ref_text():
    """The redaction boundary, checked on a real finding rather than in the
    abstract: the ref that failed must not appear anywhere in the finding."""
    findings, _facts = collect_reference_findings(_prepared(omit={"$ref:map"}))
    assert findings
    for item in findings:
        blob = item.model_dump_json()
        assert "$ref:" not in blob
        assert "db_conn" not in blob
