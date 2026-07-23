"""Unit tests for the #139 M12.4 legacy-config -> ProcessIR adapters.

These pin the adapter *boundary* contract independent of byte parity (which the
process-builder goldens + the emitter-parity oracle already lock): an adapter
produces a validated ``ProcessIRV1`` plus the exact symbol requirements the
emitter validates, records safe unknown fields as no-op paths instead of
rejecting them, and NEVER carries XML, shape ids, layout, CFG edges, or
credentials out of the boundary (ADR-001 §6).
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

from boomi_mcp.compiler.process_ir.legacy_adapters import (
    FLOW_SEQUENCE_DIALECT,
    RESERVED_DIALECTS,
    WRAPPER_SUBPROCESS_DIALECT,
    adapter_for,
    is_migrated,
    migrated_dialects,
)
from boomi_mcp.compiler.process_ir.legacy_adapters.contracts import (
    LegacyAdapterResultV1,
)
from boomi_mcp.compiler.process_ir.legacy_adapters.flow_sequence import (
    adapt_flow_sequence,
)
from boomi_mcp.compiler.process_ir.legacy_adapters.wrapper_subprocess import (
    adapt_wrapper_subprocess,
)
from boomi_mcp.models.process_ir import ProcessIRV1

_C1 = "11111111-1111-1111-1111-111111111111"
_C2 = "22222222-2222-2222-2222-222222222222"
_DB_CONN = "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"
_DB_OP = "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb"
_REST_CONN = "cccccccc-cccc-cccc-cccc-cccccccccccc"
_REST_OP = "dddddddd-dddd-dddd-dddd-dddddddddddd"

# Emission/layout/CFG tokens that must never appear in an adapter result.
_FORBIDDEN_SUBSTRINGS = (
    "<",
    ">",
    "dragpoint",
    "toShape",
    "shapetype",
    'shape"',
    "shape1",
    '"x"',
    '"y"',
)


def _wrapper_cfg(**over):
    cfg = {
        "process_kind": "wrapper_subprocess",
        "process_calls": [{"process_id": _C1}, {"process_id": _C2}],
    }
    cfg.update(over)
    return cfg


def _flow_cfg(**over):
    cfg = {
        "process_kind": "database_to_api_sync",
        "source": {
            "connector_type": "database",
            "connection_id": _DB_CONN,
            "operation_id": _DB_OP,
            "action_type": "Get",
        },
        "transform": {"mode": "passthrough"},
        "target": {
            "connector_type": "rest",
            "connection_id": _REST_CONN,
            "operation_id": _REST_OP,
            "action_type": "POST",
        },
        "flow_sequence": [{"kind": "map_ref", "map_ref": "MAP-1", "label": "m"}],
    }
    cfg.update(over)
    return cfg


# ---------------------------------------------------------------------------
# Registry
# ---------------------------------------------------------------------------


def test_registry_reports_only_migrated_dialects():
    assert is_migrated(WRAPPER_SUBPROCESS_DIALECT)
    assert is_migrated(FLOW_SEQUENCE_DIALECT)
    assert migrated_dialects() == {WRAPPER_SUBPROCESS_DIALECT, FLOW_SEQUENCE_DIALECT}


def test_registry_reserved_dialects_are_not_migrated():
    # Reserved-but-pending dialects resolve to None so the legacy renderer stays
    # authoritative — they must not masquerade as migrated.
    for dialect in RESERVED_DIALECTS:
        assert adapter_for(dialect) is None
        assert not is_migrated(dialect)


def test_registry_unknown_dialect_returns_none():
    assert adapter_for("nonexistent_dialect") is None


def test_registry_is_immutable():
    from boomi_mcp.compiler.process_ir.legacy_adapters import registry

    with pytest.raises(TypeError):
        registry._MIGRATED["x"] = lambda c: None


# ---------------------------------------------------------------------------
# Result shape + no-leak invariant
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "adapt,cfg",
    [(adapt_wrapper_subprocess, _wrapper_cfg()), (adapt_flow_sequence, _flow_cfg())],
)
def test_adapter_returns_validated_ir(adapt, cfg):
    result = adapt(cfg)
    assert isinstance(result, LegacyAdapterResultV1)
    assert isinstance(result.process_ir, ProcessIRV1)


@pytest.mark.parametrize(
    "adapt,cfg",
    [(adapt_wrapper_subprocess, _wrapper_cfg()), (adapt_flow_sequence, _flow_cfg())],
)
def test_adapter_result_leaks_no_emission_artifacts(adapt, cfg):
    result = adapt(cfg)
    blob = json.dumps(result.model_dump(mode="json"))
    for token in _FORBIDDEN_SUBSTRINGS:
        assert token not in blob, f"adapter result leaked {token!r}"


def test_flow_requirements_are_occurrence_scoped_and_typed(_pin=None):
    # #139B: requirements are keyed by an occurrence-scoped ALIAS
    # ($ref:legacy.adapter:<pointer>); the real id lives in legacy_selector, and
    # the emitter type comes from the plan. Index by source_pointer.
    result = adapt_flow_sequence(_flow_cfg())
    by_ptr = {r.source_pointer: r for r in result.symbol_requirements}
    assert by_ptr["/source/connection_id"].expected_component_type == "connector-settings"
    assert by_ptr["/source/operation_id"].expected_component_type == "connector-action"
    assert by_ptr["/target/connection_id"].expected_component_type == "connector-settings"
    assert by_ptr["/target/operation_id"].expected_component_type == "connector-action"
    assert by_ptr["/flow_sequence/0/map_ref"].expected_component_type == "transform.map"
    # Aliases are path-only (no authored id) and unique per occurrence.
    for ptr, r in by_ptr.items():
        assert r.ir_ref == f"$ref:legacy.adapter:{ptr}"
    assert len({r.ir_ref for r in result.symbol_requirements}) == len(result.symbol_requirements)
    # legacy_selector carries the ORIGINAL id.
    assert by_ptr["/source/operation_id"].legacy_selector == _DB_OP
    assert by_ptr["/target/operation_id"].legacy_selector == _REST_OP
    assert by_ptr["/flow_sequence/0/map_ref"].legacy_selector == "MAP-1"
    # Connector metadata rides on the OPERATION requirement only; not connections.
    assert by_ptr["/source/operation_id"].connector_type == "database"
    assert by_ptr["/source/operation_id"].action_type == "Get"
    assert by_ptr["/target/operation_id"].connector_type == "rest"
    assert by_ptr["/target/operation_id"].action_type == "POST"
    assert by_ptr["/source/connection_id"].connector_type is None
    assert by_ptr["/target/connection_id"].connector_type is None


def test_flow_reused_id_gets_distinct_aliases_with_independent_metadata():
    # #139B: source op == target op reused with different families -> TWO aliases,
    # one real id in legacy_selector, each keeping its own family/action.
    shared = "cccccccc-cccc-cccc-cccc-cccccccccccc"
    result = adapt_flow_sequence(_flow_cfg(
        source={"connector_type": "database", "connection_id": _DB_CONN, "operation_id": shared, "action_type": "Get"},
        target={"connector_type": "rest", "connection_id": _REST_CONN, "operation_id": shared, "action_type": "POST"},
    ))
    by_ptr = {r.source_pointer: r for r in result.symbol_requirements}
    src, tgt = by_ptr["/source/operation_id"], by_ptr["/target/operation_id"]
    assert src.ir_ref != tgt.ir_ref                       # distinct aliases
    assert src.legacy_selector == tgt.legacy_selector == shared  # same real id
    assert src.connector_type == "database" and src.action_type == "Get"
    assert tgt.connector_type == "rest" and tgt.action_type == "POST"


def test_flow_cross_type_id_reuse_yields_typed_requirements():
    # #139B: one id used as a map_ref and TWICE as a document_cache_id -> three
    # DISTINCT aliases resolving to the same real id, with the correct type split
    # (was SYMBOL_UNRESOLVED pre-#139B).
    shared = "cccccccc-cccc-cccc-cccc-cccccccccccc"
    result = adapt_flow_sequence(_flow_cfg(flow_sequence=[
        {"kind": "map_ref", "map_ref": shared},
        {"kind": "doccacheload", "document_cache_id": shared},
        {"kind": "doccacheretrieve", "document_cache_id": shared},
    ]))
    by_ptr = {r.source_pointer: r for r in result.symbol_requirements}
    ptrs = ["/flow_sequence/0/map_ref", "/flow_sequence/1/document_cache_id", "/flow_sequence/2/document_cache_id"]
    # All three occurrences present, distinct aliases, one real id.
    assert {r.ir_ref for p in ptrs for r in [by_ptr[p]]} == {f"$ref:legacy.adapter:{p}" for p in ptrs}
    assert len({by_ptr[p].ir_ref for p in ptrs}) == 3
    assert all(by_ptr[p].legacy_selector == shared for p in ptrs)
    # Type split: one transform.map, two documentcache.
    types = [by_ptr[p].expected_component_type for p in ptrs]
    assert types == ["transform.map", "documentcache", "documentcache"]


def test_flow_dead_root_target_connection_and_operation_excluded():
    # #139B: a branch terminal makes the root target dead — the codec drops it, so
    # NEITHER its connection nor its operation alias reaches the CFG (both selectors
    # absent), while the emitted leg targets remain.
    dead_conn = "deadc000-0000-0000-0000-00000000c0nn"[:36]
    dead_op = "dead0000-0000-0000-0000-0000000000op"[:36]
    result = adapt_flow_sequence(_flow_cfg(
        target={"connector_type": "rest", "connection_id": dead_conn, "operation_id": dead_op, "action_type": "POST"},
        flow_sequence=[{"kind": "branch", "legs": [
            {"steps": [{"kind": "map_ref", "map_ref": "MAP-A"}], "target": {"connector_type": "rest", "connection_id": _REST_CONN, "operation_id": _REST_OP, "action_type": "POST", "label": "A"}},
            {"steps": [{"kind": "map_ref", "map_ref": "MAP-B"}], "target": {"connector_type": "rest", "connection_id": "55555555-5555-5555-5555-555555555555", "operation_id": "66666666-6666-6666-6666-666666666666", "action_type": "POST", "label": "B"}},
        ]}],
    ))
    selectors = {r.legacy_selector for r in result.symbol_requirements}
    assert dead_conn not in selectors and dead_op not in selectors  # both dead-target refs excluded
    assert _REST_OP in selectors                                     # emitted leg target present


def test_flow_aliasing_is_deterministic_across_repeated_adaptations():
    # #139B: aliases are path-only, so repeated adaptation yields the identical
    # ordered alias set (no authored value, no ordering nondeterminism).
    first = adapt_flow_sequence(_flow_cfg())
    second = adapt_flow_sequence(_flow_cfg())
    assert [r.ir_ref for r in first.symbol_requirements] == [r.ir_ref for r in second.symbol_requirements]
    assert [r.ir_ref for r in first.symbol_requirements] == sorted(
        f"$ref:legacy.adapter:{p}" for p in (
            "/source/connection_id", "/source/operation_id",
            "/target/connection_id", "/target/operation_id",
            "/flow_sequence/0/map_ref",
        )
    )


def test_flow_nested_and_profile_refs_are_path_pinned():
    # #139B (architect review): every representative nested occurrence aliases to
    # its EXACT source pointer with the right selector + type — Data Process profile
    # refs, set-property profile refs, a DECISION arm's nested map + cache refs, and
    # a nested branch leg target inside the decision's false arm. Each occurrence's
    # (pointer, legacy_selector, expected_component_type) is pinned explicitly, so a
    # wrong or omitted nested path would fail (not just self-consistency).
    result = adapt_flow_sequence(_flow_cfg(flow_sequence=[
        {"kind": "dataprocess", "steps": [{"operation": "split_documents", "profile_type": "json", "profile_id": "PROF-DP", "link_element_key": "1", "link_element_name": "n"}]},
        {"kind": "set_ddp", "name": "D", "source_values": [{"value_type": "profile", "element_id": "E", "element_name": "N", "profile_id": "PROF-SP", "profile_type": "profile.json"}]},
        {"kind": "decision", "comparison": "equals",
         "left": {"value_type": "track", "property_id": "dynamicdocument.D"},
         "right": {"value_type": "static", "static_value": "A"},
         "true_steps": [
             {"kind": "map_ref", "map_ref": "MAP-DEC"},
             {"kind": "doccacheload", "document_cache_id": "CACHE-DEC"},
             {"kind": "doccacheretrieve", "document_cache_id": "CACHE-DEC"},
         ],
         "false_steps": [{"kind": "branch", "legs": [
             {"steps": [{"kind": "map_ref", "map_ref": "MAP-A"}], "target": {"connector_type": "rest", "connection_id": _REST_CONN, "operation_id": "op-leg-a0000000000000000000000000", "action_type": "POST", "label": "A"}},
             {"steps": [{"kind": "map_ref", "map_ref": "MAP-B"}], "target": {"connector_type": "rest", "connection_id": "55555555-5555-5555-5555-555555555555", "operation_id": "op-leg-b0000000000000000000000000", "action_type": "POST", "label": "B"}},
         ]}]},
    ]))
    by_ptr = {r.source_pointer: r for r in result.symbol_requirements}
    expected = {
        "/flow_sequence/0/steps/0/profile_id": ("PROF-DP", "profile.json"),
        "/flow_sequence/1/source_values/0/profile_id": ("PROF-SP", "profile.json"),
        "/flow_sequence/2/true_steps/0/map_ref": ("MAP-DEC", "transform.map"),
        "/flow_sequence/2/true_steps/1/document_cache_id": ("CACHE-DEC", "documentcache"),
        "/flow_sequence/2/true_steps/2/document_cache_id": ("CACHE-DEC", "documentcache"),
        "/flow_sequence/2/false_steps/0/legs/0/target/operation_id": ("op-leg-a0000000000000000000000000", "connector-action"),
        "/flow_sequence/2/false_steps/0/legs/1/target/operation_id": ("op-leg-b0000000000000000000000000", "connector-action"),
    }
    for ptr, (selector, ctype) in expected.items():
        assert ptr in by_ptr, ptr
        assert by_ptr[ptr].legacy_selector == selector, ptr
        assert by_ptr[ptr].expected_component_type == ctype, ptr
        assert by_ptr[ptr].ir_ref == f"$ref:legacy.adapter:{ptr}", ptr


def test_flow_connector_metadata_only_on_operation_requirements():
    # #139B: connector_type/action_type are None on every non-connector-action
    # requirement (connections, maps, caches, profiles, processes).
    result = adapt_flow_sequence(_flow_cfg(flow_sequence=[
        {"kind": "map_ref", "map_ref": "MAP-1"},
        {"kind": "doccacheload", "document_cache_id": "CACHE-1"},
        {"kind": "doccacheretrieve", "document_cache_id": "CACHE-1"},
    ]))
    for r in result.symbol_requirements:
        if r.expected_component_type != "connector-action":
            assert r.connector_type is None and r.action_type is None, r.source_pointer


def test_flow_live_ref_without_recorded_selector_fails_closed():
    # #139B med (architect review): a live CFG reference with no recorded alias fact
    # (a future codec vocabulary addition producing a ref outside _ID_REF_KEYS) must
    # fail closed with LEGACY_ADAPTER_SEMANTIC_LOSS BEFORE lowering — not a generic
    # compile error.
    from boomi_mcp.compiler.process_ir.legacy_adapters import flow_sequence as fs
    from boomi_mcp.compiler.process_ir.legacy_adapters.contracts import LegacyAdapterError
    from boomi_mcp.models._process_ir_compat import legacy_flow_sequence_to_ir

    projected, _ = fs._project(_flow_cfg())
    facts: dict = {}
    ir = legacy_flow_sequence_to_ir(fs._alias_refs(projected, "", facts))
    facts.pop(next(a for a in facts if a.endswith("/source/operation_id")))  # simulate an unaliased live ref
    with pytest.raises(LegacyAdapterError) as exc:
        fs._requirements_from_ir(ir, facts)
    assert [d.code for d in exc.value.diagnostics] == ["LEGACY_ADAPTER_SEMANTIC_LOSS"]


def test_wrapper_requirements_are_process_typed_and_deduped():
    # The same child called twice yields ONE requirement (SymbolTableV1 rejects
    # duplicate refs; the adapter must not emit a colliding pair). Wrapper calls are
    # NOT role-scoped: legacy_selector == ir_ref == pid.
    result = adapt_wrapper_subprocess(
        {"process_kind": "wrapper_subprocess", "process_calls": [{"process_id": _C1}, {"process_id": _C1}]}
    )
    refs = [r.ir_ref for r in result.symbol_requirements]
    assert refs == [_C1]
    assert result.symbol_requirements[0].legacy_selector == _C1
    assert result.symbol_requirements[0].expected_component_type == "process"


# ---------------------------------------------------------------------------
# Lenient projection: safe unknown fields become no-op paths, never rejections
# ---------------------------------------------------------------------------


def test_wrapper_records_unknown_root_and_call_keys_as_noop():
    result = adapt_wrapper_subprocess(
        _wrapper_cfg(unknown_root="x", process_calls=[{"process_id": _C1, "future_flag": True}])
    )
    assert "/unknown_root" in result.compatibility_noop_paths
    assert "/process_calls/0/future_flag" in result.compatibility_noop_paths


def test_flow_records_unknown_root_and_binding_keys_as_noop():
    src = {
        "connector_type": "database",
        "connection_id": _DB_CONN,
        "operation_id": _DB_OP,
        "action_type": "Get",
        "future_src": "y",
    }
    result = adapt_flow_sequence(_flow_cfg(source=src, future_root="z"))
    assert "/future_root" in result.compatibility_noop_paths
    assert "/source/future_src" in result.compatibility_noop_paths


def test_flow_process_extensions_is_envelope_owned_not_noop():
    # process_extensions is consumed by the component assembler, so it is neither
    # codec input nor a recorded no-op.
    result = adapt_flow_sequence(_flow_cfg(process_extensions={"connections": []}))
    assert "/process_extensions" not in result.compatibility_noop_paths


def test_flow_records_inert_transform_and_reliability_extras_as_noop():
    # Codex #139A review r-arch (finding 6): the lenient inert sibling blocks
    # (transform, reliability, reliability.dlq) are passed to the codec wholesale;
    # their accepted-and-ignored extras must be recorded as compatibility no-op paths.
    result = adapt_flow_sequence(_flow_cfg(
        transform={"mode": "passthrough", "future_t": 1},
        reliability={"retry_count": 0, "dlq": {"mode": "disabled", "future_d": 2}, "future_r": 3},
    ))
    paths = set(result.compatibility_noop_paths)
    assert "/transform/future_t" in paths
    assert "/reliability/future_r" in paths
    assert "/reliability/dlq/future_d" in paths
    # Consumed fields are never recorded as no-ops.
    assert "/transform/mode" not in paths
    assert "/reliability/retry_count" not in paths
