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
    SYNC_PIPELINE_DIALECT,
    WRAPPER_SUBPROCESS_DIALECT,
    adapter_for,
    is_migrated,
    migrated_dialects,
)
from boomi_mcp.compiler.process_ir.legacy_adapters.contracts import (
    LegacyAdapterError,
    LegacyAdapterResultV1,
)
from boomi_mcp.compiler.process_ir.legacy_adapters.flow_sequence import (
    adapt_flow_sequence,
)
from boomi_mcp.categories.components.builders.process_flow_builder import (
    ProcessFlowBuilder,
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
    # #175: a wrapper parent is exactly ONE process call — the call ends its
    # path, so a chain would need each child's return paths bound to it. The
    # withdrawn two-call shape is kept as a refusal witness below.
    cfg = {
        "process_kind": "wrapper_subprocess",
        "process_calls": [{"process_id": _C1}],
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
    # #139C moved sync_pipeline from reserved to migrated.
    assert is_migrated(SYNC_PIPELINE_DIALECT)
    assert migrated_dialects() == {
        WRAPPER_SUBPROCESS_DIALECT,
        FLOW_SEQUENCE_DIALECT,
        SYNC_PIPELINE_DIALECT,
    }


def test_every_migrated_adapter_accepts_its_raw_dialect_config():
    """The registry's actual contract: raw validated dialect config -> IR.

    `adapter_for(d)` is the seam that exists so a caller can adapt a dialect
    WITHOUT knowing which internal function does it. That only works if every
    entry accepts the same thing the dialect's own `validate_config` accepts.

    #139C broke this on its first attempt by registering the production
    `adapt_sync_pipeline`, which takes the LOWERED core rather than the raw config
    — so every call through the registry failed at `/pipeline`. Nothing caught it,
    because the registry has no production caller. Parametrizing over
    `migrated_dialects()` means a future dialect cannot be registered wrong: it
    fails here the moment it is added, with no new test to remember to write.
    """
    raw_by_dialect = {
        WRAPPER_SUBPROCESS_DIALECT: {
            "process_kind": "wrapper_subprocess",
            "process_calls": [{"process_id": "P1"}],
        },
        FLOW_SEQUENCE_DIALECT: {
            "process_kind": "database_to_api_sync",
            "source": {"connector_type": "database", "action_type": "Get",
                       "connection_id": "C", "operation_id": "O"},
            "transform": {"mode": "passthrough"},
            "target": {"connector_type": "rest_client", "action_type": "POST",
                       "connection_id": "TC", "operation_id": "TO"},
            "flow_sequence": [{"kind": "map_ref", "map_ref": "M1"}],
        },
        SYNC_PIPELINE_DIALECT: {
            "process_kind": "sync_pipeline",
            "pipeline": {
                "stages": [
                    {"key": "s", "kind": "read", "config": {
                        "primitive": "db_read", "connection_id": "C", "operation_id": "O"}},
                    {"key": "t", "kind": "send", "config": {
                        "primitive": "rest_send", "action_type": "POST",
                        "connection_id": "TC", "operation_id": "TO"}},
                ],
                "dependencies": [{"from_stage": "s", "to_stage": "t"}],
            },
        },
    }
    # Every migrated dialect must have a sample here — that is what makes this
    # test fail loudly for a newly registered dialect instead of skipping it.
    assert set(raw_by_dialect) == migrated_dialects()

    for dialect, raw in sorted(raw_by_dialect.items()):
        result = adapter_for(dialect)(raw)
        assert isinstance(result, LegacyAdapterResultV1), dialect
        assert result.process_ir is not None, dialect


def test_registry_sync_pipeline_entry_adapts_a_listener_config():
    """A listener config through the registry RETURNS IR since #158.

    Until #158 the listener arm deferred to the legacy renderer, and because an
    adapter cannot express "use the legacy renderer" the registry entry raised.
    The listener entry is canonical now: the raw config lowers to the root-only
    ``listener`` node — operation-only — followed by the target and the stop, and
    its one listener requirement is rooted in the RAW config the registry accepts.
    """
    listener_cfg = {
        "process_kind": "sync_pipeline",
        "pipeline": {
            "stages": [
                {"key": "l", "kind": "listener", "config": {
                    "primitive": "wss_listen", "operation_id": "WSSOP-1"}},
                {"key": "t", "kind": "send", "config": {
                    "primitive": "rest_send", "action_type": "POST",
                    "connection_id": "TC", "operation_id": "TO"}},
            ],
            "dependencies": [{"from_stage": "l", "to_stage": "t"}],
        },
    }
    result = adapter_for(SYNC_PIPELINE_DIALECT)(listener_cfg)
    assert [step.kind for step in result.process_ir.body.steps] == [
        "listener", "target", "stop",
    ]
    listener = [
        r for r in result.symbol_requirements if r.role == "start_listen.operation"
    ]
    assert [(r.source_pointer, r.legacy_selector) for r in listener] == [
        ("/pipeline/stages/0/config/operation_id", "WSSOP-1")
    ]


def test_registry_reserved_dialects_are_not_migrated():
    # Reserved-but-pending dialects resolve to None so the legacy renderer stays
    # authoritative — they must not masquerade as migrated.
    assert RESERVED_DIALECTS, "the reserved set must stay meaningful, not empty"
    assert not (RESERVED_DIALECTS & migrated_dialects())
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
    #
    # #184 amendment 3: the two cache occurrences were a LINEAR doccacheload ->
    # doccacheretrieve pair. Add to Cache hands on no documents (measured), so the
    # read wired after it never runs and the builder now refuses that pair at
    # flow_sequence[2].kind. The staging write is therefore the terminal of a
    # target-less branch leg and the read sits in the LATER leg — the legal form.
    # The id reuse under test is unchanged; only the pointers moved into the legs.
    shared = "cccccccc-cccc-cccc-cccc-cccccccccccc"
    result = adapt_flow_sequence(_flow_cfg(flow_sequence=[
        {"kind": "map_ref", "map_ref": shared},
        {"kind": "branch", "legs": [
            {"steps": [{"kind": "doccacheload", "document_cache_id": shared}]},
            {"steps": [{"kind": "doccacheretrieve", "document_cache_id": shared}],
             "target": {"connector_type": "rest", "connection_id": _REST_CONN, "operation_id": _REST_OP, "action_type": "POST", "label": "read"}},
        ]},
    ]))
    by_ptr = {r.source_pointer: r for r in result.symbol_requirements}
    ptrs = [
        "/flow_sequence/0/map_ref",
        "/flow_sequence/1/legs/0/steps/0/document_cache_id",
        "/flow_sequence/1/legs/1/steps/0/document_cache_id",
    ]
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


def _nested_flow_sequence():
    """The nested-reference flow_sequence, built fresh for each consumer.

    #184 amendment 3: the decision's TRUE arm used to chain doccacheload ->
    doccacheretrieve linearly. Add to Cache hands on no documents (measured), so
    the read after it never ran and the builder now refuses that chain at the
    read's ``.kind``. The arm's cache refs therefore sit in the legal ordered-leg
    form: a target-less staging leg whose terminal is the load, then a later leg
    that reads and routes to its own target. The nested-path pinning this fixture
    exists for is unchanged — the cache refs are simply one container deeper.
    """
    return [
        {"kind": "dataprocess", "steps": [{"operation": "split_documents", "profile_type": "json", "profile_id": "PROF-DP", "link_element_key": "1", "link_element_name": "n"}]},
        {"kind": "set_ddp", "name": "D", "source_values": [{"value_type": "profile", "element_id": "E", "element_name": "N", "profile_id": "PROF-SP", "profile_type": "profile.json"}]},
        {"kind": "decision", "comparison": "equals",
         "left": {"value_type": "track", "property_id": "dynamicdocument.D"},
         "right": {"value_type": "static", "static_value": "A"},
         "true_steps": [
             {"kind": "map_ref", "map_ref": "MAP-DEC"},
             {"kind": "branch", "legs": [
                 {"steps": [{"kind": "doccacheload", "document_cache_id": "CACHE-DEC"}]},
                 {"steps": [{"kind": "doccacheretrieve", "document_cache_id": "CACHE-DEC"}], "target": {"connector_type": "rest", "connection_id": _REST_CONN, "operation_id": "op-leg-t0000000000000000000000000", "action_type": "POST", "label": "T"}},
             ]},
         ],
         "false_steps": [{"kind": "branch", "legs": [
             {"steps": [{"kind": "map_ref", "map_ref": "MAP-A"}], "target": {"connector_type": "rest", "connection_id": _REST_CONN, "operation_id": "op-leg-a0000000000000000000000000", "action_type": "POST", "label": "A"}},
             {"steps": [{"kind": "map_ref", "map_ref": "MAP-B"}], "target": {"connector_type": "rest", "connection_id": "55555555-5555-5555-5555-555555555555", "operation_id": "op-leg-b0000000000000000000000000", "action_type": "POST", "label": "B"}},
         ]}]},
    ]


def test_flow_nested_and_profile_refs_are_path_pinned():
    # #139B (architect review): every representative nested occurrence aliases to
    # its EXACT source pointer with the right selector + type — Data Process profile
    # refs, set-property profile refs, a DECISION arm's nested map + cache refs, and
    # a nested branch leg target inside the decision's false arm. Each occurrence's
    # (pointer, legacy_selector, expected_component_type) is pinned explicitly, so a
    # wrong or omitted nested path would fail (not just self-consistency).
    result = adapt_flow_sequence(_flow_cfg(flow_sequence=_nested_flow_sequence()))
    by_ptr = {r.source_pointer: r for r in result.symbol_requirements}
    expected = {
        "/flow_sequence/0/steps/0/profile_id": ("PROF-DP", "profile.json"),
        "/flow_sequence/1/source_values/0/profile_id": ("PROF-SP", "profile.json"),
        "/flow_sequence/2/true_steps/0/map_ref": ("MAP-DEC", "transform.map"),
        "/flow_sequence/2/true_steps/1/legs/0/steps/0/document_cache_id": ("CACHE-DEC", "documentcache"),
        "/flow_sequence/2/true_steps/1/legs/1/steps/0/document_cache_id": ("CACHE-DEC", "documentcache"),
        "/flow_sequence/2/true_steps/1/legs/1/target/operation_id": ("op-leg-t0000000000000000000000000", "connector-action"),
        "/flow_sequence/2/false_steps/0/legs/0/target/operation_id": ("op-leg-a0000000000000000000000000", "connector-action"),
        "/flow_sequence/2/false_steps/0/legs/1/target/operation_id": ("op-leg-b0000000000000000000000000", "connector-action"),
    }
    for ptr, (selector, ctype) in expected.items():
        assert ptr in by_ptr, ptr
        assert by_ptr[ptr].legacy_selector == selector, ptr
        assert by_ptr[ptr].expected_component_type == ctype, ptr
        assert by_ptr[ptr].ir_ref == f"$ref:legacy.adapter:{ptr}", ptr

    # PLACEMENT (architect re-review): the requirement side above cannot tell WHERE
    # each alias lands in the IR. Build the emitted XML and pin the positional order
    # so a nested-path swap (e.g. leg 0 and leg 1 operations trading positions) fails:
    # each leg's operation id must sit inside its OWN leg segment (after its map,
    # before the next leg's map). The two profile refs and the decision-arm cache
    # are also present, and no alias leaks.
    xml = ProcessFlowBuilder.build(_flow_cfg(flow_sequence=_nested_flow_sequence()), name="N")
    assert "$ref:legacy.adapter:" not in xml
    assert (
        xml.index("MAP-A") < xml.index("op-leg-a0000000000000000000000000")
        < xml.index("MAP-B") < xml.index("op-leg-b0000000000000000000000000")
    )
    assert "MAP-DEC" in xml and "CACHE-DEC" in xml
    # The true arm's staging load precedes its reader's target in emitted order.
    assert (
        xml.index("MAP-DEC") < xml.index('docCache="CACHE-DEC"')
        < xml.index("op-leg-t0000000000000000000000000")
    )


def test_flow_connector_metadata_only_on_operation_requirements():
    # #139B: connector_type/action_type are None on every non-connector-action
    # requirement (connections, maps, caches, profiles, processes).
    #
    # #184 amendment 3: the cache refs were a linear doccacheload ->
    # doccacheretrieve pair, which the builder now refuses (Add to Cache hands on
    # no documents, so the read after it never runs). They sit in the legal
    # ordered-leg form instead: a staging leg ending at the load, a later leg that
    # reads and routes to its own target.
    result = adapt_flow_sequence(_flow_cfg(flow_sequence=[
        {"kind": "map_ref", "map_ref": "MAP-1"},
        {"kind": "branch", "legs": [
            {"steps": [{"kind": "doccacheload", "document_cache_id": "CACHE-1"}]},
            {"steps": [{"kind": "doccacheretrieve", "document_cache_id": "CACHE-1"}],
             "target": {"connector_type": "rest", "connection_id": _REST_CONN, "operation_id": _REST_OP, "action_type": "POST", "label": "read"}},
        ]},
    ]))
    checked_types = set()
    for r in result.symbol_requirements:
        if r.expected_component_type != "connector-action":
            assert r.connector_type is None and r.action_type is None, r.source_pointer
            checked_types.add(r.expected_component_type)
    # Non-vacuity: the loop really did inspect a map, both cache occurrences'
    # type, and a connection — not an empty requirement set.
    assert {"transform.map", "documentcache", "connector-settings"} <= checked_types


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


def test_wrapper_requirements_are_process_typed():
    # Wrapper calls are NOT role-scoped: legacy_selector == ir_ref == pid.
    result = adapt_wrapper_subprocess(
        {"process_kind": "wrapper_subprocess", "process_calls": [{"process_id": _C1}]}
    )
    refs = [r.ir_ref for r in result.symbol_requirements]
    assert refs == [_C1]
    assert result.symbol_requirements[0].legacy_selector == _C1
    assert result.symbol_requirements[0].expected_component_type == "process"


def test_a_multi_call_wrapper_is_refused_not_truncated():
    """#175 F24. This is the ADAPTER's own boundary, reached only by a caller
    that skipped `WrapperSubprocessBuilder.validate_config`.

    The failure mode it guards is silent DATA LOSS: emitting the first call and
    dropping the rest would produce a process that quietly does less than the
    config asked for. (The old dedup case lived here — the same child called
    twice, collapsing to one requirement. That shape is refused now, so the
    dedup path is unreachable from a valid wrapper; it stays covered where it is
    still real, on the symbol table's own duplicate-ref rule.)
    """
    for calls in (
        [{"process_id": _C1}, {"process_id": _C2}],
        [{"process_id": _C1}, {"process_id": _C1}],   # even the same child twice
    ):
        with pytest.raises(LegacyAdapterError) as exc:
            adapt_wrapper_subprocess(
                {"process_kind": "wrapper_subprocess", "process_calls": calls}
            )
        assert [d.code for d in exc.value.diagnostics] == ["LEGACY_ADAPTER_SEMANTIC_LOSS"]
        assert exc.value.diagnostics[0].legacy_source_path == "/process_calls"


def test_a_return_documents_wrapper_is_refused_not_dropped():
    """#175 F24, the other half: discarding an authored Return Documents
    terminal would lose the caller's stated intent just as silently."""
    with pytest.raises(LegacyAdapterError) as exc:
        adapt_wrapper_subprocess(
            _wrapper_cfg(return_documents={"enabled": True, "label": "out"})
        )
    assert [d.code for d in exc.value.diagnostics] == ["LEGACY_ADAPTER_SEMANTIC_LOSS"]
    assert exc.value.diagnostics[0].legacy_source_path == "/return_documents/enabled"


def test_a_singleton_wrapper_emits_no_terminal_after_its_call():
    """#175: the adapter appends neither a Stop nor a Return Documents node —
    the call IS the terminal, which is what removes the orphan from the canvas."""
    result = adapt_wrapper_subprocess(_wrapper_cfg())
    kinds = [step.kind for step in result.process_ir.body.steps]
    assert kinds == ["process_call"]


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


# ===========================================================================
# #159 S3 — derived zero-caller census
# ===========================================================================
#
# #159 ends in a STATE: no production code path and no shipped example reaches a
# legacy process-kind adapter, so #160 removes the adapters, the builder dispatch
# and the ``flow_sequence`` sub-dialect as a pure deletion. This section is the
# gate for that state, built BEFORE the migration so it can be watched shrinking.
#
# * What is an adapter is DERIVED, never typed out here: the dialect registry
#   (``legacy_adapters/registry.py``), the alias/authority resolution
#   (``authority._resolve_process_kind`` / ``_is_authoring_process``), the dialect
#   split the production bridge uses (``legacy_bridge._adapter_for_config``), the
#   builder dispatch table (``PROCESS_FLOW_BUILDERS``), pattern discovery
#   (``PatternRegistry.from_package``) and the composition part model.
# * The scan is the #149 AST scanner (``tests/_m12_12_legacy_inventory.py``) run
#   with an ADAPTER vocabulary — leaf pass only, never the served-artifact
#   collection and never the full fixed-point closure, which is what keeps this
#   cheap. Leaf call sites are sufficient and fail-closed: a retained route counts
#   whether or not anything calls it.
# * ``_PRE_CUTOVER_CENSUS`` pins today's findings; the census must EQUAL it. The
#   integrator deletes entries as callers migrate. At cutover the list is empty
#   and ``ZERO_CALLER_CUTOVER_COMPLETE`` flips to ``True`` (one line).
# * When #160 deletes the registry modules, the census keeps working from
#   ``_AUDITED_ADAPTER_AUTHORITY`` — the same facts, captured from the
#   pre-removal registry and pinned against it in both directions while it lives.
#
# Nothing in this section imports a legacy module at import time.

import ast as _ast
import re
import copy as _copy
import functools as _functools
import inspect as _inspect
import types as _types
import typing as _typing

#: Flip to ``True`` in the same commit that empties ``_PRE_CUTOVER_CENSUS``.
ZERO_CALLER_CUTOVER_COMPLETE = True

#: The audited adapter authority, retained as TEST DATA so the census survives
#: #160's physical deletion of the modules it was derived from.
#:
#: Provenance: captured at ed17ddee6e55bc65869c4e3009a5df864f2d6579 (#159 S1 tip,
#: branch codex/issue-159-s1) by ``_live_adapter_authority()`` below, reading
#: ``legacy_adapters/registry.py`` (``_MIGRATED``, ``RESERVED_DIALECTS``),
#: ``legacy_adapters/authority.py:206`` (``_resolve_process_kind`` — the
#: ``.get`` keys it reads), ``process_flow_builder.py:6140`` (``PROCESS_FLOW_BUILDERS``,
#: its ``__all__`` lookups, and the public methods of each registered class),
#: ``semantic_validation/legacy_bridge.py`` (its public functions), the public
#: functions and classes of every other module in the adapter package, and the
#: module files of all of the above. ``test_audited_adapter_authority_matches_the_live_registry``
#: pins it to the live derivation in BOTH directions; it can only change by a
#: reviewed edit here, never by drift.
_AUDITED_ADAPTER_AUTHORITY: _typing.Dict[str, _typing.Any] = {
    "migrated_dialects": {
        "database_to_api_sync/flow_sequence":
            "boomi_mcp.compiler.process_ir.legacy_adapters.flow_sequence.adapt_flow_sequence",
        "sync_pipeline":
            "boomi_mcp.compiler.process_ir.legacy_adapters.sync_pipeline.adapt_sync_pipeline_config",
        "wrapper_subprocess":
            "boomi_mcp.compiler.process_ir.legacy_adapters.wrapper_subprocess.adapt_wrapper_subprocess",
    },
    "reserved_dialects": ("database_to_api_sync",),
    "adapter_callables": (
        "boomi_mcp.compiler.process_ir.legacy_adapters.flow_sequence.adapt_flow_sequence",
        "boomi_mcp.compiler.process_ir.legacy_adapters.registry.adapter_for",
        "boomi_mcp.compiler.process_ir.legacy_adapters.registry.is_migrated",
        "boomi_mcp.compiler.process_ir.legacy_adapters.registry.migrated_dialects",
        "boomi_mcp.compiler.process_ir.legacy_adapters.sync_pipeline.adapt_sync_pipeline",
        "boomi_mcp.compiler.process_ir.legacy_adapters.sync_pipeline.adapt_sync_pipeline_config",
        "boomi_mcp.compiler.process_ir.legacy_adapters.wrapper_subprocess.adapt_wrapper_subprocess",
    ),
    "definition_callables": (
        "boomi_mcp.compiler.process_ir.legacy_adapters.authority.AuthorityDecision",
        "boomi_mcp.compiler.process_ir.legacy_adapters.authority.evaluate_pipeline_authority",
        "boomi_mcp.compiler.process_ir.legacy_adapters.contracts.LegacyAdapterDiagnosticV1",
        "boomi_mcp.compiler.process_ir.legacy_adapters.contracts.LegacyAdapterError",
        "boomi_mcp.compiler.process_ir.legacy_adapters.contracts.LegacyAdapterResultV1",
        "boomi_mcp.compiler.process_ir.legacy_adapters.contracts.LegacySymbolRequirementV1",
        "boomi_mcp.compiler.process_ir.legacy_adapters.contracts.adapter_diagnostic",
        "boomi_mcp.compiler.process_ir.legacy_adapters.emission.LegacyEmissionV1",
        "boomi_mcp.compiler.process_ir.legacy_adapters.emission.emit_legacy_result",
        "boomi_mcp.compiler.process_ir.legacy_adapters.emission.emit_legacy_result_with_profile",
    ),
    "registry_callables": (
        "boomi_mcp.compiler.process_ir.legacy_adapters.registry.adapter_for",
        "boomi_mcp.compiler.process_ir.legacy_adapters.registry.is_migrated",
        "boomi_mcp.compiler.process_ir.legacy_adapters.registry.migrated_dialects",
    ),
    "builder_kinds": {
        "database_to_api_sync":
            "boomi_mcp.categories.components.builders.process_flow_builder.ProcessFlowBuilder",
        "sync_pipeline":
            "boomi_mcp.categories.components.builders.process_flow_builder.SyncPipelineBuilder",
        "wrapper_subprocess":
            "boomi_mcp.categories.components.builders.process_flow_builder.WrapperSubprocessBuilder",
    },
    "builder_tables": ("PROCESS_FLOW_BUILDERS",),
    "builder_lookups": ("get_process_flow_builder",),
    "builder_methods": (
        "build",
        "lower_config",
        "redact_forbidden_secret_fields_in_place",
        "scan_forbidden_secret_fields",
        "validate_config",
    ),
    "semantic_bridge_callables": ("blocking_codes", "validate_legacy_process_config"),
    "kind_selectors": ("process_kind", "process_type"),
    "definition_modules": (
        "src/boomi_mcp/categories/components/builders/process_flow_builder.py",
        "src/boomi_mcp/compiler/process_ir/legacy_adapters/__init__.py",
        "src/boomi_mcp/compiler/process_ir/legacy_adapters/authority.py",
        "src/boomi_mcp/compiler/process_ir/legacy_adapters/contracts.py",
        "src/boomi_mcp/compiler/process_ir/legacy_adapters/emission.py",
        "src/boomi_mcp/compiler/process_ir/legacy_adapters/flow_sequence.py",
        "src/boomi_mcp/compiler/process_ir/legacy_adapters/registry.py",
        "src/boomi_mcp/compiler/process_ir/legacy_adapters/sync_pipeline.py",
        "src/boomi_mcp/compiler/process_ir/legacy_adapters/wrapper_subprocess.py",
        "src/boomi_mcp/compiler/process_ir/semantic_validation/legacy_bridge.py",
    ),
}

#: Census classes that are FINDINGS — every one must be gone at cutover.
_FINDING_CLASSES = frozenset({
    "adapter_route",            # production code calling / importing / dispatching an adapter
    "adapter_producer",         # production code constructing an adapter process config
    "archetype_dispatch",       # a call into an archetype that authors through an adapter
    "unclassified",             # fail-closed residue: reaches the vocabulary, cannot be classified
    "example_process_config",   # a shipped example authoring a process through an adapter config
    "example_archetype_invocation",
    "example_composition_invocation",
})

#: Classified NON-findings, recorded so the census is total and reviewable.
_NON_FINDING_CLASSES = frozenset({
    "legacy_definition",        # inside the adapter/builder/bridge modules themselves
    "validator_selector_read",  # reads the kind selector (a refusal gate may keep doing so)
    "raw_schema_value",         # module-scope literal: served template / schema data
    "reexport_declaration",     # the name appears in an ``__all__`` declaration
    "retired_implementation",   # module no live entry point or registration imports
    "definition_dependency",    # imports a helper/constant from a definition module (#160 input)
    "unreferenced_producer",    # producer nothing in the scanned production tree refers to (#160 input)
})

_UNCLASSIFIED_DIALECT = "<unclassified-dialect>"


def _repo_rel(path) -> str:
    return Path(path).resolve().relative_to(_ROOT).as_posix()


def _qualname(obj) -> str:
    return "%s.%s" % (obj.__module__, obj.__qualname__)


def _public_functions(module) -> _typing.Set[str]:
    return {
        name for name, obj in vars(module).items()
        if _inspect.isfunction(obj) and obj.__module__ == module.__name__
        and not name.startswith("_")
    }


def _selector_keys(function) -> _typing.Tuple[str, ...]:
    """The literal keys an authority function reads with ``.get(...)``."""
    tree = _ast.parse(_inspect.cleandoc("\n" + _inspect.getsource(function)))
    keys = {
        node.args[0].value
        for node in _ast.walk(tree)
        if isinstance(node, _ast.Call) and isinstance(node.func, _ast.Attribute)
        and node.func.attr == "get" and node.args
        and isinstance(node.args[0], _ast.Constant) and isinstance(node.args[0].value, str)
    }
    return tuple(sorted(keys))


def _live_legacy_modules():
    """The legacy authority modules, or ``None`` once #160 has deleted them."""
    try:
        from boomi_mcp.categories.components.builders import process_flow_builder as pfb
        from boomi_mcp.compiler.process_ir.legacy_adapters import authority as auth
        from boomi_mcp.compiler.process_ir.legacy_adapters import registry as reg
        from boomi_mcp.compiler.process_ir.semantic_validation import legacy_bridge as bridge
    except ImportError:
        return None
    return _types.SimpleNamespace(pfb=pfb, auth=auth, reg=reg, bridge=bridge)


def _live_adapter_authority() -> _typing.Optional[_typing.Dict[str, _typing.Any]]:
    """Derive the adapter authority from the live registry, dispatch and bridge."""
    mods = _live_legacy_modules()
    if mods is None:
        return None
    pfb, auth, reg, bridge = mods.pfb, mods.auth, mods.reg, mods.bridge
    adapter_modules = sorted({fn.__module__ for fn in reg._MIGRATED.values()})
    adapter_callables = set()
    for name in adapter_modules:
        module = sys.modules[name]
        adapter_callables |= {"%s.%s" % (name, fn) for fn in _public_functions(module)}
    registry_callables = {"%s.%s" % (reg.__name__, fn) for fn in _public_functions(reg)}
    builder_classes = tuple(pfb.PROCESS_FLOW_BUILDERS.values())
    package_dir = Path(reg.__file__).resolve().parent
    # Everything else the adapter package defines publicly (the authority
    # evaluator, the result contracts, the emission seam): a caller of any of it
    # outside the package depends on the adapters.
    import importlib

    package = reg.__name__.rsplit(".", 1)[0]
    definition_callables = set()
    for module_path in sorted(package_dir.glob("*.py")):
        module = importlib.import_module(
            package if module_path.stem == "__init__" else "%s.%s" % (package, module_path.stem))
        for name, obj in vars(module).items():
            if not name.startswith("_") and getattr(obj, "__module__", None) == module.__name__ \
                    and (_inspect.isfunction(obj) or _inspect.isclass(obj)):
                definition_callables.add("%s.%s" % (module.__name__, name))
    definition_callables -= adapter_callables | registry_callables
    tables = sorted(
        name for name in pfb.__all__
        if isinstance(getattr(pfb, name, None), dict) and getattr(pfb, name)
        and all(value in builder_classes for value in getattr(pfb, name).values())
    )
    lookups = sorted(n for n in pfb.__all__ if _inspect.isfunction(getattr(pfb, n, None)))
    methods = sorted({
        name for cls in builder_classes for name, value in vars(cls).items()
        if not name.startswith("_")
        and isinstance(value, (classmethod, staticmethod, _types.FunctionType))
    })
    definition_modules = {_repo_rel(p) for p in package_dir.glob("*.py")}
    definition_modules |= {_repo_rel(sys.modules[c.__module__].__file__) for c in builder_classes}
    definition_modules.add(_repo_rel(bridge.__file__))
    return {
        "migrated_dialects": {d: _qualname(fn) for d, fn in sorted(reg._MIGRATED.items())},
        "reserved_dialects": tuple(sorted(reg.RESERVED_DIALECTS)),
        "adapter_callables": tuple(sorted(adapter_callables | registry_callables)),
        "definition_callables": tuple(sorted(definition_callables)),
        "registry_callables": tuple(sorted(registry_callables)),
        "builder_kinds": {k: _qualname(c) for k, c in sorted(pfb.PROCESS_FLOW_BUILDERS.items())},
        "builder_tables": tuple(tables),
        "builder_lookups": tuple(lookups),
        "builder_methods": tuple(methods),
        "semantic_bridge_callables": tuple(sorted(_public_functions(bridge))),
        "kind_selectors": _selector_keys(auth._resolve_process_kind),
        "definition_modules": tuple(sorted(definition_modules)),
    }


class _AdapterAuthority:
    """The adapter facts plus the three resolutions the census needs.

    ``live`` delegates every resolution to the production function that owns it.
    The snapshot arm models the same three rules from ``_AUDITED_ADAPTER_AUTHORITY``
    and is pinned to the live arm by ``test_snapshot_resolutions_match_the_live_authority``.
    """

    def __init__(self, data: _typing.Dict[str, _typing.Any], live=None) -> None:
        self.data = data
        self.live = live
        self.selectors = tuple(data["kind_selectors"])
        self.builder_kinds = dict(data["builder_kinds"])
        self.migrated = dict(data["migrated_dialects"])
        self.reserved = frozenset(data["reserved_dialects"])
        self.dialects = frozenset(self.migrated) | self.reserved
        self.definition_modules = frozenset(data["definition_modules"])
        # A qualified dialect is ``<kind>/<config key>``: the key whose non-empty
        # list moves a config of that kind onto the sub-dialect's adapter.
        self.split = {
            dialect.split("/", 1)[1]: dialect for dialect in self.migrated if "/" in dialect
        }
        self.callable_module = {
            q.rsplit(".", 1)[1]: q.rsplit(".", 1)[0] for q in data["adapter_callables"]
        }
        self.registry_callables = {q.rsplit(".", 1)[1] for q in data["registry_callables"]}
        self.definition_callables = {q.rsplit(".", 1)[1] for q in data["definition_callables"]}
        self.callable_module.update(
            (q.rsplit(".", 1)[1], q.rsplit(".", 1)[0]) for q in data["definition_callables"])
        self.builder_class_kind = {q.rsplit(".", 1)[1]: k for k, q in self.builder_kinds.items()}

    # -- resolutions --------------------------------------------------------
    def resolve_kind(self, config: _typing.Any) -> str:
        if self.live is not None:
            return self.live.auth._resolve_process_kind(config)
        for key in self.selectors:
            value = config.get(key) if isinstance(config, dict) else None
            if isinstance(value, str) and value.strip():
                return value.strip()
        return ""

    def is_authoring_process(self, component: _typing.Dict[str, _typing.Any]) -> bool:
        # A RAW row, read through the raw route's own alias authority (``type`` else
        # ``component_type``; ``config`` else ``spec``) — #159 arch-r1 #7.
        from boomi_mcp.categories.integration_builder import (
            raw_component_config,
            raw_component_type,
        )

        view = _types.SimpleNamespace(
            type=raw_component_type(component), config=raw_component_config(component),
            action=component.get("action"),
        )
        if self.live is not None:
            return self.live.auth._is_authoring_process(view)
        if not isinstance(view.type, str) or view.type.strip().lower() != "process":
            return False
        reference_only = isinstance(view.config, dict) and bool(view.config.get("reference_only"))
        return not (view.action == "create" and reference_only)

    def dialect(self, config: _typing.Any) -> _typing.Optional[str]:
        """The adapter dialect a process config dispatches to, or ``None``.

        ``None`` means the production dispatch would not route it to any adapter.
        An adapter kind that no dialect accounts for is ``_UNCLASSIFIED_DIALECT``
        — still a finding (fail closed).
        """
        kind = self.resolve_kind(config)
        if not kind or kind.lower() not in self.builder_kinds:
            return None
        if self.live is not None:
            adapter, _policy = self.live.bridge._adapter_for_config(kind, config)
            if adapter is not None:
                hits = [d for d, fn in self.live.reg._MIGRATED.items() if fn is adapter]
                return hits[0] if len(hits) == 1 else _UNCLASSIFIED_DIALECT
        else:
            if kind in self.migrated:
                return kind
            for key, dialect in self.split.items():
                value = config.get(key)
                if isinstance(value, list) and value:
                    return dialect
        return kind if kind in self.reserved else _UNCLASSIFIED_DIALECT

    # -- reach annotation ---------------------------------------------------
    def dialects_of_kind(self, kind: str) -> _typing.Tuple[str, ...]:
        return tuple(sorted(d for d in self.dialects if d.split("/", 1)[0] == kind))

    def dialects_of_callable(self, name: str) -> _typing.Tuple[str, ...]:
        if name in self.registry_callables:
            return tuple(sorted(self.migrated))
        if name in self.definition_callables:
            return tuple(sorted(self.dialects))  # package-wide seams serve every dialect
        module = self.callable_module.get(name)
        return tuple(sorted(
            d for d, q in self.migrated.items() if q.rsplit(".", 1)[0] == module))


@_functools.lru_cache(maxsize=None)
def _adapter_authority() -> _AdapterAuthority:
    live = _live_adapter_authority()
    if live is None:
        return _AdapterAuthority(_AUDITED_ADAPTER_AUTHORITY)
    return _AdapterAuthority(live, live=_live_legacy_modules())


def _inventory_module():
    import _m12_12_legacy_inventory as inventory  # tests/ is on sys.path under pytest

    return inventory


def _scanner_vocabulary(authority: _AdapterAuthority) -> _typing.Dict[str, _typing.Tuple[str, ...]]:
    """The #149 scanner's families, re-pointed at the adapter authority.

    The scanner reports a call to a ``legacy_emitters`` name by its bare name, so
    that family carries the adapter callables; the XML-write, raw-invoker and
    legacy ``_emit_*`` families are out of this census's scope and stay empty.
    """
    data = authority.data
    builder_classes = tuple(sorted(authority.builder_class_kind))
    return {
        "registry_names": tuple(sorted(set(data["builder_tables"]) | set(data["builder_lookups"]))),
        "builder_classes": builder_classes,
        "builder_methods": tuple(data["builder_methods"]),
        "legacy_emitters": tuple(sorted(authority.callable_module)),
        "legacy_semantic_validation": tuple(data["semantic_bridge_callables"]),
        "component_xml_write_sinks": (),
        "raw_api_invokers": (),
        "producer_selectors": authority.selectors,
    }


# ---------------------------------------------------------------------------
# Python census
# ---------------------------------------------------------------------------

_ROUTE_ROWS = frozenset({
    "registry_lookup", "renderer_call", "legacy_emitter", "legacy_semantic_validation",
})


def _finding(cls, path, symbol, form, line, reaches=()):
    return {"class": cls, "path": path, "symbol": symbol, "form": form,
            "line": line, "reaches": tuple(reaches)}


def _all_declaration_lines(tree) -> _typing.Set[int]:
    lines: _typing.Set[int] = set()
    for node in tree.body:
        targets = node.targets if isinstance(node, _ast.Assign) else (
            [node.target] if isinstance(node, (_ast.AugAssign, _ast.AnnAssign)) else [])
        if any(isinstance(t, _ast.Name) and t.id == "__all__" for t in targets):
            lines.update(range(node.lineno, (node.end_lineno or node.lineno) + 1))
    return lines


def _route_reaches(row, authority: _AdapterAuthority) -> _typing.Tuple[str, ...]:
    census, form = row["census"], row["form"]
    if census == "legacy_emitter":
        name = form.replace("import ", "").split("(")[0].split(" ")[0]
        return authority.dialects_of_callable(name)
    if census == "renderer_call":
        owner = form.split(".", 1)[0]
        kind = authority.builder_class_kind.get(owner)
        if kind is not None:
            return authority.dialects_of_kind(kind)
        return tuple(sorted(authority.dialects))
    if census == "legacy_semantic_validation":
        return tuple(sorted(authority.migrated))
    return tuple(sorted(authority.dialects))  # registry lookup: any registered kind


def _producer_value(form: str) -> _typing.Optional[str]:
    if "=" not in form:
        return None
    value = form.split("=", 1)[1]
    if len(value) >= 2 and value[0] == value[-1] and value[0] in "'\"":
        return value[1:-1]
    return None


def _classify_rows(rows, path, tree, authority):
    """Every scanner row lands in exactly one census class."""
    out = []
    in_all = _all_declaration_lines(tree)
    vocabulary = set(_scanner_vocabulary(authority)["registry_names"]) \
        | set(authority.builder_class_kind) | set(authority.callable_module)
    adapter_symbols: _typing.Set[str] = set()
    pending_specs = []
    definition = path in authority.definition_modules
    for row in sorted(rows, key=lambda r: (r["census"], r["symbol"], r["form"])):
        census, symbol, form, line = row["census"], row["symbol"], row["form"], row["evidence_line"]
        if census in ("http_client_call", "component_xml_write", "raw_api_invoker"):
            continue  # scanner families outside this census (their vocabulary is empty)
        if definition:
            out.append(_finding("legacy_definition", path, symbol, form, line))
            continue
        if census in _ROUTE_ROWS:
            adapter_symbols.add(symbol)
            out.append(_finding("adapter_route", path, symbol, form, line,
                                _route_reaches(row, authority)))
        elif census == "process_kind_consumer":
            out.append(_finding("validator_selector_read", path, symbol, form, line))
        elif census == "process_kind_producer":
            if symbol == "<module>":
                out.append(_finding("raw_schema_value", path, symbol, form, line))
            elif form.startswith("IntegrationComponentSpec("):
                pending_specs.append(row)
            else:
                value = _producer_value(form)
                if value is not None and value.strip().lower() in authority.builder_kinds:
                    adapter_symbols.add(symbol)
                    out.append(_finding("adapter_producer", path, symbol, form, line,
                                        authority.dialects_of_kind(value.strip().lower())))
                else:
                    out.append(_finding("unclassified", path, symbol,
                                        form + " [process kind is not a statically resolvable adapter kind]",
                                        line))
        elif census == "unclassified_dynamic":
            out.append(_finding("unclassified", path, symbol, form, line))
        elif census == "unclassified_reference":
            if "selector" in form:
                out.append(_finding("validator_selector_read", path, symbol, form, line))
            elif "Component-endpoint" in form:
                continue  # #149's endpoint residue: not this census's vocabulary
            elif "symbolic literal" in form and line in in_all:
                out.append(_finding("reexport_declaration", path, symbol, form, line))
            elif "symbolic literal" in form:
                # A bare string naming an adapter or builder (``required_builders =
                # ["ProcessFlowBuilder"]``) is served metadata. Turning one into a
                # call needs ``getattr``, which the scanner reports on its own as
                # ``unclassified_dynamic``.
                out.append(_finding("raw_schema_value", path, symbol, form, line))
            elif any(form.startswith(name + " ") or form.startswith(repr(name) + " ")
                     for name in vocabulary):
                out.append(_finding("unclassified", path, symbol, form, line))
            else:  # pragma: no cover - the scanner's residue forms are closed
                out.append(_finding("unclassified", path, symbol, form, line))
        else:  # pragma: no cover - an unknown scanner row kind fails closed
            out.append(_finding("unclassified", path, symbol, form, line))
    for row in pending_specs:
        # A process component built in a scope that also builds or routes an
        # adapter config is that config's carrier; on its own it is unexplained.
        if row["symbol"] in adapter_symbols:
            out.append(_finding("adapter_producer", path, row["symbol"], row["form"],
                                row["evidence_line"]))
        else:
            out.append(_finding("unclassified", path, row["symbol"],
                                row["form"] + " [no adapter config in the same scope]",
                                row["evidence_line"]))
    return out


def _scan_leaf(path, tree, vocab, known, inventory):
    scanner = inventory._Scanner(path, vocab, local_defs=inventory._module_level_functions(tree),
                                 known_paths=known)
    scanner.visit(tree)
    residue = inventory._ResidueScanner(path, vocab, scanner._consumed, aliases=scanner._aliases)
    residue.visit(tree)
    return list(scanner.rows.values()) + list(residue.rows.values())


def _pattern_authority():
    """Discovered archetypes and the composition route, or ``None`` if gone."""
    try:
        from boomi_mcp import patterns as patterns_pkg
        from boomi_mcp.patterns import PatternKind, PatternRegistry
        from boomi_mcp.patterns.base import ArchetypePattern
    except ImportError:  # pragma: no cover - the pattern layer outlives #160
        return None
    registry = PatternRegistry.from_package(patterns_pkg)
    archetypes = {}
    for cls in registry.list_patterns(kind=PatternKind.ARCHETYPE):
        archetypes[cls.metadata.name] = (
            cls.__qualname__, _repo_rel(sys.modules[cls.__module__].__file__))
    composition = None
    try:
        from boomi_mcp.patterns.composition import CompositionPart
    except ImportError:
        CompositionPart = None
    if CompositionPart is not None:
        kinds = _typing.get_args(CompositionPart.model_fields["kind"].annotation)
        composition = (frozenset(kinds), _repo_rel(sys.modules[CompositionPart.__module__].__file__))
    return _types.SimpleNamespace(
        archetypes=archetypes,
        dispatch_methods=tuple(sorted(ArchetypePattern.__abstractmethods__)),
        package_dir=_repo_rel(Path(patterns_pkg.__file__).parent),
        composition=composition,
        # The discovery walk named above: a live call to it re-roots every archetype.
        discovery=PatternRegistry.from_package.__name__,
    )


def _bearing_pairs(findings, trees, inventory):
    pairs = set()
    for f in findings:
        if f["class"] in ("adapter_route", "adapter_producer", "unclassified", "legacy_definition") \
                and "." not in f["symbol"] and f["symbol"] != "<module>" \
                and f["path"] in trees \
                and f["symbol"] in inventory._module_level_functions(trees[f["path"]]):
            pairs.add((f["path"], f["symbol"]))
    return pairs


def _pattern_reach(findings, sources, trees_cache, known, patterns, inventory, vocab):
    """``{path: {symbol}}`` of pattern-package scopes that reach an adapter.

    A one-package fixed point of the #149 transitive pass: it is how
    ``http_listener_to_rest`` is seen to author through the adapter config that
    ``http_listener_to_db._build_listener_main_process`` constructs.
    """
    prefix = patterns.package_dir + "/"
    for p, text in sources.items():
        if p.startswith(prefix) and p not in trees_cache:
            trees_cache[p] = _ast.parse(text, filename=p)
    trees = {p: t for p, t in trees_cache.items() if p.startswith(prefix)}
    all_trees = trees_cache
    reexports = inventory.reexport_index(all_trees)
    reach: _typing.Dict[str, _typing.Set[str]] = {}
    for f in findings:
        if f["class"] in _FINDING_CLASSES:
            reach.setdefault(f["path"], set()).add(f["symbol"])
    bearing = _bearing_pairs(findings, all_trees, inventory)
    seen: _typing.Set[_typing.Tuple[str, str]] = set()
    while bearing - seen:
        seen |= bearing
        for path, tree in sorted(trees.items()):
            scanner = inventory._Scanner(
                path, vocab, transitive_targets=frozenset(bearing),
                local_defs=inventory._module_level_functions(tree), known_paths=known,
                reexports=reexports)
            scanner.visit(tree)
            for row in scanner.rows.values():
                if row["census"] != "legacy_transitive_call":
                    continue
                reach.setdefault(path, set()).add(row["symbol"])
                if "." not in row["symbol"] and row["symbol"] != "<module>" \
                        and row["symbol"] in inventory._module_level_functions(tree):
                    bearing = bearing | {(path, row["symbol"])}
    return reach


def _candidate_archetypes(patterns, reach) -> _typing.Dict[str, str]:
    """Discovered archetypes that author a process through an adapter."""
    out = {}
    for name, (qualname, path) in sorted(patterns.archetypes.items()):
        symbols = reach.get(path, set())
        if any(s == qualname or s.startswith(qualname + ".") for s in symbols):
            out[name] = qualname
    return out


class _DispatchVisitor(_ast.NodeVisitor):
    """Calls of the archetype dispatch method (``emit_spec``), with their scope."""

    def __init__(self, methods) -> None:
        self.methods = set(methods)
        self.stack: _typing.List[str] = []
        self.calls: _typing.List[_typing.Tuple[str, _typing.Optional[str], str, int]] = []

    def _scoped(self, node) -> None:
        self.stack.append(node.name)
        self.generic_visit(node)
        self.stack.pop()

    visit_FunctionDef = visit_AsyncFunctionDef = visit_ClassDef = _scoped

    def visit_Call(self, node) -> None:  # noqa: N802
        func = node.func
        if isinstance(func, _ast.Attribute) and func.attr in self.methods:
            receiver = func.value
            last = receiver.attr if isinstance(receiver, _ast.Attribute) else (
                receiver.id if isinstance(receiver, _ast.Name) else None)
            self.calls.append((".".join(self.stack) or "<module>", last, func.attr, node.lineno))
        self.generic_visit(node)


def _dispatch_findings(sources, patterns, candidates):
    """Archetype dispatch: static calls into a candidate, and registry dispatch.

    The receiver of a registry-resolved dispatch (``cls.emit_spec(...)`` after
    ``PatternRegistry.get``) is not statically known, so it is resolved from the
    REAL registration table: it reaches every discovered archetype that authors
    through an adapter.
    """
    findings, sites = [], []
    by_class = {qual.split(".")[-1]: name for name, (qual, _p) in patterns.archetypes.items()}
    archetype_paths = {p for _q, p in patterns.archetypes.values()}
    for path in sorted(sources):
        text = sources[path]
        if not path.startswith("src/") or not any(
                method in text for method in patterns.dispatch_methods):
            continue
        visitor = _DispatchVisitor(patterns.dispatch_methods)
        visitor.visit(_ast.parse(text, filename=path))
        for symbol, receiver, method, line in visitor.calls:
            if receiver in by_class:
                name = by_class[receiver]
                if name in candidates:
                    findings.append(_finding(
                        "archetype_dispatch", path, symbol, "%s.%s(...)" % (receiver, method),
                        line, ("archetype:%s" % name,)))
            elif receiver in ("cls", "self") and path in archetype_paths:
                continue  # an archetype's own body calling itself
            else:
                sites.append((path, symbol, line))
                if candidates:
                    findings.append(_finding(
                        "archetype_dispatch", path, symbol,
                        "<registry-resolved>.%s(...)" % method, line,
                        tuple("archetype:%s" % n for n in sorted(candidates))))
    return findings, sites


def _mark_unreferenced_producers(records, sources, trees) -> None:
    """Relabel a producer function nothing in the scanned tree refers to.

    #159 S3 (cutover decision 4): a producer that no module in the scanned
    production tree names — by a ``Name``, an ``Attribute``, an import alias, or
    a string constant spelling it (``getattr`` by name) — cannot construct an
    adapter config on any production path, so it is an audited isolated
    definition (``unreferenced_producer``, a non-finding #160 deletes), not a
    caller to migrate. Until S3 it stayed a finding labelled as a dead-definition
    candidate. Any reference in the scanned tree flips it back to a finding
    (``test_census_counts_an_unreferenced_producer_only_while_nothing_names_it``).
    A ROUTE is never relabelled: a retained route counts uncalled.
    """
    names = {r["symbol"] for r in records
             if r["class"] == "adapter_producer" and "." not in r["symbol"]}
    referenced: _typing.Set[str] = set()
    for path, text in sources.items():
        wanted = {n for n in names - referenced if n in text}
        if not wanted:
            continue
        tree = trees.get(path) or _ast.parse(text, filename=path)
        for node in _ast.walk(tree):
            if isinstance(node, _ast.Name) and node.id in wanted:
                referenced.add(node.id)
            elif isinstance(node, _ast.Attribute) and node.attr in wanted:
                referenced.add(node.attr)
            elif isinstance(node, _ast.alias) and node.name in wanted:
                referenced.add(node.name)
            elif isinstance(node, _ast.Constant) and isinstance(node.value, str) \
                    and node.value in wanted:
                referenced.add(node.value)
    for record in records:
        if record["class"] == "adapter_producer" and record["symbol"] in names - referenced:
            record["class"] = "unreferenced_producer"
            record["form"] += " [no reference in the scanned tree: isolated definition]"


_IMPORT_STATEMENT = re.compile(
    r"^[ \t]*(from[ \t]+[.\w]+[ \t]+import[ \t]+(?:\([^)]*\)|[^\n]+)|import[ \t]+[^\n]+)",
    re.MULTILINE)


@_functools.lru_cache(maxsize=None)
def _import_statements_of(text):
    """``(statement, line)`` for every import statement in ``text``, at any depth.

    Python's own parser is the authority on what an import IS (#159 arch-r1 #7):
    every ``Import``/``ImportFrom`` node of the module's AST, so a compound
    statement (``if True: import x``, ``try: import x``) is seen exactly like a
    standalone one. Unioned with the line scan this census used before, which
    also reads import-shaped lines inside strings and docstrings — kept, because
    every approximation here may only make MORE modules live (fail closed).
    """
    found = []
    try:
        tree = _ast.parse(text)
    except SyntaxError:
        tree = None
    if tree is not None:
        found.extend((node, node.lineno) for node in _ast.walk(tree)
                     if isinstance(node, (_ast.Import, _ast.ImportFrom)))
    for match in _IMPORT_STATEMENT.finditer(text):
        try:
            statement = _ast.parse(match.group(1).strip()).body[0]
        except (SyntaxError, IndexError):
            continue
        found.append((statement, text.count("\n", 0, match.start(1)) + 1))
    return tuple(found)


def _imports(path, text, known, inventory):
    """``(target module, imported name or None, line)`` for every import in ``path``.

    At any depth (lazy imports and compound statements included), read from
    :func:`_import_statements_of`; a statement both arms see is reported once.
    """
    out = []
    for statement, line in _import_statements_of(text):
        if isinstance(statement, _ast.Import):
            for alias in statement.names:
                parts = alias.name.split(".")
                for depth in range(1, len(parts) + 1):
                    target = inventory._resolve_module_path(
                        ".".join(parts[:depth]), 0, path, known)
                    if target:
                        out.append((target, None, line))
        elif isinstance(statement, _ast.ImportFrom):
            level = statement.level or 0
            base = inventory._resolve_module_path(statement.module, level, path, known)
            for alias in statement.names:
                dotted = "%s.%s" % (statement.module, alias.name) if statement.module \
                    else alias.name
                target = inventory._resolve_module_path(dotted, level, path, known)
                if target:
                    out.append((target, None, line))
                elif base:
                    out.append((base, alias.name, line))
            if base:
                out.append((base, None, line))
    return list(dict.fromkeys(out))


def _import_edges(path, text, known, inventory) -> _typing.Set[str]:
    return {target for target, _name, _line in _imports(path, text, known, inventory)}


def _definition_dependencies(sources, known, authority, inventory):
    """Non-definition modules importing a name from a definition module.

    Not a finding by itself — a shared constant or connector-type helper does not
    reach an adapter (an adapter name imported this way also yields its own
    ``adapter_route`` row) — but it is exactly what #160's deletion must sever,
    so the census records it.
    """
    out = []
    for path in sorted(sources):
        if not path.startswith("src/") or path in authority.definition_modules:
            continue
        for target, name, line in _imports(path, sources[path], known, inventory):
            if target in authority.definition_modules and name is not None:
                out.append(_finding("definition_dependency", path, "<import>",
                                    "from %s import %s" % (target, name), line))
    return out


def _live_modules(sources, known, roots, inventory) -> _typing.Set[str]:
    """Modules reachable by static import from the production roots.

    Roots are the repo-root entry modules (``server.py`` registers the MCP
    tools) and — only while a live module calls the discovery walk or dispatches
    through a registry-resolved receiver (``_pattern_route_is_live``) — the
    modules defining every discovered pattern (the pattern registry imports and
    dispatches them dynamically). A module outside this
    closure is a dead implementation behind a retired registration: the plan
    keeps such files until #160 deletes them.
    """
    live: _typing.Set[str] = set()
    frontier = [r for r in sorted(roots) if r in sources]
    while frontier:
        path = frontier.pop()
        if path in live:
            continue
        live.add(path)
        # Importing ``a/b/c.py`` executes the ``__init__`` of every package above it.
        parts = path.split("/")[:-1]
        for depth in range(1, len(parts) + 1):
            package = "/".join(parts[:depth]) + "/__init__.py"
            if package in sources and package not in live:
                frontier.append(package)
        frontier.extend(sorted(_import_edges(path, sources[path], known, inventory) - live))
    return live


def _pattern_route_is_live(sources, trees, live, patterns, sites) -> bool:
    """Does a live module reach the pattern registry's dynamic discovery or dispatch?

    The discovery entry point is the one ``_pattern_authority`` itself calls
    (``patterns.discovery``); a registry-resolved dispatch site is one
    ``_dispatch_findings`` reported. Either one in a live module makes every
    discovered archetype reachable without a static import.
    """
    if any(path in live for path, _symbol, _line in sites):
        return True
    method = getattr(patterns, "discovery", None)
    if not method:
        return False
    for path in sorted(live):
        text = sources.get(path, "")
        if method not in text:
            continue
        tree = trees.get(path) or _ast.parse(text, filename=path)
        for node in _ast.walk(tree):
            if isinstance(node, _ast.Call):
                func = node.func
                name = func.attr if isinstance(func, _ast.Attribute) else (
                    func.id if isinstance(func, _ast.Name) else None)
                if name == method:
                    return True
    return False


def _retire_dead_records(records, live, authority) -> None:
    for record in records:
        if record["class"] in _FINDING_CLASSES and record["path"] not in live \
                and record["path"] not in authority.definition_modules:
            record["class"] = "retired_implementation"


def _python_census(sources, authority, known_paths=None, patterns=None):
    """``(records, context)`` for ``{repo-relative path: source}``.

    Rows are produced for ``src/`` modules (the production tree); every other
    module in ``sources`` is an entry point whose imports decide liveness.
    """
    inventory = _inventory_module()
    vocab = _scanner_vocabulary(authority)
    # Builder METHOD names are deliberately not tokens: the scanner only counts
    # ``build`` / ``validate_config`` on a receiver bound to a builder class or
    # to the dispatch table, and those names are tokens themselves.
    tokens = set().union(*(v for k, v in vocab.items() if k != "builder_methods")) \
        | {"IntegrationComponentSpec"}
    known = frozenset(known_paths if known_paths is not None else sources)
    records, trees = [], {}
    for path in sorted(sources):
        if not path.startswith("src/"):
            continue
        text = sources[path]
        # Sound pre-filter: every scanner row keys on a vocabulary token spelled
        # literally in the source (an alias is bound by an import that names it).
        if not any(token in text for token in tokens):
            continue
        tree = trees[path] = _ast.parse(text, filename=path)
        records.extend(_classify_rows(_scan_leaf(path, tree, vocab, known, inventory),
                                      path, tree, authority))
    _mark_unreferenced_producers(records, sources, trees)
    candidates, sites, reach = {}, [], {}
    if patterns is not None:
        reach = _pattern_reach(records, sources, trees, known, patterns, inventory, vocab)
        candidates = _candidate_archetypes(patterns, reach)
        dispatch, sites = _dispatch_findings(sources, patterns, candidates)
        records.extend(dispatch)
    records.extend(_definition_dependencies(sources, known, authority, inventory))
    roots = {p for p in sources if "/" not in p}
    live = _live_modules(sources, known, roots, inventory)
    if patterns is not None:
        # #159 S3: a discovered archetype is an entry point only while a LIVE module
        # reaches it dynamically — calls the discovery walk, or dispatches through a
        # registry-resolved receiver. Static imports already make it live on their own.
        # With no live discovery or dispatch, the modules the package walk would find
        # are implementations behind a retired registration (#160 deletes them).
        if _pattern_route_is_live(sources, trees, live, patterns, sites):
            roots |= {path for _qualname, path in patterns.archetypes.values()}
            live = _live_modules(sources, known, roots, inventory)
    _retire_dead_records(records, live, authority)
    reach = {p: symbols for p, symbols in reach.items() if p in live}
    return records, _types.SimpleNamespace(candidates=candidates, dispatch_sites=sites,
                                           reach=reach, live=live)


# ---------------------------------------------------------------------------
# Shipped-example census
# ---------------------------------------------------------------------------


def _pointer(base: str, token) -> str:
    return "%s/%s" % (base, str(token).replace("~", "~0").replace("/", "~1"))


def _example_census(path, document, authority, patterns, context):
    """``(records, envelopes)`` for one JSON document, visited structurally.

    Three process-bearing envelope shapes, recognised by structure wherever they
    nest: a component list (an ``IntegrationSpecV1`` body), an archetype tool
    call (``{"tool", "arguments"}`` naming a tool whose action dispatches
    archetypes) and a composition request (``parts`` of composition part kinds).
    """
    # #159 arch-r1 #7: a component row is read through the raw route's alias authority,
    # so ``component_type``/``spec`` are the same row as ``type``/``config``.
    from boomi_mcp.categories.integration_builder import raw_component_config, raw_component_type

    records, envelopes = [], []
    archetype_names = set(patterns.archetypes) if patterns is not None else set()
    dispatch_tools = {
        symbol.split(".")[0][: -len("_action")]
        for _p, symbol, _l in context.dispatch_sites
        if symbol.split(".")[0].endswith("_action")
    }
    composition = patterns.composition if patterns is not None else None
    composition_reaches = composition is not None and bool(context.reach.get(composition[1]))

    counter = [0]

    def visit(node, ptr, in_spec=False):
        counter[0] += 1
        if isinstance(node, list):
            for index, item in enumerate(node):
                visit(item, _pointer(ptr, index), in_spec)
            return
        if not isinstance(node, dict):
            return
        components = node.get("components")
        is_spec = isinstance(components, list) and bool(components) and all(
            isinstance(c, dict) and raw_component_type(c) is not None for c in components)
        if is_spec:
            for index, component in enumerate(components):
                cptr = _pointer(_pointer(ptr, "components"), index)
                if not authority.is_authoring_process(component):
                    envelopes.append(("component", cptr, "not an authored process"))
                    continue
                config = raw_component_config(component)
                # The key the authority read it from (by identity), for the pointer.
                config_key = next((k for k, v in component.items() if v is config), "config")
                dialect = authority.dialect(config) if isinstance(config, dict) else None
                if dialect is None:
                    selector = isinstance(config, dict) and any(
                        k in config for k in authority.selectors)
                    if selector:
                        records.append(_finding("unclassified", path, _pointer(cptr, config_key),
                                                "process kind is not an adapter kind", 0))
                    envelopes.append(("component", cptr, "process with no adapter kind"))
                    continue
                envelopes.append(("component", cptr, "adapter process config"))
                records.append(_finding("example_process_config", path,
                                        _pointer(cptr, config_key), dialect, 0, (dialect,)))
        tool, arguments = node.get("tool"), node.get("arguments")
        if isinstance(tool, str) and isinstance(arguments, dict):
            name = arguments.get("name")
            aptr = _pointer(_pointer(ptr, "arguments"), "name")
            if tool in dispatch_tools:
                if name in context.candidates:
                    envelopes.append(("tool_call", ptr, "archetype authoring through an adapter"))
                    records.append(_finding("example_archetype_invocation", path, aptr,
                                            "%s:%s" % (tool, name), 0, ("archetype:%s" % name,)))
                elif name in archetype_names:
                    envelopes.append(("tool_call", ptr, "canonical archetype"))
                else:
                    envelopes.append(("tool_call", ptr, "unknown archetype"))
                    records.append(_finding("unclassified", path, aptr,
                                            "%s:%s [archetype not discovered]" % (tool, name), 0))
            elif isinstance(name, str) and name in archetype_names:
                envelopes.append(("tool_call", ptr, "archetype name through an unknown tool"))
                records.append(_finding("unclassified", path, aptr,
                                        "%s:%s [names an archetype; tool is not a dispatch route]"
                                        % (tool, name), 0))
        parts = node.get("parts")
        if isinstance(parts, list) and parts and all(
                isinstance(p, dict) and isinstance(p.get("kind"), str) for p in parts):
            kinds = {p["kind"] for p in parts}
            if composition is not None and kinds <= composition[0] and in_spec:
                # A spec body echoing the composition that produced it (its
                # ``validation_rules``) describes a past request; it invokes nothing.
                envelopes.append(("composition", ptr, "composition echo in spec metadata"))
            elif composition is not None and kinds <= composition[0]:
                envelopes.append(("composition", ptr, "composition request"))
                if composition_reaches:
                    records.append(_finding("example_composition_invocation", path,
                                            _pointer(ptr, "parts"), "compose_archetypes", 0,
                                            ("composition:%s" % composition[1],)))
            elif composition is None:
                records.append(_finding("unclassified", path, _pointer(ptr, "parts"),
                                        "parts-shaped request with no composition authority", 0))
        for key, value in node.items():
            visit(value, _pointer(ptr, key), in_spec or is_spec)

    visit(document, "")
    return records, envelopes, counter[0]


# ---------------------------------------------------------------------------
# The census over the real tree
# ---------------------------------------------------------------------------


@_functools.lru_cache(maxsize=None)
def _production_census():
    inventory = _inventory_module()
    authority = _adapter_authority()
    patterns = _pattern_authority()
    sources = {p: s for p, s in inventory.python_sources().items()
               if p.startswith("src/") or "/" not in p}
    records, context = _python_census(sources, authority, patterns=patterns)
    documents = inventory.example_documents()
    envelopes, visited = {}, {}
    for path, document in documents.items():
        found, envelopes[path], visited[path] = _example_census(
            path, document, authority, patterns, context)
        records.extend(found)
    return _types.SimpleNamespace(records=records, context=context, documents=documents,
                                  envelopes=envelopes, visited=visited, sources=sources,
                                  patterns=patterns)


def _census_key(record):
    return (record["class"], record["path"], record["symbol"], record["form"])


def _finding_keys(records):
    return {_census_key(r) for r in records if r["class"] in _FINDING_CLASSES}


# ---------------------------------------------------------------------------
# The pinned pre-cutover census — the migration worklist
# ---------------------------------------------------------------------------
# Measured at ed17dde. Key: (class, path, symbol-or-JSON-pointer, form). Line
# numbers are deliberately NOT part of the key (other lanes edit these files);
# a failure message prints them. DELETE an entry when its caller is migrated;
# never add one without a recorded decision. At cutover this is empty and
# ``ZERO_CALLER_CUTOVER_COMPLETE`` is ``True``.
# #159 S3: EMPTY — every pinned caller migrated or was retired with its registration;
# the 47 entries measured at ed17dde are in this file's history (git log -p).
_PRE_CUTOVER_CENSUS: _typing.FrozenSet[_typing.Tuple[str, str, str, str]] = frozenset()


def _describe(records, keys) -> str:
    lines = []
    for record in sorted(records, key=lambda r: (_census_key(r), r["line"])):
        if _census_key(record) in keys:
            where = record["path"] + (":%d" % record["line"] if record["line"] else "")
            lines.append("  %s  %s  %s  %s  reaches=%s" % (
                record["class"], where, record["symbol"], record["form"],
                ",".join(record["reaches"]) or "-"))
    return "\n".join(lines)


def test_audited_adapter_authority_matches_the_live_registry():
    """The retained authority equals the live derivation, in both directions."""
    live = _live_adapter_authority()
    if live is None:
        # #160 deleted the modules: the snapshot is now the only authority. Make
        # sure that is what happened rather than an unrelated import failure.
        with pytest.raises(ImportError):
            from boomi_mcp.compiler.process_ir.legacy_adapters import registry  # noqa: F401
        return
    assert live == _AUDITED_ADAPTER_AUTHORITY, (
        "the adapter authority moved; re-audit _AUDITED_ADAPTER_AUTHORITY against "
        "the live registry (and record why):\n  live=%r" % (live,))
    # Non-vacuity and internal agreement: every dialect is hosted by a builder
    # kind, and every builder kind hosts at least one dialect.
    dialects = set(live["migrated_dialects"]) | set(live["reserved_dialects"])
    assert dialects and live["builder_kinds"] and live["kind_selectors"]
    assert {d.split("/", 1)[0] for d in dialects} == set(live["builder_kinds"])
    assert set(live["definition_modules"]) <= set(_production_census().sources)


def _authority_probes(authority):
    """Configs varying every axis the dialect resolution reads."""
    probes = [None, {}, {"unrelated": 1}]
    for kind in sorted(authority.builder_kinds):
        for selector in authority.selectors:
            for spelling in (kind, " %s " % kind, kind.upper()):
                base = {selector: spelling}
                probes.append(base)
                for key in sorted(authority.split):
                    for value in ([{"kind": "map_ref"}], [], {"kind": "map_ref"}):
                        probes.append(dict(base, **{key: value}))
    probes.append({authority.selectors[0]: "not_an_adapter_kind"})
    return probes


def test_snapshot_resolutions_match_the_live_authority():
    """The snapshot arm's three rules are the live functions' rules.

    Pinned on probes built from the authority's own axes, so the census gives
    the same answer the day #160 removes the functions it delegates to.
    """
    live = _adapter_authority()
    if live.live is None:
        return  # post-#160: nothing left to compare against; the snapshot rules stand
    snapshot = _AdapterAuthority(_AUDITED_ADAPTER_AUTHORITY)
    probes = _authority_probes(live)
    assert [live.dialect(p) if isinstance(p, dict) else None for p in probes] == \
        [snapshot.dialect(p) if isinstance(p, dict) else None for p in probes]
    assert [live.resolve_kind(p) for p in probes if isinstance(p, dict)] == \
        [snapshot.resolve_kind(p) for p in probes if isinstance(p, dict)]
    # Non-vacuity: the probes reach every dialect, the no-adapter answer and the
    # fail-closed answer.
    outcomes = {live.dialect(p) for p in probes if isinstance(p, dict)}
    assert live.dialects | {None, _UNCLASSIFIED_DIALECT} <= outcomes
    components = [
        {"type": t, "action": a, "config": c}
        for t in ("process", "Process", "connector-action", None)
        for a in ("create", "update", None)
        for c in ({"reference_only": True}, {"reference_only": False}, {}, None)
    ]
    assert [live.is_authoring_process(c) for c in components] == \
        [snapshot.is_authoring_process(c) for c in components]
    assert {live.is_authoring_process(c) for c in components} == {True, False}


def test_zero_caller_census_matches_the_pinned_pre_cutover_worklist():
    """The derived census EQUALS the pinned worklist — both directions."""
    census = _production_census()
    known = _FINDING_CLASSES | _NON_FINDING_CLASSES
    assert {r["class"] for r in census.records} <= known
    measured = _finding_keys(census.records)
    added = measured - _PRE_CUTOVER_CENSUS
    removed = _PRE_CUTOVER_CENSUS - measured
    assert not added, (
        "NEW process-kind adapter caller(s). Author the process as a ProcessIR "
        "unit instead; do not add the entry here without a recorded decision:\n"
        + _describe(census.records, added))
    assert not removed, (
        "pinned caller(s) no longer present — delete these entries from "
        "_PRE_CUTOVER_CENSUS (that is the migration's bookkeeping):\n  "
        + "\n  ".join(map(repr, sorted(removed))))


def test_zero_caller_cutover_state_is_declared():
    """At cutover the worklist is empty AND the flag says so — never only one."""
    if ZERO_CALLER_CUTOVER_COMPLETE:
        assert not _PRE_CUTOVER_CENSUS
        assert not _finding_keys(_production_census().records)
    else:
        assert _PRE_CUTOVER_CENSUS, (
            "the worklist is empty: set ZERO_CALLER_CUTOVER_COMPLETE = True")


_DISPATCH_CONTROL_PACKAGE = "src/boomi_mcp/l8_control_patterns"

#: An in-memory pattern package: one archetype whose ``emit_spec`` reaches an
#: adapter config through a module-level helper, one that does not, a static
#: dispatch into the first, and an action resolving archetypes from a registry.
_DISPATCH_CONTROL_SOURCES = {
    _DISPATCH_CONTROL_PACKAGE + "/__init__.py": "",
    _DISPATCH_CONTROL_PACKAGE + "/legacy_one.py": '''
def _main_process():
    return {"process_kind": "sync_pipeline", "pipeline": {}}


class LegacyOneArchetype:
    @classmethod
    def emit_spec(cls, parameters):
        return _main_process()
''',
    _DISPATCH_CONTROL_PACKAGE + "/canonical_one.py": '''
class CanonicalOneArchetype:
    @classmethod
    def emit_spec(cls, parameters):
        return {"processes": []}
''',
    _DISPATCH_CONTROL_PACKAGE + "/compose.py": '''
from .legacy_one import LegacyOneArchetype


def compose(parameters):
    return LegacyOneArchetype.emit_spec(parameters)
''',
    "src/boomi_mcp/l8_control_actions.py": '''
from .l8_control_patterns.compose import compose


def build_control_action(registry, name, parameters):
    cls = registry.get(name)
    return cls.emit_spec(parameters), compose(parameters)
''',
    "l8_control_entry.py": "from boomi_mcp import l8_control_actions\n",
}


def _dispatch_control_patterns():
    package = _DISPATCH_CONTROL_PACKAGE
    return _types.SimpleNamespace(
        archetypes={
            "legacy_one": ("LegacyOneArchetype", package + "/legacy_one.py"),
            "canonical_one": ("CanonicalOneArchetype", package + "/canonical_one.py"),
        },
        dispatch_methods=("emit_spec",),
        package_dir=package,
        composition=None,
    )


def test_census_resolves_archetype_dispatch_from_the_registration_table():
    """Candidate archetypes come from discovery intersected with adapter reach,
    and both dispatch spellings resolve against that set — in memory, so the
    control does not depend on which archetypes are registered at the time."""
    records, context = _python_census(
        _DISPATCH_CONTROL_SOURCES, _AdapterAuthority(_AUDITED_ADAPTER_AUTHORITY),
        patterns=_dispatch_control_patterns())
    assert context.candidates == {"legacy_one": "LegacyOneArchetype"}
    dispatch = {(r["path"].rsplit("/", 1)[1], r["symbol"], r["form"], r["reaches"])
                for r in records if r["class"] == "archetype_dispatch"}
    assert dispatch == {
        ("compose.py", "compose", "LegacyOneArchetype.emit_spec(...)", ("archetype:legacy_one",)),
        ("l8_control_actions.py", "build_control_action",
         "<registry-resolved>.emit_spec(...)", ("archetype:legacy_one",)),
    }
    assert [s[1] for s in context.dispatch_sites] == ["build_control_action"]
    # The real tree: whatever is registered, candidates are discovered archetypes.
    census = _production_census()
    assert set(census.context.candidates) <= set(census.patterns.archetypes)
    assert census.documents and set(census.envelopes) == set(census.documents)


_INJECTED_CONTROL_PATH = "src/boomi_mcp/_l8_injected_census_control.py"

#: A reachable internal caller, written the ways the census claims to see:
#: an aliased registry lookup, builder dispatch through an aliased lookup, a
#: dynamic builder attribute, and a constructed adapter config. In memory, so it
#: keeps working after #160 deletes the modules it names.
_INJECTED_CONTROL_SOURCE = '''
from boomi_mcp.compiler.process_ir.legacy_adapters import registry as _r
from boomi_mcp.categories.components.builders import get_process_flow_builder as _pick
from boomi_mcp.categories.components.builders.process_flow_builder import SyncPipelineBuilder


def injected_route(config):
    return _r.adapter_for("sync_pipeline")(config)


def injected_dispatch(config):
    return _pick(config["process_kind"]).build(config, name="x")


def injected_dynamic(config, verb):
    return getattr(SyncPipelineBuilder, verb)(config)


def injected_constructor():
    return {"process_kind": "database_to_api_sync", "flow_sequence": [{"kind": "map_ref"}]}


def injected_caller():
    return injected_dispatch(injected_constructor())
'''


@pytest.mark.parametrize("arm", ["live", "snapshot"])
def test_census_detects_an_injected_reachable_adapter_call(arm):
    authority = _adapter_authority() if arm == "live" else \
        _AdapterAuthority(_AUDITED_ADAPTER_AUTHORITY)
    entry = {"l8_control_entry.py": "import boomi_mcp._l8_injected_census_control\n"}
    sources = dict(entry, **{_INJECTED_CONTROL_PATH: _INJECTED_CONTROL_SOURCE})
    records, _ = _python_census(sources, authority)
    found = {(r["class"], r["symbol"], r["form"], r["line"]) for r in records
             if r["class"] in _FINDING_CLASSES}
    assert found == {
        ("adapter_route", "injected_route", "adapter_for(...)", 8),
        ("adapter_route", "injected_dispatch", "get_process_flow_builder(...)", 12),
        ("adapter_route", "injected_dispatch", "<registry-bound>.build(...)", 12),
        ("unclassified", "injected_dynamic", "getattr(<builder>, <dynamic>)", 16),
        ("unclassified", "injected_dynamic", "SyncPipelineBuilder (unclassified reference)", 16),
        ("adapter_producer", "injected_constructor",
         "dict-literal process_kind='database_to_api_sync'", 20),
    }
    assert all(r["path"] == _INJECTED_CONTROL_PATH for r in records)
    # #159 arch-r1 #7: an import inside a compound statement is the same edge.
    for compound in ("if True: import boomi_mcp._l8_injected_census_control\n",
                     "try:\n    import boomi_mcp._l8_injected_census_control\n"
                     "except ImportError:\n    pass\n"):
        nested = {"l8_control_entry.py": compound, _INJECTED_CONTROL_PATH: _INJECTED_CONTROL_SOURCE}
        assert {(r["class"], r["symbol"], r["form"], r["line"]) for r in _python_census(
            nested, authority)[0] if r["class"] in _FINDING_CLASSES} == found, compound
    route = next(r for r in records if r["symbol"] == "injected_route")
    assert "sync_pipeline" in route["reaches"]
    # The selector read in the dispatch is a validator, not a caller.
    assert ("validator_selector_read", "injected_dispatch") in {
        (r["class"], r["symbol"]) for r in records}
    # Retirement is derived, not declared: the same module with nothing live
    # importing it is a dead implementation, and every one of its findings is.
    dead, _ = _python_census({_INJECTED_CONTROL_PATH: _INJECTED_CONTROL_SOURCE}, authority)
    assert {(r["symbol"], r["form"], r["line"]) for r in dead
            if r["class"] == "retired_implementation"} == {f[1:] for f in found}
    assert not [r for r in dead if r["class"] in _FINDING_CLASSES]


#: An in-memory module whose producer nothing names, and the two ways a module
#: can name it (a call, and a ``getattr`` by string).
_ORPHAN_PRODUCER_PATH = "src/boomi_mcp/_l8_orphan_producer.py"
_ORPHAN_PRODUCER_SOURCE = '''
def orphan_producer():
    return {"process_kind": "sync_pipeline", "pipeline": {}}
'''


@pytest.mark.parametrize("reference", [
    None,
    "from boomi_mcp._l8_orphan_producer import orphan_producer\n\n\ndef caller():\n"
    "    return orphan_producer()\n",
    "import boomi_mcp._l8_orphan_producer as m\n\n\ndef caller():\n"
    "    return getattr(m, \"orphan_producer\")()\n",
], ids=["unreferenced", "called", "getattr-by-name"])
def test_census_counts_an_unreferenced_producer_only_while_nothing_names_it(reference):
    """#159 S3: an adapter-config producer no production module names is an
    isolated definition (non-finding); any reference flips it back to a finding."""
    authority = _AdapterAuthority(_AUDITED_ADAPTER_AUTHORITY)
    entry = "import boomi_mcp._l8_orphan_producer\n"
    if reference is not None:
        entry += "import boomi_mcp._l8_orphan_caller\n"
    sources = {"l8_control_entry.py": entry, _ORPHAN_PRODUCER_PATH: _ORPHAN_PRODUCER_SOURCE}
    if reference is not None:
        sources["src/boomi_mcp/_l8_orphan_caller.py"] = reference
    records, context = _python_census(sources, authority)
    assert _ORPHAN_PRODUCER_PATH in context.live
    rows = {(r["class"], r["symbol"]) for r in records if r["path"] == _ORPHAN_PRODUCER_PATH}
    if reference is None:
        assert rows == {("unreferenced_producer", "orphan_producer")}
        assert not _finding_keys(records)
    else:
        assert rows == {("adapter_producer", "orphan_producer")}
    # The real tree: the two audited ``ir_to_legacy_flow_sequence`` producer sites
    # (cutover decision 4) are seen, and seen as isolated definitions.
    real = {(r["path"], r["symbol"]) for r in _production_census().records
            if r["class"] == "unreferenced_producer"}
    assert ("src/boomi_mcp/models/_process_ir_compat.py", "ir_to_legacy_flow_sequence") in real


#: Every discovered archetype module that reached an adapter while the pattern
#: registry was a live entry point (measured on the cutover tree), and the bridge
#: the API->API archetype imports.
_ARCHETYPE_FINDING_PATHS = frozenset({
    "src/boomi_mcp/patterns/archetypes/api_to_api_sync.py",
    "src/boomi_mcp/patterns/archetypes/api_to_database_sync.py",
    "src/boomi_mcp/patterns/archetypes/database_to_api_sync.py",
    "src/boomi_mcp/patterns/archetypes/http_listener_to_db.py",
    "src/boomi_mcp/patterns/archetypes/http_listener_to_rest.py",
    "src/boomi_mcp/patterns/recipe_bridge.py",
})


def test_census_roots_discovered_archetypes_only_while_a_live_module_reaches_them():
    """#159 S3: the package walk discovers archetypes from disk, so discovery alone
    is not reachability. A live call to the discovery walk (or a registry-resolved
    dispatch, ``test_census_resolves_archetype_dispatch_from_the_registration_table``)
    re-roots every discovered archetype, and its adapter rows come back."""
    census = _production_census()
    assert census.patterns.discovery == "from_package"
    assert not _finding_keys(census.records)
    archetype_paths = {path for _q, path in census.patterns.archetypes.values()}
    assert archetype_paths and not (archetype_paths & census.context.live)
    injected = dict(census.sources)
    injected["l8_discovery_entry.py"] = (
        "from boomi_mcp.patterns import PatternRegistry\n"
        "REGISTRY = PatternRegistry.from_package('boomi_mcp.patterns')\n")
    records, context = _python_census(injected, _adapter_authority(), patterns=census.patterns)
    assert archetype_paths <= context.live
    assert {path for _c, path, _s, _f in _finding_keys(records)} == _ARCHETYPE_FINDING_PATHS
    # In memory: the same rule with no real archetype involved.
    package = _DISPATCH_CONTROL_PACKAGE
    sources = {
        package + "/__init__.py": "",
        package + "/legacy_one.py": _DISPATCH_CONTROL_SOURCES[package + "/legacy_one.py"],
        "l8_control_entry.py": "import boomi_mcp.l8_control_patterns\n",
    }
    patterns = _dispatch_control_patterns()
    patterns.discovery = "from_package"
    dead, _ = _python_census(sources, _AdapterAuthority(_AUDITED_ADAPTER_AUTHORITY), patterns=patterns)
    assert not _finding_keys(dead)
    sources["l8_control_entry.py"] += "Registry.from_package('boomi_mcp.l8_control_patterns')\n"
    live, _ = _python_census(sources, _AdapterAuthority(_AUDITED_ADAPTER_AUTHORITY), patterns=patterns)
    assert {(c, p.rsplit("/", 1)[1]) for c, p, _s, _f in _finding_keys(live)} == {
        ("adapter_producer", "legacy_one.py")}


def _flow_sequence_example():
    """A shipped-example envelope authoring ``database_to_api_sync`` with a
    ``flow_sequence`` — the module's own frozen ``_flow_cfg`` adapter input."""
    return {"integration_spec": {"components": [
        {"key": "main_process", "type": "process", "action": "create",
         "config": _flow_cfg()},
    ]}}


@pytest.mark.parametrize("arm", ["live", "snapshot"])
def test_census_detects_a_database_to_api_sync_flow_sequence_example(arm):
    census = _production_census()
    authority = _adapter_authority() if arm == "live" else \
        _AdapterAuthority(_AUDITED_ADAPTER_AUTHORITY)
    path = "examples/l8_control/flow_sequence.integration.json"
    records, envelopes, _visited = _example_census(
        path, _flow_sequence_example(), authority, census.patterns, census.context)
    assert [(r["class"], r["path"], r["symbol"], r["form"]) for r in records] == [
        ("example_process_config", path, "/integration_spec/components/0/config",
         "database_to_api_sync/flow_sequence"),
    ]
    # The alias spelling reaches the same adapter; a reference-only process does not author.
    aliased = _flow_sequence_example()
    config = aliased["integration_spec"]["components"][0]["config"]
    config["process_type"] = config.pop("process_kind")
    assert [r["form"] for r in _example_census(
        path, aliased, authority, census.patterns, census.context)[0]] == [
        "database_to_api_sync/flow_sequence"]
    # #159 arch-r1 #7: the raw route's row aliases (``component_type``, ``spec``) are the
    # same process row, found at the alias's own pointer.
    row_aliased = _flow_sequence_example()
    row = row_aliased["integration_spec"]["components"][0]
    row["component_type"], row["spec"] = row.pop("type"), row.pop("config")
    assert [(r["class"], r["symbol"], r["form"]) for r in _example_census(
        path, row_aliased, authority, census.patterns, census.context)[0]] == [
        ("example_process_config", "/integration_spec/components/0/spec",
         "database_to_api_sync/flow_sequence")]
    referenced = _flow_sequence_example()
    referenced["integration_spec"]["components"][0]["config"]["reference_only"] = True
    assert _example_census(path, referenced, authority, census.patterns, census.context)[0] == []


_NARRATIVE_EXAMPLE = "examples/authoring/authoring_workflow_sequence_example.json"


def test_the_workflow_example_is_the_served_authoring_workflow():
    """#159 ARCH-r2 #4 (structural, second instance of ARCH-r1 #12): the example's ``workflow``
    is not a hand copy of the served workflow — it IS the served
    ``build_integration_from_description`` object. A served change or an example edit fails here
    (bidirectional pin), so its calls (``config={'authoring_request': ...}``) cannot drift again."""
    import json

    from boomi_mcp.categories.meta_tools import get_schema_template_action

    document = json.loads(Path(_NARRATIVE_EXAMPLE).read_text(encoding="utf-8"))
    assert document["workflow_name"] == "build_integration_from_description"
    # ARCH-r3 #2: pinned to the PUBLIC route the example names as its exact source — not to the
    # internal builder, and not to list_capabilities(), which drops steps whose tool is absent.
    selector = "workflow:build_integration_from_description"
    assert f"get_schema_template(schema_name='{selector}')" in document["purpose"]
    served = get_schema_template_action(schema_name=selector)
    assert served["_success"] is True, served
    assert document["workflow"] == served["workflow"]


def test_narrative_workflow_example_is_excluded_for_the_right_reason():
    census = _production_census()
    # 1. Discovered — the exclusion is not a skipped file.
    assert _NARRATIVE_EXAMPLE in census.documents
    # 2. Visited, and it holds no process-bearing envelope.
    assert census.envelopes[_NARRATIVE_EXAMPLE] == []
    assert not [r for r in census.records if r["path"] == _NARRATIVE_EXAMPLE]
    # 3. The visitor walked the WHOLE document, every container and scalar.
    def _nodes(value):
        if isinstance(value, dict):
            return 1 + sum(_nodes(v) for v in value.values())
        if isinstance(value, list):
            return 1 + sum(_nodes(v) for v in value)
        return 1
    document = census.documents[_NARRATIVE_EXAMPLE]
    assert census.visited[_NARRATIVE_EXAMPLE] == _nodes(document) > 1
    # 4. The same document with a real envelope injected IS a finding, at the
    #    injected pointer — the visitor walks this document's structure.
    injected = _copy.deepcopy(document)
    injected["workflow"]["fallback"]["integration_spec"] = \
        _flow_sequence_example()["integration_spec"]
    records, envelopes, _visited = _example_census(
        _NARRATIVE_EXAMPLE, injected, _adapter_authority(), census.patterns, census.context)
    assert [(r["class"], r["symbol"]) for r in records] == [
        ("example_process_config", "/workflow/fallback/integration_spec/components/0/config")]
    assert envelopes
