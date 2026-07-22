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


def test_flow_requirements_are_correctly_typed_and_carry_connector_metadata():
    result = adapt_flow_sequence(_flow_cfg())
    by_ref = {r.ir_ref: r for r in result.symbol_requirements}
    # Both connector bindings + the map are present with the emitter's exact types.
    assert by_ref[_DB_CONN].expected_component_type == "connector-settings"
    assert by_ref[_DB_OP].expected_component_type == "connector-action"
    assert by_ref[_REST_CONN].expected_component_type == "connector-settings"
    assert by_ref[_REST_OP].expected_component_type == "connector-action"
    assert by_ref["MAP-1"].expected_component_type == "transform.map"
    # Connector metadata rides on the OPERATION requirement, derived from config.
    assert by_ref[_DB_OP].connector_type == "database"
    assert by_ref[_DB_OP].action_type == "Get"
    assert by_ref[_REST_OP].connector_type == "rest"
    assert by_ref[_REST_OP].action_type == "POST"


def test_wrapper_requirements_are_process_typed_and_deduped():
    # The same child called twice yields ONE requirement (SymbolTableV1 rejects
    # duplicate refs; the adapter must not emit a colliding pair).
    result = adapt_wrapper_subprocess(
        {"process_kind": "wrapper_subprocess", "process_calls": [{"process_id": _C1}, {"process_id": _C1}]}
    )
    refs = [r.ir_ref for r in result.symbol_requirements]
    assert refs == [_C1]
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
