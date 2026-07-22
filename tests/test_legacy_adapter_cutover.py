"""Cutover guarantees for #139 M12.4.

Proves the migrated build paths (``wrapper_subprocess`` and the composed
``database_to_api_sync`` ``flow_sequence``) now emit their shapes through the ONE
canonical chain — ``adapter -> compile_process_ir_v1 -> emit_process`` — and that
the pre-#139 composed-flow XML orchestration is GONE (no duplicate emitter path).
A post-validation compiler/emitter defect is translated to the builder's existing
public error family, never surfaced as a raw ``LEGACY_ADAPTER_*`` / compiler code.
"""

from __future__ import annotations

import sys
from pathlib import Path
from unittest.mock import patch

import pytest

_ROOT = Path(__file__).resolve().parent.parent
for _p in (str(_ROOT), str(_ROOT / "src")):
    if _p not in sys.path:
        sys.path.insert(0, _p)

from boomi_mcp.categories.components.builders import process_flow_builder as pfb
from boomi_mcp.categories.components.builders.process_flow_builder import (
    BuilderValidationError,
    ProcessFlowBuilder,
    WrapperSubprocessBuilder,
)
from boomi_mcp.compiler.process_ir.diagnostics import ProcessIRCompileError
from boomi_mcp.compiler.process_ir.legacy_adapters import emission as emission_mod

_C1 = "11111111-1111-1111-1111-111111111111"
_DB_CONN = "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"
_DB_OP = "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb"
_REST_CONN = "cccccccc-cccc-cccc-cccc-cccccccccccc"
_REST_OP = "dddddddd-dddd-dddd-dddd-dddddddddddd"

_WRAPPER_CFG = {
    "process_kind": "wrapper_subprocess",
    "process_calls": [{"process_id": _C1, "label": "call"}],
}
_FLOW_CFG = {
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


# ---------------------------------------------------------------------------
# The canonical chain is actually driven
# ---------------------------------------------------------------------------


def test_wrapper_build_drives_emit_process():
    with patch.object(
        emission_mod, "emit_process", wraps=emission_mod.emit_process
    ) as spy:
        xml = WrapperSubprocessBuilder.build(_WRAPPER_CFG, name="W")
    assert spy.call_count == 1
    assert "<bns:Component" in xml and "processcall" in xml


def test_flow_sequence_build_drives_emit_process():
    with patch.object(
        emission_mod, "emit_process", wraps=emission_mod.emit_process
    ) as spy:
        xml = ProcessFlowBuilder.build(_FLOW_CFG, name="F")
    assert spy.call_count == 1
    assert "<bns:Component" in xml and "shapetype=\"map\"" in xml


def test_flow_sequence_build_drives_compile_process_ir():
    with patch.object(
        emission_mod, "compile_process_ir_v1", wraps=emission_mod.compile_process_ir_v1
    ) as spy:
        ProcessFlowBuilder.build(_FLOW_CFG, name="F")
    assert spy.call_count == 1


# ---------------------------------------------------------------------------
# No duplicate emitter path — the pre-#139 composed-flow orchestration is gone
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "name",
    [
        "_emit_composed_flow_shapes",
        "_source_prefix_flow_entries",
        "_target_terminal_entries",
        "_append_path",
        "_append_branch",
        "_append_decision",
        "_emit_seq_linear",
        "_append_linear_entries",
        "_seq_step_to_flow_entry",
        "_seq_exception_params",
    ],
)
def test_legacy_composed_flow_helpers_are_deleted(name):
    assert not hasattr(pfb, name), f"{name} should be deleted after the flow_sequence cutover"


# ---------------------------------------------------------------------------
# Failure translation: internal compiler defect -> public builder code
# ---------------------------------------------------------------------------


def _raise_compile_error(*_a, **_k):
    from boomi_mcp.compiler.process_ir.diagnostics import diagnostic
    from boomi_mcp.errors import PROCESS_IR_COMPILE_INTERNAL

    raise ProcessIRCompileError([diagnostic(PROCESS_IR_COMPILE_INTERNAL, "xml_emission", "")])


@pytest.mark.parametrize(
    "builder,cfg",
    [(WrapperSubprocessBuilder, _WRAPPER_CFG), (ProcessFlowBuilder, _FLOW_CFG)],
)
def test_post_validation_compile_failure_translates_to_public_code(builder, cfg):
    # A validated config whose canonical emission unexpectedly fails must surface
    # the builder's existing external code — never a raw compiler/LEGACY_ADAPTER_ one.
    with patch.object(emission_mod, "emit_process", _raise_compile_error):
        with pytest.raises(BuilderValidationError) as exc:
            builder.build(cfg, name="X")
    assert exc.value.error_code == "PROCESS_XML_VALIDATION_FAILED"
    assert "LEGACY_ADAPTER" not in str(exc.value.error_code)
