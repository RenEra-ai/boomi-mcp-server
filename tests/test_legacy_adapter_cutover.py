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
_RB_CONN = "55555555-5555-5555-5555-555555555555"
_RB_OP = "66666666-6666-6666-6666-666666666666"

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


# ---------------------------------------------------------------------------
# Backward compatibility: the adapter must not tighten inputs the legacy
# validate+build path accepted-and-coerced (Codex #139A review round 1).
# ---------------------------------------------------------------------------


def _base_flow(**over):
    cfg = dict(_FLOW_CFG)
    cfg.update(over)
    return cfg


def test_wrapper_non_string_label_still_builds():
    # validate_config does not type-check label; the pre-#139 emitter did
    # str(...). A validated non-string label must still build, not raise.
    cfg = {"process_kind": "wrapper_subprocess", "process_calls": [{"process_id": _C1, "label": 7}]}
    assert WrapperSubprocessBuilder.validate_config(cfg, depends_on=[]) is None
    xml = WrapperSubprocessBuilder.build(cfg, name="W")
    assert 'userlabel="7"' in xml


def test_flow_non_string_endpoint_label_still_builds():
    cfg = _base_flow(
        flow_sequence=[{"kind": "map_ref", "map_ref": "MAP-1"}],
        source={"connector_type": "database", "connection_id": _DB_CONN, "operation_id": _DB_OP, "action_type": "Get", "label": 123},
    )
    assert ProcessFlowBuilder.validate_config(cfg, depends_on=[]) is None
    assert "<bns:Component" in ProcessFlowBuilder.build(cfg, name="F")


def test_flow_whitespace_padded_ids_are_stripped_like_legacy():
    # The old builder wrote str(id).strip(); the strict IR ref validator rejects
    # surrounding whitespace, so the adapter must strip to preserve acceptance
    # AND byte output (the stripped id, never the padded spelling).
    padded_op = f"  {_REST_OP}  "
    cfg = _base_flow(
        flow_sequence=[{"kind": "map_ref", "map_ref": "MAP-1"}],
        target={"connector_type": "rest", "connection_id": _REST_CONN, "operation_id": padded_op, "action_type": "POST"},
    )
    assert ProcessFlowBuilder.validate_config(cfg, depends_on=[]) is None
    xml = ProcessFlowBuilder.build(cfg, name="F")
    assert _REST_OP in xml and padded_op not in xml


def test_flow_branch_leg_target_extra_key_still_builds():
    # A branch leg target is validated by the lenient _validate_target_binding, so
    # a safe unknown key on it was accepted-and-ignored before — it must remain so.
    def rt(conn, op, label, **extra):
        d = {"connector_type": "rest", "connection_id": conn, "operation_id": op, "action_type": "POST", "label": label}
        d.update(extra)
        return d
    cfg = _base_flow(flow_sequence=[{"kind": "branch", "legs": [
        {"steps": [{"kind": "map_ref", "map_ref": "MAP-A"}], "target": rt(_REST_CONN, _REST_OP, "A", future_leg_key="x")},
        {"steps": [{"kind": "map_ref", "map_ref": "MAP-B"}], "target": rt(_RB_CONN, _RB_OP, "B")},
    ]}])
    assert ProcessFlowBuilder.validate_config(cfg, depends_on=[]) is None
    assert "<bns:Component" in ProcessFlowBuilder.build(cfg, name="F")


def test_flow_conflicting_connector_metadata_for_shared_id_fails_closed():
    # Codex #139A review r2 (P2): one component id bound with CONFLICTING connector
    # families (a database source op == a REST target op) is semantically invalid;
    # the adapter must fail closed to PROCESS_XML_VALIDATION_FAILED rather than
    # silently emit the source connector with the target's family.
    shared = "cccccccc-cccc-cccc-cccc-cccccccccccc"
    cfg = _base_flow(
        source={"connector_type": "database", "connection_id": _DB_CONN, "operation_id": f"  {shared}  ", "action_type": "Get"},
        target={"connector_type": "rest", "connection_id": _REST_CONN, "operation_id": shared, "action_type": "POST"},
        flow_sequence=[{"kind": "map_ref", "map_ref": "MAP-1"}],
    )
    assert ProcessFlowBuilder.validate_config(cfg, depends_on=[]) is None
    with pytest.raises(BuilderValidationError) as exc:
        ProcessFlowBuilder.build(cfg, name="F")
    assert exc.value.error_code == "PROCESS_XML_VALIDATION_FAILED"


def test_flow_same_endpoint_reused_across_branch_legs_still_builds():
    # Consistent reuse (same id, same connector family) is NOT a conflict — two
    # branch legs to one REST endpoint must keep building.
    def rt(op, label):
        return {"connector_type": "rest", "connection_id": _REST_CONN, "operation_id": op, "action_type": "POST", "label": label}
    cfg = _base_flow(flow_sequence=[{"kind": "branch", "legs": [
        {"steps": [{"kind": "map_ref", "map_ref": "MAP-A"}], "target": rt(_REST_OP, "A")},
        {"steps": [{"kind": "map_ref", "map_ref": "MAP-B"}], "target": rt(_REST_OP, "B")},
    ]}])
    assert ProcessFlowBuilder.validate_config(cfg, depends_on=[]) is None
    assert "<bns:Component" in ProcessFlowBuilder.build(cfg, name="F")


_OP_A = "e1111111-1111-1111-1111-111111111111"
_OP_B = "e2222222-2222-2222-2222-222222222222"


def test_flow_shared_connection_with_different_actions_is_not_a_conflict():
    # Codex #139A review r3 (P1): one REST connection legitimately hosts operations
    # with different actions (GET + POST). action_type belongs to the operation, not
    # the connection, so a shared connection_id must NOT be a conflict.
    cfg = _base_flow(flow_sequence=[{"kind": "branch", "legs": [
        {"steps": [{"kind": "map_ref", "map_ref": "MAP-A"}], "target": {"connector_type": "rest", "connection_id": _REST_CONN, "operation_id": _OP_A, "action_type": "GET", "label": "A"}},
        {"steps": [{"kind": "map_ref", "map_ref": "MAP-B"}], "target": {"connector_type": "rest", "connection_id": _REST_CONN, "operation_id": _OP_B, "action_type": "POST", "label": "B"}},
    ]}])
    assert ProcessFlowBuilder.validate_config(cfg, depends_on=[]) is None
    assert "<bns:Component" in ProcessFlowBuilder.build(cfg, name="F")


def test_flow_equivalent_connector_aliases_are_not_a_conflict():
    # Codex #139A review r3 (P2): rest / rest_client canonicalize to one family, so
    # the same operation referenced via equivalent aliases is not a conflict.
    cfg = _base_flow(flow_sequence=[{"kind": "branch", "legs": [
        {"steps": [{"kind": "map_ref", "map_ref": "MAP-A"}], "target": {"connector_type": "rest", "connection_id": _REST_CONN, "operation_id": _OP_A, "action_type": "POST", "label": "A"}},
        {"steps": [{"kind": "map_ref", "map_ref": "MAP-B"}], "target": {"connector_type": "rest_client", "connection_id": _REST_CONN, "operation_id": _OP_A, "action_type": "POST", "label": "B"}},
    ]}])
    assert ProcessFlowBuilder.validate_config(cfg, depends_on=[]) is None
    assert "<bns:Component" in ProcessFlowBuilder.build(cfg, name="F")


def test_flow_shared_connection_with_conflicting_family_fails_closed():
    # Codex #139A review r4 (P1): a database source and a REST target reusing ONE
    # connection_id references a single connection component as two families — no
    # Boomi connection can be both, so fail closed (connection conflict = FAMILY,
    # not action).
    shared_conn = "cccccccc-cccc-cccc-cccc-cccccccccccc"
    cfg = _base_flow(
        source={"connector_type": "database", "connection_id": shared_conn, "operation_id": _DB_OP, "action_type": "Get"},
        target={"connector_type": "rest", "connection_id": shared_conn, "operation_id": _REST_OP, "action_type": "POST"},
        flow_sequence=[{"kind": "map_ref", "map_ref": "MAP-1"}],
    )
    assert ProcessFlowBuilder.validate_config(cfg, depends_on=[]) is None
    with pytest.raises(BuilderValidationError) as exc:
        ProcessFlowBuilder.build(cfg, name="F")
    assert exc.value.error_code == "PROCESS_XML_VALIDATION_FAILED"


def test_flow_same_operation_id_with_conflicting_action_fails_closed():
    # Codex #139A review r4 (P1): one operation id reused with DIFFERENT actions
    # (GET on one leg, POST on another) would silently emit one action for both
    # (POST/POST) — an operation component has one action, so fail closed.
    def rt(action, label):
        return {"connector_type": "rest", "connection_id": _REST_CONN, "operation_id": _OP_A, "action_type": action, "label": label}
    cfg = _base_flow(flow_sequence=[{"kind": "branch", "legs": [
        {"steps": [{"kind": "map_ref", "map_ref": "MAP-A"}], "target": rt("GET", "A")},
        {"steps": [{"kind": "map_ref", "map_ref": "MAP-B"}], "target": rt("POST", "B")},
    ]}])
    assert ProcessFlowBuilder.validate_config(cfg, depends_on=[]) is None
    with pytest.raises(BuilderValidationError) as exc:
        ProcessFlowBuilder.build(cfg, name="F")
    assert exc.value.error_code == "PROCESS_XML_VALIDATION_FAILED"


def test_flow_dead_root_target_is_excluded_from_conflict_detection():
    # Codex #139A review r5 (P2): a branch/exception (or return_documents) terminal
    # makes the top-level `target` DEAD config (dropped from the IR). A dead root
    # target reusing an EMITTED leg's operation id with another action is NOT a
    # real conflict — it must not be flagged, since the branch emits correctly.
    cfg = _base_flow(
        target={"connector_type": "rest", "connection_id": _REST_CONN, "operation_id": _OP_A, "action_type": "POST"},  # DEAD
        flow_sequence=[{"kind": "branch", "legs": [
            {"steps": [{"kind": "map_ref", "map_ref": "MAP-A"}], "target": {"connector_type": "rest", "connection_id": _REST_CONN, "operation_id": _OP_A, "action_type": "GET", "label": "A"}},
            {"steps": [{"kind": "map_ref", "map_ref": "MAP-B"}], "target": {"connector_type": "rest", "connection_id": _RB_CONN, "operation_id": _RB_OP, "action_type": "POST", "label": "B"}},
        ]}],
    )
    assert ProcessFlowBuilder.validate_config(cfg, depends_on=[]) is None
    xml = ProcessFlowBuilder.build(cfg, name="F")
    # The emitted leg keeps its own GET action; the dead root target contributes nothing.
    assert "<bns:Component" in xml
    assert 'actionType="GET"' in xml


def test_flow_decision_self_terminating_true_arm_makes_root_target_dead():
    # Codex #139A review r6 (P2): a top-level decision whose true_steps ends in a
    # nested branch/exception uses that terminal instead of the root target, so the
    # root target is DEAD and must be excluded from conflict detection.
    decision = {
        "kind": "decision", "comparison": "equals",
        "left": {"value_type": "track", "property_id": "dynamicdocument.D"},
        "right": {"value_type": "static", "static_value": "A"},
        "true_steps": [{"kind": "branch", "legs": [
            {"steps": [{"kind": "map_ref", "map_ref": "MAP-A"}], "target": {"connector_type": "rest", "connection_id": _REST_CONN, "operation_id": _OP_A, "action_type": "GET", "label": "A"}},
            {"steps": [{"kind": "map_ref", "map_ref": "MAP-B"}], "target": {"connector_type": "rest", "connection_id": _RB_CONN, "operation_id": _RB_OP, "action_type": "POST", "label": "B"}},
        ]}],
        "false_steps": [{"kind": "exception", "message_template": "halt {1}"}],
    }
    # dead root target reuses the true-arm leg's op id with a DIFFERENT action.
    cfg = _base_flow(
        target={"connector_type": "rest", "connection_id": _REST_CONN, "operation_id": _OP_A, "action_type": "POST"},
        flow_sequence=[decision],
    )
    assert ProcessFlowBuilder.validate_config(cfg, depends_on=[]) is None
    xml = ProcessFlowBuilder.build(cfg, name="F")
    assert "<bns:Component" in xml and 'actionType="GET"' in xml


def test_flow_decision_linear_true_arm_root_target_conflict_still_caught():
    # The converse: a decision with a LINEAR true arm DOES emit the root target as
    # the true-arm fallthrough, so a genuine conflict with it is still caught.
    decision = {
        "kind": "decision", "comparison": "equals",
        "left": {"value_type": "track", "property_id": "dynamicdocument.D"},
        "right": {"value_type": "static", "static_value": "A"},
        "true_steps": [{"kind": "map_ref", "map_ref": "MAP-T"}],
        "false_steps": [{"kind": "branch", "legs": [
            {"steps": [{"kind": "map_ref", "map_ref": "MAP-A"}], "target": {"connector_type": "rest", "connection_id": _REST_CONN, "operation_id": _OP_A, "action_type": "GET", "label": "A"}},
            {"steps": [{"kind": "map_ref", "map_ref": "MAP-B"}], "target": {"connector_type": "rest", "connection_id": _RB_CONN, "operation_id": _RB_OP, "action_type": "POST", "label": "B"}},
        ]}],
    }
    # EMITTED root target (true-arm fallthrough) reuses the false-arm leg's op with a different action.
    cfg = _base_flow(
        target={"connector_type": "rest", "connection_id": _REST_CONN, "operation_id": _OP_A, "action_type": "POST"},
        flow_sequence=[decision],
    )
    assert ProcessFlowBuilder.validate_config(cfg, depends_on=[]) is None
    with pytest.raises(BuilderValidationError) as exc:
        ProcessFlowBuilder.build(cfg, name="F")
    assert exc.value.error_code == "PROCESS_XML_VALIDATION_FAILED"
