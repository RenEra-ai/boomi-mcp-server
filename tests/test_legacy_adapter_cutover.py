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


def test_resolver_return_value_lands_in_xml_never_the_alias_or_selector():
    # #139B (architect review): the resolver's RETURN VALUE (the resolved component
    # id) is what reaches the XML — proven by a resolver that maps each authored
    # `$ref:KEY` selector to a DISTINCT real id: the resolved ids appear in the XML,
    # while neither the `$ref:KEY` selectors nor the synthetic `$ref:legacy.adapter:`
    # aliases do. Only legacy_selector values are passed to the resolver.
    from boomi_mcp.compiler.process_ir.legacy_adapters.flow_sequence import adapt_flow_sequence

    cfg = {
        "process_kind": "database_to_api_sync",
        "source": {"connector_type": "database", "connection_id": "$ref:srcconn", "operation_id": "$ref:srcop", "action_type": "Get"},
        "transform": {"mode": "passthrough"},
        "target": {"connector_type": "rest", "connection_id": "$ref:tgtconn", "operation_id": "$ref:tgtop", "action_type": "POST"},
        "flow_sequence": [{"kind": "map_ref", "map_ref": "$ref:themap"}],
    }
    result = adapt_flow_sequence(cfg)
    resolved = {
        "$ref:srcconn": "res-11111111-1111-1111-1111-111111111111",
        "$ref:srcop": "res-22222222-2222-2222-2222-222222222222",
        "$ref:tgtconn": "res-33333333-3333-3333-3333-333333333333",
        "$ref:tgtop": "res-44444444-4444-4444-4444-444444444444",
        "$ref:themap": "res-55555555-5555-5555-5555-555555555555",
    }
    seen = []

    def spy(selector):
        seen.append(selector)
        return resolved[selector]

    xml = "".join(emission_mod.emit_legacy_result(result, resolver=spy).shape_xml_parts)
    # Only the authored selectors were passed to the resolver — never an alias.
    assert set(seen) == set(resolved)
    assert not any(s.startswith("$ref:legacy.adapter:") for s in seen)
    # The resolver's RETURN values land in the XML; selectors and aliases do not.
    for real_id in resolved.values():
        assert real_id in xml
    assert "$ref:legacy.adapter:" not in xml
    for selector in resolved:
        assert selector not in xml


def test_canonical_emit_failure_internal_cause_is_output_parity_failed():
    # Codex #139A review r-arch (finding 5): emit_legacy_result wraps a canonical
    # compile/emit failure as the plan-mandated internal LEGACY_ADAPTER_OUTPUT_PARITY_FAILED.
    from boomi_mcp.compiler.process_ir.legacy_adapters.contracts import LegacyAdapterError
    from boomi_mcp.compiler.process_ir.legacy_adapters.flow_sequence import adapt_flow_sequence

    result = adapt_flow_sequence(_FLOW_CFG)
    with patch.object(emission_mod, "emit_process", _raise_compile_error):
        with pytest.raises(LegacyAdapterError) as exc:
            emission_mod.emit_legacy_result(result)
    assert [d.code for d in exc.value.diagnostics] == ["LEGACY_ADAPTER_OUTPUT_PARITY_FAILED"]


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


def test_flow_non_string_decision_operand_fields_still_build():
    # Codex #139A review r-arch (finding 2): validate_config does not type-check the
    # decision track-operand's property_name/default_value; the pre-#139 renderer
    # applied str(x or "") — the adapter must reproduce that so a numeric value still
    # builds instead of raising strict-ProcessIR PROCESS_IR_SCHEMA_INVALID.
    for field, value in (("default_value", 5), ("property_name", 7)):
        cfg = _base_flow(flow_sequence=[{
            "kind": "decision", "comparison": "equals",
            "left": {"value_type": "track", "property_id": "dynamicdocument.D", field: value},
            "right": {"value_type": "static", "static_value": "A"},
            "true_steps": [{"kind": "map_ref", "map_ref": "MAP-1"}],
            "false_steps": [{"kind": "exception", "message_template": "halt {1}"}],
        }])
        assert ProcessFlowBuilder.validate_config(cfg, depends_on=[]) is None, field
        assert "<bns:Component" in ProcessFlowBuilder.build(cfg, name="F"), field


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


def _assert_reuse_byte_faithful(shared_cfg, control_cfg, subs):
    """#139B oracle: a config that REUSES one id must emit exactly what the
    distinct-id control emits with each distinct id replaced by the shared id — so
    reuse is byte-faithful to the pre-#139 builder — and no alias leaks into XML."""
    shared_xml = ProcessFlowBuilder.build(shared_cfg, name="F")
    expected = ProcessFlowBuilder.build(control_cfg, name="F")
    for distinct, shared in subs.items():
        expected = expected.replace(distinct, shared)
    assert shared_xml == expected
    assert "$ref:legacy.adapter:" not in shared_xml
    return shared_xml


def test_flow_conflicting_connector_family_shared_op_builds_byte_faithfully():
    # Codex #139A r2 -> #139B: a database source op == a REST target op reuses ONE
    # id across incompatible families. Role-scoped aliases let each occurrence keep
    # its own family, so it now BUILDS byte-faithfully (was a fail-closed guard).
    shared = "cccccccc-cccc-cccc-cccc-cccccccccccc"
    _D_SRC, _D_TGT = "d5d50000-0000-0000-0000-0000000005c1", "d5d50000-0000-0000-0000-0000000007a6"
    shared_cfg = _base_flow(
        source={"connector_type": "database", "connection_id": _DB_CONN, "operation_id": f"  {shared}  ", "action_type": "Get"},
        target={"connector_type": "rest", "connection_id": _REST_CONN, "operation_id": shared, "action_type": "POST"},
        flow_sequence=[{"kind": "map_ref", "map_ref": "MAP-1"}],
    )
    control_cfg = _base_flow(
        source={"connector_type": "database", "connection_id": _DB_CONN, "operation_id": _D_SRC, "action_type": "Get"},
        target={"connector_type": "rest", "connection_id": _REST_CONN, "operation_id": _D_TGT, "action_type": "POST"},
        flow_sequence=[{"kind": "map_ref", "map_ref": "MAP-1"}],
    )
    xml = _assert_reuse_byte_faithful(shared_cfg, control_cfg, {_D_SRC: shared, _D_TGT: shared})
    # Each connector keeps its own family: database for the source, REST for the target.
    assert 'connectorType="database"' in xml
    assert "officialboomi-X3979C-rest-prod" in xml


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


def test_flow_shared_connection_conflicting_family_builds_byte_faithfully():
    # Codex #139A r4 -> #139B: a database source and a REST target reusing ONE
    # connection_id now builds byte-faithfully (each occurrence keeps its family).
    shared = "cccccccc-cccc-cccc-cccc-cccccccccccc"
    _D_SRC, _D_TGT = "d5d50000-0000-0000-0000-0000000005c2", "d5d50000-0000-0000-0000-0000000007a7"
    shared_cfg = _base_flow(
        source={"connector_type": "database", "connection_id": shared, "operation_id": _DB_OP, "action_type": "Get"},
        target={"connector_type": "rest", "connection_id": shared, "operation_id": _REST_OP, "action_type": "POST"},
        flow_sequence=[{"kind": "map_ref", "map_ref": "MAP-1"}],
    )
    control_cfg = _base_flow(
        source={"connector_type": "database", "connection_id": _D_SRC, "operation_id": _DB_OP, "action_type": "Get"},
        target={"connector_type": "rest", "connection_id": _D_TGT, "operation_id": _REST_OP, "action_type": "POST"},
        flow_sequence=[{"kind": "map_ref", "map_ref": "MAP-1"}],
    )
    _assert_reuse_byte_faithful(shared_cfg, control_cfg, {_D_SRC: shared, _D_TGT: shared})


def test_flow_same_operation_id_different_actions_builds_byte_faithfully():
    # Codex #139A r4 -> #139B: two branch legs reusing one operation id with
    # DIFFERENT actions (GET, POST) each keep their own action via role-scoping.
    shared = _OP_A
    _D_A, _D_B = "d5d50000-0000-0000-0000-00000000000a", "d5d50000-0000-0000-0000-00000000000b"
    def rt(op, action, label):
        return {"connector_type": "rest", "connection_id": _REST_CONN, "operation_id": op, "action_type": action, "label": label}
    shared_cfg = _base_flow(flow_sequence=[{"kind": "branch", "legs": [
        {"steps": [{"kind": "map_ref", "map_ref": "MAP-A"}], "target": rt(shared, "GET", "A")},
        {"steps": [{"kind": "map_ref", "map_ref": "MAP-B"}], "target": rt(shared, "POST", "B")},
    ]}])
    control_cfg = _base_flow(flow_sequence=[{"kind": "branch", "legs": [
        {"steps": [{"kind": "map_ref", "map_ref": "MAP-A"}], "target": rt(_D_A, "GET", "A")},
        {"steps": [{"kind": "map_ref", "map_ref": "MAP-B"}], "target": rt(_D_B, "POST", "B")},
    ]}])
    xml = _assert_reuse_byte_faithful(shared_cfg, control_cfg, {_D_A: shared, _D_B: shared})
    # Both actions survive — leg A stays GET, leg B stays POST (not POST/POST).
    assert 'actionType="GET"' in xml and 'actionType="POST"' in xml


def test_flow_cross_type_id_reuse_builds_byte_faithfully():
    # #139B (architect finding 1): one id used as both a map_ref and a
    # document_cache_id was `PROCESS_IR_COMPILE_SYMBOL_UNRESOLVED` before role
    # scoping; now the two aliases resolve to the same real id with distinct types,
    # so it builds byte-faithfully — the real id appears in both mapId and docCache.
    shared = "cccccccc-cccc-cccc-cccc-cccccccccccc"
    _D_MAP, _D_CACHE = "d5d50000-0000-0000-0000-00000000ma01", "d5d50000-0000-0000-0000-00000000ca01"
    steps = lambda m, c: [
        {"kind": "map_ref", "map_ref": m},
        {"kind": "doccacheload", "document_cache_id": c},
        {"kind": "doccacheretrieve", "document_cache_id": c},
    ]
    shared_cfg = _base_flow(flow_sequence=steps(shared, shared))
    control_cfg = _base_flow(flow_sequence=steps(_D_MAP, _D_CACHE))
    xml = _assert_reuse_byte_faithful(shared_cfg, control_cfg, {_D_MAP: shared, _D_CACHE: shared})
    assert f'mapId="{shared}"' in xml and f'docCache="{shared}"' in xml


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


def test_flow_decision_emitted_root_target_reuse_builds_byte_faithfully():
    # #139A r-guard -> #139B: a decision with a LINEAR true arm emits the root
    # target; when it reuses a false-arm leg's op with a different action, each
    # occurrence keeps its own action via role-scoping, so it now builds faithfully.
    shared = _OP_A
    _D_ROOT, _D_LEG = "d5d50000-0000-0000-0000-0000000000c1", "d5d50000-0000-0000-0000-0000000000c2"
    def decision(leg_op, root_op):
        return {
            "kind": "decision", "comparison": "equals",
            "left": {"value_type": "track", "property_id": "dynamicdocument.D"},
            "right": {"value_type": "static", "static_value": "A"},
            "true_steps": [{"kind": "map_ref", "map_ref": "MAP-T"}],
            "false_steps": [{"kind": "branch", "legs": [
                {"steps": [{"kind": "map_ref", "map_ref": "MAP-A"}], "target": {"connector_type": "rest", "connection_id": _REST_CONN, "operation_id": leg_op, "action_type": "GET", "label": "A"}},
                {"steps": [{"kind": "map_ref", "map_ref": "MAP-B"}], "target": {"connector_type": "rest", "connection_id": _RB_CONN, "operation_id": _RB_OP, "action_type": "POST", "label": "B"}},
            ]}],
        }, {"connector_type": "rest", "connection_id": _REST_CONN, "operation_id": root_op, "action_type": "POST"}
    dec_s, tgt_s = decision(shared, shared)
    dec_c, tgt_c = decision(_D_LEG, _D_ROOT)
    shared_cfg = _base_flow(target=tgt_s, flow_sequence=[dec_s])
    control_cfg = _base_flow(target=tgt_c, flow_sequence=[dec_c])
    _assert_reuse_byte_faithful(shared_cfg, control_cfg, {_D_ROOT: shared, _D_LEG: shared})
