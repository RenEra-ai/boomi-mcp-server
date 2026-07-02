"""Issue #123 (M11.4, epic #118) — cache/property lineage validation tests.

Covers the architect's positive/negative matrix, organized by the M11.4
cardinality classes (1:1 carry-forward, 1:N broadcast, split, N:1 merge) and
their failure modes, plus the backward-compat exemptions (legacy retrieve
kinds, decision-operand leniency, wildcard script/map writers).
"""

from __future__ import annotations

import sys
from pathlib import Path

_project_root = str(Path(__file__).resolve().parent.parent)
if _project_root not in sys.path:
    sys.path.insert(0, _project_root)

from src.boomi_mcp.categories.components.builders import ProcessFlowBuilder
from src.boomi_mcp.categories.components.builders.cache_property_lineage import (
    collect_lineage_events,
    validate_config_lineage,
)

_REST_TARGET = {
    "connector_type": "rest",
    "connection_id": "33333333-3333-3333-3333-333333333333",
    "operation_id": "44444444-4444-4444-4444-444444444444",
    "action_type": "POST",
}


def _seq_config(flow_sequence, **overrides):
    cfg = {
        "process_kind": "database_to_api_sync",
        "source": {
            "connector_type": "database",
            "connection_id": "11111111-1111-1111-1111-111111111111",
            "operation_id": "22222222-2222-2222-2222-222222222222",
            "action_type": "Get",
        },
        "transform": {"mode": "passthrough"},
        "target": dict(_REST_TARGET),
        "flow_sequence": flow_sequence,
    }
    cfg.update(overrides)
    return cfg


def _set_ddp(name, sources=None, **extra):
    return {
        "kind": "set_ddp",
        "name": name,
        "source_values": sources or [{"value_type": "static", "value": "v"}],
        **extra,
    }


def _set_dpp(name, sources=None, **extra):
    return {
        "kind": "set_dpp",
        "name": name,
        "source_values": sources or [{"value_type": "static", "value": "v"}],
        **extra,
    }


def _read_ddp(via_name, ddp_name, **source_extra):
    return _set_ddp(
        via_name, sources=[{"value_type": "ddp", "property_name": ddp_name, **source_extra}]
    )


def _read_dpp(via_name, dpp_name, **source_extra):
    return _set_dpp(
        via_name, sources=[{"value_type": "dpp", "property_name": dpp_name, **source_extra}]
    )


def _branch(*leg_step_lists):
    return {
        "kind": "branch",
        "legs": [
            {"steps": list(steps), "target": dict(_REST_TARGET)}
            for steps in leg_step_lists
        ],
    }


def _err(flow_sequence):
    return ProcessFlowBuilder.validate_config(_seq_config(flow_sequence))


# --- 1:1 same-branch carry-forward ------------------------------------------


def test_one_to_one_trunk_write_then_read_passes():
    assert _err([_set_ddp("DDP_ID"), _read_ddp("DDP_OUT", "DDP_ID")]) is None
    assert _err([_set_dpp("DPP_ID"), _read_dpp("DPP_OUT", "DPP_ID")]) is None


def test_property_read_before_any_write_fails_strict():
    err = _err([_read_ddp("DDP_OUT", "DDP_NEVER_SET")])
    assert err is not None
    assert err.error_code == "PROCESS_LINEAGE_PROPERTY_READ_BEFORE_WRITE"
    err = _err([_read_dpp("DPP_OUT", "DPP_NEVER_SET")])
    assert err.error_code == "PROCESS_LINEAGE_PROPERTY_READ_BEFORE_WRITE"


def test_explicit_default_declares_absence_ok():
    assert (
        _err([_read_dpp("DPP_OUT", "DPP_NEVER_SET", default_value="")]) is None
    )


# --- 1:N broadcast (DPP set on trunk, read in multiple legs) -----------------


def test_one_to_n_broadcast_dpp_trunk_write_read_in_legs_passes():
    seq = [
        _set_dpp("DPP_RUN"),
        _branch(
            [_read_dpp("DPP_A", "DPP_RUN")],
            [_read_dpp("DPP_B", "DPP_RUN")],
        ),
    ]
    assert _err(seq) is None


def test_ddp_trunk_write_visible_in_every_leg():
    seq = [
        _set_ddp("DDP_KEY"),
        _branch(
            [_read_ddp("DDP_A", "DDP_KEY")],
            [_read_ddp("DDP_B", "DDP_KEY")],
        ),
    ]
    assert _err(seq) is None


# --- split (DDP travels with the per-document copy) ---------------------------


def test_split_ddp_write_before_flow_control_read_after_passes():
    seq = [
        _set_ddp("DDP_KEY"),
        {"kind": "flow_control", "for_each_count": 1},
        _read_ddp("DDP_OUT", "DDP_KEY"),
    ]
    assert _err(seq) is None


# --- DDP branch-leg scope ------------------------------------------------------


def test_ddp_written_in_sibling_leg_fails_scope():
    seq = [
        _branch(
            [_set_ddp("DDP_LEG")],
            [_read_ddp("DDP_OUT", "DDP_LEG")],
        )
    ]
    err = _err(seq)
    assert err is not None
    assert err.error_code == "PROCESS_LINEAGE_DDP_SCOPE_INVALID"


def test_ddp_written_and_read_in_same_leg_passes():
    seq = [
        _branch(
            [_set_ddp("DDP_LEG"), _read_ddp("DDP_OUT", "DDP_LEG")],
            [],
        )
    ]
    assert _err(seq) is None


# --- N:1 merge/join via Document Cache ---------------------------------------


def test_cache_put_in_earlier_leg_get_in_later_leg_passes():
    seq = [
        _branch(
            [{"kind": "cache_put", "document_cache_id": "$ref:cache"}],
            [{"kind": "cache_get", "document_cache_id": "$ref:cache"}],
        )
    ]
    cfg = _seq_config(seq)
    assert ProcessFlowBuilder.validate_config(cfg, depends_on=["cache"]) is None


def test_cache_get_before_put_leg_order_fails():
    seq = [
        _branch(
            [{"kind": "cache_get", "document_cache_id": "$ref:cache"}],
            [{"kind": "cache_put", "document_cache_id": "$ref:cache"}],
        )
    ]
    err = ProcessFlowBuilder.validate_config(_seq_config(seq), depends_on=["cache"])
    assert err is not None
    assert err.error_code == "PROCESS_LINEAGE_BRANCH_ORDER_INVALID"


def test_dpp_cross_leg_sequential_order_enforced():
    # Leg order is deterministic: leg-1 write feeds leg-2 read (pass);
    # reversed legs fail with the branch-order error.
    ok = [_branch([_set_dpp("DPP_X")], [_read_dpp("DPP_OUT", "DPP_X")])]
    assert _err(ok) is None
    bad = [_branch([_read_dpp("DPP_OUT", "DPP_X")], [_set_dpp("DPP_X")])]
    err = _err(bad)
    assert err is not None
    assert err.error_code == "PROCESS_LINEAGE_BRANCH_ORDER_INVALID"


def test_cache_get_without_any_writer_fails():
    err = _err([{"kind": "cache_get", "document_cache_id": "CACHE-1"}])
    assert err is not None
    assert err.error_code == "PROCESS_LINEAGE_CACHE_WRITER_MISSING"


def test_cache_get_external_writer_passes():
    assert (
        _err(
            [
                {
                    "kind": "cache_get",
                    "document_cache_id": "CACHE-1",
                    "external_writer": True,
                }
            ]
        )
        is None
    )


def test_cache_ids_must_match_verbatim():
    seq = [
        {"kind": "cache_put", "document_cache_id": "CACHE-A"},
        {"kind": "cache_get", "document_cache_id": "CACHE-B"},
    ]
    err = _err(seq)
    assert err is not None
    assert err.error_code == "PROCESS_LINEAGE_CACHE_WRITER_MISSING"


# --- exclusive (decision) paths ----------------------------------------------


def test_write_only_on_sibling_exclusive_path_fails_ambiguous():
    seq = [
        {
            "kind": "decision",
            "comparison": "equals",
            "left": {"value_type": "track", "property_id": "dynamicdocument.DDP_S"},
            "right": {"value_type": "static", "static_value": "GO"},
            "true_steps": [_set_dpp("DPP_FLAG")],
            "false_steps": [_read_dpp("DPP_OUT", "DPP_FLAG")],
        }
    ]
    err = _err(seq)
    assert err is not None
    assert err.error_code == "PROCESS_LINEAGE_AMBIGUOUS_LAST_WRITE"


def test_write_on_trunk_read_inside_decision_leg_passes():
    seq = [
        _set_dpp("DPP_FLAG"),
        {
            "kind": "decision",
            "comparison": "equals",
            "left": {"value_type": "track", "property_id": "dynamicdocument.DDP_S"},
            "right": {"value_type": "static", "static_value": "GO"},
            "true_steps": [_read_dpp("DPP_OUT", "DPP_FLAG")],
            "false_steps": [{"kind": "message", "message_text": "no"}],
        }
    ]
    assert _err(seq) is None


# --- backward-compat exemptions ------------------------------------------------


def test_legacy_doccacheretrieve_kind_stays_exempt():
    # The M10 standalone-retrieve subprocess pattern (live-verified, #119
    # census) must keep planning clean with no in-process writer.
    seq = [{"kind": "doccacheretrieve", "document_cache_id": "CACHE-1"}]
    assert _err(seq) is None


def test_decision_operand_read_without_writer_is_lenient():
    # Legacy decision operands emit defaultValue="" on the wire — absence is
    # a defined runtime value, and the #117 goldens predate this contract.
    seq = [
        {
            "kind": "decision",
            "comparison": "equals",
            "left": {"value_type": "track", "property_id": "dynamicdocument.DDP_S"},
            "right": {"value_type": "static", "static_value": "GO"},
            "true_steps": [],
            "false_steps": [{"kind": "message", "message_text": "no"}],
        }
    ]
    assert _err(seq) is None


def test_wildcard_script_writer_satisfies_downstream_reads():
    seq = [
        {
            "kind": "dataprocess",
            "steps": [
                {"operation": "custom_scripting", "script": "dataContext.storeStream(is, props);"}
            ],
        },
        _read_dpp("DPP_OUT", "DPP_SET_BY_SCRIPT"),
    ]
    assert _err(seq) is None


def test_wildcard_writer_never_condemns_a_read():
    # A later map/script must not produce a misplaced-writer diagnosis for a
    # strict read with no named writer — absence stays READ_BEFORE_WRITE.
    seq = [
        _read_dpp("DPP_OUT", "DPP_NEVER"),
        {"kind": "map_ref", "map_ref": "MAP-1"},
    ]
    err = _err(seq)
    assert err is not None
    assert err.error_code == "PROCESS_LINEAGE_PROPERTY_READ_BEFORE_WRITE"


# --- event collection details ---------------------------------------------------


def test_collect_events_orders_and_paths():
    cfg = _seq_config(
        [
            _set_dpp("DPP_A"),
            _branch(
                [_set_ddp("DDP_L1")],
                [{"kind": "cache_put", "document_cache_id": "C1"}],
            ),
        ]
    )
    events = collect_lineage_events(cfg)
    kinds = [(e.event, e.scope, e.name) for e in events]
    assert ("write", "dpp", "DPP_A") in kinds
    assert ("write", "ddp", "DDP_L1") in kinds
    assert ("write", "cache", "C1") in kinds
    ddp_write = next(e for e in events if e.name == "DDP_L1")
    cache_write = next(e for e in events if e.name == "C1")
    assert ddp_write.branch_path and ddp_write.branch_path[0][1] == 0
    assert cache_write.branch_path and cache_write.branch_path[0][1] == 1
    assert ddp_write.seq < cache_write.seq  # legs walk in execution order


def test_dynamic_path_events_collected_for_legacy_configs():
    # collect_lineage_events observes the dynamic-path write/read events even
    # though legacy (non-flow_sequence) configs are not lineage-enforced.
    cfg = _seq_config([])
    del cfg["flow_sequence"]
    cfg["transform"] = {"mode": "map_ref", "map_ref": "MAP-1"}
    cfg["target"] = dict(
        _REST_TARGET,
        dynamic_path={
            "ddp_name": "DDP_PATH",
            "request_profile_id": "P-1",
            "profile_type": "profile.json",
            "segments": [
                {"type": "static", "value": "/v1/"},
                {"type": "dpp", "property_name": "DPP_SEG"},
            ],
        },
    )
    events = collect_lineage_events(cfg)
    kinds = [(e.event, e.scope, e.name) for e in events]
    assert ("write", "wildcard", "*") in kinds
    assert ("read", "dpp", "DPP_SEG") in kinds
    assert ("write", "ddp", "DDP_PATH") in kinds
    # And enforcement passes because the wildcard transform precedes the read.
    assert validate_config_lineage(cfg) is None
