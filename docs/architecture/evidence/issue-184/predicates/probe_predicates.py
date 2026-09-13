#!/usr/bin/env python3
"""Measure every #184 predicate-table form at a given tree: parse, then compile.

usage: probe_predicates.py <tree-root>
Prints one JSON line per form: parser outcome and compiler outcome (code + pointer).
"""
import json
import sys
from pathlib import Path

root = Path(sys.argv[1]).resolve()
sys.path.insert(0, str(root / "src"))

from boomi_mcp.compiler.process_ir import connector_capabilities as CC  # noqa: E402
from boomi_mcp.compiler.process_ir.contracts import ComponentSymbolV1, SymbolTableV1  # noqa: E402
from boomi_mcp.compiler.process_ir.pipeline import compile_process_ir_v1  # noqa: E402
from boomi_mcp.compiler.process_ir.diagnostics import ProcessIRCompileError  # noqa: E402
from boomi_mcp.compiler.process_ir.emitter_registry import emit_process  # noqa: E402
from boomi_mcp.compiler.process_ir.semantic_validation.contracts import (  # noqa: E402
    ExternalWriterContractV1,
    ProcessIRValidationCapabilitiesV1,
)
from boomi_mcp.models.process_ir import ProcessIRValidationError, parse_process_ir_v1  # noqa: E402

assert Path(sys.modules["boomi_mcp"].__file__).resolve().is_relative_to(root), "wrong tree"

REST = CC.REST_FAMILY


def S(ref, cid, ctype, **kw):
    return ComponentSymbolV1(ref="$ref:" + ref, component_id=cid, component_type=ctype, **kw)


SYMBOLS = SymbolTableV1(symbols=(
    S("RCONN", "RCONN", "connector-settings", connector_type=REST),
    S("GET", "GETOP", "connector-action", connector_type=REST, action_type="GET",
      connection_ref="$ref:RCONN", output_profile_ref="$ref:P_GET"),
    S("PATCH", "PATCHOP", "connector-action", connector_type=REST, action_type="PATCH",
      connection_ref="$ref:RCONN", input_profile_ref="$ref:P_PATCH"),
    S("MAP_A", "MAPA", "transform.map", input_profile_ref="$ref:P_GET", output_profile_ref="$ref:P_MID"),
    S("MAP_B", "MAPB", "transform.map", input_profile_ref="$ref:P_MID", output_profile_ref="$ref:P_PATCH"),
    S("MAP_GP", "MAPGP", "transform.map", input_profile_ref="$ref:P_GET", output_profile_ref="$ref:P_PATCH"),
    S("MAP_WRONG", "MAPW", "transform.map", input_profile_ref="$ref:P_X", output_profile_ref="$ref:P_PATCH"),
    S("P_GET", "PGET", "profile.json"),
    S("P_MID", "PMID", "profile.json"),
    S("P_PATCH", "PPATCH", "profile.json"),
    S("P_X", "PX", "profile.json"),
    S("CACHE", "CACHE1", "documentcache"),
    S("CHILD", "CHILD1", "process"),
))

CAPS_EXT = ProcessIRValidationCapabilitiesV1(
    external_writers=(ExternalWriterContractV1(cache_ref="$ref:CACHE"),)
)

GET = {"kind": "connector_call", "operation_ref": "$ref:GET"}
PATCH = {"kind": "connector_call", "operation_ref": "$ref:PATCH"}
STOP = {"kind": "stop"}
PUT = {"kind": "cache_put", "cache_ref": "$ref:CACHE"}
CGET = {"kind": "cache_get", "cache_ref": "$ref:CACHE"}
CGET_EXT = {"kind": "cache_get", "cache_ref": "$ref:CACHE", "external_writer": True}
PC = {"kind": "process_call", "process_ref": "$ref:CHILD"}
MSG = {"kind": "message", "text": "x"}


def M(ref):
    return {"kind": "map_ref", "map_ref": "$ref:" + ref}


def DDP(name, *sources):
    return {"kind": "set_ddp", "name": name,
            "source_values": list(sources) or [{"value_type": "static", "value": "v"}]}


def DPP(name, *sources):
    return {"kind": "set_dpp", "name": name,
            "source_values": list(sources) or [{"value_type": "static", "value": "v"}]}


def PROF(ref):
    return {"value_type": "profile", "element_id": "3", "element_name": "id (Root/id)",
            "profile_ref": "$ref:" + ref, "profile_type": "profile.json"}


def doc(*steps):
    return {"version": "1", "body": {"kind": "sequence", "steps": list(steps)}}


def leg(steps, terminal):
    return {"steps": list(steps), "terminal": terminal}


def branch(*legs):
    return {"kind": "branch", "legs": list(legs)}


def decision(true_arm, false_arm):
    return {"kind": "decision", "comparison": "equals",
            "left": {"value_type": "static", "static_value": "a"},
            "right": {"value_type": "static", "static_value": "a"},
            "true_arm": true_arm, "false_arm": false_arm}


def bound(call, prop, profile=None):
    out = dict(call)
    out["path_binding"] = {"property_name": prop}
    if profile:
        out["path_binding"]["request_profile_ref"] = "$ref:" + profile
    return out


PATH_SRC = {"value_type": "static", "value": "/v1/items/"}

FORMS = {
    # --- root map bracketing / end-on-call / call-free root
    "root_call_map_map_call": (doc(GET, M("MAP_A"), M("MAP_B"), PATCH, STOP), None),
    "root_call_map_stop": (doc(GET, M("MAP_A"), STOP), None),
    "root_cacheget_map_call": (doc(CGET_EXT, M("MAP_GP"), PATCH, STOP), CAPS_EXT),
    "root_call_setddp_stop": (doc(GET, DDP("X"), STOP), None),
    "root_call_put_get_stop": (doc(GET, PUT, CGET, STOP), None),
    "root_linear_cacheget_map_stop": (doc(CGET_EXT, M("MAP_GP"), STOP), CAPS_EXT),
    "root_linear_message_stop": (doc(MSG, STOP), None),
    # --- same-body prefix before terminal call
    "leg_prefix_cget_map_ddp_pc": (doc(branch(
        leg([GET], PUT), leg([CGET, M("MAP_GP"), DDP("X")], PC))), None),
    "leg_prefix_ddp_pc": (doc(branch(leg([GET], PUT), leg([DDP("X")], PC))), None),
    "true_arm_prefix_cget_pc": (doc(branch(leg([GET], PUT), leg([], decision(
        leg([CGET], PC), leg([], STOP))))), None),
    "root_prefixed_call": (doc(CGET_EXT, M("MAP_GP"), PC), CAPS_EXT),
    # --- compiler pending-map rule in legs
    "leg_call_map_map_put": (doc(branch(leg([GET, M("MAP_A"), M("MAP_B")], PUT),
                                        leg([CGET], STOP))), None),
    "leg_call_map_put": (doc(branch(leg([GET, M("MAP_GP")], PUT), leg([CGET], STOP))), None),
    "leg_call_map_call_map_stop": (doc(branch(leg([GET, M("MAP_GP"), PATCH, M("MAP_A")], STOP),
                                              leg([GET], STOP))), None),
    # --- cache-origin map
    "cache_origin_map_wrong_profile": (doc(branch(leg([GET], PUT),
                                                  leg([CGET, M("MAP_WRONG"), PATCH], STOP))), None),
    "cache_origin_map_right_profile": (doc(branch(leg([GET], PUT),
                                                  leg([CGET, M("MAP_GP"), PATCH], STOP))), None),
    # --- absent-stream map / cache_put
    "absent_stream_maps_in_legs": (doc(branch(leg([M("MAP_GP")], STOP), leg([M("MAP_GP")], STOP))), None),
    "absent_stream_map_put": (doc(branch(leg([M("MAP_GP")], PUT), leg([CGET], STOP))), None),
    "message_then_patch": (doc(MSG, PATCH, STOP), None),
    # --- connector on the call's path
    "leg_get_map_pc": (doc(branch(leg([GET, M("MAP_GP")], PC), leg([GET], STOP))), None),
    # --- stale DDP read after a cache read
    "stale_ddp_after_cacheget": (doc(
        GET, DDP("X"), PUT, CGET,
        DPP("Y", {"value_type": "ddp", "property_name": "X"}),
        PATCH, STOP), None),
    # --- EVAL-155-02 spine and the hoisted form
    "eval155_legacy_spine": (doc(
        DDP("SRC_PATH", PATH_SRC, PROF("P_GET")), bound(GET, "SRC_PATH", "P_GET"),
        M("MAP_GP"),
        DDP("TGT_PATH", PATH_SRC, PROF("P_PATCH")), bound(PATCH, "TGT_PATH", "P_PATCH"),
        STOP), None),
    "eval155_hoisted": (doc(
        DDP("SRC_PATH", PATH_SRC, PROF("P_GET")), DDP("TGT_PATH", PATH_SRC, PROF("P_PATCH")),
        bound(GET, "SRC_PATH", "P_GET"), M("MAP_GP"), bound(PATCH, "TGT_PATH", "P_PATCH"),
        STOP), None),
    # --- reference / preserved shapes (should compile today)
    "ref_staging_skeleton": (doc(branch(leg([GET], PUT), leg([CGET, M("MAP_GP"), DDP("X")], STOP))), None),
    "ref_decision_arm_prefix": (doc(branch(leg([GET], PUT), leg([CGET, M("MAP_GP"), DDP("X")], decision(
        leg([], PC), leg([], STOP))))), None),
    "ref_reversed_legs": (doc(branch(leg([CGET], STOP), leg([GET], PUT))), None),
    "ref_producer_prefix_root": (doc(CGET_EXT, PATCH, STOP), CAPS_EXT),
    "ref_empty_prefix_pc_sibling_connector": (doc(branch(leg([GET], STOP), leg([], PC))), None),
}


def outcome_of(exc):
    return [(d.code, d.path) for d in exc.diagnostics]


for name, (payload, caps) in FORMS.items():
    row = {"form": name}
    try:
        ir = parse_process_ir_v1(payload)
        row["parse"] = "ok"
    except ProcessIRValidationError as exc:
        row["parse"] = outcome_of(exc)
        print(json.dumps(row))
        continue
    try:
        _cfg, plan = compile_process_ir_v1(ir, SYMBOLS, capabilities=caps)
        emit_process(plan, SYMBOLS)
        row["compile"] = "ok+emit"
    except ProcessIRCompileError as exc:
        row["compile"] = [(d.code, d.path, d.phase) for d in exc.diagnostics]
    except Exception as exc:  # noqa: BLE001
        row["compile"] = "RAISED {0}: {1}".format(type(exc).__name__, str(exc)[:200])
    print(json.dumps(row))
