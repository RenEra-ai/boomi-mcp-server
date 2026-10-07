"""Batch 21a coverage matrix probe. Usage: matrix.py <arm_root_with_src> <label> [<tests_dir>]

Axes (brief): removal {proved, unproved, none} x ride-on {same cache, re-cache into CACHE2}
x step {none, message} x site {own, passthrough discharge, inherited row} x child fills the
ride-on cache itself {yes, no} x consumer {bound, ordinary, content}. (Declared reads are
measured by a separate block at the end.)

Callers per cell: `bad` (a document that must be refused stored in the ride-on cache beside
the good ones), `good` (only satisfying documents), `nothing` (nothing in the ride-on cache).
"""
import json
import sys
from pathlib import Path

ARM = Path(sys.argv[1]).resolve()
LABEL = sys.argv[2]
TESTS = Path(sys.argv[3]).resolve() if len(sys.argv) > 3 else ARM / "tests"
sys.path.insert(0, str(TESTS))
sys.path.insert(0, str(ARM / "src"))

import boomi_mcp  # noqa: E402

assert str(Path(boomi_mcp.__file__).resolve()).startswith(str(ARM)), boomi_mcp.__file__
from unittest.mock import MagicMock  # noqa: E402

from boomi_mcp.categories import integration_builder  # noqa: E402

integration_builder.paginate_metadata = MagicMock(return_value=[])
import test_issue_184_child_entries as CE  # noqa: E402

for mod in list(sys.modules):
    if mod.startswith("boomi_mcp") and getattr(sys.modules[mod], "__file__", None):
        assert str(Path(sys.modules[mod].__file__).resolve()).startswith(str(ARM)), mod

_legs, _call, _errors, _compile_errors, _doc = CE._legs, CE._call, CE._errors, CE._compile_errors, CE._doc
_MSG, _STOP, _BOUND_GET, _STATIC_X, _DYNAMIC_X, _ENTRY = CE._MSG, CE._STOP, CE._BOUND_GET, CE._STATIC_X, CE._DYNAMIC_X, CE._ENTRY
_resolve = CE._resolve

C, C2 = "$ref:CACHE", "$ref:CACHE2"
GET = {"kind": "connector_call", "operation_ref": "$ref:GET"}
GETP1 = {"kind": "connector_call", "operation_ref": "$ref:GETP1"}
M12 = {"kind": "map_ref", "map_ref": "$ref:M12"}
M22 = {"kind": "map_ref", "map_ref": "$ref:M22"}
READS_X = {"kind": "set_dpp", "name": "Y", "source_values": [{"value_type": "ddp", "property_name": "X"}]}


def read(ref):
    return {"kind": "cache_get", "cache_ref": ref}


def put(ref):
    return {"kind": "cache_put", "cache_ref": ref}


def remove(ref):
    return {"kind": "cache_remove", "cache_ref": ref}


def leg(steps, terminal):
    return {"steps": list(steps), "terminal": terminal}


CONSUMERS = {
    # consumer steps, what satisfies it, what violates it, the passthrough child that consumes it
    "bound": ([_BOUND_GET], [GET, _DYNAMIC_X], [GET, _STATIC_X], _doc(_ENTRY, _BOUND_GET, _STOP)),
    "ordinary": ([READS_X], [GET, _DYNAMIC_X], [GET], _doc(_ENTRY, READS_X, _STOP)),
    "content": ([M22], [GETP1, M12], [GETP1], _doc(_ENTRY, M22, _STOP)),
}


def s(v):
    return "clean" if not v else sorted({(c.replace("PROCESS_IR_", ""), p) for c, p in v})


def safe(fn, *a):
    try:
        return s(fn(*a))
    except Exception as exc:  # noqa: BLE001
        return "EXC %s: %s" % (type(exc).__name__, str(exc)[:160])


def cell(removal, form, step, site, fills, consumer):
    uses, good, bad, pt_child = CONSUMERS[consumer]
    ride = C if form == "same" else C2
    msg = [_MSG] if step == "message" else []
    pre = []
    if removal == "proved":
        pre.append(leg([], remove(ride)))
    elif removal == "unproved":
        pre.append(leg([GET], remove(ride)))
    if form == "recache":
        # re-cache what the seeded cache holds; an ordinary read of X on the property channels
        pre.append(leg([read(C)] + ([READS_X] if consumer != "content" else []), put(C2)))
    if fills:
        pre.append(leg(good, put(ride)))
    if site == "own":
        tail = [leg([read(ride)] + msg + uses, _STOP)]
        retriever_legs = pre + tail
        roots_extra = []
        flat_tail = tail
    elif site == "passthrough":
        tail = [leg([read(ride)] + msg, _call("BOUND", wait=True))]
        retriever_legs = pre + tail
        roots_extra = [("BOUND", pt_child)]
        flat_tail = [leg([read(ride)] + msg + uses, _STOP)]
    else:  # inherited: the middle does everything but the retrieve and use
        grandchild = _legs(leg([read(ride)] + msg + uses, _STOP), leg([_MSG], _STOP))
        retriever_legs = pre + [leg([], _call("CACHE_CHILD_P1"))]
        roots_extra = [("CACHE_CHILD_P1", grandchild)]
        flat_tail = [leg([read(ride)] + msg + uses, _STOP)]
    child = _legs(*retriever_legs) if len(retriever_legs) > 1 else _legs(*(retriever_legs + [leg([_MSG], _STOP)]))
    callers = {}
    if form == "same":
        callers["bad"] = [leg(good, put(C)), leg(bad, put(C))]
        callers["good"] = [leg(good, put(C))]
        callers["nothing"] = [leg([_MSG], _STOP)]
    else:
        callers["bad"] = [leg(good, put(C)), leg(bad, put(C2))]
        callers["good"] = [leg(good, put(C)), leg(good, put(C2))]
        callers["nothing"] = [leg(good, put(C))]
    out = {}
    for caller, caller_legs in callers.items():
        parent = _legs(*(caller_legs + [leg([], _call("CACHE_CHILD"))]))
        roots = [("PARENT", parent), ("CACHE_CHILD", child)] + roots_extra
        flat = _legs(*(caller_legs + pre + flat_tail))
        rec = {
            "parent_v": safe(_errors, roots, "PARENT"),
            "parent_c": safe(_compile_errors, roots, "PARENT"),
            "child_v": safe(_errors, roots, "CACHE_CHILD"),
            "flat_v": safe(_errors, [("PARENT", flat)], "PARENT"),
        }
        for key, _doc_ in roots_extra:
            rec[key.lower() + "_v"] = safe(_errors, roots, key)
        out[caller] = rec
    return out


results = {}
for removal in ("proved", "unproved", "none"):
    for form in ("same", "recache"):
        for step in ("none", "message"):
            for site in ("own", "passthrough", "inherited"):
                for fills in (True, False):
                    for consumer in sorted(CONSUMERS):
                        name = "|".join((removal, form, step, site, "fills" if fills else "nofill", consumer))
                        results[name] = cell(removal, form, step, site, fills, consumer)

print(json.dumps({"arm": LABEL, "cells": results}, indent=1, default=str))
