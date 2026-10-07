"""#186: a reader one call deeper — PARENT -> MIDDLE -> CACHE_CHILD — against the twin flattened twice.

Usage: deep.py <tree> <out.json>"""
import json
import sys

tree, out = sys.argv[1], sys.argv[2]
sys.path.insert(0, tree + "/src")
sys.path.insert(0, tree + "/tests")
import boomi_mcp  # noqa: E402

assert boomi_mcp.__file__.startswith(tree), boomi_mcp.__file__
import test_issue_184_child_state_transfer as T  # noqa: E402

GET, MSG, STOP, ENTRY, DYN_X = T._GET, T._MSG, T._STOP, T._ENTRY, T._DYNAMIC_X
SCRIPT = T._B30_SCRIPT
SET_P = {"kind": "set_dpp", "name": "X", "source_values": [{"value_type": "static", "value": "v"}]}
STEPS = {"none": [], "message": [MSG], "script": [SCRIPT]}


def use(item):
    return T._b30_decision() if item == "ddp" else T._b30s_decision("process.X")


def writer(item):
    return [GET, DYN_X] if item == "ddp" else [SET_P]


def head(form):
    return (ENTRY,) if form == "passthrough" else ()


def legs_of(doc):
    return doc["body"]["steps"][-1]["legs"]


def call(key):
    return T._call(key, wait=True, abort_on_error=True)


rows = []
for item in ("ddp", "dpp"):
    for mform in ("no_data", "passthrough"):
        for rform in ("no_data", "passthrough"):
            for mname, msteps in STEPS.items():
                for rname, rsteps in STEPS.items():
                    for placement in ("none", "earlier", "later", "middle_writes_elsewhere", "middle_writes_later"):
                        reader = T._doc(*head(rform), {"kind": "branch", "legs": [
                            {"steps": list(rsteps), "terminal": use(item)}, {"steps": [MSG], "terminal": STOP}]})
                        mid_legs = [{"steps": list(msteps), "terminal": call("CACHE_CHILD")}]
                        if placement == "middle_writes_elsewhere":
                            mid_legs = [{"steps": writer(item), "terminal": STOP}] + mid_legs
                        elif placement == "middle_writes_later":
                            mid_legs = mid_legs + [{"steps": writer(item), "terminal": STOP}]
                        else:
                            mid_legs = mid_legs + [{"steps": [MSG], "terminal": STOP}]
                        middle = T._doc(*head(mform), {"kind": "branch", "legs": mid_legs})
                        to_mid = {"steps": [], "terminal": call("WRITER")}
                        other = {"steps": writer(item) if placement in ("earlier", "later") else [GET], "terminal": STOP}
                        plegs = [to_mid, other] if placement == "later" else [other, to_mid]
                        parent = T._legs(*plegs)
                        roots = [("PARENT", parent), ("WRITER", middle), ("CACHE_CHILD", reader)]
                        flat_mid = []
                        for leg in mid_legs:
                            if (leg.get("terminal") or {}).get("process_ref") == "$ref:CACHE_CHILD":
                                flat_mid += [dict(rl, steps=list(leg["steps"]) + list(rl["steps"])) for rl in legs_of(reader)]
                            else:
                                flat_mid.append(leg)
                        flat_legs = []
                        for leg in plegs:
                            flat_legs += flat_mid if leg is to_mid else [leg]
                        flat = T._doc({"kind": "branch", "legs": flat_legs})
                        row = dict(item=item, mform=mform, rform=rform, mid=mname, rsteps=rname, placement=placement)
                        try:
                            composed = set()
                            for key, _d in roots:
                                composed |= {c for c, _p in T._b28_verdict(roots, key, True)[0]}
                            twin = {c for c, _p in T._b28_verdict([("PARENT", flat)], "PARENT", True)[0]}
                        except Exception as exc:  # noqa: BLE001
                            row["error"] = "%s: %s" % (type(exc).__name__, str(exc)[:300])
                            rows.append(row)
                            continue
                        row.update(composed=sorted(composed), twin=sorted(twin))
                        rows.append(row)
json.dump(rows, open(out, "w"), indent=0, sort_keys=True)
print("cells", len(rows), "errors", sum(1 for r in rows if "error" in r))
