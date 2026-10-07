"""#186 property-read matrix: composition vs flattened twin, on ONE tree.

Usage: matrix.py <tree> <out.json>
Cells: item (ddp|dpp) x child entry form (no_data|passthrough) x between kind x writer placement x dialect.
"""
import json
import sys

tree, out = sys.argv[1], sys.argv[2]
sys.path.insert(0, tree + "/src")
sys.path.insert(0, tree + "/tests")
import boomi_mcp  # noqa: E402

assert boomi_mcp.__file__.startswith(tree), boomi_mcp.__file__
import test_issue_184_child_state_transfer as T  # noqa: E402

GET, MSG, STOP, ENTRY, DYN_X = T._GET, T._MSG, T._STOP, T._ENTRY, T._DYNAMIC_X
SRC = T._B27_SOURCE
SET_P = {"kind": "set_dpp", "name": "X", "source_values": [{"value_type": "static", "value": "v"}]}


def use(item):
    return T._b30_decision() if item == "ddp" else T._b30s_decision("process.X")


def writer_steps(item):
    return [GET, DYN_X] if item == "ddp" else [SET_P]


def child_doc(form, typed, steps, terminal):
    head = (ENTRY,) if form == "passthrough" else (() if typed else (SRC,))
    return T._doc(*head, {"kind": "branch", "legs": [
        {"steps": list(steps), "terminal": terminal}, {"steps": [MSG], "terminal": STOP}]})


WRITER_CHILD = lambda item: T._legs({"steps": writer_steps(item), "terminal": STOP},  # noqa: E731
                                    {"steps": [MSG], "terminal": STOP})


def build(item, form, typed, steps, terminal, placement):
    head = ()
    child = child_doc(form, typed, steps, terminal)
    twin_head = (SRC,) if (form == "no_data" and not typed) else ()
    child_legs = child["body"]["steps"][-1]["legs"]
    call = {"steps": [], "terminal": T._call("CACHE_CHILD", wait=True, abort_on_error=True)}
    roots_extra = []
    other = {"steps": [GET], "terminal": STOP}
    writer = {"steps": [GET] + writer_steps(item)[1:] if item == "ddp" else writer_steps(item), "terminal": STOP}
    if item == "ddp":
        writer = {"steps": [GET, DYN_X], "terminal": STOP}
    twin_writer_legs = None
    if placement == "none":
        legs = [other, call]
    elif placement == "earlier":
        legs = [writer, call]
    elif placement == "later":
        legs = [call, writer]
    elif placement == "same":
        call = {"steps": writer_steps(item)[-1:], "terminal": call["terminal"]}
        legs = [other, call]
    elif placement.startswith("sibling"):
        wcall = {"steps": [], "terminal": T._call(
            "WRITER", **({"wait": False} if placement == "sibling_unwaited" else {"wait": True, "abort_on_error": True}))}
        legs = [call, wcall] if placement == "sibling_later" else [wcall, call]
        roots_extra = [("WRITER", WRITER_CHILD(item))]
        twin_writer_legs = WRITER_CHILD(item)["body"]["steps"][-1]["legs"]
    else:
        raise ValueError(placement)
    parent = T._doc(*head, {"kind": "branch", "legs": legs})
    flat_legs = []
    for leg in legs:
        terminal_ = leg.get("terminal") or {}
        if terminal_.get("process_ref") == "$ref:CACHE_CHILD":
            flat_legs += [dict(cl, steps=list(leg["steps"]) + list(cl["steps"])) for cl in child_legs]
        elif terminal_.get("process_ref") == "$ref:WRITER":
            flat_legs += twin_writer_legs
        else:
            flat_legs.append(leg)
    flat = T._doc(*twin_head, {"kind": "branch", "legs": flat_legs})
    return [("PARENT", parent), ("CACHE_CHILD", child)] + roots_extra, flat


def codes(found):
    return sorted({code for code, _path in found})


def verdict(roots, key, typed):
    return T._b28_verdict(roots, key, typed)[0]


rows = []
kinds = ["none"] + T._b28_kinds_that_can_be_between()
for item in ("ddp", "dpp"):
    for dialect, typed in (("typed", True), ("legacy", False)):
        for form in ("no_data", "passthrough"):
            for kind in kinds:
                for cell, steps, terminal in T._b30_between(kind, use(item)):
                    for placement in ("none", "earlier", "later", "same", "sibling_earlier", "sibling_later",
                                      "sibling_unwaited"):
                        row = {"item": item, "dialect": dialect, "form": form, "cell": cell, "placement": placement}
                        try:
                            roots, flat = build(item, form, typed, steps, terminal, placement)
                            per_root = {key: verdict(roots, key, typed) for key, _doc in roots}
                            twin = verdict([("PARENT", flat)], "PARENT", typed)
                        except Exception as exc:  # noqa: BLE001
                            row["error"] = "%s: %s" % (type(exc).__name__, str(exc)[:300])
                            rows.append(row)
                            continue
                        composed = sorted({c for found in per_root.values() for c, _p in found})
                        row.update(composed=composed, twin=codes(twin),
                                   per_root={k: sorted(set(v)) for k, v in per_root.items()})
                        rows.append(row)
json.dump(rows, open(out, "w"), indent=0, sort_keys=True)
print("cells", len(rows), "errors", sum(1 for r in rows if "error" in r))
