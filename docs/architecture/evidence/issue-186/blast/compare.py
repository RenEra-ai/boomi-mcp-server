"""Batch 30: request-level comparison of the two arms' replays (base b86aea9, work = this tree).
A request is admitted iff every root validates clean; its twin is every TOP root flattened (work arm).
Usage: compare.py <corpus> [<corpus> ...]   (reads out/<arm>_<corpus>_*.jsonl)"""
import collections
import glob
import json
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent


def load(arm, corpus):
    rows = {}
    for f in sorted(glob.glob(str(HERE / "out" / ("%s_%s_*.jsonl" % (arm, corpus))))):
        for line in open(f):
            r = json.loads(line)
            rows[r["id"]] = r
    return rows


def codes(r):
    v = r.get("none")
    if isinstance(v, str):
        return v
    return sorted({c for k, val in v.items() if k not in ("ok", "findings") for c, _p in val})


grand = collections.Counter()
for corpus in sys.argv[1:]:
    base, work = load("base", corpus), load("work", corpus)
    assert set(base) == set(work), (corpus, len(base), len(work))
    tally, moves = collections.Counter(), []
    for rid in sorted(base):
        b, w = base[rid], work[rid]
        tally["requests"] += 1
        if b["status"] != "ok" or w["status"] != "ok":
            tally["parse-refused" + ("" if b["status"] == w["status"] else ":DIFFERENT")] += 1
            continue
        if not w["spellings_equal"] or not b["spellings_equal"]:
            tally["SPELLINGS DIFFER (base %s, work %s)" % (b["spellings_equal"], w["spellings_equal"])] += 1
        tally["rows-" + ("identical" if b["rows"] == w["rows"] else "moved")] += 1
        if b["none"] == w["none"]:
            tally["verdicts-identical"] += 1
            continue
        tally["verdicts-moved"] += 1
        bc, wc = codes(b), codes(w)
        tw = None
        if w["complete_twin"] and w["twins"] and all(isinstance(t, list) for t in w["twins"].values()):
            tw = sorted({c for t in w["twins"].values() for c in t})
        if isinstance(bc, str) or isinstance(wc, str):
            kind = "raised: base %s / work %s" % (isinstance(bc, str), isinstance(wc, str))
        elif bc == wc:
            kind = "request codes unchanged (moved between roots/paths)"
        elif not bc and wc:
            kind = "admission->refusal, twin " + ("n/a" if tw is None else ("refuses" if tw else "ADMITS (OVER-REFUSAL)"))
        elif bc and not wc:
            kind = "refusal->admission, twin " + ("n/a" if tw is None else ("admits" if not tw else "REFUSES (REGRESSION)"))
        else:
            kind = "refused both, codes moved"
            if tw is not None:
                kind += " (work==twin)" if set(wc) == set(tw) else (" (base==twin)" if set(bc) == set(tw) else "")
        if w["legacy"]:
            kind = "LEGACY " + kind
        tally[kind] += 1
        moves.append({"id": rid, "kind": kind, "base": bc, "work": wc, "twin": tw,
                      "base_twin": b.get("twins"), "work_twin": w.get("twins")})
    with open(HERE / ("moves_%s.jsonl" % corpus), "w") as fh:
        for m in moves:
            fh.write(json.dumps(m) + "\n")
    print("==", corpus)
    for k, v in sorted(tally.items()):
        print("  %6d %s" % (v, k))
        grand[k] += v
print("== TOTAL")
for k, v in sorted(grand.items()):
    print("  %6d %s" % (v, k))
