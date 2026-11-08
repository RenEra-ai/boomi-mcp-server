"""E1 #184: derive every row verdict (ADMIT / REFUTE / OPEN) from the ARCHIVED evidence.

ADMIT  = the stated behaviour is attested by every named check.
REFUTE = the stated behaviour is contradicted, and every check of the contrary observation holds.
OPEN   = a check failed or the row was not measured.
"""
import os, sys, json
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import lib184 as L

PS = L.load_state("provision")
CO = PS["components"]
N = PS["nonce"]
RS = L.load_state("runs")["runs"]


def rd(cap, rid, f):
    with open(os.path.join(L.EVD, cap, "runs", rid, f), encoding="utf-8") as fh:
        return fh.read()


def ev(cap, rid):
    r = RS[f"{cap}/{rid}"]
    return (r, rd(cap, rid, "process_log.txt"),
            [l for l in rd(cap, rid, "mock_access_log_window.txt").splitlines() if l.strip()],
            json.loads(rd(cap, rid, "wire_inbound_requests_window.json")))


def p(run, role):
    return f"/__E1_184__/{N}/{run}/{role}"


def line(path):
    return f'"POST {path} HTTP/1.1"'


def row(name, checks, observation, evidence, kind="ADMIT"):
    verdict = kind if all(checks.values()) else "OPEN"
    return {"row": name, "verdict": verdict, "checks": checks, "observation": observation, "evidence": evidence}


V = {}
# ---------------------------------------------------------------- cache-put successor
cap = "cap184-cache-put-successor"
rows = []
for rid, label in (("p1", "cache EMPTY before the run"), ("p2", "cache PRIMED in an earlier Branch leg")):
    r, log, m, w = ev(cap, rid)
    skip = "Document Cache Load\t\tNo documents found. Skipping execution for the Document Cache Retrieve step."
    rows.append(row(
        f"Linear Add to Cache -> Retrieve(all) -> connector ({label}): the retrieve and the connector run",
        {"status_COMPLETE": r["record"]["status"] == "COMPLETE",
         "load_skip_line_present": skip in log,
         "retrieve_never_executed": "Document Cache Retrieve\t\tShape executed" not in log,
         "after_retrieve_connector_never_executed": f"op_{rid}_after_{N}" not in log,
         "mock_window_is_exactly_the_witness": m and len(m) == 1 and line(p(rid, "witness")) in m[0],
         "no_after_retrieve_request_on_wire": not any(p(rid, "after-retrieve") in x["start_line"] for x in w)},
        f"REFUTED: '{skip}' — neither the retrieve nor the connector ran; the process read COMPLETE; the witness "
        "posted in the same execution.", [f"runs/{rid}/*"], kind="REFUTE"))
r, log, m, w = ev(cap, "p3")
skip3 = "Document Cache Load\t\tNo documents found. Skipping execution for the E1 deliberate exception step."
rows.append(row(
    "Try/Catch catch body 'Add to Cache -> Exception(caught {token}: {1})': the Exception fires",
    {"status_COMPLETE_not_ERROR": r["record"]["status"] == "COMPLETE" and not r["record"].get("message"),
     "catch_path_entered (protected connector request on the wire)": any(
         p("p3", "protected-connector-404-rae-false") in x["start_line"] and f"{N}-p3-doc" in x["body"] for x in w),
     "load_skip_line_names_the_exception_step": skip3 in log,
     "witness_present": any(line(p("p3", "witness")) in l for l in m)},
    f"REFUTED: the catch path ran (the protected 404 request is on the wire), then '{skip3}'. The Exception never "
    "fired and the execution reads COMPLETE: the caught error is swallowed.", ["runs/p3/*"], kind="REFUTE"))
r, log, m, w = ev(cap, "p3c")
rows.append(row(
    "ADDED control: catch body 'Exception(caught {token}: {1})' alone fires",
    {"status_ERROR": r["record"]["status"] == "ERROR",
     "message_names_the_exception_and_caught_error": f"caught {N}-p3c: [404] Not Found" in (r["record"].get("message") or ""),
     "catch_path_entered": any(f"{N}-p3c-doc" in x["body"] for x in w),
     "witness_present": any(line(p("p3c", "witness")) in l for l in m)},
    f"ERROR, record message: {r['record'].get('message')!r} — proves the Exception shape fires when reached, so p3's "
    "COMPLETE is the Add to Cache emitting nothing, not an inert Exception.", ["runs/p3c/*"]))
V[cap] = {"rows": rows}

# ---------------------------------------------------------------- remove -> read
cap = "cap184-cache-remove-read"
r, log, m, w = ev(cap, "rr")
skipr = "Document Cache Remove\t\tNo documents found. Skipping execution for the Document Cache Retrieve step."
V[cap] = {"rows": [row(
    "Remove from Cache(all) -> Retrieve from Cache(all) -> connector, cache primed in an earlier leg: the retrieve runs",
    {"status_COMPLETE": r["record"]["status"] == "COMPLETE",
     "remove_skip_line_present": skipr in log,
     "retrieve_never_executed": "Document Cache Retrieve\t\tShape executed" not in log,
     "mock_window_is_exactly_the_witness": len(m) == 1 and line(p("rr", "witness")) in m[0],
     "no_after_retrieve_request": not any(p("rr", "after-retrieve") in x["start_line"] for x in w)},
    f"REFUTED: '{skipr}'.", ["runs/rr/*"], kind="REFUTE")]}

# ---------------------------------------------------------------- retrieve DDP replacement
cap = "cap184-retrieve-ddp-replacement"
rows = []
expect = {"r1": ("X-cached", "X-current"), "r2": ("X-current", "X-cached"), "r3": ("X-cached", "X-current")}
names = {"r1": "R1 cached doc X=cached, current doc X=current -> the retrieved document's X is the CACHED value",
         "r2": "R2 cached doc has NO X, current doc X=current -> the retrieved document's X is the CURRENT value (not empty)",
         "r3": "R3 control (repeats e0): cached doc X=cached, current doc sets nothing -> X is the CACHED value"}
for rid, (seen, absent) in expect.items():
    r, log, m, w = ev(cap, rid)
    bound = [x for x in w if x["start_line"].startswith(f"POST /__E1_184__/{N}/{rid}/X-") or x["start_line"] == "POST / HTTP/1.1"]
    rows.append(row(names[rid],
                    {"status_COMPLETE": r["record"]["status"] == "COMPLETE",
                     f"mock_has_{seen}": any(line(p(rid, seen)) in l for l in m),
                     f"mock_has_no_{absent}_and_no_POST_root": not any(line(p(rid, absent)) in l or '"POST / HTTP/1.1"' in l for l in m),
                     "exactly_one_bound_request_and_its_body_is_the_CACHED_document":
                         len(bound) == 1 and bound[0]["body"] == json.dumps({"k": f"{N}-{rid}-cached"}),
                     "witness_present": any(line(p(rid, "witness")) in l for l in m)},
                    f"wire: {[x['start_line'] for x in bound]} body {[x['body'] for x in bound]}", [f"runs/{rid}/*"]))
r, log, m, w = ev(cap, "r4")
bx = [x for x in w if p("r4", "X-") in x["start_line"]]
by = [x for x in w if p("r4", "Y-") in x["start_line"]]
cached_body = json.dumps({"k": f"{N}-r4-cached"})
rows.append(row(
    "R4 as R1 plus Y=current set ONLY on the current doc -> X is CACHED and Y SURVIVES the retrieve",
    {"status_COMPLETE": r["record"]["status"] == "COMPLETE",
     "X_request_is_X-cached": len(bx) == 1 and bx[0]["start_line"] == f"POST {p('r4', 'X-cached')} HTTP/1.1",
     "Y_request_is_Y-current": len(by) == 1 and by[0]["start_line"] == f"POST {p('r4', 'Y-current')} HTTP/1.1",
     "both_bodies_are_the_cached_document": [x["body"] for x in bx + by] == [cached_body, cached_body],
     "no_POST_root": not any('"POST / HTTP/1.1"' in l for l in m),
     "witness_present": any(line(p("r4", "witness")) in l for l in m)},
    "X reached the wire as X-cached and Y as Y-current, both on the cached document's body.", ["runs/r4/*"]))
rows.append({"row": "Rule scope: N current documents x M cached documents (which current document's properties "
                    "each retrieved document inherits)",
             "verdict": "OPEN", "checks": {"measured": False},
             "observation": "Every R run retrieved exactly 1 cached document for exactly 1 current document.",
             "evidence": []})
V[cap] = {"rows": rows}

V["cap184-send-then-read"] = {"rows": [{"row": "non-producing connector -> Retrieve from Cache (primed) -> witness: the retrieve runs",
                                        "verdict": "OPEN", "checks": {"measured": False},
                                        "observation": "Not executed: no cheap non-producing operation on this stack (see RECORD.json).",
                                        "evidence": ["RECORD.json"]}]}
L.save_state("verdicts", V)
for c, v in V.items():
    for x in v["rows"]:
        bad = [k for k, ok in x["checks"].items() if not ok]
        print(f"{x['verdict']:6} {c:34} {x['row'][:96]}" + (f"  FAILED:{bad}" if bad and x['verdict'] == 'OPEN' and x['checks'] != {'measured': False} else ""))
