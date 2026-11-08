"""E1 #184 runner. usage: p22_run.py <run-id> [...]  (run ids = p1 p2 p3 p3c rr r1 r2 r3 r4)

Per run archives: execute envelope, ExecutionRecord (bound to THIS execution id), the full process log,
the extracted per-shape lines + every "No documents found. Skipping execution for ..." line, the VERBATIM
cds-mock window, and the wire-captured inbound requests (start line + body) in the same window.
"""
import os, sys, json, time, re, datetime
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import lib184 as L
import wire184 as WR

PS = L.load_state("provision")
CO = PS["components"]
N = PS["nonce"]
RS = L.load_state("runs") or {"runs": {}}
WIRE = os.path.join(L.HERE, "wire", "wire.jsonl")
RUNS = {"p1": ("cap184-cache-put-successor", "p1_put_get_empty"),
        "p2": ("cap184-cache-put-successor", "p2_put_get_primed"),
        "p3": ("cap184-cache-put-successor", "p3_catch_put_exception"),
        "p3c": ("cap184-cache-put-successor", "p3c_catch_exception_control"),
        "rr": ("cap184-cache-remove-read", "rr_remove_then_retrieve"),
        "r1": ("cap184-retrieve-ddp-replacement", "r1_ddp_replacement"),
        "r2": ("cap184-retrieve-ddp-replacement", "r2_ddp_replacement"),
        "r3": ("cap184-retrieve-ddp-replacement", "r3_ddp_replacement"),
        "r4": ("cap184-retrieve-ddp-replacement", "r4_ddp_replacement")}


def iso_back(sec):
    return (datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(seconds=sec)).strftime("%Y-%m-%dT%H:%M:%SZ")


def run(rid):
    cap, role = RUNS[rid]
    key = f"{cap}/{rid}"
    if RS["runs"].get(key, {}).get("done"):
        print("skip", key)
        return
    pid = CO[role]["id"]
    since_log, since_rec, t0 = iso_back(3), iso_back(10), time.time() - 3
    print(f"\n=== {key}: {role} {pid}")
    res = L.execute(pid)
    eid = res["execution_id"]
    if not eid:
        for _ in range(12):
            rows = [x for x in (((L.api_query("ExecutionRecord", "processId", pid).get("data") or {}).get("result")) or [])
                    if (x.get("executionTime") or "") >= since_rec]
            if rows:
                eid = sorted(rows, key=lambda x: x["executionTime"])[-1]["executionId"]
                break
            time.sleep(5)
    rows = L.poll_record(eid) if eid else []
    prow = rows[0] if rows else {}
    time.sleep(10)
    window = [l.rstrip() for l in L.mock_log(since_log).splitlines() if l.strip()]
    reqs = WR.requests(WIRE, t0, time.time() + 1)
    wire = [{"client": r["client"], "start_line": r["start_line"], "body": r["body"], "body_bytes": r["body_bytes"]}
            for r in reqs]
    log, _ = L.exec_logs(eid) if eid else ("", {})
    skip = [l for l in log.splitlines() if "No documents found. Skipping execution for" in l]
    shape_lines = [l for l in log.splitlines() if re.search(r"\t(INFO|WARNING|SEVERE|FINE)\t", l)]
    print(f"  exec={eid} status={prow.get('status')} in={prow.get('inboundDocumentCount')} "
          f"out={prow.get('outboundDocumentCount')} message={str(prow.get('message'))[:200]!r}")
    for l in window:
        print("  MOCK|", l)
    for w in wire:
        print("  WIRE|", w["start_line"], "| body:", repr(w["body"][:120]))
    for l in skip:
        print("  SKIP|", l)
    d = f"runs/{rid}"
    files = [L.archive(cap, f"{d}/execute.json", res["execute"]),
             L.archive(cap, f"{d}/execution_record.json", prow),
             L.archive(cap, f"{d}/process_log.txt", log),
             L.archive(cap, f"{d}/process_log_skip_lines.txt", "\n".join(skip) + ("\n" if skip else "")),
             L.archive(cap, f"{d}/mock_access_log_window.txt", "\n".join(window) + ("\n" if window else "")),
             L.archive(cap, f"{d}/wire_inbound_requests_window.json", wire)]
    RS["runs"][key] = {"capture": cap, "run_id": rid, "root_role": role, "root_id": pid,
                       "started_utc": res["started_utc"], "ended_utc": L.utc(), "execution_id": eid,
                       "record": {k: prow.get(k) for k in ("status", "executionType", "inboundDocumentCount",
                                                           "outboundDocumentCount", "inboundErrorDocumentCount",
                                                           "message", "executionTime")},
                       "mock_window": window, "wire": wire, "skip_lines": skip,
                       "files": files, "tree": L.tree_stamp(), "done": True}
    L.save_state("runs", RS)


for rid in sys.argv[1:]:
    run(rid)
