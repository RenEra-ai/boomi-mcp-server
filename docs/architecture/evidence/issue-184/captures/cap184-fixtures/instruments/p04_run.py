"""E0 #184 runner: execute one capture's runs and archive, per run:
execute envelope, parent ExecutionRecord, child ExecutionRecords, process logs,
the VERBATIM cds-mock access-log window, and the wire-captured inbound requests
(start line + body) in the same window.

usage: p04_run.py <capture-key> [run-id ...]
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

CAPS = {
    "grp": ("cap184-passthrough-group", [("grp-1", "pa_grp", ["ch_grp"], 15)]),
    "wait": ("cap184-passthrough-wait-true", [("wf-1", "pa_wf", ["ch_grp"], 45)]),
    "nd": ("cap184-nodata-per-document", [("nd-1", "pa_nd", ["ch_nd"], 15)]),
    "ddp": ("cap184-passthrough-ddp-handoff", [("ddp-pt-1", "pa_ddp_pt", ["ch_ddp_pt"], 15),
                                               ("ddp-nd-1", "pa_ddp_nd", ["ch_ddp_nd"], 15),
                                               ("ddp-cache-1", "pa_ddp_cache", ["ch_ddp_cache"], 15),
                                               ("ddp-cache-inproc-1", "pa_ddp_cache_inproc", [], 12)]),
    "cache": ("cache", None),
    "dpp": ("cap184-dpp-both-ways", [("dpp-pt-1", "pa_dpp_pt", ["ch_dpp_pt"], 15),
                                     ("dpp-nd-1", "pa_dpp_nd", ["ch_dpp_nd"], 15)]),
    "sa": ("cap184-passthrough-standalone", [("sa-1", "ch_grp", [], 12)]),
}
CAPS["cache"] = ("cap184-shared-cache", [("cache-1", "pa_cache", ["ch_cache_pt", "ch_cache_nd"], 15),
                                         ("cache-sa-pt", "ch_cache_pt", [], 12),
                                         ("cache-sa-nd", "ch_cache_nd", [], 12),
                                         ("cache-sa-control", "ch_grp", [], 12)])
CAPS["xrem"] = ("cap184-prefix-predecessors", [("xr-remove-successor-1", "xr_remove_successor", [], 12)])
CAPS["xddp"] = ("cap184-passthrough-ddp-handoff", [("xr-ddp-cache-linear-1", "xr_ddp_cache_linear", [], 12)])
PFX = sorted(r for r in CO if r.startswith("pfx_"))
CAPS["pfx"] = ("cap184-prefix-predecessors", [(f"{r}-1", r, ["ch_pfx"], 12) for r in PFX])


def iso_back(sec):
    return (datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(seconds=sec)).strftime(
        "%Y-%m-%dT%H:%M:%SZ")


def child_records(child_id, since_iso, expect_min=0, tries=5):
    rows = []
    for i in range(tries):
        r = L.api_query("ExecutionRecord", "processId", child_id)
        allrows = ((r.get("data") or {}).get("result")) or []
        rows = [x for x in allrows if (x.get("executionTime") or "") >= since_iso]
        pending = [x for x in rows if x.get("status") in ("INPROCESS", "STARTED")]
        if len(rows) >= expect_min and not pending and i >= 1:
            break
        time.sleep(6)
    return sorted(rows, key=lambda x: x.get("executionTime") or "")


def keep_headers(h):
    return {k: v for k, v in h.items() if k in ("host", "content-type", "content-length", "user-agent",
                                                "transfer-encoding", "accept")}


def run(cap, run_id, root, children, settle):
    key = f"{cap}/{run_id}"
    if RS["runs"].get(key, {}).get("done"):
        print("skip", key)
        return RS["runs"][key]
    pid = CO[root]["id"]
    since_log = iso_back(3)
    since_rec = iso_back(10)
    t_wire0 = time.time() - 3
    print(f"\n=== {key}: {root} {pid}")
    res = L.execute(pid)
    eid = res["execution_id"]
    rows = L.poll_record(eid) if eid else []
    prow = rows[0] if rows else {}
    print(f"  parent exec={eid} wait_status={res['wait_status']} record_status={prow.get('status')} "
          f"type={prow.get('executionType')} in={prow.get('inboundDocumentCount')} out={prow.get('outboundDocumentCount')}")
    time.sleep(settle)
    kids = {}
    for c in children:
        kids[c] = child_records(CO[c]["id"], since_rec, expect_min=1)
        for x in kids[c]:
            print(f"  child {c}: exec={x.get('executionId')} status={x.get('status')} type={x.get('executionType')} "
                  f"in={x.get('inboundDocumentCount')} out={x.get('outboundDocumentCount')} "
                  f"parent={x.get('parentExecutionId')} top={x.get('topLevelExecutionId')}")
        if not kids[c]:
            print(f"  child {c}: NO ExecutionRecord rows since {since_rec}")
    t_wire1 = time.time() + 1
    window = [l.rstrip() for l in L.mock_log(since_log).splitlines() if l.strip()]
    nonce_lines = [l for l in window if N in l]
    reqs = WR.requests(WIRE, t_wire0, t_wire1)
    wire = [{"client": r["client"], "start_line": r["start_line"], "headers": keep_headers(r["headers"]),
             "body": r["body"], "body_bytes": r["body_bytes"]} for r in reqs]
    for l in nonce_lines:
        print("  MOCK|", l)
    for w in wire:
        print("  WIRE|", w["start_line"], "| body:", repr(w["body"][:120]))
    plog, plog_env = L.exec_logs(eid) if eid else ("", {})
    klogs = {}
    for c, xs in kids.items():
        for x in xs:
            t, _ = L.exec_logs(x["executionId"])
            klogs[x["executionId"]] = t
    d = f"runs/{run_id}"
    files = [L.archive(cap, f"{d}/execute.json", res["execute"]),
             L.archive(cap, f"{d}/parent_execution_record.json", prow),
             L.archive(cap, f"{d}/child_execution_records.json", kids),
             L.archive(cap, f"{d}/parent_process_log.txt", plog),
             L.archive(cap, f"{d}/mock_access_log_window.txt", "\n".join(window) + "\n"),
             L.archive(cap, f"{d}/wire_inbound_requests_window.json", wire)]
    for k, t in klogs.items():
        files.append(L.archive(cap, f"{d}/child_{k}_process_log.txt", t))
    rec = {"capture": cap, "run_id": run_id, "root_role": root, "root_id": pid, "children": {c: CO[c]["id"] for c in children},
           "started_utc": res["started_utc"], "ended_utc": L.utc(), "mock_log_since": since_log,
           "wire_window_epoch": [t_wire0, t_wire1], "execution_id": eid, "wait_status": res["wait_status"],
           "parent_record": {k: prow.get(k) for k in ("status", "executionType", "inboundDocumentCount",
                                                     "outboundDocumentCount", "inboundErrorDocumentCount",
                                                     "message", "executionTime", "executionDuration")},
           "child_records": {c: [{k: x.get(k) for k in ("executionId", "status", "executionType", "inboundDocumentCount",
                                                         "outboundDocumentCount", "inboundErrorDocumentCount",
                                                         "parentExecutionId", "topLevelExecutionId", "executionTime",
                                                         "message")} for x in xs] for c, xs in kids.items()},
           "mock_nonce_lines": nonce_lines, "mock_window_line_count": len(window),
           "wire_nonce_requests": [w for w in wire if N in w["start_line"] or N in w["body"]],
           "wire_all_request_count": len(wire),
           "parent_log_process_call_lines": [l for l in plog.splitlines() if re.search(r"(?i)process call|subprocess|child", l)],
           "files": files, "done": True, "tree": L.tree_stamp()}
    RS["runs"][key] = rec
    L.save_state("runs", RS)
    return rec


if __name__ == "__main__":
    cap_key = sys.argv[1]
    only = set(sys.argv[2:])
    cap, runs = CAPS[cap_key]
    for run_id, root, children, settle in runs:
        if only and run_id not in only:
            continue
        run(cap, run_id, root, children, settle)
