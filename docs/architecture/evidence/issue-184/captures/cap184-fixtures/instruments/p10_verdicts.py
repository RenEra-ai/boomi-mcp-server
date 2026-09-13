"""E0 #184: derive every admission-row verdict from the ARCHIVED evidence (never from memory).

A row is ADMIT only when every named check is True; a failed check makes it OPEN unless the row
is explicitly a refutation row (observed contrary to the dispatch expectation -> REFUSE of that
expectation). Also runs the child-ExecutionRecord positive control live and archives it.
"""
import os, sys, json, re
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import lib184 as L

PS = L.load_state("provision")
CO = PS["components"]
N = PS["nonce"]
RS = L.load_state("runs")["runs"]
NAMEP = "_TEST_184E0_"


def rdir(cap, rid):
    return os.path.join(L.EVD, cap, "runs", rid)


def txt(cap, rid, f):
    with open(os.path.join(rdir(cap, rid), f), encoding="utf-8") as fh:
        return fh.read()


def mock(cap, rid):
    return [l for l in txt(cap, rid, "mock_access_log_window.txt").splitlines() if l.strip()]


def wire(cap, rid):
    return json.loads(txt(cap, rid, "wire_inbound_requests_window.json"))


def kids(cap, rid):
    return json.loads(txt(cap, rid, "child_execution_records.json"))


def rec(cap, rid):
    return RS[f"{cap}/{rid}"]


def body_k(i, tag):
    return json.dumps({"k": f"{N}-{tag}{i}"}, separators=(",", ":"))


def row(name, checks, observation, evidence, refutes=False):
    ok = all(checks.values())
    if refutes:
        verdict = "REFUSE" if ok else "OPEN"
    else:
        verdict = "ADMIT" if ok else "OPEN"
    return {"row": name, "verdict": verdict, "checks": checks, "observation": observation, "evidence": evidence}


def mline(p, n):
    return f'"POST {p} HTTP/1.1"'


def norm(x):
    return x.replace("&apos;", "'").replace("&quot;", '"')


def stored_check(role):
    c = CO[role]
    cap = c["captures"][0]
    d = os.path.join(L.EVD, cap, "components")
    sub = open(os.path.join(d, f"{role}.submitted.xml"), encoding="utf-8").read()
    sto = open(os.path.join(d, f"{role}.stored.xml"), encoding="utf-8").read()
    s1 = re.search(r"<shapes>.*</shapes>", sub, re.S).group(0)
    s2 = re.search(r"<shapes>.*</shapes>", sto, re.S).group(0)
    return {"stored_shapes_equal_modulo_entity_escaping": norm(s1) == norm(s2),
            "stored_shapes_byte_equal": s1 == s2,
            "options_submitted": c.get("process_options_submitted"), "options_stored": c.get("process_options_stored")}


V = {}
PT_ROLES = [r for r, c in CO.items() if c["kind"] == "process"]
store = {r: stored_check(r) for r in PT_ROLES}

# ---------------------------------------------------------------- child-record positive control (live)
ctl = {}
for role in ("ch_grp", "ch_cache_pt", "ch_ddp_pt", "ch_ddp_cache", "ch_dpp_pt", "ch_pfx", "ch_nd"):
    r = L.api_query("ExecutionRecord", "processId", CO[role]["id"])
    rows = ((r.get("data") or {}).get("result")) or []
    ctl[role] = [{k: x.get(k) for k in ("executionId", "executionType", "status", "topLevelExecutionId",
                                         "inboundDocumentCount", "outboundDocumentCount")} for x in rows]
standalone_ids = {rec("cap184-passthrough-standalone", "sa-1")["execution_id"],
                  rec("cap184-shared-cache", "cache-sa-control")["execution_id"]}
ctl_ok = {
    "ch_grp_rows_are_exactly_the_two_standalone_runs": {x["executionId"] for x in ctl["ch_grp"]} == standalone_ids
    and all(x["executionType"] == "exec_manual" for x in ctl["ch_grp"]),
    "ch_cache_pt_rows_are_exactly_its_standalone_run": [x["executionId"] for x in ctl["ch_cache_pt"]]
    == [rec("cap184-shared-cache", "cache-sa-pt")["execution_id"]],
    "passthrough_children_never_run_standalone_have_zero_rows": all(
        ctl[r] == [] for r in ("ch_ddp_pt", "ch_ddp_cache", "ch_dpp_pt", "ch_pfx")),
    "nodata_child_has_sub_process_rows": len(ctl["ch_nd"]) == 3 and all(
        x["executionType"] == "sub_process" for x in ctl["ch_nd"]),
}
ctl_file = L.archive("cap184-passthrough-group", "controls/child_execution_record_query_control.json",
                     {"queried_utc": L.utc(), "rows": ctl, "checks": ctl_ok})
print("record-query control:", json.dumps(ctl_ok))

# ---------------------------------------------------------------- passthrough group
cap = "cap184-passthrough-group"
r, log, m, w, k = rec(cap, "grp-1"), txt(cap, "grp-1", "parent_process_log.txt"), mock(cap, "grp-1"), wire(cap, "grp-1"), kids(cap, "grp-1")
p = f"/__E0_184__/{N}/grp/child"
V[cap] = {"rows": [row(
    "Data Passthrough child, wait=true abort=true: N parent documents reach ONE child invocation as one group",
    {"parent_COMPLETE": r["parent_record"]["status"] == "COMPLETE",
     "parent_outbound_3": r["parent_record"]["outboundDocumentCount"] == 3,
     "mock_window_exactly_3_child_lines": len(m) == 3 and all(mline(p, 0) in l for l in m),
     "wire_bodies_are_d1_d2_d3": sorted(x["body"] for x in w if p in x["start_line"]) == [body_k(i, "grp-d") for i in (1, 2, 3)],
     "one_Process_Call_shape_with_3_docs": log.count("Executing Process Call Shape with 3 document(s).") == 1,
     "one_child_invocation": log.count(f"Executing process {NAMEP}ch_grp_{N}") == 1,
     "child_connector_one_execution_with_3_docs": log.count(f"{NAMEP}op_grp_child_{N}\tExecuting Connector Shape with 3 document(s).") == 1,
     "log_says_passing_in_start_data": "passing in start data, no return data" in log,
     "child_has_NO_own_ExecutionRecord": k.get("ch_grp") == [],
     "record_query_control_passed": all(ctl_ok.values()),
     "stored_processcall_wait_true_abort_true": CO["pa_grp"]["processcall_attrs_stored"] == [
         f'<processcall abort="true" processId="{CO["ch_grp"]["id"]}" wait="true">'],
     "stored_start_passthroughaction": CO["ch_grp"]["start_config_stored"] == ["<passthroughaction/>"]},
    "One Process Call shape execution with 3 documents; the child's shapes run INSIDE the parent execution's log "
    "(one Connector execution with 3 documents -> 3 requests, bodies d1..d3); no ExecutionRecord for the child "
    "(query proven by control: the same query returns the child's standalone rows).",
    ["runs/grp-1/*", "controls/child_execution_record_query_control.json"])]}

# ---------------------------------------------------------------- wait
cap = "cap184-passthrough-wait-true"
r, log, m, w, k = rec(cap, "wf-1"), txt(cap, "wf-1", "parent_process_log.txt"), mock(cap, "wf-1"), wire(cap, "wf-1"), kids(cap, "wf-1")
V[cap] = {"rows": [
    row("wait=true against a Data Passthrough child is stored as authored",
        {"stored_wait_true": 'wait="true"' in CO["pa_grp"]["processcall_attrs_stored"][0],
         "executed_green_grp_1": rec("cap184-passthrough-group", "grp-1")["parent_record"]["status"] == "COMPLETE"},
        "Readback of pa_grp: <processcall abort=\"true\" ... wait=\"true\">; grp-1 executed COMPLETE.",
        ["../cap184-passthrough-group/components/pa_grp.stored.xml"]),
    row("wait=false against a Data Passthrough child (docs: the Wait box 'cannot be turned off')",
        {"api_stored_wait_false": 'wait="false"' in CO["pa_wf"]["processcall_attrs_stored"][0],
         "parent_COMPLETE": r["parent_record"]["status"] == "COMPLETE",
         "log_asynchronous_launch_with_3_docs": f"Executing asynchronous process '{NAMEP}ch_grp_{N}' with 3 document(s)" in log,
         "parent_cleanup_waited_for_child": "Waited for 1 child process(es)" in log,
         "child_passthrough_received_3": "Executing Passthrough Shape with 3 passed in document(s)." in log,
         "mock_window_exactly_3_child_lines": len(m) == 3,
         "wire_bodies_wf_d1_d3": sorted(x["body"] for x in w) == [body_k(i, "wf-d") for i in (1, 2, 3)],
         "child_has_NO_own_ExecutionRecord": k.get("ch_grp") == [],
         "UI_rendering_observed": False},
        "The platform API STORED wait=\"false\" unchanged and EXECUTED it: the Process Call shape launched the child "
        "asynchronously with all 3 documents as one group (6 ms shape), the parent's cleanup logged 'Waited for 1 child "
        "process(es)' before the parent completed, the child ran inside the parent's log with no ExecutionRecord. "
        "NOT observed: what the UI shows/authors for this stored value (browser extension not connected). One run only.",
        ["components/pa_wf.stored.xml", "runs/wf-1/*"])]}

# ---------------------------------------------------------------- no data per document
cap = "cap184-nodata-per-document"
r, log, m, w, k = rec(cap, "nd-1"), txt(cap, "nd-1", "parent_process_log.txt"), mock(cap, "nd-1"), wire(cap, "nd-1"), kids(cap, "nd-1")
eid = r["execution_id"]
pf, pm = f"/__E0_184__/{N}/nd/child-first-step", f"/__E0_184__/{N}/nd/child-after-message"
kl = [txt(cap, "nd-1", f"child_{x['executionId']}_process_log.txt") for x in k["ch_nd"]]
V[cap] = {"rows": [row(
    "No Data child: one child execution per parent document, each starting with one EMPTY document",
    {"parent_COMPLETE": r["parent_record"]["status"] == "COMPLETE",
     "three_child_records": len(k["ch_nd"]) == 3,
     "all_sub_process_COMPLETE_top_is_parent": all(x["executionType"] == "sub_process" and x["status"] == "COMPLETE"
                                                  and x["topLevelExecutionId"] == eid for x in k["ch_nd"]),
     "each_child_inbound_1": all(x["inboundDocumentCount"] == 1 for x in k["ch_nd"]),
     "parent_log_standalone_start_data_3_invocations": "standalone start data, no return data" in log
     and log.count(f"Executing process '{NAMEP}ch_nd_{N}") == 3,
     "mock_3_first_step_3_after_message_only": sum(mline(pf, 0) in l for l in m) == 3
     and sum(mline(pm, 0) in l for l in m) == 3 and len(m) == 6,
     "first_step_bodies_all_empty": [x["body"] for x in w if pf in x["start_line"]] == ["", "", ""],
     "after_message_bodies_are_message": [x["body"] for x in w if pm in x["start_line"]]
     == [json.dumps({"k": f"{N}-nd-child-msg"})] * 3,
     "each_child_log_NoData_1_document": all("Start\tNoData\t1 document(s) found for processing." in t for t in kl)},
    "3 sub_process ExecutionRecords (topLevelExecutionId = parent, in=1 out=2 each); parent log 'standalone start "
    "data' with 3 'Executing process' lines; each child's first connector sent an EMPTY body (0 bytes) x3.",
    ["runs/nd-1/*"])]}

# ---------------------------------------------------------------- ddp handoff
cap = "cap184-passthrough-ddp-handoff"
rows = []
r, log, m, w, k = rec(cap, "ddp-pt-1"), txt(cap, "ddp-pt-1", "parent_process_log.txt"), mock(cap, "ddp-pt-1"), wire(cap, "ddp-pt-1"), kids(cap, "ddp-pt-1")
cp = f"/__E0_184__/{N}/ddp-pt/child/"
pairs = [(x["start_line"].split(" ")[1], x["body"]) for x in w if cp in x["start_line"]]
rows.append(row(
    "Data Passthrough child: each document carries its own parent-set DDP into the child (no Set Properties in child)",
    {"parent_COMPLETE": r["parent_record"]["status"] == "COMPLETE",
     "3_child_requests_each_path_tail_equals_its_body_k": len(pairs) == 3 and all(
         pt == cp + json.loads(b)["k"] for pt, b in pairs),
     "paths_are_d1_d2_d3": sorted(pt for pt, _ in pairs) == [cp + f"{N}-ddp-pt-d{i}" for i in (1, 2, 3)],
     "mock_has_the_3_paths": all(any(mline(cp + f"{N}-ddp-pt-d{i}", 0) in l for l in m) for i in (1, 2, 3)),
     "witness_present": any(f"/ddp-pt/parent-witness" in l for l in m),
     "child_no_own_record": k.get("ch_ddp_pt") == []},
    "Counterparty paths /ddp-pt/child/<k of the same document> x3; the child has no Set Properties shape.",
    ["runs/ddp-pt-1/*"]))
r, m, w, k = rec(cap, "ddp-nd-1"), mock(cap, "ddp-nd-1"), wire(cap, "ddp-nd-1"), kids(cap, "ddp-nd-1")
rows.append(row(
    "No Data child control: the parent-set DDP is ABSENT in the child (bound Path empty)",
    {"parent_COMPLETE": r["parent_record"]["status"] == "COMPLETE",
     "three_sub_process_child_records": len(k["ch_ddp_nd"]) == 3 and all(x["executionType"] == "sub_process" for x in k["ch_ddp_nd"]),
     "mock_3_POST_root_lines": sum('"POST / HTTP/1.1"' in l for l in m) == 3,
     "wire_3_POST_root_empty_bodies": [x["body"] for x in w if x["start_line"] == "POST / HTTP/1.1"] == ["", "", ""],
     "no_request_with_ddp_nd_child_prefix": not any("/ddp-nd/child/" in l for l in m),
     "witness_present_same_window": any("/ddp-nd/parent-witness" in l for l in m)},
    "3 child executions each issued 'POST /' with an empty body: the Path DDP is unset in a No Data child. "
    "Attribution of the nonce-less 'POST /' lines: the window holds only this execution (4 lines total incl. witness), "
    "3 sub_process records, and each child log shows its connector executing.",
    ["runs/ddp-nd-1/*"]))
r, m, w, k = rec(cap, "ddp-cache-1"), mock(cap, "ddp-cache-1"), wire(cap, "ddp-cache-1"), kids(cap, "ddp-cache-1")
cc = f"/__E0_184__/{N}/ddp-cache/child/"
pairs = [(x["start_line"].split(" ")[1], x["body"]) for x in w if cc in x["start_line"]]
rows.append(row(
    "Dispatch expectation 'child first step = document cache retrieve LOSES the handed-off DDP'",
    {"parent_COMPLETE": r["parent_record"]["status"] == "COMPLETE",
     "3_requests_path_tail_equals_body_k (DDP SURVIVED)": len(pairs) == 3 and all(pt == cc + json.loads(b)["k"] for pt, b in pairs),
     "bodies_are_the_CACHED_docs_c1_c3": sorted(b for _, b in pairs) == [body_k(i, "ddp-cache-c") for i in (1, 2, 3)],
     "no_POST_root": not any('"POST / HTTP/1.1"' in l for l in m),
     "witness_present": any("/ddp-cache/parent-witness" in l for l in m)},
    "REFUTED in this configuration: the retrieved (cached) documents c1..c3 reached the wire with their own "
    "parent-set DDP paths; nothing was lost.", ["runs/ddp-cache-1/*"], refutes=True))
r, m, w = rec(cap, "ddp-cache-inproc-1"), mock(cap, "ddp-cache-inproc-1"), wire(cap, "ddp-cache-inproc-1")
ci = f"/__E0_184__/{N}/ddp-inproc/parent-bound/"
pairs = [(x["start_line"].split(" ")[1], x["body"]) for x in w if ci in x["start_line"]]
rows.append(row(
    "ADDED control: DDP through Add-to-Cache -> retrieve-all within ONE process (no child), same cache/DDP source",
    {"parent_COMPLETE": r["parent_record"]["status"] == "COMPLETE",
     "3_requests_path_tail_equals_body_k (DDP SURVIVED)": len(pairs) == 3 and all(pt == ci + json.loads(b)["k"] for pt, b in pairs),
     "witness_present": any("/ddp-inproc/parent-witness" in l for l in m),
     "consistent_with_archived_155_r17_LOST_row": False},
    "Survives in-process too, so the child boundary is not what preserved it in ddp-cache-1. This CONTRADICTS the "
    "archived #155 r17 fact table (document_cache_retrieve -> LOST), measured on a different account with a linear "
    "load->retrieve wiring and a static+process-property DDP source. Survival is configuration-dependent, not a kind rule.",
    ["runs/ddp-cache-inproc-1/*", "components/pa_ddp_cache_inproc.*"]))
if f"{cap}/xr-ddp-cache-linear-1" in RS:
    r, m, w = rec(cap, "xr-ddp-cache-linear-1"), mock(cap, "xr-ddp-cache-linear-1"), wire(cap, "xr-ddp-cache-linear-1")
    cl = f"/__E0_184__/{N}/ddp-linear/bound/"
    bound = [(x["start_line"].split(" ")[1], x["body"]) for x in w if cl in x["start_line"]]
    root = [x["body"] for x in w if x["start_line"] == "POST / HTTP/1.1"]
    survived = bool(bound) and all(pt == cl + json.loads(b)["k"] for pt, b in bound)
    lin_log = txt(cap, "xr-ddp-cache-linear-1", "parent_process_log.txt")
    load_consumed = "Document Cache Load\t\tNo documents found. Skipping execution for the Document Cache Retrieve step." in lin_log
    outcome = ("survived" if survived and not root else ("lost" if root and not bound else
               ("NOT-TESTABLE: forward-connected doccacheload emitted ZERO documents, retrieve never ran"
                if load_consumed and not bound and not root else "mixed-or-no-request")))
    rows.append({"row": "ADDED control: #155 r17 WIRING (doccacheload forward-connected to doccacheretrieve) with the E0 "
                        "DDP source — does the DDP survive?",
                 "verdict": "OPEN",
                 "checks": {"parent_COMPLETE": r["parent_record"]["status"] == "COMPLETE",
                            "witness_present": any("/ddp-linear/parent-witness" in l for l in m)},
                 "observed_outcome": outcome, "bound_requests": bound, "root_requests_bodies": root,
                 "observation": f"outcome={outcome}: {len(bound)} requests on the DDP-bound path, {len(root)} 'POST /'. "
                                "Recorded to discriminate wiring from the account roll / DDP source; the lineage rule "
                                "stays OPEN either way (a general cache-retrieve property rule is not attested).",
                 "evidence": ["runs/xr-ddp-cache-linear-1/*", "components/xr_ddp_cache_linear.*"]})
V[cap] = {"rows": rows}

# ---------------------------------------------------------------- shared cache
cap = "cap184-shared-cache"
r, log, m, w, k = rec(cap, "cache-1"), txt(cap, "cache-1", "parent_process_log.txt"), mock(cap, "cache-1"), wire(cap, "cache-1"), kids(cap, "cache-1")
cpt, cnd = f"/__E0_184__/{N}/cache/child-pt", f"/__E0_184__/{N}/cache/child-nd"
cached = [body_k(i, "cache-c") for i in (1, 2, 3)]
sa = {rid: (rec(cap, rid), mock(cap, rid), txt(cap, rid, "parent_process_log.txt")) for rid in ("cache-sa-pt", "cache-sa-nd", "cache-sa-control")}
V[cap] = {"rows": [
    row("Data Passthrough child retrieves the parent's document-cache contents",
        {"parent_COMPLETE": r["parent_record"]["status"] == "COMPLETE",
         "child_pt_bodies_are_cached_c1_c3": sorted(x["body"] for x in w if x["start_line"] == f"POST {cpt} HTTP/1.1") == cached,
         "not_the_passed_in_callpt_doc": not any("cache-callpt" in x["body"] for x in w),
         "child_pt_no_own_record": k.get("ch_cache_pt") == []},
        "Child passthrough (passed 1 doc 'callpt') -> retrieve-all -> 3 requests with the parent's cached c1..c3.",
        ["runs/cache-1/*"]),
    row("No Data child (own sub_process execution) retrieves the parent's document-cache contents",
        {"child_nd_bodies_are_cached_c1_c3": sorted(x["body"] for x in w if x["start_line"] == f"POST {cnd} HTTP/1.1") == cached,
         "one_sub_process_record_in1_out3": len(k["ch_cache_nd"]) == 1 and k["ch_cache_nd"][0]["executionType"] == "sub_process"
         and k["ch_cache_nd"][0]["inboundDocumentCount"] == 1 and k["ch_cache_nd"][0]["outboundDocumentCount"] == 3,
         "witness_present": any("/cache/parent-witness" in l for l in m)},
        "Child sub_process execution retrieved c1..c3 from the parent's cache.", ["runs/cache-1/*"]),
    row("No carry-over: each child run on its own finds an empty cache",
        {"sa_pt_COMPLETE_no_lines": sa["cache-sa-pt"][0]["parent_record"]["status"] == "COMPLETE" and sa["cache-sa-pt"][1] == [],
         "sa_nd_COMPLETE_no_lines": sa["cache-sa-nd"][0]["parent_record"]["status"] == "COMPLETE" and sa["cache-sa-nd"][1] == [],
         "both_logs_No_documents_found": all("No documents found. Skipping execution for the Connector step." in sa[x][2]
                                             for x in ("cache-sa-pt", "cache-sa-nd")),
         "adjacent_control_run_line_present": any(f"/__E0_184__/{N}/grp/child" in l for l in sa["cache-sa-control"][1])},
        "Standalone runs: 'No documents found. Skipping execution for the Connector step.', zero counterparty lines; "
        "the control run executed right after posted its line (channel live).",
        ["runs/cache-sa-pt/*", "runs/cache-sa-nd/*", "runs/cache-sa-control/*"])]}

# ---------------------------------------------------------------- dpp both ways
cap = "cap184-dpp-both-ways"
rows = []
for rid, tag, child in (("dpp-pt-1", "dpp-pt", "ch_dpp_pt"), ("dpp-nd-1", "dpp-nd", "ch_dpp_nd")):
    r, m, k = rec(cap, rid), mock(cap, rid), kids(cap, rid)
    inp = f"/__E0_184__/{N}/{tag}/child-read-in/{N}-{tag}-parent-set-in"
    outp = f"/__E0_184__/{N}/{tag}/parent-read-out/{N}-{tag}-child-set-out"
    ch = {"parent_COMPLETE": r["parent_record"]["status"] == "COMPLETE",
          "child_read_parent_DPP": sum(mline(inp, 0) in l for l in m) == 1,
          "parent_read_child_DPP_after_call": sum(mline(outp, 0) in l for l in m) == 1,
          "window_exactly_2_lines": len(m) == 2}
    if child == "ch_dpp_nd":
        ch["one_sub_process_record"] = len(k[child]) == 1 and k[child][0]["executionType"] == "sub_process"
    else:
        ch["child_no_own_record"] = k[child] == []
    rows.append(row(f"DPP both ways with a {'Data Passthrough' if tag == 'dpp-pt' else 'No Data'} child (wait=true)", ch,
                    "Child's path carried the parent-set DPP; the parent's post-call leg carried the child-set DPP.",
                    [f"runs/{rid}/*"]))
V[cap] = {"rows": rows}

# ---------------------------------------------------------------- standalone
cap = "cap184-passthrough-standalone"
r, log, m, w = rec(cap, "sa-1"), txt(cap, "sa-1", "parent_process_log.txt"), mock(cap, "sa-1"), wire(cap, "sa-1")
V[cap] = {"rows": [row(
    "Data Passthrough process executed directly runs as No Data (one empty document)",
    {"COMPLETE_exec_manual": r["parent_record"]["status"] == "COMPLETE" and r["parent_record"]["executionType"] == "exec_manual",
     "start_logged_NoData_1_document": "Start\tNoData\t1 document(s) found for processing." in log,
     "no_Passthrough_shape_line": "Passthrough" not in log,
     "one_request_empty_body": [x["body"] for x in w] == [""] and len(m) == 1},
    "Start logged 'NoData ... 1 document(s) found'; the first step's request body was empty (0 bytes).",
    ["runs/sa-1/*"])]}

# ---------------------------------------------------------------- prefix predecessors
cap = "cap184-prefix-predecessors"
rows = []
for role in sorted(x for x in CO if x.startswith("pfx_")):
    rid = f"{role}-1"
    kind, ctx = role[4:].rsplit("_", 1)
    key = f"{cap}/{rid}"
    if key not in RS:
        rows.append({"row": f"{kind} -> terminal processcall in a {'Branch leg' if ctx == 'br' else 'Decision TRUE arm'}",
                     "verdict": "OPEN", "checks": {"executed": False}, "observation": "not executed", "evidence": []})
        continue
    r, log, m, w = rec(cap, rid), txt(cap, rid, "parent_process_log.txt"), mock(cap, rid), wire(cap, rid)
    tok = f"{N}-pfx-{kind}-{ctx}" + ("-msgkind" if kind == "message" else "")
    pp = f"/__E0_184__/{N}/pfx/child"
    bodies = [x["body"] for x in w if x["start_line"] == f"POST {pp} HTTP/1.1"]
    if kind in ("cache_remove", "cache_remove_primed"):
        ch = {"stored_shapes_equal_modulo_entity_escaping": store[role]["stored_shapes_equal_modulo_entity_escaping"],
              "parent_COMPLETE": r["parent_record"]["status"] == "COMPLETE",
              "parent_outbound_0": r["parent_record"]["outboundDocumentCount"] == 0,
              "remove_logged_zero_docs_and_skipped_process_call":
                  "No documents found. Skipping execution for the Process Call step." in log,
              "process_call_shape_never_executed": "Executing Process Call Shape" not in log,
              "zero_child_requests_in_window": bodies == [] and not any(pp in l for l in m),
              "same_child_channel_live_in_retrieve_rows": all(
                  any(pp in l for l in mock(cap, f"pfx_cache_retrieve_{c}-1")) for c in ("br", "dec"))}
        primed = "primed" in kind
        rows.append(row(
            f"cache_remove ({'cache PRIMED by an earlier Branch leg' if primed else 'cache empty'}) wired directly into a "
            f"terminal Process Call in a {'Branch leg' if ctx == 'br' else 'Decision TRUE arm'}: documents reach the call",
            ch, "REFUTED: 'Executing Remove from Cache Shape with 1 document(s)' -> 'Successfully Removed Documents from "
                "Cache' -> 'No documents found. Skipping execution for the Process Call step.'; parent COMPLETE with "
                "outbound 0; no counterparty request. The process reads green while the call never runs.",
            [f"runs/{rid}/*", f"components/{role}.*"], refutes=True))
        continue
    ch = {"stored_shapes_equal_modulo_entity_escaping": store[role]["stored_shapes_equal_modulo_entity_escaping"],
          "stored_processcall_has_no_outgoing": f'processId="{CO["ch_pfx"]["id"]}"' in CO[role]["processcall_attrs_stored"][0],
          "parent_COMPLETE": r["parent_record"]["status"] == "COMPLETE",
          "process_call_executed_with_1_doc": "Executing Process Call Shape with 1 document(s)." in log,
          "child_request_exactly_1": len(bodies) == 1 and sum(pp in l for l in m) == 1,
          "child_body_carries_this_parent_token": len(bodies) == 1 and tok in bodies[0]}
    rows.append(row(f"{kind} wired directly into a terminal Process Call in a {'Branch leg' if ctx == 'br' else 'Decision TRUE arm'}",
                    ch, f"body={bodies!r}; UI rendering not observed (browser extension not connected)",
                    [f"runs/{rid}/*", f"components/{role}.*"]))
if f"{cap}/xr-remove-successor-1" in RS:
    rid = "xr-remove-successor-1"
    r, log, m = rec(cap, rid), txt(cap, rid, "parent_process_log.txt"), mock(cap, rid)
    sp = f"/__E0_184__/{N}/remove-successor/connector"
    ran = any(sp in l for l in m)
    ch = {"parent_COMPLETE": r["parent_record"]["status"] == "COMPLETE",
          "witness_present_same_window": any("/remove-successor/parent-witness" in l for l in m)}
    if ran:
        ch["successor_connector_request_present"] = True
        rows.append(row("ADDED control: a CONNECTOR successor of doccacheremove executes", ch,
                        "The connector after Remove from Cache issued its request.", [f"runs/{rid}/*"]))
    else:
        ch.update({"remove_logged_zero_docs_and_skipped_successor (message names the step by its userlabel)":
                   "No documents found. Skipping execution for the successor of remove step." in log,
                   "successor_connector_never_executed": f"{NAMEP}op_xr_rem_succ_{N}" not in log,
                   "zero_successor_requests": True})
        rows.append(row("ADDED control: a CONNECTOR successor of doccacheremove executes (the shipped legacy builder and "
                        "the ProcessIR lowering both emit doccacheremove as a linear step with a successor)", ch,
                        "REFUTED: Remove from Cache emitted zero documents and the platform skipped the connector; the "
                        "parent reads COMPLETE while the step after the remove never runs.", [f"runs/{rid}/*",
                        "components/xr_remove_successor.*"], refutes=True))
V[cap] = {"rows": rows}

V["_stored_vs_submitted"] = store
V["_record_query_control"] = {"checks": ctl_ok, "file": ctl_file}
L.save_state("verdicts", V)
for c, v in V.items():
    if c.startswith("_"):
        continue
    for x in v["rows"]:
        bad = [k for k, ok in x["checks"].items() if not ok]
        print(f"{x['verdict']:6} {c:34} {x['row'][:90]}" + (f"  FAILED:{bad}" if bad else ""))
print("stored!=submitted (modulo escaping):", [r for r, s in store.items() if not s["stored_shapes_equal_modulo_entity_escaping"]])
print("options differ:", {r: (s["options_submitted"], s["options_stored"]) for r, s in store.items() if s["options_submitted"] != s["options_stored"]})
