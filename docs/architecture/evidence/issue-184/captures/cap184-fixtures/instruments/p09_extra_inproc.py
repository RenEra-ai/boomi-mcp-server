"""E0 #184 ADDED CONTROL (not in the dispatch): in-process DDP survival through a document cache.

ddp-cache-1 showed a parent-set DDP surviving Add-to-Cache -> (Process Call) -> child cache
retrieve -> bound connector, contradicting the dispatch's "expect loss" and #155 r17's
in-process LOST row. This parent reproduces the SAME cache write (Branch leg: fan-out ->
Set Properties DDP from profile element k -> Add to Cache) but retrieves in a LATER leg of
the SAME process and binds the connector Path to the DDP. It separates "the child boundary"
from "document cache retrieval" as the cause. Provisions, archives, and deploys (resumable).
"""
import os, sys, json, re
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import lib184 as L
import g184 as G

PS = L.load_state("provision")
CO = PS["components"]
N = PS["nonce"]
DS = L.load_state("deploy")
CAP = "cap184-passthrough-ddp-handoff"
FIX = "cap184-fixtures"


def nm(r):
    return f"_TEST_184E0_{r}_{N}"


def path(cap, r):
    return f"/__E0_184__/{N}/{cap}/{r}"


def rest_op_obj(p, verb="POST"):
    return (f'<Operation xmlns="" returnApplicationErrors="true" trackResponse="true">'
            '<Archiving directory="" enabled="false"/><Configuration>'
            f'<GenericOperationConfig customOperationType="{verb}" operationType="EXECUTE">'
            '<field id="followRedirects" type="string" value="NONE"/>'
            f'<field id="path" type="string" value="{p}"/>'
            '<field id="queryParameters" type="customproperties"><customProperties/></field>'
            '<field id="requestHeaders" type="customproperties"><customProperties/></field>'
            '<Options/></GenericOperationConfig></Configuration>'
            '<Tracking><TrackedFields/></Tracking><Caching/></Operation>')


def ensure(role, caps, xml, provenance, kind):
    if CO.get(role, {}).get("id"):
        return CO[role]["id"]
    r = L.call("manage_component", profile=L.P, action="create", config=json.dumps({"xml": xml}))
    comp = r.get("component") or {}
    cid = r.get("component_id") or comp.get("component_id") or comp.get("id")
    if not cid:
        print(json.dumps(r, indent=1, default=str)[:2500])
        raise SystemExit(f"create failed: {role}")
    stored_raw = L.component_xml(cid)
    stored = L.redact(stored_raw)
    ver = re.search(r'\bversion="(\d+)"', stored_raw)
    rec = {"role": role, "id": cid, "name": nm(role), "kind": kind, "captures": caps, "provenance": provenance,
           "created_utc": L.utc(), "stored_version": ver.group(1) if ver else None,
           "submitted_sha256": L.sha(xml), "stored_redacted_sha256": L.sha(stored), "added_control": True}
    if kind == "process":
        rec["shapes_submitted_sha256"] = L.sha(G.shapes_of(xml))
        rec["shapes_stored_sha256"] = L.sha(G.shapes_of(stored_raw))
        rec["shapes_stored_equal_submitted"] = G.shapes_of(xml) == G.shapes_of(stored_raw)
        rec["process_options_submitted"] = G.process_options_of(xml)
        rec["process_options_stored"] = G.process_options_of(stored_raw)
        rec["processcall_attrs_stored"] = re.findall(r"<processcall [^>]*>", stored_raw)
        rec["start_config_stored"] = re.findall(r'shapetype="start"[^>]*><configuration>(<[a-z]+/>)', stored_raw)
    for cap in caps:
        rec.setdefault("archived", {})[cap] = [
            L.archive(cap, f"components/{role}.submitted.xml", xml),
            L.archive(cap, f"components/{role}.stored.xml", stored)]
    CO[role] = rec
    L.save_state("provision", PS)
    print(f"  {role:24} {cid} v{rec['stored_version']}")
    return cid


w = ensure("op_w_ddpinproc", [FIX], L.comp_xml(nm("op_w_ddpinproc"), "connector-action",
                                               rest_op_obj(path("ddp-inproc", "parent-witness")), L.REST_SUB),
           "REST operation raw XML: the #156 oracle p1 shape (live-captured from _QA_FIXTURE_155_G1_src_GET), "
           f"verb POST, returnApplicationErrors=true; static path {path('ddp-inproc', 'parent-witness')!r}",
           "connector-action")

role = "pa_ddp_cache_inproc"
g = G.Graph(nm(role))
prof = G.src_profile(CO["profile"]["id"], PS["profile_k_key"], "k (E0Doc/Object/k)")
g.start("shape1", 1, 0, "shape2")
g.branch("shape2", 2, 0, ["shape3", "shape6", "shape9"])
g.groovy("shape3", 3, 0, G.fanout_script(f"{N}-ddp-inproc-c"), "shape4", label="fan-out 3 cache docs")
g.setprops("shape4", 4, 0, [("ddp", "DDP_E0PATH", [G.src_static(path("ddp-inproc", "parent-bound") + "/"), prof])],
           "shape5")
g.cache_load("shape5", 5, 0, CO["cache"]["id"])
g.cache_retrieve("shape6", 3, 1, CO["cache"]["id"], "shape7")
g.connector("shape7", 4, 1, CO["op_blank"]["id"], "shape8", ddp="DDP_E0PATH")
g.stop("shape8", 5, 1)
g.connector("shape9", 3, 2, w, "shape10", label="witness")
g.stop("shape10", 4, 2)
cid = ensure(role, [CAP], g.xml(), sorted(g.prov) + ["ADDED in-process control for ddp-cache-1; Groovy fan-out script hand-authored"],
             "process")

if not DS["deployments"].get(role, {}).get("deployment_id"):
    res = L.package_and_deploy(cid, f"#184 E0 {role} {N}")
    DS["deployments"][role] = {"role": role, "component_id": cid, "package_id": res["package_id"],
                               "deployment_id": res["deployment_id"], "utc": L.utc(),
                               "package_success": (res["package"] or {}).get("_success"),
                               "deploy_success": (res["deploy"] or {}).get("_success")}
    L.save_state("deploy", DS)
    print("deploy:", json.dumps(DS["deployments"][role]))
