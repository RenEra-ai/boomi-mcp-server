"""E0 #184 ADDED CONTROLS (not in the dispatch), provisioned + deployed (resumable):

xr_remove_successor : start -> message -> [Branch leg1] doccacheremove -> CONNECTOR (static path) -> stop
                                           [Branch leg2] witness connector -> stop
    Does a step AFTER Remove from Cache execute? The prefix rows measured only a terminal Process Call
    successor ("No documents found. Skipping execution for the Process Call step."). The shipped legacy
    builder and the ProcessIR lowering both emit doccacheremove as a LINEAR step with a successor.

xr_ddp_cache_linear : start -> [Branch leg1] fan-out -> Set Properties DDP -> doccacheload -> doccacheretrieve
                                             -> connector(Path = DDP) -> stop
                             [Branch leg2] witness connector -> stop
    #155 r17 (LOST) wired load DIRECTLY into retrieve on one path; the E0 survival captures used separate
    Branch legs. Same cache, same DDP source (static + profile element k) as ddp-cache-1/inproc.
"""
import os, sys, json, re
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import lib184 as L
import g184 as G

PS = L.load_state("provision")
CO = PS["components"]
N = PS["nonce"]
DS = L.load_state("deploy")
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


PROV_OP = ("REST operation raw XML: the #156 oracle p1 shape (live-captured from _QA_FIXTURE_155_G1_src_GET), "
           "verb POST, returnApplicationErrors=true")


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
        rec.update({"shapes_submitted_sha256": L.sha(G.shapes_of(xml)),
                    "shapes_stored_sha256": L.sha(G.shapes_of(stored_raw)),
                    "shapes_stored_equal_submitted": G.shapes_of(xml) == G.shapes_of(stored_raw),
                    "process_options_submitted": G.process_options_of(xml),
                    "process_options_stored": G.process_options_of(stored_raw),
                    "processcall_attrs_stored": re.findall(r"<processcall [^>]*>", stored_raw),
                    "start_config_stored": re.findall(r'shapetype="start"[^>]*><configuration>(<[a-z]+/>)', stored_raw)})
    for cap in caps:
        rec.setdefault("archived", {})[cap] = [
            L.archive(cap, f"components/{role}.submitted.xml", xml),
            L.archive(cap, f"components/{role}.stored.xml", stored)]
    CO[role] = rec
    L.save_state("provision", PS)
    print(f"  {role:28} {cid} v{rec['stored_version']}")
    return cid


def deploy(role, cid):
    if DS["deployments"].get(role, {}).get("deployment_id"):
        return
    res = L.package_and_deploy(cid, f"#184 E0 {role} {N}")
    DS["deployments"][role] = {"role": role, "component_id": cid, "package_id": res["package_id"],
                               "deployment_id": res["deployment_id"], "utc": L.utc(),
                               "package_success": (res["package"] or {}).get("_success"),
                               "deploy_success": (res["deploy"] or {}).get("_success")}
    L.save_state("deploy", DS)
    print("  deploy:", json.dumps(DS["deployments"][role]))


CACHE_ID = CO["cache"]["id"]
ops = {}
for role, p in (("op_xr_rem_succ", path("remove-successor", "connector")),
                ("op_w_xr_rem", path("remove-successor", "parent-witness")),
                ("op_w_xr_lin", path("ddp-linear", "parent-witness"))):
    ops[role] = ensure(role, [FIX], L.comp_xml(nm(role), "connector-action", rest_op_obj(p), L.REST_SUB),
                       PROV_OP + f"; static path {p!r}", "connector-action")

# --- remove -> connector successor
cap = "cap184-prefix-predecessors"
role = "xr_remove_successor"
g = G.Graph(nm(role))
g.start("shape1", 1, 0, "shape2")
g.message("shape2", 2, 0, json.dumps({"k": f"{N}-xr-remove-successor"}), "shape3")
g.branch("shape3", 3, 0, ["shape4", "shape7"])
g.cache_remove("shape4", 4, 0, CACHE_ID, "shape5")
g.connector("shape5", 5, 0, ops["op_xr_rem_succ"], "shape6", label="successor of remove")
g.stop("shape6", 6, 0)
g.connector("shape7", 4, 1, ops["op_w_xr_rem"], "shape8", label="witness")
g.stop("shape8", 5, 1)
cid = ensure(role, [cap], g.xml(), sorted(g.prov) + ["ADDED control: connector successor of doccacheremove"], "process")
deploy(role, cid)

# --- linear load -> retrieve with a DDP-bound connector
cap = "cap184-passthrough-ddp-handoff"
role = "xr_ddp_cache_linear"
prof = G.src_profile(CO["profile"]["id"], PS["profile_k_key"], "k (E0Doc/Object/k)")
g = G.Graph(nm(role))
g.start("shape1", 1, 0, "shape2")
g.branch("shape2", 2, 0, ["shape3", "shape9"])
g.groovy("shape3", 3, 0, G.fanout_script(f"{N}-ddp-linear-c"), "shape4", label="fan-out 3 docs")
g.setprops("shape4", 4, 0, [("ddp", "DDP_E0PATH", [G.src_static(path("ddp-linear", "bound") + "/"), prof])], "shape5")
g.cache_load("shape5", 5, 0, CACHE_ID, "shape6")
g.cache_retrieve("shape6", 6, 0, CACHE_ID, "shape7")
g.connector("shape7", 7, 0, CO["op_blank"]["id"], "shape8", ddp="DDP_E0PATH")
g.stop("shape8", 8, 0)
g.connector("shape9", 3, 1, ops["op_w_xr_lin"], "shape10", label="witness")
g.stop("shape10", 4, 1)
cid = ensure(role, [cap], g.xml(), sorted(g.prov) + [
    "ADDED control: #155 r17 wiring (doccacheload forward-connected to doccacheretrieve) with the E0 DDP source; "
    "Groovy fan-out script hand-authored"], "process")
deploy(role, cid)
