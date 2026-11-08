"""E0 #184 ADDED CONTROL (not in the dispatch): cache remove as a direct Process Call predecessor
with a PRIMED cache.

pfx_cache_remove_{br,dec}-1 removed from an EMPTY cache and emitted zero documents ("No documents
found. Skipping execution for the Process Call step."). These parents prime the same cache in an
earlier Branch leg (the cache_retrieve rows' wiring), then remove-all -> terminal processcall, so
the row separates "remove consumes the inbound documents" from "remove outputs what it removed".
Provisions, archives, and deploys (resumable).
"""
import os, sys, json, re
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import lib184 as L
import g184 as G

PS = L.load_state("provision")
CO = PS["components"]
N = PS["nonce"]
DS = L.load_state("deploy")
CAP = "cap184-prefix-predecessors"


def nm(r):
    return f"_TEST_184E0_{r}_{N}"


def ensure(role, caps, xml, provenance):
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
    rec = {"role": role, "id": cid, "name": nm(role), "kind": "process", "captures": caps, "provenance": provenance,
           "created_utc": L.utc(), "stored_version": ver.group(1) if ver else None,
           "submitted_sha256": L.sha(xml), "stored_redacted_sha256": L.sha(stored), "added_control": True,
           "shapes_submitted_sha256": L.sha(G.shapes_of(xml)), "shapes_stored_sha256": L.sha(G.shapes_of(stored_raw)),
           "shapes_stored_equal_submitted": G.shapes_of(xml) == G.shapes_of(stored_raw),
           "process_options_submitted": G.process_options_of(xml),
           "process_options_stored": G.process_options_of(stored_raw),
           "processcall_attrs_stored": re.findall(r"<processcall [^>]*>", stored_raw),
           "start_config_stored": re.findall(r'shapetype="start"[^>]*><configuration>(<[a-z]+/>)', stored_raw)}
    for cap in caps:
        rec.setdefault("archived", {})[cap] = [
            L.archive(cap, f"components/{role}.submitted.xml", xml),
            L.archive(cap, f"components/{role}.stored.xml", stored)]
    CO[role] = rec
    L.save_state("provision", PS)
    print(f"  {role:30} {cid} v{rec['stored_version']}")
    return cid


CACHE_ID = CO["cache"]["id"]
CH_PFX = CO["ch_pfx"]["id"]
for ctx in ("br", "dec"):
    role = f"pfx_cache_remove_primed_{ctx}"
    tok = f"{N}-pfx-cache_remove_primed-{ctx}"
    g = G.Graph(nm(role))
    g.start("shape1", 1, 0, "shape2")
    g.message("shape2", 2, 0, json.dumps({"k": tok}), "shape3")
    g.branch("shape3", 3, 0, ["shape4", "shape5"])
    g.cache_load("shape4", 4, 0, CACHE_ID)
    if ctx == "br":
        g.cache_remove("shape5", 4, 1, CACHE_ID, "shape6")
        g.processcall("shape6", 5, 1, CH_PFX)
    else:
        g.decision_true("shape5", 4, 1, "shape6", "shape8")
        g.cache_remove("shape6", 5, 1, CACHE_ID, "shape7")
        g.processcall("shape7", 6, 1, CH_PFX)
        g.stop("shape8", 5, 2)
    cid = ensure(role, [CAP], g.xml(), sorted(g.prov) + [
        "ADDED control: cache primed by an earlier Branch leg, remove-all wired directly into a terminal processcall"])
    if not DS["deployments"].get(role, {}).get("deployment_id"):
        res = L.package_and_deploy(cid, f"#184 E0 {role} {N}")
        DS["deployments"][role] = {"role": role, "component_id": cid, "package_id": res["package_id"],
                                   "deployment_id": res["deployment_id"], "utc": L.utc(),
                                   "package_success": (res["package"] or {}).get("_success"),
                                   "deploy_success": (res["deploy"] or {}).get("_success")}
        L.save_state("deploy", DS)
        print("  deploy:", json.dumps(DS["deployments"][role]))
