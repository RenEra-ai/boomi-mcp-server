"""E0 #184 provisioning (resumable): REST ops, JSON profile, document cache, map,
children, parents. Archives submitted + stored (redacted) XML per capture.

Provenance per component is recorded in state/provision.json and in each
capture's components/ directory. Nothing is produced by #184 code.
"""
import os, sys, json, time, re
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import lib184 as L
import g184 as G
from boomi_mcp.categories.components.builders.json_profile_builder import JSONGeneratedProfileBuilder
from boomi_mcp.categories.components.builders.document_cache_builder import DocumentCacheBuilder
from boomi_mcp.categories.components.builders.map_builder import DirectMapBuilder

ST = L.load_state("provision") or {"components": {}}
if "nonce" not in ST:
    ST["nonce"] = "E0184" + time.strftime("%m%d%H%M%S", time.gmtime())
    ST["created_utc"] = L.utc()
    ST["tree_at_start"] = L.tree_stamp()
    ST["account_id"] = L.ACCOUNT
N = ST["nonce"]
CO = ST["components"]
L.save_state("provision", ST)
print("NONCE", N)

FIX = "cap184-fixtures"
GRP = "cap184-passthrough-group"
WAIT = "cap184-passthrough-wait-true"
ND = "cap184-nodata-per-document"
DDP = "cap184-passthrough-ddp-handoff"
CACHE = "cap184-shared-cache"
DPP = "cap184-dpp-both-ways"
SA = "cap184-passthrough-standalone"
PFX = "cap184-prefix-predecessors"

PROV_OP = ("REST operation raw XML: the #156 oracle p1 shape (live-captured from _QA_FIXTURE_155_G1_src_GET), "
           "verb POST, returnApplicationErrors=true")
PROV_PROFILE = "JSONGeneratedProfileBuilder.build at branch point cbab28f (legacy builder)"
PROV_CACHE = "DocumentCacheBuilder.build at branch point cbab28f (legacy builder), enforceSingleLucene=true"
PROV_MAP = "DirectMapBuilder.build at branch point cbab28f (legacy builder)"


def nm(role):
    return f"_TEST_184E0_{role}_{N}"


def path(cap, role):
    return f"/__E0_184__/{N}/{cap}/{role}"


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
           "submitted_sha256": L.sha(xml), "stored_redacted_sha256": L.sha(stored)}
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
    L.save_state("provision", ST)
    extra = "" if kind != "process" else f" shapes_equal={rec['shapes_stored_equal_submitted']} opts_equal={rec['process_options_submitted'] == rec['process_options_stored']}"
    print(f"  {role:24} {cid} v{rec['stored_version']}{extra}")
    return cid


# ------------------------------------------------------------------ fixtures
OPS = {
    "op_grp_child": path("grp", "child"),
    "op_nd_first": path("nd", "child-first-step"),
    "op_nd_msg": path("nd", "child-after-message"),
    "op_blank": "",
    "op_w_ddppt": path("ddp-pt", "parent-witness"),
    "op_w_ddpnd": path("ddp-nd", "parent-witness"),
    "op_w_ddpc": path("ddp-cache", "parent-witness"),
    "op_w_cache": path("cache", "parent-witness"),
    "op_cache_pt": path("cache", "child-pt"),
    "op_cache_nd": path("cache", "child-nd"),
    "op_pfx_child": path("pfx", "child"),
}
op = {}
for role, p in OPS.items():
    op[role] = ensure(role, [FIX], L.comp_xml(nm(role), "connector-action", rest_op_obj(p), L.REST_SUB),
                      PROV_OP + f"; static path {p!r}", "connector-action")

pcfg = {"component_name": nm("profile"), "profile_type": "json.generated",
        "root": {"name": "E0Doc", "kind": "object",
                 "children": [{"name": "k", "kind": "simple", "data_type": "character"}]}}
pb = JSONGeneratedProfileBuilder()
idx = pb.build_field_index(pcfg)
K_KEY = idx["E0Doc/k"]["key"]
K_NAME = "k (E0Doc/Object/k)"
PROFILE = ensure("profile", [FIX], pb.build(**pcfg), PROV_PROFILE, "profile.json")
ST["profile_k_key"] = K_KEY

ccfg = {"component_name": nm("cache"), "profile_type": "profile.json", "profile_id": PROFILE,
        "enforce_single_lucene": True,
        "indexes": [{"index_id": 1, "index_name": "by k", "keys": [{"id": 1, "name": "k", "element_key": str(K_KEY)}]}]}
CACHE_ID = ensure("cache", [FIX], DocumentCacheBuilder().build(**ccfg), PROV_CACHE, "documentcache")

mcfg = {"component_name": nm("map"), "map_type": "direct", "source_profile_id": PROFILE,
        "target_profile_id": PROFILE, "source_profile_type": "profile.json", "target_profile_type": "profile.json", "field_mappings": [{"source_path": "E0Doc/k", "target_path": "E0Doc/k"}]}
MAP_ID = ensure("map", [FIX], DirectMapBuilder().build(source_index=idx, target_index=idx, **mcfg), PROV_MAP,
                "transform.map")


def proc(role, caps, g, note=""):
    x = g.xml()
    prov = sorted(g.prov)
    if note:
        prov.append(note)
    return ensure(role, caps, x, prov, "process")


def prof_src():
    return G.src_profile(PROFILE, K_KEY, K_NAME)


# ------------------------------------------------------------------ children
def simple_child(role, pt, op_id):
    g = G.Graph(nm(role), passthrough=pt)
    g.start("shape1", 1, 0, "shape2")
    g.connector("shape2", 2, 0, op_id, "shape3")
    g.stop("shape3", 3, 0)
    return g


CH = {}
CH["ch_grp"] = proc("ch_grp", [GRP, WAIT, SA], simple_child("ch_grp", True, op["op_grp_child"]))

g = G.Graph(nm("ch_nd"))
g.start("shape1", 1, 0, "shape2")
g.connector("shape2", 2, 0, op["op_nd_first"], "shape3")
g.message("shape3", 3, 0, json.dumps({"k": f"{N}-nd-child-msg"}), "shape4")
g.connector("shape4", 4, 0, op["op_nd_msg"], "shape5")
g.stop("shape5", 5, 0)
CH["ch_nd"] = proc("ch_nd", [ND], g)

for role, pt in (("ch_ddp_pt", True), ("ch_ddp_nd", False)):
    g = G.Graph(nm(role), passthrough=pt)
    g.start("shape1", 1, 0, "shape2")
    g.connector("shape2", 2, 0, op["op_blank"], "shape3", ddp="DDP_E0PATH")
    g.stop("shape3", 3, 0)
    CH[role] = proc(role, [DDP], g, "connector Path=track DDP via rendering.render_connectoraction(dynamic_path) "
                                    "(legacy #100 G2; live-attested cap155-e1-source-dynamic-path)")

g = G.Graph(nm("ch_ddp_cache"), passthrough=True)
g.start("shape1", 1, 0, "shape2")
g.cache_retrieve("shape2", 2, 0, CACHE_ID, "shape3")
g.connector("shape3", 3, 0, op["op_blank"], "shape4", ddp="DDP_E0PATH")
g.stop("shape4", 4, 0)
CH["ch_ddp_cache"] = proc("ch_ddp_cache", [DDP], g)

for role, pt, o in (("ch_cache_pt", True, "op_cache_pt"), ("ch_cache_nd", False, "op_cache_nd")):
    g = G.Graph(nm(role), passthrough=pt)
    g.start("shape1", 1, 0, "shape2")
    g.cache_retrieve("shape2", 2, 0, CACHE_ID, "shape3")
    g.connector("shape3", 3, 0, op[o], "shape4")
    g.stop("shape4", 4, 0)
    CH[role] = proc(role, [CACHE], g)

for role, pt, tag in (("ch_dpp_pt", True, "dpp-pt"), ("ch_dpp_nd", False, "dpp-nd")):
    g = G.Graph(nm(role), passthrough=pt)
    g.start("shape1", 1, 0, "shape2")
    g.setprops("shape2", 2, 0, [("ddp", "DDP_E0PATH", [G.src_static(path(tag, "child-read-in") + "/"),
                                                        G.src_dpp("DPP_E0IN")])], "shape3")
    g.connector("shape3", 3, 0, op["op_blank"], "shape4", ddp="DDP_E0PATH")
    g.setprops("shape4", 4, 0, [("dpp", "DPP_E0OUT", [G.src_static(f"{N}-{tag}-child-set-out")])], "shape5")
    g.stop("shape5", 5, 0)
    CH[role] = proc(role, [DPP], g)

CH["ch_pfx"] = proc("ch_pfx", [PFX], simple_child("ch_pfx", True, op["op_pfx_child"]))

# ------------------------------------------------------------------ parents
for role, tag, wait in (("pa_grp", "grp", True), ("pa_wf", "wf", False)):
    g = G.Graph(nm(role))
    g.start("shape1", 1, 0, "shape2")
    g.groovy("shape2", 2, 0, G.fanout_script(f"{N}-{tag}-d"), "shape3", label="fan-out 3 docs")
    g.processcall("shape3", 3, 0, CH["ch_grp"], wait=wait, abort=True)
    proc(role, [GRP, WAIT] if role == "pa_grp" else [WAIT], g,
         "Groovy fan-out script hand-authored; wait=false set via render_processcall(wait=False)" if not wait
         else "Groovy fan-out script hand-authored")

g = G.Graph(nm("pa_nd"))
g.start("shape1", 1, 0, "shape2")
g.groovy("shape2", 2, 0, G.fanout_script(f"{N}-nd-d"), "shape3", label="fan-out 3 docs")
g.processcall("shape3", 3, 0, CH["ch_nd"], wait=True, abort=True)
proc("pa_nd", [ND], g, "Groovy fan-out script hand-authored")

for role, child, tag, w in (("pa_ddp_pt", "ch_ddp_pt", "ddp-pt", "op_w_ddppt"),
                            ("pa_ddp_nd", "ch_ddp_nd", "ddp-nd", "op_w_ddpnd")):
    g = G.Graph(nm(role))
    g.start("shape1", 1, 0, "shape2")
    g.branch("shape2", 2, 0, ["shape3", "shape6"])
    g.groovy("shape3", 3, 0, G.fanout_script(f"{N}-{tag}-d"), "shape4", label="fan-out 3 docs")
    g.setprops("shape4", 4, 0, [("ddp", "DDP_E0PATH", [G.src_static(path(tag, "child") + "/"), prof_src()])], "shape5",
               label="per-document DDP from k")
    g.processcall("shape5", 5, 0, CH[child], wait=True, abort=True)
    g.connector("shape6", 3, 1, op[w], "shape7", label="witness")
    g.stop("shape7", 4, 1)
    proc(role, [DDP], g, "Groovy fan-out script hand-authored")

g = G.Graph(nm("pa_ddp_cache"))
g.start("shape1", 1, 0, "shape2")
g.branch("shape2", 2, 0, ["shape3", "shape6", "shape9"])
g.groovy("shape3", 3, 0, G.fanout_script(f"{N}-ddp-cache-c"), "shape4", label="fan-out 3 cache docs")
g.setprops("shape4", 4, 0, [("ddp", "DDP_E0PATH", [G.src_static(path("ddp-cache", "child") + "/"), prof_src()])], "shape5")
g.cache_load("shape5", 5, 0, CACHE_ID)
g.groovy("shape6", 3, 1, G.fanout_script(f"{N}-ddp-cache-d"), "shape7", label="fan-out 3 call docs")
g.setprops("shape7", 4, 1, [("ddp", "DDP_E0PATH", [G.src_static(path("ddp-cache", "child") + "/"), prof_src()])], "shape8")
g.processcall("shape8", 5, 1, CH["ch_ddp_cache"], wait=True, abort=True)
g.connector("shape9", 3, 2, op["op_w_ddpc"], "shape10", label="witness")
g.stop("shape10", 4, 2)
proc("pa_ddp_cache", [DDP], g, "Groovy fan-out script hand-authored")

g = G.Graph(nm("pa_cache"))
g.start("shape1", 1, 0, "shape2")
g.branch("shape2", 2, 0, ["shape3", "shape5", "shape7", "shape9"])
g.groovy("shape3", 3, 0, G.fanout_script(f"{N}-cache-c"), "shape4", label="fan-out 3 cache docs")
g.cache_load("shape4", 4, 0, CACHE_ID)
g.message("shape5", 3, 1, json.dumps({"k": f"{N}-cache-callpt"}), "shape6")
g.processcall("shape6", 4, 1, CH["ch_cache_pt"], wait=True, abort=True)
g.message("shape7", 3, 2, json.dumps({"k": f"{N}-cache-callnd"}), "shape8")
g.processcall("shape8", 4, 2, CH["ch_cache_nd"], wait=True, abort=True)
g.connector("shape9", 3, 3, op["op_w_cache"], "shape10", label="witness")
g.stop("shape10", 4, 3)
proc("pa_cache", [CACHE], g, "Groovy fan-out script hand-authored")

for role, child, tag in (("pa_dpp_pt", "ch_dpp_pt", "dpp-pt"), ("pa_dpp_nd", "ch_dpp_nd", "dpp-nd")):
    g = G.Graph(nm(role))
    g.start("shape1", 1, 0, "shape2")
    g.setprops("shape2", 2, 0, [("dpp", "DPP_E0IN", [G.src_static(f"{N}-{tag}-parent-set-in")])], "shape3")
    g.branch("shape3", 3, 0, ["shape4", "shape5"])
    g.processcall("shape4", 4, 0, CH[child], wait=True, abort=True)
    g.setprops("shape5", 4, 1, [("ddp", "DDP_E0PATH", [G.src_static(path(tag, "parent-read-out") + "/"),
                                                        G.src_dpp("DPP_E0OUT")])], "shape6")
    g.connector("shape6", 5, 1, op["op_blank"], "shape7", ddp="DDP_E0PATH")
    g.stop("shape7", 6, 1)
    proc(role, [DPP], g)

# ------------------------------------------------------------------ prefix predecessors
KINDS = ["map", "set_ddp", "set_dpp", "cache_retrieve", "cache_remove", "flow_control", "message", "data_process"]


def kind_shape(g, kind, sid, col, row, nxt, tok):
    if kind == "map":
        g.map(sid, col, row, MAP_ID, nxt)
    elif kind == "set_ddp":
        g.setprops(sid, col, row, [("ddp", "DDP_E0PFX", [G.src_static(tok + "-ddp")])], nxt)
    elif kind == "set_dpp":
        g.setprops(sid, col, row, [("dpp", "DPP_E0PFX", [G.src_static(tok + "-dpp")])], nxt)
    elif kind == "cache_retrieve":
        g.cache_retrieve(sid, col, row, CACHE_ID, nxt)
    elif kind == "cache_remove":
        g.cache_remove(sid, col, row, CACHE_ID, nxt)
    elif kind == "flow_control":
        g.flowcontrol(sid, col, row, nxt, 1)
    elif kind == "message":
        g.message(sid, col, row, json.dumps({"k": tok + "-msgkind"}), nxt)
    elif kind == "data_process":
        g.groovy(sid, col, row, G.PASSTHROUGH_SCRIPT, nxt, label="passthrough script (UI capture text)")


for kind in KINDS:
    for ctx in ("br", "dec"):
        role = f"pfx_{kind}_{ctx}"
        tok = f"{N}-pfx-{kind}-{ctx}"
        g = G.Graph(nm(role))
        g.start("shape1", 1, 0, "shape2")
        g.message("shape2", 2, 0, json.dumps({"k": tok}), "shape3")
        if kind == "cache_retrieve":
            g.branch("shape3", 3, 0, ["shape4", "shape5"])
            g.cache_load("shape4", 4, 0, CACHE_ID)
            if ctx == "br":
                kind_shape(g, kind, "shape5", 4, 1, "shape6", tok)
                g.processcall("shape6", 5, 1, CH["ch_pfx"])
            else:
                g.decision_true("shape5", 4, 1, "shape6", "shape8")
                kind_shape(g, kind, "shape6", 5, 1, "shape7", tok)
                g.processcall("shape7", 6, 1, CH["ch_pfx"])
                g.stop("shape8", 5, 2)
        elif ctx == "br":
            g.branch("shape3", 3, 0, ["shape4", "shape6"])
            kind_shape(g, kind, "shape4", 4, 0, "shape5", tok)
            g.processcall("shape5", 5, 0, CH["ch_pfx"])
            g.stop("shape6", 4, 1)
        else:
            g.decision_true("shape3", 3, 0, "shape4", "shape6")
            kind_shape(g, kind, "shape4", 4, 0, "shape5", tok)
            g.processcall("shape5", 5, 0, CH["ch_pfx"])
            g.stop("shape6", 4, 1)
        proc(role, [PFX], g, "kind shape wired directly into a terminal processcall" +
             ("; cache primed by an earlier Branch leg" if kind == "cache_retrieve" else ""))

ST["tree_at_end"] = L.tree_stamp()
ST["finished_utc"] = L.utc()
L.save_state("provision", ST)
bad = [r for r in CO.values() if r["kind"] == "process" and not r["shapes_stored_equal_submitted"]]
print("processes:", sum(1 for r in CO.values() if r["kind"] == "process"),
      "shapes-not-equal:", [r["role"] for r in bad])
print("tree end:", ST["tree_at_end"])
