"""E1 #184 provisioning (resumable): confirmation captures for QA-184-e0-01/02/03.

Reuses e0 fixtures (read back and archived, never modified): cache 0370d8d8 (single-document, index on k),
JSON profile 61449393, blank-path POST op 187aa243. Creates per-run static-path ops (witness + successor
marks), a failing op (404 with returnApplicationErrors=false = a certain connector failure), and 9 processes.
Every process: Branch leg 1 = witness connector (posts FIRST, so an Exception later cannot hide it).
Shapes from the branch-point legacy renderer (g184/g184x); no #184 code.
"""
import os, sys, json, time, re
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import lib184 as L
import g184 as G
import g184x as GX

ST = L.load_state("provision") or {"components": {}}
if "nonce" not in ST:
    ST["nonce"] = "E1184" + time.strftime("%m%d%H%M%S", time.gmtime())
    ST["created_utc"] = L.utc()
    ST["tree_at_start"] = L.tree_stamp()
    ST["account_id"] = L.ACCOUNT
N = ST["nonce"]
CO = ST["components"]
L.save_state("provision", ST)
print("NONCE", N)

FIX = "cap184-e1-fixtures"
PUT = "cap184-cache-put-successor"
REM = "cap184-cache-remove-read"
REPL = "cap184-retrieve-ddp-replacement"
CACHE = "0370d8d8-2c63-42d7-ae11-9aa5bbf64262"
PROFILE = "61449393-c75f-4ec0-a035-3f841ccc4f18"
OP_BLANK = "187aa243-5f7a-44a2-be07-4ba92ed32fff"
PROV_OP = ("REST operation raw XML: the #156 oracle p1 shape (live-captured from _QA_FIXTURE_155_G1_src_GET), "
           "verb POST")


def nm(role):
    return f"_TEST_184E1_{role}_{N}"


def path(cap, role):
    return f"/__E1_184__/{N}/{cap}/{role}"


def rest_op_obj(p, rae=True):
    return (f'<Operation xmlns="" returnApplicationErrors="{"true" if rae else "false"}" trackResponse="true">'
            '<Archiving directory="" enabled="false"/><Configuration>'
            '<GenericOperationConfig customOperationType="POST" operationType="EXECUTE">'
            '<field id="followRedirects" type="string" value="NONE"/>'
            f'<field id="path" type="string" value="{p}"/>'
            '<field id="queryParameters" type="customproperties"><customProperties/></field>'
            '<field id="requestHeaders" type="customproperties"><customProperties/></field>'
            '<Options/></GenericOperationConfig></Configuration>'
            '<Tracking><TrackedFields/></Tracking><Caching/></Operation>')


# ---- reused e0 fixtures: read back, prove live + unchanged kind, archive
if "reused" not in ST:
    ST["reused"] = {}
    for role, cid in (("e0_cache", CACHE), ("e0_profile", PROFILE), ("e0_op_blank", OP_BLANK)):
        x = L.component_xml(cid)
        assert 'deleted="false"' in x, (role, "deleted or unreadable")
        red = L.redact(x)
        ST["reused"][role] = {"id": cid, "stored_redacted_sha256": L.sha(red),
                              "version": re.search(r'\bversion="(\d+)"', x).group(1),
                              "archived": L.archive(FIX, f"reused/{role}.stored.xml", red)}
    L.save_state("provision", ST)
print("reused:", json.dumps({k: (v["id"][:8], v["version"]) for k, v in ST["reused"].items()}))


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
        norm = lambda s: s.replace("&apos;", "'").replace("&quot;", '"')
        rec["shapes_stored_equal_submitted_modulo_entities"] = norm(G.shapes_of(xml)) == norm(G.shapes_of(stored_raw))
        rec["shapes_stored_byte_equal"] = G.shapes_of(xml) == G.shapes_of(stored_raw)
    for cap in caps:
        rec.setdefault("archived", {})[cap] = [
            L.archive(cap, f"components/{role}.submitted.xml", xml),
            L.archive(cap, f"components/{role}.stored.xml", stored)]
    CO[role] = rec
    L.save_state("provision", ST)
    extra = f" shapes_equal(mod entities)={rec.get('shapes_stored_equal_submitted_modulo_entities')}" if kind == "process" else ""
    print(f"  {role:22} {cid} v{rec['stored_version']}{extra}")
    return cid


RUNS = {"p1": PUT, "p2": PUT, "p3": PUT, "p3c": PUT, "rr": REM, "r1": REPL, "r2": REPL, "r3": REPL, "r4": REPL}
op = {}
for run, cap in RUNS.items():
    op[f"w_{run}"] = ensure(f"op_w_{run}", [cap], L.comp_xml(nm(f"op_w_{run}"), "connector-action",
                                                             rest_op_obj(path(run, "witness")), L.REST_SUB),
                            PROV_OP + f", returnApplicationErrors=true; static path {path(run, 'witness')!r}",
                            "connector-action")
for role, cap, p, rae in (("op_p1_after", PUT, path("p1", "after-retrieve"), True),
                          ("op_p2_after", PUT, path("p2", "after-retrieve"), True),
                          ("op_fail", PUT, path("p3", "protected-connector-404-rae-false"), False),
                          ("op_rr_after", REM, path("rr", "after-retrieve"), True)):
    op[role] = ensure(role, [cap], L.comp_xml(nm(role), "connector-action", rest_op_obj(p, rae), L.REST_SUB),
                      PROV_OP + f", returnApplicationErrors={'true' if rae else 'false'}; static path {p!r}",
                      "connector-action")


def k(run, tag):
    return json.dumps({"k": f"{N}-{run}-{tag}"})


def proc(role, cap, g, note):
    return ensure(role, [cap], g.xml(), sorted(g.prov) + [note], "process")


# ---- cap184-cache-put-successor
g = GX.GraphX(nm("p1_put_get_empty"))
g.start("shape1", 1, 0, "shape2")
g.branch("shape2", 2, 0, ["shape3", "shape5"])
g.connector("shape3", 3, 0, op["w_p1"], "shape4", label="witness")
g.stop("shape4", 4, 0)
g.message("shape5", 3, 1, k("p1", "doc"), "shape6")
g.cache_load("shape6", 4, 1, CACHE, "shape7")
g.cache_retrieve("shape7", 5, 1, CACHE, "shape8")
g.connector("shape8", 6, 1, op["op_p1_after"], "shape9", label="after retrieve")
g.stop("shape9", 7, 1)
proc("p1_put_get_empty", PUT, g, "linear Add to Cache -> Retrieve(all) -> connector; cache empty before the run")

g = GX.GraphX(nm("p2_put_get_primed"))
g.start("shape1", 1, 0, "shape2")
g.branch("shape2", 2, 0, ["shape3", "shape5", "shape7"])
g.connector("shape3", 3, 0, op["w_p2"], "shape4", label="witness")
g.stop("shape4", 4, 0)
g.message("shape5", 3, 1, k("p2", "prime"), "shape6")
g.cache_load("shape6", 4, 1, CACHE)
g.message("shape7", 3, 2, k("p2", "doc"), "shape8")
g.cache_load("shape8", 4, 2, CACHE, "shape9")
g.cache_retrieve("shape9", 5, 2, CACHE, "shape10")
g.connector("shape10", 6, 2, op["op_p2_after"], "shape11", label="after retrieve")
g.stop("shape11", 7, 2)
proc("p2_put_get_primed", PUT, g, "same as p1 with the cache primed by the earlier Branch leg 2")

for role, run, via_cache in (("p3_catch_put_exception", "p3", True), ("p3c_catch_exception_control", "p3c", False)):
    g = GX.GraphX(nm(role))
    g.start("shape1", 1, 0, "shape2")
    g.branch("shape2", 2, 0, ["shape3", "shape5"])
    g.connector("shape3", 3, 0, op[f"w_{run}"], "shape4", label="witness")
    g.stop("shape4", 4, 0)
    g.message("shape5", 3, 1, k(run, "doc"), "shape6")
    g.catcherrors("shape6", 4, 1, "shape7", "shape9")
    g.connector("shape7", 5, 1, op["op_fail"], "shape8", label="protected connector fails")
    g.stop("shape8", 6, 1)
    if via_cache:
        g.cache_load("shape9", 5, 2, CACHE, "shape10")
        g.exception("shape10", 6, 2, f"caught {N}-{run}: {{1}}")
    else:
        g.exception("shape9", 5, 2, f"caught {N}-{run}: {{1}}")
    proc(role, PUT, g, "Try/Catch: protected connector = 404 with returnApplicationErrors=false; catch body "
                       + ("Add to Cache -> Exception" if via_cache else "Exception only (ADDED control)"))

# ---- cap184-cache-remove-read
g = GX.GraphX(nm("rr_remove_then_retrieve"))
g.start("shape1", 1, 0, "shape2")
g.branch("shape2", 2, 0, ["shape3", "shape5", "shape7"])
g.connector("shape3", 3, 0, op["w_rr"], "shape4", label="witness")
g.stop("shape4", 4, 0)
g.message("shape5", 3, 1, k("rr", "prime"), "shape6")
g.cache_load("shape6", 4, 1, CACHE)
g.message("shape7", 3, 2, k("rr", "doc"), "shape8")
g.cache_remove("shape8", 4, 2, CACHE, "shape9")
g.cache_retrieve("shape9", 5, 2, CACHE, "shape10")
g.connector("shape10", 6, 2, op["op_rr_after"], "shape11", label="after retrieve")
g.stop("shape11", 7, 2)
proc("rr_remove_then_retrieve", REM, g, "Remove(all) -> Retrieve(all) -> connector; cache primed in Branch leg 2")


# ---- cap184-retrieve-ddp-replacement
def xset(name, value):
    return [("ddp", name, [G.src_static(value)])]


for run in ("r1", "r2", "r3", "r4"):
    X_c, X_d, Y_d = path(run, "X-cached"), path(run, "X-current"), path(run, "Y-current")
    g = GX.GraphX(nm(f"{run}_ddp_replacement"))
    g.start("shape1", 1, 0, "shape2")
    g.branch("shape2", 2, 0, ["shape3", "shape5", "shape8"])
    g.connector("shape3", 3, 0, op[f"w_{run}"], "shape4", label="witness")
    g.stop("shape4", 4, 0)
    # leg 2 stages the cache
    g.message("shape5", 3, 1, k(run, "cached"), "shape6")
    if run in ("r1", "r3", "r4"):
        g.setprops("shape6", 4, 1, xset("DDP_E1X", X_c), "shape7", label="X = cached")
        g.cache_load("shape7", 5, 1, CACHE)
    else:  # r2: the cached document carries no X; the message wires straight to the load
        g.cache_load("shape6", 4, 1, CACHE)
    # leg 3 reads
    g.message("shape8", 3, 2, k(run, "current"), "shape9")
    nxt = "shape9"
    col = 4
    if run in ("r1", "r2", "r4"):
        g.setprops("shape9", col, 2, xset("DDP_E1X", X_d), "shape10", label="X = current")
        nxt, col = "shape10", col + 1
    if run == "r4":
        g.setprops("shape10", col, 2, xset("DDP_E1Y", Y_d), "shape11", label="Y = current (never cached)")
        nxt, col = "shape11", col + 1
    ret = nxt
    after = f"shape{int(ret[5:]) + 1}"
    if run != "r4":
        g.cache_retrieve(ret, col, 2, CACHE, after)
        g.connector(after, col + 1, 2, OP_BLANK, f"shape{int(after[5:]) + 1}", ddp="DDP_E1X", label="Path = X")
        g.stop(f"shape{int(after[5:]) + 1}", col + 2, 2)
    else:
        b = after
        cx, sx, cy, sy = (f"shape{int(b[5:]) + i}" for i in (1, 2, 3, 4))
        g.cache_retrieve(ret, col, 2, CACHE, b)
        g.branch(b, col + 1, 2, [cx, cy])
        g.connector(cx, col + 2, 2, OP_BLANK, sx, ddp="DDP_E1X", label="Path = X")
        g.stop(sx, col + 3, 2)
        g.connector(cy, col + 2, 3, OP_BLANK, sy, ddp="DDP_E1Y", label="Path = Y")
        g.stop(sy, col + 3, 3)
    note = {"r1": "cached doc carries X=cached; current doc sets X=current before the retrieve",
            "r2": "cached doc carries NO X; current doc sets X=current before the retrieve",
            "r3": "control: cached doc carries X=cached; current doc sets nothing",
            "r4": "as r1 plus Y=current set only on the current doc; X and Y each bound on a connector in a Branch after the retrieve"}[run]
    proc(f"{run}_ddp_replacement", REPL, g, note)

ST["tree_at_end"] = L.tree_stamp()
ST["finished_utc"] = L.utc()
L.save_state("provision", ST)
print("tree end:", ST["tree_at_end"])
