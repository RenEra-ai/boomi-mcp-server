"""#184 r28 INTERNAL hunt explorer (labelled INTERNAL: the compiler pipeline in-process, no account, no MCP boundary).

Purpose: grade MANY constructions (composition vs its flattened twin) quickly at either arm, so the public matrix
spends its account budget on the cells that matter. Every hit here is re-proved through build_integration.

Self-contained: imports ONLY src/ of the arm on PYTHONPATH (Q184_TREE), never the repo's tests, so each arm is
the extracted tree's own code (asserted).  The pipeline is the one `_b27_verdict` drives (effect resolution with
both declaration spellings -> validate_process_ir and compile_process_ir_v1, asserted equal), over OUR symbol table.

FIXTURE PROVENANCE: node spellings = served ProcessIRV1 $defs (out/shapes27_head_ProcessIRV1.json, r27 t27_shapes);
symbol shapes = ComponentSymbolV1 fields; expected verdicts come from the flattened twin (same legs, one process),
never from this implementation's output for the composition.
"""
import os, sys, json, itertools, traceback
T = os.environ["Q184_TREE"]
sys.path[:0] = [os.path.join(T, "src"), T]
import boomi_mcp  # noqa: E402
assert os.path.realpath(boomi_mcp.__file__).startswith(os.path.realpath(T)), boomi_mcp.__file__
from boomi_mcp.authoring.process_ir_effects import resolve_process_ir_effect_declarations  # noqa: E402
from boomi_mcp.compiler.process_ir import connector_capabilities as CC  # noqa: E402
from boomi_mcp.compiler.process_ir.contracts import ComponentSymbolV1, SymbolTableV1  # noqa: E402
from boomi_mcp.compiler.process_ir.diagnostics import ProcessIRCompileError  # noqa: E402
from boomi_mcp.compiler.process_ir.pipeline import compile_process_ir_v1  # noqa: E402
from boomi_mcp.compiler.process_ir.semantic_validation.contracts import DEFAULT_VALIDATION_CAPABILITIES  # noqa: E402
from boomi_mcp.compiler.process_ir.semantic_validation.pipeline import validate_process_ir  # noqa: E402
from boomi_mcp.models.authoring_workflow import ProcessIREffectDeclarationsV1  # noqa: E402
from boomi_mcp.models.process_ir import parse_process_ir_v1  # noqa: E402

PROCS = ("PARENT", "CHILD", "MID", "GC", "NODATA")


def symbols(typed):
    rest = CC.REST_FAMILY

    def sym(ref, cid, ctype, **kw):
        return ComponentSymbolV1(ref="$ref:" + ref, component_id=cid, component_type=ctype, **kw)

    m22 = dict(input_profile_ref="$ref:P2", output_profile_ref="$ref:P2") if typed else {}
    return SymbolTableV1(symbols=(
        sym("RCONN", "RCONN", "connector-settings", connector_type=rest),
        sym("GET", "GETOP", "connector-action", connector_type=rest, action_type="GET", connection_ref="$ref:RCONN"),
        sym("GETP2", "GETP2OP", "connector-action", connector_type=rest, action_type="GET",
            connection_ref="$ref:RCONN", output_profile_ref="$ref:P2"),
        sym("PATCH", "PATCHOP", "connector-action", connector_type=rest, action_type="PATCH",
            connection_ref="$ref:RCONN"),
        sym("M22", "M22", "transform.map", **m22),
        sym("P1", "PROFILE-ONE", "profile.json"),
        sym("P2", "PROFILE-TWO", "profile.json"),
        sym("CACHE", "CACHE", "documentcache"),
        sym("CACHE2", "CACHE2", "documentcache"),
        sym("CACHE3", "CACHE3", "documentcache"),
    ) + tuple(sym(k, k + "-PROC", "process") for k in PROCS))


STOP = {"kind": "stop"}
MSG = {"kind": "message", "text": "m"}
SRC = {"kind": "source", "connection_ref": "$ref:RCONN", "operation_ref": "$ref:GET"}
ENTRY = {"kind": "passthrough", "label": "Receive"}
GET = {"kind": "connector_call", "operation_ref": "$ref:GET"}
GETP2 = {"kind": "connector_call", "operation_ref": "$ref:GETP2"}
PATCH = {"kind": "connector_call", "operation_ref": "$ref:PATCH"}


def BOUND(p="X"):
    return {"kind": "connector_call", "operation_ref": "$ref:GET", "path_binding": {"property_name": p}}


def TARGET(p="X"):
    return {"kind": "target", "connection_ref": "$ref:RCONN", "operation_ref": "$ref:GET",
            "path_binding": {"property_name": p}}


def rd(c):
    return {"kind": "cache_get", "cache_ref": "$ref:" + c}


def dcr(c):
    return {"kind": "document_cache_retrieve", "cache_ref": "$ref:" + c}


def put(c):
    return {"kind": "cache_put", "cache_ref": "$ref:" + c}


def call(k, **kw):
    return dict({"kind": "process_call", "process_ref": "$ref:" + k}, **kw)


STATIC_X = {"kind": "set_ddp", "name": "X", "source_values": [{"value_type": "static", "value": "/c/1"}]}
DYN_X = {"kind": "set_ddp", "name": "X", "source_values": [
    {"value_type": "static", "value": "/clients/"}, {"value_type": "dpp", "property_name": "key", "default_value": ""}]}
READS_X = {"kind": "set_dpp", "name": "Y", "source_values": [{"value_type": "ddp", "property_name": "X"}]}
ORD_X = {"kind": "set_dpp", "name": "Z", "source_values": [{"value_type": "ddp", "property_name": "X"}]}
W_FROM_X = {"kind": "set_ddp", "name": "W", "source_values": [{"value_type": "ddp", "property_name": "X"}]}
PROF = {"profile_type": "json", "profile_ref": "$ref:P2", "link_element_key": "1", "link_element_name": "root"}
STEP = {
    "conn": GET, "connp2": GETP2, "patch": PATCH,
    "map": {"kind": "map_ref", "map_ref": "$ref:M22"},
    "msg": MSG,
    "script": {"kind": "data_process", "steps": [{"operation": "custom_scripting", "language": "groovy2",
                                                  "script": "// s"}]},
    "split": {"kind": "data_process", "steps": [dict(PROF, operation="split_documents")]},
    "combine": {"kind": "data_process", "steps": [dict(PROF, operation="combine_documents")]},
    "fc": {"kind": "flow_control", "for_each_count": 1},
    "ddp": {"kind": "set_ddp", "name": "Z", "source_values": [{"value_type": "static", "value": "v"}]},
    "dpp": {"kind": "set_dpp", "name": "Z", "source_values": [{"value_type": "static", "value": "v"}]},
    "notify": {"kind": "notify", "level": "INFO", "message_template": "n"},
    "nodata": call("NODATA", wait=True),
}
CALLER = {"lit": [GET, STATIC_X], "none": [GET], "dyn": [GET, DYN_X]}


def doc(*steps):
    return {"version": "1", "body": {"kind": "sequence", "steps": list(steps)}}


def legs(head, *ls):
    return doc(*(head + [{"kind": "branch", "legs": [{"steps": list(s), "terminal": t} for s, t in ls]}]))


def dec(true_steps, true_term, false_steps=(MSG,), false_term=STOP):
    return {"kind": "decision", "comparison": "equals", "left": {"value_type": "static", "static_value": "a"},
            "right": {"value_type": "static", "static_value": "a"},
            "true_arm": {"steps": list(true_steps), "terminal": true_term},
            "false_arm": {"steps": list(false_steps), "terminal": false_term}}


def nest(steps0, term0=STOP):
    return {"kind": "branch", "legs": [{"steps": list(steps0), "terminal": term0}, {"steps": [MSG], "terminal": STOP}]}


def tcatch(try_steps, catch_steps):
    return {"kind": "try_catch", "scope": "process", "try_body": {"steps": list(try_steps), "terminal": STOP},
            "catch_body": {"steps": list(catch_steps), "terminal": STOP}}


def S(*names):
    return [STEP[n] for n in names]


# ------------------------------------------------------------------ the standard composition (r27 REPRO shape)
def std(caller, child_legs, head, extra=None, flat_legs=None, parent_extra=()):
    """parent: Branch [dyn X -> CACHE][caller -> CACHE2][*parent_extra][-> call CHILD];
    child: head + Branch[child_legs]; twin: head + Branch[parent legs..., flat_legs or child_legs]."""
    pl = [(CALLER["dyn"], put("CACHE")), (CALLER[caller], put("CACHE2"))] + list(parent_extra)
    roots = [("PARENT", legs([], *pl, ([], call("CHILD", wait=True)))), ("CHILD", legs(head, *child_legs))]
    roots += list(extra or ())
    twin = legs(head, *(pl + list(flat_legs if flat_legs is not None else child_legs)))
    return roots, twin


RECACHE = ([rd("CACHE"), READS_X], put("CACHE2"))


def between(names, use=None):
    return lambda caller, head: std(caller, [RECACHE, ([rd("CACHE2")] + S(*names) + (use or [BOUND()]), STOP)], head)


def placed(pre, term):
    return lambda caller, head: std(caller, [RECACHE, ([rd("CACHE2")] + S(*pre), term)], head)


C = {}
# single steps (KINDS, internal mirror)
for n in STEP:
    C[f"k_{n}"] = between([n])
C["k_dec_true"] = placed([], dec([BOUND()], STOP))
C["k_nest"] = placed([], nest([BOUND()]))
C["k_tc_try"] = placed([], tcatch([BOUND()], [MSG]))
# two steps around the connector call
for a, b in (("conn", "conn"), ("conn", "map"), ("map", "conn"), ("conn", "msg"), ("msg", "conn"), ("conn", "fc"),
             ("fc", "conn"), ("conn", "ddp"), ("conn", "dpp"), ("connp2", "map"), ("patch", "conn"),
             ("conn", "script"), ("script", "conn"), ("conn", "split"), ("conn", "nodata"), ("nodata", "conn")):
    C[f"two_{a}_{b}"] = between([a, b])
# controls behind the connector call
C["conn_dec_true"] = placed(["conn"], dec([BOUND()], STOP))
C["conn_dec_false"] = placed(["conn"], dec([MSG], STOP, [BOUND()], STOP))
C["conn_nest"] = placed(["conn"], nest([BOUND()]))
C["conn_nest_conn"] = placed([], nest(S("conn") + [BOUND()]))
C["dec_conn"] = placed([], dec(S("conn") + [BOUND()], STOP))
C["conn_tc_try"] = placed(["conn"], tcatch([BOUND()], [MSG]))
C["tc_conn_try"] = placed([], tcatch(S("conn") + [BOUND()], [MSG]))
C["conn_dec_nest"] = placed(["conn"], dec([], nest([BOUND()])))
# uses other than a bound GET
C["conn_viaddp"] = between(["conn"], [W_FROM_X, BOUND("W")])
C["conn_ordinary"] = between(["conn"], [ORD_X])
C["conn_target"] = between(["conn"], [TARGET()])
C["target_direct"] = between([], [TARGET()])
C["conn_set_x_dyn"] = between(["conn"], [DYN_X, BOUND()])  # control: the child overwrites X dynamically
# retrieves
C["dcr_conn"] = lambda caller, head: std(caller, [RECACHE, ([dcr("CACHE2"), GET, BOUND()], STOP)], head)
C["dcr_direct"] = lambda caller, head: std(caller, [RECACHE, ([dcr("CACHE2"), BOUND()], STOP)], head)
for fill, fsteps in (("bare", [GET]), ("dyn", [GET, DYN_X]), ("lit", [GET, STATIC_X])):
    for mid in ((), ("conn",), ("map",), ("msg",)):
        tag = "_".join(mid) or "none"
        C[f"rr_c3{fill}_{tag}"] = (lambda caller, head, fsteps=fsteps, mid=mid: std(
            caller, [RECACHE, (list(fsteps), put("CACHE3")), ([rd("CACHE2")] + S(*mid) + [rd("CACHE3"), BOUND()], STOP)],
            head))
        C[f"rr_c3{fill}_first_{tag}"] = (lambda caller, head, fsteps=fsteps, mid=mid: std(
            caller, [RECACHE, (list(fsteps), put("CACHE3")), ([rd("CACHE3")] + S(*mid) + [rd("CACHE2"), BOUND()], STOP)],
            head))
C["rr_c2_c2_conn"] = between(["conn"], [rd("CACHE2"), BOUND()])
C["rr_c2_conn_c1"] = between(["conn"], [rd("CACHE"), BOUND()])  # CACHE holds the parent's dynamic X
C["rr_c2_c1"] = between([], [rd("CACHE"), BOUND()])
C["rr_c3empty"] = between([], [rd("CACHE3"), BOUND()])


# LIMIT: retrieve CACHE (the caller's) -> step -> re-cache CACHE2 -> retrieve CACHE2 -> use
def limit(names, use):
    def f(caller, head):
        pl = [(CALLER[caller], put("CACHE"))]
        cl = [([rd("CACHE")] + S(*names), put("CACHE2")), ([rd("CACHE2")] + use, STOP)]
        roots = [("PARENT", legs([], *pl, ([], call("CHILD", wait=True)))), ("CHILD", legs(head, *cl))]
        return roots, legs(head, *(pl + cl))
    return f


for n in ("conn", "connp2", "map", "msg", "patch", "script", "fc", None):
    for u, us in (("bound", [BOUND()]), ("ord", [ORD_X])):
        C[f"lim_{n or 'none'}_{u}"] = limit([n] if n else [], us)


# chain: CACHE -n-> CACHE2 -n-> CACHE3 -n-> bound ; caller X in CACHE
def chain(n):
    def f(caller, head):
        pl = [(CALLER[caller], put("CACHE"))]
        cl = [([rd("CACHE")] + S(n), put("CACHE2")), ([rd("CACHE2")] + S(n), put("CACHE3")),
              ([rd("CACHE3")] + S(n) + [BOUND()], STOP)]
        roots = [("PARENT", legs([], *pl, ([], call("CHILD", wait=True)))), ("CHILD", legs(head, *cl))]
        return roots, legs(head, *(pl + cl))
    return f


for n in ("conn", "map", "msg"):
    C[f"chain_{n}"] = chain(n)


# passthrough grandchild behind a connector call
def ptgc(pre, gc_body, flat_tail):
    def f(caller, head):
        return std(caller, [RECACHE, ([rd("CACHE2")] + S(*pre), call("GC", wait=True))], head,
                   extra=[("GC", doc(ENTRY, *gc_body))],
                   flat_legs=[RECACHE, ([rd("CACHE2")] + S(*pre) + flat_tail[0], flat_tail[1])])
    return f


C["pt_conn"] = ptgc(["conn"], [BOUND(), STOP], ([BOUND()], STOP))
C["pt_conn_in_gc"] = ptgc([], [GET, BOUND(), STOP], ([GET, BOUND()], STOP))
C["pt_conn_both"] = ptgc(["conn"], [GET, BOUND(), STOP], ([GET, BOUND()], STOP))
C["pt_map_in_gc"] = ptgc([], [STEP["map"], BOUND(), STOP], ([STEP["map"], BOUND()], STOP))
C["pt_conn_gc_dec"] = ptgc(["conn"], [dec([BOUND()], STOP)], ([], dec([BOUND()], STOP)))


# return path: the child retrieves CACHE2 -> steps -> return_documents; the caller binds on what it gets back
def ret(pre):
    def f(caller, head):
        pl = [(CALLER["dyn"], put("CACHE")), (CALLER[caller], put("CACHE2"))]
        roots = [("PARENT", legs([], *pl, ([call("CHILD", wait=True), BOUND()], STOP))),
                 ("CHILD", doc(*(head + [rd("CACHE2")] + S(*pre) + [{"kind": "return_documents"}])))]
        twin = legs(head, *(pl + [([rd("CACHE2")] + S(*pre) + [BOUND()], STOP)]))
        return roots, twin
    return f


for pre in ((), ("conn",), ("map",), ("conn", "map")):
    C["ret_" + ("_".join(pre) or "none")] = ret(pre)


# forwarder hop: PARENT -> MID -> CHILD; the child binds behind a connector call
def fwd(names):
    def f(caller, head):
        pl = [(CALLER["dyn"], put("CACHE")), (CALLER[caller], put("CACHE2"))]
        cl = [RECACHE, ([rd("CACHE2")] + S(*names) + [BOUND()], STOP)]
        roots = [("PARENT", legs([], *pl, ([], call("MID", wait=True)))),
                 ("MID", legs([], ([], call("CHILD", wait=True)))), ("CHILD", legs(head, *cl))]
        return roots, legs(head, *(pl + cl))
    return f


C["fwd_conn"] = fwd(["conn"])
C["fwd_map"] = fwd(["map"])


# backflow: the child re-caches the caller's CACHE2 documents into CACHE3 through steps; the CALLER binds after
def back(names):
    def f(caller, head):
        w = (CALLER[caller], put("CACHE2"))
        rc = ([rd("CACHE2")] + S(*names), put("CACHE3"))
        use = ([rd("CACHE3"), BOUND()], STOP)
        roots = [("PARENT", legs([], w, ([], call("CHILD", wait=True)), use)), ("CHILD", legs(head, rc))]
        return roots, legs(head, w, rc, use)
    return f


for n in (("conn",), ("map",), ("msg",), ()):
    C["back_" + ("_".join(n) or "none")] = back(n)


# the CALLER retrieves its own CACHE2 -> steps -> a waited passthrough child binds
def ptparent(names):
    def f(caller, head):
        w = (CALLER[caller], put("CACHE2"))
        roots = [("PARENT", legs([], w, ([rd("CACHE2")] + S(*names), call("GC", wait=True)))),
                 ("GC", doc(ENTRY, BOUND(), STOP))]
        return roots, legs([], w, ([rd("CACHE2")] + S(*names) + [BOUND()], STOP))
    return f


C["ptparent_conn"] = ptparent(["conn"])
C["ptparent_map"] = ptparent(["map"])


# ---- round 2 (r28): cardinality-fixed forwarder/backflow, passthrough grandchildren on attested predecessors,
# re-cache inside a passthrough grandchild, ordinary uses on returned documents, more retrieve pairs
def back2(names):
    def f(caller, head):
        w = (CALLER[caller], put("CACHE2"))
        rc = ([rd("CACHE2")] + S(*names), put("CACHE3"))
        use = ([rd("CACHE3"), BOUND()], STOP)
        roots = [("PARENT", legs([], w, ([], call("CHILD", wait=True)), use)),
                 ("CHILD", legs(head, rc, ([MSG], STOP)))]
        return roots, legs(head, w, rc, use)
    return f


for n in (("conn",), ("map",), ("msg",), (), ("conn", "map")):
    C["back2_" + ("_".join(n) or "none")] = back2(n)


def fwd2(names):
    def f(caller, head):
        pl = [(CALLER["dyn"], put("CACHE")), (CALLER[caller], put("CACHE2"))]
        cl = [RECACHE, ([rd("CACHE2")] + S(*names) + [BOUND()], STOP)]
        roots = [("PARENT", legs([], *pl, ([], call("MID", wait=True)))),
                 ("MID", legs([], ([], call("CHILD", wait=True)), ([MSG], STOP))), ("CHILD", legs(head, *cl))]
        return roots, legs(head, *(pl + cl))
    return f


for n in (("conn",), ("map",), ("conn", "map")):
    C["fwd2_" + "_".join(n)] = fwd2(n)
for pre in (("conn", "map"), ("conn", "msg"), ("conn", "fc"), ("conn", "ddp"), ("map",), ("msg",), ()):
    tag = "_".join(pre) or "none"
    C[f"ptA_{tag}"] = ptgc(list(pre), [BOUND(), STOP], ([BOUND()], STOP))
    C[f"ptB_{tag}_gcconn"] = ptgc(list(pre), [GET, BOUND(), STOP], ([GET, BOUND()], STOP))


def pt_recache(pre, gc_mid):
    """child: rd CACHE2 -> pre -> call GC; GC: ENTRY, Branch[[gc_mid] -> put CACHE3][rd CACHE3 -> bound]."""
    def f(caller, head):
        gc = doc(ENTRY, {"kind": "branch", "legs": [{"steps": S(*gc_mid), "terminal": put("CACHE3")},
                                                     {"steps": [rd("CACHE3"), BOUND()], "terminal": STOP}]})
        return std(caller, [RECACHE, ([rd("CACHE2")] + S(*pre), call("GC", wait=True))], head,
                   extra=[("GC", gc)],
                   flat_legs=[RECACHE, ([rd("CACHE2")] + S(*pre) + S(*gc_mid), put("CACHE3")),
                              ([rd("CACHE3"), BOUND()], STOP)])
    return f


for pre in (("map",), ("msg",), ("conn", "map")):
    for gm in (("conn",), ("map",), ("msg",), ()):
        C[f"ptrc_{'_'.join(pre)}__{'_'.join(gm) or 'none'}"] = pt_recache(pre, gm)


def ret_ord(pre):
    def f(caller, head):
        pl = [(CALLER["dyn"], put("CACHE")), (CALLER[caller], put("CACHE2"))]
        roots = [("PARENT", legs([], *pl, ([call("CHILD", wait=True), ORD_X], STOP))),
                 ("CHILD", doc(*(head + [rd("CACHE2")] + S(*pre) + [{"kind": "return_documents"}])))]
        return roots, legs(head, *(pl + [([rd("CACHE2")] + S(*pre) + [ORD_X], STOP)]))
    return f


for pre in ((), ("conn",), ("map",)):
    C["retord_" + ("_".join(pre) or "none")] = ret_ord(pre)
C["k_nodata_ord"] = between(["nodata"], [ORD_X])
C["conn_nodata_ord"] = between(["conn", "nodata"], [ORD_X])
for n in ("none", "map", "msg", "conn", "fc"):
    C[f"viaddp_{n}"] = between([] if n == "none" else [n], [W_FROM_X, BOUND("W")])
    C[f"ord_{n}"] = between([] if n == "none" else [n], [ORD_X])
C["rr_dcr3_conn"] = lambda caller, head: std(caller, [RECACHE, ([GET], put("CACHE3")),
                                                      ([rd("CACHE2"), GET, dcr("CACHE3"), BOUND()], STOP)], head)
C["rr_c2_conn_c3_conn"] = lambda caller, head: std(caller, [RECACHE, ([GET], put("CACHE3")),
                                                            ([rd("CACHE2"), GET, rd("CACHE3"), GET, BOUND()], STOP)], head)
C["conn_split_bound"] = between(["conn", "split"])
C["conn_combine_bound"] = between(["conn", "combine"])
C["conn_script_conn"] = between(["conn", "script", "conn"])
C["dec_recache"] = lambda caller, head: std(caller, [RECACHE, ([rd("CACHE2"), GET], dec([], put("CACHE3"))),
                                                     ([rd("CACHE3"), BOUND()], STOP)], head)
C["conn_recache_conn"] = lambda caller, head: std(caller, [RECACHE, ([rd("CACHE2"), GET], put("CACHE3")),
                                                           ([rd("CACHE3"), GET, BOUND()], STOP)], head)


# ---- round 3 (r28): caller-shared second retrieve, top-level controls, bound PATCH, mixed data process, direct
def rr_caller(fill, order, mid=()):
    def f(caller, head):
        extra = [(CALLER[fill], put("CACHE3"))]
        first, second = ("CACHE2", "CACHE3") if order == "c2c3" else ("CACHE3", "CACHE2")
        return std(caller, [RECACHE, ([rd(first)] + S(*mid) + [rd(second), BOUND()], STOP)], head, parent_extra=extra)
    return f


for fill in ("none", "dyn", "lit"):
    for order in ("c2c3", "c3c2"):
        for mid in ((), ("conn",)):
            C[f"rrcal_{fill}_{order}_{'_'.join(mid) or 'none'}"] = rr_caller(fill, order, mid)


def top(pre, term):
    """the child's TOP-LEVEL body (no re-cache leg): rd CACHE2 -> pre -> term (a control holding the bound use)."""
    def f(caller, head):
        pl = [(CALLER["dyn"], put("CACHE")), (CALLER[caller], put("CACHE2"))]
        roots = [("PARENT", legs([], *pl, ([], call("CHILD", wait=True)))),
                 ("CHILD", doc(*(head + [rd("CACHE2")] + S(*pre) + [term])))]
        if term.get("kind") == "branch":
            flat = [(pl_s, pl_t) for pl_s, pl_t in pl] + [([rd("CACHE2")] + S(*pre) + term["legs"][0]["steps"], term["legs"][0]["terminal"])]
        else:
            flat = pl + [([rd("CACHE2")] + S(*pre), term)]
        return roots, legs(head, *flat)
    return f


for pre in (("conn",), ("map",), ()):
    t = "_".join(pre) or "none"
    C[f"top_{t}_branch"] = top(pre, nest([BOUND()]))
    C[f"top_{t}_dec"] = top(pre, dec([BOUND()], STOP))
    C[f"top_{t}_seq"] = (lambda caller, head, pre=pre: (lambda r: r)(top(pre, STOP)(caller, head)) if False else
                         top_seq(pre)(caller, head))


def top_seq(pre):
    def f(caller, head):
        pl = [(CALLER["dyn"], put("CACHE")), (CALLER[caller], put("CACHE2"))]
        roots = [("PARENT", legs([], *pl, ([], call("CHILD", wait=True)))),
                 ("CHILD", doc(*(head + [rd("CACHE2")] + S(*pre) + [BOUND(), STOP])))]
        return roots, legs(head, *(pl + [([rd("CACHE2")] + S(*pre) + [BOUND()], STOP)]))
    return f


BOUND_PATCH = {"kind": "connector_call", "operation_ref": "$ref:PATCH", "path_binding": {"property_name": "X"}}
C["k_direct"] = between([])
C["conn_boundpatch"] = between(["conn"], [BOUND_PATCH])
C["patch_boundpatch"] = between(["patch"], [BOUND_PATCH])
STEP["dpmix"] = {"kind": "data_process", "steps": [dict(PROF, operation="split_documents"),
                                                   dict(PROF, operation="combine_documents")]}
STEP["dp2script"] = {"kind": "data_process", "steps": [
    {"operation": "custom_scripting", "language": "groovy2", "script": "// a"},
    {"operation": "custom_scripting", "language": "groovy2", "script": "// b"}]}
C["k_dpmix"] = between(["dpmix"])
C["k_dp2script"] = between(["dp2script"])
C["conn_dpmix"] = between(["conn", "dpmix"])


# ---- round 4 (r28): NO re-cache leg - the child's only contact with X is the caller's CACHE2 documents
def norc(names, use=None):
    return lambda caller, head: std(caller, [([rd("CACHE2")] + S(*names) + (use or [BOUND()]), STOP), ([MSG], STOP)], head)


for n in ("none", "map", "msg", "conn", "connp2", "fc", "ddp", "dpp", "script"):
    C[f"norc_{n}"] = norc([] if n == "none" else [n])
    C[f"norc_{n}_ord"] = norc([] if n == "none" else [n], [ORD_X])
C["norc_conn_map"] = norc(["conn", "map"])
C["norc_dec_conn"] = lambda caller, head: std(caller, [([rd("CACHE2"), GET], dec([BOUND()], STOP)), ([MSG], STOP)], head)
C["norc_dcr_conn"] = lambda caller, head: std(caller, [([dcr("CACHE2"), GET, BOUND()], STOP), ([MSG], STOP)], head)
C["top_msg_seq"] = top_seq(["msg"])
C["top_fc_seq"] = top_seq(["fc"])


# --------------------------------------------------------------------------- the pipeline
def verdict(roots, key, typed):
    syms = symbols(typed)
    parsed = [(name, parse_process_ir_v1(d)) for name, d in roots]
    seen, rows, findings = [], [], []
    for declarations in (None, ProcessIREffectDeclarationsV1()):
        res = resolve_process_ir_effect_declarations(parsed, declarations, syms, [],
                                                     child_roots={"$ref:" + n: ir for n, ir in parsed})
        if not res.ok:
            findings.append(sorted({getattr(f, "code", str(f)) for f in res.findings}))
            seen.append([("RESOLUTION:" + c, None) for c in findings[-1]])
            rows.append(None)
            continue
        caps = res.capabilities_by_root[key] or DEFAULT_VALIDATION_CAPABILITIES
        val = sorted((i.code, i.path) for i in validate_process_ir(dict(parsed)[key], syms, capabilities=caps).errors)
        try:
            compile_process_ir_v1(dict(parsed)[key], syms, capabilities=caps)
            comp = []
        except ProcessIRCompileError as exc:
            comp = sorted((i.code, i.path) for i in exc.diagnostics)
        assert comp == val, (key, val, comp)
        seen.append(val)
        called = res.capabilities_by_root.get("PARENT")
        row = None
        if called is not None and key == "PARENT":
            try:
                c = called.child_entry_contract("$ref:CHILD")
                row = sorted(map(str, c.cache_property_requirements)) if c is not None else None
            except Exception:  # noqa: BLE001
                row = "ERR"
        rows.append(row)
    assert seen[0] == seen[1], seen
    return seen[0], rows[0]


SHORT = lambda c: c.replace("PROCESS_IR_SEMANTIC_", "").replace("PROCESS_IR_", "")  # noqa: E731
BOUNDC = {"PROCESS_IR_SEMANTIC_DYNAMIC_PATH_NO_DYNAMIC_SEGMENT", "PROCESS_IR_SEMANTIC_DYNAMIC_PATH_DDP_NOT_ESTABLISHED",
          "PROCESS_IR_SEMANTIC_LINEAGE_PROPERTY_READ_BEFORE_WRITE"}


def klass(comp, twin):
    if comp == twin:
        return "MATCH"
    if (twin & BOUNDC) - comp:
        return "HIT" if not comp else "ESCAPE"
    if (comp & BOUNDC) - twin:
        return "BOUND_OVER"
    return "EXTRA"


def run(name, caller, dialect):
    typed = dialect == "typed"
    head = [] if typed else [SRC]
    try:
        roots, twin = C[name](caller, head)
        per = {}
        for k, _d in roots:
            per[k], row = verdict(roots, k, typed)
            if k == "PARENT":
                prow = row
        tw, _ = verdict([("PARENT", twin)], "PARENT", typed)
    except Exception as exc:  # noqa: BLE001
        return {"error": f"{type(exc).__name__}: {str(exc)[:300]}"}
    comp = {c for v in per.values() for c, _p in v}
    tset = {c for c, _p in tw}
    return {"per_root": {k: [(SHORT(c), p) for c, p in v] for k, v in per.items()}, "twin": [(SHORT(c), p) for c, p in tw],
            "comp_codes": sorted(map(SHORT, comp)), "twin_codes": sorted(map(SHORT, tset)), "class": klass(comp, tset),
            "parent_row": prow}


if __name__ == "__main__":
    arm = os.environ.get("Q184_ARM", "head")
    names = sys.argv[1:] or sorted(C)
    out = {}
    for name in names:
        for dialect in ("legacy", "typed"):
            for caller in ("lit", "none", "dyn"):
                out[f"{name}|{dialect}|{caller}"] = run(name, caller, dialect)
    p = os.path.join(os.path.dirname(os.path.abspath(__file__)), "out", f"x28_hunt_{arm}.json")
    json.dump(out, open(p, "w"), indent=1, default=str)
    from collections import Counter
    print(arm, Counter(v.get("class", "ERROR") for v in out.values()))
    for k, v in out.items():
        if v.get("class") not in ("MATCH",):
            print(k, v.get("class") or v.get("error"), "comp=", v.get("comp_codes"), "twin=", v.get("twin_codes"))
