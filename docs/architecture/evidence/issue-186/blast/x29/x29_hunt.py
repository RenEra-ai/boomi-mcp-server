"""#184 r29 INTERNAL hunt explorer — the READER side of the stream markers (labelled INTERNAL: compiler pipeline
in-process, no account, no MCP boundary). Built on x28_hunt (same pipeline, same symbol table + one typed-input
connector symbol), graded against the flattened twin exactly as x28 does.

FIXTURE PROVENANCE: node spellings = served ProcessIRV1 $defs (q184r27/out/shapes27_head_ProcessIRV1.json:
TrackOperandV1 {value_type:'track', property_id, property_name?, default_value?}; the 'dynamicdocument.' prefix is
the wire scope the served Decision operand carries); verdict expectations come from the flattened twin only.

Families (each x 2 dialects x callers):
  nu_<P>_<U>      no re-cache child: Branch[[rd c2] + P + U][msg]                       (P = between, U = use)
  ru_<P>_<U>      re-caching child:  Branch[RECACHE][[rd c2] + P + U]
  cpb_<P>         copy BEFORE the step: [rd c2, W=X] + P + [bound W]  (norc)
  rr2_<A>_<B>_<C> re-cache whose SECOND retrieve sits behind a property-preserving step:
                  Branch[[rd c2]+A -> put c3][B + [rd c3] + C + bound]   (norc and with RECACHE: rr2r_)
  rrc_<M>         caller fills c3 too: [rd c2] + M + [rd c3, bound]; and [rd c3] + M + [rd c2, bound]
  fn_<P>_<U>      forwarding middle: PARENT -> MID (Branch[call CHILD][msg]) -> CHILD no re-cache
  ty_<P>          CONTENT question: typed consumer (map M22 in=P2 / PUTP2 declared input P2) behind P on the
                  caller's cached documents; callers p2 (GETP2 -> c2), bare (GET -> c2), p2dyn (GETP2 + dyn X)
  oc_<P>_<U>      the child re-caches ANOTHER cache (c1 -> c3) and uses c2 behind P without re-caching it
  dcr_<P>_<U>     document_cache_retrieve of c2 instead of a cache_get (norc)
"""
import os, sys, json
from collections import Counter
HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
import x28_hunt as X  # noqa: E402  (asserts the arm tree)
from x28_hunt import (C, STEP, CALLER, std, rd, dcr, put, call, legs, doc, dec, BOUND, ORD_X, W_FROM_X, READS_X,  # noqa
                      RECACHE, MSG, STOP, GET, GETP2, DYN_X, STATIC_X, S, verdict, klass, SHORT)
from boomi_mcp.compiler.process_ir import connector_capabilities as CC  # noqa: E402
from boomi_mcp.compiler.process_ir.contracts import ComponentSymbolV1, SymbolTableV1  # noqa: E402

_sym = X.symbols


def symbols(typed):
    base = _sym(typed)
    extra = ComponentSymbolV1(ref="$ref:PUTP2", component_id="PATCHP2OP", component_type="connector-action",
                              connector_type=CC.REST_FAMILY, action_type="PATCH", connection_ref="$ref:RCONN",
                              input_profile_ref="$ref:P2")
    return SymbolTableV1(symbols=tuple(base.symbols) + (extra,))


X.symbols = symbols

TRACK_X = {"value_type": "track", "property_id": "dynamicdocument.X", "property_name": "X"}
TRACK_XD = dict(TRACK_X, default_value="")
STAT_A = {"value_type": "static", "static_value": "a"}


def dec_on(left, true_steps, true_term=STOP, false_steps=(MSG,)):
    return {"kind": "decision", "comparison": "equals", "left": left, "right": STAT_A,
            "true_arm": {"steps": list(true_steps), "terminal": true_term},
            "false_arm": {"steps": list(false_steps), "terminal": STOP}}


W_ORD = {"kind": "set_dpp", "name": "Z", "source_values": [{"value_type": "ddp", "property_name": "W"}]}
STEP.update({
    "putp2": {"kind": "connector_call", "operation_ref": "$ref:PUTP2"},
    "dpmix": STEP.get("dpmix") or {"kind": "data_process", "steps": [dict(X.PROF, operation="split_documents"),
                                                                      dict(X.PROF, operation="combine_documents")]},
})
# uses: (steps, terminal). Decision forms end the leg; everything else is followed by STOP.
USES = {
    "bound": ([BOUND()], STOP),
    "ord": ([ORD_X], STOP),
    "viab": ([W_FROM_X, BOUND("W")], STOP),
    "viao": ([W_FROM_X, W_ORD], STOP),
    "dtr": ([], dec_on(TRACK_X, [MSG])),                 # a Decision operand reading X (non-strict)
    "dtrd": ([], dec_on(TRACK_XD, [MSG])),               # ... with a default
    "dtrb": ([], dec_on(TRACK_X, [BOUND()])),            # Decision reading X, TRUE arm binds X
    "decb": ([], dec_on(STAT_A, [BOUND()])),             # constant Decision, TRUE arm binds X
}
BETWEEN = {
    "none": [], "msg": ["msg"], "map": ["map"], "conn": ["conn"], "connp2": ["connp2"], "patch": ["patch"],
    "fc": ["fc"], "ddp": ["ddp"], "dpp": ["dpp"], "script": ["script"], "split": ["split"], "combine": ["combine"],
    "dpmix": ["dpmix"], "conn_map": ["conn", "map"], "map_conn": ["map", "conn"], "msg_conn": ["msg", "conn"],
    "conn_msg": ["conn", "msg"], "conn_conn": ["conn", "conn"], "msg_msg": ["msg", "msg"], "map_msg": ["map", "msg"],
}
NEW = {}


def leg_use(pre_steps, use):
    us, ut = USES[use]
    return (pre_steps + us, ut)


for p, names in BETWEEN.items():
    for u in USES:
        NEW[f"nu_{p}_{u}"] = (lambda caller, head, names=names, u=u:
                              std(caller, [leg_use([rd("CACHE2")] + S(*names), u), ([MSG], STOP)], head))
        NEW[f"ru_{p}_{u}"] = (lambda caller, head, names=names, u=u:
                              std(caller, [RECACHE, leg_use([rd("CACHE2")] + S(*names), u)], head))
        NEW[f"oc_{p}_{u}"] = (lambda caller, head, names=names, u=u:
                              std(caller, [([rd("CACHE"), READS_X], put("CACHE3")),
                                           leg_use([rd("CACHE2")] + S(*names), u)], head))
    for u in ("bound", "ord", "dtrb"):
        NEW[f"dcr_{p}_{u}"] = (lambda caller, head, names=names, u=u:
                               std(caller, [leg_use([dcr("CACHE2")] + S(*names), u), ([MSG], STOP)], head))
    NEW[f"cpb_{p}"] = (lambda caller, head, names=names:
                       std(caller, [([rd("CACHE2"), W_FROM_X] + S(*names) + [BOUND("W")], STOP), ([MSG], STOP)], head))
    NEW[f"cpbo_{p}"] = (lambda caller, head, names=names:
                        std(caller, [([rd("CACHE2"), W_FROM_X] + S(*names) + [W_ORD], STOP), ([MSG], STOP)], head))

# re-cache whose second retrieve is behind a property-preserving step
PP = {"none": [], "msg": ["msg"], "conn": ["conn"], "map": ["map"]}
for a, an in PP.items():
    for b, bn in (("msg", ["msg"]), ("conn", ["conn"]), ("map", ["map"]), ("none", [])):
        for c, cn in (("none", []), ("msg", ["msg"]), ("conn", ["conn"])):
            NEW[f"rr2_{a}_{b}_{c}"] = (lambda caller, head, an=an, bn=bn, cn=cn: std(
                caller, [([rd("CACHE2")] + S(*an), put("CACHE3")), (S(*bn) + [rd("CACHE3")] + S(*cn) + [BOUND()], STOP)],
                head))
            NEW[f"rr2r_{a}_{b}_{c}"] = (lambda caller, head, an=an, bn=bn, cn=cn: std(
                caller, [RECACHE, ([rd("CACHE2")] + S(*an), put("CACHE3")),
                         (S(*bn) + [rd("CACHE3")] + S(*cn) + [BOUND()], STOP)], head))
# the caller fills c3 too; second retrieve behind a step
for fill in ("none", "dyn", "lit"):
    for m in ("msg", "map", "conn_map"):
        mn = BETWEEN[m]
        NEW[f"rrc_{fill}_c2c3_{m}"] = (lambda caller, head, fill=fill, mn=mn: std(
            caller, [([rd("CACHE2")] + S(*mn) + [rd("CACHE3"), BOUND()], STOP), ([MSG], STOP)], head,
            parent_extra=[(CALLER[fill], put("CACHE3"))]))
        NEW[f"rrc_{fill}_c3c2_{m}"] = (lambda caller, head, fill=fill, mn=mn: std(
            caller, [([rd("CACHE3")] + S(*mn) + [rd("CACHE2"), BOUND()], STOP), ([MSG], STOP)], head,
            parent_extra=[(CALLER[fill], put("CACHE3"))]))


# forwarding middle: PARENT -> MID -> CHILD (no re-cache)
def fwdn(names, u, recache=False):
    def f(caller, head):
        pl = [(CALLER["dyn"], put("CACHE")), (CALLER[caller], put("CACHE2"))]
        cl = ([RECACHE] if recache else []) + [leg_use([rd("CACHE2")] + S(*names), u)] + ([] if recache else [([MSG], STOP)])
        roots = [("PARENT", legs([], *pl, ([], call("MID", wait=True)))),
                 ("MID", legs([], ([], call("CHILD", wait=True)), ([MSG], STOP))), ("CHILD", legs(head, *cl))]
        return roots, legs(head, *(pl + cl))
    return f


for p in ("none", "msg", "map", "conn", "conn_map", "fc"):
    for u in ("bound", "ord", "viab", "dtrb"):
        NEW[f"fn_{p}_{u}"] = fwdn(BETWEEN[p], u)
        NEW[f"fr_{p}_{u}"] = fwdn(BETWEEN[p], u, recache=True)


# a middle that retrieves the caller's cache, steps, and forwards to a waited No Data child that binds X
# (the middle's documents do not reach a No Data child; the child's own retrieve does)
def midret(names, u):
    def f(caller, head):
        pl = [(CALLER["dyn"], put("CACHE")), (CALLER[caller], put("CACHE2"))]
        mid_legs = [([rd("CACHE2")] + S(*names) + [MSG], STOP), ([], call("CHILD", wait=True))]
        cl = [leg_use([rd("CACHE2")] + S(*names), u), ([MSG], STOP)]
        roots = [("PARENT", legs([], *pl, ([], call("MID", wait=True)))), ("MID", legs([], *mid_legs)),
                 ("CHILD", legs(head, *cl))]
        return roots, legs(head, *(pl + mid_legs[:1] + cl))
    return f


for p in ("msg", "conn", "map"):
    for u in ("bound", "ord"):
        NEW[f"mr_{p}_{u}"] = midret(BETWEEN[p], u)

# CONTENT question: typed consumers behind P on the caller's cached documents
CALLER.update({"p2": [GETP2], "p2dyn": [GETP2, DYN_X], "bare": [GET]})
TY_CONSUMERS = {"map": [STEP["map"]], "putp2": [STEP["putp2"]], "mapb": [STEP["map"], BOUND()]}
for p in ("none", "msg", "conn", "connp2", "fc", "ddp", "map", "conn_map"):
    for cons, cs in TY_CONSUMERS.items():
        NEW[f"ty_{p}_{cons}"] = (lambda caller, head, names=BETWEEN[p], cs=cs:
                                 std(caller, [([rd("CACHE2")] + S(*names) + list(cs), STOP), ([MSG], STOP)], head))
        NEW[f"tyr_{p}_{cons}"] = (lambda caller, head, names=BETWEEN[p], cs=cs:
                                  std(caller, [RECACHE, ([rd("CACHE2")] + S(*names) + list(cs), STOP)], head))

FAMILY_CALLERS = {"ty_": ("p2", "bare", "p2dyn", "lit"), "tyr_": ("p2", "bare", "p2dyn", "lit")}


def callers_for(name):
    for pfx, cl in FAMILY_CALLERS.items():
        if name.startswith(pfx):
            return cl
    return ("lit", "none", "dyn")


C.update(NEW)

if __name__ == "__main__":
    arm = os.environ.get("Q184_ARM", "head")
    names = sys.argv[1:] or sorted(NEW)
    out = {}
    for name in names:
        for dialect in ("legacy", "typed"):
            for caller in callers_for(name):
                out[f"{name}|{dialect}|{caller}"] = X.run(name, caller, dialect)
    p = os.path.join(HERE, "out", f"x29_hunt_{arm}.json")
    json.dump(out, open(p, "w"), indent=1, default=str)
    print(arm, len(names), "constructions", len(out), "cells", Counter(v.get("class", "ERROR") for v in out.values()))
