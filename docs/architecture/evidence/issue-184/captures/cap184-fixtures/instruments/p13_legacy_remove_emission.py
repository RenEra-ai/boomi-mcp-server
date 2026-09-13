"""E0 #184 (offline, branch-point legacy code, NO network): does the shipped legacy builder emit
doccacheremove / doccacheload with a FORWARD successor? Pure builder call; prints the shape chain and the
remove/load shapes' dragpoints. Positive control: the same config with transform.mode=message must render a
forward message shape (proves the chain reader sees successors at all)."""
import os, sys, re, copy, json
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import lib184 as L
L.srv()
from boomi_mcp.categories.components.builders.process_flow_builder import ProcessFlowBuilder
import boomi_mcp.categories.components.builders.process_flow_builder as PFB
assert os.path.realpath(PFB.__file__).startswith(os.path.realpath(L.WT))

base = {"process_kind": "database_to_api_sync",
        "source": {"connector_type": "database", "connection_id": "9a2e06c1-e4ac-42b3-9db2-31991f9113fd",
                   "operation_id": "cacbde23-8bbf-4ba4-8595-356c53e423e5", "action_type": "Get"},
        "target": {"connector_type": "rest", "connection_id": L.REST_CONN,
                   "operation_id": "f58b2b52-0000-0000-0000-000000000000", "action_type": "POST"}}


def chain(xml):
    shapes = {m.group(1): m.group(0) for m in re.finditer(r'<shape [^>]*name="(shape\d+)".*?</shape>', xml, re.S)}
    out = []
    for name, body in shapes.items():
        st = re.search(r'shapetype="([a-z]+)"', body).group(1)
        to = re.findall(r'toShape="([^"]+)"', body)
        out.append((name, st, to))
    return out


res = {}
for mode, extra in (("message", {"message_text": "x"}), ("doccacheremove", {"document_cache_id": "0370d8d8-2c63-42d7-ae11-9aa5bbf64262"}),
                     ("doccacheretrieve", {"document_cache_id": "0370d8d8-2c63-42d7-ae11-9aa5bbf64262"})):
    cfg = copy.deepcopy(base)
    cfg["transform"] = dict({"mode": mode}, **extra)
    err = ProcessFlowBuilder.validate_config(copy.deepcopy(cfg)) if hasattr(ProcessFlowBuilder, "validate_config") else None
    try:
        xml = ProcessFlowBuilder().build(copy.deepcopy(cfg), name=f"_offline_{mode}")
    except TypeError:
        xml = ProcessFlowBuilder.build(copy.deepcopy(cfg), name=f"_offline_{mode}")
    except Exception as e:
        print(mode, "BUILD RAISED", type(e).__name__, str(e)[:300], "validate:", err)
        continue
    c = chain(xml)
    res[mode] = c
    print(f"== mode={mode} validate_err={getattr(err, 'error_code', err)}")
    for name, st, to in c:
        print(f"   {name:8} {st:18} -> {to}")
tr = {m: [(st, to) for _, st, to in c if st in ("message", "doccacheremove", "doccacheretrieve")] for m, c in res.items()}
print("control (message has a forward successor):", bool(tr.get("message") and tr["message"][0][1]))
print("doccacheremove forward successor:", tr.get("doccacheremove"))
