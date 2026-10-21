"""Batch 30 blast radius: replay serialized corpus requests on ONE arm (its own src on sys.path).
Per request: every root's validation verdict under BOTH resolver spellings (None and an empty
ProcessIREffectDeclarationsV1), a digest of the served child rows, and the verdict of each TOP root's
flattened twin (every trailing empty-prefix call into a root inlined, recursively; the batch-29
request-level construction), all on this arm.
Usage: run_arm.py <tree> <req.jsonl> <start> <end> <out.jsonl>"""
import hashlib
import json
import sys

tree, req, start, end, out = sys.argv[1], sys.argv[2], int(sys.argv[3]), int(sys.argv[4]), sys.argv[5]
sys.path.insert(0, tree + "/src")
import boomi_mcp  # noqa: E402

assert boomi_mcp.__file__.startswith(tree), boomi_mcp.__file__
from boomi_mcp.authoring import process_ir_effects as PE  # noqa: E402
from boomi_mcp.compiler.process_ir.contracts import SymbolTableV1  # noqa: E402
from boomi_mcp.compiler.process_ir.semantic_validation.contracts import DEFAULT_VALIDATION_CAPABILITIES  # noqa: E402
from boomi_mcp.compiler.process_ir.semantic_validation.pipeline import validate_process_ir  # noqa: E402
from boomi_mcp.models.authoring_workflow import ProcessIREffectDeclarationsV1  # noqa: E402
from boomi_mcp.models.process_ir import parse_process_ir_v1  # noqa: E402


def verdicts(roots, declarations, syms):
    parsed = [(k, parse_process_ir_v1(d)) for k, d in roots]
    try:
        res = PE.resolve_process_ir_effect_declarations(
            parsed, declarations, syms, [], child_roots={"$ref:" + k: ir for k, ir in parsed})
    except Exception as exc:  # noqa: BLE001
        return "RAISED %s: %s" % (type(exc).__name__, str(exc)[:200]), None
    rows = json.dumps({k: (None if v is None else v.model_dump(mode="json"))
                       for k, v in res.capabilities_by_root.items()}, sort_keys=True, default=str)
    out = {"ok": bool(res.ok), "findings": sorted({getattr(f, "code", str(f)) for f in res.findings})}
    for key, ir in parsed:
        caps = res.capabilities_by_root.get(key) or DEFAULT_VALIDATION_CAPABILITIES
        out[key] = sorted({(e.code, e.path) for e in validate_process_ir(ir, syms, capabilities=caps).errors})
    return out, hashlib.sha256(rows.encode()).hexdigest()[:16]


def _branch(document):
    steps = document.get("body", {}).get("steps", [])
    for index, step in enumerate(steps):
        if step.get("kind") == "branch":
            return steps[:index], step
    return None, None


def flattened(roots, key, depth=0):
    documents = dict(roots)
    prefix, branch = _branch(documents[key])
    if branch is None or depth > 6:
        return None
    legs = list(branch["legs"])
    last = legs[-1]
    terminal = last.get("terminal") or {}
    if terminal.get("kind") != "process_call" or last.get("steps"):
        return None
    callee = terminal.get("process_ref", "")[5:]
    if callee not in documents or callee == key:
        return None
    inner = flattened(roots, callee, depth + 1) or documents[callee]
    cprefix, cbranch = _branch(inner)
    if cbranch is None:
        return None
    body_prefix = [s for s in cprefix if s.get("kind") != "passthrough"]
    own_prefix = [s for s in prefix if s.get("kind") != "passthrough"]
    if body_prefix and own_prefix:
        return None
    entry = [s for s in prefix if s.get("kind") == "passthrough"]
    steps = entry + (own_prefix or body_prefix) + [dict(branch, legs=legs[:-1] + list(cbranch["legs"]))]
    return {"version": documents[key].get("version", "1"), "body": {"kind": "sequence", "steps": steps}}


def calls(document):
    found = []

    def walk(value):
        if isinstance(value, dict):
            if value.get("kind") == "process_call" and isinstance(value.get("process_ref"), str):
                found.append(value["process_ref"][5:])
            for item in value.values():
                walk(item)
        elif isinstance(value, list):
            for item in value:
                walk(item)
    walk(document)
    return found


with open(req) as fh, open(out, "w") as w:
    for index, line in enumerate(fh):
        if index < start:
            continue
        if index >= end:
            break
        rec = json.loads(line)
        roots = [tuple(r) for r in rec["roots"]]
        syms = SymbolTableV1.model_validate(rec["symbols"])
        row = {"id": rec["id"], "legacy": any(
            s.get("kind") == "source" for _k, d in roots for s in d.get("body", {}).get("steps", []))}
        try:
            [parse_process_ir_v1(d) for _k, d in roots]
        except Exception:  # noqa: BLE001
            row["status"] = "parse-refused"
            w.write(json.dumps(row) + "\n")
            continue
        none, rows_none = verdicts(roots, None, syms)
        v1, rows_v1 = verdicts(roots, ProcessIREffectDeclarationsV1(), syms)
        row.update(status="ok", none=none, spellings_equal=(none == v1 and rows_none == rows_v1), rows=rows_none)
        called = {c for _k, d in roots for c in calls(d)}
        tops = [k for k, _d in roots if k not in called]
        twins, complete = {}, True
        for top in tops:
            flat = flattened(roots, top)
            if flat is None or set(calls(flat)) & set(dict(roots)):
                complete = False
                continue
            try:
                got, _ = verdicts([("FLAT", flat)], None, syms)
                twins[top] = got if isinstance(got, str) else sorted({c for c, _p in got["FLAT"]})
            except Exception as exc:  # noqa: BLE001
                twins[top] = "unparseable: " + str(exc)[:100]
                complete = False
        row.update(tops=tops, twins=twins, complete_twin=complete)
        w.write(json.dumps(row, default=str) + "\n")
print("done", req, start, end)
