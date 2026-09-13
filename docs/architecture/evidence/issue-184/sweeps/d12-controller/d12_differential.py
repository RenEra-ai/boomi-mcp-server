"""#184 D12 refactor differential: the recursive lineage walker vs the iterative one.

Usage, one run per tree, then a compare:
  PYTHONPATH=<tree>/src:<tree>/tests:<tree>/tests/patterns python d12_differential.py dump <tree> <out.jsonl>
  python d12_differential.py compare <old.jsonl> <new.jsonl>

`dump` walks every committed ProcessIR fixture under tests/fixtures/process_ir (any
JSON document that parses as ProcessIRV1) under each corpus symbol table, plus two
generated long cases, and records for each the walk's findings, required reads,
guaranteed writes, and — where the tree still has it — the truncation flag.

`compare` requires exact equality wherever the old walk did not truncate, and on
every truncated old walk requires the new walk to be a superset of its findings (the
old walk stopped short, so it may only have MISSED findings, never found extra).
"""
import json
import sys
from pathlib import Path


def _docs(tree):
    # ALL committed fixtures: ProcessIR documents also live outside `process_ir/`
    # (measured: 38 documents in 34 files across `tests/fixtures`, against 27 found
    # under `process_ir/` alone).
    base = Path(tree) / "tests" / "fixtures"
    for path in sorted(base.rglob("*.json")):
        try:
            data = json.loads(path.read_text(encoding="utf-8"))
        except Exception:
            continue
        # RECURSIVE discovery: a first revision looked only at the top level and one
        # level down, and measured 27 of the 38 committed documents.
        stack = [(str(path.relative_to(tree)), data)]
        while stack:
            label, value = stack.pop()
            if isinstance(value, dict):
                if value.get("version") == "1" and isinstance(value.get("body"), dict):
                    yield label, value
                    continue
                for key in sorted(value, reverse=True):
                    stack.append(("{0}#{1}".format(label, key), value[key]))
            elif isinstance(value, list):
                for index in range(len(value) - 1, -1, -1):
                    stack.append(("{0}#{1}".format(label, index), value[index]))


def _long_cases():
    filler = [{"kind": "set_dpp", "name": "F%d" % i,
               "source_values": [{"value_type": "static", "value": "v"}]} for i in range(300)]
    yield "generated#linear_300_late_read", {"version": "1", "body": {"kind": "sequence", "steps": [
        {"kind": "source", "connection_ref": "$ref:CONN", "operation_ref": "$ref:GETOP"}]
        + filler + [{"kind": "set_dpp", "name": "OUT",
                     "source_values": [{"value_type": "dpp", "property_name": "LATE"}]},
                    {"kind": "return_documents"}]}}
    yield "generated#linear_300_late_profile", {"version": "1", "body": {"kind": "sequence", "steps": [
        {"kind": "connector_call", "operation_ref": "$ref:op_rest_get"}]
        + filler + [{"kind": "set_ddp", "name": "X", "source_values": [
            {"value_type": "profile", "profile_ref": "prof_soap_in", "profile_type": "xml",
             "element_id": "3", "element_name": "id"}]},
            {"kind": "connector_call", "operation_ref": "$ref:op_rest_patch"}, {"kind": "stop"}]}}


def dump(tree, out):
    import _wave_gate_golden_corpus as corpus
    from boomi_mcp.compiler.process_ir.contracts import SymbolTableV1
    from boomi_mcp.compiler.process_ir.semantic_validation.context import prepare_validation_context
    from boomi_mcp.compiler.process_ir.semantic_validation.lineage import walk_lineage
    from boomi_mcp.models.process_ir import parse_process_ir_v1

    tables = [("empty", SymbolTableV1(symbols=()))]
    for name in ("error_symbols", "rich_symbols", "issue155_symbols"):
        factory = getattr(corpus, name, None)
        if factory is not None:
            tables.append((name, factory()))

    rows = 0
    with open(out, "w", encoding="utf-8") as handle:
        for label, doc in list(_docs(tree)) + list(_long_cases()):
            try:
                ir = parse_process_ir_v1(doc)
            except Exception as exc:  # not a valid document: skip, but count
                handle.write(json.dumps({"doc": label, "parse": type(exc).__name__}) + "\n")
                continue
            for table_name, table in tables:
                record = {"doc": label, "symbols": table_name}
                try:
                    walk = walk_lineage(prepare_validation_context(ir, table))
                except Exception as exc:
                    record["walk_error"] = type(exc).__name__
                    record["codes"] = [getattr(d, "code", None) for d in getattr(exc, "diagnostics", [])]
                else:
                    record["findings"] = sorted(
                        [f.code, f.path, f.severity, f.phase] for f in walk.findings)
                    record["unestablished_reads"] = [list(k) for k in walk.unestablished_reads]
                    record["established_at_exit"] = [list(k) for k in walk.established_at_exit]
                    record["truncated"] = bool(getattr(walk, "truncated", False))
                handle.write(json.dumps(record, sort_keys=True) + "\n")
                rows += 1
    print("rows", rows)


def compare(old_path, new_path):
    def load(path):
        out = {}
        for line in open(path, encoding="utf-8"):
            row = json.loads(line)
            out[(row["doc"], row.get("symbols"))] = row
        return out

    old, new = load(old_path), load(new_path)
    assert set(old) == set(new), sorted(set(old) ^ set(new))[:10]
    equal = truncated = superset = 0
    mismatches = []
    for key in sorted(old):
        a, b = old[key], new[key]
        if "findings" not in a or "findings" not in b:
            if a.get("walk_error") != b.get("walk_error") or a.get("parse") != b.get("parse"):
                mismatches.append((key, "error", a, b))
            else:
                equal += 1
            continue
        if a["truncated"]:
            truncated += 1
            if set(map(tuple, a["findings"])) <= set(map(tuple, b["findings"])):
                superset += 1
            else:
                mismatches.append((key, "truncated_old_not_subset", a, b))
            continue
        same = all(a[field] == b[field] for field in ("findings", "unestablished_reads", "established_at_exit"))
        if same:
            equal += 1
        else:
            mismatches.append((key, "differs", a, b))
    report = {"compared": len(old), "equal": equal, "old_truncated": truncated,
              "old_truncated_new_superset": superset, "mismatches": len(mismatches)}
    print(json.dumps(report, indent=2))
    for key, why, a, b in mismatches[:20]:
        print(why, key)
        print("  old", json.dumps(a)[:600])
        print("  new", json.dumps(b)[:600])
    return 0 if not mismatches else 1


if __name__ == "__main__":
    if sys.argv[1] == "dump":
        dump(sys.argv[2], sys.argv[3])
    else:
        raise SystemExit(compare(sys.argv[2], sys.argv[3]))
