"""#186 blast radius, part 1: every ACTIVE golden rendered on ONE arm, hashed.

Usage: goldens.py <arm-tree> <manifest goldens.jsonl> <out.json>
Runs in a child with the arm's own src and tests first on sys.path (asserted)."""
import hashlib
import json
import sys

tree, manifest, out = sys.argv[1], sys.argv[2], sys.argv[3]
sys.path[:0] = [tree + "/src", tree + "/tests"]
import boomi_mcp  # noqa: E402

assert boomi_mcp.__file__.startswith(tree), boomi_mcp.__file__
import _wave_gate_golden_corpus as corpus  # noqa: E402

assert corpus.__file__.startswith(tree), corpus.__file__
rows = [json.loads(line) for line in open(manifest, encoding="utf-8")]
results = {}
for row in rows:
    if row.get("kind") != "golden" or row.get("state") != "active":
        continue
    try:
        rendered = corpus.render_golden_case(row["input_case"], row["renderer"])
        data = rendered if isinstance(rendered, bytes) else str(rendered).encode("utf-8")
        expected = open(tree + "/" + row["expected_file"], "rb").read()
        results[row["id"]] = {"sha256": hashlib.sha256(data).hexdigest(), "len": len(data),
                              "matches_expected": data == expected}
    except Exception as exc:  # noqa: BLE001
        results[row["id"]] = {"raised": "%s: %s" % (type(exc).__name__, str(exc)[:300])}
json.dump(results, open(out, "w"), indent=1, sort_keys=True)
print(len(results), "goldens;", sum(1 for r in results.values() if r.get("matches_expected")), "byte-identical to the committed file")
