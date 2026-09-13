"""Freeze the canonical A8 golden's expected bytes from the pre-baseline legacy oracle.

#184 A8 requires the canonical spelling of the mapped both-sides dynamic path to
emit `<shapes>` byte-identical to the legacy renderer's. The expected bytes are cut
from the oracle frozen BEFORE the step-0 baseline
(`tests/fixtures/golden_xml/dynamic_path_both_sides.xml`, golden-000079). They are
never taken from the canonical compiler, whose output is the thing under test.

Transform (the #155 canonical-row convention): slice `<shapes>…</shapes>` out of the
frozen component, then wrap it as `<process xmlns="">…</process>`. There is no id
substitution: the oracle's config names the connection, operation, map and profile
ids the canonical symbol table uses (`RCONN`/`ROP`, `CONN-UUID`/`OP-UUID`, `MAP-UUID`,
`PROFILE-REQUEST`).

The authored fixture is written from the literal below. The spelling is the attested
design plan's (`.codex/plans/issue-184.md` §3):
`set_ddp → connector_call{path_binding} → map_ref → set_ddp → connector_call{path_binding} → stop`,
with the target-side writer after the map.

Run from the repository root: `python docs/architecture/evidence/issue-184/oracle/freeze_both_sides_canonical.py`.
"""

from __future__ import annotations

import hashlib
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[5]
ORACLE = ROOT / "tests/fixtures/golden_xml/dynamic_path_both_sides.xml"
ORACLE_SHA256 = "633a197f3b67a0773a828acb378451d64183ca63a8c5ef60ddb095bbc053aede"
GOLDEN = ROOT / "tests/fixtures/golden_xml/issue184_both_sides_dynamic_path.xml"
FIXTURE = ROOT / "tests/fixtures/process_ir/issue184/both_sides_dynamic_path.json"
MANIFEST = Path(__file__).resolve().parent / "both_sides_dynamic_path.MANIFEST.json"

AUTHORED = {
    "version": "1",
    "body": {
        "kind": "sequence",
        "steps": [
            {
                "kind": "set_ddp",
                "name": "DDP_PATH_SOURCE",
                "source_values": [
                    {"value_type": "static", "value": "/v1/customers/"},
                    {"value_type": "dpp", "property_name": "seed_id", "default_value": ""},
                ],
            },
            {
                "kind": "connector_call",
                "operation_ref": "$ref:ROP",
                "path_binding": {"property_name": "DDP_PATH_SOURCE"},
            },
            {"kind": "map_ref", "map_ref": "$ref:MAP"},
            {
                "kind": "set_ddp",
                "name": "DDP_PATH_TARGET",
                "source_values": [
                    {"value_type": "static", "value": "/v1/requests/"},
                    {
                        "value_type": "profile",
                        "profile_ref": "$ref:PREQ",
                        "profile_type": "profile.json",
                        "element_id": "3",
                        "element_name": "requestId (Root/Object/requestId)",
                    },
                ],
            },
            {
                "kind": "connector_call",
                "operation_ref": "$ref:OP",
                "path_binding": {
                    "property_name": "DDP_PATH_TARGET",
                    "request_profile_ref": "$ref:PREQ",
                },
            },
            {"kind": "stop"},
        ],
    },
}


def _sha(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def main() -> int:
    oracle = ORACLE.read_bytes()
    if _sha(oracle) != ORACLE_SHA256:
        raise SystemExit("the frozen oracle's bytes changed — refuse to derive from it")
    text = oracle.decode("utf-8")
    start, end = text.index("<shapes>"), text.index("</shapes>") + len("</shapes>")
    shapes = text[start:end]
    expected = ('<process xmlns="">' + shapes + "</process>").encode("utf-8")
    fixture = (json.dumps(AUTHORED, indent=1, sort_keys=True) + "\n").encode("utf-8")
    GOLDEN.write_bytes(expected)
    FIXTURE.write_bytes(fixture)
    MANIFEST.write_text(json.dumps({
        "purpose": "#184 A8 canonical golden, cut from the pre-baseline legacy oracle",
        "oracle": str(ORACLE.relative_to(ROOT)),
        "oracle_sha256": ORACLE_SHA256,
        "shapes_slice_sha256": _sha(shapes.encode("utf-8")),
        "substitutions": [],
        "wrapper": "<process xmlns=\"\"> + shapes + </process>",
        "golden": str(GOLDEN.relative_to(ROOT)),
        "golden_sha256": _sha(expected),
        "golden_bytes": len(expected),
        "fixture": str(FIXTURE.relative_to(ROOT)),
        "fixture_sha256": _sha(fixture),
        "interpreter": sys.version.split()[0],
    }, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print(_sha(expected), len(expected))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
