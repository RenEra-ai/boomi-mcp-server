"""Freeze the safe replacement for golden-000072 (#184 amendment 2 §5).

golden-000072 rendered a first writer that reads a PROFILE element before anything
produces a document. Under a scheduled start the entry document is the empty No Data
document, so that read addresses nothing, and #184 refuses it. Its replacement keeps
the scheduled source role and composes the path from a run-supplied dynamic process
property instead — the exact spine the platform executed green in capture
`cap155-e1-source-dynamic-path`.

Provenance class: live capture of an artifact PROVEN OPERABLE (executed COMPLETE, one
inbound and two outbound documents, the counterparty log recording the composed GET
path). The expected bytes are cut from the platform's STORED process — never rendered
through the compiler under test:

1. slice `<shapes>…</shapes>` out of the archived `stored_process.xml`;
2. substitute the platform component ids with the corpus symbol ids, per shape, each
   substitution asserted to occur exactly once in its shape;
3. wrap the slice as `<process xmlns="">…</process>`, the #155 canonical-row
   convention (`tests/fixtures/process_ir/issue155/PROVENANCE.md`).

The authored fixture is written from the literal below. Its field names come from the
same capture: a static segment plus a `dpp` segment named `key` with an empty default
(`processpropertydefaultvalue=""` in the stored shape), and a path binding naming the
property with no request profile (the stored GET carries no `parameter-profile`).

Run from the repository root: `python docs/architecture/evidence/issue-184/oracle/freeze_source_dynamic_path_dpp.py`.
"""

from __future__ import annotations

import hashlib
import json
import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[5]
CAPTURE = ROOT / "docs/architecture/evidence/issue-155/captures/cap155-e1-source-dynamic-path/stored_process.xml"
GOLDEN = ROOT / "tests/fixtures/golden_xml/issue184_source_dynamic_path_dpp.xml"
FIXTURE = ROOT / "tests/fixtures/process_ir/issue184/source_dynamic_path_dpp.json"
MANIFEST = Path(__file__).resolve().parent / "source_dynamic_path_dpp.MANIFEST.json"

#: Platform id -> corpus symbol id, per stored shape. The capture used ONE connection
#: for both calls; `issue155_symbols()` binds the GET to `RCONN` and the PATCH to
#: `CONN-UUID`, so the connection substitution is per shape, not global.
SUBSTITUTIONS = {
    "shape3": (
        ('connectionId="c4281346-83e9-4026-856d-ede718ec68a0"', 'connectionId="RCONN"'),
        ('operationId="547becc5-c291-4f6a-a739-a4c845ad4493"', 'operationId="ROP"'),
    ),
    "shape4": (
        ('connectionId="c4281346-83e9-4026-856d-ede718ec68a0"', 'connectionId="CONN-UUID"'),
        ('operationId="5717df63-0e55-4d9a-ab87-54cb193c8fd2"', 'operationId="OP-UUID"'),
    ),
}

AUTHORED = {
    "version": "1",
    "body": {
        "kind": "sequence",
        "steps": [
            {
                "kind": "set_ddp",
                "name": "DDP_PATH_CLIENTS",
                "source_values": [
                    {"value_type": "static", "value": "/admin/cdscm/api/v1/clients/"},
                    {"value_type": "dpp", "property_name": "key", "default_value": ""},
                ],
            },
            {
                "kind": "connector_call",
                "operation_ref": "$ref:ROP",
                "path_binding": {"property_name": "DDP_PATH_CLIENTS"},
            },
            {"kind": "connector_call", "operation_ref": "$ref:OP"},
            {"kind": "stop"},
        ],
    },
}


def _sha(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def main() -> int:
    captured = CAPTURE.read_bytes()
    text = captured.decode("utf-8")
    start, end = text.index("<shapes>"), text.index("</shapes>") + len("</shapes>")
    pieces = []
    applied = []
    for piece in re.split(r"(?=<shape )", text[start:end]):
        name = re.match(r'<shape [^>]*name="(shape\d+)"', piece)
        for old, new in SUBSTITUTIONS.get(name.group(1) if name else "", ()):
            if piece.count(old) != 1:
                raise SystemExit("substitution {0!r} is not unique in {1}".format(old, name.group(1)))
            piece = piece.replace(old, new)
            applied.append({"shape": name.group(1), "from": old, "to": new})
        pieces.append(piece)
    if len(applied) != sum(len(v) for v in SUBSTITUTIONS.values()):
        raise SystemExit("not every recorded substitution applied")
    expected = ('<process xmlns="">' + "".join(pieces) + "</process>").encode("utf-8")

    fixture = (json.dumps(AUTHORED, indent=1, sort_keys=True) + "\n").encode("utf-8")
    GOLDEN.write_bytes(expected)
    FIXTURE.write_bytes(fixture)
    MANIFEST.write_text(json.dumps({
        "purpose": "golden-000072 replacement survivor (#184 amendment 2 §5)",
        "capture": str(CAPTURE.relative_to(ROOT)),
        "capture_sha256": _sha(captured),
        "capture_execution": "execution-b91fb002-0a98-4e51-b9fb-ad503ea01241-2026.08.26 (COMPLETE, 1 in / 2 out)",
        "shapes_slice_sha256": _sha(text[start:end].encode("utf-8")),
        "substitutions": applied,
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
