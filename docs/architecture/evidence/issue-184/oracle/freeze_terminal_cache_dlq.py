"""Freeze the safe replacements for goldens 000005, 000059 and 000060 (#184 amendment 3 §6).

Each retired golden stages a caught document into a DLQ document cache and then wires
the Add to Cache step to a synthetic Stop. Add to Cache emits zero documents
(captures `cap184-cache-put-successor`, `cap184-passthrough-ddp-handoff`), so that
Stop is a dead successor. The corrected shape ends the path ON the cache step.

The expected bytes are a documented transform of the IMMUTABLE pre-change golden:
- remove the cache step's single outgoing dragpoint, leaving `<dragpoints/>`;
- delete the synthetic Stop shape it pointed at.

Nothing else changes. Remaining shape ids and coordinates are preserved, so the
removed Stop's slot stays reserved (amendment 3 §5, synthetic-Stop normalization).
Every substitution is asserted to occur exactly once, and no reference to a removed
shape may remain. The changed emitter supplies no expected bytes.

Run from the repository root.
"""

from __future__ import annotations

import hashlib
import json
import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[5]
MANIFEST = ROOT / "docs/architecture/evidence/issue-184/oracle/terminal_cache_dlq.MANIFEST.json"

#: (retired id, source golden, new golden, [(cache shape, synthetic stop shape), ...])
TRANSFORMS = (
    ("golden-000005",
     "tests/fixtures/golden_xml/connector_scoped_trycatch_notify_dlq_document_cache.xml",
     "tests/fixtures/golden_xml/issue184_cache_notify_connector_terminal.xml",
     (("shape9", "shape10"), ("shape12", "shape13"))),
    ("golden-000059",
     "tests/fixtures/golden_xml/try_catch_notify_dlq_document_cache.xml",
     "tests/fixtures/golden_xml/issue184_cache_notify_terminal.xml",
     (("shape7", "shape8"),)),
    ("golden-000060",
     "tests/fixtures/golden_xml/try_catch_notify_dlq_document_cache_archetype.xml",
     "tests/fixtures/golden_xml/issue184_cache_notify_archetype_terminal.xml",
     (("shape9", "shape10"), ("shape12", "shape13"))),
)


#: executed COMPLETE (R1 admitted) with a terminal Add to Cache leg: the corrected runtime shape
TERMINAL_LOAD_AUTHORITY = "docs/architecture/evidence/issue-184/captures/cap184-retrieve-ddp-replacement/components/r1_ddp_replacement.stored.xml"
_LOAD = re.compile(r'<shape [^>]*shapetype="doccacheload"[^>]*>.*?</shape>', re.S)


def _sha(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def _normalized(shape: str) -> str:
    return re.sub(r' (name|x|y|userlabel|toShape|docCache)="[^"]*"', r' \1="#"', shape)


def terminal_load_checked(text: str) -> int:
    pool = {_normalized(m.group(0)) for m in _LOAD.finditer((ROOT / TERMINAL_LOAD_AUTHORITY).read_text())}
    loads = [m.group(0) for m in _LOAD.finditer(text)]
    for shape in loads:
        if _normalized(shape) not in pool:
            raise SystemExit("a doccacheload shape matches no terminal load in " + TERMINAL_LOAD_AUTHORITY)
    return len(loads)


def transform(text: str, pairs) -> str:
    for cache_shape, stop_shape in pairs:
        dragpoints = re.compile(
            r'(<shape [^>]*name="%s" shapetype="doccacheload"[^>]*>.*?)'
            r'<dragpoints><dragpoint name="%s\.dragpoint1" toShape="%s" x="[0-9.]+" y="[0-9.]+"/></dragpoints>'
            % (cache_shape, cache_shape, stop_shape),
            re.S,
        )
        text, count = dragpoints.subn(r"\1<dragpoints/>", text)
        if count != 1:
            raise SystemExit("expected one outgoing dragpoint on {0}, found {1}".format(cache_shape, count))
        stop = re.compile(
            r'<shape image="stop_icon" name="%s" shapetype="stop" x="[0-9.]+" y="[0-9.]+">'
            r'<configuration><stop continue="true"/></configuration><dragpoints/></shape>' % stop_shape
        )
        text, count = stop.subn("", text)
        if count != 1:
            raise SystemExit("expected one synthetic Stop {0}, found {1}".format(stop_shape, count))
        if 'name="%s"' % stop_shape in text or 'toShape="%s"' % stop_shape in text:
            raise SystemExit("a reference to removed {0} remains".format(stop_shape))
    return text


def main() -> int:
    records = []
    for retired, source, target, pairs in TRANSFORMS:
        before = (ROOT / source).read_bytes()
        after = transform(before.decode("utf-8"), pairs).encode("utf-8")
        (ROOT / target).write_bytes(after)
        records.append({
            "retired": retired,
            "source": source,
            "source_sha256": _sha(before),
            "target": target,
            "target_sha256": _sha(after),
            "target_bytes": len(after),
            "removed": [{"cache_shape": c, "synthetic_stop": s} for c, s in pairs],
            "terminal_loads_matching_authority": terminal_load_checked(after.decode("utf-8")),
        })
        print(retired, "->", target, _sha(after))
    MANIFEST.write_text(json.dumps({
        "purpose": "#184 amendment 3 §6: terminal cache staging replacements for the notify/DLQ goldens",
        "transform": "remove the cache step's single outgoing dragpoint and the synthetic Stop it pointed at; nothing else",
        "script": "docs/architecture/evidence/issue-184/oracle/freeze_terminal_cache_dlq.py",
        "terminal_load_authority": TERMINAL_LOAD_AUTHORITY,
        "terminal_load_authority_sha256": _sha((ROOT / TERMINAL_LOAD_AUTHORITY).read_bytes()),
        "records": records,
        "interpreter": sys.version.split()[0],
    }, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
