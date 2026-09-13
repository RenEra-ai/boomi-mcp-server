"""Freeze the safe replacements for goldens 000005, 000059 and 000060 (#184 amendment 3 §6, ledger C16).

Each retired golden stages a caught document into a DLQ document cache, then wires the
Add to Cache step to a synthetic Stop. Add to Cache emits zero documents (captures
`cap184-cache-put-successor`, `cap184-passthrough-ddp-handoff`), so that Stop is a dead
successor. The corrected shape ends the path ON the cache step.

Expected bytes are a documented transform of the IMMUTABLE pre-change golden, read from a
detached worktree of the branch point. The script asserts the worktree head and its clean
state. Per retired golden, the transform does three things:

1. removes the cache step's single outgoing dragpoint, leaving ``<dragpoints/>``;
2. deletes the synthetic Stop it pointed at;
3. renumbers every later shape down by the number of Stops removed before it — its name,
   its dragpoint names and every ``toShape`` naming it. It also moves the shape's x, and
   its dragpoints' x, to the column of its new number.

Nothing else changes. Every substitution is asserted to occur exactly once, and no
reference to a removed shape may remain.

The column rule is not assumed. Before anything moves, every shape in the source golden
is asserted to sit at ``x = 96 + 160·(n-1)``, and every dragpoint at its shape's x + 144.

Renumbering, rather than reserving the removed slots, is ledger correction C16. It is what
the legacy builder already does after a terminal Process Call (#175). It is also what the
canonical compiler emits for the same graph. For 000005 and 000059 that is cross-checked
here: the ``<shapes>`` result must equal the PRISTINE branch-point canonical compiler's
render of #156's canonical documents for those goldens, with the catch-body cache write
authored as the catch terminal. That slot was already admitted at the branch point. The
archetype golden 000060 has no canonical document and rests on the transform alone. The
changed emitter supplies no expected bytes.

Usage, from the repository root:
    BRANCH_POINT_WORKTREE=<detached worktree of cbab28f> .venv/bin/python docs/architecture/evidence/issue-184/oracle/freeze_terminal_cache_dlq.py
"""

from __future__ import annotations

import copy
import hashlib
import json
import os
import re
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[5]
BRANCH_POINT_SHA = "cbab28ffc176ddb2378283cf7132eb9c37afc374"
WORKTREE = Path(os.environ["BRANCH_POINT_WORKTREE"]).resolve()
MANIFEST = ROOT / "docs/architecture/evidence/issue-184/oracle/terminal_cache_dlq.MANIFEST.json"

#: executed COMPLETE (R1 admitted) with a terminal Add to Cache leg: the corrected runtime shape
TERMINAL_LOAD_AUTHORITY = "docs/architecture/evidence/issue-184/captures/cap184-retrieve-ddp-replacement/components/r1_ddp_replacement.stored.xml"

head = subprocess.run(["git", "-C", str(WORKTREE), "rev-parse", "HEAD"], capture_output=True, text=True).stdout.strip()
if head != BRANCH_POINT_SHA:
    raise SystemExit("the branch-point worktree is at {0}, not {1}".format(head, BRANCH_POINT_SHA))
if subprocess.run(["git", "-C", str(WORKTREE), "status", "--porcelain", "--untracked-files=no"],
                  capture_output=True, text=True).stdout.strip():
    raise SystemExit("the branch-point worktree has tracked changes")
for entry in (WORKTREE / "tests" / "patterns", WORKTREE / "tests", WORKTREE / "src"):
    sys.path.insert(0, str(entry))

import boomi_mcp  # noqa: E402
import test_process_ir_notify_recovery as recovery  # noqa: E402
from boomi_mcp.compiler.process_ir import lowering  # noqa: E402
from boomi_mcp.compiler.process_ir.emitter_registry import emit_process  # noqa: E402
from boomi_mcp.models.process_ir import parse_process_ir_v1  # noqa: E402

for module in (boomi_mcp, recovery):
    if not os.path.realpath(module.__file__).startswith(os.path.realpath(str(WORKTREE)) + os.sep):
        raise SystemExit("{0} was not imported from the branch-point worktree".format(module.__name__))

#: (retired id, source golden, new golden, [(cache shape, synthetic stop shape), ...], canonical document or None)
TRANSFORMS = (
    ("golden-000005",
     "tests/fixtures/golden_xml/connector_scoped_trycatch_notify_dlq_document_cache.xml",
     "tests/fixtures/golden_xml/issue184_cache_notify_connector_terminal.xml",
     (("shape9", "shape10"), ("shape12", "shape13")),
     ("_CHAIN_DLQ_DOCUMENT", "_chain_symbols")),
    ("golden-000059",
     "tests/fixtures/golden_xml/try_catch_notify_dlq_document_cache.xml",
     "tests/fixtures/golden_xml/issue184_cache_notify_terminal.xml",
     (("shape7", "shape8"),),
     ("_NOTIFY_DLQ_DOCUMENT", "_dlq_symbols")),
    ("golden-000060",
     "tests/fixtures/golden_xml/try_catch_notify_dlq_document_cache_archetype.xml",
     "tests/fixtures/golden_xml/issue184_cache_notify_archetype_terminal.xml",
     (("shape9", "shape10"), ("shape12", "shape13")),
     None),
)

_SHAPE_X0, _COLUMN, _DRAGPOINT_DX = 96.0, 160.0, 144.0
_LOAD = re.compile(r'<shape [^>]*shapetype="doccacheload"[^>]*>.*?</shape>', re.S)


def _sha(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def _column(number: int) -> float:
    return _SHAPE_X0 + _COLUMN * (number - 1)


def _assert_column_rule(text: str) -> None:
    for number, x in re.findall(r'<shape [^>]*name="shape(\d+)"[^>]*\bx="([0-9.]+)"', text):
        if float(x) != _column(int(number)):
            raise SystemExit("shape{0} is not at its column (x={1})".format(number, x))
    for number, x in re.findall(r'<dragpoint [^>]*name="shape(\d+)\.dragpoint\d+"[^>]*\bx="([0-9.]+)"', text):
        if float(x) != _column(int(number)) + _DRAGPOINT_DX:
            raise SystemExit("a dragpoint of shape{0} is not at its column (x={1})".format(number, x))


def _remove_dead_wires(text: str, pairs) -> str:
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


def _renumber(text: str, removed) -> tuple:
    removed_numbers = sorted(int(stop[len("shape"):]) for _cache, stop in removed)

    def new_number(old: int) -> int:
        return old - sum(1 for gone in removed_numbers if gone < old)

    mapping = {}

    def shape_tag(match):
        attrs = match.group(1)
        old = int(re.search(r'name="shape(\d+)"', attrs).group(1))
        new = new_number(old)
        if new != old:
            mapping["shape%d" % old] = "shape%d" % new
            attrs = re.sub(r'name="shape\d+"', 'name="shape%d"' % new, attrs, count=1)
            attrs = re.sub(r'\bx="[0-9.]+"', 'x="%s"' % _column(new), attrs, count=1)
        return "<shape %s>" % attrs

    def dragpoint_tag(match):
        attrs = match.group(1)
        owner, point = re.search(r'name="shape(\d+)\.dragpoint(\d+)"', attrs).groups()
        owner_new = new_number(int(owner))
        target = int(re.search(r'toShape="shape(\d+)"', attrs).group(1))
        attrs = re.sub(r'toShape="shape\d+"', 'toShape="shape%d"' % new_number(target), attrs, count=1)
        if owner_new != int(owner):
            attrs = re.sub(r'name="shape\d+\.dragpoint\d+"', 'name="shape%d.dragpoint%s"' % (owner_new, point), attrs, count=1)
            attrs = re.sub(r'\bx="[0-9.]+"', 'x="%s"' % (_column(owner_new) + _DRAGPOINT_DX), attrs, count=1)
        return "<dragpoint %s/>" % attrs

    text = re.sub(r"<shape ([^>]*)>", shape_tag, text)
    text = re.sub(r"<dragpoint ([^>]*)/>", dragpoint_tag, text)
    return text, mapping


def _shapes(xml: str) -> str:
    return re.search(r"<shapes>.*</shapes>", xml, re.S).group(0)


def _canonical_shapes(document_name: str, symbols_name: str) -> str:
    document = copy.deepcopy(getattr(recovery, document_name))
    for step in document["body"]["steps"]:
        if step.get("kind") != "try_catch":
            continue
        body = step["catch_body"]
        staged = body["steps"].pop()
        if staged["kind"] != "cache_put":
            raise SystemExit("{0}: the catch body does not end in a cache write".format(document_name))
        body["terminal"] = staged
    symbols = getattr(recovery, symbols_name)()
    plan = lowering.lower_cfg_to_emission_plan(lowering.lower_process_ir_to_cfg(parse_process_ir_v1(document)), symbols)
    return _shapes(emit_process(plan, symbols).process_xml)


def _normalized_load(shape: str) -> str:
    return re.sub(r' (name|x|y|userlabel|toShape|docCache)="[^"]*"', r' \1="#"', shape)


def main() -> int:
    authority = (ROOT / TERMINAL_LOAD_AUTHORITY).read_text()
    load_pool = {_normalized_load(m.group(0)) for m in _LOAD.finditer(authority)}
    records = []
    for retired, source, target, pairs, canonical in TRANSFORMS:
        before = (WORKTREE / source).read_bytes()
        text = before.decode("utf-8")
        _assert_column_rule(text)
        text = _remove_dead_wires(text, pairs)
        text, mapping = _renumber(text, pairs)
        _assert_column_rule(text)
        after = text.encode("utf-8")
        for shape in _LOAD.findall(text):
            if _normalized_load(shape) not in load_pool:
                raise SystemExit("{0}: a terminal Add to Cache matches no stored terminal load".format(retired))
        cross_check = "none: no canonical document for this route"
        if canonical is not None:
            if _canonical_shapes(*canonical) != _shapes(text):
                raise SystemExit("{0}: the transform disagrees with the branch-point canonical render".format(retired))
            cross_check = "shapes byte-equal to the branch-point canonical render of tests/test_process_ir_notify_recovery.py::{0} with the catch cache write as the catch terminal".format(canonical[0])
        (ROOT / target).write_bytes(after)
        records.append({
            "retired": retired,
            "source": source,
            "source_sha256": _sha(before),
            "target": target,
            "target_sha256": _sha(after),
            "target_bytes": len(after),
            "removed": [{"cache_shape": c, "synthetic_stop": s} for c, s in pairs],
            "renumbered": mapping,
            "terminal_loads_matching_authority": len(_LOAD.findall(text)),
            "canonical_cross_check": cross_check,
        })
        print(retired, "->", target, _sha(after), "renumbered", mapping)
    MANIFEST.write_text(json.dumps({
        "purpose": "#184 amendment 3 §6 and ledger C16: terminal cache staging replacements for the notify/DLQ goldens",
        "branch_point": BRANCH_POINT_SHA,
        "source_goldens_read_from": "the branch-point worktree",
        "transform": (
            "remove the cache step's single outgoing dragpoint and the synthetic Stop it pointed at; renumber "
            "later shapes down past the removed Stops, moving their x and their dragpoints' x to the new column "
            "(x = 96 + 160*(n-1), dragpoint x = shape x + 144, asserted on the source first)"
        ),
        "script": "docs/architecture/evidence/issue-184/oracle/freeze_terminal_cache_dlq.py",
        "terminal_load_authority": TERMINAL_LOAD_AUTHORITY,
        "terminal_load_authority_sha256": _sha((ROOT / TERMINAL_LOAD_AUTHORITY).read_bytes()),
        "records": records,
        "interpreter": sys.version.split()[0],
    }, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
