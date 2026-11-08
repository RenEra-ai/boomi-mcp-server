"""Freeze the canonical safe replacements for goldens 000012, 000018, 000019 and 000066.

#184 amendment 3 §6. Each retired golden emits a cache step that has a successor. The
platform skips any step with no inbound documents, and Add to Cache and Remove from
Cache emit none (captures `cap184-cache-put-successor`, `cap184-cache-remove-read`,
`cap184-prefix-predecessors`). The replacements end every such path ON the cache step.

Expected bytes come from the PRISTINE branch-point compiler, never from the #184 tree:
the canonical compiler and emitter at `cbab28f`, imported from a detached worktree of
that commit. The script asserts both the import origin and the worktree head. Two
graphs render there directly:
- `cache_stage_read`: a terminal cache write in one Branch leg, and a separately
  triggered cache read in the next;
- `catch_exception_without_cache_put`: 000066's try/catch with the catch-body write
  removed.

The terminal cache REMOVE form does not exist at the branch point. Its graph renders
as `[cache_remove] → stop` in the LAST Branch leg, then one recorded transform removes
the remove step's single outgoing dragpoint and the Stop. That is the same dead-successor
transform the DLQ replacements use. Putting the remove leg last makes that Stop the final
allocated shape, so no shape id or coordinate is renumbered. (Amendment 3's table lists
the remove leg first; the order changes no behaviour, and this placement is what keeps
the transform free of renumbering.)

The authored inputs written beside the goldens use the NEW spelling: a Branch leg whose
terminal is `cache_remove`.

Usage, from the repository root:
    BRANCH_POINT_WORKTREE=<detached worktree of cbab28f> .venv/bin/python docs/architecture/evidence/issue-184/oracle/freeze_terminal_cache_canonical.py
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
MANIFEST = Path(__file__).resolve().parent / "terminal_cache_canonical.MANIFEST.json"

head = subprocess.run(["git", "-C", str(WORKTREE), "rev-parse", "HEAD"], capture_output=True, text=True).stdout.strip()
if head != BRANCH_POINT_SHA:
    raise SystemExit("the branch-point worktree is at {0}, not {1}".format(head, BRANCH_POINT_SHA))
dirty = subprocess.run(["git", "-C", str(WORKTREE), "status", "--porcelain", "--untracked-files=no"],
                       capture_output=True, text=True).stdout.strip()
if dirty:
    raise SystemExit("the branch-point worktree has tracked changes")
for entry in (WORKTREE / "tests" / "patterns", WORKTREE / "tests", WORKTREE / "src"):
    sys.path.insert(0, str(entry))

import _wave_gate_golden_corpus as corpus  # noqa: E402
import boomi_mcp  # noqa: E402
from boomi_mcp.compiler.process_ir.emitter_registry import emit_process  # noqa: E402

for module in (corpus, boomi_mcp):
    if not os.path.realpath(module.__file__).startswith(os.path.realpath(str(WORKTREE)) + os.sep):
        raise SystemExit("{0} was not imported from the branch-point worktree".format(module.__name__))

SRC = {"kind": "source", "connection_ref": "$ref:CONN", "operation_ref": "$ref:GETOP"}
TGT = {"kind": "target", "connection_ref": "$ref:CONN", "operation_ref": "$ref:PATCHOP"}
PUT = {"kind": "cache_put", "cache_ref": "$ref:CACHE"}
GET = {"kind": "cache_get", "cache_ref": "$ref:CACHE"}
REM = {"kind": "cache_remove", "cache_ref": "$ref:CACHE"}
STOP = {"kind": "stop"}


def _doc(*steps):
    return {"version": "1", "body": {"kind": "sequence", "steps": list(steps)}}


def _branch(*legs):
    return {"kind": "branch", "legs": list(legs)}


_CATCH_066 = json.loads((WORKTREE / "tests/fixtures/process_ir/issue154/catch_cache_put_exception.json").read_text())
_CATCH_085 = copy.deepcopy(_CATCH_066)
_CATCH_085["body"]["steps"][0]["catch_body"]["steps"] = []
# the old message said "staged then raised"; nothing is staged any more
_CATCH_085["body"]["steps"][0]["catch_body"]["terminal"]["message_template"] = "caught then raised: {1}"

#: shapetype -> the independent artifact whose stored shape the emitted one must equal, ids and layout aside
AUTHORITIES = {
    # executed COMPLETE (R1 admitted): a terminal Add to Cache leg and a separately triggered retrieve leg
    "doccacheload": "docs/architecture/evidence/issue-184/captures/cap184-retrieve-ddp-replacement/components/r1_ddp_replacement.stored.xml",
    "doccacheretrieve": "docs/architecture/evidence/issue-184/captures/cap184-retrieve-ddp-replacement/components/r1_ddp_replacement.stored.xml",
    # platform-authored terminal all-document removes (#119 census, pre-baseline)
    "doccacheremove": "tests/fixtures/live_xml/m11/process_cache_branch_load_remove.xml",
    # executed control: the catch ends in the Exception alone and the run reads ERROR with the caught message
    "exception": "docs/architecture/evidence/issue-184/captures/cap184-cache-put-successor/components/p3c_catch_exception_control.stored.xml",
}
_SHAPE = re.compile(r'<shape [^>]*shapetype="(%s)"[^>]*>.*?</shape>' % "|".join(AUTHORITIES), re.S)

#: basename -> (retired golden, authored NEW spelling, branch-point render graph, (remove shape, stop shape) or None)
CASES = {
    "cache_remove_terminal_branch": (
        "golden-000012",
        _doc(SRC, _branch({"steps": [], "terminal": TGT}, {"steps": [], "terminal": REM})),
        _doc(SRC, _branch({"steps": [], "terminal": TGT}, {"steps": [REM], "terminal": STOP})),
        ("shape6", "shape7"),
    ),
    "cache_stage_read_remove": (
        "golden-000018",
        _doc(SRC, _branch({"steps": [], "terminal": PUT}, {"steps": [GET], "terminal": TGT},
                          {"steps": [], "terminal": REM})),
        _doc(SRC, _branch({"steps": [], "terminal": PUT}, {"steps": [GET], "terminal": TGT},
                          {"steps": [REM], "terminal": STOP})),
        ("shape8", "shape9"),
    ),
    "cache_stage_read": (
        "golden-000019",
        _doc(SRC, _branch({"steps": [], "terminal": PUT}, {"steps": [GET], "terminal": TGT})),
        _doc(SRC, _branch({"steps": [], "terminal": PUT}, {"steps": [GET], "terminal": TGT})),
        None,
    ),
    "catch_exception_without_cache_put": (
        "golden-000066",
        _CATCH_085,
        _CATCH_085,
        None,
    ),
}


def _sha(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def _normalized(shape: str) -> str:
    shape = re.sub(r' (name|x|y|userlabel|toShape|docCache|title)="[^"]*"', r' \1="#"', shape)
    return re.sub(r"<exMessage>.*?</exMessage>", "<exMessage>#</exMessage>", shape, flags=re.S)


def _authority_checks(text: str):
    checked = []
    for match in _SHAPE.finditer(text):
        kind = match.group(1)
        source = ROOT / AUTHORITIES[kind]
        pool = {_normalized(a.group(0)) for a in _SHAPE.finditer(source.read_text()) if a.group(1) == kind}
        if _normalized(match.group(0)) not in pool:
            raise SystemExit("the emitted {0} shape matches no shape in {1}".format(kind, AUTHORITIES[kind]))
        checked.append({"shapetype": kind, "authority": AUTHORITIES[kind], "authority_sha256": _sha(source.read_bytes())})
    if not checked:
        raise SystemExit("no shape was checked against an authority")
    return checked


def _terminal_remove(text: str, remove_shape: str, stop_shape: str) -> str:
    dragpoints = re.compile(
        r'(<shape [^>]*name="%s" shapetype="doccacheremove"[^>]*>.*?)'
        r'<dragpoints><dragpoint name="%s\.dragpoint1" toShape="%s" x="[0-9.]+" y="[0-9.]+"/></dragpoints>'
        % (remove_shape, remove_shape, stop_shape),
        re.S,
    )
    text, count = dragpoints.subn(r"\1<dragpoints/>", text)
    if count != 1:
        raise SystemExit("expected one outgoing dragpoint on {0}, found {1}".format(remove_shape, count))
    stop = re.compile(
        r'<shape image="stop_icon" name="%s" shapetype="stop" x="[0-9.]+" y="[0-9.]+">'
        r'<configuration><stop continue="true"/></configuration><dragpoints/></shape>' % stop_shape
    )
    text, count = stop.subn("", text)
    if count != 1:
        raise SystemExit("expected one Stop {0}, found {1}".format(stop_shape, count))
    last = re.findall(r'<shape [^>]*name="(shape\d+)"', text)[-1]
    if last != remove_shape:
        raise SystemExit("the removed Stop was not the final shape; the transform would renumber")
    if 'name="%s"' % stop_shape in text or 'toShape="%s"' % stop_shape in text:
        raise SystemExit("a reference to removed {0} remains".format(stop_shape))
    return text


def main() -> int:
    symbols = corpus.error_symbols()
    records = []
    for basename, (retired, authored, render_graph, remove_pair) in CASES.items():
        _cfg, plan = corpus.error_compile(render_graph, symbols)
        xml = emit_process(plan, symbols).process_xml
        rendered = xml if isinstance(xml, str) else xml.decode("utf-8")
        expected = rendered if remove_pair is None else _terminal_remove(rendered, *remove_pair)
        golden = ROOT / "tests/fixtures/golden_xml" / ("issue184_" + basename + ".xml")
        fixture = ROOT / "tests/fixtures/process_ir/issue184" / (basename + ".json")
        golden.write_bytes(expected.encode("utf-8"))
        fixture_bytes = (json.dumps(authored, indent=1, sort_keys=True) + "\n").encode("utf-8")
        fixture.write_bytes(fixture_bytes)
        records.append({
            "retired": retired,
            "basename": basename,
            "branch_point_render_sha256": _sha(rendered.encode("utf-8")),
            "transform": (
                "none" if remove_pair is None else
                "removed {0}'s single outgoing dragpoint and the final Stop {1}".format(*remove_pair)
            ),
            "golden": str(golden.relative_to(ROOT)),
            "golden_sha256": _sha(expected.encode("utf-8")),
            "golden_bytes": len(expected.encode("utf-8")),
            "authority_checks": _authority_checks(expected),
            "fixture": str(fixture.relative_to(ROOT)),
            "fixture_sha256": _sha(fixture_bytes),
        })
        print(retired, "->", golden.name, _sha(expected.encode("utf-8")))
    MANIFEST.write_text(json.dumps({
        "purpose": "#184 amendment 3 §6: canonical terminal cache replacements",
        "branch_point": BRANCH_POINT_SHA,
        "symbols": "error_symbols() at the branch point",
        "renderer": "compiler + emit_process at the branch point (process-xml-v1)",
        "records": records,
        "interpreter": sys.version.split()[0],
    }, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
