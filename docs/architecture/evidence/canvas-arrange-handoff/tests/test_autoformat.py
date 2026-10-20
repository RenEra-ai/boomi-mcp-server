"""Emulator vs. Boomi: every recorded Arrange result must be reproduced exactly."""
import re
import sys
import xml.etree.ElementTree as ET
from collections import Counter
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "lab"))
from boomi_autoformat import UnarrangeableProcess, autoformat_xml, compare  # noqa: E402
from boomi_autoformat import layout  # noqa: E402
from boomi_autoformat.model import local, parse  # noqa: E402
import evaluate  # noqa: E402

PAIRS = [p for p in evaluate.pairs() if p[4]]
IDS = [f"{g}:{n}" for g, n, *_ in PAIRS]


# ------------------------------------------------------------------ recorded Boomi results

@pytest.mark.parametrize("group,name,before,after,_", PAIRS, ids=IDS)
def test_matches_boomi_arrange(group, name, before, after, _):
    assert evaluate.check(group, name, before, after) == []


def test_recorded_corpus_is_complete():
    counts = Counter(g for g, *_ in PAIRS)
    assert counts["lab"] == evaluate.EXPECTED_COUNTS["lab"]


@pytest.mark.parametrize("group,name,before,after,_", PAIRS, ids=IDS)
def test_idempotent(group, name, before, after, _):
    """Arranging twice moves nothing. Unset dragpoints are excluded on purpose: their height follows
    the shape's position before Arrange, in Boomi as in the emulator."""
    once = autoformat_xml(before.read_text(encoding="utf-8"))
    assert compare(autoformat_xml(once), once, include_unset=False) == []


_COORD = re.compile(r"""(\s[xy]\s*=\s*)("[^"]*"|'[^']*')""")


def _mask_coordinates(text):
    return _COORD.sub(r'\1"#"', text)


@pytest.mark.parametrize("group,name,before,after,_", PAIRS, ids=IDS)
def test_bytes_outside_coordinates_unchanged(group, name, before, after, _):
    """Byte-level: declaration, comments, quoting, whitespace and line ends all survive."""
    with open(before, encoding="utf-8", newline="") as f:
        src = f.read()
    assert _mask_coordinates(autoformat_xml(src)) == _mask_coordinates(src)


def _without_coordinates(xml_text):
    """Canonical tree with shape/dragpoint x and y removed."""
    root = ET.fromstring(xml_text.encode("utf-8"))
    for el in root.iter():
        if isinstance(el.tag, str) and local(el.tag) in ("shape", "dragpoint"):
            el.attrib.pop("x", None)
            el.attrib.pop("y", None)
    return ET.tostring(root)


@pytest.mark.parametrize("group,name,before,after,_", PAIRS, ids=IDS)
def test_only_coordinates_change(group, name, before, after, _):
    src = before.read_text(encoding="utf-8")
    assert _without_coordinates(autoformat_xml(src)) == _without_coordinates(src)


def test_cli_check(tmp_path):
    from boomi_autoformat.__main__ import main
    before, after = next((b, a) for g, n, b, a, _ in PAIRS if n == "b03")
    out = tmp_path / "out.xml"
    assert main([str(before), "-o", str(out), "--check", str(after)]) == 0
    assert out.exists()


def test_unreachable_cycle_is_rejected_like_boomi():
    src = (ROOT / "lab" / "before" / "p17_orphan_cycle.saved.xml").read_text(encoding="utf-8")
    with pytest.raises(UnarrangeableProcess):
        autoformat_xml(src)


# ------------------------------------------------------------------ review findings (Codex, 2026-09-30)

def proc(shapes: str) -> str:
    return f"<process><shapes>{shapes}</shapes></process>"


START = '<shape name="s" shapetype="start" x="1" y="2"><dragpoints><dragpoint name="s.d1" toShape="t" x="0" y="0"/></dragpoints></shape>'
STOP = '<shape name="t" shapetype="stop" x="5" y="6"><dragpoints/></shape>'


def xy(xml_text, shape):
    s = next(s for s in parse(xml_text) if s.name == shape)
    return s.x, s.y


def test_label_that_looks_like_coordinates_is_left_alone():
    src = proc("<shape userlabel=' x=\"bad\" y=\"bad\"' name=\"s\" shapetype=\"start\" x=\"1\" y=\"2\"/>")
    out = autoformat_xml(src)
    assert "userlabel=' x=\"bad\" y=\"bad\"'" in out
    assert xy(out, "s") == (48.0, 46.0)


def test_shape_markup_inside_cdata_and_comments_is_left_alone():
    fake = '<shape name="t" x="7" y="8"/>'
    src = proc(START + "<!-- " + fake + " -->" + STOP.replace("<dragpoints/>", f"<![CDATA[{fake}]]><dragpoints/>"))
    out = autoformat_xml(src)
    assert out.count(fake) == 2
    assert xy(out, "t") == (240.0, 48.0)


@pytest.mark.parametrize("variant", [
    ('name="s"', 'name="s&amp;x"', "s&x"),                           # entity in the name
    ('name="s" shapetype="start" x="1" y="2"', "name='s' shapetype='start' x='1' y='2'", "s"),  # single quotes
    ('x="1" y="2"', 'x = "1"  y= "2"', "s"),                          # spaces around =
    ('shapetype="start"', 'userlabel="a > b" shapetype="start"', "s"),  # unescaped > in a value
])
def test_valid_xml_spellings_are_rewritten(variant):
    old, new, shape = variant
    src = proc(START.replace(old, new).replace('toShape="t"', 'toShape="t"') + STOP)
    out = autoformat_xml(src)
    assert xy(out, shape) == (48.0, 46.0)
    assert xy(out, "t") == (240.0, 48.0)


def test_missing_coordinates_are_inserted():
    """Emulator contract, not recorded from Boomi: absent x/y count as 0 and are written."""
    src = proc('<shape name="s" shapetype="start"><dragpoints><dragpoint name="d" toShape="unset"/></dragpoints></shape>')
    out = autoformat_xml(src)
    s = parse(out)[0]
    assert (s.x, s.y) == (48.0, 46.0)
    assert (s.dragpoints[0].x, s.dragpoints[0].y) == (224.0, 10.0)


def test_namespaced_process():
    src = f'<process xmlns="urn:boomi"><shapes>{START}{STOP}</shapes></process>'
    out = autoformat_xml(src)
    assert xy(out, "t") == (240.0, 48.0)


def test_compare_reports_expected_only_and_extra_shapes():
    one = proc(STOP)
    two = proc(STOP + '<shape name="u" shapetype="stop" x="5" y="6"/>')
    assert [d[0] for d in compare(one, two)] == ["missing"]
    assert [d[0] for d in compare(two, one)] == ["extra"]


def test_compare_reports_missing_dragpoint():
    a = proc('<shape name="s" shapetype="start" x="1" y="2"><dragpoints/></shape>')
    b = proc('<shape name="s" shapetype="start" x="1" y="2"><dragpoints><dragpoint name="d" toShape="unset" x="2" y="3"/></dragpoints></shape>')
    assert [d[0] for d in compare(a, b)] == ["topology"]


def test_compare_matches_dragpoints_by_position_not_target():
    def two_to_t(x1):
        return proc('<shape name="s" shapetype="branch" x="1" y="2"><dragpoints>'
                    f'<dragpoint name="a" toShape="t" x="{x1}" y="3"/><dragpoint name="b" toShape="t" x="2" y="3"/>'
                    "</dragpoints></shape>" + STOP)
    diffs = compare(two_to_t(2), two_to_t(999))
    assert [(d[0], d[1]) for d in diffs] == [("dragpoint", "s[0]")]


def test_single_output_to_missing_shape_is_rejected_like_boomi():
    src = proc(START.replace('toShape="t"', 'toShape="gone"'))
    with pytest.raises(UnarrangeableProcess):
        autoformat_xml(src)


def test_multi_output_to_missing_shape_is_treated_as_unconnected():
    src = proc('<shape name="s" shapetype="start" x="1" y="2"><dragpoints/></shape>'.replace("<dragpoints/>", "")
               .replace("</shape>", '<dragpoints><dragpoint name="s.d1" toShape="t" x="0" y="0"/></dragpoints></shape>')
               + '<shape name="t" shapetype="decision" x="0" y="100"><dragpoints>'
               '<dragpoint name="t.d1" toShape="gone" x="0" y="0"/><dragpoint name="t.d2" toShape="u" x="0" y="0"/>'
               '</dragpoints></shape><shape name="u" shapetype="stop" x="0" y="0"/>')
    out = autoformat_xml(src)
    assert xy(out, "u") == (432.0, 208.0)            # second outcome still opens row 1
    d = parse(out)[1].dragpoints[0]
    assert (d.x, d.y) == (416.0, 86.0)               # unset rule: round16(100)=96, + 10 - 20


def test_no_start_leaves_the_process_unchanged():
    """Read from Boomi's Ike (returns when there is no start), not seen in the UI."""
    src = proc('<shape name="a" shapetype="stop" x="1" y="2"/>')
    assert autoformat_xml(src) == src


def test_several_starts_last_one_wins():
    """Read from Boomi's graph builder (Kke), not seen in the UI."""
    src = proc(START.replace('name="s"', 'name="s0"').replace('toShape="t"', 'toShape="unset"')
               + START + STOP)
    out = autoformat_xml(src)
    assert xy(out, "s") == (48.0, 46.0)
    assert xy(out, "s0") == (48.0, 206.0)            # an unreachable root on the next row


def test_duplicate_dragpoint_names_in_different_shapes():
    src = proc(START.replace('name="s.d1"', 'name="d"')
               + STOP.replace("<dragpoints/>", '<dragpoints><dragpoint name="d" toShape="u" x="0" y="0"/></dragpoints>')
               .replace('shapetype="stop"', 'shapetype="message"')
               + '<shape name="u" shapetype="stop" x="0" y="0"/>')
    out = autoformat_xml(src)
    s, t, _ = parse(out)
    assert (s.dragpoints[0].x, s.dragpoints[0].y) == (224.0, 56.0)
    assert (t.dragpoints[0].x, t.dragpoints[0].y) == (416.0, 56.0)


def test_duplicate_shape_names_are_rejected():
    with pytest.raises(ValueError):
        autoformat_xml(proc(STOP + STOP))


def test_evaluate_fails_on_a_wrong_result(tmp_path, monkeypatch):
    before = ROOT / "lab" / "before" / "p01_linear.saved.xml"
    bad = tmp_path / "p01_linear.saved.xml"
    bad.write_text((ROOT / "lab" / "after" / "p01_linear.saved.xml").read_text(encoding="utf-8")
                   .replace('x="432.0"', 'x="433.0"', 1), encoding="utf-8")
    monkeypatch.setattr(evaluate, "pairs", lambda: iter([("lab", "p01_linear", before, bad, True)]))
    assert evaluate.main([]) == 1


# ------------------------------------------------------------------ review findings (Grok, 2026-09-30)

def assert_canvas_consistent(src):
    """The shapes model.parse reads are exactly the shapes xmlpatch rewrites."""
    pos, dps = layout.arrange(parse(src))
    for s in parse(autoformat_xml(src)):
        assert (s.x, s.y) == pos[s.name]
        for k, d in enumerate(s.dragpoints):
            assert (d.x, d.y) == dps[(s.name, k)]


def test_only_the_first_process_canvas_changes():
    second = '<process><shapes><shape name="s" shapetype="start" x="9" y="9"/></shapes></process>'
    src = f"<list>{proc(START + STOP)}{second}</list>"
    out = autoformat_xml(src)
    assert out.endswith(second + "</list>")
    assert_canvas_consistent(src)


def test_process_markup_nested_in_a_shape_is_left_alone():
    inner = '<process><shapes><shape name="s" x="5" y="5"/></shapes></process>'
    src = proc('<shape name="s" shapetype="start" x="1" y="2"><configuration>' + inner + '</configuration>'
               '<dragpoints><dragpoint name="d" toShape="unset" x="0" y="0"/></dragpoints></shape>')
    out = autoformat_xml(src)
    assert inner in out
    d = parse(out)[0].dragpoints[0]
    assert (d.x, d.y) == (224.0, 10.0)
    assert_canvas_consistent(src)


def test_nested_process_before_the_canvas_is_chosen_consistently():
    src = ('<process><description><process><shapes><shape name="a" shapetype="start" x="3" y="3"/></shapes>'
           '</process></description><shapes>' + START + STOP + '</shapes></process>')
    assert_canvas_consistent(src)


def test_bare_shapes_root_is_arranged():
    src = '<shapes>' + START.replace('toShape="t"', 'toShape="unset"') + '</shapes>'
    out = autoformat_xml(src)
    assert xy(out, "s") == (48.0, 46.0)
    assert_canvas_consistent(src)


@pytest.mark.parametrize("raw", ["a\tb", "a\nb", "a\r\nb"])
def test_raw_whitespace_in_a_name_is_matched_like_the_parser(raw):
    src = proc(START.replace('name="s"', f'name="{raw}"').replace('toShape="t"', 'toShape="unset"'))
    shape = parse(src)[0].name
    assert xy(autoformat_xml(src), shape) == (48.0, 46.0)


@pytest.mark.parametrize("src", [
    '<!DOCTYPE process [<!ENTITY nm "s">]>' + proc('<shape name="&nm;" shapetype="start" x="1" y="2"/>'),
    '<?xml version="1.0"?><!-- c --><!DOCTYPE l [<!ENTITY a "aaaaaaaaaa"><!ENTITY b "&a;&a;&a;&a;&a;&a;&a;&a;">]>'
    + proc('<shape name="&b;" shapetype="start" x="1" y="2"/>'),
])
def test_doctype_is_rejected(src):
    with pytest.raises(ValueError, match="DOCTYPE"):
        autoformat_xml(src)


def test_doctype_text_inside_cdata_is_fine():
    src = proc(START.replace("<dragpoints>", "<configuration><![CDATA[<!DOCTYPE html>]]></configuration><dragpoints>") + STOP)
    assert "<![CDATA[<!DOCTYPE html>]]>" in autoformat_xml(src)


def test_cli_keeps_crlf_and_reports_invalid_xml(tmp_path):
    from boomi_autoformat.__main__ import main
    src = tmp_path / "in.xml"
    src.write_bytes(proc(START + STOP).replace("<shapes>", "<shapes>\r\n").replace("</shapes>", "\r\n</shapes>").encode())
    out = tmp_path / "out.xml"
    assert main([str(src), "-o", str(out)]) == 0
    assert out.read_bytes().count(b"\r\n") == 2
    bad = tmp_path / "bad.xml"
    bad.write_text("<process><shapes>", encoding="utf-8")
    assert main([str(bad)]) == 3


def test_evaluate_fails_when_a_corpus_is_short(monkeypatch):
    monkeypatch.setattr(evaluate, "pairs", lambda: iter([]))
    assert evaluate.main([]) == 1
    assert evaluate.main(["--allow-missing"]) == 0
