"""Unit tests for the process-graph integrity verifier (issue #80, M9.4).

Each fixture isolates one acceptance-criteria condition. Tests assert the exact
error/warning codes, that warning-only fixtures produce no errors, that the
valid fixture is fully clean, and that the verifier never raises (malformed XML
is reported, not thrown).

Run with PYTHONPATH=src (the editable install .pth is stale):
    PYTHONPATH=src pytest tests/test_process_graph_verifier.py
"""

from pathlib import Path

import pytest

from boomi_mcp.categories.components.process_graph_verifier import verify_process_graph

_FIXTURES = Path(__file__).parent / "fixtures" / "process_graph"


def _load(name: str) -> str:
    return (_FIXTURES / name).read_text(encoding="utf-8")


def _codes(issues):
    return {i["code"] for i in issues}


def test_valid_linear_process_is_clean():
    result = verify_process_graph(_load("valid_linear_process.xml"))
    assert result["errors"] == [], result["errors"]
    assert result["warnings"] == [], result["warnings"]
    assert result["shapes_checked"] == 3


def test_orphan_unreachable_shape():
    result = verify_process_graph(_load("orphan_unreachable_shape.xml"))
    assert "SHAPE_UNREACHABLE" in _codes(result["errors"])
    # The unreachable connectoraction has a valid outbound edge, so it must not
    # also be flagged as a dead end — only unreachability.
    assert "NON_TERMINAL_SHAPE_DEAD_END" not in _codes(result["errors"])
    assert result["warnings"] == [], result["warnings"]
    # The offending shape is named in the finding.
    unreachable = [e for e in result["errors"] if e["code"] == "SHAPE_UNREACHABLE"]
    assert unreachable[0]["shape"] == "shape3"


def test_dangling_to_shape():
    result = verify_process_graph(_load("dangling_to_shape.xml"))
    codes = _codes(result["errors"])
    assert "DRAGPOINT_TO_SHAPE_UNRESOLVED" in codes
    # The shape keeps a valid edge to the stop, so it is not a dead end.
    assert "NON_TERMINAL_SHAPE_DEAD_END" not in codes
    assert "SHAPE_UNREACHABLE" not in codes
    assert result["warnings"] == [], result["warnings"]


def test_branch_output_unset():
    result = verify_process_graph(_load("branch_output_unset.xml"))
    codes = _codes(result["errors"])
    assert "BRANCH_OUTPUT_UNSET" in codes
    assert "NON_TERMINAL_SHAPE_DEAD_END" not in codes
    assert "BRANCH_NUM_BRANCHES_MISMATCH" not in _codes(result["warnings"])


def test_non_terminal_no_outbound():
    result = verify_process_graph(_load("non_terminal_no_outbound.xml"))
    codes = _codes(result["errors"])
    assert "NON_TERMINAL_SHAPE_DEAD_END" in codes
    dead = [e for e in result["errors"] if e["code"] == "NON_TERMINAL_SHAPE_DEAD_END"]
    assert dead[0]["shape"] == "shape2"
    # The shape carries an (empty) <dragpoints/> element, so no missing-element lint.
    assert "DRAGPOINTS_ELEMENT_MISSING" not in _codes(result["warnings"])


def test_stop_missing_continue_is_error():
    """Issue #102 C1: a bare <stop/> with no continue= is a runtime NPE — it is
    now a hard error (was a warning under #80)."""
    result = verify_process_graph(_load("stop_missing_continue.xml"))
    assert "STOP_CONTINUE_MISSING" in _codes(result["errors"])
    assert "STOP_CONTINUE_MISSING" not in _codes(result["warnings"])


def test_branch_numbranches_mismatch_is_warning_only():
    result = verify_process_graph(_load("branch_numbranches_mismatch.xml"))
    assert result["errors"] == [], result["errors"]
    assert "BRANCH_NUM_BRANCHES_MISMATCH" in _codes(result["warnings"])


def test_missing_dragpoints_element_is_warning_only():
    result = verify_process_graph(_load("missing_dragpoints_element.xml"))
    assert result["errors"] == [], result["errors"]
    assert "DRAGPOINTS_ELEMENT_MISSING" in _codes(result["warnings"])


def test_missing_display_attrs_is_warning_only():
    result = verify_process_graph(_load("missing_display_attrs.xml"))
    assert result["errors"] == [], result["errors"]
    warn_codes = _codes(result["warnings"])
    assert "DISPLAY_ATTRIBUTE_MISSING" in warn_codes


def test_exception_terminal_is_clean():
    """A process ending in a terminal Exception step (empty <dragpoints/>) must
    verify clean — Exception terminates execution and is not a dead end."""
    result = verify_process_graph(_load("exception_terminal_process.xml"))
    assert result["errors"] == [], result["errors"]
    assert result["warnings"] == [], result["warnings"]
    assert result["shapes_checked"] == 3


def test_duplicate_shape_name_is_error():
    """Two shapes sharing a name make the graph ambiguous and must not pass
    clean — the index would otherwise collapse them and mask wiring problems."""
    xml = (
        '<process xmlns=""><shapes>'
        '<shape image="start" name="shape1" shapetype="start" x="1" y="1">'
        '<configuration><noaction/></configuration>'
        '<dragpoints><dragpoint name="d1" toShape="shape2" x="2" y="2"/></dragpoints></shape>'
        '<shape image="connectoraction_icon" name="shape2" shapetype="connectoraction" x="2" y="1">'
        '<configuration/><dragpoints><dragpoint name="d2" toShape="shape3" x="3" y="2"/></dragpoints></shape>'
        '<shape image="connectoraction_icon" name="shape2" shapetype="connectoraction" x="2" y="3">'
        '<configuration/><dragpoints/></shape>'
        '<shape image="stop_icon" name="shape3" shapetype="stop" x="3" y="1">'
        '<configuration><stop continue="true"/></configuration><dragpoints/></shape>'
        "</shapes></process>"
    )
    result = verify_process_graph(xml)
    codes = _codes(result["errors"])
    assert "DUPLICATE_SHAPE_NAME" in codes
    dup = [e for e in result["errors"] if e["code"] == "DUPLICATE_SHAPE_NAME"]
    assert dup[0]["shape"] == "shape2"


def test_missing_shape_name_is_error():
    """A shape with no name cannot be referenced or reached; flag it."""
    xml = (
        '<process xmlns=""><shapes>'
        '<shape image="start" name="shape1" shapetype="start" x="1" y="1">'
        '<configuration><noaction/></configuration>'
        '<dragpoints><dragpoint name="d1" toShape="shape2" x="2" y="2"/></dragpoints></shape>'
        '<shape image="stop_icon" name="shape2" shapetype="stop" x="2" y="1">'
        '<configuration><stop continue="true"/></configuration><dragpoints/></shape>'
        '<shape image="stop_icon" shapetype="stop" x="3" y="1">'
        '<configuration><stop continue="true"/></configuration><dragpoints/></shape>'
        "</shapes></process>"
    )
    result = verify_process_graph(xml)
    assert "SHAPE_NAME_MISSING" in _codes(result["errors"])


def test_return_docs_into_stop_is_error():
    """Issue #102 C2a: Return Documents and Stop are mutually exclusive
    terminals — a Return-Documents shape wired into a Stop is a hard error."""
    xml = (
        '<process xmlns=""><shapes>'
        '<shape image="start" name="shape1" shapetype="start" x="1" y="1">'
        '<configuration><noaction/></configuration>'
        '<dragpoints><dragpoint name="d1" toShape="shape2" x="2" y="2"/></dragpoints></shape>'
        '<shape image="returndocuments_icon" name="shape2" shapetype="returndocuments" x="2" y="1">'
        '<configuration/>'
        '<dragpoints><dragpoint name="d2" toShape="shape3" x="3" y="2"/></dragpoints></shape>'
        '<shape image="stop_icon" name="shape3" shapetype="stop" x="3" y="1">'
        '<configuration><stop continue="true"/></configuration><dragpoints/></shape>'
        "</shapes></process>"
    )
    result = verify_process_graph(xml)
    codes = _codes(result["errors"])
    assert "RETURN_DOCS_STOP_EXCLUSIVE" in codes
    rd = [e for e in result["errors"] if e["code"] == "RETURN_DOCS_STOP_EXCLUSIVE"]
    assert rd[0]["shape"] == "shape2"


def test_terminal_return_documents_is_clean():
    """A Return Documents used as a proper terminal (no outbound edge) is clean
    — C2a only flags the Return-Documents -> Stop wiring."""
    xml = (
        '<process xmlns=""><shapes>'
        '<shape image="start" name="shape1" shapetype="start" x="1" y="1">'
        '<configuration><noaction/></configuration>'
        '<dragpoints><dragpoint name="d1" toShape="shape2" x="2" y="2"/></dragpoints></shape>'
        '<shape image="returndocuments_icon" name="shape2" shapetype="returndocuments" x="2" y="1">'
        '<configuration/><dragpoints/></shape>'
        "</shapes></process>"
    )
    result = verify_process_graph(xml)
    assert "RETURN_DOCS_STOP_EXCLUSIVE" not in _codes(result["errors"])
    assert result["errors"] == [], result["errors"]


def test_control_branch_bare_stop_is_warning():
    """Issue #102 C2b: a Decision/Route/Try-Catch branch wired straight into a
    Stop drops rejected documents untraceably — a warning (intentional drops are
    legal), never a hard error that would block emission."""
    xml = (
        '<process xmlns=""><shapes>'
        '<shape image="start" name="shape1" shapetype="start" x="1" y="1">'
        '<configuration><noaction/></configuration>'
        '<dragpoints><dragpoint name="d1" toShape="shape2" x="2" y="2"/></dragpoints></shape>'
        '<shape image="decision_icon" name="shape2" shapetype="decision" x="2" y="1">'
        '<configuration/>'
        '<dragpoints>'
        '<dragpoint name="d2t" toShape="shape3" x="3" y="2"/>'
        '<dragpoint name="d2f" toShape="shape4" x="3" y="3"/>'
        "</dragpoints></shape>"
        '<shape image="stop_icon" name="shape3" shapetype="stop" x="3" y="1">'
        '<configuration><stop continue="true"/></configuration><dragpoints/></shape>'
        '<shape image="stop_icon" name="shape4" shapetype="stop" x="3" y="3">'
        '<configuration><stop continue="true"/></configuration><dragpoints/></shape>'
        "</shapes></process>"
    )
    result = verify_process_graph(xml)
    assert "CONTROL_BRANCH_BARE_STOP" in _codes(result["warnings"])
    # Advisory only — it must never block emission.
    assert "CONTROL_BRANCH_BARE_STOP" not in _codes(result["errors"])


def test_malformed_xml_reported_not_raised():
    result = verify_process_graph("<process><shapes><shape></shapes>")  # unbalanced
    assert "PROCESS_XML_PARSE_FAILED" in _codes(result["errors"])
    assert result["shapes_checked"] == 0


def test_empty_xml_reported_not_raised():
    result = verify_process_graph("   ")
    assert "PROCESS_XML_EMPTY" in _codes(result["errors"])
    assert result["shapes_checked"] == 0


def test_no_process_element_reported():
    result = verify_process_graph(
        '<bns:Component xmlns:bns="http://api.platform.boomi.com/" type="connector-settings"/>'
    )
    assert "PROCESS_GRAPH_NOT_FOUND" in _codes(result["errors"])


def test_missing_start_shape_reported():
    xml = (
        '<process xmlns=""><shapes>'
        '<shape image="stop_icon" name="shape1" shapetype="stop" x="1" y="1">'
        '<configuration><stop continue="true"/></configuration><dragpoints/></shape>'
        "</shapes></process>"
    )
    result = verify_process_graph(xml)
    assert "PROCESS_START_MISSING" in _codes(result["errors"])


def test_bare_process_root_is_supported():
    """The raw escape hatch where the root element is itself <process>."""
    xml = (
        '<process xmlns=""><shapes>'
        '<shape image="start" name="shape1" shapetype="start" userlabel="" x="1" y="1">'
        '<configuration><noaction/></configuration>'
        '<dragpoints><dragpoint name="shape1.dragpoint1" toShape="shape2" x="2" y="2"/></dragpoints>'
        "</shape>"
        '<shape image="stop_icon" name="shape2" shapetype="stop" x="3" y="1">'
        '<configuration><stop continue="true"/></configuration><dragpoints/></shape>'
        "</shapes></process>"
    )
    result = verify_process_graph(xml)
    assert result["errors"] == [], result["errors"]
    assert result["warnings"] == [], result["warnings"]
    assert result["shapes_checked"] == 2
