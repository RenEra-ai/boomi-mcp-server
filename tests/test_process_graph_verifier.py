"""Unit tests for the process-graph integrity verifier (issue #80, M9.4).

Each fixture isolates one acceptance-criteria condition. Tests assert the exact
error/warning codes, that warning-only fixtures produce no errors, that the
valid fixture is fully clean, and that the verifier never raises (malformed XML
is reported, not thrown).

Run with PYTHONPATH=src (the editable install .pth is stale):
    PYTHONPATH=src pytest tests/test_process_graph_verifier.py
"""

import re
from pathlib import Path

import pytest

from boomi_mcp.categories.components.builders.process_flow_builder import ProcessFlowBuilder
from boomi_mcp.categories.components.process_graph_verifier import verify_process_graph

_FIXTURES = Path(__file__).parent / "fixtures" / "process_graph"


def _branch_process_xml(num_extra_legs: int = 1) -> str:
    """Build a real Branch fan-out process via ProcessFlowBuilder (issue #112)."""
    legs = [
        {
            "connector_type": "rest",
            "connection_id": f"5555555{i}-5555-5555-5555-555555555555",
            "operation_id": f"6666666{i}-6666-6666-6666-666666666666",
            "action_type": "PUT",
        }
        for i in range(num_extra_legs)
    ]
    cfg = {
        "process_kind": "database_to_api_sync",
        "source": {"connector_type": "database", "action_type": "Get",
                   "connection_id": "11111111-1111-1111-1111-111111111111",
                   "operation_id": "22222222-2222-2222-2222-222222222222"},
        "target": {"connector_type": "rest", "action_type": "POST",
                   "connection_id": "33333333-3333-3333-3333-333333333333",
                   "operation_id": "44444444-4444-4444-4444-444444444444"},
        "branch": {"enabled": True, "targets": legs},
    }
    return ProcessFlowBuilder.build(cfg, name="Branch Fanout")


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


def _doccacheretrieve_process_xml() -> str:
    """Build a real linear Document Cache Retrieve process via ProcessFlowBuilder
    (issue #109 M10.5): start -> source -> doccacheretrieve -> target -> stop."""
    cfg = {
        "process_kind": "database_to_api_sync",
        "source": {"connector_type": "database", "action_type": "Get",
                   "connection_id": "11111111-1111-1111-1111-111111111111",
                   "operation_id": "22222222-2222-2222-2222-222222222222"},
        "transform": {"mode": "doccacheretrieve",
                      "document_cache_id": "8540619c-9f1e-4832-9b1a-5128c399aa52",
                      "label": "Get From Cache"},
        "target": {"connector_type": "rest", "action_type": "POST",
                   "connection_id": "33333333-3333-3333-3333-333333333333",
                   "operation_id": "44444444-4444-4444-4444-444444444444"},
    }
    return ProcessFlowBuilder.build(cfg, name="Cache Retrieve Sync")


def test_doccacheretrieve_wired_is_clean():
    """Issue #109 M10.5: a wired Document Cache Retrieve (a forward edge to the
    next shape) is a normal linear NON-terminal step and must verify fully
    clean — it is not classified terminal/branching, so its forward edge passes."""
    result = verify_process_graph(_doccacheretrieve_process_xml())
    assert result["errors"] == [], result["errors"]
    assert result["warnings"] == [], result["warnings"]
    # start, source connectoraction, doccacheretrieve, target connectoraction, stop
    assert result["shapes_checked"] == 5


def test_doccacheretrieve_zero_outbound_is_dead_end():
    """Issue #109 M10.5: a Document Cache Retrieve with no outbound edge is a
    NON_TERMINAL_SHAPE_DEAD_END — it is NOT a terminal shape (unlike
    doccacheload/returndocuments/exception, which are clean with empty
    <dragpoints/>), so an unwired retrieve must be flagged."""
    xml = (
        '<process xmlns=""><shapes>'
        '<shape image="start" name="shape1" shapetype="start" x="1" y="1">'
        '<configuration><noaction/></configuration>'
        '<dragpoints><dragpoint name="d1" toShape="shape2" x="2" y="2"/></dragpoints></shape>'
        '<shape image="doccacheretrieve_icon" name="shape2" shapetype="doccacheretrieve" x="2" y="1">'
        '<configuration><doccacheretrieve docCache="CACHE-1" emptyCacheBehavior="stopprocess" loadAllDoc="true"><cacheKeyValues/></doccacheretrieve></configuration>'
        '<dragpoints/></shape>'
        "</shapes></process>"
    )
    result = verify_process_graph(xml)
    codes = _codes(result["errors"])
    assert "NON_TERMINAL_SHAPE_DEAD_END" in codes
    dead = [e for e in result["errors"] if e["code"] == "NON_TERMINAL_SHAPE_DEAD_END"]
    assert dead[0]["shape"] == "shape2"


def _doccacheremove_process_xml() -> str:
    """Build a real linear Document Cache Remove process via ProcessFlowBuilder
    (issue #110 M10.6): start -> source -> doccacheremove -> target -> stop."""
    cfg = {
        "process_kind": "database_to_api_sync",
        "source": {"connector_type": "database", "action_type": "Get",
                   "connection_id": "11111111-1111-1111-1111-111111111111",
                   "operation_id": "22222222-2222-2222-2222-222222222222"},
        "transform": {"mode": "doccacheremove",
                      "document_cache_id": "8540619c-9f1e-4832-9b1a-5128c399aa52",
                      "label": "Clear Cache"},
        "target": {"connector_type": "rest", "action_type": "POST",
                   "connection_id": "33333333-3333-3333-3333-333333333333",
                   "operation_id": "44444444-4444-4444-4444-444444444444"},
    }
    return ProcessFlowBuilder.build(cfg, name="Cache Remove Sync")


def test_doccacheremove_wired_is_clean():
    """Issue #110 M10.6: a wired Document Cache Remove (a forward edge to the next
    shape) is a normal linear NON-terminal step and must verify fully clean — it is
    not classified terminal/branching, so its forward edge passes (mirrors the #109
    retrieve verifier behavior; the issue locks the verifier as a linear cache op)."""
    result = verify_process_graph(_doccacheremove_process_xml())
    assert result["errors"] == [], result["errors"]
    assert result["warnings"] == [], result["warnings"]
    # start, source connectoraction, doccacheremove, target connectoraction, stop
    assert result["shapes_checked"] == 5


def test_doccacheremove_zero_outbound_is_dead_end():
    """Issue #110 M10.6: a Document Cache Remove with no outbound edge is a
    NON_TERMINAL_SHAPE_DEAD_END — per #110 the builder shape is a linear
    non-terminal (NOT classified terminal like doccacheload/returndocuments/
    exception), so an unwired remove must be flagged."""
    xml = (
        '<process xmlns=""><shapes>'
        '<shape image="start" name="shape1" shapetype="start" x="1" y="1">'
        '<configuration><noaction/></configuration>'
        '<dragpoints><dragpoint name="d1" toShape="shape2" x="2" y="2"/></dragpoints></shape>'
        '<shape image="doccacheremove_icon" name="shape2" shapetype="doccacheremove" x="2" y="1">'
        '<configuration><doccacheremove docCache="CACHE-1" removeAllDocuments="true"><cacheKeyValues/></doccacheremove></configuration>'
        '<dragpoints/></shape>'
        "</shapes></process>"
    )
    result = verify_process_graph(xml)
    codes = _codes(result["errors"])
    assert "NON_TERMINAL_SHAPE_DEAD_END" in codes
    dead = [e for e in result["errors"] if e["code"] == "NON_TERMINAL_SHAPE_DEAD_END"]
    assert dead[0]["shape"] == "shape2"


def _flow_control_process_xml() -> str:
    """Build a real linear Flow Control process via ProcessFlowBuilder
    (issue #111 M10.7): start -> source -> flowcontrol -> target -> stop."""
    cfg = {
        "process_kind": "database_to_api_sync",
        "source": {"connector_type": "database", "action_type": "Get",
                   "connection_id": "11111111-1111-1111-1111-111111111111",
                   "operation_id": "22222222-2222-2222-2222-222222222222"},
        "flow_control": {"enabled": True, "for_each_count": 10, "label": "Batch by 10"},
        "target": {"connector_type": "rest", "action_type": "POST",
                   "connection_id": "33333333-3333-3333-3333-333333333333",
                   "operation_id": "44444444-4444-4444-4444-444444444444"},
    }
    return ProcessFlowBuilder.build(cfg, name="Flow Control Sync")


def test_flow_control_wired_is_clean():
    """Issue #111 M10.7: a wired Flow Control shape (a forward edge to the next
    shape) is a normal linear NON-terminal step and must verify fully clean — it is
    not classified terminal/branching/control-branch, so its single forward edge
    passes (mirrors the #109/#110 cache-op verifier behavior)."""
    result = verify_process_graph(_flow_control_process_xml())
    assert result["errors"] == [], result["errors"]
    assert result["warnings"] == [], result["warnings"]
    # start, source connectoraction, flowcontrol, target connectoraction, stop
    assert result["shapes_checked"] == 5


def test_flow_control_zero_outbound_is_dead_end():
    """Issue #111 M10.7: a Flow Control with no outbound edge is a
    NON_TERMINAL_SHAPE_DEAD_END — the builder shape is a linear non-terminal (NOT
    classified terminal like doccacheload/returndocuments/exception), so an unwired
    Flow Control must be flagged."""
    xml = (
        '<process xmlns=""><shapes>'
        '<shape image="start" name="shape1" shapetype="start" x="1" y="1">'
        '<configuration><noaction/></configuration>'
        '<dragpoints><dragpoint name="d1" toShape="shape2" x="2" y="2"/></dragpoints></shape>'
        '<shape image="flowcontrol_icon" name="shape2" shapetype="flowcontrol" x="2" y="1">'
        '<configuration><flowcontrol chunkStyle="threadOnly" chunks="0" forEachCount="10"/></configuration>'
        '<dragpoints/></shape>'
        "</shapes></process>"
    )
    result = verify_process_graph(xml)
    codes = _codes(result["errors"])
    assert "NON_TERMINAL_SHAPE_DEAD_END" in codes
    dead = [e for e in result["errors"] if e["code"] == "NON_TERMINAL_SHAPE_DEAD_END"]
    assert dead[0]["shape"] == "shape2"


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


def test_return_docs_reaches_stop_via_intermediate_is_error():
    """Issue #102 C2a (Codex review): Return Documents reaching a Stop downstream
    via an intervening shape (returndocuments -> message -> stop) still uses both
    terminal mechanisms — flagged by reachability, not just a direct edge."""
    xml = (
        '<process xmlns=""><shapes>'
        '<shape image="start" name="shape1" shapetype="start" x="1" y="1">'
        '<configuration><noaction/></configuration>'
        '<dragpoints><dragpoint name="d1" toShape="shape2" x="2" y="2"/></dragpoints></shape>'
        '<shape image="returndocuments_icon" name="shape2" shapetype="returndocuments" x="2" y="1">'
        '<configuration/>'
        '<dragpoints><dragpoint name="d2" toShape="shape3" x="3" y="2"/></dragpoints></shape>'
        '<shape image="message_icon" name="shape3" shapetype="message" x="3" y="1">'
        '<configuration/>'
        '<dragpoints><dragpoint name="d3" toShape="shape4" x="4" y="2"/></dragpoints></shape>'
        '<shape image="stop_icon" name="shape4" shapetype="stop" x="4" y="1">'
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


def test_terminal_shape_with_outbound_edge_is_error():
    """An always-terminal shape that carries an outbound dragpoint is malformed.

    `returndocuments -> message -> exception` reaches no Stop, so the #102 C2a
    reachability check stays silent, yet Return Documents ends the path and must
    not have an outbound edge — the verifier must flag it on its own.
    """
    xml = (
        '<process xmlns=""><shapes>'
        '<shape image="start" name="shape1" shapetype="start" x="1" y="1">'
        '<configuration><noaction/></configuration>'
        '<dragpoints><dragpoint name="d1" toShape="shape2" x="2" y="2"/></dragpoints></shape>'
        '<shape image="returndocuments_icon" name="shape2" shapetype="returndocuments" x="2" y="1">'
        '<configuration/>'
        '<dragpoints><dragpoint name="d2" toShape="shape3" x="3" y="2"/></dragpoints></shape>'
        '<shape image="message_icon" name="shape3" shapetype="message" x="3" y="1">'
        '<configuration/>'
        '<dragpoints><dragpoint name="d3" toShape="shape4" x="4" y="2"/></dragpoints></shape>'
        '<shape image="exception_icon" name="shape4" shapetype="exception" x="4" y="1">'
        '<configuration/><dragpoints/></shape>'
        "</shapes></process>"
    )
    result = verify_process_graph(xml)
    codes = _codes(result["errors"])
    assert "TERMINAL_SHAPE_HAS_OUTBOUND" in codes
    assert "RETURN_DOCS_STOP_EXCLUSIVE" not in codes  # no downstream Stop reached
    bad = [e for e in result["errors"] if e["code"] == "TERMINAL_SHAPE_HAS_OUTBOUND"]
    assert bad[0]["shape"] == "shape2"


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


def test_builder_branch_fanout_is_clean():
    """Issue #112 M10.8: a builder-emitted Branch fan-out passes the graph verifier
    with zero errors AND zero warnings — numBranches matches the dragpoint count
    (no BRANCH_NUM_BRANCHES_MISMATCH), every dragpoint has a real toShape (no
    BRANCH_OUTPUT_UNSET / dangling edge)."""
    for extra_legs in (1, 3):
        result = verify_process_graph(_branch_process_xml(extra_legs))
        assert result["errors"] == [], (extra_legs, result["errors"])
        assert result["warnings"] == [], (extra_legs, result["warnings"])


def test_branch_to_stop_legs_do_not_trigger_control_branch_bare_stop():
    """Issue #112 M10.8: Branch legs legitimately end in a Stop, so a Branch wired
    to Stops must NOT raise CONTROL_BRANCH_BARE_STOP — Branch is deliberately kept
    out of _CONTROL_BRANCH_SHAPE_TYPES (unlike Decision/Route/Try-Catch, where a
    rejected-document path into a bare Stop drops documents untraceably)."""
    result = verify_process_graph(_branch_process_xml(1))
    assert "CONTROL_BRANCH_BARE_STOP" not in _codes(result["warnings"])
    assert "CONTROL_BRANCH_BARE_STOP" not in _codes(result["errors"])


def _dataprocess_process_xml(step, label="DP") -> str:
    """Build a real Data Process (Split/Combine) process via ProcessFlowBuilder."""
    cfg = {
        "process_kind": "database_to_api_sync",
        "source": {"connector_type": "database", "action_type": "Get",
                   "connection_id": "11111111-1111-1111-1111-111111111111",
                   "operation_id": "22222222-2222-2222-2222-222222222222"},
        "transform": {"mode": "dataprocess", "label": label, "steps": [step]},
        "target": {"connector_type": "rest", "action_type": "POST",
                   "connection_id": "33333333-3333-3333-3333-333333333333",
                   "operation_id": "44444444-4444-4444-4444-444444444444"},
    }
    return ProcessFlowBuilder.build(cfg, name="DataProcess Flow")


def test_dataprocess_split_documents_verifies_clean_and_linear():
    """Issue #115 M10.2a: a builder-emitted Split Documents shape passes the graph
    verifier with zero errors AND zero warnings — it is a normal linear NON-terminal
    processing shape (document 1->N multiplexing is data-plane, not a control branch,
    so no CONTROL_BRANCH_BARE_STOP / dead-end)."""
    xml = _dataprocess_process_xml({
        "operation": "split_documents",
        "profile_type": "json",
        "profile_id": "PID-1",
        "link_element_key": "9",
        "link_element_name": "ArrayElement1 (Root/Object/list)",
    })
    result = verify_process_graph(xml)
    assert result["errors"] == [], result["errors"]
    assert result["warnings"] == [], result["warnings"]
    # start -> connectoraction -> dataprocess -> connectoraction -> stop
    assert result["shapes_checked"] == 5
    assert "CONTROL_BRANCH_BARE_STOP" not in _codes(result["warnings"])


def test_dataprocess_combine_documents_verifies_clean_and_linear():
    """Issue #115 M10.2a: a builder-emitted Combine Documents shape (N->1) also
    verifies clean and stays a linear non-terminal/non-branching shape."""
    xml = _dataprocess_process_xml({
        "operation": "combine_documents",
        "profile_type": "xml",
        "profile_id": "PID-2",
        "link_element_key": "4",
        "link_element_name": "Group (Envelope/Body/Groups/Group)",
    })
    result = verify_process_graph(xml)
    assert result["errors"] == [], result["errors"]
    assert result["warnings"] == [], result["warnings"]
    assert result["shapes_checked"] == 5


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
    assert "START_SHAPE_HAS_INBOUND" not in _codes(result["errors"])


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


# ---------------------------------------------------------------------------
# Data Process shape classification (issue #106 M10.2)
#
# dataprocess is a NORMAL LINEAR processing shape — NOT terminal, NOT branching.
# No new verifier source rule is needed: the existing dead-end pass already
# treats it correctly (one outbound = clean; zero outbound = dead end). These
# tests pin that behavior so a future verifier change can't silently regress it.
# ---------------------------------------------------------------------------


def test_dataprocess_single_outbound_is_clean():
    """A builder-emitted Data Process shape (one forward edge) verifies clean."""
    from boomi_mcp.categories.components.builders.process_flow_builder import (
        ProcessFlowBuilder,
    )

    cfg = {
        "process_kind": "database_to_api_sync",
        "source": {
            "connector_type": "database",
            "connection_id": "11111111-1111-1111-1111-111111111111",
            "operation_id": "22222222-2222-2222-2222-222222222222",
            "action_type": "Get",
        },
        "transform": {
            "mode": "dataprocess",
            "label": "Tag documents",
            "steps": [
                {"operation": "custom_scripting", "script": "dataContext.storeStream(is, props);"}
            ],
        },
        "target": {
            "connector_type": "rest",
            "connection_id": "33333333-3333-3333-3333-333333333333",
            "operation_id": "44444444-4444-4444-4444-444444444444",
            "action_type": "POST",
        },
    }
    xml = ProcessFlowBuilder.build(cfg, name="DataProcess Verify")
    result = verify_process_graph(xml)
    assert result["errors"] == [], result["errors"]
    assert result["warnings"] == [], result["warnings"]


def test_dataprocess_zero_outbound_is_dead_end():
    """A non-terminal Data Process shape with no outbound edge is a dead end —
    proving dataprocess is treated as a normal (non-terminal) linear shape."""
    xml = (
        '<process xmlns=""><shapes>'
        '<shape image="start" name="shape1" shapetype="start" userlabel="" x="1" y="1">'
        '<configuration><noaction/></configuration>'
        '<dragpoints><dragpoint name="shape1.dragpoint1" toShape="shape2" x="2" y="2"/></dragpoints>'
        "</shape>"
        '<shape image="dataprocess_icon" name="shape2" shapetype="dataprocess" userlabel="" x="3" y="1">'
        '<configuration><dataprocess><step index="1" key="1" name="Custom Scripting" processtype="12">'
        '<dataprocessscript language="groovy2" useCache="true"><script>x</script></dataprocessscript>'
        "</step></dataprocess></configuration><dragpoints/></shape>"
        "</shapes></process>"
    )
    result = verify_process_graph(xml)
    codes = _codes(result["errors"])
    assert "NON_TERMINAL_SHAPE_DEAD_END" in codes
    dead = [e for e in result["errors"] if e["code"] == "NON_TERMINAL_SHAPE_DEAD_END"]
    assert dead[0]["shape"] == "shape2"
    assert dead[0]["shape_type"] == "dataprocess"


# ---------------------------------------------------------------------------
# Return Documents terminal classification (issue #107 M10.3)
#
# returndocuments is ALREADY terminal in _TERMINAL_SHAPE_TYPES and the verifier
# already enforces RETURN_DOCS_STOP_EXCLUSIVE (a Return Documents path must never
# reach a Stop). Per issue #107 this layer is VERIFY + TEST ONLY — no
# reclassification. These tests pin both behaviors against the typed builder's own
# emitted Return Documents terminal so a future verifier change can't regress them.
# ---------------------------------------------------------------------------


def test_returndocuments_terminal_is_clean():
    """A builder-emitted Return Documents terminal verifies clean: it is terminal
    (no dead end) and there is no RETURN_DOCS_STOP_EXCLUSIVE (no Stop follows it)."""
    from boomi_mcp.categories.components.builders.process_flow_builder import (
        ProcessFlowBuilder,
    )

    cfg = {
        "process_kind": "database_to_api_sync",
        "source": {
            "connector_type": "database",
            "connection_id": "11111111-1111-1111-1111-111111111111",
            "operation_id": "22222222-2222-2222-2222-222222222222",
            "action_type": "Get",
        },
        "target": {
            "connector_type": "rest",
            "connection_id": "33333333-3333-3333-3333-333333333333",
            "operation_id": "44444444-4444-4444-4444-444444444444",
            "action_type": "POST",
        },
        "return_documents": {"enabled": True, "label": "Status Updates"},
    }
    xml = ProcessFlowBuilder.build(cfg, name="Return Documents Verify")
    result = verify_process_graph(xml)
    assert result["errors"] == [], result["errors"]
    assert result["warnings"] == [], result["warnings"]
    assert "RETURN_DOCS_STOP_EXCLUSIVE" not in _codes(result["errors"])


def test_returndocuments_routing_to_stop_is_rejected():
    """A Return Documents path that reaches a Stop fails RETURN_DOCS_STOP_EXCLUSIVE
    (the verifier already enforces this — pinned here, not reclassified)."""
    xml = (
        '<process xmlns=""><shapes>'
        '<shape image="start" name="shape1" shapetype="start" userlabel="" x="1" y="1">'
        '<configuration><noaction/></configuration>'
        '<dragpoints><dragpoint name="shape1.dragpoint1" toShape="shape2" x="2" y="2"/></dragpoints>'
        "</shape>"
        '<shape image="returndocuments_icon" name="shape2" shapetype="returndocuments" userlabel="" x="3" y="1">'
        '<configuration><returndocuments label=""/></configuration>'
        '<dragpoints><dragpoint name="shape2.dragpoint1" toShape="shape3" x="4" y="2"/></dragpoints>'
        "</shape>"
        '<shape image="stop_icon" name="shape3" shapetype="stop" x="5" y="1">'
        '<configuration><stop continue="true"/></configuration><dragpoints/></shape>'
        "</shapes></process>"
    )
    result = verify_process_graph(xml)
    assert "RETURN_DOCS_STOP_EXCLUSIVE" in _codes(result["errors"])


# ---------------------------------------------------------------------------
# Issue #108 M10.4 — builder catch-leg Exception (Throw) verifier coverage
# ---------------------------------------------------------------------------

def _exception_process_config(catch_exception, dlq=None, catch_notify=None, scope="process"):
    cfg = {
        "process_kind": "database_to_api_sync",
        "source": {"connector_type": "database", "action_type": "Get",
                   "connection_id": "C1", "operation_id": "O1"},
        "target": {"connector_type": "rest", "action_type": "POST",
                   "connection_id": "C2", "operation_id": "O2"},
        "reliability": {"try_catch_scope": scope, "catch_exception": catch_exception},
    }
    if dlq is not None:
        cfg["reliability"]["dlq"] = dlq
    if catch_notify is not None:
        cfg["reliability"]["catch_notify"] = catch_notify
    return cfg


def test_builder_catch_exception_is_clean():
    """A builder-emitted bare catch -> exception leg verifies clean: exception is a
    recognized terminal (no NON_TERMINAL_SHAPE_DEAD_END) and the catcherrors Catch
    routes into it, not a bare Stop (no CONTROL_BRANCH_BARE_STOP)."""
    from boomi_mcp.categories.components.builders import ProcessFlowBuilder
    xml = ProcessFlowBuilder.build(
        _exception_process_config({"message_template": "halt {1}", "parameter_source": "caught_error"}),
        name="P",
    )
    result = verify_process_graph(xml)
    assert result["errors"] == []
    assert "CONTROL_BRANCH_BARE_STOP" not in _codes(result["warnings"])


def test_builder_catch_exception_with_dlq_and_notify_is_clean():
    from boomi_mcp.categories.components.builders import ProcessFlowBuilder
    xml = ProcessFlowBuilder.build(
        _exception_process_config(
            {"message_template": "halt {1}", "parameter_source": "current_document"},
            dlq={"mode": "document_cache_ref", "document_cache_id": "CACHE-1"},
            catch_notify={"level": "ERROR", "message_template": "f: meta.base.catcherrorsmessage"},
        ),
        name="P",
    )
    result = verify_process_graph(xml)
    assert result["errors"] == []
    assert "CONTROL_BRANCH_BARE_STOP" not in _codes(result["warnings"])


def test_builder_catch_exception_connector_scope_is_clean():
    from boomi_mcp.categories.components.builders import ProcessFlowBuilder
    xml = ProcessFlowBuilder.build(
        _exception_process_config(
            {"message_template": "boom", "parameter_source": "none"}, scope="connector"
        ),
        name="P",
    )
    result = verify_process_graph(xml)
    assert result["errors"] == []
    assert "CONTROL_BRANCH_BARE_STOP" not in _codes(result["warnings"])


def test_catcherrors_into_exception_is_not_bare_stop_warning():
    """A catcherrors branch routed into an Exception is NOT a bare-Stop drop — the
    Exception terminal records the failure, so CONTROL_BRANCH_BARE_STOP must not
    fire (it would for a catcherrors -> stop edge)."""
    # Try path routes through a connector before its Stop (so the catcherrors Try
    # edge is not itself a bare-Stop drop); the Catch edge targets the Exception.
    xml = (
        '<process xmlns=""><shapes>'
        '<shape image="start" name="shape1" shapetype="start" x="1" y="1">'
        '<configuration><noaction/></configuration>'
        '<dragpoints><dragpoint name="d1" toShape="shape2" x="2" y="2"/></dragpoints></shape>'
        '<shape image="catcherrors_icon" name="shape2" shapetype="catcherrors" x="2" y="1">'
        '<configuration><catcherrors catchAll="true" retryCount="0"/></configuration>'
        '<dragpoints>'
        '<dragpoint identifier="default" name="d2t" text="Try" toShape="shape3" x="3" y="2"/>'
        '<dragpoint identifier="error" name="d2c" text="Catch" toShape="shape4" x="3" y="3"/>'
        '</dragpoints></shape>'
        '<shape image="connectoraction_icon" name="shape3" shapetype="connectoraction" x="3" y="1">'
        '<configuration/>'
        '<dragpoints><dragpoint name="d3" toShape="shape5" x="4" y="1"/></dragpoints></shape>'
        '<shape image="exception_icon" name="shape4" shapetype="exception" x="3" y="3">'
        '<configuration><exception stopProcessReturnSingleDoc="false" stopsingledoc="false" title="t">'
        '<exMessage>halt {1}</exMessage>'
        '<exParameters><parametervalue key="0" valueType="current"/></exParameters>'
        '</exception></configuration><dragpoints/></shape>'
        '<shape image="stop_icon" name="shape5" shapetype="stop" x="4" y="1">'
        '<configuration><stop continue="true"/></configuration><dragpoints/></shape>'
        '</shapes></process>'
    )
    result = verify_process_graph(xml)
    assert "CONTROL_BRANCH_BARE_STOP" not in _codes(result["warnings"])
    assert "CONTROL_BRANCH_BARE_STOP" not in _codes(result["errors"])
    # The exception is a recognized terminal — no dead-end error.
    assert "NON_TERMINAL_SHAPE_DEAD_END" not in _codes(result["errors"])


# ---------------------------------------------------------------------------
# Issue #113 M10.9 — Decision (conditional two-path routing) + loops
# ---------------------------------------------------------------------------

_DECISION_BASE = {
    "process_kind": "database_to_api_sync",
    "source": {"connector_type": "database", "action_type": "Get",
               "connection_id": "11111111-1111-1111-1111-111111111111",
               "operation_id": "22222222-2222-2222-2222-222222222222"},
    "target": {"connector_type": "rest", "action_type": "POST",
               "connection_id": "33333333-3333-3333-3333-333333333333",
               "operation_id": "44444444-4444-4444-4444-444444444444"},
}


def _decision_process_xml(**decision_overrides) -> str:
    """Build a real Decision process via ProcessFlowBuilder (issue #113)."""
    decision = {
        "comparison": "equals",
        "label": "Check Status",
        "left": {"value_type": "track", "property_id": "dynamicdocument.DDP_STATUS"},
        "right": {"value_type": "static", "static_value": "active"},
        "false_notify": "status was not active",
    }
    decision.update(decision_overrides)
    return ProcessFlowBuilder.build({**_DECISION_BASE, "decision": decision}, name="Decision Process")


def test_decision_is_classified_as_branching_and_control_branch():
    from boomi_mcp.categories.components.process_graph_verifier import (
        _BRANCHING_SHAPE_TYPES, _CONTROL_BRANCH_SHAPE_TYPES,
    )
    assert "decision" in _BRANCHING_SHAPE_TYPES
    assert "decision" in _CONTROL_BRANCH_SHAPE_TYPES


def test_builder_decision_true_false_verifies_clean():
    # A builder-emitted decision with the false leg routed through a Message is
    # clean: no errors AND no CONTROL_BRANCH_BARE_STOP warning.
    result = verify_process_graph(_decision_process_xml())
    assert result["errors"] == []
    assert "CONTROL_BRANCH_BARE_STOP" not in _codes(result["warnings"])


def test_decision_bare_false_stop_fires_control_branch_bare_stop_warning():
    # No false_notify, no loop: the false dragpoint goes straight to a Stop, which
    # is the advisory CONTROL_BRANCH_BARE_STOP warning (not an error).
    result = verify_process_graph(_decision_process_xml(false_notify=None))
    assert result["errors"] == []
    assert "CONTROL_BRANCH_BARE_STOP" in _codes(result["warnings"])


def test_decision_loop_back_verifies_clean():
    # The false dragpoint loops back to the source (shape2): the reachability BFS
    # tolerates the back-edge (visited set), so there are no errors and no bare-stop
    # warning (the false output targets a connector, not a Stop).
    result = verify_process_graph(_decision_process_xml(false_notify=None, false_next="shape2"))
    assert result["errors"] == []
    assert "CONTROL_BRANCH_BARE_STOP" not in _codes(result["warnings"])
    assert "SHAPE_UNREACHABLE" not in _codes(result["errors"])


def test_decision_loop_back_through_message_verifies_clean():
    # false_notify + false_next: the false leg runs a Message that loops back to an
    # earlier shape (the live shape31 false->shape32->shape27 pattern); clean.
    result = verify_process_graph(_decision_process_xml(false_notify="retry", false_next="shape2"))
    assert result["errors"] == []
    assert "CONTROL_BRANCH_BARE_STOP" not in _codes(result["warnings"])


# ---------------------------------------------------------------------------
# Issue #117 M10 follow-up — composed flow_sequence graphs verify clean
# ---------------------------------------------------------------------------

_FS_DB = {"connector_type": "database", "connection_id": "c", "operation_id": "o", "action_type": "Get"}


def _fs_rest(label="t", conn="rc", op="ro"):
    return {"connector_type": "rest", "connection_id": conn, "operation_id": op, "action_type": "POST", "label": label}


def _fs_base(flow_sequence):
    return {
        "process_kind": "database_to_api_sync",
        "source": _FS_DB,
        "target": _fs_rest("Main"),
        "flow_sequence": flow_sequence,
    }


def test_composed_decision_dataprocess_branch_map_verifies_clean():
    cfg = _fs_base(
        [
            {
                "kind": "decision",
                "comparison": "equals",
                "left": {"value_type": "track", "property_id": "dynamicdocument.S"},
                "right": {"value_type": "static", "static_value": "A"},
                "true_steps": [
                    {"kind": "dataprocess", "steps": [{"operation": "custom_scripting", "script": "x;"}]}
                ],
                "false_steps": [
                    {
                        "kind": "branch",
                        "legs": [
                            {"steps": [{"kind": "map_ref", "map_ref": "M1"}], "target": _fs_rest("A", "ca", "oa")},
                            {"steps": [{"kind": "map_ref", "map_ref": "M2"}], "target": _fs_rest("B", "cb", "ob")},
                        ],
                    }
                ],
            }
        ]
    )
    result = verify_process_graph(ProcessFlowBuilder.build(cfg, name="Composed"))
    assert result["errors"] == []
    assert result["warnings"] == []


def test_composed_cache_load_retrieve_remove_verifies_clean():
    cfg = _fs_base(
        [
            {"kind": "doccacheload", "document_cache_id": "C"},
            {"kind": "doccacheretrieve", "document_cache_id": "C"},
            {"kind": "doccacheremove", "document_cache_id": "C"},
        ]
    )
    result = verify_process_graph(ProcessFlowBuilder.build(cfg, name="Cache CRUD"))
    assert result["errors"] == []
    assert result["warnings"] == []


def test_composed_exception_terminal_verifies_clean_no_dead_end():
    cfg = _fs_base(
        [
            {"kind": "message", "message_text": "log"},
            {"kind": "exception", "title": "Halt", "message_template": "{1}", "parameter_source": "caught_error"},
        ]
    )
    result = verify_process_graph(ProcessFlowBuilder.build(cfg, name="Exc"))
    assert result["errors"] == []
    assert result["warnings"] == []


def test_composed_decision_false_exception_no_bare_stop_warning():
    # A decision whose false leg throws (rather than a bare Stop) keeps the
    # verifier CONTROL_BRANCH_BARE_STOP-clean.
    cfg = _fs_base(
        [
            {
                "kind": "decision",
                "comparison": "equals",
                "left": {"value_type": "track", "property_id": "dynamicdocument.S"},
                "right": {"value_type": "static", "static_value": "A"},
                "true_steps": [],
                "false_steps": [
                    {"kind": "exception", "title": "Reject", "message_template": "{1}", "parameter_source": "caught_error"}
                ],
            }
        ]
    )
    result = verify_process_graph(ProcessFlowBuilder.build(cfg, name="DecExc"))
    assert result["errors"] == []
    codes = {w["code"] for w in result["warnings"]}
    assert "CONTROL_BRANCH_BARE_STOP" not in codes


# ---------------------------------------------------------------------------
# Issue #130 M9.9 — START_SHAPE_HAS_INBOUND (start must have no inbound edge)
# ---------------------------------------------------------------------------


def test_inbound_edge_into_start_shape_is_error():
    """start(shape1) -> message(shape2) -> shape1: the message loops back into
    the start, which is the sole entry point. Exactly one START_SHAPE_HAS_INBOUND
    (naming shape2), and neither shape is a dead end / unreachable — the mirror
    of test_terminal_shape_with_outbound_edge_is_error."""
    xml = (
        '<process xmlns=""><shapes>'
        '<shape image="start" name="shape1" shapetype="start" x="1" y="1">'
        '<configuration><noaction/></configuration>'
        '<dragpoints><dragpoint name="d1" toShape="shape2" x="2" y="2"/></dragpoints></shape>'
        '<shape image="message_icon" name="shape2" shapetype="message" x="2" y="1">'
        '<configuration/>'
        '<dragpoints><dragpoint name="d2" toShape="shape1" x="1" y="2"/></dragpoints></shape>'
        "</shapes></process>"
    )
    result = verify_process_graph(xml)
    codes = _codes(result["errors"])
    assert "START_SHAPE_HAS_INBOUND" in codes
    inbound = [e for e in result["errors"] if e["code"] == "START_SHAPE_HAS_INBOUND"]
    assert len(inbound) == 1
    assert inbound[0]["shape"] == "shape1"
    assert inbound[0]["shape_type"] == "start"
    assert "shape2" in inbound[0]["message"]
    assert "SHAPE_UNREACHABLE" not in codes
    assert "NON_TERMINAL_SHAPE_DEAD_END" not in codes


def test_start_self_edge_is_error():
    """A start with a dragpoint to itself (start -> start) is one form of
    inbound-to-start and must be flagged."""
    xml = (
        '<process xmlns=""><shapes>'
        '<shape image="start" name="shape1" shapetype="start" x="1" y="1">'
        '<configuration><noaction/></configuration>'
        '<dragpoints><dragpoint name="d1" toShape="shape1" x="1" y="2"/></dragpoints></shape>'
        "</shapes></process>"
    )
    result = verify_process_graph(xml)
    codes = _codes(result["errors"])
    assert "START_SHAPE_HAS_INBOUND" in codes
    inbound = [e for e in result["errors"] if e["code"] == "START_SHAPE_HAS_INBOUND"]
    assert len(inbound) == 1
    assert inbound[0]["shape"] == "shape1"
    assert "shape1" in inbound[0]["message"]


def test_builder_loop_back_does_not_trigger_start_inbound():
    """A builder-emitted decision loop-back targets a downstream shape (shape2),
    never the start — START_SHAPE_HAS_INBOUND must not fire on legal loop-backs."""
    result = verify_process_graph(_decision_process_xml(false_notify=None, false_next="shape2"))
    assert "START_SHAPE_HAS_INBOUND" not in _codes(result["errors"])


# ---------------------------------------------------------------------------
# Issue #145 (M12.10) — every migrated executable fixture verifies clean
# ---------------------------------------------------------------------------
#
# The verifier already runs INSIDE ``emit_process``, so a recipe that produced a
# broken graph would raise rather than return. These tests therefore assert the
# stronger, observable thing: the artifact comes back with an empty error list
# and a non-zero shape count, for every migrated surface and every handoff mode.
# A zero shape count would make "no errors" vacuous.


def _recipe_artifacts():
    import sys

    sys.path.insert(0, str(Path(__file__).parent / "patterns"))
    from boomi_mcp.categories.integration_authoring import (
        build_from_archetype_action,
        compose_archetypes_action,
    )
    from boomi_mcp.models.integration_models import IntegrationSpecV1
    from boomi_mcp.patterns.archetypes.api_to_api_sync import ApiToApiSyncArchetype
    from boomi_mcp.patterns.archetypes.api_to_database_sync import (
        ApiToDatabaseSyncArchetype,
    )
    from boomi_mcp.patterns.recipe_bridge import (
        run_fanout_recipe,
        run_sync_preset_recipe,
    )
    from boomi_mcp.recipes.builtins.catalog import (
        RECIPE_API_TO_API_SYNC,
        RECIPE_API_TO_DATABASE_SYNC,
        RECIPE_DB_REST_FANOUT,
    )
    from test_archetype_composition import _cache_links, _options, _parts

    artifacts = {}

    for label, links in (
        ("compose_stream", None),
        ("compose_mixed_cache", _cache_links("billing")),
        ("compose_all_cache", _cache_links("orders", "billing")),
    ):
        options = dict(_options())
        if links:
            options["links"] = links
        response = compose_archetypes_action(parts=_parts(), options=options)
        spec = IntegrationSpecV1.model_validate(response["integration_spec"])
        result = run_fanout_recipe(
            recipe_id=RECIPE_DB_REST_FANOUT,
            components=spec.components,
            process=spec.components[-1],
        )
        artifacts[label] = result.artifact_for(spec.components[-1].key)

    for label, archetype, cls, recipe_id in (
        ("api_to_api", "api_to_api_sync", ApiToApiSyncArchetype, RECIPE_API_TO_API_SYNC),
        (
            "api_to_db",
            "api_to_database_sync",
            ApiToDatabaseSyncArchetype,
            RECIPE_API_TO_DATABASE_SYNC,
        ),
    ):
        response = build_from_archetype_action(archetype, cls.examples[0].parameters)
        spec = IntegrationSpecV1.model_validate(response["integration_spec"])
        result = run_sync_preset_recipe(
            recipe_id=recipe_id, components=spec.components, process=spec.components[-1]
        )
        artifacts[label] = result.artifact_for(spec.components[-1].key)

    return artifacts


_RECIPE_ARTIFACTS = None


def _artifact(label):
    global _RECIPE_ARTIFACTS
    if _RECIPE_ARTIFACTS is None:
        _RECIPE_ARTIFACTS = _recipe_artifacts()
    return _RECIPE_ARTIFACTS[label]


@pytest.mark.parametrize(
    "label",
    [
        "compose_stream",
        "compose_mixed_cache",
        "compose_all_cache",
        "api_to_api",
        "api_to_db",
    ],
)
def test_migrated_recipe_fixture_verifies_with_zero_errors(label):
    artifact = _artifact(label)
    assert artifact.verifier.errors == ()
    assert artifact.verifier.shapes_checked > 0, "a zero shape count makes this vacuous"


@pytest.mark.parametrize(
    "label",
    [
        "compose_stream",
        "compose_mixed_cache",
        "compose_all_cache",
        "api_to_api",
        "api_to_db",
    ],
)
def test_migrated_recipe_xml_reverifies_standalone(label):
    """Re-run the verifier on the emitted bytes, independently of the emitter.

    ``emit_process`` verifies internally; running the verifier again on the
    returned XML proves the clean result belongs to the OUTPUT rather than to
    some intermediate state the emitter happened to hold.
    """
    result = verify_process_graph(_artifact(label).process_xml)
    assert _codes(result["errors"]) == set()


# ---------------------------------------------------------------------------
# #175 — a Process Call may not continue without a declared return path.
#
# The invariant is DERIVED from the runtime authority: Boomi projects a Process
# Call's outbound connection from the CALLED process's Return Documents shapes,
# named in `configuration/processcall/returnpaths`. `_is_terminal` already knew
# the permissive half (no return path => the call may end the path); nothing
# rejected the contradictory pairing, so every process this repo emitted passed
# clean while the UI declined to draw the connection and left the downstream
# shape orphaned on the canvas.
# ---------------------------------------------------------------------------

_M11 = Path(__file__).resolve().parent / "fixtures" / "live_xml" / "m11"

# The one Process Call in the tree that DECLARES a return path — UI-built and
# frozen long before this slice, which is what makes it a clean-room positive
# oracle rather than a restatement of our own emitter.
_LIVE_RETURNING = (
    '<returnpaths><returnpaths childShapeName="shape233" returnLabel=""/></returnpaths>'
)


def _live_variant() -> str:
    return (_M11 / "process_doccacheretrieve_loadalldoc_variant.xml").read_text(
        encoding="utf-8"
    )


def _orphan_codes(result):
    return sorted(
        i["shape"] for i in result["errors"]
        if i["code"] == "PROCESS_CALL_ORPHAN_CONTINUATION"
    )


def test_a_process_call_with_no_return_path_may_not_continue():
    """The frozen negative control: a graph this repo actually produced.

    Captured from the wrapper golden at the pre-fix baseline, so it is a real
    artifact rather than a hand-built shape invented to satisfy the rule.
    """
    result = verify_process_graph(_load("processcall_orphan_continuation.xml"))
    assert _orphan_codes(result) == ["shape2"]
    issue = next(
        i for i in result["errors"] if i["code"] == "PROCESS_CALL_ORPHAN_CONTINUATION"
    )
    assert issue["shape_type"] == "processcall"
    assert result["shapes_checked"] == 3, "a zero shape count makes this vacuous"


def test_a_terminal_process_call_is_clean_in_both_spellings():
    """The permissive half must survive: an ABSENT and an EMPTY returnpaths
    element are both "the child returns nothing", and neither carries an edge."""
    empty = _load("processcall_orphan_continuation.xml").replace(
        '<dragpoints><dragpoint name="shape2.dragpoint1" toShape="shape3" '
        'x="400.0" y="104.0"/></dragpoints>',
        "<dragpoints/>",
    )
    assert _orphan_codes(verify_process_graph(empty)) == []
    absent = empty.replace("<parameters/><returnpaths/>", "<parameters/>")
    assert "<returnpaths/>" not in absent
    assert _orphan_codes(verify_process_graph(absent)) == []


def test_the_live_returning_call_keeps_its_connection():
    """The positive oracle, and the guard against a one-sided rule.

    `shape10` of the UI-built m11 capture declares a return path AND carries an
    outgoing dragpoint — the platform's own answer to what a valid connected
    Process Call looks like. A rule that merely banned "processcall with an
    edge" would reject it.
    """
    assert _orphan_codes(verify_process_graph(_live_variant())) == []


def test_clearing_only_the_live_return_path_produces_the_error():
    """Two-directional mutation control on that same capture.

    Mutating ONLY `shape10`'s own element (a document-wide replace would rewrite
    `shape4`'s empty element, which appears earlier — that mistake made an early
    run of this control report a false result), and asserting the restore is
    byte-identical, so a pass cannot come from the harness rather than the rule.
    """
    src = _live_variant()
    shape10 = re.search(r'<shape\b[^>]*name="shape10"[^>]*>.*?</shape>', src, re.S).group(0)
    assert _LIVE_RETURNING in shape10 and 'identifier="shape233"' in shape10

    mutant = src.replace(shape10, shape10.replace(_LIVE_RETURNING, "<returnpaths/>"), 1)
    assert _orphan_codes(verify_process_graph(mutant)) == ["shape10"]

    restored = mutant.replace(shape10.replace(_LIVE_RETURNING, "<returnpaths/>"), shape10, 1)
    assert restored == src, "the control is only meaningful if the restore is exact"
    assert _orphan_codes(verify_process_graph(restored)) == []


def test_an_unresolved_dragpoint_target_still_reports_the_call():
    """Keyed on RAW dragpoint children, not on resolved edges.

    A continuation the author asked for is a continuation whether or not its
    target resolves, so the call must be reported even when the generic
    dangling-edge diagnostic also fires — reading `edges[...]` instead would have
    let a malformed target hide the real defect.
    """
    xml = _load("processcall_orphan_continuation.xml").replace(
        'toShape="shape3" x="400.0"', 'toShape="nonexistent" x="400.0"'
    )
    result = verify_process_graph(xml)
    assert _orphan_codes(result) == ["shape2"]
    assert "DRAGPOINT_TO_SHAPE_UNRESOLVED" in _codes(result["errors"])


@pytest.mark.parametrize(
    "declaration,label",
    [
        ("<returnpaths><returnpaths/></returnpaths>", "no childShapeName"),
        ('<returnpaths><returnpaths childShapeName="" returnLabel=""/></returnpaths>',
         "empty childShapeName"),
        ('<returnpaths><returnpaths childShapeName="   "/></returnpaths>',
         "whitespace childShapeName"),
        ("<returnpaths><bogus/></returnpaths>", "unrelated child element"),
    ],
)
def test_a_malformed_return_path_cannot_certify_a_continuation(declaration, label):
    """Stage-2 review round 1. Counting CHILD ELEMENTS was not enough.

    The first version asked "does `returnpaths` have any children?", so a
    hand-authored or escape-hatch document declaring a child that names no shape
    in the called process counted as returning — and SUPPRESSED the continuation
    error for a path the platform cannot bind. The check now asks what the live
    capture shows a real entry to be: a `returnpaths` child with a non-empty
    `childShapeName`.

    Failing toward "declares nothing" is the safe direction: it reports the
    continuation rather than certifying it.
    """
    xml = _load("processcall_orphan_continuation.xml").replace(
        "<returnpaths/>", declaration
    )
    assert _orphan_codes(verify_process_graph(xml)) == ["shape2"], label


# The base fixture's dragpoint deliberately carries NO identifier, so turning it
# into a VALID connected call takes both halves of the live pairing.
_NO_ID_DRAGPOINT = (
    '<dragpoint name="shape2.dragpoint1" toShape="shape3" x="400.0" y="104.0"/>'
)


def _connected(identifier=None, declared="shape233"):
    """The fixture rewritten as a connected call, per the live wire shape."""
    xml = _load("processcall_orphan_continuation.xml").replace(
        "<returnpaths/>",
        f'<returnpaths><returnpaths childShapeName="{declared}" returnLabel=""/></returnpaths>',
    )
    if identifier is not None:
        xml = xml.replace(
            _NO_ID_DRAGPOINT,
            _NO_ID_DRAGPOINT.replace("<dragpoint ", f'<dragpoint identifier="{identifier}" '),
        )
    return xml


def test_a_real_return_path_declaration_still_certifies_the_continuation():
    """The discriminator, without which the tightening above could be satisfied
    by a rule that simply flagged every processcall carrying an edge.

    The fixture reproduces the UI-built m11 capture verbatim — the return path's
    `childShapeName` and the outgoing dragpoint's `identifier` carrying the SAME
    value — because copying an attested shape is how a fixture earns its
    authority. `returnLabel` is legitimately EMPTY there, which is why only
    `childShapeName` is required of the declaration.

    What the capture does NOT establish is that the pairing is REQUIRED: it is
    one sample, and #175's rule was narrowed to the empty-declaration case for
    exactly that reason (see the scope record below). So this asserts only that
    the attested shape verifies clean — which is a fact about the shape, not a
    claim that unpaired shapes are invalid.
    """
    assert _orphan_codes(verify_process_graph(_connected(identifier="shape233"))) == []


@pytest.mark.parametrize(
    "identifier,label",
    [(None, "no identifier at all"), ("shapeXXX", "identifier names an undeclared path")],
)
def test_a_populated_declaration_is_not_judged_here(identifier, label):
    """SCOPE RECORD, not an endorsement: #175 does not judge a POPULATED declaration.

    An earlier revision flagged these — a declared return path whose key no
    outgoing dragpoint carries — because the single connected Process Call in the
    live corpus pairs `returnpaths/@childShapeName` with `dragpoint/@identifier`.
    ONE observation is not a platform rule. If any valid platform form omits the
    identifier, that check would reject a customer's legitimate process through
    `build_integration(action="verify")`: a false positive on real data inferred
    from a single sample. Two internal review rounds asked for the wider rule and
    both reasoned from that same sample, which is agreement rather than evidence.

    So this pins the BOUNDARY of #175 rather than the behaviour of #176: these
    shapes verify clean here, and the binding contract that decides them is
    #176's to establish from a UI-built returning parent. If #176 proves the
    pairing is required, this is the test to invert — left visible for that
    reason rather than deleted.
    """
    assert _orphan_codes(verify_process_graph(_connected(identifier=identifier))) == [], label


def test_a_call_declaring_nothing_bindable_is_flagged_however_it_is_spelled():
    """The half #175 DOES own, and what makes the narrowing above safe.

    Every spelling of "declares no bindable return path" still reports, so the
    reported defect — an empty `returnpaths` beside an outgoing connection — is
    caught however the empty declaration is written. This is the case the live
    captures evidence four times over and that QA measured at runtime.
    """
    for declaration in ("<returnpaths/>",
                        "<returnpaths><returnpaths/></returnpaths>",
                        '<returnpaths><returnpaths childShapeName=""/></returnpaths>',
                        "<returnpaths><bogus/></returnpaths>"):
        xml = _load("processcall_orphan_continuation.xml").replace(
            "<returnpaths/>", declaration
        )
        assert _orphan_codes(verify_process_graph(xml)) == ["shape2"], declaration



def test_a_multi_return_fan_out_with_every_branch_bound_is_clean():
    """The discriminator for the rule above: a multi-return fan-out — one branch
    per Return Documents step, each attributed — must verify clean, or the rule
    would have banned legitimate fan-out.

    PROVENANCE: this fixture is SYNTHETIC, hand-extended from the single-return
    m11 capture by adding a second declaration/dragpoint pair in the same shape.
    No multi-return parent has been captured live — that is #176's probe 4, still
    open. So it is a NEGATIVE guard only: it proves this rule does not fire on a
    plausible fan-out, and it must not be read as attesting the platform's
    multi-return wire form, which remains unmeasured.
    """
    xml = _load("processcall_orphan_continuation.xml").replace(
        "<returnpaths/>",
        '<returnpaths>'
        '<returnpaths childShapeName="shape233" returnLabel=""/>'
        '<returnpaths childShapeName="shape244" returnLabel="second"/>'
        "</returnpaths>",
    ).replace(
        _NO_ID_DRAGPOINT,
        '<dragpoint identifier="shape233" name="shape2.dragpoint1" toShape="shape3" '
        'x="400.0" y="104.0"/>'
        '<dragpoint identifier="shape244" name="shape2.dragpoint2" toShape="shape3" '
        'x="400.0" y="140.0"/>',
    )
    assert _orphan_codes(verify_process_graph(xml)) == []
