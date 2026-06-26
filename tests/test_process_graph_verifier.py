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
