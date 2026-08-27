"""The execution-connector action, and the dispatch table that now defines the set."""

from __future__ import annotations

import types

import pytest

from boomi.models.utils.sentinel import SENTINEL
from boomi_mcp.categories.monitoring import (
    _MONITORING_ACTIONS,
    _unset_to_none,
    handle_execution_connectors,
    monitor_platform_action,
)


class _Row:
    def __init__(self, **kw):
        for k, v in kw.items():
            setattr(self, k, v)


class _FakeClient:
    """Stands in for the platform at the SDK boundary only."""

    def __init__(self, rows):
        self.execution_connector = types.SimpleNamespace(
            query_execution_connector=lambda request_body: types.SimpleNamespace(result=rows)
        )


_REAL_EXECUTION = "execution-1957bb8f-9a89-4254-b169-9ddbf41fddf8-2026.08.26"


def test_the_dispatch_table_is_the_only_list_of_actions():
    """The served 'valid actions' message must be derived, not re-typed.

    Asserted by MUTATION rather than by reading: adding an entry to the table must
    change what an unknown action reports. A hand-written list would not move.
    """
    before = monitor_platform_action(_FakeClient([]), "renera", "no-such-action", {})
    assert before["_success"] is False
    assert set(before["valid_actions"]) == set(_MONITORING_ACTIONS)

    _MONITORING_ACTIONS["__probe__"] = (lambda *a, **k: None, False)
    try:
        after = monitor_platform_action(_FakeClient([]), "renera", "no-such-action", {})
        assert "__probe__" in after["valid_actions"], (
            "the advertised action list did not follow the dispatch table, so it is "
            "still a second hand-maintained copy"
        )
    finally:
        del _MONITORING_ACTIONS["__probe__"]


def test_execution_connectors_is_dispatchable():
    assert "execution_connectors" in _MONITORING_ACTIONS
    result = monitor_platform_action(_FakeClient([]), "renera", "execution_connectors",
                                     {"execution_id": _REAL_EXECUTION})
    assert result["_success"] is True
    assert result["count"] == 0


def test_the_execution_id_is_required():
    result = handle_execution_connectors(_FakeClient([]), {})
    assert result["_success"] is False
    assert "execution_id" in result["error"]


def test_sentinels_are_flagged_not_silently_returned_as_connectors():
    """`nodata` and `return` appear beside genuine connectors in real data.

    A caller counting connector rows would otherwise report three connectors for
    an execution that used one.
    """
    rows = [
        _Row(id_="ec1", execution_id=_REAL_EXECUTION,
             connector_type="officialboomi-X3979C-rest-prod", action_type="EXECUTE",
             is_start_shape=False, record_type="rest", success_count=1, error_count=0, size=270),
        _Row(id_="ec2", execution_id=_REAL_EXECUTION, connector_type="nodata",
             action_type="nodata", is_start_shape=False, record_type="nodata",
             success_count=0, error_count=0, size=0),
        _Row(id_="ec3", execution_id=_REAL_EXECUTION, connector_type="return",
             action_type="return", is_start_shape=False, record_type="return",
             success_count=1, error_count=0, size=0),
    ]
    result = handle_execution_connectors(_FakeClient(rows), {"execution_id": _REAL_EXECUTION})
    flags = {r["connector_type"]: r["is_execution_sentinel"] for r in result["connectors"]}
    assert flags == {
        "officialboomi-X3979C-rest-prod": False,
        "nodata": True,
        "return": True,
    }


def test_the_platform_action_field_is_named_for_what_it_is():
    """It cannot distinguish a read from a delete, and the name must not imply it can."""
    rows = [_Row(id_="ec1", execution_id=_REAL_EXECUTION,
                 connector_type="officialboomi-X3979C-rest-prod", action_type="EXECUTE",
                 is_start_shape=False, record_type="rest", success_count=1,
                 error_count=0, size=1)]
    result = handle_execution_connectors(_FakeClient(rows), {"execution_id": _REAL_EXECUTION})
    row = result["connectors"][0]
    assert "platform_action_type" in row
    assert "action_type" not in row, (
        "a bare 'action_type' invites the reader to treat it as the HTTP verb"
    )
    assert "operation component" in result["note"]


def test_a_zero_count_survives_and_an_unset_field_is_absent():
    """Zero is a fact; unset is an absence. Truthiness would conflate them.

    The fixture is a REAL SDK record with no arguments, not a hand-built stand-in.
    The previous version of this test `setattr`d a sentinel onto its own object —
    a state the SDK cannot produce — so it looked like coverage of the sentinel
    path while proving nothing about how real records behave.
    """
    from boomi.models import ExecutionConnector

    assert _unset_to_none(0) == 0

    real_unset = ExecutionConnector()
    row = handle_execution_connectors(
        _FakeClient([real_unset]), {"execution_id": _REAL_EXECUTION}
    )["connectors"][0]
    # Nothing was supplied, so nothing may be served: unset fields are omitted
    # rather than serialised as a bare object repr.
    assert "size" not in row
    assert "connector_type" not in row

    # And a record that DOES carry zeros keeps them.
    counted = _Row(id_="ec1", execution_id=_REAL_EXECUTION, connector_type="x",
                   action_type="EXECUTE", is_start_shape=False, record_type="r",
                   success_count=0, error_count=0, size=0)
    row = handle_execution_connectors(_FakeClient([counted]),
                                      {"execution_id": _REAL_EXECUTION})["connectors"][0]
    assert row["success_count"] == 0 and row["error_count"] == 0 and row["size"] == 0


def test_the_sdk_never_actually_yields_the_sentinel():
    """The measurement behind the normaliser's docstring, pinned.

    If the SDK ever starts assigning SENTINEL for omitted arguments, this fails and
    the defensive branch becomes load-bearing — which is worth being told about
    rather than discovering through a bare object repr in a served payload.
    """
    from boomi.models import ExecutionConnector

    record = ExecutionConnector()
    sentinel_valued = [
        f for f in ("connector_type", "action_type", "size", "success_count")
        if getattr(record, f, None) is SENTINEL
    ]
    assert sentinel_valued == [], (
        "the SDK now yields SENTINEL from getattr for {0}; the normaliser's "
        "docstring says it does not, and must be corrected".format(sentinel_valued)
    )


def test_the_connector_identity_is_served():
    """Without this field the rows say a connector ran but not WHICH one."""
    rows = [_Row(id_="ec1", execution_id=_REAL_EXECUTION, connector_type="x",
                 action_type="EXECUTE", execution_connector="the-connector-name",
                 is_start_shape=False, record_type="r", success_count=1,
                 error_count=0, size=1)]
    row = handle_execution_connectors(_FakeClient(rows),
                                      {"execution_id": _REAL_EXECUTION})["connectors"][0]
    assert row["execution_connector"] == "the-connector-name"


def test_an_empty_result_says_it_cannot_distinguish_its_two_causes():
    """A bare success invites the reader to conclude 'no connectors were used'."""
    result = handle_execution_connectors(_FakeClient([]), {"execution_id": _REAL_EXECUTION})
    assert result["_success"] is True and result["count"] == 0
    note = result["empty_result_is_ambiguous"]
    assert "does not exist" in note and "not yet materialised" in note


def test_the_capability_catalog_is_derived_from_the_dispatch_table():
    """The sibling this slice missed the first time.

    Deriving the router's own error envelope was not enough: the same fact was
    hand-written in the discovery catalog too, so adding an action silently
    regressed that surface from complete to incomplete while every test stayed
    green. Asserted by MUTATION, so a re-frozen copy fails.
    """
    from boomi_mcp.categories.meta_tools import _monitoring_actions

    assert set(_monitoring_actions()) == set(_MONITORING_ACTIONS)

    _MONITORING_ACTIONS["__probe__"] = (lambda *a, **k: None, False)
    try:
        assert "__probe__" in _monitoring_actions(), (
            "the discovery catalog no longer follows the dispatch table"
        )
    finally:
        del _MONITORING_ACTIONS["__probe__"]


def test_the_tool_docstring_names_every_action():
    """Detection where derivation is impossible.

    A tool docstring is captured at decoration time, so it cannot be computed from
    the table the way the catalog is. It can still be held to the table: this fails
    loudly when an action ships without being documented, which is exactly what
    happened to `execution_connectors`.
    """
    import inspect

    import server

    doc = inspect.getdoc(getattr(server.monitor_platform, "fn", server.monitor_platform)) or ""
    missing = sorted(a for a in _MONITORING_ACTIONS if a not in doc)
    assert missing == [], (
        "the monitor_platform docstring does not mention {0}. A caller reading the "
        "tool's own documentation cannot discover them.".format(missing)
    )
