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


def test_a_zero_count_survives_but_an_unset_field_does_not():
    """Zero is a fact; unset is an absence. Truthiness would conflate them."""
    assert _unset_to_none(0) == 0
    assert _unset_to_none(SENTINEL) is None
    rows = [_Row(id_="ec1", execution_id=_REAL_EXECUTION, connector_type="x",
                 action_type="EXECUTE", is_start_shape=False, record_type="r",
                 success_count=0, error_count=0, size=SENTINEL)]
    row = handle_execution_connectors(_FakeClient(rows),
                                      {"execution_id": _REAL_EXECUTION})["connectors"][0]
    assert row["success_count"] == 0 and row["error_count"] == 0
    assert "size" not in row, "an unset field must be absent, not a bare object repr"
