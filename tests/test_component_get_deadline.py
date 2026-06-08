"""Tests for the component-XML GET wall-clock deadline.

component_get_xml() is synchronous and runs inside FastMCP's anyio worker
thread, so a stalled backend fetch is bounded with a thread-based wall-clock
deadline (BOOMI_COMPONENT_GET_DEADLINE_SECONDS) that raises
ComponentGetDeadlineExceeded instead of hanging. Call sites map that into a
structured COMPONENT_GET_DEADLINE_EXCEEDED envelope.
"""
from __future__ import annotations

import threading
import time
from unittest.mock import Mock

import pytest

# Imported via the canonical `boomi_mcp` package (the suite runs with
# `PYTHONPATH=src`, matching the modules' own relative imports).
from boomi_mcp.categories.components import query_components
from boomi_mcp.categories.components import manage_component
from boomi_mcp.categories.components._shared import (
    ComponentGetDeadlineExceeded,
    component_get_xml,
    component_get_deadline_envelope,
    component_get_deadline_item,
    _component_get_deadline_seconds,
    _run_with_deadline,
)


# --- env parse / clamp -----------------------------------------------------

def test_deadline_default_when_unset(monkeypatch):
    monkeypatch.delenv("BOOMI_COMPONENT_GET_DEADLINE_SECONDS", raising=False)
    assert _component_get_deadline_seconds() == 90


def test_deadline_empty_uses_default(monkeypatch):
    monkeypatch.setenv("BOOMI_COMPONENT_GET_DEADLINE_SECONDS", "   ")
    assert _component_get_deadline_seconds() == 90


def test_deadline_invalid_uses_default(monkeypatch, capsys):
    monkeypatch.setenv("BOOMI_COMPONENT_GET_DEADLINE_SECONDS", "abc")
    assert _component_get_deadline_seconds() == 90
    assert "[WARNING]" in capsys.readouterr().out


def test_deadline_clamps_high(monkeypatch):
    monkeypatch.setenv("BOOMI_COMPONENT_GET_DEADLINE_SECONDS", "999")
    assert _component_get_deadline_seconds() == 240


@pytest.mark.parametrize("value", ["0", "-5"])
def test_deadline_clamps_low(monkeypatch, value):
    monkeypatch.setenv("BOOMI_COMPONENT_GET_DEADLINE_SECONDS", value)
    assert _component_get_deadline_seconds() == 1


def test_deadline_valid_passthrough(monkeypatch):
    monkeypatch.setenv("BOOMI_COMPONENT_GET_DEADLINE_SECONDS", "120")
    assert _component_get_deadline_seconds() == 120


# --- runner primitive ------------------------------------------------------

def test_run_with_deadline_success():
    assert _run_with_deadline(lambda: 42, "cid", 5) == 42


def test_run_with_deadline_preserves_original_exception():
    class Boom(Exception):
        pass

    def raise_boom():
        raise Boom("original")

    with pytest.raises(Boom, match="original"):
        _run_with_deadline(raise_boom, "cid", 5)


def test_run_with_deadline_times_out():
    released = threading.Event()
    try:
        start = time.monotonic()
        with pytest.raises(ComponentGetDeadlineExceeded) as exc_info:
            _run_with_deadline(lambda: released.wait(), "cid-x", 1)
        elapsed = time.monotonic() - start
        assert exc_info.value.component_id == "cid-x"
        assert exc_info.value.deadline_seconds == 1
        assert exc_info.value.elapsed_seconds >= 1
        assert elapsed < 5  # returns promptly, does not wait for the worker
    finally:
        released.set()  # free the abandoned worker


# --- envelope builders -----------------------------------------------------

def test_deadline_envelope_shape():
    env = component_get_deadline_envelope(ComponentGetDeadlineExceeded("cid", 90, 1.234))
    assert env == {
        "_success": False,
        "error_code": "COMPONENT_GET_DEADLINE_EXCEEDED",
        "exception_type": "ComponentGetDeadlineExceeded",
        "component_id": "cid",
        "deadline_seconds": 90,
        "elapsed_seconds": 1.234,
        "retryable": True,
        "hint": env["hint"],
    }
    assert "Retry" in env["hint"]


def test_deadline_item_shape_has_no_success_key():
    item = component_get_deadline_item(ComponentGetDeadlineExceeded("cid", 90, 1.0))
    assert "_success" not in item
    assert item["component_id"] == "cid"
    assert item["error_code"] == "COMPONENT_GET_DEADLINE_EXCEEDED"
    assert item["retryable"] is True


# --- component_get_xml end-to-end ------------------------------------------

_SAMPLE_XML = (
    '<bns:Component xmlns:bns="http://api.platform.boomi.com/" '
    'componentId="cid-1" name="My Component" type="process" '
    'folderId="f1" folderName="Home" version="3">'
    "<bns:description>hello</bns:description>"
    "</bns:Component>"
)


class _FakeAuth:
    def get_headers(self):
        return {}


class _FakeComponentService:
    """Minimal stand-in for boomi_client.component used by component_get_xml."""

    base_url = "https://api.example.com"

    def __init__(self, *, block_event=None, response=None):
        self._block_event = block_event
        self._response = response
        self.send_calls = 0

    def get_access_token(self):
        return _FakeAuth()

    def get_basic_auth(self):
        return _FakeAuth()

    def send_request(self, request):
        self.send_calls += 1
        if self._block_event is not None:
            self._block_event.wait()
        return self._response


class _FakeClient:
    def __init__(self, svc):
        self.component = svc


def test_component_get_xml_success_shape(monkeypatch):
    monkeypatch.delenv("BOOMI_COMPONENT_GET_DEADLINE_SECONDS", raising=False)
    svc = _FakeComponentService(response=(_SAMPLE_XML, 200, "application/xml"))
    result = component_get_xml(_FakeClient(svc), "cid-1")
    assert result["component_id"] == "cid-1"
    assert result["name"] == "My Component"
    assert result["type"] == "process"
    assert result["version"] == 3
    assert result["description"] == "hello"
    assert result["xml"] == _SAMPLE_XML
    assert svc.send_calls == 1


def test_component_get_xml_times_out_when_backend_stalls(monkeypatch):
    monkeypatch.setenv("BOOMI_COMPONENT_GET_DEADLINE_SECONDS", "1")
    released = threading.Event()
    svc = _FakeComponentService(block_event=released, response=(_SAMPLE_XML, 200, "x"))
    try:
        start = time.monotonic()
        with pytest.raises(ComponentGetDeadlineExceeded) as exc_info:
            component_get_xml(_FakeClient(svc), "cid-blocked")
        assert time.monotonic() - start < 5
        assert exc_info.value.component_id == "cid-blocked"
        assert exc_info.value.deadline_seconds == 1
    finally:
        released.set()


# --- call-site envelope mapping --------------------------------------------

def test_get_component_maps_deadline_to_envelope(monkeypatch):
    def boom(_client, _cid):
        raise ComponentGetDeadlineExceeded("cid-9", 90, 1.5)

    monkeypatch.setattr(query_components, "component_get_xml", boom)
    result = query_components.get_component(Mock(), "work", "cid-9")
    assert result["_success"] is False
    assert result["error_code"] == "COMPONENT_GET_DEADLINE_EXCEEDED"
    assert result["component_id"] == "cid-9"
    assert result["retryable"] is True


def test_update_component_maps_deadline_to_envelope(monkeypatch):
    def boom(_client, _cid):
        raise ComponentGetDeadlineExceeded("cid-up", 90, 2.0)

    # Partial-update path (no 'xml' key) reaches component_get_xml.
    monkeypatch.setattr(manage_component, "component_get_xml", boom)
    result = manage_component.update_component(
        Mock(), "work", "cid-up", {"name": "Renamed"}
    )
    assert result["_success"] is False
    assert result["error_code"] == "COMPONENT_GET_DEADLINE_EXCEEDED"
    assert result["component_id"] == "cid-up"


def test_bulk_get_records_per_item_deadline_and_keeps_siblings(monkeypatch):
    def side_effect(_client, cid):
        if cid == "bad":
            raise ComponentGetDeadlineExceeded("bad", 90, 1.1)
        return {"component_id": cid, "name": cid, "xml": "<x/>"}

    monkeypatch.setattr(query_components, "component_get_xml", side_effect)
    result = query_components.bulk_get_components(Mock(), "work", ["good", "bad"])
    # one sibling succeeded → not all-failed
    assert result["_success"] is True
    assert result["total_count"] == 1
    assert result["components"][0]["component_id"] == "good"
    assert len(result["errors"]) == 1
    err = result["errors"][0]
    assert err["component_id"] == "bad"
    assert err["error_code"] == "COMPONENT_GET_DEADLINE_EXCEEDED"
    assert err["retryable"] is True
