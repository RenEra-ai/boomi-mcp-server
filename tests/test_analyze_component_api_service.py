"""Issue #133 (M6.1) — analyze_component webservice (API Service) analysis tests.

Mocked-client coverage of the ASC route extraction added to
``find_dependencies``: base/override parsing from the live fixture, effective
``/ws/rest`` path computation with WSS-operation inheritance (case-verbatim),
per-route validation flags (missing/unreadable/not_process/not_wss_listen/
duplicate_effective_path), tolerance for absent override attributes in live
XML, and the read-budget degradation flag.
"""

from __future__ import annotations

import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

_project_root = str(Path(__file__).resolve().parent.parent)
if _project_root not in sys.path:
    sys.path.insert(0, _project_root)

from src.boomi_mcp.categories.components.analyze_component import (
    _analyze_api_service,
    _extract_wss_listen_binding,
    _extract_wss_operation_config,
    _parse_api_service_xml,
)

_FIXTURE = (
    Path(__file__).resolve().parent
    / "fixtures"
    / "live_xml"
    / "m6"
    / "api_service_minimal.xml"
)

_PROCESS_ID = "c991a424-e7e3-4af1-b2ab-3ddba4a43974"
_OP_ID = "a5d9f624-0000-0000-0000-000000000001"

_PATCH = "src.boomi_mcp.categories.components.analyze_component.component_get_xml"

_LISTENER_PROCESS_XML = (
    '<bns:Component xmlns:bns="http://api.platform.boomi.com/" type="process" '
    f'componentId="{_PROCESS_ID}" name="Weblistener"><bns:object>'
    '<process xmlns=""><shapes><shape shapetype="start"><configuration>'
    f'<connectoraction actionType="Listen" connectorType="wss" operationId="{_OP_ID}"/>'
    "</configuration></shape></shapes></process></bns:object></bns:Component>"
)

_PLAIN_PROCESS_XML = (
    '<bns:Component xmlns:bns="http://api.platform.boomi.com/" type="process" '
    'componentId="p2" name="Plain"><bns:object>'
    '<process xmlns=""><shapes><shape shapetype="start"><configuration>'
    '<connectoraction actionType="Get" connectorType="database" operationId="x"/>'
    "</configuration></shape></shapes></process></bns:object></bns:Component>"
)

_WSS_OP_XML = (
    '<bns:Component xmlns:bns="http://api.platform.boomi.com/" '
    f'type="connector-action" componentId="{_OP_ID}" name="Web Listener">'
    "<bns:object><Operation><Configuration>"
    '<WebServicesServerListenAction inputType="singlejson" '
    'objectName="generalListener" operationType="EXECUTE" outputType="none"/>'
    "</Configuration></Operation></bns:object></bns:Component>"
)


def _asc_xml(routes_xml: str, base: str = "") -> str:
    return (
        '<bns:Component xmlns:bns="http://api.platform.boomi.com/" '
        'type="webservice" componentId="asc-1" name="ASC"><bns:object>'
        f'<webservice xmlns="" urlPath="{base}"><restApi>{routes_xml}</restApi>'
        '<soapApi fullEnvelopePassthrough="false" singleWsdlSchema="false" '
        'suppressWrappers="false" wsdlNamespace="" wsdlServiceName="">'
        "<SOAPVersion>SOAP_1_1</SOAPVersion></soapApi><odataApi/>"
        '<metaInfo title="t" version="1.0.0"><description/><termsOfService/>'
        "</metaInfo><profileOverrides/><capturedHeaders/><apiRoles/>"
        "</webservice></bns:object></bns:Component>"
    )


def _route(process_id: str, **attrs) -> str:
    defaults = {
        "httpMethod": "",
        "inputProfileKey": "",
        "inputType": "",
        "objectName": "",
        "outputType": "",
        "urlPath": "",
    }
    defaults.update(attrs)
    attr_text = " ".join(f'{k}="{v}"' for k, v in sorted(defaults.items()))
    return (
        f'<route processId="{process_id}"><overrides {attr_text}/>'
        "<description/></route>"
    )


def _fake_reads(mapping):
    def _get(client, cid, deadline_seconds=None):
        entry = mapping.get(cid)
        if entry is None:
            raise Exception("GET failed (HTTP 404): not found")
        return entry
    return _get


_DEFAULT_READS = {
    _PROCESS_ID: {"type": "process", "xml": _LISTENER_PROCESS_XML},
    "p2": {"type": "process", "xml": _PLAIN_PROCESS_XML},
    _OP_ID: {"type": "connector-action", "xml": _WSS_OP_XML},
}


def _analyze(asc_xml: str, reads=None):
    with patch(_PATCH, side_effect=_fake_reads(reads or _DEFAULT_READS)):
        return _analyze_api_service(
            MagicMock(), "asc-1", {"xml": asc_xml, "type": "webservice"}
        )


# ---------------------------------------------------------------------------
# Pure parsing helpers
# ---------------------------------------------------------------------------


def test_parse_live_fixture():
    parsed = _parse_api_service_xml(_FIXTURE.read_text())
    assert parsed["base_url_path"] == ""
    assert len(parsed["routes"]) == 1
    route = parsed["routes"][0]
    assert route["process_id"] == _PROCESS_ID
    assert route["overrides"]["http_method"] == "POST"
    assert route["overrides"]["object_name"] == ""
    assert parsed["placeholder_validation"]["missing"] == []
    assert parsed["placeholder_validation"]["profile_overrides"] == "empty"


def test_parse_flags_missing_placeholders_and_populated_profile_overrides():
    xml = (
        '<bns:Component xmlns:bns="http://api.platform.boomi.com/" '
        'type="webservice" name="Bad"><bns:object><webservice xmlns="" urlPath="">'
        "<restApi/>"
        '<profileOverrides><profileOverride processId="x"/></profileOverrides>'
        "</webservice></bns:object></bns:Component>"
    )
    parsed = _parse_api_service_xml(xml)
    assert set(parsed["placeholder_validation"]["missing"]) == {
        "soapApi",
        "odataApi",
        "metaInfo",
        "capturedHeaders",
        "apiRoles",
    }
    assert (
        parsed["placeholder_validation"]["profile_overrides"]
        == "preserved_not_authored"
    )


def test_parse_tolerates_absent_override_attrs():
    # Live XML from other tenants may omit override attributes entirely.
    xml = _asc_xml(
        f'<route processId="{_PROCESS_ID}"><overrides httpMethod="POST"/>'
        "<description/></route>"
    )
    parsed = _parse_api_service_xml(xml)
    overrides = parsed["routes"][0]["overrides"]
    assert overrides["http_method"] == "POST"
    assert overrides["object_name"] is None  # absent, not empty-inherit


def test_extract_wss_listen_binding():
    assert _extract_wss_listen_binding(_LISTENER_PROCESS_XML) == {
        "has_listen": True,
        "operation_id": _OP_ID,
    }
    assert _extract_wss_listen_binding(_PLAIN_PROCESS_XML) == {
        "has_listen": False,
        "operation_id": "",
    }


def test_extract_wss_operation_config():
    cfg = _extract_wss_operation_config(_WSS_OP_XML)
    assert cfg == {
        "object_name": "generalListener",
        "operation_type": "EXECUTE",
        "input_type": "singlejson",
        "output_type": "none",
    }
    assert _extract_wss_operation_config(_PLAIN_PROCESS_XML) is None


# ---------------------------------------------------------------------------
# _analyze_api_service — effective paths + validation flags
# ---------------------------------------------------------------------------


def test_all_inherit_route_resolves_via_wss_operation():
    result = _analyze(_FIXTURE.read_text())
    assert result["route_count"] == 1
    route = result["routes"][0]
    assert route["effective_method"] == "POST"
    assert route["effective_path"] == "/ws/rest/generalListener"  # verbatim casing
    assert route["wss_operation"]["object_name"] == "generalListener"
    assert route["flags"] == []
    assert result["route_validation"] == []
    assert result["effective_paths"] == ["POST /ws/rest/generalListener"]


def test_base_and_url_path_segments_compose():
    xml = _asc_xml(
        _route(_PROCESS_ID, httpMethod="POST", urlPath="V1"), base="Intake"
    )
    result = _analyze(xml)
    assert result["routes"][0]["effective_path"] == "/ws/rest/Intake/generalListener/V1"


def test_not_wss_listen_flagged():
    xml = _asc_xml(_route("p2", httpMethod="POST", objectName="x"))
    result = _analyze(xml)
    assert "not_wss_listen" in result["routes"][0]["flags"]


def test_unreadable_process_flagged():
    xml = _asc_xml(_route("00000000-0000-0000-0000-0000000000ff", objectName="x", httpMethod="POST"))
    result = _analyze(xml)
    assert "process_unreadable" in result["routes"][0]["flags"]


def test_non_process_route_flagged():
    reads = dict(_DEFAULT_READS)
    reads["op-as-route"] = {"type": "connector-action", "xml": _WSS_OP_XML}
    xml = _asc_xml(_route("op-as-route", objectName="x", httpMethod="POST"))
    result = _analyze(xml, reads)
    assert "not_process" in result["routes"][0]["flags"]


def test_missing_process_id_flagged():
    xml = _asc_xml(_route("", httpMethod="POST", objectName="x"))
    result = _analyze(xml)
    assert "process_missing" in result["routes"][0]["flags"]


def test_duplicate_effective_paths_flagged_on_both_routes():
    xml = _asc_xml(
        _route(_PROCESS_ID, httpMethod="POST", objectName="intake")
        + _route(_PROCESS_ID, httpMethod="POST", objectName="intake")
    )
    result = _analyze(xml)
    assert "duplicate_effective_path" in result["routes"][0]["flags"]
    assert "duplicate_effective_path" in result["routes"][1]["flags"]


def test_unresolved_inherit_path_not_collision_flagged():
    # WSS op unreadable + no explicit objectName: the effective path is
    # partial — flagged unresolved, never collision-compared.
    reads = {
        _PROCESS_ID: {"type": "process", "xml": _LISTENER_PROCESS_XML},
        # _OP_ID intentionally unreadable
    }
    xml = _asc_xml(_route(_PROCESS_ID) + _route(_PROCESS_ID))
    result = _analyze(xml, reads)
    for route in result["routes"]:
        assert "effective_path_unresolved" in route["flags"]
        # All-inherit routes leave the method unresolved too.
        assert "effective_method_unresolved" in route["flags"]
        assert "duplicate_effective_path" not in route["flags"]
        assert "wss_operation_unreadable" in route["flags"]


def test_unresolved_inherit_method_not_collision_flagged():
    # Codex review r1: explicit objectName but inherited method (no explicit
    # httpMethod, no explicit inputType) with an unreadable WSS operation —
    # the POST default is a guess (the real op may be inputType='none' ->
    # GET), so the route is method-unresolved and never collision-compared,
    # even against another route with the same explicit path.
    reads = {
        _PROCESS_ID: {"type": "process", "xml": _LISTENER_PROCESS_XML},
        # _OP_ID intentionally unreadable
    }
    xml = _asc_xml(
        _route(_PROCESS_ID, objectName="intake")
        + _route(_PROCESS_ID, objectName="intake")
    )
    result = _analyze(xml, reads)
    for route in result["routes"]:
        assert "effective_method_unresolved" in route["flags"]
        assert "effective_path_unresolved" not in route["flags"]  # path IS explicit
        assert "duplicate_effective_path" not in route["flags"]
    # An explicit inputType pins the method (none -> GET) without the op:
    # no method-unresolved flag, and the duplicate check applies again.
    xml2 = _asc_xml(
        _route(_PROCESS_ID, objectName="intake", inputType="none")
        + _route(_PROCESS_ID, objectName="intake", inputType="none")
    )
    result2 = _analyze(xml2, reads)
    assert result2["routes"][0]["effective_method"] == "GET"
    for route in result2["routes"]:
        assert "effective_method_unresolved" not in route["flags"]
        assert "duplicate_effective_path" in route["flags"]


def test_budget_exhaustion_flags_instead_of_reading():
    with patch(
        "src.boomi_mcp.categories.components.analyze_component._component_get_deadline_seconds",
        return_value=0,
    ):
        result = _analyze(_asc_xml(_route(_PROCESS_ID, httpMethod="POST")))
    assert "analysis_budget_exhausted" in result["routes"][0]["flags"]
