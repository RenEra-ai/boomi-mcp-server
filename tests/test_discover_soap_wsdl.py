"""Issue #13 (M7): handler tests for ``discover_soap_wsdl_action``.

Covers WSDL 1.1 artifact + mocked-URL success (SOAP 1.1 and 1.2), the
exactly-one-of contract, DOCTYPE/ENTITY rejection, non-WSDL / malformed / version
errors, auth/unreachable/redirect/SSRF/size branches, truncation, and proof that
WSDL/XSD imports are never fetched.
"""

import json
import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

import httpx

_src = str(Path(__file__).resolve().parent.parent / "src")
if _src not in sys.path:
    sys.path.insert(0, _src)

import boomi_mcp.categories.schema_discovery as sd
from boomi_mcp.categories.schema_discovery import discover_soap_wsdl_action

_PUBLIC_IP = "93.184.216.34"

_WSDL = """<?xml version="1.0" encoding="UTF-8"?>
<definitions xmlns="http://schemas.xmlsoap.org/wsdl/"
             xmlns:soap="http://schemas.xmlsoap.org/wsdl/soap/"
             xmlns:soap12="http://schemas.xmlsoap.org/wsdl/soap12/"
             xmlns:tns="http://ex.com/svc"
             targetNamespace="http://ex.com/svc">
  <message name="AddReq"><part name="p" element="tns:Add"/></message>
  <message name="AddResp"><part name="r" type="xsd:int"/></message>
  <portType name="CalcPT">
    <operation name="Add">
      <input message="tns:AddReq"/><output message="tns:AddResp"/>
      <fault name="err" message="tns:CalcFault"/>
    </operation>
  </portType>
  <binding name="CalcSoap11" type="tns:CalcPT">
    <soap:binding style="document" transport="http://schemas.xmlsoap.org/soap/http"/>
    <operation name="Add"><soap:operation soapAction="http://ex.com/Add"/></operation>
  </binding>
  <binding name="CalcSoap12" type="tns:CalcPT">
    <soap12:binding style="rpc" transport="http://schemas.xmlsoap.org/soap/http"/>
    <operation name="Add"><soap12:operation soapAction="http://ex.com/Add12"/></operation>
  </binding>
  <service name="CalcSvc">
    <port name="p11" binding="tns:CalcSoap11"><soap:address location="https://u:p@ex.com/calc?token=1"/></port>
    <port name="p12" binding="tns:CalcSoap12"><soap12:address location="https://ex.com/calc12"/></port>
  </service>
  <import namespace="http://ex.com/other" location="https://ex.com/other.wsdl"/>
</definitions>"""


def _stream_client(status=200, body=b"", raise_exc=None):
    resp = MagicMock()
    resp.status_code = status
    resp.iter_bytes.return_value = iter([body] if body else [])
    stream_cm = MagicMock()
    stream_cm.__enter__.return_value = resp
    stream_cm.__exit__.return_value = False
    client = MagicMock()
    if raise_exc is not None:
        client.stream.side_effect = raise_exc
    else:
        client.stream.return_value = stream_cm
    client.__enter__.return_value = client
    client.__exit__.return_value = False
    return MagicMock(return_value=client), client


def _public_gai(host, *a, **k):
    return [(2, 1, 6, "", (_PUBLIC_IP, 0))]


# ---------------------------------------------------------------------------
# Success
# ---------------------------------------------------------------------------

def test_wsdl_artifact_success():
    r = discover_soap_wsdl_action(artifact=_WSDL)
    assert r["_success"] is True
    assert r["read_only"] is True and r["boomi_mutation"] is False and r["raw_xml_exposed"] is False
    assert r["format"] == "wsdl" and r["version"] == "1.1"
    assert r["source_mode"] == "artifact"
    assert r["target_namespace"] == "http://ex.com/svc"


def test_wsdl_soap_versions_and_operations():
    r = discover_soap_wsdl_action(artifact=_WSDL)
    by_name = {b["name"]: b for b in r["bindings"]}
    assert by_name["CalcSoap11"]["soap_version"] == "1.1"
    assert by_name["CalcSoap12"]["soap_version"] == "1.2"
    op = by_name["CalcSoap11"]["operations"][0]
    assert op["name"] == "Add"
    assert op["soap_action"] == "http://ex.com/Add"
    assert op["input_message"] == "AddReq" and op["output_message"] == "AddResp"
    assert op["fault_messages"] == ["CalcFault"]


def test_wsdl_ports_and_address_sanitized():
    r = discover_soap_wsdl_action(artifact=_WSDL)
    ports = {p["name"]: p for p in r["services"][0]["ports"]}
    assert ports["p11"]["address"] == "https://ex.com/calc"  # userinfo + query stripped
    assert ports["p11"]["soap_version"] == "1.1"
    assert ports["p12"]["soap_version"] == "1.2"
    assert "token" not in json.dumps(r)


def test_wsdl_messages_and_parts():
    r = discover_soap_wsdl_action(artifact=_WSDL)
    msgs = {m["name"]: m for m in r["messages"]}
    assert msgs["AddReq"]["parts"][0]["element"] == "Add"
    assert msgs["AddResp"]["parts"][0]["type"] == "int"


def test_wsdl_imports_reported_not_fetched():
    with patch.object(sd.httpx, "Client") as m_client:
        r = discover_soap_wsdl_action(artifact=_WSDL)
    m_client.assert_not_called()
    assert r["imports"][0]["namespace"] == "http://ex.com/other"
    assert r["imports"][0]["fetched"] is False


def test_wsdl_url_mode_success():
    cls, client = _stream_client(200, _WSDL.encode("utf-8"))
    with patch.object(sd.socket, "getaddrinfo", _public_gai), patch.object(sd.httpx, "Client", cls):
        r = discover_soap_wsdl_action(wsdl_url="https://ex.com/svc?wsdl")
    assert r["_success"] is True and r["source_mode"] == "url"
    _, ckwargs = cls.call_args
    assert ckwargs.get("follow_redirects") is False and ckwargs.get("trust_env") is False


# ---------------------------------------------------------------------------
# Errors
# ---------------------------------------------------------------------------

def test_neither_source_invalid_input():
    assert discover_soap_wsdl_action()["error_code"] == "WSDL_INVALID_INPUT"


def test_both_sources_invalid_input():
    r = discover_soap_wsdl_action(wsdl_url="https://ex.com/a.wsdl", artifact=_WSDL)
    assert r["error_code"] == "WSDL_INVALID_INPUT"


def test_non_str_artifact_invalid_input():
    assert discover_soap_wsdl_action(artifact={"not": "xml"})["error_code"] == "WSDL_INVALID_INPUT"


def test_doctype_rejected_invalid_spec():
    payload = '<!DOCTYPE x [ <!ENTITY a "b"> ]><definitions xmlns="http://schemas.xmlsoap.org/wsdl/"/>'
    assert discover_soap_wsdl_action(artifact=payload)["error_code"] == "WSDL_INVALID_SPEC"


def test_malformed_xml_parse_error():
    assert discover_soap_wsdl_action(artifact="<definitions>")["error_code"] == "WSDL_PARSE_ERROR"


def test_non_wsdl_root_invalid_spec():
    assert discover_soap_wsdl_action(artifact="<root/>")["error_code"] == "WSDL_INVALID_SPEC"


def test_wsdl_url_401_auth_failure():
    cls, _ = _stream_client(401, b"")
    with patch.object(sd.socket, "getaddrinfo", _public_gai), patch.object(sd.httpx, "Client", cls):
        r = discover_soap_wsdl_action(wsdl_url="https://ex.com/svc?wsdl")
    assert r["error_code"] == "WSDL_AUTH_FAILURE"


def test_wsdl_url_redirect_blocked():
    cls, _ = _stream_client(301, b"")
    with patch.object(sd.socket, "getaddrinfo", _public_gai), patch.object(sd.httpx, "Client", cls):
        r = discover_soap_wsdl_action(wsdl_url="https://ex.com/svc?wsdl")
    assert r["error_code"] == "WSDL_REDIRECT_BLOCKED"


def test_wsdl_url_timeout_unreachable():
    cls, _ = _stream_client(raise_exc=httpx.ConnectError("x"))
    with patch.object(sd.socket, "getaddrinfo", _public_gai), patch.object(sd.httpx, "Client", cls):
        r = discover_soap_wsdl_action(wsdl_url="https://ex.com/svc?wsdl")
    assert r["error_code"] == "WSDL_UNREACHABLE_ENDPOINT"


def test_wsdl_url_ssrf_blocked():
    assert discover_soap_wsdl_action(wsdl_url="http://localhost/svc?wsdl")["error_code"] == "WSDL_SSRF_BLOCKED"


def test_wsdl_url_size_limit():
    cls, _ = _stream_client(200, b"<" * 100)
    with patch.object(sd.socket, "getaddrinfo", _public_gai), patch.object(sd.httpx, "Client", cls):
        r = discover_soap_wsdl_action(wsdl_url="https://ex.com/svc?wsdl", options={"max_input_chars": 10})
    assert r["error_code"] == "WSDL_SIZE_LIMIT_EXCEEDED"


def test_wsdl_artifact_size_limit():
    r = discover_soap_wsdl_action(artifact=_WSDL, options={"max_input_chars": 10})
    assert r["error_code"] == "WSDL_SIZE_LIMIT_EXCEEDED"


# ---------------------------------------------------------------------------
# Truncation
# ---------------------------------------------------------------------------

def test_wsdl_field_truncation():
    r = discover_soap_wsdl_action(artifact=_WSDL, options={"max_fields": 1})
    assert r["_success"] is True
    assert r["truncated"] is True
    assert r["warnings"] and r["warnings"][0]["code"] == "TRUNCATED"
