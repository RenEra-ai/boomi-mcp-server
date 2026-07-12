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


# ---------------------------------------------------------------------------
# Codex review regressions
# ---------------------------------------------------------------------------

_WSDL_DUP_OPNAME = """<definitions xmlns="http://schemas.xmlsoap.org/wsdl/"
  xmlns:soap="http://schemas.xmlsoap.org/wsdl/soap/" xmlns:tns="http://ex.com/s"
  targetNamespace="http://ex.com/s">
  <message name="AReq"><part name="p" type="xsd:string"/></message>
  <message name="AResp"><part name="p" type="xsd:string"/></message>
  <message name="BReq"><part name="p" type="xsd:string"/></message>
  <message name="BResp"><part name="p" type="xsd:string"/></message>
  <portType name="PtA"><operation name="Do"><input message="tns:AReq"/><output message="tns:AResp"/></operation></portType>
  <portType name="PtB"><operation name="Do"><input message="tns:BReq"/><output message="tns:BResp"/></operation></portType>
  <binding name="BindA" type="tns:PtA"><soap:binding style="document" transport="t"/>
    <operation name="Do"><soap:operation soapAction="a"/></operation></binding>
  <binding name="BindB" type="tns:PtB"><soap:binding style="document" transport="t"/>
    <operation name="Do"><soap:operation soapAction="b"/></operation></binding>
</definitions>"""


def test_same_operation_name_across_porttypes_resolved_per_binding():
    """Two portTypes defining 'Do' must not collide; each binding gets its own
    port type's messages (Codex P2)."""
    r = discover_soap_wsdl_action(artifact=_WSDL_DUP_OPNAME)
    by_name = {b["name"]: b for b in r["bindings"]}
    a_op = by_name["BindA"]["operations"][0]
    b_op = by_name["BindB"]["operations"][0]
    assert a_op["input_message"] == "AReq" and a_op["output_message"] == "AResp"
    assert b_op["input_message"] == "BReq" and b_op["output_message"] == "BResp"


def test_empty_services_messages_imports_consume_node_budget():
    """Services/messages/imports must count against max_nodes so a doc full of
    empty top-level elements cannot exceed the bounded-summary contract (Codex P2)."""
    payload = """<definitions xmlns="http://schemas.xmlsoap.org/wsdl/" targetNamespace="http://ex.com/s">
      <message name="M1"/><message name="M2"/><message name="M3"/>
      <service name="S1"/><service name="S2"/>
      <import namespace="n1" location="l1"/><import namespace="n2" location="l2"/>
    </definitions>"""
    r = discover_soap_wsdl_action(artifact=payload, options={"max_nodes": 2})
    assert r["_success"] is True
    assert r["truncated"] is True
    total_top = len(r["services"]) + len(r["messages"]) + len(r["imports"]) + len(r["bindings"])
    assert total_top == 2  # only max_nodes top-level records emitted


def test_utf16_encoded_doctype_rejected():
    """A UTF-16 fetched document's DOCTYPE must still be rejected — decoding as
    UTF-8 would hide it behind interleaved NULs (Codex P1)."""
    payload = (
        '<?xml version="1.0" encoding="UTF-16"?>'
        '<!DOCTYPE x [<!ENTITY a "b">]>'
        '<definitions xmlns="http://schemas.xmlsoap.org/wsdl/"/>'
    ).encode("utf-16")
    try:
        sd._safe_xml(payload, "WSDL_PARSE_ERROR", "WSDL_INVALID_SPEC")
        raise AssertionError("UTF-16 DOCTYPE was not rejected")
    except sd._DiscoveryError as e:
        assert e.error_code == "WSDL_INVALID_SPEC"


def test_porttype_qname_with_whitespace_resolved():
    """A schema-valid binding/@type with surrounding whitespace ('tns:Pt ') must
    still resolve its portType so the operation's messages are not lost (Codex
    round-5 P2)."""
    payload = (
        '<definitions xmlns="http://schemas.xmlsoap.org/wsdl/" '
        'xmlns:soap="http://schemas.xmlsoap.org/wsdl/soap/" xmlns:tns="http://ex.com/s" '
        'targetNamespace="http://ex.com/s">'
        '<message name="R"><part name="p" type="xsd:int"/></message>'
        '<portType name="Pt"><operation name="Do"><input message="tns:R"/></operation></portType>'
        '<binding name="B" type="tns:Pt  "><soap:binding style="d" transport="t"/>'
        '<operation name="Do"><soap:operation soapAction="a"/></operation></binding>'
        '</definitions>'
    )
    r = discover_soap_wsdl_action(artifact=payload)
    op = r["bindings"][0]["operations"][0]
    assert op["input_message"] == "R"


def test_porttype_declaration_name_with_whitespace_resolved():
    """Whitespace in the portType/@name DECLARATION must normalize to match the
    (already-normalized) binding/@type reference — both sides of the key (Codex
    round-6 P2)."""
    payload = (
        '<definitions xmlns="http://schemas.xmlsoap.org/wsdl/" '
        'xmlns:soap="http://schemas.xmlsoap.org/wsdl/soap/" xmlns:tns="http://ex.com/s" '
        'targetNamespace="http://ex.com/s">'
        '<message name="R"><part name="p" type="xsd:int"/></message>'
        '<portType name=" Pt "><operation name="Do"><input message="tns:R"/></operation></portType>'
        '<binding name="B" type="tns:Pt"><soap:binding style="d" transport="t"/>'
        '<operation name="Do"><soap:operation soapAction="a"/></operation></binding>'
        '</definitions>'
    )
    r = discover_soap_wsdl_action(artifact=payload)
    op = r["bindings"][0]["operations"][0]
    assert op["input_message"] == "R"


def test_binding_declaration_whitespace_matches_port_reference_and_fallback():
    """A whitespaced binding/@name declaration must normalize like the port's
    binding reference, so names match AND the soap-version fallback resolves
    (Codex round-7 P2)."""
    payload = (
        '<definitions xmlns="http://schemas.xmlsoap.org/wsdl/" '
        'xmlns:soap="http://schemas.xmlsoap.org/wsdl/soap/" xmlns:tns="http://ex.com/s" '
        'targetNamespace="http://ex.com/s">'
        '<message name="R"><part name="p" type="xsd:int"/></message>'
        '<portType name="Pt"><operation name="Do"><input message="tns:R"/></operation></portType>'
        '<binding name="B " type="tns:Pt"><soap:binding style="d" transport="t"/>'
        '<operation name="Do"><soap:operation soapAction="a"/></operation></binding>'
        '<service name="S"><port name="P" binding="tns:B "></port></service>'
        '</definitions>'
    )
    r = discover_soap_wsdl_action(artifact=payload)
    assert r["bindings"][0]["name"] == "B"
    port = r["services"][0]["ports"][0]
    assert port["binding"] == "B"  # normalized declaration == normalized reference
    assert port["soap_version"] == "1.1"  # fallback lookup via normalized binding name


def test_wsdl_import_namespace_sanitized():
    """An import namespace URI carrying credentials must be sanitized like the
    location, not echoed verbatim (§6 impl-review #4)."""
    payload = (
        '<definitions xmlns="http://schemas.xmlsoap.org/wsdl/">'
        '<import namespace="http://user:SEKRET@ns.example.com/x?token=abc" '
        'location="https://u:p@ex.com/other.wsdl?key=9"/></definitions>'
    )
    r = discover_soap_wsdl_action(artifact=payload)
    assert "SEKRET" not in json.dumps(r) and "token" not in json.dumps(r) and "key=9" not in json.dumps(r)
    assert r["imports"][0]["namespace"] == "http://ns.example.com/x"


def test_wsdl_top_level_node_budget_single_path():
    """The single semantic node budget bounds top-level constructs
    (services/bindings/messages/imports) with accurate, non-overlapping omitted
    counts (repo-gate: single XML node budget)."""
    payload = (
        '<definitions xmlns="http://schemas.xmlsoap.org/wsdl/">'
        '<message name="M1"/><message name="M2"/><message name="M3"/>'
        '</definitions>'
    )
    r = discover_soap_wsdl_action(artifact=payload, options={"max_nodes": 1})
    assert r["_success"] is True and r["truncated"] is True
    assert len(r["messages"]) == 1
    # exactly 2 omitted (3 messages - 1 budget), no double-counting
    msg_reasons = [x for x in r["truncation"]["reasons"] if x["kind"] == "nodes:messages"]
    assert msg_reasons and msg_reasons[0]["omitted"] == 2


def test_long_porttype_names_do_not_collide_on_lookup():
    """Two portType names that share their first 512 chars must resolve to their
    OWN messages: lookups use the FULL normalized name, only the emitted value is
    clipped (repo-gate: preserve full QNames when resolving bindings)."""
    base = "P" * 600
    a, b = base + "A", base + "B"  # differ only after char 600
    payload = (
        '<definitions xmlns="http://schemas.xmlsoap.org/wsdl/" '
        'xmlns:soap="http://schemas.xmlsoap.org/wsdl/soap/" xmlns:tns="http://ex.com/s" '
        'targetNamespace="http://ex.com/s">'
        '<message name="MA"/><message name="MB"/>'
        f'<portType name="{a}"><operation name="Do"><input message="tns:MA"/></operation></portType>'
        f'<portType name="{b}"><operation name="Do"><input message="tns:MB"/></operation></portType>'
        f'<binding name="BA" type="tns:{a}"><soap:binding style="d" transport="t"/>'
        '<operation name="Do"><soap:operation soapAction="a"/></operation></binding>'
        f'<binding name="BB" type="tns:{b}"><soap:binding style="d" transport="t"/>'
        '<operation name="Do"><soap:operation soapAction="b"/></operation></binding>'
        '</definitions>'
    )
    r = discover_soap_wsdl_action(artifact=payload)
    by = {x["name"]: x for x in r["bindings"]}
    assert by["BA"]["operations"][0]["input_message"] == "MA"
    assert by["BB"]["operations"][0]["input_message"] == "MB"


def test_wsdl_network_path_namespace_sanitized():
    """A network-path (scheme-relative) namespace reference with userinfo must be
    sanitized — the '://'-only check missed it (§6 re-review #3)."""
    payload = (
        '<definitions xmlns="http://schemas.xmlsoap.org/wsdl/">'
        '<import namespace="//user:SEKRET@host.example.com/x?token=abc" location="//u:p@ex.com/o?key=9"/>'
        '</definitions>'
    )
    r = discover_soap_wsdl_action(artifact=payload)
    assert "SEKRET" not in json.dumps(r) and "token" not in json.dumps(r) and "key=9" not in json.dumps(r)
    assert r["imports"][0]["namespace"] == "//host.example.com/x"


def test_utf16_doctype_rejected_via_url_mode():
    payload = (
        '<?xml version="1.0" encoding="UTF-16"?><!DOCTYPE x>'
        '<definitions xmlns="http://schemas.xmlsoap.org/wsdl/"/>'
    ).encode("utf-16")
    cls, _ = _stream_client(200, payload)
    with patch.object(sd.socket, "getaddrinfo", _public_gai), patch.object(sd.httpx, "Client", cls):
        r = discover_soap_wsdl_action(wsdl_url="https://ex.com/svc?wsdl")
    assert r["error_code"] == "WSDL_INVALID_SPEC"
