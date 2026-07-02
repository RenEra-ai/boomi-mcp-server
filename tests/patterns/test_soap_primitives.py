"""Issue #126 (M5.10) — tests for the ``soap_fetch`` / ``soap_send`` primitives.

Covers registry discovery, connection create/reuse, the EXECUTE operation with
caller-provided WSDL metadata, the source/target fragments, dependency refs, and
the absence of any canned SOAP envelope/payload surface. All tests are pure — no
live Boomi calls; XML + validation is delegated to the SOAP Client builders.
"""

from __future__ import annotations

import pytest
from pydantic import ValidationError

from boomi_mcp.patterns.base import PatternKind, PrimitiveBuildContext
from boomi_mcp.patterns.primitives import SoapFetchPrimitive, SoapSendPrimitive
from boomi_mcp.patterns.registry import PatternRegistry


def _ctx() -> PrimitiveBuildContext:
    return PrimitiveBuildContext(integration_name="Demo", component_prefix="DEMO", folder_path="/Demo")


def _wsdl_metadata():
    return {
        "operation_name": "Ping",
        "soap_action": "http://x/Ping",
        "metadata_connection_url": "http://host/svc.asmx",
        "service_name": "Svc",
        "service_namespace": "http://x",
        "port_name": "SvcSoap",
        "binding_style": "document",
        "binding_use": "literal",
        "binding_protocol": "soap_1_1",
        "operation_style": "document",
        "operation_use": "literal",
        "input_message_name": "Svc_Ping_In",
        "input_message_namespace": "http://x",
        "output_message_name": "Svc_Ping_Out",
        "output_message_namespace": "http://x",
        "input_parameters": [{"name": "Ping", "element_name": "Ping", "element_ns": "http://x"}],
        "output_parameters": [{"name": "PingResponse", "element_name": "PingResponse", "element_ns": "http://x"}],
        "rpc_optional_parameters": True,
        "using_envelope": True,
    }


def _params(**overrides):
    params = {
        "key_prefix": "cust",
        "connection": {
            "mode": "create",
            "wsdl_url": "https://host/svc.asmx?wsdl",
            "endpoint_url": "https://host/svc.asmx",
            "security": "NETWORK_AUTH",
            "username": "svc",
            "credential_ref": "credential://vendor/soap",
        },
        "operation": {
            "request_profile_id": "$ref:cust_req_profile",
            "response_profile_id": "$ref:cust_resp_profile",
            "wsdl_metadata": _wsdl_metadata(),
        },
    }
    params.update(overrides)
    return params


@pytest.mark.parametrize("prim,name", [(SoapFetchPrimitive, "soap_fetch"), (SoapSendPrimitive, "soap_send")])
def test_registry_discovers_primitive(prim, name):
    reg = PatternRegistry.from_package("boomi_mcp.patterns")
    assert reg.get(name) is prim
    assert prim.metadata.kind == PatternKind.PRIMITIVE
    assert prim.metadata.name == name


@pytest.mark.parametrize("prim", [SoapFetchPrimitive, SoapSendPrimitive])
def test_emit_components_create_connection_and_operation(prim):
    comps = prim.emit_components(_ctx(), prim.validate_parameters(_params()))
    assert [c.type for c in comps] == ["connector-settings", "connector-action"]
    conn, op = comps
    assert conn.config["connector_type"] == "soap_client"
    assert conn.config["wsdl_url"] == "https://host/svc.asmx?wsdl"
    assert op.config["connector_type"] == "soap_client"
    assert op.config["operation_mode"] == "execute"
    assert op.config["wsdl_metadata"]["operation_name"] == "Ping"
    # Operation depends on the connection + both XML profiles.
    assert conn.key in op.depends_on
    assert "cust_req_profile" in op.depends_on
    assert "cust_resp_profile" in op.depends_on


def test_soap_fetch_emits_source_fragment_execute():
    frag = SoapFetchPrimitive.emit_fragment(_ctx(), SoapFetchPrimitive.validate_parameters(_params()))
    assert "source" in frag["process_config"]
    src = frag["process_config"]["source"]
    assert src["connector_type"] == "soap_client"
    assert src["action_type"] == "EXECUTE"
    assert all(k.startswith("cust_") for k in frag["depends_on"])


def test_soap_send_emits_target_fragment_execute():
    frag = SoapSendPrimitive.emit_fragment(_ctx(), SoapSendPrimitive.validate_parameters(_params()))
    assert "target" in frag["process_config"]
    tgt = frag["process_config"]["target"]
    assert tgt["connector_type"] == "soap_client"
    assert tgt["action_type"] == "EXECUTE"


def test_source_and_target_roles_are_collision_free():
    # A flow emitting both a soap_fetch source and a soap_send target under the
    # same key_prefix must produce distinct component keys.
    fetch = SoapFetchPrimitive.emit_components(_ctx(), SoapFetchPrimitive.validate_parameters(_params()))
    send = SoapSendPrimitive.emit_components(_ctx(), SoapSendPrimitive.validate_parameters(_params()))
    fetch_keys = {c.key for c in fetch}
    send_keys = {c.key for c in send}
    assert fetch_keys.isdisjoint(send_keys)


@pytest.mark.parametrize("prim", [SoapFetchPrimitive, SoapSendPrimitive])
def test_connection_reuse_reference_only(prim):
    params = _params(connection={"mode": "reuse", "component_id": "11111111-1111-1111-1111-111111111111"})
    comps = prim.emit_components(_ctx(), prim.validate_parameters(params))
    conn = comps[0]
    assert conn.config.get("reference_only") is True
    assert conn.component_id == "11111111-1111-1111-1111-111111111111"


@pytest.mark.parametrize("prim", [SoapFetchPrimitive, SoapSendPrimitive])
@pytest.mark.parametrize("canned", ["soap_body", "soap_envelope", "raw_envelope", "request_payload", "headers"])
def test_canned_envelope_fields_rejected_at_boundary(prim, canned):
    params = _params()
    params["operation"][canned] = "<x/>"
    with pytest.raises(ValidationError):
        prim.validate_parameters(params)


@pytest.mark.parametrize("prim", [SoapFetchPrimitive, SoapSendPrimitive])
def test_incomplete_wsdl_metadata_rejected_at_boundary(prim):
    params = _params()
    del params["operation"]["wsdl_metadata"]["service_name"]
    with pytest.raises(ValidationError):
        prim.validate_parameters(params)


@pytest.mark.parametrize("prim", [SoapFetchPrimitive, SoapSendPrimitive])
def test_required_builders_declared(prim):
    assert "SoapClientConnectionBuilder" in prim.required_builders
    assert "SoapClientOperationBuilder" in prim.required_builders
