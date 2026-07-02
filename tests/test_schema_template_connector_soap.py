"""Schema-template tests for the SOAP Client connector protocols (#126, M5.10).

Covers connector-settings 'soap.client' and connector-action 'soap.operation'.
Anti-template policy: examples MUST use angle-bracket placeholders and
`credential://...` references — no canned SOAP envelopes, WSDL operation values,
or plaintext credentials.
"""

import pytest

from boomi_mcp.categories.meta_tools import get_schema_template_action

_FORBIDDEN_SECRET_FIELDS = ("password", "password_ref", "secret", "token", "access_token", "client_secret")
# Canned SOAP envelope / raw-XML markers that must never appear in a placeholder
# template. (SQL substrings are not relevant to a SOAP connector template.)
_FORBIDDEN_TEMPLATE_SUBSTRINGS = (
    "<soap:envelope",
    "<soapenv:",
    "<?xml",
    "<envelope",
)


def _call(**kwargs):
    return get_schema_template_action(resource_type="component", operation="create", **kwargs)


# ---------------------------------------------------------------------------
# Dispatch
# ---------------------------------------------------------------------------


def test_settings_overview_lists_soap_client():
    result = _call(component_type="connector-settings")
    assert result["_success"] is True
    assert "soap.client" in result["available_protocols"]


def test_action_overview_lists_soap_operation():
    result = _call(component_type="connector-action")
    assert result["_success"] is True
    assert "soap.operation" in result["available_protocols"]


def test_unknown_soap_protocol_returns_structured_error():
    result = _call(component_type="connector-settings", protocol="soap.bogus")
    assert result["_success"] is False
    assert "soap.client" in result["valid_protocols"]


# ---------------------------------------------------------------------------
# soap.client (connection)
# ---------------------------------------------------------------------------


def test_soap_client_template_structure():
    r = _call(component_type="connector-settings", protocol="soap.client")
    assert r["_success"] is True
    assert r["protocol"] == "soap.client"
    assert r["boomi_subtype"] == "wssoapclientsdk"
    assert set(r["public_aliases"]) == {"soap_client", "web_services_soap_client", "wssoapclientsdk"}
    for field in ("connector_type", "component_name", "wsdl_url", "endpoint_url", "username", "credential_ref"):
        assert field in r["required"]
    assert r["defaults"]["connector_type"] == "soap_client"
    assert r["supported_security_modes"] == ["NETWORK_AUTH"]


def test_soap_client_credential_ref_uses_credential_scheme():
    r = _call(component_type="connector-settings", protocol="soap.client")
    assert r["template"]["credential_ref"].startswith("credential://")


def test_soap_client_no_forbidden_secret_keys_or_payloads():
    r = _call(component_type="connector-settings", protocol="soap.client")

    def _walk(obj):
        if isinstance(obj, dict):
            for k, v in obj.items():
                assert k not in _FORBIDDEN_SECRET_FIELDS, f"leaks secret key {k!r}"
                _walk(v)
        elif isinstance(obj, list):
            for item in obj:
                _walk(item)

    _walk(r["template"])
    blob = repr(r).lower()
    for forbidden in _FORBIDDEN_TEMPLATE_SUBSTRINGS:
        assert forbidden not in blob, f"leaks forbidden substring {forbidden!r}"


# ---------------------------------------------------------------------------
# soap.operation (connector-action)
# ---------------------------------------------------------------------------


def test_soap_operation_template_structure():
    r = _call(component_type="connector-action", protocol="soap.operation")
    assert r["_success"] is True
    assert r["protocol"] == "soap.operation"
    assert r["boomi_subtype"] == "wssoapclientsdk"
    assert r["defaults"]["operation_mode"] == "execute"
    for field in ("operation_mode", "connection_ref_key", "request_profile_id", "response_profile_id", "wsdl_metadata"):
        assert field in r["required"]


def test_soap_operation_profiles_are_xml_only():
    r = _call(component_type="connector-action", protocol="soap.operation")
    assert r["template"]["request_profile_type"] == "xml"
    assert r["template"]["response_profile_type"] == "xml"


def test_soap_operation_documents_wsdl_metadata_placeholders():
    r = _call(component_type="connector-action", protocol="soap.operation")
    meta = r["template"]["wsdl_metadata"]
    # WSDL-derived values are placeholders, never canned real operation values.
    assert meta["operation_name"].startswith("<<")
    assert isinstance(meta["input_parameters"], list) and meta["input_parameters"]
    assert meta["binding_protocol"] == "soap_1_1"


def test_soap_operation_no_canned_payload_and_documents_constraint():
    r = _call(component_type="connector-action", protocol="soap.operation")
    blob = repr(r).lower()
    for forbidden in _FORBIDDEN_TEMPLATE_SUBSTRINGS:
        assert forbidden not in blob, f"leaks forbidden substring {forbidden!r}"
    # The template must not carry a canned request-body field.
    assert "soap_body" not in r["template"]
    assert "no_canned_payload" in r["constraints"]
