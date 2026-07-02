"""Tests for SoapClientConnectionBuilder (issue #126, M5.10).

XML shape byte-locked (semantically — insignificant whitespace ignored, as with
the REST/DB builders) against the verified `work`-account live exports:
  connection 2dc6f20a-3bb8-45c8-b50d-5c753364ad08 ("3E SOAP", concrete WSDL url)
  connection 456db4ba-a391-47f2-be9f-5e9092a4f756 ("Walls SOAP API", SET BY EXTENSION)

Builder emits Boomi Web Services SOAP Client connection components
(subType="wssoapclientsdk"). v1 ships only NETWORK_AUTH security and XML profiles.
"""

import xml.etree.ElementTree as ET

import pytest

from boomi_mcp.categories.components.builders.connector_builder import (
    BuilderValidationError,
    SoapClientConnectionBuilder,
    SOAP_CLIENT_SUBTYPE,
    _resolve_soap_client_connector_type,
    get_connector_builder,
)

NS = {"bns": "http://api.platform.boomi.com/"}


# Live `work` connection 2dc6f20a — the byte-lock reference (concrete WSDL url).
_LIVE_CONN_OBJECT = (
    '<bns:object xmlns:bns="http://api.platform.boomi.com/">'
    '<GenericConnectionConfig xmlns="">'
    '<field id="url" type="string" value="https://&lt;HOST&gt;/&lt;DB_ENV&gt;/WebUI/TransactionService.asmx?wsdl"/>'
    '<field id="endpoint" type="string" value="https://&lt;HOST&gt;/&lt;DB_ENV&gt;/WebUI/TransactionService.asmx"/>'
    '<field id="security" type="string" value="NETWORK_AUTH"/>'
    '<field id="username" type="string" value="SET_BY_EXTENSION"/>'
    '<field id="password" type="password" value=""/>'
    '<field id="clientsslalias" type="privatecertificate"/>'
    '<field id="trustsslalias" type="publiccertificate"/>'
    '<field id="wsssecurityOptions" type="wssecurity"><WSSecurityOptions/></field>'
    '</GenericConnectionConfig>'
    '</bns:object>'
)


def _strip_ws(el):
    if el.text is not None and el.text.strip() == "":
        el.text = None
    if el.tail is not None and el.tail.strip() == "":
        el.tail = None
    for c in el:
        _strip_ws(c)
    return el


def _canon_object(component_xml):
    root = ET.fromstring(component_xml)
    obj = root.find("bns:object", NS)
    _strip_ws(obj)
    return ET.canonicalize(ET.tostring(obj))


def _minimal_config(**overrides):
    config = {
        "connector_type": "soap_client",
        "component_name": "3E SOAP",
        "folder_name": "Import",
        "wsdl_url": "https://<HOST>/<DB_ENV>/WebUI/TransactionService.asmx?wsdl",
        "endpoint_url": "https://<HOST>/<DB_ENV>/WebUI/TransactionService.asmx",
        "security": "NETWORK_AUTH",
        "username": "SET_BY_EXTENSION",
        "credential_ref": "credential://vault/3e/soap",
    }
    config.update(overrides)
    return config


# ---------------------------------------------------------------------------
# Golden XML shape
# ---------------------------------------------------------------------------


def test_connection_xml_matches_live_shape():
    built = SoapClientConnectionBuilder().build(**_minimal_config())
    assert _canon_object(built) == _strip_ws_str(_LIVE_CONN_OBJECT)


def _strip_ws_str(object_xml):
    root = ET.fromstring(object_xml)
    _strip_ws(root)
    return ET.canonicalize(ET.tostring(root))


def test_connection_header_type_and_subtype():
    built = SoapClientConnectionBuilder().build(**_minimal_config())
    root = ET.fromstring(built)
    assert root.attrib["type"] == "connector-settings"
    assert root.attrib["subType"] == "wssoapclientsdk"
    # password is emitted empty; encryptedValues is empty (creds via extension)
    assert root.find("bns:encryptedValues", NS) is not None
    assert list(root.find("bns:encryptedValues", NS)) == []


def test_connection_cert_aliases_self_closing_when_empty_and_value_when_set():
    built_empty = SoapClientConnectionBuilder().build(**_minimal_config())
    assert '<field id="clientsslalias" type="privatecertificate"/>' in built_empty
    assert '<field id="trustsslalias" type="publiccertificate"/>' in built_empty

    cert = "21f598a6-1d90-4578-a35a-d0350c50b747"
    built = SoapClientConnectionBuilder().build(
        **_minimal_config(client_ssl_alias=cert, trust_ssl_alias=cert)
    )
    assert f'<field id="clientsslalias" type="privatecertificate" value="{cert}"/>' in built
    assert f'<field id="trustsslalias" type="publiccertificate" value="{cert}"/>' in built


# ---------------------------------------------------------------------------
# Alias resolution
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("alias", ["soap_client", "web_services_soap_client", "WSSOAPCLIENTSDK", "wssoapclientsdk"])
def test_alias_resolves_to_canonical(alias):
    assert _resolve_soap_client_connector_type(alias) == SOAP_CLIENT_SUBTYPE


@pytest.mark.parametrize("ambiguous", ["soap", "wss", "web_services", "soap_server", "SOAP", ""])
def test_ambiguous_aliases_rejected(ambiguous):
    assert _resolve_soap_client_connector_type(ambiguous) is None


def test_registry_maps_soap_aliases():
    assert isinstance(get_connector_builder("soap_client"), SoapClientConnectionBuilder)
    assert isinstance(get_connector_builder("web_services_soap_client"), SoapClientConnectionBuilder)
    assert isinstance(get_connector_builder("wssoapclientsdk"), SoapClientConnectionBuilder)


# ---------------------------------------------------------------------------
# Validation paths
# ---------------------------------------------------------------------------


def test_missing_component_name_rejected():
    err = SoapClientConnectionBuilder.validate_config(_minimal_config(component_name=""))
    assert err is not None and err.error_code == "SOAP_CONNECTOR_VALIDATION_FAILED"
    assert err.field == "component_name"


def test_missing_wsdl_url_rejected():
    cfg = _minimal_config()
    del cfg["wsdl_url"]
    err = SoapClientConnectionBuilder.validate_config(cfg)
    assert err is not None and err.error_code == "SOAP_WSDL_URL_REQUIRED"


def test_missing_endpoint_url_rejected():
    cfg = _minimal_config()
    del cfg["endpoint_url"]
    err = SoapClientConnectionBuilder.validate_config(cfg)
    assert err is not None and err.error_code == "SOAP_ENDPOINT_URL_REQUIRED"


def test_unsupported_security_rejected_with_escape_hatch():
    err = SoapClientConnectionBuilder.validate_config(_minimal_config(security="OAUTH2"))
    assert err is not None and err.error_code == "SOAP_UNSUPPORTED_SECURITY"
    assert "config.xml" in (err.hint or "")


def test_missing_username_rejected():
    cfg = _minimal_config()
    del cfg["username"]
    err = SoapClientConnectionBuilder.validate_config(cfg)
    assert err is not None and err.error_code == "SOAP_CONNECTOR_VALIDATION_FAILED"
    assert err.field == "username"


def test_missing_credential_ref_rejected():
    cfg = _minimal_config()
    del cfg["credential_ref"]
    err = SoapClientConnectionBuilder.validate_config(cfg)
    assert err is not None and err.error_code == "MISSING_CREDENTIAL_REF"


def test_malformed_credential_ref_rejected():
    err = SoapClientConnectionBuilder.validate_config(
        _minimal_config(credential_ref="not-a-credential-uri")
    )
    assert err is not None and err.error_code == "SOAP_SECRET_VALUE_FORBIDDEN"


def test_plaintext_secret_rejected():
    err = SoapClientConnectionBuilder.validate_config(_minimal_config(password="hunter2"))
    assert err is not None and err.error_code == "PLAINTEXT_SECRET_REJECTED"


def test_unknown_field_rejected_with_escape_hatch():
    err = SoapClientConnectionBuilder.validate_config(_minimal_config(sneaky_field="x"))
    assert err is not None and err.error_code == "SOAP_UNSUPPORTED_FIELD"
    assert err.field == "sneaky_field"
    assert "config.xml" in (err.hint or "")


def test_nonempty_wss_security_options_rejected():
    err = SoapClientConnectionBuilder.validate_config(
        _minimal_config(wss_security_options={"policy": "UsernameToken"})
    )
    assert err is not None and err.error_code == "UNSUPPORTED_SOAP_WSS_SECURITY"


def test_malformed_cert_alias_rejected():
    err = SoapClientConnectionBuilder.validate_config(
        _minimal_config(client_ssl_alias="not-a-uuid")
    )
    assert err is not None and err.error_code == "SOAP_CONNECTOR_VALIDATION_FAILED"
    assert err.field == "client_ssl_alias"


def test_misplaced_set_by_extension_rejected():
    # SET_BY_EXTENSION is only valid on the three extension-bound fields.
    err = SoapClientConnectionBuilder.validate_config(
        _minimal_config(description="SET_BY_EXTENSION")
    )
    assert err is not None and err.error_code == "SET_BY_EXTENSION_FIELD_NOT_ALLOWED"


def test_set_by_extension_allowed_on_endpoint_and_username():
    # Live connection B stores url/endpoint/username as SET BY EXTENSION — the
    # underscore sentinel is accepted on those three fields.
    cfg = _minimal_config(
        wsdl_url="SET_BY_EXTENSION",
        endpoint_url="SET_BY_EXTENSION",
        username="SET_BY_EXTENSION",
    )
    assert SoapClientConnectionBuilder.validate_config(cfg) is None


def test_build_raises_on_invalid_config():
    with pytest.raises(BuilderValidationError):
        SoapClientConnectionBuilder().build(**_minimal_config(security="BASIC"))
