"""Tests for SoapClientOperationBuilder (issue #126, M5.10).

Operation XML byte-locked (semantically) against live `work`-account export
operation 0131372a-4805-4e7d-b592-5193e2f862da ("3E SOAP Ping", EXECUTE),
including the INPUT cookie's serialized WSDL-metadata document.
"""

import xml.etree.ElementTree as ET

import pytest

from boomi_mcp.categories.components.builders.connector_builder import (
    BuilderValidationError,
    SoapClientOperationBuilder,
    get_connector_action_builder,
)

NS = {"bns": "http://api.platform.boomi.com/"}


_LIVE_OP_OBJECT = (
    '<bns:object xmlns:bns="http://api.platform.boomi.com/">'
    '<Operation xmlns="" returnApplicationErrors="false" trackResponse="false">'
    '<Archiving directory="" enabled="false"/>'
    '<Configuration>'
    '<GenericOperationConfig objectTypeId="Ping" objectTypeName="Ping" operationType="EXECUTE" '
    'requestProfile="a37dd5e3-c21e-4435-9356-6b7c33e6ef94" requestProfileType="xml" '
    'responseProfile="66707e59-09ed-4323-a064-bfa89b9c9871" responseProfileType="xml">'
    '<field id="exposeRequestEnvelope" type="boolean" value="true"/>'
    '<field id="exposeResponseEnvelope" type="boolean" value="false"/>'
    '<field id="attachmentCache" type="component"/>'
    '<cookie role="INPUT"><value>'
    '&lt;?xml version="1.0" encoding="UTF-8" standalone="yes"?&gt;'
    '&lt;WebServiceOperation operationName="Ping" soapAction="http://tempuri.org//ServiceExecuteProcess/TransactionService/Ping"&gt;'
    '&lt;WebServiceConnection url="http://elite3x.intapp.net/te_3e_sample303/Web/TransactionService.asmx"/&gt;'
    '&lt;WebServiceMetaData&gt;&lt;WebServiceDescription serviceName="TransactionService" '
    'serviceNamespace="http://tempuri.org//ServiceExecuteProcess" portName="TransactionServiceSoap_soap" '
    'bindingStyle="document" bindingUse="literal" bindingProtocol="soap_1_1"&gt;&lt;Operations&gt;'
    '&lt;WebServiceOperation operationName="Ping" soapAction="http://tempuri.org//ServiceExecuteProcess/TransactionService/Ping" '
    'operationUse="literal" operationStyle="document"&gt;'
    '&lt;Inputs messageName="TransactionService_Ping_InputMessage" messageNamespace="http://tempuri.org//ServiceExecuteProcess"&gt;'
    '&lt;Parameter hidden="false" soapLocation="body"&gt;&lt;name&gt;Ping&lt;/name&gt;&lt;elementName&gt;Ping&lt;/elementName&gt;'
    '&lt;elementNS&gt;http://tempuri.org//ServiceExecuteProcess&lt;/elementNS&gt;&lt;/Parameter&gt;&lt;/Inputs&gt;'
    '&lt;Outputs messageName="TransactionService_Ping_OutputMessage" messageNamespace="http://tempuri.org//ServiceExecuteProcess"&gt;'
    '&lt;Parameter hidden="false" soapLocation="body"&gt;&lt;name&gt;PingResponse&lt;/name&gt;&lt;elementName&gt;PingResponse&lt;/elementName&gt;'
    '&lt;elementNS&gt;http://tempuri.org//ServiceExecuteProcess&lt;/elementNS&gt;&lt;/Parameter&gt;&lt;/Outputs&gt;'
    '&lt;CustomConfiguration&gt;&lt;comBoomiWsRpcOptionalParameters&gt;true&lt;/comBoomiWsRpcOptionalParameters&gt;'
    '&lt;comBoomiWsUsingEnvelope&gt;true&lt;/comBoomiWsUsingEnvelope&gt;&lt;/CustomConfiguration&gt;'
    '&lt;/WebServiceOperation&gt;&lt;/Operations&gt;&lt;/WebServiceDescription&gt;&lt;/WebServiceMetaData&gt;&lt;/WebServiceOperation&gt;'
    '</value></cookie>'
    '<Options><QueryOptions><Fields><ConnectorObject name="Ping"><FieldList/></ConnectorObject></Fields><Inputs/></QueryOptions></Options>'
    '</GenericOperationConfig>'
    '</Configuration>'
    '<Tracking><TrackedFields/></Tracking>'
    '<Caching/>'
    '</Operation>'
    '</bns:object>'
)


def _wsdl_metadata():
    return {
        "operation_name": "Ping",
        "soap_action": "http://tempuri.org//ServiceExecuteProcess/TransactionService/Ping",
        "metadata_connection_url": "http://elite3x.intapp.net/te_3e_sample303/Web/TransactionService.asmx",
        "service_name": "TransactionService",
        "service_namespace": "http://tempuri.org//ServiceExecuteProcess",
        "port_name": "TransactionServiceSoap_soap",
        "binding_style": "document",
        "binding_use": "literal",
        "binding_protocol": "soap_1_1",
        "operation_style": "document",
        "operation_use": "literal",
        "input_message_name": "TransactionService_Ping_InputMessage",
        "input_message_namespace": "http://tempuri.org//ServiceExecuteProcess",
        "output_message_name": "TransactionService_Ping_OutputMessage",
        "output_message_namespace": "http://tempuri.org//ServiceExecuteProcess",
        "input_parameters": [{"name": "Ping", "element_name": "Ping", "element_ns": "http://tempuri.org//ServiceExecuteProcess"}],
        "output_parameters": [{"name": "PingResponse", "element_name": "PingResponse", "element_ns": "http://tempuri.org//ServiceExecuteProcess"}],
        "rpc_optional_parameters": True,
        "using_envelope": True,
    }


def _minimal_config(**overrides):
    config = {
        "connector_type": "soap_client",
        "component_name": "3E SOAP Ping",
        "folder_name": "Import",
        "operation_mode": "execute",
        "connection_ref_key": "soap_conn",
        "request_profile_id": "a37dd5e3-c21e-4435-9356-6b7c33e6ef94",
        "response_profile_id": "66707e59-09ed-4323-a064-bfa89b9c9871",
        "wsdl_metadata": _wsdl_metadata(),
    }
    config.update(overrides)
    return config


def _strip_ws(el):
    if el.text is not None and el.text.strip() == "":
        el.text = None
    if el.tail is not None and el.tail.strip() == "":
        el.tail = None
    for c in el:
        _strip_ws(c)
    return el


def _canon(component_xml, from_object=False):
    root = ET.fromstring(component_xml)
    obj = root if from_object else root.find("bns:object", NS)
    _strip_ws(obj)
    return ET.canonicalize(ET.tostring(obj))


# ---------------------------------------------------------------------------
# Golden XML shape (includes the cookie WSDL-metadata document)
# ---------------------------------------------------------------------------


def test_operation_xml_matches_live_shape():
    built = SoapClientOperationBuilder().build(**_minimal_config())
    assert _canon(built) == _canon(_LIVE_OP_OBJECT, from_object=True)


def test_operation_header_type_and_subtype():
    built = SoapClientOperationBuilder().build(**_minimal_config())
    root = ET.fromstring(built)
    assert root.attrib["type"] == "connector-action"
    assert root.attrib["subType"] == "wssoapclientsdk"


def test_object_type_defaults_from_operation_name():
    built = SoapClientOperationBuilder().build(**_minimal_config())
    assert 'objectTypeId="Ping"' in built and 'objectTypeName="Ping"' in built


def test_cookie_value_escapes_only_angle_brackets_not_quotes():
    # The cookie <value> is text-content escaped: quotes stay literal, < / > escape.
    built = SoapClientOperationBuilder().build(**_minimal_config())
    assert 'operationName="Ping"' in built  # literal quotes inside <value>
    assert "&lt;WebServiceOperation" in built


def test_attachment_cache_self_closing_when_absent_and_value_when_set():
    built = SoapClientOperationBuilder().build(**_minimal_config())
    assert '<field id="attachmentCache" type="component"/>' in built
    cache = "21f598a6-1d90-4578-a35a-d0350c50b747"
    built2 = SoapClientOperationBuilder().build(**_minimal_config(attachment_cache_id=cache))
    assert f'<field id="attachmentCache" type="component" value="{cache}"/>' in built2


# ---------------------------------------------------------------------------
# Validation paths
# ---------------------------------------------------------------------------


def test_non_execute_mode_rejected():
    err = SoapClientOperationBuilder.validate_config(_minimal_config(operation_mode="get"))
    assert err is not None and err.error_code == "UNSUPPORTED_SOAP_OPERATION_MODE"


def test_missing_connection_ref_key_rejected():
    cfg = _minimal_config()
    del cfg["connection_ref_key"]
    err = SoapClientOperationBuilder.validate_config(cfg)
    assert err is not None and err.error_code == "SOAP_CONNECTION_REF_REQUIRED"


def test_missing_request_profile_rejected():
    cfg = _minimal_config()
    del cfg["request_profile_id"]
    err = SoapClientOperationBuilder.validate_config(cfg)
    assert err is not None and err.error_code == "SOAP_OPERATION_VALIDATION_FAILED"
    assert err.field == "request_profile_id"


def test_non_xml_profile_type_rejected():
    err = SoapClientOperationBuilder.validate_config(_minimal_config(request_profile_type="json"))
    assert err is not None and err.error_code == "UNSUPPORTED_SOAP_PROFILE_TYPE"


def test_empty_profile_ref_token_rejected():
    err = SoapClientOperationBuilder.validate_config(_minimal_config(request_profile_id="$ref:"))
    assert err is not None and err.error_code == "SOAP_PROFILE_REF_UNRESOLVED"


@pytest.mark.parametrize("forbidden", ["soap_body", "soap_envelope", "raw_envelope", "request_payload", "headers"])
def test_canned_payload_fields_rejected(forbidden):
    err = SoapClientOperationBuilder.validate_config(_minimal_config(**{forbidden: "<x/>"}))
    assert err is not None and err.error_code == "SOAP_UNSUPPORTED_FIELD"
    assert err.field == forbidden


def test_unknown_field_rejected():
    err = SoapClientOperationBuilder.validate_config(_minimal_config(sneaky="x"))
    assert err is not None and err.error_code == "SOAP_UNSUPPORTED_FIELD"


def test_missing_wsdl_metadata_rejected():
    cfg = _minimal_config()
    del cfg["wsdl_metadata"]
    err = SoapClientOperationBuilder.validate_config(cfg)
    assert err is not None and err.error_code == "SOAP_WSDL_METADATA_REQUIRED"


def test_incomplete_wsdl_metadata_scalar_rejected():
    meta = _wsdl_metadata()
    del meta["service_name"]
    err = SoapClientOperationBuilder.validate_config(_minimal_config(wsdl_metadata=meta))
    assert err is not None and err.error_code == "SOAP_WSDL_METADATA_INCOMPLETE"
    assert err.field == "wsdl_metadata.service_name"


def test_empty_wsdl_metadata_params_rejected():
    meta = _wsdl_metadata()
    meta["input_parameters"] = []
    err = SoapClientOperationBuilder.validate_config(_minimal_config(wsdl_metadata=meta))
    assert err is not None and err.error_code == "SOAP_WSDL_METADATA_INCOMPLETE"
    assert err.field == "wsdl_metadata.input_parameters"


def test_plaintext_secret_rejected():
    err = SoapClientOperationBuilder.validate_config(_minimal_config(password="x"))
    assert err is not None and err.error_code == "PLAINTEXT_SECRET_REJECTED"


def test_registry_maps_soap_execute():
    assert isinstance(
        get_connector_action_builder("soap_client", "execute"), SoapClientOperationBuilder
    )
    assert get_connector_action_builder("soap_client", "get") is None
