"""SOAP Client structured-update preservation tests (issue #126, M5.10).

The connection policy key-merges GenericConnectionConfig by field id (owned
fields replace; unknown fields/siblings preserved). The operation policy owns the
Operation envelope attrs + the GenericOperationConfig attrs/fields and fully
regenerates the cookie + Options subtrees while preserving unrelated siblings.
"""

import xml.etree.ElementTree as ET

from boomi_mcp.categories.components.builders.connector_builder import (
    SoapClientConnectionBuilder,
    SoapClientOperationBuilder,
    SOAP_CLIENT_SUBTYPE,
)
from boomi_mcp.categories.components.component_update_preservation import merge_for_update

from test_soap_client_connector_builder import _minimal_config as _conn_config
from test_soap_client_operation_builder import _minimal_config as _op_config

NS = {"bns": "http://api.platform.boomi.com/"}


def test_connection_policy_attached():
    policy = SoapClientConnectionBuilder.PRESERVATION_POLICY
    assert policy.component_type == "connector-settings"
    assert policy.subtype == SOAP_CLIENT_SUBTYPE
    assert any(op.path == "bns:object/GenericConnectionConfig" for op in policy.owned_paths)


def test_operation_policy_attached():
    policy = SoapClientOperationBuilder.PRESERVATION_POLICY
    assert policy.component_type == "connector-action"
    assert policy.subtype == SOAP_CLIENT_SUBTYPE


def test_connection_update_preserves_unknown_field_replaces_owned():
    desired = SoapClientConnectionBuilder().build(**_conn_config(username="newuser"))
    current = SoapClientConnectionBuilder().build(**_conn_config(username="olduser"))
    # Inject an unknown future field the builder never emits.
    current = current.replace(
        '<field id="wsssecurityOptions" type="wssecurity"><WSSecurityOptions/></field>',
        '<field id="wsssecurityOptions" type="wssecurity"><WSSecurityOptions/></field>'
        '<field id="futureSoapField" type="string" value="opaque"/>',
    )
    merged = merge_for_update(current, desired, SoapClientConnectionBuilder.PRESERVATION_POLICY)
    root = ET.fromstring(merged)
    gcc = root.find("bns:object/GenericConnectionConfig", NS)
    by_id = {f.attrib["id"]: f for f in gcc}
    # Owned field replaced from desired.
    assert by_id["username"].attrib["value"] == "newuser"
    # Unknown sibling preserved.
    assert "futureSoapField" in by_id
    assert by_id["futureSoapField"].attrib["value"] == "opaque"


def test_connection_update_preserves_live_encrypted_password():
    """A UI/extension-set encrypted password marker in the live component must
    survive a structured update (SOAP does not own the encrypted password path)."""
    desired = SoapClientConnectionBuilder().build(**_conn_config(username="newuser"))
    current = SoapClientConnectionBuilder().build(**_conn_config(username="olduser"))
    current = current.replace(
        "<bns:encryptedValues/>",
        '<bns:encryptedValues>'
        '<bns:encryptedValue path="//GenericConnectionConfig/field[@type=\'password\']" isSet="true"/>'
        '</bns:encryptedValues>',
    )
    merged = merge_for_update(current, desired, SoapClientConnectionBuilder.PRESERVATION_POLICY)
    root = ET.fromstring(merged)
    markers = root.findall("bns:encryptedValues/bns:encryptedValue", NS)
    assert any(m.attrib.get("isSet") == "true" for m in markers)


def test_operation_update_regenerates_cookie_preserves_unknown_sibling():
    desired = SoapClientOperationBuilder().build(**_op_config())
    current = SoapClientOperationBuilder().build(**_op_config(expose_response_envelope=True))
    # Inject an unknown sibling inside Operation that the builder never emits.
    current = current.replace(
        "<Caching/>",
        "<Caching/><FutureOpBlock retained=\"yes\"/>",
    )
    merged = merge_for_update(current, desired, SoapClientOperationBuilder.PRESERVATION_POLICY)
    root = ET.fromstring(merged)
    op = root.find("bns:object/Operation", NS)
    # Owned field regenerated from desired (expose_response back to false).
    goc = op.find("Configuration/GenericOperationConfig")
    by_id = {f.attrib["id"]: f for f in goc.findall("field")}
    assert by_id["exposeResponseEnvelope"].attrib["value"] == "false"
    # Unknown Operation sibling preserved.
    assert op.find("FutureOpBlock") is not None
