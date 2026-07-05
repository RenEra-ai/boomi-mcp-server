"""Tests for WssListenerOperationBuilder (M6, issue #12).

Operation XML locked against the live renera Process Library export
601cf5a3-b0be-4c63-9dda-7665384f89d1 ("Configure a Web Listener",
type="connector-action" subType="wss"), captured 2026-07-04
(.codex/plans/m6-listener-recon.md §1).
"""

import xml.etree.ElementTree as ET

import pytest

from boomi_mcp.categories.components.builders.connector_builder import (
    BuilderValidationError,
    WssListenerOperationBuilder,
    WSS_SUBTYPE,
    _resolve_wss_connector_type,
    get_connector_action_builder,
)

NS = {"bns": "http://api.platform.boomi.com/"}


def _build(**overrides):
    config = {
        "component_name": "Configure a Web Listener",
        "operation_mode": "listen",
        "object_name": "generalListener",
        "operation_type": "CREATE",
        "input_type": "singlejson",
        "output_type": "none",
        "response_content_type": "text/plain",
    }
    config.update(overrides)
    return WssListenerOperationBuilder().build(**config)


def _action_element(xml: str) -> ET.Element:
    root = ET.fromstring(xml)
    action = root.find(".//WebServicesServerListenAction")
    assert action is not None, "WebServicesServerListenAction missing"
    return action


# ---------------------------------------------------------------------------
# Live-shape emission
# ---------------------------------------------------------------------------


def test_operation_xml_matches_live_shape():
    """The emitted body matches the live capture: Operation envelope with
    Archiving / Configuration>WebServicesServerListenAction / Tracking / Caching."""
    xml = _build()
    root = ET.fromstring(xml)
    assert root.attrib["type"] == "connector-action"
    assert root.attrib["subType"] == "wss"
    operation = root.find("bns:object/Operation", NS)
    assert operation is not None
    assert [child.tag for child in operation] == [
        "Archiving",
        "Configuration",
        "Tracking",
        "Caching",
    ]
    archiving = operation.find("Archiving")
    assert archiving.attrib == {"directory": "", "enabled": "false"}
    action = operation.find("Configuration/WebServicesServerListenAction")
    assert action.attrib == {
        "inputType": "singlejson",
        "objectName": "generalListener",
        "operationType": "CREATE",
        "outputType": "none",
        "responseContentType": "text/plain",
    }
    tracking = operation.find("Tracking")
    assert [child.tag for child in tracking] == ["TrackedFields"]


def test_live_attribute_order_alphabetical():
    """Attribute ORDER matches the live capture (alphabetical): inputType,
    objectName, operationType, outputType, [requestProfile],
    responseContentType, [responseProfile]."""
    xml = _build(
        input_type="singlejson",
        request_profile="REQ-1",
        output_type="singlejson",
        response_profile="RESP-1",
        response_content_type="application/json",
    )
    action_src = xml.split("<WebServicesServerListenAction ", 1)[1].split("/>", 1)[0]
    names = [chunk.split("=", 1)[0] for chunk in action_src.split() if "=" in chunk]
    assert names == [
        "inputType",
        "objectName",
        "operationType",
        "outputType",
        "requestProfile",
        "responseContentType",
        "responseProfile",
    ]


def test_defaults_execute_singlejson_ack_only():
    """Omitted optionals default to EXECUTE / singlejson / none / text/plain."""
    xml = WssListenerOperationBuilder().build(
        component_name="Min Listener",
        operation_mode="listen",
        object_name="minListener",
    )
    action = _action_element(xml)
    assert action.attrib == {
        "inputType": "singlejson",
        "objectName": "minListener",
        "operationType": "EXECUTE",
        "outputType": "none",
        "responseContentType": "text/plain",
    }


def test_object_name_preserved_verbatim():
    """objectName is emitted verbatim — no case-folding (the /ws/simple casing
    question is settled live, never by the builder)."""
    action = _action_element(_build(object_name="CamelCaseName"))
    assert action.attrib["objectName"] == "CamelCaseName"


def test_operation_type_uppercased():
    action = _action_element(_build(operation_type="execute"))
    assert action.attrib["operationType"] == "EXECUTE"


def test_profiles_emitted_only_when_set():
    xml = _build()
    assert "requestProfile" not in xml
    assert "responseProfile" not in xml
    xml_with_req = _build(request_profile="REQ-1")
    action = _action_element(xml_with_req)
    assert action.attrib["requestProfile"] == "REQ-1"
    assert "responseProfile" not in xml_with_req


def test_json_input_without_request_profile_allowed():
    """The live Process Library listener serves singlejson with NO requestProfile
    (payload read via DDP) — a profile is optional even for JSON input."""
    action = _action_element(_build(input_type="singlejson"))
    assert "requestProfile" not in action.attrib


def test_no_connection_component_and_no_encrypted_values():
    xml = _build()
    root = ET.fromstring(xml)
    assert root.attrib["subType"] == "wss"
    assert "connectionId" not in xml
    encrypted = root.find("bns:encryptedValues", NS)
    assert encrypted is not None and len(encrypted) == 0


# ---------------------------------------------------------------------------
# Validation rejections
# ---------------------------------------------------------------------------


def test_non_listen_mode_rejected():
    with pytest.raises(BuilderValidationError) as exc:
        _build(operation_mode="execute")
    assert exc.value.error_code == "UNSUPPORTED_WSS_OPERATION_MODE"


def test_missing_component_name_rejected():
    with pytest.raises(BuilderValidationError) as exc:
        _build(component_name="")
    assert exc.value.error_code == "WSS_OPERATION_CONFIG_INVALID"
    assert exc.value.field == "component_name"


def test_missing_object_name_rejected():
    with pytest.raises(BuilderValidationError) as exc:
        _build(object_name="  ")
    assert exc.value.field == "object_name"


@pytest.mark.parametrize("bad_name", ["has space", "a/b", "x?y", "p#q", "pct%"])
def test_url_unsafe_object_name_rejected(bad_name):
    with pytest.raises(BuilderValidationError) as exc:
        _build(object_name=bad_name)
    assert exc.value.field == "object_name"


@pytest.mark.parametrize("verb", ["POST", "PUT", "PATCH", "post"])
def test_http_verb_operation_type_rejected_with_method_hint(verb):
    """POST/PUT/PATCH are HTTP verbs, not WSS operationTypes — the method derives
    from input_type and is never set on the operation."""
    with pytest.raises(BuilderValidationError) as exc:
        _build(operation_type=verb)
    assert exc.value.error_code == "WSS_OPERATION_CONFIG_INVALID"
    assert exc.value.field == "operation_type"
    assert "input_type" in (exc.value.hint or "")


def test_unknown_operation_type_rejected():
    with pytest.raises(BuilderValidationError) as exc:
        _build(operation_type="LISTEN")
    assert exc.value.field == "operation_type"


def test_unknown_input_type_rejected():
    with pytest.raises(BuilderValidationError) as exc:
        _build(input_type="json")
    assert exc.value.field == "input_type"


def test_unknown_output_type_rejected():
    with pytest.raises(BuilderValidationError) as exc:
        _build(output_type="ack")
    assert exc.value.field == "output_type"


def test_unknown_response_content_type_rejected():
    with pytest.raises(BuilderValidationError) as exc:
        _build(response_content_type="text/html")
    assert exc.value.field == "response_content_type"


def test_request_profile_on_non_json_xml_input_rejected():
    with pytest.raises(BuilderValidationError) as exc:
        _build(input_type="singledata", request_profile="REQ-1")
    assert exc.value.field == "request_profile"


def test_response_profile_on_none_output_rejected():
    with pytest.raises(BuilderValidationError) as exc:
        _build(output_type="none", response_profile="RESP-1")
    assert exc.value.field == "response_profile"


def test_connection_binding_rejected():
    """WSS has no connection component — a connection_ref_key signals a misrouted
    REST/SOAP-style config."""
    with pytest.raises(BuilderValidationError) as exc:
        _build(connection_ref_key="conn_key")
    assert exc.value.field == "connection_ref_key"


# ---------------------------------------------------------------------------
# Registry + resolver
# ---------------------------------------------------------------------------


def test_registry_maps_wss_listen_aliases():
    for alias in ("wss", "web_services", "web_services_server"):
        builder = get_connector_action_builder(alias, "listen")
        assert isinstance(builder, WssListenerOperationBuilder), alias
    # No non-listen mode is registered.
    assert get_connector_action_builder("wss", "execute") is None


def test_resolver_claims_only_listener_aliases():
    assert _resolve_wss_connector_type("wss") == WSS_SUBTYPE
    assert _resolve_wss_connector_type(" Web_Services ") == WSS_SUBTYPE
    assert _resolve_wss_connector_type("web_services_server") == WSS_SUBTYPE
    # The outbound-client and generic tokens must never route here.
    for token in ("soap", "soap_client", "web_services_soap_client", "rest", "http", "database", None, 7):
        assert _resolve_wss_connector_type(token) is None, token


def test_injected_framework_keys_accepted():
    """build_integration injects routing metadata (component_type/key/action/
    depends_on/name) into the payload; the builder must tolerate them."""
    xml = _build(
        component_type="connector-action",
        connector_type="wss",
        key="source_wss_listener_operation",
        action="create",
        depends_on=["listener_request_profile"],
        name="Configure a Web Listener",
        folder_name="Integrations/Listeners",
        description="demo",
    )
    root = ET.fromstring(xml)
    assert root.attrib["folderName"] == "Integrations/Listeners"


def test_preservation_policy_owns_listen_action_attrs():
    policy = WssListenerOperationBuilder.PRESERVATION_POLICY
    assert policy.component_type == "connector-action"
    assert policy.subtype == "wss"
    (owned,) = policy.owned_paths
    assert owned.path.endswith("WebServicesServerListenAction")
    assert "objectName" in owned.owned_attrs
    assert "requestProfile" in owned.owned_attrs
