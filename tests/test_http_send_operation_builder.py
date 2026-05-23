"""Tests for HttpClientOperationBuilder (issue #24).

Shape locked against two verified live exports:
- POST: profile=work, component_id=1428893f-0357-4311-b59a-e09847cc6552
- GET:  profile=reneraai-5RO3DD, component_id=03ec828a-8777-4f39-862c-b8d6a69da6e9

Key invariants:
- POST/PUT/PATCH/DELETE emit <HttpSendAction>.
- GET emits <HttpGetAction>; NO returnResponses attribute on GET.
- Header keys start at 1000000 and increment.
- Path element keys start at 2000000 and increment.
- Exactly one leading '/' stripped from path on emission.
- connection_ref_key, payload_source_ref_key, credential_ref, raw payload
  bodies are plan-only metadata and never appear in XML.
"""

import xml.etree.ElementTree as ET

import pytest

from boomi_mcp.categories.components.builders.connector_builder import (
    BuilderValidationError,
    HttpClientOperationBuilder,
)


NS = {"bns": "http://api.platform.boomi.com/"}

_FORBIDDEN_SECRET_FIELDS = (
    "password",
    "password_ref",
    "secret",
    "token",
    "access_token",
    "client_secret",
)


def _minimal_send_config(**overrides):
    """Minimal-valid POST send config (issue-#24 public_contract example)."""
    params = {
        "component_type": "connector-action",
        "connector_type": "http",
        "operation_mode": "send",
        "component_name": "Send Target Record",
        "connection_ref_key": "rest_connection",
        "method": "POST",
        "path": "/v1/items",
    }
    params.update(overrides)
    return params


def _minimal_get_config(**overrides):
    return _minimal_send_config(method="GET", **overrides)


def _build_send(**overrides):
    return HttpClientOperationBuilder().build(**_minimal_send_config(**overrides))


def _build_get(**overrides):
    return HttpClientOperationBuilder().build(**_minimal_get_config(**overrides))


def _find_action(xml: str, tag: str) -> ET.Element:
    root = ET.fromstring(xml)
    action = root.find(f"bns:object/Operation/Configuration/{tag}", NS)
    assert action is not None, f"<{tag}> not found in {xml!r}"
    return action


# ----------------------------------------------------------------------------
# Component shell
# ----------------------------------------------------------------------------

def test_component_shell_has_correct_type_and_subtype():
    xml = _build_send()
    root = ET.fromstring(xml)
    assert root.tag == "{http://api.platform.boomi.com/}Component"
    assert root.attrib["type"] == "connector-action"
    assert root.attrib["subType"] == "http"
    assert root.attrib["name"] == "Send Target Record"
    assert root.attrib["folderName"] == "Home"


def test_operation_envelope_child_order():
    xml = _build_send()
    root = ET.fromstring(xml)
    op = root.find("bns:object/Operation", NS)
    assert op is not None
    tags = [child.tag for child in op]
    assert tags == ["Archiving", "Configuration", "Tracking", "Caching"]


def test_archiving_defaults_match_live_shape():
    xml = _build_send()
    root = ET.fromstring(xml)
    archiving = root.find("bns:object/Operation/Archiving", NS)
    assert archiving is not None
    assert archiving.attrib["directory"] == ""
    assert archiving.attrib["enabled"] == "false"


def test_caching_is_empty_self_closing():
    xml = _build_send()
    root = ET.fromstring(xml)
    caching = root.find("bns:object/Operation/Caching", NS)
    assert caching is not None
    assert list(caching) == []


# ----------------------------------------------------------------------------
# POST → HttpSendAction
# ----------------------------------------------------------------------------

@pytest.mark.parametrize("method", ["POST", "PUT", "PATCH", "DELETE"])
def test_send_methods_emit_HttpSendAction(method):
    xml = _build_send(method=method)
    action = _find_action(xml, "HttpSendAction")
    assert action.attrib["methodType"] == method


def test_HttpSendAction_default_attributes():
    action = _find_action(_build_send(), "HttpSendAction")
    assert action.attrib["dataContentType"] == "application/json"
    assert action.attrib["followRedirects"] == "false"
    assert action.attrib["mimePassthrough"] == "false"
    assert action.attrib["requestProfileType"] == "NONE"
    assert action.attrib["responseProfileType"] == "NONE"
    assert action.attrib["returnErrors"] == "true"
    assert action.attrib["returnMimeResponse"] == "false"
    assert action.attrib["returnResponses"] == "true"


def test_HttpSendAction_custom_content_type():
    action = _find_action(
        _build_send(content_type="multipart/form-data; boundary=ABC"),
        "HttpSendAction",
    )
    assert action.attrib["dataContentType"] == "multipart/form-data; boundary=ABC"


def test_HttpSendAction_children_order():
    action = _find_action(_build_send(), "HttpSendAction")
    tags = [child.tag for child in action]
    assert tags == ["requestHeaders", "pathElements", "responseHeaderMapping", "reflectHeaders"]


def test_HttpSendAction_path_element():
    action = _find_action(_build_send(path="/v3/mail/send"), "HttpSendAction")
    elements = action.findall("pathElements/element")
    assert len(elements) == 1
    assert elements[0].attrib["key"] == "2000000"
    assert elements[0].attrib["name"] == "v3/mail/send"


# ----------------------------------------------------------------------------
# GET → HttpGetAction
# ----------------------------------------------------------------------------

def test_get_method_emits_HttpGetAction():
    xml = _build_get()
    action = _find_action(xml, "HttpGetAction")
    assert action.attrib["methodType"] == "GET"


def test_HttpGetAction_default_attributes():
    action = _find_action(_build_get(), "HttpGetAction")
    assert action.attrib["dataContentType"] == "application/json"
    assert action.attrib["followRedirects"] == "false"
    assert action.attrib["mimePassthrough"] == "false"
    assert action.attrib["requestProfileType"] == "NONE"
    assert action.attrib["responseProfileType"] == "NONE"
    assert action.attrib["returnErrors"] == "false"
    assert action.attrib["returnMimeResponse"] == "false"


def test_HttpGetAction_does_not_emit_returnResponses():
    """Verified against live Renera GET export (component 03ec828a) — the
    returnResponses attribute is present on Send but absent on Get."""
    action = _find_action(_build_get(), "HttpGetAction")
    assert "returnResponses" not in action.attrib


def test_HttpGetAction_request_headers_can_be_empty():
    action = _find_action(_build_get(), "HttpGetAction")
    headers_elem = action.find("requestHeaders")
    assert headers_elem is not None
    assert list(headers_elem) == []


# ----------------------------------------------------------------------------
# Path stripping
# ----------------------------------------------------------------------------

@pytest.mark.parametrize(
    "input_path,expected_name",
    [
        ("/v1/items", "v1/items"),       # one leading slash stripped
        ("v1/items", "v1/items"),        # no slash, unchanged
        ("//v1/items", "/v1/items"),     # only one slash stripped
        ("/", ""),                        # bare slash becomes empty
        ("/a/b/c", "a/b/c"),             # multi-segment with leading slash
    ],
)
def test_path_strips_exactly_one_leading_slash(input_path, expected_name):
    action = _find_action(_build_send(path=input_path), "HttpSendAction")
    element = action.find("pathElements/element")
    assert element.attrib["name"] == expected_name


# ----------------------------------------------------------------------------
# Headers
# ----------------------------------------------------------------------------

def test_header_keys_start_at_1000000_and_increment():
    xml = _build_send(headers=[
        {"name": "A", "value": "1"},
        {"name": "B", "value": "2"},
        {"name": "C", "value": "3"},
    ])
    action = _find_action(xml, "HttpSendAction")
    headers = action.findall("requestHeaders/header")
    assert [h.attrib["key"] for h in headers] == ["1000000", "1000001", "1000002"]


def test_fixed_value_header_does_not_emit_isVariable():
    xml = _build_send(headers=[{"name": "Accept", "value": "application/json"}])
    action = _find_action(xml, "HttpSendAction")
    header = action.find("requestHeaders/header")
    assert header.attrib["headerName"] == "Accept"
    assert header.attrib["headerValue"] == "application/json"
    assert "isVariable" not in header.attrib


def test_variable_header_emits_isVariable_true_and_empty_value():
    xml = _build_send(headers=[{"name": "Authorization", "is_variable": True}])
    action = _find_action(xml, "HttpSendAction")
    header = action.find("requestHeaders/header")
    assert header.attrib["headerName"] == "Authorization"
    assert header.attrib["headerValue"] == ""
    assert header.attrib["isVariable"] == "true"


def test_mixed_headers_match_live_sendgrid_shape():
    """Locks the SendGrid POST shape: Authorization variable + Accept + Expect."""
    xml = _build_send(headers=[
        {"name": "Authorization", "is_variable": True},
        {"name": "Accept", "value": "*/*"},
        {"name": "Expect", "value": "100-continue"},
    ])
    action = _find_action(xml, "HttpSendAction")
    headers = action.findall("requestHeaders/header")
    assert len(headers) == 3
    assert headers[0].attrib == {
        "headerName": "Authorization",
        "headerValue": "",
        "isVariable": "true",
        "key": "1000000",
    }
    assert headers[1].attrib == {
        "headerName": "Accept",
        "headerValue": "*/*",
        "key": "1000001",
    }
    assert headers[2].attrib == {
        "headerName": "Expect",
        "headerValue": "100-continue",
        "key": "1000002",
    }


# ----------------------------------------------------------------------------
# Request profile passthrough
# ----------------------------------------------------------------------------

def test_request_profile_id_uuid_emitted_verbatim():
    xml = _build_send(
        request_profile_id="abc-uuid-1234",
        request_profile_type="JSON",
    )
    action = _find_action(xml, "HttpSendAction")
    assert action.attrib["requestProfile"] == "abc-uuid-1234"
    assert action.attrib["requestProfileType"] == "JSON"


def test_request_profile_id_ref_token_preserved():
    """integration_builder._resolve_dependency_tokens resolves $ref at apply
    time; the builder must NOT touch it."""
    xml = _build_send(
        request_profile_id="$ref:target_json_profile",
        request_profile_type="JSON",
    )
    action = _find_action(xml, "HttpSendAction")
    assert action.attrib["requestProfile"] == "$ref:target_json_profile"


# ----------------------------------------------------------------------------
# Plan-only metadata never appears in XML
# ----------------------------------------------------------------------------

def test_connection_ref_key_not_in_xml():
    xml = _build_send(connection_ref_key="rest_connection")
    assert "rest_connection" not in xml
    assert "connection_ref_key" not in xml


def test_payload_source_ref_key_not_in_xml():
    xml = _build_send(payload_source_ref_key="payload_map")
    assert "payload_map" not in xml
    assert "payload_source_ref_key" not in xml


def test_credential_ref_not_in_xml():
    xml = _build_send(credential_ref="credential://target-api/bearer")
    assert "credential://target-api/bearer" not in xml
    assert "credential_ref" not in xml


# ----------------------------------------------------------------------------
# Validation
# ----------------------------------------------------------------------------

def test_validate_config_returns_none_for_valid_post():
    assert HttpClientOperationBuilder.validate_config(_minimal_send_config()) is None


def test_validate_config_returns_none_for_valid_get():
    assert HttpClientOperationBuilder.validate_config(_minimal_get_config()) is None


@pytest.mark.parametrize("bad_mode", ["get", "GET", "read", "", None])
def test_unsupported_operation_mode_rejected(bad_mode):
    cfg = _minimal_send_config()
    if bad_mode is None:
        cfg.pop("operation_mode")
    else:
        cfg["operation_mode"] = bad_mode
    with pytest.raises(BuilderValidationError) as excinfo:
        HttpClientOperationBuilder().build(**cfg)
    err = excinfo.value
    assert err.error_code == "UNSUPPORTED_HTTP_OPERATION_MODE"
    assert err.field == "operation_mode"


@pytest.mark.parametrize("bad_method", ["HEAD", "OPTIONS", "TRACE", "", None, "foo"])
def test_unsupported_method_rejected(bad_method):
    cfg = _minimal_send_config()
    if bad_method is None:
        cfg.pop("method")
    else:
        cfg["method"] = bad_method
    with pytest.raises(BuilderValidationError) as excinfo:
        HttpClientOperationBuilder().build(**cfg)
    err = excinfo.value
    assert err.error_code == "UNSUPPORTED_HTTP_METHOD"
    assert err.field == "method"


def test_missing_component_name_returns_structured_error():
    with pytest.raises(BuilderValidationError) as excinfo:
        HttpClientOperationBuilder().build(**_minimal_send_config(component_name=""))
    err = excinfo.value
    assert err.error_code == "HTTP_OPERATION_VALIDATION_FAILED"
    assert err.field == "component_name"


def test_missing_connection_ref_key_returns_structured_error():
    cfg = _minimal_send_config()
    cfg.pop("connection_ref_key")
    with pytest.raises(BuilderValidationError) as excinfo:
        HttpClientOperationBuilder().build(**cfg)
    err = excinfo.value
    assert err.error_code == "MISSING_HTTP_DEPENDENCY"
    assert err.field == "connection_ref_key"


def test_missing_path_returns_structured_error():
    cfg = _minimal_send_config()
    cfg.pop("path")
    with pytest.raises(BuilderValidationError) as excinfo:
        HttpClientOperationBuilder().build(**cfg)
    err = excinfo.value
    assert err.error_code == "HTTP_OPERATION_VALIDATION_FAILED"
    assert err.field == "path"


def test_empty_ref_token_in_request_profile_id_rejected():
    """A bare '$ref:' token is meaningless and would slip past validation."""
    with pytest.raises(BuilderValidationError) as excinfo:
        HttpClientOperationBuilder().build(
            **_minimal_send_config(request_profile_id="$ref:"),
        )
    err = excinfo.value
    assert err.error_code == "MISSING_HTTP_REQUEST_PROFILE_REF"
    assert err.field == "request_profile_id"


@pytest.mark.parametrize("forbidden", _FORBIDDEN_SECRET_FIELDS)
def test_forbidden_secret_fields_rejected(forbidden):
    cfg = _minimal_send_config(**{forbidden: "LEAKED_OP_DEADBEEF"})
    with pytest.raises(BuilderValidationError) as excinfo:
        HttpClientOperationBuilder().build(**cfg)
    err = excinfo.value
    assert err.error_code == "PLAINTEXT_SECRET_REJECTED"
    assert err.field == forbidden
    assert "LEAKED_OP_DEADBEEF" not in str(err)
    assert "LEAKED_OP_DEADBEEF" not in (err.hint or "")


def test_headers_must_be_list():
    with pytest.raises(BuilderValidationError) as excinfo:
        HttpClientOperationBuilder().build(**_minimal_send_config(headers="not-a-list"))
    err = excinfo.value
    assert err.error_code == "HTTP_OPERATION_VALIDATION_FAILED"
    assert err.field == "headers"


def test_header_entries_must_be_dicts():
    with pytest.raises(BuilderValidationError) as excinfo:
        HttpClientOperationBuilder().build(
            **_minimal_send_config(headers=["not-a-dict"]),
        )
    err = excinfo.value
    assert err.error_code == "HTTP_OPERATION_VALIDATION_FAILED"


def test_header_missing_name_rejected():
    with pytest.raises(BuilderValidationError) as excinfo:
        HttpClientOperationBuilder().build(
            **_minimal_send_config(headers=[{"value": "no-name-here"}]),
        )
    err = excinfo.value
    assert err.error_code == "HTTP_OPERATION_VALIDATION_FAILED"
    assert err.field is not None
    assert "headers" in err.field


# ----------------------------------------------------------------------------
# XML escaping round-trip
# ----------------------------------------------------------------------------

def test_special_characters_round_trip_through_attributes():
    xml = _build_send(
        component_name='Send "Prod" & <Dev>',
        path='/path/with space?q=a&b=<c>',
        headers=[{"name": 'X-A&B', "value": 'val "with" <stuff>'}],
        description="Acme & Co <legacy>",
    )
    root = ET.fromstring(xml)
    assert root.attrib["name"] == 'Send "Prod" & <Dev>'
    desc = root.find("bns:description", NS)
    assert desc.text == "Acme & Co <legacy>"
    action = _find_action(xml, "HttpSendAction")
    header = action.find("requestHeaders/header")
    assert header.attrib["headerName"] == 'X-A&B'
    assert header.attrib["headerValue"] == 'val "with" <stuff>'
    element = action.find("pathElements/element")
    assert element.attrib["name"] == "path/with space?q=a&b=<c>"
