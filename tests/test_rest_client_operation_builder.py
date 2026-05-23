"""Tests for RestClientOperationBuilder (issue #24, M2.4).

Shapes locked against two verified Renera live exports:
- GET:   profile=reneraai-5RO3DD, component_id=e268ea19-bbbe-4e1f-b406-b5129358575a
- PATCH: profile=reneraai-5RO3DD, component_id=64c4eafd-f2e7-49e2-b128-c9b1c50f81b9

Builder emits Boomi REST Client operation components
(subType="officialboomi-X3979C-rest-prod"). Issue #24 ships only GET and
PATCH. POST/PUT/DELETE/HEAD/OPTIONS/TRACE return
UNVERIFIED_REST_XML_VARIANT until a live export proves their XML shape.
"""

import xml.etree.ElementTree as ET

import pytest

from boomi_mcp.categories.components.builders.connector_builder import (
    BuilderValidationError,
    RestClientOperationBuilder,
    REST_CLIENT_SUBTYPE,
    get_connector_action_builder,
)


NS = {"bns": "http://api.platform.boomi.com/"}

_FORBIDDEN_TOPLEVEL_SECRET_FIELDS = (
    "password",
    "password_ref",
    "secret",
    "token",
    "access_token",
    "client_secret",
)


def _minimal_get_config(**overrides):
    """Minimal-valid REST GET operation config."""
    params = {
        "component_type": "connector-action",
        "connector_type": "rest",
        "operation_mode": "execute",
        "component_name": "Read Target Record",
        "connection_ref_key": "target_rest_connection",
        "method": "GET",
        "path": "/v1/items/42",
    }
    params.update(overrides)
    return params


def _minimal_patch_config(**overrides):
    return _minimal_get_config(method="PATCH", **overrides)


def _build_get(**overrides):
    return RestClientOperationBuilder().build(**_minimal_get_config(**overrides))


def _build_patch(**overrides):
    return RestClientOperationBuilder().build(**_minimal_patch_config(**overrides))


def _find_generic_op_config(xml: str) -> ET.Element:
    root = ET.fromstring(xml)
    config = root.find("bns:object/Operation/Configuration/GenericOperationConfig", NS)
    assert config is not None
    return config


def _field_value(xml: str, field_id: str) -> str:
    config = _find_generic_op_config(xml)
    for field in config:
        if field.tag == "field" and field.attrib.get("id") == field_id:
            return field.attrib.get("value", "")
    raise AssertionError(f"field id={field_id!r} not found")


def _find_field(xml: str, field_id: str):
    config = _find_generic_op_config(xml)
    for field in config:
        if field.tag == "field" and field.attrib.get("id") == field_id:
            return field
    return None


# ----------------------------------------------------------------------------
# Aliases / dispatch
# ----------------------------------------------------------------------------

@pytest.mark.parametrize("alias", ["rest", "rest_client", REST_CLIENT_SUBTYPE])
def test_alias_resolves_to_rest_operation_builder(alias):
    builder = get_connector_action_builder(alias, "execute")
    assert isinstance(builder, RestClientOperationBuilder)


# ----------------------------------------------------------------------------
# Component shell
# ----------------------------------------------------------------------------

def test_component_shell_has_correct_type_and_subtype():
    xml = _build_get()
    root = ET.fromstring(xml)
    assert root.tag == "{http://api.platform.boomi.com/}Component"
    assert root.attrib["type"] == "connector-action"
    assert root.attrib["subType"] == REST_CLIENT_SUBTYPE
    assert root.attrib["name"] == "Read Target Record"
    assert root.attrib["folderName"] == "Home"


def test_operation_envelope_attributes():
    xml = _build_get()
    root = ET.fromstring(xml)
    op = root.find("bns:object/Operation", NS)
    assert op is not None
    assert op.attrib["returnApplicationErrors"] == "true"
    assert op.attrib["trackResponse"] == "true"


def test_operation_envelope_child_order():
    xml = _build_get()
    root = ET.fromstring(xml)
    op = root.find("bns:object/Operation", NS)
    tags = [child.tag for child in op]
    assert tags == ["Archiving", "Configuration", "Tracking", "Caching"]


def test_archiving_defaults():
    xml = _build_get()
    root = ET.fromstring(xml)
    archiving = root.find("bns:object/Operation/Archiving", NS)
    assert archiving.attrib["directory"] == ""
    assert archiving.attrib["enabled"] == "false"


# ----------------------------------------------------------------------------
# GET → HttpGetAction-style envelope (with followRedirects)
# ----------------------------------------------------------------------------

def test_get_emits_custom_operation_type_get():
    config = _find_generic_op_config(_build_get())
    assert config.attrib["customOperationType"] == "GET"
    assert config.attrib["operationType"] == "EXECUTE"
    assert config.attrib["requestProfileType"] == "xml"
    assert config.attrib["responseProfileType"] == "xml"


def test_get_field_order_matches_live_shape():
    """Verified against e268ea19 export ([Rest Test GET] Get Client).
    GET includes followRedirects BEFORE path."""
    xml = _build_get()
    config = _find_generic_op_config(xml)
    children = [
        (child.tag, child.attrib.get("id"))
        for child in config
    ]
    assert children == [
        ("field", "followRedirects"),
        ("field", "path"),
        ("field", "queryParameters"),
        ("field", "requestHeaders"),
        ("Options", None),
    ]


def test_get_follow_redirects_defaults_to_none():
    assert _field_value(_build_get(), "followRedirects") == "NONE"


def test_get_path_field_carries_value():
    assert _field_value(_build_get(path="/admin/cdscm/api/v1/clients/CLI001"), "path") == \
        "/admin/cdscm/api/v1/clients/CLI001"


def test_get_query_parameters_empty_customproperties_container():
    field = _find_field(_build_get(), "queryParameters")
    assert field is not None
    assert field.attrib["type"] == "customproperties"
    custom = field.find("customProperties")
    assert custom is not None
    assert list(custom) == []


def test_get_request_headers_empty_customproperties_container():
    field = _find_field(_build_get(), "requestHeaders")
    assert field is not None
    assert field.attrib["type"] == "customproperties"
    custom = field.find("customProperties")
    assert custom is not None
    assert list(custom) == []


def test_get_options_element_present_and_empty():
    config = _find_generic_op_config(_build_get())
    options = config.find("Options")
    assert options is not None
    assert list(options) == []


def test_get_tracking_and_caching_children():
    root = ET.fromstring(_build_get())
    tracking = root.find("bns:object/Operation/Tracking", NS)
    assert tracking is not None
    assert tracking.find("TrackedFields") is not None
    caching = root.find("bns:object/Operation/Caching", NS)
    assert caching is not None
    assert list(caching) == []


# ----------------------------------------------------------------------------
# PATCH → HttpSendAction-style envelope (NO followRedirects by default)
# ----------------------------------------------------------------------------

def test_patch_emits_custom_operation_type_patch():
    config = _find_generic_op_config(_build_patch())
    assert config.attrib["customOperationType"] == "PATCH"
    assert config.attrib["operationType"] == "EXECUTE"


def test_patch_field_order_omits_follow_redirects():
    """Verified against 64c4eafd export ([Rest Test PATCH] Update matter).
    PATCH does NOT emit a followRedirects field by default."""
    xml = _build_patch()
    config = _find_generic_op_config(xml)
    children = [
        (child.tag, child.attrib.get("id"))
        for child in config
    ]
    assert children == [
        ("field", "path"),
        ("field", "queryParameters"),
        ("field", "requestHeaders"),
        ("Options", None),
    ]


def test_patch_follow_redirects_emitted_when_explicitly_supplied():
    xml = _build_patch(follow_redirects="STRICT")
    field = _find_field(xml, "followRedirects")
    assert field is not None
    assert field.attrib["value"] == "STRICT"


# ----------------------------------------------------------------------------
# Path verbatim (REST Client does NOT strip leading slashes)
# ----------------------------------------------------------------------------

@pytest.mark.parametrize(
    "input_path",
    [
        "/v1/items",                       # leading slash
        "v1/items",                        # no leading slash
        "//v1/items",                      # double leading slash
        "admin/cdscm/api/v1/clients/CLI001",
        "v1/items/{id}",
    ],
)
def test_path_emitted_verbatim(input_path):
    """REST Client live exports preserve whatever path the caller supplies."""
    xml = _build_get(path=input_path)
    assert _field_value(xml, "path") == input_path


# ----------------------------------------------------------------------------
# Profile passthrough (UUID + $ref tokens)
# ----------------------------------------------------------------------------

def test_request_profile_id_uuid_emitted_as_attribute():
    xml = _build_patch(request_profile_id="abc-uuid-1234",
                       request_profile_type="json")
    config = _find_generic_op_config(xml)
    assert config.attrib["requestProfile"] == "abc-uuid-1234"
    assert config.attrib["requestProfileType"] == "json"


def test_response_profile_id_uuid_emitted_as_attribute():
    xml = _build_get(response_profile_id="xyz-uuid",
                     response_profile_type="json")
    config = _find_generic_op_config(xml)
    assert config.attrib["responseProfile"] == "xyz-uuid"
    assert config.attrib["responseProfileType"] == "json"


def test_request_profile_id_ref_token_preserved():
    xml = _build_patch(request_profile_id="$ref:target_json_profile")
    config = _find_generic_op_config(xml)
    assert config.attrib["requestProfile"] == "$ref:target_json_profile"


def test_response_profile_id_ref_token_preserved():
    xml = _build_get(response_profile_id="$ref:target_response_profile")
    config = _find_generic_op_config(xml)
    assert config.attrib["responseProfile"] == "$ref:target_response_profile"


def test_no_request_profile_attribute_when_not_supplied():
    """Live exports don't emit requestProfile when no profile is referenced."""
    config = _find_generic_op_config(_build_patch())
    assert "requestProfile" not in config.attrib


# ----------------------------------------------------------------------------
# Plan-only metadata never leaks into XML
# ----------------------------------------------------------------------------

def test_connection_ref_key_not_in_xml():
    xml = _build_patch(connection_ref_key="target_rest_connection")
    assert "target_rest_connection" not in xml


def test_payload_source_ref_key_not_in_xml():
    xml = _build_patch(payload_source_ref_key="payload_map")
    assert "payload_map" not in xml


def test_credential_ref_not_in_xml():
    xml = _build_patch(credential_ref="credential://target-api/headers")
    assert "credential://target-api/headers" not in xml


def test_raw_request_body_not_in_xml():
    """If a caller mistakenly includes a request body, it must not appear in
    the emitted operation XML (request bodies are upstream message/map step
    output, not connector-action XML)."""
    xml = _build_patch(request_body='{"foo":"bar-DEADBEEF"}')
    assert "DEADBEEF" not in xml
    assert "bar-DEADBEEF" not in xml


# ----------------------------------------------------------------------------
# Method gating
# ----------------------------------------------------------------------------

def test_method_get_buildable():
    _build_get()  # smoke


def test_method_patch_buildable():
    _build_patch()  # smoke


@pytest.mark.parametrize("verified_pending", ["POST", "PUT", "DELETE", "HEAD", "OPTIONS", "TRACE"])
def test_other_recognized_methods_return_unverified_variant(verified_pending):
    cfg = _minimal_get_config(method=verified_pending)
    with pytest.raises(BuilderValidationError) as excinfo:
        RestClientOperationBuilder().build(**cfg)
    err = excinfo.value
    assert err.error_code == "UNVERIFIED_REST_XML_VARIANT"
    assert err.field == "method"


@pytest.mark.parametrize("bad_method", ["FOO", "BAR", "X-CUSTOM"])
def test_unknown_methods_return_unsupported(bad_method):
    cfg = _minimal_get_config(method=bad_method)
    with pytest.raises(BuilderValidationError) as excinfo:
        RestClientOperationBuilder().build(**cfg)
    err = excinfo.value
    assert err.error_code == "UNSUPPORTED_REST_METHOD"
    assert err.field == "method"


def test_missing_method_rejected():
    cfg = _minimal_get_config()
    cfg.pop("method")
    with pytest.raises(BuilderValidationError) as excinfo:
        RestClientOperationBuilder().build(**cfg)
    assert excinfo.value.error_code == "UNSUPPORTED_REST_METHOD"


# ----------------------------------------------------------------------------
# Operation mode
# ----------------------------------------------------------------------------

@pytest.mark.parametrize("bad_mode", ["get", "send", "read", "create", "", None])
def test_operation_mode_must_be_execute(bad_mode):
    cfg = _minimal_get_config()
    if bad_mode is None:
        cfg.pop("operation_mode")
    else:
        cfg["operation_mode"] = bad_mode
    with pytest.raises(BuilderValidationError) as excinfo:
        RestClientOperationBuilder().build(**cfg)
    err = excinfo.value
    assert err.error_code == "UNSUPPORTED_REST_OPERATION_MODE"
    assert err.field == "operation_mode"


# ----------------------------------------------------------------------------
# Required fields
# ----------------------------------------------------------------------------

def test_missing_component_name_rejected():
    cfg = _minimal_get_config(component_name="")
    with pytest.raises(BuilderValidationError) as excinfo:
        RestClientOperationBuilder().build(**cfg)
    err = excinfo.value
    assert err.error_code == "REST_OPERATION_VALIDATION_FAILED"
    assert err.field == "component_name"


def test_missing_connection_ref_key_rejected():
    cfg = _minimal_get_config()
    cfg.pop("connection_ref_key")
    with pytest.raises(BuilderValidationError) as excinfo:
        RestClientOperationBuilder().build(**cfg)
    err = excinfo.value
    assert err.error_code == "REST_CONNECTION_REF_REQUIRED"
    assert err.field == "connection_ref_key"


def test_missing_path_rejected():
    cfg = _minimal_get_config()
    cfg.pop("path")
    with pytest.raises(BuilderValidationError) as excinfo:
        RestClientOperationBuilder().build(**cfg)
    err = excinfo.value
    assert err.error_code == "REST_PATH_REQUIRED"
    assert err.field == "path"


# ----------------------------------------------------------------------------
# Query parameters / request headers — empty only until verified
# ----------------------------------------------------------------------------

def test_empty_query_parameters_accepted():
    """Empty dict is the verified-empty shape: customproperties container
    with no children."""
    xml = _build_get(query_parameters={})
    field = _find_field(xml, "queryParameters")
    assert list(field.find("customProperties")) == []


def test_non_empty_query_parameters_rejected():
    cfg = _minimal_get_config(query_parameters={"q": "search"})
    with pytest.raises(BuilderValidationError) as excinfo:
        RestClientOperationBuilder().build(**cfg)
    err = excinfo.value
    assert err.error_code == "NEEDS_REST_EXAMPLE"
    assert err.field == "query_parameters"


def test_non_empty_request_headers_rejected():
    cfg = _minimal_get_config(request_headers={"Authorization": "Bearer x"})
    with pytest.raises(BuilderValidationError) as excinfo:
        RestClientOperationBuilder().build(**cfg)
    err = excinfo.value
    assert err.error_code == "NEEDS_REST_EXAMPLE"
    assert err.field == "request_headers"


def test_empty_request_headers_accepted():
    xml = _build_get(request_headers={})
    field = _find_field(xml, "requestHeaders")
    assert list(field.find("customProperties")) == []


def test_query_parameters_must_be_dict():
    with pytest.raises(BuilderValidationError) as excinfo:
        RestClientOperationBuilder().build(**_minimal_get_config(query_parameters="oops"))
    err = excinfo.value
    assert err.error_code == "REST_OPERATION_VALIDATION_FAILED"
    assert err.field == "query_parameters"


# ----------------------------------------------------------------------------
# Profile $ref empty token
# ----------------------------------------------------------------------------

def test_empty_ref_in_request_profile_id_rejected():
    with pytest.raises(BuilderValidationError) as excinfo:
        RestClientOperationBuilder().build(**_minimal_patch_config(request_profile_id="$ref:"))
    err = excinfo.value
    assert err.error_code == "REST_PROFILE_REF_UNRESOLVED"
    assert err.field == "request_profile_id"


def test_empty_ref_in_response_profile_id_rejected():
    with pytest.raises(BuilderValidationError) as excinfo:
        RestClientOperationBuilder().build(**_minimal_get_config(response_profile_id="$ref:"))
    err = excinfo.value
    assert err.error_code == "REST_PROFILE_REF_UNRESOLVED"
    assert err.field == "response_profile_id"


# ----------------------------------------------------------------------------
# Plaintext secret rejection
# ----------------------------------------------------------------------------

@pytest.mark.parametrize("forbidden", _FORBIDDEN_TOPLEVEL_SECRET_FIELDS)
def test_top_level_forbidden_secret_fields_rejected(forbidden):
    cfg = _minimal_get_config(**{forbidden: "DEADBEEF_REST_OP"})
    with pytest.raises(BuilderValidationError) as excinfo:
        RestClientOperationBuilder().build(**cfg)
    err = excinfo.value
    assert err.error_code == "PLAINTEXT_SECRET_REJECTED"
    assert err.field == forbidden
    assert "DEADBEEF_REST_OP" not in str(err)


def test_validate_config_returns_none_for_valid_get():
    assert RestClientOperationBuilder.validate_config(_minimal_get_config()) is None


def test_validate_config_returns_none_for_valid_patch():
    assert RestClientOperationBuilder.validate_config(_minimal_patch_config()) is None


# ----------------------------------------------------------------------------
# XML escaping
# ----------------------------------------------------------------------------

def test_special_characters_in_path_and_name_round_trip():
    xml = _build_get(
        component_name='REST "Prod" & <op>',
        path="/v1/items?q=a&b=<c>",
    )
    root = ET.fromstring(xml)
    assert root.attrib["name"] == 'REST "Prod" & <op>'
    assert _field_value(xml, "path") == "/v1/items?q=a&b=<c>"
