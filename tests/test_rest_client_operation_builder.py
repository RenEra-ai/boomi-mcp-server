"""Tests for RestClientOperationBuilder (issue #24, M2.4).

Shapes locked against two verified Renera live exports:
- GET:   profile=legacy-ref-acct (decommissioned), component_id=e268ea19-bbbe-4e1f-b406-b5129358575a
- PATCH: profile=legacy-ref-acct (decommissioned), component_id=64c4eafd-f2e7-49e2-b128-c9b1c50f81b9

Builder emits Boomi REST Client operation components
(subType="officialboomi-X3979C-rest-prod"). Issue #24 ships only GET and
PATCH. POST/PUT/DELETE/HEAD/OPTIONS/TRACE return
UNVERIFIED_REST_XML_VARIANT until a live export proves their XML shape.
"""

import xml.etree.ElementTree as ET
from unittest.mock import MagicMock

import pytest

from boomi_mcp.categories.components.builders.connector_builder import (
    BuilderValidationError,
    RestClientOperationBuilder,
    REST_CLIENT_SUBTYPE,
    get_connector_action_builder,
)
from boomi_mcp.categories.components.connectors import create_connector


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


@pytest.mark.parametrize("method", ["POST", "PUT", "DELETE", "HEAD", "OPTIONS", "TRACE"])
def test_phase5_methods_buildable(method):
    """Phase 5 of issue #24 follow-up: PUT/POST/DELETE/HEAD/OPTIONS/TRACE
    are now buildable alongside GET/PATCH. Verified against live exports:
      - 7524cfae (REST Create Matter POST)
      - 868e3b5d (REST Update matter PUT)
      - 3d843e38 (REST Delete Matter DELETE)
      - f7d08bdb (REST Check matter HEAD)
      - 0c1e7528 (REST Matter OPTIONS)
      - 63f63c32 (REST Matter TRACE)
    """
    cfg = _minimal_get_config(method=method)
    xml = RestClientOperationBuilder().build(**cfg)
    config = _find_generic_op_config(xml)
    assert config.attrib["customOperationType"] == method
    assert config.attrib["operationType"] == "EXECUTE"


def test_verified_pending_methods_emptied():
    """Phase 5: every previously-pending method is now buildable. Confirm
    the registry is empty (canary against accidentally re-introducing
    pending-method gating)."""
    assert RestClientOperationBuilder.VERIFIED_PENDING_METHODS == ()


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
# customProperties — plain entries (Phase 6 of issue #24 follow-up).
# Live shape verified against:
#   - 9ede2c08 (REST Query Param GET) — queryParameters with plain + encrypted
#     entries (we accept plain; reject encrypted)
#   - 4986d5eb (REST Headers GET) — requestHeaders same shape
# ----------------------------------------------------------------------------


def _properties_children(xml: str, field_id: str):
    """Return the list of <properties .../> children for queryParameters or
    requestHeaders, preserving insertion order."""
    f = _find_field(xml, field_id)
    assert f is not None
    cp = f.find("customProperties")
    assert cp is not None
    return list(cp)


def test_phase6_query_parameters_plain_entry_emitted_as_properties_element():
    """Single plain query_parameters entry emits the verified properties
    shape: `<properties key="..." value="..."/>`."""
    xml = _build_get(query_parameters={"limit": "100"})
    props = _properties_children(xml, "queryParameters")
    assert len(props) == 1
    assert props[0].tag == "properties"
    assert props[0].attrib["key"] == "limit"
    assert props[0].attrib["value"] == "100"
    # Plain entries must NOT carry the encrypted marker.
    assert "encrypted" not in props[0].attrib


def test_phase6_request_headers_plain_entry_emitted_as_properties_element():
    xml = _build_get(request_headers={"Accept": "application/json"})
    props = _properties_children(xml, "requestHeaders")
    assert len(props) == 1
    assert props[0].attrib["key"] == "Accept"
    assert props[0].attrib["value"] == "application/json"
    assert "encrypted" not in props[0].attrib


def test_phase6_multiple_plain_entries_preserve_insertion_order():
    """Python dict preserves insertion order (3.7+); builder must too."""
    xml = _build_get(query_parameters={
        "limit": "100",
        "offset": "0",
        "filter": "active=true",
    })
    props = _properties_children(xml, "queryParameters")
    keys = [p.attrib["key"] for p in props]
    assert keys == ["limit", "offset", "filter"]


def test_phase6_xml_value_special_chars_in_properties_are_escaped():
    """Verify XML escaping for both key and value of properties."""
    xml = _build_get(query_parameters={
        "search": "a & b <c>",
        "url": 'https://x?q="quoted"',
    })
    props = _properties_children(xml, "queryParameters")
    assert props[0].attrib["value"] == "a & b <c>"
    assert props[1].attrib["value"] == 'https://x?q="quoted"'


def test_phase6_empty_dict_still_emits_empty_customproperties():
    """Sanity: the empty-dict shape (the only path supported pre-Phase-6)
    still works — `<customProperties/>` with no children."""
    xml = _build_get(query_parameters={})
    props = _properties_children(xml, "queryParameters")
    assert props == []


# ----------------------------------------------------------------------------
# customProperties — secret rejection (Phase 6).
# Plain values are accepted; encrypted markers, secret-shaped keys, and
# secret-shaped values are rejected with structured errors.
# ----------------------------------------------------------------------------


@pytest.mark.parametrize(
    "secret_key",
    [
        "Authorization",
        "authorization",
        "X-API-Key",
        "x-api-key",
        "X-Auth-Token",
        "Bearer",
        "api-key",
        "API_KEY",
        "Token",
        "Password",
        "Secret",
        "Client-Secret",
        "Credential",
    ],
)
def test_phase6_secret_shaped_keys_rejected_in_query_parameters(secret_key):
    cfg = _minimal_get_config(query_parameters={secret_key: "anyvalue"})
    with pytest.raises(BuilderValidationError) as excinfo:
        RestClientOperationBuilder().build(**cfg)
    err = excinfo.value
    assert err.error_code == "REST_SECRET_VALUE_FORBIDDEN"
    assert err.field == "query_parameters"


@pytest.mark.parametrize(
    "secret_key",
    ["Authorization", "X-API-Key", "Bearer", "Token", "Password"],
)
def test_phase6_secret_shaped_keys_rejected_in_request_headers(secret_key):
    cfg = _minimal_get_config(request_headers={secret_key: "anyvalue"})
    with pytest.raises(BuilderValidationError) as excinfo:
        RestClientOperationBuilder().build(**cfg)
    err = excinfo.value
    assert err.error_code == "REST_SECRET_VALUE_FORBIDDEN"
    assert err.field == "request_headers"


@pytest.mark.parametrize(
    "secret_value",
    [
        # JWT shape (eyJ prefix is JSON base64).
        "eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiIxIn0.signaturepart",
        # Long base64-shaped ciphertext (Boomi-style encrypted-marker value).
        "TnaM0aAr9aWr/r7gzxyB8Babcdefghijklmnopqrstuvwxyz0123456789",
        # Explicit Boomi encrypted-value marker prefix.
        "[encrypted]somerandomciphertext",
    ],
)
def test_phase6_secret_shaped_values_rejected(secret_value):
    cfg = _minimal_get_config(query_parameters={"Filter": secret_value})
    with pytest.raises(BuilderValidationError) as excinfo:
        RestClientOperationBuilder().build(**cfg)
    err = excinfo.value
    assert err.error_code == "REST_SECRET_VALUE_FORBIDDEN"
    assert err.field == "query_parameters"


def test_phase6_encrypted_marker_dict_rejected():
    """If caller forwards a Boomi-style `{"encrypted": True, "key": ...,
    "value": ...}` dict shape (e.g. copied from a live encrypted-entry
    export), reject it. We do not support encrypted custom properties
    until a secret-safe encryption/write path exists."""
    cfg = _minimal_get_config(query_parameters={
        "encrypted": True,
        "key": "secret",
        "value": "abc",
    })
    with pytest.raises(BuilderValidationError) as excinfo:
        RestClientOperationBuilder().build(**cfg)
    err = excinfo.value
    assert err.error_code == "UNSUPPORTED_REST_ENCRYPTED_CUSTOM_PROPERTY"
    assert err.field == "query_parameters"


def test_phase6_non_string_property_key_rejected():
    cfg = _minimal_get_config(query_parameters={42: "value"})
    with pytest.raises(BuilderValidationError) as excinfo:
        RestClientOperationBuilder().build(**cfg)
    err = excinfo.value
    assert err.error_code == "REST_CUSTOM_PROPERTY_INVALID"
    assert err.field == "query_parameters"


def test_phase6_non_string_property_value_rejected():
    cfg = _minimal_get_config(query_parameters={"key": 12345})
    with pytest.raises(BuilderValidationError) as excinfo:
        RestClientOperationBuilder().build(**cfg)
    err = excinfo.value
    assert err.error_code == "REST_CUSTOM_PROPERTY_INVALID"
    assert err.field == "query_parameters"


def test_phase6_safe_headers_accepted():
    """Sanity: common safe headers must NOT trip the secret-detection
    heuristic — Accept, Content-Type, User-Agent, Cache-Control, etc."""
    xml = _build_get(request_headers={
        "Accept": "application/json",
        "Content-Type": "application/json",
        "User-Agent": "boomi-test/1.0",
        "Cache-Control": "no-cache",
        "Accept-Language": "en-US",
        "X-Request-Id": "abc-123-def",
    })
    props = _properties_children(xml, "requestHeaders")
    assert len(props) == 6
    keys = [p.attrib["key"] for p in props]
    assert "Accept" in keys
    assert "Content-Type" in keys


def test_phase6_safe_query_parameters_accepted():
    xml = _build_get(query_parameters={
        "limit": "100",
        "offset": "0",
        "filter": "active=true",
        "sort": "-created_at",
        "include": "metadata",
    })
    props = _properties_children(xml, "queryParameters")
    assert len(props) == 5


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

# ----------------------------------------------------------------------------
# Enum validation + case normalization (codex round-2 P2 #C)
# ----------------------------------------------------------------------------

@pytest.mark.parametrize("bogus", ["BOGUS", "always", "follow", "true", "1"])
def test_follow_redirects_must_be_in_supported_enum(bogus):
    cfg = _minimal_get_config(follow_redirects=bogus)
    with pytest.raises(BuilderValidationError) as excinfo:
        RestClientOperationBuilder().build(**cfg)
    err = excinfo.value
    assert err.error_code == "REST_OPERATION_VALIDATION_FAILED"
    assert err.field == "follow_redirects"


@pytest.mark.parametrize("value", ["NONE", "STRICT", "LAX"])
def test_follow_redirects_accepts_documented_values(value):
    xml = _build_get(follow_redirects=value)
    field = _find_field(xml, "followRedirects")
    assert field is not None
    assert field.attrib["value"] == value


@pytest.mark.parametrize(
    "input_value,expected_xml",
    [
        ("JSON", "json"),
        ("json", "json"),
        ("XML", "xml"),
        ("xml", "xml"),
        ("NONE", "none"),
        ("none", "none"),
    ],
)
def test_request_profile_type_is_normalized_to_lowercase(input_value, expected_xml):
    """Live exports use lowercase ('xml'); the schema template accepts both
    casings as input and normalizes on the way to XML."""
    xml = _build_get(request_profile_type=input_value)
    config = _find_generic_op_config(xml)
    assert config.attrib["requestProfileType"] == expected_xml


@pytest.mark.parametrize(
    "input_value,expected_xml",
    [
        ("JSON", "json"),
        ("xml", "xml"),
        ("None", "none"),
    ],
)
def test_response_profile_type_is_normalized_to_lowercase(input_value, expected_xml):
    xml = _build_get(response_profile_type=input_value)
    config = _find_generic_op_config(xml)
    assert config.attrib["responseProfileType"] == expected_xml


@pytest.mark.parametrize("bogus", ["text", "yaml", "csv", "JSON5", ""])
def test_request_profile_type_rejects_unsupported_values(bogus):
    cfg = _minimal_get_config(request_profile_type=bogus)
    with pytest.raises(BuilderValidationError) as excinfo:
        RestClientOperationBuilder().build(**cfg)
    err = excinfo.value
    assert err.error_code == "REST_OPERATION_VALIDATION_FAILED"
    assert err.field == "request_profile_type"


@pytest.mark.parametrize("bogus", ["text", "yaml"])
def test_response_profile_type_rejects_unsupported_values(bogus):
    cfg = _minimal_get_config(response_profile_type=bogus)
    with pytest.raises(BuilderValidationError) as excinfo:
        RestClientOperationBuilder().build(**cfg)
    err = excinfo.value
    assert err.error_code == "REST_OPERATION_VALIDATION_FAILED"
    assert err.field == "response_profile_type"


def test_special_characters_in_path_and_name_round_trip():
    xml = _build_get(
        component_name='REST "Prod" & <op>',
        path="/v1/items?q=a&b=<c>",
    )
    root = ET.fromstring(xml)
    assert root.attrib["name"] == 'REST "Prod" & <op>'
    assert _field_value(xml, "path") == "/v1/items?q=a&b=<c>"


# ----------------------------------------------------------------------------
# Dispatcher fallback in connectors.py — wrong operation_mode on REST
# connector_type should surface UNSUPPORTED_REST_OPERATION_MODE instead of
# a generic "no builder available" message.
# ----------------------------------------------------------------------------

def _make_mock_client():
    client = MagicMock()
    client.connector.get_connector.return_value = MagicMock()
    return client


def test_dispatcher_surfaces_unsupported_rest_operation_mode_for_wrong_mode():
    """Caller passes connector_type='rest' with operation_mode='get' (typo
    for 'execute'). The dispatcher must fall through to
    RestClientOperationBuilder.validate_config and return the structured
    UNSUPPORTED_REST_OPERATION_MODE envelope."""
    client = _make_mock_client()
    result = create_connector(client, "test", {
        "component_type": "connector-action",
        "connector_type": "rest",
        "operation_mode": "get",
        "component_name": "Bad Op",
        "connection_ref_key": "x",
        "method": "GET",
        "path": "/x",
    })
    assert result["_success"] is False
    assert result["error_code"] == "UNSUPPORTED_REST_OPERATION_MODE"
    assert result["field"] == "operation_mode"


def test_dispatcher_uses_rest_builder_when_operation_mode_is_execute():
    """Sanity: the canonical (rest, execute) pair routes through
    RestClientOperationBuilder.build (no fallback path)."""
    client = _make_mock_client()
    # Make _create_component_raw return a stub success — we only care that
    # the dispatcher reached the builder and produced XML.
    client.component.create_component_raw.return_value = MagicMock(
        component_id="op-id-001", name="Dispatch Op",
        type_="connector-action", sub_type="officialboomi-X3979C-rest-prod",
    )
    # Bypass _create_component_raw by patching at module level since it's
    # imported into connectors.py — keep this test focused on validation
    # passing through, not on the Boomi API call.
    from unittest.mock import patch
    with patch(
        "boomi_mcp.categories.components.connectors._create_component_raw",
        return_value={
            "component_id": "op-id-001",
            "name": "Dispatch Op",
            "type": "connector-action",
            "sub_type": "officialboomi-X3979C-rest-prod",
        },
    ):
        result = create_connector(client, "test", {
            "component_type": "connector-action",
            "connector_type": "rest",
            "operation_mode": "execute",
            "component_name": "Dispatch Op",
            "connection_ref_key": "x",
            "method": "GET",
            "path": "/v1/x",
        })
    assert result["_success"] is True
    assert result["component_id"] == "op-id-001"


# ----------------------------------------------------------------------------
# followRedirects emission rule (Phase 5 — issue #24 follow-up).
# Verified per-method against live exports:
#   - GET (e268ea19): emits value="NONE" by default
#   - POST (7524cfae): emits value="NONE" by default
#   - HEAD (f7d08bdb): emits value="NONE" by default
#   - DELETE (3d843e38): emits value="NONE" by default
#   - PATCH (64c4eafd): field absent entirely by default
#   - PUT (868e3b5d): field absent entirely by default
#   - OPTIONS (0c1e7528): field absent entirely by default
#   - TRACE (63f63c32): field absent entirely by default
# Explicit values (STRICT/LAX/NONE) always emit regardless of method.
# ----------------------------------------------------------------------------


_FOLLOW_REDIRECTS_DEFAULT_NONE_METHODS = ("GET", "POST", "HEAD", "DELETE")
_FOLLOW_REDIRECTS_OMIT_METHODS = ("PATCH", "PUT", "OPTIONS", "TRACE")


@pytest.mark.parametrize("method", _FOLLOW_REDIRECTS_DEFAULT_NONE_METHODS)
def test_follow_redirects_default_none_methods(method):
    """GET/POST/HEAD/DELETE emit `<field id="followRedirects" type="string"
    value="NONE"/>` when caller omits follow_redirects (matches live shape)."""
    cfg = _minimal_get_config(method=method)
    xml = RestClientOperationBuilder().build(**cfg)
    fr = _find_field(xml, "followRedirects")
    assert fr is not None, (
        f"{method} must emit a followRedirects field by default (value=NONE)"
    )
    assert fr.attrib.get("value") == "NONE"


@pytest.mark.parametrize("method", _FOLLOW_REDIRECTS_OMIT_METHODS)
def test_follow_redirects_omitted_methods(method):
    """PATCH/PUT/OPTIONS/TRACE OMIT the followRedirects field when caller
    doesn't supply it (matches live shape — Boomi treats these methods as
    redirect-irrelevant)."""
    cfg = _minimal_get_config(method=method)
    xml = RestClientOperationBuilder().build(**cfg)
    fr = _find_field(xml, "followRedirects")
    assert fr is None, (
        f"{method} must omit followRedirects when not explicitly supplied; "
        f"got {fr.attrib if fr is not None else None}"
    )


@pytest.mark.parametrize(
    "method,follow_value",
    [
        (m, v)
        for m in (
            "GET", "POST", "HEAD", "DELETE",
            "PATCH", "PUT", "OPTIONS", "TRACE",
        )
        for v in ("NONE", "STRICT", "LAX")
    ],
)
def test_follow_redirects_explicit_values_always_emit(method, follow_value):
    """Explicit NONE/STRICT/LAX values are always emitted regardless of
    method. Verified against the live Lax/Strict GET examples
    (0407d35d, 6dd23a22) which carry explicit followRedirects values."""
    cfg = _minimal_get_config(method=method, follow_redirects=follow_value)
    xml = RestClientOperationBuilder().build(**cfg)
    fr = _find_field(xml, "followRedirects")
    assert fr is not None
    assert fr.attrib.get("value") == follow_value


@pytest.mark.parametrize("method", ("PATCH", "PUT", "OPTIONS", "TRACE"))
def test_follow_redirects_explicit_overrides_omit_default(method):
    """Sanity: PATCH/PUT/OPTIONS/TRACE omit by default, but if the caller
    explicitly supplies followRedirects, the field is emitted."""
    xml = RestClientOperationBuilder().build(
        **_minimal_get_config(method=method, follow_redirects="STRICT")
    )
    fr = _find_field(xml, "followRedirects")
    assert fr is not None
    assert fr.attrib.get("value") == "STRICT"


@pytest.mark.parametrize("bad_value", ["MAYBE", "true", "auto", "none"])
def test_follow_redirects_invalid_value_rejected(bad_value):
    cfg = _minimal_get_config(follow_redirects=bad_value)
    with pytest.raises(BuilderValidationError) as excinfo:
        RestClientOperationBuilder().build(**cfg)
    err = excinfo.value
    assert err.error_code == "REST_OPERATION_VALIDATION_FAILED"
    assert err.field == "follow_redirects"


# ----------------------------------------------------------------------------
# Per-method XML shape regression tests (Phase 5).
# ----------------------------------------------------------------------------


@pytest.mark.parametrize(
    "method,expected_path",
    [
        ("POST", "/v1/items"),
        ("PUT", "/v1/items/42"),
        ("DELETE", "/v1/items/42"),
        ("HEAD", "/v1/items/42"),
        ("OPTIONS", "/v1/items"),
        ("TRACE", "/v1/items/42"),
    ],
)
def test_phase5_methods_emit_path_field(method, expected_path):
    """Path field emission is method-agnostic — verify each new method
    plumbs `path` through to the XML."""
    xml = RestClientOperationBuilder().build(
        **_minimal_get_config(method=method, path=expected_path)
    )
    assert _field_value(xml, "path") == expected_path


@pytest.mark.parametrize("method", ["POST", "PUT", "DELETE", "HEAD", "OPTIONS", "TRACE"])
def test_phase5_methods_emit_empty_customproperty_containers(method):
    """All methods emit empty customProperties containers for queryParameters
    and requestHeaders when none are supplied (Phase 5 — non-empty support
    lands in Phase 6)."""
    xml = RestClientOperationBuilder().build(
        **_minimal_get_config(method=method)
    )
    qp_field = _find_field(xml, "queryParameters")
    rh_field = _find_field(xml, "requestHeaders")
    assert qp_field is not None and qp_field.attrib["type"] == "customproperties"
    assert rh_field is not None and rh_field.attrib["type"] == "customproperties"
    assert qp_field.find("customProperties") is not None
    assert rh_field.find("customProperties") is not None


# ----------------------------------------------------------------------------
# Codex review round 3 — extended secret-key + secret-value detection
# (P1 #1). Previously the secret-shaped-key regex only matched the exact
# `Authorization` header, leaving `Proxy-Authorization` and other variants
# wide open. The value-pattern set missed the canonical HTTP authorization
# scheme prefixes (Bearer/Basic/Digest/Negotiate/NTLM + token) which is
# what `Proxy-Authorization` values typically carry.
# ----------------------------------------------------------------------------


@pytest.mark.parametrize(
    "secret_key",
    [
        "Proxy-Authorization",
        "proxy-authorization",
        "proxy_authorization",
        "Cookie",
        "Set-Cookie",
        "WWW-Authenticate",
        "X-CSRF-Token",
        "X-Session-Token",
        "X-Auth-Password",
    ],
)
def test_codex_round3_extended_secret_keys_rejected(secret_key):
    """Codex review round-3 P1 #1: extend secret-shaped key detection to
    catch Proxy-Authorization and similar credential-bearing headers."""
    cfg = _minimal_get_config(request_headers={secret_key: "anyvalue"})
    with pytest.raises(BuilderValidationError) as excinfo:
        RestClientOperationBuilder().build(**cfg)
    err = excinfo.value
    assert err.error_code == "REST_SECRET_VALUE_FORBIDDEN", (
        f"key {secret_key!r} should match the secret-key pattern"
    )
    assert err.field == "request_headers"


@pytest.mark.parametrize(
    "auth_scheme_value",
    [
        # RFC 7617 Basic auth value shape.
        "Basic dXNlcjpwYXNzd29yZA==",
        # RFC 6750 Bearer.
        "Bearer eyJhbGciOiJIUzI1NiJ9.payload.signature",
        # RFC 7616 Digest.
        "Digest username=\"admin\", realm=\"x\"",
        # RFC 4559 SPNEGO/Negotiate.
        "Negotiate YHsGBisGAQUFAqBxMG+gMDAuB...",
        # Microsoft NTLM challenge token.
        "NTLM TlRMTVNTUAACAAA...",
        # Case-insensitive variants.
        "basic SGVsbG86V29ybGQ=",
        "bearer abcdef",
    ],
)
def test_codex_round3_authorization_scheme_values_rejected(auth_scheme_value):
    """Codex review round-3 P1 #1: the value-pattern set should catch HTTP
    Authorization scheme prefixes (Bearer/Basic/Digest/Negotiate/NTLM)
    even when the KEY isn't named Authorization — e.g. a caller using
    `X-Custom-Header: Basic dXNlcjpwYXNz` would slip past key detection."""
    # Use a non-secret-shaped key so we exercise the VALUE path specifically.
    cfg = _minimal_get_config(request_headers={"X-Custom-Header": auth_scheme_value})
    with pytest.raises(BuilderValidationError) as excinfo:
        RestClientOperationBuilder().build(**cfg)
    err = excinfo.value
    assert err.error_code == "REST_SECRET_VALUE_FORBIDDEN"
    assert err.field == "request_headers"


def test_codex_round3_proxy_authorization_with_basic_value_full_path():
    """End-to-end: `Proxy-Authorization: Basic dXNlcjpwYXNz` was the
    specific example called out in the codex review. Verify the rejection
    happens AND the credential canary doesn't leak into the error payload."""
    cfg = _minimal_get_config(request_headers={
        "Proxy-Authorization": "Basic dXNlcjpwYXNzd29yZF9DQU5BUlk=",  # canary in base64
    })
    with pytest.raises(BuilderValidationError) as excinfo:
        RestClientOperationBuilder().build(**cfg)
    err = excinfo.value
    assert err.error_code == "REST_SECRET_VALUE_FORBIDDEN"
    assert "dXNlcjpwYXNzd29yZF9DQU5BUlk" not in str(err)
    assert "dXNlcjpwYXNzd29yZF9DQU5BUlk" not in (err.hint or "")


# Regression: the existing safe-headers test must still pass after extending
# the secret pattern. Make sure common headers don't false-positive.
def test_codex_round3_common_safe_headers_still_pass():
    """Defense: extended secret detection must not flag normal headers."""
    xml = RestClientOperationBuilder().build(
        **_minimal_get_config(request_headers={
            "Accept": "application/json",
            "Content-Type": "application/json",
            "User-Agent": "boomi-test/1.0",
            "Cache-Control": "no-cache",
            "Accept-Language": "en-US",
            "X-Request-Id": "abc-123-def",
            "X-Trace-Id": "trace-001",
            "If-None-Match": '"etag-value"',
            "Accept-Encoding": "gzip, deflate",
        })
    )
    # Builder succeeded — all 9 headers emitted.
    qp_field = _find_field(xml, "requestHeaders")
    cp = qp_field.find("customProperties")
    assert len(list(cp)) == 9


# ----------------------------------------------------------------------------
# Codex round 2 — strict bool validation for return_application_errors and
# track_response. Before the fix the build path used bool(...) coercion, so
# `"false"` (string) became True (Python truthy) and `0` became False
# silently, corrupting the emitted XML attribute. The validator must reject
# non-bool values up front.
# ----------------------------------------------------------------------------


@pytest.mark.parametrize("bool_field", ["return_application_errors", "track_response"])
@pytest.mark.parametrize("non_bool_value", ["true", "false", "False", "True", 0, 1, "yes", []])
def test_non_bool_operation_flag_rejected(bool_field, non_bool_value):
    """Non-bool values for return_application_errors / track_response must
    be rejected, not silently coerced to True/False by bool()."""
    cfg = _minimal_get_config(**{bool_field: non_bool_value})
    with pytest.raises(BuilderValidationError) as excinfo:
        RestClientOperationBuilder().build(**cfg)
    err = excinfo.value
    assert err.error_code == "REST_OPERATION_VALIDATION_FAILED"
    assert err.field == bool_field


@pytest.mark.parametrize("bool_field", ["return_application_errors", "track_response"])
@pytest.mark.parametrize("bool_value", [True, False])
def test_bool_operation_flag_accepted_and_emitted_verbatim(bool_field, bool_value):
    """Bool True/False must pass validation AND emit the corresponding
    attribute value in the XML envelope. No coercion — caller's choice
    flows through."""
    xml = _build_get(**{bool_field: bool_value})
    root = ET.fromstring(xml)
    op = root.find("bns:object/Operation", NS)
    attr_name = {
        "return_application_errors": "returnApplicationErrors",
        "track_response": "trackResponse",
    }[bool_field]
    expected = "true" if bool_value else "false"
    assert op.attrib[attr_name] == expected


@pytest.mark.parametrize("bool_field", ["return_application_errors", "track_response"])
def test_missing_operation_flag_defaults_to_true(bool_field):
    """When the caller omits the flag entirely, the default is True
    (matches existing live-export behavior)."""
    cfg = _minimal_get_config()
    cfg.pop(bool_field, None)
    xml = RestClientOperationBuilder().build(**cfg)
    root = ET.fromstring(xml)
    op = root.find("bns:object/Operation", NS)
    attr_name = {
        "return_application_errors": "returnApplicationErrors",
        "track_response": "trackResponse",
    }[bool_field]
    assert op.attrib[attr_name] == "true"


@pytest.mark.parametrize("bool_field", ["return_application_errors", "track_response"])
def test_none_operation_flag_accepted_as_default(bool_field):
    """Caller passes the key explicitly with value None — treat as
    "not supplied" (default True) for consistency with the other
    optional-field gates."""
    cfg = _minimal_get_config(**{bool_field: None})
    xml = RestClientOperationBuilder().build(**cfg)
    root = ET.fromstring(xml)
    op = root.find("bns:object/Operation", NS)
    attr_name = {
        "return_application_errors": "returnApplicationErrors",
        "track_response": "trackResponse",
    }[bool_field]
    assert op.attrib[attr_name] == "true"


# ============================================================================
# Issue #45 — Component XML update preservation
# ============================================================================


def test_rest_client_operation_preservation_policy_attached():
    policy = RestClientOperationBuilder.PRESERVATION_POLICY
    assert policy.component_type == "connector-action"
    assert policy.subtype == REST_CLIENT_SUBTYPE
    paths_by_mode = {op.mode: op for op in policy.owned_paths}
    # Operation envelope owns specific attrs (returnApplicationErrors,
    # trackResponse) — Codex r1 P2 follow-up.
    op_envelope = paths_by_mode["attrs_only"]
    assert op_envelope.path == "bns:object/Operation"
    assert "returnApplicationErrors" in op_envelope.owned_attrs
    assert "trackResponse" in op_envelope.owned_attrs
    # GenericOperationConfig owns its attrs + keyed children. owned_keys
    # is required so a method change (GET→PATCH dropping followRedirects)
    # actually clears the stale field — Codex r1 P2 follow-up.
    cfg = paths_by_mode["key_merge"]
    assert cfg.path == "bns:object/Operation/Configuration/GenericOperationConfig"
    assert cfg.key_attr == "id"
    assert "followRedirects" in cfg.owned_keys


def test_rest_client_operation_update_preserves_unknown_operation_fields_and_siblings():
    """Operation envelope siblings (Tracking, Caching) and unknown field
    ids inside GenericOperationConfig must survive a builder-driven update."""
    from boomi_mcp.categories.components.component_update_preservation import (
        merge_for_update,
    )

    desired = _build_get(path="/v1/items/new")
    current = _build_get(path="/v1/items/old")
    # Inject Tracking config + an unknown field id in current
    current = current.replace(
        "<Tracking><TrackedFields/></Tracking>",
        "<Tracking><TrackedFields><TrackedField name=\"x\" path=\"//y\"/></TrackedFields></Tracking>",
    )
    current = current.replace(
        "                </GenericOperationConfig>",
        (
            '                    <field id="futureRestField" type="string" value="opaque"/>\n'
            "                </GenericOperationConfig>"
        ),
    )

    merged = merge_for_update(
        current, desired, RestClientOperationBuilder.PRESERVATION_POLICY
    )
    root = ET.fromstring(merged)
    # Builder-owned `path` field was replaced
    fields = {
        f.attrib.get("id"): f.attrib.get("value")
        for f in root.findall(
            "bns:object/Operation/Configuration/GenericOperationConfig/field", NS
        )
    }
    assert fields.get("path") == "/v1/items/new"
    # Unknown future field id preserved
    assert fields.get("futureRestField") == "opaque"
    # Operation envelope Tracking sibling preserved
    tracked = root.find(
        "bns:object/Operation/Tracking/TrackedFields/TrackedField", NS
    )
    assert tracked is not None
    assert tracked.attrib["name"] == "x"
