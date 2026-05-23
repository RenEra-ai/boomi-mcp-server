"""Tests for RestClientConnectionBuilder (issue #24, M2.4).

Shape locked against the verified Renera live export:
  profile=reneraai-5RO3DD, component_id=d6ee8b5b-6d83-44c0-9e77-216a60adb452

Builder emits Boomi REST Client connection components
(subType="officialboomi-X3979C-rest-prod"). Issue #24 ships only the
OAUTH2 client_credentials shape; all other auth modes return
UNSUPPORTED_REST_AUTH_MODE.
"""

import xml.etree.ElementTree as ET

import pytest

from boomi_mcp.categories.components.builders.connector_builder import (
    BuilderValidationError,
    RestClientConnectionBuilder,
    REST_CLIENT_SUBTYPE,
    get_connector_builder,
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


def _minimal_oauth2_config(**overrides):
    """Minimal-valid REST OAUTH2 client_credentials config dict."""
    params = {
        "connector_type": "rest",
        "component_name": "Target REST OAuth2 Connection",
        "base_url": "https://api.example.com",
        "auth": "OAUTH2",
        "oauth2": {
            "grant_type": "client_credentials",
            "client_id": "boomi-client",
            "client_secret_ref": "credential://target-api/oauth-client-secret",
            "access_token_url": "https://api.example.com/oauth/token",
            "scope": "",
            "credentials_assertion_type": "client_secret",
        },
    }
    params.update(overrides)
    return params


def _build_minimal(**overrides):
    """Render minimal-valid XML — used for XML-shape tests."""
    params = _minimal_oauth2_config(**overrides)
    params.pop("connector_type", None)
    return RestClientConnectionBuilder().build(**params)


# ----------------------------------------------------------------------------
# Subtype and aliases
# ----------------------------------------------------------------------------

def test_rest_client_subtype_constant():
    assert REST_CLIENT_SUBTYPE == "officialboomi-X3979C-rest-prod"


@pytest.mark.parametrize("alias", ["rest", "rest_client", REST_CLIENT_SUBTYPE])
def test_alias_resolves_to_rest_client_builder(alias):
    builder = get_connector_builder(alias)
    assert isinstance(builder, RestClientConnectionBuilder)


# ----------------------------------------------------------------------------
# Component shell
# ----------------------------------------------------------------------------

def test_minimum_required_fields_produce_valid_xml():
    xml = _build_minimal()
    root = ET.fromstring(xml)
    assert root.tag == "{http://api.platform.boomi.com/}Component"
    assert root.attrib["type"] == "connector-settings"
    assert root.attrib["subType"] == REST_CLIENT_SUBTYPE
    assert root.attrib["name"] == "Target REST OAuth2 Connection"
    assert root.attrib["folderName"] == "Home"


def test_encrypted_values_header_references_client_secret_path():
    xml = _build_minimal()
    root = ET.fromstring(xml)
    encrypted_values = root.find("bns:encryptedValues", NS)
    assert encrypted_values is not None
    entries = encrypted_values.findall("bns:encryptedValue", NS)
    assert len(entries) == 1
    assert entries[0].attrib["path"] == "//GenericConnectionConfig/field/OAuth2Config/credentials/@clientSecret"
    # Builder emits isSet="false" — the secret value is supplied via the
    # Boomi UI (or pre-encrypted XML escape hatch) after create.
    assert entries[0].attrib["isSet"] == "false"


def test_genericconnectionconfig_present():
    xml = _build_minimal()
    root = ET.fromstring(xml)
    gcc = root.find("bns:object/GenericConnectionConfig", NS)
    assert gcc is not None


def test_field_order_matches_live_shape():
    """Verified against d6ee8b5b export (RenEra Local REST Connection)."""
    xml = _build_minimal()
    root = ET.fromstring(xml)
    gcc = root.find("bns:object/GenericConnectionConfig", NS)
    field_ids = [child.attrib["id"] for child in gcc if child.tag == "field"]
    assert field_ids == [
        "url",
        "auth",
        "username",
        "password",
        "domain",
        "workstation",
        "customAuthCredentials",
        "preemptive",
        "awsAccessKey",
        "awsSecretKey",
        "awsService",
        "customAwsService",
        "awsRegion",
        "customAwsRegion",
        "awsProfileArn",
        "awsRoleArn",
        "awsTrustAnchorArn",
        "awsRolesAnywhereRegion",
        "awsRolesAnywhereCustomRegion",
        "awsSessionName",
        "awsDuration",
        "awsPublicCertificate",
        "awsPrivateKey",
        "oauthContext",
        "privateCertificate",
        "publicCertificate",
        "connectTimeout",
        "readTimeout",
        "cookieScope",
        "enableConnectionPooling",
        "maxTotal",
        "idleTimeout",
    ]


def _field_value(xml: str, field_id: str) -> str:
    root = ET.fromstring(xml)
    for field in root.find("bns:object/GenericConnectionConfig", NS):
        if field.tag == "field" and field.attrib.get("id") == field_id:
            return field.attrib.get("value", "")
    raise AssertionError(f"field id={field_id!r} not found")


def test_url_field_carries_base_url():
    xml = _build_minimal(base_url="https://api.acme.test")
    assert _field_value(xml, "url") == "https://api.acme.test"


def test_auth_field_set_to_oauth2():
    xml = _build_minimal()
    assert _field_value(xml, "auth") == "OAUTH2"


def test_defaults_for_preemptive_timeouts_pool():
    xml = _build_minimal()
    assert _field_value(xml, "preemptive") == "false"
    assert _field_value(xml, "connectTimeout") == "-1"
    assert _field_value(xml, "readTimeout") == "-1"
    assert _field_value(xml, "cookieScope") == "GLOBAL"
    assert _field_value(xml, "enableConnectionPooling") == "false"


def test_aws_fields_emitted_empty():
    """Live shape always emits the AWS skeleton even when AWS auth is unused."""
    xml = _build_minimal()
    for aws_field in (
        "awsAccessKey",
        "awsSecretKey",
        "awsService",
        "customAwsService",
        "awsRegion",
        "customAwsRegion",
    ):
        assert _field_value(xml, aws_field) == ""


# ----------------------------------------------------------------------------
# OAuth2Config inner block
# ----------------------------------------------------------------------------

def _oauth2_config(xml: str) -> ET.Element:
    root = ET.fromstring(xml)
    for field in root.find("bns:object/GenericConnectionConfig", NS):
        if field.tag == "field" and field.attrib.get("id") == "oauthContext":
            oa = field.find("OAuth2Config")
            assert oa is not None
            return oa
    raise AssertionError("oauthContext field not found")


def test_oauth2_config_grant_type_client_credentials():
    oa = _oauth2_config(_build_minimal())
    assert oa.attrib["grantType"] == "client_credentials"


def test_oauth2_config_children_in_correct_order():
    oa = _oauth2_config(_build_minimal())
    tags = [child.tag for child in oa]
    assert tags == [
        "credentials",
        "authorizationTokenEndpoint",
        "authorizationParameters",
        "accessTokenEndpoint",
        "accessTokenParameters",
        "scope",
        "jwtParameters",
        "credentialsAssertionType",
    ]


def test_oauth2_credentials_emit_clientid_but_blank_secret():
    """Plaintext clientSecret value must NEVER appear in builder output —
    Boomi stores it as ciphertext, set via the UI after create."""
    oa = _oauth2_config(_build_minimal())
    creds = oa.find("credentials")
    assert creds is not None
    assert creds.attrib["clientId"] == "boomi-client"
    assert creds.attrib["clientSecret"] == ""
    # accessTokenKey is also Boomi-generated; builder emits empty.
    assert creds.attrib["accessTokenKey"] == ""


def test_oauth2_access_token_endpoint_url():
    oa = _oauth2_config(_build_minimal())
    ate = oa.find("accessTokenEndpoint")
    assert ate is not None
    assert ate.attrib["url"] == "https://api.example.com/oauth/token"
    assert ate.find("sslOptions") is not None


def test_oauth2_authorization_token_endpoint_empty_for_client_credentials():
    """client_credentials doesn't use authorizationTokenEndpoint, but Boomi's
    UI export still emits the empty element with url='' and an sslOptions
    child. Match that shape."""
    oa = _oauth2_config(_build_minimal())
    ate = oa.find("authorizationTokenEndpoint")
    assert ate is not None
    assert ate.attrib["url"] == ""
    assert ate.find("sslOptions") is not None


def test_oauth2_jwt_parameters_default_expiration_zero():
    oa = _oauth2_config(_build_minimal())
    jwt = oa.find("jwtParameters")
    assert jwt is not None
    expiration = jwt.find("expiration")
    assert expiration is not None
    assert (expiration.text or "") == "0"


def test_oauth2_credentials_assertion_type_default_client_secret():
    oa = _oauth2_config(_build_minimal())
    cat = oa.find("credentialsAssertionType")
    assert cat is not None
    assert cat.text == "client_secret"


def test_oauth2_client_secret_value_not_in_xml():
    """Defense-in-depth: even if a plaintext oauth2.client_secret value
    somehow reaches build() (shouldn't, validation rejects it), it must
    not appear in emitted XML."""
    # Construct via build() bypassing validation isn't possible — this test
    # asserts the bytes via the shape: clientSecret attribute is empty.
    xml = _build_minimal()
    assert 'clientSecret=""' in xml
    assert "DEADBEEF" not in xml  # canary value


# ----------------------------------------------------------------------------
# XML escaping
# ----------------------------------------------------------------------------

def test_special_xml_characters_in_values_are_escaped():
    xml = _build_minimal(
        component_name='REST "Prod" & <Dev>',
        base_url="https://api.example.com/?q=a&b=<x>",
    )
    root = ET.fromstring(xml)
    assert root.attrib["name"] == 'REST "Prod" & <Dev>'
    assert _field_value(xml, "url") == "https://api.example.com/?q=a&b=<x>"


# ----------------------------------------------------------------------------
# Validation — required fields
# ----------------------------------------------------------------------------

def test_missing_component_name_raises():
    cfg = _minimal_oauth2_config(component_name="")
    with pytest.raises(BuilderValidationError) as excinfo:
        RestClientConnectionBuilder().build(**cfg)
    err = excinfo.value
    assert err.error_code == "REST_CONNECTOR_VALIDATION_FAILED"
    assert err.field == "component_name"


def test_missing_base_url_raises_structured_error():
    cfg = _minimal_oauth2_config()
    cfg["base_url"] = ""
    with pytest.raises(BuilderValidationError) as excinfo:
        RestClientConnectionBuilder().build(**cfg)
    err = excinfo.value
    assert err.error_code == "REST_BASE_URL_REQUIRED"
    assert err.field == "base_url"


@pytest.mark.parametrize("bad_url", ["ftp://x", "file:///tmp", "not-a-url"])
def test_invalid_base_url_scheme(bad_url):
    with pytest.raises(BuilderValidationError) as excinfo:
        RestClientConnectionBuilder().build(**_minimal_oauth2_config(base_url=bad_url))
    err = excinfo.value
    assert err.error_code == "REST_BASE_URL_INVALID"
    assert err.field == "base_url"


def test_https_base_url_accepted():
    xml = _build_minimal(base_url="https://api.example.com")
    assert _field_value(xml, "url") == "https://api.example.com"


def test_http_base_url_accepted():
    xml = _build_minimal(base_url="http://host.docker.internal:8080")
    assert _field_value(xml, "url") == "http://host.docker.internal:8080"


# ----------------------------------------------------------------------------
# Validation — auth mode gating
# ----------------------------------------------------------------------------

@pytest.mark.parametrize(
    "unsupported_auth",
    ["NONE", "BASIC", "PASSWORD_DIGEST", "NTLM", "CUSTOM",
     "AWS_SIGNATURE", "AWS_IAM_ROLES_ANYWHERE"],
)
def test_unsupported_auth_modes_rejected(unsupported_auth):
    cfg = _minimal_oauth2_config()
    cfg["auth"] = unsupported_auth
    cfg.pop("oauth2", None)
    with pytest.raises(BuilderValidationError) as excinfo:
        RestClientConnectionBuilder().build(**cfg)
    err = excinfo.value
    assert err.error_code == "UNSUPPORTED_REST_AUTH_MODE"
    assert err.field == "auth"


def test_unknown_auth_mode_rejected():
    cfg = _minimal_oauth2_config(auth="MAGIC")
    cfg.pop("oauth2", None)
    with pytest.raises(BuilderValidationError) as excinfo:
        RestClientConnectionBuilder().build(**cfg)
    assert excinfo.value.error_code == "UNSUPPORTED_REST_AUTH_MODE"


# ----------------------------------------------------------------------------
# Validation — OAuth2 sub-block
# ----------------------------------------------------------------------------

def test_oauth2_block_required_when_auth_is_oauth2():
    cfg = _minimal_oauth2_config()
    cfg.pop("oauth2")
    with pytest.raises(BuilderValidationError) as excinfo:
        RestClientConnectionBuilder().build(**cfg)
    err = excinfo.value
    assert err.error_code == "REST_CONNECTOR_VALIDATION_FAILED"
    assert err.field == "oauth2"


def test_oauth2_grant_type_only_client_credentials():
    cfg = _minimal_oauth2_config()
    cfg["oauth2"]["grant_type"] = "authorization_code"
    with pytest.raises(BuilderValidationError) as excinfo:
        RestClientConnectionBuilder().build(**cfg)
    err = excinfo.value
    assert err.error_code == "UNSUPPORTED_REST_AUTH_MODE"
    assert err.field == "oauth2.grant_type"


@pytest.mark.parametrize("field", ["client_id", "client_secret_ref", "access_token_url"])
def test_oauth2_required_subfields(field):
    cfg = _minimal_oauth2_config()
    cfg["oauth2"][field] = ""
    with pytest.raises(BuilderValidationError) as excinfo:
        RestClientConnectionBuilder().build(**cfg)
    err = excinfo.value
    assert err.error_code == "REST_CONNECTOR_VALIDATION_FAILED"
    assert err.field == f"oauth2.{field}"


def test_oauth2_client_secret_ref_must_use_credential_scheme():
    cfg = _minimal_oauth2_config()
    cfg["oauth2"]["client_secret_ref"] = "raw-secret-value"
    with pytest.raises(BuilderValidationError) as excinfo:
        RestClientConnectionBuilder().build(**cfg)
    err = excinfo.value
    assert err.error_code == "REST_SECRET_VALUE_FORBIDDEN"
    assert err.field == "oauth2.client_secret_ref"


def test_oauth2_client_secret_plaintext_rejected():
    """Defense against caller putting the raw value under oauth2.client_secret
    (instead of oauth2.client_secret_ref)."""
    cfg = _minimal_oauth2_config()
    cfg["oauth2"]["client_secret"] = "DEADBEEF_OAUTH2_SECRET"
    with pytest.raises(BuilderValidationError) as excinfo:
        RestClientConnectionBuilder().build(**cfg)
    err = excinfo.value
    assert err.error_code in ("REST_SECRET_VALUE_FORBIDDEN", "PLAINTEXT_SECRET_REJECTED")
    assert "DEADBEEF_OAUTH2_SECRET" not in str(err)
    assert "DEADBEEF_OAUTH2_SECRET" not in (err.hint or "")


# ----------------------------------------------------------------------------
# Plaintext secret rejection (top-level)
# ----------------------------------------------------------------------------

@pytest.mark.parametrize("forbidden", _FORBIDDEN_TOPLEVEL_SECRET_FIELDS)
def test_top_level_forbidden_secret_fields_rejected(forbidden):
    cfg = _minimal_oauth2_config(**{forbidden: "DEADBEEF_REST_CONN"})
    with pytest.raises(BuilderValidationError) as excinfo:
        RestClientConnectionBuilder().build(**cfg)
    err = excinfo.value
    assert err.error_code == "PLAINTEXT_SECRET_REJECTED"
    assert err.field == forbidden
    assert "DEADBEEF_REST_CONN" not in str(err)
    assert "DEADBEEF_REST_CONN" not in (err.hint or "")


def test_validate_config_returns_none_for_valid_oauth2():
    assert RestClientConnectionBuilder.validate_config(_minimal_oauth2_config()) is None


def test_validate_config_surfaces_error_without_raising():
    err = RestClientConnectionBuilder.validate_config(_minimal_oauth2_config(base_url=""))
    assert isinstance(err, BuilderValidationError)
    assert err.error_code == "REST_BASE_URL_REQUIRED"


def test_scan_forbidden_secret_fields_descends_into_oauth2():
    err = RestClientConnectionBuilder.scan_forbidden_secret_fields({
        "oauth2": {"password": "DEEP_DEADBEEF"},
    })
    assert isinstance(err, BuilderValidationError)
    assert err.error_code == "PLAINTEXT_SECRET_REJECTED"
    assert err.field == "oauth2.password"


# ----------------------------------------------------------------------------
# Pooling + timeout type validation (codex round-2 P2 #B)
# ----------------------------------------------------------------------------

def test_connection_pooling_must_be_dict():
    """A string in place of the connection_pooling dict used to crash with
    AttributeError when build() called pooling.get(...). Now rejected
    cleanly with REST_POOLING_INVALID."""
    cfg = _minimal_oauth2_config(connection_pooling="not-a-dict")
    with pytest.raises(BuilderValidationError) as excinfo:
        RestClientConnectionBuilder().build(**cfg)
    err = excinfo.value
    assert err.error_code == "REST_POOLING_INVALID"
    assert err.field == "connection_pooling"


def test_connection_pooling_enabled_must_be_bool():
    cfg = _minimal_oauth2_config(connection_pooling={"enabled": "yes"})
    with pytest.raises(BuilderValidationError) as excinfo:
        RestClientConnectionBuilder().build(**cfg)
    err = excinfo.value
    assert err.error_code == "REST_POOLING_INVALID"
    assert err.field == "connection_pooling.enabled"


def test_connection_pooling_max_total_must_be_int():
    cfg = _minimal_oauth2_config(connection_pooling={"enabled": True, "max_total": "abc"})
    with pytest.raises(BuilderValidationError) as excinfo:
        RestClientConnectionBuilder().build(**cfg)
    err = excinfo.value
    assert err.error_code == "REST_POOLING_INVALID"
    assert err.field == "connection_pooling.max_total"


def test_connection_pooling_idle_timeout_must_be_int():
    cfg = _minimal_oauth2_config(connection_pooling={"enabled": True, "idle_timeout_seconds": 3.14})
    with pytest.raises(BuilderValidationError) as excinfo:
        RestClientConnectionBuilder().build(**cfg)
    err = excinfo.value
    assert err.error_code == "REST_POOLING_INVALID"
    assert err.field == "connection_pooling.idle_timeout_seconds"


def test_connection_pooling_unknown_key_rejected():
    cfg = _minimal_oauth2_config(connection_pooling={"enabled": False, "bogus_pool_key": 1})
    with pytest.raises(BuilderValidationError) as excinfo:
        RestClientConnectionBuilder().build(**cfg)
    err = excinfo.value
    assert err.error_code == "REST_POOLING_INVALID"
    assert "bogus_pool_key" in (err.field or "")


def test_connection_pooling_dict_with_valid_values_accepted():
    """Sanity: a well-formed pooling block builds without error."""
    cfg = _minimal_oauth2_config(
        connection_pooling={"enabled": True, "max_total": 20, "idle_timeout_seconds": 30},
    )
    xml = RestClientConnectionBuilder().build(**cfg)
    assert _field_value(xml, "enableConnectionPooling") == "true"
    assert _field_value(xml, "maxTotal") == "20"
    assert _field_value(xml, "idleTimeout") == "30"


def test_connect_timeout_ms_must_be_int():
    cfg = _minimal_oauth2_config(connect_timeout_ms="abc")
    with pytest.raises(BuilderValidationError) as excinfo:
        RestClientConnectionBuilder().build(**cfg)
    err = excinfo.value
    assert err.error_code == "REST_CONNECTOR_VALIDATION_FAILED"
    assert err.field == "connect_timeout_ms"


def test_read_timeout_ms_must_be_int():
    cfg = _minimal_oauth2_config(read_timeout_ms=3.14)
    with pytest.raises(BuilderValidationError) as excinfo:
        RestClientConnectionBuilder().build(**cfg)
    err = excinfo.value
    assert err.error_code == "REST_CONNECTOR_VALIDATION_FAILED"
    assert err.field == "read_timeout_ms"


def test_preemptive_must_be_bool():
    cfg = _minimal_oauth2_config(preemptive="false")  # string, not bool
    with pytest.raises(BuilderValidationError) as excinfo:
        RestClientConnectionBuilder().build(**cfg)
    err = excinfo.value
    assert err.error_code == "REST_CONNECTOR_VALIDATION_FAILED"
    assert err.field == "preemptive"


def test_timeouts_accept_negative_int_for_indefinite_wait():
    """Boomi docs: timeout values ≤ 0 mean wait indefinitely."""
    xml = _build_minimal(connect_timeout_ms=-1, read_timeout_ms=0)
    assert _field_value(xml, "connectTimeout") == "-1"
    assert _field_value(xml, "readTimeout") == "0"
