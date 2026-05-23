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
    ["PASSWORD_DIGEST", "CUSTOM", "AWS_SIGNATURE", "AWS_IAM_ROLES_ANYWHERE"],
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


def test_oauth2_grant_type_unsupported_grants_rejected():
    """resource_owner_credentials and jwt_bearer remain deferred until
    verified live exports exist. authorization_code is now supported
    (Phase 4) and is exercised by the OAuth2 authorization_code test
    section below."""
    cfg = _minimal_oauth2_config()
    cfg["oauth2"]["grant_type"] = "resource_owner_credentials"
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


# ----------------------------------------------------------------------------
# NONE auth (issue #24 follow-up — new_findings_2026_05_23 expansion).
# Live shape verified against:
#   - 7f7e0730-1152-4467-b912-e3a8ed12782a (REST None — pooling disabled, no certs)
#   - 49402e41-522f-4b33-83f3-c95d907efa23 (REST None Pooling — pooling enabled)
#   - 499e5bd6-598a-4c50-b941-527c8f7470dc (REST Certificate — privateCertificate + publicCertificate refs, auth=NONE)
# ----------------------------------------------------------------------------


def _minimal_none_config(**overrides):
    """Minimal-valid REST NONE auth config dict."""
    params = {
        "connector_type": "rest",
        "component_name": "Target REST NONE Connection",
        "base_url": "https://api.example.com",
        "auth": "NONE",
    }
    params.update(overrides)
    return params


def _build_minimal_none(**overrides):
    params = _minimal_none_config(**overrides)
    params.pop("connector_type", None)
    return RestClientConnectionBuilder().build(**params)


def test_none_auth_minimum_required_fields_produce_valid_xml():
    xml = _build_minimal_none()
    root = ET.fromstring(xml)
    assert root.attrib["type"] == "connector-settings"
    assert root.attrib["subType"] == REST_CLIENT_SUBTYPE
    assert _field_value(xml, "auth") == "NONE"
    assert _field_value(xml, "url") == "https://api.example.com"


def test_none_auth_validate_config_returns_none():
    assert RestClientConnectionBuilder.validate_config(_minimal_none_config()) is None


def test_none_auth_does_not_require_oauth2_block():
    """For NONE auth the oauth2 sub-block is irrelevant. Builder must accept
    a config without it."""
    cfg = _minimal_none_config()
    assert "oauth2" not in cfg
    xml = RestClientConnectionBuilder().build(**{k: v for k, v in cfg.items() if k != "connector_type"})
    assert _field_value(xml, "auth") == "NONE"


def test_none_auth_emits_empty_encrypted_values_block():
    """Live REST None / REST Certificate / REST None Pooling all carry
    `<bns:encryptedValues/>` (self-closing, no inner entries). Verified
    against 7f7e0730, 499e5bd6, 49402e41."""
    xml = _build_minimal_none()
    root = ET.fromstring(xml)
    encrypted_values = root.find("bns:encryptedValues", NS)
    assert encrypted_values is not None
    assert encrypted_values.findall("bns:encryptedValue", NS) == [], (
        "NONE auth must emit empty <bns:encryptedValues/> — no OAuth2 "
        "clientSecret path leaks into the connection envelope."
    )


def test_none_auth_preemptive_field_emitted_with_empty_value():
    """Live REST None / NTLM emit `<field id="preemptive" type="boolean" value=""/>`
    (NOT value="false"). Preemptive is irrelevant for non-BASIC/non-OAUTH2
    auth so Boomi leaves it blank. Match the live shape."""
    xml = _build_minimal_none()
    assert _field_value(xml, "preemptive") == ""


def test_none_auth_oauth2_config_skeleton_uses_grant_type_code():
    """Live non-OAUTH2 exports keep the OAuth2Config child as a SKELETON
    with grantType='code', empty credentials/endpoints/scope, and NO
    credentialsAssertionType element. Match that shape (so the connection
    can be safely UI-promoted to OAuth2 later without re-creating)."""
    xml = _build_minimal_none()
    oa = _oauth2_config(xml)
    assert oa.attrib["grantType"] == "code"
    creds = oa.find("credentials")
    assert creds is not None
    assert creds.attrib.get("clientId", "") == ""
    assert "clientSecret" not in creds.attrib, (
        "Skeleton OAuth2 credentials must omit clientSecret entirely "
        "(only populated grants emit it)."
    )
    assert "accessTokenKey" not in creds.attrib
    # credentialsAssertionType element is absent in the skeleton.
    assert oa.find("credentialsAssertionType") is None


def test_none_auth_field_order_matches_live_shape():
    """NONE auth must emit the same field skeleton order as OAUTH2 — the
    skeleton is universal across all auth modes (live behavior)."""
    xml = _build_minimal_none()
    root = ET.fromstring(xml)
    gcc = root.find("bns:object/GenericConnectionConfig", NS)
    field_ids = [child.attrib["id"] for child in gcc if child.tag == "field"]
    # Same order as test_field_order_matches_live_shape (OAUTH2 test) —
    # the skeleton is universal.
    assert field_ids == [
        "url", "auth", "username", "password", "domain", "workstation",
        "customAuthCredentials", "preemptive",
        "awsAccessKey", "awsSecretKey", "awsService", "customAwsService",
        "awsRegion", "customAwsRegion", "awsProfileArn", "awsRoleArn",
        "awsTrustAnchorArn", "awsRolesAnywhereRegion",
        "awsRolesAnywhereCustomRegion", "awsSessionName", "awsDuration",
        "awsPublicCertificate", "awsPrivateKey",
        "oauthContext",
        "privateCertificate", "publicCertificate",
        "connectTimeout", "readTimeout", "cookieScope",
        "enableConnectionPooling", "maxTotal", "idleTimeout",
    ]


def test_none_auth_private_certificate_self_closing_when_no_ref():
    """Live REST None / REST None Pooling emit
    `<field id="privateCertificate" type="privatecertificate"/>` —
    self-closing, NO value attribute — when no cert ref is supplied.
    (REST Certificate populates the value attribute.)"""
    xml = _build_minimal_none()
    root = ET.fromstring(xml)
    private_cert = None
    public_cert = None
    for field in root.find("bns:object/GenericConnectionConfig", NS):
        if field.tag == "field":
            fid = field.attrib.get("id")
            if fid == "privateCertificate":
                private_cert = field
            elif fid == "publicCertificate":
                public_cert = field
    assert private_cert is not None
    assert public_cert is not None
    # Self-closing form: NO value attribute when no ref supplied.
    assert "value" not in private_cert.attrib, (
        "privateCertificate must be self-closing (no value attribute) when "
        f"no ref supplied. Got attribs: {private_cert.attrib}"
    )
    assert "value" not in public_cert.attrib


def test_none_auth_does_not_emit_plaintext_secret_canary():
    """Defense-in-depth: a NONE auth connection shouldn't expose any secret
    bytes since it has none. Confirms no stale OAuth2 emission leaks
    plaintext."""
    xml = _build_minimal_none()
    assert "DEADBEEF" not in xml
    assert 'isSet="true"' not in xml  # all encrypted markers should be absent or isSet="false"


# ----------------------------------------------------------------------------
# NONE auth + connection pooling (live: REST None Pooling — 49402e41).
# ----------------------------------------------------------------------------


def test_none_auth_with_pooling_enabled_emits_max_total_and_idle_timeout():
    xml = _build_minimal_none(
        connection_pooling={"enabled": True, "max_total": 20, "idle_timeout_seconds": 30},
    )
    assert _field_value(xml, "enableConnectionPooling") == "true"
    assert _field_value(xml, "maxTotal") == "20"
    assert _field_value(xml, "idleTimeout") == "30"


def test_none_auth_with_pooling_disabled_emits_empty_max_total_and_idle_timeout():
    """Live REST None: pooling disabled → maxTotal and idleTimeout are
    emitted with value="" (NOT absent). Builder must match."""
    xml = _build_minimal_none(connection_pooling={"enabled": False})
    assert _field_value(xml, "enableConnectionPooling") == "false"
    assert _field_value(xml, "maxTotal") == ""
    assert _field_value(xml, "idleTimeout") == ""


# ----------------------------------------------------------------------------
# Certificate refs (independent option — works with any auth, per user
# direction "cert refs cound be with other auth too").
# Live reference: 499e5bd6 (REST Certificate, auth=NONE) — but the cert
# refs themselves are NOT tied to auth=NONE; they are a separate REST
# client-cert option that can co-occur with any auth selection.
# ----------------------------------------------------------------------------


_PRIV_CERT_REF = "21f598a6-1d90-4578-a35a-d0350c50b747"
_PUB_CERT_REF = "ea82aa0c-484b-40b1-890c-f142ab8fecad"


def test_private_certificate_ref_emitted_as_field_value():
    xml = _build_minimal_none(private_certificate_ref=_PRIV_CERT_REF)
    root = ET.fromstring(xml)
    for field in root.find("bns:object/GenericConnectionConfig", NS):
        if field.tag == "field" and field.attrib.get("id") == "privateCertificate":
            assert field.attrib.get("value") == _PRIV_CERT_REF
            assert field.attrib.get("type") == "privatecertificate"
            return
    raise AssertionError("privateCertificate field not found")


def test_public_certificate_ref_emitted_as_field_value():
    xml = _build_minimal_none(public_certificate_ref=_PUB_CERT_REF)
    root = ET.fromstring(xml)
    for field in root.find("bns:object/GenericConnectionConfig", NS):
        if field.tag == "field" and field.attrib.get("id") == "publicCertificate":
            assert field.attrib.get("value") == _PUB_CERT_REF
            assert field.attrib.get("type") == "publiccertificate"
            return
    raise AssertionError("publicCertificate field not found")


def test_both_certificate_refs_can_be_supplied_simultaneously():
    """Live REST Certificate uses both refs at once."""
    xml = _build_minimal_none(
        private_certificate_ref=_PRIV_CERT_REF,
        public_certificate_ref=_PUB_CERT_REF,
    )
    root = ET.fromstring(xml)
    seen = {}
    for field in root.find("bns:object/GenericConnectionConfig", NS):
        if field.tag == "field" and field.attrib.get("id") in ("privateCertificate", "publicCertificate"):
            seen[field.attrib["id"]] = field.attrib.get("value")
    assert seen == {
        "privateCertificate": _PRIV_CERT_REF,
        "publicCertificate": _PUB_CERT_REF,
    }


def test_certificate_refs_combined_with_pooling():
    """User direction: cert refs are independent of every other option,
    including pooling. They must coexist cleanly."""
    xml = _build_minimal_none(
        private_certificate_ref=_PRIV_CERT_REF,
        public_certificate_ref=_PUB_CERT_REF,
        connection_pooling={"enabled": True, "max_total": 20, "idle_timeout_seconds": 30},
    )
    assert _field_value(xml, "enableConnectionPooling") == "true"
    assert _field_value(xml, "maxTotal") == "20"
    # Cert refs still emitted.
    root = ET.fromstring(xml)
    for field in root.find("bns:object/GenericConnectionConfig", NS):
        if field.tag == "field" and field.attrib.get("id") == "privateCertificate":
            assert field.attrib.get("value") == _PRIV_CERT_REF
            return
    raise AssertionError("privateCertificate field not found")


def test_private_certificate_ref_must_be_string():
    cfg = _minimal_none_config(private_certificate_ref=12345)
    with pytest.raises(BuilderValidationError) as excinfo:
        RestClientConnectionBuilder().build(**cfg)
    err = excinfo.value
    assert err.error_code == "REST_CONNECTOR_VALIDATION_FAILED"
    assert err.field == "private_certificate_ref"


def test_public_certificate_ref_must_be_string():
    cfg = _minimal_none_config(public_certificate_ref=["not", "a", "string"])
    with pytest.raises(BuilderValidationError) as excinfo:
        RestClientConnectionBuilder().build(**cfg)
    err = excinfo.value
    assert err.error_code == "REST_CONNECTOR_VALIDATION_FAILED"
    assert err.field == "public_certificate_ref"


# ----------------------------------------------------------------------------
# Codex review round 3 P2 #2: cert refs must match Boomi UUID component-id
# shape. The previous validator only checked `isinstance(value, str)`, so a
# caller accidentally passing PEM/private-key content as a string would
# emit the key material into the XML and the plan echo.
# ----------------------------------------------------------------------------


@pytest.mark.parametrize(
    "pem_or_key_value",
    [
        "-----BEGIN PRIVATE KEY-----\nMIIEvQIBADANBgkqhkiG...",
        "-----BEGIN RSA PRIVATE KEY-----\nMIIEowIBAAKCAQEA...",
        "-----BEGIN CERTIFICATE-----\nMIIDXTCCAkWgAwIBAgIJ...",
        "-----BEGIN OPENSSH PRIVATE KEY-----\nb3BlbnNzaC1rZXktdjE...",
        "ssh-rsa AAAAB3NzaC1yc2EAAAADAQABAAABAQ user@host",
        "ssh-ed25519 AAAAC3NzaC1lZDI1NTE5 user@host",
    ],
)
def test_codex_round3_cert_ref_pem_content_rejected(pem_or_key_value):
    """PEM-headed or SSH-key content must NOT be accepted as a Boomi
    certificate component id. Reject before emission."""
    cfg = _minimal_none_config(private_certificate_ref=pem_or_key_value)
    with pytest.raises(BuilderValidationError) as excinfo:
        RestClientConnectionBuilder().build(**cfg)
    err = excinfo.value
    assert err.error_code == "REST_CONNECTOR_VALIDATION_FAILED"
    assert err.field == "private_certificate_ref"


@pytest.mark.parametrize(
    "non_guid_string",
    [
        "not-a-guid",
        "abc",
        "21f598a6",  # truncated GUID
        "21f598a6-1d90-4578-a35a",  # truncated GUID
        "21f598a6_1d90_4578_a35a_d0350c50b747",  # underscores not hyphens
        "21f598a6-1d90-4578-a35a-d0350c50b747-extra",  # extra trailing
        "zzzzzzzz-zzzz-zzzz-zzzz-zzzzzzzzzzzz",  # non-hex chars
    ],
)
def test_codex_round3_cert_ref_non_guid_string_rejected(non_guid_string):
    """Boomi certificate component IDs are UUIDs. Anything that doesn't
    match the canonical 8-4-4-4-12 hex shape must be rejected."""
    cfg = _minimal_none_config(private_certificate_ref=non_guid_string)
    with pytest.raises(BuilderValidationError) as excinfo:
        RestClientConnectionBuilder().build(**cfg)
    err = excinfo.value
    assert err.error_code == "REST_CONNECTOR_VALIDATION_FAILED"
    assert err.field == "private_certificate_ref"


@pytest.mark.parametrize(
    "valid_guid",
    [
        "21f598a6-1d90-4578-a35a-d0350c50b747",
        "ea82aa0c-484b-40b1-890c-f142ab8fecad",
        # Uppercase hex should also be accepted (UUIDs are case-insensitive).
        "21F598A6-1D90-4578-A35A-D0350C50B747",
    ],
)
def test_codex_round3_cert_ref_valid_guid_still_accepted(valid_guid):
    """Regression sanity: valid Boomi component-id GUIDs must continue to
    pass cert ref validation."""
    xml = _build_minimal_none(private_certificate_ref=valid_guid)
    root = ET.fromstring(xml)
    for field in root.find("bns:object/GenericConnectionConfig", NS):
        if field.tag == "field" and field.attrib.get("id") == "privateCertificate":
            assert field.attrib.get("value") == valid_guid
            return
    raise AssertionError("privateCertificate field not found")


def test_codex_round3_public_cert_ref_also_validated():
    """Public-cert ref must enforce the same shape as private-cert ref."""
    cfg = _minimal_none_config(
        public_certificate_ref="-----BEGIN CERTIFICATE-----\nMIIDXTCCAkWgAwIBAgIJ...",
    )
    with pytest.raises(BuilderValidationError) as excinfo:
        RestClientConnectionBuilder().build(**cfg)
    err = excinfo.value
    assert err.error_code == "REST_CONNECTOR_VALIDATION_FAILED"
    assert err.field == "public_certificate_ref"


def test_codex_round3_cert_ref_pem_canary_does_not_leak():
    """If the caller pastes PEM content (often containing the literal key
    material), the rejection MUST NOT echo the key bytes into the error
    envelope. The hint may reference what shape is expected but not
    repeat the offending value."""
    cfg = _minimal_none_config(
        private_certificate_ref=(
            "-----BEGIN PRIVATE KEY-----\n"
            "PEMCANARY_THIS_IS_PRIVATE_KEY_MATERIAL_DEADBEEF\n"
            "-----END PRIVATE KEY-----"
        ),
    )
    with pytest.raises(BuilderValidationError) as excinfo:
        RestClientConnectionBuilder().build(**cfg)
    err = excinfo.value
    assert "PEMCANARY_THIS_IS_PRIVATE_KEY_MATERIAL_DEADBEEF" not in str(err)
    assert "PEMCANARY_THIS_IS_PRIVATE_KEY_MATERIAL_DEADBEEF" not in (err.hint or "")


def test_empty_certificate_refs_treated_as_omitted():
    """Empty string refs == not supplied → emit self-closing cert fields
    (matching live REST None shape)."""
    xml = _build_minimal_none(private_certificate_ref="", public_certificate_ref="")
    root = ET.fromstring(xml)
    for field in root.find("bns:object/GenericConnectionConfig", NS):
        if field.tag == "field" and field.attrib.get("id") in ("privateCertificate", "publicCertificate"):
            assert "value" not in field.attrib, (
                f"Empty cert ref must emit self-closing field, got attribs: {field.attrib}"
            )


# ----------------------------------------------------------------------------
# BASIC auth (Phase 2 — issue #24 follow-up).
# Live shape verified against:
#   - 587b5fe0-eafc-48fa-9ccf-05dd70855f80 (REST Basic — username + encrypted
#     password + preemptive=false; auth=BASIC)
# ----------------------------------------------------------------------------


def _minimal_basic_config(**overrides):
    """Minimal-valid REST BASIC auth config dict."""
    params = {
        "connector_type": "rest",
        "component_name": "Target REST BASIC Connection",
        "base_url": "https://api.example.com",
        "auth": "BASIC",
        "username": "boomi-user",
        "credential_ref": "credential://target-api/basic-password",
    }
    params.update(overrides)
    return params


def _build_minimal_basic(**overrides):
    params = _minimal_basic_config(**overrides)
    params.pop("connector_type", None)
    return RestClientConnectionBuilder().build(**params)


def test_basic_auth_minimum_required_fields_produce_valid_xml():
    xml = _build_minimal_basic()
    assert _field_value(xml, "auth") == "BASIC"
    assert _field_value(xml, "username") == "boomi-user"


def test_basic_auth_validate_config_returns_none():
    assert RestClientConnectionBuilder.validate_config(_minimal_basic_config()) is None


def test_basic_auth_password_field_emitted_empty():
    """Builder never writes plaintext password into XML. The password field
    is emitted with value='' and the secret is supplied later via the
    Boomi UI (or pre-encrypted raw-XML payload)."""
    xml = _build_minimal_basic()
    assert _field_value(xml, "password") == ""


def test_basic_auth_emits_password_encrypted_values_marker():
    """Live REST Basic: `<bns:encryptedValues><bns:encryptedValue
    path="//GenericConnectionConfig/field[@type='password']" isSet="true"/></bns:encryptedValues>`.
    Builder emits the same xpath but with isSet=false (a brand-new component
    has no secret stored yet — Boomi flips isSet to true when the value is
    saved in the UI)."""
    xml = _build_minimal_basic()
    root = ET.fromstring(xml)
    encrypted_values = root.find("bns:encryptedValues", NS)
    assert encrypted_values is not None
    entries = encrypted_values.findall("bns:encryptedValue", NS)
    assert len(entries) == 1
    assert entries[0].attrib["path"] == "//GenericConnectionConfig/field[@type='password']"
    assert entries[0].attrib["isSet"] == "false"


def test_basic_auth_preemptive_defaults_to_explicit_false():
    """Live REST Basic emits preemptive value='false' (NOT empty). For BASIC,
    the default is explicit false — preemptive is relevant for BASIC auth
    per Boomi docs."""
    xml = _build_minimal_basic()
    assert _field_value(xml, "preemptive") == "false"


def test_basic_auth_preemptive_can_be_overridden_true():
    xml = _build_minimal_basic(preemptive=True)
    assert _field_value(xml, "preemptive") == "true"


def test_basic_auth_oauth2_skeleton_with_grant_type_code():
    """Non-OAUTH2 modes (BASIC) emit the OAuth2Config skeleton with
    grantType='code', same as NONE."""
    oa = _oauth2_config(_build_minimal_basic())
    assert oa.attrib["grantType"] == "code"
    assert oa.find("credentialsAssertionType") is None


def test_basic_auth_domain_and_workstation_emitted_empty():
    """Live REST Basic: domain and workstation fields are emitted with
    value='' (NTLM-only fields stay empty for BASIC)."""
    xml = _build_minimal_basic()
    assert _field_value(xml, "domain") == ""
    assert _field_value(xml, "workstation") == ""


def test_basic_auth_with_cert_refs():
    """Cert refs work with BASIC auth too (independent option per user
    direction 'cert refs cound be with other auth too')."""
    xml = _build_minimal_basic(
        private_certificate_ref=_PRIV_CERT_REF,
        public_certificate_ref=_PUB_CERT_REF,
    )
    root = ET.fromstring(xml)
    seen = {}
    for field in root.find("bns:object/GenericConnectionConfig", NS):
        if field.tag == "field" and field.attrib.get("id") in ("privateCertificate", "publicCertificate"):
            seen[field.attrib["id"]] = field.attrib.get("value")
    assert seen == {
        "privateCertificate": _PRIV_CERT_REF,
        "publicCertificate": _PUB_CERT_REF,
    }


def test_basic_auth_missing_username_rejected():
    cfg = _minimal_basic_config()
    cfg["username"] = ""
    with pytest.raises(BuilderValidationError) as excinfo:
        RestClientConnectionBuilder().build(**cfg)
    err = excinfo.value
    assert err.error_code == "REST_CONNECTOR_VALIDATION_FAILED"
    assert err.field == "username"


def test_basic_auth_username_must_be_string():
    cfg = _minimal_basic_config(username=12345)
    with pytest.raises(BuilderValidationError) as excinfo:
        RestClientConnectionBuilder().build(**cfg)
    err = excinfo.value
    assert err.error_code == "REST_CONNECTOR_VALIDATION_FAILED"
    assert err.field == "username"


def test_basic_auth_missing_credential_ref_rejected():
    """BASIC password is supplied as an opaque credential_ref (consistent
    with database connector convention). Missing/empty credential_ref must
    fail validation explicitly."""
    cfg = _minimal_basic_config()
    cfg["credential_ref"] = ""
    with pytest.raises(BuilderValidationError) as excinfo:
        RestClientConnectionBuilder().build(**cfg)
    err = excinfo.value
    assert err.error_code == "REST_CONNECTOR_VALIDATION_FAILED"
    assert err.field == "credential_ref"


def test_basic_auth_credential_ref_must_use_credential_scheme():
    """A raw secret value passed as credential_ref must be rejected — the
    builder never accepts plaintext secrets, only opaque credential://
    references."""
    cfg = _minimal_basic_config(credential_ref="raw-password-value")
    with pytest.raises(BuilderValidationError) as excinfo:
        RestClientConnectionBuilder().build(**cfg)
    err = excinfo.value
    assert err.error_code == "REST_SECRET_VALUE_FORBIDDEN"
    assert err.field == "credential_ref"


def test_basic_auth_rejects_plaintext_password_field():
    """The forbidden-secret-fields scan already catches top-level `password`,
    but cover the BASIC path explicitly — verify the canary doesn't leak."""
    cfg = _minimal_basic_config(password="DEADBEEF_BASIC_PASSWORD")
    with pytest.raises(BuilderValidationError) as excinfo:
        RestClientConnectionBuilder().build(**cfg)
    err = excinfo.value
    assert err.error_code in ("PLAINTEXT_SECRET_REJECTED", "REST_SECRET_VALUE_FORBIDDEN")
    assert "DEADBEEF_BASIC_PASSWORD" not in str(err)
    assert "DEADBEEF_BASIC_PASSWORD" not in (err.hint or "")


# ----------------------------------------------------------------------------
# NTLM auth (Phase 3 — issue #24 follow-up).
# Live shape verified against:
#   - 1de43085-6d58-41dc-8d3c-03504df86b91 (REST NTLM — username + encrypted
#     password + domain + workstation; preemptive empty, NOT explicit false)
# ----------------------------------------------------------------------------


def _minimal_ntlm_config(**overrides):
    """Minimal-valid REST NTLM auth config dict."""
    params = {
        "connector_type": "rest",
        "component_name": "Target REST NTLM Connection",
        "base_url": "https://api.example.com",
        "auth": "NTLM",
        "username": "login",
        "credential_ref": "credential://target-api/ntlm-password",
        "domain": "corp.example.com",
        "workstation": "WORKSTATION1",
    }
    params.update(overrides)
    return params


def _build_minimal_ntlm(**overrides):
    params = _minimal_ntlm_config(**overrides)
    params.pop("connector_type", None)
    return RestClientConnectionBuilder().build(**params)


def test_ntlm_auth_minimum_required_fields_produce_valid_xml():
    xml = _build_minimal_ntlm()
    assert _field_value(xml, "auth") == "NTLM"
    assert _field_value(xml, "username") == "login"
    assert _field_value(xml, "domain") == "corp.example.com"
    assert _field_value(xml, "workstation") == "WORKSTATION1"


def test_ntlm_auth_validate_config_returns_none():
    assert RestClientConnectionBuilder.validate_config(_minimal_ntlm_config()) is None


def test_ntlm_auth_password_field_emitted_empty():
    xml = _build_minimal_ntlm()
    assert _field_value(xml, "password") == ""


def test_ntlm_auth_emits_password_encrypted_values_marker():
    """NTLM uses the same `//GenericConnectionConfig/field[@type='password']`
    xpath as BASIC. Verified against live REST NTLM (1de43085)."""
    xml = _build_minimal_ntlm()
    root = ET.fromstring(xml)
    encrypted_values = root.find("bns:encryptedValues", NS)
    assert encrypted_values is not None
    entries = encrypted_values.findall("bns:encryptedValue", NS)
    assert len(entries) == 1
    assert entries[0].attrib["path"] == "//GenericConnectionConfig/field[@type='password']"
    assert entries[0].attrib["isSet"] == "false"


def test_ntlm_auth_preemptive_emitted_empty():
    """Live REST NTLM (1de43085) emits preemptive value='' (empty), NOT
    value='false'. Preemptive is irrelevant for NTLM per Boomi docs. Match
    the live shape rather than emitting an inert false."""
    xml = _build_minimal_ntlm()
    assert _field_value(xml, "preemptive") == ""


def test_ntlm_auth_oauth2_skeleton_with_grant_type_code():
    """Non-OAUTH2 modes (NTLM) emit the OAuth2Config skeleton with
    grantType='code', same as NONE and BASIC."""
    oa = _oauth2_config(_build_minimal_ntlm())
    assert oa.attrib["grantType"] == "code"
    assert oa.find("credentialsAssertionType") is None


def test_ntlm_auth_missing_domain_rejected():
    cfg = _minimal_ntlm_config()
    cfg["domain"] = ""
    with pytest.raises(BuilderValidationError) as excinfo:
        RestClientConnectionBuilder().build(**cfg)
    err = excinfo.value
    assert err.error_code == "REST_CONNECTOR_VALIDATION_FAILED"
    assert err.field == "domain"


def test_ntlm_auth_missing_workstation_rejected():
    cfg = _minimal_ntlm_config()
    cfg["workstation"] = ""
    with pytest.raises(BuilderValidationError) as excinfo:
        RestClientConnectionBuilder().build(**cfg)
    err = excinfo.value
    assert err.error_code == "REST_CONNECTOR_VALIDATION_FAILED"
    assert err.field == "workstation"


def test_ntlm_auth_domain_must_be_string():
    cfg = _minimal_ntlm_config(domain=12345)
    with pytest.raises(BuilderValidationError) as excinfo:
        RestClientConnectionBuilder().build(**cfg)
    err = excinfo.value
    assert err.error_code == "REST_CONNECTOR_VALIDATION_FAILED"
    assert err.field == "domain"


def test_ntlm_auth_workstation_must_be_string():
    cfg = _minimal_ntlm_config(workstation=["nope"])
    with pytest.raises(BuilderValidationError) as excinfo:
        RestClientConnectionBuilder().build(**cfg)
    err = excinfo.value
    assert err.error_code == "REST_CONNECTOR_VALIDATION_FAILED"
    assert err.field == "workstation"


def test_ntlm_auth_missing_username_rejected():
    """NTLM, like BASIC, requires username via the shared password-backed
    auth gate. Confirm the gate fires for NTLM too."""
    cfg = _minimal_ntlm_config()
    cfg["username"] = ""
    with pytest.raises(BuilderValidationError) as excinfo:
        RestClientConnectionBuilder().build(**cfg)
    err = excinfo.value
    assert err.error_code == "REST_CONNECTOR_VALIDATION_FAILED"
    assert err.field == "username"


def test_ntlm_auth_missing_credential_ref_rejected():
    cfg = _minimal_ntlm_config()
    cfg["credential_ref"] = ""
    with pytest.raises(BuilderValidationError) as excinfo:
        RestClientConnectionBuilder().build(**cfg)
    err = excinfo.value
    assert err.error_code == "REST_CONNECTOR_VALIDATION_FAILED"
    assert err.field == "credential_ref"


def test_ntlm_auth_credential_ref_must_use_credential_scheme():
    cfg = _minimal_ntlm_config(credential_ref="raw-secret")
    with pytest.raises(BuilderValidationError) as excinfo:
        RestClientConnectionBuilder().build(**cfg)
    err = excinfo.value
    assert err.error_code == "REST_SECRET_VALUE_FORBIDDEN"
    assert err.field == "credential_ref"


def test_ntlm_auth_with_cert_refs():
    """Cert refs work with NTLM too."""
    xml = _build_minimal_ntlm(
        private_certificate_ref=_PRIV_CERT_REF,
        public_certificate_ref=_PUB_CERT_REF,
    )
    root = ET.fromstring(xml)
    seen = {}
    for field in root.find("bns:object/GenericConnectionConfig", NS):
        if field.tag == "field" and field.attrib.get("id") in ("privateCertificate", "publicCertificate"):
            seen[field.attrib["id"]] = field.attrib.get("value")
    assert seen == {
        "privateCertificate": _PRIV_CERT_REF,
        "publicCertificate": _PUB_CERT_REF,
    }


def test_ntlm_auth_rejects_plaintext_password_field():
    cfg = _minimal_ntlm_config(password="DEADBEEF_NTLM_PASSWORD")
    with pytest.raises(BuilderValidationError) as excinfo:
        RestClientConnectionBuilder().build(**cfg)
    err = excinfo.value
    assert err.error_code in ("PLAINTEXT_SECRET_REJECTED", "REST_SECRET_VALUE_FORBIDDEN")
    assert "DEADBEEF_NTLM_PASSWORD" not in str(err)
    assert "DEADBEEF_NTLM_PASSWORD" not in (err.hint or "")


# ----------------------------------------------------------------------------
# OAuth2 authorization_code (token-not-set) — Phase 4 of issue #24 follow-up.
# Live shape verified against:
#   - 7abf0ad2-ed87-4226-bdc9-a19fd59d8967 (REST Auth Code Token Not Set —
#     auth=OAUTH2, grantType=code, populated authorizationTokenEndpoint and
#     accessTokenEndpoint, scope populated, NO cached accessToken attribute,
#     NO credentialsAssertionType element)
# ----------------------------------------------------------------------------


def _minimal_oauth2_authcode_config(**overrides):
    """Minimal-valid REST OAUTH2 authorization_code (token-not-set) config."""
    params = {
        "connector_type": "rest",
        "component_name": "Target REST OAuth2 AuthCode Connection",
        "base_url": "https://api.example.com",
        "auth": "OAUTH2",
        "oauth2": {
            "grant_type": "authorization_code",
            "client_id": "boomi-client",
            "client_secret_ref": "credential://target-api/oauth-client-secret",
            "authorization_url": "https://api.example.com/oauth/authorize",
            "access_token_url": "https://api.example.com/oauth/token",
            "scope": "cds.read cds.write",
        },
    }
    params.update(overrides)
    return params


def _build_minimal_oauth2_authcode(**overrides):
    params = _minimal_oauth2_authcode_config(**overrides)
    params.pop("connector_type", None)
    return RestClientConnectionBuilder().build(**params)


def test_oauth2_authcode_minimum_required_fields_produce_valid_xml():
    xml = _build_minimal_oauth2_authcode()
    assert _field_value(xml, "auth") == "OAUTH2"
    oa = _oauth2_config(xml)
    assert oa.attrib["grantType"] == "code"


def test_oauth2_authcode_validate_config_returns_none():
    assert RestClientConnectionBuilder.validate_config(
        _minimal_oauth2_authcode_config()
    ) is None


def test_oauth2_authcode_grant_type_alias_code_accepted():
    """`code` and `authorization_code` are equivalent input aliases for the
    same grant — both must validate and emit grantType='code'."""
    cfg = _minimal_oauth2_authcode_config()
    cfg["oauth2"]["grant_type"] = "code"
    xml = RestClientConnectionBuilder().build(
        **{k: v for k, v in cfg.items() if k != "connector_type"}
    )
    oa = _oauth2_config(xml)
    assert oa.attrib["grantType"] == "code"


def test_oauth2_authcode_authorization_endpoint_url_populated():
    """authorization_code grant populates the authorizationTokenEndpoint url
    (vs client_credentials which emits it empty). Verified against live
    REST Auth Code Token Not Set."""
    oa = _oauth2_config(_build_minimal_oauth2_authcode())
    ate = oa.find("authorizationTokenEndpoint")
    assert ate is not None
    assert ate.attrib["url"] == "https://api.example.com/oauth/authorize"
    assert ate.find("sslOptions") is not None


def test_oauth2_authcode_access_token_endpoint_url_populated():
    oa = _oauth2_config(_build_minimal_oauth2_authcode())
    ate = oa.find("accessTokenEndpoint")
    assert ate is not None
    assert ate.attrib["url"] == "https://api.example.com/oauth/token"


def test_oauth2_authcode_scope_populated():
    oa = _oauth2_config(_build_minimal_oauth2_authcode())
    scope = oa.find("scope")
    assert scope is not None
    assert (scope.text or "") == "cds.read cds.write"


def test_oauth2_authcode_credentials_no_access_token_key():
    """Live REST Auth Code Token Not Set: credentials element has clientId +
    clientSecret only — NO accessTokenKey attribute (that's client_credentials
    only)."""
    oa = _oauth2_config(_build_minimal_oauth2_authcode())
    creds = oa.find("credentials")
    assert creds is not None
    assert "accessTokenKey" not in creds.attrib


def test_oauth2_authcode_credentials_no_cached_access_token():
    """Builder must NEVER emit the cached `accessToken` attribute on
    credentials, even if the live source export carried one. Live
    'REST Auth Code Token Not Set' has none; live 'REST Auth Code' DOES
    have it — but neither path should ever leak from the builder."""
    oa = _oauth2_config(_build_minimal_oauth2_authcode())
    creds = oa.find("credentials")
    assert creds is not None
    assert "accessToken" not in creds.attrib


def test_oauth2_authcode_no_credentials_assertion_type_element():
    """Live REST Auth Code Token Not Set has NO credentialsAssertionType
    element (that's client_credentials only)."""
    oa = _oauth2_config(_build_minimal_oauth2_authcode())
    assert oa.find("credentialsAssertionType") is None


def test_oauth2_authcode_emits_clientsecret_encrypted_values_marker():
    """OAuth2 authorization_code uses the same client-secret xpath as
    client_credentials."""
    xml = _build_minimal_oauth2_authcode()
    root = ET.fromstring(xml)
    encrypted_values = root.find("bns:encryptedValues", NS)
    assert encrypted_values is not None
    entries = encrypted_values.findall("bns:encryptedValue", NS)
    assert len(entries) == 1
    assert entries[0].attrib["path"] == "//GenericConnectionConfig/field/OAuth2Config/credentials/@clientSecret"


def test_oauth2_authcode_preemptive_default_false():
    """Like client_credentials, OAUTH2 authorization_code emits an explicit
    preemptive value (default false). Verified against live REST Auth Code
    Token Not Set (preemptive='false' explicit)."""
    xml = _build_minimal_oauth2_authcode()
    assert _field_value(xml, "preemptive") == "false"


def test_oauth2_authcode_missing_authorization_url_rejected():
    cfg = _minimal_oauth2_authcode_config()
    cfg["oauth2"]["authorization_url"] = ""
    with pytest.raises(BuilderValidationError) as excinfo:
        RestClientConnectionBuilder().build(**cfg)
    err = excinfo.value
    assert err.error_code == "REST_CONNECTOR_VALIDATION_FAILED"
    assert err.field == "oauth2.authorization_url"


def test_oauth2_authcode_missing_access_token_url_rejected():
    cfg = _minimal_oauth2_authcode_config()
    cfg["oauth2"]["access_token_url"] = ""
    with pytest.raises(BuilderValidationError) as excinfo:
        RestClientConnectionBuilder().build(**cfg)
    err = excinfo.value
    assert err.error_code == "REST_CONNECTOR_VALIDATION_FAILED"
    assert err.field == "oauth2.access_token_url"


def test_oauth2_authcode_missing_client_id_rejected():
    cfg = _minimal_oauth2_authcode_config()
    cfg["oauth2"]["client_id"] = ""
    with pytest.raises(BuilderValidationError) as excinfo:
        RestClientConnectionBuilder().build(**cfg)
    err = excinfo.value
    assert err.error_code == "REST_CONNECTOR_VALIDATION_FAILED"
    assert err.field == "oauth2.client_id"


def test_oauth2_authcode_client_secret_ref_must_use_credential_scheme():
    cfg = _minimal_oauth2_authcode_config()
    cfg["oauth2"]["client_secret_ref"] = "raw-secret"
    with pytest.raises(BuilderValidationError) as excinfo:
        RestClientConnectionBuilder().build(**cfg)
    err = excinfo.value
    assert err.error_code == "REST_SECRET_VALUE_FORBIDDEN"
    assert err.field == "oauth2.client_secret_ref"


def test_oauth2_authcode_cached_access_token_field_rejected():
    """If the caller forwards a cached `oauth2.access_token` value (e.g.
    copied from a Boomi export), reject it. We do NOT accept or emit
    cached OAuth tokens — users must authorize in the Boomi UI after create."""
    cfg = _minimal_oauth2_authcode_config()
    cfg["oauth2"]["access_token"] = "DEADBEEF_CACHED_TOKEN"
    with pytest.raises(BuilderValidationError) as excinfo:
        RestClientConnectionBuilder().build(**cfg)
    err = excinfo.value
    # The forbidden-secret scan catches `access_token` as a top-level
    # forbidden key (it's in FORBIDDEN_SECRET_FIELDS, scanned recursively
    # under the oauth2 sub-dict).
    assert err.error_code in ("PLAINTEXT_SECRET_REJECTED", "REST_SECRET_VALUE_FORBIDDEN")
    assert "DEADBEEF_CACHED_TOKEN" not in str(err)
    assert "DEADBEEF_CACHED_TOKEN" not in (err.hint or "")


def test_oauth2_authcode_client_credentials_path_still_works():
    """Regression: adding authorization_code must not break the existing
    client_credentials path (which still emits credentialsAssertionType
    and a populated accessTokenEndpoint with empty authorizationTokenEndpoint)."""
    xml = _build_minimal()  # client_credentials happy path
    oa = _oauth2_config(xml)
    assert oa.attrib["grantType"] == "client_credentials"
    assert oa.find("credentialsAssertionType") is not None
    # authorizationTokenEndpoint url is empty for client_credentials.
    ate = oa.find("authorizationTokenEndpoint")
    assert ate.attrib["url"] == ""


def test_oauth2_authcode_with_cert_refs():
    """Cert refs work with OAUTH2 authorization_code too."""
    xml = _build_minimal_oauth2_authcode(
        private_certificate_ref=_PRIV_CERT_REF,
        public_certificate_ref=_PUB_CERT_REF,
    )
    root = ET.fromstring(xml)
    seen = {}
    for field in root.find("bns:object/GenericConnectionConfig", NS):
        if field.tag == "field" and field.attrib.get("id") in ("privateCertificate", "publicCertificate"):
            seen[field.attrib["id"]] = field.attrib.get("value")
    assert seen == {
        "privateCertificate": _PRIV_CERT_REF,
        "publicCertificate": _PUB_CERT_REF,
    }


# ----------------------------------------------------------------------------
# Codex review round 1 — stale-block validation gates.
# Before the round-1 fix, a stale `oauth2` block or `credential_ref` could
# survive validation when the auth mode no longer used it, and the raw value
# would echo through the plan output (the redaction sweep only fires AFTER
# a REST validation error). These tests lock the post-fix behavior.
# ----------------------------------------------------------------------------


@pytest.mark.parametrize("non_oauth2_auth", ["NONE", "BASIC", "NTLM"])
def test_stale_oauth2_block_rejected_for_non_oauth2_auth(non_oauth2_auth):
    """`auth='NONE'` plus a stale `oauth2={...}` block is always a config
    mistake (typo'd auth, or stale field left over from an OAUTH2 → other
    transition). Reject before the raw secret reaches the plan echo."""
    cfg = {
        "connector_type": "rest",
        "component_name": "Stale OAuth2 block",
        "base_url": "https://api.example.com",
        "auth": non_oauth2_auth,
        # BASIC/NTLM require username + credential_ref — supply them so
        # the validator doesn't bail on the password-backed gate before
        # reaching the oauth2 check.
        "username": "u",
        "credential_ref": "credential://x/y",
        "domain": "d",
        "workstation": "w",
        "oauth2": {
            "grant_type": "client_credentials",
            "client_id": "id",
            # Raw secret value (must NOT leak via plan echo).
            "client_secret_ref": "raw-secret-LEAK-CANARY-DEADBEEF",
            "access_token_url": "https://api.example.com/token",
        },
    }
    with pytest.raises(BuilderValidationError) as excinfo:
        RestClientConnectionBuilder().build(**{k: v for k, v in cfg.items() if k != "connector_type"})
    err = excinfo.value
    assert err.error_code == "REST_CONNECTOR_VALIDATION_FAILED"
    assert err.field == "oauth2"
    assert "LEAK-CANARY-DEADBEEF" not in str(err)
    assert "LEAK-CANARY-DEADBEEF" not in (err.hint or "")


def test_empty_oauth2_block_accepted_for_non_oauth2_auth():
    """An EMPTY oauth2 dict for a non-OAUTH2 auth is treated as
    "no oauth2 block supplied" — accept it. Only non-empty stale blocks
    are rejected. Edge case for callers that always emit the key with `{}`
    regardless of auth."""
    cfg = _minimal_none_config()
    cfg["oauth2"] = {}
    err = RestClientConnectionBuilder.validate_config(cfg)
    assert err is None


@pytest.mark.parametrize("non_password_auth", ["NONE", "OAUTH2"])
def test_stale_credential_ref_rejected_for_non_password_auth(non_password_auth):
    """`credential_ref` is BASIC/NTLM-only. With NONE/OAUTH2 supplying a
    `credential_ref` is a config mistake — reject before the raw value
    reaches the plan echo."""
    if non_password_auth == "OAUTH2":
        cfg = _minimal_oauth2_config()
        cfg["credential_ref"] = "raw-secret-LEAK-CANARY-DEADBEEF"
    else:
        cfg = _minimal_none_config(credential_ref="raw-secret-LEAK-CANARY-DEADBEEF")
    with pytest.raises(BuilderValidationError) as excinfo:
        RestClientConnectionBuilder().build(**{k: v for k, v in cfg.items() if k != "connector_type"})
    err = excinfo.value
    assert err.error_code == "REST_CONNECTOR_VALIDATION_FAILED"
    assert err.field == "credential_ref"
    assert "LEAK-CANARY-DEADBEEF" not in str(err)
    assert "LEAK-CANARY-DEADBEEF" not in (err.hint or "")


def test_empty_credential_ref_accepted_for_non_password_auth():
    """Empty-string credential_ref on a non-password auth is treated as
    "not supplied" — accept it (edge case)."""
    cfg = _minimal_none_config(credential_ref="")
    assert RestClientConnectionBuilder.validate_config(cfg) is None


@pytest.mark.parametrize(
    "unhashable_grant",
    [[], {}, ["client_credentials"], {"key": "value"}],
)
def test_grant_type_unhashable_value_returns_structured_error(unhashable_grant):
    """Codex review P2 #3: unhashable grant_type (list, dict, etc.) used to
    crash `cls._OAUTH2_GRANT_TYPE_ALIASES.get(grant_input)` with TypeError
    because dict lookups require hashable keys. Must return a structured
    UNSUPPORTED_REST_AUTH_MODE error instead of crashing."""
    cfg = _minimal_oauth2_config()
    cfg["oauth2"]["grant_type"] = unhashable_grant
    with pytest.raises(BuilderValidationError) as excinfo:
        RestClientConnectionBuilder().build(**{k: v for k, v in cfg.items() if k != "connector_type"})
    err = excinfo.value
    assert err.error_code == "UNSUPPORTED_REST_AUTH_MODE"
    assert err.field == "oauth2.grant_type"


def test_grant_type_integer_value_returns_structured_error():
    """Scalar non-string flavor. 123 IS hashable (so wouldn't TypeError),
    but it's still not a valid grant_type — must surface as
    UNSUPPORTED_REST_AUTH_MODE."""
    cfg = _minimal_oauth2_config()
    cfg["oauth2"]["grant_type"] = 12345
    with pytest.raises(BuilderValidationError) as excinfo:
        RestClientConnectionBuilder().build(**{k: v for k, v in cfg.items() if k != "connector_type"})
    err = excinfo.value
    assert err.error_code == "UNSUPPORTED_REST_AUTH_MODE"
    assert err.field == "oauth2.grant_type"


# ----------------------------------------------------------------------------
# Codex review round 2 — non-dict/non-string stale value gaps.
# Round-1 gates only fired for `oauth2` being a non-empty DICT and for
# `credential_ref` being a non-empty STRING. A malformed config that
# leaves `oauth2` as a string/list, or `credential_ref` as a dict/list,
# bypassed validation and the raw value would echo through the plan
# output. Round-2: reject ANY truthy stale value for those auths,
# regardless of type.
# ----------------------------------------------------------------------------


@pytest.mark.parametrize(
    "stale_value",
    [
        "raw-secret-LEAK_CANARY_R2_OAUTH2",
        ["raw-secret-LEAK_CANARY_R2_OAUTH2"],
        12345,
        True,
    ],
)
def test_stale_oauth2_non_dict_value_rejected_for_non_oauth2_auth(stale_value):
    """Round-2 P2 #1: a non-dict `oauth2` value with a non-OAUTH2 auth
    must also be rejected. Previously only non-empty DICT triggered the
    gate; non-dict truthy values slipped through and could echo their
    raw payload in the plan output."""
    cfg = _minimal_none_config()
    cfg["oauth2"] = stale_value
    with pytest.raises(BuilderValidationError) as excinfo:
        RestClientConnectionBuilder().build(**{k: v for k, v in cfg.items() if k != "connector_type"})
    err = excinfo.value
    assert err.error_code == "REST_CONNECTOR_VALIDATION_FAILED"
    assert err.field == "oauth2"
    # Canary must NOT leak into the error envelope.
    assert "LEAK_CANARY_R2_OAUTH2" not in str(err)
    assert "LEAK_CANARY_R2_OAUTH2" not in (err.hint or "")


@pytest.mark.parametrize(
    "stale_value",
    [
        ["raw-secret-LEAK_CANARY_R2_CREDREF"],
        {"value": "raw-secret-LEAK_CANARY_R2_CREDREF"},
        12345,
        True,
    ],
)
def test_stale_credential_ref_non_string_value_rejected_for_non_password_auth(stale_value):
    """Round-2 P2 #2: a non-string `credential_ref` value with a
    non-password auth (NONE/OAUTH2) must also be rejected. Previously
    only non-empty STRING triggered the gate; non-string truthy values
    slipped through and could echo their raw payload via the plan
    redaction sweep, which only runs after a REST validation error."""
    cfg = _minimal_none_config()
    cfg["credential_ref"] = stale_value
    with pytest.raises(BuilderValidationError) as excinfo:
        RestClientConnectionBuilder().build(**{k: v for k, v in cfg.items() if k != "connector_type"})
    err = excinfo.value
    assert err.error_code == "REST_CONNECTOR_VALIDATION_FAILED"
    assert err.field == "credential_ref"
    assert "LEAK_CANARY_R2_CREDREF" not in str(err)
    assert "LEAK_CANARY_R2_CREDREF" not in (err.hint or "")


def test_stale_oauth2_none_value_still_accepted():
    """Regression sanity: `oauth2=None` for non-OAUTH2 auth still passes
    (treated as "not supplied", same as missing key)."""
    cfg = _minimal_none_config()
    cfg["oauth2"] = None
    assert RestClientConnectionBuilder.validate_config(cfg) is None


def test_stale_credential_ref_none_value_still_accepted():
    """Regression sanity: `credential_ref=None` for non-password auth
    still passes."""
    cfg = _minimal_none_config()
    cfg["credential_ref"] = None
    assert RestClientConnectionBuilder.validate_config(cfg) is None


# ----------------------------------------------------------------------------
# Field-dependency audit — stale `username` for non-password-backed auth.
# username is required for BASIC/NTLM; supplying it with NONE/OAUTH2 is a
# stale-field config mistake (caller's intent silently emitted into XML
# where it has no semantic effect). Reject up front.
# ----------------------------------------------------------------------------


@pytest.mark.parametrize("non_password_auth", ["NONE", "OAUTH2"])
def test_stale_username_rejected_for_non_password_auth(non_password_auth):
    """`username` is BASIC/NTLM-only. With NONE/OAUTH2 supplying a non-empty
    username is a config mistake — caller's value would otherwise be emitted
    in the XML where Boomi ignores it at runtime."""
    if non_password_auth == "OAUTH2":
        cfg = _minimal_oauth2_config()
        cfg["username"] = "alice-stale"
    else:
        cfg = _minimal_none_config(username="alice-stale")
    with pytest.raises(BuilderValidationError) as excinfo:
        RestClientConnectionBuilder().build(**{k: v for k, v in cfg.items() if k != "connector_type"})
    err = excinfo.value
    assert err.error_code == "REST_CONNECTOR_VALIDATION_FAILED"
    assert err.field == "username"


def test_empty_username_accepted_for_non_password_auth():
    """Empty-string username on a non-password auth is treated as
    "not supplied" — accept it (edge case for callers that always include
    the key)."""
    cfg = _minimal_none_config(username="")
    assert RestClientConnectionBuilder.validate_config(cfg) is None


def test_whitespace_username_accepted_for_non_password_auth():
    """Whitespace-only username on non-password auth is treated as
    "not supplied" — accept it."""
    cfg = _minimal_none_config(username="   ")
    assert RestClientConnectionBuilder.validate_config(cfg) is None


def test_none_username_accepted_for_non_password_auth():
    """Regression sanity: `username=None` for non-password auth still passes."""
    cfg = _minimal_none_config()
    cfg["username"] = None
    assert RestClientConnectionBuilder.validate_config(cfg) is None


@pytest.mark.parametrize(
    "stale_value",
    [
        ["alice-stale"],
        {"value": "alice-stale"},
        12345,
        True,
    ],
)
def test_stale_username_non_string_value_rejected_for_non_password_auth(stale_value):
    """Non-string truthy `username` with non-password auth must also be
    rejected — mirrors the round-2 fix for `credential_ref`. Malformed
    payloads should never validate clean and emit into the XML."""
    cfg = _minimal_none_config()
    cfg["username"] = stale_value
    with pytest.raises(BuilderValidationError) as excinfo:
        RestClientConnectionBuilder().build(**{k: v for k, v in cfg.items() if k != "connector_type"})
    err = excinfo.value
    assert err.error_code == "REST_CONNECTOR_VALIDATION_FAILED"
    assert err.field == "username"


# ----------------------------------------------------------------------------
# Field-dependency audit — stale `domain` / `workstation` for non-NTLM auth.
# Both fields are NTLM-only per Boomi docs and live exports. Supplying them
# with any other auth is a config mistake — Boomi ignores them at runtime,
# but they would otherwise be emitted in the XML.
# ----------------------------------------------------------------------------


@pytest.mark.parametrize("non_ntlm_auth", ["NONE", "BASIC", "OAUTH2"])
def test_stale_domain_rejected_for_non_ntlm_auth(non_ntlm_auth):
    """`domain` is NTLM-only — supplying it with NONE/BASIC/OAUTH2 is
    always a config mistake. Reject up front so it doesn't get emitted."""
    if non_ntlm_auth == "OAUTH2":
        cfg = _minimal_oauth2_config()
    elif non_ntlm_auth == "BASIC":
        cfg = _minimal_basic_config()
    else:
        cfg = _minimal_none_config()
    cfg["domain"] = "MYCORP"
    with pytest.raises(BuilderValidationError) as excinfo:
        RestClientConnectionBuilder().build(**{k: v for k, v in cfg.items() if k != "connector_type"})
    err = excinfo.value
    assert err.error_code == "REST_CONNECTOR_VALIDATION_FAILED"
    assert err.field == "domain"


@pytest.mark.parametrize("non_ntlm_auth", ["NONE", "BASIC", "OAUTH2"])
def test_stale_workstation_rejected_for_non_ntlm_auth(non_ntlm_auth):
    """`workstation` is NTLM-only — same rule as `domain`."""
    if non_ntlm_auth == "OAUTH2":
        cfg = _minimal_oauth2_config()
    elif non_ntlm_auth == "BASIC":
        cfg = _minimal_basic_config()
    else:
        cfg = _minimal_none_config()
    cfg["workstation"] = "WS-01"
    with pytest.raises(BuilderValidationError) as excinfo:
        RestClientConnectionBuilder().build(**{k: v for k, v in cfg.items() if k != "connector_type"})
    err = excinfo.value
    assert err.error_code == "REST_CONNECTOR_VALIDATION_FAILED"
    assert err.field == "workstation"


@pytest.mark.parametrize("stale_field", ["domain", "workstation"])
def test_stale_ntlm_field_empty_value_accepted_for_non_ntlm_auth(stale_field):
    """Empty-string / whitespace `domain` / `workstation` is treated as
    "not supplied" — accept it (edge case for callers that always include
    the key with an empty value regardless of auth)."""
    cfg = _minimal_none_config()
    cfg[stale_field] = ""
    assert RestClientConnectionBuilder.validate_config(cfg) is None
    cfg[stale_field] = "   "
    assert RestClientConnectionBuilder.validate_config(cfg) is None


@pytest.mark.parametrize("stale_field", ["domain", "workstation"])
def test_stale_ntlm_field_none_value_accepted_for_non_ntlm_auth(stale_field):
    """Regression sanity: `domain=None` / `workstation=None` for non-NTLM
    auth still passes."""
    cfg = _minimal_none_config()
    cfg[stale_field] = None
    assert RestClientConnectionBuilder.validate_config(cfg) is None


@pytest.mark.parametrize("stale_field", ["domain", "workstation"])
@pytest.mark.parametrize(
    "stale_value",
    [
        ["MYCORP"],
        {"value": "MYCORP"},
        12345,
        True,
    ],
)
def test_stale_ntlm_field_non_string_value_rejected_for_non_ntlm_auth(stale_field, stale_value):
    """Non-string truthy `domain` / `workstation` with non-NTLM auth must
    also be rejected. Mirrors the round-2 pattern for credential_ref."""
    cfg = _minimal_none_config()
    cfg[stale_field] = stale_value
    with pytest.raises(BuilderValidationError) as excinfo:
        RestClientConnectionBuilder().build(**{k: v for k, v in cfg.items() if k != "connector_type"})
    err = excinfo.value
    assert err.error_code == "REST_CONNECTOR_VALIDATION_FAILED"
    assert err.field == stale_field


# ----------------------------------------------------------------------------
# Field-dependency audit — stale `preemptive` for auth outside (BASIC, OAUTH2).
# Boomi docs: "Applicable for Basic and OAuth 2.0 authentication." So the
# field is meaningful only when auth in (BASIC, OAUTH2). Supplying it with
# NONE / NTLM is a config mistake — Boomi ignores it at runtime.
# Note: detection uses presence-check (not truthy) because False is a
# caller-supplied value with intent (docs distinguish "selected" vs "cleared").
# ----------------------------------------------------------------------------


@pytest.mark.parametrize("non_applicable_auth", ["NONE", "NTLM"])
@pytest.mark.parametrize("preemptive_value", [True, False])
def test_stale_preemptive_rejected_for_non_applicable_auth(non_applicable_auth, preemptive_value):
    """preemptive=True OR preemptive=False with auth not in (BASIC, OAUTH2)
    is a config mistake. Both boolean values count as caller intent — only
    the absence of the key is "not supplied"."""
    if non_applicable_auth == "NTLM":
        cfg = _minimal_ntlm_config()
    else:
        cfg = _minimal_none_config()
    cfg["preemptive"] = preemptive_value
    with pytest.raises(BuilderValidationError) as excinfo:
        RestClientConnectionBuilder().build(**{k: v for k, v in cfg.items() if k != "connector_type"})
    err = excinfo.value
    assert err.error_code == "REST_CONNECTOR_VALIDATION_FAILED"
    assert err.field == "preemptive"


@pytest.mark.parametrize("applicable_auth", ["BASIC", "OAUTH2"])
@pytest.mark.parametrize("preemptive_value", [True, False])
def test_preemptive_accepted_for_applicable_auth(applicable_auth, preemptive_value):
    """preemptive is accepted with any boolean value when auth is BASIC or
    OAUTH2 — both are listed as "applicable" by Boomi docs."""
    if applicable_auth == "OAUTH2":
        cfg = _minimal_oauth2_config()
    else:
        cfg = _minimal_basic_config()
    cfg["preemptive"] = preemptive_value
    assert RestClientConnectionBuilder.validate_config(cfg) is None


def test_preemptive_absent_accepted_for_any_auth():
    """When the caller omits the preemptive key entirely, validation passes
    for any auth (no stale-field intent to flag)."""
    for cfg_factory in (
        _minimal_none_config,
        _minimal_basic_config,
        _minimal_ntlm_config,
        _minimal_oauth2_config,
    ):
        cfg = cfg_factory()
        cfg.pop("preemptive", None)
        assert RestClientConnectionBuilder.validate_config(cfg) is None


def test_preemptive_none_accepted_for_non_applicable_auth():
    """`preemptive=None` (caller passes the key with a null value) is
    treated as "not supplied" — accept it for non-applicable auths too."""
    cfg = _minimal_none_config()
    cfg["preemptive"] = None
    assert RestClientConnectionBuilder.validate_config(cfg) is None


@pytest.mark.parametrize("stale_value", ["true", 1, ["true"], {"value": True}])
def test_stale_preemptive_non_bool_value_rejected_for_non_applicable_auth(stale_value):
    """Non-bool preemptive value with non-applicable auth is rejected by
    the stale gate before the type check runs. Field is still 'preemptive'."""
    cfg = _minimal_none_config()
    cfg["preemptive"] = stale_value
    with pytest.raises(BuilderValidationError) as excinfo:
        RestClientConnectionBuilder().build(**{k: v for k, v in cfg.items() if k != "connector_type"})
    err = excinfo.value
    assert err.error_code == "REST_CONNECTOR_VALIDATION_FAILED"
    assert err.field == "preemptive"
