"""Schema-template tests for component / create / connector-settings / http.client.

Issue #24 (M2.4). Anti-template policy: examples MUST use angle-bracket
placeholders only (and `credential://...` opaque references). No canned
endpoints (no SendGrid URLs, no example.com bearer tokens), no SQL, no
SOAP envelopes, no plaintext secrets.
"""

import pytest

from boomi_mcp.categories.meta_tools import get_schema_template_action


_FORBIDDEN_SECRET_FIELDS = (
    "password",
    "password_ref",
    "secret",
    "token",
    "access_token",
    "client_secret",
)

_FORBIDDEN_TEMPLATE_SUBSTRINGS = (
    "select ",
    "insert ",
    "update ",
    "delete ",
    " from ",
    " where ",
    "<sql>",
    "<dbstatement",
    "<process",
    "<connector",
    "<?xml",
    "$filter=",
    "$select=",
    "$expand=",
    "bearer ",
    "api-key",
    "x-api-key",
)


def _call(**kwargs):
    return get_schema_template_action(
        resource_type="component",
        operation="create",
        **kwargs,
    )


# ----------------------------------------------------------------------------
# Dispatch
# ----------------------------------------------------------------------------

def test_overview_lists_http_client_in_available_protocols():
    result = _call(component_type="connector-settings")
    assert result["_success"] is True
    assert "http.client" in result["available_protocols"]


def test_unknown_http_protocol_returns_structured_error():
    result = _call(component_type="connector-settings", protocol="http.bogus")
    assert result["_success"] is False
    assert "http.client" in result["valid_protocols"]


def test_full_template_returned_for_http_client_protocol():
    result = _call(component_type="connector-settings", protocol="http.client")
    assert result["_success"] is True
    assert result["component_type"] == "connector-settings"
    assert result["protocol"] == "http.client"
    assert result["tool"] == "manage_connector (action='create')"


# ----------------------------------------------------------------------------
# Structure
# ----------------------------------------------------------------------------

def test_template_documents_required_fields():
    result = _call(component_type="connector-settings", protocol="http.client")
    required = set(result["required"])
    for expected in ("connector_type", "component_name", "url"):
        assert expected in required


def test_template_documents_defaults():
    result = _call(component_type="connector-settings", protocol="http.client")
    defaults = result["defaults"]
    assert defaults["auth_type"] == "NONE"
    assert defaults["folder_name"] == "Home"


def test_template_lists_supported_and_unsupported_auth_modes():
    result = _call(component_type="connector-settings", protocol="http.client")
    assert result["supported_auth_modes"] == ["NONE"]
    unsupported = set(result["unsupported_future_auth_modes"])
    for mode in ("BASIC", "OAUTH2", "PASSWORD_DIGEST", "CUSTOM", "OAUTH"):
        assert mode in unsupported


def test_template_documents_error_codes():
    result = _call(component_type="connector-settings", protocol="http.client")
    codes = result["error_codes"]
    for expected in (
        "HTTP_CONNECTOR_VALIDATION_FAILED",
        "MISSING_HTTP_ENDPOINT",
        "UNSUPPORTED_HTTP_AUTH_MODE",
        "PLAINTEXT_SECRET_REJECTED",
    ):
        assert expected in codes


def test_template_documents_forbidden_secret_fields():
    result = _call(component_type="connector-settings", protocol="http.client")
    assert set(result["forbidden_secret_fields"]) == set(_FORBIDDEN_SECRET_FIELDS)


# ----------------------------------------------------------------------------
# Anti-leak hygiene
# ----------------------------------------------------------------------------

def test_template_does_not_carry_forbidden_secret_keys():
    result = _call(component_type="connector-settings", protocol="http.client")

    def _walk(obj):
        if isinstance(obj, dict):
            for k, v in obj.items():
                assert k not in _FORBIDDEN_SECRET_FIELDS, (
                    f"Schema template leaks a forbidden secret-shaped key: {k!r}"
                )
                _walk(v)
        elif isinstance(obj, list):
            for item in obj:
                _walk(item)

    _walk(result["template"])
    _walk(result["example"])


def test_template_contains_no_canned_payloads_or_sql_or_soap():
    result = _call(component_type="connector-settings", protocol="http.client")
    blob = repr(result).lower()
    for forbidden in _FORBIDDEN_TEMPLATE_SUBSTRINGS:
        assert forbidden not in blob, f"Template leaks forbidden substring: {forbidden!r}"


def test_template_credential_ref_uses_placeholder():
    result = _call(component_type="connector-settings", protocol="http.client")
    if "credential_ref" in result["template"]:
        assert result["template"]["credential_ref"].startswith("credential://")
    if "credential_ref" in result["example"]["config"]:
        assert result["example"]["config"]["credential_ref"].startswith("credential://")
