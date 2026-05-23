"""Schema-template tests for component / create / connector-settings / rest.client.

Issue #24 (M2.4). Anti-template policy: examples MUST use angle-bracket
placeholders (and `credential://...` opaque references). No canned API
URLs, SQL, SOAP envelopes, or plaintext credential values.
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
    "delete ",
    " from ",
    " where ",
    "<sql>",
    "<dbstatement",
    "<process",
    "<?xml",
    "$filter=",
    "$select=",
    "$expand=",
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

def test_overview_lists_rest_client():
    result = _call(component_type="connector-settings")
    assert result["_success"] is True
    assert "rest.client" in result["available_protocols"]


def test_overview_does_not_list_http_protocols():
    """Sanity that the superseded HTTP issue-#24 protocols are gone."""
    result = _call(component_type="connector-settings")
    assert "http.client" not in result["available_protocols"]


def test_unknown_rest_protocol_returns_structured_error():
    result = _call(component_type="connector-settings", protocol="rest.bogus")
    assert result["_success"] is False
    assert "rest.client" in result["valid_protocols"]


def test_full_template_returned_for_rest_client_protocol():
    result = _call(component_type="connector-settings", protocol="rest.client")
    assert result["_success"] is True
    assert result["component_type"] == "connector-settings"
    assert result["protocol"] == "rest.client"
    assert result["tool"] == "manage_connector (action='create')"


# ----------------------------------------------------------------------------
# Structure
# ----------------------------------------------------------------------------

def test_template_documents_required_fields():
    result = _call(component_type="connector-settings", protocol="rest.client")
    required = set(result["required"])
    for expected in ("connector_type", "component_name", "base_url", "auth"):
        assert expected in required


def test_template_documents_defaults():
    result = _call(component_type="connector-settings", protocol="rest.client")
    defaults = result["defaults"]
    assert defaults["connector_type"] == "rest"
    assert defaults["auth"] == "OAUTH2"
    assert defaults["folder_name"] == "Home"


def test_template_lists_supported_auth_modes():
    result = _call(component_type="connector-settings", protocol="rest.client")
    supported = set(result["supported_auth_modes"])
    assert "NONE" in supported
    assert "BASIC" in supported
    assert "NTLM" in supported
    assert "OAUTH2" in supported


def test_template_lists_unsupported_future_auth_modes():
    result = _call(component_type="connector-settings", protocol="rest.client")
    unsupported = set(result["unsupported_future_auth_modes"])
    for mode in ("PASSWORD_DIGEST", "CUSTOM", "AWS_SIGNATURE", "AWS_IAM_ROLES_ANYWHERE"):
        assert mode in unsupported
    # Sanity: NONE / BASIC / NTLM are no longer in the deferred list.
    assert "NONE" not in unsupported
    assert "BASIC" not in unsupported
    assert "NTLM" not in unsupported


def test_template_documents_cert_ref_fields():
    """Cert refs (privateCertificate / publicCertificate) are an independent
    client-cert option — they may be supplied with any auth mode. Schema must
    surface this so callers know the fields are accepted."""
    result = _call(component_type="connector-settings", protocol="rest.client")
    blob = repr(result)
    assert "private_certificate_ref" in blob
    assert "public_certificate_ref" in blob


def test_template_documents_oauth2_buildable_grant_types():
    result = _call(component_type="connector-settings", protocol="rest.client")
    buildable = set(result["buildable_oauth2_grant_types"])
    assert "client_credentials" in buildable
    assert "authorization_code" in buildable


def test_template_documents_oauth2_grant_type_aliases():
    """`code` is an XML-side alias for `authorization_code` — surfaced so
    callers know either input works."""
    result = _call(component_type="connector-settings", protocol="rest.client")
    aliases = result.get("oauth2_grant_type_aliases", {})
    assert aliases.get("code") == "authorization_code"
    assert aliases.get("authorization_code") == "authorization_code"


def test_template_documents_subtype_constant():
    result = _call(component_type="connector-settings", protocol="rest.client")
    assert result["boomi_subtype"] == "officialboomi-X3979C-rest-prod"


def test_template_documents_public_aliases():
    result = _call(component_type="connector-settings", protocol="rest.client")
    aliases = set(result["public_aliases"])
    assert aliases == {"rest", "rest_client", "officialboomi-X3979C-rest-prod"}


def test_template_documents_error_codes():
    result = _call(component_type="connector-settings", protocol="rest.client")
    codes = result["error_codes"]
    for expected in (
        "REST_CONNECTOR_VALIDATION_FAILED",
        "REST_BASE_URL_REQUIRED",
        "REST_BASE_URL_INVALID",
        "UNSUPPORTED_REST_AUTH_MODE",
        "REST_SECRET_VALUE_FORBIDDEN",
        "PLAINTEXT_SECRET_REJECTED",
    ):
        assert expected in codes


def test_template_documents_forbidden_secret_fields():
    result = _call(component_type="connector-settings", protocol="rest.client")
    assert set(result["forbidden_secret_fields"]) == set(_FORBIDDEN_SECRET_FIELDS)


# ----------------------------------------------------------------------------
# Anti-leak hygiene
# ----------------------------------------------------------------------------

def test_template_does_not_carry_forbidden_secret_keys():
    result = _call(component_type="connector-settings", protocol="rest.client")

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


def test_template_contains_no_canned_payloads_or_sql():
    result = _call(component_type="connector-settings", protocol="rest.client")
    blob = repr(result).lower()
    for forbidden in _FORBIDDEN_TEMPLATE_SUBSTRINGS:
        assert forbidden not in blob, (
            f"Template leaks forbidden substring: {forbidden!r}"
        )


def test_template_client_secret_ref_uses_credential_scheme():
    result = _call(component_type="connector-settings", protocol="rest.client")
    template_oauth2 = result["template"]["oauth2"]
    assert template_oauth2["client_secret_ref"].startswith("credential://")
    example_oauth2 = result["example"]["config"]["oauth2"]
    assert example_oauth2["client_secret_ref"].startswith("credential://")


def test_out_of_scope_does_not_list_supported_auth_modes_or_grants():
    """Codex round-1 P2 #4: the out_of_scope block must NOT list BASIC,
    NTLM, or plain authorization_code as deferred — those are all
    buildable post-Phases 2/3/4. Only truly-unsupported variants should
    remain (CUSTOM, PASSWORD_DIGEST, AWS_*, resource_owner, jwt_bearer,
    authorization_code-with-cached-token)."""
    result = _call(component_type="connector-settings", protocol="rest.client")
    oos_blob = repr(result.get("out_of_scope", {}))
    # The deferred-emission text must NOT name supported modes.
    # Using whole-word patterns: drop literal "BASIC " / "NTLM " etc.
    import re
    for supported in ("BASIC", "NTLM"):
        assert not re.search(rf"\b{supported}\b", oos_blob), (
            f"out_of_scope still names supported auth mode {supported!r} as "
            f"deferred; remove it from the non_emitted_auth_modes entry."
        )
    # Plain authorization_code is supported (Phase 4); only the
    # cached-token variant remains out of scope. The string
    # "authorization_code" may still appear inside the qualified phrase
    # "authorization_code with cached" — but it must not appear by itself
    # as a fully-deferred grant.
    if "authorization_code" in oos_blob:
        assert "cached" in oos_blob, (
            "out_of_scope mentions authorization_code but doesn't qualify "
            "with 'cached' — implies the whole grant is deferred when "
            "only the cached-token variant is."
        )
