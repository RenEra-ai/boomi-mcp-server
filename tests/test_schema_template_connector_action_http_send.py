"""Schema-template tests for component / create / connector-action / http.send.

Issue #24 (M2.4). Anti-template policy: examples MUST use angle-bracket
placeholders and $ref tokens only — no canned payloads, endpoints, SQL,
SOAP envelopes, or plaintext secrets.
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
    "<httpsendaction",
    "<httpgetaction",
    "<?xml",
    "$filter=",
    "$select=",
    "$expand=",
    "bearer ",
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

def test_overview_lists_http_send_in_available_protocols():
    result = _call(component_type="connector-action")
    assert result["_success"] is True
    assert "http.send" in result["available_protocols"]


def test_unknown_http_protocol_returns_structured_error():
    result = _call(component_type="connector-action", protocol="http.bogus")
    assert result["_success"] is False
    assert "http.send" in result["valid_protocols"]


def test_full_template_returned_for_http_send_protocol():
    result = _call(component_type="connector-action", protocol="http.send")
    assert result["_success"] is True
    assert result["component_type"] == "connector-action"
    assert result["protocol"] == "http.send"
    assert result["tool"] == "manage_connector (action='create')"


# ----------------------------------------------------------------------------
# Structure
# ----------------------------------------------------------------------------

def test_template_documents_required_fields():
    result = _call(component_type="connector-action", protocol="http.send")
    required = set(result["required"])
    for expected in (
        "component_type",
        "connector_type",
        "operation_mode",
        "component_name",
        "connection_ref_key",
        "method",
        "path",
    ):
        assert expected in required


def test_template_documents_defaults():
    result = _call(component_type="connector-action", protocol="http.send")
    defaults = result["defaults"]
    assert defaults["component_type"] == "connector-action"
    assert defaults["connector_type"] == "http"
    assert defaults["operation_mode"] == "send"
    assert defaults["content_type"] == "application/json"
    assert defaults["folder_name"] == "Home"


def test_template_lists_supported_operation_modes():
    result = _call(component_type="connector-action", protocol="http.send")
    assert result["supported_operation_modes"] == ["send"]


def test_template_lists_supported_methods():
    result = _call(component_type="connector-action", protocol="http.send")
    assert set(result["supported_methods"]) == {"GET", "POST", "PUT", "PATCH", "DELETE"}


def test_template_documents_method_to_action_mapping():
    result = _call(component_type="connector-action", protocol="http.send")
    mapping = result["methods_to_action_mapping"]
    assert mapping["GET"] == "HttpGetAction"
    for send_method in ("POST", "PUT", "PATCH", "DELETE"):
        assert mapping[send_method] == "HttpSendAction"


def test_template_documents_depends_on_requirements():
    result = _call(component_type="connector-action", protocol="http.send")
    reqs = result["depends_on_requirements"]
    text = " ".join(reqs)
    assert "connection_ref_key" in text
    assert "$ref" in text
    assert "payload_source_ref_key" in text


def test_template_documents_error_codes():
    result = _call(component_type="connector-action", protocol="http.send")
    codes = result["error_codes"]
    for expected in (
        "UNSUPPORTED_HTTP_OPERATION_MODE",
        "UNSUPPORTED_HTTP_METHOD",
        "MISSING_HTTP_DEPENDENCY",
        "MISSING_HTTP_REQUEST_PROFILE_REF",
        "HTTP_OPERATION_VALIDATION_FAILED",
        "PLAINTEXT_SECRET_REJECTED",
    ):
        assert expected in codes


def test_template_documents_forbidden_secret_fields():
    result = _call(component_type="connector-action", protocol="http.send")
    assert set(result["forbidden_secret_fields"]) == set(_FORBIDDEN_SECRET_FIELDS)


def test_template_example_uses_ref_tokens_and_placeholders():
    result = _call(component_type="connector-action", protocol="http.send")
    example = result["example"]
    cfg = example["config"]
    # request_profile_id must be a $ref token, not a UUID.
    assert isinstance(cfg["request_profile_id"], str)
    assert cfg["request_profile_id"].startswith("$ref:")


# ----------------------------------------------------------------------------
# Anti-leak hygiene
# ----------------------------------------------------------------------------

def test_template_does_not_carry_forbidden_secret_keys():
    result = _call(component_type="connector-action", protocol="http.send")

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


def test_template_contains_no_canned_payloads_or_sql_or_xml():
    result = _call(component_type="connector-action", protocol="http.send")
    blob = repr(result).lower()
    for forbidden in _FORBIDDEN_TEMPLATE_SUBSTRINGS:
        assert forbidden not in blob, f"Template leaks forbidden substring: {forbidden!r}"


def test_template_documents_path_slash_stripping():
    """The 'one leading slash stripped' rule is a footgun without
    documentation — it must appear in gotchas or template note."""
    result = _call(component_type="connector-action", protocol="http.send")
    blob = repr(result).lower()
    assert "leading" in blob and "slash" in blob


def test_template_documents_header_and_path_key_starts():
    result = _call(component_type="connector-action", protocol="http.send")
    blob = repr(result)
    # 1000000 + 2000000 markers describe the deterministic key sequence.
    assert "1000000" in blob
    assert "2000000" in blob
