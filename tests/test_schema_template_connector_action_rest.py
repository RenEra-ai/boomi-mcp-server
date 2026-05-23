"""Schema-template tests for component / create / connector-action / rest.operation.

Issue #24 (M2.4). Anti-template policy: examples MUST use angle-bracket
placeholders and $ref tokens only. No canned API paths, headers, SQL,
SOAP envelopes, or plaintext credential values.
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
    # "delete " excluded: legitimately referenced as the DELETE HTTP verb
    #   in supported_methods after Phase 5.
    # " from " excluded: legitimately appears in credential_note ("...header
    #   from the encrypted credential store") — non-SQL prose.
    " where ",
    "<sql>",
    "<dbstatement",
    "<process",
    "<?xml",
    "$filter=",
    "$select=",
    "$expand=",
    # "x-api-key" excluded post-Phase-6: schema documentation legitimately
    # names X-API-Key as a REJECTED secret-shaped key. Naming a rejected
    # pattern is the opposite of a credential leak.
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

def test_overview_lists_rest_operation():
    result = _call(component_type="connector-action")
    assert result["_success"] is True
    assert "rest.operation" in result["available_protocols"]


def test_overview_does_not_list_http_protocols():
    result = _call(component_type="connector-action")
    assert "http.send" not in result["available_protocols"]


def test_unknown_rest_protocol_returns_structured_error():
    result = _call(component_type="connector-action", protocol="rest.bogus")
    assert result["_success"] is False
    assert "rest.operation" in result["valid_protocols"]


def test_full_template_returned_for_rest_operation_protocol():
    result = _call(component_type="connector-action", protocol="rest.operation")
    assert result["_success"] is True
    assert result["component_type"] == "connector-action"
    assert result["protocol"] == "rest.operation"
    assert result["tool"] == "manage_connector (action='create')"


# ----------------------------------------------------------------------------
# Structure
# ----------------------------------------------------------------------------

def test_template_documents_required_fields():
    result = _call(component_type="connector-action", protocol="rest.operation")
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
    result = _call(component_type="connector-action", protocol="rest.operation")
    defaults = result["defaults"]
    assert defaults["component_type"] == "connector-action"
    assert defaults["connector_type"] == "rest"
    assert defaults["operation_mode"] == "execute"
    assert defaults["folder_name"] == "Home"


def test_template_lists_supported_operation_modes():
    result = _call(component_type="connector-action", protocol="rest.operation")
    assert result["supported_operation_modes"] == ["execute"]


def test_template_lists_supported_methods():
    result = _call(component_type="connector-action", protocol="rest.operation")
    assert set(result["supported_methods"]) == {
        "GET", "PATCH", "PUT", "POST", "DELETE", "HEAD", "OPTIONS", "TRACE",
    }


def test_template_unverified_pending_methods_empty():
    """Phase 5 made all 8 REST methods buildable. The unverified-pending
    list must be empty (no method recognized-but-not-buildable)."""
    result = _call(component_type="connector-action", protocol="rest.operation")
    assert result["unverified_pending_methods"] == []


def test_template_documents_follow_redirects_emission_rule():
    """Phase 5: per-method followRedirects emission rule must be advertised
    so callers know which methods default to NONE vs which omit the field."""
    result = _call(component_type="connector-action", protocol="rest.operation")
    rule = result["follow_redirects_emission_rule"]
    assert isinstance(rule, dict)
    assert set(rule["default_none_methods"]) == {"GET", "POST", "HEAD", "DELETE"}
    assert set(rule["omit_methods"]) == {"PATCH", "PUT", "OPTIONS", "TRACE"}
    assert rule["explicit_values_always_emit"] is True


def test_template_documents_query_parameters_status():
    """Phase 6 made plain customProperties buildable for both maps."""
    result = _call(component_type="connector-action", protocol="rest.operation")
    assert result["query_parameters_status"] == "plain_buildable"
    assert result["request_headers_status"] == "plain_buildable"


def test_template_documents_customproperties_shape():
    """Phase 6: schema surfaces the customProperty rules (plain examples
    + rejected secret patterns)."""
    result = _call(component_type="connector-action", protocol="rest.operation")
    shape = result["customproperties_shape"]
    assert isinstance(shape, dict)
    assert "limit" in shape["plain_examples"]["query_parameters"]
    assert "Accept" in shape["plain_examples"]["request_headers"]
    # Rejected key list mentions Authorization, api-key, bearer (lowercased).
    rejected_keys_blob = shape["rejected_secret_shaped_keys"].lower()
    assert "authorization" in rejected_keys_blob
    assert "bearer" in rejected_keys_blob


def test_template_documents_depends_on_requirements():
    result = _call(component_type="connector-action", protocol="rest.operation")
    reqs = result["depends_on_requirements"]
    joined = " ".join(reqs)
    assert "connection_ref_key" in joined
    assert "$ref" in joined
    assert "response_profile_id" in joined
    assert "payload_source_ref_key" in joined


def test_template_documents_error_codes():
    result = _call(component_type="connector-action", protocol="rest.operation")
    codes = result["error_codes"]
    for expected in (
        "UNSUPPORTED_REST_OPERATION_MODE",
        "UNSUPPORTED_REST_METHOD",
        "UNVERIFIED_REST_XML_VARIANT",
        "REST_CUSTOM_PROPERTY_INVALID",
        "UNSUPPORTED_REST_ENCRYPTED_CUSTOM_PROPERTY",
        "REST_SECRET_VALUE_FORBIDDEN",
        "REST_PATH_REQUIRED",
        "REST_CONNECTION_REF_REQUIRED",
        "REST_DEPENDENCY_REQUIRED",
        "REST_PROFILE_REF_UNRESOLVED",
        "PLAINTEXT_SECRET_REJECTED",
    ):
        assert expected in codes


def test_template_documents_forbidden_secret_fields():
    result = _call(component_type="connector-action", protocol="rest.operation")
    assert set(result["forbidden_secret_fields"]) == set(_FORBIDDEN_SECRET_FIELDS)


def test_template_documents_subtype_constant():
    result = _call(component_type="connector-action", protocol="rest.operation")
    assert result["boomi_subtype"] == "officialboomi-X3979C-rest-prod"


def test_template_example_uses_ref_tokens():
    result = _call(component_type="connector-action", protocol="rest.operation")
    cfg = result["example"]["config"]
    assert cfg["request_profile_id"].startswith("$ref:")


def test_patch_template_omits_follow_redirects():
    """Codex review P3 regression: the template defaults method to PATCH,
    and the verified PATCH live export (64c4eafd) omits the followRedirects
    field entirely. Including follow_redirects in the template would cause
    users who copy the template verbatim to emit an unverified PATCH XML
    variant."""
    result = _call(component_type="connector-action", protocol="rest.operation")
    template = result["template"]
    assert template["method"] == "PATCH"
    assert "follow_redirects" not in template, (
        "rest.operation template must NOT include follow_redirects when its "
        "default method is PATCH — the verified PATCH live export omits the "
        "field. Document follow_redirects via follow_redirects_values / "
        "follow_redirects_emission_rule instead."
    )
    example_cfg = result["example"]["config"]
    assert "follow_redirects" not in example_cfg, (
        "rest.operation example must NOT include follow_redirects for the "
        "same reason."
    )


# ----------------------------------------------------------------------------
# Anti-leak hygiene
# ----------------------------------------------------------------------------

def test_template_does_not_carry_forbidden_secret_keys():
    result = _call(component_type="connector-action", protocol="rest.operation")

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
    result = _call(component_type="connector-action", protocol="rest.operation")
    blob = repr(result).lower()
    for forbidden in _FORBIDDEN_TEMPLATE_SUBSTRINGS:
        assert forbidden not in blob, (
            f"Template leaks forbidden substring: {forbidden!r}"
        )


def test_field_method_dependency_map_independent_fields_present():
    """The schema must declare which operation fields are independent of
    method (work with any of the 8 supported verbs). Most REST operation
    fields are method-orthogonal — only follow_redirects has a method-tied
    default-emission rule."""
    result = _call(component_type="connector-action", protocol="rest.operation")
    dep_map = result.get("field_method_dependency_map")
    assert dep_map is not None, "field_method_dependency_map must be present"
    independent = dep_map.get("independent")
    assert isinstance(independent, list)
    for field in (
        "path",
        "query_parameters",
        "request_headers",
        "request_profile_ref",
        "response_profile_ref",
        "request_profile_type",
        "response_profile_type",
        "return_application_errors",
        "track_response",
    ):
        assert field in independent, (
            f"field {field!r} must be listed as method-independent in "
            "field_method_dependency_map.independent"
        )


def test_field_method_dependency_map_follow_redirects_default():
    """The schema must declare the per-method default-emission split for
    follow_redirects so callers know which methods auto-emit NONE and which
    omit the field when the caller doesn't supply a value."""
    result = _call(component_type="connector-action", protocol="rest.operation")
    method_tied = result["field_method_dependency_map"].get("method_tied")
    assert isinstance(method_tied, dict)
    follow = method_tied.get("follow_redirects_default")
    assert isinstance(follow, dict)
    assert sorted(follow.get("emit_NONE", [])) == sorted(["GET", "POST", "HEAD", "DELETE"])
    assert sorted(follow.get("omit", [])) == sorted(["PATCH", "PUT", "OPTIONS", "TRACE"])


def test_out_of_scope_does_not_list_supported_methods_or_customproperties():
    """Codex round-1 P2 #5: out_of_scope must NOT advertise the Phase-5
    methods (POST/PUT/DELETE/HEAD/OPTIONS/TRACE) as deferred, and must NOT
    say populated customProperties return NEEDS_REST_EXAMPLE — those paths
    are now buildable."""
    result = _call(component_type="connector-action", protocol="rest.operation")
    oos = result.get("out_of_scope", {})
    oos_blob = repr(oos)
    # No supported method should appear as deferred.
    import re
    for method in ("POST", "PUT", "DELETE", "HEAD", "OPTIONS", "TRACE"):
        assert not re.search(rf"\b{method}\b", oos_blob), (
            f"out_of_scope still names supported method {method!r} as "
            "deferred; remove from the unverified_methods entry."
        )
    # The deferred-customProperty key must be gone.
    assert "non_empty_query_parameters_and_headers" not in oos
    # And neither code should appear in the deferred docs.
    assert "UNVERIFIED_REST_XML_VARIANT" not in oos_blob
    assert "NEEDS_REST_EXAMPLE" not in oos_blob
