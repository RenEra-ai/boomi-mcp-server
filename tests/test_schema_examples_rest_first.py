"""Regression tests for issue #24: schema discovery examples must show REST
Client as the canonical M2 target, not HTTP Client.

Codex review (round 2, item P2 #A) found the integration-plan template and
the manage_connector capability examples still steered LLM clients toward
the HTTP Client connector even though issue #24 made REST Client the M2
target. These tests lock the post-#24 state.
"""

import pytest

from boomi_mcp.categories.meta_tools import (
    get_schema_template_action,
    list_capabilities_action,
)


def test_integration_plan_template_uses_rest_connection_example():
    """The build_integration plan template's canonical connector-settings
    example must reference REST Client (post-#24 M2 target), not HTTP Client."""
    result = get_schema_template_action(resource_type="integration", operation="plan")
    assert result["_success"] is True

    components = (
        result.get("template", {})
        .get("source_description", {})
        .get("components", [])
    )
    connector_settings = [
        c for c in components if c.get("type") == "connector-settings"
    ]
    assert connector_settings, (
        "Integration plan template should include at least one "
        "connector-settings example."
    )

    # First connector-settings example is the canonical one.
    canonical = connector_settings[0]
    config = canonical.get("config", {})
    assert config.get("connector_type") == "rest", (
        f"Integration plan example connector_type must be 'rest' "
        f"(post-#24 M2 target), got {config.get('connector_type')!r}."
    )
    # REST connection shape uses base_url + auth + oauth2, not the
    # HTTP shape url + auth_type.
    assert "base_url" in config, (
        "REST connection template must use 'base_url' (not 'url')."
    )
    assert "auth" in config, (
        "REST connection template must use 'auth' (not 'auth_type')."
    )
    assert "url" not in config, (
        "REST connection template must not carry the HTTP-shape 'url' field."
    )
    assert "auth_type" not in config, (
        "REST connection template must not carry the HTTP-shape 'auth_type' field."
    )


def test_integration_plan_template_steers_dependents_to_rest_connection_key():
    """Components that depended on the old http_connection key need their
    depends_on updated to the new rest_connection key (otherwise the
    example doesn't compose)."""
    result = get_schema_template_action(resource_type="integration", operation="plan")
    components = (
        result.get("template", {})
        .get("source_description", {})
        .get("components", [])
    )
    connector_settings = [c for c in components if c.get("type") == "connector-settings"]
    assert connector_settings
    rest_key = connector_settings[0]["key"]
    # No depends_on should still point at "http_connection".
    for comp in components:
        for dep in comp.get("depends_on", []) or []:
            assert dep != "http_connection", (
                f"Component {comp.get('key')!r} still depends on the "
                "obsolete 'http_connection' key — update to the REST "
                f"connection key {rest_key!r}."
            )


def test_no_residual_http_client_references_in_capability_or_plan():
    """Issue #24 follow-up: the legacy HTTP Client connector has been
    removed entirely. The integration plan template, manage_connector
    capability examples, and REST schema templates must carry zero
    residual HTTP Client steering — no 'connector_type: http' configs,
    no HttpSettings emission, no 'HTTP Client' name-drops in the REST
    docs that compared the two connectors. (Trading-partner protocol
    'http' is unrelated — this test does not look at trading-partner
    templates.)"""
    integration_plan_blob = repr(get_schema_template_action(
        resource_type="integration", operation="plan",
    ))
    rest_client_blob = repr(get_schema_template_action(
        resource_type="component", operation="create",
        component_type="connector-settings", protocol="rest.client",
    ))
    rest_operation_blob = repr(get_schema_template_action(
        resource_type="component", operation="create",
        component_type="connector-action", protocol="rest.operation",
    ))
    capability_blob = repr(
        list_capabilities_action().get("tools", {}).get("manage_connector", {})
    )

    blobs = {
        "integration_plan": integration_plan_blob,
        "rest_client_template": rest_client_blob,
        "rest_operation_template": rest_operation_blob,
        "manage_connector_capability": capability_blob,
    }

    # Substrings that signal HTTP-Client-shaped content. These must not
    # appear anywhere in the schema/capability text. (Free-form text
    # 'http://example.com' inside URLs is fine — we only forbid the
    # HTTP-connector field shape and the HTTP Client connector name.)
    forbidden_substrings = [
        "'connector_type': 'http'",
        '"connector_type": "http"',
        "'auth_type':",
        '"auth_type":',
        "HttpSettings",
        "HTTP Client",
        "http.client",
        "http.send",
    ]

    for blob_name, blob in blobs.items():
        for forbidden in forbidden_substrings:
            assert forbidden not in blob, (
                f"{blob_name!r} carries residual HTTP Client steering: "
                f"substring {forbidden!r} found. Remove this leftover after "
                "deleting HttpConnectorBuilder."
            )


def test_integration_plan_template_has_no_dangling_http_connection_refs():
    """Regression for codex round-3: after renaming the connector key from
    'http_connection' to 'rest_connection', any `$ref:http_connection` token
    anywhere inside the template (e.g. process shapes' connector_id) would
    point at a non-existent component and apply would silently leave the
    binding unresolved."""
    result = get_schema_template_action(resource_type="integration", operation="plan")
    blob = repr(result)
    assert "$ref:http_connection" not in blob, (
        "Integration plan template still carries '$ref:http_connection' "
        "somewhere (process shape config?). Update to '$ref:rest_connection' "
        "so the apply path resolves the REST connector binding correctly."
    )


def test_manage_connector_capability_create_example_uses_rest():
    """list_capabilities surfaces example invocations. The create example
    must demonstrate REST Client, not HTTP Client (post-#24)."""
    catalog = list_capabilities_action()
    tools = catalog.get("tools", {})
    examples = tools.get("manage_connector", {}).get("examples", [])
    assert examples, "manage_connector capability should expose example invocations."
    # The create example is the one that contains action="create".
    create_examples = [ex for ex in examples if "action=\"create\"" in ex]
    assert create_examples, (
        "manage_connector capability should include at least one "
        "action=\"create\" example."
    )
    # At least one create example must steer callers to REST.
    has_rest_example = any(
        '"connector_type": "rest"' in ex
        or '"connector_type": "rest_client"' in ex
        for ex in create_examples
    )
    assert has_rest_example, (
        "After issue #24, manage_connector create examples must show REST "
        "Client (connector_type='rest' or 'rest_client') as the canonical "
        "M2 target. Found only: " + " | ".join(create_examples)
    )
