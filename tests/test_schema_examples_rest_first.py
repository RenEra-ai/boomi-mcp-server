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
