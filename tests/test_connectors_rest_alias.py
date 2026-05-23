"""Tests for REST alias normalization in connector read/list dispatchers
plus REST-first error hint contents (issue #24, codex round-4 fixes).

The MCP server advertises 'rest' and 'rest_client' as public aliases for
the canonical Boomi subtype 'officialboomi-X3979C-rest-prod'. The
create/apply path already normalizes; these tests cover the gap for
get_type and list filters, plus the error-hint copy that previously
steered users toward HTTP Client.
"""

from unittest.mock import MagicMock, patch

import pytest

from boomi_mcp.categories.components.connectors import (
    get_connector_type,
    list_connectors,
    manage_connector_action,
)


CANONICAL = "officialboomi-X3979C-rest-prod"


# ----------------------------------------------------------------------------
# Fix 1 — get_connector_type normalizes REST aliases before the Boomi API call.
# ----------------------------------------------------------------------------

@pytest.mark.parametrize("alias", ["rest", "rest_client", CANONICAL, "REST", "Rest_Client"])
def test_get_connector_type_normalizes_rest_aliases(alias):
    """Boomi's connector catalog only knows the canonical subtype. The MCP
    boundary must translate the public alias before the SDK call."""
    client = MagicMock()
    fake_result = MagicMock()
    fake_result.name = "REST Client"
    fake_result.type_ = CANONICAL
    fake_result.field = []
    fake_result.operation_type = []
    client.connector.get_connector.return_value = fake_result

    result = get_connector_type(client, alias)
    assert result["_success"] is True
    # SDK was invoked with the canonical, never the alias.
    args = [c.args for c in client.connector.get_connector.call_args_list]
    assert args == [(CANONICAL,)]


def test_get_connector_type_passes_non_rest_through_unchanged():
    """Database / http / arbitrary connector types must not be touched."""
    client = MagicMock()
    fake_result = MagicMock(name="db")
    fake_result.field = []
    fake_result.operation_type = []
    client.connector.get_connector.return_value = fake_result
    get_connector_type(client, "database")
    assert client.connector.get_connector.call_args.args == ("database",)


# ----------------------------------------------------------------------------
# Fix 1 — list_connectors normalizes REST aliases in the subtype filter.
# ----------------------------------------------------------------------------

def _capture_subtype_filter(query_config):
    """Walk the query expression and return the SUBTYPE filter's argument."""
    # paginate_metadata receives ComponentMetadataQueryConfig with a query_filter
    expression = query_config.query_filter.expression
    # The root might be a Grouping (AND) or a single SimpleExpression.
    candidates = []
    if hasattr(expression, "nested_expression") and expression.nested_expression:
        candidates.extend(expression.nested_expression)
        for nested in expression.nested_expression:
            if hasattr(nested, "nested_expression") and nested.nested_expression:
                candidates.extend(nested.nested_expression)
    candidates.append(expression)
    for cand in candidates:
        prop = getattr(cand, "property", None)
        # SubType property name is the enum SUBTYPE; compare its string value.
        if prop is not None and str(prop).endswith("SUBTYPE"):
            return cand.argument
    return None


@pytest.mark.parametrize("alias", ["rest", "rest_client", "REST_CLIENT"])
def test_list_connectors_normalizes_rest_alias_in_subtype_filter(alias):
    """A filter like {"connector_type": "rest"} must query Boomi for the
    canonical subtype 'officialboomi-X3979C-rest-prod', otherwise zero
    components match (Boomi components carry the canonical subType)."""
    captured = {}

    def fake_paginate(client, query_config, show_all=False):
        captured["query_config"] = query_config
        return []

    client = MagicMock()
    with patch(
        "boomi_mcp.categories.components.connectors.paginate_metadata",
        side_effect=fake_paginate,
    ):
        result = list_connectors(client, "test", filters={"connector_type": alias})
    assert result["_success"] is True
    argument = _capture_subtype_filter(captured["query_config"])
    assert argument == [CANONICAL]


def test_list_connectors_passes_database_filter_unchanged():
    captured = {}

    def fake_paginate(client, query_config, show_all=False):
        captured["query_config"] = query_config
        return []

    with patch(
        "boomi_mcp.categories.components.connectors.paginate_metadata",
        side_effect=fake_paginate,
    ):
        list_connectors(MagicMock(), "test", filters={"connector_type": "database"})
    argument = _capture_subtype_filter(captured["query_config"])
    assert argument == ["database"]


# ----------------------------------------------------------------------------
# Fix 2 — error hints no longer steer users to HTTP Client.
# ----------------------------------------------------------------------------

def test_create_missing_config_hint_uses_rest_example():
    """The 'config is required for create' hint must show a REST OAuth2
    example, not the legacy HTTP url-shape example."""
    result = manage_connector_action(MagicMock(), "test", "create")
    assert result["_success"] is False
    hint = result.get("hint", "")
    assert "connector_type" in hint
    # REST is the M2 target — hint must steer users toward REST shape.
    assert "rest" in hint.lower(), (
        "create error hint should mention 'rest' as the M2 target connector."
    )
    # Must NOT carry the HTTP-shape 'url' field, 'auth_type' field, or
    # encourage connector_type='http'.
    assert '"connector_type": "http"' not in hint
    assert '"url": "https://...' not in hint
    assert '"auth_type"' not in hint


def test_get_type_missing_connector_type_hint_uses_rest_or_canonical_example():
    """The 'connector_type is required for get_type' hint must use either
    the canonical Boomi subtype or a REST alias, not an HTTP example."""
    result = manage_connector_action(MagicMock(), "test", "get_type")
    assert result["_success"] is False
    hint = result.get("hint", "")
    has_rest_or_canonical = (
        '"connector_type": "rest"' in hint
        or '"connector_type": "rest_client"' in hint
        or CANONICAL in hint
    )
    assert has_rest_or_canonical, (
        "get_type missing-arg hint should reference REST (alias) or the "
        "canonical 'officialboomi-X3979C-rest-prod' subtype, not 'http'."
    )
    assert '"connector_type": "http"' not in hint


def test_create_missing_component_name_hint_does_not_show_http_url():
    """When component_name is missing and connector_type is supplied, the
    hint shows a config example for the chosen connector. For REST callers
    the example must NOT use the HTTP-shape 'url' field."""
    client = MagicMock()
    client.connector.get_connector.return_value = MagicMock()
    result = manage_connector_action(
        client,
        "test",
        "create",
        config={"connector_type": "rest"},
    )
    assert result["_success"] is False
    hint = result.get("hint", "")
    # REST connections use base_url, not url; never auth_type.
    assert '"url": "https://...' not in hint
    assert '"auth_type"' not in hint


def test_update_missing_config_hint_does_not_show_http_url():
    """The update-missing-config hint must be connector-type-agnostic. The
    pre-fix hint suggested {"url": "https://new-url.com"}, which is the
    HTTP Client shape — REST Client does not accept field-level url
    updates and most universal updates are component-level (name,
    description, folder_name)."""
    result = manage_connector_action(
        MagicMock(), "test", "update", component_id="abc-123",
    )
    assert result["_success"] is False
    hint = result.get("hint", "")
    # Must NOT show the HTTP-shape example.
    assert '"url": "https://new-url.com"' not in hint
    # Should mention at least one of the universally-updatable fields.
    has_universal_field = any(
        field in hint for field in ("name", "description", "folder_name")
    )
    assert has_universal_field, (
        "update hint should mention name/description/folder_name as the "
        "universally-updatable fields across connector types."
    )
    # Should point at the raw-XML escape hatch for field-level edits on
    # non-HTTP connectors.
    assert "xml" in hint.lower(), (
        "update hint should mention the raw-XML escape hatch (config.xml=...) "
        "for field-level edits on REST / database / other connectors."
    )
