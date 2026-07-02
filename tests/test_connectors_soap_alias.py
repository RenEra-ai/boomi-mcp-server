"""SOAP Client alias normalization in connector read/list/create dispatch (#126).

The MCP server advertises 'soap_client' / 'web_services_soap_client' as public
aliases for the canonical Boomi subtype 'wssoapclientsdk'. get_type / list must
normalize before the SDK call, create must select the SOAP builders, and a
non-execute SOAP connector-action must surface the structured EXECUTE-only error.
"""

from unittest.mock import MagicMock, patch

import pytest

from boomi_mcp.categories.components.connectors import (
    get_connector_type,
    list_connectors,
    manage_connector_action,
)
from boomi_mcp.categories.components.builders.connector_builder import (
    SoapClientConnectionBuilder,
    SoapClientOperationBuilder,
    get_connector_builder,
    get_connector_action_builder,
)

CANONICAL = "wssoapclientsdk"


@pytest.mark.parametrize("alias", ["soap_client", "web_services_soap_client", CANONICAL, "SOAP_CLIENT"])
def test_get_connector_type_normalizes_soap_aliases(alias):
    client = MagicMock()
    fake = MagicMock()
    fake.name = "Web Services SOAP Client"
    fake.type_ = CANONICAL
    fake.field = []
    fake.operation_type = []
    client.connector.get_connector.return_value = fake
    result = get_connector_type(client, alias)
    assert result["_success"] is True
    args = [c.args for c in client.connector.get_connector.call_args_list]
    assert args == [(CANONICAL,)]


def _capture_subtype_filter(query_config):
    expression = query_config.query_filter.expression
    candidates = []
    if hasattr(expression, "nested_expression") and expression.nested_expression:
        candidates.extend(expression.nested_expression)
        for nested in expression.nested_expression:
            if hasattr(nested, "nested_expression") and nested.nested_expression:
                candidates.extend(nested.nested_expression)
    candidates.append(expression)
    for cand in candidates:
        prop = getattr(cand, "property", None)
        if prop is not None and str(prop).endswith("SUBTYPE"):
            return cand.argument
    return None


@pytest.mark.parametrize("alias", ["soap_client", "web_services_soap_client", "SOAP_CLIENT"])
def test_list_connectors_normalizes_soap_alias(alias):
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
    assert _capture_subtype_filter(captured["query_config"]) == [CANONICAL]


def test_registry_dispatch_selects_soap_builders():
    assert isinstance(get_connector_builder("soap_client"), SoapClientConnectionBuilder)
    assert isinstance(get_connector_action_builder("soap_client", "execute"), SoapClientOperationBuilder)


def test_missing_component_name_hint_uses_soap_example():
    client = MagicMock()
    client.connector.get_connector.return_value = MagicMock()
    result = manage_connector_action(
        client, "test", "create", config={"connector_type": "soap_client"}
    )
    assert result["_success"] is False
    hint = result.get("hint", "")
    assert "wsdl_url" in hint and "endpoint_url" in hint


def test_non_execute_soap_action_surfaces_structured_error():
    """A connector-action with connector_type='soap_client' and a non-execute
    operation_mode must surface UNSUPPORTED_SOAP_OPERATION_MODE, not a generic
    'no builder' message."""
    client = MagicMock()
    client.connector.get_connector.return_value = MagicMock()
    result = manage_connector_action(
        client,
        "test",
        "create",
        config={
            "component_type": "connector-action",
            "connector_type": "soap_client",
            "operation_mode": "get",
            "component_name": "Bad SOAP Op",
        },
    )
    assert result["_success"] is False
    haystack = (result.get("error", "") + " " + result.get("error_code", "")).upper()
    assert "UNSUPPORTED_SOAP_OPERATION_MODE" in haystack or "EXECUTE" in haystack.upper()


@pytest.mark.parametrize("ambiguous", ["soap", "wss", "web_services", "soap_server"])
def test_ambiguous_soap_aliases_not_normalized(ambiguous):
    """The ambiguous tokens must pass through unchanged (never routed to the
    outbound SOAP Client) so an inbound Web Services Server can't be misrouted."""
    client = MagicMock()
    fake = MagicMock()
    fake.field = []
    fake.operation_type = []
    client.connector.get_connector.return_value = fake
    get_connector_type(client, ambiguous)
    assert client.connector.get_connector.call_args.args == (ambiguous,)
