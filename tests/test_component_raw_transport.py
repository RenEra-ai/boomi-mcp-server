"""SDK 3.0.0 raw-XML transport for the shared component helpers.

``_create_component_raw`` and ``_update_component_xml`` route through
``component.create_component`` / ``update_component``, which take raw ``str``/
``bytes`` and return raw response *bytes*. These verify the helpers pass raw XML
straight through (never a typed model) and parse the byte response into the
public metadata dict, and that ``ApiError`` maps to the prior failure text.
"""
from unittest.mock import MagicMock

import pytest

from boomi.net.transport.api_error import ApiError
from boomi_mcp.categories.components._shared import (
    _create_component_raw,
    _update_component_xml,
)

_RESP = (
    '<bns:Component xmlns:bns="http://api.platform.boomi.com/" '
    'componentId="new-1" name="Made" type="tradingpartner" '
    'folderName="Home" version="1"/>'
)


def _client_returning(resp_bytes):
    client = MagicMock()
    client.component.create_component.return_value = resp_bytes
    client.component.update_component.return_value = resp_bytes
    return client


def test_create_passes_raw_xml_and_parses_bytes():
    client = _client_returning(_RESP.encode())
    xml = '<bns:Component xmlns:bns="http://api.platform.boomi.com/" type="tradingpartner"/>'
    out = _create_component_raw(client, xml)
    # The raw XML string is passed straight through — never a model or dict.
    client.component.create_component.assert_called_once_with(xml)
    arg = client.component.create_component.call_args.args[0]
    assert isinstance(arg, (str, bytes))
    assert out["component_id"] == "new-1"
    assert out["name"] == "Made"
    assert out["type"] == "tradingpartner"


def test_create_maps_api_error():
    client = MagicMock()
    client.component.create_component.side_effect = ApiError(
        "Failed to create component: HTTP 400", 400, None
    )
    with pytest.raises(Exception) as ei:
        _create_component_raw(client, "<bns:Component/>")
    assert "Create failed" in str(ei.value) and "400" in str(ei.value)


def test_update_passes_id_and_raw_xml_and_parses_bytes():
    client = _client_returning(_RESP.encode())
    xml = '<bns:Component xmlns:bns="http://api.platform.boomi.com/" componentId="new-1"/>'
    out = _update_component_xml(client, "new-1", xml)
    client.component.update_component.assert_called_once_with("new-1", xml)
    assert out["component_id"] == "new-1"
    assert out["version"] == "1"


def test_update_maps_api_error():
    client = MagicMock()
    client.component.update_component.side_effect = ApiError(
        "Failed to update component: HTTP 404", 404, None
    )
    with pytest.raises(Exception) as ei:
        _update_component_xml(client, "x", "<bns:Component/>")
    assert "Update failed" in str(ei.value) and "404" in str(ei.value)
