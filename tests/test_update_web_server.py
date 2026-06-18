"""Regression test for BUG-27: max_threads alias in _action_update_web_server.

Verifies that passing max_threads= writes generalSettings.maxNumberOfThreads in
the outgoing update body, so the short alias cannot silently regress. The MCP now
routes the GET -> modify -> POST through the SDK 3.0.1 lossless JSON dict methods
(``get_shared_web_server_json`` / ``update_shared_web_server_json``).
"""

from unittest.mock import MagicMock, patch

from boomi_mcp.categories.shared_resources import _action_update_web_server


def _fake_sdk(current_doc):
    """Return a MagicMock sdk whose SharedWebServer JSON GET returns ``current_doc``
    and whose update echoes (and captures) the POSTed body."""
    sdk = MagicMock()
    calls = []
    sdk.shared_web_server.get_shared_web_server_json.return_value = current_doc

    def _update(_rid, body):
        calls.append(body)
        return body  # echo back as the "updated" response

    sdk.shared_web_server.update_shared_web_server_json.side_effect = _update
    return sdk, calls


_MINIMAL_DOC = {
    "generalSettings": {
        "baseUrl": "https://example.com",
        "maxNumberOfThreads": 20,
    },
}


@patch("boomi_mcp.categories.shared_resources._web_server_to_dict", side_effect=lambda d: d)
def test_max_threads_alias_maps_correctly(mock_to_dict):
    """max_threads kwarg must write generalSettings.maxNumberOfThreads."""
    sdk, calls = _fake_sdk({"generalSettings": {**_MINIMAL_DOC["generalSettings"]}})

    result = _action_update_web_server(
        sdk, profile="dev", resource_id="ATOM-1", max_threads=50,
    )

    assert result["_success"] is True
    assert calls, "Expected a POST call with updated body"
    gs = calls[0]["generalSettings"]
    assert gs["maxNumberOfThreads"] == 50
    assert "max_threads" in result["updated_fields"]


@patch("boomi_mcp.categories.shared_resources._web_server_to_dict", side_effect=lambda d: d)
def test_max_number_of_threads_still_works(mock_to_dict):
    """Original long-form key must still work alongside the alias."""
    sdk, calls = _fake_sdk({"generalSettings": {**_MINIMAL_DOC["generalSettings"]}})

    result = _action_update_web_server(
        sdk, profile="dev", resource_id="ATOM-1", max_number_of_threads=100,
    )

    assert result["_success"] is True
    gs = calls[0]["generalSettings"]
    assert gs["maxNumberOfThreads"] == 100


def test_no_fields_error_mentions_max_threads():
    """Help text should mention the max_threads alias."""
    result = _action_update_web_server(
        MagicMock(), profile="dev", resource_id="ATOM-1",
    )

    assert result["_success"] is False
    assert "max_threads" in result["error"]
