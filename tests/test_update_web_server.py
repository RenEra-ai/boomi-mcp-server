"""Regression test for BUG-27: max_threads alias in _action_update_web_server.

Verifies that passing max_threads= writes generalSettings.maxNumberOfThreads
in the outgoing PUT body, so the short alias cannot silently regress.
"""

from unittest.mock import MagicMock, patch

from boomi_mcp.categories.shared_resources import _action_update_web_server


def _fake_raw(current_doc):
    """Return a _raw_web_server_request stub that captures the PUT body."""
    calls = []

    def _stub(_sdk, _rid, method="GET", body=None):
        if method == "GET":
            return current_doc
        calls.append(body)
        return body  # echo back as the "updated" response

    return _stub, calls


_MINIMAL_DOC = {
    "generalSettings": {
        "baseUrl": "https://example.com",
        "maxNumberOfThreads": 20,
    },
}


@patch("boomi_mcp.categories.shared_resources._raw_web_server_request")
@patch("boomi_mcp.categories.shared_resources._web_server_to_dict", side_effect=lambda d: d)
def test_max_threads_alias_maps_correctly(mock_to_dict, mock_raw):
    """max_threads kwarg must write generalSettings.maxNumberOfThreads."""
    stub, calls = _fake_raw({**_MINIMAL_DOC, "generalSettings": {**_MINIMAL_DOC["generalSettings"]}})
    mock_raw.side_effect = stub

    result = _action_update_web_server(
        MagicMock(), profile="dev", resource_id="ATOM-1", max_threads=50,
    )

    assert result["_success"] is True
    assert calls, "Expected a POST call with updated body"
    gs = calls[0]["generalSettings"]
    assert gs["maxNumberOfThreads"] == 50
    assert "max_threads" in result["updated_fields"]


@patch("boomi_mcp.categories.shared_resources._raw_web_server_request")
@patch("boomi_mcp.categories.shared_resources._web_server_to_dict", side_effect=lambda d: d)
def test_max_number_of_threads_still_works(mock_to_dict, mock_raw):
    """Original long-form key must still work alongside the alias."""
    stub, calls = _fake_raw({**_MINIMAL_DOC, "generalSettings": {**_MINIMAL_DOC["generalSettings"]}})
    mock_raw.side_effect = stub

    result = _action_update_web_server(
        MagicMock(), profile="dev", resource_id="ATOM-1", max_number_of_threads=100,
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
