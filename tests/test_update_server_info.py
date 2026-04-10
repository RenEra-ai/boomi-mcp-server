"""Regression test for BUG-05: _action_update_server_info atom_id serialization.

Verifies that the outgoing request body serializes atomId into the dict
that the SDK sends to the API. The fix assigns atom_id post-init; this
test calls _map() on the captured body to prove the value survives
serialization through JsonMap, not just that the Python attribute is set.
"""

from unittest.mock import MagicMock

from boomi.models import SharedServerInformation

from boomi_mcp.categories.shared_resources import _action_update_server_info


def _make_sdk():
    sdk = MagicMock()
    sdk.shared_server_information.update_shared_server_information.return_value = MagicMock(
        atom_id="ATOM-456",
        api_type="ADVANCED",
        auth="BASIC",
        url="https://example.com",
    )
    return sdk


def test_update_body_has_atom_id():
    sdk = _make_sdk()
    result = _action_update_server_info(
        sdk, profile="dev", resource_id="ATOM-456", api_type="advanced"
    )

    assert result["_success"] is True
    call_args = sdk.shared_server_information.update_shared_server_information.call_args
    body = call_args.kwargs["request_body"]

    assert isinstance(body, SharedServerInformation)
    # Exercise the actual SDK serialization path (_map via JsonMap)
    serialized = body._map()
    assert serialized["atomId"] == "ATOM-456"


def test_update_missing_resource_id():
    sdk = _make_sdk()
    result = _action_update_server_info(sdk, profile="dev", api_type="advanced")
    assert result["_success"] is False
    assert "resource_id" in result["error"]


def test_update_no_fields():
    sdk = _make_sdk()
    result = _action_update_server_info(sdk, profile="dev", resource_id="ATOM-456")
    assert result["_success"] is False
    assert "No valid update fields" in result["error"]
