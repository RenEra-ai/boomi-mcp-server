"""Regression test for BUG-04: _action_pg_update request-body ID mapping.

Verifies that the outgoing request body serializes componentId into the
dict that the SDK sends to the API. The fix assigns component_id post-init;
this test calls _map() on the captured body to prove the value survives
serialization through JsonMap.
"""

from unittest.mock import MagicMock

from boomi.models import TradingPartnerProcessingGroup

from boomi_mcp.categories.components.trading_partners import _action_pg_update


def _make_sdk():
    sdk = MagicMock()
    sdk.trading_partner_processing_group.update_trading_partner_processing_group.return_value = MagicMock(
        component_id="PG-123",
        component_name="Updated PG",
        deleted=False,
        description="desc",
        folder_id=None,
        folder_name=None,
        branch_id=None,
        branch_name=None,
    )
    return sdk


def test_update_body_has_component_id():
    sdk = _make_sdk()
    result = _action_pg_update(
        sdk, profile="dev", resource_id="PG-123", component_name="Updated PG"
    )

    assert result["_success"] is True
    call_args = sdk.trading_partner_processing_group.update_trading_partner_processing_group.call_args
    body = call_args.kwargs["request_body"]

    assert isinstance(body, TradingPartnerProcessingGroup)
    serialized = body._map()
    assert serialized["componentId"] == "PG-123"


def test_update_missing_resource_id():
    sdk = _make_sdk()
    result = _action_pg_update(sdk, profile="dev", component_name="No ID")
    assert result["_success"] is False
    assert "resource_id" in result["error"]


def test_update_no_fields():
    sdk = _make_sdk()
    result = _action_pg_update(sdk, profile="dev", resource_id="PG-123")
    assert result["_success"] is False
    assert "No valid update fields" in result["error"]
