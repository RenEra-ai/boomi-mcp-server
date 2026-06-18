"""Trading partner create/get/update use the SDK 3.0.1 JSON methods.

SDK 3.0.1 added first-class JSON create/get/update for TradingPartnerComponent,
so the MCP calls those directly instead of hand-rolling a JSON transport. These
verify a typed *model* is passed to create, dict responses hydrate back into a
model (TradingPartnerComponent._unmap is root-tolerant), a non-2xx ApiError maps
to the failure envelope, the dead ``bulk_create_trading_partners`` wrapper is
gone, and the parent-component lookup uses the SDK-backed ``component_get_xml``.
"""
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

from boomi.models import TradingPartnerComponent
from boomi.net.transport.api_error import ApiError
import boomi_mcp.categories.components.trading_partners as tp

_TP_JSON = {
    "componentId": "tp-1",
    "componentName": "TP1",
    "standard": "x12",
    "classification": "tradingpartner",
}


def _api_error(status, message):
    return ApiError(
        message=f"{status} error",
        status=status,
        response=SimpleNamespace(body={"message": message}),
    )


def test_bulk_create_wrapper_is_removed():
    assert not hasattr(tp, "bulk_create_trading_partners")


def test_create_transports_model_as_json():
    client = MagicMock()
    client.trading_partner_component.create_trading_partner_component_json.return_value = dict(_TP_JSON)

    out = tp.create_trading_partner(client, "work", {"component_name": "TP1", "standard": "x12"})

    assert out["_success"] is True, out
    assert out["trading_partner"]["component_id"] == "tp-1"
    create = client.trading_partner_component.create_trading_partner_component_json
    create.assert_called_once()
    # The typed model is transported — never raw XML / a plain dict.
    assert isinstance(create.call_args[0][0], TradingPartnerComponent)


def test_create_error_maps_to_failure():
    client = MagicMock()
    client.trading_partner_component.create_trading_partner_component_json.side_effect = _api_error(400, "no B2B")

    out = tp.create_trading_partner(client, "work", {"component_name": "TP1", "standard": "x12"})

    assert out["_success"] is False
    assert "no B2B" in out["error"]


def test_get_hydrates_and_returns_details():
    client = MagicMock()
    client.trading_partner_component.get_trading_partner_component_json.return_value = dict(_TP_JSON)

    out = tp.get_trading_partner(client, "work", "tp-1")

    assert out["_success"] is True, out


def test_update_does_get_then_post():
    client = MagicMock()
    client.trading_partner_component.get_trading_partner_component_json.return_value = dict(_TP_JSON)
    client.trading_partner_component.update_trading_partner_component_json.return_value = dict(_TP_JSON)

    out = tp.update_trading_partner(client, "work", "tp-1", {"component_name": "TP1b"})

    assert out["_success"] is True, out
    client.trading_partner_component.get_trading_partner_component_json.assert_called_once()
    client.trading_partner_component.update_trading_partner_component_json.assert_called_once()
    assert "component_name" in out["trading_partner"]["updated_fields"]


def test_update_unknown_id_returns_not_found_envelope():
    client = MagicMock()
    client.trading_partner_component.get_trading_partner_component_json.side_effect = _api_error(404, "nope")

    out = tp.update_trading_partner(client, "work", "missing", {"component_name": "X"})

    assert out["_success"] is False
    assert "Component not found" in out["error"]
    client.trading_partner_component.update_trading_partner_component_json.assert_not_called()


def test_analyze_uses_component_get_xml_for_parent():
    # One reference whose parent should be resolved via component_get_xml (not a
    # raw Serializer GET).
    ref = MagicMock()
    ref.parent_component_id = "parent-1"
    ref.parent_version = 2
    result_item = MagicMock()
    result_item.references = [ref]
    query_result = MagicMock()
    query_result.result = [result_item]

    client = MagicMock()
    client.component_reference.query_component_reference.return_value = query_result
    client.trading_partner_component.get_trading_partner_component_json.return_value = {
        "componentId": "tp-1", "componentName": "TP1",
    }

    with patch.object(tp, "component_get_xml", return_value={"name": "ParentProc", "type": "process"}) as cgx:
        out = tp.analyze_trading_partner_usage(client, "work", "tp-1")

    assert out["_success"] is True, out
    cgx.assert_called_once()
    refs = out["trading_partner"]["referenced_by"] if "referenced_by" in out.get("trading_partner", {}) else out.get("referenced_by")
    # Parent resolved with name/type from component_get_xml
    assert any(r.get("name") == "ParentProc" for r in (refs or []))
