"""Trading partner create/get/update keep JSON under SDK 3.0.0.

TradingPartnerComponent accepts JSON; SDK 3.0.0's typed create/get/update are
XML-only, so the MCP keeps building/reading the typed model and transports JSON
via ``component_family_json_request``. These verify a typed *model* (never raw
XML) is transported, responses hydrate back into a model, the dead
``bulk_create_trading_partners`` wrapper is gone, and the parent-component lookup
uses the SDK-backed ``component_get_xml`` helper.
"""
from unittest.mock import MagicMock, patch

from boomi.models import TradingPartnerComponent
import boomi_mcp.categories.components.trading_partners as tp


def test_bulk_create_wrapper_is_removed():
    assert not hasattr(tp, "bulk_create_trading_partners")


def test_create_transports_model_as_json():
    calls = {}

    def fake(service, path, method="POST", body=None, body_content_type="application/json"):
        calls.update(path=path, method=method, body=body)
        return {"componentId": "tp-1", "componentName": "TP1", "standard": "x12",
                "classification": "tradingpartner"}, 200

    with patch.object(tp, "component_family_json_request", fake):
        out = tp.create_trading_partner(MagicMock(), "work", {"component_name": "TP1", "standard": "x12"})
    assert out["_success"] is True, out
    assert calls["path"] == "TradingPartnerComponent"
    assert calls["method"] == "POST"
    # The typed model is transported — never raw XML / a plain dict.
    assert isinstance(calls["body"], TradingPartnerComponent)
    assert out["trading_partner"]["component_id"] == "tp-1"


def test_create_error_status_maps_to_failure():
    with patch.object(tp, "component_family_json_request", lambda *a, **k: ({"message": "no B2B"}, 400)):
        out = tp.create_trading_partner(MagicMock(), "work", {"component_name": "TP1", "standard": "x12"})
    assert out["_success"] is False
    assert "no B2B" in out["error"]


def test_get_hydrates_and_returns_details():
    resp = {"componentId": "tp-1", "componentName": "TP1", "standard": "x12",
            "classification": "tradingpartner"}
    with patch.object(tp, "component_family_json_request", lambda *a, **k: (resp, 200)):
        out = tp.get_trading_partner(MagicMock(), "work", "tp-1")
    assert out["_success"] is True, out


def test_update_does_get_then_post():
    seq = []
    resp = {"componentId": "tp-1", "componentName": "TP1", "standard": "x12",
            "classification": "tradingpartner"}

    def fake(service, path, method="POST", body=None, body_content_type="application/json"):
        seq.append(method)
        return resp, 200

    with patch.object(tp, "component_family_json_request", fake):
        out = tp.update_trading_partner(MagicMock(), "work", "tp-1", {"component_name": "TP1b"})
    assert out["_success"] is True, out
    assert seq[0] == "GET" and seq[-1] == "POST"
    assert "component_name" in out["trading_partner"]["updated_fields"]


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

    def fake_json(service, path, method="POST", body=None, body_content_type="application/json"):
        return {"componentId": "tp-1", "componentName": "TP1"}, 200

    with patch.object(tp, "component_family_json_request", fake_json), \
         patch.object(tp, "component_get_xml", return_value={"name": "ParentProc", "type": "process"}) as cgx:
        out = tp.analyze_trading_partner_usage(client, "work", "tp-1")
    assert out["_success"] is True, out
    cgx.assert_called_once()
    refs = out["trading_partner"]["referenced_by"] if "referenced_by" in out.get("trading_partner", {}) else out.get("referenced_by")
    # Parent resolved with name/type from component_get_xml
    assert any(r.get("name") == "ParentProc" for r in (refs or []))
