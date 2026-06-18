"""Shared communication channel create/get/update keep JSON under SDK 3.0.0.

The SharedCommunicationChannelComponent endpoint accepts JSON; SDK 3.0.0's typed
create/get/update are XML-only, so the MCP keeps building the typed model and
transports JSON via ``component_family_json_request``. ``_channel_to_dict`` now
accepts both JSON dicts (transport path) and typed models (query path).
"""
from unittest.mock import MagicMock, patch

from boomi.models import SharedCommunicationChannelComponent
import boomi_mcp.categories.shared_resources as sr


def test_create_channel_transports_model_as_json():
    calls = {}

    def fake(service, path, method="POST", body=None, body_content_type="application/json"):
        calls.update(path=path, method=method, body=body)
        return {"componentId": "ch-1", "componentName": "Ch1", "communicationType": "HTTP"}, 200

    with patch.object(sr, "component_family_json_request", fake):
        out = sr._action_create_channel(MagicMock(), "work", name="Ch1", channel_type="HTTP")
    assert out["_success"] is True, out
    assert calls["path"] == "SharedCommunicationChannelComponent"
    assert calls["method"] == "POST"
    assert isinstance(calls["body"], SharedCommunicationChannelComponent)
    assert out["channel"]["id"] == "ch-1"
    assert out["channel"]["type"] == "HTTP"


def test_create_channel_error_status():
    with patch.object(sr, "component_family_json_request", lambda *a, **k: ({"message": "bad"}, 400)):
        out = sr._action_create_channel(MagicMock(), "work", name="Ch1", channel_type="HTTP")
    assert out["_success"] is False
    assert "bad" in out["error"]


def test_get_channel_reads_json_dict():
    resp = {"componentId": "ch-1", "componentName": "Ch1", "communicationType": "FTP", "folderName": "Home"}
    with patch.object(sr, "component_family_json_request", lambda *a, **k: (resp, 200)):
        out = sr._action_get_channel(MagicMock(), "work", resource_id="ch-1")
    assert out["_success"] is True
    assert out["channel"]["id"] == "ch-1"
    assert out["channel"]["type"] == "FTP"
    assert out["channel"]["folder_name"] == "Home"


def test_update_channel_get_then_post():
    seq = []
    resp = {"componentId": "ch-1", "componentName": "Ch1", "communicationType": "HTTP",
            "PartnerCommunication": {}, "PartnerArchiving": {}}

    def fake(service, path, method="POST", body=None, body_content_type="application/json"):
        seq.append(method)
        return resp, 200

    with patch.object(sr, "component_family_json_request", fake):
        out = sr._action_update_channel(MagicMock(), "work", resource_id="ch-1", name="Ch1b")
    assert out["_success"] is True, out
    assert seq[0] == "GET" and seq[-1] == "POST"


def test_channel_to_dict_accepts_typed_model():
    # The query/list path still yields typed models — _channel_to_dict must handle them.
    model = SharedCommunicationChannelComponent(
        partner_archiving=sr.PartnerArchiving(),
        partner_communication=sr.PartnerCommunication(),
        component_name="Ch1",
        communication_type="HTTP",
    )
    model.component_id = "ch-1"
    d = sr._channel_to_dict(model)
    assert d["id"] == "ch-1"
    assert d["name"] == "Ch1"
