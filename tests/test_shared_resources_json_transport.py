"""Shared communication channel create/get/update use the SDK 3.0.1 JSON methods.

SDK 3.0.1 added first-class JSON create/get/update for
SharedCommunicationChannelComponent, so the MCP calls those directly instead of
hand-rolling a JSON transport. Non-2xx errors now propagate as ApiError to the
action router (``manage_shared_resources_action``). ``_channel_to_dict`` accepts
both JSON dicts and typed models; the update merge normalizes via
``_channel_to_wire_dict``.
"""
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

from boomi.models import SharedCommunicationChannelComponent
from boomi.net.transport.api_error import ApiError
import boomi_mcp.categories.shared_resources as sr


def _api_error(status, message):
    return ApiError(
        message=f"{status} error",
        status=status,
        response=SimpleNamespace(body={"message": message}),
    )


def test_create_channel_transports_model_as_json():
    sdk = MagicMock()
    create = sdk.shared_communication_channel_component.create_shared_communication_channel_component_json
    create.return_value = {"componentId": "ch-1", "componentName": "Ch1", "communicationType": "HTTP"}

    out = sr._action_create_channel(sdk, "work", name="Ch1", channel_type="HTTP")

    assert out["_success"] is True, out
    create.assert_called_once()
    assert isinstance(create.call_args[0][0], SharedCommunicationChannelComponent)
    assert out["channel"]["id"] == "ch-1"
    assert out["channel"]["type"] == "HTTP"


def test_create_channel_error_routes_through_router():
    sdk = MagicMock()
    create = sdk.shared_communication_channel_component.create_shared_communication_channel_component_json
    create.side_effect = _api_error(400, "bad")

    out = sr.manage_shared_resources_action(
        sdk, "work", "create_channel", config_data={"name": "Ch1", "channel_type": "HTTP"}
    )

    assert out["_success"] is False
    assert "bad" in out["error"]
    assert out["exception_type"] == "ApiError"


def test_get_channel_reads_json_dict():
    sdk = MagicMock()
    sdk.shared_communication_channel_component.get_shared_communication_channel_component_json.return_value = {
        "componentId": "ch-1", "componentName": "Ch1", "communicationType": "FTP", "folderName": "Home",
    }

    out = sr._action_get_channel(sdk, "work", resource_id="ch-1")

    assert out["_success"] is True
    assert out["channel"]["id"] == "ch-1"
    assert out["channel"]["type"] == "FTP"
    assert out["channel"]["folder_name"] == "Home"


def test_update_channel_get_then_post():
    sdk = MagicMock()
    resp = {"componentId": "ch-1", "componentName": "Ch1", "communicationType": "HTTP",
            "PartnerCommunication": {}, "PartnerArchiving": {}}
    update = sdk.shared_communication_channel_component.update_shared_communication_channel_component_json
    update.return_value = resp

    # The update merge reads the existing channel via the lossless raw-JSON GET.
    with patch.object(sr, "_get_channel_raw_json", return_value=dict(resp)) as raw_get:
        out = sr._action_update_channel(sdk, "work", resource_id="ch-1", name="Ch1b")

    assert out["_success"] is True, out
    raw_get.assert_called_once()
    update.assert_called_once()
    # The merged wire dict is POSTed back with the rename applied.
    posted = update.call_args[0][1]
    assert posted["componentName"] == "Ch1b"


def test_update_channel_is_lossless():
    """A metadata-only update must POST the FULL existing document, preserving
    nested protocol config the generated model drops on a _map() round-trip
    (e.g. HTTPCommunicationOptions.sharedClientSSLCertificate) and unmodeled keys
    (folderFullPath). Regression guard for the channel update data-loss finding.
    """
    sdk = MagicMock()
    full_doc = {
        "componentId": "ch-1",
        "componentName": "Ch1",
        "communicationType": "HTTP",
        "folderFullPath": "Home/Sub",
        "PartnerArchiving": {"enableArchiving": False},
        "PartnerCommunication": {
            "HTTPCommunicationOptions": {
                "HTTPSettings": {"url": "https://ex"},
                "sharedClientSSLCertificate": {"clientauthEnabled": True},
            }
        },
    }
    update = sdk.shared_communication_channel_component.update_shared_communication_channel_component_json
    update.return_value = full_doc

    with patch.object(sr, "_get_channel_raw_json", return_value=dict(full_doc)):
        out = sr._action_update_channel(sdk, "work", resource_id="ch-1", description="new desc")

    assert out["_success"] is True, out
    posted = update.call_args[0][1]
    # Nested SSL-cert config and the unmodeled folderFullPath survive GET->mutate->POST.
    http_opts = posted["PartnerCommunication"]["HTTPCommunicationOptions"]
    assert http_opts["sharedClientSSLCertificate"]["clientauthEnabled"] is True
    assert posted["folderFullPath"] == "Home/Sub"


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


def test_channel_to_dict_preserves_unmodeled_folder_full_path():
    """get_*_json hydrates to a model; folderFullPath is NOT a modeled attribute,
    so it lands in _kwargs. _channel_to_dict must still surface it (the old raw-dict
    display path did) — regression guard for the hydrated-display data-loss finding.
    """
    model = SharedCommunicationChannelComponent._unmap({
        "componentId": "ch-1",
        "componentName": "Ch1",
        "communicationType": "HTTP",
        "folderName": "Home",
        "folderFullPath": "Home/Sub",
        "PartnerArchiving": {},
        "PartnerCommunication": {},
    })
    assert "folderFullPath" in getattr(model, "_kwargs", {})  # precondition: unmodeled
    d = sr._channel_to_dict(model)
    assert d["folder_full_path"] == "Home/Sub"
    assert d["folder_name"] == "Home"
