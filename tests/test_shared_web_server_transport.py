"""Why SharedWebServer get/update use the SDK's lossless JSON *dict* methods.

The typed ``SharedWebServer`` model is LOSSY for cloud runtimes:
``SharedWebServerCloudTennantGeneral`` omits real fields (externalHost,
internalHost, sslCertificate, maxNumberOfThreads), so a typed GET -> ``_map()`` ->
update round-trip silently strips them (data loss on update). SDK 3.0.1 added the
lossless ``get_shared_web_server_json`` / ``update_shared_web_server_json`` *dict*
methods for exactly this, and the MCP routes web-server get/update through them
(it no longer hand-rolls a raw JSON transport). These tests pin the lossy-model
fact (the reason) and that untouched cloud fields survive a GET -> mutate -> POST
through the SDK dict methods (the fix).
"""
import inspect
from unittest.mock import MagicMock

from boomi.models import (
    SharedWebServer,
    SharedWebServerCloudTennantGeneral,
)
import boomi_mcp.categories.shared_resources as sr

_CLOUD_DROPPED_FIELDS = ("external_host", "internal_host", "ssl_certificate", "max_number_of_threads")


def test_sdk_cloud_tenant_model_omits_real_fields():
    params = set(inspect.signature(SharedWebServerCloudTennantGeneral.__init__).parameters)
    still_missing = [f for f in _CLOUD_DROPPED_FIELDS if f not in params]
    assert still_missing == list(_CLOUD_DROPPED_FIELDS), (
        "SharedWebServerCloudTennantGeneral now defines "
        f"{[f for f in _CLOUD_DROPPED_FIELDS if f in params]!r} — the cloud-tenant "
        "model may be complete; the SDK lossless dict methods may no longer be "
        "needed for these fields."
    )


def test_typed_roundtrip_drops_cloud_fields():
    """A typed GET->_map() round-trip would lose externalHost/maxNumberOfThreads."""
    cloud = {
        "atomId": "a",
        "cloudTennantGeneral": {
            "apiType": "basic",
            "baseUrl": "https://x",
            "externalHost": "host.example",
            "maxNumberOfThreads": 10,
            "listenerPorts": {"port": [{"port": 9090, "ssl": True}]},
        },
    }
    rt = SharedWebServer._unmap(dict(cloud, cloudTennantGeneral=dict(cloud["cloudTennantGeneral"])))._map()
    ctg = rt.get("cloudTennantGeneral", {})
    # Proof of the data loss the lossless dict methods avoid:
    assert "externalHost" not in ctg
    assert "maxNumberOfThreads" not in ctg
    # Sanity: fields the model DOES define survive.
    assert ctg.get("baseUrl") == "https://x"


def test_mcp_get_uses_sdk_json_dict_method():
    sdk = MagicMock()
    sdk.shared_web_server.get_shared_web_server_json.return_value = {"atomId": "a"}

    out = sr._action_list_web_servers(sdk, "work", resource_id="a")

    assert out["_success"] is True
    sdk.shared_web_server.get_shared_web_server_json.assert_called_once_with("a")
    # The MCP must NOT hydrate the typed (lossy) SharedWebServer for get/update.
    sdk.shared_web_server.get_shared_web_server.assert_not_called()


def test_mcp_update_posts_full_dict_preserving_cloud_fields():
    sdk = MagicMock()
    doc = {
        "generalSettings": {
            "baseUrl": "https://x",
            "externalHost": "host.example",
            "maxNumberOfThreads": 5,
        }
    }
    sdk.shared_web_server.get_shared_web_server_json.return_value = {
        k: dict(v) for k, v in doc.items()
    }
    sdk.shared_web_server.update_shared_web_server_json.side_effect = lambda _rid, body: body

    out = sr._action_update_web_server(sdk, "work", resource_id="a", base_url="https://y")

    assert out["_success"] is True, out
    update = sdk.shared_web_server.update_shared_web_server_json
    update.assert_called_once()
    posted_gs = update.call_args[0][1]["generalSettings"]
    # The lossy cloud fields are preserved through GET -> mutate -> POST (the
    # whole point of the lossless JSON dict method); the edited field is applied.
    assert posted_gs["externalHost"] == "host.example"
    assert posted_gs["maxNumberOfThreads"] == 5
    assert posted_gs["baseUrl"] == "https://y"
    # The typed (lossy) update must NOT be used.
    sdk.shared_web_server.update_shared_web_server.assert_not_called()
