"""Justification for keeping the SharedWebServer JSON transport.

A code review proposed replacing ``_raw_web_server_request`` with the SDK's typed
``get_shared_web_server`` / ``update_shared_web_server``. SDK 3.0.0 did fix the old
``_unmap`` TypeError (the read model now tolerates sparse GETs), but the typed
path is still LOSSY for **cloud** runtimes: ``SharedWebServerCloudTennantGeneral``
omits real fields, so a typed GET -> ``_map()`` -> update would silently strip
them (data loss on update). These tests pin that fact; if the cloud-tenant model
ever gains the fields, the first test fails as a prompt to re-evaluate migrating
to the typed SDK methods.
"""
import inspect

from boomi.models import (
    SharedWebServer,
    SharedWebServerCloudTennantGeneral,
)

_CLOUD_DROPPED_FIELDS = ("external_host", "internal_host", "ssl_certificate", "max_number_of_threads")


def test_sdk_cloud_tenant_model_omits_real_fields():
    params = set(inspect.signature(SharedWebServerCloudTennantGeneral.__init__).parameters)
    still_missing = [f for f in _CLOUD_DROPPED_FIELDS if f not in params]
    assert still_missing == list(_CLOUD_DROPPED_FIELDS), (
        "SharedWebServerCloudTennantGeneral now defines "
        f"{[f for f in _CLOUD_DROPPED_FIELDS if f in params]!r} — the cloud-tenant "
        "model may be complete; re-evaluate moving SharedWebServer get/update to "
        "the typed SDK methods (and dropping the JSON transport)."
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
    # Proof of the data loss the raw JSON transport avoids:
    assert "externalHost" not in ctg
    assert "maxNumberOfThreads" not in ctg
    # Sanity: fields the model DOES define survive.
    assert ctg.get("baseUrl") == "https://x"
