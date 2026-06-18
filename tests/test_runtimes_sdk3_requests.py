"""manage_runtimes builds SDK 3.0.1 request models that actually serialize.

Two manage_runtimes request builders were carried over from the Boomi SDK v2
era and passed keyword args that v3 no longer models. The v3 SDK silently
stuffs unknown kwargs into ``_kwargs`` and ``_map()`` drops them, so the calls
were transporting incomplete requests while the tool still reported success:

* Java upgrade passed ``JavaUpgrade(target_version=...)`` — v3 only accepts
  ``atom_id`` (the platform upgrades to the latest supported Java; a specific
  version cannot be requested), so the target version vanished from the wire.
* The quota bulk-get passed ``AccountCloudAttachmentQuotaBulkRequest(id_=...)``
  — v3 expects ``type_`` + a ``request`` list of ``BulkId`` models, so the
  request serialized to only its ``@type`` with zero IDs.

These tests pin the serialized wire shape (``_map()``) and assert nothing is
stranded in ``_kwargs``.
"""
from types import SimpleNamespace
from unittest.mock import MagicMock

from boomi.models import AccountCloudAttachmentQuotaBulkRequest, BulkId, JavaUpgrade
import boomi_mcp.categories.runtimes as runtimes


def test_java_upgrade_serializes_atom_id_only():
    """upgrade transports a JavaUpgrade carrying only the atom id, nothing dropped."""
    sdk = MagicMock()

    out = runtimes._action_configure_java(
        sdk, "work", resource_id="atom-1", java_action="upgrade"
    )

    assert out["_success"] is True
    create = sdk.java_upgrade.create_java_upgrade
    create.assert_called_once()
    body = create.call_args.kwargs["request_body"]
    assert isinstance(body, JavaUpgrade)
    assert body._map() == {"@type": "JavaUpgrade", "atomId": "atom-1"}
    assert body._kwargs == {}


def test_java_upgrade_rejects_target_version():
    """A specific target_version cannot be honored by v3 — reject, do not silently drop."""
    sdk = MagicMock()

    out = runtimes._action_configure_java(
        sdk, "work", resource_id="atom-1", java_action="upgrade", target_version="17"
    )

    assert out["_success"] is False
    assert "target_version" in out["error"]
    sdk.java_upgrade.create_java_upgrade.assert_not_called()


def test_quota_bulk_serializes_bulk_ids():
    """Bulk-get transports a GET request whose BulkId list carries every id."""
    sdk = MagicMock()
    sdk.account_cloud_attachment_quota.bulk_account_cloud_attachment_quota.return_value = (
        SimpleNamespace(result=[])
    )

    out = runtimes._action_list_account_cloud_attachment_quotas(
        sdk, "work", resource_ids=["q1", "q2"]
    )

    assert out["_success"] is True
    bulk = sdk.account_cloud_attachment_quota.bulk_account_cloud_attachment_quota
    bulk.assert_called_once()
    body = bulk.call_args.kwargs["request_body"]
    assert isinstance(body, AccountCloudAttachmentQuotaBulkRequest)
    assert body.type_ == "GET"
    assert [type(x).__name__ for x in body.request] == ["BulkId", "BulkId"]
    assert all(isinstance(x, BulkId) for x in body.request)
    assert body._map() == {
        "@type": "AccountCloudAttachmentQuotaBulkRequest",
        "request": [
            {"@type": "BulkId", "id": "q1"},
            {"@type": "BulkId", "id": "q2"},
        ],
        "type": "GET",
    }
    assert body._kwargs == {}
