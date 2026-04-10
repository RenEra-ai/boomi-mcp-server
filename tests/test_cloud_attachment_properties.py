"""Regression tests for BUG-17: allowlist drops legitimate queue properties.

Verifies that _CLOUD_ATTACHMENT_PROPERTY_FIELDS includes all SDK model fields
except session_id and status_code, preventing silent data loss in
get/update_account_cloud_attachment_properties handlers.
"""

import pytest
from types import SimpleNamespace
from unittest.mock import MagicMock

from boomi.models import AccountCloudAttachmentProperties

from boomi_mcp.categories.runtimes import (
    _CLOUD_ATTACHMENT_PROPERTY_FIELDS,
    _action_get_account_cloud_attachment_properties,
    _action_update_account_cloud_attachment_properties,
)


# Fields that are SDK metadata, not real properties
_EXCLUDED_METADATA = {'session_id', 'status_code'}


def _sdk_model_fields():
    """Return the set of real fields from the SDK model."""
    mapping = AccountCloudAttachmentProperties._JsonMap__json_mapping  # noqa: SLF001
    return {py_name for py_name in mapping if py_name not in _EXCLUDED_METADATA}


def test_allowlist_matches_sdk_model():
    """The allowlist must include every SDK field except session_id/status_code."""
    expected = _sdk_model_fields()
    missing = expected - _CLOUD_ATTACHMENT_PROPERTY_FIELDS
    extra = _CLOUD_ATTACHMENT_PROPERTY_FIELDS - expected
    assert not missing, f"Allowlist is missing SDK fields: {missing}"
    assert not extra, f"Allowlist has fields not in SDK model: {extra}"


def test_get_handler_returns_queue_fields():
    """get handler must serialize queue_* fields from the async response."""
    sdk = MagicMock()
    # Build a mock item with all queue fields set
    item = SimpleNamespace(
        queue_commit_batch_limit=100,
        queue_max_batch_size=200,
        queue_max_doc_size=300,
        queue_msg_throttle_rate=50,
        queue_use_file_persistence=True,
        queue_incoming_message_rate_limit=500,
    )
    item.__getattr__ = lambda name: None  # other fields -> None

    # initiate_fn returns an object with .async_token.token
    initiate_result = SimpleNamespace(async_token=SimpleNamespace(token="tok-1"))
    sdk.account_cloud_attachment_properties.async_get_account_cloud_attachment_properties.return_value = initiate_result
    # poll_fn returns the async result with .result list
    poll_result = SimpleNamespace(result=[item])
    sdk.account_cloud_attachment_properties.async_token_account_cloud_attachment_properties.return_value = poll_result

    result = _action_get_account_cloud_attachment_properties(sdk, profile="dev", resource_id="atom-123")
    assert result["_success"] is True
    props = result["properties"]
    assert props["queue_commit_batch_limit"] == 100
    assert props["queue_max_batch_size"] == 200
    assert props["queue_max_doc_size"] == 300
    assert props["queue_msg_throttle_rate"] == 50
    assert props["queue_use_file_persistence"] is True
    assert props["queue_incoming_message_rate_limit"] == 500


def test_update_handler_returns_queue_fields():
    """update handler must serialize queue_* fields from the response."""
    sdk = MagicMock()
    result_obj = SimpleNamespace(
        queue_commit_batch_limit=150,
        queue_max_batch_size=250,
        queue_max_doc_size=350,
        queue_msg_throttle_rate=75,
        queue_use_file_persistence=False,
    )
    result_obj.__getattr__ = lambda name: None

    sdk.account_cloud_attachment_properties.update_account_cloud_attachment_properties.return_value = result_obj

    result = _action_update_account_cloud_attachment_properties(
        sdk, profile="dev", resource_id="atom-456",
        request_body={"queue_commit_batch_limit": 150},
    )
    assert result["_success"] is True
    props = result["properties"]
    assert props["queue_commit_batch_limit"] == 150
    assert props["queue_max_batch_size"] == 250
    assert props["queue_max_doc_size"] == 350
    assert props["queue_msg_throttle_rate"] == 75
    assert props["queue_use_file_persistence"] is False
