"""Tests for list_process_environment_attachments action.

Verifies that the handler builds the correct query expression for each
filter branch (process_id, environment_id, unfiltered) and pages through
query_token results.
"""

import pytest
from types import SimpleNamespace
from unittest.mock import MagicMock

from boomi.models import (
    ProcessEnvironmentAttachmentSimpleExpressionOperator,
    ProcessEnvironmentAttachmentSimpleExpressionProperty,
)

from boomi_mcp.categories.deployment.packages import manage_deployment_action


def _make_sdk(attachments=None, query_token=None):
    sdk = MagicMock()
    result = SimpleNamespace(result=attachments or [], query_token=query_token)
    sdk.deployment.query_process_environment_attachment.return_value = result
    sdk.deployment.query_more_process_environment_attachment.return_value = SimpleNamespace(
        result=[], query_token=None
    )
    return sdk


def test_filter_by_process_id():
    sdk = _make_sdk()
    result = manage_deployment_action(
        sdk, profile="dev", action="list_process_environment_attachments",
        config_data={"process_id": "proc-111"},
    )
    assert result["_success"] is True

    call_args = sdk.deployment.query_process_environment_attachment.call_args
    request_body = call_args.kwargs["request_body"]
    expr = request_body.query_filter.expression
    assert expr.property == ProcessEnvironmentAttachmentSimpleExpressionProperty.PROCESSID
    assert expr.operator == ProcessEnvironmentAttachmentSimpleExpressionOperator.EQUALS
    assert expr.argument == ["proc-111"]


def test_filter_by_environment_id():
    sdk = _make_sdk()
    result = manage_deployment_action(
        sdk, profile="dev", action="list_process_environment_attachments",
        config_data={"environment_id": "env-222"},
    )
    assert result["_success"] is True

    call_args = sdk.deployment.query_process_environment_attachment.call_args
    request_body = call_args.kwargs["request_body"]
    expr = request_body.query_filter.expression
    assert expr.property == ProcessEnvironmentAttachmentSimpleExpressionProperty.ENVIRONMENTID
    assert expr.operator == ProcessEnvironmentAttachmentSimpleExpressionOperator.EQUALS
    assert expr.argument == ["env-222"]


def test_unfiltered_uses_is_not_null():
    sdk = _make_sdk()
    result = manage_deployment_action(
        sdk, profile="dev", action="list_process_environment_attachments",
    )
    assert result["_success"] is True

    call_args = sdk.deployment.query_process_environment_attachment.call_args
    request_body = call_args.kwargs["request_body"]
    expr = request_body.query_filter.expression
    assert expr.property == ProcessEnvironmentAttachmentSimpleExpressionProperty.PROCESSID
    assert expr.operator == ProcessEnvironmentAttachmentSimpleExpressionOperator.ISNOTNULL


def test_pagination_follows_query_token():
    page1_item = SimpleNamespace(id="att-1", process_id="p1", environment_id="e1")
    page2_item = SimpleNamespace(id="att-2", process_id="p2", environment_id="e2")

    sdk = MagicMock()
    sdk.deployment.query_process_environment_attachment.return_value = SimpleNamespace(
        result=[page1_item], query_token="token-abc"
    )
    sdk.deployment.query_more_process_environment_attachment.return_value = SimpleNamespace(
        result=[page2_item], query_token=None
    )

    result = manage_deployment_action(
        sdk, profile="dev", action="list_process_environment_attachments",
    )
    assert result["_success"] is True
    assert result["total_count"] == 2
    sdk.deployment.query_more_process_environment_attachment.assert_called_once_with(
        request_body="token-abc"
    )
