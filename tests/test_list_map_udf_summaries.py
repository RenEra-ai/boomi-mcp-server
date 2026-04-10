"""Regression tests for BUG-14: list_map_udf_summaries environment_map_extension_id branch.

Verifies that _action_list_map_udf_summaries builds a query using
ENVIRONMENTMAPEXTENSIONID when environment_map_extension_id is provided,
and that all three filter parameters produce the correct expression property.
"""

import pytest
from types import SimpleNamespace
from unittest.mock import MagicMock

from boomi.models import (
    EnvironmentMapExtensionUserDefinedFunctionSummarySimpleExpressionProperty,
)

from boomi_mcp.categories.environments import _action_list_map_udf_summaries


def _make_sdk(summaries=None):
    sdk = MagicMock()
    result = SimpleNamespace(result=summaries or [], query_token=None)
    sdk.environment_map_extension_user_defined_function_summary \
        .query_environment_map_extension_user_defined_function_summary.return_value = result
    return sdk


def test_environment_map_extension_id_builds_correct_query():
    """The environment_map_extension_id branch must use ENVIRONMENTMAPEXTENSIONID."""
    sdk = _make_sdk()
    result = _action_list_map_udf_summaries(
        sdk, profile="dev",
        environment_map_extension_id="eme-123",
    )
    assert result["_success"] is True

    call_args = sdk.environment_map_extension_user_defined_function_summary \
        .query_environment_map_extension_user_defined_function_summary.call_args
    request_body = call_args.kwargs["request_body"]
    expr = request_body.query_filter.expression
    assert expr.property == EnvironmentMapExtensionUserDefinedFunctionSummarySimpleExpressionProperty.ENVIRONMENTMAPEXTENSIONID
    assert expr.argument == ["eme-123"]


def test_environment_id_builds_correct_query():
    sdk = _make_sdk()
    result = _action_list_map_udf_summaries(
        sdk, profile="dev",
        environment_id="env-456",
    )
    assert result["_success"] is True

    call_args = sdk.environment_map_extension_user_defined_function_summary \
        .query_environment_map_extension_user_defined_function_summary.call_args
    request_body = call_args.kwargs["request_body"]
    expr = request_body.query_filter.expression
    assert expr.property == EnvironmentMapExtensionUserDefinedFunctionSummarySimpleExpressionProperty.ENVIRONMENTID
    assert expr.argument == ["env-456"]


def test_extension_group_id_builds_correct_query():
    sdk = _make_sdk()
    result = _action_list_map_udf_summaries(
        sdk, profile="dev",
        extension_group_id="grp-789",
    )
    assert result["_success"] is True

    call_args = sdk.environment_map_extension_user_defined_function_summary \
        .query_environment_map_extension_user_defined_function_summary.call_args
    request_body = call_args.kwargs["request_body"]
    expr = request_body.query_filter.expression
    assert expr.property == EnvironmentMapExtensionUserDefinedFunctionSummarySimpleExpressionProperty.EXTENSIONGROUPID
    assert expr.argument == ["grp-789"]


def test_environment_map_extension_id_takes_priority():
    """When multiple filter params are given, environment_map_extension_id wins."""
    sdk = _make_sdk()
    result = _action_list_map_udf_summaries(
        sdk, profile="dev",
        environment_map_extension_id="eme-1",
        environment_id="env-2",
        extension_group_id="grp-3",
    )
    assert result["_success"] is True

    call_args = sdk.environment_map_extension_user_defined_function_summary \
        .query_environment_map_extension_user_defined_function_summary.call_args
    request_body = call_args.kwargs["request_body"]
    expr = request_body.query_filter.expression
    assert expr.property == EnvironmentMapExtensionUserDefinedFunctionSummarySimpleExpressionProperty.ENVIRONMENTMAPEXTENSIONID


def test_no_filter_returns_error():
    result = _action_list_map_udf_summaries(MagicMock(), profile="dev")
    assert result["_success"] is False
    assert "environment_map_extension_id" in result["error"]
