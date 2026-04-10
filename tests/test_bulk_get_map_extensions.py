"""Regression test for BUG-10: _action_bulk_get_map_extensions mixed response.

A bulk response can contain items with statusCode and errorMessage but no
Result (e.g. when an ID is not found).  The handler must return both
successful and error entries without raising.
"""

from types import SimpleNamespace
from unittest.mock import MagicMock

from boomi_mcp.categories.environments import _action_bulk_get_map_extensions


def _make_sdk(response_items):
    sdk = MagicMock()
    result = MagicMock()
    result.response = response_items
    sdk.environment_map_extension.bulk_environment_map_extension.return_value = result
    return sdk


def _make_success_item():
    """Item with result — models a found resource."""
    return SimpleNamespace(
        id_="ext-good",
        status_code=200,
        error_message=None,
        result=SimpleNamespace(environment_id="env-1", process_id="proc-1"),
    )


def _make_error_item():
    """Item with statusCode/errorMessage but NO result attribute — the real
    BUG-10 shape produced by the SDK for not-found entries."""
    return SimpleNamespace(
        id_="ext-bad",
        status_code=404,
        error_message="Resource not found",
    )


def test_mixed_success_and_error_items():
    sdk = _make_sdk([_make_success_item(), _make_error_item()])
    result = _action_bulk_get_map_extensions(sdk, profile="dev", ids=["ext-good", "ext-bad"])

    assert result["_success"] is True
    assert result["total_count"] == 2

    entries = result["responses"]
    success = next(e for e in entries if e["id"] == "ext-good")
    error = next(e for e in entries if e["id"] == "ext-bad")

    assert "result" in success
    assert success["status_code"] == 200

    assert "result" not in error
    assert error["status_code"] == 404
    assert error["error_message"] == "Resource not found"


def test_all_error_items():
    sdk = _make_sdk([_make_error_item()])
    result = _action_bulk_get_map_extensions(sdk, profile="dev", ids=["ext-bad"])

    assert result["_success"] is True
    assert result["total_count"] == 1
    assert "result" not in result["responses"][0]
    assert result["responses"][0]["error_message"] == "Resource not found"


def test_missing_ids():
    sdk = _make_sdk([])
    result = _action_bulk_get_map_extensions(sdk, profile="dev")
    assert result["_success"] is False
    assert "ids" in result["error"]
