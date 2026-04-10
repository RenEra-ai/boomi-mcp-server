"""Regression tests for _truncate_json_response and invoke_api truncation.

P1: Oversized JSON that is not a dict-with-list (root arrays, dicts without
    lists) must still be capped at MAX_RESPONSE_SIZE.
P2: The JSONDecodeError fallback must not append '... [TRUNCATED]' when the
    raw response was not actually truncated.
"""

import json
import sys
from pathlib import Path

_project_root = str(Path(__file__).resolve().parent.parent)
if _project_root not in sys.path:
    sys.path.insert(0, _project_root)

from boomi_mcp.categories.meta_tools import _truncate_json_response


# ---------------------------------------------------------------------------
# P1: Root-level list truncation
# ---------------------------------------------------------------------------

def test_root_list_truncated_to_fit():
    """A root-level list exceeding max_size must be trimmed."""
    big_list = [{"id": i, "data": "x" * 200} for i in range(500)]
    max_size = 5000
    result, meta = _truncate_json_response(big_list, max_size)
    serialized = json.dumps(result)
    assert len(serialized) <= max_size
    assert meta["items_total"] == 500
    assert meta["items_returned"] < 500
    assert meta["items_returned"] > 0


def test_root_list_under_limit_unchanged():
    """A root-level list already under max_size is returned as-is."""
    small_list = [1, 2, 3]
    result, meta = _truncate_json_response(small_list, 50000)
    assert result == [1, 2, 3]
    assert meta["items_returned"] == 3
    assert meta["items_total"] == 3


def test_root_empty_list_unchanged():
    """An empty root list is returned as-is (no items to trim)."""
    result, meta = _truncate_json_response([], 100)
    assert result == []
    assert meta == {}


# ---------------------------------------------------------------------------
# P1: Dict without list fields — hard-truncation fallback
# ---------------------------------------------------------------------------

def test_dict_without_list_hard_truncated():
    """A dict with no list values that exceeds max_size must be hard-capped."""
    big_dict = {"key": "v" * 100000}
    max_size = 5000
    result, meta = _truncate_json_response(big_dict, max_size)
    # Result is a hard-truncated string
    assert isinstance(result, str)
    assert len(result) <= max_size
    assert "note" in meta


def test_dict_without_list_under_limit_unchanged():
    """A dict with no list values under the limit is returned as-is."""
    small_dict = {"key": "value"}
    result, meta = _truncate_json_response(small_dict, 50000)
    assert result == {"key": "value"}
    assert meta == {}


# ---------------------------------------------------------------------------
# P1: Dict with list field (existing behavior — sanity check)
# ---------------------------------------------------------------------------

def test_dict_with_list_truncated():
    """Standard Boomi dict-with-list pattern is truncated correctly."""
    data = {
        "result": [{"id": i} for i in range(1000)],
        "numberOfResults": 1000,
    }
    max_size = 5000
    result, meta = _truncate_json_response(data, max_size)
    serialized = json.dumps(result)
    assert len(serialized) <= max_size
    assert meta["items_total"] == 1000
    assert meta["items_returned"] < 1000


# ---------------------------------------------------------------------------
# P2: JSONDecodeError fallback — TRUNCATED suffix only when actually truncated
# ---------------------------------------------------------------------------

def _make_invoke_api_result(raw_response, status=200):
    """Simulate the response-parsing section of invoke_api for a given raw string.

    This avoids mocking the full SDK chain and focuses on the truncation logic.
    """
    import json as json_mod

    MAX_RESPONSE_SIZE = 50000
    raw = raw_response
    truncated = len(raw) > MAX_RESPONSE_SIZE

    result = {
        "_success": 200 <= status < 300,
        "status_code": status,
        "method": "GET",
        "endpoint": "/test",
        "url": "https://test/test",
        "profile": "test",
    }

    # Replicate the accept == "json" branch from invoke_api
    try:
        parsed = json_mod.loads(raw)
        if truncated:
            parsed, trunc_meta = _truncate_json_response(parsed, MAX_RESPONSE_SIZE)
            result["truncated"] = True
            result["total_size"] = len(raw)
            result.update(trunc_meta)
        if truncated and isinstance(parsed, str):
            # Hard-truncated fallback — not valid JSON, use raw_response
            result["raw_response"] = parsed + "... [TRUNCATED]"
        else:
            result["data"] = parsed
    except (json_mod.JSONDecodeError, TypeError):
        if truncated:
            result["truncated"] = True
            result["total_size"] = len(raw)
            result["raw_response"] = raw[:MAX_RESPONSE_SIZE] + "... [TRUNCATED]"
        else:
            result["raw_response"] = raw

    return result


def test_oversized_dict_without_list_goes_to_raw_response():
    """P1: Oversized dict-without-list must land in raw_response, not data."""
    import json
    big_dict = json.dumps({"key": "v" * 100000})
    result = _make_invoke_api_result(big_dict)
    assert result.get("truncated") is True
    assert "data" not in result, "hard-truncated content must not be in 'data'"
    assert "raw_response" in result
    assert "TRUNCATED" in result["raw_response"]
    assert len(result["raw_response"]) <= 50000 + 20  # max_size + suffix


def test_json_string_value_not_misclassified():
    """A valid JSON string response must go to data, not raw_response."""
    import json
    raw = json.dumps("hello")  # valid JSON whose parsed value is a str
    result = _make_invoke_api_result(raw)
    assert result.get("data") == "hello"
    assert "raw_response" not in result
    assert result.get("truncated") is not True


def test_malformed_json_short_no_truncated_suffix():
    """P2: Short malformed JSON must NOT get '... [TRUNCATED]' appended."""
    short_bad = "{not valid json"
    result = _make_invoke_api_result(short_bad)
    assert result.get("raw_response") == short_bad
    assert "TRUNCATED" not in result.get("raw_response", "")
    assert result.get("truncated") is not True


def test_malformed_json_oversized_gets_truncated_suffix():
    """P2: Oversized malformed JSON SHOULD get '... [TRUNCATED]' appended."""
    big_bad = "{not valid " + "x" * 60000
    result = _make_invoke_api_result(big_bad)
    assert result.get("truncated") is True
    assert "TRUNCATED" in result.get("raw_response", "")
    assert len(result["raw_response"]) < len(big_bad)
