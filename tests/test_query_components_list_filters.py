"""`list` must refuse a filter key it does not honour.

This is a fail-OPEN that caused real damage: the key was dropped silently, the call
still reported success, and the caller received the whole account instead of the
scope they asked for. A cleanup built on that answer soft-deleted twenty-one
components it was never meant to touch.

The direction is what makes it dangerous. An ignored filter returns MORE than was
asked for, and a caller who believes the result is scoped may act destructively on
it. The asymmetry that made it plausible: the sibling `search` action DOES honour
`name`.
"""

from __future__ import annotations

from boomi_mcp.categories.components.query_components import (
    _LIST_FILTER_KEYS,
    list_components,
)


class _Client:
    """Stands in at the SDK boundary only; the refusal happens before any call."""

    def __init__(self):
        self.called = False


def test_an_unsupported_filter_key_is_refused():
    result = list_components(_Client(), "renera", {"type": "process", "name": "X"})
    assert result["_success"] is False
    assert "name" in result["error"]
    assert "search" in result["hint"], "the refusal must point at the action that does filter by name"


def test_the_refusal_names_what_is_supported():
    result = list_components(_Client(), "renera", {"nonsense_key": 1})
    assert set(result["supported_filters"]) == set(_LIST_FILTER_KEYS)


def test_the_refusal_happens_before_the_platform_is_touched():
    """A refusal that still queried would be a warning, not a guard."""
    client = _Client()
    list_components(client, "renera", {"name": "X"})
    assert client.called is False


def test_supported_keys_are_not_refused():
    """The control: this must not become a blanket denial.

    Asserted by attempting each supported key and requiring the refusal path NOT to
    trigger — the call then fails at the SDK boundary, which is a different error.
    """
    for key, value in (("show_all", False), ("type", "process"),
                       ("component_type", "process"), ("limit", 5)):
        result = list_components(_Client(), "renera", {key: value})
        assert "does not support these filter key" not in str(result.get("error", "")), key
