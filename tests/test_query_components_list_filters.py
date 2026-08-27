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


def test_the_allowlist_matches_what_the_function_actually_reads():
    """DERIVED from the source, because the hand-written set was already wrong once.

    The first version omitted `folder_name`, which `list_components` has always
    honoured — so a refusal added to stop a silent WIDENING instead broke a working
    filter, failing in the opposite direction. Parsing what the function reads
    catches drift both ways: a key honoured but not allowed is a regression, and a
    key allowed but not honoured is the original silent-ignore defect returning.
    """
    import ast
    from pathlib import Path

    source = (Path(__file__).resolve().parents[1]
              / "src/boomi_mcp/categories/components/query_components.py").read_text()
    fn = next(n for n in ast.walk(ast.parse(source))
              if isinstance(n, ast.FunctionDef) and n.name == "list_components")

    read: set[str] = set()
    for node in ast.walk(fn):
        if (isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute)
                and node.func.attr == "get" and isinstance(node.func.value, ast.Name)
                and node.func.value.id == "filters" and node.args
                and isinstance(node.args[0], ast.Constant)):
            read.add(node.args[0].value)
        if (isinstance(node, ast.Subscript) and isinstance(node.value, ast.Name)
                and node.value.id == "filters" and isinstance(node.slice, ast.Constant)):
            read.add(node.slice.value)

    assert read == set(_LIST_FILTER_KEYS), {
        "honoured but refused (a regression)": sorted(read - set(_LIST_FILTER_KEYS)),
        "allowed but ignored (silently widens)": sorted(set(_LIST_FILTER_KEYS) - read),
    }


def test_folder_name_is_not_refused():
    """The specific regression: an existing, working filter must keep working."""
    result = list_components(_Client(), "renera", {"folder_name": "X"})
    assert "does not support these filter key" not in str(result.get("error", ""))
