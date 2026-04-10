"""Tests for name LIKE pattern handling in search_components.

Callers may pass bare strings, or explicit LIKE patterns with '%'.
Bare strings get auto-wrapped as '%value%' (substring match).
Explicit patterns like 'Order%' or '%Order' must be preserved as-is.
"""

import sys
from pathlib import Path
from unittest.mock import MagicMock, patch, call

_project_root = str(Path(__file__).resolve().parent.parent)
if _project_root not in sys.path:
    sys.path.insert(0, _project_root)
    sys.path.insert(0, str(Path(_project_root) / "src"))


def _get_name_argument(name_filter):
    """Run search_components with a name filter and capture the argument
    passed to the LIKE expression."""
    with patch(
        "boomi_mcp.categories.components.query_components.ComponentMetadataSimpleExpression"
    ) as mock_expr, patch(
        "boomi_mcp.categories.components.query_components.paginate_metadata",
        return_value=[],
    ):
        mock_expr.return_value = MagicMock()
        from boomi_mcp.categories.components.query_components import search_components

        search_components(MagicMock(), "test", {"name": name_filter})

        # Find the call that used LIKE operator (name filter)
        for c in mock_expr.call_args_list:
            _, kwargs = c
            if "LIKE" in str(kwargs.get("operator", "")):
                return kwargs["argument"][0]
        # Fallback: first call's argument
        _, kwargs = mock_expr.call_args_list[0]
        return kwargs["argument"][0]


def test_bare_string_gets_wrapped():
    """A plain string with no '%' should become '%value%'."""
    assert _get_name_argument("Order") == "%Order%"


def test_starts_with_pattern_preserved():
    """'Order%' should remain 'Order%', not become '%Order%'."""
    assert _get_name_argument("Order%") == "Order%"


def test_ends_with_pattern_preserved():
    """'%Order' should remain '%Order', not become '%Order%'."""
    assert _get_name_argument("%Order") == "%Order"


def test_full_wildcard_pattern_preserved():
    """'%Order%' should remain unchanged."""
    assert _get_name_argument("%Order%") == "%Order%"


def test_middle_wildcard_preserved():
    """'Ord%er' should remain unchanged (has explicit %)."""
    assert _get_name_argument("Ord%er") == "Ord%er"
