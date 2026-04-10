"""Regression tests for _parse_diff_response and helpers.

P1: ChangeValue objects must be serialized to plain dicts, not passed through raw.
P2: Composite element keys (multiple key_parts) must all be preserved.
"""

import sys
from pathlib import Path
from types import SimpleNamespace

_project_root = str(Path(__file__).resolve().parent.parent)
if _project_root not in sys.path:
    sys.path.insert(0, _project_root)

from boomi_mcp.categories.components.analyze_component import (
    _change_value_to_plain,
    _element_key_to_dict,
    _parse_diff_response,
)


# ---------------------------------------------------------------------------
# P1: _change_value_to_plain
# ---------------------------------------------------------------------------


def test_change_value_plain_string():
    assert _change_value_to_plain("hello") == "hello"


def test_change_value_none():
    assert _change_value_to_plain(None) == ""


def test_change_value_object_with_value_only():
    cv = SimpleNamespace(value="42", xpath=None)
    result = _change_value_to_plain(cv)
    assert result == {"value": "42"}
    assert "xpath" not in result


def test_change_value_object_with_xpath():
    cv = SimpleNamespace(value="42", xpath="/root/node")
    result = _change_value_to_plain(cv)
    assert result == {"value": "42", "xpath": "/root/node"}


def test_change_value_xpath_only_no_value_attr():
    """SDK sentinel: ChangeValue with xpath but no value attribute on instance."""
    cv = SimpleNamespace(xpath="/Component[1]/description[1]/text()[1]")
    result = _change_value_to_plain(cv)
    assert result == {"value": "", "xpath": "/Component[1]/description[1]/text()[1]"}


def test_change_value_unknown_object_stringified():
    result = _change_value_to_plain(12345)
    assert result == "12345"


# ---------------------------------------------------------------------------
# P2: _element_key_to_dict — composite keys
# ---------------------------------------------------------------------------


def test_element_key_single_via_key_parts():
    """Single key_part: emits key_part only, no key_parts list."""
    ek = SimpleNamespace(
        element_name="connector",
        key_parts=[SimpleNamespace(attribute="type", value="http")],
        key_part=SimpleNamespace(attribute="type", value="http"),
    )
    result = _element_key_to_dict(ek)
    assert result == {
        "element_name": "connector",
        "key_part": {"attribute": "type", "value": "http"},
    }
    assert "key_parts" not in result


def test_element_key_composite_two_parts():
    """Composite key: emits both key_part (first) and key_parts (full list)."""
    ek = SimpleNamespace(
        element_name="field",
        key_parts=[
            SimpleNamespace(attribute="name", value="firstName"),
            SimpleNamespace(attribute="dataType", value="string"),
        ],
        key_part=SimpleNamespace(attribute="name", value="firstName"),
    )
    result = _element_key_to_dict(ek)
    assert result["element_name"] == "field"
    assert result["key_part"] == {"attribute": "name", "value": "firstName"}
    assert len(result["key_parts"]) == 2
    assert result["key_parts"][0] == {"attribute": "name", "value": "firstName"}
    assert result["key_parts"][1] == {"attribute": "dataType", "value": "string"}


def test_element_key_fallback_to_singular():
    """When key_parts is absent, fall back to key_part singular."""
    ek = SimpleNamespace(
        element_name="node",
        key_part=SimpleNamespace(attribute="id", value="99"),
    )
    result = _element_key_to_dict(ek)
    assert result == {
        "element_name": "node",
        "key_part": {"attribute": "id", "value": "99"},
    }
    assert "key_parts" not in result


def test_element_key_none():
    assert _element_key_to_dict(None) is None


def test_element_key_no_element_name():
    assert _element_key_to_dict(SimpleNamespace(foo="bar")) is None


# ---------------------------------------------------------------------------
# P1 + P2 integrated: _parse_diff_response
# ---------------------------------------------------------------------------


def _make_diff_result(additions=None, deletions=None, modifications=None):
    """Build a minimal fake ComponentDiffResponseCreate."""
    generic_diff = SimpleNamespace(
        addition=SimpleNamespace(total=len(additions or []), change=additions or []),
        deletion=SimpleNamespace(total=len(deletions or []), change=deletions or []),
        modification=SimpleNamespace(total=len(modifications or []), change=modifications or []),
    )
    cdr = SimpleNamespace(message="diff ok", generic_diff=generic_diff)
    return SimpleNamespace(component_diff_response=cdr)


def test_parse_additions_serializes_change_value():
    cv = SimpleNamespace(value="new_val", xpath="/root")
    change = SimpleNamespace(
        type_="add",
        changed_particle_name="field_a",
        new_value=cv,
        element_key=None,
    )
    result = _parse_diff_response(_make_diff_result(additions=[change]))
    entry = result["additions"][0]
    assert entry["new_value"] == {"value": "new_val", "xpath": "/root"}


def test_parse_deletions_serializes_change_value():
    cv = SimpleNamespace(value="old_val", xpath=None)
    change = SimpleNamespace(
        type_="del",
        changed_particle_name="field_b",
        old_value=cv,
        element_key=None,
    )
    result = _parse_diff_response(_make_diff_result(deletions=[change]))
    entry = result["deletions"][0]
    assert entry["old_value"] == {"value": "old_val"}


def test_parse_modifications_serializes_both_values():
    old_cv = SimpleNamespace(value="before", xpath="/a")
    new_cv = SimpleNamespace(value="after", xpath="/b")
    change = SimpleNamespace(
        type_="mod",
        changed_particle_name="field_c",
        old_value=old_cv,
        new_value=new_cv,
        element_key=None,
    )
    result = _parse_diff_response(_make_diff_result(modifications=[change]))
    entry = result["modifications"][0]
    assert entry["old_value"] == {"value": "before", "xpath": "/a"}
    assert entry["new_value"] == {"value": "after", "xpath": "/b"}


def test_parse_modification_with_composite_element_key():
    change = SimpleNamespace(
        type_="mod",
        changed_particle_name="connector_field",
        old_value="plain_old",
        new_value="plain_new",
        element_key=SimpleNamespace(
            element_name="connector",
            key_parts=[
                SimpleNamespace(attribute="type", value="http"),
                SimpleNamespace(attribute="version", value="2"),
            ],
            key_part=SimpleNamespace(attribute="type", value="http"),
        ),
    )
    result = _parse_diff_response(_make_diff_result(modifications=[change]))
    entry = result["modifications"][0]
    assert entry["old_value"] == "plain_old"
    assert entry["new_value"] == "plain_new"
    ek = entry["element_key"]
    # key_part is always the first entry (backward compat)
    assert ek["key_part"] == {"attribute": "type", "value": "http"}
    # key_parts only present for composite (>1) keys
    assert len(ek["key_parts"]) == 2
    assert ek["key_parts"][1] == {"attribute": "version", "value": "2"}


def test_parse_plain_string_values_unchanged():
    change = SimpleNamespace(
        type_="add",
        changed_particle_name="simple",
        new_value="just a string",
        element_key=None,
    )
    result = _parse_diff_response(_make_diff_result(additions=[change]))
    assert result["additions"][0]["new_value"] == "just a string"
