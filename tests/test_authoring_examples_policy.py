"""Anti-template policy tests for examples/authoring/ (Issue #10).

Exit criterion: examples cannot be mistaken for reusable SQL, mapping, or
payload templates. Every example file must carry machine-checkable labels and
must not contain executable SQL, raw XML, real-looking component ids, or
secret-shaped values.
"""

import json
import re
from pathlib import Path

import pytest

EXAMPLES_DIR = Path(__file__).resolve().parent.parent / "examples" / "authoring"

REQUIRED_LABELS = {
    "example_not_template": True,
    "is_template": False,
    "template_status": "example_only_not_reusable_template",
}

SQL_CRUD_RE = re.compile(
    r"(?i)\b(select\s+.+\s+from|insert\s+into|update\s+\w+\s+set|delete\s+from)\b"
)
RAW_XML_RE = re.compile(r"<\?xml|</?\w+Component")
UUID_RE = re.compile(
    r"[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}"
)
SECRET_KEY_RE = re.compile(r"(?i)(password|client_secret|api_key|token|authorization)")
PLACEHOLDER_RE = re.compile(r"^<<.*>>$")


def _example_files():
    return sorted(EXAMPLES_DIR.glob("*_example.json"))


def _iter_items(node, path=""):
    """Yield (path, key, value) for every dict entry, recursively."""
    if isinstance(node, dict):
        for key, value in node.items():
            yield path, key, value
            yield from _iter_items(value, f"{path}/{key}")
    elif isinstance(node, list):
        for i, value in enumerate(node):
            yield from _iter_items(value, f"{path}[{i}]")


def test_examples_directory_has_expected_files():
    files = _example_files()
    assert len(files) >= 3, f"expected >=3 example files, found {[f.name for f in files]}"


@pytest.mark.parametrize("path", _example_files(), ids=lambda p: p.name)
def test_example_carries_anti_template_labels(path):
    data = json.loads(path.read_text())
    for label, expected in REQUIRED_LABELS.items():
        assert data.get(label) == expected, (
            f"{path.name}: label {label!r} must be {expected!r}, got {data.get(label)!r}"
        )
    assert isinstance(data.get("purpose"), str) and data["purpose"].strip()
    assert isinstance(data.get("do_not_reuse"), str) and data["do_not_reuse"].strip()


@pytest.mark.parametrize("path", _example_files(), ids=lambda p: p.name)
def test_example_contains_no_reusable_sql(path):
    text = path.read_text()
    match = SQL_CRUD_RE.search(text)
    assert match is None, f"{path.name}: SQL CRUD pattern found: {match.group(0)!r}"


@pytest.mark.parametrize("path", _example_files(), ids=lambda p: p.name)
def test_example_contains_no_raw_xml(path):
    text = path.read_text()
    match = RAW_XML_RE.search(text)
    assert match is None, f"{path.name}: raw XML found: {match.group(0)!r}"


@pytest.mark.parametrize("path", _example_files(), ids=lambda p: p.name)
def test_example_contains_no_uuid_shaped_ids(path):
    text = path.read_text()
    match = UUID_RE.search(text)
    assert match is None, f"{path.name}: UUID-shaped id found: {match.group(0)!r}"


@pytest.mark.parametrize("path", _example_files(), ids=lambda p: p.name)
def test_example_secret_shaped_keys_hold_placeholders_only(path):
    data = json.loads(path.read_text())
    for node_path, key, value in _iter_items(data):
        if not SECRET_KEY_RE.search(key):
            continue
        assert isinstance(value, str), (
            f"{path.name}: secret-shaped key {node_path}/{key} must hold a string placeholder"
        )
        assert PLACEHOLDER_RE.match(value) or value.startswith("credential://") or value == "", (
            f"{path.name}: secret-shaped key {node_path}/{key} holds a non-placeholder "
            f"value {value!r}"
        )


@pytest.mark.parametrize("path", _example_files(), ids=lambda p: p.name)
def test_example_uses_placeholder_sentinels(path):
    """Parameter-carrying examples must visibly use <<placeholder>> sentinels —
    that is the cue that values are authored per-task, not copied. Workflow-as-
    data examples carry no parameters, so the rule applies to tool_call files."""
    data = json.loads(path.read_text())
    if "tool_call" not in data:
        pytest.skip(f"{path.name} carries no tool parameters")
    assert "<<" in path.read_text(), f"{path.name}: no <<placeholder>> sentinels found"
