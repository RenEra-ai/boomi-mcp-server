"""Regression tests for BUG-29: get_schema_template unknown-operation contract.

Each resource type's template helper must return ``_success=False``, an
``error`` string containing the bogus operation name, and a
``valid_operations`` list when called with an unrecognised operation.
This prevents a future refactor from silently reverting to the old
``_success=True`` path.
"""

import pytest

from boomi_mcp.categories.meta_tools import get_schema_template_action
from boomi_mcp.errors import SCHEMA_LOOKUP_FAILED

# (resource_type, expected valid_operations)
RESOURCE_CASES = [
    ("trading_partner", ["create", "list", "update"]),
    ("process", ["create", "list"]),
    ("integration", ["plan", "apply", "verify"]),
    ("component", ["create", "search", "clone", "compare_versions", "safe_edit"]),
    ("environment", ["create"]),
    ("package", ["create", "deploy"]),
    ("execution_request", ["execute"]),
    ("organization", ["list", "get", "create", "update", "delete"]),
    (
        "folder",
        ["list", "get", "create", "move_component", "delete", "restore", "contents"],
    ),
]


@pytest.mark.parametrize("resource_type, expected_ops", RESOURCE_CASES, ids=[c[0] for c in RESOURCE_CASES])
def test_unknown_operation_returns_structured_error(resource_type, expected_ops):
    result = get_schema_template_action(resource_type=resource_type, operation="__bogus__")

    assert result["_success"] is False
    assert result["error"] == f"Unknown {resource_type} operation: __bogus__"
    assert result["valid_operations"] == expected_ops
    assert result["error_code"] == SCHEMA_LOOKUP_FAILED


def test_monitoring_unknown_operation():
    """monitoring valid_operations is dynamic (derived from dict keys), so test separately."""
    result = get_schema_template_action(resource_type="monitoring", operation="__bogus__")

    assert result["_success"] is False
    assert result["error"] == "Unknown monitoring operation: __bogus__"
    assert result["error_code"] == SCHEMA_LOOKUP_FAILED
    assert isinstance(result["valid_operations"], list)
    assert len(result["valid_operations"]) > 0
    # Spot-check a few known monitoring operations
    for expected in ("execution_records", "audit_logs", "events"):
        assert expected in result["valid_operations"]


def test_unknown_resource_type():
    """Completely unknown resource_type must also fail with structured error."""
    result = get_schema_template_action(resource_type="__nonexistent__")

    assert result["_success"] is False
    assert result["error"] == "Unknown resource_type: __nonexistent__"
    assert "valid_types" in result
    assert result["error_code"] == SCHEMA_LOOKUP_FAILED


def test_component_safe_edit_operation_returns_template():
    """The new M9.7 safe_edit operation must resolve to a structured template."""
    result = get_schema_template_action(resource_type="component", operation="safe_edit")

    assert result["_success"] is True
    assert result["operation"] == "safe_edit"
    assert "prepare_component_edit (read-only preview)" in result["tools"]
    assert "apply_component_edit (confirmed write)" in result["tools"]
    assert "COMPONENT_EDIT_DRIFT_DETECTED" in result["error_codes"]
