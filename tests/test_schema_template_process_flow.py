"""Schema-template tests for the database_to_api_sync process protocol (issue #25).

Confirms get_schema_template returns the new structured template for
resource_type='process', protocol='database_to_api_sync', documents all
structured-error codes, and obeys the anti-template rule (no SQL, no
real component IDs, no Groovy, etc. — only <<...>> placeholders and
$ref tokens).
"""

from __future__ import annotations

import json
import re
import sys
from pathlib import Path

import pytest

_project_root = str(Path(__file__).resolve().parent.parent)
if _project_root not in sys.path:
    sys.path.insert(0, _project_root)

from src.boomi_mcp.categories.meta_tools import get_schema_template_action


_UUID_RE = re.compile(r"\b[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}\b")

# Anti-template patterns that must NOT match the template's serialized
# JSON. Tightened past bare SQL verbs (DELETE / UPDATE / etc.) to
# specific SQL/syntax shapes — bare verb names collide with REST HTTP
# methods which legitimately appear in supported_connector_action_bindings.
_FORBIDDEN_PATTERNS = (
    r"\bSELECT\s+[\w*]+\s+FROM\b",
    r"\bINSERT\s+INTO\b",
    r"\bUPDATE\s+\w+\s+SET\b",
    r"\bDELETE\s+FROM\b",
    r"\bCREATE\s+TABLE\b",
    r"\bDROP\s+TABLE\b",
    r"<envelope",
    r"</envelope",
    r"\bOData\b",
    r"\$filter=",
    r"\bdef\s+\w+\s*\(",       # Groovy/Python method-def syntax
    r"\bgroovy\b",
    r"-----BEGIN\b",            # PEM / PGP key headers
)


@pytest.fixture
def template():
    result = get_schema_template_action(
        resource_type="process",
        operation="create",
        protocol="database_to_api_sync",
    )
    assert result["_success"] is True
    return result


def test_template_advertises_process_kind(template):
    assert template["process_kind"] == "database_to_api_sync"
    assert template["protocol"] == "database_to_api_sync"
    assert template["resource_type"] == "process"
    assert template["operation"] == "create"


def test_template_lists_required_fields(template):
    required = template["required_fields"]
    for field in (
        "source.connector_type",
        "source.connection_id",
        "source.operation_id",
        "source.action_type",
        "target.connector_type",
        "target.connection_id",
        "target.operation_id",
        "target.action_type",
    ):
        assert field in required, f"required field {field!r} missing from template"


def test_template_lists_optional_fields(template):
    optional = template["optional_fields"]
    for field in (
        "folder_name",
        "transform",
        "transform.mode",
        "reliability",
        "reliability.retry_count",
        "reliability.dlq.mode",
    ):
        assert field in optional, f"optional field {field!r} missing from template"


def test_template_deferred_fields_lists_unimplemented_surface(template):
    """Codex review r3 P2: execution.* and reliability.on_failure were
    advertised as accepted optional fields but silently ignored by the
    builder. They must instead be documented as deferred so callers can't
    mistake them for working surface area."""
    deferred = {entry["field"]: entry["tracked_by"] for entry in template["deferred_fields"]}
    assert deferred.get("execution.trigger") == "#28"
    assert deferred.get("execution.run_metadata") == "#28"
    assert deferred.get("reliability.on_failure") == "#28"


def test_template_optional_fields_excludes_deferred(template):
    """Defense-in-depth: a deferred field must not also appear in
    optional_fields, or the schema sends mixed signals."""
    optional = set(template["optional_fields"])
    deferred_fields = {e["field"] for e in template["deferred_fields"]}
    leaked = optional & deferred_fields
    assert leaked == set(), f"deferred fields leaked into optional_fields: {leaked}"


def test_template_example_does_not_use_deferred_fields(template):
    """The example must not demonstrate deferred fields, or callers will
    copy patterns that the builder silently ignores."""
    example_config = template["example_component_spec"]["config"]
    assert "execution" not in example_config
    assert "on_failure" not in example_config.get("reliability", {})


def test_template_supported_transform_modes(template):
    assert set(template["supported_transform_modes"]) == {"passthrough", "message", "map_ref"}


def test_template_supported_dlq_modes(template):
    assert set(template["supported_dlq_modes"]) == {
        "disabled", "document_cache_ref", "error_subprocess_ref",
    }


def test_template_supported_connector_bindings(template):
    bindings = template["supported_connector_action_bindings"]
    assert bindings["database_source"]["connector_type"] == "database"
    assert bindings["database_source"]["action_type"] == "Get"
    # REST target must include the canonical subtype somewhere in the spelling list.
    assert "officialboomi-X3979C-rest-prod" in bindings["rest_target"]["connector_type"]


def test_template_documents_all_structured_error_codes(template):
    codes = {e["error_code"] for e in template["structured_errors"]}
    for required in (
        "PROCESS_KIND_UNSUPPORTED",
        "PROCESS_KIND_XML_CONFLICT",
        "PROCESS_NAME_REQUIRED",
        "PROCESS_NAME_CONFLICT",
        "MISSING_PROCESS_DEPENDENCY",
        "PROCESS_CONNECTOR_BINDING_INVALID",
        "PROCESS_SHAPE_UNSUPPORTED",
        "PROCESS_RETRY_UNVERIFIED",
        "PROCESS_DLQ_BINDING_INVALID",
        "PROCESS_XML_VALIDATION_FAILED",
        "PLAINTEXT_SECRET_REJECTED",
    ):
        assert required in codes, f"error code {required!r} not documented in template"


def test_example_uses_placeholder_labels_and_refs(template):
    example = template["example_component_spec"]
    assert example["type"] == "process"
    assert example["config"]["process_kind"] == "database_to_api_sync"
    source = example["config"]["source"]
    target = example["config"]["target"]
    # Refs must use the $ref:KEY token form.
    for binding in (source, target):
        assert binding["connection_id"].startswith("$ref:")
        assert binding["operation_id"].startswith("$ref:")
    # Every ref key in the example must appear in depends_on (matches the
    # MISSING_PROCESS_DEPENDENCY contract that the builder enforces).
    declared = set(example["depends_on"])
    referenced = {
        source["connection_id"].split(":", 1)[1],
        source["operation_id"].split(":", 1)[1],
        target["connection_id"].split(":", 1)[1],
        target["operation_id"].split(":", 1)[1],
    }
    assert referenced.issubset(declared), (
        f"example references {referenced} but only declares {declared}"
    )


def test_template_obeys_anti_template_rule(template):
    serialized = json.dumps(template)
    for pattern in _FORBIDDEN_PATTERNS:
        match = re.search(pattern, serialized, re.IGNORECASE)
        assert match is None, (
            f"template matches forbidden pattern {pattern!r} "
            f"({match.group(0)!r}) — see the anti-template rule "
            f"in issue #25 plan."
        )
    # No real-looking Boomi component UUIDs should leak into the example.
    assert not _UUID_RE.search(serialized), (
        "template example contains a UUID-shaped string — examples must "
        "use $ref:KEY tokens and <<...>> placeholders only."
    )


def test_unknown_protocol_returns_error():
    result = get_schema_template_action(
        resource_type="process",
        operation="create",
        protocol="not_a_real_protocol",
    )
    assert result["_success"] is False
    assert "not_a_real_protocol" in result["error"]
    assert "database_to_api_sync" in result["valid_protocols"]


def test_protocol_overrides_operation_overview():
    # protocol alone (no operation) still returns the protocol template
    result = get_schema_template_action(
        resource_type="process",
        protocol="database_to_api_sync",
    )
    assert result["_success"] is True
    assert result.get("process_kind") == "database_to_api_sync"
