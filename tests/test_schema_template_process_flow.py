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
        # Issue #51 M3.R1a: DLQ catch-path bindings now consumed by the builder.
        "reliability.dlq.document_cache_id",
        "reliability.dlq.process_id",
    ):
        assert field in optional, f"optional field {field!r} missing from template"


def test_template_deferred_fields_lists_unimplemented_surface(template):
    """Codex review r3 P2: execution.* fields are produced by #28 primitives
    but silently ignored by the builder, so they must stay documented as
    deferred (not optional) so callers can't mistake them for working surface.

    Issue #51 M3.R1a: reliability.on_failure is NO LONGER deferred — the
    dlq_writer fragment is now consumed into a verified Try/Catch + DLQ
    catch-path for retry_count == 0 (see test_template_lists_optional_fields).
    The error_classifier fragment is still not consumed, so it remains
    deferred in its place. `tracked_by` points at the milestone/issue that
    will wire each remaining field into the executable process."""
    deferred = {entry["field"]: entry["tracked_by"] for entry in template["deferred_fields"]}
    assert deferred.get("execution.trigger") == "M3 (deploy + schedule activation)"
    assert deferred.get("execution.run_metadata") == "#51 (run-metadata / dynamic process-property wiring)"
    # on_failure (the DLQ intent) is now consumed → must NOT be deferred.
    assert "reliability.on_failure" not in deferred
    # The classifier half of the old on_failure umbrella is still unconsumed.
    assert "reliability.error_classifier" in deferred

    # Each deferred field names the issue-#28 primitive that produces it and the
    # #29 surface that now represents it, so callers understand the field exists
    # as metadata even though the process builder does not yet read it.
    produced_by = {entry["field"]: entry.get("produced_by", "") for entry in template["deferred_fields"]}
    assert "schedule_envelope" in produced_by["execution.trigger"]
    assert "run_metadata" in produced_by["execution.run_metadata"]
    assert "error_classifier" in produced_by["reliability.error_classifier"]

    represented_by = {entry["field"]: entry.get("represented_by", "") for entry in template["deferred_fields"]}
    for field in ("execution.trigger", "execution.run_metadata", "reliability.error_classifier"):
        assert "#29" in represented_by[field], field
        assert "operational_intent" in represented_by[field], field


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
    assert set(template["supported_transform_modes"]) == {
        "passthrough", "message", "map_ref", "dataprocess",
    }


def test_template_documents_dataprocess_surface(template):
    # Issue #106 M10.2: dataprocess transform fields + v1 operation set + the
    # two new structured errors are all documented.
    optional = template["optional_fields"]
    for field in (
        "transform.label",
        "transform.steps",
        "transform.steps[].operation",
        "transform.steps[].script",
    ):
        assert field in optional, field
    assert template["supported_dataprocess_operations"] == ["custom_scripting"]
    codes = {e["error_code"] for e in template["structured_errors"]}
    assert "PROCESS_DATAPROCESS_CONFIG_INVALID" in codes
    assert "PROCESS_DATAPROCESS_OPERATION_UNSUPPORTED" in codes


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
        "PROCESS_REF_TYPE_MISMATCH",
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


def test_template_lists_catch_notify_optional_fields(template):
    # Issue #89: catch_notify surface is documented as optional.
    optional = template["optional_fields"]
    for field in (
        "reliability.catch_notify",
        "reliability.catch_notify.message_template",
        "reliability.catch_notify.level",
    ):
        assert field in optional, f"optional field {field!r} missing from template"


def test_template_lists_supported_notify_levels(template):
    assert template["supported_notify_levels"] == ["INFO", "WARNING", "ERROR"]


def test_template_documents_notify_config_error(template):
    codes = {e["error_code"] for e in template["structured_errors"]}
    assert "PROCESS_NOTIFY_CONFIG_INVALID" in codes


def test_template_lists_process_extensions_surface(template):
    # Issue #92 M4.5.7: connection-field environment-extension declaration.
    optional = template["optional_fields"]
    assert "process_extensions" in optional
    assert "process_extensions.connections" in optional
    codes = {e["error_code"] for e in template["structured_errors"]}
    assert "PROCESS_EXTENSIONS_INVALID" in codes
    notes_blob = " ".join(template["notes"]).lower()
    assert "get_extensions" in notes_blob
    assert "create" in notes_blob  # CREATE-only behavior documented


def test_template_example_includes_process_extensions(template):
    # Issue #92 M4.5.7: the example must demonstrate the process_extensions
    # block so callers copy a working override declaration, not omit it.
    example = template["example_component_spec"]
    pe = example["config"].get("process_extensions")
    assert pe is not None, "example must demonstrate process_extensions"
    conn = pe["connections"][0]
    # The override id reuses a $ref already declared in depends_on.
    assert conn["connection_id"].startswith("$ref:")
    assert conn["connection_id"].split(":", 1)[1] in example["depends_on"]
    field_ids = {f["id"] for f in conn["fields"]}
    assert {"username", "password"} <= field_ids
    for field in conn["fields"]:
        assert field["label"] and field["xpath"]


def test_example_demonstrates_wired_dlq_and_catch_notify(template):
    example = template["example_component_spec"]
    reliability = example["config"]["reliability"]
    # Wired DLQ bound by $ref, with the DLQ ref declared in depends_on.
    assert reliability["dlq"]["mode"] == "document_cache_ref"
    assert reliability["dlq"]["document_cache_id"] == "$ref:dlq_document_cache"
    assert "dlq_document_cache" in example["depends_on"]
    # catch_notify present, references the caught-error property, valid level.
    notify = reliability["catch_notify"]
    assert notify["level"] in ("INFO", "WARNING", "ERROR")
    assert "meta.base.catcherrorsmessage" in notify["message_template"]
    # Placeholder-only message body (no canned content) — anti-template hygiene.
    assert "<<" in notify["message_template"]


def test_wrapper_subprocess_protocol_documented():
    # Issue #90: the wrapper_subprocess structure is documented in get_schema_template.
    result = get_schema_template_action(
        resource_type="process",
        operation="create",
        protocol="wrapper_subprocess",
    )
    assert result["_success"] is True
    assert result["process_kind"] == "wrapper_subprocess"
    assert "process_calls" in result["required_fields"]
    for field in (
        "process_calls[].subprocess_ref",
        "process_calls[].process_id",
        "process_calls[].wait",
        "process_calls[].abort_on_error",
    ):
        assert field in result["optional_fields"], field
    codes = {e["error_code"] for e in result["structured_errors"]}
    for code in (
        "PROCESS_REF_MISSING",
        "PROCESS_REF_AMBIGUOUS",
        "PROCESS_REF_SELF_REFERENCE",
        "PROCESS_REF_NOT_FOUND",
        "PROCESS_REF_TYPE_MISMATCH",
    ):
        assert code in codes, code
    # Parent-redeploy implication is documented; example uses $ref + placeholders only.
    notes_blob = " ".join(result["notes"]).lower()
    assert "redeploy" in notes_blob
    example = result["example_component_spec"]
    assert example["config"]["process_kind"] == "wrapper_subprocess"
    call0 = example["config"]["process_calls"][0]
    assert call0["subprocess_ref"] == "$ref:main_logic"
    assert "main_logic" in example["depends_on"]
    serialized = json.dumps(result)
    assert not _UUID_RE.search(serialized)


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


def test_process_create_without_protocol_returns_removal_guidance():
    # Legacy freeform process JSON authoring has been removed: process
    # operation='create' with no protocol returns removal guidance instead of
    # a shape-graph template.
    result = get_schema_template_action(
        resource_type="process",
        operation="create",
    )
    assert result["_success"] is True
    assert result["removed"] is True
    assert "single_process_template" not in result
    assert "shape_reference" not in result
    # Steers to the typed authoring paths.
    assert "database_to_api_sync" in result["process_protocols"]
    assert "wrapper_subprocess" in result["process_protocols"]
    blob = json.dumps(result).lower()
    assert "build_from_archetype" in blob or "build_integration" in blob


def test_process_overview_is_read_only_listget():
    # The process overview (no operation, no protocol) advertises list/get only.
    result = get_schema_template_action(resource_type="process")
    assert result["_success"] is True
    assert result["available_actions"] == ["list", "get"]
    assert result.get("read_only") is True
    assert "shape_types" not in result


def test_workflow_sequences_drops_manage_process_create():
    # Discovery must no longer steer callers to manage_process(action='create').
    result = get_schema_template_action(schema_name="workflow_sequences")
    assert result["_success"] is True
    blob = json.dumps(result["workflow_sequences"])
    assert "manage_process(action='create'" not in blob
    assert "manage_process(action=\"create\"" not in blob
