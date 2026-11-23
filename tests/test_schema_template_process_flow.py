"""Schema-template tests for the process protocols (issue #25; retired in #159 S3).

#159 S3: the three legacy process_kind protocol templates are retired; every
protocol answers with a value-free removal envelope. Node ids are kept (the
manifest pins them) and their bodies are removal witnesses.

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


def _protocol_template(protocol):
    return get_schema_template_action(
        resource_type="process", operation="create", protocol=protocol
    )


def _assert_retired(result, legacy_key=None):
    """#159 S3: a retired process_kind protocol answers with the value-free
    removal envelope — no template body, a pointer to authoring_request and the
    canonical process unit schema, and the refusal code a body would get."""
    assert result["_success"] is True
    assert result["removed"] is True
    assert result["resource_type"] == "process"
    if legacy_key is not None:
        assert legacy_key not in result, legacy_key
    for key in ("process_kind", "example_component_spec", "required_fields",
                "optional_fields", "structured_errors", "deferred_fields"):
        assert key not in result, key
    blob = json.dumps(result)
    assert "PROCESS_COMPONENT_AUTHORING_UNIT_REQUIRED" in blob
    assert "authoring_request" in result["use_instead"]
    assert "ProcessIRV1" in result["use_instead"]["process_unit"]
    for pattern in _FORBIDDEN_PATTERNS:
        assert not re.search(pattern, blob), pattern


def test_template_advertises_process_kind(template):
    """#159 S3 (tombstone candidate): the legacy process_kind template
    this pinned is retired; the protocol answers with the value-free
    removal envelope pointing at authoring_request."""
    _assert_retired(template, 'process_kind')


def test_template_lists_required_fields(template):
    """#159 S3 (tombstone candidate): the legacy process_kind template
    this pinned is retired; the protocol answers with the value-free
    removal envelope pointing at authoring_request."""
    _assert_retired(template, 'required_fields')


def test_template_lists_optional_fields(template):
    """#159 S3 (tombstone candidate): the legacy process_kind template
    this pinned is retired; the protocol answers with the value-free
    removal envelope pointing at authoring_request."""
    _assert_retired(template, 'optional_fields')


def test_template_deferred_fields_lists_unimplemented_surface(template):
    """#159 S3 (tombstone candidate): the legacy process_kind template
    this pinned is retired; the protocol answers with the value-free
    removal envelope pointing at authoring_request."""
    _assert_retired(template, 'deferred_fields')


def test_template_optional_fields_excludes_deferred(template):
    """#159 S3 (tombstone candidate): the legacy process_kind template
    this pinned is retired; the protocol answers with the value-free
    removal envelope pointing at authoring_request."""
    _assert_retired(template, 'optional_fields')


def test_template_example_does_not_use_deferred_fields(template):
    """#159 S3 (tombstone candidate): the legacy process_kind template
    this pinned is retired; the protocol answers with the value-free
    removal envelope pointing at authoring_request."""
    _assert_retired(template, 'example_component_spec')


def test_template_supported_transform_modes(template):
    """#159 S3 (tombstone candidate): the legacy process_kind template
    this pinned is retired; the protocol answers with the value-free
    removal envelope pointing at authoring_request."""
    _assert_retired(template, 'supported_transform_modes')


def test_template_documents_doccacheretrieve_surface(template):
    """#159 S3 (tombstone candidate): the legacy process_kind template
    this pinned is retired; the protocol answers with the value-free
    removal envelope pointing at authoring_request."""
    _assert_retired(template, 'optional_fields')


def test_template_documents_doccacheremove_surface(template):
    """#159 S3 (tombstone candidate): the legacy process_kind template
    this pinned is retired; the protocol answers with the value-free
    removal envelope pointing at authoring_request."""
    _assert_retired(template, 'optional_fields')


def test_template_documents_dataprocess_surface(template):
    """#159 S3 (tombstone candidate): the legacy process_kind template
    this pinned is retired; the protocol answers with the value-free
    removal envelope pointing at authoring_request."""
    _assert_retired(template, 'optional_fields')


def test_template_documents_return_documents_surface(template):
    """#159 S3 (tombstone candidate): the legacy process_kind template
    this pinned is retired; the protocol answers with the value-free
    removal envelope pointing at authoring_request."""
    _assert_retired(template, 'optional_fields')


def test_template_documents_branch_surface(template):
    """#159 S3 (tombstone candidate): the legacy process_kind template
    this pinned is retired; the protocol answers with the value-free
    removal envelope pointing at authoring_request."""
    _assert_retired(template, 'optional_fields')


def test_template_documents_decision_surface(template):
    """#159 S3 (tombstone candidate): the legacy process_kind template
    this pinned is retired; the protocol answers with the value-free
    removal envelope pointing at authoring_request."""
    _assert_retired(template, 'optional_fields')


def test_template_reserved_decision_stage_kind_flipped_in_sync_pipeline():
    """#159 S3 (tombstone candidate): the legacy process_kind template
    this pinned is retired; the protocol answers with the value-free
    removal envelope pointing at authoring_request."""
    _assert_retired(_protocol_template("sync_pipeline"), None)


def test_template_documents_flow_control_surface(template):
    """#159 S3 (tombstone candidate): the legacy process_kind template
    this pinned is retired; the protocol answers with the value-free
    removal envelope pointing at authoring_request."""
    _assert_retired(template, 'optional_fields')


def test_template_reserved_flow_control_stage_kind_flipped_in_sync_pipeline():
    """#159 S3 (tombstone candidate): the legacy process_kind template
    this pinned is retired; the protocol answers with the value-free
    removal envelope pointing at authoring_request."""
    _assert_retired(_protocol_template("sync_pipeline"), None)


def test_both_process_protocols_advertise_return_documents_surface():
    # #159 S3 (tombstone candidate): neither retired protocol serves a template
    # any more, so neither advertises the Return Documents surface; both answer
    # with the same value-free removal envelope.
    linear = _protocol_template("database_to_api_sync")
    wrapper = _protocol_template("wrapper_subprocess")
    for result in (linear, wrapper):
        _assert_retired(result, "optional_fields")
        assert "supported_terminal_shapes" not in result
    assert linear == wrapper


def test_template_documents_catch_exception_surface(template):
    """#159 S3 (tombstone candidate): the legacy process_kind template
    this pinned is retired; the protocol answers with the value-free
    removal envelope pointing at authoring_request."""
    _assert_retired(template, 'optional_fields')


def test_template_supported_dlq_modes(template):
    """#159 S3 (tombstone candidate): the legacy process_kind template
    this pinned is retired; the protocol answers with the value-free
    removal envelope pointing at authoring_request."""
    _assert_retired(template, 'supported_dlq_modes')


def test_template_supported_connector_bindings(template):
    """#159 S3 (tombstone candidate): the legacy process_kind template
    this pinned is retired; the protocol answers with the value-free
    removal envelope pointing at authoring_request."""
    _assert_retired(template, 'supported_connector_action_bindings')


def test_template_documents_all_structured_error_codes(template):
    """#159 S3 (tombstone candidate): the legacy process_kind template
    this pinned is retired; the protocol answers with the value-free
    removal envelope pointing at authoring_request."""
    _assert_retired(template, 'structured_errors')


def test_example_uses_placeholder_labels_and_refs(template):
    """#159 S3 (tombstone candidate): the legacy process_kind template
    this pinned is retired; the protocol answers with the value-free
    removal envelope pointing at authoring_request."""
    _assert_retired(template, 'example_component_spec')


def test_template_obeys_anti_template_rule(template):
    """#159 S3 (tombstone candidate): the legacy process_kind template
    this pinned is retired; the protocol answers with the value-free
    removal envelope pointing at authoring_request."""
    _assert_retired(template, None)


def test_template_lists_catch_notify_optional_fields(template):
    """#159 S3 (tombstone candidate): the legacy process_kind template
    this pinned is retired; the protocol answers with the value-free
    removal envelope pointing at authoring_request."""
    _assert_retired(template, 'optional_fields')


def test_template_lists_supported_notify_levels(template):
    """#159 S3 (tombstone candidate): the legacy process_kind template
    this pinned is retired; the protocol answers with the value-free
    removal envelope pointing at authoring_request."""
    _assert_retired(template, 'supported_notify_levels')


def test_template_documents_notify_config_error(template):
    """#159 S3 (tombstone candidate): the legacy process_kind template
    this pinned is retired; the protocol answers with the value-free
    removal envelope pointing at authoring_request."""
    _assert_retired(template, 'structured_errors')


def test_template_lists_process_extensions_surface(template):
    """#159 S3 (tombstone candidate): the legacy process_kind template
    this pinned is retired; the protocol answers with the value-free
    removal envelope pointing at authoring_request."""
    _assert_retired(template, 'optional_fields')


def test_template_example_includes_process_extensions(template):
    """#159 S3 (tombstone candidate): the legacy process_kind template
    this pinned is retired; the protocol answers with the value-free
    removal envelope pointing at authoring_request."""
    _assert_retired(template, 'example_component_spec')


def test_example_demonstrates_wired_dlq_and_catch_notify(template):
    """#159 S3 (tombstone candidate): the legacy process_kind template
    this pinned is retired; the protocol answers with the value-free
    removal envelope pointing at authoring_request."""
    _assert_retired(template, 'example_component_spec')


def test_wrapper_subprocess_protocol_documented():
    """#159 S3 (tombstone candidate): the legacy process_kind template
    this pinned is retired; the protocol answers with the value-free
    removal envelope pointing at authoring_request."""
    _assert_retired(_protocol_template("wrapper_subprocess"), None)


def test_sync_pipeline_protocol_documented():
    """#159 S3 (tombstone candidate): the legacy process_kind template
    this pinned is retired; the protocol answers with the value-free
    removal envelope pointing at authoring_request."""
    _assert_retired(_protocol_template("sync_pipeline"), None)


def test_sync_pipeline_in_valid_protocols_and_removal_guidance():
    # The retired protocols stay addressable (unknown-protocol errors list them)
    # and every one answers with the removal envelope (#159 S3).
    bad = get_schema_template_action(
        resource_type="process", operation="create", protocol="nope",
    )
    assert "sync_pipeline" in bad["valid_protocols"]
    _assert_retired(_protocol_template("sync_pipeline"), "pipeline")
    removed = get_schema_template_action(resource_type="process", operation="create")
    assert "sync_pipeline" in removed["retired_process_protocols"]
    assert "process_protocols" not in removed


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
    # protocol alone (no operation) still selects the protocol answer — which is
    # now the removal envelope, not the overview (#159 S3).
    result = get_schema_template_action(
        resource_type="process",
        protocol="database_to_api_sync",
    )
    _assert_retired(result, "process_kind")
    assert "available_actions" not in result


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
    # #159 S3: the typed process_kind kinds are retired too; the guidance
    # steers to canonical units and names the retired protocols as such.
    assert set(result["retired_process_protocols"]) == {
        "database_to_api_sync", "wrapper_subprocess", "sync_pipeline",
    }
    blob = json.dumps(result)
    assert "build_from_archetype" not in blob
    assert "authoring_request" in blob
    assert "PROCESS_COMPONENT_AUTHORING_UNIT_REQUIRED" in blob


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


def test_template_documents_flow_sequence_surface(template):
    """#159 S3 (tombstone candidate): the legacy process_kind template
    this pinned is retired; the protocol answers with the value-free
    removal envelope pointing at authoring_request."""
    _assert_retired(template, 'optional_fields')


def test_template_legacy_single_slot_blocks_stay_mutually_exclusive(template):
    """#159 S3 (tombstone candidate): the legacy process_kind template
    this pinned is retired; the protocol answers with the value-free
    removal envelope pointing at authoring_request."""
    _assert_retired(template, 'structured_errors')


def test_template_sync_pipeline_points_multi_shape_to_flow_sequence():
    """#159 S3 (tombstone candidate): the legacy process_kind template
    this pinned is retired; the protocol answers with the value-free
    removal envelope pointing at authoring_request."""
    _assert_retired(_protocol_template("sync_pipeline"), None)


def test_dataprocess_points_at_script_authoring_schema(template):
    """#159 S3 (tombstone candidate): the legacy process_kind template
    this pinned is retired; the protocol answers with the value-free
    removal envelope pointing at authoring_request."""
    _assert_retired(template, None)


# ---------------------------------------------------------------------------
# #175 QA-175-r1-03 — a code the builder can raise must be discoverable from the
# template that governs the protocol.
#
# The finding and QA-175-r1-01 are the same shape: a #175 consequence applied to
# the `wrapper_subprocess` protocol at every layer and to `database_to_api_sync`
# at only one. Two instances of one mechanism, so the response is structural —
# the served list is checked against what the builder ACTUALLY raises, DERIVED by
# driving it, rather than against a hand-maintained expectation that has to be
# remembered next time.
# ---------------------------------------------------------------------------

def test_each_protocol_publishes_the_process_call_code_it_can_raise():
    # #159 S3 (tombstone candidate): the builders this pinned are no longer a
    # served route — a process_kind body is refused at admission — so no
    # protocol template publishes their codes; every protocol serves the same
    # removal envelope instead.
    for protocol in ("database_to_api_sync", "wrapper_subprocess"):
        served = _protocol_template(protocol)
        _assert_retired(served, "structured_errors")
        assert "PROCESS_CALL_CONFIG_INVALID" not in json.dumps(served)
