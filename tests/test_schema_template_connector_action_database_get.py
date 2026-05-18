"""Schema-template tests for component / create / connector-action / database.get.

Issue #23 (M2.3). Anti-template policy: examples MUST use angle-bracket
placeholders only and $ref:KEY tokens — no canned payloads or CDS wrapper
snippets. Database Send/write is OUT OF SCOPE — protocol='database.send'
must return _success: False with an issue-#32 hint.
"""

import json

import pytest

from boomi_mcp.categories.meta_tools import get_schema_template_action


_FORBIDDEN_SECRET_FIELDS = (
    "password",
    "password_ref",
    "secret",
    "token",
    "access_token",
    "client_secret",
)

_FORBIDDEN_TEMPLATE_SUBSTRINGS = (
    "select ",
    "insert ",
    "update ",
    "delete ",
    " from ",
    " where ",
    " join ",
    " group by ",
    " order by ",
    "<sql>",
    "<dbstatement",
    "<databasegetaction",
    "<process",
    "<connector",
    "<?xml",
    "$filter=",
    "$select=",
    "$expand=",
)


def _call(**kwargs):
    return get_schema_template_action(
        resource_type="component",
        operation="create",
        **kwargs,
    )


# ----------------------------------------------------------------------------
# Dispatch
# ----------------------------------------------------------------------------

def test_overview_returned_when_no_protocol_supplied():
    result = _call(component_type="connector-action")
    assert result["_success"] is True
    assert result["component_type"] == "connector-action"
    assert "database.get" in result["available_protocols"]


def test_unknown_protocol_returns_structured_error():
    result = _call(component_type="connector-action", protocol="database.bogus")
    assert result["_success"] is False
    assert "database.get" in result["valid_protocols"]


def test_full_template_returned_for_database_get_protocol():
    result = _call(component_type="connector-action", protocol="database.get")
    assert result["_success"] is True
    assert result["component_type"] == "connector-action"
    assert result["protocol"] == "database.get"


def test_database_send_protocol_returns_explicit_issue_32_error():
    # Send/write is intentionally OUT OF SCOPE for issue #23.
    result = _call(component_type="connector-action", protocol="database.send")
    assert result["_success"] is False
    assert result["error_code"] == "UNSUPPORTED_DB_OPERATION_MODE"
    assert "#32" in result["hint"]


# ----------------------------------------------------------------------------
# Structure
# ----------------------------------------------------------------------------

def test_template_documents_required_fields():
    result = _call(component_type="connector-action", protocol="database.get")
    required = result["required"]
    for expected in (
        "component_type",
        "connector_type",
        "operation_mode",
        "component_name",
        "connection_ref_key",
        "read_profile_id",
    ):
        assert expected in required


def test_template_documents_defaults():
    result = _call(component_type="connector-action", protocol="database.get")
    assert result["defaults"]["batch_count"] == 0
    assert result["defaults"]["max_rows"] == 0
    assert result["defaults"]["operation_mode"] == "get"
    assert result["defaults"]["folder_name"] == "Home"


def test_template_lists_unsupported_operation_modes():
    result = _call(component_type="connector-action", protocol="database.get")
    assert result["supported_operation_modes"] == ["get"]
    assert "send" in result["unsupported_operation_modes"]
    assert "#32" in result["unsupported_operation_modes_note"]


def test_template_documents_link_element_deferral():
    result = _call(component_type="connector-action", protocol="database.get")
    assert result["link_element_status"] == "unsupported_pending_shape_verification"
    assert "link_element" in result["link_element_note"].lower() or \
        "link element" in result["link_element_note"].lower()


def test_template_documents_depends_on_requirements():
    result = _call(component_type="connector-action", protocol="database.get")
    requirements = result["depends_on_requirements"]
    assert any("connection_ref_key" in r for r in requirements)
    assert any("$ref" in r for r in requirements)


def test_template_documents_error_codes():
    result = _call(component_type="connector-action", protocol="database.get")
    codes = result["error_codes"]
    for expected in (
        "UNSUPPORTED_DB_OPERATION_MODE",
        "MISSING_DB_READ_PROFILE_REF",
        "MISSING_DB_DEPENDENCY",
        "UNSUPPORTED_DB_GET_FIELD",
    ):
        assert expected in codes


def test_template_documents_forbidden_secret_fields():
    result = _call(component_type="connector-action", protocol="database.get")
    forbidden = result["forbidden_secret_fields"]
    for field in _FORBIDDEN_SECRET_FIELDS:
        assert field in forbidden


def test_template_documents_out_of_scope_send_pointer():
    result = _call(component_type="connector-action", protocol="database.get")
    out_of_scope = result["out_of_scope"]
    assert "database_send" in out_of_scope
    assert "#32" in out_of_scope["database_send"]


# ----------------------------------------------------------------------------
# Anti-template policy
# ----------------------------------------------------------------------------

def test_example_uses_placeholder_or_ref_values_only():
    result = _call(component_type="connector-action", protocol="database.get")
    example = result["example"]
    assert example["config"]["component_name"].startswith("<<")
    # read_profile_id must be a $ref token so callers learn to use dependency wiring.
    assert example["config"]["read_profile_id"].startswith("$ref:")
    # depends_on must reference the connection + read profile keys.
    assert set(example["depends_on"]) == {"db_connection", "db_read_profile"}


def test_template_and_example_have_no_canned_sql_or_xml_markers():
    result = _call(component_type="connector-action", protocol="database.get")
    # Restrict the scan to template/example/defaults — gotchas + workflow
    # prose may use SQL terms ("read extractions").
    payload = json.dumps({
        "template": result["template"],
        "example": result["example"],
        "defaults": result["defaults"],
    }).lower()
    for marker in _FORBIDDEN_TEMPLATE_SUBSTRINGS:
        assert marker not in payload, (
            f"connector-action example/template contains forbidden marker {marker!r}"
        )


def test_example_carries_explicit_placeholder_disclaimer():
    result = _call(component_type="connector-action", protocol="database.get")
    note = result["example"].get("_example_note", "")
    assert "placeholder" in note.lower()
    assert "$ref" in note


def test_template_does_not_reference_cds_wrapper():
    # CDS may be mentioned in prose (gotchas/workflow) for educational
    # context — what matters is that the actual template/example/defaults
    # carry no CDS-specific values that could be mistaken for reusable
    # material.
    result = _call(component_type="connector-action", protocol="database.get")
    payload = json.dumps({
        "template": result["template"],
        "example": result["example"],
        "defaults": result["defaults"],
    }).lower()
    for marker in ("cds", "intapp", "global sql xml"):
        assert marker not in payload
