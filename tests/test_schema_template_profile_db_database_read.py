"""Schema-template tests for component / create / profile.db / database.read.

Issue #23 (M2.3). Anti-template policy: examples MUST use angle-bracket
placeholders only (<<task-authored SQL>>, <<field_name>>). No canned SQL,
no CDS wrapper snippets, no table/column names, no Groovy.
"""

import json
import re

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

# Substrings that signal canned SQL / payload / XML templates in an example.
# Checked against lowercase-stringified template + example so word boundaries
# don't matter — any occurrence flags a violation.
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
    "<databaseprofile",
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
    result = _call(component_type="profile.db")
    assert result["_success"] is True
    assert result["component_type"] == "profile.db"
    assert "database.read" in result["available_protocols"]
    assert "hint" in result


def test_unknown_protocol_returns_structured_error():
    result = _call(component_type="profile.db", protocol="database.bogus")
    assert result["_success"] is False
    assert "database.read" in result["valid_protocols"]


def test_full_template_returned_for_database_read_protocol():
    result = _call(component_type="profile.db", protocol="database.read")
    assert result["_success"] is True
    assert result["component_type"] == "profile.db"
    assert result["protocol"] == "database.read"


# ----------------------------------------------------------------------------
# Structure
# ----------------------------------------------------------------------------

def test_template_documents_required_fields():
    result = _call(component_type="profile.db", protocol="database.read")
    required = result["required"]
    assert "component_type" in required
    assert "profile_type" in required
    assert "component_name" in required
    assert "query" in required
    assert "output_fields" in required


def test_template_documents_defaults():
    result = _call(component_type="profile.db", protocol="database.read")
    assert result["defaults"]["profile_type"] == "database.read"
    assert result["defaults"]["folder_name"] == "Home"
    assert result["defaults"]["parameters"] == []


def test_template_documents_output_field_shape():
    result = _call(component_type="profile.db", protocol="database.read")
    shape = result["output_field_shape"]
    assert shape["name"]["required"] is True
    assert shape["data_type"]["default"] == "character"
    assert shape["data_type"]["supported"] == ["character"]


def test_template_documents_parameter_shape():
    result = _call(component_type="profile.db", protocol="database.read")
    shape = result["parameter_shape"]
    assert shape["name"]["required"] is True
    assert shape["data_type"]["default"] == "character"


def test_template_documents_error_codes():
    result = _call(component_type="profile.db", protocol="database.read")
    codes = result["error_codes"]
    for expected in (
        "MISSING_DB_QUERY",
        "MISSING_DB_OUTPUT_FIELDS",
        "UNSUPPORTED_DB_PROFILE_MODE",
        "UNSUPPORTED_DB_PROFILE_FIELD_TYPE",
    ):
        assert expected in codes


def test_template_documents_forbidden_secret_fields():
    result = _call(component_type="profile.db", protocol="database.read")
    forbidden = result["forbidden_secret_fields"]
    for field in _FORBIDDEN_SECRET_FIELDS:
        assert field in forbidden


def test_template_documents_out_of_scope_variants():
    result = _call(component_type="profile.db", protocol="database.read")
    out_of_scope = result["out_of_scope"]
    assert "stored_procedure_read" in out_of_scope
    assert "write_profile" in out_of_scope
    assert "#32" in out_of_scope["write_profile"]


# ----------------------------------------------------------------------------
# Anti-template policy
# ----------------------------------------------------------------------------

def test_example_uses_angle_bracket_placeholders_only():
    result = _call(component_type="profile.db", protocol="database.read")
    example = result["example"]
    # Component_name and query MUST be <<placeholder>>-shaped.
    assert example["config"]["component_name"].startswith("<<")
    assert example["config"]["query"].startswith("<<")
    assert example["config"]["query"].endswith(">>")
    # Output fields must use a placeholder column name.
    for field in example["config"]["output_fields"]:
        assert field["name"].startswith("<<")


def test_template_and_example_have_no_canned_sql_or_xml_markers():
    result = _call(component_type="profile.db", protocol="database.read")
    # Walk the template + example only — gotchas/recommended_workflow may
    # mention SQL keywords in prose (e.g. "Select" capitalized as a noun).
    payload = json.dumps({
        "template": result["template"],
        "example": result["example"],
        "defaults": result["defaults"],
    }).lower()
    for marker in _FORBIDDEN_TEMPLATE_SUBSTRINGS:
        assert marker not in payload, (
            f"profile.db example/template contains forbidden marker {marker!r}"
        )


def test_example_carries_explicit_placeholder_disclaimer():
    result = _call(component_type="profile.db", protocol="database.read")
    note = result["example"].get("_example_note", "")
    assert "placeholder" in note.lower()
    assert "do not copy" in note.lower()


def test_template_does_not_reference_cds_wrapper():
    # CDS may be mentioned in prose for educational context; what matters
    # is that the template/example/defaults carry no CDS-specific values.
    result = _call(component_type="profile.db", protocol="database.read")
    payload = json.dumps({
        "template": result["template"],
        "example": result["example"],
        "defaults": result["defaults"],
    }).lower()
    for marker in ("cds", "intapp", "global sql xml"):
        assert marker not in payload, (
            f"profile.db template contains CDS-specific marker {marker!r}"
        )
