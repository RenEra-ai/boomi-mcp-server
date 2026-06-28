"""Schema-template tests for component / create / profile.db / database.write.

Issue #32 (M5.6). Anti-template policy: the template MUST use angle-bracket
placeholders only — no canned SQL / table / procedure / column values that
could be mistaken for a reusable template.
"""

import json

from boomi_mcp.categories.meta_tools import get_schema_template_action


def _call(**kwargs):
    return get_schema_template_action(
        resource_type="component",
        operation="create",
        **kwargs,
    )


# ----------------------------------------------------------------------------
# Dispatch
# ----------------------------------------------------------------------------

def test_overview_lists_database_write():
    result = _call(component_type="profile.db")
    assert result["_success"] is True
    assert "database.write" in result["available_protocols"]


def test_full_template_returned_for_database_write_protocol():
    result = _call(component_type="profile.db", protocol="database.write")
    assert result["_success"] is True
    assert result["component_type"] == "profile.db"
    assert result["protocol"] == "database.write"


# ----------------------------------------------------------------------------
# Structure
# ----------------------------------------------------------------------------

def test_template_documents_required_fields():
    result = _call(component_type="profile.db", protocol="database.write")
    required = result["required"]
    for expected in (
        "component_type",
        "profile_type",
        "component_name",
        "statement_type",
    ):
        assert expected in required


def test_template_documents_all_statement_types():
    result = _call(component_type="profile.db", protocol="database.write")
    statement_types = result["statement_types"]
    for expected in (
        "standardinsertupdatedelete",
        "dynamicinsert",
        "dynamicupdate",
        "dynamicdelete",
        "storedprocedurewrite",
    ):
        assert expected in statement_types


def test_template_documents_error_codes():
    result = _call(component_type="profile.db", protocol="database.write")
    codes = result["error_codes"]
    for expected in (
        "UNSUPPORTED_DB_STATEMENT_TYPE",
        "MISSING_DB_SQL",
        "MISSING_DB_TABLE_NAME",
        "MISSING_DB_STORED_PROCEDURE",
        "MISSING_DB_FIELDS",
        "MISSING_DB_CONDITIONS",
        "PLAINTEXT_SECRET_REJECTED",
    ):
        assert expected in codes


def test_template_documents_forbidden_secret_fields():
    result = _call(component_type="profile.db", protocol="database.write")
    forbidden = result["forbidden_secret_fields"]
    for field in ("password", "secret", "token"):
        assert field in forbidden


def test_template_points_at_send_operation_via_see_also():
    result = _call(component_type="profile.db", protocol="database.write")
    see_also = result.get("see_also", {})
    assert "send_operation" in see_also
    assert "database.send" in see_also["send_operation"]


# ----------------------------------------------------------------------------
# Anti-template policy
# ----------------------------------------------------------------------------

# SQL/XML markers that must not appear as canned values in the template /
# example / defaults blocks. (Documentation prose — statement_types, gotchas,
# workflow — legitimately uses words like "UPDATE" to explain behavior, so the
# SQL-marker scan is scoped to the value-bearing blocks, per the get-template
# test convention.)
_FORBIDDEN_TEMPLATE_SUBSTRINGS = (
    "insert into",
    "update ",
    "delete from",
    " where ",
    "<sql>",
    "<dbstatement",
    "<?xml",
)

# Live-account identifiers must never leak ANYWHERE in the template output.
_FORBIDDEN_LIVE_IDENTIFIERS = (
    "hbm_client",
    "client_uno",
    "usp_insertclient",
)


def test_template_and_example_have_no_canned_sql_markers():
    result = _call(component_type="profile.db", protocol="database.write")
    payload = json.dumps({
        "template": result["template"],
        "example": result["example"],
        "defaults": result["defaults"],
    }).lower()
    for forbidden in _FORBIDDEN_TEMPLATE_SUBSTRINGS:
        assert forbidden not in payload, f"forbidden template substring: {forbidden!r}"


def test_no_live_identifiers_leak_anywhere():
    result = _call(component_type="profile.db", protocol="database.write")
    blob = json.dumps(result).lower()
    for forbidden in _FORBIDDEN_LIVE_IDENTIFIERS:
        assert forbidden not in blob, f"live identifier leaked: {forbidden!r}"


def test_template_sql_and_table_are_placeholders():
    result = _call(component_type="profile.db", protocol="database.write")
    template = result["template"]
    assert template["sql"].startswith("<<")
    assert template["table_name"].startswith("<<")
    assert template["statement_type"].startswith("<<")
    assert template["fields"][0]["name"].startswith("<<")


def test_example_uses_placeholder_values_only():
    result = _call(component_type="profile.db", protocol="database.write")
    example = result["example"]
    assert example["config"]["component_name"].startswith("<<")
    assert example["config"]["table_name"].startswith("<<")
