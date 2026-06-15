"""Schema-template tests for component / create / profile.db /
database.stored_procedure_read.

M2.3 follow-up to Issue #23. Anti-template policy: examples MUST use
angle-bracket placeholders only. No canned procedure names, no canned
column names, no values copied from the live reference profile
(legacy-ref-acct (decommissioned) component 439fd4ae).
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

# Substrings that signal canned SQL / payload / XML templates in an example.
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

# Identifiers copied from the live reference profile that MUST NOT appear
# anywhere in the template surface or example. The reference is for shape
# verification only; templates must use placeholder tokens.
_FORBIDDEN_LIVE_IDENTIFIERS = (
    "usp_GetMatterWIPSummary",
    "Expert.dbo",
    "MATTER_CODE", "MATTER_NAME", "CLIENT_CODE", "CLIENT_NAME",
    "OFFC_CODE", "RESP_EMPL_CODE", "RESP_EMPL_NAME",
    "MATTER_STATUS", "CURRENCY_CODE", "TIME_HOURS",
    "TIME_AMOUNT", "COST_AMOUNT", "WIP_TOTAL", "LAST_TIME_DATE",
    "@ClientCode", "@MatterCode", "@OfficeCode",
    "@DateFrom", "@DateTo",
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

def test_overview_lists_stored_procedure_read_protocol():
    result = _call(component_type="profile.db")
    assert result["_success"] is True
    assert "database.stored_procedure_read" in result["available_protocols"]


def test_full_template_returned_for_stored_procedure_protocol():
    result = _call(component_type="profile.db", protocol="database.stored_procedure_read")
    assert result["_success"] is True
    assert result["component_type"] == "profile.db"
    assert result["protocol"] == "database.stored_procedure_read"


def test_unknown_protocol_returns_structured_error():
    result = _call(component_type="profile.db", protocol="database.bogus")
    assert result["_success"] is False
    # Both supported protocols listed in the error
    assert "database.read" in result["valid_protocols"]
    assert "database.stored_procedure_read" in result["valid_protocols"]


# ----------------------------------------------------------------------------
# Structure
# ----------------------------------------------------------------------------

def test_template_documents_required_fields():
    result = _call(component_type="profile.db", protocol="database.stored_procedure_read")
    required = result["required"]
    assert "component_type" in required
    assert "profile_type" in required
    assert "component_name" in required
    assert "procedure_name" in required
    assert "output_fields" in required
    # query is NOT required for SP (SQL goes via procedure invocation)
    assert "query" not in required


def test_template_documents_defaults():
    result = _call(component_type="profile.db", protocol="database.stored_procedure_read")
    assert result["defaults"]["profile_type"] == "database.stored_procedure_read"
    assert result["defaults"]["folder_name"] == "Home"
    assert result["defaults"]["parameters"] == []


def test_template_documents_output_field_shape():
    result = _call(component_type="profile.db", protocol="database.stored_procedure_read")
    shape = result["output_field_shape"]
    assert shape["name"]["required"] is True
    assert shape["data_type"]["default"] == "character"
    assert set(shape["data_type"]["supported"]) == {"character", "number", "datetime"}


def test_template_documents_parameter_shape_with_mode():
    result = _call(component_type="profile.db", protocol="database.stored_procedure_read")
    shape = result["parameter_shape"]
    assert shape["name"]["required"] is True
    assert shape["data_type"]["default"] == "character"
    # SP-specific: mode field documented
    assert "mode" in shape
    assert shape["mode"]["default"] == "in"
    assert set(shape["mode"]["supported"]) == {"in", "out", "in_out", "return"}


def test_template_documents_error_codes():
    result = _call(component_type="profile.db", protocol="database.stored_procedure_read")
    codes = result["error_codes"]
    for expected in (
        "MISSING_DB_PROCEDURE_NAME",
        "MISSING_DB_OUTPUT_FIELDS",
        "INVALID_DB_PARAMETER_MODE",
        "UNSUPPORTED_DB_PROFILE_MODE",
        "UNSUPPORTED_DB_PROFILE_FIELD_TYPE",
        "PLAINTEXT_SECRET_REJECTED",
    ):
        assert expected in codes


def test_template_documents_forbidden_secret_fields():
    result = _call(component_type="profile.db", protocol="database.stored_procedure_read")
    forbidden = result["forbidden_secret_fields"]
    for field in _FORBIDDEN_SECRET_FIELDS:
        assert field in forbidden


def test_template_documents_out_of_scope():
    result = _call(component_type="profile.db", protocol="database.stored_procedure_read")
    out_of_scope = result["out_of_scope"]
    # Pure-action procs and write profiles are explicitly out of scope.
    assert "no_result_set" in out_of_scope
    assert "write_profile" in out_of_scope
    assert "#32" in out_of_scope["write_profile"]


def test_template_points_at_select_variant_via_see_also():
    result = _call(component_type="profile.db", protocol="database.stored_procedure_read")
    see_also = result.get("see_also", {})
    assert "select_statement_read" in see_also
    assert "database.read" in see_also["select_statement_read"]


def test_template_documents_gotchas_for_mode_and_vendor_syntax():
    result = _call(component_type="profile.db", protocol="database.stored_procedure_read")
    gotchas = " ".join(result["gotchas"]).lower()
    # All four modes mentioned, including the explicit "in_out underscore" call-out
    # and the return-direction.
    assert "'in'" in gotchas
    assert "'out'" in gotchas
    assert "'in_out'" in gotchas
    assert "'return'" in gotchas
    assert "underscore" in gotchas  # explicit warning that it's in_out, not inout
    # One-return constraint must be documented.
    assert "only one" in gotchas or "at most one" in gotchas
    # Procedure-name-is-verbatim warning present
    assert "verbatim" in gotchas
    # Self-closing sql noted
    assert "self-closing" in gotchas or "<sql/>" in " ".join(result["gotchas"])


def test_template_documents_multiple_return_error_code():
    result = _call(component_type="profile.db", protocol="database.stored_procedure_read")
    codes = result["error_codes"]
    assert "MULTIPLE_DB_RETURN_PARAMETERS" in codes


# ----------------------------------------------------------------------------
# Anti-template policy
# ----------------------------------------------------------------------------

def test_example_uses_angle_bracket_placeholders_only():
    result = _call(component_type="profile.db", protocol="database.stored_procedure_read")
    example = result["example"]
    assert example["config"]["component_name"].startswith("<<")
    assert example["config"]["procedure_name"].startswith("<<")
    assert example["config"]["procedure_name"].endswith(">>")
    for field in example["config"]["output_fields"]:
        assert field["name"].startswith("<<")
    for parameter in example["config"]["parameters"]:
        assert parameter["name"].startswith("<<")


def test_template_and_example_have_no_canned_sql_or_xml_markers():
    result = _call(component_type="profile.db", protocol="database.stored_procedure_read")
    payload = json.dumps({
        "template": result["template"],
        "example": result["example"],
        "defaults": result["defaults"],
    }).lower()
    for marker in _FORBIDDEN_TEMPLATE_SUBSTRINGS:
        assert marker not in payload, (
            f"SP template/example contains forbidden marker {marker!r}"
        )


def test_template_and_example_have_no_live_reference_identifiers():
    result = _call(component_type="profile.db", protocol="database.stored_procedure_read")
    # Check the entire template payload (gotchas + everything).
    payload = json.dumps(result)
    for marker in _FORBIDDEN_LIVE_IDENTIFIERS:
        assert marker not in payload, (
            f"SP template contains live-reference identifier {marker!r}. "
            f"Templates must use placeholder tokens, not copied identifiers."
        )


def test_example_carries_explicit_placeholder_disclaimer():
    result = _call(component_type="profile.db", protocol="database.stored_procedure_read")
    note = result["example"].get("_example_note", "")
    assert "placeholder" in note.lower()
    assert "do not copy" in note.lower()


def test_template_template_field_uses_placeholders():
    result = _call(component_type="profile.db", protocol="database.stored_procedure_read")
    template = result["template"]
    assert template["component_name"].startswith("<<")
    assert template["procedure_name"].startswith("<<")
    for field in template["output_fields"]:
        assert field["name"].startswith("<<")
    for parameter in template["parameters"]:
        assert parameter["name"].startswith("<<")
