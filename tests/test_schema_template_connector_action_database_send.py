"""Schema-template tests for component / create / connector-action / database.send.

Issue #32 (M5.6). Anti-template policy: the template MUST use angle-bracket
placeholders only and $ref:KEY tokens — no canned payloads.
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

def test_overview_lists_database_send():
    result = _call(component_type="connector-action")
    assert result["_success"] is True
    assert "database.send" in result["available_protocols"]


def test_full_template_returned_for_database_send_protocol():
    result = _call(component_type="connector-action", protocol="database.send")
    assert result["_success"] is True
    assert result["component_type"] == "connector-action"
    assert result["protocol"] == "database.send"


# ----------------------------------------------------------------------------
# Structure
# ----------------------------------------------------------------------------

def test_template_documents_required_fields():
    result = _call(component_type="connector-action", protocol="database.send")
    required = result["required"]
    for expected in (
        "component_type",
        "connector_type",
        "operation_mode",
        "component_name",
        "connection_ref_key",
        "write_profile_id",
    ):
        assert expected in required


def test_template_documents_commit_options():
    result = _call(component_type="connector-action", protocol="database.send")
    commit_values = result["commit_option_values"]
    assert "commitprofile" in commit_values
    assert "commitrows" in commit_values


def test_template_defaults():
    result = _call(component_type="connector-action", protocol="database.send")
    defaults = result["defaults"]
    assert defaults["operation_mode"] == "send"
    assert defaults["commit_option"] == "commitprofile"
    assert defaults["batch_count"] == 0
    assert defaults["enable_batching"] is True


def test_template_documents_error_codes():
    result = _call(component_type="connector-action", protocol="database.send")
    codes = result["error_codes"]
    for expected in (
        "UNSUPPORTED_DB_OPERATION_MODE",
        "MISSING_DB_WRITE_PROFILE_REF",
        "INVALID_DB_COMMIT_OPTION",
        "INVALID_DB_BATCH_CONFIG",
        "MISSING_DB_DEPENDENCY",
        "DB_REF_TYPE_MISMATCH",
    ):
        assert expected in codes


def test_template_documents_depends_on_requirements():
    result = _call(component_type="connector-action", protocol="database.send")
    requirements = result["depends_on_requirements"]
    assert any("connection_ref_key" in r for r in requirements)
    assert any("$ref" in r for r in requirements)


def test_template_points_at_write_profile_via_see_also():
    result = _call(component_type="connector-action", protocol="database.send")
    see_also = result.get("see_also", {})
    assert "write_profile" in see_also
    assert "database.write" in see_also["write_profile"]


# ----------------------------------------------------------------------------
# Anti-template policy
# ----------------------------------------------------------------------------

def test_example_uses_placeholder_or_ref_values_only():
    result = _call(component_type="connector-action", protocol="database.send")
    example = result["example"]
    assert example["config"]["component_name"].startswith("<<")
    # write_profile_id must be a $ref token so callers learn dependency wiring.
    assert example["config"]["write_profile_id"].startswith("$ref:")


def test_template_write_profile_id_is_ref_placeholder():
    result = _call(component_type="connector-action", protocol="database.send")
    assert result["template"]["write_profile_id"].startswith("$ref:")


def test_no_canned_sql_or_live_identifiers():
    result = _call(component_type="connector-action", protocol="database.send")
    blob = json.dumps(result).lower()
    for forbidden in ("insert into", "<sql>", "<?xml", "hbm_client", "client_uno"):
        assert forbidden not in blob
