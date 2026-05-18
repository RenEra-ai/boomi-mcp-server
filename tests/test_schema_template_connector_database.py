"""Schema-template lookup tests for the connector-settings / database.sqlserver path.

Complements tests/test_schema_template_unknown_op.py — those tests verify the
unknown-operation contract, these verify the happy path for the new
database connector template.

M2.2 extends the template with auth_mode, credential_ref, supported driver
variants, supported/unsupported auth modes, and forbidden-secret-field
discoverability.
"""

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


def _call(**kwargs):
    return get_schema_template_action(resource_type="component", operation="create", **kwargs)


def test_database_sqlserver_returns_full_template():
    result = _call(component_type="connector-settings", protocol="database.sqlserver")
    assert result["_success"] is True
    assert result["component_type"] == "connector-settings"
    assert result["protocol"] == "database.sqlserver"
    assert result["tool"] == "manage_connector (action='create')"

    template = result["template"]
    for key in ("connector_type", "driver_id", "auth_mode", "component_name", "host", "port",
                "dbname", "username", "credential_ref", "additional"):
        assert key in template, key
    assert template["connector_type"] == "database"
    assert template["driver_id"] == "sqlserver"
    assert template["auth_mode"] == "username_password"

    assert set(result["required"]) >= {
        "connector_type", "driver_id", "auth_mode", "component_name",
        "host", "dbname", "username", "credential_ref",
    }

    assert "password_note" in result
    gotchas = result["gotchas"]
    assert isinstance(gotchas, list) and len(gotchas) >= 2
    joined = " ".join(gotchas).lower()
    assert "host.docker.internal" in joined
    assert "encrypt" in joined and "trustservercertificate" in joined

    example = result["example"]
    assert example["host"] == "host.docker.internal"
    assert example["port"] == 11433
    assert example["additional"] == ";encrypt=true;trustServerCertificate=true"


def test_template_documents_supported_driver_ids():
    result = _call(component_type="connector-settings", protocol="database.sqlserver")
    assert result["supported_driver_ids"] == ["sqlserver", "microsoft_jdbc", "jtds"]


def test_template_documents_supported_and_unsupported_auth_modes():
    result = _call(component_type="connector-settings", protocol="database.sqlserver")
    assert result["supported_auth_modes"] == ["username_password"]
    assert result["unsupported_future_auth_modes"] == ["windows_integrated"]


def test_template_documents_forbidden_secret_fields():
    result = _call(component_type="connector-settings", protocol="database.sqlserver")
    assert set(result["forbidden_secret_fields"]) == set(_FORBIDDEN_SECRET_FIELDS)


def test_template_example_uses_credential_ref_placeholder_not_password():
    result = _call(component_type="connector-settings", protocol="database.sqlserver")

    def _walk(obj):
        if isinstance(obj, dict):
            for k, v in obj.items():
                if k in _FORBIDDEN_SECRET_FIELDS:
                    raise AssertionError(
                        f"Schema template leaks a forbidden secret-shaped key: {k!r}"
                    )
                _walk(v)
        elif isinstance(obj, list):
            for item in obj:
                _walk(item)

    _walk(result["template"])
    _walk(result["example"])

    assert isinstance(result["template"]["credential_ref"], str)
    assert result["template"]["credential_ref"].startswith("credential://")
    assert isinstance(result["example"]["credential_ref"], str)
    assert result["example"]["credential_ref"].startswith("credential://")


def test_template_password_note_mentions_credential_ref():
    result = _call(component_type="connector-settings", protocol="database.sqlserver")
    note = result["password_note"]
    assert "credential_ref" in note
    assert "PLAINTEXT_SECRET_REJECTED" in note


def test_template_driver_note_documents_alias_and_jtds():
    result = _call(component_type="connector-settings", protocol="database.sqlserver")
    note = result.get("driver_note", "")
    assert "microsoft_jdbc" in note
    assert "jtds" in note
    assert "UNSUPPORTED_DB_DRIVER" in note


def test_connector_settings_no_protocol_returns_overview_with_available_protocols():
    result = _call(component_type="connector-settings")
    assert result["_success"] is True
    assert "available_protocols" in result
    assert "database.sqlserver" in result["available_protocols"]


def test_connector_settings_unknown_protocol_returns_structured_error():
    result = _call(component_type="connector-settings", protocol="database.bogus")
    assert result["_success"] is False
    assert "database.bogus" in result["error"]
    assert "database.sqlserver" in result["valid_protocols"]


def test_existing_paths_still_resolve():
    """Adding the new branches must not regress the customlibrary or default paths."""
    cust = _call(component_type="customlibrary")
    assert cust["_success"] is True
    assert cust["component_type"] == "customlibrary"

    default = _call()
    assert default["_success"] is True
    assert "xml_template" in default
