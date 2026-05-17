"""Schema-template lookup tests for the connector-settings / database.sqlserver path.

Complements tests/test_schema_template_unknown_op.py — those tests verify the
unknown-operation contract, these verify the happy path for the new
database connector template.
"""

import pytest

from boomi_mcp.categories.meta_tools import get_schema_template_action


def _call(**kwargs):
    return get_schema_template_action(resource_type="component", operation="create", **kwargs)


def test_database_sqlserver_returns_full_template():
    result = _call(component_type="connector-settings", protocol="database.sqlserver")
    assert result["_success"] is True
    assert result["component_type"] == "connector-settings"
    assert result["protocol"] == "database.sqlserver"
    assert result["tool"] == "manage_connector (action='create')"

    template = result["template"]
    for key in ("connector_type", "driver_id", "component_name", "host", "port",
                "dbname", "username", "additional"):
        assert key in template, key
    assert template["connector_type"] == "database"
    assert template["driver_id"] == "sqlserver"

    assert set(result["required"]) == {
        "connector_type", "driver_id", "component_name", "host", "dbname", "username",
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
