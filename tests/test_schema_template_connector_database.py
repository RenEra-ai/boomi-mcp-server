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


# ---------------------------------------------------------------------------
# Issue #31 — driver_variants, recognized_driver_ids, pooling, write_options
# ---------------------------------------------------------------------------

_RECOGNIZED = ("sqlserver", "microsoft_jdbc", "jtds", "custom")
_SUPPORTED = ("sqlserver", "microsoft_jdbc", "jtds")


def test_template_exposes_recognized_driver_ids_including_custom():
    result = _call(component_type="connector-settings", protocol="database.sqlserver")
    assert tuple(result["recognized_driver_ids"]) == _RECOGNIZED


def test_template_supported_driver_ids_excludes_custom():
    """Explicit asymmetry: custom is recognized (so callers can discover it)
    but NOT supported (the builder cannot emit it yet)."""
    result = _call(component_type="connector-settings", protocol="database.sqlserver")
    assert "custom" in result["recognized_driver_ids"]
    assert "custom" not in result["supported_driver_ids"]
    assert tuple(result["supported_driver_ids"]) == _SUPPORTED


def test_template_exposes_driver_variants_for_every_recognized_driver():
    result = _call(component_type="connector-settings", protocol="database.sqlserver")
    assert set(result["driver_variants"]) == set(_RECOGNIZED)


def test_template_driver_variants_sqlserver_carries_shape_buildable_class_url_port():
    result = _call(component_type="connector-settings", protocol="database.sqlserver")
    sqlserver = result["driver_variants"]["sqlserver"]
    assert sqlserver["shape"] == "host_port_db"
    assert sqlserver["buildable"] is True
    assert sqlserver["emits_driver_id"] == "sqlserver"
    assert sqlserver["class_name"] == "com.microsoft.sqlserver.jdbc.SQLServerDriver"
    assert sqlserver["url_format"] == "jdbc:sqlserver://{0}:{1};database={2}{3}"
    assert sqlserver["default_port"] == 1433
    assert "credential_ref" in sqlserver["required"]
    assert "component_name" in sqlserver["required"]


def test_template_driver_variants_jtds_shape_and_class():
    result = _call(component_type="connector-settings", protocol="database.sqlserver")
    jtds = result["driver_variants"]["jtds"]
    assert jtds["shape"] == "host_port_db"
    assert jtds["buildable"] is True
    assert jtds["emits_driver_id"] == "jtds"
    assert jtds["class_name"] == "net.sourceforge.jtds.jdbc.Driver"
    assert jtds["url_format"] == "jdbc:jtds:sqlserver://{0}:{1}/{2}{3}"


def test_template_driver_variants_microsoft_jdbc_is_alias_of_sqlserver():
    result = _call(component_type="connector-settings", protocol="database.sqlserver")
    assert result["driver_variants"]["microsoft_jdbc"] == {"alias_of": "sqlserver"}


def test_template_driver_variants_custom_is_unsupported_with_shape_error_code():
    result = _call(component_type="connector-settings", protocol="database.sqlserver")
    custom = result["driver_variants"]["custom"]
    assert custom["shape"] == "custom_url"
    assert custom["buildable"] is False
    assert custom["unsupported_error_code"] == "UNSUPPORTED_DB_DRIVER_SHAPE"
    reason = custom["unsupported_reason"].lower()
    assert "custom" in reason
    assert "raw-xml" in reason or "raw xml" in reason or "reuse" in reason


def test_template_recommended_additional_present_for_sqlserver_not_jtds():
    """Microsoft JDBC ≥12 TLS workaround surface — discoverable on sqlserver,
    not on jtds (jTDS has no TLS by default, doesn't need the clause)."""
    result = _call(component_type="connector-settings", protocol="database.sqlserver")
    assert "recommended_additional" in result["driver_variants"]["sqlserver"]
    assert "encrypt=true" in result["driver_variants"]["sqlserver"]["recommended_additional"]
    assert "recommended_additional" not in result["driver_variants"]["jtds"]


# --- pooling schema sub-tree ---------------------------------------------

_POOLING_KEYS = (
    "enabled", "exhausted_action", "max_active", "max_idle", "max_idle_time",
    "max_wait", "min_idle", "number_of_tests", "test_idle", "test_on_borrow",
    "test_on_return", "time_between_runs", "validation_query",
)


def test_template_exposes_pooling_schema_with_defaults():
    result = _call(component_type="connector-settings", protocol="database.sqlserver")
    pooling = result["pooling"]
    assert "description" in pooling
    assert pooling["error_code"] == "DATABASE_POOLING_VALIDATION_FAILED"
    assert pooling["defaults_when_omitted"] == {
        "enabled": False,
        "exhausted_action": 1,
        "max_active": 0,
        "max_idle": 0,
        "max_idle_time": 0,
        "max_wait": 0,
        "min_idle": 0,
        "number_of_tests": 0,
        "test_idle": False,
        "test_on_borrow": False,
        "test_on_return": False,
        "time_between_runs": 0,
        "validation_query": "",
    }
    assert pooling["defaults_when_enabled"] == {
        "enabled": True,
        "exhausted_action": 1,
        "max_active": -1,
        "max_idle": -1,
        "max_idle_time": 0,
        "max_wait": 0,
        "min_idle": 0,
        "number_of_tests": 0,
        "test_idle": False,
        "test_on_borrow": False,
        "test_on_return": False,
        "time_between_runs": 0,
        "validation_query": "",
    }


def test_template_pooling_schema_includes_all_thirteen_keys():
    result = _call(component_type="connector-settings", protocol="database.sqlserver")
    pooling_fields = result["pooling"]["fields"]
    assert set(pooling_fields) == set(_POOLING_KEYS)


# --- write_options schema sub-tree ---------------------------------------

def test_template_exposes_write_options_schema_with_defaults():
    result = _call(component_type="connector-settings", protocol="database.sqlserver")
    wo = result["write_options"]
    assert "description" in wo
    assert wo["error_code"] == "DATABASE_WRITE_OPTIONS_VALIDATION_FAILED"
    assert wo["defaults"] == {"write_sql_to_file": False, "sql_file_path": "tmp/sqldebug.txt"}
    assert set(wo["fields"]) == {"write_sql_to_file", "sql_file_path"}


def test_template_example_pooling_and_write_options_show_explicit_defaults():
    result = _call(component_type="connector-settings", protocol="database.sqlserver")
    example = result["example"]
    assert example["pooling"] == {"enabled": False}
    assert example["write_options"] == {
        "write_sql_to_file": False,
        "sql_file_path": "tmp/sqldebug.txt",
    }
