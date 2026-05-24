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
    # Issue #31: all 8 driver IDs (including aliases sap_hana / microsoft_jdbc
    # and the underscore form sap_hana) are buildable.
    result = _call(component_type="connector-settings", protocol="database.sqlserver")
    assert result["supported_driver_ids"] == [
        "sqlserver", "microsoft_jdbc", "jtds",
        "oracle", "mysql", "sap_hana", "sap-hana", "custom",
    ]


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

_RECOGNIZED = (
    "sqlserver", "microsoft_jdbc", "jtds",
    "oracle", "mysql", "sap_hana", "sap-hana", "custom",
)
# After Issue #31 every recognized driver is also supported (buildable).
_SUPPORTED = _RECOGNIZED


def test_template_exposes_recognized_driver_ids_including_custom():
    result = _call(component_type="connector-settings", protocol="database.sqlserver")
    assert tuple(result["recognized_driver_ids"]) == _RECOGNIZED


def test_template_supported_driver_ids_now_includes_custom():
    """Issue #31 promoted Custom (and Oracle/MySQL/SAP HANA) to buildable, so
    the recognized/supported asymmetry disappears."""
    result = _call(component_type="connector-settings", protocol="database.sqlserver")
    assert "custom" in result["recognized_driver_ids"]
    assert "custom" in result["supported_driver_ids"]
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


def test_template_driver_variants_custom_is_buildable_with_custom_url_shape():
    """Issue #31: Custom flips to buildable=True with the custom_url shape."""
    result = _call(component_type="connector-settings", protocol="database.sqlserver")
    custom = result["driver_variants"]["custom"]
    assert custom["shape"] == "custom_url"
    assert custom["buildable"] is True
    assert custom["emits_driver_id"] == "custom"
    assert custom["class_name_source"] == "custom_class_name"
    assert custom["url_format_source"] == "connection_url"
    assert custom["default_port"] is None
    # Caller-side required fields visible — host/port/dbname/additional absent.
    required = set(custom["required"])
    assert {"custom_class_name", "connection_url", "credential_ref"} <= required
    assert "host" not in required
    assert "port" not in required
    # Live #Common reference + runtime driver prereq exposed for clients.
    assert custom["live_reference_component_id"] == "39fb519d-e970-4aaf-a1f7-4eba39158e9d"
    assert "library" in custom["runtime_driver_prerequisite"].lower()


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


# ---------------------------------------------------------------------------
# Issue #31 — new buildable driver variants + shape_metadata block
# ---------------------------------------------------------------------------


def test_template_driver_variants_oracle():
    result = _call(component_type="connector-settings", protocol="database.sqlserver")
    oracle = result["driver_variants"]["oracle"]
    assert oracle["shape"] == "host_port_db"
    assert oracle["buildable"] is True
    assert oracle["emits_driver_id"] == "oracle"
    assert oracle["class_name"] == "oracle.jdbc.driver.OracleDriver"
    assert oracle["url_format"] == "jdbc:oracle:thin:@{0}:{1}:{2}"
    assert oracle["default_port"] == 1521
    # Codex r2: oracle accepts `additional` like the other host_port_db
    # drivers (Boomi appends it to the URL end). The note documents the
    # runtime caveat — Oracle Thin SID may not accept arbitrary trailing
    # options — and points at custom_url for service-name URLs.
    assert "additional_supported" not in oracle
    assert "additional" in oracle["note"].lower()
    assert "service" in oracle["note"].lower()
    assert oracle["live_reference_component_id"] == "6adf9e1e-39c8-4104-bc6c-9769b93aa161"


def test_template_driver_variants_mysql():
    result = _call(component_type="connector-settings", protocol="database.sqlserver")
    mysql = result["driver_variants"]["mysql"]
    assert mysql["shape"] == "host_port_db"
    assert mysql["buildable"] is True
    assert mysql["emits_driver_id"] == "mysql"
    assert mysql["class_name"] == "com.mysql.jdbc.Driver"
    assert mysql["url_format"] == "jdbc:mysql://{0}:{1}/{2}{3}"
    assert mysql["default_port"] == 3306
    assert mysql["live_reference_component_id"] == "bfbfea6f-39c7-498e-859b-6036959a20c8"
    assert "library" in mysql["runtime_driver_prerequisite"].lower()


def test_template_driver_variants_sap_hana_canonical():
    result = _call(component_type="connector-settings", protocol="database.sqlserver")
    sap = result["driver_variants"]["sap-hana"]
    assert sap["shape"] == "host_port_db"
    assert sap["buildable"] is True
    assert sap["emits_driver_id"] == "sap-hana"
    assert sap["class_name"] == "com.sap.db.jdbc.Driver"
    assert sap["url_format"] == "jdbc:sap://{0}:{1}/?databaseName={2}{3}"
    # No verified default port — callers must supply it.
    assert sap["default_port"] is None
    assert sap["port_required"] is True
    # port shows up in the variant-level required list to make this obvious.
    assert "port" in sap["required"]
    assert sap["live_reference_component_id"] == "c9077711-39a4-4d52-9f91-27bdf1f5b8ec"
    assert "ngdbc" in sap["runtime_driver_prerequisite"].lower()


def test_template_driver_variants_sap_hana_underscore_alias():
    result = _call(component_type="connector-settings", protocol="database.sqlserver")
    assert result["driver_variants"]["sap_hana"] == {"alias_of": "sap-hana"}


def test_template_exposes_shape_metadata_for_both_shapes():
    result = _call(component_type="connector-settings", protocol="database.sqlserver")
    shape_meta = result["shape_metadata"]

    host_port_db = shape_meta["host_port_db"]
    assert set(host_port_db["required"]) >= {
        "component_name", "driver_id", "auth_mode",
        "host", "dbname", "username", "credential_ref",
    }
    assert "port" in host_port_db["optional"]
    assert "additional" in host_port_db["optional"]
    assert host_port_db["forbidden"] == ["custom_class_name", "connection_url"]
    applies_to = set(host_port_db["applies_to"])
    assert {"sqlserver", "microsoft_jdbc", "jtds", "oracle", "mysql",
            "sap_hana", "sap-hana"} <= applies_to
    assert "custom" not in applies_to

    custom_url = shape_meta["custom_url"]
    assert set(custom_url["required"]) >= {
        "component_name", "driver_id", "auth_mode",
        "custom_class_name", "connection_url", "username", "credential_ref",
    }
    assert custom_url["forbidden"] == ["host", "port", "dbname", "additional"]
    assert custom_url["applies_to"] == ["custom"]


@pytest.mark.parametrize("driver_id", ["mysql", "sap-hana", "custom"])
def test_template_runtime_driver_prerequisite_present_for_non_bundled_drivers(driver_id):
    """MySQL Connector/J, SAP HANA ngdbc, and Custom JDBC jars are not
    pre-deployed on the Boomi runtime — surfaced for clients."""
    result = _call(component_type="connector-settings", protocol="database.sqlserver")
    variant = result["driver_variants"][driver_id]
    assert "runtime_driver_prerequisite" in variant
    assert variant["runtime_driver_prerequisite"]


@pytest.mark.parametrize("driver_id", ["sqlserver", "jtds", "oracle"])
def test_template_no_runtime_driver_prerequisite_on_bundled_drivers(driver_id):
    """SQL Server JDBC, jTDS, and Oracle Thin ship with the Boomi runtime."""
    result = _call(component_type="connector-settings", protocol="database.sqlserver")
    variant = result["driver_variants"][driver_id]
    assert "runtime_driver_prerequisite" not in variant


def test_template_driver_note_mentions_all_supported_drivers_and_shapes():
    """The free-text driver_note must catch up with Issue #31 — it's the
    discoverability surface for human readers who skim the schema."""
    result = _call(component_type="connector-settings", protocol="database.sqlserver")
    note = result["driver_note"]
    for needle in ("sqlserver", "microsoft_jdbc", "jtds",
                   "oracle", "mysql", "sap_hana", "custom_url"):
        assert needle in note, needle


def test_template_gotchas_cover_sap_hana_port_and_runtime_jars():
    """sap-hana port and runtime jar requirements must be visible in gotchas."""
    result = _call(component_type="connector-settings", protocol="database.sqlserver")
    joined = " ".join(result["gotchas"]).lower()
    assert "sap hana" in joined or "sap_hana" in joined or "sap-hana" in joined
    assert "port" in joined
    assert "library" in joined or "ngdbc" in joined
