"""Unit tests for DatabaseConnectorBuilder.

Verifies the XML matches the structure of a real exported Boomi component
(MS SQL Server Microsoft, component 4ace95d7-6ee4-4f83-8fad-723d3fabdb2f
on the renera account) and that field-level defaults / required-field
validation behave correctly.

M2.2 adds: auth_mode, credential_ref, jtds driver, microsoft_jdbc alias,
structured BuilderValidationError envelope, plaintext-secret rejection
beyond just `password`.
"""

import xml.etree.ElementTree as ET
from unittest.mock import MagicMock

import pytest

from boomi_mcp.categories.components.builders.connector_builder import (
    BuilderValidationError,
    CONNECTOR_BUILDERS,
    DatabaseConnectorBuilder,
    get_connector_builder,
)
from boomi_mcp.categories.components.connectors import create_connector


NS = {"bns": "http://api.platform.boomi.com/"}


def _minimal_config(**overrides):
    """Raw config dict — used for validate_config + create_connector tests."""
    params = {
        "connector_type": "database",
        "component_name": "Test SQL",
        "driver_id": "sqlserver",
        "auth_mode": "username_password",
        "host": "host.docker.internal",
        "dbname": "Expert",
        "username": "sa",
        "credential_ref": "credential://test/sqlserver/password",
    }
    params.update(overrides)
    return params


def _build_minimal(**overrides):
    """Render minimal-valid XML — used for XML-shape tests."""
    params = _minimal_config(**overrides)
    params.pop("connector_type", None)  # builder doesn't consume this key
    return DatabaseConnectorBuilder().build(**params)


def test_database_registered_in_connector_builders():
    assert "database" in CONNECTOR_BUILDERS
    assert get_connector_builder("database").__class__ is DatabaseConnectorBuilder


def test_minimum_required_fields_produce_valid_xml():
    xml = _build_minimal()
    root = ET.fromstring(xml)

    assert root.tag == "{http://api.platform.boomi.com/}Component"
    assert root.attrib["type"] == "connector-settings"
    assert root.attrib["subType"] == "database"
    assert root.attrib["name"] == "Test SQL"
    assert root.attrib["folderName"] == "Home"


def test_encrypted_password_block_marked_unset():
    xml = _build_minimal()
    root = ET.fromstring(xml)
    enc = root.find("bns:encryptedValues/bns:encryptedValue", NS)
    assert enc is not None
    assert enc.attrib["path"] == "//DatabaseConnectionSettings/@password"
    assert enc.attrib["isSet"] == "false"


def test_database_connection_settings_attributes_match_working_component():
    xml = _build_minimal(
        component_name="MS SQL Server Microsoft",
        folder_name="Process Library",
        host="host.docker.internal",
        dbname="Expert",
        username="sa",
        port=11433,
        additional=";encrypt=true;trustServerCertificate=true",
    )
    root = ET.fromstring(xml)
    dcs = root.find("bns:object/DatabaseConnectionSettings", NS)
    assert dcs is not None
    attrs = dict(dcs.attrib)

    assert attrs["driverId"] == "sqlserver"
    assert attrs["className"] == "com.microsoft.sqlserver.jdbc.SQLServerDriver"
    assert attrs["urlFormat"] == "jdbc:sqlserver://{0}:{1};database={2}{3}"
    assert attrs["host"] == "host.docker.internal"
    assert attrs["port"] == "11433"
    assert attrs["dbname"] == "Expert"
    assert attrs["username"] == "sa"
    assert attrs["isPoolEnabled"] == "false"
    assert attrs["additional"] == ";encrypt=true;trustServerCertificate=true"


def test_default_port_falls_back_to_driver_default():
    xml = _build_minimal()
    root = ET.fromstring(xml)
    dcs = root.find("bns:object/DatabaseConnectionSettings", NS)
    assert dcs.attrib["port"] == "1433"


def test_write_options_and_adapter_pool_info_defaults_present():
    xml = _build_minimal()
    root = ET.fromstring(xml)
    dcs = root.find("bns:object/DatabaseConnectionSettings", NS)

    wo = dcs.find("WriteOptions")
    assert wo is not None
    assert wo.attrib["sqlFilePath"] == "tmp/sqldebug.txt"
    assert wo.attrib["writeSQLToFile"] == "false"

    api = dcs.find("AdapterPoolInfo")
    assert api is not None
    assert api.attrib["exhaustedAction"] == "1"
    for zero_attr in ("maxActive", "maxIdle", "maxIdleTime", "maxWait",
                      "minIdle", "numberOfTests", "timeBetweenRuns"):
        assert api.attrib[zero_attr] == "0", zero_attr
    for false_attr in ("testIdle", "testOnBorrow", "testOnReturn"):
        assert api.attrib[false_attr] == "false", false_attr
    assert api.attrib["validationQuery"] == ""


@pytest.mark.parametrize(
    "missing",
    ["component_name", "driver_id", "auth_mode", "host", "dbname", "username", "credential_ref"],
)
def test_missing_required_field_raises_value_error(missing):
    params = {
        "component_name": "Test SQL",
        "driver_id": "sqlserver",
        "auth_mode": "username_password",
        "host": "host.docker.internal",
        "dbname": "Expert",
        "username": "sa",
        "credential_ref": "credential://test/sqlserver/password",
    }
    params.pop(missing)
    with pytest.raises(BuilderValidationError) as excinfo:
        DatabaseConnectorBuilder().build(**params)
    assert excinfo.value.field == missing


def test_unknown_driver_id_raises_structured_error():
    # Postgres remains unsupported in Issue #31 — no verified live #Common
    # export to anchor the XML emission against, so the builder rejects it.
    with pytest.raises(BuilderValidationError) as excinfo:
        DatabaseConnectorBuilder().build(**_minimal_config(driver_id="postgres"))
    err = excinfo.value
    assert err.error_code == "UNSUPPORTED_DB_DRIVER"
    assert err.field == "driver_id"
    assert "sqlserver" in (err.hint or "")
    assert "jtds" in (err.hint or "")


def test_password_in_config_is_rejected_loudly():
    """Silently dropping a supplied password would yield an unusable connection
    that appears configured. Builder must fail with structured error instead."""
    with pytest.raises(BuilderValidationError) as excinfo:
        DatabaseConnectorBuilder().build(**_minimal_config(password="hunter2"))
    err = excinfo.value
    assert err.error_code == "PLAINTEXT_SECRET_REJECTED"
    assert err.field == "password"


def test_special_xml_characters_in_values_are_escaped():
    xml = _build_minimal(
        component_name='SQL "Prod" & <Dev>',
        dbname='db&name',
        additional=";encrypt=true&special=<x>",
    )
    root = ET.fromstring(xml)
    assert root.attrib["name"] == 'SQL "Prod" & <Dev>'
    dcs = root.find("bns:object/DatabaseConnectionSettings", NS)
    assert dcs.attrib["dbname"] == "db&name"
    assert dcs.attrib["additional"] == ";encrypt=true&special=<x>"


# ---------------------------------------------------------------------------
# M2.2 additions
# ---------------------------------------------------------------------------

def test_microsoft_jdbc_alias_resolves_to_sqlserver_driver_id():
    xml = _build_minimal(driver_id="microsoft_jdbc")
    root = ET.fromstring(xml)
    dcs = root.find("bns:object/DatabaseConnectionSettings", NS)
    assert dcs.attrib["driverId"] == "sqlserver"
    assert dcs.attrib["className"] == "com.microsoft.sqlserver.jdbc.SQLServerDriver"
    assert dcs.attrib["urlFormat"] == "jdbc:sqlserver://{0}:{1};database={2}{3}"


def test_jtds_driver_emits_jtds_class_and_url_format():
    xml = _build_minimal(driver_id="jtds")
    root = ET.fromstring(xml)
    dcs = root.find("bns:object/DatabaseConnectionSettings", NS)
    assert dcs.attrib["driverId"] == "jtds"
    assert dcs.attrib["className"] == "net.sourceforge.jtds.jdbc.Driver"
    assert dcs.attrib["urlFormat"] == "jdbc:jtds:sqlserver://{0}:{1}/{2}{3}"
    assert dcs.attrib["port"] == "1433"


def test_credential_ref_required_for_username_password_auth():
    params = _minimal_config()
    params.pop("credential_ref")
    with pytest.raises(BuilderValidationError) as excinfo:
        DatabaseConnectorBuilder().build(**{k: v for k, v in params.items() if k != "connector_type"})
    err = excinfo.value
    assert err.error_code == "MISSING_CREDENTIAL_REF"
    assert err.field == "credential_ref"


def test_credential_ref_not_written_to_emitted_xml():
    sentinel = "credential://SENTINEL_REF_DEADBEEF/secret"
    xml = _build_minimal(credential_ref=sentinel)
    assert "SENTINEL_REF_DEADBEEF" not in xml
    assert "credential_ref" not in xml
    assert "credential://" not in xml


def test_windows_integrated_auth_mode_returns_unsupported_error():
    with pytest.raises(BuilderValidationError) as excinfo:
        DatabaseConnectorBuilder().build(**_minimal_config(auth_mode="windows_integrated"))
    err = excinfo.value
    assert err.error_code == "UNSUPPORTED_DB_AUTH_MODE"
    assert err.field == "auth_mode"
    assert "future" in (err.hint or "").lower()


def test_unknown_auth_mode_returns_unsupported_error():
    with pytest.raises(BuilderValidationError) as excinfo:
        DatabaseConnectorBuilder().build(**_minimal_config(auth_mode="kerberos_v5"))
    err = excinfo.value
    assert err.error_code == "UNSUPPORTED_DB_AUTH_MODE"
    assert err.field == "auth_mode"
    assert "username_password" in (err.hint or "")


# Issue #31 promoted oracle, mysql, sap-hana, and custom to buildable.
# Only drivers without a verified live #Common reference (Postgres, DB2,
# Snowflake-via-host_port_db, etc.) remain UNSUPPORTED_DB_DRIVER.
@pytest.mark.parametrize("unsupported_driver", ["postgres", "db2", "snowflake"])
def test_unsupported_driver_returns_unsupported_db_driver_error(unsupported_driver):
    with pytest.raises(BuilderValidationError) as excinfo:
        DatabaseConnectorBuilder().build(**_minimal_config(driver_id=unsupported_driver))
    err = excinfo.value
    assert err.error_code == "UNSUPPORTED_DB_DRIVER"
    assert err.field == "driver_id"
    assert "sqlserver" in (err.hint or "")


@pytest.mark.parametrize(
    "forbidden",
    ["password", "password_ref", "secret", "token", "access_token", "client_secret"],
)
def test_forbidden_secret_fields_rejected(forbidden):
    params = _minimal_config(**{forbidden: "leaked_value_DEADBEEF"})
    params.pop("connector_type", None)
    with pytest.raises(BuilderValidationError) as excinfo:
        DatabaseConnectorBuilder().build(**params)
    err = excinfo.value
    assert err.error_code == "PLAINTEXT_SECRET_REJECTED"
    assert err.field == forbidden
    # Crucially, the offender's value must not appear in the error message —
    # field name is fine, value leakage is not.
    assert "leaked_value_DEADBEEF" not in str(err)
    assert "leaked_value_DEADBEEF" not in (err.hint or "")


def test_validate_config_returns_error_without_raising():
    err = DatabaseConnectorBuilder.validate_config(
        _minimal_config(driver_id="db2")
    )
    assert isinstance(err, BuilderValidationError)
    assert err.error_code == "UNSUPPORTED_DB_DRIVER"


def test_validate_config_returns_none_for_valid_config():
    assert DatabaseConnectorBuilder.validate_config(_minimal_config()) is None


def test_scan_forbidden_secret_fields_returns_first_offender():
    """Independent classmethod — callers (integration_builder preflight)
    use this to reject plaintext secrets even when the builder won't run."""
    err = DatabaseConnectorBuilder.scan_forbidden_secret_fields(
        {"connector_type": "database", "password": "leak"}
    )
    assert isinstance(err, BuilderValidationError)
    assert err.error_code == "PLAINTEXT_SECRET_REJECTED"
    assert err.field == "password"


def test_scan_forbidden_secret_fields_returns_none_when_clean():
    assert DatabaseConnectorBuilder.scan_forbidden_secret_fields(
        {"connector_type": "database",
         "credential_ref": "credential://opaque/ref"}
    ) is None


def test_builder_validation_error_carries_error_code_field_hint():
    err = BuilderValidationError(
        "boom",
        error_code="X_CODE",
        field="some_field",
        hint="do this",
    )
    assert isinstance(err, ValueError)  # so `except ValueError` still catches it
    assert str(err) == "boom"
    assert err.error_code == "X_CODE"
    assert err.field == "some_field"
    assert err.hint == "do this"


# ---------------------------------------------------------------------------
# create_connector wrapper — structured-error envelope
# ---------------------------------------------------------------------------

def _make_boomi_mock_with_database_type():
    """Boomi client mock where connector.get_connector('database') returns a
    truthy object (so create_connector reaches the builder)."""
    client = MagicMock()
    client.connector.get_connector.return_value = MagicMock()
    return client


def test_create_connector_returns_structured_error_for_database_builder_validation():
    client = _make_boomi_mock_with_database_type()
    bad_config = _minimal_config(auth_mode="windows_integrated")
    result = create_connector(client, "test", bad_config)
    assert result["_success"] is False
    assert result["error_code"] == "UNSUPPORTED_DB_AUTH_MODE"
    assert result["field"] == "auth_mode"
    assert result["hint"]
    assert result["profile"] == "test"
    # _create_component_raw must not have been called.
    client.component.create_component_raw.assert_not_called()


def test_create_connector_secret_value_not_echoed_in_structured_error():
    """If a caller passes a real-looking credential_ref alongside a different
    failure, the error envelope must surface field names, never values."""
    client = _make_boomi_mock_with_database_type()
    sentinel = "credential://SENTINEL_VALUE_DEADBEEF/secret"
    bad_config = _minimal_config(credential_ref=sentinel)
    bad_config.pop("host")  # trigger a different validation failure
    result = create_connector(client, "test", bad_config)
    assert result["_success"] is False
    for v in result.values():
        if isinstance(v, str):
            assert "SENTINEL_VALUE_DEADBEEF" not in v


# ---------------------------------------------------------------------------
# Issue #31 — driver shape discriminator, pooling, write_options
# ---------------------------------------------------------------------------

def _adapter_pool_info_attrs(xml: str) -> dict:
    root = ET.fromstring(xml)
    api = root.find("bns:object/DatabaseConnectionSettings/AdapterPoolInfo", NS)
    assert api is not None
    return dict(api.attrib)


def _write_options_attrs(xml: str) -> dict:
    root = ET.fromstring(xml)
    wo = root.find("bns:object/DatabaseConnectionSettings/WriteOptions", NS)
    assert wo is not None
    return dict(wo.attrib)


def _is_pool_enabled(xml: str) -> str:
    root = ET.fromstring(xml)
    dcs = root.find("bns:object/DatabaseConnectionSettings", NS)
    return dcs.attrib["isPoolEnabled"]


def test_all_recognized_drivers_are_now_supported():
    # Issue #31: every recognized driver is also buildable. No asymmetry
    # remains until a future driver is added in deferred form.
    recognized = set(DatabaseConnectorBuilder.RECOGNIZED_DRIVER_IDS)
    supported = set(DatabaseConnectorBuilder.SUPPORTED_DRIVER_IDS)
    assert recognized == supported
    for required_id in (
        "sqlserver", "microsoft_jdbc", "jtds",
        "oracle", "mysql", "sap_hana", "sap-hana", "custom",
    ):
        assert required_id in recognized, required_id


def test_driver_registry_carries_shape_and_buildable_metadata():
    for driver_id in ("sqlserver", "jtds", "oracle", "mysql", "sap-hana", "custom"):
        entry = DatabaseConnectorBuilder.DRIVERS[driver_id]
        assert "shape" in entry, driver_id
        assert "buildable" in entry, driver_id
        assert entry["buildable"] is True, driver_id
    assert DatabaseConnectorBuilder.DRIVERS["custom"]["shape"] == "custom_url"
    assert DatabaseConnectorBuilder.DRIVERS["sap-hana"]["port_required"] is True
    assert DatabaseConnectorBuilder.DRIVERS["sap-hana"]["default_port"] is None


def test_custom_driver_with_only_host_port_db_fields_fails_with_shape_mismatch():
    """Custom is buildable now (Issue #31) but uses the custom_url shape,
    which requires custom_class_name + connection_url and rejects host."""
    params = _minimal_config(driver_id="custom")  # carries host/dbname/etc.
    with pytest.raises(BuilderValidationError) as excinfo:
        DatabaseConnectorBuilder().build(**{k: v for k, v in params.items() if k != "connector_type"})
    err = excinfo.value
    assert err.error_code == "DATABASE_CONNECTOR_VALIDATION_FAILED"
    # custom_class_name is required and listed before forbidden fields in
    # the validation order, so the first failure is the missing required.
    assert err.field == "custom_class_name"


def test_recommended_additional_is_metadata_not_enforcement():
    """The sqlserver driver's recommended_additional appears in DRIVERS metadata
    but the builder does NOT inject it or warn when omitted. Caller chooses."""
    sqlserver = DatabaseConnectorBuilder.DRIVERS["sqlserver"]
    assert "encrypt=true" in sqlserver["recommended_additional"]
    # build() with no additional clause succeeds silently
    xml = _build_minimal()  # additional defaults to ""
    root = ET.fromstring(xml)
    dcs = root.find("bns:object/DatabaseConnectionSettings", NS)
    assert dcs.attrib["additional"] == ""


# --- Pooling --------------------------------------------------------------

def test_omitted_pooling_preserves_default_adapter_pool_info_xml_exactly():
    xml = _build_minimal()  # no pooling key
    attrs = _adapter_pool_info_attrs(xml)
    expected = {
        "exhaustedAction": "1",
        "maxActive": "0", "maxIdle": "0", "maxIdleTime": "0", "maxWait": "0",
        "minIdle": "0", "numberOfTests": "0",
        "testIdle": "false", "testOnBorrow": "false", "testOnReturn": "false",
        "timeBetweenRuns": "0",
        "validationQuery": "",
    }
    assert attrs == expected
    assert _is_pool_enabled(xml) == "false"


def test_pooling_explicitly_disabled_emits_same_xml_as_omitted():
    a = _build_minimal()
    b = _build_minimal(pooling={"enabled": False})
    assert _adapter_pool_info_attrs(a) == _adapter_pool_info_attrs(b)
    assert _is_pool_enabled(a) == _is_pool_enabled(b) == "false"


def test_pooling_enabled_true_defaults_emit_max_active_minus_one_and_max_idle_minus_one():
    xml = _build_minimal(pooling={"enabled": True})
    attrs = _adapter_pool_info_attrs(xml)
    assert _is_pool_enabled(xml) == "true"
    assert attrs["maxActive"] == "-1"
    assert attrs["maxIdle"] == "-1"
    # Everything else stays at omitted-default values
    assert attrs["exhaustedAction"] == "1"
    assert attrs["maxIdleTime"] == "0"
    assert attrs["maxWait"] == "0"
    assert attrs["minIdle"] == "0"
    assert attrs["numberOfTests"] == "0"
    assert attrs["testIdle"] == "false"
    assert attrs["testOnBorrow"] == "false"
    assert attrs["testOnReturn"] == "false"
    assert attrs["timeBetweenRuns"] == "0"
    assert attrs["validationQuery"] == ""


@pytest.mark.parametrize("snake,xml_attr,value,expected_xml", [
    ("exhausted_action", "exhaustedAction", 3, "3"),
    ("max_active", "maxActive", 50, "50"),
    ("max_idle", "maxIdle", 10, "10"),
    ("max_idle_time", "maxIdleTime", 60000, "60000"),
    ("max_wait", "maxWait", 5000, "5000"),
    ("min_idle", "minIdle", 5, "5"),
    ("number_of_tests", "numberOfTests", 7, "7"),
    ("test_idle", "testIdle", True, "true"),
    ("test_on_borrow", "testOnBorrow", True, "true"),
    ("test_on_return", "testOnReturn", True, "true"),
    ("time_between_runs", "timeBetweenRuns", 30000, "30000"),
    ("validation_query", "validationQuery", "SELECT 1", "SELECT 1"),
])
def test_pooling_explicit_values_map_to_xml_attributes(snake, xml_attr, value, expected_xml):
    xml = _build_minimal(pooling={"enabled": True, snake: value})
    attrs = _adapter_pool_info_attrs(xml)
    assert attrs[xml_attr] == expected_xml


def test_pooling_validation_query_is_xml_escaped():
    xml = _build_minimal(pooling={"enabled": True, "validation_query": "SELECT a & b"})
    attrs = _adapter_pool_info_attrs(xml)
    assert attrs["validationQuery"] == "SELECT a & b"


@pytest.mark.parametrize("bad_key", ["bogus", "minActive", "validationquery", "max_active_size"])
def test_pooling_unknown_key_returns_database_pooling_validation_failed(bad_key):
    with pytest.raises(BuilderValidationError) as excinfo:
        DatabaseConnectorBuilder().build(**_minimal_config(pooling={bad_key: 1}))
    err = excinfo.value
    assert err.error_code == "DATABASE_POOLING_VALIDATION_FAILED"
    assert err.field == f"pooling.{bad_key}"


@pytest.mark.parametrize("not_dict", [["enabled"], "enabled", 5, True])
def test_pooling_non_dict_returns_database_pooling_validation_failed(not_dict):
    with pytest.raises(BuilderValidationError) as excinfo:
        DatabaseConnectorBuilder().build(**_minimal_config(pooling=not_dict))
    err = excinfo.value
    assert err.error_code == "DATABASE_POOLING_VALIDATION_FAILED"
    assert err.field == "pooling"


@pytest.mark.parametrize("key,bad_value", [
    ("max_active", "oops"),
    ("max_active", True),               # bool not accepted as int
    ("validation_query", 123),
    ("test_idle", "yes"),
    ("test_idle", 1),                   # int not accepted as bool
])
def test_pooling_invalid_scalar_type_returns_database_pooling_validation_failed(key, bad_value):
    with pytest.raises(BuilderValidationError) as excinfo:
        DatabaseConnectorBuilder().build(**_minimal_config(pooling={key: bad_value}))
    err = excinfo.value
    assert err.error_code == "DATABASE_POOLING_VALIDATION_FAILED"
    assert err.field == f"pooling.{key}"


@pytest.mark.parametrize("bad_enabled", ["true", 1, 0, "yes"])
def test_pooling_enabled_non_bool_returns_database_pooling_validation_failed(bad_enabled):
    with pytest.raises(BuilderValidationError) as excinfo:
        DatabaseConnectorBuilder().build(**_minimal_config(pooling={"enabled": bad_enabled}))
    err = excinfo.value
    assert err.error_code == "DATABASE_POOLING_VALIDATION_FAILED"
    assert err.field == "pooling.enabled"


def test_validate_pooling_returns_none_for_omitted_and_clean_configs():
    assert DatabaseConnectorBuilder.validate_pooling(None) is None
    assert DatabaseConnectorBuilder.validate_pooling({}) is None
    assert DatabaseConnectorBuilder.validate_pooling({"enabled": True}) is None
    assert DatabaseConnectorBuilder.validate_pooling(
        {"enabled": True, "max_active": 100, "validation_query": "SELECT 1"}
    ) is None


# --- write_options --------------------------------------------------------

def test_omitted_write_options_preserves_default_write_options_xml_exactly():
    xml = _build_minimal()  # no write_options key
    assert _write_options_attrs(xml) == {
        "sqlFilePath": "tmp/sqldebug.txt",
        "writeSQLToFile": "false",
    }


def test_write_options_explicit_defaults_match_omitted():
    a = _build_minimal()
    b = _build_minimal(write_options={"write_sql_to_file": False, "sql_file_path": "tmp/sqldebug.txt"})
    assert _write_options_attrs(a) == _write_options_attrs(b)


def test_write_options_write_sql_to_file_true_emits_attrs():
    xml = _build_minimal(
        write_options={"write_sql_to_file": True, "sql_file_path": "/var/log/sql.txt"},
    )
    attrs = _write_options_attrs(xml)
    assert attrs["writeSQLToFile"] == "true"
    assert attrs["sqlFilePath"] == "/var/log/sql.txt"


def test_write_options_sql_file_path_is_xml_escaped():
    xml = _build_minimal(
        write_options={"write_sql_to_file": True, "sql_file_path": "/tmp/<sql>&debug.txt"},
    )
    attrs = _write_options_attrs(xml)
    assert attrs["sqlFilePath"] == "/tmp/<sql>&debug.txt"


def test_write_options_write_sql_to_file_true_without_sql_file_path_returns_validation_failed():
    with pytest.raises(BuilderValidationError) as excinfo:
        DatabaseConnectorBuilder().build(
            **_minimal_config(write_options={"write_sql_to_file": True}),
        )
    err = excinfo.value
    assert err.error_code == "DATABASE_WRITE_OPTIONS_VALIDATION_FAILED"
    assert err.field == "write_options.sql_file_path"


def test_write_options_write_sql_to_file_true_with_blank_sql_file_path_returns_validation_failed():
    with pytest.raises(BuilderValidationError) as excinfo:
        DatabaseConnectorBuilder().build(
            **_minimal_config(write_options={"write_sql_to_file": True, "sql_file_path": "   "}),
        )
    err = excinfo.value
    assert err.error_code == "DATABASE_WRITE_OPTIONS_VALIDATION_FAILED"
    assert err.field == "write_options.sql_file_path"


@pytest.mark.parametrize("bad_key", ["bogus", "writeSQLToFile", "sqlfile_path"])
def test_write_options_unknown_key_returns_validation_failed(bad_key):
    with pytest.raises(BuilderValidationError) as excinfo:
        DatabaseConnectorBuilder().build(**_minimal_config(write_options={bad_key: "anything"}))
    err = excinfo.value
    assert err.error_code == "DATABASE_WRITE_OPTIONS_VALIDATION_FAILED"
    assert err.field == f"write_options.{bad_key}"


@pytest.mark.parametrize("key,bad_value", [
    ("write_sql_to_file", "yes"),
    ("write_sql_to_file", 1),
    ("sql_file_path", 123),
    ("sql_file_path", False),
])
def test_write_options_invalid_scalar_type_returns_validation_failed(key, bad_value):
    with pytest.raises(BuilderValidationError) as excinfo:
        DatabaseConnectorBuilder().build(**_minimal_config(write_options={key: bad_value}))
    err = excinfo.value
    assert err.error_code == "DATABASE_WRITE_OPTIONS_VALIDATION_FAILED"
    assert err.field == f"write_options.{key}"


@pytest.mark.parametrize("not_dict", [["a"], "x", 7])
def test_write_options_non_dict_returns_validation_failed(not_dict):
    with pytest.raises(BuilderValidationError) as excinfo:
        DatabaseConnectorBuilder().build(**_minimal_config(write_options=not_dict))
    err = excinfo.value
    assert err.error_code == "DATABASE_WRITE_OPTIONS_VALIDATION_FAILED"
    assert err.field == "write_options"


def test_validate_write_options_returns_none_for_omitted_and_clean_configs():
    assert DatabaseConnectorBuilder.validate_write_options(None) is None
    assert DatabaseConnectorBuilder.validate_write_options({}) is None
    assert DatabaseConnectorBuilder.validate_write_options({"write_sql_to_file": False}) is None
    assert DatabaseConnectorBuilder.validate_write_options(
        {"write_sql_to_file": True, "sql_file_path": "/tmp/x.txt"}
    ) is None


# --- Nested secret rejection (Issue #31 Codex P1 follow-up) --------------
# A forbidden secret-shaped key smuggled inside a sub-block (pooling /
# write_options) must trip the plaintext-secret check, not the sub-block
# validator. Otherwise the integration_builder redaction (which only fires
# on PLAINTEXT_SECRET_REJECTED) is bypassed and the value leaks into plan
# output.

@pytest.mark.parametrize("forbidden", [
    "password", "password_ref", "secret", "token", "access_token", "client_secret",
])
def test_pooling_with_forbidden_secret_key_returns_plaintext_secret_rejected(forbidden):
    leaked = f"LEAK_{forbidden}_DEADBEEF"
    with pytest.raises(BuilderValidationError) as excinfo:
        DatabaseConnectorBuilder().build(
            **_minimal_config(pooling={forbidden: leaked}),
        )
    err = excinfo.value
    assert err.error_code == "PLAINTEXT_SECRET_REJECTED"
    assert err.field == f"pooling.{forbidden}"
    assert leaked not in str(err)
    assert leaked not in (err.hint or "")


@pytest.mark.parametrize("forbidden", [
    "password", "password_ref", "secret", "token", "access_token", "client_secret",
])
def test_write_options_with_forbidden_secret_key_returns_plaintext_secret_rejected(forbidden):
    leaked = f"LEAK_WO_{forbidden}_DEADBEEF"
    with pytest.raises(BuilderValidationError) as excinfo:
        DatabaseConnectorBuilder().build(
            **_minimal_config(write_options={forbidden: leaked}),
        )
    err = excinfo.value
    assert err.error_code == "PLAINTEXT_SECRET_REJECTED"
    assert err.field == f"write_options.{forbidden}"
    assert leaked not in str(err)


def test_scan_forbidden_secret_fields_detects_nested_pooling_secret():
    err = DatabaseConnectorBuilder.scan_forbidden_secret_fields(
        {"connector_type": "database",
         "pooling": {"password": "LEAK_NESTED_DEADBEEF"}},
    )
    assert isinstance(err, BuilderValidationError)
    assert err.error_code == "PLAINTEXT_SECRET_REJECTED"
    assert err.field == "pooling.password"


def test_scan_forbidden_secret_fields_top_level_takes_priority_over_nested():
    """When both a top-level and a nested secret exist, the top-level wins.
    M2.2 compat: existing callers asserting field='password' at top-level
    must keep working."""
    err = DatabaseConnectorBuilder.scan_forbidden_secret_fields(
        {"connector_type": "database",
         "password": "TOP_LEVEL_DEADBEEF",
         "pooling": {"secret": "NESTED_DEADBEEF"}},
    )
    assert err.field == "password"  # no prefix — top-level


def test_scan_forbidden_secret_fields_arbitrary_depth():
    """Defensive: should walk past 1 level too."""
    err = DatabaseConnectorBuilder.scan_forbidden_secret_fields(
        {"connector_type": "database",
         "pooling": {"sub": {"token": "DEEP_DEADBEEF"}}},
    )
    assert err is not None
    assert err.error_code == "PLAINTEXT_SECRET_REJECTED"
    assert err.field == "pooling.sub.token"


def test_redact_forbidden_secret_fields_in_place_scrubs_nested():
    config = {
        "connector_type": "database",
        "password": "TOP_DEADBEEF",
        "pooling": {"token": "POOL_DEADBEEF",
                    "sub": {"secret": "DEEP_DEADBEEF"}},
        "write_options": {"access_token": "WO_DEADBEEF"},
        "credential_ref": "credential://safe/path",
    }
    DatabaseConnectorBuilder.redact_forbidden_secret_fields_in_place(config)
    assert config["password"] == "[REDACTED]"
    assert config["pooling"]["token"] == "[REDACTED]"
    assert config["pooling"]["sub"]["secret"] == "[REDACTED]"
    assert config["write_options"]["access_token"] == "[REDACTED]"
    # Non-secret values untouched
    assert config["connector_type"] == "database"
    assert config["credential_ref"] == "credential://safe/path"


def test_redact_forbidden_secret_fields_in_place_handles_non_dict_gracefully():
    """The helper must no-op on None / scalars / lists rather than raise."""
    DatabaseConnectorBuilder.redact_forbidden_secret_fields_in_place(None)
    DatabaseConnectorBuilder.redact_forbidden_secret_fields_in_place("not a dict")
    DatabaseConnectorBuilder.redact_forbidden_secret_fields_in_place(42)
    DatabaseConnectorBuilder.redact_forbidden_secret_fields_in_place([{"password": "leak"}])
    # No assertion needed — just must not throw.


# --- List-of-dicts traversal (Codex P2 follow-up) ------------------------
# The builder ignores unknown top-level keys, so a caller can smuggle a
# list-of-dicts past validate_config (e.g. `extra: [{"password": "..."}]`).
# Without descent into list elements, the plan echo would still leak the
# plaintext value. scan + redactor must both walk dict elements of lists.

def test_scan_forbidden_secret_fields_detects_secret_in_list_of_dicts():
    err = DatabaseConnectorBuilder.scan_forbidden_secret_fields(
        {"connector_type": "database",
         "extra": [{"password": "LEAK_LIST_DEADBEEF"}]},
    )
    assert isinstance(err, BuilderValidationError)
    assert err.error_code == "PLAINTEXT_SECRET_REJECTED"
    assert err.field == "extra[0].password"
    assert "LEAK_LIST_DEADBEEF" not in str(err)


def test_scan_forbidden_secret_fields_reports_correct_list_index():
    """When the offender is at a non-zero index, the path reflects the index."""
    err = DatabaseConnectorBuilder.scan_forbidden_secret_fields(
        {"connector_type": "database",
         "extra": [{}, {"safe": "ok"}, {"token": "LEAK_IDX2_DEADBEEF"}]},
    )
    assert err.field == "extra[2].token"


def test_scan_forbidden_secret_fields_walks_dict_inside_list_inside_dict():
    err = DatabaseConnectorBuilder.scan_forbidden_secret_fields(
        {"connector_type": "database",
         "wrapper": {"items": [{"secret": "LEAK_DEEP_DEADBEEF"}]}},
    )
    assert err.error_code == "PLAINTEXT_SECRET_REJECTED"
    assert err.field == "wrapper.items[0].secret"


def test_scan_forbidden_secret_fields_no_false_positive_on_list_of_scalars():
    """A list of non-dict values has no keys to scan — must not trip the
    secret check or raise."""
    assert DatabaseConnectorBuilder.scan_forbidden_secret_fields(
        {"connector_type": "database",
         "tags": ["safe", "also-safe", 42, None]},
    ) is None


def test_scan_forbidden_secret_fields_top_level_takes_priority_over_list_nested():
    """Top-level offender wins over a list-nested one (shallowest first)."""
    err = DatabaseConnectorBuilder.scan_forbidden_secret_fields(
        {"connector_type": "database",
         "password": "TOP_DEADBEEF",
         "extra": [{"token": "NESTED_DEADBEEF"}]},
    )
    assert err.field == "password"  # no path prefix


def test_redact_forbidden_secret_fields_in_place_scrubs_secrets_in_list_of_dicts():
    config = {
        "connector_type": "database",
        "extra": [
            {"password": "L1_DEADBEEF"},
            {"safe": "ok", "token": "L2_DEADBEEF"},
        ],
        "wrapper": {"items": [{"secret": "L3_DEADBEEF"}]},
        "credential_ref": "credential://safe/path",
    }
    DatabaseConnectorBuilder.redact_forbidden_secret_fields_in_place(config)
    assert config["extra"][0]["password"] == "[REDACTED]"
    assert config["extra"][1]["token"] == "[REDACTED]"
    assert config["extra"][1]["safe"] == "ok"  # non-secret preserved
    assert config["wrapper"]["items"][0]["secret"] == "[REDACTED]"
    assert config["credential_ref"] == "credential://safe/path"
    assert "L1_DEADBEEF" not in repr(config)
    assert "L2_DEADBEEF" not in repr(config)
    assert "L3_DEADBEEF" not in repr(config)


def test_redact_forbidden_secret_fields_in_place_leaves_scalar_lists_alone():
    config = {"connector_type": "database", "tags": ["a", "b", 3, None]}
    DatabaseConnectorBuilder.redact_forbidden_secret_fields_in_place(config)
    assert config["tags"] == ["a", "b", 3, None]


# --- Arbitrary-depth container traversal (Codex P2 follow-up #2) ----------
# Lists nested inside lists, dicts inside lists inside dicts inside lists,
# etc. Defense-in-depth: any JSON container that can hold a dict-keyed value
# must be walked.

def test_scan_forbidden_secret_fields_detects_secret_in_list_of_lists():
    err = DatabaseConnectorBuilder.scan_forbidden_secret_fields(
        {"connector_type": "database",
         "matrix": [[{"password": "LEAK_LIST_OF_LIST_DEADBEEF"}]]},
    )
    assert err is not None
    assert err.error_code == "PLAINTEXT_SECRET_REJECTED"
    assert err.field == "matrix[0][0].password"
    assert "LEAK_LIST_OF_LIST_DEADBEEF" not in str(err)


def test_scan_forbidden_secret_fields_reports_indices_in_nested_lists():
    """Outer index 1, inner index 1 → matrix[1][1]."""
    err = DatabaseConnectorBuilder.scan_forbidden_secret_fields(
        {"connector_type": "database",
         "matrix": [[{}, {}], [{}, {"token": "LEAK_GRID_DEADBEEF"}]]},
    )
    assert err.field == "matrix[1][1].token"


def test_scan_forbidden_secret_fields_walks_dict_list_dict_list_dict_chain():
    """Deep mixed chain: dict → list → dict → list → dict containing secret."""
    err = DatabaseConnectorBuilder.scan_forbidden_secret_fields(
        {"connector_type": "database",
         "a": {"b": [{"c": [{"secret": "LEAK_CHAIN_DEADBEEF"}]}]}},
    )
    assert err.field == "a.b[0].c[0].secret"


def test_scan_forbidden_secret_fields_triple_nested_list():
    """Three levels of list nesting (cube)."""
    err = DatabaseConnectorBuilder.scan_forbidden_secret_fields(
        {"connector_type": "database",
         "cube": [[[{"password": "LEAK_CUBE_DEADBEEF"}]]]},
    )
    assert err.field == "cube[0][0][0].password"


def test_scan_forbidden_secret_fields_no_false_positive_on_nested_scalar_lists():
    """Lists of lists of scalars — no dicts anywhere — must not trip."""
    assert DatabaseConnectorBuilder.scan_forbidden_secret_fields(
        {"connector_type": "database",
         "matrix": [[1, 2, 3], ["a", "b"], [None, None]]},
    ) is None


def test_redact_forbidden_secret_fields_in_place_scrubs_secrets_in_nested_lists():
    config = {
        "connector_type": "database",
        "matrix": [
            [{"password": "L1_DEADBEEF"}, {"safe": "ok"}],
            [{}, {"token": "L2_DEADBEEF"}],
        ],
        "cube": [[[{"secret": "L3_DEADBEEF"}]]],
    }
    DatabaseConnectorBuilder.redact_forbidden_secret_fields_in_place(config)
    assert config["matrix"][0][0]["password"] == "[REDACTED]"
    assert config["matrix"][0][1]["safe"] == "ok"  # non-secret preserved
    assert config["matrix"][1][1]["token"] == "[REDACTED]"
    assert config["cube"][0][0][0]["secret"] == "[REDACTED]"
    for marker in ("L1_DEADBEEF", "L2_DEADBEEF", "L3_DEADBEEF"):
        assert marker not in repr(config), marker


def test_redact_forbidden_secret_fields_in_place_handles_arbitrary_nesting():
    """Mixed scalars, dicts, and lists at irregular depths."""
    config = {
        "connector_type": "database",
        "weird": [
            42,
            "scalar",
            [None, {"token": "INNER_DEADBEEF"}],
            {"deep": [[{"password": "DEEP_DEADBEEF"}]]},
        ],
    }
    DatabaseConnectorBuilder.redact_forbidden_secret_fields_in_place(config)
    assert config["weird"][2][1]["token"] == "[REDACTED]"
    assert config["weird"][3]["deep"][0][0]["password"] == "[REDACTED]"
    assert config["weird"][0] == 42  # scalars preserved
    assert config["weird"][1] == "scalar"
    assert "INNER_DEADBEEF" not in repr(config)
    assert "DEEP_DEADBEEF" not in repr(config)


def test_scan_forbidden_secret_fields_top_level_still_beats_arbitrary_nesting():
    """Even with deep nesting, top-level offender wins (no path prefix)."""
    err = DatabaseConnectorBuilder.scan_forbidden_secret_fields(
        {"password": "TOP_DEADBEEF",
         "matrix": [[{"secret": "DEEP_DEADBEEF"}]]},
    )
    assert err.field == "password"


# ===========================================================================
# Issue #31 — Oracle / MySQL / SAP HANA host_port_db drivers + Custom shape
# ===========================================================================


def _custom_config(**overrides):
    """Raw config dict for the custom_url shape."""
    params = {
        "connector_type": "database",
        "component_name": "Snowflake via Custom",
        "driver_id": "custom",
        "auth_mode": "username_password",
        "username": "INTEG_USER",
        "credential_ref": "credential://test/custom/password",
        "custom_class_name": "net.snowflake.client.jdbc.SnowflakeDriver",
        "connection_url": "jdbc:snowflake://acct.snowflakecomputing.com/?db=PROD",
    }
    params.update(overrides)
    return params


def _build_custom(**overrides):
    params = _custom_config(**overrides)
    params.pop("connector_type", None)
    return DatabaseConnectorBuilder().build(**params)


# --- host_port_db: new buildable drivers --------------------------------------


@pytest.mark.parametrize("driver_id,emits_driver_id,class_name,url_format,default_port", [
    ("oracle", "oracle", "oracle.jdbc.driver.OracleDriver",
     "jdbc:oracle:thin:@{0}:{1}:{2}", "1521"),
    ("mysql", "mysql", "com.mysql.jdbc.Driver",
     "jdbc:mysql://{0}:{1}/{2}{3}", "3306"),
])
def test_new_host_port_db_driver_emits_expected_attributes(
    driver_id, emits_driver_id, class_name, url_format, default_port,
):
    xml = _build_minimal(driver_id=driver_id)
    root = ET.fromstring(xml)
    dcs = root.find("bns:object/DatabaseConnectionSettings", NS)
    assert dcs.attrib["driverId"] == emits_driver_id
    assert dcs.attrib["className"] == class_name
    assert dcs.attrib["urlFormat"] == url_format
    assert dcs.attrib["port"] == default_port
    # Shared envelope still emits the encrypted-password marker.
    enc = root.find("bns:encryptedValues/bns:encryptedValue", NS)
    assert enc.attrib["path"] == "//DatabaseConnectionSettings/@password"
    assert enc.attrib["isSet"] == "false"


@pytest.mark.parametrize("driver_id", ["sap-hana", "sap_hana"])
def test_sap_hana_emits_canonical_driver_id_and_class(driver_id):
    xml = _build_minimal(driver_id=driver_id, port=30015)
    root = ET.fromstring(xml)
    dcs = root.find("bns:object/DatabaseConnectionSettings", NS)
    assert dcs.attrib["driverId"] == "sap-hana"
    assert dcs.attrib["className"] == "com.sap.db.jdbc.Driver"
    assert dcs.attrib["urlFormat"] == "jdbc:sap://{0}:{1}/?databaseName={2}{3}"
    assert dcs.attrib["port"] == "30015"


def test_sap_hana_without_port_fails_with_database_connector_validation_failed():
    params = _minimal_config(driver_id="sap_hana")
    params.pop("port", None)  # caller did not supply port
    with pytest.raises(BuilderValidationError) as excinfo:
        DatabaseConnectorBuilder().build(
            **{k: v for k, v in params.items() if k != "connector_type"},
        )
    err = excinfo.value
    assert err.error_code == "DATABASE_CONNECTOR_VALIDATION_FAILED"
    assert err.field == "port"
    assert "sap" in (err.hint or "").lower()


def test_sap_hana_blank_port_string_also_fails():
    params = _minimal_config(driver_id="sap-hana", port="   ")
    with pytest.raises(BuilderValidationError) as excinfo:
        DatabaseConnectorBuilder().build(
            **{k: v for k, v in params.items() if k != "connector_type"},
        )
    assert excinfo.value.error_code == "DATABASE_CONNECTOR_VALIDATION_FAILED"
    assert excinfo.value.field == "port"


@pytest.mark.parametrize("driver_id,forbidden_field,forbidden_value", [
    ("mysql", "custom_class_name", "com.example.Driver"),
    ("mysql", "connection_url",   "jdbc:mysql://example/db"),
    ("oracle", "custom_class_name", "com.example.Driver"),
    ("sap_hana", "connection_url",   "jdbc:sap://example/db"),
])
def test_host_port_db_drivers_reject_custom_url_shape_fields(
    driver_id, forbidden_field, forbidden_value,
):
    """A host_port_db driver must reject custom_class_name / connection_url
    so a caller can't accidentally smuggle Custom-only fields in."""
    extra = {"port": 30015} if driver_id == "sap_hana" else {}
    params = _minimal_config(driver_id=driver_id, **{forbidden_field: forbidden_value}, **extra)
    with pytest.raises(BuilderValidationError) as excinfo:
        DatabaseConnectorBuilder().build(
            **{k: v for k, v in params.items() if k != "connector_type"},
        )
    err = excinfo.value
    assert err.error_code == "DATABASE_CONNECTOR_VALIDATION_FAILED"
    assert err.field == forbidden_field


# --- custom_url shape --------------------------------------------------------


def test_custom_url_shape_emits_caller_class_name_and_connection_url():
    xml = _build_custom()
    root = ET.fromstring(xml)
    dcs = root.find("bns:object/DatabaseConnectionSettings", NS)
    assert dcs.attrib["driverId"] == "custom"
    assert dcs.attrib["className"] == "net.snowflake.client.jdbc.SnowflakeDriver"
    assert dcs.attrib["urlFormat"] == "jdbc:snowflake://acct.snowflakecomputing.com/?db=PROD"
    # host/port/dbname/additional are emitted as empty attributes to match
    # Boomi's live #Common Custom export shape (component 39fb519d-...).
    assert dcs.attrib["host"] == ""
    assert dcs.attrib["port"] == ""
    assert dcs.attrib["dbname"] == ""
    assert dcs.attrib["additional"] == ""
    # Envelope invariants still hold.
    enc = root.find("bns:encryptedValues/bns:encryptedValue", NS)
    assert enc.attrib["path"] == "//DatabaseConnectionSettings/@password"
    assert enc.attrib["isSet"] == "false"
    assert dcs.find("WriteOptions") is not None
    assert dcs.find("AdapterPoolInfo") is not None


def test_custom_url_shape_xml_escapes_class_name_and_connection_url():
    xml = _build_custom(
        custom_class_name="com.example.<Driver>",
        connection_url="jdbc:example://host?a=1&b=2",
    )
    root = ET.fromstring(xml)
    dcs = root.find("bns:object/DatabaseConnectionSettings", NS)
    assert dcs.attrib["className"] == "com.example.<Driver>"
    assert dcs.attrib["urlFormat"] == "jdbc:example://host?a=1&b=2"


@pytest.mark.parametrize("missing", ["custom_class_name", "connection_url"])
def test_custom_url_missing_required_field_fails(missing):
    params = _custom_config()
    params.pop(missing)
    with pytest.raises(BuilderValidationError) as excinfo:
        DatabaseConnectorBuilder().build(
            **{k: v for k, v in params.items() if k != "connector_type"},
        )
    err = excinfo.value
    assert err.error_code == "DATABASE_CONNECTOR_VALIDATION_FAILED"
    assert err.field == missing


@pytest.mark.parametrize("forbidden,value", [
    ("host",        "host.example.com"),
    ("port",        5432),
    ("dbname",      "MyDatabase"),
    ("additional",  ";encrypt=true"),
])
def test_custom_url_shape_rejects_host_port_db_fields(forbidden, value):
    """custom_url forbids host/port/dbname/additional in the JSON contract —
    the XML still emits them as empty attrs, but caller-supplied values
    must fail validation before build()."""
    params = _custom_config(**{forbidden: value})
    with pytest.raises(BuilderValidationError) as excinfo:
        DatabaseConnectorBuilder().build(
            **{k: v for k, v in params.items() if k != "connector_type"},
        )
    err = excinfo.value
    assert err.error_code == "DATABASE_CONNECTOR_VALIDATION_FAILED"
    assert err.field == forbidden
    assert "custom_url" in (err.hint or "")


def test_custom_url_empty_string_carry_over_does_not_trip_forbidden_check():
    """An explicit empty string on a forbidden field is a no-op (some clients
    serialize all fields with defaults). validate_config should not reject."""
    xml = _build_custom(host="", port="", dbname="", additional="")
    root = ET.fromstring(xml)
    dcs = root.find("bns:object/DatabaseConnectionSettings", NS)
    assert dcs.attrib["driverId"] == "custom"


def test_custom_url_credential_ref_still_required():
    params = _custom_config()
    params.pop("credential_ref")
    with pytest.raises(BuilderValidationError) as excinfo:
        DatabaseConnectorBuilder().build(
            **{k: v for k, v in params.items() if k != "connector_type"},
        )
    assert excinfo.value.error_code == "MISSING_CREDENTIAL_REF"


def test_custom_url_plaintext_secret_in_connection_url_siblings_still_caught():
    """Defense-in-depth: a secret-shaped key alongside a valid custom_url
    config still trips PLAINTEXT_SECRET_REJECTED."""
    params = _custom_config(password="LEAK_CUSTOM_DEADBEEF")
    with pytest.raises(BuilderValidationError) as excinfo:
        DatabaseConnectorBuilder().build(
            **{k: v for k, v in params.items() if k != "connector_type"},
        )
    err = excinfo.value
    assert err.error_code == "PLAINTEXT_SECRET_REJECTED"
    assert err.field == "password"
    assert "LEAK_CUSTOM_DEADBEEF" not in str(err)


# --- Live-reference metadata + runtime prerequisites -------------------------


@pytest.mark.parametrize("driver_id,expected_ref", [
    ("sqlserver", "4ace95d7-6ee4-4f83-8fad-723d3fabdb2f"),
    ("jtds",      "107aaef1-cb1e-4975-be44-69d120803864"),
    ("oracle",    "6adf9e1e-39c8-4104-bc6c-9769b93aa161"),
    ("mysql",     "bfbfea6f-39c7-498e-859b-6036959a20c8"),
    ("sap-hana",  "c9077711-39a4-4d52-9f91-27bdf1f5b8ec"),
    ("custom",    "39fb519d-e970-4aaf-a1f7-4eba39158e9d"),
])
def test_driver_registry_carries_live_reference_component_id(driver_id, expected_ref):
    assert DatabaseConnectorBuilder.DRIVERS[driver_id]["live_reference_component_id"] == expected_ref


@pytest.mark.parametrize("driver_id", ["mysql", "sap-hana", "custom"])
def test_drivers_without_bundled_runtime_jar_carry_prerequisite_note(driver_id):
    note = DatabaseConnectorBuilder.DRIVERS[driver_id].get("runtime_driver_prerequisite")
    assert note, driver_id
    assert "library" in note.lower()


def test_sqlserver_and_jtds_have_no_runtime_prerequisite_note():
    """SQL Server JDBC and jTDS ship with the Boomi runtime — no prereq note."""
    for driver_id in ("sqlserver", "jtds", "oracle"):
        assert "runtime_driver_prerequisite" not in DatabaseConnectorBuilder.DRIVERS[driver_id]


# --- Oracle `additional` is accepted (codex r1 reverted after KB check) ------
# Boomi's Database Legacy docs say `additional` is appended to the end of the
# connection URL across all drivers, not gated per-driver. The Oracle Thin SID
# URL may not accept arbitrary trailing options at runtime, but that's a
# runtime concern — the builder must emit whatever the caller supplies and
# leave runtime acceptance to the JDBC layer. The schema's oracle variant
# note documents the limitation and points at driver_id='custom' for
# service-name URLs.


def test_oracle_driver_has_no_additional_supported_flag():
    """Codex r2 walked back the per-driver `additional` gate — Boomi appends
    `additional` to the end of the URL for every driver per the Database
    Legacy docs. The flag mechanism was removed; no current driver opts out."""
    for driver_id in DatabaseConnectorBuilder.DRIVERS:
        assert "additional_supported" not in DatabaseConnectorBuilder.DRIVERS[driver_id], driver_id


def test_oracle_accepts_additional_and_emits_it_into_xml():
    xml = _build_minimal(driver_id="oracle", additional=";readonly=true")
    root = ET.fromstring(xml)
    dcs = root.find("bns:object/DatabaseConnectionSettings", NS)
    assert dcs.attrib["driverId"] == "oracle"
    assert dcs.attrib["additional"] == ";readonly=true"
    # urlFormat in the registry remains the SID form — Boomi appends
    # `additional` to the formed URL at JDBC-formatting time (vendor-side).
    assert dcs.attrib["urlFormat"] == "jdbc:oracle:thin:@{0}:{1}:{2}"


@pytest.mark.parametrize("driver_id", ["sqlserver", "jtds", "oracle", "mysql", "sap_hana"])
def test_every_host_port_db_driver_accepts_additional(driver_id):
    """All host_port_db drivers accept `additional` — Boomi handles the
    URL-append semantics on its side."""
    extra = {"port": 30015} if driver_id == "sap_hana" else {}
    xml = _build_minimal(driver_id=driver_id, additional=";foo=bar", **extra)
    root = ET.fromstring(xml)
    dcs = root.find("bns:object/DatabaseConnectionSettings", NS)
    assert dcs.attrib["additional"] == ";foo=bar"


# --- Port validation and XML-attribute safety (codex r2) ---------------------
# build() drops `port` straight into an XML attribute via f-string. Without
# validation a caller string like '1433" injected="1' would inject an extra
# XML attribute. Restrict port to int (non-bool) or all-digit string and
# format via _format_xml_value() as defense-in-depth.


@pytest.mark.parametrize("bad_port,bad_type_marker", [
    (True,          "bool"),    # bool is a subclass of int — reject explicitly
    (False,         "bool"),
    (1.5,           "float"),
    ([1433],        "list"),
    ({"p": 1433},  "dict"),
])
def test_port_with_unsupported_type_fails_validation(bad_port, bad_type_marker):
    with pytest.raises(BuilderValidationError) as excinfo:
        DatabaseConnectorBuilder().build(**_minimal_config(port=bad_port))
    err = excinfo.value
    assert err.error_code == "DATABASE_CONNECTOR_VALIDATION_FAILED"
    assert err.field == "port"


@pytest.mark.parametrize("bad_port", [
    '1433" injected="1',           # XML injection vector — extra attribute
    "1433;DROP TABLE x",
    "1433 extra",
    "abc",
    "-1",                          # negative port (digit-string rejects '-')
    "12.5",                        # decimal point
])
def test_port_with_non_digit_string_fails_validation(bad_port):
    """Critical security check: a non-digit string in port must never reach
    the XML f-string template (line 899)."""
    with pytest.raises(BuilderValidationError) as excinfo:
        DatabaseConnectorBuilder().build(**_minimal_config(port=bad_port))
    err = excinfo.value
    assert err.error_code == "DATABASE_CONNECTOR_VALIDATION_FAILED"
    assert err.field == "port"


def test_port_negative_integer_fails_validation():
    with pytest.raises(BuilderValidationError) as excinfo:
        DatabaseConnectorBuilder().build(**_minimal_config(port=-1))
    assert excinfo.value.error_code == "DATABASE_CONNECTOR_VALIDATION_FAILED"
    assert excinfo.value.field == "port"


@pytest.mark.parametrize("good_port,expected_xml", [
    (1433,    "1433"),
    (65535,   "65535"),
    ("1433",  "1433"),
    ("11433", "11433"),
])
def test_port_with_valid_int_or_digit_string_builds(good_port, expected_xml):
    xml = _build_minimal(port=good_port)
    root = ET.fromstring(xml)
    dcs = root.find("bns:object/DatabaseConnectionSettings", NS)
    assert dcs.attrib["port"] == expected_xml


def test_port_injection_attempt_does_not_appear_in_emitted_xml():
    """Even if validate_config has a future gap, the XML must not contain the
    injection string verbatim (defense-in-depth via _format_xml_value)."""
    sentinel = '1433" injected="SHOULD_NOT_APPEAR'
    with pytest.raises(BuilderValidationError):
        DatabaseConnectorBuilder().build(**_minimal_config(port=sentinel))
    # validate_config blocks the call; sentinel never lands in any output.
    # Also confirm scan_forbidden_secret_fields doesn't trip on the bare port —
    # it's about explicit type/format guards, not secret detection.


# --- Port null/blank rejection (codex r3) -----------------------------------
# A JSON client serializing `port: null` or `port: ""` for "use default"
# previously slipped past validate_config: section 5d treated it as a skip,
# but build()'s params.get('port', default) preserves the explicit value, so
# the emitted XML carried port="None" or port="". Section 5d now rejects
# both forms and tells callers to OMIT the key for default-port behavior.
# SAP HANA (no default) keeps its required-port error via section 5c.


@pytest.mark.parametrize("driver_id", ["sqlserver", "jtds", "oracle", "mysql"])
def test_port_explicit_none_fails_validation_on_defaulted_driver(driver_id):
    params = _minimal_config(driver_id=driver_id, port=None)
    with pytest.raises(BuilderValidationError) as excinfo:
        DatabaseConnectorBuilder().build(
            **{k: v for k, v in params.items() if k != "connector_type"},
        )
    err = excinfo.value
    assert err.error_code == "DATABASE_CONNECTOR_VALIDATION_FAILED"
    assert err.field == "port"
    assert "null" in str(err) or "blank" in str(err)
    assert "omit" in (err.hint or "").lower()


@pytest.mark.parametrize("blank_port", ["", "   ", "\t", "\n"])
def test_port_blank_string_fails_validation(blank_port):
    """Empty / whitespace-only strings must not slip through. build() would
    otherwise emit `port=""` and clients would see a corrupted attribute."""
    with pytest.raises(BuilderValidationError) as excinfo:
        DatabaseConnectorBuilder().build(**_minimal_config(port=blank_port))
    err = excinfo.value
    assert err.error_code == "DATABASE_CONNECTOR_VALIDATION_FAILED"
    assert err.field == "port"


def test_omitting_port_still_uses_driver_default():
    """Regression guard: omitting the `port` key (no key, not null) still
    falls back to the driver default — distinct from null/blank rejection."""
    params = _minimal_config()
    params.pop("port", None)  # _minimal_config has no port, but be explicit
    xml = DatabaseConnectorBuilder().build(
        **{k: v for k, v in params.items() if k != "connector_type"},
    )
    root = ET.fromstring(xml)
    dcs = root.find("bns:object/DatabaseConnectionSettings", NS)
    assert dcs.attrib["port"] == "1433"  # sqlserver default


# --- Port range checks (codex r3 — uniform 1..65535) -------------------------
# r2 enforced port > 0 on int paths but missed the upper bound (65535) and
# the lower bound on string paths. r3 normalizes to int after the digit
# check and applies the same 1..65535 envelope to both code paths.


@pytest.mark.parametrize("driver_id,bad_port", [
    # Out-of-range int (above the TCP ceiling)
    ("sqlserver", 65536),
    ("sqlserver", 100000),
    ("oracle",    1000000),
    # Out-of-range int (zero — OS-chosen, not valid for client connect)
    ("sqlserver", 0),
    # Out-of-range string forms
    ("sqlserver", "0"),
    ("sqlserver", "0000"),
    ("sqlserver", "65536"),
    ("sqlserver", "100000"),
])
def test_port_outside_tcp_range_fails_validation(driver_id, bad_port):
    params = _minimal_config(driver_id=driver_id, port=bad_port)
    with pytest.raises(BuilderValidationError) as excinfo:
        DatabaseConnectorBuilder().build(
            **{k: v for k, v in params.items() if k != "connector_type"},
        )
    err = excinfo.value
    assert err.error_code == "DATABASE_CONNECTOR_VALIDATION_FAILED"
    assert err.field == "port"


@pytest.mark.parametrize("port_value,expected_xml", [
    (1,      "1"),       # lower bound
    (1433,   "1433"),
    (65535,  "65535"),   # upper bound
    ("1",    "1"),
    ("65535", "65535"),
])
def test_port_at_tcp_range_boundaries_builds(port_value, expected_xml):
    xml = _build_minimal(port=port_value)
    root = ET.fromstring(xml)
    dcs = root.find("bns:object/DatabaseConnectionSettings", NS)
    assert dcs.attrib["port"] == expected_xml


# --- Port string edge cases (codex r4) ---------------------------------------
# str.isdigit() returns True for non-ASCII digit-category chars (superscripts,
# fullwidth digits, Arabic-Indic numerals, etc.). Some of those int() parses
# (fullwidth → ok) and some it rejects (superscript → ValueError) — either
# way, build() would emit the original caller string in the XML attribute,
# so the validator must restrict to ASCII 0-9 only. Plus: Python 3.11+
# caps int-string parsing at PYTHONINTMAXSTRDIGITS (default 4300), so a
# very long digit string would raise an unstructured ValueError from int()
# instead of returning a BuilderValidationError. Both gaps closed in r4.


@pytest.mark.parametrize("non_ascii_digit", [
    "²",       # SUPERSCRIPT TWO — isdigit()=True, int() raises ValueError
    "２",       # FULLWIDTH DIGIT TWO — isdigit()=True, int() returns 2
    "٣",       # ARABIC-INDIC DIGIT THREE — isdigit()=True, int() returns 3
    "1４33",   # mixed ASCII + fullwidth
    "14３3",
])
def test_port_with_non_ascii_digit_chars_fails_validation(non_ascii_digit):
    """Even when str.isdigit() returns True, non-ASCII digit chars must
    fail validation. Otherwise the validator might pass and build() would
    emit the original glyph in the XML attribute (validator/emission
    mismatch)."""
    with pytest.raises(BuilderValidationError) as excinfo:
        DatabaseConnectorBuilder().build(**_minimal_config(port=non_ascii_digit))
    err = excinfo.value
    assert err.error_code == "DATABASE_CONNECTOR_VALIDATION_FAILED"
    assert err.field == "port"
    assert "ascii" in (err.hint or "").lower()
    # Critically, the offending glyph must never reach the XML.
    assert non_ascii_digit not in str(err)


def test_port_very_long_digit_string_fails_with_structured_error():
    """Python 3.11+ caps int-string parsing at PYTHONINTMAXSTRDIGITS
    (default 4300). isdigit() passes for any length, but int() raises
    ValueError. validate_config must wrap that as DATABASE_CONNECTOR_VALIDATION_FAILED
    to preserve the structured-error contract — not let the raw ValueError
    propagate."""
    long_digit_port = "9" * 4301
    with pytest.raises(BuilderValidationError) as excinfo:
        DatabaseConnectorBuilder().build(**_minimal_config(port=long_digit_port))
    err = excinfo.value
    # The error MUST be the structured BuilderValidationError type, not a
    # bare ValueError. BuilderValidationError subclasses ValueError, so
    # explicitly assert the subclass to lock in the contract.
    assert isinstance(err, BuilderValidationError)
    assert err.error_code == "DATABASE_CONNECTOR_VALIDATION_FAILED"
    assert err.field == "port"


def test_port_validator_never_raises_bare_value_error():
    """Defensive contract test: even on pathological inputs that crash
    int(), validate_config returns a BuilderValidationError rather than
    letting the underlying exception propagate unstructured."""
    for pathological in ("9" * 4301, "9" * 10000):
        err = DatabaseConnectorBuilder.validate_config(
            _minimal_config(port=pathological),
        )
        assert isinstance(err, BuilderValidationError)
        assert err.error_code == "DATABASE_CONNECTOR_VALIDATION_FAILED"
        assert err.field == "port"


# ============================================================================
# Issue #45 — Component XML update preservation
# ============================================================================


def test_database_connector_preservation_policy_attached():
    policy = DatabaseConnectorBuilder.PRESERVATION_POLICY
    assert policy.component_type == "connector-settings"
    assert policy.subtype == "database"
    assert any(
        op.path == "bns:object/DatabaseConnectionSettings"
        for op in policy.owned_paths
    )


def test_database_connector_update_preserves_unknown_root_attr_and_secret():
    """Live current XML carries an unknown root attr and a populated
    encryptedValue (isSet=true). After read-merge-write, both must
    survive, even though the builder always emits isSet=false."""
    from boomi_mcp.categories.components.component_update_preservation import (
        merge_for_update,
    )

    desired = _build_minimal(component_name="renamed")
    current = _build_minimal(component_name="original")
    current = current.replace(
        'name="original"',
        'name="original" futureRootAttr="opaque"',
    )
    current = current.replace(
        '<bns:encryptedValue path="//DatabaseConnectionSettings/@password" isSet="false"/>',
        (
            '<bns:encryptedValue path="//DatabaseConnectionSettings/@password" isSet="true"/>'
            '<bns:encryptedValue path="//DatabaseConnectionSettings/@futureSecret" isSet="true"/>'
        ),
    )

    merged = merge_for_update(
        current, desired, DatabaseConnectorBuilder.PRESERVATION_POLICY
    )
    root = ET.fromstring(merged)
    assert root.attrib.get("futureRootAttr") == "opaque"
    assert root.attrib["name"] == "renamed"  # owned attr was replaced
    ev_paths = {
        entry.attrib.get("path"): entry.attrib.get("isSet")
        for entry in root.findall("bns:encryptedValues/bns:encryptedValue", NS)
    }
    assert ev_paths["//DatabaseConnectionSettings/@password"] == "true"
    assert ev_paths["//DatabaseConnectionSettings/@futureSecret"] == "true"


def test_database_connector_update_preserves_unknown_attr_and_child_inside_settings():
    """Review follow-up: the DB connector policy uses subtree_merge, not
    wholesale replace, so unknown/future attributes or child blocks on
    DatabaseConnectionSettings (where Boomi/UI adds driver/auth/pooling
    fields) survive a structured update while owned fields still update."""
    from boomi_mcp.categories.components.component_update_preservation import (
        merge_for_update,
    )

    desired = _build_minimal(dbname="NewDB")
    current = _build_minimal(dbname="OldDB")
    # Inject an unknown attr + unknown child onto the live
    # DatabaseConnectionSettings element.
    current = current.replace(
        ' username="sa">',
        ' username="sa" futureBoomiAttr="opaque">'
        '<FutureBlock retained="yes"/>',
        1,
    )

    merged = merge_for_update(
        current, desired, DatabaseConnectorBuilder.PRESERVATION_POLICY
    )
    root = ET.fromstring(merged)
    settings = root.find("bns:object/DatabaseConnectionSettings", NS)
    # Owned attr updated.
    assert settings.attrib["dbname"] == "NewDB"
    # Unknown attr + child preserved (would be lost under wholesale replace).
    assert settings.attrib.get("futureBoomiAttr") == "opaque"
    assert settings.find("FutureBlock") is not None
    # Owned child blocks still present (replaced from desired).
    assert settings.find("WriteOptions") is not None
    assert settings.find("AdapterPoolInfo") is not None
