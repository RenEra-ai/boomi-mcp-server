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
    with pytest.raises(BuilderValidationError) as excinfo:
        DatabaseConnectorBuilder().build(**_minimal_config(driver_id="mysql"))
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


@pytest.mark.parametrize("unsupported_driver", ["postgres", "oracle", "mysql"])
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
        _minimal_config(driver_id="postgres")
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


def test_create_connector_http_value_error_path_unchanged():
    """HTTP builder raises plain ValueError (no error_code); the legacy
    flat envelope must still surface for non-BuilderValidationError ValueErrors."""
    client = MagicMock()
    client.connector.get_connector.return_value = MagicMock()
    result = create_connector(client, "test", {"connector_type": "http", "component_name": "X"})
    # Missing url → HttpConnectorBuilder raises ValueError (not BuilderValidationError)
    assert result["_success"] is False
    assert "error" in result
    assert "error_code" not in result  # flat envelope, no structured fields


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


def test_recognized_driver_ids_includes_custom_but_supported_does_not():
    assert "custom" in DatabaseConnectorBuilder.RECOGNIZED_DRIVER_IDS
    assert "custom" not in DatabaseConnectorBuilder.SUPPORTED_DRIVER_IDS


def test_driver_registry_carries_shape_and_buildable_metadata():
    for driver_id in ("sqlserver", "jtds", "custom"):
        entry = DatabaseConnectorBuilder.DRIVERS[driver_id]
        assert "shape" in entry, driver_id
        assert "buildable" in entry, driver_id
    assert DatabaseConnectorBuilder.DRIVERS["sqlserver"]["buildable"] is True
    assert DatabaseConnectorBuilder.DRIVERS["jtds"]["buildable"] is True
    assert DatabaseConnectorBuilder.DRIVERS["custom"]["buildable"] is False
    assert DatabaseConnectorBuilder.DRIVERS["custom"]["shape"] == "custom_url"


def test_custom_driver_id_returns_unsupported_db_driver_shape():
    with pytest.raises(BuilderValidationError) as excinfo:
        DatabaseConnectorBuilder().build(**_minimal_config(driver_id="custom"))
    err = excinfo.value
    assert err.error_code == "UNSUPPORTED_DB_DRIVER_SHAPE"
    assert err.field == "driver_id"
    hint = (err.hint or "").lower()
    assert "custom" in hint or "reuse" in hint or "raw-xml" in hint or "raw xml" in hint


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
