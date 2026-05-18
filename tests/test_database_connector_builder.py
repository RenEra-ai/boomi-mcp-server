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
