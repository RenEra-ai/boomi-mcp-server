"""Unit tests for DatabaseConnectorBuilder.

Verifies the XML matches the structure of a real exported Boomi component
(MS SQL Server Microsoft, component 4ace95d7-6ee4-4f83-8fad-723d3fabdb2f
on the renera account) and that field-level defaults / required-field
validation behave correctly.
"""

import xml.etree.ElementTree as ET

import pytest

from boomi_mcp.categories.components.builders.connector_builder import (
    CONNECTOR_BUILDERS,
    DatabaseConnectorBuilder,
    get_connector_builder,
)


NS = {"bns": "http://api.platform.boomi.com/"}


def _build_minimal(**overrides):
    params = {
        "component_name": "Test SQL",
        "driver_id": "sqlserver",
        "host": "host.docker.internal",
        "dbname": "Expert",
        "username": "sa",
    }
    params.update(overrides)
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


@pytest.mark.parametrize("missing", ["component_name", "driver_id", "host", "dbname", "username"])
def test_missing_required_field_raises_value_error(missing):
    params = {
        "component_name": "Test SQL",
        "driver_id": "sqlserver",
        "host": "host.docker.internal",
        "dbname": "Expert",
        "username": "sa",
    }
    params.pop(missing)
    with pytest.raises(ValueError, match=missing):
        DatabaseConnectorBuilder().build(**params)


def test_unknown_driver_id_raises_value_error():
    with pytest.raises(ValueError, match="mysql"):
        DatabaseConnectorBuilder().build(
            component_name="x",
            driver_id="mysql",
            host="h",
            dbname="d",
            username="u",
        )


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
