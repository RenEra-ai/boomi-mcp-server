"""Tests for HttpConnectorBuilder validation + XML emission (issue #24).

The XML shape for auth_type='NONE' is locked by integration_builder's existing
`test_http_connector_settings_skips_database_preflight` test — any change to
the emitted XML would silently break dev flows that already consume it. Issue
#24 only adds structured validation on top of the existing emission path.
"""

import xml.etree.ElementTree as ET

import pytest

from boomi_mcp.categories.components.builders.connector_builder import (
    BuilderValidationError,
    HttpConnectorBuilder,
)


NS = {"bns": "http://api.platform.boomi.com/"}

_FORBIDDEN_SECRET_FIELDS = (
    "password",
    "password_ref",
    "secret",
    "token",
    "access_token",
    "client_secret",
)


def _minimal_config(**overrides):
    """Raw config dict — used for validate_config + create_connector tests."""
    params = {
        "connector_type": "http",
        "component_name": "Target API Connection",
        "url": "https://api.example.com",
        "auth_type": "NONE",
    }
    params.update(overrides)
    return params


def _build_minimal(**overrides):
    """Render minimal-valid XML — used for XML-shape tests."""
    params = _minimal_config(**overrides)
    params.pop("connector_type", None)  # builder doesn't consume this key
    return HttpConnectorBuilder().build(**params)


# ----------------------------------------------------------------------------
# Happy path
# ----------------------------------------------------------------------------

def test_minimum_required_fields_produce_valid_xml():
    xml = _build_minimal()
    root = ET.fromstring(xml)
    assert root.tag == "{http://api.platform.boomi.com/}Component"
    assert root.attrib["type"] == "connector-settings"
    assert root.attrib["subType"] == "http"
    assert root.attrib["name"] == "Target API Connection"
    assert root.attrib["folderName"] == "Home"


def test_http_settings_carries_url_and_auth_type():
    xml = _build_minimal()
    root = ET.fromstring(xml)
    settings = root.find("bns:object/HttpSettings", NS)
    assert settings is not None
    assert settings.attrib["authenticationType"] == "NONE"
    assert settings.attrib["url"] == "https://api.example.com"


def test_description_is_emitted_as_child_element():
    xml = _build_minimal(description="Target API for sync sends")
    root = ET.fromstring(xml)
    desc = root.find("bns:description", NS)
    assert desc is not None
    assert desc.text == "Target API for sync sends"


def test_xml_special_characters_are_escaped():
    xml = _build_minimal(
        component_name='Target "Prod" & <Dev>',
        url='https://api.example.com/?q=a&b=<x>',
        description='Calls Acme & Co APIs <legacy>',
        folder_name='Process "Library"',
    )
    root = ET.fromstring(xml)
    assert root.attrib["name"] == 'Target "Prod" & <Dev>'
    assert root.attrib["folderName"] == 'Process "Library"'
    settings = root.find("bns:object/HttpSettings", NS)
    assert settings.attrib["url"] == "https://api.example.com/?q=a&b=<x>"


def test_validate_config_returns_none_for_valid_config():
    assert HttpConnectorBuilder.validate_config(_minimal_config()) is None


# ----------------------------------------------------------------------------
# Required-field validation
# ----------------------------------------------------------------------------

def test_missing_component_name_returns_structured_error():
    with pytest.raises(BuilderValidationError) as excinfo:
        HttpConnectorBuilder().build(**_minimal_config(component_name=""))
    err = excinfo.value
    assert err.error_code == "HTTP_CONNECTOR_VALIDATION_FAILED"
    assert err.field == "component_name"


def test_missing_url_returns_structured_error():
    with pytest.raises(BuilderValidationError) as excinfo:
        HttpConnectorBuilder().build(**_minimal_config(url=""))
    err = excinfo.value
    assert err.error_code == "MISSING_HTTP_ENDPOINT"
    assert err.field == "url"


def test_validate_config_surfaces_missing_url_without_raising():
    err = HttpConnectorBuilder.validate_config(_minimal_config(url=""))
    assert isinstance(err, BuilderValidationError)
    assert err.error_code == "MISSING_HTTP_ENDPOINT"


# ----------------------------------------------------------------------------
# Auth-mode gating — only NONE is buildable for issue #24
# ----------------------------------------------------------------------------

@pytest.mark.parametrize(
    "unsupported",
    ["BASIC", "OAUTH2", "PASSWORD_DIGEST", "CUSTOM", "OAUTH"],
)
def test_recognized_but_unsupported_auth_modes_rejected(unsupported):
    with pytest.raises(BuilderValidationError) as excinfo:
        HttpConnectorBuilder().build(**_minimal_config(auth_type=unsupported))
    err = excinfo.value
    assert err.error_code == "UNSUPPORTED_HTTP_AUTH_MODE"
    assert err.field == "auth_type"


def test_unknown_auth_mode_returns_unsupported_with_supported_hint():
    with pytest.raises(BuilderValidationError) as excinfo:
        HttpConnectorBuilder().build(**_minimal_config(auth_type="FOO"))
    err = excinfo.value
    assert err.error_code == "UNSUPPORTED_HTTP_AUTH_MODE"
    assert "NONE" in (err.hint or "")


def test_auth_type_none_is_buildable():
    # Sanity: NONE remains the supported mode after the auth-gating change.
    xml = _build_minimal(auth_type="NONE")
    root = ET.fromstring(xml)
    assert root.find("bns:object/HttpSettings", NS).attrib["authenticationType"] == "NONE"


# ----------------------------------------------------------------------------
# Plaintext secret rejection
# ----------------------------------------------------------------------------

@pytest.mark.parametrize("forbidden", _FORBIDDEN_SECRET_FIELDS)
def test_forbidden_secret_fields_rejected(forbidden):
    params = _minimal_config(**{forbidden: "LEAKED_HTTP_DEADBEEF"})
    with pytest.raises(BuilderValidationError) as excinfo:
        HttpConnectorBuilder().build(**params)
    err = excinfo.value
    assert err.error_code == "PLAINTEXT_SECRET_REJECTED"
    assert err.field == forbidden
    # The offender's value must not appear in the error envelope.
    assert "LEAKED_HTTP_DEADBEEF" not in str(err)
    assert "LEAKED_HTTP_DEADBEEF" not in (err.hint or "")


def test_validate_config_detects_secrets_without_raising():
    err = HttpConnectorBuilder.validate_config(
        _minimal_config(password="hunter2"),
    )
    assert isinstance(err, BuilderValidationError)
    assert err.error_code == "PLAINTEXT_SECRET_REJECTED"
    assert err.field == "password"


def test_scan_forbidden_secret_fields_descends_into_nested_dicts():
    err = HttpConnectorBuilder.scan_forbidden_secret_fields({
        "wrapper": {"password": "DEEP_DEADBEEF"},
    })
    assert isinstance(err, BuilderValidationError)
    assert err.error_code == "PLAINTEXT_SECRET_REJECTED"
    assert err.field == "wrapper.password"
