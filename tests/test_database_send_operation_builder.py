"""Unit tests for DatabaseSendOperationBuilder (Issue #32, M5.6).

Structural ElementTree assertions against the shape of real renera
connector-action database Send exports (2026-06-27): "Standard Insert"
(commitOption="commitprofile") and "Commit by Rows" (commitOption="commitrows"),
both referencing a write profile via <WriteProfile profileId="..."/>.

The builder must:
- Emit the Operation envelope (Archiving -> Configuration -> Tracking -> Caching)
  with subType="database".
- Emit <DatabaseSendAction batchCount commitOption enableBatching> with a
  <WriteProfile profileId="..."/> child.
- Preserve write_profile_id verbatim (UUID or $ref:KEY token).
- Reject missing write profile, invalid commit option, invalid batch config.
"""

import xml.etree.ElementTree as ET

import pytest

from boomi_mcp.categories.components.builders.connector_builder import (
    BuilderValidationError,
    CONNECTOR_ACTION_BUILDERS,
    DatabaseSendOperationBuilder,
    get_connector_action_builder,
)


NS = {"bns": "http://api.platform.boomi.com/"}


def _cfg(**overrides):
    params = {
        "component_type": "connector-action",
        "connector_type": "database",
        "operation_mode": "send",
        "component_name": "Test DB Write",
        "connection_ref_key": "db_connection",
        "write_profile_id": "b7ad0684-db76-445a-89a4-1c6a832ef204",
    }
    params.update(overrides)
    return params


def _action(xml: str):
    root = ET.fromstring(xml)
    return root.find(
        "bns:object/Operation/Configuration/DatabaseSendAction", NS
    )


# ----------------------------------------------------------------------------
# Registry
# ----------------------------------------------------------------------------

def test_database_send_registered():
    assert ("database", "send") in CONNECTOR_ACTION_BUILDERS
    builder = get_connector_action_builder("database", "send")
    assert builder is not None
    assert builder.__class__ is DatabaseSendOperationBuilder


def test_factory_case_insensitive():
    assert get_connector_action_builder("DATABASE", "SEND") is not None


# ----------------------------------------------------------------------------
# Envelope / golden shape
# ----------------------------------------------------------------------------

def test_envelope_is_database_connector_action():
    xml = DatabaseSendOperationBuilder().build(**_cfg())
    root = ET.fromstring(xml)
    assert root.attrib["type"] == "connector-action"
    assert root.attrib["subType"] == "database"
    assert root.attrib["name"] == "Test DB Write"


def test_operation_envelope_order():
    xml = DatabaseSendOperationBuilder().build(**_cfg())
    obj = ET.fromstring(xml).find("bns:object", NS)
    operation = obj.find("Operation")
    tags = [c.tag for c in operation]
    assert tags == ["Archiving", "Configuration", "Tracking", "Caching"]


def test_commit_profile_defaults():
    action = _action(DatabaseSendOperationBuilder().build(**_cfg()))
    assert action.get("commitOption") == "commitprofile"
    assert action.get("batchCount") == "0"
    assert action.get("enableBatching") == "true"
    assert action.find("WriteProfile").get("profileId") == (
        "b7ad0684-db76-445a-89a4-1c6a832ef204"
    )


def test_commit_rows_with_batch():
    action = _action(
        DatabaseSendOperationBuilder().build(
            **_cfg(commit_option="commitrows", batch_count=200,
                   enable_batching=False)
        )
    )
    assert action.get("commitOption") == "commitrows"
    assert action.get("batchCount") == "200"
    assert action.get("enableBatching") == "false"


def test_commit_rows_with_zero_batch_is_allowed():
    # Matches the live "Commit by Rows" export (batchCount=0, commitrows).
    action = _action(
        DatabaseSendOperationBuilder().build(
            **_cfg(commit_option="commitrows", batch_count=0)
        )
    )
    assert action.get("commitOption") == "commitrows"
    assert action.get("batchCount") == "0"


def test_write_profile_ref_token_preserved_verbatim():
    xml = DatabaseSendOperationBuilder().build(
        **_cfg(write_profile_id="$ref:db_write_profile")
    )
    action = _action(xml)
    assert action.find("WriteProfile").get("profileId") == "$ref:db_write_profile"


def test_connection_ref_key_not_emitted():
    # Boomi binds the connection at the process connector step, not the op XML.
    xml = DatabaseSendOperationBuilder().build(**_cfg())
    assert "db_connection" not in xml
    assert "connection_ref_key" not in xml


# ----------------------------------------------------------------------------
# Negative cases
# ----------------------------------------------------------------------------

@pytest.mark.parametrize("missing", [None, "", "   "])
def test_missing_write_profile_rejected(missing):
    with pytest.raises(BuilderValidationError) as exc:
        DatabaseSendOperationBuilder().build(**_cfg(write_profile_id=missing))
    assert exc.value.error_code == "MISSING_DB_WRITE_PROFILE_REF"


def test_invalid_commit_option_rejected():
    with pytest.raises(BuilderValidationError) as exc:
        DatabaseSendOperationBuilder().build(**_cfg(commit_option="commitnow"))
    assert exc.value.error_code == "INVALID_DB_COMMIT_OPTION"
    assert exc.value.field == "commit_option"


@pytest.mark.parametrize("bad", [-1, "5", True, 1.5])
def test_invalid_batch_count_rejected(bad):
    with pytest.raises(BuilderValidationError) as exc:
        DatabaseSendOperationBuilder().build(**_cfg(batch_count=bad))
    assert exc.value.error_code == "INVALID_DB_BATCH_CONFIG"
    assert exc.value.field == "batch_count"


@pytest.mark.parametrize("bad", ["true", 1, None])
def test_invalid_enable_batching_rejected(bad):
    with pytest.raises(BuilderValidationError) as exc:
        DatabaseSendOperationBuilder().build(**_cfg(enable_batching=bad))
    assert exc.value.error_code == "INVALID_DB_BATCH_CONFIG"
    assert exc.value.field == "enable_batching"


@pytest.mark.parametrize("mode", [None, "", "get", "upsert"])
def test_wrong_operation_mode_rejected(mode):
    with pytest.raises(BuilderValidationError) as exc:
        DatabaseSendOperationBuilder().build(**_cfg(operation_mode=mode))
    assert exc.value.error_code == "UNSUPPORTED_DB_OPERATION_MODE"


def test_missing_component_name_rejected():
    with pytest.raises(BuilderValidationError) as exc:
        DatabaseSendOperationBuilder().build(**_cfg(component_name=""))
    assert exc.value.error_code == "DATABASE_OPERATION_VALIDATION_FAILED"


def test_plaintext_secret_rejected():
    with pytest.raises(BuilderValidationError) as exc:
        DatabaseSendOperationBuilder().build(**_cfg(password="leak"))
    assert exc.value.error_code == "PLAINTEXT_SECRET_REJECTED"


# ----------------------------------------------------------------------------
# Standalone manage_connector dispatcher
# ----------------------------------------------------------------------------

def test_create_connector_dispatcher_builds_send_operation():
    from unittest.mock import MagicMock, patch
    from boomi_mcp.categories.components import connectors

    boomi_client = MagicMock()
    with patch.object(
        connectors,
        "_create_component_raw",
        return_value={
            "component_id": "new-id",
            "name": "Test DB Write",
            "type": "connector-action",
            "sub_type": "database",
        },
    ):
        result = connectors.create_connector(boomi_client, "dev", _cfg())
    assert result["_success"] is True
    assert result["component_id"] == "new-id"
