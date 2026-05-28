"""Unit tests for DatabaseGetOperationBuilder (Issue #23).

Verifies the emitted XML matches the structure of real exported Boomi
connector-action components (work-profile c4b1f2b8 "[Intapp CDS] Get DB
Server Current Date" with batchCount=0 and 949b3239 "[Intapp CDS] Global
SQL XML - Batch 50000" with batchCount=50000, fetched 2026-05-18).

The builder must:
- Emit a deterministic Operation envelope (Archiving -> Configuration ->
  Tracking -> Caching).
- Preserve `read_profile_id` verbatim (UUID or $ref:KEY token — substitution
  happens upstream in _resolve_dependency_tokens).
- Reject operation_mode='send' with UNSUPPORTED_DB_OPERATION_MODE and a
  hint pointing at issue #32.
- Reject `link_element` until its live XML shape is verified.
"""

import xml.etree.ElementTree as ET

import pytest

from boomi_mcp.categories.components.builders.connector_builder import (
    BuilderValidationError,
    CONNECTOR_ACTION_BUILDERS,
    DatabaseGetOperationBuilder,
    get_connector_action_builder,
)


NS = {"bns": "http://api.platform.boomi.com/"}


def _minimal_config(**overrides):
    params = {
        "component_type": "connector-action",
        "connector_type": "database",
        "operation_mode": "get",
        "component_name": "Test DB Query",
        "connection_ref_key": "db_connection",
        "read_profile_id": "5fe35b85-d8f4-409d-8197-03eee5c0c129",
    }
    params.update(overrides)
    return params


def _build_minimal(**overrides):
    return DatabaseGetOperationBuilder().build(**_minimal_config(**overrides))


# ----------------------------------------------------------------------------
# Registry
# ----------------------------------------------------------------------------

def test_database_get_registered_in_action_builders():
    assert ("database", "get") in CONNECTOR_ACTION_BUILDERS
    builder = get_connector_action_builder("database", "get")
    assert builder is not None
    assert builder.__class__ is DatabaseGetOperationBuilder


def test_get_connector_action_builder_unknown_returns_none():
    assert get_connector_action_builder("database", "send") is None
    assert get_connector_action_builder("http", "get") is None
    assert get_connector_action_builder("", "") is None


def test_get_connector_action_builder_is_case_insensitive():
    assert get_connector_action_builder("DATABASE", "GET") is not None


# ----------------------------------------------------------------------------
# Golden XML shape
# ----------------------------------------------------------------------------

def test_minimum_config_produces_valid_component_xml():
    xml = _build_minimal()
    root = ET.fromstring(xml)
    assert root.attrib["type"] == "connector-action"
    assert root.attrib["subType"] == "database"
    assert root.attrib["name"] == "Test DB Query"


def test_operation_envelope_order_matches_reference():
    xml = _build_minimal()
    root = ET.fromstring(xml)
    obj = root.find("bns:object", NS)
    operation = obj.find("Operation")
    assert operation is not None
    children = list(operation)
    tags = [c.tag for c in children]
    assert tags == ["Archiving", "Configuration", "Tracking", "Caching"]


def test_archiving_is_disabled_with_empty_directory():
    xml = _build_minimal()
    root = ET.fromstring(xml)
    archiving = root.find("bns:object/Operation/Archiving", NS)
    assert archiving.attrib["enabled"] == "false"
    assert archiving.attrib["directory"] == ""


def test_default_batch_count_and_max_rows_are_zero():
    xml = _build_minimal()
    root = ET.fromstring(xml)
    action = root.find("bns:object/Operation/Configuration/DatabaseGetAction", NS)
    assert action.attrib["batchCount"] == "0"
    assert action.attrib["maxRows"] == "0"


def test_cds_style_large_batch_count_is_emitted_verbatim():
    xml = _build_minimal(batch_count=50000)
    root = ET.fromstring(xml)
    action = root.find("bns:object/Operation/Configuration/DatabaseGetAction", NS)
    assert action.attrib["batchCount"] == "50000"


def test_max_rows_emitted_verbatim_when_provided():
    xml = _build_minimal(max_rows=100)
    root = ET.fromstring(xml)
    action = root.find("bns:object/Operation/Configuration/DatabaseGetAction", NS)
    assert action.attrib["maxRows"] == "100"


def test_read_profile_id_uuid_is_emitted_verbatim():
    xml = _build_minimal(read_profile_id="abc-123-def-456")
    root = ET.fromstring(xml)
    read_profile = root.find(
        "bns:object/Operation/Configuration/DatabaseGetAction/ReadProfile",
        NS,
    )
    assert read_profile.attrib["profileId"] == "abc-123-def-456"


def test_read_profile_id_ref_token_is_preserved_for_upstream_resolution():
    # The builder MUST NOT try to resolve $ref tokens itself —
    # _resolve_dependency_tokens in integration_builder handles that during
    # apply, after the read profile component has been created and its id
    # registered. Build-time output should retain the token verbatim so
    # plan-time inspection shows the dependency clearly.
    xml = _build_minimal(read_profile_id="$ref:db_read_profile")
    root = ET.fromstring(xml)
    read_profile = root.find(
        "bns:object/Operation/Configuration/DatabaseGetAction/ReadProfile",
        NS,
    )
    assert read_profile.attrib["profileId"] == "$ref:db_read_profile"


def test_tracking_emits_empty_tracked_fields():
    xml = _build_minimal()
    root = ET.fromstring(xml)
    tracking = root.find("bns:object/Operation/Tracking", NS)
    tracked = tracking.find("TrackedFields")
    assert tracked is not None
    assert list(tracked) == []


def test_caching_is_self_closing_empty_element():
    xml = _build_minimal()
    root = ET.fromstring(xml)
    caching = root.find("bns:object/Operation/Caching", NS)
    assert caching is not None
    assert list(caching) == []


def test_connection_ref_key_not_embedded_in_xml():
    # Boomi binds the connection at the process connector step, not in the
    # operation XML. The builder must not leak connection_ref_key into the
    # emitted output.
    xml = _build_minimal(connection_ref_key="some_special_key")
    assert "some_special_key" not in xml
    assert "connection_ref_key" not in xml


def test_folder_name_defaults_to_home():
    xml = _build_minimal()
    root = ET.fromstring(xml)
    assert root.attrib["folderName"] == "Home"


def test_component_name_and_folder_name_xml_escape():
    xml = _build_minimal(component_name="A & B <C>", folder_name="X/<y>")
    root = ET.fromstring(xml)
    assert root.attrib["name"] == "A & B <C>"
    assert root.attrib["folderName"] == "X/<y>"


# ----------------------------------------------------------------------------
# Structured validation errors
# ----------------------------------------------------------------------------

def test_operation_mode_send_is_rejected_with_issue32_hint():
    with pytest.raises(BuilderValidationError) as excinfo:
        DatabaseGetOperationBuilder().build(**_minimal_config(operation_mode="send"))
    assert excinfo.value.error_code == "UNSUPPORTED_DB_OPERATION_MODE"
    assert excinfo.value.field == "operation_mode"
    assert "#32" in (excinfo.value.hint or "")


@pytest.mark.parametrize("missing_value", [None, "", "   "])
def test_operation_mode_missing_is_rejected(missing_value):
    with pytest.raises(BuilderValidationError) as excinfo:
        DatabaseGetOperationBuilder().build(**_minimal_config(operation_mode=missing_value))
    assert excinfo.value.error_code == "UNSUPPORTED_DB_OPERATION_MODE"


def test_operation_mode_unknown_is_rejected():
    with pytest.raises(BuilderValidationError) as excinfo:
        DatabaseGetOperationBuilder().build(**_minimal_config(operation_mode="upsert"))
    assert excinfo.value.error_code == "UNSUPPORTED_DB_OPERATION_MODE"


@pytest.mark.parametrize("missing_value", [None, "", "   "])
def test_missing_component_name_raises_structured_error(missing_value):
    with pytest.raises(BuilderValidationError) as excinfo:
        DatabaseGetOperationBuilder().build(**_minimal_config(component_name=missing_value))
    assert excinfo.value.error_code == "DATABASE_OPERATION_VALIDATION_FAILED"
    assert excinfo.value.field == "component_name"


@pytest.mark.parametrize("missing_value", [None, "", "   "])
def test_missing_read_profile_id_raises_missing_db_read_profile_ref(missing_value):
    with pytest.raises(BuilderValidationError) as excinfo:
        DatabaseGetOperationBuilder().build(**_minimal_config(read_profile_id=missing_value))
    assert excinfo.value.error_code == "MISSING_DB_READ_PROFILE_REF"
    assert excinfo.value.field == "read_profile_id"


@pytest.mark.parametrize("bad_batch", [-1, 1.5, "many", True])
def test_invalid_batch_count_raises_structured_error(bad_batch):
    with pytest.raises(BuilderValidationError) as excinfo:
        DatabaseGetOperationBuilder().build(**_minimal_config(batch_count=bad_batch))
    assert excinfo.value.error_code == "DATABASE_OPERATION_VALIDATION_FAILED"
    assert excinfo.value.field == "batch_count"


@pytest.mark.parametrize("bad_max", [-5, 2.7, "all", False])
def test_invalid_max_rows_raises_structured_error(bad_max):
    with pytest.raises(BuilderValidationError) as excinfo:
        DatabaseGetOperationBuilder().build(**_minimal_config(max_rows=bad_max))
    assert excinfo.value.error_code == "DATABASE_OPERATION_VALIDATION_FAILED"
    assert excinfo.value.field == "max_rows"


def test_link_element_is_rejected_pending_shape_verification():
    with pytest.raises(BuilderValidationError) as excinfo:
        DatabaseGetOperationBuilder().build(**_minimal_config(link_element="someField"))
    assert excinfo.value.error_code == "UNSUPPORTED_DB_GET_FIELD"
    assert excinfo.value.field == "link_element"


# ----------------------------------------------------------------------------
# Secret scanning (delegated to DatabaseConnectorBuilder shape)
# ----------------------------------------------------------------------------

def test_plaintext_secret_in_config_is_rejected():
    cfg = _minimal_config()
    cfg["password"] = "supersecret"
    with pytest.raises(BuilderValidationError) as excinfo:
        DatabaseGetOperationBuilder().build(**cfg)
    assert excinfo.value.error_code == "PLAINTEXT_SECRET_REJECTED"


# ----------------------------------------------------------------------------
# validate_config separate from build()
# ----------------------------------------------------------------------------

def test_validate_config_returns_none_for_minimal_valid_config():
    assert DatabaseGetOperationBuilder.validate_config(_minimal_config()) is None


def test_validate_config_returns_first_error_without_raising():
    err = DatabaseGetOperationBuilder.validate_config(_minimal_config(operation_mode="send"))
    assert err is not None
    assert err.error_code == "UNSUPPORTED_DB_OPERATION_MODE"


# ----------------------------------------------------------------------------
# Standalone manage_connector dispatcher must surface structured error for
# database send (regression for Bug #121: dispatcher was returning a generic
# "no builder" envelope instead of UNSUPPORTED_DB_OPERATION_MODE + #32 hint).
# ----------------------------------------------------------------------------

def test_create_connector_dispatcher_surfaces_unsupported_db_operation_mode_for_send():
    from unittest.mock import MagicMock
    from boomi_mcp.categories.components.connectors import create_connector

    boomi_client = MagicMock()
    boomi_client.connector.get_connector.return_value = MagicMock()
    result = create_connector(
        boomi_client,
        "dev",
        _minimal_config(operation_mode="send"),
    )
    assert result["_success"] is False
    assert result["error_code"] == "UNSUPPORTED_DB_OPERATION_MODE"
    assert "#32" in (result["hint"] or "")
    assert result["field"] == "operation_mode"


def test_create_connector_dispatcher_surfaces_link_element_rejection_through_validator():
    from unittest.mock import MagicMock
    from boomi_mcp.categories.components.connectors import create_connector

    boomi_client = MagicMock()
    boomi_client.connector.get_connector.return_value = MagicMock()
    # link_element is rejected during build() via validate_config (the
    # registered builder runs validate_config inside build()).
    cfg = _minimal_config(link_element="some_field")
    result = create_connector(boomi_client, "dev", cfg)
    assert result["_success"] is False
    assert result["error_code"] == "UNSUPPORTED_DB_GET_FIELD"


# ============================================================================
# Issue #45 — Component XML update preservation
# ============================================================================


def test_database_get_operation_preservation_policy_attached():
    policy = DatabaseGetOperationBuilder.PRESERVATION_POLICY
    assert policy.component_type == "connector-action"
    assert policy.subtype == "database"
    paths = {op.path for op in policy.owned_paths}
    assert paths == {"bns:object/Operation/Configuration/DatabaseGetAction"}


def test_database_get_operation_update_preserves_tracking_caching_archiving():
    """Operation-level Archiving / Tracking / Caching are NOT owned —
    builder emits empty placeholders but live current values must survive."""
    from boomi_mcp.categories.components.component_update_preservation import (
        merge_for_update,
    )

    desired = _build_minimal(read_profile_id="new-profile-id", batch_count=500)
    current = _build_minimal(read_profile_id="old-profile-id", batch_count=100)
    # Replace placeholders in current with realistic configured values
    current = current.replace(
        '<Archiving directory="" enabled="false"/>',
        '<Archiving directory="/var/log/boomi/archive" enabled="true"/>',
    )
    current = current.replace(
        '<Tracking><TrackedFields/></Tracking>',
        '<Tracking><TrackedFields><TrackedField name="custom" path="//x"/></TrackedFields></Tracking>',
    )
    current = current.replace(
        '<Caching/>',
        '<Caching enabled="true" ttl="3600"/>',
    )

    merged = merge_for_update(
        current, desired, DatabaseGetOperationBuilder.PRESERVATION_POLICY
    )
    root = ET.fromstring(merged)
    # Action subtree was replaced from desired
    action = root.find("bns:object/Operation/Configuration/DatabaseGetAction", NS)
    assert action is not None
    assert action.attrib["batchCount"] == "500"
    assert action.find("ReadProfile").attrib["profileId"] == "new-profile-id"
    # But the Operation envelope siblings survive verbatim
    archiving = root.find("bns:object/Operation/Archiving", NS)
    assert archiving.attrib["directory"] == "/var/log/boomi/archive"
    assert archiving.attrib["enabled"] == "true"
    tracking = root.find("bns:object/Operation/Tracking/TrackedFields/TrackedField", NS)
    assert tracking is not None
    assert tracking.attrib["name"] == "custom"
    caching = root.find("bns:object/Operation/Caching", NS)
    assert caching.attrib.get("ttl") == "3600"


def test_database_get_operation_update_preserves_unknown_attr_inside_action():
    """Review follow-up: DatabaseGetAction uses subtree_merge, so unknown
    attrs/children on the action element survive a structured update
    while owned batch attrs + ReadProfile still update."""
    from boomi_mcp.categories.components.component_update_preservation import (
        merge_for_update,
    )

    desired = _build_minimal(read_profile_id="new-profile", batch_count=500)
    current = _build_minimal(read_profile_id="old-profile", batch_count=100)
    # Inject unknown attr + child onto DatabaseGetAction.
    current = current.replace(
        '<DatabaseGetAction batchCount="100" maxRows=',
        '<DatabaseGetAction futureAttr="opaque" batchCount="100" maxRows=',
        1,
    )
    current = current.replace(
        "</DatabaseGetAction>",
        '<FutureBlock retained="yes"/></DatabaseGetAction>',
        1,
    )

    merged = merge_for_update(
        current, desired, DatabaseGetOperationBuilder.PRESERVATION_POLICY
    )
    root = ET.fromstring(merged)
    action = root.find("bns:object/Operation/Configuration/DatabaseGetAction", NS)
    # Owned attr + ReadProfile updated.
    assert action.attrib["batchCount"] == "500"
    assert action.find("ReadProfile").attrib["profileId"] == "new-profile"
    # Unknown attr + child preserved.
    assert action.attrib.get("futureAttr") == "opaque"
    assert action.find("FutureBlock") is not None
