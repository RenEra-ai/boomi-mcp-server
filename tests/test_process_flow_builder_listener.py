"""Listener (WSS Listen) process emission + sync_pipeline lowering tests (M6, #12).

Start-shape XML locked against the live renera Process Library process
a5d9f624-e254-41a9-af55-172989d1a712 ("Weblistener to Slack"), captured
2026-07-04 (.codex/plans/m6-listener-recon.md §1): the connectoraction lives
INSIDE the start shape (actionType="Listen" allowDynamicCredentials="NONE"
connectorType="wss" hideSettings="true", bare <parameters/>, NO connectionId),
and the process options are allowSimultaneous="true" ... updateRunDates="false"
with NO stopProcessingIfZeroDocuments attribute.
"""

import xml.etree.ElementTree as ET

import pytest

from boomi_mcp.categories.components.builders.connector_builder import (
    BuilderValidationError,
)
from boomi_mcp.categories.components.builders.process_flow_builder import (
    ProcessFlowBuilder,
    SyncPipelineBuilder,
    _LISTENER_PROCESS_OPTIONS,
)


# ---------------------------------------------------------------------------
# Config builders
# ---------------------------------------------------------------------------


def _listener_pipeline_config(**listener_overrides):
    listener_config = {"primitive": "wss_listen", "operation_id": "WSSOP-1"}
    listener_config.update(listener_overrides)
    return {
        "process_kind": "sync_pipeline",
        "pipeline": {
            "stages": [
                {"key": "listen", "kind": "listener", "config": listener_config},
                {
                    "key": "map",
                    "kind": "map",
                    "config": {"primitive": "map", "map_ref": "MAP-1"},
                },
                {
                    "key": "send",
                    "kind": "send",
                    "config": {
                        "primitive": "rest_send",
                        "action_type": "POST",
                        "connection_id": "CONN-1",
                        "operation_id": "OP-1",
                    },
                },
            ],
            "dependencies": [
                {"from_stage": "listen", "to_stage": "map"},
                {"from_stage": "map", "to_stage": "send"},
            ],
        },
    }


def _lowered_listener_config(**extra):
    """A directly lowered database_to_api_sync-shaped config with a WSS source
    (what SyncPipelineBuilder.lower_config produces)."""
    config = {
        "process_kind": "database_to_api_sync",
        "source": {
            "connector_type": "wss",
            "action_type": "Listen",
            "operation_id": "WSSOP-1",
        },
        "transform": {"mode": "passthrough"},
        "target": {
            "connector_type": "rest",
            "action_type": "POST",
            "connection_id": "CONN-1",
            "operation_id": "OP-1",
        },
    }
    config.update(extra)
    return config


def _scheduled_config():
    return {
        "process_kind": "database_to_api_sync",
        "source": {
            "connector_type": "database",
            "action_type": "Get",
            "connection_id": "DBCONN-1",
            "operation_id": "DBOP-1",
        },
        "transform": {"mode": "passthrough"},
        "target": {
            "connector_type": "rest",
            "action_type": "POST",
            "connection_id": "CONN-1",
            "operation_id": "OP-1",
        },
    }


def _shapes(xml: str):
    root = ET.fromstring(xml)
    return root.findall(".//process/shapes/shape")


def _process_element(xml: str) -> ET.Element:
    return ET.fromstring(xml).find(".//process")


# ---------------------------------------------------------------------------
# Listen start shape emission (via the sync_pipeline lowering)
# ---------------------------------------------------------------------------


def test_listener_start_shape_matches_live_capture():
    xml = SyncPipelineBuilder.build(_listener_pipeline_config(), name="L")
    start = _shapes(xml)[0]
    assert start.attrib["shapetype"] == "start"
    assert start.attrib["image"] == "start"
    connectoraction = start.find("configuration/connectoraction")
    assert connectoraction is not None, "connectoraction must be INSIDE the start shape"
    assert connectoraction.attrib == {
        "actionType": "Listen",
        "allowDynamicCredentials": "NONE",
        "connectorType": "wss",
        "hideSettings": "true",
        "operationId": "WSSOP-1",
    }
    # No connectionId attribute (WSS has no connection component) and a bare
    # <parameters/> body — exactly the live capture.
    assert "connectionId" not in connectoraction.attrib
    assert [child.tag for child in connectoraction] == ["parameters"]
    # No <noaction/> start on a listener build.
    assert start.find("configuration/noaction") is None


def test_listener_start_replaces_source_connector_shape():
    """The listener collapses start+source into ONE shape: start -> map -> target -> stop."""
    xml = SyncPipelineBuilder.build(_listener_pipeline_config(), name="L")
    shapetypes = [s.attrib["shapetype"] for s in _shapes(xml)]
    assert shapetypes == ["start", "map", "connectoraction", "stop"]
    # The start dragpoint wires straight to the next emitted shape.
    start = _shapes(xml)[0]
    dragpoint = start.find("dragpoints/dragpoint")
    assert dragpoint.attrib["toShape"] == "shape2"


def test_listener_process_options_locked_to_live_capture():
    xml = SyncPipelineBuilder.build(_listener_pipeline_config(), name="L")
    process = _process_element(xml)
    assert process.attrib == {
        "allowSimultaneous": "true",
        "enableUserLog": "false",
        "processLogOnErrorOnly": "false",
        "purgeDataImmediately": "false",
        "updateRunDates": "false",
        "workload": "general",
    }
    # The live listener capture omits stopProcessingIfZeroDocuments entirely.
    assert "stopProcessingIfZeroDocuments" not in process.attrib
    # The constant used by build() matches this exact attribute set.
    assert 'allowSimultaneous="true"' in _LISTENER_PROCESS_OPTIONS
    assert 'updateRunDates="false"' in _LISTENER_PROCESS_OPTIONS


def test_scheduled_process_options_byte_unchanged():
    """Non-listener output keeps the pre-M6 scheduled options byte-for-byte."""
    xml = ProcessFlowBuilder.build(_scheduled_config(), name="S")
    assert (
        '<process xmlns="" allowSimultaneous="false" enableUserLog="false" '
        'processLogOnErrorOnly="false" purgeDataImmediately="false" '
        'stopProcessingIfZeroDocuments="true" updateRunDates="true" '
        'workload="general">'
    ) in xml


def test_listener_label_lowered_to_start_userlabel():
    xml = SyncPipelineBuilder.build(
        _listener_pipeline_config(label="Order Intake"), name="L"
    )
    start = _shapes(xml)[0]
    assert start.attrib["userlabel"] == "Order Intake"


def test_listener_write_chain_lowers():
    config = _listener_pipeline_config()
    config["pipeline"]["stages"][2] = {
        "key": "write",
        "kind": "write",
        "config": {
            "primitive": "db_write",
            "connection_id": "DBCONN-1",
            "operation_id": "DBSEND-1",
        },
    }
    config["pipeline"]["dependencies"][1] = {"from_stage": "map", "to_stage": "write"}
    assert SyncPipelineBuilder.validate_config(config) is None
    xml = SyncPipelineBuilder.build(config, name="L")
    shapes = _shapes(xml)
    assert shapes[0].find("configuration/connectoraction").attrib["actionType"] == "Listen"
    target = shapes[2].find("configuration/connectoraction")
    assert target.attrib["connectorType"] == "database"
    assert target.attrib["actionType"] == "Send"


def test_listener_send_without_map_lowers():
    config = _listener_pipeline_config()
    del config["pipeline"]["stages"][1]
    config["pipeline"]["dependencies"] = [{"from_stage": "listen", "to_stage": "send"}]
    assert SyncPipelineBuilder.validate_config(config) is None
    xml = SyncPipelineBuilder.build(config, name="L")
    assert [s.attrib["shapetype"] for s in _shapes(xml)] == [
        "start",
        "connectoraction",
        "stop",
    ]


# ---------------------------------------------------------------------------
# sync_pipeline listener stage validation
# ---------------------------------------------------------------------------


def test_listener_stage_requires_wss_listen_primitive():
    config = _listener_pipeline_config(primitive="rest_fetch")
    err = SyncPipelineBuilder.validate_config(config)
    assert err is not None
    assert err.error_code == "SYNC_PIPELINE_STAGE_UNSUPPORTED"
    assert "rest_fetch" in str(err)


def test_wss_listen_primitive_on_wrong_stage_rejected():
    config = _listener_pipeline_config()
    config["pipeline"]["stages"][0] = {
        "key": "listen",
        "kind": "fetch",
        "config": {"primitive": "wss_listen", "operation_id": "WSSOP-1"},
    }
    err = SyncPipelineBuilder.validate_config(config)
    assert err is not None
    assert "listener" in (err.hint or "")


def test_listener_stage_rejects_connection_id():
    config = _listener_pipeline_config(connection_id="CONN-X")
    err = SyncPipelineBuilder.validate_config(config)
    assert err is not None
    assert err.error_code == "SYNC_PIPELINE_CONFIG_INVALID"
    assert "connection" in (err.hint or "").lower()


def test_listener_stage_rejects_action_type_override():
    config = _listener_pipeline_config(action_type="Listen")
    err = SyncPipelineBuilder.validate_config(config)
    assert err is not None
    assert err.error_code == "SYNC_PIPELINE_CONFIG_INVALID"


def test_listener_stage_requires_operation_id():
    config = _listener_pipeline_config()
    del config["pipeline"]["stages"][0]["config"]["operation_id"]
    err = SyncPipelineBuilder.validate_config(config)
    assert err is not None
    assert err.error_code == "PROCESS_CONNECTOR_BINDING_INVALID"


def test_listener_stage_rejects_non_wss_connector_type():
    config = _listener_pipeline_config(connector_type="rest")
    err = SyncPipelineBuilder.validate_config(config)
    assert err is not None
    assert err.error_code == "SYNC_PIPELINE_CONFIG_INVALID"


def test_listener_stage_accepts_wss_alias_connector_type():
    config = _listener_pipeline_config(connector_type="web_services")
    assert SyncPipelineBuilder.validate_config(config) is None


def test_read_write_chain_still_rejected():
    """Adding listener chains must not loosen the existing chain gate."""
    config = _listener_pipeline_config()
    config["pipeline"]["stages"][0] = {
        "key": "listen",
        "kind": "read",
        "config": {
            "primitive": "db_read",
            "connection_id": "DBCONN-1",
            "operation_id": "DBOP-1",
        },
    }
    config["pipeline"]["stages"][2] = {
        "key": "send",
        "kind": "write",
        "config": {
            "primitive": "db_write",
            "connection_id": "DBCONN-1",
            "operation_id": "DBSEND-1",
        },
    }
    err = SyncPipelineBuilder.validate_config(config)
    assert err is not None
    assert err.error_code == "SYNC_PIPELINE_CONTROL_FLOW_UNSUPPORTED"


# ---------------------------------------------------------------------------
# Base-builder listener source validation
# ---------------------------------------------------------------------------


def test_hand_authored_database_to_api_sync_rejects_wss_source():
    """allow_listener_source stays False for the base protocol."""
    err = ProcessFlowBuilder.validate_config(_lowered_listener_config())
    assert err is not None
    assert err.error_code == "PROCESS_CONNECTOR_BINDING_INVALID"
    assert err.field == "source.connector_type"


def test_lowered_listener_source_accepted_with_flag():
    err = ProcessFlowBuilder.validate_config(
        _lowered_listener_config(), allow_listener_source=True
    )
    assert err is None


def test_listener_source_requires_listen_action():
    config = _lowered_listener_config()
    config["source"]["action_type"] = "GET"
    err = ProcessFlowBuilder.validate_config(config, allow_listener_source=True)
    assert err is not None
    assert err.field == "source.action_type"


def test_listener_source_rejects_connection_id():
    config = _lowered_listener_config()
    config["source"]["connection_id"] = "CONN-X"
    err = ProcessFlowBuilder.validate_config(config, allow_listener_source=True)
    assert err is not None
    assert err.field == "source.connection_id"


def test_listener_source_rejects_dynamic_path():
    config = _lowered_listener_config()
    config["source"]["dynamic_path"] = {"ddp_name": "X", "segments": []}
    err = ProcessFlowBuilder.validate_config(config, allow_listener_source=True)
    assert err is not None
    assert err.field == "source.dynamic_path"


def test_listener_source_requires_operation_id():
    config = _lowered_listener_config()
    config["source"]["operation_id"] = "  "
    err = ProcessFlowBuilder.validate_config(config, allow_listener_source=True)
    assert err is not None
    assert err.field == "source.operation_id"


# ---------------------------------------------------------------------------
# Composition guards (M6 verified-linear)
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "block, value",
    [
        ("reliability", {"retry_count": 1, "dlq": {"mode": "document_cache_ref", "document_cache_id": "C1"}}),
        ("flow_control", {"enabled": True, "for_each_count": 5}),
        ("branch", {"enabled": True, "targets": []}),
        ("decision", {"enabled": True}),
        ("return_documents", {"enabled": True}),
    ],
)
def test_listener_composition_blocks_rejected_on_validate(block, value):
    config = _lowered_listener_config(**{block: value})
    err = ProcessFlowBuilder.validate_config(config, allow_listener_source=True)
    assert err is not None
    assert err.error_code == "PROCESS_LISTENER_COMPOSITION_UNSUPPORTED"
    assert err.field == block


def test_listener_composition_blocks_rejected_on_direct_build():
    """Totality: a validate_config bypass raises the same structured error."""
    config = _lowered_listener_config(
        reliability={"retry_count": 1, "dlq": {"mode": "document_cache_ref", "document_cache_id": "C1"}}
    )
    with pytest.raises(BuilderValidationError) as exc:
        ProcessFlowBuilder.build(config, name="L")
    assert exc.value.error_code == "PROCESS_LISTENER_COMPOSITION_UNSUPPORTED"


def test_sync_pipeline_still_gates_reliability_top_level():
    config = _listener_pipeline_config()
    config["reliability"] = {"retry_count": 1}
    err = SyncPipelineBuilder.validate_config(config)
    assert err is not None
    assert err.error_code == "SYNC_PIPELINE_CONFIG_INVALID"


def test_flow_sequence_with_wss_source_rejected():
    """The composed flow_sequence path stays DB-source-only — a listener source
    is rejected inside its own validation order."""
    config = _lowered_listener_config(
        flow_sequence=[{"kind": "message", "config": {"text": "hi"}}]
    )
    err = ProcessFlowBuilder.validate_config(config, allow_listener_source=True)
    assert err is not None
    assert err.error_code == "PROCESS_CONNECTOR_BINDING_INVALID"
