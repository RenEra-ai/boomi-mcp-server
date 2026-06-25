"""Unit tests for ProcessFlowBuilder (issue #25 / M2.5).

Asserts attribute-level structure of emitted process Component XML using
ElementTree (matches the style of test_database_get_operation_builder.py
and test_rest_client_operation_builder.py). Also exercises every
structured-error path in validate_config.

Structural reference: live Renera process XML (DB Test, Rest Test GET,
Rest Test PATCH) captured transiently during issue #25 implementation —
no live XML is committed as a fixture.
"""

from __future__ import annotations

import sys
from pathlib import Path

import pytest
import xml.etree.ElementTree as ET

_project_root = str(Path(__file__).resolve().parent.parent)
if _project_root not in sys.path:
    sys.path.insert(0, _project_root)

from src.boomi_mcp.categories.components.builders import (
    BuilderValidationError,
    PROCESS_FLOW_BUILDERS,
    ProcessFlowBuilder,
    REST_CLIENT_SUBTYPE,
    get_process_flow_builder,
)


NS = {"bns": "http://api.platform.boomi.com/"}

_DB_CONN_ID = "11111111-1111-1111-1111-111111111111"
_DB_OP_ID = "22222222-2222-2222-2222-222222222222"
_REST_CONN_ID = "33333333-3333-3333-3333-333333333333"
_REST_OP_ID = "44444444-4444-4444-4444-444444444444"


def _base_config(**overrides):
    cfg = {
        "process_kind": "database_to_api_sync",
        "source": {
            "connector_type": "database",
            "connection_id": _DB_CONN_ID,
            "operation_id": _DB_OP_ID,
            "action_type": "Get",
        },
        "transform": {"mode": "passthrough"},
        "target": {
            "connector_type": "rest",
            "connection_id": _REST_CONN_ID,
            "operation_id": _REST_OP_ID,
            "action_type": "POST",
        },
    }
    cfg.update(overrides)
    return cfg


def _parse_process(xml: str):
    """Parse Component XML and return (component, process, shapes_list)."""
    root = ET.fromstring(xml)
    process = root.find("bns:object/process", NS)
    assert process is not None, "Component XML is missing <bns:object>/<process>"
    shapes = process.find("shapes")
    assert shapes is not None
    return root, process, list(shapes.findall("shape"))


# ---------------------------------------------------------------------------
# Registry exposure
# ---------------------------------------------------------------------------

def test_registry_exposes_database_to_api_sync():
    assert "database_to_api_sync" in PROCESS_FLOW_BUILDERS
    assert get_process_flow_builder("database_to_api_sync") is ProcessFlowBuilder
    assert get_process_flow_builder("unknown_kind") is None
    assert get_process_flow_builder(None) is None


# ---------------------------------------------------------------------------
# build() — Component XML envelope
# ---------------------------------------------------------------------------

def test_build_emits_component_envelope():
    xml = ProcessFlowBuilder.build(_base_config(), name="My Process")
    root, process, shapes = _parse_process(xml)
    assert root.attrib["type"] == "process"
    assert root.attrib["name"] == "My Process"
    # Process envelope must carry the static workflow defaults observed
    # in the live Renera examples (DB Test, Rest Test GET).
    assert process.attrib["allowSimultaneous"] == "false"
    assert process.attrib["enableUserLog"] == "false"
    assert process.attrib["workload"] == "general"
    assert process.attrib["stopProcessingIfZeroDocuments"] == "true"
    assert process.attrib["updateRunDates"] == "true"
    # processOverrides must exist (Boomi components carry it even when empty)
    assert root.find("bns:processOverrides", NS) is not None


def test_build_folder_full_path_attribute():
    # Codex review r8 F2: folderName is the writable folder attribute
    # (folderFullPath is response-only metadata Boomi ignores on writes).
    xml = ProcessFlowBuilder.build(
        _base_config(),
        name="My Process",
        folder_name="Some/Folder",
    )
    root = ET.fromstring(xml)
    assert root.attrib.get("folderName") == "Some/Folder"
    assert "folderFullPath" not in root.attrib


def test_build_omits_folder_when_not_supplied():
    xml = ProcessFlowBuilder.build(_base_config(), name="My Process")
    root = ET.fromstring(xml)
    assert "folderName" not in root.attrib
    assert "folderFullPath" not in root.attrib


# ---------------------------------------------------------------------------
# Shape graph — passthrough (minimal database -> REST)
# ---------------------------------------------------------------------------

def test_passthrough_shape_graph_is_start_source_target_stop():
    xml = ProcessFlowBuilder.build(_base_config(), name="My Process")
    _, _, shapes = _parse_process(xml)
    # Start, source connectoraction, target connectoraction, stop
    assert len(shapes) == 4
    assert [s.attrib["name"] for s in shapes] == ["shape1", "shape2", "shape3", "shape4"]
    assert [s.attrib["shapetype"] for s in shapes] == [
        "start", "connectoraction", "connectoraction", "stop",
    ]


def test_start_shape_uses_noaction_configuration():
    xml = ProcessFlowBuilder.build(_base_config(), name="My Process")
    _, _, shapes = _parse_process(xml)
    start = shapes[0]
    assert start.find("configuration/noaction") is not None


def test_source_connectoraction_carries_database_binding():
    xml = ProcessFlowBuilder.build(_base_config(), name="My Process")
    _, _, shapes = _parse_process(xml)
    source = shapes[1]
    ca = source.find("configuration/connectoraction")
    assert ca is not None
    assert ca.attrib["connectorType"] == "database"
    assert ca.attrib["actionType"] == "Get"
    assert ca.attrib["connectionId"] == _DB_CONN_ID
    assert ca.attrib["operationId"] == _DB_OP_ID
    assert ca.attrib["allowDynamicCredentials"] == "NONE"
    assert ca.attrib["hideSettings"] == "false"
    # Both <parameters/> and <dynamicProperties/> must be present as
    # empty children — matches the live XML and the connectoraction XSD.
    assert ca.find("parameters") is not None
    assert ca.find("dynamicProperties") is not None


def test_target_connectoraction_normalizes_rest_aliases():
    xml = ProcessFlowBuilder.build(_base_config(), name="My Process")
    _, _, shapes = _parse_process(xml)
    target = shapes[2]
    ca = target.find("configuration/connectoraction")
    assert ca is not None
    assert ca.attrib["connectorType"] == REST_CLIENT_SUBTYPE
    assert ca.attrib["actionType"] == "POST"
    assert ca.attrib["connectionId"] == _REST_CONN_ID
    assert ca.attrib["operationId"] == _REST_OP_ID


def test_target_accepts_canonical_rest_subtype_directly():
    cfg = _base_config()
    cfg["target"]["connector_type"] = REST_CLIENT_SUBTYPE
    xml = ProcessFlowBuilder.build(cfg, name="My Process")
    _, _, shapes = _parse_process(xml)
    ca = shapes[2].find("configuration/connectoraction")
    assert ca.attrib["connectorType"] == REST_CLIENT_SUBTYPE


def test_target_action_type_is_uppercased_in_xml():
    """Codex review C3: lowercase 'post' passes validation (which uppercases
    for membership check) but build() must emit uppercase actionType."""
    cfg = _base_config()
    cfg["target"]["action_type"] = "post"
    xml = ProcessFlowBuilder.build(cfg, name="My Process")
    _, _, shapes = _parse_process(xml)
    ca = shapes[2].find("configuration/connectoraction")
    assert ca.attrib["actionType"] == "POST"


def test_target_action_type_uppercase_with_leading_whitespace():
    cfg = _base_config()
    cfg["target"]["action_type"] = "  patch  "
    xml = ProcessFlowBuilder.build(cfg, name="My Process")
    _, _, shapes = _parse_process(xml)
    ca = shapes[2].find("configuration/connectoraction")
    assert ca.attrib["actionType"] == "PATCH"


# Codex review r6 P2.2 — emitted XML must canonicalize binding values
# even when the validator accepted case-insensitive / whitespace-padded
# input. Boomi's connector resolution is case-sensitive on the source
# side and treats id whitespace as literal characters.

def test_source_connector_type_capitalized_is_lowercased_in_xml():
    cfg = _base_config()
    cfg["source"]["connector_type"] = "Database"  # capitalized
    xml = ProcessFlowBuilder.build(cfg, name="N")
    _, _, shapes = _parse_process(xml)
    ca = shapes[1].find("configuration/connectoraction")
    assert ca.attrib["connectorType"] == "database"


def test_source_action_type_whitespace_stripped_in_xml():
    cfg = _base_config()
    cfg["source"]["action_type"] = "  Get  "
    xml = ProcessFlowBuilder.build(cfg, name="N")
    _, _, shapes = _parse_process(xml)
    ca = shapes[1].find("configuration/connectoraction")
    assert ca.attrib["actionType"] == "Get"


def test_source_ids_whitespace_stripped_in_xml():
    cfg = _base_config()
    cfg["source"]["connection_id"] = "  C1  "
    cfg["source"]["operation_id"] = "  O1  "
    xml = ProcessFlowBuilder.build(cfg, name="N")
    _, _, shapes = _parse_process(xml)
    ca = shapes[1].find("configuration/connectoraction")
    assert ca.attrib["connectionId"] == "C1"
    assert ca.attrib["operationId"] == "O1"


def test_target_ids_whitespace_stripped_in_xml():
    cfg = _base_config()
    cfg["target"]["connection_id"] = "  C2  "
    cfg["target"]["operation_id"] = "  O2  "
    xml = ProcessFlowBuilder.build(cfg, name="N")
    _, _, shapes = _parse_process(xml)
    ca = shapes[2].find("configuration/connectoraction")
    assert ca.attrib["connectionId"] == "C2"
    assert ca.attrib["operationId"] == "O2"


def test_map_id_literal_whitespace_stripped_in_xml():
    """Codex review r8 F3: validate_config accepts a padded literal
    map_ref/map_id as long as ref.strip() is non-empty, but the
    unstripped value used to flow into the emitted mapId attribute,
    breaking map shape resolution at apply. Strip at emission to mirror
    the r6.2 fix for connection_id/operation_id."""
    cfg = _base_config(transform={"mode": "map_ref", "map_ref": "  MAP-UUID-9999  "})
    xml = ProcessFlowBuilder.build(cfg, name="N")
    _, _, shapes = _parse_process(xml)
    assert shapes[2].attrib["shapetype"] == "map"
    assert shapes[2].find("configuration/map").attrib["mapId"] == "MAP-UUID-9999"


def test_stop_shape_has_continue_true_default():
    xml = ProcessFlowBuilder.build(_base_config(), name="My Process")
    _, _, shapes = _parse_process(xml)
    stop = shapes[-1]
    assert stop.attrib["shapetype"] == "stop"
    assert stop.find("configuration/stop").attrib["continue"] == "true"
    assert list(stop.find("dragpoints")) == []


# ---------------------------------------------------------------------------
# Dragpoint wiring (deterministic; every toShape exists)
# ---------------------------------------------------------------------------

def test_dragpoints_chain_each_shape_to_its_successor():
    xml = ProcessFlowBuilder.build(_base_config(), name="My Process")
    _, _, shapes = _parse_process(xml)
    names = [s.attrib["name"] for s in shapes]
    for i, shape in enumerate(shapes[:-1]):
        dragpoints = list(shape.find("dragpoints"))
        assert len(dragpoints) == 1, f"shape{i+1} should have one outgoing edge"
        dp = dragpoints[0]
        assert dp.attrib["toShape"] == names[i + 1]
        assert dp.attrib["name"] == f"shape{i+1}.dragpoint1"


def test_every_dragpoint_target_resolves_to_a_real_shape():
    xml = ProcessFlowBuilder.build(_base_config(), name="My Process")
    _, _, shapes = _parse_process(xml)
    shape_names = {s.attrib["name"] for s in shapes}
    for shape in shapes:
        for dp in shape.find("dragpoints"):
            assert dp.attrib["toShape"] in shape_names


# ---------------------------------------------------------------------------
# Transform variants (matches live Rest Test PATCH structure)
# ---------------------------------------------------------------------------

def test_message_transform_inserts_message_shape_between_source_and_target():
    # Issue #102 C3: the caller hands RAW JSON; the emitter owns MessageFormat
    # quoting and wraps it in single quotes so its braces are not read as {N}.
    cfg = _base_config(transform={
        "mode": "message",
        "message_text": "{\"status\":\"CLSD\"}",
    })
    xml = ProcessFlowBuilder.build(cfg, name="With Message")
    _, _, shapes = _parse_process(xml)
    assert [s.attrib["shapetype"] for s in shapes] == [
        "start", "connectoraction", "message", "connectoraction", "stop",
    ]
    msg = shapes[2].find("configuration/message")
    assert msg.attrib["combined"] == "false"
    assert msg.find("msgTxt").text == "'{\"status\":\"CLSD\"}'"
    assert msg.find("msgParameters") is not None


def test_message_text_is_xml_escaped():
    cfg = _base_config(transform={
        "mode": "message",
        "message_text": "<x a=\"&b\">",
    })
    xml = ProcessFlowBuilder.build(cfg, name="N")
    _, _, shapes = _parse_process(xml)
    # ElementTree decodes the escaped text back to its raw form, which
    # proves the encoder produced well-formed XML. Non-JSON text with no
    # apostrophes is unchanged by the MessageFormat escaper.
    assert shapes[2].find("configuration/message/msgTxt").text == "<x a=\"&b\">"


def test_message_text_apostrophe_doubled_by_emitter():
    # Issue #102 C3: a literal apostrophe in plain message text is doubled so it
    # renders (Boomi strips a lone single quote) and does not escape the rest of
    # the message. Not JSON -> not wrapped.
    cfg = _base_config(transform={"mode": "message", "message_text": "today's date {1}"})
    xml = ProcessFlowBuilder.build(cfg, name="N")
    _, _, shapes = _parse_process(xml)
    assert shapes[2].find("configuration/message/msgTxt").text == "today''s date {1}"


def test_message_text_json_with_apostrophe_wrapped_and_doubled():
    # Issue #102 C3: raw JSON is wrapped in single quotes AND its internal
    # apostrophe is doubled (so the wrap is not broken by the inner quote).
    cfg = _base_config(transform={"mode": "message", "message_text": "{\"q\":\"it's\"}"})
    xml = ProcessFlowBuilder.build(cfg, name="N")
    _, _, shapes = _parse_process(xml)
    assert shapes[2].find("configuration/message/msgTxt").text == "'{\"q\":\"it''s\"}'"


def test_map_ref_transform_inserts_map_shape_with_map_id():
    cfg = _base_config(transform={"mode": "map_ref", "map_ref": "map-uuid-9999"})
    xml = ProcessFlowBuilder.build(cfg, name="With Map")
    _, _, shapes = _parse_process(xml)
    assert [s.attrib["shapetype"] for s in shapes] == [
        "start", "connectoraction", "map", "connectoraction", "stop",
    ]
    assert shapes[2].find("configuration/map").attrib["mapId"] == "map-uuid-9999"


# ---------------------------------------------------------------------------
# Data Process transform (issue #106 M10.2)
# ---------------------------------------------------------------------------

# The canonical dataContext loop (companion data_process_groovy_step.md). The
# bare ``<`` exercises the emitter's XML escaping (-> ``&lt;``).
_DATAPROCESS_GROOVY_SCRIPT = (
    "import java.util.Properties;\n"
    "import java.io.InputStream;\n"
    "\n"
    "for( int i = 0; i < dataContext.getDataCount(); i++ ) {\n"
    "    InputStream is = dataContext.getStream(i);\n"
    "    Properties props = dataContext.getProperties(i);\n"
    "    dataContext.storeStream(is, props);\n"
    "}"
)

_DATAPROCESS_GOLDEN = (
    Path(__file__).resolve().parent
    / "fixtures"
    / "golden_xml"
    / "dataprocess_groovy_transform.xml"
)


def _dataprocess_config(steps=None, label="Tag documents", **overrides):
    transform = {"mode": "dataprocess", "label": label}
    transform["steps"] = (
        steps
        if steps is not None
        else [{"operation": "custom_scripting", "script": _DATAPROCESS_GROOVY_SCRIPT}]
    )
    return _base_config(transform=transform, **overrides)


def test_dataprocess_groovy_transform_inserts_shape_between_source_and_target():
    xml = ProcessFlowBuilder.build(_dataprocess_config(), name="DataProcess Groovy Sync")
    _, _, shapes = _parse_process(xml)
    assert [s.attrib["shapetype"] for s in shapes] == [
        "start", "connectoraction", "dataprocess", "connectoraction", "stop",
    ]
    dp = shapes[2]
    assert dp.attrib["image"] == "dataprocess_icon"
    assert dp.attrib["userlabel"] == "Tag documents"
    step = dp.find("configuration/dataprocess/step")
    assert step.attrib["index"] == "1"
    assert step.attrib["key"] == "1"
    assert step.attrib["name"] == "Custom Scripting"
    assert step.attrib["processtype"] == "12"
    script_el = step.find("dataprocessscript")
    assert script_el.attrib["language"] == "groovy2"
    assert script_el.attrib["useCache"] == "true"
    # The bare '<' was emitted entity-escaped and round-trips back through parse.
    assert "i < dataContext.getDataCount()" in script_el.find("script").text


def test_dataprocess_groovy_transform_matches_golden_fixture():
    """Byte-exact golden (issue #106 g): raw-string equality, not canonicalized —
    the <script> body escaping (&lt;), literal newlines, and no-CDATA form are
    load-bearing and must match the live capture byte-for-byte."""
    emitted = ProcessFlowBuilder.build(_dataprocess_config(), name="DataProcess Groovy Sync")
    assert emitted == _DATAPROCESS_GOLDEN.read_text()


def test_dataprocess_multi_step_chain_emits_sequential_index_and_key():
    steps = [
        {"operation": "custom_scripting", "script": "dataContext.storeStream(is, props); // a"},
        {"operation": "custom_scripting", "script": "dataContext.storeStream(is, props); // b"},
    ]
    xml = ProcessFlowBuilder.build(_dataprocess_config(steps=steps), name="Multi")
    _, _, shapes = _parse_process(xml)
    dp = next(s for s in shapes if s.attrib["shapetype"] == "dataprocess")
    step_elems = dp.findall("configuration/dataprocess/step")
    assert [s.attrib["index"] for s in step_elems] == ["1", "2"]
    assert [s.attrib["key"] for s in step_elems] == ["1", "2"]


def test_dataprocess_script_is_xml_escaped():
    # Angle brackets / ampersands in the script body must be emitted well-formed
    # and round-trip back to their raw text through the parser.
    raw = "if (a < b && c > d) { dataContext.storeStream(is, props); }"
    xml = ProcessFlowBuilder.build(
        _dataprocess_config(steps=[{"operation": "custom_scripting", "script": raw}]),
        name="N",
    )
    _, _, shapes = _parse_process(xml)
    dp = next(s for s in shapes if s.attrib["shapetype"] == "dataprocess")
    assert dp.find("configuration/dataprocess/step/dataprocessscript/script").text == raw


def test_dataprocess_rejects_missing_script():
    cfg = _dataprocess_config(steps=[{"operation": "custom_scripting"}])
    err = ProcessFlowBuilder.validate_config(cfg, depends_on=[])
    assert err is not None
    assert err.error_code == "PROCESS_DATAPROCESS_CONFIG_INVALID"
    assert err.field == "transform.steps[0].script"


def test_dataprocess_rejects_empty_steps():
    cfg = _dataprocess_config(steps=[])
    err = ProcessFlowBuilder.validate_config(cfg, depends_on=[])
    assert err is not None
    assert err.error_code == "PROCESS_DATAPROCESS_CONFIG_INVALID"
    assert err.field == "transform.steps"


def test_dataprocess_rejects_unsupported_operation():
    cfg = _dataprocess_config(
        steps=[{"operation": "search_replace", "text_to_find": "x"}]
    )
    err = ProcessFlowBuilder.validate_config(cfg, depends_on=[])
    assert err is not None
    assert err.error_code == "PROCESS_DATAPROCESS_OPERATION_UNSUPPORTED"
    assert err.field == "transform.steps[0].operation"


def test_dataprocess_rejects_character_encoding_until_xml_is_verified():
    cfg = _dataprocess_config(steps=[{"operation": "character_encoding"}])
    err = ProcessFlowBuilder.validate_config(cfg, depends_on=[])
    assert err is not None
    assert err.error_code == "PROCESS_DATAPROCESS_OPERATION_UNSUPPORTED"


def test_dataprocess_rejects_non_groovy2_language():
    cfg = _dataprocess_config(
        steps=[{"operation": "custom_scripting", "script": "x", "language": "groovy"}]
    )
    err = ProcessFlowBuilder.validate_config(cfg, depends_on=[])
    assert err is not None
    assert err.error_code == "PROCESS_DATAPROCESS_CONFIG_INVALID"
    assert err.field == "transform.steps[0].language"


def test_dataprocess_rejects_use_cache_false():
    cfg = _dataprocess_config(
        steps=[{"operation": "custom_scripting", "script": "x", "use_cache": False}]
    )
    err = ProcessFlowBuilder.validate_config(cfg, depends_on=[])
    assert err is not None
    assert err.error_code == "PROCESS_DATAPROCESS_CONFIG_INVALID"
    assert err.field == "transform.steps[0].use_cache"


def test_dataprocess_build_bypass_empty_steps_raises():
    # build() stays total on the validate_config-bypass path: empty steps must
    # raise rather than emit a semantically broken <dataprocess/> with no <step>.
    cfg = _base_config(transform={"mode": "dataprocess", "label": "x", "steps": []})
    with pytest.raises(BuilderValidationError) as exc:
        ProcessFlowBuilder.build(cfg, name="N")
    assert exc.value.error_code == "PROCESS_DATAPROCESS_CONFIG_INVALID"
    assert exc.value.field == "transform.steps"


def test_dataprocess_rejects_unknown_step_key():
    cfg = _dataprocess_config(
        steps=[{"operation": "custom_scripting", "script": "x", "bogus": 1}]
    )
    err = ProcessFlowBuilder.validate_config(cfg, depends_on=[])
    assert err is not None
    assert err.error_code == "PROCESS_DATAPROCESS_CONFIG_INVALID"


def test_name_attribute_is_xml_escaped():
    xml = ProcessFlowBuilder.build(_base_config(), name='Quote " & angle <name>')
    root = ET.fromstring(xml)
    assert root.attrib["name"] == 'Quote " & angle <name>'


# ---------------------------------------------------------------------------
# validate_config — structured-error paths
# ---------------------------------------------------------------------------

class TestValidateConfig:
    def test_passes_on_minimal_valid_config(self):
        err = ProcessFlowBuilder.validate_config(_base_config(), depends_on=[])
        assert err is None

    def test_rejects_unknown_process_kind(self):
        cfg = _base_config(process_kind="something_else")
        err = ProcessFlowBuilder.validate_config(cfg, depends_on=[])
        assert err is not None
        assert err.error_code == "PROCESS_KIND_UNSUPPORTED"
        assert err.field == "process_kind"

    def test_rejects_missing_source_connection_id(self):
        cfg = _base_config()
        del cfg["source"]["connection_id"]
        err = ProcessFlowBuilder.validate_config(cfg, depends_on=[])
        assert err.error_code == "PROCESS_CONNECTOR_BINDING_INVALID"
        assert err.field == "source.connection_id"

    def test_rejects_non_database_source_connector_type(self):
        cfg = _base_config()
        cfg["source"]["connector_type"] = "rest"
        err = ProcessFlowBuilder.validate_config(cfg, depends_on=[])
        assert err.error_code == "PROCESS_CONNECTOR_BINDING_INVALID"
        assert err.field == "source.connector_type"

    def test_rejects_non_get_source_action_type(self):
        cfg = _base_config()
        cfg["source"]["action_type"] = "Send"
        err = ProcessFlowBuilder.validate_config(cfg, depends_on=[])
        assert err.error_code == "PROCESS_CONNECTOR_BINDING_INVALID"
        assert err.field == "source.action_type"

    def test_rejects_non_rest_target_connector_type(self):
        cfg = _base_config()
        cfg["target"]["connector_type"] = "database"
        err = ProcessFlowBuilder.validate_config(cfg, depends_on=[])
        assert err.error_code == "PROCESS_CONNECTOR_BINDING_INVALID"
        assert err.field == "target.connector_type"

    def test_rejects_invalid_rest_http_method(self):
        cfg = _base_config()
        cfg["target"]["action_type"] = "FETCH"
        err = ProcessFlowBuilder.validate_config(cfg, depends_on=[])
        assert err.error_code == "PROCESS_CONNECTOR_BINDING_INVALID"
        assert err.field == "target.action_type"

    def test_accepts_all_standard_http_methods(self):
        for method in ("GET", "POST", "PUT", "PATCH", "DELETE",
                       "HEAD", "OPTIONS", "TRACE"):
            cfg = _base_config()
            cfg["target"]["action_type"] = method
            assert ProcessFlowBuilder.validate_config(cfg, depends_on=[]) is None, method

    def test_rejects_missing_target_operation_id(self):
        cfg = _base_config()
        del cfg["target"]["operation_id"]
        err = ProcessFlowBuilder.validate_config(cfg, depends_on=[])
        assert err.error_code == "PROCESS_CONNECTOR_BINDING_INVALID"
        assert err.field == "target.operation_id"

    def test_rejects_unsupported_transform_mode(self):
        cfg = _base_config(transform={"mode": "groovy_script"})
        err = ProcessFlowBuilder.validate_config(cfg, depends_on=[])
        assert err.error_code == "PROCESS_SHAPE_UNSUPPORTED"
        assert err.field == "transform.mode"

    def test_rejects_message_mode_without_text(self):
        cfg = _base_config(transform={"mode": "message"})
        err = ProcessFlowBuilder.validate_config(cfg, depends_on=[])
        assert err.error_code == "PROCESS_SHAPE_UNSUPPORTED"
        assert err.field == "transform.message_text"

    def test_rejects_map_ref_mode_without_ref(self):
        cfg = _base_config(transform={"mode": "map_ref"})
        err = ProcessFlowBuilder.validate_config(cfg, depends_on=[])
        assert err.error_code == "PROCESS_SHAPE_UNSUPPORTED"
        assert err.field == "transform.map_ref"

    def test_rejects_retry_count_positive_without_dlq(self):
        # Issue #88: positive retry needs a wired Try/Catch catch path (DLQ).
        cfg = _base_config(reliability={"retry_count": 1})
        err = ProcessFlowBuilder.validate_config(cfg, depends_on=[])
        assert err.error_code == "PROCESS_RETRY_UNVERIFIED"
        assert err.field == "reliability.retry_count"

    def test_accepts_retry_count_positive_with_dlq(self):
        # Issue #88: retry_count 1..5 with a wired DLQ catch path is un-gated.
        for rc in (1, 5):
            cfg = _base_config(reliability={
                "retry_count": rc,
                "dlq": {
                    "mode": "document_cache_ref",
                    "document_cache_id": "11111111-1111-1111-1111-111111111111",
                },
            })
            assert ProcessFlowBuilder.validate_config(cfg, depends_on=[]) is None, rc

    def test_rejects_retry_count_out_of_range(self):
        for rc in (99, -1, 6):
            cfg = _base_config(reliability={"retry_count": rc})
            err = ProcessFlowBuilder.validate_config(cfg, depends_on=[])
            assert err.error_code == "PROCESS_RETRY_UNVERIFIED", rc

    def test_rejects_catch_notify_without_wired_dlq(self):
        # Issue #89: Notify lives on a wired catch leg — without a DLQ there is
        # no catch path to host it.
        cfg = _base_config(reliability={
            "retry_count": 0,
            "catch_notify": {
                "level": "ERROR",
                "message_template": "failed: meta.base.catcherrorsmessage",
            },
        })
        err = ProcessFlowBuilder.validate_config(cfg, depends_on=[])
        assert err.error_code == "PROCESS_NOTIFY_CONFIG_INVALID"
        assert err.field == "reliability.catch_notify"

    def test_accepts_catch_notify_with_wired_dlq(self):
        cfg = _base_config(reliability={
            "retry_count": 0,
            "dlq": {
                "mode": "document_cache_ref",
                "document_cache_id": "11111111-1111-1111-1111-111111111111",
            },
            "catch_notify": {
                "level": "ERROR",
                "message_template": "failed: meta.base.catcherrorsmessage",
            },
        })
        assert ProcessFlowBuilder.validate_config(cfg, depends_on=[]) is None

    def test_rejects_catch_notify_missing_caught_error_token(self):
        cfg = _base_config(reliability={
            "retry_count": 0,
            "dlq": {
                "mode": "document_cache_ref",
                "document_cache_id": "11111111-1111-1111-1111-111111111111",
            },
            "catch_notify": {"level": "ERROR", "message_template": "no token here"},
        })
        err = ProcessFlowBuilder.validate_config(cfg, depends_on=[])
        assert err.error_code == "PROCESS_NOTIFY_CONFIG_INVALID"
        assert err.field == "reliability.catch_notify.message_template"

    def test_rejects_retry_count_wrong_type(self):
        # str and bool both rejected (bool is an int subclass — guarded).
        for rc in ("1", True):
            cfg = _base_config(reliability={"retry_count": rc})
            err = ProcessFlowBuilder.validate_config(cfg, depends_on=[])
            assert err.error_code == "PROCESS_RETRY_UNVERIFIED", rc

    def test_dlq_document_cache_mode_without_binding_is_invalid(self):
        # Issue #51 M3.R1a: document_cache_ref with retry_count == 0 is now
        # un-gated (no longer PROCESS_RETRY_UNVERIFIED), but a missing cache
        # binding is rejected as PROCESS_DLQ_BINDING_INVALID — the catch leg
        # needs a real target. (Full Try/Catch coverage lives in
        # test_process_flow_builder_trycatch_dlq.py.)
        cfg = _base_config(reliability={"retry_count": 0, "dlq": {"mode": "document_cache_ref"}})
        err = ProcessFlowBuilder.validate_config(cfg, depends_on=[])
        assert err.error_code == "PROCESS_DLQ_BINDING_INVALID"
        assert err.field == "reliability.dlq.document_cache_id"

    def test_accepts_dlq_disabled_mode(self):
        cfg = _base_config(reliability={"retry_count": 0, "dlq": {"mode": "disabled"}})
        assert ProcessFlowBuilder.validate_config(cfg, depends_on=[]) is None

    def test_rejects_unknown_dlq_mode(self):
        # Codex A3: unknown dlq mode is a caller typo, not a deferred-feature
        # gate, so distinguish it as PROCESS_DLQ_BINDING_INVALID.
        cfg = _base_config(reliability={"retry_count": 0, "dlq": {"mode": "unicorn"}})
        err = ProcessFlowBuilder.validate_config(cfg, depends_on=[])
        assert err.error_code == "PROCESS_DLQ_BINDING_INVALID"
        assert err.field == "reliability.dlq.mode"

    def test_rejects_dlq_non_dict(self):
        # Codex A3: shape error → caller mistake → PROCESS_DLQ_BINDING_INVALID.
        cfg = _base_config(reliability={"retry_count": 0, "dlq": "disabled"})
        err = ProcessFlowBuilder.validate_config(cfg, depends_on=[])
        assert err.error_code == "PROCESS_DLQ_BINDING_INVALID"
        assert err.field == "reliability.dlq"

    def test_rejects_non_string_process_kind(self):
        # Codex L1: numeric process_kind used to crash with AttributeError;
        # now coerces to "123" and falls out as a clean structured error.
        cfg = _base_config(process_kind=123)
        err = ProcessFlowBuilder.validate_config(cfg, depends_on=[])
        assert err is not None
        assert err.error_code == "PROCESS_KIND_UNSUPPORTED"

    def test_rejects_non_string_transform_mode(self):
        cfg = _base_config(transform={"mode": 1})
        err = ProcessFlowBuilder.validate_config(cfg, depends_on=[])
        assert err is not None
        assert err.error_code == "PROCESS_SHAPE_UNSUPPORTED"

    def test_rejects_non_string_dlq_mode(self):
        cfg = _base_config(reliability={"retry_count": 0, "dlq": {"mode": 1}})
        err = ProcessFlowBuilder.validate_config(cfg, depends_on=[])
        assert err is not None
        assert err.error_code == "PROCESS_DLQ_BINDING_INVALID"

    def test_rejects_undeclared_ref_in_source(self):
        cfg = _base_config()
        cfg["source"]["connection_id"] = "$ref:undeclared_key"
        err = ProcessFlowBuilder.validate_config(cfg, depends_on=["db_connection"])
        assert err.error_code == "MISSING_PROCESS_DEPENDENCY"
        assert err.field == "depends_on"

    def test_accepts_ref_when_declared_in_depends_on(self):
        cfg = _base_config()
        cfg["source"]["connection_id"] = "$ref:db_connection"
        cfg["source"]["operation_id"] = "$ref:db_query_operation"
        cfg["target"]["connection_id"] = "$ref:target_rest_connection"
        cfg["target"]["operation_id"] = "$ref:target_rest_operation"
        depends = [
            "db_connection", "db_query_operation",
            "target_rest_connection", "target_rest_operation",
        ]
        assert ProcessFlowBuilder.validate_config(cfg, depends_on=depends) is None

    def test_rejects_undeclared_ref_in_transform_map(self):
        cfg = _base_config(transform={"mode": "map_ref", "map_ref": "$ref:my_map"})
        err = ProcessFlowBuilder.validate_config(cfg, depends_on=[])
        assert err.error_code == "MISSING_PROCESS_DEPENDENCY"

    # Codex review r7 P2.2: padded $ref tokens bypass
    # _resolve_dependency_tokens (which requires startswith at byte 0),
    # then build()'s whitespace stripping emits the unresolved token as
    # if it were a UUID. Reject the malformed shape at plan-time.
    def test_rejects_leading_whitespace_in_ref_source(self):
        cfg = _base_config()
        cfg["source"]["connection_id"] = " $ref:db_connection"
        err = ProcessFlowBuilder.validate_config(
            cfg, depends_on=["db_connection"],
        )
        assert err is not None
        assert err.error_code == "MISSING_PROCESS_DEPENDENCY"
        assert "source.connection_id" in (err.field or "")

    def test_rejects_trailing_whitespace_in_ref_source(self):
        cfg = _base_config()
        cfg["source"]["operation_id"] = "$ref:db_query_operation "
        err = ProcessFlowBuilder.validate_config(
            cfg, depends_on=["db_query_operation"],
        )
        assert err is not None
        assert err.error_code == "MISSING_PROCESS_DEPENDENCY"

    def test_rejects_padded_ref_in_target(self):
        cfg = _base_config()
        cfg["target"]["connection_id"] = " $ref:target_rest_connection "
        err = ProcessFlowBuilder.validate_config(
            cfg, depends_on=["target_rest_connection"],
        )
        assert err is not None
        assert err.error_code == "MISSING_PROCESS_DEPENDENCY"

    def test_rejects_padded_ref_in_transform_map(self):
        cfg = _base_config(
            transform={"mode": "map_ref", "map_ref": " $ref:my_map"},
        )
        err = ProcessFlowBuilder.validate_config(
            cfg, depends_on=["my_map"],
        )
        assert err is not None
        assert err.error_code == "MISSING_PROCESS_DEPENDENCY"

    def test_accepts_unpadded_refs(self):
        """Regression guard — clean refs continue to validate."""
        cfg = _base_config()
        cfg["source"]["connection_id"] = "$ref:db_connection"
        cfg["source"]["operation_id"] = "$ref:db_query_operation"
        cfg["target"]["connection_id"] = "$ref:target_rest_connection"
        cfg["target"]["operation_id"] = "$ref:target_rest_operation"
        assert ProcessFlowBuilder.validate_config(
            cfg,
            depends_on=[
                "db_connection", "db_query_operation",
                "target_rest_connection", "target_rest_operation",
            ],
        ) is None


# ---------------------------------------------------------------------------
# scan_forbidden_secret_fields
# ---------------------------------------------------------------------------

class TestSecretScan:
    def test_passes_clean_config(self):
        assert ProcessFlowBuilder.scan_forbidden_secret_fields(_base_config()) is None

    def test_rejects_top_level_password(self):
        cfg = _base_config(password="hunter2")
        err = ProcessFlowBuilder.scan_forbidden_secret_fields(cfg)
        assert err is not None
        assert err.error_code == "PLAINTEXT_SECRET_REJECTED"

    def test_rejects_nested_secret(self):
        cfg = _base_config()
        cfg["source"]["api_key"] = "sk-live-xxxx"
        err = ProcessFlowBuilder.scan_forbidden_secret_fields(cfg)
        assert err is not None
        assert err.error_code == "PLAINTEXT_SECRET_REJECTED"
        assert "source.api_key" in (err.field or "")

    # Codex review r3 P1: cover the keys the old substring scanner missed.
    @pytest.mark.parametrize("key", [
        "token",
        "authorization",
        "bearer",
        "bearer_token",
        "credentials",
    ])
    def test_rejects_newly_covered_secret_keys_top_level(self, key):
        cfg = _base_config()
        cfg[key] = "LEAK_VALUE"
        err = ProcessFlowBuilder.scan_forbidden_secret_fields(cfg)
        assert err is not None, f"key {key!r} should be flagged"
        assert err.error_code == "PLAINTEXT_SECRET_REJECTED"
        assert err.field == key

    @pytest.mark.parametrize("key", [
        "token",
        "authorization",
        "bearer_token",
    ])
    def test_rejects_newly_covered_secret_keys_nested(self, key):
        cfg = _base_config()
        cfg["source"][key] = "LEAK_VALUE"
        err = ProcessFlowBuilder.scan_forbidden_secret_fields(cfg)
        assert err is not None
        assert err.error_code == "PLAINTEXT_SECRET_REJECTED"
        assert err.field == f"source.{key}"

    def test_credential_ref_is_not_a_secret(self):
        """credential_ref carries a URI reference (credential://...) — it's
        the pointer to the secret, not the secret itself. DB/REST builders'
        FORBIDDEN_SECRET_FIELDS exclude it too, by contract."""
        cfg = _base_config()
        cfg["source"]["credential_ref"] = "credential://example/secret/path"
        assert ProcessFlowBuilder.scan_forbidden_secret_fields(cfg) is None

    def test_redaction_replaces_value(self):
        cfg = _base_config()
        cfg["token"] = "LEAK"
        cfg["source"]["authorization"] = "Bearer abc"
        ProcessFlowBuilder.redact_forbidden_secret_fields_in_place(cfg)
        assert cfg["token"] == "[REDACTED]"
        assert cfg["source"]["authorization"] == "[REDACTED]"

    # Codex review r4 P1: substring matching must catch variant key names
    # that the r3 exact-match scanner missed. These were caught by the
    # pre-r3 substring scanner — preserve that coverage.
    @pytest.mark.parametrize("key", [
        "apiKey",
        "API_KEY",
        "db_password",
        "DB_PASSWORD",
        "customerPassword",
        "user_token",
        "AUTH_TOKEN",
        "customerSecret",
        "bearer_token",
    ])
    def test_rejects_variant_secret_key_names(self, key):
        cfg = _base_config()
        cfg["source"][key] = "LEAK_VALUE"
        err = ProcessFlowBuilder.scan_forbidden_secret_fields(cfg)
        assert err is not None, f"variant key {key!r} should be flagged"
        assert err.error_code == "PLAINTEXT_SECRET_REJECTED"
        assert err.field == f"source.{key}"

    # Codex review r4 P1: container-shape secrets — a forbidden key
    # whose value is a dict or list. The r3 scanner only rejected string
    # values, so these slipped through. The whole subtree is suspect.
    def test_rejects_dict_under_forbidden_key(self):
        cfg = _base_config()
        cfg["source"]["authorization"] = {"value": "Bearer LEAK", "scheme": "Bearer"}
        err = ProcessFlowBuilder.scan_forbidden_secret_fields(cfg)
        assert err is not None
        assert err.error_code == "PLAINTEXT_SECRET_REJECTED"
        assert err.field == "source.authorization"

    def test_rejects_list_under_forbidden_key(self):
        cfg = _base_config()
        cfg["source"]["token"] = ["LEAK_1", "LEAK_2"]
        err = ProcessFlowBuilder.scan_forbidden_secret_fields(cfg)
        assert err is not None
        assert err.error_code == "PLAINTEXT_SECRET_REJECTED"

    def test_redaction_obliterates_dict_under_forbidden_key(self):
        cfg = _base_config()
        cfg["source"]["password"] = {"plaintext": "hunter2", "note": "do not commit"}
        cfg["source"]["apiKey"] = "sk-LEAK"  # variant key
        ProcessFlowBuilder.redact_forbidden_secret_fields_in_place(cfg)
        assert cfg["source"]["password"] == "[REDACTED]"
        assert cfg["source"]["apiKey"] == "[REDACTED]"
        # Nested plaintext under the redacted subtree must be obliterated too —
        # confirm the whole value is replaced, not just inner string leaves.
        assert "hunter2" not in str(cfg)

    def test_empty_string_at_forbidden_key_is_skipped(self):
        """Empty defaults are not secrets (matches DB builder convention)."""
        cfg = _base_config()
        cfg["source"]["password"] = ""
        assert ProcessFlowBuilder.scan_forbidden_secret_fields(cfg) is None

    def test_scalar_at_forbidden_key_is_skipped(self):
        """None/bool/int at a forbidden key carries no plaintext to leak."""
        cfg = _base_config()
        cfg["source"]["password"] = None
        assert ProcessFlowBuilder.scan_forbidden_secret_fields(cfg) is None
        cfg["source"]["password"] = False
        assert ProcessFlowBuilder.scan_forbidden_secret_fields(cfg) is None


# ---------------------------------------------------------------------------
# build() rejects empty name
# ---------------------------------------------------------------------------

def test_build_rejects_empty_name():
    with pytest.raises(BuilderValidationError) as exc:
        ProcessFlowBuilder.build(_base_config(), name="")
    assert exc.value.error_code == "PROCESS_XML_VALIDATION_FAILED"


def test_build_coerces_non_string_description():
    """Codex review r2 Q4: validate_config does not type-check description,
    so a non-string value crashed _escape_xml's .replace() with
    AttributeError. build() must coerce."""
    cfg = _base_config()
    cfg["description"] = 12345
    xml = ProcessFlowBuilder.build(cfg, name="N")
    root = ET.fromstring(xml)
    assert root.find("bns:description", NS).text == "12345"


def test_build_coerces_non_string_folder_name():
    xml = ProcessFlowBuilder.build(_base_config(), name="N", folder_name=42)
    root = ET.fromstring(xml)
    assert root.attrib["folderName"] == "42"


def test_build_coerces_non_string_name():
    """Pydantic normally coerces IntegrationComponentSpec.name, but
    _execute_component's fallback `comp.name or payload.get('name') or
    comp.key` can route a raw int through if the caller bypassed the
    pydantic model. Defense-in-depth via str() coercion."""
    xml = ProcessFlowBuilder.build(_base_config(), name=12345)
    root = ET.fromstring(xml)
    assert root.attrib["name"] == "12345"


# ============================================================================
# Issue #45 — Component XML update preservation
# ============================================================================


def test_process_flow_preservation_policy_attached():
    policy = ProcessFlowBuilder.PRESERVATION_POLICY
    assert policy.component_type == "process"
    paths = {op.path for op in policy.owned_paths}
    assert paths == {"bns:object/process"}


def test_process_flow_update_preserves_process_overrides():
    """`bns:processOverrides` is populated by Boomi (UI per-environment
    override config). Builders never author it, so it MUST survive a
    structured update — only the inner `<process>` flow is owned."""
    from boomi_mcp.categories.components.component_update_preservation import (
        merge_for_update,
    )

    desired = ProcessFlowBuilder.build(_base_config(), name="renamed")
    current = ProcessFlowBuilder.build(_base_config(), name="original")
    # Inject a populated processOverrides in current (builder emits empty)
    current = current.replace(
        "<bns:processOverrides/>",
        (
            '<bns:processOverrides>'
            '<override path="//conn/@host" environmentId="env-1" value="prod.db"/>'
            '<override path="//conn/@host" environmentId="env-2" value="staging.db"/>'
            "</bns:processOverrides>"
        ),
    )

    merged = merge_for_update(
        current, desired, ProcessFlowBuilder.PRESERVATION_POLICY
    )
    root = ET.fromstring(merged)
    overrides = root.find("bns:processOverrides", NS)
    assert overrides is not None
    entries = overrides.findall("override")
    assert len(entries) == 2
    envs = {o.attrib.get("environmentId") for o in entries}
    assert envs == {"env-1", "env-2"}
    # And the process subtree was renamed via owned root attr
    assert root.attrib["name"] == "renamed"


# ---------------------------------------------------------------------------
# Issue #92 M4.5.7 — environment-extension declarations for connection fields
# ---------------------------------------------------------------------------

_FIXTURE_OVERRIDES = (
    Path(__file__).resolve().parent
    / "fixtures"
    / "process_overrides"
    / "database_connection_fields_process_overrides.xml"
)

# The four DB connection fields, in the live-verified exemplar order. The
# ConnectionOverride id deliberately matches the source binding's connection id
# so the override targets the same connection the process reads from.
_EXTENSION_FIELDS = [
    {"id": "host", "label": "Host", "xpath": "DatabaseConnectionSettings/@host"},
    {"id": "port", "label": "Port", "xpath": "DatabaseConnectionSettings/@port"},
    {"id": "username", "label": "User", "xpath": "DatabaseConnectionSettings/@username"},
    {"id": "password", "label": "Password", "xpath": "DatabaseConnectionSettings/@password"},
]


def _extension_config(**overrides):
    return _base_config(
        process_extensions={
            "connections": [
                {
                    "connection_id": _DB_CONN_ID,
                    "connector_type": "database",
                    "fields": _EXTENSION_FIELDS,
                }
            ]
        },
        **overrides,
    )


def _canon(elem_or_xml) -> str:
    """Canonicalize a processOverrides element (or XML string) for comparison.

    Registering the boomi prefix makes serialization deterministic across the
    built tree and the parsed fixture, and ET.canonicalize normalizes attribute
    ordering + whitespace so the pretty-printed fixture matches the inline build.
    """
    ET.register_namespace("bns", "http://api.platform.boomi.com/")
    if not isinstance(elem_or_xml, str):
        elem_or_xml = ET.tostring(elem_or_xml, encoding="unicode")
    # strip_text drops the fixture's pretty-print whitespace so the readable
    # fixture matches the builder's inline (no inter-tag whitespace) output.
    return ET.canonicalize(elem_or_xml, strip_text=True)


def test_build_emits_rest_id_keyed_override_without_xpath():
    """Issue #102 B1: a REST Client override keys purely by field id and emits NO
    xpath attribute — live_verified from the `Rest Example` process export."""
    cfg = _base_config(
        process_extensions={
            "connections": [
                {
                    "connection_id": _DB_CONN_ID,  # any resolvable id; REST shape under test
                    "connector_type": "rest",
                    "fields": [
                        {"id": "url", "label": "Base URL"},
                        {"id": "username", "label": "User"},
                        {"id": "password", "label": "Password"},
                    ],
                }
            ]
        }
    )
    root = ET.fromstring(ProcessFlowBuilder.build(cfg, name="REST Ext"))
    fields = root.findall("bns:processOverrides/Overrides/Connections/ConnectionOverride/field", NS)
    assert [f.attrib["id"] for f in fields] == ["url", "username", "password"]
    for f in fields:
        assert f.attrib["overrideable"] == "true"
        assert "xpath" not in f.attrib  # REST overrides carry no xpath


def test_rest_connector_aliases_accept_id_keyed_override_without_xpath():
    """Issue #102 B1 (Codex review): every accepted REST connector alias
    (rest / rest_client / the canonical subtype) is recognized as id-keyed, so a
    no-xpath override is valid — only true non-REST (DB) entries require xpath."""
    for alias in ("rest", "rest_client", "officialboomi-X3979C-rest-prod"):
        cfg = _base_config(
            process_extensions={
                "connections": [
                    {
                        "connection_id": _DB_CONN_ID,
                        "connector_type": alias,
                        "fields": [{"id": "username", "label": "User"}],
                    }
                ]
            }
        )
        root = ET.fromstring(ProcessFlowBuilder.build(cfg, name="REST alias"))
        field = root.find("bns:processOverrides/Overrides/Connections/ConnectionOverride/field", NS)
        assert field.attrib["id"] == "username"
        assert "xpath" not in field.attrib, f"alias {alias!r} should be id-keyed"


def test_id_keyed_override_without_connector_type_builds():
    """Issue #102 B1 (Codex review): a hand-authored id-keyed override (fields
    without xpath) that OMITS connector_type still builds — only an explicit
    connector_type='database' entry requires xpath."""
    cfg = _base_config(
        process_extensions={
            "connections": [
                {
                    "connection_id": _DB_CONN_ID,  # no connector_type
                    "fields": [{"id": "username", "label": "User"}],
                }
            ]
        }
    )
    root = ET.fromstring(ProcessFlowBuilder.build(cfg, name="No CT"))
    field = root.find("bns:processOverrides/Overrides/Connections/ConnectionOverride/field", NS)
    assert field.attrib["id"] == "username"
    assert "xpath" not in field.attrib


def test_build_emits_connection_field_environment_extensions():
    xml = ProcessFlowBuilder.build(_extension_config(), name="Ext Process")
    root = ET.fromstring(xml)
    overrides = root.find("bns:processOverrides", NS)
    assert overrides is not None
    inner = overrides.find("Overrides")
    assert inner is not None
    connection_overrides = inner.findall("Connections/ConnectionOverride")
    assert len(connection_overrides) == 1
    # ConnectionOverride id must equal the DB connection the source binds to.
    assert connection_overrides[0].attrib["id"] == _DB_CONN_ID
    fields = connection_overrides[0].findall("field")
    assert [f.attrib["id"] for f in fields] == ["host", "port", "username", "password"]
    for f in fields:
        assert f.attrib["overrideable"] == "true"
        assert f.attrib["xpath"].startswith("DatabaseConnectionSettings/@")
        assert f.attrib["label"]
    # Verified live sibling order under <Overrides>.
    assert [child.tag for child in list(inner)] == [
        "Connections",
        "Operations",
        "PartnerOverrides",
        "Properties",
        "Extensions",
        "CrossReferenceOverrides",
        "PGPOverrides",
        "DefinedProcessPropertyOverrides",
    ]
    ext = inner.find("Extensions")
    assert ext.find("ObjectDefinitions/unusedProfiles") is not None
    assert ext.find("DataMaps/unusedMaps") is not None


def test_build_process_overrides_matches_golden_fixture():
    xml = ProcessFlowBuilder.build(_extension_config(), name="Ext Process")
    root = ET.fromstring(xml)
    built = root.find("bns:processOverrides", NS)
    assert built is not None
    fixture_root = ET.parse(_FIXTURE_OVERRIDES).getroot()
    assert _canon(built) == _canon(fixture_root)


def test_build_without_process_extensions_emits_empty_overrides():
    # Byte-for-byte regression: no process_extensions -> empty element exactly.
    xml = ProcessFlowBuilder.build(_base_config(), name="No Ext")
    assert "<bns:processOverrides/>" in xml
    assert "<Overrides" not in xml


def test_wrapper_subprocess_emits_empty_overrides():
    from src.boomi_mcp.categories.components.builders import WrapperSubprocessBuilder

    xml = WrapperSubprocessBuilder.build(
        {
            "process_kind": "wrapper_subprocess",
            "process_calls": [{"process_id": "55555555-5555-5555-5555-555555555555"}],
        },
        name="Wrapper",
    )
    assert "<bns:processOverrides/>" in xml
    assert "<Overrides" not in xml


def test_build_update_discards_emitted_declaration_preserving_live_overrides():
    """CREATE-only: an emitted declaration must NOT clobber live per-environment
    override VALUES on a structured update (processOverrides is unowned)."""
    from boomi_mcp.categories.components.component_update_preservation import (
        merge_for_update,
    )

    # A freshly-built process now CARRIES a declaration (the desired update).
    desired = ProcessFlowBuilder.build(_extension_config(), name="renamed")
    assert "<Overrides" in desired
    # The live component has UI-populated per-environment override values.
    current = ProcessFlowBuilder.build(_base_config(), name="original").replace(
        "<bns:processOverrides/>",
        (
            '<bns:processOverrides>'
            '<override field="password" environmentId="env-1" value="prod-secret"/>'
            "</bns:processOverrides>"
        ),
    )
    merged = merge_for_update(
        current, desired, ProcessFlowBuilder.PRESERVATION_POLICY
    )
    root = ET.fromstring(merged)
    overrides = root.find("bns:processOverrides", NS)
    # The live override survives; the builder's declaration is discarded.
    assert overrides.find("override") is not None
    assert overrides.find("Overrides") is None


@pytest.mark.parametrize(
    "process_extensions",
    [
        "not-a-dict",
        # Present non-empty block whose connections key is missing/misspelled or
        # null must be rejected, not silently dropped (Codex review finding).
        {"connection": [{"connection_id": _DB_CONN_ID, "fields": _EXTENSION_FIELDS}]},
        {"operations": []},
        {"connections": None},
        {"connections": "not-a-list"},
        {"connections": ["not-a-dict"]},
        {"connections": [{"connection_id": "", "fields": _EXTENSION_FIELDS}]},
        {"connections": [{"connection_id": _DB_CONN_ID, "fields": []}]},
        {"connections": [{"connection_id": _DB_CONN_ID, "fields": ["not-a-dict"]}]},
        # xpath is REQUIRED only for an EXPLICIT database override (xpath-keyed);
        # omitting it there is rejected (#102 B1, Codex review).
        {"connections": [{"connection_id": _DB_CONN_ID, "connector_type": "database", "fields": [{"id": "x", "label": "X"}]}]},
        # A PRESENT but blank xpath is rejected for any connector type.
        {"connections": [{"connection_id": _DB_CONN_ID, "connector_type": "rest", "fields": [{"id": "x", "label": "X", "xpath": " "}]}]},
        {"connections": [{"connection_id": _DB_CONN_ID, "fields": [{"id": " ", "label": "X", "xpath": "y"}]}]},
    ],
)
def test_build_rejects_malformed_process_extensions(process_extensions):
    cfg = _base_config(process_extensions=process_extensions)
    with pytest.raises(BuilderValidationError) as excinfo:
        ProcessFlowBuilder.build(cfg, name="Bad Ext")
    assert excinfo.value.error_code == "PROCESS_EXTENSIONS_INVALID"


@pytest.mark.parametrize("process_extensions", [{}, {"connections": []}])
def test_build_empty_process_extensions_is_noop(process_extensions):
    # An absent/empty block or an explicitly empty connections list emits the
    # empty override element (no declaration), never an error.
    cfg = _base_config(process_extensions=process_extensions)
    xml = ProcessFlowBuilder.build(cfg, name="Empty Ext")
    assert "<bns:processOverrides/>" in xml
    assert "<Overrides" not in xml


def test_validate_config_surfaces_process_extensions_error():
    cfg = _base_config(
        process_extensions={"connections": [{"connection_id": _DB_CONN_ID, "fields": []}]}
    )
    err = ProcessFlowBuilder.validate_config(cfg, depends_on=set())
    assert err is not None
    assert err.error_code == "PROCESS_EXTENSIONS_INVALID"


def test_validate_config_accepts_ref_connection_id_in_depends_on():
    # A $ref connection_id in process_extensions is reachability-checked like any
    # other ref: present in depends_on -> OK; absent -> MISSING_PROCESS_DEPENDENCY.
    cfg = _base_config(
        source={
            "connector_type": "database",
            "connection_id": "$ref:db_conn",
            "operation_id": _DB_OP_ID,
            "action_type": "Get",
        },
        process_extensions={
            "connections": [
                {"connection_id": "$ref:db_conn", "fields": _EXTENSION_FIELDS}
            ]
        },
    )
    assert ProcessFlowBuilder.validate_config(cfg, depends_on={"db_conn"}) is None
    missing = ProcessFlowBuilder.validate_config(cfg, depends_on=set())
    assert missing is not None
    assert missing.error_code == "MISSING_PROCESS_DEPENDENCY"
