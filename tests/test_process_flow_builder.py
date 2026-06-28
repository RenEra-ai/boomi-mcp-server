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


def test_registry_exposes_sync_pipeline():
    # Issue #70 M5.2: the verified-linear sync_pipeline kind is registered.
    from src.boomi_mcp.categories.components.builders import SyncPipelineBuilder

    assert "sync_pipeline" in PROCESS_FLOW_BUILDERS
    assert get_process_flow_builder("sync_pipeline") is SyncPipelineBuilder


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


def test_rest_get_source_keeps_canonical_subtype_and_empty_body():
    # Issue #72 M5.4: a REST fetch source (connector_type='rest', action_type='GET')
    # emits the canonical mixed-case REST Client subtype un-lowercased (an
    # unconditional .lower() would corrupt the uppercase 'X' in X3979C) with the
    # uppercased verb and an empty request document.
    cfg = _base_config(
        source={
            "connector_type": "rest",
            "connection_id": _REST_CONN_ID,
            "operation_id": _REST_OP_ID,
            "action_type": "get",  # lowercase input -> uppercased on emit
        },
    )
    xml = ProcessFlowBuilder.build(cfg, name="API Fetch")
    _, _, shapes = _parse_process(xml)
    ca = shapes[1].find("configuration/connectoraction")
    assert ca is not None
    assert ca.attrib["connectorType"] == REST_CLIENT_SUBTYPE  # NOT lowercased
    assert ca.attrib["actionType"] == "GET"
    assert ca.attrib["connectionId"] == _REST_CONN_ID
    assert ca.attrib["operationId"] == _REST_OP_ID
    # The #72 empty-request guarantee: empty parameters + dynamicProperties, no
    # process-step dynamicProperties (runtime slot binding is #96, M5.4a).
    assert ca.find("parameters") is not None
    assert ca.find("dynamicProperties") is not None
    assert "propertyId=" not in ET.tostring(ca.find("dynamicProperties"), encoding="unicode")


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


def test_dataprocess_rejects_unknown_transform_key():
    # A typo'd top-level transform key must not be silently dropped (mirrors the
    # step-level strictness and the DataProcessPrimitive's extra="forbid").
    cfg = _dataprocess_config()
    cfg["transform"]["bogus"] = 1
    err = ProcessFlowBuilder.validate_config(cfg, depends_on=[])
    assert err is not None
    assert err.error_code == "PROCESS_DATAPROCESS_CONFIG_INVALID"
    assert err.field == "transform"


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


# ---------------------------------------------------------------------------
# Data Process Split / Combine Documents (issue #115 M10.2a)
# ---------------------------------------------------------------------------

_GOLDEN_DIR = Path(__file__).resolve().parent / "fixtures" / "golden_xml"
_DATAPROCESS_SPLIT_JSON_GOLDEN = _GOLDEN_DIR / "dataprocess_split_json_transform.xml"
_DATAPROCESS_SPLIT_XML_GOLDEN = _GOLDEN_DIR / "dataprocess_split_xml_transform.xml"
_DATAPROCESS_COMBINE_JSON_GOLDEN = _GOLDEN_DIR / "dataprocess_combine_json_transform.xml"
_DATAPROCESS_COMBINE_XML_GOLDEN = _GOLDEN_DIR / "dataprocess_combine_xml_transform.xml"

_JSON_PROFILE_ID = "55555555-5555-5555-5555-555555555555"
_XML_PROFILE_ID = "66666666-6666-6666-6666-666666666666"
_JSON_LINK_NAME = "ArrayElement1 (Root/Object/samplearray/samplearray/ArrayElement1)"
_XML_LINK_NAME = "Group (Envelope/Body/Groups/Group)"


def _split_step(profile_type="json", profile_id=None, key=None, name=None, **extra):
    step = {
        "operation": "split_documents",
        "profile_type": profile_type,
        "profile_id": profile_id
        or (_JSON_PROFILE_ID if profile_type == "json" else _XML_PROFILE_ID),
        "link_element_key": key or ("9" if profile_type == "json" else "4"),
        "link_element_name": name
        or (_JSON_LINK_NAME if profile_type == "json" else _XML_LINK_NAME),
    }
    step.update(extra)
    return step


def _combine_step(profile_type="json", profile_id=None, key=None, name=None, **extra):
    step = {
        "operation": "combine_documents",
        "profile_type": profile_type,
        "profile_id": profile_id
        or (_JSON_PROFILE_ID if profile_type == "json" else _XML_PROFILE_ID),
        "link_element_key": key or ("9" if profile_type == "json" else "4"),
        "link_element_name": name
        or (_JSON_LINK_NAME if profile_type == "json" else _XML_LINK_NAME),
    }
    step.update(extra)
    return step


def _dataprocess_split_shape(xml):
    _, _, shapes = _parse_process(xml)
    dp = next(s for s in shapes if s.attrib["shapetype"] == "dataprocess")
    return dp.find("configuration/dataprocess/step")


def test_dataprocess_split_json_inserts_shape():
    xml = ProcessFlowBuilder.build(
        _dataprocess_config(steps=[_split_step("json")], label="Split orders JSON"),
        name="DataProcess Split JSON Sync",
    )
    _, _, shapes = _parse_process(xml)
    assert [s.attrib["shapetype"] for s in shapes] == [
        "start", "connectoraction", "dataprocess", "connectoraction", "stop",
    ]
    step = _dataprocess_split_shape(xml)
    assert step.attrib["name"] == "Split Documents"
    assert step.attrib["processtype"] == "8"
    opts = step.find("documentsplit/SplitOptions/JSONOptions")
    assert opts is not None
    assert step.find("documentsplit").attrib["profileType"] == "json"
    assert opts.attrib["linkElementKey"] == "9"
    assert opts.attrib["linkElementName"] == _JSON_LINK_NAME
    assert opts.attrib["profileId"] == _JSON_PROFILE_ID


def test_dataprocess_split_xml_inserts_shape():
    xml = ProcessFlowBuilder.build(
        _dataprocess_config(steps=[_split_step("xml")], label="Split groups XML"),
        name="DataProcess Split XML Sync",
    )
    step = _dataprocess_split_shape(xml)
    assert step.attrib["processtype"] == "8"
    assert step.find("documentsplit").attrib["profileType"] == "xml"
    opts = step.find("documentsplit/SplitOptions/XMLOptions")
    assert opts is not None
    assert opts.attrib["linkElementKey"] == "4"
    assert opts.attrib["linkElementName"] == _XML_LINK_NAME
    assert opts.attrib["profileId"] == _XML_PROFILE_ID


def test_dataprocess_combine_json_inserts_shape():
    xml = ProcessFlowBuilder.build(
        _dataprocess_config(steps=[_combine_step("json")], label="Combine orders JSON"),
        name="DataProcess Combine JSON Sync",
    )
    step = _dataprocess_split_shape(xml)
    assert step.attrib["name"] == "Combine Documents"
    assert step.attrib["processtype"] == "9"
    assert step.find("dataprocesscombine").attrib["profileType"] == "json"
    opts = step.find("dataprocesscombine/JSONOptions")
    assert opts is not None
    # combineIntoLinkElementKey defaults to the literal "null" (combine into root).
    assert opts.attrib["combineIntoLinkElementKey"] == "null"
    assert opts.attrib["linkElementKey"] == "9"
    assert opts.attrib["profileId"] == _JSON_PROFILE_ID


def test_dataprocess_combine_xml_inserts_shape():
    xml = ProcessFlowBuilder.build(
        _dataprocess_config(steps=[_combine_step("xml")], label="Combine groups XML"),
        name="DataProcess Combine XML Sync",
    )
    step = _dataprocess_split_shape(xml)
    assert step.attrib["processtype"] == "9"
    assert step.find("dataprocesscombine").attrib["profileType"] == "xml"
    opts = step.find("dataprocesscombine/XMLOptions")
    assert opts is not None
    assert opts.attrib["combineIntoLinkElementKey"] == "null"
    assert opts.attrib["linkElementKey"] == "4"
    assert opts.attrib["profileId"] == _XML_PROFILE_ID


def test_dataprocess_split_json_matches_golden_fixture():
    emitted = ProcessFlowBuilder.build(
        _dataprocess_config(steps=[_split_step("json")], label="Split orders JSON"),
        name="DataProcess Split JSON Sync",
    )
    assert emitted == _DATAPROCESS_SPLIT_JSON_GOLDEN.read_text()


def test_dataprocess_split_xml_matches_golden_fixture():
    emitted = ProcessFlowBuilder.build(
        _dataprocess_config(steps=[_split_step("xml")], label="Split groups XML"),
        name="DataProcess Split XML Sync",
    )
    assert emitted == _DATAPROCESS_SPLIT_XML_GOLDEN.read_text()


def test_dataprocess_combine_json_matches_golden_fixture():
    emitted = ProcessFlowBuilder.build(
        _dataprocess_config(steps=[_combine_step("json")], label="Combine orders JSON"),
        name="DataProcess Combine JSON Sync",
    )
    assert emitted == _DATAPROCESS_COMBINE_JSON_GOLDEN.read_text()


def test_dataprocess_combine_xml_matches_golden_fixture():
    emitted = ProcessFlowBuilder.build(
        _dataprocess_config(steps=[_combine_step("xml")], label="Combine groups XML"),
        name="DataProcess Combine XML Sync",
    )
    assert emitted == _DATAPROCESS_COMBINE_XML_GOLDEN.read_text()


def test_dataprocess_mixed_chain_emits_sequential_index_and_key():
    steps = [
        {"operation": "custom_scripting", "script": "dataContext.storeStream(is, props);"},
        _split_step("json"),
        _combine_step("xml"),
    ]
    xml = ProcessFlowBuilder.build(_dataprocess_config(steps=steps), name="Mixed")
    _, _, shapes = _parse_process(xml)
    dp = next(s for s in shapes if s.attrib["shapetype"] == "dataprocess")
    step_elems = dp.findall("configuration/dataprocess/step")
    assert [s.attrib["index"] for s in step_elems] == ["1", "2", "3"]
    assert [s.attrib["key"] for s in step_elems] == ["1", "2", "3"]
    assert [s.attrib["processtype"] for s in step_elems] == ["12", "8", "9"]


def test_dataprocess_combine_custom_parent_key_is_emitted():
    step = _combine_step("json", combine_into_link_element_key="5")
    xml = ProcessFlowBuilder.build(
        _dataprocess_config(steps=[step]), name="N"
    )
    opts = _dataprocess_split_shape(xml).find("dataprocesscombine/JSONOptions")
    assert opts.attrib["combineIntoLinkElementKey"] == "5"


def test_dataprocess_split_link_name_is_xml_escaped():
    raw = "A & B <Root/x> \"q\""
    xml = ProcessFlowBuilder.build(
        _dataprocess_config(steps=[_split_step("json", name=raw)]), name="N"
    )
    opts = _dataprocess_split_shape(xml).find("documentsplit/SplitOptions/JSONOptions")
    assert opts.attrib["linkElementName"] == raw


def test_dataprocess_split_rejects_invalid_profile_type():
    cfg = _dataprocess_config(steps=[_split_step("yaml")])
    err = ProcessFlowBuilder.validate_config(cfg, depends_on=[])
    assert err is not None
    assert err.error_code == "PROCESS_DATAPROCESS_CONFIG_INVALID"
    assert err.field == "transform.steps[0].profile_type"


def test_dataprocess_split_rejects_missing_profile_id():
    step = _split_step("json")
    del step["profile_id"]
    err = ProcessFlowBuilder.validate_config(
        _dataprocess_config(steps=[step]), depends_on=[]
    )
    assert err is not None
    assert err.error_code == "PROCESS_DATAPROCESS_CONFIG_INVALID"
    assert err.field == "transform.steps[0].profile_id"


def test_dataprocess_split_rejects_blank_link_element_key():
    err = ProcessFlowBuilder.validate_config(
        _dataprocess_config(steps=[_split_step("json", key="  ")]), depends_on=[]
    )
    assert err is not None
    assert err.error_code == "PROCESS_DATAPROCESS_CONFIG_INVALID"
    assert err.field == "transform.steps[0].link_element_key"


def test_dataprocess_split_rejects_unknown_step_key():
    err = ProcessFlowBuilder.validate_config(
        _dataprocess_config(steps=[_split_step("json", bogus=1)]), depends_on=[]
    )
    assert err is not None
    assert err.error_code == "PROCESS_DATAPROCESS_CONFIG_INVALID"
    assert err.field == "transform.steps[0]"


def test_dataprocess_split_rejects_combine_only_key():
    # combine_into_link_element_key is not allowed on a split step.
    err = ProcessFlowBuilder.validate_config(
        _dataprocess_config(steps=[_split_step("json", combine_into_link_element_key="null")]),
        depends_on=[],
    )
    assert err is not None
    assert err.error_code == "PROCESS_DATAPROCESS_CONFIG_INVALID"
    assert err.field == "transform.steps[0]"


def test_dataprocess_combine_rejects_blank_combine_into_key():
    err = ProcessFlowBuilder.validate_config(
        _dataprocess_config(
            steps=[_combine_step("json", combine_into_link_element_key="  ")]
        ),
        depends_on=[],
    )
    assert err is not None
    assert err.error_code == "PROCESS_DATAPROCESS_CONFIG_INVALID"
    assert err.field == "transform.steps[0].combine_into_link_element_key"


def test_dataprocess_build_bypass_unknown_profile_type_raises():
    # build() stays total on the validate_config-bypass path: an unknown
    # profile_type that reaches the emitter raises rather than emitting a step
    # with no option element.
    bypass_step = _split_step("json")
    bypass_step["profile_type"] = "yaml"
    cfg = _base_config(
        transform={"mode": "dataprocess", "label": "x", "steps": [bypass_step]}
    )
    with pytest.raises(BuilderValidationError) as exc:
        ProcessFlowBuilder.build(cfg, name="N")
    assert exc.value.error_code == "PROCESS_DATAPROCESS_CONFIG_INVALID"


# ---------------------------------------------------------------------------
# Document Cache Retrieve transform (issue #109 M10.5)
# ---------------------------------------------------------------------------

_DOCCACHE_RETRIEVE_GOLDEN = (
    Path(__file__).resolve().parent
    / "fixtures"
    / "golden_xml"
    / "document_cache_retrieve.xml"
)

# The live-captured Document Cache component id (work component
# 64e5397b-... shape2; see .codex/plans/issue-109-live-captures.md).
_DOCCACHE_ID = "8540619c-9f1e-4832-9b1a-5128c399aa52"


def _doccacheretrieve_config(label="Get Status Updates From Cache", **overrides):
    transform = {"mode": "doccacheretrieve", "document_cache_id": _DOCCACHE_ID}
    if label is not None:
        transform["label"] = label
    transform.update(overrides.pop("transform_extra", {}))
    return _base_config(transform=transform, **overrides)


def test_doccacheretrieve_inserts_linear_shape_between_source_and_target():
    xml = ProcessFlowBuilder.build(_doccacheretrieve_config(), name="Cache Retrieve Sync")
    _, _, shapes = _parse_process(xml)
    assert [s.attrib["shapetype"] for s in shapes] == [
        "start", "connectoraction", "doccacheretrieve", "connectoraction", "stop",
    ]
    dcr = shapes[2]
    assert dcr.attrib["image"] == "doccacheretrieve_icon"
    assert dcr.attrib["userlabel"] == "Get Status Updates From Cache"
    cfg = dcr.find("configuration/doccacheretrieve")
    assert cfg.attrib["docCache"] == _DOCCACHE_ID
    assert cfg.attrib["emptyCacheBehavior"] == "stopprocess"
    assert cfg.attrib["loadAllDoc"] == "true"
    # All-document retrieve emits an empty <cacheKeyValues/> (keyed retrieval deferred).
    key_values = cfg.find("cacheKeyValues")
    assert key_values is not None and list(key_values) == []
    # Linear, non-terminal: exactly one forward dragpoint to the next shape.
    dragpoints = dcr.find("dragpoints")
    assert [dp.attrib["toShape"] for dp in dragpoints] == ["shape4"]


def test_doccacheretrieve_matches_golden_fixture():
    """Byte-exact golden (issue #109 g): the docCache/emptyCacheBehavior/loadAllDoc
    attribute order and the empty <cacheKeyValues/> child are load-bearing and must
    match the live capture byte-for-byte."""
    emitted = ProcessFlowBuilder.build(
        _doccacheretrieve_config(), name="DocumentCacheRetrieve Sync"
    )
    assert emitted == _DOCCACHE_RETRIEVE_GOLDEN.read_text()


def test_doccacheretrieve_accepts_ref_document_cache_id():
    cfg = _doccacheretrieve_config()
    cfg["transform"]["document_cache_id"] = "$ref:MyCache"
    # Reachable when declared in depends_on...
    assert ProcessFlowBuilder.validate_config(cfg, depends_on=["MyCache"]) is None
    # ...and MISSING_PROCESS_DEPENDENCY when not (generic ref-reachability walk).
    err = ProcessFlowBuilder.validate_config(cfg, depends_on=[])
    assert err is not None
    assert err.error_code == "MISSING_PROCESS_DEPENDENCY"


def test_doccacheretrieve_rejects_missing_document_cache_id():
    cfg = _base_config(transform={"mode": "doccacheretrieve", "label": "x"})
    err = ProcessFlowBuilder.validate_config(cfg, depends_on=[])
    assert err is not None
    assert err.error_code == "PROCESS_DOCCACHE_RETRIEVE_CONFIG_INVALID"
    assert err.field == "transform.document_cache_id"


def test_doccacheretrieve_rejects_blank_document_cache_id():
    cfg = _base_config(
        transform={"mode": "doccacheretrieve", "document_cache_id": "   "}
    )
    err = ProcessFlowBuilder.validate_config(cfg, depends_on=[])
    assert err is not None
    assert err.error_code == "PROCESS_DOCCACHE_RETRIEVE_CONFIG_INVALID"
    assert err.field == "transform.document_cache_id"


def test_doccacheretrieve_rejects_unsupported_empty_cache_behavior():
    # The backward-compat "fail document with errors" wire value has no live
    # capture — only the recommended 'stopprocess' is accepted in v1.
    cfg = _doccacheretrieve_config()
    cfg["transform"]["empty_cache_behavior"] = "returnerror"
    err = ProcessFlowBuilder.validate_config(cfg, depends_on=[])
    assert err is not None
    assert err.error_code == "PROCESS_DOCCACHE_RETRIEVE_CONFIG_INVALID"
    assert err.field == "transform.empty_cache_behavior"


def test_doccacheretrieve_rejects_keyed_retrieval_load_all_false():
    # Keyed/index retrieval (loadAllDoc=false) is deferred pending a live capture.
    cfg = _doccacheretrieve_config()
    cfg["transform"]["load_all_documents"] = False
    err = ProcessFlowBuilder.validate_config(cfg, depends_on=[])
    assert err is not None
    assert err.error_code == "PROCESS_DOCCACHE_RETRIEVE_CONFIG_INVALID"
    assert err.field == "transform.load_all_documents"


def test_doccacheretrieve_rejects_unknown_transform_key():
    cfg = _doccacheretrieve_config()
    cfg["transform"]["bogus"] = 1
    err = ProcessFlowBuilder.validate_config(cfg, depends_on=[])
    assert err is not None
    assert err.error_code == "PROCESS_DOCCACHE_RETRIEVE_CONFIG_INVALID"
    assert err.field == "transform"


def test_doccacheretrieve_xml_escapes_label_and_cache_id():
    cfg = _base_config(
        transform={
            "mode": "doccacheretrieve",
            "label": "A & B <retrieve>",
            "document_cache_id": "CACHE&<1>",
        }
    )
    xml = ProcessFlowBuilder.build(cfg, name="N")
    _, _, shapes = _parse_process(xml)
    dcr = next(s for s in shapes if s.attrib["shapetype"] == "doccacheretrieve")
    # Round-trips back through the parser to the raw text (well-formed escaping).
    assert dcr.attrib["userlabel"] == "A & B <retrieve>"
    assert dcr.find("configuration/doccacheretrieve").attrib["docCache"] == "CACHE&<1>"


def test_doccacheretrieve_build_bypass_empty_cache_id_raises():
    # build() stays total on the validate_config-bypass path: an empty docCache
    # would emit a semantically broken docCache="" (well-formed XML the parse-back
    # guard would not catch), so raise rather than emit it.
    cfg = _base_config(transform={"mode": "doccacheretrieve", "document_cache_id": "   "})
    with pytest.raises(BuilderValidationError) as exc:
        ProcessFlowBuilder.build(cfg, name="N")
    assert exc.value.error_code == "PROCESS_DOCCACHE_RETRIEVE_CONFIG_INVALID"
    assert exc.value.field == "transform.document_cache_id"


def test_doccacheretrieve_build_bypass_unsupported_empty_cache_behavior_raises():
    # build() (bypassing validate_config) must not serialize an unsupported
    # emptyCacheBehavior — the emitter re-guards the v1 'stopprocess'-only invariant
    # so a direct build cannot emit emptyCacheBehavior="returnerror".
    cfg = _base_config(transform={
        "mode": "doccacheretrieve",
        "document_cache_id": "CACHE-1",
        "empty_cache_behavior": "returnerror",
    })
    with pytest.raises(BuilderValidationError) as exc:
        ProcessFlowBuilder.build(cfg, name="N")
    assert exc.value.error_code == "PROCESS_DOCCACHE_RETRIEVE_CONFIG_INVALID"
    assert exc.value.field == "transform.empty_cache_behavior"


def test_doccacheretrieve_build_bypass_keyed_retrieval_raises():
    # build() (bypassing validate_config) must not serialize loadAllDoc="false"
    # with an empty <cacheKeyValues/> (a broken keyed retrieve); the emitter
    # re-guards the v1 all-document-only invariant.
    cfg = _base_config(transform={
        "mode": "doccacheretrieve",
        "document_cache_id": "CACHE-1",
        "load_all_documents": False,
    })
    with pytest.raises(BuilderValidationError) as exc:
        ProcessFlowBuilder.build(cfg, name="N")
    assert exc.value.error_code == "PROCESS_DOCCACHE_RETRIEVE_CONFIG_INVALID"
    assert exc.value.field == "transform.load_all_documents"


def test_doccacheretrieve_build_bypass_non_bool_load_all_raises():
    # A non-bool truthy load_all_documents (e.g. the string "true") is NOT True, so
    # the emitter rejects it rather than emit loadAllDoc from an unvetted value.
    cfg = _base_config(transform={
        "mode": "doccacheretrieve",
        "document_cache_id": "CACHE-1",
        "load_all_documents": "true",
    })
    with pytest.raises(BuilderValidationError) as exc:
        ProcessFlowBuilder.build(cfg, name="N")
    assert exc.value.error_code == "PROCESS_DOCCACHE_RETRIEVE_CONFIG_INVALID"
    assert exc.value.field == "transform.load_all_documents"


def test_doccacheretrieve_composes_with_try_catch_wrapper():
    # The retrieve shape sits in the middle-transform slot, so it composes with the
    # verified Try/Catch + DLQ wrapper unchanged (the wrapped chain still contains
    # exactly one doccacheretrieve, non-terminal, between source and target).
    cfg = _doccacheretrieve_config(
        reliability={
            "retry_count": 1,
            "dlq": {"mode": "document_cache_ref", "document_cache_id": "$ref:DLQ"},
        }
    )
    err = ProcessFlowBuilder.validate_config(cfg, depends_on=["DLQ"])
    assert err is None
    xml = ProcessFlowBuilder.build(cfg, name="Wrapped Retrieve")
    _, _, shapes = _parse_process(xml)
    retrieves = [s for s in shapes if s.attrib["shapetype"] == "doccacheretrieve"]
    assert len(retrieves) == 1
    # Wrapped in a Try/Catch (catcherrors present) and the retrieve still forwards.
    assert any(s.attrib["shapetype"] == "catcherrors" for s in shapes)
    assert list(retrieves[0].find("dragpoints")) != []


# ---------------------------------------------------------------------------
# Document Cache Remove transform (issue #110 M10.6)
# ---------------------------------------------------------------------------

_DOCCACHE_REMOVE_GOLDEN = (
    Path(__file__).resolve().parent
    / "fixtures"
    / "golden_xml"
    / "document_cache_remove.xml"
)


def _doccacheremove_config(label="Clear Status Cache", **overrides):
    transform = {"mode": "doccacheremove", "document_cache_id": _DOCCACHE_ID}
    if label is not None:
        transform["label"] = label
    transform.update(overrides.pop("transform_extra", {}))
    return _base_config(transform=transform, **overrides)


def test_doccacheremove_inserts_linear_shape_between_source_and_target():
    xml = ProcessFlowBuilder.build(_doccacheremove_config(), name="Cache Remove Sync")
    _, _, shapes = _parse_process(xml)
    assert [s.attrib["shapetype"] for s in shapes] == [
        "start", "connectoraction", "doccacheremove", "connectoraction", "stop",
    ]
    dcr = shapes[2]
    assert dcr.attrib["image"] == "doccacheremove_icon"
    assert dcr.attrib["userlabel"] == "Clear Status Cache"
    cfg = dcr.find("configuration/doccacheremove")
    assert cfg.attrib["docCache"] == _DOCCACHE_ID
    assert cfg.attrib["removeAllDocuments"] == "true"
    # Remove carries NO emptyCacheBehavior / loadAllDoc (those are retrieve-only).
    assert "emptyCacheBehavior" not in cfg.attrib
    assert "loadAllDoc" not in cfg.attrib
    # All-document remove emits an empty <cacheKeyValues/> (keyed removal deferred).
    key_values = cfg.find("cacheKeyValues")
    assert key_values is not None and list(key_values) == []
    # Linear, non-terminal: exactly one forward dragpoint to the next shape.
    dragpoints = dcr.find("dragpoints")
    assert [dp.attrib["toShape"] for dp in dragpoints] == ["shape4"]


def test_doccacheremove_matches_golden_fixture():
    """Byte-exact golden (issue #110 g): the docCache/removeAllDocuments attribute
    order and the empty <cacheKeyValues/> child are load-bearing and must match the
    live capture byte-for-byte."""
    emitted = ProcessFlowBuilder.build(
        _doccacheremove_config(), name="DocumentCacheRemove Sync"
    )
    assert emitted == _DOCCACHE_REMOVE_GOLDEN.read_text()


def test_doccacheremove_accepts_ref_document_cache_id():
    cfg = _doccacheremove_config()
    cfg["transform"]["document_cache_id"] = "$ref:MyCache"
    # Reachable when declared in depends_on...
    assert ProcessFlowBuilder.validate_config(cfg, depends_on=["MyCache"]) is None
    # ...and MISSING_PROCESS_DEPENDENCY when not (generic ref-reachability walk).
    err = ProcessFlowBuilder.validate_config(cfg, depends_on=[])
    assert err is not None
    assert err.error_code == "MISSING_PROCESS_DEPENDENCY"


def test_doccacheremove_rejects_missing_document_cache_id():
    cfg = _base_config(transform={"mode": "doccacheremove", "label": "x"})
    err = ProcessFlowBuilder.validate_config(cfg, depends_on=[])
    assert err is not None
    assert err.error_code == "PROCESS_DOCCACHE_REMOVE_CONFIG_INVALID"
    assert err.field == "transform.document_cache_id"


def test_doccacheremove_rejects_blank_document_cache_id():
    cfg = _base_config(
        transform={"mode": "doccacheremove", "document_cache_id": "   "}
    )
    err = ProcessFlowBuilder.validate_config(cfg, depends_on=[])
    assert err is not None
    assert err.error_code == "PROCESS_DOCCACHE_REMOVE_CONFIG_INVALID"
    assert err.field == "transform.document_cache_id"


def test_doccacheremove_rejects_keyed_removal_remove_all_false():
    # Keyed/index removal (removeAllDocuments=false) is deferred pending a live capture.
    cfg = _doccacheremove_config()
    cfg["transform"]["remove_all_documents"] = False
    err = ProcessFlowBuilder.validate_config(cfg, depends_on=[])
    assert err is not None
    assert err.error_code == "PROCESS_DOCCACHE_REMOVE_CONFIG_INVALID"
    assert err.field == "transform.remove_all_documents"


def test_doccacheremove_rejects_unknown_transform_key():
    cfg = _doccacheremove_config()
    cfg["transform"]["bogus"] = 1
    err = ProcessFlowBuilder.validate_config(cfg, depends_on=[])
    assert err is not None
    assert err.error_code == "PROCESS_DOCCACHE_REMOVE_CONFIG_INVALID"
    assert err.field == "transform"


def test_doccacheremove_rejects_retrieve_only_keys():
    # empty_cache_behavior / load_all_documents are retrieve-only — on a remove they
    # are unknown keys and rejected (never silently dropped).
    cfg = _doccacheremove_config()
    cfg["transform"]["empty_cache_behavior"] = "stopprocess"
    err = ProcessFlowBuilder.validate_config(cfg, depends_on=[])
    assert err is not None
    assert err.error_code == "PROCESS_DOCCACHE_REMOVE_CONFIG_INVALID"
    assert err.field == "transform"


def test_doccacheremove_xml_escapes_label_and_cache_id():
    cfg = _base_config(
        transform={
            "mode": "doccacheremove",
            "label": "A & B <remove>",
            "document_cache_id": "CACHE&<1>",
        }
    )
    xml = ProcessFlowBuilder.build(cfg, name="N")
    _, _, shapes = _parse_process(xml)
    dcr = next(s for s in shapes if s.attrib["shapetype"] == "doccacheremove")
    # Round-trips back through the parser to the raw text (well-formed escaping).
    assert dcr.attrib["userlabel"] == "A & B <remove>"
    assert dcr.find("configuration/doccacheremove").attrib["docCache"] == "CACHE&<1>"


def test_doccacheremove_build_bypass_empty_cache_id_raises():
    # build() stays total on the validate_config-bypass path: an empty docCache
    # would emit a semantically broken docCache="" (well-formed XML the parse-back
    # guard would not catch), so raise rather than emit it.
    cfg = _base_config(transform={"mode": "doccacheremove", "document_cache_id": "   "})
    with pytest.raises(BuilderValidationError) as exc:
        ProcessFlowBuilder.build(cfg, name="N")
    assert exc.value.error_code == "PROCESS_DOCCACHE_REMOVE_CONFIG_INVALID"
    assert exc.value.field == "transform.document_cache_id"


def test_doccacheremove_build_bypass_keyed_removal_raises():
    # build() (bypassing validate_config) must not serialize removeAllDocuments="false"
    # with an empty <cacheKeyValues/> (a broken keyed remove); the emitter re-guards
    # the v1 all-document-only invariant.
    cfg = _base_config(transform={
        "mode": "doccacheremove",
        "document_cache_id": "CACHE-1",
        "remove_all_documents": False,
    })
    with pytest.raises(BuilderValidationError) as exc:
        ProcessFlowBuilder.build(cfg, name="N")
    assert exc.value.error_code == "PROCESS_DOCCACHE_REMOVE_CONFIG_INVALID"
    assert exc.value.field == "transform.remove_all_documents"


def test_doccacheremove_build_bypass_non_bool_remove_all_raises():
    # A non-bool truthy remove_all_documents (e.g. the string "true") is NOT True, so
    # the emitter rejects it rather than emit removeAllDocuments from an unvetted value.
    cfg = _base_config(transform={
        "mode": "doccacheremove",
        "document_cache_id": "CACHE-1",
        "remove_all_documents": "true",
    })
    with pytest.raises(BuilderValidationError) as exc:
        ProcessFlowBuilder.build(cfg, name="N")
    assert exc.value.error_code == "PROCESS_DOCCACHE_REMOVE_CONFIG_INVALID"
    assert exc.value.field == "transform.remove_all_documents"


def test_doccacheremove_composes_with_try_catch_wrapper():
    # The remove shape sits in the middle-transform slot, so it composes with the
    # verified Try/Catch + DLQ wrapper unchanged (the wrapped chain still contains
    # exactly one doccacheremove, non-terminal, between source and target).
    cfg = _doccacheremove_config(
        reliability={
            "retry_count": 1,
            "dlq": {"mode": "document_cache_ref", "document_cache_id": "$ref:DLQ"},
        }
    )
    err = ProcessFlowBuilder.validate_config(cfg, depends_on=["DLQ"])
    assert err is None
    xml = ProcessFlowBuilder.build(cfg, name="Wrapped Remove")
    _, _, shapes = _parse_process(xml)
    removes = [s for s in shapes if s.attrib["shapetype"] == "doccacheremove"]
    assert len(removes) == 1
    # Wrapped in a Try/Catch (catcherrors present) and the remove still forwards.
    assert any(s.attrib["shapetype"] == "catcherrors" for s in shapes)
    assert list(removes[0].find("dragpoints")) != []


# ---------------------------------------------------------------------------
# Return Documents terminal (issue #107 M10.3)
# ---------------------------------------------------------------------------

_RETURN_DOCUMENTS_GOLDEN = (
    Path(__file__).resolve().parent
    / "fixtures"
    / "golden_xml"
    / "return_documents_terminal.xml"
)


def test_return_documents_terminal_replaces_stop():
    cfg = _base_config(return_documents={"enabled": True})
    xml = ProcessFlowBuilder.build(cfg, name="Return Documents Subprocess")
    _, _, shapes = _parse_process(xml)
    assert [s.attrib["shapetype"] for s in shapes] == [
        "start", "connectoraction", "connectoraction", "returndocuments",
    ]
    # Return Documents is terminal: no Stop shape and empty dragpoints.
    assert 'shapetype="stop"' not in xml
    rd = shapes[-1]
    assert rd.attrib["image"] == "returndocuments_icon"
    assert list(rd.find("dragpoints")) == []  # empty <dragpoints/>


def test_return_documents_terminal_matches_golden_fixture():
    """Byte-exact golden (issue #107 g): the live Return Documents structure —
    empty inner label, empty dragpoints, NO trailing Stop — is load-bearing and
    must match the live capture byte-for-byte."""
    cfg = _base_config(return_documents={"enabled": True})
    emitted = ProcessFlowBuilder.build(cfg, name="Return Documents Subprocess")
    assert emitted == _RETURN_DOCUMENTS_GOLDEN.read_text()


def test_return_documents_label_maps_to_userlabel_and_inner_label():
    cfg = _base_config(return_documents={"enabled": True, "label": "Status Updates"})
    xml = ProcessFlowBuilder.build(cfg, name="N")
    _, _, shapes = _parse_process(xml)
    rd = next(s for s in shapes if s.attrib["shapetype"] == "returndocuments")
    # The single public label maps to BOTH the shape userlabel and the inner
    # <returndocuments label="..."> custom label (live capture shows both equal).
    assert rd.attrib["userlabel"] == "Status Updates"
    assert rd.find("configuration/returndocuments").attrib["label"] == "Status Updates"


def test_return_documents_label_is_xml_escaped():
    cfg = _base_config(return_documents={"enabled": True, "label": "a < b & c > d"})
    xml = ProcessFlowBuilder.build(cfg, name="N")
    _, _, shapes = _parse_process(xml)
    rd = next(s for s in shapes if s.attrib["shapetype"] == "returndocuments")
    # Angle brackets / ampersands round-trip back to raw text in both attributes.
    assert rd.attrib["userlabel"] == "a < b & c > d"
    assert rd.find("configuration/returndocuments").attrib["label"] == "a < b & c > d"


def test_return_documents_disabled_keeps_default_stop():
    # enabled=False preserves the trailing Stop byte-for-byte (== no block at all).
    cfg = _base_config(return_documents={"enabled": False})
    xml = ProcessFlowBuilder.build(cfg, name="N")
    assert xml == ProcessFlowBuilder.build(_base_config(), name="N")
    _, _, shapes = _parse_process(xml)
    assert shapes[-1].attrib["shapetype"] == "stop"


def test_return_documents_composes_with_reliability_try_catch():
    # return_documents + a wired DLQ Try/Catch: the Return Documents terminal sits
    # at the end of the Try (success) path; the catch leg keeps its own terminal.
    # Return Documents never reaches the catch Stop, so the graph stays valid.
    cfg = _base_config(
        return_documents={"enabled": True},
        reliability={
            "retry_count": 2,
            "dlq": {"mode": "document_cache_ref", "document_cache_id": "CACHE-1"},
        },
    )
    xml = ProcessFlowBuilder.build(cfg, name="RD+DLQ")
    _, _, shapes = _parse_process(xml)
    types = [s.attrib["shapetype"] for s in shapes]
    assert "catcherrors" in types
    assert "returndocuments" in types
    rd = next(s for s in shapes if s.attrib["shapetype"] == "returndocuments")
    assert list(rd.find("dragpoints")) == []  # terminal even inside the Try chain


def test_return_documents_rejects_non_object():
    cfg = _base_config(return_documents="yes")
    err = ProcessFlowBuilder.validate_config(cfg, depends_on=[])
    assert err is not None
    assert err.error_code == "PROCESS_RETURN_DOCUMENTS_CONFIG_INVALID"
    assert err.field == "return_documents"


def test_return_documents_rejects_non_bool_enabled():
    cfg = _base_config(return_documents={"enabled": "true"})
    err = ProcessFlowBuilder.validate_config(cfg, depends_on=[])
    assert err is not None
    assert err.error_code == "PROCESS_RETURN_DOCUMENTS_CONFIG_INVALID"
    assert err.field == "return_documents.enabled"


def test_return_documents_rejects_non_string_label():
    cfg = _base_config(return_documents={"enabled": True, "label": 5})
    err = ProcessFlowBuilder.validate_config(cfg, depends_on=[])
    assert err is not None
    assert err.error_code == "PROCESS_RETURN_DOCUMENTS_CONFIG_INVALID"
    assert err.field == "return_documents.label"


def test_return_documents_rejects_unknown_key():
    cfg = _base_config(return_documents={"enabled": True, "bogus": 1})
    err = ProcessFlowBuilder.validate_config(cfg, depends_on=[])
    assert err is not None
    assert err.error_code == "PROCESS_RETURN_DOCUMENTS_CONFIG_INVALID"
    assert err.field == "return_documents"


def test_wrapper_subprocess_ends_in_return_documents():
    # A wrapper/facade that is itself a subprocess can end in Return Documents.
    from src.boomi_mcp.categories.components.builders import WrapperSubprocessBuilder

    cfg = {
        "process_kind": "wrapper_subprocess",
        "process_calls": [{"process_id": "55555555-5555-5555-5555-555555555555"}],
        "return_documents": {"enabled": True, "label": "Done"},
    }
    assert WrapperSubprocessBuilder.validate_config(cfg, depends_on=[]) is None
    xml = WrapperSubprocessBuilder.build(cfg, name="Wrapper RD")
    _, _, shapes = _parse_process(xml)
    assert [s.attrib["shapetype"] for s in shapes] == [
        "start", "processcall", "returndocuments",
    ]
    assert 'shapetype="stop"' not in xml
    assert shapes[-1].attrib["userlabel"] == "Done"


def test_wrapper_subprocess_rejects_invalid_return_documents():
    from src.boomi_mcp.categories.components.builders import WrapperSubprocessBuilder

    cfg = {
        "process_kind": "wrapper_subprocess",
        "process_calls": [{"process_id": "55555555-5555-5555-5555-555555555555"}],
        "return_documents": {"enabled": "yes"},
    }
    err = WrapperSubprocessBuilder.validate_config(cfg, depends_on=[])
    assert err is not None
    assert err.error_code == "PROCESS_RETURN_DOCUMENTS_CONFIG_INVALID"


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

    def test_rejects_unsupported_source_connector_type(self):
        # Database (db_read Get) and REST Client (rest_fetch GET) are the supported
        # source connectors; an unrelated connector (sftp) is still rejected.
        cfg = _base_config()
        cfg["source"]["connector_type"] = "sftp"
        err = ProcessFlowBuilder.validate_config(cfg, depends_on=[])
        assert err.error_code == "PROCESS_CONNECTOR_BINDING_INVALID"
        assert err.field == "source.connector_type"

    def test_rejects_rest_source_for_database_to_api_sync(self):
        # Issue #72 M5.4: the base database_to_api_sync protocol stays DB-source-only.
        # A REST source is valid ONLY through the sync_pipeline fetch lowering (which
        # passes allow_rest_source=True); a hand-authored database_to_api_sync with a
        # REST source is rejected here.
        cfg = _base_config()
        cfg["source"]["connector_type"] = "rest"
        cfg["source"]["action_type"] = "GET"
        err = ProcessFlowBuilder.validate_config(cfg, depends_on=[])
        assert err.error_code == "PROCESS_CONNECTOR_BINDING_INVALID"
        assert err.field == "source.connector_type"

    def test_accepts_rest_get_source_when_allow_rest_source(self):
        # With allow_rest_source=True (the sync_pipeline delegate path), a REST GET
        # source binding validates clean.
        cfg = _base_config()
        cfg["source"]["connector_type"] = "rest"
        cfg["source"]["action_type"] = "GET"
        assert ProcessFlowBuilder.validate_config(
            cfg, depends_on=[], allow_rest_source=True
        ) is None

    def test_rejects_non_get_rest_source_when_allow_rest_source(self):
        # Under allow_rest_source=True a REST source is GET-only — POST/PATCH/etc.
        # are rejected on source.action_type so a source-side write is never modeled.
        cfg = _base_config()
        cfg["source"]["connector_type"] = "rest"
        cfg["source"]["action_type"] = "POST"
        err = ProcessFlowBuilder.validate_config(
            cfg, depends_on=[], allow_rest_source=True
        )
        assert err.error_code == "PROCESS_CONNECTOR_BINDING_INVALID"
        assert err.field == "source.action_type"

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


# ---------------------------------------------------------------------------
# Issue #108 M10.4 — reliability.catch_exception validation + emission
# ---------------------------------------------------------------------------

def _exception_reliability(**overrides):
    catch_exception = {
        "title": "Halt",
        "message_template": "boom: {1}",
        "parameter_source": "caught_error",
    }
    catch_exception.update(overrides)
    return {"reliability": {"catch_exception": catch_exception}}


def test_catch_exception_bare_throw_emits_catch_leg_exception():
    # A catch_exception ALONE (no DLQ, no notify) wires a Try/Catch whose catch
    # leg throws an Exception — the live "deliberate fail/halt" shape.
    cfg = _base_config(**_exception_reliability(parameter_source="current_document"))
    assert ProcessFlowBuilder.validate_config(cfg, depends_on=[]) is None
    xml = ProcessFlowBuilder.build(cfg, name="P")
    _root, _process, shapes = _parse_process(xml)
    types = [s.attrib["shapetype"] for s in shapes]
    assert "catcherrors" in types and "exception" in types
    ex = next(s for s in shapes if s.attrib["shapetype"] == "exception")
    # Terminal: empty dragpoints.
    assert ex.find("dragpoints") is not None and list(ex.find("dragpoints")) == []
    assert ex.attrib["image"] == "exception_icon"
    config = ex.find("configuration/exception")
    assert config.attrib["stopProcessReturnSingleDoc"] == "false"
    assert config.attrib["stopsingledoc"] == "false"
    assert config.attrib["title"] == "Halt" and ex.attrib["userlabel"] == "Halt"
    assert config.find("exMessage").text == "boom: {1}"
    # current_document -> self-closing valueType="current", no usesEncryption.
    pv = config.find("exParameters/parametervalue")
    assert pv.attrib["valueType"] == "current" and "usesEncryption" not in pv.attrib
    assert list(pv) == []
    # The catcherrors Catch dragpoint targets the exception (no bare Stop).
    ce = next(s for s in shapes if s.attrib["shapetype"] == "catcherrors")
    catch_dp = next(d for d in ce.find("dragpoints") if d.attrib.get("identifier") == "error")
    assert catch_dp.attrib["toShape"] == ex.attrib["name"]


def test_catch_exception_caught_error_track_binding():
    cfg = _base_config(**_exception_reliability(parameter_source="caught_error"))
    xml = ProcessFlowBuilder.build(cfg, name="P")
    _root, _process, shapes = _parse_process(xml)
    ex = next(s for s in shapes if s.attrib["shapetype"] == "exception")
    tp = ex.find("configuration/exception/exParameters/parametervalue/trackparameter")
    assert tp.attrib["propertyId"] == "meta.base.catcherrorsmessage"
    assert tp.attrib["propertyName"] == "Base - Try/Catch Message"
    pv = ex.find("configuration/exception/exParameters/parametervalue")
    assert pv.attrib["valueType"] == "track" and "usesEncryption" not in pv.attrib


def test_catch_exception_none_omits_exparameters():
    cfg = _base_config(
        **_exception_reliability(parameter_source="none", message_template="static halt")
    )
    xml = ProcessFlowBuilder.build(cfg, name="P")
    _root, _process, shapes = _parse_process(xml)
    ex = next(s for s in shapes if s.attrib["shapetype"] == "exception")
    assert ex.find("configuration/exception/exParameters") is None


def test_catch_exception_stop_single_document_true():
    cfg = _base_config(**_exception_reliability(stop_single_document=True))
    xml = ProcessFlowBuilder.build(cfg, name="P")
    _root, _process, shapes = _parse_process(xml)
    ex = next(s for s in shapes if s.attrib["shapetype"] == "exception")
    assert ex.find("configuration/exception").attrib["stopsingledoc"] == "true"


def test_catch_exception_with_notify_and_dlq_composes():
    # notify -> dlq route -> exception; retry_count > 0 allowed with the leg.
    cfg = _base_config(reliability={
        "retry_count": 2,
        "dlq": {"mode": "document_cache_ref", "document_cache_id": "CACHE-1"},
        "catch_notify": {"level": "ERROR", "message_template": "f: meta.base.catcherrorsmessage"},
        "catch_exception": {"message_template": "halt: {1}", "parameter_source": "caught_error"},
    })
    assert ProcessFlowBuilder.validate_config(cfg, depends_on=[]) is None
    xml = ProcessFlowBuilder.build(cfg, name="P")
    _root, _process, shapes = _parse_process(xml)
    types = [s.attrib["shapetype"] for s in shapes]
    # The catch leg ends in exception (no catch-row Stop after the DLQ route).
    assert types.count("exception") == 1
    assert "notify" in types and "doccacheload" in types
    # Exactly one Stop (the normal Try-path terminal); the catch leg throws instead.
    assert types.count("stop") == 1


def test_catch_exception_message_format_escaping():
    cfg = _base_config(**_exception_reliability(
        parameter_source="current_document", message_template="it's a halt {1}"
    ))
    xml = ProcessFlowBuilder.build(cfg, name="P")
    _root, _process, shapes = _parse_process(xml)
    ex = next(s for s in shapes if s.attrib["shapetype"] == "exception")
    # Single quote doubled by the shared MessageFormat escaper; {1} preserved.
    assert ex.find("configuration/exception/exMessage").text == "it''s a halt {1}"


def test_catch_exception_retry_without_dlq_allowed():
    cfg = _base_config(reliability={
        "retry_count": 3,
        "catch_exception": {"message_template": "halt {1}", "parameter_source": "caught_error"},
    })
    assert ProcessFlowBuilder.validate_config(cfg, depends_on=[]) is None


def _exc_err(catch_exception):
    cfg = _base_config(reliability={"catch_exception": catch_exception})
    return ProcessFlowBuilder.validate_config(cfg, depends_on=[])


def test_catch_exception_rejects_non_dict():
    err = _exc_err("nope")
    assert err is not None and err.error_code == "PROCESS_EXCEPTION_CONFIG_INVALID"
    assert err.field == "reliability.catch_exception"


def test_catch_exception_rejects_unknown_key():
    err = _exc_err({"message_template": "x {1}", "bogus": 1})
    assert err is not None and err.error_code == "PROCESS_EXCEPTION_CONFIG_INVALID"
    assert "unsupported keys" in str(err) and "bogus" in str(err)


def test_catch_exception_requires_message_template():
    for bad in ({}, {"message_template": ""}, {"message_template": "   "}, {"message_template": 5}):
        err = _exc_err(bad)
        assert err is not None and err.error_code == "PROCESS_EXCEPTION_CONFIG_INVALID"
        assert err.field == "reliability.catch_exception.message_template"


def test_catch_exception_rejects_bad_parameter_source():
    err = _exc_err({"message_template": "x {1}", "parameter_source": "bogus"})
    assert err is not None and err.error_code == "PROCESS_EXCEPTION_CONFIG_INVALID"
    assert err.field == "reliability.catch_exception.parameter_source"


def test_catch_exception_rejects_non_bool_stop_single_document():
    err = _exc_err({"message_template": "x {1}", "stop_single_document": "yes"})
    assert err is not None and err.error_code == "PROCESS_EXCEPTION_CONFIG_INVALID"
    assert err.field == "reliability.catch_exception.stop_single_document"


def test_catch_exception_rejects_non_string_title():
    err = _exc_err({"message_template": "x {1}", "title": 7})
    assert err is not None and err.error_code == "PROCESS_EXCEPTION_CONFIG_INVALID"
    assert err.field == "reliability.catch_exception.title"


def test_catch_exception_requires_placeholder_when_source_binds():
    # caught_error/current_document need {1}; none does not.
    err = _exc_err({"message_template": "no placeholder", "parameter_source": "caught_error"})
    assert err is not None and err.error_code == "PROCESS_EXCEPTION_CONFIG_INVALID"
    assert err.field == "reliability.catch_exception.message_template"
    assert _exc_err({"message_template": "no placeholder", "parameter_source": "none"}) is None


def test_build_bypass_guard_raises_for_invalid_catch_exception():
    # validate_config-bypass: an invalid (non-dict) catch_exception that does not
    # wire a Try/Catch must raise rather than silently emit a linear flow.
    cfg = _base_config(reliability={"catch_exception": "nope"})
    with pytest.raises(BuilderValidationError) as exc:
        ProcessFlowBuilder.build(cfg, name="P")
    assert exc.value.error_code == "PROCESS_EXCEPTION_CONFIG_INVALID"


def test_build_bypass_raises_for_malformed_catch_exception_dict():
    # validate_config-bypass: a malformed catch_exception DICT (e.g. missing
    # message_template) makes _should_emit_try_catch True (it is a dict), so it
    # reaches the catch-leg emitter — which must re-validate and raise rather than
    # emit an empty <exMessage> (totality, mirrors the DLQ/Notify emitter guards).
    cfg = _base_config(reliability={"catch_exception": {}})
    with pytest.raises(BuilderValidationError) as exc:
        ProcessFlowBuilder.build(cfg, name="P")
    assert exc.value.error_code == "PROCESS_EXCEPTION_CONFIG_INVALID"


def test_build_bypass_raises_for_non_dict_catch_exception_with_valid_dlq():
    # validate_config-bypass: a non-dict catch_exception alongside a VALID DLQ mode
    # still reaches the catch-leg emitter (dlq makes _should_emit_try_catch True),
    # where _emit_exception would AttributeError on "str".get(...). The emitter
    # guard must raise PROCESS_EXCEPTION_CONFIG_INVALID instead.
    cfg = _base_config(reliability={
        "dlq": {"mode": "document_cache_ref", "document_cache_id": "CACHE-1"},
        "catch_exception": "nope",
    })
    with pytest.raises(BuilderValidationError) as exc:
        ProcessFlowBuilder.build(cfg, name="P")
    assert exc.value.error_code == "PROCESS_EXCEPTION_CONFIG_INVALID"


# ---------------------------------------------------------------------------
# Issue #112 M10.8 — Branch (N-way forward fan-out)
# ---------------------------------------------------------------------------

_REST_CONN_ID_2 = "55555555-5555-5555-5555-555555555555"
_REST_OP_ID_2 = "66666666-6666-6666-6666-666666666666"
_REST_CONN_ID_3 = "77777777-7777-7777-7777-777777777777"
_REST_OP_ID_3 = "88888888-8888-8888-8888-888888888888"

_BRANCH_FANOUT_GOLDEN = (
    Path(__file__).resolve().parent / "fixtures" / "golden_xml" / "branch_fanout.xml"
)


def _branch_leg(connection_id=_REST_CONN_ID_2, operation_id=_REST_OP_ID_2,
                action_type="PUT", **extra):
    leg = {
        "connector_type": "rest",
        "connection_id": connection_id,
        "operation_id": operation_id,
        "action_type": action_type,
    }
    leg.update(extra)
    return leg


def _branch_config(targets=None, enabled=True, **overrides):
    branch = {"enabled": enabled, "targets": targets if targets is not None else [_branch_leg()]}
    return _base_config(branch=branch, **overrides)


def test_branch_inserts_branch_after_source_with_target_stop_legs():
    xml = ProcessFlowBuilder.build(_branch_config(), name="Branch Fanout")
    _, _, shapes = _parse_process(xml)
    assert [s.attrib["shapetype"] for s in shapes] == [
        "start", "connectoraction", "branch",
        "connectoraction", "stop",   # leg 1 (top-level target)
        "connectoraction", "stop",   # leg 2 (branch.targets[0])
    ]


def test_branch_shape_attributes_and_num_branches():
    xml = ProcessFlowBuilder.build(_branch_config(), name="Branch Fanout")
    _, _, shapes = _parse_process(xml)
    branch = next(s for s in shapes if s.attrib["shapetype"] == "branch")
    assert branch.attrib["image"] == "branch_icon"
    assert branch.attrib["userlabel"] == ""
    inner = branch.find("configuration/branch")
    assert inner is not None
    assert inner.attrib["numBranches"] == "2"


def test_branch_dragpoints_are_sequential_and_target_each_leg():
    xml = ProcessFlowBuilder.build(_branch_config(), name="Branch Fanout")
    _, _, shapes = _parse_process(xml)
    branch = next(s for s in shapes if s.attrib["shapetype"] == "branch")
    dragpoints = branch.findall("dragpoints/dragpoint")
    assert [d.attrib["identifier"] for d in dragpoints] == ["1", "2"]
    assert [d.attrib["text"] for d in dragpoints] == ["1", "2"]
    assert [d.attrib["name"] for d in dragpoints] == [
        "shape3.dragpoint1", "shape3.dragpoint2",
    ]
    # Leg 1 first shape is shape4, leg 2 first shape is shape6 (each leg = target+stop).
    assert [d.attrib["toShape"] for d in dragpoints] == ["shape4", "shape6"]
    # Every dragpoint wires to a real shape (no unset/dangling output).
    shape_names = {s.attrib["name"] for s in shapes}
    assert all(d.attrib["toShape"] in shape_names for d in dragpoints)


def test_branch_numbranches_scales_with_leg_count():
    cfg = _branch_config(targets=[
        _branch_leg(),
        _branch_leg(connection_id=_REST_CONN_ID_3, operation_id=_REST_OP_ID_3, action_type="GET"),
    ])
    xml = ProcessFlowBuilder.build(cfg, name="Branch Fanout 3way")
    _, _, shapes = _parse_process(xml)
    branch = next(s for s in shapes if s.attrib["shapetype"] == "branch")
    assert branch.find("configuration/branch").attrib["numBranches"] == "3"
    dragpoints = branch.findall("dragpoints/dragpoint")
    assert [d.attrib["identifier"] for d in dragpoints] == ["1", "2", "3"]
    # Three independent target->stop legs after the branch.
    assert [s.attrib["shapetype"] for s in shapes].count("branch") == 1
    assert [s.attrib["shapetype"] for s in shapes].count("connectoraction") == 4  # source + 3 legs


def test_branch_leg_target_carries_its_own_rest_binding():
    cfg = _branch_config(targets=[_branch_leg(action_type="PUT")])
    xml = ProcessFlowBuilder.build(cfg, name="Branch Fanout")
    _, _, shapes = _parse_process(xml)
    # leg-2 target is the 2nd connectoraction after the branch (shape6).
    leg2 = next(s for s in shapes if s.attrib["name"] == "shape6")
    ca = leg2.find("configuration/connectoraction")
    assert ca.attrib["actionType"] == "PUT"
    assert ca.attrib["connectionId"] == _REST_CONN_ID_2
    assert ca.attrib["operationId"] == _REST_OP_ID_2


def test_branch_fanout_matches_golden_fixture():
    """Byte-exact golden (issue #112 g): the Branch shape, its derived numBranches,
    the sequential identifier/text dragpoints, and the per-leg target/stop layout
    are load-bearing and must match byte-for-byte."""
    emitted = ProcessFlowBuilder.build(
        _branch_config(), name="Branch Fanout", folder_name="Golden/Fixtures"
    )
    assert emitted == _BRANCH_FANOUT_GOLDEN.read_text()


def test_branch_disabled_is_byte_identical_to_no_branch():
    """branch.enabled=false keeps the single-target linear flow byte-for-byte."""
    no_branch = ProcessFlowBuilder.build(_base_config(), name="P")
    disabled = ProcessFlowBuilder.build(
        _base_config(branch={"enabled": False, "targets": [_branch_leg()]}), name="P"
    )
    assert no_branch == disabled


def test_branch_enabled_missing_targets_raises_branch_output_unset():
    # Includes non-iterable truthy scalars (1 / True / 1.5): the combo guard now
    # runs BEFORE the targets-is-list check (review reorder), so it must stay total
    # on a malformed targets rather than raising TypeError on enumerate(scalar).
    for branch in ({"enabled": True}, {"enabled": True, "targets": []},
                   {"enabled": True, "targets": "x"}, {"enabled": True, "targets": 1},
                   {"enabled": True, "targets": True}, {"enabled": True, "targets": 1.5},
                   {"enabled": True, "targets": {"k": 1}}):
        err = ProcessFlowBuilder.validate_config(_base_config(branch=branch))
        assert err is not None and err.error_code == "BRANCH_OUTPUT_UNSET", branch
        assert err.field == "branch.targets"


def test_branch_validate_config_and_build_report_identical_errors():
    # Issue #112 review: validate_config and build() funnel through ONE branch
    # validator, so for every malformed branch config they MUST report the same
    # structured (error_code, field) — never diverge (e.g. validate_config saying
    # dynamic_path while build() says BRANCH_OUTPUT_UNSET).
    malformed_branches = [
        {"enabled": True},                                   # no targets
        {"enabled": True, "targets": []},                    # empty
        {"enabled": True, "targets": 1},                     # non-iterable scalar
        {"enabled": True, "targets": [_branch_leg(dynamic_path=_VALID_DYNAMIC_PATH)]},
        {"enabled": True, "targets": [_branch_leg(dynamic_path={"x": 1})]},
        {"enabled": True, "targets": [_branch_leg(connection_id="")]},
        {"enabled": True, "targets": [_branch_leg() for _ in range(25)]},
        {"enabled": True, "targets": [_branch_leg()], "foo": 1},
        # A malformed branch block that makes _branch_enabled() return false must
        # STILL raise the same structured error in build() as validate_config —
        # build() must not silently emit linear XML and drop the branch.
        1,                                                   # non-dict branch
        [],                                                  # non-dict branch
        {"enabled": "true", "targets": [_branch_leg()]},     # non-bool enabled (string)
        {"enabled": 1, "targets": [_branch_leg()]},          # non-bool enabled (int)
    ]
    extra_for_combo = [
        # no-targets + an unsupported composition: BRANCH_OUTPUT_UNSET wins (the
        # more fundamental error), in BOTH paths.
        ({"enabled": True}, {"reliability": {"retry_count": 0, "dlq": {"mode": "document_cache_ref", "document_cache_id": "C"}}}),
        ({"enabled": True, "targets": [_branch_leg()]}, {"return_documents": {"enabled": True}}),
    ]
    def _vc_err(cfg):
        return ProcessFlowBuilder.validate_config(cfg)
    def _build_err(cfg):
        try:
            ProcessFlowBuilder.build(cfg, name="P")
            return None
        except BuilderValidationError as exc:
            return exc
    for branch in malformed_branches:
        cfg = _base_config(branch=branch)
        v, b = _vc_err(cfg), _build_err(cfg)
        assert v is not None and b is not None, branch
        assert (v.error_code, v.field) == (b.error_code, b.field), (branch, v.error_code, b.error_code)
    for branch, extra in extra_for_combo:
        cfg = _base_config(branch=branch, **extra)
        v, b = _vc_err(cfg), _build_err(cfg)
        assert v is not None and b is not None, (branch, extra)
        assert (v.error_code, v.field) == (b.error_code, b.field), (branch, extra, v.error_code, b.error_code)


def test_branch_build_bypass_total_on_malformed_targets():
    # build() must stay total on a validate_config bypass with malformed targets —
    # raise BRANCH_OUTPUT_UNSET, never crash (TypeError) or emit a degenerate
    # 1-leg fan-out (numBranches=1).
    for targets in (1, True, [], "x", None, {"k": 1}):
        with pytest.raises(BuilderValidationError) as exc:
            ProcessFlowBuilder.build(
                _base_config(branch={"enabled": True, "targets": targets}), name="P"
            )
        assert exc.value.error_code == "BRANCH_OUTPUT_UNSET", targets


def test_branch_invalid_leg_binding_is_field_scoped():
    err = ProcessFlowBuilder.validate_config(
        _base_config(branch={"enabled": True, "targets": [_branch_leg(connection_id="")]})
    )
    assert err is not None and err.error_code == "PROCESS_CONNECTOR_BINDING_INVALID"
    assert err.field == "branch.targets[0].connection_id"


def test_branch_too_many_legs_rejected():
    err = ProcessFlowBuilder.validate_config(
        _base_config(branch={"enabled": True, "targets": [_branch_leg() for _ in range(25)]})
    )
    assert err is not None and err.error_code == "PROCESS_BRANCH_CONFIG_INVALID"
    assert err.field == "branch.targets"


def test_branch_unknown_key_rejected():
    err = ProcessFlowBuilder.validate_config(
        _base_config(branch={"enabled": True, "targets": [_branch_leg()], "foo": 1})
    )
    assert err is not None and err.error_code == "PROCESS_BRANCH_CONFIG_INVALID"
    assert err.field == "branch"


def test_branch_does_not_compose_with_reliability_in_v1():
    err = ProcessFlowBuilder.validate_config(_base_config(
        reliability={"retry_count": 0, "dlq": {"mode": "document_cache_ref", "document_cache_id": "C"}},
        branch={"enabled": True, "targets": [_branch_leg()]},
    ))
    assert err is not None and err.error_code == "PROCESS_BRANCH_CONFIG_INVALID"
    assert err.field == "reliability"


def test_branch_does_not_compose_with_return_documents_in_v1():
    err = ProcessFlowBuilder.validate_config(_base_config(
        return_documents={"enabled": True},
        branch={"enabled": True, "targets": [_branch_leg()]},
    ))
    assert err is not None and err.error_code == "PROCESS_BRANCH_CONFIG_INVALID"
    assert err.field == "return_documents"


_VALID_DYNAMIC_PATH = {
    "ddp_name": "DDP_PATH",
    "request_profile_id": "p1",
    "segments": [{"type": "profile", "element_id": "e1", "element_name": "id"}],
}


def test_branch_with_dynamic_path_uniformly_rejects_branch_config_invalid():
    # Issue #112 review: dynamic_path + Branch is the unsupported v1 composition, so
    # it is rejected as PROCESS_BRANCH_CONFIG_INVALID — the persistent blocker —
    # whether the dynamic_path is well-formed or malformed (a malformed dynamic_path
    # is moot once you learn it cannot be used with Branch at all).
    for dp in (_VALID_DYNAMIC_PATH, {"x": 1}):
        top = ProcessFlowBuilder.validate_config(_base_config(
            target={**_base_config()["target"], "dynamic_path": dp},
            branch={"enabled": True, "targets": [_branch_leg()]},
        ))
        assert top is not None and top.error_code == "PROCESS_BRANCH_CONFIG_INVALID"
        assert top.field == "target.dynamic_path"
        leg = ProcessFlowBuilder.validate_config(_base_config(
            branch={"enabled": True, "targets": [_branch_leg(dynamic_path=dp)]},
        ))
        assert leg is not None and leg.error_code == "PROCESS_BRANCH_CONFIG_INVALID"
        assert leg.field == "branch.targets[0].dynamic_path"


def test_non_branch_malformed_dynamic_path_still_reports_path_replacement_invalid():
    # The combo reorder must NOT change non-branch validation: a malformed
    # dynamic_path with no Branch stays the specific PROCESS_PATH_REPLACEMENT_INVALID.
    err = ProcessFlowBuilder.validate_config(_base_config(
        target={**_base_config()["target"], "dynamic_path": {"x": 1}},
    ))
    assert err is not None and err.error_code == "PROCESS_PATH_REPLACEMENT_INVALID"
    assert err.field == "target.dynamic_path"


def test_branch_build_bypass_raises_for_unsupported_combo():
    # validate_config-bypass: branch + reliability(Try/Catch) must raise rather than
    # silently dropping the reliability block.
    cfg = _base_config(
        reliability={"retry_count": 2, "dlq": {"mode": "document_cache_ref", "document_cache_id": "C"}},
        branch={"enabled": True, "targets": [_branch_leg()]},
    )
    with pytest.raises(BuilderValidationError) as exc:
        ProcessFlowBuilder.build(cfg, name="P")
    assert exc.value.error_code == "PROCESS_BRANCH_CONFIG_INVALID"


# ---------------------------------------------------------------------------
# Issue #113 M10.9 — Decision (conditional two-path routing) + loops
# ---------------------------------------------------------------------------

_DECISION_CONDITIONAL_GOLDEN = (
    Path(__file__).resolve().parent / "fixtures" / "golden_xml" / "decision_conditional.xml"
)


def _decision_block(**overrides):
    decision = {
        "comparison": "equals",
        "label": "Check Status",
        "left": {
            "value_type": "track",
            "property_id": "dynamicdocument.DDP_STATUS",
            "default_value": "",
            "property_name": "Dynamic Document Property - DDP_STATUS",
        },
        "right": {"value_type": "static", "static_value": "active"},
        "false_notify": "Decision false path: status was not active",
    }
    decision.update(overrides)
    return decision


def _decision_config(decision=None, **overrides):
    return _base_config(decision=decision if decision is not None else _decision_block(), **overrides)


def test_decision_inserts_decision_after_source_with_true_false_legs():
    xml = ProcessFlowBuilder.build(_decision_config(), name="Decision Process")
    _, _, shapes = _parse_process(xml)
    assert [s.attrib["shapetype"] for s in shapes] == [
        "start", "connectoraction", "decision",
        "connectoraction", "stop",   # true leg (top-level target -> stop)
        "message", "stop",           # false leg (false_notify message -> stop)
    ]


def test_decision_shape_attributes_and_comparison():
    xml = ProcessFlowBuilder.build(_decision_config(), name="Decision Process")
    _, _, shapes = _parse_process(xml)
    decision = next(s for s in shapes if s.attrib["shapetype"] == "decision")
    assert decision.attrib["image"] == "decision_icon"
    # Both userlabel (shape) and name (decision) carry the same display value.
    assert decision.attrib["userlabel"] == "Check Status"
    inner = decision.find("configuration/decision")
    assert inner is not None
    assert inner.attrib["comparison"] == "equals"
    assert inner.attrib["name"] == "Check Status"


def test_decision_dragpoints_are_labeled_true_false():
    xml = ProcessFlowBuilder.build(_decision_config(), name="Decision Process")
    _, _, shapes = _parse_process(xml)
    decision = next(s for s in shapes if s.attrib["shapetype"] == "decision")
    dragpoints = decision.findall("dragpoints/dragpoint")
    assert len(dragpoints) == 2
    assert [d.attrib["identifier"] for d in dragpoints] == ["true", "false"]
    assert [d.attrib["text"] for d in dragpoints] == ["True", "False"]
    assert [d.attrib["name"] for d in dragpoints] == ["shape3.dragpoint1", "shape3.dragpoint2"]
    # True leg first shape is shape4 (target); false leg first shape is shape6 (message).
    assert [d.attrib["toShape"] for d in dragpoints] == ["shape4", "shape6"]
    shape_names = {s.attrib["name"] for s in shapes}
    assert all(d.attrib["toShape"] in shape_names for d in dragpoints)


def test_decision_operands_emit_track_and_static():
    xml = ProcessFlowBuilder.build(_decision_config(), name="Decision Process")
    _, _, shapes = _parse_process(xml)
    decision = next(s for s in shapes if s.attrib["shapetype"] == "decision")
    values = decision.findall("configuration/decision/decisionvalue")
    assert [v.attrib["valueType"] for v in values] == ["track", "static"]
    track = values[0].find("trackparameter")
    assert track.attrib["propertyId"] == "dynamicdocument.DDP_STATUS"
    assert track.attrib["defaultValue"] == ""
    assert track.attrib["propertyName"] == "Dynamic Document Property - DDP_STATUS"
    static = values[1].find("staticparameter")
    assert static.attrib["staticproperty"] == "active"


def test_decision_true_leg_carries_top_level_target_binding():
    xml = ProcessFlowBuilder.build(_decision_config(), name="Decision Process")
    _, _, shapes = _parse_process(xml)
    # True leg target is the connectoraction right after the decision (shape4).
    true_target = next(s for s in shapes if s.attrib["name"] == "shape4")
    ca = true_target.find("configuration/connectoraction")
    assert ca.attrib["actionType"] == "POST"
    assert ca.attrib["connectionId"] == _REST_CONN_ID
    assert ca.attrib["operationId"] == _REST_OP_ID


def test_decision_false_notify_routes_through_message_before_stop():
    xml = ProcessFlowBuilder.build(_decision_config(), name="Decision Process")
    _, _, shapes = _parse_process(xml)
    # The false dragpoint targets a Message (shape6), which forwards to a Stop.
    message = next(s for s in shapes if s.attrib["name"] == "shape6")
    assert message.attrib["shapetype"] == "message"
    nxt = message.find("dragpoints/dragpoint")
    assert nxt is not None and nxt.attrib["toShape"] == "shape7"
    stop = next(s for s in shapes if s.attrib["name"] == "shape7")
    assert stop.attrib["shapetype"] == "stop"


def test_decision_empty_static_is_is_empty_check():
    # comparison=equals against an empty static value is the live "is empty" check.
    cfg = _decision_config(_decision_block(
        right={"value_type": "static", "static_value": ""}, false_notify=None,
    ))
    assert ProcessFlowBuilder.validate_config(cfg) is None
    xml = ProcessFlowBuilder.build(cfg, name="Is Empty")
    _, _, shapes = _parse_process(xml)
    decision = next(s for s in shapes if s.attrib["shapetype"] == "decision")
    static = decision.findall("configuration/decision/decisionvalue")[1].find("staticparameter")
    assert static.attrib["staticproperty"] == ""


def test_decision_loop_back_sets_false_dragpoint_backward():
    # false_next names an earlier shape (the source, shape2) -> the false dragpoint
    # loops backward (no false-leg Stop). With no false_notify it points directly.
    cfg = _decision_config(_decision_block(false_notify=None, false_next="shape2"))
    xml = ProcessFlowBuilder.build(cfg, name="Decision Loop")
    _, _, shapes = _parse_process(xml)
    decision = next(s for s in shapes if s.attrib["shapetype"] == "decision")
    dragpoints = decision.findall("dragpoints/dragpoint")
    assert dragpoints[1].attrib["identifier"] == "false"
    assert dragpoints[1].attrib["toShape"] == "shape2"
    # The true leg still terminates forward at its own Stop; no false-leg message/stop.
    assert [s.attrib["shapetype"] for s in shapes] == [
        "start", "connectoraction", "decision", "connectoraction", "stop",
    ]


def test_decision_loop_back_through_message_then_backward():
    # false_notify + false_next: the false leg runs a Message that loops backward
    # (the live shape31 false->shape32->shape27 pattern).
    cfg = _decision_config(_decision_block(false_notify="retrying", false_next="shape2"))
    xml = ProcessFlowBuilder.build(cfg, name="Decision Loop Msg")
    _, _, shapes = _parse_process(xml)
    decision = next(s for s in shapes if s.attrib["shapetype"] == "decision")
    false_dp = decision.findall("dragpoints/dragpoint")[1]
    message = next(s for s in shapes if s.attrib["name"] == false_dp.attrib["toShape"])
    assert message.attrib["shapetype"] == "message"
    assert message.find("dragpoints/dragpoint").attrib["toShape"] == "shape2"
    # No false-leg Stop is emitted (the Message loops back instead).
    assert [s.attrib["shapetype"] for s in shapes].count("stop") == 1


def test_decision_matches_golden_fixture():
    """Byte-exact golden (issue #113 g): the Decision shape, its comparison + two
    decisionvalue operands, the labelled true/false dragpoints, and the per-leg
    target/notify layout are load-bearing and must match byte-for-byte."""
    emitted = ProcessFlowBuilder.build(
        _decision_config(), name="Decision Conditional", folder_name="Golden/Fixtures"
    )
    assert emitted == _DECISION_CONDITIONAL_GOLDEN.read_text()


def test_decision_disabled_is_byte_identical_to_no_decision():
    no_decision = ProcessFlowBuilder.build(_base_config(), name="P")
    disabled = ProcessFlowBuilder.build(
        _base_config(decision=_decision_block(enabled=False)), name="P"
    )
    assert no_decision == disabled


def test_decision_comparison_invalid_raises():
    err = ProcessFlowBuilder.validate_config(_decision_config(_decision_block(comparison="nope")))
    assert err is not None and err.error_code == "PROCESS_DECISION_CONFIG_INVALID"
    assert err.field == "decision.comparison"


def test_decision_missing_left_operand_raises():
    err = ProcessFlowBuilder.validate_config(
        _decision_config({"comparison": "equals", "right": {"value_type": "static", "static_value": "y"}})
    )
    assert err is not None and err.error_code == "PROCESS_DECISION_CONFIG_INVALID"
    assert err.field == "decision.left"


def test_decision_track_missing_property_id_raises():
    err = ProcessFlowBuilder.validate_config(
        _decision_config(_decision_block(left={"value_type": "track"}))
    )
    assert err is not None and err.error_code == "PROCESS_DECISION_CONFIG_INVALID"
    assert err.field == "decision.left.property_id"


def test_decision_static_missing_static_value_raises():
    err = ProcessFlowBuilder.validate_config(
        _decision_config(_decision_block(right={"value_type": "static"}))
    )
    assert err is not None and err.error_code == "PROCESS_DECISION_CONFIG_INVALID"
    assert err.field == "decision.right.static_value"


def test_decision_operand_unknown_value_type_raises():
    err = ProcessFlowBuilder.validate_config(
        _decision_config(_decision_block(left={"value_type": "profile", "property_id": "x"}))
    )
    assert err is not None and err.error_code == "PROCESS_DECISION_CONFIG_INVALID"
    assert err.field == "decision.left.value_type"


def test_decision_unknown_key_rejected():
    err = ProcessFlowBuilder.validate_config(_decision_config(_decision_block(foo=1)))
    assert err is not None and err.error_code == "PROCESS_DECISION_CONFIG_INVALID"
    assert err.field == "decision"


def test_decision_false_next_non_earlier_shape_rejected():
    err = ProcessFlowBuilder.validate_config(_decision_config(_decision_block(false_next="shape9")))
    assert err is not None and err.error_code == "PROCESS_DECISION_CONFIG_INVALID"
    assert err.field == "decision.false_next"


def test_decision_does_not_compose_with_branch_in_v1():
    err = ProcessFlowBuilder.validate_config(_base_config(
        decision=_decision_block(),
        branch={"enabled": True, "targets": [_branch_leg()]},
    ))
    assert err is not None and err.error_code == "PROCESS_DECISION_CONFIG_INVALID"
    assert err.field == "branch"


def test_decision_does_not_compose_with_reliability_in_v1():
    err = ProcessFlowBuilder.validate_config(_base_config(
        decision=_decision_block(),
        reliability={"retry_count": 0, "dlq": {"mode": "document_cache_ref", "document_cache_id": "C"}},
    ))
    assert err is not None and err.error_code == "PROCESS_DECISION_CONFIG_INVALID"
    assert err.field == "reliability"


def test_decision_does_not_compose_with_return_documents_in_v1():
    err = ProcessFlowBuilder.validate_config(_base_config(
        decision=_decision_block(), return_documents={"enabled": True},
    ))
    assert err is not None and err.error_code == "PROCESS_DECISION_CONFIG_INVALID"
    assert err.field == "return_documents"


def test_decision_validate_config_and_build_report_identical_errors():
    # validate_config and build() funnel through ONE decision validator, so for
    # every malformed decision config they MUST report the same (error_code, field).
    malformed = [
        1,                                                       # non-dict decision
        [],                                                      # non-dict decision
        {"enabled": "true", **_decision_block()},                # non-bool enabled
        _decision_block(comparison="nope"),                      # bad comparison
        {"comparison": "equals", "right": {"value_type": "static", "static_value": "y"}},  # missing left
        _decision_block(left={"value_type": "track"}),           # track no property_id
        _decision_block(right={"value_type": "static"}),         # static no static_value
        _decision_block(left={"value_type": "profile", "property_id": "x"}),  # bad value_type
        _decision_block(foo=1),                                  # unknown key
        _decision_block(false_next="shape9"),                    # non-earlier loop target
    ]

    def _vc_err(cfg):
        return ProcessFlowBuilder.validate_config(cfg)

    def _build_err(cfg):
        try:
            ProcessFlowBuilder.build(cfg, name="P")
            return None
        except BuilderValidationError as exc:
            return exc

    for decision in malformed:
        cfg = _base_config(decision=decision)
        v, b = _vc_err(cfg), _build_err(cfg)
        assert v is not None and b is not None, decision
        assert (v.error_code, v.field) == (b.error_code, b.field), (decision, v.error_code, b.error_code)


def test_decision_padded_comparison_emits_canonical_token():
    # Codex #113 review P2: validate_config accepts a padded comparison via
    # comparison.strip() membership, so the emitter must serialize the canonical
    # operator token (no leading/trailing whitespace), never the padded value.
    cfg = _decision_config(_decision_block(comparison=" equals "))
    assert ProcessFlowBuilder.validate_config(cfg) is None
    xml = ProcessFlowBuilder.build(cfg, name="Padded Comparison")
    _, _, shapes = _parse_process(xml)
    decision = next(s for s in shapes if s.attrib["shapetype"] == "decision")
    assert decision.find("configuration/decision").attrib["comparison"] == "equals"


def test_decision_false_next_with_malformed_transform_stays_total():
    # Codex #113 review P2: a decision.false_next alongside a malformed (non-dict)
    # transform must NOT raise AttributeError in _decision_pre_shape_names —
    # validate_config stays total and surfaces the transform's structured error.
    cfg = _base_config(transform=1, decision=_decision_block(false_notify=None, false_next="shape2"))
    err = ProcessFlowBuilder.validate_config(cfg)
    assert err is not None and err.error_code == "PROCESS_SHAPE_UNSUPPORTED"


def test_decision_operands_validated_symmetrically_swapped_orientation():
    # Architect-review #113 (finding 1): operands are validated symmetrically — a
    # static LEFT missing static_value and a track RIGHT missing property_id are
    # both reachable PROCESS_DECISION_CONFIG_INVALID field paths (the swapped
    # orientation the structured_errors row documents).
    left_static_missing = ProcessFlowBuilder.validate_config(_decision_config({
        "comparison": "equals",
        "left": {"value_type": "static"},
        "right": {"value_type": "track", "property_id": "dynamicdocument.DDP_X"},
    }))
    assert left_static_missing is not None
    assert left_static_missing.error_code == "PROCESS_DECISION_CONFIG_INVALID"
    assert left_static_missing.field == "decision.left.static_value"
    right_track_missing = ProcessFlowBuilder.validate_config(_decision_config({
        "comparison": "equals",
        "left": {"value_type": "static", "static_value": "x"},
        "right": {"value_type": "track"},
    }))
    assert right_track_missing is not None
    assert right_track_missing.error_code == "PROCESS_DECISION_CONFIG_INVALID"
    assert right_track_missing.field == "decision.right.property_id"


# ---------------------------------------------------------------------------
# Flow Control (per-document batching) shape (issue #111 M10.7)
# ---------------------------------------------------------------------------

_FLOW_CONTROL_GOLDEN = (
    Path(__file__).resolve().parent
    / "fixtures"
    / "golden_xml"
    / "flow_control_batching.xml"
)


def _flow_control_config(label="Batch by 10", for_each_count=10, **overrides):
    flow_control = {"enabled": True, "for_each_count": for_each_count}
    if label is not None:
        flow_control["label"] = label
    flow_control.update(overrides.pop("flow_control_extra", {}))
    return _base_config(flow_control=flow_control, **overrides)


def test_flow_control_inserts_linear_shape_between_source_and_transform():
    xml = ProcessFlowBuilder.build(_flow_control_config(), name="FlowControl Sync")
    _, _, shapes = _parse_process(xml)
    # The Flow Control shape sits right after the source (before the target), so the
    # whole downstream chain runs per batch. passthrough transform adds no shape.
    assert [s.attrib["shapetype"] for s in shapes] == [
        "start", "connectoraction", "flowcontrol", "connectoraction", "stop",
    ]
    fc = shapes[2]
    assert fc.attrib["image"] == "flowcontrol_icon"
    assert fc.attrib["userlabel"] == "Batch by 10"
    cfg = fc.find("configuration/flowcontrol")
    # Byte-exact attribute model from the live capture: per-document batching.
    assert cfg.attrib["chunkStyle"] == "threadOnly"
    assert cfg.attrib["chunks"] == "0"
    assert cfg.attrib["forEachCount"] == "10"
    # No userdefoptions in the batching mode.
    assert cfg.find("userdefoptions") is None
    # Linear, non-terminal: exactly one forward dragpoint to the next shape.
    dragpoints = fc.find("dragpoints")
    assert [dp.attrib["toShape"] for dp in dragpoints] == ["shape4"]


def test_flow_control_sits_before_transform_shape():
    # With a message transform, the order is start -> source -> flowcontrol ->
    # message -> target -> stop (batch boundary fans the whole downstream chain).
    cfg = _flow_control_config(transform={"mode": "message", "message_text": "hi"})
    xml = ProcessFlowBuilder.build(cfg, name="FlowControl Transform Sync")
    _, _, shapes = _parse_process(xml)
    assert [s.attrib["shapetype"] for s in shapes] == [
        "start", "connectoraction", "flowcontrol", "message", "connectoraction", "stop",
    ]


def test_flow_control_matches_golden_fixture():
    """Byte-exact golden (issue #111 g): the chunkStyle/chunks/forEachCount attribute
    order is load-bearing and must match the live capture byte-for-byte."""
    emitted = ProcessFlowBuilder.build(
        _flow_control_config(), name="FlowControl Batching Sync"
    )
    assert emitted == _FLOW_CONTROL_GOLDEN.read_text()


def test_flow_control_absent_is_byte_identical_to_base():
    # Default (no flow_control block) keeps the pre-#111 flow byte-for-byte.
    base = ProcessFlowBuilder.build(_base_config(), name="N")
    absent = ProcessFlowBuilder.build(_base_config(flow_control=None), name="N")
    assert absent == base


def test_flow_control_disabled_is_byte_identical_to_base():
    base = ProcessFlowBuilder.build(_base_config(), name="N")
    disabled = ProcessFlowBuilder.build(
        _base_config(flow_control={"enabled": False, "for_each_count": 10}), name="N"
    )
    assert disabled == base


def test_flow_control_rejects_missing_for_each_count():
    cfg = _base_config(flow_control={"enabled": True})
    err = ProcessFlowBuilder.validate_config(cfg)
    assert err is not None
    assert err.error_code == "PROCESS_FLOW_CONTROL_CONFIG_INVALID"
    assert err.field == "flow_control.for_each_count"


def test_flow_control_rejects_non_positive_for_each_count():
    for bad in (0, -1):
        cfg = _base_config(flow_control={"enabled": True, "for_each_count": bad})
        err = ProcessFlowBuilder.validate_config(cfg)
        assert err is not None
        assert err.error_code == "PROCESS_FLOW_CONTROL_CONFIG_INVALID"
        assert err.field == "flow_control.for_each_count"


def test_flow_control_rejects_bool_for_each_count():
    # A bool is an int subclass; reject it so flow_control={"for_each_count": True}
    # is not silently treated as forEachCount=1.
    cfg = _base_config(flow_control={"enabled": True, "for_each_count": True})
    err = ProcessFlowBuilder.validate_config(cfg)
    assert err is not None
    assert err.error_code == "PROCESS_FLOW_CONTROL_CONFIG_INVALID"
    assert err.field == "flow_control.for_each_count"


def test_flow_control_rejects_non_int_for_each_count():
    cfg = _base_config(flow_control={"enabled": True, "for_each_count": "10"})
    err = ProcessFlowBuilder.validate_config(cfg)
    assert err is not None
    assert err.error_code == "PROCESS_FLOW_CONTROL_CONFIG_INVALID"
    assert err.field == "flow_control.for_each_count"


def test_flow_control_rejects_non_bool_enabled():
    cfg = _base_config(flow_control={"enabled": 1, "for_each_count": 10})
    err = ProcessFlowBuilder.validate_config(cfg)
    assert err is not None
    assert err.error_code == "PROCESS_FLOW_CONTROL_CONFIG_INVALID"
    assert err.field == "flow_control.enabled"


def test_flow_control_rejects_unknown_key():
    cfg = _base_config(flow_control={"enabled": True, "for_each_count": 10, "bogus": 1})
    err = ProcessFlowBuilder.validate_config(cfg)
    assert err is not None
    assert err.error_code == "PROCESS_FLOW_CONTROL_CONFIG_INVALID"
    assert err.field == "flow_control"


def test_flow_control_rejects_non_dict_block():
    cfg = _base_config(flow_control=1)
    err = ProcessFlowBuilder.validate_config(cfg)
    assert err is not None
    assert err.error_code == "PROCESS_FLOW_CONTROL_CONFIG_INVALID"
    assert err.field == "flow_control"


def test_flow_control_rejects_non_string_label():
    cfg = _base_config(flow_control={"enabled": True, "for_each_count": 10, "label": 1})
    err = ProcessFlowBuilder.validate_config(cfg)
    assert err is not None
    assert err.error_code == "PROCESS_FLOW_CONTROL_CONFIG_INVALID"
    assert err.field == "flow_control.label"


def test_flow_control_rejects_branch_composition():
    # v1 Flow Control does not compose with a Branch fan-out (topology-changing).
    cfg = _flow_control_config()
    cfg["branch"] = {
        "enabled": True,
        "targets": [{
            "connector_type": "rest",
            "connection_id": _REST_CONN_ID,
            "operation_id": _REST_OP_ID,
            "action_type": "POST",
        }],
    }
    err = ProcessFlowBuilder.validate_config(cfg)
    assert err is not None
    assert err.error_code == "PROCESS_FLOW_CONTROL_CONFIG_INVALID"
    assert err.field == "flow_control"


def test_flow_control_rejects_decision_composition():
    # v1 Flow Control does not compose with a Decision route (topology-changing).
    cfg = _flow_control_config()
    cfg["decision"] = {
        "enabled": True,
        "comparison": "equals",
        "left": {"value_type": "track", "property_id": "dynamicdocument.DDP_X"},
        "right": {"value_type": "static", "static_value": "y"},
    }
    err = ProcessFlowBuilder.validate_config(cfg)
    assert err is not None
    assert err.error_code == "PROCESS_FLOW_CONTROL_CONFIG_INVALID"
    assert err.field == "flow_control"


def test_flow_control_xml_escapes_label():
    cfg = _base_config(
        flow_control={"enabled": True, "for_each_count": 5, "label": "A & B <batch>"}
    )
    xml = ProcessFlowBuilder.build(cfg, name="N")
    _, _, shapes = _parse_process(xml)
    fc = next(s for s in shapes if s.attrib["shapetype"] == "flowcontrol")
    assert fc.attrib["userlabel"] == "A & B <batch>"
    assert fc.find("configuration/flowcontrol").attrib["forEachCount"] == "5"


def test_flow_control_build_bypass_invalid_for_each_count_raises():
    # build() stays total on the validate_config-bypass path: a non-positive
    # forEachCount would emit a semantically broken batch size, so raise.
    cfg = _base_config(flow_control={"enabled": True, "for_each_count": 0})
    with pytest.raises(BuilderValidationError) as exc:
        ProcessFlowBuilder.build(cfg, name="N")
    assert exc.value.error_code == "PROCESS_FLOW_CONTROL_CONFIG_INVALID"
    assert exc.value.field == "flow_control.for_each_count"


def test_flow_control_build_bypass_branch_composition_raises():
    # build() funnels through the same validator: a flow_control + branch combo
    # raises before any branch emission path is taken (totality).
    cfg = _flow_control_config()
    cfg["branch"] = {
        "enabled": True,
        "targets": [{
            "connector_type": "rest",
            "connection_id": _REST_CONN_ID,
            "operation_id": _REST_OP_ID,
            "action_type": "POST",
        }],
    }
    with pytest.raises(BuilderValidationError) as exc:
        ProcessFlowBuilder.build(cfg, name="N")
    assert exc.value.error_code == "PROCESS_FLOW_CONTROL_CONFIG_INVALID"


def test_flow_control_composes_with_reliability_try_catch():
    # Flow Control is a plain linear shape; it sits inside the Try/Catch wrapper just
    # like the transform shapes do (same mechanism), so the combo emits cleanly.
    cfg = _flow_control_config(
        reliability={"retry_count": 1, "dlq": {"mode": "error_subprocess_ref", "process_id": "PROC-1"}},
    )
    err = ProcessFlowBuilder.validate_config(cfg, depends_on=["PROC-1"])
    assert err is None
    xml = ProcessFlowBuilder.build(cfg, name="FlowControl Reliable Sync")
    _, _, shapes = _parse_process(xml)
    shapetypes = [s.attrib["shapetype"] for s in shapes]
    assert "flowcontrol" in shapetypes
    assert "catcherrors" in shapetypes


# ---------------------------------------------------------------------------
# Issue #96 M5.4a — runtime_bindings gating + Branch/Decision composition
# ---------------------------------------------------------------------------


_RB = [{"location": "query_parameter", "slot": "x",
        "source": {"kind": "static", "value": "1"}}]


def test_target_runtime_bindings_rejected_unverified():
    # The builder emits a path binding via dynamic_path; a raw runtime_bindings
    # block (query/header/DDP/DPP) is gated, never silently dropped.
    err = ProcessFlowBuilder.validate_config(
        _base_config(target={**_base_config()["target"], "runtime_bindings": _RB})
    )
    assert err is not None and err.error_code == "PROCESS_RUNTIME_BINDING_UNVERIFIED"
    assert err.field == "target.runtime_bindings"


def test_source_runtime_bindings_rejected_unverified():
    rest_source = {
        "connector_type": "rest",
        "connection_id": _REST_CONN_ID,
        "operation_id": _REST_OP_ID,
        "action_type": "GET",
        "runtime_bindings": _RB,
    }
    err = ProcessFlowBuilder.validate_config(
        _base_config(source=rest_source), allow_rest_source=True
    )
    assert err is not None and err.error_code == "PROCESS_RUNTIME_BINDING_UNVERIFIED"
    assert err.field == "source.runtime_bindings"


def test_runtime_bindings_rejected_under_branch():
    err = ProcessFlowBuilder.validate_config(
        _branch_config(target={**_base_config()["target"], "runtime_bindings": _RB})
    )
    assert err is not None and err.error_code == "PROCESS_BRANCH_CONFIG_INVALID"
    assert err.field == "target.runtime_bindings"


def test_branch_leg_runtime_bindings_rejected():
    err = ProcessFlowBuilder.validate_config(
        _branch_config(targets=[_branch_leg(runtime_bindings=_RB)])
    )
    assert err is not None and err.error_code == "PROCESS_BRANCH_CONFIG_INVALID"
    assert err.field == "branch.targets[0].runtime_bindings"


def test_runtime_bindings_rejected_under_decision():
    err = ProcessFlowBuilder.validate_config(
        _decision_config(target={**_base_config()["target"], "runtime_bindings": _RB})
    )
    assert err is not None and err.error_code == "PROCESS_DECISION_CONFIG_INVALID"
    assert err.field == "target.runtime_bindings"


def test_runtime_bindings_build_bypass_raises_under_branch():
    # validate_config-bypass: build() funnels through the same branch guard, so a
    # runtime_bindings + Branch composition raises rather than silently dropping.
    cfg = _branch_config(target={**_base_config()["target"], "runtime_bindings": _RB})
    with pytest.raises(BuilderValidationError) as exc:
        ProcessFlowBuilder.build(cfg, name="P")
    assert exc.value.error_code == "PROCESS_BRANCH_CONFIG_INVALID"


def _rest_source_with_dynamic_path():
    return {
        "connector_type": "rest",
        "connection_id": _REST_CONN_ID,
        "operation_id": _REST_OP_ID,
        "action_type": "GET",
        "dynamic_path": _VALID_DYNAMIC_PATH,
    }


_CONNECTOR_SCOPE_RELIABILITY = {
    "retry_count": 1,
    "try_catch_scope": "connector",
    "dlq": {"mode": "error_subprocess_ref", "process_id": "ERRSUB"},
}


def test_source_dynamic_path_rejected_with_connector_scoped_try_catch():
    # The connector-scoped emitter assumes flow[1] is the source connector; a source
    # Set Properties (from a source path runtime binding) would mis-wrap it. Reject.
    cfg = _base_config(
        source=_rest_source_with_dynamic_path(),
        reliability=_CONNECTOR_SCOPE_RELIABILITY,
    )
    err = ProcessFlowBuilder.validate_config(cfg, allow_rest_source=True)
    assert err is not None and err.error_code == "PROCESS_RUNTIME_BINDING_UNVERIFIED"
    assert err.field == "source.dynamic_path"
    # build() funnels through the same guard for totality on a validate_config bypass.
    with pytest.raises(BuilderValidationError) as exc:
        ProcessFlowBuilder.build(cfg, name="P")
    assert exc.value.error_code == "PROCESS_RUNTIME_BINDING_UNVERIFIED"


def test_source_dynamic_path_allowed_with_process_scoped_try_catch():
    # The whole-process Try/Catch wraps the entire chain (flow[1:]), so a pre-source
    # Set Properties is inside the wrap — the composition is allowed.
    cfg = _base_config(
        source=_rest_source_with_dynamic_path(),
        reliability={
            "retry_count": 1,
            "try_catch_scope": "process",
            "dlq": {"mode": "error_subprocess_ref", "process_id": "ERRSUB"},
        },
    )
    assert ProcessFlowBuilder.validate_config(cfg, allow_rest_source=True) is None
    xml = ProcessFlowBuilder.build(cfg, name="P")
    assert "documentproperties" in xml  # source path Set Properties emitted


def test_dynamic_path_profile_ref_requires_depends_on():
    # A dynamic_path request_profile_id that is a $ref must be reachable via
    # depends_on (the primitive now adds it to the fragment deps, #96 review).
    dyn = {
        "ddp_name": "DDP_PATH",
        "request_profile_id": "$ref:req_profile",
        "segments": [{"type": "profile", "element_id": "e1", "element_name": "id"}],
    }
    cfg = _base_config(target={**_base_config()["target"], "dynamic_path": dyn})
    assert ProcessFlowBuilder.validate_config(cfg, depends_on=[]) is not None
    assert ProcessFlowBuilder.validate_config(cfg, depends_on=["req_profile"]) is None


# ---------------------------------------------------------------------------
# Issue #117 M10 follow-up — multi-control-shape composition (flow_sequence)
# ---------------------------------------------------------------------------

_REST_B_CONN_ID = "55555555-5555-5555-5555-555555555555"
_REST_B_OP_ID = "66666666-6666-6666-6666-666666666666"
_SEQ_GROOVY = "dataContext.storeStream(is, props);"

_FLOW_SEQ_DECISION_BRANCH_GOLDEN = _GOLDEN_DIR / "flow_sequence_decision_branch_map.xml"
_FLOW_SEQ_CACHE_CRUD_GOLDEN = _GOLDEN_DIR / "flow_sequence_cache_load_retrieve_remove.xml"
_FLOW_SEQ_EXCEPTION_GOLDEN = _GOLDEN_DIR / "flow_sequence_exception_terminal.xml"


def _rest_target(conn=_REST_CONN_ID, op=_REST_OP_ID, label="t", verb="POST"):
    return {
        "connector_type": "rest",
        "connection_id": conn,
        "operation_id": op,
        "action_type": verb,
        "label": label,
    }


def _seq_config(flow_sequence, **overrides):
    return _base_config(flow_sequence=flow_sequence, **overrides)


def _decision_branch_config():
    """The canonical acceptance graph: Decision + Data Process on the true leg
    (-> top-level target) and a Branch whose legs each carry a Map (issue #117)."""
    return _seq_config(
        [
            {
                "kind": "decision",
                "comparison": "equals",
                "left": {"value_type": "track", "property_id": "dynamicdocument.DDP_STATUS"},
                "right": {"value_type": "static", "static_value": "ACTIVE"},
                "label": "Status check",
                "true_steps": [
                    {
                        "kind": "dataprocess",
                        "label": "Tag",
                        "steps": [{"operation": "custom_scripting", "script": _SEQ_GROOVY}],
                    }
                ],
                "false_steps": [
                    {
                        "kind": "branch",
                        "legs": [
                            {
                                "steps": [{"kind": "map_ref", "map_ref": "MAP-A", "label": "Map A"}],
                                "target": _rest_target(label="Leg A"),
                            },
                            {
                                "steps": [{"kind": "map_ref", "map_ref": "MAP-B", "label": "Map B"}],
                                "target": _rest_target(_REST_B_CONN_ID, _REST_B_OP_ID, "Leg B"),
                            },
                        ],
                    }
                ],
            }
        ]
    )


def _cache_crud_config():
    return _seq_config(
        [
            {"kind": "doccacheload", "document_cache_id": "CACHE-1", "label": "Add to cache"},
            {"kind": "doccacheretrieve", "document_cache_id": "CACHE-1", "label": "Read cache"},
            {"kind": "doccacheremove", "document_cache_id": "CACHE-1", "label": "Clear cache"},
        ]
    )


def _exception_terminal_config():
    return _seq_config(
        [
            {"kind": "message", "message_text": "processing", "label": "Log"},
            {
                "kind": "exception",
                "title": "Halt",
                "message_template": "halted: {1}",
                "parameter_source": "caught_error",
            },
        ]
    )


def _edges(shapes):
    """Map each shape name -> ordered list of dragpoint toShape targets."""
    out = {}
    for s in shapes:
        dps = s.find("dragpoints")
        targets = []
        if dps is not None:
            for dp in dps.findall("dragpoint"):
                t = dp.get("toShape")
                if t:
                    targets.append(t)
        out[s.attrib["name"]] = targets
    return out


# --- acceptance bullet 1: Decision + downstream + Branch leg carrying a Map ---

def test_flow_sequence_decision_true_dataprocess_then_branch_map_legs():
    cfg = _decision_branch_config()
    assert ProcessFlowBuilder.validate_config(cfg, depends_on=[]) is None
    xml = ProcessFlowBuilder.build(cfg, name="Composed")
    _, _, shapes = _parse_process(xml)
    by_name = {s.attrib["name"]: s for s in shapes}
    types = {s.attrib["name"]: s.attrib["shapetype"] for s in shapes}
    edges = _edges(shapes)
    # The composed graph contains a decision, a dataprocess on the true leg, a
    # branch with two map-carrying legs, three connector targets and three stops.
    assert sorted(types.values()) == sorted(
        [
            "start", "connectoraction", "decision", "dataprocess", "connectoraction",
            "stop", "branch", "map", "connectoraction", "stop", "map", "connectoraction", "stop",
        ]
    )
    decision = next(s for s in shapes if s.attrib["shapetype"] == "decision")
    true_to, false_to = edges[decision.attrib["name"]]
    assert types[true_to] == "dataprocess"          # true leg starts with Data Process
    assert types[edges[true_to][0]] == "connectoraction"  # ... then the top-level target
    branch = next(s for s in shapes if s.attrib["shapetype"] == "branch")
    assert false_to == branch.attrib["name"]        # false leg routes into the branch
    assert branch.find("configuration/branch").attrib["numBranches"] == "2"
    leg_firsts = edges[branch.attrib["name"]]
    assert len(leg_firsts) == 2
    for leg_first in leg_firsts:
        assert types[leg_first] == "map"            # each branch leg carries a Map
        target = edges[leg_first][0]
        assert types[target] == "connectoraction"   # ... then its own target
        assert types[edges[target][0]] == "stop"    # ... then its own Stop


def test_flow_sequence_decision_branch_map_matches_golden_fixture():
    emitted = ProcessFlowBuilder.build(_decision_branch_config(), name="Flow Sequence Decision Branch Map")
    assert emitted == _FLOW_SEQ_DECISION_BRANCH_GOLDEN.read_text()


def test_flow_sequence_decision_branch_map_verifies_clean():
    from src.boomi_mcp.categories.components.process_graph_verifier import verify_process_graph

    xml = ProcessFlowBuilder.build(_decision_branch_config(), name="Composed")
    result = verify_process_graph(xml)
    assert result["errors"] == []
    assert result["warnings"] == []


# --- acceptance bullet 2: cache load -> retrieve -> remove ---

def test_flow_sequence_cache_load_retrieve_remove():
    cfg = _cache_crud_config()
    assert ProcessFlowBuilder.validate_config(cfg, depends_on=[]) is None
    xml = ProcessFlowBuilder.build(cfg, name="Cache CRUD")
    _, _, shapes = _parse_process(xml)
    assert [s.attrib["shapetype"] for s in shapes] == [
        "start", "connectoraction", "doccacheload", "doccacheretrieve",
        "doccacheremove", "connectoraction", "stop",
    ]
    load = next(s for s in shapes if s.attrib["shapetype"] == "doccacheload")
    # Add-to-Cache sits on the MAIN row (not the catch row) and forwards.
    assert load.attrib["y"] == "96.0"
    assert load.attrib["userlabel"] == "Add to cache"
    assert load.find("configuration/doccacheload").attrib["docCache"] == "CACHE-1"
    dp = load.find("dragpoints/dragpoint")
    assert dp is not None and dp.get("toShape")  # non-terminal: has a forward edge


def test_flow_sequence_cache_load_retrieve_remove_matches_golden_fixture():
    emitted = ProcessFlowBuilder.build(_cache_crud_config(), name="Flow Sequence Cache Load Retrieve Remove")
    assert emitted == _FLOW_SEQ_CACHE_CRUD_GOLDEN.read_text()


# --- exception terminal ---

def test_flow_sequence_exception_terminal():
    cfg = _exception_terminal_config()
    assert ProcessFlowBuilder.validate_config(cfg, depends_on=[]) is None
    xml = ProcessFlowBuilder.build(cfg, name="Exc")
    _, _, shapes = _parse_process(xml)
    assert [s.attrib["shapetype"] for s in shapes] == [
        "start", "connectoraction", "message", "exception",
    ]
    exc = next(s for s in shapes if s.attrib["shapetype"] == "exception")
    assert exc.attrib["y"] == "96.0"               # main-row terminal, not the catch row
    assert exc.find("dragpoints") is not None and len(exc.find("dragpoints")) == 0  # terminal


def test_flow_sequence_exception_terminal_matches_golden_fixture():
    emitted = ProcessFlowBuilder.build(_exception_terminal_config(), name="Flow Sequence Exception Terminal")
    assert emitted == _FLOW_SEQ_EXCEPTION_GOLDEN.read_text()


def test_flow_sequence_return_documents_linear_terminal():
    cfg = _seq_config(
        [{"kind": "map_ref", "map_ref": "MAP-1"}],
        return_documents={"enabled": True, "label": "out"},
    )
    assert ProcessFlowBuilder.validate_config(cfg, depends_on=[]) is None
    xml = ProcessFlowBuilder.build(cfg, name="RD")
    _, _, shapes = _parse_process(xml)
    assert [s.attrib["shapetype"] for s in shapes] == [
        "start", "connectoraction", "map", "returndocuments",
    ]


# --- byte-stability: a legacy (no flow_sequence) config is unchanged ---

def test_legacy_single_shape_unchanged_when_flow_sequence_absent():
    # The early-return gate is keyed on flow_sequence presence, so a config without
    # it takes the exact pre-#117 single-shape path. Spot-check the passthrough and
    # a branch fan-out still emit their established shape graphs.
    passthrough = ProcessFlowBuilder.build(_base_config(), name="Legacy")
    _, _, shapes = _parse_process(passthrough)
    assert [s.attrib["shapetype"] for s in shapes] == [
        "start", "connectoraction", "connectoraction", "stop",
    ]
    branch_cfg = _base_config(branch={"enabled": True, "targets": [_rest_target(label="Leg2")]})
    _, _, b_shapes = _parse_process(ProcessFlowBuilder.build(branch_cfg, name="Legacy Branch"))
    assert "branch" in [s.attrib["shapetype"] for s in b_shapes]


# --- negatives (sequence STRUCTURE -> PROCESS_FLOW_SEQUENCE_CONFIG_INVALID) ---

def _seq_err(flow_sequence, **overrides):
    return ProcessFlowBuilder.validate_config(_seq_config(flow_sequence, **overrides), depends_on=[])


def test_flow_sequence_rejects_empty_list():
    err = _seq_err([])
    assert err is not None and err.error_code == "PROCESS_FLOW_SEQUENCE_CONFIG_INVALID"


def test_flow_sequence_rejects_unknown_kind():
    err = _seq_err([{"kind": "teleport"}])
    assert err.error_code == "PROCESS_FLOW_SEQUENCE_CONFIG_INVALID"
    assert err.field == "flow_sequence[0].kind"


def test_flow_sequence_rejects_unknown_step_key():
    err = _seq_err([{"kind": "map_ref", "map_ref": "M", "bogus": 1}])
    assert err.error_code == "PROCESS_FLOW_SEQUENCE_CONFIG_INVALID"


def test_flow_sequence_rejects_control_not_last():
    err = _seq_err(
        [
            {"kind": "branch", "legs": [{"target": _rest_target()}, {"target": _rest_target()}]},
            {"kind": "message", "message_text": "after"},
        ]
    )
    assert err.error_code == "PROCESS_FLOW_SEQUENCE_CONFIG_INVALID"
    assert err.field == "flow_sequence[0]"


def test_flow_sequence_rejects_branch_too_few_legs():
    err = _seq_err([{"kind": "branch", "legs": [{"target": _rest_target()}]}])
    assert err.error_code == "PROCESS_FLOW_SEQUENCE_CONFIG_INVALID"
    assert err.field == "flow_sequence[0].legs"


def test_flow_sequence_rejects_branch_too_many_legs():
    legs = [{"target": _rest_target()} for _ in range(26)]
    err = _seq_err([{"kind": "branch", "legs": legs}])
    assert err.error_code == "PROCESS_FLOW_SEQUENCE_CONFIG_INVALID"


def test_flow_sequence_rejects_decision_empty_false_steps():
    err = _seq_err(
        [
            {
                "kind": "decision",
                "comparison": "equals",
                "left": {"value_type": "static", "static_value": "a"},
                "right": {"value_type": "static", "static_value": "b"},
                "false_steps": [],
            }
        ]
    )
    assert err.error_code == "PROCESS_FLOW_SEQUENCE_CONFIG_INVALID"
    assert err.field == "flow_sequence[0].false_steps"


def test_flow_sequence_rejects_nested_decision_in_decision_leg():
    inner = {
        "kind": "decision",
        "comparison": "equals",
        "left": {"value_type": "static", "static_value": "a"},
        "right": {"value_type": "static", "static_value": "b"},
        "false_steps": [{"kind": "message", "message_text": "x"}],
    }
    err = _seq_err(
        [
            {
                "kind": "decision",
                "comparison": "equals",
                "left": {"value_type": "static", "static_value": "a"},
                "right": {"value_type": "static", "static_value": "b"},
                "false_steps": [inner],
            }
        ]
    )
    assert err.error_code == "PROCESS_FLOW_SEQUENCE_CONFIG_INVALID"


def test_flow_sequence_rejects_doccacheload_missing_cache_id():
    err = _seq_err([{"kind": "doccacheload"}])
    assert err.error_code == "PROCESS_FLOW_SEQUENCE_CONFIG_INVALID"
    assert err.field == "flow_sequence[0].document_cache_id"


def test_flow_sequence_rejects_branch_leg_in_branch_leg():
    # branch legs are linear-only in v1 (no nested control)
    err = _seq_err(
        [
            {
                "kind": "branch",
                "legs": [
                    {
                        "steps": [
                            {"kind": "branch", "legs": [{"target": _rest_target()}, {"target": _rest_target()}]}
                        ],
                        "target": _rest_target(),
                    },
                    {"target": _rest_target()},
                ],
            }
        ]
    )
    assert err.error_code == "PROCESS_FLOW_SEQUENCE_CONFIG_INVALID"


def test_flow_sequence_rejects_sibling_transform():
    err = _seq_err([{"kind": "map_ref", "map_ref": "M"}], transform={"mode": "message", "message_text": "x"})
    assert err.error_code == "PROCESS_FLOW_SEQUENCE_CONFIG_INVALID"
    assert err.field == "transform"


def test_flow_sequence_rejects_sibling_branch_block():
    err = _seq_err([{"kind": "map_ref", "map_ref": "M"}], branch={"enabled": True, "targets": [_rest_target()]})
    assert err.error_code == "PROCESS_FLOW_SEQUENCE_CONFIG_INVALID"
    assert err.field == "branch"


def test_flow_sequence_rejects_sibling_decision_block():
    decision = {
        "comparison": "equals",
        "left": {"value_type": "static", "static_value": "a"},
        "right": {"value_type": "static", "static_value": "b"},
        "false_notify": "x",
    }
    err = _seq_err([{"kind": "map_ref", "map_ref": "M"}], decision=decision)
    assert err.error_code == "PROCESS_FLOW_SEQUENCE_CONFIG_INVALID"
    assert err.field == "decision"


def test_flow_sequence_rejects_sibling_flow_control_block():
    err = _seq_err([{"kind": "map_ref", "map_ref": "M"}], flow_control={"enabled": True, "for_each_count": 5})
    assert err.error_code == "PROCESS_FLOW_SEQUENCE_CONFIG_INVALID"
    assert err.field == "flow_control"


def test_flow_sequence_rejects_sibling_try_catch_reliability():
    reliability = {"retry_count": 1, "dlq": {"mode": "document_cache_ref", "document_cache_id": "C"}}
    err = _seq_err([{"kind": "map_ref", "map_ref": "M"}], reliability=reliability)
    assert err.error_code == "PROCESS_FLOW_SEQUENCE_CONFIG_INVALID"
    assert err.field == "reliability"


def test_flow_sequence_rejects_return_documents_with_control():
    err = _seq_err(
        [{"kind": "exception", "title": "x", "message_template": "y", "parameter_source": "none"}],
        return_documents={"enabled": True},
    )
    assert err.error_code == "PROCESS_FLOW_SEQUENCE_CONFIG_INVALID"
    assert err.field == "return_documents"


def test_flow_sequence_rejects_source_dynamic_path():
    src = {**_base_config()["source"], "dynamic_path": {"ddp_name": "x", "segments": [{"type": "ddp", "property_name": "p"}]}}
    err = ProcessFlowBuilder.validate_config(
        _seq_config([{"kind": "map_ref", "map_ref": "M"}], source=src), depends_on=[]
    )
    assert err.error_code == "PROCESS_FLOW_SEQUENCE_CONFIG_INVALID"
    assert err.field == "source.dynamic_path"


def test_flow_sequence_rejects_branch_leg_dynamic_path():
    leg = {"steps": [], "target": {**_rest_target(), "dynamic_path": {"ddp_name": "x", "segments": [{"type": "ddp", "property_name": "p"}]}}}
    err = _seq_err([{"kind": "branch", "legs": [leg, {"target": _rest_target()}]}])
    assert err.error_code == "PROCESS_FLOW_SEQUENCE_CONFIG_INVALID"


# --- step BODY errors reuse the per-shape config-invalid codes ---

def test_flow_sequence_dataprocess_body_error_uses_dataprocess_code():
    err = _seq_err([{"kind": "dataprocess", "steps": []}])
    assert err.error_code == "PROCESS_DATAPROCESS_CONFIG_INVALID"


def test_flow_sequence_decision_operand_error_uses_decision_code():
    err = _seq_err(
        [
            {
                "kind": "decision",
                "comparison": "equals",
                "left": {"value_type": "track"},  # missing property_id
                "right": {"value_type": "static", "static_value": "b"},
                "false_steps": [{"kind": "message", "message_text": "x"}],
            }
        ]
    )
    assert err.error_code == "PROCESS_DECISION_CONFIG_INVALID"


def test_flow_sequence_branch_leg_binding_error_uses_connector_code():
    err = _seq_err(
        [{"kind": "branch", "legs": [{"target": {"connector_type": "rest"}}, {"target": _rest_target()}]}]
    )
    assert err.error_code == "PROCESS_CONNECTOR_BINDING_INVALID"


def test_flow_sequence_exception_body_error_uses_exception_code():
    err = _seq_err([{"kind": "exception", "message_template": "no placeholder", "parameter_source": "caught_error"}])
    assert err.error_code == "PROCESS_EXCEPTION_CONFIG_INVALID"


# --- build() totality on a validate_config bypass ---

def test_flow_sequence_build_bypass_empty_sequence_raises():
    with pytest.raises(BuilderValidationError) as exc:
        ProcessFlowBuilder.build(_seq_config([]), name="X")
    assert exc.value.error_code == "PROCESS_FLOW_SEQUENCE_CONFIG_INVALID"


def test_flow_sequence_build_bypass_sibling_branch_raises():
    cfg = _seq_config([{"kind": "map_ref", "map_ref": "M"}], branch={"enabled": True, "targets": [_rest_target()]})
    with pytest.raises(BuilderValidationError) as exc:
        ProcessFlowBuilder.build(cfg, name="X")
    assert exc.value.error_code == "PROCESS_FLOW_SEQUENCE_CONFIG_INVALID"


# --- ref reachability: nested $ref tokens must be declared in depends_on ---

def test_flow_sequence_nested_ref_requires_depends_on():
    cfg = _seq_config(
        [
            {
                "kind": "branch",
                "legs": [
                    {"steps": [{"kind": "map_ref", "map_ref": "$ref:legmap"}], "target": _rest_target()},
                    {"target": _rest_target()},
                ],
            }
        ]
    )
    assert ProcessFlowBuilder.validate_config(cfg, depends_on=[]) is not None
    assert ProcessFlowBuilder.validate_config(cfg, depends_on=["legmap"]) is None


def test_flow_sequence_source_dynamic_path_validate_build_parity():
    # QA #142 regression: a malformed source.dynamic_path on a flow_sequence config
    # must yield the SAME structured error from validate_config and build() (the
    # flow_sequence composition guard fires before the generic dynamic_path-shape
    # check on BOTH paths).
    src = {**_base_config()["source"], "dynamic_path": {"ddp_name": "", "segments": []}}
    cfg = _seq_config([{"kind": "map_ref", "map_ref": "M"}], source=src)
    v = ProcessFlowBuilder.validate_config(cfg, depends_on=[])
    assert v is not None and v.error_code == "PROCESS_FLOW_SEQUENCE_CONFIG_INVALID"
    with pytest.raises(BuilderValidationError) as exc:
        ProcessFlowBuilder.build(cfg, name="X")
    assert exc.value.error_code == v.error_code


def test_flow_sequence_build_bypass_malformed_source_binding_raises():
    # build()'s composed path validates the source binding (totality on a
    # validate_config bypass) — a missing source connection_id raises.
    bad_source = {"connector_type": "database", "operation_id": "o", "action_type": "Get"}
    cfg = _seq_config([{"kind": "map_ref", "map_ref": "M"}], source=bad_source)
    with pytest.raises(BuilderValidationError) as exc:
        ProcessFlowBuilder.build(cfg, name="X")
    assert exc.value.error_code == "PROCESS_CONNECTOR_BINDING_INVALID"
