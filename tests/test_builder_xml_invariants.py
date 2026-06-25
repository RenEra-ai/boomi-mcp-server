"""Issue #82 (M9.6) — builder XML invariant audit, locked as executable docs.

Roughly half of the Companion's silent-failure catalog is XML-shape invariants
a typed compiler can make impossible by construction. Our builders were grown
from live exports and already satisfy most of them, but none was *asserted* —
so a future builder change could silently regress one. This module pins every
applicable invariant against PARSED builder output (the dominant builder-test
convention — focused ``ET`` attribute assertions; the heavier per-shape golden
fixtures under ``tests/fixtures/golden_xml/`` continue to lock whole-shape byte
output elsewhere) and records the disposition of every catalog item in the
``INVARIANT_DISPOSITIONS`` table below.

Disposition vocabulary (issue #82):
  * ``guaranteed-by-construction`` — the typed builder already emits it; this
    module adds the regression lock.
  * ``fixed-here`` — an audit gap closed in this issue (the only behavioral
    change M9.6 ships is the non-blocking script lints, see test_script_lints).
  * ``not-applicable-yet`` — no typed builder emits this shape yet (e.g.
    Salesforce, listener, flat-file, branch); table-only until the builder
    lands and inherits this checklist.
  * ``disputed-owned-elsewhere`` — the REST request/response ProfileType
    attributes: the Companion says never-emit, but this account's live exports
    carry them on working operations and our builder emits them by design.
    Disposition is owned by #50 (M5.5); this module only PINS current emission,
    it never flips it.

Verification rule (issue #82, 2026-06-10): the Companion catalog is the
checklist *source*, not evidence. Each lock here pins our own builder output;
invariants are asserted at the level the live evidence supports (attribute
PRESENCE for map FunctionStep, since values vary per function).

Run with ``PYTHONPATH=src pytest`` (bare ``boomi_mcp`` imports — the
editable-install ``.pth`` is stale).
"""

from __future__ import annotations

from typing import Any, Dict, List
import xml.etree.ElementTree as ET

from boomi_mcp.categories.components.builders.connector_builder import (
    RestClientOperationBuilder,
)
from boomi_mcp.categories.components.component_update_preservation import (
    merge_for_update,
)
from boomi_mcp.categories.components.builders.json_profile_builder import (
    JSONGeneratedProfileBuilder,
)
from boomi_mcp.categories.components.builders.map_builder import MapFunctionBuilder
from boomi_mcp.categories.components.builders.process_flow_builder import (
    ProcessFlowBuilder,
)
from boomi_mcp.categories.components.builders.profile_builder import (
    DatabaseReadProfileBuilder,
)
from boomi_mcp.categories.components.builders.xml_profile_builder import (
    XMLGeneratedProfileBuilder,
)


NS = {"bns": "http://api.platform.boomi.com/"}

_DB_CONN_ID = "11111111-1111-1111-1111-111111111111"
_DB_OP_ID = "22222222-2222-2222-2222-222222222222"
_REST_CONN_ID = "33333333-3333-3333-3333-333333333333"
_REST_OP_ID = "44444444-4444-4444-4444-444444444444"
_CACHE_ID = "55555555-5555-5555-5555-555555555555"
_NOTIFY_TOKEN = "meta.base.catcherrorsmessage"


# ---------------------------------------------------------------------------
# Config fixtures (mirror the existing per-builder unit tests)
# ---------------------------------------------------------------------------


def _process_config(**overrides: Any) -> Dict[str, Any]:
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


def _dynamic_path_config() -> Dict[str, Any]:
    return _process_config(
        transform={"mode": "map_ref", "map_ref": "MAP-UUID"},
        target={
            "connector_type": "rest",
            "action_type": "PATCH",
            "connection_id": _REST_CONN_ID,
            "operation_id": _REST_OP_ID,
            "dynamic_path": {
                "ddp_name": "DDP_PATH_CLIENTS",
                "request_profile_id": "PROFILE-UUID",
                "profile_type": "profile.json",
                "segments": [
                    {"type": "static", "value": "/api/v1/clients/"},
                    {
                        "type": "profile",
                        "element_id": 3,
                        "element_name": "clientId (Root/Object/clientId)",
                    },
                ],
            },
        },
    )


def _notify_config() -> Dict[str, Any]:
    return _process_config(
        reliability={
            "retry_count": 2,
            "dlq": {"mode": "document_cache_ref", "document_cache_id": _CACHE_ID},
            "catch_notify": {
                "level": "ERROR",
                # apostrophe (MessageFormat escape) + caught-error token, so the
                # emitter must double the quote and substitute the token for {1}.
                "message_template": f"It's broken. Caught: {_NOTIFY_TOKEN}",
            },
        }
    )


def _xml_profile_config() -> Dict[str, Any]:
    return {
        "component_type": "profile.xml",
        "profile_type": "xml.generated",
        "component_name": "XML Source",
        "root": {
            "name": "rows",
            "kind": "element",
            "children": [
                {"name": "row", "kind": "element", "max_occurs": -1, "children": [
                    {"name": "name", "kind": "element", "data_type": "character"},
                    {"name": "amount", "kind": "element", "data_type": "number"},
                    {"name": "tax", "kind": "element", "data_type": "number"},
                ]},
            ],
        },
    }


def _json_profile_config() -> Dict[str, Any]:
    return {
        "component_type": "profile.json",
        "profile_type": "json.generated",
        "component_name": "JSON Target",
        "root": {
            "name": "Root",
            "kind": "object",
            "children": [
                {"name": "list", "kind": "array", "children": [
                    {"name": "name", "kind": "simple", "data_type": "character"},
                    {"name": "total", "kind": "simple", "data_type": "number"},
                ]},
            ],
        },
    }


def _function_map_config(**overrides: Any) -> Dict[str, Any]:
    cfg: Dict[str, Any] = {
        "component_type": "transform.map",
        "map_type": "function",
        "component_name": "XML to JSON Function Map",
        "source_profile_id": "aaaaaaaa-1111-1111-1111-111111111111",
        "source_profile_type": "profile.xml",
        "target_profile_id": "bbbbbbbb-2222-2222-2222-222222222222",
        "target_profile_type": "profile.json",
        "function_mappings": [
            {
                "function_type": "math",
                "inputs": ["rows/row[]/amount", "rows/row[]/tax"],
                "target_path": "Root/list[]/total",
                "parameters": {"operation": "add"},
            },
        ],
    }
    cfg.update(overrides)
    return cfg


def _rest_get_config(**overrides: Any) -> Dict[str, Any]:
    params = {
        "component_type": "connector-action",
        "connector_type": "rest",
        "operation_mode": "execute",
        "component_name": "Read Target Record",
        "connection_ref_key": "target_rest_connection",
        "method": "GET",
        "path": "/v1/items/42",
    }
    params.update(overrides)
    return params


def _parse_process_shapes(xml: str) -> List[ET.Element]:
    root = ET.fromstring(xml)
    shapes = root.find("bns:object/process/shapes", NS)
    assert shapes is not None
    return list(shapes.findall("shape"))


def _build_function_map_xml() -> str:
    src_idx = XMLGeneratedProfileBuilder.build_field_index(_xml_profile_config())
    tgt_idx = JSONGeneratedProfileBuilder.build_field_index(_json_profile_config())
    return MapFunctionBuilder().build(
        source_index=src_idx, target_index=tgt_idx, **_function_map_config()
    )


# ---------------------------------------------------------------------------
# Invariant assertions — Process flow builder
# ---------------------------------------------------------------------------


def test_inv_message_quote_escaping_handled_by_emitter():
    # The caller hands RAW JSON-payload text (quotes, angle brackets, ampersand,
    # apostrophe); the emitter alone owns the MessageFormat quoting (#102 C3) —
    # wrapping the JSON in single quotes and doubling the internal apostrophe —
    # and produces well-formed XML that round-trips through the parser.
    payload = "{\"status\":\"<a & b>\",\"q\":\"it's\"}"
    xml = ProcessFlowBuilder.build(
        _process_config(transform={"mode": "message", "message_text": payload}),
        name="P",
    )
    shapes = _parse_process_shapes(xml)
    msg = next(s for s in shapes if s.attrib["shapetype"] == "message")
    assert (
        msg.find("configuration/message/msgTxt").text
        == "'{\"status\":\"<a & b>\",\"q\":\"it''s\"}'"
    )


def test_inv_message_combined_attr_not_combinedocuments():
    xml = ProcessFlowBuilder.build(
        _process_config(
            transform={"mode": "message", "message_text": "{\"a\":1}"}
        ),
        name="P",
    )
    shapes = _parse_process_shapes(xml)
    message = next(
        s for s in shapes if s.attrib["shapetype"] == "message"
    ).find("configuration/message")
    assert message.attrib.get("combined") == "false"
    assert "combineDocuments" not in message.attrib


def test_inv_setproperties_shapetype_documentproperties():
    xml = ProcessFlowBuilder.build(_dynamic_path_config(), name="P")
    assert 'shapetype="setproperties"' not in xml
    shapes = _parse_process_shapes(xml)
    dp_shapes = [s for s in shapes if s.attrib["shapetype"] == "documentproperties"]
    assert len(dp_shapes) == 1
    prop = dp_shapes[0].find(
        "configuration/documentproperties/documentproperty"
    )
    assert prop.attrib["propertyId"].startswith("dynamicdocument.")


def test_inv_dataprocessscript_attrs_groovy2_usecache_true():
    # Issue #106 M10.2: a Custom Scripting Data Process step always emits the
    # mandatory dataprocessscript attributes language="groovy2" / useCache="true"
    # (a missing language attribute fails the platform at runtime). The shapetype
    # is dataprocess and the step carries the standard operation name.
    xml = ProcessFlowBuilder.build(
        _process_config(
            transform={
                "mode": "dataprocess",
                "label": "Tag documents",
                "steps": [
                    {
                        "operation": "custom_scripting",
                        "script": "dataContext.storeStream(is, props);",
                    }
                ],
            }
        ),
        name="P",
    )
    shapes = _parse_process_shapes(xml)
    dp = next(s for s in shapes if s.attrib["shapetype"] == "dataprocess")
    step = dp.find("configuration/dataprocess/step")
    assert step.attrib["name"] == "Custom Scripting"
    assert step.attrib["processtype"] == "12"
    assert step.attrib["index"] == "1" and step.attrib["key"] == "1"
    script = step.find("dataprocessscript")
    assert script.attrib["language"] == "groovy2"
    assert script.attrib["useCache"] == "true"


def test_inv_stop_carries_continue_and_no_stopaction():
    xml = ProcessFlowBuilder.build(_process_config(), name="P")
    assert "stopaction" not in xml
    shapes = _parse_process_shapes(xml)
    stops = [s for s in shapes if s.attrib["shapetype"] == "stop"]
    assert stops, "process must emit at least one stop"
    for stop in stops:
        node = stop.find("configuration/stop")
        assert node is not None
        assert "continue" in node.attrib


def test_inv_connectoraction_shape_header():
    xml = ProcessFlowBuilder.build(_process_config(), name="P")
    shapes = _parse_process_shapes(xml)
    cas = [
        s.find("configuration/connectoraction")
        for s in shapes
        if s.attrib["shapetype"] == "connectoraction"
    ]
    assert cas, "process must emit connectoraction shapes"
    for ca in cas:
        assert ca.attrib["allowDynamicCredentials"] == "NONE"
        for attr in ("actionType", "connectionId", "connectorType", "operationId"):
            assert ca.attrib.get(attr), attr


def test_inv_dragpoints_present_on_nonterminal_shapes():
    xml = ProcessFlowBuilder.build(_process_config(), name="P")
    shapes = _parse_process_shapes(xml)
    names = [s.attrib["name"] for s in shapes]
    for i, shape in enumerate(shapes):
        dragpoints = shape.find("dragpoints")
        assert dragpoints is not None, f"{shape.attrib['name']} missing <dragpoints>"
        if shape.attrib["shapetype"] == "stop":
            # Terminal shape: dragpoints element present but empty.
            assert list(dragpoints) == []
        else:
            edges = list(dragpoints)
            assert len(edges) >= 1
            assert edges[0].attrib["toShape"] == names[i + 1]


def test_inv_start_shape_geometry_constants():
    # Lock the CURRENT deterministic layout constants (not the Companion's
    # unverified 250/100 wording — see disposition table note).
    xml = ProcessFlowBuilder.build(_process_config(), name="P")
    start = _parse_process_shapes(xml)[0]
    assert start.attrib["shapetype"] == "start"
    assert start.attrib["x"] == "96.0"
    assert start.attrib["y"] == "94.0"


def test_inv_ddp_track_binding_on_connector_path():
    xml = ProcessFlowBuilder.build(_dynamic_path_config(), name="P")
    shapes = _parse_process_shapes(xml)
    target = next(
        s for s in shapes
        if s.attrib["shapetype"] == "connectoraction"
        and s.find("configuration/connectoraction/dynamicProperties/propertyvalue")
        is not None
    )
    pv = target.find(
        "configuration/connectoraction/dynamicProperties/propertyvalue"
    )
    assert pv.attrib["valueType"] == "track"
    tp = pv.find("trackparameter")
    assert tp.attrib["propertyId"].startswith("dynamicdocument.")


def test_inv_notify_quote_escaping_and_track_binding():
    xml = ProcessFlowBuilder.build(_notify_config(), name="P")
    shapes = _parse_process_shapes(xml)
    notify = next(s for s in shapes if s.attrib["shapetype"] == "notify").find(
        "configuration/notify"
    )
    assert notify.attrib["disableEvent"] == "true"
    # Apostrophe doubled (MessageFormat literal escape) + token → {1}.
    assert notify.find("notifyMessage").text == "It''s broken. Caught: {1}"
    pv = notify.find("notifyParameters/parametervalue")
    assert pv.attrib["valueType"] == "track"
    assert pv.find("trackparameter").attrib["propertyId"] == _NOTIFY_TOKEN


def test_inv_catcherrors_try_catch_identifiers():
    xml = ProcessFlowBuilder.build(_notify_config(), name="P")
    shapes = _parse_process_shapes(xml)
    catch = next(s for s in shapes if s.attrib["shapetype"] == "catcherrors")
    cfg = catch.find("configuration/catcherrors")
    assert cfg.attrib["catchAll"] == "true"
    assert cfg.attrib["retryCount"] == "2"
    identifiers = {dp.attrib.get("identifier") for dp in catch.find("dragpoints")}
    assert identifiers == {"default", "error"}


def test_inv_componentid_absent_on_create():
    xml = ProcessFlowBuilder.build(_process_config(), name="P")
    root = ET.fromstring(xml)
    assert "componentId" not in root.attrib


def test_inv_componentid_preserved_on_update():
    # The other half of the invariant: a structured update must keep the live
    # component's GUID. The builder emits create-style XML (no componentId), so
    # the #45 preservation layer (componentId is NOT an owned root attr) must
    # carry the existing GUID through merge_for_update.
    desired = ProcessFlowBuilder.build(_process_config(), name="renamed")
    current = ProcessFlowBuilder.build(_process_config(), name="original").replace(
        "<bns:Component ", '<bns:Component componentId="GUID-LIVE-123" ', 1
    )
    merged = merge_for_update(
        current, desired, ProcessFlowBuilder.PRESERVATION_POLICY
    )
    root = ET.fromstring(merged)
    assert root.attrib.get("componentId") == "GUID-LIVE-123"
    assert root.attrib.get("name") == "renamed"  # owned attr still applied


def test_inv_display_attrs_present_at_binding_sites():
    # Null-prevention: name/propertyName/defaultValue must be present at every
    # emitted parameter-binding site or the GUI renders "null".
    notify_root = ET.fromstring(ProcessFlowBuilder.build(_notify_config(), name="P"))
    tp = notify_root.find(
        "bns:object/process/shapes/shape/configuration/notify/"
        "notifyParameters/parametervalue/trackparameter",
        NS,
    )
    assert tp is not None
    assert "defaultValue" in tp.attrib
    assert tp.attrib.get("propertyName")  # present + non-empty
    assert tp.attrib.get("propertyId")

    dp_root = ET.fromstring(ProcessFlowBuilder.build(_dynamic_path_config(), name="P"))
    # Set Properties documentproperty carries name + defaultValue.
    docprop = dp_root.find(
        "bns:object/process/shapes/shape/configuration/documentproperties/"
        "documentproperty",
        NS,
    )
    assert docprop is not None
    assert docprop.attrib.get("name")
    assert "defaultValue" in docprop.attrib
    # Connector Path trackparameter carries propertyName + defaultValue.
    path_tp = dp_root.find(
        "bns:object/process/shapes/shape/configuration/connectoraction/"
        "dynamicProperties/propertyvalue/trackparameter",
        NS,
    )
    assert path_tp is not None
    assert path_tp.attrib.get("propertyName")
    assert "defaultValue" in path_tp.attrib


def test_inv_no_branch_shapes_emitted():
    # Branch numBranches==dragpoint-count is not-applicable-yet: the plan
    # requires asserting current process builders emit NO branch shapes (so a
    # future Branch builder must add the lock when it lands).
    for cfg in (
        _process_config(),
        _process_config(transform={"mode": "message", "message_text": "{}"}),
        _process_config(transform={"mode": "map_ref", "map_ref": "M"}),
        _dynamic_path_config(),
        _notify_config(),
    ):
        shapes = _parse_process_shapes(ProcessFlowBuilder.build(cfg, name="P"))
        assert all(s.attrib["shapetype"] != "branch" for s in shapes)


# ---------------------------------------------------------------------------
# Invariant assertions — Profiles
# ---------------------------------------------------------------------------


def test_inv_json_profile_root_type_no_subtype():
    root = ET.fromstring(JSONGeneratedProfileBuilder().build(**_json_profile_config()))
    assert root.attrib["type"] == "profile.json"
    assert "subType" not in root.attrib


def test_inv_xml_profile_root_type_no_subtype():
    root = ET.fromstring(XMLGeneratedProfileBuilder().build(**_xml_profile_config()))
    assert root.attrib["type"] == "profile.xml"
    assert "subType" not in root.attrib


def test_inv_db_profile_root_type_no_subtype():
    root = ET.fromstring(
        DatabaseReadProfileBuilder().build(
            component_type="profile.db",
            profile_type="database.read",
            component_name="Test Read Profile",
            query="select 1 as one",
            output_fields=[{"name": "one"}],
        )
    )
    assert root.attrib["type"] == "profile.db"
    assert "subType" not in root.attrib


# ---------------------------------------------------------------------------
# Invariant assertions — Map functions
# ---------------------------------------------------------------------------


def test_inv_map_function_step_attrs_present():
    # PRESENCE only — values vary per function (a live Sum step is
    # cacheEnabled="false"), so never lock specific values.
    root = ET.fromstring(_build_function_map_xml())
    steps = root.findall("bns:object/Map/Functions/FunctionStep", NS)
    assert steps, "function map must emit FunctionStep(s)"
    for step in steps:
        for attr in ("cacheEnabled", "sumEnabled", "x", "y"):
            assert attr in step.attrib, attr
        # position mirrors key (live-verified convention).
        assert step.attrib["position"] == step.attrib["key"]


def test_inv_map_no_function_output_to_function_input_chaining():
    root = ET.fromstring(_build_function_map_xml())
    for mapping in root.findall("bns:object/Map/Mappings/Mapping", NS):
        # A function-output→function-input edge would carry BOTH ends as
        # functions; the builder never emits that.
        assert not (
            mapping.attrib.get("fromFunction") and mapping.attrib.get("toFunction")
        )


# ---------------------------------------------------------------------------
# Invariant assertions — REST operation (DISPUTED, pin current emission only)
# ---------------------------------------------------------------------------


def test_inv_rest_profile_type_emission_pinned():
    # DISPUTED (#50 owns disposition): this account's live exports carry these
    # attributes on working operations and our builder emits them by design.
    # We PIN the current emission (present + lowercase default), we DO NOT flip
    # to never-emit here.
    xml = RestClientOperationBuilder().build(**_rest_get_config())
    root = ET.fromstring(xml)
    config = root.find(
        "bns:object/Operation/Configuration/GenericOperationConfig", NS
    )
    assert config is not None
    assert config.attrib["requestProfileType"] == "xml"
    assert config.attrib["responseProfileType"] == "xml"


# ---------------------------------------------------------------------------
# The tracked invariant disposition table (executable documentation)
# ---------------------------------------------------------------------------

#: Every issue #82 catalog item with its emitter, disposition, and locking test.
#: ``test`` names a function in THIS module for guaranteed/fixed-here/disputed
#: locks, a sibling ``tests/*.py`` for cross-module locks, or a "table-only"
#: note for not-applicable-yet items. ``test_disposition_table_well_formed``
#: keeps this honest.
INVARIANT_DISPOSITIONS: List[Dict[str, str]] = [
    {
        "id": "message_quote_escaping",
        "invariant": "Message JSON payload quote/XML escaping handled by the emitter, never the caller",
        "emitter": "process_flow_builder._emit_message",
        "disposition": "guaranteed-by-construction",
        "test": "test_inv_message_quote_escaping_handled_by_emitter",
    },
    {
        "id": "message_combined_attr",
        "invariant": "Message uses combined= (not combineDocuments)",
        "emitter": "process_flow_builder._emit_message",
        "disposition": "guaranteed-by-construction",
        "test": "test_inv_message_combined_attr_not_combinedocuments",
    },
    {
        "id": "setproperties_shapetype",
        "invariant": 'Set Properties shape uses shapetype="documentproperties" (not setproperties)',
        "emitter": "process_flow_builder._emit_setproperties",
        "disposition": "guaranteed-by-construction",
        "test": "test_inv_setproperties_shapetype_documentproperties",
    },
    {
        "id": "stop_continue",
        "invariant": "Stop always carries continue=; never a bare <stop/> and never stopaction",
        "emitter": "process_flow_builder._emit_stop",
        "disposition": "guaranteed-by-construction",
        "test": "test_inv_stop_carries_continue_and_no_stopaction",
    },
    {
        "id": "connectoraction_header",
        "invariant": "connectoraction carries actionType/connectionId/connectorType/operationId + allowDynamicCredentials=NONE",
        "emitter": "process_flow_builder._emit_connectoraction",
        "disposition": "guaranteed-by-construction",
        "test": "test_inv_connectoraction_shape_header",
    },
    {
        "id": "dragpoints_present",
        "invariant": "<dragpoints> present on every shape (empty only on terminals)",
        "emitter": "process_flow_builder._emit_dragpoints",
        "disposition": "guaranteed-by-construction",
        "test": "test_inv_dragpoints_present_on_nonterminal_shapes",
    },
    {
        "id": "layout_constants",
        "invariant": "Deterministic shape layout constants (start x=96.0/y=94.0). Companion's 250/100 wording is unverified locally — current constants locked instead",
        "emitter": "process_flow_builder._START_SHAPE_X/_START_SHAPE_Y/_SHAPE_X_STEP",
        "disposition": "guaranteed-by-construction",
        "test": "test_inv_start_shape_geometry_constants",
    },
    {
        "id": "ddp_track_binding",
        "invariant": 'DDP reads compile to valueType="track"/<trackparameter> at the connector Path site',
        "emitter": "process_flow_builder._emit_connectoraction (dynamic_path)",
        "disposition": "guaranteed-by-construction",
        "test": "test_inv_ddp_track_binding_on_connector_path",
    },
    {
        "id": "notify_escaping_and_binding",
        "invariant": "Notify doubles apostrophes (MessageFormat), substitutes the caught-error token for {1}, and binds it as a track parameter; disableEvent=true",
        "emitter": "process_flow_builder._emit_notify",
        "disposition": "guaranteed-by-construction",
        "test": "test_inv_notify_quote_escaping_and_track_binding",
    },
    {
        "id": "catcherrors_identifiers",
        "invariant": 'catcherrors catchAll="true" + bounded retryCount; Try=default / Catch=error dragpoint identifiers',
        "emitter": "process_flow_builder._emit_catcherrors",
        "disposition": "guaranteed-by-construction",
        "test": "test_inv_catcherrors_try_catch_identifiers",
    },
    {
        "id": "componentid_create",
        "invariant": "componentId absent on create (preserved as GUID on update via the #45 preservation layer)",
        "emitter": "process_flow_builder.build (create) / component_update_preservation",
        "disposition": "guaranteed-by-construction",
        "test": "test_inv_componentid_absent_on_create, test_inv_componentid_preserved_on_update",
    },
    {
        "id": "display_null_prevention_attrs",
        "invariant": "name/propertyName/defaultValue present at every emitted parameter-binding site (GUI renders null otherwise)",
        "emitter": "process_flow_builder._emit_notify/_emit_setproperties/_emit_connectoraction param emitters",
        "disposition": "guaranteed-by-construction",
        "test": "test_inv_display_attrs_present_at_binding_sites",
    },
    {
        "id": "json_profile_type",
        "invariant": 'JSON profile root type="profile.json", no subType',
        "emitter": "json_profile_builder.build",
        "disposition": "guaranteed-by-construction",
        "test": "test_inv_json_profile_root_type_no_subtype",
    },
    {
        "id": "xml_profile_type",
        "invariant": 'XML profile root type="profile.xml", no subType',
        "emitter": "xml_profile_builder.build",
        "disposition": "guaranteed-by-construction",
        "test": "test_inv_xml_profile_root_type_no_subtype",
    },
    {
        "id": "db_profile_type",
        "invariant": 'DB profile root type="profile.db", no subType',
        "emitter": "profile_builder (DatabaseReadProfileBuilder).build",
        "disposition": "guaranteed-by-construction",
        "test": "test_inv_db_profile_root_type_no_subtype",
    },
    {
        "id": "map_function_step_attrs",
        "invariant": "Map FunctionStep carries cacheEnabled/sumEnabled/x/y (PRESENCE only — values vary)",
        "emitter": "map_function_registry.emit_function_step",
        "disposition": "guaranteed-by-construction",
        "test": "test_inv_map_function_step_attrs_present",
    },
    {
        "id": "map_no_function_chaining",
        "invariant": "No function-output→function-input chaining in emitted mappings",
        "emitter": "map_builder (function mapping renderer)",
        "disposition": "guaranteed-by-construction",
        "test": "test_inv_map_no_function_output_to_function_input_chaining",
    },
    {
        "id": "script_processing_store_stream",
        "invariant": "script.processing content lacking dataContext.storeStream( → non-blocking warning",
        "emitter": "integration_builder._lint_script_bodies",
        "disposition": "fixed-here",
        "test": "tests/test_script_lints.py",
    },
    {
        "id": "script_body_long",
        "invariant": "Inline script bodies over ~50 lines → non-blocking warning (anti-scripting threshold)",
        "emitter": "integration_builder._lint_script_bodies",
        "disposition": "fixed-here",
        "test": "tests/test_script_lints.py",
    },
    {
        "id": "rest_profile_type_attrs",
        "invariant": "REST requestProfileType/responseProfileType on GenericOperationConfig",
        "emitter": "connector_builder.RestClientOperationBuilder.build",
        "disposition": "disputed-owned-elsewhere",
        "test": "test_inv_rest_profile_type_emission_pinned",
    },
    # --- not-applicable-yet: no typed builder emits these shapes today ---
    {
        "id": "branch_numbranches",
        "invariant": "branch numBranches equals dragpoint count (un-gated Branch emission)",
        "emitter": "(no branch typed builder; process_graph_verifier checks imported XML)",
        "disposition": "not-applicable-yet",
        "test": "test_inv_no_branch_shapes_emitted (negative lock — current builders emit no branch shape)",
    },
    {
        "id": "listener_shapes",
        "invariant": 'Listener start actionType="Listen"; allowSimultaneous=true/updateRunDates=false process options',
        "emitter": "(M6 owns listener work)",
        "disposition": "not-applicable-yet",
        "test": "table-only (no listener builder yet)",
    },
    {
        "id": "dataprocessscript_attrs",
        "invariant": 'dataprocessscript language="groovy2"/useCache="true"',
        "emitter": "process_flow_builder._emit_dataprocess (issue #106 M10.2)",
        "disposition": "guaranteed-by-construction",
        "test": "test_inv_dataprocessscript_attrs_groovy2_usecache_true",
    },
    {
        "id": "dpp_processparameter_binding",
        "invariant": 'DPP reads compile to valueType="process"/<processparameter>',
        "emitter": "(no DPP-read emitter exists; only DDP/track sites are emitted today)",
        "disposition": "not-applicable-yet",
        "test": "table-only (no DPP processparameter emitter yet)",
    },
    {
        "id": "flatfile_identity_fields",
        "invariant": 'Flat-file/data-positioned identity fields default mandatory="false"; identityValue trimmed',
        "emitter": "(no flat-file/data-positioned profile builder)",
        "disposition": "not-applicable-yet",
        "test": "table-only (no flat-file profile builder yet)",
    },
    {
        "id": "salesforce_sorts",
        "invariant": "Salesforce query operations always emit <Sorts/>",
        "emitter": "(no Salesforce operation builder)",
        "disposition": "not-applicable-yet",
        "test": "table-only (no Salesforce builder yet)",
    },
    {
        "id": "course_derived_patterns",
        "invariant": (
            "Course-derived low-level mechanics (tracked fields, process route "
            "data-passthrough, cache batch count=1, start-step settings per "
            "type, branch-stop wiring, subprocess passthrough start, low-latency "
            "vs general mode, exception/notify message hygiene, set-properties "
            "consolidation, selective display-name convention, find-changes "
            "fan-out, document-cache lookup, simulated-profile, watermark, "
            "return-path wiring)"
        ),
        "emitter": "(no typed builder emits these shapes yet — M5/M6 will inherit this checklist)",
        "disposition": "not-applicable-yet",
        "test": "table-only (no typed builder yet)",
    },
]


def test_disposition_table_well_formed():
    valid = {
        "guaranteed-by-construction",
        "fixed-here",
        "not-applicable-yet",
        "disputed-owned-elsewhere",
    }
    seen_ids = set()
    for entry in INVARIANT_DISPOSITIONS:
        assert {"id", "invariant", "emitter", "disposition", "test"} <= set(entry), entry
        assert entry["disposition"] in valid, entry["disposition"]
        assert entry["id"] not in seen_ids, f"duplicate id {entry['id']}"
        seen_ids.add(entry["id"])
        assert entry["invariant"].strip()
        assert entry["test"].strip()
        # A guaranteed/fixed-here/disputed item MUST name a real locking test.
        # Local names (no '.py') must resolve to a function in this module.
        if entry["disposition"] != "not-applicable-yet":
            for tname in (t.strip() for t in entry["test"].split(",")):
                if tname and ".py" not in tname:
                    assert tname in globals(), (
                        f"{entry['id']} names missing local test {tname!r}"
                    )


def test_disposition_table_covers_every_catalog_item():
    # Sanity floor so a future edit can't silently drop catalog rows.
    dispositions = [e["disposition"] for e in INVARIANT_DISPOSITIONS]
    assert dispositions.count("disputed-owned-elsewhere") == 1  # REST profile-type → #50
    assert dispositions.count("fixed-here") == 2  # the two script lints
    assert dispositions.count("guaranteed-by-construction") >= 14
