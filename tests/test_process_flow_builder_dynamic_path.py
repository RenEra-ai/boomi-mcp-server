"""Issue #100 G2 — ProcessFlowBuilder dynamic REST path emission.

The Boomi REST Client connector cannot declare in-operation URL path parameters
(that is an HTTP-Client feature). A per-document path is supplied at process time
via the connector step's "Path" dynamic operation property, built by a Set
Properties (``documentproperties``) shape that concatenates static literals with
mapped profile-element values. Shapes here are asserted against the live REST
Client export decoded in ``.codex/plans/issue-100-live-captures.md``.
"""

import xml.etree.ElementTree as ET

from boomi_mcp.categories.components.builders.process_flow_builder import (
    ProcessFlowBuilder,
)


def _dynamic_path():
    return {
        "ddp_name": "DDP_PATH_CLIENTS",
        "request_profile_id": "PROFILE-UUID",
        "profile_type": "profile.json",
        "segments": [
            {"type": "static", "value": "/admin/cdscm/api/v1/clients/"},
            {
                "type": "profile",
                "element_id": 3,
                "element_name": "clientId (Root/Object/clientId)",
            },
        ],
    }


def _config(*, dynamic_path=None, reliability=None):
    target = {
        "connector_type": "rest",
        "action_type": "PATCH",
        "connection_id": "CONN-UUID",
        "operation_id": "OP-UUID",
    }
    if dynamic_path is not None:
        target["dynamic_path"] = dynamic_path
    cfg = {
        "process_kind": "database_to_api_sync",
        "source": {
            "connector_type": "database",
            "action_type": "Get",
            "connection_id": "DBCONN",
            "operation_id": "DBOP",
        },
        "transform": {"mode": "map_ref", "map_ref": "MAP-UUID"},
        "target": target,
    }
    if reliability is not None:
        cfg["reliability"] = reliability
    return cfg


def _shapes(xml: str):
    """Return {shape_name: (shapetype, [toShape,...])} for the built process."""
    root = ET.fromstring(xml)
    out = {}
    for sh in root.iter("shape"):
        out[sh.get("name")] = (
            sh.get("shapetype"),
            [dp.get("toShape") for dp in sh.iter("dragpoint")],
        )
    return out


# ---------------------------------------------------------------------------
# Linear flow (no Try/Catch)
# ---------------------------------------------------------------------------


def test_dynamic_path_emits_setproperties_before_target():
    xml = ProcessFlowBuilder.build(_config(dynamic_path=_dynamic_path()), name="P")
    root = ET.fromstring(xml)

    dps = [s for s in root.iter("shape") if s.get("shapetype") == "documentproperties"]
    assert len(dps) == 1, "exactly one Set Properties shape expected"
    dp = dps[0]

    prop = dp.find(".//documentproperty")
    assert prop.get("propertyId") == "dynamicdocument.DDP_PATH_CLIENTS"
    assert prop.get("name") == "Dynamic Document Property - DDP_PATH_CLIENTS"
    assert prop.get("persist") == "false"

    params = prop.findall(".//parametervalue")
    assert len(params) == 2
    assert params[0].get("valueType") == "static"
    assert params[0].find("staticparameter").get("staticproperty") == (
        "/admin/cdscm/api/v1/clients/"
    )
    assert params[1].get("valueType") == "profile"
    pe = params[1].find("profileelement")
    assert pe.get("elementId") == "3"
    assert pe.get("elementName") == "clientId (Root/Object/clientId)"
    assert pe.get("profileId") == "PROFILE-UUID"
    assert pe.get("profileType") == "profile.json"

    # Set Properties must route to the target connector step.
    shapes = _shapes(xml)
    sp_name = dp.get("name")
    next_name = shapes[sp_name][1][0]
    assert shapes[next_name][0] == "connectoraction"


def test_connectoraction_emits_path_property():
    xml = ProcessFlowBuilder.build(_config(dynamic_path=_dynamic_path()), name="P")
    root = ET.fromstring(xml)
    # target connectoraction is the REST (PATCH) one
    ca = next(
        c for c in root.iter("connectoraction") if c.get("actionType") == "PATCH"
    )
    assert ca.get("parameter-profile") == "PROFILE-UUID"
    pv = ca.find(".//dynamicProperties/propertyvalue")
    assert pv is not None
    assert pv.get("key") == "path"
    assert pv.get("name") == "Path"
    # Dynamic DOCUMENT Property source -> valueType="track" + <trackparameter>.
    assert pv.get("valueType") == "track"
    tp = pv.find("trackparameter")
    assert tp.get("propertyId") == "dynamicdocument.DDP_PATH_CLIENTS"
    assert tp.get("propertyName") == "Dynamic Document Property - DDP_PATH_CLIENTS"
    assert tp.get("defaultValue") == ""


def test_no_dynamic_path_is_unchanged():
    xml = ProcessFlowBuilder.build(_config(dynamic_path=None), name="P")
    root = ET.fromstring(xml)
    assert not [
        s for s in root.iter("shape") if s.get("shapetype") == "documentproperties"
    ], "no Set Properties shape without path replacements"
    ca = next(
        c for c in root.iter("connectoraction") if c.get("actionType") == "PATCH"
    )
    assert ca.get("parameter-profile") is None
    # empty dynamicProperties, byte-for-byte the pre-#100 shape
    assert "<parameters/><dynamicProperties/>" in xml
    assert ca.find(".//propertyvalue") is None


def test_dynamic_path_xml_is_well_formed():
    xml = ProcessFlowBuilder.build(_config(dynamic_path=_dynamic_path()), name="P")
    # parse-back must not raise (build() also parse-guards internally)
    ET.fromstring(xml)


# ---------------------------------------------------------------------------
# Validation (PROCESS_PATH_REPLACEMENT_INVALID)
# ---------------------------------------------------------------------------


def test_valid_dynamic_path_validates_clean():
    err = ProcessFlowBuilder.validate_config(
        _config(dynamic_path=_dynamic_path()), depends_on=[]
    )
    assert err is None


def test_blank_ddp_name_rejected():
    bad = _dynamic_path()
    bad["ddp_name"] = "   "
    err = ProcessFlowBuilder.validate_config(_config(dynamic_path=bad), depends_on=[])
    assert err is not None
    assert err.error_code == "PROCESS_PATH_REPLACEMENT_INVALID"


def test_missing_segments_rejected():
    bad = _dynamic_path()
    bad["segments"] = []
    err = ProcessFlowBuilder.validate_config(_config(dynamic_path=bad), depends_on=[])
    assert err is not None
    assert err.error_code == "PROCESS_PATH_REPLACEMENT_INVALID"


def test_all_static_segments_rejected():
    # A path with no profile segment is not dynamic — reject it.
    bad = _dynamic_path()
    bad["segments"] = [{"type": "static", "value": "/v1/clients/"}]
    err = ProcessFlowBuilder.validate_config(_config(dynamic_path=bad), depends_on=[])
    assert err is not None
    assert err.error_code == "PROCESS_PATH_REPLACEMENT_INVALID"


def test_malformed_profile_segment_rejected():
    bad = _dynamic_path()
    bad["segments"] = [
        {"type": "profile", "element_id": 3},  # missing element_name
    ]
    err = ProcessFlowBuilder.validate_config(_config(dynamic_path=bad), depends_on=[])
    assert err is not None
    assert err.error_code == "PROCESS_PATH_REPLACEMENT_INVALID"


# ---------------------------------------------------------------------------
# Connector-scoped Try/Catch (issue #99 G1 + #100 G2 interaction)
# ---------------------------------------------------------------------------


def test_connector_scoped_setproperties_inside_target_retry():
    reliability = {
        "retry_count": 2,
        "try_catch_scope": "connector",
        "dlq": {"mode": "error_subprocess_ref", "process_id": "ERRSUB"},
    }
    xml = ProcessFlowBuilder.build(
        _config(dynamic_path=_dynamic_path(), reliability=reliability), name="P"
    )
    shapes = _shapes(xml)

    # Identify the two catcherrors; the target one is the second in flow order.
    catcherrors = [n for n, (t, _) in shapes.items() if t == "catcherrors"]
    assert len(catcherrors) == 2, "connector scope emits source + target Try/Catch"
    src_ce, tgt_ce = sorted(catcherrors, key=lambda n: int(n.replace("shape", "")))

    # The map stays OUTSIDE the target retry (between the two catcherrors): the
    # source connector or the map routes into the target catcherrors.
    map_names = [n for n, (t, _) in shapes.items() if t == "map"]
    assert len(map_names) == 1
    assert tgt_ce in shapes[map_names[0]][1], "map routes into the target Try/Catch"

    # The target catcherrors Try branch enters at the Set Properties shape, which
    # routes to the target connector — i.e. Set Properties is INSIDE the target
    # retry unit so a retry re-applies the per-document path.
    sp_names = [n for n, (t, _) in shapes.items() if t == "documentproperties"]
    assert len(sp_names) == 1
    sp = sp_names[0]
    assert sp in shapes[tgt_ce][1], "target Try branch enters at Set Properties"
    sp_next = shapes[sp][1][0]
    assert shapes[sp_next][0] == "connectoraction"
    # The map must NOT route directly to the target connector (it is separated by
    # the target catcherrors + Set Properties).
    assert shapes[map_names[0]][1] == [tgt_ce]


# ---------------------------------------------------------------------------
# Issue #96 M5.4a — source-side dynamic path emission (mirror of the target)
# ---------------------------------------------------------------------------


def _rest_source_config(source_dynamic_path):
    return {
        "process_kind": "database_to_api_sync",
        "source": {
            "connector_type": "rest",
            "action_type": "GET",
            "connection_id": "RCONN",
            "operation_id": "ROP",
            "dynamic_path": source_dynamic_path,
        },
        "transform": {"mode": "map_ref", "map_ref": "MAP-UUID"},
        "target": {
            "connector_type": "rest",
            "action_type": "PATCH",
            "connection_id": "CONN-UUID",
            "operation_id": "OP-UUID",
        },
    }


def test_source_dynamic_path_emits_setproperties_before_source():
    xml = ProcessFlowBuilder.build(_rest_source_config(_dynamic_path()), name="P")
    root = ET.fromstring(xml)

    # Exactly one Set Properties shape (the source path DDP); target is static.
    dps = [s for s in root.iter("shape") if s.get("shapetype") == "documentproperties"]
    assert len(dps) == 1
    prop = dps[0].find(".//documentproperty")
    assert prop.get("propertyId") == "dynamicdocument.DDP_PATH_CLIENTS"

    # The GET source connectoraction emits the live-proven "Path" property; the
    # PATCH target connectoraction stays empty.
    actions = {c.get("actionType"): c for c in root.iter("connectoraction")}
    src_props = actions["GET"].findall(".//dynamicProperties/propertyvalue")
    assert len(src_props) == 1
    assert src_props[0].get("key") == "path" and src_props[0].get("name") == "Path"
    assert src_props[0].get("valueType") == "track"
    assert actions["PATCH"].findall(".//dynamicProperties/propertyvalue") == []

    # Set Properties is positioned before the source connector in the shape order.
    order = [s.get("shapetype") for s in root.iter("shape")]
    assert order.index("documentproperties") < order.index("connectoraction")


def test_source_without_dynamic_path_emits_empty_source_connectoraction():
    cfg = _rest_source_config(_dynamic_path())
    cfg["source"].pop("dynamic_path")
    xml = ProcessFlowBuilder.build(cfg, name="P")
    root = ET.fromstring(xml)
    assert not [s for s in root.iter("shape") if s.get("shapetype") == "documentproperties"]
    actions = {c.get("actionType"): c for c in root.iter("connectoraction")}
    assert actions["GET"].findall(".//dynamicProperties/propertyvalue") == []
