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
# Issue #96 §H — ddp / dpp Set Properties value-source segments
# ---------------------------------------------------------------------------


def _ddp_dynamic_path():
    # A ddp/dpp-only dynamic path: no profile segment -> no request_profile_id.
    return {
        "ddp_name": "DDP_PATH_ITEMS",
        "request_profile_id": None,
        "profile_type": None,
        "segments": [
            {"type": "static", "value": "/v1/items/"},
            {"type": "ddp", "property_name": "client_id"},
            {"type": "static", "value": "/notes/"},
            {"type": "dpp", "property_name": "run_id"},
        ],
    }


def test_ddp_dpp_segments_emit_captured_xml():
    # valueType="track" + <trackparameter> for a DDP source; valueType="process" +
    # <processparameter> for a DPP source — both live-captured (#96 §H).
    xml = ProcessFlowBuilder.build(_config(dynamic_path=_ddp_dynamic_path()), name="P")
    root = ET.fromstring(xml)
    prop = root.find(".//documentproperty")
    assert prop.get("propertyId") == "dynamicdocument.DDP_PATH_ITEMS"
    params = prop.findall(".//parametervalue")
    assert [p.get("valueType") for p in params] == ["static", "track", "static", "process"]

    ddp = params[1]
    tp = ddp.find("trackparameter")
    assert tp.get("propertyId") == "dynamicdocument.client_id"
    assert tp.get("propertyName") == "Dynamic Document Property - client_id"
    assert tp.get("defaultValue") == ""

    dpp = params[3]
    pp = dpp.find("processparameter")
    assert pp.get("processproperty") == "run_id"
    assert pp.get("processpropertydefaultvalue") == ""


def test_ddp_dpp_only_path_omits_parameter_profile():
    # No profile segment -> the connector step carries no parameter-profile attr
    # (it must NOT be emitted as parameter-profile="").
    xml = ProcessFlowBuilder.build(_config(dynamic_path=_ddp_dynamic_path()), name="P")
    assert 'parameter-profile=""' not in xml
    root = ET.fromstring(xml)
    ca = next(c for c in root.iter("connectoraction") if c.get("actionType") == "PATCH")
    assert ca.get("parameter-profile") is None
    # The "Path" property still references the DDP the Set Properties step builds.
    pv = ca.find(".//dynamicProperties/propertyvalue")
    assert pv.get("valueType") == "track"
    assert pv.find("trackparameter").get("propertyId") == "dynamicdocument.DDP_PATH_ITEMS"


def test_ddp_dpp_only_path_validates_clean():
    err = ProcessFlowBuilder.validate_config(
        _config(dynamic_path=_ddp_dynamic_path()), depends_on=[]
    )
    assert err is None


def test_blank_ddp_segment_property_name_rejected():
    bad = _ddp_dynamic_path()
    bad["segments"] = [{"type": "ddp", "property_name": "   "}]
    err = ProcessFlowBuilder.validate_config(_config(dynamic_path=bad), depends_on=[])
    assert err is not None
    assert err.error_code == "PROCESS_PATH_REPLACEMENT_INVALID"


def test_profile_segment_without_request_profile_id_rejected():
    # request_profile_id is required as soon as a profile segment is present.
    bad = _dynamic_path()
    bad["request_profile_id"] = None
    err = ProcessFlowBuilder.validate_config(_config(dynamic_path=bad), depends_on=[])
    assert err is not None
    assert err.error_code == "PROCESS_PATH_REPLACEMENT_INVALID"


def test_ddp_dpp_only_path_with_stray_request_profile_id_rejected():
    # A ddp/dpp-only path carries no profile; a stray request_profile_id (e.g.
    # copied from the old required shape) is contradictory — it would emit a
    # parameter-profile with no matching profileelement. Reject at plan time.
    bad = _ddp_dynamic_path()
    bad["request_profile_id"] = "STALE-PROFILE-UUID"
    err = ProcessFlowBuilder.validate_config(_config(dynamic_path=bad), depends_on=[])
    assert err is not None
    assert err.error_code == "PROCESS_PATH_REPLACEMENT_INVALID"


def test_ddp_dpp_only_path_with_nonstring_stray_request_profile_id_rejected():
    # None/absent is the only valid shape on a profile-less path — a malformed
    # NON-string stray value (123 / True / a list) is contradictory too and must
    # not slip past validate_config just because it is not a string.
    for stray in (123, True, ["x"], {"k": "v"}, 0):
        bad = _ddp_dynamic_path()
        bad["request_profile_id"] = stray
        err = ProcessFlowBuilder.validate_config(_config(dynamic_path=bad), depends_on=[])
        assert err is not None, f"non-string stray {stray!r} should be rejected"
        assert err.error_code == "PROCESS_PATH_REPLACEMENT_INVALID"


def test_ddp_dpp_only_path_blank_request_profile_id_accepted():
    # A blank string is treated as absent (the emitter emits no parameter-profile),
    # so it validates clean — only a meaningful (non-blank) value is contradictory.
    for blank in (None, "", "   "):
        ok = _ddp_dynamic_path()
        ok["request_profile_id"] = blank
        err = ProcessFlowBuilder.validate_config(_config(dynamic_path=ok), depends_on=[])
        assert err is None, f"blank request_profile_id {blank!r} should validate clean"


def test_ddp_dpp_only_path_stray_request_profile_id_not_emitted_on_bypass():
    # Defense-in-depth: even if validate_config is bypassed, build() must not emit a
    # parameter-profile for a profile-less path (no <profileelement> to bind to).
    bad = _ddp_dynamic_path()
    bad["request_profile_id"] = "STALE-PROFILE-UUID"
    xml = ProcessFlowBuilder.build(_config(dynamic_path=bad), name="P")
    assert "STALE-PROFILE-UUID" not in xml
    assert "parameter-profile" not in xml


def test_xml_well_formed_with_ddp_dpp_segments():
    xml = ProcessFlowBuilder.build(_config(dynamic_path=_ddp_dynamic_path()), name="P")
    ET.fromstring(xml)


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
