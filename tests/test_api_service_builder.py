"""Issue #133 (M6.1, parent #12) — ApiServiceBuilder unit tests.

Byte-locks the builder's own emission and structurally round-trips it against
the live renera capture (tests/fixtures/live_xml/m6/api_service_minimal.xml —
the ASC that served POST /ws/rest/generalListener -> 200 on the advanced
cloud attachment, 2026-07-04). Exercises the validate_config matrix: route
process references ($ref/UUID), empty-string inherit semantics, the
duplicate-route checks, method/type vocabularies, profileOverrides/raw-XML
rejection, and the preservation-policy shape (subtree_merge with
profileOverrides deliberately unowned).
"""

from __future__ import annotations

import sys
from pathlib import Path

import pytest
import xml.etree.ElementTree as ET

_project_root = str(Path(__file__).resolve().parent.parent)
if _project_root not in sys.path:
    sys.path.insert(0, _project_root)

from src.boomi_mcp.categories.components.builders.connector_builder import (
    BuilderValidationError,
)
from src.boomi_mcp.categories.components.builders.api_service_builder import (
    API_SERVICE_BUILDERS,
    ApiServiceBuilder,
    get_api_service_builder,
)

NS = {"bns": "http://api.platform.boomi.com/"}

_PROCESS_ID = "c991a424-e7e3-4af1-b2ab-3ddba4a43974"
_PROCESS_ID_2 = "415e6f5b-499e-4552-a047-d7d0a01e761e"

_MINIMAL_FIXTURE = (
    Path(__file__).resolve().parent
    / "fixtures"
    / "live_xml"
    / "m6"
    / "api_service_minimal.xml"
)


def _minimal_config(**overrides):
    cfg = {
        "component_name": "New API Service",
        "routes": [{"process": _PROCESS_ID, "http_method": "POST"}],
    }
    cfg.update(overrides)
    return cfg


def _validate(**overrides):
    return ApiServiceBuilder.validate_config(_minimal_config(**overrides))


def _canon(elem):
    """Parsed structural form (tag, sorted attrs, text, children) for
    round-trip comparison — live exports self-close empty elements."""
    return (
        elem.tag,
        tuple(sorted(elem.attrib.items())),
        (elem.text or "").strip(),
        tuple(_canon(child) for child in elem),
    )


# ---------------------------------------------------------------------------
# Registry / dispatch
# ---------------------------------------------------------------------------


def test_registry_and_lookup():
    assert API_SERVICE_BUILDERS == {"webservice": ApiServiceBuilder}
    assert get_api_service_builder("webservice") is ApiServiceBuilder
    assert get_api_service_builder("processproperty") is None
    assert ApiServiceBuilder.SUPPORTED_COMPONENT_TYPES == ("webservice",)
    assert ApiServiceBuilder.SUPPORTED_HTTP_METHODS == (
        "GET",
        "POST",
        "PUT",
        "DELETE",
        "PATCH",
    )


# ---------------------------------------------------------------------------
# Emission — golden vs the live fixture
# ---------------------------------------------------------------------------


def test_minimal_emission_matches_live_fixture_object_subtree():
    """The emitted <bns:object> subtree is structurally identical to the live
    capture (title='test' pinned like the fixture; component metadata attrs
    like componentId/version are server-assigned and absent on create)."""
    xml = ApiServiceBuilder().build(
        component_type="webservice",
        component_name="New API Service",
        title="test",
        routes=[{"process": _PROCESS_ID, "http_method": "POST"}],
    )
    built_obj = ET.fromstring(xml).find("bns:object", NS)
    live_obj = ET.fromstring(_MINIMAL_FIXTURE.read_text()).find("bns:object", NS)
    assert _canon(built_obj) == _canon(live_obj)


def test_mandatory_placeholders_always_emitted():
    xml = ApiServiceBuilder().build(**_minimal_config())
    ws = ET.fromstring(xml).find("bns:object/webservice", NS)
    tags = [child.tag for child in ws]
    assert tags == [
        "restApi",
        "soapApi",
        "odataApi",
        "metaInfo",
        "profileOverrides",
        "capturedHeaders",
        "apiRoles",
    ]
    soap = ws.find("soapApi")
    assert soap.find("SOAPVersion").text == "SOAP_1_1"
    assert soap.get("fullEnvelopePassthrough") == "false"
    meta = ws.find("metaInfo")
    assert meta.find("description") is not None
    assert meta.find("termsOfService") is not None
    # profileOverrides is emitted EMPTY — never authored.
    assert len(ws.find("profileOverrides")) == 0


def test_empty_string_overrides_preserved_as_inherit():
    """Empty route overrides are meaningful (inherit) — emitted verbatim."""
    xml = ApiServiceBuilder().build(
        **_minimal_config(routes=[{"process": _PROCESS_ID}])
    )
    overrides = ET.fromstring(xml).find(
        "bns:object/webservice/restApi/route/overrides", NS
    )
    assert overrides.attrib == {
        "httpMethod": "",
        "inputProfileKey": "",
        "inputType": "",
        "objectName": "",
        "outputType": "",
        "urlPath": "",
    }


def test_multi_route_emission_order_and_process_ids():
    xml = ApiServiceBuilder().build(
        **_minimal_config(
            routes=[
                {"process": _PROCESS_ID, "http_method": "POST", "object_name": "a"},
                {"process": _PROCESS_ID_2, "http_method": "GET", "object_name": "b"},
            ]
        )
    )
    routes = ET.fromstring(xml).findall("bns:object/webservice/restApi/route", NS)
    assert [r.get("processId") for r in routes] == [_PROCESS_ID, _PROCESS_ID_2]
    assert [r.find("overrides").get("httpMethod") for r in routes] == ["POST", "GET"]


def test_base_url_path_title_version_and_description():
    xml = ApiServiceBuilder().build(
        **_minimal_config(
            base_url_path="intake",
            title="Order Intake",
            version="2.0.0",
            description="desc & more",
            folder_path="Renera/Process Library",
        )
    )
    root = ET.fromstring(xml)
    assert root.get("folderFullPath") == "Renera/Process Library"
    assert root.find("bns:description", NS).text == "desc & more"
    ws = root.find("bns:object/webservice", NS)
    assert ws.get("urlPath") == "intake"
    meta = ws.find("metaInfo")
    assert meta.get("title") == "Order Intake"
    assert meta.get("version") == "2.0.0"


def test_title_defaults_to_component_name_and_version_to_1_0_0():
    xml = ApiServiceBuilder().build(**_minimal_config())
    meta = ET.fromstring(xml).find("bns:object/webservice/metaInfo", NS)
    assert meta.get("title") == "New API Service"
    assert meta.get("version") == "1.0.0"


def test_xml_escaping_in_name_and_route_description():
    xml = ApiServiceBuilder().build(
        **_minimal_config(
            component_name='A&B <"svc">',
            routes=[
                {
                    "process": _PROCESS_ID,
                    "http_method": "POST",
                    "description": "hits <ERP> & more",
                }
            ],
        )
    )
    root = ET.fromstring(xml)  # parse failure would mean broken escaping
    assert root.get("name") == 'A&B <"svc">'
    route = root.find("bns:object/webservice/restApi/route", NS)
    assert route.find("description").text == "hits <ERP> & more"


def test_path_segment_case_preserved_verbatim():
    xml = ApiServiceBuilder().build(
        **_minimal_config(
            base_url_path="OrderIntake",
            routes=[
                {
                    "process": _PROCESS_ID,
                    "http_method": "POST",
                    "object_name": "generalListener",
                    "url_path": "V1",
                }
            ],
        )
    )
    ws = ET.fromstring(xml).find("bns:object/webservice", NS)
    assert ws.get("urlPath") == "OrderIntake"
    overrides = ws.find("restApi/route/overrides")
    assert overrides.get("objectName") == "generalListener"
    assert overrides.get("urlPath") == "V1"


def test_method_uppercased_on_emission():
    xml = ApiServiceBuilder().build(
        **_minimal_config(routes=[{"process": _PROCESS_ID, "http_method": "post"}])
    )
    overrides = ET.fromstring(xml).find(
        "bns:object/webservice/restApi/route/overrides", NS
    )
    assert overrides.get("httpMethod") == "POST"


def test_unresolved_ref_rejected_at_emission():
    with pytest.raises(BuilderValidationError) as exc:
        ApiServiceBuilder().build(
            **_minimal_config(routes=[{"process": "$ref:main_process"}])
        )
    assert exc.value.error_code == "API_SERVICE_ROUTE_PROCESS_REF_INVALID"
    assert "unresolved" in str(exc.value)


# ---------------------------------------------------------------------------
# validate_config
# ---------------------------------------------------------------------------


def test_component_name_required():
    err = _validate(component_name="  ")
    assert err.error_code == "API_SERVICE_NAME_REQUIRED"


def test_routes_required_and_non_empty():
    assert _validate(routes=[]).error_code == "API_SERVICE_ROUTES_REQUIRED"
    cfg = _minimal_config()
    del cfg["routes"]
    assert (
        ApiServiceBuilder.validate_config(cfg).error_code
        == "API_SERVICE_ROUTES_REQUIRED"
    )


def test_route_process_required():
    err = _validate(routes=[{"http_method": "POST"}])
    assert err.error_code == "API_SERVICE_ROUTE_PROCESS_REQUIRED"
    err = _validate(routes=[{"process": "   "}])
    assert err.error_code == "API_SERVICE_ROUTE_PROCESS_REQUIRED"


def test_route_process_ref_shapes():
    # $ref with a key is valid at validate time (resolved by build_integration).
    assert _validate(routes=[{"process": "$ref:main_process"}]) is None
    # process_id alias accepted.
    assert _validate(routes=[{"process_id": _PROCESS_ID}]) is None
    # Empty $ref key rejected.
    err = _validate(routes=[{"process": "$ref:"}])
    assert err.error_code == "API_SERVICE_ROUTE_PROCESS_REF_INVALID"
    # A non-UUID literal is rejected.
    err = _validate(routes=[{"process": "not-a-uuid"}])
    assert err.error_code == "API_SERVICE_ROUTE_PROCESS_REF_INVALID"


def test_http_method_vocabulary():
    assert _validate(routes=[{"process": _PROCESS_ID, "http_method": "delete"}]) is None
    assert _validate(routes=[{"process": _PROCESS_ID, "http_method": ""}]) is None
    err = _validate(routes=[{"process": _PROCESS_ID, "http_method": "BREW"}])
    assert err.error_code == "API_SERVICE_METHOD_UNSUPPORTED"


def test_input_output_type_vocabulary():
    assert _validate(routes=[{"process": _PROCESS_ID, "input_type": "singlejson"}]) is None
    assert _validate(routes=[{"process": _PROCESS_ID, "output_type": ""}]) is None
    err = _validate(routes=[{"process": _PROCESS_ID, "input_type": "csv"}])
    assert err.error_code == "API_SERVICE_TYPE_UNSUPPORTED"
    err = _validate(routes=[{"process": _PROCESS_ID, "output_type": "blob"}])
    assert err.error_code == "API_SERVICE_TYPE_UNSUPPORTED"


def test_profile_overrides_rejected():
    err = _validate(profile_overrides={"anything": 1})
    assert err.error_code == "API_SERVICE_PROFILE_OVERRIDES_UNSUPPORTED"
    err = _validate(profileOverrides={"anything": 1})
    assert err.error_code == "API_SERVICE_PROFILE_OVERRIDES_UNSUPPORTED"
    err = _validate(
        routes=[{"process": _PROCESS_ID, "profileOverrides": {"x": 1}}]
    )
    assert err.error_code == "API_SERVICE_PROFILE_OVERRIDES_UNSUPPORTED"


def test_raw_subtree_keys_rejected():
    for key in ("restApi", "soap_api", "odataApi", "metaInfo", "api_roles", "webservice"):
        err = _validate(**{key: {"x": 1}})
        assert err.error_code == "API_SERVICE_RAW_XML_UNSUPPORTED", key


def test_unknown_keys_rejected():
    err = _validate(banana=1)
    assert err.error_code == "API_SERVICE_VALIDATION_FAILED"
    err = _validate(routes=[{"process": _PROCESS_ID, "banana": 1}])
    assert err.error_code == "API_SERVICE_VALIDATION_FAILED"


def test_exact_duplicate_routes_rejected():
    err = _validate(
        routes=[
            {"process": _PROCESS_ID, "http_method": "POST"},
            {"process": _PROCESS_ID, "http_method": "POST"},
        ]
    )
    assert err.error_code == "API_SERVICE_DUPLICATE_ROUTE"


def test_explicit_effective_path_collision_rejected():
    """Two distinct processes whose explicit overrides resolve to the same
    method+path — computable without WSS-op inheritance — are rejected."""
    err = _validate(
        routes=[
            {"process": _PROCESS_ID, "http_method": "POST", "object_name": "intake"},
            {"process": _PROCESS_ID_2, "http_method": "POST", "object_name": "intake"},
        ]
    )
    assert err.error_code == "API_SERVICE_DUPLICATE_ROUTE"


def test_inherit_dependent_routes_not_duplicate_checked():
    """All-inherit routes to different processes cannot be resolved by the
    builder (the WSS ops decide) — deferred to analyze/orchestration."""
    assert (
        _validate(
            routes=[{"process": _PROCESS_ID}, {"process": _PROCESS_ID_2}]
        )
        is None
    )


def test_case_sensitive_duplicate_check():
    """/ws/rest paths are case-verbatim, so a case-only difference is a
    DIFFERENT route (unlike bare /ws/simple)."""
    assert (
        _validate(
            routes=[
                {"process": _PROCESS_ID, "http_method": "POST", "object_name": "intake"},
                {"process": _PROCESS_ID_2, "http_method": "POST", "object_name": "Intake"},
            ]
        )
        is None
    )


def test_secret_shaped_keys_rejected():
    err = _validate(routes=[{"process": _PROCESS_ID}], token="abc")
    assert err.error_code == "PLAINTEXT_SECRET_REJECTED"


# ---------------------------------------------------------------------------
# Preservation policy
# ---------------------------------------------------------------------------


def test_preservation_policy_shape():
    policy = ApiServiceBuilder.PRESERVATION_POLICY
    assert policy.component_type == "webservice"
    assert policy.owned_root_attrs == ("name",)
    assert len(policy.owned_paths) == 1
    owned = policy.owned_paths[0]
    assert owned.path == "bns:object/webservice"
    assert owned.mode == "subtree_merge"
    assert owned.owned_attrs == ("urlPath",)
    # ALL seven blocks are owned for ORDERING — the platform XSD requires the
    # exact captured sequence and an unowned profileOverrides was displaced
    # past apiRoles by the merge (live 400, #133 QA bug #148)...
    assert owned.owned_child_tags == (
        "restApi",
        "soapApi",
        "odataApi",
        "metaInfo",
        "profileOverrides",
        "capturedHeaders",
        "apiRoles",
    )
    # ...while profileOverrides CONTENT stays never-authored: the builder's
    # empty placeholder yields to a populated live element.
    assert owned.preserve_when_desired_empty == ("profileOverrides",)
