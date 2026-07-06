"""Issue #133 (M6.1) — manage_component typed webservice UPDATE tests.

Regression lock for QA bug #149: a typed webservice config passed to
``manage_component(action="update")`` used to fall through to the metadata
smart-merge (the ``component_name`` key triggered a rename-only write),
returning success while SILENTLY discarding route changes and bumping the
component version. The typed path now runs the ApiServiceBuilder +
read-merge-write with the preservation policy — parity with create and with
build_integration's structured update.
"""

from __future__ import annotations

import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

import xml.etree.ElementTree as ET

_project_root = str(Path(__file__).resolve().parent.parent)
if _project_root not in sys.path:
    sys.path.insert(0, _project_root)

from src.boomi_mcp.categories.components.builders.api_service_builder import (
    ApiServiceBuilder,
)
from src.boomi_mcp.categories.components.manage_component import update_component

NS = {"bns": "http://api.platform.boomi.com/"}

_PROCESS_ID = "c991a424-e7e3-4af1-b2ab-3ddba4a43974"
_ASC_ID = "f7a605a0-732f-4ad8-a479-f59c5034bf45"

_GET_PATCH = "src.boomi_mcp.categories.components.manage_component.component_get_xml"


def _current_xml(*, populated_overrides=False):
    xml = ApiServiceBuilder().build(
        component_type="webservice",
        component_name="Live ASC",
        routes=[{"process": _PROCESS_ID, "http_method": ""}],
    )
    xml = xml.replace(
        "<bns:Component ",
        f'<bns:Component componentId="{_ASC_ID}" version="3" ',
        1,
    )
    if populated_overrides:
        xml = xml.replace(
            "<profileOverrides/>",
            '<profileOverrides><profileOverride processId="'
            + _PROCESS_ID
            + '" inputProfile="11111111-1111-1111-1111-111111111111"/>'
            "</profileOverrides>",
        )
    return xml


def _typed_update(config, *, current_xml=None):
    client = MagicMock()
    with patch(_GET_PATCH) as mock_get:
        mock_get.return_value = {
            "xml": current_xml or _current_xml(),
            "name": "Live ASC",
            "type": "webservice",
        }
        result = update_component(client, "prof", _ASC_ID, config)
    return result, client


def test_typed_update_applies_route_changes():
    result, client = _typed_update(
        {
            "component_type": "webservice",
            "component_name": "Live ASC",
            "routes": [{"process": _PROCESS_ID, "http_method": "PUT"}],
        }
    )
    assert result["_success"] is True, result
    pushed = client.component.update_component_raw.call_args.args[1]
    overrides = ET.fromstring(pushed).find(
        "bns:object/webservice/restApi/route/overrides", NS
    )
    # The route change actually lands — the pre-fix path silently kept "".
    assert overrides.get("httpMethod") == "PUT"


def test_typed_update_preserves_populated_profile_overrides_in_slot():
    result, client = _typed_update(
        {
            "component_type": "webservice",
            "component_name": "Live ASC",
            "routes": [{"process": _PROCESS_ID, "http_method": "PUT"}],
        },
        current_xml=_current_xml(populated_overrides=True),
    )
    assert result["_success"] is True, result
    pushed = client.component.update_component_raw.call_args.args[1]
    ws = ET.fromstring(pushed).find("bns:object/webservice", NS)
    # Canonical XSD order (bug #148) with the live profileOverrides intact.
    assert [c.tag for c in ws] == [
        "restApi",
        "soapApi",
        "odataApi",
        "metaInfo",
        "profileOverrides",
        "capturedHeaders",
        "apiRoles",
    ]
    entry = ws.find("profileOverrides/profileOverride")
    assert entry is not None
    assert entry.get("inputProfile") == "11111111-1111-1111-1111-111111111111"


def test_typed_update_validation_error_is_structured_and_pushes_nothing():
    result, client = _typed_update(
        {
            "component_type": "webservice",
            "component_name": "Live ASC",
            "routes": [{"process": _PROCESS_ID, "http_method": "BREW"}],
        }
    )
    assert result["_success"] is False
    assert result["error_code"] == "API_SERVICE_METHOD_UNSUPPORTED"
    client.component.update_component_raw.assert_not_called()


def test_metadata_only_update_still_uses_smart_merge():
    # Without a 'routes' key the config is a metadata update — the rename
    # path must keep working unchanged.
    result, client = _typed_update({"name": "Renamed ASC"})
    assert result["_success"] is True, result
    pushed = client.component.update_component_raw.call_args.args[1]
    root = ET.fromstring(pushed)
    assert root.get("name") == "Renamed ASC"
    # Body untouched by the rename.
    assert root.find("bns:object/webservice/restApi/route", NS) is not None
