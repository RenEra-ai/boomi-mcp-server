"""Organization create/get/list/update keep JSON under SDK 3.0.0.

The OrganizationComponent endpoint accepts JSON; SDK 3.0.0's typed create/get/
update are XML-only, so the MCP keeps building/reading the typed model and
transports JSON via ``component_family_json_request``. These verify a typed
*model* (never raw XML) is transported, the JSON response is hydrated back into
a model, and the public response shapes + error handling are preserved.
"""
from unittest.mock import MagicMock, patch

from boomi.models import OrganizationComponent
import boomi_mcp.categories.components.organizations as orgs

_ORG_JSON = {
    "componentId": "org-1",
    "componentName": "Acme",
    "folderName": "Home",
    "OrganizationContactInfo": {
        "contactName": "Jane",
        "email": "jane@acme.com",
        "phone": "555-1212",
    },
}


def test_create_organization_transports_model_as_json():
    calls = {}

    def fake(service, path, method="POST", body=None, body_content_type="application/json"):
        calls.update(path=path, method=method, body=body)
        return dict(_ORG_JSON), 200

    with patch.object(orgs, "component_family_json_request", fake):
        out = orgs.create_organization(
            MagicMock(), "work", {"component_name": "Acme", "contact_email": "jane@acme.com"}
        )
    assert out["_success"] is True
    assert out["organization"]["component_id"] == "org-1"
    assert calls["path"] == "OrganizationComponent"
    assert calls["method"] == "POST"
    # The typed model is transported — NOT raw XML and NOT a plain dict.
    assert isinstance(calls["body"], OrganizationComponent)


def test_get_organization_parses_json_into_fields():
    with patch.object(orgs, "component_family_json_request", lambda *a, **k: (dict(_ORG_JSON), 200)):
        out = orgs.get_organization(MagicMock(), "work", "org-1")
    assert out["_success"] is True
    o = out["organization"]
    assert o["component_id"] == "org-1"
    assert o["name"] == "Acme"
    assert o["contact_email"] == "jane@acme.com"


def test_get_organization_error_status_maps_to_failure():
    with patch.object(orgs, "component_family_json_request", lambda *a, **k: ({"message": "not found"}, 404)):
        out = orgs.get_organization(MagicMock(), "work", "missing")
    assert out["_success"] is False
    assert "not found" in out["error"]


def test_list_organizations_json():
    def fake(service, path, method="POST", body=None, body_content_type="application/json"):
        if path.endswith("query"):
            return {"result": [{"componentId": "org-1", "componentName": "Acme", "folderName": "Home"}]}, 200
        return {}, 200

    with patch.object(orgs, "component_family_json_request", fake):
        out = orgs.list_organizations(MagicMock(), "work")
    assert out["_success"] is True
    assert out["total_count"] == 1
    assert out["organizations"][0]["component_id"] == "org-1"


def test_update_organization_merges_then_posts():
    seq = []

    def fake(service, path, method="POST", body=None, body_content_type="application/json"):
        seq.append(method)
        return dict(_ORG_JSON), 200

    with patch.object(orgs, "component_family_json_request", fake):
        out = orgs.update_organization(MagicMock(), "work", "org-1", {"component_name": "Acme2"})
    assert out["_success"] is True
    assert "component_name" in out["organization"]["updated_fields"]
    assert seq[0] == "GET" and seq[-1] == "POST"
