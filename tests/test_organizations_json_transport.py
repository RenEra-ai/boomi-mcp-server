"""Organization create/get/list/update use the SDK 3.0.1 JSON methods.

SDK 3.0.1 added first-class JSON create/get/update (and the typed query already
existed) for OrganizationComponent, so the MCP now calls those directly instead
of hand-rolling a JSON transport. These verify a typed *model* is passed to
create, responses are read as the wire dict the readers expect (org stays
dict-normalized — the strict OrganizationContactInfo rejects sparse payloads), a
non-2xx ApiError maps to the failure envelope, and update does GET-then-POST.
"""
from types import SimpleNamespace
from unittest.mock import MagicMock

from boomi.models import OrganizationComponent
from boomi.net.transport.api_error import ApiError
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


def _api_error(status, message):
    """Build an ApiError whose JSON body carries a user-facing message."""
    return ApiError(
        message=f"{status} error",
        status=status,
        response=SimpleNamespace(body={"message": message}),
    )


def test_create_organization_transports_model_as_json():
    client = MagicMock()
    client.organization_component.create_organization_component_json.return_value = dict(_ORG_JSON)

    out = orgs.create_organization(
        client, "work", {"component_name": "Acme", "contact_email": "jane@acme.com"}
    )

    assert out["_success"] is True
    assert out["organization"]["component_id"] == "org-1"
    create = client.organization_component.create_organization_component_json
    create.assert_called_once()
    # The typed model is transported — NOT raw XML and NOT a plain dict.
    assert isinstance(create.call_args[0][0], OrganizationComponent)


def test_get_organization_parses_json_into_fields():
    client = MagicMock()
    client.organization_component.get_organization_component_json.return_value = dict(_ORG_JSON)

    out = orgs.get_organization(client, "work", "org-1")

    assert out["_success"] is True
    o = out["organization"]
    assert o["component_id"] == "org-1"
    assert o["name"] == "Acme"
    assert o["contact_email"] == "jane@acme.com"


def test_get_organization_error_maps_to_failure():
    client = MagicMock()
    client.organization_component.get_organization_component_json.side_effect = _api_error(404, "not found")

    out = orgs.get_organization(client, "work", "missing")

    assert out["_success"] is False
    assert "not found" in out["error"]


def test_list_organizations_via_typed_query():
    client = MagicMock()
    client.organization_component.query_organization_component.return_value = {
        "result": [{"componentId": "org-1", "componentName": "Acme", "folderName": "Home"}]
    }

    out = orgs.list_organizations(client, "work")

    assert out["_success"] is True
    assert out["total_count"] == 1
    assert out["organizations"][0]["component_id"] == "org-1"
    client.organization_component.query_organization_component.assert_called_once()


def test_update_organization_does_get_then_post():
    client = MagicMock()
    client.organization_component.get_organization_component_json.return_value = dict(_ORG_JSON)
    client.organization_component.update_organization_component_json.return_value = dict(_ORG_JSON)

    out = orgs.update_organization(client, "work", "org-1", {"component_name": "Acme2"})

    assert out["_success"] is True
    assert "component_name" in out["organization"]["updated_fields"]
    client.organization_component.get_organization_component_json.assert_called_once()
    client.organization_component.update_organization_component_json.assert_called_once()
    # The full wire dict is POSTed back (componentName updated, contact preserved).
    posted = client.organization_component.update_organization_component_json.call_args[0][1]
    assert posted["componentName"] == "Acme2"
    assert posted["OrganizationContactInfo"]["email"] == "jane@acme.com"
