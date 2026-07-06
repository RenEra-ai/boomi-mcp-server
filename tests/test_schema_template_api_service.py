"""Schema-template tests for component / create / webservice (#133 M6.1).

The typed API Service Component create template replaces the generic raw-XML
fallback, making the `webservice` advertisement truthful. Also covers the
read-only `schema_name='api_service'` authoring reference surface and the
tier-dispatch rewrite of the build_and_verify_http_listener workflow.
"""

import sys
from pathlib import Path

_project_root = str(Path(__file__).resolve().parent.parent)
if _project_root not in sys.path:
    sys.path.insert(0, _project_root)

from boomi_mcp.categories.meta_tools import (
    _valid_schema_names,
    get_schema_template_action,
)


def _create_template(component_type="webservice"):
    result = get_schema_template_action(
        resource_type="component",
        operation="create",
        component_type=component_type,
    )
    assert result["_success"] is True
    return result


def test_template_resolves_typed_not_generic_fallback():
    result = _create_template()
    assert result["component_type"] == "webservice"
    # The generic raw-XML fallback advertises config.xml as the only path;
    # the typed template carries a structured routes template instead.
    assert "routes" in result["template"]
    assert result["raw_xml_exposed"] is False


def test_template_alias_component_types_resolve():
    for alias in ("api_service", "api.service"):
        result = _create_template(alias)
        assert result["component_type"] == "webservice"


def test_template_route_contract_and_defaults():
    result = _create_template()
    assert result["required"] == ["component_type", "component_name", "routes"]
    assert result["route_required"] == ["process"]
    assert set(result["route_optional"]) == {
        "http_method",
        "url_path",
        "object_name",
        "input_type",
        "output_type",
        "input_profile_key",
        "description",
    }
    assert result["defaults"]["version"] == "1.0.0"


def test_template_documents_tier_and_inherit_semantics():
    result = _create_template()
    # apiType tier dispatch + no-cascade deploy are the two live-confirmed
    # traps; the template must state both.
    assert "advanced" in result["note"]
    assert "does NOT cascade" in result["note"] or "does not cascade" in result["note"]
    # Empty-string inherit + verbatim casing formula.
    assert "verbatim" in result["effective_path_formula"]
    assert "/ws/rest" in result["effective_path_formula"]
    # profileOverrides is never authored; preserved on structured update.
    assert "profileOverrides" in result["update_note"]


def test_template_error_codes_present():
    result = _create_template()
    for code in (
        "API_SERVICE_ROUTES_REQUIRED",
        "API_SERVICE_ROUTE_PROCESS_NOT_LISTEN",
        "API_SERVICE_DUPLICATE_ROUTE",
        "API_SERVICE_PROFILE_OVERRIDES_UNSUPPORTED",
        "PLAINTEXT_SECRET_REJECTED",
    ):
        assert code in result["error_codes"], code


def test_template_example_uses_ref_route_with_depends_on():
    result = _create_template()
    example = result["example"]
    assert example["type"] == "webservice"
    assert example["depends_on"] == ["main_process"]
    assert example["config"]["routes"][0]["process"] == "$ref:main_process"


def test_api_service_schema_name_registered_and_resolves():
    assert "api_service" in _valid_schema_names()
    result = get_schema_template_action(schema_name="api_service")
    assert result["_success"] is True
    assert result["surface"] == "api_service"
    assert result["read_only"] is True
    assert "advanced" in result["tier_dispatch"]
    assert "depends_on" in result["route_coupling"]
    assert "ListenerStatus" in result["deploy_note"]


def test_listener_workflow_reads_apitype_first_and_branches():
    result = get_schema_template_action(schema_name="workflow_sequences")
    assert result["_success"] is True
    workflow = result["workflow_sequences"]["build_and_verify_http_listener"]
    steps = workflow["steps"]
    # get_server_info is step 1 — the apiType selects the publish pattern.
    assert "get_server_info" in steps[0]
    assert "asc_wrapper" in " ".join(steps)
    # ListenerStatus is explicitly named a non-signal for WSS/ASC routes.
    assert "ListenerStatus" in workflow["description"]
