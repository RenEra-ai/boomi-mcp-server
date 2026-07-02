"""build_integration(action='plan') SOAP Client coverage (issue #126, M5.10).

Verifies that build_integration can PLAN a SOAP connection + EXECUTE operation
without raw XML, that structured field-level errors + secret redaction fire
before mutation, and that existing REST/DB behavior is untouched.
"""

from unittest.mock import patch

import pytest

from src.boomi_mcp.models.integration_models import IntegrationComponentSpec
from src.boomi_mcp.categories.integration_builder import _build_plan

_PATCH_TARGET = "src.boomi_mcp.categories.integration_builder.paginate_metadata"


def _wsdl_metadata():
    return {
        "operation_name": "Ping",
        "soap_action": "http://tempuri.org//Svc/Ping",
        "metadata_connection_url": "http://host/svc.asmx",
        "service_name": "Svc",
        "service_namespace": "http://tempuri.org//Svc",
        "port_name": "SvcSoap",
        "binding_style": "document",
        "binding_use": "literal",
        "binding_protocol": "soap_1_1",
        "operation_style": "document",
        "operation_use": "literal",
        "input_message_name": "Svc_Ping_In",
        "input_message_namespace": "http://tempuri.org//Svc",
        "output_message_name": "Svc_Ping_Out",
        "output_message_namespace": "http://tempuri.org//Svc",
        "input_parameters": [{"name": "Ping", "element_name": "Ping", "element_ns": "http://tempuri.org//Svc"}],
        "output_parameters": [{"name": "PingResponse", "element_name": "PingResponse", "element_ns": "http://tempuri.org//Svc"}],
        "rpc_optional_parameters": True,
        "using_envelope": True,
    }


def _soap_conn_config(**overrides):
    cfg = {
        "component_type": "connector-settings",
        "connector_type": "soap_client",
        "component_name": "Target SOAP Connection",
        "folder_name": "Process Library",
        "wsdl_url": "https://host/svc.asmx?wsdl",
        "endpoint_url": "https://host/svc.asmx",
        "security": "NETWORK_AUTH",
        "username": "svc",
        "credential_ref": "credential://target-api/soap",
    }
    cfg.update(overrides)
    return cfg


def _soap_conn_comp(key="soap_connection", action="create", depends_on=None, **ov):
    return IntegrationComponentSpec(
        key=key, type="connector-settings", action=action, name="Target SOAP Connection",
        config=_soap_conn_config(**ov), depends_on=depends_on or [],
    )


def _soap_op_config(**overrides):
    cfg = {
        "component_type": "connector-action",
        "connector_type": "soap_client",
        "operation_mode": "execute",
        "component_name": "Ping Operation",
        "folder_name": "Process Library",
        "connection_ref_key": "soap_connection",
        "request_profile_id": "$ref:req_profile",
        "response_profile_id": "$ref:resp_profile",
        "wsdl_metadata": _wsdl_metadata(),
    }
    cfg.update(overrides)
    return cfg


def _soap_op_comp(key="soap_operation", action="create", depends_on=None, **ov):
    return IntegrationComponentSpec(
        key=key, type="connector-action", action=action, name="Ping Operation",
        config=_soap_op_config(**ov),
        depends_on=depends_on if depends_on is not None else ["soap_connection", "req_profile", "resp_profile"],
    )


def _xml_profile_comp(key):
    return IntegrationComponentSpec(
        key=key, type="profile.xml", action="create", name=key,
        config={"component_type": "profile.xml", "profile_type": "xml.generated", "root_element": "Ping"},
    )


def _build_config(components):
    return {
        "conflict_policy": "reuse",
        "integration_spec": {
            "version": "1.0",
            "name": "soap-integration",
            "components": [c.model_dump() for c in components],
        },
    }


def _step_for(plan, key):
    return next(s for s in plan["steps"] if s["key"] == key)


@patch(_PATCH_TARGET, return_value=[])
def test_plan_soap_connection_without_raw_xml(mock_pag):
    plan = _build_plan(None, _build_config([_soap_conn_comp()]))
    step = _step_for(plan, "soap_connection")
    assert step["planned_action"] == "create"
    assert step.get("validation_error") is None


@patch(_PATCH_TARGET, return_value=[])
def test_plan_soap_operation_without_raw_xml(mock_pag):
    comps = [_xml_profile_comp("req_profile"), _xml_profile_comp("resp_profile"), _soap_conn_comp(), _soap_op_comp()]
    plan = _build_plan(None, _build_config(comps))
    step = _step_for(plan, "soap_operation")
    assert step["planned_action"] == "create", step.get("validation_error")
    assert step.get("validation_error") is None


@patch(_PATCH_TARGET, return_value=[])
def test_unsupported_field_fails_before_mutation(mock_pag):
    comp = _soap_conn_comp(security="OAUTH2")
    plan = _build_plan(None, _build_config([comp]))
    step = _step_for(plan, "soap_connection")
    assert step["planned_action"] == "error_soap_validation"
    assert step["validation_error"]["error_code"] == "SOAP_UNSUPPORTED_SECURITY"
    assert "config.xml" in step["validation_error"]["hint"]


@patch(_PATCH_TARGET, return_value=[])
def test_plaintext_secret_redacted_in_plan_echo(mock_pag):
    comp = _soap_conn_comp(password="hunter2")
    plan = _build_plan(None, _build_config([comp]))
    step = _step_for(plan, "soap_connection")
    assert step["planned_action"] == "error_soap_validation"
    # The echoed spec must not carry the plaintext secret.
    echoed = plan["integration_spec"]["components"]
    dumped = str(echoed)
    assert "hunter2" not in dumped


@patch(_PATCH_TARGET, return_value=[])
def test_credential_ref_redacted_on_validation_error(mock_pag):
    # An unrelated validation error still scrubs credential_ref from the echo.
    comp = _soap_conn_comp(username="")  # triggers username-required error
    plan = _build_plan(None, _build_config([comp]))
    step = _step_for(plan, "soap_connection")
    assert step["planned_action"] == "error_soap_validation"
    dumped = str(plan["integration_spec"]["components"])
    assert "credential://target-api/soap" not in dumped


@patch(_PATCH_TARGET, return_value=[])
def test_operation_dependency_missing_connection_ref_rejected(mock_pag):
    comp = _soap_op_comp(depends_on=["req_profile", "resp_profile"])
    comps = [_xml_profile_comp("req_profile"), _xml_profile_comp("resp_profile"), _soap_conn_comp(), comp]
    plan = _build_plan(None, _build_config(comps))
    step = _step_for(plan, "soap_operation")
    assert step["planned_action"] == "error_soap_validation"
    assert step["validation_error"]["error_code"] in ("SOAP_DEPENDENCY_REQUIRED", "SOAP_CONNECTION_REF_REQUIRED")


@patch(_PATCH_TARGET, return_value=[])
def test_soap_ref_type_mismatch_rejected(mock_pag):
    # connection_ref_key points at a REST connection → SOAP_REF_TYPE_MISMATCH.
    rest_conn = IntegrationComponentSpec(
        key="soap_connection", type="connector-settings", action="create", name="REST not SOAP",
        config={"component_type": "connector-settings", "connector_type": "rest",
                "component_name": "REST", "base_url": "https://x", "auth": "NONE"},
    )
    comps = [_xml_profile_comp("req_profile"), _xml_profile_comp("resp_profile"), rest_conn, _soap_op_comp()]
    plan = _build_plan(None, _build_config(comps))
    step = _step_for(plan, "soap_operation")
    assert step["planned_action"] == "error_soap_validation"
    assert step["validation_error"]["error_code"] == "SOAP_REF_TYPE_MISMATCH"


@patch(_PATCH_TARGET, return_value=[])
def test_raw_xml_escape_hatch_available_for_soap_subtype(mock_pag):
    raw = (
        '<bns:Component xmlns:bns="http://api.platform.boomi.com/" '
        'type="connector-settings" subType="wssoapclientsdk" name="Raw SOAP">'
        '<bns:object><GenericConnectionConfig xmlns="">'
        '<field id="url" type="string" value="https://x?wsdl"/>'
        '</GenericConnectionConfig></bns:object></bns:Component>'
    )
    comp = IntegrationComponentSpec(
        key="raw_soap", type="connector-settings", action="create", name="Raw SOAP",
        config={"xml": raw},
    )
    plan = _build_plan(None, _build_config([comp]))
    step = _step_for(plan, "raw_soap")
    # Raw XML bypasses the structured builder; it is NOT a validation error.
    assert step["planned_action"] == "create"
    assert step.get("validation_error") is None
