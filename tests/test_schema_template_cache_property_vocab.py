"""M11.1 (#120): cache_property_authoring vocabulary surface tests.

The surface is read-only reference data: it must advertise the M11 terms
WITHOUT claiming executable support (#120 ships vocabulary only; #121/#122/
#131 flip their terms as their emitters/builders land), and declaring a
reserved kind in a flow_sequence must still fail structural validation.
"""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

_project_root = str(Path(__file__).resolve().parent.parent)
if _project_root not in sys.path:
    sys.path.insert(0, _project_root)

from boomi_mcp.categories.meta_tools import (
    _valid_schema_names,
    get_schema_template_action,
)
from boomi_mcp.models.cache_property_models import (
    DocumentCacheKeyValue,
    PROPERTY_SOURCE_FIELD_CONTRACT,
    PropertyAssignment,
    PropertySourceValue,
)
from src.boomi_mcp.categories.components.builders import (
    BuilderValidationError,
    ProcessFlowBuilder,
)

from pydantic import ValidationError


_M11_TERMS = (
    "set_ddp",
    "set_dpp",
    "get_property",
    "set_process_property",
    "cache_put",
    "cache_get",
    "cache_join",
    "processproperty_component",
    "documentcache_component",
)


def _schema():
    result = get_schema_template_action(schema_name="cache_property_authoring")
    assert result["_success"] is True
    return result


def test_schema_name_is_registered():
    assert "cache_property_authoring" in _valid_schema_names()


def test_surface_is_read_only_and_xml_free():
    result = _schema()
    assert result["read_only"] is True
    assert result["raw_xml_exposed"] is False
    assert result["boomi_mutation"] is False


def test_all_m11_terms_present_and_reserved():
    # #120 ships vocabulary only: every term starts reserved_not_executable.
    # Later children flip their own terms; this test is updated in lockstep.
    terms = _schema()["terms"]
    assert set(terms) == set(_M11_TERMS)
    for name, term in terms.items():
        assert term["capability_status"] == "reserved_not_executable", name
        assert term["meaning"]
        assert term["owning_issue"].startswith("#")


def test_source_value_contract_rendered_from_models():
    contract = _schema()["source_value_contract"]
    assert set(contract) == set(PROPERTY_SOURCE_FIELD_CONTRACT)
    assert contract["profile"]["required"] == [
        "element_id",
        "element_name",
        "profile_id",
        "profile_type",
    ]
    assert contract["definedparameter"]["required"] == [
        "component_id",
        "property_key",
    ]


def test_scopes_documented():
    scopes = _schema()["scopes"]
    assert set(scopes) == {"ddp", "dpp", "processproperty", "documentcache"}


# --- vocabulary models -----------------------------------------------------


def test_property_source_value_contract_enforced():
    PropertySourceValue(value_type="static", value="")
    PropertySourceValue(value_type="current")
    PropertySourceValue(
        value_type="profile",
        element_id="3",
        element_name="count (Root/Object/count)",
        profile_id="e57c31a2-c411-4b7b-a785-c884fe64c6db",
        profile_type="profile.json",
    )
    PropertySourceValue(value_type="ddp", property_name="DDP_SKIP", default_value="0")
    PropertySourceValue(value_type="dpp", property_name="DPP_LIMIT")
    PropertySourceValue(
        value_type="definedparameter",
        component_id="$ref:props",
        property_key="0e89ebf1-cd46-46df-904e-94c7e7ade31e",
    )
    with pytest.raises(ValidationError):
        PropertySourceValue(value_type="static")  # missing value
    with pytest.raises(ValidationError):
        PropertySourceValue(value_type="current", value="x")  # field not accepted
    with pytest.raises(ValidationError):
        PropertySourceValue(value_type="profile", element_id="3")  # incomplete
    with pytest.raises(ValidationError):
        PropertySourceValue(value_type="bogus", value="x")


def test_document_cache_key_value_rejects_zero_id():
    source = PropertySourceValue(value_type="static", value="k")
    DocumentCacheKeyValue(cache_key_id=1, source=source)
    with pytest.raises(ValidationError):
        DocumentCacheKeyValue(cache_key_id=0, source=source)


def test_property_assignment_persist_rules():
    src = [PropertySourceValue(value_type="static", value="v")]
    PropertyAssignment(scope="ddp", name="DDP_X", source_values=src)
    PropertyAssignment(scope="dpp", name="DPP_X", source_values=src, persist=True)
    with pytest.raises(ValidationError):
        PropertyAssignment(scope="ddp", name="DDP_X", source_values=src, persist=True)
    with pytest.raises(ValidationError):
        PropertyAssignment(scope="processproperty", name="X", source_values=src)
    with pytest.raises(ValidationError):
        PropertyAssignment(scope="dpp", name="DPP_X", source_values=[])


# --- vocabulary != executability guard --------------------------------------


def _seq_config(flow_sequence):
    return {
        "process_kind": "database_to_api_sync",
        "source": {
            "connector_type": "database",
            "connection_id": "11111111-1111-1111-1111-111111111111",
            "operation_id": "22222222-2222-2222-2222-222222222222",
            "action_type": "Get",
        },
        "transform": {"mode": "passthrough"},
        "target": {
            "connector_type": "rest",
            "connection_id": "33333333-3333-3333-3333-333333333333",
            "operation_id": "44444444-4444-4444-4444-444444444444",
            "action_type": "POST",
        },
        "flow_sequence": flow_sequence,
    }


def test_reserved_kind_set_ddp_still_rejected_by_process_builder():
    # Reserving vocabulary (#120) must NOT make it executable: the process
    # builder keeps rejecting the kind until #121 ships its emitter.
    err = ProcessFlowBuilder.validate_config(
        _seq_config([{"kind": "set_ddp", "label": "reserved"}])
    )
    assert isinstance(err, BuilderValidationError)
    assert err.error_code == "PROCESS_FLOW_SEQUENCE_CONFIG_INVALID"
    assert "set_ddp" in str(err)
