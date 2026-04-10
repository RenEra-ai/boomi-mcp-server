"""Regression tests for BUG-11: create/update_map_udf nested step TypeError.

MapExtensionsFunctionStep requires configuration, inputs, and outputs as
positional args.  _normalize_udf_data() must inject empty defaults so
callers don't need to know the SDK wrapper shape.

Three key variants must all work:
  - steps.Step   — SDK _unmap native form
  - Steps.Step   — Boomi wire / API JSON format
  - steps.step   — round-tripped through _sdk_to_dict (read-modify-write)
"""

import pytest
from types import SimpleNamespace
from unittest.mock import MagicMock

from boomi.models import EnvironmentMapExtensionUserDefinedFunction

from boomi_mcp.categories.environments import (
    _action_create_map_udf,
    _action_update_map_udf,
    _normalize_udf_data,
)


MINIMAL_STEP = {"position": 1, "type": "CustomScripting"}


def _make_sdk(return_obj=None):
    sdk = MagicMock()
    obj = return_obj or SimpleNamespace(id_="udf-1", name="TestUDF")
    sdk.environment_map_extension_user_defined_function \
        .create_environment_map_extension_user_defined_function.return_value = obj
    sdk.environment_map_extension_user_defined_function \
        .update_environment_map_extension_user_defined_function.return_value = obj
    return sdk


def _assert_step_roundtrip(udf_data):
    """Normalize, _unmap, _map and verify the step survives."""
    normalized = _normalize_udf_data(udf_data)
    obj = EnvironmentMapExtensionUserDefinedFunction._unmap(normalized)
    mapped = obj._map()
    step_list = mapped["Steps"]["Step"]
    assert len(step_list) == 1
    assert step_list[0]["position"] == 1
    assert step_list[0]["type"] == "CustomScripting"


# -- _normalize_udf_data unit tests --


def test_normalize_injects_defaults_for_steps():
    data = {"name": "UDF", "steps": {"Step": [dict(MINIMAL_STEP)]}}
    result = _normalize_udf_data(data)
    step = result["steps"]["Step"][0]
    assert step["configuration"] == {}
    assert step["inputs"] == {}
    assert step["outputs"] == {}
    assert step["position"] == 1


def test_normalize_preserves_existing_nested_fields():
    data = {
        "steps": {
            "Step": [{
                "position": 1,
                "configuration": {"Scripting": {"script": "x"}},
                "inputs": {"Input": [{"name": "a"}]},
                "outputs": {"Output": [{"name": "b"}]},
            }]
        },
    }
    result = _normalize_udf_data(data)
    step = result["steps"]["Step"][0]
    assert step["configuration"] == {"Scripting": {"script": "x"}}
    assert step["inputs"] == {"Input": [{"name": "a"}]}


def test_normalize_no_steps_passthrough():
    data = {"name": "SimpleUDF", "description": "desc"}
    result = _normalize_udf_data(data)
    assert result == data


def test_normalize_does_not_mutate_original():
    data = {"steps": {"Step": [{"position": 1}]}}
    _normalize_udf_data(data)
    assert "configuration" not in data["steps"]["Step"][0]


# -- Key variant tests: steps.Step, Steps.Step, steps.step --


@pytest.mark.parametrize("outer,inner", [
    ("steps", "Step"),    # SDK _unmap native form
    ("Steps", "Step"),    # wire / API JSON format
    ("steps", "step"),    # _sdk_to_dict round-trip form
])
def test_normalize_handles_key_variant(outer, inner):
    """All three key combos must normalize to a shape _unmap can handle."""
    data = {outer: {inner: [dict(MINIMAL_STEP)]}}
    _assert_step_roundtrip(data)


@pytest.mark.parametrize("outer,inner", [
    ("steps", "Step"),
    ("Steps", "Step"),
    ("steps", "step"),
])
def test_create_action_handles_key_variant(outer, inner):
    """create_map_udf must accept all key variants and produce a populated
    request_body with actual step entries."""
    sdk = _make_sdk()
    result = _action_create_map_udf(
        sdk, profile="dev",
        udf_data={"name": "TestUDF", outer: {inner: [dict(MINIMAL_STEP)]}},
    )
    assert result["_success"] is True

    call_args = sdk.environment_map_extension_user_defined_function \
        .create_environment_map_extension_user_defined_function.call_args
    request_body = call_args.kwargs["request_body"]
    mapped = request_body._map()
    step_list = mapped["Steps"]["Step"]
    assert len(step_list) == 1
    assert step_list[0]["position"] == 1


@pytest.mark.parametrize("outer,inner", [
    ("steps", "Step"),
    ("Steps", "Step"),
    ("steps", "step"),
])
def test_update_action_handles_key_variant(outer, inner):
    """update_map_udf must accept all key variants."""
    sdk = _make_sdk()
    result = _action_update_map_udf(
        sdk, profile="dev",
        resource_id="udf-1",
        udf_data={"name": "Updated", outer: {inner: [dict(MINIMAL_STEP)]}},
    )
    assert result["_success"] is True

    call_args = sdk.environment_map_extension_user_defined_function \
        .update_environment_map_extension_user_defined_function.call_args
    request_body = call_args.kwargs["request_body"]
    mapped = request_body._map()
    step_list = mapped["Steps"]["Step"]
    assert len(step_list) == 1
    assert step_list[0]["position"] == 1


# -- Validation tests --


def test_create_udf_missing_udf_data():
    result = _action_create_map_udf(MagicMock(), profile="dev")
    assert result["_success"] is False
    assert "udf_data" in result["error"]


def test_create_udf_simple_payload():
    sdk = _make_sdk()
    result = _action_create_map_udf(
        sdk, profile="dev",
        udf_data={"name": "Simple", "description": "no steps"},
    )
    assert result["_success"] is True


def test_update_udf_missing_resource_id():
    result = _action_update_map_udf(
        MagicMock(), profile="dev",
        udf_data={"name": "x"},
    )
    assert result["_success"] is False
    assert "resource_id" in result["error"]


def test_create_udf_without_normalize_would_crash():
    """Prove the bug exists without normalization — direct _unmap with Step
    entries missing config/inputs/outputs raises TypeError."""
    try:
        EnvironmentMapExtensionUserDefinedFunction._unmap({
            "name": "test",
            "steps": {"Step": [{"position": 1, "type": "CustomScripting"}]},
        })
        assert False, "_unmap should have raised TypeError"
    except TypeError as exc:
        assert "configuration" in str(exc)
