"""Tests for the patterns package base contracts (Issue #15)."""

import importlib
import json
import sys
from pathlib import Path

import pytest
from pydantic import BaseModel, Field, ValidationError

_project_root = str(Path(__file__).resolve().parent.parent.parent)
if _project_root not in sys.path:
    sys.path.insert(0, _project_root)

from src.boomi_mcp.models.integration_models import (
    IntegrationComponentSpec,
    IntegrationSpecV1,
)
from src.boomi_mcp.patterns import (
    ArchetypePattern,
    NoParameters,
    PatternBase,
    PatternError,
    PatternFieldError,
    PatternIOContract,
    PatternKind,
    PatternMetadata,
    PrimitiveBuildContext,
    PrimitivePattern,
    pattern_validation_error,
)


# ---------------------------------------------------------------------------
# Test fixture subclasses
# ---------------------------------------------------------------------------

class _ExampleParams(BaseModel):
    integration_name: str = Field(..., description="Integration name")
    secret_key: str = Field(default="", description="May contain credentials")


class ExampleArchetype(ArchetypePattern):
    metadata = PatternMetadata(
        name="example_archetype",
        version="1.0.0",
        kind=PatternKind.ARCHETYPE,
        description="Test archetype",
        tags=["test"],
        use_cases=["unit-test"],
        not_for=[],
    )
    parameters_model = _ExampleParams

    @classmethod
    def emit_spec(cls, parameters):
        return IntegrationSpecV1(name=parameters.integration_name)


class ExamplePrimitive(PrimitivePattern):
    metadata = PatternMetadata(
        name="example_primitive",
        version="1.0.0",
        kind=PatternKind.PRIMITIVE,
        description="Test primitive",
    )
    parameters_model = _ExampleParams
    input_contract = PatternIOContract(
        name="in",
        description="input",
        profile_type="json",
        media_type="application/json",
        schema_={"type": "object"},
    )
    output_contract = PatternIOContract(
        name="out",
        description="output",
        profile_type="json",
        media_type="application/json",
    )
    required_builders = ["process_builder"]

    @classmethod
    def emit_components(cls, context, parameters):
        return [
            IntegrationComponentSpec(
                key=f"{context.component_prefix}-stub",
                type="process",
            )
        ]


PUBLIC_NAMES = {
    "ArchetypePattern",
    "NoParameters",
    "PatternBase",
    "PatternError",
    "PatternFieldError",
    "PatternIOContract",
    "PatternKind",
    "PatternMetadata",
    "PrimitiveBuildContext",
    "PrimitivePattern",
    "pattern_validation_error",
}


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

def test_patterns_package_imports_without_credentials():
    module = importlib.import_module("src.boomi_mcp.patterns")
    exported = set(getattr(module, "__all__", ()))
    assert PUBLIC_NAMES.issubset(exported), f"Missing exports: {PUBLIC_NAMES - exported}"
    for name in PUBLIC_NAMES:
        assert hasattr(module, name), f"Module is missing public symbol {name!r}"
    # NoParameters is the safe default for patterns that take no input.
    assert PatternBase.parameters_model is NoParameters


def test_pattern_metadata_accepts_valid_and_rejects_missing():
    archetype_meta = PatternMetadata(
        name="m", version="1.0.0", kind=PatternKind.ARCHETYPE, description="d",
    )
    assert archetype_meta.kind is PatternKind.ARCHETYPE
    assert archetype_meta.tags == []
    assert archetype_meta.use_cases == []
    assert archetype_meta.not_for == []

    primitive_meta = PatternMetadata(
        name="m",
        version="1.0.0",
        kind=PatternKind.PRIMITIVE,
        description="d",
        tags=["t"],
        use_cases=["u"],
        not_for=["n"],
    )
    assert primitive_meta.kind is PatternKind.PRIMITIVE
    assert primitive_meta.tags == ["t"]

    with pytest.raises(ValidationError):
        PatternMetadata(version="1.0.0", kind=PatternKind.ARCHETYPE, description="d")
    with pytest.raises(ValidationError):
        PatternMetadata(name="m", version="1.0.0", description="d")


def test_archetype_validates_and_emits_spec():
    params = ExampleArchetype.validate_parameters(
        {"integration_name": "demo", "secret_key": "abc"}
    )
    assert isinstance(params, _ExampleParams)
    assert params.integration_name == "demo"

    spec = ExampleArchetype.emit_spec(params)
    assert isinstance(spec, IntegrationSpecV1)
    assert spec.name == "demo"
    assert spec.version == "1.0"


def test_primitive_validates_and_emits_components():
    params = ExamplePrimitive.validate_parameters(
        {"integration_name": "demo", "secret_key": "abc"}
    )
    ctx = PrimitiveBuildContext(
        integration_name="demo",
        component_prefix="DEMO",
        folder_path="/Test",
    )
    components = ExamplePrimitive.emit_components(ctx, params)
    assert isinstance(components, list)
    assert len(components) == 1
    assert isinstance(components[0], IntegrationComponentSpec)
    assert components[0].key == "DEMO-stub"
    assert components[0].type == "process"


def test_parameter_schema_contains_field_names():
    schema = ExampleArchetype.parameter_schema()
    assert isinstance(schema, dict)
    properties = schema.get("properties", {})
    assert "integration_name" in properties
    assert "secret_key" in properties


def test_describe_includes_metadata_schema_and_primitive_contracts():
    arch_described = ExampleArchetype.describe()
    assert set(arch_described.keys()) == {"metadata", "parameter_schema"}
    assert arch_described["metadata"]["name"] == "example_archetype"
    assert arch_described["metadata"]["kind"] == "archetype"
    assert arch_described["metadata"]["tags"] == ["test"]
    assert "properties" in arch_described["parameter_schema"]

    prim_described = ExamplePrimitive.describe()
    expected_keys = {
        "metadata",
        "parameter_schema",
        "input_contract",
        "output_contract",
        "required_builders",
    }
    assert expected_keys <= set(prim_described.keys())

    input_contract = prim_described["input_contract"]
    assert input_contract is not None
    assert "schema" in input_contract
    assert "schema_" not in input_contract
    assert input_contract["schema"] == {"type": "object"}

    output_contract = prim_described["output_contract"]
    assert output_contract is not None
    assert output_contract["schema"] is None

    assert prim_described["required_builders"] == ["process_builder"]


def test_invalid_parameters_raises_validation_error():
    with pytest.raises(ValidationError):
        ExampleArchetype.validate_parameters({})


def test_validate_parameters_normalizes_none_and_accepts_model_instance():
    class _NoParamArchetype(ArchetypePattern):
        metadata = PatternMetadata(
            name="np", version="1.0.0", kind=PatternKind.ARCHETYPE, description="d",
        )

        @classmethod
        def emit_spec(cls, parameters):
            return IntegrationSpecV1(name="np-demo")

    # None must normalize to {} so callers don't have to pass an empty dict
    # when a pattern declares no parameters.
    none_params = _NoParamArchetype.validate_parameters(None)
    assert isinstance(none_params, NoParameters)

    # Omitting the argument entirely behaves the same.
    default_params = _NoParamArchetype.validate_parameters()
    assert isinstance(default_params, NoParameters)

    # Passing an already-validated model instance must be idempotent.
    params = ExampleArchetype.validate_parameters({"integration_name": "demo"})
    again = ExampleArchetype.validate_parameters(params)
    assert again is params


def test_optional_contract_and_context_fields_default_to_none():
    # Operation-style primitives (schedule, watermark, DLQ, run metadata) need
    # to be able to declare a contract without faking media/profile types.
    minimal_contract = PatternIOContract(name="schedule_trigger")
    assert minimal_contract.description is None
    assert minimal_contract.profile_type is None
    assert minimal_contract.media_type is None
    assert minimal_contract.schema_ is None

    # And the build context allows omitting folder_path for primitives that
    # use the integration default.
    minimal_ctx = PrimitiveBuildContext(
        integration_name="demo", component_prefix="DEMO",
    )
    assert minimal_ctx.folder_path is None
    assert minimal_ctx.refs == {}


def test_pattern_validation_error_sanitizes_input():
    secret = "SECRET_VALUE_DO_NOT_LEAK"
    captured: ValidationError
    try:
        _ExampleParams.model_validate({"secret_key": secret})
    except ValidationError as exc:
        captured = exc
    else:
        pytest.fail("Expected ValidationError for missing integration_name")

    err = pattern_validation_error(captured, suggestion="Provide integration_name")
    assert isinstance(err, PatternError)
    assert err.error_code == "PARAM_VALIDATION_FAILED"
    assert err.field_errors and isinstance(err.field_errors[0], PatternFieldError)

    payload = err.to_dict()
    assert payload["_success"] is False
    assert payload["error_code"] == "PARAM_VALIDATION_FAILED"
    assert payload["error"] == "Parameter validation failed"
    assert payload["suggestion"] == "Provide integration_name"
    assert payload["retryable"] is False
    assert payload["context"] == {}
    assert isinstance(payload["field_errors"], list)
    assert len(payload["field_errors"]) >= 1

    first = payload["field_errors"][0]
    assert set(first.keys()) >= {"field_path", "message", "error_type"}
    assert first["field_path"] == "integration_name"

    # Privacy guarantee: the raw input (which Pydantic echoes under ``input``)
    # must never appear anywhere in the serialized error.
    dumped = json.dumps(payload)
    assert secret not in dumped


def test_to_dict_excludes_none_optional_fields():
    try:
        _ExampleParams.model_validate({})
    except ValidationError as exc:
        captured = exc
    else:
        pytest.fail("Expected ValidationError")

    # No suggestion passed → ``suggestion`` should be absent from the MCP
    # payload, not serialized as ``null``.
    err = pattern_validation_error(captured)
    payload = err.to_dict()
    assert "suggestion" not in payload
    assert payload["_success"] is False
    assert payload["error_code"] == "PARAM_VALIDATION_FAILED"
