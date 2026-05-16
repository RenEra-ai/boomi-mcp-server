"""Stub minimal integration archetype: validates parameters and emits a zero-component spec."""

from __future__ import annotations

from pydantic import BaseModel, ConfigDict, Field, field_validator

from ...models.integration_models import IntegrationSpecV1
from ..base import ArchetypePattern, PatternExample, PatternKind, PatternMetadata


class StubMinimalIntegrationParameters(BaseModel):
    model_config = ConfigDict(extra="forbid")

    integration_name: str = Field(
        ...,
        description="Logical integration name; used as the emitted IntegrationSpecV1.name.",
    )
    goal: str = Field(
        default="Validate V3 archetype build path without Boomi mutation.",
        description="Human-readable goal recorded in the emitted spec's goals list.",
    )
    source_label: str = Field(
        default="Stub source",
        description="Display label for the stub source endpoint in the emitted spec.",
    )
    target_label: str = Field(
        default="Stub target",
        description="Display label for the stub target endpoint in the emitted spec.",
    )
    component_prefix: str = Field(
        default="STUB",
        description="Prefix recorded under spec.naming.component_prefix; the stub emits zero components, so this is illustrative only.",
    )

    @field_validator(
        "integration_name",
        "goal",
        "source_label",
        "target_label",
        "component_prefix",
    )
    @classmethod
    def _strip_and_reject_blank(cls, value: str) -> str:
        stripped = value.strip()
        if not stripped:
            raise ValueError("must not be blank")
        return stripped


class StubMinimalIntegrationArchetype(ArchetypePattern):
    metadata = PatternMetadata(
        name="stub_minimal_integration",
        version="0.1.0",
        kind=PatternKind.ARCHETYPE,
        description="Non-executable stub archetype for framework validation.",
        tags=["stub", "test", "safe", "no-boomi-mutation"],
        use_cases=["framework smoke test", "MCP wrapper validation"],
        not_for=["real integration creation", "production integration creation"],
    )
    parameters_model = StubMinimalIntegrationParameters

    capability_notes = [
        "Validates the V3 archetype pipeline end-to-end without touching Boomi.",
        "Emits a zero-component IntegrationSpecV1, so build_integration(action='plan') reports zero executable steps.",
        "Useful as a smoke test for MCP wrappers, the pattern registry, and the build planner.",
    ]
    limitations = [
        "Does not represent any real integration shape or business workflow.",
        "Emits no executable Boomi components; build_integration(action='apply') has nothing to mutate.",
        "Must never be used as a starting point for production authoring — choose a real archetype in M2+ when those land.",
    ]
    examples = [
        PatternExample(
            name="smoke_test_run",
            description=(
                "Illustrative parameter set for a framework smoke test. Demonstrates which "
                "parameters the stub archetype accepts; the emitted spec deliberately does no "
                "Boomi work and contains no executable components."
            ),
            parameters={
                "integration_name": "demo-stub-integration",
                "goal": "Smoke-test the V3 archetype pipeline",
                "source_label": "Stub source",
                "target_label": "Stub target",
                "component_prefix": "STUB",
            },
        ),
    ]

    @classmethod
    def emit_spec(
        cls, parameters: StubMinimalIntegrationParameters
    ) -> IntegrationSpecV1:
        return IntegrationSpecV1(
            version="1.0",
            name=parameters.integration_name,
            mode="redesign",
            components=[],
            goals=[
                parameters.goal,
                "Stub archetype emits no executable Boomi components.",
            ],
            endpoints=[
                {
                    "key": "stub_source",
                    "type": "stub",
                    "direction": "source",
                    "label": parameters.source_label,
                },
                {
                    "key": "stub_target",
                    "type": "stub",
                    "direction": "target",
                    "label": parameters.target_label,
                },
            ],
            flows=[
                {
                    "key": "stub_noop_flow",
                    "name": "Stub no-op flow",
                    "source": "stub_source",
                    "target": "stub_target",
                    "operation": "noop",
                    "executable": False,
                },
            ],
            naming={
                "archetype": "stub_minimal_integration",
                "component_prefix": parameters.component_prefix,
            },
            folders={},
            runtime={},
            validation_rules={
                "no_boomi_mutation": True,
                "raw_xml_exposed": False,
                "component_count": 0,
            },
        )
