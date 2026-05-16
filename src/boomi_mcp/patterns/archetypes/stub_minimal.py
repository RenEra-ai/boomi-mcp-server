"""Stub minimal integration archetype: validates parameters and emits a zero-component spec."""

from __future__ import annotations

from pydantic import BaseModel, ConfigDict, field_validator

from ...models.integration_models import IntegrationSpecV1
from ..base import ArchetypePattern, PatternKind, PatternMetadata


class StubMinimalIntegrationParameters(BaseModel):
    model_config = ConfigDict(extra="forbid")

    integration_name: str
    goal: str = "Validate V3 archetype build path without Boomi mutation."
    source_label: str = "Stub source"
    target_label: str = "Stub target"
    component_prefix: str = "STUB"

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
