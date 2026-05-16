"""Base contracts for the patterns package: enums, models, and ABC hierarchy."""

from abc import ABC, abstractmethod
from enum import Enum
from typing import Any, ClassVar, Dict, List, Optional, Type

from pydantic import BaseModel, ConfigDict, Field

from ..models.integration_models import (
    IntegrationComponentSpec,
    IntegrationSpecV1,
)


class PatternKind(str, Enum):
    """Discriminator for archetype vs primitive patterns."""

    ARCHETYPE = "archetype"
    PRIMITIVE = "primitive"


class PatternMetadata(BaseModel):
    """Descriptive metadata shared by archetypes and primitives."""

    name: str = Field(..., description="Unique pattern name")
    version: str = Field(..., description="Semantic version of the pattern contract")
    kind: PatternKind = Field(..., description="Whether this pattern is an archetype or primitive")
    description: str = Field(..., description="Human-readable summary")
    tags: List[str] = Field(default_factory=list, description="Free-form discovery tags")
    use_cases: List[str] = Field(default_factory=list, description="Recommended use cases")
    not_for: List[str] = Field(default_factory=list, description="Explicit anti-use cases")


class PatternIOContract(BaseModel):
    """Input or output profile contract for a primitive pattern."""

    # ``schema`` is the public JSON key but shadows ``BaseModel.schema``; the
    # Python attribute is ``schema_`` and the alias keeps the wire format clean.
    model_config = ConfigDict(populate_by_name=True)

    name: str = Field(..., description="Logical contract name")
    description: str = Field(..., description="Human-readable contract summary")
    profile_type: str = Field(..., description="Profile family (json, xml, edi, flatfile, database)")
    media_type: str = Field(..., description="MIME-style media type identifier")
    schema_: Optional[Dict[str, Any]] = Field(
        default=None,
        alias="schema",
        description="Optional structural schema for the profile (e.g. JSON Schema)",
    )


class PrimitiveBuildContext(BaseModel):
    """Build-time context handed to a primitive's emit_components method."""

    integration_name: str = Field(..., description="Owning integration name")
    component_prefix: str = Field(..., description="Component name prefix to use")
    folder_path: str = Field(..., description="Target Boomi folder path")
    refs: Dict[str, Any] = Field(default_factory=dict, description="References to sibling components")


class NoParameters(BaseModel):
    """Default ``parameters_model`` for patterns that take no input."""

    model_config = ConfigDict(extra="forbid")


class PatternBase(ABC):
    """Abstract base for all archetype and primitive patterns."""

    metadata: ClassVar[PatternMetadata]
    parameters_model: ClassVar[Type[BaseModel]] = NoParameters

    @classmethod
    def parameter_schema(cls) -> Dict[str, Any]:
        return cls.parameters_model.model_json_schema()

    @classmethod
    def describe(cls) -> Dict[str, Any]:
        return {
            "metadata": cls.metadata.model_dump(mode="json"),
            "parameter_schema": cls.parameter_schema(),
        }

    @classmethod
    def validate_parameters(cls, parameters: Dict[str, Any]) -> BaseModel:
        return cls.parameters_model.model_validate(parameters)


class ArchetypePattern(PatternBase):
    """Abstract archetype pattern: emits a full IntegrationSpecV1."""

    @classmethod
    @abstractmethod
    def emit_spec(cls, parameters: BaseModel) -> IntegrationSpecV1:
        ...


class PrimitivePattern(PatternBase):
    """Abstract primitive pattern: emits one or more components for a builder."""

    input_contract: ClassVar[Optional[PatternIOContract]] = None
    output_contract: ClassVar[Optional[PatternIOContract]] = None
    required_builders: ClassVar[List[str]] = []

    @classmethod
    @abstractmethod
    def emit_components(
        cls,
        context: PrimitiveBuildContext,
        parameters: BaseModel,
    ) -> List[IntegrationComponentSpec]:
        ...

    @classmethod
    def describe(cls) -> Dict[str, Any]:
        described = super().describe()
        described["input_contract"] = (
            cls.input_contract.model_dump(by_alias=True) if cls.input_contract else None
        )
        described["output_contract"] = (
            cls.output_contract.model_dump(by_alias=True) if cls.output_contract else None
        )
        described["required_builders"] = list(cls.required_builders)
        return described
