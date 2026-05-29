"""Base contracts for the patterns package: enums, models, and ABC hierarchy."""

from abc import ABC, abstractmethod
from enum import Enum
from typing import Any, ClassVar, Dict, List, Literal, Mapping, Optional, Type, Union

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

    # Only ``name`` is required: operation primitives (schedule, watermark,
    # error classifier, DLQ, run metadata) don't carry a document profile and
    # shouldn't have to invent media/profile types just to satisfy the contract.
    name: str = Field(..., description="Logical contract name")
    description: Optional[str] = Field(default=None, description="Human-readable contract summary")
    profile_type: Optional[str] = Field(
        default=None,
        description="Profile family (json, xml, edi, flatfile, database); omit for operation primitives",
    )
    media_type: Optional[str] = Field(
        default=None,
        description="MIME-style media type identifier; omit for operation primitives",
    )
    schema_: Optional[Dict[str, Any]] = Field(
        default=None,
        alias="schema",
        description="Optional structural schema for the profile (e.g. JSON Schema)",
    )


class PrimitiveBuildContext(BaseModel):
    """Build-time context handed to a primitive's emit_components method."""

    integration_name: str = Field(..., description="Owning integration name")
    component_prefix: str = Field(..., description="Component name prefix to use")
    folder_path: Optional[str] = Field(
        default=None,
        description="Target Boomi folder path; primitives may fall back to the integration default",
    )
    refs: Dict[str, Any] = Field(default_factory=dict, description="References to sibling components")


class NoParameters(BaseModel):
    """Default ``parameters_model`` for patterns that take no input."""

    model_config = ConfigDict(extra="forbid")


class PatternExample(BaseModel):
    """Documentation-only example for an archetype.

    Examples are illustrative payloads, never reusable templates. ``is_template``
    and ``template_status`` are constrained at the type level so an example cannot
    be constructed in a way that would mistake it for a hidden template.
    """

    model_config = ConfigDict(extra="forbid")

    name: str = Field(..., description="Short label for the example (not a template id)")
    description: str = Field(..., description="Plain-English description of the scenario")
    parameters: Dict[str, Any] = Field(
        default_factory=dict,
        description="Example parameter values — illustrative only, not a reusable template",
    )
    is_template: Literal[False] = False
    template_status: Literal["example_only_not_reusable_template"] = (
        "example_only_not_reusable_template"
    )


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
    def validate_parameters(
        cls,
        parameters: Optional[Union[Mapping[str, Any], BaseModel]] = None,
    ) -> BaseModel:
        # ``None`` is normalized to ``{}`` so patterns whose parameters_model is
        # NoParameters (or any model with all-defaulted fields) can be invoked
        # without callers having to pass an empty dict.
        if parameters is None:
            parameters = {}
        if isinstance(parameters, cls.parameters_model):
            return parameters
        return cls.parameters_model.model_validate(parameters)


class ArchetypePattern(PatternBase):
    """Abstract archetype pattern: emits a full IntegrationSpecV1."""

    capability_notes: ClassVar[List[str]] = []
    limitations: ClassVar[List[str]] = []
    examples: ClassVar[List[PatternExample]] = []

    @classmethod
    @abstractmethod
    def emit_spec(cls, parameters: BaseModel) -> IntegrationSpecV1:
        ...

    @classmethod
    def describe(cls) -> Dict[str, Any]:
        described = super().describe()
        described["capability_notes"] = list(cls.capability_notes)
        described["limitations"] = list(cls.limitations)
        described["examples"] = [ex.model_dump(mode="json") for ex in cls.examples]
        described["example_policy"] = "example_only_not_reusable_template"
        return described


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
    def emit_fragment(
        cls,
        context: PrimitiveBuildContext,
        parameters: BaseModel,
    ) -> Dict[str, Any]:
        """Return a structured planning fragment for issue #29 assembly.

        Default is an empty dict, so component-emitting primitives (issue #27)
        stay source-compatible without overriding this method. Operation
        primitives — schedule, watermark, error classifier, DLQ, run metadata —
        override it to describe process/archetype intent *without* inventing
        placeholder Boomi components.

        The returned dict is a free-form fragment; recognized top-level keys
        (all optional) are:

          * ``components``    — list of ``IntegrationComponentSpec`` when the
                                primitive also materializes real components
                                (a target primitive may set both this and
                                ``process_config``).
          * ``process_config``— a process-component config fragment keyed by
                                ``source`` / ``target`` / ``execution`` /
                                ``reliability`` / ``transform``.
          * ``depends_on``    — component keys the ``process_config`` references.
          * ``metadata``      — primitive-specific planning metadata.

        It is representation only: emitting a ``reliability`` fragment never
        un-gates ``ProcessFlowBuilder``'s ``PROCESS_RETRY_UNVERIFIED`` check.
        """
        return {}

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
