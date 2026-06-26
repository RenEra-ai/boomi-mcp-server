"""Issue #106 (M10.2) / #115 (M10.2a): ``data_process`` transform primitive.

A fragment-only primitive that declares a process-level Data Process shape — the
"Swiss army knife" document-manipulation step — as a ``transform.mode='dataprocess'``
process fragment consumed by ``ProcessFlowBuilder``. v1 supports the live-observed
Custom Scripting operation plus the two profile-driven, cardinality-changing
operations Split Documents (1->N) and Combine Documents (N->1); other documented
Data Process operations are deferred until each has its own byte-accurate live
capture (the builder rejects them with ``PROCESS_DATAPROCESS_OPERATION_UNSUPPORTED``).

It emits NO standalone components (``emit_components`` -> ``[]``); the operation
steps live inline on the process shape, so the primitive only contributes a
``process_config`` fragment plus the ``depends_on`` keys any Split/Combine step's
``$ref:`` ``profile_id`` references (custom-scripting-only fragments declare none).
"""

from __future__ import annotations

from typing import Annotated, Any, Dict, List, Literal, Optional, Union

from pydantic import BaseModel, ConfigDict, Field, model_validator

from ...models.integration_models import IntegrationComponentSpec
from ..base import (
    PatternIOContract,
    PatternKind,
    PatternMetadata,
    PrimitiveBuildContext,
    PrimitivePattern,
)


# ---------------------------------------------------------------------------
# Parameter models (strict)
# ---------------------------------------------------------------------------


class CustomScriptingStep(BaseModel):
    """One Custom Scripting (Groovy) Data Process step.

    ``operation`` is fixed to ``custom_scripting`` in v1. ``language`` accepts
    only the ``groovy2`` engine and ``use_cache`` only ``True`` — the exact
    values the builder emits and the platform requires. The ``Literal`` field
    types reject any other operation/language/cache value at validation time,
    matching the builder's ``PROCESS_DATAPROCESS_*`` rejections.
    """

    model_config = ConfigDict(extra="forbid")

    operation: Literal["custom_scripting"] = Field(
        ..., description="Data Process operation (v1: 'custom_scripting')"
    )
    script: str = Field(..., min_length=1, description="Custom Scripting body")
    language: Literal["groovy2"] = Field(default="groovy2", description="Script engine (only 'groovy2')")
    use_cache: Literal[True] = Field(default=True, description="Script compilation caching (must be true)")

    @model_validator(mode="after")
    def _non_blank_script(self) -> "CustomScriptingStep":
        if not self.script.strip():
            raise ValueError("script must be a non-empty string")
        return self


class SplitDocumentsStep(BaseModel):
    """One Split Documents (processtype 8) Data Process step (issue #115 M10.2a).

    Splits one document into N (1->N) on a repeating element of the referenced
    JSON/XML profile. ``profile_id`` is the profile component ($ref:KEY token in
    ``depends_on`` or a literal id); ``link_element_key`` / ``link_element_name``
    are the opaque UI-captured tokens identifying the split element. All
    caller-authored — no canned/templated values.
    """

    model_config = ConfigDict(extra="forbid")

    operation: Literal["split_documents"] = Field(
        ..., description="Data Process operation"
    )
    profile_type: Literal["json", "xml"] = Field(
        ..., description="Referenced profile kind (json|xml)"
    )
    profile_id: str = Field(..., min_length=1, description="Profile component ($ref:KEY or id)")
    link_element_key: str = Field(..., min_length=1, description="Split element key from the profile")
    link_element_name: str = Field(..., min_length=1, description="Human-readable path to the split element")

    @model_validator(mode="after")
    def _non_blank(self) -> "SplitDocumentsStep":
        for name in ("profile_id", "link_element_key", "link_element_name"):
            if not getattr(self, name).strip():
                raise ValueError(f"{name} must be a non-empty string")
        return self


class CombineDocumentsStep(BaseModel):
    """One Combine Documents (processtype 9) Data Process step (issue #115 M10.2a).

    Combines N documents into one (N->1) under a repeating element of the
    referenced JSON/XML profile. ``combine_into_link_element_key`` defaults to the
    literal ``"null"`` (combine into the document root). All fields are
    caller-authored — no canned/templated values.
    """

    model_config = ConfigDict(extra="forbid")

    operation: Literal["combine_documents"] = Field(
        ..., description="Data Process operation"
    )
    profile_type: Literal["json", "xml"] = Field(
        ..., description="Referenced profile kind (json|xml)"
    )
    profile_id: str = Field(..., min_length=1, description="Profile component ($ref:KEY or id)")
    link_element_key: str = Field(..., min_length=1, description="Combine element key from the profile")
    link_element_name: str = Field(..., min_length=1, description="Human-readable path to the combine element")
    combine_into_link_element_key: str = Field(
        default="null", min_length=1, description="Parent element key (literal 'null' = root)"
    )

    @model_validator(mode="after")
    def _non_blank(self) -> "CombineDocumentsStep":
        for name in (
            "profile_id",
            "link_element_key",
            "link_element_name",
            "combine_into_link_element_key",
        ):
            if not getattr(self, name).strip():
                raise ValueError(f"{name} must be a non-empty string")
        return self


# Discriminated union: ``operation`` selects the step model (matches the builder's
# per-operation validation and the PROCESS_DATAPROCESS_OPERATION_UNSUPPORTED gate).
DataProcessStep = Annotated[
    Union[CustomScriptingStep, SplitDocumentsStep, CombineDocumentsStep],
    Field(discriminator="operation"),
]


class DataProcessParameters(BaseModel):
    model_config = ConfigDict(extra="forbid")

    label: Optional[str] = Field(default=None, description="Descriptive shape label")
    steps: List[DataProcessStep] = Field(
        ..., min_length=1, description="Ordered Data Process operation steps"
    )


# ---------------------------------------------------------------------------
# Primitive
# ---------------------------------------------------------------------------


class DataProcessPrimitive(PrimitivePattern):
    """Declare a process-level Data Process (Custom Scripting) transform fragment."""

    metadata = PatternMetadata(
        name="data_process",
        version="1.0.0",
        kind=PatternKind.PRIMITIVE,
        description=(
            "Declare a process-level Data Process shape (document manipulation "
            "step) as a transform fragment. Supports the Custom Scripting "
            "operation and the native profile-driven cardinality operations "
            "Split Documents (1->N) and Combine Documents (N->1); other Data "
            "Process operations are deferred."
        ),
        tags=[
            "transform",
            "data-process",
            "custom-scripting",
            "split-documents",
            "combine-documents",
        ],
        use_cases=[
            "Tag or transform documents with a small Custom Scripting step",
            "Split one document into many on a JSON array / XML repeating element "
            "(native, profile-driven — no throwaway script)",
            "Combine many documents into one under a JSON array / XML repeating "
            "element (native, profile-driven)",
            "Manipulate the document stream at the process level (not in a map)",
        ],
        not_for=[
            "Structured field-to-field mapping (use field_map)",
            "Operations without a byte-accurate live capture (deferred)",
            "Large or complex scripts — prefer native components",
        ],
    )
    parameters_model = DataProcessParameters

    input_contract = PatternIOContract(
        name="document_stream",
        description="Inbound documents to be manipulated by the Data Process step.",
    )
    output_contract = PatternIOContract(
        name="processed_document_stream",
        description="Documents after the Data Process operation steps run.",
    )
    required_builders = ["ProcessFlowBuilder"]

    @classmethod
    def emit_components(
        cls,
        context: PrimitiveBuildContext,
        parameters: BaseModel,
    ) -> List[IntegrationComponentSpec]:
        # Fragment-only: the Data Process steps live inline on the process shape,
        # so this primitive materializes no standalone components.
        return []

    @classmethod
    def emit_fragment(
        cls,
        context: PrimitiveBuildContext,
        parameters: BaseModel,
    ) -> Dict[str, Any]:
        params: DataProcessParameters = parameters  # type: ignore[assignment]
        steps = [step.model_dump() for step in params.steps]
        transform: Dict[str, Any] = {
            "mode": "dataprocess",
            "steps": steps,
        }
        if params.label:
            transform["label"] = params.label
        # A split/combine step's profile_id may be a literal component id OR a
        # "$ref:KEY" token. Per the emit_fragment contract (see
        # base.PrimitivePattern), depends_on must list every component key the
        # process_config references — otherwise the merged process component fails
        # ProcessFlowBuilder.validate_config with MISSING_PROCESS_DEPENDENCY on the
        # unreachable ref (mirrors the document_cache_retrieve / branch primitives'
        # $ref collection). custom_scripting steps carry no profile_id and add none.
        depends_on: List[str] = []
        for step in steps:
            profile_id = step.get("profile_id")
            if isinstance(profile_id, str) and profile_id.startswith("$ref:"):
                key = profile_id[len("$ref:"):]
                if key and key not in depends_on:
                    depends_on.append(key)
        return {"process_config": {"transform": transform}, "depends_on": depends_on}
