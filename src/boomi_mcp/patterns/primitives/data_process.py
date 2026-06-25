"""Issue #106 (M10.2): ``data_process`` transform primitive.

A fragment-only primitive that declares a process-level Data Process shape — the
"Swiss army knife" document-manipulation step — as a ``transform.mode='dataprocess'``
process fragment consumed by ``ProcessFlowBuilder``. v1 supports only the
live-observed Custom Scripting operation (the dominant real use, captured from a
live account export); other documented Data Process operations are deferred until
each has its own byte-accurate live capture (the builder rejects them with
``PROCESS_DATAPROCESS_OPERATION_UNSUPPORTED``).

It emits NO standalone components (``emit_components`` -> ``[]``); the operation
steps live inline on the process shape, so the primitive only contributes a
``process_config`` fragment plus an empty ``depends_on``.
"""

from __future__ import annotations

from typing import Any, Dict, List, Literal, Optional

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


class DataProcessParameters(BaseModel):
    model_config = ConfigDict(extra="forbid")

    label: Optional[str] = Field(default=None, description="Descriptive shape label")
    steps: List[CustomScriptingStep] = Field(
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
            "step) as a transform fragment. v1 supports the Custom Scripting "
            "operation; other Data Process operations are deferred."
        ),
        tags=["transform", "data-process", "custom-scripting"],
        use_cases=[
            "Tag or transform documents with a small Custom Scripting step",
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
        transform: Dict[str, Any] = {
            "mode": "dataprocess",
            "steps": [step.model_dump() for step in params.steps],
        }
        if params.label:
            transform["label"] = params.label
        return {"process_config": {"transform": transform}, "depends_on": []}
