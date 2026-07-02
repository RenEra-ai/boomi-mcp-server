"""Issue #122 (M11.3, epic #118): ``document_cache_put`` primitive.

A fragment-only primitive that declares a SUCCESS-PATH Document Cache write —
the authored ``cache_put`` flow-sequence step, which lowers to the byte-locked
Add to Cache (``doccacheload``) emitter. This is distinct from the existing
DLQ/error-path cache load (``reliability.dlq.mode='document_cache_ref'``):
``cache_put`` writes the CURRENT documents on the main row so a later branch
leg / step can read them back (``cache_get``) or join them at map time
(``document_cache_joins``).

It emits NO standalone components (``emit_components`` -> ``[]``); the step
lives inline in the process ``flow_sequence``, so the primitive contributes a
``process_config.flow_sequence`` fragment plus the ``depends_on`` key its
``document_cache_id`` ``$ref`` references.
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional

from pydantic import BaseModel, ConfigDict, Field, field_validator

from ...models.integration_models import IntegrationComponentSpec
from ..base import (
    PatternIOContract,
    PatternKind,
    PatternMetadata,
    PrimitiveBuildContext,
    PrimitivePattern,
)


class DocumentCachePutParameters(BaseModel):
    """Parameters for the success-path cache write (``cache_put``) step."""

    model_config = ConfigDict(extra="forbid")

    document_cache_id: str = Field(
        ...,
        min_length=1,
        description="Document Cache component id to write to (literal id or $ref:KEY).",
    )
    label: Optional[str] = Field(default=None, description="Descriptive shape label")

    @field_validator("document_cache_id")
    @classmethod
    def _non_blank_cache_id(cls, value: str) -> str:
        if not value.strip():
            raise ValueError("document_cache_id must be a non-empty string")
        return value


class DocumentCachePutPrimitive(PrimitivePattern):
    """Declare a success-path Document Cache write (cache_put) fragment."""

    metadata = PatternMetadata(
        name="document_cache_put",
        version="1.0.0",
        kind=PatternKind.PRIMITIVE,
        description=(
            "Declare a success-path Document Cache write as an authored "
            "cache_put flow-sequence step (lowers to the Add to Cache shape): "
            "store the current documents so a later step, branch leg, or map "
            "join can read them back within the same execution."
        ),
        tags=["cache", "document-cache", "put", "handoff"],
        use_cases=[
            "Stage documents for a later branch leg to consume (cross-branch handoff)",
            "Build a keyed lookup set that a map-level DocumentCacheJoins entry joins",
        ],
        not_for=[
            "Error/DLQ capture (use reliability.dlq.mode='document_cache_ref')",
            "Durable cross-run state (caches are execution-scoped)",
        ],
    )
    parameters_model = DocumentCachePutParameters

    input_contract = PatternIOContract(
        name="document_stream",
        description="Inbound documents (Add to Cache consumes them into the cache).",
    )
    output_contract = PatternIOContract(
        name="cached_document_stream",
        description="The cache write is a terminal-ish sink on its row; documents live in the cache.",
    )
    required_builders = ["ProcessFlowBuilder"]

    @classmethod
    def emit_components(
        cls,
        context: PrimitiveBuildContext,
        parameters: BaseModel,
    ) -> List[IntegrationComponentSpec]:
        # Fragment-only: the cache_put step lives inline in the flow_sequence.
        return []

    @classmethod
    def emit_fragment(
        cls,
        context: PrimitiveBuildContext,
        parameters: BaseModel,
    ) -> Dict[str, Any]:
        params: DocumentCachePutParameters = parameters  # type: ignore[assignment]
        step: Dict[str, Any] = {
            "kind": "cache_put",
            "document_cache_id": params.document_cache_id,
        }
        if params.label:
            step["label"] = params.label
        depends_on: List[str] = []
        if params.document_cache_id.startswith("$ref:"):
            key = params.document_cache_id[len("$ref:"):]
            if key:
                depends_on.append(key)
        return {
            "process_config": {"flow_sequence": [step]},
            "depends_on": depends_on,
        }
