"""Issue #122 (M11.3, epic #118): ``document_cache_lookup`` primitive.

A fragment-only primitive that declares an authored ``cache_get`` retrieve —
read previously cached documents back into the flow (the consume half of the
``cache_put`` handoff). v1 emits the all-document form only (the byte-locked
M10 retrieve); keyed/index lookup and the Set Properties cache parameter
source are gated pending a live-captured wire shape (#119 census Outcome B) —
the strict parameter model rejects them by construction.

For map-time keyed joins use the map-level ``document_cache_joins`` config
instead (live-captured shape, #122).
"""

from __future__ import annotations

from typing import Any, Dict, List, Literal, Optional

from pydantic import BaseModel, ConfigDict, Field, field_validator

from ...models.integration_models import IntegrationComponentSpec
from ..base import (
    PatternIOContract,
    PatternKind,
    PatternMetadata,
    PrimitiveBuildContext,
    PrimitivePattern,
)


class DocumentCacheLookupParameters(BaseModel):
    """Parameters for the authored ``cache_get`` retrieve step."""

    model_config = ConfigDict(extra="forbid")

    document_cache_id: str = Field(
        ...,
        min_length=1,
        description="Document Cache component id to read from (literal id or $ref:KEY).",
    )
    empty_cache_behavior: Literal["stopprocess"] = Field(
        default="stopprocess",
        description="If-cache-is-empty behavior (only 'stopprocess' — Stop document execution).",
    )
    label: Optional[str] = Field(default=None, description="Descriptive shape label")

    @field_validator("document_cache_id")
    @classmethod
    def _non_blank_cache_id(cls, value: str) -> str:
        if not value.strip():
            raise ValueError("document_cache_id must be a non-empty string")
        return value


class DocumentCacheLookupPrimitive(PrimitivePattern):
    """Declare an authored cache_get retrieve (the consume half of cache_put)."""

    metadata = PatternMetadata(
        name="document_cache_lookup",
        version="1.0.0",
        kind=PatternKind.PRIMITIVE,
        description=(
            "Declare an authored cache_get flow-sequence step: read cached "
            "documents back into the flow (all-document form). Pairs with "
            "document_cache_put for same-execution cross-branch handoff."
        ),
        tags=["cache", "document-cache", "get", "lookup", "handoff"],
        use_cases=[
            "Consume documents a previous branch leg staged via cache_put",
            "Re-emit an accumulated cached set for downstream processing",
        ],
        not_for=[
            "Keyed/index retrieval (gated pending a live keyed capture, #119)",
            "Map-time keyed joins (use the map-level document_cache_joins config)",
            "Durable cross-run state (caches are execution-scoped)",
        ],
    )
    parameters_model = DocumentCacheLookupParameters

    input_contract = PatternIOContract(
        name="document_stream",
        description="Inbound documents (the retrieve replaces them with the cached set).",
    )
    output_contract = PatternIOContract(
        name="cached_document_stream",
        description="Documents retrieved from the Document Cache into the current flow.",
    )
    required_builders = ["ProcessFlowBuilder"]

    @classmethod
    def emit_components(
        cls,
        context: PrimitiveBuildContext,
        parameters: BaseModel,
    ) -> List[IntegrationComponentSpec]:
        # Fragment-only: the cache_get step lives inline in the flow_sequence.
        return []

    @classmethod
    def emit_fragment(
        cls,
        context: PrimitiveBuildContext,
        parameters: BaseModel,
    ) -> Dict[str, Any]:
        params: DocumentCacheLookupParameters = parameters  # type: ignore[assignment]
        step: Dict[str, Any] = {
            "kind": "cache_get",
            "document_cache_id": params.document_cache_id,
            "empty_cache_behavior": params.empty_cache_behavior,
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
