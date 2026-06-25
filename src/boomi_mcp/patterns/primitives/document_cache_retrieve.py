"""Issue #109 (M10.5): ``document_cache_retrieve`` transform primitive.

A fragment-only primitive that declares a process-level Document Cache Retrieve
shape — the READ half of Document Cache CRUD — as a
``transform.mode='doccacheretrieve'`` process fragment consumed by
``ProcessFlowBuilder``. It pulls documents from a Document Cache into the current
flow, pairing the already-shipped Add to Cache (``doccacheload``) shape.
Live-captured from the ``work`` account (component
64e5397b-3583-42c9-8fe3-08ccefb0da6c shape2; see
``.codex/plans/issue-109-live-captures.md``).

v1 emits only the live-observed all-document retrieve (``load_all_documents``
true, empty cache-key set) with the recommended ``stopprocess`` empty-cache
behavior; keyed/index retrieval and the backward-compat "fail document with
errors" behavior are deferred until each has its own byte-accurate live capture
(the builder rejects them with ``PROCESS_DOCCACHE_RETRIEVE_CONFIG_INVALID``).

It emits NO standalone components (``emit_components`` -> ``[]``); the retrieve
step lives inline on the process shape, so the primitive only contributes a
``process_config`` fragment plus the ``depends_on`` keys its ``document_cache_id``
``$ref`` references (so the merged process passes ``MISSING_PROCESS_DEPENDENCY``).
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


# ---------------------------------------------------------------------------
# Parameter model (strict)
# ---------------------------------------------------------------------------


class DocumentCacheRetrieveParameters(BaseModel):
    """Parameters for the Document Cache Retrieve shape.

    ``document_cache_id`` is the Document Cache component id (a literal id or a
    ``$ref:KEY`` token). ``empty_cache_behavior`` accepts only the live-verified
    ``stopprocess`` value and ``load_all_documents`` only ``True`` — the exact
    forms the builder emits and the only ones with a byte-accurate live capture
    (keyed/index retrieval is deferred). The ``Literal`` field types reject any
    other value at validation time, matching the builder's
    ``PROCESS_DOCCACHE_RETRIEVE_CONFIG_INVALID`` rejections.
    """

    model_config = ConfigDict(extra="forbid")

    document_cache_id: str = Field(
        ...,
        min_length=1,
        description="Document Cache component id to retrieve from (literal id or $ref:KEY).",
    )
    empty_cache_behavior: Literal["stopprocess"] = Field(
        default="stopprocess",
        description="If-cache-is-empty behavior (only 'stopprocess' — Stop document execution).",
    )
    load_all_documents: Literal[True] = Field(
        default=True,
        description="Retrieve ALL cached documents (v1 supports only the all-document form).",
    )
    label: Optional[str] = Field(default=None, description="Descriptive shape label")

    @field_validator("document_cache_id")
    @classmethod
    def _non_blank_cache_id(cls, value: str) -> str:
        if not value.strip():
            raise ValueError("document_cache_id must be a non-empty string")
        return value


# ---------------------------------------------------------------------------
# Primitive
# ---------------------------------------------------------------------------


class DocumentCacheRetrievePrimitive(PrimitivePattern):
    """Declare a process-level Document Cache Retrieve transform fragment."""

    metadata = PatternMetadata(
        name="document_cache_retrieve",
        version="1.0.0",
        kind=PatternKind.PRIMITIVE,
        description=(
            "Declare a Document Cache Retrieve shape (the read half of Document "
            "Cache CRUD) as a transform fragment: pull documents from a Document "
            "Cache into the current flow. v1 retrieves all cached documents; "
            "keyed/index retrieval is deferred."
        ),
        tags=["transform", "document-cache", "retrieve"],
        use_cases=[
            "Read previously cached documents back into a process or subprocess",
            "Re-emit an aggregated/combined cached set for downstream processing",
        ],
        not_for=[
            "Populating a cache (use the Add to Cache step / DLQ cache route)",
            "Keyed/index retrieval by cache key (deferred pending a live capture)",
            "Durable cross-run state (caches are execution-scoped)",
        ],
    )
    parameters_model = DocumentCacheRetrieveParameters

    input_contract = PatternIOContract(
        name="document_stream",
        description="Inbound documents (the retrieve step replaces them with the cached set).",
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
        # Fragment-only: the retrieve step lives inline on the process shape, so
        # this primitive materializes no standalone components.
        return []

    @classmethod
    def emit_fragment(
        cls,
        context: PrimitiveBuildContext,
        parameters: BaseModel,
    ) -> Dict[str, Any]:
        params: DocumentCacheRetrieveParameters = parameters  # type: ignore[assignment]
        transform: Dict[str, Any] = {
            "mode": "doccacheretrieve",
            "document_cache_id": params.document_cache_id,
            "empty_cache_behavior": params.empty_cache_behavior,
            "load_all_documents": params.load_all_documents,
        }
        if params.label:
            transform["label"] = params.label
        # A document_cache_id may be a literal component id OR a "$ref:KEY" token.
        # Per the emit_fragment contract (see base.PrimitivePattern), depends_on
        # must list every component key the process_config references — otherwise
        # the merged process component would fail ProcessFlowBuilder.validate_config
        # with MISSING_PROCESS_DEPENDENCY on the unreachable ref (mirrors
        # BranchPrimitive's $ref collection).
        depends_on: List[str] = []
        if params.document_cache_id.startswith("$ref:"):
            key = params.document_cache_id[len("$ref:"):]
            if key:
                depends_on.append(key)
        return {"process_config": {"transform": transform}, "depends_on": depends_on}
