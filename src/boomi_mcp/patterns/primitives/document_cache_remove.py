"""Issue #110 (M10.6): ``document_cache_remove`` transform primitive.

A fragment-only primitive that declares a process-level Document Cache Remove
shape — the DELETE half of Document Cache CRUD — as a
``transform.mode='doccacheremove'`` process fragment consumed by
``ProcessFlowBuilder``. It clears documents from a Document Cache, completing the
set alongside the already-shipped Add to Cache (``doccacheload``, write) and
Document Cache Retrieve (``doccacheretrieve``, read, #109).
Live-captured from the ``work`` account (component
6e56df6a-1fc0-43f6-8db2-1b9e4eefa7a0 "[Intapp CDS] Initialize Caches" shapes 3-7;
see ``.codex/plans/issue-110-live-captures.md``).

v1 emits only the live-observed all-document remove (``remove_all_documents``
true, empty cache-key set); keyed/index removal is deferred until it has its own
byte-accurate live capture (the builder rejects it with
``PROCESS_DOCCACHE_REMOVE_CONFIG_INVALID``). Unlike retrieve, the remove shape
carries no empty-cache behavior / load-all attributes.

It emits NO standalone components (``emit_components`` -> ``[]``); the remove
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


class DocumentCacheRemoveParameters(BaseModel):
    """Parameters for the Document Cache Remove shape.

    ``document_cache_id`` is the Document Cache component id (a literal id or a
    ``$ref:KEY`` token). ``remove_all_documents`` accepts only ``True`` — the
    exact form the builder emits and the only one with a byte-accurate live
    capture (keyed/index removal is deferred). The ``Literal`` field type rejects
    any other value at validation time, matching the builder's
    ``PROCESS_DOCCACHE_REMOVE_CONFIG_INVALID`` rejections. There is no
    ``empty_cache_behavior`` / ``load_all_documents`` (those are retrieve-only).
    """

    model_config = ConfigDict(extra="forbid")

    document_cache_id: str = Field(
        ...,
        min_length=1,
        description="Document Cache component id to remove from (literal id or $ref:KEY).",
    )
    remove_all_documents: Literal[True] = Field(
        default=True,
        description="Remove ALL cached documents (v1 supports only the all-document form).",
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


class DocumentCacheRemovePrimitive(PrimitivePattern):
    """Declare a process-level Document Cache Remove transform fragment."""

    metadata = PatternMetadata(
        name="document_cache_remove",
        version="1.0.0",
        kind=PatternKind.PRIMITIVE,
        description=(
            "Declare a Document Cache Remove shape (the delete half of Document "
            "Cache CRUD) as a transform fragment: clear documents from a Document "
            "Cache. v1 removes all cached documents; keyed/index removal is "
            "deferred."
        ),
        tags=["transform", "document-cache", "remove"],
        use_cases=[
            "Clear a Document Cache between runs or branch legs",
            "Reset cached reference data before re-populating it",
        ],
        not_for=[
            "Populating a cache (use the Add to Cache step / DLQ cache route)",
            "Reading a cache (use the Document Cache Retrieve step / #109)",
            "Keyed/index removal by cache key (deferred pending a live capture)",
        ],
    )
    parameters_model = DocumentCacheRemoveParameters

    input_contract = PatternIOContract(
        name="document_stream",
        description="Inbound documents (the remove step clears the cache, documents pass through).",
    )
    output_contract = PatternIOContract(
        name="document_stream",
        description="Documents continue downstream after the cache is cleared.",
    )
    required_builders = ["ProcessFlowBuilder"]

    @classmethod
    def emit_components(
        cls,
        context: PrimitiveBuildContext,
        parameters: BaseModel,
    ) -> List[IntegrationComponentSpec]:
        # Fragment-only: the remove step lives inline on the process shape, so
        # this primitive materializes no standalone components.
        return []

    @classmethod
    def emit_fragment(
        cls,
        context: PrimitiveBuildContext,
        parameters: BaseModel,
    ) -> Dict[str, Any]:
        params: DocumentCacheRemoveParameters = parameters  # type: ignore[assignment]
        transform: Dict[str, Any] = {
            "mode": "doccacheremove",
            "document_cache_id": params.document_cache_id,
            "remove_all_documents": params.remove_all_documents,
        }
        if params.label:
            transform["label"] = params.label
        # A document_cache_id may be a literal component id OR a "$ref:KEY" token.
        # Per the emit_fragment contract (see base.PrimitivePattern), depends_on
        # must list every component key the process_config references — otherwise
        # the merged process component would fail ProcessFlowBuilder.validate_config
        # with MISSING_PROCESS_DEPENDENCY on the unreachable ref (mirrors
        # DocumentCacheRetrievePrimitive's $ref collection).
        depends_on: List[str] = []
        if params.document_cache_id.startswith("$ref:"):
            key = params.document_cache_id[len("$ref:"):]
            if key:
                depends_on.append(key)
        return {"process_config": {"transform": transform}, "depends_on": depends_on}
