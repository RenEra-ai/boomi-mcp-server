"""Issue #107 (M10.3): ``return_documents`` terminal primitive.

A fragment-only primitive that declares a process-level Return Documents shape —
the terminal that returns the current documents to the calling source point (the
parent process via a Process Call/Route, or a web-service client). It is used to
give a subprocess a return value. Unlike the mid-flow ``data_process`` transform,
Return Documents is a TERMINAL: when enabled it replaces the trailing Stop, and
no Stop follows it (the verifier's ``RETURN_DOCS_STOP_EXCLUSIVE`` invariant).

It emits NO standalone components (``emit_components`` -> ``[]``); the terminal is
realized inline on the process flow, so the primitive only contributes a
``process_config`` fragment (keyed ``return_documents``) plus an empty
``depends_on``. ``ProcessFlowBuilder`` / ``WrapperSubprocessBuilder`` read that
block and emit the terminal shape (see ``process_flow_builder._terminal_flow_entry``
/ ``_emit_returndocuments``).
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional

from pydantic import BaseModel, ConfigDict, Field

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


class ReturnDocumentsParameters(BaseModel):
    """Parameters for the Return Documents terminal primitive.

    The single optional ``label`` is the Boomi "custom label" that identifies the
    returned document type(s) — important when the subprocess is invoked through a
    Process Call or Process Route step (return-path mapping). It is optional (the
    live capture leaves it empty). ``enabled`` is not a parameter: declaring this
    primitive IS the request for a Return Documents terminal, so the fragment
    always emits ``enabled=True``.
    """

    model_config = ConfigDict(extra="forbid")

    label: Optional[str] = Field(
        default=None,
        description="Optional Return Documents custom label (identifies the returned document type(s))",
    )


# ---------------------------------------------------------------------------
# Primitive
# ---------------------------------------------------------------------------


class ReturnDocumentsPrimitive(PrimitivePattern):
    """Declare a process-level Return Documents terminal as a process fragment."""

    metadata = PatternMetadata(
        name="return_documents",
        version="1.0.0",
        kind=PatternKind.PRIMITIVE,
        description=(
            "Declare a process-level Return Documents terminal shape that returns "
            "the current documents to the calling source point (subprocess return "
            "value). Replaces the trailing Stop; nothing follows it."
        ),
        tags=["terminal", "subprocess", "return-documents"],
        use_cases=[
            "Give a subprocess a return value (documents returned to the parent)",
            "Return documents to a Process Call / Process Route caller",
        ],
        not_for=[
            "Ending a top-level process that has no caller (use the default Stop)",
            "Mid-flow document manipulation (use data_process / a map)",
        ],
    )
    parameters_model = ReturnDocumentsParameters

    input_contract = PatternIOContract(
        name="document_stream",
        description="Inbound documents reaching the end of the document path.",
    )
    output_contract = PatternIOContract(
        name="returned_document_stream",
        description="Documents returned to the calling source point (parent process / web-service client).",
    )
    required_builders = ["ProcessFlowBuilder"]

    @classmethod
    def emit_components(
        cls,
        context: PrimitiveBuildContext,
        parameters: BaseModel,
    ) -> List[IntegrationComponentSpec]:
        # Fragment-only: the Return Documents terminal is realized inline on the
        # process flow, so this primitive materializes no standalone components.
        return []

    @classmethod
    def emit_fragment(
        cls,
        context: PrimitiveBuildContext,
        parameters: BaseModel,
    ) -> Dict[str, Any]:
        params: ReturnDocumentsParameters = parameters  # type: ignore[assignment]
        return_documents: Dict[str, Any] = {"enabled": True}
        if params.label:
            return_documents["label"] = params.label
        return {"process_config": {"return_documents": return_documents}, "depends_on": []}
