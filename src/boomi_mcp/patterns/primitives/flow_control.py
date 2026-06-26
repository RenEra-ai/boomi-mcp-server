"""Issue #111 (M10.7): ``flow_control_batching`` per-document batching primitive.

A fragment-only primitive that declares a Flow Control shape configured for the
live-verified per-document batching mode — the document stream is processed in
batches of ``for_each_count`` documents through the rest of the flow. Byte-exact to
a live ``work``-account capture (component 7ce0d74d-e71a-408b-9d59-a6f4498c64e2;
see ``.codex/plans/issue-111.md``). v1 supports ONLY this batching mode; true
parallel chunks, multiProcess, and the combine variant stay design guidance.

It emits NO standalone components (``emit_components`` -> ``[]``); the Flow Control
shape is realized inline on the process flow, so the primitive only contributes a
``process_config`` fragment (keyed ``flow_control``) plus an empty ``depends_on``
(it references no component ids). ``ProcessFlowBuilder`` reads that block and emits
the shape right after the source (see ``process_flow_builder._emit_flowcontrol`` /
``_flow_control_enabled``). It does not compose with a Branch fan-out or a Decision
route in v1.
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


class FlowControlParameters(BaseModel):
    """Parameters for the Flow Control (per-document batching) shape.

    ``for_each_count`` is the batch size (documents per batch) — a positive
    integer, mirroring the builder's ``flow_control.for_each_count`` validation.
    ``label`` is an optional shape userlabel. ``enabled`` is not a parameter:
    declaring this primitive IS the request for a Flow Control batching shape, so
    the fragment always emits ``enabled=True``.
    """

    model_config = ConfigDict(extra="forbid")

    for_each_count: int = Field(
        gt=0,
        description="Batch size — documents processed per batch (positive integer).",
    )
    label: Optional[str] = Field(
        default=None,
        description="Optional Flow Control shape label.",
    )


# ---------------------------------------------------------------------------
# Primitive
# ---------------------------------------------------------------------------


class FlowControlPrimitive(PrimitivePattern):
    """Declare a Flow Control (per-document batching) shape as a process fragment."""

    metadata = PatternMetadata(
        name="flow_control_batching",
        version="1.0.0",
        kind=PatternKind.PRIMITIVE,
        description=(
            "Declare a Flow Control shape configured for per-document batching: the "
            "document stream is processed in batches of for_each_count documents "
            "through the rest of the flow. Batching mode only — true parallel "
            "chunking and the combine variant are not emitted in v1."
        ),
        tags=["flow-control", "batching", "control"],
        use_cases=[
            "Process a large document stream in fixed-size batches",
            "Bound per-batch memory/throughput on a high-volume sync",
        ],
        not_for=[
            "True concurrent/parallel execution (this is sequential per-document batching)",
            "Combining or splitting document content (use data_process split/combine)",
        ],
    )
    parameters_model = FlowControlParameters

    input_contract = PatternIOContract(
        name="document_stream",
        description="Inbound documents to batch.",
    )
    output_contract = PatternIOContract(
        name="batched_document_stream",
        description="Documents released downstream in batches of for_each_count.",
    )
    required_builders = ["ProcessFlowBuilder"]

    @classmethod
    def emit_components(
        cls,
        context: PrimitiveBuildContext,
        parameters: BaseModel,
    ) -> List[IntegrationComponentSpec]:
        # Fragment-only: the Flow Control shape is realized inline on the process
        # flow, so this primitive materializes no standalone components.
        return []

    @classmethod
    def emit_fragment(
        cls,
        context: PrimitiveBuildContext,
        parameters: BaseModel,
    ) -> Dict[str, Any]:
        params: FlowControlParameters = parameters  # type: ignore[assignment]
        flow_control: Dict[str, Any] = {
            "enabled": True,
            "for_each_count": params.for_each_count,
        }
        if params.label:
            flow_control["label"] = params.label
        return {"process_config": {"flow_control": flow_control}, "depends_on": []}
