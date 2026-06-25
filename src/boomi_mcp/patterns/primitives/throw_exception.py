"""Issue #108 (M10.4): ``throw_exception`` catch-leg terminal primitive.

A fragment-only primitive that declares a deliberate Exception (Throw) terminal on
the Try/Catch catch leg — the leg ends in a user-defined error reported on the
Process Reporting page (a fail/halt) instead of a bare Stop. The Boomi docs are
explicit: a Stop is a *successful* conclusion; an error path uses an Exception
instead. Live-captured from the ``work`` account (component
1139079f-fff5-434c-aedc-d2758cc20525 shape10; see
``.codex/plans/issue-108-live-captures.md``).

It emits NO standalone components (``emit_components`` -> ``[]``); the Exception
terminal is realized inline on the process flow's catch leg, so the primitive only
contributes a ``process_config`` fragment (keyed ``reliability.catch_exception``)
plus an empty ``depends_on``. ``ProcessFlowBuilder`` reads that block and emits the
catch-leg Exception (see ``process_flow_builder._emit_exception`` /
``_emit_catch_leg``). It needs no DLQ — a bare ``catcherrors -> exception`` is the
live "fail/halt" shape — and composes with a Notify and/or a DLQ route.
"""

from __future__ import annotations

from typing import Any, Dict, List, Literal, Optional

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

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


class ThrowExceptionParameters(BaseModel):
    """Parameters for the deliberate Exception (Throw) catch-leg terminal.

    ``message_template`` is the error text; use the ``{1}`` placeholder for the
    value bound by ``parameter_source`` (``caught_error`` = the platform Try/Catch
    error message; ``current_document`` = the current document; ``none`` = a
    static message with no parameter). ``stop_single_document`` true fails only the
    document that reached the Exception (others continue); false (default) halts the
    whole process. ``title`` (optional) is the alert subject / process-log title.
    """

    model_config = ConfigDict(extra="forbid")

    message_template: str = Field(
        description="Exception error message; use {1} for the parameter_source-bound value.",
    )
    title: Optional[str] = Field(
        default=None,
        description="Optional Exception title (alert subject / process-log title).",
    )
    stop_single_document: bool = Field(
        default=False,
        description="true fails only the reaching document; false (default) halts the whole process.",
    )
    parameter_source: Literal["caught_error", "current_document", "none"] = Field(
        default="caught_error",
        description="Binds {1}: 'caught_error', 'current_document', or 'none'.",
    )

    # Mirror ProcessFlowBuilder._validate_catch_exception so a primitive that
    # validates successfully never emits a fragment the builder rejects (a
    # non-blank message, and the {1} placeholder whenever parameter_source binds a
    # value). The Literal above already constrains parameter_source.
    @field_validator("message_template")
    @classmethod
    def _message_template_nonblank(cls, value: str) -> str:
        if not value.strip():
            raise ValueError("message_template must be a non-empty string.")
        return value

    @model_validator(mode="after")
    def _require_placeholder_when_bound(self) -> "ThrowExceptionParameters":
        if self.parameter_source != "none" and "{1}" not in self.message_template:
            raise ValueError(
                "message_template must contain the {1} placeholder when "
                "parameter_source binds a value (caught_error / current_document); "
                "use parameter_source='none' for a static message."
            )
        return self


# ---------------------------------------------------------------------------
# Primitive
# ---------------------------------------------------------------------------


class ThrowExceptionPrimitive(PrimitivePattern):
    """Declare a deliberate Exception (Throw) catch-leg terminal as a fragment."""

    metadata = PatternMetadata(
        name="throw_exception",
        version="1.0.0",
        kind=PatternKind.PRIMITIVE,
        description=(
            "Declare a deliberate Exception (Throw) terminal on the Try/Catch catch "
            "leg — the leg fails/halts with a user-defined error reported on the "
            "Process Reporting page, instead of a bare Stop. Needs no DLQ; composes "
            "with a catch Notify and/or a DLQ route."
        ),
        tags=["terminal", "error-handling", "exception", "throw"],
        use_cases=[
            "Deliberately fail/halt a path with a custom error message",
            "Re-throw a caught Try/Catch error to surface it on the reporting page",
        ],
        not_for=[
            "Ending a successful path (use the default Stop)",
            "Queuing failed documents for replay (use a DLQ dead-letter route)",
        ],
    )
    parameters_model = ThrowExceptionParameters

    input_contract = PatternIOContract(
        name="caught_document_stream",
        description="Documents reaching the catch leg after a caught error.",
    )
    output_contract = PatternIOContract(
        name="thrown_exception",
        description="A deliberate Exception that fails/halts the path with a user-defined error.",
    )
    required_builders = ["ProcessFlowBuilder"]

    @classmethod
    def emit_components(
        cls,
        context: PrimitiveBuildContext,
        parameters: BaseModel,
    ) -> List[IntegrationComponentSpec]:
        # Fragment-only: the Exception terminal is realized inline on the process
        # flow's catch leg, so this primitive materializes no standalone components.
        return []

    @classmethod
    def emit_fragment(
        cls,
        context: PrimitiveBuildContext,
        parameters: BaseModel,
    ) -> Dict[str, Any]:
        params: ThrowExceptionParameters = parameters  # type: ignore[assignment]
        catch_exception: Dict[str, Any] = {
            "message_template": params.message_template,
            "stop_single_document": params.stop_single_document,
            "parameter_source": params.parameter_source,
        }
        if params.title:
            catch_exception["title"] = params.title
        return {
            "process_config": {"reliability": {"catch_exception": catch_exception}},
            "depends_on": [],
        }
