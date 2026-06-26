"""Issue #113 (M10.9): ``decision_route`` conditional two-path routing primitive.

A fragment-only primitive that declares a Decision shape — Boomi's if/then — which
routes the document down a labelled ``true`` or ``false`` path based on a value
comparison (binary, both outcomes explicit). The TRUE path is the process's
top-level ``target``; the FALSE path optionally runs a ``false_notify`` Message
before its own Stop (so it stays ``CONTROL_BRANCH_BARE_STOP``-clean). Live-captured
from the ``work`` account (see boomi_companion .../references/steps/decision_step.md
and ``.codex/plans/issue-113-live-captures.md``).

It emits NO standalone components (``emit_components`` -> ``[]``); the Decision shape
and its legs are realized inline on the process flow, so the primitive only
contributes a ``process_config`` fragment (keyed ``decision``) plus an empty
``depends_on`` (v1 operands are a DDP/DPP ``track`` value or a ``static`` literal —
neither is a component ref). ``ProcessFlowBuilder`` reads that block and emits the
shape (see ``process_flow_builder._emit_decision`` / ``_emit_decision_shapes``). The
backward ``false_next`` loop edge is a builder-config feature (it names an earlier
flow shape, which a standalone primitive cannot resolve) and is not exposed here in
v1; nor does the Decision compose with a Branch fan-out / Try-Catch / Return
Documents yet.
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from ...models.integration_models import IntegrationComponentSpec
from ..base import (
    PatternIOContract,
    PatternKind,
    PatternMetadata,
    PrimitiveBuildContext,
    PrimitivePattern,
)

# Kept in sync with ``process_flow_builder._DECISION_COMPARISONS`` /
# ``_DECISION_VALUE_TYPES`` (the builder is the authority that emits the XML; this
# primitive enforces the same enums so a validated fragment is never rejected by
# the builder it feeds).
_DECISION_COMPARISONS = (
    "equals",
    "greaterthaneq",
    "lessthaneq",
    "greaterthan",
    "lessthan",
    "regex",
    "wildcard",
)
_DECISION_VALUE_TYPES = ("track", "static")


# ---------------------------------------------------------------------------
# Parameter models (strict)
# ---------------------------------------------------------------------------


class DecisionOperandParameters(BaseModel):
    """One Decision operand (``left`` / ``right``) — a ``track`` or ``static`` value.

    ``track`` (a DDP/DPP) requires ``property_id`` (e.g. ``dynamicdocument.DDP_X``);
    ``static`` (a literal) requires ``static_value`` (which MAY be the empty string —
    the live "is empty" check compares a track value against an empty static).
    """

    model_config = ConfigDict(extra="forbid")

    value_type: str = Field(description="Operand source: 'track' (a DDP/DPP) or 'static' (a literal).")
    property_id: Optional[str] = Field(
        default=None, description="Tracked property id for a track operand (e.g. 'dynamicdocument.DDP_X')."
    )
    default_value: Optional[str] = Field(
        default=None, description="Optional default value for a track operand."
    )
    property_name: Optional[str] = Field(
        default=None, description="Optional display name for a track operand."
    )
    static_value: Optional[str] = Field(
        default=None, description="Literal value for a static operand (may be empty)."
    )

    @field_validator("value_type")
    @classmethod
    def _valid_value_type(cls, value: str) -> str:
        if value not in _DECISION_VALUE_TYPES:
            raise ValueError(f"value_type must be one of {list(_DECISION_VALUE_TYPES)}.")
        return value

    @model_validator(mode="after")
    def _require_source_fields(self) -> "DecisionOperandParameters":
        if self.value_type == "track":
            if not (self.property_id and self.property_id.strip()):
                raise ValueError("a track operand requires a non-blank property_id.")
        elif self.value_type == "static":
            if not isinstance(self.static_value, str):
                raise ValueError("a static operand requires a string static_value (may be empty).")
        return self


class DecisionParameters(BaseModel):
    """Parameters for the Decision (conditional two-path routing) shape.

    ``comparison`` is one of the 7 live Boomi Decision operators; ``left``/``right``
    are the two operands. ``false_notify`` is an optional Message text shown on the
    false path before its Stop (keeping the rejected path traceable).
    """

    model_config = ConfigDict(extra="forbid")

    comparison: str = Field(description="Decision operator (equals / greaterthaneq / ... / wildcard).")
    left: DecisionOperandParameters = Field(description="Left operand (the value being tested).")
    right: DecisionOperandParameters = Field(description="Right operand (the value compared against).")
    label: Optional[str] = Field(
        default=None, description="Optional display name (shape userlabel + decision name)."
    )
    false_notify: Optional[str] = Field(
        default=None, description="Optional Message text on the false path before its Stop."
    )

    @field_validator("comparison")
    @classmethod
    def _valid_comparison(cls, value: str) -> str:
        if value not in _DECISION_COMPARISONS:
            raise ValueError(f"comparison must be one of {list(_DECISION_COMPARISONS)}.")
        return value

    @field_validator("false_notify")
    @classmethod
    def _nonblank_false_notify(cls, value: Optional[str]) -> Optional[str]:
        # Mirror the builder: ``decision.false_notify`` must be non-empty after
        # stripping, so a validated primitive never emits a builder-rejected
        # fragment (the BranchPrimitive._nonblank precedent).
        if value is not None and not value.strip():
            raise ValueError("false_notify, when provided, must be a non-empty string.")
        return value


# ---------------------------------------------------------------------------
# Primitive
# ---------------------------------------------------------------------------


class DecisionPrimitive(PrimitivePattern):
    """Declare a Decision (conditional two-path routing) shape as a process fragment."""

    metadata = PatternMetadata(
        name="decision_route",
        version="1.0.0",
        kind=PatternKind.PRIMITIVE,
        description=(
            "Declare a Decision if/then: route the document down a true or false "
            "path based on a value comparison (binary, both outcomes explicit). The "
            "true path is the process's top-level target; the false path optionally "
            "runs a notify Message before its Stop. Value-comparing two-path "
            "selection — use a Branch for unconditional fan-out."
        ),
        tags=["routing", "conditional", "decision"],
        use_cases=[
            "Route on a status/flag (e.g. send active records, notify on the rest)",
            "Implement if/then logic with an explicit false path",
        ],
        not_for=[
            "Unconditional N-way fan-out of the same document (use a Branch)",
            "Per-record evaluation of a multi-document batch (a Decision inspects only the first record — use Business Rules)",
        ],
    )
    parameters_model = DecisionParameters

    input_contract = PatternIOContract(
        name="document_stream",
        description="Documents to route on the decision comparison.",
    )
    output_contract = PatternIOContract(
        name="decision_route",
        description="The document delivered down the true path (target) or the false path (notify/stop).",
    )
    required_builders = ["ProcessFlowBuilder"]

    @classmethod
    def emit_components(
        cls,
        context: PrimitiveBuildContext,
        parameters: BaseModel,
    ) -> List[IntegrationComponentSpec]:
        # Fragment-only: the Decision shape and its legs are realized inline on the
        # process flow, so this primitive materializes no standalone components.
        return []

    @classmethod
    def emit_fragment(
        cls,
        context: PrimitiveBuildContext,
        parameters: BaseModel,
    ) -> Dict[str, Any]:
        params: DecisionParameters = parameters  # type: ignore[assignment]
        decision: Dict[str, Any] = {
            "enabled": True,
            "comparison": params.comparison,
            "left": cls._operand_dict(params.left),
            "right": cls._operand_dict(params.right),
        }
        if params.label:
            decision["label"] = params.label
        if params.false_notify:
            decision["false_notify"] = params.false_notify
        # v1 operands are a DDP/DPP track value or a static literal — neither is a
        # component ref — so the fragment declares no dependencies.
        return {"process_config": {"decision": decision}, "depends_on": []}

    @staticmethod
    def _operand_dict(operand: DecisionOperandParameters) -> Dict[str, Any]:
        """Emit only the operand fields the builder reads, dropping unset optionals."""
        result: Dict[str, Any] = {"value_type": operand.value_type}
        if operand.value_type == "track":
            result["property_id"] = operand.property_id
            if operand.default_value is not None:
                result["default_value"] = operand.default_value
            if operand.property_name is not None:
                result["property_name"] = operand.property_name
        else:  # static
            result["static_value"] = operand.static_value
        return result
