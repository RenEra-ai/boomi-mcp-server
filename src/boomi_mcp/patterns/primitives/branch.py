"""Issue #112 (M10.8): ``branch_fanout`` N-way forward fan-out primitive.

A fragment-only primitive that declares a Branch shape — an unconditional N-way
fan-out where the same document runs down several independent paths in sequence,
each path executed to completion before the next begins (no rejoin). Leg 1 is the
process's top-level ``target``; this primitive's ``targets`` supply legs 2..N.
Live-captured from the ``work`` account (component
b34d3812-900d-41b6-b44c-c812fb9b04aa shape53; see
``.codex/plans/issue-112-live-captures.md``).

It emits NO standalone components (``emit_components`` -> ``[]``); the Branch shape
and its legs are realized inline on the process flow, so the primitive only
contributes a ``process_config`` fragment (keyed ``branch``) plus an empty
``depends_on``. ``ProcessFlowBuilder`` reads that block and emits the fan-out (see
``process_flow_builder._emit_branch`` / ``_emit_branch_shapes``). v1 legs are plain
REST targets (no per-leg dynamic path / retry); the fan-out does not compose with a
Try/Catch wrapper or a Return Documents terminal yet.
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


# ---------------------------------------------------------------------------
# Parameter models (strict)
# ---------------------------------------------------------------------------


class BranchTargetParameters(BaseModel):
    """One additional Branch leg (legs 2..N) — a REST connector target binding.

    Mirrors the process-config ``target`` binding the builder validates: a REST
    ``connector_type`` plus the already-resolved ``connection_id`` /
    ``operation_id`` component ids and the HTTP ``action_type`` verb. ``label`` is
    an optional shape userlabel. The builder's ``_validate_target_binding`` is the
    authority on the exact connector/verb enums; this model enforces the required
    fields are present and non-blank so a validated primitive does not emit a
    fragment the builder then rejects.
    """

    model_config = ConfigDict(extra="forbid")

    connector_type: str = Field(
        description="REST connector type ('rest' / 'rest_client').",
    )
    connection_id: str = Field(
        description="Resolved REST connector-settings component id (or $ref:KEY).",
    )
    operation_id: str = Field(
        description="Resolved REST connector-action component id (or $ref:KEY).",
    )
    action_type: str = Field(
        description="HTTP method for the leg target (GET/POST/PUT/PATCH/DELETE/...).",
    )
    label: Optional[str] = Field(
        default=None,
        description="Optional leg target shape label.",
    )

    @field_validator("connector_type", "connection_id", "operation_id", "action_type")
    @classmethod
    def _nonblank(cls, value: str) -> str:
        if not value.strip():
            raise ValueError("must be a non-empty string.")
        return value


class BranchParameters(BaseModel):
    """Parameters for the Branch (N-way forward fan-out) shape.

    ``targets`` are the additional fan-out legs (legs 2..N); leg 1 is the process's
    top-level ``target``. At least one additional target is required (a Branch needs
    2+ paths total). The builder caps the total at Boomi's 25-path limit.
    """

    model_config = ConfigDict(extra="forbid")

    targets: List[BranchTargetParameters] = Field(
        min_length=1,
        description="Additional fan-out leg targets (legs 2..N); leg 1 is the top-level target.",
    )


# ---------------------------------------------------------------------------
# Primitive
# ---------------------------------------------------------------------------


class BranchPrimitive(PrimitivePattern):
    """Declare a Branch (N-way forward fan-out) shape as a process fragment."""

    metadata = PatternMetadata(
        name="branch_fanout",
        version="1.0.0",
        kind=PatternKind.PRIMITIVE,
        description=(
            "Declare a Branch N-way fan-out: the same document runs down several "
            "independent paths in sequence (each path completes before the next "
            "begins, no rejoin). Leg 1 is the process's top-level target; the "
            "primitive's targets supply legs 2..N. Unconditional fan-out only — "
            "use a Decision/Route for value-comparing selection."
        ),
        tags=["routing", "fanout", "branch"],
        use_cases=[
            "Send the same document to several targets (e.g. a live target plus an audit log)",
            "Run independent per-target processing on one document with no rejoin",
        ],
        not_for=[
            "Value-comparing path selection (use a Decision/Route/Business Rules)",
            "True parallel execution (Branch legs run sequentially, not concurrently)",
        ],
    )
    parameters_model = BranchParameters

    input_contract = PatternIOContract(
        name="document_stream",
        description="Documents to fan out across the branch legs.",
    )
    output_contract = PatternIOContract(
        name="branch_fanout",
        description="The same document delivered to each independent forward leg in sequence.",
    )
    required_builders = ["ProcessFlowBuilder"]

    @classmethod
    def emit_components(
        cls,
        context: PrimitiveBuildContext,
        parameters: BaseModel,
    ) -> List[IntegrationComponentSpec]:
        # Fragment-only: the Branch shape and its legs are realized inline on the
        # process flow, so this primitive materializes no standalone components.
        return []

    @classmethod
    def emit_fragment(
        cls,
        context: PrimitiveBuildContext,
        parameters: BaseModel,
    ) -> Dict[str, Any]:
        params: BranchParameters = parameters  # type: ignore[assignment]
        targets: List[Dict[str, Any]] = []
        for leg in params.targets:
            target: Dict[str, Any] = {
                "connector_type": leg.connector_type,
                "connection_id": leg.connection_id,
                "operation_id": leg.operation_id,
                "action_type": leg.action_type,
            }
            if leg.label:
                target["label"] = leg.label
            targets.append(target)
        return {
            "process_config": {"branch": {"enabled": True, "targets": targets}},
            "depends_on": [],
        }
