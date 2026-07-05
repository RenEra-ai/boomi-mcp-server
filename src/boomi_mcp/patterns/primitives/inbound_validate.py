"""M6 (issue #12): ``inbound_validate`` build-time inbound-contract primitive.

A validation-only primitive: it emits NO components and NO process shapes.
``mode="profile_bound"`` asserts, at archetype-assembly time, that the listener
this integration is built around declares a machine-readable inbound contract —
a JSON/XML ``input_type`` **with a bound request profile** — so downstream
transform mappings resolve against a declared shape instead of an opaque body.

Boomi's Web Services Server performs no schema validation at the perimeter
(a bare listener accepts any body and hands it to the process), so M6
deliberately ships inbound validation as a BUILD-TIME contract, not a runtime
shape: there is no verified native "validate request against profile" step to
emit, and generating a Decision/scripting validation flow is out of scope
(deferred — see the M6 plan's deferral list). What the primitive guarantees is
that a caller who asked for inbound validation cannot end up with a
profile-less listener where the request body shape is unspecified.

Usage: the listener archetypes run :meth:`validate_contract` when the caller
sets ``inbound_validation.enabled=true``; hand-authored specs can invoke the
primitive the same way. The fragment records the contract in the emitted
spec's metadata so reviewers can see validation was requested and satisfied.
"""

from __future__ import annotations

from typing import Any, Dict, List, Literal, Optional

from pydantic import BaseModel, ConfigDict, Field

from ...categories.components.builders.connector_builder import (
    BuilderValidationError,
)
from ...models.integration_models import IntegrationComponentSpec
from ..base import (
    PatternKind,
    PatternMetadata,
    PrimitiveBuildContext,
    PrimitivePattern,
)

# Primitive-layer error code: inbound validation was requested but the listener
# declares no machine-readable request contract.
INBOUND_VALIDATION_UNSATISFIABLE = "INBOUND_VALIDATION_UNSATISFIABLE"

# Listener input types that carry a profile-bindable document shape.
_PROFILE_BOUND_INPUT_TYPES = frozenset(
    {"singlejson", "multijson", "singlexml", "multixml"}
)


class InboundValidateParameters(BaseModel):
    model_config = ConfigDict(extra="forbid")

    mode: Literal["profile_bound"] = Field(
        default="profile_bound",
        description=(
            "Validation mode. M6 ships 'profile_bound' only: the listener must "
            "declare a JSON/XML input_type with a bound request profile."
        ),
    )
    listener_input_type: str = Field(
        ...,
        description="The listener operation's input_type (as passed to wss_listen).",
    )
    listener_request_profile_id: Optional[str] = Field(
        default=None,
        description=(
            "The listener operation's request profile binding ('$ref:KEY' or "
            "UUID); None when the listener binds no profile."
        ),
    )


class InboundValidatePrimitive(PrimitivePattern):
    """Assert the listener's inbound contract at build time (no shapes emitted)."""

    metadata = PatternMetadata(
        name="inbound_validate",
        version="1.0.0",
        kind=PatternKind.PRIMITIVE,
        description=(
            "Build-time inbound-contract validation for listener integrations "
            "(mode='profile_bound'): asserts the wss_listen operation declares "
            "a JSON/XML input_type with a bound request profile, so the inbound "
            "body shape is a declared contract rather than an opaque payload. "
            "Emits no components and no process shapes — Boomi's Web Services "
            "Server does not schema-validate at the perimeter, and a generated "
            "runtime validation flow is deferred beyond M6. Fails with "
            "INBOUND_VALIDATION_UNSATISFIABLE when validation is requested but "
            "the listener has no compatible request profile."
        ),
        tags=["listener", "validation", "inbound", "contract"],
        use_cases=[
            "Guarantee a listener integration declares its inbound payload shape",
            "Fail an archetype build early when inbound validation is requested but unsatisfiable",
        ],
        not_for=[
            "Runtime payload validation (no native WSS schema-validation step; deferred beyond M6)",
            "Outbound response validation",
        ],
    )
    parameters_model = InboundValidateParameters

    @classmethod
    def validate_contract(cls, parameters: BaseModel) -> None:
        """Raise INBOUND_VALIDATION_UNSATISFIABLE when the contract cannot hold."""
        params: InboundValidateParameters = parameters  # type: ignore[assignment]
        input_type = str(params.listener_input_type or "").strip().lower()
        if input_type not in _PROFILE_BOUND_INPUT_TYPES:
            raise BuilderValidationError(
                f"inbound validation (mode='profile_bound') requires a JSON/XML "
                f"listener input_type; got {params.listener_input_type!r}",
                error_code=INBOUND_VALIDATION_UNSATISFIABLE,
                field="listener_input_type",
                hint=(
                    "Set the listener input_type to singlejson/multijson/"
                    "singlexml/multixml (a 'none'/'singledata' listener carries "
                    "no profile-bindable document), or disable inbound_validation."
                ),
            )
        profile_id = params.listener_request_profile_id
        if not isinstance(profile_id, str) or not profile_id.strip():
            raise BuilderValidationError(
                "inbound validation (mode='profile_bound') requires the listener "
                "to bind a request profile, but none is set",
                error_code=INBOUND_VALIDATION_UNSATISFIABLE,
                field="listener_request_profile_id",
                hint=(
                    "Bind the listener operation's request_profile to the "
                    "inbound payload profile, or disable inbound_validation."
                ),
            )

    @classmethod
    def emit_components(
        cls,
        context: PrimitiveBuildContext,
        parameters: BaseModel,
    ) -> List[IntegrationComponentSpec]:
        # Validation-only: the contract check runs, then no components emit.
        cls.validate_contract(parameters)
        return []

    @classmethod
    def emit_fragment(
        cls,
        context: PrimitiveBuildContext,
        parameters: BaseModel,
    ) -> Dict[str, Any]:
        cls.validate_contract(parameters)
        params: InboundValidateParameters = parameters  # type: ignore[assignment]
        return {
            "metadata": {
                "inbound_validation": {
                    "mode": params.mode,
                    "input_type": str(params.listener_input_type).strip().lower(),
                    "request_profile_id": params.listener_request_profile_id,
                }
            }
        }
