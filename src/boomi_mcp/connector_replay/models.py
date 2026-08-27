"""Typed rows of the connector-replay evidence registry.

Every model here is frozen and forbids unknown keys. A registry is a published
artifact read by code that decides whether a write may be retried; a row that
silently absorbed an unrecognised key would be a row whose meaning depends on which
version of this module loaded it.

**The vocabulary finding that shapes these types.** Measured across 95 execution-
connector rows spanning eight HTTP verbs, the platform reports the SAME action value
— a single generic execute — for every verb. The verb is not in the execution record
at all; it lives in the operation component's own method field. A mapping keyed on
the platform's action would therefore be total, injective, and useless: it would
collapse GET, POST, PUT, DELETE, HEAD, OPTIONS, TRACE and PATCH onto one row and
report a delete as replay-safe because a read was.

So :class:`ConnectorVocabularyMappingV1` maps the platform's CONNECTOR type to a
family, and takes the action from the component. The two sources are named on the
model so that a future reader cannot re-derive the action from the execution record
without noticing the field is already spoken for.
"""

from __future__ import annotations

from enum import Enum
from typing import Final

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from .ids import is_boomi_component_id, is_execution_id

__all__ = [
    "ReplayRegistryModel",
    "SideEffectV1",
    "RetrySafetyV1",
    "ActionSourceV1",
    "ConnectorVocabularyMappingV1",
    "CapabilityEvidenceRecordV1",
]


class ReplayRegistryModel(BaseModel):
    """Frozen, closed base for every registry row.

    Its own base rather than a shared one from the authoring stack: importing that
    would defeat the packaging property this whole subpackage is built around.
    """

    model_config = ConfigDict(frozen=True, extra="forbid", str_strip_whitespace=False)


class SideEffectV1(str, Enum):
    """What executing an action does to the counterparty's state.

    ``unknown`` is the honest default and is NOT a synonym for ``read``. A registry
    that guessed ``read`` for an unobserved action would authorise retrying it.
    """

    READ = "read"
    WRITE = "write"
    UNKNOWN = "unknown"


class RetrySafetyV1(str, Enum):
    """Whether re-executing an action is safe, and on what evidence.

    ``conditionally_idempotent`` is the interesting value and the reason this enum
    is not a boolean: a PATCH that converges — a second identical call leaving the
    business state where the first put it — is safe to replay only while the request
    body is unchanged and the target is the same resource. That condition is
    recorded on the evidence row, never assumed.
    """

    UNVERIFIED = "unverified"
    IDEMPOTENT = "idempotent"
    CONDITIONALLY_IDEMPOTENT = "conditionally_idempotent"
    NON_IDEMPOTENT = "non_idempotent"


class ActionSourceV1(str, Enum):
    """Where an action name was read from.

    Recorded on every vocabulary mapping because the two sources are not
    interchangeable and the difference is invisible in the value itself. See the
    module docstring: the execution record reports one generic action for every
    verb, so a row claiming ``execution_record`` as its action source is either
    describing the generic value or is wrong.
    """

    OPERATION_COMPONENT = "operation_component"
    EXECUTION_RECORD = "execution_record"


class ConnectorVocabularyMappingV1(ReplayRegistryModel):
    """Maps a raw platform connector type onto a stable family name.

    The platform's connector type carries an account-specific segment, so it is not
    portable across accounts; the family name is. The mapping is what makes evidence
    captured on one account meaningful on another.
    """

    platform_connector_type: str = Field(min_length=1)
    family: str = Field(min_length=1, pattern=r"^[a-z][a-z0-9_]*$")
    action_source: ActionSourceV1

    @field_validator("platform_connector_type")
    @classmethod
    def _reject_the_sentinels(cls, value: str) -> str:
        """The no-data and return sentinels are not connectors.

        Both appear in real execution-connector rows beside genuine connector types
        — they are how the platform reports a step that moved no document and the
        synthetic return leg. Mapping either to a family would mint evidence for a
        connector that was never called.
        """
        if value in ("nodata", "return"):
            raise ValueError(
                f"{value!r} is an execution sentinel, not a connector type; it "
                "appears in real execution-connector rows and must never be mapped "
                "to a connector family"
            )
        return value

    @model_validator(mode="after")
    def _action_must_come_from_the_component(self) -> "ConnectorVocabularyMappingV1":
        if self.action_source is not ActionSourceV1.OPERATION_COMPONENT:
            raise ValueError(
                "the action must be read from the operation component. Measured "
                "across 95 execution-connector rows covering eight HTTP verbs, the "
                "platform reports one generic action for all of them, so an action "
                "taken from the execution record cannot distinguish a read from a "
                "delete"
            )
        return self


class CapabilityEvidenceRecordV1(ReplayRegistryModel):
    """One executed observation about a (family, action) pair.

    A row exists because an execution produced it. There is deliberately no way to
    construct one from documentation: the fields that would let you — a free-text
    justification, a source URL — are absent by design.
    """

    family: str = Field(min_length=1, pattern=r"^[a-z][a-z0-9_]*$")
    action: str = Field(min_length=1)
    side_effect: SideEffectV1
    retry_safety: RetrySafetyV1
    #: sha256 over the raw captured record bytes, in manifest order.
    capture_digest: str = Field(pattern=r"^[0-9a-f]{64}$")
    #: The executions this row rests on. At least one, and each a real execution id
    #: — which is where the strict grammar earns its keep: the platform's own
    #: documented example is undated and would otherwise sail in.
    execution_ids: tuple[str, ...] = Field(min_length=1)
    #: Present only when the observation is account-bound.
    account_scope_hash: str | None = Field(default=None, pattern=r"^[0-9a-f]{64}$")
    operation_component_id: str | None = None

    @field_validator("execution_ids")
    @classmethod
    def _every_execution_id_is_real(cls, value: tuple[str, ...]) -> tuple[str, ...]:
        bad = [v for v in value if not is_execution_id(v)]
        if bad:
            raise ValueError(
                f"not Boomi execution ids: {bad!r}. Note the platform's published "
                "example is UNDATED and is rejected here on purpose"
            )
        if len(set(value)) != len(value):
            raise ValueError(
                "duplicate execution ids: a row citing the same execution twice "
                "claims more evidence than it has"
            )
        return value

    @field_validator("operation_component_id")
    @classmethod
    def _component_id_is_real(cls, value: str | None) -> str | None:
        if value is not None and not is_boomi_component_id(value):
            raise ValueError(f"not a Boomi component id: {value!r}")
        return value

    @model_validator(mode="after")
    def _a_verified_write_needs_a_condition(self) -> "CapabilityEvidenceRecordV1":
        """A conditionally-idempotent row must say which operation it holds for.

        The condition IS the operation's identity: convergence was observed against
        one operation and connection, and claiming it class-wide from a single
        endpoint is exactly the overreach this registry exists to prevent.
        """
        if (
            self.retry_safety is RetrySafetyV1.CONDITIONALLY_IDEMPOTENT
            and self.operation_component_id is None
        ):
            raise ValueError(
                "a conditionally_idempotent row must name the operation component "
                "its convergence was observed against; the condition is not "
                "transferable to another operation"
            )
        if self.side_effect is SideEffectV1.UNKNOWN and self.retry_safety is not RetrySafetyV1.UNVERIFIED:
            raise ValueError(
                "an action whose side effect is unknown cannot carry a retry-safety "
                "verdict: there is nothing observed to base it on"
            )
        return self


#: The sentinels that appear in execution-connector rows and are not connectors.
#: Exposed so callers filter with the same set the validator refuses on, rather
#: than re-listing them.
EXECUTION_SENTINELS: Final[frozenset[str]] = frozenset({"nodata", "return"})
