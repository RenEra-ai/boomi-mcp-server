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

import re
from enum import Enum
from typing import Final

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from .ids import (
    AUTHORED_CONTRACT_REF_PATTERN,
    CONTRACT_ID_SEGMENT,
    is_authored_contract_ref,
    is_evidence_component_id,
    is_execution_id,
)

__all__ = [
    "ReplayRegistryModel",
    "SideEffectV1",
    "RetrySafetyV1",
    "ActionSourceV1",
    "ConnectorVocabularyMappingV1",
    "CapabilityEvidenceRecordV1",
    "KeyMechanismV1",
    "KeyScopeV1",
    "DuplicateGuaranteeV1",
    "PlacementObservationV1",
    "InputObservationV1",
    "OutputObservationV1",
    "EffectObservationV1",
    "ReplayObservationV1",
    "EvidenceScopeV1",
    "EvidenceSourceV1",
    "ClosedCaptureObservationsV1",
    "ContractKeySemanticsDefinitionV1",
    "ComponentProjectionAllowlistV1",
    "CaptureReferenceV1",
    "LiveComponentIdentityV1",
    "StaticRouteCoverageV1",
    "ServiceWideRouteCoverageV1",
    "OperationContractRecordV1",
    "ConnectorReplayRegistryV1",
]


#: The sentinels that appear in execution-connector rows and are not connectors.
#: Exposed so callers filter with the same set the validator refuses on, rather
#: than re-listing them.
EXECUTION_SENTINELS: Final[frozenset[str]] = frozenset({"nodata", "return"})


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
    #: The actions this family is RECOGNISED to have. Closed, and evidence-derived
    #: from the verbs the archive shows exercised.
    #:
    #: Resolving the family alone was not enough: an evidence row could name any
    #: non-empty action string and still return an affirmative verdict, so an
    #: invented action inherited a mapped family's authority. The plan asks for a
    #: total, injective family-AND-action vocabulary; this is the action half.
    recognised_actions: tuple[str, ...] = Field(min_length=1)
    #: The subset that is SAFE — no request-side effect — by the transport's own
    #: specification, not by this project's judgement. Needed because "nothing
    #: changed" is consistent with a read AND with a write that no-opped, and only
    #: the verb's declared semantics separate them.
    safe_actions: tuple[str, ...] = ()
    #: The RECORDED raw-action -> grammar-safe-identifier mapping, as sorted pairs.
    #:
    #: Recorded rather than folded at the point of use. The obvious fold is
    #: lowercase, and on today's vocabulary it happens to be injective — but a fold
    #: that is injective on the values seen so far is not a mapping, it is a
    #: coincidence with a nice name, and the first raw pair that collides under it
    #: would silently give two contracts one identifier. Writing the mapping down
    #: makes a collision a refusal at mint, where the vocabulary is in view, rather
    #: than an ambiguity discovered by whoever is holding the reference later.
    #:
    #: The family half needs no second field: `family` is already the stable,
    #: grammar-safe name this maps the raw `platform_connector_type` onto, and a
    #: parallel `family_id` carrying the same value would be one more hand-kept
    #: copy of a fact that already has an owner.
    action_ids: tuple[tuple[str, str], ...] = ()

    @property
    def family_id(self) -> str:
        """The grammar-safe family identifier — `family`, named for its role."""
        return self.family

    def action_id(self, action: str) -> str:
        """The grammar-safe identifier for one raw action, or refuse."""
        for raw, ident in self.action_ids:
            if raw == action:
                return ident
        raise ValueError(
            f"action {action!r} has no recorded identifier for family "
            f"{self.family!r}; the vocabulary is closed, and an unrecorded action "
            "is one this registry cannot name"
        )

    @field_validator("action_ids")
    @classmethod
    def _identifiers_are_sorted_and_grammar_safe(
        cls, value: tuple[tuple[str, str], ...]
    ) -> tuple[tuple[str, str], ...]:
        if list(value) != sorted(value):
            raise ValueError("action identifiers must be sorted, so one map has one form")
        raws = [raw for raw, _ in value]
        if len(set(raws)) != len(raws):
            raise ValueError("an action is mapped twice, so its identifier is ambiguous")
        idents = [ident for _, ident in value]
        if len(set(idents)) != len(idents):
            # A COLLISION IS A MINT-TIME FAILURE, never a silent fold: two raw
            # actions sharing one identifier means two contracts share one
            # reference, and nothing downstream could tell them apart.
            raise ValueError(
                "two raw actions share one identifier: "
                + repr(sorted({i for i in idents if idents.count(i) > 1}))
            )
        for _, ident in value:
            if not re.fullmatch(CONTRACT_ID_SEGMENT, ident):
                raise ValueError(
                    f"action identifier {ident!r} is not grammar-safe "
                    f"({CONTRACT_ID_SEGMENT})"
                )
        return value

    @model_validator(mode="after")
    def _the_identifier_map_is_total(self) -> "ConnectorVocabularyMappingV1":
        """Every recognised action has an identifier, and no others do.

        Totality in both directions. A missing entry leaves an action this
        registry recognises but cannot name; a surplus entry names an action it
        does not recognise, which is a mapping to nothing.
        """
        if not self.action_ids:
            return self
        mapped = {raw for raw, _ in self.action_ids}
        recognised = set(self.recognised_actions)
        if mapped != recognised:
            raise ValueError(
                "the action identifier map is not total over the recognised "
                f"actions: unmapped={sorted(recognised - mapped)}, "
                f"unrecognised={sorted(mapped - recognised)}"
            )
        return self

    @field_validator("recognised_actions")
    @classmethod
    def _actions_are_sorted_and_unique(cls, value: tuple[str, ...]) -> tuple[str, ...]:
        if len(set(value)) != len(value):
            raise ValueError("duplicate recognised actions")
        if list(value) != sorted(value):
            raise ValueError("recognised actions must be sorted, so one set has one form")
        return value

    @field_validator("platform_connector_type")
    @classmethod
    def _reject_the_sentinels(cls, value: str) -> str:
        """The no-data and return sentinels are not connectors.

        Both appear in real execution-connector rows beside genuine connector types
        — they are how the platform reports a step that moved no document and the
        synthetic return leg. Mapping either to a family would mint evidence for a
        connector that was never called.
        """
        if value in EXECUTION_SENTINELS:
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
    #: What the capture OBSERVED about consuming and producing documents (A9).
    accepts_input: "InputObservationV1"
    produces_output: "OutputObservationV1"
    #: The capture this row rests on, with its closed observations. REQUIRED.
    #:
    #: Without it the row carried a declared `retry_safety` beside a digest and
    #: some execution ids, and nothing connected the verdict to anything observed —
    #: a fabricated row with a plausible digest and one grammar-valid execution id
    #: loaded and returned `idempotent`. A verdict that does not have to agree with
    #: an observation is not evidence; it is an assertion with provenance-shaped
    #: decoration.
    capture: "CaptureReferenceV1"
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
        if value is not None and not is_evidence_component_id(value):
            raise ValueError(f"not a Boomi component id: {value!r}")
        return value

    @model_validator(mode="after")
    def _the_verdict_must_follow_the_observation(self) -> "CapabilityEvidenceRecordV1":
        """The declared verdict must be one the capture's replay observation supports.

        This is the binding that makes a row evidence. The mapping is deliberately
        narrow — each verdict names the observations that can produce it, and
        anything else refuses:

        * a replay never exercised supports no affirmative verdict at all;
        * a replay that produced the same effect or the same result supports a
          conditional verdict, never an unconditional one;
        * a replay that duplicated the effect supports only `non_idempotent`.
        """
        replay = self.capture.summary.replay
        allowed: dict[ReplayObservationV1, set[RetrySafetyV1]] = {
            ReplayObservationV1.NOT_EXERCISED: {RetrySafetyV1.UNVERIFIED},
            ReplayObservationV1.SAME_EFFECT: {
                RetrySafetyV1.UNVERIFIED, RetrySafetyV1.CONDITIONALLY_IDEMPOTENT,
                RetrySafetyV1.IDEMPOTENT},
            ReplayObservationV1.SAME_RESULT: {
                RetrySafetyV1.UNVERIFIED, RetrySafetyV1.CONDITIONALLY_IDEMPOTENT,
                RetrySafetyV1.IDEMPOTENT},
            ReplayObservationV1.CONFLICT_WITHOUT_SECOND_EFFECT: {
                RetrySafetyV1.UNVERIFIED, RetrySafetyV1.CONDITIONALLY_IDEMPOTENT},
            ReplayObservationV1.DUPLICATE_EFFECT: {
                RetrySafetyV1.UNVERIFIED, RetrySafetyV1.NON_IDEMPOTENT},
        }
        if self.retry_safety not in allowed[replay]:
            raise ValueError(
                f"retry_safety {self.retry_safety.value!r} is not supported by the "
                f"capture's replay observation {replay.value!r}; a verdict must "
                "follow what was observed, not accompany it"
            )
        # An unconditional verdict additionally requires the observation to be
        # action-wide. Convergence seen on ONE operation says nothing about the
        # next one.
        if (self.retry_safety is RetrySafetyV1.IDEMPOTENT
                and self.capture.summary.scope is not EvidenceScopeV1.ACTION_SEMANTICS):
            raise ValueError(
                "an unconditional idempotent verdict needs action-wide evidence; "
                f"this capture's scope is {self.capture.summary.scope.value!r}"
            )
        return self

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



# ---------------------------------------------------------------------------
# The typed registry root and the records it carries.
#
# These exist so the packaged artifact has a SCHEMA rather than a shape. Until
# they landed, the loader read two sections it knew about and let everything else
# through as plain dictionaries — which is how an untyped operation record, and a
# key the reader had never heard of, both loaded successfully.
# ---------------------------------------------------------------------------


# The vocabularies below are the DESIGN'S, verbatim. An earlier version invented
# plausible-looking alternatives — `client_supplied_key`, `rejects_duplicate`,
# `connection` scope — which read sensibly and were not the published contract. A
# closed vocabulary is a contract with the slices that consume it, so inventing a
# near-synonym means the consuming slice cannot express what it was designed to.


class KeyMechanismV1(str, Enum):
    """How a contract's idempotency key works."""

    REQUEST_KEY_DEDUPLICATION = "request_key_deduplication"
    RESOURCE_IDENTITY_UPSERT = "resource_identity_upsert"


class KeyScopeV1(str, Enum):
    """Where a key is unique. `unknown` is not a member on purpose.

    A scope nobody has determined is an absent record, not a record with an
    unknown scope — the latter would be storable, and therefore citable.
    """

    OPERATION = "operation"
    STATIC_ROUTE = "static_route"
    SERVICE = "service"


class DuplicateGuaranteeV1(str, Enum):
    """What the counterparty does with a repeated key."""

    SAME_EFFECT = "same_effect"
    SAME_RESULT = "same_result"
    CONFLICT_WITHOUT_SECOND_EFFECT = "conflict_without_second_effect"


class PlacementObservationV1(str, Enum):
    """Where in the process the connector ran."""

    ENTRY = "entry"
    DOWNSTREAM = "downstream"


class InputObservationV1(str, Enum):
    """What the connector consumed."""

    NO_INBOUND_DOCUMENTS = "no_inbound_documents"
    DOCUMENTS_CONSUMED = "documents_consumed"


class OutputObservationV1(str, Enum):
    """What was observed RECEIVING the connector's output.

    The subject is the receiver, not the connector: these values distinguish who got
    the documents, and a connector completing successfully is not one of them. The
    earlier wording said "what the connector produced", which is a different question
    and is the one whose answer was published here by mistake.
    """

    SUCCESSOR_RECEIVED_DOCUMENTS = "successor_received_documents"
    RETURN_DOCUMENTS_RECEIVED = "return_documents_received"
    NO_OUTPUT_OBSERVED = "no_output_observed"


class EffectObservationV1(str, Enum):
    """What the counterparty's state did."""

    READ_ONLY = "read_only"
    STATE_CREATED = "state_created"
    STATE_CHANGED = "state_changed"
    STATE_DELETED = "state_deleted"
    STATE_UNCHANGED_AFTER_REPLAY = "state_unchanged_after_replay"


class ReplayObservationV1(str, Enum):
    """What a second identical execution did."""

    NOT_EXERCISED = "not_exercised"
    SAME_EFFECT = "same_effect"
    SAME_RESULT = "same_result"
    CONFLICT_WITHOUT_SECOND_EFFECT = "conflict_without_second_effect"
    DUPLICATE_EFFECT = "duplicate_effect"


class EvidenceScopeV1(str, Enum):
    """How far an observation generalises."""

    ACTION_SEMANTICS = "action_semantics"
    SINGLE_OPERATION = "single_operation"
    SERVICE_WIDE_ROUTE = "service_wide_route"


class EvidenceSourceV1(str, Enum):
    """Which platform artifact backs an observation.

    Named because the artifacts back DIFFERENT facts and are not interchangeable:
    the execution record backs status and aggregate counts, the execution connector
    backs family/action and placement, the per-document record backs per-document
    status, and only an endpoint readback backs a side-effect or replay claim.
    """

    EXECUTION_RECORD = "execution_record"
    EXECUTION_CONNECTOR = "execution_connector"
    GENERIC_CONNECTOR_RECORD = "generic_connector_record"
    ENDPOINT_READBACK = "endpoint_readback"


class ContractKeySemanticsDefinitionV1(ReplayRegistryModel):
    """What a contract's key MEANS, versioned independently of the rows citing it.

    Carries a revision because the meaning can be corrected without the evidence
    changing: a row cites a definition AND its revision, so a later correction
    cannot silently re-interpret evidence gathered under the old reading.
    """

    semantics_id: str = Field(min_length=1, pattern=r"^[a-z][a-z0-9_]*$")
    revision: int = Field(ge=1)
    mechanism: KeyMechanismV1
    key_scope: KeyScopeV1
    duplicate_guarantee: DuplicateGuaranteeV1


class ComponentProjectionAllowlistV1(ReplayRegistryModel):
    """The closed projection a config digest is computed over, as DATA.

    Versioned: a projection change alters every digest computed under it, so a row
    must be able to say which projection produced its digest. Two digests from
    different projection versions are not comparable, and nothing should have to
    infer that from context.
    """

    family: str = Field(min_length=1, pattern=r"^[a-z][a-z0-9_]*$")
    component_kind: str = Field(pattern=r"^(operation|connection)$")
    projection_version: int = Field(ge=1)
    included_attributes: tuple[str, ...] = ()
    included_value_fields: tuple[str, ...] = ()
    included_property_fields: tuple[str, ...] = ()
    #: Fields KNOWN and deliberately left out. Distinct from unknown fields, which
    #: refuse: naming an exclusion forces a decision per field instead of silent
    #: omission, and a field nobody has classified is the one that might matter.
    excluded_fields: tuple[str, ...] = ()
    #: Elements the projection scope may contain. An element outside this set is
    #: unknown content and refuses — the allowlist binds an unbounded XML space by
    #: design, and silently skipping an unrecognised element is the fail-OPEN
    #: reading of the same allowlist.
    included_elements: tuple[str, ...] = ()
    #: Attributes carried into the digest from ANY element the projection admits.
    #: Scope is derived, not enumerated per shape: every element whose name is in
    #: ``included_elements`` must have every one of its attributes classified — here,
    #: in ``excluded_scope_attributes``, or as one of the structural names the
    #: projection carries by construction. Anything else is unknown content and
    #: refuses, exactly as an unknown field or element does. Enumerating the
    #: SHAPES instead (attributes on fields, then on properties, then on config
    #: elements) found a new uncovered shape every review round; the admitted-element
    #: set is finite and the projection already owns it.
    included_scope_attributes: tuple[str, ...] = ()
    #: Attributes on an admitted element KNOWN and deliberately left out. Naming an
    #: exclusion forces a decision per attribute instead of silent omission. The
    #: OAuth endpoint and client attributes live here for the same reason the
    #: credentials element does: this digest is published and they are credential
    #: material, not routing identity. A static header's or query parameter's value
    #: is here too — keys reach the digest and values never do.
    excluded_scope_attributes: tuple[str, ...] = ()
    #: Names whose VALUES are qualified names, so canonicalisation must resolve
    #: their prefixes rather than compare them textually.
    qname_aware_tags: tuple[str, ...] = ()
    qname_aware_attrs: tuple[str, ...] = ()


class ClosedCaptureObservationsV1(ReplayRegistryModel):
    """A capture's findings, in closed vocabularies only.

    No free text and no booleans: a boolean cannot say "not exercised", and this
    record must distinguish "the replay produced the same effect" from "no replay
    was attempted". Collapsing those is how an unexercised path acquires a verdict.
    """

    placement: PlacementObservationV1
    input_observation: InputObservationV1
    output_observation: OutputObservationV1
    effect: EffectObservationV1
    replay: ReplayObservationV1
    scope: EvidenceScopeV1
    #: Which artifacts back this, sorted and unique — the claim is only as strong as
    #: the artifact behind it, and a side-effect claim without an endpoint readback
    #: rests on nothing that observed the counterparty.
    sources: tuple[EvidenceSourceV1, ...] = Field(min_length=1)

    @field_validator("sources")
    @classmethod
    def _sorted_unique(cls, value: tuple) -> tuple:
        raw = [v.value for v in value]
        if len(set(raw)) != len(raw):
            raise ValueError("duplicate evidence sources claim more backing than held")
        if raw != sorted(raw):
            raise ValueError("evidence sources must be sorted, so one set has one form")
        return value

    @model_validator(mode="after")
    def _a_state_claim_needs_a_readback(self) -> "ClosedCaptureObservationsV1":
        state_claims = {
            EffectObservationV1.STATE_CREATED, EffectObservationV1.STATE_CHANGED,
            EffectObservationV1.STATE_DELETED,
            EffectObservationV1.STATE_UNCHANGED_AFTER_REPLAY,
        }
        if self.effect in state_claims and EvidenceSourceV1.ENDPOINT_READBACK not in self.sources:
            raise ValueError(
                f"effect {self.effect.value!r} claims something about the "
                "counterparty's state, and only an endpoint readback observes that. "
                "The platform reports an execution complete even when the "
                "counterparty refused the request"
            )
        # ...AND A REPLAY CLAIM NEEDS ONE FOR THE SAME REASON. This validator
        # covered the effect and left the replay observation beside it unguarded,
        # though every replay value except "not exercised" is a statement about
        # what a SECOND identical execution did to the counterparty — the same
        # kind of fact, observable by the same single artifact. The gap was
        # reachable: an operation record could name execution-side sources only,
        # claim that a replay left the effect unchanged, be re-digested through
        # the published minter so every other check agreed, and mint a grant.
        # Provenance-shaped authorization is not evidence, and the source list is
        # where that distinction lives.
        if (
            self.replay is not ReplayObservationV1.NOT_EXERCISED
            and EvidenceSourceV1.ENDPOINT_READBACK not in self.sources
        ):
            raise ValueError(
                f"replay {self.replay.value!r} states what a second identical "
                "execution did to the counterparty, and only an endpoint readback "
                "observes that. An execution record reports that a call completed, "
                "not what it left behind"
            )
        return self


class CaptureReferenceV1(ReplayRegistryModel):
    """A pointer to the executed capture a record rests on."""

    execution_id: str
    captured_at: str = Field(min_length=1)
    account_scope_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    summary: ClosedCaptureObservationsV1
    capture_digest: str = Field(pattern=r"^[0-9a-f]{64}$")

    @field_validator("execution_id")
    @classmethod
    def _execution_id_is_real(cls, value: str) -> str:
        if not is_execution_id(value):
            raise ValueError(f"not a Boomi execution id: {value!r}")
        return value


class LiveComponentIdentityV1(ReplayRegistryModel):
    """Which component, at which version, configured which way.

    The version alone is insufficient — a credential-only edit advances it without
    changing behaviour — and the digest alone is insufficient, because it is
    computed over a projection that deliberately omits most of the component. Both
    are carried so a mismatch in either is visible.
    """

    component_id: str
    version: int = Field(ge=1)
    config_digest: str = Field(pattern=r"^ComponentConfigDigestV1:[0-9a-f]{64}$")

    @field_validator("component_id")
    @classmethod
    def _component_id_is_real(cls, value: str) -> str:
        if not is_evidence_component_id(value):
            raise ValueError(f"not a Boomi component id: {value!r}")
        return value


class StaticRouteCoverageV1(ReplayRegistryModel):
    """Coverage of specific, enumerated routes."""

    kind: str = Field(default="static_path", pattern=r"^static_path$")
    route_digests: tuple[str, ...] = Field(min_length=1)

    @field_validator("route_digests")
    @classmethod
    def _sorted_unique_and_versioned(cls, value: tuple[str, ...]) -> tuple[str, ...]:
        bad = [d for d in value if not re.fullmatch(r"RouteDigestV1:[0-9a-f]{64}", d)]
        if bad:
            raise ValueError(f"not RouteDigestV1 values: {bad!r}")
        if len(set(value)) != len(value):
            raise ValueError("duplicate route digests claim more coverage than held")
        if list(value) != sorted(value):
            raise ValueError("route digests must be sorted, so one coverage set has one form")
        return value


class ServiceWideRouteCoverageV1(ReplayRegistryModel):
    """Coverage of a service rather than of enumerated routes.

    This is what a DYNAMICALLY BOUND path requires. Its route is composed per
    document, so no static digest identifies it and none may be minted — the
    evidence has to be service-wide or it does not exist.
    """

    kind: str = Field(default="service_wide", pattern=r"^service_wide$")
    service_wide_capture: CaptureReferenceV1

    @model_validator(mode="after")
    def _the_capture_must_actually_be_service_wide(self) -> "ServiceWideRouteCoverageV1":
        """A single-endpoint capture cannot establish service-wide coverage.

        Accepting any capture here let a row claim coverage of a whole service from
        one operation's evidence — which is precisely the overreach the scope field
        exists to record.
        """
        if self.service_wide_capture.summary.scope is not EvidenceScopeV1.SERVICE_WIDE_ROUTE:
            raise ValueError(
                "service-wide route coverage needs a capture whose scope is "
                f"service-wide; this one is "
                f"{self.service_wide_capture.summary.scope.value!r}"
            )
        return self


class OperationContractRecordV1(ReplayRegistryModel):
    """An account-bound record that a specific operation's replay was observed."""

    #: Constrained by the ONE shared grammar, not by "non-empty". A reference is
    #: the name a relocatable artifact carries to reach this record, so a record
    #: whose own name the authoring surface would reject is a record nothing can
    #: cite. Whether the name is the RIGHT one — the derivation from the
    #: vocabulary — is checked at the registry, where the vocabulary is in view.
    contract_ref: str = Field(min_length=1)
    family: str = Field(min_length=1, pattern=r"^[a-z][a-z0-9_]*$")
    action: str = Field(min_length=1)
    semantics_id: str = Field(min_length=1)
    semantics_revision: int = Field(ge=1)
    account_scope_hash: str = Field(pattern=r"^[0-9a-f]{64}$")

    @field_validator("contract_ref")
    @classmethod
    def _ref_obeys_the_one_shared_grammar(cls, value: str) -> str:
        """The record's own name must be one the authoring surface would accept.

        Not a `pattern=` constraint: the grammar ends in a negative lookahead,
        which pydantic's regex engine does not support. Whether the name is the
        RIGHT one — the derivation from the registry's vocabulary — is checked at
        the registry, where the vocabulary is in view; this only rules out a
        record nothing could cite.
        """
        if not is_authored_contract_ref(value):
            raise ValueError(
                f"contract reference {value!r} does not match the one shared "
                f"grammar {AUTHORED_CONTRACT_REF_PATTERN}"
            )
        return value
    operation_identity: LiveComponentIdentityV1
    connection_identity: LiveComponentIdentityV1
    route_coverage: StaticRouteCoverageV1 | ServiceWideRouteCoverageV1
    capture: CaptureReferenceV1
    record_digest: str = Field(pattern=r"^[0-9a-f]{64}$")


class ConnectorReplayRegistryV1(ReplayRegistryModel):
    """The packaged artifact's typed root.

    Every section is required. An absent section used to default to empty, which
    turned a truncated file into an apparently valid deny-all registry — and empty
    IS the safe runtime state, so the corruption produced exactly the behaviour a
    healthy empty registry produces.
    """

    schema_version: int = Field(ge=1)
    vocabulary: tuple[ConnectorVocabularyMappingV1, ...]
    semantics_definitions: tuple[ContractKeySemanticsDefinitionV1, ...] = ()
    projection_allowlists: tuple[ComponentProjectionAllowlistV1, ...] = ()
    evidence_records: tuple[CapabilityEvidenceRecordV1, ...]
    operation_records: tuple[OperationContractRecordV1, ...]
