"""Strict typed MCP authoring contracts (issue #146, M12.11).

The authored REQUEST and the two read-only RESULTS of the versioned authoring
workflow, plus the revision binding that ties one to the other.

**What this module is for.** ``build_integration`` already owns the
materialization lifecycle. #146 adds one additive action — ``compile`` — and one
opt-in typed request so a caller can hand the server ProcessIR, a topology
adjunct, or a recipe invocation instead of a hand-assembled component plan. The
models here are that request and its answers; the orchestration that consumes
them lives in :mod:`boomi_mcp.authoring.workflow`.

**Three phases, three distinct contracts** (ADR-001 §6, #146 note):

* ``plan_integration_design``          — advisory prose in, doctrine out. Never compiles.
* ``build_integration(action="plan")`` — semantic validation + ComponentPlan preview.
* ``build_integration(action="compile")`` — canonical compilation + artifact fingerprints.

Plan and compile are read-only. Both carry ``mutation_performed: Literal[False]``
as a *typed* field rather than a convention, so a response that claims otherwise
cannot be constructed.

**Terminology is load-bearing here** (issue #146 acceptance criterion). Four
different graph-shaped things flow through this surface and none of them is
called "flow":

* ``pipeline_stages``        — the inert ``PipelineSpec`` echo (ADR-001 §5)
* ``process_cfg``            — the compiler's semantic control-flow graph
* ``component_dependencies`` — ComponentPlan materialization edges
* ``topology_relations``     — ``SystemTopologySpecV1`` relations

The pre-existing ``IntegrationSpecV1.flows`` field and the ``flow_sequence``
process-config key keep their names — they are frozen legacy surface, and
renaming them is not what this issue is for. The rule binds the names *this*
module introduces.

**Secrets.** Every collection is closed and typed; there is no configuration bag
and no free-form mapping on the result side at all. The one place a caller's
opaque payload passes through — ``RecipeInvocationRequestV1.raw_input`` — is
handed to the recipe engine, whose own input gate (``RECIPE_INPUT_INVALID``)
scans it before any recipe sees it. This module does not re-implement that scan;
it routes to it. Results carry hashes, opaque references, and value-free
diagnostics — never credentials, headers, connection properties, environment
extensions, document data, or raw XML (ADR-001 §11).
"""

from __future__ import annotations

import json
import re
from typing import (
    Annotated,
    Any,
    Dict,
    Literal,
    Mapping,
    Optional,
    Tuple,
    Union,
    get_args,
)

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    StrictBool,
    StrictInt,
    StringConstraints,
    field_validator,
    model_validator,
)
from pydantic_core import PydanticCustomError

from .integration_models import IntegrationComponentSpec, IntegrationSpecV1
from .process_component import ProcessAuthoringUnitV1
from .process_ir import ProcessIRV1
from .system_topology import SystemTopologySpecV1

#: The authoring CONTRACT version. Distinct from every schema's own version: this
#: names the shape of the request/result envelope, not of ProcessIR or topology.
#:
#: Bumped to "2" by #153 (M12.15), together with — and never separately from —
#: the breaking ``process_ir`` intent reshape below (``component_key`` +
#: ``process_ir`` -> ``units``). Version travels with SHAPE: publishing the new
#: request shape under ``contract_version: "1"`` would leave the served contract
#: self-inconsistent for any caller that bound to revision 1. There is no
#: compatibility alias, because the issue records zero users.
AUTHORING_CONTRACT_VERSION = "2"

#: The closed action set the whole surface agrees on. ``server.py``'s docstring,
#: the ``list_capabilities`` catalog, the workflow schema, and the builder's
#: dispatcher all read this — a parity test asserts they cannot drift.
AUTHORING_ACTIONS: Tuple[str, ...] = ("plan", "compile", "apply", "verify")

#: Severity ranking for deterministic diagnostic ordering. Errors first: a reader
#: scanning a truncated list must see what blocks before what merely warns.
_SEVERITY_RANK = {"error": 0, "warning": 1, "advisory": 2}

_CANONICAL_JSON = {
    "sort_keys": True,
    "separators": (",", ":"),
    "ensure_ascii": True,
    "allow_nan": False,
}

#: ``sha256:<64 lowercase hex>``. One spelling, asserted rather than assumed —
#: two spellings of a digest is how a comparison silently becomes a mismatch.
DigestString = Annotated[
    str, StringConstraints(pattern=r"^sha256:[0-9a-f]{64}$")
]

NonEmptyString = Annotated[str, StringConstraints(min_length=1)]


class _AuthoringModel(BaseModel):
    """Strict, frozen, and repr-safe.

    ``extra="forbid"`` is the point: a typed authoring request that silently
    accepted an unknown key would let a caller believe they had configured
    something the server ignored.

    ``frozen=True`` is SHALLOW (issue #145) — it stops attribute assignment, not
    mutation of a ``dict`` held in a field. Only ``RecipeInvocationRequestV1``
    holds one, and the orchestration re-parses the raw payload at the apply
    boundary rather than trusting an object handed down from an earlier phase.
    """

    model_config = ConfigDict(extra="forbid", frozen=True)


# ---------------------------------------------------------------------------
# request side
# ---------------------------------------------------------------------------


class DecisionResolutionV1(_AuthoringModel):
    """A caller's answer to one required decision."""

    decision_id: NonEmptyString
    option_id: NonEmptyString


class IntegrationSpecAuthoringIntentV1(_AuthoringModel):
    """An already-assembled component plan, entering the typed workflow."""

    intent_kind: Literal["integration_spec"] = "integration_spec"
    integration_spec: IntegrationSpecV1
    conflict_policy: Literal["reuse", "clone", "fail"] = "reuse"


class ProcessIRAuthoringIntentV1(_AuthoringModel):
    """Direct ProcessIR authoring — the canonical semantic authority.

    ``components`` is the component plan the IR's ``$ref:KEY`` tokens resolve
    against. It is REQUIRED and not derived: a ``$ref`` names a component the
    caller intends to materialize, and guessing that plan from the IR is exactly
    the legacy inference ADR-001 §6 removed.

    **#153 (M12.15) reshape — breaking, no alias.** The singular
    ``component_key`` + ``process_ir`` pair is replaced by ``units``: one
    :class:`ProcessAuthoringUnitV1` per root, each pairing exactly one envelope
    with exactly one root. Two reasons, both structural rather than cosmetic:

    * **Cardinality.** Multi-root is already real on the recipe arm, which
      normalizes several composed roots at once. A singular pair could only ever
      express one, so direct authoring was the odd surface out.
    * **Materializability.** A root cannot be APPLIED without the envelope data
      (name, action, placement, dependencies, extension bindings) that
      ``ProcessIRV1`` deliberately refuses to carry. Pairing them in one required
      model is what makes "no root applies without an envelope" unexpressible to
      violate, rather than a runtime check that can be forgotten.

    ``AUTHORING_CONTRACT_VERSION`` moves to "2" in the same change — see the
    note there. No compatibility alias for the singular fields: the issue
    records zero users, so an alias would be dead contract surface that still
    has to be served, documented and tested.
    """

    intent_kind: Literal["process_ir"] = "process_ir"
    integration_name: NonEmptyString
    units: Tuple[ProcessAuthoringUnitV1, ...]
    components: Tuple[IntegrationComponentSpec, ...] = ()
    conflict_policy: Literal["reuse", "clone", "fail"] = "reuse"

    @field_validator("units")
    @classmethod
    def _check_units(
        cls, value: Tuple[ProcessAuthoringUnitV1, ...]
    ) -> Tuple[ProcessAuthoringUnitV1, ...]:
        """At least one unit, and no two units claiming the same key.

        Non-emptiness is enforced HERE rather than with ``Field(min_length=1)``,
        and that is a correctness fix rather than a style choice. A field-level
        ``min_length`` is evaluated against the elements that VALIDATED, so a
        request carrying exactly one unit with a malformed root was reported as
        BOTH ``missing`` (right) and ``too_short`` on ``units`` (wrong, and
        actively misleading — it tells a caller who forgot one key to send a
        second unit). A validator does not run at all when an element fails, so
        the caller now sees only the diagnostic that names their actual mistake.

        Duplicate keys are caught here as well as on ``IntegrationSpecV1`` so the
        caller is told against the shape they actually authored — the spec-level
        check reports against a normalized structure they never wrote.
        """
        if not value:
            raise PydanticCustomError(
                "process_component_cardinality_invalid",
                "a process_ir intent must author at least one unit",
            )
        keys = [unit.envelope.component_key for unit in value]
        duplicated = sorted({key for key in keys if keys.count(key) > 1})
        if duplicated:
            raise PydanticCustomError(
                "integration_component_key_duplicate",
                "units declare the same component_key more than once: {keys}",
                {"keys": ", ".join(duplicated)},
            )
        return value


class RecipeInvocationRequestV1(_AuthoringModel):
    """One recipe run, as authored on the MCP surface.

    Mirrors ``boomi_mcp.recipes.engine.RecipeRequestV1`` (a frozen dataclass with
    a ``Mapping[str, Any]`` input) without embedding it: that dataclass is not a
    pydantic model and its ``raw_input`` is deliberately open, so embedding it
    would put an unvalidatable ``Any`` on a strict LLM-facing schema.

    ``raw_input`` stays open here for the same reason it is open there — a
    recipe defines its own input shape. It is NOT unchecked: the engine's input
    gate scans it for credentials, headers, SQL and raw XML before any recipe
    runs, and rejects with ``RECIPE_INPUT_INVALID``. This model routes to that
    gate rather than duplicating it.
    """

    recipe_id: NonEmptyString
    invocation_id: NonEmptyString
    raw_input: Dict[str, Any] = Field(default_factory=dict)
    recipe_version: Optional[NonEmptyString] = None


class RecipeAuthoringIntentV1(_AuthoringModel):
    """Typed recipe contributions entering the workflow (issue #145)."""

    intent_kind: Literal["recipe"] = "recipe"
    integration_name: NonEmptyString
    invocations: Tuple[RecipeInvocationRequestV1, ...] = Field(min_length=1)
    base_components: Tuple[IntegrationComponentSpec, ...] = ()
    conflict_policy: Literal["reuse", "clone", "fail"] = "reuse"


AuthoringIntentV1 = Annotated[
    Union[
        IntegrationSpecAuthoringIntentV1,
        ProcessIRAuthoringIntentV1,
        RecipeAuthoringIntentV1,
    ],
    Field(discriminator="intent_kind"),
]

#: DERIVED from the union, never hand-listed, so the capability manifest's
#: intent-kind axis cannot drift from what the union actually accepts.
#:
#: It was a literal that merely CLAIMED to be derived — a fourth union member
#: could be added and the manifest would go on advertising a three-kind axis with
#: the whole suite green, because every guard compared the literal against
#: something else built from the same literal.
AUTHORING_INTENT_KINDS: Tuple[str, ...] = tuple(
    get_args(member.model_fields["intent_kind"].annotation)[0]
    for member in get_args(get_args(AuthoringIntentV1)[0])
)


# ---------------------------------------------------------------------------
# effect declarations (#154 M12.16)
# ---------------------------------------------------------------------------
#
# WHY THESE ARE SEPARATE TYPES FROM THE COMPILER'S OWN CONTRACTS
#
# ``compiler.process_ir.semantic_validation.contracts`` already defines
# ``MapEffectContractV1``, ``ScriptEffectContractV1``, ``SubprocessSummaryV1`` and
# ``ExternalWriterContractV1``. Those are the TRUSTED context a validation run is
# given: whatever they say about reads, writes and replay safety is taken as
# established fact.
#
# The models below are a caller's CLAIM. They are the same information at a
# different trust level, and #154's whole point is that the two must not be
# confused — a declaration establishes nothing until a server-side authority
# vouches for its CONTENT. Giving them the compiler's names would have made the
# "the caller's object is never forwarded into the compiler" test unable to tell
# the two apart by type, which is precisely the property it exists to check. So
# the public spelling is ``...DeclarationV1`` throughout, and the issue's literal
# names stay with the internal contracts that already own them.
#
# The digest spelling differs too, deliberately: public is the repo-wide
# ``sha256:<hex>`` ``DigestString``, internal is bare hex. The boundary converts
# once, after an equality check, so a mismatch cannot survive as a coincidence of
# formatting.


class ProcessIRStateReferenceV1(_AuthoringModel):
    """One piece of process state an effect touches.

    ``(scope, name)`` mirrors the compiler's internal pair exactly. A richer
    public shape would have to be flattened at the boundary, and a lossy
    conversion in the middle of a trust check is not worth the ergonomics.

    ``processproperty`` is deliberately absent from the scopes: the lineage
    analysis does not track defined Process Properties, so a declaration naming
    one could never be cross-checked and would be an assertion masquerading as a
    scope.
    """

    scope: Literal["ddp", "dpp", "cache"]
    name: NonEmptyString


class ProcessIRStateEffectDeclarationV1(_AuthoringModel):
    """A claimed read/write set plus a claimed replay-safety flag.

    Ordering is not meaningful and duplicates are not meaningful, so both are
    normalised at construction — two declarations that differ only in spelling
    must compare equal to the server's derived effect, or the equality check
    below would fail for a reason that is not about effects at all.
    """

    reads: Tuple[ProcessIRStateReferenceV1, ...] = ()
    writes: Tuple[ProcessIRStateReferenceV1, ...] = ()
    replay_safe: bool = False

    @field_validator("reads", "writes")
    @classmethod
    def _canonical(cls, value):
        return tuple(sorted(set(value), key=lambda ref: (ref.scope, ref.name)))


class ProcessIRMapEffectDeclarationV1(_AuthoringModel):
    """A claim about what a map step does to process state.

    CONTENT AUTHORITY: server-side inspection of the resolved map component. The
    declaration is checked for equality against what inspection derives; it never
    substitutes for it.
    """

    map_ref: NonEmptyString
    effect: ProcessIRStateEffectDeclarationV1


class ProcessIRScriptEffectDeclarationV1(_AuthoringModel):
    """A claim about what a script does to process state.

    CONTENT AUTHORITY: a server-owned vetted-contract registry keyed by
    ``(language, digest)``, where the digest is RECOMPUTED from the resolved
    script source. ``source_sha256`` here is only ever checked for equality with
    that recomputation — a caller-supplied digest is never the thing looked up,
    because then a caller could name any script it liked.

    A declaration whose digest matches but has no registry entry is INERT: the
    server knows WHICH script it is and still has no authority for what it does.
    """

    language: NonEmptyString
    source_sha256: DigestString
    effect: ProcessIRStateEffectDeclarationV1


class ProcessIRSubprocessEffectDeclarationV1(_AuthoringModel):
    """A claim about what a called child process does to process state.

    CONTENT AUTHORITY: server-side inspection of the resolved child ProcessIR. A
    child that is a bare reference — resolvable as a component but with no
    authored root in this request — cannot be inspected, so such a declaration is
    INERT rather than trusted.
    """

    process_ref: NonEmptyString
    effect: ProcessIRStateEffectDeclarationV1


class ProcessIRExternalWriterDeclarationV1(_AuthoringModel):
    """The ONE declaration with no server-side content authority, by nature.

    An outside writer is not in the artifact, so nothing the compiler can read
    could confirm or refute it. It therefore carries no effect payload at all —
    there is nothing here that could be mistaken for evidence.

    What it can do is bounded: combined with an authored ``cache_get`` whose
    ``external_writer`` flag is set, it converts the blocking "no writer for this
    cache" error into the ``EXTERNAL_WRITER_ASSUMED`` warning, so the assumption
    stays named in the served record. It never establishes a cache write.
    """

    cache_ref: NonEmptyString


class ProcessIREffectDeclarationsV1(_AuthoringModel):
    """The optional effect-declaration envelope on an authoring request.

    Absent or empty behaves exactly as before #154: no trusted context is built,
    every map and script stays opaque, and every strict finding stands.
    """

    map_effects: Tuple[ProcessIRMapEffectDeclarationV1, ...] = ()
    script_effects: Tuple[ProcessIRScriptEffectDeclarationV1, ...] = ()
    subprocess_effects: Tuple[ProcessIRSubprocessEffectDeclarationV1, ...] = ()
    external_writers: Tuple[ProcessIRExternalWriterDeclarationV1, ...] = ()

    @model_validator(mode="after")
    def _binding_keys_are_unique(self) -> "ProcessIREffectDeclarationsV1":
        """Two declarations bound to the same thing make the result order-dependent.

        The internal contract model rejects this for the same reason; rejecting it
        here as well means a caller learns about it at the boundary they wrote,
        with a pointer into their own payload.
        """
        for field, key in (
            ("map_effects", lambda item: item.map_ref),
            ("script_effects", lambda item: (item.language, item.source_sha256)),
            ("subprocess_effects", lambda item: item.process_ref),
            ("external_writers", lambda item: item.cache_ref),
        ):
            keys = [key(item) for item in getattr(self, field)]
            if len(set(keys)) != len(keys):
                raise ValueError(
                    "duplicate binding key in {0}".format(field)
                )
        return self

    def is_empty(self) -> bool:
        return not (
            self.map_effects
            or self.script_effects
            or self.subprocess_effects
            or self.external_writers
        )


class AuthoringRequestV1(_AuthoringModel):
    """The one typed request the authoring workflow accepts.

    The three ``expected_*`` fields are the caller's BINDING. They are optional
    on plan and compile (where they are a staleness check) and REQUIRED on a
    typed apply (where they are the only thing standing between a stale plan and
    a mutation).
    """

    contract_version: Literal["2"] = "2"
    intent: AuthoringIntentV1
    topology_spec: Optional[SystemTopologySpecV1] = None
    decisions: Tuple[DecisionResolutionV1, ...] = ()
    expected_capability_revision: Optional[NonEmptyString] = None
    expected_plan_hash: Optional[DigestString] = None
    expected_compile_hash: Optional[DigestString] = None
    #: #154. Optional, and OMITTED normalises to ``None`` rather than to an empty
    #: envelope: an ``effect_declarations`` key present-but-empty in the
    #: normalised payload would rotate every existing plan hash for a request
    #: that declared nothing.
    effect_declarations: Optional[ProcessIREffectDeclarationsV1] = None

    @model_validator(mode="after")
    def _empty_declarations_are_no_declarations(self) -> "AuthoringRequestV1":
        if self.effect_declarations is not None and self.effect_declarations.is_empty():
            object.__setattr__(self, "effect_declarations", None)
        return self


# ---------------------------------------------------------------------------
# result side — every collection is an ordered tuple, present even when empty
# ---------------------------------------------------------------------------


class AuthoringEvidenceV1(_AuthoringModel):
    """One structural evidence pair, RE-VALIDATED at the public boundary (#146).

    Deliberately not the compiler's own evidence type and not a free-form map.
    Forwarding the compiler contract would put an internal model on a served
    schema; a ``Dict[str, Any]`` would put an open map on a strict LLM-facing
    surface, through which an authored value could reach a caller's logs.

    So the pair is re-validated HERE against the same closed key allowlist and
    the same token/code value rules. Re-validating rather than trusting is the
    point: this layer owns what it publishes, and a widened allowlist upstream
    cannot silently widen what crosses the boundary.
    """

    key: NonEmptyString
    value: Union[StrictBool, StrictInt, str]

    @field_validator("key")
    @classmethod
    def _key_is_allowed(cls, value: str) -> str:
        from ..compiler.process_ir.semantic_validation.contracts import (
            ValidationEvidenceV1,
        )

        if value not in ValidationEvidenceV1.allowed_keys():
            raise ValueError("evidence key is not in the closed allowlist")
        return value

    @field_validator("value")
    @classmethod
    def _value_is_safe(cls, value: Any) -> Any:
        # bool before int: a bool IS an int in Python, so checking int first
        # would take the wrong branch for True/False.
        if isinstance(value, bool) or isinstance(value, int):
            return value
        if _SAFE_EVIDENCE_TOKEN.match(value) or _SAFE_EVIDENCE_CODE.match(value):
            return value
        raise ValueError("evidence value is neither a structural token nor a code")

    @property
    def sort_key(self) -> Tuple[str, str]:
        return (self.key, str(self.value))


#: The same shapes the compiler's evidence enforces, restated at this boundary.
#: A structural token is lowercase and bounded; a code is uppercase and bounded.
#: Nothing else crosses — not an id, not a ref, not a label, not a path segment.
_SAFE_EVIDENCE_TOKEN = re.compile(r"^[a-z][a-z0-9_]{0,63}$")
_SAFE_EVIDENCE_CODE = re.compile(r"^[A-Z][A-Z0-9_]{0,95}$")


class AuthoringDiagnosticV1(_AuthoringModel):
    """One authoring-surface finding.

    Value-free by construction: ``message`` and ``remediation`` are written by
    this layer, and ``subject_id`` carries a component key or a decision id —
    never an authored value. When a canonical validator blocks compilation its
    codes are carried verbatim in ``cause_codes`` (ADR-001 §7: the canonical
    taxonomy stays authoritative about its own domain).

    #146 amendment adds three repair fields. ``node_identity`` names the
    authored node the failure sits on, ``evidence`` carries the validator's own
    structural facts, and ``authoring_contract_entry_ids`` cites the served
    rules that explain it — so a diagnostic becomes something a caller can act
    on rather than something they have to interpret.
    """

    code: NonEmptyString
    severity: Literal["error", "warning", "advisory"]
    path: str = ""
    subject_kind: str = ""
    subject_id: str = ""
    message: str = ""
    remediation: str = ""
    cause_codes: Tuple[str, ...] = ()
    node_identity: str = ""
    evidence: Tuple[AuthoringEvidenceV1, ...] = ()
    authoring_contract_entry_ids: Tuple[str, ...] = ()

    @property
    def sort_key(self) -> Tuple[Any, ...]:
        # The new fields PARTICIPATE. The ordered diagnostic list is hashed into
        # the plan/compile hash, so two diagnostics differing only in
        # node_identity, evidence or citations would otherwise order
        # nondeterministically — and a nondeterministic order makes the compile
        # hash unstable, which is the one thing a binding may not be.
        return (
            _SEVERITY_RANK.get(self.severity, 99),
            self.code,
            self.path,
            self.subject_kind,
            self.subject_id,
            self.message,
            self.node_identity,
            tuple(item.sort_key for item in self.evidence),
            self.authoring_contract_entry_ids,
        )


class CapabilityGapV1(_AuthoringModel):
    """Something the caller asked for that this server cannot do."""

    capability_id: NonEmptyString
    state: Literal["unsupported", "gated", "planned"]
    path: str = ""
    reason_code: str = ""
    detail: str = ""

    @property
    def sort_key(self) -> Tuple[str, str]:
        return (self.capability_id, self.path)


class RequiredDecisionV1(_AuthoringModel):
    """A choice the server refuses to make on the caller's behalf."""

    decision_id: NonEmptyString
    path: str = ""
    prompt: str = ""
    options: Tuple[str, ...] = ()
    resolved: bool = False
    resolved_option_id: Optional[str] = None

    @property
    def sort_key(self) -> Tuple[str, str]:
        return (self.decision_id, self.path)


class ResolvedReferenceSummaryV1(_AuthoringModel):
    """What a read-only reference lookup found — sanitized.

    ``version_marker`` is the evidence that makes staleness detectable: if the
    referenced component changed between plan and apply, this token moves and
    the recomputed plan hash moves with it.
    """

    ref: NonEmptyString
    component_type: str = ""
    resolved: bool = False
    component_id: Optional[str] = None
    version_marker: Optional[str] = None

    @property
    def sort_key(self) -> Tuple[str, str]:
        return (self.ref, self.component_type)


# NOTE — this comment is deliberately NOT the docstring of the class below.
#
# A pydantic model's docstring becomes the ``description`` of its published JSON
# Schema, and ``get_schema_template`` serves that schema to an LLM. Naming the
# compiler's internal types here would put them on an authorable surface, which
# ADR-001 §6 forbids and `tests/test_process_ir_compiler_surface.py` enforces —
# it caught exactly that leak when this rationale lived one line lower.
#
# The rationale itself: ``artifact_kind`` is deliberately not "xml". The
# canonical compile step's own deterministic output — already golden-tested — is
# what gets fingerprinted, and it is the quantity a compile-to-apply comparison
# actually compares. Live drift is a different comparison (apply-time vs
# verify-time live component XML) and is handled separately.
class ArtifactFingerprintV1(_AuthoringModel):
    """A deterministic digest of one compiled artifact.

    Carries the digest and the byte length only — never the artifact bytes.
    """

    component_key: NonEmptyString
    component_type: NonEmptyString
    artifact_kind: Literal[
        "process_ir_emission_plan",
        "process_ir_normalized",
        # #153: the RELOCATABLE materialization plan. A third kind rather
        # than a reuse of the two above, because it fingerprints a
        # different quantity: those cover semantics and emission, this
        # covers the deployable plan minus its account-bound fields.
        "process_component_materialization_plan",
    ]
    artifact_version: str = "1"
    byte_length: int = Field(ge=0)
    digest: DigestString

    @property
    def sort_key(self) -> Tuple[str, str, str]:
        return (self.component_key, self.component_type, self.artifact_kind)


class AuthoringRevisionBindingV1(_AuthoringModel):
    """What a result was computed against, and what an apply must reproduce.

    ``account_scope_hash`` is one-way over the ACCOUNT scope. It stops a binding
    minted against one account from satisfying an apply against another, without
    either identifier appearing in the response.

    It is deliberately NOT a per-profile boundary, and it is not an authorization
    token. Two credential profiles addressing one account produce the same scope
    hash, so a compile made under one can be applied under the other. That is a
    staleness/integrity guarantee, not a privilege one: Boomi's own authorization
    still governs what each credential may do, and the apply recompiles under the
    active profile before writing. The published capability states and the
    workflow contract are at get_schema_template(schema_name='authoring_workflow')
    and list_capabilities().
    """

    contract_version: Literal["2"] = "2"
    schema_revision: DigestString
    capability_revision: DigestString
    compiler_revision: DigestString
    account_scope_hash: DigestString
    semantic_hash: Optional[DigestString] = None
    plan_hash: Optional[DigestString] = None
    compile_hash: Optional[DigestString] = None


class TopologyParticipantV1(_AuthoringModel):
    """One role a topology relation binds, and the object it names."""

    role: NonEmptyString
    ref: str = ""

    @property
    def sort_key(self) -> Tuple[str, str]:
        return (self.role, self.ref)


class TopologyRelationSummaryV1(_AuthoringModel):
    """One topology relation, named as a relation and not as a "flow".

    Participants are DERIVED from each relation variant's own fields rather than
    forced into a source/target pair. Topology relations are not uniformly
    binary — ``DeploymentBindingRelationV1`` binds three objects — and the
    variants use role-specific names (``caller_process``, ``callee_process``,
    ``api_service``, …). An earlier version read ``source_key``/``target_key``,
    which no variant defines, so every relation summarized as empty strings.
    """

    relation_kind: NonEmptyString
    relation_key: str = ""
    participants: Tuple[TopologyParticipantV1, ...] = ()

    @property
    def sort_key(self) -> Tuple[str, str]:
        return (self.relation_kind, self.relation_key)


class ComponentDependencyEdgeV1(_AuthoringModel):
    """One ComponentPlan materialization edge."""

    component_key: NonEmptyString
    depends_on: NonEmptyString

    @property
    def sort_key(self) -> Tuple[str, str]:
        return (self.component_key, self.depends_on)


class ProcessCfgSummaryV1(_AuthoringModel):
    """Shape-only summary of one process's semantic CFG.

    Counts and terminal kinds, never node payloads: the CFG is a compiler
    internal (ADR-001 §6) and must not appear on an LLM-facing surface.
    """

    component_key: NonEmptyString
    node_count: int = Field(ge=0)
    edge_count: int = Field(ge=0)
    terminal_kinds: Tuple[str, ...] = ()

    @property
    def sort_key(self) -> Tuple[str, ...]:
        return (self.component_key,)


class ValidationReportSummaryV1(_AuthoringModel):
    """The unified semantic validation result, value-free."""

    is_valid: bool
    error_count: int = Field(ge=0)
    warning_count: int = Field(ge=0)
    advisory_count: int = Field(ge=0)
    codes: Tuple[str, ...] = ()


class _AuthoringResultV1(_AuthoringModel):
    """Fields both read-only phases carry."""

    contract_version: Literal["2"] = "2"
    revision_binding: AuthoringRevisionBindingV1
    #: Typed ``False``, not a convention. A read-only phase that claimed to have
    #: mutated — or was edited into doing so — cannot be constructed.
    mutation_performed: Literal[False] = False
    integration_spec_preview: IntegrationSpecV1
    pipeline_stages: Tuple[str, ...] = ()
    process_cfg: Tuple[ProcessCfgSummaryV1, ...] = ()
    component_dependencies: Tuple[ComponentDependencyEdgeV1, ...] = ()
    topology_relations: Tuple[TopologyRelationSummaryV1, ...] = ()
    resolved_references: Tuple[ResolvedReferenceSummaryV1, ...] = ()
    validation_report: ValidationReportSummaryV1
    errors: Tuple[AuthoringDiagnosticV1, ...] = ()
    warnings: Tuple[AuthoringDiagnosticV1, ...] = ()
    capability_gaps: Tuple[CapabilityGapV1, ...] = ()
    required_decisions: Tuple[RequiredDecisionV1, ...] = ()


class AuthoringPlanResultV1(_AuthoringResultV1):
    """Read-only semantic plan: validation, gaps, decisions, ComponentPlan preview."""

    phase: Literal["plan"] = "plan"


class AuthoringCompileResultV1(_AuthoringResultV1):
    """Read-only canonical compile: normalized intent + artifact fingerprints.

    There is deliberately no ``build_id`` field at all. Compile creates nothing,
    and an optional-but-always-null build id is an invitation to look for one.
    """

    phase: Literal["compile"] = "compile"
    artifact_fingerprints: Tuple[ArtifactFingerprintV1, ...] = ()
    normalized_intent_digest: Optional[DigestString] = None


class ResolvedProcessPlacementV1(_AuthoringModel):
    """Where a materialized process actually landed (issue #153).

    Both fields are optional because account-root placement supplies neither,
    and because the two are recorded from DIFFERENT sources depending on the
    action — see :class:`ProcessMutationAttestationV1`.
    """

    folder_name: Optional[NonEmptyString] = None
    folder_id: Optional[NonEmptyString] = None


class ProcessMutationAttestationV1(_AuthoringModel):
    """What apply actually did to ONE process root, bound to the plan it executed.

    The SECOND of #153's two attestations. The first — the relocatable plan
    fingerprint — certifies what WOULD be built, independent of account. This one
    certifies what WAS built, in a specific account, and the two cannot be
    collapsed:

    * ``action`` never appears in the submitted XML at all. Create and update are
      different API endpoints, so the verb is a property of the CALL, not of the
      bytes.
    * On create, ``result_component_id`` is assigned by the server. It cannot be
      in the bytes that were sent, by definition.
    * The update path's submitted bytes carry the component's CURRENT folder
      attributes, preserved from the live readback — deliberately not the
      requested placement. So the placement that was executed is not derivable
      from the submitted digest either.

    ``submitted_xml_digest`` is PROVENANCE — the bytes this mutation sent. It is
    never compared against a live readback: a readback carries server-assigned
    attributes and would mismatch on every healthy apply. Drift is detected by
    comparing readback to readback, which is what
    :class:`ProcessLiveReadbackAttestationV1` records.
    """

    component_key: NonEmptyString
    plan_fingerprint: DigestString
    account_scope_hash: DigestString
    action: Literal["create", "update"]
    target_component_id: Optional[NonEmptyString] = None
    result_component_id: NonEmptyString
    resolved_placement: ResolvedProcessPlacementV1
    submitted_xml_digest: DigestString

    @model_validator(mode="after")
    def _targeting_matches_the_action(self) -> "ProcessMutationAttestationV1":
        """An update targets something; a create cannot.

        Checked because this is the mutation-accounting record: an attestation
        that says "update" while naming no target, or "create" while naming one,
        describes a mutation that did not happen.
        """
        if self.action == "update":
            if not self.target_component_id:
                raise PydanticCustomError(
                    "process_materialization_plan_invalid",
                    "an update attestation must name the component it targeted",
                )
            if self.target_component_id != self.result_component_id:
                raise PydanticCustomError(
                    "process_materialization_plan_invalid",
                    "an update must resolve to the component it targeted "
                    "(target {target!r} != result {result!r})",
                    {
                        "target": self.target_component_id,
                        "result": self.result_component_id,
                    },
                )
        elif self.target_component_id is not None:
            raise PydanticCustomError(
                "process_materialization_plan_invalid",
                "a create attestation must not name a target component; its "
                "result id is server-assigned",
            )
        return self


class ProcessLiveReadbackAttestationV1(_AuthoringModel):
    """A digest of the component as the PLATFORM reports it, after the mutation.

    ``digest`` is ``None`` when the readback itself failed. That is deliberate
    and load-bearing: the mutation still happened, so the record must exist, and
    an unavailable baseline must read as UNKNOWN rather than as agreement. Verify
    compares this digest against a second readback taken later — readback to
    readback — which is the only comparison in which both sides carry the same
    server-assigned attributes.
    """

    component_key: NonEmptyString
    component_id: NonEmptyString
    digest: Optional[DigestString] = None


class AuthoringBuildProvenanceV1(_AuthoringModel):
    """What a typed apply recorded, and what verify compared it against.

    ``live_comparison`` is optional because apply has nothing to compare yet — it
    is populated by verify. It is DECLARED here rather than added ad hoc to the
    verify response: the model is ``extra="forbid"``, so a field verify returns
    but the schema omits makes the published schema reject the very payload the
    surface emits (issue #146 QA, bug #407).
    """

    contract_version: Literal["2"] = "2"
    revision_binding: AuthoringRevisionBindingV1
    artifact_fingerprints: Tuple[ArtifactFingerprintV1, ...] = ()
    resolved_references: Tuple[ResolvedReferenceSummaryV1, ...] = ()
    live_comparison: Optional["LiveDeploymentComparisonV1"] = None
    #: #153. ORDERED, one per root that was actually mutated. A root whose apply
    #: never ran records nothing — which is what makes a partial multi-root
    #: failure readable: the roots applied before the failure keep their
    #: attestations, and the unapplied ones are absent rather than empty.
    process_mutations: Tuple[ProcessMutationAttestationV1, ...] = ()
    #: #153. Recorded SEPARATELY from the mutation, and per root. A readback
    #: failure leaves `digest=None` here without weakening the mutation record
    #: beside it.
    process_readbacks: Tuple[ProcessLiveReadbackAttestationV1, ...] = ()


class LiveDeploymentComparisonV1(_AuthoringModel):
    """Verify-time comparison of live components against apply-time evidence.

    Revision skew and component drift are reported SEPARATELY: a server upgraded
    since the build is not the same fact as a component someone edited in the
    UI, and collapsing them sends a reader to the wrong remedy.
    """

    status: Literal["not_requested", "match", "drift", "unknown"]
    #: WHICH revisions moved, sorted. A bare "mismatch" tells a caller their
    #: binding is stale without telling them what changed — the capability
    #: surface, or the compiler's behaviour. Those have different remedies.
    revision_mismatches: Tuple[str, ...] = ()
    #: Compared revisions the binding carries NO value for, sorted. A binding
    #: minted before a field existed cannot be compared against it, and
    #: `revision_mismatches` alone cannot express that: a one-field mismatch
    #: list read as "the other one matched" when in truth it was never looked
    #: at. `mismatch` still wins the summary — a definite negative is the
    #: actionable fact — but WHICH fields backed that summary is now stated
    #: rather than inferred.
    revision_uncompared: Tuple[str, ...] = ()
    revision_skew: Literal["not_requested", "match", "mismatch", "unknown"] = (
        "not_requested"
    )
    drifted_components: Tuple[str, ...] = ()
    missing_components: Tuple[str, ...] = ()
    #: Build-owned components with no apply-time baseline to compare against —
    #: their read-back failed when the build was created. Reported separately so
    #: "not compared" can never be mistaken for "unchanged".
    unverifiable_components: Tuple[str, ...] = ()
    diagnostics: Tuple[AuthoringDiagnosticV1, ...] = ()


# ---------------------------------------------------------------------------
# ordering + canonical serialization
# ---------------------------------------------------------------------------


# ``AuthoringBuildProvenanceV1.live_comparison`` forward-references a class
# defined below it, so the model is rebuilt once both exist. Without this the
# published schema would fail to generate at import time.
AuthoringBuildProvenanceV1.model_rebuild()


def sort_authoring_diagnostics(
    diagnostics: Tuple[AuthoringDiagnosticV1, ...],
) -> Tuple[AuthoringDiagnosticV1, ...]:
    """Deterministic order: severity, then code, path, subject, message."""
    return tuple(sorted(diagnostics, key=lambda d: d.sort_key))


def sort_by_key(items):
    """Sort any of the authoring collections by their declared ``sort_key``."""
    return tuple(sorted(items, key=lambda item: item.sort_key))


def canonical_authoring_json(model: BaseModel) -> str:
    """Canonical JSON for hashing: sorted keys, compact, ASCII, no NaN."""
    return json.dumps(model.model_dump(mode="json"), **_CANONICAL_JSON)


# ---------------------------------------------------------------------------
# JSON Schema accessors — generated from the runtime models, never hand-written,
# so `get_schema_template` cannot serve a schema the wrapper does not enforce.
# ---------------------------------------------------------------------------


class AuthoringRequestProcessIRValidationError(Exception):
    """A typed request whose ProcessIR document failed ProcessIR's OWN parser.

    Exists so the public boundary can report a ProcessIR failure the way
    ProcessIR reports it — stable ``PROCESS_IR_*`` code, RFC 6901 pointer into
    the AUTHORED payload, static remediation — instead of the raw pydantic
    ``loc``/``type`` a generic envelope validation produces.

    The difference is not cosmetic. Pydantic says
    ``('intent', 'process_ir', 'body', 'steps', 1, "function-after[...]",
    'legs')`` with a ``ctx`` carrying the offending value; ProcessIR's parser
    says ``/intent/process_ir/body/steps/1/legs`` with
    ``PROCESS_IR_SCHEMA_BRANCH_CARDINALITY`` and a remediation naming the bound.
    Only the second is repairable, and only the second is value-free.
    """

    def __init__(self, diagnostics: Tuple[Dict[str, str], ...]) -> None:
        super().__init__("process_ir validation failed")
        self.diagnostics = tuple(diagnostics)


#: Where a ProcessIR document sits inside a typed request. Pointers from
#: ProcessIR's own parser address the DOCUMENT, so they are prefixed with this to
#: address the request the caller actually sent.
#: RFC 6901 prefix for a ProcessIR diagnostic raised out of a direct intent.
#: #153 moved the roots under ``units``, so the pointer is unit-INDEXED and
#: the index is appended by the caller: ``/intent/units/0/process_ir/...``.
#: The old unindexed ``/intent/process_ir`` prefix is gone rather than kept
#: as a fallback — it names a path that no longer exists in the payload, and
#: a pointer into a non-existent path is worse than no pointer at all.
_UNITS_POINTER_PREFIX = "/intent/units"


def parse_authoring_request_v1(raw_payload: Any) -> "AuthoringRequestV1":
    """Parse a typed authoring request, routing ProcessIR through its own parser.

    ONE entry point for every direct validation site — plan, compile, and the
    typed-apply preflight — so all three report a malformed ProcessIR
    identically. Before this, each site validated the envelope directly and a
    ProcessIR defect surfaced as pydantic internals at whichever site happened
    to catch it.

    The nested document is parsed FIRST. Running the envelope first would report
    the ProcessIR failure as a union-discrimination error on the intent, which
    names the wrong thing entirely.

    Never mutates the caller's mapping: the nested payload is read, not popped.
    """
    from .process_ir import ProcessIRValidationError, parse_process_ir_v1

    if isinstance(raw_payload, Mapping):
        intent = raw_payload.get("intent")
        if isinstance(intent, Mapping) and intent.get("intent_kind") == "process_ir":
            # #153: the roots moved from ONE `process_ir` to `units[i].process_ir`,
            # so the pre-parse walks the units. Every property the singular form
            # had is preserved PER UNIT, and one is added:
            #
            # * PRESENCE, not shape. Gating on `Mapping` meant `process_ir: []`,
            #   a string, a number or `null` skipped the ProcessIR parser
            #   entirely — on plan, compile AND apply — and fell through to raw
            #   pydantic, which answers `model_type` with no PROCESS_IR_* code,
            #   no remediation and no contract citations. Still a presence check
            #   and not an unconditional call: an ABSENT `process_ir` must stay
            #   pydantic's "missing" rather than become a misleading "payload
            #   must be a JSON object".
            # * The pointer is now unit-INDEXED. With several roots in one
            #   request, an unindexed `/intent/process_ir/...` pointer would name
            #   a path that does not exist and would not say WHICH root failed —
            #   the diagnostic would be actively misleading rather than merely
            #   coarse.
            #
            # A non-list `units`, or a non-Mapping entry, is left to pydantic:
            # this pre-parse exists to improve a ProcessIR diagnostic, not to
            # reimplement envelope validation.
            units = intent.get("units")
            if isinstance(units, (list, tuple)):
                for index, unit in enumerate(units):
                    if not isinstance(unit, Mapping) or "process_ir" not in unit:
                        continue
                    authored = unit["process_ir"]
                    try:
                        parse_process_ir_v1(
                            dict(authored)
                            if isinstance(authored, Mapping)
                            else authored
                        )
                    except ProcessIRValidationError as exc:
                        prefix = f"{_UNITS_POINTER_PREFIX}/{index}/process_ir"
                        raise AuthoringRequestProcessIRValidationError(
                            tuple(
                                {
                                    "code": diagnostic.code,
                                    "path": f"{prefix}{diagnostic.path}",
                                    "message": diagnostic.message,
                                    "remediation": diagnostic.remediation,
                                }
                                for diagnostic in exc.diagnostics
                            )
                        ) from None

    return AuthoringRequestV1.model_validate(raw_payload)


def authoring_request_v1_json_schema() -> Dict[str, Any]:
    return AuthoringRequestV1.model_json_schema()


def authoring_plan_result_v1_json_schema() -> Dict[str, Any]:
    return AuthoringPlanResultV1.model_json_schema()


def authoring_compile_result_v1_json_schema() -> Dict[str, Any]:
    return AuthoringCompileResultV1.model_json_schema()


def authoring_revision_binding_v1_json_schema() -> Dict[str, Any]:
    return AuthoringRevisionBindingV1.model_json_schema()


def authoring_build_provenance_v1_json_schema() -> Dict[str, Any]:
    return AuthoringBuildProvenanceV1.model_json_schema()


__all__ = [
    "AuthoringEvidenceV1",
    "AuthoringRequestProcessIRValidationError",
    "parse_authoring_request_v1",
    "AUTHORING_ACTIONS",
    "AUTHORING_CONTRACT_VERSION",
    "AUTHORING_INTENT_KINDS",
    "ArtifactFingerprintV1",
    "AuthoringBuildProvenanceV1",
    "AuthoringCompileResultV1",
    "AuthoringDiagnosticV1",
    "AuthoringIntentV1",
    "AuthoringPlanResultV1",
    "AuthoringRequestV1",
    "AuthoringRevisionBindingV1",
    "CapabilityGapV1",
    "ComponentDependencyEdgeV1",
    "DecisionResolutionV1",
    "DigestString",
    "IntegrationSpecAuthoringIntentV1",
    "LiveDeploymentComparisonV1",
    "ProcessCfgSummaryV1",
    "ProcessIRAuthoringIntentV1",
    "RecipeAuthoringIntentV1",
    "RecipeInvocationRequestV1",
    "RequiredDecisionV1",
    "ResolvedReferenceSummaryV1",
    "TopologyParticipantV1",
    "TopologyRelationSummaryV1",
    "ValidationReportSummaryV1",
    "authoring_build_provenance_v1_json_schema",
    "authoring_compile_result_v1_json_schema",
    "authoring_plan_result_v1_json_schema",
    "authoring_request_v1_json_schema",
    "authoring_revision_binding_v1_json_schema",
    "canonical_authoring_json",
    "sort_authoring_diagnostics",
    "sort_by_key",
]
