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
from typing import Annotated, Any, Dict, Literal, Optional, Tuple, Union

from pydantic import BaseModel, ConfigDict, Field, StringConstraints

from .integration_models import IntegrationComponentSpec, IntegrationSpecV1
from .process_ir import ProcessIRV1
from .system_topology import SystemTopologySpecV1

#: The authoring CONTRACT version. Distinct from every schema's own version: this
#: names the shape of the request/result envelope, not of ProcessIR or topology.
AUTHORING_CONTRACT_VERSION = "1"

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
    """

    intent_kind: Literal["process_ir"] = "process_ir"
    integration_name: NonEmptyString
    component_key: NonEmptyString
    process_ir: ProcessIRV1
    components: Tuple[IntegrationComponentSpec, ...] = ()
    conflict_policy: Literal["reuse", "clone", "fail"] = "reuse"


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
AUTHORING_INTENT_KINDS: Tuple[str, ...] = (
    "integration_spec",
    "process_ir",
    "recipe",
)


class AuthoringRequestV1(_AuthoringModel):
    """The one typed request the authoring workflow accepts.

    The three ``expected_*`` fields are the caller's BINDING. They are optional
    on plan and compile (where they are a staleness check) and REQUIRED on a
    typed apply (where they are the only thing standing between a stale plan and
    a mutation).
    """

    contract_version: Literal["1"] = "1"
    intent: AuthoringIntentV1
    topology_spec: Optional[SystemTopologySpecV1] = None
    decisions: Tuple[DecisionResolutionV1, ...] = ()
    expected_capability_revision: Optional[NonEmptyString] = None
    expected_plan_hash: Optional[DigestString] = None
    expected_compile_hash: Optional[DigestString] = None


# ---------------------------------------------------------------------------
# result side — every collection is an ordered tuple, present even when empty
# ---------------------------------------------------------------------------


class AuthoringDiagnosticV1(_AuthoringModel):
    """One authoring-surface finding.

    Value-free by construction: ``message`` and ``remediation`` are written by
    this layer, and ``subject_id`` carries a component key or a decision id —
    never an authored value. When a canonical validator blocks compilation its
    codes are carried verbatim in ``cause_codes`` (ADR-001 §7: the canonical
    taxonomy stays authoritative about its own domain).
    """

    code: NonEmptyString
    severity: Literal["error", "warning", "advisory"]
    path: str = ""
    subject_kind: str = ""
    subject_id: str = ""
    message: str = ""
    remediation: str = ""
    cause_codes: Tuple[str, ...] = ()

    @property
    def sort_key(self) -> Tuple[Any, ...]:
        return (
            _SEVERITY_RANK.get(self.severity, 99),
            self.code,
            self.path,
            self.subject_kind,
            self.subject_id,
            self.message,
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
    artifact_kind: Literal["process_ir_emission_plan", "process_ir_normalized"]
    artifact_version: str = "1"
    byte_length: int = Field(ge=0)
    digest: DigestString

    @property
    def sort_key(self) -> Tuple[str, str, str]:
        return (self.component_key, self.component_type, self.artifact_kind)


class AuthoringRevisionBindingV1(_AuthoringModel):
    """What a result was computed against, and what an apply must reproduce.

    ``account_scope_hash`` is one-way over the profile and account scope. It
    exists so a binding minted under one credential profile cannot be replayed
    against another — without either identifier appearing in the response.
    """

    contract_version: Literal["1"] = "1"
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

    contract_version: Literal["1"] = "1"
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


class AuthoringBuildProvenanceV1(_AuthoringModel):
    """What a typed apply recorded, and what verify compared it against.

    ``live_comparison`` is optional because apply has nothing to compare yet — it
    is populated by verify. It is DECLARED here rather than added ad hoc to the
    verify response: the model is ``extra="forbid"``, so a field verify returns
    but the schema omits makes the published schema reject the very payload the
    surface emits (issue #146 QA, bug #407).
    """

    contract_version: Literal["1"] = "1"
    revision_binding: AuthoringRevisionBindingV1
    artifact_fingerprints: Tuple[ArtifactFingerprintV1, ...] = ()
    resolved_references: Tuple[ResolvedReferenceSummaryV1, ...] = ()
    live_comparison: Optional["LiveDeploymentComparisonV1"] = None


class LiveDeploymentComparisonV1(_AuthoringModel):
    """Verify-time comparison of live components against apply-time evidence.

    Revision skew and component drift are reported SEPARATELY: a server upgraded
    since the build is not the same fact as a component someone edited in the
    UI, and collapsing them sends a reader to the wrong remedy.
    """

    status: Literal["not_requested", "match", "drift", "unknown"]
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
