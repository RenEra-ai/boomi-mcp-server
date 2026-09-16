"""Semantic-validation contracts: report, diagnostic, safe evidence (issue #143).

DARK in slice 1. Nothing imports this module yet; the compiler, adapters,
builders and MCP tools are untouched. Later slices attach collectors to it.

Why a second contract layer instead of reusing ``diagnostics.CompilerDiagnostic``
----------------------------------------------------------------------------
``CompilerDiagnostic`` answers "the compile failed, here is why", and its
consumer is an exception. A ``ValidationReportV1`` answers "here is everything
wrong with this payload, ranked", and its consumer is a caller deciding whether
to mutate. Three differences make them separate types rather than one widened
one:

* **Severity.** A compiler diagnostic is always fatal. A validation finding may
  be a warning or an advisory that deliberately does NOT block.
* **Accumulation.** Compilation raises on first failure at each stage; semantic
  validation must report the whole set, so a caller fixes everything in one pass
  instead of discovering defects one round-trip at a time.
* **Ownership.** Per ADR-001 §7 this issue introduces codes only in
  ``PROCESS_IR_REFERENCE_*``, ``PROCESS_IR_CAPABILITY_*``,
  ``PROCESS_IR_SEMANTIC_*`` and ``LEGACY_ADAPTER_EXEMPTION_*``. The
  ``PROCESS_IR_COMPILE_*`` family stays the compiler's, and a compile-family
  code must never appear in a ``ValidationReportV1`` — an unexpected internal
  defect escapes to the compiler's own ``_guarded`` boundary instead.

Fatal findings convert losslessly into ``CompilerDiagnostic`` values in a later
slice, so the existing ``ProcessIRCompileError`` contract is unchanged.

Security — where the redaction guarantee actually comes from
------------------------------------------------------------
Two independent controls, and it is worth being precise about which does the
work, because overstating the weaker one is how a leak ships:

1. **The closed evidence-key allowlist is the primary control.** Keys are chosen
   by code, never by a caller, and no key is defined that carries a name, id,
   ref, label, or free text. A property name such as ``dpp_customer_email`` is
   lexically indistinguishable from a structural token, so no value rule could
   reject it — the reason it cannot appear is that there is no key to put it
   under.
2. **The value-shape rule is defense in depth.** It rejects the shapes that
   *are* distinguishable: component ids (dashes), ``$ref:`` tokens, labels
   (spaces/mixed case), script and exception text (newlines), and anything over
   a short bound.

``message`` and ``remediation`` are static strings selected by code — never
interpolated — and ``__repr_args__`` suppresses every non-structural field, so
neither a log line nor a traceback can carry authored text.
"""

from __future__ import annotations

import hashlib
import json
import re
from typing import Any, Dict, FrozenSet, Iterable, List, Literal, Optional, Tuple, Union

from pydantic import BaseModel, ConfigDict, Field, computed_field, field_validator, model_validator

# --------------------------------------------------------------------------
# vocabularies
# --------------------------------------------------------------------------

ValidationSeverityV1 = Literal["error", "warning", "advisory"]

#: Only ``error`` blocks. Warnings and advisories are reported and ignored by
#: the gate, which is what lets a tightened rule ship before it is enforced.
VALIDATION_SEVERITY_ORDER: Tuple[str, ...] = ("error", "warning", "advisory")

ValidationPhaseV1 = Literal[
    "model",
    "capability",
    "reference",
    "terminal",
    "reachability",
    "profile",
    "cardinality",
    "lineage",
    "side_effect",
    "retry",
    "compatibility",
]

#: Normative phase order. Rank, not name, drives sorting — so the earliest
#: failure in the pipeline reads first regardless of alphabet. Reordering this
#: tuple changes report ordering, which is a contract change.
VALIDATION_PHASE_ORDER: Tuple[str, ...] = (
    "model",
    "capability",
    "reference",
    "terminal",
    "reachability",
    "profile",
    "cardinality",
    "lineage",
    "side_effect",
    "retry",
    "compatibility",
)

_PHASE_RANK: Dict[str, int] = {
    phase: index for index, phase in enumerate(VALIDATION_PHASE_ORDER)
}
_SEVERITY_RANK: Dict[str, int] = {
    severity: index for index, severity in enumerate(VALIDATION_SEVERITY_ORDER)
}

# --------------------------------------------------------------------------
# safe evidence
# --------------------------------------------------------------------------

#: The CLOSED set of evidence keys. This is the primary redaction control, so
#: every addition is a deliberate widening of what a diagnostic may carry.
#: A key that would hold a name, id, ref, label or free text does not belong
#: here and is rejected by an explicit test.
_EVIDENCE_KEYS: FrozenSet[str] = frozenset(
    {
        # structural position
        "leg_ordinal",
        "arm",
        "depth",
        "node_count",
        "step_index",
        # closed classifications
        "effect_kind",
        "state_scope",
        "component_type_class",
        "connector_action",
        "terminal_role",
        "cardinality",
        # counts and flags
        "retry_count",
        "external_writer",
        "wait",
        "producer_present",
        "writer_count",
        "reader_count",
        # cross-references between findings, by CODE only
        "suppressed_by",
        "related_code",
        "exemption",
    }
)

#: A structural token: lowercase, bounded, no separators that appear in ids,
#: refs, labels or paths.
_SAFE_TOKEN = re.compile(r"^[a-z][a-z0-9_]{0,63}$")

#: A diagnostic code: uppercase, bounded. Codes are the one uppercase string a
#: diagnostic legitimately carries.
_SAFE_CODE = re.compile(r"^[A-Z][A-Z0-9_]{0,95}$")


class _ValidationModel(BaseModel):
    """Frozen, strict base for every semantic-validation contract.

    The repr allowlist is deliberately this package's OWN, not an import of
    ``contracts._REPR_SAFE_FIELDS``. Importing and widening that frozenset would
    loosen redaction for the existing CFG and emission-plan models too — a
    silent blast radius well outside this issue.
    """

    model_config = ConfigDict(extra="forbid", frozen=True)

    def __repr_args__(self) -> Any:  # noqa: D105 — pydantic hook
        for key, value in super().__repr_args__():
            if key in _REPR_SAFE_FIELDS:
                yield key, value
            else:
                yield key, "..."


#: Structural discriminators only. ``message``/``remediation``/``evidence`` are
#: absent on purpose: they render as "..." so a traceback cannot carry them.
_REPR_SAFE_FIELDS: FrozenSet[str] = frozenset(
    {"version", "code", "severity", "phase", "key"}
)


class ValidationEvidenceV1(_ValidationModel):
    """One closed key/value fact attached to a diagnostic.

    ``value`` admits a bool, an int, or a string that is either a lowercase
    structural token or an uppercase diagnostic code. Everything else is
    rejected — see the module docstring for which of the two controls is doing
    the work.
    """

    key: str
    value: Union[bool, int, str]

    @field_validator("key")
    @classmethod
    def _key_is_allowlisted(cls, value: str) -> str:
        if value not in _EVIDENCE_KEYS:
            raise ValueError("evidence key is not in the closed allowlist")
        return value

    @field_validator("value")
    @classmethod
    def _value_is_safe(cls, value: Union[bool, int, str]) -> Union[bool, int, str]:
        # bool before int: bool IS an int in Python, and checking int first
        # would let a bool through the wrong branch.
        if isinstance(value, bool) or isinstance(value, int):
            return value
        if _SAFE_TOKEN.match(value) or _SAFE_CODE.match(value):
            return value
        raise ValueError("evidence value is neither a structural token nor a code")

    @staticmethod
    def allowed_keys() -> FrozenSet[str]:
        """The closed key set, exposed so a test can assert what it contains."""
        return _EVIDENCE_KEYS

    def sort_key(self) -> Tuple[str, str]:
        return (self.key, str(self.value))


class ValidationDiagnosticV1(_ValidationModel):
    """One semantic finding: stable code, severity, authored position, evidence."""

    code: str
    severity: ValidationSeverityV1

    @field_validator("code")
    @classmethod
    def _never_a_compile_family_code(cls, value: str) -> str:
        """A report may not carry a code that blames the COMPILER.

        ADR-001 §7 gives ``PROCESS_IR_COMPILE_*`` to the compiler: it means "this
        is our bug, not yours". A validation report means the opposite — "your
        payload is wrong" — so a compile-family code here would tell a caller to
        fix correct input, which is exactly how someone ends up rewriting a
        working payload to route around a compiler defect.

        Enforced structurally rather than by convention because the docs already
        claimed it and nothing checked: an unexpected internal defect is meant to
        escape to the compiler's own ``_guarded`` boundary, and ``flow.py``
        re-raises rather than translating such a diagnostic. This makes that
        contract impossible to violate by accident instead of merely unlikely.
        """
        if value.startswith("PROCESS_IR_COMPILE_"):
            raise ValueError(
                "a validation report cannot carry a compile-family code"
            )
        return value
    phase: ValidationPhaseV1
    path: str
    node_identity: str
    message: str
    remediation: str
    evidence: Tuple[ValidationEvidenceV1, ...] = ()
    internal_node_id: Optional[str] = None

    @field_validator("evidence")
    @classmethod
    def _evidence_is_canonically_ordered(
        cls, value: Tuple[ValidationEvidenceV1, ...]
    ) -> Tuple[ValidationEvidenceV1, ...]:
        # Canonicalize on the way in so two findings that differ only in the
        # order their evidence was appended dedup against each other.
        return tuple(sorted(value, key=lambda item: item.sort_key()))

    def dedup_key(self) -> Tuple[str, str, Tuple[Tuple[str, str], ...]]:
        """Identity for deduplication: code, authored path, canonical evidence.

        Severity is deliberately NOT part of this key — buckets are deduplicated
        independently, so the same code at the same path may legitimately appear
        once as an error and once as a warning from different phases.
        """
        return (self.code, self.path, tuple(e.sort_key() for e in self.evidence))

    def sort_key(self) -> Tuple[int, str, str, str, Tuple[Tuple[str, str], ...]]:
        """Total order: phase rank, path, node identity, code, evidence.

        Every component is needed for a TOTAL order. Stopping at ``code`` would
        leave two findings that differ only in evidence in arbitrary relative
        order, which is stable within a process and unstable across runs — the
        exact defect the determinism criterion exists to prevent.
        """
        return (
            _PHASE_RANK.get(self.phase, len(_PHASE_RANK)),
            self.path,
            self.node_identity,
            self.code,
            tuple(e.sort_key() for e in self.evidence),
        )


class ValidationReportV1(_ValidationModel):
    """Immutable, deterministically ordered validation result."""

    version: Literal["1"] = "1"
    errors: Tuple[ValidationDiagnosticV1, ...] = ()
    warnings: Tuple[ValidationDiagnosticV1, ...] = ()
    advisories: Tuple[ValidationDiagnosticV1, ...] = ()

    @computed_field  # type: ignore[prop-decorator]
    @property
    def is_valid(self) -> bool:
        """True when nothing blocks. Only errors block; warnings never do."""
        return not self.errors


# --------------------------------------------------------------------------
# typed capability / effect contracts
#
# These are COMPILER CONTEXT, never authored IR fields (ADR-001 §6). There is no
# ``trusted=True``, no policy id, and no free-form assertion: presence in this
# typed set IS the verification boundary. That is the whole reason the issue
# forbids a "trust me" flag — a flag can be set by whoever wrote the payload,
# whereas this set is assembled by the caller that already resolved the
# components.
# --------------------------------------------------------------------------


#: The closed set of state scopes a contract may name. Kept here rather than in
#: ``lineage`` because it is a CONSTRUCTION-time constraint: ``StateEffectV1``
#: is caller-supplied data, and an unconstrained scope element used to reach two
#: places it must never reach. It became evidence — where the closed-vocabulary
#: check raised a raw ``pydantic.ValidationError`` straight out of
#: ``validate_process_ir``, whose contract promises to raise only on a COMPILER
#: defect — and it became a lattice key, where any unrecognised scope silently
#: landed in the execution-scoped set and was treated as established.
#: Rejecting it at construction closes both at once.
STATE_SCOPES: FrozenSet[str] = frozenset({"ddp", "dpp", "cache"})


class StateEffectV1(_ValidationModel):
    """Exact state a trusted effect reads and writes.

    Names ride here because this is INPUT, not output. They are matched against
    the IR and then discarded; no name reaches a diagnostic. The SCOPE half of
    each pair does reach one, so it is constrained to ``STATE_SCOPES``.
    """

    reads: Tuple[Tuple[str, str], ...] = ()
    writes: Tuple[Tuple[str, str], ...] = ()
    replay_safe: bool = False

    @field_validator("reads", "writes")
    @classmethod
    def _scopes_are_known(cls, value):
        """Reject a scope outside the closed vocabulary.

        Applied to ``writes`` as well as ``reads``, though only ``reads``
        currently reaches a diagnostic: a write with an unknown scope is
        recorded into the lattice under a key nothing can ever match, so it
        silently vouches for state it does not establish. Both halves of the
        contract mean the same thing, so both are held to it.
        """
        for pair in value:
            if pair[0] not in STATE_SCOPES:
                raise ValueError(
                    "unknown state scope {0!r}; expected one of {1}".format(
                        pair[0], sorted(STATE_SCOPES)
                    )
                )
        return value


class MapEffectContractV1(_ValidationModel):
    """Effects of one map, bound to the MAP COMPONENT it describes."""

    map_ref: str
    effect: StateEffectV1


class ScriptEffectContractV1(_ValidationModel):
    """Effects of one script, bound to language + digest of its exact source.

    Binding to the digest rather than to a node position is what makes the
    contract non-transferable: editing the script invalidates it automatically
    instead of silently continuing to vouch for code that no longer exists.
    """

    language: str
    source_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    effect: StateEffectV1


class ExternalWriterContractV1(_ValidationModel):
    """Typed proof that something OUTSIDE this process writes a cache.

    The authored `cache_get.external_writer` boolean used to do this on its own,
    turning a blocking `…LINEAGE_CACHE_WRITER_MISSING` into a non-blocking
    warning. That is a free-form "trust me" flag suppressing a fatal safety
    rule, which #143 excludes by name: a payload cannot assert its own
    trustworthiness (§7), and capabilities are compiler context that no authored
    document can reach.

    The flag remains meaningful as a DECLARATION of intent — it is what says
    which cache reads expect an outside writer — but it now needs this contract
    to confirm it. Legacy dialects keep working through
    `LEGACY_ADAPTER_EXEMPTION_STANDALONE_CACHE_READ`, which already covers the
    same code, so compatibility comes from the named registry-owned policy
    rather than from a caller-supplied boolean.
    """

    cache_ref: str


class SubprocessSummaryV1(_ValidationModel):
    """Declared effects of a child process, bound to its ``process_ref``."""

    process_ref: str
    effect: StateEffectV1


class ChildEntryContractV1(_ValidationModel):
    """What ONE called child requires of every invocation (#184 amendment 3 §8).

    Derived by the server for every inspectable child in the request, whether or not
    anything was declared, and discharged by each call on its own: facts from two
    parents are never combined. Separate from ``SubprocessSummaryV1``, which is a
    caller's declared effect the server verifies and the only way a child's writes
    establish anything downstream.

    - ``entry_form``: ``passthrough`` receives the arriving documents as one group;
      ``scheduled`` (No Data) receives one empty document per arriving document;
      ``unknown`` when the child's entry cannot be derived (a listener, a cycle).
    - ``document_requirements``: per consumer of a passthrough child's incoming
      documents, the AUTHORED profile ref it requires, or None where nothing states
      one. None also stands for a consumer the child hands the documents on to and
      nothing states: a called process whose entry cannot be derived, or a waited
      passthrough child's own None. Refs, not resolved ids, so the contract survives
      placeholder-to-real-id conversion; each call resolves them against its own symbols.
    - ``required_reads``: state keys the child reads before establishing them. A No
      Data child's document properties are not among them: they cannot arrive.
    - ``required_writers``: document properties a passthrough child's bound request
      path composes from the caller's documents, with the request profile ref the
      binding names. The caller's writer must pass the binding checks at the call.
    - ``cache_requirements``: per consumer of documents the child reads from a cache
      no write in the child reached, ``(cache ref, AUTHORED profile ref or None)``. A
      caller's writes must have stored exactly that profile (amendment 1 rule 6).
    - ``cache_property_requirements``: per document property the child uses on
      documents it retrieved from such a cache, ``(cache ref, property name, request
      profile ref or None, bound)``. Every document a caller's writes stored there must
      carry it (amendment 3 §7), and a bound request path additionally needs each of
      their writers to pass the binding checks at the call.
    - ``mutated_state``: execution-scoped keys the child may write or remove.
    - ``guaranteed_state``: execution-scoped keys the child establishes on every normal
      completion, a subset of ``mutated_state``. A waited call with ``abort_on_error``
      that provably runs the child establishes them for later paths (amendment 1 rule
      7). No document property is among them: a child's document properties never
      land on its caller's sibling copies.
    - ``removed_caches``: the document caches the child may REMOVE outright, its own
      terminal removes and every removal it inherits from a child it calls — every removal
      the walk PROVES, whether or not the child's cache writes are all known. Not a subset
      of ``mutated_state``, which lists writes and removals alike without telling them
      apart and is stated only when the cache writes are all known: that list is a
      completeness claim and this one an existence claim, and a call nothing derives hides
      removals without erasing the ones authored beside it. A removal invalidates an
      execution-cache guarantee even though it emits no documents (amendment 3 §8), so a
      call un-establishes each of these before applying what the child guarantees;
      unknowable cache writes are answered on the write side instead, as an unknown
      possibility that leaves the establishment alone.
    - ``state_known``: False when some step's state effects are unknown; then
      ``mutated_state`` is incomplete and proves nothing about process properties.
    - ``cache_writes_known``: True when ``mutated_state`` lists every document cache the
      child may write or remove. Only an Add to Cache or Remove from Cache step writes a
      cache (no map-function or vetted-script effect row writes one), so a map or script
      with unknown effects leaves it True. A call to a child whose own cache writes are
      unknown makes it False. ``state_known`` implies it.
    """

    process_ref: str
    entry_form: Literal["passthrough", "scheduled", "unknown"]
    document_requirements: Tuple[Optional[str], ...] = ()
    required_reads: Tuple[Tuple[str, str], ...] = ()
    required_writers: Tuple[Tuple[str, Optional[str]], ...] = ()
    cache_requirements: Tuple[Tuple[str, Optional[str]], ...] = ()
    cache_property_requirements: Tuple[Tuple[str, str, Optional[str], bool], ...] = ()
    mutated_state: Tuple[Tuple[str, str], ...] = ()
    guaranteed_state: Tuple[Tuple[str, str], ...] = ()
    removed_caches: Tuple[str, ...] = ()
    state_known: bool = False
    cache_writes_known: bool = False

    @model_validator(mode="after")
    def _guarantees_are_execution_state_it_writes(self):
        """A guarantee is an execution-scoped write the child makes (amendment 1 rule 7).

        A document property is refused: neither form establishes one on the caller's
        sibling copies. A key the child is not recorded as writing is refused too, so a
        guarantee can never promise state the possible-effect side does not list.
        """
        if any(key[0] == "ddp" for key in self.guaranteed_state):
            raise ValueError("a child guarantees no document property to its caller")
        written = {(key[0], key[1]) for key in self.mutated_state}
        if any((key[0], key[1]) not in written for key in self.guaranteed_state):
            raise ValueError("a guaranteed key must be one the child may write")
        return self


class ProcessIRValidationCapabilitiesV1(_ValidationModel):
    """The trusted context a validation run is given.

    Empty by default, and an empty set is the STRICT case: with no contracts,
    every map and script is opaque and therefore establishes nothing.
    """

    map_effects: Tuple[MapEffectContractV1, ...] = ()
    script_effects: Tuple[ScriptEffectContractV1, ...] = ()
    subprocess_summaries: Tuple[SubprocessSummaryV1, ...] = ()
    external_writers: Tuple[ExternalWriterContractV1, ...] = ()
    #: State a CALLER guarantees before this process runs — its preconditions.
    #:
    #: Empty by default, which is the strict case: a top-level process starts
    #: from nothing. It is non-empty only for a root that some other root in the
    #: same request CALLS, and only for the keys the server itself derived as
    #: that child's required reads.
    #:
    #: Without it the required-reads channel was unusable end to end. Every root
    #: is validated independently from empty state, so a child that genuinely
    #: depends on caller-supplied state reported read-before-write against
    #: ITSELF — and the whole point of deriving "required reads" is that those
    #: reads are the caller's obligation, checked at the call site, where the
    #: parent's own walk already enforces a trusted contract's declared reads.
    #: Seeding them is therefore not a relaxation: the same keys are demanded of
    #: the parent, and nothing supplies them but the server's own derivation
    #: from this very child.
    established_at_entry: Tuple[Tuple[str, str], ...] = ()
    #: #184 amendment 3 §8: the entry contract of each child this process calls that
    #: the server could derive, one row per authored spelling.
    child_entry_contracts: Tuple[ChildEntryContractV1, ...] = ()
    #: #184 amendment 3 §8: document properties a CALLED passthrough child's bound
    #: request paths compose from its caller's documents. The child is validated as
    #: though a writer composed each one, because every call proves that writer
    #: against the child's binding; a bare established key never does.
    caller_supplied_writers: Tuple[Tuple[str, str], ...] = ()
    #: #184 amendment 1 rule 6: for a CALLED child, the profile each cache it reads
    #: first holds, as ``(cache ref, profile ref)``. Seeded as that cache's content only
    #: when every consumer names one profile; every call proves it against its writes.
    caller_cache_contents: Tuple[Tuple[str, str], ...] = ()
    #: #184 amendment 3 §7-§8: for a CALLED child, the document properties the documents
    #: its callers stored in a cache it reads first carry, as ``(cache ref, property
    #: name)``. Seeded as one cohort per cache whose writer is the caller; every call
    #: proves it against each cohort its own writes left there.
    caller_cache_cohorts: Tuple[Tuple[str, str], ...] = ()
    #: #184 amendment 3 §8: the contract THIS process presents to its callers, derived
    #: for a passthrough root. Recorded with the build so a direct test run or a
    #: schedule, which starts the process as No Data, is refused before any mutation.
    entry_contract: Optional[ChildEntryContractV1] = None

    def writes_cache_externally(self, cache_ref: str) -> bool:
        """Whether a typed contract vouches for an outside writer of this cache."""
        return any(item.cache_ref == cache_ref for item in self.external_writers)

    @field_validator(
        "map_effects", "script_effects", "subprocess_summaries", "external_writers",
        "child_entry_contracts",
    )
    @classmethod
    def _binding_keys_are_unique(cls, value):
        """Reject duplicate binding keys at construction.

        The lookups below take the FIRST match, so two contracts bound to the
        same map/script/process would make the result depend on tuple order —
        reordering identical inputs could change whether state is established
        and therefore change the report. That is exactly the non-determinism
        the report contract forbids, so it is rejected here rather than
        resolved by position.
        """
        keys = []
        for item in value:
            if hasattr(item, "map_ref"):
                keys.append(item.map_ref)
            elif hasattr(item, "cache_ref"):
                keys.append(item.cache_ref)
            elif hasattr(item, "process_ref"):
                keys.append(item.process_ref)
            else:
                keys.append((item.language, item.source_sha256))
        if len(set(keys)) != len(keys):
            raise ValueError("duplicate effect-contract binding key")
        return value

    def map_effect(self, map_ref: str) -> Optional[StateEffectV1]:
        for item in self.map_effects:
            if item.map_ref == map_ref:
                return item.effect
        return None

    def script_effect(self, language: str, source: str) -> Optional[StateEffectV1]:
        digest = hashlib.sha256(source.encode("utf-8")).hexdigest()
        for item in self.script_effects:
            if item.language == language and item.source_sha256 == digest:
                return item.effect
        return None

    def subprocess_effect(self, process_ref: str) -> Optional[StateEffectV1]:
        for item in self.subprocess_summaries:
            if item.process_ref == process_ref:
                return item.effect
        return None

    def child_entry_contract(self, process_ref: str) -> Optional["ChildEntryContractV1"]:
        for item in self.child_entry_contracts:
            if item.process_ref == process_ref:
                return item
        return None


#: The shipped default: no trusted contracts, so everything opaque is opaque.
DEFAULT_VALIDATION_CAPABILITIES = ProcessIRValidationCapabilitiesV1()


# --------------------------------------------------------------------------
# assembly helpers
# --------------------------------------------------------------------------


def _bucket(
    diagnostics: Iterable[ValidationDiagnosticV1], severity: str
) -> Tuple[ValidationDiagnosticV1, ...]:
    seen: Dict[Tuple[str, str, Tuple[Tuple[str, str], ...]], None] = {}
    kept: List[ValidationDiagnosticV1] = []
    for item in sorted(
        (d for d in diagnostics if d.severity == severity),
        key=lambda d: d.sort_key(),
    ):
        key = item.dedup_key()
        if key in seen:
            continue
        seen[key] = None
        kept.append(item)
    return tuple(kept)


def build_validation_report(
    diagnostics: Iterable[ValidationDiagnosticV1],
) -> ValidationReportV1:
    """Bucket by severity, sort each bucket totally, drop exact duplicates.

    Sorting happens BEFORE deduplication so which of a duplicate pair survives
    is itself deterministic, rather than depending on collection order.
    """
    collected = tuple(diagnostics)
    return ValidationReportV1(
        errors=_bucket(collected, "error"),
        warnings=_bucket(collected, "warning"),
        advisories=_bucket(collected, "advisory"),
    )


def canonical_report_json(report: ValidationReportV1) -> str:
    """Canonical serialization, matching the #136/#137 recipe.

    ``sort_keys=True`` orders object KEYS only; tuple order — which is the
    report's meaning — is preserved.
    """
    return json.dumps(
        report.model_dump(mode="json"),
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
    )


__all__: List[str] = [
    "DEFAULT_VALIDATION_CAPABILITIES",
    "MapEffectContractV1",
    "ProcessIRValidationCapabilitiesV1",
    "STATE_SCOPES",
    "ExternalWriterContractV1",
    "ScriptEffectContractV1",
    "StateEffectV1",
    "SubprocessSummaryV1",
    "VALIDATION_PHASE_ORDER",
    "VALIDATION_SEVERITY_ORDER",
    "ValidationDiagnosticV1",
    "ValidationEvidenceV1",
    "ValidationPhaseV1",
    "ValidationReportV1",
    "ValidationSeverityV1",
    "build_validation_report",
    "canonical_report_json",
]
