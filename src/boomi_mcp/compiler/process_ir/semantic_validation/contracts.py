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

from pydantic import BaseModel, ConfigDict, Field, computed_field, field_validator

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


class StateEffectV1(_ValidationModel):
    """Exact state a trusted effect reads and writes.

    Names ride here because this is INPUT, not output. They are matched against
    the IR and then discarded; no name reaches a diagnostic.
    """

    reads: Tuple[Tuple[str, str], ...] = ()
    writes: Tuple[Tuple[str, str], ...] = ()
    replay_safe: bool = False


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


class SubprocessSummaryV1(_ValidationModel):
    """Declared effects of a child process, bound to its ``process_ref``."""

    process_ref: str
    effect: StateEffectV1


class ProcessIRValidationCapabilitiesV1(_ValidationModel):
    """The trusted context a validation run is given.

    Empty by default, and an empty set is the STRICT case: with no contracts,
    every map and script is opaque and therefore establishes nothing.
    """

    map_effects: Tuple[MapEffectContractV1, ...] = ()
    script_effects: Tuple[ScriptEffectContractV1, ...] = ()
    subprocess_summaries: Tuple[SubprocessSummaryV1, ...] = ()

    @field_validator("map_effects", "script_effects", "subprocess_summaries")
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
