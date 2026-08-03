"""Recipe descriptor, provenance, and registration contracts (issue #145 M12.10).

A RECIPE is registered code with a strict input model and a declared set of typed
contribution outputs. This module defines what "registered" means; ``registry.py``
enforces it, ``engine.py`` runs it.

Four entry kinds, and the difference between them is not cosmetic:

* ``executable_recipe``     — has an executor; may emit any declared contribution
* ``constraint_only``       — has an executor; may emit ONLY ConstraintRequirement
* ``advisory``              — has NO executor, NO input schema, NO outputs
* ``compatibility_adapter`` — has NO executor; names the exact recipe it adapts to

``advisory`` is the entry kind that makes "doctrine cannot become executable"
enforceable rather than conventional. Doctrine text may point AT a recipe by
exact reference, and that reference is all a caller gets.

To be precise about the mechanism: ``RecipeRegistrationV1`` is one dataclass for
all four kinds, so an ``executor`` FIELD does exist — registry construction is
what rejects an advisory entry that populates it, and it does so with a
``ValueError`` before the registry is usable. An earlier version of this note
said there was no field at all, which was false (issue #145, live QA). What holds
regardless is the thing that matters: **prose is never parsed**, so there is no
parser to trick, and an advisory descriptor that reached a caller could not be
executed even if one tried.

**Executors receive their validated input and NOTHING else.** The issue's design
sketch had an execution-context prerequisite handed to the executor; that reopens
an I/O channel and weakens the double-execution CHECK, which re-runs each executor
against an independently rebuilt input and byte-compares its output. Here a context
prerequisite is a declaration the ENGINE must satisfy — it must hold that catalog
before it will run the recipe — not an object the executor is given. Strictly
stronger, and it costs the built-ins nothing: both need only their projected safe
input.

That check is defence in depth against ACCIDENTAL nondeterminism in trusted
executors, not a proof. A module global, an import or I/O remains available to
registered code, and §7 places those channels outside this boundary rather than
claiming to close them (issue #145, §6 architect review).
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import (
    Annotated,
    Any,
    Callable,
    Literal,
    Optional,
    Tuple,
    Union,
)

from pydantic import (
    AfterValidator,
    BaseModel,
    ConfigDict,
    Field,
    model_validator,
)
from pydantic_core import PydanticCustomError

from ..models.recipe_contributions import (
    RECIPE_CONTRIBUTION_KINDS,
    RecipeSemanticId,
)

# ---------------------------------------------------------------------------
# SemVer
# ---------------------------------------------------------------------------

#: The official SemVer 2.0.0 regex (semver.org, "Backus-Naur Form Grammar").
#: Implemented in-tree rather than pulling in ``packaging``/``semver``: this repo
#: adds no dependency for one regex plus a comparison tuple.
#:
#: ``re.ASCII`` and ``\Z`` are the two edits the port REQUIRES, and neither is
#: cosmetic. The published regex is written for ECMAScript, where ``\d`` is
#: ASCII-only and ``$`` matches at the end of input; Python defaults ``\d`` to
#: every Unicode decimal digit and lets ``$`` match just before a trailing
#: newline. Transcribed verbatim it therefore accepts two strings SemVer does
#: not: ``"1\N{ARABIC-INDIC DIGIT TWO}.0.0"`` (``[1-9]`` takes the ASCII ``1``,
#: then ``\d*`` takes the Arabic-Indic digit, and ``int()`` parses it as 12),
#: and ``"1.2.3\n"`` — which would register as a version distinct from
#: ``"1.2.3"`` while comparing exactly equal to it. Calling the constant "the
#: official regex" is only true once both dialect differences are closed
#: (issue #145, §6 architect review).
_SEMVER_RE = re.compile(
    r"^(?P<major>0|[1-9]\d*)"
    r"\.(?P<minor>0|[1-9]\d*)"
    r"\.(?P<patch>0|[1-9]\d*)"
    r"(?:-(?P<prerelease>(?:0|[1-9]\d*|\d*[a-zA-Z-][0-9a-zA-Z-]*)"
    r"(?:\.(?:0|[1-9]\d*|\d*[a-zA-Z-][0-9a-zA-Z-]*))*))?"
    r"(?:\+(?P<buildmetadata>[0-9a-zA-Z-]+(?:\.[0-9a-zA-Z-]+)*))?\Z",
    re.ASCII,
)


def parse_semver(value: str) -> Tuple[Any, ...]:
    """A totally-ordered comparison key for a SemVer string.

    Ordering follows SemVer §11: numeric core first; a prerelease sorts BEFORE
    its release (encoded as a leading ``0``/``1`` flag); build metadata is
    ignored for precedence, exactly as the spec requires. Identifiers compare
    numerically when numeric and lexically otherwise, and numeric always sorts
    lower — encoded as a ``(0, int, "")`` / ``(1, 0, str)`` pair so the tuple
    stays comparable without mixing types.
    """
    match = _SEMVER_RE.match(value)
    if match is None:
        raise ValueError(f"not a valid SemVer 2.0.0 version: {value!r}")
    core = (
        int(match.group("major")),
        int(match.group("minor")),
        int(match.group("patch")),
    )
    prerelease = match.group("prerelease")
    if prerelease is None:
        return core + (1, ())
    identifiers = []
    for part in prerelease.split("."):
        if part.isdigit():
            identifiers.append((0, int(part), ""))
        else:
            identifiers.append((1, 0, part))
    return core + (0, tuple(identifiers))


def _validate_semver(value: str) -> str:
    if _SEMVER_RE.match(value) is None:
        raise PydanticCustomError(
            "recipe_version_invalid",
            "recipe_version must be a valid SemVer 2.0.0 string",
        )
    return value


SemVerString = Annotated[
    str,
    AfterValidator(_validate_semver),
    Field(description="SemVer 2.0.0 recipe version"),
]


# ---------------------------------------------------------------------------
# Enumerations
# ---------------------------------------------------------------------------

RecipeEntryKind = Literal[
    "executable_recipe",
    "constraint_only",
    "advisory",
    "compatibility_adapter",
]

RecipeOutputType = Literal[
    "process_ir_patch",
    "system_topology_patch",
    "component_contribution",
    "constraint_requirement",
]

RecipeCapabilityAuthority = Literal[
    "process_ir",
    "process_body",
    "connector_call",
    "process_emitter",
    "system_topology",
    "component_builder",
    "recipe_registry",
]

#: The merge rules a descriptor may declare. THREE, not five: earlier drafts also
#: carried ``append_distinct_topology_key`` and ``append_distinct_component_key``,
#: but distinct keys compose unconditionally and repeated keys conflict
#: unconditionally, so neither rule was ever consulted. A declared rule that gates
#: nothing is a false affordance — it tells an author they have opted into a
#: behaviour that was never theirs to opt into. Removed rather than documented
#: (issue #145, live QA).
RecipeMergeRule = Literal[
    "insert_root_linear_step",
    "append_root_terminal_leg",
    "dedupe_identical_constraint",
]

RecipeExecutionContextKind = Literal[
    "component_catalog",
    "process_symbol_catalog",
    "topology_context",
]


class _RecipeModel(BaseModel):
    """Strict, frozen base for every descriptor-side contract."""

    model_config = ConfigDict(extra="forbid", frozen=True)


# ---------------------------------------------------------------------------
# References and prerequisites
# ---------------------------------------------------------------------------


class RecipeReferenceV1(_RecipeModel):
    """An EXACT reference to one registered recipe version.

    Exact by construction — there is no "latest", no range, no wildcard. That is
    what lets advisory doctrine point at a recipe without the pointer becoming a
    resolution policy someone could shift under it.
    """

    recipe_id: RecipeSemanticId
    recipe_version: SemVerString


class RecipeDependencyV1(_RecipeModel):
    kind: Literal["recipe"]
    recipe_id: RecipeSemanticId
    recipe_version: SemVerString


class ExecutionContextPrerequisiteV1(_RecipeModel):
    """Something the ENGINE must hold before it may run this recipe.

    Never handed to the executor — see the module docstring.
    """

    kind: RecipeExecutionContextKind


RecipePrerequisiteV1 = Annotated[
    Union[RecipeDependencyV1, ExecutionContextPrerequisiteV1],
    Field(discriminator="kind"),
]


class RecipeCapabilityRequirementV1(_RecipeModel):
    """A positive capability the descriptor requires of a canonical authority."""

    authority: RecipeCapabilityAuthority
    subject: RecipeSemanticId
    required_state: Literal["supported", "emittable", "plannable-only"]


class RecipeConflictPolicyV1(_RecipeModel):
    """Which merges this recipe is willing to participate in.

    ``mode`` has exactly one value. A conflict is an ERROR unless every recipe
    writing the contested slot declared the same merge rule — there is no
    last-writer-wins, no priority field, and no way to configure one. A single
    literal says that in the type rather than in a comment.
    """

    mode: Literal["error_unless_declared"] = "error_unless_declared"
    merge_rules: Tuple[RecipeMergeRule, ...] = ()

    @model_validator(mode="after")
    def _rules_are_sorted_and_unique(self) -> "RecipeConflictPolicyV1":
        if len(set(self.merge_rules)) != len(self.merge_rules):
            raise PydanticCustomError(
                "recipe_conflict_policy_invalid", "merge_rules must be unique"
            )
        if list(self.merge_rules) != sorted(self.merge_rules):
            raise PydanticCustomError(
                "recipe_conflict_policy_invalid",
                "merge_rules must be canonically sorted",
            )
        return self


# ---------------------------------------------------------------------------
# Provenance and descriptor
# ---------------------------------------------------------------------------


class RecipeProvenanceV1(_RecipeModel):
    """Trusted code metadata. DERIVED by the registry, never caller-supplied.

    Every field here is computed from the registered callable and model by
    ``registry.py``. There is no constructor path from a request payload — the
    contribution pre-scan additionally rejects ``provenance``,
    ``implementation_sha256`` and friends as input fields, so a caller cannot
    even attempt to assert one.
    """

    package_name: Literal["boomi_mcp"]
    package_version: str = Field(..., min_length=1)
    source_revision: str = Field(..., min_length=1)
    module: str = Field(..., min_length=1)
    symbol: str = Field(..., min_length=1)
    implementation_sha256: str = Field(..., min_length=64, max_length=64)
    descriptor_sha256: str = Field(..., min_length=64, max_length=64)


class RecipeDescriptorV1(_RecipeModel):
    """The published description of one registered recipe version."""

    schema_version: Literal["1"] = "1"
    recipe_id: RecipeSemanticId
    recipe_version: SemVerString
    is_default: bool
    entry_kind: RecipeEntryKind
    input_schema_id: Optional[str] = None
    input_schema_sha256: Optional[str] = None
    output_types: Tuple[RecipeOutputType, ...] = ()
    prerequisites: Tuple[RecipePrerequisiteV1, ...] = ()
    capability_requirements: Tuple[RecipeCapabilityRequirementV1, ...] = ()
    conflict_policy: Optional[RecipeConflictPolicyV1] = None
    adapter_target: Optional[RecipeReferenceV1] = None
    provenance: RecipeProvenanceV1

    def public_payload(self) -> dict:
        """The descriptor as served to an MCP caller.

        ``capability_requirements`` is deliberately REDACTED to a count.
        A requirement's ``subject`` names a key inside a canonical authority, and
        for the ``process_emitter`` authority those keys — ``connectoraction_source``,
        ``branch``, ``doccacheload`` — are dark compiler internals that
        ``tests/test_process_ir_compiler_surface.py`` forbids on any LLM-visible
        surface. Publishing them would leak the emitter registry through a
        descriptor, so the public view says HOW MANY requirements were checked
        without naming the internals. The requirements themselves are unchanged
        and still enforced by ``preflight_capabilities`` — this hides them from
        the caller, not from the gate.
        """
        payload = self.model_dump(mode="json")
        payload["capability_requirements"] = {
            "count": len(self.capability_requirements),
            "note": (
                "Capability subjects name internal compiler-authority keys and "
                "are not published. They are enforced at recipe preflight; an "
                "unmet requirement fails with RECIPE_CAPABILITY_GATED."
            ),
        }
        return payload


# ---------------------------------------------------------------------------
# Executor contract
# ---------------------------------------------------------------------------


class RecipeInputBase(BaseModel):
    """Strict, frozen base every registered recipe input model must subclass.

    Frozen refuses assignment to a field. It does NOT freeze the contents of one,
    so a declared ``List[str]`` stays appendable — and the engine used to run both
    executions over the same object, which let the first run change what the
    second one saw and turned the nondeterminism check into a tautology. The
    engine now builds the second input independently, from a snapshot of the
    caller's raw mapping taken before either run; ``frozen`` is the shallow half
    of that and never was the whole of it (issue #145, §6 architect review).
    """

    model_config = ConfigDict(extra="forbid", frozen=True)


#: A registered executor: validated input in, ordered contributions out.
RecipeExecutorV1 = Callable[[RecipeInputBase], Tuple[Any, ...]]


@dataclass(frozen=True)
class RecipeRegistrationV1:
    """One production registration. Code-owned; there is no runtime registrar."""

    recipe_id: str
    recipe_version: str
    entry_kind: str
    is_default: bool = False
    input_model: Optional[type] = None
    executor: Optional[RecipeExecutorV1] = None
    output_types: Tuple[str, ...] = ()
    prerequisites: Tuple[Any, ...] = ()
    capability_requirements: Tuple[RecipeCapabilityRequirementV1, ...] = ()
    conflict_policy: Optional[RecipeConflictPolicyV1] = None
    adapter_target: Optional[RecipeReferenceV1] = None


# ---------------------------------------------------------------------------
# Registry expectation + skew
# ---------------------------------------------------------------------------


class ExpectedRecipeEntryV1(_RecipeModel):
    """One entry a caller expects the live registry to carry."""

    recipe_id: RecipeSemanticId
    recipe_version: SemVerString
    implementation_sha256: Optional[str] = Field(
        default=None, min_length=64, max_length=64
    )


class ExpectedRecipeRegistryV1(_RecipeModel):
    """A caller-supplied snapshot of the registry it believes it is talking to.

    Purely comparative. Nothing here selects, gates, or alters a recipe — it is
    an assertion the server checks and reports on, so a caller can never use it
    to influence which code runs.
    """

    schema_version: Literal["1"] = "1"
    registry_revision: Optional[str] = None
    source_revision: Optional[str] = None
    entries: Tuple[ExpectedRecipeEntryV1, ...] = ()


class RecipeVersionMismatchV1(_RecipeModel):
    recipe_id: str
    expected_version: str
    live_version: str


class RecipeImplementationMismatchV1(_RecipeModel):
    recipe_id: str
    recipe_version: str
    expected_implementation_sha256: str
    live_implementation_sha256: str


class RecipeRegistrySkewV1(_RecipeModel):
    """The result of comparing a caller's expectation against the live registry.

    ``unknown`` is a real answer and never collapses into ``match``: a caller
    that asked for a comparison the server could not make must be told so, or a
    silent "looks fine" becomes evidence of parity that nobody established.

    ``match`` means the compared FIELDS agree — it is not a claim that the two
    deployments behave identically. ``source_revision`` covers the recipe layer
    and its callers; the downstream compiler, the component builders and the
    response-building layer are outside it (see ``RECIPE_LAYER_MODULES``). A
    deployed image's ``$COMMIT_SHA`` is the only revision that covers the whole
    tree.
    """

    status: Literal["not_requested", "match", "mismatch", "unknown"]
    reason: Optional[str] = None
    missing_from_live: Tuple[str, ...] = ()
    live_only: Tuple[str, ...] = ()
    version_mismatches: Tuple[RecipeVersionMismatchV1, ...] = ()
    implementation_mismatches: Tuple[RecipeImplementationMismatchV1, ...] = ()
    registry_revision_mismatch: bool = False
    source_revision_mismatch: bool = False


# The registry's own literal must equal the union's derived kinds. Asserted at
# IMPORT time, in the same spirit as the topology capability coverage check: a
# fifth contribution type added to the union without a matching literal here
# would otherwise be undeclarable, and the failure would surface as a confusing
# validation error at some later call site instead of at the seam that broke.
_DECLARED_OUTPUT_TYPES = frozenset(RecipeOutputType.__args__)  # type: ignore[attr-defined]
if _DECLARED_OUTPUT_TYPES != frozenset(RECIPE_CONTRIBUTION_KINDS):  # pragma: no cover
    raise RuntimeError(
        "RecipeOutputType is out of sync with RECIPE_CONTRIBUTION_KINDS — "
        "the contribution union and the registry's declared outputs must agree"
    )


__all__ = [
    "ExecutionContextPrerequisiteV1",
    "ExpectedRecipeEntryV1",
    "ExpectedRecipeRegistryV1",
    "RecipeCapabilityAuthority",
    "RecipeCapabilityRequirementV1",
    "RecipeConflictPolicyV1",
    "RecipeDependencyV1",
    "RecipeDescriptorV1",
    "RecipeEntryKind",
    "RecipeExecutionContextKind",
    "RecipeExecutorV1",
    "RecipeImplementationMismatchV1",
    "RecipeInputBase",
    "RecipeMergeRule",
    "RecipeOutputType",
    "RecipePrerequisiteV1",
    "RecipeProvenanceV1",
    "RecipeReferenceV1",
    "RecipeRegistrationV1",
    "RecipeRegistrySkewV1",
    "RecipeVersionMismatchV1",
    "SemVerString",
    "parse_semver",
]
