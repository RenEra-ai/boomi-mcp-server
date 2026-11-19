"""Strict typed recipe contributions (issue #145 M12.10).

The SIX — and only six — values a registered executable recipe may return:

* :class:`ProcessIRPatchV1`             — closed operations on one process's root
* :class:`SystemTopologyPatchV1`        — closed additive operations on one topology
* :class:`ComponentContributionV1`      — a component the recipe wants materialized
* :class:`ConstraintRequirementV1`      — a requirement the canonical validators check
* :class:`RecordedIntentContributionV1` — a recorded-not-wired declaration (#157)
* :class:`ProcessUnitContributionV1`    — one whole process root plus its envelope (#159)

Every model is strict (``extra="forbid"``), frozen, versioned, repr-redacted, and
canonically serializable. Together they are a CLOSED discriminated union tagged by
``contribution_kind``.

**Why a discriminator at all.** The models have disjoint field sets, so a
plain ``Union`` would parse by trial and a near-miss payload could land in the
wrong member with a confusing error. More importantly, a recipe descriptor
DECLARES which contribution types it emits, and enforcing that declaration means
naming the kind of a value that has already been built — which needs a tag on the
value, not a guess about it. ``RECIPE_CONTRIBUTION_KINDS`` is DERIVED from the
union rather than hand-listed, so the registry's ``output_types`` literal and this
module cannot drift.

**The forbidden-shape scan is separate from — and runs before — pydantic.**
``extra="forbid"`` already rejects an unknown field, but it rejects it as a
schema error at whatever depth pydantic happened to reach, and it says nothing
at all about a payload that never reaches a model (an executor returning a raw
dict, or one that built its object with ``model_construct``). The scan walks raw
payloads AND ``model_dump()`` output, so neither route can smuggle a
configuration bag, a credential FIELD, raw XML, executable code, a generic path,
or a caller-authored graph edge into a contribution.

**It matches on KEY NAMES, not on values.** A secret placed in the *value* of a
legitimately-typed field is not detected — not in an opaque reference
(``source_connection_ref: "hunter2"``), and not in one of ProcessIR's LITERAL
value fields: ``MessageNodeV1.text``, ``ExceptionNodeV1.message_template``,
``StaticOperandV1.static_value``, ``TrackOperandV1.default_value``. Those carry
authored text by design, and a scanner cannot tell a message body from a leaked
credential without inspecting content it has no business inspecting.

An earlier version of this note claimed the only string fields were references,
enumerations and labels. That was false, and live QA falsified it (issue #145).
The accurate statement is narrower: this scan removes the *structural* smuggling
routes — a config bag, a credential-named field, raw XML, executable code, a
generic path, an authored graph edge. **It is not a content filter, and nothing
here claims to stop a caller who deliberately types a secret into a message
body.** That residual is the same one direct ProcessIR authoring carries; ADR-001
§11's secret rule and ``parse_process_ir_v1``'s own secret pre-scan — which keys
on names too — are the shared boundary, not something this layer tightens.

ADR-001 §6/§11 boundaries this module inherits: no XML, no layout or shape ids,
no CFG edges, no credentials, no document content. It adds two of its own —
``custom_scripting`` is rejected even though direct ProcessIR authoring supports
it (a recipe is code the registry vouches for, and vouching for arbitrary script
text is exactly what "no LLM-generated Python" rules out), and component
``depends_on`` is rejected because materialization order is builder-derived, not
recipe-authored.

That ``depends_on`` refusal has exactly ONE positioned exception (issue #159 S1):
the typed envelope of a process-unit contribution, at
``/unit/envelope/depends_on``. The integration planner reads a process root's
declared dependency edges and refuses a root whose required edges are missing, so
a unit cannot be emitted without them. The exception is applied by
:func:`parse_recipe_contribution` only, only for ``contribution_kind ==
"process_unit"``, and only to a sequence of key strings; the recursive scan stays
in force at every other position of every contribution, and a raw recipe INPUT
gets no exception at all.
"""

from __future__ import annotations

import json
from types import MappingProxyType
from typing import Annotated, Any, List, Literal, Mapping, Optional, Tuple, Union, get_args

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    StringConstraints,
    ValidationError,
    model_validator,
)
from pydantic_core import PydanticCustomError

from ..errors import RECIPE_CONTRIBUTION_INVALID
from .governance_intent import RecordedIntentDeclarationV1
from .process_component import ProcessAuthoringUnitV1
from .process_ir import (
    BranchLegV1,
    ComponentRefV1,
    LinearNodeV1,
    ProcessIRV1,
    # Defined in ``process_ir`` and re-exported here as the SAME objects (issue
    # #159 S1): ``process_component`` keys its envelope by this alias, and this
    # module now imports ``process_component``, so the definition had to move
    # below both. ``composer`` and every recipe input keep importing it from here.
    RecipeComponentKey,
    _validate_component_key,
)
from .system_topology import (
    OpaquePlatformRefV1,
    TOPOLOGY_OBJECT_KINDS,
    TOPOLOGY_RELATION_KINDS,
    TopologyObjectKeyV1,
    TopologyObjectV1,
    TopologyRelationKeyV1,
    TopologyRelationV1,
)

RECIPE_CONTRIBUTION_VERSION = "1"


# ---------------------------------------------------------------------------
# Strict base
# ---------------------------------------------------------------------------

#: Fields safe to show in a ``repr``. Everything else renders as ``...``.
#: Same policy and the same reason as ``process_ir._REPR_SAFE_FIELDS``: a repr
#: reaches logs and exception chains, and an authored value must not.
#: Every member here is a CLOSED literal or an enum-like discriminator — never a
#: caller-authored string, key, label or reference.
_REPR_SAFE_FIELDS = frozenset(
    {
        "contribution_kind",
        "version",
        "op",
        "kind",
        "slot",
        "component_type",
        "materialization_mode",
        "authority",
        "required_state",
        "object_kind",
        "relation_kind",
    }
)


class _RecipeContributionBase(BaseModel):
    """Shared strict base: unknown fields rejected, values repr-suppressed, frozen."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    def __repr_args__(self):  # noqa: D105 — pydantic hook
        for key, value in super().__repr_args__():
            if key in _REPR_SAFE_FIELDS:
                yield key, value
            else:
                yield key, "..."


# ---------------------------------------------------------------------------
# Shared aliases
# ---------------------------------------------------------------------------

_SEMANTIC_ID_PATTERN = r"^[a-z][a-z0-9_.-]*$"

RecipeSemanticId = Annotated[
    str,
    StringConstraints(min_length=1, max_length=128, pattern=_SEMANTIC_ID_PATTERN),
    Field(description="Lower-case semantic identifier chosen by the recipe author"),
]


#: The component types a recipe may contribute. A PINNED COPY of
#: ``integration_builder._COMPONENT_NAME_PRIMARY_TYPES`` plus ``process``, not an
#: import: ``models/`` must not import ``categories/`` (builders import models;
#: the reverse would cycle), the same rule and the same precedent as ProcessIR's
#: copied secret list. ``tests/test_recipe_contribution_models.py`` pins the
#: relationship in both directions so the copy cannot drift silently.
#:
#: ``trading_partner`` is deliberately EXCLUDED: no recipe materializes one, and
#: an unreachable literal is a code you never test. The pin test asserts that
#: difference is exactly the declared one rather than accidental drift.
RECIPE_COMPONENT_TYPES: Tuple[str, ...] = (
    "connector-action",
    "connector-settings",
    "documentcache",
    "process",
    "processproperty",
    "profile.db",
    "profile.json",
    "profile.xml",
    "script.mapping",
    "transform.function",
    "transform.map",
    "webservice",
)

#: The component types in ``_COMPONENT_NAME_PRIMARY_TYPES`` that a recipe may NOT
#: contribute. Named so the pin test can assert an exact, intentional difference.
RECIPE_EXCLUDED_COMPONENT_TYPES: Tuple[str, ...] = ("trading_partner",)

RecipeComponentType = Literal[
    "connector-action",
    "connector-settings",
    "documentcache",
    "process",
    "processproperty",
    "profile.db",
    "profile.json",
    "profile.xml",
    "script.mapping",
    "transform.function",
    "transform.map",
    "webservice",
]


# ---------------------------------------------------------------------------
# ProcessIRPatch
# ---------------------------------------------------------------------------


class SetProcessRootV1(_RecipeContributionBase):
    """Establish the whole root for one process.

    The only operation that AUTHORS a root; everything else adds to one that
    already exists. Two ``set_process_root`` operations for one process key are a
    conflict even when byte-identical — see ``recipes.composer``.
    """

    operation_id: RecipeSemanticId
    op: Literal["set_process_root"]
    slot: Literal["root"]
    root: ProcessIRV1


class InsertRootLinearStepV1(_RecipeContributionBase):
    """Insert one linear step immediately before the root's terminal unit.

    "Terminal unit" is the indivisible tail of the root sequence: a terminal
    control (``branch``/``decision``/``exception``/``try_catch``), a standalone
    ``return_documents``/``stop``, or the ``target``+``stop`` PAIR — which is
    never split, because a step between a target and its stop is a step on no
    path the emitter can express.
    """

    operation_id: RecipeSemanticId
    op: Literal["insert_root_linear_step"]
    slot: Literal["root.before_terminal"]
    step: LinearNodeV1


class AppendRootTerminalLegV1(_RecipeContributionBase):
    """Append one leg to the root's terminal Branch.

    Only the FINAL root Branch is addressable. Nested branches have no stable
    semantic address in ProcessIR v1 (there are no authored node ids), and
    inventing one — an index path, a label match — is precisely the generic
    pointer patching this contract exists to forbid.
    """

    operation_id: RecipeSemanticId
    op: Literal["append_root_terminal_leg"]
    slot: Literal["root.terminal.branch.legs"]
    leg: BranchLegV1


ProcessIRPatchOperationV1 = Annotated[
    Union[SetProcessRootV1, InsertRootLinearStepV1, AppendRootTerminalLegV1],
    Field(discriminator="op"),
]


class ProcessIRPatchV1(_RecipeContributionBase):
    """Closed operations against ONE process, addressed as ``(process_key, slot)``.

    There is deliberately no node id, JSON pointer, or index path anywhere in this
    model. ProcessIR v1 has no authored node identity — ``label`` is optional and
    non-unique, and CFG/shape ids are compiler-owned — so the only addressable
    thing is the semantic pair of a process key and a closed slot literal.
    """

    contribution_kind: Literal["process_ir_patch"]
    version: Literal["1"]
    process_key: RecipeComponentKey
    operations: Tuple[ProcessIRPatchOperationV1, ...] = Field(..., min_length=1)

    @model_validator(mode="after")
    def _unique_operation_ids(self) -> "ProcessIRPatchV1":
        seen = set()
        for operation in self.operations:
            if operation.operation_id in seen:
                raise PydanticCustomError(
                    "recipe_duplicate_operation_id",
                    "operation_id must be unique within a patch",
                )
            seen.add(operation.operation_id)
        return self


# ---------------------------------------------------------------------------
# SystemTopologyPatch
# ---------------------------------------------------------------------------


class AddTopologyObjectV1(_RecipeContributionBase):
    operation_id: RecipeSemanticId
    op: Literal["add_object"]
    object: TopologyObjectV1


class AddTopologyRelationV1(_RecipeContributionBase):
    operation_id: RecipeSemanticId
    op: Literal["add_relation"]
    relation: TopologyRelationV1


SystemTopologyPatchOperationV1 = Annotated[
    Union[AddTopologyObjectV1, AddTopologyRelationV1],
    Field(discriminator="op"),
]


class SystemTopologyPatchV1(_RecipeContributionBase):
    """Additive-only operations against ONE topology document.

    No update, replace, remove, or lifecycle operation exists — #144 ships a
    PLANNER and this issue adds no mutation. The only relationships expressible
    are the existing typed relations with their named role fields; there is no
    generic edge operation, so a recipe cannot author a graph the topology
    schema would not itself admit.
    """

    contribution_kind: Literal["system_topology_patch"]
    version: Literal["1"]
    topology_id: RecipeSemanticId
    profile_ref: OpaquePlatformRefV1
    operations: Tuple[SystemTopologyPatchOperationV1, ...] = Field(..., min_length=1)

    @model_validator(mode="after")
    def _unique_operation_ids(self) -> "SystemTopologyPatchV1":
        seen = set()
        for operation in self.operations:
            if operation.operation_id in seen:
                raise PydanticCustomError(
                    "recipe_duplicate_operation_id",
                    "operation_id must be unique within a patch",
                )
            seen.add(operation.operation_id)
        return self


# ---------------------------------------------------------------------------
# ComponentContribution
# ---------------------------------------------------------------------------


class ComponentContributionV1(_RecipeContributionBase):
    """A component the recipe wants materialized, named by SLOT.

    It carries a HEADER — key, type, mode — and a ``materializer_slot`` naming an
    entry in a private, adapter-owned catalog. It carries no ``config``, name,
    component id, profile body, header, XML, dependency edge, or executable
    payload of any kind.

    That split is the whole security design. The legacy compatibility adapter
    already accepts SQL, hosts, credentials and script bodies — it has to, those
    are its existing public inputs — so it builds the real
    ``IntegrationComponentSpec`` objects itself and keeps them in a catalog that
    is never serialized and never handed to an executor. The recipe sees only an
    opaque slot name. The engine then resolves the slot and VERIFIES the catalog
    entry's key/type/mode equal this header, so a recipe cannot silently receive
    a different component than the one it declared.
    """

    contribution_kind: Literal["component_contribution"]
    version: Literal["1"]
    contribution_id: RecipeSemanticId
    component_key: RecipeComponentKey
    component_type: RecipeComponentType
    materialization_mode: Literal["create", "update", "reuse_reference"]
    materializer_slot: RecipeSemanticId


# ---------------------------------------------------------------------------
# ConstraintRequirement
# ---------------------------------------------------------------------------

_TopologyObjectKindLiteral = Literal[TOPOLOGY_OBJECT_KINDS]  # type: ignore[valid-type]
_TopologyRelationKindLiteral = Literal[TOPOLOGY_RELATION_KINDS]  # type: ignore[valid-type]


class RequireComponentV1(_RecipeContributionBase):
    kind: Literal["component"]
    component_key: RecipeComponentKey
    component_type: RecipeComponentType


class RequireProcessV1(_RecipeContributionBase):
    kind: Literal["process"]
    process_key: RecipeComponentKey


class RequireTopologyObjectV1(_RecipeContributionBase):
    kind: Literal["topology_object"]
    topology_id: RecipeSemanticId
    object_key: TopologyObjectKeyV1
    object_kind: _TopologyObjectKindLiteral


class RequireTopologyRelationV1(_RecipeContributionBase):
    kind: Literal["topology_relation"]
    topology_id: RecipeSemanticId
    relation_key: TopologyRelationKeyV1
    relation_kind: _TopologyRelationKindLiteral


class RequireCapabilityV1(_RecipeContributionBase):
    """A POSITIVE capability requirement against a named canonical authority.

    ``required_state`` admits only positive states. There is no way to require
    that something be gated, unsupported, or absent — a requirement exists to say
    "this must work", and a recipe that wanted the opposite would be asserting its
    own violation is safe, which §4 of the issue forbids outright.
    """

    kind: Literal["capability"]
    authority: Literal[
        "process_ir",
        "process_body",
        "connector_call",
        "process_emitter",
        "system_topology",
        "component_builder",
        "recipe_registry",
    ]
    subject: RecipeSemanticId
    required_state: Literal["supported", "emittable", "plannable-only"]


ConstraintCheckV1 = Annotated[
    Union[
        RequireComponentV1,
        RequireProcessV1,
        RequireTopologyObjectV1,
        RequireTopologyRelationV1,
        RequireCapabilityV1,
    ],
    Field(discriminator="kind"),
]


class ConstraintRequirementV1(_RecipeContributionBase):
    """One requirement the canonical validators must satisfy.

    There is no ``passed``, ``severity``, ``waiver``, ``exemption``, ``safe``,
    expression, callable, validator name, or caller-supplied remediation field.
    A constraint can only ADD an obligation; it can never discharge one. Recipes
    cannot mark their own violations safe — the canonical validators decide, and
    the engine evaluates these against what those validators produced.
    """

    contribution_kind: Literal["constraint_requirement"]
    version: Literal["1"]
    requirement_id: RecipeSemanticId
    requirement: ConstraintCheckV1


class RecordedIntentContributionV1(_RecipeContributionBase):
    """One recorded-not-wired declaration a recipe wants SERVED (issue #157).

    The fifth contribution kind, and the deliberate opposite of a patch: it
    changes nothing the engine compiles, materializes or hashes. A recipe uses
    it to state an operational intent — a watermark strategy, a runtime hint —
    that the caller can read back from the result, so a deliberate declaration
    is recorded rather than silently swallowed.

    ``status`` is a single-valued literal: there is no wired state. The engine
    threads these into the run result unchanged; the authoring workflow serves
    them beside the executable normalized spec and keeps them OUT of every
    fingerprint and every mutation payload (a paired test pins that only the
    served record moves).
    """

    contribution_kind: Literal["recorded_intent"]
    version: Literal["1"]
    intent_id: RecipeSemanticId
    process_key: RecipeComponentKey
    declaration: RecordedIntentDeclarationV1
    status: Literal["recorded_not_wired"]


class ProcessUnitContributionV1(_RecipeContributionBase):
    """One whole process root plus the envelope that describes it (issue #159).

    The sixth contribution kind. Where a ``set_process_root`` patch hands over a
    bare root and leaves its name, action, folder and extension bindings to be
    supplied elsewhere, a process unit carries the STRICT
    :class:`~boomi_mcp.models.process_component.ProcessAuthoringUnitV1` — exactly
    one ``ProcessIRV1`` root and exactly one envelope — so the recipe that built
    the root also owns its governance. One authority per root.

    The unit's ``envelope.component_key`` is the root's key in the shared
    component-key namespace. The composer refuses a unit whose key is also
    written by another unit, by a ``set_process_root`` patch, by a direct root,
    by a component slot, or that names an existing catalog component; root
    patches (``insert_root_linear_step``, ``append_root_terminal_leg``) apply to
    a unit's root exactly as they do to any other root, and the envelope is
    carried through unchanged.

    ``unit.envelope.depends_on`` is the ONE position in any contribution where a
    dependency edge may be authored: the integration planner reads a root's
    declared edges and refuses a root whose required edges are missing. Only a
    sequence of component keys is admitted there; ``depends_on`` anywhere else in
    this or any other contribution is still refused by the forbidden-shape scan.
    """

    contribution_kind: Literal["process_unit"]
    version: Literal["1"]
    contribution_id: RecipeSemanticId
    unit: ProcessAuthoringUnitV1


# ---------------------------------------------------------------------------
# The closed union
# ---------------------------------------------------------------------------

RecipeContributionV1 = Annotated[
    Union[
        ProcessIRPatchV1,
        SystemTopologyPatchV1,
        ComponentContributionV1,
        ConstraintRequirementV1,
        RecordedIntentContributionV1,
        ProcessUnitContributionV1,
    ],
    Field(discriminator="contribution_kind"),
]

_CONTRIBUTION_MEMBERS: Tuple[type, ...] = get_args(get_args(RecipeContributionV1)[0])

#: DERIVED from the union, never hand-listed — the registry's ``output_types``
#: allowlist is built from this and ``RecipeOutputType`` is pinned to it at
#: import, so a new contribution type cannot be added to the union without the
#: registry learning about it in the same commit.
RECIPE_CONTRIBUTION_KINDS: Tuple[str, ...] = tuple(
    get_args(member.model_fields["contribution_kind"].annotation)[0]
    for member in _CONTRIBUTION_MEMBERS
)

_KIND_TO_MODEL = dict(zip(RECIPE_CONTRIBUTION_KINDS, _CONTRIBUTION_MEMBERS))


# ---------------------------------------------------------------------------
# Server-produced run records (issue #159 S1)
# ---------------------------------------------------------------------------
#
# Neither model below is a contribution: a recipe never returns one, and neither
# is a member of ``RecipeContributionV1``. They are what the SERVER records about
# a run. They live in the model layer, not in ``recipes.contracts``, because the
# authoring result models serve them and ``models/`` must not import the
# ``recipes`` package (whose ``__init__`` pulls in the engine and the registry).
# ``recipes.contracts`` re-exports both.


class RecipeInvocationProvenanceV1(_RecipeContributionBase):
    """Which registered code one recipe invocation actually ran.

    Every field except ``invocation_id`` is copied from the descriptor the
    REGISTRY resolved for the invocation, never from the request: a request that
    named no version and one that pinned the default version resolve to the same
    descriptor and therefore record the same provenance. ``invocation_id`` is the
    caller's own identifier for the invocation, returned to the caller.

    Carries no recipe input, no contribution and no component material — only
    the identity of the code that produced them.
    """

    invocation_id: str = Field(
        ...,
        description="The caller-chosen invocation_id of the invocation this record describes",
    )
    recipe_id: RecipeSemanticId
    recipe_version: str = Field(
        ...,
        min_length=1,
        description="The SemVer version of the descriptor the registry resolved",
    )
    entry_kind: str = Field(
        ...,
        min_length=1,
        description="The resolved descriptor's entry kind",
    )
    implementation_sha256: str = Field(
        ...,
        min_length=64,
        max_length=64,
        description=(
            "The registry-derived digest of the resolved recipe's implementation"
        ),
    )
    source_revision: str = Field(
        ...,
        min_length=1,
        description="The registry-derived source revision of the recipe layer",
    )


class RecipeRunDiagnosticV1(_RecipeContributionBase):
    """One informational, value-free record about a recipe's assembled output.

    Not a failure: a hard failure is a ``RecipeError`` and stops the run. This
    record names, for one Branch leg of one recipe-assembled process, which
    recipe invocation produced the leg and which semantic part of the recipe's
    input it realizes, so a caller can trace an emitted leg back to its source.

    Every field is a closed literal, a registered identifier, the caller's own
    invocation id, a semantic key from the caller's own request, or a position.
    None carries a configuration value, a credential, or component material.
    """

    code: Literal["RECIPE_LEG_PROVENANCE"]
    invocation_id: str = Field(
        ...,
        description="The caller-chosen invocation_id of the producing invocation",
    )
    recipe_id: RecipeSemanticId
    recipe_version: str = Field(
        ...,
        min_length=1,
        description="The SemVer version of the producing recipe, as resolved",
    )
    process_key: RecipeComponentKey
    ordinal: int = Field(
        ...,
        ge=1,
        description="1-based position of the leg on the root's terminal Branch",
    )
    part_key: str = Field(
        ...,
        min_length=1,
        description="The semantic part key, from the recipe input, the leg realizes",
    )


# ---------------------------------------------------------------------------
# Forbidden-shape pre-scan
# ---------------------------------------------------------------------------

#: Exact field names that must never appear anywhere in a recipe input or a
#: contribution. Grouped by what they would reopen if admitted.
_FORBIDDEN_EXACT_KEYS: frozenset = frozenset(
    {
        # a caller-authored flows echo (#157: on the typed surface flows are a
        # DERIVED, output-only projection; a recipe may not author rows)
        "flows",
        # free-form configuration bags
        "config",
        "configuration",
        "metadata",
        "parameters",
        "extensions",
        "settings",
        "properties",
        "options",
        # raw XML / component bodies
        "xml",
        "raw_xml",
        "component_xml",
        "process_xml",
        "shape_xml",
        "shape_xml_parts",
        # headers
        "headers",
        "raw_headers",
        "default_headers",
        "custom_headers",
        # connection material
        "host",
        "hostname",
        "base_url",
        "baseurl",
        "url",
        "endpoint",
        "port",
        "driver",
        "username",
        "user",
        "sql",
        "query",
        "connection_string",
        "connection_properties",
        "jdbc_options",
        "credential_ref",
        "certificate",
        "environment_extensions",
        "profile_override",
        # executable code
        "code",
        "source_code",
        "script",
        "script_body",
        "scripting",
        "custom_scripting",
        "language",
        "callable",
        "import",
        "module",
        "class",
        # generic path / pointer / index patching
        "path",
        "json_pointer",
        "pointer",
        "index",
        "node_id",
        "shape_id",
        "cfg_id",
        "layout",
        # generic graph edges (the typed ProcessIR bodies and typed topology
        # relations are the ONLY graph semantics)
        "edge",
        "edges",
        "from",
        "to",
        "depends_on",
        "dependencies",
        # provenance / verdicts a caller must never assert
        "provenance",
        "source_revision",
        "registry_revision",
        "implementation_sha256",
        "descriptor_sha256",
        "capability_state",
        "verdict",
        "passed",
        "waiver",
        "exemption",
        "validation_policy",
        "conflict_priority",
    }
)

#: Case-insensitive substrings that mark a secret-shaped key.
#:
#: NOT a literal superset of ProcessIR's list, and the difference is deliberate:
#: where ProcessIR carries ``credentials``, this carries the shorter
#: ``credential``, which substring-matches everything the longer one does and
#: more. It adds ``passphrase``, ``private-key``, ``privatekey`` and
#: ``refresh_token``. An earlier version of this note claimed a strict superset;
#: live QA measured it false (issue #145). Detection power is strictly greater —
#: ``tests/test_recipe_security.py`` pins that — but the claim now says what is
#: true rather than what sounded tidy.
#:
#: The list is wider than ProcessIR's because a recipe input is assembled by a
#: compatibility adapter from public archetype parameters that DO carry
#: credentials, so this is the boundary those must not cross.
_FORBIDDEN_KEY_SUBSTRINGS: Tuple[str, ...] = (
    "password",
    "passcode",
    "passphrase",
    "secret",
    "private_key",
    "private-key",
    "privatekey",
    "api_key",
    "apikey",
    "api-key",
    "auth_token",
    "access_token",
    "refresh_token",
    "client_secret",
    "token",
    "authorization",
    "bearer",
    "credential",
)

#: Keys that name a REAL, allowed concept and merely collide with a forbidden
#: substring or exact key. Each is a closed literal or an opaque reference on a
#: model in this module — never a free-form value.
#:
#: ``operations`` would otherwise be caught by nothing, but is listed here for
#: the same reason the others are: to make the allow-list the single place a
#: reader checks when asking "why is this key legal?".
_ALLOWED_KEYS: frozenset = frozenset(
    {
        "operations",
        "operation_id",
        "op",
        "slot",
        "root",
        "step",
        "leg",
        "steps",
        "terminal",
        "legs",
        "kind",
        "object",
        "relation",
        "object_key",
        "object_kind",
        "relation_key",
        "relation_kind",
        "topology_id",
        "profile_ref",
        "version",
        "contribution_kind",
        "contribution_id",
        "component_key",
        "component_type",
        "materialization_mode",
        "materializer_slot",
        "requirement",
        "requirement_id",
        "process_key",
        "authority",
        "subject",
        "required_state",
        # #157 recorded-intent contribution: closed literals and its declaration
        "intent_id",
        "declaration",
        "declaration_kind",
        "status",
        "hint_kind",
        "persistence",
        "source_profile_ref",
        "query_parameter_refs",
    }
)


def scan_forbidden_recipe_shape(
    payload: Any, _path: Tuple[Any, ...] = ()
) -> Optional[Tuple[Tuple[Any, ...], str]]:
    """Return ``(path, reason)`` for the first forbidden shape, or ``None``.

    Runs on RAW payloads and on ``model_dump()`` output alike. The second is what
    closes the ``model_construct`` hole: an executor that skipped validation still
    has to hand back an object, and dumping it puts every field back on the table.

    A ``None`` value is not a violation — an optional field that is simply absent
    carries nothing. An empty container is likewise nothing. Anything else under
    a forbidden key is.

    This public scan has NO exemption. It is also the gate a raw recipe input
    passes, and the one positioned exception the contribution parser applies
    (``/unit/envelope/depends_on`` on a process unit) is applied by
    :func:`parse_recipe_contribution` through a private entry point, never here.
    """
    return _scan_forbidden_shape(payload, _path, frozenset())


#: The ONLY exact positions, as root-anchored structural paths, where a
#: contribution may carry a key the scan otherwise forbids — keyed by the
#: ``contribution_kind`` that earns the exception. Exactly one today (issue #159
#: S1): a process unit's typed envelope declares the root's dependency edges,
#: because the integration planner reads them and refuses a root whose required
#: edges are missing.
_POSITIONED_EXEMPTIONS: Mapping[str, frozenset] = MappingProxyType(
    {"process_unit": frozenset({("unit", "envelope", "depends_on")})}
)


def _is_key_sequence(value: Any) -> bool:
    """A list or tuple whose every item is a string — the typed key-sequence shape."""
    return isinstance(value, (list, tuple)) and all(isinstance(item, str) for item in value)


def _scan_forbidden_shape(
    payload: Any, _path: Tuple[Any, ...], exempt: frozenset
) -> Optional[Tuple[Tuple[Any, ...], str]]:
    """The scan itself. ``exempt`` holds root-anchored paths whose KEY is admitted.

    An exempt position admits its key only when the value there is a sequence of
    strings (the typed key-sequence shape). Any other value at that position is
    judged by the ordinary rules, so the exemption cannot carry a mapping, a
    nested structure, or anything else past the scan.
    """
    if isinstance(payload, dict):
        for key, value in payload.items():
            if isinstance(key, str):
                here = _path + (key,)
                if here in exempt and _is_key_sequence(value):
                    continue
                if key in _ALLOWED_KEYS:
                    found = _scan_forbidden_shape(value, here, exempt)
                    if found is not None:
                        return found
                    continue
                lowered = key.lower()
                if key in _FORBIDDEN_EXACT_KEYS:
                    if value is not None and value != "" and value != [] and value != {}:
                        return here, "forbidden_field"
                    continue
                if any(sub in lowered for sub in _FORBIDDEN_KEY_SUBSTRINGS):
                    if value is not None and value != "" and value != [] and value != {}:
                        return here, "secret_shaped_field"
                    continue
            found = _scan_forbidden_shape(value, _path + (key,), exempt)
            if found is not None:
                return found
    elif isinstance(payload, list):
        for index, item in enumerate(payload):
            found = _scan_forbidden_shape(item, _path + (index,), exempt)
            if found is not None:
                return found
    return None


# ---------------------------------------------------------------------------
# Parse entry point
# ---------------------------------------------------------------------------


class RecipeContributionValidationError(Exception):
    """Deterministic, value-free contribution-validation failure.

    ``diagnostics`` is a sorted tuple of ``(code, path, reason)``. Neither the
    message nor the diagnostics carry an authored value: this exception is
    logged and chained, and a recipe input can contain a sentinel that must not
    reach either.
    """

    def __init__(self, diagnostics: Tuple[Tuple[str, str, str], ...]) -> None:
        self.diagnostics = tuple(sorted(diagnostics))
        super().__init__(
            "; ".join(f"{code}@{path or '/'}" for code, path, _ in self.diagnostics)
        )

    def __repr__(self) -> str:  # noqa: D105
        return f"RecipeContributionValidationError({len(self.diagnostics)} diagnostics)"


def _pointer(path: Tuple[Any, ...]) -> str:
    """RFC 6901 pointer for a structural path. Structure only — never a value."""
    if not path:
        return "/"
    escaped = [str(part).replace("~", "~0").replace("/", "~1") for part in path]
    return "/" + "/".join(escaped)


def _derive_discriminator_fields() -> frozenset:
    """Every discriminator name reachable from a contribution, DERIVED.

    Pydantic v2 records the discriminator on each tagged-union core-schema node,
    so the complete set is one walk of the compiled schema. Hand-listing it was
    wrong by exactly one — ``value_type`` (``PropertySourceV1``,
    ``DecisionOperandV1``) was missing, so a ``set_ddp`` source-value error
    emitted a pointer that addressed nothing (issue #145, live QA).

    That miss is the point. A hand-kept list of "all the members of X" has been
    wrong here repeatedly; the only durable fix is to stop keeping one. This walk
    picks up a sixth discriminator the day someone adds one, with no test to
    remember to update.
    """
    from pydantic import TypeAdapter

    found: set = set()
    seen: set = set()

    def walk(node: Any) -> None:
        if id(node) in seen:
            return
        seen.add(id(node))
        if isinstance(node, dict):
            discriminator = node.get("discriminator")
            if isinstance(discriminator, str):
                found.add(discriminator)
            for value in node.values():
                walk(value)
        elif isinstance(node, (list, tuple)):
            for value in node:
                walk(value)

    walk(TypeAdapter(RecipeContributionV1).core_schema)
    return frozenset(found)


#: Derived once at import. A pydantic v2 union location inserts the TAG VALUE as
#: a segment, and recognizing one means asking the container what its
#: discriminator says.
_DISCRIMINATOR_FIELDS = _derive_discriminator_fields()


def _error_pointer(location: Tuple[Any, ...], payload: Any = None) -> str:
    """Pointer for a pydantic error location, resolved against the PAYLOAD.

    Pydantic v2 inserts a discriminated-union member's TAG VALUE into the
    location — a schema fact, not a position in the authored document. Keeping it
    makes the pointer unresolvable against what the caller sent: a missing
    ``operation_ref`` was reported at
    ``/operations/0/set_process_root/root/body/steps/0/source/operation_ref``,
    where neither ``set_process_root`` nor ``source`` is a key in the payload —
    they are the ``op`` and ``kind`` values. (A first version dropped segments
    ending in ``]``, pydantic v1's ``Model[tag]`` spelling, which matched
    nothing.)

    A segment is a TAG when the container's discriminator field holds exactly
    that value — asked of the payload, not guessed from a name list, and the set
    of discriminator NAMES is itself derived from the compiled schema rather than
    hand-listed. At most ONE tag is skipped per container, because pydantic emits
    exactly one per union level. That bound is the load-bearing part:
    ``MapRefNodeV1`` is the one member (of 61) whose tag value equals one of its
    own field names (``{"kind": "map_ref", "map_ref": ...}``), so a location can
    legitimately carry ``map_ref`` twice — first the tag, then the field — and
    skipping greedily swallowed the field too.

    Everything else is resolved by WALKING, which is exactly the property the
    pointer claims: RFC 6901 against the submitted document. The final segment of
    a ``missing`` error is the absent key itself and is kept unresolved. It is
    identified by POSITION rather than by value — no currently-reachable location
    distinguishes the two, so this is a correctness choice rather than a fix for
    an observed defect.
    """
    if payload is None:
        return _pointer(tuple(location))

    parts: List[Any] = []
    cursor: Any = payload
    last_index = len(location) - 1
    skipped_tag_here = False

    for index, part in enumerate(location):
        if (
            isinstance(cursor, dict)
            and not skipped_tag_here
            and any(cursor.get(field) == part for field in _DISCRIMINATOR_FIELDS)
        ):
            # A union tag. Skip it and stay on the same container.
            skipped_tag_here = True
            continue

        if isinstance(cursor, dict) and part in cursor:
            parts.append(part)
            cursor = cursor[part]
            skipped_tag_here = False
        elif isinstance(cursor, (list, tuple)) and isinstance(part, int) and (
            -len(cursor) <= part < len(cursor)
        ):
            parts.append(part)
            cursor = cursor[part]
            skipped_tag_here = False
        elif index == last_index:
            # The very key the error says is MISSING. Nothing follows it, so
            # keeping it is right and the walk ends here.
            parts.append(part)
        # Anything else is unresolvable mid-path — drop it rather than emit a
        # pointer that addresses nothing.

    return _pointer(tuple(parts))


def parse_recipe_contribution(payload: Any) -> Any:
    """Parse one payload into a validated contribution.

    Gate order — shape, then forbidden scan, then the strict model. The scan
    precedes pydantic on purpose: a payload carrying a credential must be
    rejected as a SECURITY failure with a value-free diagnostic, not as a schema
    error whose pydantic message could echo the offending input back.
    """
    if not isinstance(payload, dict):
        raise RecipeContributionValidationError(
            ((RECIPE_CONTRIBUTION_INVALID, "/", "payload_not_an_object"),)
        )

    # The positioned exemption is chosen by the payload's OWN kind, read with an
    # ``isinstance`` guard first (an unhashable or look-alike kind must not reach
    # the lookup). Any other kind — or none — is scanned with no exemption.
    claimed_kind = payload.get("contribution_kind")
    exempt = (
        _POSITIONED_EXEMPTIONS.get(claimed_kind, frozenset())
        if type(claimed_kind) is str
        else frozenset()
    )
    found = _scan_forbidden_shape(payload, (), exempt)
    if found is not None:
        path, reason = found
        raise RecipeContributionValidationError(
            ((RECIPE_CONTRIBUTION_INVALID, _pointer(path), reason),)
        )

    kind = payload.get("contribution_kind")
    # ``isinstance`` FIRST. ``kind not in _KIND_TO_MODEL`` hashes the caller's
    # value, so a dict/list/set ``contribution_kind`` raised a bare
    # ``TypeError: unhashable type`` straight through the taxonomy — including
    # through the ``model_construct`` guard, whose whole job is to turn anything
    # a caller can produce into a value-free diagnostic (issue #145, live QA).
    if not isinstance(kind, str) or kind not in _KIND_TO_MODEL:
        raise RecipeContributionValidationError(
            (
                (
                    RECIPE_CONTRIBUTION_INVALID,
                    "/contribution_kind",
                    "unknown_or_missing_contribution_kind",
                ),
            )
        )
    version = payload.get("version")
    if not isinstance(version, str) or version != RECIPE_CONTRIBUTION_VERSION:
        raise RecipeContributionValidationError(
            ((RECIPE_CONTRIBUTION_INVALID, "/version", "unsupported_version"),)
        )

    try:
        return _KIND_TO_MODEL[kind].model_validate(payload)
    except ValidationError as exc:
        raise RecipeContributionValidationError(
            tuple(
                (
                    RECIPE_CONTRIBUTION_INVALID,
                    _error_pointer(err["loc"], payload),
                    str(err["type"]),
                )
                for err in exc.errors()
            )
        ) from None


def validate_contribution_object(contribution: Any) -> Any:
    """Re-validate an ALREADY-INSTANTIATED contribution by dump-and-reparse.

    This is the ``model_construct`` guard. A registered executor returns objects,
    not dicts, and ``model_construct`` skips validation entirely — so the engine
    dumps whatever it received and runs it back through the full gate. A model
    built by fiat with a credential in it fails here exactly as a raw dict would.
    """
    if not isinstance(contribution, BaseModel):
        raise RecipeContributionValidationError(
            ((RECIPE_CONTRIBUTION_INVALID, "/", "not_a_contribution_model"),)
        )
    if type(contribution) not in _CONTRIBUTION_MEMBERS:
        raise RecipeContributionValidationError(
            ((RECIPE_CONTRIBUTION_INVALID, "/", "not_a_declared_contribution_type"),)
        )
    return parse_recipe_contribution(contribution.model_dump(mode="json"))


# ---------------------------------------------------------------------------
# Canonical serialization + schema
# ---------------------------------------------------------------------------


def _canonical_json(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=True)


def canonical_recipe_contribution_json(contribution: Any) -> str:
    """Canonical JSON: defaults and Nones included, keys sorted, list order kept.

    Same recipe as ``canonical_process_ir_json``. List order is SEMANTIC here —
    branch legs, contribution sequence, operation sequence — so it is never
    sorted; only mapping keys are.
    """
    return _canonical_json(contribution.model_dump(mode="json"))


def canonical_recipe_contributions_json(contributions: Any) -> str:
    """Canonical JSON for an ordered sequence of contributions."""
    return _canonical_json([c.model_dump(mode="json") for c in contributions])


def recipe_contribution_v1_json_schema() -> dict:
    """JSON Schema for the closed contribution union."""

    class _Envelope(BaseModel):
        model_config = ConfigDict(extra="forbid")

        contribution: RecipeContributionV1

    return _Envelope.model_json_schema()


def canonical_recipe_contribution_schema_json() -> str:
    return _canonical_json(recipe_contribution_v1_json_schema())


__all__ = [
    "RECIPE_COMPONENT_TYPES",
    "RECIPE_CONTRIBUTION_KINDS",
    "RECIPE_CONTRIBUTION_VERSION",
    "RECIPE_EXCLUDED_COMPONENT_TYPES",
    "AddTopologyObjectV1",
    "AddTopologyRelationV1",
    "AppendRootTerminalLegV1",
    "ComponentContributionV1",
    "ConstraintCheckV1",
    "ConstraintRequirementV1",
    "InsertRootLinearStepV1",
    "ProcessIRPatchOperationV1",
    "ProcessIRPatchV1",
    "ProcessUnitContributionV1",
    "RecipeComponentKey",
    "RecipeComponentType",
    "RecipeContributionV1",
    "RecipeContributionValidationError",
    "RecipeInvocationProvenanceV1",
    "RecipeRunDiagnosticV1",
    "RecipeSemanticId",
    "RecordedIntentContributionV1",
    "RequireCapabilityV1",
    "RequireComponentV1",
    "RequireProcessV1",
    "RequireTopologyObjectV1",
    "RequireTopologyRelationV1",
    "SetProcessRootV1",
    "SystemTopologyPatchOperationV1",
    "SystemTopologyPatchV1",
    "canonical_recipe_contribution_json",
    "canonical_recipe_contribution_schema_json",
    "canonical_recipe_contributions_json",
    "parse_recipe_contribution",
    "recipe_contribution_v1_json_schema",
    "scan_forbidden_recipe_shape",
    "validate_contribution_object",
]
