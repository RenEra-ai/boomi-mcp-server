"""The internal process materialization plan and its relocatable fingerprint.

Issue #153 (M12.15). This module holds the plan apply executes, and the
fingerprint that certifies it. Both are INTERNAL: the plan is apply's own record,
not a caller-authored shape, so it is deliberately absent from
``boomi_mcp.models.__all__`` — publishing it would advertise a surface no caller
may construct.

**Two attestations, and why this is only the first.** A pre-apply fingerprint
cannot certify emitted XML, because the XML does not exist yet in its final form:
compilation runs with PLACEHOLDER component ids, the real Boomi ids bind only
during ordered apply, and an update merges live preserved state that is not
knowable beforehand. So this fingerprint covers a RELOCATABLE plan — what would
be built, independent of which account builds it — and the concrete,
account-bound result is attested separately at apply time.

**What "relocatable" has to mean.** Two accounts, with different real component
ids and different folder ids, authoring the same logical process, must produce
BYTE-IDENTICAL canonical material. That is asserted on the bytes, not only on the
digest — a provider returning a constant would match on digests alone.

**Three structural guarantees, because the first draft of this module got each
of them wrong and an adversarial review caught all three.** They are recorded
here as mechanisms rather than intentions, because "we exclude the account-bound
fields" turned out to be a claim the code did not keep:

1. **Coverage is DERIVED, not hand-listed.** The canonical material is built by
   iterating the model's own fields minus :data:`EXCLUDED_PLAN_FIELDS`. The first
   draft hand-built a ten-key payload while *documenting* the exclusion set as
   the authority — so the set was dead documentation and a new field would have
   been silently uncovered. A new field is now covered BY DEFAULT, which fails
   loudly at the wave gate if it is account-bound, rather than silently.
2. **Literal component references are refused.**
   :data:`~boomi_mcp.models.process_ir.ComponentRefV1` admits a literal Boomi id
   as well as a ``$ref:KEY`` token, and the envelope's extension bindings carry
   one. The first draft covered those bindings wholesale, so a literal id made
   the plan non-relocatable while every test still passed — measured: the same
   logical process produced different bytes in two accounts.
3. **The emission plan is compiled HERE, with placeholders forced.** The first
   draft accepted a caller-supplied plan and merely assumed it was
   placeholder-backed; ``build_symbol_table`` exposes ``resolver`` publicly, so a
   real-id plan was one keyword away. The builder now rebuilds the symbol table
   with :func:`placeholder_component_id` before compiling, so an account-bound
   emission plan cannot be constructed at all.
"""

from __future__ import annotations

import dataclasses
import hashlib
import json
from typing import Any, Dict, Literal, Mapping, Optional, Tuple

from pydantic import (BaseModel, ConfigDict, Field, ValidationError,
                      field_validator,
                      model_validator)
from pydantic_core import PydanticCustomError

from ..compiler.process_ir.semantic_validation.contracts import (
    ProcessIRValidationCapabilitiesV1,
)
from ..models.authoring_workflow import DigestString
from ..models.process_component import ProcessComponentEnvelopeV1
from ..models.process_ir import ComponentRefV1
from ..models.process_ir import ProcessIRV1
from .revisions import canonical_json_bytes

#: The material's version IS the plan model's own ``version`` field, per the
#: design plan's exact canonical object — there is no separate wire version.
#: (An earlier draft carried both a ``version`` and a ``wire_version`` key; §6
#: review AR1-07 held the material to the plan's specified shape.)
PLAN_MATERIAL_WIRE_VERSION = "1"

_REF_PREFIX = "$ref:"

#: Envelope fields the fingerprint does NOT cover, with the reason. READ by
#: :func:`canonical_plan_material` — not decoration.
EXCLUDED_ENVELOPE_FIELDS: Mapping[str, str] = {
    "component_id": (
        "account-bound: the update target, or the server-assigned create result"
    ),
}

#: Plan fields the fingerprint does NOT cover, with the reason. READ by
#: :func:`canonical_plan_material`.
EXCLUDED_PLAN_FIELDS: Mapping[str, str] = {
    "resolved_folder_id": "account-bound: resolved from a folder NAME at apply time",
    "plan_fingerprint": "cannot cover itself",
}


class _PlanModel(BaseModel):
    """Strict, frozen, and repr-suppressed like every authored shape."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    def __repr_args__(self):  # noqa: D105 — pydantic hook
        for key, value in super().__repr_args__():
            if key in {"version", "execution_profile", "slot_id", "policy_id"}:
                yield key, value
            else:
                yield key, "..."


def _canonical_json_text(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=True)


class ProcessComponentSymbolSlotV1(_PlanModel):
    """One logical reference the plan must bind to a real component id at apply."""

    slot_id: str
    # The REFERENCE AUTHORITY, imported rather than respelled (§6 AR3-08). `str`
    # let `$ref:bad key` and `$ref:x ` (trailing space) into a slot, because the
    # exactness rule lived only in `ComponentRefV1` and this model carried a
    # hand-copy of the idea in prose. `_check_ref_is_logical` below now layers
    # the genuinely stricter literal-id refusal ON TOP of the authority instead
    # of standing in for it.
    ref: ComponentRefV1
    # REQUIRED per the plan; the default is kept only for construction
    # ergonomics and is VALIDATED, so it cannot be the one shape the field
    # validator never sees.
    expected_component_types: Tuple[str, ...] = Field(
        default=(), validate_default=True
    )

    @field_validator("expected_component_types")
    @classmethod
    def _expected_types_present(cls, value: Tuple[str, ...]) -> Tuple[str, ...]:
        if not value:
            raise PydanticCustomError(
                "process_materialization_plan_invalid",
                "expected_component_types is required on a symbol slot",
            )
        return value

    @field_validator("slot_id")
    @classmethod
    def _check_slot_id(cls, value: str) -> str:
        if not value or value != value.strip():
            raise PydanticCustomError(
                "process_materialization_plan_invalid",
                "slot_id must be non-blank and unpadded",
            )
        return value

    @field_validator("ref")
    @classmethod
    def _check_ref_is_logical(cls, value: str) -> str:
        """Exactly ``$ref:KEY`` — never a literal component id.

        Stricter than ``ComponentRefV1`` on purpose: a literal id is
        account-bound, so a slot carrying one would make the plan
        non-relocatable while still looking well-formed.
        """
        if not value.startswith(_REF_PREFIX) or not value[len(_REF_PREFIX) :].strip():
            raise PydanticCustomError(
                "process_materialization_reference_not_relocatable",
                "a materialization slot ref must be an exact '$ref:KEY' token, "
                "not a literal component id",
            )
        return value

    @field_validator("expected_component_types")
    @classmethod
    def _canonical_types(cls, value: Tuple[str, ...]) -> Tuple[str, ...]:
        canonical = tuple(sorted({t.strip() for t in value if t and t.strip()}))
        if not canonical:
            # Required NONEMPTY (plan §3): a slot that constrains nothing is an
            # inventory entry late binding cannot check anything against.
            raise PydanticCustomError(
                "process_materialization_plan_invalid",
                "expected_component_types must name at least one component type",
            )
        return canonical


class ProcessPreservationPolicyV1(_PlanModel):
    """The update-preservation policy this plan will execute under.

    A COMPLETE projection of the single runtime constant, carried as canonical
    JSON derived with :func:`dataclasses.asdict`. Complete by construction rather
    than by field list: the first draft hand-read two of ``PreservationPolicy``'s
    eight fields and one of ``OwnedPath``'s eleven, which meant two materially
    different preservation policies — differing in ``OwnedPath.mode``, the field
    that decides whether a subtree is REPLACED or merged, or in
    ``owned_encrypted_paths``, which decides whether live credentials survive —
    projected to byte-identical material and therefore to the same fingerprint.

    :func:`preservation_policy_v1` is the only supported way to build one.
    """

    policy_id: Literal["process.read_merge_write.v1"] = "process.read_merge_write.v1"
    #: The whole runtime policy, canonically serialized. Opaque on purpose: its
    #: job is to be COMPLETE and comparable, not to be re-read field by field.
    canonical_policy_json: str


def preservation_policy_v1() -> ProcessPreservationPolicyV1:
    """Project the ONE runtime preservation policy onto the plan, in full.

    Imported lazily: ``authoring`` must not import ``categories`` at module
    scope (the builders import authoring models, and the reverse at import time
    would cycle).
    """
    from ..categories.components.builders._process_preservation import (
        PROCESS_PRESERVATION_POLICY,
    )

    return ProcessPreservationPolicyV1(
        canonical_policy_json=_canonical_json_text(
            dataclasses.asdict(PROCESS_PRESERVATION_POLICY)
        )
    )


def _iter_envelope_refs(envelope: ProcessComponentEnvelopeV1):
    """Every component reference the ENVELOPE carries, with a JSON-pointer-ish path."""
    for index, connection in enumerate(envelope.process_extensions.connections):
        yield (
            "process_extensions/connections/{0}/connection_id".format(index),
            connection.connection_id,
        )


def iter_plan_component_refs(envelope: ProcessComponentEnvelopeV1, process_ir):
    """Every component reference the PLAN carries — envelope AND ProcessIR.

    THE single enumeration authority (§6 review, AR1-02/AR1-03). Two consumers
    need "every reference this plan binds": the relocatability rule, which must
    refuse a literal account id anywhere in covered material, and the symbol-slot
    inventory, which records what late binding must resolve. Giving each its own
    walk would be a second copy of one fact — the DC-3 class — so both chain off
    this one generator. The IR half is `iter_component_refs`, which yields only
    `ComponentRefV1`-annotated fields, so what counts as a reference is decided
    by the schema, never by what a string looks like.
    """
    from ..models.process_ir import iter_component_refs

    yield from _iter_envelope_refs(envelope)
    for path, ref in iter_component_refs(process_ir, "process_ir"):
        yield path.lstrip("/"), ref


def envelope_relocatability_offenders(
    envelope: ProcessComponentEnvelopeV1,
    process_ir=None,
) -> Tuple[str, ...]:
    """Plan paths carrying a LITERAL component id instead of ``$ref:KEY``.

    The single authority for the relocatability rule — refused by the plan model
    at apply, and reported by ``plan``/``compile`` before anything is written
    (QA-153-r2-07).

    **Covers the ProcessIR as well as the envelope** (§6 review, AR1-02). The
    first version walked only the envelope's extension bindings, while the
    canonical material covers ``process_ir`` wholesale — so a validated,
    self-consistent plan could carry a literal account id in its canonical
    bytes through any `ComponentRefV1` field of the IR, and the same logical
    process fingerprinted differently in two accounts. That is the exact
    violation the fingerprint exists to make unrepresentable, and the plan text
    mandates the model-level refusal for "materializable ProcessIR or extension
    references" alike. `process_ir=None` keeps plan-surface callers that only
    have an envelope working; the plan model always passes both.
    """
    refs = (
        iter_plan_component_refs(envelope, process_ir)
        if process_ir is not None
        else _iter_envelope_refs(envelope)
    )
    return tuple(path for path, ref in refs if not ref.startswith(_REF_PREFIX))


class ProcessComponentMaterializationPlanV1(_PlanModel):
    """Everything apply needs to materialize ONE process root, plus its fingerprint.

    ``plan_fingerprint`` is STORED rather than computed on demand so ordered
    apply can verify, before mutating anything, that the plan it is about to
    execute is the plan that was compiled. The model re-derives it and refuses a
    mismatch.
    """

    version: Literal["1"] = "1"
    envelope: ProcessComponentEnvelopeV1
    process_ir: ProcessIRV1
    #: Canonical JSON TEXT, not a mapping. A ``Mapping`` field is mutable even on
    #: a frozen model — measured: a single in-place write desynchronized the
    #: stored fingerprint from the plan's own material after validation, which
    #: defeats the tamper-evidence the stored digest exists to provide.
    emission_plan_canonical_json: str
    #: The default is KEPT (§6 evaluation 4 asked whether it should be). It used
    #: to mean "nobody passed slots", which is what let an empty inventory look
    #: valid; with `_slots_agree_with_the_references_they_inventory` below it can
    #: only mean what it says — a plan whose material contains no symbolic
    #: reference. Making the field required would refuse that plan for no reason
    #: while closing nothing the validator does not already close.
    unresolved_symbol_slots: Tuple[ProcessComponentSymbolSlotV1, ...] = ()
    execution_profile: Literal["scheduled", "listener"]
    conflict_policy: Literal["reuse", "clone", "fail"]
    preservation_policy: ProcessPreservationPolicyV1
    # The DIGEST AUTHORITY, imported rather than respelled (§6 AR3-09): plain
    # `str` accepted an empty or malformed revision on a self-consistent plan,
    # while every producer already emits a digest. A second copy of the digest
    # spelling would be the same hand-model class this slice has been closing.
    compiler_revision: DigestString
    emitter_revision: DigestString
    materializer_revision: DigestString
    #: The server-built trusted context this plan was COMPILED under (#180).
    #:
    #: Apply re-compiles `process_ir` against real ids, and that recompile is a
    #: compile entry like any other: without the same context it asks a
    #: different question than the compile that certified this plan. A root
    #: whose declaration turned a blocking finding into a warning planned
    #: clean, compiled clean, and then failed at materialization — the effect
    #: channel simply did not reach apply.
    #:
    #: The context travels ON the plan rather than being handed to each
    #: recompile site, so a future site cannot be strict by forgetting to pass
    #: it. It is COVERED by the fingerprint: a plan compiled under a declaration
    #: is not the same plan as one compiled without, and the digest must say so.
    #: Nothing a caller sends reaches it — the resolver derives it server-side.
    effect_capabilities: Optional[ProcessIRValidationCapabilitiesV1] = None
    resolved_folder_id: Optional[str] = None
    plan_fingerprint: str

    @field_validator("unresolved_symbol_slots")
    @classmethod
    def _slots_are_unique_and_ordered(
        cls, value: Tuple[ProcessComponentSymbolSlotV1, ...]
    ) -> Tuple[ProcessComponentSymbolSlotV1, ...]:
        ids = [slot.slot_id for slot in value]
        duplicated = sorted({i for i in ids if ids.count(i) > 1})
        if duplicated:
            raise PydanticCustomError(
                "process_materialization_plan_invalid",
                "duplicate symbol slot_id(s): {ids}",
                {"ids": ", ".join(duplicated)},
            )
        return tuple(sorted(value, key=lambda slot: slot.slot_id))

    @model_validator(mode="after")
    def _envelope_references_are_relocatable(
        self,
    ) -> "ProcessComponentMaterializationPlanV1":
        """No literal component id may reach the covered material.

        ``ComponentRefV1`` admits a literal Boomi id, and the envelope's
        extension bindings are COVERED by the fingerprint — so a literal id makes
        the plan non-relocatable while looking perfectly well-formed. Measured on
        the first draft: the same logical process produced different bytes under
        two account identities. Refused here rather than excluded, because
        dropping the bindings from coverage would stop a real override change
        from moving the fingerprint.
        """
        offenders = envelope_relocatability_offenders(self.envelope, self.process_ir)
        if offenders:
            raise _not_relocatable_custom_error(offenders)
        return self

    @model_validator(mode="after")
    def _slots_agree_with_the_references_they_inventory(
        self,
    ) -> "ProcessComponentMaterializationPlanV1":
        """The slot inventory must BE the plan's reference set, not resemble it.

        §6 evaluation 4: the field defaults to empty and every other validator
        checks the slots against themselves — ordering, uniqueness, and a
        fingerprint recomputed over the slots as recorded. Measured: a plan
        rebuilt with zero slots and its own correctly recomputed fingerprint
        validated cleanly, and apply then trusts that inventory to decide what
        late binding must resolve. Production derives the slots correctly, so
        this is a missing guard rather than a live defect — but "the producer
        gets it right" is precisely the claim a model exists to stop having to
        make.

        The comparison walks the SAME enumeration `derive_symbol_slots` does
        and filters literal ids the same way, so the two cannot drift into
        disagreeing about what a reference is. That shared filter is also what
        keeps a literal id out of this refusal entirely — both sides skip it, so
        it can only ever be the relocatability rule's finding, independently of
        which validator runs first (measured: reordering the two changes
        nothing).
        """
        expected = {
            (path, ref)
            for path, ref in iter_plan_component_refs(self.envelope, self.process_ir)
            if ref.startswith(_REF_PREFIX)
        }
        recorded = {(slot.slot_id, str(slot.ref)) for slot in self.unresolved_symbol_slots}
        if recorded != expected:
            missing = sorted(path for path, _ref in expected - recorded)
            extra = sorted(path for path, _ref in recorded - expected)
            raise PydanticCustomError(
                "process_materialization_plan_invalid",
                "symbol slots disagree with the plan's references "
                "(uninventoried: {missing}; unreferenced: {extra})",
                {
                    "missing": ", ".join(missing) or "none",
                    "extra": ", ".join(extra) or "none",
                },
            )
        return self

    @model_validator(mode="after")
    def _fingerprint_matches_its_own_material(
        self,
    ) -> "ProcessComponentMaterializationPlanV1":
        expected, _material = _fingerprint_of(self)
        if self.plan_fingerprint != expected:
            raise PydanticCustomError(
                "process_materialization_fingerprint_mismatch",
                "plan_fingerprint does not match the plan's canonical material",
            )
        return self


def covered_plan_fields() -> Tuple[str, ...]:
    """The plan fields the fingerprint covers — DERIVED from the model.

    The single authority for coverage. A field added to the model is covered
    automatically unless it is named in :data:`EXCLUDED_PLAN_FIELDS`, so the
    exclusion set is load-bearing rather than documentation.
    """
    return tuple(
        name
        for name in ProcessComponentMaterializationPlanV1.model_fields
        if name not in EXCLUDED_PLAN_FIELDS
    )


def _canonical_capabilities(capabilities: Any) -> Any:
    """The trusted context in an ORDER-INDEPENDENT shape, for hashing only.

    #180 Stage-2 P2. The resolver preserves the caller's authored order in these
    collections, while ``ProcessIREffectDeclarationsV1.canonical_payload()``
    deliberately makes that order hash-independent — two requests that declare
    the same external writers in a different order are the same request. Once
    this context became covered plan material, dumping it as-is reintroduced the
    dependency one layer down: swapping two equivalent declarations moved the
    plan fingerprint and therefore the compile hash, forcing a needless
    stale-binding replan.

    Only the MATERIAL is canonicalized. The stored field keeps the resolver's
    own ordering, exactly as the parsed request keeps the caller's — lineage
    looks a contract up by ref, never by position, so the two cannot disagree.

    Sorting by each row's canonical bytes gives a total order that does not
    depend on Python's own comparison rules for the row models.
    """
    dumped = capabilities.model_dump(mode="json")
    return {
        key: (sorted(value, key=canonical_json_bytes)
              if isinstance(value, list) else value)
        for key, value in dumped.items()
    }


def _dump(value: Any) -> Any:
    if isinstance(value, BaseModel):
        return value.model_dump(mode="json")
    if isinstance(value, tuple):
        return [_dump(item) for item in value]
    return value


def canonical_plan_material(plan: "ProcessComponentMaterializationPlanV1") -> bytes:
    """The exact bytes the fingerprint is taken over — the plan's SPECIFIED shape.

    Built by walking :func:`covered_plan_fields`, so COVERAGE cannot drift from
    the model — and each walked field is then mapped into the design plan's
    exact canonical object (§6 review AR1-07): ``version`` from the model field,
    the emission plan as a NESTED object rather than a double-serialized string,
    and ``policies`` / ``revisions`` as the plan's nested groups rather than
    flattened keys. Storage stays as it is (the JSON-string fields are the
    recorded S5-03/S5-05 deviations); only the hashed MATERIAL takes the
    specified layout. Returned as bytes so callers compare what was HASHED.
    """
    payload: Dict[str, Any] = {
        "kind": "process_component_materialization_plan",
        "policies": {},
        "revisions": {},
    }
    for name in covered_plan_fields():
        value = getattr(plan, name)
        if name == "envelope":
            envelope = value.model_dump(mode="json")
            for excluded in EXCLUDED_ENVELOPE_FIELDS:
                envelope.pop(excluded, None)
            payload[name] = envelope
        elif name == "emission_plan_canonical_json":
            # The stored field is canonical TEXT (S5-05: immutable on a frozen
            # model); the MATERIAL nests the object itself, per the plan.
            payload["emission_plan"] = json.loads(value)
        elif name == "conflict_policy":
            payload["policies"]["conflict_policy"] = value
        elif name == "preservation_policy":
            # The plan's canonical object places the FULL normalized projection
            # directly at `policies.preservation_policy` (§3 L252-255). The
            # first pass wrapped it in a `projection` key — a wire level the
            # spec does not have and neither recorded storage deviation
            # justifies (§6 AR2-05). `policy_id` rides alongside the projected
            # fields as a sibling.
            payload["policies"]["preservation_policy"] = {
                "policy_id": value.policy_id,
                **json.loads(value.canonical_policy_json),
            }
        elif name == "effect_capabilities":
            # Order-independent, per the note on `_canonical_capabilities`.
            payload[name] = (
                None if value is None else _canonical_capabilities(value)
            )
        elif name in ("compiler_revision", "emitter_revision", "materializer_revision"):
            payload["revisions"][name] = value
        else:
            payload[name] = _dump(value)
    return canonical_json_bytes(payload)


def _fingerprint_of(plan: "ProcessComponentMaterializationPlanV1") -> Tuple[str, bytes]:
    material = canonical_plan_material(plan)
    return "sha256:" + hashlib.sha256(material).hexdigest(), material


def process_plan_fingerprint(
    plan: "ProcessComponentMaterializationPlanV1",
) -> Tuple[str, bytes]:
    """``(digest, canonical_material)`` — the ONE way to compute it.

    Both values come from one function on purpose: the wave gate RECOMPUTES the
    digest from the returned bytes, so a second code path deriving the digest
    independently could drift from the material it claims to describe.
    """
    return _fingerprint_of(plan)


def placeholder_backed_symbols(symbols):
    """A symbol table with every ``component_id`` forced to its placeholder.

    The structural guarantee behind relocatability. ``build_symbol_table``
    exposes ``resolver`` publicly and the legacy arm already passes a real one,
    so accepting a caller's table verbatim would let real account ids reach the
    covered emission plan. Rebuilding the table here makes that unrepresentable
    rather than merely discouraged.
    """
    from ..compiler.process_ir.contracts import SymbolTableV1
    from ..recipes.materialization import placeholder_component_id

    # Every OTHER field is carried by ``rebinding``, derived from the model's own
    # field set. Enumerating them here is what would drop a new one silently.
    return SymbolTableV1.rebinding(
        symbols,
        (
            symbol.model_copy(
                update={"component_id": placeholder_component_id(symbol.ref)}
            )
            for symbol in symbols.symbols
        ),
    )


def derive_symbol_slots(
    envelope: ProcessComponentEnvelopeV1, process_ir: ProcessIRV1, symbols
) -> Tuple[ProcessComponentSymbolSlotV1, ...]:
    """The plan's unresolved-symbol-slot inventory, DERIVED — never authored.

    §6 review AR1-03: the slot layer was modelled, fingerprinted, and never
    populated — both production callers passed nothing, so every plan recorded
    ``unresolved_symbol_slots == ()`` while its emission plan was full of
    placeholders, and late binding had no recorded inventory to check against.

    One slot per reference OCCURRENCE, from the same enumeration the
    relocatability rule reads (`iter_plan_component_refs`): ``slot_id`` is the
    stable source pointer and ``ref`` the ``$ref:KEY`` token.

    ``expected_component_types`` records the symbol table's own type for that
    key — the compiler's authority, not a re-derivation.

    QA-153-r15-02 observed that this makes the field non-discriminating: it
    records whatever the key resolved to, so it can never disagree with
    anything. A ROLE rule was drafted here — a `connection_ref` must name a
    connection, an `operation_ref` an operation — and WITHDRAWN on measurement:
    the compiler applies that constraint only to `connector_call` nodes, while
    `source`/`send` steps legitimately carry other operation spellings (the
    `_m12_11_support` fixture's `connector-operation` compiles by design, and
    the drafted rule refused it). A plan-layer copy of a rule the compiler
    scopes differently is a hand-model that refuses valid requests — the defect
    class this slice exists to close, not an instance of the fix.

    The wrong-kind refusal is enforced where it already lives, by running the
    emitter: the apply's pre-write pass emits the plan DRY, so an emitter
    refusal is decided before any dependency is written.
    """
    by_ref = {symbol.ref: symbol for symbol in symbols.symbols}
    slots = []
    for path, ref in iter_plan_component_refs(envelope, process_ir):
        if not ref.startswith(_REF_PREFIX):
            continue  # a literal id is the relocatability validator's finding
        symbol = by_ref.get(ref)
        if symbol is None:
            raise PydanticCustomError(
                "process_materialization_plan_invalid",
                "reference {ref} at {path} names no symbol in the compile table",
                {"ref": ref, "path": path},
            )
        slots.append(
            ProcessComponentSymbolSlotV1(
                slot_id=path,
                ref=ref,
                expected_component_types=(symbol.component_type,),
            )
        )
    return tuple(slots)


def _not_relocatable_custom_error(offenders) -> PydanticCustomError:
    """The ONE refusal for a non-relocatable reference (§6 AR2-02).

    Two callers raise it — the model validator, which needs a bare
    ``PydanticCustomError`` for pydantic to wrap, and the pre-compile check in
    :func:`build_materialization_plan`, which needs a real ``ValidationError``
    so the apply arm's named-code map recognizes it. Both get the same type
    string and the same words from here rather than each spelling its own.
    """
    return PydanticCustomError(
        "process_materialization_reference_not_relocatable",
        "literal component id(s) at {paths}; a materializable plan may "
        "carry only '$ref:KEY' tokens — reference an existing component "
        "by logical key in the component plan instead",
        {"paths": ", ".join(offenders)},
    )


def _not_relocatable_error(offenders) -> ValidationError:
    """The same refusal as a `ValidationError`, for the pre-compile check."""
    return ValidationError.from_exception_data(
        "ProcessComponentMaterializationPlanV1",
        [{"type": _not_relocatable_custom_error(offenders),
          "loc": ("process_ir",), "input": None}],
    )


def build_materialization_plan(
    *,
    envelope: ProcessComponentEnvelopeV1,
    process_ir: ProcessIRV1,
    symbols,
    conflict_policy: str,
    compiler_revision: str,
    emitter_revision: str,
    materializer_revision: str,
    resolved_folder_id: Optional[str] = None,
    capabilities=None,
) -> ProcessComponentMaterializationPlanV1:
    """Compile, derive, and fingerprint ONE root — the only supported constructor.

    Owning compilation is what makes the emission plan provably placeholder-backed
    (see the module docstring). The execution profile is derived here for the same
    reason: a caller-supplied profile could contradict the graph — and the symbol
    SLOTS are derived here for the same reason again: a caller-supplied inventory
    could disagree with the references the plan actually binds (AR1-03).
    """
    from ..compiler.process_ir.contracts import canonical_emission_plan_json
    from ..compiler.process_ir.execution_profile import (
        derive_process_execution_profile,
    )
    from ..compiler.process_ir.pipeline import compile_process_ir_v1

    # RELOCATABILITY IS DECIDED BEFORE COMPILATION (§6 AR2-02). The model
    # validator below already refuses a literal component id, but it runs only
    # once the plan object is CONSTRUCTED — after `compile_process_ir_v1`, which
    # for a literal reference inside the IR dies first with a compile error and
    # is served as an internal error rather than the named, documented code. The
    # predicate is the same one the validator calls: one authority, two call
    # sites, so a second opinion cannot drift from the first.
    offenders = envelope_relocatability_offenders(envelope, process_ir)
    if offenders:
        raise _not_relocatable_error(offenders)

    relocatable_symbols = placeholder_backed_symbols(symbols)
    # #154 (QA-154-r1-01). This compile was strict, which made the effect channel
    # worse than useless: a declaration whose entire purpose is to turn a
    # blocking finding into a warning validated clean at plan and then failed
    # here, so a caller who used the feature as documented was strictly worse off
    # than one who omitted it.
    #
    # #180 (SELF-180-02): this comment used to COUNT the compiles — "the third,
    # and the only one still strict" — and the count was wrong. There was a
    # fourth, the apply-time recompile in `materialize_canonical_process_xml`,
    # and it stayed strict for exactly as long as the enumeration was believed.
    # The authority is now `tests/test_issue_180_compile_entry_context.py`, which
    # derives the sites from the source instead of remembering them.
    #
    # The context is INTERNAL and server-built — the same object
    # `_validate_processes` resolved. Nothing a caller sends reaches it, so this
    # is not a new trust surface; it is the existing one reaching the last site
    # that needed it. `None` keeps the strict default rather than overriding it.
    cfg, emission_plan = (
        compile_process_ir_v1(process_ir, relocatable_symbols, capabilities=capabilities)
        if capabilities is not None
        else compile_process_ir_v1(process_ir, relocatable_symbols)
    )

    covered: Dict[str, Any] = dict(
        envelope=envelope,
        process_ir=process_ir,
        emission_plan_canonical_json=canonical_emission_plan_json(emission_plan),
        # DERIVED, and sorted before the digest is taken (the first draft hashed
        # a `model_construct`ed provisional plan, skipping the canonicalizing
        # validators entirely).
        unresolved_symbol_slots=tuple(
            sorted(
                derive_symbol_slots(envelope, process_ir, relocatable_symbols),
                key=lambda slot: slot.slot_id,
            )
        ),
        execution_profile=derive_process_execution_profile(cfg, relocatable_symbols),
        conflict_policy=conflict_policy,
        preservation_policy=preservation_policy_v1(),
        compiler_revision=compiler_revision,
        emitter_revision=emitter_revision,
        materializer_revision=materializer_revision,
        # #180: RECORDED, so the apply-time recompile asks the same question
        # this compile just answered. Storing it here rather than at each
        # recompile site is what makes the property structural.
        effect_capabilities=capabilities,
        resolved_folder_id=resolved_folder_id,
    )

    provisional = ProcessComponentMaterializationPlanV1.model_construct(
        plan_fingerprint="sha256:" + "0" * 64, **covered
    )
    digest, _material = _fingerprint_of(provisional)
    return ProcessComponentMaterializationPlanV1(plan_fingerprint=digest, **covered)


__all__ = [
    "EXCLUDED_ENVELOPE_FIELDS",
    "EXCLUDED_PLAN_FIELDS",
    "PLAN_MATERIAL_WIRE_VERSION",
    "ProcessComponentMaterializationPlanV1",
    "ProcessComponentSymbolSlotV1",
    "ProcessPreservationPolicyV1",
    "build_materialization_plan",
    "canonical_plan_material",
    "covered_plan_fields",
    "derive_symbol_slots",
    "envelope_relocatability_offenders",
    "iter_plan_component_refs",
    "placeholder_backed_symbols",
    "preservation_policy_v1",
    "process_plan_fingerprint",
]
