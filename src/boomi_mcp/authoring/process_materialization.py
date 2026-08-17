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

from pydantic import BaseModel, ConfigDict, field_validator, model_validator
from pydantic_core import PydanticCustomError

from ..models.process_component import ProcessComponentEnvelopeV1
from ..models.process_ir import ProcessIRV1
from .revisions import canonical_json_bytes

#: Wire version of the CANONICAL MATERIAL. Bumped by hand only when the
#: material's SHAPE changes. It is covered by the material itself, so an old plan
#: and a new plan can never collide even if every other covered value matches.
PLAN_MATERIAL_WIRE_VERSION = "2"

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
    ref: str
    expected_component_types: Tuple[str, ...] = ()

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
        return tuple(sorted({t.strip() for t in value if t and t.strip()}))


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
    unresolved_symbol_slots: Tuple[ProcessComponentSymbolSlotV1, ...] = ()
    execution_profile: Literal["scheduled", "listener"]
    conflict_policy: Literal["reuse", "clone", "fail"]
    preservation_policy: ProcessPreservationPolicyV1
    compiler_revision: str
    emitter_revision: str
    materializer_revision: str
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
        offenders = [
            path
            for path, ref in _iter_envelope_refs(self.envelope)
            if not ref.startswith(_REF_PREFIX)
        ]
        if offenders:
            raise PydanticCustomError(
                "process_materialization_reference_not_relocatable",
                "literal component id(s) at {paths}; a materializable plan may "
                "carry only '$ref:KEY' tokens — reference an existing component "
                "by logical key in the component plan instead",
                {"paths": ", ".join(offenders)},
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


def _dump(value: Any) -> Any:
    if isinstance(value, BaseModel):
        return value.model_dump(mode="json")
    if isinstance(value, tuple):
        return [_dump(item) for item in value]
    return value


def canonical_plan_material(plan: "ProcessComponentMaterializationPlanV1") -> bytes:
    """The exact bytes the fingerprint is taken over.

    Built by walking :func:`covered_plan_fields`, so the material cannot drift
    from the model. Returned as bytes rather than a dict so callers compare what
    was HASHED, not a re-serialization of it.
    """
    payload: Dict[str, Any] = {
        "kind": "process_component_materialization_plan",
        "wire_version": PLAN_MATERIAL_WIRE_VERSION,
    }
    for name in covered_plan_fields():
        value = getattr(plan, name)
        if name == "envelope":
            envelope = value.model_dump(mode="json")
            for excluded in EXCLUDED_ENVELOPE_FIELDS:
                envelope.pop(excluded, None)
            payload[name] = envelope
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

    return SymbolTableV1(
        symbols=tuple(
            symbol.model_copy(
                update={"component_id": placeholder_component_id(symbol.ref)}
            )
            for symbol in symbols.symbols
        ),
        idempotency_contracts=symbols.idempotency_contracts,
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
    unresolved_symbol_slots: Tuple[ProcessComponentSymbolSlotV1, ...] = (),
    resolved_folder_id: Optional[str] = None,
) -> ProcessComponentMaterializationPlanV1:
    """Compile, derive, and fingerprint ONE root — the only supported constructor.

    Owning compilation is what makes the emission plan provably placeholder-backed
    (see the module docstring). The execution profile is derived here for the same
    reason: a caller-supplied profile could contradict the graph.
    """
    from ..compiler.process_ir.contracts import canonical_emission_plan_json
    from ..compiler.process_ir.execution_profile import (
        derive_process_execution_profile,
    )
    from ..compiler.process_ir.pipeline import compile_process_ir_v1

    relocatable_symbols = placeholder_backed_symbols(symbols)
    cfg, emission_plan = compile_process_ir_v1(process_ir, relocatable_symbols)

    covered: Dict[str, Any] = dict(
        envelope=envelope,
        process_ir=process_ir,
        emission_plan_canonical_json=canonical_emission_plan_json(emission_plan),
        # Sorted BEFORE the digest is taken. The first draft computed the digest
        # from a `model_construct`ed provisional plan, which skips validators —
        # so the canonicalizing sort never ran on the hashed bytes and a plan with
        # unsorted slots was impossible to construct.
        unresolved_symbol_slots=tuple(
            sorted(unresolved_symbol_slots, key=lambda slot: slot.slot_id)
        ),
        execution_profile=derive_process_execution_profile(cfg, relocatable_symbols),
        conflict_policy=conflict_policy,
        preservation_policy=preservation_policy_v1(),
        compiler_revision=compiler_revision,
        emitter_revision=emitter_revision,
        materializer_revision=materializer_revision,
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
    "placeholder_backed_symbols",
    "preservation_policy_v1",
    "process_plan_fingerprint",
]
