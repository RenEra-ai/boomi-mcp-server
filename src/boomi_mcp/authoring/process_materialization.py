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
be built, independent of which account builds it — and the concrete, account-bound
result is attested separately at apply time. Collapsing them into one is the
failure mode the issue explicitly names.

**What "relocatable" has to mean.** Two different accounts, with different real
component ids and different folder ids, authoring the same logical process, must
produce BYTE-IDENTICAL canonical material. That is asserted on the bytes, not
only on the digest — a provider returning a constant would match on digests
alone. The account-bound fields are therefore excluded by construction rather
than by hoping they never appear:

* ``component_id`` — the update target, or the server-assigned create result.
* ``resolved_folder_id`` — resolved from a folder NAME at apply time.
* the fingerprint field itself, which cannot cover itself.

Everything else is covered: the normalized envelope, the normalized ProcessIR,
the placeholder-backed emission plan, the unresolved symbol slots, the
compiler-derived execution profile, the conflict and preservation policies, and
the recorded behaviour revisions.

**Why literal component references are refused here.**
:data:`~boomi_mcp.models.process_ir.ComponentRefV1` admits either a ``$ref:KEY``
token or a literal Boomi component id. A literal id is an ACCOUNT-BOUND value, so
a root carrying one cannot have a relocatable plan at all — the same logical
process would fingerprint differently in a second account, silently. Rather than
let that produce a plan whose central promise is false, materialization planning
refuses it with ``PROCESS_MATERIALIZATION_REFERENCE_NOT_RELOCATABLE`` and points
the caller at the component plan, where an existing component is referenced by
logical key.
"""

from __future__ import annotations

import hashlib
from typing import Any, Dict, Literal, Mapping, Optional, Tuple

from pydantic import BaseModel, ConfigDict, field_validator, model_validator
from pydantic_core import PydanticCustomError

from ..models.process_component import ProcessComponentEnvelopeV1
from ..models.process_ir import ProcessIRV1
from .revisions import canonical_json_bytes

#: Wire version of the CANONICAL MATERIAL below. Bumped by hand only when the
#: material's SHAPE changes. It is covered by the material itself, so an old plan
#: and a new plan can never collide even if every other covered value matches.
PLAN_MATERIAL_WIRE_VERSION = "1"

#: The reference prefix that makes a reference logical rather than account-bound.
_REF_PREFIX = "$ref:"

#: Envelope fields the fingerprint deliberately does NOT cover, with the reason.
#: Named as data so the coverage-accounting test can read them instead of
#: re-listing them — a second hand-written copy of this set is exactly how a new
#: field ends up silently uncovered.
EXCLUDED_ENVELOPE_FIELDS: Mapping[str, str] = {
    "component_id": (
        "account-bound: the update target, or the server-assigned create result"
    ),
}

#: Plan fields excluded from the material, with the reason.
EXCLUDED_PLAN_FIELDS: Mapping[str, str] = {
    "resolved_folder_id": "account-bound: resolved from a folder NAME at apply time",
    "plan_fingerprint": "cannot cover itself",
}


class _PlanModel(BaseModel):
    """Strict, frozen, tuple-only — and repr-suppressed like every authored shape."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    def __repr_args__(self):  # noqa: D105 — pydantic hook
        for key, value in super().__repr_args__():
            if key in {"version", "execution_profile", "slot_id", "policy_id"}:
                yield key, value
            else:
                yield key, "..."


class ProcessComponentSymbolSlotV1(_PlanModel):
    """One logical reference the plan must bind to a real component id at apply.

    Recorded so ordered apply can verify, BEFORE mutating anything, that every
    reference this root needs is already resolvable — rather than discovering a
    missing dependency halfway through emitting XML.
    """

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

        Stricter than ``ComponentRefV1`` on purpose. A literal id is
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

    A PROJECTION of the single runtime constant
    (``builders._process_preservation.PROCESS_PRESERVATION_POLICY``), never an
    independent restatement of it: two descriptions of one preservation rule
    could disagree, and the disagreement would be a structured update that
    discards live state the plan promised to keep. :func:`preservation_policy_v1`
    is the only supported way to build one.
    """

    policy_id: Literal["process.read_merge_write.v1"] = "process.read_merge_write.v1"
    component_type: Literal["process"] = "process"
    owned_root_attributes: Tuple[str, ...]
    owned_paths: Tuple[str, ...]


def preservation_policy_v1() -> ProcessPreservationPolicyV1:
    """Project the ONE runtime preservation policy onto the plan.

    Imported lazily: ``authoring`` must not import ``categories`` at module
    scope (the builders import authoring models, and the reverse at import time
    would cycle). The same lazy-import discipline the rest of this package uses.

    Every field is READ from the runtime constant. Nothing is restated, so a
    change to the policy reaches the plan automatically instead of leaving the
    plan describing a rule the builders no longer follow.
    """
    from ..categories.components.builders._process_preservation import (
        PROCESS_PRESERVATION_POLICY,
    )

    policy = PROCESS_PRESERVATION_POLICY
    return ProcessPreservationPolicyV1(
        owned_root_attributes=tuple(policy.owned_root_attrs),
        owned_paths=tuple(owned.path for owned in policy.owned_paths),
    )


class ProcessComponentMaterializationPlanV1(_PlanModel):
    """Everything apply needs to materialize ONE process root, plus its fingerprint.

    Internal by design — see the module docstring. The ``plan_fingerprint`` is
    STORED rather than computed on demand so ordered apply can verify, before
    mutating anything, that the plan it is about to execute is the plan that was
    compiled. The model re-derives it on construction and refuses a mismatch, so
    a hand-assembled plan carrying someone else's fingerprint cannot exist.
    """

    version: Literal["1"] = "1"
    envelope: ProcessComponentEnvelopeV1
    process_ir: ProcessIRV1
    emission_plan: Mapping[str, Any]
    unresolved_symbol_slots: Tuple[ProcessComponentSymbolSlotV1, ...] = ()
    execution_profile: Literal["scheduled", "listener"]
    conflict_policy: Literal["reuse", "clone", "fail"]
    preservation_policy: ProcessPreservationPolicyV1
    compiler_revision: str
    emitter_revision: str
    materializer_revision: str
    #: Account-bound apply overlay. Excluded from the canonical material — see
    #: :data:`EXCLUDED_PLAN_FIELDS`.
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
        # Canonical ORDER, so the caller's iteration order cannot reach the
        # fingerprint. The slots are a set of requirements, not a sequence.
        return tuple(sorted(value, key=lambda slot: slot.slot_id))

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


def canonical_plan_material(plan: "ProcessComponentMaterializationPlanV1") -> bytes:
    """The exact bytes the fingerprint is taken over.

    Returned as bytes rather than a dict so callers compare what was HASHED, not
    a re-serialization of it. The wave gate compares these bytes across two
    synthetic account identities; if they differ, the plan is not relocatable
    even when the digests happen to agree.
    """
    envelope = plan.envelope.model_dump(mode="json")
    for excluded in EXCLUDED_ENVELOPE_FIELDS:
        envelope.pop(excluded, None)

    payload: Dict[str, Any] = {
        "kind": "process_component_materialization_plan",
        "wire_version": PLAN_MATERIAL_WIRE_VERSION,
        "version": plan.version,
        "envelope": envelope,
        "process_ir": plan.process_ir.model_dump(mode="json"),
        "emission_plan": dict(plan.emission_plan),
        "unresolved_symbol_slots": [
            slot.model_dump(mode="json") for slot in plan.unresolved_symbol_slots
        ],
        "execution_profile": plan.execution_profile,
        "policies": {
            "conflict_policy": plan.conflict_policy,
            "preservation_policy": plan.preservation_policy.model_dump(mode="json"),
        },
        "revisions": {
            "compiler_revision": plan.compiler_revision,
            "emitter_revision": plan.emitter_revision,
            "materializer_revision": plan.materializer_revision,
        },
    }
    return canonical_json_bytes(payload)


def _fingerprint_of(plan: "ProcessComponentMaterializationPlanV1") -> Tuple[str, bytes]:
    material = canonical_plan_material(plan)
    return "sha256:" + hashlib.sha256(material).hexdigest(), material


def process_plan_fingerprint(
    plan: "ProcessComponentMaterializationPlanV1",
) -> Tuple[str, bytes]:
    """``(digest, canonical_material)`` for a plan — the ONE way to compute it.

    Both values come from one function on purpose. The wave gate RECOMPUTES the
    digest from the returned bytes and refuses a mismatch, so a second code path
    that derived the digest independently could drift from the material it
    claims to describe and would be caught only there. One function makes the
    drift unrepresentable.
    """
    return _fingerprint_of(plan)


def build_plan_fingerprint_fields(**covered: Any) -> str:
    """Compute the fingerprint for a plan that does not exist yet.

    The plan stores its own fingerprint and validates it, which is circular at
    construction time: the value is needed before the model can be built. This
    resolves it by constructing the model with a placeholder digest through
    ``model_construct`` (no validation), taking the material, and returning the
    real digest for the caller to pass in.
    """
    provisional = ProcessComponentMaterializationPlanV1.model_construct(
        plan_fingerprint="sha256:" + "0" * 64, **covered
    )
    digest, _material = _fingerprint_of(provisional)
    return digest


__all__ = [
    "EXCLUDED_ENVELOPE_FIELDS",
    "EXCLUDED_PLAN_FIELDS",
    "PLAN_MATERIAL_WIRE_VERSION",
    "ProcessComponentMaterializationPlanV1",
    "ProcessComponentSymbolSlotV1",
    "ProcessPreservationPolicyV1",
    "build_plan_fingerprint_fields",
    "canonical_plan_material",
    "preservation_policy_v1",
    "process_plan_fingerprint",
]
