"""The relocatable materialization plan and its fingerprint (issue #153).

Every test here exists because an adversarial review REFUTED an earlier draft of
this module. The draft's docstring claimed the account-bound fields were
"excluded by construction rather than by hoping they never appear"; measured, the
same logical process produced different bytes under two account identities. Each
property below is therefore asserted as a case the design excludes, not as a
restatement of the design's intent.
"""

import copy
import dataclasses
import json
import sys
from pathlib import Path

import pytest
from pydantic import ValidationError

_src = str(Path(__file__).resolve().parent.parent / "src")
if _src not in sys.path:
    sys.path.insert(0, _src)
sys.path.insert(0, str(Path(__file__).resolve().parent))

from boomi_mcp.authoring import process_materialization as pm  # noqa: E402
from boomi_mcp.authoring.contract import get_authoring_revisions  # noqa: E402
from boomi_mcp.compiler.process_ir.emitter_registry import (  # noqa: E402
    emitter_revision,
)
from boomi_mcp.models.process_component import (  # noqa: E402
    ProcessComponentEnvelopeV1,
    ProcessConnectionOverrideV1,
    ProcessExtensionBindingsV1,
    ProcessOverrideFieldV1,
)
from boomi_mcp.models.process_ir import parse_process_ir_v1  # noqa: E402
from boomi_mcp.recipes.materialization import build_symbol_table  # noqa: E402

from _m12_11_support import VALID_IR_DOC, components  # noqa: E402

_META = {"db_op": ("database", "GET"), "api_op": ("http", "SEND")}


def _supporting():
    return [c for c in components() if c.type != "process"]


def _symbols(resolver=None):
    kwargs = {"connector_metadata": _META}
    if resolver is not None:
        kwargs["resolver"] = resolver
    return build_symbol_table(_supporting(), **kwargs)


def _envelope(**overrides):
    kwargs = {"component_key": "proc", "name": "P", "action": "create"}
    kwargs.update(overrides)
    return ProcessComponentEnvelopeV1(**kwargs)


def _plan(doc=None, envelope=None, resolver=None, **overrides):
    kwargs = dict(
        envelope=envelope or _envelope(),
        process_ir=parse_process_ir_v1(copy.deepcopy(doc or VALID_IR_DOC)),
        symbols=_symbols(resolver),
        conflict_policy="reuse",
        compiler_revision=get_authoring_revisions()["compiler_revision"],
        emitter_revision=emitter_revision(),
        materializer_revision="sha256:" + "a" * 64,
    )
    kwargs.update(overrides)
    return pm.build_materialization_plan(**kwargs)


# ---------------------------------------------------------------------------
# Relocatability — the promise the whole design rests on
# ---------------------------------------------------------------------------


def test_two_accounts_authoring_the_same_process_produce_identical_bytes():
    """Asserted on the BYTES, not only the digest.

    A provider returning a constant would match on digests alone, which is why
    the wave gate compares canonical material too.
    """
    a = pm.process_plan_fingerprint(
        _plan(envelope=_envelope(action="update", component_id="ACCOUNT-A-COMPONENT"),
              resolved_folder_id="ACCOUNT-A-FOLDER")
    )
    b = pm.process_plan_fingerprint(
        _plan(envelope=_envelope(action="update", component_id="ACCOUNT-B-COMPONENT"),
              resolved_folder_id="ACCOUNT-B-FOLDER")
    )
    assert a[1] == b[1]
    assert a[0] == b[0]
    for bound in (
        b"ACCOUNT-A-COMPONENT", b"ACCOUNT-B-COMPONENT",
        b"ACCOUNT-A-FOLDER", b"ACCOUNT-B-FOLDER",
    ):
        assert bound not in a[1]


def test_a_real_id_resolver_cannot_reach_the_covered_material():
    """The structural guarantee, tested by trying to defeat it.

    ``build_symbol_table`` exposes ``resolver`` publicly and the legacy arm
    already passes a real one, so an earlier draft that accepted a
    caller-compiled plan was one keyword away from an account-bound fingerprint.
    The builder now rebuilds the table with placeholders before compiling.
    """
    placeholder = pm.process_plan_fingerprint(_plan())[1]
    real = pm.process_plan_fingerprint(
        _plan(resolver=lambda ref: "REAL-" + ref.split(":")[1].upper())
    )[1]
    assert placeholder == real
    assert b"REAL-" not in real
    # Positive control: the resolver really does change a plan compiled WITHOUT
    # the guarantee, so the equality above is the guarantee working, not the
    # resolver being ignored.
    from boomi_mcp.compiler.process_ir.contracts import canonical_emission_plan_json
    from boomi_mcp.compiler.process_ir.pipeline import compile_process_ir_v1

    ir = parse_process_ir_v1(copy.deepcopy(VALID_IR_DOC))
    unguarded = canonical_emission_plan_json(
        compile_process_ir_v1(ir, _symbols(lambda r: "REAL-" + r.split(":")[1].upper()))[1]
    )
    assert "REAL-" in unguarded


def test_a_literal_component_reference_is_refused():
    """``ComponentRefV1`` admits a literal id; a materializable plan may not.

    Refused rather than excluded from coverage: dropping the extension bindings
    from the material would stop a REAL override change from moving the
    fingerprint, which is a worse failure than refusing the input.
    """
    bindings = ProcessExtensionBindingsV1(
        connections=(
            ProcessConnectionOverrideV1(
                connection_id="ACCOUNT-A-REAL-CONNECTION-ID",
                fields=(ProcessOverrideFieldV1(id="password", label="Password"),),
            ),
        )
    )
    with pytest.raises(ValidationError) as excinfo:
        _plan(envelope=_envelope(process_extensions=bindings))
    assert (
        excinfo.value.errors()[0]["type"]
        == "process_materialization_reference_not_relocatable"
    )

    # Positive control: the same binding with a logical ref is accepted, so the
    # refusal is about the literal id and not about extensions in general.
    ok = ProcessExtensionBindingsV1(
        connections=(
            ProcessConnectionOverrideV1(
                connection_id="$ref:db_conn",
                fields=(ProcessOverrideFieldV1(id="password", label="Password"),),
            ),
        )
    )
    assert _plan(envelope=_envelope(process_extensions=ok)) is not None


def test_a_slot_ref_must_be_logical_too():
    with pytest.raises(ValidationError):
        pm.ProcessComponentSymbolSlotV1(slot_id="s1", ref="REAL-COMPONENT-ID")
    assert pm.ProcessComponentSymbolSlotV1(slot_id="s1", ref="$ref:db_conn").ref


# ---------------------------------------------------------------------------
# Coverage is derived, not hand-listed
# ---------------------------------------------------------------------------


def test_every_plan_field_is_either_covered_or_explicitly_excluded():
    """The accounting that makes a new field impossible to miss.

    A field added to the model is covered BY DEFAULT — which fails loudly at the
    wave gate if it happens to be account-bound, rather than silently weakening
    the fingerprint.
    """
    all_fields = set(pm.ProcessComponentMaterializationPlanV1.model_fields)
    covered = set(pm.covered_plan_fields())
    excluded = set(pm.EXCLUDED_PLAN_FIELDS)
    assert covered | excluded == all_fields
    assert covered & excluded == set()
    assert excluded <= all_fields, excluded - all_fields
    # Every exclusion carries a stated reason — an unexplained exclusion is how
    # an account-bound field gets quietly dropped from coverage.
    for reason in pm.EXCLUDED_PLAN_FIELDS.values():
        assert reason.strip()


def test_the_exclusion_set_is_load_bearing_not_documentation(monkeypatch):
    """Mutating the exclusion set must change the material.

    An earlier draft DOCUMENTED this set as the authority while hand-building
    the payload, so the set could say anything and change nothing.
    """
    plan = _plan()
    before = pm.canonical_plan_material(plan)
    monkeypatch.setitem(pm.EXCLUDED_PLAN_FIELDS, "conflict_policy", "test override")
    after = pm.canonical_plan_material(plan)
    assert after != before
    assert b"conflict_policy" not in after


def test_the_envelope_exclusion_is_also_read_not_described():
    plan = _plan(envelope=_envelope(action="update", component_id="ACCOUNT-X"))
    material = pm.canonical_plan_material(plan)
    assert b"ACCOUNT-X" not in material
    assert "component_id" in pm.EXCLUDED_ENVELOPE_FIELDS


# ---------------------------------------------------------------------------
# The four mutation kinds the wave gate requires
# ---------------------------------------------------------------------------


def _mutated(kind):
    doc = copy.deepcopy(VALID_IR_DOC)
    if kind == "semantic":
        doc["body"]["steps"].insert(1, {"kind": "message", "text": "mutated"})
    return pm.process_plan_fingerprint(
        _plan(
            doc=doc,
            envelope=_envelope(name="MUTATED" if kind == "envelope" else "P"),
            conflict_policy="fail" if kind == "policy" else "reuse",
            materializer_revision="sha256:"
            + ("b" if kind == "revision" else "a") * 64,
        )
    )


@pytest.mark.parametrize("kind", ["semantic", "envelope", "policy", "revision"])
def test_each_mutation_kind_moves_both_material_and_digest(kind):
    base = pm.process_plan_fingerprint(_plan())
    mutated = _mutated(kind)
    assert mutated[1] != base[1], "canonical material did not move"
    assert mutated[0] != base[0], "digest did not move"


def test_the_four_mutation_kinds_are_mutually_distinct():
    """The gate's hardest check: four names must not mean one change.

    A provider declaring four kinds and applying one identical "changed" plan
    every time satisfies every per-kind check while proving nothing.
    """
    materials = {kind: _mutated(kind)[1] for kind in
                 ("semantic", "envelope", "policy", "revision")}
    assert len(set(materials.values())) == 4


# ---------------------------------------------------------------------------
# Fingerprint integrity
# ---------------------------------------------------------------------------


def test_the_plan_refuses_a_fingerprint_that_is_not_its_own():
    plan = _plan()
    fields = {
        name: getattr(plan, name)
        for name in pm.ProcessComponentMaterializationPlanV1.model_fields
        if name != "plan_fingerprint"
    }
    with pytest.raises(ValidationError) as excinfo:
        pm.ProcessComponentMaterializationPlanV1(
            plan_fingerprint="sha256:" + "f" * 64, **fields
        )
    assert (
        excinfo.value.errors()[0]["type"]
        == "process_materialization_fingerprint_mismatch"
    )


def test_the_emission_plan_is_immutable_text_not_a_mutable_mapping():
    """A ``Mapping`` field is mutable even on a frozen model.

    Measured on an earlier draft: one in-place write desynchronized the stored
    fingerprint from the plan's own material after validation, defeating the
    tamper-evidence the stored digest exists to provide.
    """
    plan = _plan()
    assert isinstance(plan.emission_plan_canonical_json, str)
    with pytest.raises(Exception):
        plan.emission_plan_canonical_json = "{}"
    # It really is the emission plan, not an empty placeholder.
    parsed = json.loads(plan.emission_plan_canonical_json)
    assert parsed["nodes"] and parsed["entry_shape_id"]


def test_slots_are_canonically_ordered_before_the_digest_is_taken():
    """The DERIVED inventory is sorted by slot_id in the constructed plan.

    Reworked for §6 AR1-03: slots are no longer caller-supplied, so ordering is
    asserted on the builder's own output — the model's field validator sorts,
    and the fingerprint is taken over the sorted form.
    """
    plan = _plan()
    ids = [s.slot_id for s in plan.unresolved_symbol_slots]
    assert ids == sorted(ids)
    assert len(ids) == len(set(ids))


def test_duplicate_slot_ids_are_refused():
    slots = (
        pm.ProcessComponentSymbolSlotV1(
            slot_id="dup", ref="$ref:db_conn", expected_component_types=("connector-settings",)
        ),
        pm.ProcessComponentSymbolSlotV1(
            slot_id="dup", ref="$ref:api_conn", expected_component_types=("connector-settings",)
        ),
    )
    base = _plan()
    with pytest.raises(ValidationError):
        pm.ProcessComponentMaterializationPlanV1(
            **{**base.model_dump(), "unresolved_symbol_slots": slots,
               "envelope": base.envelope, "process_ir": base.process_ir}
        )


def test_the_builder_populates_the_slot_inventory():
    """§6 AR1-03 witness: production plans record what late binding must resolve.

    The slot layer was modelled and fingerprinted but never populated — every
    production plan carried `unresolved_symbol_slots == ()` while its emission
    plan was full of placeholders. Slots are now DERIVED from the same
    enumeration the relocatability rule reads, so the inventory cannot disagree
    with the references the plan actually binds.
    """
    plan = _plan()
    slots = plan.unresolved_symbol_slots
    assert slots, "a plan with $ref-bearing IR must record a non-empty inventory"
    refs = sorted({s.ref for s in slots})
    # The fixture IR references its connections and operations by $ref.
    assert all(r.startswith("$ref:") for r in refs)
    for slot in slots:
        # slot_id is a stable source pointer into the plan's own material.
        assert slot.slot_id.startswith(("process_ir", "process_extensions")), slot.slot_id
        # ...and the expected type comes from the compile symbol table (nonempty).
        assert slot.expected_component_types


def test_a_slot_with_no_expected_type_is_refused():
    """Plan §3: `expected_component_types` is required NONEMPTY — a slot that
    constrains nothing is an inventory entry late binding cannot check."""
    with pytest.raises(ValidationError):
        pm.ProcessComponentSymbolSlotV1(slot_id="s", ref="$ref:x", expected_component_types=())


def test_the_digest_is_sha256_of_the_returned_material():
    """Exactly what the wave gate recomputes."""
    import hashlib

    digest, material = pm.process_plan_fingerprint(_plan())
    assert digest == "sha256:" + hashlib.sha256(material).hexdigest()
    assert isinstance(material, bytes) and material


def test_the_fingerprint_is_deterministic_across_repeated_builds():
    assert pm.process_plan_fingerprint(_plan()) == pm.process_plan_fingerprint(_plan())


# ---------------------------------------------------------------------------
# The preservation projection
# ---------------------------------------------------------------------------


def test_the_preservation_projection_carries_every_runtime_field():
    from boomi_mcp.categories.components.builders._preservation_policy import OwnedPath
    from boomi_mcp.categories.components.builders._process_preservation import (
        PROCESS_PRESERVATION_POLICY,
    )

    projected = json.loads(pm.preservation_policy_v1().canonical_policy_json)
    assert set(projected) == {
        f.name for f in dataclasses.fields(PROCESS_PRESERVATION_POLICY)
    }
    assert set(projected["owned_paths"][0]) == {
        f.name for f in dataclasses.fields(OwnedPath)
    }
    # The two fields whose loss silently changes whether live state survives.
    assert "mode" in projected["owned_paths"][0]
    assert "owned_encrypted_paths" in projected


def test_a_changed_preservation_policy_moves_the_fingerprint(monkeypatch):
    """The projection must be load-bearing, not decorative.

    The wave-gate provider holds the preservation policy CONSTANT across all four
    mutation kinds, so the gate alone could never catch a lossy projection — this
    is where that gap is closed.
    """
    import boomi_mcp.categories.components.builders._process_preservation as shared
    from boomi_mcp.categories.components.builders._preservation_policy import (
        OwnedPath,
        PreservationPolicy,
    )

    before = pm.process_plan_fingerprint(_plan())[1]
    monkeypatch.setattr(
        shared,
        "PROCESS_PRESERVATION_POLICY",
        PreservationPolicy(
            component_type="process",
            # `mode` differs — the field an earlier lossy projection dropped.
            owned_paths=(OwnedPath(path="bns:object/process", mode="key_merge"),),
        ),
    )
    after = pm.process_plan_fingerprint(_plan())[1]
    assert after != before, "a changed preservation policy did not move the material"
