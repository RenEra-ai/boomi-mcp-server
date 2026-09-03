"""Late binding, materialization and the two attestations (issue #153).

The canonical chain's apply half. A root is compiled with PLACEHOLDER ids so its
plan fingerprint is relocatable; the real Boomi ids exist only after the
dependencies have been applied in topological order. This module proves the
handover: the plan is RECOMPILED against the real symbol table (never
string-patched), the emitted XML carries real ids and no placeholders, and what
happened is attested in two separate records.

The fixture builds its own components rather than reusing ``_m12_11_support``:
that fixture's operations are typed ``connector-operation``, and the emitter's
symbol requirement accepts ``connector-action`` — so it compiles but cannot
EMIT. Measured while writing these tests, which is why the distinction is
recorded here instead of being rediscovered.
"""

import sys
from pathlib import Path

import pytest
from pydantic import ValidationError

_src = str(Path(__file__).resolve().parent.parent / "src")
if _src not in sys.path:
    sys.path.insert(0, _src)
sys.path.insert(0, str(Path(__file__).resolve().parent))

from boomi_mcp.authoring.contract import get_authoring_revisions  # noqa: E402
from boomi_mcp.authoring.process_materialization import (  # noqa: E402
    ProcessComponentMaterializationPlanV1,
    build_materialization_plan,
    process_plan_fingerprint,
)
from boomi_mcp.categories.components import canonical_process_apply as cpa  # noqa: E402
from boomi_mcp.compiler.process_ir.emitter_registry import (  # noqa: E402
    emitter_revision,
)
from boomi_mcp.models.integration_models import IntegrationComponentSpec  # noqa: E402
from boomi_mcp.models.process_component import (  # noqa: E402
    ProcessComponentEnvelopeV1,
    ProcessConnectionOverrideV1,
    ProcessExtensionBindingsV1,
    ProcessOverrideFieldV1,
)
from boomi_mcp.models.process_ir import parse_process_ir_v1  # noqa: E402
from boomi_mcp.recipes.materialization import build_symbol_table  # noqa: E402

_DIGEST = "sha256:" + "c" * 64

_DOC = {
    "version": "1",
    "body": {
        "kind": "sequence",
        "steps": [
            {
                "kind": "source",
                "connection_ref": "$ref:src_conn",
                "operation_ref": "$ref:src_op",
            },
            {"kind": "message", "text": "hello"},
            {
                "kind": "target",
                "connection_ref": "$ref:dst_conn",
                "operation_ref": "$ref:dst_op",
            },
            {"kind": "stop"},
        ],
    },
}

#: Applied component ids, as the ordered apply loop's registry would publish them.
_REGISTRY = {
    "src_conn": "REAL-SRC-CONN",
    "src_op": "REAL-SRC-OP",
    "dst_conn": "REAL-DST-CONN",
    "dst_op": "REAL-DST-OP",
}


def _components():
    return [
        IntegrationComponentSpec(
            key="src_conn", type="connector-settings", name="Src",
            config={"connector_type": "database"},
        ),
        # `connector-action`, not `connector-operation` — the emitter's symbol
        # requirement accepts only the former.
        IntegrationComponentSpec(
            key="src_op", type="connector-action", name="SrcOp",
            config={"connection_ref_key": "src_conn"},
        ),
        IntegrationComponentSpec(
            key="dst_conn", type="connector-settings", name="Dst",
            config={"connector_type": "http"},
        ),
        IntegrationComponentSpec(
            key="dst_op", type="connector-action", name="DstOp",
            config={"connection_ref_key": "dst_conn"},
        ),
    ]


def _symbols():
    return build_symbol_table(
        _components(),
        connector_metadata={"src_op": ("database", "GET"), "dst_op": ("http", "SEND")},
    )


def _plan(envelope=None, **overrides):
    kwargs = dict(
        envelope=envelope
        or ProcessComponentEnvelopeV1(
            component_key="root",
            name="Canonical Root",
            action="create",
            depends_on=tuple(_REGISTRY),
        ),
        process_ir=parse_process_ir_v1(dict(_DOC)),
        symbols=_symbols(),
        conflict_policy="reuse",
        compiler_revision=get_authoring_revisions()["compiler_revision"],
        emitter_revision=emitter_revision(),
        materializer_revision="sha256:" + "a" * 64,
    )
    kwargs.update(overrides)
    return build_materialization_plan(**kwargs)


# ---------------------------------------------------------------------------
# Late binding
# ---------------------------------------------------------------------------


def test_the_emitted_xml_carries_real_ids_and_no_placeholders():
    """The whole point of late binding, asserted in both directions.

    "Real ids present" alone would pass if the placeholders were ALSO still
    there; "no placeholders" alone would pass for empty output.
    """
    xml = cpa.materialize_canonical_process_xml(
        plan=_plan(), id_registry=_REGISTRY, symbols=_symbols()
    )
    for real in _REGISTRY.values():
        assert real in xml, real
    for key in _REGISTRY:
        assert "id-{0}".format(key) not in xml
    assert xml.startswith("<?xml")
    assert 'type="process"' in xml


def test_the_plan_itself_still_holds_placeholders():
    """Binding happens at apply — the PLAN stays relocatable.

    If binding leaked back into the plan, the stored fingerprint would describe
    an account-bound artifact and relocatability would be silently lost.
    """
    plan = _plan()
    assert "id-src_conn" in plan.emission_plan_canonical_json
    for real in _REGISTRY.values():
        assert real not in plan.emission_plan_canonical_json


def test_a_dependency_with_no_applied_id_fails_closed():
    """Leaving a placeholder would submit a dangling reference that LOOKS applied.

    Boomi accepts an unknown component id as an opaque string, so the component
    would be created and only fail when executed.
    """
    with pytest.raises(cpa.CanonicalProcessApplyError) as excinfo:
        cpa.materialize_canonical_process_xml(
            plan=_plan(), id_registry={"src_conn": "X"}, symbols=_symbols()
        )
    assert excinfo.value.error_code == "PROCESS_MATERIALIZATION_SYMBOL_BINDING_INVALID"
    assert excinfo.value.component_key == "root"


def test_a_plan_whose_fingerprint_does_not_match_is_refused():
    """A plan that is not the plan that was compiled must not be materialized."""
    plan = _plan()
    forged = plan.model_copy(update={"plan_fingerprint": "sha256:" + "0" * 64})
    with pytest.raises(cpa.CanonicalProcessApplyError) as excinfo:
        cpa.materialize_canonical_process_xml(
            plan=forged, id_registry=_REGISTRY, symbols=_symbols()
        )
    assert excinfo.value.error_code == "PROCESS_MATERIALIZATION_FINGERPRINT_MISMATCH"


def test_extension_refs_are_resolved_to_applied_ids():
    envelope = ProcessComponentEnvelopeV1(
        component_key="root",
        name="Canonical Root",
        action="create",
        depends_on=tuple(_REGISTRY),
        process_extensions=ProcessExtensionBindingsV1(
            connections=(
                ProcessConnectionOverrideV1(
                    connection_id="$ref:src_conn",
                    connector_type="database",
                    fields=(
                        ProcessOverrideFieldV1(
                            id="username", label="Username",
                            xpath="DatabaseConnectionSettings/@username",
                        ),
                    ),
                ),
            )
        ),
    )
    xml = cpa.materialize_canonical_process_xml(
        plan=_plan(envelope=envelope), id_registry=_REGISTRY, symbols=_symbols()
    )
    assert '<ConnectionOverride id="REAL-SRC-CONN">' in xml
    assert "$ref:src_conn" not in xml
    assert 'xpath="DatabaseConnectionSettings/@username"' in xml


def test_extension_field_order_is_preserved():
    """The renderer emits fields verbatim, so a reorder moves emitted bytes."""
    envelope = ProcessComponentEnvelopeV1(
        component_key="root", name="R", action="create", depends_on=tuple(_REGISTRY),
        process_extensions=ProcessExtensionBindingsV1(
            connections=(
                ProcessConnectionOverrideV1(
                    connection_id="$ref:src_conn",
                    fields=tuple(
                        ProcessOverrideFieldV1(id="f{0}".format(i), label="L{0}".format(i))
                        for i in (3, 1, 2)
                    ),
                ),
            )
        ),
    )
    xml = cpa.materialize_canonical_process_xml(
        plan=_plan(envelope=envelope), id_registry=_REGISTRY, symbols=_symbols()
    )
    assert xml.index('id="f3"') < xml.index('id="f1"') < xml.index('id="f2"')


# ---------------------------------------------------------------------------
# The two attestations
# ---------------------------------------------------------------------------


def test_a_create_attestation_binds_the_plan_it_executed():
    plan = _plan()
    xml = cpa.materialize_canonical_process_xml(
        plan=plan, id_registry=_REGISTRY, symbols=_symbols()
    )
    att = cpa.build_mutation_attestation(
        plan=plan, action="create", target_component_id=None,
        result_component_id="NEW-ROOT", submitted_xml=xml, account_scope_hash=_DIGEST,
    )
    assert att.plan_fingerprint == plan.plan_fingerprint
    assert att.action == "create"
    assert att.target_component_id is None
    assert att.result_component_id == "NEW-ROOT"
    assert att.account_scope_hash == _DIGEST
    assert att.submitted_xml_digest.startswith("sha256:")


def test_the_submitted_digest_is_over_the_bytes_that_were_sent():
    """Provenance, and it must track the bytes rather than the plan.

    Two different submissions of the same plan — different applied ids — must
    produce different submitted digests, or the digest is not describing what
    was sent.
    """
    plan = _plan()
    first = cpa.materialize_canonical_process_xml(
        plan=plan, id_registry=_REGISTRY, symbols=_symbols()
    )
    other = dict(_REGISTRY, src_conn="OTHER-SRC-CONN")
    second = cpa.materialize_canonical_process_xml(
        plan=plan, id_registry=other, symbols=_symbols()
    )
    assert first != second

    a = cpa.build_mutation_attestation(
        plan=plan, action="create", target_component_id=None,
        result_component_id="R1", submitted_xml=first, account_scope_hash=_DIGEST)
    b = cpa.build_mutation_attestation(
        plan=plan, action="create", target_component_id=None,
        result_component_id="R1", submitted_xml=second, account_scope_hash=_DIGEST)
    assert a.submitted_xml_digest != b.submitted_xml_digest
    # ...while the PLAN fingerprint is identical, which is the whole reason the
    # two attestations cannot be collapsed into one.
    assert a.plan_fingerprint == b.plan_fingerprint


def test_a_create_without_a_result_id_fails_closed():
    """An attestation naming no result describes a mutation nobody can verify."""
    plan = _plan()
    xml = cpa.materialize_canonical_process_xml(
        plan=plan, id_registry=_REGISTRY, symbols=_symbols())
    with pytest.raises(cpa.CanonicalProcessApplyError) as excinfo:
        cpa.build_mutation_attestation(
            plan=plan, action="create", target_component_id=None,
            result_component_id=None, submitted_xml=xml, account_scope_hash=_DIGEST)
    assert excinfo.value.error_code == "PROCESS_MATERIALIZATION_RESULT_ID_MISSING"


def test_an_update_attestation_names_its_target():
    plan = _plan(
        envelope=ProcessComponentEnvelopeV1(
            component_key="root", name="R", action="update",
            component_id="EXISTING", depends_on=tuple(_REGISTRY)))
    xml = cpa.materialize_canonical_process_xml(
        plan=plan, id_registry=_REGISTRY, symbols=_symbols())
    att = cpa.build_mutation_attestation(
        plan=plan, action="update", target_component_id="EXISTING",
        result_component_id="EXISTING", submitted_xml=xml, account_scope_hash=_DIGEST)
    assert att.target_component_id == "EXISTING"
    assert att.result_component_id == "EXISTING"


def test_a_readback_failure_records_unknown_not_agreement():
    """``digest=None`` is the record that the mutation stands but is unverified.

    Omitting the record entirely would let verify compare only the roots whose
    readback happened to succeed and report agreement over an incomplete
    baseline — which is the failure the separate readback attestation prevents.
    """
    failed = cpa.build_readback_attestation(
        component_key="root", component_id="NEW-ROOT", digest=None)
    assert failed.digest is None
    assert failed.component_id == "NEW-ROOT"

    ok = cpa.build_readback_attestation(
        component_key="root", component_id="NEW-ROOT", digest=_DIGEST)
    assert ok.digest == _DIGEST


def test_the_mutation_and_readback_records_are_separate_objects():
    """They answer different questions and are never merged.

    The mutation record carries what was SENT; the readback carries what the
    platform REPORTS. Comparing submitted bytes to a readback would mismatch on
    every healthy apply, because a readback carries server-assigned attributes.
    """
    plan = _plan()
    xml = cpa.materialize_canonical_process_xml(
        plan=plan, id_registry=_REGISTRY, symbols=_symbols())
    mutation = cpa.build_mutation_attestation(
        plan=plan, action="create", target_component_id=None,
        result_component_id="NEW-ROOT", submitted_xml=xml, account_scope_hash=_DIGEST)
    readback = cpa.build_readback_attestation(
        component_key="root", component_id="NEW-ROOT", digest=_DIGEST)

    assert type(mutation) is not type(readback)
    assert not hasattr(readback, "submitted_xml_digest")
    assert not hasattr(mutation, "digest")


def _rebuilt_with_slots(plan, slots):
    """The reviewer's exact construction: a plan rebuilt with a different slot
    inventory AND its own correctly recomputed fingerprint, so a refusal cannot
    be the fingerprint check firing by accident."""
    probe = plan.model_copy(
        update={"unresolved_symbol_slots": tuple(sorted(slots, key=lambda s: s.slot_id))}
    )
    payload = probe.model_dump(mode="python")
    payload["plan_fingerprint"] = process_plan_fingerprint(probe)[0]
    with pytest.raises(ValidationError) as excinfo:
        ProcessComponentMaterializationPlanV1(**payload)
    return str(excinfo.value)


def test_a_plan_whose_slot_inventory_disagrees_with_its_references_is_refused():
    """§6 evaluation 4: every other plan validator checked the slots against
    themselves.

    The reviewer rebuilt a real multi-slot plan with ZERO slots and its own
    correctly recomputed fingerprint, and it validated cleanly — ordering,
    uniqueness and fingerprint all agree with an inventory that inventories
    nothing, and apply then trusts it to decide what late binding must resolve.

    Both directions are asserted, because a guard that only rejected an empty
    inventory would be satisfied by any non-empty one: a DROPPED slot and an
    INVENTED slot are each refused, and the message names which is which.
    """
    plan = _plan()
    slots = list(plan.unresolved_symbol_slots)
    assert len(slots) >= 2, slots

    dropped = _rebuilt_with_slots(plan, slots[:-1])
    assert "symbol slots disagree" in dropped, dropped
    assert "unreferenced: none" in dropped, dropped
    assert slots[-1].slot_id in dropped, dropped

    invented = slots + [slots[0].model_copy(update={"slot_id": "steps/999/nowhere"})]
    extra = _rebuilt_with_slots(plan, invented)
    assert "unreferenced: steps/999/nowhere" in extra, extra
    assert "uninventoried: none" in extra, extra

    assert "symbol slots disagree" in _rebuilt_with_slots(plan, [])


def test_the_slot_disagreement_refusal_is_not_the_relocatability_refusal():
    """The control the previous test needs to mean anything.

    Both refusals come from the same model with the same error type, so the
    previous test would be satisfied by a guard that had simply captured the
    relocatability case. It has not: a literal component id is filtered out of
    BOTH sides of the slot comparison — the recorded inventory and the derived
    one skip it identically — so the two rules cannot collide, and a literal-id
    plan still reports relocatability.

    That independence is a MEASUREMENT, not an ordering argument. The first
    draft of this docstring claimed the relocatability validator wins because it
    is defined first; moving the new validator above it was then measured to
    change nothing at all, because the collision it describes does not exist.
    """
    literal = ProcessComponentEnvelopeV1(
        component_key="root",
        name="Canonical Root",
        action="create",
        depends_on=tuple(_REGISTRY),
        process_extensions=ProcessExtensionBindingsV1(
            connections=(
                ProcessConnectionOverrideV1(
                    connection_id="12345678-1234-1234-1234-123456789abc",
                    connector_type="database",
                    fields=(
                        ProcessOverrideFieldV1(
                            id="username", label="Username",
                            xpath="DatabaseConnectionSettings/@username",
                        ),
                    ),
                ),
            )
        ),
    )
    with pytest.raises(ValidationError) as excinfo:
        _plan(envelope=literal)
    rendered = str(excinfo.value)
    assert "symbol slots disagree" not in rendered, rendered
    assert "relocatable" in rendered, rendered


def test_a_fingerprint_consistent_plan_with_a_mutated_ir_refuses_before_emission():
    """#178: apply recompiles the plan's root, so it is the LAST place a
    grammar-invalid document could reach emitted XML.

    The fingerprint guard alone does not cover this: it proves the plan matches
    its own material, not that the material is legal. A plan forged so its
    fingerprint AGREES therefore walks straight past it — and before #178 the
    compile that follows judged the caller's model on the compiler's own rules,
    which for a mutated `version` meant no refusal at all (no compiler stage
    reads `version`). The compile entry now re-validates through the parser.

    Both tripwires assert the refusal happens BEFORE anything is produced: an
    apply that refused only after emitting would still have built the bytes.
    """
    import copy as _copy

    from boomi_mcp.authoring.process_materialization import process_plan_fingerprint
    from boomi_mcp.compiler.process_ir.diagnostics import ProcessIRCompileError

    plan = _plan()
    mutated = _copy.deepcopy(plan.process_ir)
    mutated.version = "2"
    # `model_copy` skips validation, and pydantic does not revalidate an
    # already-built nested model — so a fingerprint-CONSISTENT forgery is
    # constructible, which is precisely why this path needs its own guard.
    forged = plan.model_copy(update={"process_ir": mutated})
    digest, _material = process_plan_fingerprint(
        forged.model_copy(update={"plan_fingerprint": "sha256:" + "0" * 64})
    )
    forged = forged.model_copy(update={"plan_fingerprint": digest})

    # `emit_process` is imported INSIDE the function, so it must be patched at
    # its SOURCE module — patching the apply module would silently no-op and the
    # tripwire would read as "emission never ran" for every possible outcome.
    from boomi_mcp.compiler.process_ir import emitter_registry as _er

    emitted = []
    real_emit = _er.emit_process

    def _tripwire(*args, **kwargs):
        emitted.append(1)
        return real_emit(*args, **kwargs)

    _er.emit_process = _tripwire
    try:
        with pytest.raises(ProcessIRCompileError) as excinfo:
            cpa.materialize_canonical_process_xml(
                plan=forged, id_registry=_REGISTRY, symbols=_symbols()
            )
    finally:
        _er.emit_process = real_emit

    served = excinfo.value.diagnostics[0]
    assert served.code == "PROCESS_IR_SCHEMA_VERSION_UNSUPPORTED", [
        (d.code, d.path) for d in excinfo.value.diagnostics
    ]
    assert served.phase == "schema"
    assert emitted == [], "emission ran before the document was refused"


def test_the_unforged_control_still_materializes():
    """Non-vacuity for the test above: without the mutation the same call
    succeeds, so the refusal is the mutation's doing and not a broken forgery."""
    xml = cpa.materialize_canonical_process_xml(
        plan=_plan(), id_registry=_REGISTRY, symbols=_symbols()
    )
    assert xml.startswith("<?xml")


# ---------------------------------------------------------------------------
# The evidence bindings the attestation carries
# ---------------------------------------------------------------------------

_SCOPE_A = "a" * 64
_SCOPE_B = "b" * 64


def _binding(**over):
    """One resolved binding, in the mapping shape the recheck boundary hands over."""
    base = {
        "contract_ref": "$ref:icv1:rest:patch:c1:1",
        "operation_ref": "$ref:op",
        "call_source_path": "/body/steps/0",
        "process_root_ref": "$ref:ROOT",
        "connection_ref": "$ref:conn",
        "record_digest": "c" * 64,
        "account_scope_hash": _SCOPE_A,
        "operation_component_id": "op-1",
        "operation_version": 3,
        "connection_component_id": "cn-1",
        "connection_version": 5,
        "operation_config_digest": "ComponentConfigDigestV1:" + ("d" * 64),
        "connection_config_digest": "ComponentConfigDigestV1:" + ("e" * 64),
        "route_coverage_kind": "service_wide",
    }
    base.update(over)
    return base


def _attest(bindings, *, scope=_SCOPE_A):
    plan = _plan()
    xml = cpa.materialize_canonical_process_xml(
        plan=plan, id_registry=_REGISTRY, symbols=_symbols()
    )
    return cpa.build_mutation_attestation(
        plan=plan, action="create", target_component_id=None,
        result_component_id="NEW-ROOT", submitted_xml=xml,
        account_scope_hash=_DIGEST,
        replay_evidence_bindings=bindings,
        replay_account_scope_hash=scope,
    )


def test_one_consumed_grant_is_attested_once():
    """A duplicate is a second claim that a second grant was consumed.

    The construction sorted on three of the five key fields and deduplicated on
    none, so the same binding arriving twice — the same root, call, contract,
    operation and connection — was recorded twice. An accounting record that
    double-counts what authorised a write is describing something that did not
    happen.
    """
    att = _attest([_binding(), _binding()])
    assert len(att.replay_evidence_bindings) == 1, att.replay_evidence_bindings

    # ...and two GENUINELY different calls of the same contract stay two: the
    # dedup must collapse repeats, never distinct bindings.
    att = _attest([_binding(), _binding(call_source_path="/body/steps/4")])
    assert len(att.replay_evidence_bindings) == 2, att.replay_evidence_bindings


def test_the_binding_order_is_the_key_the_contract_names():
    """Byte-identical records for the same set, whatever order it arrives in."""
    one = _attest([
        _binding(call_source_path="/body/steps/4"),
        _binding(connection_ref="$ref:aaa"),
        _binding(),
    ])
    other = _attest([
        _binding(),
        _binding(call_source_path="/body/steps/4"),
        _binding(connection_ref="$ref:aaa"),
    ])
    assert [b.model_dump() for b in one.replay_evidence_bindings] == [
        b.model_dump() for b in other.replay_evidence_bindings
    ]
    # NON-VACUITY: three distinct keys, so the comparison is over an order that
    # could have differed rather than over a one-element list.
    assert len(one.replay_evidence_bindings) == 3


def test_a_binding_from_another_account_refuses_rather_than_being_attested():
    """The nested-scope invariant, at the last gate before the record is durable.

    The plan words it as "nested account scope equals the parent attestation
    scope". Taken literally that is unsatisfiable and the test would be
    unwritable: the parent's `account_scope_hash` is the authoring layer's
    fingerprint over a keyed payload, the binding's is the registry's plain
    `sha256(account_id)` — two digests of the same account by two owners, never
    equal. So the comparison is against the REGISTRY-side scope for this apply,
    which is the value the boundary recheck itself compared against.
    """
    with pytest.raises(cpa.CanonicalProcessApplyError) as raised:
        _attest([_binding(account_scope_hash=_SCOPE_B)])
    # THE CODE, not merely the exception type. The first version asserted only
    # that something was raised, and the code it raised said a component id was
    # missing — a served code describing a different failure, which a machine
    # reader would have acted on.
    assert raised.value.error_code == (
        "CONNECTOR_REPLAY_POST_SUBMISSION_RECONCILIATION_DRIFT"
    )

    # A binding that makes NO account claim is not a foreign one. Absent means
    # "not captured" — the model says so — and refusing it would invent a
    # disagreement out of silence.
    att = _attest([_binding(account_scope_hash=None)])
    assert len(att.replay_evidence_bindings) == 1


def test_the_attestation_records_what_the_boundary_compared():
    """Both configuration digests, or the record cannot show they were checked.

    A credential-only version advance is the case these digests exist to catch,
    and the boundary compares them — but the durable record carried neither, so
    an auditor could see that ids and versions were checked and could not see
    that configuration was.
    """
    att = _attest([_binding()])
    bound = att.replay_evidence_bindings[0]
    assert bound.operation_config_digest == "ComponentConfigDigestV1:" + ("d" * 64)
    assert bound.connection_config_digest == "ComponentConfigDigestV1:" + ("e" * 64)
    assert bound.process_root_ref == "$ref:ROOT"
    assert bound.connection_ref == "$ref:conn"


def test_the_binding_records_which_route_authorised_the_write():
    """The kind alone could not say WHICH route, and the registry rotates.

    Recording only the coverage kind was justified on the ground that a static
    coverage "enumerates the routes it covers, and a route is a path". The model
    enumerates `route_digests` — versioned one-way hashes — so the reasoning was
    right about paths and wrong about the field it described. The recheck
    computes and compares the live route digest; a durable binding that omits
    what was compared cannot be reconstructed once the row it rested on changes.
    """
    digests = ("RouteDigestV1:" + "f" * 64,)
    att = _attest([_binding(route_coverage_kind="static_path", route_digests=digests)])
    bound = att.replay_evidence_bindings[0]
    assert tuple(bound.route_digests) == digests
    assert bound.route_coverage_kind == "static_path"

    # A coverage that enumerates nothing records nothing — service-wide coverage
    # has no route list, and inventing an empty one as a value would be a claim.
    plain = _attest([_binding()])
    assert tuple(plain.replay_evidence_bindings[0].route_digests) == ()
