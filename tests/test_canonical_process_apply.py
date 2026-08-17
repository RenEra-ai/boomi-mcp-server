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

_src = str(Path(__file__).resolve().parent.parent / "src")
if _src not in sys.path:
    sys.path.insert(0, _src)
sys.path.insert(0, str(Path(__file__).resolve().parent))

from boomi_mcp.authoring.contract import get_authoring_revisions  # noqa: E402
from boomi_mcp.authoring.process_materialization import (  # noqa: E402
    build_materialization_plan,
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
