"""#158: the listener entry on the canonical chain — bytes, resolution, options.

What this file pins, and against what:

* the two SOAP-target anchors are byte-identical to the evidence captured from
  the LEGACY renderer at the step-0 baseline, before either routing gate changed
  (``docs/architecture/evidence/issue-158/preflip/``) — the only independent
  source of those bytes;
* every WSS spelling the builder accepts adapts to the same canonical bytes the
  legacy renderer emits for it;
* a non-empty label reaches the fused Start's ``userlabel`` exactly as the shared
  renderer writes it (every committed listener anchor has an empty label, so byte
  identity to the anchors cannot see a dropped label);
* the listener ``<process>`` options appear on BOTH canonical arms — the
  sync_pipeline cut-over and the #153 materializer — derived from the entry node;
* the listener entry's operation resolution and inbound contract, at the
  compiler boundary;
* the #155 outbound checks still fire for the calls downstream of a listener.

Symbol ids in the compiler-level tests are opaque test values; the byte-level
tests use the legacy anchors' own ids.
"""

from __future__ import annotations

import copy
import hashlib
import json
from pathlib import Path

import pytest

from boomi_mcp.authoring.contract import get_authoring_revisions
from boomi_mcp.authoring.process_materialization import (
    build_materialization_plan,
    process_plan_fingerprint,
)
from boomi_mcp.categories.components import canonical_process_apply as cpa
from boomi_mcp.categories.components.builders.process_emitters import rendering
from boomi_mcp.categories.components.builders.process_flow_builder import (
    ProcessFlowBuilder,
    SyncPipelineBuilder,
)
from boomi_mcp.categories.components.process_component_materializer import (
    LISTENER_PROCESS_OPTIONS,
)
from boomi_mcp.compiler.process_ir.contracts import ComponentSymbolV1, SymbolTableV1
from boomi_mcp.compiler.process_ir.diagnostics import ProcessIRCompileError
from boomi_mcp.compiler.process_ir.emitter_registry import emit_process, emitter_revision
from boomi_mcp.compiler.process_ir.legacy_adapters.emission import (
    emit_legacy_result_with_profile,
)
from boomi_mcp.compiler.process_ir.legacy_adapters.sync_pipeline import (
    adapt_sync_pipeline,
)
from boomi_mcp.compiler.process_ir.pipeline import compile_process_ir_v1
from boomi_mcp.errors import (
    PROCESS_IR_CAPABILITY_CONNECTOR_ACTION_UNSUPPORTED,
    PROCESS_IR_CAPABILITY_UNSUPPORTED,
    PROCESS_IR_REFERENCE_CONNECTION_MISMATCH,
    PROCESS_IR_REFERENCE_LISTENER_OPERATION_INVALID,
    PROCESS_IR_REFERENCE_OPERATION_NOT_FOUND,
    PROCESS_IR_SEMANTIC_DYNAMIC_PATH_REQUIRED,
    PROCESS_IR_SEMANTIC_LISTENER_INBOUND_CONTRACT_UNSATISFIED,
)
from boomi_mcp.models.integration_models import IntegrationComponentSpec
from boomi_mcp.models.process_component import ProcessComponentEnvelopeV1
from boomi_mcp.models.process_ir import parse_process_ir_v1
from boomi_mcp.recipes.materialization import build_symbol_table

import _wave_gate_golden_corpus as corpus

_ROOT = Path(__file__).resolve().parent.parent
_PREFLIP = _ROOT / "docs" / "architecture" / "evidence" / "issue-158" / "preflip"
_GOLDEN = _ROOT / "tests" / "fixtures" / "golden_xml"


# ---------------------------------------------------------------------------
# The pre-flip anchors
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "chain", ["listener_soap_send", "listener_map_soap_send"]
)
def test_soap_target_anchors_are_the_preflip_legacy_bytes(chain):
    """The committed goldens ARE the pre-flip evidence, byte for byte, and the
    canonical chain reproduces them. The manifest's digest is the recorded
    provenance: captured from the legacy renderer at the step-0 baseline."""
    manifest = json.loads((_PREFLIP / "MANIFEST.json").read_text())
    record = next(a for a in manifest["artifacts"] if a["chain"] == chain)
    golden = (_GOLDEN / record["output"]).read_bytes()
    assert golden == (_PREFLIP / record["output"]).read_bytes()
    assert hashlib.sha256(golden).hexdigest() == record["sha256"]
    assert manifest["baseline_sha"] == "3a8469e109cbf414aff1f9db35649ef06071fb66"

    emitted = SyncPipelineBuilder.build(
        corpus.listener_pipeline(copy.deepcopy(corpus.LISTENER_CHAINS[chain])),
        name=corpus.listener_chain_golden_name(chain),
        folder_name="Golden/Fixtures",
    )
    assert emitted.encode("utf-8") == golden


# ---------------------------------------------------------------------------
# Aliases, label, options
# ---------------------------------------------------------------------------


def _listener_core(connector_type, **source):
    return {
        "process_kind": "database_to_api_sync",
        "source": {"connector_type": connector_type, "action_type": "Listen",
                   "operation_id": "WSSOP-1", **source},
        "transform": {"mode": "map_ref", "map_ref": "MAP-1"},
        "target": {"connector_type": "rest", "action_type": "POST",
                   "connection_id": "TGT-CONN", "operation_id": "TGT-OP"},
    }


def _legacy_shapes(core):
    legacy = ProcessFlowBuilder.build(copy.deepcopy(core), name="P", folder_name="F")
    return legacy[legacy.index("<shapes>") + len("<shapes>"):legacy.index("</shapes>")]


@pytest.mark.parametrize(
    "spelling", ["wss", "WSS", "  wss  ", "web_services", "Web_Services_Server"]
)
def test_every_accepted_alias_emits_the_legacy_bytes(spelling):
    core = _listener_core(spelling)
    emission = emit_legacy_result_with_profile(
        adapt_sync_pipeline(copy.deepcopy(core)), dialect="sync_pipeline"
    )
    assert "".join(emission.artifact.shape_xml_parts) == _legacy_shapes(core)
    assert emission.execution_profile == "listener"


def test_a_non_empty_label_reaches_the_fused_start():
    """Escaped exactly as the shared renderer escapes it, and identical to what
    the legacy builder emits for the same label."""
    label = 'Receive <orders> & "more"'
    core = _listener_core("wss", label=label)
    emission = emit_legacy_result_with_profile(
        adapt_sync_pipeline(copy.deepcopy(core)), dialect="sync_pipeline"
    )
    shapes = "".join(emission.artifact.shape_xml_parts)
    start = emission.artifact.shape_xml_parts[0]
    assert 'userlabel="Receive &lt;orders&gt; &amp; &quot;more&quot;"' in start
    ctx = rendering.ShapeRenderContext(
        shape_id="shape1", x=96.0, y=94.0,
        transitions=(rendering.RenderTransition(
            dragpoint_name="shape1.dragpoint1", to_shape_id="shape2", x=240.0, y=104.0),),
    )
    assert start == rendering.render_start_listen(ctx, userlabel=label, operation_id="WSSOP-1")
    assert shapes == _legacy_shapes(core)


@pytest.mark.parametrize("chain", sorted(corpus.LISTENER_CHAINS))
def test_the_cutover_arm_carries_the_derived_listener_options(chain):
    xml = SyncPipelineBuilder.build(
        corpus.listener_pipeline(copy.deepcopy(corpus.LISTENER_CHAINS[chain])),
        name="P", folder_name="F",
    )
    assert "<process xmlns=\"\" " + LISTENER_PROCESS_OPTIONS + ">" in xml
    assert "stopProcessingIfZeroDocuments" not in xml


# ---------------------------------------------------------------------------
# Resolution of the listener's operation, at the compiler boundary
# ---------------------------------------------------------------------------


def _doc(first=None, *rest):
    listener = first or {"kind": "listener", "operation_ref": "$ref:op"}
    steps = [listener] + list(rest or (
        {"kind": "target", "connection_ref": "$ref:tc", "operation_ref": "$ref:to"},
        {"kind": "stop"},
    ))
    return {"version": "1", "body": {"kind": "sequence", "steps": steps}}


def _outbound():
    return (
        ComponentSymbolV1(ref="$ref:tc", component_id="TGT-CONN",
                          component_type="connector-settings", connector_type="rest"),
        ComponentSymbolV1(ref="$ref:to", component_id="TGT-OP",
                          component_type="connector-action", connector_type="rest",
                          action_type="POST"),
    )


def _table(operation, *extra):
    return SymbolTableV1(symbols=(operation,) + _outbound() + tuple(extra))


def _op(**over):
    fields = dict(ref="$ref:op", component_id="WSSOP-1", component_type="connector-action",
                  connector_type="wss", action_type="Listen")
    fields.update(over)
    return ComponentSymbolV1(**fields)


def _compile_code(doc, symbols):
    with pytest.raises(ProcessIRCompileError) as excinfo:
        compile_process_ir_v1(parse_process_ir_v1(doc), symbols)
    return excinfo.value.diagnostics[0]


@pytest.mark.parametrize(
    "family", ["wss", "WSS", "  web_services  ", "web_services_server"]
)
def test_the_listener_resolves_every_accepted_family_spelling(family):
    _cfg, plan = compile_process_ir_v1(parse_process_ir_v1(_doc()), _table(_op(connector_type=family)))
    assert plan.nodes[0].emitter_input.operation_id == "WSSOP-1"


@pytest.mark.parametrize(
    "operation,code",
    [
        (_op(connector_type="wssserver"), PROCESS_IR_REFERENCE_LISTENER_OPERATION_INVALID),
        (_op(connector_type="listener"), PROCESS_IR_REFERENCE_LISTENER_OPERATION_INVALID),
        (_op(connector_type="rest", action_type="GET"), PROCESS_IR_REFERENCE_LISTENER_OPERATION_INVALID),
        (_op(action_type="EXECUTE"), PROCESS_IR_REFERENCE_LISTENER_OPERATION_INVALID),
        (_op(action_type=None), PROCESS_IR_REFERENCE_LISTENER_OPERATION_INVALID),
        (_op(connection_ref="$ref:tc"), PROCESS_IR_REFERENCE_LISTENER_OPERATION_INVALID),
        (_op(component_type="connector-settings"), PROCESS_IR_REFERENCE_OPERATION_NOT_FOUND),
        (_op(ref="$ref:elsewhere"), PROCESS_IR_REFERENCE_OPERATION_NOT_FOUND),
    ],
    ids=[
        "refused-spelling-wssserver", "refused-spelling-listener", "not-wss", "not-listen",
        "no-action", "carries-a-connection", "not-an-operation", "missing",
    ],
)
def test_a_listener_operation_that_is_not_an_operation_only_listen_is_refused(operation, code):
    diagnostic = _compile_code(_doc(), _table(operation))
    assert diagnostic.code == code
    assert diagnostic.path == "/body/steps/0/operation_ref"


def test_a_wss_source_endpoint_is_still_refused():
    """A listener is authored as the `listener` node or not at all: a WSS family
    arriving through the source endpoint would be emitted as start + connector."""
    doc = {"version": "1", "body": {"kind": "sequence", "steps": [
        {"kind": "source", "connection_ref": "$ref:sc", "operation_ref": "$ref:op"},
        {"kind": "target", "connection_ref": "$ref:tc", "operation_ref": "$ref:to"},
        {"kind": "stop"},
    ]}}
    symbols = _table(_op(connection_ref="$ref:sc"), ComponentSymbolV1(
        ref="$ref:sc", component_id="SC", component_type="connector-settings",
        connector_type="wss"))
    assert _compile_code(doc, symbols).code == PROCESS_IR_CAPABILITY_UNSUPPORTED


# ---------------------------------------------------------------------------
# inbound_validation — a build-time contract read off the RESOLVED operation
# ---------------------------------------------------------------------------


_PROFILE = ComponentSymbolV1(ref="$ref:req_profile", component_id="PROF-1",
                             component_type="profile.json")
_MAP_SYMBOL = ComponentSymbolV1(ref="$ref:a_map", component_id="MAP-9",
                                component_type="transform.map")


def _validated():
    return {"kind": "listener", "operation_ref": "$ref:op",
            "inbound_validation": {"mode": "profile_bound"}}


@pytest.mark.parametrize(
    "input_type,profile_ref,accepted",
    [
        ("singlejson", "$ref:req_profile", True),
        ("multixml", "$ref:req_profile", True),
        ("SingleXML", "literal-profile-id", True),
        ("none", "$ref:req_profile", False),
        ("singledata", "$ref:req_profile", False),
        (None, "$ref:req_profile", False),
        ("singlejson", None, False),
        ("singlejson", "$ref:a_map", False),
        ("singlejson", "$ref:not_in_table", False),
    ],
    ids=[
        "json-bound", "xml-bound", "literal-profile-id", "none", "singledata",
        "type-unknown", "no-profile", "profile-ref-names-a-map", "dangling-profile-ref",
    ],
)
def test_inbound_validation_reads_the_resolved_operation(input_type, profile_ref, accepted):
    operation = _op(input_document_type=input_type, input_profile_ref=profile_ref)
    symbols = _table(operation, _PROFILE, _MAP_SYMBOL)
    if accepted:
        compile_process_ir_v1(parse_process_ir_v1(_doc(_validated())), symbols)
        return
    diagnostic = _compile_code(_doc(_validated()), symbols)
    assert diagnostic.code == PROCESS_IR_SEMANTIC_LISTENER_INBOUND_CONTRACT_UNSATISFIED
    assert diagnostic.path == "/body/steps/0/inbound_validation"


def test_absent_inbound_validation_adds_no_requirement():
    """A listener that asks for nothing is not held to a contract it never
    requested — the live Process Library listener binds no request profile."""
    compile_process_ir_v1(
        parse_process_ir_v1(_doc()), _table(_op(input_document_type="none"))
    )


# ---------------------------------------------------------------------------
# The outbound checks still fire downstream of a listener (#155)
# ---------------------------------------------------------------------------


def _call_doc(call):
    return _doc(None, call, {"kind": "stop"})


def _rest(**over):
    fields = dict(ref="$ref:call", component_id="CALL-OP", component_type="connector-action",
                  connector_type="rest", action_type="GET", connection_ref="$ref:rc")
    fields.update(over)
    return ComponentSymbolV1(**fields)


_REST_CONN = ComponentSymbolV1(ref="$ref:rc", component_id="RC",
                               component_type="connector-settings", connector_type="rest")


def test_a_listener_is_a_document_producer_for_the_calls_after_it():
    """The inbound request IS the flow's documents: a documents-required call
    directly after the listener compiles, and is emitted as a downstream call."""
    _cfg, plan = compile_process_ir_v1(
        parse_process_ir_v1(_call_doc({"kind": "connector_call", "operation_ref": "$ref:call"})),
        _table(_op(), _rest(action_type="PATCH"), _REST_CONN),
    )
    assert plan.nodes[1].emitter_input.emitter_kind == "connectoraction_target"


@pytest.mark.parametrize(
    "call_symbol,extra,code",
    [
        (_rest(action_type="POST"), (_REST_CONN,), PROCESS_IR_CAPABILITY_CONNECTOR_ACTION_UNSUPPORTED),
        (_rest(requires_path_binding=True), (_REST_CONN,), PROCESS_IR_SEMANTIC_DYNAMIC_PATH_REQUIRED),
        (_rest(), (ComponentSymbolV1(ref="$ref:rc", component_id="RC",
                                     component_type="connector-settings",
                                     connector_type="database"),),
         PROCESS_IR_REFERENCE_CONNECTION_MISMATCH),
    ],
    ids=["unpublished-action", "blank-path-needs-binding", "connection-family-mismatch"],
)
def test_outbound_checks_stay_active_inside_a_listener_flow(call_symbol, extra, code):
    diagnostic = _compile_code(
        _call_doc({"kind": "connector_call", "operation_ref": "$ref:call"}),
        _table(_op(), call_symbol, *extra),
    )
    assert diagnostic.code == code


# ---------------------------------------------------------------------------
# The #153 seam: plan records the profile, apply re-derives and compares it
# ---------------------------------------------------------------------------


_REGISTRY = {
    "wss_op": "REAL-WSS-OP",
    "rest_conn": "REAL-REST-CONN",
    "rest_op": "REAL-REST-OP",
}


def _components():
    return [
        IntegrationComponentSpec(
            key="wss_op", type="connector-action", name="Inbound",
            config={"connector_type": "wss", "operation_mode": "listen",
                    "object_name": "orders", "component_name": "Inbound"},
        ),
        IntegrationComponentSpec(
            key="rest_conn", type="connector-settings", name="Out",
            config={"connector_type": "rest"},
        ),
        IntegrationComponentSpec(
            key="rest_op", type="connector-action", name="OutOp",
            config={"connector_type": "rest", "method": "POST",
                    "connection_ref_key": "rest_conn"},
        ),
    ]


def _symbols():
    from boomi_mcp.authoring.workflow import _connector_metadata_from_components

    return build_symbol_table(
        _components(), connector_metadata=_connector_metadata_from_components(_components())
    )


def _plan():
    ir = parse_process_ir_v1({"version": "1", "body": {"kind": "sequence", "steps": [
        {"kind": "listener", "operation_ref": "$ref:wss_op", "label": "Inbound orders"},
        {"kind": "target", "connection_ref": "$ref:rest_conn", "operation_ref": "$ref:rest_op"},
        {"kind": "stop"},
    ]}})
    return build_materialization_plan(
        envelope=ProcessComponentEnvelopeV1(
            component_key="root", name="Listener Root", action="create",
            depends_on=tuple(_REGISTRY),
        ),
        process_ir=ir,
        symbols=_symbols(),
        conflict_policy="reuse",
        compiler_revision=get_authoring_revisions()["compiler_revision"],
        emitter_revision=emitter_revision(),
        materializer_revision="sha256:" + "a" * 64,
    )


def test_a_native_wss_operation_derives_listen_with_no_authored_action():
    symbol = _symbols().build_index()["$ref:wss_op"]
    assert (symbol.connector_type, symbol.action_type, symbol.connection_ref) == (
        "wss", "Listen", None,
    )


def test_the_plan_records_the_listener_profile_and_apply_emits_its_options():
    plan = _plan()
    assert plan.execution_profile == "listener"
    xml = cpa.materialize_canonical_process_xml(
        plan=plan, id_registry=_REGISTRY, symbols=_symbols()
    )
    assert "<process xmlns=\"\" " + LISTENER_PROCESS_OPTIONS + ">" in xml
    assert "stopProcessingIfZeroDocuments" not in xml
    assert 'actionType="Listen"' in xml and 'operationId="REAL-WSS-OP"' in xml
    assert 'userlabel="Inbound orders"' in xml
    assert "connectionId=\"REAL-WSS" not in xml


def test_a_forged_scheduled_profile_is_refused_before_emission():
    """A plan re-fingerprinted so its recorded profile says `scheduled` walks past
    the fingerprint guard; apply's RE-DERIVATION from the recompiled entry refuses
    it before a byte is emitted."""
    plan = _plan()
    forged = plan.model_copy(update={"execution_profile": "scheduled"})
    digest, _material = process_plan_fingerprint(
        forged.model_copy(update={"plan_fingerprint": "sha256:" + "0" * 64})
    )
    forged = forged.model_copy(update={"plan_fingerprint": digest})

    from boomi_mcp.compiler.process_ir import emitter_registry as _er

    emitted = []
    real_emit = _er.emit_process

    def _tripwire(*args, **kwargs):
        emitted.append(1)
        return real_emit(*args, **kwargs)

    _er.emit_process = _tripwire
    try:
        with pytest.raises(cpa.CanonicalProcessApplyError) as excinfo:
            cpa.materialize_canonical_process_xml(
                plan=forged, id_registry=_REGISTRY, symbols=_symbols()
            )
    finally:
        _er.emit_process = real_emit
    assert excinfo.value.error_code == "PROCESS_MATERIALIZATION_EXECUTION_PROFILE_INVALID"
    assert emitted == []


def test_the_listener_start_is_graph_verified():
    """`emit_process` runs the process graph verifier over the whole artifact —
    a fused Start with a dangling or doubled wire would be refused there."""
    symbols = SymbolTableV1(symbols=(_op(),) + _outbound())
    _cfg, plan = compile_process_ir_v1(parse_process_ir_v1(_doc()), symbols)
    artifact = emit_process(plan, symbols)
    assert artifact.shape_xml_parts[0].startswith('<shape image="start" name="shape1"')
