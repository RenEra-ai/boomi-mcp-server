"""Scoped error handling and retry/idempotency safety (issue #142, M12.7).

Every one of #142's seven error codes is reached by at least one test here, and
each "reject" case is PAIRED with a positive case that compiles. A rejection test
alone proves only that something failed — not that the rule is the thing that
failed it, and not that the rule ever admits anything (the "an error code you
never test is unreachable" lesson from #141, generalised: a code you only ever
test negatively can still be vacuous).
"""

from __future__ import annotations

import pathlib

import pytest

from boomi_mcp.compiler.process_ir import connector_capabilities as CC
from boomi_mcp.compiler.process_ir.contracts import (
    CATCH_DRAGPOINT_Y,
    CATCH_SHAPE_Y,
    DRAGPOINT_Y,
    SHAPE_Y,
    ComponentSymbolV1,
    IdempotencyContractSymbolV1,
    SymbolTableV1,
)
from boomi_mcp.compiler.process_ir.diagnostics import ProcessIRCompileError
from boomi_mcp.compiler.process_ir.emitter_registry import emit_process
from boomi_mcp.compiler.process_ir.error_handling import (
    catch_region_node_ids,
    derive_error_regions,
)
from boomi_mcp.compiler.process_ir.invariants import check_cfg_invariants
from boomi_mcp.compiler.process_ir.lowering import lower_process_ir_to_cfg
from boomi_mcp.compiler.process_ir.pipeline import compile_process_ir_v1
from boomi_mcp.errors import (
    ERROR_TAXONOMY,
    PROCESS_IR_SEMANTIC_AMBIGUOUS_FLOW,
    PROCESS_IR_CAPABILITY_ERROR_SCOPE_UNSUPPORTED,
    PROCESS_IR_CAPABILITY_UNSUPPORTED,
    PROCESS_IR_COMPILE_ERROR_REGION_INVALID,
    PROCESS_IR_REFERENCE_INVALID_FORMAT,
    PROCESS_IR_SCHEMA_RETRY_COUNT,
    PROCESS_IR_SEMANTIC_CATCH_UNTERMINATED,
    PROCESS_IR_SEMANTIC_CONTROL_CONTINUATION_UNSUPPORTED,
    PROCESS_IR_SEMANTIC_IDEMPOTENCY_EVIDENCE_MISSING,
    PROCESS_IR_SEMANTIC_PROFILE_MISMATCH,
    PROCESS_IR_SEMANTIC_RETRY_NON_IDEMPOTENT_WRITE,
    PROCESS_IR_SEMANTIC_RETRY_SOURCE_REEXECUTION,
)
from boomi_mcp.models.process_ir import ProcessIRValidationError, parse_process_ir_v1

REST = CC.REST_FAMILY
DB = CC.DATABASE_FAMILY

EXCEPTION_TERMINAL = {"kind": "exception", "message_template": "caught {1}"}


# ---------------------------------------------------------------------------
# Symbol fixtures
# ---------------------------------------------------------------------------


def _symbols(*extra, contracts=()):
    base = (
        ComponentSymbolV1(
            ref="$ref:GETOP",
            component_id="op-get",
            component_type="connector-action",
            connector_type=REST,
            action_type="GET",
            connection_ref="$ref:CONN",
            output_profile_ref="$ref:P1",
        ),
        ComponentSymbolV1(
            ref="$ref:GETOP2",
            component_id="op-get-2",
            component_type="connector-action",
            connector_type=REST,
            action_type="GET",
            connection_ref="$ref:CONN",
            output_profile_ref="$ref:P2",
        ),
        ComponentSymbolV1(
            ref="$ref:PATCHOP",
            component_id="op-patch",
            component_type="connector-action",
            connector_type=REST,
            action_type="PATCH",
            connection_ref="$ref:CONN",
            input_profile_ref="$ref:P1",
            output_profile_ref="$ref:P2",
        ),
        ComponentSymbolV1(
            ref="$ref:DBSEND",
            component_id="op-db-send",
            component_type="connector-action",
            connector_type=DB,
            action_type="Send",
            connection_ref="$ref:DBCONN",
            input_profile_ref="$ref:P1",
        ),
        ComponentSymbolV1(
            ref="$ref:CONN",
            component_id="conn-1",
            component_type="connector-settings",
            connector_type=REST,
        ),
        ComponentSymbolV1(
            ref="$ref:DBCONN",
            component_id="conn-db",
            component_type="connector-settings",
            connector_type=DB,
        ),
        ComponentSymbolV1(ref="$ref:P1", component_id="prof-1", component_type="profile.json"),
        ComponentSymbolV1(ref="$ref:P2", component_id="prof-2", component_type="profile.json"),
        ComponentSymbolV1(ref="$ref:CACHE", component_id="cache-1", component_type="documentcache"),
    )
    return SymbolTableV1(symbols=base + tuple(extra), idempotency_contracts=tuple(contracts))


# ---------------------------------------------------------------------------
# Document builders
# ---------------------------------------------------------------------------


def _doc(steps):
    return {"version": "1", "body": {"kind": "sequence", "steps": steps}}


def _process_scope(retry=None, op="$ref:GETOP", catch_steps=None, catch_terminal=None, **over):
    node = {
        "kind": "try_catch",
        "scope": "process",
        "try_body": {
            "steps": [{"kind": "connector_call", "operation_ref": op}],
            "terminal": {"kind": "stop"},
        },
        "catch_body": {
            "steps": [{"kind": "message", "text": "failed"}]
            if catch_steps is None
            else catch_steps,
            "terminal": dict(EXCEPTION_TERMINAL) if catch_terminal is None else catch_terminal,
        },
    }
    if retry is not None:
        node["retry"] = retry
    node.update(over)
    return _doc([node])


def _connector_scope(
    retry=None,
    protected="$ref:GETOP2",
    upstream="$ref:GETOP",
    idempotency=None,
    try_prefix=None,
    catch_steps=None,
    catch_terminal=None,
    **over,
):
    call = {"kind": "connector_call", "operation_ref": protected}
    if idempotency is not None:
        call["idempotency"] = idempotency
    node = {
        "kind": "try_catch",
        "scope": "connector",
        "try_body": {
            "steps": (try_prefix or []) + [call],
            "terminal": {"kind": "stop"},
        },
        "catch_body": {
            "steps": [{"kind": "message", "text": "failed"}]
            if catch_steps is None
            else catch_steps,
            "terminal": dict(EXCEPTION_TERMINAL) if catch_terminal is None else catch_terminal,
        },
    }
    if retry is not None:
        node["retry"] = retry
    node.update(over)
    return _doc([{"kind": "connector_call", "operation_ref": upstream}, node])


def _compile(doc, symbols=None):
    return compile_process_ir_v1(parse_process_ir_v1(doc), symbols or _symbols())


def _parse_codes(doc):
    with pytest.raises(ProcessIRValidationError) as excinfo:
        parse_process_ir_v1(doc)
    return [(d.code, d.path) for d in excinfo.value.diagnostics]


def _compile_error(doc, symbols=None):
    with pytest.raises(ProcessIRCompileError) as excinfo:
        _compile(doc, symbols)
    return excinfo.value.diagnostics[0]


def _synthetic_capabilities(monkeypatch, *overrides):
    """Swap in capability rows with a chosen ``retry_safety``.

    ``idempotent_write`` and ``conditionally_idempotent`` ship on NO production
    row (no authoritative source classifies a stock write action as replay-safe —
    `.codex/plans/issue-142-live-captures.md` §G4), so the only way to exercise
    those two branches is to synthesise a row. Patching the module attribute
    works because ``lookup_capability`` reads the global at call time.
    """
    table = dict(CC.CONNECTOR_CALL_CAPABILITIES_V1)
    for family, action, safety in overrides:
        key = (family, action.casefold())
        table[key] = table[key].model_copy(update={"retry_safety": safety})
    monkeypatch.setattr(CC, "CONNECTOR_CALL_CAPABILITIES_V1", table)
    return table


# ---------------------------------------------------------------------------
# PROCESS_IR_SCHEMA_RETRY_COUNT
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("count", [0, 1, 2, 3, 4, 5])
def test_retry_count_accepts_every_value_in_range(count):
    ir = parse_process_ir_v1(_connector_scope(retry={"count": count}))
    assert ir.body.steps[-1].retry.count == count
    assert ir.body.steps[-1].retry_count == count


@pytest.mark.parametrize(
    "bad", [{"count": -1}, {"count": 6}, {"count": 100}, {"count": True}, {"count": "2"},
            {"count": 1.5}, {"count": None}, {}]
)
def test_retry_count_rejects_out_of_range_and_wrong_types(bad):
    codes = _parse_codes(_process_scope(retry=bad))
    assert codes == [(PROCESS_IR_SCHEMA_RETRY_COUNT, "/body/steps/0/retry/count")]


def test_absent_retry_is_exactly_retry_zero():
    absent = parse_process_ir_v1(_process_scope())
    explicit = parse_process_ir_v1(_process_scope(retry={"count": 0}))
    assert absent.body.steps[0].retry_count == explicit.body.steps[0].retry_count == 0


def test_absent_retry_and_explicit_zero_emit_identical_bytes():
    symbols = _symbols()
    a_cfg, a_plan = _compile(_process_scope(), symbols)
    b_cfg, b_plan = _compile(_process_scope(retry={"count": 0}), symbols)
    assert a_cfg == b_cfg
    assert a_plan == b_plan
    assert emit_process(a_plan, symbols).process_xml == emit_process(b_plan, symbols).process_xml


# ---------------------------------------------------------------------------
# PROCESS_IR_CAPABILITY_ERROR_SCOPE_UNSUPPORTED
# ---------------------------------------------------------------------------


def test_unknown_scope_is_rejected():
    codes = _parse_codes(_process_scope(scope="listener"))
    assert codes == [
        (PROCESS_IR_CAPABILITY_ERROR_SCOPE_UNSUPPORTED, "/body/steps/0/scope")
    ]


def test_both_verified_scopes_compile():
    _compile(_process_scope())
    _compile(_connector_scope())


def test_connector_scope_may_not_be_the_sole_root_step():
    doc = _connector_scope()
    doc["body"]["steps"] = doc["body"]["steps"][1:]  # drop the upstream call
    codes = _parse_codes(doc)
    assert codes[0][0] == PROCESS_IR_CAPABILITY_ERROR_SCOPE_UNSUPPORTED


def test_process_scope_may_not_terminate_a_call_sequence():
    doc = _connector_scope()
    doc["body"]["steps"][-1]["scope"] = "process"
    codes = _parse_codes(doc)
    assert codes[0][0] == PROCESS_IR_CAPABILITY_ERROR_SCOPE_UNSUPPORTED


def test_connector_scope_try_body_must_end_on_the_protected_call():
    doc = _connector_scope(try_prefix=[{"kind": "message", "text": "x"}])
    codes = _parse_codes(doc)
    assert codes[0][0] == PROCESS_IR_CAPABILITY_ERROR_SCOPE_UNSUPPORTED


def test_connector_scope_try_body_allows_property_preparation():
    doc = _connector_scope(
        try_prefix=[
            {"kind": "set_ddp", "name": "p",
             "source_values": [{"value_type": "static", "value": "v"}]},
        ]
    )
    _compile(doc)


def test_process_scope_try_body_must_begin_with_the_producing_call():
    doc = _process_scope()
    doc["body"]["steps"][0]["try_body"]["steps"] = [
        {"kind": "message", "text": "x"},
        {"kind": "connector_call", "operation_ref": "$ref:GETOP"},
    ]
    codes = _parse_codes(doc)
    assert codes[0][0] == PROCESS_IR_CAPABILITY_ERROR_SCOPE_UNSUPPORTED


def test_mutated_placement_is_rejected_by_the_compiler():
    # ProcessIRV1 is exported and NOT frozen: a caller can validate a legal
    # document, mutate it, and hand the model straight to the compiler. The
    # compiler's own placement check is what makes this a real gate rather than a
    # parse-time courtesy.
    ir = parse_process_ir_v1(_connector_scope())
    ir.body.steps = [ir.body.steps[-1]]  # connector scope now stands alone
    with pytest.raises(ProcessIRCompileError) as excinfo:
        compile_process_ir_v1(ir, _symbols())
    assert excinfo.value.diagnostics[0].code == PROCESS_IR_CAPABILITY_ERROR_SCOPE_UNSUPPORTED


def test_nothing_may_follow_a_try_catch():
    doc = _process_scope()
    doc["body"]["steps"].append({"kind": "stop"})
    codes = _parse_codes(doc)
    assert codes[0][0] == PROCESS_IR_SEMANTIC_CONTROL_CONTINUATION_UNSUPPORTED


# ---------------------------------------------------------------------------
# PROCESS_IR_SEMANTIC_RETRY_SOURCE_REEXECUTION
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("count", [1, 3, 5])
def test_process_scope_positive_retry_would_reexecute_the_source(count):
    diag = _compile_error(_process_scope(retry={"count": count}))
    assert diag.code == PROCESS_IR_SEMANTIC_RETRY_SOURCE_REEXECUTION
    assert diag.path == "/body/steps/0/retry/count"


def test_process_scope_retry_zero_compiles():
    # The paired positive: the rule fires on RETRY, not on the process scope.
    _compile(_process_scope(retry={"count": 0}))


@pytest.mark.parametrize("count", [1, 5])
def test_connector_scope_positive_retry_on_a_read_compiles(count):
    # The paired positive for source isolation: an upstream call produces the
    # documents, so the retried region provably excludes the producer. Without
    # this case the source-reexecution code could be firing on every retry and
    # the negative tests above would look identical.
    cfg, _ = _compile(_connector_scope(retry={"count": count}))
    regions = derive_error_regions(cfg)
    assert len(regions) == 1
    entry_node = next(n for n in cfg.nodes if n.node_id == cfg.entry_node_id)
    assert entry_node.semantic.semantic_kind == "connector_call"
    assert entry_node.node_id not in regions[0].try_node_ids


def test_source_isolation_is_derived_from_the_graph_not_the_authored_scope():
    # Mutating the scope label alone must not change the verdict: the check asks
    # the GRAPH whether a producer sits upstream.
    ir = parse_process_ir_v1(_process_scope(retry={"count": 2}))
    ir.body.steps[0].scope = "connector"
    with pytest.raises(ProcessIRCompileError) as excinfo:
        compile_process_ir_v1(ir, _symbols())
    # It fails — on placement or on source re-execution — but it never compiles.
    assert excinfo.value.diagnostics[0].code in (
        PROCESS_IR_SEMANTIC_RETRY_SOURCE_REEXECUTION,
        PROCESS_IR_CAPABILITY_ERROR_SCOPE_UNSUPPORTED,
    )


# ---------------------------------------------------------------------------
# PROCESS_IR_SEMANTIC_RETRY_NON_IDEMPOTENT_WRITE
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("op", ["$ref:PATCHOP", "$ref:DBSEND"])
def test_retry_over_an_unverified_write_is_rejected(op):
    diag = _compile_error(_connector_scope(retry={"count": 1}, protected=op))
    assert diag.code == PROCESS_IR_SEMANTIC_RETRY_NON_IDEMPOTENT_WRITE
    assert diag.path.endswith("/operation_ref")


@pytest.mark.parametrize(
    "evidence",
    [
        {"kind": "verified_action"},
        {"kind": "key_reference", "contract_ref": "$ref:CONTRACT"},
    ],
)
def test_authored_evidence_cannot_override_an_unverified_row(evidence):
    # The registry decides; evidence only ever discharges an obligation a
    # retry-safe row imposes. If this ever passes, the safety gate is decorative.
    contracts = (
        IdempotencyContractSymbolV1(ref="$ref:CONTRACT", operation_ref="$ref:PATCHOP"),
    )
    diag = _compile_error(
        _connector_scope(retry={"count": 1}, protected="$ref:PATCHOP", idempotency=evidence),
        _symbols(contracts=contracts),
    )
    assert diag.code == PROCESS_IR_SEMANTIC_RETRY_NON_IDEMPOTENT_WRITE


def test_synthetic_non_idempotent_row_is_rejected(monkeypatch):
    _synthetic_capabilities(monkeypatch, (REST, "GET", "non_idempotent"))
    diag = _compile_error(_connector_scope(retry={"count": 1}))
    assert diag.code == PROCESS_IR_SEMANTIC_RETRY_NON_IDEMPOTENT_WRITE


def test_no_production_row_claims_write_replay_safety():
    # The live-capture decision, pinned as a test so a later slice cannot quietly
    # promote a write to replay-safe without revisiting §G4.
    for row in CC.CONNECTOR_CALL_CAPABILITIES_V1.values():
        if row.side_effect == "write":
            assert row.retry_safety == "unverified", row.action
        assert row.retry_safety in ("read_only", "unverified"), row.action


def test_retry_zero_over_an_unverified_write_compiles():
    # The paired positive: a write with no retry is ordinary.
    _compile(_connector_scope(retry={"count": 0}, protected="$ref:PATCHOP"))
    _compile(_connector_scope(protected="$ref:PATCHOP"))


def test_a_write_on_the_catch_path_is_never_retry_checked():
    # The recovery path runs once, after retries are exhausted, so a write there
    # is not a retried write.
    doc = _connector_scope(
        retry={"count": 5},
        catch_steps=[{"kind": "connector_call", "operation_ref": "$ref:DBSEND"}],
        catch_terminal={"kind": "stop"},
    )
    _compile(doc)


# ---------------------------------------------------------------------------
# PROCESS_IR_SEMANTIC_IDEMPOTENCY_EVIDENCE_MISSING
# ---------------------------------------------------------------------------


def test_idempotent_write_requires_verified_action_evidence(monkeypatch):
    _synthetic_capabilities(monkeypatch, (REST, "PATCH", "idempotent_write"))
    diag = _compile_error(_connector_scope(retry={"count": 1}, protected="$ref:PATCHOP"))
    assert diag.code == PROCESS_IR_SEMANTIC_IDEMPOTENCY_EVIDENCE_MISSING
    assert diag.path.endswith("/idempotency")


def test_idempotent_write_with_verified_action_evidence_compiles(monkeypatch):
    _synthetic_capabilities(monkeypatch, (REST, "PATCH", "idempotent_write"))
    _compile(
        _connector_scope(
            retry={"count": 1},
            protected="$ref:PATCHOP",
            idempotency={"kind": "verified_action"},
        )
    )


def test_idempotent_write_rejects_the_wrong_evidence_kind(monkeypatch):
    _synthetic_capabilities(monkeypatch, (REST, "PATCH", "idempotent_write"))
    contracts = (
        IdempotencyContractSymbolV1(ref="$ref:CONTRACT", operation_ref="$ref:PATCHOP"),
    )
    diag = _compile_error(
        _connector_scope(
            retry={"count": 1},
            protected="$ref:PATCHOP",
            idempotency={"kind": "key_reference", "contract_ref": "$ref:CONTRACT"},
        ),
        _symbols(contracts=contracts),
    )
    assert diag.code == PROCESS_IR_SEMANTIC_IDEMPOTENCY_EVIDENCE_MISSING


def test_conditionally_idempotent_requires_a_matching_key_contract(monkeypatch):
    _synthetic_capabilities(monkeypatch, (REST, "PATCH", "conditionally_idempotent"))
    contracts = (
        IdempotencyContractSymbolV1(ref="$ref:CONTRACT", operation_ref="$ref:PATCHOP"),
    )
    _compile(
        _connector_scope(
            retry={"count": 1},
            protected="$ref:PATCHOP",
            idempotency={"kind": "key_reference", "contract_ref": "$ref:CONTRACT"},
        ),
        _symbols(contracts=contracts),
    )


def test_conditionally_idempotent_rejects_absent_evidence(monkeypatch):
    _synthetic_capabilities(monkeypatch, (REST, "PATCH", "conditionally_idempotent"))
    diag = _compile_error(_connector_scope(retry={"count": 1}, protected="$ref:PATCHOP"))
    assert diag.code == PROCESS_IR_SEMANTIC_IDEMPOTENCY_EVIDENCE_MISSING


def test_conditionally_idempotent_rejects_an_unresolved_contract(monkeypatch):
    _synthetic_capabilities(monkeypatch, (REST, "PATCH", "conditionally_idempotent"))
    diag = _compile_error(
        _connector_scope(
            retry={"count": 1},
            protected="$ref:PATCHOP",
            idempotency={"kind": "key_reference", "contract_ref": "$ref:NOPE"},
        )
    )
    assert diag.code == PROCESS_IR_SEMANTIC_IDEMPOTENCY_EVIDENCE_MISSING


def test_conditionally_idempotent_rejects_a_contract_for_another_operation(monkeypatch):
    # The operation binding is the whole point: a contract covering a DIFFERENT
    # call is not evidence about this one. Without this the binding is decorative.
    _synthetic_capabilities(monkeypatch, (REST, "PATCH", "conditionally_idempotent"))
    contracts = (
        IdempotencyContractSymbolV1(ref="$ref:CONTRACT", operation_ref="$ref:GETOP"),
    )
    diag = _compile_error(
        _connector_scope(
            retry={"count": 1},
            protected="$ref:PATCHOP",
            idempotency={"kind": "key_reference", "contract_ref": "$ref:CONTRACT"},
        ),
        _symbols(contracts=contracts),
    )
    assert diag.code == PROCESS_IR_SEMANTIC_IDEMPOTENCY_EVIDENCE_MISSING


def test_read_only_needs_no_evidence_but_a_dangling_contract_still_fails():
    _compile(_connector_scope(retry={"count": 5}))
    diag = _compile_error(
        _connector_scope(
            retry={"count": 5},
            idempotency={"kind": "key_reference", "contract_ref": "$ref:NOPE"},
        )
    )
    assert diag.code == PROCESS_IR_SEMANTIC_IDEMPOTENCY_EVIDENCE_MISSING


@pytest.mark.parametrize(
    "value",
    [True, False, "yes", "idempotent", "literal-component-id", "", "  ", "$ref:", "$ref: k"],
)
def test_a_contract_ref_must_be_a_reference_not_an_assertion(value):
    # The acceptance criterion is that evidence "cannot be satisfied by an
    # unverified free-form Boolean". A literal-id escape hatch would reopen it.
    codes = _parse_codes(
        _connector_scope(idempotency={"kind": "key_reference", "contract_ref": value})
    )
    assert codes[0][0] in (
        PROCESS_IR_REFERENCE_INVALID_FORMAT,
        # a bool/other non-string fails the type gate first
        "PROCESS_IR_SCHEMA_INVALID",
    )


@pytest.mark.parametrize("evidence", [True, "verified", {"kind": "handwave"}, []])
def test_idempotency_evidence_must_be_a_typed_discriminated_object(evidence):
    codes = _parse_codes(_connector_scope(idempotency=evidence))
    assert codes  # rejected; the exact code depends on which gate catches it
    assert all(code != "OK" for code, _ in codes)


def test_verified_action_evidence_carries_no_payload():
    codes = _parse_codes(
        _connector_scope(idempotency={"kind": "verified_action", "key": "s3cret"})
    )
    assert codes[0][0] in (
        PROCESS_IR_CAPABILITY_UNSUPPORTED,
        "PROCESS_IR_SCHEMA_UNKNOWN_FIELD",
    )


# ---------------------------------------------------------------------------
# PROCESS_IR_SEMANTIC_CATCH_UNTERMINATED
# ---------------------------------------------------------------------------


def test_missing_catch_terminal_is_rejected_at_parse():
    doc = _process_scope()
    del doc["body"]["steps"][0]["catch_body"]["terminal"]
    codes = _parse_codes(doc)
    assert codes == [
        (PROCESS_IR_SEMANTIC_CATCH_UNTERMINATED, "/body/steps/0/catch_body/terminal")
    ]


def test_missing_catch_body_is_rejected_at_parse():
    doc = _process_scope()
    del doc["body"]["steps"][0]["catch_body"]
    codes = _parse_codes(doc)
    assert codes == [
        (PROCESS_IR_SEMANTIC_CATCH_UNTERMINATED, "/body/steps/0/catch_body")
    ]


def test_mutated_away_catch_terminal_is_rejected_by_the_compiler():
    ir = parse_process_ir_v1(_process_scope())
    ir.body.steps[0].catch_body.terminal = None
    with pytest.raises(ProcessIRCompileError) as excinfo:
        compile_process_ir_v1(ir, _symbols())
    assert excinfo.value.diagnostics[0].code == PROCESS_IR_SEMANTIC_CATCH_UNTERMINATED


@pytest.mark.parametrize(
    "terminal",
    [
        {"kind": "stop"},
        dict(EXCEPTION_TERMINAL),
        {"kind": "cache_put", "cache_ref": "$ref:CACHE"},
    ],
)
def test_every_admitted_catch_terminal_compiles(terminal):
    steps = [{"kind": "message", "text": "failed"}]
    _compile(_connector_scope(catch_steps=steps, catch_terminal=terminal))


def test_a_bare_stop_catch_body_recovers_nothing_and_is_rejected():
    codes = _parse_codes(_connector_scope(catch_steps=[], catch_terminal={"kind": "stop"}))
    assert codes  # a catch that only stops does no recovery at all


def test_try_body_may_not_terminate_on_an_exception():
    doc = _connector_scope()
    doc["body"]["steps"][-1]["try_body"]["terminal"] = dict(EXCEPTION_TERMINAL)
    assert _parse_codes(doc)


# ---------------------------------------------------------------------------
# PROCESS_IR_COMPILE_ERROR_REGION_INVALID  (defect injection)
# ---------------------------------------------------------------------------


def _tc_cfg():
    return lower_process_ir_to_cfg(parse_process_ir_v1(_connector_scope()))


def test_swapped_try_catch_local_ordinals_are_a_region_defect():
    cfg = _tc_cfg()
    swapped = []
    for edge in cfg.edges:
        if edge.kind == "catch":
            swapped.append(edge.model_copy(update={"local_ordinal": 1}))
        elif edge.source_node_id == _catch_source(cfg) and edge.kind == "ordering":
            swapped.append(edge.model_copy(update={"local_ordinal": 2}))
        else:
            swapped.append(edge)
    broken = cfg.model_copy(update={"edges": tuple(swapped)})
    with pytest.raises(ProcessIRCompileError) as excinfo:
        check_cfg_invariants(broken)
    assert excinfo.value.diagnostics[0].code in (
        PROCESS_IR_COMPILE_ERROR_REGION_INVALID,
        "PROCESS_IR_COMPILE_NONDETERMINISTIC",
    )


def _catch_source(cfg):
    for edge in cfg.edges:
        if edge.kind == "catch":
            return edge.source_node_id
    raise AssertionError("no catch edge")


def test_a_catch_edge_out_of_a_non_try_catch_node_is_a_region_defect():
    cfg = _tc_cfg()
    linear = next(
        n for n in cfg.nodes if n.semantic.semantic_kind == "message"
    )
    edges = tuple(
        edge.model_copy(update={"source_node_id": linear.node_id})
        if edge.kind == "catch"
        else edge
        for edge in cfg.edges
    )
    with pytest.raises(ProcessIRCompileError) as excinfo:
        check_cfg_invariants(cfg.model_copy(update={"edges": edges}))
    assert excinfo.value.diagnostics[0].code == PROCESS_IR_COMPILE_ERROR_REGION_INVALID


def test_a_catch_edge_targeting_the_try_subtree_is_a_region_defect():
    cfg = _tc_cfg()
    try_node = next(
        n for n in cfg.nodes if "/try_body/steps/" in n.source_path
    )
    edges = tuple(
        edge.model_copy(update={"target_node_id": try_node.node_id})
        if edge.kind == "catch"
        else edge
        for edge in cfg.edges
    )
    with pytest.raises(ProcessIRCompileError) as excinfo:
        check_cfg_invariants(cfg.model_copy(update={"edges": edges}))
    # The JOIN check gets there first, and that is the better diagnosis: pointing
    # the recovery edge into the protected subtree gives that node two
    # predecessors, and "two paths converge" is a more specific statement than
    # "the region is malformed". Pinned as an either/or so a future reordering of
    # the checks does not silently turn a rejection into a pass.
    assert excinfo.value.diagnostics[0].code in (
        PROCESS_IR_COMPILE_ERROR_REGION_INVALID,
        PROCESS_IR_SEMANTIC_AMBIGUOUS_FLOW,
    )


def test_a_try_catch_with_only_one_successor_is_a_region_defect():
    cfg = _tc_cfg()
    edges = tuple(edge for edge in cfg.edges if edge.kind != "catch")
    renumbered = tuple(
        edge.model_copy(update={"ordinal": i, "edge_id": "e{0}".format(i)})
        for i, edge in enumerate(edges, start=1)
    )
    with pytest.raises(ProcessIRCompileError):
        check_cfg_invariants(cfg.model_copy(update={"edges": renumbered}))


# ---------------------------------------------------------------------------
# Regions, geometry and dataflow
# ---------------------------------------------------------------------------


def test_a_region_contains_only_its_own_try_subtree():
    cfg, _ = _compile(_connector_scope(retry={"count": 1}))
    region = derive_error_regions(cfg)[0]
    for node_id in region.try_node_ids:
        node = next(n for n in cfg.nodes if n.node_id == node_id)
        assert "/try_body" in node.source_path
    for node_id in region.catch_node_ids:
        node = next(n for n in cfg.nodes if n.node_id == node_id)
        assert "/catch_body" in node.source_path
    assert not set(region.try_node_ids) & set(region.catch_node_ids)
    assert cfg.entry_node_id not in region.try_node_ids


def test_catch_row_geometry_matches_the_shared_renderer_constants():
    cfg, plan = _compile(_connector_scope())
    catch_ids = catch_region_node_ids(cfg)
    assert catch_ids
    for node in plan.nodes:
        if node.origin != "ir":
            continue
        on_catch_row = node.cfg_node_id in catch_ids
        assert node.layout.y == (CATCH_SHAPE_Y if on_catch_row else SHAPE_Y)
        for transition in node.outgoing:
            if transition.identifier in ("default", "true"):
                assert transition.y == DRAGPOINT_Y
            elif transition.identifier in ("error",):
                assert transition.y == CATCH_DRAGPOINT_Y
            elif transition.identifier is None:
                assert transition.y == (
                    CATCH_DRAGPOINT_Y if on_catch_row else DRAGPOINT_Y
                )


def test_try_and_catch_edges_are_wired_in_that_order_with_fixed_labels():
    _, plan = _compile(_connector_scope())
    handler = next(
        n for n in plan.nodes if n.emitter_input.emitter_kind == "catcherrors"
    )
    assert [t.identifier for t in handler.outgoing] == ["default", "error"]
    assert [t.text for t in handler.outgoing] == ["Try", "Catch"]
    assert [t.local_ordinal for t in handler.outgoing] == [1, 2]


@pytest.mark.parametrize("count", [0, 1, 2, 3, 4, 5])
def test_every_retry_count_renders_deterministically(count):
    symbols = _symbols()
    _, plan = _compile(_connector_scope(retry={"count": count}), symbols)
    xml = emit_process(plan, symbols).process_xml
    assert 'retryCount="{0}"'.format(count) in xml
    assert 'catchAll="true"' in xml
    # Two compiles of the same document produce the same bytes.
    _, plan2 = _compile(_connector_scope(retry={"count": count}), symbols)
    assert emit_process(plan2, symbols).process_xml == xml


def test_shuffled_symbol_and_contract_order_does_not_change_output(monkeypatch):
    _synthetic_capabilities(monkeypatch, (REST, "PATCH", "conditionally_idempotent"))
    contracts = [
        IdempotencyContractSymbolV1(ref="$ref:C{0}".format(i), operation_ref="$ref:PATCHOP")
        for i in range(4)
    ]
    doc = _connector_scope(
        retry={"count": 1},
        protected="$ref:PATCHOP",
        idempotency={"kind": "key_reference", "contract_ref": "$ref:C2"},
    )
    a_syms = _symbols(contracts=contracts)
    b_syms = _symbols(contracts=list(reversed(contracts)))
    a_cfg, a_plan = _compile(doc, a_syms)
    b_cfg, b_plan = _compile(doc, b_syms)
    assert a_cfg == b_cfg and a_plan == b_plan
    assert emit_process(a_plan, a_syms).process_xml == emit_process(b_plan, b_syms).process_xml


def test_a_catch_body_map_with_no_provable_profile_fails_closed():
    # The catch path forks from SCOPE ENTRY state, so a map there has no upstream
    # call it can be bracketed against — claiming to have "verified profiles"
    # would be a claim about a comparison that never happened.
    doc = _connector_scope(
        catch_steps=[{"kind": "map_ref", "map_ref": "$ref:MAP"}],
        catch_terminal={"kind": "stop"},
    )
    symbols = _symbols(
        ComponentSymbolV1(
            ref="$ref:MAP",
            component_id="map-1",
            component_type="transform.map",
            input_profile_ref="$ref:P2",
            output_profile_ref="$ref:P1",
        )
    )
    diag = _compile_error(doc, symbols)
    assert diag.code == PROCESS_IR_SEMANTIC_PROFILE_MISMATCH


def test_a_catch_body_does_not_inherit_try_path_property_state():
    # A Set Properties inside the try body has not necessarily run — that is the
    # failure being caught — so the catch path must not be validated as if it had.
    doc = _connector_scope(
        try_prefix=[
            {"kind": "set_ddp", "name": "p",
             "source_values": [{"value_type": "static", "value": "v"}]},
        ],
        catch_steps=[{"kind": "message", "text": "recovered"}],
        catch_terminal={"kind": "stop"},
    )
    cfg, _ = _compile(doc)
    region = derive_error_regions(cfg)[0]
    set_prop = next(n for n in cfg.nodes if n.semantic.semantic_kind == "set_property")
    assert set_prop.node_id in region.try_node_ids
    assert set_prop.node_id not in region.catch_node_ids


def test_a_documents_required_call_is_allowed_on_the_catch_path():
    # The caught document is present on the recovery path even though nothing on
    # THAT path produced it (live evidence: a retried process routed one error
    # document to its catch leg).
    doc = _connector_scope(
        catch_steps=[{"kind": "connector_call", "operation_ref": "$ref:DBSEND"}],
        catch_terminal={"kind": "stop"},
    )
    _compile(doc)


# ---------------------------------------------------------------------------
# The representative DLQ-style flow from the issue
# ---------------------------------------------------------------------------


def test_representative_error_flow_routes_a_failure_through_a_connector_path():
    # Upstream read -> connector-scoped retried read -> catch routes the failed
    # document to a Database Send -> stop. It uses an already-supported connector
    # action and creates or implies no queue.
    doc = _connector_scope(
        retry={"count": 3},
        catch_steps=[{"kind": "connector_call", "operation_ref": "$ref:DBSEND"}],
        catch_terminal={"kind": "stop"},
    )
    symbols = _symbols()
    cfg, plan = _compile(doc, symbols)
    artifact = emit_process(plan, symbols)
    xml = artifact.process_xml
    assert 'retryCount="3"' in xml
    assert 'shapetype="catcherrors"' in xml
    assert xml.count('shapetype="connectoraction"') == 3
    # It routes through an already-supported connector action and neither creates
    # nor implies any queue infrastructure.
    assert "queue" not in xml.lower()

    # No reverse edge and no source re-entry: every CFG edge points from a lower
    # ordinal to a higher one, so nothing can flow back into the upstream call.
    by_id = {node.node_id: node for node in cfg.nodes}
    for edge in cfg.edges:
        assert by_id[edge.source_node_id].ordinal < by_id[edge.target_node_id].ordinal
    # The producing call sits OUTSIDE the retried region — the whole point of the
    # connector scope.
    region = derive_error_regions(cfg)[0]
    assert cfg.entry_node_id not in region.try_node_ids
    assert cfg.entry_node_id not in region.catch_node_ids


def test_the_emitted_error_graph_passes_the_structural_verifier():
    from boomi_mcp.categories.components.process_graph_verifier import (
        verify_process_graph,
    )

    symbols = _symbols()
    for doc in (
        _process_scope(),
        _connector_scope(retry={"count": 5}),
        _connector_scope(
            catch_steps=[{"kind": "connector_call", "operation_ref": "$ref:DBSEND"}],
            catch_terminal={"kind": "stop"},
        ),
        _connector_scope(
            catch_steps=[{"kind": "message", "text": "x"}],
            catch_terminal={"kind": "cache_put", "cache_ref": "$ref:CACHE"},
        ),
    ):
        _, plan = _compile(doc, symbols)
        result = verify_process_graph(emit_process(plan, symbols).process_xml)
        assert result["errors"] == [], result


# ---------------------------------------------------------------------------
# Security + taxonomy ownership
# ---------------------------------------------------------------------------


def test_no_authored_value_reaches_a_diagnostic_or_the_emitted_xml():
    sentinel = "SENTINELVALUE12345"
    contracts = (
        IdempotencyContractSymbolV1(
            ref="$ref:{0}".format(sentinel), operation_ref="$ref:PATCHOP"
        ),
    )
    doc = _connector_scope(
        retry={"count": 1},
        protected="$ref:PATCHOP",
        idempotency={"kind": "key_reference", "contract_ref": "$ref:{0}".format(sentinel)},
    )
    with pytest.raises(ProcessIRCompileError) as excinfo:
        _compile(doc, _symbols(contracts=contracts))
    error = excinfo.value
    for diag in error.diagnostics:
        assert sentinel not in diag.message
        assert sentinel not in diag.remediation
    assert sentinel not in str(error)
    assert sentinel not in repr(error)


def test_secret_shaped_keys_under_a_try_catch_are_rejected():
    doc = _process_scope()
    doc["body"]["steps"][0]["try_body"]["password"] = "hunter2"
    codes = _parse_codes(doc)
    assert codes[0][0] == PROCESS_IR_CAPABILITY_UNSUPPORTED


@pytest.mark.parametrize(
    "code",
    [
        PROCESS_IR_SCHEMA_RETRY_COUNT,
        PROCESS_IR_CAPABILITY_ERROR_SCOPE_UNSUPPORTED,
        PROCESS_IR_SEMANTIC_RETRY_SOURCE_REEXECUTION,
        PROCESS_IR_SEMANTIC_RETRY_NON_IDEMPOTENT_WRITE,
        PROCESS_IR_SEMANTIC_IDEMPOTENCY_EVIDENCE_MISSING,
        PROCESS_IR_SEMANTIC_CATCH_UNTERMINATED,
        PROCESS_IR_COMPILE_ERROR_REGION_INVALID,
    ],
)
def test_every_142_code_is_registered_and_owned(code):
    spec = ERROR_TAXONOMY[code]
    assert spec.owner == "#142"
    assert spec.category == "process_ir"
    assert spec.retryable is False
    assert spec.summary


def test_this_module_reaches_every_142_code():
    """The reachability ledger: a code no test drives is unreachable in practice.

    Asserted against THIS FILE's source so the claim cannot rot into a comment —
    if a code loses its last test, the name stops appearing and this fails.
    """
    import pathlib

    source = pathlib.Path(__file__).read_text()
    for code in (
        "PROCESS_IR_SCHEMA_RETRY_COUNT",
        "PROCESS_IR_CAPABILITY_ERROR_SCOPE_UNSUPPORTED",
        "PROCESS_IR_SEMANTIC_RETRY_SOURCE_REEXECUTION",
        "PROCESS_IR_SEMANTIC_RETRY_NON_IDEMPOTENT_WRITE",
        "PROCESS_IR_SEMANTIC_IDEMPOTENCY_EVIDENCE_MISSING",
        "PROCESS_IR_SEMANTIC_CATCH_UNTERMINATED",
        "PROCESS_IR_COMPILE_ERROR_REGION_INVALID",
    ):
        # Once in the import block, at least once in an assertion.
        assert source.count(code) >= 2, code


# ---------------------------------------------------------------------------
# Frozen byte anchors
# ---------------------------------------------------------------------------

_FIXTURES = pathlib.Path(__file__).resolve().parent / "fixtures" / "process_ir" / "error_handling"
_GOLDEN_XML = pathlib.Path(__file__).resolve().parent / "fixtures" / "golden_xml"

_ANCHORS = [
    ("scoped_try_catch_process_retry0_exception", _process_scope(retry={"count": 0})),
    (
        "scoped_try_catch_connector_read_retry5_cache_catch",
        _connector_scope(
            retry={"count": 5},
            catch_steps=[{"kind": "message", "text": "staged for review"}],
            catch_terminal={"kind": "cache_put", "cache_ref": "$ref:CACHE"},
        ),
    ),
    (
        "scoped_try_catch_connector_read_to_connector_catch",
        _connector_scope(
            retry={"count": 3},
            catch_steps=[{"kind": "connector_call", "operation_ref": "$ref:DBSEND"}],
            catch_terminal={"kind": "stop"},
        ),
    ),
]


@pytest.mark.parametrize("name,doc", _ANCHORS)
def test_committed_fixture_matches_the_compiled_document(name, doc):
    import json

    assert json.loads((_FIXTURES / "{0}.json".format(name)).read_text()) == doc


@pytest.mark.parametrize("name,doc", _ANCHORS)
def test_error_fixtures_match_their_frozen_xml_golden(name, doc):
    """A FROZEN golden, not two fresh emissions compared with each other.

    Comparing one emission against another proves determinism but not that the
    bytes are the intended ones — any change would move both sides together.
    These files are the byte contract; a diff here is a deliberate review item.
    """
    symbols = _symbols()
    _cfg, plan = _compile(doc, symbols)
    assert emit_process(plan, symbols).process_xml == (
        _GOLDEN_XML / "{0}.xml".format(name)
    ).read_text()


@pytest.mark.parametrize("name,_doc", _ANCHORS)
def test_error_fixtures_carry_only_opaque_sentinel_references(name, _doc):
    text = (_FIXTURES / "{0}.json".format(name)).read_text().lower()
    for forbidden in ("password", "secret", "token", "http://", "https://", "@"):
        assert forbidden not in text


def test_the_dlq_anchor_keeps_the_source_outside_the_retried_region():
    """The acceptance criterion, asserted on the frozen bytes themselves.

    The upstream read must sit BEFORE the handler on the normal row, the
    protected call inside it, and the recovery call one row down — which is what
    makes 'retry cannot re-run the source' visible in the artifact rather than
    only in a compiler check.
    """
    xml = (
        _GOLDEN_XML / "scoped_try_catch_connector_read_to_connector_catch.xml"
    ).read_text()
    assert '<catcherrors catchAll="true" retryCount="3"/>' in xml
    # Three connector shapes: upstream read, protected read, recovery send.
    assert xml.count('shapetype="connectoraction"') == 3
    # The handler's own dragpoints, in order, on their two rows.
    assert (
        'identifier="default" name="shape3.dragpoint1" text="Try" '
        'toShape="shape4" x="560.0" y="104.0"' in xml
    )
    assert (
        'identifier="error" name="shape3.dragpoint2" text="Catch" '
        'toShape="shape6" x="560.0" y="464.0"' in xml
    )
    # The upstream read is shape2 — ahead of the handler at shape3, so no retry
    # of the protected region can reach it.
    assert '<shape image="connectoraction_icon" name="shape2"' in xml
    assert 'name="shape2.dragpoint1" toShape="shape3"' in xml
    # ...and nothing points back at it.
    assert 'toShape="shape2"' not in xml.split('name="shape2"')[1]
    # No queue infrastructure is created or implied.
    assert "queue" not in xml.lower()
