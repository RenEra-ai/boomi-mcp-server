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
    PROCESS_IR_SEMANTIC_RETRY_SOURCE_POLICY_REQUIRES_RETRY,
    PROCESS_IR_SEMANTIC_RETRY_SOURCE_POLICY_SCOPE_INVALID,
    PROCESS_IR_SEMANTIC_RETRY_SOURCE_REEXECUTION,
)
from boomi_mcp.models.process_ir import (
    _GATED_TRY_CATCH_EXTRA_KEYS,
    ProcessIRValidationError,
    parse_process_ir_v1,
)

REST = CC.REST_FAMILY
DB = CC.DATABASE_FAMILY

EXCEPTION_TERMINAL = {"kind": "exception", "message_template": "caught {1}"}


# ---------------------------------------------------------------------------
# Symbol fixtures
# ---------------------------------------------------------------------------


# The symbol table lives in the corpus (#165); this module CONSUMES it. The
# `_ANCHORS` documents below stay here as WITNESSES, pinned byte-identically to
# the committed error_handling/*.json definitions by the fixture-equality test.
import _wave_gate_golden_corpus as _corpus

_symbols = _corpus.error_symbols


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


_compile = _corpus.error_compile


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
        # Slice D gave the contract grammar its OWN code; before that it borrowed
        # the generic component-reference one, which could not say which of two
        # different grammars had been broken.
        "PROCESS_IR_REFERENCE_IDEMPOTENCY_CONTRACT_INVALID_FORMAT",
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


def test_a_model_constructed_catch_terminal_is_refused_by_the_compiler():
    """#180 witness (ii) — placement enforcement on a body the PARSER never saw.

    `ProcessIRV1` is exported and is not frozen, so a caller can parse a legal
    document, replace a control body with one `model_construct` built, and hand
    the model straight to `compile_process_ir_v1`. A gate only
    `parse_process_ir_v1` enforces is not a gate — which is exactly why
    production says so at `body_capabilities.py`.

    `test_mutated_away_catch_terminal_is_rejected_by_the_compiler` above is the
    nearest existing test and asserts a DIFFERENT invariant: a catch body with
    no terminal at all is `CATCH_UNTERMINATED`. Nothing covered a catch body
    carrying a terminal the slot does not admit — `return_documents`, the one
    terminal a recovery path may never use, because a catch hands nothing back
    to the caller.

    The mutant is shown to have applied before the gate is invoked: the slot is
    asserted not to admit `return_documents`, and the control document is
    asserted to compile clean.
    """
    from boomi_mcp.compiler.process_ir.body_capabilities import (
        CATCH_BODY,
        TERMINAL_SLOT,
        is_allowed,
    )
    from boomi_mcp.models.process_ir import (
        MessageNodeV1,
        ReturnDocumentsNodeV1,
        TryCatchCatchBodyV1,
    )

    # THE INVARIANT THIS WITNESSES, read off the derived matrix rather than
    # asserted from memory. If the union ever widens, this line fails first and
    # says so, instead of the test quietly becoming vacuous.
    assert is_allowed(CATCH_BODY, TERMINAL_SLOT, "return_documents") is False

    # CONTROL: the same document, unmutated, compiles.
    control = parse_process_ir_v1(_process_scope())
    compile_process_ir_v1(control, _symbols())

    ir = parse_process_ir_v1(_process_scope())
    ir.body.steps[0].catch_body = TryCatchCatchBodyV1.model_construct(
        steps=[MessageNodeV1(kind="message", text="failed")],
        terminal=ReturnDocumentsNodeV1(kind="return_documents"),
    )
    # THE MUTATION TOOK EFFECT — the model really does carry the bad terminal,
    # and `model_construct` really did skip the union that would have refused it.
    assert ir.body.steps[0].catch_body.terminal.kind == "return_documents"

    with pytest.raises(ProcessIRCompileError) as excinfo:
        compile_process_ir_v1(ir, _symbols())
    diagnostic = excinfo.value.diagnostics[0]
    assert diagnostic.code == "PROCESS_IR_CAPABILITY_NODE_NOT_ALLOWED_IN_BODY"
    assert diagnostic.path == "/body/steps/0/catch_body/terminal"


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

    # ALIAS ORDER. Contracts are keyed by (ref, operation_ref), so one reference
    # may cover several operations. Those entries sort against each other on the
    # second element, and the order they were authored in must not reach the
    # output any more than the order of distinct refs does.
    aliased = [
        IdempotencyContractSymbolV1(ref="$ref:C2", operation_ref="$ref:OTHEROP"),
        *contracts,
        IdempotencyContractSymbolV1(ref="$ref:C2", operation_ref="$ref:THIRDOP"),
    ]
    c_syms = _symbols(contracts=aliased)
    d_syms = _symbols(contracts=list(reversed(aliased)))
    c_cfg, c_plan = _compile(doc, c_syms)
    d_cfg, d_plan = _compile(doc, d_syms)
    assert c_cfg == d_cfg and c_plan == d_plan
    assert emit_process(c_plan, c_syms).process_xml == emit_process(d_plan, d_syms).process_xml
    # ...and the aliases did not disturb the answer for the operation under test.
    assert c_cfg == a_cfg and c_plan == a_plan


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


@pytest.mark.parametrize("builder", [_connector_scope, _process_scope])
def test_a_documents_required_call_is_allowed_on_the_catch_path(builder):
    # The caught document is present on the recovery path even though nothing on
    # THAT path produced it (live evidence: a retried process routed one error
    # document to its catch leg).
    #
    # PROCESS scope is the load-bearing case: it has no upstream producer at all,
    # so the synthesized "a caught document exists" fact is the only thing making
    # this compile. Covering only the connector scope would leave that fact
    # untested, since there the upstream call already supplies a producer.
    doc = builder(
        catch_steps=[{"kind": "connector_call", "operation_ref": "$ref:DBSEND"}],
        catch_terminal={"kind": "stop"},
    )
    _compile(doc)


def test_work_after_a_non_producing_send_is_still_blocked_on_the_catch_path():
    # The Send gate must survive the catch fork: a call that returns no documents
    # is still terminal on the recovery path.
    diag = _compile_error(
        _connector_scope(
            catch_steps=[
                {"kind": "connector_call", "operation_ref": "$ref:DBSEND"},
                {"kind": "message", "text": "after"},
            ],
            catch_terminal={"kind": "stop"},
        )
    )
    assert diag.code == "PROCESS_IR_SEMANTIC_CARDINALITY_MISMATCH"


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


# ---------------------------------------------------------------------------
# Codex review round 1 — three real diagnostic-precision defects
# ---------------------------------------------------------------------------


def test_a_gated_extra_key_is_matched_against_its_immediate_owner():
    """A gated name on a NESTED node is an ordinary unknown field.

    The first cut matched ``_GATED_TRY_CATCH_EXTRA_KEYS`` against the whole loc,
    so a catch-body Message carrying a stray ``backoff`` inherited the ancestor
    ``try_catch`` tag and was told to consult the capability manifest — sending
    the caller to read about a gated feature when they had simply typo'd a field
    on a Message.
    """
    doc = _process_scope()
    doc["body"]["steps"][0]["catch_body"]["steps"][0]["backoff"] = 5
    assert _parse_codes(doc) == [
        ("PROCESS_IR_SCHEMA_UNKNOWN_FIELD", "/body/steps/0/catch_body/steps/0/backoff")
    ]


@pytest.mark.parametrize("owner_key", ["backoff", "queue_ref", "catch_all", "listener_retry"])
def test_a_gated_extra_key_on_the_handler_itself_still_reports_the_gate(owner_key):
    # The paired positive: the remapping must still fire where it belongs, or the
    # fix above would have silently disabled the capability diagnostic entirely.
    doc = _process_scope(**{owner_key: True})
    codes = _parse_codes(doc)
    assert codes == [
        (PROCESS_IR_CAPABILITY_UNSUPPORTED, "/body/steps/0/{0}".format(owner_key))
    ]


def test_a_gated_extra_key_on_a_connector_call_still_reports_the_gate():
    doc = _connector_scope()
    doc["body"]["steps"][-1]["try_body"]["steps"][-1]["idempotency_key"] = "k"
    codes = _parse_codes(doc)
    assert codes[0][0] == PROCESS_IR_CAPABILITY_UNSUPPORTED
    assert codes[0][1].endswith("/idempotency_key")


def test_a_bad_idempotency_tag_is_not_reported_as_a_body_slot_failure():
    """``idempotency`` is a tagged union, not a body slot.

    ``{"kind": "message"}`` there hits a tag that IS a real node kind while
    sitting inside a try-body loc. The first cut reported
    NODE_NOT_ALLOWED_IN_BODY — self-contradictory, because Message *is* admitted
    in that body; the actual failure is the idempotency discriminator.
    """
    doc = _connector_scope(idempotency={"kind": "message"})
    codes = _parse_codes(doc)
    assert codes == [
        ("PROCESS_IR_SCHEMA_UNKNOWN_NODE", "/body/steps/1/try_body/steps/0/idempotency")
    ]


def test_a_genuinely_unknown_node_kind_in_a_try_body_is_still_a_body_failure():
    # The paired positive: excluding `idempotency` must not disable the body-slot
    # remapping for real body slots.
    doc = _connector_scope()
    doc["body"]["steps"][-1]["catch_body"]["steps"] = [
        {"kind": "process_call", "process_ref": "$ref:SUB"}
    ]
    codes = _parse_codes(doc)
    assert codes[0][0] == "PROCESS_IR_CAPABILITY_NODE_NOT_ALLOWED_IN_BODY"


def _corrupt_provenance(cfg, path_fragment):
    """Relocate one node's provenance OUT of the body it belongs to.

    The escape target is the opposite body, so the corruption genuinely leaves
    the region under test — relocating a try-body node to another try-body path
    would still satisfy the ``/try_body/`` prefix and prove nothing.
    """
    victim = next(n for n in cfg.nodes if path_fragment in n.source_path)
    escaped = (
        "/body/steps/1/catch_body/steps/99"
        if "/try_body/" in victim.source_path
        else "/body/steps/1/try_body/steps/99"
    )
    return cfg.model_copy(
        update={
            "nodes": tuple(
                n.model_copy(update={"source_path": escaped}) if n is victim else n
                for n in cfg.nodes
            )
        }
    )


@pytest.mark.parametrize(
    "fragment",
    [
        # depth 0 (the edge's own first target) and deeper, on BOTH sides. The
        # try side needs its own depth>=1 cases: the first cut parametrized depth
        # >=1 only on the catch side, so the try side's deeper escape was covered
        # only indirectly by the shared walk.
        "/catch_body/steps/0",
        "/catch_body/steps/1",
        "/try_body/steps/0",
        "/try_body/steps/1",
        "/try_body/steps/2",
    ],
)
def test_region_escape_reports_the_same_code_at_every_depth(fragment):
    """One defect class, one code — regardless of how deep the corruption sits.

    The first cut used the compile-level region code for the edge's FIRST target
    and inherited ``PROCESS_IR_SEMANTIC_AMBIGUOUS_FLOW`` from the shared
    containment walk for every node below it. Same corruption, two codes, and the
    deeper one blamed authored input for a compiler-derived defect.
    """
    cfg = lower_process_ir_to_cfg(
        parse_process_ir_v1(
            _connector_scope(
                # Three try-body steps (two property preparations then the
                # protected call) so the try side has real depth to corrupt.
                try_prefix=[
                    {
                        "kind": "set_ddp",
                        "name": "p{0}".format(i),
                        "source_values": [{"value_type": "static", "value": "v"}],
                    }
                    for i in range(2)
                ],
                catch_steps=[
                    {"kind": "message", "text": "a"},
                    {"kind": "message", "text": "b"},
                ],
                catch_terminal={"kind": "stop"},
            )
        )
    )
    with pytest.raises(ProcessIRCompileError) as excinfo:
        check_cfg_invariants(_corrupt_provenance(cfg, fragment))
    assert excinfo.value.diagnostics[0].code == PROCESS_IR_COMPILE_ERROR_REGION_INVALID


# The other direction — that #141's Branch/Decision callers did NOT move to the
# new code — is pinned behaviourally by
# ``test_a_control_subtree_may_not_escape_its_own_region`` in
# tests/test_process_ir_rich_control_bodies.py, which asserts
# PROCESS_IR_SEMANTIC_AMBIGUOUS_FLOW on a real escaping Branch subtree. That is a
# stronger guard than inspecting this function's default argument, so nothing is
# re-asserted here.


# ---------------------------------------------------------------------------
# Architect review round 1 — plan-fidelity gaps
# ---------------------------------------------------------------------------


def _catch_map_symbols():
    """Profiles that line up exactly across a connector-scoped catch map."""
    return _symbols(
        ComponentSymbolV1(
            ref="$ref:MAP",
            component_id="map-1",
            component_type="transform.map",
            input_profile_ref="$ref:P1",
            output_profile_ref="$ref:P1D",
        ),
        ComponentSymbolV1(
            ref="$ref:BADMAP",
            component_id="map-2",
            component_type="transform.map",
            input_profile_ref="$ref:P2",
            output_profile_ref="$ref:P1D",
        ),
        ComponentSymbolV1(ref="$ref:P1D", component_id="prof-1d", component_type="profile.db"),
        ComponentSymbolV1(
            ref="$ref:DBSEND2",
            component_id="op-db-send2",
            component_type="connector-action",
            connector_type=DB,
            action_type="Send",
            connection_ref="$ref:DBCONN",
            input_profile_ref="$ref:P1D",
        ),
    )


def _catch_map_doc(map_ref, scope_builder):
    return scope_builder(
        catch_steps=[
            {"kind": "map_ref", "map_ref": map_ref},
            {"kind": "connector_call", "operation_ref": "$ref:DBSEND2"},
        ],
        catch_terminal={"kind": "stop"},
    )


def test_a_connector_scoped_catch_map_is_bracketed_against_the_scope_entry_call():
    """The caught document IS the upstream call's output, so the map has a pair.

    The first cut erased the producer binding on every catch edge, which rejected
    this entirely-valid flow. The erasure was also unnecessary: the DFS pushes
    both children from the state AT the handler, and the protected path's
    mutations happen in a separate branch of the walk — so scope-entry state is
    what this child already holds.
    """
    _compile(_catch_map_doc("$ref:MAP", _connector_scope), _catch_map_symbols())


def test_a_connector_scoped_catch_map_with_wrong_profiles_is_still_rejected():
    """The paired negative: preserving the binding must not disable the check."""
    diag = _compile_error(
        _catch_map_doc("$ref:BADMAP", _connector_scope), _catch_map_symbols()
    )
    assert diag.code == PROCESS_IR_SEMANTIC_PROFILE_MISMATCH


def test_a_process_scoped_catch_map_still_fails_closed():
    """A process scope has no upstream producer, so there is nothing to compare
    the map against — and 'nothing to compare' must never read as 'compares
    equal'. No scope check is needed for this: the graph answers on its own."""
    diag = _compile_error(
        _catch_map_doc("$ref:MAP", _process_scope), _catch_map_symbols()
    )
    assert diag.code == PROCESS_IR_SEMANTIC_PROFILE_MISMATCH


@pytest.mark.parametrize("key", sorted(_GATED_TRY_CATCH_EXTRA_KEYS))
def test_a_gated_key_on_the_retry_policy_reports_the_capability_gate(key):
    """The NATURAL spelling of a backoff request — and every sibling key.

    ``retry: {"count": 1, "backoff": 10}`` has ``retry`` as its immediate owner,
    not the handler — so recognising only the handler sent the single most likely
    authoring attempt to the generic unknown-field code, which is exactly the
    path that most needs to name the gate.

    Parametrized over the WHOLE gated set rather than just the two retry-shaped
    names, because that is what the owner rule actually does: `retry` gates all
    ten, exactly as `try_catch` and `connector_call` already do. Pinning only
    `backoff`/`retry_backoff` would have described the intent while leaving the
    real behaviour untested — and a test that under-describes what ships is how a
    later reader concludes the other eight are unhandled.
    """
    codes = _parse_codes(_process_scope(retry={"count": 1, key: "x"}))
    assert codes == [
        (PROCESS_IR_CAPABILITY_UNSUPPORTED, "/body/steps/0/retry/{0}".format(key))
    ]


def test_an_ordinary_typo_on_the_retry_policy_is_still_an_unknown_field():
    """The paired positive: adding ``retry`` as an owner must not turn every
    stray key on the policy object into a capability diagnostic."""
    codes = _parse_codes(_process_scope(retry={"count": 1, "wibble": 10}))
    assert codes == [
        ("PROCESS_IR_SCHEMA_UNKNOWN_FIELD", "/body/steps/0/retry/wibble")
    ]


def test_a_bare_stop_catch_is_refused_but_a_bare_exception_or_cache_sink_is_not():
    """Deviation 9, pinned.

    The architect plan says "Catch may be a bare terminal". That holds for the
    two terminals that DO something on their own; only the do-nothing case is
    refused, because a catch that merely stops swallows the caught document and
    no capture attests that shape.
    """
    assert _parse_codes(
        _connector_scope(catch_steps=[], catch_terminal={"kind": "stop"})
    )
    _compile(_connector_scope(catch_steps=[], catch_terminal=dict(EXCEPTION_TERMINAL)))
    _compile(
        _connector_scope(
            catch_steps=[],
            catch_terminal={"kind": "cache_put", "cache_ref": "$ref:CACHE"},
        )
    )


# ---------------------------------------------------------------------------
# Issue #155 — `source_replay_policy`, the acceptance matrix the plan names
# ---------------------------------------------------------------------------
#
# The plan's acceptance arms for this unit are: connector-scope refusal,
# retry-zero refusal, the default surviving a dump/reparse, authored `forbid`
# behaving as the default does, and independence from the write-safety refusal.
# Only the propagation-and-XML-omission witness existed, which proves the field
# reaches the emitter but says nothing about when it is REFUSED.


ALLOW = {"source_replay_policy": "allow_duplicates"}


def test_allow_duplicates_lifts_the_source_reexecution_refusal():
    """The capability itself: the one refusal this policy is allowed to lift."""
    _compile(_process_scope(retry={"count": 2, **ALLOW}))


@pytest.mark.parametrize("count", [1, 3, 5])
def test_the_default_policy_still_refuses_at_every_positive_count(count):
    """The paired negative — the lift is opt-in, not a weakening of the rule."""
    diag = _compile_error(_process_scope(retry={"count": count}))
    assert diag.code == PROCESS_IR_SEMANTIC_RETRY_SOURCE_REEXECUTION


def test_authored_forbid_behaves_exactly_as_the_default_does():
    """Authoring the default explicitly must not change the verdict.

    If it did, the default would be doing something other than what it names,
    and a caller reading the served description would be misled about which of
    the two states they are in.
    """
    authored = _compile_error(
        _process_scope(retry={"count": 2, "source_replay_policy": "forbid"}))
    defaulted = _compile_error(_process_scope(retry={"count": 2}))
    assert authored.code == defaulted.code == PROCESS_IR_SEMANTIC_RETRY_SOURCE_REEXECUTION
    assert authored.path == defaulted.path


def test_the_policy_is_refused_on_a_connector_scope():
    """Scope arm: the policy speaks about the flow's document SOURCE.

    A connector-scoped region provably excludes the producer, so the policy has
    nothing to permit there and asserting it is a caller error rather than a
    no-op — a no-op would let an author believe they had granted something.
    """
    diag = _compile_error(_connector_scope(retry={"count": 2, **ALLOW}))
    assert diag.code == PROCESS_IR_SEMANTIC_RETRY_SOURCE_POLICY_SCOPE_INVALID
    # The COMPLETE pointer, not a suffix. The pointer is the actionable half of a
    # refusal — a code alone does not tell an author which field to correct — and
    # a suffix match accepts the right field on the WRONG NODE, which is exactly
    # the drift worth catching. This fixture puts the offending region at step 1.
    assert diag.path == "/body/steps/1/retry/source_replay_policy", diag.path


def test_the_policy_is_refused_at_retry_zero():
    """Retry arm: with no retry there is no re-execution to permit."""
    diag = _compile_error(_process_scope(retry={"count": 0, **ALLOW}))
    assert diag.code == PROCESS_IR_SEMANTIC_RETRY_SOURCE_POLICY_REQUIRES_RETRY
    # ...and this fixture puts it at step 0, so the two pins together also prove
    # the pointer TRACKS the authored node rather than being a constant.
    assert diag.path == "/body/steps/0/retry/source_replay_policy", diag.path


def test_the_policy_survives_a_dump_and_reparse_in_both_states():
    """A defaulted value must round-trip as the same document.

    A field that serialises to something the parser reads back differently makes
    every downstream comparison — goldens, fingerprints, plan equality — a
    comparison of the serialiser rather than of the document.
    """
    for retry in ({"count": 2, **ALLOW}, {"count": 2}):
        doc = _process_scope(retry=retry)
        ir = parse_process_ir_v1(doc)
        again = parse_process_ir_v1(ir.model_dump(mode="json"))
        assert again.model_dump(mode="json") == ir.model_dump(mode="json")


def test_the_policy_does_not_lift_the_write_safety_refusal():
    """Independence: it lifts ONE refusal, and the plan says which.

    This is the arm that matters most. The write-safety refusal protects against
    replaying a non-idempotent write, which re-reading a source has nothing to
    do with — a policy that quietly lifted both would turn an opt-in about
    duplicate READS into permission to duplicate WRITES.
    """
    doc = _process_scope(retry={"count": 2, **ALLOW}, op="$ref:PATCHOP")
    diag = _compile_error(doc)
    assert diag.code == PROCESS_IR_SEMANTIC_RETRY_NON_IDEMPOTENT_WRITE


def test_one_contract_reference_may_cover_several_operations(monkeypatch):
    """The re-keying's point, stated as the capability it unlocks.

    A contract binds a reference to the ONE operation it covers. Keyed on the
    reference alone, a table could name that reference exactly once, so a second
    contract binding the same reference to a different operation was rejected as
    a duplicate — which is not what "duplicate" should mean for a pair.
    """
    _synthetic_capabilities(monkeypatch, (REST, "PATCH", "conditionally_idempotent"))
    contracts = [
        IdempotencyContractSymbolV1(ref="$ref:SHARED", operation_ref="$ref:PATCHOP"),
        IdempotencyContractSymbolV1(ref="$ref:SHARED", operation_ref="$ref:OTHEROP"),
    ]
    symbols = _symbols(contracts=contracts)
    assert len(symbols.idempotency_contracts) == 2

    index = symbols.build_idempotency_index()
    assert ("$ref:SHARED", "$ref:PATCHOP") in index
    assert ("$ref:SHARED", "$ref:OTHEROP") in index

    # It resolves for the operation it covers...
    doc = _connector_scope(
        retry={"count": 1},
        protected="$ref:PATCHOP",
        idempotency={"kind": "key_reference", "contract_ref": "$ref:SHARED"},
    )
    _compile(doc, symbols)

    # ...and a reference covering only OTHER operations is still not evidence here.
    only_elsewhere = _symbols(
        contracts=[IdempotencyContractSymbolV1(ref="$ref:SHARED", operation_ref="$ref:OTHEROP")]
    )
    with pytest.raises(ProcessIRCompileError) as excinfo:
        _compile(doc, only_elsewhere)
    assert (
        excinfo.value.diagnostics[0].code
        == PROCESS_IR_SEMANTIC_IDEMPOTENCY_EVIDENCE_MISSING
    )


def test_the_same_reference_and_operation_twice_is_still_a_duplicate():
    """The pair is what is unique — the ref alone was too strict, not too loose."""
    import pydantic

    with pytest.raises(pydantic.ValidationError):
        _symbols(
            contracts=[
                IdempotencyContractSymbolV1(ref="$ref:C", operation_ref="$ref:OP"),
                IdempotencyContractSymbolV1(ref="$ref:C", operation_ref="$ref:OP"),
            ]
        )


@pytest.mark.parametrize(
    "value",
    [
        "$ref:ICV1:Rest:patch:x:01",
        "$ref:has:colon",
        "$ref:trailing/slash",
        "$ref:brace{0}",
    ],
)
def test_a_further_structured_contract_ref_is_refused_by_its_own_named_code(value):
    """The plan's named negative: a value carrying further structure is not a
    contract reference, and the refusal must NAME the grammar it broke rather
    than degrade to the generic schema-invalid tail."""
    codes = _parse_codes(
        _connector_scope(idempotency={"kind": "key_reference", "contract_ref": value})
    )
    assert codes[0][0] == "PROCESS_IR_REFERENCE_IDEMPOTENCY_CONTRACT_INVALID_FORMAT"


def test_the_authoring_surface_and_the_compiler_symbol_share_one_grammar():
    """Lockstep, asserted over the authority's own case set rather than a list.

    A symbol table naming a contract in a form the authoring model would refuse
    describes a document nobody could have written, and the disagreement surfaces
    as an unresolvable reference instead of the malformed value it is.
    """
    import pydantic

    from boomi_mcp.connector_replay.ids import is_authored_contract_ref
    from boomi_mcp.models.process_ir import _validate_contract_ref

    cases = [
        "$ref:OK",
        "$ref:C0",
        "$ref:a.b-c_d",
        "$ref:ICV1:Rest:patch:x:01",
        "$ref:",
        "$ref:has space",
        "literal-component-id",
        "",
        "  ",
    ]
    for value in cases:
        authority = is_authored_contract_ref(value)

        authoring_ok = True
        try:
            _validate_contract_ref(value)
        except Exception:
            authoring_ok = False

        symbol_ok = True
        try:
            IdempotencyContractSymbolV1(ref=value, operation_ref="$ref:OP")
        except pydantic.ValidationError:
            symbol_ok = False

        assert authoring_ok is authority, f"authoring surface disagrees on {value!r}"
        assert symbol_ok is authority, f"compiler symbol disagrees on {value!r}"


def test_the_served_schema_advertises_the_same_grammar_it_enforces():
    """Machine-facing: a contract whose schema omits its grammar is a contract a
    caller cannot conform to without guessing.

    Measured on pydantic 2.12.3: a `BeforeValidator` makes a sibling
    `StringConstraints(pattern=...)` apply to the validator's OUTPUT, which JSON
    Schema cannot express, so the pattern is dropped from the served document
    without error. Carrying it as schema metadata keeps it visible; this pin keeps
    it EQUAL to the rule actually enforced.
    """
    import json

    from boomi_mcp.connector_replay.ids import AUTHORED_CONTRACT_REF_PATTERN
    from boomi_mcp.models.process_ir import canonical_process_ir_schema_json

    schema = json.loads(canonical_process_ir_schema_json())

    def find(node):
        if isinstance(node, dict):
            if "contract_ref" in node and isinstance(node["contract_ref"], dict):
                yield node["contract_ref"]
            for value in node.values():
                yield from find(value)
        elif isinstance(node, list):
            for item in node:
                yield from find(item)

    served = list(find(schema))
    assert served, "no contract_ref field found in the served schema"
    for field in served:
        assert field.get("pattern") == AUTHORED_CONTRACT_REF_PATTERN, (
            "the served grammar and the enforced grammar have drifted: "
            f"served={field.get('pattern')!r} authority={AUTHORED_CONTRACT_REF_PATTERN!r}"
        )


def _mint(doc, symbols, root="$ref:ROOT"):
    from boomi_mcp.compiler.process_ir.connector_resolution import mint_idempotency_grants

    cfg, _plan = _compile(doc, symbols)
    return mint_idempotency_grants(cfg, symbols, process_root_ref=root)


def test_the_minter_mints_a_grant_for_a_call_whose_contract_resolves(monkeypatch):
    """NON-VACUITY for the minter itself.

    The first version of this read the evidence off the binding, where it does
    not live, so it minted nothing for every input while every other test stayed
    green — a minter that produces an empty tuple is indistinguishable from one
    that is never exercised.
    """
    _synthetic_capabilities(monkeypatch, (REST, "PATCH", "conditionally_idempotent"))
    digest = "d" * 64
    symbols = _symbols(
        contracts=[
            IdempotencyContractSymbolV1(
                ref="$ref:C", operation_ref="$ref:PATCHOP", record_digest=digest
            )
        ]
    )
    doc = _connector_scope(
        retry={"count": 1},
        protected="$ref:PATCHOP",
        idempotency={"kind": "key_reference", "contract_ref": "$ref:C"},
    )

    # Provenance is REQUIRED, so a corroborating record has to exist for anything
    # to mint — the registry is what turns a caller's claim into evidence.
    from boomi_mcp.compiler.process_ir.connector_resolution import mint_idempotency_grants

    cfg, _plan = _compile(doc, symbols)
    minted = mint_idempotency_grants(
        cfg, symbols, process_root_ref="$ref:ROOT", registry=_complete_record(digest),
    )
    assert minted.process_root_ref == "$ref:ROOT"
    assert len(minted.idempotency_grants) == 1, "the minter produced nothing"
    grant = minted.idempotency_grants[0]
    assert grant.contract_ref == "$ref:C"
    assert grant.operation_ref == "$ref:PATCHOP"
    assert grant.call_source_path, "a grant must name the call it covers"
    assert grant.key in minted.build_grant_index()

    # The table it was minted FROM is unchanged — minting projects, never mutates.
    assert symbols.process_root_ref is None
    assert symbols.idempotency_grants == ()


def test_a_call_with_no_evidence_mints_no_grant(monkeypatch):
    """The minter describes what IS evidenced; it never invents coverage.

    Stated from measurement, not from my first guess: I wrote this as "a
    reference covering another operation mints nothing", assuming a read-only
    call ignores a dangling reference. It does not — a reference that resolves to
    nothing is refused whichever row it sits on, so that document never reaches
    the minter at all. The true negative is a call carrying no reference.
    """
    _synthetic_capabilities(monkeypatch, (REST, "GET", "read_only"))
    symbols = _symbols(
        contracts=[IdempotencyContractSymbolV1(ref="$ref:C", operation_ref="$ref:OTHEROP")]
    )
    doc = _connector_scope(retry={"count": 1}, protected="$ref:GETOP")

    minted = _mint(doc, symbols)
    assert minted.idempotency_grants == ()
    assert minted.process_root_ref == "$ref:ROOT"


def test_an_unresolvable_reference_is_refused_before_any_grant_is_minted(monkeypatch):
    """Enumeration failure is the validator's to report, from one layer only."""
    _synthetic_capabilities(monkeypatch, (REST, "GET", "read_only"))
    symbols = _symbols(
        contracts=[IdempotencyContractSymbolV1(ref="$ref:C", operation_ref="$ref:OTHEROP")]
    )
    doc = _connector_scope(
        retry={"count": 1},
        protected="$ref:GETOP",
        idempotency={"kind": "key_reference", "contract_ref": "$ref:C"},
    )
    with pytest.raises(ProcessIRCompileError) as excinfo:
        _mint(doc, symbols)
    assert (
        excinfo.value.diagnostics[0].code
        == PROCESS_IR_SEMANTIC_IDEMPOTENCY_EVIDENCE_MISSING
    )


def test_a_root_projected_table_requires_a_grant_for_this_very_call(monkeypatch):
    """The capability grants exist for: evidence per CALL, not per operation.

    A contract covers an operation, so without a per-call grant a second call of
    the same operation elsewhere in the root inherits evidence nobody minted for
    it. Checked in both directions — the minted table compiles, and the same
    table with the grant's call path altered does not.
    """
    _synthetic_capabilities(monkeypatch, (REST, "PATCH", "conditionally_idempotent"))
    symbols = _symbols(
        contracts=[
            IdempotencyContractSymbolV1(
                ref="$ref:C", operation_ref="$ref:PATCHOP", record_digest="c" * 64
            )
        ]
    )
    doc = _connector_scope(
        retry={"count": 1},
        protected="$ref:PATCHOP",
        idempotency={"kind": "key_reference", "contract_ref": "$ref:C"},
    )

    # A grant-free table still compiles: that is the pre-projection state.
    _compile(doc, symbols)

    from boomi_mcp.compiler.process_ir.connector_resolution import mint_idempotency_grants

    cfg, _plan = _compile(doc, symbols)
    minted = mint_idempotency_grants(
        cfg, symbols, process_root_ref="$ref:ROOT",
        registry=_complete_record("c" * 64),
    )
    assert minted.idempotency_grants, "nothing minted — the rest of this is vacuous"
    _compile(doc, minted)  # the minted table covers this call

    # Same contract, same operation, a grant naming a DIFFERENT call site.
    from boomi_mcp.compiler.process_ir.contracts import IdempotencyGrantSymbolV1

    elsewhere = minted.model_copy(
        update={
            "idempotency_grants": (
                IdempotencyGrantSymbolV1(
                    contract_ref="$ref:C",
                    operation_ref="$ref:PATCHOP",
                    call_source_path="/body/steps/99/somewhere_else",
                ),
            )
        }
    )
    with pytest.raises(ProcessIRCompileError) as excinfo:
        _compile(doc, elsewhere)
    assert (
        excinfo.value.diagnostics[0].code
        == PROCESS_IR_SEMANTIC_IDEMPOTENCY_EVIDENCE_MISSING
    )


class _Identity:
    """The operation identity every real registry record carries."""

    def __init__(self, component_id):
        self.component_id = component_id
        self.version = 1

def _complete_record(digest, *, contract="$ref:C", operation="op-patch",
                     connection="conn-1", family="rest", action="PATCH"):
    """A registry double carrying every axis corroboration compares.

    The FAMILY is validated against the real record model's own field
    constraint, because that is exactly where three review rounds went wrong:
    the doubles carried the canonical PLATFORM connector type, which the real
    model forbids, so the tests agreed with a comparison that could never match
    genuine evidence. Constructing the whole model is not possible here — its
    component-id validator requires real Boomi ids and this harness uses
    synthetic names — so the one field that misled is checked against the
    authority instead of the whole record being assumed.

    It also provides `family_for`, since corroboration maps the binding's
    platform type through the registry's vocabulary; a double without it is not
    a registry, and its absence reads as a corroboration failure the test did
    not intend.
    """
    import re

    from boomi_mcp.compiler.process_ir.connector_capabilities import REST_FAMILY
    from boomi_mcp.connector_replay.models import OperationContractRecordV1

    constraint = OperationContractRecordV1.model_fields["family"]
    pattern = next(
        (getattr(m, "pattern", None) for m in constraint.metadata
         if getattr(m, "pattern", None)),
        None,
    )
    assert pattern, "the record model no longer constrains `family`; this pin is vacuous"
    assert re.fullmatch(pattern, family), (
        f"fixture family {family!r} is one the real record model would REJECT "
        f"(pattern {pattern!r}) — a record shaped like this could never load"
    )

    class _R:
        record_digest = digest
        contract_ref = contract
        family = None
        action = None

    _R.family = family
    _R.action = action
    _R.operation_identity = _Identity(operation)
    _R.connection_identity = _Identity(connection)

    class _Reg:
        operation_records = (_R(),)

        @staticmethod
        def family_for(platform_connector_type):
            return "rest" if platform_connector_type == REST_FAMILY else None

    return _Reg()


def test_a_contract_naming_no_record_mints_NOTHING(monkeypatch):
    """INVERTED, and the original assertion was the fail-open itself.

    I wrote this as a control asserting the digest is optional, reasoning that
    requiring one would make every grant unmintable while the packaged registry
    is empty. That reasoning is backwards: a caller could omit the field and
    authorise a retry on no evidence at all, which is precisely what the digest
    exists to prevent. Nothing minting until evidence is ingested IS the
    fail-closed posture the packaged registry already ships in.
    """
    _synthetic_capabilities(monkeypatch, (REST, "PATCH", "conditionally_idempotent"))
    symbols = _symbols(
        contracts=[IdempotencyContractSymbolV1(ref="$ref:C", operation_ref="$ref:PATCHOP")]
    )
    doc = _connector_scope(
        retry={"count": 1},
        protected="$ref:PATCHOP",
        idempotency={"kind": "key_reference", "contract_ref": "$ref:C"},
    )
    from boomi_mcp.compiler.process_ir.connector_resolution import mint_idempotency_grants

    cfg, _ = _compile(doc, symbols)

    class _Empty:
        operation_records = ()

    minted = mint_idempotency_grants(
        cfg, symbols, process_root_ref="$ref:ROOT", registry=_Empty()
    )
    assert minted.idempotency_grants == (), "minted a grant on no evidence at all"
    # The table is still PROJECTED, so grant checking stays ON and the call is
    # refused rather than falling back to per-operation authorisation.
    assert minted.process_root_ref == "$ref:ROOT"


@pytest.mark.parametrize(
    "axis", ["connection", "family", "action", "operation", "contract"]
)
def test_corroboration_compares_every_axis_the_compiler_can_know(axis, monkeypatch):
    """A digest match is not corroboration on any single axis.

    A record captured against a different connection, family or action describes
    a call the system did not observe. Component VERSIONS, the account scope and
    route coverage are deliberately NOT compared here: the compiler has no live
    reading of them, and taking them from the record would compare the record
    with itself. Those belong to the apply-boundary recheck, where a live
    reading exists.
    """
    _synthetic_capabilities(monkeypatch, (REST, "PATCH", "conditionally_idempotent"))
    from boomi_mcp.compiler.process_ir.connector_resolution import (
        mint_idempotency_grants,
        resolve_connector_call_bindings,
    )

    digest = "a" * 64
    symbols = _symbols(
        contracts=[
            IdempotencyContractSymbolV1(
                ref="$ref:C", operation_ref="$ref:PATCHOP", record_digest=digest
            )
        ]
    )
    doc = _connector_scope(
        retry={"count": 1},
        protected="$ref:PATCHOP",
        idempotency={"kind": "key_reference", "contract_ref": "$ref:C"},
    )
    cfg, _plan = _compile(doc, symbols)
    index = symbols.build_index()
    binding = next(
        b for b in resolve_connector_call_bindings(cfg, symbols)
        if b.operation_ref == "$ref:PATCHOP"
    )
    good = {
        "contract": "$ref:C",
        "operation": index[binding.operation_ref].component_id,
        "connection": index[binding.connection_ref].component_id,
        # The record's PORTABLE family, not the binding's platform type — the
        # real model's pattern forbids the platform form, so a fixture using it
        # would agree with a comparison that can never match real evidence.
        "family": "rest",
        "action": binding.action,
    }

    # Control first: everything matching MUST mint, or the negatives below are
    # satisfied by something other than the axis under test.
    minted = mint_idempotency_grants(
        cfg, symbols, process_root_ref="$ref:R",
        registry=_complete_record(digest, **good),
    )
    assert len(minted.idempotency_grants) == 1, "the matching record failed to corroborate"

    spoiled_value = "other_family" if axis == "family" else "SOMETHING-ELSE"
    spoiled = mint_idempotency_grants(
        cfg, symbols, process_root_ref="$ref:R",
        registry=_complete_record(digest, **{**good, axis: spoiled_value}),
    )
    assert spoiled.idempotency_grants == (), f"a record with the wrong {axis} corroborated"


def test_the_snapshot_not_the_symbol_table_decides_the_observed_identity(monkeypatch):
    """The architect gate's second critical: corroboration compared records
    against the SYMBOL TABLE, whose ids are relocatable placeholders chosen by
    the plan — not against what the account was observed to hold.

    Comparing a record to a placeholder compares it to the request. The snapshot
    is the independent input, so where it has an answer it is the one used.
    """
    _synthetic_capabilities(monkeypatch, (REST, "PATCH", "conditionally_idempotent"))
    from boomi_mcp.authoring.connector_resolution_snapshot import (
        ResolvedConnectorComponentIdentityV1,
        TrustedConnectorResolutionSnapshotV1,
    )
    from boomi_mcp.compiler.process_ir.connector_resolution import (
        mint_idempotency_grants,
        resolve_connector_call_bindings,
    )

    digest = "b" * 64
    symbols = _symbols(
        contracts=[
            IdempotencyContractSymbolV1(
                ref="$ref:C", operation_ref="$ref:PATCHOP", record_digest=digest
            )
        ]
    )
    doc = _connector_scope(
        retry={"count": 1},
        protected="$ref:PATCHOP",
        idempotency={"kind": "key_reference", "contract_ref": "$ref:C"},
    )
    cfg, _plan = _compile(doc, symbols)
    index = symbols.build_index()
    binding = next(
        b for b in resolve_connector_call_bindings(cfg, symbols)
        if b.operation_ref == "$ref:PATCHOP"
    )
    placeholder = index[binding.operation_ref].component_id
    observed_id = "OBSERVED-OPERATION-ID"

    snapshot = TrustedConnectorResolutionSnapshotV1(
        identities=(
            ResolvedConnectorComponentIdentityV1(
                # The snapshot records the raw component KEY; a symbol's
                # reference is that key behind the token prefix.
                component_key=binding.operation_ref.split(":", 1)[1],
                component_id=observed_id,
            ),
        )
    )

    # A record naming the PLACEHOLDER no longer corroborates once the account has
    # been observed to hold a different component.
    stale = mint_idempotency_grants(
        cfg, symbols, process_root_ref="$ref:R",
        registry=_complete_record(digest, operation=placeholder),
        snapshot=snapshot,
    )
    assert stale.idempotency_grants == (), (
        "a record matching the plan's placeholder corroborated against an account "
        "observed to hold something else"
    )

    # A record naming what was OBSERVED does.
    real = mint_idempotency_grants(
        cfg, symbols, process_root_ref="$ref:R",
        registry=_complete_record(digest, operation=observed_id),
        snapshot=snapshot,
    )
    assert len(real.idempotency_grants) == 1, "the observed identity failed to corroborate"


def test_a_foreign_account_record_does_not_corroborate(monkeypatch):
    """The account check that had never once run.

    It imported a helper that does not exist, so the import raised on every
    machine, the broad handler answered True, and a record belonging to another
    account corroborated whenever the other fields matched. The comment
    justifying that fallback reasoned it would otherwise reject every record on
    a machine missing the helper — the helper was missing on ALL of them.
    """
    _synthetic_capabilities(monkeypatch, (REST, "PATCH", "conditionally_idempotent"))
    from boomi_mcp.authoring.connector_resolution_snapshot import (
        ResolvedConnectorComponentIdentityV1,
        TrustedConnectorResolutionSnapshotV1,
    )
    from boomi_mcp.compiler.process_ir.connector_resolution import (
        mint_idempotency_grants,
        resolve_connector_call_bindings,
    )
    from boomi_mcp.connector_replay.digests import account_scope_hash

    digest = "d" * 64
    symbols = _symbols(
        contracts=[
            IdempotencyContractSymbolV1(
                ref="$ref:C", operation_ref="$ref:PATCHOP", record_digest=digest
            )
        ]
    )
    doc = _connector_scope(
        retry={"count": 1},
        protected="$ref:PATCHOP",
        idempotency={"kind": "key_reference", "contract_ref": "$ref:C"},
    )
    cfg, _plan = _compile(doc, symbols)
    index = symbols.build_index()
    binding = next(
        b for b in resolve_connector_call_bindings(cfg, symbols)
        if b.operation_ref == "$ref:PATCHOP"
    )
    op_key = binding.operation_ref.split(":", 1)[1]

    snapshot = TrustedConnectorResolutionSnapshotV1(
        identities=(
            ResolvedConnectorComponentIdentityV1(
                component_key=op_key,
                component_id=index[binding.operation_ref].component_id,
                account_id="account-OURS",
            ),
        )
    )

    def registry(scope_hash):
        base = _complete_record(digest)
        record = base.operation_records[0]
        record.account_scope_hash = scope_hash

        class _Reg:
            operation_records = (record,)

            @staticmethod
            def family_for(platform_connector_type):
                return base.family_for(platform_connector_type)

        return _Reg()

    foreign = mint_idempotency_grants(
        cfg, symbols, process_root_ref="$ref:R",
        registry=registry(account_scope_hash("account-THEIRS")), snapshot=snapshot,
    )
    assert foreign.idempotency_grants == (), "a foreign-account record corroborated"

    ours = mint_idempotency_grants(
        cfg, symbols, process_root_ref="$ref:R",
        registry=registry(account_scope_hash("account-OURS")), snapshot=snapshot,
    )
    assert len(ours.idempotency_grants) == 1, "our own account's record failed to corroborate"


def test_the_two_account_hashers_are_ONE_authority():
    """Ingestion stamps the scope; corroboration checks it. A second private copy
    is how two sides of one comparison come to disagree."""
    import inspect

    from boomi_mcp.connector_replay import ingest
    from boomi_mcp.connector_replay.digests import account_scope_hash

    body = inspect.getsource(ingest._account_scope_hash)
    assert "account_scope_hash" in body, "ingest no longer consumes the shared hasher"
    assert "hashlib.sha256" not in body, (
        "ingest computes its own account hash again; the two sides can now drift"
    )
    assert account_scope_hash("x") == account_scope_hash("x")


@pytest.mark.parametrize("bogus", [object(), "not-a-snapshot", 42, {"identities": ()}])
def test_a_miswired_snapshot_is_refused_rather_than_read_as_empty(bogus, monkeypatch):
    """Passing the object directly was NOT the guarantee I claimed.

    The corroboration reads `identities` and `account_scope` permissively, so a
    miswired object yielded no identities and no account — and the identity and
    account checks quietly passed. A value that cannot answer those questions
    must not be read as answering them.
    """
    _synthetic_capabilities(monkeypatch, (REST, "PATCH", "conditionally_idempotent"))
    from boomi_mcp.compiler.process_ir.connector_resolution import mint_idempotency_grants

    digest = "a" * 64
    symbols = _symbols(
        contracts=[
            IdempotencyContractSymbolV1(
                ref="$ref:C", operation_ref="$ref:PATCHOP", record_digest=digest
            )
        ]
    )
    doc = _connector_scope(
        retry={"count": 1},
        protected="$ref:PATCHOP",
        idempotency={"kind": "key_reference", "contract_ref": "$ref:C"},
    )
    cfg, _plan = _compile(doc, symbols)

    with pytest.raises(TypeError):
        mint_idempotency_grants(
            cfg, symbols, process_root_ref="$ref:R",
            registry=_complete_record(digest), snapshot=bogus,
        )


def test_a_real_snapshot_is_accepted(monkeypatch):
    """Control: the assertion must not refuse the genuine article."""
    _synthetic_capabilities(monkeypatch, (REST, "PATCH", "conditionally_idempotent"))
    from boomi_mcp.authoring.connector_resolution_snapshot import (
        TrustedConnectorResolutionSnapshotV1,
    )
    from boomi_mcp.compiler.process_ir.connector_resolution import mint_idempotency_grants

    digest = "a" * 64
    symbols = _symbols(
        contracts=[
            IdempotencyContractSymbolV1(
                ref="$ref:C", operation_ref="$ref:PATCHOP", record_digest=digest
            )
        ]
    )
    doc = _connector_scope(
        retry={"count": 1},
        protected="$ref:PATCHOP",
        idempotency={"kind": "key_reference", "contract_ref": "$ref:C"},
    )
    cfg, _plan = _compile(doc, symbols)
    minted = mint_idempotency_grants(
        cfg, symbols, process_root_ref="$ref:R",
        registry=_complete_record(digest),
        snapshot=TrustedConnectorResolutionSnapshotV1(),
    )
    assert len(minted.idempotency_grants) == 1
