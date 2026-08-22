"""One executable witness per `PROCESS_IR_V1_CAPABILITIES` key — manifest-keyed.

#177 invariant 2. The manifest is a SERVED promise: `supported` tells a caller the
construct will be admitted, `gated` tells them it will be refused. Nothing checked either
direction, which is DC-175-D read at the capability level — served text describing a
capability the enforcement no longer grants.

WHY MANIFEST-KEYED AND NOT MATRIX-ENUMERATED
--------------------------------------------
The obvious shape is to enumerate today's placement matrix and assert over it. #154
rewrites that matrix, so such a pin would be rewritten along with the thing it exists to
police. This registry is keyed by the MANIFEST instead: the guard asserts
`set(CAPABILITY_WITNESSES) == set(PROCESS_IR_V1_CAPABILITIES)`, so a new capability key
with no witness FAILS, a retired key leaves a stale witness and FAILS, and #154 extends the
gate by adding rows rather than by rewriting it.

WHY THE STATE IS NOT STORED HERE
--------------------------------
A witness records what it DOES (admits / refuses / is dispositioned), never what the
manifest SAYS. Copying the state into this file would be a second record of one fact — the
exact mechanism this issue exists to close — and a capability flipped in the manifest would
still pass against its stale copy. The guard reads the live manifest and requires the
witness KIND to match it, so a flip fails until the witness is deliberately rewritten.

FIXTURE PROVENANCE
------------------
All five committed ProcessIR documents used below were frozen BEFORE this slice's step-0
baseline `6f26caff7481356119fee5b36a1730cec0fb5df2` (latest is `3c07ad2`, 2026-08-20), so
they are causally independent of the code under test.

An earlier version of this note said inline documents are only REFUSAL inputs. That was not
true of the file it sat in — five ADMISSION witnesses were inline, and two of the frozen
fixtures listed here were never loaded at all. The architect review caught it. Both are
fixed: `scoped_try_catch` and `bounded_retry` now load their frozen anchors, so every
admission witness that has a frozen fixture available uses it.

What remains inline, and why each is acceptable:

* REFUSAL inputs. Their job is to be rejected, so they carry no served field names the
  implementation could have taught me — a wrong guess produces the wrong diagnostic and the
  witness fails.
* Three ADMISSION inputs — `generalized_connector_call`, `mixed_connector_execution` and
  `typed_idempotency_evidence` — for which no frozen fixture exists. These are declared
  `PROVENANCE_INLINE_ADMISSION` rather than described as something stronger, and the
  enforcement gate checks that declaration against a closed set instead of accepting any
  non-blank string.
"""

from __future__ import annotations

import copy
import hashlib
import json
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Callable, Tuple

_ROOT = Path(__file__).resolve().parent.parent
for _p in (str(_ROOT), str(_ROOT / "src")):
    if _p not in sys.path:
        sys.path.insert(0, _p)

from _process_ir_entrypoint_differential import measure_entrypoints  # noqa: E402
from _wave_gate_golden_corpus import error_symbols  # noqa: E402

_FIXTURES = _ROOT / "tests" / "fixtures" / "process_ir"

#: Frozen documents, with the commit that froze them. All predate the step-0 baseline.
FIXTURE_PROVENANCE = {
    "rich_control/branch_process_call.json": "3c07ad2 (2026-08-20)",
    "rich_control/branch_mixed_connectors.json": "db7d177 (2026-07-25)",
    "rich_control/decision_nested_bare_false_stop.json": "395398f (2026-07-25)",
    "error_handling/scoped_try_catch_process_retry0_exception.json": "cde125f (2026-07-25)",
    "error_handling/scoped_try_catch_connector_read_retry5_cache_catch.json": (
        "cde125f (2026-07-25)"
    ),
}

EXCEPTION_TERMINAL = {"kind": "exception", "message_template": "caught {1}"}

#: The closed set of provenance kinds a witness may declare. The enforcement gate compares
#: against this rather than merely requiring a non-blank string, so "a fixture you wrote is
#: not evidence" is a check instead of a sentence.
PROVENANCE_FROZEN_FIXTURE = "frozen fixture"
PROVENANCE_INLINE_ADMISSION = "inline admission document (no frozen fixture exists)"
PROVENANCE_INLINE_REFUSAL = "inline refusal document"
PROVENANCE_SYNTHETIC_CFG = "synthetic CFG (not authorable)"
PROVENANCE_KINDS = frozenset(
    {
        PROVENANCE_FROZEN_FIXTURE,
        PROVENANCE_INLINE_ADMISSION,
        PROVENANCE_INLINE_REFUSAL,
        PROVENANCE_SYNTHETIC_CFG,
    }
)


@dataclass(frozen=True)
class CapabilityWitness:
    """An executable case proving one capability is admitted or refused.

    `kind` is `"admits"` or `"refuses"` — what this witness OBSERVES, never a copy of the
    manifest state. `run` returns whatever `observe` needs; `observe` carries the
    feature-specific assertion, so a witness cannot pass by being merely a valid document.
    """

    key: str
    kind: str
    provenance: str
    run: Callable[[], object]
    observe: Callable[[object], None]


@dataclass(frozen=True)
class UnsupportedDisposition:
    """An `unsupported` manifest row, with the reason no execution contract is claimed.

    `unsupported` means "outside this authoring contract", not "there is one stable error
    for it". Asserting a specific refusal would invent a promise the contract does not
    make. These rows are recorded, never silently skipped: the guard requires the
    dispositioned set to equal the manifest's `unsupported` partition exactly.
    """

    key: str
    reason: str


#: Which fixtures `_fixture()` has read, and which MODELS actually reached the compiler.
#:
#: This binding moved four times, each time because the weaker form admitted a witness that
#: satisfied the letter of its claim: file opens, then parses, then compiles recorded in the
#: two obvious helpers, then set equality over those. The last version was still fail-open
#: because other helpers (`_measure`, `_compile_refusal_for_model`) call
#: `compile_process_ir_v1` DIRECTLY and were not instrumented — and instrumenting helpers
#: one at a time is the same whack-a-mole this slice already paid four rounds for with an
#: AST resolver. So the recording happens at the ONE compiler boundary every helper goes
#: through, and no helper can be forgotten because none of them is the thing instrumented.
_LOADED_FIXTURES = {}
_COMPILED_MODELS = []


def _digest(document):
    return hashlib.sha256(
        json.dumps(document, sort_keys=True, separators=(",", ":")).encode("utf-8")
    ).hexdigest()


def _model_digest(ir):
    """Digest of the MODEL the compiler received, so every path is comparable.

    A fixture's raw JSON and the model built from it differ (the parser expands defaults),
    so the claimed side is put through the same parse before digesting.
    """
    return _digest(ir.model_dump(mode="json", warnings=False))


def expected_model_digest(relative):
    return _model_digest(_parse(_fixture(relative)))


class record_compiles:
    """Record every model reaching the shared compile CORE.

    Patching `pipeline.compile_process_ir_v1` was not enough: the package re-exports that
    name as its own alias (`boomi_mcp.compiler.process_ir.compile_process_ir_v1`), which
    keeps pointing at the original, and `parse_and_compile_process_ir_v1` is a second public
    entry point entirely. Patching each binding is the same open-ended chase that the AST
    resolver and the per-helper recording both lost.

    `_compile_parsed_process_ir_v1` is the one function BOTH public entry points call
    (`pipeline.py:434`, `:461`, `:571`), so instrumenting it covers every binding, present
    or future, including aliases this module has never heard of.
    """

    def __enter__(self):
        from boomi_mcp.compiler.process_ir import pipeline

        self._pipeline = pipeline
        self._real = pipeline._compile_parsed_process_ir_v1

        def _recording(ir, *args, **kwargs):
            _COMPILED_MODELS.append(_model_digest(ir))
            return self._real(ir, *args, **kwargs)

        pipeline._compile_parsed_process_ir_v1 = _recording
        return self

    def __exit__(self, *exc):
        self._pipeline._compile_parsed_process_ir_v1 = self._real
        return False


def reset_loaded_fixtures():
    _LOADED_FIXTURES.clear()
    del _COMPILED_MODELS[:]


def loaded_fixtures():
    return dict(_LOADED_FIXTURES)


def compiled_digests():
    return tuple(_COMPILED_MODELS)


def _fixture(relative):
    assert relative in FIXTURE_PROVENANCE, relative
    document = json.loads((_FIXTURES / relative).read_text(encoding="utf-8"))
    _LOADED_FIXTURES[relative] = _digest(document)
    return document


def _doc(steps):
    return {"version": "1", "body": {"kind": "sequence", "steps": steps}}


def _process_scope(retry=None, op="$ref:GETOP", **over):
    node = {
        "kind": "try_catch",
        "scope": "process",
        "try_body": {
            "steps": [{"kind": "connector_call", "operation_ref": op}],
            "terminal": {"kind": "stop"},
        },
        "catch_body": {
            "steps": [{"kind": "message", "text": "failed"}],
            "terminal": dict(EXCEPTION_TERMINAL),
        },
    }
    if retry is not None:
        node["retry"] = retry
    node.update(over)
    return _doc([node])


def _connector_scope(
    retry=None, protected="$ref:GETOP2", upstream="$ref:GETOP", idempotency=None, **over
):
    call = {"kind": "connector_call", "operation_ref": protected}
    if idempotency is not None:
        call["idempotency"] = idempotency
    node = {
        "kind": "try_catch",
        "scope": "connector",
        "try_body": {"steps": [call], "terminal": {"kind": "stop"}},
        "catch_body": {
            "steps": [{"kind": "message", "text": "failed"}],
            "terminal": dict(EXCEPTION_TERMINAL),
        },
    }
    if retry is not None:
        node["retry"] = retry
    node.update(over)
    return _doc([{"kind": "connector_call", "operation_ref": upstream}, node])


def _parse(doc):
    from boomi_mcp.models.process_ir import parse_process_ir_v1

    return parse_process_ir_v1(copy.deepcopy(doc))


def _measure(doc, symbols, mode="full", capabilities=None):
    """Both entry points on one document — reachability from the exported-model path too.

    #178's driver is IMPORTED, never copied: the issue's own instruction. Cross-entry
    diagnostic IDENTITY stays #178's invariant (it gates a derived product over the whole
    placement matrix); what a capability witness needs from it is that the enforcement is
    reached from BOTH public entry points, not just the one the witness happened to call.
    """
    return measure_entrypoints(
        _parse(doc), mode=mode, symbols=symbols, capabilities=capabilities
    )


def _parser_refusal(doc):
    """Refusal observed at the PARSE entry point."""
    from boomi_mcp.models.process_ir import ProcessIRValidationError

    try:
        _parse(doc)
    except ProcessIRValidationError as exc:
        return tuple((d.code, d.path) for d in exc.diagnostics)
    return ()


def _compile_refusal(doc, symbols, capabilities=None):
    """Refusal observed at the COMPILE entry point, past the grammar."""
    from boomi_mcp.compiler.process_ir.diagnostics import ProcessIRCompileError
    from boomi_mcp.compiler.process_ir.pipeline import compile_process_ir_v1

    try:
        compile_process_ir_v1(_parse(doc), symbols, capabilities=capabilities)
    except ProcessIRCompileError as exc:
        return tuple((d.code, d.path) for d in exc.diagnostics)
    return ()


def _compiles(doc, symbols, capabilities=None):
    from boomi_mcp.compiler.process_ir.pipeline import compile_process_ir_v1

    return compile_process_ir_v1(_parse(doc), symbols, capabilities=capabilities)


def _rich_compiles(relative):
    from _wave_gate_golden_corpus import rich_compile_doc

    # `rich_compile_doc` parses internally rather than through `_parse`, so the digest is
    # recorded here — otherwise the content binding could not see the document that was
    # actually compiled and would report a true claim as false.
    return rich_compile_doc(_fixture(relative))


def _emitter_kinds(plan):
    """The emitter kinds the plan will render.

    #177 architect review: seven of eight admission witnesses inspected only the CFG or the
    authored model, both derived straight from the input, so a compiler that produced a
    correct CFG and no emission plan left them green. A capability the manifest advertises
    is one the compiler EMITS, so the plan is asserted too.
    """
    return [node.emitter_input.emitter_kind for node in plan.nodes]


def _semantic_kinds(cfg):
    return [type(node.semantic).__name__ for node in cfg.nodes]


# ---------------------------------------------------------------------------
# Admission witnesses (`supported` rows)
#
# Each observes a FEATURE-SPECIFIC fact, never merely "it compiled": a witness that
# only asserted success would pass on any valid document and would keep passing if
# the capability it names were quietly dropped.
# ---------------------------------------------------------------------------


def _w_generalized_connector_call():
    doc = _doc(
        [{"kind": "connector_call", "operation_ref": "$ref:GETOP"}, {"kind": "stop"}]
    )

    def run():
        cfg, plan = _compiles(doc, error_symbols())
        return cfg, plan, _measure(doc, error_symbols())

    def observe(result):
        cfg, plan, (parser, compiler) = result
        assert parser[0] == "ACCEPTED", parser
        assert compiler[0] == "ACCEPTED", compiler
        assert "ConnectorCallSemanticV1" in _semantic_kinds(cfg), _semantic_kinds(cfg)
        assert "connectoraction_source" in _emitter_kinds(plan), _emitter_kinds(plan)

    return CapabilityWitness(
        "generalized_connector_call", "admits", PROVENANCE_INLINE_ADMISSION, run, observe
    )


def _w_mixed_connector_execution():
    # RECORDED SUBSTITUTION: the design plan named a REST GET -> map -> SOAP EXECUTE case.
    # `error_symbols()` registers REST and DATABASE families and no SOAP action, and adding
    # one would mean inventing a symbol table rather than reusing the corpus's. REST -> DB
    # exercises the property the manifest row actually states — two connector calls of
    # DIFFERENT families on one root-to-leaf path — and the observer below pins both
    # families AND both emitter forms. The map step is dropped with it: it carries no
    # connector family and so contributes nothing to this row.
    doc = _doc(
        [
            {"kind": "connector_call", "operation_ref": "$ref:GETOP"},
            {"kind": "connector_call", "operation_ref": "$ref:DBSEND"},
            {"kind": "stop"},
        ]
    )

    def run():
        return _compiles(doc, error_symbols())

    def observe(result):
        cfg, plan = result
        # DISTINCT emitter FORMS, not just two connector actions. Counting alone passed a
        # mutant that emitted the DB target as a second `connectoraction_source`, which is
        # not the construct this manifest row names.
        emitted = sorted(k for k in _emitter_kinds(plan) if k.startswith("connectoraction_"))
        assert emitted == ["connectoraction_source", "connectoraction_target"], emitted
        # TWO connector calls of DIFFERENT families on ONE root-to-leaf path — the
        # construct the manifest row names, not just "a connector call compiled".
        kinds = _semantic_kinds(cfg)
        assert kinds.count("ConnectorCallSemanticV1") == 2, kinds
        # The FAMILY is a property of the resolved symbol, not of the semantic node, so
        # it is read back through the symbol table the compile used.
        symbols = {symbol.ref: symbol for symbol in error_symbols().symbols}
        families = {
            symbols[node.semantic.operation_ref].connector_type
            for node in cfg.nodes
            if type(node.semantic).__name__ == "ConnectorCallSemanticV1"
        }
        assert len(families) == 2, families

    return CapabilityWitness(
        "mixed_connector_execution", "admits", PROVENANCE_INLINE_ADMISSION, run, observe
    )


def _w_connector_call_in_control_body():
    relative = "rich_control/branch_mixed_connectors.json"

    def run():
        (cfg, plan), _table = _rich_compiles(relative)
        return cfg, plan

    def observe(result):
        cfg, plan = result
        kinds = _emitter_kinds(plan)
        assert "branch" in kinds and kinds.count("connectoraction_target") >= 2, kinds
        inside = [
            node
            for node in cfg.nodes
            if type(node.semantic).__name__ == "ConnectorCallSemanticV1"
            and "/legs/" in node.source_path
        ]
        assert inside, [node.source_path for node in cfg.nodes]

    return CapabilityWitness(
        "connector_call_in_control_body",
        "admits",
        PROVENANCE_FROZEN_FIXTURE + " " + relative + " (" + FIXTURE_PROVENANCE[relative] + ")",
        run,
        observe,
    )


def _w_terminal_process_call():
    relative = "rich_control/branch_process_call.json"

    def run():
        (cfg, plan), _table = _rich_compiles(relative)
        return cfg, plan

    def observe(result):
        cfg, plan = result
        assert _emitter_kinds(plan).count("processcall") == 2, _emitter_kinds(plan)
        calls = [
            node
            for node in cfg.nodes
            if type(node.semantic).__name__ == "ProcessCallSemanticV1"
        ]
        assert calls, _semantic_kinds(cfg)
        # TERMINAL: every call is an exit of the graph, with no outgoing edge. That is
        # the whole content of the capability — a call that continued would be the
        # gated `process_call_return_path_binding` instead.
        outgoing = {edge.source_node_id for edge in cfg.edges}
        for node in calls:
            assert node.node_id not in outgoing, node.node_id
            assert node.node_id in cfg.exit_node_ids, (node.node_id, cfg.exit_node_ids)

    return CapabilityWitness(
        "terminal_process_call",
        "admits",
        PROVENANCE_FROZEN_FIXTURE + " " + relative + " (" + FIXTURE_PROVENANCE[relative] + ")",
        run,
        observe,
    )


def _w_rich_branch_decision_bodies():
    relative = "rich_control/decision_nested_bare_false_stop.json"

    def run():
        (cfg, plan), _table = _rich_compiles(relative)
        return cfg, plan

    def observe(result):
        cfg, plan = result
        kinds = _semantic_kinds(cfg)
        # NESTED decision — one Decision inside another's arm — plus the bare false
        # Stop. A single Decision would not witness the "rich bodies" row.
        assert kinds.count("DecisionSemanticV1") >= 2, kinds
        assert "StopSemanticV1" in kinds, kinds
        emitted = _emitter_kinds(plan)
        assert emitted.count("decision") >= 2, emitted
        assert "stop" in emitted, emitted

    return CapabilityWitness(
        "rich_branch_decision_bodies",
        "admits",
        PROVENANCE_FROZEN_FIXTURE + " " + relative + " (" + FIXTURE_PROVENANCE[relative] + ")",
        run,
        observe,
    )


def _w_scoped_try_catch():
    relative = "error_handling/scoped_try_catch_process_retry0_exception.json"

    def run():
        # Read INSIDE the run: the provenance gate binds a frozen claim to what the run
        # actually loaded, and an import-time read is invisible to it.
        return _compiles(_fixture(relative), error_symbols())

    def observe(result):
        cfg, plan = result
        scopes = [
            node.semantic.scope
            for node in cfg.nodes
            if type(node.semantic).__name__ == "TryCatchSemanticV1"
        ]
        assert scopes == ["process"], scopes
        assert "catcherrors" in _emitter_kinds(plan), _emitter_kinds(plan)

    return CapabilityWitness(
        "scoped_try_catch",
        "admits",
        PROVENANCE_FROZEN_FIXTURE + " " + relative + " (" + FIXTURE_PROVENANCE[relative] + ")",
        run,
        observe,
    )


def _w_bounded_retry():
    relative = "error_handling/scoped_try_catch_connector_read_retry5_cache_catch.json"

    def run():
        return _compiles(_fixture(relative), error_symbols())

    def observe(result):
        cfg, plan = result
        counts = [
            node.semantic.retry_count
            for node in cfg.nodes
            if type(node.semantic).__name__ == "TryCatchSemanticV1"
        ]
        # 5 is the top of the platform's own 0-5 bound: the boundary value, so a
        # narrowed bound fails here rather than passing on a mid-range count.
        assert counts == [5], counts

        # ...and the LOWERED value, which is the one that reaches the emitter. The CFG
        # semantic record is derived straight from the authored input, so inspecting it
        # alone would still pass if lowering hardcoded zero or dropped the value — the
        # capability the manifest advertises is the retry the compiler EMITS, not the
        # retry the caller typed.
        lowered = [
            node.emitter_input.retry_count
            for node in plan.nodes
            if type(getattr(node, "emitter_input", None)).__name__ == "CatchErrorsInputV1"
        ]
        assert lowered == [5], lowered

    return CapabilityWitness(
        "bounded_retry",
        "admits",
        PROVENANCE_FROZEN_FIXTURE + " " + relative + " (" + FIXTURE_PROVENANCE[relative] + ")",
        run,
        observe,
    )


def _w_typed_idempotency_evidence():
    doc = _connector_scope(
        protected="$ref:PATCHOP",
        retry={"count": 2},
        idempotency={"kind": "verified_action"},
    )

    def run():
        # DISCLOSED LIMITATION: no PRODUCTION connector row is classified replay-safe
        # (`retry_safety` is `unverified` for every stock write action — there is no
        # authoritative source classifying one, which is why
        # `verified_write_replay_safety` is itself gated). So the only way to reach the
        # admitted branch is to synthesise the row. The witness records that rather
        # than hiding it: this capability is admitted by the CONTRACT, and its
        # activation waits on the classification #155 owns.
        from boomi_mcp.compiler.process_ir import connector_capabilities as CC

        original = CC.CONNECTOR_CALL_CAPABILITIES_V1
        table = dict(original)
        key = (CC.REST_FAMILY, "patch")
        assert key in table, sorted(table)[:5]
        assert table[key].retry_safety == "unverified", table[key].retry_safety
        table[key] = table[key].model_copy(update={"retry_safety": "idempotent_write"})
        CC.CONNECTOR_CALL_CAPABILITIES_V1 = table
        try:
            cfg, plan = _compiles(doc, error_symbols())
        finally:
            CC.CONNECTOR_CALL_CAPABILITIES_V1 = original
        return cfg, plan

    def observe(result):
        cfg, plan = result
        kinds = _emitter_kinds(plan)
        assert "catcherrors" in kinds and "connectoraction_target" in kinds, kinds
        # EXACT authored evidence, not merely "something non-null". Proving only that the
        # field is populated passed a mutant that injected an invented `contract_ref` during
        # lowering — the capability is that the AUTHORED evidence is preserved.
        evidence = [
            node.semantic.idempotency.model_dump(mode="json", warnings=False)
            for node in cfg.nodes
            if type(node.semantic).__name__ == "ConnectorCallSemanticV1"
            and getattr(node.semantic, "idempotency", None) is not None
        ]
        # The model expands the union's optional fields, so the authored evidence appears as
        # `{"kind": "verified_action", "contract_ref": None}` — asserted whole rather than
        # trimmed, so an INVENTED `contract_ref` fails here instead of being normalised away.
        assert evidence == [{"kind": "verified_action", "contract_ref": None}], evidence

    return CapabilityWitness(
        "typed_idempotency_evidence",
        "admits",
        PROVENANCE_INLINE_ADMISSION,
        run,
        observe,
    )


# ---------------------------------------------------------------------------
# Refusal witnesses (`gated` rows)
#
# Each asserts the OWNING entry point refuses with the exact code and pointer, and
# that BOTH public entry points refuse — so a capability cannot be gated on one path
# and reachable on the other. Cross-entry diagnostic IDENTITY stays #178's invariant.
# ---------------------------------------------------------------------------


def _parser_gated(key, doc, code, path, provenance=PROVENANCE_INLINE_REFUSAL, mutate=None):
    """A capability the GRAMMAR refuses, asserted at the owning entry point.

    `mutate` closes the compile-side half where the construct is REACHABLE on a validated
    model. `ProcessIRV1` is exported and mutable and assignment is not re-validated, so a
    caller can parse a legal document, set a gated field, and hand the model straight to
    `compile_process_ir_v1` — the exact premise #178 is built on. Where that is possible,
    `mutate` builds the valid model, sets the field, and this asserts the compiler refuses
    it too.

    Where it is NOT possible, `mutate` is None and that is a MEASURED fact, not an excuse.
    Exactly one gated capability is in that position:

    * `catch_all` is an EXTRA key with no field on `TryCatchNodeV1`, so assignment raises
      before any model can carry it — measured verbatim:
      `ValueError: "TryCatchNodeV1" object has no field "catch_all"`.

    An earlier version of this docstring made the same claim for `definedparameter`, on the
    grounds that the `PropertySourceV1` union has no member class for it. That reasoning was
    WRONG and the Stage-2 reviewer caught it: `source_values` is an ordinary mutable list, so
    a caller can parse a legal `set_dpp` and assign the raw dict into `source_values[0]`
    without any union member existing. Measured: the compiler then refuses with
    `PROCESS_IR_CAPABILITY_UNSUPPORTED` at `/body/steps/1/source_values/0`. That path now has
    its own `mutate`, because a real enforcement nothing pins is a real enforcement that can
    regress.

    #178's derived product does not cover any of these: it varies node KINDS and PLACEMENTS
    from `BODY_CAPABILITIES_V1`, never field VALUES.
    """

    def run():
        refusal = _parser_refusal(doc)
        compiled = None
        if mutate is not None:
            model, clean, expected = mutate()
            # The unmutated model must COMPILE, so the refusal below is attributable to
            # the gated field and not to some unrelated defect in the carrier document.
            assert clean == (), ("carrier does not compile clean", clean)
            compiled = (_compile_refusal_for_model(model), expected)
        return refusal, compiled

    def observe(result):
        refusal, compiled = result
        assert refusal == ((code, path),), refusal
        if mutate is not None:
            observed, expected = compiled
            assert observed == expected, (observed, expected)

    return CapabilityWitness(key, "refuses", provenance, run, observe)


def _compile_refusal_for_model(model, symbols=None):
    """Refusal observed at the COMPILE entry point on an already-built model."""
    from boomi_mcp.compiler.process_ir.diagnostics import ProcessIRCompileError
    from boomi_mcp.compiler.process_ir.pipeline import compile_process_ir_v1

    try:
        compile_process_ir_v1(model, symbols or error_symbols())
    except ProcessIRCompileError as exc:
        return tuple((d.code, d.path) for d in exc.diagnostics)
    return ()


def _w_process_call_connector_mixing():
    return _parser_gated(
        "process_call_connector_mixing",
        _doc(
            [
                {"kind": "process_call", "process_ref": "a"},
                {"kind": "connector_call", "operation_ref": "$ref:GETOP"},
                {"kind": "stop"},
            ]
        ),
        "PROCESS_IR_CAPABILITY_UNSUPPORTED",
        "/body",
    )


def _w_process_call_return_path_binding():
    """Both halves of the historical `L2-r6-01` defect, verbatim.

    The old selection was written as "the first step that is not a process_call", which
    has TWO failure modes and the witness pins both:

    * an ALL-CALL root has no such element, so `next(...)` raised `StopIteration` straight
      out of the validator and escaped as an untyped crash;
    * `[call, call, stop]` selected the trailing STOP (`/body/steps/2`) rather than the
      second CALL (`/body/steps/1`) — but removing the stop would not fix the document, so
      the pointer sent the caller to the wrong node.

    Measured under the restored defect: `StopIteration` and `/body/steps/2` respectively.
    """
    code = "PROCESS_IR_CAPABILITY_PROCESS_CALL_RETURN_PATH_BINDING_UNSUPPORTED"
    all_calls = _doc(
        [
            {"kind": "process_call", "process_ref": "a"},
            {"kind": "process_call", "process_ref": "b"},
        ]
    )
    with_stop = _doc(
        [
            {"kind": "process_call", "process_ref": "a"},
            {"kind": "process_call", "process_ref": "b"},
            {"kind": "stop"},
        ]
    )

    def run():
        return _parser_refusal(all_calls), _parser_refusal(with_stop)

    def observe(result):
        crash_case, pointer_case = result
        assert crash_case == ((code, "/body/steps/1"),), crash_case
        assert pointer_case == ((code, "/body/steps/1"),), pointer_case

    return CapabilityWitness(
        "process_call_return_path_binding",
        "refuses",
        PROVENANCE_INLINE_REFUSAL,
        run,
        observe,
    )


def _w_continuation_after_branch_or_decision():
    return _parser_gated(
        "continuation_after_branch_or_decision",
        _doc(
            [
                {
                    "kind": "branch",
                    "legs": [
                        {
                            "steps": [{"kind": "message", "text": "a"}],
                            "terminal": {"kind": "stop"},
                        },
                        {
                            "steps": [{"kind": "message", "text": "b"}],
                            "terminal": {"kind": "stop"},
                        },
                    ],
                },
                {"kind": "message", "text": "after"},
            ]
        ),
        "PROCESS_IR_SEMANTIC_CONTROL_CONTINUATION_UNSUPPORTED",
        "/body",
    )


def _w_catch_failure_trigger_selection():
    return _parser_gated(
        "catch_failure_trigger_selection",
        _process_scope(catch_all=False),
        "PROCESS_IR_CAPABILITY_UNSUPPORTED",
        "/body/steps/0/catch_all",
    )


def _w_listener_error_scope():
    def mutate():
        # A VALID process-scope document, parsed, then the scope field set to the gated
        # literal — the model-to-compiler path a caller can actually take.
        carrier = _parse(_process_scope())
        clean = _compile_refusal_for_model(carrier)
        model = _parse(_process_scope())
        model.body.steps[0].scope = "listener"
        return model, clean, (
            ("PROCESS_IR_CAPABILITY_ERROR_SCOPE_UNSUPPORTED", "/body/steps/0/scope"),
        )

    return _parser_gated(
        "listener_error_scope",
        _process_scope(scope="listener"),
        "PROCESS_IR_CAPABILITY_ERROR_SCOPE_UNSUPPORTED",
        "/body/steps/0/scope",
        mutate=mutate,
    )


def _w_nested_try_catch():
    inner = {
        "kind": "try_catch",
        "scope": "process",
        "try_body": {
            "steps": [{"kind": "connector_call", "operation_ref": "$ref:GETOP"}],
            "terminal": {"kind": "stop"},
        },
        "catch_body": {
            "steps": [{"kind": "message", "text": "x"}],
            "terminal": dict(EXCEPTION_TERMINAL),
        },
    }
    return _parser_gated(
        "nested_try_catch",
        _doc(
            [
                {
                    "kind": "try_catch",
                    "scope": "process",
                    "try_body": {"steps": [inner], "terminal": {"kind": "stop"}},
                    "catch_body": {
                        "steps": [{"kind": "message", "text": "y"}],
                        "terminal": dict(EXCEPTION_TERMINAL),
                    },
                }
            ]
        ),
        "PROCESS_IR_CAPABILITY_NODE_NOT_ALLOWED_IN_BODY",
        "/body/steps/0/try_body/steps/0",
    )


def _w_keyed_cache():
    def _carrier():
        # A cleanly COMPILING document: the `cache_put` upstream satisfies the cache-writer
        # rule, so the only thing the mutation changes is the gated field.
        return _doc(
            [
                {
                    "kind": "source",
                    "connection_ref": "$ref:CONN",
                    "operation_ref": "$ref:GETOP",
                },
                {"kind": "cache_put", "cache_ref": "$ref:CACHE"},
                {"kind": "document_cache_retrieve", "cache_ref": "$ref:CACHE"},
                {
                    "kind": "target",
                    "connection_ref": "$ref:DBCONN",
                    "operation_ref": "$ref:DBSEND",
                },
                {"kind": "stop"},
            ]
        )

    def mutate():
        clean = _compile_refusal_for_model(_parse(_carrier()))
        model = _parse(_carrier())
        model.body.steps[2].load_all_documents = False
        return model, clean, (
            ("PROCESS_IR_CAPABILITY_UNSUPPORTED", "/body/steps/2/load_all_documents"),
        )

    return _parser_gated(
        "keyed_cache",
        _doc(
            [
                {
                    "kind": "document_cache_retrieve",
                    "cache_ref": "$ref:CACHE",
                    "load_all_documents": False,
                },
                {"kind": "stop"},
            ]
        ),
        "PROCESS_IR_CAPABILITY_UNSUPPORTED",
        "/body/steps/0/load_all_documents",
        mutate=mutate,
    )


def _w_definedparameter_property_source():
    def _carrier():
        return _doc(
            [
                {
                    "kind": "source",
                    "connection_ref": "$ref:CONN",
                    "operation_ref": "$ref:GETOP",
                },
                {
                    "kind": "set_dpp",
                    "name": "X",
                    "source_values": [{"value_type": "static", "value": "v"}],
                },
                {
                    "kind": "target",
                    "connection_ref": "$ref:DBCONN",
                    "operation_ref": "$ref:DBSEND",
                },
                {"kind": "stop"},
            ]
        )

    def mutate():
        clean = _compile_refusal_for_model(_parse(_carrier()))
        model = _parse(_carrier())
        # `source_values` is an ordinary list: no union member class is needed to get the
        # gated shape onto a validated model.
        model.body.steps[1].source_values[0] = {
            "value_type": "definedparameter",
            "component_id": "c",
            "property_key": "k",
        }
        return model, clean, (
            ("PROCESS_IR_CAPABILITY_UNSUPPORTED", "/body/steps/1/source_values/0"),
        )

    return _parser_gated(
        "definedparameter_property_source",
        _doc(
            [
                {
                    "kind": "set_dpp",
                    "name": "x",
                    "source_values": [
                        {
                            "value_type": "definedparameter",
                            "component_id": "c",
                            "property_key": "k",
                        }
                    ],
                },
                {"kind": "stop"},
            ]
        ),
        "PROCESS_IR_CAPABILITY_UNSUPPORTED",
        "/body/steps/0/source_values/0",
        mutate=mutate,
    )


def _w_verified_write_replay_safety():
    doc = _connector_scope(
        protected="$ref:PATCHOP",
        retry={"count": 2},
        idempotency={"kind": "verified_action"},
    )

    def run():
        # The GRAMMAR admits authored verified evidence; the gate is at compile time,
        # where no production row classifies a stock write replay-safe. So this witness
        # measures the COMPILE boundary, not the parser.
        return _compile_refusal(doc, error_symbols())

    def observe(refusal):
        assert refusal == (
            (
                "PROCESS_IR_SEMANTIC_RETRY_NON_IDEMPOTENT_WRITE",
                "/body/steps/1/try_body/steps/0/operation_ref",
            ),
        ), refusal

    return CapabilityWitness(
        "verified_write_replay_safety",
        "refuses",
        PROVENANCE_INLINE_REFUSAL,
        run,
        observe,
    )


def _cfg_gated(key, build, code):
    """Refusal observed at the CFG-invariant boundary.

    `joins` and `loops` are refused there rather than at either public entry point,
    because `caller_authored_cfg_edges` is UNSUPPORTED — a caller cannot author the
    edge that would create a join or a cycle. Fabricating authoring syntax merely to
    reuse the dual-entry driver would witness a construct the contract does not have,
    so this is deliberately NOT described as public authored reachability.
    """

    def run():
        from boomi_mcp.compiler.process_ir.diagnostics import ProcessIRCompileError
        from boomi_mcp.compiler.process_ir.invariants import check_cfg_invariants

        try:
            check_cfg_invariants(build())
        except ProcessIRCompileError as exc:
            return tuple((d.code, d.path) for d in exc.diagnostics)
        return ()

    def observe(refusal):
        assert refusal and refusal[0][0] == code, refusal

    return CapabilityWitness(key, "refuses", PROVENANCE_SYNTHETIC_CFG, run, observe)


def _cfg_node(ordinal, semantic=None, path=None, exit_role=None):
    from boomi_mcp.compiler.process_ir.contracts import CfgNodeV1, MessageSemanticV1

    return CfgNodeV1(
        node_id="n{0}".format(ordinal),
        ordinal=ordinal,
        source_path=path or "/body/steps/{0}".format(ordinal - 1),
        semantic=semantic or MessageSemanticV1(text="m"),
        exit_role=exit_role,
    )


def _cfg_edge(ordinal, source, target, kind="ordering", local=1, **extra):
    from boomi_mcp.compiler.process_ir.contracts import CfgEdgeV1

    return CfgEdgeV1(
        edge_id="e{0}".format(ordinal),
        ordinal=ordinal,
        source_node_id="n{0}".format(source),
        target_node_id="n{0}".format(target),
        kind=kind,
        local_ordinal=local,
        provenance_path="/body/steps/{0}".format(target - 1),
        **extra
    )


def _join_cfg():
    from boomi_mcp.compiler.process_ir.contracts import (
        BranchSemanticV1,
        SemanticCfgV1,
        StopSemanticV1,
    )

    # Two Branch legs CONVERGING on one Stop — a node with two predecessors.
    return SemanticCfgV1(
        entry_node_id="n1",
        nodes=(
            _cfg_node(1, BranchSemanticV1(leg_count=2)),
            _cfg_node(2, path="/body/steps/0/legs/0/terminal"),
            _cfg_node(
                3,
                StopSemanticV1(),
                path="/body/steps/0/legs/1/terminal",
                exit_role="stop",
            ),
        ),
        edges=(
            _cfg_edge(1, 1, 2, kind="branch_leg", local=1, leg_ordinal=1),
            _cfg_edge(2, 1, 3, kind="branch_leg", local=2, leg_ordinal=2),
            _cfg_edge(3, 2, 3, kind="terminal"),
        ),
        exit_node_ids=("n3",),
    )


def _cycle_cfg():
    from boomi_mcp.compiler.process_ir.contracts import SemanticCfgV1, StopSemanticV1

    return SemanticCfgV1(
        entry_node_id="n1",
        nodes=(_cfg_node(1), _cfg_node(2, StopSemanticV1(), exit_role="stop")),
        edges=(_cfg_edge(1, 1, 2, kind="terminal"), _cfg_edge(2, 2, 1)),
        exit_node_ids=("n2",),
    )


# ---------------------------------------------------------------------------
# The registry. Keys are compared with `==` against the live manifest.
# ---------------------------------------------------------------------------

_ENTRIES: Tuple[object, ...] = (
    _w_generalized_connector_call(),
    _w_mixed_connector_execution(),
    _w_connector_call_in_control_body(),
    _w_terminal_process_call(),
    _w_rich_branch_decision_bodies(),
    _w_scoped_try_catch(),
    _w_bounded_retry(),
    _w_typed_idempotency_evidence(),
    _w_process_call_connector_mixing(),
    _w_process_call_return_path_binding(),
    _w_continuation_after_branch_or_decision(),
    _w_catch_failure_trigger_selection(),
    _w_listener_error_scope(),
    _w_nested_try_catch(),
    _w_keyed_cache(),
    _w_definedparameter_property_source(),
    _w_verified_write_replay_safety(),
    _cfg_gated("joins", _join_cfg, "PROCESS_IR_SEMANTIC_JOIN_UNSUPPORTED"),
    # MEASURED, not assumed: a two-node cycle is refused as AMBIGUOUS_FLOW — flow past
    # a terminal is detected before any loop-specific rule, so that is the refusal a
    # caller actually receives.
    _cfg_gated("loops", _cycle_cfg, "PROCESS_IR_SEMANTIC_AMBIGUOUS_FLOW"),
    UnsupportedDisposition(
        "catch_error_type_lists",
        "no wire representation exists: the emitted error-handling shape carries only "
        "an all-errors flag and a retry count (#142 capture §G2)",
    ),
    UnsupportedDisposition(
        "retry_backoff_authoring",
        "the retry wait schedule is platform-owned and has no authorable field "
        "(#142 capture §G1)",
    ),
    UnsupportedDisposition(
        "queue_topology",
        "no queue component is modelled and creating topology is out of scope "
        "(#142 capture §G5)",
    ),
    UnsupportedDisposition(
        "caller_authored_cfg_edges",
        "edges are compiler-derived; there is no authored input shape to exercise "
        "(ADR-001 §12)",
    ),
    UnsupportedDisposition(
        "xml_or_layout_or_shape_ids",
        "identifiers and layout are compiler-owned and never authored (ADR-001 §12)",
    ),
    UnsupportedDisposition(
        "secret_values",
        "secret material is excluded from the contract; the existing secret scans are "
        "additional defence, not this capability's witness (ADR-001 §12)",
    ),
    UnsupportedDisposition(
        "parallel_branch_execution",
        "Branch legs are ordered and sequential by construction; concurrency is "
        "different semantics, not a setting (#146)",
    ),
    UnsupportedDisposition(
        "flow_control_parallel_chunks",
        "the platform has the setting; this contract authors no field for it (#146)",
    ),
)

CAPABILITY_WITNESSES = {entry.key: entry for entry in _ENTRIES}

assert len(CAPABILITY_WITNESSES) == len(_ENTRIES), "duplicate capability key"
