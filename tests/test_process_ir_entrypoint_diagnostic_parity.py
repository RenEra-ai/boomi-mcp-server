"""#178: the two ProcessIR V1 entry points serve ONE diagnostic identity.

`ProcessIRV1` is exported and MUTABLE, so a caller may parse a legal document,
mutate the model, and hand it straight to `compile_process_ir_v1` — reaching the
compile stages with a document the parser would have refused. Before #178 the two
paths agreed on the DECISION for most such documents but not on which diagnostic
they served, and — measured at the `cdd7a3b` baseline — not always on the decision
either: a Branch leg with a trailing `cache_put`, a root `source` out of position,
a one-leg Branch, and any mutated `version` were all refused by the parser and
ACCEPTED by the compiler, which models none of those rules.

#175 answered that class structurally FOUR times (one body verdict, one root
verdict, admissibility guards, precedence yields) and each round revealed the next
ring, because the two paths were never written to share a rule ORDERING — only
outcomes. So the fix is not another verdict function: public compile re-parses
through the parser, which becomes the single authority for grammar.

WHY THIS FILE IS DERIVED, NOT HAND-LISTED
-----------------------------------------
#175's parity witnesses were hand-enumerated samples, and every round found a
sample they lacked — which is the recorded reason it deviated to #178. So the case
set here is a PRODUCT generated from the two runtime authorities:

* `BODY_CAPABILITIES_V1` — the `(context, slot) -> admitted kinds` matrix the
  compiler actually consults;
* `process_ir_v1_node_kinds()` — the parser's own closed node vocabulary.

Nothing below hard-codes a case count. The expected total is recomputed from those
authorities inside the test, so adding a node kind or a matrix row changes the
expectation automatically and a generator that silently stopped producing cases
fails instead of passing.

THE VACUITY PROBLEM THIS FILE IS BUILT AGAINST
----------------------------------------------
This repo has four recorded instances of a guard that passed because it enumerated
nothing (#149, #151, #162, #175). Four defences, all mechanical:

1. every untouched carrier is parsed and validated before it is mutated, so a
   broken carrier fails loudly instead of emptying a whole family;
2. both partitions (parser-accepted and parser-refused) are asserted non-empty;
3. the generated count must equal the product formula recomputed at runtime, and
   every matrix-DENIED cell must own at least one parser-refused case;
4. `test_the_gate_fails_when_the_structural_fix_is_removed` deletes the fix and
   asserts this same collector goes red.

Measured when written, at the fixed tree: 820 cases, 126 parser-accepted /
694 parser-refused, ZERO mismatches. With `_reparse_process_ir_for_compile`
neutered: 422 mismatches of 820. The witness is not theoretical.
"""

import copy
import sys
from pathlib import Path
from typing import get_args

import pytest
from pydantic import TypeAdapter

_ROOT = Path(__file__).resolve().parent.parent
for _p in (str(_ROOT), str(_ROOT / "src")):
    if _p not in sys.path:
        sys.path.insert(0, _p)

from boomi_mcp.compiler.process_ir import body_capabilities as bc  # noqa: E402
from boomi_mcp.compiler.process_ir import pipeline as pl  # noqa: E402
from boomi_mcp.compiler.process_ir.diagnostics import (  # noqa: E402
    ProcessIRCompileError,
)
from boomi_mcp.models.process_ir import (  # noqa: E402
    ProcessIRValidationError,
    ProcessNodeV1,
    canonical_process_ir_json,
    parse_process_ir_v1,
    process_ir_v1_node_kinds,
    process_ir_v1_parse_diagnostic_specs,
)

from _process_ir_entrypoint_differential import (  # noqa: E402
    GrammarBoundary,
    diagnostic_vector,
    measure_entrypoints,
)

NODE = TypeAdapter(ProcessNodeV1)

#: The parser's own vocabulary, minus the body wrapper. `sequence` is the BODY,
#: never a step, so it is not a placement candidate.
KINDS = tuple(sorted(set(process_ir_v1_node_kinds()) - {"sequence"}))
#: The compiler's authority. Read, never restated.
MATRIX = bc.BODY_CAPABILITIES_V1
CONTEXTS = tuple(sorted({context for context, _slot in MATRIX}))

_REF = "c_ref"
_STOP = {"kind": "stop"}
_CONN = {"kind": "connector_call", "operation_ref": _REF}


def _atom(kind):
    """A minimal, internally valid node of `kind`.

    The FIELD SET is derived from each model class's own required fields (see
    `test_every_atom_is_minimal_against_its_model`); only the VALUES are authored
    here, and they are inert placeholders. This is not a hand-model of the
    grammar — a kind whose requirements change fails that test rather than
    silently producing an invalid atom that makes its whole family vacuous.
    """
    if kind == "branch":
        return {
            "kind": "branch",
            "legs": [
                {"steps": [{"kind": "message", "text": "a"}], "terminal": dict(_STOP)},
                {"steps": [{"kind": "message", "text": "b"}], "terminal": dict(_STOP)},
            ],
        }
    if kind == "decision":
        return {
            "kind": "decision",
            "comparison": "equals",
            "left": {"value_type": "static", "static_value": "1"},
            "right": {"value_type": "static", "static_value": "1"},
            "true_arm": {
                "steps": [{"kind": "message", "text": "t"}],
                "terminal": dict(_STOP),
            },
            "false_arm": {
                "steps": [{"kind": "message", "text": "f"}],
                "terminal": dict(_STOP),
            },
        }
    if kind == "try_catch":
        return {
            "kind": "try_catch",
            "scope": "process",
            "try_body": {"steps": [dict(_CONN)], "terminal": dict(_STOP)},
            "catch_body": {
                "steps": [{"kind": "message", "text": "c"}],
                "terminal": dict(_STOP),
            },
        }
    if kind == "data_process":
        return {
            "kind": "data_process",
            "steps": [
                {
                    "operation": "custom_scripting",
                    "language": "groovy2",
                    "script": "x",
                }
            ],
        }
    simple = {
        "cache_get": {"cache_ref": _REF},
        "cache_put": {"cache_ref": _REF},
        "cache_remove": {"cache_ref": _REF},
        "document_cache_retrieve": {"cache_ref": _REF},
        "connector_call": {"operation_ref": _REF},
        # The `{1}` placeholder is REQUIRED by the model's own validator.
        "exception": {"message_template": "boom {1}"},
        "flow_control": {"for_each_count": 1},
        "map_ref": {"map_ref": _REF},
        "message": {"text": "m"},
        "process_call": {"process_ref": _REF},
        "return_documents": {},
        "stop": {},
        "set_ddp": {
            "name": "n",
            "source_values": [{"value_type": "static", "value": "v"}],
        },
        "set_dpp": {
            "name": "n",
            "source_values": [{"value_type": "static", "value": "v"}],
        },
        "source": {"connection_ref": _REF, "operation_ref": _REF},
        "target": {"connection_ref": _REF, "operation_ref": _REF},
    }
    return {"kind": kind, **simple[kind]}


#: A legal root used only as something to MUTATE. Root cases cannot be built as
#: raw payloads: an illegal root payload never parses into a model at all, so
#: there would be nothing to hand the compiler and the entire root half of the
#: product — which is where corpus rows 2 and 3 live — would silently skip itself.
_ROOT_CARRIER = {
    "version": "1",
    "body": {"kind": "sequence", "steps": [dict(_CONN), dict(_STOP)]},
}


def _carrier(context, mode):
    """An untouched, LEGAL document holding the target body."""
    # EXACT routing. A catch-all `else` sent any unrecognised context down the
    # try/catch branch, so a matrix row added later would silently be exercised
    # against `catch_body` and its family would look covered while testing the
    # wrong slot. Unknown contexts must fail loudly instead.
    assert context in CONTEXTS, "unrecognised body context: {0}".format(context)
    if context == bc.BRANCH_LEG:
        control = _atom("branch")
    elif context in (bc.DECISION_TRUE_ARM, bc.DECISION_FALSE_ARM):
        control = _atom("decision")
    elif context in (bc.TRY_BODY, bc.CATCH_BODY):
        control = copy.deepcopy(_atom("try_catch"))
        if mode == "connector_above":
            # The ancestor mode is NOT free for try/catch: a connector scope must
            # protect the call that produces the documents, so the scope changes
            # with the mode rather than a connector merely being prepended.
            control["scope"] = "connector"
            control["try_body"] = {"steps": [dict(_CONN)], "terminal": dict(_STOP)}
    else:  # pragma: no cover - the assertion above forbids reaching this
        raise AssertionError(context)
    steps = [dict(_CONN), control] if mode == "connector_above" else [control]
    return {"version": "1", "body": {"kind": "sequence", "steps": steps}}


def _body_of(ir, context, mode):
    """The target body. Exact, for the same reason `_carrier` is."""
    node = ir.body.steps[1] if mode == "connector_above" else ir.body.steps[0]
    if context == bc.BRANCH_LEG:
        return node.legs[0]
    if context == bc.DECISION_TRUE_ARM:
        return node.true_arm
    if context == bc.DECISION_FALSE_ARM:
        return node.false_arm
    if context == bc.TRY_BODY:
        return node.try_body
    if context == bc.CATCH_BODY:
        return node.catch_body
    raise AssertionError("unrecognised body context: {0}".format(context))


MODES = ("clean", "connector_above")


def _anchored(context, mode, prefix_nodes, candidate_at_end=None):
    """Steps for a body, preserving the Try-body's mandatory connector anchor.

    A process-scoped try body must BEGIN with the connector_call that produces
    the flow's documents; a connector-scoped one must END with the call it
    protects. Overwriting `steps` wholesale — which an earlier revision did —
    violates the anchor in every try-body case, so each one collapsed onto the
    same scope diagnostic and the slot-admission rule underneath was never
    reached. The candidate is placed AROUND the anchor instead.
    """
    nodes = list(prefix_nodes)
    if candidate_at_end is not None:
        nodes.append(candidate_at_end)
    if context not in (bc.TRY_BODY,):
        return nodes
    anchor = NODE.validate_python(copy.deepcopy(_atom("connector_call")))
    return nodes + [anchor] if mode == "connector_above" else [anchor] + nodes


def _build(context, mode, slot, kind, neighbor):
    """One generated document: `kind` in the target slot, `neighbor` opposite it.

    The NEIGHBOUR is the dimension the first revision of this generator dropped,
    and it is not decorative: corpus row 1 is precisely a (prefix `cache_put`,
    terminal `process_call`) INTERACTION, which no single-slot product can
    produce. The plan crosses every STEP candidate with every legal terminal, and
    every TERMINAL candidate with the empty prefix and every legal step.
    """
    ir = parse_process_ir_v1(copy.deepcopy(_carrier(context, mode)))
    body = _body_of(ir, context, mode)
    candidate = NODE.validate_python(copy.deepcopy(_atom(kind)))
    if slot == bc.STEP_SLOT:
        body.steps = _anchored(context, mode, [candidate])
        body.terminal = NODE.validate_python(copy.deepcopy(_atom(neighbor)))
    else:
        prefix = (
            []
            if neighbor is None
            else [NODE.validate_python(copy.deepcopy(_atom(neighbor)))]
        )
        body.steps = _anchored(context, mode, prefix)
        body.terminal = candidate
    return ir


def _observed_signature(ir, context, mode, slot):
    """Read the (candidate, neighbour) pair back out of a constructed document.

    INDEPENDENT of `_build` on purpose. An earlier revision navigated with
    `_body_of` — the same helper `_build` mutates through — which made the context
    dimension self-validating: had `DECISION_FALSE_ARM` been routed to `true_arm`,
    every false-arm case AND this check would have inspected the true arm together
    and agreed. So the walk is done here from the root, and the control node's own
    kind is asserted against the context that claims it.

    It is also COMPLETE where the earlier revision was partial: it discarded the
    Try anchor without checking what it was and ignored anything past `steps[0]`,
    so a malformed anchor or an extra step passed silently. Both are asserted.
    """
    root = list(ir.body.steps)
    expected_root = 2 if mode == "connector_above" else 1
    assert len(root) == expected_root, (context, mode, len(root))
    if mode == "connector_above":
        assert root[0].kind == "connector_call", root[0].kind
    node = root[1] if mode == "connector_above" else root[0]

    if context == bc.BRANCH_LEG:
        assert node.kind == "branch", node.kind
        body = node.legs[0]
    elif context == bc.DECISION_TRUE_ARM:
        assert node.kind == "decision", node.kind
        body = node.true_arm
        assert body is not node.false_arm
    elif context == bc.DECISION_FALSE_ARM:
        assert node.kind == "decision", node.kind
        body = node.false_arm
        assert body is not node.true_arm
    elif context == bc.TRY_BODY:
        assert node.kind == "try_catch", node.kind
        body = node.try_body
    elif context == bc.CATCH_BODY:
        assert node.kind == "try_catch", node.kind
        body = node.catch_body
    else:  # pragma: no cover
        raise AssertionError(context)

    steps = list(body.steps)
    if context == bc.TRY_BODY:
        # The anchor's KIND and side are asserted, not assumed away.
        assert steps, "the try anchor vanished"
        if mode == "connector_above":
            assert steps[-1].kind == "connector_call", steps[-1].kind
            steps = steps[:-1]
        else:
            assert steps[0].kind == "connector_call", steps[0].kind
            steps = steps[1:]

    # Exactly one candidate-or-prefix, never more: an extra step would otherwise
    # ride along invisibly behind `steps[0]`.
    assert len(steps) <= 1, [s.kind for s in steps]
    terminal_kind = getattr(body.terminal, "kind", None)
    step_kind = steps[0].kind if steps else None
    if slot == bc.STEP_SLOT:
        assert len(steps) == 1, "the candidate step vanished"
        return step_kind, terminal_kind
    return terminal_kind, step_kind


def _neighbours(context, slot):
    """The opposite slot's legal vocabulary, read from the matrix.

    For a TERMINAL candidate the empty prefix is included as `None`: "no steps at
    all" is a distinct authored shape, and it is the one corpus row 5 turns on.
    """
    if slot == bc.STEP_SLOT:
        return sorted(MATRIX[(context, bc.TERMINAL_SLOT)])
    return [None] + sorted(MATRIX[(context, bc.STEP_SLOT)])


def _generate():
    """The derived product. Returns (cases, carrier_failures)."""
    cases = []
    carrier_failures = []
    for context, slot in sorted(MATRIX):
        for mode in MODES:
            base = _carrier(context, mode)
            try:
                parse_process_ir_v1(copy.deepcopy(base))
            except ProcessIRValidationError as exc:  # pragma: no cover - guard
                carrier_failures.append((context, slot, mode, str(exc)[:160]))
                continue
            for kind in KINDS:
                for neighbor in _neighbours(context, slot):
                    cases.append(
                        (
                            "{0}/{1}={2}/opp={3}/mode={4}".format(
                                context, slot, kind, neighbor or "EMPTY", mode
                            ),
                            _build(context, mode, slot, kind, neighbor),
                        )
                    )
    # ROOT product — the matrix has no root row, and root precedence is exactly
    # where two rules collide (corpus rows 2 and 3).
    parse_process_ir_v1(copy.deepcopy(_ROOT_CARRIER))
    for first in KINDS:
        ir = parse_process_ir_v1(copy.deepcopy(_ROOT_CARRIER))
        ir.body.steps = [NODE.validate_python(copy.deepcopy(_atom(first)))]
        cases.append(("root/[{0}]".format(first), ir))
        for second in KINDS:
            other = parse_process_ir_v1(copy.deepcopy(_ROOT_CARRIER))
            other.body.steps = [
                NODE.validate_python(copy.deepcopy(_atom(first))),
                NODE.validate_python(copy.deepcopy(_atom(second))),
            ]
            cases.append(("root/[{0},{1}]".format(first, second), other))
    return cases, carrier_failures


# #177 extracted this driver to `tests/_process_ir_entrypoint_differential.py` so
# the capability-enforcement gate can measure the SAME two entry points over a
# different case set. The issue's own instruction was to share it by IMPORT rather
# than by merge — copying it would have created a second record of one fact inside
# the machinery built to detect exactly that. The aliases below keep this module's
# call sites and its historical mutant test unchanged.
_GrammarBoundary = GrammarBoundary
_vector = diagnostic_vector
_measure = measure_entrypoints


_CACHE = {}


def _measured():
    """Measure every case ONCE; the parity and safety tests read this."""
    if not _CACHE:
        cases, failures = _generate()
        _CACHE["failures"] = failures
        _CACHE["cases"] = cases
        _CACHE["rows"] = [(cid, ir) + _measure(ir) for cid, ir in cases]
    return _CACHE


# ---------------------------------------------------------------------------
# Structure — the generator itself must be sound before its verdict means
# anything.
# ---------------------------------------------------------------------------


def test_every_atom_is_minimal_against_its_model():
    """The palette is keyed by the runtime vocabulary and validates against the
    runtime union. A new node kind fails HERE, closed, instead of quietly having
    no cases generated for it."""
    assert set(KINDS), "the node vocabulary is empty — everything below is vacuous"
    for kind in KINDS:
        NODE.validate_python(copy.deepcopy(_atom(kind)))
    union_kinds = set()
    for cls in get_args(get_args(ProcessNodeV1)[0]):
        field = cls.model_fields.get("kind")
        if field is not None:
            union_kinds.add(get_args(field.annotation)[0])
    assert union_kinds == set(KINDS), union_kinds.symmetric_difference(set(KINDS))


def test_the_matrix_is_a_total_grid_and_every_carrier_is_legal():
    """A context missing a slot, or a carrier that is already invalid, would empty
    a whole family and leave the gate green for the wrong reason."""
    for context in CONTEXTS:
        for slot in (bc.STEP_SLOT, bc.TERMINAL_SLOT):
            assert (context, slot) in MATRIX, (context, slot)
    assert _measured()["failures"] == [], _measured()["failures"]


def test_the_generated_count_equals_the_runtime_product():
    """Derived, never hard-coded. A generator that stopped early — or a matrix row
    that vanished — changes this equality instead of shrinking coverage
    silently."""
    # The PLAN's product, recomputed from the authorities — a candidate in each
    # slot crossed with the OPPOSITE slot's legal vocabulary. An earlier revision
    # asserted `len(MATRIX) * modes * kinds`, which recomputed its own reduced
    # formula and so agreed with itself while generating 400 body cases instead
    # of 3000. A count derived from the wrong formula is not a derived count.
    # The exact expected ID SET, built from `MATRIX` directly — not via
    # `_neighbours`, the helper the generator itself uses, and not as a count.
    #
    # Two rounds of this gate found two different ways a weaker check passes.
    # Sharing `_neighbours` made the expectation move with the generator, so
    # shrinking it restored the old 820-case product with everything green.
    # Comparing only CARDINALITY then let the vocabulary be substituted:
    # swapping the Branch STEP neighbour `target` for `exception` keeps the count
    # identical while removing all 40 required `opp=target` cases, and the count,
    # uniqueness, partition, denied-cell, parity, safety and killed-reparse checks
    # all stayed green. Only comparing the SET catches that.
    expected_ids = set()
    for context, slot in MATRIX:
        if slot == bc.STEP_SLOT:
            opposite = sorted(MATRIX[(context, bc.TERMINAL_SLOT)])
        else:
            opposite = [None] + sorted(MATRIX[(context, bc.STEP_SLOT)])
        for kind in KINDS:
            for neighbor in opposite:
                for mode in MODES:
                    expected_ids.add(
                        "{0}/{1}={2}/opp={3}/mode={4}".format(
                            context, slot, kind, neighbor or "EMPTY", mode
                        )
                    )
    for first in KINDS:
        expected_ids.add("root/[{0}]".format(first))
        for second in KINDS:
            expected_ids.add("root/[{0},{1}]".format(first, second))

    cases = _measured()["cases"]
    generated_ids = {cid for cid, _ir in cases}
    assert generated_ids == expected_ids, {
        "missing": sorted(expected_ids - generated_ids)[:20],
        "unexpected": sorted(generated_ids - expected_ids)[:20],
    }
    assert len(generated_ids) == len(cases), "case ids collide"


def test_every_document_matches_the_signature_its_id_claims():
    """Labels are not evidence. Each constructed document is projected back to its
    `(candidate, neighbour)` signature and compared with its own ID, so a case
    that is mislabelled fails here rather than silently covering the wrong cell.
    """
    mismatched = []
    for cid, ir in _measured()["cases"]:
        if cid.startswith("root/"):
            continue
        context, rest = cid.split("/", 1)
        slot_kind, opp, _mode = rest.split("/", 2)
        slot, kind = slot_kind.split("=", 1)
        claimed_neighbor = opp.split("=", 1)[1]
        mode = _mode.split("=", 1)[1]
        observed_kind, observed_neighbor = _observed_signature(ir, context, mode, slot)
        expected_neighbor = None if claimed_neighbor == "EMPTY" else claimed_neighbor
        if observed_kind != kind or observed_neighbor != expected_neighbor:
            mismatched.append(
                (cid, observed_kind, observed_neighbor)
            )
    assert mismatched == [], mismatched[:20]


def test_both_partitions_are_populated():
    """The whole gate is vacuous if everything lands on one side."""
    rows = _measured()["rows"]
    accepted = [r for r in rows if r[2][0] == "ACCEPTED"]
    refused = [r for r in rows if r[2][0] == "REFUSED"]
    assert accepted, "no parser-accepted case — the parity check proves nothing"
    assert refused, "no parser-refused case — the parity check proves nothing"


def test_every_denied_matrix_cell_owns_a_refused_case():
    """Coverage claimed from the AUTHORITY's own case set: each `(context, slot,
    kind)` the matrix does not admit must actually be exercised and refused."""
    refused_ids = {
        cid for cid, _ir, parser, _c in _measured()["rows"] if parser[0] == "REFUSED"
    }
    missing = []
    for (context, slot), admitted in sorted(MATRIX.items()):
        for kind in KINDS:
            if kind in admitted:
                continue
            if not any(
                cid.startswith("{0}/{1}={2}/".format(context, slot, kind))
                for cid in refused_ids
            ):
                missing.append((context, slot, kind))
    assert missing == [], missing


# ---------------------------------------------------------------------------
# The gate itself.
# ---------------------------------------------------------------------------


def test_both_entry_points_serve_one_diagnostic_identity():
    """THE GATE. For every generated document the two entry points must agree on
    the complete ordered diagnostic vector — code, JSON pointer, message and
    remediation — not merely on whether they refuse."""
    mismatches = [
        (cid, parser, compiler)
        for cid, _ir, parser, compiler in _measured()["rows"]
        if parser != compiler
    ]
    assert mismatches == [], "\n".join(
        "{0}\n  parser  : {1}\n  compiler: {2}".format(*m) for m in mismatches[:20]
    )


def test_a_parser_finding_is_served_through_the_compile_error_family():
    """`ProcessIRValidationError` and `ProcessIRCompileError` are unrelated types,
    and every production handler catches only the latter. A raw parser error
    escaping the compile entry would defeat all of them and serve a refusal with
    no error code."""
    seen = 0
    for _cid, _ir, parser, compiler in _measured()["rows"]:
        if parser[0] != "REFUSED":
            continue
        seen += 1
        assert compiler[0] == "REFUSED"
    assert seen, "no refused case observed — the assertion would be vacuous"


def test_translated_parser_diagnostics_carry_the_schema_phase():
    """`phase` is the compiler-side half of the served identity and has no parser
    counterpart, so it is pinned separately rather than left to drift."""
    checked = 0
    for _cid, ir, parser, _compiler in _measured()["rows"]:
        if parser[0] != "REFUSED":
            continue
        with pytest.raises(ProcessIRCompileError) as excinfo:
            pl.compile_process_ir_v1(ir, None)
        assert all(d.phase == "schema" for d in excinfo.value.diagnostics), [
            (d.code, d.phase) for d in excinfo.value.diagnostics
        ]
        checked += 1
        if checked >= 40:  # a sample is enough for a field-level invariant
            break
    assert checked, "no refused case observed — the assertion would be vacuous"


def test_the_safety_property_holds_in_both_directions():
    """ACCEPTANCE ONLY — deliberately not derived from diagnostic equality, so it
    still means something if the identity gate above is ever weakened.

    This property did NOT hold at the `cdd7a3b` baseline, contrary to #178's own
    premise: the compiler accepted and fully compiled a Branch leg with a trailing
    `cache_put`, a root `source` out of position, and any mutated `version`. So
    this test FAILED before the fix and passes after it, which is the strongest
    available witness that it is not vacuous.

    Scope: grammar acceptance at the compile entry. Full compilation may still
    reject a parser-valid document on symbol, CFG or lineage grounds the parser
    cannot reach, which is why CFG lowering is replaced by a boundary sentinel.
    """
    divergent = [
        (cid, parser[0], compiler[0])
        for cid, _ir, parser, compiler in _measured()["rows"]
        if (parser[0] == "ACCEPTED") != (compiler[0] == "ACCEPTED")
    ]
    assert divergent == [], divergent[:20]


def test_a_legal_document_round_trips_through_the_reparse_unchanged():
    """The re-parse must be IDENTITY on canonical JSON for accepted documents.

    `build_materialization_plan` stores the CALLER's model in the plan and
    fingerprints it, while the emission plan now comes from the re-parsed copy.
    Those agree only because of this property, and nothing else pinned it.
    """
    checked = 0
    for _cid, ir, parser, _compiler in _measured()["rows"]:
        if parser[0] != "ACCEPTED":
            continue
        reparsed = parse_process_ir_v1(ir.model_dump(mode="json", warnings=False))
        assert canonical_process_ir_json(reparsed) == canonical_process_ir_json(ir)
        checked += 1
    assert checked, "no accepted case observed — the assertion would be vacuous"


def test_no_legacy_exemption_can_reach_a_parser_diagnostic():
    """The re-parse is unconditional, including for policy-bearing legacy callers.
    That is sound only because a legacy policy downgrades post-lowering SEMANTIC
    findings while the parser raises GRAMMAR findings. Derived from both
    authorities, so it fails closed if either set grows."""
    from boomi_mcp.compiler.process_ir.semantic_validation import (
        validation_policy as vp,
    )

    parser_codes = {spec["code"] for spec in process_ir_v1_parse_diagnostic_specs()}
    exempt_codes = set(vp._EXEMPT_CODE.values())
    assert parser_codes and exempt_codes, "one side is empty — the check is vacuous"
    assert parser_codes & exempt_codes == set(), sorted(parser_codes & exempt_codes)


# ---------------------------------------------------------------------------
# Non-vacuity: delete the fix, and this same collector must go red.
# ---------------------------------------------------------------------------


def test_the_gate_fails_when_the_structural_fix_is_removed(monkeypatch):
    """THE WITNESS. Neuter `_reparse_process_ir_for_compile` — the exact edit a
    future refactor might make "because the caller already parsed" — and assert
    the collector above reports real mismatches, including the specific corpus
    case. Hand-run when written: 422 mismatches of 820.

    A guard whose failure mode has never been observed is not a guard.
    """
    # The probe is a case measured to diverge under the mutant — a witness aimed
    # at a case that agrees either way would pass while proving nothing. This one
    # is the ACCEPT-DIRECTION hole: the parser refuses a trailing `cache_put` in a
    # Branch leg and the unfixed compiler models no such rule, so it accepts and
    # compiles the document.
    # Corpus row 1's ACTUAL shape: a `process_call` TERMINAL whose leg carries a
    # `cache_put` prefix. That interaction is the one #175 deferred and the one a
    # single-slot product cannot generate, so it is the case the witness must
    # pin — an earlier revision pinned `step=cache_put/opp=stop`, which diverges
    # too but is not the mandated interaction.
    probe = "branch_leg/terminal=process_call/opp=cache_put/mode=clean"
    cases = _measured()["cases"]
    assert any(cid == probe for cid, _ir in cases), "the probe case vanished"

    monkeypatch.setattr(pl, "_reparse_process_ir_for_compile", lambda ir: ir)

    mismatches = {}
    for cid, ir in cases:
        parser, compiler = _measure(ir)
        if parser != compiler:
            mismatches[cid] = (parser, compiler)

    assert mismatches, "removing the re-parse changed nothing — the gate is vacuous"
    assert probe in mismatches, sorted(mismatches)[:20]
    parser, compiler = mismatches[probe]
    # Corpus row 1 is a DIAGNOSTIC-IDENTITY divergence: both paths refuse, and
    # they disagree on which rule owns the document. That is the shape #175
    # deferred, so the witness asserts it precisely rather than settling for
    # "these differ somehow".
    assert parser[0] == "REFUSED" and compiler[0] == "REFUSED", (parser, compiler)
    assert parser[1][0] == "PROCESS_IR_SCHEMA_INVALID_CARDINALITY", parser
    assert (
        compiler[1][0]
        == "PROCESS_IR_CAPABILITY_PROCESS_CALL_RETURN_PATH_BINDING_UNSUPPORTED"
    ), compiler
    assert parser[1][0] != compiler[1][0]
    # The witness must be BROAD, not a single lucky cell: the unfixed compiler
    # diverges across the grammar, which is why a verdict function per rule could
    # never close this class.
    assert len(mismatches) > 100, len(mismatches)


# ---------------------------------------------------------------------------
# The five inherited divergences, as a regression corpus.
# ---------------------------------------------------------------------------


def _corpus_branch_cache_prefix_call():
    ir = parse_process_ir_v1(_carrier(bc.BRANCH_LEG, "clean"))
    leg = ir.body.steps[0].legs[0]
    leg.steps = [NODE.validate_python(_atom("cache_put"))]
    leg.terminal = NODE.validate_python(_atom("process_call"))
    return ir


def _corpus_root_branch_then_call():
    ir = parse_process_ir_v1(copy.deepcopy(_ROOT_CARRIER))
    ir.body.steps = [
        NODE.validate_python(_atom("branch")),
        NODE.validate_python(_atom("process_call")),
    ]
    return ir


def _corpus_root_call_then_source():
    ir = parse_process_ir_v1(copy.deepcopy(_ROOT_CARRIER))
    ir.body.steps = [
        NODE.validate_python(_atom("process_call")),
        NODE.validate_python(_atom("source")),
    ]
    return ir


def _corpus_process_try_call_first_step():
    ir = parse_process_ir_v1(_carrier(bc.TRY_BODY, "clean"))
    ir.body.steps[0].try_body.steps = [NODE.validate_python(_atom("process_call"))]
    return ir


def _corpus_connector_above_leg_terminal_call():
    """Row 5, NO-PREFIX reading — the message-only divergence.

    `leg.steps` is cleared deliberately: the carrier's legs each carry a `message`
    step, and leaving it makes this the PREFIX reading instead, which is a
    different defect with a different code. #175's wording ("Root connector ->
    Branch leg terminal Process Call") does not say which, so both are pinned.
    """
    ir = parse_process_ir_v1(_carrier(bc.BRANCH_LEG, "connector_above"))
    leg = ir.body.steps[1].legs[0]
    leg.steps = []
    leg.terminal = NODE.validate_python(_atom("process_call"))
    return ir


def _corpus_connector_above_leg_prefix_terminal_call():
    """Row 5, PREFIX reading — a CODE divergence rather than a message one."""
    ir = parse_process_ir_v1(_carrier(bc.BRANCH_LEG, "connector_above"))
    leg = ir.body.steps[1].legs[0]
    leg.steps = [NODE.validate_python(_atom("set_dpp"))]
    leg.terminal = NODE.validate_python(_atom("process_call"))
    return ir


#: Each row pins the PARSER triple MEASURED AT THE BASELINE `cdd7a3b`, before any
#: source edit, and archived in
#: `docs/architecture/evidence/issue-178/baseline-corpus-characterization.md`.
#: The assertion is that public compile now serves that same triple. Reintroducing
#: any of the five divergences fails here.
CORPUS = [
    (
        "branch-cache-prefix-process-call-terminal",
        _corpus_branch_cache_prefix_call,
        "PROCESS_IR_SCHEMA_INVALID_CARDINALITY",
        "/body/steps/0/legs/0",
        (
            "a trailing cache_put belongs in the leg terminal (target-less staging leg), not in steps"
        ),
    ),
    (
        "root-branch-then-process-call",
        _corpus_root_branch_then_call,
        "PROCESS_IR_SEMANTIC_CONTROL_CONTINUATION_UNSUPPORTED",
        "/body",
        (
            "no step may follow a branch or decision — control nodes are terminal fan-out in ProcessIR v1 (continuation_after_branch_or_decision is gated)"
        ),
    ),
    (
        "root-process-call-then-source",
        _corpus_root_call_then_source,
        "PROCESS_IR_SCHEMA_INVALID_CARDINALITY",
        "/body",
        (
            "source may appear only as the first step"
        ),
    ),
    (
        "process-try-process-call-first-step",
        _corpus_process_try_call_first_step,
        "PROCESS_IR_CAPABILITY_NODE_NOT_ALLOWED_IN_BODY",
        "/body/steps/0/try_body/steps/0",
        (
            "this node kind is not admitted in this control-body slot"
        ),
    ),
    (
        "root-connector-branch-process-call-terminal",
        _corpus_connector_above_leg_terminal_call,
        "PROCESS_IR_CAPABILITY_NODE_NOT_ALLOWED_IN_BODY",
        "/body/steps/1/legs/0/terminal",
        (
            "a process_call may not share a root-to-leaf path with a connector step — a connector runs upstream of this body (process_call_connector_mixing is gated)"
        ),
    ),
    (
        "root-connector-branch-process-call-terminal-with-prefix",
        _corpus_connector_above_leg_prefix_terminal_call,
        "PROCESS_IR_CAPABILITY_PROCESS_CALL_RETURN_PATH_BINDING_UNSUPPORTED",
        "/body/steps/1/legs/0/terminal",
        (
            "a process_call branch leg terminal admits no preceding steps — a call whose child returns no documents ends the path it is on, and a prefix before it is not attested"
        ),
    ),
]


@pytest.mark.parametrize(
    "case_id,build,expected_code,expected_path,expected_message",
    CORPUS,
    ids=[row[0] for row in CORPUS],
)
def test_the_inherited_divergences_are_pinned(
    case_id, build, expected_code, expected_path, expected_message
):
    """#175 ledger rows `L3R3-01` and `L3R3-02`, discharged.

    Both were deferred once under `blocked-by-mechanism`, so neither may be
    deferred again. Two of the five were reviewer-reported and never independently
    reproduced at #175; all five reproduce at the baseline, and row 3
    (`root-process-call-then-source`) needed a `source` carrying BOTH
    `connection_ref` and `operation_ref` — the shape that defeated the earlier
    probe.
    """
    ir = build()

    with pytest.raises(ProcessIRValidationError) as parser_exc:
        parse_process_ir_v1(ir.model_dump(mode="json", warnings=False))
    parser_first = parser_exc.value.diagnostics[0]
    # The MEASURED baseline triple, hard-pinned. Taking the message from the live
    # parser and comparing compile against THAT would pass if both paths drifted
    # together — which is precisely the failure mode #178 exists to prevent, since
    # the two paths drifting in step is what "identical diagnostics" would look
    # like from the inside. These strings are the ones recorded at `cdd7a3b` in
    # `docs/architecture/evidence/issue-178/baseline-corpus-characterization.md`.
    assert (parser_first.code, parser_first.path, parser_first.message) == (
        expected_code,
        expected_path,
        expected_message,
    )

    with pytest.raises(ProcessIRCompileError) as compile_exc:
        pl.compile_process_ir_v1(ir, None)
    served = compile_exc.value.diagnostics[0]

    assert (served.code, served.path, served.message) == (
        parser_first.code,
        parser_first.path,
        parser_first.message,
    )
    assert served.phase == "schema"


def test_the_corpus_message_clause_divergence_is_closed():
    """Row 5's no-prefix reading was a MESSAGE-ONLY divergence: identical code and
    pointer, and only the parser's message carried the upstream-connector clause.
    A code-and-pointer assertion would have passed throughout, so the clause is
    asserted explicitly."""
    ir = _corpus_connector_above_leg_terminal_call()
    with pytest.raises(ProcessIRValidationError) as parser_exc:
        parse_process_ir_v1(ir.model_dump(mode="json", warnings=False))
    with pytest.raises(ProcessIRCompileError) as compile_exc:
        pl.compile_process_ir_v1(ir, None)
    parser_message = parser_exc.value.diagnostics[0].message
    assert "a connector runs upstream of this body" in parser_message
    assert compile_exc.value.diagnostics[0].message == parser_message


# ---------------------------------------------------------------------------
# Stage-2 round 1 finding: a json dump REPAIRS some invalid mutated values.
# ---------------------------------------------------------------------------


def _control_flow_ir():
    import json

    doc = json.loads(
        (_ROOT / "tests" / "fixtures" / "process_ir" / "process_ir_v1.json").read_text()
    )["control_flow"]
    return parse_process_ir_v1(copy.deepcopy(doc))


def test_a_wrong_typed_mutation_is_refused_exactly_as_the_parser_refuses_it():
    """A json dump is COERCIVE, so the re-parse alone did not close the hole.

    `ProcessIRV1` is not validate-on-assignment, so `node.text = datetime(...)`
    leaves a `str` field holding a `datetime`, and `model_dump(mode="json")`
    renders it as `'2020-01-01T00:00:00'` — the parser is handed an
    already-repaired document and accepts it.

    The fix mirrors the PARSER against the raw state; it does not invent a
    stricter rule. That distinction is the whole point and is asserted below by
    `test_bytes_is_accepted_because_the_parser_accepts_it`: an earlier revision
    strict-validated here, which refused `bytes` that the parser ACCEPTS, and so
    made the two entry points disagree where they had agreed — DC-175-E
    reintroduced one layer up while nominally fixing it.
    """
    import datetime as _dt

    ir = _control_flow_ir()
    ir.body.steps[1].text = _dt.datetime(2020, 1, 1)

    # The coercion is real: the json dump alone yields a document the parser
    # accepts. Without this control the test could pass for the wrong reason.
    parse_process_ir_v1(copy.deepcopy(ir.model_dump(mode="json", warnings=False)))

    with pytest.raises(ProcessIRCompileError) as excinfo:
        pl.compile_process_ir_v1(ir, None)
    served = excinfo.value.diagnostics[0]
    assert served.code == "PROCESS_IR_SCHEMA_INVALID", [
        (d.code, d.path) for d in excinfo.value.diagnostics
    ]
    # A REAL pointer, not a bare refusal — the parser's own translated `loc`.
    assert served.path == "/body/steps/1/text"
    assert served.phase == "schema"


def test_bytes_is_accepted_because_the_parser_accepts_it():
    """The compile entry must not be STRICTER than its own authority.

    `parse_process_ir_v1` lax-coerces `bytes` to `str`, so it accepts a `bytes`
    value in a `str` field. The compile entry therefore must too — otherwise the
    two public entry points disagree, which is the exact defect class this slice
    closes. Measured: a strict-validating revision refused this and had to be
    withdrawn.
    """
    from boomi_mcp.models.process_ir import (
        assert_process_ir_v1_type_faithful,
        raw_process_ir_payload,
    )

    ir = _control_flow_ir()
    ir.body.steps[1].text = b"abc"

    # The authority accepts the RAW state...
    parse_process_ir_v1(raw_process_ir_payload(ir))
    assert_process_ir_v1_type_faithful(ir)
    # ...so the compile entry must reach the compile stages rather than refuse at
    # the grammar boundary. `symbols=None` fails later, which is not our business.
    try:
        pl.compile_process_ir_v1(ir, None)
    except ProcessIRCompileError as exc:
        assert exc.diagnostics[0].phase != "schema", [
            (d.code, d.phase) for d in exc.diagnostics
        ]


def test_a_mutated_value_never_reaches_a_serializer_warning():
    """AR2-01, swept across every ProcessIR dump site (#178 QA round 2).

    A dump of a possibly-mutated model under pydantic's DEFAULT `warnings=True`
    interpolates the authored value into a warning — measured to emit a planted
    secret verbatim. QA found one hardened site and three unhardened siblings, so
    this pins the property behaviourally rather than by scanning source.
    """
    import warnings as _warnings

    from boomi_mcp.models.process_ir import (
        canonical_process_ir_json,
        raw_process_ir_payload,
    )

    canary = "QA178-CANARY-sk_live_0ff1ce"
    ir = _control_flow_ir()
    ir.body.steps[1].text = canary.encode()

    for label, call in (
        ("canonical_process_ir_json", lambda: canonical_process_ir_json(ir)),
        ("raw_process_ir_payload", lambda: raw_process_ir_payload(ir)),
        ("compile entry", lambda: pl._reparse_process_ir_for_compile(ir)),
    ):
        with _warnings.catch_warnings(record=True) as caught:
            _warnings.simplefilter("always")
            call()
        leaked = [w for w in caught if canary in str(w.message)]
        assert leaked == [], "{0} leaked the authored value".format(label)

    # NON-VACUITY: the unhardened form this swept away really does leak, so the
    # assertions above are observations rather than a guard that cannot fire.
    with _warnings.catch_warnings(record=True) as caught:
        _warnings.simplefilter("always")
        ir.model_dump(mode="json")
    assert any(canary in str(w.message) for w in caught), (
        "the control did not leak — this test can no longer detect a regression"
    )


def test_type_faithfulness_accepts_every_legal_generated_document():
    """Non-vacuity in the other direction: the new strict check must not refuse a
    single legal document. Measured 0 false positives across the whole
    parser-accepted partition when the check was chosen."""
    from boomi_mcp.models.process_ir import assert_process_ir_v1_type_faithful

    checked = 0
    for _cid, ir, parser, _compiler in _measured()["rows"]:
        if parser[0] != "ACCEPTED":
            continue
        assert_process_ir_v1_type_faithful(ir)
        checked += 1
    assert checked, "no accepted case observed — the assertion would be vacuous"


def test_a_one_shot_iterable_field_is_read_exactly_once():
    """The re-parse must not DESTROY the state it is judging (#178 Stage-2 r2).

    `ProcessIRV1` is not validate-on-assignment, so a field can hold a one-shot
    iterable. An earlier revision dumped to json as a cheap type-violation
    detector and re-dumped raw when that raised — but the first dump CONSUMES a
    generator, so the second saw an exhausted one, reported an empty
    `/body/steps`, and refused a document the parser accepts. Worse, the served
    diagnostic described the probe's own damage (a cardinality violation) rather
    than anything about the document.

    The compile entry now reads the state exactly once. This test pins that
    property where it is observable: through the public entry, end to end.
    """
    ir = _control_flow_ir()
    original = len(ir.body.steps)
    assert original > 1, "the control needs several steps to be meaningful"
    ir.body.steps = (step for step in ir.body.steps)

    revalidated = pl._reparse_process_ir_for_compile(ir)
    assert len(revalidated.body.steps) == original, (
        "the re-parse consumed the iterable before judging it"
    )
    assert canonical_process_ir_json(revalidated) == canonical_process_ir_json(
        _control_flow_ir()
    )


def test_the_reparse_never_builds_a_json_projection_of_the_caller_model():
    """A `mode="json"` dump REPAIRS wrong-typed values, so building one — even
    only to probe — reopens the hole this entry exists to close. Pinned on the
    source because the defect is the CALL, not an observable output."""
    import ast
    import inspect
    import textwrap

    # Scan the CODE, not the prose. The docstring names `mode="json"` in order to
    # explain why it is wrong, so a raw substring check matches the explanation
    # and reports the very thing it is meant to permit — the first draft of this
    # test did exactly that.
    tree = ast.parse(
        textwrap.dedent(inspect.getsource(pl._reparse_process_ir_for_compile))
    )
    func = tree.body[0]
    body = func.body[1:] if ast.get_docstring(func) else func.body
    code = "\n".join(ast.dump(node) for node in body)
    assert "json" not in code, ast.unparse(ast.Module(body=body, type_ignores=[]))
    assert "raw_process_ir_payload" in code


def test_the_caller_model_is_read_exactly_once_and_that_is_the_contract():
    """A one-shot iterable field makes a model SINGLE-USE. Pinned, not fixed.

    Re-validating reads every field, so the first compile drains a generator and
    a second compile of the same object refuses. This cannot be sequenced away —
    the identical drain occurs on the pre-#178 dump shape, so it is a property of
    re-parsing at all rather than of any particular implementation, and Python
    offers no way to read a one-shot iterable twice.

    It is pinned here so the behaviour is a stated contract rather than a
    surprise: QA measured the canonical apply path handing ONE model object to
    the public entry three times, which is safe only because production models
    come from a parse and hold materialised containers.
    """
    ir = _control_flow_ir()
    original = len(ir.body.steps)
    ir.body.steps = (step for step in ir.body.steps)

    first = pl._reparse_process_ir_for_compile(ir)
    assert len(first.body.steps) == original

    with pytest.raises(ProcessIRCompileError) as excinfo:
        pl._reparse_process_ir_for_compile(ir)
    assert excinfo.value.diagnostics[0].code == "PROCESS_IR_SCHEMA_INVALID_CARDINALITY"

    # ...and the documented escape hatch really is one: the model handed back by
    # the first call is materialised, so it compiles repeatedly.
    for _ in range(3):
        again = pl._reparse_process_ir_for_compile(first)
        assert len(again.body.steps) == original


def test_the_precondition_is_stated_on_the_public_entry():
    """A contract nobody can find is not a contract. This is the one place a
    caller of the public entry would look."""
    import inspect

    doc = inspect.getdoc(pl.compile_process_ir_v1) or ""
    assert "READ EXACTLY ONCE" in doc
    assert "one-shot" in doc.lower()


def test_a_dump_failure_cannot_smuggle_an_authored_diagnostic():
    """A dump exception is INTERNAL, whatever type it claims to be.

    `ProcessIRV1` is exported, so a caller can subclass it and raise
    `ProcessIRValidationError` from `model_dump`. An earlier revision forwarded
    that verbatim on the reasoning that the type means "parser-authored" — but
    parsing has not started when the dump runs, so the type proves nothing. The
    consequence was measured, not theorised: an arbitrary code, pointer and
    message travelled through the compiler's error channel, carrying a planted
    secret into the served diagnostic and reopening AR2-01.
    """
    from boomi_mcp.models.process_ir import ProcessIRV1

    secret = "QA178-SUBCLASS-sk_live_0ff1ce"

    class _Smuggler(ProcessIRV1):
        def model_dump(self, **_kwargs):
            raise ProcessIRValidationError(
                [
                    type(
                        "_D",
                        (),
                        {
                            "code": "TOTALLY_MADE_UP_CODE",
                            "path": "/attacker/controlled",
                            "message": secret,
                            "remediation": "do whatever the caller says",
                        },
                    )()
                ]
            )

    base = _control_flow_ir()
    smuggler = _Smuggler.model_construct(
        **{name: getattr(base, name) for name in type(base).model_fields}
    )

    with pytest.raises(ProcessIRCompileError) as excinfo:
        pl._reparse_process_ir_for_compile(smuggler)
    served = excinfo.value.diagnostics[0]
    assert served.code == "PROCESS_IR_COMPILE_INTERNAL", served.code
    assert served.path == ""
    assert secret not in (served.message or "")
    assert "TOTALLY_MADE_UP_CODE" not in (served.message or "")
    assert secret not in (getattr(served, "remediation", "") or "")


def _forging_subclass(raise_from_items):
    """A caller-supplied model whose dump RETURNS a hostile container.

    This is the vector three separate rounds kept re-finding: the dump does not
    raise, it hands back a mapping whose `items()` runs caller code inside
    `parse_process_ir_v1` — the secret pre-scan walks the payload — and raises a
    diagnostic the parser never authored.
    """
    from boomi_mcp.models.process_ir import ProcessIRV1

    base = _control_flow_ir()

    class _Hostile(dict):
        def items(self):
            raise raise_from_items()

    class _Sub(ProcessIRV1):
        def model_dump(self, **_kwargs):
            return _Hostile(base.model_dump(mode="python", warnings=False))

    return _Sub.model_construct(
        **{name: getattr(base, name) for name in ProcessIRV1.model_fields}
    )


CANARY = "PARITY-FORGE-CANARY-sk_live_c0ffee"


def test_a_forged_diagnostic_with_a_REAL_parser_code_cannot_serve_its_text():
    """The variant an allowlist cannot catch, closed at the input instead.

    Checking the served diagnostic could never win this: with a genuine parser
    code attached, forged `path`/`message`/`remediation` are indistinguishable
    from authored ones by inspection. The payload is made inert BEFORE parsing, so
    caller code never runs inside the parser and the forged error is never raised
    at all.
    """
    def _forge():
        return ProcessIRValidationError(
            [
                type(
                    "_D",
                    (),
                    {
                        "code": "PROCESS_IR_SCHEMA_INVALID",  # a REAL parser code
                        "path": "/forged",
                        "message": "forged message " + CANARY,
                        "remediation": "forged remediation " + CANARY,
                    },
                )()
            ]
        )

    with pytest.raises(ProcessIRCompileError) as excinfo:
        pl._reparse_process_ir_for_compile(_forging_subclass(_forge))
    served = excinfo.value.diagnostics[0]
    assert served.code == "PROCESS_IR_COMPILE_INTERNAL", served.code
    assert served.path == ""
    assert CANARY not in (served.message or "")
    assert CANARY not in (getattr(served, "remediation", "") or "")


def test_a_forged_compile_error_from_inside_the_parse_cannot_pass_through():
    """The deleted passthrough arm, pinned.

    `_parse_payload_for_compile` used to carry `except ProcessIRCompileError:
    raise`, which forwarded a forged compile error with no allowlist at all. It
    was dead for its stated purpose — nothing under `models/` raises that type —
    and it is the same shape as the arm deleted one boundary earlier.
    """
    def _forge():
        from boomi_mcp.compiler.process_ir.diagnostics import CompilerDiagnostic

        return ProcessIRCompileError(
            [
                CompilerDiagnostic(
                    code="FORGED_COMPILE_CODE",
                    phase="schema",
                    path="/forged/compile",
                    node_identity="",
                    message="forged " + CANARY,
                    remediation="forged " + CANARY,
                )
            ]
        )

    with pytest.raises(ProcessIRCompileError) as excinfo:
        pl._reparse_process_ir_for_compile(_forging_subclass(_forge))
    served = excinfo.value.diagnostics[0]
    assert served.code == "PROCESS_IR_COMPILE_INTERNAL", served.code
    assert CANARY not in (served.message or "")
    assert "FORGED_COMPILE_CODE" not in (served.message or "")


def test_the_inert_rebuild_keeps_scalars_and_flattens_containers():
    """The rebuild must not become a coercion.

    Flattening containers is the whole defence; coercing SCALARS would re-open the
    `datetime` -> ISO-string repair that #178 exists to close, so a wrong-typed
    scalar must still arrive at the parser wrong-typed.
    """
    import datetime as _dt

    class _HostileDict(dict):
        pass

    payload = _HostileDict(
        {"a": _HostileDict({"b": (1, 2)}), "when": _dt.datetime(2020, 1, 1)}
    )
    inert = pl._inert_payload(payload)
    assert type(inert) is dict
    assert type(inert["a"]) is dict
    assert type(inert["a"]["b"]) is list
    # the scalar is untouched, by design
    assert inert["when"] == _dt.datetime(2020, 1, 1)
    assert isinstance(inert["when"], _dt.datetime)


def test_scalar_normalisation_preserves_the_value_it_normalises():
    """Stripping hooks must not CHANGE the document.

    `builtin(value)` does both jobs badly: it dispatches an overridable
    conversion, and for `class V(str, Enum): ONE = "1"` it yields `"V.ONE"` — so
    the compile entry would refuse a version the parser ACCEPTS. That is a parity
    break in the accept direction, introduced by the hardening meant to establish
    parity. The base-class slot bypasses the override AND preserves the value.
    """
    from enum import Enum

    class _V(str, Enum):
        ONE = "1"

    class _IE(int, Enum):
        TWO = 2

    assert pl._plain_scalar(_V.ONE) == "1"
    assert type(pl._plain_scalar(_V.ONE)) is str
    assert pl._plain_scalar(_IE.TWO) == 2
    assert type(pl._plain_scalar(_IE.TWO)) is int

    # ...and end to end: a str-Enum version must still COMPILE, not be refused.
    from boomi_mcp.models.process_ir import ProcessIRV1

    base = _control_flow_ir()

    class _Sub(ProcessIRV1):
        def model_dump(self, **_kwargs):
            payload = base.model_dump(mode="python", warnings=False)
            payload["version"] = _V.ONE
            return payload

    smuggler = _Sub.model_construct(
        **{name: getattr(base, name) for name in ProcessIRV1.model_fields}
    )
    revalidated = pl._reparse_process_ir_for_compile(smuggler)
    assert revalidated.version == "1"
    assert type(revalidated.version) is str


def test_hook_stripping_and_value_preservation_hold_together():
    """The two properties are not in tension, and both are asserted on ONE value.

    A fix that stripped hooks by corrupting the value, or preserved the value by
    keeping the hooks, would satisfy one of these tests and fail the other.
    """
    class _Sneaky(str):
        def __str__(self):
            return "CHANGED"

    reduced = pl._plain_scalar(_Sneaky("kept"))
    assert reduced == "kept", reduced          # value preserved
    assert type(reduced) is str                # hooks stripped
