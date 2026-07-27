"""State lineage lattice (issue #143, M12.8) — slice 4. Still DARK.

The three rules that are easiest to get subtly wrong each get their own test,
because they are not variations of one rule — they follow from two DIFFERENT
scoping facts:

1. pre-Branch DDP is visible in every leg      (documents are copied FROM it)
2. leg-local DDP does NOT satisfy a sibling    (each leg has its own copies)
3. an earlier leg's DPP/cache write DOES reach a later leg  (legs run in order)

A model that got 1 and 2 right by treating legs as isolated would get 3 wrong,
which is why 3 is asserted separately and in both directions.
"""

from __future__ import annotations

import json
import re
import sys
from pathlib import Path

import pytest

_ROOT = Path(__file__).resolve().parent.parent
for _p in (str(_ROOT), str(_ROOT / "src")):
    if _p not in sys.path:
        sys.path.insert(0, _p)

from boomi_mcp.compiler.process_ir.contracts import SymbolTableV1
from boomi_mcp.compiler.process_ir.semantic_validation.context import (
    prepare_validation_context,
)
from boomi_mcp.compiler.process_ir.semantic_validation.lineage import (
    DDP,
    DPP,
    _State,
    collect_lineage_findings,
)
from boomi_mcp.errors import (
    PROCESS_IR_SEMANTIC_LINEAGE_BRANCH_ORDER_INVALID,
    PROCESS_IR_SEMANTIC_LINEAGE_CACHE_WRITER_MISSING,
    PROCESS_IR_SEMANTIC_LINEAGE_DDP_SCOPE_INVALID,
    PROCESS_IR_SEMANTIC_LINEAGE_EFFECT_UNKNOWN,
    PROCESS_IR_SEMANTIC_LINEAGE_EXTERNAL_WRITER_ASSUMED,
    PROCESS_IR_SEMANTIC_LINEAGE_PROPERTY_READ_BEFORE_WRITE,
)
from boomi_mcp.models.process_ir import parse_process_ir_v1


def _set_prop(scope, name, sources=None):
    return {
        "kind": "set_ddp" if scope == "ddp" else "set_dpp",
        "name": name,
        "source_values": sources or [{"value_type": "static", "value": "x"}],
    }


def _read_prop(scope, name, target="OUT", default=None):
    source = {"value_type": scope, "property_name": name}
    if default is not None:
        source["default_value"] = default
    return {
        "kind": "set_dpp",
        "name": target,
        "source_values": [source],
    }


_SOURCE = {"kind": "source", "connection_ref": "$ref:conn", "operation_ref": "$ref:op"}
_TARGET = {"kind": "target", "connection_ref": "$ref:tconn", "operation_ref": "$ref:top"}


def _doc(steps):
    """A valid ROOT connector-flow sequence wrapping ``steps``.

    The root sequence rules are strict: it must start with the source endpoint
    and end in target+stop. A bare list of property steps is not a legal
    document, so the linear steps under test are bracketed rather than authored
    at the root. Neither endpoint reads or writes process state, so they are
    inert for lineage purposes.
    """
    return {
        "version": "1",
        "body": {
            "kind": "sequence",
            "steps": [_SOURCE] + list(steps) + [_TARGET, {"kind": "stop"}],
        },
    }


def _branch_doc(legs):
    return {
        "version": "1",
        "body": {
            "kind": "sequence",
            "steps": [
                {
                    "kind": "branch",
                    "legs": [
                        {"steps": leg, "terminal": {"kind": "stop"}} for leg in legs
                    ],
                }
            ],
        },
    }


def _findings(doc):
    prepared = prepare_validation_context(
        parse_process_ir_v1(doc), SymbolTableV1(symbols=())
    )
    return collect_lineage_findings(prepared)


def _codes(doc):
    return {f.code for f in _findings(doc)}


# ---------------------------------------------------------------------------
# the lattice itself
# ---------------------------------------------------------------------------


def test_the_merge_is_an_intersection_not_a_union():
    """Union here would be the single easiest way to make the module unsound:
    a property written on ONE Decision arm would count as established after the
    merge, on a path where it does not exist."""
    left = _State(document=frozenset({(DDP, "A")}), execution=frozenset({(DPP, "X")}))
    right = _State(document=frozenset(), execution=frozenset({(DPP, "X"), (DPP, "Y")}))
    merged = left.merged_with(right)
    assert merged.execution == frozenset({(DPP, "X")})
    assert merged.document == frozenset()


def test_a_ddp_write_lands_in_document_scope_and_a_dpp_write_in_execution():
    state = _State().with_write((DDP, "A")).with_write((DPP, "B"))
    assert state.establishes((DDP, "A"))
    assert state.establishes((DPP, "B"))
    assert not state.establishes((DPP, "A"))


# ---------------------------------------------------------------------------
# straight-line reads
# ---------------------------------------------------------------------------


def test_a_write_then_read_is_clean():
    assert _codes(_doc([_set_prop("dpp", "A"), _read_prop("dpp", "A")])) == set()


def test_a_read_with_no_write_anywhere_is_read_before_write():
    codes = _codes(_doc([_read_prop("dpp", "A")]))
    assert PROCESS_IR_SEMANTIC_LINEAGE_PROPERTY_READ_BEFORE_WRITE in codes


def test_a_defaulted_read_cannot_fail():
    """A default establishes the value, so treating it as a hard dependency
    would reject a payload that runs perfectly well."""
    assert _codes(_doc([_read_prop("dpp", "A", default="fallback")])) == set()


def test_read_before_write_is_ordered_not_merely_present():
    """The write exists but comes AFTER the read — order is the whole point."""
    codes = _codes(_doc([_read_prop("dpp", "A"), _set_prop("dpp", "A")]))
    assert PROCESS_IR_SEMANTIC_LINEAGE_PROPERTY_READ_BEFORE_WRITE in codes


# ---------------------------------------------------------------------------
# Branch: the three rules
# ---------------------------------------------------------------------------


def test_rule_1_pre_branch_ddp_is_visible_in_every_leg():
    doc = {
        "version": "1",
        "body": {
            "kind": "sequence",
            "steps": [
                _SOURCE,
                _set_prop("ddp", "A"),
                {
                    "kind": "branch",
                    "legs": [
                        {
                            "steps": [_read_prop("ddp", "A")],
                            "terminal": {"kind": "stop"},
                        },
                        {
                            "steps": [_read_prop("ddp", "A")],
                            "terminal": {"kind": "stop"},
                        },
                    ],
                },
            ],
        },
    }
    assert _codes(doc) == set()


def test_rule_2_leg_local_ddp_does_not_satisfy_a_sibling_leg():
    """Each leg gets its OWN document copies, so leg 0's DDP write is not on
    leg 1's documents. Reported as a scope error, not read-before-write,
    because the property IS written — just onto a different copy."""
    codes = _codes(
        _branch_doc(
            [
                [_set_prop("ddp", "A")],
                [_read_prop("ddp", "A")],
            ]
        )
    )
    assert PROCESS_IR_SEMANTIC_LINEAGE_DDP_SCOPE_INVALID in codes


def test_rule_3_an_earlier_legs_dpp_write_does_reach_a_later_leg():
    """Legs run SEQUENTIALLY. A model that treated them as isolated would
    wrongly reject this, and a model that treated them as parallel would too."""
    assert (
        _codes(
            _branch_doc(
                [
                    [_set_prop("dpp", "A")],
                    [_read_prop("dpp", "A")],
                ]
            )
        )
        == set()
    )


def test_rule_3_inverse_a_later_legs_write_does_not_reach_an_earlier_leg():
    """The other direction must still fail, or rule 3 is just 'anything goes'.

    It reports BRANCH_ORDER_INVALID specifically, not the generic
    read-before-write: the write is right there in the payload, so telling the
    author it is missing would send them hunting for something that exists. The
    defect is its POSITION.
    """
    codes = _codes(
        _branch_doc(
            [
                [_read_prop("dpp", "A")],
                [_set_prop("dpp", "A")],
            ]
        )
    )
    assert PROCESS_IR_SEMANTIC_LINEAGE_BRANCH_ORDER_INVALID in codes
    assert PROCESS_IR_SEMANTIC_LINEAGE_PROPERTY_READ_BEFORE_WRITE not in codes


def test_branch_order_invalid_carries_the_offending_leg_ordinal():
    findings = _findings(
        _branch_doc([[_read_prop("dpp", "A")], [_set_prop("dpp", "A")]])
    )
    order = [
        f
        for f in findings
        if f.code == PROCESS_IR_SEMANTIC_LINEAGE_BRANCH_ORDER_INVALID
    ]
    assert order
    keys = {e.key for e in order[0].evidence}
    assert keys == {"state_scope", "leg_ordinal"}


def test_a_missing_write_outside_any_branch_is_still_read_before_write():
    """The order diagnostic must not swallow the plain case."""
    codes = _codes(_doc([_read_prop("dpp", "A")]))
    assert PROCESS_IR_SEMANTIC_LINEAGE_PROPERTY_READ_BEFORE_WRITE in codes
    assert PROCESS_IR_SEMANTIC_LINEAGE_BRANCH_ORDER_INVALID not in codes


def test_an_earlier_leg_cache_write_reaches_a_later_leg():
    """Cache is execution-scoped like DPP, so leg 0's write reaches leg 1.

    Leg 0 reads its own cache immediately after writing it because the schema
    requires a cache_put to be followed by a stream-replacing read — it may not
    feed a terminal directly. That extra read is a schema obligation, not part
    of what this test is asserting.
    """
    _get = {
        "kind": "cache_get",
        "cache_ref": "$ref:c",
        "empty_cache_behavior": "stopprocess",
        "external_writer": False,
    }
    doc = _branch_doc(
        [
            [{"kind": "cache_put", "cache_ref": "$ref:c"}, dict(_get)],
            [dict(_get)],
        ]
    )
    assert PROCESS_IR_SEMANTIC_LINEAGE_CACHE_WRITER_MISSING not in _codes(doc)


# ---------------------------------------------------------------------------
# cache
# ---------------------------------------------------------------------------


def test_a_cache_read_with_no_writer_is_reported():
    doc = _doc(
        [
            {
                "kind": "cache_get",
                "cache_ref": "$ref:c",
                "empty_cache_behavior": "stopprocess",
                "external_writer": False,
            }
        ]
    )
    assert PROCESS_IR_SEMANTIC_LINEAGE_CACHE_WRITER_MISSING in _codes(doc)


def test_a_declared_external_writer_downgrades_to_a_warning():
    doc = _doc(
        [
            {
                "kind": "cache_get",
                "cache_ref": "$ref:c",
                "empty_cache_behavior": "stopprocess",
                "external_writer": True,
            }
        ]
    )
    findings = _findings(doc)
    assumed = [
        f
        for f in findings
        if f.code == PROCESS_IR_SEMANTIC_LINEAGE_EXTERNAL_WRITER_ASSUMED
    ]
    assert assumed, "external writer should be recorded"
    assert assumed[0].severity == "warning"
    assert PROCESS_IR_SEMANTIC_LINEAGE_CACHE_WRITER_MISSING not in {
        f.code for f in findings
    }


# ---------------------------------------------------------------------------
# opaque effects — uncertainty, never proof
# ---------------------------------------------------------------------------


def test_a_map_contributes_uncertainty_not_proof():
    """The inversion of the legacy wildcard default: a map may NOT satisfy a
    read it does not declare."""
    doc = _doc([{"kind": "map_ref", "map_ref": "$ref:m"}, _read_prop("dpp", "A")])
    codes = _codes(doc)
    assert PROCESS_IR_SEMANTIC_LINEAGE_EFFECT_UNKNOWN in codes
    assert PROCESS_IR_SEMANTIC_LINEAGE_PROPERTY_READ_BEFORE_WRITE in codes


def test_the_unknown_effect_finding_is_a_warning_not_an_error():
    doc = _doc([{"kind": "map_ref", "map_ref": "$ref:m"}])
    unknown = [
        f for f in _findings(doc) if f.code == PROCESS_IR_SEMANTIC_LINEAGE_EFFECT_UNKNOWN
    ]
    assert unknown and unknown[0].severity == "warning"


def test_the_unknown_effect_finding_names_the_effect_class_not_the_component():
    doc = _doc([{"kind": "map_ref", "map_ref": "$ref:m"}])
    unknown = [
        f for f in _findings(doc) if f.code == PROCESS_IR_SEMANTIC_LINEAGE_EFFECT_UNKNOWN
    ]
    blob = unknown[0].model_dump_json()
    assert "$ref:m" not in blob
    assert [(e.key, e.value) for e in unknown[0].evidence] == [("effect_kind", "map")]


def test_a_custom_script_step_is_opaque():
    doc = _doc(
        [
            {
                "kind": "data_process",
                "steps": [
                    {
                        "operation": "custom_scripting",
                        "script": "SENTINEL_SCRIPT_BODY",
                        "language": "groovy2",
                        # the schema pins this to True
                        "use_cache": True,
                    }
                ],
            }
        ]
    )
    findings = _findings(doc)
    assert PROCESS_IR_SEMANTIC_LINEAGE_EFFECT_UNKNOWN in {f.code for f in findings}
    for item in findings:
        assert "SENTINEL_SCRIPT_BODY" not in item.model_dump_json()


# ---------------------------------------------------------------------------
# redaction + robustness
# ---------------------------------------------------------------------------


def test_no_lineage_finding_carries_a_property_name():
    doc = _doc([_read_prop("dpp", "SENTINEL_PROPERTY_NAME")])
    findings = _findings(doc)
    assert findings
    for item in findings:
        assert "SENTINEL_PROPERTY_NAME" not in item.model_dump_json()


def test_the_walk_terminates_on_the_control_flow_golden():
    _FIX = _ROOT / "tests" / "fixtures" / "process_ir" / "process_ir_v1.json"
    docs = json.loads(_FIX.read_text())
    prepared = prepare_validation_context(
        parse_process_ir_v1(docs["control_flow"]), SymbolTableV1(symbols=())
    )
    collect_lineage_findings(prepared)  # must return, not hang


def test_one_finding_per_code_per_node_even_on_a_diamond():
    """A node reachable by two paths must not produce the same finding twice."""
    doc = {
        "version": "1",
        "body": {
            "kind": "sequence",
            "steps": [
                {
                    "kind": "branch",
                    "legs": [
                        {
                            "steps": [_read_prop("dpp", "A")],
                            "terminal": {"kind": "stop"},
                        },
                        {
                            "steps": [_read_prop("dpp", "B")],
                            "terminal": {"kind": "stop"},
                        },
                    ],
                }
            ],
        },
    }
    findings = _findings(doc)
    seen = [(f.code, f.internal_node_id) for f in findings]
    assert len(seen) == len(set(seen))


# ---------------------------------------------------------------------------
# regressions found by validating the SHIPPED goldens
#
# Both of these were real defects in the collector, caught only because the
# control_flow golden was run through it. A rule proven solely against
# purpose-built fixtures agrees with whatever the fixture author assumed.
# ---------------------------------------------------------------------------


def test_a_decision_operand_scope_comes_from_its_property_id_prefix():
    """``dynamicdocument.X`` is a DDP, ``process.X`` is a DPP.

    A Decision operand carries a fully-qualified ``property_id`` instead of a
    bare name plus a scope field. Assuming DPP misclassifies every document
    property and yields confident, wrong diagnostics.
    """
    from boomi_mcp.compiler.process_ir.semantic_validation.lineage import (
        _tracked_property_key,
    )

    assert _tracked_property_key("dynamicdocument.DDP_S", None) == (DDP, "DDP_S")
    assert _tracked_property_key("process.PROP", None) == (DPP, "PROP")
    # unprefixed ids keep the historical DPP reading
    assert _tracked_property_key("BARE", None) == (DPP, "BARE")


def test_a_decision_operand_tolerates_a_property_nothing_writes():
    """Decision operands are NON-strict: they emit ``defaultValue=""`` on the
    wire, so an unwritten property is a defined empty string, not an error.

    The legacy walker encodes the same rule
    (``cache_property_lineage.LineageEvent.strict``), and the shipped
    ``control_flow`` golden depends on it — its router reads a DDP that nothing
    in the process writes.
    """
    _FIX = _ROOT / "tests" / "fixtures" / "process_ir" / "process_ir_v1.json"
    docs = json.loads(_FIX.read_text())
    prepared = prepare_validation_context(
        parse_process_ir_v1(docs["control_flow"]), SymbolTableV1(symbols=())
    )
    codes = {f.code for f in collect_lineage_findings(prepared)}
    assert PROCESS_IR_SEMANTIC_LINEAGE_PROPERTY_READ_BEFORE_WRITE not in codes


def test_a_non_strict_read_still_fails_when_the_writer_exists_but_is_invisible():
    """Non-strict tolerates ABSENCE, not a misplaced write. If the author wrote
    the property into a sibling leg, they plainly meant it to be read here."""
    # A decision is a TERMINAL control, never a leg step.
    doc = {
        "version": "1",
        "body": {
            "kind": "sequence",
            "steps": [
                {
                    "kind": "branch",
                    "legs": [
                        {
                            "steps": [_set_prop("ddp", "A")],
                            "terminal": {"kind": "stop"},
                        },
                        {
                            "steps": [{"kind": "message", "text": "x"}],
                            "terminal": {
                                "kind": "decision",
                                "comparison": "equals",
                                "left": {
                                    "value_type": "track",
                                    "property_id": "dynamicdocument.A",
                                },
                                "right": {
                                    "value_type": "static",
                                    "static_value": "x",
                                },
                                "true_arm": {
                                    "steps": [{"kind": "message", "text": "t"}],
                                    "terminal": {"kind": "stop"},
                                },
                                "false_arm": {
                                    "steps": [{"kind": "message", "text": "f"}],
                                    "terminal": {"kind": "stop"},
                                },
                            },
                        },
                    ],
                }
            ],
        },
    }
    assert PROCESS_IR_SEMANTIC_LINEAGE_DDP_SCOPE_INVALID in _codes(doc)


# ---------------------------------------------------------------------------
# F6 (repo Codex review): the leg-write index ignored trusted contract writes
# ---------------------------------------------------------------------------


def test_a_later_legs_trusted_contract_write_is_visible_to_branch_ordering():
    """`_leg_write_index` recorded only explicit node writes, so a reverse-leg
    dependency satisfied by a MAP CONTRACT in a later leg was silently
    downgraded to 'never written anywhere'."""
    from boomi_mcp.compiler.process_ir.semantic_validation import (
        MapEffectContractV1,
        ProcessIRValidationCapabilitiesV1,
        StateEffectV1,
    )
    from boomi_mcp.compiler.process_ir.semantic_validation.lineage import (
        _leg_write_index,
    )

    doc = _branch_doc(
        [
            [_read_prop("dpp", "A")],
            [{"kind": "map_ref", "map_ref": "$ref:m"}],
        ]
    )
    prepared = prepare_validation_context(
        parse_process_ir_v1(doc), SymbolTableV1(symbols=())
    )
    capabilities = ProcessIRValidationCapabilitiesV1(
        map_effects=(
            MapEffectContractV1(
                map_ref="$ref:m", effect=StateEffectV1(writes=(("dpp", "A"),))
            ),
        )
    )

    without = _leg_write_index(prepared)
    with_caps = _leg_write_index(prepared, capabilities)
    assert not any(("dpp", "A") in w for w in without.values())
    assert any(("dpp", "A") in w for w in with_caps.values())

    # and it changes the diagnostic: the read is now an ORDER defect, not
    # "nothing writes this anywhere"
    codes = {f.code for f in collect_lineage_findings(prepared, capabilities)}
    assert PROCESS_IR_SEMANTIC_LINEAGE_BRANCH_ORDER_INVALID in codes


# ---------------------------------------------------------------------------
# QA #199 / #200 (round 14): the declared-read loop added for F1 diverged from
# the authored one in two ways at once — it fed an unconstrained caller string
# into the closed evidence vocabulary, and it skipped every refinement.
# ---------------------------------------------------------------------------


def _caps_reading(pairs, map_ref="$ref:m"):
    from boomi_mcp.compiler.process_ir.semantic_validation import (
        MapEffectContractV1,
        ProcessIRValidationCapabilitiesV1,
        StateEffectV1,
    )

    return ProcessIRValidationCapabilitiesV1(
        map_effects=(
            MapEffectContractV1(
                map_ref=map_ref, effect=StateEffectV1(reads=tuple(pairs))
            ),
        )
    )


def _declared_codes(doc, pairs):
    prepared = prepare_validation_context(
        parse_process_ir_v1(doc), SymbolTableV1(symbols=())
    )
    return {f.code for f in collect_lineage_findings(prepared, _caps_reading(pairs))}


_MAP = {"kind": "map_ref", "map_ref": "$ref:m"}


def test_a_contract_scope_outside_the_closed_vocabulary_is_rejected_at_construction():
    """QA #199. The scope element reaches EVIDENCE, which accepts only a closed
    vocabulary — so an arbitrary caller string used to escape as a raw
    `pydantic.ValidationError` out of `validate_process_ir`, whose contract
    promises to raise only on a COMPILER defect. Rejecting it where the value
    enters closes that by construction rather than by catching it later."""
    from pydantic import ValidationError

    from boomi_mcp.compiler.process_ir.semantic_validation import StateEffectV1

    for bad in ("my-scope", "", "Ddp Scope", "DPP", "dpp_customer_email"):
        with pytest.raises(ValidationError):
            StateEffectV1(reads=((bad, "X"),))
        with pytest.raises(ValidationError):
            StateEffectV1(writes=((bad, "X"),))


def test_every_known_scope_is_still_accepted():
    """The discriminator: a validator that rejected everything would satisfy
    the case above and make the whole contract surface unusable."""
    from boomi_mcp.compiler.process_ir.semantic_validation import STATE_SCOPES, StateEffectV1

    for scope in sorted(STATE_SCOPES):
        assert StateEffectV1(reads=((scope, "X"),)).reads == ((scope, "X"),)
        assert StateEffectV1(writes=((scope, "X"),)).writes == ((scope, "X"),)


def test_the_contract_vocabulary_and_the_lattice_scopes_cannot_drift():
    """`STATE_SCOPES` gates construction; DDP/DPP/CACHE key the lattice. If the
    two ever disagree, a scope accepted at the boundary lands in a lattice slot
    nothing matches — established state that silently is not."""
    from boomi_mcp.compiler.process_ir.semantic_validation import STATE_SCOPES
    from boomi_mcp.compiler.process_ir.semantic_validation.lineage import (
        CACHE,
        DDP,
        DPP,
    )

    assert STATE_SCOPES == {DDP, DPP, CACHE}


def test_a_declared_read_satisfied_in_a_later_leg_is_an_ORDER_defect():
    """QA #200, case 1. The authored path calls this `…BRANCH_ORDER_INVALID`
    because the writer is right there, just later. A contract reader used to
    get the flat fallback for the identical graph."""
    doc = _branch_doc([[_MAP], [_set_prop("dpp", "A")]])
    codes = _declared_codes(doc, [("dpp", "A")])
    assert PROCESS_IR_SEMANTIC_LINEAGE_BRANCH_ORDER_INVALID in codes
    assert PROCESS_IR_SEMANTIC_LINEAGE_PROPERTY_READ_BEFORE_WRITE not in codes


def test_a_declared_ddp_read_written_on_another_copy_is_a_SCOPE_defect():
    """QA #200, case 2."""
    doc = _branch_doc([[_MAP], [_set_prop("ddp", "D")]])
    codes = _declared_codes(doc, [("ddp", "D")])
    assert PROCESS_IR_SEMANTIC_LINEAGE_DDP_SCOPE_INVALID in codes
    assert PROCESS_IR_SEMANTIC_LINEAGE_PROPERTY_READ_BEFORE_WRITE not in codes


def test_a_declared_cache_read_with_no_writer_names_the_CACHE():
    """QA #200, case 3."""
    codes = _declared_codes(_doc([_MAP]), [("cache", "$ref:C")])
    assert PROCESS_IR_SEMANTIC_LINEAGE_CACHE_WRITER_MISSING in codes
    assert PROCESS_IR_SEMANTIC_LINEAGE_PROPERTY_READ_BEFORE_WRITE not in codes


def test_a_declared_read_still_falls_back_when_no_refinement_applies():
    """The discriminator for #200: refinement must be selective. A dpp read
    with no writer anywhere has no sharper story than 'read before write'."""
    codes = _declared_codes(_doc([_MAP]), [("dpp", "A")])
    assert codes == {PROCESS_IR_SEMANTIC_LINEAGE_PROPERTY_READ_BEFORE_WRITE}


def test_a_satisfied_declared_read_reports_nothing():
    """The strongest discriminator: the refinements must not turn a CLEAN
    contract into a finding."""
    doc = _doc([_set_prop("dpp", "A"), _MAP])
    assert _declared_codes(doc, [("dpp", "A")]) == set()


def test_the_declared_read_marker_survives_refinement():
    """`effect_kind: declared_read` is what tells an author the dependency came
    from a contract rather than from a step they can see. Routing through the
    shared classifier must not drop it."""
    doc = _branch_doc([[_MAP], [_set_prop("dpp", "A")]])
    prepared = prepare_validation_context(
        parse_process_ir_v1(doc), SymbolTableV1(symbols=())
    )
    findings = collect_lineage_findings(prepared, _caps_reading([("dpp", "A")]))
    ordered = [
        f
        for f in findings
        if f.code == PROCESS_IR_SEMANTIC_LINEAGE_BRANCH_ORDER_INVALID
    ]
    assert ordered
    values = {(e.key, e.value) for e in ordered[0].evidence}
    assert ("effect_kind", "declared_read") in values


def test_an_authored_and_a_declared_read_of_the_same_graph_agree():
    """The property #200 is really about: one condition, one code, regardless of
    who declared the read."""
    authored = _codes(_branch_doc([[_read_prop("dpp", "A")], [_set_prop("dpp", "A")]]))
    declared = _declared_codes(_branch_doc([[_MAP], [_set_prop("dpp", "A")]]), [("dpp", "A")])
    assert authored == declared == {PROCESS_IR_SEMANTIC_LINEAGE_BRANCH_ORDER_INVALID}


def test_no_node_can_produce_both_an_authored_and_a_declared_read():
    """`_report` dedups on `(code, node_id)` and ignores evidence, so if one
    node could raise the same code down both read paths, the first would win
    and the `declared_read` marker would silently vanish.

    It cannot today: the two sources cover disjoint semantic kinds. That is an
    INVARIANT, not a coincidence — extending `_trusted_effects` to a kind
    `_reads_of` already handles would reintroduce the collapse with no test
    failing anywhere else. Pinned here so that change has to be deliberate.
    """
    import inspect

    from boomi_mcp.compiler.process_ir.semantic_validation import lineage

    def _kinds(fn):
        found = set()
        for line in inspect.getsource(fn).splitlines():
            for token in re.findall(r'kind\s*(?:==|in)\s*(\(?[^:]+)', line):
                found.update(re.findall(r'"([a-z_]+)"', token))
        return found

    authored = _kinds(lineage._reads_of)
    trusted = _kinds(lineage._trusted_effects)
    assert authored, "the authored-read kinds could not be recovered"
    assert trusted, "the trusted-effect kinds could not be recovered"
    assert authored.isdisjoint(trusted), sorted(authored & trusted)
