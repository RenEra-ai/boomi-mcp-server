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
    PROCESS_IR_SEMANTIC_DYNAMIC_PATH_DDP_NOT_ESTABLISHED,
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


def test_a_declared_external_writer_alone_no_longer_downgrades():
    """#143 excludes "relaxing rules via free-form 'trust me' flags" and requires
    that no free-form assertion suppress a fatal safety rule. The authored
    `external_writer` boolean did exactly that: it turned a blocking
    `…CACHE_WRITER_MISSING` into a non-blocking warning, so a payload could
    unblock its own build."""
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
    assert PROCESS_IR_SEMANTIC_LINEAGE_CACHE_WRITER_MISSING in _codes(doc)


def test_a_typed_external_writer_contract_does_downgrade():
    """The flag DECLARES the expectation; the contract CONFIRMS it. Capabilities
    are compiler context no authored document can reach, which is what makes
    this a trust boundary rather than a self-assertion."""
    from boomi_mcp.compiler.process_ir.semantic_validation import (
        ExternalWriterContractV1,
        ProcessIRValidationCapabilitiesV1,
    )

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
    prepared = prepare_validation_context(
        parse_process_ir_v1(doc), SymbolTableV1(symbols=())
    )
    capabilities = ProcessIRValidationCapabilitiesV1(
        external_writers=(ExternalWriterContractV1(cache_ref="$ref:c"),)
    )
    findings = collect_lineage_findings(prepared, capabilities)
    codes = {f.code for f in findings}
    assert PROCESS_IR_SEMANTIC_LINEAGE_EXTERNAL_WRITER_ASSUMED in codes
    assert PROCESS_IR_SEMANTIC_LINEAGE_CACHE_WRITER_MISSING not in codes


def test_a_contract_for_a_DIFFERENT_cache_does_not_downgrade():
    """The discriminator: the contract is bound to a cache, not to the idea of
    external writers."""
    from boomi_mcp.compiler.process_ir.semantic_validation import (
        ExternalWriterContractV1,
        ProcessIRValidationCapabilitiesV1,
    )

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
    prepared = prepare_validation_context(
        parse_process_ir_v1(doc), SymbolTableV1(symbols=())
    )
    capabilities = ProcessIRValidationCapabilitiesV1(
        external_writers=(ExternalWriterContractV1(cache_ref="$ref:other"),)
    )
    codes = {f.code for f in collect_lineage_findings(prepared, capabilities)}
    assert PROCESS_IR_SEMANTIC_LINEAGE_CACHE_WRITER_MISSING in codes


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


# ---------------------------------------------------------------------------
# QA #205: "a non-waiting subprocess establishes no downstream state" first
# shipped inside the main traversal ONLY. `_leg_write_index` and
# `_written_anywhere` kept counting the async write, and because both decide
# WHICH code a finding gets — and `_written_anywhere` gates the non-strict-read
# skip — attaching a summary to an unrelated `wait=False` child turned a VALID
# payload into a rejected one.
# ---------------------------------------------------------------------------


def _async_decision_doc(wait):
    """A non-strict Decision `track` operand in leg 0, a subprocess in leg 1."""
    decision = {
        "kind": "decision", "comparison": "equals",
        "left": {"value_type": "track", "property_id": "process.P"},
        "right": {"value_type": "static", "static_value": "x"},
        "true_arm": {"steps": [{"kind": "message", "text": "t"}],
                     "terminal": {"kind": "stop"}},
        "false_arm": {"steps": [{"kind": "message", "text": "f"}],
                      "terminal": {"kind": "stop"}},
    }
    return {"version": "1", "body": {"kind": "sequence", "steps": [
        {"kind": "branch", "legs": [
            {"steps": [{"kind": "message", "text": "m"}], "terminal": decision},
            {"steps": [], "terminal": {"kind": "process_call", "process_ref": "$ref:a",
                                                  "wait": wait, "abort_on_error": False}}]}]}}


def _async_case(wait, with_contract):
    from boomi_mcp.compiler.process_ir.contracts import ComponentSymbolV1
    from boomi_mcp.compiler.process_ir.semantic_validation import (
        ProcessIRValidationCapabilitiesV1,
        SubprocessSummaryV1,
        validate_process_ir,
    )
    from boomi_mcp.compiler.process_ir.semantic_validation import (
        StateEffectV1 as _Effect,
    )

    symbols = SymbolTableV1(symbols=(
        ComponentSymbolV1(ref="$ref:a", component_id="i1",
                          component_type="process"),))
    caps = ProcessIRValidationCapabilitiesV1(
        subprocess_summaries=(SubprocessSummaryV1(
            process_ref="$ref:a",
            effect=_Effect(writes=(("dpp", "P"),), replay_safe=True)),)
    ) if with_contract else ProcessIRValidationCapabilitiesV1()
    return validate_process_ir(
        parse_process_ir_v1(_async_decision_doc(wait)), symbols, caps
    )


def test_an_async_summary_cannot_turn_a_valid_payload_into_a_rejected_one():
    """A `wait=False` child establishes nothing, so attaching its summary must
    not change any verdict. A non-strict `track` operand is clean when nothing
    writes it — and stayed clean without the contract, which is what made the
    contract-induced rejection a pure false positive."""
    assert _async_case(False, with_contract=False).is_valid is True
    assert _async_case(False, with_contract=True).is_valid is True


def test_a_waiting_childs_write_is_still_analysed():
    """The discriminator. The exclusion must be about `wait`, not about
    ignoring subprocess summaries: a WAITING child's write genuinely lands, and
    here it lands in a LATER leg, so the read is a real ordering defect."""
    report = _async_case(True, with_contract=True)
    assert report.is_valid is False
    assert PROCESS_IR_SEMANTIC_LINEAGE_BRANCH_ORDER_INVALID in {
        f.code for f in report.errors
    }


def test_the_three_lattice_consumers_ask_three_different_questions():
    """The corrected shape, pinned because I got it wrong in BOTH directions.

    First the async-write exclusion was applied to the traversal only, and
    `_written_anywhere` / `_leg_write_index` kept counting the async write —
    which rejected a valid payload. Then it was applied to all three, which
    silently ACCEPTED a cross-copy DDP read (a false negative) and downgraded a
    later-leg ordering defect. The sites ask different questions:

    * `_visit`            — does this write ESTABLISH state downstream? async: no
    * `_written_anywhere` — did anyone write this key AT ALL?          async: yes
    * `_leg_write_index`  — WHERE is the write?                        async: yes
    """
    import inspect

    from boomi_mcp.compiler.process_ir.semantic_validation import lineage

    # the two INDEXES must count async writes — they ask existence/position
    for fn in (lineage._leg_write_index, lineage._written_anywhere):
        assert "effect.writes" in inspect.getsource(fn), fn.__name__
        assert "_establishes_downstream(" not in inspect.getsource(fn), fn.__name__

    # and the establishment question must consult the predicate
    assert "_establishes_downstream(" in inspect.getsource(lineage._established_anywhere)


def test_a_nonstrict_ddp_read_still_fails_on_a_cross_copy_async_write():
    """The false NEGATIVE the over-broad fix introduced. A DDP write on another
    document copy can NEVER reach this reader — that is not absence, and a
    non-strict reader only tolerates absence."""
    from boomi_mcp.compiler.process_ir.contracts import ComponentSymbolV1
    from boomi_mcp.compiler.process_ir.semantic_validation import (
        ProcessIRValidationCapabilitiesV1,
        StateEffectV1 as _Effect,
        SubprocessSummaryV1,
        validate_process_ir,
    )

    decision = {
        "kind": "decision", "comparison": "equals",
        "left": {"value_type": "track", "property_id": "dynamicdocument.P"},
        "right": {"value_type": "static", "static_value": "x"},
        "true_arm": {"steps": [{"kind": "message", "text": "t"}],
                     "terminal": {"kind": "stop"}},
        "false_arm": {"steps": [{"kind": "message", "text": "f"}],
                      "terminal": {"kind": "stop"}},
    }
    doc = {"version": "1", "body": {"kind": "sequence", "steps": [
        {"kind": "branch", "legs": [
            {"steps": [{"kind": "message", "text": "m"}], "terminal": decision},
            {"steps": [], "terminal": {"kind": "process_call", "process_ref": "$ref:a",
                                                  "wait": False, "abort_on_error": False}}]}]}}
    symbols = SymbolTableV1(symbols=(
        ComponentSymbolV1(ref="$ref:a", component_id="i1",
                          component_type="process"),))
    caps = ProcessIRValidationCapabilitiesV1(subprocess_summaries=(
        SubprocessSummaryV1(process_ref="$ref:a",
                            effect=_Effect(writes=(("ddp", "P"),),
                                           replay_safe=True)),))
    codes = {f.code for f in validate_process_ir(
        parse_process_ir_v1(doc), symbols, caps).errors}
    assert PROCESS_IR_SEMANTIC_LINEAGE_DDP_SCOPE_INVALID in codes


def test_a_later_leg_async_write_keeps_the_precise_ordering_code():
    """The downgrade the over-broad fix introduced: `wait` does not move a
    write, so a later-leg write is still a branch-ORDER defect, not a generic
    missing-write one."""
    from boomi_mcp.compiler.process_ir.contracts import ComponentSymbolV1
    from boomi_mcp.compiler.process_ir.semantic_validation import (
        ProcessIRValidationCapabilitiesV1,
        StateEffectV1 as _Effect,
        SubprocessSummaryV1,
        validate_process_ir,
    )

    doc = {"version": "1", "body": {"kind": "sequence", "steps": [
        {"kind": "branch", "legs": [
            {"steps": [_read_prop("dpp", "A")], "terminal": {"kind": "stop"}},
            {"steps": [], "terminal": {"kind": "process_call", "process_ref": "$ref:a",
                                                  "wait": False, "abort_on_error": False}}]}]}}
    symbols = SymbolTableV1(symbols=(
        ComponentSymbolV1(ref="$ref:a", component_id="i1",
                          component_type="process"),))
    caps = ProcessIRValidationCapabilitiesV1(subprocess_summaries=(
        SubprocessSummaryV1(process_ref="$ref:a",
                            effect=_Effect(writes=(("dpp", "A"),),
                                           replay_safe=True)),))
    codes = {f.code for f in validate_process_ir(
        parse_process_ir_v1(doc), symbols, caps).errors}
    assert PROCESS_IR_SEMANTIC_LINEAGE_BRANCH_ORDER_INVALID in codes


# ---------------------------------------------------------------------------
# #155 M12.17 — the per-document request path and the writer that composes it
# ---------------------------------------------------------------------------


def _dynpath_symbols():
    from boomi_mcp.compiler.process_ir.contracts import ComponentSymbolV1

    rest = "officialboomi-X3979C-rest-prod"
    return SymbolTableV1(
        symbols=[
            ComponentSymbolV1(ref="$ref:CONN", component_id="C",
                              component_type="connector-settings", connector_type=rest),
            ComponentSymbolV1(ref="$ref:GETOP", component_id="O",
                              component_type="connector-action", connector_type=rest,
                              action_type="GET", connection_ref="$ref:CONN"),
            ComponentSymbolV1(ref="$ref:PROF", component_id="P",
                              component_type="profile.json"),
            ComponentSymbolV1(ref="$ref:CACHE", component_id="K",
                              component_type="documentcache"),
        ]
    )


_STATIC = {"value_type": "static", "value": "/admin/cdscm/api/v1/clients/"}
_DPPSEG = {"value_type": "dpp", "property_name": "key", "default_value": ""}
_WRITER = {"kind": "set_ddp", "name": "P", "source_values": [_STATIC, _DPPSEG]}
_BOUND = {"kind": "connector_call", "operation_ref": "$ref:GETOP",
          "path_binding": {"property_name": "P"}}


def _dynpath_codes(steps):
    """Compile a document and return the diagnostic codes it is refused with."""
    from boomi_mcp.compiler.process_ir.diagnostics import ProcessIRCompileError
    from boomi_mcp.compiler.process_ir.pipeline import compile_process_ir_v1

    doc = {"version": "1", "body": {"kind": "sequence", "steps": steps}}
    try:
        compile_process_ir_v1(parse_process_ir_v1(doc), _dynpath_symbols())
    except ProcessIRCompileError as exc:
        # `diagnostics` is the accessor; `codes` exists only in the repr, and
        # reading it returns None — a helper that did so would report every
        # refusal as an empty tuple and quietly pass a test that never ran.
        return tuple(item.code for item in exc.diagnostics)
    return ()


def test_the_stream_replacing_authority_matches_the_served_contract():
    """ONE authority, pinned to the published answer in BOTH directions.

    The dynamic-path rule reads `DOCUMENT_STREAM_REPLACING_KINDS` and nothing
    else. The served authoring contract publishes the same fact per node as
    `document_semantics.output_documents == "stream_replacing"`. If the two ever
    disagree — a kind added to one and not the other — the compiler would refuse
    (or admit) a binding the contract says the opposite about, which is precisely
    the drift a hand-written second copy produces.

    This test exists because the first version of the rule DID hold a second copy:
    it qualified `data_process` to split/combine and gated the cache reads on a
    `load_all_documents` field that `CacheGetSemanticV1` does not have, so the
    canonical cache read fell through fail-open. Two wrong entries in one 37-line
    change is what a derived-and-pinned authority prevents.
    """
    from boomi_mcp.authoring.process_ir_projection import (
        process_ir_authoring_revision_payload,
    )
    from boomi_mcp.compiler.process_ir.semantic_validation.lineage import (
        DOCUMENT_STREAM_REPLACING_KINDS,
    )

    served = {
        entry["subject"]
        for entry in process_ir_authoring_revision_payload()["entries"]
        if entry.get("entry_type") == "node"
        and (entry.get("document_semantics") or {}).get("output_documents")
        == "stream_replacing"
    }
    assert served, "the served contract published no stream-replacing node — the pin would be vacuous"
    assert DOCUMENT_STREAM_REPLACING_KINDS == served, {
        "only_in_compiler": sorted(DOCUMENT_STREAM_REPLACING_KINDS - served),
        "only_in_served_contract": sorted(served - DOCUMENT_STREAM_REPLACING_KINDS),
    }


#: One authored instance of each stream-replacing kind. Shared by every test
#: that needs a replacement, so a kind added to one is not missed by the other.
_REPLACING_KINDS = [
    pytest.param({"kind": "message", "text": "hello"}, id="message"),
    pytest.param(
        {"kind": "data_process", "steps": [
            {"operation": "split_documents", "profile_type": "json",
             "profile_ref": "$ref:PROF", "link_element_key": "1",
             "link_element_name": "root"}]},
        id="data_process_split",
    ),
    pytest.param(
        {"kind": "data_process", "steps": [
            {"operation": "custom_scripting", "language": "groovy2",
             "script": "// emits its own documents"}]},
        id="data_process_script",
    ),
    pytest.param({"kind": "cache_get", "cache_ref": "$ref:CACHE"}, id="cache_get"),
]


@pytest.mark.parametrize("replacing", _REPLACING_KINDS)
def test_a_document_replacement_between_the_writer_and_the_call_is_refused(replacing):
    """Every replacing kind, not just the two the first version happened to name.

    `cache_get` and `message` are the regressions this parametrisation exists for:
    both were accepted by the first rule, and both hand a bound call documents that
    never carried the property composing its request path — so the request would
    have addressed the wrong resource rather than failed.
    """
    codes = _dynpath_codes([_WRITER, replacing, _BOUND, {"kind": "stop"}])
    assert PROCESS_IR_SEMANTIC_DYNAMIC_PATH_DDP_NOT_ESTABLISHED in codes, codes


def test_the_replacement_rule_does_not_over_fire():
    """The three controls that make the test above mean something.

    A rule that refused everything would pass the parametrisation and be useless.
    """
    # No replacement at all: the ordinary valid form.
    assert _dynpath_codes([_WRITER, _BOUND, {"kind": "stop"}]) == ()
    # Writing AFTER the replacement is legitimate — the new documents carry it.
    assert _dynpath_codes(
        [{"kind": "message", "text": "hello"}, _WRITER, _BOUND, {"kind": "stop"}]
    ) == ()
    # A replacement with no binding anywhere is simply not this rule's business.
    assert _dynpath_codes(
        [_WRITER, {"kind": "message", "text": "hello"},
         {"kind": "connector_call", "operation_ref": "$ref:GETOP"}, {"kind": "stop"}]
    ) == ()


# ---------------------------------------------------------------------------
# #155 — an OPTIONAL component reference is still a component reference
# ---------------------------------------------------------------------------


def test_an_optional_component_ref_field_is_recognised():
    """The predicate must look one level into an Optional, in both directions.

    pydantic does not lift a `ComponentRefV1`'s metadata onto
    `Optional[ComponentRefV1]`: the field's own `metadata` is empty and the
    validator sits on the non-None arm. Reading only the outer metadata answered
    False for every optional reference — and #155 shipped
    `path_binding.request_profile_ref` as exactly that shape, so the served
    reference list, the relocatability gate, symbol-slot derivation and the
    dependency preflight all skipped it while reporting success.

    The negative half matters as much: the unwrap must find THIS validator, not
    any `AfterValidator`, or every annotated string in the model becomes a
    "reference".
    """
    from boomi_mcp.models.process_ir import (
        ConnectorCallNodeV1,
        ConnectorPathBindingV1,
        SourceEndpointV1,
        _is_component_ref_field,
    )

    # Optional ref — the case that was silently missed.
    assert _is_component_ref_field(
        ConnectorPathBindingV1.model_fields["request_profile_ref"]
    )
    # Plain refs still work.
    assert _is_component_ref_field(SourceEndpointV1.model_fields["connection_ref"])
    assert _is_component_ref_field(ConnectorCallNodeV1.model_fields["operation_ref"])
    # Not references: a constrained string beside a ref, and a plain optional.
    assert not _is_component_ref_field(
        ConnectorPathBindingV1.model_fields["property_name"]
    )
    assert not _is_component_ref_field(ConnectorCallNodeV1.model_fields["action"])


def test_a_bound_paths_profile_ref_is_enumerated_as_a_component_reference():
    """The consumer-visible half: the walker must YIELD the optional ref.

    Asserting the predicate alone would not prove this — `iter_component_refs`
    could still drop the value on its own path. The literal-id case is the one
    with teeth: `envelope_relocatability_offenders` and `derive_symbol_slots`
    both read this walker, so a ref it does not yield is a component id that
    reaches a plan claiming to be relocatable.
    """
    from boomi_mcp.models.process_ir import iter_component_refs, parse_process_ir_v1

    profile_source = {
        "value_type": "profile", "element_id": "3", "element_name": "clientId",
        "profile_ref": "$ref:PROF", "profile_type": "profile.json",
    }
    ir = parse_process_ir_v1({"version": "1", "body": {"kind": "sequence", "steps": [
        {"kind": "set_ddp", "name": "P", "source_values": [
            {"value_type": "static", "value": "/c/"}, profile_source]},
        {"kind": "connector_call", "operation_ref": "$ref:GETOP",
         "path_binding": {"property_name": "P", "request_profile_ref": "$ref:PROF"}},
        {"kind": "stop"},
    ]}})
    paths = {path for path, _value in iter_component_refs(ir)}
    assert "/body/steps/1/path_binding/request_profile_ref" in paths, sorted(paths)

    # And a LITERAL component id there is now visible to the relocatability gate.
    literal = "1a2b3c4d-0000-0000-0000-000000000000"
    ir_literal = parse_process_ir_v1({"version": "1", "body": {"kind": "sequence", "steps": [
        {"kind": "set_ddp", "name": "P", "source_values": [
            {"value_type": "static", "value": "/c/"},
            {**profile_source, "profile_ref": literal}]},
        {"kind": "connector_call", "operation_ref": "$ref:GETOP",
         "path_binding": {"property_name": "P", "request_profile_ref": literal}},
        {"kind": "stop"},
    ]}})
    literals = {
        path for path, value in iter_component_refs(ir_literal)
        if not value.startswith("$ref:")
    }
    assert "/body/steps/1/path_binding/request_profile_ref" in literals, sorted(literals)


# ---------------------------------------------------------------------------
# Stage-2 review round 1 (#155): the bound path's key must be the RUNTIME key
# ---------------------------------------------------------------------------


_CURRENT = {"value_type": "current"}


def test_a_bound_path_composed_from_an_unestablished_current_value_is_refused():
    """CDX round 1 P1. A `current` source reads the property's own value.

    The served model says so outright — "Because it READS the property, lineage
    validation still requires an earlier write on the same path to establish
    it" — but nothing recorded that read, while the rule that demands a dynamic
    segment counted `current` as one. So a writer whose only dynamic segment was
    `current`, with nothing having written the property, satisfied the rule and
    emitted a Path bound to an empty value: the request addresses the wrong
    resource, and does it silently.
    """
    writer = {"kind": "set_ddp", "name": "P", "source_values": [_STATIC, _CURRENT]}
    codes = _dynpath_codes([writer, _BOUND, {"kind": "stop"}])
    assert PROCESS_IR_SEMANTIC_DYNAMIC_PATH_DDP_NOT_ESTABLISHED in codes, codes


def test_a_bound_path_may_append_to_a_value_an_earlier_step_established():
    """The over-fire control, and the shape `current` exists for.

    Appending to what an earlier write established is the whole point of a
    `current` source. Refusing it would break the composition pattern rather
    than the defect.
    """
    established = {"kind": "set_ddp", "name": "P", "source_values": [_STATIC]}
    appender = {"kind": "set_ddp", "name": "P", "source_values": [_CURRENT, _DPPSEG]}
    assert _dynpath_codes([established, appender, _BOUND, {"kind": "stop"}]) == ()


def test_the_current_read_is_recognised_only_for_a_BOUND_path():
    """The scope of the fix, pinned — and it is deliberately narrow.

    The reviewer proposed recording this read in the general lineage model. That
    would have been wrong for this repository: the LEGACY chain accepts a
    `current` composition with no earlier write, and the shipped parity golden
    freezes exactly that shape, so the general model would have diverged from
    the oracle this compiler is measured against. An ordinary property composed
    from an unset `current` is an empty string, which the platform runs; the
    same value as a request PATH addresses the wrong resource. The rule
    therefore lives where that distinction is decided.
    """
    unbound_call = {"kind": "connector_call", "operation_ref": "$ref:GETOP"}
    writer = {"kind": "set_ddp", "name": "P", "source_values": [_STATIC, _CURRENT]}
    assert _dynpath_codes([writer, unbound_call, {"kind": "stop"}]) == ()


@pytest.mark.parametrize(
    "writer_name, binding_name",
    [(" P ", "P"), ("P", " P "), (" P ", " P "), ("P", "P")],
)
def test_surrounding_whitespace_does_not_change_which_property_is_named(
    writer_name, binding_name
):
    """CDX round 1 P2. The name validator accepts padding; the runtime ignores it.

    The property writer's name is stripped when it is lowered, and the emitter
    strips the binding's name again before writing the wire attribute — so
    `" P "` and `"P"` are one property in every sense that reaches the platform.
    The binding snapshot was the only place that kept the padding, which made
    one of these four pairings refuse and its mirror compile, for a document
    that emits identically either way.
    """
    writer = {"kind": "set_ddp", "name": writer_name, "source_values": [_STATIC, _DPPSEG]}
    bound = {"kind": "connector_call", "operation_ref": "$ref:GETOP",
             "path_binding": {"property_name": binding_name}}
    assert _dynpath_codes([writer, bound, {"kind": "stop"}]) == ()


def test_a_binding_naming_a_genuinely_different_property_still_refuses():
    """The over-fire control for the normalization above."""
    bound = {"kind": "connector_call", "operation_ref": "$ref:GETOP",
             "path_binding": {"property_name": "SOMETHING_ELSE"}}
    codes = _dynpath_codes([_WRITER, bound, {"kind": "stop"}])
    assert PROCESS_IR_SEMANTIC_DYNAMIC_PATH_DDP_NOT_ESTABLISHED in codes, codes


def _dynpath_symbols_with_profile_alias():
    """The dynamic-path symbols plus a SECOND ref naming the same profile.

    Two authored keys resolving to one component is an ordinary outcome of a
    plan that reuses an existing profile under more than one key.
    """
    from boomi_mcp.compiler.process_ir.contracts import ComponentSymbolV1

    base = _dynpath_symbols()
    return SymbolTableV1(
        symbols=list(base.symbols)
        + [
            ComponentSymbolV1(ref="$ref:PROF_ALIAS", component_id="P",
                              component_type="profile.json"),
            ComponentSymbolV1(ref="$ref:PROF_OTHER", component_id="P2",
                              component_type="profile.json"),
        ],
        idempotency_contracts=base.idempotency_contracts,
    )


def _dynpath_codes_aliased(steps):
    from boomi_mcp.compiler.process_ir.diagnostics import ProcessIRCompileError
    from boomi_mcp.compiler.process_ir.pipeline import compile_process_ir_v1

    doc = {"version": "1", "body": {"kind": "sequence", "steps": steps}}
    try:
        compile_process_ir_v1(parse_process_ir_v1(doc), _dynpath_symbols_with_profile_alias())
    except ProcessIRCompileError as exc:
        return tuple(item.code for item in exc.diagnostics)
    return ()


def _profile_source(ref):
    return {"value_type": "profile", "profile_ref": ref, "profile_type": "json",
            "element_id": "3", "element_name": "clientId (Root/Object/clientId)"}


@pytest.mark.parametrize(
    "writer_ref, binding_ref",
    [("$ref:PROF", "$ref:PROF_ALIAS"), ("$ref:PROF_ALIAS", "$ref:PROF")],
)
def test_two_refs_naming_one_profile_component_agree(writer_ref, binding_ref):
    """CDX round 1 P2. The emitter writes the RESOLVED id, so that is the identity.

    Comparing the authored tokens reported a mismatch for a pair that emits a
    byte-identical `parameter-profile` attribute — the validator's key was
    weaker than the runtime's, which is the same defect as the whitespace one in
    its other half.
    """
    writer = {"kind": "set_ddp", "name": "P",
              "source_values": [_STATIC, _profile_source(writer_ref)]}
    bound = {"kind": "connector_call", "operation_ref": "$ref:GETOP",
             "path_binding": {"property_name": "P", "request_profile_ref": binding_ref}}
    assert _dynpath_codes_aliased([writer, bound, {"kind": "stop"}]) == ()


def test_several_sources_aliased_to_one_profile_are_one_profile():
    """The `len(pairs) > 1` half: aliases are not several profiles."""
    writer = {"kind": "set_ddp", "name": "P", "source_values": [
        _STATIC, _profile_source("$ref:PROF"), _profile_source("$ref:PROF_ALIAS")]}
    bound = {"kind": "connector_call", "operation_ref": "$ref:GETOP",
             "path_binding": {"property_name": "P", "request_profile_ref": "$ref:PROF"}}
    assert _dynpath_codes_aliased([writer, bound, {"kind": "stop"}]) == ()


@pytest.mark.parametrize(
    "writer_ref, binding_ref, label",
    [
        ("$ref:PROF", "$ref:PROF_OTHER", "binding names a different component"),
        ("$ref:PROF_OTHER", "$ref:PROF", "writer names a different component"),
    ],
)
def test_two_refs_naming_DIFFERENT_profile_components_still_mismatch(
    writer_ref, binding_ref, label
):
    """The over-fire control: resolving identity must not collapse real differences."""
    writer = {"kind": "set_ddp", "name": "P",
              "source_values": [_STATIC, _profile_source(writer_ref)]}
    bound = {"kind": "connector_call", "operation_ref": "$ref:GETOP",
             "path_binding": {"property_name": "P", "request_profile_ref": binding_ref}}
    from boomi_mcp.errors import (
        PROCESS_IR_SEMANTIC_DYNAMIC_PATH_PROFILE_BINDING_MISMATCH,
    )

    codes = _dynpath_codes_aliased([writer, bound, {"kind": "stop"}])
    assert PROCESS_IR_SEMANTIC_DYNAMIC_PATH_PROFILE_BINDING_MISMATCH in codes, (label, codes)


def test_the_served_current_description_states_the_rule_at_the_scope_it_holds():
    """QA-155-r13-02. Served text is machine-facing API and must be true.

    The description said an earlier write is "still required" without
    qualification. Before this slice that was false everywhere — nothing
    enforced it. After the bound-path rule it is true at a path binding and
    false elsewhere, deliberately: an unestablished `current` composes the empty
    string, which the platform runs and the legacy chain emits, so requiring a
    writer generally would refuse documents that execute correctly.

    Pinned against the SERVED schema rather than the source docstring, because
    the served copy is what a caller reads.
    """
    import json

    from boomi_mcp.models.process_ir import canonical_process_ir_schema_json

    served = json.loads(canonical_process_ir_schema_json())
    text = " ".join(
        served["$defs"]["CurrentPropertySourceV1"]["description"].lower().split()
    )
    # It must SCOPE the requirement...
    assert "request path" in text
    assert "path binding" in text
    # ...and say what holds elsewhere, so a caller is not left inferring it.
    assert "empty string" in text
    # ...and it must no longer assert the requirement without qualification.
    assert "lineage validation still requires an earlier write" not in text


@pytest.mark.parametrize("replacing", _REPLACING_KINDS)
def test_a_current_value_does_not_survive_a_document_replacement(replacing):
    """CDX round 2 P1. The `current` check must ask which documents carry the value.

    A property written before a stream-replacing node, then APPENDED to after it
    with a `current` source, was accepted: the state model reports the key as
    established — deliberately, since #154's model answers "was this written
    anywhere on this path" — but the documents reaching the writer are new ones
    that carry none of it. The composed path is then the appended fragment
    alone, and the request addresses a different resource.

    The check is asked of the reaching-writer map instead, which IS cleared of
    document-scoped keys at a replacement, so it answers the question the rule
    needs: does a writer exist whose value THESE documents still carry. Reusing
    that clearing is also why the fix needs no second provenance model.
    """
    writer = {"kind": "set_ddp", "name": "P", "source_values": [_STATIC]}
    appender = {"kind": "set_ddp", "name": "P", "source_values": [_STATIC, _CURRENT]}
    codes = _dynpath_codes([writer, replacing, appender, _BOUND, {"kind": "stop"}])
    assert PROCESS_IR_SEMANTIC_DYNAMIC_PATH_DDP_NOT_ESTABLISHED in codes, codes


@pytest.mark.parametrize("replacing", _REPLACING_KINDS)
def test_a_replacement_does_not_refuse_a_writer_that_needs_no_earlier_value(replacing):
    """The over-fire control: only a `current` source depends on what came before.

    A writer that composes entirely from sources of its own is unaffected by a
    replacement, so re-writing the property after one must still satisfy the
    bound-path rule — the rule is about a value that has to survive, not about
    replacement itself.

    Asserted as the ABSENCE of this rule's code rather than as an empty result,
    because one of these kinds is refused here for an unrelated reason: a cache
    read with no cache writer. Demanding a clean compile would have made this
    control fail for something it does not test, and weakening it to a
    try/except would have hidden a real firing.
    """
    writer = {"kind": "set_ddp", "name": "P", "source_values": [_STATIC]}
    rewriter = {"kind": "set_ddp", "name": "P", "source_values": [_STATIC, _DPPSEG]}
    codes = _dynpath_codes([writer, replacing, rewriter, _BOUND, {"kind": "stop"}])
    assert PROCESS_IR_SEMANTIC_DYNAMIC_PATH_DDP_NOT_ESTABLISHED not in codes, codes


def test_appending_to_a_value_with_no_replacement_between_still_compiles():
    """The other control: without a replacement the value is still there."""
    writer = {"kind": "set_ddp", "name": "P", "source_values": [_STATIC]}
    appender = {"kind": "set_ddp", "name": "P", "source_values": [_STATIC, _CURRENT]}
    assert _dynpath_codes([writer, appender, _BOUND, {"kind": "stop"}]) == ()


def _script_step(source: str):
    return {"kind": "data_process", "steps": [
        {"operation": "custom_scripting", "language": "groovy2", "script": source}]}


def _declaring(source: str, *keys):
    """Capabilities in which a script CONTRACTS to write `keys`."""
    import hashlib

    from boomi_mcp.compiler.process_ir.semantic_validation.contracts import (
        ProcessIRValidationCapabilitiesV1,
        ScriptEffectContractV1,
        StateEffectV1,
    )

    return ProcessIRValidationCapabilitiesV1(
        script_effects=(
            ScriptEffectContractV1(
                language="groovy2",
                source_sha256=hashlib.sha256(source.encode()).hexdigest(),
                effect=StateEffectV1(writes=tuple(keys)),
            ),
        )
    )


def _codes_with(steps, capabilities=None):
    from boomi_mcp.compiler.process_ir.diagnostics import ProcessIRCompileError
    from boomi_mcp.compiler.process_ir.pipeline import compile_process_ir_v1

    doc = {"version": "1", "body": {"kind": "sequence", "steps": steps}}
    kwargs = {"capabilities": capabilities} if capabilities is not None else {}
    try:
        compile_process_ir_v1(parse_process_ir_v1(doc), _dynpath_symbols(), **kwargs)
    except ProcessIRCompileError as exc:
        return tuple(item.code for item in exc.diagnostics)
    return ()


def test_a_contract_declared_write_establishes_a_value_current_may_append_to():
    """QA-155-r15-01. Three channels establish a key; the rule must see all three.

    The first version of this rule asked the state model, which is too BROAD —
    it survives a document replacement. The second asked the reaching-writer
    map, which is too NARROW — that map is populated only by authored property
    nodes, so a write a trusted script CONTRACTS to perform was discarded along
    with the stale ones. Both are proxies for one fact, and this is that fact:
    which document-scoped keys the documents at this point actually carry, fed
    by every channel that establishes one and emptied by the single event that
    invalidates them.

    The pair below differs ONLY in whether the declaration is supplied.
    """
    source = "// declares that it writes P"
    steps = [_script_step(source),
             {"kind": "set_ddp", "name": "P", "source_values": [_STATIC, _CURRENT]},
             _BOUND, {"kind": "stop"}]

    assert _codes_with(steps, _declaring(source, ("ddp", "P"))) == ()

    undeclared = _codes_with(steps)
    assert PROCESS_IR_SEMANTIC_DYNAMIC_PATH_DDP_NOT_ESTABLISHED in undeclared, undeclared


def test_a_declaring_node_that_replaces_the_stream_keeps_its_own_write():
    """The ordering half, and it is not a detail.

    A contracted script both REPLACES the document stream and writes onto the
    documents it emits. Emptying the carried set at such a node without keeping
    what the node itself established refused a value that genuinely survives —
    the declaration is about the emitted documents.
    """
    source = "// declares that it writes P"
    caps = _declaring(source, ("ddp", "P"))
    appended = {"kind": "set_ddp", "name": "P", "source_values": [_STATIC, _CURRENT]}

    # Its own write survives its own replacement...
    assert _codes_with([_script_step(source), appended, _BOUND, {"kind": "stop"}], caps) == ()
    # ...but a LATER replacement still invalidates it.
    later = _codes_with(
        [_script_step(source), {"kind": "message", "text": "hi"}, appended, _BOUND,
         {"kind": "stop"}], caps)
    assert PROCESS_IR_SEMANTIC_DYNAMIC_PATH_DDP_NOT_ESTABLISHED in later, later


def test_a_caller_declared_entry_value_establishes_one_too():
    """The third channel: a key the caller declares established at entry.

    Same question asked of the same one notion, so this needs no rule of its
    own — it needs the notion to be fed from here as well.
    """
    from boomi_mcp.compiler.process_ir.semantic_validation.contracts import (
        ProcessIRValidationCapabilitiesV1,
    )

    appended = {"kind": "set_ddp", "name": "P", "source_values": [_STATIC, _CURRENT]}
    steps = [appended, _BOUND, {"kind": "stop"}]

    at_entry = ProcessIRValidationCapabilitiesV1(established_at_entry=(("ddp", "P"),))
    assert _codes_with(steps, at_entry) == ()

    # Control: without the declaration, and with a replacement after it.
    assert PROCESS_IR_SEMANTIC_DYNAMIC_PATH_DDP_NOT_ESTABLISHED in _codes_with(steps)
    replaced = _codes_with([{"kind": "message", "text": "hi"}] + steps, at_entry)
    assert PROCESS_IR_SEMANTIC_DYNAMIC_PATH_DDP_NOT_ESTABLISHED in replaced, replaced
